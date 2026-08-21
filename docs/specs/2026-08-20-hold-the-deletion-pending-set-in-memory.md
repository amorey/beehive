# Hold the deletion-pending set in memory

- **Status:** Planned.
- **Date:** 2026-08-20
- **Depends on:** [a write mark per kind](2026-08-20-a-write-mark-per-kind.md)
  for its shape, though it keeps a set rather than a mark.

## Why

The GC sweeper lists every deletion-pending object twice a minute
(`beehive.go:339`):

```sql
SELECT id, "group", kind FROM objects
 WHERE deletion_requested_at IS NOT NULL ORDER BY id
```

On a store where nothing is being deleted — the normal state — that is an index
scan finding nothing, forever.

Rows enter that set through `requestDeletion`, the owner cascade, and nothing
else. They leave it through `objectsDelete`. All four are in this process.

## The change

`Beehive` keeps the set:

```go
// deletionPending is the set the sweeper walks. Maintained rather than listed:
// a row becomes deletion-pending only through this process. The listing stays
// as the seed at Start and as a periodic re-read, because a set that drifts
// silently strands the row it forgot.
```

The sweep walks the set. It still lists from the store in two cases: once at
`Start`, and every `reconcileEvery` sweeps (say 20, so ten minutes at the default
cadence) as a correctness net.

## The rule this rests on

**A forgotten row is stranded forever.** For a client-only kind this sweep is the
only collector — the code says so at the call site. A mark that is merely stale
costs a late scan; a set that is missing an id costs an object that is never
collected, and nothing reports it. That asymmetry is why this one keeps a periodic
re-read where the other gates do not.

## Edge cases the implementer would otherwise guess at

- **Every producer must add.** `DeletionRequests().Create`, `CreateByName`,
  `CreateFromOwner` (which marks a whole subtree, and reports the children it
  marked), and the FK cascade behind an owner's physical delete. Route the adds
  off `DeletionCascadeResult.Children` rather than re-deriving them.

- **Removal is on the physical delete, not on the collect attempt.** `gcCollect`
  returns `deleted`; remove only when it is true. A row blocked by a finalizer or
  a RESTRICT stays pending and must stay in the set.

- **The re-read reconciles both ways.** It adds ids the set is missing and drops
  ids the store no longer has. Log a difference at warn — a drift is a bug in one
  of the producers above, and it should be findable.

- **Order.** The listing is `ORDER BY id`, and the sweep's per-row failures are
  logged individually. Walk the set in id order so failures stay reproducible.

- **`AdminClient` writes outside a pass**, including on a stopped beehive. It
  cannot reach this set. That is fine — a stopped beehive has no sweeper, and the
  next `Start` seeds from the store — but it is the reason the seed exists.

## Tests

In `gc_test.go`:

- A sweep with an empty set issues no listing.
- A delete request makes the next sweep collect, with no listing in between.
- A cascade's children are collected without a listing.
- A row that fails to collect is retried on the next sweep.
- The periodic re-read picks up a row inserted behind the beehive's back (a
  direct store write in the test) and warns. This is out of contract, and it is
  the honest way to test the net.
- A fresh beehive over a store with deletion-pending rows collects them, seeded
  by the listing at `Start`.

## On ship

ADR: **the sweeper walks a set it maintains**, recording the seed, the periodic
re-read, and why this gate is stricter than the others.

`CLAUDE.md`'s GC bullet describes two backstops. Both stay; add that the global
sweeper's listing is now a seed and a net rather than the sweep itself.
