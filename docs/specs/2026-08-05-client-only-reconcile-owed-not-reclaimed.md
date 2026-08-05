# Reclaim a client-only dependent's `reconcile_owed` count

- **Status:** Implemented. The rationale now lives in
  [the ADR](../adr/2026-08-05-reclaim-a-client-only-owed-count.md); this spec is
  kept as the record of how the design was reached.
- **Date:** 2026-08-05

## Problem

`EdgesAdd` stamps `reconcile_owed` on the edge's source, atomically with the
edge. Exactly one thing ever drains it: `ReconcileOwedDecrement`, called by
`typedController.reconcile` after a successful pass (`reconciler.go:113`). A kind
with no registered controller has the producer and no consumer, so the count and
its index entry last as long as the row.

Edges are deliberately cross-kind, so this is legal and unremarkable: a
controller for kind A declares an edge whose `fromID` is a client-only B object.
Re-creating a deleted edge satisfies the edge-new test again and increments
again, so it is not bounded by the number of distinct targets either.

There is exactly one producer to worry about. `RelationDependsOn` has a single
`EdgesAdd` call site, `ControllerClient.DependenciesAdd` (`controller.go:167`);
`Client`'s only `EdgesAdd` is `RelationOwnedBy`, which stamps nothing. The
stale-dependents pass's `ReconcileOwedStamp` cannot produce it either — its refs
come from `DependentsListStaleSince`, whose kind filter applies to the
*dependent* and is `sd.kinds`, the registered kinds (`sqlite/store.go:986`).

## Impact

Storage only, and it does not compound. Nothing reads the count while the kind
stays client-only, and if the kind later gains a controller the first reconcile
subtracts the whole observed count, so N accrued increments cost one extra pass
rather than N. What is left is a row's worth of bytes and an entry in
`idx_objects_reconcile_owed` that nobody collects.

## Why reclaiming is safe

**The count is redundant with the dependency watermark.** That is the whole
argument, and it is checkable in one place.

> **Invariant R.** A nonzero `reconcile_owed` never co-exists with a dependency
> watermark high enough to hide that dependent from a sweep starting at
> cursor 0.

It holds because it holds at both producers and at the drain:

- `EdgesAdd` stamps the count and *deletes* the dependent's watermark row in the
  same transaction (`sqlite/store.go:2034`). `DependentsListStaleSince` treats
  `c.reconciled_against IS NULL` as stale (`sqlite/store.go:987`), so the
  dependent is listable from cursor 0 for as long as the edge exists.
- `ReconcileOwedStamp`, the stale pass's producer, stamps exactly the refs
  `DependentsListStaleSince` just returned *because* their watermark was low,
  and does not raise it. Only a successful reconcile does
  (`DependencyWatermarksSet`).
- The drain moves both together: a successful reconcile decrements the count and
  sets the watermark. Count and clue fall in step.

So clearing the count destroys the prompt record and never the derivable one. A
process sweeping from cursor 0 re-lists every affected dependent, re-stamps via
`ReconcileOwedStamp`, and enqueues it. Clear the count, never the clue that
regenerates it.

One boundary, not a gap: the clue is re-derivable while the edge exists, and if
the target was physically deleted the edge cascaded with it — so there is no
dependency left and nothing is owed.

### Corollary: a kind that gains a controller later

This is the case that argued for keeping the count, and Invariant R answers it
without a separate argument. `Register` errors once `bh.state != beehiveNew`
(`beehive.go:392`), so a kind gaining a controller necessarily means a new
process. A new process builds a `staleDependents` with `cursor: 0` —
process-local and deliberately never persisted — and the newly registered kind
now in `sd.kinds`. By R, its first sweep re-derives and re-stamps everything the
reclaim threw away.

### Residual

Under genuine multi-process — which `CLAUDE.md`'s waker bullet contemplates
("a write this process did not publish — a second process, or one issued
straight to the `Store`") — one narrow case survives: process A stamps and
enqueues in memory, process B without that kind registered zeroes the count, A
loses the enqueue, and A's stale cursor has already moved past the target. That
dependent strands until a restart. Documented rather than closed: the fix is to
state single-process as an API-level contract somewhere durable, which is a
separate decision from this sweep and must not be smuggled in as a complication
of it.

## Rejected: gate the stamp instead of reclaiming it

Not stamping in the first place is cheaper in the abstract and worse in
practice. `DependenciesAdd` does not learn `fromID`'s kind until `res.From`
comes back *from* the `EdgesAdd` call, so gating means either resolving the
source's kind in a pre-read on a path controllers re-run every pass, or pushing
the registered-kind set into `EdgesAddInput` and gating in SQL — a kind list
threaded through every edge write. Splitting it into a read then a stamp also
breaks the indivisibility the comment at `controller.go:161` exists to protect.

The sweep needs the same kind list, once per sweep instead of once per write.

## Design

One store verb, one call site, riding the existing partial index.

### Store surface

Add to `storeapi.Store`, listed alphabetically within the `ReconcileOwed*`
family (so: `Clear`, `Decrement`, `ListIDs`, `Stamp`):

```go
// ReconcileOwedClear zeroes reconcile_owed for every object whose kind is not
// in keep, and returns how many rows it cleared. An empty keep clears every
// nonzero row, which is correct: with no reconcilers, nothing consumes a count.
// Bumps no resource_version and appends no write-log entry.
ReconcileOwedClear(ctx context.Context, keep []GroupKind) (int64, error)
```

`keep` rather than an exclude list reads better at the call site and matches
what the caller has.

The sqlite implementation is a single no-emit `UPDATE`, sibling to
`ReconcileOwedIncrement`/`Decrement` (`sqlite/store.go:921`):

```sql
UPDATE objects SET reconcile_owed = 0
 WHERE reconcile_owed != 0
   AND ("group", kind) NOT IN (VALUES (?, ?), (?, ?))
```

- **Build the tuple list the way `DependentsListStaleSince` does**
  (`sqlite/store.go:971-986`): `tuples[i] = "(?, ?)"`, two args appended per
  kind, joined into `IN (VALUES ` + `strings.Join(tuples, ", ")` + `)`. The
  `placeholders` helper is **not** usable here — it emits a flat `?, ?, ?`
  (`sqlite/store.go:2591`), and `("group", kind) NOT IN (?, ?, ?, ?)` is an
  arity mismatch, not a working query.
- `NOT IN` is NULL-safe here without a guard: `"group"` and `kind` are both
  `NOT NULL`, so the usual `NOT IN` trap does not apply.
- An empty `keep` must drop the `NOT IN` clause entirely — `NOT IN (VALUES)` is
  a syntax error, and the semantics wanted are "clear everything".
- The leading `reconcile_owed != 0` is what should drive the planner to
  `idx_objects_reconcile_owed` (`sqlite/migrations/0001_init.sql:81`). There is
  no equality constraint here, so this is a preference, not a certainty — pin
  it with an EQP assertion (see **Tests**) rather than asserting it in a
  comment.
- Return `res.RowsAffected()`.

### Call site

`gcSweeperRun` (`beehive.go:211`), between `writeLogRetentionSweep` and
`freePagesSweep`, so anything it frees is released on the same tick:

```go
bh.reconcileOwedReclaimSweep(ctx)
```

The sweep itself mirrors `freePagesSweep` — best-effort, warn and swallow,
retried on the next tick, and logging what it actually did:

```go
// reconcileOwedReclaimSweep zeroes the owed count on rows whose kind has no
// reconcile loop, which nothing else drains. See docs/adr/<file>.
func (bh *Beehive) reconcileOwedReclaimSweep(ctx context.Context) {
	cleared, err := bh.store.ReconcileOwedClear(ctx, bh.registeredKinds())
	if err != nil {
		bh.log().Warn("reconcile-owed reclaim failed; retry next sweep", "err", err)
		return
	}
	if cleared > 0 {
		bh.log().Debug("reclaimed owed counts", "rows", cleared)
	}
}
```

Logging the count rather than discarding it is deliberate: a return value nobody
reads sits badly against
[the write-shapes ADR](../adr/2026-07-30-store-write-shapes.md), and
`freePagesSweep` already sets this precedent.

`registeredKinds()` is **new** — add it next to `isRegistered` (`beehive.go:440`)
and take `bh.mu`, as every other accessor there does.
`staleDependentsRun` reads `bh.order` without the lock (`waker.go:495`) and is
not a precedent to copy. `bh.order` is frozen after `Start`, so one snapshot per
sweep is stable; taking it per sweep rather than once keeps the sweep
independent of start ordering.

### Accepted cost

Unlike `eventRetentionSweep`/`writeLogRetentionSweep`, which early-return when
unconfigured, this sweep issues an unconditional `UPDATE` on every GC tick
(`defaultGCInterval`, 30s) forever. A zero-row `UPDATE` dirties no pages, so it
costs no WAL, but it does take the write path on the single connection every
tick. Accepted: there is nothing to configure, and a read-then-write to avoid it
would cost the same round trip. Noted here so the asymmetry with its neighbours
reads as a decision rather than an oversight.

## Invariants

- **`EdgesAdd` keeps clearing the dependency watermark unconditionally**
  (`sqlite/store.go:2034`). That clear is Invariant R's first leg and the whole
  recovery path. This is the one thing a reviewer must check.
- **The clear is no-emit.** It must not route through `bumpObject`, bump
  `resource_version`, or append to `object_writes`. An emitting sweep would wake
  every watch tailer and the dependency waker on every GC tick, for a write no
  consumer cares about.
- **Registered kinds are never touched**, so the sweep cannot race a legitimate
  stamp. `ReconcileOwedDecrement` subtracts an observed count and this sets an
  absolute 0, but they never address the same row.
- Idempotent and repeatable, like every other sweep.

## Tests

Store level, `sqlite/store_test.go`:

- Clears a nonzero count for a kind absent from `keep`; leaves a kind present in
  `keep` untouched, count intact.
- Empty `keep` clears every nonzero row.
- Returns the number of rows cleared; returns 0 and no error when there is
  nothing to clear.
- Bumps no `resource_version` (`ResourceVersionsMaxIssued` unchanged across the
  call) and appends no `object_writes` entry.
- **EQP assertion** via the existing `queryPlan` helper (`sqlite/store_test.go:586`),
  asserting the plan uses `idx_objects_reconcile_owed` — the way the
  `idx_objects_deleting` and `idx_events_object_rv` assertions do, with the same
  "keep them aligned" framing. This pins the one performance claim the design
  makes.

Beehive level, `beehive_test.go`, where the other sweeps are tested:

- A client-only object with an accrued count has it zeroed by a sweep; a
  registered kind's object with a count still has it when the sweep returns, and
  its owed pass still finds it.
- **No-emit, where it bites**: an active `ObjectsWatch` on the cleared kind
  receives nothing from a sweep that clears counts. The store-level assertion
  above is indirect; this is the failure the second Invariant guards against.
- **Empty `keep` in production shape**: a `Beehive` with zero registered
  controllers sweeping counts left by a prior process. This is the path that
  exercises the dropped `NOT IN` clause.
- A failing `ReconcileOwedClear` is warned and swallowed, and the other sweeps
  in the same tick still run.

Whitebox in `package beehive`, `require` for preconditions, `assert` for
independent checks, signals not sleeps — as everywhere else.

`fakeStore` in `testutils_test.go` needs the new method. Answer `(0, nil)` like
the listings above it rather than `panic`, since the sweeper runs in fixtures
that do not care about it.

## Docs to update when this lands

- New ADR, `docs/adr/2026-08-05-reclaim-a-client-only-owed-count.md`. Its
  content is Invariant R plus the rejected alternative — the mechanism is
  trivial, the reasoning is not. Record the multi-process residual there too.
  Link it from `docs/adr/README.md`.
- `internal/storeapi/storeapi.go:180` — the `ReconcileOwed` field godoc says the
  count is "moved only by EdgesAdd's stamp, ReconcileOwedStamp and
  ReconcileOwedDecrement". Add `ReconcileOwedClear`.
- `CLAUDE.md` — the GC bullet, which lists what the global sweeper does.
- `docs/TODO.md` — delete the "client-only dependent's `reconcile_owed` count is
  never reclaimed" entry (lines ~328–361). Its "gating the stamp is the wrong
  fix" reasoning survives in **Rejected** above; its "the kind gains a controller
  later case argues for keeping the count" hesitation is answered by Invariant R
  and does not.
- `docs/reconcile-triggers.md` needs **no** entry: this sweep triggers no
  reconcile. Worth one line in the ADR saying so, since a reader will wonder.
