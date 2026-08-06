# A deletion mark pushes the target it unblocks

- **Status:** Proposed — not yet built, and the build trigger is not met (see
  "Should this be built at all"). Tracked at `docs/TODO.md`.
- **Date:** 2026-08-06

## Problem

`gcCollect` refuses to remove a deletion-pending row while `EdgesHasIncoming`
reports a live referrer under RESTRICT. `EdgesHasIncoming` discounts a
`depends_on` edge whose source is itself deletion-pending
(`sqlite/store.go:2404-2417`), so marking the last live referrer deletion-pending
lifts the target's block on the spot.

Nothing signals it. `Client.Delete` and `Client.DeleteByName` call
`signalDeletionRequested` (`client.go:867-870`, `:897-900`), which enqueues only
the object that was just marked — never the targets its new deletion-pending
state may have unblocked. Those targets wait for the GC sweeper's next tick.

This is the fourth exit from the RESTRICT block enumerated in
[`docs/reconcile-triggers.md`](../reconcile-triggers.md) case 11, and the only
one that does not push. Cost: one GC interval of latency, never divergence — the
target stays deletion-pending, a durable predicate the sweeper lists, and the
sweeper cannot be disabled.

## Design

Route 2 ([physical delete pushes its owner](../adr/2026-08-05-a-physical-delete-pushes-its-owner.md),
`gc.go:35-116`) lists outgoing edges and then `ObjectsGetMeta`s each target.
**That is the wrong template here.** `ObjectsGetMeta` reads the whole row —
`objectColumns` includes `spec` and `status` (`sqlite/store.go:511-515,
751-753`). Route 2 affords one such read because an object has one owner; here N
is the object's *dependency* count, unbounded, on a public path walked by every
`Delete`, overwhelmingly in the common case where nothing is deletion-pending
and nothing gets pushed.

Route 3 — the actual sibling in shape — computes its verdict in one SQL
statement over both endpoints and returns `Unblocked`
(`sqlite/store.go:2253-2269`). **Follow route 3.**

### The predicate

```sql
SELECT o.id, o."group", o.kind
FROM edges r JOIN objects o ON o.id = r.to_id
WHERE r.from_id = ? AND r.relation = 'depends_on'
  AND o.deletion_requested_at IS NOT NULL
  AND o.id <> r.from_id
```

Both sides are covered. `edges` is `WITHOUT ROWID` with
`PRIMARY KEY (from_id, to_id, relation)`
(`sqlite/migrations/0001_init.sql:120-136`), so the `from_id`-prefixed source
side is a primary-key range scan — this is the same access
`EdgesListOutgoingByRelation` already relies on; there is no `idx_edges_from`
and none is needed. The target side is answered by `idx_objects_deleting`, whose
key is `objects(id, "group", kind)` under a partial
`WHERE deletion_requested_at IS NOT NULL` (`:66-72`) — exactly the three columns
selected, under exactly this predicate — so it is a covering probe with no row
fetch and no blob touched.

That is the substantive win over route 2's shape, and it is a property of the
schema, not a hope: no `spec`/`status` transfer, one round trip instead of N+1.
The `idx_objects_deleting` comment says "keep them aligned"; this becomes its
second consumer, which the ADR should record so a later edit to either does not
silently reinstate a row fetch. Confirm with `EXPLAIN QUERY PLAN` before
landing — the planner's choice is never a promise.

Results scan through the existing `scanObjectRefs`, so each ref routes a requeue
with no further read.

### Where it runs: inside the mark's transaction

The read is folded into `requestDeletion`'s existing `Within`
(`sqlite/store.go:2007-2014`) and returned from the deletion-request writes.

**The alternative — reading at the call site after the mark commits — is
rejected, and not on the grounds first supposed.** It looked cheaper because it
adds no signature change; it does not. The read reaches the store through
`bh.store`, so it is a *new member on the exported `Store` interface*, which
this project already counts as a break of the same class: `docs/TODO.md:218-221`
records that `EventsMaxVersion` "added a `Store` member and broke every external
backend." Once the cascade needs the grouped form too, that route costs **two**
new members against this one's changes to methods that already exist.

Both options being breaks, atomicity is the only live axis, and it is decisive:

- The mark has already committed by the time `marked` returns, so a call-site
  read runs in no transaction unless the caller supplied an ambient `Within`.
  The sweeper can physically remove a target in the gap, so the site would need
  a documented `ErrNotFound`-skip policy, and a "do not fail the `Delete`" rule
  for every other read error — because the mark is durable and a retried
  `Delete` returns `marked == false`, pushing nothing. Inside the transaction
  none of that exists: a failed read rolls the mark back for a clean retry.
- The predicate omits "and the source is deletion-pending" because the caller
  just marked it. As an exported `Store` member that precondition has to be
  documented, and a later caller passing a live source gets pushes for targets
  it still blocks — a wrong answer, not merely a wasted one. Inside the write
  that establishes the precondition, it cannot be violated.

### Store surface

Three writes gain results. `DeletionRequestsCreateByName` already returns
`(ObjectID, bool, error)` (`sqlite/store.go:2029`), so adding refs would make
four values — the exact case the
[write-shapes ADR](../adr/2026-07-30-store-write-shapes.md) produced
`EdgesAddResult`/`EdgesDeleteResult` for. Introduce result structs so the break
is one coherent change:

```go
// DeletionRequestResult ... ID is set by the name-keyed sibling only.
type DeletionRequestResult struct {
    ID        ObjectID
    Marked    bool
    Unblocked []ObjectRef
}

// DeletionCascadeResult ... Unblocked is flat across children: gc.go merges
// every push into one hook.
type DeletionCascadeResult struct {
    Children  []DeletionCascadeChild
    Unblocked []ObjectRef
}
```

`Unblocked` is flat on the cascade result rather than a third field on
`DeletionCascadeChild` (`internal/storeapi/storeapi.go:251-254`) because no
caller reads it per child — see below. Final names are the ADR's call.

### Gate analysis

- **The mark actually happened.** `Marked` bounds the push to once per object,
  the same bound `signalDeletionRequested` already relies on. On the cascade
  path the store must apply the same gate before collecting a child's targets:
  `gcCollect` reruns after every reconcile of a deleting object, and an ungated
  push would re-arm the subtree each time (`gc.go:62-66`).
- **The target is deletion-pending.** A live target was never blocked. A
  correctness gate, not an optimisation: `signalRequeueManyNow` → `requeueNow`
  bypasses the re-enqueue floor (`beehive.go:517-529`), so pushing live targets
  on every delete would spin.
- **The relation is `depends_on`.** `EdgesHasIncoming`'s discount is
  `depends_on`-specific — `owned_by` always counts until physical removal — so
  only a `depends_on` target can have been unblocked. `owned_by` edges from the
  same source belong to route 2.
- **Not a self-edge.** A `depends_on` self-edge is structurally allowed
  (`controller_test.go:453`) and the waker skips `from_id == to_id` for the same
  reason. Without the exclusion, marking such an object pushes it twice.

**Not gated on being the genuinely last live referrer.** That would cost an
`EdgesHasIncoming` per target to save a dispatch `gcCollect` already re-derives
(`gc.go:82-88`) — the trade route 3 rejects, and the probe-not-verdict stance
`docs/reconcile-triggers.md:597-600` states for all three existing routes.

**Immediate, not throttled**, for route 3's reason: the target is finalizing and
already carries the alarm its own delete push armed, so a throttled push would
be absorbed.

### Call site 1: both client deletes

Fold the push into `signalDeletionRequested` (`client.go:876`) rather than into
`Delete`. `DeleteByName` takes a different store call but the same signal, so
one change covers both and neither can drift.

`signalRequeueManyNow` registers an `AfterCommit` hook **of its own**
(`beehive.go:521`); it is not appended to the one `signalRequeueNow` already
registered. Two hooks, in registration order. Outside an ambient transaction
both run inline immediately, so "at commit" is misleading language at this site
and should not appear in the ADR.

### Call site 2: the cascade

`deletionRequestsCreateFromOwner` (`sqlite/store.go:2079-2093`) marks children
deletion-pending one by one, and any child may be a live `depends_on` referrer —
the identical gap. Leaving it out would make the fourth exit only *partly*
closed, contradicting `docs/reconcile-triggers.md:591-594` and the
dropped-dependency ADR's Consequences.

**The store collects; `gc.go` pushes.** `signalRequeueManyNow` is not reachable
from the store, and `gc.go:50-68` is already the place that turns cascade
results into pushes. So `DeletionRequestsCreateFromOwner` returns
`DeletionCascadeResult.Unblocked`, gathered inside the transaction it already
runs, using the grouped form of the same predicate — `EdgesGroupOutgoingByID`
(`sqlite/store.go:2369`) is the existing bulk shape: one query for every child
marked, not one per child.

**The refs merge into `gc.go`'s existing `pushed` slice** (`gc.go:56, 68`), so
the cascade still registers exactly one `signalRequeueManyNow` hook. That is why
the `AfterCommit` user count in `CLAUDE.md` goes eleven → **twelve**, not
thirteen: this adds one new user (the deletion-mark push in
`signalDeletionRequested`) and extends one existing user's payload.

## Testing plan

Whitebox (`package beehive`), following `TestPhysicalDelete*` (`gc_test.go:452ff`)
and `TestDelete*` (`client_test.go:3106ff`).

Client deletes:

- `TestDeleteRequestPushesTheBlockedTarget` — sole live referrer marked; target
  already deletion-pending; target pushed.
- `TestDeleteRequestPushesNoLiveTarget` — target not deletion-pending; nothing
  pushed.
- `TestDeleteRequestPushesNoOwnedByTarget` — an `owned_by` edge from the same
  source is not pushed.
- `TestDeleteRequestPushesNoSelfEdge` — a `depends_on` self-edge yields exactly
  one enqueue, not two.
- `TestDeleteRequestPushesEveryBlockedTarget` — several deletion-pending
  targets, all pushed (`addEdge`, `testutils_test.go:947`).
- `TestDeleteRequestPushesEvenWithAnotherLiveReferrer` — probe semantics: the
  push fires, and the pushed reconcile leaves the row uncollected.
- `TestDeleteRequestPushesAcrossKinds`, `TestDeleteRequestSkipsClientOnlyTarget`.
- `TestDeleteByNamePushesTheBlockedTarget` — the name-keyed store call.
- `TestRepeatedDeleteRequestPushesOnce` — the `Marked` gate.

Cascade:

- `TestCascadeMarkPushesAChildsBlockedTarget` — an owned child is a live
  referrer of a deletion-pending target; cascading the owner pushes that target.
- `TestCascadeMarkPushesEveryChildsTargets` — two children, one target each,
  both pushed in one hook.
- `TestCascadeMarkPushesNoTargetOfAnUnmarkedChild` — a child already
  deletion-pending contributes nothing on the re-cascade (the `Marked` gate,
  the re-arm case `gc.go:62-66` names).
- `TestCascadeMarkMergesTargetsIntoOnePush` — pins the one-hook property the
  `AfterCommit` count depends on.

Integration pairs (each proving its path suffices alone):

- `TestIntegrationDeleteRequestUnblocksTargetWithoutASweep` / `...WithoutThePush`
- `TestIntegrationCascadeMarkUnblocksTargetWithoutASweep`

## Docs to update when this lands

- `docs/TODO.md` — delete the entry.
- `docs/reconcile-triggers.md` — **a new route 4 under case 11**, not just an
  edit: the existing routes are numbered entries with their filters called out
  as load-bearing (`:569-590`), the "fourth exit is unsignalled" paragraph
  (`:591-594`) is deleted, the probe-not-verdict paragraph (`:597-600`) grows a
  fourth clause, and the case gets a `Tests:` line in the house style of
  `:646-650`.
- `docs/adr/2026-08-05-a-dropped-dependency-pushes-its-target.md` — its
  Consequences section asserts the fourth exit is unsignalled.
- `CLAUDE.md` — the `AfterCommit` user count (eleven → twelve, per above) and
  its ADR link list.
- This spec collapses into an ADR and is deleted, per
  [`docs/adr/README.md`](../adr/README.md). The title must not collide with
  `2026-08-04-a-delete-request-pushes-its-own-collect.md`, which names a
  different fact.

## Should this be built at all

`docs/TODO.md` sets the trigger: build it "when the latency is measured to
matter, or when a second consumer of the same read appears." **Neither has been
demonstrated.** No measurement is on record, and the cascade site is a second
consumer only because this spec puts it in scope — which is circular, and is
recorded here rather than leaned on.

What is bought is latency alone — one GC interval, never divergence. What is
paid is one indexed query per successful mark, plus a `Store` break across three
deletion-request writes. Defensible, but a trade: the ADR should open by saying
which trigger fired rather than inheriting the assumption that one did.

## Alternatives considered

- **Route 2's shape (`ObjectsGetMeta` per target).** Rejected on cost: N+1
  full-row reads including blobs, unbounded fan-out, on every delete.
- **Reading at the call site instead of inside the mark.** Rejected above: it is
  a `Store` break too, so it buys nothing, and it gives up atomicity, an
  undocumentable precondition and an error policy it would have to invent.
- **A third field on `DeletionCascadeChild`.** Rejected: no caller reads the
  refs per child — `gc.go` merges them into one slice — so the flat field on the
  result matches what is actually read.
- **Gating on `EdgesHasIncoming` for a genuine last-referrer verdict.**
  Rejected: a query per target to save a dispatch `gcCollect` re-derives anyway.
- **Folding into the dropped-dependency push.** Rejected by that ADR itself:
  different trigger, different site, its own gates.
