# A dropped dependency pushes the collect it was blocking

- **Status:** Ready to implement. Closes route 3 of case 11 in
  [`../reconcile-triggers.md`](../reconcile-triggers.md) and retires the latency
  half of the "edge write is invisible to every cursor" entry in
  [`../TODO.md`](../TODO.md).
- **Date:** 2026-08-05
- **Base:** the `refactor` tip (`e7727a4`); line references are as of it.
- **Commit:** `feat(edges)!` — `Store` is a public alias (`types.go`), so the
  `EdgesDelete` signature change breaks the exported interface, as in
  `feat!: narrow the storeapi write contract`.

## Problem

Most writes leave a footprint a cursor is watching for: a bumped
`resource_version`, a row in the write log, or a stamped counter. `EdgesAdd` and
`EdgesDelete` leave none — a ref is not a field of the object. `EdgesAdd` covers
itself with a deliberate `reconcile_owed` stamp. `EdgesDelete` covers nothing.

One consumer pays. `gcCollect` refuses to remove a deletion-pending row while
`EdgesHasIncoming` reports a referrer under RESTRICT. Three routes lead out of
that block, and two of them push at commit: a cleared finalizer
([ADR](../adr/2026-08-05-a-cleared-finalizer-pushes-its-own-collect.md)) and a
physically deleted last child
([ADR](../adr/2026-08-05-a-physical-delete-pushes-its-owner.md)). The third —
`ControllerClient.DependenciesDelete` dropping the last live referrer — signals
nothing, so the target waits for the GC sweeper's next tick (30s by default).

No divergence: the sweep always finds it. Latency only, in this one spot.

## Why the counter is not the fix

The deferred direction in `TODO.md` was a single monotonic epoch bumped by
`EdgesAdd` and `EdgesDelete`, shaped like `driver_cursors`. **It does not fix
this.** A counter is something a driver *reads on its own schedule*: it lets a
tick skip work when nothing moved, which is a cost optimisation. Nothing here is
paying cost — the sweeper's idle tick is already cheap. What is paying is
latency, and latency needs a wake, which needs the identity of the object to
enqueue. An epoch carries no identity, so a consumer of one would still have to
tick to discover the bump and then re-derive who moved — the tick this spec
exists to beat.

So: no new counter, no new table, no `object_writes` entry for edges (the
four-count argument against that stays in `TODO.md` and stays correct). The fix
is the same shape as routes 1 and 2 — the write reports what it changed, and the
caller pushes at commit.

## Decision

`EdgesDelete` returns what a caller needs to follow up on the edge it dropped;
`DependenciesDelete` pushes the target's collect at commit when the drop could
have unblocked one.

### Store surface

`storeapi.Store`:

```go
// EdgesDelete removes the (fromID, toID, relation) edge; removing a missing
// one does nothing. Bumps no version.
EdgesDelete(ctx context.Context, fromID, toID ObjectID, relation Relation) (EdgesDeleteResult, error)
```

`EdgesDeleteResult` goes beside `EdgesAddResult` in
`internal/storeapi/storeapi.go`, not with the interface:

```go
// EdgesDeleteResult is what a caller needs to follow up on an edge it dropped;
// like EdgesAddResult, all of it falls out of work EdgesDelete already does.
type EdgesDeleteResult struct {
	// To is the target's GroupKind, needed to route a requeue to it; edges are
	// cross-kind. Zero unless Unblocked.
	To GroupKind
	// Unblocked reports that this call removed an edge that was holding a
	// RESTRICT block: the target is deletion-pending and the source is not. A
	// probe, not a verdict — it does not check that this was the last referrer.
	Unblocked bool
}
```

One field rather than the three facts behind it, because only their conjunction
is ever read — see the
[write-shapes ADR](../adr/2026-07-30-store-write-shapes.md).

`sqlite/store.go`, two statements, **no self-`Within`** (see below):

1. `DELETE FROM edges WHERE from_id = ? AND to_id = ? AND relation = ?`; take
   `RowsAffected`. Zero → zero result, no second query. (`EdgesAdd`'s stamp
   already depends on modernc's count the same way; the comment there applies.)
2. Only on a removal, both endpoints in one row — a join rather than scalar
   subqueries, for the reason `EdgesAdd` gives:

   ```sql
   SELECT t."group", t.kind,
          t.deletion_requested_at IS NOT NULL AND f.deletion_requested_at IS NULL
   FROM objects t, objects f WHERE t.id = ? AND f.id = ?
   ```

   Scanned as `int` for the flag, as `EdgesHasIncoming` does.

`DELETE … RETURNING` cannot serve step 2: SQLite forbids subqueries in
`RETURNING` expressions, and the columns live on `objects`.

**No self-`Within`, and `sql.ErrNoRows` from step 2 means not-unblocked.**
`EdgesDelete` is one autocommit `Exec` today. Wrapping it would hold the store's
**sole connection** (`SetMaxOpenConns(1)`, `sqlite/sqlite.go`) across a
`BEGIN`…`COMMIT` for a call that is one statement now — on a path a controller
hits every pass, and against writers that have nowhere else to go. That is the
argument the watch tail's page budget already rests on; the round-trip count is
the smaller half of it. `EdgesAdd` is not a precedent: there the stamp, the
clear and the insert genuinely must be one unit.

What the transaction would buy is only the guarantee that step 2 finds its rows.
The gap between the two statements is real **in-process** — the GC sweeper is a
concurrent goroutine, and in autocommit its `ObjectsDelete` can land between
them — and every way it can bite is benign:

- The sweeper collected the target between the two statements → `ErrNoRows` →
  no push. Correct: there is no row left to collect.
- The source was physically deleted → `ErrNoRows` → no push. Correct: a row
  reaching `ObjectsDelete` was deletion-pending, so its edge was already
  discounted.
- The target was marked deletion-pending in the gap → `Unblocked` for an edge
  dropped while it was live. Harmless: it is blocked-or-not *now*, and the push
  is a probe.

There is no transition in the other direction — nothing un-deletes a row — so
the window admits no wrong answer worth a transaction. A caller that needs the
drop and the read atomic with its own writes opens `Within` itself, and both
statements join it, unchanged.

### Call site

`controller.go`, replacing `DependenciesDelete`'s pass-through (and its
"schedules nothing" doc comment):

```go
res, err := c.bh.store.EdgesDelete(ctx, fromID, toID, RelationDependsOn)
if err != nil {
	return err
}
if res.Unblocked {
	c.bh.signalRequeueNow(ctx, ObjectRef{ID: toID, Group: res.To.Group, Kind: res.To.Kind})
}
return nil
```

`AfterCommit` gains its tenth user — `signalEventsWritten` (`7c7a7f0`) is the
ninth. The enqueue sits outside the store's own
statements, so with no ambient transaction it runs inline after the delete
commits, and a caller's savepoint unwind discards it — same as `DependenciesAdd`.

### The gates, and why each one

A dispatch is not cheap: `requeueNow` runs the controller's `Reconcile` in full
before `gcCollect` gets to re-check the block (`reconciler.go`). So each gate
below is worth its query.

- **An edge was actually removed.** Bounds the push to once per edge ever
  created, matching `EdgesAdd`'s edge-new gate. A controller that calls
  `DependenciesDelete` unconditionally each pass would otherwise push at
  reconcile rate.
- **The target is deletion-pending.** A live target was never blocked, and
  `requeueNow` bypasses the re-enqueue floor. Same gate, same reason, as the
  physical delete's owner push.
- **The source is *not* deletion-pending.** `EdgesHasIncoming` discounts a
  `depends_on` edge from a deletion-pending source, so dropping one cannot
  unblock anything. This is not an edge case: the natural caller is a finalizing
  dependent releasing its refs during cleanup, which is exactly the shape that
  would push on every pass for nothing. Route 2 already treats the symmetric
  filter as load-bearing (case 11, filter one); route 3 matches it. It is free —
  step 2 is already reading `objects`.
- **Not "was this the last referrer".** That is the one check to skip: it would
  cost an `EdgesHasIncoming` on every drop to save a dispatch that `gcCollect`
  re-derives anyway, and the two gates above already establish the push as a
  probe.

**Immediate, not throttled.** A throttled push would be absorbed by the target's
own pending alarm — armed by its delete push a moment earlier — and the backoff
ladder it lands on tops out at `defaultMaxRetryInterval`, which *equals* the
default GC interval (both 30s). So an alarm armed just after a tick pushes the
collect past the next one, and the absorbed push buys nothing over the sweep it
was meant to beat. Termination: the target's row is on its way out, and the work
queue coalesces repeat pushes at a queued or in-flight id.

**A client-only target gets no push** — `signalRequeueNow` resolves no reconciler
and drops it — and falls back to the sweeper, as every push path does.

## Non-goals

- **`EdgesDeleteFinalizingDependsOn` stays `error`-returning.** It removes edges
  into the row `gcCollect` is collecting right now, in that collect's own
  transaction, and the collect re-checks `EdgesHasIncoming` two statements
  later. There is nothing to signal.
- **The `ON DELETE CASCADE` of a physical delete stays unsignalled.** It removes
  the dying row's outgoing edges, and that row was deletion-pending, so
  `EdgesHasIncoming` was already discounting its `depends_on` edges: those
  targets were never blocked. The `owned_by` half is route 2.
- **Edge writes remain invisible to cursors in general.** This spec closes the
  one consumer, not the class.
- **A fourth route out of the same block stays open** — see below. Disclosed,
  not fixed here.

## The fourth route, disclosed not closed

The source gate above declines the push when the dropped edge came from a
deletion-pending dependent, and that is right: nothing was unblocked *by the
drop*. But the unblocking already happened — at the **mark**. Marking the last
live referrer deletion-pending lifts the target's RESTRICT block, because
`EdgesHasIncoming` discounts a `depends_on` edge from a deletion-pending source,
and `signalDeletionRequested` (`client.go`) enqueues only the object it marked.
So the target waits for the sweep: 30s, route 3's shape exactly, reached through
the ordinary "delete a dependent, then its target" sequence.

Two consequences for this spec:

- **Case 11 must not claim the block is fully pushed.** The planned rewrite says
  "all three routes push"; it has to name this exit and point at a `TODO.md`
  entry, or the doc reads as complete over a block that still has an unsignalled
  way out.
- **The fix is the shape of route 2, at the delete-request site**:
  `EdgesListOutgoingByRelation(id, RelationDependsOn)` before the mark commits,
  filtered to deletion-pending targets, pushed with `signalRequeueManyNow`. It
  is a follow-up rather than a widening of this one — it touches `client.go`'s
  delete path and the `…ByName` sibling, needs its own gate analysis (the mark's
  `marked` bool already bounds it to once per object), and its own ADR.

## Tests

`controller_test.go`. `fakeStore` holds no edge state and `fakeStore.EdgesAdd`
is still a `panic`, so these follow the established per-test canned-result
pattern (`failEdgesAddStore`) rather than growing the fake:

- `TestDependenciesDeletePushesTheBlockedTarget`
- `TestDependenciesDeletePushesNothingForALiveTarget`
- `TestDependenciesDeletePushesNothingForAMissingEdge`
- `TestDependenciesDeletePushesNothingForAFinalizingDependent` — the source gate
- `TestDependenciesDeletePushesAcrossKinds` — routed by `res.To`, not the
  controller's own kind
- `TestDependenciesDeletePushBeatsAPendingAlarm` — `requeueNow`, not `enqueue`
- `TestDependenciesDeleteSkipsClientOnlyTarget`

`sqlite/store_test.go`, where the gates meet real rows:

- `TestEdgesDeleteReportsTheUnblockedTarget` — `Unblocked` and `To`
- `TestEdgesDeleteReportsNothingForAMissingEdge`
- `TestEdgesDeleteReportsNothingForALiveTarget`
- `TestEdgesDeleteReportsNothingForADeletingSource`
- `TestEdgesDeleteJoinsTheAmbientTransaction` — a rollback discards the drop

`gc_test.go`: `TestIntegrationGCDeleteDependencyUnblocksTarget` (which already
drives `depDroppingController`) splits into the pair the other two routes use —
`TestIntegrationDroppedDependencyCollectsWithoutASweep`, and a
`…WithoutThePush` twin that calls `store.EdgesDelete` directly so no hook fires
and the sweep is the route.

### Call sites the signature change touches

`fakeStore.EdgesDelete` (keep the `panic`, new signature),
`controller_test.go`'s `failEdgesDeleteStore`, the `store.EdgesDelete` call in
`reconciler_test.go`'s `TestClientOnlyTargetDeletionUnwedges`, and the direct
`store.EdgesDelete` calls in `sqlite/store_test.go`.
`gc_test.go`'s `collectFakeStore` needs nothing: it embeds `fakeStore`, and
`gcCollect` never calls `EdgesDelete`.

## Docs to update

- **New ADR**, `docs/adr/2026-08-05-a-dropped-dependency-pushes-its-target.md`,
  plus its line in the ADR index.
- **`CLAUDE.md`**: `Store.AfterCommit` has *ten* users — the new one inserted
  into a list that already ends `signalKindWritten` … `and signalEventsWritten`,
  so it goes before that tail, not after it; the GC bullet gains this push
  beside the delete-request, cascade and physical-delete ones.
- **`docs/reconcile-triggers.md`** section 1: "exactly six push paths" becomes
  seven, with a row for this one (made by `ControllerClient.DependenciesDelete`,
  starting the dropped edge's target, gated on `EdgesDeleteResult.Unblocked`),
  and the "five of the six are immediate" sentence follows it.
- **`docs/reconcile-triggers.md`** case 11: rewrite route 3 with its two
  filters, drop the "Route 3 waits for the next sweep" line, fold the two
  duplicated "the push is a probe" paragraphs into one, extend the test list —
  and add the mark-side exit above as an unsignalled fourth route rather than
  claiming every route pushes.
- **`docs/TODO.md`**: the edge-invisibility entry shrinks to what it is actually
  for — the record that `edges` writes do not belong in `object_writes` — with
  the route-3 cost and the counter direction removed. A new entry for the
  mark-side route, with the fix shape above.
- **`docs/adr/2026-08-05-a-physical-delete-pushes-its-owner.md`**: its closing
  "Route 3 … is untouched" paragraph, and the `DependenciesDelete` mention in
  the cleared-finalizer ADR's context.
- **`README.md`** (line 734 on the base above): an addition, not a correction — that paragraph
  says only that these calls commit on their own. Note that dropping the last
  live `depends_on` edge to a deleting target now collects it without waiting
  for a sweep. The public signature at line 623 is unchanged.

## Risks

- **Wrong `RowsAffected`** would silently skip the push, permanently and
  invisibly — the same exposure `EdgesAdd`'s stamp already carries, and worth the
  same one-line comment.
- **A drop-and-redeclare loop against a finalizing target** would push per pass.
  It is pathological (declaring a dependency on a dying object), it terminates
  when the row goes, and queue coalescing bounds it.
