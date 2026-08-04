# A cursor for the stale-dependents pass

- **Status:** Implemented. This document records the mechanism as built. The
  decision and its rationale are in
  [the ADR](../adr/2026-08-03-stale-dependents-cursor.md).
- **Date:** 2026-08-03, revised on implementation.

## 1. Purpose

This document specifies how the stale-dependents pass stops rescanning the whole
dependency graph on every sweep.

The cost before the change tracked the edge count, not the change rate. Measured
on an in-memory store, one kind, one converged sweep:

| Objects | `depends_on` edges | One sweep |
| --- | --- | --- |
| 1,000 | 2,000 | 1.5 ms |
| 10,000 | 20,000 | 17 ms |
| 50,000 | 250,000 | 190 ms |

A converged sweep is the worst case. `LIMIT` cannot stop the scan early when
nothing matches, so a healthy system paid the full scan in one query.

## 2. What a cursor puts at risk

A cursor limits the sweep to targets written since the last one. It therefore
never re-examines a dependent that is already stale and whose target has gone
quiet. There are four routes to that state.

1. The pass found the dependent and the process stopped before the reconcile
   ran. The enqueue was in memory. Nothing failed, so nothing was recorded.
2. The reconcile succeeded and the watermark write failed. `reconciler.go` wrote
   the watermark independently and logged the error, on the stated ground that
   the pass would re-derive it.
3. `EdgesAdd` cleared the watermark for a new edge whose target is quiet. This
   one was already covered, because `EdgesAdd` stamps `reconcile_owed`.
4. The process was killed between a reconcile's owed decrement and its watermark
   write. These are two statements, not one transaction.

**Route 4 was not in the proposed version of this document, and it is the one
that shapes the design.** No durable record can name that dependent: the count
is gone, the object is settled by its status write, and the target may never be
written again. The decrement that erased the record was correct when it ran.

## 3. Decision

Add the cursor, and cover each route with a named mechanism. Three mechanisms do
the work, and they are not interchangeable.

| Route | Covered by |
| --- | --- |
| 1, 4 | The cursor is process-local, so every process re-derives once (§5.2) |
| 2 | The reconciler stamps `reconcile_owed` when the watermark write fails (§4.3) |
| 3 | `EdgesAdd`'s existing stamp — unchanged |

The proposed version of this document claimed that the pass's own stamp (§4.2)
is what makes the cursor sound. **That is not correct**, and the ADR states why
the stamp is kept regardless. It is not load-bearing against any of the four
routes.

## 4. Durable findings

### 4.1 A second and third `reconcile_owed` producer

`storeapi.Store` recorded that "there is deliberately no standalone
`reconcile_owed` increment", and that one should be added "only when a producer
other than `EdgesAdd` exists". Two such producers now exist, so that note is
gone. As landed:

```go
// ReconcileOwedStamp increments reconcile_owed for each ref, so a finding
// outlives the in-memory queue. An id that is gone is skipped, not
// reported. Empty refs writes nothing. Bumps no resource_version.
//
// Not kind-scoped, unlike ReconcileOwedDecrement: the refs come from the
// store's own listing, which spans every registered kind in one page.
ReconcileOwedStamp(ctx context.Context, refs []ObjectRef) error
```

**It takes `[]ObjectRef` and is not kind-scoped.** The listing pages across
every registered kind at once, so a page routinely spans kinds. A `gk`-scoped
`[]ObjectID` would force the caller to group each page first, and would cost one
statement for each page and kind.

Dropping the `gk` scope costs nothing here. `ErrWrongKind` guards
`ControllerClient`, where the caller is untrusted controller code. These
producers are the driver and the reconciler, and the refs come from the store's
own listing.

The SQLite form is one `UPDATE … WHERE id IN (…)`. A missing id matches no row,
which is how a dependent collected mid-sweep is skipped. Duplicate ids inside
one page collapse to a single increment, because `IN` matches the row once.

### 4.2 The pass stamps and still enqueues

`staleDependents.sweep` stamps each page before it enqueues that page. The
enqueue stays.

The stamp must commit before the enqueue is dispatched. A stamp that lands
before a crash costs one spare reconcile, which is idempotent. Neither order
strands a dependent.

The stamp is defence rather than a guarantee. With the current `workQueue` it
cannot change any outcome: `add` marks an in-flight id dirty rather than
dropping it, `done` re-queues it, and the retry ladder has no maximum-retry
rule, so the only way to lose a finding is to stop the process — which §5.2
covers. It is kept because a finding should be recorded before it is queued, and
because a persisted cursor would need it. See the ADR.

### 4.3 The swallowed watermark error stamps

`reconciler.go` logged and continued when `DependencyWatermarksSet` failed, with
the message "the stale-dependents pass will re-derive it".

**That sentence stopped being true when the cursor landed.** A cursor-bound pass
re-derives nothing for a target that has since gone quiet, inside a process that
keeps running. The handler now stamps `reconcile_owed` for the object.

This was called the sharpest edge in the change, and it was: the comment was
correct before, became wrong on the day the cursor landed, and no test failed
when it flipped. `TestReconcileStampsOwedWhenTheWatermarkWriteFails` pins it.

One thing this document did not anticipate: **the stamp is skipped when
`ctx.Err() != nil`.** A cancelled watermark write is a shutdown rather than a
lost pass. The stamp would fail the same way, and reporting it would fault every
clean stop.

The sibling path needed nothing. `ReconcileOwedDecrement` is already gated on
`reconcileErr == nil`, so a failed reconcile already leaves the count standing.

## 5. The cursor

### 5.1 The query

The unbounded form, `DependentsListStale`, drove from `edges` and compared
`objects.resource_version` against `dependency_watermarks.reconciled_against`.
Two columns in two rows, which no index can serve. **It is removed**; see §6.

`DependentsListStaleSince` lists targets whose `resource_version` is above the
cursor, then resolves their dependents through `edges`, then filters on the
watermark as before. Cost becomes the change rate.

```sql
SELECT t.resource_version, t.id, e.from_id, d."group", d.kind
  FROM objects t
  CROSS JOIN edges e ON e.to_id = t.id AND e.relation = 'depends_on'
  CROSS JOIN objects d ON d.id = e.from_id
  LEFT JOIN dependency_watermarks c ON c.object_id = e.from_id
 WHERE (t.resource_version, t.id, e.from_id) > (?, ?, ?)
   AND t.resource_version <= ?
   ...
 ORDER BY t.resource_version, t.id, e.from_id
 LIMIT ?
```

Two details of the landed query were not in the proposal.

- **The `CROSS JOIN`s pin the join order**: targets, then incoming edges, then
  dependents. Without them the planner reads the whole graph and the cursor
  saves nothing. `TestDependentsListStaleSinceDrivesFromTheVersionIndex` holds
  that.
- **There is no `GROUP BY`**, unlike the unbounded form. A row is one
  `(target, dependent)` pair, because the resume position needs both. A
  dependent therefore appears once for each moved target it depends on. Stamping
  and enqueuing are both idempotent, so a duplicate costs a pass rather than
  correctness.

Two properties make this cheaper here than for the dependency waker:

- It scans `objects.resource_version`, which `idx_objects_rv` already serves. It
  does not scan `object_writes`, so **there is no retention horizon to fall
  off**. The waker can lose entries to the 24 hour trim. This cursor cannot,
  because the rows persist.
- A target that no longer exists cannot strand a dependent. `edges.to_id` is
  `ON DELETE RESTRICT`, so a target outlives its edges.

### 5.2 The cursor is **not** persisted

**This reverses the proposal, which specified `driver_cursors` through
`DriverCursorer`.** The cursor lives in the `staleDependents` struct, starts at 0
in every process, and is never written down.

Route 4 of §2 is why. A persisted cursor would sit above the quiet target, the
owed pass would see no stamp, and the unsettled pass would see a settled row.
That dependent would never reconcile again. No stamp can repair it, because the
decrement that removed the stamp was correct.

Starting at 0 makes the first sweep of each process re-derive the whole graph
once, which finds it. This needs no case analysis: routes 1 and 4 are produced
only by a crash, and a crash is a restart.

The alternative — enumerating every enqueue path that could lose a wake — is not
worth trusting. A dependent woken by the dependency waker carries no stamp at
all. Nor does one enqueued by its own spec write, by `Client.Requeue`, or by a
`RequeueAfter`. Any future enqueue path would join them silently.

The cost is one full scan for each process: 190 ms at 250,000 edges, which this
pass paid every 60 s before the change.

Two consequences follow, and both are simplifications:

- **No scoping.** A persisted cursor would have to be keyed by the registered
  kind set, because a cursor earned while a kind had no controller says nothing
  about that kind's dependents.
- **No repair path.** A persisted cursor would need a way to move backwards,
  because a monotone write can never replace a position above this database's
  sequence.

### 5.3 A failed sweep holds the cursor

A failed page abandons the sweep and holds the cursor, so the next tick reads the
same range again. This applies to both the listing and the stamp.

Holding it re-stamps the pages that already succeeded. That is safe:
`ReconcileOwedDecrement` subtracts the whole count observed at load, so an
inflated count drains in one pass, and a double stamp costs one reconcile, which
is idempotent.

`TestStaleDependentsSweepWarnsAndRetriesOnListFailure` recorded the old contract
with the comment "there is no cursor to hold and nothing was drained". That
sentence is what the change invalidated, so the test was renamed to
`TestStaleDependentsSweepHoldsTheCursorOnListFailure` and now pins the new one.
`TestStaleDependentsSweepHoldsTheCursorOnStampFailure` covers the other half.

### 5.4 The mark

**This section is new. The proposal did not specify it, and the pass is not
correct without it.**

Each sweep reads a mark before it scans, and scans no higher: the query's
`t.resource_version <= ?` bound. The mark, not the highest row the scan
returned, is what the cursor becomes when the sweep completes.

The mark does two things.

- **It keeps the sweep finite.** A store taking writes faster than the caller
  pages would never reach a short page, so the sweep would never end and the
  cursor would never move.
- **It keeps a concurrent write from being skipped.** A target written while the
  sweep runs sits above the mark, so the next sweep finds it. Taking the highest
  target the scan returned would skip exactly those targets.

The mark comes from `ResourceVersionsMaxIssued`, which reads
`resource_version_seq`. **It must not come from `ObjectWritesMaxVersionAll`:**
retention lowers that value, and an idle store past its retention window reads 0.
A mark that falls compares wrongly against a stored position.

Reading the sequence also gives the idle skip. A tick where the mark equals the
cursor does no listing at all, because no target can have moved. The sequence
moves for an event write too, so this is a "did anything change" answer rather
than a log position. A sweep triggered by an event write finds nothing and costs
one empty indexed range query.

### 5.5 The position is a triple

Paging in the unbounded form is `afterID` over `edges.from_id` with a fixed
predicate. Driving from target `resource_version` cannot page that way: a target
with more dependents than `staleDependentsPageCap` (256) would either loop on its
own first page forever, or have the rest of its fan-out skipped when the cursor
advanced past it.

The paging key is therefore `storeapi.StalePos` —
`(TargetVersion, TargetID, DependentID)` — and a page resumes inside a target's
fan-out rather than at a target boundary. Both halves are index-served:
`idx_objects_rv` orders targets by `(resource_version, id)`, and `idx_edges_to`
is `(to_id, relation)` on a `WITHOUT ROWID` table, so its entries carry `from_id`
in order. The page cap is a soft bound.

**Only the first component outlives the sweep.** The other two are paging state.
That was specified as what keeps the cursor storable in one `int64`, and it
survives the reversal in §5.2 for a simpler reason: there is nothing to store.

**A completed sweep resumes at the next version, from the start**, not at
`(mark, 0, 0)`. This is `staleResumeAt`, and the proposal did not consider it.
Ids are positive, so `(mark, 0, 0)` still matches every target at the consumed
version. A target sitting exactly there whose dependents have not reconciled yet
would have its whole fan-out listed and stamped again on every sweep.
`TestStaleDependentsSweepDoesNotRestampAConsumedVersion` pins it.

Tuple paging is for resuming inside one sweep, where the position is a row the
scan actually returned. It is not for resuming between sweeps.

## 6. The pass and the waker have converged

**This is still open, and it must be decided rather than discovered.**

Listing targets above a cursor and resolving their dependents through `edges`
**is the dependency waker**. There are now two drivers doing one thing, and they
differ in two ways: this one stamps what it finds, and its cursor dies with the
process.

Merging them is the recommendation, and it is what would make the pass's stamp
(§4.2) load-bearing, because a merged driver would inherit the waker's durable
cursor. That merge must also handle route 4, which the durable cursor reopens.
The fix recorded in `docs/TODO.md` — folding the owed decrement and the watermark
write into one transaction — closes route 4 and is the natural precondition.

`DependentsListStale`, the unbounded form, was removed rather than kept for the
tests. It had no production caller after the change, and a second staleness
listing on `storeapi.Store` is the kind of thing a later reader restores a caller
to. `DependentsListStaleSince` is the only one now, and it carries the contract.

Its tests were ported rather than deleted. `staleIDs` — the "who is stale right
now" oracle they share — reads the cursor form from the beginning, unbounded
above, and dedupes. Two tests only restated the old shape and did not survive:
paging by dependent id, which
`TestDependentsListStaleSincePagesInsideAFanOut` replaces, and one row per
dependent, which `TestDependentsListStaleSinceReturnsAPairPerMovedTarget` now
inverts.

## 7. What is given up

The full scan was immune to a lost or wrong cursor by construction. Two of the
three costs the proposal listed still apply.

1. ~~It reintroduces a durable cursor that a future push conversion would delete
   along with the waker.~~ **Not applicable.** The cursor is not durable (§5.2).
2. It turns a read-only pass into one that writes once for each stale dependent,
   on the single connection.
3. One cursor shared by two processes on one database breaks, because each would
   skip work the other's cursor claimed. This is bounded by the single-writer,
   single-process constraint `driver_cursors` already documents. Note that the
   constraint still applies even though nothing is persisted: two processes on
   one database each hold their own cursor, and neither sees the other's work.

## 8. Tests

Store level, in `sqlite/store_test.go`:

- `TestDependentsListStaleSincePagesInsideAFanOut` — §5.5.
- `TestDependentsListStaleSinceStopsAtTheMark` — §5.4.
- `TestDependentsListStaleSinceDrivesFromTheVersionIndex` — the cost assertion of
  §5.1. This is the one that fails if the `CROSS JOIN`s are removed.
- `TestDependentsListStaleSinceSkipsConvergedAndSpentPositions`,
  `TestDependentsListStaleSinceReturnsAPairPerMovedTarget` — §5.1.
- `TestDependentsListStaleSinceIsEmptyWithoutKindsOrLimit`,
  `TestDependentsListStaleSinceQueryError`.
- The semantics ported from the removed unbounded form:
  `TestDependentsListStaleSinceFindsMovedTargets`,
  `TestDependentsListStaleSinceTreatsMissingWatermarkAsStale`,
  `TestDependentsListStaleSinceExcludesSelfEdges`,
  `TestDependentsListStaleSinceFiltersByKind`,
  `TestDependentsListStaleSinceFindsDependentsOfUnregisteredTargets`.

Pass level, in `waker_test.go`:

- `TestStaleDependentsPassEnqueuesStaleDependents` closes every other route to
  the dependent — the waker off, the full pass off, the dependent settled — so it
  asserts the pass finds a dependent nobody told it about. That is the property
  the change put at risk. It was kept, not deleted.
- `TestStaleDependentsSweepStampsWhatItFinds` and
  `TestStaleDependentsSweepLeavesADurableFinding` — §4.2.
- `TestStaleDependentsSweepAdvancesToThePreScanMark` and
  `TestStaleDependentsSweepSkipsAQuietStore` — §5.4.
- `TestStaleDependentsSweepStartsEveryProcessAtTheBeginning` and
  `TestStaleDependentsSweepRepairsALostWatermarkAfterRestart` — §5.2. The second
  is the route 4 test: it drops the watermark, restarts, and asserts the
  dependent reconciles.
- `TestStaleDependentsSweepDoesNotRestampAConsumedVersion` — §5.5.
- `TestStaleDependentsSweepHoldsTheCursorOnListFailure` and
  `TestStaleDependentsSweepHoldsTheCursorOnStampFailure` — §5.3.
- `TestStaleDependentsSweepWarnsWhenTheMarkReadFails`,
  `TestStaleDependentsSweepIsQuietOnShutdown`.

Reconciler level: `TestReconcileStampsOwedWhenTheWatermarkWriteFails` — §4.3.
This was the case with no test before the change.

The benchmark was rebuilt as `BenchmarkStaleDependentsSweep`
(`waker_bench_test.go`), because the measurements in §1 came from a throwaway
harness. It does not reproduce that table. It measures the claim the table
motivated, at 1,000 and 10,000 objects, with three cases: `cold-start` holds the
cursor at 0 and is the uncursored reference, `one-target-moved` should track the
change rather than the graph, and `quiet` should be one
`ResourceVersionsMaxIssued` and no listing at all.

## 9. The interval arithmetic, corrected

`docs/TODO.md` computed the deferral trigger against "a five-minute interval".
**That figure was wrong**, and it has been corrected on merge.
`defaultStaleDependentsInterval` is 60 s (`beehive.go`), and the pass cannot be
disabled.

At 250,000 edges the sweep was 190 ms every 60 s, which is **0.32%** of one
connection, not 0.06%. At 2.5 million edges it would be near 2 s a sweep, or
about 3.3% of one connection, held on the single connection every minute.

The "roughly ten times the current graph" trigger was therefore five times too
lax. The corrected arithmetic is what moved this from deferred to done.

## 10. The idle gate is not needed

This change **removed the need for the idle gate**, and the gate's own
specification (`edges-epoch-in-memory.md`) was deleted with the merge rather than
restored.

After the change an idle sweep is one read of `resource_version_seq` (§5.4), so
there is no 190 ms to skip.

The proposal gave a second reason — "a restart resumes from the cursor rather
than rescanning, so there is no startup sweep to skip either". **That reason is
false as built.** Every process re-derives the whole graph once (§5.2), so the
startup sweep is exactly what a gate could not skip anyway.

The edge-set epoch lost its main consumer. It has one left — a deletion
unblocked by `DependenciesDelete` waits for the next GC tick, case 9 in
[`reconcile-triggers.md`](../reconcile-triggers.md) — and that is recorded in
[`TODO.md`](../TODO.md).
