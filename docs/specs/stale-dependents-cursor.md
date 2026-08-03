# A cursor for the stale-dependents pass, made sound by durable findings

- **Status:** Proposed — not implemented.
- **Date:** 2026-08-03

## 1. Purpose

This document specifies how the stale-dependents pass stops rescanning the
whole dependency graph on every sweep.

The cost today tracks the edge count, not the change rate. Measured on an
in-memory store, one kind, one converged sweep:

| Objects | `depends_on` edges | One sweep |
| --- | --- | --- |
| 1,000 | 2,000 | 1.5 ms |
| 10,000 | 20,000 | 17 ms |
| 50,000 | 250,000 | 190 ms |

A converged sweep is the worst case. `LIMIT` cannot stop the scan early when
nothing matches, so a healthy system pays the full scan in one query.

## 2. Why there is no cursor today

A cursor would limit the sweep to targets written since the last one. It would
then never re-examine a dependent that is already stale and whose target has
gone quiet. There are three ways to reach that state:

1. The pass found the dependent and the process stopped before the reconcile
   ran. The enqueue was in memory. Nothing failed, so nothing was recorded.
2. The reconcile succeeded and the watermark write failed. `reconciler.go`
   writes it independently and logs the error, on the stated ground that "the
   stale-dependents pass will re-derive it".
3. `EdgesAdd` cleared the watermark for a new edge whose target is quiet. This
   one is already covered, because `EdgesAdd` stamps `reconcile_owed`.

Thus the pass must re-derive from current state. That is what the full scan
buys, and it is why the scan cannot simply be bounded.

## 3. Decision

**Make every finding durable first. Add the cursor second.**

The pass stamps `reconcile_owed` instead of relying on an in-memory enqueue.
The owed pass then drains the stamp on an already-empty partial index. The
sweep may advance a cursor, because it never has to re-find anything.

The two phases must land in this order. A cursor added before the stamps
strands the dependents of section 2, silently and permanently.

## 4. Phase 1: durable findings

### 4.1 A second `reconcile_owed` producer

`storeapi.Store` records today that "there is deliberately no standalone
`reconcile_owed` increment", and that one should be added "only when a producer
other than `EdgesAdd` exists". This phase is that producer.

```go
// ReconcileOwedStamp increments reconcile_owed for each ref, so a finding
// survives a restart. A missing id is skipped, not an error — a dependent
// collected mid-sweep is normal.
ReconcileOwedStamp(ctx context.Context, refs []ObjectRef) error
```

**It takes `[]ObjectRef` and is not kind-scoped.** `DependentsListStale` pages
across every registered kind at once and returns `[]ObjectRef`, so a page
routinely spans kinds. A `gk`-scoped `[]ObjectID` would force the caller to
group each page first and would cost one statement per page and kind, which
defeats the reason for taking a slice.

Dropping the `gk` scope costs nothing here. `ErrWrongKind` guards
`ControllerClient`, where the caller is untrusted controller code. This
producer is the driver itself, and the refs come from the store's own listing.

### 4.2 The pass stamps and still enqueues

`staleDependentsSweep` stamps each page before it enqueues it. The enqueue
stays. This mirrors `DependenciesAdd`: the stamp is the guarantee, the enqueue
is the prompt half.

The stamp must commit before the enqueue is dispatched. A stamp that lands
after a crash costs one spurious reconcile, which is idempotent. An enqueue
serviced before its stamp commits can leave the count up with nothing owed,
which the decrement then clears on the next pass. Neither is a strand.

### 4.3 The swallowed watermark error must stamp

`reconciler.go` logs and continues when `DependencyWatermarksSet` fails, with
the message "failed to record the dependency watermark; the stale-dependents
pass will re-derive it".

**That sentence stops being true in phase 2.** A cursor-bound pass re-derives
nothing for a quiet target. The handler must stamp `reconcile_owed` for the
object instead of only logging.

This is the sharpest edge in the whole change. The comment is correct today,
becomes wrong on the day the cursor lands, and no test fails when it flips.

The sibling path needs nothing: `ReconcileOwedDecrement` is already gated on
`reconcileErr == nil`, so a failed reconcile already leaves the count up.

## 5. Phase 2: the cursor

### 5.1 The query changes shape

Today `DependentsListStale` drives from `edges` and compares
`objects.resource_version` against `dependency_watermarks.reconciled_against`.
Two columns in two rows, which no index can serve.

The cursor form lists targets whose `resource_version` is above the cursor,
then resolves their dependents through `edges`. Cost becomes the change rate.

Two properties make this cheaper here than for the dependency waker:

- It scans `objects.resource_version`, which `idx_objects_rv` already serves.
  It does not scan `object_writes`, so **there is no retention horizon to fall
  off**. The waker can lose entries to the 24 hour trim. This cursor cannot,
  because the rows persist.
- A target that no longer exists cannot strand a dependent. `edges.to_id` is
  `ON DELETE RESTRICT`, so a target outlives its edges.

### 5.2 The cursor is persisted

`driver_cursors`, through the existing `DriverCursorer`. A new name, alongside
`dependency_waker`. Nothing new is needed in the store or the schema.

**Only the target `resource_version` is durable.** `DriverCursorer` stores one
`int64` per name with set-if-greater semantics, and that is all this needs. The
rest of the composite key in section 5.4 is in-memory paging state for the
duration of one sweep.

### 5.3 A failed sweep must hold the cursor

Today a failed page abandons the sweep and the next tick re-derives the same
set. `TestStaleDependentsSweepWarnsAndRetriesOnListFailure` records that with
the comment "there is no cursor to hold and nothing was drained".

That comment is invalidated by this change. Holding the cursor on a failed
sweep becomes a requirement, and that test is where it must be pinned.

Holding it re-stamps the pages that already succeeded on the next tick. That is
safe, and the doc must say why rather than leave a reader to find it:
`ReconcileOwedDecrement` subtracts the whole count observed at load
(`reconciler.go`), so an inflated count drains in one pass. A double stamp costs
one reconcile, which is idempotent.

### 5.4 What the cursor is, exactly

Three properties, each of which is a silent strand if it is got wrong.

**Record the version read before the scan, not the highest target the scan
returned.** This is the same rule as step 5 of the in-memory gate. Recording
the returned maximum loses every write that lands during the sweep, because
those writes are above the cursor the sweep started from but at or below the
maximum it saw.

**The cursor is composite, and advances only past a fully emitted target.**
Paging today is `afterID` over `edges.from_id` with a fixed predicate. Driving
from target `resource_version` cannot page that way: a target with more
dependents than `staleDependentsPageCap` (256) would either loop on its own
first page forever, or have the rest of its fan-out skipped when the cursor
advanced past it.

Thus the paging key is `(target resource_version, target id, dependent id)`,
and a page resumes inside a target's fan-out rather than at a target boundary.
Both halves are index-served: `idx_objects_rv` orders targets by
`(resource_version, id)`, and `idx_edges_to` is `(to_id, relation)` on a
`WITHOUT ROWID` table, so its entries carry `from_id` in order. The page cap
becomes a soft bound.

**Only the first component is durable**, which is what keeps section 5.2 true.
`(target id, dependent id)` lives in memory for one sweep and never reaches
`driver_cursors`.

Nothing is lost by that, because the durable value never advances mid-sweep.
The version recorded is the one read before the scan, and a failed sweep holds
the cursor entirely, so the persisted value moves only when a sweep completes.
A restart mid-sweep therefore resumes from the last completed sweep's version
and re-derives the rest, which is idempotent — the re-stamping argument of
section 5.3 covers it.

Do not encode the tuple into the single `int64` to make it durable. It would
buy nothing, and packing breaks set-if-greater: two positions can compare in
the wrong order once the low components are folded in.

**The initial cursor is 0, not the current maximum.** A cursor seeded from
"now" would skip the entire backlog that exists at the moment phase 2 is
deployed — every dependent left stale by a lost enqueue before phase 1 landed.
Seeding at 0 makes the first sweep after deployment re-derive it once. This is
the opposite of the dependency waker, which seeds from
`ObjectWritesMaxVersionAll` because its losses are bounded by this pass.

## 6. Phase 3: the pass and the waker converge

**This must be decided, not discovered.**

Listing targets above a cursor and resolving their dependents through `edges`
**is the dependency waker**, with a durable stamp added. After phase 2 there
are two drivers doing one thing.

There are two ways to end:

- **Merge them.** One driver, one cursor, one stamp. The waker's own doc says
  it is "an optimisation, not a guarantee", and the guarantee it defers to is
  the pass that phase 2 just turned into it. The stamp removes the
  distinction.
- **Keep both.** Accept a duplicated scan and two cursors over the same change
  set.

Merging is the recommendation. Keeping both leaves two mechanisms whose
difference nobody can state after this change.

## 7. What is given up

The full scan is immune to a lost or wrong cursor by construction. After this
change it is not, and three costs follow. All three are in `docs/TODO.md` and
are the reason the item waits:

1. It reintroduces the durable cursor that a future push conversion would
   delete along with the waker.
2. It turns a read-only pass into one that writes once for each stale
   dependent, on the single connection.
3. One cursor shared by two processes on one database breaks, because each
   would skip work the other's cursor claimed. The full scan is immune to that
   by construction.

Cost 3 is bounded by the single-writer, single-process constraint that
`driver_cursors` already documents.

## 8. Tests

- `TestStaleDependentsPassEnqueuesStaleDependents` closes every other route to
  the dependent — the waker off, the full pass off, the dependent settled — so
  it asserts the pass finds a dependent nobody told it about. That is the
  property this change puts at risk. **Re-express it against the durable stamp.
  Do not delete it.** The property still holds; it is observed in
  `reconcile_owed` rather than in the queue.
- `TestStaleDependentsSweepWarnsAndRetriesOnListFailure` must pin "a failed
  sweep holds the cursor" (section 5.3).
- A reconcile whose `DependencyWatermarksSet` fails leaves `reconcile_owed`
  stamped (section 4.3). This is the case with no test today.
- A dependent found by a sweep is reconciled after a restart that loses the
  in-memory queue.
- A target with more dependents than `staleDependentsPageCap` has its whole
  fan-out stamped, across a page boundary (section 5.4).
- A write that lands during a sweep is found by the next sweep (section 5.4).
- Rebuild the benchmark before changing anything. The measurements in section 1
  came from a throwaway harness.

Four test doubles implement `DependentsListStale` and all are touched: `fakeStore`
(`testutils_test.go`), `staleListErrorStore` and `listProbeStore`
(`waker_test.go`, `testutils_test.go`), and the direct call sites in
`reconciler_test.go`. `fakeStore` also needs `ReconcileOwedStamp`. The listing's
signature changes, so none of these fail to compile silently.

## 9. When

`docs/TODO.md` computes this trigger against "a five-minute interval". **That
figure is wrong.** `defaultStaleDependentsInterval` is 60 s (`beehive.go`), and
the pass cannot be disabled. Correct both the TODO and the numbers below on
merge.

At 250,000 edges the sweep is 190 ms every 60 s, which is **0.32%** of one
connection, not 0.06%. At 2.5 million edges it is near 2 s a sweep, or about
3.3% of one connection, held on the single connection every minute.

Thus the "roughly ten times the current graph" trigger is five times too lax
and must be re-derived. On the corrected arithmetic it fires several times
sooner than the TODO implies.

The interval is the other lever, and it moves the wrong way: 60 s is already
the shipped value, so a reduction raises the cost further. If 60 s was itself
raised from something lower, record what it was raised from — the cost is per
sweep, and that history is what says whether the interval can absorb any of
this.

## 10. Relationship to the idle gate

This change **removes the need for the idle gate**, which should not outlive
it.

After phase 2 an idle sweep is one empty indexed range query, so there is no
190 ms to skip. A restart resumes from the cursor rather than rescanning, so
there is no startup sweep to skip either. The edge-set epoch loses its only
consumer, and case 3 of section 2 is already covered by `EdgesAdd`'s stamp.

[`edges-epoch-in-memory.md`](edges-epoch-in-memory.md) is written to be removed
on that day: no schema, no new required `Store` member, and nothing that
survives the delete. Every piece of it is unexported or a member of an optional
capability, so it can be implemented without knowing when this document is
scheduled. A durable gate was specified and rejected for the opposite reason —
see section 2 of that document.
