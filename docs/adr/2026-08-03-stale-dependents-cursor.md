# The stale-dependents pass scans from a cursor, and stamps every finding to make that sound

- **Status:** Accepted — implemented in `waker.go` (`staleDependents`),
  `sqlite/store.go` (`DependentsListStaleSince`, `ReconcileOwedStamp`,
  `ResourceVersionsMaxIssued`), `reconciler.go`.
- **Date:** 2026-08-03

## Context

The stale-dependents pass is the correctness backstop behind the dependency
waker. It cannot be disabled. Before this change it re-derived staleness from
current state on every sweep, over the whole dependency graph.

Its cost tracked the edge count, not the change rate. Measured on an in-memory
store, one kind, one converged sweep:

| Objects | `depends_on` edges | One sweep |
| --- | --- | --- |
| 1,000 | 2,000 | 1.5 ms |
| 10,000 | 20,000 | 17 ms |
| 50,000 | 250,000 | 190 ms |

A converged sweep is the worst case. `LIMIT` cannot stop the scan early when
nothing matches, so a healthy system pays the full scan every time.

`docs/TODO.md` recorded this cost against "a five-minute interval", which put it
at 0.06% of one connection. That figure was wrong.
`defaultStaleDependentsInterval` is 60 s, so the true cost was **0.32%**, five
times higher, and the deferral rested on it.

### Why the pass could not simply take a cursor

A cursor bounds the scan to targets written since the last sweep. It therefore
never re-examines a dependent that is already stale and whose target has gone
quiet. Three routes reach that state:

1. The pass found the dependent and the process stopped before the reconcile
   ran. The enqueue was in memory. Nothing failed, so nothing was recorded.
2. The reconcile succeeded and the watermark write failed. `reconciler.go`
   logged the error and continued, on the stated ground that the stale pass
   would re-derive it.
3. `EdgesAdd` cleared the watermark for a new edge whose target is quiet. This
   route was already covered by `EdgesAdd`'s own `reconcile_owed` stamp.

The full scan is what covered routes 1 and 2. A cursor removes that cover.

## Decision

**Make every finding durable first. Bound the scan second.** The order is the
whole of the argument. A cursor added before the stamps strands the dependents
of routes 1 and 2, silently and permanently.

### Durable findings

`ReconcileOwedStamp` increments `reconcile_owed` for a page of refs in one
statement. The stale pass stamps each page before it enqueues it, so the stamp
is the guarantee and the enqueue is the prompt half. A crash between the two
costs a spare reconcile, which is idempotent.

`reconciler.go` now stamps when `DependencyWatermarksSet` fails, rather than
relying on re-derivation. Route 2 is closed by that stamp alone.

Route 3 needed nothing. `ReconcileOwedDecrement` needed nothing either: it is
already gated on `reconcileErr == nil`, so a failed reconcile already leaves the
count standing.

### The bounded scan

`DependentsListStaleSince` lists the dependents of targets written above a
cursor, then filters on the watermark as before. It is ordered by
`(target resource_version, target id, dependent id)` and returns the position of
its last row.

The position is a triple because a target with more dependents than one page
must resume **inside** its own fan-out. Cutting at a target boundary would drop
the rest of it. Only the first component is durable; the other two are paging
state for one sweep.

The `CROSS JOIN`s pin the join order. Without them the planner reads the whole
graph and the cursor buys nothing.
`TestDependentsListStaleSinceDrivesFromTheVersionIndex` holds that.

### The cursor's value

The pass records the version read **before** the scan, never the highest target
the scan returned. A target written during the sweep sits above the recorded
value, so the next sweep still finds it.

That value comes from `ResourceVersionsMaxIssued`, which reads
`resource_version_seq`. It must not come from `ObjectWritesMaxVersionAll`:
retention lowers that one, and an idle store past its retention window reads 0.
A falling cursor compares wrongly against a stored position.

A completed sweep consumed its mark, so the next one resumes at the **next**
version, from the start. Resuming at `(mark, 0, 0)` would not do: ids are
positive, so that position still matches every target at the consumed version,
and a target sitting exactly there whose dependents have not reconciled yet
would have its whole fan-out listed and stamped again on every sweep. Tuple
paging is for resuming inside one sweep, where the position is a row the scan
actually returned.

The cursor moves only when a sweep reaches the end. A failed page abandons the
sweep and holds it, so the next tick reads the same range again. Re-reading is
free: the stamp accumulates and `ReconcileOwedDecrement` subtracts the whole
count observed at load.

A tick where nothing has been issued since the last sweep skips the listing
entirely, because no target can have moved.

### The cursor is process-local

**It is deliberately not persisted, and every process re-derives once.**

A reconcile clears the owed count in one statement and records its watermark in
another. A process killed between them leaves a dependent stale with nothing
durable naming it: the count is gone, the object is settled by its status write,
and its target may never be written again. The owed pass sees no stamp, the
unsettled pass sees a settled row, and a persisted cursor would already sit
above the quiet target. That dependent would never reconcile again.

The unbounded scan re-derived it within 60 s. Re-deriving once per process is
the smallest thing that keeps the guarantee, and it needs no case analysis: this
strand can only be produced by a crash, and a crash is a restart.

Two other routes reach the same state, which is why the enumeration is not worth
trusting. A dependent woken by the dependency waker carries no stamp at all, so
there is nothing to leave standing. So does one enqueued by its own spec write,
or by `Client.Requeue`, or by a `RequeueAfter`. Any future enqueue path would
join them silently.

The cost is one full scan per process — 190 ms at 250,000 edges, which this pass
paid every 60 s before this change. The per-sweep win is untouched.

## Consequences

- **`reconcile_owed` has three producers, where the contract said it should have
  one.** `storeapi.Store` carried the note "there is deliberately no standalone
  `reconcile_owed` increment… add one only when a producer other than `EdgesAdd`
  exists." This record is that condition being met, twice: the pass and the
  reconciler's watermark fallback.
- **A live process that loses both writes is still exposed.** If
  `DependencyWatermarksSet` fails and the repair stamp fails with it, that
  dependent stays stale until the process restarts — the re-derivation above
  repairs a crash, not a running process that keeps running. Folding the owed
  decrement and the watermark set into one store transaction closes it, so a
  failure leaves `reconcile_owed` standing by construction. Recorded in
  `docs/TODO.md`, not done here.
- **The reconciler's fallback is best-effort against the store that just
  failed.** This is the one place the change trades a derived guarantee for a
  durable write that might not land. The store cannot own the fallback — a
  compensating write inside `DependencyWatermarksSet` fails for the same reason
  the watermark write did — so the caller is the right level. But the two calls
  share one connection and usually fail together, and unlike every other
  bookkeeping failure in that function, this one is unrecoverable if the second
  write also fails. The deeper fix, not taken here, is to fold the owed
  decrement and the watermark set into one store transaction, so a failure
  leaves `reconcile_owed` standing by construction.
- **The pass and the dependency waker are now the same mechanism.** Both scan
  from a persisted cursor and wake dependents. The waker's own doc calls it "an
  optimisation, not a guarantee", and the guarantee it defers to is this pass —
  which now differs from it only by a durable stamp. Merging them is the
  expected end state and is deliberately not done here.
- **`DependentsListStale`, the unbounded form, has no production caller.** It
  survives for the tests that assert against a full re-derivation. Removing it
  belongs with the merge above.
- **A process-local cursor needs no scoping and no repair path.** A persisted
  one would: it would have to be keyed by the registered kind set, because a
  cursor earned while a kind had no controller says nothing about that kind's
  dependents; and it would need a way to move backwards, because a position
  above this database's sequence can never be replaced by a monotone write. Both
  disappear when the cursor starts at 0 in every process.
- **A cursor shared by two processes on one database breaks**, because each
  would skip work the other's cursor claimed. The full scan was immune to that
  by construction. This is bounded by the single-writer, single-process
  constraint `driver_cursors` already documents.
- **An idle sweep is now one read.** That removes the case for an idle gate over
  this pass. A gate keyed on an edge-set counter was specified and is not
  needed: after this change there is no full scan to skip, and a restart resumes
  from the cursor rather than re-deriving.
