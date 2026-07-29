# Declaring a dependency is caller-versioned, and the wake it owes is durable

- **Status:** Accepted — implemented in `sqlite/store.go` (`EdgesAdd`,
  `reconcile_owed`), `reconciler.go` (drain).
- **Date:** 2026-07-27 (recorded retroactively)

## Context

A controller reads target T, decides, then declares the edge. A change to T that
lands in that gap reaches nobody: the waker resolves dependents when its scan passes
the change, and that can happen before the edge exists. The dependent then settles
at its own generation on the stale read, where `ObjectsListUnsettledIDs` cannot see
it.

## Decision: `DependenciesAdd(…, targetResourceVersion)`

The caller passes **the version of T its decision was based on**, and
the store records `fromID` as owing a reconcile pass when **both** halves hold:

1. this call created the edge, **and**
2. T's current `resource_version` has moved past the version passed.

Re-reading T to get that value defeats the guard: a fresher version claims to have
seen changes the decision did not. Pass `0` for "no opinion", which is right when
the edge is declared *before* reading T and so has no gap to close.

### The conjunction is what converges

Either half on its own loops, with nothing to throttle it — the dispatch path has
no already-settled skip, and `workQueue.addLocked` has no rate limiter:

- *target-moved alone* fires on every pass for a caller whose version never
  advances, such as one cached across passes;
- *edge-new alone* fires on every pass for a controller that clears and re-declares
  its dependency set.

Requiring both limits the wake to **once per edge**, which is exactly the window
being closed: every later change reaches an edge the waker can already see.

### A future version is rejected in the store, before the insert

A version above T's current one gets `ErrTargetResourceVersionFuture`. Versions only
move forward, so such a value cannot have come from reading T, and an edge whose
guard can never fire should not be written. The check lives next to the read that
already knows T's version, just as `UpdateStatus` rejects a generation from the
future.

Checking afterwards in `DependenciesAdd` would leave the guarantee up to the caller.
A nested `Within` is a plain `fn(ctx)` with no transaction of its own, so returning
an error unwinds nothing, and a caller that logs and carries on would commit the
edge inside its own transaction.

The check is deliberately partial. It catches a version read from the wrong object,
but not one read from the right object at the wrong time — a stale version looks
just like an old read, and a freshly re-read one looks just like a decision made
this instant.

### Neither half costs a query

`EdgesAdd` already checks that both endpoints exist, so it returns `fromID`'s
`GroupKind` and `toID`'s `resource_version` from that same read. Adding a separate
pre-read to test whether the edge is new is what sank an earlier version of this
guard.

`EdgesAdd` **self-wraps in `Within`**, like the other mutators. Reporting a
`resource_version` that another write could land behind before the edge is inserted
would recreate the very gap this closes, and it would be invisible both to the
result and to the waker. So atomicity is the store's job, not an unstated
requirement on the caller or on SQLite's single writer. (`EdgesAdd` returns the
endpoint metadata to every caller; the owner-edge path in `insertObject` ignores it
and claims version `0`.)

The stamp lands on **`fromID`'s** row, and the pass it buys runs on `fromID`'s kind,
not the caller's — the edge is deliberately cross-kind, and enqueuing a foreign id
onto the caller's reconciler would decode another kind's bytes as this one's `Spec`.
Routing is not the declare path's problem: `ReconcileOwedListIDs` is per-kind, so
each reconciler picks up only its own rows.

## The wake is durable, so a crash can't lose it

The conjunction increments `objects.reconcile_owed`, indivisibly with the edge. That
count *is* the wake — there is no in-memory requeue beside it to race, and a process
that dies the instant after the commit finds the row still owed on restart.

### The stamp is inside `EdgesAdd`, and *before* the insert

The result reports it back as `EdgesAddResult.ReconcileOwedStamped`, so a caller can
see whether both halves held without working it out again.

Running the stamp as a second store call *after* `EdgesAdd` would be wrong. A nested
`Within` unwinds nothing, so a caller that handled `DependenciesAdd`'s error would
commit the edge with no wake — precisely the stranded dependent this guard exists to
prevent. Ordering is the only guarantee available while nesting has no rollback
boundary, and it points the leftover failure the harmless way: a stamp with no edge
is one spurious owed wake that drains back to 0, while an edge with no stamp is
invisible forever.

The stamp's own `WHERE … NOT EXISTS (SELECT 1 FROM edges …)` is the **only** test for
whether the edge is new. It is a probe straight down the edges primary key — which is
the table itself, since `edges` is `WITHOUT ROWID` (see
[edges WITHOUT ROWID](2026-07-26-edges-without-rowid.md)) — so there is no pre-read
and no second version of the same test to fall out of step with it.

### The stamp is not gated on `fromID`'s kind being registered

A client-only dependent never drains its count, and nothing scans it either, since
`ReconcileOwedListIDs` is per-kind and only that kind's reconciler calls it. So the
count is *unread* — which is not the same as free. It is a permanent nonzero column
and index entry, and re-declaring an edge (delete, then add again) with a stale claim
increments it once more, so it can grow. If the kind later gains a controller, the
whole accrued count costs **one** spurious pass, because the reconcile subtracts all
of it at once.

Gating the stamp on registration is not the cheap fix it looks like. The stamp is SQL
inside `EdgesAdd`, and the store cannot know which kinds are registered, so the caller
would have to resolve `fromID`'s kind before every declare — the per-call pre-read
that sank an earlier guard. It would also freeze a fact that changes between runs,
losing the wake outright for a kind that gains a controller later. The shape that
would reclaim these off the hot path is a cross-kind sweeper, the `reconcile_owed`
equivalent of the GC sweeper's `DeletionRequestsList`. It is unbuilt, and in TODO.md.

Leaving it ungated also keeps the policy out of the store. Registration is decided
where the count is *read*, because a kind with no reconciler never lists its own owed
rows. One place decides instead of two, and no beehive logic runs inside a write
transaction holding the single connection.

`TestRefsAddStampFailureLeavesNoEdge` pins the ordering, injecting a stamp failure
with `blockObjectUpdates`' `BEFORE UPDATE ON objects` trigger — `RAISE(ABORT)`
undoes the statement, not the transaction, so the outer caller *can* swallow and
commit, which is the whole point.

`ReconcileOwedIncrement` is deliberately **not on the `Store` interface**. `EdgesAdd`
is the only thing that produces a wake and `ReconcileOwedDecrement` the only thing
that consumes one, so a standalone increment would be API the declare path cannot use
correctly — it could not be made atomic with the edge — and that nothing else uses at
all. Leaving it off makes "the stamp rides `EdgesAdd`" true at compile time rather
than something a test has to police. It still exists on the concrete sqlite store, so
tests can seed a count without staging the whole race, and that is where a future
non-edge producer would hook in. `TestAddDependencyStampRidesRefsAdd` covers the part
that isn't structural: that folding the stamp in really does stamp.

A general fix for the class — SAVEPOINTs making every nested `Within` a real
rollback boundary — is in TODO.md, unbuilt.

### `reconcile_owed` is a count, not a flag

`typedController.reconcile` subtracts, on a successful pass, **the count it loaded**
(`ReconcileOwedDecrement(id, observed)`, floored at 0).

A count rather than a single token is what survives a wake owed *while an earlier one
is already being reconciled*. Increments that land after the load sit above
`observed`, so they survive the subtraction and keep the object owed. A token holding
the same value would have been cleared by that pass, and then lost in a crash.

Subtracting `observed` rather than 1 is the other half. One pass reads the target's
current state, which answers every wake outstanding when it started, and the backstop
queues a row only once because the work queue coalesces. Subtracting 1 would leave a
remainder with nothing to re-queue it — forever, if every periodic driver is off.

Nothing schedules a follow-up, because nothing in beehive schedules anything: a
leftover count keeps the row in `ReconcileOwedListIDs`, which is what the next owed pass
tick reads. A failed subtraction is logged rather than retried for the same reason —
the count stays up for that tick, where retrying would spin against a store that keeps
failing.

### The backstop

`ReconcileOwedListIDs` reads the partial index
`idx_objects_reconcile_owed WHERE reconcile_owed != 0`. An owed wake has nothing to do
with spec convergence — a settled object can still owe one — so it is not folded into
`ObjectsListUnsettledIDs`. It sits beside it in `enqueueOwedPass`, which runs at
startup and on every owed-pass tick, so turning off the startup full pass does not turn
off recovery.

Pinned end-to-end by `TestDependencyRequeueLostAcrossRestart` (one store, two
`Beehive`s, restart under `WithStartupFullPass(false)` + `fullPassInterval=0`) and, for
the concurrent-wake case, `TestReconcileOwedSurvivesConcurrentIncrement`.
