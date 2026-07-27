# Declaring a dependency is caller-versioned, and the wake has a durable twin

- **Status:** Accepted — implemented in `controller.go` (policy), `sqlite/store.go`
  (`EdgesAdd`, `pending_wake`), `reconciler.go` (drain).
- **Date:** 2026-07-27 (recorded retroactively)

## Context

A controller reads target T, decides, then declares the edge — and a change to T
landing in that window reaches nobody, since `dependentsWake` resolves
dependents at the instant of the change and the edge did not exist yet. The
dependent then settles at its own generation on the stale read, where
`ObjectsListUnsettledIDs` structurally cannot see it.

## Decision: `DependenciesAdd(…, targetResourceVersion)`

The caller passes **the version of T its decision was based on**, and
`DependenciesAdd` requeues `fromID` when **both** halves hold:

1. this call created the edge, **and**
2. T's current `resource_version` has moved past the version passed.

Re-reading T to obtain that value defeats the guard — a version fresher than the
read claims to have seen changes the decision did not. `0` is "no opinion",
correct when the edge is declared *before* reading T, which is already race-free.

### The conjunction is what converges

Either half alone spins, unthrottled — the dispatch path has no already-settled
skip and `workQueue.addLocked` has no rate limiter:

- *target-moved alone* re-fires every pass for a caller whose version never
  advances (one cached across passes);
- *edge-new alone* re-fires every pass for a controller that clears and
  re-declares its dependency set — the guard TODO.md records as built and
  reverted.

Requiring both bounds the wake to **once per edge**, which is the entire window:
every later change reaches an edge the waker can already see.

### A future version is rejected in the store, before the insert

A version above T's current one gets `ErrTargetResourceVersionFuture` — versions
only move forward, so it cannot have come from reading T, and an edge whose guard
can never fire must not persist. The check sits next to the read that knows T's
version, exactly as `UpdateStatus` rejects a future `observedGeneration`.

Checking it *after* in `DependenciesAdd` would make the guarantee conditional on the
caller: a nested `Within` is a bare `fn(ctx)` with no transaction of its own, so
returning an error unwinds nothing, and a caller that logs and carries on would
commit the edge inside its own transaction.

It is deliberately partial: it catches a version read off the wrong object, not one
read off the right object at the wrong time (a stale version is indistinguishable
from an old read, a freshly re-read one from a decision made this instant).

### Neither half costs a query

`EdgesAdd` returns an `EdgesAddResult` projecting `fromID`'s `GroupKind` and `toID`'s
`resource_version` from the endpoint check it already runs. A *pre-read* for
edge-newness is exactly what sank the earlier guard.

`EdgesAdd` **self-wraps in `Within`** like the other mutators: reporting a
`resource_version` from a statement that a write could land behind before the edge
is inserted would recreate this very window — invisible to the result *and* to
`dependentsWake` — so the atomicity is the store's to guarantee, not an
unstated precondition on the caller or on sqlite's single-writer serialization.
(`EdgesAdd` returns the endpoint metadata to every caller; the owner-edge path in
`insertObject` discards it and passes a `0` version claim.)

The store still interprets nothing — the wake policy lives in `controller.go`. The
wake routes by **`fromID`'s** kind, not the caller's (the edge is deliberately
cross-kind, and enqueuing a foreign id onto the caller's reconciler would decode
another kind's bytes as this one's `Spec`), and is registered post-commit via
`wakeAfterCommit`, not the reconcile-scoped `pendingWakes`, which is nil for the
out-of-band call where `Register` hands the application a `ControllerClient`.

## The wake has a durable twin so a crash can't lose it

The conjunction increments `objects.pending_wake`, so a process that dies between
the commit and the in-memory requeue leaves a persisted "reconcile owed".

### The stamp is inside `EdgesAdd`, and *before* the insert

Reported back as `EdgesAddResult.WakeStamped`, which the requeue gates on instead of
recomputing the conjunction. Sequencing it as a second store call after `EdgesAdd`
returned is the shape a reviewer caught: a nested `Within` unwinds nothing, so a
caller who handled `DependenciesAdd`'s error would commit the edge with no wake — the
stranded dependent this whole guard exists to prevent.

Ordering is the only guarantee available under the no-savepoint nesting contract,
and it points the residual failure the harmless way: a stamp with no edge is one
spurious owed wake that drains back to 0, where an edge with no stamp is invisible
forever.

The stamp's own `WHERE … NOT EXISTS (SELECT 1 FROM edges …)` is the **sole**
edge-new test — a probe straight down the edges primary key, which is the table
itself since `edges` is `WITHOUT ROWID` (see
[edges WITHOUT ROWID](2026-07-26-edges-without-rowid.md)) — no pre-read, and no
second derivation (the old `EdgesAddResult.Inserted`) left to fall out of agreement
with it.

### The stamp is not gated on `fromID`'s kind being registered

A client-only dependent never drains its count and nothing scans it either
(`WakesListPendingIDs` is per-kind, called only by that kind's reconciler), so the
count is *unread* — not free: it is a permanent nonzero column and index entry, and
re-declaring an edge (delete + re-add) with a stale claim increments it again, so it
can grow. What it costs when that kind later gains a controller is bounded to **one**
spurious pass, since the reconcile subtracts the whole observed count.

Gating is not the cheap alternative it looks: the stamp is SQL inside `EdgesAdd`, and
the store cannot know registration, so the caller would have to resolve `fromID`'s
kind *before* the call — the per-declare pre-read that sank the earlier guard — and
it would bake in a fact that changes between runs, losing the wake outright for a
kind that gains a controller later. A cross-kind sweeper (the `pending_wake`
analogue of the global GC sweeper's `DeletionRequestsList`) is the shape that
would reclaim it off the hot path; it is unbuilt, and in TODO.md.

That is also what keeps the policy out of the store: the in-memory requeue
self-gates through `enqueueIfRegistered`, so registration is decided in exactly one
place instead of two, and no beehive predicate runs inside a write transaction
holding the single connection.

`TestAddRefStampFailureLeavesNoEdge` pins the ordering, injecting a stamp failure
with `blockObjectUpdates`' `BEFORE UPDATE ON objects` trigger — `RAISE(ABORT)`
undoes the statement, not the transaction, so the outer caller *can* swallow and
commit, which is the whole point.

`WakesIncrement` is deliberately **not on the `Store` interface** — `EdgesAdd`
is production's only wake producer and `WakesDecrement` its only consumer, so
a standalone increment would be surface the declare path *cannot* use correctly (it
can't be made atomic with the edge) and nothing else uses at all. Leaving it off
makes "the stamp rides `EdgesAdd`" a compile-time property instead of something a test
polices; it survives on the concrete sqlite store so tests can seed a count without
staging the whole declare race, and is where a future non-edge producer would hook
in. `TestAddDependencyStampRidesAddRef` now pins only the half that isn't
structural: that folding the stamp in actually stamps.

A general fix for the class — SAVEPOINTs making every nested `Within` a real
rollback boundary — is in TODO.md, unbuilt.

### `pending_wake` is a count, not a flag

`typedController.reconcile` subtracts, on a successful pass, **the count it loaded**
(`WakesDecrement(id, observed)`, floored at 0).

A count rather than a single token is what survives the reviewer-surfaced case — a
wake owed *while an earlier one is being reconciled*: increments landing after the
load sit above `observed`, so they survive the subtraction and keep the object owed,
where a same-valued token would have been clobbered by the clear and then lost to a
crash.

Subtracting `observed` rather than 1 is the other half, and also a reviewer catch:
one pass reads the target's *current* state, which addresses every wake outstanding
when it started, and the backstop enqueues a row only once (the work queue
coalesces) — so a `-1` would strand the remainder with nothing to re-enqueue it,
forever when every periodic driver is disabled.

Nothing schedules a follow-up here: a residual only exists when an increment landed
mid-pass, and that increment brought its own in-memory requeue. A failed subtraction
is logged, not requeued — the count stays up for the backstop, where requeueing
would spin against a store that keeps failing.

### The backstop

`WakesListPendingIDs` over the partial index
`idx_objects_pending_wake WHERE pending_wake != 0`. An owed wake is orthogonal to
spec convergence (a spec-settled object can still owe one), so it is *not* folded
into `ObjectsListUnsettledIDs` but sits beside it in `enqueueCatchup`, which runs
unconditionally at startup and on each catchup tick — so declining the startup
resync does not suppress recovery.

Pinned end-to-end by `TestDependencyRequeueLostAcrossRestart` (one store, two
`Beehive`s, restart under `WithStartupResync(false)` + `resyncInterval=0`) and, for
the concurrent-wake case, `TestReconcilePendingWakeSurvivesConcurrentWake`.
