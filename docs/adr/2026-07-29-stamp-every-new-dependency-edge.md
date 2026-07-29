# Every new depends_on edge stamps a durable owed reconcile

- **Status:** Accepted — implemented in `sqlite/store.go` (`EdgesAdd`,
  `reconcile_owed`), `reconciler.go` (drain). Replaces
  `2026-07-27-caller-versioned-dependencies.md`, whose conditional stamp left one
  strand; everything from it that still governs live code is folded in below.
- **Date:** 2026-07-29

## Context

A dependent must eventually reconcile against every target it declares, whatever
else is happening when the edge is declared: a target that moved between the
caller's read and the edge's commit, a declare made on another object's behalf
(the cross-kind edge deliberately allows one), a crash before the wake is
serviced. The previous design recorded a wake only when the target had *moved past*
the caller's claimed `targetResourceVersion`, and covered the remaining shapes by
clearing the dependent's `dependency_watermarks` row so the stale-dependents pass
would find it (an absent row means stale).

That left exactly one interleaving uncovered, and it was a strand rather than
latency: a **third party declaring between a dependent's `ObjectsGetForReconcile`
and that dependent's own watermark write**. The in-flight pass — which never read
the new target — rewrote the row the declare had just cleared, from a load cursor
that may already sit above the quiet target's version. The dependent then read as
converged with nothing left to re-derive it, so a target that never moved again was
never reconciled against. Invalidating derived state cannot survive a pass that
re-derives it concurrently; only recorded owed work can.

## Decision

`EdgesAdd` increments `fromID`'s `objects.reconcile_owed` for **every `depends_on`
edge the call creates**, unconditionally. Self-edges are
excluded, as every scan excludes them: an object's own pass always reads its
current self, so a self-wake has nothing to deliver. Owner edges stamp nothing —
an `owned_by` edge is not a dependency.

This is sound under every interleaving because `ReconcileOwedDecrement` subtracts
only the count the pass observed at *load*: a stamp landing mid-pass sits above
that count, survives the subtraction, and keeps the object owed for the owed pass —
the property the count was built with (see "a count, not a flag" below).

The **edge-new gate is the whole convergence argument** now. A wake per edge ever
created cannot loop: a level-triggered controller that re-asserts its dependency
set every pass inserts nothing after the first and so stamps nothing. What the old
conjunction's target-moved half bought — no stamp at all for a declare whose read
was current — is given up deliberately: it cost one reconcile per edge to keep, and
keeping it was what left the strand. A controller that *deletes and re-declares*
its set every pass stamps once per re-create; that churn shape was the argument for
the conjunction, and it now costs what it costs rather than being subsidised by a
correctness hole.

The `targetResourceVersion` parameter is removed outright, from
`ControllerClient.DependenciesAdd` and `Store.EdgesAdd`, along with
`ErrTargetResourceVersionFuture`. Once the stamp stopped conditioning on it, the
claim's only remaining job was a pre-write sanity check — rejecting a version
above the target's current one as "read from the wrong object". That rejection
was load-bearing under the conditional stamp, where an impossible claim silently
and permanently disabled the wake guard; with the stamp unconditional, a garbage
claim can disable nothing, and the check degrades to a probabilistic bug detector
(it fires only when the wrong version happens to exceed the target's). An API
that asks every caller to carefully thread a value with no functional effect
misleads more than that weak signal is worth, so the parameter went with the
condition. The trade accepted: a caller wiring versions from the wrong object now
gets no error at all — it just converges anyway.

The watermark clear on a new edge stays, demoted from coverage to hygiene: a cursor
recorded over a smaller dependency set cannot speak for a target just added, and
leaving the row standing would misreport convergence to `DependentsListStale` for
the window until the stamped pass runs and rewrites it honestly.

## What carries over unchanged

**The wake is durable, so a crash can't lose it.** The count *is* the wake — there
is no in-memory requeue beside it to race — and `enqueueOwedPass` drains
`ReconcileOwedListIDs` at startup unconditionally (not gated on the startup full
pass), so a process that dies the instant after the commit finds the row still owed
on restart.

**The stamp is inside `EdgesAdd`, and before the insert.** A nested `Within` is a
bare `fn(ctx)` that unwinds nothing, so a stamp issued as a second store call after
`EdgesAdd` returned would let a caller that swallows the error commit the edge with
no wake — the stranded dependent this mechanism exists to prevent. Ordering points
the leftover failure the harmless way: a stamp with no edge is one spurious owed
wake that drains back to 0, while an edge with no stamp is invisible forever. The
stamp's own `WHERE … NOT EXISTS` is the **only** edge-new test (a probe down the
`WITHOUT ROWID` primary key, so no pre-read), shared verbatim with the watermark
clear via the `edgeIsNew` const so the two cannot drift. Every other fallible step
sits on the same side of the insert for the same reason. `EdgesAdd` self-wraps in
`Within`, so endpoint check, stamp, clear and insert are one atomic unit however it
is called.

**The stamp lands on `fromID`'s row and is routed by `fromID`'s kind.** The edge is
cross-kind, so the pass it buys runs on the dependent's reconciler, not the
declarer's; `ReconcileOwedListIDs` is per-kind, so each reconciler picks up only
its own rows.

**The stamp is not gated on `fromID`'s kind being registered.** The store cannot
know registrations, gating would cost a per-declare pre-read (what sank an earlier
version of the guard), and it would lose the wake outright for a kind that gains a
controller later — which now costs **one** spurious pass, since a reconcile
subtracts the whole accrued count. A client-only dependent's count goes unread; the
cross-kind sweeper that would reclaim it is unbuilt, in `TODO.md`.

**`reconcile_owed` is a count, not a flag.** A successful pass subtracts the count
it loaded, floored at 0. Increments landing after the load survive; subtracting
`observed` rather than 1 leaves no remainder that nothing would re-queue.
`ReconcileOwedIncrement` stays off the `Store` interface so "the stamp rides
`EdgesAdd`" is true at compile time; it exists on the concrete sqlite store for
tests and future non-edge producers.

## Consequences

- The mid-pass third-party declare is closed: pinned by
  `TestReconcileMidPassDeclareLeavesTheDependentOwed`, which stages the exact
  interleaving (quiet target, declare inside the dependent's pass) and
  asserts the derived state is blind while the count survives.
- Every declared dependency now costs exactly one reconcile of the dependent per
  edge created, including declares whose read was current. That pass reads the
  target and records an honest watermark, so it is the over-reconcile direction
  this design errs in throughout.
- `EdgesAddResult.ReconcileOwedStamped` now reports "this call created a
  depends_on edge (non-self)" rather than the old conjunction.
- The anti-spin tripwires consolidated: `TestAddDependencyWakesOncePerEdge` and
  `TestRefsAddStampsOnlyNewEdge` pin the once-per-edge bound where their
  predecessors pinned the per-claim conditions.
- With the claim gone, `DependenciesAdd(ctx, fromID, toID)` and
  `EdgesAdd(ctx, fromID, toID, relation)` are the whole declare surface; the
  version-guard tests (`TestAddDependencyRejectsFutureResourceVersion` and kin)
  went with the error they pinned.
