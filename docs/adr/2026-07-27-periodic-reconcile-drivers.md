# Three independent periodic drivers, not one resync tick

- **Status:** Accepted — implemented in `beehive.go`, `reconciler.go`, `gc.go`,
  `options.go`.
- **Date:** 2026-07-27 (recorded retroactively)

## Context

Beehive is level-triggered: events are a latency optimization, and what backstops
them is periodic work. That work used to ride a single `WithResyncInterval` tick.
Three different jobs shared it, and they have different cost curves — tuning
either of the cheap ones moved the expensive one, and disabling the reconcile tick
silently disabled GC.

## Decision

Split them into three drivers.

### `WithCatchupInterval` (default 30s, per-kind)

`enqueueCatchup` = `ObjectsListUnsettledIDs` + `WakesListPendingIDs`. Work the store
*records* as owed, so its cost is bounded by what is outstanding and it returns
nothing in a converged system. The two listings stay separate rather than unioned
in SQL so one failing still lets the other through, and `enqueueFrom`'s log names
which lost its pass.

### `WithResyncInterval` (default 0, off, per-kind)

`enqueueAll`. The only driver that reaches an object nothing recorded as owing
anything: process-scoped state a restart invalidated, or a wake lost for a reason
nothing observed. Opt-in because its cost scales with object count, and because
the startup pass already covers that ground once per process.

**Its meaning changed** — it used to pace the owed-work tick; an unchanged call
still compiles and now buys a full pass.

### `WithGCInterval` (default 30s, global)

`gcSweeperRun`: `sweepDeletionPending` + `sweepEventRetention`. Global because it
spans kinds with no controller.

`sweepDeletionPending` **routes** rather than collecting: a registered kind is
enqueued (only a reconcile can clear a finalizer — `collect` cascades then returns
while any remain), a client-only kind is collected directly. That is why
`DeletionRequestsList` returns `[]Referrer`, not ids. The routing lives in one
place, `advanceDeletion`, and the sweeper is its only caller: the event-driven
path (`advanceGCNow`) *only* requeues, so a client-only kind is always left for
the sweeper's next tick (`enqueueIfRegistered`'s no-op arm) and every `collect`
runs on the sweeper's goroutine rather than a caller's.

## This is the one interval that cannot be disabled

`WithGCInterval` rejects `d <= 0` with `ErrInvalidOption`, checked *before* the
target type-switch: the value is nonsense wherever it was aimed.

The reconcile knobs accept 0 because `Client.Requeue` still drives a pass by hand.
Nothing public triggers `collect`, so a sweeper-less `Beehive` accumulates
deletion-pending rows with no recourse, each `owned_by` edge RESTRICT-blocking its
owner's delete.

That invariant is load-bearing *inside* the sweeper too: every failure there is
logged and swallowed on the promise of a next tick, which is only true while a
cadence is guaranteed. Under the old startup-only mode, a swallowed
`DeletionRequestsList` error was the process's single attempt, stranding rows
for its lifetime. Requiring a cadence deleted that hole, `advanceGCNow`'s
synchronous-collect arm, and that arm's caller-cancellation strand (a cascade
abandoned mid-flight with nothing scheduled to resume it) in one move.

`gcSweeperRun` still returns early on a non-positive interval — unreachable
through `New`, kept so a `Beehive` assembled field-by-field (the
`withoutGCSweeper()` test helper) has no sweeper instead of panicking in
`NewTicker`.

## Startup is two steps, only one of them a choice

`enqueueCatchup` runs unconditionally — an object already owed a pass is not a
cheapness knob — and `WithStartupResync` (default true) adds the full pass. That
closed the old `StartupReconcileNone` + `resync=0` hole structurally rather than
warning about it; the `StartupReconcileStrategy` enum is gone.

Deletion-pending is *not* resumed here: the GC sweeper's own unconditional startup
pass routes it.

## Consequences

- GC survives any reconcile-knob setting.
- A converged system pays only the catchup listings, which return nothing.
- Full passes are opt-in per kind, plus one at startup.
