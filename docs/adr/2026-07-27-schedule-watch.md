# The schedule watch is an in-memory gauge, not a store stream

- **Status:** Accepted — implemented in `workqueue.go`, `reconciler.go`, `client.go`.
- **Date:** 2026-07-27 (recorded retroactively)

## Context

`Client.WatchSchedule(id)` / `GetSchedule(id)` observe an object's next-requeue time
live: the reschedules (`addAfter` backoff / `RequeueAfter`, `requeueNow`, dispatch,
external `add`s from resync, dependency wakes, or `Requeue`) that bump no generation
and no `resource_version`, and so never fire the object `Watch`. No other signal
captures them.

## Decision

Unlike `WatchEvents`, this lives **entirely in the beehive/reconciler layer, not the
sqlite store**: the schedule *is* the in-memory `workQueue` state (`dirty` /
`alarms`), so there is no store, migration, or `storeapi` involvement — it reuses
`gobus/conflate` (the external `github.com/amorey/gobus` bus library) directly.

### Emission

The `workQueue` emits through an `onSchedule func(id, at, scheduled)` callback, kept
domain-agnostic — native `(time, bool)`, never the `Schedule` type — fired **under
`q.mu` at genuine transitions only**: `addLocked` (guarded by its already-dirty
early-return), `get`, and `addAfter`-when-not-dirty. `done` never emits, since it
only shuffles processing/items, which `nextRequeueAt` ignores.

There is **no dedup memory**: `dirty` / `alarms` are the sole source of truth and the
emit sites are exactly the transition points, so consecutive emits can't repeat a
value. (An earlier design carried a `lastSched` / `schedSig` dedup map; it was
removed once the emit sites were made transition-precise.)

`Register` builds a per-kind `conflate.Hub[ObjectID, Schedule]` — `mergeSchedule` is
latest-wins and **never annihilates**, because an unscheduled/zero `Schedule` is a
real gauge value a subscriber must see, unlike the object watch's create-then-delete
annihilation — and wires `onSchedule = r.publishSchedule`, so the reconciler owns the
`(time) → Schedule` mapping, not the queue.

### Subscribe without a cursor

`WatchSchedule` registers the hub receiver and reads the snapshot **atomically under
`q.mu`** (`workQueue.subscribeSchedule`, which runs an opaque `subscribe func()`
under the lock), closing the subscribe→snapshot race with no high-water cursor: all
state is one in-memory lock, versus `WatchEvents`' DB+hub that needs
`snapshotHighWaterRV` dedup.

### Teardown

`reconciler.run`'s teardown defer calls `scheduleHub.Close()` (after `work.stop()`)
so live streams end on control-plane stop, mirroring `sqliteStore.Close()` closing
its hubs. A subscriber `ctx` closes its own stream independently.

### Client-only kinds

`GetSchedule` (the point read) folds unknown / foreign / client-only to a zero
`Schedule` plus nil error, via a no-store, no-kind-guard read of
`reconciler.nextRequeueAt`. There is no public bare-`time.Time` getter — the
`Schedule` struct is the sole read shape, leaving room for the reserved trigger
field.

`WatchSchedule` instead returns `ErrNoController` for a client-only kind: a live
stream that can never emit should say so rather than hang.

## Naming

A third watch surface alongside the object-change streams (`Client.Watch` /
`WatchList`, and the waker's `WatchObjectChanges`) and `WatchEvents` (the log). A
schedule is a single mutable *future* gauge, deliberately **not** routed through the
append-only, retained event log.
