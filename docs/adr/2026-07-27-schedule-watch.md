# The schedule watch is an in-memory gauge, not a store stream

- **Status:** Accepted — implemented in `workqueue.go`, `watchpoll.go`, `client.go`.
- **Date:** 2026-07-27 (recorded retroactively)

## Context

`Client.SchedulesWatch(id)` / `SchedulesGet(id)` observe an object's next-requeue
time: the reschedules (`addAfter` backoff / `RequeueAfter`, `requeueNow`, dispatch,
enqueues from a owed pass, a full pass or a dependency wake, or `Requeue`) that bump
no generation and no `resource_version`, and so are invisible to `ObjectsWatch`. No
other signal captures them.

## Decision

The schedule is **reconciler state, not store state.** It lives entirely in the
beehive layer: the value *is* the in-memory `workQueue`'s view (`dirty` / `alarms`,
read through `nextRequeueAt`), so there is no table, no migration, and no
`storeapi` involvement. Persisting it would be wrong, not merely expensive — a
next-requeue time is a fact about *this process's* queue, and a restart legitimately
invalidates it.

`SchedulesWatch` polls that value on the watch-poll interval — the same cadence as
the object and event watches, since it is the same kind of surface — and emits only
when it differs from the last value reported. It is a **gauge**: it streams
`Schedule` itself rather than a `ScheduleChange`, because "what happened to it" is
not a question a consumer can act on (see the naming ADR's rule 2).

Two properties fall out of polling rather than having to be designed for:

- **No emit-site discipline to get right.** A push design would have to fire from
  the queue at exactly the genuine transitions and dedupe consecutive equal values;
  a poll compares values, so a repeated value is impossible by construction and no
  transition can be missed or double-counted.
- **No subscribe→snapshot race.** There is no snapshot to race: the first tick reads
  the current value like every tick after it.

The zero `Schedule` ("nothing scheduled") is a **real gauge value**, not an absence,
so it is emitted like any other — a `first` flag, not a zero check, is what
distinguishes "never reported" from "last reported the zero value".

### Client-only kinds

`SchedulesGet` (the point read) folds unknown / foreign / client-only to a zero
`Schedule` plus a nil error, via a no-store, no-kind-guard read of
`reconciler.nextRequeueAt`. There is no public bare-`time.Time` getter — the
`Schedule` struct is the sole read shape, leaving room for the reserved trigger
field.

`SchedulesWatch` instead returns `ErrNoController` for a client-only kind: a stream
that can never emit should say so rather than hang. An id that does not *exist* is
fine, though — it simply streams the zero `Schedule` until something schedules it.

## Consequences

- It reports only **per-id timers**, so it is not a prediction of the next
  reconcile: the owed pass, full pass and dependency-wake drivers all reconcile without
  one, and a zero `NextRequeueAt` means "nothing scheduled", not "will not
  reconcile". Observability, not a guarantee.
- It is the third watch surface alongside the object-change streams
  (`ObjectsWatch` / `ObjectsWatchList`) and `EventsWatch` (the log). A schedule is a
  single mutable *future* value, deliberately not routed through the append-only,
  retained event log.
