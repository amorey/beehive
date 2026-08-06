# The work queue floors how often one object is dispatched, and a pending alarm absorbs a wake

- **Status:** Accepted — implemented in `workqueue.go`, `internal/rategate/`,
  `beehive.go`, `options.go`, `client.go`, `controller.go`.
- **Date:** 2026-08-04

## Context

`workQueue.addLocked` had no rate limiter and the dispatch path had no
already-settled skip. Nothing bounded how often one id could be dispatched.

Three timers hid that. A dependency wake arrived at the waker's 1s tick, an owed
pass at 30s, a stale-dependents finding at 60s. Each interval was a rate limit
nobody chose, and two defects sat under them:

- **A dependency cycle of length ≥ 2 reconciled forever.** A wakes B, B wakes A.
  The waker's tick bounded it to one round trip per second, so it was a leak
  rather than a hot loop. With the tick removed — which a commit wake in front of
  the waker would do — the pair spins at store speed on the single write
  connection.
- **A standing `reconcile_owed` count overrode the backoff ladder above 30s.**
  `add` consulted only the queued-now set, so an id parked on a backoff alarm was
  not in it and the owed pass dispatched at once. The alarm later fired into a
  no-op.

Both wanted the same fact at the `add` call site: is this id already scheduled,
and how recently did it run?

## Decision

`addLocked` applies four rules in order. Rules 0 and 3 run in both modes; rules
1 and 2 are for wake-driven adds only, so an alarm firing and `Client.Requeue`
skip them.

```
0. q.gauge.isQueued(id)                     → return
1. a pending alarmBackoff or alarmFloor     → return; the alarm owns the dispatch
2. q.gate.OpensAt(id, now) reports held     → hold a floor alarm to the window end
3. otherwise                                → mark dirty and queue, as before
```

**Rule 0 tests `isQueued`, not `markDirty`.** `markDirty` is a mutator, so
testing with it would queue every arriving add before rules 1 and 2 could run.

**An `alarmKind` records why each alarm exists**, because a retry, a controller's
own schedule and a held wake answer differently when they meet. `alarmRequeueAfter`
is the iota zero value so an alarm literal that misses the field behaves as the
queue did before kinds existed.

**`addAfter` is no longer unconditionally newest-wins.** A floor alarm is not a
schedule — it is a real wake being held — and `runWorker` calls `done` and *then*
`addAfter`, so a wake that arrived mid-pass would have been destroyed by whatever
the pass set afterwards. A `RequeueAfter` of ten minutes would have served that
wake in ten minutes, where the queue previously dispatched it at once.

Arbitration is by kind *and* fire time. A backoff always takes the slot: the
ladder owns the retry and is meant to push the dispatch out. Where either side
is a floor, the earlier fire time wins, so the floor never delays work already
scheduled sooner and is never dropped by later work; a tie keeps the pending
alarm, which is the oldest-wins rule the floor needs. Deciding on kind alone
would have let a wake push a 10 ms `RequeueAfter` out to the floor — and buy
nothing, since a `RequeueAfter` alarm firing on its own dispatches immediately,
so the floor never bounded that cadence in the first place.

**A floor alarm that opens mid-pass re-arms rather than marking the id dirty.**
The floor bounds the gap between dispatches, and while a pass is running the
last dispatch has not finished, so the window has not started. Marking the id
dirty would let `done` queue it ahead of the alarm the pass sets a line later —
which held the ladder only for passes shorter than the floor, and any controller
doing real I/O exceeds it.

**The floor lives in `internal/rategate`**, a keyed gate that owns no timer and
starts no goroutine: the caller arms the timer it already has. Eviction validates
lazily, because re-admitting a still-held key appends a second queue entry while
the first is live and popping the first must not free the key. The waker's scan
limit and the object tail's drain limit want the same primitive.

**The two commit-time pushes split** into `signalRequeueNow` and
`signalRequeueThrottled`. The choice cannot be a parameter on `addLocked`:
`requeueNow` clears the pending alarm before `addLocked` runs, destroying what
rule 1 reads.

| Push | Entry | Why |
|---|---|---|
| A spec write enqueues its own object | `requeueNow` | new information; must not wait out a backoff |
| A new edge enqueues the edge's source | `add` | a controller can declare on every pass |

Pinned by `TestWorkQueueFloorDoesNotDelayASoonerSchedule`,
`TestWorkQueueASoonerScheduleReplacesAFloorAlarm` and
`TestWorkQueueFloorAlarmMidPassKeepsTheLadder`.

## Consequences

**The floor is per key, and that is the point.** It is cheap for N distinct
objects once and expensive for one object N times, which is the shape of the
pathology: a cycle is "the same id keeps coming back". A single global rate limit
inverts it and taxes the startup owed drain, the system's most important bulk
path. A chain A → B → C still propagates at full speed, because every hop is a
different object and each is quiet.

**A fan-in pays.** D depends on B and C; both change; C's wake arrives inside the
window D's dispatch opened and is floored. Bounded by the interval, latency
never divergence. Telling "a second distinct target moved" from "the same target
moved again" needs per-edge state the queue does not have.

**The cycle is bounded, not fixed.** `TestADependencyCycleIsBoundedByTheFloor`
pins it: without the floor the pair runs 25 reconciles in 30ms; with it, one
round trip per interval. Nothing converges. See the cycle item in `TODO.md`.

**`defaultMinRequeueInterval` matches `defaultWakeInterval` on purpose.** 1s is
what a cycle costs today, because the waker's tick is what bounds it, so a commit
wake in front of the waker can remove that tick and change the cycle rate by
nothing. If the wake interval is ever raised, that argument stops holding and
this constant should be reconsidered with it.

**A zero interval does not restore the old behaviour.** It turns off rule 2 and
nothing else — rule 1 absorbs an add over a backoff alarm at any interval,
because the gate is not on that path. Rule 1 is a semantic change, not a latency
one.

**`WithMaxRetryInterval` now means what it says.** Before this, the owed pass
floored the retry rate at `owedPassInterval`, so every rung above 30s was
unreachable. That was the actual defect behind the `reconcile_owed` item.

**Taking an id without dispatching it must release the floor.** `get` means
"dispatched", so it admits. `workQueue.discard` is the verb for the other case,
over `rategate.Forget`.

### Alternatives considered

**One global rate limit.** Simpler, and it targets the real harm directly, since
contention for the single write connection is an aggregate. Rejected: a 1/s
global rate drains a 10,000-object restart in about three hours, and a token
bucket does not rescue it — the refill rate must be low enough to bound a cycle
and high enough to drain a backlog, and those are the same number. Discarding the
key discards the only signal separating a broken system from a busy one. What the
idea is right about is that nothing measures aggregate dispatch rate today, which
is why the interval rests on the argument above rather than on a measurement.

**Rejecting cycles when the edge is declared.** Needs a recursive CTE on the
single connection, in `DependenciesAdd` — strictly more expensive than the
pre-read that already sank an earlier declare-time guard. It is also still open
whether beehive should support cycles at all.

**Reusing `addAfter` for the floor.** Its newest-wins rule would push the item
back on every arrival and starve a steadily woken id. Both objections dissolved
in the implementation — `(*alarm).outranks` made newest-wins conditional, and the
lock shape is what a `Locked` split answers — so the floor and `addAfter` share
`addAfterLocked`.

**Keeping the floor over any competing schedule.** The first draft decided on
kind alone, on the argument that taking the earlier of the two breaks the floor
whenever a `RequeueAfter` is shorter than the window. It does not: that alarm
fires through `addImmediate` and is never floored anyway, so the only effect was
to delay a controller's cadence when a driver wake happened to race it.

**Putting the gate in `amorey/gobus`.** Both that module and `amorey/gochan`
contain no goroutines and no timers at all; a throttling *receiver* cannot be
built without one, because `Chan()` must stay selectable. A gate needs neither,
but it is not bus semantics either.
