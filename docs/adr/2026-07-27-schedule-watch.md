# The schedule watch is an in-memory gauge, not a store stream

- **Status:** Accepted — implemented in `workqueue.go` (the gauge and its hub),
  `scheduleswatch.go`, `client.go`. **The gauge stays memory. The
  delivery changed: it is pushed, and it has no poll.** See "Superseded: the
  poll" below.
- **Date:** 2026-07-27 (recorded retroactively); delivery amended 2026-08-01

## Context

`Client.SchedulesWatch(id)` / `SchedulesGet(id)` observe an object's next-requeue
time: the reschedules (`addAfter` backoff / `RequeueAfter`, `requeueNow`, dispatch,
enqueues from a owed pass, a full pass or a dependency wake, or `Requeue`) that bump
no generation and no `resource_version`, and so are invisible to `Client.Watch`. No
other signal captures them.

## Decision

The schedule is **reconciler state, not store state.** It lives entirely in the
beehive layer: the value *is* the in-memory `workQueue`'s view (the `gauge`'s
`dirty` / `alarms`), so there is no table, no migration, and no
`storeapi` involvement. Persisting it would be wrong, not merely expensive — a
next-requeue time is a fact about *this process's* queue, and a restart legitimately
invalidates it.

*(Historical, and no longer how it is delivered — see "Superseded: the poll".)*
`SchedulesWatch` polled that value on the watch-poll interval — the same cadence as
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
`reconciler.scheduleAt`. There is no public bare-`time.Time` getter — the
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
  (`Watch` / `WatchList`) and `EventsWatch` (the log). A schedule is a
  single mutable *future* value, deliberately not routed through the append-only,
  retained event log.

## Superseded: the poll

The gauge is still memory, and everything above about *what* it reports still
holds. **How it reaches a subscriber changed: the queue pushes each move to a
`gobus/watch` hub and the poll is gone.** This is the one stream in beehive with
no backstop at all.

### Why this one, and nothing else

Two properties hold here and nowhere else in the system.

**The hub sees every writer that exists.** Push in beehive is registered above
the store, so a hub normally sees a write only if it went through this
`Beehive`, while a poll scans the store and sees the row however it got there —
a GC path, a store call made below the hook, a publish that was lost. That gap
is structural and no care at the notify sites closes it. The schedule has no
such gap: `workQueue` is unexported, process-local, and owned by one reconciler,
and it is never backed by the store at all.

**Every move is reported from one type.** `gauge` owns the two maps the watch
reports on, and each of its mutators returns whether the observable schedule
changed. A queue operation cannot move the schedule without calling one, so the
failure a backstop would otherwise catch — a mutation that does not notify — is
confined to one small type with a test that drives every mutator.

Together they replace the backstop. Neither is sufficient alone, and **a future
change that gives `workQueue` a second writer** — an exported handle, a shared
queue, a durable schedule — **invalidates the argument and the poll has to come
back.**

The compensating fact is that nothing outlives a disagreement: the queue, the hub
and the subscriber are one process, and a restart rebuilds the schedule from the
startup owed pass rather than restoring it. There is no durable record for a
stale view to be measured against.

### What the poll used to buy, and what replaced it

The two properties this ADR credited to polling are now designed for:

- **Emit-site discipline** is the `gauge` type. A repeated value is still
  impossible, because a mutator that does not move the observable schedule
  reports nothing, and the stream compares values before sending.
- **The subscribe race** is closed by `workQueue.watchSchedule`, which reads the
  gauge and registers the watch in *one* critical section. The value read becomes
  the hub's baseline for that receiver, so a publish that predates it is rejected
  by the same rule that rejects a reordered one.

### Ordering, and why `Accept` carries it

A publish happens after `workQueue.mu` is released — it must, because `Send`
takes the bus lock and nesting that inside the queue's critical section would put
a second lock on the hot path of every enqueue. So two moves can unlock in one
order and reach the bus in the other.

Each move therefore carries a `Seq` assigned under the queue lock, and the hub's
`Accept` rule keeps the higher one. It runs once for each receiver against that
receiver's own slot, because two streams on one id can be seeded at different
moments and a hub-wide answer would be wrong for one of them.

**Do not reintroduce a consumer-side staleness filter.** `Accept` owns that. The
value comparison that remains in the stream is for coalescing only: the gauge can
move away and back while nobody reads, and a repeated value must not reach the
consumer.

### The stream now ends at shutdown

It used to report the stopped queue's empty schedule until its own context ended.
Now `workQueue.stop` publishes the final value of each id whose schedule moved,
and closing the **sender** lets each receiver read that value once before its
stream ends. `Hub.Close` is never called: it is hard tear-down with no drain, so
a receiver that had not yet read the final value would lose it, and the loss
would depend on timing.

The final emit covers **every** id the gauge still describes, not only the ids
whose schedule `stop` moved. That completeness is what makes a publish racing the
close harmless: a move needs the queue lock, and `stop` sets `stopped` under it,
so nothing can move after the snapshot and anything still in flight can only
carry a duplicate of it. Publishing the moves alone would let a subscriber end on
a value the queue had already left.

An id queued for immediate dispatch still ends on a due-now that will not happen.
`stop` clears alarms, not the dirty set, and the poll reported the same thing
because the gauge genuinely still says due-now.

### Costs taken knowingly

- **A watched object emits more, not less.** The poll collapsed an interval's
  movement into at most one report per second. Push reports every move, so a hot
  object produces roughly two changes per reconcile — the enqueue and the
  dispatch that clears it. `SchedulesWatch` already tells a consumer it may skip
  intermediate values.
- **One subscriber puts every publish for that hub on the locked path.** The
  bus's fast path is hub-wide: `Send` returns early only when no receiver is
  live. The hub sits beside the reconciler, so the blast radius is one kind's
  queue rather than the process — which is the argument for that placement.
