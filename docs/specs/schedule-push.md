# Spec: push the schedule gauge, and retire its poll

Status: draft — **standalone.** It depends on no other spec and no other
spec depends on it.
Date: 2026-08-01
Scope: `workqueue.go` (the gauge and the sites that move it), `watchpoll.go`
(`SchedulesWatch`), `reconciler.go` (`nextRequeueAt`, the hub field, shutdown
ordering), `beehive.go` (`Register` builds the hub beside the queue),
`client.go` (`SchedulesGet` reads the gauge), `testutils_test.go` (the leak
filter), and `go.mod` (`github.com/amorey/gobus` v0.4.0).
Related: **this spec reverses a recorded decision.** `docs/TODO.md` says
`SchedulesWatch` "is the one watch left polling after the push conversion,
and no backstop can be built for it". This spec argues the opposite, from
the same fact. The [schedule-watch ADR](../adr/2026-07-27-schedule-watch.md)
holds why the gauge is memory, and this spec depends on that staying true.
The [drivers ADR](../adr/2026-07-28-periodic-scan-drivers.md) states that
every driver is a periodic scan; this spec is the first exception to it.

## Why this is a spec of its own

Beehive is planned to gain a push layer, so that a commit starts the work a
tick starts today. That plan is written up separately, it is large, and
every part of it turns on the same question: what backs a push path up when
a notify is lost.

**The schedule gauge answers that question differently from everything
else, and it can be built without waiting for any of it.** It touches no
store method, needs no new store read, and shares no code with the object
watches or the wake paths. It is the only watch whose delivery can be
converted rather than doubled. So it is carved out and implemented on its
own, and the rest of the plan is written assuming it has landed.

Read this file alone. It repeats the policy it needs rather than pointing
at the wider plan, so a reader on this branch needs nothing else.

## Goal

`SchedulesWatch` streams the work queue's next-requeue time for one object.
Today it polls that value every second and emits when it changed.

Convert it. A hub delivers the gauge when the queue moves it, and the poll
goes, so the watch reports a change when the change happens.

## Why this one converts fully

Two facts hold for this value and for nothing else in beehive. Together
they are the whole argument, and neither is sufficient alone.

### 1. The hub can see every writer that exists

Push in beehive is registered above the store, so a hub sees a write only
if it went through this process. A poll scans the store, so it also sees a
second process on the same database, and the embedder writing through the
`Store` they constructed and passed to `New`. That gap is structural: no
care at the notify sites closes it. It is why every store-backed poll has
to stay, whatever is added beside it.

**The schedule has no such gap.** `workQueue` is unexported, process-local,
and owned by one reconciler. No other process can move the gauge; the
embedder holds no handle that reaches it. The notify sits inside the
queue's own critical sections, so the set of writes the hub can observe is
the set of writes that can happen.

This is what the recorded objection got backwards. `docs/TODO.md` reasons
that a gauge living in memory can have no backstop and therefore cannot be
pushed. The same fact — that the value lives in memory this process owns
outright — is what makes it the one value a hub can track exhaustively. The
watch that looked least convertible is the most completely convertible.

### 2. Every move is reported from one small type

A backstop's second job is catching the failure no monitor sees: a mutation
that happens and does not notify. A dead goroutine is detectable; a missing
notify call is not.

The gauge is not a field. `nextRequeueAtLocked` derives it from `dirty[id]`
first, then `alarms[id]`, so "which operations move it" has a non-obvious
answer that a checklist will eventually get wrong. The existing code
already carries the evidence: `done` documents that it moves an id between
`processing` and `items` and therefore changes nothing observable, while
`get` documents that clearing the dirty slot leaves the id unscheduled
absent an alarm.

**So move the two maps into a `gauge` type that reports its own movement,
and make them unreachable from anywhere else** — see "The gauge owns the
maps" below. A new queue operation cannot move the schedule without calling
a `gauge` method, and every `gauge` method returns what moved.

State the guarantee at its real strength: this does not make a missing
notify impossible to write. It confines it to one type with five mutators,
each of which returns a report its caller must consume, and it is pinned by
a tripwire test that drives every mutator and asserts a report. That is an
auditable surface rather than a proof, and it is the strongest form
available without a language that can express the invariant. Do not write
it up as "the type will not compile".

### What is given up, stated plainly

There is no repair path. If the hub and the queue ever disagree, they
disagree until the process restarts. The two facts above are load-bearing
for exactly that reason, and they are the only thing standing where a
backstop stands elsewhere.

The compensating fact is that nothing outlives the disagreement. The queue,
the hub and the subscriber are one process. A restart rebuilds the schedule
from the startup owed pass rather than restoring it, so there is no durable
record for a stale view to be measured against, and no observer that
survives to hold one.

**A future change that gives `workQueue` a second writer invalidates this.**
An exported handle, a shared queue, a second process, a durable schedule —
any of them breaks fact 1, and the poll would have to come back. Say so
where the `gauge` type lives, not only here.

## What lands, in two commits

A removal belongs in its own commit, so a bisect can tell a new path's bug
from a cleanup's regression:

1. **Add the `gauge` type, the hub and the notify.** The poll still runs.
   Both paths feed one stream through the existing `last`/`first` check, so
   a value delivered by push is not delivered again by the next tick, and
   the reverse. Everything is observable and the old path still carries the
   contract.

   **Commit 1 must not close the sender.** The shutdown ordering below
   belongs to commit 2, because closing the sender is what ends a stream at
   beehive shutdown — the public behaviour change this split exists to
   isolate. In commit 1 the receiver is released by its own teardown and
   nothing else, and a stream still ends only with its caller's ctx.

   **One goroutine owns the stream, and this is the substance of commit 1.**
   `last` is a local of the stream goroutine and `out` has one sender, so the
   two paths must not be two goroutines — that is a data race on `last` and a
   double send on `out`. Replace `driver.Run` with an explicit loop that
   selects over three things: `ctx.Done()`, a `time.Ticker` at the poll
   interval, and `rx.Chan()`. Both arms run the same equality check and the
   same send.

   `driver.Run` cannot be reused here, because it owns its own loop and its
   step takes no second wake source. Its eager first step becomes an explicit
   read before the loop, which is where the snapshot already goes.

   **Commit 1 must read with `Chan`, not `RecvContext`.** A select needs a
   channel, and `RecvContext` is a blocking call — so while there is a ticker
   arm, `Chan` is the only shape. "Delivery" below describes the end state,
   after commit 2 removes the ticker.

   Two consequences hold for the length of commit 1, and both stop being true
   afterwards:

   - **The leak-filter widening is load-bearing, not insurance.** `Chan`
     starts a feeder goroutine for each subscriber, whose frames are all in
     `gobus`. Widen `beehiveStacks` in commit 1.
   - **The `Chan` caveats apply.** A value already committed to delivery can
     be superseded, and one value can still arrive after a close. Both are
     tolerable here and only here, because the poll is still the backstop.

   Two shapes are rejected. A forwarding goroutine that turns `RecvContext`
   into a channel is `Chan` rewritten by hand. Polling the receiver with
   `TryRecv` on the ticker discards the immediacy commit 1 exists to
   demonstrate.

   **The poll is unstamped, and that is fine.** It reads the gauge directly
   and updates `last` only; it never reaches `Accept`. The two paths can
   still interleave to emit one stale value — a push that `Accept` allowed
   against the hub's baseline while a poll tick had already reported a newer
   gauge — and that is accepted: the poll is the backstop in this commit and
   the next tick corrects it. Do not try to stamp the poll.
2. **Remove the poll**, and take the shutdown change with it. This is where
   the stream's lifetime changes, so it is where the contract note, the
   `reconciler.go` comment edit and the ADR update belong.

   The ticker arm goes, so the select has one arm left and the loop becomes a
   blocking read: **switch `Chan` to `RecvContext` here**, as part of the same
   change. The feeder goroutine disappears with it, and the leak filter drops
   back to insurance.

If commit 1 lands and commit 2 does not, the result is a supported state —
push beside a poll, with today's lifetime — rather than a half-migration.
Stop there if the notify path does not earn confidence.

## Non-goals

- **Do not make the schedule durable.** `docs/TODO.md` argues against that
  separately under the `RequeueAfter` item, for reasons that still hold.
  This spec needs it not to be durable: a durable schedule would have a
  second writer, and fact 1 would fail.
- Do not change when the queue schedules anything, or what a `Schedule`
  reports. This spec changes how the value is delivered, one lifetime rule
  (below, deliberately), and nothing else.
- Do not change `ErrNoController`. A client-only kind has no reconciler, so
  no queue, no hub and no stream.
- Do not convert any other watch, and do not remove any other poll. The
  argument above is specific to a process-local value with no second
  writer. No other watch has that shape, and this spec must not be cited as
  precedent for one that does not.
- Do not fence writes. A slow subscriber must never stop the queue.

## The bus

Use [`github.com/amorey/gobus`](https://github.com/amorey/gobus) `v0.4.0`,
package `watch`: a **keyed latest-value state bus**. One receiver watches one
key and holds one slot; `Hub.Watch(k, initial)` seeds that slot with the
value the caller has just read; a caller-supplied `Accept` decides whether an
incoming value replaces it.

It is the right shape rather than a convenient one. `SchedulesWatch` is a
**gauge** — it streams the value itself, which is what `CLAUDE.md` requires
for a watch over a gauge or a log. `watch` is the keyed gauge bus. Its sister
`gobus/conflate` is a keyed *event* bus with coalescing and annihilation, and
that is what the object-watch deltas want, not this stream.

Three properties carry this design, and each removes code that an earlier
draft of this spec had to write by hand:

| Property | What it removes here |
| --- | --- |
| `Watch(k, initial)` is the snapshot | The register-before-read rule, its argument, and the test that held a goroutine in the window |
| `initial` is the `prev` of the first `Accept` | The per-stream `Seq` filter in the read loop |
| `Accept` runs per receiver, against that receiver's own slot | The merge, and the reasoning about which of two racing publishes wins |

**`Accept` is what makes a reordered publish safe.** Sends happen after
`q.mu` is released — they must, because `Send` takes the bus lock and nesting
that inside the queue's critical section would put a second lock on the hot
path of every enqueue in the system. So two moves can unlock in one order and
reach `Send` in the other. `Accept` resolves that by comparing the values
rather than trusting arrival: whichever send runs second sees the first's
value as `prev`, so the slot ends up the same either way.

Note the constraint the package states: **`Accept` must not take any lock a
caller may hold while calling `Watch`, `Send` or a `Close`.** `Watch` is
expressly safe under a producer's lock, so an `Accept` that took `q.mu` would
invert the two orders and deadlock. Ours reads its two arguments and nothing
else, which the package documents as always safe. Say so where it is
written.

**Build the hub where the queue is built.** `Register` constructs the
`workQueue` (`beehive.go`), not `reconciler.run`, so the hub is constructed
there too. A `SchedulesWatch` between `Register` and `Start` must find a hub
rather than a nil pointer.

The companion fact, because nobody will predict it from the code: after the
hub closes, `Watch` returns a pre-closed receiver rather than nil or a panic.
So a `SchedulesWatch` issued after shutdown delivers the snapshot it read and
then ends. That is the right behaviour and it needs no special case, but it
should be written down.

Two rules for the code that uses it:

## Failure policy

- **A panic in the push machinery crashes the process.** No goroutine this
  spec adds may panic and be swallowed. The restart runs the startup owed
  pass, which is a path already tested. With no backstop behind this hub, a
  silently dead goroutine is the worst outcome available.
- **`gobus.ErrClosed` is not a failure.** `Send` returns it once the sender
  is closed, which is the ordinary end of the shutdown sequence below: a
  worker draining after `Sender.Close()` will see it. Log at debug and
  return. A rule of "crash on any error from the push machinery" applied
  naively here panics on every clean shutdown.
- **A subscriber that falls behind is collapsed, not failed.** Conflation
  bounds it by one key, so it catches up on the current value instead of
  overflowing. That is the level-triggered contract applied to delivery,
  and for a gauge it is exactly right: an intermediate schedule is owed to
  nobody.
- **Every goroutine this spec adds must end when its owner stops**, and the
  existing tripwire does not yet see them. See "The leak filter must be
  widened".

## Design

### The gauge owns the maps

Move `dirty` and `alarms` off `workQueue` and into a `gauge` value the
queue holds. Nothing outside `gauge` touches either map.

Every mutator returns what moved, stamped:

```go
// stamped is the gauge's value plus the order it moved in. Seq is assigned
// under workQueue.mu, so it is the queue's order and not arrival order.
type stamped struct {
    Schedule Schedule
    Seq      uint64
}

// keyed is one id's move, for the mutator that moves many at once.
type keyed struct {
    ID ObjectID
    stamped
}

// Each mutator reports the ids whose observable schedule changed. A caller
// holds workQueue.mu. An empty return means nothing a watcher can see moved.
func (g *gauge) markDirty(id ObjectID) (stamped, bool)
func (g *gauge) setAlarm(id ObjectID, a *alarm) (stamped, bool)
func (g *gauge) clearAlarm(id ObjectID) (stamped, bool)
func (g *gauge) clearDirty(id ObjectID) (stamped, bool)
func (g *gauge) clearAllAlarms() []keyed // shutdown; one entry per id that had one
func (g *gauge) at(id ObjectID) stamped  // reads, including the subscribe read
```

`Seq` increments on every reported move. `at` returns the current `Seq`
without incrementing, so a subscribe read can be compared against later
sends.

Dropping the `ok` reaches two call sites outside `workqueue.go`.
`reconciler.nextRequeueAt` returns `(time.Time, bool)` and folds in a
`r.work == nil` branch for a kind with no queue; `Client.SchedulesGet` reads
it and discards the bool. Fold the nil branch into the reconciler's accessor
and let both return the schedule alone.

**`at` returns no `ok`, and that is deliberate.** `nextRequeueAt` returns
`(time.Time, bool)`, and both of its callers discard the bool today
(`watchpoll.go` and `client.go` both read `at, _ :=`). Nothing distinguishes
"not scheduled" from the zero time, because the zero `Schedule` already
means exactly that and is a real value the watch delivers. So dropping the
bool is a simplification, not a lost distinction. Do not preserve
`(stamped, bool)` out of caution — it would carry a dead result into every
new call site.

This is what makes fact 2 true, and it is also what fixes the site problem
an earlier draft had: there is no single before-and-after wrapper that fits
every caller, because the callers do not have a common shape.

### The queue holds a sender interface, not the hub

The queue does not hold a `*watch.Hub` or a `*watch.Sender`. It holds an
unexported single-method interface:

```go
// scheduleSender is what workQueue needs from the hub, and all it may reach.
type scheduleSender interface {
    Send(id ObjectID, s stamped) error
}
```

Two things fall out, and both are reasons rather than tidiness:

- **The queue becomes testable without a hub.** A double satisfies one
  method, so a test can assert exactly what the queue published for a given
  mutation without standing up a hub and a receiver to observe it.
- **"No gobus type is exported" becomes mechanically checkable.** The
  boundary is one interface in one file, so the rule is a property of a
  declaration rather than a review habit.

### The notify sites, one per critical section

Each of these is one critical section that collects whatever the `gauge`
reported and sends after unlocking:

| Site | Gauge calls | Note |
| --- | --- | --- |
| `add` | `markDirty` | |
| `addAfter`, `delay > 0` | `setAlarm` | Reports nothing when the id is already dirty: `at` reads `dirty` first, so the alarm is not observable |
| `addAfter`, `delay <= 0` | `markDirty` | It delegates to `q.add`, so this branch sets no alarm and clears none. The gauge answer comes from `dirty` either way |
| `requeueNow` | `clearAlarm`, `markDirty` | Two calls, **one emit** — see below |
| `timerFired` | `clearAlarm`, `markDirty` | Must become one critical section — see below |
| `get` | `clearDirty` | The id is not a parameter; it is `items[0]`, known only inside |
| `stop` | `clearAllAlarms` | Emits, then the sender closes — see "Shutdown" |

**The `q.stopped` guard stays above the gauge call at every site.**
`addLocked` and `addAfter` both return early once the queue is stopped. A
site that moved its `gauge` call above that check would publish a due-now
after `stop` already emitted the final value, and with no poll behind it that
value would be the subscriber's last word.

`addLocked` is **not** a site. It is the shared body that `add` and
`requeueNow` both call under the lock, so treating it as a site would nest
inside `requeueNow`'s own collection and emit twice.

`done` is not a site and needs no exemption: it calls no `gauge` mutator,
because it moves an id between `processing` and `items`, which `at` does
not read.

**Coalesce per critical section, keeping the last.** `requeueNow` clears an
alarm and then marks dirty. Reported individually that is "nothing
scheduled" followed by "due now", and the first is a state no watcher
should ever see — it never existed between two consistent points. Collect
into a small per-section map keyed by id and send only the final value for
each. This is one rule that covers every multi-call site.

### `timerFired` must become one critical section

Today it takes the lock, deletes `alarms[id]`, unlocks, and then calls
`q.add(id)`, which takes the lock again. Under a poll, the window between
them is almost never observed. Under push it is observed every time: a
subscriber sees the zero `Schedule` and then due-now, for a transition that
is logically one step.

Fold both into one critical section, preserving what the split was for: the
superseded check (`q.alarms[id] != a` means a newer schedule owns the
enqueue, so do nothing) and the no-op when the queue is stopped. With the
coalescing rule above, the fused section emits due-now once.

### `Accept` is the ordering rule

The gauge stamps every move with a `Seq` assigned under `q.mu`, so `Seq` is
the queue's order rather than an arrival order. The hub carries the pair and
`Accept` compares it:

```go
hub := watch.New[ObjectID](watch.WithAccept(
    func(prev, next stamped) bool { return next.Seq > prev.Seq },
))
```

That one line replaces the merge and the read-loop filter an earlier draft
needed. Three things make it sufficient, and all three are the package's
guarantees rather than ours:

- **It runs per receiver, against that receiver's own slot.** Two streams on
  one id can be seeded at different moments, so a value can be new for one
  and old for the other. A hub-wide evaluation would answer wrong for one of
  them.
- **`initial` is the `prev` of the first call.** The value read at subscribe
  is therefore the staleness baseline, which is what closes the subscribe
  seam — see "Subscribe".
- **A rejected value reaches nothing.** It does not enter the slot, so it
  cannot displace a value the feeder is already delivering. An earlier draft
  of the upstream spec replaced a waiting value on arrival order alone; that
  would have let a stale publish evict a correct one, and it was fixed before
  release.

**`Peek` is how a test sees `Accept` work.** `v0.4.0` adds
`Receiver.Peek`: `TryRecv` minus the take, with the same precedence. It is
the only way to assert that a value was *rejected* rather than delivered and
then dropped downstream — a distinction this design needs, because the
delivery loop's own equality check would mask an `Accept` that never ran. See
the test plan.

**A `Peek` test must own its receiver.** `Peek` is a single-consumer read,
and a live `SchedulesWatch` stream is that consumer: its `RecvContext` takes
a value as soon as one is unread, so a `Peek` from the test goroutine races
the stream and usually loses — reporting `ErrEmpty` for the wrong reason. So
a test that asserts at the bus boundary calls `hub.Watch(id, seed)` itself
and never starts a stream on that receiver. A test that asserts on delivery
reads the stream and does not `Peek`. Keep the two shapes apart; the tests
below say which each one is.

Three cautions, all from the package doc, because each is a plausible misuse
here:

- **`Peek` is not a read of current state.** A receiver that has caught up
  reports `ErrEmpty` even though its slot holds a good value. `SchedulesGet`
  must keep reading the gauge directly; it must not be "simplified" into a
  `Peek`.
- **`Peek` takes the hub lock**, the same one that serializes the whole send
  fan-out. It belongs in a test, once per assertion. It must never appear in
  the delivery loop, which reads with `RecvContext`.
- **`Peek` can deregister.** It runs the same terminal check the reading
  paths do, so a peek at a drained receiver tears it down. That is correct
  and harmless, but it means `Peek` is not a pure observation.

One useful property, which the shutdown test below relies on: after
`Sender.Close`, a receiver holding an unread value is **not** yet terminal,
so `Peek` still reports that value. The final "nothing scheduled" is
therefore assertable at the bus after the sender closes.

`Accept` never combines values and never annihilates. A rejected value leaves
the slot as it was; there is no "the key is gone" state, and a gauge does not
need one — the zero `Schedule` is "nothing scheduled", a real value this
stream delivers.

### Subscribe

Read the gauge and register the watch in **one** critical section:

```go
func (q *workQueue) watchSchedule(id ObjectID) (*watch.Receiver[ObjectID, stamped], stamped) {
    q.mu.Lock()
    defer q.mu.Unlock()
    cur := q.gauge.at(id)
    return q.schedules.Watch(id, cur), cur
}
```

`Watch` calls no caller code, so it is safe under `q.mu`. That is what makes
the single critical section legal, and it is stated by the package rather
than assumed here.

**There is no ordering rule left to get wrong.** An earlier draft of this
spec had to register the receiver first and read second, argue why the
reverse loses a move forever, and pin it with a test that blocked a goroutine
in the window. None of that survives:

- A move whose critical section ran **before** this read is already in `cur`.
  Its later `Send` carries a `Seq` at or below `cur.Seq`, so `Accept` rejects
  it against the baseline. No duplicate.
- A move whose critical section ran **after** finds the receiver registered,
  and its `Seq` exceeds the baseline. No loss.

**The bus does not deliver `initial` back.** It is the caller's own argument,
and a receiver reads only once a `Send` supersedes it. So this stream
delivers `cur` itself as the first value, then serves the receiver. A fresh
receiver's `TryRecv` reports `ErrEmpty`, which means "nothing has changed
since you subscribed".

### Delivery

This is the end state, after commit 2. Commit 1 reads with `Chan`, because
its loop still has a ticker arm; see "What lands, in two commits".

**Read with `RecvContext`, not `Chan`.** Once the ticker is gone the stream
goroutine waits on exactly two things — the caller's ctx and the receiver —
and `RecvContext` is that, already implemented and already tested upstream.

```go
last := cur.Schedule
if !sendOrDone(ctx, out, cur.Schedule) {
    return
}
for {
    ev, err := rx.RecvContext(ctx)
    if err != nil {
        return // ErrClosed, or the caller's ctx ended
    }
    // No staleness check: Accept rejected every value the queue
    // superseded. This comparison is for coalescing only.
    if ev.Value.Schedule == last {
        continue
    }
    last = ev.Value.Schedule
    if !sendOrDone(ctx, out, ev.Value.Schedule) {
        return
    }
}
```

Three things fall out, and the third is why this is not a style preference:

- **No feeder goroutine.** `Chan` starts one per receiver. `RecvContext`
  runs on the goroutine this stream already has, so a watch costs one
  goroutine rather than two.
- **No `Chan` caveats.** `Chan` documents that a value already committed to
  delivery can be superseded, and that one value can still arrive after a
  close because both select arms are ready and Go picks at random. Neither
  applies to a synchronous read.
- **The shutdown race below disappears on the delivery side**, because there
  is no parked delivery to race with a close.

`RecvContext` returning `ctx.Err()` does not close or deregister the
receiver, so `defer rx.Close()` is still required. The package doc is
explicit that an abandoned handle holds its key against the hub for the
hub's lifetime.

**The value comparison stays, and it is not redundant.** A receiver that has
not read its slot delivers only the newest accepted value, so the gauge can
move away and back — A to B to A — while nobody reads, and the stream would
otherwise report A twice. The schedule-watch ADR requires that a repeated
value be impossible. That is a property of the gauge, not of the bus, and no
bus guarantee removes it.

Compare the `Schedule`, never the `Seq`: two stamps always differ, and
comparing them would emit forever.

### Shutdown: this changes the stream's lifetime, deliberately

An earlier draft asserted that "the streams end with the same ctx that
stops the queue". **That is false, and the code says so today.**
`SchedulesWatch` takes the caller's ctx; the reconciler runs under
`Beehive.Start`'s. `reconciler.go` states the consequence in a comment: "A
live `SchedulesWatch` needs nothing here: it polls, so it simply reports
the stopped queue's empty schedule until its own context ends."

So two things change, and both are decisions rather than side effects:

- **The stream now ends at beehive shutdown, not only at caller-ctx
  cancel.** Closing the sender closes every subscriber's channel once it
  has drained.
- **The final "nothing scheduled" must still be delivered.** Today `stop`
  clears the alarms, which moves the gauge, and the next poll tick reports
  it. A design that notifies nothing from `stop` would deliver *less* than
  the poll does. So `stop` emits `clearAllAlarms`'s reports, and only then
  does the sender close.

`clearAllAlarms` reports only the ids whose *observable* schedule moved, like
every other mutator. An id that is both dirty and alarmed reads as due-now
before and after — `at` consults `dirty` first — so clearing its alarm changes
nothing a watcher can see, and it must not be reported. Reporting it would
publish a `Seq` bump that `Accept` takes and the stream's equality check then
discards: harmless, but it would contradict the contract stated with the
mutator signatures.

**That final emit is partial, and the limit is real.** `clearAllAlarms`
reports one entry per id whose schedule moved. `stop` touches neither `dirty`
nor `items`, so an id queued for immediate dispatch still reads as due-now,
and its stream now closes on a due-now that will never happen.

This is not a regression — the poll reports the same thing today, because
the gauge genuinely still says due-now — and clearing `dirty` in `stop`
would change what the queue does, which the non-goals exclude. So state the
limit rather than implying that every subscriber ends on an accurate value:
**a stream ends on the last gauge value, which after shutdown may name a
dispatch that will not occur.** Put it in the doc comment, because "the
final value is delivered" reads as a stronger promise than this is.

The chosen end state, in order, inside `reconciler.run`'s defer:

```
wg.Wait()          // drain the workers
q.stop()           // cancel timers, clear alarms, emit the final values
sender.Close()     // graceful: each receiver reads its last value, then ends
```

**Do not call `Hub.Close`.** An earlier draft did, right after
`Sender.Close`, and it would have defeated the drain this section depends on.
`Hub.Close` is hard tear-down: it closes every live receiver at once with no
drain. A receiver that had not yet read the final value would lose it, and
the loss would depend on timing — so the "Shutdown delivers, then closes"
test below would be flaky rather than failing, which is the worse outcome.
Sequencing the two calls buys nothing without joining the stream goroutines
in between, and joining them means tracking them.

Nothing is leaked by omitting it. Each stream runs `defer rx.Close()`, and a
receiver that drains after a sender close deregisters itself, so the hub
holds nothing once the streams end.

**`Sender.Close` is documented as unsafe to call concurrently with an active
`Send` from another goroutine, and beehive cannot guarantee quiescence.**
`Client.Requeue` is public and reaches `workQueue.requeueNow` from a user
goroutine at any time, including during teardown. The implementation is
memory-safe — the live count is atomic and everything else is under the bus
lock — and a racing publish gets `ErrClosed`, which the failure policy above
already treats as expected. Say this rather than describing the teardown as
quiesced, because a reader who checks the upstream doc will otherwise think
this ordering is unsound.

**The alternative, stated so the choice is visible:** leave the sender open
and let each stream end only with its caller's ctx, preserving today's
lifetime exactly. Rejected because a hub with no queue behind it can never
produce another value, so an open channel that will never speak again is a
worse signal than a closed one. But it is a public behaviour change, so it
needs the ADR note, the `CLAUDE.md` note, and an edit to that
`reconciler.go` comment, which will otherwise describe the opposite of what
the code does.

### The no-subscriber case: handled upstream

`Send` would otherwise take the bus lock and fan out on every enqueue in the
system, including in the overwhelmingly common case where nobody watches any
schedule.

**`watch` has never had it.** `Send` and `SendContext` check a lock-free
idle count and return before taking the lock when no receiver is live. An
unwatched hub therefore costs one atomic load per publish, and beehive builds
no mitigation of its own.

Do **not** add a subscriber count to `workQueue`. Earlier drafts of this spec
had one, with a guarded teardown and a panic on a negative count. The
upstream count is derived from the live receiver set rather than incremented
and decremented, so it cannot drift below the truth — the only unsafe
direction, since an under-report drops a value the bus will never retry. A
consumer-side counter reintroduces exactly that drift.

One cost, stated as the certainty it is rather than as a risk. The fast path
is hub-wide: `Send` returns early only when the hub has **no** live receiver.
Past that check it takes the bus lock unconditionally and resolves the key
under it. So **one subscriber on one object puts every publish for every
other object on the locked path, always.**

Two things bound it. The hub sits beside the reconciler, so the blast radius
is one kind's queue rather than the process. And the locked section is a map
lookup plus one `Accept` per receiver of that key, which for this stream is
one. Measure it; do not describe it as a possibility.

### Teardown

The stream goroutine releases its receiver on every exit path:

```go
defer rx.Close()
```

`Receiver.Close` is documented idempotent and serializes through the hub's
mutex, so no `sync.Once` is needed. Earlier drafts wrapped this in one — that
guard existed to protect a subscriber-count decrement, and with no count
there is nothing left for it to protect.

### The leak filter: load-bearing in commit 1, insurance after

Commit 1 reads with `Chan`, which starts a feeder goroutine for each
receiver, and the library warns that abandoning the channel without
`Receiver.Close()` pins it. Those frames are all in `gobus`, so the widening
below is required in commit 1, not optional.

Commit 2 switches to `RecvContext`, which starts no goroutine of its own —
the stream goroutine is then beehive's and the existing filter already sees
it. Keep the widening anyway, as insurance against a later switch back.

`beehiveStacks` in `testutils_test.go` keeps only profile records containing
`github.com/amorey/beehive`, so a pinned `watch` feeder — whose frames
are all in `github.com/amorey/gobus` — is invisible to the tripwire. The
failure policy's goroutine rule is therefore unenforced for the one new
goroutine class this spec introduces.

Two changes, both required:

- `defer rx.Close()` on the stream goroutine, so the receiver is released
  on every exit path. See "Teardown".
- Widen `beehiveStacks` to keep records naming `github.com/amorey/gobus`
  as well.

### The repeated-value trap

`workqueue.go` records that `dirty[id]` is stamped once at add time and not
refreshed, so repeated reads return a stable value; the ADR states that a
repeated value must be impossible. Push adds a second way to violate it: a
notify that constructs `time.Now()` rather than reading the gauge would
make every send look like a change.

Send what `at` returned. Never construct a time at the notify site.

## Honest costs

- **The payoff is uniformity and latency on a watch nobody has profiled.**
  `docs/TODO.md` is explicit that there is no correctness problem here and no
  measured cost. One ticker and one memory read per subscriber go away. Land
  this because a schedule change should be reported when it happens.
- **It puts a publish on the hot path of every enqueue.** Up to four map
  lookups inside a critical section the queue already holds, a `Seq`
  increment, a local capture, and then `Send`. With a subscriber present that
  takes the bus lock and evaluates `Accept` once for each receiver of the
  key. Every enqueue in the system pays it, so measure the watched case.
  The unwatched case is one atomic load and no lock.
- **A library owns part of the contract.** `Accept`'s per-receiver
  evaluation, the baseline rule for `initial`, and close precedence live in
  `gobus`. Pin the version, and treat an upgrade as a change to the watch
  contract until the release notes prove otherwise.
- **A watched object emits more, not less.** The poll collapsed a whole
  interval's movement into at most one report per second, and often zero. Push
  reports every move, so a hot object produces roughly two changes per
  reconcile — one for the enqueue, one for the dispatch that clears it. This
  is contract-legal: `Client.SchedulesWatch` already tells a consumer it may
  skip intermediate values. It is still a real change in what a panel sees,
  and it belongs beside the latency win rather than only in the code.
- **A public behaviour change.** The stream now closes at beehive shutdown.
- **It is the only stream in beehive with no repair path.** Defended above,
  and it must go in the ADR rather than be left for a reader to discover.

## Test plan

Write whitebox tests in `package beehive`, as `CLAUDE.md` requires.
Synchronize on channels and fakes, never on sleeps.

The tests fall into four groups by what they hold, and the grouping is not
cosmetic: **a `Peek` assertion is only meaningful on a receiver nothing else
is reading.** A live `SchedulesWatch` stream takes a value the instant it
lands, so a `Peek` racing it reports `ErrEmpty` on a coin-flip and proves
nothing.

### Gauge tests — no bus at all

- **Every `gauge` mutator reports.** Drive all five and assert each returns a
  report when it moves the observable value and none when it does not. This
  is the tripwire that keeps fact 2 honest as the queue grows.
- **`done` calls no mutator:** an id moving from processing to items changes
  nothing `at` reads.
- **`addAfter` on a dirty id reports nothing**, because `at` reads `dirty`
  first, so the alarm is not observable yet.
- **`clearAllAlarms` skips a dirty id**, for the same reason: its observable
  schedule is due-now before and after.

### Publish tests — a `scheduleSender` double, no hub

- **`requeueNow` publishes once:** a requeue over a pending alarm publishes
  due-now and never the intermediate "nothing scheduled". This is the
  per-section coalescing rule.
- **`timerFired` publishes once:** an alarm firing publishes due-now only. If
  the fused critical section regresses to two, this test sees the phantom
  deschedule.
- **Dispatch publishes:** `get` clearing the dirty slot reports the id as
  unscheduled, or as its pending alarm when it has one.
- **A stopped queue publishes nothing:** an `add` after `stop` reaches the
  sender zero times. This is the `q.stopped` guard, and without it a due-now
  lands after the final emit.
- **The no-subscriber path publishes nothing:** with no stream open, a burst
  of enqueues reaches the sender zero times.

### Bus-boundary tests — a receiver from `hub.Watch`, with no stream on it

These are the only tests that call `Peek`. Take the receiver directly; do not
start a `SchedulesWatch` on the same id.

- **A stale send never reaches the slot.** Publish `S2`, then the older `S1`.
  `Peek` must report `S2`, and the test's own read must return `S2` and never
  `S1`.

  `Peek` is what makes this test mean what it says. A delivery-only test
  cannot distinguish "`Accept` rejected `S1`" from "`S1` arrived and the
  equality check swallowed it", and the second passes with the hub wired
  without `Accept` at all. Drive it through the real hub: an `Accept` unit
  test passes whether or not the hub was given the option.
- **A stale send does not displace an unread value.** Same shape, with `S2`
  still unread when `S1` arrives. This is the case that made an earlier
  upstream draft unusable: it replaced a waiting value on arrival order
  alone.
- **The subscribe baseline rejects a duplicate.** Complete a move, `Watch`
  with the seed it produced, then let the publish land. It carries a `Seq` at
  or below the baseline, so `Peek` reports `ErrEmpty` — the value never
  entered the slot.
- **Key scope:** a receiver watching id A is untouched by id B's moves.
  Publish for B and assert `Peek` reports `ErrEmpty`; then publish for A and
  assert `Peek` reports it. The second half is what makes the first mean "B
  did not reach A" rather than "this receiver never receives anything", and
  both fit on one receiver because `Peek` takes nothing.
- **The final value is queued before the sender closes.** Stop the beehive,
  then `Peek` after `Sender.Close`: the final "nothing scheduled" is unread,
  so the receiver is not yet terminal and `Peek` reports it.

### Stream tests — through `SchedulesWatch`, never `Peek`

- **Immediate delivery:** a requeue reaches a subscriber with no tick, and in
  commit 2 with no poll in the process at all.
- **The stale send is invisible to a subscriber.** The delivery-side twin of
  the first bus test: publish `S2` then `S1`, and assert the stream reports
  `S2` once and never `S1`. Neither test alone shows the two halves are wired
  together.
- **The gauge, not the clock:** two moves that compute the same schedule
  deliver one value. This is the repeated-value trap the ADR forbids.
- **Supersession:** a second `addAfter` cancels the first and the subscriber
  sees the newer fire time, never the superseded one.
- **The first value is read at subscribe:** a stream reports the current
  gauge before any mutation, including the zero `Schedule` when nothing is
  scheduled, and the bus does not deliver that baseline a second time.
- **A move during subscribe is not lost.** Move the schedule while a
  subscribe is in flight and assert the subscriber converges on the new
  value. The single critical section is what makes this hold; a future
  refactor that splits the read from the `Watch` call fails here.
- **Coalescing:** N moves before the subscriber reads deliver one value, the
  final one by queue order. Hold the subscriber on a channel the test closes;
  `Send` never blocks, so the moves land in the one slot.
- **Fan-out:** two subscribers on one id both receive, and one that stops
  reading does not delay the other.
- **A closed stream stops receiving, and its siblings do not:** open two
  streams on one id, close one, and assert the other still receives every
  move. Closing the same stream twice changes nothing, since
  `Receiver.Close` is idempotent.
- **`ErrNoController` unchanged:** a client-only kind still gets the error
  and no stream.
- **Shutdown delivers, then ends:** stopping the beehive delivers the final
  "nothing scheduled" for an id that had a pending alarm, and *then* ends the
  stream. Assert both halves — a test that only asserts the end would pass
  with the final value dropped. This is also the tripwire for `Hub.Close`:
  reintroduce it and this becomes flaky rather than red, so run it under
  `-race` in a loop when the shutdown path changes.
- **Shutdown's emit is partial, on purpose:** an id that was queued for
  immediate dispatch ends on due-now, not on "nothing scheduled". Pin the
  documented limit so nobody later "fixes" it by clearing `dirty` in `stop`,
  which would change what the queue does.
- **`ErrClosed` is not fatal:** a publish that reaches `Send` after the sender
  closed logs and returns rather than panicking.
- **No poll remains** (commit 2): a `SchedulesWatch` stream makes no periodic
  read, and a stream left running on a quiet queue produces nothing and wakes
  nothing.
- **The existing tripwires pass unchanged.**
  `TestSchedulesWatchEmitsOnlyOnChange` and `TestClientWatchScheduleSnapshot`
  are the contract. The shutdown test above is the one place the contract
  deliberately moves.
- **`SchedulesGet` still answers:** it reads the gauge directly and does not
  go through the hub, so it works with no subscriber and after shutdown.
- **A stream outlives nothing:** after shutdown, no receiver is left
  registered and no goroutine remains. Commit 1 needs the widened leak filter
  for this, because `Chan`'s feeder frames are all in `gobus`.
- **No gobus type is exported:** no signature in the public API names
  `watch`, `gobus.Event` or the stamp.

## Docs to update when this lands

- **`docs/TODO.md`: delete the `SchedulesWatch` entry.** It records the
  decision this spec reverses, and leaving it would leave two answers in
  the repository. Its tripwire notes move into the ADR.
- The [schedule-watch ADR](../adr/2026-07-27-schedule-watch.md): the gauge
  stays memory, which that ADR decided and this spec depends on. Add the
  delivery half, the no-repair-path asymmetry, the two facts that carry it,
  and what would invalidate the whole argument. Record that `Accept` carries
  the ordering, so a later reader does not reintroduce a consumer-side
  staleness filter that the bus already owns.
- The [drivers ADR](../adr/2026-07-28-periodic-scan-drivers.md): it says
  nothing is pushed and every driver is a periodic scan. Record the
  exception and its criterion, and keep the rule for everything
  store-backed.
- **`reconciler.go`'s teardown comment**, which currently says a live
  `SchedulesWatch` "needs nothing here: it polls". After this it needs the
  shutdown ordering, and the comment would otherwise describe the opposite
  of the code.
- `CLAUDE.md`: the schedule-watch bullet says the watch polls
  `nextRequeueAt` and emits on change. It becomes a push path, and the
  stream's new close-at-shutdown behaviour belongs in the same bullet. The
  drivers list drops the watch poll's third consumer.
- `docs/reconcile-triggers.md`: section D, "In-memory only", describes the
  gauge. Its delivery changes; what it reports does not.

## Open questions

- *(none open)*

Three questions earlier drafts carried are closed:

- **The hub lives on the reconciler**, beside the queue it observes, and
  `Register` builds both. The scope line commits to this rather than leaving
  it open. A beehive-level hub would need the kind threaded through every
  queue operation, and it would widen the blast radius of the send lock from
  one kind's queue to the whole process — which settles it, given that a
  single subscriber puts every publish on that hub's locked path.

- **The stamp is a counter.** The `dirty` timestamp cannot serve, and not
  only because a coarse clock could tie — `get` and `stop` both move the
  gauge without writing a timestamp at all, so there would be nothing to
  read.
- **The bus is `gobus/watch`, and it exists.** Earlier drafts built this on
  `gobus/conflate` and carried a merge plus a read-loop filter to make an
  event bus behave like a state bus. `watch` shipped in `v0.3.0` with the
  keyed slot, the seeded baseline and `Accept`, so those two workarounds are
  gone. Do not reintroduce them. `v0.4.0` added `Peek`, which the test plan
  uses to assert that `Accept` rejected a value rather than inferring it.
