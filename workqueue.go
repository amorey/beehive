// Copyright 2026 Andres Morey
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package beehive

import (
	"sync"
	"time"

	"github.com/amorey/gobus/watch"
)

// workQueue is a FIFO queue of ObjectIDs with set semantics: adding an ID that
// is already queued is a no-op. It is safe for concurrent use.
//
// Callers select on ready, call get to retrieve the next item, and MUST call
// done once they finish processing it. Between get and done the ID is held in a
// "processing" state and is never dispatched again — so two workers can never
// reconcile the same object concurrently. An add that arrives while the ID is
// processing is remembered (dirty) and re-queued by done, so no wakeup is lost.
// This is the standard Kubernetes work-queue discipline.
type workQueue struct {
	mu sync.Mutex
	// gauge owns the state SchedulesWatch reports: which ids are queued now and
	// which hold a pending alarm. It is separate from the queue's own bookkeeping
	// so that every move of the observable schedule is reported from one place.
	// See "The schedule" below.
	gauge      *gauge
	processing map[ObjectID]struct{} // handed out via get, not yet done
	items      []ObjectID
	ready      chan struct{} // pulsed when items are available
	stopped    bool          // set by stop; adds become no-ops
	// schedules carries every move of the gauge to this kind's subscribers. Always
	// present: a queue exists only for a kind with a reconciler, and newWorkQueue
	// builds the hub with it.
	schedules scheduleSender
}

// alarm is a pending delayed enqueue: the timer that will enqueue the id and the
// absolute time it fires, so the gauge can report when an id is next due without
// re-deriving it from the timer.
type alarm struct {
	timer  *time.Timer
	fireAt time.Time
}

func newWorkQueue() *workQueue {
	return &workQueue{
		gauge:      newGauge(),
		processing: make(map[ObjectID]struct{}),
		ready:      make(chan struct{}, 1),
		schedules:  newScheduleHub(),
	}
}

// publish hands one move to the hub. It runs *after* q.mu is released: Send
// takes the bus lock, and nesting that inside the queue's critical section would
// put a second lock on the hot path of every enqueue in the system.
//
// Two moves for one id can therefore reach Send in the reverse of the order they
// became true. The hub's Accept rule resolves that by comparing their order, so
// the slot settles the same way whichever send runs second.
//
// gobus.ErrClosed is expected rather than exceptional: Client.Requeue is public
// and reaches this queue from a user goroutine at any time, including while the
// beehive tears down, so a publish can race the sender close and be dropped.
//
// Dropping it loses nothing, and that is a property of stop rather than luck.
// A move requires the queue lock and stop sets stopped under it, so no move can
// follow the snapshot stop publishes. Anything still in flight therefore carries
// a value that snapshot already covers. See gauge.remaining.
func (q *workQueue) publish(id ObjectID, m move) {
	if !m.set {
		return
	}
	_ = q.schedules.Send(id, m.value)
}

// publishAll is publish for the one caller that moves many ids at once.
func (q *workQueue) publishAll(moves []keyedValue) {
	for _, m := range moves {
		_ = q.schedules.Send(m.ID, m.gaugeValue)
	}
}

// move is what one critical section has to publish: at most one value, for the
// one id that section touches.
//
// A section can report twice for that id. requeueNow clears an alarm and then
// marks it dirty; reported separately that is "nothing scheduled" followed by
// "due now", and the first is a state that never existed between two consistent
// points. The later report simply overwrites the earlier one, which is the whole
// of the coalescing rule.
//
// It is a value, not a container, because every site here moves exactly one id.
// The one place that moves many — stop, draining every alarm — publishes the
// gauge's own slice instead.
type move struct {
	value gaugeValue
	set   bool
}

// put records a move. Callers gate on the gauge's own report, so anything that
// reaches here did change the observable schedule.
func (m *move) put(s gaugeValue) { m.value, m.set = s, true }

// add enqueues id unless it is already queued. If id is currently being
// processed it is marked dirty instead of queued, so done re-queues it once the
// in-flight reconcile completes rather than dispatching a second one in parallel.
func (q *workQueue) add(id ObjectID) {
	var moved move
	q.mu.Lock()
	q.addLocked(id, &moved)
	q.mu.Unlock()
	q.publish(id, moved)
}

// addLocked is the shared body of add, requeueNow and timerFired. It is not a
// publish site of its own: the caller owns the critical section, so the caller
// owns the publish, and treating this as a site would emit twice for the callers
// that touch the gauge before it.
//
// The stopped check stays above the gauge call. Below it, a post-stop add would
// move the gauge and publish a due-now after stop already sent the final values.
func (q *workQueue) addLocked(id ObjectID, moved *move) {
	if q.stopped {
		return
	}
	s, ok := q.gauge.markDirty(id)
	if !ok {
		return // already queued
	}
	moved.put(s)
	if _, ok := q.processing[id]; !ok {
		q.items = append(q.items, id)
		q.signal()
	}
	// else in flight: leave it dirty; done will re-queue it, not dispatchable now.
}

func (q *workQueue) signal() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

// addAfter enqueues id after delay has elapsed. A zero or negative delay
// enqueues immediately. The timer is tracked per id so stop can cancel it (a
// torn-down queue must not be woken by a retry or a far-future RequeueAfter
// scheduled just before shutdown) and so requeueNow and the gauge can reach
// it. A second addAfter for the same id supersedes the first: the prior timer is
// cancelled so only the newest schedule fires.
func (q *workQueue) addAfter(id ObjectID, delay time.Duration) {
	if delay <= 0 {
		q.add(id) // this branch sets no alarm, so its move comes from markDirty
		return
	}
	var moved move
	q.mu.Lock()
	if !q.stopped {
		if prev := q.gauge.alarmAt(id); prev != nil {
			prev.timer.Stop() // newest schedule wins; don't let the stale one fire
		}
		a := &alarm{fireAt: time.Now().Add(delay)}
		a.timer = time.AfterFunc(delay, func() { q.timerFired(id, a) })
		if s, ok := q.gauge.setAlarm(id, a); ok {
			moved.put(s)
		}
	}
	q.mu.Unlock()
	q.publish(id, moved)
}

// timerFired runs when an alarm's timer fires. It enqueues id only if a is still
// the current schedule: a newer addAfter or a requeueNow may have replaced (or
// cleared) the slot while this already-fired timer was blocked on the lock, and
// that newer schedule — not this superseded one — owns the enqueue. Adding here
// regardless would run the work early, ignoring the newer delay.
func (q *workQueue) timerFired(id ObjectID, a *alarm) {
	var moved move
	q.mu.Lock()
	if q.gauge.alarmAt(id) == a {
		// One critical section, so a subscriber never sees the id go unscheduled
		// between the clear and the enqueue. Under a poll that window was almost
		// never observed; under push it would be observed every time.
		if s, ok := q.gauge.clearAlarm(id); ok {
			moved.put(s)
		}
		q.addLocked(id, &moved) // a no-op if stop ran between firing and here
	}
	q.mu.Unlock()
	q.publish(id, moved)
}

// requeueNow cancels any pending delayed add for id and makes it immediately
// dispatchable, in a single critical section so no schedule can interleave
// between the two. It is the queue primitive behind reconciler.requeueNow: a stale
// backoff timer is dropped and the id is requeued for immediate reconcile.
func (q *workQueue) requeueNow(id ObjectID) {
	var moved move
	q.mu.Lock()
	if a := q.gauge.alarmAt(id); a != nil {
		a.timer.Stop()
		if s, ok := q.gauge.clearAlarm(id); ok {
			moved.put(s)
		}
	}
	q.addLocked(id, &moved)
	q.mu.Unlock()
	q.publish(id, moved)
}

// scheduleAt reports id's current schedule. An id that is only being processed,
// or one a periodic pass might later pick up, reports the zero Schedule: a pass
// is kind-wide and conditional, not a per-id schedule.
func (q *workQueue) scheduleAt(id ObjectID) Schedule {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.gauge.at(id).Schedule
}

// stop quiesces the queue: it cancels every pending addAfter timer and makes all
// further adds no-ops, so no goroutine wakes the queue after the reconcile loop
// has drained. Idempotent.
func (q *workQueue) stop() {
	q.mu.Lock()
	q.stopped = true
	// Every mutator is guarded on stopped, so from here the gauge cannot move and
	// this snapshot is its final state. The alarm clears are moves; the rest is
	// the state those moves leave behind. Both go out, because the snapshot is
	// what makes a publish that races the closing sender harmless — see
	// gauge.remaining.
	final := append(q.gauge.clearAllAlarms(), q.gauge.remaining()...)
	q.mu.Unlock()
	// The final values go out before the sender closes, so a subscriber's last
	// word is what the schedule became rather than nothing at all.
	q.publishAll(final)
}

// get removes and returns the next item, moving it into the processing state
// until done is called. If more items remain it re-signals ready so the consumer
// loops back immediately. Returns false if the queue is empty.
func (q *workQueue) get() (ObjectID, bool) {
	var moved move
	q.mu.Lock()
	// Stopped dispatches nothing, for the same reason it enqueues nothing: the
	// reconcile loop has drained and the work would never be done. It also keeps
	// the gauge still after stop, which is what lets stop's snapshot be the final
	// state — see gauge.remaining. Without it this is the one mutator that could
	// move the schedule after the snapshot and lose that move to a closed sender.
	if q.stopped || len(q.items) == 0 {
		q.mu.Unlock()
		return 0, false
	}
	id := q.items[0]
	q.items = q.items[1:]
	// Dispatch clears the dirty slot: absent a future alarm, the id is now
	// unscheduled. The id is items[0] rather than a parameter, so the gauge call
	// sits here rather than in a wrapper around the method.
	if s, ok := q.gauge.clearDirty(id); ok {
		moved.put(s)
	}
	q.processing[id] = struct{}{}
	if len(q.items) > 0 {
		q.signal()
	}
	// Unlocked explicitly rather than deferred: a deferred publish would run
	// before a deferred unlock and hold q.mu across the bus lock, which is what
	// publish exists to avoid.
	q.mu.Unlock()
	q.publish(id, moved)
	return id, true
}

// done marks id's processing as complete. If id was re-added while processing,
// it is queued now so the pending change is reconciled exactly once more.
func (q *workQueue) done(id ObjectID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.processing, id)
	if q.gauge.dirtyAt(id) {
		// Re-added during processing: make it dispatchable now.
		q.items = append(q.items, id)
		q.signal()
	}
	// done calls no gauge mutator: it only moves the id between processing and
	// items, which the gauge does not read, so the schedule is unchanged.
}

// ============================================================
// The schedule: the queue's observable state, and how it is published.
// ============================================================

// gaugeValue is what the gauge reports and what the hub carries: a schedule plus
// the order it moved in.
//
// Seq is a counter, not a clock — the schedule already holds a time, and that
// time cannot serve here, because get and stop both move the gauge without
// writing one. It is assigned under workQueue.mu, so it is the *queue's* order
// and not the order two publishes happen to reach the bus in.
//
// That distinction is the whole reason it exists: a publish happens after the
// queue lock is released, so two moves can unlock in one order and be sent in the
// other. The hub's Accept rule compares Seq, so whichever send arrives second
// sees the other as prev and the slot settles the same way either way.
type gaugeValue struct {
	Schedule Schedule
	Seq      uint64
}

// keyedValue names the id a gaugeValue belongs to, for the paths that carry many
// at once: the shutdown snapshot and the publish that drains it.
type keyedValue struct {
	ID ObjectID
	gaugeValue
}

// gauge owns the two maps SchedulesWatch reports on. Nothing outside it touches
// them, so a queue operation cannot move the schedule without calling a method
// here — and every method that moves it says so.
//
// That is not a proof: it is one small type whose five mutators each return a
// report their caller must consume, plus the TestGauge* tests below that drive
// all of them. It is what stands in for the backstop this stream does not have, so keep
// the surface small and keep every mutator reporting.
//
// A future change that gives workQueue a second writer — an exported handle, a
// shared queue, a durable schedule — breaks the argument entirely and the poll
// would have to come back.
//
// The caller holds workQueue.mu for every method here, including the reads.
type gauge struct {
	// dirty maps a queued id to the moment it became due. The value is what makes
	// the schedule a gauge rather than a clock: at answers with it, so an id that
	// sits queued behind a slow reconcile reports the same time on every read,
	// where time.Now() would differ on each one and make SchedulesWatch emit
	// forever (see the schedule-watch ADR: a repeated value must be impossible).
	dirty  map[ObjectID]time.Time // queued (in items) and awaiting dispatch
	alarms map[ObjectID]*alarm    // pending delayed adds (addAfter), keyedValue by id
	seq    uint64
}

func newGauge() *gauge {
	return &gauge{
		dirty:  make(map[ObjectID]time.Time),
		alarms: make(map[ObjectID]*alarm),
	}
}

// scheduleLocked is the gauge's whole reading rule: an id queued for immediate
// dispatch is due now, otherwise a pending alarm names its fire time, otherwise
// nothing is scheduled.
//
// dirty is consulted first because "due now" is the truthful answer for an id
// that holds both — it is dispatchable, and the alarm is a later fallback rather
// than the next event. That ordering is why setAlarm on a dirty id reports no
// move: the alarm is real, but invisible.
func (g *gauge) scheduleLocked(id ObjectID) Schedule {
	if at, ok := g.dirty[id]; ok {
		return Schedule{NextRequeueAt: at}
	}
	if a := g.alarms[id]; a != nil {
		return Schedule{NextRequeueAt: a.fireAt}
	}
	return Schedule{}
}

// at reports id's schedule without moving anything.
//
// It returns no ok. The zero Schedule already means "nothing scheduled", which
// is a real value this watch delivers rather than an absence, so a second
// result would carry nothing a caller could act on — both callers of the old
// nextRequeueAt discarded it.
func (g *gauge) at(id ObjectID) gaugeValue {
	return gaugeValue{Schedule: g.scheduleLocked(id), Seq: g.seq}
}

// report stamps a move, or reports nothing when the observable schedule did not
// change. Every mutator below routes its answer through here, so "did a watcher
// see this" is decided in one place rather than at each site.
func (g *gauge) report(id ObjectID, before Schedule) (gaugeValue, bool) {
	after := g.scheduleLocked(id)
	if after == before {
		return gaugeValue{}, false
	}
	g.seq++
	return gaugeValue{Schedule: after, Seq: g.seq}, true
}

// markDirty queues id for immediate dispatch. It is a no-op for an id already
// queued, which is what keeps a burst of adds to one id at one reported move.
func (g *gauge) markDirty(id ObjectID) (gaugeValue, bool) {
	if _, ok := g.dirty[id]; ok {
		return gaugeValue{}, false
	}
	before := g.scheduleLocked(id)
	g.dirty[id] = time.Now()
	return g.report(id, before)
}

// dirtyAt reports whether id is queued for immediate dispatch.
func (g *gauge) dirtyAt(id ObjectID) bool {
	_, ok := g.dirty[id]
	return ok
}

// clearDirty drops id's queued-now slot. The id then reads as its pending alarm,
// or as unscheduled when it has none.
func (g *gauge) clearDirty(id ObjectID) (gaugeValue, bool) {
	before := g.scheduleLocked(id)
	delete(g.dirty, id)
	return g.report(id, before)
}

// setAlarm records a pending delayed add. It reports nothing when id is already
// dirty, because at reads dirty first and the alarm is therefore invisible.
func (g *gauge) setAlarm(id ObjectID, a *alarm) (gaugeValue, bool) {
	before := g.scheduleLocked(id)
	g.alarms[id] = a
	return g.report(id, before)
}

// alarmAt returns id's pending alarm, or nil. It is how the queue tests whether
// a fired timer is still the current schedule without reaching into the maps.
func (g *gauge) alarmAt(id ObjectID) *alarm { return g.alarms[id] }

// clearAlarm drops id's pending alarm without stopping its timer — the caller
// owns that, because only the caller knows whether the timer is the one firing.
func (g *gauge) clearAlarm(id ObjectID) (gaugeValue, bool) {
	before := g.scheduleLocked(id)
	delete(g.alarms, id)
	return g.report(id, before)
}

// clearAllAlarms stops every pending timer, drops every alarm, and reports the
// ids whose observable schedule moved.
//
// An id that is also dirty is *not* reported: it reads as due now before and
// after, since at consults dirty first. Reporting it would publish a Seq bump
// for a change no watcher can see — harmless, but it would break the rule every
// other mutator keeps.
func (g *gauge) clearAllAlarms() []keyedValue {
	var moved []keyedValue
	for id, a := range g.alarms {
		a.timer.Stop()
		before := g.scheduleLocked(id)
		delete(g.alarms, id)
		if s, ok := g.report(id, before); ok {
			moved = append(moved, keyedValue{ID: id, gaugeValue: s})
		}
	}
	return moved
}

// remaining reports the current schedule of every id the gauge still describes.
// Call it after clearAllAlarms, so what it returns is the ids still queued for
// immediate dispatch.
//
// Shutdown needs this on top of the moves clearAllAlarms reports, and the reason
// is a race rather than a state. An enqueue can move the gauge, release the
// queue's lock, and only then publish — so its publish can lose the race to the
// closing sender and be dropped. Report the moves alone and that subscriber would
// end on a value the queue had already left.
//
// A dropped publish loses nothing because every workQueue mutator is guarded on
// stopped, which stop sets under the same lock this runs beneath. Nothing can
// move the gauge after this snapshot, so anything still in flight can only carry
// a duplicate of it.
//
// The stamps are the gauge's current sequence rather than the one each id last
// moved at, which is what makes the snapshot idempotent: an id whose publish did
// land is rejected by the stream's own comparison rather than reported twice.
func (g *gauge) remaining() []keyedValue {
	out := make([]keyedValue, 0, len(g.dirty))
	for id := range g.dirty {
		out = append(out, keyedValue{ID: id, gaugeValue: g.at(id)})
	}
	return out
}

// scheduleHub carries the work queue's gauge to the SchedulesWatch subscribers
// of one kind.
//
// gobus/watch is a keyedValue latest-value *state* bus: one slot for each watched
// key, seeded at registration with the value the caller has just read. That is
// the right shape because SchedulesWatch is a gauge — it streams the value
// itself rather than a change. Its sibling gobus/conflate is an event bus with
// coalescing and annihilation, which is what a change stream wants and this one
// does not.
//
// scheduleSender is what workQueue needs in order to publish a move and to
// register a subscriber. The queue holds this rather than the concrete hub, so a
// test can record what it published without standing up a hub and a receiver to
// observe it.
type scheduleSender interface {
	Send(id ObjectID, s gaugeValue) error
	Watch(id ObjectID, initial gaugeValue) *watch.Receiver[ObjectID, gaugeValue]
	Close()
}

// The hub lives beside the queue it observes, and newWorkQueue builds both. A
// beehive-level hub would need the kind threaded through every queue operation,
// and it would widen the send lock's blast radius from one kind's queue to the
// whole process — which matters because a single subscriber puts every publish
// for that hub on the locked path.
//
// scheduleHub adapts watch.Hub to scheduleSender. Close is the *sender's* close,
// never the hub's: Hub.Close is hard tear-down with no drain, so a receiver that
// had not yet read the final value would lose it on a timing coin flip.
type scheduleHub struct {
	hub *watch.Hub[ObjectID, gaugeValue]
}

func (h scheduleHub) Send(id ObjectID, s gaugeValue) error { return h.hub.Sender().Send(id, s) }

func (h scheduleHub) Watch(id ObjectID, initial gaugeValue) *watch.Receiver[ObjectID, gaugeValue] {
	return h.hub.Watch(id, initial)
}

func (h scheduleHub) Close() { h.hub.Sender().Close() }

// newScheduleHub builds the hub with the rule that makes a reordered publish
// safe.
//
// A publish happens after workQueue.mu is released, so two moves can unlock in
// one order and reach Send in the other. Accept compares the queue's own order
// rather than trusting arrival: whichever send runs second sees the first's
// value as prev, so the slot settles the same way either way.
//
// The rule runs once for each receiver, against that receiver's own slot. Two
// streams on one id can be seeded at different moments, so a value can be new
// for one and old for the other, and a hub-wide answer would be wrong for one of
// them. It also runs against the value passed to Watch, which is what rejects a
// publish that predates a subscriber's snapshot.
//
// Accept runs under the bus lock and must not take a lock a caller may hold
// while calling Watch, Send or a Close: Watch is expressly safe under the
// queue's lock, so an Accept that took that lock would invert the two orders and
// deadlock. This one reads its two arguments and nothing else.
func newScheduleHub() scheduleHub {
	return scheduleHub{hub: watch.New[ObjectID](watch.WithAccept(
		func(prev, next gaugeValue) bool { return next.Seq > prev.Seq },
	))}
}

// watchSchedule registers a receiver for id, seeded with id's current schedule,
// in one critical section.
//
// The single critical section is the whole of the correctness here. Watch calls
// no caller code, so it is safe under this lock, and seeding it with the value
// read under the same lock closes the subscribe race in both directions:
//
//   - a move whose critical section ran *before* this read is already in the
//     seed, and its later publish carries a Seq at or below it, so Accept
//     rejects it and the subscriber sees no duplicate;
//   - a move whose critical section runs *after* finds the receiver registered,
//     and its Seq exceeds the seed, so nothing is lost.
//
// The bus does not deliver the seed back — it is the caller's own argument — so
// the caller reports it as the stream's first value and reads the receiver for
// what supersedes it.
// It returns the seed as well as the receiver. A caller that re-read the gauge
// afterwards would take a second critical section and reopen the race this one
// closes.
func (q *workQueue) watchSchedule(id ObjectID) (*watch.Receiver[ObjectID, gaugeValue], gaugeValue) {
	q.mu.Lock()
	defer q.mu.Unlock()
	cur := q.gauge.at(id)
	return q.schedules.Watch(id, cur), cur
}

// closeHub ends every schedule stream of this kind. Call it after stop, so the
// final values are already published and each receiver reads its last one before
// its stream ends.
func (q *workQueue) closeHub() {
	q.schedules.Close()
}
