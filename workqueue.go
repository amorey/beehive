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

// workQueue is a FIFO queue of ObjectIDs with set semantics. Adding an id that is
// already queued does nothing. It is safe for concurrent use.
//
// A caller selects on ready, calls get to take the next id, and MUST call done
// when it finishes with that id. Between get and done the id is in the
// processing state, and the queue does not dispatch it again. Two workers can
// therefore never reconcile one object at the same time. An add that arrives
// while the id is processing marks the id dirty, and done queues it again, so no
// wakeup is lost. This is the standard Kubernetes work-queue discipline.
//
// The queue also reports a schedule. See "The schedule" at the foot of this
// file for what that means and how it reaches a subscriber.
type workQueue struct {
	mu sync.Mutex
	// gauge holds the state SchedulesWatch reports: which ids are queued now, and
	// which hold a pending alarm. It is separate from the queue's own bookkeeping
	// so that one type answers whether a change is visible to a subscriber.
	gauge      *gauge
	processing map[ObjectID]struct{} // handed out via get, not yet done
	items      []ObjectID
	ready      chan struct{} // pulsed when items are available
	stopped    bool          // set by stop; adds become no-ops
	// schedules carries each schedule change to this kind's subscribers. It is
	// always present. A queue exists only for a kind that has a reconciler, and
	// newWorkQueue builds the bus with the queue.
	schedules scheduleBus
}

// alarm is a pending delayed enqueue. It holds the timer that will queue the id
// and the absolute time that timer fires. The gauge reads fireAt, so it reports
// when an id is next due without deriving it from the timer.
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

// publish sends one schedule change to the bus. It runs after q.mu is released.
// Send takes the bus lock, and to hold the queue lock across it would put a
// second lock on the path of every enqueue in the system.
//
// Two changes to one id can therefore reach Send in the reverse of the order the
// queue made them. The bus resolves that: each value carries the queue's own
// order, and the bus keeps the higher one. See newScheduleHub.
//
// An error here is expected, not exceptional. Client.Requeue is public, so a user
// goroutine can reach this queue at any time, including while the beehive stops.
// A publish can therefore reach a closed bus and be dropped.
//
// A dropped publish loses nothing. stop takes a snapshot of the whole gauge, and
// no caller can change the gauge after that, so anything still in flight repeats
// a value the snapshot already carries. See gauge.finalValues.
func (q *workQueue) publish(id ObjectID, m pendingSend) {
	if !m.set {
		return
	}
	_ = q.schedules.Send(id, m.value)
}

// publishAll sends many changes. Only stop needs it.
func (q *workQueue) publishAll(moves []keyedGaugeValue) {
	for _, m := range moves {
		_ = q.schedules.Send(m.ID, m.gaugeValue)
	}
}

// pendingSend holds what one critical section owes the bus: at most one value,
// for the one id that section touches.
//
// A section can change that id twice. requeueNow drops an alarm and then queues
// the id. Sent separately those are "nothing scheduled" and then "due now", and
// the first is a state that never existed between two consistent points. The
// second put overwrites the first, so the subscriber sees only the result. That
// is the whole coalescing rule.
//
// It holds one value rather than a set, because every site below changes exactly
// one id. stop is the only caller that changes many, and it sends the gauge's own
// slice instead.
type pendingSend struct {
	value gaugeValue
	set   bool
}

// put records a change. A caller reaches here only when the gauge reported that
// the schedule changed, so nothing here needs to check again.
func (m *pendingSend) put(s gaugeValue) { m.value, m.set = s, true }

// add queues id, unless it is already queued. If a worker is processing id, add
// marks it dirty instead. done then queues it, so the queue does not dispatch a
// second reconcile beside the first.
func (q *workQueue) add(id ObjectID) {
	var pending pendingSend
	q.mu.Lock()
	q.addLocked(id, &pending)
	q.mu.Unlock()
	q.publish(id, pending)
}

// addLocked is the shared body of add, requeueNow and timerFired. It does not
// publish. The caller owns the critical section, so the caller owns the publish.
// Two of its callers change the gauge before they call it, and a publish here
// would send that intermediate state.
//
// The stopped check stays above the gauge call. Below it, an add after stop would
// change the gauge and send a due-now after stop sent the final values.
func (q *workQueue) addLocked(id ObjectID, pending *pendingSend) {
	if q.stopped {
		return
	}
	s, ok := q.gauge.markDirty(id)
	if !ok {
		return // already queued
	}
	pending.put(s)
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

// addAfter queues id once delay has elapsed. A delay of zero or less queues it
// at once.
//
// The queue tracks the timer for each id, for three reasons. stop can cancel it,
// so a torn-down queue is not woken by a retry or by a far-future RequeueAfter
// set just before shutdown. requeueNow can cancel it. And the gauge can read its
// fire time.
//
// A second addAfter for one id replaces the first. It stops the earlier timer, so
// only the newest schedule fires.
func (q *workQueue) addAfter(id ObjectID, delay time.Duration) {
	if delay <= 0 {
		q.add(id) // no alarm on this branch, so add reports the change
		return
	}
	var pending pendingSend
	q.mu.Lock()
	if !q.stopped {
		if prev := q.gauge.alarmFor(id); prev != nil {
			prev.timer.Stop() // newest schedule wins; don't let the stale one fire
		}
		a := &alarm{fireAt: time.Now().Add(delay)}
		a.timer = time.AfterFunc(delay, func() { q.timerFired(id, a) })
		if s, ok := q.gauge.setAlarm(id, a); ok {
			pending.put(s)
		}
	}
	q.mu.Unlock()
	q.publish(id, pending)
}

// timerFired runs when an alarm's timer fires. It queues id only if a is still
// the current alarm.
//
// A newer addAfter, or a requeueNow, can replace or drop that alarm while this
// timer waits for the lock. The newer schedule then owns the enqueue. To queue
// the id here anyway would run the work early and ignore the newer delay.
func (q *workQueue) timerFired(id ObjectID, a *alarm) {
	var pending pendingSend
	q.mu.Lock()
	if q.gauge.alarmFor(id) == a {
		// One critical section, so a subscriber never sees the id go unscheduled
		// between dropping the alarm and queueing it. A poll almost never caught
		// that window. A push path would report it every time.
		if s, ok := q.gauge.clearAlarm(id); ok {
			pending.put(s)
		}
		q.addLocked(id, &pending) // a no-op if stop ran between firing and here
	}
	q.mu.Unlock()
	q.publish(id, pending)
}

// requeueNow drops any pending delayed add for id and makes it dispatchable at
// once. Both happen in one critical section, so no other schedule can land
// between them.
//
// It is the queue primitive behind reconciler.requeueNow: it drops a stale
// backoff timer and queues the id to reconcile now.
func (q *workQueue) requeueNow(id ObjectID) {
	var pending pendingSend
	q.mu.Lock()
	if a := q.gauge.alarmFor(id); a != nil {
		a.timer.Stop()
		if s, ok := q.gauge.clearAlarm(id); ok {
			pending.put(s)
		}
	}
	q.addLocked(id, &pending)
	q.mu.Unlock()
	q.publish(id, pending)
}

// scheduleAt reports id's current schedule.
//
// The zero Schedule means nothing is scheduled. An id that is only being
// processed reads as zero, and so does an id that a periodic pass may later pick
// up: a pass covers a whole kind and depends on state, so it is not a schedule
// for one id.
func (q *workQueue) scheduleAt(id ObjectID) Schedule {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.gauge.at(id).Schedule
}

// stop quiesces the queue. It stops every pending timer and makes every later add
// do nothing, so no goroutine wakes the queue once the reconcile loop has
// drained. It is idempotent.
//
// It also sends each subscriber a last schedule. See gauge.finalValues.
func (q *workQueue) stop() {
	q.mu.Lock()
	q.stopped = true
	final := q.gauge.finalValues()
	q.mu.Unlock()
	// The final values go out before the bus closes. A subscriber therefore ends
	// on the schedule the queue left, not on nothing.
	q.publishAll(final)
}

// get removes and returns the next id and puts it in the processing state until
// done is called. If more ids remain, it signals ready again so the consumer
// loops straight back. It returns false when the queue is empty.
func (q *workQueue) get() (ObjectID, bool) {
	var pending pendingSend
	q.mu.Lock()
	// A stopped queue dispatches nothing, for the reason it queues nothing: the
	// reconcile loop has drained, so the work would never be done.
	//
	// The check also holds the gauge still after stop. Without it, get is the one
	// path that could change the schedule after stop took its snapshot, and that
	// change would go to a closed bus and be lost. See gauge.finalValues.
	if q.stopped || len(q.items) == 0 {
		q.mu.Unlock()
		return 0, false
	}
	id := q.items[0]
	q.items = q.items[1:]
	// Dispatch clears the dirty slot. The id then reads as its pending alarm, or
	// as unscheduled when it has none. The id is items[0] rather than a parameter,
	// so the gauge call sits here and not in a wrapper around this method.
	if s, ok := q.gauge.clearDirty(id); ok {
		pending.put(s)
	}
	q.processing[id] = struct{}{}
	if len(q.items) > 0 {
		q.signal()
	}
	// Unlock explicitly, not with defer. Deferred calls run last in first out, so
	// a deferred publish would run before a deferred unlock and hold q.mu across
	// the bus lock. publish exists to avoid exactly that.
	q.mu.Unlock()
	q.publish(id, pending)
	return id, true
}

// done marks id's processing complete. If something added id while it was
// processing, done queues it, so the queue reconciles that change exactly once
// more.
func (q *workQueue) done(id ObjectID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.processing, id)
	if q.gauge.isQueued(id) {
		// Re-added during processing: make it dispatchable now.
		q.items = append(q.items, id)
		q.signal()
	}
	// done calls no gauge method. It moves the id between processing and items,
	// and the gauge reads neither, so the schedule does not change.
}

// ============================================================
// The schedule: the queue's observable state, and how it is published.
// ============================================================

// gaugeValue is what the gauge reports and what the hub carries: a schedule plus
// the order it moved in.
//
// Seq is a counter, not a clock — the schedule already holds a time, and that
// time cannot order these: get and stop both change the schedule without writing
// one. It is assigned under workQueue.mu, so it is the *queue's* order
// and not the order two publishes happen to reach the bus in.
//
// That distinction is the whole reason it exists: a publish happens after the
// queue lock is released, so two changes can leave the lock in one order and be
// sent in the other. The hub's Accept rule compares Seq, so whichever send arrives second
// sees the other as prev and the slot settles the same way either way.
type gaugeValue struct {
	Schedule Schedule
	Seq      uint64
}

// keyedGaugeValue names the id a gaugeValue belongs to, for the two paths that
// carry many at once: the shutdown snapshot and the publish that drains it.
// Everywhere else the id is already a parameter, so the bare gaugeValue is what
// travels.
type keyedGaugeValue struct {
	ID ObjectID
	gaugeValue
}

// gauge owns the two maps SchedulesWatch reports on. Nothing outside this type
// touches them. A queue operation therefore cannot change the schedule without
// calling a method here, and every method that changes it says so.
//
// This is not a proof. It is one small type with five changing methods, each
// returning a result its caller must read, and the TestGauge tests drive all
// five. That is what stands in for the backstop this stream does not have. Keep
// the surface small, and keep every method reporting.
//
// The argument fails if workQueue ever gains a second writer: an exported handle,
// a shared queue, or a durable schedule. The poll would then have to come back.
//
// The caller holds workQueue.mu for every method here, reads included.
type gauge struct {
	// dirty maps a queued id to the moment it became due. The value is what makes
	// the schedule a gauge rather than a clock: at answers with it, so an id that
	// sits queued behind a slow reconcile reports the same time on every read,
	// where time.Now() would differ on each one and make SchedulesWatch emit
	// forever (see the schedule-watch ADR: a repeated value must be impossible).
	dirty  map[ObjectID]time.Time // queued (in items) and awaiting dispatch
	alarms map[ObjectID]*alarm    // pending delayed adds (addAfter), keyed by id
	seq    uint64
}

func newGauge() *gauge {
	return &gauge{
		dirty:  make(map[ObjectID]time.Time),
		alarms: make(map[ObjectID]*alarm),
	}
}

// scheduleOf is the whole reading rule. An id queued for immediate dispatch is
// due now. Otherwise a pending alarm gives its fire time. Otherwise nothing is
// scheduled.
//
// dirty comes first because "due now" is the true answer for an id that has both.
// The id is dispatchable, and the alarm is a later fallback rather than the next
// event. That order is why setAlarm on a queued id reports no change: the alarm
// is real, but no subscriber can see it.
func (g *gauge) scheduleOf(id ObjectID) Schedule {
	if at, ok := g.dirty[id]; ok {
		return Schedule{NextRequeueAt: at}
	}
	if a := g.alarms[id]; a != nil {
		return Schedule{NextRequeueAt: a.fireAt}
	}
	return Schedule{}
}

// at reads id's schedule and changes nothing.
//
// It returns no second result. The zero Schedule already means "nothing
// scheduled", and this watch delivers that as a real value. A bool would
// therefore tell a caller nothing it could act on. Both callers of the older
// nextRequeueAt discarded it.
func (g *gauge) at(id ObjectID) gaugeValue {
	return gaugeValue{Schedule: g.scheduleOf(id), Seq: g.seq}
}

// report answers whether a subscriber can see the change, and stamps it when it
// can. Every changing method below returns through here, so one place decides it
// and no site decides it again.
func (g *gauge) report(id ObjectID, before Schedule) (gaugeValue, bool) {
	after := g.scheduleOf(id)
	if after == before {
		return gaugeValue{}, false
	}
	g.seq++
	return gaugeValue{Schedule: after, Seq: g.seq}, true
}

// markDirty queues id for immediate dispatch. It does nothing for an id that is
// already queued, which is what keeps a burst of adds to one id at one change.
func (g *gauge) markDirty(id ObjectID) (gaugeValue, bool) {
	if _, ok := g.dirty[id]; ok {
		return gaugeValue{}, false
	}
	before := g.scheduleOf(id)
	g.dirty[id] = time.Now()
	return g.report(id, before)
}

// isQueued reports whether id is queued for immediate dispatch.
func (g *gauge) isQueued(id ObjectID) bool {
	_, ok := g.dirty[id]
	return ok
}

// clearDirty drops id's queued-now slot. The id then reads as its pending alarm,
// or as unscheduled when it has none.
func (g *gauge) clearDirty(id ObjectID) (gaugeValue, bool) {
	before := g.scheduleOf(id)
	delete(g.dirty, id)
	return g.report(id, before)
}

// setAlarm records a pending delayed add. It reports no change when id is already
// queued, because scheduleOf reads dirty first and no subscriber can see the
// alarm.
func (g *gauge) setAlarm(id ObjectID, a *alarm) (gaugeValue, bool) {
	before := g.scheduleOf(id)
	g.alarms[id] = a
	return g.report(id, before)
}

// alarmFor returns id's pending alarm, or nil. The queue uses it to test whether
// a fired timer is still the current alarm, without reaching into the maps.
func (g *gauge) alarmFor(id ObjectID) *alarm { return g.alarms[id] }

// clearAlarm drops id's pending alarm and does not stop its timer. The caller
// stops it, because only the caller knows whether that timer is the one firing.
func (g *gauge) clearAlarm(id ObjectID) (gaugeValue, bool) {
	before := g.scheduleOf(id)
	delete(g.alarms, id)
	return g.report(id, before)
}

// finalValues quiesces the gauge and returns the last schedule of every id it
// describes. stop calls it, and what it returns is the last thing each subscriber
// sees.
//
// It stops every timer and drops every alarm. An id that only had an alarm is now
// unscheduled. An id that is queued keeps its due-now, because dropping alarms
// dequeues nothing.
//
// It answers for every id it described, not only the ids it changed. That is what
// makes a publish that races the closing bus harmless:
//
//   - A caller changes the gauge, releases the queue lock, and publishes after
//     it. A publish can therefore reach the bus after the bus closed, and be
//     dropped.
//   - No caller can change the gauge once stop has run, because every path that
//     changes it checks stopped under the lock this method runs beneath.
//   - A dropped publish therefore repeats a value this snapshot also carries.
//
// Answer for the changed ids alone, and that subscriber ends on a schedule the
// queue had already left.
//
// The Seq advances once, so every value here outranks whatever a subscriber
// holds. Without that step, an id that nothing else changed would carry the Seq
// it was last published at, and the bus would reject its own last value.
//
// Two subscribers can then be told the same schedule twice: once by the racing
// publish and once by this snapshot. That is safe. The stream compares the two
// schedules, finds them equal, and sends nothing.
func (g *gauge) finalValues() []keyedGaugeValue {
	// Every id the gauge describes, taken before the alarms go: an id that was
	// only alarmed becomes unscheduled, and its subscriber has to be told.
	described := make(map[ObjectID]struct{}, len(g.alarms)+len(g.dirty))
	for id, a := range g.alarms {
		a.timer.Stop()
		described[id] = struct{}{}
	}
	for id := range g.dirty {
		described[id] = struct{}{}
	}
	clear(g.alarms)

	// One step, so every value here supersedes what a subscriber holds. Without
	// it an id whose schedule nothing else moved would carry the Seq it was last
	// published at, and Accept would reject its own final value.
	g.seq++

	out := make([]keyedGaugeValue, 0, len(described))
	for id := range described {
		out = append(out, keyedGaugeValue{ID: id, gaugeValue: g.at(id)})
	}
	return out
}

// scheduleBus is what workQueue needs from the bus: publish a change, register a
// subscriber, and end every stream. The queue holds this interface rather than
// the concrete hub, so a test can record what the queue published without
// building a hub and a receiver to watch it.
type scheduleBus interface {
	Send(id ObjectID, s gaugeValue) error
	Watch(id ObjectID, initial gaugeValue) *watch.Receiver[ObjectID, gaugeValue]
	Close()
}

// scheduleHub adapts gobus/watch to scheduleBus.
//
// watch is a keyed latest-value *state* bus: one slot for each watched key,
// seeded at registration with the value the caller has just read. That fits,
// because SchedulesWatch reports a value rather than a change. Its sibling
// gobus/conflate is an event bus with coalescing and annihilation, which is what
// a change stream wants and this stream does not.
//
// The hub sits beside the queue it reports on, and newWorkQueue builds both. One
// hub for the whole beehive would have to carry the kind through every queue
// operation. It would also widen the reach of the bus lock from one kind's queue
// to the whole process, and that lock is taken on every publish once any
// subscriber exists.
//
// Close closes the *sender*, never the hub. watch.Hub.Close is a hard tear-down
// with no drain, so a receiver that had not yet read the last value would lose it
// on a timing race.
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
// A publish runs after workQueue.mu is released, so two changes can leave the
// lock in one order and reach Send in the other. Accept compares the queue's own
// order instead of trusting arrival order. Whichever send runs second sees the
// other value as prev, so the slot ends the same way either way.
//
// Accept runs once for each receiver, against that receiver's own slot. Two
// streams on one id can be seeded at different moments, so one value can be new
// for one stream and old for the other. One answer for the whole hub would be
// wrong for one of them.
//
// It also runs against the value passed to Watch. That is what rejects a publish
// made before a subscriber took its snapshot.
//
// Accept runs under the bus lock. It must not take a lock that a caller may hold
// while calling Watch, Send or Close. watchSchedule calls Watch under the queue
// lock, so an Accept that took the queue lock would invert the two orders and
// deadlock. This one reads its two arguments and nothing else.
func newScheduleHub() scheduleHub {
	return scheduleHub{hub: watch.New[ObjectID](watch.WithAccept(
		func(prev, next gaugeValue) bool { return next.Seq > prev.Seq },
	))}
}

// watchSchedule registers a receiver for id and seeds it with id's current
// schedule. Both happen in one critical section.
//
// The single critical section is the whole of the correctness here. Watch calls
// no caller code, so it is safe to call under this lock. Seeding it with the
// value read under the same lock closes the subscribe race in both directions:
//
//   - A change made before this read is already in the seed. Its later publish
//     carries a Seq at or below the seed, so Accept rejects it and the
//     subscriber sees no duplicate.
//   - A change made after this read finds the receiver registered, and its Seq
//     is above the seed, so nothing is lost.
//
// It returns the seed with the receiver. A caller that read the gauge again
// afterwards would take a second critical section and reopen the race this one
// closes.
//
// The bus does not deliver the seed back, because it is the caller's own
// argument. The caller reports it as the stream's first value, then reads the
// receiver for whatever replaces it.
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
