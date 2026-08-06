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

	"github.com/amorey/beehive/internal/rategate"
	"github.com/amorey/gobus/watch"
)

// workQueue is a FIFO queue of ObjectIDs with set semantics, safe for
// concurrent use. A caller selects on ready, calls get, and MUST call done when
// finished. Between get and done the id is processing and never dispatched
// again; an add that arrives meanwhile marks it dirty, and done re-queues it —
// the standard Kubernetes work-queue discipline.
//
// The queue also reports a schedule; see "The schedule" below.
type workQueue struct {
	mu sync.Mutex
	// gauge holds the state SchedulesWatch reports: which ids are queued now and
	// which hold a pending alarm.
	gauge      *gauge
	processing map[ObjectID]struct{} // handed out via get, not yet done
	items      []ObjectID
	ready      chan struct{} // pulsed when items are available
	stopped    bool          // set by stop; adds become no-ops
	// gate floors the gap between two dispatches of one id. Always non-nil; a
	// zero interval holds nothing, which is how the floor is turned off.
	gate *rategate.Gate[ObjectID]
	// schedules carries each schedule change to this kind's subscribers.
	schedules scheduleBus
}

// alarm is a pending delayed enqueue: the timer that will queue the id and the
// absolute time it fires (so the gauge can report it).
type alarm struct {
	timer  *time.Timer
	fireAt time.Time
	kind   alarmKind
}

// alarmKind says why an alarm exists. It decides whether an arriving add is
// absorbed by the alarm and whether a later addAfter may replace it.
type alarmKind uint8

const (
	// First, so a literal that misses the field never absorbs an add.
	alarmRequeueAfter alarmKind = iota // Result.RequeueAfter: the controller's own schedule
	alarmBackoff                       // reconciler.backoffNext: the pass failed
	alarmFloor                         // the re-enqueue floor: the id ran too recently
)

// absorbsAdd reports whether a is already the next dispatch, so an arriving
// wake needs no enqueue of its own. Nil-safe: no alarm absorbs nothing.
func (a *alarm) absorbsAdd() bool {
	return a != nil && (a.kind == alarmBackoff || a.kind == alarmFloor)
}

// outranks reports whether a survives an addAfter of kind incoming firing at
// fireAt, dropping the newcomer instead of being replaced by it.
func (a *alarm) outranks(incoming alarmKind, fireAt time.Time) bool {
	switch {
	case a == nil:
		return false
	case incoming == alarmBackoff:
		// A backoff always takes the slot: the ladder owns the retry and is
		// meant to push the dispatch out.
		return false
	case a.kind == alarmFloor || incoming == alarmFloor:
		// Earlier fire time wins, so a held wake never delays work already
		// scheduled sooner and is never dropped by later work. A tie keeps the
		// pending alarm — the oldest-wins rule the floor needs.
		return !a.fireAt.After(fireAt)
	default:
		// Two controller schedules arbitrate as they always have: newest wins.
		return false
	}
}

func newWorkQueue() *workQueue {
	return &workQueue{
		gauge:      newGauge(),
		processing: make(map[ObjectID]struct{}),
		ready:      make(chan struct{}, 1),
		gate:       rategate.New[ObjectID](0),
		schedules:  newScheduleHub(),
	}
}

// publish sends one schedule change to the bus, after q.mu is released — Send
// takes the bus lock, and holding both would put a second lock on every
// enqueue. Two changes to one id can therefore reach Send reordered; each value
// carries the queue's own Seq and the bus keeps the higher one. An error is
// expected: a publish can race the closing bus and be dropped, which loses
// nothing because stop's snapshot already carries the value (see
// gauge.finalValues).
func (q *workQueue) publish(id ObjectID, m pendingSend) {
	if !m.set {
		return
	}
	_ = q.schedules.Send(id, m.value)
}

// publishAll sends many changes; only stop needs it.
func (q *workQueue) publishAll(moves []keyedGaugeValue) {
	for _, m := range moves {
		_ = q.schedules.Send(m.ID, m.gaugeValue)
	}
}

// pendingSend holds what one critical section owes the bus: at most one value,
// for the one id that section touches. A section that changes the id twice
// (requeueNow drops an alarm, then queues) overwrites the first value, so the
// subscriber sees only the result — the coalescing rule.
type pendingSend struct {
	value gaugeValue
	set   bool
}

// put records a change the gauge already confirmed.
func (m *pendingSend) put(s gaugeValue) { m.value, m.set = s, true }

// addMode selects whether a pending alarm may absorb an add. A wake may be
// absorbed; an alarm firing and an explicit requeue may not.
type addMode uint8

const (
	addThrottled addMode = iota
	addImmediate
)

// add queues id, unless it is already queued or a pending alarm absorbs it. If
// a worker is processing id, it is marked dirty instead and done queues it.
func (q *workQueue) add(id ObjectID) {
	var pending pendingSend
	q.mu.Lock()
	q.addLocked(id, &pending, addThrottled)
	q.mu.Unlock()
	q.publish(id, pending)
}

// addLocked is the shared body of add, requeueNow and timerFired. It does not
// publish: the caller owns the critical section, so the caller owns the
// publish. The stopped check stays above the gauge call, or an add after stop
// would send a due-now after the final values.
func (q *workQueue) addLocked(id ObjectID, pending *pendingSend, mode addMode) {
	if q.stopped {
		return
	}
	// A read, not markDirty yet: only an add that clears the throttle checks
	// below may queue the id.
	if q.gauge.isQueued(id) {
		return
	}
	if mode == addThrottled {
		if q.gauge.alarmFor(id).absorbsAdd() {
			return // the alarm owns the dispatch; oldest wins
		}
		if opensAt, held := q.gate.OpensAt(id, time.Now()); held {
			q.addAfterLocked(id, opensAt, alarmFloor, pending)
			return
		}
	}
	s, _ := q.gauge.markDirty(id)
	pending.put(s)
	if _, ok := q.processing[id]; !ok {
		q.items = append(q.items, id)
		q.signal()
	}
	// else in flight: leave it dirty; done will re-queue it.
}

// setFloor sets the minimum gap between two dispatches of one id; <= 0 turns
// the floor off. Call before the queue is in use.
func (q *workQueue) setFloor(d time.Duration) {
	q.gate = rategate.New[ObjectID](d)
}

// forget ends id's processing and drops everything the queue holds for it: a
// re-add that arrived mid-pass, a pending alarm and the floor entry. For an id
// nothing can act on again — ids are never reused, so a row the pass collected
// can only read back ErrNotFound.
func (q *workQueue) forget(id ObjectID) {
	var pending pendingSend
	q.mu.Lock()
	// Above the gauge call, as in get: stop has already published its finals, and
	// a later report would outrank them on seq.
	if q.stopped {
		q.mu.Unlock()
		return
	}
	if a := q.gauge.alarmFor(id); a != nil {
		a.timer.Stop()
		if s, ok := q.gauge.clearAlarm(id); ok {
			pending.put(s)
		}
	}
	if s, ok := q.gauge.clearDirty(id); ok {
		pending.put(s)
	}
	q.gate.Forget(id)
	delete(q.processing, id)
	q.mu.Unlock()
	q.publish(id, pending)
}

func (q *workQueue) signal() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

// addAfter queues id once delay has elapsed; delay <= 0 queues at once. The
// timer is tracked per id so stop and requeueNow can cancel it and the gauge
// can read its fire time. A second addAfter for one id replaces the first,
// except where the pending alarm outranks it.
func (q *workQueue) addAfter(id ObjectID, delay time.Duration, kind alarmKind) {
	if delay <= 0 {
		q.add(id)
		return
	}
	var pending pendingSend
	q.mu.Lock()
	q.addAfterLocked(id, time.Now().Add(delay), kind, &pending)
	q.mu.Unlock()
	q.publish(id, pending)
}

// addAfterLocked is addAfter's body, and the floor's: the throttle sets its
// alarm from inside addLocked, which already holds q.mu. It does not publish;
// the caller owns the critical section, so the caller owns the publish.
func (q *workQueue) addAfterLocked(id ObjectID, fireAt time.Time, kind alarmKind, pending *pendingSend) {
	prev := q.gauge.alarmFor(id)
	if q.stopped || prev.outranks(kind, fireAt) {
		return
	}
	if prev != nil {
		prev.timer.Stop() // newest schedule wins
	}
	a := &alarm{fireAt: fireAt, kind: kind}
	a.timer = time.AfterFunc(time.Until(fireAt), func() { q.timerFired(id, a) })
	if s, ok := q.gauge.setAlarm(id, a); ok {
		pending.put(s)
	}
}

// timerFired queues id only if a is still the current alarm — a newer addAfter
// or a requeueNow that replaced it while this timer waited for the lock owns
// the enqueue instead.
func (q *workQueue) timerFired(id ObjectID, a *alarm) {
	var pending pendingSend
	q.mu.Lock()
	if q.gauge.alarmFor(id) == a {
		// One critical section, so a subscriber never sees the id go
		// unscheduled between dropping the alarm and queueing it.
		if s, ok := q.gauge.clearAlarm(id); ok {
			pending.put(s)
		}
		if _, inFlight := q.processing[id]; inFlight && a.kind == alarmFloor {
			// The floor bounds the gap between dispatches and the last one has
			// not finished, so the window has not started. Marking the id dirty
			// here would let done queue it ahead of the alarm the pass sets a
			// line later, losing a failing pass its ladder.
			q.addAfterLocked(id, time.Now().Add(q.gate.Interval()), alarmFloor, &pending)
		} else {
			q.addLocked(id, &pending, addImmediate) // no-op if stop ran between firing and here
		}
	}
	q.mu.Unlock()
	q.publish(id, pending)
}

// requeueNow drops any pending delayed add for id and makes it dispatchable at
// once, in one critical section. The primitive behind reconciler.requeueNow.
func (q *workQueue) requeueNow(id ObjectID) {
	var pending pendingSend
	q.mu.Lock()
	if a := q.gauge.alarmFor(id); a != nil {
		a.timer.Stop()
		if s, ok := q.gauge.clearAlarm(id); ok {
			pending.put(s)
		}
	}
	q.addLocked(id, &pending, addImmediate)
	q.mu.Unlock()
	q.publish(id, pending)
}

// scheduleAt reports id's current schedule; the zero Schedule means nothing is
// scheduled (an id that is only processing reads as zero too).
func (q *workQueue) scheduleAt(id ObjectID) Schedule {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.gauge.at(id).Schedule
}

// stop quiesces the queue: every pending timer stops and every later add is a
// no-op. Idempotent. It also sends each subscriber a last schedule (see
// gauge.finalValues).
func (q *workQueue) stop() {
	q.mu.Lock()
	q.stopped = true
	final := q.gauge.finalValues()
	q.mu.Unlock()
	// The final values go out before the bus closes, so a subscriber ends on
	// the schedule the queue left.
	q.publishAll(final)
}

// get removes and returns the next id and puts it in the processing state until
// done. Returns false when the queue is empty or stopped — the stopped check
// also keeps the gauge still after stop took its snapshot.
func (q *workQueue) get() (ObjectID, bool) {
	var pending pendingSend
	q.mu.Lock()
	if q.stopped || len(q.items) == 0 {
		q.mu.Unlock()
		return 0, false
	}
	id := q.items[0]
	q.items = q.items[1:]
	q.gate.Admit(id, time.Now())
	// Dispatch clears the dirty slot; the id then reads as its pending alarm or
	// as unscheduled.
	if s, ok := q.gauge.clearDirty(id); ok {
		pending.put(s)
	}
	q.processing[id] = struct{}{}
	if len(q.items) > 0 {
		q.signal()
	}
	// Explicit unlock: a deferred publish would run before a deferred unlock
	// and hold q.mu across the bus lock.
	q.mu.Unlock()
	q.publish(id, pending)
	return id, true
}

// done marks id's processing complete, queueing it again if something added it
// while it was processing. No gauge call: moving between processing and items
// does not change the schedule.
func (q *workQueue) done(id ObjectID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.processing, id)
	if q.gauge.isQueued(id) {
		q.items = append(q.items, id)
		q.signal()
	}
}

// ============================================================
// The schedule: the queue's observable state, and how it is published.
// ============================================================

// gaugeValue is what the gauge reports and the hub carries: a schedule plus the
// order it moved in. Seq is assigned under workQueue.mu, so it is the queue's
// order, not the order two publishes happen to reach the bus in — which is the
// whole reason it exists.
type gaugeValue struct {
	Schedule Schedule
	Seq      uint64
}

// keyedGaugeValue names the id a gaugeValue belongs to, for the two paths that
// carry many at once (the shutdown snapshot and its publish).
type keyedGaugeValue struct {
	ID ObjectID
	gaugeValue
}

// gauge owns the two maps SchedulesWatch reports on. Nothing outside this type
// touches them, and every changing method returns a result its caller must
// read — that discipline is what stands in for the backstop this stream does
// not have. If workQueue ever gains a second writer, the poll has to come back
// (see the schedule-watch ADR). The caller holds workQueue.mu for every method,
// reads included.
type gauge struct {
	// dirty maps a queued id to the moment it became due. Storing the time is
	// what makes the schedule a gauge: a queued id reports the same time on
	// every read, where time.Now() would emit forever.
	dirty  map[ObjectID]time.Time
	alarms map[ObjectID]*alarm // pending delayed adds, keyed by id
	seq    uint64
}

func newGauge() *gauge {
	return &gauge{
		dirty:  make(map[ObjectID]time.Time),
		alarms: make(map[ObjectID]*alarm),
	}
}

// scheduleOf is the reading rule: queued-now wins, then a pending alarm, then
// nothing. dirty first is why setAlarm on a queued id reports no change.
func (g *gauge) scheduleOf(id ObjectID) Schedule {
	if at, ok := g.dirty[id]; ok {
		return Schedule{NextRequeueAt: at}
	}
	if a := g.alarms[id]; a != nil {
		return Schedule{NextRequeueAt: a.fireAt}
	}
	return Schedule{}
}

// at reads id's schedule and changes nothing. No second result: the zero
// Schedule is itself a real value this watch delivers.
func (g *gauge) at(id ObjectID) gaugeValue {
	return gaugeValue{Schedule: g.scheduleOf(id), Seq: g.seq}
}

// report decides in one place whether a subscriber can see the change, and
// stamps it when it can.
func (g *gauge) report(id ObjectID, before Schedule) (gaugeValue, bool) {
	after := g.scheduleOf(id)
	if after == before {
		return gaugeValue{}, false
	}
	g.seq++
	return gaugeValue{Schedule: after, Seq: g.seq}, true
}

// markDirty queues id for immediate dispatch; a no-op for an id already queued.
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

// clearDirty drops id's queued-now slot.
func (g *gauge) clearDirty(id ObjectID) (gaugeValue, bool) {
	before := g.scheduleOf(id)
	delete(g.dirty, id)
	return g.report(id, before)
}

// setAlarm records a pending delayed add. It reports no change when id is
// already queued, because scheduleOf reads dirty first.
func (g *gauge) setAlarm(id ObjectID, a *alarm) (gaugeValue, bool) {
	before := g.scheduleOf(id)
	g.alarms[id] = a
	return g.report(id, before)
}

// alarmFor returns id's pending alarm, or nil.
func (g *gauge) alarmFor(id ObjectID) *alarm { return g.alarms[id] }

// clearAlarm drops id's pending alarm without stopping its timer — only the
// caller knows whether that timer is the one firing.
func (g *gauge) clearAlarm(id ObjectID) (gaugeValue, bool) {
	before := g.scheduleOf(id)
	delete(g.alarms, id)
	return g.report(id, before)
}

// finalValues quiesces the gauge and returns the last schedule of every id it
// describes — every id, not only the changed ones, so a publish dropped by the
// closing bus only ever repeats a value this snapshot already carries. Seq
// advances once so every value here outranks whatever a subscriber holds; a
// duplicate delivery is safe because the stream compares schedules and sends
// nothing for an equal one.
func (g *gauge) finalValues() []keyedGaugeValue {
	// Collect ids before dropping alarms: an id that was only alarmed becomes
	// unscheduled, and its subscriber has to be told.
	described := make(map[ObjectID]struct{}, len(g.alarms)+len(g.dirty))
	for id, a := range g.alarms {
		a.timer.Stop()
		described[id] = struct{}{}
	}
	for id := range g.dirty {
		described[id] = struct{}{}
	}
	clear(g.alarms)

	g.seq++

	out := make([]keyedGaugeValue, 0, len(described))
	for id := range described {
		out = append(out, keyedGaugeValue{ID: id, gaugeValue: g.at(id)})
	}
	return out
}

// scheduleBus is what workQueue needs from the bus, an interface so a test can
// record publishes without building a hub.
type scheduleBus interface {
	Send(id ObjectID, s gaugeValue) error
	Watch(id ObjectID, initial gaugeValue) *watch.Receiver[ObjectID, gaugeValue]
	Close()
}

// scheduleHub adapts gobus/watch — a keyed latest-value state bus, which fits a
// stream that reports a value rather than a change — to scheduleBus. One hub
// per queue: a process-wide hub would carry the kind through every operation
// and widen the bus lock to the whole process. Close, and the close discipline
// behind it, come from watchHub.
type scheduleHub struct {
	watchHub[ObjectID, gaugeValue]
}

func (h scheduleHub) Send(id ObjectID, s gaugeValue) error { return h.send(id, s) }

// The queue always builds its hub, so the zero-hub case watchHub reports cannot
// arise here and scheduleBus need not carry it.
func (h scheduleHub) Watch(id ObjectID, initial gaugeValue) *watch.Receiver[ObjectID, gaugeValue] {
	rx, _ := h.watch(id, initial)
	return rx
}

// newScheduleHub builds the hub with the Accept rule that makes a reordered
// publish safe: compare the queue's own Seq instead of trusting arrival order.
// Accept runs under the bus lock and must take no other lock — watchSchedule
// calls Watch under the queue lock, so an Accept that took the queue lock would
// deadlock.
func newScheduleHub() scheduleHub {
	return scheduleHub{watchHub[ObjectID, gaugeValue]{hub: watch.New[ObjectID](watch.WithAccept(
		func(prev, next gaugeValue) bool { return next.Seq > prev.Seq },
	))}}
}

// watchSchedule registers a receiver for id and seeds it with id's current
// schedule, in one critical section — which closes the subscribe race in both
// directions: a change made before the read is in the seed and its publish is
// rejected by Seq; a change made after finds the receiver registered. The bus
// does not deliver the seed back; the caller reports it as the stream's first
// value.
func (q *workQueue) watchSchedule(id ObjectID) (*watch.Receiver[ObjectID, gaugeValue], gaugeValue) {
	q.mu.Lock()
	defer q.mu.Unlock()
	cur := q.gauge.at(id)
	return q.schedules.Watch(id, cur), cur
}

// closeHub ends every schedule stream of this kind. Call it after stop, so each
// receiver reads its final value before its stream ends.
func (q *workQueue) closeHub() {
	q.schedules.Close()
}
