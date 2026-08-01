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
	// See gauge.go.
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
// absolute time it fires, so nextRequeueAt can report when an id is next due
// without re-deriving it from the timer.
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
// became true. The hub's Accept rule resolves that by comparing the stamps, so
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
func (q *workQueue) publishAll(moves []keyed) {
	for _, m := range moves {
		_ = q.schedules.Send(m.ID, m.stamped)
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
	value stamped
	set   bool
}

// put records a move. Callers gate on the gauge's own report, so anything that
// reaches here did change the observable schedule.
func (m *move) put(s stamped) { m.value, m.set = s, true }

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
