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
	mu         sync.Mutex
	dirty      map[ObjectID]struct{} // queued (in items) and awaiting dispatch
	processing map[ObjectID]struct{} // handed out via get, not yet done
	items      []ObjectID
	ready      chan struct{}       // pulsed when items are available
	stopped    bool                // set by stop; adds become no-ops
	alarms     map[ObjectID]*alarm // pending delayed adds (addAfter), keyed by id

	onSchedule func(id ObjectID, at time.Time, scheduled bool) // fires under mu when next-requeue changes; must not re-enter the queue
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
		dirty:      make(map[ObjectID]struct{}),
		processing: make(map[ObjectID]struct{}),
		ready:      make(chan struct{}, 1),
		alarms:     make(map[ObjectID]*alarm),
	}
}

// add enqueues id unless it is already queued. If id is currently being
// processed it is marked dirty instead of queued, so done re-queues it once the
// in-flight reconcile completes rather than dispatching a second one in parallel.
func (q *workQueue) add(id ObjectID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.addLocked(id)
}

func (q *workQueue) addLocked(id ObjectID) {
	if q.stopped {
		return
	}
	if _, ok := q.dirty[id]; ok {
		return
	}
	q.dirty[id] = struct{}{}
	if _, ok := q.processing[id]; !ok {
		q.items = append(q.items, id)
		q.signal()
	}
	// else in flight: leave it dirty; done will re-queue it, not dispatchable now.
	q.emitScheduleLocked(id)
}

// emitScheduleLocked publishes id's current schedule to onSchedule. Caller holds
// mu. It carries no dedup memory: callers invoke it only at genuine transitions of
// the dirty/alarms state (which are the sole source of truth for the schedule), so
// no consecutive emit repeats a value. See addLocked/get/addAfter.
func (q *workQueue) emitScheduleLocked(id ObjectID) {
	if q.onSchedule == nil {
		return
	}
	at, scheduled := q.nextRequeueAtLocked(id)
	q.onSchedule(id, at, scheduled)
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
// scheduled just before shutdown) and so requeueNow/nextRequeueAt can reach
// it. A second addAfter for the same id supersedes the first: the prior timer is
// cancelled so only the newest schedule fires.
func (q *workQueue) addAfter(id ObjectID, delay time.Duration) {
	if delay <= 0 {
		q.add(id)
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return
	}
	if prev := q.alarms[id]; prev != nil {
		prev.timer.Stop() // newest schedule wins; don't let the stale one fire
	}
	a := &alarm{fireAt: time.Now().Add(delay)}
	a.timer = time.AfterFunc(delay, func() { q.timerFired(id, a) })
	q.alarms[id] = a
	// A due-now (dirty) id already reported due-now, which dominates this future
	// alarm (nextRequeueAt prefers queued), so only emit the fire time when the id
	// isn't already queued for immediate dispatch.
	if _, dirty := q.dirty[id]; !dirty {
		q.emitScheduleLocked(id)
	}
}

// timerFired runs when an alarm's timer fires. It enqueues id only if a is still
// the current schedule: a newer addAfter or a requeueNow may have replaced (or
// cleared) the slot while this already-fired timer was blocked on the lock, and
// that newer schedule — not this superseded one — owns the enqueue. Adding here
// regardless would run the work early, ignoring the newer delay.
func (q *workQueue) timerFired(id ObjectID, a *alarm) {
	q.mu.Lock()
	superseded := q.alarms[id] != a
	if !superseded {
		delete(q.alarms, id)
	}
	q.mu.Unlock()
	if superseded {
		return
	}
	q.add(id) // a no-op if stop ran between firing and here
}

// requeueNow cancels any pending delayed add for id and makes it immediately
// dispatchable, in a single critical section so no schedule can interleave
// between the two. It is the queue primitive behind reconciler.requeueNow: a stale
// backoff timer is dropped and the id is requeued for immediate reconcile.
func (q *workQueue) requeueNow(id ObjectID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if a := q.alarms[id]; a != nil {
		a.timer.Stop()
		delete(q.alarms, id)
	}
	q.addLocked(id)
}

// nextRequeueAt reports when id is next due to be dispatched: an id already
// queued for immediate dispatch returns now (it is due); otherwise a pending
// delayed add returns its fire time. Queued-now is checked first because an id
// can hold both — a future backoff/RequeueAfter timer plus an immediate add from
// a store change or requeue — and "due now" is the truthful answer then, not the
// stale future time. ok is false when nothing is firmly scheduled — an id that is
// only being processed, or one the periodic resync might later pick up, reports
// nothing, since resync is conditional and not a per-id schedule.
func (q *workQueue) nextRequeueAt(id ObjectID) (time.Time, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.nextRequeueAtLocked(id)
}

// subscribeSchedule runs subscribe — which registers a schedule hub receiver —
// and reads id's current next-requeue time atomically under mu. Holding mu across
// both means no emit can interleave between the snapshot and the subscription, so
// the receiver captures every change strictly after the returned snapshot (a gauge
// consumer that briefly sees a value twice just reconverges). Returns the zero time
// when nothing is scheduled — the caller folds that into an empty Schedule anyway.
func (q *workQueue) subscribeSchedule(id ObjectID, subscribe func()) time.Time {
	q.mu.Lock()
	defer q.mu.Unlock()
	subscribe()
	at, _ := q.nextRequeueAtLocked(id)
	return at
}

// nextRequeueAtLocked is the lock-free core of nextRequeueAt, shared with the
// schedule-emit path. Caller holds mu.
func (q *workQueue) nextRequeueAtLocked(id ObjectID) (time.Time, bool) {
	if _, ok := q.dirty[id]; ok {
		return time.Now(), true
	}
	if a := q.alarms[id]; a != nil {
		return a.fireAt, true
	}
	return time.Time{}, false
}

// stop quiesces the queue: it cancels every pending addAfter timer and makes all
// further adds no-ops, so no goroutine wakes the queue after the reconcile loop
// has drained. Idempotent.
func (q *workQueue) stop() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
	for _, a := range q.alarms {
		a.timer.Stop()
	}
	q.alarms = nil
}

// get removes and returns the next item, moving it into the processing state
// until done is called. If more items remain it re-signals ready so the consumer
// loops back immediately. Returns false if the queue is empty.
func (q *workQueue) get() (ObjectID, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return 0, false
	}
	id := q.items[0]
	q.items = q.items[1:]
	delete(q.dirty, id)
	q.processing[id] = struct{}{}
	if len(q.items) > 0 {
		q.signal()
	}
	// Dispatch clears the dirty slot: absent a future alarm, the id is now unscheduled.
	q.emitScheduleLocked(id)
	return id, true
}

// done marks id's processing as complete. If id was re-added while processing,
// it is queued now so the pending change is reconciled exactly once more.
func (q *workQueue) done(id ObjectID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.processing, id)
	if _, ok := q.dirty[id]; ok {
		// Re-added during processing: make it dispatchable now.
		q.items = append(q.items, id)
		q.signal()
	}
	// No schedule emit: done only moves the id between processing and items, which
	// nextRequeueAt ignores — the dirty/alarms state (hence the schedule) is unchanged.
}
