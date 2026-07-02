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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkQueueGetEmpty(t *testing.T) {
	q := newWorkQueue()
	_, ok := q.get()
	assert.False(t, ok)
}

func TestWorkQueueFIFO(t *testing.T) {
	q := newWorkQueue()
	q.add(1)
	q.add(2)
	q.add(3)

	id, ok := q.get()
	require.True(t, ok)
	assert.Equal(t, ObjectID(1), id)

	id, ok = q.get()
	require.True(t, ok)
	assert.Equal(t, ObjectID(2), id)

	id, ok = q.get()
	require.True(t, ok)
	assert.Equal(t, ObjectID(3), id)

	_, ok = q.get()
	assert.False(t, ok)
}

func TestWorkQueueDedup(t *testing.T) {
	q := newWorkQueue()
	q.add(42)
	q.add(42) // duplicate — must be ignored
	q.add(42)

	id, ok := q.get()
	require.True(t, ok)
	assert.Equal(t, ObjectID(42), id)

	_, ok = q.get()
	assert.False(t, ok, "duplicate adds must not produce extra items")
}

func TestWorkQueueReadySignaledOnAdd(t *testing.T) {
	q := newWorkQueue()

	// No signal before any add.
	select {
	case <-q.ready:
		t.Fatal("ready signaled on empty queue")
	default:
	}

	q.add(1)

	select {
	case <-q.ready:
	default:
		t.Fatal("ready not signaled after add")
	}
}

func TestWorkQueueReadyResignaledWhenItemsRemain(t *testing.T) {
	q := newWorkQueue()
	q.add(1)
	q.add(2)

	// Drain the initial signal and get the first item.
	<-q.ready
	id, ok := q.get()
	require.True(t, ok)
	assert.Equal(t, ObjectID(1), id)

	// get() must have re-signaled ready because item 2 remains.
	select {
	case <-q.ready:
	default:
		t.Fatal("ready not re-signaled after get when items remain")
	}
}

func TestWorkQueueReadyNotRepeatedWhenQueueDrained(t *testing.T) {
	q := newWorkQueue()
	q.add(1)

	<-q.ready
	_, _ = q.get() // drain

	// No extra signal after draining.
	select {
	case <-q.ready:
		t.Fatal("ready signaled on empty queue after drain")
	default:
	}
}

func TestWorkQueueAddAfter(t *testing.T) {
	q := newWorkQueue()
	q.addAfter(1, 20*time.Millisecond)

	// Not immediately available.
	_, ok := q.get()
	assert.False(t, ok)

	// Available after the delay fires.
	select {
	case <-q.ready:
	case <-time.After(2 * time.Second):
		t.Fatal("item not delivered after delay")
	}
	id, ok := q.get()
	require.True(t, ok)
	assert.Equal(t, ObjectID(1), id)
}

// TestWorkQueueStopCancelsPendingTimers verifies stop cancels timers scheduled
// by addAfter so they never fire on a dead queue, and that adds after stop are
// no-ops — so a stopped reconciler fully quiesces instead of leaking timers that
// keep calling add (up to a RequeueAfter that could be hours out).
func TestWorkQueueStopCancelsPendingTimers(t *testing.T) {
	q := newWorkQueue()
	q.addAfter(1, time.Hour) // would fire long after the queue is dead
	q.stop()

	select {
	case <-q.ready:
		t.Fatal("ready signaled after stop; timer was not cancelled")
	default:
	}
	_, ok := q.get()
	assert.False(t, ok, "no item should be queued after stop cancels the timer")

	// Adds after stop must not enqueue.
	q.add(2)
	q.addAfter(3, 0)
	_, ok = q.get()
	assert.False(t, ok, "add/addAfter after stop must not enqueue")
}

// TestWorkQueueAddAfterOnStoppedQueue verifies addAfter is a no-op once the queue
// is stopped: a positive-delay schedule arriving after stop must not register a
// timer or enqueue, so a torn-down queue stays quiesced.
func TestWorkQueueAddAfterOnStoppedQueue(t *testing.T) {
	q := newWorkQueue()
	q.stop()

	q.addAfter(1, time.Hour)

	assert.Nil(t, q.alarms, "stopped queue must not track a new timer")
	_, ok := q.get()
	assert.False(t, ok, "addAfter on a stopped queue must not enqueue")
}

func TestWorkQueueAddAfterZeroDelay(t *testing.T) {
	q := newWorkQueue()
	q.addAfter(1, 0)

	// Zero delay must enqueue immediately (same as add).
	select {
	case <-q.ready:
	default:
		t.Fatal("item not enqueued immediately for zero delay")
	}
}

// TestWorkQueueNoConcurrentDispatch verifies that an ID handed out by get() is
// not dispatchable again until done() is called, even if it is re-added while
// still being processed. This is what prevents two workers from reconciling the
// same object concurrently.
func TestWorkQueueNoConcurrentDispatch(t *testing.T) {
	q := newWorkQueue()
	q.add(1)

	id, ok := q.get() // worker A takes 1; it is now "processing"
	require.True(t, ok)
	require.Equal(t, ObjectID(1), id)

	// A live event re-enqueues 1 while worker A is still reconciling it.
	q.add(1)

	// 1 must NOT be dispatchable to a second worker until A calls done.
	_, ok = q.get()
	assert.False(t, ok, "id must not be dispatched again while still processing")

	// Once A finishes, the queued re-add becomes dispatchable exactly once.
	q.done(1)
	id, ok = q.get()
	require.True(t, ok)
	assert.Equal(t, ObjectID(1), id)

	q.done(1)
	_, ok = q.get()
	assert.False(t, ok, "no spurious re-dispatch after done")
}

// TestWorkQueueReaddAfterDone verifies an ID can be queued again once its prior
// processing has completed via done().
func TestWorkQueueReaddAfterDone(t *testing.T) {
	q := newWorkQueue()
	q.add(7)
	_, _ = q.get() // 7 is now processing
	q.done(7)      // processing complete

	// Same ID can be added again once it's been completed.
	q.add(7)
	id, ok := q.get()
	require.True(t, ok)
	assert.Equal(t, ObjectID(7), id)
}

// TestWorkQueueNextRequeueAtEmpty verifies an unknown ID reports nothing
// scheduled.
func TestWorkQueueNextRequeueAtEmpty(t *testing.T) {
	q := newWorkQueue()
	_, ok := q.nextRequeueAt(1)
	assert.False(t, ok, "unknown id must report nothing scheduled")
}

// TestWorkQueueNextRequeueAtDispatchable verifies an ID queued for immediate
// dispatch reports a due-now time (not after now).
func TestWorkQueueNextRequeueAtDispatchable(t *testing.T) {
	q := newWorkQueue()
	q.add(1)

	at, ok := q.nextRequeueAt(1)
	require.True(t, ok, "queued id must report as scheduled")
	assert.False(t, at.After(time.Now()), "a queued-now id is due now, not in the future")
}

// TestWorkQueueNextRequeueAtAfter verifies a delayed add reports its future fire
// time.
func TestWorkQueueNextRequeueAtAfter(t *testing.T) {
	q := newWorkQueue()
	q.addAfter(1, time.Hour)

	at, ok := q.nextRequeueAt(1)
	require.True(t, ok, "delayed id must report as scheduled")
	assert.True(t, at.After(time.Now().Add(time.Minute)), "fire time must be ~1h out, got %s", at)
}

// TestWorkQueueAddAfterNewestWins verifies a second addAfter for the same id
// supersedes the first: the reported fire time is the newer one and only one
// timer remains.
func TestWorkQueueAddAfterNewestWins(t *testing.T) {
	q := newWorkQueue()
	q.addAfter(1, time.Hour)
	q.addAfter(1, 3*time.Hour)

	at, ok := q.nextRequeueAt(1)
	require.True(t, ok)
	assert.True(t, at.After(time.Now().Add(2*time.Hour)), "newest schedule must win, got %s", at)
}

// TestWorkQueueNextRequeueAtPrefersQueued verifies that when an id has both a
// future delayed schedule and an immediate enqueue (e.g. a pending backoff timer
// plus a store-change add), nextRequeueAt reports it as due now — not at the
// stale future time, which would contradict "now if already queued".
func TestWorkQueueNextRequeueAtPrefersQueued(t *testing.T) {
	q := newWorkQueue()
	q.addAfter(1, time.Hour) // future backoff/RequeueAfter timer
	q.add(1)                 // ...then enqueued immediately

	at, ok := q.nextRequeueAt(1)
	require.True(t, ok)
	assert.False(t, at.After(time.Now()), "a queued-now id is due now, not the future timer; got %s", at)
}

// TestWorkQueueSupersededTimerDoesNotEnqueue verifies a delayed-add timer whose
// slot was replaced (by a newer addAfter or requeueNow) does not enqueue the id
// when it finally fires: the newest schedule owns the enqueue, so a stale timer
// that already fired but lost the race for the lock must not run the work early.
func TestWorkQueueSupersededTimerDoesNotEnqueue(t *testing.T) {
	q := newWorkQueue()
	stale := &alarm{}
	q.alarms[1] = &alarm{} // a newer schedule now occupies the slot

	q.timerFired(1, stale) // the superseded timer fires late

	_, ok := q.get()
	assert.False(t, ok, "a superseded timer must not enqueue the id")
	assert.NotNil(t, q.alarms[1], "the newer schedule must be left intact")
}

// TestWorkQueueTimerFiredEnqueues verifies a current (non-superseded) timer
// clears its slot and enqueues the id when it fires.
func TestWorkQueueTimerFiredEnqueues(t *testing.T) {
	q := newWorkQueue()
	a := &alarm{}
	q.alarms[1] = a

	q.timerFired(1, a)

	assert.Nil(t, q.alarms[1], "firing must clear the schedule slot")
	id, ok := q.get()
	require.True(t, ok, "a current timer must enqueue the id")
	assert.Equal(t, ObjectID(1), id)
}

// TestWorkQueueRequeueNow verifies requeueNow drops a pending delayed add (so the
// stale timer never fires) and makes the id immediately dispatchable.
func TestWorkQueueRequeueNow(t *testing.T) {
	q := newWorkQueue()
	q.addAfter(1, time.Hour)

	q.requeueNow(1)

	assert.Nil(t, q.alarms[1], "requeueNow must drop the pending delayed add")
	id, ok := q.get()
	require.True(t, ok, "requeueNow must make the id dispatchable now")
	assert.Equal(t, ObjectID(1), id)
}

// schedRecorder captures onSchedule emissions for assertions. It is unbuffered-safe:
// the callback runs under q.mu, so record must not block on the queue; a generous
// buffer keeps the producer non-blocking while the test drains.
type schedRecorder struct {
	ch chan schedEmit
}

type schedEmit struct {
	id        ObjectID
	at        time.Time
	scheduled bool
}

func newSchedRecorder() *schedRecorder { return &schedRecorder{ch: make(chan schedEmit, 64)} }

func (r *schedRecorder) record(id ObjectID, at time.Time, scheduled bool) {
	r.ch <- schedEmit{id, at, scheduled}
}

// next returns the next emission, failing the test on timeout so a missing emit is
// a failure rather than a hang.
func (r *schedRecorder) next(t *testing.T) schedEmit {
	t.Helper()
	select {
	case e := <-r.ch:
		return e
	case <-time.After(2 * time.Second):
		t.Fatal("expected a schedule emission, got none")
		return schedEmit{}
	}
}

// expectNone asserts no emission arrives promptly — used to prove the dedup guard
// suppressed a redundant equal value.
func (r *schedRecorder) expectNone(t *testing.T) {
	t.Helper()
	select {
	case e := <-r.ch:
		t.Fatalf("expected no emission, got %+v", e)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestWorkQueueOnScheduleAddEmitsNow verifies an immediate add emits a due-now
// Schedule for the id.
func TestWorkQueueOnScheduleAddEmitsNow(t *testing.T) {
	rec := newSchedRecorder()
	q := newWorkQueue()
	q.onSchedule = rec.record

	q.add(1)

	e := rec.next(t)
	assert.Equal(t, ObjectID(1), e.id)
	assert.True(t, e.scheduled, "a queued-now id must emit as scheduled")
	assert.False(t, e.at.IsZero(), "a queued-now id must emit a non-zero time")
	assert.False(t, e.at.After(time.Now()), "queued-now must be due now, not future")
}

// TestWorkQueueOnScheduleDedupsEqual verifies a redundant add (id already queued,
// still due-now) does not emit a second, identical Schedule.
func TestWorkQueueOnScheduleDedupsEqual(t *testing.T) {
	rec := newSchedRecorder()
	q := newWorkQueue()
	q.onSchedule = rec.record

	q.add(1)
	rec.next(t) // the first, real emission

	q.add(1) // duplicate: still due-now, value unchanged
	rec.expectNone(t)
}

// TestWorkQueueOnScheduleAddAfterEmitsFireTime verifies a delayed add emits its
// future fire time.
func TestWorkQueueOnScheduleAddAfterEmitsFireTime(t *testing.T) {
	rec := newSchedRecorder()
	q := newWorkQueue()
	q.onSchedule = rec.record

	q.addAfter(1, time.Hour)

	e := rec.next(t)
	assert.Equal(t, ObjectID(1), e.id)
	assert.True(t, e.scheduled, "a delayed add must emit as scheduled")
	assert.True(t, e.at.After(time.Now().Add(time.Minute)),
		"delayed add must emit its ~1h fire time, got %s", e.at)
}

// TestWorkQueueOnScheduleGetEmitsZero verifies dispatching an id (get) with no
// pending future alarm emits the zero/unscheduled Schedule — the transition a
// countdown UI shows as "reconciling now, nothing scheduled".
func TestWorkQueueOnScheduleGetEmitsZero(t *testing.T) {
	rec := newSchedRecorder()
	q := newWorkQueue()
	q.onSchedule = rec.record

	q.add(1)
	rec.next(t) // due-now emit from add

	_, ok := q.get()
	require.True(t, ok)

	e := rec.next(t)
	assert.Equal(t, ObjectID(1), e.id)
	assert.False(t, e.scheduled, "dispatch must emit as unscheduled")
	assert.True(t, e.at.IsZero(), "dispatch must emit the unscheduled zero time")
}

// TestWorkQueueOnScheduleTimerFiredEmitsNow verifies an alarm firing (its timer
// enqueues the id) emits a due-now Schedule.
func TestWorkQueueOnScheduleTimerFiredEmitsNow(t *testing.T) {
	rec := newSchedRecorder()
	q := newWorkQueue()
	q.onSchedule = rec.record

	q.addAfter(1, 20*time.Millisecond)
	rec.next(t) // the future fire-time emit from addAfter

	// When the timer fires it enqueues the id: the schedule flips to due-now.
	e := rec.next(t)
	assert.Equal(t, ObjectID(1), e.id)
	assert.False(t, e.at.After(time.Now()), "a fired timer makes the id due now")
}

// TestWorkQueueOnScheduleRequeueNowEmitsNow verifies requeueNow (which drops a
// pending future alarm and enqueues) emits a due-now Schedule.
func TestWorkQueueOnScheduleRequeueNowEmitsNow(t *testing.T) {
	rec := newSchedRecorder()
	q := newWorkQueue()
	q.onSchedule = rec.record

	q.addAfter(1, time.Hour)
	rec.next(t) // future fire-time emit

	q.requeueNow(1)

	e := rec.next(t)
	assert.Equal(t, ObjectID(1), e.id)
	assert.False(t, e.at.After(time.Now()), "requeueNow must emit due-now")
}

// TestWorkQueueOnScheduleDoneRequeueEmitsNow verifies the schedule emits track
// dirty transitions across a mid-processing re-add: the re-add reports due-now, and
// done — which only shuffles processing/items, not the schedule — emits nothing.
func TestWorkQueueOnScheduleDoneRequeueEmitsNow(t *testing.T) {
	rec := newSchedRecorder()
	q := newWorkQueue()
	q.onSchedule = rec.record

	q.add(1)
	rec.next(t) // due-now from add
	_, ok := q.get()
	require.True(t, ok)
	rec.next(t) // zero from dispatch

	q.add(1) // re-added while processing: marked dirty, still emits due-now
	e := rec.next(t)
	assert.Equal(t, ObjectID(1), e.id)
	assert.False(t, e.at.After(time.Now()))

	q.done(1) // shuffles processing/items only; schedule unchanged, so no emit
	rec.expectNone(t)
}
