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
	"testing"
	"time"

	"github.com/amorey/gobus/watch"
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
	case <-time.After(testTimeout):
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

	assert.Nil(t, q.gauge.alarmAt(1), "stopped queue must not track a new timer")
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

// TestWorkQueueScheduleAtEmpty verifies an unknown ID reports nothing
// scheduled.
func TestWorkQueueScheduleAtEmpty(t *testing.T) {
	q := newWorkQueue()
	assert.True(t, q.scheduleAt(1).NextRequeueAt.IsZero(),
		"unknown id must report nothing scheduled")
}

// TestWorkQueueScheduleAtDispatchable verifies an ID queued for immediate
// dispatch reports a due-now time (not after now).
func TestWorkQueueScheduleAtDispatchable(t *testing.T) {
	q := newWorkQueue()
	q.add(1)

	at := q.scheduleAt(1).NextRequeueAt
	require.False(t, at.IsZero(), "queued id must report as scheduled")
	assert.False(t, at.After(time.Now()), "a queued-now id is due now, not in the future")
}

// TestWorkQueueScheduleAtAfter verifies a delayed add reports its future fire
// time.
func TestWorkQueueScheduleAtAfter(t *testing.T) {
	q := newWorkQueue()
	q.addAfter(1, time.Hour)

	at := q.scheduleAt(1).NextRequeueAt
	require.False(t, at.IsZero(), "delayed id must report as scheduled")
	assert.True(t, at.After(time.Now().Add(time.Minute)), "fire time must be ~1h out, got %s", at)
}

// TestWorkQueueAddAfterNewestWins verifies a second addAfter for the same id
// supersedes the first: the reported fire time is the newer one and only one
// timer remains.
func TestWorkQueueAddAfterNewestWins(t *testing.T) {
	q := newWorkQueue()
	q.addAfter(1, time.Hour)
	q.addAfter(1, 3*time.Hour)

	at := q.scheduleAt(1).NextRequeueAt
	require.False(t, at.IsZero())
	assert.True(t, at.After(time.Now().Add(2*time.Hour)), "newest schedule must win, got %s", at)
}

// TestWorkQueueScheduleAtPrefersQueued verifies that when an id has both a
// future delayed schedule and an immediate enqueue (e.g. a pending backoff timer
// plus a store-change add), scheduleAt reports it as due now — not at the
// stale future time, which would contradict "now if already queued".
func TestWorkQueueScheduleAtPrefersQueued(t *testing.T) {
	q := newWorkQueue()
	q.addAfter(1, time.Hour) // future backoff/RequeueAfter timer
	q.add(1)                 // ...then enqueued immediately

	at := q.scheduleAt(1).NextRequeueAt
	require.False(t, at.IsZero())
	assert.False(t, at.After(time.Now()), "a queued-now id is due now, not the future timer; got %s", at)
}

// TestWorkQueueSupersededTimerDoesNotEnqueue verifies a delayed-add timer whose
// slot was replaced (by a newer addAfter or requeueNow) does not enqueue the id
// when it finally fires: the newest schedule owns the enqueue, so a stale timer
// that already fired but lost the race for the lock must not run the work early.
func TestWorkQueueSupersededTimerDoesNotEnqueue(t *testing.T) {
	q := newWorkQueue()
	stale := &alarm{timer: time.NewTimer(time.Hour)}
	q.gauge.setAlarm(1, &alarm{timer: time.NewTimer(time.Hour)}) // a newer schedule now occupies the slot

	q.timerFired(1, stale) // the superseded timer fires late

	_, ok := q.get()
	assert.False(t, ok, "a superseded timer must not enqueue the id")
	assert.NotNil(t, q.gauge.alarmAt(1), "the newer schedule must be left intact")
}

// TestWorkQueueTimerFiredEnqueues verifies a current (non-superseded) timer
// clears its slot and enqueues the id when it fires.
func TestWorkQueueTimerFiredEnqueues(t *testing.T) {
	q := newWorkQueue()
	a := &alarm{timer: time.NewTimer(time.Hour)}
	q.gauge.setAlarm(1, a)

	q.timerFired(1, a)

	assert.Nil(t, q.gauge.alarmAt(1), "firing must clear the schedule slot")
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

	assert.Nil(t, q.gauge.alarmAt(1), "requeueNow must drop the pending delayed add")
	id, ok := q.get()
	require.True(t, ok, "requeueNow must make the id dispatchable now")
	assert.Equal(t, ObjectID(1), id)
}

// These tests hold the publish half: which queue operations reach the sender,
// and what they carry. They use a double rather than a hub, so a failure names
// the queue rather than the bus.

// fakeScheduleSender records what the queue published.
type fakeScheduleSender struct {
	mu   sync.Mutex
	sent []keyed
	err  error
}

func (f *fakeScheduleSender) Send(id ObjectID, s stamped) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, keyed{ID: id, stamped: s})
	return f.err
}

// Watch and Close satisfy scheduleSender. These tests assert on what the queue
// published, so neither needs to do anything.
func (f *fakeScheduleSender) Watch(ObjectID, stamped) *watch.Receiver[ObjectID, stamped] {
	panic("not implemented: fakeScheduleSender.Watch")
}

func (f *fakeScheduleSender) Close() {}

func (f *fakeScheduleSender) taken() []keyed {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]keyed(nil), f.sent...)
}

// publishingQueue is a queue wired to a recording sender.
func publishingQueue() (*workQueue, *fakeScheduleSender) {
	tx := &fakeScheduleSender{}
	q := newWorkQueue()
	q.schedules = tx
	return q, tx
}

func TestPublishAddSendsDueNow(t *testing.T) {
	q, tx := publishingQueue()

	q.add(1)

	sent := tx.taken()
	require.Len(t, sent, 1)
	assert.Equal(t, ObjectID(1), sent[0].ID)
	assert.False(t, sent[0].Schedule.NextRequeueAt.IsZero())
}

// A second add for a queued id moves nothing, so it publishes nothing.
func TestPublishRepeatedAddSendsOnce(t *testing.T) {
	q, tx := publishingQueue()

	q.add(1)
	q.add(1)

	assert.Len(t, tx.taken(), 1)
}

// done moves an id between processing and items, which the gauge does not read.
func TestPublishDoneSendsNothing(t *testing.T) {
	q, tx := publishingQueue()
	q.add(1)
	id, ok := q.get()
	require.True(t, ok)
	before := len(tx.taken())

	q.done(id)

	assert.Len(t, tx.taken(), before, "done must publish nothing")
}

// Dispatch clears the dirty slot, so the id becomes unscheduled.
func TestPublishGetSendsUnscheduled(t *testing.T) {
	q, tx := publishingQueue()
	q.add(1)

	_, ok := q.get()
	require.True(t, ok)

	sent := tx.taken()
	require.Len(t, sent, 2, "the add and the dispatch")
	assert.True(t, sent[1].Schedule.NextRequeueAt.IsZero(), "a dispatched id is unscheduled")
}

// requeueNow clears an alarm and marks the id dirty in one critical section.
// Reported separately that is "nothing scheduled" then "due now", and the first
// is a state that never existed between two consistent points.
func TestPublishRequeueNowSendsOnce(t *testing.T) {
	q, tx := publishingQueue()
	q.addAfter(1, time.Hour)
	before := len(tx.taken())

	q.requeueNow(1)

	sent := tx.taken()[before:]
	require.Len(t, sent, 1, "one publish, not a deschedule followed by a schedule")
	assert.False(t, sent[0].Schedule.NextRequeueAt.IsZero(), "due now, not nothing scheduled")
	assert.False(t, sent[0].Schedule.NextRequeueAt.After(time.Now()))
}

// timerFired clears the alarm and enqueues in one critical section too, so a
// subscriber never sees the phantom deschedule between them.
func TestPublishTimerFiredSendsOnce(t *testing.T) {
	q, tx := publishingQueue()
	a := &alarm{timer: time.NewTimer(time.Hour), fireAt: time.Now().Add(time.Hour)}
	q.gauge.setAlarm(1, a)
	before := len(tx.taken())

	q.timerFired(1, a)

	sent := tx.taken()[before:]
	require.Len(t, sent, 1, "one publish, not a deschedule followed by a schedule")
	assert.False(t, sent[0].Schedule.NextRequeueAt.After(time.Now()), "due now")
}

// A superseded timer owns no enqueue, so it moves nothing and publishes nothing.
func TestPublishSupersededTimerSendsNothing(t *testing.T) {
	q, tx := publishingQueue()
	stale := &alarm{timer: time.NewTimer(time.Hour)}
	q.gauge.setAlarm(1, &alarm{timer: time.NewTimer(time.Hour)})
	before := len(tx.taken())

	q.timerFired(1, stale)

	assert.Len(t, tx.taken(), before)
}

// addAfter on an id already queued for immediate dispatch changes nothing a
// watcher can see, because the gauge reads dirty first.
func TestPublishAddAfterOnDirtyIDSendsNothing(t *testing.T) {
	q, tx := publishingQueue()
	q.add(1)
	before := len(tx.taken())

	q.addAfter(1, time.Hour)

	assert.Len(t, tx.taken(), before)
}

// The stopped guard sits above the gauge call. Without it a post-stop add would
// publish a due-now after the final values, and with no poll behind it that
// would be the subscriber's last word.
func TestPublishStoppedQueueSendsNothing(t *testing.T) {
	q, tx := publishingQueue()
	q.stop()
	before := len(tx.taken())

	q.add(1)
	q.addAfter(2, time.Hour)
	q.requeueNow(3)

	assert.Len(t, tx.taken(), before, "a stopped queue must publish nothing")
}

// stop publishes the final value of every id whose schedule moved, so a
// subscriber's last word is accurate rather than absent.
func TestPublishStopSendsTheFinalValues(t *testing.T) {
	q, tx := publishingQueue()
	q.addAfter(1, time.Hour)
	before := len(tx.taken())

	q.stop()

	sent := tx.taken()[before:]
	require.Len(t, sent, 1)
	assert.Equal(t, ObjectID(1), sent[0].ID)
	assert.True(t, sent[0].Schedule.NextRequeueAt.IsZero(), "nothing scheduled")
}

// stop clears alarms, not the dirty set, so an id queued for immediate dispatch
// still reads as due-now afterwards. Its final value is therefore a dispatch that
// will never happen — which is exactly what the poll reported too, because the
// gauge genuinely still says due-now.
//
// Pinned so nobody "fixes" it by clearing dirty in stop, which would change what
// the queue does rather than what it reports.
func TestPublishStopLeavesAQueuedIDDueNow(t *testing.T) {
	q, tx := publishingQueue()
	q.add(1)
	before := len(tx.taken())

	q.stop()

	assert.Len(t, tx.taken(), before, "a queued id moves nothing at stop")
	assert.False(t, q.scheduleAt(1).NextRequeueAt.IsZero(),
		"a queued id is not descheduled by stop")
}

// A queue with no hub is a client-only kind. It must not panic.
func TestPublishWithNoSenderIsSafe(t *testing.T) {
	q := newWorkQueue() // no schedules sender

	assert.NotPanics(t, func() {
		q.add(1)
		q.get()
		q.requeueNow(1)
		q.stop()
	})
}

// BenchmarkWorkQueueHotPath measures one reconcile's worth of queue work with
// nobody watching, which is the ordinary case for every object in the system.
//
// The push path publishes from inside these methods, so this is the number the
// design has to keep honest: the capture must stay off the heap and the bus's
// idle check must be all an unwatched publish costs.
func BenchmarkWorkQueueHotPath(b *testing.B) {
	q := newWorkQueue()
	for b.Loop() {
		q.add(1)
		id, _ := q.get()
		q.done(id)
	}
}
