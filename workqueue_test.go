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

	"github.com/amorey/gobus"
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

	q.stop()

	assert.False(t, q.scheduleAt(1).NextRequeueAt.IsZero(),
		"a queued id is not descheduled by stop")
	last := tx.taken()
	require.NotEmpty(t, last)
	assert.False(t, last[len(last)-1].Schedule.NextRequeueAt.IsZero(),
		"its final published value is that due-now, not a deschedule")
}

// stop publishes the final schedule of every id the gauge still describes, not
// only the ids whose schedule it moved.
//
// That completeness is what makes a publish racing shutdown harmless. An enqueue
// can move the gauge, release q.mu, and then lose its own publish to the closing
// sender. Because no gauge move is possible once stopped is set, stop's snapshot
// is the final state, so the lost publish can only ever have carried a duplicate
// of it. Without the snapshot the subscriber would end on a value the queue had
// already left.
func TestPublishStopSnapshotsEveryScheduledID(t *testing.T) {
	q, tx := publishingQueue()
	q.add(1)                 // queued now
	q.addAfter(2, time.Hour) // pending alarm
	before := len(tx.taken())

	q.stop()

	final := make(map[ObjectID]Schedule)
	for _, m := range tx.taken()[before:] {
		final[m.ID] = m.Schedule
	}
	require.Len(t, final, 2, "both ids must appear in the final snapshot")
	assert.True(t, final[2].NextRequeueAt.IsZero(), "the cleared alarm reads as unscheduled")
	assert.False(t, final[1].NextRequeueAt.IsZero(), "the queued id keeps its due-now")
}

// Dispatch is guarded on stopped like every other mutator. That is what makes
// stop's snapshot the gauge's final state: get is otherwise the one mutator that
// could move the schedule afterwards, and its publish would go to a closed
// sender and be lost.
func TestWorkQueueStoppedQueueDispatchesNothing(t *testing.T) {
	q, tx := publishingQueue()
	q.add(1)
	q.stop()
	before := len(tx.taken())

	_, ok := q.get()

	assert.False(t, ok, "a stopped queue must dispatch nothing")
	assert.Len(t, tx.taken(), before, "and must therefore publish nothing")
	assert.False(t, q.scheduleAt(1).NextRequeueAt.IsZero(),
		"the gauge is unchanged, so the final snapshot still describes it")
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

// pendingAlarm builds an alarm as addAfter does. A real alarm always carries a
// live timer, and clearAllAlarms stops it, so one built without a timer is not a
// state the gauge ever holds.
func pendingAlarm(in time.Duration) *alarm {
	return &alarm{timer: time.NewTimer(in), fireAt: time.Now().Add(in)}
}

// The gauge is what SchedulesWatch reports, and the whole push design rests on
// one property: a mutator that moves the observable schedule says so, and one
// that does not stays quiet. These tests hold that line without a bus, a queue
// or a stream in the way.

func TestGaugeMarkDirtyReports(t *testing.T) {
	g := newGauge()

	got, moved := g.markDirty(7)
	require.True(t, moved, "an id that becomes due now moves the gauge")
	assert.False(t, got.Schedule.NextRequeueAt.IsZero(), "due-now carries the moment it became due")
	assert.NotZero(t, got.Seq)

	// Already dirty: addLocked would return early, and so does the gauge.
	_, moved = g.markDirty(7)
	assert.False(t, moved, "a second markDirty changes nothing observable")
}

func TestGaugeSetAlarmReports(t *testing.T) {
	g := newGauge()
	a := pendingAlarm(time.Hour)

	got, moved := g.setAlarm(7, a)
	require.True(t, moved)
	assert.Equal(t, a.fireAt, got.Schedule.NextRequeueAt)
}

// at reads dirty before alarms, so an alarm on an id already queued for
// immediate dispatch changes nothing a watcher can see.
func TestGaugeSetAlarmOnDirtyIDReportsNothing(t *testing.T) {
	g := newGauge()
	_, moved := g.markDirty(7)
	require.True(t, moved)

	_, moved = g.setAlarm(7, pendingAlarm(time.Hour))
	assert.False(t, moved, "the id already reads as due now")
}

func TestGaugeClearDirtyReports(t *testing.T) {
	g := newGauge()
	_, moved := g.markDirty(7)
	require.True(t, moved)

	got, moved := g.clearDirty(7)
	require.True(t, moved, "dispatch leaves the id unscheduled")
	assert.True(t, got.Schedule.NextRequeueAt.IsZero())

	_, moved = g.clearDirty(7)
	assert.False(t, moved, "clearing what is already clear moves nothing")
}

// Dispatching an id that also holds a future alarm reports the alarm, not the
// zero schedule: the id is still scheduled, just not now.
func TestGaugeClearDirtyFallsBackToTheAlarm(t *testing.T) {
	g := newGauge()
	a := pendingAlarm(time.Hour)
	g.setAlarm(7, a)
	g.markDirty(7)

	got, moved := g.clearDirty(7)
	require.True(t, moved)
	assert.Equal(t, a.fireAt, got.Schedule.NextRequeueAt)
}

func TestGaugeClearAlarmReports(t *testing.T) {
	g := newGauge()
	g.setAlarm(7, pendingAlarm(time.Hour))

	got, moved := g.clearAlarm(7)
	require.True(t, moved)
	assert.True(t, got.Schedule.NextRequeueAt.IsZero())

	_, moved = g.clearAlarm(7)
	assert.False(t, moved, "clearing an absent alarm moves nothing")
}

// Shutdown clears every alarm, but an id that is also dirty reads as due-now
// before and after, so it must not be reported. Reporting it would publish a
// Seq bump for a schedule nobody can see change.
func TestGaugeClearAllAlarmsSkipsADirtyID(t *testing.T) {
	g := newGauge()
	g.setAlarm(1, pendingAlarm(time.Hour))
	g.setAlarm(2, pendingAlarm(time.Hour))
	g.markDirty(2) // 2 now reads as due now, so its alarm is invisible

	got := g.clearAllAlarms()

	require.Len(t, got, 1, "only the id whose observable schedule moved")
	assert.Equal(t, ObjectID(1), got[0].ID)
	assert.True(t, got[0].Schedule.NextRequeueAt.IsZero())
}

// Seq is the queue's order, so it advances on every reported move and never
// on a quiet one. Accept compares it, so a stalled Seq would let a stale
// publish win.
func TestGaugeSeqAdvancesOnlyOnAReportedMove(t *testing.T) {
	g := newGauge()

	first, moved := g.markDirty(1)
	require.True(t, moved)
	second, moved := g.markDirty(2)
	require.True(t, moved)
	assert.Greater(t, second.Seq, first.Seq)

	_, moved = g.markDirty(1) // already dirty
	require.False(t, moved)
	third, moved := g.markDirty(3)
	require.True(t, moved)
	assert.Equal(t, second.Seq+1, third.Seq, "a quiet call consumed no Seq")
}

func TestGaugeAtReportsTheCurrentSchedule(t *testing.T) {
	g := newGauge()
	assert.True(t, g.at(7).Schedule.NextRequeueAt.IsZero(), "an unknown id is unscheduled")

	a := pendingAlarm(time.Hour)
	g.setAlarm(7, a)
	assert.Equal(t, a.fireAt, g.at(7).Schedule.NextRequeueAt)

	// at does not move the gauge, so it does not consume a Seq.
	before := g.at(7).Seq
	assert.Equal(t, before, g.at(7).Seq)
}

// Bus-boundary tests. Each holds a receiver from the hub and starts no stream on
// it, because Peek is a single-consumer read: a live SchedulesWatch takes a
// value the instant it lands, so a Peek racing it reports ErrEmpty on a
// coin-flip and proves nothing.
//
// Peek is what makes these tests mean what they say. Reading alone cannot
// distinguish "Accept rejected the value" from "the value arrived and the
// stream's equality check swallowed it", and the second passes with the hub
// wired without Accept at all.

func TestHubStaleSendNeverReachesTheSlot(t *testing.T) {
	q := newWorkQueue()
	rx, _ := q.watchSchedule(1)
	defer rx.Close()

	// Two moves for one id, published in the reverse of the order they became
	// true — the shape two racing publishes produce.
	newer := stamped{Schedule: Schedule{NextRequeueAt: time.Now().Add(time.Hour)}, Seq: 9}
	older := stamped{Schedule: Schedule{NextRequeueAt: time.Now().Add(time.Minute)}, Seq: 8}
	require.NoError(t, q.schedules.Send(1, newer))
	require.NoError(t, q.schedules.Send(1, older))

	ev, err := rx.Peek()
	require.NoError(t, err)
	assert.Equal(t, newer.Schedule, ev.Value.Schedule, "Accept must reject the older Seq")

	ev, err = rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, newer.Schedule, ev.Value.Schedule)
}

// The same shape with the newer value still unread. An earlier draft of the
// upstream package replaced a waiting value on arrival order alone, which would
// have let the stale send evict it.
func TestHubStaleSendDoesNotDisplaceAnUnreadValue(t *testing.T) {
	q := newWorkQueue()
	rx, _ := q.watchSchedule(1)
	defer rx.Close()

	newer := stamped{Schedule: Schedule{NextRequeueAt: time.Now().Add(time.Hour)}, Seq: 5}
	require.NoError(t, q.schedules.Send(1, newer))
	// newer is now unread in the slot.
	require.NoError(t, q.schedules.Send(1, stamped{Seq: 4}))

	ev, err := rx.Peek()
	require.NoError(t, err)
	assert.Equal(t, newer.Schedule, ev.Value.Schedule)
}

// The value read at subscribe is the prev of the first Accept call, so a publish
// that predates it is rejected by the same rule that rejects a reordered one.
// This is what closes the subscribe seam.
func TestHubSubscribeBaselineRejectsADuplicate(t *testing.T) {
	q := newWorkQueue()
	q.add(1) // the move completes...

	rx, seed := q.watchSchedule(1) // ...and the subscriber reads it as its baseline
	defer rx.Close()

	// A publish carrying that same move now lands late.
	require.NoError(t, q.schedules.Send(1, seed))

	_, err := rx.Peek()
	assert.ErrorIs(t, err, gobus.ErrEmpty, "the duplicate must never enter the slot")
}

// A receiver watching one id holds nothing for another. The second half is what
// makes the first mean "B did not reach A" rather than "this receiver never
// receives anything", and both fit on one receiver because Peek takes nothing.
func TestHubKeyScope(t *testing.T) {
	q := newWorkQueue()
	rx, _ := q.watchSchedule(1)
	defer rx.Close()

	q.add(2)
	_, err := rx.Peek()
	assert.ErrorIs(t, err, gobus.ErrEmpty, "another id's move must not reach this receiver")

	q.add(1)
	ev, err := rx.Peek()
	require.NoError(t, err, "this id's move must reach it")
	assert.False(t, ev.Value.Schedule.NextRequeueAt.IsZero())
}

// stop publishes the final values before the sender closes, and a receiver
// holding an unread value is not yet terminal — so the last word is readable.
func TestHubFinalValueIsQueuedBeforeTheSenderCloses(t *testing.T) {
	q := newWorkQueue()
	q.addAfter(1, time.Hour)
	rx, _ := q.watchSchedule(1)
	defer rx.Close()

	q.stop()
	q.schedules.Close()

	ev, err := rx.Peek()
	require.NoError(t, err, "the final value is unread, so the receiver is not terminal")
	assert.True(t, ev.Value.Schedule.NextRequeueAt.IsZero(), "nothing scheduled")
}

// watchSchedule reads the gauge and registers the watch in one critical section,
// so the baseline is the value current at registration.
func TestHubWatchScheduleSeedsFromTheGauge(t *testing.T) {
	q := newWorkQueue()
	q.addAfter(1, time.Hour)
	rx, want := q.watchSchedule(1)
	defer rx.Close()

	// Nothing has superseded the baseline, so there is nothing to read...
	_, err := rx.Peek()
	assert.ErrorIs(t, err, gobus.ErrEmpty)

	// ...but the baseline is the prev, so an older publish is still rejected.
	require.NoError(t, q.schedules.Send(1, stamped{Seq: want.Seq - 1}))
	_, err = rx.Peek()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
}
