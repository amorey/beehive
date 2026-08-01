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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	assert.False(t, q.scheduleAt(1).Schedule.NextRequeueAt.IsZero(),
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
