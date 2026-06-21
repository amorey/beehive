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

	assert.Nil(t, q.timers, "stopped queue must not track a new timer")
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
