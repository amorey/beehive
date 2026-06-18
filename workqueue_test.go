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

func TestWorkQueueReaddAfterGet(t *testing.T) {
	q := newWorkQueue()
	q.add(7)
	_, _ = q.get() // item leaves the set

	// Same ID can be added again once it's been consumed.
	q.add(7)
	id, ok := q.get()
	require.True(t, ok)
	assert.Equal(t, ObjectID(7), id)
}
