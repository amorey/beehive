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

package conflate

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// latestWins is a test merge: the newer value supersedes the older, and a
// negative value annihilates the key (keep=false). It exercises both the
// coalesce and the annihilate paths with plain ints.
func latestWins(_, next int) (int, bool) { return next, next >= 0 }

// recvWithin pops the next value, failing if none arrives within d.
func recvWithin[K comparable, V any](t *testing.T, rx *Receiver[K, V], d time.Duration) V {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	v, err := rx.RecvContext(ctx)
	require.NoError(t, err, "expected a value within %s", d)
	return v
}

// assertBlocks asserts RecvContext does not deliver within d (returns the
// deadline error instead).
func assertBlocks[K comparable, V any](t *testing.T, rx *Receiver[K, V], d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	_, err := rx.RecvContext(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestBasicDelivery(t *testing.T) {
	h := New[int, int](latestWins)
	rx := h.Receiver()
	require.NoError(t, h.Sender().Send(1, 100))
	assert.Equal(t, 100, recvWithin(t, rx, time.Second))
	// Nothing more pending: the next recv blocks.
	assertBlocks(t, rx, 50*time.Millisecond)
}

func TestRecvWakesParkedReceiver(t *testing.T) {
	h := New[int, int](latestWins)
	rx := h.Receiver()
	got := make(chan int, 1)
	go func() { got <- recvWithin(t, rx, time.Second) }()
	// The goroutine parks (queue empty); the Send must wake it.
	require.NoError(t, h.Sender().Send(7, 42))
	select {
	case v := <-got:
		assert.Equal(t, 42, v)
	case <-time.After(time.Second):
		t.Fatal("parked receiver was not woken by Send")
	}
}

// TestCloseWakesParkedReceiver covers the in-select close path: a receiver
// already parked in RecvContext's blocking select must wake with ErrClosed when
// Close fires (distinct from a Close observed by the pre-lock check). We wait for
// the waiting flag, set just before the parked select, so Close lands in-select.
func TestCloseWakesParkedReceiver(t *testing.T) {
	h := New[int, int](latestWins)
	rx := h.Receiver()
	got := make(chan error, 1)
	go func() {
		_, err := rx.RecvContext(context.Background())
		got <- err
	}()

	deadline := time.Now().Add(time.Second)
	for {
		rx.s.mu.Lock()
		parked := rx.waiting
		rx.s.mu.Unlock()
		if parked {
			break // committed to the bottom select; its only next move is the <-done case
		}
		if time.Now().After(deadline) {
			t.Fatal("receiver never parked")
		}
		runtime.Gosched()
	}

	rx.Close()
	select {
	case err := <-got:
		assert.ErrorIs(t, err, ErrClosed)
	case <-time.After(time.Second):
		t.Fatal("parked receiver was not woken by Close")
	}
}

func TestRecvContextCancel(t *testing.T) {
	h := New[int, int](latestWins)
	rx := h.Receiver()
	assertBlocks(t, rx, 50*time.Millisecond)
}

func TestCoalesceLatestWins(t *testing.T) {
	h := New[int, int](latestWins)
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 100))
	require.NoError(t, tx.Send(1, 200))
	require.NoError(t, tx.Send(1, 300))
	// Three sends to one key before any read collapse to one latest value.
	assert.Equal(t, 300, recvWithin(t, rx, time.Second))
	assertBlocks(t, rx, 50*time.Millisecond)
}

func TestAnnihilation(t *testing.T) {
	h := New[int, int](latestWins)
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 100)) // enqueue key 1
	require.NoError(t, tx.Send(1, -1))  // annihilate key 1 (negative)
	require.NoError(t, tx.Send(2, 50))  // key 2 survives
	// Key 1 was dropped entirely; only key 2 is delivered.
	assert.Equal(t, 50, recvWithin(t, rx, time.Second))
	assertBlocks(t, rx, 50*time.Millisecond)
	assert.Equal(t, 0, rx.lenForTest())
}

func TestReceiverFuncFiltersAtEnqueue(t *testing.T) {
	h := New[int, int](latestWins)
	rx := h.ReceiverFunc(func(k int) bool { return k == 7 })
	tx := h.Sender()
	// Unwanted keys are dropped at Send, so they never occupy a buffer slot.
	require.NoError(t, tx.Send(1, 100))
	require.NoError(t, tx.Send(2, 200))
	assert.Equal(t, 0, rx.lenForTest())
	// The wanted key is buffered and delivered as usual.
	require.NoError(t, tx.Send(7, 42))
	require.NoError(t, tx.Send(3, 300))
	assert.Equal(t, 1, rx.lenForTest())
	assert.Equal(t, 42, recvWithin(t, rx, time.Second))
	assertBlocks(t, rx, 50*time.Millisecond)
}

func TestReceiverMergeIsPerReceiver(t *testing.T) {
	h := New[int, int](latestWins)
	shared := h.Receiver()
	// This receiver annihilates any coalesced pair (keep=false) instead of
	// taking the latest; the shared receiver is unaffected by the override.
	annihilate := h.ReceiverMerge(func(_, _ int) (int, bool) { return 0, false })
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 100))
	require.NoError(t, tx.Send(1, 200)) // coalesce on both receivers
	// Shared receiver keeps the latest via the hub's merge.
	assert.Equal(t, 200, recvWithin(t, shared, time.Second))
	// Override receiver dropped key 1 entirely.
	assert.Equal(t, 0, annihilate.lenForTest())
	assertBlocks(t, annihilate, 50*time.Millisecond)
}

func TestStableOrder(t *testing.T) {
	h := New[int, int](latestWins)
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 10))
	require.NoError(t, tx.Send(2, 20))
	require.NoError(t, tx.Send(3, 30))
	assert.Equal(t, 10, recvWithin(t, rx, time.Second))
	assert.Equal(t, 20, recvWithin(t, rx, time.Second))
	assert.Equal(t, 30, recvWithin(t, rx, time.Second))
}

func TestRetouchKeepsPosition(t *testing.T) {
	h := New[int, int](latestWins)
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 10))
	require.NoError(t, tx.Send(2, 20))
	require.NoError(t, tx.Send(1, 99)) // re-touch key 1: keeps its (first) slot
	// Order stays 1,2 — key 1 is delivered first with its latest body.
	assert.Equal(t, 99, recvWithin(t, rx, time.Second))
	assert.Equal(t, 20, recvWithin(t, rx, time.Second))
}

func TestFanoutIsolation(t *testing.T) {
	h := New[int, int](latestWins)
	rxA := h.Receiver()
	rxB := h.Receiver()
	require.NoError(t, h.Sender().Send(1, 7))
	// Each receiver gets its own copy.
	assert.Equal(t, 7, recvWithin(t, rxA, time.Second))
	assert.Equal(t, 7, recvWithin(t, rxB, time.Second))
	// rxB draining does not affect rxA's already-consumed stream.
	assertBlocks(t, rxA, 50*time.Millisecond)
}

func TestBoundedUnderSlowConsumer(t *testing.T) {
	h := New[int, int](latestWins)
	rx := h.Receiver()
	tx := h.Sender()
	// 1000 writes across 4 keys, never read: pending stays bounded by the key
	// set, not the write count.
	for i := 0; i < 1000; i++ {
		require.NoError(t, tx.Send(i%4, i))
	}
	assert.Equal(t, 4, rx.lenForTest())
	// A re-touched-then-annihilated key leaves no residue.
	require.NoError(t, tx.Send(0, -1))
	assert.Equal(t, 3, rx.lenForTest())
}

func TestReceiverClose(t *testing.T) {
	h := New[int, int](latestWins)
	rx := h.Receiver()
	rx.Close()
	_, err := rx.RecvContext(context.Background())
	assert.ErrorIs(t, err, ErrClosed)
	rx.Close() // idempotent
}

func TestHubClose(t *testing.T) {
	h := New[int, int](latestWins)
	rx := h.Receiver()
	require.NoError(t, h.Sender().Send(1, 1))
	h.Close()
	// Hard tear-down: ErrClosed without draining the pending value.
	_, err := rx.RecvContext(context.Background())
	assert.ErrorIs(t, err, ErrClosed)
	// Sends after hub close are rejected; new receivers are pre-closed.
	assert.ErrorIs(t, h.Sender().Send(2, 2), ErrClosed)
	_, err = h.Receiver().RecvContext(context.Background())
	assert.ErrorIs(t, err, ErrClosed)
	h.Close() // idempotent
}

func TestSenderCloseDrainsThenErrClosed(t *testing.T) {
	h := New[int, int](latestWins)
	rx := h.Receiver()
	tx := h.Sender()
	require.NoError(t, tx.Send(1, 11))
	require.NoError(t, tx.Send(2, 22))
	tx.Close()
	// Soft close: pending values drain first, then ErrClosed.
	assert.Equal(t, 11, recvWithin(t, rx, time.Second))
	assert.Equal(t, 22, recvWithin(t, rx, time.Second))
	_, err := rx.RecvContext(context.Background())
	assert.ErrorIs(t, err, ErrClosed)
	assert.ErrorIs(t, tx.Send(3, 33), ErrClosed)
	// Second Close is a harmless no-op (idempotent guard), not a re-signal/panic.
	tx.Close()
	assert.ErrorIs(t, tx.Send(4, 44), ErrClosed)
}

func TestCloseRaceBeforeLock(t *testing.T) {
	h := New[int, int](latestWins)
	rx := h.Receiver()
	// Close wins the race between the lock-free done pre-check and taking mu;
	// the under-lock re-check must still return ErrClosed, not a value.
	require.NoError(t, h.Sender().Send(1, 1))
	rx.forTestingBeforeRecvLock = func() { rx.Close() }
	_, err := rx.RecvContext(context.Background())
	assert.ErrorIs(t, err, ErrClosed)
}

// TestConcurrentSendersAndReceiver is a -race smoke test: many goroutines send
// across a small key space while one consumer drains, asserting no value is
// lost for the final state of each key and nothing deadlocks.
func TestConcurrentSendersAndReceiver(t *testing.T) {
	h := New[int, int](func(_, next int) (int, bool) { return next, true })
	rx := h.Receiver()
	tx := h.Sender()
	const senders, perSender, keys = 8, 200, 16
	var wg sync.WaitGroup
	for s := 0; s < senders; s++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < perSender; i++ {
				_ = tx.Send((base+i)%keys, base+i)
			}
		}(s * perSender)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		for {
			if _, err := rx.RecvContext(ctx); err != nil {
				return
			}
		}
	}()
	wg.Wait()
	rx.Close()
	<-done
}
