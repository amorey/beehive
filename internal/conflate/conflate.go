// Package conflate provides a single-producer, multi-consumer keyed
// latest-value fan-out hub.
//
// Where [github.com/amorey/gochan/watch] keeps one latest-value slot and
// [github.com/amorey/gochan/broadcast] keeps a fixed ring of every value
// (returning ErrLagged on overflow), conflate keeps the latest value *per
// key*: each receiver holds one slot per key plus an insertion-ordered queue,
// and a [Sender.Send] for a key that is already pending coalesces into that
// slot rather than appending. A slow receiver therefore never lags-as-loss —
// it catches up to the latest value of every key, in first-touch order, and its
// memory stays bounded by the live key set rather than by write volume.
//
// Coalescing policy is supplied by the caller as a [Merge] function so the hub
// stays domain-agnostic: Merge decides how an undelivered pending value
// combines with a newly sent one, and may annihilate the slot entirely (e.g. a
// create followed by a delete the consumer never observed).
//
// Send never blocks. Closing the sender ([Sender.Close]) drains each receiver's
// pending values once before reporting [ErrClosed]; closing the hub
// ([Hub.Close]) is hard tear-down with no drain. A single [Receiver] is intended
// for one consumer goroutine.
package conflate

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

// ErrClosed is returned by [Sender.Send] and [Receiver.RecvContext] once the
// sender, receiver, or hub has been closed.
var ErrClosed = errors.New("conflate: closed")

// Merge combines an undelivered pending value with a newly sent value for the
// same key. It is invoked only when the key already has a pending (not yet
// delivered) slot. It returns the surviving value and whether to keep the slot;
// keep == false drops the key entirely (annihilation).
type Merge[V any] func(prev, next V) (merged V, keep bool)

// shared is the hub state common to the sender and every receiver. A single
// mutex guards all receivers' queues: Send fans a write across them and each
// Recv pops from its own, so one lock keeps enqueue/coalesce/pop consistent
// without per-receiver locking races.
type shared[K comparable, V any] struct {
	mu        sync.Mutex
	merge     Merge[V]
	receivers map[*Receiver[K, V]]struct{}
	txClosed  bool
	hubClosed bool
}

// Hub is the construction handle for a conflate pipeline.
type Hub[K comparable, V any] struct {
	s  *shared[K, V]
	tx *Sender[K, V]
}

// Sender is the singleton send-side handle. Safe to share across goroutines.
type Sender[K comparable, V any] struct{ s *shared[K, V] }

// Receiver is a receive-side handle, intended for one consumer goroutine. It
// holds an insertion-ordered key queue plus a per-key value slot; coalescing
// happens on Send into these structures, so Recv is a plain pop under the lock.
type Receiver[K comparable, V any] struct {
	s       *shared[K, V]
	keep    func(K) bool        // nil = accept all keys; else enqueue only matching keys
	merge   Merge[V]            // nil = use the hub's shared merge; else this receiver's own
	order   *list.List          // K in first-touch order; bounded by live keys
	elems   map[K]*list.Element // key -> its order element, for O(1) coalesce/remove
	pending map[K]V             // key -> latest undelivered value
	notify  chan struct{}       // closed+replaced to wake a parked Recv
	waiting bool                // this receiver is parked in Recv
	closed  bool
	done    chan struct{}

	// forTestingBeforeRecvLock, if non-nil, runs after the lock-free closed
	// check and before taking s.mu, so tests can exercise the close-wins-the-
	// race re-check under the lock. nil in production.
	forTestingBeforeRecvLock func()
}

// New creates a hub whose receivers coalesce per key using merge.
func New[K comparable, V any](merge Merge[V]) *Hub[K, V] {
	s := &shared[K, V]{merge: merge, receivers: make(map[*Receiver[K, V]]struct{})}
	return &Hub[K, V]{s: s, tx: &Sender[K, V]{s: s}}
}

// Sender returns the singleton send-side handle.
func (h *Hub[K, V]) Sender() *Sender[K, V] { return h.tx }

// Receiver returns a new receiver bound to the hub. If the hub is already
// closed the receiver is pre-closed and reports ErrClosed on use.
func (h *Hub[K, V]) Receiver() *Receiver[K, V] { return h.receiver(nil, nil) }

// ReceiverFunc returns a receiver that only enqueues keys for which keep
// returns true; all other keys are dropped at Send time and never buffered.
// Filtering at enqueue keeps a selective receiver's memory bounded by the keys
// it actually wants rather than the producer's whole key space — important for
// a receiver interested in a single key out of a high-cardinality producer.
// keep is called under the hub lock, so it must not call back into the hub.
func (h *Hub[K, V]) ReceiverFunc(keep func(K) bool) *Receiver[K, V] { return h.receiver(keep, nil) }

// ReceiverMerge returns a receiver that coalesces with its own merge instead of
// the hub's shared one. This lets a single consumer apply a stricter policy —
// e.g. annihilating pending values others must retain — without affecting the
// rest of the hub. merge is called under the hub lock, like the shared one.
func (h *Hub[K, V]) ReceiverMerge(merge Merge[V]) *Receiver[K, V] { return h.receiver(nil, merge) }

func (h *Hub[K, V]) receiver(keep func(K) bool, merge Merge[V]) *Receiver[K, V] {
	rx := &Receiver[K, V]{
		s:       h.s,
		keep:    keep,
		merge:   merge,
		order:   list.New(),
		elems:   make(map[K]*list.Element),
		pending: make(map[K]V),
		notify:  make(chan struct{}),
		done:    make(chan struct{}),
	}
	h.s.mu.Lock()
	if h.s.hubClosed {
		rx.closed = true
		close(rx.done)
	} else {
		h.s.receivers[rx] = struct{}{}
	}
	h.s.mu.Unlock()
	return rx
}

// Close is hard tear-down: the sender and every live receiver are closed
// immediately, with no final-value drain. Use [Sender.Close] for the soft path.
// Idempotent.
func (h *Hub[K, V]) Close() {
	s := h.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hubClosed {
		return
	}
	s.hubClosed = true
	s.txClosed = true
	for rx := range s.receivers {
		rx.closeLocked()
	}
	s.receivers = nil
}

// Send publishes v under key k to every receiver. Never blocks. For a receiver
// that already has k pending, the caller's Merge coalesces into that slot;
// otherwise k is appended at the back of the receiver's queue.
func (tx *Sender[K, V]) Send(k K, v V) error {
	s := tx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.txClosed {
		return ErrClosed
	}
	for rx := range s.receivers {
		rx.enqueueLocked(k, v)
	}
	return nil
}

// Close closes the sender. Receivers drain their pending values once before
// subsequent Recv calls return ErrClosed. Further Send calls return ErrClosed.
// Idempotent.
func (tx *Sender[K, V]) Close() {
	s := tx.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.txClosed {
		return
	}
	s.txClosed = true
	for rx := range s.receivers {
		rx.signalLocked()
	}
}

// enqueueLocked merges or appends v for key k. Caller holds s.mu.
func (rx *Receiver[K, V]) enqueueLocked(k K, v V) {
	if rx.keep != nil && !rx.keep(k) {
		return // not a key this receiver wants; never buffer it
	}
	if e, ok := rx.elems[k]; ok {
		merge := rx.merge
		if merge == nil {
			merge = rx.s.merge
		}
		merged, keep := merge(rx.pending[k], v)
		if keep {
			rx.pending[k] = merged // coalesce in place; queue position unchanged
		} else {
			rx.order.Remove(e) // annihilate: no residue in queue or slot
			delete(rx.elems, k)
			delete(rx.pending, k)
		}
	} else {
		rx.elems[k] = rx.order.PushBack(k)
		rx.pending[k] = v
	}
	rx.signalLocked()
}

// signalLocked wakes this receiver if it is parked. Caller holds s.mu.
func (rx *Receiver[K, V]) signalLocked() {
	if rx.waiting {
		close(rx.notify)
		rx.notify = make(chan struct{})
	}
}

// closeLocked closes the receiver once. Caller holds s.mu.
func (rx *Receiver[K, V]) closeLocked() {
	if !rx.closed {
		rx.closed = true
		close(rx.done)
	}
}

// RecvContext blocks until a key is pending, then pops and returns the oldest
// pending key's value. It returns ctx.Err() if ctx is cancelled first, or
// ErrClosed once the receiver/hub is closed (with the sender's soft
// close, after the pending values have drained).
func (rx *Receiver[K, V]) RecvContext(ctx context.Context) (V, error) {
	var z V
	ctxDone := ctx.Done()
	parked := false
	defer func() {
		if parked {
			rx.s.mu.Lock()
			rx.waiting = false
			rx.s.mu.Unlock()
		}
	}()
	for {
		select {
		case <-rx.done:
			return z, ErrClosed
		default:
		}
		if rx.forTestingBeforeRecvLock != nil {
			rx.forTestingBeforeRecvLock()
		}
		rx.s.mu.Lock()
		if parked {
			rx.waiting = false
			parked = false
		}
		// Re-check closed under the lock: Close serializes through s.mu, so a
		// Close that won the race against the pre-lock check is visible here and
		// cannot be handed a pending value.
		select {
		case <-rx.done:
			rx.s.mu.Unlock()
			return z, ErrClosed
		default:
		}
		if e := rx.order.Front(); e != nil {
			k := e.Value.(K)
			rx.order.Remove(e)
			delete(rx.elems, k)
			v := rx.pending[k]
			delete(rx.pending, k)
			rx.s.mu.Unlock()
			return v, nil
		}
		if rx.s.txClosed {
			rx.s.mu.Unlock()
			return z, ErrClosed
		}
		rx.waiting = true
		parked = true
		notify := rx.notify
		rx.s.mu.Unlock()
		select {
		case <-notify:
		case <-rx.done:
			return z, ErrClosed
		case <-ctxDone:
			return z, ctx.Err()
		}
	}
}

// Close closes this receiver only; other receivers and the sender are
// unaffected. Any pending values are abandoned. Idempotent.
func (rx *Receiver[K, V]) Close() {
	rx.s.mu.Lock()
	rx.closeLocked()
	delete(rx.s.receivers, rx)
	rx.s.mu.Unlock()
}

// lenForTest reports the number of pending keys. Test-only.
func (rx *Receiver[K, V]) lenForTest() int {
	rx.s.mu.Lock()
	defer rx.s.mu.Unlock()
	return rx.order.Len()
}
