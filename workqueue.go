package beehive

import (
	"sync"
	"time"
)

// workQueue is a FIFO queue of ObjectIDs with set semantics: adding an ID that
// is already queued is a no-op. It is safe for concurrent use.
//
// Callers select on ready, then call get to retrieve the next item. get
// re-signals ready if more items remain, so callers naturally drain the queue
// without a separate loop.
type workQueue struct {
	mu    sync.Mutex
	set   map[ObjectID]struct{}
	items []ObjectID
	ready chan struct{} // pulsed when items are available
}

func newWorkQueue() *workQueue {
	return &workQueue{
		set:   make(map[ObjectID]struct{}),
		ready: make(chan struct{}, 1),
	}
}

// add enqueues id if it is not already present.
func (q *workQueue) add(id ObjectID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.set[id]; ok {
		return
	}
	q.set[id] = struct{}{}
	q.items = append(q.items, id)
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

// addAfter enqueues id after delay has elapsed. A zero or negative delay
// enqueues immediately.
func (q *workQueue) addAfter(id ObjectID, delay time.Duration) {
	if delay <= 0 {
		q.add(id)
		return
	}
	time.AfterFunc(delay, func() { q.add(id) })
}

// get removes and returns the next item. If more items remain it re-signals
// ready so the consumer loops back immediately. Returns false if the queue is
// empty.
func (q *workQueue) get() (ObjectID, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return 0, false
	}
	id := q.items[0]
	q.items = q.items[1:]
	delete(q.set, id)
	if len(q.items) > 0 {
		select {
		case q.ready <- struct{}{}:
		default:
		}
	}
	return id, true
}
