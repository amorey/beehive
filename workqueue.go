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
//
// get re-signals ready if more items remain, so callers naturally drain the
// queue without a separate loop.
type workQueue struct {
	mu         sync.Mutex
	dirty      map[ObjectID]struct{} // queued (in items) and awaiting dispatch
	processing map[ObjectID]struct{} // handed out via get, not yet done
	items      []ObjectID
	ready      chan struct{} // pulsed when items are available
}

func newWorkQueue() *workQueue {
	return &workQueue{
		dirty:      make(map[ObjectID]struct{}),
		processing: make(map[ObjectID]struct{}),
		ready:      make(chan struct{}, 1),
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
	if _, ok := q.dirty[id]; ok {
		return
	}
	q.dirty[id] = struct{}{}
	if _, ok := q.processing[id]; ok {
		// In flight: leave it dirty; done will re-queue it. Not dispatchable now.
		return
	}
	q.items = append(q.items, id)
	q.signal()
}

func (q *workQueue) signal() {
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
}
