package sqlite

import (
	"context"
	"errors"
	"sync"

	"github.com/amorey/beehive/internal/storeapi"
	"github.com/amorey/gochan/broadcast"
)

// errStoreClosed is returned by Watch/WatchList once the store has been closed.
var errStoreClosed = errors.New("beehive/sqlite: store is closed")

// hubBufferSize is the per-kind broadcast ring capacity. A receiver that lags
// further than this behind the sender drops the oldest unread events.
const hubBufferSize = 256

// hubFor returns the broadcast hub for gk, creating it on first use. It returns
// nil if the store is closed. Hub lookup is not a hot path (the store
// serializes writes on a single connection), so a single write lock is simpler
// than double-checked locking and avoids a race-only, untestable branch.
func (s *sqliteStore) hubFor(gk storeapi.GroupKind) *broadcast.Hub[storeapi.RawWatchEvent] {
	s.hubMu.Lock()
	defer s.hubMu.Unlock()
	if s.closed {
		return nil
	}
	h := s.hubs[gk]
	if h == nil {
		h = broadcast.New[storeapi.RawWatchEvent](hubBufferSize)
		s.hubs[gk] = h
	}
	return h
}

// collectorKey carries the in-flight transaction's eventCollector through the
// context, mirroring txKey, so mutators can buffer events until Within commits.
type collectorKey struct{}

// pendingEvent is a watch event awaiting its transaction's commit.
type pendingEvent struct {
	gk storeapi.GroupKind
	ev storeapi.RawWatchEvent
}

// eventCollector buffers events emitted during a transaction. The mutex guards
// against a Within whose fn fans store calls across goroutines on the tx ctx.
type eventCollector struct {
	mu     sync.Mutex
	events []pendingEvent
}

func (c *eventCollector) add(p pendingEvent) {
	c.mu.Lock()
	c.events = append(c.events, p)
	c.mu.Unlock()
}

func gkOf(raw *storeapi.RawObject) storeapi.GroupKind {
	return storeapi.GroupKind{Group: raw.Group, Kind: raw.Kind}
}

// emit delivers an event for the written row. Inside a transaction it queues on
// the ambient collector (flushed after commit by Within); outside one it
// publishes immediately.
func (s *sqliteStore) emit(ctx context.Context, typ storeapi.WatchEventType, raw *storeapi.RawObject) {
	gk := gkOf(raw)
	ev := storeapi.RawWatchEvent{Type: typ, Object: raw}
	if c, ok := ctx.Value(collectorKey{}).(*eventCollector); ok {
		c.add(pendingEvent{gk: gk, ev: ev})
		return
	}
	s.publish(gk, ev)
}

// publish sends ev to gk's hub. Send never blocks; a closed hub drops it.
func (s *sqliteStore) publish(gk storeapi.GroupKind, ev storeapi.RawWatchEvent) {
	if h := s.hubFor(gk); h != nil {
		_ = h.Sender().Send(ev)
	}
}

// flush publishes a committed transaction's buffered events.
func (s *sqliteStore) flush(coll *eventCollector) {
	coll.mu.Lock()
	defer coll.mu.Unlock()
	for _, p := range coll.events {
		s.publish(p.gk, p.ev)
	}
}

// watcherImpl streams a snapshot followed by live events on out. A merge
// goroutine owns out and the receiver; Close cancels its context, which makes
// the goroutine exit, close the receiver, and close out.
type watcherImpl struct {
	out    chan storeapi.RawWatchEvent
	cancel context.CancelFunc
}

func (w *watcherImpl) Events() <-chan storeapi.RawWatchEvent { return w.out }
func (w *watcherImpl) Close()                                { w.cancel() }

func (s *sqliteStore) WatchList(ctx context.Context, gk storeapi.GroupKind) (storeapi.Watcher, error) {
	return s.watch(ctx, gk, nil, func(ctx context.Context) ([]*storeapi.RawObject, error) {
		return s.ListObjects(ctx, gk)
	})
}

func (s *sqliteStore) WatchEvents(ctx context.Context, gk storeapi.GroupKind) (storeapi.Watcher, error) {
	// An empty snapshot loader streams live events only: the snapshotRV dedup
	// degrades to "<= 0", and resource_version is always >= 1, so nothing is
	// dropped.
	return s.watch(ctx, gk, nil, func(context.Context) ([]*storeapi.RawObject, error) {
		return nil, nil
	})
}

func (s *sqliteStore) Watch(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID) (storeapi.Watcher, error) {
	filterID := id
	return s.watch(ctx, gk, &filterID, func(ctx context.Context) ([]*storeapi.RawObject, error) {
		raw, err := s.GetObject(ctx, id)
		if errors.Is(err, storeapi.ErrNotFound) {
			return nil, nil // not found yet: empty snapshot, stream the Added when it lands
		}
		if err != nil {
			return nil, err
		}
		return []*storeapi.RawObject{raw}, nil
	})
}

// watch subscribes to gk's hub, loads a snapshot, and returns a Watcher whose
// stream is the snapshot (as Added events) followed by live events not already
// covered by the snapshot. filterID, if non-nil, restricts live events to that
// object.
//
// The receiver is created BEFORE the snapshot is loaded so events that commit
// during the load are buffered, not lost; events whose resource version is at
// or below the snapshot's for the same object are then dropped as duplicates.
func (s *sqliteStore) watch(
	ctx context.Context,
	gk storeapi.GroupKind,
	filterID *storeapi.ObjectID,
	loadSnapshot func(context.Context) ([]*storeapi.RawObject, error),
) (storeapi.Watcher, error) {
	h := s.hubFor(gk)
	if h == nil {
		return nil, errStoreClosed
	}
	rx := h.Receiver()
	if s.beforeSnapshot != nil {
		s.beforeSnapshot() // test seam: inject events into the subscribe→snapshot window
	}
	snapshot, err := loadSnapshot(ctx)
	if err != nil {
		rx.Close()
		return nil, err
	}
	snapshotRV := make(map[storeapi.ObjectID]int64, len(snapshot))
	for _, raw := range snapshot {
		snapshotRV[raw.ID] = raw.ResourceVersion
	}

	wctx, cancel := context.WithCancel(ctx)
	w := &watcherImpl{out: make(chan storeapi.RawWatchEvent), cancel: cancel}
	go func() {
		// Registered first so it runs last (after out is closed), letting tests
		// await exit without reading out.
		if s.afterStream != nil {
			defer s.afterStream()
		}
		defer close(w.out)
		defer rx.Close()
		// send delivers ev, or reports false if a reader never takes it because
		// the caller's context was cancelled (wctx) or the store was closed
		// (s.done). The store-close arm matters when no one is reading: closing
		// the hub only wakes a receive, not a parked send.
		send := func(ev storeapi.RawWatchEvent) bool {
			select {
			case w.out <- ev:
				return true
			case <-wctx.Done():
				return false
			case <-s.done:
				return false
			}
		}
		// Emit the snapshot as Added events before streaming live events.
		for _, raw := range snapshot {
			if !send(storeapi.RawWatchEvent{Type: storeapi.WatchEventAdded, Object: raw}) {
				return
			}
		}
		for {
			ev, err := rx.RecvContext(wctx)
			if err != nil {
				return // ctx cancelled, watcher closed, or hub closed
			}
			if ev.Object.ResourceVersion <= snapshotRV[ev.Object.ID] {
				continue // already represented by the snapshot
			}
			if filterID != nil && ev.Object.ID != *filterID {
				continue
			}
			if !send(ev) {
				return
			}
		}
	}()
	return w, nil
}
