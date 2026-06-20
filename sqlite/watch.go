package sqlite

import (
	"context"
	"errors"
	"sync"

	"github.com/amorey/beehive/internal/storeapi"
	"github.com/amorey/gochan"
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
	return s.watch(ctx, gk, nil, func(ctx context.Context) ([]*storeapi.RawObject, int64, error) {
		return s.snapshotAt(ctx, func(ctx context.Context) ([]*storeapi.RawObject, error) {
			return s.ListObjects(ctx, gk)
		})
	})
}

func (s *sqliteStore) WatchEvents(ctx context.Context, gk storeapi.GroupKind) (storeapi.Watcher, error) {
	// No snapshot, so the dedup floor is 0: a fresh receiver already starts at
	// the current write position, and resource_version is always >= 1, so every
	// event the receiver sees is genuinely post-subscribe and nothing is dropped.
	return s.watch(ctx, gk, nil, func(context.Context) ([]*storeapi.RawObject, int64, error) {
		return nil, 0, nil
	})
}

func (s *sqliteStore) Watch(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID) (storeapi.Watcher, error) {
	filterID := id
	return s.watch(ctx, gk, &filterID, func(ctx context.Context) ([]*storeapi.RawObject, int64, error) {
		return s.snapshotAt(ctx, func(ctx context.Context) ([]*storeapi.RawObject, error) {
			raw, err := s.GetObject(ctx, id)
			if errors.Is(err, storeapi.ErrNotFound) {
				return nil, nil // not found yet: empty snapshot, stream the Added when it lands
			}
			if err != nil {
				return nil, err
			}
			return []*storeapi.RawObject{raw}, nil
		})
	})
}

// snapshotAt runs load inside one consistent read and returns the listed objects
// together with the global resource-version cursor as of that read. Because
// resource_version is a single, globally monotonic cursor, that scalar is a
// complete dedup floor: every buffered event at or below it is already reflected
// in the returned objects, every later event is genuinely new. Reading the
// objects and the cursor in the same transaction is what makes the floor exact —
// a separate cursor read could span a write the list itself didn't, dropping a
// real event or replaying a snapshotted one. (A "max RV over the listed objects"
// shortcut can't substitute: a delete committed just before the snapshot removes
// its row, so its version is absent from the list yet must still be deduped.)
func (s *sqliteStore) snapshotAt(ctx context.Context, load func(context.Context) ([]*storeapi.RawObject, error)) ([]*storeapi.RawObject, int64, error) {
	var objs []*storeapi.RawObject
	var hw int64
	err := s.Within(ctx, func(ctx context.Context) error {
		var err error
		if objs, err = load(ctx); err != nil {
			return err
		}
		hw, err = currentResourceVersion(ctx, s.conn(ctx))
		return err
	})
	if err != nil {
		return nil, 0, err
	}
	return objs, hw, nil
}

// watch subscribes to gk's hub, loads a snapshot, and returns a Watcher whose
// stream is the snapshot (as Added events) followed by live events not already
// covered by the snapshot. filterID, if non-nil, restricts live events to that
// object.
//
// The receiver is created BEFORE the snapshot is loaded so events that commit
// during the load are buffered, not lost; events whose resource version is at or
// below the snapshot's global high-water are then dropped as duplicates.
func (s *sqliteStore) watch(
	ctx context.Context,
	gk storeapi.GroupKind,
	filterID *storeapi.ObjectID,
	loadSnapshot func(context.Context) ([]*storeapi.RawObject, int64, error),
) (storeapi.Watcher, error) {
	h := s.hubFor(gk)
	if h == nil {
		return nil, errStoreClosed
	}
	rx := h.Receiver()
	if s.beforeSnapshot != nil {
		s.beforeSnapshot() // test seam: inject events into the subscribe→snapshot window
	}
	snapshot, snapshotHighWaterRV, err := loadSnapshot(ctx)
	if err != nil {
		rx.Close()
		return nil, err
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
		// live is the set of IDs the watcher believes currently exist, so relist
		// can detect which vanished during a lag gap (live − relist) and report a
		// Deleted for each. We track IDs only, not bodies, so the cost stays bounded
		// to the live set and cheap. sendTracked keeps it current: a Deleted drops
		// the id, any other event records it.
		live := make(map[storeapi.ObjectID]struct{}, len(snapshot))
		sendTracked := func(ev storeapi.RawWatchEvent) bool {
			if ev.Type == storeapi.WatchEventDeleted {
				delete(live, ev.Object.ID)
			} else {
				live[ev.Object.ID] = struct{}{}
			}
			return send(ev)
		}
		// relist re-emits current state as Modified after the receiver drops
		// events (ErrLagged). A single-object watch re-fetches just that row; a
		// list/events watch re-lists the whole kind. It re-reads the store directly
		// rather than reusing loadSnapshot, because WatchEvents loads an empty
		// startup snapshot yet still needs the full current set here to re-converge
		// its consumer (the dependency waker, which acts only on Modified). The
		// dedup high-water is advanced to the relist's consistent read point so the
		// buffered events the lag left behind are deduped as we drain them. Any ID
		// we'd previously seen that is absent from the relist was deleted during the
		// gap, so we report a Deleted for it — otherwise a cache-maintaining consumer
		// would retain it forever. The row is gone, so the event carries a tombstone
		// (id + kind, null spec) rather than the real last object; a consumer keyed
		// by id can still evict it. Returns false if a send is abandoned (ctx
		// cancelled / store closed).
		relist := func() bool {
			objs, hw, err := s.snapshotAt(wctx, func(ctx context.Context) ([]*storeapi.RawObject, error) {
				if filterID != nil {
					raw, err := s.GetObject(ctx, *filterID)
					if err != nil && !errors.Is(err, storeapi.ErrNotFound) {
						return nil, err
					}
					if raw == nil {
						return nil, nil
					}
					return []*storeapi.RawObject{raw}, nil
				}
				return s.ListObjects(ctx, gk)
			})
			if err != nil {
				return false
			}
			snapshotHighWaterRV = hw
			present := make(map[storeapi.ObjectID]bool, len(objs))
			for _, raw := range objs {
				present[raw.ID] = true
				if !sendTracked(storeapi.RawWatchEvent{Type: storeapi.WatchEventModified, Object: raw}) {
					return false
				}
			}
			// Deleting from live mid-range (via sendTracked) is allowed in Go.
			for id := range live {
				if present[id] {
					continue
				}
				// Spec is JSON null, not "{}": the typed watch decodes every event's
				// spec, and null unmarshals into any Go type (scalar, slice, struct)
				// as the zero value, whereas "{}" fails for non-object specs and would
				// silently close the watch.
				tombstone := &storeapi.RawObject{ID: id, Group: gk.Group, Kind: gk.Kind, Spec: []byte("null")}
				if !sendTracked(storeapi.RawWatchEvent{Type: storeapi.WatchEventDeleted, Object: tombstone}) {
					return false
				}
			}
			return true
		}
		// Emit the snapshot as Added events before streaming live events.
		for _, raw := range snapshot {
			if !sendTracked(storeapi.RawWatchEvent{Type: storeapi.WatchEventAdded, Object: raw}) {
				return
			}
		}
		for {
			ev, err := rx.RecvContext(wctx)
			if err != nil {
				// ErrLagged is non-terminal: the receiver fell behind the ring and
				// dropped events but stays usable. Re-list so consumers don't
				// silently lose those changes (including deletes), then keep
				// streaming.
				if _, ok := errors.AsType[gochan.ErrLagged](err); ok {
					if !relist() {
						return
					}
					continue
				}
				return // ctx cancelled, watcher closed, or hub closed
			}
			if ev.Object.ResourceVersion <= snapshotHighWaterRV {
				continue // already represented by the snapshot
			}
			if filterID != nil && ev.Object.ID != *filterID {
				continue
			}
			if !sendTracked(ev) {
				return
			}
		}
	}()
	return w, nil
}
