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

package sqlite

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/amorey/beehive/internal/conflate"
	"github.com/amorey/beehive/internal/storeapi"
)

// errStoreClosed is returned by Watch/WatchList once the store has been closed.
var errStoreClosed = errors.New("beehive/sqlite: store is closed")

// hubFor returns the conflating hub for gk, creating it on first use. It returns
// nil if the store is closed. Hub lookup is not a hot path (the store
// serializes writes on a single connection), so a single write lock is simpler
// than double-checked locking and avoids a race-only, untestable branch.
func (s *sqliteStore) hubFor(gk storeapi.GroupKind) *conflate.Hub[storeapi.ObjectID, storeapi.RawWatchEvent] {
	s.hubMu.Lock()
	defer s.hubMu.Unlock()
	if s.closed {
		return nil
	}
	h := s.hubs[gk]
	if h == nil {
		h = conflate.New[storeapi.ObjectID](mergeWatchEvent)
		s.hubs[gk] = h
	}
	return h
}

// mergeWatchEvent coalesces a receiver's undelivered pending event for an object
// with a newly published one. The store's resource_version is a global monotonic
// cursor, so the higher-versioned event is always the newer lifecycle state.
// A surviving update keeps Added type when prev was Added (it is still "new" to
// the consumer) while taking the latest body. A delete always keeps the tombstone:
// this shared default cannot annihilate, because a pending Added may represent an
// object that is already covered by the subscriber's snapshot (born in the
// subscribe→snapshot race window), in which case the consumer must still see the
// delete. The seenIDs guard in watch() drops tombstones for objects the consumer
// truly never observed. WatchList/WatchEvents override this with
// annihilatingMerge, which can drop such tombstones early (WatchList preserving
// snapshot-covered deletes, WatchEvents preserving nothing).
func mergeWatchEvent(prev, next storeapi.RawWatchEvent) (storeapi.RawWatchEvent, bool) {
	hi := next
	if prev.Object.ResourceVersion > next.Object.ResourceVersion {
		hi = prev
	}
	if hi.Type == storeapi.WatchEventDeleted {
		return hi, true // real-body tombstone; seenIDs in watch() guards the rest
	}
	typ := hi.Type
	if prev.Type == storeapi.WatchEventAdded {
		typ = storeapi.WatchEventAdded // still new to the consumer
	}
	return storeapi.RawWatchEvent{Type: typ, Object: hi.Object}, true
}

// snapshotIDs is an immutable set of the ids a watcher's snapshot contained,
// published to a snapshot-based watcher's merge through an atomic pointer once
// the snapshot is loaded.
type snapshotIDs map[storeapi.ObjectID]struct{}

// annihilatingMerge is a per-receiver merge that extends mergeWatchEvent with one
// annihilation: when an undelivered pending Added coalesces with a Deleted, the
// consumer was never told the object existed, so the resulting tombstone is pure
// noise — drop the slot entirely. This is what keeps a slow consumer's memory
// bounded by the live key set instead of growing one tombstone per transient id
// in a high-churn kind. mergeWatchEvent (the shared default) cannot do this
// blindly: a snapshot-covered object born in the subscribe→snapshot race window
// also coalesces Added→Deleted, and its delete MUST survive. preserve reports the
// ids whose delete must be kept; an Added→Deleted pair is annihilated only when
// preserve is nil (WatchEvents has no snapshot, so nothing is pre-known) or
// preserve returns false for the id.
func annihilatingMerge(preserve func(storeapi.ObjectID) bool) conflate.Merge[storeapi.RawWatchEvent] {
	return func(prev, next storeapi.RawWatchEvent) (storeapi.RawWatchEvent, bool) {
		if prev.Type == storeapi.WatchEventAdded && next.Type == storeapi.WatchEventDeleted {
			if preserve == nil || !preserve(next.Object.ID) {
				return storeapi.RawWatchEvent{}, false // unobserved transient: annihilate
			}
		}
		return mergeWatchEvent(prev, next)
	}
}

// snapshotPreserve builds the preserve predicate for a snapshot-based watcher: a
// delete is kept while the snapshot id set is not yet known (the race window,
// where membership cannot be decided — those few leftovers are bounded by the
// snapshot window and dropped at delivery by the seenIDs orphan guard) or when
// the id was in the snapshot (the consumer learned of it and must see its delete).
func snapshotPreserve(seed *atomic.Pointer[snapshotIDs]) func(storeapi.ObjectID) bool {
	return func(id storeapi.ObjectID) bool {
		ids := seed.Load()
		if ids == nil {
			return true
		}
		_, inSnapshot := (*ids)[id]
		return inSnapshot
	}
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

// publish sends ev to gk's hub, keyed by object id so per-object updates
// coalesce. Send never blocks; a closed hub drops it.
func (s *sqliteStore) publish(gk storeapi.GroupKind, ev storeapi.RawWatchEvent) {
	if h := s.hubFor(gk); h != nil {
		_ = h.Sender().Send(ev.Object.ID, ev)
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
	return s.watch(ctx, gk, nil, true, func(ctx context.Context) ([]*storeapi.RawObject, int64, error) {
		return s.snapshotAt(ctx, func(ctx context.Context) ([]*storeapi.RawObject, error) {
			return s.ListObjects(ctx, gk)
		})
	})
}

func (s *sqliteStore) WatchEvents(ctx context.Context, gk storeapi.GroupKind) (storeapi.Watcher, error) {
	// No snapshot, so the dedup floor is 0: a fresh receiver already starts at
	// the current write position, and resource_version is always >= 1, so every
	// event the receiver sees is genuinely post-subscribe and nothing is dropped.
	return s.watch(ctx, gk, nil, false, func(context.Context) ([]*storeapi.RawObject, int64, error) {
		return nil, 0, nil
	})
}

func (s *sqliteStore) Watch(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID) (storeapi.Watcher, error) {
	filterID := id
	return s.watch(ctx, gk, &filterID, true, func(ctx context.Context) ([]*storeapi.RawObject, int64, error) {
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
// object. hasSnapshot must be true for WatchList/Watch (which take a real
// snapshot) and false for WatchEvents (which skip the snapshot entirely).
//
// The receiver is created BEFORE the snapshot is loaded so events that commit
// during the load are buffered, not lost; events whose resource version is at or
// below the snapshot's global high-water are then dropped as duplicates.
//
// When hasSnapshot is true, a seenIDs set tracks which objects the consumer has
// been told about (via the snapshot or a live Added). It serves two roles:
//   - A race-window Added for object X followed by a post-snapshot Modified
//     coalesces to Added in the buffer; seenIDs detects that the consumer already
//     has X from the snapshot and promotes the type to Modified.
//   - A race-window Added for X followed by a post-snapshot Deleted coalesces to
//     Deleted (mergeWatchEvent never annihilates, to preserve real tombstones for
//     snapshot-covered objects); if X was in the snapshot seenIDs lets it through,
//     otherwise it is dropped — the object was born and died without the consumer
//     ever observing it, and emitting a lone Deleted would be spurious.
func (s *sqliteStore) watch(
	ctx context.Context,
	gk storeapi.GroupKind,
	filterID *storeapi.ObjectID,
	hasSnapshot bool,
	loadSnapshot func(context.Context) ([]*storeapi.RawObject, int64, error),
) (storeapi.Watcher, error) {
	h := s.hubFor(gk)
	if h == nil {
		return nil, errStoreClosed
	}
	// Register exactly the receiver we keep — an unfiltered one created first
	// would leak as a live hub subscriber that buffers every object forever.
	//   - Single-object watch: scope the subscription to that id so the receiver
	//     never buffers unrelated objects (memory bounded by the one id).
	//   - WatchList/WatchEvents: an annihilating merge so transient objects the
	//     consumer never saw are dropped at enqueue (memory bounded by the live
	//     key set, not by the count of distinct deleted ids a slow consumer falls
	//     behind on). WatchList must preserve snapshot-covered deletes; WatchEvents
	//     has no snapshot, so it annihilates every unobserved Added→Deleted pair.
	var rx *conflate.Receiver[storeapi.ObjectID, storeapi.RawWatchEvent]
	var seed atomic.Pointer[snapshotIDs] // published to the merge once the snapshot is known
	switch {
	case filterID != nil:
		want := *filterID
		rx = h.ReceiverFunc(func(id storeapi.ObjectID) bool { return id == want })
	case hasSnapshot:
		rx = h.ReceiverMerge(annihilatingMerge(snapshotPreserve(&seed)))
	default:
		rx = h.ReceiverMerge(annihilatingMerge(nil))
	}
	if s.beforeSnapshot != nil {
		s.beforeSnapshot() // test seam: inject events into the subscribe→snapshot window
	}
	snapshot, snapshotHighWaterRV, err := loadSnapshot(ctx)
	if err != nil {
		rx.Close()
		return nil, err
	}
	// Publish the snapshot's id set so listMerge can distinguish a snapshot-covered
	// object's delete (must survive) from a transient one's (annihilate). Stored
	// before the stream goroutine starts; concurrent race-window enqueues that ran
	// while seed was nil kept conservatively and are reconciled at delivery.
	if hasSnapshot && filterID == nil {
		ids := make(snapshotIDs, len(snapshot))
		for _, raw := range snapshot {
			ids[raw.ID] = struct{}{}
		}
		seed.Store(&ids)
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
		// seenIDs tracks every object ID the consumer has been told about, so
		// the live stream can correct event types and drop orphan tombstones.
		// Only used when there is a real snapshot; WatchEvents (hasSnapshot=false)
		// streams raw live events and needs no correction.
		var seenIDs map[storeapi.ObjectID]struct{}
		if hasSnapshot {
			seenIDs = make(map[storeapi.ObjectID]struct{}, len(snapshot))
		}
		// Emit the snapshot as Added events before streaming live events, then
		// release it: the goroutine outlives the snapshot by the whole streaming
		// lifetime, and holding the slice would pin every object's spec/status
		// blobs until the watcher closes.
		for _, raw := range snapshot {
			seenIDs[raw.ID] = struct{}{}
			if !send(storeapi.RawWatchEvent{Type: storeapi.WatchEventAdded, Object: raw}) {
				return
			}
		}
		snapshot = nil
		// The conflating hub never drops events — it coalesces per object — so a
		// lagging watcher converges to each object's latest state (including a
		// delete, which carries the real final row) rather than observing a gap.
		// No relist or tombstone synthesis is needed.
		for {
			ev, err := rx.RecvContext(wctx)
			if err != nil {
				return // ctx cancelled, watcher closed, or hub closed
			}
			if ev.Object.ResourceVersion <= snapshotHighWaterRV {
				continue // already represented by the snapshot
			}
			// No id filter here: a single-object watch uses an id-scoped receiver
			// (see filterID above), so unrelated ids never reach this loop.
			if seenIDs != nil {
				switch ev.Type {
				case storeapi.WatchEventAdded:
					if _, ok := seenIDs[ev.Object.ID]; ok {
						// Conflation promoted a race-window Added to Added, but the
						// consumer already has this object from the snapshot.
						ev.Type = storeapi.WatchEventModified
					} else {
						seenIDs[ev.Object.ID] = struct{}{}
					}
				case storeapi.WatchEventModified:
					seenIDs[ev.Object.ID] = struct{}{}
				case storeapi.WatchEventDeleted:
					if _, ok := seenIDs[ev.Object.ID]; !ok {
						// Object was born and died without the consumer ever observing
						// it (race-window Added coalesced into this Deleted, but the
						// object was not in the snapshot). Drop the orphan tombstone.
						continue
					}
					delete(seenIDs, ev.Object.ID)
				}
			}
			if !send(ev) {
				return
			}
		}
	}()
	return w, nil
}
