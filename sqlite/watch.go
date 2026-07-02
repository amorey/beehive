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
func (s *sqliteStore) hubFor(gk storeapi.GroupKind) *conflate.Hub[storeapi.ObjectID, storeapi.RawChange] {
	s.hubMu.Lock()
	defer s.hubMu.Unlock()
	if s.closed {
		return nil
	}
	h := s.hubs[gk]
	if h == nil {
		h = conflate.New[storeapi.ObjectID](mergeChange)
		s.hubs[gk] = h
	}
	return h
}

// eventHubFor returns the event-log hub for gk, creating it on first use, or nil
// if the store is closed. Mirrors hubFor.
func (s *sqliteStore) eventHubFor(gk storeapi.GroupKind) *conflate.Hub[eventKey, storeapi.Event] {
	s.hubMu.Lock()
	defer s.hubMu.Unlock()
	if s.closed {
		return nil
	}
	h := s.eventHubs[gk]
	if h == nil {
		h = conflate.New[eventKey](mergeEvent)
		s.eventHubs[gk] = h
	}
	return h
}

// eventKey identifies a run in an event hub. Keying by run id (EventID) makes a
// run's count-bumps conflate into one slot while distinct runs stay separate;
// carrying ObjectID in the key lets a per-object subscriber filter by key alone,
// so its receiver never buffers other objects' runs.
type eventKey struct {
	ObjectID storeapi.ObjectID
	EventID  storeapi.EventID
}

// mergeEvent coalesces a run's pending event with a newer one: resource_version
// is globally monotonic, so the higher-versioned row is the newer run state.
// There are no tombstones, so it never drops the slot.
func mergeEvent(prev, next storeapi.Event) (storeapi.Event, bool) {
	if prev.ResourceVersion > next.ResourceVersion {
		return prev, true
	}
	return next, true
}

// mergeChange coalesces a receiver's undelivered pending event for an object
// with a newly published one. The store's resource_version is a global monotonic
// cursor, so the higher-versioned event is always the newer lifecycle state.
// A surviving update keeps Added type when prev was Added (it is still "new" to
// the consumer) while taking the latest body. A delete always keeps the tombstone:
// this shared default cannot annihilate, because a pending Added may represent an
// object that is already covered by the subscriber's snapshot (born in the
// subscribe→snapshot race window), in which case the consumer must still see the
// delete. The seenIDs guard in watch() drops tombstones for objects the consumer
// truly never observed. WatchList/WatchChanges override this with
// annihilatingMerge, which can drop such tombstones early (WatchList preserving
// snapshot-covered deletes, WatchChanges preserving nothing).
func mergeChange(prev, next storeapi.RawChange) (storeapi.RawChange, bool) {
	hi := next
	if prev.Object.ResourceVersion > next.Object.ResourceVersion {
		hi = prev
	}
	if hi.Type == storeapi.Deleted {
		return hi, true // real-body tombstone; seenIDs in watch() guards the rest
	}
	typ := hi.Type
	if prev.Type == storeapi.Added {
		typ = storeapi.Added // still new to the consumer
	}
	return storeapi.RawChange{Type: typ, Object: hi.Object}, true
}

// snapshotIDs is an immutable set of the ids a watcher's snapshot contained,
// published to a snapshot-based watcher's merge through an atomic pointer once
// the snapshot is loaded.
type snapshotIDs map[storeapi.ObjectID]struct{}

// annihilatingMerge is a per-receiver merge that extends mergeChange with one
// annihilation: when an undelivered pending Added coalesces with a Deleted, the
// consumer was never told the object existed, so the resulting tombstone is pure
// noise — drop the slot entirely. This is what keeps a slow consumer's memory
// bounded by the live key set instead of growing one tombstone per transient id
// in a high-churn kind. mergeChange (the shared default) cannot do this
// blindly: a snapshot-covered object born in the subscribe→snapshot race window
// also coalesces Added→Deleted, and its delete MUST survive. preserve reports the
// ids whose delete must be kept; an Added→Deleted pair is annihilated only when
// preserve is nil (WatchChanges has no snapshot, so nothing is pre-known) or
// preserve returns false for the id.
func annihilatingMerge(preserve func(storeapi.ObjectID) bool) conflate.Merge[storeapi.RawChange] {
	return func(prev, next storeapi.RawChange) (storeapi.RawChange, bool) {
		if prev.Type == storeapi.Added && next.Type == storeapi.Deleted {
			if preserve == nil || !preserve(next.Object.ID) {
				return storeapi.RawChange{}, false // unobserved transient: annihilate
			}
		}
		return mergeChange(prev, next)
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
	ev storeapi.RawChange
}

// pendingEventRow is an event-log run awaiting its transaction's commit.
type pendingEventRow struct {
	gk storeapi.GroupKind
	ev storeapi.Event
}

// eventCollector buffers events emitted during a transaction. The mutex guards
// against a Within whose fn fans store calls across goroutines on the tx ctx.
type eventCollector struct {
	mu      sync.Mutex
	events  []pendingEvent    // object watch events
	logRows []pendingEventRow // event-log runs
}

func (c *eventCollector) add(p pendingEvent) {
	c.mu.Lock()
	c.events = append(c.events, p)
	c.mu.Unlock()
}

func (c *eventCollector) addEventRow(p pendingEventRow) {
	c.mu.Lock()
	c.logRows = append(c.logRows, p)
	c.mu.Unlock()
}

func gkOf(raw *storeapi.RawObject) storeapi.GroupKind {
	return storeapi.GroupKind{Group: raw.Group, Kind: raw.Kind}
}

// emit delivers an event for the written row. Inside a transaction it queues on
// the ambient collector (flushed after commit by Within); outside one it
// publishes immediately.
func (s *sqliteStore) emit(ctx context.Context, typ storeapi.ChangeType, raw *storeapi.RawObject) {
	gk := gkOf(raw)
	ev := storeapi.RawChange{Type: typ, Object: raw}
	if c, ok := ctx.Value(collectorKey{}).(*eventCollector); ok {
		c.add(pendingEvent{gk: gk, ev: ev})
		return
	}
	s.publish(gk, ev)
}

// publish sends ev to gk's hub, keyed by object id so per-object updates
// coalesce. Send never blocks; a closed hub drops it.
func (s *sqliteStore) publish(gk storeapi.GroupKind, ev storeapi.RawChange) {
	if h := s.hubFor(gk); h != nil {
		_ = h.Sender().Send(ev.Object.ID, ev)
	}
}

// emitEvent delivers a written run to event-log watchers: queued on the tx
// collector inside a transaction (flushed after commit by Within), published
// immediately otherwise. Mirrors emit.
func (s *sqliteStore) emitEvent(ctx context.Context, gk storeapi.GroupKind, ev *storeapi.Event) {
	if c, ok := ctx.Value(collectorKey{}).(*eventCollector); ok {
		c.addEventRow(pendingEventRow{gk: gk, ev: *ev})
		return
	}
	s.publishEvent(gk, *ev)
}

// publishEvent sends a run to gk's event hub, keyed by (object, run) so per-run
// updates coalesce. Send never blocks; a closed hub drops it.
func (s *sqliteStore) publishEvent(gk storeapi.GroupKind, ev storeapi.Event) {
	if h := s.eventHubFor(gk); h != nil {
		_ = h.Sender().Send(eventKey{ObjectID: ev.ObjectID, EventID: ev.ID}, ev)
	}
}

// flush publishes a committed transaction's buffered events (object changes then
// event-log runs).
func (s *sqliteStore) flush(coll *eventCollector) {
	coll.mu.Lock()
	defer coll.mu.Unlock()
	for _, p := range coll.events {
		s.publish(p.gk, p.ev)
	}
	for _, p := range coll.logRows {
		s.publishEvent(p.gk, p.ev)
	}
}

// watcherImpl streams a snapshot followed by live items on out. A merge
// goroutine owns out and the receiver; Close cancels its context, which makes
// the goroutine exit, close the receiver, and close out. V is the streamed item
// type — RawChange for object watches, Event for the event log.
type watcherImpl[V any] struct {
	out    chan V
	cancel context.CancelFunc
}

// Changes and Events are the same accessor under the two interface names the
// shared impl satisfies: Watcher.Changes (V = RawChange) and EventWatcher.Events
// (V = Event). Each instantiation is only ever used through its own interface.
func (w *watcherImpl[V]) Changes() <-chan V { return w.out }
func (w *watcherImpl[V]) Events() <-chan V  { return w.out }
func (w *watcherImpl[V]) Close()            { w.cancel() }

func (s *sqliteStore) WatchList(ctx context.Context, gk storeapi.GroupKind) (storeapi.Watcher, error) {
	return s.watch(ctx, gk, nil, true, func(ctx context.Context) ([]*storeapi.RawObject, int64, error) {
		return s.snapshotAt(ctx, func(ctx context.Context) ([]*storeapi.RawObject, error) {
			return s.ListObjects(ctx, gk)
		})
	})
}

func (s *sqliteStore) WatchChanges(ctx context.Context, gk storeapi.GroupKind) (storeapi.Watcher, error) {
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
// snapshot) and false for WatchChanges (which skip the snapshot entirely).
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
//     Deleted (mergeChange never annihilates, to preserve real tombstones for
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
	//   - WatchList/WatchChanges: an annihilating merge so transient objects the
	//     consumer never saw are dropped at enqueue (memory bounded by the live
	//     key set, not by the count of distinct deleted ids a slow consumer falls
	//     behind on). WatchList must preserve snapshot-covered deletes; WatchChanges
	//     has no snapshot, so it annihilates every unobserved Added→Deleted pair.
	var rx *conflate.Receiver[storeapi.ObjectID, storeapi.RawChange]
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
	w := &watcherImpl[storeapi.RawChange]{out: make(chan storeapi.RawChange), cancel: cancel}
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
		send := func(ev storeapi.RawChange) bool {
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
		// Only used when there is a real snapshot; WatchChanges (hasSnapshot=false)
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
			if !send(storeapi.RawChange{Type: storeapi.Added, Object: raw}) {
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
				case storeapi.Added:
					if _, ok := seenIDs[ev.Object.ID]; ok {
						// Conflation promoted a race-window Added to Added, but the
						// consumer already has this object from the snapshot.
						ev.Type = storeapi.Modified
					} else {
						seenIDs[ev.Object.ID] = struct{}{}
					}
				case storeapi.Modified:
					seenIDs[ev.Object.ID] = struct{}{}
				case storeapi.Deleted:
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

// eventMatchesQuery reports whether a live run passes q's field filters. Limit
// bounds only the snapshot, so it is not applied here.
func eventMatchesQuery(ev storeapi.Event, q storeapi.EventQuery) bool {
	if q.Category != nil && ev.Category != *q.Category {
		return false
	}
	if q.Type != "" && ev.Type != q.Type {
		return false
	}
	if q.Reason != "" && ev.Reason != q.Reason {
		return false
	}
	// Compare at stored (millisecond) precision, matching ListEvents' toMillis(Since)
	// bound: a sub-millisecond Since (e.g. time.Now()) must not drop a live run in
	// that same millisecond that the snapshot query would keep.
	if !q.Since.IsZero() && toMillis(ev.LastAt) < toMillis(q.Since) {
		return false
	}
	return true
}

// WatchEvents streams id's event log within gk: the runs matching q as a
// snapshot, then live runs. The receiver is created before the snapshot loads so
// runs committed during the load are buffered, not lost; a run already reflected
// in the snapshot (resource_version at or below its high-water) is then dropped.
func (s *sqliteStore) WatchEvents(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, q storeapi.EventQuery) (storeapi.EventWatcher, error) {
	h := s.eventHubFor(gk)
	if h == nil {
		return nil, errStoreClosed
	}
	// Key-filter to this object: the run id in the key makes the filter exact
	// without inspecting values, so the receiver never buffers other objects' runs.
	rx := h.ReceiverFunc(func(k eventKey) bool { return k.ObjectID == id })
	if s.beforeSnapshot != nil {
		s.beforeSnapshot() // test seam: inject runs into the subscribe→snapshot window
	}
	// Snapshot the current runs and the global cursor in one read (snapshotAt's
	// event twin): the scalar high-water dedups any live run already listed.
	var snapshot []storeapi.Event
	var hw int64
	var objectExists bool
	err := s.Within(ctx, func(ctx context.Context) error {
		// Scope the snapshot to gk: the live stream is already gk-scoped (its hub),
		// so an unscoped ListEvents(id) would leak a foreign object's log and
		// disagree with the live half. A missing or wrong-kind id yields an empty
		// snapshot — the live stream delivers nothing for it either.
		var err error
		if objectExists, err = s.objectInKind(ctx, gk, id); err != nil {
			return err
		}
		if objectExists {
			if snapshot, err = s.ListEvents(ctx, id, q); err != nil {
				return err
			}
		}
		hw, err = currentResourceVersion(ctx, s.conn(ctx))
		return err
	})
	if err != nil {
		rx.Close()
		return nil, err
	}

	wctx, cancel := context.WithCancel(ctx)
	w := &watcherImpl[storeapi.Event]{out: make(chan storeapi.Event), cancel: cancel}
	go func() {
		if s.afterStream != nil {
			defer s.afterStream()
		}
		defer close(w.out)
		defer rx.Close()
		send := func(ev storeapi.Event) bool {
			select {
			case w.out <- ev:
				return true
			case <-wctx.Done():
				return false
			case <-s.done:
				return false
			}
		}
		// ListEvents is newest-first; deliver the snapshot oldest-first so the
		// timeline builds in order. Record which runs it carried, to dedup their
		// race-window republish below.
		seen := make(map[storeapi.EventID]struct{}, len(snapshot))
		for i := len(snapshot) - 1; i >= 0; i-- {
			seen[snapshot[i].ID] = struct{}{}
			if !send(snapshot[i]) {
				return
			}
		}
		snapshot = nil
		for {
			ev, err := rx.RecvContext(wctx)
			if err != nil {
				return // ctx cancelled, watcher closed, or hub closed
			}
			// Drop a run committed at or below the snapshot's high-water when it is
			// already reflected: either the snapshot delivered it, or the object was
			// deleted before the snapshot (its log empty by deletion, not Limit
			// truncation) so the buffered run is stale. A Limit-truncated run of a live
			// object is NOT dropped — Limit bounds only the snapshot, so it streams live.
			if _, inSnapshot := seen[ev.ID]; ev.ResourceVersion <= hw && (!objectExists || inSnapshot) {
				continue
			}
			if !eventMatchesQuery(ev, q) {
				continue // q filters the live stream too, not just the snapshot
			}
			if !send(ev) {
				return
			}
		}
	}()
	return w, nil
}
