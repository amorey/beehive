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

	"github.com/amorey/beehive/internal/storeapi"
	"github.com/amorey/gobus"
	"github.com/amorey/gobus/conflate"
)

// errStoreClosed is returned by Watch/ObjectsWatchList once the store has been closed.
var errStoreClosed = errors.New("beehive/sqlite: store is closed")

// isClosed reports whether Close has run. The flag is guarded by hubMu, the same
// lock hubFor takes for the same reason.
func (s *sqliteStore) isClosed() bool {
	s.hubMu.Lock()
	defer s.hubMu.Unlock()
	return s.closed
}

// hubFor returns the conflating hub for gk, creating it on first use. It returns
// nil if the store is closed. Hub lookup is not a hot path (the store
// serializes writes on a single connection), so a single write lock is simpler
// than double-checked locking and avoids a race-only, untestable branch.
func (s *sqliteStore) hubFor(gk storeapi.GroupKind) *conflate.Hub[storeapi.ObjectID, storeapi.RawObjectChange] {
	s.hubMu.Lock()
	defer s.hubMu.Unlock()
	if s.closed {
		return nil
	}
	h := s.hubs[gk]
	if h == nil {
		h = conflate.New[storeapi.ObjectID](changeMerge)
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
		h = conflate.New[eventKey](eventMerge)
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

// eventMerge coalesces a run's pending event with a newer one: resource_version
// is globally monotonic, so the higher-versioned row is the newer run state.
// There are no tombstones, so it never drops the slot.
func eventMerge(prev, next storeapi.Event) (storeapi.Event, bool) {
	if prev.ResourceVersion > next.ResourceVersion {
		return prev, true
	}
	return next, true
}

// changeMerge coalesces a receiver's undelivered pending event for an object
// with a newly published one. The store's resource_version is a global monotonic
// cursor, so the higher-versioned event is always the newer lifecycle state.
// A surviving update keeps Added type when prev was Added (it is still "new" to
// the consumer) while taking the latest body. A delete always keeps the tombstone:
// this shared default cannot annihilate, because a pending Added may represent an
// object that is already covered by the subscriber's snapshot (born in the
// subscribe→snapshot race window), in which case the consumer must still see the
// delete. The seenIDs guard in watch() drops tombstones for objects the consumer
// truly never observed. ObjectsWatchList overrides this with transientDropMerge, which
// can drop such tombstones early while preserving snapshot-covered ones. The
// store-wide stream shares neither: it carries writeSignal, and writeSignalMerge is
// its own policy.
func changeMerge(prev, next storeapi.RawObjectChange) (storeapi.RawObjectChange, bool) {
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
	return storeapi.RawObjectChange{Type: typ, Object: hi.Object}, true
}

// writeSignal is what the store-wide hub carries. Identity is already the hub key,
// so the value holds only the lifecycle type and the resource_version conflation
// compares on — deliberately not a RawObjectChange: this hub sees every write in the
// process, and a pending *RawObject would pin that row's spec and status blobs
// until the value is delivered.
type writeSignal struct {
	typ storeapi.ChangeType
	rv  int64

	// firstRV is the version of the write that *created* this slot, where rv is the
	// newest one merged into it. A consumer reading the stream as a cursor needs a
	// version below which nothing is still queued, and delivery is in first-touch
	// order — so the head of the backlog holds the earliest-touched key, and its
	// firstRV is the lowest version still pending. Merge must never advance it; see
	// writeSignalMerge.
	firstRV int64
}

// writeSignalMerge is changeMerge plus annihilation, over the projected value. The
// store-wide stream has no snapshot, so nothing is pre-known: an Added the
// consumer never saw, coalescing with a Deleted, is a transient object it has no
// reason to hear about at all — dropping the slot is what bounds a slow
// consumer's memory by the live key set instead of by churn.
func writeSignalMerge(prev, next writeSignal) (writeSignal, bool) {
	if prev.typ == storeapi.Added && next.typ == storeapi.Deleted {
		return writeSignal{}, false // unobserved transient: annihilate
	}
	hi := next
	if prev.rv > next.rv {
		hi = prev
	}
	// The slot keeps the newest rv but the *earliest* firstRV: rv is what a consumer
	// reads as "the state to go look at", while firstRV is how far back the slot
	// reaches. prev is already in the slot, so prev.firstRV is the first touch; the
	// min is belt-and-braces for a publisher that sends out of version order, where
	// this would otherwise advance.
	hi.firstRV = min(prev.firstRV, next.firstRV)
	if hi.typ == storeapi.Deleted {
		return hi, true
	}
	if prev.typ == storeapi.Added {
		hi.typ = storeapi.Added // still new to the consumer
	}
	return hi, true
}

// snapshotIDs is an immutable set of the ids a watcher's snapshot contained,
// published to a snapshot-based watcher's merge through an atomic pointer once
// the snapshot is loaded.
type snapshotIDs map[storeapi.ObjectID]struct{}

// transientDropMerge is a per-receiver merge that extends changeMerge with one
// annihilation: when an undelivered pending Added coalesces with a Deleted, the
// consumer was never told the object existed, so the resulting tombstone is pure
// noise — drop the slot entirely. This is what keeps a slow consumer's memory
// bounded by the live key set instead of growing one tombstone per transient id
// in a high-churn kind. changeMerge (the shared default) cannot do this
// blindly: a snapshot-covered object born in the subscribe→snapshot race window
// also coalesces Added→Deleted, and its delete MUST survive. preserve, which is
// required, reports the ids whose delete must be kept.
//
// Only ObjectsWatchList reaches this. The snapshot-less consumer is the store-wide
// stream, whose annihilation lives in writeSignalMerge — unconditional there,
// because it has no snapshot to preserve deletes for.
func transientDropMerge(preserve func(storeapi.ObjectID) bool) conflate.Merge[storeapi.RawObjectChange] {
	return func(prev, next storeapi.RawObjectChange) (storeapi.RawObjectChange, bool) {
		if prev.Type == storeapi.Added && next.Type == storeapi.Deleted && !preserve(next.Object.ID) {
			return storeapi.RawObjectChange{}, false // unobserved transient: annihilate
		}
		return changeMerge(prev, next)
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

// pendingEvent is a watch event awaiting its transaction's commit.
type pendingEvent struct {
	gk storeapi.GroupKind
	ev storeapi.RawObjectChange
}

// pendingEventRow is an event-log run awaiting its transaction's commit.
type pendingEventRow struct {
	gk storeapi.GroupKind
	ev storeapi.Event
}

// eventCollector buffers events emitted during a transaction. The mutex guards
// against a Within whose fn fans store calls across goroutines on the tx ctx.
//
// It outlives its transaction: a post-commit hook can hold the tx ctx it was
// registered on (rather than the detached one it is handed) and reach the
// collector through it. Buffering there would be a silent drop, since flush has
// already drained it — so the collector latches flushed and every add reports
// whether it took ownership, leaving the caller to act immediately instead.
type eventCollector struct {
	mu      sync.Mutex
	events  []pendingEvent    // object watch events
	logRows []pendingEventRow // event-log runs
	hooks   []func()          // post-commit callbacks, run after the events
	flushed bool              // set by flush; buffering is closed from here on
}

// buffer runs store under the lock unless the collector is already flushed,
// reporting whether it took ownership. A false return means flush has drained
// the collector (a hook reaching it through a captured tx ctx), so the caller
// must act now rather than queue where nothing will look again.
func (c *eventCollector) buffer(store func()) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.flushed {
		return false
	}
	store()
	return true
}

func (c *eventCollector) add(p pendingEvent) bool {
	return c.buffer(func() { c.events = append(c.events, p) })
}

func (c *eventCollector) addEventRow(p pendingEventRow) bool {
	return c.buffer(func() { c.logRows = append(c.logRows, p) })
}

func (c *eventCollector) addHook(fn func()) bool {
	return c.buffer(func() { c.hooks = append(c.hooks, fn) })
}

func gkOf(raw *storeapi.RawObject) storeapi.GroupKind {
	return storeapi.GroupKind{Group: raw.Group, Kind: raw.Kind}
}

// changeEmit delivers an event for the written row. Inside a transaction it queues on
// the ambient collector (flushed after commit by Within); outside one it
// publishes immediately.
func (s *sqliteStore) changeEmit(ctx context.Context, typ storeapi.ChangeType, raw *storeapi.RawObject) {
	gk := gkOf(raw)
	ev := storeapi.RawObjectChange{Type: typ, Object: raw}
	if st, ok := txFrom(ctx); ok && st.coll.add(pendingEvent{gk: gk, ev: ev}) {
		return
	}
	s.changePublish(gk, ev)
}

// changePublish sends ev to gk's hub and to the store-wide writeHub, keyed by object
// id so per-object updates coalesce in both. Send never blocks; a closed hub
// drops it, which is also what makes the unguarded writeHub send safe after
// Close.
func (s *sqliteStore) changePublish(gk storeapi.GroupKind, ev storeapi.RawObjectChange) {
	if h := s.hubFor(gk); h != nil {
		_ = h.Sender().Send(ev.Object.ID, ev)
	}
	// The store-wide hub carries the projection, not the row: see writeSignal.
	// firstRV starts equal to rv: Merge is never called on a first touch, so the
	// value stored verbatim here is what establishes the field for the slot's life.
	rv := ev.Object.ResourceVersion
	_ = s.writeHub.Sender().Send(ev.Object.ID, writeSignal{typ: ev.Type, rv: rv, firstRV: rv})
}

// eventEmit delivers a written run to event-log watchers: queued on the tx
// collector inside a transaction (flushed after commit by Within), published
// immediately otherwise. Mirrors emit.
func (s *sqliteStore) eventEmit(ctx context.Context, gk storeapi.GroupKind, ev *storeapi.Event) {
	if st, ok := txFrom(ctx); ok && st.coll.addEventRow(pendingEventRow{gk: gk, ev: *ev}) {
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
// event-log runs) and returns its post-commit hooks for the caller to run after
// them — a hook that wakes a reconciler must not run before the events it should
// follow. Within runs them once it has released publishMu; see the note at the
// return below for why they cannot run here.
//
// The buffers are taken under the lock and the callbacks run without it: a hook
// runs code from the layer above this store and may re-enter it, so holding the
// collector's mutex across it would invite a deadlock. Taking them also latches
// the collector closed, so a re-entrant emit or AfterCommit reaching it through a
// captured tx ctx acts immediately rather than appending to a slice this flush
// has already passed.
func (s *sqliteStore) flush(coll *eventCollector) []func() {
	coll.mu.Lock()
	events, logRows, hooks := coll.events, coll.logRows, coll.hooks
	coll.events, coll.logRows, coll.hooks = nil, nil, nil
	coll.flushed = true
	coll.mu.Unlock()

	for _, p := range events {
		s.changePublish(p.gk, p.ev)
	}
	for _, p := range logRows {
		s.publishEvent(p.gk, p.ev)
	}
	// The hooks are handed back rather than run: Within holds publishMu across this
	// call to keep publication in commit order, and a hook is user code that may
	// write to the store — running it here would re-enter Within and deadlock on a
	// lock this goroutine already holds. Ordering only ever constrained the stream.
	return hooks
}

// stream is the store side of a subscription: the channel a merge goroutine owns
// and the cancel that releases it. The goroutine exits on cancel, closes the
// receiver, and closes out. V is the streamed item type — RawObjectChange for
// object watches, Event for the event log, []ObjectWrite for the write stream.
type stream[V any] struct {
	out    chan V
	cancel context.CancelFunc
}

func newStream[V any](cancel context.CancelFunc) *stream[V] {
	return &stream[V]{out: make(chan V), cancel: cancel}
}

// subscription hands the caller the read side. One accessor, one name: the three
// stream-specific interfaces this used to satisfy differed only in what they
// called it, which is why the impl once carried Changes/Events/Batches over the
// same channel.
func (w *stream[V]) subscription() *storeapi.Subscription[V] {
	return storeapi.NewSubscription(w.out, w.cancel)
}

// send delivers v, or reports false if a reader never takes it because the
// stream's context was cancelled (wctx) or the store was closed (storeDone). The
// store-close arm matters when no one is reading: closing the hub only wakes a
// receive, not a parked send.
func (w *stream[V]) send(wctx context.Context, storeDone <-chan struct{}, v V) bool {
	select {
	case w.out <- v:
		return true
	case <-wctx.Done():
		return false
	case <-storeDone:
		return false
	}
}

func (s *sqliteStore) ObjectsWatchList(ctx context.Context, gk storeapi.GroupKind) (*storeapi.ObjectsSubscription, error) {
	return s.watch(ctx, gk, nil, func(ctx context.Context) ([]*storeapi.RawObject, int64, error) {
		return s.snapshotAt(ctx, func(ctx context.Context) ([]*storeapi.RawObject, error) {
			return s.ObjectsList(ctx, gk)
		})
	})
}

// backlogUnknown is ObjectWriteBatch.OldestPending when the backlog could not be
// read at all, which a closed handle cannot distinguish from an empty one.
const backlogUnknown = -1

// backlogBound turns a Peek at the receiver's head into the bound a batch carries.
// Three answers, not two: ErrClosed is deliberately not folded into "nothing
// pending", because Hub.Close and Receiver.Close abandon whatever is still queued —
// so a closed handle says nothing about the backlog, and a consumer must hold its
// cursor rather than claim everything it has seen.
//
// Written to take Peek's own results so it can be exercised directly; the closed arm
// is otherwise reachable only in the window between a drain and the read after it.
func backlogBound(head gobus.Event[storeapi.ObjectID, writeSignal], err error) int64 {
	switch {
	case err == nil:
		return head.Value.firstRV
	case errors.Is(err, gobus.ErrEmpty):
		return 0 // nothing queued behind this batch
	default:
		return backlogUnknown // a closed handle abandoned whatever it held
	}
}

// writeBatchCap bounds how many references one batch carries. It bounds the
// slice, not retained memory: what a lagging consumer holds is its receiver's
// pending set, which conflates per object and so is bounded by the store's live
// key set either way.
//
// A backend detail, not part of the contract: a consumer learns how far the backlog
// reaches from ObjectWriteBatch.OldestPending, not by comparing a batch's length
// against this.
const writeBatchCap = 64

// ObjectWritesSubscribe streams every kind's live changes as blob-free references. It
// takes no snapshot, so the dedup floor is 0 — a fresh receiver starts at the
// current write position and everything it sees is genuinely post-subscribe —
// and the hub's own writeSignalMerge both conflates and annihilates, so no
// per-receiver merge is needed.
//
// It also returns the cursor the stream starts from, for a consumer that resumes
// from a watermark rather than re-deriving state. Returning it here rather than
// exposing a separate cursor read is what makes the ordering unrepresentable: the
// receiver is registered first, so a write landing between the two is either already
// in the receiver or above the returned value. The reverse order would leave a
// window whose writes reach neither, which is the same hazard snapshotAt's
// same-transaction read exists to close for the per-kind streams.
func (s *sqliteStore) ObjectWritesSubscribe(ctx context.Context) (*storeapi.ObjectWritesSubscription, int64, error) {
	if s.isClosed() {
		return nil, 0, errStoreClosed
	}
	rx := s.writeHub.Receiver()

	cursor, err := currentResourceVersion(ctx, s.conn(ctx))
	if err != nil {
		rx.Close()
		return nil, 0, err
	}

	wctx, cancel := context.WithCancel(ctx)
	w := newStream[storeapi.ObjectWriteBatch](cancel)
	go func() {
		// Registered first so it runs last (after out is closed), letting tests
		// await exit without reading out.
		if s.afterStream != nil {
			defer s.afterStream()
		}
		defer close(w.out)
		defer rx.Close()
		for {
			wev, err := rx.RecvContext(wctx)
			if err != nil {
				return // ctx cancelled, watcher closed, or hub closed
			}
			batch := []storeapi.ObjectWrite{{ID: wev.Key, Type: wev.Value.typ, ResourceVersion: wev.Value.rv}}
			// Drain whatever else is already pending. Taking it from the receiver
			// rather than from a buffered out channel is what keeps conflation
			// intact up to this point: until a value is popped, another write to the
			// same object merges into its slot, so a burst of writes to one object
			// costs one entry, not one per write.
			for len(batch) < writeBatchCap {
				next, err := rx.TryRecv()
				if err != nil {
					break // drained, or the hub closed (the next Recv reports it)
				}
				batch = append(batch, storeapi.ObjectWrite{ID: next.Key, Type: next.Value.typ, ResourceVersion: next.Value.rv})
			}
			// Ask what is left behind this batch, here and on this goroutine. The
			// receiver has a single consumer, and a concurrent Send either coalesces in
			// place or appends at the back — neither can move the head — so this answer
			// is exact for this batch. Reading it later, or from elsewhere, could report
			// a head further along and hand the consumer a bound above what it was
			// actually given.
			//
			oldestPending := backlogBound(rx.Peek())
			if s.beforeLiveSend != nil {
				s.beforeLiveSend() // test seam: act while the goroutine is provably about to park
			}
			if !w.send(wctx, s.done, storeapi.ObjectWriteBatch{Writes: batch, OldestPending: oldestPending}) {
				return
			}
		}
	}()
	return w.subscription(), cursor, nil
}

func (s *sqliteStore) ObjectsWatch(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID) (*storeapi.ObjectsSubscription, error) {
	filterID := id
	return s.watch(ctx, gk, &filterID, func(ctx context.Context) ([]*storeapi.RawObject, int64, error) {
		return s.snapshotAt(ctx, func(ctx context.Context) ([]*storeapi.RawObject, error) {
			raw, err := s.ObjectsGet(ctx, id)
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

// watch subscribes to gk's hub, loads a snapshot, and returns a subscription whose
// stream is the snapshot (as Added events) followed by live events not already
// covered by the snapshot. filterID, if non-nil, restricts live events to that
// object. Both callers (ObjectsWatchList and Watch) take a real snapshot; the
// snapshot-less stream is ObjectWritesSubscribe, which subscribes to the store-wide
// hub directly and shares none of this.
//
// The receiver is created BEFORE the snapshot is loaded so events that commit
// during the load are buffered, not lost; events whose resource version is at or
// below the snapshot's global high-water are then dropped as duplicates.
//
// A seenIDs set tracks which objects the consumer has been told about (via the
// snapshot or a live Added). It serves two roles:
//   - A race-window Added for object X followed by a post-snapshot Modified
//     coalesces to Added in the buffer; seenIDs detects that the consumer already
//     has X from the snapshot and promotes the type to Modified.
//   - A race-window Added for X followed by a post-snapshot Deleted coalesces to
//     Deleted (changeMerge never annihilates, to preserve real tombstones for
//     snapshot-covered objects); if X was in the snapshot seenIDs lets it through,
//     otherwise it is dropped — the object was born and died without the consumer
//     ever observing it, and emitting a lone Deleted would be spurious.
func (s *sqliteStore) watch(
	ctx context.Context,
	gk storeapi.GroupKind,
	filterID *storeapi.ObjectID,
	loadSnapshot func(context.Context) ([]*storeapi.RawObject, int64, error),
) (*storeapi.ObjectsSubscription, error) {
	h := s.hubFor(gk)
	if h == nil {
		return nil, errStoreClosed
	}
	// Register exactly the receiver we keep — an unfiltered one created first
	// would leak as a live hub subscriber that buffers every object forever.
	//   - Single-object watch: scope the subscription to that id so the receiver
	//     never buffers unrelated objects (memory bounded by the one id).
	//   - ObjectsWatchList: an annihilating merge so transient objects the consumer never
	//     saw are dropped at enqueue (memory bounded by the live key set, not by
	//     the count of distinct deleted ids a slow consumer falls behind on), while
	//     snapshot-covered deletes are preserved.
	var rx *conflate.Receiver[storeapi.ObjectID, storeapi.RawObjectChange]
	var seed atomic.Pointer[snapshotIDs] // published to the merge once the snapshot is known
	if filterID != nil {
		want := *filterID
		rx = h.Receiver(h.WithKeyFilter(func(id storeapi.ObjectID) bool { return id == want }))
	} else {
		rx = h.Receiver(h.WithMerge(transientDropMerge(snapshotPreserve(&seed))))
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
	if filterID == nil {
		ids := make(snapshotIDs, len(snapshot))
		for _, raw := range snapshot {
			ids[raw.ID] = struct{}{}
		}
		seed.Store(&ids)
	}

	wctx, cancel := context.WithCancel(ctx)
	w := newStream[storeapi.RawObjectChange](cancel)
	go func() {
		// Registered first so it runs last (after out is closed), letting tests
		// await exit without reading out.
		if s.afterStream != nil {
			defer s.afterStream()
		}
		defer close(w.out)
		defer rx.Close()
		send := func(ev storeapi.RawObjectChange) bool { return w.send(wctx, s.done, ev) }
		// seenIDs tracks every object ID the consumer has been told about, so
		// the live stream can correct event types and drop orphan tombstones.
		seenIDs := make(map[storeapi.ObjectID]struct{}, len(snapshot))
		// Emit the snapshot as Added events before streaming live events, then
		// release it: the goroutine outlives the snapshot by the whole streaming
		// lifetime, and holding the slice would pin every object's spec/status
		// blobs until the watcher closes.
		for _, raw := range snapshot {
			seenIDs[raw.ID] = struct{}{}
			if !send(storeapi.RawObjectChange{Type: storeapi.Added, Object: raw}) {
				return
			}
		}
		snapshot = nil
		// The conflating hub never drops events — it coalesces per object — so a
		// lagging watcher converges to each object's latest state (including a
		// delete, which carries the real final row) rather than observing a gap.
		// No relist or tombstone synthesis is needed.
		for {
			wev, err := rx.RecvContext(wctx)
			if err != nil {
				return // ctx cancelled, watcher closed, or hub closed
			}
			ev := wev.Value
			if ev.Object.ResourceVersion <= snapshotHighWaterRV {
				continue // already represented by the snapshot
			}
			// No id filter here: a single-object watch uses an id-scoped receiver
			// (see filterID above), so unrelated ids never reach this loop.
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
			if s.beforeLiveSend != nil {
				s.beforeLiveSend() // test seam: act while the goroutine is provably past the receive
			}
			if !send(ev) {
				return
			}
		}
	}()
	return w.subscription(), nil
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
	// Compare at stored (millisecond) precision, matching EventsList' toMillis(Since)
	// bound: a sub-millisecond Since (e.g. time.Now()) must not drop a live run in
	// that same millisecond that the snapshot query would keep.
	if !q.Since.IsZero() && toMillis(ev.LastAt) < toMillis(q.Since) {
		return false
	}
	return true
}

// EventsWatch streams id's event log within gk: the runs matching q as a
// snapshot, then live runs. The receiver is created before the snapshot loads so
// runs committed during the load are buffered, not lost; a run already reflected
// in the snapshot (resource_version at or below its high-water) is then dropped.
func (s *sqliteStore) EventsWatch(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, q storeapi.EventQuery) (*storeapi.EventsSubscription, error) {
	h := s.eventHubFor(gk)
	if h == nil {
		return nil, errStoreClosed
	}
	// Key-filter to this object: the run id in the key makes the filter exact
	// without inspecting values, so the receiver never buffers other objects' runs.
	rx := h.Receiver(h.WithKeyFilter(func(k eventKey) bool { return k.ObjectID == id }))
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
		// so an unscoped EventsList(id) would leak a foreign object's log and
		// disagree with the live half. A missing or wrong-kind id yields an empty
		// snapshot — the live stream delivers nothing for it either.
		var err error
		if objectExists, err = s.objectInKind(ctx, gk, id); err != nil {
			return err
		}
		if objectExists {
			if snapshot, err = s.EventsList(ctx, id, q); err != nil {
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
	w := newStream[storeapi.Event](cancel)
	go func() {
		if s.afterStream != nil {
			defer s.afterStream()
		}
		defer close(w.out)
		defer rx.Close()
		send := func(ev storeapi.Event) bool { return w.send(wctx, s.done, ev) }
		// EventsList is newest-first; deliver the snapshot oldest-first so the
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
			wev, err := rx.RecvContext(wctx)
			if err != nil {
				return // ctx cancelled, watcher closed, or hub closed
			}
			ev := wev.Value
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
			if s.beforeLiveSend != nil {
				s.beforeLiveSend() // test seam: act while the goroutine is provably past the receive
			}
			if !send(ev) {
				return
			}
		}
	}()
	return w.subscription(), nil
}
