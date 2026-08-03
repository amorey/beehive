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

package beehive

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/amorey/beehive/internal/driver"
	"github.com/amorey/gobus/conflate"
	"github.com/amorey/gobus/watch"
)

// This file is the object watch, both halves of it. One tailer per kind owns
// the kind's cursor, reads the write log when a commit wakes it, and fans raw
// changes out over a conflate hub; each watch is a subscriber that decodes for
// itself and drops what its own snapshot already held. The two halves live
// together because the invariants span them — the merge table is written
// against a hub-wide pending slot meeting a per-subscriber floor.
// See docs/adr/2026-08-03-watch-shared-tail.md.

// WatchOption configures one watch call. A distinct type from Option: these are
// meaningful only here, and dispatching them on a Beehive or a controller would
// silently accept nonsense.
type WatchOption func(*watchConfig)

type watchConfig struct {
	// resumeFrom is the position to stream above, or nil to take a snapshot.
	resumeFrom *int64
	loads      LoadSet
}

// WithResumeFrom streams the changes above rv instead of taking a snapshot. The
// returned snapshot holds no objects and carries rv back. A position retention
// has already passed ends the stream with a Failed change carrying
// ErrWatchTooOld — the same way a live stream reports it — which the caller
// answers by subscribing again without this option.
func WithResumeFrom(rv int64) WatchOption {
	return func(c *watchConfig) { c.resumeFrom = &rv }
}

// WithLoads eager-loads the same secondary lookups List takes, on the snapshot
// and on every delivered batch. Batched per batch, not per object, so a watch
// does not become an N+1.
func WithLoads(loads ...LoadOption) WatchOption {
	return func(c *watchConfig) { c.loads = resolveLoads(loads) }
}

func resolveWatch(opts []WatchOption) watchConfig {
	var cfg watchConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// watchFloor returns the object tail's floor interval, with a fallback for the
// same reason as watchPoll: a zero would make the tailer's timer fire in a
// loop.
func (bh *Beehive) watchFloor() time.Duration {
	if bh.watchFloorInterval <= 0 {
		return defaultWatchFloorInterval
	}
	return bh.watchFloorInterval
}

// WatchList streams changes to every object of this client's kind. See
// the Client interface for the contract.
func (c *clientImpl[Spec, Status]) WatchList(ctx context.Context, opts ...WatchOption) (ObjectListSnapshot[Spec, Status], <-chan ObjectChange[Spec, Status], error) {
	return c.tailStream(ctx, resolveWatch(opts), nil)
}

// Watch streams changes to the single object id: an id that does not exist yet
// streams nothing until created, and its removal reads as a Deleted.
func (c *clientImpl[Spec, Status]) Watch(ctx context.Context, id ObjectID, opts ...WatchOption) (ObjectSnapshot[Spec, Status], <-chan ObjectChange[Spec, Status], error) {
	// The tail is shared per kind — the log has no index on object_id — so a
	// single-object watch joins the kind's reader and filters the fan-out down
	// to its own id.
	list, ch, err := c.tailStream(ctx, resolveWatch(opts), &id)
	if err != nil {
		return ObjectSnapshot[Spec, Status]{}, nil, err
	}
	snap := ObjectSnapshot[Spec, Status]{ResourceVersion: list.ResourceVersion}
	if len(list.Objects) > 0 {
		snap.Object = list.Objects[0]
	}
	return snap, ch, nil
}

// snapshot reads the watch's starting state: one object, or the whole kind.
func (c *clientImpl[Spec, Status]) snapshot(ctx context.Context, only *ObjectID) ([]*RawObject, int64, error) {
	if only != nil {
		return c.bh.store.ObjectWritesSnapshotByID(ctx, c.gk, *only)
	}
	return c.bh.store.ObjectWritesSnapshot(ctx, c.gk)
}

// kindWriteHub tells a kind's tailer that the kind moved. Keyed by GroupKind,
// one slot per receiver: a burst of commits collapses into one pending signal,
// and a publish that lands mid-read waits in the slot. The signal carries no
// value — the tailer reads its position from the store — so no Accept gate is
// set and every send is taken. Close closes the sender, never the hub
// (scheduleHub's rule).
// See docs/adr/2026-08-03-watch-shared-tail.md.
type kindWriteHub struct {
	hub *watch.Hub[GroupKind, struct{}]
}

func newKindWriteHub() kindWriteHub {
	return kindWriteHub{hub: watch.New[GroupKind, struct{}]()}
}

// Send is a no-op on the zero hub, which a Beehive built without New has —
// the same rule bh.log() applies to an unresolved logger.
func (h kindWriteHub) Send(gk GroupKind) error {
	if h.hub == nil {
		return nil
	}
	return h.hub.Sender().Send(gk, struct{}{})
}

// Watch registers a receiver for gk. Registration is the baseline and the bus
// never delivers it back, so a receiver reads only writes that follow it.
//
// ok is false on the zero hub. Unlike Send and Close this cannot no-op: a
// receiver has to be tied to a hub, so there is nothing to hand back. The
// caller turns that into an error rather than a nil dereference.
func (h kindWriteHub) Watch(gk GroupKind) (*watch.Receiver[GroupKind, struct{}], bool) {
	if h.hub == nil {
		return nil, false
	}
	return h.hub.Watch(gk, struct{}{}), true
}

// Close is a no-op on the zero hub; see Send.
func (h kindWriteHub) Close() {
	if h.hub != nil {
		h.hub.Sender().Close()
	}
}

// tailerFor returns the kind's tailer with a subscriber lease held on it,
// starting one on the kind's first watch. Every caller owes exactly one
// release.
//
// Guarded by tailMu, never bh.mu: bh.mu is not reentrant, and migratorFor and
// reconcilerFor take it. The lock is held across the build, cursor read
// included. That read happens once per kind per process, and building outside
// the lock buys that one latency at the price of a second registry check and a
// discard path for the race it opens. It cannot move into run either: the
// cursor has to be read before this returns, or a subscriber's snapshot could
// fall into the gap below it.
//
// The health check is not redundant with the registry. A tailer that ended
// below the horizon stays registered until the last subscriber holding a lease
// on it releases, and this is what stops a resubscribe from rejoining the
// tailer that just told it ErrWatchTooOld.
func (bh *Beehive) tailerFor(ctx context.Context, gk GroupKind) (*objectTailer, error) {
	bh.tailMu.Lock()
	defer bh.tailMu.Unlock()
	if t, ok := bh.tailers[gk]; ok && t.failure() == nil {
		t.refs++
		return t, nil
	}

	t, err := newObjectTailer(ctx, bh, gk)
	if err != nil {
		return nil, err
	}
	if bh.tailers == nil {
		bh.tailers = make(map[GroupKind]*objectTailer)
	}
	t.refs = 1
	// Overwrites a tailer that ended below the horizon; release compares
	// identity, so its subscribers cannot evict this one on their way out.
	bh.tailers[gk] = t
	// After stop the wake hub is closed, so the tailer closes its fan-out at
	// once, which ends the watch that asked for it.
	go t.run()
	return t, nil
}

// release drops one subscriber's lease, ending the tailer when the last one
// goes: a kind watched once must not cost a goroutine and a floor tick for the
// rest of the process's life.
//
// The count moves only under tailMu, which is what closes the teardown race:
// a watch arriving during a teardown either takes the lock first and joins a
// tailer that is still registered, or takes it after and finds the map entry
// gone, and starts a fresh one.
func (t *objectTailer) release() {
	t.bh.tailMu.Lock()
	defer t.bh.tailMu.Unlock()
	t.refs--
	if t.refs > 0 {
		return
	}
	// Identity, not presence: a failed tailer was already replaced in the map,
	// and its last subscriber must not evict the successor.
	if t.bh.tailers[t.gk] == t {
		delete(t.bh.tailers, t.gk)
	}
	t.cancel()
}

// horizonErr returns ErrWatchTooOld when retention has trimmed the log past
// cursor, else nil. Strictly <: a cursor exactly on the horizon has lost
// nothing, and a kind that stops writing ends up exactly there.
func horizonErr(gk GroupKind, what string, cursor, trimmedThrough int64) error {
	if cursor >= trimmedThrough {
		return nil
	}
	return fmt.Errorf("%w: %s/%s trimmed through %d, %s was at %d",
		ErrWatchTooOld, gk.Group, gk.Kind, trimmedThrough, what, cursor)
}

// errNoRowImage marks a delete log entry with no row image to build the
// Deleted change from.
var errNoRowImage = errors.New("beehive: delete log entry carries no row image")

// tailPageCap caps one read of the log, so a burst stays bounded. Anything
// beyond a full page is read by the next step, not deferred.
const tailPageCap = 512

// changeType maps a log op to what a subscriber is told. A soft delete is a
// WriteUpdate, so a finalizing object reports Modified; Deleted means the row
// is gone. Coalescing promotes Modified to Added when the run began with a
// create, and a run ending in a delete stays Deleted — the row is gone and the
// entry carries the state to report.
func changeType(op WriteOp) ChangeType {
	switch op {
	case WriteCreate:
		return Added
	case WriteDelete:
		return Deleted
	default:
		return Modified
	}
}

// rawChange is what the tailer fans out: one object's newest log entry plus
// the state to report with it. Undecoded, because two clients can watch one
// kind with different type parameters — each subscriber decodes for itself.
type rawChange struct {
	ID              ObjectID
	Op              WriteOp
	ResourceVersion int64
	// Object is the row as read back, or the log entry's row image for a delete.
	Object *RawObject
}

// objectTailer is one kind's shared log reader: it owns the kind's cursor,
// runs the position check, log page and batched read once, and fans the result
// out to every watch on the kind. One goroutine per kind, not per watch.
type objectTailer struct {
	bh         *Beehive
	gk         GroupKind
	hub        *conflate.Hub[ObjectID, rawChange]
	kindWrites *watch.Receiver[GroupKind, struct{}]
	// ctx is run's, cancelled when the last subscriber leaves. Held here
	// because the goroutine outlives the call that started it.
	ctx    context.Context
	cancel context.CancelFunc
	// refs counts live subscribers. Guarded by bh.tailMu, which is also what
	// makes "in bh.tailers" and "refs > 0" the same condition.
	refs int
	// cursor is only touched by run.
	cursor int64
	// failed records why the fan-out closed, for subscribers to report. Written
	// before the sender closes, read after.
	failed atomic.Pointer[error]
}

// newObjectTailer registers the wake receiver BEFORE reading the starting
// cursor: a write landing between the two would otherwise be missed by both.
// The cursor is read before returning, so a subscriber that snapshots after
// this call cannot fall into the gap either.
func newObjectTailer(ctx context.Context, bh *Beehive, gk GroupKind) (*objectTailer, error) {
	written, ok := bh.kindWriteHub.Watch(gk)
	if !ok {
		return nil, errors.New("beehive: a watch needs a Beehive built by New")
	}
	t := &objectTailer{
		bh:         bh,
		gk:         gk,
		hub:        conflate.New[ObjectID](mergeRawChange),
		kindWrites: written,
	}
	t.ctx, t.cancel = context.WithCancel(context.Background())
	at, err := bh.store.ObjectWritesMaxVersion(ctx, gk)
	if err != nil {
		t.close()
		return nil, err
	}
	t.cursor = at
	return t, nil
}

// close ends the tailer and releases its hub and wake receiver. Idempotent, and
// safe on one that never ran.
func (t *objectTailer) close() {
	t.cancel()
	t.kindWrites.Close()
	t.hub.Sender().Close()
}

// mergeRawChange collapses two undelivered changes for one object into one:
// newest wins, and a run that began with an unseen create still reports as a
// create. It never drops a pair — the pending slot is shared by all
// subscribers but snapshots are per subscriber, so dropping an unread
// create/delete pair would leave a subscriber that snapshotted between the two
// holding the object forever.
func mergeRawChange(prev, next rawChange) (rawChange, bool) {
	if next.ResourceVersion <= prev.ResourceVersion {
		return prev, true // stale send: the pending value is already newer
	}
	if prev.Op == WriteCreate && next.Op != WriteDelete {
		next.Op = WriteCreate
	}
	return next, true
}

// failure returns why the tailer ended, or nil when it ended with the beehive.
func (t *objectTailer) failure() error {
	if err := t.failed.Load(); err != nil {
		return *err
	}
	return nil
}

// run tails the kind's log until the last subscriber leaves. A commit wakes it;
// the floor timer covers what a wake cannot. Entries at or below the starting
// cursor belong to the subscribers' snapshots, not to the tail.
func (t *objectTailer) run() {
	defer t.close()

	ctx := t.ctx
	written := t.kindWrites.Chan()
	floor := t.bh.watchFloor()
	timer := time.NewTimer(floor)
	defer timer.Stop()

	retry := driver.Backoff{Base: watchRetryBase, Max: floor}
	// backingOff drops commit wakes until the retry timer fires. A wake carries
	// no information — the drain reads its position from the store — so dropping
	// one loses nothing, and the timer reads the log either way. Honouring one
	// would void the backoff exactly when it is needed: a commit landing during
	// a failed drain refills the wake slot, so a live writer would keep a
	// degraded store re-reading as fast as it can fail.
	backingOff := false
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-written:
			if !ok {
				return // the beehive stopped
			}
			// Consumed rather than ignored, so the closed arm above stays live.
			if backingOff {
				continue
			}
		case <-timer.C:
		}

		next := floor
		if err := t.drain(ctx); err != nil {
			// The cursor is shared, so a trimmed cursor ends every subscriber:
			// skipping past the horizon would silently drop changes for all of
			// them, and there is no single subscriber to hand the error to.
			// The next watch starts a fresh tailer.
			if errors.Is(err, ErrWatchTooOld) {
				t.bh.log().Warn("watch tail fell below the retention horizon; ending its subscribers",
					"kind", t.gk.Kind, "err", err)
				t.failed.Store(&err)
				return
			}
			t.bh.log().Warn("watch tail step failed; retrying", "kind", t.gk.Kind, "err", err)
			next = retry.Next()
			backingOff = true
		} else {
			retry.Reset()
			backingOff = false
		}
		timer.Stop()
		timer.Reset(next)
	}
}

// drain reads pages until one comes back short, so a burst that collapsed into
// one wake is read in full. The page length is the stop test: it says exactly
// when the log is drained, and a second position read would cost an extra
// query for the same answer.
func (t *objectTailer) drain(ctx context.Context) error {
	for {
		n, err := t.step(ctx)
		if err != nil {
			return err
		}
		if n < tailPageCap {
			return nil
		}
	}
}

// step reads one page of the kind's log above the cursor, publishes what it
// found, and returns the page length. The position check up front makes a
// quiet wake cost one scalar read.
func (t *objectTailer) step(ctx context.Context) (int, error) {
	at, err := t.bh.store.ObjectWritesMaxVersion(ctx, t.gk)
	if err != nil {
		return 0, err
	}
	// The position includes the retention horizon, so it only rises: test with
	// >, and an unmoved position means nothing was written.
	if at <= t.cursor {
		return 0, nil
	}
	page, trimmedThrough, err := t.bh.store.ObjectWritesListSince(ctx, t.gk, t.cursor, tailPageCap)
	if err != nil {
		return 0, err
	}
	if err := horizonErr(t.gk, "the tail", t.cursor, trimmedThrough); err != nil {
		return 0, err
	}
	if len(page) == 0 {
		return 0, nil
	}

	changes, err := collectChanges(ctx, t.bh, t.gk, page)
	if err != nil {
		return 0, err
	}
	for _, ch := range changes {
		if err := t.hub.Sender().Send(ch.ID, ch); err != nil {
			return 0, nil // sender closed: the beehive is stopping
		}
	}
	t.cursor = page[len(page)-1].ResourceVersion
	return len(page), nil
}

// collectChanges coalesces a log page to the last entry per object and reads the
// current state of everything still live, in one batch. Shared by the tailer and
// by a resume's replay.
func collectChanges(ctx context.Context, bh *Beehive, gk GroupKind, page []ObjectWrite) ([]rawChange, error) {
	// The page arrives ascending and resource_version is the log's primary
	// key, so keeping each id's highest-version entry preserves write order
	// without a sort.
	last := make(map[ObjectID]int64, len(page))
	// created marks runs that began with a create. Create-then-update
	// coalesces to a WriteUpdate, but the object was not in the subscriber's
	// snapshot, so reporting Modified would give a cache a change for an id it
	// does not hold.
	created := make(map[ObjectID]bool, len(page))
	for _, w := range page {
		last[w.ID] = w.ResourceVersion
		if w.Op == WriteCreate {
			created[w.ID] = true
		}
	}
	order := make([]ObjectWrite, 0, len(last))
	for _, w := range page {
		if last[w.ID] == w.ResourceVersion {
			order = append(order, w)
		}
	}

	// One batched read for everything still live. Per-object reads would run
	// one after another on the single connection.
	live := make([]ObjectID, 0, len(order))
	for _, w := range order {
		if w.Op != WriteDelete {
			live = append(live, w.ID)
		}
	}
	rows, err := bh.store.ObjectsListByIDs(ctx, gk, live)
	if err != nil {
		return nil, err
	}
	byID := make(map[ObjectID]*RawObject, len(rows))
	for _, raw := range rows {
		byID[raw.ID] = raw
	}

	changes := make([]rawChange, 0, len(order))
	for _, w := range order {
		raw := w.Final
		if w.Op != WriteDelete {
			// A missing row was collected between the two reads. Skip it: the
			// delete wrote its own entry above this page, so it arrives later.
			if raw = byID[w.ID]; raw == nil {
				continue
			}
		}
		if raw == nil {
			// A delete entry with no row image. The store contract forbids this,
			// but Store is a public extension point.
			bh.log().Warn("beehive: skipping undecodable object",
				"op", "Watch", "group", gk.Group, "kind", gk.Kind, "id", w.ID, "err", errNoRowImage)
			continue
		}
		op := w.Op
		if op == WriteUpdate && created[w.ID] {
			op = WriteCreate
		}
		changes = append(changes, rawChange{
			ID:              w.ID,
			Op:              op,
			ResourceVersion: w.ResourceVersion,
			Object:          raw,
		})
	}
	return changes, nil
}

// tailStream is a watch on the shared tail: it registers a fan-out receiver,
// takes its own snapshot (or replays a resume's gap), and delivers what the
// tailer publishes above that position.
//
// Registration comes BEFORE the snapshot — conflate has no replay, so a change
// published in between needs a receiver already in place. The overlap is safe
// to drop: resource_version is one log-wide sequence and the snapshot is a
// consistent cut at it, so anything at or below the snapshot's position is
// already in the snapshot.
func (c *clientImpl[Spec, Status]) tailStream(
	ctx context.Context,
	cfg watchConfig,
	only *ObjectID,
) (ObjectListSnapshot[Spec, Status], <-chan ObjectChange[Spec, Status], error) {
	var empty ObjectListSnapshot[Spec, Status]
	tailer, err := c.bh.tailerFor(ctx, c.gk)
	if err != nil {
		return empty, nil, fmt.Errorf("beehive: watch on %s/%s: %w", c.gk.Group, c.gk.Kind, err)
	}

	var opts []conflate.ReceiverOption[ObjectID, rawChange]
	if only != nil {
		// Bounds this subscriber's memory to one key.
		opts = append(opts, tailer.hub.WithKeyFilter(func(k ObjectID) bool { return k == *only }))
	}
	rx := tailer.hub.Receiver(opts...)
	// Owed by every path that returns without a stream: the receiver holds a key
	// in the fan-out, and the lease holds the tailer open.
	abandon := func() { rx.Close(); tailer.release() }

	mig := c.bh.migratorFor(c.gk) // invariant for the stream's lifetime

	var snap ObjectListSnapshot[Spec, Status]
	// floor is what the caller already holds; deliveries at or below it are
	// dropped. A resume raises it as the replay advances.
	var floor int64
	if cfg.resumeFrom != nil {
		// A resume reads nothing here: the caller supplies the position, and the
		// replay checks the horizon on every page it reads anyway. Probing it
		// once more first would cost a round trip to answer the same question a
		// moment earlier, in a second place the caller has to handle.
		snap.ResourceVersion, floor = *cfg.resumeFrom, *cfg.resumeFrom
	} else {
		raws, at, err := c.snapshot(ctx, only)
		if err != nil {
			abandon()
			return empty, nil, fmt.Errorf("beehive: watch on %s/%s: initial read failed: %w",
				c.gk.Group, c.gk.Kind, err)
		}
		snap.ResourceVersion, floor = at, at
		snap.Objects = c.decodeList(raws, "Watch")
		if err := c.loadListRelated(ctx, snap.Objects, cfg.loads); err != nil {
			abandon()
			return empty, nil, fmt.Errorf("beehive: watch on %s/%s: initial read failed: %w",
				c.gk.Group, c.gk.Kind, err)
		}
	}

	out := make(chan ObjectChange[Spec, Status])
	go func() {
		// LIFO: the receiver leaves the fan-out, then the lease goes, then the
		// caller sees the stream end.
		defer close(out)
		defer tailer.release()
		defer rx.Close()
		if cfg.resumeFrom != nil {
			at, ok := c.replay(ctx, mig, cfg, only, floor, out)
			if !ok {
				return
			}
			floor = at
		}
		c.consume(ctx, tailer, rx, mig, cfg, floor, out)
	}()
	return snap, out, nil
}

// consume delivers what the tailer publishes above floor until ctx ends or the
// fan-out closes.
func (c *clientImpl[Spec, Status]) consume(
	ctx context.Context,
	tailer *objectTailer,
	rx *conflate.Receiver[ObjectID, rawChange],
	mig Migrator,
	cfg watchConfig,
	floor int64,
	out chan<- ObjectChange[Spec, Status],
) {
	for {
		ev, err := rx.RecvContext(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// The fan-out closed. A tailer that fell below the retention
			// horizon reports the failure; one that ended with the beehive
			// just closes the stream.
			if failure := tailer.failure(); failure != nil {
				sendOrDone(ctx, out, ObjectChange[Spec, Status]{Type: Failed, Err: failure})
			}
			return
		}
		changes, ok := c.decodeBatch(ctx, drainPending(ev.Value, rx), mig, cfg, floor)
		if !ok {
			return // ctx ended while retrying a failed relation load
		}
		for _, ch := range changes {
			if !sendOrDone(ctx, out, ch) {
				return
			}
		}
	}
}

// drainPending takes first plus everything already queued behind it, so a burst
// costs one relation query rather than one per object. TryRecvAll is one atomic
// cut, so the batch is everything pending as of one instant — a TryRecv loop
// would be a sequence of instants with no defined membership. Its error is
// ErrEmpty or ErrClosed, both meaning nothing more to add; consume hears about
// a closed fan-out on its next receive.
//
// Ascending by resource version, and that is a correctness requirement rather
// than tidiness: a caller checkpoints the version on a delivered change and
// resumes above it, so a version delivered after a higher one would be skipped
// for good. The cut comes back in queue order, and the fan-out coalesces in
// place, so a re-written object sits at its original position carrying a newer
// version — sorting is what stops that reaching the caller.
func drainPending(first rawChange, rx *conflate.Receiver[ObjectID, rawChange]) []rawChange {
	rest, _ := rx.TryRecvAll()
	batch := make([]rawChange, 0, len(rest)+1)
	// first left the queue before the cut, so it can repeat a key the cut holds.
	// Harmless: both are delivered, in version order, and the later one wins.
	batch = append(batch, first)
	for _, ev := range rest {
		batch = append(batch, ev.Value)
	}
	slices.SortFunc(batch, func(a, b rawChange) int {
		return cmp.Compare(a.ResourceVersion, b.ResourceVersion)
	})
	return batch
}

// decodeBatch is decodeChanges with the relation load retried instead of
// skipped: the tailer's cursor has already moved past these entries, so no
// later read brings them back. It reports false only when ctx ended.
func (c *clientImpl[Spec, Status]) decodeBatch(
	ctx context.Context,
	batch []rawChange,
	mig Migrator,
	cfg watchConfig,
	floor int64,
) ([]ObjectChange[Spec, Status], bool) {
	retry := driver.Backoff{Base: watchRetryBase, Max: c.bh.watchFloor()}
	for {
		changes, err := c.decodeChanges(ctx, batch, mig, cfg, floor)
		if err == nil {
			return changes, true
		}
		if !c.pollFailed(ctx, "watch relation load", err) || !retry.Wait(ctx) {
			return nil, false
		}
	}
}

// decodeChanges turns raw changes into typed ones, dropping entries the
// caller's snapshot already covered and skipping rows that do not decode.
func (c *clientImpl[Spec, Status]) decodeChanges(
	ctx context.Context,
	batch []rawChange,
	mig Migrator,
	cfg watchConfig,
	floor int64,
) ([]ObjectChange[Spec, Status], error) {
	changes := make([]ObjectChange[Spec, Status], 0, len(batch))
	// Deleted objects have no relations to load: the edges went with the row.
	var loaded []*Object[Spec, Status]
	for _, raw := range batch {
		if raw.ResourceVersion <= floor {
			continue
		}
		obj, err := rawToTyped[Spec, Status](raw.Object, mig)
		if err != nil {
			// Quarantine: one bad row must not kill a live watcher.
			c.warnUndecodable("Watch", raw.ID, err)
			continue
		}
		changes = append(changes, ObjectChange[Spec, Status]{
			Type:            changeType(raw.Op),
			ResourceVersion: raw.ResourceVersion,
			Object:          obj,
		})
		if raw.Op != WriteDelete && cfg.loads != 0 {
			loaded = append(loaded, obj)
		}
	}
	if err := c.loadListRelated(ctx, loaded, cfg.loads); err != nil {
		return nil, err
	}
	return changes, nil
}

// replay delivers the gap between a resume's position and the tail, in pages —
// with a day of retention the gap can be far more than one page. It returns
// the position reached, or false when the stream is over.
func (c *clientImpl[Spec, Status]) replay(
	ctx context.Context,
	mig Migrator,
	cfg watchConfig,
	only *ObjectID,
	from int64,
	out chan<- ObjectChange[Spec, Status],
) (int64, bool) {
	cursor := from
	retry := driver.Backoff{Base: watchRetryBase, Max: c.bh.watchFloor()}
	for {
		page, trimmedThrough, err := c.bh.store.ObjectWritesListSince(ctx, c.gk, cursor, tailPageCap)
		if err != nil {
			// A failed read costs a retry, not the stream: the cursor has not
			// moved, so nothing is lost.
			if !c.pollFailed(ctx, "watch resume", err) || !retry.Wait(ctx) {
				return 0, false
			}
			continue
		}
		// Retention can overtake a replay that is still paging.
		if err := horizonErr(c.gk, "the resume", cursor, trimmedThrough); err != nil {
			sendOrDone(ctx, out, ObjectChange[Spec, Status]{Type: Failed, Err: err})
			return 0, false
		}
		if len(page) == 0 {
			return cursor, true
		}
		next := page[len(page)-1].ResourceVersion
		full := len(page) == tailPageCap
		if only != nil {
			// Filter before the read: collectChanges would otherwise read back
			// every object in the page to deliver at most one.
			page = slices.DeleteFunc(page, func(w ObjectWrite) bool { return w.ID != *only })
		}
		raws, err := collectChanges(ctx, c.bh, c.gk, page)
		if err != nil {
			if !c.pollFailed(ctx, "watch resume", err) || !retry.Wait(ctx) {
				return 0, false
			}
			continue
		}
		retry.Reset()
		changes, ok := c.decodeBatch(ctx, raws, mig, cfg, cursor)
		if !ok {
			return 0, false
		}
		for _, ch := range changes {
			if !sendOrDone(ctx, out, ch) {
				return 0, false
			}
		}
		cursor = next
		if !full {
			return cursor, true
		}
	}
}
