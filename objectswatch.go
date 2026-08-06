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
	"github.com/amorey/beehive/internal/rategate"
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
	// scope is set by the watch call, not by an option: a caller cannot ask for
	// a scope the entry point did not choose.
	scope watchScope
}

// WithResumeFrom streams the changes above rv instead of taking a snapshot. The
// returned snapshot holds no objects and carries rv back. A position retention
// has already passed ends the stream with a Failed change carrying
// ErrWatchTooOld — the same way a live stream reports it — which the caller
// answers by subscribing again without this option. A position above the log's
// head arrives the same way with ErrWatchTooNew: it did not come from this
// store, so no retention window would have kept it.
func WithResumeFrom(rv int64) WatchOption {
	return func(c *watchConfig) { c.resumeFrom = &rv }
}

// WithLoads eager-loads the same secondary lookups List takes, on the snapshot
// and on every delivered batch. Batched per batch, not per object, so a watch
// does not become an N+1.
func WithLoads(loads ...LoadOption) WatchOption {
	return func(c *watchConfig) { c.loads = resolveLoads(loads) }
}

// remainingLoads is what a delivered batch still has to read. A scoped watch
// resolves the owner in the tailer, once for every subscriber on the kind.
func (c watchConfig) remainingLoads() LoadSet {
	if c.scope.ownedBy != nil {
		return c.loads &^ LoadOwnerBit
	}
	return c.loads
}

func resolveWatch(opts []WatchOption) watchConfig {
	var cfg watchConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// watchFloor returns the interval a watch reads at without a wake. The fallback
// covers a Beehive built field by field in a test: a zero would make the timer
// fire in a loop.
func (bh *Beehive) watchFloor() time.Duration {
	if bh.watchFloorInterval <= 0 {
		return defaultWatchFloorInterval
	}
	return bh.watchFloorInterval
}

// watchBackoff is the retry ladder every watch path climbs after a failed read:
// the tailer's, and a subscriber's own relation load and resume replay. Capped
// at the floor interval, which is what a healthy quiet kind already costs, so a
// failing read settles at a cadence the store is sized for.
//
// One statement, three callers: a tailer and its own subscribers backing off on
// different curves would be a tuning accident, not a decision.
func (bh *Beehive) watchBackoff() driver.Backoff {
	return driver.Backoff{Base: watchRetryBase, Max: bh.watchFloor()}
}

// WatchList streams changes to every object of this client's kind. See
// the Client interface for the contract.
func (c *clientImpl[Spec, Status]) WatchList(ctx context.Context, opts ...WatchOption) (ObjectListSnapshot[Spec, Status], <-chan ObjectChange[Spec, Status], error) {
	return c.tailStream(ctx, resolveWatch(opts))
}

// Watch streams changes to the single object id: an id that does not exist yet
// streams nothing until created, and its removal reads as a Deleted.
func (c *clientImpl[Spec, Status]) Watch(ctx context.Context, id ObjectID, opts ...WatchOption) (ObjectSnapshot[Spec, Status], <-chan ObjectChange[Spec, Status], error) {
	// The tail is shared per kind — the log has no index on object_id — so a
	// single-object watch joins the kind's reader and filters the fan-out down
	// to its own id.
	cfg := resolveWatch(opts)
	cfg.scope.only = &id
	list, ch, err := c.tailStream(ctx, cfg)
	if err != nil {
		return ObjectSnapshot[Spec, Status]{}, nil, err
	}
	snap := ObjectSnapshot[Spec, Status]{ResourceVersion: list.ResourceVersion}
	if len(list.Objects) > 0 {
		snap.Object = list.Objects[0]
	}
	return snap, ch, nil
}

// OwnedObjectsWatchList streams the objects of this client's kind owned by
// ownerID. See the Client interface for the contract.
func (c *clientImpl[Spec, Status]) OwnedObjectsWatchList(ctx context.Context, ownerID ObjectID, opts ...WatchOption) (ObjectListSnapshot[Spec, Status], <-chan ObjectChange[Spec, Status], error) {
	// Ownership is not in the log, so unlike Watch this cannot filter the fan-out
	// by key: the tailer resolves each change's owner and the subscriber matches
	// on it. See docs/adr/2026-08-06-owner-scoped-watches.md.
	cfg := resolveWatch(opts)
	cfg.scope.ownedBy = &ownerID
	return c.tailStream(ctx, cfg)
}

// watchScope narrows a watch to part of its kind. At most one field is set; the
// zero value is the whole kind.
type watchScope struct {
	only    *ObjectID // one object
	ownedBy *ObjectID // one owner's children
}

// snapshot reads the watch's starting state, at the position its stream begins
// above.
func (c *clientImpl[Spec, Status]) snapshot(ctx context.Context, scope watchScope) ([]*RawObject, int64, error) {
	switch {
	case scope.only != nil:
		return c.bh.store.ObjectWritesSnapshotByID(ctx, c.gk, *scope.only)
	case scope.ownedBy != nil:
		return c.bh.store.ObjectWritesSnapshotByOwner(ctx, c.gk, *scope.ownedBy)
	}
	return c.bh.store.ObjectWritesSnapshot(ctx, c.gk)
}

// kindWriteHub tells a kind's tailer that the kind moved.
// See docs/adr/2026-08-03-watch-shared-tail.md.
type kindWriteHub struct {
	signalHub[GroupKind]
}

func newKindWriteHub() kindWriteHub { return kindWriteHub{newSignalHub[GroupKind]()} }

// WatchAcross registers a receiver for every kind, for the dependency waker:
// its cursor is store-wide, and an edge can point at a kind no per-kind watch
// would name.
func (h kindWriteHub) WatchAcross() (*watch.Receiver[GroupKind, struct{}], bool) {
	return h.watchAcross(struct{}{})
}

// tailerFor returns the kind's tailer with a subscriber lease held on it,
// starting one on the kind's first watch. Every caller owes exactly one
// release.
//
// Guarded by tailMu, never bh.mu: bh.mu is not reentrant, and migratorFor and
// reconcilerFor take it. The build runs outside tailMu, and the cursor read
// must still happen before this returns. The health check is not redundant with
// the registry: a failed tailer stays registered until its subscribers release.
// See docs/adr/2026-08-03-watch-shared-tail.md.
func (bh *Beehive) tailerFor(ctx context.Context, gk GroupKind) (*objectTailer, error) {
	if t, ok := bh.joinTailer(gk); ok {
		return t, nil
	}

	t, err := newObjectTailer(ctx, bh, gk)
	if err != nil {
		return nil, err
	}

	bh.tailMu.Lock()
	// Another goroutine may have registered one for this kind while this build
	// was reading. Joining it and discarding ours keeps one tailer per kind.
	if winner, ok := joinLocked(bh.tailers, gk); ok {
		bh.tailMu.Unlock()
		t.close() // never registered and never ran: no lease to give back
		return winner, nil
	}
	if bh.tailers == nil {
		bh.tailers = make(map[GroupKind]*objectTailer)
	}
	t.refs = 1
	// Overwrites a tailer that ended below the horizon; release compares
	// identity, so its subscribers cannot evict this one on their way out.
	bh.tailers[gk] = t
	bh.tailMu.Unlock()

	// After stop the wake hub is closed, so the tailer closes its fan-out at
	// once, which ends the watch that asked for it.
	go t.run()
	return t, nil
}

// joinTailer takes a lease on the kind's registered tailer, if it has a healthy
// one.
func (bh *Beehive) joinTailer(gk GroupKind) (*objectTailer, bool) {
	bh.tailMu.Lock()
	defer bh.tailMu.Unlock()
	return joinLocked(bh.tailers, gk)
}

// joinLocked is joinTailer's body, for the caller that already holds tailMu.
func joinLocked(tailers map[GroupKind]*objectTailer, gk GroupKind) (*objectTailer, bool) {
	t, ok := tailers[gk]
	if !ok || t.failure() != nil {
		return nil, false
	}
	t.refs++
	return t, true
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

// tooNewErr returns ErrWatchTooNew when cursor sits above everything the log
// has held, else nil. what names the log for the message.
func tooNewErr(what string, cursor, head int64) error {
	if cursor <= head {
		return nil
	}
	return fmt.Errorf("%w: %s resumed at %d, past its log's %d", ErrWatchTooNew, what, cursor, head)
}

// errNoRowImage marks a delete log entry with no row image to build the
// Deleted change from.
var errNoRowImage = errors.New("beehive: delete log entry carries no row image")

// tailPageCap caps one read of the log, so a burst stays bounded. Anything
// beyond a full page is read by the next step, not deferred.
const tailPageCap = 512

// defaultTailPagesPerDrain bounds one drain, so a resume after a long gap
// cannot monopolise the single connection. The remainder is read by the next
// drain, which the throttle paces.
//
// Two, not the waker's four: a tail page is 512 rows against 256, and costs a
// batched object read and a fan-out on top of the listing.
// BenchmarkTailerDrainRateUnderSustainedWrites measures ~3.6ms a page, so two
// pages behind a 100ms floor holds the connection under 7% of the time.
const defaultTailPagesPerDrain = 2

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
	// Owner is the object's current owner, nil when it has none. Resolved only
	// while the tailer has an owner-scoped subscriber; see objectTailer.ownerScoped.
	Owner *ObjectRef
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
	// ownerScoped turns on the per-page owner lookup. Set before a scoped
	// subscriber registers, and never cleared: a change published without an
	// owner while one is live would be dropped silently.
	// See docs/adr/2026-08-06-owner-scoped-watches.md.
	ownerScoped atomic.Bool
	// cursor is only touched by run.
	cursor int64
	// floor, retry and scanGate are only touched by run and the pass it calls,
	// which is the single goroutine rategate requires.
	floor    time.Duration
	retry    driver.Backoff
	scanGate *rategate.Single
	// now is replaced only before run starts; run's goroutine reads it.
	now func() time.Time
	// pagesPerDrain bounds one drain; a field so a benchmark can sweep it.
	pagesPerDrain int
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
		floor:      bh.watchFloor(),
		retry:      bh.watchBackoff(),
		scanGate:   rategate.NewSingle(bh.watchScanMinInterval),
		now:        time.Now,

		pagesPerDrain: defaultTailPagesPerDrain,
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
	next.Op = coalesceOp(prev.Op, next.Op)
	return next, true
}

// coalesceOp folds a run of writes to one object into the op to report: a run
// that began with a create still reports as a create, unless it ends in a
// delete. Create-then-update coalesces to a WriteUpdate on its own, but the
// object was not in the subscriber's snapshot, so reporting Modified would give
// a cache a change for an id it does not hold.
//
// Stated here because runs fold in two places, and which one applies is a
// matter of timing: writes inside one log page fold in collectChanges, writes
// spanning two wakes fold in the fan-out's mergeRawChange. Two copies of this
// rule would diverge only under a particular interleaving, handing one
// subscriber Added and another Modified for the same run.
func coalesceOp(began, ended WriteOp) WriteOp {
	if began == WriteCreate && ended != WriteDelete {
		return WriteCreate
	}
	return ended
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
	runWatchLoop(ctx, t.kindWrites.Chan(), t.floor, func() {
		// Only stop closes the wake hub — the last subscriber leaving comes
		// through ctx — so this is shutdown, and saying so is what keeps a closed
		// stream distinguishable from a cancelled one.
		err := error(ErrStopped)
		t.failed.Store(&err)
	}, func(backingOff bool) (time.Duration, bool, bool) {
		return t.pass(ctx, t.now(), backingOff)
	})
}

// pass is one turn of the run loop: drain, then report how long to wait,
// whether to drop the wakes arriving meanwhile, and whether the tailer is
// finished. backingOff is the loop's current answer to the second, carried in
// because a pass that does not drain has no new one. Split from run so the
// cadence is testable without waiting on it.
func (t *objectTailer) pass(ctx context.Context, now time.Time, backingOff bool) (next time.Duration, stillBackingOff, done bool) {
	// A refused wake is remembered by the re-arm: the drain that runs then reads
	// its position from the store. backingOff is carried rather than cleared, or
	// a retry timer firing inside the window would clear it.
	// See docs/adr/2026-08-05-the-object-tail-throttles-its-drains.md.
	if opensAt, held := t.scanGate.Allow(now); held {
		return opensAt.Sub(now), backingOff, false
	}

	more, err := t.drain(ctx)
	if err != nil {
		// The cursor is shared, so a trimmed cursor ends every subscriber:
		// skipping past the horizon would silently drop changes for all of them,
		// and there is no single subscriber to hand the error to. The next watch
		// starts a fresh tailer.
		if errors.Is(err, ErrWatchTooOld) {
			t.bh.log().Warn("watch tail fell below the retention horizon; ending its subscribers",
				"kind", t.gk.Kind, "err", err)
			t.failed.Store(&err)
			return 0, false, true
		}
		t.bh.log().Warn("watch tail step failed; retrying", "kind", t.gk.Kind, "err", err)
		return t.retry.Next(), true, false
	}

	t.retry.Reset()
	if !more {
		return t.floor, false, false
	}
	// Minus the drain that just ran, since the gate opened before it: re-arming
	// for a whole interval would pace a resume end-to-start.
	return max(0, t.scanGate.Interval()-t.now().Sub(now)), false, false
}

// drain reads pages until one comes back short or the budget is spent, so a
// burst that collapsed into one wake is read in full but a backlog is not. The
// page length is the stop test: it says exactly when the log is drained, and a
// second position read would cost an extra query for the same answer.
//
// The position check is that read, and it runs once here rather than once per
// page. It is the quiet-wake gate — a wake with nothing behind it costs one
// scalar query and no listing — but a full page is already proof there is more,
// so paying it per page would buy an answer the page length just gave, on the
// store's single connection.
// The bool reports whether the budget stopped it with work still above the
// cursor.
func (t *objectTailer) drain(ctx context.Context) (more bool, err error) {
	at, err := t.bh.store.ObjectWritesMaxVersion(ctx, t.gk)
	if err != nil {
		return false, err
	}
	// The position includes the retention horizon, so it only rises: test with
	// >, and an unmoved position means nothing was written.
	if at <= t.cursor {
		return false, nil
	}
	// max: a zero budget would read nothing and still report more, turning the
	// loop forever without draining.
	for range max(1, t.pagesPerDrain) {
		n, err := t.step(ctx)
		if err != nil {
			return false, err
		}
		if n < tailPageCap {
			return false, nil
		}
	}
	return true, nil
}

// step reads one page of the kind's log above the cursor, publishes what it
// found, and returns the page length. Its caller gates it; see drain.
func (t *objectTailer) step(ctx context.Context) (int, error) {
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

	changes, err := collectChanges(ctx, t.bh, t.gk, page, t.ownerScoped.Load())
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
// by a resume's replay. withOwners resolves each change's current owner, for an
// owner-scoped subscriber to filter on.
func collectChanges(ctx context.Context, bh *Beehive, gk GroupKind, page []ObjectWrite, withOwners bool) ([]rawChange, error) {
	// The page arrives ascending and resource_version is the log's primary
	// key, so keeping each id's highest-version entry preserves write order
	// without a sort.
	last := make(map[ObjectID]int64, len(page))
	// first is where each id's run in this page begins, which is what decides
	// the op to report; see coalesceOp. An id is created once and never reused,
	// so a create can only be the run's first entry.
	first := make(map[ObjectID]WriteOp, len(page))
	for _, w := range page {
		last[w.ID] = w.ResourceVersion
		if _, seen := first[w.ID]; !seen {
			first[w.ID] = w.Op
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

	// Only the live ids: a collected object's edges cascaded away, so it takes
	// its owner off the log entry's row image instead.
	var owners map[ObjectID][]ObjectRef
	if withOwners {
		if owners, err = bh.store.EdgesGroupOutgoingByID(ctx, live, RelationOwnedBy); err != nil {
			return nil, err
		}
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
		op := coalesceOp(first[w.ID], w.Op)
		// A collected object's owner survives only in the image; a live one is
		// read from its edges, since the image is a delete entry's alone.
		owner := raw.Owner
		if refs := owners[w.ID]; len(refs) > 0 {
			owner = &refs[0]
		}
		changes = append(changes, rawChange{
			ID:              w.ID,
			Op:              op,
			ResourceVersion: w.ResourceVersion,
			Object:          raw,
			Owner:           owner,
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
) (ObjectListSnapshot[Spec, Status], <-chan ObjectChange[Spec, Status], error) {
	var empty ObjectListSnapshot[Spec, Status]
	tailer, err := c.bh.tailerFor(ctx, c.gk)
	if err != nil {
		return empty, nil, fmt.Errorf("beehive: watch on %s/%s: %w", c.gk.Group, c.gk.Kind, err)
	}

	if cfg.scope.ownedBy != nil {
		// Before the receiver and the snapshot, or a change published in between
		// would reach this subscriber with no owner and be dropped as another's.
		tailer.ownerScoped.Store(true)
	}
	var opts []conflate.ReceiverOption[ObjectID, rawChange]
	if cfg.scope.only != nil {
		// Bounds this subscriber's memory to one key. An owner scope has no such
		// filter: which keys belong is what the watch is there to find out.
		opts = append(opts, tailer.hub.WithKeyFilter(func(k ObjectID) bool { return k == *cfg.scope.only }))
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
		raws, at, err := c.snapshot(ctx, cfg.scope)
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

		// work ends when the caller's ctx does or when the tailer stops. Every
		// read and retry below runs under it: they retry a failing store until
		// their context ends, so on the caller's ctx alone a store that keeps
		// failing past stop would hold this goroutine — and its lease — forever,
		// and the stream would never report ErrStopped. The lease is what makes
		// tailer.ctx unambiguous here: no other subscriber can end the tailer
		// while this one holds it, so a cancel means the tailer itself stopped.
		work, stopWork := mergeDone(ctx, tailer.ctx)
		defer stopWork()

		// One place sends the terminal Failed change; see endStream.
		if cfg.resumeFrom != nil {
			at, fail, ok := c.replay(work, mig, cfg, floor, out)
			if !ok {
				c.endStream(ctx, tailer, fail, out)
				return
			}
			floor = at
		}
		c.consume(ctx, work, tailer, rx, mig, cfg, floor, out)
	}()
	return snap, out, nil
}

// mergeDone returns a context that ends when either parent does. The returned
// func must be called to release the watch on second.
func mergeDone(first, second context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(first)
	stop := context.AfterFunc(second, cancel)
	return ctx, func() { stop(); cancel() }
}

// endStream sends the one Failed change a stream is allowed, or nothing when
// the caller's own context ended — a silent close is how that case is reported,
// so a supervisor can tell its own cancellation from everything else. fail is
// what the subscriber discovered itself; absent that, the tailer says why.
func (c *clientImpl[Spec, Status]) endStream(
	ctx context.Context,
	tailer *objectTailer,
	fail error,
	out chan<- ObjectChange[Spec, Status],
) {
	if ctx.Err() != nil {
		return
	}
	if fail == nil {
		fail = tailer.failure()
	}
	if fail != nil {
		sendOrDone(ctx, out, ObjectChange[Spec, Status]{Type: Failed, Err: fail})
	}
}

// consume delivers what the tailer publishes above floor until the fan-out
// closes or work ends. ctx is the caller's, for reporting; work also ends when
// the tailer does, so a retry cannot outlive it.
func (c *clientImpl[Spec, Status]) consume(
	ctx context.Context,
	work context.Context,
	tailer *objectTailer,
	rx *conflate.Receiver[ObjectID, rawChange],
	mig Migrator,
	cfg watchConfig,
	floor int64,
	out chan<- ObjectChange[Spec, Status],
) {
	for {
		ev, err := rx.RecvContext(work)
		if err != nil {
			// Either the fan-out closed or work ended. Both resolve the same
			// way: the tailer says why unless the caller cancelled.
			c.endStream(ctx, tailer, nil, out)
			return
		}
		changes, ok := c.decodeBatch(work, drainPending(ev.Value, rx), mig, cfg, floor)
		if !ok {
			c.endStream(ctx, tailer, nil, out) // the tailer stopped under a retry
			return
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

// decodeBatch decodes the batch and loads its relations, retrying the load
// instead of skipping it: the tailer's cursor has already moved past these
// entries, so no later read brings them back. It reports false only when ctx
// ended.
//
// Only the load is retried. Decoding is pure and cannot fail the call, so
// repeating it would re-unmarshal and re-migrate a whole page — up to
// tailPageCap objects — on every backoff step, and re-log every quarantined row
// with it.
func (c *clientImpl[Spec, Status]) decodeBatch(
	ctx context.Context,
	batch []rawChange,
	mig Migrator,
	cfg watchConfig,
	floor int64,
) ([]ObjectChange[Spec, Status], bool) {
	changes, loaded := c.decodeChanges(batch, mig, cfg, floor)
	retry := c.bh.watchBackoff()
	for {
		err := c.loadListRelated(ctx, loaded, cfg.remainingLoads())
		if err == nil {
			return changes, true
		}
		if !c.pollFailed(ctx, "watch relation load", err) || !retry.Wait(ctx) {
			return nil, false
		}
	}
}

// decodeChanges turns raw changes into typed ones, dropping entries the
// caller's snapshot already covered and skipping rows that do not decode. It
// cannot fail — an undecodable row is quarantined, never reported — so it takes
// no context and returns none. loaded is the subset whose relations the caller
// still has to read.
func (c *clientImpl[Spec, Status]) decodeChanges(
	batch []rawChange,
	mig Migrator,
	cfg watchConfig,
	floor int64,
) ([]ObjectChange[Spec, Status], []*Object[Spec, Status]) {
	changes := make([]ObjectChange[Spec, Status], 0, len(batch))
	// Deleted objects have no relations to load: the edges went with the row.
	var loaded []*Object[Spec, Status]
	for _, raw := range batch {
		if raw.ResourceVersion <= floor {
			continue
		}
		if owner := cfg.scope.ownedBy; owner != nil && (raw.Owner == nil || raw.Owner.ID != *owner) {
			// A nil owner means "unowned" and "not resolved" alike, and the second
			// would drop this change for good. The gate is armed before a scoped
			// subscriber registers precisely so it cannot happen.
			if raw.Owner == nil {
				c.bh.log().Warn("beehive: dropping a change with an unresolved owner",
					"op", "Watch", "group", c.gk.Group, "kind", c.gk.Kind,
					"id", raw.ID, "resourceVersion", raw.ResourceVersion)
			}
			continue
		}
		obj, err := rawToTyped[Spec, Status](raw.Object, mig)
		if err != nil {
			// Quarantine: one bad row must not kill a live watcher.
			c.warnUndecodable("Watch", raw.ID, err)
			// A create or update is repaired by the next write to the row. A
			// physical delete is the last thing the log will ever say about this
			// id — ids are never reused — so dropping it strands the object in
			// every mirror for good. Report the removal without the state.
			if raw.Op != WriteDelete {
				continue
			}
			obj = nil
		}
		changes = append(changes, ObjectChange[Spec, Status]{
			Type:            changeType(raw.Op),
			ID:              raw.ID,
			ResourceVersion: raw.ResourceVersion,
			Object:          obj,
		})
		if raw.Op != WriteDelete && cfg.loads != 0 {
			if cfg.scope.ownedBy != nil {
				// A scoped change reached here only by matching, so its owner is
				// known and re-reading the edge would repeat the tailer's query.
				obj.owner, obj.loaded = raw.Owner, obj.loaded|LoadOwnerBit
			}
			loaded = append(loaded, obj)
		}
	}
	return changes, loaded
}

// replay delivers the gap between a resume's position and the tail, in pages —
// with a day of retention the gap can be far more than one page. It returns
// the position reached, or ok false with the failure to report — nil when there
// is nothing to report beyond whatever the tailer says. One place sends the
// Failed change, so a replay that ends as the tailer does cannot send two.
func (c *clientImpl[Spec, Status]) replay(
	ctx context.Context,
	mig Migrator,
	cfg watchConfig,
	from int64,
	out chan<- ObjectChange[Spec, Status],
) (int64, error, bool) {
	cursor := from
	retry := c.bh.watchBackoff()
	for {
		page, trimmedThrough, err := c.bh.store.ObjectWritesListSince(ctx, c.gk, cursor, tailPageCap)
		if err != nil {
			// A failed read costs a retry, not the stream: the cursor has not
			// moved, so nothing is lost.
			if !c.pollFailed(ctx, "watch resume", err) || !retry.Wait(ctx) {
				return 0, nil, false
			}
			continue
		}
		// Retention can overtake a replay that is still paging.
		if err := horizonErr(c.gk, "the resume", cursor, trimmedThrough); err != nil {
			return 0, err, false
		}
		if len(page) == 0 {
			// Caught up — or resuming above everything this kind's log has held,
			// which means the position did not come from this store. An empty
			// page cannot tell those apart, and one scalar read can: unreported,
			// the second holds floor above every later change and drops them all
			// silently. Only a resume at or beyond the head gets this far, so no
			// replay with real work to do pays for it.
			at, err := c.bh.store.ObjectWritesMaxVersion(ctx, c.gk)
			if err != nil {
				if !c.pollFailed(ctx, "watch resume", err) || !retry.Wait(ctx) {
					return 0, nil, false
				}
				continue
			}
			if err := tooNewErr(c.gk.Group+"/"+c.gk.Kind, cursor, at); err != nil {
				return 0, err, false
			}
			return cursor, nil, true
		}
		next := page[len(page)-1].ResourceVersion
		full := len(page) == tailPageCap
		if cfg.scope.only != nil {
			// Filter before the read: collectChanges would otherwise read back
			// every object in the page to deliver at most one.
			page = slices.DeleteFunc(page, func(w ObjectWrite) bool { return w.ID != *cfg.scope.only })
		}
		// An owner scope cannot narrow the page first — ownership is not in the
		// log — so it reads the page back in full and filters after.
		raws, err := collectChanges(ctx, c.bh, c.gk, page, cfg.scope.ownedBy != nil)
		if err != nil {
			if !c.pollFailed(ctx, "watch resume", err) || !retry.Wait(ctx) {
				return 0, nil, false
			}
			continue
		}
		retry.Reset()
		changes, ok := c.decodeBatch(ctx, raws, mig, cfg, cursor)
		if !ok {
			return 0, nil, false
		}
		for _, ch := range changes {
			if !sendOrDone(ctx, out, ch) {
				return 0, nil, false
			}
		}
		cursor = next
		if !full {
			return cursor, nil, true
		}
	}
}
