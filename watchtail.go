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

// wakeHub tells a kind's tailer that the kind moved. Keyed by GroupKind and
// latest-value: a burst coalesces into one pending wake, and a publish that
// lands mid-read waits in the slot. The value is a process-local tick, not a
// resource version — it only has to rise. Close closes the sender, never the
// hub — scheduleHub's rule. See docs/adr/2026-08-03-watch-shared-tail.md.
type wakeHub struct {
	hub *watch.Hub[GroupKind, int64]
	seq *atomic.Int64
}

func newWakeHub() wakeHub {
	return wakeHub{
		hub: watch.New[GroupKind](watch.WithAccept(
			func(prev, next int64) bool { return next > prev },
		)),
		seq: new(atomic.Int64),
	}
}

// Send is a no-op on the zero hub, which is what a Beehive assembled without
// New has — the same courtesy bh.log() extends to an unresolved logger.
func (h wakeHub) Send(gk GroupKind) error {
	if h.hub == nil {
		return nil
	}
	return h.hub.Sender().Send(gk, h.seq.Add(1))
}

// Watch registers a receiver for gk, seeded at zero: the tailer reads its own
// starting cursor from the store, not from the hub.
func (h wakeHub) Watch(gk GroupKind) *watch.Receiver[GroupKind, int64] {
	return h.hub.Watch(gk, 0)
}

// Close is a no-op on the zero hub; see Send.
func (h wakeHub) Close() {
	if h.hub != nil {
		h.hub.Sender().Close()
	}
}

// tailerFor returns the kind's tailer, starting one on the first watch. A tailer
// that ended (a trimmed cursor, or the beehive stopping) is replaced, so a
// caller never joins a dead fan-out.
//
// Guarded by tailMu, never bh.mu: bh.mu is not reentrant and the resolvers under
// it (migratorFor, reconcilerFor) take it. The store read that builds a tailer
// stays outside the lock, so a first watch on one kind does not queue behind
// another kind's query; the loser of a race discards its tailer.
func (bh *Beehive) tailerFor(ctx context.Context, gk GroupKind) (*objectTailer, error) {
	bh.tailMu.Lock()
	if t, ok := bh.tailers[gk]; ok && t.failure() == nil {
		bh.tailMu.Unlock()
		return t, nil
	}
	bh.tailMu.Unlock()

	t, err := newObjectTailer(ctx, bh, gk)
	if err != nil {
		return nil, err
	}

	bh.tailMu.Lock()
	defer bh.tailMu.Unlock()
	if live, ok := bh.tailers[gk]; ok && live.failure() == nil {
		t.close()
		return live, nil
	}
	if bh.tailers == nil {
		bh.tailers = make(map[GroupKind]*objectTailer)
	}
	bh.tailers[gk] = t
	// A tailer started after stop finds tailCtx already cancelled and closes its
	// fan-out at once, which is what ends the watch that asked for it.
	bh.tailWG.Go(func() { t.run(bh.tailCtx) })
	return t, nil
}

// horizonErr reports a cursor the write log has already trimmed past, or nil.
// Strictly <: a cursor sitting exactly on the horizon has lost nothing, and a
// kind that stops writing converges onto exactly that.
func horizonErr(gk GroupKind, what string, cursor, trimmedThrough int64) error {
	if cursor >= trimmedThrough {
		return nil
	}
	return fmt.Errorf("%w: %s/%s trimmed through %d, %s was at %d",
		ErrWatchTooOld, gk.Group, gk.Kind, trimmedThrough, what, cursor)
}

// errNoRowImage reports a delete entry the store returned without the row image
// a Deleted change is built from.
var errNoRowImage = errors.New("beehive: delete log entry carries no row image")

// tailPageCap bounds one read of the log, which is what keeps a burst from being
// unbounded. A fuller page than this is drained by the next step, not deferred.
const tailPageCap = 512

// changeType maps one log entry to what a subscriber is told. The soft delete is
// a WriteUpdate, so a finalizing object reports Modified — Deleted means the row
// is gone. A coalesced run promotes Modified to Added when the run began with a
// create; a run ending in a delete stays Deleted either way, since the row is
// gone and the entry carries the state to report.
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

// rawChange is what the tailer fans out: one object's newest log entry plus the
// state to report it with. Undecoded — two clients may watch one kind with
// different type parameters, so decode belongs to each subscriber.
type rawChange struct {
	ID              ObjectID
	Op              WriteOp
	ResourceVersion int64
	// Object is the row as read back, or the log entry's row image for a delete.
	Object *RawObject
}

// objectTailer is one kind's shared reader: it owns the kind's cursor, runs the
// gate, log page and batched read once, and fans the result out to every watch
// on the kind. One goroutine per kind, not per watch.
type objectTailer struct {
	bh    *Beehive
	gk    GroupKind
	hub   *conflate.Hub[ObjectID, rawChange]
	wakes *watch.Receiver[GroupKind, int64]
	// cursor is the tailer's alone; only run touches it.
	cursor int64
	// failed records why the fan-out closed, for subscribers to report. Written
	// before the sender closes, read after.
	failed atomic.Pointer[error]
}

// newObjectTailer registers the wake receiver BEFORE reading the starting
// cursor: a write landing between the two would otherwise be lost to both. The
// cursor is read before returning, so a subscriber that snapshots after this
// call cannot fall into the gap either.
func newObjectTailer(ctx context.Context, bh *Beehive, gk GroupKind) (*objectTailer, error) {
	t := &objectTailer{
		bh:    bh,
		gk:    gk,
		hub:   conflate.New[ObjectID](mergeRawChange),
		wakes: bh.wakes.Watch(gk),
	}
	at, err := bh.store.ObjectWritesMaxVersion(ctx, gk)
	if err != nil {
		t.close()
		return nil, err
	}
	t.cursor = at
	return t, nil
}

// close releases the tailer's hub and wake receiver.
func (t *objectTailer) close() {
	t.wakes.Close()
	t.hub.Sender().Close()
}

// mergeRawChange resolves two undelivered changes for one object into one:
// newest wins, and a run whose start the subscriber has not seen still reports
// as a create. It never drops a pair — the pending slot is hub-wide while a
// snapshot is per subscriber, so annihilating an unread create/delete would
// strand the object at a subscriber that snapshotted between the two.
func mergeRawChange(prev, next rawChange) (rawChange, bool) {
	if next.ResourceVersion <= prev.ResourceVersion {
		return prev, true // a send the pending value already covers
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

// run tails the kind's log until ctx ends, waking on a commit rather than a
// tick. Everything at or below the starting cursor is a subscriber's snapshot
// to report, not the tail's.
func (t *objectTailer) run(ctx context.Context) {
	defer t.close()

	wakes := t.wakes.Chan()
	floor := t.bh.watchFloor()
	timer := time.NewTimer(floor)
	defer timer.Stop()

	retry := driver.Backoff{Base: watchRetryBase, Max: floor}
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-wakes:
			if !ok {
				return // the beehive stopped
			}
		case <-timer.C:
		}

		next := floor
		if err := t.drain(ctx); err != nil {
			// The cursor is shared, so a trimmed cursor ends every subscriber
			// rather than one: advancing past the horizon would drop changes for
			// all of them silently, and a shared reader has no one subscriber to
			// hand the error to. The next watch starts a fresh tailer.
			if errors.Is(err, ErrWatchTooOld) {
				t.bh.log().Warn("watch tail fell below the retention horizon; ending its subscribers",
					"kind", t.gk.Kind, "err", err)
				t.failed.Store(&err)
				return
			}
			t.bh.log().Warn("watch tail step failed; retrying", "kind", t.gk.Kind, "err", err)
			next = retry.Next()
		} else {
			retry.Reset()
		}
		timer.Stop()
		timer.Reset(next)
	}
}

// drain reads pages until one comes back short, so a burst that coalesced into
// one wake is not left half-read. Short page, not a second position read: the
// page length answers it exactly and step already paid for the gate.
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
// found, and returns the page length. The gate read makes a quiet wake cost one
// number.
func (t *objectTailer) step(ctx context.Context) (int, error) {
	at, err := t.bh.store.ObjectWritesMaxVersion(ctx, t.gk)
	if err != nil {
		return 0, err
	}
	// The position folds in the retention horizon, so it only rises: > is the
	// test, and an unmoved position means nothing was written.
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
	// The page arrives ascending and resource_version is the log's primary key,
	// so keeping the entry that matches each id's highest version preserves write
	// order without a sort.
	last := make(map[ObjectID]int64, len(page))
	// Whether this run began with a create. The coalesced entry is the last one,
	// which for create-then-update is a WriteUpdate — but the object was absent
	// from the subscriber's snapshot, so reporting Modified would hand a cache a
	// change for an id it does not hold.
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

	// One batched read for everything still live. Per-object reads would be
	// serialized round trips on a single connection.
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
			// Absent means collected between the two reads. Skip it: the delete
			// appended its own entry above this page, so it arrives later.
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
// published in between must already have a receiver to land in. Dropping at or
// below the snapshot's position is what makes that safe: resource_version is one
// log-wide sequence and the snapshot is a consistent cut at it.
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

	mig := c.bh.migratorFor(c.gk) // invariant for the stream's lifetime

	var snap ObjectListSnapshot[Spec, Status]
	// floor is what the caller already holds; deliveries at or below it are
	// dropped. A resume raises it as the replay advances.
	var floor int64
	if cfg.resumeFrom != nil {
		// A resume reads no state. It only proves the position is still inside
		// the log, which is the one failure the caller can act on; the gap
		// itself is replayed on the stream's goroutine.
		if err := c.resumable(ctx, *cfg.resumeFrom); err != nil {
			rx.Close()
			return empty, nil, err
		}
		snap.ResourceVersion, floor = *cfg.resumeFrom, *cfg.resumeFrom
	} else {
		raws, at, err := c.snapshot(ctx, only)
		if err != nil {
			rx.Close()
			return empty, nil, fmt.Errorf("beehive: watch on %s/%s: initial read failed: %w",
				c.gk.Group, c.gk.Kind, err)
		}
		snap.ResourceVersion, floor = at, at
		snap.Objects = c.decodeList(raws, "Watch")
		if err := c.loadListRelated(ctx, snap.Objects, cfg.loads); err != nil {
			rx.Close()
			return empty, nil, fmt.Errorf("beehive: watch on %s/%s: initial read failed: %w",
				c.gk.Group, c.gk.Kind, err)
		}
	}

	out := make(chan ObjectChange[Spec, Status])
	go func() {
		defer close(out)
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
			// The fan-out closed. A tailer that ended below the retention
			// horizon cannot continue truthfully, and says so; one that ended
			// with the beehive just closes the stream.
			if failure := tailer.failure(); failure != nil {
				sendOrDone(ctx, out, ObjectChange[Spec, Status]{Type: Failed, Err: failure})
			}
			return
		}
		// Drain what is already pending before loading relations, so a burst
		// costs one relation query rather than one per object.
		batch := []rawChange{ev.Value}
		for {
			next, err := rx.TryRecv()
			if err != nil {
				break
			}
			batch = append(batch, next.Value)
		}
		changes, ok := c.decodeBatch(ctx, batch, mig, cfg, floor)
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

// decodeBatch is decodeChanges with the relation load retried rather than
// skipped: the cursor has already moved past these entries, so no later read
// brings them back. It reports false only when ctx ended.
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

// decodeChanges turns raw changes into typed ones, dropping what the caller's
// snapshot already carried and quarantining what will not decode.
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
// with a day of retention the gap can far exceed one page. It returns the
// position reached, or false when the stream is over.
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
			// A transient read costs a retry, not the stream: the cursor has not
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
			// Before the read, not after: collectChanges would otherwise read
			// back every object in the page to deliver at most one.
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
