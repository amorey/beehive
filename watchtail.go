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
	"sync/atomic"
	"time"

	"github.com/amorey/gobus/conflate"
	"github.com/amorey/gobus/watch"
)

// wakeHub tells a kind's tailer that the kind moved. Keyed by GroupKind and
// latest-value: a burst coalesces into one pending wake, and a publish that
// lands mid-read waits in the slot. Close closes the sender, never the hub —
// scheduleHub's rule.
//
// The value is a process-local tick, not the write's resource version: most
// store writes return no version (see the write-shapes ADR), and the tailer
// reads its own cursor from the store anyway. All the value has to do is rise,
// so Accept can drop a publish the hub has already superseded.
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
// Guarded by tailMu, not bh.mu: bh.mu is not reentrant and the resolvers under
// it (migratorFor, reconcilerFor) take it, and the store read below would hold
// it for the duration of a query.
func (bh *Beehive) tailerFor(ctx context.Context, gk GroupKind) (*objectTailer, error) {
	bh.tailMu.Lock()
	defer bh.tailMu.Unlock()
	if t, ok := bh.tailers[gk]; ok && t.failure() == nil {
		return t, nil
	}
	t, err := newObjectTailer(ctx, bh, gk)
	if err != nil {
		return nil, err
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

// rawChange is what the tailer fans out: one object's newest log entry plus the
// state to report it with. Undecoded on purpose — two clients may watch one kind
// with different type parameters, so decode belongs to each subscriber.
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
// cursor — a write landing between the two would otherwise be lost to both —
// and reads the cursor before returning, so a subscriber that snapshots after
// this call cannot fall into the gap either.
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
// newest wins, and a run the subscriber has not seen the start of still reports
// as a create.
//
// Nothing is ever dropped. Annihilating an unread create/delete pair looks safe
// and is not: the pending slot is hub-wide while a snapshot is per subscriber,
// so a subscriber that snapshotted between the two would hold the object forever
// with no delete coming. A delete for a key a consumer never saw is a no-op at
// any cache.
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
	floor := t.bh.watchFloorInterval
	timer := time.NewTimer(floor)
	defer timer.Stop()

	retry := watchRetryBase
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
			// Bounded backoff, capped at the floor: a transient failure recovers
			// sooner than a floor tick, a persistent one costs no more.
			next = min(retry, floor)
			retry *= 2
		} else {
			retry = watchRetryBase
		}
		timer.Stop()
		timer.Reset(next)
	}
}

// drain reads pages until one comes back short. A burst coalesces into one wake,
// so a tailer that read a single page per wake would strand the remainder until
// some later write. A short page means the log is drained — exactly, and without
// the second position read that asking the store again would cost.
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
	// Strictly <: a cursor sitting exactly on the horizon has lost nothing.
	if t.cursor < trimmedThrough {
		return 0, fmt.Errorf("%w: %s/%s trimmed through %d, tail was at %d",
			ErrWatchTooOld, t.gk.Group, t.gk.Kind, trimmedThrough, t.cursor)
	}
	if len(page) == 0 {
		return 0, nil
	}

	changes, err := t.collect(ctx, page)
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

// collect coalesces a log page to the last entry per object and reads the
// current state of everything still live, in one batch.
func (t *objectTailer) collect(ctx context.Context, page []ObjectWrite) ([]rawChange, error) {
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
	rows, err := t.bh.store.ObjectsListByIDs(ctx, t.gk, live)
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
			t.bh.log().Warn("watch tail dropped a change", "kind", t.gk.Kind, "id", w.ID, "err", errNoRowImage)
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
