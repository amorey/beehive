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
	"time"

	"github.com/amorey/beehive/internal/driver"
)

// This file is the client's watch surface. The store pushes nothing, so a watch
// polls and diffs: read current state on a tick, compare with what this
// subscriber was last told, send the difference. Consequences callers trip on:
// intermediate states are invisible (changes within one interval collapse into
// the latest state), and latency is the poll interval, not the write.

// watchPoll returns the poll interval; the fallback covers only a Beehive built
// field by field in a test.
func (bh *Beehive) watchPoll() time.Duration {
	if bh.watchPollInterval <= 0 {
		return defaultWatchPollInterval
	}
	return bh.watchPollInterval
}

// sendOrDone delivers v unless ctx is cancelled first, and reports whether it
// landed. Cancellation is checked first, on its own: once a reader parks on
// out, both select arms are ready and Go picks at random — a subscriber that
// gave up must not be handed one more value.
func sendOrDone[V any](ctx context.Context, out chan<- V, v V) bool {
	select {
	case <-ctx.Done():
		return false
	default:
	}
	select {
	case out <- v:
		return true
	case <-ctx.Done():
		return false
	}
}

// pollFailed logs a failed poll and says whether to keep going: a transient
// store error costs one tick, not the stream; a cancelled context is shutdown.
func (c *clientImpl[Spec, Status]) pollFailed(ctx context.Context, what string, err error, args ...any) bool {
	if ctx.Err() != nil {
		return false
	}
	c.bh.log().WarnContext(ctx, "beehive: "+what+" poll failed; retrying on the next tick",
		append([]any{"group", c.gk.Group, "kind", c.gk.Kind, "err", err}, args...)...)
	return true
}

// ObjectsWatchList streams changes to every object of this client's kind. See
// the Client interface for the contract.
func (c *clientImpl[Spec, Status]) ObjectsWatchList(ctx context.Context) (Snapshot[Spec, Status], <-chan ObjectChange[Spec, Status], error) {
	if !c.bh.isRegistered(c.gk) {
		return Snapshot[Spec, Status]{}, nil, fmt.Errorf("beehive: no controller registered for %s/%s", c.gk.Group, c.gk.Kind)
	}
	return c.objectStream(ctx,
		func(ctx context.Context) ([]*RawObject, int64, error) {
			return c.bh.store.ObjectWritesSnapshot(ctx, c.gk)
		},
		func(ObjectID) bool { return true })
}

// ObjectsWatch streams changes to the single object id, polling a one-row
// listing: an id that does not exist yet streams nothing until created, and its
// removal reads as a Deleted.
func (c *clientImpl[Spec, Status]) ObjectsWatch(ctx context.Context, id ObjectID) (Snapshot[Spec, Status], <-chan ObjectChange[Spec, Status], error) {
	if !c.bh.isRegistered(c.gk) {
		return Snapshot[Spec, Status]{}, nil, fmt.Errorf("beehive: no controller registered for %s/%s", c.gk.Group, c.gk.Kind)
	}
	// The tail is the kind's, filtered here: the log carries no index under
	// object_id, so a single-object watch costs what its kind writes.
	return c.objectStream(ctx,
		func(ctx context.Context) ([]*RawObject, int64, error) {
			return c.bh.store.ObjectWritesSnapshotByID(ctx, c.gk, id)
		},
		func(changed ObjectID) bool { return changed == id })
}

// objectStream is the tail behind both object watches.
//
// The returned Snapshot is read on the caller's goroutine, which is what makes
// "subscribe, then act" safe: a change the caller makes next — including a
// delete — lands above the position the snapshot carries. A failed first read
// therefore returns an error instead of a stream whose guarantee is quietly
// void.
//
// After that the stream tails the write log from that position. Change types
// come from the log: a create entry is Added, a physical delete is Deleted
// carrying the entry's row image, anything else is Modified. The soft delete is
// an ordinary update, so a finalizing object arrives as Modified with
// DeletionRequestedAt set.
//
// Most ticks read nothing else: an unmoved log position for this kind proves
// nothing was written, so a quiet tick costs one scalar read. The position is
// the write log's, NOT the shared version counter — the counter moves for event
// writes too, which would defeat the optimization permanently for any controller
// that records an event per reconcile.
//
// A row that fails to decode is quarantined as List does it.
func (c *clientImpl[Spec, Status]) objectStream(
	ctx context.Context,
	snapshot func(context.Context) ([]*RawObject, int64, error),
	match func(ObjectID) bool,
) (Snapshot[Spec, Status], <-chan ObjectChange[Spec, Status], error) {
	// The migrator is invariant for the stream's lifetime.
	mig := c.bh.migratorFor(c.gk)

	// The snapshot, on the caller's goroutine; its failure is the one the
	// caller can act on. Every later failure costs one tick.
	raws, at, err := snapshot(ctx)
	if err != nil {
		return Snapshot[Spec, Status]{}, nil, fmt.Errorf("beehive: watch on %s/%s: initial read failed: %w",
			c.gk.Group, c.gk.Kind, err)
	}

	snap := Snapshot[Spec, Status]{ResourceVersion: at}
	for _, raw := range raws {
		obj, err := rawToTyped[Spec, Status](raw, mig)
		if err != nil {
			c.warnUndecodable("Watch", raw.ID, err)
			continue
		}
		snap.Objects = append(snap.Objects, obj)
	}

	out := make(chan ObjectChange[Spec, Status])
	go func() {
		defer close(out)
		// Seeded from the snapshot: the stream starts where the listing ended, so
		// it neither repeats it nor skips what followed.
		cursor := at
		driver.Run(ctx, c.bh.watchPoll(), func(ctx context.Context) bool {
			changes, err := c.poll(ctx, mig, &cursor)
			if errors.Is(err, ErrWatchTooOld) {
				// Terminal, unlike a transient read failure: the entries this
				// stream had not read are gone, so it cannot continue truthfully.
				sendOrDone(ctx, out, ObjectChange[Spec, Status]{Type: Failed, Err: err})
				return false
			}
			if err != nil {
				return c.pollFailed(ctx, "watch", err)
			}
			for _, ch := range changes {
				if !match(ch.Object.ID) {
					continue
				}
				if !sendOrDone(ctx, out, ch) {
					return false
				}
			}
			return true
		})
	}()
	return snap, out, nil
}

// tailPageCap bounds one tick's read of the log. A busier interval than this
// spills into the next tick, which is what keeps a burst from being unbounded.
const tailPageCap = 512

// poll runs one tick: read the kind's log position, and if it moved, tail the
// entries above the cursor and turn them into changes.
//
// Entries coalesce by object: only the highest per id survives, and its current
// state is read back in one batch. A subscriber therefore sees what is, never a
// version already superseded — the level-triggered contract the rest of beehive
// keeps. Changes come back in write order, which is NOT the id order the batched
// read returns.
func (c *clientImpl[Spec, Status]) poll(
	ctx context.Context,
	mig Migrator,
	cursor *int64,
) ([]ObjectChange[Spec, Status], error) {
	at, err := c.bh.store.ObjectWritesMaxVersion(ctx, c.gk)
	if err != nil {
		return nil, err
	}
	// The position folds in the retention horizon, so it only rises: > is the
	// test, and an unmoved position means nothing was written.
	if at <= *cursor {
		return nil, nil
	}
	page, trimmedThrough, err := c.bh.store.ObjectWritesListSince(ctx, c.gk, *cursor, tailPageCap)
	if err != nil {
		return nil, err
	}
	// Strictly <: a cursor sitting exactly on the horizon has lost nothing, and a
	// kind that stops writing converges onto exactly that. The check rides this
	// read because retention can move between ticks.
	if *cursor < trimmedThrough {
		return nil, fmt.Errorf("%w: %s/%s trimmed through %d, stream was at %d",
			ErrWatchTooOld, c.gk.Group, c.gk.Kind, trimmedThrough, *cursor)
	}
	if len(page) == 0 {
		return nil, nil
	}

	// Coalesce to the last entry per object, then order by that entry: a
	// subscriber is told what happened in the order it happened, and an object
	// written twice reports once.
	latest := make(map[ObjectID]ObjectWrite, len(page))
	for _, w := range page {
		latest[w.ID] = w
	}
	order := make([]ObjectWrite, 0, len(latest))
	for _, w := range latest {
		order = append(order, w)
	}
	slices.SortFunc(order, func(a, b ObjectWrite) int {
		return cmp.Compare(a.ResourceVersion, b.ResourceVersion)
	})

	// One batched read for everything still live. Per-object reads would be
	// serialized round trips on a single connection, which is what made the old
	// full listing competitive.
	var live []ObjectID
	for _, w := range order {
		if w.Op != WriteDelete {
			live = append(live, w.ID)
		}
	}
	rows, err := c.bh.store.ObjectsListByIDs(ctx, c.gk, live)
	if err != nil {
		return nil, err
	}
	byID := make(map[ObjectID]*RawObject, len(rows))
	for _, raw := range rows {
		byID[raw.ID] = raw
	}

	changes := make([]ObjectChange[Spec, Status], 0, len(order))
	for _, w := range order {
		raw := w.Final
		if w.Op != WriteDelete {
			// Absent means collected between the two reads. Skip it: the delete
			// appended its own entry above this page, so it arrives as a Deleted
			// on a later tick.
			if raw = byID[w.ID]; raw == nil {
				continue
			}
		}
		obj, err := rawToTyped[Spec, Status](raw, mig)
		if err != nil {
			// Quarantine: one bad row must not kill a live watcher.
			c.warnUndecodable("Watch", w.ID, err)
			continue
		}
		changes = append(changes, ObjectChange[Spec, Status]{Type: changeType(w.Op), Object: obj})
	}
	*cursor = page[len(page)-1].ResourceVersion
	return changes, nil
}

// changeType maps a log entry to what a subscriber is told. The soft delete is a
// WriteUpdate, so a finalizing object reports Modified — Deleted means the row
// is gone.
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

// EventsWatch streams id's event log. Runs are keyed by id and compared by
// resource_version, so a run extended between ticks is sent again with its new
// state. No tombstones: an append-only log means a run only appears or grows. A
// quiet tick reads only EventsMaxVersion — one scalar over this object's log —
// and lists only when it moved.
func (c *clientImpl[Spec, Status]) EventsWatch(ctx context.Context, id ObjectID, opts ...EventOption) (<-chan Event, error) {
	if !c.bh.isRegistered(c.gk) {
		return nil, fmt.Errorf("beehive: no controller registered for %s/%s", c.gk.Group, c.gk.Kind)
	}
	q := resolveEvents(opts)
	out := make(chan Event)
	seen := make(map[EventID]int64)
	// The log's high-water mark as of the last listing; an empty log reads 0
	// and never pays for a listing.
	var cursor int64
	// scoped/foreign latch the kind check: group and kind are fixed at insert
	// and ids never reused, so once the id resolves the answer cannot change.
	// Only "not found yet" stays unlatched — the id can still be created later.
	var scoped, foreign bool

	go func() {
		defer close(out)
		driver.Run(ctx, c.bh.watchPoll(), func(ctx context.Context) bool {
			if foreign {
				// This id belongs to another kind and always will. The stream
				// stays open and silent, which is the contract.
				return true
			}
			if !scoped {
				// Kind-scope the read, as the object watches do; a missing id
				// streams nothing until it exists.
				raw, err := c.bh.store.ObjectsGetMeta(ctx, id)
				if errors.Is(err, ErrNotFound) {
					return true
				}
				if err != nil {
					return c.pollFailed(ctx, "event watch", err, "id", id)
				}
				if raw.Group != c.gk.Group || raw.Kind != c.gk.Kind {
					foreign = true
					return true
				}
				scoped = true
			}
			at, err := c.bh.store.EventsMaxVersion(ctx, id)
			if err != nil {
				return c.pollFailed(ctx, "event watch", err, "id", id)
			}
			if at == cursor {
				// Nothing added or extended; a vanished run is retention, not a
				// change.
				return true
			}
			runs, err := c.bh.store.EventsList(ctx, id, q)
			if err != nil {
				return c.pollFailed(ctx, "event watch", err, "id", id)
			}
			// Advanced only past a successful listing, so a failed one is
			// retried next tick.
			cursor = at
			// Rebuilt per listing so the map is bounded by the query, not by
			// every run ever seen — retention deletes runs. A quiet tick does
			// not rebuild, so a trimmed run's id lingers until the next real
			// event; bounded, memory only.
			next := make(map[EventID]int64, len(runs))
			// EventsList is newest-first; deliver oldest-first so the timeline
			// builds in order.
			for i := len(runs) - 1; i >= 0; i-- {
				run := runs[i]
				prev, known := seen[run.ID]
				next[run.ID] = run.ResourceVersion
				if known && prev == run.ResourceVersion {
					continue
				}
				if !sendOrDone(ctx, out, eventFromRaw(run)) {
					return false
				}
			}
			seen = next
			return true
		})
	}()
	return out, nil
}

// SchedulesWatch streams id's schedule as a gauge; a client-only kind returns
// ErrNoController. The one watch that does not poll: the work queue publishes
// each move to a hub and this stream reads its own receiver. That is sound only
// because the queue is unexported and process-local (the hub sees every writer
// that exists) and the gauge reports every move from one type — give workQueue
// a second writer and the poll has to come back. See
// docs/adr/2026-07-27-schedule-watch.md. The zero Schedule is a real value and
// is delivered like any other.
func (c *clientImpl[Spec, Status]) SchedulesWatch(ctx context.Context, id ObjectID) (<-chan Schedule, error) {
	r, ok := c.bh.reconcilerFor(c.gk)
	if !ok || r.work == nil {
		// The nil queue is unreachable through Register; guarded so this agrees
		// with reconciler.scheduleAt rather than panicking.
		return nil, ErrNoController
	}
	rx, cur := r.work.watchSchedule(id)
	out := make(chan Schedule)

	go func() {
		defer close(out)
		// The receiver holds its key against the hub until closed.
		defer rx.Close()

		// last is what the subscriber has been told; send says whether the
		// value in hand still needs telling. The snapshot always does — the bus
		// does not deliver the seed back.
		last, send := cur.Schedule, true
		for {
			if send && !sendOrDone(ctx, out, last) {
				return // the caller's ctx ended
			}
			ev, err := rx.RecvContext(ctx)
			if err != nil {
				return // the sender closed, or the caller's ctx ended
			}
			// Coalescing only — Accept already rejected superseded values, but
			// the gauge can move away and back while nobody reads, and a
			// repeated value must not reach the consumer.
			next := ev.Value.Schedule
			send, last = next != last, next
		}
	}()
	return out, nil
}
