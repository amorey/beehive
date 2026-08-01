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
	"time"

	"github.com/amorey/beehive/internal/driver"
)

// This file is the client's whole watch surface. The store pushes nothing, so a
// watch polls and diffs: read current state on a tick, compare it with what this
// subscriber was last told, send the difference. So a subscriber sees the same
// level-triggered view as the rest of beehive — it converges on current state and is
// never handed a history.
//
// Two consequences the doc comments repeat, because they are what callers trip on:
//
//   - Intermediate states are invisible. Three writes between two ticks produce one
//     Modified carrying the third, and an object created and deleted inside one
//     interval is never reported at all.
//   - Latency is the poll interval (withWatchPollInterval), not the write.

// watchPoll returns the interval the watches poll at. The option rejects a
// non-positive value, so the fallback is only for a Beehive built field by field in a
// test rather than through New — the same reason Beehive.log guards against nil.
func (bh *Beehive) watchPoll() time.Duration {
	if bh.watchPollInterval <= 0 {
		return defaultWatchPollInterval
	}
	return bh.watchPollInterval
}

// sendOrDone delivers v unless ctx is cancelled first, and reports whether it
// landed. Every send in this file goes through it, so a subscriber that stops reading
// cannot wedge its own poll goroutine past cancellation.
func sendOrDone[V any](ctx context.Context, out chan<- V, v V) bool {
	select {
	case out <- v:
		return true
	case <-ctx.Done():
		return false
	}
}

// pollFailed logs a failed poll and says whether to keep going. A transient store
// error costs one tick, not the stream: ending the stream would leave the subscriber
// receiving nothing, with no way to notice it should resubscribe. A cancelled context
// is shutdown rather than failure, and ends the loop quietly.
func (c *clientImpl[Spec, Status]) pollFailed(ctx context.Context, what string, err error, args ...any) bool {
	if ctx.Err() != nil {
		return false
	}
	c.bh.log().WarnContext(ctx, "beehive: "+what+" poll failed; retrying on the next tick",
		append([]any{"group", c.gk.Group, "kind", c.gk.Kind, "err", err}, args...)...)
	return true
}

// tracked is what an object watch remembers about an object it has reported: the
// version, and the body to build a tombstone from if the row later disappears. obj is
// nil for a row that failed to decode; its version is still tracked, so the failure is
// reported once per change rather than once per tick, but there is nothing to
// tombstone.
type tracked[Spec, Status any] struct {
	rv  int64
	obj *Object[Spec, Status]
}

// ObjectsWatchList streams changes to every object of this client's kind. See the
// Client interface for the contract, and this file's header for what polling
// costs a subscriber.
func (c *clientImpl[Spec, Status]) ObjectsWatchList(ctx context.Context) (<-chan ObjectChange[Spec, Status], error) {
	if !c.bh.isRegistered(c.gk) {
		return nil, fmt.Errorf("beehive: no controller registered for %s/%s", c.gk.Group, c.gk.Kind)
	}
	return c.objectStream(ctx,
		func(ctx context.Context) ([]*RawObject, error) {
			return c.bh.store.ObjectsList(ctx, c.gk)
		},
		func(ctx context.Context) ([]ObjectID, error) {
			return c.bh.store.ObjectsListIDs(ctx, c.gk)
		})
}

// ObjectsWatch streams changes to the single object id. It polls the same way
// ObjectsWatchList does, over a one-row listing: a missing id is an empty listing,
// so an id that does not exist yet streams nothing until it is created, and its
// removal reads as a Deleted.
func (c *clientImpl[Spec, Status]) ObjectsWatch(ctx context.Context, id ObjectID) (<-chan ObjectChange[Spec, Status], error) {
	if !c.bh.isRegistered(c.gk) {
		return nil, fmt.Errorf("beehive: no controller registered for %s/%s", c.gk.Group, c.gk.Kind)
	}
	return c.objectStream(ctx,
		func(ctx context.Context) ([]*RawObject, error) {
			raw, err := c.scopedRow(ctx, id, c.bh.store.ObjectsGet)
			if raw == nil || err != nil {
				return nil, err
			}
			return []*RawObject{raw}, nil
		},
		// The liveness probe is scoped to this one id rather than listing the kind:
		// this stream can only ever have that id in seen, so asking for every id of
		// the kind would make the cheap half of the poll the expensive half, and scale
		// it with the kind rather than with the watch.
		func(ctx context.Context) ([]ObjectID, error) {
			raw, err := c.scopedRow(ctx, id, c.bh.store.ObjectsGetMeta)
			if raw == nil || err != nil {
				return nil, err
			}
			return []ObjectID{raw.ID}, nil
		})
}

// scopedRow reads id through read and folds both "not visible through this kind's
// client" cases into (nil, nil): a missing id, so an id that does not exist yet
// streams nothing until it is created, and a foreign one, as invisible here as it
// is through Get. read is a parameter so the same fold serves the blob-bearing
// watch read and the blob-free liveness probe.
func (c *clientImpl[Spec, Status]) scopedRow(
	ctx context.Context,
	id ObjectID,
	read func(context.Context, ObjectID) (*RawObject, error),
) (*RawObject, error) {
	raw, err := read(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if raw.Group != c.gk.Group || raw.Kind != c.gk.Kind {
		return nil, nil
	}
	return raw, nil
}

// objectStream is the poll-and-diff engine behind both object watches.
//
// **A returned stream always carries a snapshot**, and that is what makes
// "subscribe, then act" safe: everything the caller does next lands above state
// this stream already holds, so a change it makes — including a delete — is
// reported. Taking the snapshot on the first tick instead would leave the window
// between subscribing and that tick invisible, and an object created and collected
// inside it is never reported at all, so a caller waiting on its Deleted waits
// forever. A failed first read therefore returns an error instead of a stream: the
// alternative is handing back a watch whose guarantee is quietly void.
//
// It remembers the resource_version of every object it has reported and works out
// the change type by comparison:
//
//   - absent before, present now -> Added
//   - present in both, version moved -> Modified
//   - present before, absent now -> Deleted
//
// A version that has not moved sends nothing, which is what keeps the steady state
// silent instead of re-sending the world every tick.
//
// Most ticks skip the listing entirely. resource_version is a store-wide cursor, so a
// cursor that has not moved proves no row was created or modified anywhere. The one
// thing it cannot show is a delete, since a removed row draws no version. So a tick
// reads the scalar cursor, and pays for the listing that carries specs and statuses
// only when that moved or the ids it tracks have shrunk. In a quiet system a
// subscriber costs one scalar read plus one blob-free liveness read per tick.
//
// The cursor is the object write log's high-water mark, not the version counter
// behind it, and the difference decides whether this optimization survives contact
// with a real workload: the counter is shared with the event log, so a single
// EventsAdd anywhere in the store would move it while touching no objects row —
// and a controller that records an event per reconcile, the shape the events example
// encourages, would defeat it permanently.
//
// A Deleted change carries the object's last known state, because the row is gone by
// the time the poll notices and there is nothing left to read. That body can be one
// interval stale, but it is the object's final state either way — nothing was written
// after it. Keeping those bodies is what a list watch costs in memory: the decoded
// state of the kind, for as long as the stream runs.
//
// A row that fails to decode is quarantined the same way List does it, so one bad
// object cannot kill the stream.
func (c *clientImpl[Spec, Status]) objectStream(
	ctx context.Context,
	list func(context.Context) ([]*RawObject, error),
	live func(context.Context) ([]ObjectID, error),
) (<-chan ObjectChange[Spec, Status], error) {
	// The migrator is invariant for the stream's lifetime; resolve it once rather
	// than re-locking the registry on every poll.
	mig := c.bh.migratorFor(c.gk)
	seen := make(map[ObjectID]tracked[Spec, Status])
	var cursor int64

	// The snapshot, on the caller's goroutine. Its failure is returned rather than
	// logged, because it is the one poll whose failure the caller can act on and the
	// only one that leaves nothing behind: a stream handed back after a failed first
	// read would carry no state to compare against, so an object deleted next would
	// never be reported — the exact hole the guarantee above closes. Every later
	// failure costs one tick, since the state from the last good poll is still there.
	initial, err := c.poll(ctx, list, live, mig, seen, &cursor)
	if err != nil {
		return nil, fmt.Errorf("beehive: watch on %s/%s: initial read failed: %w",
			c.gk.Group, c.gk.Kind, err)
	}

	out := make(chan ObjectChange[Spec, Status])
	go func() {
		defer close(out)
		// The snapshot's changes are what the driver's eager first step delivers, so
		// that step spends itself handing over a read that has already happened rather
		// than repeating it, and polling begins on the tick after. Sending them here
		// rather than above is not a choice: a send blocks until the subscriber reads,
		// and it cannot read until objectStream has returned.
		pending, delivered := initial, false
		driver.Run(ctx, c.bh.watchPoll(), func(ctx context.Context) bool {
			if delivered {
				var err error
				pending, err = c.poll(ctx, list, live, mig, seen, &cursor)
				if err != nil {
					return c.pollFailed(ctx, "watch", err)
				}
			}
			delivered = true
			for _, ch := range pending {
				if !sendOrDone(ctx, out, ch) {
					return false
				}
			}
			pending = nil // the snapshot's objects are not held past their delivery
			return true
		})
	}()
	return out, nil
}

// poll runs one object-watch tick: it reads current state, folds it into seen, and
// returns the changes to send. The error is returned rather than handled, because
// the same read means different things to its two callers — fatal for the snapshot,
// one lost tick for the loop.
//
// Deriving every change before sending any is what lets the snapshot run on the
// caller's goroutine: the sends block on a subscriber that cannot read until
// subscribing returns, so the two must not be interleaved.
func (c *clientImpl[Spec, Status]) poll(
	ctx context.Context,
	list func(context.Context) ([]*RawObject, error),
	live func(context.Context) ([]ObjectID, error),
	mig Migrator,
	seen map[ObjectID]tracked[Spec, Status],
	cursor *int64,
) ([]ObjectChange[Spec, Status], error) {
	at, err := c.bh.store.ObjectWritesMaxVersion(ctx)
	if err != nil {
		return nil, err
	}
	if at == *cursor {
		// Nothing was created or modified. A delete is still possible and draws no
		// version, so check that what this stream tracks is still there — one
		// blob-free read — and skip the expensive listing unless something actually
		// vanished.
		gone, err := c.deletedSince(ctx, seen, live)
		if err != nil {
			return nil, err
		}
		if !gone {
			return nil, nil
		}
	}
	raws, err := list(ctx)
	if err != nil {
		return nil, err
	}
	*cursor = at

	var changes []ObjectChange[Spec, Status]
	present := make(map[ObjectID]struct{}, len(raws))
	for _, raw := range raws {
		present[raw.ID] = struct{}{}
		if prev, known := seen[raw.ID]; known && prev.rv == raw.ResourceVersion {
			continue // unchanged since the last report
		}
		obj, err := rawToTyped[Spec, Status](raw, mig)
		if err != nil {
			// Quarantine, don't tear down: skip a poison row and keep the stream alive
			// so one un-decodable object can't silently kill a live watcher (mirrors
			// List). Recording the version keeps it from re-warning every tick until
			// someone rewrites it.
			seen[raw.ID] = tracked[Spec, Status]{rv: raw.ResourceVersion}
			c.warnUndecodable("Watch", raw.ID, err)
			continue
		}
		typ := Modified
		if _, known := seen[raw.ID]; !known {
			typ = Added
		}
		seen[raw.ID] = tracked[Spec, Status]{rv: raw.ResourceVersion, obj: obj}
		changes = append(changes, ObjectChange[Spec, Status]{Type: typ, Object: obj})
	}
	for id, prev := range seen {
		if _, ok := present[id]; ok {
			continue
		}
		delete(seen, id)
		if prev.obj == nil {
			continue // only ever seen as a poison row: no body to tombstone
		}
		changes = append(changes, ObjectChange[Spec, Status]{Type: Deleted, Object: prev.obj})
	}
	return changes, nil
}

// deletedSince reports whether any object this stream has reported is gone. It is
// the cheap half of the poll: live is a blob-free read scoped to what the stream
// tracks — the kind's id column for a list watch, one row for a single-object one —
// where the listing it gates carries every row's spec and status. Only ever
// consulted when no object write has landed, so a create cannot be in flight and a
// shrunk id set means a delete.
func (c *clientImpl[Spec, Status]) deletedSince(
	ctx context.Context,
	seen map[ObjectID]tracked[Spec, Status],
	live func(context.Context) ([]ObjectID, error),
) (bool, error) {
	if len(seen) == 0 {
		return false, nil
	}
	ids, err := live(ctx)
	if err != nil {
		return false, err
	}
	stillThere := make(map[ObjectID]struct{}, len(ids))
	for _, id := range ids {
		stillThere[id] = struct{}{}
	}
	for id := range seen {
		if _, ok := stillThere[id]; !ok {
			return true, nil
		}
	}
	return false, nil
}

// EventsWatch streams id's event log. See the Client interface for the contract.
//
// Runs are keyed by their own id and compared by resource_version, so a run extended
// between ticks — EventsAdd bumping its count — is sent again with its new state,
// the same convergence the object watches give. There are no tombstones, because an
// append-only log means a run only appears or grows. A run that both appears and is
// trimmed by retention inside one interval is never seen.
//
// A quiet tick skips the listing, the way the object watches skip theirs: it reads
// EventsMaxVersion — one scalar over this object's log — and lists only when that
// moved. There is no liveness half here, because there is nothing this watch could
// do with a disappearance: it reports no tombstones, so a run trimmed by retention
// is not an event, and a version that has not moved means there is nothing to send.
func (c *clientImpl[Spec, Status]) EventsWatch(ctx context.Context, id ObjectID, opts ...EventOption) (<-chan Event, error) {
	if !c.bh.isRegistered(c.gk) {
		return nil, fmt.Errorf("beehive: no controller registered for %s/%s", c.gk.Group, c.gk.Kind)
	}
	q := resolveEvents(opts)
	out := make(chan Event)
	seen := make(map[EventID]int64)
	// The log's high-water mark as of the last listing. Zero before the first one,
	// and an object with no runs reads as 0 too, so a log that is still empty never
	// pays for a listing at all — there is nothing in it to report.
	var cursor int64
	// scoped and foreign latch the kind check below, in both directions. An object's
	// group and kind are fixed at insert and its id is never reused, so once the id
	// resolves the answer cannot change while the stream runs — checking it once keeps
	// the steady-state cost at one query per tick rather than two, and a foreign id at
	// none. Only "not found yet" stays unlatched: ids are assigned on insert, so an id
	// that does not exist can still be created later, as any kind.
	var scoped, foreign bool

	go func() {
		defer close(out)
		driver.Run(ctx, c.bh.watchPoll(), func(ctx context.Context) bool {
			if foreign {
				// Latched: this id belongs to another kind and always will, so there is
				// nothing to re-read. The stream stays open and silent, which is the
				// contract — closing it would tell the subscriber something different.
				return true
			}
			if !scoped {
				// Kind-scope the read: the object watches are scoped, and an unscoped log
				// read would let a foreign id stream another kind's events through this
				// client. A missing id simply streams nothing until it exists.
				raw, err := c.bh.store.ObjectsGetMeta(ctx, id)
				if errors.Is(err, ErrNotFound) {
					return true
				}
				if err != nil {
					return c.pollFailed(ctx, "event watch", err, "id", id)
				}
				if raw.Group != c.gk.Group || raw.Kind != c.gk.Kind {
					foreign = true // nothing of this kind's to stream, ever
					return true
				}
				scoped = true
			}
			at, err := c.bh.store.EventsMaxVersion(ctx, id)
			if err != nil {
				return c.pollFailed(ctx, "event watch", err, "id", id)
			}
			if at == cursor {
				// The log has not moved, so no run was added or extended and there is
				// nothing this watch reports. Unlike the object watches there is no
				// second read to make: a run that vanished is retention, not a change.
				return true
			}
			runs, err := c.bh.store.EventsList(ctx, id, q)
			if err != nil {
				return c.pollFailed(ctx, "event watch", err, "id", id)
			}
			// Only advanced past a listing that succeeded, so a failed one is retried
			// on the next tick rather than skipped.
			cursor = at
			// Rebuilt per listing rather than added to, so the map is bounded by what
			// the query returns instead of by every run the stream has ever seen:
			// retention physically deletes runs, and their ids would otherwise be held
			// for the life of the stream. A run that leaves the window and comes back
			// must have been extended to get there, so its version moved and it is owed
			// a delivery anyway.
			//
			// The gate above means a quiet tick does not rebuild, so a run trimmed
			// while the log is quiet keeps its id here until the next real event. That
			// is bounded by the runs this stream has seen, and the next event clears
			// it — trading a quiet tick's listing for it is the point.
			next := make(map[EventID]int64, len(runs))
			// EventsList is newest-first; deliver oldest-first so the timeline builds
			// in order, as a reader of an append-only log expects.
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

// SchedulesWatch streams id's schedule as a gauge. It requires a registered
// controller (the reconcile loop that owns the work queue); a client-only kind has
// none, so it returns ErrNoController. See the Client interface for the full
// contract.
//
// Unlike every other watch in beehive this one does not poll. The work queue
// publishes each move of the schedule to a hub and this stream reads its own
// receiver, so there is no tick and no backstop behind it.
//
// That is sound here and nowhere else. The queue is unexported and process-local,
// so no second process and no embedder can move the gauge: the hub sees every
// writer that exists, which no store-backed hub can say. And the gauge reports
// each move from one type, so a queue operation cannot change the schedule
// silently. Give workQueue a second writer and both halves fail — the poll would
// have to come back. See docs/adr/2026-07-27-schedule-watch.md.
//
// The zero Schedule ("nothing scheduled") is a real value rather than an absence,
// so it is delivered like any other.
func (c *clientImpl[Spec, Status]) SchedulesWatch(ctx context.Context, id ObjectID) (<-chan Schedule, error) {
	r, ok := c.bh.reconcilerFor(c.gk)
	if !ok {
		return nil, ErrNoController
	}
	rx, cur := r.work.watchSchedule(id)
	out := make(chan Schedule)

	go func() {
		defer close(out)
		// The receiver holds its key against the hub until it is closed, whatever
		// ends the stream.
		defer rx.Close()

		// last is what the subscriber has been told; send says whether the value in
		// hand still needs telling. The snapshot always does — the bus does not
		// deliver the seed back, since it is the caller's own argument — and every
		// value after it does unless it repeats what was already reported.
		last, send := cur.Schedule, true
		for {
			if send && !sendOrDone(ctx, out, last) {
				return // the caller's ctx ended
			}
			ev, err := rx.RecvContext(ctx)
			if err != nil {
				return // the sender closed, or the caller's ctx ended
			}
			// No staleness check: Accept rejected every value the queue
			// superseded. This comparison is for coalescing only — the gauge can
			// move away and back while nobody reads, and a repeated value must not
			// reach the consumer.
			next := ev.Value.Schedule
			send, last = next != last, next
		}
	}()
	return out, nil
}
