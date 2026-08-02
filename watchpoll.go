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

// tracked is what an object watch remembers about an object it has reported:
// the version, and the body to build a tombstone from. obj is nil for a row
// that failed to decode — the version is still tracked so the failure is
// reported once per change, not once per tick.
type tracked[Spec, Status any] struct {
	rv  int64
	obj *Object[Spec, Status]
}

// ObjectsWatchList streams changes to every object of this client's kind. See
// the Client interface for the contract.
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

// ObjectsWatch streams changes to the single object id, polling a one-row
// listing: an id that does not exist yet streams nothing until created, and its
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
		// The liveness probe reads this one id, not the kind, so it scales with
		// the watch rather than the kind.
		func(ctx context.Context) ([]ObjectID, error) {
			raw, err := c.scopedRow(ctx, id, c.bh.store.ObjectsGetMeta)
			if raw == nil || err != nil {
				return nil, err
			}
			return []ObjectID{raw.ID}, nil
		})
}

// scopedRow reads id through read, folding both "not visible through this
// kind's client" cases — missing and foreign — into (nil, nil).
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
// A returned stream always carries a snapshot, which is what makes "subscribe,
// then act" safe: a change the caller makes next — including a delete — lands
// above state the stream already holds. A failed first read therefore returns
// an error instead of a stream whose guarantee is quietly void.
//
// Change types come from comparing remembered resource_versions: absent→present
// is Added, version moved is Modified, present→absent is Deleted (carrying the
// last known body, since the row is gone). An unmoved version sends nothing.
//
// Most ticks skip the listing: an unmoved store-wide cursor proves no row was
// created or modified, so a quiet tick costs one scalar read plus one blob-free
// liveness read for deletes (which draw no version). The cursor is the object
// write log's high-water mark, NOT the shared version counter — the counter
// moves for event writes too, which would defeat the optimization permanently
// for any controller that records an event per reconcile.
//
// A row that fails to decode is quarantined as List does it.
func (c *clientImpl[Spec, Status]) objectStream(
	ctx context.Context,
	list func(context.Context) ([]*RawObject, error),
	live func(context.Context) ([]ObjectID, error),
) (<-chan ObjectChange[Spec, Status], error) {
	// The migrator is invariant for the stream's lifetime.
	mig := c.bh.migratorFor(c.gk)
	seen := make(map[ObjectID]tracked[Spec, Status])
	var cursor int64

	// The snapshot, on the caller's goroutine; its failure is the one the
	// caller can act on. Every later failure costs one tick.
	initial, err := c.poll(ctx, list, live, mig, seen, &cursor)
	if err != nil {
		return nil, fmt.Errorf("beehive: watch on %s/%s: initial read failed: %w",
			c.gk.Group, c.gk.Kind, err)
	}

	out := make(chan ObjectChange[Spec, Status])
	go func() {
		defer close(out)
		// The driver's eager first step delivers the snapshot instead of
		// repeating the read; sending must happen here because a send blocks
		// until the subscriber reads, which it can't until objectStream returns.
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
			pending = nil // don't hold the snapshot's objects past delivery
			return true
		})
	}()
	return out, nil
}

// poll runs one object-watch tick: read current state, fold it into seen,
// return the changes to send. Deriving every change before sending any is what
// lets the snapshot run on the caller's goroutine.
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
		// Nothing created or modified; a delete is still possible and draws no
		// version, so check liveness cheaply and skip the listing unless
		// something vanished.
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
			// Quarantine: one bad row must not kill a live watcher. Recording
			// the version keeps it from re-warning every tick.
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

// deletedSince reports whether any object this stream has reported is gone. It
// is only consulted when no object write has landed, so a create cannot be in
// flight and a shrunk id set means a delete.
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
