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

// This file is the client's watch surface. The object watches subscribe to
// their kind's shared tail (watchtail.go), which a commit wakes; the event
// watch still polls and diffs — read current state on a tick, compare with
// what this subscriber was last told, send the difference. Both are
// level-triggered: subscribers get the latest state per object, never the
// intermediate states.

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
// returned snapshot holds no objects and carries rv back. Fails with
// ErrWatchTooOld when retention has already removed entries above rv, which the
// caller answers by subscribing again without this option.
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

// watchPoll returns the poll interval; the fallback covers only a Beehive built
// field by field in a test.
func (bh *Beehive) watchPoll() time.Duration {
	if bh.watchPollInterval <= 0 {
		return defaultWatchPollInterval
	}
	return bh.watchPollInterval
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

// resumable reports whether a stream may start above rv, reading the horizon the
// tail's own listing reports.
func (c *clientImpl[Spec, Status]) resumable(ctx context.Context, rv int64) error {
	_, trimmedThrough, err := c.bh.store.ObjectWritesListSince(ctx, c.gk, rv, 1)
	if err != nil {
		return fmt.Errorf("beehive: watch on %s/%s: resume check failed: %w",
			c.gk.Group, c.gk.Kind, err)
	}
	return horizonErr(c.gk, "the resume", rv, trimmedThrough)
}

// snapshot reads the watch's starting state: one object, or the whole kind.
func (c *clientImpl[Spec, Status]) snapshot(ctx context.Context, only *ObjectID) ([]*RawObject, int64, error) {
	if only != nil {
		return c.bh.store.ObjectWritesSnapshotByID(ctx, c.gk, *only)
	}
	return c.bh.store.ObjectWritesSnapshot(ctx, c.gk)
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
