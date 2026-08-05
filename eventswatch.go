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

// This file is the event watch. Unlike the object watches it did not follow
// the write log: it polls an object's event log and diffs by run id, which is
// sound because an append-only log has no tombstones — a run only appears or
// grows. See docs/TODO.md for the asymmetry that leaves.

// watchPoll returns the poll interval; the fallback covers only a Beehive built
// field by field in a test.
func (bh *Beehive) watchPoll() time.Duration {
	if bh.watchPollInterval <= 0 {
		return defaultWatchPollInterval
	}
	return bh.watchPollInterval
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
	q := resolveEvents(opts).query
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
					// This id belongs to another kind and always will, so there
					// is nothing left to poll for. Ending the driver rather than
					// ticking on it costs one wakeup per interval for as long as
					// the caller holds the stream.
					foreign = true
					return false
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
		if foreign {
			// The stream stays open and silent until the caller lets go, which
			// is the contract; the driver ended early rather than the watch.
			<-ctx.Done()
		}
	}()
	return out, nil
}
