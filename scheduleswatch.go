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
)

// WatchSchedule streams id's schedule as a gauge; a client-only kind returns
// ErrNoController. The one watch that does not poll: the work queue publishes
// each move to a hub and this stream reads its own receiver. That is sound only
// because the queue is unexported and process-local (the hub sees every writer
// that exists) and the gauge reports every move from one type — give workQueue
// a second writer and the poll has to come back. See
// docs/adr/2026-07-27-schedule-watch.md. The zero Schedule is a real value and
// is delivered like any other.
func (c *clientImpl[Spec, Status]) WatchSchedule(ctx context.Context, id ObjectID) (<-chan Schedule, error) {
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
