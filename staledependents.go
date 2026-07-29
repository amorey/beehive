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

import "context"

// staleDependentsPageCap bounds one page of the stale-dependents scan. The query
// is bounded by the depends_on edges of registered kinds and cheap; the cost is
// round trips on the store's single connection, which every writer shares.
const staleDependentsPageCap = 256

// staleDependentsRun is the correctness backstop behind the dependency waker. The
// waker is fast and in-memory, so a wake it loses — to a crash, to a process with
// no controllers, to a bug — is lost for good; here the same question is asked of
// current state instead, by comparing each dependent's durable watermark against
// its targets' resource_version. A wake lost by *any* means, including means that
// do not exist yet, costs latency until this pass rather than permanent divergence.
//
// That is why it cannot be disabled and why it is store-derived rather than
// recorded: there is no bookkeeping to keep in sync, and re-deriving twice is
// re-deriving once.
func (bh *Beehive) staleDependentsRun(ctx context.Context) {
	// With no registered controllers there is nothing to enqueue, and the kind list
	// the scan filters on would be empty anyway — the same early return the waker makes.
	if len(bh.order) == 0 {
		return
	}
	// order is frozen after Start, so the kind list is built once rather than per pass.
	kinds := make([]GroupKind, 0, len(bh.order))
	for _, r := range bh.order {
		kinds = append(kinds, r.gk)
	}
	runDriver(ctx, bh.staleDependentsInterval, func(ctx context.Context) bool {
		bh.staleDependentsSweep(ctx, kinds)
		return true
	})
}

// staleDependentsSweep pages the staleness listing to exhaustion and enqueues
// each dependent under its own kind. It pages to exhaustion rather than one page
// per tick, as the waker's scan does, so the startup step enqueues everything
// stale rather than a slice of it — which matters most on the first start after
// this mechanism lands, when no object has a watermark yet and the whole
// dependency graph is stale at once. That herd is one-time and self-extinguishing:
// each pass records a watermark.
//
// A failed page is logged and the sweep abandoned. The cursor is local to the
// sweep and nothing was drained, so the next tick re-derives the same set — there
// is no state to hold or repair.
func (bh *Beehive) staleDependentsSweep(ctx context.Context, kinds []GroupKind) {
	// Resolve each kind's reconciler once for the whole sweep rather than per
	// dependent: resolving takes the mutex Register, migratorFor and stop also want,
	// and one sweep can reach many dependents across a handful of kinds. The cache may
	// outlive a page safely — the registration set is frozen after Start (see
	// enqueuerForPage).
	enqueue := bh.enqueuerForPage()
	var after ObjectID
	for {
		page, err := bh.store.DependentsListStale(ctx, kinds, after, staleDependentsPageCap)
		if err != nil {
			if ctx.Err() != nil {
				return // shutdown cancelled this read; not a loss of its own
			}
			bh.log().WarnContext(ctx, "listing stale dependents failed; the next pass re-derives them",
				"afterID", after, "err", err)
			return
		}
		if len(page) == 0 {
			return
		}
		for _, d := range page {
			enqueue(d.GroupKind(), d.ID)
		}
		after = page[len(page)-1].ID
		if len(page) < staleDependentsPageCap {
			// A short page means nothing was above it when the store answered. Anything
			// that has gone stale since belongs to the next tick.
			return
		}
	}
}
