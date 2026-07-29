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

// waker requeues the dependents of everything that has changed. On each tick it
// scans the store's write log from a scan watermark and wakes what it finds. There is
// one per control plane; it is its own type so the cursor it carries between scans
// has an owner. Only the waker goroutine touches these fields, so none needs a lock.
//
// It is an optimisation, not a guarantee. What makes a dependency wake certain is the
// stale-dependents pass, which re-derives staleness from each dependent's durable
// watermark (see Beehive.staleDependentsRun) — so a wake this waker drops costs
// latency until that pass rather than permanent divergence. Read every "the next tick
// retries it" below against that: holding the cursor is how the waker repairs its own
// losses promptly, not the only thing standing between a change and a stranded
// dependent.
//
// The scan is store-wide, not per registered kind: a depends_on edge can point at
// an object of any kind, including one used through Client and never registered.
// Such a target has no reconciler, so a per-kind scan could not name it. Routing is
// unaffected, because it never depended on the scan — dependentsWake queues each
// dependent under its own kind.
type waker struct {
	bh *Beehive

	// watermark is the highest resource_version this waker has processed. The cursor
	// is store-wide, always increasing and never reused, so "everything above this"
	// is exactly what changed since the last scan — however long ago that was, which
	// is what lets a tick do the job of a stream.
	watermark int64

	// seeded says the watermark holds a real cursor. "watermark != 0" cannot say
	// that, because an empty store's cursor really is zero. Seeding at startup keeps
	// the first scan from replaying every object ever written; a failed seed leaves
	// this false so the next tick tries again.
	seeded bool
}

// wakeScanPageCap bounds one scan page. The query itself is an indexed range scan
// and cheap; the cost is round trips, since each page also runs an edges lookup that
// queues behind every writer on the store's single connection.
const wakeScanPageCap = 256

// run drives the waker for the life of the control plane. A non-positive interval
// turns it off, which is a supported choice: the reconcile_owed stamp still covers a
// dependency declared against a target that had already moved, and only a later
// change to a settled dependency goes unnoticed.
func (dw *waker) run(ctx context.Context) {
	// With no registered controllers there is nothing to wake, and the scan is not
	// free — it would run an edges query per change in the whole store only to find
	// there is nowhere to queue the result.
	if len(dw.bh.order) == 0 {
		return
	}
	runDriver(ctx, dw.bh.wakeInterval, func(ctx context.Context) bool {
		dw.scan(ctx)
		return true
	})
}

// seed takes the object write log's current high-water mark as the starting
// watermark, so the first scan reports changes from startup rather than all
// history. It reports whether it got
// one. On failure the waker stays unseeded and the next tick tries again, taking the
// cursor as of *then* — so a change committed in between is below the watermark and is
// never scanned. The reconciler's startup pass covers that only for an object the
// store records as owed; a settled dependent stranded in the window is found by the
// stale-dependents pass instead, which is why this is now a latency gap rather than a
// hole (the startup seed race in TODO.md is the same shape). Retrying is still right:
// an unseeded watermark is zero, and scanning from there would replay every object
// ever written on the strength of a transient error.
func (dw *waker) seed(ctx context.Context) bool {
	cursor, err := dw.bh.store.ObjectWritesMaxVersion(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return false // shutdown, not a loss
		}
		dw.bh.log().WarnContext(ctx, "dependency waker could not read the store's write cursor; retrying on the next tick, and changes made before it are not replayed",
			"err", err)
		return false
	}
	dw.watermark, dw.seeded = cursor, true
	return true
}

// scan runs one pass: everything above the watermark, a page at a time. The cursor
// advances per page, which is sound because pages come back in resource_version
// order, so a page that succeeded really does mean everything below it is done. A
// page that fails leaves the cursor alone, and the next tick re-reads what is still
// owed.
//
// The cost is what changed, not what exists, which is why this can run once a second
// where the passes beside it cannot. A settled dependent is invisible to every
// owed-work listing, because its own generation never moved; re-reading the change
// that stranded it is how this driver finds one, and comparing dependency watermarks
// is how the stale-dependents pass finds the ones this driver missed.
func (dw *waker) scan(ctx context.Context) {
	// Seeding on this tick sets the cursor and nothing more. Everything at or below
	// it is accounted for by definition, so this pass has nothing left to read.
	if !dw.seeded {
		dw.seed(ctx)
		return
	}
	for {
		page, err := dw.bh.store.ObjectWritesListSince(ctx, dw.watermark, wakeScanPageCap)
		if err != nil {
			if ctx.Err() != nil {
				return // shutdown cancelled this read; not a loss of its own
			}
			dw.bh.log().WarnContext(ctx, "scanning for changed dependencies failed; the wakes are still owed and the next tick retries them",
				"watermark", dw.watermark, "err", err)
			return
		}
		if len(page) == 0 {
			return
		}
		if !dw.dependentsWake(ctx, page) {
			return
		}
		if len(page) < wakeScanPageCap {
			// A short page means nothing was above it when the store answered. Stopping
			// here saves the empty query that would otherwise end every scan; anything
			// committed since sits above the watermark and belongs to the next tick.
			return
		}
	}
}

// dependentsWake queues every object that depends_on one of the page's targets, each
// under its own kind, and advances the watermark past the page on success. Waking too
// eagerly is harmless: unregistered kinds are ignored, and the work queue collapses
// duplicates.
//
// It resolves the whole page in one edges query, which is more than an optimization.
// The store runs on a single connection, so every lookup the waker makes queues
// behind every writer in the process — and the waker sees every change in the store,
// not just the registered kinds'. One query per page instead of one per change is
// what stops a write-heavy kind from taxing all of them.
//
// Targets are deduplicated first. One reconcile pass usually writes status, then
// conditions, then the generation handshake: three versions on one row. Without the
// dedup that id would appear several times in the query, and its dependents would be
// walked once per write.
//
// Dependents are addressed by bare id, which both skips the self-edge and routes
// through d.GroupKind(). That works because an ObjectID is unique across kinds —
// objects.id is one AUTOINCREMENT primary key for the whole table. With per-kind ids
// the self-edge check would need the GroupKind too, or it would silently drop
// another object's wake.
func (dw *waker) dependentsWake(ctx context.Context, page []ObjectWrite) bool {
	var high int64
	ids := make([]ObjectID, 0, len(page))
	seen := make(map[ObjectID]struct{}, len(page))
	for _, ref := range page {
		high = max(high, ref.ResourceVersion)
		if _, dup := seen[ref.ID]; dup {
			continue
		}
		seen[ref.ID] = struct{}{}
		ids = append(ids, ref.ID)
	}
	byTarget, err := dw.bh.store.EdgesGroupIncomingByID(ctx, ids, RelationDependsOn)
	if err != nil {
		// Shutdown cancels this same ctx, so a page read just as Stop lands fails
		// here for no reason of its own.
		if ctx.Err() != nil {
			return false
		}
		// Every dependent of these targets just missed their changes, and a dependent
		// that has settled is invisible to every owed-work listing — its own
		// generation never moved. Nothing here can name who was missed: the lookup
		// that failed is exactly the one that would have said. Holding the watermark
		// is what repairs it — the next tick re-reads the same changes.
		dw.bh.log().WarnContext(ctx, "dependents lookup failed; these changes stay above the watermark for the next tick",
			"targetIDs", ids, "err", err)
		return false
	}
	// Resolve each kind's reconciler once per page rather than per dependent.
	// Resolving takes the control plane's mutex, which Register, migratorFor and stop
	// also want, and one page can reach thousands of dependents across a handful of
	// kinds.
	enqueue := dw.bh.enqueuerForPage()
	for _, targetID := range ids {
		for _, d := range byTarget[targetID] {
			if d.ID == targetID {
				// Self-edge: nothing is owed a wake here. A spec write already leaves the
				// object unsettled, and a status or condition write came from this object's
				// own pass, which just ran. Waking it would only re-queue it with nothing
				// left to converge. Cycles of two or more still loop; see TODO.md.
				continue
			}
			enqueue(d.GroupKind(), d.ID)
		}
	}
	dw.watermark = max(dw.watermark, high)
	return true
}
