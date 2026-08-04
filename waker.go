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

	"github.com/amorey/beehive/internal/driver"
)

// waker requeues the dependents of everything that changed: each tick scans the
// store's write log from a watermark and wakes what it finds. One per control
// plane; only the waker goroutine touches its fields.
//
// It is an optimisation, not a guarantee — the stale-dependents pass
// (staleDependentsRun) is what makes a dependency wake certain, so a wake lost
// here costs latency, never divergence.
//
// The scan is store-wide, not per kind: a depends_on edge can point at a
// client-only kind no per-kind query would name.
type waker struct {
	bh *Beehive

	// cursors persists the watermark across restarts when the store supports it;
	// nil means every restart reseeds from ObjectWritesMaxVersionAll.
	cursors DriverCursorer

	// watermark is the highest resource_version this waker has processed. The
	// cursor is store-wide, always increasing and never reused, so "everything
	// above this" is exactly what changed since the last scan.
	watermark int64

	// persisted is what the stored cursor row holds (noStoredCursor when none).
	// Comparing against it keeps a tick from paying a round trip for a write
	// DriverCursorsSet would discard anyway.
	persisted int64

	// persistFailures counts the current streak of failed cursor writes;
	// persistSkips is how many persists to sit out before the next attempt.
	persistFailures int
	persistSkips    int

	// seeded says watermark holds a real cursor. "watermark != 0" cannot say
	// that, because an empty store's cursor really is zero.
	seeded bool
}

// cursorNameWaker is this waker's key in driver_cursors.
const cursorNameWaker = "dependency_waker"

// noStoredCursor marks waker.persisted as "no row yet"; zero is a legitimate
// cursor value, so it cannot double as the sentinel.
const noStoredCursor = int64(-1)

// wakePersistRetryCap bounds the backoff between retries of a failing cursor
// write, in persists sat out — a minute at the default wake interval.
const wakePersistRetryCap = 60

// wakeScanPageCap bounds one scan page. The query is cheap; the cost is round
// trips on the store's single connection.
const wakeScanPageCap = 256

// wakeScanPagesPerTick bounds one tick's scan so resuming after a long gap
// cannot monopolise the single connection. The remainder rides the in-memory
// watermark to the next tick.
const wakeScanPagesPerTick = 16

// run drives the waker for the life of the control plane. A non-positive
// interval turns it off — the reconcile_owed stamp and the stale-dependents
// pass still cover correctness.
func (dw *waker) run(ctx context.Context) {
	// No registered controllers means nowhere to queue anything.
	if len(dw.bh.order) == 0 {
		return
	}
	driver.Run(ctx, dw.bh.wakeInterval, func(ctx context.Context) bool {
		dw.scan(ctx)
		return true
	})
}

// seed sets the starting watermark: the persisted cursor when the store has
// one, otherwise the write log's current high-water mark (so the first scan
// reports changes from startup, not all history). It persists the seed point so
// a run that never sees a write still leaves its successor somewhere to resume.
// On failure the waker stays unseeded and the next tick retries; a change
// committed in that window is below the watermark and is left to the
// stale-dependents pass — a latency gap, not a hole.
func (dw *waker) seed(ctx context.Context) bool {
	mark, err := dw.bh.store.ObjectWritesMaxVersionAll(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return false // shutdown, not a loss
		}
		dw.bh.log().WarnContext(ctx, "dependency waker could not read the store's write cursor; retrying on the next tick, and changes made before it are not replayed",
			"err", err)
		return false
	}

	var stored int64
	var ok bool
	if dw.cursors != nil {
		if stored, ok, err = dw.cursors.DriverCursorsGet(ctx, cursorNameWaker); err != nil {
			if ctx.Err() != nil {
				return false
			}
			dw.bh.log().WarnContext(ctx, "dependency waker could not read its persisted cursor; retrying on the next tick",
				"err", err)
			return false
		}
	}

	watermark := resumeWatermark(stored, ok, mark)

	// persisted tracks the row, not the resume point: after a clamp the row sits
	// above the watermark, and tracking the watermark would retry a doomed write
	// every tick until it climbed past.
	persisted := noStoredCursor
	if ok {
		persisted = stored
	}

	dw.watermark, dw.persisted, dw.seeded = watermark, persisted, true

	// Persist the seed point now: a process that seeds and stops without seeing
	// a write would otherwise leave its successor to skip everything committed
	// in between. No-op when the row already covers the watermark.
	dw.persist(ctx)
	return true
}

// resumeWatermark decides where seed resumes from; pure so the clamp is
// testable. min, not stored: retention trims the log's tail, so the mark can sit
// below a cursor the waker really did process, and that is not evidence of a
// foreign database. Clamping replays from the mark, which is free — the wakes it
// re-derives are idempotent, and the stale-dependents pass is the guarantee
// either way.
func resumeWatermark(stored int64, ok bool, mark int64) int64 {
	if !ok {
		return mark
	}
	return min(stored, mark)
}

// scan runs one pass: everything above the watermark, a page at a time. The
// cursor advances per page — pages come back in resource_version order, so a
// page that succeeded means everything below it is done. A failed page holds
// the cursor and the next tick re-reads it.
func (dw *waker) scan(ctx context.Context) {
	// Seeding on this tick sets the cursor and nothing more.
	if !dw.seeded {
		dw.seed(ctx)
		return
	}
	// A defer, so every early return below — including the error path — still
	// persists whatever earlier pages advanced the watermark to.
	defer dw.persist(ctx)
	for pages := 0; pages < wakeScanPagesPerTick; pages++ {
		page, err := dw.bh.store.ObjectWritesListSinceAll(ctx, dw.watermark, wakeScanPageCap)
		if err != nil {
			if ctx.Err() != nil {
				return // shutdown cancelled this read
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
			// Nothing was above this page when the store answered; anything
			// since belongs to the next tick.
			return
		}
	}
}

// persist writes the watermark through cursors when it has advanced since the
// last successful write. Deliberately a bare statement outside any transaction
// (see docs/adr/2026-07-30-durable-waker-cursor.md). A failed write retries
// with backoff, warning only on the first of a streak; the watermark itself is
// never rolled back — the wakes are already queued.
func (dw *waker) persist(ctx context.Context) {
	// ctx.Err() checked here so a shutdown mid-scan isn't logged as a failure.
	if dw.cursors == nil || dw.watermark <= dw.persisted || ctx.Err() != nil {
		return
	}
	if dw.persistSkips > 0 {
		dw.persistSkips--
		return
	}
	if err := dw.cursors.DriverCursorsSet(ctx, cursorNameWaker, dw.watermark); err != nil {
		dw.persistFailures++
		dw.persistSkips = wakePersistRetrySkips(dw.persistFailures)
		if dw.persistFailures > 1 {
			dw.bh.log().DebugContext(ctx, "persisting the dependency waker's cursor failed again",
				"watermark", dw.watermark, "failures", dw.persistFailures, "err", err)
			return
		}
		dw.bh.log().WarnContext(ctx, "persisting the dependency waker's cursor failed; retries continue with backoff, and a restart before one lands re-scans from the last cursor that was persisted",
			"watermark", dw.watermark, "err", err)
		return
	}
	if dw.persistFailures > 0 {
		dw.bh.log().InfoContext(ctx, "dependency waker's cursor is being persisted again",
			"watermark", dw.watermark, "failures", dw.persistFailures)
		dw.persistFailures, dw.persistSkips = 0, 0
	}
	dw.persisted = dw.watermark
}

// wakePersistRetrySkips is how many persists to sit out after `failures`
// consecutive failures: none after the first, then doubling to a cap.
func wakePersistRetrySkips(failures int) int {
	if failures >= 8 { // 1<<7 - 1 is already past the cap
		return wakePersistRetryCap
	}
	return min(1<<(failures-1)-1, wakePersistRetryCap)
}

// dependentsWake queues every object that depends_on one of the page's targets,
// each under its own kind, and advances the watermark past the page on success.
// One edges query per page, not per change: every lookup queues behind every
// writer on the single connection. Targets are deduplicated first — one pass
// typically writes several versions of one row.
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
		if ctx.Err() != nil {
			return false
		}
		// Nothing can name who was missed — the lookup that failed is the one
		// that would have said. Holding the watermark makes the next tick
		// re-read the same changes.
		dw.bh.log().WarnContext(ctx, "dependents lookup failed; these changes stay above the watermark for the next tick",
			"targetIDs", ids, "err", err)
		return false
	}
	// Resolve each kind's reconciler once per page; one page can reach
	// thousands of dependents.
	enqueue := dw.bh.enqueuerForPage()
	for _, targetID := range ids {
		for _, d := range byTarget[targetID] {
			if d.ID == targetID {
				// Self-edge: nothing is owed. A spec write already leaves the
				// object unsettled, and a status write came from its own pass.
				continue
			}
			enqueue(d.GroupKind(), d.ID)
		}
	}
	dw.watermark = max(dw.watermark, high)
	return true
}

// staleDependentsPageCap bounds one page of the stale-dependents scan.
const staleDependentsPageCap = 256

// cursorNameStaleDependents is the stale-dependents pass's key in driver_cursors.
const cursorNameStaleDependents = "stale_dependents"

// staleDependents is the correctness backstop behind the waker: it finds the
// dependents their targets have moved past, stamps each one, and enqueues it.
// It cannot be disabled.
//
// The scan is bounded by a cursor over target resource versions, so its cost is
// what changed, not the size of the graph. That is only safe because the stamp
// is durable — an enqueue dies with the process.
type staleDependents struct {
	bh      *Beehive
	kinds   []GroupKind
	cursors DriverCursorer // nil when the store cannot persist a cursor

	// cursor is the target version the last completed sweep covered.
	cursor int64
}

func (bh *Beehive) staleDependentsRun(ctx context.Context) {
	if len(bh.order) == 0 {
		return
	}
	// order is frozen after Start, so the kind list is built once.
	kinds := make([]GroupKind, 0, len(bh.order))
	for _, r := range bh.order {
		kinds = append(kinds, r.gk)
	}
	cursors, _ := bh.store.(DriverCursorer)
	sd := &staleDependents{bh: bh, kinds: kinds, cursors: cursors}
	sd.resume(ctx)
	driver.Run(ctx, bh.staleDependentsInterval, func(ctx context.Context) bool {
		sd.sweep(ctx)
		return true
	})
}

// resume reads the position the last completed sweep recorded. Staying at 0 is
// the safe answer: it re-derives the whole graph once.
func (sd *staleDependents) resume(ctx context.Context) {
	if sd.cursors == nil {
		return
	}
	stored, ok, err := sd.cursors.DriverCursorsGet(ctx, cursorNameStaleDependents)
	if err != nil {
		sd.bh.log().WarnContext(ctx, "reading the stale-dependents cursor failed; re-deriving from the start", "err", err)
		return
	}
	if ok {
		sd.cursor = stored
	}
}

// sweep pages the listing to exhaustion, stamping and enqueuing each dependent
// under its own kind. A failed page abandons the sweep and holds the cursor, so
// the next tick reads the same range again.
//
// The stamp is the guarantee and the enqueue is the prompt half. Stamp first, so
// a crash between the two costs a spare reconcile rather than a lost one.
func (sd *staleDependents) sweep(ctx context.Context) {
	log := sd.bh.log()
	// Read the mark before the scan, never after, and scan only up to it. A
	// target written while the sweep runs sits above the mark, so the next sweep
	// finds it — and the scan stays finite under sustained writes. Taking the
	// highest target the scan returned instead would skip exactly those targets.
	mark, err := sd.bh.store.ResourceVersionsMaxIssued(ctx)
	if err != nil {
		if ctx.Err() == nil {
			log.WarnContext(ctx, "reading the resource version failed; the next pass retries", "err", err)
		}
		return
	}
	if sd.cursor > mark {
		// A cursor above the store's own sequence came from another database.
		// Re-derive everything once rather than scan above a point this store
		// never reached.
		sd.cursor = 0
	}
	if sd.cursor == mark {
		return // nothing issued since the last sweep, so nothing can be stale
	}

	abandon := func(msg string, pos StalePos, err error) {
		if ctx.Err() == nil {
			log.WarnContext(ctx, msg, "targetVersion", pos.TargetVersion, "err", err)
		}
	}
	enqueue := sd.bh.enqueuerForPage()
	pos := StalePos{TargetVersion: sd.cursor}
	for {
		page, next, err := sd.bh.store.DependentsListStaleSince(ctx, sd.kinds, pos, mark, staleDependentsPageCap)
		if err != nil {
			abandon("listing stale dependents failed; the next pass resumes from the same cursor", pos, err)
			return
		}
		if len(page) == 0 {
			break
		}
		if err := sd.bh.store.ReconcileOwedStamp(ctx, page); err != nil {
			abandon("stamping stale dependents failed; the next pass resumes from the same cursor", pos, err)
			return
		}
		for _, d := range page {
			enqueue(d.GroupKind(), d.ID)
		}
		pos = next
		if len(page) < staleDependentsPageCap {
			break
		}
	}

	// Only a sweep that reached the end may move the cursor.
	sd.cursor = mark
	if sd.cursors != nil {
		if err := sd.cursors.DriverCursorsSet(ctx, cursorNameStaleDependents, mark); err != nil {
			log.WarnContext(ctx, "persisting the stale-dependents cursor failed; a restart re-derives from the stored one", "err", err)
		}
	}
}
