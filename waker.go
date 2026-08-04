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
	"time"

	"github.com/amorey/beehive/internal/driver"
	"github.com/amorey/beehive/internal/rategate"
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

	// now is the waker's only clock, so the rate tests drive it by hand.
	now func() time.Time

	// persistGate floors the cursor write at the wake interval. Without it a
	// wake-driven pass would write the cursor at the wake rate, on the one
	// connection every commit needs.
	persistGate *rategate.Gate[struct{}]

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

// gateKey is the single key every waker rate gate holds: the limits are
// per waker, not per object.
var gateKey = struct{}{}

// cursorNameWaker is this waker's key in driver_cursors.
const cursorNameWaker = "dependency_waker"

// noStoredCursor marks waker.persisted as "no row yet"; zero is a legitimate
// cursor value, so it cannot double as the sentinel.
const noStoredCursor = int64(-1)

// wakePersistRetryCap bounds the backoff between retries of a failing cursor
// write, in persists sat out — a minute at the default wake interval. It reads
// as seconds only because persistGate floors a persist *attempt* at that
// interval, which is why the gate is consulted before the skip ladder.
const wakePersistRetryCap = 60

// wakeScanPageCap bounds one scan page. The query is cheap; the cost is round
// trips on the store's single connection.
const wakeScanPageCap = 256

// wakeScanPagesPerPass bounds one pass's scan so resuming after a long gap
// cannot monopolise the single connection. The remainder rides the in-memory
// watermark to the next pass.
const wakeScanPagesPerPass = 16

// run drives the waker for the life of the control plane. A non-positive
// interval turns it off — the reconcile_owed stamp and the stale-dependents
// pass still cover correctness.
func (dw *waker) run(ctx context.Context) {
	// No registered controllers means nowhere to queue anything.
	if len(dw.bh.order) == 0 {
		return
	}
	dw.arm()
	driver.Run(ctx, dw.bh.wakeInterval, func(ctx context.Context) bool {
		dw.scan(ctx)
		return true
	})
}

// arm builds what the waker reads from its Beehive's options, once, on first
// use. Never in New: New constructs the waker before it applies options, so
// anything built there captures the defaults and silently ignores the option.
func (dw *waker) arm() {
	if dw.persistGate != nil {
		return
	}
	if dw.now == nil {
		dw.now = time.Now
	}
	dw.persistGate = rategate.New[struct{}](dw.bh.wakeInterval)
}

// scanResult says what a pass found. The run loop dispatches on it: how soon to
// look again, and whether to drop the wakes arriving meanwhile.
type scanResult uint8

const (
	scanIdle   scanResult = iota // nothing above the watermark
	scanMore                     // work remains: the page budget stopped the scan, or a seed resumed below the mark
	scanFailed                   // a read failed, so the watermark is held for the next pass
)

// seed sets the starting watermark: the persisted cursor when the store has
// one, otherwise the write log's current high-water mark (so the first scan
// reports changes from startup, not all history). It persists the seed point so
// a run that never sees a write still leaves its successor somewhere to resume.
// On failure the waker stays unseeded and the next pass retries; a change
// committed in that window is below the watermark and is left to the
// stale-dependents pass — a latency gap, not a hole.
func (dw *waker) seed(ctx context.Context) scanResult {
	mark, err := dw.bh.store.ObjectWritesMaxVersionAll(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return scanFailed // shutdown, not a loss
		}
		dw.bh.log().WarnContext(ctx, "dependency waker could not read the store's write cursor; retrying on the next pass, and changes made before it are not replayed",
			"err", err)
		return scanFailed
	}

	var stored int64
	var ok bool
	if dw.cursors != nil {
		if stored, ok, err = dw.cursors.DriverCursorsGet(ctx, cursorNameWaker); err != nil {
			if ctx.Err() != nil {
				return scanFailed
			}
			dw.bh.log().WarnContext(ctx, "dependency waker could not read its persisted cursor; retrying on the next pass",
				"err", err)
			return scanFailed
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

	// A resume below the mark has a backlog waiting, and making the loop wait a
	// floor for its first page is the one case where a restart is slower than a
	// steady state.
	if watermark < mark {
		return scanMore
	}
	return scanIdle
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
// the cursor and the next pass re-reads it.
func (dw *waker) scan(ctx context.Context) scanResult {
	// Seeding on this pass sets the cursor and nothing more, which is what
	// keeps a wake from turning a failed seed into a scan.
	if !dw.seeded {
		return dw.seed(ctx)
	}
	// A defer, so every early return below — including the error path — still
	// persists whatever earlier pages advanced the watermark to.
	defer dw.persist(ctx)
	for pages := 0; pages < wakeScanPagesPerPass; pages++ {
		page, err := dw.bh.store.ObjectWritesListSinceAll(ctx, dw.watermark, wakeScanPageCap)
		if err != nil {
			if ctx.Err() != nil {
				return scanFailed // shutdown cancelled this read
			}
			dw.bh.log().WarnContext(ctx, "scanning for changed dependencies failed; the wakes are still owed and the next pass retries them",
				"watermark", dw.watermark, "err", err)
			return scanFailed
		}
		if len(page) == 0 {
			return scanIdle
		}
		if !dw.dependentsWake(ctx, page) {
			return scanFailed
		}
		if len(page) < wakeScanPageCap {
			// Nothing was above this page when the store answered; anything
			// since belongs to the next pass.
			return scanIdle
		}
	}
	return scanMore
}

// persist writes the watermark through cursors when it has advanced since the
// last successful write. Deliberately a bare statement outside any transaction
// (see docs/adr/2026-07-30-durable-waker-cursor.md). A failed write retries
// with backoff, warning only on the first of a streak; the watermark itself is
// never rolled back — the wakes are already queued.
func (dw *waker) persist(ctx context.Context) {
	dw.arm()
	// ctx.Err() checked here so a shutdown mid-scan isn't logged as a failure.
	if dw.cursors == nil || dw.watermark <= dw.persisted || ctx.Err() != nil {
		return
	}
	// Ahead of the skip ladder, not just ahead of the write: a gate below it
	// would let refused passes burn skips at the wake rate, which is what turns
	// wakePersistRetryCap from a minute into a handful of seconds.
	if _, held := dw.persistGate.Allow(gateKey, dw.now()); held {
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

// staleDependents is the correctness backstop behind the waker: it finds the
// dependents their targets have moved past, stamps each one, and enqueues it.
// It cannot be disabled.
//
// The scan is bounded by a cursor over target resource versions, so its cost is
// what changed, not the size of the graph.
//
// The cursor is process-local, and deliberately not persisted: a finding whose
// enqueue was lost in memory has no other repair, and starting every process at
// 0 guarantees one — a crash is a restart. A lost watermark write needs no such
// repair, because it leaves the watermark low and low only over-reports
// staleness. See docs/adr/2026-08-03-stale-dependents-cursor.md.
type staleDependents struct {
	bh    *Beehive
	kinds []GroupKind

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
	sd := &staleDependents{bh: bh, kinds: kinds}
	driver.Run(ctx, bh.staleDependentsInterval, func(ctx context.Context) bool {
		sd.sweep(ctx)
		return true
	})
}

// staleResumeAt turns the cursor — the version the last completed sweep consumed
// through — into a scan position.
//
// The next version from the start, not (consumed, 0, 0). Ids are positive, so
// that position still matches every target at the consumed version: a target
// there whose dependents are still stale would have its whole fan-out listed and
// stamped again on every sweep. Tuple paging is for resuming inside one sweep,
// where the position is a row the scan actually returned.
func staleResumeAt(consumed int64) StalePos {
	return StalePos{TargetVersion: consumed + 1}
}

// sweep pages the listing to exhaustion, stamping and enqueuing each dependent
// under its own kind. A failed page abandons the sweep and holds the cursor, so
// the next tick reads the same range again.
//
// Stamp before enqueuing, so a finding outlives the queue and a crash between
// the two costs a spare reconcile rather than a lost one.
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
	if sd.cursor == mark {
		return // nothing issued since the last sweep, so nothing can be stale
	}

	abandon := func(msg string, pos StalePos, err error) {
		if ctx.Err() == nil {
			log.WarnContext(ctx, msg, "targetVersion", pos.TargetVersion, "err", err)
		}
	}
	enqueue := sd.bh.enqueuerForPage()
	pos := staleResumeAt(sd.cursor)
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
}
