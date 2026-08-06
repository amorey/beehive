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
	"github.com/amorey/gobus"
	"github.com/amorey/gobus/watch"
)

// waker requeues the dependents of everything that changed: each commit wakes a
// scan of the store's write log from a watermark. One per control plane; only
// prime and the waker goroutine touch its fields, and prime runs before that
// goroutine exists.
//
// It is an optimisation, not a guarantee — the stale-dependents pass
// (staleDependentsRun) is what makes a dependency wake certain, so a wake lost
// here costs latency, never divergence. A drain that pass has overtaken is handed
// to it outright; see abandonAfter.
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

	// retry is the delay after a failed scan, and the loop's only timer.
	retry driver.Backoff

	// persistGate floors the cursor write. Without it a wake-driven pass would
	// write the cursor at the wake rate, on the one connection every commit
	// needs.
	persistGate *rategate.Single

	// scanGate floors wake-driven scans, so a sustained write stream cannot
	// hold the connection at the full paging budget. Eager after a quiet
	// period, so an idle-to-active transition adds no latency.
	scanGate *rategate.Single

	// watermark is the highest resource_version this waker has processed. The
	// cursor is store-wide, always increasing and never reused, so "everything
	// above this" is exactly what changed since the last scan.
	watermark int64

	// persisted is what the stored cursor row holds (noStoredCursor when none).
	// Comparing against it keeps a pass from paying a round trip for a write
	// DriverCursorsSet would discard anyway.
	persisted int64

	// persistFailures counts the current streak of failed cursor writes, so the
	// log carries one line per cause; persistRetry paces the attempts and
	// persistOpensAt is when the next one is worth making.
	persistFailures int
	persistRetry    driver.Backoff
	persistOpensAt  time.Time

	// seeded says watermark holds a real cursor. "watermark != 0" cannot say
	// that, because an empty store's cursor really is zero.
	seeded bool

	// primed is what the seed in prime found, and what run's first turn starts
	// from.
	primed scanResult

	// rx carries the commit wakes; nil when the Beehive was assembled without a
	// hub.
	rx *watch.Receiver[GroupKind, struct{}]

	// drainSince is when the current run of budget-exhausting passes began; zero
	// when no drain is running. Only paging counts, so a gate refusal is not a
	// drain.
	drainSince time.Time

	// abandonAfter is how long a drain may run before the stale-dependents pass
	// has found everything it is still working toward — the same boundary
	// retry.Max takes, for the same reason. Non-positive drains unbounded.
	abandonAfter time.Duration
}

// cursorNameWaker is this waker's key in driver_cursors.
const cursorNameWaker = "dependency_waker"

// noStoredCursor marks waker.persisted as "no row yet"; zero is a legitimate
// cursor value, so it cannot double as the sentinel.
const noStoredCursor = int64(-1)

// wakePersistRetryMax bounds the backoff between retries of a failing cursor
// write. Every attempt costs the pass that carries it, so a ladder in time is
// what keeps a store that cannot accept the write from being read once a floor
// for as long as it stays broken.
const wakePersistRetryMax = time.Minute

// wakeRetryBase is the first delay after a failed scan; it doubles up to the
// stale-dependents cadence, past which a retry only re-derives what that pass
// has already found. A failed pass must re-arm it: backingOff drops the wakes
// arriving meanwhile, so nothing else would look again.
const wakeRetryBase = 100 * time.Millisecond

// wakeScanPageCap bounds one scan page. The query is cheap; the cost is round
// trips on the store's single connection.
const wakeScanPageCap = 256

// wakeScanPagesPerPass bounds one pass's scan so resuming after a long gap
// cannot monopolise the single connection. The remainder rides the in-memory
// watermark to the next pass.
//
// Four, not sixteen, because a wake-driven pass can run ten times a second:
// BenchmarkWakerScanRateUnderSustainedWrites measures a full-budget pass at
// ~4ms against ~16ms, so a writer waits behind a hold a quarter as long. The
// budget and the throttle multiply, so a resume drains 2.5x faster than one
// pass per second could and holds a larger share of the connection while it
// does — shorter waits, not less total work.
const wakeScanPagesPerPass = 4

// run drives the waker for the life of the control plane: a commit wakes it and
// nothing else does, so an idle waker reads nothing. A write this process did
// not publish is left to the stale-dependents pass. See
// docs/adr/2026-08-05-the-waker-is-wake-driven.md.
func (dw *waker) run(ctx context.Context) {
	if dw.off() {
		return
	}
	defer dw.teardown()

	// A nil channel blocks forever, so a Beehive assembled without a hub waits
	// out its context.
	var written <-chan gobus.Event[GroupKind, struct{}]
	if dw.rx != nil {
		written = dw.rx.Chan()
	}

	timer := time.NewTimer(0)
	timer.Stop() // armed below only for what the seed left behind
	defer timer.Stop()
	// The instant timer.C is set to fire, zero when nothing is armed. A wake
	// that would only push it later leaves it alone.
	var armedFor time.Time

	backingOff := !dw.seeded // an unseeded waker drops wakes until its retry fires
	if wait := dw.primedWait(); wait != wakeIdle {
		driver.Rearm(timer, wait)
		armedFor = dw.now().Add(wait)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case _, open := <-written:
			if !open {
				return // only stop closes the hub, and the waker has nobody to tell
			}
			// Consumed rather than skipped, so the closed arm above stays live.
			if backingOff {
				continue
			}
		case <-timer.C:
			armedFor = time.Time{} // fired: nothing is armed until the Rearm below
		}

		now := dw.now()
		var next time.Duration
		next, backingOff = dw.pass(ctx, now, backingOff)
		switch nextTimer(next, now, armedFor) {
		case timerStop:
			// Stop, not just continue: a timer that was already ready when a
			// wake won the select would otherwise drive a pass nobody asked
			// for. Since Go 1.23 Stop leaves no stale value to receive.
			timer.Stop()
			armedFor = time.Time{}
		case timerArm:
			driver.Rearm(timer, next)
			armedFor = now.Add(next)
		}
	}
}

// off reports that there is nowhere to queue a wake.
func (dw *waker) off() bool { return len(dw.bh.order) == 0 || dw.bh.wakerOff }

// prime subscribes to the commit wakes and seeds the watermark. Start calls it
// before it returns, so both precede every write a caller could make; a failed
// seed leaves the waker unseeded, which run retries. See
// docs/adr/2026-08-06-the-waker-seeds-before-start-returns.md.
//
// Subscribe before the seed: the waker has no tick, so a commit landing before
// the subscribe wakes nothing at all. Every kind, because an edge can point at
// one the waker cannot name.
func (dw *waker) prime(ctx context.Context) {
	if dw.off() {
		return
	}
	// An aborted Start leaves the Beehive startable, and this attempt's seed is
	// the only one it may run on: inherited, a failed seed reads as caught up
	// and arms no retry.
	dw.seeded = false
	if rx, ok := dw.bh.kindWriteHub.WatchAcross(); ok {
		dw.rx = rx
	}
	dw.primed = dw.seed(ctx)
}

// teardown ends the wake subscription. Idempotent, so an aborted Start and a
// returning run can both call it.
func (dw *waker) teardown() {
	if dw.rx != nil {
		dw.rx.Close()
		dw.rx = nil
	}
}

// newWaker builds a waker over bh's cadences. Call it after the options are
// applied: the gates take their intervals here, so one built earlier would hold
// the defaults whatever the caller asked for.
func newWaker(bh *Beehive) *waker {
	cursors, _ := bh.store.(DriverCursorer)
	return &waker{
		bh:      bh,
		cursors: cursors,
		now:     time.Now,
		retry:   driver.Backoff{Base: wakeRetryBase, Max: bh.staleDependentsInterval},
		persistRetry: driver.Backoff{
			Base: max(bh.wakePersistInterval, wakeRetryBase),
			Max:  wakePersistRetryMax,
		},
		scanGate:     rategate.NewSingle(bh.wakeScanMinInterval),
		persistGate:  rategate.NewSingle(bh.wakePersistInterval),
		abandonAfter: bh.staleDependentsInterval,
	}
}

// pass is one turn of the run loop: scan under the throttle, and report how
// long to wait and whether to drop the wakes arriving meanwhile. backingOff is
// the loop's current answer to that, carried in because a pass that does not
// scan has no new one. Split from run so the rate tests drive it at instants of
// their own choosing.
func (dw *waker) pass(ctx context.Context, now time.Time, backingOff bool) (time.Duration, bool) {
	if opensAt, held := dw.scanGate.Allow(now); held {
		// Re-arming for what is left of the throttle is what remembers the
		// wake: the scan that runs then reads its position from the store.
		//
		// backingOff is carried through rather than cleared: the retry timer
		// armed before a failure can fire inside the throttle window, and
		// answering "not backing off" there would hand a degraded store back to
		// the wakes at the throttle's rate.
		return opensAt.Sub(now), backingOff
	}
	result := dw.scan(ctx)
	if result == scanFailed {
		return dw.retry.Next(), true
	}
	dw.retry.Reset()
	if result == scanMore {
		return dw.scanGate.Interval(), false // keep draining, at the throttle's rate
	}
	// A refused cursor write is a reason of its own: the wakes are queued
	// either way, but a successor that finds no row reseeds at the mark and
	// skips everything committed while this process was down.
	if wait, owed := dw.persistWait(now); owed {
		return wait, false
	}
	return wakeIdle, false
}

// persistWait reports how long until a cursor write is worth attempting again,
// and whether one is owed at all. Both pacers count, since the pass that
// carries the attempt costs a scan: the gate floors every attempt, the retry
// ladder paces a failing one.
func (dw *waker) persistWait(now time.Time) (time.Duration, bool) {
	if dw.cursors == nil || dw.watermark <= dw.persisted {
		return 0, false
	}
	wait := dw.persistOpensAt.Sub(now)
	if opensAt, held := dw.persistGate.OpensAt(now); held {
		wait = max(wait, opensAt.Sub(now))
	}
	return max(wait, wakeRetryBase), true
}

// primedWait is how long run waits before its first pass, and climbs the retry
// ladder when the seed did not land. Gated on seeded, not on primed: scanIdle is
// scanResult's zero value.
func (dw *waker) primedWait() time.Duration {
	switch {
	case !dw.seeded:
		return dw.retry.Next()
	case dw.primed == scanMore:
		return 0
	default:
		return wakeIdle
	}
}

// wakeIdle is pass's "no reason to look again": the loop arms nothing.
const wakeIdle time.Duration = -1

// timerAction is what pass's answer means for the loop's one timer.
type timerAction uint8

const (
	timerKeep timerAction = iota // a pending timer already fires sooner
	timerArm                     // (re)arm for the delay pass returned
	timerStop                    // nothing to look again for
)

// nextTimer decides that action from pass's delay and the instant the timer is
// currently set to fire (zero when nothing is armed). Under a sustained stream
// the loop turns at commit rate, so a wake that would only push the deadline
// later must leave it alone.
func nextTimer(next time.Duration, now, armedFor time.Time) timerAction {
	if next == wakeIdle {
		return timerStop
	}
	if armedFor.IsZero() || now.Add(next).Before(armedFor) {
		return timerArm
	}
	return timerKeep
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
	// persists whatever the watermark reached. It must stay in scan: in scanPages
	// it would run before the jump.
	defer dw.persist(ctx)
	result := dw.scanPages(ctx)
	if result == scanMore {
		result = dw.abandonIfOvertaken(ctx)
	}
	if result != scanMore {
		dw.drainSince = time.Time{} // only an unbroken drain holds the connection
	}
	return result
}

// scanPages reads up to wakeScanPagesPerPass pages, returning scanMore when the
// budget rather than the log is what stopped it.
func (dw *waker) scanPages(ctx context.Context) scanResult {
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

// abandonIfOvertaken jumps the watermark to the write log's mark once a drain has
// run for abandonAfter, leaving the range it skips to the stale-dependents pass.
// See docs/adr/2026-08-05-the-waker-abandons-an-overtaken-drain.md.
func (dw *waker) abandonIfOvertaken(ctx context.Context) scanResult {
	if dw.abandonAfter <= 0 {
		return scanMore // no threshold, no backstop to be overtaken by
	}
	now := dw.now()
	if dw.drainSince.IsZero() {
		dw.drainSince = now // this pass is the drain's first
	}
	drained := now.Sub(dw.drainSince)
	if drained < dw.abandonAfter {
		return scanMore
	}
	mark, err := dw.bh.store.ObjectWritesMaxVersionAll(ctx)
	if err != nil {
		// Not scanFailed: no wake depends on this read, and backing off would drop
		// the wakes arriving meanwhile. Restarting the window is what paces the
		// retry — held here, a full-budget pass would re-read at the wake rate.
		if ctx.Err() == nil {
			dw.bh.log().WarnContext(ctx, "reading the write log's mark failed; the drain continues and a later pass retries the skip",
				"watermark", dw.watermark, "err", err)
		}
		// Dated after the read, not from now above: a read that blocked longer than
		// the window would otherwise leave the retry unpaced.
		dw.drainSince = dw.now()
		return scanMore
	}
	dw.bh.log().WarnContext(ctx, "the dependency waker's backlog outlasted the stale-dependents cadence; skipping to the write log's mark, and that pass delivers the wakes in between",
		"watermark", dw.watermark, "mark", mark, "drained", drained)
	// The mark folds in no horizon: a trimmed log reads below the watermark, a
	// fully trimmed one reads 0.
	dw.watermark = max(dw.watermark, mark)
	return scanIdle
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
	now := dw.now()
	if _, held := dw.persistGate.Allow(now); held {
		return
	}
	if now.Before(dw.persistOpensAt) {
		return
	}
	if err := dw.cursors.DriverCursorsSet(ctx, cursorNameWaker, dw.watermark); err != nil {
		dw.persistFailures++
		dw.persistOpensAt = now.Add(dw.persistRetry.Next())
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
		dw.persistFailures, dw.persistOpensAt = 0, time.Time{}
		dw.persistRetry.Reset()
	}
	dw.persisted = dw.watermark
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
		// that would have said. Holding the watermark makes the next pass
		// re-read the same changes.
		dw.bh.log().WarnContext(ctx, "dependents lookup failed; these changes stay above the watermark for the next pass",
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
	// order is frozen after Start, so the kind list is built once.
	kinds := bh.registeredKinds()
	if len(kinds) == 0 {
		return
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
