# The waker abandons a drain the backstop has already overtaken

**Status:** not built. Replaces the "waker resuming a very stale cursor" entry in
[`../TODO.md`](../TODO.md), which shrinks to a pointer here.

## Problem

`waker.scan` drains its backlog page by page with no upper bound on how long it
keeps doing it. After a long downtime `seed` resumes a cursor far behind the write
log, and every re-armed pass reads its full `wakeScanPagesPerPass` budget (4 pages
of `wakeScanPageCap`, 256 rows) until the log runs out. `scanGate` floors passes at
`wakeScanMinInterval` (100ms), so a resume runs at up to 40 queries a second on the
one connection every commit and every reconcile loop shares — potentially for
minutes.

Nothing in the loop decides that draining has stopped being worth it. Past a point
it has: the stale-dependents pass (60s, cannot be disabled) derives staleness from
`dependency_watermarks` rather than from anything the waker recorded, and its cursor
is process-local and starts at 0, so **the first sweep of the process finds every
stale dependent in the store** — including every dependent the drain is still
grinding toward. After that sweep, the remaining pages buy nothing but a wake for
work already found, stamped `reconcile_owed`, and enqueued.

No deployment has been observed hitting this. The cost is latency and connection
contention, never divergence.

**This is a safety valve, not a change to the normal resume path.** 60s at ~40
queries a second is ~2400 pages and ~614k rows drained before anything sheds, so a
restart whose backlog is under about 600k log rows never abandons at all. What the
bound covers is a store whose write rate outruns the drain — which is also the only
way to reach the threshold outside a resume.

## What exists

- `waker.scan` (`waker.go`) pages from `dw.watermark` via
  `ObjectWritesListSinceAll`, returns `scanMore` when the page budget stops it,
  `scanIdle` on a short or empty page, `scanFailed` on a read error, and persists
  whatever the pages reached through a `defer dw.persist(ctx)`.
- `waker.pass` gates on `scanGate` before scanning, and on `scanMore` returns
  `dw.scanGate.Interval()` — which is what paces a drain today.
- `dw.now` is the waker's only clock, and `persist` already reads it directly
  rather than taking an instant as a parameter.
- `seed`/`resumeWatermark` resume from the persisted cursor however far behind it
  sits, clamped down to the mark (`min`) because retention trims the log's tail.
- The object tail took the same pacing pair — a `rategate` floor plus a page budget
  — and bounds one drain by pages only
  ([ADR](../adr/2026-08-05-the-object-tail-throttles-its-drains.md)). Neither
  driver bounds a *sequence* of full-budget drains, which is what this spec adds
  for the waker.

**The obvious bound is one that was already removed, and it must not come back.**
Capping `mark - stored` at seed reads a `resource_version` distance, and `EventsAdd`
draws from that same sequence while writing nothing the scan reads. The distance
overstates the real backlog by an unbounded factor, so a store logging events at any
rate would abandon cursors that were a few passes of work.
`TestWakerResumesAnEnormousBacklog` pins that.

## Proposal

**Measure the draining, not the distance.** Track when the current run of
full-budget passes began; once it has run for `staleDependentsInterval`, stop
paging, read a fresh mark, jump the watermark to it, and report idle.

One new field and one derived threshold on `waker`:

```go
// drainSince is when the current run of budget-exhausting passes began; zero
// when the last pass did not exhaust its budget. Only paging counts.
drainSince time.Time

// abandonAfter is how long a drain may run before the stale-dependents pass has
// found everything it is still working toward. Non-positive turns the abandon
// off, which is what a Beehive assembled field by field gets.
abandonAfter time.Duration
```

`scan` keeps its body and **keeps its `defer dw.persist(ctx)`**; only the paging loop
moves into `scanPages`. Every outcome other than a budget-exhausted one clears the
streak (`seed` returns before this, so a resume's `scanMore` correctly leaves
`drainSince` alone):

```go
result := dw.scanPages(ctx)   // today's loop, without the defer above it
if result != scanMore {
    dw.drainSince = time.Time{}
    return result
}
return dw.abandonIfOvertaken(ctx)
```

`abandonIfOvertaken`:

0. If `dw.abandonAfter <= 0`, return `scanMore`: unbounded draining is today's
   behaviour, and it is the safe answer for a waker whose threshold was never set.
1. `now := dw.now()`. If `drainSince` is zero, stamp it and return `scanMore` — the
   streak's clock starts at the first exhausting pass, so a resume pays one pass
   before it is counted.
2. If `now.Sub(dw.drainSince) < dw.abandonAfter`, return `scanMore`.
3. Read `ObjectWritesMaxVersionAll`. On error: log, leave `drainSince` alone, return
   `scanMore` — nothing is lost, and the next pass tries the abandon again. It must
   **not** become `scanFailed`: that would arm the backoff and drop wakes over a
   read the drain does not depend on.
4. `dw.watermark = max(dw.watermark, mark)`. `max`, not assignment, and this is
   load-bearing: `ObjectWritesMaxVersionAll` is a bare `MAX(resource_version)` with
   no horizon fold — unlike the per-kind `ObjectWritesMaxVersion` the tailer gates on
   — so a fully trimmed log answers 0. Assigning would reset the watermark to zero
   and replay the whole log. The watermark never moves backwards.
5. Clear `drainSince`, warn with the watermark, the mark, the versions skipped and how
   long the drain ran — one line per abandon, so sustained shedding logs once per
   threshold window — and return `scanIdle`.

Because the `defer` stays in `scan`, it runs after `abandonIfOvertaken` has set the
jumped watermark, and a restart does not re-drain the range that was just abandoned.
Move the `defer` into `scanPages` and it persists the pre-jump watermark instead;
`TestWakerAbandonsADrainTheBackstopOvertook`'s `setCalls` assertion is what catches
that. If the persist gate holds the write, `pass`'s `persistWait` path already owes
it.

`scanIdle` then takes `pass` to `wakeIdle`, the loop stops the timer, and the waker
waits for the next commit — which is the correct steady state, because it is caught
up.

### Why elapsed time, not a pass count

The claim that justifies abandoning is a claim about the backstop's *cadence*: after
one `staleDependentsInterval` a sweep has completed and covered this ground. Pass
count reaches the same place only through `wakeScanMinInterval`, so it is a proxy
that breaks exactly where the floor is changed or disabled — `wakeScanMinInterval`
is an unexported test option and some tests set it to 0, which makes a
`interval`-to-`threshold` division either meaningless or a division by zero.
Elapsed time needs no guard and states the argument directly.

### No new option

The threshold is `staleDependentsInterval` — the cadence the argument is about, and
one `withStaleDependentsInterval` already validates as positive for every `Beehive`
that came from `New`. A knob of its own would be a guessed number asking to be tuned.
Tests reach the behaviour by driving `dw.now` with `fakeClock`, as the rate tests
already do.

Step 0 exists because that validation covers `New` and nothing else: the whitebox
tests assemble bare `Beehive` structs, and `wakerOver` sets the cadences only because
someone remembered to. `retry.Max` leans on the same invariant, but the failure modes
are not comparable — a zero `Max` is a loud tight retry loop, while a zero
`abandonAfter` would make the second exhausting pass shed the whole backlog behind
one log line.

## What must stay true

- **The stale-dependents pass is still the guarantee.** The abandon converts "the
  waker will scan this" into "the backstop already found it, or finds it on the next
  sweep". That is the same trade three documented gaps already accept — a store with
  no `DriverCursorer`, a wake queued but never delivered, a log trimmed under the
  cursor. The cost for the abandoned range is at most two sweep intervals of latency,
  one in the common case: the threshold guarantees a sweep *started*, and a sweep that
  fails a page abandons and holds its cursor for the next one. Never divergence.
- **The backstop's finding stays a superset of the waker's.** `DependentsListStaleSince`
  drives from `objects t`, so a physically deleted target's dependents would be
  invisible to it — but `edges.to_id` is `ON DELETE RESTRICT`, so a target with live
  `depends_on` edges cannot be physically deleted, and the waker finds nothing there
  either. Relax that constraint and this abandon becomes a hole.
- **`seed` does not change.** No bound reads a `resource_version` distance.
  `TestWakerResumesAnEnormousBacklog` and `TestResumeWatermark` must pass untouched
  — the `TODO.md` entry's claim that this tripwire "is exactly what such a bound
  would change" is true only of the distance bound, not of this one.
- **The watermark never decreases**, on this path or any other.
- **Only paging keeps the streak.** A short page, an empty page, a failed page or a
  failed edges lookup clears `drainSince`; a pass the gate held scans nothing and
  changes nothing. So an abandon requires a genuinely continuous drain — with the
  production cadences, sustained writes above ~10k/s for the whole threshold window.
  Under that load shedding to the backstop is the right answer, and the warn line is
  how it is diagnosable.
- **`scanFailed` keeps its meaning:** a read the wakes depend on failed. The mark
  read in step 3 is not one of those.
- **The cursor still records what was scanned, never what was woken.** A jump
  records that this waker will not scan the range, which is exactly what the next
  process must not re-drain.
- **`wakeScanPagesPerPass` and `wakeScanMinInterval` keep their measured values.**
  This spec bounds how many full-budget passes run in a row; it does not retune one.
  `BenchmarkWakerScanRateUnderSustainedWrites` is unaffected.

## Tests

New, in `waker_test.go` (whitebox, `seededWaker`'s fake clock, `cursorStore` over
`replayStore`):

- `TestWakerAbandonsADrainTheBackstopOvertook` — a backlog larger than the threshold
  window can drain; advance the clock by `wakeScanMinInterval` per pass; assert the
  watermark jumps to the store's mark, the result is `scanIdle`, no page is read
  after that, and the jumped cursor is in `store.setCalls`.
- `TestWakerKeepsDrainingInsideTheBackstopInterval` — one tick short of the
  threshold still returns `scanMore` and still pages.
- `TestWakerDrainStreakResetsOnAShortPage` — drain to just under the threshold, serve
  one short page, drain again: no abandon, because the streak restarted.
- `TestWakerDrainStreakResetsOnAFailedPage` — same shape via `err`/`failFromCall`.
- `TestWakerAbandonHoldsTheWatermarkWhenTheMarkIsLower` — `store.seed` below the
  watermark (a trimmed log): `scanIdle`, watermark unchanged.
- `TestWakerAbandonRetriesAFailedMarkRead` — set `seedErr` after seeding: the pass
  returns `scanMore` rather than `scanFailed`, and the next one abandons.
- `TestWakerWithNoThresholdNeverAbandons` — `abandonAfter` zero (a hand-built
  `Beehive`): full-budget passes keep returning `scanMore` however far the clock
  advances. Step 0's tripwire.

Preserved as tripwires, unchanged: `TestWakerResumesAnEnormousBacklog`,
`TestResumeWatermark`, `TestWakerStopsAtThePageBudget` (real clock, two scans
microseconds apart: the first exhausts and stamps `drainSince`, the second serves a
short page and clears it — it must not start abandoning).

## Landing it

- `waker.go`: the field, `abandonAfter` in `newWaker`, `scanPages` extracted,
  `abandonIfOvertaken`.
- New ADR, `docs/adr/2026-08-05-the-waker-abandons-an-overtaken-drain.md`, carrying
  the rationale above (why measured not distance, why elapsed not pass count, why no
  option, why the safety valve sheds only above ~600k rows) plus an index line in
  `docs/adr/README.md`. Record the `max()` in the sharper form: the store-wide mark
  folds in no horizon, so a trimmed log answers 0 and an assignment would replay the
  whole log — the asymmetry with the per-kind `ObjectWritesMaxVersion` the tailer
  gates on is the trap worth writing down.
- `CLAUDE.md`: one clause on the waker bullet — a drain that outlasts the
  stale-dependents cadence jumps to the mark and leaves the range to that pass —
  with the ADR link.
- `docs/reconcile-triggers.md`: case 6's paging paragraph, and the new tests in its
  test list.
- `docs/TODO.md`: delete the "waker resuming a very stale cursor" entry; the
  remaining "retention trimmed the log out from under its cursor" entry keeps its own
  reasoning and gains nothing here.
- Delete this spec on merge: a spec's rationale moves to the ADR and the spec goes,
  the way the four shipped ones did.
- Commit: `feat(waker): abandon a drain the stale-dependents pass has overtaken`.
