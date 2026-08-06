# The dependency waker detects a log retention trimmed under its cursor

- **Status:** Implemented 2026-08-06 — rationale now lives in
  [the ADR](../adr/2026-08-06-the-waker-sees-a-retention-trim.md), and the deferred
  `../TODO.md` entry it supersedes is deleted. One planned piece was dropped
  during implementation; see "Revision notes".
- **Date:** 2026-08-05, rewritten 2026-08-06

## Problem

`object_writes` is trimmed by retention (24h by default). What a sweep removed is
recorded per kind in `object_writes_horizon`, and the per-kind read pair reports it:
`ObjectWritesListSince` returns `trimmedThrough` alongside the page, and
`ObjectWritesMaxVersion` folds the horizon in so the position only ever rises. That
is how `objectTailer` recognises a cursor below the boundary and ends its
subscribers with `ErrWatchTooOld` (`horizonErr`, `objectswatch.go:230`).

The store-wide pair reports neither. `ObjectWritesListSinceAll` returns entries and
an error; `ObjectWritesMaxVersionAll` is a bare `MAX(resource_version)` over
`object_writes` that never reads the horizon table. So the waker cannot distinguish
"nothing changed above my cursor" from "everything above my cursor was deleted
before I read it". Two paths reach that state:

1. **A resume after a long downtime.** `seed` takes the persisted cursor, clamps it
   with `resumeWatermark` — `min(stored, mark)`, and `mark` does not report the
   trim either — and scans from there. Entries between the cursor and the horizon
   are gone; the dependents they would have woken are skipped with them.
2. **A live waker overtaken by retention.** A waker whose scans have been failing
   (`driver.Backoff` caps at the stale-dependents cadence, so it retries forever)
   or whose backlog has outlasted the retention window sits below a horizon that
   keeps rising underneath it.

In both, the skip is silent: no log line, no counter, nothing an operator can see.

## Impact

**Latency and observability, not correctness.** Those dependents are still found by
the stale-dependents pass, which derives staleness from `dependency_watermarks` and
`reconcile_owed` — neither of which the write log's trim can touch — and which
cannot be disabled. Case 1 opens at a restart, which is also when that pass
re-derives the whole graph from a cursor of 0, so its cost there is effectively
nil; case 2 costs at most one `staleDependentsInterval`.

What is actually missing is the report. A silent skip on a path that only opens
after a downtime longer than retention is exactly the kind of thing that is
diagnosed from a log line or not at all.

## Design

The horizon is read where it is free, and reported against the position the waker
is actually known to have processed.

### Store: two reads, each one statement

**`ObjectWritesMaxVersionAll` gains a second return, not new semantics.**

```go
// ObjectWritesMaxVersionAll is ObjectWritesMaxVersion across every kind. at is
// the log's bare maximum: not monotonic — a delete or a retention sweep lowers
// it — so consumers compare for inequality. trimmedThrough is the highest
// version retention has removed from ANY kind, 0 when nothing has been trimmed.
ObjectWritesMaxVersionAll(ctx context.Context) (at int64, trimmedThrough int64, err error)
```

`at` keeps exactly the value it has today. That is load-bearing for
`abandonIfOvertaken`, which relies on a trimmed log reading *below* the watermark
and guards with `max(dw.watermark, mark)`
([ADR](../adr/2026-08-05-the-waker-abandons-an-overtaken-drain.md)); that caller
discards the new value (`mark, _, err :=`) and is otherwise untouched. In SQLite
both halves are scalar subqueries in one statement, the second over a `WITHOUT
ROWID` table holding one row per kind.

This is the authoritative read, and `seed` is its only consumer — once per
process, which is exactly where case 1 lives.

**`ObjectWritesListSinceAll` reports the horizon its own page statement carries.**

```go
// ObjectWritesListSinceAll is ObjectWritesListSince across every kind, for the
// dependency waker: an edge can point at a kind with no controller.
// trimmedThrough is the retention horizon as of the page's statement — the
// highest version trimmed from any kind — so afterRV < trimmedThrough means
// entries were trimmed unread; equality is fine, since the next unread entry is
// trimmedThrough + 1. An EMPTY page reports 0: the horizon rides the rows, and
// ObjectWritesMaxVersionAll is what answers the boundary on its own.
ObjectWritesListSinceAll(ctx context.Context, afterRV int64, limit int) (page []ObjectWrite, trimmedThrough int64, err error)
```

**No `Within`, and no empty-page fallback read** — the difference from
`ObjectWritesListSince`, which the previous draft mirrored wholesale. That
function's transaction exists for `attachImages`; the store-wide read carries no
row images by design, and the page and the horizon already share one statement via
the scalar subquery. Wrapping it would put a `BEGIN`/`COMMIT` pair on the store's
single connection for every page, and a second statement on every *empty* page —
which is the shape of the quiet pass the wake-driven design prices at ~21µs
([ADR](../adr/2026-08-05-the-waker-is-wake-driven.md), `BenchmarkWakerScanRateUnderSustainedWrites`).
A waker that runs per commit must not double its idle cost to detect a condition
that opens after 24h of downtime.

Reading page and horizon unsynchronized loses no soundness. A horizon that rose
between two reads can only mean entries were genuinely trimmed unread: the page
bounds what was live at its own instant, and versions are monotonic, so anything a
later sweep removes was either already below the cursor — no report, no jump — or
really is gone before this waker read it.

### Waker: report against what was processed, jump only where nothing readable is skipped

The exactness argument for a cross-kind max needs one qualification the previous
draft got wrong. "A watermark of W means nothing above W was processed" is *not*
true immediately after `seed`, because `resumeWatermark` deliberately clamps the
watermark **down** to `min(stored, mark)` — the very case this spec is about. A
waker that processed through 950 against a log trimmed empty (`mark` 0, horizon
900) would resume at 0 and report a 0–900 span it never skipped, on every restart;
with a real gap at `stored` 500 it would report 0–900 instead of 500–900.

So the report is gated on the pre-clamp position, and `waker` gains two fields:

```go
// horizon is the highest retention boundary any read has reported. A scan that
// runs out of readable log jumps the watermark to it.
horizon int64

// trimBaseline is the highest position this waker is known to have processed,
// which is not the watermark: seed clamps the watermark DOWN to the log's mark,
// so comparing a horizon against it would report a span nothing skipped.
trimBaseline int64
```

**`seed`** reads both values, and `resumeWatermark` takes the horizon so the clamp
stays pure and testable — **superseded, see revision note 3: the horizon moves no
cursor, and this signature was reverted**:

```go
func resumeWatermark(stored int64, ok bool, mark, trimmedThrough int64) int64 {
	if !ok {
		return max(mark, trimmedThrough)
	}
	return max(min(stored, mark), trimmedThrough)
}
```

Raising to the horizon skips only what retention already deleted, and it is what
stops a resume from replaying a range that no longer exists. `seed` then sets
`trimBaseline` to `stored` when the cursor row exists (`mark` otherwise — a waker
with no stored cursor owes nothing from before its own startup, so it never
reports), sets `horizon` to the value read, and warns when `ok && trimmedThrough >
stored`, naming the real span `(stored, trimmedThrough]`.

**`scanPages`** compares after every successful read, **before** consuming the
page:

- `dw.horizon = max(dw.horizon, trimmed)`, unconditionally — the field is the
  highest boundary seen, independent of whether this read reported anything.
- report when `trimmed > max(dw.watermark, dw.trimBaseline)`: one warn line naming
  the skipped span and saying the stale-dependents pass delivers it, then
  `dw.trimBaseline = trimmed`. The gate is what keeps a large backlog from
  re-reporting the same boundary once per page and once per pass.
- Checking at read time, not at the end of the scan, is what makes a backlog
  visible: it drains past the horizon, so a check deferred to the last page would
  find the watermark already above it and report nothing.

**The jump** is separate from the report and happens only on the two `scanIdle`
returns (empty page, short page): `dw.watermark = max(dw.watermark, dw.horizon)`.
Two things make it legal there, and both are needed. Nothing was above the page
when the store answered, so no live entry below the horizon was passed over; and
nothing can appear below it afterwards, because `resource_version` is monotonic and
`trimmed_through` is always a version that has already been issued, so every later
commit lands above the horizon and inside a later page. On the `scanMore`
(budget-exhausted) path the watermark must **not** jump: another kind may still
have live entries between the watermark and the horizon.

Without the jump, a waker below a rising horizon re-reports forever and its
persisted cursor never records that the span is dead; with it, it resyncs once.

**Not done:** forcing an out-of-band stale-dependents sweep on detection. It buys
nothing in case 1 — that pass's eager first sweep of the process already covers
every dependent in the store — and at most one interval in case 2, in exchange for
a wake channel between two drivers that are otherwise independent.
`staleDependentsRun` stays a plain `driver.Run`.

## Files

| File | Change |
| --- | --- |
| `internal/storeapi/storeapi.go` | Both signatures above; docs state the cross-kind max, the `<` vs `=` boundary, the empty-page 0, and that `at` is unchanged. |
| `sqlite/store.go` | `ObjectWritesMaxVersionAll`: second scalar subquery over `object_writes_horizon`. `ObjectWritesListSinceAll`: the same subquery as a trailing column, scanned like `writeLogPage` does. No transaction in either. |
| `waker.go` | `horizon` and `trimBaseline` fields; `resumeWatermark` takes the horizon; `seed` reports and raises; `scanPages` reads the third return, reports, and jumps on the idle paths; `abandonIfOvertaken` discards the new value. |
| `testutils_test.go` | `fakeStore` (:492, :512) and `replayStore` (:580, :611) signatures; `replayStore` gains a scripted `trimmed` value. |
| `waker_test.go` | `slowMarkStore.ObjectWritesMaxVersionAll` (:1108). |
| `reconciler_test.go` | `seedProbeStore.ObjectWritesMaxVersionAll` (:2861). |
| `waker_bench_test.go` | `wakeCountingStore.ObjectWritesListSinceAll` (:185). |
| `sqlite/store_test.go` | 12 `ObjectWritesListSinceAll` call sites and 3 `ObjectWritesMaxVersionAll` call sites take the extra return. |

No schema change: `object_writes_horizon` already holds everything needed, and
`TestTheSchemaIsOneMigration` stays untouched.

## Tests

New, in `waker_test.go` unless noted:

- `TestWakerReportsATrimmedSpanAtSeed` — a stored cursor below the horizon: one
  warn naming `(stored, trimmedThrough]`, and the watermark resumes at the horizon
  rather than at the clamped mark.
- `TestWakerReportsNothingWhenTheClampLowersTheWatermark` — the regression this
  revision exists for: stored cursor **above** the horizon with the log trimmed
  empty (`mark` 0) reports nothing, on the seed and on every scan after it.
- `TestWakerReportsATrimmedSpanOnce` — the same horizon over several pages and
  several passes logs one line; a horizon that rises again logs a second.
- `TestWakerResyncsPastATrimmedSpan` — short page with the horizon above the
  page's top: the watermark lands on the horizon, the cursor is persisted there,
  and the next scan reads from it.
- `TestWakerHoldsTheWatermarkWhileDraining` — a full-budget page with a horizon
  above the watermark returns `scanMore` and does **not** jump, so live entries
  from another kind below the horizon are still read.
- `TestWakerReportsNothingOnAnUntrimmedLog` — horizon 0, and horizon exactly equal
  to the baseline (equality lost nothing), report nothing.
- `resumeWatermark` table test: the horizon raises both the `ok` and `!ok`
  branches, and never lowers either.
- `sqlite/store_test.go: TestObjectWritesListSinceAllReportsTheHorizon` — trim one
  kind by age, read store-wide from below the boundary, assert the reported value;
  and assert an empty page reports 0.
- `sqlite/store_test.go: TestObjectWritesMaxVersionAllReportsTheHorizon` — the
  authoritative read answers the boundary with the log trimmed empty, where the
  page read cannot.
- `sqlite/store_test.go: TestObjectWritesHorizonAllIsTheMaxAcrossKinds` — two kinds
  trimmed to different depths report the higher, on both reads.

Existing tripwires that must keep passing unchanged:
`TestWakerAbandonHoldsTheWatermarkWhenTheMarkIsLower` (the bare `at`),
`TestWakerSeedsFromTheStoredCursor`, `TestWakerSeedsFromTheWriteLogMax`,
`TestResourceVersionsMaxIssuedNeverFalls`.

## Docs

- New ADR, `docs/adr/2026-08-06-the-waker-detects-a-trimmed-log.md`: why a max
  across kinds is exact only against the pre-clamp position, why the report and the
  jump are separated, why the store-wide read needs no transaction where the
  per-kind one does, why `at` stays bare, and why no sweep is forced. Link it from
  `docs/adr/README.md`.
- `CLAUDE.md`: the waker bullet gains one clause — the store-wide reads report the
  retention horizon, and a cursor below it resyncs with a warning rather than
  silently skipping.
- `docs/TODO.md`: delete the deferred entry this spec replaces.
- `docs/reconcile-triggers.md`: the waker case notes the horizon and the resync;
  add the new tests to its test list.
- `docs/adr/2026-08-02-object-write-log.md`: its "`ObjectWritesMaxVersion` folds the
  horizon in" section gets a line on the store-wide reads reporting one alongside.

## Out of scope

- The waker's startup seed race (separate `docs/TODO.md` entry).
- Any change to `at`'s value, to the clamp's downward half, or to
  `abandonIfOvertaken`'s behaviour.
- A `Client`-visible error for the gap. There are no subscribers to end; the waker
  is an optimisation over the stale-dependents pass, never a guarantee.

## Revision notes

Four things changed after the first draft was checked against the code, the last
two during and after implementation:

1. **The report gate.** Comparing the horizon against `dw.watermark` alone is a
   false positive on every restart of a store whose log was trimmed empty, because
   `resumeWatermark` clamps the watermark down — and it misstates the span in the
   true-positive case. Hence `trimBaseline` and the pre-clamp comparison.
2. **The read shape.** The first draft wrapped the store-wide list in `Within` with
   an empty-page fallback, mirroring `ObjectWritesListSince`. That transaction
   exists for row images the store-wide read does not carry, and the fallback would
   add a second statement to the quiet pass that runs per commit. The authoritative
   horizon read moved to `ObjectWritesMaxVersionAll`, which `seed` already calls.
3. **The horizon moves no cursor at all.** Both the mid-scan jump and the resume
   raise were wrong, for the same reason found in review: the horizon is a `MAX`
   over kinds, and `ObjectWritesSweep`'s per-kind count bound trims a chatty kind
   past entries a quiet one still holds. A store-wide horizon of 1000 does not mean
   the range below it is empty, so resuming there drops a surviving entry at 500 for
   good. `resumeWatermark` keeps its original three-argument clamp;
   `TestWakerResumeKeepsEntriesBelowTheHorizon` guards it. The horizon is now used
   for the report and nothing else.
4. **The mid-scan jump was dropped**, before the above was found. With `resumeWatermark` raising at seed and
   `trimBaseline` deduping the report, jumping the watermark to the horizon on an
   idle scan changes nothing observable: the trimmed span holds no entries to skip,
   and a lagging cursor is raised at the next seed. `TestWakerResyncsPastATrimmedSpan`
   and `TestWakerHoldsTheWatermarkWhileDraining` were dropped with it.
