# The dependency waker sees a retention trim under its cursor

- **Status:** Accepted — implemented in `waker.go`, `sqlite/store.go`,
  `internal/storeapi/storeapi.go`.
- **Date:** 2026-08-06

## Context

The write log is trimmed by retention (24h by default), and what a sweep removed
is recorded per kind in `object_writes_horizon`. The per-kind reads report it —
`ObjectWritesListSince` returns `trimmedThrough`, `ObjectWritesMaxVersion` folds
the horizon in — which is how `objectTailer` ends a subscriber below the boundary
with `ErrWatchTooOld`.

The store-wide pair the waker reads reported neither, so the waker could not tell
"nothing changed above my cursor" from "everything above it was deleted before I
read it". Two paths reach that state: a resume from a cursor older than retention,
and a live waker whose failing scans leave it below a horizon that keeps rising.
The dependents in the skipped span are still found by the stale-dependents pass,
which derives staleness from `dependency_watermarks` — so what was missing was the
report, not the wake.

## Decision

**Report the horizon on both store-wide reads, and use it for nothing else.**

`ObjectWritesMaxVersionAll` returns `(at, trimmedThrough)`. `at` keeps its bare
`MAX(resource_version)` semantics — `abandonIfOvertaken` depends on a trimmed log
reading *below* the watermark — so the horizon rides beside it rather than folded
in. `ObjectWritesListSinceAll` carries the horizon as a trailing column on the
page's own statement.

`noteTrim` warns once per boundary. The resume point is unchanged: see "the
horizon cannot move a cursor" below.

### The report compares against the stored cursor, not the watermark

`resumeWatermark` clamps *down* to the log's mark, which is exactly what the
retention case makes it do: a waker that processed through 950 against a log
trimmed empty resumes at 0. Comparing a horizon of 900 against that watermark
reports a span nothing skipped, on every restart, and with a real gap at 400 it
reports 0–900 instead of 400–900. So `noteTrim` takes the position known
*processed* — the stored cursor at seed, the watermark during a scan — and
`trimBaseline` keeps a boundary that spans many pages to one line.

### The list read is one statement, and needs no transaction

`ObjectWritesListSince` is wrapped in `Within` because it attaches row images and
must answer for one instant. The store-wide read carries no images, and this is
the waker's whole quiet pass — the ~21µs `BenchmarkWakerScanRateUnderSustainedWrites`
prices, run per commit. A `BEGIN`/`COMMIT` pair per page, plus a second statement
for the empty page that *is* the quiet pass, is a real cost to detect a condition
that opens after 24h of downtime.

Reading page and horizon unsynchronized loses nothing: the page bounds what was
live at its own instant and versions only rise, so a horizon that rose in between
means entries really were trimmed unread.

### An empty page pays for the horizon once per watermark

An empty page carries no row to carry the horizon, and that is exactly the case a
stalled waker hits once retention has removed *every* entry above its cursor — the
loss it most needs to report. So `noteTrimIdle` reads `ObjectWritesMaxVersionAll`
on its own there, gated on `horizonAt`: the watermark that read already answered
for.

Gated, because on this machine the empty page costs ~29µs and the extra read
~13µs — a 45% surcharge on the pass that runs per commit, to re-ask a question
whose answer cannot have changed while the watermark stands still. `seed` sets
`horizonAt` from its own mark read, and a non-empty page sets it too (its horizon
was read at the same instant as its rows), so the steady state pays nothing and a
run of quiet passes pays once.

### The horizon detects a loss; it cannot move a cursor

A `MAX` over the horizon table is exact **for detection**. The waker's cursor is
store-wide and monotonic, so a cursor at W means nothing above W was processed for
any kind, and a kind whose `trimmed_through` exceeds W had entries deleted unread.
No false positive, and no true gap escaping under a shallower kind.

It says nothing about the range being *empty*, and the difference is not academic:
`ObjectWritesSweep`'s count bound caps each kind to its newest N entries, so a
chatty kind can be trimmed through 1000 while a quiet one still holds an unread
entry at 500. A store-wide maximum of 1000 proves entries below it were deleted —
not which ones. **Skipping to it would drop that surviving entry for good.**

So the horizon moves no cursor anywhere. `resumeWatermark` keeps the clamp it
always had, and neither the seed nor a scan jumps. An earlier draft did both;
`TestWakerResumeKeepsEntriesBelowTheHorizon` is the guard.

### No forced sweep

Forcing an out-of-band stale-dependents sweep on detection was dropped too. It
buys nothing on the resume path — that pass's eager first sweep already covers
every dependent in the store — and at most one interval on the stalled path, in
exchange for a wake channel between two drivers that are otherwise independent.

## Consequences

- **A skipped span is a warning naming both ends**, on the seed that found it or
  on the first page read after the boundary rose. `TestWakerReportsATrimmedSpanAtSeed`
  and `TestWakerReportsATrimmedSpanOnce` pin it.
- **A clamped resume reports nothing.**
  `TestWakerReportsNothingWhenTheClampLowersTheWatermark` is the regression guard;
  it is the common case, not the rare one.
- **A resume is unchanged by any of this.** It still clamps to the log's mark,
  and an entry a shallower kind kept below the horizon is still scanned.
- **The quiet pass is still one statement** in the steady state, and two on the
  first empty page after the watermark moves. `TestWakerSkipsTheHorizonReadAfterAPageCarriedIt`
  pins the gate; `TestWakerReportsAFullyTrimmedBacklog` pins the case it exists for.
- **Latency is unchanged.** This is an observability change: the wakes in the
  skipped span were, and still are, delivered by the stale-dependents pass.
