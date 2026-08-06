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

**Report the horizon on both store-wide reads, and resume above it.**

`ObjectWritesMaxVersionAll` returns `(at, trimmedThrough)`. `at` keeps its bare
`MAX(resource_version)` semantics — `abandonIfOvertaken` depends on a trimmed log
reading *below* the watermark — so the horizon rides beside it rather than folded
in. `ObjectWritesListSinceAll` carries the horizon as a trailing column on the
page's own statement.

`resumeWatermark` takes the horizon and never resumes below it, and `noteTrim`
warns once per boundary.

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
means entries really were trimmed unread. An empty page reports 0 rather than
paying for a second read; `ObjectWritesMaxVersionAll` is what answers the boundary
alone, and `seed` is where that matters.

### The horizon is the deepest trim over any kind

The waker's cursor is store-wide and monotonic, so a watermark of W means nothing
above W was processed for any kind, and a kind whose `trimmed_through` exceeds W
had entries deleted unread. A `MAX` over the horizon table is therefore exact
rather than conservative — no false positive, and no true gap escaping under a
shallower kind.

### No mid-scan jump, and no forced sweep

An earlier draft also jumped the watermark to the horizon when a scan ran out of
readable log. It is redundant: the trimmed span holds no entries to skip, the
repeat report is already prevented by `trimBaseline`, and a cursor left lagging is
raised by `resumeWatermark` at the next seed. A field and two guarded call sites
for no observable difference.

Forcing an out-of-band stale-dependents sweep on detection was also dropped. It
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
- **A resume never replays a range retention deleted.** `TestResumeWatermark`
  covers the horizon in both branches.
- **The quiet pass is still one statement.** Nothing about the wake-driven cost
  argument changes.
- **Latency is unchanged.** This is an observability change: the wakes in the
  skipped span were, and still are, delivered by the stale-dependents pass.
