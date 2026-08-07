# The dependency waker abandons a drain the stale-dependents pass has overtaken

- **Status:** Accepted — implemented in `waker.go`.
- **Date:** 2026-08-05

## Context

The waker drains its backlog a page at a time and nothing bounded how long it
would keep doing that. After a long downtime `seed` resumes a cursor far behind
the write log, every pass reads its full `wakeScanPagesPerPass` budget, and
`scanGate` floors the passes 100ms apart — so a resume runs at up to 40 queries a
second on the one connection every commit and every reconcile loop shares, for as
long as the backlog lasts.

Past a point that work is redundant rather than merely slow. The stale-dependents
pass derives staleness from `dependency_watermarks` instead of from anything the
waker recorded, and its cursor is process-local and starts at 0, so the first
sweep of the process finds every stale dependent in the store — including every
one the drain is still working toward. After that sweep the remaining pages
deliver wakes for work already found, stamped `reconcile_owed` and enqueued.

## Decision

**Track how long the current run of budget-exhausting passes has been going, and
once it reaches `staleDependentsInterval`, stop paging and jump the watermark to
the write log's mark.** The skipped range is the backstop's to deliver.

Only continuous paging counts. A short page, an empty page, a failed page and a
failed edges lookup all end the drain, so an abandon takes a genuinely unbroken
drain — with the production cadences, ~2400 pages and ~614k rows. A restart whose
backlog is smaller than that never abandons at all. This is a safety valve for a
store whose write rate outruns the drain, not a change to the normal resume path.

### The bound is measured, not a distance

Capping `mark - stored` at seed was tried and removed. It reads a
`resource_version` distance, and `Events().Add` draws from that same sequence while
writing nothing the scan reads, so the distance overstates the backlog by an
unbounded factor: a store logging events at any rate would abandon cursors that
were a few passes of work. `TestWakerResumesAnEnormousBacklog` pins that seed
still resumes however far behind it sits — a measured bound leaves `seed` alone.

### Elapsed time, not a count of passes

The claim that justifies abandoning is about the backstop's *cadence*, so
wall-clock is the unit that states it. A pass count reaches the same place only by
dividing through `wakeScanMinInterval`, which tests set to 0 — a proxy that breaks
where the floor moves. `dw.now` is the waker's only clock, so the fake-clock tests
drive this the way they drive the gates.

### No option of its own

The threshold *is* `staleDependentsInterval`. A knob would be a guessed number
asking to be tuned, and the cadence it would be tuned against is already
configured. `abandonAfter <= 0` restores unbounded draining, which is what a
`Beehive` assembled field by field gets: `withStaleDependentsInterval` validates a
positive interval, but only for a `Beehive` that came from `New`.

### Two traps in the jump

**The mark folds in no horizon.** `ObjectWrites().MaxVersionAll` is a bare
`MAX(resource_version)`, unlike the per-kind `ObjectWrites().MaxVersion` the watch
tail gates on. Retention lowers it, and a fully trimmed log reads 0, so the jump
takes `max(watermark, mark)`. Assigning would rewind the watermark onto rows
already scanned and replay the whole log.

**A failed mark read is not `scanFailed`.** No wake depends on that read, and
`scanFailed` arms the retry backoff, which drops the wakes arriving meanwhile. The
pass returns `scanMore` and restarts the window instead. Holding `drainSince` there
would have been the obvious choice and is the wrong one: the next full-budget pass
is already past the threshold, so a read that keeps failing would be retried at the
wake rate — 10 full-log `MAX()` scans a second on the connection this change exists
to relieve, each with a warn line. Restarting the window costs one delayed shed and
paces both to once per threshold window.

`scan` keeps its `defer dw.persist(ctx)` above the split. In `scanPages` it would
run before the jump and persist the pre-jump watermark, and a restart would
re-drain exactly the range that was just abandoned.

## Consequences

The abandoned range costs at most two sweep intervals of latency, one in the
common case: reaching the threshold proves a sweep *started*, and a sweep that
fails a page holds its cursor for the next one. Never divergence — this is the
same trade three documented gaps already accept (a store that persists no cursor,
a wake queued but never delivered, a log trimmed under the cursor).

It leans on the backstop finding a superset of what the waker finds.
`Dependencies().ListStaleSince` drives from `objects t`, so a physically deleted
target's dependents would be invisible to it — but `edges.to_id` is `ON DELETE
RESTRICT`, so a target with live `depends_on` edges cannot be physically deleted,
and the waker finds nothing there either. Relaxing that constraint would turn this
abandon into a hole.

Both warn lines — the shed and the failed mark read — land at most once per
threshold window, which is the rate that makes them diagnosable without becoming
noise.

`scan` owns the drain's end (`drainSince` cleared on any result but `scanMore`) and
`abandonIfOvertaken` owns where its window starts. Splitting the two writers the
other way — clearing inside the jump — put the same invariant in two functions.

`BenchmarkWakerScanRateUnderSustainedWrites` is unaffected: its harness resets the
watermark per iteration, and the one extra `ObjectWrites().MaxVersionAll` an abandon
costs is not one of the reads it counts.
