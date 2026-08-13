# Event retention is a ring per timeline, and it is off by default

- **Status:** Accepted — implemented in `options.go`, `beehive.go`,
  `sqlite/store.go`.
- **Date:** 2026-08-06

## Context

`WithEventRetention(perObject, maxAge)` was one option enforced by one sweep,
and neither its name nor its godoc carried its shape. `perObject` was not per
object — it capped each `(object, category)` timeline — and it did not count
events, it counted runs. Both bounds were off by default, so the stock
configuration was an unbounded log. Meanwhile the cap's partition had become
load-bearing elsewhere: `events_horizon` is keyed to match it, because a
horizon has to describe what a trim actually deleted or a resume is refused for
a hole in a timeline it never read.

None of that was derivable from the code, which is what made it an audit rather
than a bug fix.

## Decision

### The log is a bounded ring per timeline, plus an optional recency window

Both bounds stay, because they bound different things. `maxAge` bounds age, not
size: inside the window the log grows with reconcile rate, since a controller
emitting a distinct `(type, reason)` per reconcile appends a run per reconcile.
The ring keeps the log proportional to the object count instead, which is the
failure mode an event log meets first. Dropping the ring would collapse the
window function, the `events_horizon` key and `Events().ListSince`'s `category`
parameter into something simpler — and would leave no bound that survives a
flapping controller.

The cap partitions by `(object, category)` and the horizon key matches it. The
partition was inherited from run aggregation rather than chosen for retention,
and it is kept deliberately: it is what stops a chatty timeline evicting a quiet
one on the same object, and what lets a resume be refused per timeline instead
of per object.

The unit is a **run**. An extend grows a run in place, so occurrences are
unbounded within one; rows are what cost disk, and rows are what the cap counts.
`perObject` is now `perTimeline`, which names the partition the way
`WithWriteLogRetention(perKind, …)` names its own.

### A sweep costs what is over cap, not what the log holds

The ring's first implementation ranked the whole table with a window function,
and `trimEvents` evaluated that predicate twice — once for the horizon, once for
the delete — inside one transaction holding the single write connection. It paid
the same price whether or not anything was over cap.

Instead the sweep asks which timelines are over cap
(`GROUP BY object_id, category HAVING COUNT(*) > ?`, index-only on a key leading
with those columns) and trims each by a seek. On a 2048-run log with nothing to
trim: **5.9ms → 0.48ms**. `TestEventsSweepSelectsCandidatesByIndex` pins the
plan, since a temp B-tree there would silently restore the sort.

A budget bounds the timelines one sweep trims. The scoped statements are seeks,
but an unbounded backlog of them is still an unbounded hold on the write
connection. Retention is therefore progressive across sweeps, which the watch
contract already tolerates: the horizon only ever rises, and the cap was never
tight to begin with — it is enforced on the GC interval, so a burst sits above
it until the next sweep either way.

The budget is a parameter on `Events().Sweep`, not a constant, because it is work
per *sweep* against a cap the caller thinks of per unit time: the GC loop scales
it with `WithGCInterval`, so a sweeper on a long cadence still trims at the same
rate rather than leaving the log over its cap indefinitely. `eventCapBudget`
(256) is what an implementation applies when the caller names none, and it is
the number the GC loop scales from.
→ [the cadences ADR](2026-08-06-driver-cadences-are-configurable.md).

### Both bounds stay off by default

Measured, on disk, over a log where nothing is over cap:

| Timelines | Runs | Quiet sweep |
| --- | --- | --- |
| 512 | 2,048 | 0.48 ms |
| 4,096 | 16,384 | 3.29 ms |

The candidate query is index-only but still counts every row, so a quiet sweep
scales with log size rather than with what it trims — roughly 200ms per million
runs, every GC interval, on the connection the writers need. That is affordable
and it is not free, and a default that trims is a default that deletes data
nobody asked to delete, on a log an embedder may be keeping deliberately.

So retention stays opt-in, and the audit closes on the documentation rather than
on a default: `WithEventRetention`'s godoc now states the unit, the partition,
the cutoff and the enforcement granularity, which is what was missing.

Turning the ring on by default remains available, and would want the sweep to be
O(over-cap) first — a per-timeline run counter maintained by `Events().Add` would
do it, at the cost of a table and a write in the hot event path. Not built,
because nothing today pays the O(size) tick: the sweep is a no-op unless a bound
is set.

## Consequences

The horizon over-reports in two ways, both the safe direction, both now pinned
by tests. A reader filtering on type, reason or time filters client-side, so the
horizon cannot account for it and refuses a resume for a trim of runs that
reader would have dropped (`TestEventsWatchHorizonIgnoresClientSideFilters`).
And the ring orders by `last_at DESC, id DESC` while cursors order by
`resource_version`, so same-millisecond extends can leave a survivor below a
trimmed run. The alternative in both cases is vouching for an absence the store
never checked.

A `nil` category is **not** an over-report: it means "every category
interleaved", so a trim anywhere is a real loss and the horizon's `MAX` across
timelines is exactly right (`TestEventsWatchHorizonIsPerTimeline`).

`maxAge` remains a flat cutoff over the table where the cap partitions, and it
reads a run's *end*, so a run that keeps being extended never ages out. That is
the recency-window semantics; it is not a second size bound.

## The bound is readable from the stream

`EventStream.Retention` reports the configured bounds. A watch consumer that
holds runs in memory grows past what the store keeps unless it caps its own
list, and to cap it correctly it needs this number — without it on the stream,
callers hardcode a mirror of the server's config that goes silently wrong when
the config changes.

It is a readout of the option, not a per-stream fact, and it is deliberately not
a prune signal: the stream stays "the snapshot, and what grows above it", and
what a trim can actually cost a reader is already answered by the horizon and
`ErrWatchTooOld`. An unenforced bound reads zero — the sweeper gates on `> 0`,
so a negative configured value is reported as unset, and the field says what is
enforced rather than what was passed.

It sits on the stream rather than on `Client` because the caller who needs the
number is the one already holding the struct that grows, so the bound and the
position it bounds arrive together. `ListEvents` has the same gap and no
additive place to put it; a `Client` accessor would be the answer, and would
read this same configuration rather than a second copy of it.
