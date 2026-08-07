# The dependency waker persists its scan cursor, as an optimisation over the stale-dependents pass

- **Status:** Accepted — implemented in `sqlite/migrations/0001_init.sql`
  (`driver_cursors`), `internal/storeapi/storeapi.go` (`Store.DriverCursors*`),
  `store.go`, `sqlite/store.go`, `waker.go`, `beehive.go`.
- **Date:** 2026-07-30

## Context

The waker's scan watermark (`waker.watermark`) was in-memory only. `seed` took
`ObjectWritesMaxVersion` at startup, so every restart discarded whatever interval
the process had been down for: writes committed while nothing was running sat
below the new watermark and were never scanned by this driver. That was not a
correctness gap — [the dependency-watermarks ADR](2026-07-29-dependency-watermarks.md)
had already made the waker "an optimisation, not a guarantee" by adding the
stale-dependents pass as a backstop that re-derives staleness from durable
per-dependent state and needs no cursor of its own. It was a latency gap: a
dependent stranded across a restart waited up to the stale-dependents interval
(60s) where the waker promises 1s.

That earlier ADR considered and rejected making the waker's *record* durable —
stamping `reconcile_owed` on every dependent it woke, advancing a cursor in the
same transaction — because that gives the waker a guarantee it cannot keep: a
promise checked only by the code that makes it, with a permanent hole wherever
the waker was not running or was wrong. It left the door open for a narrower
version:

> It can come back later as a pure startup optimisation, and the composition is
> sound *because* the watermark is the ground truth: a checkpoint that is stale,
> wrong, or written by a buggy build costs startup latency and nothing else.

This is that version, built on top of the ground truth rather than replacing it.

## Decision

**Persist the watermark; keep everything downstream of it exactly as
approximate as it already was.**

### Cursor persistence, a required `Store` member

```go
DriverCursorsGet(ctx context.Context, name string) (cursor int64, ok bool, err error)
DriverCursorsSet(ctx context.Context, name string, cursor int64) error
```

This began as an optional `DriverCursorer` the waker type-asserted, so a `Store`
that didn't implement it was simply not resumed across restarts — the waker's
original, tested behaviour. It is now required, because `ok=false` already says
exactly that: a backend that persists nothing reports absence forever, reseeds
from the write log's max every restart, and the stale-dependents pass covers the
gap as it always did. The optionality bought a nil check rather than any
semantics. See [the grouped-Store spec](../specs/2026-08-07-grouped-store-api.md),
D3. `ok bool` rather than
`ErrNotFound` marks absence as the normal first-run state rather than a fault;
`ErrNotFound`'s own contract scopes it to "no object matches", and zero is a
legitimate cursor value on an empty store, so it cannot double as "no cursor"
the way it can for `Get`/`GetOrCreate`.

The sqlite implementation is `driver_cursors`, one row per driver name (one row
today: `"dependency_waker"`), `WITHOUT ROWID` because the key is `TEXT` rather
than the `INTEGER PRIMARY KEY`-aliases-the-rowid case `dependency_watermarks`
makes its own rowid argument on. `DriverCursorsSet` is the same monotone,
self-suppressing upsert as `DependencyWatermarksSet`: a lower cursor is refused,
and a cursor that hasn't advanced dirties no page — load-bearing at a 1s tick
rate on a store that may otherwise be idle. Single-writer, and documented as
such in the migration comment: nothing in this project documents or tests two
embedding processes sharing one file, so a shared cursor row is a constraint to
keep true rather than a gap to close.

### Seed clamps rather than trusts or resets

`seed` reads the stored cursor and takes `min(stored, max)` against
`ObjectWritesMaxVersion`, never the stored value outright. `max` is a max over
*live* rows, so deleting the highest-versioned object legitimately lowers it
below a cursor the waker really did process — a stored cursor above `max` is
therefore not evidence of a swapped or truncated database. Clamping down costs
nothing: the first scan then asks for everything above `max`, which is empty by
definition, so it degrades to exactly the pre-existing seed-from-max behaviour.

### A per-tick page budget bounds the cost this adds

Seeding from `max` always made the first scan free by construction. A stored
cursor does not: resuming after a long gap means the first tick has real work to
do, on the single connection the reconcile loops and the startup owed pass also
need. `wakeScanPagesPerTick` caps how many pages one tick reads (16, so 4096
changes); the remainder is not lost, since the in-memory watermark carries it to
the next tick and the persisted cursor carries it across restarts.

**There is deliberately no second bound on how far behind a cursor may be before
`seed` abandons it.** An earlier revision had one, keyed on `max - stored`, and
it was wrong in a way no tuning fixes: that distance is in `resource_version`
units, and `EventsAdd` draws from the same sequence without writing anything
this scan reads. A store logging events at any rate inflates the gap by an
unbounded factor against the object rows actually behind it, so the threshold
fires on backlogs that were a few ticks of work and throws away a cursor that
would have drained fine. Nor does the case that motivated it survive scrutiny: a
cursor cannot have come from a *different*, larger database, because it lives in
the database it describes.

What remains is the genuine one — a restart after enough real downtime that
draining costs minutes of full-budget ticks. That is bounded per tick, and the
work is real rather than wasted, so it is a latency question rather than a
safety one. If it is ever observed to matter, the fix is to measure paging work
actually done rather than to guess from a version distance; `docs/TODO.md` carries
the shape.

### Persisted, not committed with the wakes

The cursor write is a bare statement, in a `defer` at the top of `scan` (every
exit from the paging loop is a `return`, so an end-of-loop write would be
unreachable), outside any transaction. It can therefore run ahead of the wakes
it produced: after `dependentsWake` returns, a wake exists only in the in-memory
`workQueue`, so a crash immediately after a persisted write loses those wakes
without the next start re-scanning them. That is a smaller version of the same
exposure seeding-from-max already had — where today's next start skips lost
wakes for a different reason (the watermark starts *above* them) — and it is
covered the same way: by the stale-dependents pass, unchanged. **This must not
be read as making the waker a guarantee**, and it must not become transactional:
committing the cursor inside every woken dependent's reconcile would couple a
store-wide driver cursor to per-object work and buy nothing the backstop does
not already provide.

`persisted` tracks what the *row* holds, which is not the same as where the scan
resumed: a clamp leaves the row above the watermark, so tracking the watermark
instead would make every tick issue a write the store's own upsert discards, a
round trip apiece on a connection every driver shares, until the watermark
climbed past it.

A failed write holds `persisted` where it is, which is what makes the next tick
retry — and, left there, what would make *every* tick retry, since the watermark
stays above it for good on a database that is read-only or full. So the retries
double to a one-minute cap and only the first failure of a streak warns, with
the streak length reported on recovery. The first retry stays immediate: a
transient error is the common case and one round trip is cheap. Nothing gives up
on the write, because nothing has to — a stalled cursor costs latency after a
restart and nothing else.

**`seed` writes the point it settled on, before any scanning.** An absent row is
what makes the next start seed from the mark as of *then*, so a run that seeds
and stops without ever seeing a write would otherwise leave its successor to skip
everything committed in between — this cursor's whole purpose, defeated for the
entire first run of a fresh store. That write includes a cursor of zero, which is
a real position (an empty write log) rather than an absence, so `persisted`
starts at a `noStoredCursor` sentinel below every valid cursor rather than at
zero. Once the row exists the write is suppressed on both sides — `persisted`
short-circuits it in Go, and the upsert's `WHERE` would discard it anyway — so
this costs one write per fresh store, not one per start.

## Consequences

**The seed race in `docs/TODO.md` narrows to first run only.** With a stored cursor,
writes committed between `Start` returning and the waker's seed goroutine being
scheduled sit *above* it and are scanned on the next tick. The race survives
only on the very first start of a fresh database, where there is nothing stored
yet and the fallback is `max`, as before. That weakens the case for seeding
synchronously in `Start` without removing it — and a synchronous seed would now
read *two* rows (`ObjectWritesMaxVersion` and `DriverCursorsGet`) inside
`Start`'s critical section instead of one, which is the same hesitation that
item already records, doubled.

**Multi-process sharing one database file is unsupported, and this cursor is one
of the reasons.** Two processes would share the one cursor row, each consuming
pages the other then never sees, and each queues only its own registered kinds —
so a change whose only dependent is registered in the other process is scanned
away. The stale-dependents pass would usually cover it, but nothing here relies
on that and nothing tests it. The store is owned by one process running one
`Beehive`; see
[one process, one Beehive](2026-08-05-one-process-one-beehive-sole-writer.md).

**Cost is otherwise unchanged in the common case.** A quiet store writes
nothing; a normally-running process writes one small row per tick that actually
saw new writes, on the same connection those writes already serialise against.

### Rejected alternatives

Considered and rejected in the earlier ADR, and unaffected by landing this:
stamping `reconcile_owed` per wake with a transactional cursor advance (still
gives the waker a guarantee it cannot keep, and is strictly more expensive:
writes proportional to change events × dependents rather than to coalesced
reconciles). Not reconsidered here, because nothing about persisting the
optimisation changes that argument — only a startup-latency checkpoint composes
safely with a backstop that treats it as disposable.
