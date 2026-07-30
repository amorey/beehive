# The dependency waker persists its scan cursor, as an optimisation over the stale-dependents pass

- **Status:** Accepted — implemented in `sqlite/migrations/0001_init.sql`
  (`driver_cursors`), `internal/storeapi/storeapi.go` (`DriverCursorer`),
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

### `DriverCursorer`, an optional capability

```go
type DriverCursorer interface {
    DriverCursorsGet(ctx context.Context, name string) (cursor int64, ok bool, err error)
    DriverCursorsSet(ctx context.Context, name string, cursor int64) error
}
```

Not a `Store` member, for the same reason `FreePagesReleaser` is not: a `Store`
that doesn't implement it is simply not resumed across restarts — the waker's
original, tested behaviour — rather than every implementation and test double
having to answer a question only some of them need to. `ok bool` rather than
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

### Two bounds close the cost this adds

Seeding from `max` always made the first scan free by construction. A stored
cursor does not: resuming after a long gap means the first tick has real work to
do, on the single connection the reconcile loops and the startup owed pass also
need. Two constants bound that:

- `wakeScanPagesPerTick` caps how many pages one tick reads (16, so 4096
  changes). The remainder is not lost — the cursor persists at whatever the tick
  reached, and the next tick resumes there.
- `wakeSeedBacklogCap` makes `seed` give up resuming a cursor that is too far
  behind `max` and jump straight to `max` instead, logging the gap. This is not
  a compromise: every dependent in the skipped range is still found by the
  stale-dependents pass, which needs no cursor, so the trade is "the first tick
  pages through the whole gap" for "these dependents converge within one
  stale-dependents interval instead of one wake-interval tick" — the same trade
  the per-tick budget makes one tick at a time, taken all at once for a gap large
  enough that draining it a page at a time would take many minutes.

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

`persisted`, the last value actually written, seeds from the same value as
`watermark` at seed time, never from zero — a clamped seed (stored cursor above
`max`) would otherwise make every quiet tick issue a write the store's own
upsert just discards, paying a round trip for nothing on a connection every
driver shares.

## Consequences

**The seed race in `TODO.md` narrows to first run only.** With a stored cursor,
writes committed between `Start` returning and the waker's seed goroutine being
scheduled sit *above* it and are scanned on the next tick. The race survives
only on the very first start of a fresh database, where there is nothing stored
yet and the fallback is `max`, as before. That weakens the case for seeding
synchronously in `Start` without removing it — and a synchronous seed would now
read *two* rows (`ObjectWritesMaxVersion` and `DriverCursorsGet`) inside
`Start`'s critical section instead of one, which is the same hesitation that
item already records, doubled.

**Multi-process sharing one database file degrades to the backstop, not to
breakage.** Two processes would share the cursor row, each consuming pages the
other then never sees, and each queues only its own registered kinds — so a
change whose only dependent is registered in the other process can be scanned
away. Nothing here claims that configuration works today, so this is a
documented constraint rather than a regression: each process still runs its own
stale-dependents pass, so a stolen page costs 60s rather than forever.

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
