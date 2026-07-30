# A durable scan cursor for the dependency waker

- **Status:** Proposed — not implemented.
- **Date:** 2026-07-30
- **Touches:** `waker.go`, `internal/storeapi/storeapi.go`, `store.go`,
  `sqlite/store.go`, `sqlite/migrations/0001_init.sql`, `beehive.go`,
  `waker_test.go`, `sqlite/store_test.go`, `docs/reconcile-triggers.md`,
  `CLAUDE.md`, `TODO.md`.

## Problem

`waker.watermark` (`waker.go:44`) is already the cursor this spec is about:
store-wide, always increasing, never reused. It is not durable. `seed`
(`waker.go:86`) takes `ObjectWritesMaxVersion` at startup, so every restart
**discards the interval it was down for**: object writes committed while no
waker was running are below the new watermark and are never scanned.

The consequence is not divergence, because staleness is re-derived — a stranded
dependent is found by the stale-dependents pass comparing
`dependency_watermarks.reconciled_against` against its targets' versions (see
[the ADR](../docs/adr/2026-07-29-dependency-watermarks.md)). It is **latency**: a
dependent whose target moved across a restart waits up to
`staleDependentsInterval` (60s) where the waker promises 1s. The same shape
covers three cases that are separate today:

- a restart of any length,
- the startup seed race in `TODO.md` (a caller writes as soon as `Start` returns,
  below the watermark the waker then takes),
- a failed seed, which re-seeds from the cursor as of the *next* tick and skips
  everything committed in between.

Nothing here is a throughput problem. The waker's cost is already bounded by what
changed, and its in-memory cursor already keeps a tick from redoing a previous
tick's work within one process lifetime. **The whole benefit of this change is
across restarts.** The other five drivers do not want a cursor at all — see
[Non-goals](#non-goals).

**And it has a cost the naive version hides.** Seeding from `max` makes the first
scan free *by construction*: `ObjectWritesListSince(max)` returns nothing. A
stored cursor turns startup into O(writes during downtime), unbounded, on the
single connection, concurrently with the startup owed pass and the reconcile loops
a backlog is trying to feed. [Bounding the first
scan](#bounding-the-backlog-not-optional) is therefore part of the design, not a
refinement of it.

## Design

### Store surface: an optional capability, not a `Store` method

Add a capability interface beside `FreePagesReleaser`, not two more members on
`storeapi.Store`:

```go
// DriverCursorer is an optional capability a Store may implement to persist a
// driver's scan position across restarts. A Store that does not implement it
// leaves the dependency waker on its in-memory cursor, which is the behaviour
// that shipped before this existed.
type DriverCursorer interface {
    DriverCursorsGet(ctx context.Context, name string) (cursor int64, ok bool, err error)
    DriverCursorsSet(ctx context.Context, name string, cursor int64) error
}
```

Three reasons this is a capability rather than a contract change. `Store` is
externally implementable and each break is paid per break (see
[the write-shapes ADR](../docs/adr/2026-07-30-store-write-shapes.md)); this is a
latency optimisation over a mechanism that is already guaranteed, which is the
weakest possible case for a break. `fakeStore` in `testutils_test.go` needs no
new methods. And the degraded path is not a stub — it is today's tested
behaviour, so "unimplemented" has a correct meaning rather than a `panic`.

Re-export the alias from `store.go` next to `FreePagesReleaser`.

**On the `ok bool`.** Every other read on this surface signals absence with
`ErrNotFound`, and the only other `bool`-returning member is `EdgesHasIncoming`,
so this is a new idiom and needs a stated reason rather than an implementer's
guess. It is deliberate: a first run has no cursor, and that is the *normal*
state on every fresh database, not an exceptional one — a driver that must call
`errors.Is(err, ErrNotFound)` on its ordinary path is being handed an error for a
fact. `ErrNotFound`'s own godoc scopes it to "no object matches", and a driver
cursor is not an object. The narrower alternative — return 0 for absent — is
wrong here for the same reason `waker.seeded` exists: zero is a legitimate cursor
value on an empty store, so it cannot also mean "no cursor".

### Schema (added to `0001_init.sql`)

The project is unreleased, so there is no on-disk database to migrate and no
compatibility to preserve. This table belongs in `0001_init.sql` beside
`dependency_watermarks`, not in a second migration file — a `0002` would exist
only to record a schema history that, pre-release, nobody depends on. Add the
`CREATE TABLE` there now; start numbering real migrations at `0002` once
something has actually shipped against `0001`.

```sql
CREATE TABLE driver_cursors (
    name       TEXT PRIMARY KEY, -- driver identity, not a kind; see waker.go
    cursor     INTEGER NOT NULL, -- resource_version scanned through, inclusive
    updated_at INTEGER NOT NULL  -- millis; moves only with cursor
) STRICT, WITHOUT ROWID;
```

`WITHOUT ROWID` because the key is `TEXT`: a rowid table would build a separate
unique index to enforce the primary key and store every name twice, once in the
record and once in that index. This is *not* the `edges` rationale
(`0001_init.sql:202`) — that one is "every column is in the key" — nor is it the
case `dependency_watermarks` makes for the rowid form, which turns on an
`INTEGER PRIMARY KEY` aliasing the rowid. Neither applies to a text-keyed table
with two payload columns. At one row the storage class is immaterial in practice;
the reason is written down so it is not read as either precedent.

One row today (`"dependency_waker"`). The table is named for drivers in general
because a second one would otherwise arrive as a second table, not because any is
planned.

No foreign key, no `ON DELETE CASCADE`: the cursor refers to a position in the
write log, not to a row that can be collected.

`DriverCursorsSet` is a monotone, self-suppressing upsert, the same shape as
`DependencyWatermarksSet` (`sqlite/store.go:1085`):

```sql
INSERT INTO driver_cursors (name, cursor, updated_at) VALUES (?, ?, ?)
    ON CONFLICT(name) DO UPDATE
   SET cursor = excluded.cursor, updated_at = excluded.updated_at
 WHERE excluded.cursor > driver_cursors.cursor
```

The `WHERE` does both jobs there: it makes the stored cursor monotonic, so an
out-of-order write cannot regress it into replaying history, and it suppresses
the write entirely when the cursor has not moved — no page dirtied, no WAL frame.
A suppressed upsert is **not** an error and reports nothing, so a caller cannot
tell it happened; see `persisted` below for why that is fine and why it still has
to be said.

### Waker changes

`waker` gains two fields: `cursors DriverCursorer` (nil when the store does not
implement it) and `persisted int64`, the cursor value last written. Both are
touched only by the waker goroutine, like the existing fields, so neither needs a
lock. Assign `cursors` once where the waker is constructed (`beehive.go:379`)
rather than asserting at the use site as `freePagesSweep` does (`beehive.go:258`):
the waker already owns per-run state that has an owner for exactly this reason,
and a type assertion per tick to reach it would be the odd choice here even
though the sweeper's is right there.

**Seed.** `seed` reads the stored cursor first:

```
stored, ok, err := cursors.DriverCursorsGet(ctx, cursorNameWaker)  // skipped when cursors == nil
max, err := store.ObjectWritesMaxVersion(ctx)                     // as today
watermark = max
if ok { watermark = min(stored, max) }   // and then the backlog bound, below
persisted = watermark
```

A read error on either call leaves the waker unseeded and retries on the next
tick, exactly as today — `TestWakerRetriesSeedOnTheNextTick` still governs, and
must stay green through a failure of *either* read.

`persisted = watermark` at seed, never 0. That matters precisely because of the
clamp: with `stored = 100` and `max = 90`, a `persisted` of 0 would make every
tick issue a `DriverCursorsSet` that the SQL `WHERE` silently discards — the
"nothing written on an idle store" property survives, but one wasted round trip
per second on the single connection does not. Seeding does not itself write the
cursor: there is no progress to record, and on a fresh store the fallback to `max`
is the behaviour a missing row already produces.

The consequence of seeding `persisted` from `stored` is that the Go-side value
can sit *above* the row whenever the clamp bit — the suppressed upsert leaves the
row at 100 while `persisted` walks up from 90. That is harmless in the only
direction it can fail: the row is too *high*, so a later start clamps against
`max` again and re-derives the same answer. It is written down because "persisted
tracks the row" is the natural assumption and it is not true.

**Persist.** The write goes in a `defer` at the top of `scan`, not after the
paging loop. Every exit from that loop is a `return` — the error path, the empty
page, a failed `dependentsWake`, and the short page that is the overwhelmingly
common one — so an end-of-loop write would be unreachable code. `defer` also
gets the case an end-of-loop write would have got wrong: an error exit has
usually already advanced the watermark past earlier *successful* pages, and that
progress is exactly what should survive.

```go
func (dw *waker) persist(ctx context.Context) {
    if dw.cursors == nil || dw.watermark <= dw.persisted || ctx.Err() != nil {
        return
    }
    if err := dw.cursors.DriverCursorsSet(ctx, cursorNameWaker, dw.watermark); err != nil {
        dw.bh.log().WarnContext(ctx, "persisting the dependency waker's cursor failed; the next tick retries it, and a restart before then re-scans from the stored cursor", "watermark", dw.watermark, "err", err)
        return
    }
    dw.persisted = dw.watermark
}
```

The `ctx.Err()` check is not defensive noise: `stop` cancels this ctx, so without
it every shutdown mid-scan logs a warn for a write that failed for no reason of
its own. The rest of the waker treats a cancelled ctx as "not a loss" and this has
to match.

The unseeded early return in `scan` happens *before* the `defer` is installed,
which is correct — a tick that only seeded has nothing to record.

A failed write never rolls the in-memory watermark back. The wakes for those pages
are already queued; re-queueing is the cheap direction and re-scanning is not.

The write is a bare statement, never wrapped in `Within`. It participates in no
transaction and must not: see [below](#why-this-is-sound-without-being-transactional).

### Bounding the backlog (not optional)

Two bounds, because the failure they prevent is the one thing this change makes
worse than the code it replaces.

**A per-tick page budget.** `scan` currently pages to exhaustion. Cap it at
`wakeScanPagesPerTick` (16, so 4096 changes) and return; the cursor is persisted
by the `defer`, so the next tick continues where this one stopped instead of
re-reading. This keeps one tick from monopolising the connection the reconcile
loops need, and it is what makes the middle persist cadence fall out for free —
per-page writes would put a row write behind every page of a backlog, per-tick
alone would let a long drain discard minutes of scanning on a crash, and a
bounded tick makes those the same thing.

**A seed distance jump.** If `max - stored` exceeds `wakeSeedBacklogCap`, log at
warn and seed from `max` anyway. The distance is only an estimate of the row count
— the event log draws from the same counter, and deletes remove rows the scan
would have skipped — which is fine for a threshold and should be said so nobody
tunes it as though it were exact.

The jump is not a compromise; it is the backstop doing its stated job. Every
dependent the skipped range would have woken is still found by the
stale-dependents pass, which re-derives staleness from durable per-dependent
state and needs no cursor. Trading "the first tick issues thousands of round
trips" for "these dependents converge within 60s instead of 1s" is the trade this
whole design already rests on. It is also what covers the case the clamp does
*not*: a database file swapped for a larger one has `stored < max`, so the clamp
never fires and the distance bound is the only thing standing between startup and
a full-history replay.

Note what the clamp actually costs, since the obvious framing is backwards: it
costs **nothing, ever**. After clamping to `max`, `ObjectWritesListSince(max)`
returns an empty page by definition, so there are no pages to walk and no edge
lookups to pay for. The replay exposure runs the other way — pages scanned but
not yet persisted when the process died — and it is bounded by the persist
cadence above, not by the clamp.

## Why this is sound without being transactional

The obvious worry is that a durable cursor commits progress the process then
loses, since a wake is an entry in the in-memory `workQueue` and nothing more.
That is real, and it is already the contract:

- **The cursor may run ahead of the wakes, by design.** After `dependentsWake`
  returns, the wake exists only in memory. Persisting the cursor makes a crash
  lose those wakes *without* the next start re-scanning them — where today the
  next start skips them for a different reason (it seeds from `max`, which is
  above them). The exposure is unchanged in kind, and strictly smaller in size.
- **What covers it is the stale-dependents pass, unchanged.** It re-derives
  staleness per dependent from durable state and is deliberately not driven by
  any cursor, which is precisely what makes it immune to the class of bug this
  spec introduces. A wake lost with the process costs latency until that pass —
  the same sentence the waker's doc comment already carries (`waker.go:26`).
- **So this change must not be read as making the waker a guarantee.** It is
  still an optimisation over the backstop. Do not use it to justify widening
  `staleDependentsInterval` or making that pass configurable; the correct reading
  is that the waker's *own* losses now heal at 1s across a restart instead of at
  60s.
- **And it must not become transactional.** Committing the cursor inside the
  reconcile transaction of every woken dependent would couple a store-wide driver
  cursor to per-object work, serialise the scan behind reconciles, and buy nothing
  the backstop does not already provide.

## Decided, and failure modes

- **`driver_cursors` is single-writer, and that is the whole answer to the
  multi-process question.** Two processes on one database file would share the
  row, each consuming pages the other then never sees, and each queues only its
  own registered kinds — so a change whose only dependent is registered in the
  other process can be scanned away. But nothing claims multi-process support:
  `Open` sets `SetMaxOpenConns(1)` (`sqlite/sqlite.go:49`), and neither
  `README.md`, `docs/` nor `CLAUDE.md` describes two embedding processes sharing a
  file. Document the table as single-writer in the migration comment and move on.
  Keying the row by a per-process identity is rejected: it gives up the durability
  that is the entire point. (Should multi-process ever be supported, the degraded
  behaviour is bounded rather than broken — each process runs its own
  stale-dependents pass, so a stolen page costs 60s.)
- **`OpenMemory`** gets the table from `0001_init.sql` like everything else, and
  the cursor is per-process by construction. Nothing to decide; worth one test so
  the in-memory path is known to exercise the write.
- **The seed race in `TODO.md` narrows to first run only.** With a stored cursor,
  writes committed between `Start` returning and the seed goroutine being
  scheduled sit *above* it and are scanned. The race survives only on the very
  first start of a fresh database, where the fallback is `max`. That weakens the
  case for the synchronous-seed fix rather than replacing it, so update that item
  — and note the other direction too: a synchronous seed would now read *two*
  rows inside `Start`'s critical section, doubling the exact hesitation that item
  records. The next person should not have to rediscover that this change makes
  the remaining fix slightly more expensive to justify, not less.

## Non-goals

Named because the natural next question is "why not all six drivers", and the
answer is that five of them would be made worse:

- **Owed pass** (`ObjectsListUnsettledIDs`, `ReconcileOwedListIDs`) — a predicate
  on current state, and self-clearing: a settled object leaves the listing, so
  there is no repeated work to skip. A version cursor would also be *unsound*
  here, because `EdgesAdd`'s `reconcile_owed` increment bumps no
  `resource_version` — a cursor scan cannot see the very stamp that makes owed
  work crash-safe.
- **Full pass** — its contract is every object; a cursor makes it a different
  driver.
- **GC sweeper** — self-clearing too, and the transitions that unblock a
  deletion-pending row are invisible to a version cursor: "nothing references me
  any more" changes when *another* row is deleted, and a deleted row draws no
  version. A cursor would strand blocked rows, which is the one outcome
  `WithGCInterval` is non-disableable to prevent.
- **Stale-dependents pass** — already has the durable watermark, per dependent
  (`dependency_watermarks`), and its listing self-clears. A global cursor on top
  would restrict it to "targets that moved since X", i.e. reimplement the waker,
  and forfeit the independence that makes it a backstop.
- **Watch poll** — per-stream marks are in memory because the subscriber is too;
  on restart there is no stream to resume. A resumable "watch from version X" is a
  feature request for consumers, not a driver optimisation.

Also out of scope: the unpaged listings (`DeletionRequestsList`,
`ObjectsListUnsettledIDs`, `ReconcileOwedListIDs` all return the full set in one
query). That is the larger scaling cost in the driver set and it needs no durable
state; it belongs in its own change.

## Testing

Whitebox in `package beehive`, signals not sleeps, per the conventions.

- `TestWakerResumesFromTheStoredCursor` — write, stop, write again while down,
  start: the dependent of the second write is woken without waiting for the
  stale-dependents pass.
- **Rename `TestWakerSeedsFromTheStoreCursor` → `TestWakerSeedsFromTheWriteLogMax`**
  (`waker_test.go:67`) as part of this change. "Store cursor" currently means
  `ObjectWritesMaxVersion`; after this lands it reads as the stored *driver*
  cursor, sitting next to a new test that means precisely that. Renaming is not
  cosmetic here — leaving both names in one file is how the next reader gets the
  seed order backwards.
- `TestWakerSeedsFromMaxWithoutAStoredCursor` — first start on a fresh store keeps
  today's behaviour.
- `TestWakerClampsAStoredCursorAboveTheMark` — stored cursor above
  `ObjectWritesMaxVersion` (delete the top-versioned row) clamps rather than
  resetting to zero, and issues no listing.
- `TestWakerJumpsAnOversizedBacklog` — `max - stored` past
  `wakeSeedBacklogCap` seeds from `max` and logs; assert no page was read.
- `TestWakerStopsAtThePageBudget` — a backlog longer than
  `wakeScanPagesPerTick` returns after the budget with the cursor persisted, and
  the next tick continues rather than re-reading.
- `TestWakerRetriesSeedOnTheNextTick` — unchanged, and must stay green through a
  failure of either seed read.
- `TestWakerPersistsOnceWhenTheCursorMoves` / `…SkipsTheWriteWhenQuiet` /
  `…SkipsTheWriteOnShutdown` — count `DriverCursorsSet` calls through a store
  wrapper: a quiet tick writes nothing, a clamped seed writes nothing, and a scan
  cancelled by `stop` neither writes nor logs.
- `TestWakerPersistsProgressOnAFailedPage` — the `defer` records the pages that
  succeeded before the error.
- `TestWakerFallsBackWithoutTheCapability` — a `Store` that does not implement
  `DriverCursorer` behaves exactly as before.
- `sqlite/store_test.go`: monotonicity (a lower cursor is refused), suppression
  (`updated_at` does not move when the cursor does not), and absent-row
  `ok == false`. No migration-upgrade test is needed — there is no pre-existing
  database to upgrade.

**`sqlite/sqlite_test.go` needs nothing**, and that is worth stating so the next
reader does not add it. Its table lists (`:35`, `:47`) are spot-checks, not
inventories — they already omit `events` and `dependency_watermarks` — so adding
`driver_cursors` to them would imply an exhaustiveness those assertions do not
have and do not want.

## Documentation on landing

Per the [ADR README](../docs/adr/README.md), records describe code that exists:
when this lands, fold it into an ADR (or into
[the dependency-watermarks ADR](../docs/adr/2026-07-29-dependency-watermarks.md),
which already carries the waker-versus-backstop argument) and **delete this
file**. Also update `docs/reconcile-triggers.md` (the waker's restart answer
changes), the waker bullet in `CLAUDE.md`, and the two `TODO.md` items named
above.
