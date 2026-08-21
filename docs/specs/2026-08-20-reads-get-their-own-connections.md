# Reads get their own connections

- **Status:** Planned. Independent of
  [cache prepared statements](2026-08-20-cache-prepared-statements.md) — an
  earlier draft of this spec claimed to gate it, on a misreading corrected under
  *The tax, accurately*. [A read-only transaction](2026-08-20-a-read-only-transaction.md)
  does depend on it.
- **Date:** 2026-08-20
- **Supersedes:** the "Reads and writes share one connection" item in
  [`TODO.md`](../TODO.md), which this builds.

## Why

`OpenPool` sets `journal_mode(WAL)`, which lets one writer and many readers run
at once. Then `sqlite.Open` passes `maxConns = 1`, so every read queues behind
every write in Go's pool and that concurrency is unused. The limit is
`database/sql`, not SQLite.

**A live watch slows writes.** `BenchmarkWritesUnderWatch`, on disk, beehive not
started:

| watches | ns/op |
|---|---|
| none | 172,000 |
| 1 kind, 1 watch | 215,000 |
| 1 kind, 64 watches | 223,000 |
| 16 kinds, 1 watch each | 326,000 |

Watch *count* is free — the shared tailer working as designed — and watched
*kinds* is the axis that costs, because each tailer wakes with nothing to
coalesce and sixteen drains contend for the one connection. That row is the worst
case by construction; the deltas are the finding.

**The drivers read on a cadence forever.** The owed pass, the stale-dependents
pass, the waker and the watch floors all re-read the same indexes on a fixed
schedule, and every one of those reads sits in the writers' queue today.

## The tax, accurately

Holding prepared statements open on a connection slows work on that connection —
but only work that is *itself* still being prepared per call. Ten indexed reads in
one transaction, on disk, one connection:

| | 0 held | 20 held | 70 held |
|---|---|---|---|
| uncached | 91.8 µs | 114.5 µs (+25%) | — |
| cached | 40.6 µs | 43.3 µs (+7%) | 43.9 µs (+8%) |

So the tax is the cost of preparing on a busy connection, not a cost of holding
statements. A cached statement barely pays it and does not scale with the held
count. **Caching more is what avoids it; caching elsewhere is not.**

An earlier draft of this spec read the top row alone and concluded that statement
caching needed the split to pay. That was wrong, and it is worth recording because
it reordered the whole project for a day: the configuration that regressed was one
where cursor-holding reads were left *uncached* inside a transaction to dodge a
corruption hazard, so they paid the tax and collected nothing. The fix for that is
in the caching spec, and it is not this one.

What survives is real and much smaller. SQL that can never be cached pays the tax
on a busy connection: the builder-rendered batch statements, and `BEGIN` /
`COMMIT` / `SAVEPOINT`. That is an argument about the batch paths, not a reason to
sequence anything.

## The change

Two pools over one file.

- **The writer** is what exists today: `OpenPool(path, 1)`, `_txlock=immediate`.
- **The reader** is a second pool with `_pragma=query_only(true)` and N
  connections.

`query_only`, not `mode=ro`, because it is genuinely enforced and reports why:
both `INSERT` and `BEGIN IMMEDIATE` fail with `attempt to write a readonly
database (8)`. That is the reason to prefer it.

`TODO.md` gives a different one — that `mode=ro` "cannot recover the `-wal`/`-shm`
files" — and it **does not reproduce**: a database left with a hot 836 KB WAL
opened and read correctly under `mode=ro`, with the `-shm` present, with it
removed, and in a read-only directory. Do not carry that claim forward, and do not
write a test for it: the test would pass under either DSN.

**The reader's DSN must not carry `_txlock=immediate`.** `BEGIN IMMEDIATE` takes a
write lock, which `query_only` refuses, so every transaction on the reader would
fail.

`OpenPool` bakes that flag in, and its godoc actively invites the mistake —
"maxConns caps the pool: 1 for a writer pool, larger for a WAL reader pool". Give
it a sibling, `OpenReadPool(path, n)`, rather than a parameter: the two DSNs
differ in two pragmas and a txlock, and a boolean at the call site says less than
the name does. **Fix that godoc in the same PR**; the moment this lands it
describes a pool nobody should build.

`s.conn(ctx)` keeps its job. A sibling selects the reader:

```go
// read returns the connection a read-only statement runs on: the ambient
// transaction while it is live, else the read pool.
//
// Returning the transaction is not an optimisation — it is the whole safety
// property. A read issued inside a transaction on any other connection sees the
// database as of before that transaction's own uncommitted writes.
//
// "While it is live" is the second half, and it is the half that is easy to
// drop: a closed txState degrades to the pool exactly as conn does, because a
// ctx outlives its transaction. Written as `if fr, ok := txFrom(ctx); ok`, every
// read on a hook's ctx returns sql.ErrTxDone.
func (s *sqliteStore) read(ctx context.Context) dbtx
```

Both accessors want the same frame lookup, so factor it once — `liveTx(ctx)
*txState`, nil when there is no live transaction — rather than writing the
`isClosed` check twice and letting the two drift.

Every read-only method moves from `s.conn(ctx)` to `s.read(ctx)`. Methods that
read as part of a write — `updateSpec`'s resolve, `Conditions().Set`'s gate,
`Events().Add`'s latest-run lookup — need no thought: they run inside a
transaction, so `s.read` hands them that transaction.

## Configuring N

`sqlite.Open` gains options:

```go
// Open opens (or creates) a Beehive SQLite database at path, running any
// pending schema migrations before returning.
func Open(path string, opts ...Option) (*sqliteStore, error)

// WithReadConnections sets how many connections serve reads. Reads run on their
// own pool, so this bounds read concurrency, not total connections: the writer
// is always one. Below 1 is ErrInvalidOption.
func WithReadConnections(n int) Option
```

Default **4**. It is a guess, and should be named as one in the godoc — one
connection already buys the whole win against writers, and N past that only helps
readers that genuinely overlap. The drivers do overlap: the owed pass runs per
kind, the tailers per watched kind, the event readers per watch.

This is `sqlite`'s own option type, not `beehive`'s. The store is constructed by
the embedder and handed to `beehive.New`, so beehive's option machinery never
sees it; say so where the type is defined, because the repo has exactly one
option convention today and this is deliberately not it.

**On the reader, `SetMaxIdleConns` must be N and `SetConnMaxIdleTime` left off.**
`database/sql` keeps two idle connections by default, so a pool of four would
close and reopen two of them continuously. `OpenPool` also sets a five-minute
`ConnMaxIdleTime`, which reaps idle readers on its own — a quiet beehive would
drop reader connections between ticks. Fixing only the idle *count* leaves that
churn in place.

**The writer keeps its five minutes here**, because nothing is held on it and
reopening one idle connection costs nothing. That stops being true the moment
statements are cached on it, so the caching spec owns revisiting it — noted in
both.

**`ErrInvalidOption` lives in the root `beehive` package** (`options.go:36`), so
returning it from `sqlite` would make the store import the control plane and
invert the dependency `storeapi` exists to keep clean. Use a sentinel in
`storeapi`, or one local to `sqlite`.

## The rule everything rests on

**`s.read(ctx)` returns the ambient transaction whenever one is present.**

Today a read issued outside its transaction while inside one *deadlocks*: it
waits for the connection the transaction holds. Loud, deterministic, and stated
as a caller-facing rule in `Client.Watch`'s godoc. With a read pool the same
mistake becomes a **silent stale read** — the reader sees committed state, the
transaction's own writes are not committed yet, and nothing fails.

That is the same failure category as the shared-cursor bug in
[cache prepared statements](2026-08-20-cache-prepared-statements.md) — a rule
that reads fine and returns wrong data — which is why this spec asks for a test
that can fail rather than an invariant in a comment.

## Edge cases the implementer would otherwise guess at

- **`OpenMemory` cannot be split, so `WithReadConnections` is a no-op there.**
  `file::memory:` is per-connection, so a second pool is a different and empty
  database. The reader falls back to the writer, which means the suite runs
  today's semantics and **only on-disk tests cover the split**. Say so in the
  option's godoc: silently ignoring a value the caller passed is the kind of thing
  this repo writes down. Do not reach for `mode=memory&cache=shared` to avoid
  that: shared-cache mode changes SQLite to table-level locking and introduces
  `SQLITE_LOCKED`, so it would be testing a different database than production.

- **A prepared statement belongs to the pool that prepared it.** Nothing here
  prepares anything, but
  [cache prepared statements](2026-08-20-cache-prepared-statements.md) does, and a
  statement prepared on the writer then executed through `s.read` runs *on the
  writer* — silently undoing this change. Whichever of the two lands second owns
  keeping the table per pool.

- **Migrations run on the writer, before the reader opens.** A reader opened
  first would cache a schema that does not exist yet.

- **`Close` closes both pools**, reader first. It stays idempotent.

- **`ReclaimSpace` stays on the writer.** It takes its own `*sql.Conn` and
  `PRAGMA incremental_vacuum` writes.

- **A long read transaction holds back WAL checkpointing**, since the WAL cannot
  be truncated past the oldest reader's snapshot. Nothing to bound here: neither
  paging drain opens a transaction — `waker.go`'s `scanPages` reads each page in
  autocommit, and neither `waker.go` nor `objectswatch.go` contains a `Within`
  call. The exposure arrives with
  [spec 2](2026-08-20-a-read-only-transaction.md), alongside the stale-snapshot
  hazard below, and belongs there.

- **Budgets justified by "the single connection" need re-deriving, not
  assuming.** The waker's page budget exists so a resume "cannot monopolise the
  single connection" (`waker.go:101`, `:105`, `:428`); the tailer reads "one
  after another on the single connection" (`objectswatch.go:583`); `workqueue.go`
  reasons about a deadlock on it. Each is still defensible, but for a different
  reason, and a reader who finds the old reason will believe the wrong thing.

- **The reader sees committed state only, and whole transactions.** A read
  outside a transaction may lag a write in flight on the writer, so the two
  connections do not see the same instant. What makes that harmless is that a WAL
  snapshot never shows half a transaction: every record written beside its horizon
  — `object_writes` with `object_writes_horizon`, `events` with `events_horizon` —
  stays consistent across the split, because both halves commit together. This is
  the safety argument for the whole change and belongs in the ADR.

- **A wake can arrive inside an open read snapshot.** An autocommit read starts a
  fresh snapshot, so a commit wake is seen today. Once reads are grouped
  ([spec 2](2026-08-20-a-read-only-transaction.md)), a wake arriving while a read
  transaction is open reads state from before that commit. For the tailers the
  floor tick bounds it; for the dependency waker, which by design has no tick, it
  falls through to the 60s stale-dependents pass. That is a new way for "a commit
  wakes it" to mean less than it says, and it needs recording wherever the grouped
  read lands.

- **`busy_timeout` applies to readers too**, and WAL readers can still take
  `SQLITE_BUSY` against a checkpointer. Keep the 5s the writer uses.

## Tests

On-disk stores, since in-memory falls back:

- **A read inside a transaction sees that transaction's uncommitted writes.**
  This is the test the whole spec exists for. Write inside `Within`, read inside
  the same `Within`, assert the write is visible. Against an `s.read` that
  forgets the transaction it reads stale and fails — where the current code would
  hang.
- A read *outside* the transaction does not see them, from another goroutine,
  driven by a signal rather than a sleep.
- Reads proceed while a write transaction is open — the point of the change.
  Assert on completion, not on timing.
- `WithReadConnections(0)` and negative are `ErrInvalidOption`; the default is 4.
- Idle connections are not reaped: run more than two concurrent reads, then
  assert `db.Stats().MaxIdleClosed` and `MaxIdleTimeClosed` are both zero. Named
  because the alternative is a test that sleeps out a five-minute timer, which is
  no test at all.
- `Close` closes both pools and is idempotent.
- `OpenMemory` still works, with reads on the writer.
- A write on the reader fails, and fails as a write rather than a lock timeout —
  the `query_only` choice. There is deliberately no test for the `mode=ro`
  recovery claim: it did not reproduce, so a test would pass either way.

`BenchmarkWritesUnderWatch` is the measurement: its no-watch row is the baseline
the other rows should move toward.

## On ship

ADR: **reads get their own connections**, recording the per-connection tax, the
`s.read` rule, and the `query_only` choice. It supersedes the `TODO.md` item,
which shrinks to nothing.

`CLAUDE.md` says "the store's single connection" in several places, and the
drivers ADRs lean on it. Each needs the re-derived reason rather than a deletion.

The page-cache item in [`TODO.md`](../TODO.md) currently discounts a larger cache
because "the store is one connection, so a larger cache is not shared across
concurrent readers the way the advice assumes". That premise is gone; the item
should be re-opened rather than left standing with a false reason.
