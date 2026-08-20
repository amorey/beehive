# Reads get their own connections

- **Status:** Planned. Prerequisite of
  [a read-only transaction](2026-08-20-a-read-only-transaction.md) and
  [cache prepared statements](2026-08-20-cache-prepared-statements.md).
- **Date:** 2026-08-20
- **Supersedes:** the "Reads and writes share one connection" item in
  [`TODO.md`](../TODO.md), which this builds.

## Why

`OpenPool` sets `journal_mode(WAL)`, which lets one writer and many readers run
at once. Then `sqlite.Open` passes `maxConns = 1`, so every read queues behind
every write in Go's pool and that concurrency is unused. The limit is
`database/sql`, not SQLite.

Three things pay for changing it. The first is new.

**A prepared statement taxes the connection it is held on.** Holding statements
open slows every *other* statement run on that same connection — measured on an
arm64 sandbox, ten indexed reads in one transaction:

| statements held open | cost |
|---|---|
| 0 | 92 µs |
| 1 | 92 µs |
| 10 | 108 µs (+17%) |
| 20 | 115 µs (+24%) |
| 70 | 117 µs (+27%) |

It plateaus around +25%, so caching *fewer* statements does not avoid it. But it
is **per connection**, which is what makes it escapable — two pools over one
file, twenty statements held, transaction on the writer:

| held on | cost |
|---|---|
| nothing | 91.0 µs |
| the reader | 91.5 µs |
| the writer | 113.3 µs (+24%) |

So the split is what lets statement caching be a win everywhere instead of a
trade. Without it, caching regresses read-heavy transactions by 26%, or every
write by 12%, or costs a documented concurrency guarantee. With it, the writer
holds nothing and the tax is gone from the write path.

**A live watch slows writes.** `BenchmarkWritesUnderWatch`, on disk, beehive not
started: 172,000 ns/op with no watch, 215,000 with one watched kind, 326,000 with
sixteen. Watch *count* is free — the shared tailer working as designed — and
watched *kinds* is the axis that costs, because each tailer wakes with nothing to
coalesce and they contend for the one connection.

**The drivers read on a cadence forever.** The owed pass, the stale-dependents
pass, the waker and the watch floors all re-read the same indexes on a fixed
schedule, and every one of those reads currently sits in the writers' queue.

## The change

Two pools over one file.

- **The writer** is what exists today: `OpenPool(path, 1)`, `_txlock=immediate`.
- **The reader** is a second pool with `_pragma=query_only(true)` and N
  connections.

`query_only`, not `mode=ro`: a `mode=ro` connection cannot create or recover the
`-wal` and `-shm` files, so it fails to open a database that was not shut down
cleanly.

`s.conn(ctx)` keeps its job. A sibling selects the reader:

```go
// read returns the connection a read-only statement runs on: the ambient
// transaction if there is one, else the read pool.
//
// Returning the transaction is not an optimisation — it is the whole safety
// property. A read issued inside a transaction on any other connection sees the
// database as of before that transaction's own uncommitted writes.
func (s *sqliteStore) read(ctx context.Context) dbtx
```

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

**`SetMaxIdleConns` must be set to N as well.** `database/sql` keeps two idle
connections by default, so a pool of four would close and reopen two of them
continuously — and reopening a SQLite connection re-prepares every statement
cached on it.

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

- **`OpenMemory` cannot be split.** `file::memory:` is per-connection, so a
  second pool is a different and empty database. The reader falls back to the
  writer there, which means the suite runs today's semantics and **only on-disk
  tests cover the split**. Do not reach for `mode=memory&cache=shared` to avoid
  that: shared-cache mode changes SQLite to table-level locking and introduces
  `SQLITE_LOCKED`, so it would be testing a different database than production.

- **Migrations run on the writer, before the reader opens.** A reader opened
  first would cache a schema that does not exist yet.

- **`Close` closes both pools**, reader first. It stays idempotent.

- **`ReclaimSpace` stays on the writer.** It takes its own `*sql.Conn` and
  `PRAGMA incremental_vacuum` writes.

- **A long read transaction holds back WAL checkpointing.** The WAL cannot be
  truncated past the oldest reader's snapshot, so a reader that sits open — a
  paging drain, a slow consumer — grows the file. Today this cannot happen,
  because a read never outlives the one connection's turn. Bound the drains that
  page, or accept it and say where the bound is.

- **Budgets justified by "the single connection" need re-deriving, not
  assuming.** The waker's page budget exists so a resume "cannot monopolise the
  single connection" (`waker.go:101`, `:105`, `:428`); the tailer reads "one
  after another on the single connection" (`objectswatch.go:583`); `workqueue.go`
  reasons about a deadlock on it. Each is still defensible, but for a different
  reason, and a reader who finds the old reason will believe the wrong thing.

- **The reader sees committed state only.** A read outside a transaction may lag
  a write in flight on the writer. That is correct and is what the sole-writer
  model already implies, but it is worth stating: the two connections do not see
  the same instant.

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
- Idle connections are not reaped: run more than two concurrent reads and assert
  the pool did not churn.
- `Close` closes both pools and is idempotent.
- `OpenMemory` still works, with reads on the writer.
- A database left with a hot `-wal` opens and recovers — the `query_only` versus
  `mode=ro` choice, which a test pins because the failure only appears after an
  unclean shutdown.

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
