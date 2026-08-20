# Cache prepared statements

- **Status:** Planned. No semantic change; the whole of it is a statement table
  in front of `s.conn`.
- **Date:** 2026-08-20
- **Depends on:** nothing.

## Why

`database/sql` prepares a statement, runs it, and throws it away on every
`QueryContext` / `ExecContext`. SQLite parses and plans the SQL each time. The
store issues around seventy distinct SQL strings and the pool is one connection,
so every one of those parses is repeated forever.

Measured on an arm64 sandbox, on disk, WAL, `synchronous=NORMAL`. The saving is
per statement and scales with how much SQL there is to parse, so the statement
has to be named beside the number:

| Statement | plain | cached | saved |
|---|---|---|---|
| one column by primary key | 5.9 µs | 3.0 µs | 2.9 µs |
| ...the same, inside a transaction | 9.4 µs | 6.8 µs | 2.6 µs |
| ...at ten statements per transaction, per statement | 4.8 µs | 2.6 µs | 2.2 µs |
| seventeen columns by primary key, 1000-row table | 15.9 µs | 9.7 µs | 6.2 µs |

**Take ~2.2 µs per statement as the working figure**: most statements run inside
a transaction, and the ten-per-transaction row is the closest thing here to a
reconcile pass. The wide-select row is the ceiling, not the average.

**A standalone `Prepare` costs more than the parse it saves, and the two numbers
measure different things.** `PrepareContext` plus `Close` of the seventeen-column
select is 13.8 µs against 17.8 µs for the whole plain query; a trivial statement
prepares in 3.3 µs against 5.9 µs plain. Preparing in a loop at open reads higher
still, 16–23 µs per statement, because each one pays a pool round trip and
`database/sql`'s per-statement bookkeeping. The saving in the table above is the
marginal parse the plain path pays inline. Neither figure contradicts the other;
a reader who sees only the second lands on "the implementation underperformed".

The practical consequence: **each SQL shape costs about 19 µs once, and 2.2 µs
back on every execution after that.** Break-even is roughly nine executions per
shape.

## The rule everything else follows from

**You cannot prepare on the pool while a transaction holds the only connection.**
`SetMaxOpenConns(1)`, so `db.PrepareContext` waits for a connection the ambient
transaction is holding. Measured: with a 2 s deadline it returns
`context deadline exceeded` after 2.001 s; with the background context the store
actually passes, it never returns.

Every write is inside a transaction — `Objects().Create` self-wraps, and
`s.conn(ctx)` hands back `fr.st.tx` (`sqlite/store.go:479`) — so a cache that
prepares on miss inside one hangs the process on the first create.

Two facts bound the ways out, both measured:

- `tx.PrepareContext` works, but the statement is closed at commit
  (`sql: statement is closed` after). It buys nothing.
- A statement prepared on the pool and bound with `tx.StmtContext` is reused
  across transactions correctly, because the transaction holds the connection the
  statement was prepared on.

## The change

A statement table on `sqliteStore`, filled two ways and read one way.

```go
// stmts holds one prepared statement per SQL string. Never filled from inside a
// transaction: the pool is one connection, so preparing there waits on the
// connection the transaction holds and never returns.
stmts   map[string]*sql.Stmt
stmtsMu sync.RWMutex
```

**Reading.** `s.conn(ctx)` returns a wrapper implementing the existing `dbtx`
interface. On a hit inside a transaction the statement is bound with
`tx.StmtContext`; outside one it runs directly. On a miss it runs the SQL
uncached, exactly as today.

**Filling — two paths, and the split is the whole point.**

- **A miss outside a transaction prepares inline.** No ambient transaction is
  holding the connection, so this is an ordinary pool wait. This is what warms the
  driver reads, which run bare with no `Within` behind them:
  `ListUnsettledIDs` (`:972`), `MaxVersionAll` (`:1189`), `ListSinceAll` (`:1209`),
  `MaxVersion` (`:3117`), `ListStaleSince` (`:1115`).
- **A miss inside a transaction is recorded and prepared at the commit tail**,
  after `Within` runs its `AfterCommit` hooks. This is what warms the write paths,
  whose SQL never executes outside a transaction.

Deferring *everything* to the commit tail would leave an idle beehive — no write
commits, which is the entire point of specs 6–12 — running its per-tick reads
uncached forever, and a read-only embedder caching nothing at all.

**Never for SQL whose text varies with data.** See below; those go through an
explicit raw path that neither reads nor fills the table.

## What may be cached

The line is **whether the text varies with data**, not whether a function built
it. `selectScoped`, `listObjectsWhere`, `trimEvents`, `eventHorizon` and
`deleteWriteLogRows` all take a fragment parameter and are fine: their text ranges
over a handful of compile-time constants, so the table holds a handful of entries.

What must not be cached is text rendered from a slice length. Caching those fills
the table with single-use strings, and a batch already amortises one preparation
over hundreds of rows — the wrong place to spend the cache. **Twelve call sites**,
each of which must move to the raw accessor:

| Site | Function |
|---|---|
| `:708` | `appendWriteLogUpdates` |
| `:953` | `conditionsByIDsChunk` |
| `:1034` | `ReconcileOwed().Stamp` |
| `:1055` | `ReconcileOwed().Sweep` — built by `reconcileOwedSweepQuery` at `:1046` |
| `:1123` | `Dependencies().ListStaleSince` — `kindTuples` at `:1110` |
| `:1301` | `readImages` |
| `:1677` | `loadForConditionSetChunk` |
| `:1817` | `upsertConditions` |
| `:2382` | `markManyForDeletionChunk` |
| `:2474` | `unblockedTargetsChunk` |
| `:2807` | `edgesByIDsChunk` |
| `:3303` | `Objects().ListByIDs` |

`:953` and `:2807` build the placeholder list into a **local** `[]string` named
`placeholders`, shadowing the package function of that name. A grep for
`placeholders(` misses both. That is exactly how the thirteenth one gets added
later without anyone noticing.

**`Events().List` (`:1995`) is the judgement call.** It assembles its `WHERE` from
an optional predicate slice plus an optional `LIMIT` — up to 32 shapes, bounded
and small, so caching it is defensible. Put it on the raw path anyway: the bound
is a property of today's `EventQuery`, not of the code, and a sixth optional
predicate doubles it silently. Say so in the code, or the next reader will "fix"
it.

**So "no call site changes" does not hold**, and the enumeration above is the
change. A missed site poisons the table with single-use entries until the cap
stops it, after which every constant statement runs uncached forever with no
signal. Only the drift test catches that.

## Edge cases the implementer would otherwise guess at

- **Warm after the hooks, not before.** The hooks are the push path
  (`signalRequeueNow`, `signalKindWritten`); the preparations are bookkeeping. A
  first create warms several shapes at once — the insert, the write log append,
  the edge insert — so warming first would put ~100 µs of preparation ahead of the
  very first wake in a process.

- **The deferred preparation needs a detached context.** The caller's may already
  be cancelled by the time the commit tail runs — that is what shutdown looks like
  — and nothing would ever warm. Use `context.WithoutCancel`, as `AfterCommit`
  detaches its hooks for a different reason.

- **Recording a miss must not unwind with a savepoint.** Everything else in
  `txState` does, so this needs a line: the SQL text is valid whatever the frame's
  outcome, and a rolled-back frame's statement is still worth preparing.

- **The pending-miss set is bounded and cleared on preparation.** It holds SQL
  strings between the miss and the commit tail. Bound it like the table, and clear
  each entry as it is prepared or dropped.

- **A cap is a backstop, not a policy.** With data-rendered SQL excluded the table
  cannot grow past the source's constant set. Keep a cap anyway, and make it
  cap-and-stop: **never evict.** Closing a `*sql.Stmt` another goroutine is
  mid-execution on yields `sql: statement is closed`.

- **A failed preparation is dropped, not retried and not surfaced.** At the commit
  tail the caller has already committed and has nothing to do with the error; on
  the inline path, fall through and run uncached. Log at debug either way. The one
  exception is a context cancellation on the inline path, which must propagate.

- **`Close` must close what it prepared.** Not because it would otherwise block —
  measured, `db.Close()` with live cached statements returns nil immediately — but
  because it frees modernc's compiled programs. `Stmt.Close` twice is nil, so the
  path is safe on an already-closed store.

- **`map` under an `RWMutex`, not `sync.Map`.** The store is one connection wide,
  so lock contention is not the constraint, and a cap needs a length the `sync.Map`
  API does not give.

- **Two goroutines racing to prepare the same SQL** is a wasted preparation, not a
  fault. Keep the winner and close the loser under the write lock.

- **PRAGMAs already route around this.** `ReclaimSpace` takes its own `*sql.Conn`
  and hands it to `freePagesRelease` / `pageCounters` as a `dbtx`; neither touches
  `s.conn(ctx)`. Keep it that way — `PRAGMA incremental_vacuum` must be `Exec`'d
  and carries its page count in the text.

- **Savepoint SQL goes straight at `st.tx`** (`sqlite/store.go:331`) and must stay
  there. Worth a line in the code so nobody routes it through the table while
  tidying.

- **A test runs `ROLLBACK` through `store.conn`** (`sqlite/store_test.go:6453`).
  Transaction control is on the raw path, so it keeps working — but it is the kind
  of call site that makes a cache-everything design wrong.

- **`tx.StmtContext` allocates a per-transaction `*sql.Stmt`** held on the
  transaction until commit. Bounded by the statements one transaction runs.

- **A cached statement survives a pool connection recycle, on both paths.**
  `ConnMaxIdleTime` is five minutes. `database/sql` re-prepares transparently, and
  `Tx.StmtContext` records its re-preparation on the parent statement, so a
  transaction-only statement does not re-prepare per transaction forever: 7.2 µs
  before a forced recycle, 7.1 then 6.8 µs after.

## Tests

In `sqlite/store_test.go`:

- **The deadlock, pinned.** A miss inside a transaction completes, and the
  statement is cached after the commit. Without this test the whole design
  regresses silently into a hang. Give it a context deadline so a regression fails
  rather than wedging the suite.
- **A pool-path read is cached inline**, on its first call, with no transaction
  anywhere. This is the blocking bug the two-path split exists for.
- A driver read repeated on a tick issues one preparation, not one per tick.
- A write path that only ever runs inside a transaction is cached after one call.
- Data-rendered SQL is never added to the table, at any arity — one case per
  chunked call site, or the enumeration rots.
- The cap stops filling and does not evict; already-cached statements keep working.
- A rolled-back frame's miss is still prepared at the commit tail.
- A store closed while misses are pending does not leak or panic.
- `Close` on a store that ran queries returns nil, and a second `Close` is nil.
- A `RETURNING` statement is correct through the table, repeatedly, on the pool
  and inside a transaction.

**The drift test.** Per store, asserted at `Close`: every recorded miss was
data-rendered SQL, transaction control, or is in the table by the end. A miss is
legitimate — under lazy filling every constant statement misses exactly once — so
the assertion is about what happened *after* the miss, not about the miss. Keep
the accounting on the store rather than in package state: the suite runs many
store instances, some in parallel.

Add `BenchmarkCachedStatements` to `sqlite/store_bench_test.go` covering a single
statement and a ten-statement transaction, so both numbers above have a tripwire.

## Not in this spec

**Eagerly preparing a fixed list at `open()`.** It buys no speed: lazy filling
pays the same ~19 µs per shape, just at a commit tail or a first read instead of
at open, and with ~70 shapes that is the same ~1.3 ms either way. What it would
buy is that every constant SQL string is validated at open, so a typo on a cold
path is a startup error rather than a runtime one — worth having, but it is a
different argument, it carries a list to maintain, and it moves ~1.3 ms into
`open()` for an embedder that may not want it there. Decide it separately, against
an embedder that cares.

## On ship

No ADR: nothing about the design changes, and there is no rule a later reader
could break by accident — except one, which belongs in the code rather than a
document. State it where the table is defined: **never prepare inside a
transaction.**

Re-run `BenchmarkConvergedSpecWrite` and record the new baseline in
[the spec-write ADR](../adr/2026-08-19-a-spec-write-takes-its-transaction-unconditionally.md).
Its 41.3 µs converged write includes several preparations; the ADR's break-even
arithmetic moves, and it says what would reopen the question.
