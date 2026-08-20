# Cache prepared statements

- **Status:** Planned, with one question open — see *Read transactions*.
- **Date:** 2026-08-20
- **Depends on:** nothing. An earlier draft made this wait on the connection
  split ([ADR](../adr/2026-08-20-reads-get-their-own-connections.md)), on the belief that a
  held statement taxes its connection; measured properly, a *cached* statement
  barely pays that and caching more is what avoids it. The two are independent
  and compose.
- **Related:** [a read that groups is a read transaction](../adr/2026-08-20-a-read-that-groups-is-a-read-transaction.md),
  which decides whether the store's grouped reads can share a statement.

## Why

`database/sql` prepares a statement, runs it, and throws it away on every
`QueryContext` / `ExecContext`. SQLite parses and plans the SQL each time, and
the store issues about seventy distinct SQL strings, so every one of those parses
repeats forever.

Measured on an arm64 sandbox, on disk, WAL, `synchronous=NORMAL`. The saving is
per statement and scales with how much SQL there is to parse, so the statement
has to be named beside the number:

| Statement | plain | cached | saved |
|---|---|---|---|
| one column by primary key | 5.9 µs | 3.0 µs | 2.9 µs |
| seventeen columns by primary key, 1000-row table | 15.9 µs | 9.7 µs | 6.2 µs |

End to end, a `GetMeta` on a store with a warm cache of 22 statements ran 21.2 µs
plain against 7.7 µs cached — **−63%**.

**A standalone `Prepare` costs more than the parse it saves, and the two numbers
measure different things.** `PrepareContext` plus `Close` of the seventeen-column
select is 13.8 µs against 17.8 µs for the whole plain query. Preparing in a loop
reads higher still, 16–23 µs each, because every one pays a pool round trip and
`database/sql`'s per-statement bookkeeping. The saving above is the marginal
parse the plain path pays inline. Neither figure contradicts the other; a reader
who sees only the second concludes the implementation underperformed.

So **each SQL shape costs about 19 µs once and returns 2–6 µs per execution**,
and break-even is a handful of executions per shape.

## Where the statements live

**Everywhere the SQL is constant** — the pool, and inside transactions — with one
exception, which is the whole of the design below.

Measured on one connection, ten indexed reads in a transaction: 91.8 µs uncached,
40.6 µs cached. The same store measured four ways, before any split:

| | pool read | write transaction | ten reads in a transaction |
|---|---|---|---|
| no cache | 21.2 µs | 89.0 µs | 202.4 µs |
| cached everywhere | 7.8 µs (−63%) | 67.2 µs (−24%) | 73.4 µs (−64%) |
| cached except cursor-holding reads in a transaction | 7.7 µs (−64%) | 81.6 µs (−8%) | 255.5 µs (**+26%**) |

The third row is the shape to avoid, and it is why the exception below is the
central question rather than a detail: a statement left uncached on a connection
that holds others pays for the caching without receiving it.

## Filling the table

**A miss outside a transaction prepares inline.** Nothing holds the connection, so
this is an ordinary pool wait.

**A miss inside a transaction cannot prepare**, because the pool is one connection
and preparing waits on the one the transaction is holding — measured: `context
deadline exceeded` after 2.001 s with a deadline, and no return at all with the
background context the store passes. Queue it on the frame and prepare it at the
commit tail, after the hooks run. Every write path warms after one call.

Keep the queue on the frame and never in the table. A marker in the table outlives
the transaction that would have cleared it, so a failed `Within` leaves that SQL
uncached for the life of the store.

The [connection split](../adr/2026-08-20-reads-get-their-own-connections.md)
confines the deadlock to the writer — a read miss prepares on the reader while
the transaction holds the writer — but the rule stands either way, and is
cheaper to keep than to make conditional on which pool a caller is on.

## The rule everything rests on

**A statement is cached only where nothing else can reach it concurrently.**

A cached statement is one compiled handle per connection, and `modernc`'s rows
keep it positioned until they close (`stmt.pstmt`, and `reuseStmt = true` on the
rows it returns). Two callers sharing one handle is silent corruption, not an
error. Measured, two goroutines each listing 1000 rows through one shared
statement: **309** and **1691** rows, `err == nil`.

**Outside a transaction it cannot happen.** `database/sql` checks a connection
out to one caller for as long as its rows are open, and prepares a separate driver
statement per connection, so two concurrent callers hold two handles. Safety comes
from the pool, not from a rule anyone has to follow.

**Inside a transaction it can**, because the connection is already held and
`txState.mu` licenses sibling goroutines: *"AfterCommit and bare reads stay legal
concurrently."* That sentence is the whole problem, and the question below.

## The question to settle before building this

**How does a statement inside a transaction become safe to cache?** The table in
*Where the statements live* prices the answers: caching inside transactions is
worth −24% on writes and −64% on read-heavy transactions, and declining to is
worth +26% on the latter. So this is the decision the spec turns on, not a detail.

Three answers, in the order I would consider them.

**Mark the transaction's origin.** Beehive opens its own transactions and never
fans out inside them; a caller-opened `Within` might. A flag on the frame, set
where the transaction is opened, caches everything inside beehive's own and stays
conservative inside a caller's. Nesting resolves correctly: a store method's
self-wrapped `Within` joins the caller's frame and inherits its conservatism.

No public API, no contract change, and no way for a caller to get it wrong. The
one impurity: `client.go`'s four `Within` calls run `c.decode` inside the
transaction, which invokes the caller's `UnmarshalJSON`. Marking those internal
assumes nobody unmarshals by fanning out concurrent store calls — absurd, but an
assumption, and it belongs in a sentence beside the flag. Mark only the `sqlite`
package's own transactions and the assumption goes away, but so does most of the
win, since `client.update` wraps every spec write.

**Narrow the contract.** State that a `Within` fn must not issue concurrent store
calls, and cache everything. Simplest to build and the fastest result. It costs a
documented guarantee that controller authors using `ControllerClient.Within` may
be relying on, and the failure mode for anyone who is is silent.

**Decline, and cache only where a cursor cannot be shared.** `Exec` completes
under the connection lock, so it is always safe; `Query` and `QueryRow` inside a
transaction are not. This is what the withdrawn implementation did. It is safe and
needs no argument, and it is the +26% row.

**Recommendation: mark the origin.** It buys nearly all of the contract change
without asking anyone to promise anything, and the assumption it does rest on is
one line to state and one test to pin.

## What may be cached

The line is **whether the text varies with data**, not whether a function built
it. `selectScoped`, `listObjectsWhere`, `trimEvents`, `eventHorizon` and
`deleteWriteLogRows` all take a fragment parameter and are fine: their text ranges
over a handful of compile-time constants.

Text rendered from a slice length must not be cached — one entry per arity fills
the table with single-use statements, and a batch already amortises one
preparation over every row it binds. Fourteen call sites, each moving to a raw
accessor:

| Site | Function |
|---|---|
| `:708` | `appendWriteLogUpdates` |
| `:953` | `conditionsByIDsChunk` |
| `:1034` | `ReconcileOwed().Stamp` |
| `:1055` | `ReconcileOwed().Sweep` |
| `:1123` | `Dependencies().ListStaleSince` |
| `:1301` | `readImages` |
| `:1677` | `loadForConditionSetChunk` |
| `:1817` | `upsertConditions` |
| `:1995` | `Events().List` |
| `:2382` | `markManyForDeletionChunk` |
| `:2474` | `unblockedTargetsChunk` |
| `:2807` | `edgesByIDsChunk` |
| `:3303` | `Objects().ListByIDs` |

Route on the SQL's own shape, not on which connection the call happens to use
today: that is a fact about the call graph, and it changes.

`:953` and `:2807` build the placeholder list into a **local** `[]string` named
`placeholders`, shadowing the package function. A grep for `placeholders(` misses
both — which is how the fifteenth gets added unnoticed.

`Events().List` (`:1995`) is the judgement call: its `WHERE` is assembled from an
optional predicate slice plus an optional `LIMIT`, so up to 32 bounded shapes.
Raw anyway — the bound is a property of today's `EventQuery`, not of the code, and
a sixth optional predicate doubles it silently.

## Edge cases the implementer would otherwise guess at

- **The table is per pool.** A `*sql.Stmt` belongs to the pool that prepared it,
  so one prepared on the writer and executed through `s.read` runs on the writer
  and quietly undoes the
  [split](../adr/2026-08-20-reads-get-their-own-connections.md). Key the table by
  pool, or hold one per pool.

- **A statement is compiled once per connection.** `database/sql` prepares
  lazily, so a hot statement is compiled on each reader that runs it. Memory scales with N,
  and so does the small residual cost a cached statement pays on a busy
  connection (+7%).

- **Revisit `ConnMaxIdleTime` on whichever pool holds the table.** `OpenPool`
  reaps a connection idle for five minutes, and reopening it drops every statement
  prepared on it. Harmless today; it is a repeated cold start once the table
  exists.

- **Never prepare while holding a connection.** A prepare that waits for a
  connection its own goroutine is holding open — through live `Rows` — deadlocks
  the same way the single pool did, just at N callers instead of one. The store
  never issues a query while rows are open (every scan helper closes first), which
  is what makes this safe. Say it where the table is defined; it is invisible
  otherwise.

- **A cap is a backstop, not a policy.** With data-rendered SQL excluded the key
  space is the constant SQL in the package. Keep a cap anyway, cap-and-stop, and
  **never evict**: closing a `*sql.Stmt` another goroutine is running yields
  `sql: statement is closed`. Check the room *before* `PrepareContext`, or a full
  table prepares and closes on every call, which is worse than not caching.

- **A failed preparation records nothing** and runs uncached; a later call tries
  again. Do not leave a marker: a marker that outlives the thing that would have
  cleared it is how a statement stays uncached for the life of the store.

- **`Close` closes the table before the pools.** Not because it would otherwise
  block — measured, `db.Close()` with live statements returns nil immediately —
  but because it frees the driver's compiled programs. Idempotent, so `Close`
  stays so.

- **`map` under an `RWMutex`, not `sync.Map`**: a cap needs a length, and the
  store is one writer wide so contention is not the constraint.

- **PRAGMAs stay off the table.** `ReclaimSpace` takes its own `*sql.Conn` on the
  writer; `PRAGMA incremental_vacuum` must be `Exec`'d and carries its page count
  in the text.

- **Transaction control and savepoint SQL go straight at the transaction**
  (`sqlite/store.go:331`), never through the table.

- **`OpenMemory` keeps the table.** Unlike the connection split, nothing here
  depends on there being two pools, so the suite exercises the same code the
  on-disk store runs.

## Tests

In `sqlite/store_test.go`:

- A read outside a transaction is prepared once and reused.
- A read *inside a write transaction* is not cached, and the table does not grow.
- Concurrent reads through one cached statement each see every row — the
  regression test for the shared-handle corruption. It can only fail on broken
  code, so it is a detector rather than a flake.
- Data-rendered SQL never reaches the table, one case per chunked call site.
- The cap stops filling and never evicts; cached statements keep working.
- A failed preparation leaves nothing behind and the next call retries.
- `Close` returns nil, twice, and releases the table.
- `OpenMemory` caches nothing.

**The drift guard.** Two entries that collapse to the same text — placeholder
runs and `VALUES` tuple sets reduced to one — differ only in how many values they
bind, which only a rendered list can produce. Matching the placeholder text
directly does not work: `placeholders(n)` renders `"?, ?"` **with a space**, and a
hand-written constant `VALUES (?, ?)` would fail it. Run the guard from the
suite's store constructor so every test is a sample, not from one hand-picked
test.

Benchmark the pool read and a write transaction, so both the win and the
"unchanged" claim have a tripwire.

## Not in this spec

**Eagerly preparing a fixed list at `open()`.** It buys no speed — lazy filling
pays the same ~19 µs per shape, just at first use — and it adds ~1.3 ms to
`open()` for statements a short-lived process may never run. What it would buy is
SQL validated at open, so a typo on a cold path is a startup error rather than a
runtime one. A different argument, with a list to maintain; decide it separately,
against an embedder that cares about startup.
