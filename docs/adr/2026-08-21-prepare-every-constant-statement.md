# Prepare every constant statement at startup

- **Status:** Accepted — `sqlite/statements.go`, over 59 call sites.
  Retires `docs/specs/2026-08-20-cache-prepared-statements.md`, which git holds.
- **Date:** 2026-08-21

## Context

`database/sql` prepares a statement, runs it, and throws it away on every
`QueryContext` / `ExecContext`. SQLite parses and plans the SQL each time, so
every one of those parses repeated forever. `modernc` has no statement cache of
its own — `conn.QueryContext` calls `sqlite3_prepare_v2` per query and finalizes
after — and neither does `database/sql`. `*sql.Stmt` is the mechanism on offer.

## Decision

**Every statement whose text is constant is prepared at startup into a named
slot, one set per pool.** Nothing is prepared at runtime, so there is no table,
no key, no fill path, no cap and no eviction.

```go
type stmtID int
type stmtSet [numStmts]*sql.Stmt
```

**The call site names the id, never a preparation.** A `*sql.Stmt` is one
preparation, and choosing between them is what `stmtFor` exists to do: a site
that picked the reader's copy would have chosen before anything looked at the
ctx. `stmtFor` routes on three cases — the pool's own statement outside a
transaction, `tx.StmtContext(readStmts[id])` inside a read transaction, and
`tx.StmtContext(writeStmts[id])` inside a write one, for reads as well as
writes. A read-pool statement inside a write transaction reads from before that
transaction's own uncommitted writes; measured, a read of a row the open writer
had just inserted returned nothing, silently.

**A write is prepared on the writer alone, and its reader slot stays nil.**
`PrepareContext` of an `INSERT` against a `query_only(true)` pool *succeeds* —
only execution fails, with `attempt to write a readonly database (8)`. The nil
slot is the only representation `Open` can check, and a wrong route hits it at
routing time rather than at execution.

**Preparation happens in two halves, because two constraints disagree.** The
writer's set is filled in `open`, before the version seed draws through it; the
reader's once `Open` has assigned a real read pool, since a statement prepared
before that binds to the writer. `OpenMemory` aliases both to one pool.

**`open` then warms the reader's other connections.** `PrepareContext` compiles
on the connection it grabs and records it, so the rest would compile at first
use — during a process's startup pass. The warm-up holds every reader connection
at once, because the pool otherwise hands back the one just released, and calls
each statement with **no arguments**: the compile lands before the argument count
is checked, and that error is not `ErrBadConn`, so nothing retries. Reads only —
an argless write binding no placeholders would run.

**Text rendered from a runtime count is not a field.** One statement per arity
would fill the set with single-use entries, and a batch already amortises one
preparation over every row it binds. Thirteen sites render an `IN` list, a
`VALUES` tuple set or an optional predicate.
`TestOnlyRenderedSQLLivesInAFunction` lists them and fails on a fourteenth: with
every constant statement hoisted, SQL text left inside a function means a
statement someone forgot to prepare.

A helper that took a fragment takes an id instead — `selectScoped`,
`markForDeletion`, `trimEvents`, `trimWriteLog` — since every fragment they were
passed is a compile-time constant. `listObjectsWhere` splits: its two
constant-tail callers are prepared, and `Objects().ListByIDs` keeps the rendered
path.

**Nothing new is promised about concurrency.** A prepared statement is one
compiled handle per connection, and two callers sharing one interleave on its
cursor — measured, two goroutines each listing 1000 rows got 309 and 1691 back
with `err == nil`. Outside a transaction the pool prevents it at any pool size.
Inside one, what prevents it is the contract `internal/storeapi` already states:
*a transaction ctx belongs to one goroutine*. Enforcement stays partial and
deliberately so, as it is for the
[sole-writer rule](2026-08-05-one-process-one-beehive-sole-writer.md): a sibling
goroutine entering a nested `Within` is refused by `pushSavepoint`'s depth check,
and overlapping bare reads are not detected. Catching those would need a `defer`
in every store method, because the hazard window is "cursor open" and not "call
in progress".

**Idle connections are kept on both pools.** Reopening one drops every statement
compiled on it. `OpenMemory`'s reap was worse than a cold start — `file::memory:`
is per-connection, so it discarded the database.

## Consequences

Measured on disk, against the commit this work started from:

| | before | after | |
|---|---|---|---|
| `ReadUnderWrites` 0 writers | 22.8 µs | 7.73 µs | **−66%** |
| `ReadUnderWrites` 1 writer | 47.2 µs | 9.88 µs | **−79%** |
| `ReadUnderWrites` 4 writers | 34.4 µs | 9.59 µs | **−72%** |
| converged spec write | 42.3 µs | 18.9 µs | **−55%** |
| changed spec write | 151 µs | 83.3 µs | **−45%** |

**A connection holding prepared statements taxes everything running on it**,
prepared and unprepared alike, and the thirteen rendered statements pay it with
nothing against them. Measured alternating (`BenchmarkResidencyToll`): +9% on an
unprepared execution, +3% on a prepared one, against a ~17 µs saving per prepared
execution. It is a step rather than a slope — nearly all of it arrives by fifteen
resident statements and sixteen times more adds almost nothing — so the set can
grow without the toll growing. Likely SQLite's per-connection lookaside
allocator, which `modernc` exposes no knob for; stated so nobody hunts for one.

**Benchmark arms must be interleaved across separate `go test` invocations.**
`-count` repeats each sub-benchmark consecutively, so slow machine drift lands
entirely on whichever ran first: two earlier runs put the toll at +25% and +6%,
both that artefact.

**`refillVersions` strips the transaction frame, not only the deadline.** It runs
in `runTx`'s settle — after the commit but before `flush` latches the frame
closed — so routing by ctx would draw on a transaction that was already over. It
passed a pool handle explicitly before, which hid the question.

**Nine `conn` calls survive for their refusal alone.** A write inside a read
frame has to be refused before a no-op early return, which is the one thing
`stmtFor` cannot stand in for: it is only reached when a statement is actually
issued.

The cost at startup is one preparation per statement per pool plus the warm-up,
a few milliseconds paid by a process that may run none of them, and the resident
compiled programs scale with statements × connections for the life of the
process.

## Not done

**Eagerly validating SQL is a side effect, not a goal.** A statement that will
not prepare now fails `Open` rather than a cold path at runtime. That was an
argument for this shape, not the reason for it.

**A runtime table keyed by SQL text** would catch every statement rather than the
named ones, at the cost of a fill path that cannot prepare inside a transaction,
a cap that must never evict, and a drift guard over rendered SQL. This store's
SQL is fixed at compile time, so the catching is worth less than the machinery.
Git holds the worked-through version of that design.
