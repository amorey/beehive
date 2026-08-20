# Cache prepared statements

- **Status:** Planned. Every statement whose text is constant is **prepared at
  startup, into a named field on each pool's set**. Nothing is prepared at
  runtime. The one measurement this spec owed has been taken — see *What
  residency costs*.
- **Date:** 2026-08-20
- **Depends on:** [reads get their own connections](../adr/2026-08-20-reads-get-their-own-connections.md),
  which is why there are two pools to prepare against.
- **Also changes:** four places where the transaction contract is stated
  wrongly, not at all, or as a defence it is not — see *The contract this rests
  on* — and two structural tripwires that the SQL hoist re-keys, see *What is
  prepared*.

## Why

`database/sql` prepares a statement, runs it, and throws it away on every
`QueryContext` / `ExecContext`. SQLite parses and plans the SQL each time, so
every one of those parses repeats forever.

Measured on an arm64 sandbox, on disk, WAL, `synchronous=NORMAL`. The saving is
per statement and scales with how much SQL there is to parse, so the statement
has to be named beside the number:

| Statement | plain | prepared | saved |
|---|---|---|---|
| one column by primary key | 5.9 µs | 3.0 µs | 2.9 µs |
| seventeen columns by primary key, 1000-row table | 15.9 µs | 9.7 µs | 6.2 µs |

The store measured three ways:

| | pool read | write transaction | ten reads in a transaction |
|---|---|---|---|
| nothing prepared | 21.2 µs | 89.0 µs | 202.4 µs |
| **prepared everywhere** | **7.8 µs (−63%)** | **67.2 µs (−24%)** | **73.4 µs (−64%)** |
| prepared except cursor-holding reads in a transaction | 7.7 µs (−64%) | 81.6 µs (−8%) | 255.5 µs (**+26%**) |

The middle row is what this spec builds. The third is the shape to avoid: an
unprepared statement sharing a connection with prepared ones pays for the
preparation without receiving it, which is why the answer is *every* constant
statement rather than a hot subset. What every statement pays for sharing a
connection with prepared ones — prepared statements included — is measured below.

**A standalone `Prepare` costs more than the parse it saves, and the two numbers
measure different things.** `PrepareContext` plus `Close` of the seventeen-column
select is 13.8 µs against 17.8 µs for the whole plain query. The saving above is
the marginal parse the plain path pays inline. Neither figure contradicts the
other; a reader who sees only the second concludes the implementation
underperformed.

So **each statement costs about 19 µs per connection, once, at `open`, and
returns 2–6 µs per execution.**

## How they are prepared

**At `open`, into named fields, one preparation per pool.** Nothing is prepared
at runtime, so there is no table, no key, no fill path and no cap.

**Two sets, indexed by an id.**

```go
type stmtID int

const (
	stmtGetMeta stmtID = iota
	stmtUpdateSpec
	// ...
	numStmts
)

// stmtSet is one pool's preparations. A write's slot in the reader's set stays
// nil: preparing a write there succeeds and only fails on execution.
type stmtSet [numStmts]*sql.Stmt
```

A read is prepared in both sets; a write only in the writer's. **That asymmetry
is load-bearing.** `PrepareContext` of an `INSERT` against a `query_only(true)`
pool *succeeds* — measured, it returns nil, and only the execution fails with
`attempt to write a readonly database (8)`. A slot that stays nil is the only
representation `Open` can check.

**The id, not a `*sql.Stmt`, is what a call site names.** A `*sql.Stmt` is one
preparation, and choosing the preparation is the whole job: a site passing the
reader's copy has chosen before anything looked at the ctx, and case 3 would then
run `tx.StmtContext` with a read-pool statement on a write frame — the silent
stale read. The site cannot choose correctly either, because the answer depends
on the ctx it is about to pass.

```go
// stmtFor returns id bound to the connection ctx selects: the transaction's own
// connection while one is live, else the pool's own preparation. A nil slot is a
// routing bug, and is reported here rather than at execution.
func (s *sqliteStore) stmtFor(ctx context.Context, id stmtID) (*sql.Stmt, error)
```

`prepareStatements` asserts the invariant at startup — every read id non-nil in
both sets, every write id nil in the reader's — which is stronger than any test
over it, and makes the completeness check a loop rather than reflection over two
struct types.

Three cases:

1. **No live transaction** — the statement from the pool's own set.
2. **A `readOnly` frame** — `tx.StmtContext(ctx, s.readStmts[id])`. Never the pooled
   statement itself: it belongs to the *pool*, so executing it inside
   `withinRead` checks out a second connection and reads from outside the
   transaction's snapshot — the silent failure `read` exists to prevent
   (`sqlite/store.go:531`) — and at `WithReadConnections(1)`, or under
   `OpenMemory`, it deadlocks instead of lying.
3. **A write frame** — `tx.StmtContext(ctx, s.writeStmts[id])`, for reads as well as
   writes. A read-pool statement here misses the transaction's own uncommitted
   writes: measured, a read of a row the open writer transaction had just
   inserted returned 0 rows, silently. This is why every read is prepared in the
   writer's set as well.

`StmtContext` reuses the handle the transaction's connection already compiled, or
compiles it there, taking no second connection either way.

Call sites keep `s.read(ctx)` / `s.conn(ctx)` and gain a statement argument, so
what is prepared is visible where the query is issued rather than inferred from a
rule.

**Preparation is its own step, and `open` is the wrong place for it.** `open`
sets `readDB: db` (`sqlite.go:105`); `Open` installs the real read pool
afterwards, at `sqlite.go:76`. Preparing inside `open` would bind every read
statement to the *writer* for the on-disk store — finding the readonly failure
above, in the other direction and just as late. So `prepareStatements` is called
from `Open` after `s.readDB` is assigned, and from `OpenMemory` before it
returns.

It still runs after the migrations: a statement prepared before `Apply` names a
schema that does not exist yet. A failed preparation fails the constructor and
closes **both** pools — `open`'s existing failure path (`sqlite.go:111`) closes
`db` alone, which is correct only while there is one pool to close. That is the
point of preparing eagerly: SQL is validated at startup, so a typo on a cold path
is a startup error rather than a runtime one.

**Then warm the reader's other connections — only those.**
`DB.PrepareContext` compiles on the connection it grabs and records it
(`stmt.css`), so the statement is already compiled on one connection of each
pool. The writer is one connection, so it is done. The reader needs its other
N−1.

Warming needs no argument lists, which is what keeps it from becoming sixty
signatures kept in sync forever: call the statement with **no arguments**.
`Stmt.QueryContext` reaches `connStmt` — which compiles on the connection it
grabbed and records it — before `driverArgsConnLocked` rejects the argument
count, and an argument-count error is not `ErrBadConn`, so `database/sql` does
not retry. The compile lands; the error is discarded. **Warm reads only**: an
argless execution of a write that happens to bind no placeholders would really
run.

Hold every reader connection with `Conn` until the last is out, or the pool hands
the same one back and it gets warmed N times.

The shape of the cost: reads are prepared twice and writes once, so the
preparations are more than the statement count; only reads land on the reader, so
the warm-up is reads × (N−1). Both are milliseconds at `open`, paid by a
short-lived process that may run none of them. The `Open` benchmark reports the
number; there is no point deriving it here.

**`OpenMemory` prepares one set and points both at it**, because `readDB` is
aliased to `db` there. It also wants `SetConnMaxIdleTime(0)`, and for a reason
larger than statements: `file::memory:` is per-connection, so reaping that
connection discards *the database*. Today's five minutes (`sqlite.go:93`) is a
latent bug that no test has been slow enough to hit; preparing on that connection
does not cause it, only makes it more expensive when it fires.

## What is prepared

**Every statement whose text is constant** — the middle row of the table above is
the whole set, not a subset of it. A statement is prepared iff it is a field, and
the only thing that keeps it from being one is text that varies with data.

Text rendered from a **runtime count** cannot be a field: one statement per
arity, each used once, and a batch already amortises one preparation over every
row it binds. A *count*, not a slice length — `conditionSetLoad(types int)`
renders from a number, and a rule stated over slices does not reach it. Thirteen
call sites, line numbers as of this branch:

| Site | Function | Renders with |
|---|---|---|
| `:906` | `appendWriteLogUpdates` | `tupleRows` |
| `:1154` | `conditionsByIDsChunk` | local `placeholders` |
| `:1237` | `ReconcileOwed().Stamp` | `placeholders` |
| `:1249` | `reconcileOwedSweepQuery` | `kindTuples` |
| `:1325` | `Dependencies().ListStaleSince` | `kindTuples` |
| `:1520` | `readImages` | `placeholders` |
| `:1870` | `conditionSetLoad` | `placeholders(types)` |
| `:2052` | `upsertConditions` | `tupleRows` |
| `:2236` | `Events().List` | `strings.Join(where)` |
| `:2629` | `markManyForDeletionChunk` | `tupleRows` |
| `:2721` | `unblockedTargetsChunk` | `placeholders` |
| `:3066` | `edgesByIDsChunk` | local `placeholders` |
| `:3578` | `Objects().ListByIDs` | `placeholders` |

Three helpers render: `placeholders` (`:3582`), `tupleRows` (`:3588`) and
`kindTuples` (`:3593`), the last serving two sites. `:1154` and `:3066` build the
placeholder list into a **local** `[]string` named `placeholders`, shadowing the
package function, so a grep for `placeholders(` misses both.

Six of the thirteen only ever run on the writer, `unblockedTargetsChunk` (`:2721`)
among them: it reads through `s.read`, but is sound only inside the write
transaction that set the marks it reads (`:2694`), so it never sees the reader.
That decides nothing about how they are routed — the toll below is charged per
connection, not per pool — but it is what the count of seven rests on.

`Events().List` (`:2236`) is the judgement call: its `WHERE` is assembled from an
optional predicate slice plus an optional `LIMIT`, so up to 32 bounded shapes.
Excluded anyway — the bound is a property of today's `EventQuery`, not of the
code, and a sixth optional predicate doubles it silently.

**The SQL has to be hoisted into named constants** for the set to be enumerable,
which is what makes the completeness test below possible. That is most of the
diff — and it **breaks two structural tripwires**, which have to be re-keyed in
the same change or two documented invariants go unpinned:

- `TestObjectStatusIsWrittenInOnePlace` (`store_test.go:8661`) asserts that
  exactly `{sqliteStore.objectsCreate, sqliteObjects.UpdateStatus}` hold SQL
  writing `objects.status` — the single-writer property the whole status-baseline
  skip rests on.
- `TestNoWriteBypassesConn` (`store_test.go:9101`) asserts every write is issued
  through `conn`.

Both read through `sqlSites`, which attributes a string literal to the enclosing
`FuncDecl` by walking the file in source order (`inspectPackage`, `:8748`). A
package-level `const` is inside no function, so its literal lands on whichever
function textually precedes it, or on `""`. They fail loudly rather than pass
wrongly, which is the good outcome, but the repair is a decision this spec owes
rather than a mechanical edit.

**The re-keying is two hops.** After this change `UpdateStatus` holds neither the
literal nor the const — it holds a statement *field*. So `sqlSites` resolves
field → const → text: parse the package's const declarations for `name → literal`,
parse `prepareStatements` for `field → const`, then attribute a field reference
inside a `FuncDecl` to that function. The claim each test makes survives exactly:
"which functions issue SQL that writes `objects.status`" becomes "which functions
reference a statement whose SQL writes it". `TestOwnedByIsWrittenInOnePlace` is
call-keyed and unaffected.

**`listObjectsWhere` needs splitting**, because it is the `s.read` for callers on
both sides of the constant line (`:1098`). `Objects().List` (`:1079`) and
`ListByIncomingEdge` (`:1088`) pass constant tails, so each holds a field with
its own whole `SELECT`; `Objects().ListByIDs` (`:3577`) renders its tail and
keeps today's path. Split the helper so the scan-and-attach tail stays shared and
only the query differs.

## What residency costs

**A connection holding prepared statements taxes everything that runs on it**,
prepared and unprepared alike — but the tax is small beside what preparing saves.
Measured on one read connection, on disk, 1000 objects, seventeen columns by
primary key (`BenchmarkResidencyToll`):

| | unprepared | prepared | preparing saves |
|---|---|---|---|
| no statements resident | 24.7 µs | 7.38 µs | −17.3 µs (−70%) |
| 60 resident | 26.9 µs | 7.60 µs | −19.3 µs (−72%) |
| residency costs | +2.3 µs (+9%) | +0.2 µs (+3%) | |

**Arms must be interleaved across separate `go test` invocations.** `-count`
repeats each sub-benchmark consecutively, so slow machine drift lands entirely on
whichever arm ran first: two earlier runs put the unprepared toll at +25% and
+6%, both from that artefact. Every figure above is the median of four
alternating invocations, with under 1.5% spread inside each arm.

**It is a step, not a slope.** Nearly all of the toll arrives by fifteen resident
statements, and sixteen times more adds almost nothing. Whatever the set grows
to, the toll does not — and two resident statements cost under 1%, so a partly
migrated store pays almost nothing, it simply has not collected the win yet.

**The net is not close.** A prepared execution saves about 17 µs; an unprepared
one pays about 2.3 µs. The migration is ahead as long as prepared executions are
more than about an eighth of unprepared ones, which every call pattern in this
store satisfies by a wide margin.

The thirteen rendered statements cannot be prepared, and seven of them can reach
the read pool, so they pay the full toll. What that costs in place
(`BenchmarkUnpreparedBesidePrepared`, `Objects().ListByIDs`, sixty resident):

| | none resident | 60 resident | toll |
|---|---|---|---|
| 1 id | 48.5 µs | 57.5 µs | +19% |
| 1 id, in a read transaction | 53.9 µs | 63.2 µs | +17% |
| 64 ids | 385 µs | 388 µs | +0.8% |
| 64 ids, in a read transaction | 395 µs | 400 µs | +1.2% |

Those four are from grouped `-count` runs, so read them for shape and not for
magnitude: a large batch amortises the toll to nothing, a single id does not, and
`ListByIDs` takes whatever a tailer page holds. Re-measure them alternating if
the exact figure ever matters.

**None of this moves the headline.** The −63% end to end was measured against a
store with 22 statements resident, so the tax is already inside it.

**Likely mechanism, and why it is worth a line.** SQLite's per-connection
lookaside allocator: each resident statement holds lookaside memory for its
lifetime, and once the small default pool is exhausted every allocation on that
connection falls back to the general allocator. It predicts exactly what was
measured — a step completing around fifteen statements, flat to 240, paid by
prepared and unprepared alike. Not actionable: `modernc` exposes no lookaside
knob, and the word does not appear in the package. Worth stating so the next
reader does not go looking for a way to make the toll shrink with the set.

## The contract this rests on

**A prepared statement is executed only where nothing else can reach it
concurrently.**

A compiled statement is one handle per connection, and `modernc`'s rows keep it
positioned until they close (`stmt.pstmt`, and `reuseStmt = true` on the rows it
returns). Two callers sharing one handle is silent corruption, not an error.
Measured, two goroutines each listing 1000 rows through one shared statement:
**309** and **1691** rows, `err == nil`.

**Outside a transaction, the pool guarantees it.** `database/sql` checks a
connection out to one caller for as long as its rows are open, and compiles a
separate driver statement per connection. Two concurrent callers hold two
handles; at a pool of one they queue instead. A `*sql.Stmt` is safe for
concurrent use, and that is why.

**Inside a transaction, the contract guarantees it**, and it is already written
down (`internal/storeapi/storeapi.go:429`): *"A transaction ctx belongs to one
goroutine — the refusal is a tripwire, not a lock."* `database/sql` permits
concurrent use of a `Tx` and serialises the calls, which is what makes the
violation silent rather than an error: the calls are ordered, the cursor is not.
Nothing else in this design needs a promise from anyone.

Four places say otherwise, say nothing, or overstate what is being guarded.
This change fixes them:

- **`txState.mu`'s comment** — *"AfterCommit and bare reads stay legal
  concurrently"* — asserts the opposite of the contract. It describes what the
  implementation tolerates today. Correct it to the rule.
- **`ControllerClient.Within`'s godoc** says "pass fn's ctx to every store call
  it makes" and nothing about goroutines. The contract lives on
  `internal/storeapi`, which an embedder never sees. Add the rule where a
  controller author reads it.
- **The README's `Within` rule** (line 633) covers which ctx to pass. Add what
  goes wrong if fn fans out: interleaved rows, no error.
- **[The read-transaction ADR](../adr/2026-08-20-a-read-that-groups-is-a-read-transaction.md)**
  justifies two of its defences — the nested read's savepoint, and putting the
  read-frame draw check inside `sealForCommit` — as guards against a sibling
  goroutine committing or drawing mid-read. Both are worth keeping and neither is
  a guard: they are tripwires for a contract violation, which is what
  `ErrConcurrentNestedTx`'s own doc already calls them. Say that, so the next
  reader does not take concurrent use of a transaction for a case the design owes
  an answer to.

**Enforcement stays partial, deliberately.** A sibling goroutine entering a
nested `Within` is refused by `pushSavepoint`'s depth check, which covers the five
self-wrapping store methods. Overlapping *bare* reads are not detected, and
catching them would mean a `defer` in every store method — the hazard window is
"cursor open", not "call in progress", so a chokepoint counter would drop while
rows are still live. That is the same posture as the
[sole-writer rule](../adr/2026-08-05-one-process-one-beehive-sole-writer.md),
which is documented and not enforced, and it is a bigger constraint than this one.

## Edge cases the implementer would otherwise guess at

- **A `*sql.Stmt` belongs to the pool that prepared it.** Executing a read-pool
  statement on the writer's transaction is a silent stale read, not an error.
  `stmtFor` is the only place that chooses, which is what keeps that
  unexpressible at a call site.

- **The writer's idle reap now matters.** `OpenPool` sets
  `SetConnMaxIdleTime(5 * time.Minute)` and no `MaxIdleConns`
  (`internal/sqlitemigrate/sqlitemigrate.go:57`). Reopening a connection drops
  every statement compiled on it, so a beehive quiet for five minutes — which
  `examples/lowpower` is by design — pays every writer parse again on its next
  write. Set it to 0, for the reason `OpenReadPool` already keeps its idle
  readers: *"either would have a quiet beehive drop reader connections between
  ticks and reopen them on the next one."* The reader needs no change.

- **Preparing compiles on one connection, not none and not all.**
  `DB.PrepareContext` compiles on the connection it grabbed and records it in
  `stmt.css`; the pool's other connections compile at first use. That is the
  whole reason the warm-up exists, and the reason it skips the writer.

- **A warm-up that does not hold its connections warms one.** `database/sql`
  hands back the connection just released, so a loop that runs a statement N
  times compiles it on the same connection N times. Hold every `*sql.Conn` until
  the last one is out.

- **Compiled programs scale with statements × connections**, and live for the
  process. Sixty over five connections is the resident cost, not sixty.

- **Each execution inside a transaction allocates a tx-scoped `*sql.Stmt`** that
  `Tx.StmtContext` appends to `tx.stmts`, released when the transaction ends. At
  the sizes beehive's transactions run — a handful of statements — that is
  nothing.

- **`Close` closes the statements before the pools.** Not because it would
  otherwise block — measured, `db.Close()` with live statements returns nil
  immediately — but because it frees the driver's compiled programs. Idempotent,
  so `Close` stays so.

- **A failed preparation fails `Open`**, and must close whatever opened above it.
  `open` already has that shape for `seedVersions` (`sqlite.go:111`).

- **PRAGMAs are not fields.** `ReclaimSpace` takes its own `*sql.Conn` on the
  writer; `PRAGMA incremental_vacuum` must be `Exec`'d and carries its page count
  in the text.

- **Transaction control and savepoint SQL go straight at the transaction**
  (`sqlite/store.go:331`). Savepoint names are interpolated, so the text varies
  by construction.

## Tests

In `sqlite/store_test.go`:

- **A prepared read inside a read transaction sees the transaction's snapshot.**
  Warm the statement, commit a write from the writer while the read transaction
  is open, read again: it must return the pre-write row. The regression test for
  executing `st.read` instead of `tx.StmtContext` — on a pool of one that mistake
  hangs, but on four it quietly answers from outside the snapshot.
- **A read inside a write transaction sees that transaction's own uncommitted
  writes.** The regression test for case 3, and the reason both pools are
  prepared: write inside a `Within`, read the same row, see the new value.
- **Completeness: no constant SQL reaches an unprepared path.** The load-bearing
  test of this design, and the only guard against a new hot read being silently
  unprepared. Walk the package for const string declarations holding SQL, compare
  against the two statement sets by reflection, and fail on a constant no set
  names. It shares its machinery with the re-keyed `sqlSites`, which is the
  argument for doing the re-keying properly rather than patching it.
- **The re-keyed tripwires still hold**: `TestObjectStatusIsWrittenInOnePlace`
  and `TestNoWriteBypassesConn` name the same functions after the hoist as
  before.
- **A write is never prepared on the reader.** Assert the read set holds no
  statement whose SQL writes — the failure `Open` cannot catch, since preparing
  a write against `query_only` succeeds.
- Data-rendered SQL stays unprepared, one case per site, including both sides of
  the `listObjectsWhere` split.
- Concurrent reads through one prepared statement each see every row — the
  regression test for the shared-handle corruption. It can only fail on broken
  code, so it is a detector rather than a flake.
- The warm-up compiles on every reader connection, not N times on one: assert
  over the connections it held, never over timing.
- A failed preparation fails `Open` and leaks no pool.
- `Close` returns nil, twice, and releases the statements.
- `OpenMemory` prepares once and the suite still passes over it.

Benchmark the pool read, a read transaction and a write transaction, on disk —
one per column of the table in *Why*, so each claimed number has a tripwire.
`OpenMemory` shares one connection between the pools, a shape the on-disk store
never has, so it is the wrong place to measure any of them.

## Not in this spec

**A statement table filled at runtime**, keyed by SQL text. It catches every
statement rather than the named ones, and pays for that with a fill path that
cannot prepare inside a transaction, a cap that must never evict, and a drift
guard over rendered SQL. This store's SQL is fixed at compile time, so what the
catching buys is worth less than the machinery costs. Git holds the worked
version.

**Read-pool pragmas and `PRAGMA optimize`**, both in [`TODO.md`](../TODO.md). The
first is one line of DSN that may be worth more than this whole spec, and it
should be priced on the benchmarks above.

**`OpenMemory`'s five-minute idle reap discards the database**, since
`file::memory:` is per-connection. This spec sets it to 0 because it also drops
prepared statements, but the bug predates it and is worth fixing whether or not
this ships.
