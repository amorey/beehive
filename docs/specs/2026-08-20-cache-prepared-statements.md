# Cache prepared statements

- **Status:** Planned, with one benchmark owed *before* implementation — see
  *Measure this first*. The question this spec turned on is settled: the table
  serves the read pool, and the writer is left alone.
- **Date:** 2026-08-20
- **Depends on:** two things already shipped.
  [Reads get their own connections](../adr/2026-08-20-reads-get-their-own-connections.md)
  draws the line the table stops at.
  [A read that groups is a read transaction](../adr/2026-08-20-a-read-that-groups-is-a-read-transaction.md)
  is **unexported** — no caller can open a read transaction, which is what makes
  caching inside one safe without asking anyone to promise anything.

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

**On the read pool, and nowhere else.** Bare reads, and reads inside a read
transaction. Every statement the writer runs — every write, and every read inside
a write transaction — is left exactly as it is today.

The store measured four ways, before any split:

| | pool read | write transaction | ten reads in a transaction |
|---|---|---|---|
| no cache | 21.2 µs | 89.0 µs | 202.4 µs |
| cached everywhere | 7.8 µs (−63%) | 67.2 µs (−24%) | 73.4 µs (−64%) |
| cached except cursor-holding reads in a transaction | 7.7 µs (−64%) | 81.6 µs (**−8%**) | 255.5 µs (**+26%**) |

The third row is what happens when an uncached statement shares a connection with
cached ones: it pays for the caching without receiving it. Reading the table
against the rule above:

- **Pool read** takes the first column: −63%, the widest number here and the one
  on the hottest path.
- **Write transaction** is untouched. Not the −24%, and not the −8% either —
  nothing on that connection changes, so the figure stays 89.0 µs.
- **Ten reads in a read transaction** is the second row's shape: one connection,
  every statement on it cached. The 73.4 µs should carry, but it was measured on
  the writer, so treat it as the expectation and not as a measurement of this
  configuration.

**The third row is what we are buying our way out of, and one edge of it
remains.** A caller's `Within` cannot regress: the writer carries nothing cached
to mix with. The reader can — up to seven of the excluded data-rendered reads run
there beside cached statements, the same mixed shape. That is the one number to settle
before building any of this; see *Measure this first*.

## Filling the table

The table holds `*sql.Stmt` values prepared on the read pool. Outside a
transaction they are executed directly; inside one they are not, and that is the
part to get right.

**A hit inside a read transaction executes through `tx.StmtContext(ctx, stmt)`,
never through the pooled statement itself.** A `*sql.Stmt` from
`readDB.PrepareContext` belongs to the *pool*: calling its `QueryContext` inside
`withinRead` checks out a second connection, so the read runs outside the
transaction's snapshot — the silent failure `read` exists to prevent
(`sqlite/store.go:531`) — and at `WithReadConnections(1)`, or under `OpenMemory`,
it deadlocks instead of lying. `StmtContext` reuses the driver handle if this
connection already compiled it and compiles it on this connection otherwise,
taking no second connection either way.

**A hit outside a transaction executes the pooled statement directly.**

**A miss inside a read transaction cannot fill the table.** Putting an entry in it
means `readDB.PrepareContext`, which waits for a connection while this goroutine
is holding one — a guaranteed deadlock at a pool of one, and at N when N readers
miss at once. Run the statement raw, queue its SQL, and prepare it once the
transaction has returned.

**A miss outside a transaction prepares inline.** Nothing holds a connection, so
this is an ordinary pool wait.

The queue is what the implementer would otherwise get wrong:

- **It lives on the `txState`, not the `txFrame`, and drains only when the
  outermost frame returns.** A nested `withinRead` joins on a savepoint with a new
  frame and the same state (`store.go:652`); draining when *that* frame returns
  calls `PrepareContext` while the outermost transaction still holds the
  connection, which is the deadlock the queue exists to prevent.
- **It is guarded by `txState.mu`**, like every other field there: sibling
  goroutines on one frame are legal.
- **It drains synchronously in `withinRead`'s tail**, after `runTx` has returned
  and released the connection, on a `context.WithoutCancel` ctx. Not in `settle`,
  which is commit-only, and not in a goroutine: the store owns none.
- **It drains whether or not `fn` succeeded**, or a read transaction that errors
  leaves nothing warm.

What the queue holds is SQL, never a marker in the table. A marker outlives the
transaction that would have cleared it, so a failed read leaves that SQL uncached
for the life of the store.

**The table warms after one call; the compilation does not.** A statement is
compiled per connection, so on a four-connection reader a hot statement is
compiled four times, lazily, one reader at a time.

**Every hit inside a read transaction allocates a tx-scoped `*sql.Stmt`** that
`Tx.StmtContext` appends to `tx.stmts`, released only when the transaction ends.
At the two to four reads the four `withinRead` bodies make, that is nothing — and
the body that would make it something is the long, fanning-out one the invariant
below already forbids.

## The accessors

Two, and the routing lives inside them so no call site decides anything:

- **`s.read(ctx)`** keeps its name and now returns a caching `roDBTX`. It caches
  when the ctx carries no live transaction, or one that is `readOnly`; otherwise
  it hands back what it hands back today. Both `QueryContext` and
  `QueryRowContext` route — 17 of the 39 sites are `QueryRowContext`, so a caching
  pair that covers only `Query` covers less than half of them.
- **`s.readRaw(ctx)`** is today's behaviour, for the data-rendered reads below.

Seven of the 39 sites move to `readRaw`. The other 32 are untouched — including
`listObjectsWhere` (`:1098`), which needs the split described next rather than a
move.

**`listObjectsWhere` is shared across the line and has to be split.** It is the
`s.read` for all three of its callers: `Objects().List` (`:1079`) and
`ListByIncomingEdge` (`:1088`) pass constant tails and are cacheable, while
`Objects().ListByIDs` (`:3577`) passes a rendered one and must not be. Give it a
raw twin — the body takes the `roDBTX`, `listObjectsWhere` passes `s.read` and
`listObjectsWhereRaw` passes `s.readRaw` — and point `ListByIDs` at the twin.
`ListByIDs` is the statement *Measure this first* benchmarks, so this split comes
before the experiment, not with the migration.

`conditionSetLoad` is the near miss that needs nothing: it takes `s.read`
directly and has one caller.

## The rule everything rests on

**A statement is cached only where nothing else can reach it concurrently.**

A cached statement is one compiled handle per connection, and `modernc`'s rows
keep it positioned until they close (`stmt.pstmt`, and `reuseStmt = true` on the
rows it returns). Two callers sharing one handle is silent corruption, not an
error. Measured, two goroutines each listing 1000 rows through one shared
statement: **309** and **1691** rows, `err == nil`.

The read pool is where that rule holds for free.

**Outside a transaction, the pool guarantees it.** `database/sql` checks a
connection out to one caller for as long as its rows are open, and prepares a
separate driver statement per connection. Two concurrent callers hold two
handles; at a pool of one they queue instead. Safety comes from the pool at any
pool size, `OpenMemory` included, not from a rule anyone has to follow.

**Inside a read transaction, an invariant guarantees it — not the pool.**
`StmtContext` deliberately reuses the driver handle already compiled on the
transaction's connection, `database/sql` does not serialize a `Tx`, and
`txState.mu` licenses sibling goroutines on a frame. Two goroutines issuing the
same SQL inside one read transaction would therefore share one handle. What stops
them is that **no `withinRead` body fans out**: all four run strictly
sequentially, and `sqlite/store.go` contains no `go` statement at all.
`withinRead` being unexported is what keeps that true — only this package can add
a caller — but it is not what makes it true, and the difference matters, because
a fifth caller that spawns a goroutine is silent corruption rather than an error.

State the property on `withinRead`, where a fifth caller will read it, and pin it
structurally: a test that walks the package AST, finds every `withinRead` call,
and fails on a `go` statement in the literal it is passed. The suite already
carries this kind of tripwire — `TestNoWriteBypassesConn`,
`TestEveryDrawSiteIsInsideATransaction`, `TestOwnedByIsWrittenInOnePlace`.

**Inside a write transaction the hazard is real**, because the connection is
already held and `txState.mu` licenses sibling goroutines: *"AfterCommit and bare
reads stay legal concurrently."* Nothing on the writer is cached, so nothing there
can be entered twice.

## Why the writer is left alone

Earlier drafts turned on a question — how a statement inside a transaction
becomes safe to cache — and offered three answers: **mark the origin** (a trusted
flag on beehive's own transactions), **narrow the contract** (document that a
`Within` fn must not issue concurrent store calls), or **decline** and cache only
where a cursor cannot be shared.

Confining the table to the read pool answers it by removing it. Against the
three:

- Nothing is withdrawn from `ControllerClient.Within`, so no controller author
  who fans out breaks silently.
- No origin flag, and none of the plumbing to carry one from `beehive` into
  `sqlite`.
- No assumption to state or pin. *Mark the origin* had one — that `client.go`'s
  `c.decode`, which runs the caller's `UnmarshalJSON` inside a transaction, does
  not fan out concurrent store calls.
- No path gets slower. *Decline* was the +26% row applied to all 23 of beehive's
  own transactions; *mark the origin* was the same row applied to a caller's,
  which is a regression on a public path that its recommendation did not price.

**What it gives up is the −24% on write transactions.** That is the whole cost,
and it is recoverable later: with the read pool cached and measured, marking the
origin or narrowing the contract is still available as a follow-on, against
evidence rather than against an estimate.

## Measure this first

**Before the migration, not after it.** The +26% row is the whole argument for
leaving the writer alone, and its mechanism has never been isolated. By *What may
be cached*, up to seven excluded data-rendered reads run on the read pool beside
cached statements — the same mixed shape. If that costs what the third row costs, the
conclusion does not take a patch: it flips toward caching everywhere, which is the
row that measured −24% and −64%.

The experiment needs a table and one hot read, not the 39-call-site migration:
benchmark `Objects().ListByIDs` on a reader whose table is warm against the same
read with the table empty, on disk. Settle that number, then build.

## What may be cached

The line is **whether the text varies with data**, not whether a function built
it. `selectScoped`, `listObjectsWhere`, `trimEvents`, `eventHorizon` and
`deleteWriteLogRows` all take a fragment parameter and are fine: their text ranges
over a handful of compile-time constants.

Text rendered from a **runtime count** must not be cached — one entry per arity
fills the table with single-use statements, and a batch already amortises one
preparation over every row it binds. A count, not a slice length:
`conditionSetLoad(types int)` renders from a number, and a rule stated over
slices does not reach it.

Thirteen call sites, line numbers as of this branch:

| Site | Function | Renders with | Can reach |
|---|---|---|---|
| `:906` | `appendWriteLogUpdates` | `tupleRows` | writer |
| `:1154` | `conditionsByIDsChunk` | local `placeholders` | reader |
| `:1237` | `ReconcileOwed().Stamp` | `placeholders` | writer |
| `:1249` | `reconcileOwedSweepQuery` | `kindTuples` | writer |
| `:1325` | `Dependencies().ListStaleSince` | `kindTuples` | reader |
| `:1520` | `readImages` | `placeholders` | reader |
| `:1870` | `conditionSetLoad` | `placeholders(types)` | reader |
| `:2052` | `upsertConditions` | `tupleRows` | writer |
| `:2236` | `Events().List` | `strings.Join(where)` | reader |
| `:2629` | `markManyForDeletionChunk` | `tupleRows` | writer |
| `:2721` | `unblockedTargetsChunk` | `placeholders` | writer |
| `:3066` | `edgesByIDsChunk` | local `placeholders` | reader |
| `:3578` | `Objects().ListByIDs` | `placeholders` | reader |

Three helpers render: `placeholders` (`:3582`), `tupleRows` (`:3588`) and
`kindTuples` (`:3593`), the last serving two sites.

The last column is *paths that reach it*, not a property of the statement.
`conditionsByIDsChunk` and `edgesByIDsChunk` are called from both kinds of
caller; `unblockedTargetsChunk` reads through `s.read` but is sound only inside
the write transaction that set the marks it reads (`:2694`), so it never sees the
reader. Route on the SQL's own shape regardless: which connection a call uses
today is a fact about the call graph, and it changes.

The six writer rows are excluded twice over as things stand. **The seven that can
reach the reader are what costs something** — they are the uncached statements
that will sit beside cached ones on the read pool, the shape *Measure this first*
is about. Seven is the upper bound, since two of them only sometimes arrive
there.

`:1154` and `:3066` build the placeholder list into a **local** `[]string` named
`placeholders`, shadowing the package function. A grep for `placeholders(` misses
both — which is how the fourteenth gets added unnoticed.

`Events().List` (`:2236`) is the judgement call: its `WHERE` is assembled from an
optional predicate slice plus an optional `LIMIT`, so up to 32 bounded shapes.
Raw anyway — the bound is a property of today's `EventQuery`, not of the code, and
a sixth optional predicate doubles it silently.

## Edge cases the implementer would otherwise guess at

- **The table belongs to the read pool.** A `*sql.Stmt` belongs to the pool that
  prepared it, so one prepared on the writer and executed through `s.read` runs on
  the writer and quietly undoes the
  [split](../adr/2026-08-20-reads-get-their-own-connections.md). One table, reached
  only from `read`, is what keeps that from being expressible.

- **Route on the frame, not on the method.** A statement is cached when the ctx
  carries no live transaction, or one that is `readOnly`. A `withinRead` nested
  inside a caller's `Within` joins the writer's frame on a savepoint, so it is
  neither — and stays uncached, with no special case needed to say so.

- **A statement is compiled once per connection.** `database/sql` prepares
  lazily, so a hot statement is compiled on each reader that runs it. Memory scales with N,
  and so does the small residual cost a cached statement pays on a busy
  connection (+7%).

- **The reader is already exempt from idle reaping; `OpenMemory` is not.**
  Reopening a connection drops every statement compiled on it, so an idle reap is
  a repeated cold start once the table exists. `OpenReadPool` sets no
  `ConnMaxIdleTime` and keeps `MaxIdleConns(maxConns)` — deliberately, and it says
  so (`internal/sqlitemigrate/sqlitemigrate.go:81`). The five-minute reap is
  `OpenPool`'s, on the writer, which holds no table. The one pool that reaps a
  table-holding connection is `OpenMemory`'s (`sqlite.go:92`). Change nothing;
  know which is which.

- **Never prepare while holding a connection.** The rule *Filling the table*
  rests on: a prepare inside a read transaction waits for a connection this
  goroutine already holds — deadlock at a pool of one, and at N when N readers
  miss at once. Hence the queue. Say it where the table is defined; it is
  invisible otherwise.

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
  read path is a lookup under `RLock` that N readers take concurrently.

- **PRAGMAs stay off the table.** `ReclaimSpace` takes its own `*sql.Conn` on the
  writer; `PRAGMA incremental_vacuum` must be `Exec`'d and carries its page count
  in the text.

- **Transaction control and savepoint SQL go straight at the transaction**
  (`sqlite/store.go:331`), never through the table.

- **`OpenMemory` keeps the table, on a connection the on-disk store never has.**
  `readDB` is aliased to `db` there, so cached read statements sit on the same
  single connection that runs write transactions — the mixed shape the read-pool
  boundary exists to avoid. Correct, because the pool still hands that connection
  to one caller at a time; but it means the in-memory suite does not measure the
  on-disk shape, so **the benchmarks run on disk**.

## Tests

In `sqlite/store_test.go`:

- A read outside a transaction is prepared once and reused.
- **A cached read inside a read transaction still sees the transaction's
  snapshot.** Warm the statement inside a read transaction, commit a write from
  the writer while that transaction is open, read again through the cached
  statement: it must return the pre-write row. This is the regression test for
  executing a pooled `*sql.Stmt` instead of `tx.StmtContext`, and nothing else in
  this list catches it — on a pool of one that mistake hangs, but on four it
  quietly answers from outside the snapshot.
- A read *inside a read transaction* runs uncached the first time and cached the
  next, which is the queue working; the table does not grow mid-transaction.
- A nested `withinRead` drains nothing until the outermost frame returns.
- A read *inside a write transaction* is never cached, and neither is a write. The
  table does not grow either way.
- A `withinRead` nested inside a `Within` caches nothing.
- Concurrent reads through one cached statement each see every row — the
  regression test for the shared-handle corruption. It can only fail on broken
  code, so it is a detector rather than a flake.
- Data-rendered SQL never reaches the table, one case per chunked call site —
  including both sides of the `listObjectsWhere` split: `Objects().List` caches,
  `Objects().ListByIDs` does not.
- No `withinRead` body fans out: the AST tripwire from *The rule everything rests
  on*, which is what makes caching inside a read transaction safe.
- The cap stops filling and never evicts; cached statements keep working.
- A failed preparation leaves nothing behind and the next call retries.
- `Close` returns nil, twice, and releases the table.

**The drift guard, in two halves.** Collapsing placeholder runs and `VALUES`
tuple sets to one and comparing entries catches a rendered statement — but only
once two arities of the same shape are cached, and a site that always binds a full
chunk, or a test that always passes the same length, produces one key and passes.
So pair it with a **golden list of the cached SQL**, asserted at the end of the
suite: a fourteenth site then shows up as a diff rather than as a collision that
may never happen. Matching the placeholder text directly does not work:
`placeholders(n)` renders `"?, ?"` **with a space** (`:3583`), and a hand-written
constant `VALUES (?, ?)` would fail it. Run the collision half from the suite's
store constructor so every test is a sample, not from one hand-picked test.

Benchmark three, on disk: the pool read and a read transaction for the win, and a
write transaction for the "unchanged" claim, which is a promise this design makes
and not merely an omission. The fourth — the excluded data-rendered read beside
cached ones — is *Measure this first*, and it runs before any of this.

## Not in this spec

**Eagerly preparing a fixed list at `open()`.** It buys no speed — lazy filling
pays the same ~19 µs per shape, just at first use — and it adds ~1.3 ms to
`open()` for statements a short-lived process may never run. What it would buy is
SQL validated at open, so a typo on a cold path is a startup error rather than a
runtime one. A different argument, with a list to maintain; decide it separately,
against an embedder that cares about startup.

**Caching on the writer.** Worth −24% on a write transaction, and unavailable
without either marking beehive's own transactions or narrowing what a `Within` fn
may do — see *Why the writer is left alone*. Decide it after this ships, with the
reader's numbers in hand.
