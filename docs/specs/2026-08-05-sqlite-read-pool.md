# A read pool beside the write pool

- **Status:** Proposed — [step zero](#step-zero-does-modernc-actually-do-this)
  run and passing; the design now covers the reads that wrap themselves. See
  [Open](#open).
- **Date:** 2026-08-05 (revised 2026-08-06 against `b4e674c`)
- **Tracks:** the "reads and writes share one connection" and "page cache and
  `mmap_size` are untuned" entries in [`docs/TODO.md`](../TODO.md).

## Problem

`sqlitemigrate.OpenPool` sets `journal_mode(WAL)`, which lets one writer and
many readers run at the same time. `sqlite.Open` then passes `maxConns = 1`
(`sqlite/sqlite.go:36`), so every read queues behind every write in Go's pool
and that concurrency is unused. The limit is `database/sql`, not SQLite.

The cost is paid by anything that reads while a write is in flight — today the
watch tailers, tomorrow the drivers as their count grows.

### Measured

`BenchmarkWritesUnderWatch` (`objectswatch_bench_test.go`), on disk, beehive not
started so no driver competes:

| Watches | ns/op | p50 | p99 |
| --- | --- | --- | --- |
| none | 172,000 | 151 µs | 465 µs |
| 1 kind, 1 watch | 215,000 | 190 µs | 550 µs |
| 1 kind, 64 watches | 223,000 | 197 µs | 715 µs |
| 16 kinds, 1 watch each | 326,000 | 271 µs | 995 µs |

One watch costs about a quarter of write throughput. Watch *count* is free —
the shared tailer working as designed. Watched *kinds* is the axis that costs:
the writes round-robin, so each tailer wakes with nothing to coalesce and 16
drains contend for the one connection. That row is the worst case by
construction; real traffic bursts per kind and collapses more. Absolute numbers
are machine-specific, the deltas are the finding.

## Goals

- Reads issued outside a transaction stop queueing behind writes.
- **Multi-statement reads that need one snapshot get it from the read pool**,
  not from the write pool's `BEGIN IMMEDIATE`.
- The write path keeps exactly today's serialisation: one writer, queued in Go,
  never `SQLITE_BUSY`.
- A read issued with a transaction ctx keeps reading that transaction's
  uncommitted state — no new class of silent stale read.

## Non-goals

- Concurrent writers. `_txlock=immediate` is load-bearing for read-modify-write
  atomicity (`sqlite/store.go:446`); nothing here relaxes it for the write pool.
- Changing `Store`'s interface, or any driver's cadence or budget.
- Making `OpenMemory` concurrent (see [In memory](#in-memory)).

## Design

### Two pools, not a larger one

Raising `maxConns` on the single pool breaks writes: with `_txlock=immediate`
every `Within` takes `BEGIN IMMEDIATE`, so concurrent transactions would collide
at the SQLite level and take `SQLITE_BUSY` behind a 5s `busy_timeout`, where
today they queue in Go. Instead:

- **write pool** — unchanged: one connection, `_txlock=immediate`.
- **read pool** — N connections, `_pragma=query_only(true)`, **and no
  `_txlock`**.

`query_only` rather than `mode=ro`: a read-only connection cannot recover the
`-wal`/`-shm` files, so a `mode=ro` pool fails on a database no writer has
opened yet, and on recovery after a crash. Step zero confirms both cases.

Dropping `_txlock` is now load-bearing rather than forward-looking. `OpenPool`
hard-codes `_txlock=immediate` (`internal/sqlitemigrate/sqlitemigrate.go:50`);
left on a `query_only` pool, any `BeginTx` there issues `BEGIN IMMEDIATE`, takes
the write lock and fails `SQLITE_READONLY`. `s.readWithin` (below) does exactly
that `BeginTx`, on day one and on the hottest read path in the store. So
`OpenPool` grows an **options struct**, not a bool: the two pools differ in three
independent ways (`maxConns`, `query_only`, `_txlock`) and a positional bool
cannot say which.

Only the write pool runs `Apply`.

`N`: start at `min(4, GOMAXPROCS)`. A constant, not an option — there is no
measurement behind a knob yet, and the benchmark's 16-kind row is what a larger
`N` would have to move before one is worth exposing. Note what `N` bounds once
`readWithin` exists: not concurrent read *statements* but concurrent read
*protocols*, since a `readWithin` frame holds its connection across a page read
plus a horizon read. The 16-kind acceptance row is 16 tailers against `N`, so
their drains serialise four at a time by construction. Still 4× today, and the
row should move — but the two quantities are not the same guess.

### `s.read(ctx)`

`s.conn(ctx)` (`sqlite/store.go:435`) is already the single place connection
selection happens. Add a sibling:

```go
// read returns the ambient transaction if ctx carries a live one, else the read
// pool. The transaction case is not an optimisation: a read that skipped it
// would silently miss the transaction's own uncommitted writes.
func (s *sqliteStore) read(ctx context.Context) dbtx {
	if fr, ok := txFrom(ctx); ok && !fr.st.isClosed() {
		return fr.st.tx
	}
	return s.readDB
}
```

The two functions differ only in their fallback, which is the point: the
transaction branch is shared and stated once.

### `s.readWithin(ctx, fn)` — the part an earlier draft missed

**Four read-only methods already wrap themselves in `Within`**, and between them
they are six of the store's public reads and most of what the watch paths
actually cost:

| Method | Declared | Wraps at |
| --- | --- | --- |
| `EventsSnapshot` | `:1792` | `:1797` |
| `EventsListSince` | `:1816` | `:1824` |
| `ObjectWritesListSince` | `:2757` | `:2764` |
| `snapshot` — `ObjectWritesSnapshot` (`:2926`), `…ByID` (`:2934`), `…ByOwner` (`:2949`) | `:2955` | `:2962` |

They wrap for a reason that is not going away: each reads two things that must
agree. `EventsSnapshot` reads runs and position in one transaction "because two
reads either drop a write or deliver it twice" (`CLAUDE.md`); the write-log
readers pair a page of entries with the retention horizon, and a page read
against one snapshot and a horizon read against a later one can claim a loss that
did not happen, or miss one that did.

By the rule in [Reads inside `Within` get no benefit](#reads-inside-within-get-no-benefit),
`s.read(ctx)` inside these returns the ambient transaction — so moving their leaf
helpers onto `s.read` is a **literal no-op**. That matters because it is
precisely the expensive statement in each protocol:

- **`objectTailer.drain`** is `ObjectWritesMaxVersion` (`:2800`, bare — moves) →
  `ObjectWritesListSince` (`:2757`, wrapped — would not move) →
  `ObjectsListByIDs` (`:2980`, bare — moves). The page read is the one that
  stays.
- **The event watch** is `EventsMaxVersion` (`:1894`) and `ObjectsGetMeta`
  (`:774`), both bare, around `EventsListSince` and `EventsSnapshot`, both
  wrapped.
- **A client watch's opening snapshot** is `ObjectWritesSnapshot*` — wrapped
  end to end.

Left as-is, the change would move the scalar gates and leave the page reads
behind, and [Acceptance](#acceptance) would fail for a reason that has nothing to
do with whether the connection was the contention.

There is a second consequence, smaller but worth stating because it is easy to
overstate. `Within` calls `s.db.BeginTx` (`:459`) on a DSN carrying
`_txlock=immediate`, so these read-only methods take `BEGIN IMMEDIATE` — the
write lock — for the duration of the page read. On today's `maxConns = 1` that
costs little on its own: the single connection already serialises everything, so
the lock class buys nobody anything and the overhead is the extra
`BEGIN`/`COMMIT` pair. Its significance is structural, not a share of the
current regression: it is what puts these reads on the write pool, and it is why
the fix is a real read-transaction path rather than a wider pool.

So:

```go
// readWithin runs fn inside one read snapshot on the read pool. It joins the
// ambient transaction instead if ctx carries a live one, so a read nested in a
// caller's Within still sees that transaction's uncommitted writes.
//
// fn must not write: the frame it installs is resolved by conn as well as read,
// so a write inside fn fails SQLITE_READONLY on the query_only pool rather than
// landing on the write pool outside the snapshot.
func (s *sqliteStore) readWithin(ctx context.Context, fn func(ctx context.Context) error) error
```

Three properties, each deliberate:

- **It joins an ambient transaction** rather than opening a second one, by
  delegating to `s.Within` when `txFrom(ctx)` is live. That preserves nested
  `Within`'s SAVEPOINT semantics and the goroutine-ownership refusal
  (`ErrConcurrentNestedTx`) for free, and keeps reads-inside-a-caller's-`Within`
  exactly as they are today.
- **Its frame is resolved by `s.conn` too**, not only by `s.read`. That is what
  makes misuse loud: a write reaching a `query_only` connection fails with
  `attempt to write a readonly database (8)`, verified in step zero, including
  from inside an open read transaction. The alternative — a private ctx key that
  only `s.read` consults — would route a stray write to the write pool, where it
  would succeed *outside* the snapshot. Silent, which is the failure mode this
  spec spends §1 avoiding.
- **A read frame carries no `AfterCommit` queue.** A read registers no hooks, and
  `AfterCommit` (`:493`) returns nothing, so there is no error to hand back —
  which leaves panic or log, and it should not be the implementer's call.
  **Panic**, matching the repo's convention for a caller mistake with no return
  path (`panic("not implemented: …")`, the nested-`Within` goroutine tripwire).
  Silently dropping the hook, or silently running it after a read commits, are
  the two outcomes that must not happen. Pinned by test.

The transaction is `DEFERRED` — that is the whole reason `_txlock` comes off the
read pool's DSN.

`Within` itself does **not** learn to route to the read pool when nothing writes.
It cannot know in advance, and a `Within` that guessed wrong would have to
upgrade mid-transaction. The split is explicit: `Within` writes, `readWithin`
reads.

### The call-site change

**Bare reads → `s.read`** (all verified to resolve `s.conn` directly, no
`Within`):

`ObjectsGet` (`:742`), `ObjectsGetForReconcile` (`:753`), `ObjectsGetMeta`
(`:774`), `ObjectsGetByName` (`:787`), `ObjectsList` (`:795`),
`ObjectsListByIncomingEdge` (`:804`), `ObjectsListUnsettledIDs` (`:888`),
`DeletionRequestsList` (`:901`), `ReconcileOwedListIDs` (`:915`),
`ResourceVersionsMaxIssued` (`:930`), `DependentsListStaleSince` (`:1023`),
`ObjectsListIDs` (`:1085`), `ObjectWritesMaxVersionAll` (`:1104`),
`ObjectWritesListSinceAll` (`:1121`), `EventsList` (`:1756`), `EventsMaxVersion`
(`:1894`), `EventsGetLatest` (`:1901`), `EdgesListIncoming` (`:2464`),
`EdgesGroupIncomingByID` (`:2478`), `EdgesListOutgoing` (`:2532`),
`EdgesListOutgoingByRelation` (`:2545`), `EdgesGroupOutgoingByID` (`:2559`),
`EdgesHasIncoming` (`:2594`), `DriverCursorsGet` (`:2611`),
`ObjectWritesMaxVersion` (`:2800`), `ObjectsListByIDs` (`:2980`).

**Self-wrapping reads → `s.readWithin`**: the four wrap sites in the table above.
Their bodies do not otherwise change; their leaf helpers reach the read frame
through `s.read` like everything else.

`ResourceVersionsMaxIssued` is in the first list and its name gains teeth. It
reads the sequence, so on the read pool it returns max-*committed*, never an
uncommitted draw. That is the same answer a bare read gets today, so there is no
behaviour change — but its godoc (`internal/storeapi/storeapi.go:650`) says "the
highest resource version issued", and once snapshot isolation is real,
issued-versus-committed stops being a distinction without a difference. One
clause in the godoc, in the same PR.

Explicitly **not** moved:

- `FreePagesRelease` (`:86`) — `incremental_vacuum` writes; it keeps `s.db.Conn`.
- `pageCounters` (`:127`) — issues no write, so the rule above would sweep it
  onto the read pool, wrongly. `FreePagesRelease` reads counters (`:101`), execs
  `incremental_vacuum`, then re-reads on the **same** `dbtx` (`:114`) to compute
  `released`; split across pools that delta spans two snapshots and the count
  stops meaning anything. It takes its `dbtx` from its caller and must keep
  doing so.
- `writeLogKinds` (`:2861`) and `deleteWriteLogRows` (`:2881`) — reached only
  from `ObjectWritesSweep`'s transaction (`:2822`). Mutator-only; see below.

### There is no shared-helper problem

An earlier draft claimed the shared read helpers (`getObjectRow`,
`listObjectsWhere`, `loadConditions`, `edgesByIDs`, `writeLogPage`, `snapshot`,
…) were a trap, because each is reached both from a bare read and from inside a
mutator's read-modify-write, and a helper hard-coding `s.read` would break the
mutator's compare. That was wrong.

`s.read(ctx)` returns the ambient transaction whenever ctx carries a live one —
it differs from `s.conn` **only** in the no-transaction case. And every mutator
self-wraps, so inside one, ctx always carries a live transaction. The complete
list of self-wrapping mutators, from `grep -n 's.Within(ctx' sqlite/store.go`
minus the four read-only wrappers above:

`ObjectsCreate` (`:552`), `updateSpec` (`:1302`, both `ObjectsUpdateSpec*`),
`ObjectsUpdateStatus` (`:1354`), `ConditionsSet` (`:1578`), `ConditionsDelete`
(`:1616`), `EventsAdd` (`:1706`), `EventsSweep` (`:1908`), `FinalizersDelete`
(`:2020`), `requestDeletion` (`:2136`, both `DeletionRequestsCreate*`),
`DeletionRequestsCreateFromOwner` (`:2215`), `ObjectsDelete` (`:2281`),
`EdgesAdd` (`:2338`), `ObjectWritesSweep` (`:2822`).

So a helper that calls `s.read(ctx)` from inside a mutator gets the transaction,
and the read-modify-write is untouched. `markForDeletion` (`:2071`) was the one
case worth checking, since it resolves `s.conn(ctx)` itself: both call sites —
`:2138` inside `requestDeletion`'s `Within`, and `:2262` inside
`deletionRequestsCreateFromOwner`, reached only under
`DeletionRequestsCreateFromOwner`'s `Within` — are inside a transaction. No
`dbtx` rethreading, no rule about which helpers may resolve their own connection.

**What this leaves is two changes, not one.** The bare-read list is close to a
mechanical `s.conn` → `s.read`. `readWithin` is new machinery — a second frame
kind, a resolution rule shared with `s.conn`, an `AfterCommit` refusal, and its
own tests — and it is the half the win depends on. Cost the PR as that, not as
the rename.

Two residuals, neither structural:

- A helper reached *only* from a mutator gains nothing from `s.read` and should
  stay on `s.conn`. Preference, not correctness.
- The **drain-rows-before-the-next-query** discipline — five sites: `:820`,
  `:860`, `:2249`, `:2502`, `:2880`. Keep all five. It stays load-bearing inside
  a transaction, where both statements share the tx connection — which now
  includes every `readWithin`, so the discipline gets *more* reach, not less — in
  memory, where the pools are aliased, and at `:2249` and `:2880`, which are
  inside mutators and never see the read pool at all. The comments say "the
  single connection"; at `:820`, `:860` and `:2502` that wording narrows to the
  transaction case, and at `:2249` and `:2880` it stands as written.

### The invariants this rests on

There are two, and they are unrelated. The first is a caller-mistake hazard
guarded by one function. The second is a property every driver already depends
on, which is currently implied by `maxConns = 1` and which this change makes
load-bearing on its own.

#### 1. A read on a transaction ctx must reach the transaction

Today a read issued on a non-transaction ctx while inside a transaction
**deadlocks**: it waits for the connection the transaction holds. That is loud
and deterministic, and it is stated as a caller-facing rule in four places:

- `README.md:542` — "Do not open a watch inside `Within`", stated in terms of
  the connection. Per `CLAUDE.md` the README is the spec, so this is the
  caller-facing copy of the guarantee being weakened, and it changes first.
- `internal/storeapi/storeapi.go:369` — `Store.Within`, "deadlocks a
  single-connection backend".
- `controller.go:74` — `ControllerClient.Within`.
- `client.go:289` — `Client.Watch`.

With a read pool the same mistake becomes a **silent stale read**: the read
succeeds against the last committed snapshot and misses the transaction's own
writes. `s.read(ctx)` returning the transaction whenever one is present is the
whole defence, and there is nothing else behind it. `readWithin`'s join-the-
ambient-transaction branch is the same invariant, one level up.

The four rules stay rules; only their stated failure mode changes, from a hang
to a wrong answer. That is a weaker guarantee and has to be said plainly rather
than quietly dropped.

#### 2. Snapshots must be monotone across connections

Three read protocols span several statements with **no** transaction, and today
their correctness argument is "one connection":

- **`staleDependents.sweep`** (`waker.go:686`) reads `mark` via
  `ResourceVersionsMaxIssued` (`:692`), then pages `DependentsListStaleSince`
  bounded by it. The comment at `:688` — "Read the mark before the scan, never
  after, and scan only up to it" — is a snapshot-ordering argument, and it is the
  one that keeps the scan finite under sustained writes.
- **The waker's** cursor read followed by paged `ObjectWritesListSinceAll`
  (`waker.go:483`), plus its per-page `EdgesGroupIncomingByID` (`:603`).
- **`objectTailer.drain`** (`objectswatch.go:519`) reads
  `ObjectWritesMaxVersion`, then the page, then `ObjectsListByIDs` (`:603`).

The waker is the clean case: `grep -c Within waker.go` is **0**, and the same for
`objectswatch.go` and `eventswatch.go` — no driver opens a transaction, so all of
these move. The tailer's middle statement becomes a `readWithin`, which makes its
page-plus-horizon pair *stronger* than today, but the three statements remain
three snapshots relative to one another.

So after this change these statements are separate reads, potentially on
different pool connections, and their correctness rests on **cross-connection
snapshot monotonicity**: statement N, issued after statement 1 returned, must
observe a snapshot including everything statement 1 saw.

WAL provides this — a read transaction takes the current wal-index end-mark, so
snapshots are monotone in real time regardless of which connection serves them —
so all three stay correct. The problem is not that they break; it is that:

- Nothing in the repo states the property, and no test pins it. It is currently
  implied by `maxConns = 1`, which this change removes.
- It is what a future pool-width change, or a new driver written to the same
  read-mark-then-page pattern, would break **silently** — the same failure shape
  §1 spends a section on.

One case reads like a hazard and is not, so say it: the tailer's
`ObjectsListByIDs` after the page depends on monotonicity in the *permissive*
direction. A newer object than the log entry named is fine — delivery is
latest-per-object, and the entry is a routing key, not a payload
(`docs/adr/2026-08-02-object-write-log.md`). Only a *staler* object would be a
bug, and monotonicity is exactly what forbids it.

This gets its own test (§Testing, test 2) and one line in the ADR that replaces
this spec. Left unstated, the spec ships with its most broadly-depended-on new
assumption implicit.

## In memory

`OpenMemory` (`sqlite/sqlite.go:44`) uses `file::memory:`, which is
per-connection: a second pool there is a different, empty database. So
`readDB == db` in memory, `s.read` aliases `s.conn`, `readWithin` is `Within`
on the write pool, and `Close` must not double close.

**This is the spec's weakest point and it is not neutral.** Effectively the whole
suite is `OpenMemory` (`reconciler_test.go`, `waker_test.go`, `client_test.go`,
`beehive_test.go`, every example). So the functions called "the ones that must
not be got wrong" would be covered by a handful of hand-written tests while every
behavioural test in the repo runs the *other* configuration. It is blind in both
directions: a rows-held-open nested query deadlocks only in memory, a stale read
appears only on disk.

`readWithin` sharpens this rather than sharing it. In memory it *is* `Within` on
the write pool, so the `query_only` enforcement behind "fn must not write" — the
thing that makes a stray write loud — **does not exist in memory at all**. A
write added inside a `readWithin` later would pass every in-memory test and fail
only on disk. The guard for this spec's newest invariant is a pool property the
in-memory configuration does not have.

Decision: **add a file-backed constructor and run a slice of the suite on it.**

- `testutils_test.go` grows `newFileStore(t)` — `sqlite.Open(filepath.Join(t.TempDir(), "test.db"))` with a `t.Cleanup` close.
- A suite-level switch (an env var read once into a `storeFactory` var,
  defaulting to memory; `-tags` is wrong here) lets CI run the integration suite
  a second time on disk. **That second run is required, not available** — per the
  paragraph above it is the only run in which the read pool's write refusal
  exists, so a green CI without it does not cover the invariant. A local
  `go test ./...` staying on memory is the point of the default.
- The tests that must run on disk regardless — the nine below, plus
  `BenchmarkWritesUnderWatch` — call `newFileStore` directly rather than going
  through the switch.

**Routing cost, priced.** Cheaper than it looks, because the suite already
funnels. There are **293** `newTestBeehive` calls; `newClientTestStore`
(`client_test.go:222`) is a single chokepoint behind **127** of them and is
called **247** times in all, and the rest take a `store` the test built itself.
Outside `sqlite/` there are **22** direct `OpenMemory()` sites. So the switch is
one helper plus 22 sites, not a sweep.

Two exclusions, which is why it cannot be a blind replace:

- `sqlite/sqlite_test.go`'s `TestOpenMemorySetsAutoVacuum` and its sibling are
  *about* the in-memory DSN. They stay in memory.
- The **34** `fakeStore` sites never touch SQLite at all and are untouched by any
  of this.

**Flake risk, and the answer.** The integration suite is signal-synchronised
against `fastTick = 2 * time.Millisecond` (`testutils_test.go:185`). On disk,
with `synchronous(NORMAL)` fsyncs in the loop, a 2ms tick is a plausible flake
source, and an on-disk run that flakes is worse than no on-disk run — it teaches
people to re-run CI. **The on-disk run gets its own tick constants**: `fastTick`
and `staleDependentsTick` (`:195`) become vars the `storeFactory` switch scales
(start at 10×), keeping the `10 ×` ratio between them that `:187` documents. If
that still flakes, the on-disk run narrows to the tests that exercise store
concurrency rather than being made slower again.

Rejected: a shared `file::memory:?cache=shared`. It changes locking semantics
across the whole suite, and `auto_vacuum` and WAL do not behave as on disk, so
the tests would pin a third configuration nobody ships.

## What has to be re-read, not assumed

Every site below reasons from "the store is one connection". Each needs a
decision in the same PR — several of these budgets may well stay, but not by
default, and none may keep its current stated justification.

**Reads that move to the read pool, so the rationale genuinely weakens:**

- `waker.go:122` (`wakeScanPageCap`, "the cost is round trips on the store's
  single connection"), `:126` (`wakeScanPagesPerPass`, "cannot monopolise the
  single connection"), `:588` (one edges query per page), `:589` and `:977`,
  `:1232` in `waker_test.go`. The waker opens no transaction, so all of this
  moves outright. Whether the budgets stay is now a question about read-pool
  saturation and reconcile fan-out, not about the write connection.
- `objectswatch.go:292` (the drain page budget), `:515` (the per-drain position
  check), `:596` (one batched read rather than N per-object reads). The batched
  read remains right on round trips alone; the *stated* reason is the connection.
- `sqlite/store.go:820`, `:860`, `:2502` — drain-rows-before-the-next-query, the
  three sites reachable from a bare read. Keep the discipline, narrow the reason
  to the transaction case. `:2249` and `:2880` are mutator-only and stand as
  written.

**Rationale that stands, and should be said so explicitly:**

- `waker.go:52` — `persistGate` floors the cursor *write*. Writes still
  serialise on the one write connection; unchanged.
- `client.go:512` — the marshal stays outside the transaction so user
  `MarshalJSON` does not hold the write lock. Unchanged.
- `sqlite/store.go:1292`, `:2124`, `:2257` — `BEGIN IMMEDIATE`, one connection,
  as the basis for read-modify-write. Unchanged, and it is the write pool.

**Tests and test rationale that encode the assumption:**

- `sqlite/store_test.go:5832` — `TestResourceVersionMonotonicInCommitOrder`, with
  the argument at `:5826` and the comment at `:5830` saying "raising
  `SetMaxOpenConns` should fail here". It guards the *write* pool, which this
  change does not touch, so it must keep passing; its wording narrows to say
  write pool.
- `sqlite/store_test.go:6999` and
  `docs/adr/2026-07-29-auto-vacuum-incremental.md:127` — both already say the
  free-page delta skews "on a pool wider than one connection". That pool now
  exists (see [Acceptance](#acceptance)).
- `sqlite/store_test.go:1013`, `reconciler_test.go:3125`,
  `objectswatch_test.go:1931`, `:2018`, `:3403`, `waker_test.go:977`, `:1232`,
  `waker_bench_test.go:113`, `:211`, `objectswatch_bench_test.go:39`, `:160`,
  `testutils_test.go:183`, `:191`.

**Prose:**

- ADRs, from `grep -rn 'single connection\|one connection\|SetMaxOpenConns\|maxConns' docs/adr/` — **18** files:
  `2026-07-26-edges-without-rowid.md`, `2026-07-27-name-keyed-writes.md` (cites
  `SetMaxOpenConns(1)` directly), `2026-07-28-periodic-scan-drivers.md`,
  `2026-07-29-auto-vacuum-incremental.md:127`,
  `2026-07-29-dependency-watermarks.md`, `2026-07-30-durable-waker-cursor.md`,
  `2026-07-30-store-write-shapes.md`,
  `2026-07-31-a-spec-write-enqueues-its-own-object.md`,
  `2026-08-02-object-write-log.md`, `2026-08-03-stale-dependents-cursor.md`,
  `2026-08-03-watch-shared-tail.md`, `2026-08-04-work-queue-re-enqueue-floor.md`,
  `2026-08-05-a-create-pushes-a-deleting-owners-collect.md`,
  `2026-08-05-events-get-a-cursor-and-a-commit-wake.md`,
  `2026-08-05-the-waker-abandons-an-overtaken-drain.md`,
  `2026-08-06-owner-scoped-watches.md`,
  `2026-08-06-the-waker-sees-a-retention-trim.md`.
- `docs/TODO.md:32`, `:85`, `:246`–`:305` (this item), `:307`–`:323` (the
  page-cache item).

An ADR whose *decision* rested on the single connection needs folding into the
new ADR rather than a line edit; the list above does not distinguish, and
triaging it is part of the work.

### Reads inside `Within` get no benefit

Say it plainly, because it bounds the win: a read on a *caller's* transaction ctx
runs on the write connection, by design. What changed since the first draft is
which reads those are — a read the store wraps *for itself* now runs on the read
pool via `readWithin`, and only a read nested in a caller's `Within` stays on the
write pool. The drivers are the population that benefits — the waker, the
tailers, the event readers, the owed and stale-dependents passes — and none of
them opens a transaction.

## Signature changes

`open` takes a `*sql.DB`, not a path (`sqlite/sqlite.go:52`), so "the read pool
is opened after migrations succeed" is not expressible today. `open` becomes
`open(write *sql.DB, read *sql.DB)` — with `Open` building both and `OpenMemory`
passing the same handle twice — or takes a path and builds the pools itself.
Prefer the former: it keeps `open` free of DSN knowledge, which is
`sqlitemigrate`'s job.

`Close` closes both, write pool last, and no-ops the second close when aliased.

## Step zero: does modernc actually do this?

**Run, at `b4e674c`, and passing.** The design rests on `modernc.org/sqlite`'s
pure-Go VFS running WAL readers concurrently with a writer through its `-shm`
implementation — a pure-Go translation, not a cgo binding. A throwaway
file-store program, two pools with the DSNs above:

1. **A `query_only` pool read completed while a writer held `BEGIN IMMEDIATE`**,
   and did not see the uncommitted row. Readers are genuinely concurrent; the
   design is not void.
2. **A `DEFERRED` read transaction on the read pool held a stable snapshot across
   two concurrent commits** and did not block the writer. This is the
   `readWithin` path, and it is the one the revision added.
3. **`query_only` refuses writes** — `attempt to write a readonly database (8)` —
   both in autocommit and from inside an open read transaction. That is the
   enforcement behind `readWithin`'s "fn must not write".
4. **A `query_only` pool opened a database no writer of its own had touched and
   read through a 16KB hot `-wal` left by an unclean close**, recovering all
   committed rows. This is the empirical case for `query_only` over `mode=ro`
   (test 5 below).

DSN pragma order turned out to be irrelevant: `query_only` before or after
`journal_mode(WAL)`/`auto_vacuum` both open cleanly.

(These are tests 2, 4, 5 and 6 below, promoted. Write them once, keep them.)

## Testing

The suite as it stands would pass an `s.read(ctx)` that forgets the transaction
case. Required, all on a file store:

1. **`read` returns the transaction.** Inside `Within`, assert `s.read(ctx)` is
   the same `dbtx` as `s.conn(ctx)`, and that a read-only method called with the
   transaction ctx observes a write made earlier in that transaction. This is
   invariant §1; it must fail if the transaction branch is removed.
2. **Snapshots are monotone across connections** — invariant §2, which nothing
   pins today. Drive the read-mark-then-page shape directly: read a mark on one
   pool connection, commit a write, then page on connections that did not serve
   the mark read, and require every page reflects at least the mark's snapshot.
   Force connection spread by holding reads open concurrently rather than
   trusting `database/sql`'s reuse order.
3. **`readWithin` gives one snapshot and does not block a writer.** Open it,
   read, commit a write from the write pool, read again on the same frame, and
   require both reads agree — while the writer's commit returns without waiting.
4. **`readWithin` joins an ambient transaction.** Called inside a caller's
   `Within`, it must observe that transaction's uncommitted writes and must not
   open a second transaction. Pair it with the negative: a write attempted inside
   `readWithin` **outside** any caller transaction fails
   `SQLITE_READONLY` rather than landing on the write pool, and `AfterCommit` on
   a read frame is refused.
5. **A closed `txState` degrades to the read pool**, mirroring `conn`'s existing
   behaviour for the write pool.
6. **The read pool is `query_only`**: a write attempted on it errors rather than
   silently landing.
7. **The read pool recovers WAL state** — this is the reason `query_only` was
   chosen over `mode=ro`, so it is the thing to test, and "migrations run once"
   does not reach it. Two cases: open the read pool on a path no writer has
   opened, and open it after an unclean close that left a non-empty `-wal`. Both
   must succeed and read committed data.
8. **A read concurrent with a live write completes without waiting for it.** The
   test that proves the change did anything, and the step-zero probe.
   Signal-synchronised, not timing: block a writer inside `Within` on a channel,
   issue a read, require the read returns before the writer is released.
9. **`Close` closes both pools and is idempotent**, including the aliased
   in-memory case.

`internal/sqlitemigrate`'s `TestOpenPool*` grows a case for the read-pool
options — `query_only` set, `_txlock` absent.

## Acceptance

`BenchmarkWritesUnderWatch` is the tripwire. Its `no-watcher` row is the
baseline the split should move the others toward; the 16-kind row is where the
win should be largest. Report before/after for all four rows on one machine.

Not done unless: `1 kind, 1 watch` lands within noise of `no-watcher`, and
`16 kinds` improves materially. If it does not, the finding is that the
contention was not the connection, and this spec is wrong rather than
incomplete — **provided `readWithin` shipped**. Measured without it, the
benchmark exercises a drain whose page read never left the write pool, and a
null result would say nothing about the hypothesis.

That measurement is one-sided — it measures writes under read load and never
what the pool costs. Three known costs, each of which must get a number or an
explicit "accepted":

- **Read latency and cache fragmentation — one number, not two.** A read pays
  connection acquisition, and on a cold pool connection a fresh page cache. The
  larger effect is steady-state rather than cold-start: `database/sql` spreads
  reads across `N` connections, and `cache_size` is per connection, so the
  drivers' working set — the same indexes rescanned forever — is cached up to
  `N` times over at `N ×` the memory, each copy colder than the single cache it
  replaced. This is the §Follow-on `cache_size` point arriving early, so pick
  the number once: add a read-latency row to the benchmark and state resident
  memory at the chosen `N`, together.
- **Checkpoint latency.** Stated as a mechanism, because an earlier draft
  asserted "a real regression, not a wash" at a strength the analysis does not
  support. WAL readers hold back a checkpoint only **while a read transaction is
  open**. Most reads here are a single autocommit statement, and
  `SetConnMaxIdleTime` (`sqlitemigrate.go:54`) means an idle-but-open pool
  connection holds no snapshot. So a *single* reader's exposure is bounded by the
  **longest open read transaction**, not by idle pool depth — and `readWithin` is
  what makes that longer than one statement, so it is the thing to watch.

  That makes the prediction falsifiable and the candidates specific: the full
  pass's listings, `DependentsListStaleSince`'s pages, and now the tailer's
  `ObjectWritesListSince` frame — not the scalar gates, which are the frequent
  ones. "Measure freelist drain with the read pool busy" now has a defined
  *busy*: drive the long listings and a deep tailer backlog, not a read flood.
  `sqlite/store_test.go:6999` and
  `docs/adr/2026-07-29-auto-vacuum-incremental.md:127` already anticipate the
  wider pool skewing the free-page delta.

- **`-wal` high-water mark**, which is a different quantity from the one above
  and the one this change is most likely to regress unnoticed.
  `grep -rn 'wal_autocheckpoint\|wal_checkpoint\|journal_size_limit'` over the
  repo returns **nothing**, so the only checkpointing is SQLite's default
  1000-page PASSIVE auto-checkpoint on the writer's `COMMIT`.

  A PASSIVE checkpoint copies pages only up to the **oldest active reader's**
  mark, and *resetting* the WAL — reusing the file from offset 0 instead of
  appending — needs a moment with **no reader at all**. So the governing quantity
  is not the longest open read transaction but the **longest gap with no reader
  open**, and with `N` connections and continuously-draining tailers that gap can
  approach zero while every individual read stays short. The `-wal` file then
  sticks at its high-water mark. One reader and N overlapping readers are not the
  same exposure, and the bullet above is only correct about the first.

  This is the same on-disk footprint concern
  [`docs/adr/2026-07-29-auto-vacuum-incremental.md`](../adr/2026-07-29-auto-vacuum-incremental.md)
  exists to manage, and incremental vacuum does not touch it — a freelist drain
  shrinks the main database, not the write-ahead log. So: **report peak `-wal`
  size before and after, on the same benchmark run as the other two numbers.**
  Without it the change can pass [Acceptance](#acceptance) on throughput and
  regress footprint with nothing to catch it.

  If it does regress, the levers are ordinary and none is in scope here: a
  `journal_size_limit` so a checkpoint that *does* reset truncates the file, an
  explicit `TRUNCATE` checkpoint on the sweeper's cadence, or a smaller `N`. Pick
  one against a number, not in advance.

## Follow-on: page cache and `mmap_size`

Deliberately **not** in scope, but this work changes its premise, so it is
recorded together.

`OpenPool` sets five pragmas and leaves SQLite's stock ~2MB cache and disabled
memory mapping alone. Two things make tuning them less obviously a win than the
standard advice suggests:

- We run on `modernc.org/sqlite`, a pure-Go translation rather than a cgo
  binding, so **whether `mmap_size` maps anything at all there is unverified**.
  That is the first thing to check; if it is a no-op the item halves. Step zero
  verified WAL reader concurrency there, not this.
- The "one connection, so a larger cache is not shared across concurrent
  readers" argument stops applying once the read pool exists — but it inverts
  rather than disappears: `cache_size` is per connection, so an N-connection
  read pool multiplies what a given `cache_size` costs. Any number picked after
  this change is picked as `N ×`, which is why it appears in
  [Acceptance](#acceptance) above rather than only here.

The drivers' repeated scans remain the one real argument for a larger cache: the
owed pass, the stale-dependents pass, the waker and the watch floor all re-read
the same indexes on a fixed cadence forever, which is exactly the working set a
page cache is for. Still deferred on the same grounds — a tuning change with no
measurement behind it, and adding pragmas we cannot show a number for is how a
config grows cargo. Revisit with a benchmark on a store large enough that the
scans miss cache.

## Open

Resolved across three review rounds: the shared-helper problem (did not exist),
`_txlock` on the read pool (dropped, via an options struct, and now load-bearing
rather than forward-looking), the re-read inventory (rebuilt from `grep` at
`b4e674c`), the in-memory blindness (file-backed constructor, priced, with its
own tick constants), the second invariant (cross-connection snapshot
monotonicity, now stated and tested), the modernc feasibility question
([step zero](#step-zero-does-modernc-actually-do-this), run and passing), and
**the reads that wrap themselves** — the finding that made the first draft's
"near-mechanical" cost estimate and its acceptance criteria both unreachable.

Still open:

- **Nothing here has a number attached except the problem.**
  [Acceptance](#acceptance) names four measurements and reports none. The
  checkpoint prediction now has a mechanism and a falsifiable shape, which is
  progress from an assertion, but it is still untested — and `readWithin` is the
  thing that makes it more than theoretical, since it is the only open read
  transaction on a hot path. The `-wal` high-water mark is the one of the four
  that can regress while the headline benchmark improves.
- **`N` is a guess about two quantities now**, not one. `min(4, GOMAXPROCS)` has
  the 16-kind benchmark row behind it as a target and nothing behind it as a
  value — and since `readWithin`, it bounds concurrent read *protocols* rather
  than read *statements*, so the same number is doing a second job nobody has
  measured either.

Not open: the design. Two pools, `query_only`, `s.read` and `s.readWithin`
sharing the ambient-transaction branch, and a call-site change that is mechanical
for the bare reads and four wrap sites for the rest.

## When to build this

Not yet. Revisit when a deployment is write-bound with watches attached, or when
the driver count grows enough that read contention shows up with no watch at
all. The trade this defers on is the safety property, not the size of the win:
a loud, deterministic deadlock becomes a silent stale read, guarded by one
function — two, now.
