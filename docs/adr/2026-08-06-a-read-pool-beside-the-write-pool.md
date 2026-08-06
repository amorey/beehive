# A read pool beside the write pool

- **Status:** Accepted — implemented in `sqlite/sqlite.go`, `sqlite/store.go`,
  `internal/sqlitemigrate/sqlitemigrate.go`.
- **Date:** 2026-08-06
- Supersedes `docs/specs/2026-08-05-sqlite-read-pool.md`, deleted with this
  change.

## Context

`OpenPool` sets `journal_mode(WAL)`, which lets one writer and many readers run
at the same time. `sqlite.Open` passed `maxConns = 1`, so every read queued
behind every write in Go's pool and that concurrency went unused. The limit was
`database/sql`, not SQLite.

`BenchmarkWritesUnderWatch`, on disk with no driver running, measured the cost:
one watched kind cost about a quarter of write throughput, and sixteen watched
kinds cost most of it. Watch *count* was free — the shared tailer working as
designed — so the axis was watched kinds, each tailer's drain contending for the
one connection with the writers waking it.

## Decision

**Two pools, not a wider one.** Raising `maxConns` on the single pool would
break writes: with `_txlock=immediate` every `Within` takes `BEGIN IMMEDIATE`,
so concurrent transactions would collide at the SQLite level and take
`SQLITE_BUSY` behind a 5s `busy_timeout`, where today they queue in Go.

- **write pool** — unchanged: one connection, `_txlock=immediate`. Every
  invariant that rests on one writer rests on this pool, including
  `resource_version` being monotonic in commit order.
- **read pool** — `min(4, GOMAXPROCS)` connections, `query_only(true)`, and no
  `_txlock`.

`query_only` rather than `mode=ro`: a read-only connection cannot recover the
`-wal`/`-shm` files, so `mode=ro` fails on a database no writer has opened yet
and after an unclean close. `_txlock` comes off so `BeginTx` on the read pool
opens a DEFERRED read transaction instead of grabbing a write lock it cannot
have — which `readWithin` does on the hottest read path in the store, so this is
load-bearing rather than forward-looking.

**`s.read(ctx)` is `s.conn(ctx)` with a different fallback.** It returns the
ambient transaction whenever ctx carries a live one, and the read pool
otherwise. The transaction branch is not an optimisation: a read that skipped it
would miss the transaction's own uncommitted writes.

**`s.readWithin(ctx, fn)` gives a multi-statement read one snapshot.** Four
read-only methods wrap themselves — `EventsSnapshot`, `EventsListSince`,
`ObjectWritesListSince`, and the `snapshot` behind `ObjectWritesSnapshot*` —
because each pairs two reads that must agree: runs with log position, a page of
entries with the retention horizon. They wrapped in `Within`, which put them on
the write pool holding `BEGIN IMMEDIATE`, and `s.read` alone would have left
them there — while they are the page reads in the watch drains, the expensive
statement in every protocol the change exists to speed up.

Three properties of `readWithin`, each deliberate:

- **It joins an ambient transaction** by delegating to `Within`, so nested
  `Within`'s SAVEPOINT semantics and goroutine-ownership refusal come along for
  free and a read inside a caller's transaction is unchanged.
- **Its frame is resolved by `conn` as well as `read`.** A write inside it
  therefore reaches a `query_only` connection and fails loudly. A private ctx
  key that only `read` consulted would route a stray write to the write pool,
  where it would succeed *outside* the snapshot — silent, which is the failure
  mode this whole design is arranged to avoid.
- **A read frame owes no hooks.** `AfterCommit` returns nothing, so there is no
  error to hand back; on a read frame it panics rather than silently dropping
  the hook or silently running it after a read commits.

**The read pool is warmed at `Open`.** A connection attaching to a WAL database
for the first time blocks while a writer holds a transaction. Already-attached
connections read concurrently, so an unwarmed pool pays exactly the wait it
exists to avoid — on its first use, and again on any connection that gets
retired. `WarmPool` opens all N up front, and the reader pool never retires an
idle connection.

**In memory the read pool is the write pool.** `file::memory:` is
per-connection, so a second pool there would be a different, empty database.

## Consequences

**A caller-facing guarantee got weaker, and it is stated as such.** A read
issued on a non-transaction ctx from inside a transaction used to deadlock —
loud and deterministic. It now returns the last committed snapshot, missing the
transaction's own writes. The rule was already stated in `README.md`,
`Store.Within`, `ControllerClient.Within` and `Client.Watch`; those four now
describe a stale read rather than a hang. `s.read` returning the transaction is
the entire defence, and there is nothing behind it.

**Cross-connection snapshot monotonicity is now load-bearing.** Read-a-mark then
page-up-to-it is the shape of `staleDependents.sweep`, the waker's scan and the
object tailer's drain, none of which opens a transaction. On one connection the
ordering was free. It now rests on WAL taking the current wal-index end-mark per
read, so snapshots are monotone in real time whichever connection serves them.
`TestSnapshotsAreMonotoneAcrossConnections` pins it, because nothing else did.
The tailer's `ObjectsListByIDs` after a page depends on this only in the
permissive direction — a *newer* object than the entry named is fine, delivery
being latest-per-object.

**Every budget that cited "the single connection" now cites the read pool.** The
waker's page caps, the tailer's pages-per-drain, `withWatchScanMinInterval`, the
event drain's cap. The numbers stand; what they bound changed.

**The suite runs twice.** In memory the split does not exist, so none of it —
including `readWithin`'s refusal of a write — was covered by the default run.
`BEEHIVE_TEST_STORE=file` swaps `OpenMemory` for `Open`, and CI runs both. The
on-disk run gets a 10× tick, since `synchronous(NORMAL)` fsyncs land in the same
loop that the 2ms in-memory cadence assumes.

**Not measured yet.** `BenchmarkWritesUnderWatch` before/after, read latency and
resident memory at N connections, and the `-wal` high-water mark. The last is
the one that can regress while the headline number improves: nothing sets
`wal_autocheckpoint` or `journal_size_limit`, so the only checkpointing is the
default PASSIVE one on the writer's `COMMIT`, and a PASSIVE checkpoint copies
only up to the oldest active reader's mark while a WAL *reset* needs a moment
with no reader at all. With N connections and continuously-draining tailers that
gap can approach zero. `docs/TODO.md` carries all three.
