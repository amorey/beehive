# Reads get their own connections

- **Status:** Accepted — `sqlitemigrate.OpenReadPool`, `sqliteStore.read` and
  `sqliteStore.readDB`, measured by `BenchmarkReadUnderWrites`.
- **Date:** 2026-08-20

## Context

`OpenPool` sets `journal_mode(WAL)`, which lets one writer and many readers run
at once. `sqlite.Open` then passed `maxConns = 1`, so every read queued behind
every write in Go's pool and that concurrency went unused. The limit was
`database/sql`, not SQLite.

The drivers are what paid for it. The owed pass, the stale-dependents pass, the
waker and the watch floor tick all read on a fixed cadence forever, and each of
those reads sat in the writers' queue.

## Decision

**Two pools over one file.** The writer is what it was — one connection,
`_txlock=immediate`. The reader is `sqlitemigrate.OpenReadPool`: N connections,
`query_only(true)`, and **no** `_txlock=immediate`, because `BEGIN IMMEDIATE`
takes a write lock `query_only` refuses and would fail every transaction on it.

`query_only` rather than `mode=ro` because it is enforced and says so: a write
fails with "attempt to write a readonly database" instead of blocking.

**N is `WithReadConnections`, defaulting to 4.** One connection already keeps
reads out of the writers' queue; more helps only readers that genuinely overlap.
That is measurable — eight overlapping readers with one writer run at 43.3µs
per read at N=1, 26.7µs at N=2, 16.4µs at N=4, 14.6µs at N=8. The option is
`sqlite`'s own, not beehive's: the embedder builds the store and hands it to
`New`, so beehive's option machinery never sees one. `ErrInvalidOption` is local
for the same reason — the store must not import the control plane.

**`s.read(ctx)` returns the ambient transaction while it is live, else the read
pool.** That is the whole safety property. It also degrades to the pool on a
*closed* frame, exactly as `conn` does, because a ctx outlives its transaction —
without that, every read on a hook's ctx returns `sql.ErrTxDone`.

**`read` returns a read-only interface**, so a write does not compile onto the
reader. A runtime check could not stand in: inside a transaction `read` hands
back that transaction, where a write would commit on the writer and look
correct. The guard is not airtight — a `DELETE … RETURNING` is issued through
`QueryContext` — and `deleteWriteLogRows` is held on `conn` by a comment.

**`OpenMemory` aliases the reader to the writer.** `file::memory:` is
per-connection, so a second pool would be a different and empty database. The
suite therefore runs unsplit, and the split is covered by on-disk tests alone.

**Why the split is safe at all**: a WAL snapshot never shows half a
transaction. Every record written beside its horizon — `object_writes` with
`object_writes_horizon`, `events` with `events_horizon` — stays consistent
across the two connections, because both halves commit together. The two
connections do not see the same instant, and that is harmless for exactly this
reason.

## Consequences

**A read on the wrong ctx now reads stale instead of deadlocking.** The rule is
unchanged — pass the ctx you were given to every store call inside a transaction
— but the failure it prevents is quieter. It used to wait for the connection the
transaction held, which hangs a test. It now returns committed state, missing
the transaction's own writes, and nothing reports it. `ControllerClient.Within`,
`Store.Within` and `Client.Watch` say so.

**Reads stopped scaling with write pressure.** `BenchmarkReadUnderWrites`, on
disk: 20.8µs idle either way; 113–136µs → 36–38µs under one writer; 376–382µs →
35–39µs under four.

**Watches are unmoved, and the reason matters.** `BenchmarkWritesUnderWatch` at
its shipping throttle is the same before and after on all five cases. With the
tailer's scan floor set to 0 — `withWatchScanMinInterval`, unexported, so no
embedder can select it — the light-watch rows get 11–15% worse: the tailer's
position read moved to the reader, so with nothing pacing it the loop cycles
faster and issues more page reads, and those still take the writer.

That is because five pure-read store APIs self-wrap in `Within`:
`ObjectWrites().ListSince`, `snapshot`, `Events().Snapshot`,
`Events().ListSince`, and `Events().Sweep`'s candidate scan. Their cost grows
with writer count where a read-pool read is flat — 403µs against 31µs at four
writers. Moving them onto a deferred read transaction is what wins the watch
path, and it is not this change.

**The budgets that cited "the single connection" have new reasons, not new
conclusions.** A chunk loop still closes its rows before the next chunk, because
a held result set pins one of N reader connections rather than the only one. The
two loops that run inside a write transaction really do have one connection and
now say so.

**Two exposures arrive with grouped reads, not with this.** A held read
transaction pins the WAL against checkpointing, and a commit wake arriving
inside an open snapshot is read against state from before it. Both are recorded
in [`TODO.md`](../TODO.md); neither is reachable while every read is autocommit.

### Rejected alternatives

**A larger single pool.** `_txlock=immediate` means concurrent `Within` calls
collide at the SQLite level and take `SQLITE_BUSY` behind a 5s `busy_timeout`,
where today they queue in Go.

**`mode=ro` for the reader.** `TODO.md` justified `query_only` on the grounds
that `mode=ro` cannot recover the `-wal`/`-shm` files. That does not reproduce —
a database left with a hot 836KB WAL opens and reads under `mode=ro`, with the
`-shm` present, without it, and in a read-only directory. `query_only` is still
right, for the enforcement above.

**A boolean on `OpenPool`.** The two DSNs differ in three pragmas and a txlock,
and a boolean at the call site says less than `OpenReadPool` does.
