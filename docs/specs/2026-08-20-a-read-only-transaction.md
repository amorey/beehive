# A read-only transaction

- **Status:** Proposed, not decided. Measured twice — it is worth 2-8% on
  writes that have a watch attached, and **nothing at all** on how fast a watch
  delivers. See *What it is worth*, then decide whether that earns a place ahead
  of the rest of the list. The prototype is not kept; git holds it.
- **Date:** 2026-08-20
- **Depends on:** the read pool, which shipped
  ([ADR](../adr/2026-08-20-reads-get-their-own-connections.md)). A grouped read
  belongs on that pool, and needs a transaction there to see one instant across
  its statements.

## Why

Five places group two or more reads so they describe one instant. They do it
with `Within`, and the DSN sets `_txlock=immediate`, so **a read takes the write
lock**:

- `Events().Snapshot` (`sqlite/store.go:2059`) — the runs and the position.
- `Events().ListSince` (`:2083`) — the page, the horizon and the existence probe.
- `ObjectWrites().ListSince` (`:3120`) — the page, the horizon and the row images.
- `snapshot` (`:3319`) — a listing and the write log's mark.
- `Events().Sweep`'s candidate scan (`:2172`, via `overCapTimelines` at `:2221`).

The third is every watch tailer's page read, on every drain.

Measured on an arm64 sandbox, on disk:

| | |
|---|---|
| empty `BEGIN IMMEDIATE` + `COMMIT` | 10.2 µs |
| empty `BEGIN` (deferred) + `COMMIT` | 7.4 µs |

So about 2.8 µs per grouped read today, and a write lock held for no reason.
The lock costs nothing while the pool is one connection, because everything
already queues there.

With the read pool it stops being merely wasteful. These five reads belong on the
reader, and the reader is opened `query_only(true)`, which refuses a write lock —
so `IMMEDIATE` there does not work at all. This is what lets those call sites move.

`modernc.org/sqlite` gives us the deferred begin for free: `newTx` applies the
DSN's `_txlock` only when `opts.ReadOnly` is false (`tx.go:22-25`). So
`BeginTx(ctx, &sql.TxOptions{ReadOnly: true})` is a plain `BEGIN` with no DSN
change.

## What it is worth, measured

A prototype — `withinRead` over the four read-only sites, on disk, medians of
three runs of 300. `BenchmarkWritesUnderWatch`, ns/op:

| | pre-split | + read pool | + this | net |
|---|---|---|---|---|
| no-watcher | 177 | 176 | 178 | +0% |
| one-watcher | 180 | 180 | 169 | **−6%** |
| 64-watchers-one-kind | 180 | 178 | 170 | **−5%** |
| 16-kinds-one-watcher-each | 189 | 178 | 174 | **−8%** |
| one-owner-scoped-watcher | 180 | 177 | 177 | −2% |

That is the shipping throttle. **The win is real and modest** — do not go in
expecting the read pool's 10× on bare reads.

The same benchmark with the tailer's scan floor at 0 — `withWatchScanMinInterval`,
unexported, so unreachable — is where the effect is large, because the floor is
what otherwise caps the drain rate:

| | pre-split | + read pool | + this |
|---|---|---|---|
| one-watcher | 223 | 246 | 226 |
| 64-watchers-one-kind | 230 | 261 | 239 |
| 16-kinds-one-watcher-each | 334 | 305 | **228 (−32%)** |
| one-owner-scoped-watcher | 220 | 251 | 230 |

Two things to read off it. This change **reverses the regression the read pool
introduced there** — the tailer's page read stops taking the writer, which is
what that regression was. And the light-watch rows still land 1–5% above
pre-split, so it does not fully pay that back; only the 16-kind row, where
sixteen tailers were contending, comes out clearly ahead.

### It does not make watches faster

Both tables above are the *writer's* side: a tailer stops making writers wait.
Delivery latency — commit to the watcher seeing it — is set by the tailer's scan
floor and nothing else. Measured separately, 50 iterations, on disk:

| throttle | kinds | before | + this |
|---|---|---|---|
| 100ms (default) | 1 | 98.9ms | 99.8ms |
| 100ms (default) | 16 | 99.4ms | 99.8ms |
| 0 | 1 | 0.33ms | 0.34ms |
| 0 | 16 | 5.35ms | 3.90ms |

At the default, sixteen concurrent tailers deliver as fast as one:
`defaultWatchScanMinInterval` paces them long before the connection does, so
there is no contention left here to remove.

With the floor off there is — sixteen kinds cost sixteen times one, which is
near-perfect serialisation on the writer. This change recovers about a quarter of
that, and no more, because the read pool defaults to four connections and sixteen
tailers still queue four at a time. Anyone who wants faster delivery should be
looking at the floor, not at this.

## The change

A sibling of `Within`, unexported on `sqliteStore` (see *Exported, or not*):

```go
// withinRead runs fn inside a read transaction on the read pool: every read sees
// one snapshot, and no write lock is taken. A write issued inside fn is a
// programming error — SQLite refuses it, and the error is not wrapped into
// anything a caller can handle.
//
// A nested withinRead joins the ambient transaction, read or write. Nested inside
// a write transaction it is therefore a write transaction, which is correct: the
// outer one already holds the lock, and its uncommitted writes must be visible.
func (s *sqliteStore) withinRead(ctx context.Context, fn func(ctx context.Context) error) error
```

Implementation is `Within` with three differences: it begins on `s.readDB`,
`BeginTx` gets `&sql.TxOptions{ReadOnly: true}` (which is what makes it a
deferred `BEGIN` — `modernc` applies the DSN's `_txlock` only when `ReadOnly` is
false), and `txCount` is not incremented, since it counts write transactions and
a test asserts a fast path took none.

**A nested call joins by running `fn` on the same ctx, not by taking a
savepoint.** A savepoint is a rollback boundary, and a read has nothing to roll
back. The measured prototype is:

```go
func (s *sqliteStore) withinRead(ctx context.Context, fn func(ctx context.Context) error) error {
	if st := liveTx(ctx); st != nil {
		return fn(ctx) // already grouped, read or write
	}
	tx, err := s.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	...
}
```

Note what the prototype skipped and the real one must not: it dropped the
`AfterCommit` queue. See the hook rule below.

**The hook queue is not one of the differences.** A read transaction still opens
a frame, so `AfterCommit` queues onto it, and a read transaction still commits —
releasing its snapshot. Flush on that commit exactly as `Within` does. Skipping
the flush because "a read has nothing to defer to" drops the hook silently, which
is the one way this can go wrong without failing.

Four of the five call sites switch to it. `Store` is unchanged, so `fakeStore`
needs nothing.

`Events().Sweep` is the fifth and does **not** switch: its candidate scan is a
read but the trim that follows is a write, in the same transaction. Moving the
scan alone would split them across two connections and two snapshots, which is
the one thing the grouping exists to prevent.

## Is it worth building

Against the rest of the list, this is the weakest measured case in its group:
2-8% on watched writes, and nothing on delivery. What it also buys, and what
may matter more:

- It settles the export question that
  [cache prepared statements](2026-08-20-cache-prepared-statements.md) turns on.
- It is the right shape regardless. Four pure reads taking `BEGIN IMMEDIATE` on
  the writer is wrong on its face, and stays wrong however little it costs
  today — the cost grows with write pressure, where a read-pool read is flat.

Neither is urgent. If the list is ranked by measured value this sits below
[resource version blocks](2026-08-20-reserve-resource-versions-in-blocks.md) and
[the spec write's read](2026-08-20-a-spec-write-writes-before-it-reads.md), and
that is how the README now orders it.

## Exported, or not

[Cache prepared statements](2026-08-20-cache-prepared-statements.md) turns on this
choice, so make it here.

A cached statement is one compiled handle, and two callers sharing one corrupts
both silently. Inside a transaction the connection is already held, so sharing is
possible — unless nothing can reach that transaction concurrently.

- **Unexported**, used only by the four call sites above: all are the store's own
  and single-threaded, so statements inside a read transaction can be cached.
  `ObjectWrites().ListSince` is every watch tailer's page read, so this is the hot
  path.
- **Exported on `Store`**: a caller can fan out inside it, and statements inside a
  read transaction must not be cached.

**Recommendation: keep it unexported** until something outside the store needs
grouped reads. Nothing does today, and exporting costs the tailer's page read for
a capability with no caller. If it is exported later, the caching rule changes
with it — record that dependency in both ADRs rather than in one.

## Edge cases the implementer would otherwise guess at

- **Nesting a write inside a read is not defended against.** A `Within` reached
  from inside a `withinRead` joins the read transaction and its writes fail at
  the SQLite level. That is loud and correct. Say so in the godoc; do not add a
  runtime check for it.

- **The savepoint machinery is shared.** `txState` does not care which begin verb
  opened the transaction, so nested frames, `sealForCommit` and the unwind rules
  are untouched. Do not fork them.

- **A read transaction holds one of the reader's N connections** for its whole
  life, so a drain that pages holds one for the whole drain.

- **A commit wake can arrive inside an open snapshot.** An autocommit read
  starts a fresh snapshot every statement, so a wake is always seen. Grouped in a
  transaction, a wake arriving mid-read is answered from before that commit. For
  the object tailers the floor tick bounds it. For the dependency waker, which by
  design has no tick, it falls through to the 60s stale-dependents pass — so
  **the waker's reads must not be grouped**, and `ObjectWrites().ListSinceAll`
  (`:1253`) is deliberately not on the list above. Say so where it is not, or
  someone will add it for symmetry.

- **A read transaction pins the WAL**, which is this change's real operational
  cost and is measured in [`TODO.md`](../TODO.md): with autocommit reads only,
  2000 inserts peak at 4.1MB and a checkpoint truncates to nothing; with one open
  read transaction, 25.8MB and the checkpoint comes back busy. `ListSince` pages
  under a budget already — check the budget bounds the transaction and not just
  the rows, and decide there whether `journal_size_limit` earns its place now
  that something finally holds a snapshot.

## Tests

In `sqlite/store_test.go`:

- A `withinRead` that groups two reads sees one snapshot: write between them from
  the same store and the second read must not see it. Drive the write from
  another goroutine through a seam, not by timing.
- A write inside `withinRead` fails, and the failure is the driver's.
- `withinRead` does not increment `txCount`, and `TestSpecWriteTakesItsTransaction`
  (or whichever test reads that counter) still passes.
- A `withinRead` nested in a `Within` commits with the outer one, and its reads
  see the outer transaction's uncommitted writes.
- `AfterCommit` inside a `withinRead` runs when it commits, not never. Pin it:
  the hook queue is the part most likely to be dropped as "not needed for a
  read", and a dropped hook fails nothing. The measured prototype dropped it.
- On disk, a grouped read runs on the reader while a write transaction is open —
  the property the whole change is for, and one `OpenMemory` cannot show, since
  the reader is the writer there.
- `Events().Sweep` still trims what its scan found, in one transaction on the
  writer.
- The waker's `ListSinceAll` is still ungrouped, so a commit wake mid-scan is
  seen. Without this the next reader groups it for symmetry and the waker's only
  backstop becomes the 60s pass.

## On ship

ADR: **a read that groups is a read transaction**. It records the one rule that
matters for later readers — `Within` means "I intend to write", and choosing it
for a read is now a mistake rather than a style. Note the modernc detail, because
the whole thing rests on it and it is invisible in our code.

The read-pool ADR and this one describe one boundary from two sides; cross-link
them rather than restating either.
