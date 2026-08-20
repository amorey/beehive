# A read-only transaction

- **Status:** Planned.
- **Date:** 2026-08-20
- **Depends on:** the read pool, which shipped —
  [ADR](../adr/2026-08-20-reads-get-their-own-connections.md). A grouped read belongs on that pool,
  and it needs a transaction there to see one instant across its statements.

## Why

Five places group two or more reads so they describe one instant. They do it
with `Within`, and the DSN sets `_txlock=immediate`, so **a read takes the write
lock**:

- `Events().Snapshot` (`sqlite/store.go:2010`) — the runs and the position.
- `Events().ListSince` (`:2042`) — the page, the horizon and the existence probe.
- `ObjectWrites().ListSince` (`:3077`) — the page, the horizon and the row images.
- `snapshot` (`:3275`) — a listing and the write log's mark.
- `Events().Sweep`'s candidate scan (`:2129`).

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

It is also the whole of the watch win. The reader is flat in writer count where
these are not: 403µs against 31µs at four writers, and moving them is what takes
`BenchmarkWritesUnderWatch` down rather than sideways.

`modernc.org/sqlite` gives us the deferred begin for free: `newTx` applies the
DSN's `_txlock` only when `opts.ReadOnly` is false (`tx.go:22-25`). So
`BeginTx(ctx, &sql.TxOptions{ReadOnly: true})` is a plain `BEGIN` with no DSN
change.

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

Implementation is `Within` with two differences: `BeginTx` gets
`&sql.TxOptions{ReadOnly: true}`, and `txCount` is not incremented — it counts
write transactions, and a test asserts a fast path took none.

**The hook queue is not one of the differences.** A read transaction still opens
a frame, so `AfterCommit` queues onto it, and a read transaction still commits —
releasing its snapshot. Flush on that commit exactly as `Within` does. Skipping
the flush because "a read has nothing to defer to" drops the hook silently, which
is the one way this can go wrong without failing.

The five call sites above switch to it. `Store` is unchanged, so `fakeStore`
needs nothing.

## Exported, or not

[Cache prepared statements](2026-08-20-cache-prepared-statements.md) turns on this
choice, so make it here.

A cached statement is one compiled handle, and two callers sharing one corrupts
both silently. Inside a transaction the connection is already held, so sharing is
possible — unless nothing can reach that transaction concurrently.

- **Unexported**, used only by the five call sites above: all are the store's own
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

- **`Events().Sweep` writes.** Its candidate scan is a read but the trim is not.
  Only the scan moves; the sweep keeps its write transaction.

- **The savepoint machinery is shared.** `txState` does not care which begin verb
  opened the transaction, so nested frames, `sealForCommit` and the unwind rules
  are untouched. Do not fork them.

- **A read transaction still holds a connection**, one of the reader's N. It
  also pins the WAL: it cannot be checkpointed past this snapshot while the
  transaction is open, so a grouped read that pages is what grows the file.

- **A read transaction pins the WAL.** It cannot be checkpointed past this
  snapshot while the transaction is open, so a grouped read that pages is what
  grows the file. `ObjectWrites().ListSince` pages under a budget already; check
  that the budget bounds the transaction and not just the rows.

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
  read", and a dropped hook fails nothing.

## On ship

ADR: **a read that groups is a read transaction**. It records the one rule that
matters for later readers — `Within` means "I intend to write", and choosing it
for a read is now a mistake rather than a style. Note the modernc detail, because
the whole thing rests on it and it is invisible in our code.

The read-pool ADR and this one describe one boundary from two sides; cross-link
them rather than restating either.
