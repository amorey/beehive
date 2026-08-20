# A read-only transaction

- **Status:** Planned.
- **Date:** 2026-08-20
- **Depends on:** nothing. Wanted before the read-pool split in
  [`TODO.md`](../TODO.md).

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
already queues there. It starts costing the moment there is a read pool, which
is the change `TODO.md` parks. Doing this first means that change does not have
to also fix these five call sites.

`modernc.org/sqlite` gives us the deferred begin for free: `newTx` applies the
DSN's `_txlock` only when `opts.ReadOnly` is false (`tx.go:22-25`). So
`BeginTx(ctx, &sql.TxOptions{ReadOnly: true})` is a plain `BEGIN` with no DSN
change.

## The change

A sibling of `Within` on `Store`:

```go
// WithinRead runs fn inside a read transaction: every read sees one snapshot of
// the database, and no write lock is taken. A write issued inside fn is a
// programming error — SQLite refuses it, and the error is not wrapped into
// anything a caller can handle.
//
// A nested WithinRead joins the ambient transaction, read or write. A WithinRead
// nested inside a write transaction is therefore a write transaction, which is
// correct: the outer one already holds the lock.
WithinRead(ctx context.Context, fn func(ctx context.Context) error) error
```

Implementation is `Within` with three differences: `BeginTx` gets
`&sql.TxOptions{ReadOnly: true}`, there are no `AfterCommit` hooks to flush, and
`txCount` is not incremented (it counts write transactions, and one test asserts
a fast path took none).

The five call sites above switch to it.

## Edge cases the implementer would otherwise guess at

- **Nesting a write inside a read is not defended against.** A `Within` reached
  from inside a `WithinRead` joins the read transaction and its writes fail at
  the SQLite level. That is loud and correct. Say so in the godoc; do not add a
  runtime check for it.

- **`Events().Sweep` writes.** Its candidate scan is a read but the trim is not.
  Only the scan moves; the sweep keeps its write transaction.

- **The savepoint machinery is shared.** `txState` does not care which begin verb
  opened the transaction, so nested frames, `sealForCommit` and the unwind rules
  are untouched. Do not fork them.

- **A read transaction still holds the connection.** The rule that a read issued
  outside the transaction while inside one deadlocks is unchanged, and is still
  stated in `Client.Watch`'s godoc.

- **`fakeStore` in `testutils_test.go`** gains `WithinRead`, running fn inline as
  its `Within` does.

## Tests

In `sqlite/store_test.go`:

- A `WithinRead` that groups two reads sees one snapshot: write between them from
  the same store and the second read must not see it. Drive the write from
  another goroutine through a seam, not by timing.
- A write inside `WithinRead` fails, and the failure is the driver's.
- `WithinRead` does not increment `txCount`, and `TestSpecWriteTakesItsTransaction`
  (or whichever test reads that counter) still passes.
- A `WithinRead` nested in a `Within` commits with the outer one, and its reads
  see the outer transaction's uncommitted writes.
- `AfterCommit` inside a `WithinRead` runs inline, since there is no commit to
  defer to. Pin it: the hook queue is the part most likely to be copied over by
  mistake.

## On ship

ADR: **a read that groups is a read transaction**. It records the one rule that
matters for later readers — `Within` means "I intend to write", and choosing it
for a read is now a mistake rather than a style. Note the modernc detail, because
the whole thing rests on it and it is invisible in our code.

Add a line to the read-pool item in [`TODO.md`](../TODO.md): the five call sites
are already correct, so that change is the pool plus `s.read(ctx)`, nothing more.
