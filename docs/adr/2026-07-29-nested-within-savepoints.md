# A nested `Within` is a rollback boundary, implemented with `SAVEPOINT`

- **Status:** Accepted — implemented in `sqlite/store.go`, contract in
  `internal/storeapi/storeapi.go`.
- **Date:** 2026-07-29

## Context

`sqliteStore.Within` opens a transaction only when the ctx does not already carry
one. The nested branch used to return `fn(ctx)` and open nothing, so an error
returned from inside a nested `Within` unwound nothing. Only the outermost caller,
handing that error back to the real `Within`, rolled anything back — and a caller
that logged it and carried on committed every write that had already landed.

Any pair of writes was therefore atomic only by the grace of its callers.

`EdgesAdd` shows both the local escape and its limit. Issuing the `reconcile_owed`
stamp as a second store call after the edge insert would let a caller that swallowed
the stamp's error commit an edge with no stamp — the stranded dependent that
[the stamp-every-new-edge ADR](2026-07-29-stamp-every-new-dependency-edge.md) exists
to prevent. The answer there was *ordering*: fold the stamp in ahead of the insert,
so no write can fail after the edge exists. That works only because there are two
writes and one of them can be moved. Reorder anything whose second write depends on
the first and there is nowhere to put it.

## Decision

The nested branch runs `fn` inside a `SAVEPOINT`: `ROLLBACK TO` then `RELEASE` on
error, `RELEASE` on success. The outermost transaction is still the only thing that
commits, still under `BEGIN IMMEDIATE`. A savepoint adds no fsync, so the durability
floor is unchanged.

Five things make it correct rather than merely plausible:

- **The hook watermark.** `txState.hooks` is append-only and drains at the outermost
  commit, so an unwind truncates it back to the length taken at the `SAVEPOINT`.
  Otherwise a `WithOnCreate` registered inside a rolled-back frame still fires, for a
  row that is gone. `flushed` needs no consideration during an unwind: `takeHooks`
  sets it only after the outermost commit, so while a nested frame is in flight the
  list is provably open.

- **Depth travels with the transaction state, in one context value** (`txFrame`).
  A key of its own would be sticky — it survives everything that does not explicitly
  clear it, and installing a transaction only installs `txKey` — so a ctx carrying a
  stale depth into a fresh transaction would make the first ordinary nested call look
  like a concurrent one. `AfterCommit`'s hook ctx is exactly such a ctx: it strips
  `txKey` and nothing else. This is the same argument `txState` already makes for
  keeping `tx` and `hooks` together.

- **A sibling goroutine is refused, deep nesting is not.** Savepoints are a stack, so
  two goroutines nesting concurrently can interleave such that one unwind discards
  work the other already released. A frame whose ctx depth equals the live stack
  height is the rightful next one; a mismatch returns `ErrConcurrentNestedTx`.
  Serialising instead would deadlock as soon as `fn` waited on another goroutine that
  also wanted the store. Ordinary depth is not a concurrency signal — a
  `ControllerClient.Within` around a `Client.Create` around `ObjectsCreate`'s
  self-wrap is three frames in production.

- **A failed unwind poisons.** If `ROLLBACK TO` or `RELEASE` fails, the state is
  unknown: the outermost `Within` refuses to commit and falls to its deferred
  rollback, and the nested branch refuses further frames so a caller that swallowed
  the error cannot pile writes on. A failed `SAVEPOINT` does *not* poison — nothing
  was pushed, so the state is still known. This is not theoretical: SQLite rolls the
  whole transaction back on `SQLITE_FULL`, `SQLITE_IOERR` and `SQLITE_NOMEM`, and a
  `ROLLBACK TO` after one of those fails because the savepoint is gone. `SQLITE_BUSY`
  and constraint violations abort a statement, not the transaction, so savepoints
  behave normally through them.

- **A closed transaction degrades as a whole.** `AfterCommit`'s contract lets a hook
  pass back the tx ctx it captured, so a ctx outlives its transaction. `closed`
  latches on both outcomes, and `Within`, `conn` and `addHook` all consult it:
  `Within` opens a fresh transaction, `conn` falls back to the pool, and `addHook`
  distinguishes committed-and-drained (run the hook now) from rolled-back (discard
  it — running it would fire a `WithOnCreate` for a row that never landed). `closed`
  is set immediately after `Commit`, *before* the hook drain, because the drain runs
  inside `Within`'s body and a deferred set would read false for exactly the window
  it exists to cover.

Nothing recovers, and nothing balances the stack on a panic: a panic skips the
`RELEASE`, and the outermost deferred `tx.Rollback` discards the whole transaction.
A recover here would turn a panic into a half-committed transaction.

## Consequences

**`EdgesAdd`'s reverse residual is gone.** `EdgesAdd` self-wraps, so a failed insert
now unwinds the stamp issued ahead of it; a caller that swallows the error commits
neither. `TestRefsAddEdgeFailureLeavesStamp` pinned that residual as a tolerated cost
and is renamed to `…UnwindsTheStamp` to pin its absence. The ordering argument is
unaffected and still governs `EdgesAdd`: stamp-then-insert is chosen because the
opposite leaves an edge with no wake, which nothing re-derives. Savepoints make the
failure atomic; they do not make the ordering arbitrary.

**Cost, measured.** `BenchmarkWithinNestedMutators` (one outer `Within` enclosing
eight self-wrapping creates — the shape `gcCollect` and the owed pass run on):

| | ns/op | B/op | allocs/op |
|---|---|---|---|
| before | 450,800 | 42,357 | 1,146 |
| after | 481,300 | 44,768 | 1,259 |

**+6.8% wall clock, +113 allocations** — about 14 per nested frame across the two
extra statements. `modernc.org/sqlite` is a pure-Go translation with no statement
cache by default, so each `SAVEPOINT`/`RELEASE` pays a fresh prepare-and-step; that,
not the savepoint itself, is the cost. Statements are built with `strconv.AppendInt`
into a stack array rather than `fmt`, which keeps it to one allocation each. The
figure is the isolated worst case: eight nested frames doing trivial inserts, where
the savepoints are the largest share of the work they will ever be.

Accepted as the price of the guarantee. Revisit if a profile shows a transaction
whose nested-frame count is large *and* whose per-frame work is small — that is the
only shape where 6.8% could grow.

**A contract change for backend authors.** `type Store = storeapi.Store` is an alias,
so the boundary is now something any implementation must provide, even though no
signature moved. `fakeStore` satisfies it only vacuously (it opens no transaction, so
there is nothing to unwind, while its in-memory mutations would not unwind) — no test
may use it to exercise the guarantee.

**Not done, deliberately:** `EdgesAdd`'s ordering is left alone. The boundary makes
the reordering unnecessary, not wrong, and undoing it would mix a mechanism change
with an invariant that the `synchronous=NORMAL` argument leans on.
