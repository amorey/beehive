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

Seven things make it correct rather than merely plausible:

- **Queued hooks record their owning frame.** `txState.hooks` drains at the outermost
  commit, so an unwind must take with it the hooks registered inside the frame —
  otherwise a `WithOnCreate` fires for a row that is gone. Ownership is the frame's
  savepoint id, and an unwind drops `id >= name`: ids are monotonic and a frame can
  only open while its enclosing frame is live, so that is exactly "registered inside
  this frame". A positional watermark was the first attempt and is wrong, because
  `AfterCommit` is *not* a nested `Within` and stays legal from another goroutine — a
  concurrent append lands at a position that says nothing about which frame it belongs
  to, and truncating by length silently discards an enclosing frame's hook. That is
  reachable on one goroutine: the enclosing ctx is in lexical scope inside the nested
  fn.

  Dropping the queued hooks is only half of it, because a frame's ctx outlives the
  frame. An unwind therefore also marks the frame's whole id range dead, and a later
  registration against any of it is discarded rather than queued fresh — otherwise it
  would ride the outer commit and fire for writes that were rolled back, which is the
  guarantee this feature exists for arriving through the back door.

- **The savepoint statements outlive `fn`'s context.** `fn` runs on the caller's ctx,
  and a caller may hand a nested frame a cancellable child and cancel it inside `fn`.
  `ExecContext` returns before running a statement on a canceled ctx, so an unwind
  issued on that ctx would skip the `ROLLBACK TO` entirely — leaving the frame's writes
  applied and poisoning a transaction that is otherwise healthy, which takes the whole
  outer transaction down with it. `ROLLBACK TO` and `RELEASE` therefore run on
  `context.WithoutCancel`. Cancellation is the caller's signal about `fn`, not about
  the bookkeeping that cleans up after it.

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

  The check is a tripwire, not a lock. It sees nested `Within`s; a store call that
  joins the transaction *without* opening a frame — the mutators that do not
  self-wrap, reaching the tx through `conn` — is invisible to it, and a write issued
  that way from a second goroutine can be discarded by an open sibling frame's unwind.
  The contract states the real rule, which the tripwire only samples: a transaction
  ctx belongs to one goroutine.

- **An abandoned frame poisons.** A panic unwinding through a nested frame skips its
  `RELEASE` and leaves the savepoint open. The outermost deferred `tx.Rollback` covers
  that only while the panic keeps escaping — a caller that recovers inside its own
  `fn` and returns nil reaches `COMMIT`, which releases every open savepoint and lands
  the writes of a frame that never completed. So a frame that exits without settling
  its savepoint poisons the transaction, which is the one state a recover cannot
  paper over. Nothing here recovers; poisoning is how an abandoned frame is noticed,
  not how it is handled.

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
  inside `Within`'s body and a deferred set would read false for exactly the window it
  exists to cover, and latching and draining share one critical section so nothing
  lands between them.

  `AfterCommit` does **not** share that rule, and the difference is the point.
  `Within` and `conn` are answering "how do I reach the store from this ctx", where
  degrading to a fresh transaction or the pool is harmless. `AfterCommit` is
  answering "should this side effect fire", where it is not. Its rule is its own:
  **a hook runs if and only if the transaction it was registered against committed,
  and the frame it was registered against did not unwind.** So `closed` is paired
  with `committed`, a rolled-back transaction discards a late registration rather
  than running it inline, and a registration against an unwound frame is discarded
  even while the outer transaction is still heading for a commit. A rolled-back
  outermost transaction is a frame unwinding one level up, and gets the same answer.

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
| before | 730,800 | 42,367 | 1,145 |
| after | 785,600 | 44,978 | 1,266 |

**+7.5% wall clock, +121 allocations** — about 15 per nested frame across the two
extra statements. Take the ratio, not the absolutes: the same two commits measured
~450k and ~480k ns/op on a quieter machine, so only a back-to-back pair on one host
means anything. `modernc.org/sqlite` is a pure-Go translation with no statement cache
by default, so each `SAVEPOINT`/`RELEASE` pays a fresh prepare-and-step; that, not the
savepoint itself, is the cost. Statements are built with `strconv.AppendInt`
into a stack array rather than `fmt`, which keeps it to one allocation each. The
figure is the isolated worst case: eight nested frames doing trivial inserts, where
the savepoints are the largest share of the work they will ever be.

Accepted as the price of the guarantee. Revisit if a profile shows a transaction
whose nested-frame count is large *and* whose per-frame work is small — that is the
only shape where 7.5% could grow.

**A contract change for backend authors.** `type Store = storeapi.Store` is an alias,
so the boundary is now something any implementation must provide, even though no
signature moved. `fakeStore` satisfies it only vacuously (it opens no transaction, so
there is nothing to unwind, while its in-memory mutations would not unwind) — no test
may use it to exercise the guarantee.

**Not done, deliberately:** `EdgesAdd`'s ordering is left alone. The boundary makes
the reordering unnecessary, not wrong, and undoing it would mix a mechanism change
with an invariant that the `synchronous=NORMAL` argument leans on.
