# Spec: a nested `Within` is a real rollback boundary

- **Status:** Implemented. The accepted design and the measured cost live in
  [the ADR](../docs/adr/2026-07-29-nested-within-savepoints.md); this file is kept for
  the audit and test-plan reasoning, which does not belong in a decision record.
- **Amended in review, after implementation.** Three things below were specced wrong
  and are corrected in the ADR: the hook watermark is positional here and must be
  ownership by frame id (a concurrent `AfterCommit` on an enclosing frame's ctx lands
  above the mark and would be discarded); the unwind statements must run on
  `context.WithoutCancel`, since `fn`'s ctx may be canceled by `fn` itself and
  `ExecContext` would skip the `ROLLBACK TO`; and latching `closed` must share a
  critical section with the hook drain, or a registration between them sees a
  committed transaction as a rolled-back one. A second review added two more: a frame
  abandoned by a panic must poison (the outermost `tx.Rollback` only covers a panic
  that keeps escaping, and `COMMIT` releases open savepoints), and `AfterCommit` on a
  closed ctx runs the hook inline rather than discarding it — reversing the
  "deliberate drop" this spec argued for, because `Within` and `conn` already treat a
  closed ctx as carrying no transaction and three consumers should not disagree.
- **Date:** 2026-07-29
- **Touches:** `sqlite/store.go` (`Within`, `txState`, `AfterCommit`),
  `internal/storeapi/storeapi.go` (the `Within` contract), `sqlite/store_test.go`

## Problem

`sqliteStore.Within` (`sqlite/store.go:135`) opens a transaction only when the ctx
does not already carry one. On the nested branch it returns `fn(ctx)` and opens
nothing:

```go
if _, ok := txFrom(ctx); ok {
    return fn(ctx) // nested: joins the outer tx and its hook queue
}
```

So an error returned from inside a nested `Within` unwinds nothing. Only the
outermost caller, handing that error back to the real `Within`, rolls anything
back — and a caller that logs it and carries on commits every write that already
landed. **Any pair of writes in this store is atomic only by the grace of its
callers.**

Today the reachable case is a controller's own multi-write `ControllerClient.Within`
block (`controller.go:265`), because every store mutator self-wraps and is a single
write. That is a thin margin, and it is not a design — it is an accident of the
current mutator set.

`DependenciesAdd` shows both the local escape and its limit. A stamp issued as a
second store call after `EdgesAdd` would let a caller that swallowed the stamp's
error commit the edge with no stamp — exactly the stranded dependent the
[stamp-every-new-edge ADR](../docs/adr/2026-07-29-stamp-every-new-dependency-edge.md)
exists to prevent. The answer there was *ordering*: fold the stamp into `EdgesAdd`
ahead of the insert, so no write can fail after the edge exists. That works only
because there are two writes and one of them can be moved. Reorder anything whose
second write depends on the first and there is nowhere to put it.

This spec retires the class rather than the instance.

## Goal

After this change, a nested `Within` that returns an error leaves the store exactly
as it was when that nested `Within` was entered, whatever the outer caller then does
with the error — including nothing.

Everything else stays as it is. In particular:

- The outermost transaction is still the only thing that commits, still under
  `BEGIN IMMEDIATE`, still holding the sole WAL write lock throughout.
- Durability is unchanged. A savepoint is an in-transaction marker; it adds no
  fsync and does not change what `synchronous=NORMAL` can lose.
- No existing reordering is undone. `EdgesAdd`'s stamp-ahead-of-insert stays where
  it is (see [Non-goals](#non-goals)).

## Design

### Nested branch

```
SAVEPOINT <name>
  fn(ctx)
    err  -> ROLLBACK TO <name>; RELEASE <name>; return err
    nil  -> RELEASE <name>;                     return nil
```

`ROLLBACK TO` does not pop the savepoint in SQLite — it rewinds to it and leaves it
on the stack. The `RELEASE` after it is required, or the stack grows for the life of
the transaction.

### Savepoint names

Names come from a monotonic counter on `txState`, taken under `mu`, formatted
`bh_sp_<n>`. Monotonic rather than depth-indexed: a depth counter reuses a name
after an unwind, and `ROLLBACK TO` on a duplicate name rewinds to the *most recent*
match, which is correct but leaves the reader proving it. The counter is per
transaction, so it resets with each outermost `Within` and never has to be large.

The name is interpolated into the statement, not bound — SQLite does not accept a
parameter where a savepoint name goes. It is generated from an `int64` we own, so
there is no injection surface, and the code should say so in one line.

Build the statement with `strconv.AppendInt` into a stack array
(`append(buf[:0], "SAVEPOINT bh_sp_"...)` then `AppendInt`) rather than
`fmt.Sprintf`. No table of preformatted names: the counter is monotonic *per
transaction*, so it indexes how many nested `Within`s that transaction has made in
total, not how deep it currently is — a `ControllerClient.Within` block doing twenty
creates at depth 1 would fall off any fixed table almost immediately. `AppendInt`
has no such cliff and nothing to explain.

### The hook watermark

`txState.hooks` is append-only and drains at the outermost commit. A savepoint
rollback has to unwind it too, or a `WithOnCreate` fires for a row that was rolled
back — which is the single guarantee `AfterCommit` exists to provide
(`internal/storeapi/storeapi.go:261`).

The nested branch takes `mark := len(st.hooks)` under `mu` at the `SAVEPOINT`, and
on the error path truncates back to it: `st.hooks = st.hooks[:mark]`.

The `flushed` latch does not complicate this, and the reason is worth writing down
next to the code: `flushed` is set by `takeHooks`, which runs only after the
outermost `Commit`. While any nested `Within` is in flight the transaction has not
committed, so `flushed` is provably false and the truncation never has to reason
about a drained list. What *can* happen is a nested `Within` entered on a stale
`txState` after the outer transaction has finished — see below.

### A closed `txState`, and `conn` with it

This is the one behaviour change beyond error-unwinding, and it exists because
`AfterCommit` deliberately hands a hook a live `txState` on a dead `*sql.Tx`.

`AfterCommit` strips the transaction from the ctx it passes a hook, but the contract
explicitly supports a hook that passes back the transaction ctx *it captured*
(`storeapi.go`, "This holds even when the hook passes back the transaction context
it captured rather than the detached one it was handed"), and
`TestAfterCommit` pins it. That ctx still carries the `txState`, whose `tx` is
committed. Today a nested `Within` on it succeeds silently as long as `fn` makes no
store call; under this spec the `SAVEPOINT` statement would go to a dead `*sql.Tx`
and fail.

Add an explicit `closed bool` to `txState`. **Set it immediately after `tx.Commit()`
returns, before the hook drain loop** — not only in a `defer`. The drain runs in
`Within`'s body, so a deferred set is still false for the entire window in which
hooks execute, which is precisely the window this flag exists for. Keep a deferred
set as well, under `mu` and idempotent, to cover the rollback and early-return
paths: `flushed` is no substitute there, since a rolled-back transaction never sets
it.

`Within`'s nested branch treats `closed` as *no ambient transaction* and opens a
fresh one, which is the same answer `AfterCommit` already gives for a late
registration: the transaction is over, so "inside it" cannot be honoured, and
running now is what the caller could have meant.

**`conn` must agree.** `s.conn` (`sqlite/store.go:108`) returns `st.tx` whenever the
ctx carries a `txState`, so without a matching check the same ctx would take a fresh
transaction through `Within` and a dead one through a bare `ObjectsGet` — half the
ctx behaving as "the transaction is over". `conn` falls back to `s.db` when
`closed`, so the whole ctx degrades together.

**`addHook` must consult `closed` too, and the two closed states differ.** Today
`addHook` refuses only when `flushed`, which `takeHooks` sets — and `takeHooks` runs
only on the commit path. A hook registered on a captured ctx whose transaction
*rolled back* therefore appends to a list nothing will ever drain: a silent drop, the
exact failure `addHook`'s took-ownership return was built to prevent. That is
pre-existing, but `closed` is the flag that closes it and this spec is what adds it,
so it is in scope. The three outcomes are not two:

- `flushed` — the transaction committed and drained. **Run inline.** "After the
  commit" is now. This is today's behaviour, unchanged.
- `closed && !flushed` — the transaction rolled back. **Discard, and run nothing.**
  Running inline here would fire a `WithOnCreate` for a row that never landed, which
  is the single guarantee `AfterCommit` exists to provide
  (`internal/storeapi/storeapi.go:261`). The drop is deliberate and must say so in a
  comment; it is the correct reading of "a rolled-back transaction never runs its
  hooks".
- otherwise — **queue**.

So `addHook`'s bool becomes a three-way result. Both non-queue arms need naming in
the code, or the next reader will collapse them back into one.

### A failed `ROLLBACK TO`

If `ROLLBACK TO` or `RELEASE` itself errors, the transaction is in an unknown state
and must not commit. Set a `poisoned` flag on `txState`. The outermost `Within`
checks it after `fn` returns and rolls back instead of committing, returning the
savepoint error joined with whatever `fn` returned (`errors.Join`). Hooks are
discarded, as on any rollback.

**The nested branch also checks `poisoned` on entry and returns immediately**, so a
caller that swallows the poison error cannot keep issuing writes against a
transaction in unknown state. The outermost check is what guarantees the rollback;
this one narrows the window in which anything else happens.

This is not defensive padding — silently committing after a failed unwind is the
exact failure this whole change is meant to make impossible.

### Depth, and detecting a sibling goroutine

`txState.mu` exists because a `Within` fn may fan store calls across goroutines on
the tx ctx. Savepoints are a stack, so two concurrent nested `Within`s on one
transaction can interleave such that one's `ROLLBACK TO` discards work the other had
already released.

**Decision: document concurrent nested `Within` on a single transaction ctx as
unsupported, and detect it rather than serialise it.** Serialising by holding a
mutex across `fn` deadlocks the moment `fn` waits on another goroutine that also
wants the store.

**Detection must not be a shared in-flight counter.** A counter cannot tell "another
goroutine is inside a nested `Within`" from "I am legitimately two frames deep", and
deep nesting is the normal case here, not an edge: `ControllerClient.Within`
(`controller.go:265`, outermost) → `Client.Create`'s `store.Within`
(`client.go:276`) → `ObjectsCreate`'s self-wrap (`sqlite/store.go:222`) is depth 3
in production, with the same shape through `gc.go:46` →
`DeletionRequestsCreateFromOwner`. A counter would reject `Create`.

Carry the depth **on the context** instead:

- Each `Within` frame puts `depth+1` on the ctx it passes to `fn`.
- `txState` holds the current stack height, guarded by `mu`.
- On entry, compare the ctx's depth against the stack height. **Equal** means the
  caller is the rightful next frame — push. **Unequal** means someone pushed
  underneath this goroutine while it held its ctx, which is exactly the interleaving
  worth refusing: return `ErrConcurrentNestedTx`.

Naming still comes from the monotonic counter above, not from the depth — the
name-reuse argument is unaffected.

**The depth must live in the same context value as the `txState`, not under a key of
its own.** A separate key is sticky: it survives every operation that does not
explicitly clear it, and the outermost branch only installs `txKey`. A ctx carrying a
stale depth from a finished transaction would then install a fresh `txState` at
height 0 while still reporting nonzero depth, and the first nested call inside it
would mismatch and return `ErrConcurrentNestedTx` on one goroutine doing nothing
wrong.

That is not hypothetical — it is `TestAfterCommit`'s hook arm
(`sqlite/store_test.go:1791`), the test this spec's audit names as the direct
tripwire. `AfterCommit` builds its hook ctx with
`context.WithValue(ctx, txKey{}, nil)`, which strips exactly one key, so a
separately-keyed depth rides straight through into the hook. The hook's own `Within`
is outermost at height 0, `ObjectsUpdateSpec`'s self-wrap arrives at stale depth 2,
and the assertion "a hook must be able to open its own transaction" fails. The
`closed` path inherits the same defect: a fresh transaction opened on a captured ctx
would carry the dead transaction's depth, failing test 5 for an unrelated reason.

So the context value is one struct:

```go
type txFrame struct {
    st    *txState
    depth int
}
```

Installing a new `txState` necessarily resets the depth, because they are one value.
This is the same argument the existing `txState` doc comment makes about `tx` and
`hooks` — "the two travel as one value on purpose … answering them from two
independent context keys would let a future path install one without the other". A
second key for depth reintroduces exactly the failure mode that comment exists to
prevent. (If two keys were kept, the outermost branch would have to stamp depth 0
explicitly — an invariant nothing enforces and that breaks silently.)

### Restoring the height

The stack height decrements on **every** exit path from a nested frame, including the
one where `SAVEPOINT` itself fails. If the check-and-push happens under `mu` and the
exec is issued after, a failed `SAVEPOINT` that left the height incremented would
give every subsequent sibling on that transaction a spurious
`ErrConcurrentNestedTx`. A `defer` taken immediately after a successful push is the
straightforward way to get this on all paths at once.

**A failed `SAVEPOINT` does not poison.** Nothing was pushed on the SQLite side, so
the transaction is in a known state and the caller's ordinary error handling is the
right answer. Poison is for a failed *unwind*, where the state is unknown.

`ErrConcurrentNestedTx` is a contract-level error every backend must be able to
return, so it is declared in `internal/storeapi` alongside `ErrNotFound` and
re-exported from the `beehive` package the same way (`store.go`).

This is not a new restriction in substance. The `storeapi` `Within` doc already
warns that a store call on any *other* ctx deadlocks on a single-connection
backend, and `database/sql` serialises statements on a `*sql.Tx` but not blocks of
them. Concurrent nested `Within` was already meaningless; this makes it loud.

### Panics

The nested branch must **not** recover, and must not use a `defer` that "balances the
stack" on the way out of a panic. A panic unwinding through a nested `Within` skips
its `RELEASE`, and that is fine: the outermost `Within`'s existing
`defer tx.Rollback()` discards the whole transaction, savepoint stack included. A
recover here would convert a panic into a half-committed transaction. This matches
the library's standing position that panics surface (`Reconcile` is not recovered
either).

### The `storeapi` contract

`Store.Within`'s godoc gains the boundary guarantee, and this is the part that
reaches beyond the sqlite package: `type Store = storeapi.Store` is an alias, so
this is a **contract change for any backend author**, even though no signature
moves. State plainly that a nested `Within` must unwind its own writes and its own
queued `AfterCommit` hooks on error, and that the outermost transaction is still the
only thing that commits.

`fakeStore` (`testutils_test.go:155`) needs no code change, but the comment must be
precise about *why*: it satisfies the new contract **vacuously**, not correctly. It
opens no transaction, so there is nothing to unwind — while its overridden mutators
really do mutate in-memory maps, and those mutations will not unwind. The load-bearing
line is therefore a prohibition, not a reassurance: **no test may use `fakeStore` to
exercise the rollback-boundary guarantee.** Every test in this spec runs on the real
sqlite store.

## Cost

Two extra statements per nested `Within`. Every self-wrapping mutator called inside
a caller's `Within` block becomes its own savepoint: the twelve `s.Within` sites in
`sqlite/store.go`, plus every mutator reached from `client.go:276,418,469,528`,
`gc.go:46`, and `controller.go:265`.

No journal flush and no fsync — this is in-memory bookkeeping inside an open
transaction. But "free" is too strong on `modernc.org/sqlite`: it is a pure-Go
translation with no statement cache by default, so each `SAVEPOINT`/`RELEASE` pays a
full prepare-and-step, and for statements this trivial the compilation dominates.
`gcCollect` nests roughly four mutators per object per sweep, so a GC pass over N
objects adds on the order of 8N prepares — all of them extending the hold on the sole
WAL write lock.

Almost certainly noise against the real work, but **measure rather than assume**: a
two-line benchmark over `gcCollect` and the owed pass, before and after, is part of
this change. Statement construction uses `strconv.AppendInt` into a stack array (see
[Savepoint names](#savepoint-names)), which has no allocation cliff at any depth or
call count.

### When the poison path actually fires

Worth one line in the code so the flag reads as engineering rather than paranoia.
SQLite rolls back the *entire* transaction — savepoint stack included — on
`SQLITE_FULL`, `SQLITE_IOERR` and `SQLITE_NOMEM`, and a `ROLLBACK TO` issued after
one of those genuinely fails, because the savepoint it names no longer exists. The
common errors are not in that class: `SQLITE_BUSY` and constraint violations are
statement-level aborts, so savepoints behave normally through them. The poison flag
is for the first class.

## Audit

The TODO's second deferral reason is that this changes the meaning of every nested
`Within` at once, on a store where the whole suite runs through this one function.
That audit is part of the work, not a follow-up. What it has to establish:

- **Nothing relies on "a nested error unwinds nothing."** Reviewed ahead of writing
  this: `TestReconcileRunsGCAfterCommittedWritesOnError` (`reconciler_test.go:1779`)
  looks like the risk and is not. Its `FinalizersDelete` runs with no ambient
  transaction, so that mutator's own `Within` is *outermost* and commits on its own;
  the reconcile then errors outside any transaction. Writes that committed on their
  own stay committed — this change touches only compositions. Confirm the same for
  every `Within` site listed above, one by one.
- **`TestAfterCommit`** (`sqlite/store_test.go:1791`) is the direct tripwire. Its
  nested-`Within` arm and its hook-registers-a-hook arm both have to keep passing
  unchanged; if either needs editing, the `closed` handling above is wrong.
- **`sqlite/store_test.go:1923` and `:2201`** nest `Within` explicitly and are the
  next-closest existing constraints.

## Tests

New, in `sqlite/store_test.go` (test files mirror source files):

1. **A nested error unwinds its own writes, and the outer caller's survive.** Outer
   `Within` writes A; nested `Within` writes B and returns a sentinel; the outer
   *swallows* the error and returns nil. Assert the outer commits, A exists, B does
   not. This is the whole feature in one test, and it fails on today's code.
2. **A nested error discards only its own hooks.** Same shape with an `AfterCommit`
   registered either side of the nested block: the outer hook runs, the nested one
   does not. This is the watermark.
3. **Sibling nested blocks are independent.** Two sequential nested `Within`s under
   one outer; the first fails and is swallowed, the second succeeds. Assert the
   second's write commits — a rewind must not take a later sibling with it.
4. **Depth ≥ 3 unwinds to the right marker.** Rollback at depth 2 leaves depth 1's
   writes intact and the outer commit clean.
5. **A nested `Within` on a captured, committed tx ctx opens a fresh transaction**
   and its write lands — the `closed` path. **This must be driven from inside an
   `AfterCommit` hook**, passing back the transaction ctx the hook captured. Driving
   it after the outer `Within` has already returned exercises only the deferred set
   and passes even when `closed` is set too late; the hook-drain window is the one
   that has to be pinned, because it is the window the contract promises.
6. **A bare read on that same captured ctx also runs on the pool** rather than the
   dead `*sql.Tx` — the `conn` half of the `closed` path, so the two cannot drift.
7. **Concurrent nested `Within` is rejected** with `ErrConcurrentNestedTx` rather
   than corrupting the savepoint stack: a second goroutine entering a nested
   `Within` on the tx ctx while the first is in flight. The ctx-depth check is what
   this pins, so it must *not* be reachable by ordinary deep nesting — test 4 is its
   companion, asserting depth ≥ 3 on one goroutine is accepted.

   **Synchronize on a signal, not a sleep** (house rule): the first nested `fn`
   closes a channel to announce it is inside, the second goroutine waits on that
   channel before entering, and the first waits on a channel the second closes before
   returning. Without it this lands as the flaky test in the suite.
8. **A poisoned transaction refuses further nested `Within`s and never commits.**
   Swallow the poison error in the outer `fn`, attempt another nested write, and
   assert both that it is refused and that the outermost `Within` rolls back rather
   than committing.

   **The seam needs no production test hook**: from inside the nested `fn`, issue
   `RELEASE bh_sp_<n>` directly on the tx ctx. The savepoint is then already gone
   when the nested frame unwinds, so its `ROLLBACK TO` fails with "no such
   savepoint". The repo's only existing fault-injection trick (`newRawStore` +
   `db.Close()`) is wrong here — it kills the connection, which makes "the outermost
   rolls back rather than commits" unobservable.

Existing tests to keep green unchanged: `TestAfterCommit` in full, and the two
explicit-nesting sites named in the audit.

## Non-goals

- **Reordering `EdgesAdd`.** The stamp stays folded in ahead of the insert. This
  change makes the reordering no longer *necessary*, not wrong, and unwinding it in
  the same commit would mix a mechanism change with a load-bearing invariant. It is
  also cited by the `synchronous=NORMAL` argument (TODO, last item) as the reason a
  lost commit takes edge and stamp together; if that reordering is ever revisited,
  the ADR and that argument move with it.
- **Exposing savepoints publicly.** No new `Store` method, no nesting depth in any
  signature. The improvement is entirely in what `Within` already promises.
- **Changing what the outermost transaction does.** Isolation, `BEGIN IMMEDIATE`,
  hook ordering and hook detachment are all untouched.

## Sequencing

Land this before the `Store` shape break (`ObjectsCreate`'s input type, mutator
return shapes, `ReconcileOwedDecrement`'s kind scoping). That work churns every
mutator's write path, and the audit above is worth running once against a stable set
of mutators rather than twice.

## Follow-up

Fold the accepted design into a new ADR under `docs/adr/` once implemented —
`YYYY-MM-DD-nested-within-savepoints.md` — with the one-or-two-sentence summary and
link in `CLAUDE.md`, and delete the TODO item. The ADR keeps the *decision*
(savepoint boundary, watermark, unsupported concurrency); this spec's audit and test
list do not need to outlive the change.
