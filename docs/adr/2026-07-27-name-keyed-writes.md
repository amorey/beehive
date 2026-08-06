# Name-keyed writes differ only in conflict policy

- **Status:** Accepted — implemented in `client.go`, `sqlite/store.go`.
- **Date:** 2026-07-27 (recorded retroactively)

> **Superseded in part.** The `Client` CRUD verbs are keyed on the id, and the name
> forms are `GetByName`, `UpdateByName` and `DeleteByName`. The name is also
> required, which retired `WithName` and `ErrConflictingOption` — so the `WithName`
> paragraph below is void, not merely renamed. Everything else here, the
> transaction-boundary reasoning above all, is unchanged and is what the newer
> decision builds on.
> → [ADR](2026-08-02-id-primary-key-with-byname-siblings.md)

## Reconcile is not transactional

`typedController.reconcile` loads the object and calls `Reconcile` with no enclosing
transaction. Each `ControllerClient` write commits on its own, so a write that lands
before a returned error stays committed and the next pass works from it.

Each write is still atomic by itself, because the store mutators self-wrap in
`Within`. The kind check rides along: every id-keyed write is scoped to the caller's
`GroupKind`, either folded into the statement's `WHERE` or checked by a bare-row read
inside the mutator's own transaction. So another kind's id is rejected with
`ErrWrongKind`, and there is no separate kind-check transaction to keep atomic with
the write.

A controller that needs several writes to land together wraps them in
`ControllerClient.Within`, which is `store.Within`; nested writes join it through the
ctx's `txKey`. The store runs on a single connection (`SetMaxOpenConns(1)`,
`_txlock=immediate`), so an open transaction blocks every other writer for as long as
it is open. That is why atomicity is opt-in per composition rather than held across a
whole reconcile.

## The name-keyed writes differ only in their conflict policy

`Create` errors, `GetOrCreate` returns the row untouched (`created=false`).

`GetOrCreate` wraps its read-and-write in one `store.Within` —
that transaction, not the caller, is what makes the name race safe (`Create`
doesn't read at all; its loser just takes the `UNIQUE` error).

They share `insertObject` for the created-row shape (spec-version stamp,
finalizers, owner ref); a new column or stamp is a one-site change there.

### There is no name-keyed upsert

A third policy — update the row to the new spec — shipped as `CreateOrUpdate` and was
removed on 2026-07-30. Nothing replaces it. What is left is a sharper rule than the
spectrum it sat in the middle of: **no name-keyed write ever writes to a row it
found.** Both surviving policies are decisions about *creating*, and mutation is
`Update`, keyed by id.

That is worth stating as a rule rather than an absence, because `CreateOrUpdate`'s
found branch was the one place the name-keyed family could resurrect a
deletion-pending row — it updated whatever the name named, tombstone included, which
sits badly beside `GetOrCreate`'s deliberate refusal to do exactly that a few lines
away. A caller that genuinely wants ensure-then-set still composes `GetOrCreate` with
`Update` inside its own `Within`, which costs one extra statement on the create path
and makes the tombstone question the caller's to answer rather than one this API
answered for them by accident.

~~`GetOrCreate` rejects `WithName` with `ErrConflictingOption` rather than ignoring
it.~~ **Void**: the name is now positional on every name-keyed write, so there is no
option left to contradict it. `WithName` and `ErrConflictingOption` are both deleted.

`GetOrCreate`'s found branch does no write at all, which is the point. A
deletion-pending row comes back with its tombstone intact rather than being
resurrected by a spec update, and it still holds the name's `UNIQUE` constraint, so
waiting for GC is the caller's only way forward.

### `DeleteByName` is `GetOrCreate`'s remove-side partner

It is the one name-keyed write that needs no client-side `Within`: the store
resolves and marks in a single statement via `DeletionRequestsCreateByName`, which folds
the name into the `UPDATE`'s `WHERE` exactly as `DeletionRequestsCreate` folds in the
kind.

That is why `markForDeletion` takes the caller's **whole row predicate** rather than
an id plus an extra clause. One statement template, with the key supplied per caller:
`id = ?` plus the kind scope for `DeletionRequestsCreate`, a bare `id = ?` for the
`DeletionRequestsCreateFromOwner` cascade, and the group/kind/name triple for the name
path. Keying it a new way is a change at the call site, not a second copy of the
statement.

The mark-or-reread protocol above it is shared too, as `requestDeletion`, which takes
the predicate plus a `reread` closure. `markForDeletion` returns `ErrNotFound` for
three different situations — already deleting, another kind's row, or simply gone —
and the reread is what tells them apart. It also supplies the current row that the
no-op path returns. That reread is the only part that differs between the two entry
points.

Its `ErrNotFound` is therefore unambiguous — nothing of this kind holds the name —
and the client folds it to `nil`, the one place a name delete departs from
`Delete`, which reports a missing id. Beyond the mark it runs nothing at all: the
`deletion_requested_at` it wrote is what puts the row in the GC sweeper's listing.

## `AfterCommit` defers exactly one thing

`Store.AfterCommit` is a post-commit callback queue with nothing published on it.
Its only user is `WithOnCreate` — a caller-facing guarantee that a create-conditional
side effect never fires for a row a rollback discarded. Beehive's own machinery
registers nothing there: a write records its trace and a periodic driver finds it
(see [periodic scan drivers](2026-07-28-periodic-scan-drivers.md)).

The deferral is to the **outermost** commit, so a `Create` nested in a caller's
`ControllerClient.Within` fires its hook only once that whole transaction lands.
Outside a transaction the hook runs inline.

### Mechanics worth not rediscovering

- The hook's ctx is **detached** from the committed transaction (`txKey` stripped
  in `sqliteStore.AfterCommit`) — a hook that writes must open a fresh
  transaction, not join a dead `*sql.Tx`.
- The connection and the hook list ride the ctx as **one** `txState` value under
  `txKey`, not two keys: "am I in a transaction?" and "where do I buffer?" are the
  same question, and splitting them would let a future path install a tx with no
  buffer, making `AfterCommit` fire inline mid-transaction.
- The `txState` outlives its transaction (a hook can hold the tx ctx it captured),
  so `takeHooks` latches it `flushed` and `addHook` returns a took-ownership bool:
  a late registration runs inline instead of queueing where nothing will drain it.

Per-method contracts live in the godoc, not here.
