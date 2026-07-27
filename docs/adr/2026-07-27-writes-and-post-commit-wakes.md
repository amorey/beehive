# Slug-keyed writes, and wakes that run after the outermost commit

- **Status:** Accepted — implemented in `client.go`, `sqlite/store.go`.
- **Date:** 2026-07-27 (recorded retroactively)

## Reconcile is not transactional

`typedController.reconcile` loads the object and calls `Reconcile` with no
enclosing transaction; each `ControllerClient` write commits on its own (a write
before a returned error stays committed, and the level loop re-derives from it).

Each write is still internally atomic — the store mutators self-wrap in `Within`.
Cross-kind access is enforced inside the mutators too: each id-keyed write is
scoped to the caller's `GroupKind` (folded into the statement's `WHERE`, or
checked by a bare-row read in the mutator's existing transaction), so a foreign id
is rejected with `ErrWrongKind` and there is no separate kind-check transaction to
keep atomic.

A controller that needs several writes atomic wraps them in
`ControllerClient.Within` (which is `store.Within`; nested CC writes join via the
ctx's `txKey`). The store runs on a single connection (`SetMaxOpenConns(1)`,
`_txlock=immediate`), so an open transaction serializes all other writers for its
duration — which is why holding one across a whole reconcile was removed in favor
of this opt-in.

## The slug-keyed writes differ only in their conflict policy

`Create` errors, `CreateOrUpdate` updates the row to the new spec, `GetOrCreate`
returns it untouched (`created=false`).

`CreateOrUpdate` / `GetOrCreate` wrap their read-and-write in one `store.Within` —
that transaction, not the caller, is what makes the slug race safe (`Create`
doesn't read at all; its loser just takes the `UNIQUE` error).

They share `insertObject` for the created-row shape (spec-version stamp,
finalizers, owner ref); a new column or stamp is a one-site change there.

`GetOrCreate` rejects `WithSlug` with `ErrConflictingOption` rather than ignoring
it — the deliberate exception to "an option ignores targets it doesn't recognize",
since here the target *does* recognize it and the call site would be discarding a
value the caller believes took effect. (The original spec said ignore; it was
changed after review.)

`GetOrCreate`'s found branch does *no* write, which is the whole point: a
deletion-pending row comes back with its tombstone intact rather than being
resurrected by an `ObjectsUpdateSpec`, and it still holds `UNIQUE(slug)`, so waiting for
GC is the caller's only way forward.

### `DeleteBySlug` is `GetOrCreate`'s remove-side partner

It is the one slug-keyed write that needs no client-side `Within`: the store
resolves and marks in a single statement via `DeletionRequestsCreateBySlug`, which folds
the slug into the `UPDATE`'s `WHERE` exactly as `DeletionRequestsCreate` folds in the
kind.

That is why `markForDeletion` takes the caller's **whole row predicate** rather
than an id plus an `extraWhere` — one statement template, key supplied per caller
(`id = ?` + scope for `DeletionRequestsCreate`, bare `id = ?` for the
`DeletionRequestsCreateFromOwner` cascade, the group/kind/slug triple for the slug path); a
new keying is a call-site change, not a second copy of the statement.

The mark-or-reread protocol above it is shared too (`requestDeletion`, taking the
predicate plus a `reread` closure): `markForDeletion`'s `ErrNotFound` can't
distinguish already-deleting from out-of-scope from gone, and the reread that
resolves it — and supplies the current row the no-op path still needs for
`gcAdvance` — is the only part that differs between the two entry points.

Its `ErrNotFound` is therefore unambiguous — nothing of this kind holds the slug —
and the client folds it to `nil`, the one place a slug delete departs from
`Delete`, which reports a missing id. All `DeleteBySlug` still runs itself is
`gcAdvance` (pinned by `TestClientDeleteBySlugAdvancesGC`).

## Wakes run after the outermost commit

Every client write path registers its wake through `Store.AfterCommit`:
`Create` / `CreateOrUpdate` / `GetOrCreate` / `Update` via
`clientImpl.wakeAfterCommit`, and `Delete` via `gcAdvance`, which registers the
whole GC follow-up (`advanceGCNow`) as the hook. So the spec event precedes the
wake.

The wake is **gated on a real change**: `ObjectsUpdateSpec` returns `(obj, changed, err)`
(mirroring `DeletionRequestsCreate`), and `Update` / `CreateOrUpdate` skip the wake when
identical bytes made the write a no-op — otherwise the wake would be the only
signal claiming something happened, and a controller re-applying its own kind's
spec each pass would spin.

The hook is buffered on the tx-scoped `eventCollector` and run by `flush` after
the *outermost* commit, so a write nested in `ControllerClient.Within` can't wake
a controller at a row that is still uncommitted (or that rolls back): a controller
must not be woken at a row whose tombstone is not yet visible, or for a deletion
the caller then rolls back. Deferring `collect` is safe because it is
level-triggered — it re-reads `DeletionRequestedAt`, so a rolled-back deletion is
simply nothing to collect. Outside a transaction the hook runs inline, so
unnested behavior is unchanged.

### Mechanics worth not rediscovering

- The hook's ctx is **detached** from the committed transaction (`txKey` stripped
  in `sqliteStore.AfterCommit`) — a hook that writes must open a fresh
  transaction, not join a dead `*sql.Tx`.
- The connection and the collector ride the ctx as **one** `txState` value under
  `txKey`, not two keys: "am I in a transaction?" and "where do I buffer?" are the
  same question, and splitting them would let a future path install a tx with no
  collector, making `AfterCommit` fire inline mid-transaction.
- The collector outlives its transaction (a hook can hold the tx ctx it captured),
  so `flush` latches it `flushed` and `add` / `addEventRow` / `addHook` return a
  took-ownership bool: a late registration runs inline instead of queueing where
  nothing will drain it.

Per-method contracts live in the godoc, not here.
