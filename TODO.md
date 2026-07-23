# TODO

Deferred work, with the reasoning that led to deferring it. An item earns a place
here when it is a real defect or gap that we chose *not* to fix yet — not a
wishlist. Each one records what would make it worth doing, so the next reader can
tell "we decided against this for now" from "nobody thought of it."

- **Slug-keyed delete as a store mutator (`RequestDeletionBySlug`)** — deferred.
  `Client.DeleteBySlug` resolves the slug with `GetObjectBySlug` and then delegates
  to the id-keyed `Delete`, so the resolve and the delete are two statements with a
  window between them: a row collected in that window makes the call return nil
  (folded from `ErrNotFound`) even if a new row has since taken the slug. A
  slug-scoped mutator would be one atomic statement, would drop a round trip, and
  would make "no such slug" a first-class answer instead of an `ErrNotFound` the
  client unmasks.

  Deferred because the cost is larger than it looks and the payoff is small. The
  window's outcomes are all *linearizable* — for GC to collect the resolved row it
  must already be deletion-pending, which leaves an interval where nothing holds the
  slug, and "already gone → nil" is the documented contract at that point. Atomicity
  also does not fix the case that motivates it: a concurrent `GetOrCreate` can
  recreate the slug the instant the delete commits, so a live row after a nil return
  is reachable either way; the level-triggered loop is what actually converges it.
  Meanwhile `markForDeletion`'s `extraWhere` does *not* extend to this — it adds
  guards to a `WHERE id = ?` statement, so a slug variant means generalizing the key
  predicate of a mutator the GC cascade (`MarkOwnedForDeletion`) also calls — and
  `Store` is externally implementable (`type Store = storeapi.Store` is an alias and
  its whole method-set surface is re-exported), so a new method is an API break.

  The resolve is also wasteful in its own right: `GetObjectBySlug` is a full
  `SELECT objectColumns` plus an `attachConditions` query, so every `DeleteBySlug`
  materializes both JSON blobs and the conditions to read `obj.ID`. A metadata-only
  or `SELECT id` slug read would drop one query and the blobs — the slug sibling of
  `GetObjectMeta`, which already exists for exactly this reason but is id-keyed. It
  is a strictly weaker variant of the same fix, though: it keeps the non-atomic
  two-step and still costs a new `Store` method, i.e. the same API break for less
  benefit. Prefer the mutator if the break is being taken at all.

  Worth doing when a **second** slug-keyed write appears. Today every store write is
  id-keyed with `GetObjectBySlug` the lone slug read; one caller is not enough
  evidence for where the general slug-keyed mutator boundary belongs, and guessing
  wrong bakes it into the public `Store`. At that point: new `storeapi` method,
  generalized `markForDeletion` key predicate, a slug re-read to tell
  already-pending from absent, plus `fakeStore` and `sqlite/store_test.go` updates —
  as its own commit. The cheaper intermediate, wrapping the resolve+delete in
  `store.Within`, was considered and rejected: it takes the single write lock on
  every call including the already-gone path, which is the hot path for an
  idempotent remove a controller runs each reconcile.

## Resolved

- **Post-commit hook on the `Store` interface** — done.
  `Store.AfterCommit(ctx, fn)` buffers `fn` on the transaction-scoped
  `eventCollector` and runs it after the outermost commit's `flush` (inline when
  unnested, discarded on rollback, and handed a ctx detached from the committed
  transaction). Every client write path registers its follow-up through it:
  `Create`/`CreateOrUpdate`/`GetOrCreate`/`Update` via `clientImpl.wakeAfterCommit`,
  `Delete` via `advanceGC`. This closes the `Added`-after-`Modified` inversion, the
  wake-for-a-rolled-back-row case, and the cascade-of-physical-deletes-inside-the-
  caller's-transaction case on `advanceGC`'s synchronous-collect branch. The one
  consequence it does *not* fix — a `created=true` returned from a transaction that
  later aborts — is inherent to nesting and stays documented in the `GetOrCreate`
  godoc and the README's *Writes* section.
