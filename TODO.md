# TODO

Deferred work, with the reasoning that led to deferring it. An item earns a place
here when it is a real defect or gap that we chose *not* to fix yet — not a
wishlist. Each one records what would make it worth doing, so the next reader can
tell "we decided against this for now" from "nobody thought of it."

- **`DeleteBySlug` on an absent slug costs a write transaction** — known, not fixed.
  `RequestDeletionBySlug` opens `Within` (so `BEGIN IMMEDIATE`) and its first act
  inside is `nextResourceVersion`, an `UPDATE` on the sequence — both before
  anything is known to match. A slug no row holds therefore costs a write
  transaction, a sequence write, the zero-row `UPDATE`, the re-read, and a
  rollback, where the pre-mutator client code cost a single lock-free `SELECT`.
  The rollback means no cursor value is burned, but the journal/page work happens.

  This is the steady state of the operation the method exists for: an idempotent
  remove a controller re-runs each reconcile keeps hitting the absent path long
  after the one call that did the deleting. The two paths that *do* touch a row
  each got two statements cheaper, so the change is a net win — this is the one
  path that regressed.

  The fix is a lock-free probe (`getObjectRowBySlug`) before `Within`, short-
  circuiting both idempotent outcomes — no such slug, and a row already
  deletion-pending — and falling through to the atomic mark otherwise. That gives
  absent 1 statement, no-op 2, happy path 4 (one more than today). Deferred because
  it reintroduces the read-then-write shape this change removed, as a fast path,
  and its no-op branch would answer outside a transaction where the id-keyed
  sibling answers inside one — a divergence worth more thought than the saving
  justifies right now. Revisit if a profile shows absent-path deletes are hot, or
  if `RequestDeletion` ever gets the same treatment (its absent path has always
  had this shape, so the probe would belong in `requestDeletion` for both).

## Resolved

- **Slug-keyed delete as a store mutator (`RequestDeletionBySlug`)** — done.
  `Client.DeleteBySlug` previously resolved the slug with `GetObjectBySlug` and then
  delegated to the id-keyed `Delete`: two statements with a window between them, in
  which the row could be collected and the slug retaken, leaving the call to report
  nil against a live row it never touched. It now calls a slug-keyed mutator that
  folds the slug into the `UPDATE`'s own `WHERE`, the way the kind already rode in
  for `RequestDeletion`, so the resolve and the mark are one atomic statement.

  The enabling change was generalizing `markForDeletion`'s key predicate: it took an
  id plus an `extraWhere` for guards, and now takes the caller's whole row predicate
  (`id = ?` plus scope for the two id-keyed callers, `group`/`kind`/`slug` for the new
  one). `MarkOwnedForDeletion` — the GC cascade — shares that mutator and moved with
  it. Beyond atomicity this drops two round trips (5 queries to 3 on the success
  path) and *halves* the row materialization rather than removing it: the old code
  pulled `objectColumns` plus conditions twice — once for the resolve, once from the
  `UPDATE … RETURNING` — and now does it once. It also makes `ErrNotFound`
  unambiguous: *nothing of this kind holds the slug*, where the two-step could only
  say the id it had already resolved was gone.

  The remaining materialization is not slack to be reclaimed on the success path:
  `RETURNING objectColumns` feeds `scanAndEmit`, and the `Modified` event it
  publishes carries the object body, so narrowing it to `id` would strip the watch
  event. The no-op branch is the arguable one — it emits nothing, yet its re-read
  still pulls the blobs and conditions the sole caller ignores (`client.go` reads
  only `obj.ID`) — but the `Store` contract says a returned `RawObject` matches the
  `Get` shape, so narrowing it means narrowing that contract first.

  Taken as an API break on the externally-implementable `Store` (`type Store =
  storeapi.Store` is an alias, so the internal path doesn't protect it) — accepted
  deliberately at v0.17.0.

  On the alternatives, with one earlier argument corrected: wrapping the old
  two-step in `store.Within` was first rejected for "taking the write lock on every
  call, including the already-gone path". That was never the discriminator — the
  shipped mutator opens a transaction on every call too (`requestDeletion` wraps
  everything in `s.Within`), and on a store with `SetMaxOpenConns(1)` every caller
  is serialized by the connection regardless. What actually separates them is
  round-trip count: one statement instead of a lookup plus a write. The
  `SELECT id`-by-slug read stays rejected on its own terms — same interface break
  for strictly less, since it is still two statements and still non-atomic.

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
