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

- **The periodic resync is unsettled-only, so it is not a backstop for
  event-driven wakes** — known, not fixed. The architecture says "events are a
  latency optimization; periodic resync is the correctness backstop", but the
  tick (`reconciler.run`) enqueues only `ListUnsettledIDs` +
  `ListDeletionPendingIDs`, and unsettled is `observed_generation < generation`.
  A *settled* object is therefore never re-dispatched by the tick, so the claim
  holds for an object's own spec convergence and for deletion progress — and for
  nothing else.

  The gap is the ordinary dependent pattern. D depends on T; D reconciles, sees T
  not ready, writes a "waiting" status at `obj.Generation`, and is now settled.
  When T becomes ready, D's own generation never moves, so the dependency waker is
  the *only* thing that re-reconciles it. Lose that one wake (see the item below)
  and D sits at "waiting" until its spec changes or the process restarts. The
  startup pass already does the full sweep (`StartupReconcileAll`) on the reasoning
  that "staleness is purely a startup concern" — that is the assumption at issue
  here, and it holds only if no wake is ever lost at runtime. Kubernetes'
  resync period re-delivers every object precisely so missed events self-heal.

  The fix is to let the tick enqueue everything: always, every Nth tick, or behind
  a `WithResyncStrategy` mirroring the existing `WithStartupReconcileStrategy`. The
  cost is one reconcile per object per period, which is bounded and mostly free —
  reconciles are level-triggered, and the content no-ops mean a converged object's
  pass writes nothing and emits nothing, so it cannot cascade into other kinds.
  Deferred because it is a behavior change for existing users and the shape (on by
  default vs opt-in) wants a decision, not a guess. Revisit before making any
  further claim that resync backstops event delivery — today it does not.

- **Three silent loss points in the dependency waker, none of them logged** —
  known, not fixed. Given the item above, a dropped dependency wake is permanent
  for the process, so each of these is a stuck-dependent bug rather than a latency
  hiccup:

    - `wakeDependents` swallows its `ListIncomingRefs` error and returns. One
      transient failure and every dependent of that target misses that change,
      silently. This is the same efficiency-flavored silent drop that the
      `UpdateStatus` no-op path was fixed away from, and the cheapest to correct.
    - `runDependencyWaker` returns on a closed change stream with no log and no
      re-subscribe, so a kind's wakes would be dead for the process lifetime with
      nothing saying so. No reachable path closes that channel short of store
      `Close` today, so this is latent rather than live — but it is unlogged
      either way.
    - `Start`'s subscribe-failure branch warns "relying on resync", which per the
      item above is not the coverage it sounds like: the timer reaches that
      controller's unsettled objects, not the dependency wakes the failed
      subscription was to deliver. The code is fine; the comment promises
      something that does not exist, and is the likely reason a reader would
      believe the gap is already handled.

  The fix for the first two is a `missedWake` flag that forces the next resync
  tick to enqueue everything for registered kinds — the targeted version of the
  item above, free in the normal case — plus logging at Warn. The third is a
  comment correction and should land regardless of what happens to the other two.
  Deferred with them because the flag only means something once the tick can do a
  full pass.

- **`advanceGCNow`'s synchronous collect inherits the caller's cancellation** —
  known, not fixed, and already documented inline. With resync disabled, a
  `Delete` (or freed-target wake) whose caller cancels immediately after commit
  abandons the collect mid-flight; `Start` is one-shot, so nothing in that process
  retries and the row stays deletion-pending, RESTRICT-blocking its owner, until a
  *fresh* `Beehive` runs its unconditional startup sweep over the same store.

  The fix is `context.WithoutCancel(ctx)` for the collect — finishing work the
  caller stopped waiting on, which is the trade this deliberately declined. That
  call predates the decision (recorded on the `UpdateStatus` handshake) to prefer
  a slightly wasteful self-healing path over a silent strand, so it is worth
  re-taking on those terms rather than left as settled. One line if so.

- **`reconciler.enqueueFrom` swallows its list error** — known, not fixed, and the
  benign member of this set: a failed resync list skips one tick and the next
  retries, so it self-heals on cadence. The only gap is that it is silent, unlike
  the GC sweeper's equivalent (`sweepDeletionPending`), which warns. Worth a log
  line when one of the items above is touched; not worth a commit on its own.

- **Mutators materialize a `RawObject` no caller reads** — known, not fixed, and
  the general form of the point the `DeleteBySlug` item above records for one
  method. Every mutator returns a full `*RawObject` with conditions assembled:
  `scanAndEmit` calls `attachConditions` on the write path, and the branches that
  emit nothing (`UpdateSpec`'s content no-op, `UpdateStatus`'s settled no-op,
  `requestDeletion`'s already-pending re-read) still pay a conditions query
  to build a value their sole caller discards — `controllerClientImpl.UpdateStatus`
  drops it, `client.go`'s delete path reads only `obj.ID`. On an emitting path the
  work is load-bearing (the `Modified` event carries the object body and its
  conditions), so this is only slack on the silent branches.

  Skipping `attachConditions` per-branch is the wrong fix and was rejected: it
  would make one method's return shape depend on which branch it took, and diverge
  from its sibling a few lines away. The contract is the thing to change — the
  `Store` godoc says a returned `RawObject` matches the `Get` shape, so the options
  are narrowing that promise for the silent branches, splitting off mutator
  variants that return nothing (or just the id/`resource_version`), or making the
  discard explicit at the `storeapi` boundary so the store can skip the assembly.
  Deferred because `type Store = storeapi.Store` is an alias, so any of these is a
  break on an externally-implementable interface, and the saving is one indexed
  query per silent write. Revisit when the next `Store` break is on the table
  anyway — the v0.17.0 `RequestDeletionBySlug` change was exactly such a moment and
  would have been the cheap time to take this with it.

- **`incoming == 0` conflates "no migrator" with "unversioned", so an old build can
  launder reshaped bytes under the stored schema version** — known, not fixed.
  Explore a `WithSchemaVersion(n)` option that lets a kind declare its schema
  version independently of registering a `Migrator`.

  The mechanism: `convertBlob`'s `current == 0` identity lets a build with no
  migrator decode a v3 row untouched. If that build's struct is the *older* shape,
  `json.Unmarshal` silently drops the v3-only fields; the write-back then reports
  `incoming == 0`, `stampVersion` keeps the stored tag, and v-old-shaped bytes end
  up labeled v3. A later v3 reader sees `from == current`, skips conversion, and
  misinterprets them. `stampVersion` (`sqlite/store.go:535`) is where it gets
  laundered, but it is not where the information is lost.

  The obvious guard — reject a content change when `stored > 0 && incoming == 0` —
  was considered and rejected. `incoming == 0` means "no migrator registered", not
  "old struct", and those come apart constantly: registering a `Migrator` is
  optional, so a build carrying the *current* struct with no converter yet writes
  faithful v3 bytes and reports 0; and a client-only kind (never `Register`ed)
  cannot attach a migrator at all, so any embedder driving the DB purely through
  `Client` reports 0 by construction. The predicate is therefore dominated by the
  benign case, and the guard would wedge those writers permanently (every reconcile
  erroring) to defend against a mixed-binary rollback.

  Nor can the store pick a better tag from what it has. Bytes changing does not
  imply reshaping — an ordinary `Update` changes bytes too. Stamping `stored` is
  wrong when the round-trip was lossy; stamping `0` is wrong when it was faithful
  (a restored build would then see `from < current` and re-convert already-converted
  data — the case the `CLAUDE.md` handshake bullet argues through). There is no
  third answer available at that call site, which is what makes this a signal
  problem rather than a stamping bug.

  Hence `WithSchemaVersion(n)`: give a kind a way to say "my shape is v3" without
  shipping a converter, reachable from client-only kinds too. Then `incoming == 0`
  really does mean unversioned, the guard above becomes sound, and the fix lands
  where the ambiguity is instead of on top of it.

  The read path deserves the same pass in the same sitting: if an unversioned build
  cannot be trusted with a v3 row, `from > 0 && current == 0` is arguably the same
  "older build reading newer data" downgrade as `from > current`, and refusing it in
  `convertBlob` stops the lossy decode *before* it can become a write. Blocking only
  the write yields a process that can observe but not act — defensible, but it
  should be one deliberate decision across both sides.

  Deferred because it needs a new public option and a read-path policy change
  together, and the corruption it prevents requires two builds of differing struct
  shape sharing one DB file. Revisit before the first real `Migrator` consumer ships
  a v2, or the first time a rollback across a schema bump is a supported operation.

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
