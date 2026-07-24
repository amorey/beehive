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

  **`AddDependency`'s guard does not cover these, by construction.** It would have
  the evidence to: a dependent re-declaring its edge after a dropped wake passes the
  version it read *before* the change, and the target's current version is already
  past it. But the wake is gated on that call having created the edge, so a
  re-declaration never fires it. Dropping that half to pick up the repair trades a
  bounded miss for an unbounded one — a caller whose version never advances (cached
  across passes) would then re-fire every pass, unthrottled, and unlike a missed wake
  that spin is invisible to the caller and permanent for the process. The guard is
  deliberately an intent recorder for the declare window, not a staleness detector;
  what repairs a *lost* wake is a backstop that re-derives staleness, which is the
  `observed_cursor` watermark recorded under the item above. Noted here so the
  conjunction is not "simplified" away later on the reasoning that it is redundant.

  The fix for the first two is a `missedWake` flag that forces the next resync
  tick to enqueue everything for registered kinds — the targeted version of the
  item above, free in the normal case — plus logging at Warn. The third is a
  comment correction and should land regardless of what happens to the other two.
  Deferred with them because the flag only means something once the tick can do a
  full pass.

- **A dependency wake is in-memory, so a crash between the edge's commit and the
  requeue loses it** — known, not fixed. This is the durability half of the
  read-then-declare race; the deriving half is fixed (`AddDependency`'s
  `targetResourceVersion`, see CLAUDE.md, pinned by
  `TestDependencyRequeueRaceOnDeclare` and its out-of-band mirror).

  The guard produces an ordinary `wakeAfterCommit` requeue — bounded to once per
  edge by its edge-new ∧ target-moved conjunction — and the work queue does not
  outlive the process. A crash between the edge's commit and the dispatch leaves
  the edge durably in place, the dependent settled on the stale read, and nothing
  anywhere recording that a reconcile is owed — the same permanent, silent end state
  the race had, now reachable with a correct guard in place. It is not only a crash:
  `q.stopped` makes `add` a no-op, so a wake registered during shutdown goes the same
  way.

  What makes this specific to dependency wakes is that every other in-memory wake has
  a durable twin. A lost spec-write wake still leaves `generation >
  observed_generation`, so the resync tick re-derives it. Dependency staleness leaves
  no persisted trace at all.

  Today the default `StartupReconcileAll` hides it by sweeping everything on restart,
  so the exposure is confined to `StartupReconcileUnsettled` and
  `StartupReconcileNone` (`beehive.go:128` already names the latter as the strategy
  that breaks dependents relying on dependency events). That confinement is an
  accident of the default rather than a guarantee: recovering an owed wake is not a
  spec-convergence question, so it should not be a startup-*strategy* question
  either. Pinned, skipped, by `TestDependencyRequeueLostAcrossRestart`, which spells
  the crash as a stopped work queue — both writes commit and the wake lands on a
  queue whose `addLocked` returns early on `q.stopped`, indistinguishable from dying
  between commit and dispatch from the store's side, and deterministic without
  goroutine timing. It fails at the post-restart assertion 3/3, and passes if the
  restart is switched to `StartupReconcileAll`, which isolates the missing signal
  from the restart scaffolding itself.

  **Planned fix: a `pending_wake` token on `objects`.** Set inside `AddDependency`'s
  transaction under the same comparison that fires the wake — so the durable record
  and the edge commit together, with no window between them — and cleared by the pass
  that serves it. `INTEGER NOT NULL DEFAULT 0`, 0 meaning nothing owed, read by a
  partial index. It is a **CAS token, not a watermark**: the value is the target's
  `resource_version` only because a monotone token was free, and the clear is
  `WHERE id = ? AND pending_wake = ?` against the value read at load, so a flag
  re-set during a pass survives that pass's clear. Nothing ever compares it to
  another object's version. Enqueued at startup regardless of `startupReconcile` and
  on the resync tick, cleared only on a successful pass, skipping the write entirely
  when the loaded token is 0 — so an unflagged reconcile issues no statement.

  Note the scope this buys: it is a durable *wake*, not a derived backstop. It
  recovers only signals something explicitly raised, so a wake lost because
  `wakeDependents` swallowed its `ListIncomingRefs` error (see the item above) still
  heals never, and the "resync is the correctness backstop" claim stays false in
  general.

  Alternatives considered and rejected. A **per-object `observed_cursor` watermark**
  — stamp the global `resource_version` as of the reconciler's load, advance it on
  every successful pass, and enqueue any dependent whose target's version exceeds it
  — is the strictly stronger design, because it *derives* staleness instead of
  recording an intent and therefore heals any lost wake, including the three above.
  It costs a write on every successful pass where the token approach writes only when
  something is owed, and it over-flags (a target change during a pass costs an extra
  tick even when the read was late enough to have seen it). Keep it in mind if the
  waker's silent loss points are ever taken on; it subsumes `pending_wake`. Storing
  the cursor **per-edge on `refs`** looks cheaper still and is not: advanced only on
  re-assert, a controller that declares an edge once and never re-asserts freezes its
  cursor and the backstop query re-enqueues it every tick forever. *Documenting that
  controllers should requeue themselves* turns a silent race into documentation, and
  a controller doing it naively (`Requeue: true` unconditionally) spins.

- **`Create` accepts a `WithOwner` naming an already-deleting owner, stranding both
  rows when resync is off** — known, not fixed. The ownership mirror of the
  `AddDependency` race above: there the edge is declared after the *change*, here
  after the *cascade*. `insertObject` checks nothing about the owner's lifecycle, and
  `AddRef` only verifies both endpoints exist — never that the target is live — so a
  child created against an owner that is already deletion-pending, and whose
  `MarkOwnedForDeletion` pass has already run, is born live and unmarked under a
  finalizing owner. Its `owned_by` edge is an unconditional live claim in
  `HasIncomingRefs` (only deletion-pending `depends_on` sources are excluded), so the
  owner can never be physically collected.

  Nothing event-driven recovers it. `AddRef` bumps no `resource_version` and emits
  nothing, so no watcher fires; `wakeDependents` reads only `depends_on` and would
  ignore the edge regardless; the child's own `collect` returns at the
  `DeletionRequestedAt == nil` early-out because *it* is not finalizing; and the owner
  is re-woken by `collect`'s `toWake` referents only when a child row is physically
  *removed*, which this one never will be. Reproduced with `WithResyncInterval(0)`, an
  owner held alive by a finalizer, and the cascade provably complete (its first child
  already collected): the second child stays alive and unmarked indefinitely while the
  owner sits deletion-pending, 3/3 runs.

  **Unlike the `depends_on` race, this one self-heals whenever resync is on.**
  `sweepDeletionPending` and `enqueueDeletionPending` re-list the still-pending owner
  and `collect` re-runs `MarkOwnedForDeletion`, which is explicitly built to be re-run
  and picks the new child up; the exposure is one resync interval. The permanent
  strand is confined to `resyncInterval = 0` — which is exactly the configuration the
  GC tests treat as supported, and the one where every other GC path was deliberately
  made event-complete. It is also *visible* where the dependency race is not: the
  owner is observably stuck deletion-pending rather than silently settled on a stale
  read.

  **The fix looks cheap, but the behavior is the open question.** `insertObject`
  already runs inside the create transaction, so reading the owner's
  `DeletionRequestedAt` there is one indexed read paid once per child creation — not
  the once-per-reconcile-forever tax that sank `AddDependency`'s pre-read guard. What
  it should *do* is undecided. Rejecting with an error is the honest signal (the
  caller asked to attach to something being torn down) but adds a new failure mode to
  `Create` and races anyway — the owner can be deleted the instant after the check.
  Creating the child already-marked is self-consistent and needs no new error, but
  manufactures a deletion-pending object the caller never asked to delete, and its
  spec is then unreachable. A third option is to leave `Create` alone and make the
  *sweeper* the answer by having it run unconditionally at least once per some
  cadence even when resync is disabled — which is really the resync-strategy item
  above wearing a different hat.

  Whichever is chosen, the owner's `DeletionRequestedAt` is already on the row
  `AddRef` reads for its endpoint check — the same check that reports
  `TargetResourceVersion` for the dependency guard. Adding it to `AddRefResult`
  would let `insertObject` see the owner's lifecycle from the write it already
  makes, rather than a second read, exactly as `AddDependency` reads the target's
  version from that one call.

  Deferred because the window needs a finalizer-held owner *plus* disabled resync to
  become permanent, and because picking between reject / create-marked is a public API
  decision worth making alongside the resync-strategy item rather than ahead of it.
  Revisit if a controller is found that creates children against owners it does not
  itself hold a finalizer on, or when the resync strategy is settled. There is no test
  for it yet: the repro exists only as a throwaway, and `TestMarkOwnedForDeletionCascadesThenIsNoOp`
  re-cascades over a fixed child set, so it never adds a child between passes.

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
