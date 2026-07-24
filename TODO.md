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

- **A target change landing *mid-reconcile* is safe in memory, but has no durable
  twin** — analyzed, in-memory half verified sound, durable half not fixed. The
  scenario: D depends on T, T changes and wakes D, and T changes *again* while D's
  reconcile is still running. The question is whether the second change is lost to
  the pass that was already in flight when it arrived.

  **In memory it is not.** `workQueue` keeps `dirty` (queued) and `processing`
  (handed out, not yet `done`) as separate sets. `get` moves the id from `dirty` to
  `processing`; a wake arriving mid-pass hits `addLocked`, which sets `dirty[id]`
  but sees the id in `processing` and deliberately does *not* push to `items`; and
  `runWorker` calls `work.done(id)` unconditionally after `adapter.reconcile`
  returns — error or not — where `done` finds the dirty bit and makes the id
  dispatchable immediately. So the second change costs one extra pass, never a lost
  one. The publish side orders correctly too: the wake is registered post-commit
  (`wakeAfterCommit` → `flush`), so the requeue can never point at an uncommitted
  row. The benign variant — T changing after `get` but *before* D reads T — is
  simply a level-triggered no-op re-pass on a value D already saw.

  **What is missing is durability.** This wake is in-memory only. `pending_wake`
  covers the *declare* window (`AddRef` stamps when a new edge's caller-supplied
  target version is already stale) and nothing else, so an ordinary target-change
  wake has no persisted "reconcile owed". A crash between T's commit and D's
  re-dispatch loses it permanently: D settled at its own generation, so
  `ListUnsettledIDs` structurally cannot see it, and per the resync item above the
  tick is unsettled-only. The mid-reconcile timing makes the window *wider* than
  the plain case — the owed wake sits in `dirty` behind a whole reconcile pass
  rather than a queue hop — but the loss mode is the same one, not a new one.

  Deferred because the fix is not local to this timing. Stamping `pending_wake` in
  `wakeDependents` would make every target change a write per dependent on the
  wake path — the cost `pending_wake` avoids today by riding a write `AddRef` was
  already doing — and the decrement is per-pass, so a stamp-per-change would need
  the same count-not-flag reasoning re-derived against a much higher write volume.
  The cheaper repair is the `observed_cursor` watermark / full-sweep resync
  recorded under the two items above: it re-derives staleness instead of tracking
  each owed wake. Revisit with those; this item exists so the in-memory analysis
  is not redone, and so "the queue handles it" is not mistaken for "it survives a
  restart."

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

- **A client-only dependent's `pending_wake` count is never reclaimed** — known, not
  fixed. Edges are deliberately cross-kind, so `AddDependency(clientOnlyID, target,
  staleRV)` is legal and its stamp lands on a row whose kind has no reconcile loop.
  Nothing drains it (`DecrementPendingWake` is called only by a reconcile) and
  nothing scans it (`ListPendingWakeIDs` is per-kind, called only by that kind's
  reconciler), so the count and its `idx_objects_pending_wake` entry persist for the
  life of the row. Re-declaring an edge — `DeleteDependency` then `AddDependency`
  with a claim the target has already moved past — satisfies the edge-new half again
  and increments it again, so it is not even bounded by the distinct-target count.
  Earlier prose called this "inert"; it is *unread*, which is not the same thing.

  The impact is small and does not compound. Nothing reads the count, so there is no
  behavioural effect while the kind stays client-only; and if it later gains a
  controller, the first reconcile subtracts the whole observed count, so the accrued
  N costs **one** spurious pass, not N. What remains is durable storage and an index
  entry nobody harvests.

  **Gating the stamp on registration is the wrong fix**, and is why it wasn't taken.
  The stamp is SQL inside `AddRef` (it has to be, for the ordering guarantee the
  nested-`Within` contract forces), and the store cannot know which kinds are
  registered — so the caller would have to resolve `fromID`'s kind before every
  declare, which is exactly the per-call pre-read that sank the original wake guard,
  on a path level-triggered controllers re-run forever. It would also bake in a fact
  that changes between runs: a kind registered later would have lost its wake
  outright.

  The fix that fits is a **cross-kind sweeper**, the `pending_wake` analogue of the
  global GC sweeper's `ListAllDeletionPendingIDs`: list rows with a nonzero count
  across all kinds, zero the ones whose kind has no registered reconciler, on the
  sweeper's existing cadence. Off the hot path, symmetric with machinery that already
  exists, and it reclaims the index entry. Deferred because it is new store surface
  plus a sweep for an effect that is storage-only, and because the "kind gains a
  controller later" case argues for keeping the count — so whether to reclaim at all
  is a judgement call worth making deliberately. Revisit if a deployment is found
  that declares many edges from client-only kinds, where the index entries would
  actually be measurable.

- **`CreateObject` takes a `RawObject` and silently drops most of it** — known, not
  fixed, and the input-side twin of the item below. `RawObject` is a *read*-shaped
  DTO (it mirrors the full row, and is publicly aliased as `beehive.RawObject`), but
  it is also `CreateObject`'s parameter. The INSERT binds six caller fields — group,
  kind, slug, spec, schema_version_spec, finalizers — and ignores the other twelve:
  `ID`, `ResourceVersion`, `CreatedAt`, `UpdatedAt` (store-assigned), `Status` and
  `Generation` (hardcoded `NULL` and `1`), `StatusVersion`, `ObservedGeneration`,
  `ObservedAt`, `DeletionRequestedAt`, `Conditions`, and `PendingWake`. A caller
  passing any of them gets a row without it and no error.

  `Status` is the sharp one: seeding a status on create is a plausible thing to
  attempt, and it is discarded silently. The store-assigned fields are defensible
  (a caller cannot pick its own id or cursor), but nothing says so at the call site —
  the struct advertises eighteen settable fields and honours six.

  The fix is to stop reusing the read shape for the write: a `CreateObjectInput` (or
  functional options) carrying only the six fields create actually accepts, so the
  compiler rejects the rest instead of the store dropping them. Deferred because
  `RawObject` is an exported alias, so narrowing the parameter is a breaking change
  to an externally-implementable `Store`, and it wants doing alongside the return-shape
  item below rather than as a second separate break. Surfaced in review of the
  `pending_wake` work, where the new field simply joined the existing eleven; noted
  here so the next reader sees it is a pre-existing shape problem and not something
  that arrived with the durable wake.

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

- **A nested `Within` is not a rollback boundary, so no multi-write composition is
  atomic against a caller that handles its error** — known, not fixed, and the
  general form of a defect already fixed once in the specific.

  `sqliteStore.Within` returns `fn(ctx)` when it finds an ambient transaction: the
  nested call joins the outer one and opens nothing of its own. So an error
  returned from inside a nested `Within` unwinds *nothing*. Only the outermost
  caller propagating it to the real `Within` rolls anything back, and a caller that
  logs and carries on commits every write that already landed. Any method
  performing two writes is therefore atomic only by the grace of its callers:
  `ControllerClient.DeleteDependency` (the `DeleteRef` plus its GC re-check), any
  controller's own two-write `Within` block, and — until it was restructured —
  `AddDependency`.

  That last one is the worked example. Its durable wake stamp used to be a second
  store call sequenced after `AddRef`; a reviewer pointed out that a caller
  swallowing the stamp's error would commit the edge with neither the stamp nor the
  requeue, which is exactly the stranded-dependent race the edge's version claim
  exists to close. The fix was local and ordering-based: fold the stamp into
  `AddRef` ahead of the insert, so the only write that can fail after the edge
  exists is none. That is the same trick `ErrTargetResourceVersionFuture` already
  used, and it works precisely because there were two writes and one could be moved.
  It does not generalize — reorder anything whose second write depends on the first
  and you have nowhere to put it.

  The general fix is `SAVEPOINT`. The nested branch of `Within` would issue
  `SAVEPOINT`, `ROLLBACK TO` on an `fn` error and `RELEASE` on success, making every
  nested composition a real boundary and retiring the class — including the
  contortions above, which could then be written in whatever order reads best.

  Deferred because the cost is not in the SQL. The tx-scoped `eventCollector` is
  append-only and drains once at the outermost commit, so a savepoint rollback has
  to unwind it too — buffered emits *and* `AfterCommit` hooks truncated back to a
  watermark taken at `SAVEPOINT`, with the `flushed` latch's late-registration path
  thought through against it — or writes that were just rolled back still publish to
  watchers and still fire their wakes. And it changes the semantics of every nested
  `Within` in the codebase at once, on a store where the whole suite runs through
  this one function; anything today relying on "a nested error unwinds nothing"
  silently starts behaving differently, which needs an audit rather than an
  assumption. Two extra statements per nested `Within` on a single-connection store
  is the smaller cost.

  Revisit when a second instance of the class shows up that ordering *cannot* fix —
  that is the signal that the specific fixes have run out — or if the collector ever
  grows a watermark for another reason, which removes most of the work.


## Resolved

- **`idx_objects_deleting` made covering for its one remaining reader** — done.
  The index was `objects(deletion_requested_at) WHERE deletion_requested_at IS NOT
  NULL`, and its two readers both planned as `SEARCH … USING INDEX` plus
  `USE TEMP B-TREE FOR ORDER BY` — not covering (a row fetch per match) and
  sorting, because the key orders by `(deletion_requested_at, id)` rather than by
  `id`. The lost *covering* scan arrived with `ListAllDeletionPending`, which added
  `group`/`kind` to a `SELECT id` that had been index-only; the sort predates it.

  Deferred at the time because the reader set was mid-change, and that was the
  right call: retiring the reconciler's own deletion-pending backstop removed
  `ListDeletionPendingIDs`, leaving the cross-kind sweeper query as the sole
  reader — and the two candidates measured earlier traded against each other
  precisely on that second reader. With it gone, candidate B is unambiguous.

  `idx_objects_deleting` is rekeyed to `(id, "group", kind)`, same partial
  `WHERE`, folded into `0001_init.sql` — pre-release, the schema is edited in
  place rather than accreting migrations (`0001` has been amended six times), so
  a fresh database is the only supported upgrade path. The sweeper query now plans as a plain `SCAN … USING INDEX`:
  covering, and already in id order, so the sort disappears too. Re-probed with
  5000 rows / 50 deletion-pending after `ANALYZE`. The only other consumer of the
  index — the `from_id IN (SELECT id FROM objects WHERE deletion_requested_at IS
  NOT NULL)` semi-join in the finalizing-refs cleanup — still plans identically,
  since `id` leads the new key.

  The partial `WHERE` is what keeps the wider key cheap: only finalizing rows are
  indexed, and `id`/`group`/`kind` are write-once, so entries are inserted when a
  delete is requested and dropped when the row is collected, never updated between.

  Still unmeasured, and deliberately so: whether any of this is observable in
  wall-clock. The deletion-pending set is small by construction, so this was a
  correctness-of-claim and tidiness fix, not a bottleneck anyone had hit. The
  remaining question the earlier entry raised — whether `ORDER BY id` earns its
  keep at all, since the sweeper only iterates and routes — is now moot: with the
  new key the ordering is free, so it stays for test determinism at no cost.

- **`StartupReconcileNone` + `resyncInterval = 0` silently disabled all crash
  recovery for unsettled objects** — done, by **documenting the configuration and
  making it announce itself, not by taking it away**. The behaviour is unchanged:
  `enqueueUnsettled` still runs only inside `reconciler.run`'s strategy switch or on
  a resync tick, so with the startup pass off and no ticker, an object a prior
  process left unconverged (crashed mid-reconcile, or created with
  `observed_generation IS NULL`) is still not resumed by beehive. What changed is
  that it is now a *choice* the caller can make and be told about:
  `reconciler.run` logs a Warn at startup naming the configuration and the two
  primitives that make it usable, and the `StartupReconcileNone`,
  `WithResyncInterval` and `Client.Requeue` godocs carry the recipe.

  **Why honoring it won.** The entry originally read this as a contradiction between
  two knobs — and its own last line said "*the silence* is the defect", which turned
  out to be the accurate diagnosis. Reaching the cell takes two explicit non-default
  settings, and both primitives an embedder needs to drive convergence itself are
  already public: `Store.ListUnsettledIDs` (via the `type Store = storeapi.Store`
  alias, on the store the embedder opened and passed to `New`) reports exactly the
  objects owed a pass, and `Client.Requeue` dispatches one. So the configuration
  means "I reconcile on my own schedule", which is a real use — and the library
  overriding two deliberately-set knobs would take it away with nothing offered in
  its place. Pinned by `TestStartupReconcileNoneWithoutResyncDrivesNothing` (the
  opt-out is honored), `TestStartupReconcileNoneWithoutResyncWarns` (it is not
  silent), and `TestStartupReconcileNoneSelfDrivenRecovery`, which runs the
  documented recipe end to end so the escape hatch can't quietly stop working.

  **The alternative was built, then reverted** — recorded so it is not rebuilt.
  Hoisting `enqueueUnsettled` out of the strategy switch (next to
  `enqueueDeletionPending`/`enqueuePendingWake`) makes recovery unconditional and
  reduces the strategy to "do you *also* sweep settled objects". It passes the whole
  suite. Two things sank it. It deletes the only way to express "drive nothing
  automatically" — `StartupReconcileNone` and `StartupReconcileUnsettled` become
  behaviourally identical, and no coherent third meaning exists once owed work is
  unconditional. And its justification assumed the caller had no way to recover the
  objects themselves, which is false. The supporting argument that unsettled work is
  "owed, like a deletion-pending row" is still true as far as it goes — it is why
  those two *stay* unconditional even under `None` (a half-deleted row
  RESTRICT-blocks its owner; an owed wake was explicitly stamped; neither is
  something an embedder driving specs by hand would know to chase) — but it does not
  extend to spec convergence, which is precisely what the strategy exists to let the
  caller own.

  Also examined: **rejecting the combination** at `Register`/`Start`. Rejected on
  evidence — it is the repo's own technique for isolating one backstop under test
  (`TestIntegrationGCResumesDanglingDeleteOnStartup`,
  `TestDependencyRequeueLostAcrossRestart`), so banning it would make those tests
  unwritable in the form that gives them meaning; and it forces a periodic
  full-table sweep on the caller who explicitly asked for no timers.

  One test change survives from the reverted attempt, kept on its own merits:
  `TestIntegrationGCResumesDanglingDeleteOnStartup` built its row with a raw
  `CreateObject` (`observed_generation` NULL) and now settles it first, so
  `enqueueDeletionPending` being the only path that can reach the row is explicit
  rather than incidental. Deletion does not undo it — `markForDeletion` leaves
  `generation` alone.

  Note what this does *not* resolve. The failure mode still exists for that one
  configuration, now by design and with a warning attached; and the neighbouring
  items above are untouched — the resync tick is still unsettled-only, so a settled
  dependent whose generation never moves is reachable only by a dependency wake, and
  a wake lost to one of the waker's three silent drops still heals never.

- **`reconciler.enqueueFrom` logs its list error** — done. It was silent, on the
  reasoning that a failed resync list skips one tick and the next retries, so it
  self-heals on cadence. `enqueuePendingWake` broke that premise: at
  `resyncInterval = 0` its startup call is the *only* invocation, so a failed
  `ListPendingWakeIDs` defers every recorded owed wake to the next process start —
  the one backstop whose entire purpose is not losing them, failing indistinguishably
  from "nothing was owed". It now warns, and takes a `source` naming which backstop
  lost its pass, since the cost differs sharply between them. Surfaced in review of
  the `pending_wake` work, which is exactly the "when one of the items above is
  touched" trigger the deferred entry named. `reconciler.log()` was added alongside
  it (the nil-safe accessor `typedController` already had), because the enqueue
  helpers are reachable on reconcilers built outside `Register`.

- **Durable twin for the in-memory dependency wake (`pending_wake`)** — done.
  `AddDependency`'s guard (see CLAUDE.md) produced an ordinary `wakeAfterCommit`
  requeue, and the work queue does not outlive the process: a crash between the
  edge's commit and the dispatch left the edge in place, the dependent settled on a
  stale read, and *nothing* recording that a reconcile was owed — the same permanent,
  silent end state the race had, now reachable with a correct guard. Unlike every
  other in-memory wake, dependency staleness left no persisted trace (a lost
  spec-write wake still leaves `generation > observed_generation` for the resync tick
  to re-derive).

  The fix increments `objects.pending_wake` under the same edge-new ∧ target-moved
  conjunction that fires the wake — so the durable record and the edge commit
  together. It lives *inside* `AddRef` and *before* the insert, reporting through
  `AddRefResult.WakeStamped`; as a second store call after `AddRef` returned it was
  not actually indivisible from the edge, since a nested `Within` unwinds nothing
  (see "A nested `Within` is not a rollback boundary" above, which records both the
  reviewer's finding and why the general fix was not taken). The stamp is
  deliberately *not* gated on `fromID`'s kind being registered: an ungated stamp on
  a client-only kind is an unread count nothing scans (see the sweeper item above), where a gated one loses the wake
  outright if that kind ever gains a controller — and gating would have meant
  running a beehive predicate inside the write transaction and deciding
  registration in two places, when `enqueueIfRegistered` already decides it once.
  `typedController.reconcile` subtracts, on a successful pass, **the count it
  loaded** (`DecrementPendingWake(id, observed)`, floored at 0), skipping the write
  when that count is 0. `pending_wake` is a **count of outstanding wakes, not a
  single token** — that is what survives a wake owed *while an earlier one is being
  reconciled*: it lands above `observed` and so survives the subtraction, where a
  token compared to the loaded value (an earlier design used the target's rv) would
  have been dropped when two wakes for the same unchanged target shared a value, then
  lost to a crash (surfaced in review, pinned by
  `TestReconcilePendingWakeSurvivesConcurrentWake`). Subtracting `observed` rather
  than 1 is the second half, also from review: one pass reads the target's *current*
  state, addressing every wake outstanding when it began, and the backstop enqueues a
  row only once (the work queue coalesces), so a `-1` would strand the remainder with
  nothing to re-enqueue it — indefinitely at `resyncInterval=0`, one per tick
  otherwise (`TestReconcileDrainsMultiplePendingWakes`). No follow-up requeue is
  scheduled: a residual exists only when an increment landed mid-pass, and that
  increment carried its own in-memory requeue. A failed subtraction is logged and
  left for the backstop rather than requeued, which would spin against a store that
  keeps failing. The
  backstop `ListPendingWakeIDs` rides the partial index
  `idx_objects_pending_wake WHERE pending_wake != 0` — a sibling of
  `ListDeletionPendingIDs`, not folded into `ListUnsettledIDs` (an owed wake is
  orthogonal to spec convergence), enqueued unconditionally at startup and each
  resync tick so `StartupReconcileNone` + `resyncInterval=0` still recovers.
  `TestDependencyRequeueLostAcrossRestart` now passes unskipped (restart under
  `StartupReconcileNone` + `resyncInterval=0`, 3/3).

  Two things it deliberately does *not* do. It is a durable *wake*, not a derived
  backstop: it recovers only signals something explicitly raised, so a wake lost
  because `wakeDependents` swallowed its `ListIncomingRefs` error (see "Three silent
  loss points" above) still heals never, and "resync is the correctness backstop"
  stays false in general. And it shares spec-convergence's coverage everywhere *but*
  the unconditional startup/tick enqueue — which is why it works under
  `StartupReconcileNone` + `resyncInterval=0` where plain unsettled recovery does not
  (see "`StartupReconcileNone` + `resyncInterval = 0` silently disabled all crash
  recovery for unsettled objects" below, where honoring that opt-out for *spec*
  convergence is the decision, and this stamp is one of the two signals deliberately
  exempted from it); a wake owed is a specific known-owed reconcile, like a pending
  deletion, so unconditional resumption is justified where a broad unsettled sweep
  would not be.

  The **per-object `observed_cursor` watermark** stays the recorded stronger
  alternative: stamp the global `resource_version` as of the reconciler's load,
  advance it on every successful pass, and enqueue any dependent whose target's
  version exceeds it. Because it *derives* staleness rather than recording an intent,
  it heals any lost wake — including the three silent loss points — and subsumes
  `pending_wake`. It costs a write on every successful pass (vs only when something is
  owed) and over-flags (a target change during a pass costs an extra tick even when
  the read was late enough), so it was not taken now; revisit it if the waker's silent
  loss points are addressed. Storing the cursor **per-edge on `refs`** was rejected
  outright: advanced only on re-assert, a controller that declares an edge once and
  never re-asserts freezes its cursor and the backstop re-enqueues it every tick
  forever.

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
