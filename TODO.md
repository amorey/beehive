# TODO

Deferred work, with the reasoning that led to deferring it. An item earns a place
here when it is a real defect or gap that we chose *not* to fix yet — not a
wishlist. Each one records what would make it worth doing, so the next reader can
tell "we decided against this for now" from "nobody thought of it."

- **A dependency cycle of length ≥ 2 spins the waker at full speed** — known, not
  fixed; the self-edge half *is* fixed. `dependentsWake` skips `from_id == to_id`
  (see the guard's comment), so a self-dependency no longer re-enqueues itself.
  Two objects that depend on each other still do: A's emitted write wakes B, B's
  wakes A, with no rate limiter in `workQueue.addLocked` and no already-settled
  skip on the dispatch path. Any write family the store emits sustains it —
  changing status bytes, a byte-identical `UpdateStatus` at a generation the
  object hasn't settled at, any non-no-op condition write (`bumpObjectAndEmit`),
  `FinalizersDelete` — so byte-stable status is not a defence. `EventsRecord` is the
  exception: it bumps no object `resource_version`.

  It costs more than background CPU. The store runs on a single connection, so
  the spin's write transactions serialize every other writer in the process —
  client writes, other kinds' reconciles, the GC sweeper, event retention — while
  it also holds a reconcile worker slot, burns the global `resource_version`
  counter, and drives every change watcher and `SchedulesWatch` subscriber at
  reconcile speed. All of it on a store where every object is converged and every
  generation matches, so no convergence signal reports anything wrong.

  Two candidate fixes. **Reachability at declare time** (reject an edge that
  closes a cycle in `DependenciesAdd`) is a recursive CTE on the single connection,
  and the declare path is one pre-read away from the performance problem that sank
  the earlier raced-declare guard — a reachability probe is strictly more
  expensive than the read that was rejected. **A per-item minimum re-enqueue
  interval on the work queue** — what controller-runtime does for this class —
  bounds every cycle length, needs no reachability query, and costs nothing on the
  hot path; it does not make a cycle converge, but turns "full speed forever" into
  "one pass per interval forever", which removes the contention. It is **not** a
  reuse of `addAfter`, whose newest-wins alarm would push the item out on every
  fresh wake and starve it; it wants an oldest-wins watermark on `addLocked`, the
  path every wake actually takes. Cost: that watermark, a new unanchored constant,
  and an interaction with `Result.RequeueAfter` and backoff worked out. Evaluate
  the rate limiter first.

  Deferred on **fix cost**, not on likelihood: the self-edge is one comparison on
  values the loop already holds, while the general case is either a recursive CTE
  on the declare path or a new work-queue primitive. Likelihood points the other
  way and this entry should not pretend otherwise — a self-edge requires naming
  your own id, whereas a mutual dependency is what two independently-written
  controllers fall into with neither author seeing both halves, which is why
  `EdgesDeleteFinalizingDependsOn` exists at all. Whether beehive should support
  cycles is also unsettled, which is a reason not to guard hastily.

  **Each candidate has its own tripwire, because no one test constrains both.**
  For reachability, `TestAddDependencyAcceptsCycle` asserts that a cycle-closing
  edge and a self-edge are both accepted today — the exact fact that fix would
  change. For the rate limiter, the tripwires already exist in
  `workqueue_test.go`: `TestWorkQueueNoConcurrentDispatch` and
  `TestWorkQueueReaddAfterDone` both assert that the *second* dispatch of one id
  is immediately available, which is precisely the latency a minimum interval
  renegotiates. (`TestWorkQueueFIFO` and the dedup/ready tests are *not*
  tripwires — they add distinct ids once each, and a first add stays immediately
  dispatchable under any sane throttle.) `TestWakeDependentsTwoCycle` pins the
  waker's both-directions behaviour but is not the record either: it drives edges
  through a fake, so declare-time rejection never reaches it, and its wakes are
  all first wakes.

- **`DeleteBySlug` on an absent slug costs a write transaction** — known, not fixed.
  `DeletionRequestsCreateBySlug` opens `Within` (so `BEGIN IMMEDIATE`) and its first act
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
  if `DeletionRequestsCreate` ever gets the same treatment (its absent path has always
  had this shape, so the probe would belong in `requestDeletion` for both).

- **A crash during a waker outage strands a settled dependent, and no in-memory
  recovery can fix it** — analyzed, not fixed, and deliberately scoped out of the
  watermark recovery that shipped (see
  [the ADR](docs/adr/2026-07-27-waker-watermark-replay.md)). The gap predates that
  work rather than being introduced by it: the escalation flags it replaced
  (`resyncOnce`/`resyncAlways`) were in-memory too, so a restart lost the armed repair
  exactly as it now loses a watermark. The in-process half is closed; this is the half
  left.

  The scenario: D depends on T, the waker loses T's change (any of the three loss
  points), and the process dies before the repair runs. On restart nothing replays
  it. `enqueueCatchup` runs unconditionally at startup, but it lists unsettled
  objects plus `reconcile_owed` stamps, and a settled dependent has neither — its own
  generation never moved and nothing stamped it. `enqueueAll` is what would catch it,
  and that is gated on `startupResync`, which defaults to `true`. **So the exposure
  is `WithStartupResync(false)` plus a crash mid-outage** — narrow, but it is the same
  configuration in which the in-process gap is most acute, so a reader who sees that
  gap fixed will reasonably assume this one is too.

  **Persisting the watermark is not the fix**, which is the analysis worth keeping. A
  watermark records *delivery* ("the waker reached rv N"); what has to survive a
  restart is *convergence* ("D actually reconciled against T's new state"). Those
  come apart in the ordinary case: if the waker requeues D, advances past T, and the
  process dies before D's reconcile runs, a persisted cursor is already past T and D
  is stranded anyway. Persisting buys half the hole, leaves the twin half open, and
  charges a write per consumed batch on a single connection.

  Nor is the cheap durable trick available: you cannot stamp `reconcile_owed` on the
  dependent at failure time, because the lookup that failed is the one that would
  have named the dependents. Recorded intent needs to know who to record against, and
  that is exactly the information the failure destroyed.

  **The fix is `observed_cursor`** (recorded under the pending-wake backstop item
  below) — per-object, so durability attaches to the thing that must converge, and
  derived rather than recorded, so it needs no knowledge of who was missed. Not fixed
  now because it is a schema-and-write-path change well past the scope of the
  in-process repair, and because the exposure needs an opt-out knob *and* a crash.
  Revisit with `observed_cursor`; until then, `WithStartupResync(false)` should
  document that it opts out of crash recovery for settled dependents.

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

  **What is missing is durability.** This wake is in-memory only. `reconcile_owed`
  covers the *declare* window (`EdgesAdd` stamps when a new edge's caller-supplied
  target version is already stale) and nothing else, so an ordinary target-change
  wake has no persisted "reconcile owed". A crash between T's commit and D's
  re-dispatch loses it: D settled at its own generation, so no owed-work listing
  can see it. The mid-reconcile timing makes the window *wider* than the plain
  case — the owed wake sits in `dirty` behind a whole reconcile pass rather than a
  queue hop — but the loss mode is the same one, not a new one.

  **Under stock defaults it heals at the next process start, and that is the point
  most likely to be misread.** The loss is permanent for the *current* process: the
  catchup tick sees only what the store records as owed, and the waker's own replay
  cannot reach it either, since nothing *observed* the loss — the wake was delivered,
  so the watermark legitimately advanced past T. But the crash that loses the wake
  also ends the process, and the next one starts with `WithStartupResync` true (the
  default), re-dispatching every object: D reconciles, reads T's current state,
  converges. So the durable gap is not "a stranded dependent forever" but "a
  stranded dependent until restart".

  One configuration removes that backstop, and it is the one that actually wants
  `WithResyncInterval` set: `WithStartupResync(false)`, where the restart no longer
  sweeps. (Disabling every ticker no longer matters here — the waker recovers its own
  losses now — but this loss is not one the waker can see.) This is also why it is not
  an argument for making the full resync a default — the startup pass already
  covers the crash case, so a periodic full pass would pay forever for a residual
  the restart path already handles.

  Deferred as a *durable* fix because it is not local to this timing. Stamping
  `reconcile_owed` in `dependentsWake` would make every target change a write per
  dependent on the wake path — the cost `reconcile_owed` avoids today by riding a
  write `EdgesAdd` was already doing — and the decrement is per-pass, so a
  stamp-per-change would need the same count-not-flag reasoning re-derived against
  a much higher write volume. The `observed_cursor` watermark stays the recorded
  alternative that would make it durable under *any* configuration: it re-derives
  staleness instead of tracking each owed wake. This item exists so the in-memory
  analysis is not redone, and so "the queue handles it" is not mistaken for "it
  survives a restart."

- **`Create` accepts a `WithOwner` naming an already-deleting owner, stranding both
  rows when resync is off** — known, not fixed. The ownership mirror of the
  `DependenciesAdd` race above: there the edge is declared after the *change*, here
  after the *cascade*. `insertObject` checks nothing about the owner's lifecycle, and
  `EdgesAdd` only verifies both endpoints exist — never that the target is live — so a
  child created against an owner that is already deletion-pending, and whose
  `DeletionRequestsCreateFromOwner` pass has already run, is born live and unmarked under a
  finalizing owner. Its `owned_by` edge is an unconditional live claim in
  `EdgesHasIncoming` (only deletion-pending `depends_on` sources are excluded), so the
  owner can never be physically collected.

  Nothing event-driven recovers it. `EdgesAdd` bumps no `resource_version` and emits
  nothing, so no watcher fires; `dependentsWake` reads only `depends_on` and would
  ignore the edge regardless; the child's own `collect` returns at the
  `DeletionRequestedAt == nil` early-out because *it* is not finalizing; and the owner
  is re-woken by `collect`'s `toWake` referents only when a child row is physically
  *removed*, which this one never will be. Reproduced with `WithResyncInterval(0)`, an
  owner held alive by a finalizer, and the cascade provably complete (its first child
  already collected): the second child stays alive and unmarked indefinitely while the
  owner sits deletion-pending, 3/3 runs.

  **Unlike the `depends_on` race, this one self-heals whenever the GC sweeper runs.**
  `sweepDeletionPending` re-lists the still-pending owner and `collect` re-runs
  `DeletionRequestsCreateFromOwner`, which is explicitly built to be re-run and picks the new
  child up; the exposure is one GC interval, always — **there is no longer a
  permanent-strand configuration at all.** It used to follow from `resyncInterval =
  0`, a documented and commonly-used way to say "event-driven only"; splitting the
  intervals confined it to `WithGCInterval(0)`, and rejecting a non-positive GC
  interval (see Resolved) removed even that. (The repro above predates the split and
  was written against `WithResyncInterval(0)`; it now needs a GC interval long enough
  to observe the window rather than a disabled one.) It is also *visible* where the dependency race is not: the
  owner is observably stuck deletion-pending rather than silently settled on a stale
  read.

  **The fix looks cheap, but the behavior is the open question.** `insertObject`
  already runs inside the create transaction, so reading the owner's
  `DeletionRequestedAt` there is one indexed read paid once per child creation — not
  the once-per-reconcile-forever tax that sank `DependenciesAdd`'s pre-read guard. What
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
  `EdgesAdd` reads for its endpoint check — the same check that reports
  `TargetResourceVersion` for the dependency guard. Adding it to `EdgesAddResult`
  would let `insertObject` see the owner's lifecycle from the write it already
  makes, rather than a second read, exactly as `DependenciesAdd` reads the target's
  version from that one call.

  Deferred because the window needs a finalizer-held owner *plus* disabled resync to
  become permanent, and because picking between reject / create-marked is a public API
  decision worth making alongside the resync-strategy item rather than ahead of it.
  Revisit if a controller is found that creates children against owners it does not
  itself hold a finalizer on, or when the resync strategy is settled. There is no test
  for it yet: the repro exists only as a throwaway, and `TestMarkOwnedForDeletionCascadesThenIsNoOp`
  re-cascades over a fixed child set, so it never adds a child between passes.

- **A client-only dependent's `reconcile_owed` count is never reclaimed** — known, not
  fixed. Edges are deliberately cross-kind, so `DependenciesAdd(clientOnlyID, target,
  staleRV)` is legal and its stamp lands on a row whose kind has no reconcile loop.
  Nothing drains it (`ReconcileOwedDecrement` is called only by a reconcile) and
  nothing scans it (`ReconcileOwedListIDs` is per-kind, called only by that kind's
  reconciler), so the count and its `idx_objects_reconcile_owed` entry persist for the
  life of the row. Re-declaring an edge — `DependenciesDelete` then `DependenciesAdd`
  with a claim the target has already moved past — satisfies the edge-new half again
  and increments it again, so it is not even bounded by the distinct-target count.
  Earlier prose called this "inert"; it is *unread*, which is not the same thing.

  The impact is small and does not compound. Nothing reads the count, so there is no
  behavioural effect while the kind stays client-only; and if it later gains a
  controller, the first reconcile subtracts the whole observed count, so the accrued
  N costs **one** spurious pass, not N. What remains is durable storage and an index
  entry nobody harvests.

  **Gating the stamp on registration is the wrong fix**, and is why it wasn't taken.
  The stamp is SQL inside `EdgesAdd` (it has to be, for the ordering guarantee the
  nested-`Within` contract forces), and the store cannot know which kinds are
  registered — so the caller would have to resolve `fromID`'s kind before every
  declare, which is exactly the per-call pre-read that sank the original wake guard,
  on a path level-triggered controllers re-run forever. It would also bake in a fact
  that changes between runs: a kind registered later would have lost its wake
  outright.

  The fix that fits is a **cross-kind sweeper**, the `reconcile_owed` analogue of the
  global GC sweeper's `DeletionRequestsList`: list rows with a nonzero count
  across all kinds, zero the ones whose kind has no registered reconciler, on the
  sweeper's existing cadence. Off the hot path, symmetric with machinery that already
  exists, and it reclaims the index entry. Deferred because it is new store surface
  plus a sweep for an effect that is storage-only, and because the "kind gains a
  controller later" case argues for keeping the count — so whether to reclaim at all
  is a judgement call worth making deliberately. Revisit if a deployment is found
  that declares many edges from client-only kinds, where the index entries would
  actually be measurable.

- **`ReconcileOwedDecrement` is not kind-scoped** — known, not fixed. Its UPDATE is keyed
  `WHERE id = ?` with no group/kind in the predicate, so it will decrement any row in
  `objects` whose id it is handed, of any kind. Every other id-keyed mutator in the
  store is scoped to a `GroupKind` and rejects a foreign id with `ErrWrongKind` —
  either in the `WHERE` or via the scoped re-read — and this is the sole exception.

  It is safe today for a narrow reason: the only caller is the reconciler
  (`reconciler.go`), which passes the id of a row it loaded one line earlier for its
  own kind, along with the count it read off that same row. No path reaches it with a
  foreign id, and the `max(… , 0)` floor means even a mistaken call cannot corrupt the
  count into an invalid state — it would only clear a wake another kind was owed.

  So the guard is caller discipline where the rest of the store has a structural one.
  The fix is small — add `AND "group" = ? AND kind = ?` and thread a `GroupKind`
  through — but it changes a `storeapi.Store` signature and wants a test pinning the
  foreign-id rejection, which is more than the two-line diff it looks like. Deferred
  because there is no reachable defect to fix, only an invariant to move from
  convention into the schema. Revisit when a second caller appears — a cross-kind
  sweeper (the item above) would be exactly that, and would be reaching for rows of
  kinds it does not own, which is the case the scoping exists to catch.

  Note the pending `reconcile_owed` rename (`specs/1-reconcile-owed-rename.md`)
  renames this method to `ReconcileOwedDecrement` without touching the predicate.

- **`ObjectsCreate` takes a `RawObject` and silently drops most of it** — known, not
  fixed, and the input-side twin of the item below. `RawObject` is a *read*-shaped
  DTO (it mirrors the full row, and is publicly aliased as `beehive.RawObject`), but
  it is also `ObjectsCreate`'s parameter. The INSERT binds six caller fields — group,
  kind, slug, spec, schema_version_spec, finalizers — and ignores the other twelve:
  `ID`, `ResourceVersion`, `CreatedAt`, `UpdatedAt` (store-assigned), `Status` and
  `Generation` (hardcoded `NULL` and `1`), `StatusVersion`, `ObservedGeneration`,
  `ObservedAt`, `DeletionRequestedAt`, `Conditions`, and `ReconcileOwed`. A caller
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
  `reconcile_owed` work, where the new field simply joined the existing eleven; noted
  here so the next reader sees it is a pre-existing shape problem and not something
  that arrived with the durable wake.

- **Mutators materialize a `RawObject` no caller reads** — known, not fixed, and
  the general form of the point the `DeleteBySlug` item above records for one
  method. Every mutator returns a full `*RawObject` with conditions assembled:
  `scanAndEmit` calls `attachConditions` on the write path, and the branches that
  emit nothing (`ObjectsUpdateSpec`'s content no-op, `UpdateStatus`'s settled no-op,
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
  anyway — the v0.17.0 `DeletionRequestsCreateBySlug` change was exactly such a moment and
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
  `ControllerClient.DependenciesDelete` (the `EdgesDelete` plus its GC re-check), any
  controller's own two-write `Within` block, and — until it was restructured —
  `DependenciesAdd`.

  That last one is the worked example. Its durable wake stamp used to be a second
  store call sequenced after `EdgesAdd`; a reviewer pointed out that a caller
  swallowing the stamp's error would commit the edge with neither the stamp nor the
  requeue, which is exactly the stranded-dependent race the edge's version claim
  exists to close. The fix was local and ordering-based: fold the stamp into
  `EdgesAdd` ahead of the insert, so the only write that can fail after the edge
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


- **The store keeps two hubs carrying the same changes** — known, not fixed.
  `hubs[gk]` (keyed `ObjectID`, carrying `RawObjectChange`) and `changeHub` (keyed
  `ObjectID`, carrying the projected `pendingChange`) are fed by the same `publish`,
  so every object write pays two `Send`s and `Close` has one more hub to tear
  down. One hub keyed `struct{GroupKind; ObjectID}` would serve all three
  consumers — `ObjectsWatchList(gk)` filtering on the kind half, `Watch(gk, id)` on the
  whole key (it already filters by id alone, since ids are globally unique), and
  `ObjectWritesSubscribe` not filtering at all — with conflation granularity unchanged.
  Deferred because the value types have deliberately diverged: the store-wide
  stream must not carry `*RawObject` (it would pin blobs), so a single hub means
  either giving the snapshot watchers the projection and re-reading rows, or
  giving the waker the blobs back. Worth revisiting if a third consumer appears,
  or if `conflate` grows a value-projecting receiver.

## Resolved

- **Dependency targets of client-only kinds got no waker at all** — done.
  `Start` subscribed one waker per *registered* kind, but a `depends_on` edge may
  point at an object of any kind, including one used through `Client` with no
  `Register`. Changes to such a target reached no waker: not a dropped wake, none
  attempted, so nothing in healthy operation repaired it, and only
  `WithStartupResync` covered it — at the *next process start*. The sharper case
  had no cover at all: `edges.to_id` is `ON DELETE RESTRICT`, so deleting such a
  target only sets a tombstone, and with no dependent woken to drop the edge the
  row stays deletion-pending while the GC sweeper retries it every tick, forever.

  The fix is one store-wide stream: `Store.ObjectWritesSubscribe(ctx)` replaces the
  per-kind `WatchChanges(gk)` (which had no other caller), and `Start` runs a
  single waker over it. It is the only option that cannot go stale — any per-kind
  subscription list has to be computed from something (the registered set, the
  kinds present at `Start`, the kinds currently referenced by an edge), and each
  of those misses a case. Routing needed no change: dependents were always
  enqueued by their own kind through `enqueueIfRegistered`.

  Two costs came with it and are paid inside the same change. The stream carries
  `ObjectWrite{ID, Type}` rather than `RawObjectChange`, because it sees every write in
  the process and an undelivered `*RawObject` pins that row's blobs; and its
  feeder drains the receiver with `TryRecv`, so a burst costs one
  `EdgesGroupIncomingByID` rather than one `EdgesListIncoming` per change — the
  store is single-connection, so the waker's reads serialize against every
  writer. Draining at the receiver rather than through a buffered channel is what
  keeps conflation alive to the handoff.

  What it does *not* fix, and what it costs. Both waker failure branches are now
  process-wide: one lost subscription or one ended stream kills dependency wakes
  for every kind. Two `Beehive`s on one store each observe the other's kinds —
  filtered correctly, but paid for in edges queries. And **the single waker
  goroutine is a process-wide head-of-line block**: K independent wakers became
  one, so a slow `EdgesGroupIncomingByID` — which queues behind writers on the
  single connection — now delays wakes for *every* kind, where before it delayed
  only its own. Batching was the agreed mitigation and it bounds throughput
  (O(bursts) queries, not O(changes)), but it does not bound *latency*: a batch
  still waits for the query ahead of it. Accepted deliberately — the alternative
  is per-kind wakers, which is the defect. If it ever bites, the shape is a small
  pool of drain goroutines over the one subscription, partitioned by target id so
  a kind's wakes stay ordered; unbuilt, and not worth the concurrency until a
  workload shows the stall. Full rationale in
  `docs/adr/2026-07-27-store-wide-dependency-change-stream.md`.

- **GC can no longer be disabled, which deleted two strand bugs instead of patching
  them** — done. `WithGCInterval(d <= 0)` now returns `ErrInvalidOption` (a new
  sentinel: an option whose *value* is meaningless, checked before the target switch,
  where `ErrConflictingOption` is about the value contradicting an argument the call
  already carries).

  **Why GC is the one interval that cannot be off.** The reconcile knobs accept 0
  because the operator keeps a way through — `Client.Requeue` drives a pass by hand.
  Nothing on the public surface triggers `collect`, and the sweeper is also the only
  *cross-kind* driver, so a sweeper-less `Beehive` accumulates deletion-pending rows
  with no recourse at all, each one's `owned_by` edge RESTRICT-blocking its owner's
  delete. The old answer was a Warn at startup saying exactly that, which is a log
  line reporting a configuration the library should not have accepted.

  **Two open strands closed with the branch.** Review flagged that
  `sweepDeletionPending`'s swallowed `DeletionRequestsList` error left *no* startup
  driver for finalizing rows once the per-kind `enqueueDeletionPending` was dropped —
  true only with GC disabled, where the startup pass was the process's single
  attempt. With a cadence guaranteed, "retry next sweep" is a true statement and a
  transient failure costs one interval of latency. That also removed
  `advanceGCNow`'s synchronous-collect arm, and with it the entry that used to sit
  above about that collect inheriting the caller's cancellation (a `Delete` whose
  caller cancels right after commit abandoning a cascade nothing retries). Both were
  the same defect wearing two hats: work whose only retry was a pass that ran once.

  **What went away:** the `<-ctx.Done()` park and its Warn in `gcSweeperRun`;
  `advanceGCNow`'s second arm (it is now just `enqueueIfRegistered`, so it needs no
  `ctx`); `advanceDeletion`'s second caller — the routing is still one function, now
  reached only by the sweeper, so every `collect` runs on the sweeper's goroutine
  rather than a caller's; and three tests whose premise was the disabled mode.
  `gcSweeperRun` keeps a non-positive guard that returns immediately, unreachable
  through `New` and documented as such: it exists so a `Beehive` assembled
  field-by-field has no sweeper rather than panicking in `NewTicker`, which is what
  the `withoutGCSweeper()` test helper uses.

  **Cost:** a breaking change for anyone passing `WithGCInterval(0)` — the only
  callers affected, since the default is 30s. A long interval expresses "collect
  rarely"; there is deliberately no way to express "never".

- **The waker recovers its own losses from a `resource_version` watermark** — done,
  replacing the tick escalation outright. The escalation could not run at the
  configuration that needed it most: it set `resyncOnce`/`resyncAlways` on every
  reconciler, those flags were read only inside the catchup ticker's case, and with
  `catchupInterval <= 0` and `resyncInterval <= 0` — the latter already the default —
  they were set and never read. `hasPeriodicPass` detected exactly that and reported it
  to the operator without fixing it, while a settled dependent stayed stale forever
  because its own generation never moved.

  The waker now holds the highest `resource_version` it has processed and, on a failed
  lookup or a re-established subscription, replays everything above it through the same
  wake path — O(changes missed) rather than O(table). Three store changes carried it:
  `ObjectWrite.ResourceVersion`, `ObjectWritesListSince` (blob-free, kind-agnostic,
  paged), and `ObjectWritesSubscribe` returning its starting cursor so the
  subscribe-then-read ordering is unrepresentable rather than documented.

  **The part worth remembering is that a cursor is not a trigger.** Nothing survived the
  loss points — a failed subscribe returned, a closed stream ended the loop — so the
  watermark needed a driver, and the waker got its own resubscribe loop with a backoff
  seam. That does not remove the tick dependency so much as move the timer from the
  reconciler into the waker, which is the trade: one goroutine, bounded work, and no
  knob whose default disables a correctness repair. → [ADR](docs/adr/2026-07-27-waker-watermark-replay.md)

- **Three periodic drivers instead of one, and all three dependency-waker loss
  points repaired** — done, as one series. `resyncInterval` had been governing four
  unrelated jobs: the per-kind reconcile tick, the global GC sweeper, event-log
  retention, and `gcAdvance`'s sync-vs-defer routing. Tuning the first moved the
  other three, and setting it to 0 — a documented, supported way to say
  "event-driven only" — silently disabled GC as well. Separately the tick was
  owed-work-only, so "periodic resync is the correctness backstop" was false for
  the case that mattered most: a *settled* dependent, reachable only by a
  dependency wake, of which the waker dropped three kinds silently.

  **The knobs.** `WithCatchupInterval` (30s, per-kind) drains what the store
  records as owed — `ObjectsListUnsettledIDs` + `ReconcileOwedListIDs`, bounded by what is
  outstanding. `WithResyncInterval` (**0, off**, per-kind) re-dispatches every
  object; opt-in because it scales with object count and the startup pass covers
  the same ground once per process. Its *meaning changed* while keeping its name,
  the one break that is not a compile error: an unchanged call now buys a full pass
  at that cadence. `WithGCInterval` (30s, global) paces the sweeper.
  `WithStartupResync` (true) replaces the `StartupReconcileStrategy` enum.

  **Startup stopped being a strategy question.** `enqueueCatchup` runs
  unconditionally and `WithStartupResync` only adds the full pass — which closes
  the old `StartupReconcileNone` + `resync = 0` recovery hole structurally rather
  than warning about it, and reverses the decision recorded below (see that entry).

  **GC routes rather than collects.** `ListAllDeletionPendingIDs` became
  `DeletionRequestsList`, returning `[]ObjectRef`, because the sweeper needs each
  row's kind: `collect` cannot clear a finalizer (it cascades, then returns while
  any remain), so a registered kind must be *enqueued* for its controller and only
  a client-only kind collected directly. That let `enqueueDeletionPending` and
  `Store.ListDeletionPendingIDs` go entirely — a per-kind listing that only
  duplicated the cross-kind sweep — which in turn made the index rekey possible
  (see the entry above).

  **The waker's three loss points** — the escalation described here was later
  replaced by watermark replay (see the entry below); kept as written, because the
  reasoning is what that replacement had to answer. All three logged at Warn and
  repaired by escalating the *catchup* ticker (not resync, which is off by default — a repair
  hung off an opt-in knob would be dead where it is needed most): a failed
  `EdgesGroupIncomingByID` arms one full pass (it cannot be narrower — the lookup that
  failed is what would have named the dependents); a closed change stream and a
  failed subscription each force every later pass, since they keep dropping changes
  rather than having dropped one. `Beehive.resyncKindsNextTick`/`EveryTick` fan out
  to **every** registered reconciler, because edges are cross-kind — an earlier
  attempt kept the flags process-wide but consumed them per-kind, repairing one
  arbitrary kind and silently spending the repair for the rest;
  `TestDroppedWakeEscalatesEveryKind` registers two kinds to pin it. The
  subscribe-failure message finally says something true: it used to claim "relying
  on resync", which was false even before resync became opt-in.

  **Two store breaks** (`DeletionRequestsList`'s signature,
  `ListDeletionPendingIDs`'s removal) plus the `WithResyncInterval` meaning change.
  Taken deliberately; `type Store = storeapi.Store` is an alias, so the interface is
  externally implementable and both are visible to embedders.

  **What this does not do.** The full resync stays off by default, so an
  unobserved wake loss is still bounded by a restart rather than by an interval
  (see the mid-reconcile item above, re-filed against these defaults). And the race
  detector never ran on any of it — this sandbox has no cgo compiler, and the
  escalation flags are the series' only new cross-goroutine state.

- **`idx_objects_deleting` made covering for its one remaining reader** — done.
  The index was `objects(deletion_requested_at) WHERE deletion_requested_at IS NOT
  NULL`, and its two readers both planned as `SEARCH … USING INDEX` plus
  `USE TEMP B-TREE FOR ORDER BY` — not covering (a row fetch per match) and
  sorting, because the key orders by `(deletion_requested_at, id)` rather than by
  `id`. The lost *covering* scan arrived with `DeletionRequestsList`, which added
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
  NOT NULL)` semi-join in the finalizing-edges cleanup — still plans identically,
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
  recovery for unsettled objects** — first resolved by documenting the
  configuration and making it announce itself; **that decision was later reversed**
  when the intervals were split (see the entry above). Startup now drains owed work
  unconditionally, so the hole is closed structurally and the warning this entry
  describes is gone along with the `StartupReconcileStrategy` enum. The reasoning
  below is kept because it is why the *first* answer was documentation rather than
  behaviour — the configuration was reachable and had a legitimate use, and the
  alternative on the table then would have taken it away with nothing offered in
  its place. The interval split changed that: there is no longer a configuration
  being taken away, only a hole being closed.

  The original resolution follows.

  Resolved by **documenting the configuration and making it announce itself, not by
  taking it away**. The behaviour is unchanged:
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
  already public: `Store.ObjectsListUnsettledIDs` (via the `type Store = storeapi.Store`
  alias, on the store the embedder opened and passed to `New`) reports exactly the
  objects owed a pass, and `Client.Requeue` dispatches one. So the configuration
  means "I reconcile on my own schedule", which is a real use — and the library
  overriding two deliberately-set knobs would take it away with nothing offered in
  its place. Pinned today by `TestDisabledBackstopsAnnounceThemselves` (the opt-out
  is honored and is not silent) and `TestSelfDrivenRecovery`, which runs the
  documented recipe end to end so the escape hatch can't quietly stop working. (The
  names above are post-rename; the entry's own prose predates the
  `StartupReconcileStrategy` → `WithStartupResync` collapse.)

  **The alternative was built, then reverted** — recorded so it is not rebuilt.
  Hoisting `enqueueUnsettled` out of the strategy switch (next to
  `enqueueDeletionPending`/`enqueueReconcileOwed`) makes recovery unconditional and
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
  `ObjectsCreate` (`observed_generation` NULL) and now settles it first, so
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
  self-heals on cadence. `enqueueReconcileOwed` broke that premise: at
  `resyncInterval = 0` its startup call is the *only* invocation, so a failed
  `ReconcileOwedListIDs` defers every recorded owed wake to the next process start —
  the one backstop whose entire purpose is not losing them, failing indistinguishably
  from "nothing was owed". It now warns, and takes a `source` naming which backstop
  lost its pass, since the cost differs sharply between them. Surfaced in review of
  the `reconcile_owed` work, which is exactly the "when one of the items above is
  touched" trigger the deferred entry named. `reconciler.log()` was added alongside
  it (the nil-safe accessor `typedController` already had), because the enqueue
  helpers are reachable on reconcilers built outside `Register`.

- **Durable twin for the in-memory dependency wake (`reconcile_owed`)** — done.
  `DependenciesAdd`'s guard (see CLAUDE.md) produced an ordinary `wakeAfterCommit`
  requeue, and the work queue does not outlive the process: a crash between the
  edge's commit and the dispatch left the edge in place, the dependent settled on a
  stale read, and *nothing* recording that a reconcile was owed — the same permanent,
  silent end state the race had, now reachable with a correct guard. Unlike every
  other in-memory wake, dependency staleness left no persisted trace (a lost
  spec-write wake still leaves `generation > observed_generation` for the resync tick
  to re-derive).

  The fix increments `objects.reconcile_owed` under the same edge-new ∧ target-moved
  conjunction that fires the wake — so the durable record and the edge commit
  together. It lives *inside* `EdgesAdd` and *before* the insert, reporting through
  `EdgesAddResult.ReconcileOwedStamped`; as a second store call after `EdgesAdd` returned it was
  not actually indivisible from the edge, since a nested `Within` unwinds nothing
  (see "A nested `Within` is not a rollback boundary" above, which records both the
  reviewer's finding and why the general fix was not taken). The stamp is
  deliberately *not* gated on `fromID`'s kind being registered: an ungated stamp on
  a client-only kind is an unread count nothing scans (see the sweeper item above), where a gated one loses the wake
  outright if that kind ever gains a controller — and gating would have meant
  running a beehive predicate inside the write transaction and deciding
  registration in two places, when `enqueueIfRegistered` already decides it once.
  `typedController.reconcile` subtracts, on a successful pass, **the count it
  loaded** (`ReconcileOwedDecrement(id, observed)`, floored at 0), skipping the write
  when that count is 0. `reconcile_owed` is a **count of outstanding wakes, not a
  single token** — that is what survives a wake owed *while an earlier one is being
  reconciled*: it lands above `observed` and so survives the subtraction, where a
  token compared to the loaded value (an earlier design used the target's rv) would
  have been dropped when two wakes for the same unchanged target shared a value, then
  lost to a crash (surfaced in review, pinned by
  `TestReconcileOwedSurvivesConcurrentIncrement`). Subtracting `observed` rather
  than 1 is the second half, also from review: one pass reads the target's *current*
  state, addressing every wake outstanding when it began, and the backstop enqueues a
  row only once (the work queue coalesces), so a `-1` would strand the remainder with
  nothing to re-enqueue it — indefinitely at `resyncInterval=0`, one per tick
  otherwise (`TestReconcileDrainsMultipleOwedPasses`). No follow-up requeue is
  scheduled: a residual exists only when an increment landed mid-pass, and that
  increment carried its own in-memory requeue. A failed subtraction is logged and
  left for the backstop rather than requeued, which would spin against a store that
  keeps failing. The
  backstop `ReconcileOwedListIDs` rides the partial index
  `idx_objects_reconcile_owed WHERE reconcile_owed != 0` — a sibling of
  `ListDeletionPendingIDs`, not folded into `ObjectsListUnsettledIDs` (an owed wake is
  orthogonal to spec convergence), enqueued unconditionally at startup and each
  resync tick so `StartupReconcileNone` + `resyncInterval=0` still recovers.
  `TestDependencyRequeueLostAcrossRestart` now passes unskipped (restart under
  `StartupReconcileNone` + `resyncInterval=0`, 3/3).

  Two things it deliberately does *not* do. It is a durable *wake*, not a derived
  backstop: it recovers only signals something explicitly raised, so a wake lost
  because `dependentsWake` swallowed its edges-lookup error (see "Three silent
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
  `reconcile_owed`. It costs a write on every successful pass (vs only when something is
  owed) and over-flags (a target change during a pass costs an extra tick even when
  the read was late enough), so it was not taken now; revisit it if the waker's silent
  loss points are addressed. Storing the cursor **per-edge on `edges`** was rejected
  outright: advanced only on re-assert, a controller that declares an edge once and
  never re-asserts freezes its cursor and the backstop re-enqueues it every tick
  forever.

- **Slug-keyed delete as a store mutator (`DeletionRequestsCreateBySlug`)** — done.
  `Client.DeleteBySlug` previously resolved the slug with `ObjectsGetBySlug` and then
  delegated to the id-keyed `Delete`: two statements with a window between them, in
  which the row could be collected and the slug retaken, leaving the call to report
  nil against a live row it never touched. It now calls a slug-keyed mutator that
  folds the slug into the `UPDATE`'s own `WHERE`, the way the kind already rode in
  for `DeletionRequestsCreate`, so the resolve and the mark are one atomic statement.

  The enabling change was generalizing `markForDeletion`'s key predicate: it took an
  id plus an `extraWhere` for guards, and now takes the caller's whole row predicate
  (`id = ?` plus scope for the two id-keyed callers, `group`/`kind`/`slug` for the new
  one). `DeletionRequestsCreateFromOwner` — the GC cascade — shares that mutator and moved with
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
  `Delete` via `gcAdvance`. This closes the `Added`-after-`Modified` inversion, the
  wake-for-a-rolled-back-row case, and the cascade-of-physical-deletes-inside-the-
  caller's-transaction case on `gcAdvance`'s synchronous-collect branch. The one
  consequence it does *not* fix — a `created=true` returned from a transaction that
  later aborts — is inherent to nesting and stays documented in the `GetOrCreate`
  godoc and the README's *Writes* section.
