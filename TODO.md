# TODO

Deferred work, and why. An item belongs here when it is a real defect or gap we
chose not to fix yet — not a wishlist. Each one says what would make it worth doing,
so the next reader can tell "we decided against this" from "nobody thought of it".

- **A dependency cycle of length ≥ 2 reconciles forever** — known, not fixed. The
  self-edge case *is* fixed: `dependentsWake` skips `from_id == to_id`, so an object
  that depends on itself does not re-queue itself. Two objects that depend on each
  other still do. A's write wakes B, B's wakes A, and nothing stops it —
  `workQueue.addLocked` has no rate limiter, and the dispatch path has no
  already-settled skip.

  Almost any write sustains it: changed status bytes, a byte-identical `UpdateStatus`
  at a generation the object has not settled at, any real condition write, or
  `FinalizersDelete`. Keeping status byte-stable is no defence. Only `EventsAdd` is
  safe, because it bumps no object `resource_version`.

  **What it costs.** The wake interval bounds the rate to one round trip per tick, so
  it is not a hot loop, but it never converges and never stops. The store runs on one
  connection, so the loop's write transactions queue ahead of every other writer —
  client writes, other kinds' reconciles, the GC sweeper, event retention — while
  holding a reconcile worker and consuming `resource_version` numbers. Every watch poll
  sees the pair change on every tick. And nothing reports a problem, because every
  object is converged and every generation matches.

  **Two possible fixes.**

  1. *Reject cycles when the edge is declared.* This needs a recursive CTE on the
     single connection, in `DependenciesAdd`. That is strictly more expensive than the
     pre-read that already sank an earlier version of the declare-time guard.
  2. *Give the work queue a minimum re-enqueue interval per item*, which is what
     controller-runtime does for this. It bounds cycles of any length, needs no graph
     query, and costs nothing on the hot path. It does not make a cycle converge, but
     it turns "forever, at speed" into "forever, once per interval", which removes the
     contention.

  **The stale-dependents pass raises the value of fixing this, and removes the one
  escape hatch.** It sustains a cycle exactly as the waker does — two mutually
  dependent controllers that write on every pass keep re-staling each other — but
  unlike the waker it cannot be turned off, since it is what makes a dependency wake a
  guarantee. Where a test could previously quiet a cycle by disabling the waker,
  nothing can now. Its 60s cadence is noise against the 1s waker, so this changes the
  cost of a cycle not at all; it changes only whether there is a way out.

  Option 2 is the one to try first. It cannot reuse `addAfter`, whose newest-wins alarm
  would push the item back on every fresh wake and starve it; it needs an oldest-wins
  watermark on `addLocked`, which is the path every wake takes. The cost is that
  watermark, one new constant with no natural value, and working out how it interacts
  with `Result.RequeueAfter` and backoff.

  **Deferred on fix cost, not on likelihood.** The self-edge case was one comparison
  on values the loop already had; this needs either a recursive CTE or a new work-queue
  primitive. Likelihood argues the other way: a self-edge means naming your own id,
  while a mutual dependency is what two separately written controllers fall into when
  neither author sees both halves. It is also still open whether beehive should support
  cycles at all, which is a reason not to guard hastily.

  **Tripwires**, since no single test constrains both fixes. For option 1,
  `TestAddDependencyAcceptsCycle` asserts that cycle-closing and self edges are both
  accepted today — exactly what that fix would change. For option 2,
  `TestWorkQueueNoConcurrentDispatch` and `TestWorkQueueReaddAfterDone` both assert
  that the *second* dispatch of an id is immediately available, which is the latency a
  minimum interval renegotiates. `TestWorkQueueFIFO` and the dedup tests are not
  tripwires: they add distinct ids once each, and a first add stays immediately
  dispatchable under any sane throttle.

- **`DeleteBySlug` on an absent slug costs a write transaction** — known, not fixed.
  `DeletionRequestsCreateBySlug` opens `Within` (so `BEGIN IMMEDIATE`) and its first act
  inside is `nextResourceVersion`, an `UPDATE` on the sequence — both before
  anything is known to match. A slug no row holds therefore costs a write
  transaction, a sequence write, the zero-row `UPDATE`, the re-read, and a
  rollback, where the pre-mutator client code cost a single lock-free `SELECT`.
  The rollback means no cursor value is burned, but the journal/page work happens.

    That absent path is the steady state of what this method is for. A controller that
  idempotently removes a child re-runs the call every reconcile, so it keeps hitting
  the absent path long after the one call that actually deleted something.

  The fix is a lock-free `getObjectRowBySlug` before `Within`, short-circuiting both
  idempotent outcomes — no such slug, and a row already deletion-pending — and falling
  through to the atomic mark otherwise. That makes absent 1 statement, no-op 2, and the
  happy path 4, one more than today.

  Deferred because it brings back a read-then-write shape as a fast path, and because
  its no-op branch would answer outside a transaction where the id-keyed sibling
  answers inside one. That divergence needs more thought than the saving currently
  justifies. Revisit if a profile shows absent-path deletes are hot, or if
  `DeletionRequestsCreate` gets the same treatment — its absent path has always had
  this shape, so the probe would belong in `requestDeletion` for both.

- **The waker's startup seed race costs latency, not convergence** — known, no longer
  a correctness hole. `Start` launches the waker with `bh.wg.Go` and returns; `seed`
  runs whenever the Go runtime first schedules that goroutine, and `runDriver`'s eager
  first step is a seed that reads `ObjectWritesMaxVersion` and returns without
  scanning. Nothing orders those against `Start`'s return, so a caller that writes
  target T as soon as `Start` hands back its stop func can commit T's new version
  *below* the watermark the waker then takes. A failed seed is the same hole by another
  route: the next tick seeds from the cursor as of *then*, so everything committed in
  between is below the watermark and never scanned.

  Either way that change is never read by any scan — and a settled dependent D of T is
  invisible to every owed-work listing, since D's own generation never moved and
  nothing stamped `reconcile_owed`. **What closes it is the stale-dependents pass**,
  which re-derives staleness from `dependency_watermarks` rather than from anything the
  waker recorded (see [the ADR](docs/adr/2026-07-29-dependency-watermarks.md)). So D
  converges within one stale-pass interval instead of never, and what is left here is
  60 seconds of latency where the waker promises one.

  **The fix is still to seed synchronously in `Start`**, under `startCtx`, before the
  reconcile loops are launched: the watermark then provably precedes every write any
  caller could make, because no caller holds the stop func yet. A few lines, no schema
  change. Not done because it moves a store read into `Start`'s critical section, where
  it is the first thing that can fail there for a reason unrelated to configuration —
  and the answer to "does a failed seed abort startup" has to be no, which means
  keeping the retry-on-next-tick path alive rather than replacing it. Worth doing on
  latency grounds alone, but no longer urgent.

  **Tripwires.** `TestWakerSeedsFromTheStoreCursor` pins that the first scan starts at
  the seed. `TestWakerRetriesSeedOnTheNextTick` is the one that constrains the fix: a
  seed that fails must leave the waker unseeded and scanning nothing, so a synchronous
  seed in `Start` must fall back to that path rather than returning an error.

- **A `RequeueAfter` chain does not survive a restart** — known, not fixed. The
  dependency-wake half is closed: staleness is re-derived from
  `dependency_watermarks`, so a wake lost with the process that owed it is found again
  by the stale-dependents pass. This is the self-scheduling half, and nothing
  re-derives it, because there is nothing in the store to re-derive it from.

  A controller that keeps itself running with `Result.RequeueAfter` — polling an
  external system, re-checking a lease — leaves *no durable trace at all* once its
  object has settled. It is not unsettled, it is not `reconcile_owed`, it is not
  deletion-pending. The schedule lives only in `workQueue.alarms`, and `stop` cancels
  every one of them. So the chain restarts only if something re-dispatches the object
  cold, and on stock defaults nothing does: the owed drain finds nothing owed, and both
  full passes are off. The controller simply stops running, with every object
  converged and no error anywhere. Unlike a stranded dependency wake it does not even
  heal at the *next* restart, because every restart is equally silent.

  **No durable fix is proposed, because durability is the wrong frame for it.** A
  `RequeueAfter` is a controller's private timer, not a fact about the object, and
  persisting it would mean writing a row per poll on the single connection — the cost
  `reconcile_owed` exists to avoid.

  **The real question this raises is whether a self-polling controller should be
  expressed this way at all.** `RequeueAfter` is sound as a *retry* — a bounded
  follow-up to a pass that has more to do — and unsound as a process's only heartbeat,
  because a heartbeat that exists solely in one process's memory is not a property of
  the system. An embedder that needs periodic re-derivation has two honest options:
  own the ticker itself and call `Client.Requeue`, which is explicit about the schedule
  living in the application, or enable `WithFullPassInterval`, which is explicit about
  paying per object. Until that is written down, `RequeueAfter`'s doc reads as though
  it were a durable schedule. Documenting the boundary is the actual deliverable here,
  not a mechanism.

- **A controller that never calls `UpdateStatus` is permanently unsettled** — known,
  not fixed. `observed_generation` is NULL until the first `UpdateStatus`, and
  `ObjectsListUnsettledIDs` lists `observed_generation IS NULL OR observed_generation <
  generation`. A controller with no meaningful status to write — one whose whole job is
  a side effect elsewhere — therefore never settles a single object, and the owed pass
  re-enqueues every one of them on every tick, forever.

  It is not a hot loop (the owed-pass cadence bounds it) and it is not incorrect: the
  object genuinely has not reported convergence. But it is indistinguishable at a
  glance from a healthy system, it scales with the object count rather than with what
  changed — the one property the owed pass is designed not to have — and it defeats
  every gate on `ObservedGeneration == Generation`, including a caller waiting for one.

  **This is a documentation gap before it is a code one.** The handshake ADR says
  `ObservedGeneration` is nil until the first `UpdateStatus`; what it does not say is
  that *never* calling it is a supported-looking way to opt out of convergence
  entirely. A code fix would mean either a no-status escape hatch (an explicit
  "settled, nothing to report" call, which is `UpdateStatus` with a zero value and so
  adds surface for nothing) or having the reconciler record the handshake itself on a
  clean return — which would settle objects the controller never actually converged,
  and is a much larger change to what the generation handshake means. Neither is worth
  it against saying so plainly where controllers are introduced.

- **`Create` accepts a `WithOwner` naming an already-deleting owner** — known, not
    fixed. This is the ownership version of the read-then-declare race that
  `EdgesAdd`'s new-edge stamp closes for dependencies (see [the stamp-every-new-edge
  ADR](docs/adr/2026-07-29-stamp-every-new-dependency-edge.md)): there the edge is
  declared after the *change*, here after the *cascade*.

  `insertObject` checks nothing about the owner's lifecycle, and `EdgesAdd` only
  verifies that both endpoints exist, never that the target is alive. So a child
  created against an owner that is already deletion-pending — and whose cascade has
  already run — is born live and unmarked under a finalizing owner. Its `owned_by` edge
  counts as a live claim in `EdgesHasIncoming`, which excludes only deletion-pending
  `depends_on` sources, so the owner can never be collected.

  Nothing the child does recovers it. `EdgesAdd` bumps no `resource_version`, so no
  scan of the write log finds the edge; `dependentsWake` reads only `depends_on` and
  would ignore it anyway; and the child's own `gcCollect` returns immediately because
  the child is not finalizing. Reproduced 3 times out of 3 with an owner held alive by
  a finalizer and its cascade provably complete: the second child stays alive and
  unmarked while the owner sits deletion-pending.

  **Unlike the `depends_on` race, this heals on the next GC sweep.**
  `deletionPendingSweep` re-lists the still-pending owner, and `gcCollect` re-runs
  `DeletionRequestsCreateFromOwner`, which is built to be re-run and picks the new child
  up. So the exposure is one GC interval, and **no configuration makes it permanent**,
  because `WithGCInterval` rejects a non-positive value. (Reproducing the window
  therefore needs a long GC interval, not a disabled one.) It is also visible where the
  dependency race is not: the owner is plainly stuck deletion-pending, rather than
  quietly settled on a stale read.

    **Detecting it is cheap; deciding what to do is the open question.** `insertObject`
  already runs inside the create transaction, so reading the owner's
  `DeletionRequestedAt` there costs one indexed read per child creation — not the
  per-reconcile-forever tax that sank `DependenciesAdd`'s pre-read guard. Three
  options, none obviously right:

  1. *Reject with an error.* Honest — the caller asked to attach to something being
     torn down — but it adds a failure mode to `Create` and races anyway, since the
     owner can be deleted the instant after the check.
  2. *Create the child already marked.* Self-consistent and needs no new error, but it
     manufactures a deletion-pending object the caller never asked to delete, whose
     spec is then unreachable.
  3. *Leave `Create` alone* and accept the sweeper as the answer, since it runs on an
     interval that cannot be disabled. That makes this a latency bound rather than a
     correctness gap.

  Whichever wins, the owner's `DeletionRequestedAt` is already on the row `EdgesAdd`
  reads for its endpoint check — the same read that reports the target's version for
  the dependency guard. Adding it to `EdgesAddResult` would let `insertObject` see the
  owner's lifecycle without a second read.

  Deferred because the exposure is one GC interval rather than a strand, and because
  choosing between those options is a public API decision. Revisit if a controller
  turns up that creates children against owners it does not itself hold a finalizer on.
  There is no test yet: the repro exists only as a throwaway, and
  `TestDeletionRequestsCreateFromOwnerCascadesThenIsNoOp` re-cascades over a fixed child
  set, so it never adds a child between passes.

- **A client-only dependent's `reconcile_owed` count is never reclaimed** — known, not
  fixed. Edges are deliberately cross-kind, so declaring a dependency from a
  client-only object is legal, and the stamp lands on a row whose kind has no reconcile
  loop. Nothing drains it, since only a reconcile calls `ReconcileOwedDecrement`, and
  nothing scans it, since `ReconcileOwedListIDs` is per-kind. So the count and its
  index entry last as long as the row. Every new `depends_on` edge stamps (see
  [the stamp-every-new-edge ADR](docs/adr/2026-07-29-stamp-every-new-dependency-edge.md)),
  and re-creating a deleted edge satisfies the edge-new test once more and increments
  it again, so it is not even bounded by the number of distinct targets. The count is
  *unread*, which is not the same as harmless.

  The impact is small and does not compound. Nothing reads the count while the kind
  stays client-only, and if it later gains a controller the first reconcile subtracts
  the whole observed count, so N accrued increments cost **one** extra pass rather than
  N. What is left is stored bytes and an index entry nobody collects.

  **Gating the stamp on registration is the wrong fix.** The stamp is SQL inside
  `EdgesAdd` — it has to be, for the ordering the nested-`Within` contract forces — and
  the store cannot know which kinds are registered. So the caller would have to resolve
  `fromID`'s kind before every declare, which is the per-call pre-read that sank the
  original guard, on a path controllers re-run forever. It would also freeze a fact
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

    It is safe today for one narrow reason: the reconciler is the only caller, and it
  passes the id of a row it loaded a line earlier for its own kind, with the count read
  off that same row. Nothing reaches it with another kind's id, and the `max(…, 0)`
  floor means even a mistaken call could not corrupt the count — it would only clear a
  wake another kind was owed.

  So this invariant rests on caller discipline where the rest of the store has
  structure. The fix is small in itself — add `AND "group" = ? AND kind = ?` and thread
  a `GroupKind` through — but it changes a `storeapi.Store` signature and wants a test
  pinning the rejection, which is more than the two-line diff it looks like. Deferred
  because there is no reachable defect here, only an invariant to move from convention
  into the schema. Revisit when a second caller appears: the cross-kind sweeper above
  would be one, and it would deliberately reach for rows of kinds it does not own —
  exactly the case scoping exists to catch.

- **`ObjectsCreate` takes a `RawObject` and silently drops most of it** — known, not
  fixed, and the input-side twin of the item below. `RawObject` mirrors a whole row and
  is shaped for reads, but it is also `ObjectsCreate`'s parameter. The INSERT binds six
  of its fields — group, kind, slug, spec, schema_version_spec, finalizers — and
  ignores the other twelve. A caller that sets any of those gets a row without it and
  no error.

  Some of the twelve are defensible: `ID`, `ResourceVersion`, `CreatedAt` and
  `UpdatedAt` are store-assigned, and `Generation` starts at 1. `Status` is the sharp
  one, because seeding a status on create is a reasonable thing to try and it is
  discarded silently. Nothing at the call site says which is which — the struct offers
  eighteen fields and honours six.

  The fix is to stop using the read shape for the write: a `CreateObjectInput`, or
  functional options, carrying only the six fields create accepts, so the compiler
  rejects the rest instead of the store dropping them. Deferred because `RawObject` is
  an exported alias, so narrowing the parameter breaks an externally implementable
  `Store`, and it should be done together with the return-shape item below rather than
  as a second separate break.

- **Mutators build a `RawObject` no caller reads** — known, not fixed. This is the
  general form of the point the `DeleteBySlug` item makes about one method. Every
  mutator returns a full `*RawObject` with conditions attached, because `scanWritten`
  calls `attachConditions` on the write path. On the branches whose row nobody reads —
  `ObjectsUpdateSpec`'s content no-op, `UpdateStatus`'s settled no-op,
  `requestDeletion`'s already-pending re-read — that conditions query builds a value
  the only caller throws away: `controllerClientImpl.UpdateStatus` drops it, and the
  delete path reads only `obj.ID`. Where the client returns the object to the user, in
  `Create`, `Update` and `CreateOrUpdate`, the work is needed.

  Skipping `attachConditions` per branch was rejected: it would make one method's
  return shape depend on which branch it took, and differ from its sibling a few lines
  away. The contract is the thing to change. The options are narrowing what the `Store`
  godoc promises about a returned row, adding mutator variants that return nothing or
  just an id and version, or making the discard explicit at the `storeapi` boundary so
  the store can skip the work.

  Deferred because `type Store = storeapi.Store` is an alias, so any of those breaks an
  externally implementable interface, and the saving is one indexed query per
  discarding write. Revisit when the next `Store` break is on the table anyway.

- **`incoming == 0` conflates "no migrator" with "unversioned", so an old build can
  launder reshaped bytes under the stored schema version** — known, not fixed.
  Explore a `WithSchemaVersion(n)` option that lets a kind declare its schema
  version independently of registering a `Migrator`.

    **How it happens.** `convertBlob` passes a blob through untouched when the current
  version is 0, so a build with no migrator decodes a v3 row as-is. If that build's
  struct is the *older* shape, `json.Unmarshal` quietly drops the v3-only fields. The
  write-back then reports `incoming == 0`, `stampVersion` keeps the stored tag, and
  old-shaped bytes end up labelled v3. A later v3 reader sees a matching version, skips
  conversion, and misreads them. `stampVersion` is where the mislabelling happens, but
  it is not where the information was lost.

  **The obvious guard doesn't work.** Rejecting a content change when
  `stored > 0 && incoming == 0` sounds right, but `incoming == 0` means "no migrator
  registered", not "old struct", and those come apart constantly. Registering a
  `Migrator` is optional, so a build carrying the current struct with no converter yet
  writes faithful v3 bytes and reports 0. A client-only kind cannot attach a migrator
  at all, so an application driving the DB purely through `Client` reports 0 by
  construction. The benign case dominates, and the guard would wedge those writers
  permanently — every reconcile erroring — to defend against a mixed-binary rollback.

  **Nor can the store choose a better tag.** Changed bytes do not imply a reshape; an
  ordinary `Update` changes bytes too. Stamping the stored version is wrong when the
  round trip was lossy, and stamping 0 is wrong when it was faithful, because a
  restored build would then re-convert already-converted data. There is no third answer
  available at that call site, which is what makes this a missing-signal problem rather
  than a stamping bug.

  **Hence `WithSchemaVersion(n)`**: let a kind declare "my shape is v3" without
  shipping a converter, and let client-only kinds do it too. Then `incoming == 0`
  really does mean unversioned, and the guard above becomes sound.

  The read path deserves the same pass at the same time. If an unversioned build cannot
  be trusted with a v3 row, then `from > 0 && current == 0` is arguably the same
  "older build reading newer data" downgrade as `from > current`, and refusing it in
  `convertBlob` stops the lossy decode before it can become a write. Blocking only the
  write leaves a process that can observe but not act — defensible, but it should be
  one deliberate decision across both sides.

  Deferred because it needs a new public option and a read-path policy change together,
  and because the corruption requires two builds with different struct shapes sharing
  one database file. Revisit before the first real `Migrator` consumer ships a v2, or
  the first time a rollback across a schema bump has to be supported.

- **`OpenPool` never sets `auto_vacuum`, and that door closes behind us** — known, not
  fixed, and the only item here with a deadline. The mode has to be chosen *before the
  first table exists*; on a database that already has one, switching it requires a full
  `VACUUM` rewrite. `0001_init.sql` sets nothing, so every file we have ever written is
  `auto_vacuum=NONE` and stays there unless someone pays that rewrite.

  We do delete rows, so free pages genuinely accumulate: `gcCollect` removes collected
  objects and their edges, and event retention trims the log on the GC sweeper's
  cadence. A long-lived store whose object count churns therefore keeps its high-water
  page count forever. Nothing is incorrect — the pages are reused by later inserts —
  and the file simply never shrinks.

  Deferred because the cost is unmeasured and the fix is not free either: `INCREMENTAL`
  needs someone to actually call `PRAGMA incremental_vacuum`, which is a new
  responsibility with no obvious owner (the GC sweeper is the natural home, and it is
  the one driver that cannot be disabled), and `FULL` moves that work onto every commit
  on the single write connection. Revisit before the first deployment that expects to
  run for months with heavy object churn — after that, every existing file needs the
  `VACUUM`, so the decision gets strictly more expensive to reverse the longer it waits.

- **The page cache and `mmap_size` are untuned, and it is unclear whether tuning them
  buys anything here** — known, not fixed. `OpenPool` sets four pragmas and leaves
  SQLite's stock ~2MB cache and disabled memory mapping alone.

  Two things make this less obviously a win than the standard advice suggests. We run
  on `modernc.org/sqlite`, a pure-Go translation rather than a cgo binding, so whether
  `mmap_size` maps anything at all there is **unverified** — that is the first thing to
  check, and if it is a no-op the item halves. And the store is one connection, so a
  larger cache is not shared across concurrent readers the way the advice assumes; it
  helps only by keeping the drivers' repeated scans off the disk.

  Those scans are the one real argument for it. The owed pass, the stale-dependents
  pass, the waker and the watch poll all re-read the same indexes on a fixed cadence
  forever, which is exactly the working set a page cache is for. Deferred because it is
  a tuning change with no measurement behind it, and adding pragmas we cannot show a
  number for is how a config grows cargo. Revisit with a benchmark on a store large
  enough that the scans miss cache — until then there is nothing to compare against.

- **Nothing writes down why `synchronous=NORMAL` is safe for us** — a documentation gap,
  not a defect. The pragma's usual summary is that WAL plus `NORMAL` cannot corrupt the
  database, which is true and is not the property we actually depend on: a power loss
  can also lose the *most recently committed* transactions. Stated plainly, that reads
  as a direct contradiction of the crash-safety the whole `reconcile_owed` design exists
  to provide.

  It is safe for a specific reason, and the reason is the interesting part. The loss is
  transactionally consistent at the tail: `EdgesAdd` stamps `reconcile_owed` in the same
  statement sequence as the edge it stamps for, so a lost commit takes the edge *and*
  its stamp together. There is no interleaving in which the durable trace survives and
  the work it owes does not — which is precisely the strand the stamp-every-new-edge ADR
  argues against. The same holds for a spec write and the generation it bumps.

  So the durability floor is "we may lose the last few commits entirely", never "we may
  lose the wake but keep the write". That distinction is load-bearing and lives nowhere
  — `OpenPool`'s comment lists the pragma without it. It belongs next to the
  [stamp-every-new-edge ADR](docs/adr/2026-07-29-stamp-every-new-dependency-edge.md)'s
  interleaving argument, which is where a reader is already thinking about exactly this.
  Deferred only because it is prose; it is cheap and should be written the next time
  that ADR is touched. If the invariant ever stops holding — a design that records owed
  work in a *different* transaction from the change that owes it — the answer is
  `synchronous=FULL`, and that is the tripwire to watch for.
