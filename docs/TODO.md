# TODO

Deferred work, and why. An item belongs here when it is a real defect or gap we
chose not to fix yet — not a wishlist. Each one says what would make it worth doing,
so the next reader can tell "we decided against this" from "nobody thought of it".

Once we decide to build one, it moves to [`specs/`](specs/README.md) and the entry
here shrinks to a pointer.

- **Do not remove `EventsAdd`'s return value, even though the write-shapes
  rule says to** — a deliberate exception, recorded so nobody applies the
  rule mechanically. `Store.EventsAdd` returns a run that no caller reads
  today, which by the
  [write-shapes ADR](adr/2026-07-30-store-write-shapes.md) means it should
  return `error` alone.

  An events push path builds its delta from exactly that return value. The
  store already computes the run to write it, so returning it costs nothing
  and saves the push path a read. Tidying it away now would have to be
  undone.

  Revisit only if an events push path is ruled out for good.

- **An edge write is invisible to every cursor in the system, and the fix is one
  counter rather than a log** — known, not fixed, and recorded here mostly to
  settle the question it keeps raising: whether `edges` writes belong in
  `object_writes`. They do not.

  `EdgesAdd` and `EdgesDelete` bump no `resource_version` and append no write-log
  entry, because a ref is not a field of the object. So no log cursor can see a
  new edge, a dropped one, or the `dependency_watermarks` clear that rides a new
  one. Two places pay for it, both in case 11 of
  [`reconcile-triggers.md`](reconcile-triggers.md): a deletion unblocked by
  `DependenciesDelete`, and one unblocked by its last child's removal. Both wait for
  the next GC tick with nothing able to signal them. The third route out of a blocked
  collect — the cleared finalizer — now pushes, which removes the most common reason
  anyone would notice these two.

  **The stale-dependents pass no longer pays.** Its scan is bounded by a cursor
  over target `resource_version`, which an edge write also cannot move — but a
  new edge stamps `reconcile_owed` inside `EdgesAdd`, so the owed pass carries
  that dependent and the scan is not asked to see it. An idle tick of the pass is
  now one `ResourceVersionsMaxIssued` read, so there is no full scan left for a
  gate to skip. See
  [the ADR](adr/2026-08-03-stale-dependents-cursor.md).

  **Recording edges in `object_writes` is the wrong fix, on four counts.**
  `edges.from_id` is `ON DELETE CASCADE`, so collecting an object removes its
  outgoing edges inside SQLite with no Go code on the path — a faithful log would
  need a trigger, or would under-record exactly the case a log exists to make
  recoverable. The tail emits one change per entry, so an edge add would deliver a
  `Modified` to every subscriber of the dependent's kind for an object whose spec,
  status and conditions are identical, and whose refs are not even in the
  delivered object unless the caller asked for them. Retention is bounded and the
  horizon moves, so an entry could never replace `EdgesAdd`'s `reconcile_owed`
  stamp — it would be a second, weaker record of the same fact. And it would cost
  a `resource_version` and an append for each edge write, on a path that is free
  today, which a controller re-declaring its edge set every pass pays per pass.

  **One monotonic counter covers what actually needs covering.** A consumer that
  needs "did the edge set move" needs one bit, not a stream: an epoch bumped by
  `EdgesAdd` and `EdgesDelete`, shaped like `driver_cursors`. No retention, no
  watch noise, and the cascade needs no trigger, because a cascade that removes
  edges also collects an object and that already appends.

  Deferred because the consumers left are the two edge-driven routes of case 11,
  where the cost is a single GC tick of latency on a deletion. Build the counter only
  when a consumer outside that case appears, or when the latency is measured to
  matter.

- **A dependency cycle of length ≥ 2 never converges** — rate-limited, not fixed.
  The self-edge case *is* fixed: `dependentsWake` skips `from_id == to_id`. Two
  objects that depend on each other still wake each other forever. A's write wakes
  B, B's wakes A, and no generation ever moves, so nothing reports a problem.

  Almost any write sustains it: changed status bytes, a byte-identical
  `UpdateStatus` at a generation the object has not settled at, any real condition
  write, or `FinalizersDelete`. Only `EventsAdd` is safe, because it bumps no
  object `resource_version`.

  **The contention is gone.** The work queue's re-enqueue floor bounds the loop to
  one round trip per `defaultMinRequeueInterval`, whatever the wake rate, so it no
  longer queues write transactions ahead of every other writer. Pinned by
  `TestADependencyCycleIsBoundedByTheFloor`, which runs 25 reconciles in 30ms with
  the floor off. See [the ADR](adr/2026-08-04-work-queue-re-enqueue-floor.md).

  **What is left is that it never stops.** A reconcile worker is occupied once per
  interval forever, `resource_version` numbers are consumed forever, and every
  watch sees the pair change forever — quietly, since every object is converged
  and every generation matches.

  **The remaining fix is to reject cycles when the edge is declared**, which needs
  a recursive CTE on the single connection in `DependenciesAdd` — strictly more
  expensive than the pre-read that already sank an earlier declare-time guard. It
  is also still open whether beehive should support cycles at all, which is a
  reason not to guard hastily. Deferred on that question rather than on cost now
  that the contention is bounded.

  Tripwire: `TestAddDependencyAcceptsCycle` asserts that cycle-closing and
  self edges are both accepted today — exactly what such a guard would change.

- **The waker's startup seed race costs latency, not convergence** — known, no longer
  a correctness hole, and narrower than it was since the waker started persisting its
  cursor (see [the ADR](docs/adr/2026-07-30-durable-waker-cursor.md)). `Start` launches
  the waker with `bh.wg.Go` and returns; `seed` runs whenever the Go runtime first
  schedules that goroutine, and `driver.Run`'s eager first step is a seed that returns
  without scanning. Nothing orders those against `Start`'s return, so a caller that
  writes target T as soon as `Start` hands back its stop func can commit T's new
  version *below* the watermark the waker then takes. A failed seed is the same hole by
  another route: the next tick seeds from the cursor as of *then*, so everything
  committed in between is below the watermark and never scanned.

  **The race is now much narrower.** Once the waker has persisted a cursor, `seed`
  resumes from it rather than from `ObjectWritesMaxVersion`, and a write racing
  `Start`'s return lands *above* that stored cursor — scanned on the next tick, not
  skipped. What still reopens the original window is a seed that falls back to
  `max`: a store with no `DriverCursorer`, or the first start of a fresh one.

  Either way that change is never read by any scan — and a settled dependent D of T is
  invisible to every owed-work listing, since D's own generation never moved and
  nothing stamped `reconcile_owed`. **What closes it is the stale-dependents pass**,
  which derives staleness from `dependency_watermarks` rather than from anything the
  waker recorded (see [the ADR](docs/adr/2026-07-29-dependency-watermarks.md)). Its
  cursor does not narrow this case: the racing write bumps T's `resource_version`, so T
  sits inside the range the next sweep reads. So D
  converges within one stale-pass interval instead of never, and what is left here is
  60 seconds of latency where the waker promises one.

  **The fix is still to seed synchronously in `Start`**, under `startCtx`, before the
  reconcile loops are launched: the watermark then provably precedes every write any
  caller could make, because no caller holds the stop func yet. Not done because a
  synchronous seed now reads *two* rows inside `Start`'s critical section rather than
  one, which only sharpens the hesitation recorded above about putting a store read
  there at all (see [the ADR](docs/adr/2026-07-30-durable-waker-cursor.md)), and
  because the answer to "does a failed seed abort startup" has to be no, which means
  keeping the retry-on-next-tick path alive rather than replacing it. Worth doing on
  latency grounds, but no longer urgent and now bounded to a smaller case.

  **Tripwires.** `TestWakerSeedsFromTheWriteLogMax` pins that the first scan on a store
  with no stored cursor starts at the write log's max; `TestWakerSeedsFromTheStoredCursor`
  pins that a store with one resumes from it instead. `TestWakerRetriesSeedOnTheNextTick`
  and `TestWakerRetriesSeedOnAFailedCursorRead` are the ones that constrain the fix: a
  seed that fails, on either read, must leave the waker unseeded and scanning nothing,
  so a synchronous seed in `Start` must fall back to that path rather than returning an
  error.

- **A waker resuming a very stale cursor can scan at full budget for minutes, and
  nothing decides that draining is no longer worth it** — known, not fixed, and
  deliberately unbounded rather than bounded by a guess. After a long enough
  downtime, `seed` resumes a cursor far behind the write log and every tick reads
  its full `wakeScanPagesPerTick` budget until it catches up: 32 queries a second
  on the one connection the reconcile loops share, potentially for minutes.

  **The obvious bound is the one that was removed, and it must not come back in that
  form.** Capping `max - stored` at seed reads a `resource_version` distance, but
  `EventsAdd` draws from that same sequence without writing anything the scan reads,
  so the distance overstates the real backlog by an unbounded factor. A store logging
  events at any rate would abandon cursors that were a few ticks of work. (The other
  motivation for it — a database file swapped for a larger one — does not exist: a
  stored cursor lives in the database it describes.)

  **A measured bound would work.** Count consecutive ticks that exhausted the page
  budget, and past some number of them stop draining and jump to a fresh
  `ObjectWritesMaxVersion`. That counts paging actually done, in the right unit and
  immune to event traffic. The natural threshold is one `staleDependentsInterval`
  of draining, because past that point the backstop has already swept every dependent
  the drain is still working toward, so the remaining wakes deliver nothing.

  Deferred because the cost is latency rather than divergence, the drain is doing
  real work rather than wasting it, and no deployment has been observed where it
  matters — the threshold above is reasoning, not measurement. Revisit if a restart
  after a long outage is seen to starve the reconcile loops. Tripwire:
  `TestWakerResumesAnEnormousBacklog` pins that a far-behind cursor is resumed today,
  which is exactly what such a bound would change.

- **The waker cannot tell that retention trimmed the log out from under its
  cursor** — known, not fixed, and latency rather than divergence. The per-kind
  read reports the boundary: `ObjectWritesListSince` returns `trimmedThrough`,
  and `ObjectWritesMaxVersion` folds the horizon in, which is how a watch tail
  detects a cursor below it and ends its subscribers with `ErrWatchTooOld`. The
  store-wide pair does neither. `ObjectWritesListSinceAll` returns entries and an
  error, and `ObjectWritesMaxVersionAll` reads a bare `MAX(resource_version)` off
  `object_writes` with no reference to `object_writes_horizon`.

  So a waker that resumes a cursor below the horizon — a stored cursor older than
  the write log's retention, 24h by default, after a downtime longer than that —
  scans from that cursor and gets whatever survived the trim. `resumeWatermark`
  clamps against `max`, which does not move down when retention trims the tail, so
  the clamp cannot see it either. The entries between the cursor and the horizon
  are silently skipped, and every dependent they would have woken is skipped with
  them.

  **Correctness is intact and that is why this is deferred:** those dependents are
  found by the stale-dependents pass, which derives staleness from
  `dependency_watermarks` and from `reconcile_owed`, neither of which the write
  log's trim can touch. This path opens only after a restart, which is also when
  that pass re-derives the whole graph from a cursor of 0. The cost is
  up to one `staleDependentsInterval` of latency, on a path that only opens after
  a downtime exceeding the write log's retention.

  **The fix is to surface the horizon on the store-wide reads** and have `seed`
  compare its resume point against it, as `objectTailer` does per kind. The waker
  cannot answer a gap the way a tailer does — there are no subscribers to end, and
  restarting from `max` is what it already does without a cursor — so the useful
  behaviour is to log the skipped span and force one stale-dependents sweep rather
  than wait for its tick. Not done because it widens `storeapi.Store` for a case
  no deployment has hit, and because the same reasoning that makes the gap benign
  makes the forced sweep an optimisation. Revisit when the store-wide reads are
  next touched, or if a long outage is seen to leave dependents stale for a
  minute after restart.

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

- **`EventsAdd` still takes the read shape, so the write-shapes rule has one
  exception** — known, not fixed. The
  [write-shapes ADR](docs/adr/2026-07-30-store-write-shapes.md) says a write takes
  only what it honours, and `ObjectsCreate` was narrowed to `ObjectsCreateInput` for
  exactly that reason. `EventsAdd(ctx, gk, id, ev Event)` still takes `Event`, the
  read shape, and reads five of its eleven fields — `Category`, `Type`, `Reason`,
  `Message`, `Detail`. The store assigns the rest, and its godoc says so *in prose*,
  which is the silent-drop shape the create path was just fixed for, on the same
  interface.

  It is milder than `ObjectsCreate`'s was: the dropped fields are `ID`, `ObjectID`,
  `Count`, `FirstAt`, `LastAt` and `ResourceVersion`, all obviously store-assigned,
  where `Status` on a create was a plausible thing to seed. So there is no trap here
  today, only an inconsistency.

  The fix is an `EventsAddInput` beside `ObjectsCreateInput`, same shape, same reason.
  Deferred because it is a third break of an externally implementable `Store` for a
  case with no reachable defect, and the ADR's own argument is that the break cost is
  paid per break rather than per method — so this wants to ride along with the next
  one that has to happen anyway, not to be its own. Revisit then, or sooner if a
  field is ever added to `Event` that a caller might reasonably try to set.

  **It missed the `EventsMaxVersion` window** (2026-07-31), which added a `Store`
  member and broke every external backend. That was a change to the same file and
  the same event family, and taking this along would have cost those backends
  nothing extra — the argument above says so in as many words. It was left out
  because the change was scoped to the watch gate. So the next break is now the
  *second* one an external backend pays for this, which is a point in favour of
  taking it early rather than waiting for a third.

- **Four types in `Store`'s signatures have no public alias, so the interface is not
  externally implementable today** — known, not fixed, and recorded because the rest
  of this file reasons about the cost of breaking external backends as though they
  exist.

  `Store` is aliased into the root package, and most of what its methods mention is
  aliased alongside it (`RawObject`, `ObjectRef`, `ObjectsCreateInput`, `RawEvent`,
  `DeletionCascadeChild`). Four are not: `Condition`, `EdgesAddResult`, `EventQuery`
  and `ReconcileLoad` live only in `internal/storeapi`, which an external module
  cannot import. A backend outside this module cannot write those signatures at all,
  so it cannot satisfy the interface — which makes "this break costs external
  backends" an argument about a population of zero.

  Three are a one-line alias each. `Condition` is not: the root package already
  exports a richer `Condition` for the typed API, so the alias needs a distinct name
  — `RawCondition`, exactly as `RawEvent` already resolves the same collision for
  `Event`.

  Not done here because it is API surface unrelated to the change that surfaced it,
  and because it is worth deciding deliberately: aliasing these promotes every field
  of four store-shaped structs to public API, which is a commitment the internal
  package currently avoids. Revisit when an external backend is actually attempted,
  or fold into the next break that touches these types — the same ride-along argument
  the `EventsAddInput` entry above makes.

- **Two writes still read the whole row to answer a narrow question** — known, not
  fixed, and the tail of the write-shapes pass. Every write that reports *no* row now
  answers from metadata alone: the deletion probes, `ReconcileOwedDecrement`'s fault
  probe and both condition mutators' kind gates go through `checkObjectScoped`. What
  is left is the pre-read on the two writes that genuinely need row *content*, and
  those read more of it than they use — `ObjectsUpdateStatus` selects all 17 columns
  (including `spec`, the largest, and it unmarshals `finalizers`) to read six;
  `FinalizersDelete` needs three.

  One fold remains available on top of narrower `SELECT`s: `ConditionsSet` runs its
  kind gate and `getCondition` as separate statements against the same key, which one
  `LEFT JOIN` from `objects` to `conditions` collapses. Worth naming because
  `ConditionsSet` is the hottest write in the system, nested inside the reconcile
  transaction — though the gate is now two columns, so the fold saves a round trip
  rather than a blob read.

  Deferred as a family rather than piecemeal: each is a new hand-written `SELECT` list
  or join that has to stay in step with `objectColumns` and `scanObject`, which is a
  maintenance cost the write-shapes pass deliberately did not take on while it was
  changing signatures. None of it changes a contract, so none of it needs a break.

  **Not in this family any more**, both settled: the condition gates (now
  `checkObjectScoped`) and the version drawn before a deletion mark knew it would
  stamp one (now drawn lazily, so a guard-blocked mark writes no counter page —
  pinned by `TestDeletionMarkDrawsAVersionOnlyWhenItStamps`). The one place a version
  draw per row survives is `deletionRequestsCreateFromOwner`, which calls
  `markForDeletion` per child, so an N-child cascade draws N versions where one
  `value + N` draw would do. It only pays on children not already deleting — the
  cascade skips the rest before calling — so the steady-state re-cascade is unaffected
  and this is the first-pass cost of a large subtree only.

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

- **Reads and writes share one connection, so a live watch slows the write path** —
  known, not fixed, and deferred on the safety property it trades away rather than
  on the size of the win. `OpenPool` sets `journal_mode(WAL)`, which lets one writer
  and many readers run at the same time. Then `sqlite.Open` passes `maxConns = 1`, so
  every read queues behind every write in Go's pool and that concurrency is unused.
  The limit is `database/sql`, not SQLite.

  **Measured cost.** `BenchmarkWritesUnderWatch` (`objectswatch_bench_test.go`), on
  disk, beehive not started so no driver competes:

  | Watches | ns/op | p50 | p99 |
  | --- | --- | --- | --- |
  | none | 172,000 | 151 µs | 465 µs |
  | 1 kind, 1 watch | 215,000 | 190 µs | 550 µs |
  | 1 kind, 64 watches | 223,000 | 197 µs | 715 µs |
  | 16 kinds, 1 watch each | 326,000 | 271 µs | 995 µs |

  One watch costs about a quarter of write throughput. Watch *count* is free, which
  is the shared tailer working as designed. Watched *kinds* is the axis that costs:
  the writes round-robin, so each tailer wakes with nothing to coalesce and 16 drains
  contend for the one connection. That row is the worst case by construction — real
  traffic bursts per kind and collapses more. Absolute numbers are machine-specific;
  the deltas are the finding.

  **The fix is two pools, not a larger one.** Raising `maxConns` alone breaks writes:
  the DSN sets `_txlock=immediate`, so concurrent `Within` calls would collide at the
  SQLite level and take `SQLITE_BUSY` with a 5s `busy_timeout` behind it, where today
  they queue in Go. Instead keep a write pool of 1 and add a read pool of N with
  `_pragma=query_only(true)` — `mode=ro` is worse, because a read-only connection
  cannot recover the `-wal`/`-shm` files. `s.conn(ctx)` is already the single place
  connection selection happens, so the change is a sibling `s.read(ctx)` returning
  the read pool, plus moving the read-only methods onto it.

  **What makes this more than a refactor.** Today a read issued outside the
  transaction while inside one deadlocks: it waits for the connection the
  transaction holds. That is loud and deterministic, and it is stated as a
  caller-facing rule in `Client.Watch`'s godoc. With a read pool the same mistake
  becomes a *silent stale read* on a second snapshot. `s.read(ctx)` must return the
  transaction whenever one is present, and that invariant is the whole defence.

  The tests would not exercise the new path. `OpenMemory` uses `file::memory:`, which
  is per-connection, so a second pool there is a different and empty database. The
  read pool has to fall back to the write pool in memory, which means the suite keeps
  today's semantics and only on-disk runs cover the split.

  **Several components budget their work against one connection**, and their
  reasoning would have to be re-read rather than assumed: the waker's page budget
  exists so a resume "cannot monopolise the single connection" (`waker.go:72`, `:76`,
  `:240`), the tailer reads "one after another on the single connection"
  (`objectswatch.go:462`), and `workqueue.go:421` reasons about a deadlock on it.
  This also changes the premise of the page-cache item below, which discounts a
  larger cache because "the store is one connection, so a larger cache is not shared
  across concurrent readers the way the advice assumes".

  Revisit when a deployment is write-bound with watches attached, or when the driver
  count grows enough that read contention shows up without any watch at all. Tripwire:
  `BenchmarkWritesUnderWatch` is the measurement, and its `no-watcher` row is the
  baseline the split should move the others toward. There is no test pinning the
  in-transaction deadlock — it would hang rather than fail — so a `s.read(ctx)` that
  forgets the transaction case would pass the suite today.

- **The page cache and `mmap_size` are untuned, and it is unclear whether tuning them
  buys anything here** — known, not fixed. `OpenPool` sets five pragmas and leaves
  SQLite's stock ~2MB cache and disabled memory mapping alone.

  Two things make this less obviously a win than the standard advice suggests. We run
  on `modernc.org/sqlite`, a pure-Go translation rather than a cgo binding, so whether
  `mmap_size` maps anything at all there is **unverified** — that is the first thing to
  check, and if it is a no-op the item halves. And the store is one connection, so a
  larger cache is not shared across concurrent readers the way the advice assumes; it
  helps only by keeping the drivers' repeated scans off the disk.

  Those scans are the one real argument for it. The owed pass, the stale-dependents
  pass, the waker and the watch floor all re-read the same indexes on a fixed cadence
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

- **Owner-scoped watches** — wanted, deliberately out of the first write-log
  design ([ADR](adr/2026-08-02-object-write-log.md)). `OwnedObjectsList` has a
  typed, batched read but no watch counterpart, so a subscriber that wants one
  owner's children watches the whole kind and filters client-side.

  The obvious implementation is wrong. Denormalising `owner_id` into
  `object_writes` filters on ownership *as of the write*, and ownership can
  change afterwards: a re-parented object keeps arriving on its old owner's
  stream and never appears on its new one, until something writes to it again.
  Confirming each entry against current state costs a read per entry, which is
  most of what the index was there to buy.

  Worth doing when someone has a real fan-out of children per owner. The design
  choice to settle first is whether an ownership change should itself be a write
  to the child — which would make the logged value correct by construction, and
  would also fix `DependentsList`/`DependenciesList`, which have the same hole.

- **`EventsWatch` did not follow the object watches onto the write log** — so
  two watch surfaces on the same `Client` now behave differently for reasons a
  caller cannot see. Object watches return a snapshot plus a stream, resume
  from a `resource_version`, and end with `ErrWatchTooOld` when their entries
  are trimmed. `EventsWatch` still polls, diffs by `EventID`, hands its initial
  runs back through the channel, and cannot resume.

  The asymmetry is not obviously wrong — an event log has no tombstones, so
  absence is not a change, and the poll-and-diff it uses is sound. But the
  argument for splitting the snapshot out ("am I synced?" should be a value, not
  a guess) applies to events unchanged, and a caller reading both surfaces has
  to hold two models.

  Worth doing when events grow a consumer that needs to resume — a UI that
  reconnects, or an exporter. The work is mostly mechanical; the real decision
  is whether events get a retention horizon of their own, since
  `WithEventRetention` already trims runs out from under a reader with no way to
  tell them.

- **The object watch tail has no minimum interval between wake-driven drains** —
  known, not fixed, and a duty-cycle question rather than a query-count one.
  `objectTailer.run` re-arms its floor timer after every drain, but nothing bounds
  how soon the *next* drain may start: a commit landing during a drain refills the
  wake slot, so the loop goes straight around. Under a sustained write load to a
  watched kind the tailer therefore reads as fast as the store can answer, and every
  one of those reads holds the single connection that writers need.

  **It is not spinning on nothing.** `drain`'s position read is the quiet-wake gate —
  a wake with nothing behind it costs one scalar query and no listing — and page
  batching means a burst of 256 writes is read by one drain rather than 256. So the
  cost per write *falls* as the rate rises. What does not fall is the fraction of
  time the tailer is running, and that is the part that competes with writers.

  **The fix is a floor between wake-driven drains** — eager on the first wake after
  a quiet period, so an idle-to-write transition keeps its zero added latency, then
  rate-limited while writes continue. That trades a bounded delay for connection
  headroom exactly where per-write latency is already dominated by queueing.

  **The shared limit now exists and the tailer has not adopted it.** The dependency
  waker took `rategate.Allow` for exactly this, floored at
  `wakeScanMinInterval` — see
  [the ADR](adr/2026-08-05-a-commit-wakes-the-dependency-waker.md). It went there
  first because that loop runs unconditionally for the life of the process, where
  the tailer is demand-scoped: it runs only while something watches, and a caller
  that opened a watch has asked for the latency. Adopting it here is a gate, a
  constant and the `Allow`/`Rearm` pair the waker's loop already uses. Revisit if a
  watched kind under heavy write load is measured to slow its own writers.

  Tripwire: none pins the current cadence. `TestTailerDeliversOnWake` asserts that a
  wake delivers, not how soon, and `TestTailerDrainsBurstAbovePageCap` and
  `TestWatchLoadsAreBatchedPerDrain` assert what a drain *reads*, not how soon the
  next one may start. A throttle under the tests' failsafe timeouts would trip none
  of them — which is itself worth knowing before adding one.

- **The dependency waker's 1s pass is a heartbeat with nothing to find** — known,
  not fixed, and now that the commit wake has
  [shipped](adr/2026-08-05-a-commit-wakes-the-dependency-waker.md) it is the one
  loop in the system that runs unconditionally for the life of the process
  without anything demanding it. In a healthy single-process beehive a lost push
  is close to impossible by construction — the subscription is registered before
  the seed read, the hub closes only at stop, and one slot per receiver means a
  burst cannot drop one — so essentially every tick is a quiet pass: one
  `ObjectWritesListSinceAll` that comes back empty, measured at ~21µs by
  `BenchmarkWakerScanRateUnderSustainedWrites`. That is 86,400 queries a day of
  insurance against an event the push path makes very hard to arrange.

  **The proposal is to arm the timer only when there is a reason to look again**,
  leaving the waker wake-driven: `scanIdle` re-arms nothing and the loop blocks on
  the wake channel, `scanMore` keeps the throttle re-arm, and `scanFailed` re-arms
  a retry. The eager first pass stays — it seeds, and a restart still resumes from
  the persisted cursor on it rather than on a tick.

  **The failure retry is the coupling that has to move first.** `backingOff` drops
  arriving wakes, and today only the floor timer clears it, so a `scanFailed` that
  re-arms nothing wedges the waker until the process restarts. It needs its own
  delay — `driver.Backoff`, as `objectTailer` already uses — and that timer is
  conditional rather than a heartbeat, so it does not reintroduce what this
  removes. Two smaller ones follow: `wakeInterval` also sets the cursor-write
  floor (and through it what `wakePersistRetryCap` means in seconds), which wants
  its own constant; and `wakeInterval <= 0` currently means "no waker at all",
  so a tick that becomes optional needs to be a knob of its own rather than that
  sentinel.

  **What it gives up is the multi-process case, and that is the decision to make
  deliberately.** `signalKindWritten` publishes to an in-process hub, so a second
  process writing the same store produces no wake here: the tick is that
  deployment's only dependency-wake path, and its cadence is that deployment's
  latency. An opt-in periodic pass, off by default, is the shape that keeps both
  — but defaulting every embedded single-process beehive to a 1s loop to serve it
  is the trade worth naming. Correctness is unaffected either way: the
  stale-dependents pass (60s, cannot be disabled) derives staleness rather than
  replaying a cursor, so it finds a superset of what the waker finds, and
  `reconcile_owed` outlives the queue behind it.

  Note the corollary for any interim tuning: a floor at or above
  `staleDependentsInterval` is strictly redundant, since the backstop runs at that
  cadence and finds more. The tick only earns its keep strictly faster than 60s.

  Tripwire: none pins the cadence. `TestWakerScansWhenAWriteCommits` and
  `TestClientOnlyTargetWakesDependent` assert that a wake and a tick each reach
  the dependent, not that the tick runs at all, so removing it would trip neither.
  What a change here must add is the pair nothing covers today: that a failed scan
  recovers with no heartbeat behind it, and that an idle beehive issues no waker
  queries at all.

- **`rategate`'s eviction copies the whole live set, and only runs on `Admit`** —
  known, not fixed, and unmeasured at the cardinality where it would matter. The
  package holds each key for a fixed interval and bounds its map by *admissions
  within one interval* rather than by the key space, which is the invariant a
  per-key limiter map usually gets wrong. Three things about how it does that are
  worth revisiting, in this order:

  **The compaction is O(live) per eviction round.** `evict` ends with
  `append(g.order[:0], g.order[i:]...)`, which copies the entire live tail
  whenever even one entry has expired, and it runs on every `Admit`. That is O(n)
  per admission where it should be amortised O(1). For the work queue's gate
  (interval 1s, key `ObjectID`, entry 8 + 24 bytes), 1,000 distinct objects
  dispatched per second is ~32KB copied per admission, ≈32MB/s of memmove;
  10,000/s is ≈3.2GB/s, which is not survivable. What keeps it off the floor
  today is that the store's single connection cannot feed reconciles at that
  rate — a coincidental bound with a narrower margin than it looks. **The fix is
  a head index** slid forward on eviction, compacting only when it passes half
  the slice; same semantics, no API change.

  **Memory is reclaimed only inside `Admit`.** A kind that dispatches ten thousand
  objects and then goes quiet holds their records until its next admission, which
  may never come. It cannot grow past one interval's admissions, so this is
  retention rather than a leak, but nothing returns it. `OpensAt` could evict
  opportunistically without breaking its documented "records nothing" contract —
  evicting is not recording.

  **Each entry carries a `time.Time` twice**, in the map value and the queue
  entry: 24 bytes each, and the `loc` pointer is the only part of this package
  the GC has to trace. An internal monotonic int64 would halve it. Micro, and
  last.

  Deferred because nothing here is on a measured path. The one benchmark that
  touches the package — `BenchmarkWakerScanRateUnderSustainedWrites` — drives the
  waker's gates, which hold a single key each and so exercise none of this; the
  ~40ns alloc-free `Admit` recorded during review was that same single-key case.
  **The missing evidence is a benchmark of the work queue's gate at 1k and 10k
  distinct keys per interval**, and it should come before the fix rather than
  after it.

  Related, and cheaper: the waker holds two single-key gates
  (`scanGate`, `persistGate`) that each carry a map and an eviction queue for one
  key. A `rategate.Single` — one instant, no map, `Allow(now)` — would drop both,
  and a third single-key consumer would settle it. Note that none of this is an
  argument for a token bucket: a per-key `rate.Limiter` map inherits the same key
  space and usually ships no eviction at all.

  Tripwire: none. `internal/rategate`'s tests pin semantics — hold, expiry,
  re-admission, `Forget` — and say nothing about cost, so every change proposed
  here would keep them green.
