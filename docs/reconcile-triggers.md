# How an object becomes owed a reconcile

This document lists every way an object can come to owe a reconcile pass. For each
way it gives the durable record, the driver that finds it, and the behaviour after a
restart.

This is a coverage map. It is not a design record. The decisions are in
[the drivers ADR](adr/2026-07-28-periodic-scan-drivers.md) and
[the stamp-every-new-edge ADR](adr/2026-07-29-stamp-every-new-dependency-edge.md).

The document exists because no single file holds the guarantee. A write leaves a
record in one file. A driver in a second file finds it. A third file decides what
happens after a restart. If you read only one of them, the system looks more
push-driven than it is.

**Keep this document in step with the code.** When you add a way for work to be
owed, add it here. Give its record, its driver, and its restart behaviour. Do not
list gaps here. A gap belongs in [`TODO.md`](TODO.md), linked from the case it
affects.

## 1. Push and pull

Two words are used throughout this document. They have exact meanings.

**Pull** means a driver runs on a timer. The driver reads the store. It finds work
that a write left behind. A pull needs no help from the writer. It works after a
restart, and it works when another process made the write.

**Push** means a write starts the work itself, at commit time. A push holds no
state in the store. It puts an object in a work queue in memory.

**Every push has a pull behind it. A push is never the only route to a reconcile.**

This rule is the centre of the design. A push lives in memory only. A crash between
the commit and the dispatch discards it. Thus a lost push must cost time, and must
never cost correctness. The durable record and its driver are what make that true.

### The push paths

There are exactly six push paths that cause a reconcile. All use
`Store.AfterCommit`, so a rollback discards them, and none can run before the row
can be read.

| Push path | Made by | Starts | Gate |
|---|---|---|---|
| A spec write enqueues its own object | `clientImpl.signalSpecWritten` | the object that was written | the store reports `changed` |
| A new edge enqueues the edge's source | `ControllerClient.DependenciesAdd` | the source of the new `depends_on` edge | `EdgesAddResult.ReconcileOwedStamped` |
| A delete request enqueues its own object | `clientImpl.signalDeletionRequested` | the object that was marked | the store reports `marked` |
| A cascade enqueues the children it marked | `Beehive.signalRequeueManyNow` | each newly-marked owned child | `DeletionCascadeChild.Marked` |
| A cleared finalizer enqueues its own object | `ControllerClient.FinalizersDelete` | the object whose block it lifted | the store reports `clearedLast` |
| A physical delete enqueues the owners it unblocked | `Beehive.gcCollect` | each deletion-pending owner of the deleted row | the owner's `deletion_requested_at` |

Five of the six are immediate: their gate is a store write that lands once, so
they carry new information and cancel a pending alarm rather than being absorbed by
one. The new edge is the exception, and it is throttled because a controller can
declare the same edge on every pass, so it goes through `work.add` and respects the
source's backoff ladder and re-enqueue floor.
→ [the floor ADR](adr/2026-08-04-work-queue-re-enqueue-floor.md).

The physical delete is the one push gated on a *different* object than the one it
enqueues, and the one whose fan-out is N→1 — N children converging on one owner. Its
gate reads the owner rather than the write's own result, because the delete itself
lands once per *row* and rows are unbounded: an ungated push would let a controller
that replaces an owned child each pass drive itself, with the floor bypassed.
→ [its ADR](adr/2026-08-05-a-physical-delete-pushes-its-owner.md).
Cases 1, 5, 9, 10 and 11 describe them in full.

Every push is confined to a registered kind: each resolves a reconciler inside its
own hook, and a client-only kind resolves to none. So a client-only object waits for
a driver, always.

A target change is the one wake that starts anywhere. The commit wakes the
dependency waker, whose scan is store-wide, so a *client-only* target reaches its
dependents — but what the waker enqueues is still per registered kind. It is the
one wake with no tick behind it: the stale-dependents pass is its backstop
instead. See case 6.

Nothing else pushes a reconcile. `Client.Requeue` is an explicit call by the
embedder, not a write, and it is case 13.

### The pull drivers

| Driver | Scope | Cadence | Can it be turned off? |
|---|---|---|---|
| Owed pass | per kind | 30s | no |
| GC sweeper | global | `WithGCInterval`, 30s | no; a non-positive value is rejected |
| Dependency waker | global | a commit wake; no tick at all | yes, by an unexported option |
| Stale-dependents pass | global | 60s | no |
| Full pass | per kind | `WithFullPassInterval`, off | yes; it is off by default |

The full pass is not a coverage mechanism. Section E explains why.

## 2. The durable records

There are five durable records. A sixth mechanism records nothing at all.

| Record | Where it lives | Listed by | Driver |
|---|---|---|---|
| The spec has not converged | `generation` and `observed_generation` | `ObjectsListUnsettledIDs` | owed pass |
| A wake is owed | `reconcile_owed` | `ReconcileOwedListIDs` | owed pass |
| A delete was requested | `deletion_requested_at` | `DeletionRequestsList` | GC sweeper |
| An object changed | a row in `object_writes` | `ObjectWritesListSinceAll` | dependency waker |
| A dependency moved | `dependency_watermarks.reconciled_against` and the target's `resource_version` | `DependentsListStaleSince` | stale-dependents pass |
| *(none)* | in memory only | — | `workQueue` |

**Two of these records are predicates. One is a cursor. The difference matters.**

Records 1, 2, 3 and 5 are predicates. Each names a condition that means "work is
owed". A driver selects rows that satisfy the condition. Thus a restart finds the
work again, because the condition is still true.

Record 4 is different. There is no value of `resource_version` that means "work is
owed". The record is the existence of a row in `object_writes`. The waker reads
`resource_version` only to order the rows and to resume a scan. Section B, case 6
explains this in full.

Because record 4 is a cursor, a restart finds only what is above the watermark the
waker starts from. That is why record 5 exists. Record 5 is the backstop that makes
record 4 an optimisation.

The `workQueue` record does not survive a restart at all.

Record 5 has a second writer that only *clears*. A new `depends_on` edge drops the
dependent's watermark. A cursor recorded over a smaller dependency set cannot speak
for a target that was just added. See case 7. The clear records nothing new. The
same declaration also makes case 5's stamp, and that stamp is the guarantee.

## 3. What a restart guarantees

Startup runs exactly three things:

1. `reconciler.enqueueOwedPass`, in `reconciler.run`, before the workers start.
2. The GC sweeper's first sweep, which `driver.Run` runs at once.
3. The stale-dependents pass's first sweep, which `driver.Run` also runs at once.

The first two always run. The third runs when any kind is registered. That is the
only case in which it can enqueue anything.

That is the whole of what a restart guarantees.

## 4. The full pass is not in this document

Neither `WithFullPassInterval` nor `WithStartupFullPass` is on by default. **No
reconcile may depend on either.** Both scale with the object count, not with the
work outstanding. A guarantee that rests on one holds only while the sweep stays
affordable.

The full pass is a tool to re-confirm process-scoped state. See
[section E](#e-not-triggers). It is not an answer to the question "how is this work
found?". If the only answer for a path is "the full pass finds it", that path is a
defect. Record it in [`TODO.md`](TODO.md). Do not record it as coverage here.

---

## A. Spec-derived

Cases 1 to 4 leave the same record and are found the same way. The coverage
argument is shared.

**Record:** `generation` is ahead of `observed_generation`.

**Push:** the write itself, at commit. `clientImpl.signalSpecWritten` registers an
`AfterCommit` hook that enqueues the row. The hook is gated on the store reporting
that the write changed the object. That is the `changed` value the two
`ObjectsUpdateSpec*` mutators return. A create always changes the object. A
byte-identical update changes nothing and enqueues nothing.
See [the ADR](adr/2026-07-31-a-spec-write-enqueues-its-own-object.md).

**The push must not be gated on the row being unsettled.** That was the first
design and it was wrong. A failing reconcile leaves the row unsettled for a long
time. A controller that re-applies its own spec would then pass the gate on every
pass. The enqueue is worse than a scan here, because `requeueNow` cancels the
backoff alarm and marks the in-flight id dirty. `work.done` then dispatches it at
once. `TestFailingRespecControllerKeepsItsBackoff` pins this.

A write that composes with another still enqueues a duplicate. A spec write
followed by its `UpdateStatus`, in one outer `Within`, commits a settled row and
still enqueues. The signal reports the write, not the final state. The extra
dispatch is harmless and intended.

**Pull:** `reconciler.enqueueUnsettled` calls `ObjectsListUnsettledIDs`, which
selects `observed_generation IS NULL OR observed_generation < generation`. The owed
pass runs it every 30 seconds.

**Restart:** covered. `reconciler.run` calls `enqueueOwedPass` before its workers
start. It is not gated on `startupFullPass`. A write made after `Register` but
before `Start` still enqueues, because `Register` builds the work queue and the item
waits there. A write to a kind that was never registered resolves no reconciler. The
owed pass covers that case.

**Tests:** `TestIntegrationStartupEnqueuesUnsettled`,
`TestStartupAlwaysDrainsOwedWork`, `TestFailingRespecControllerKeepsItsBackoff`,
`TestNoOpUpdateOnAnUnsettledObjectEnqueuesNothing`.

### 1. Create

`Create` and `GetOrCreate` insert the row with `generation` 1 and
`observed_generation` NULL, in `sqliteStore.objectsCreate`.

Tests: `TestIntegrationCreateTriggersReconcile`,
`TestClientGetOrCreateOwesAPassOnlyOnCreate`, `TestCreateEnqueuesItsFirstReconcile`.

### 2. Spec update

`ObjectsUpdateSpec` bumps `generation`. The bump is suppressed when the bytes are
equal at the same schema version. That is what stops a controller re-applying its
own spec from waking itself forever.

Tests: `TestObjectsUpdateSpecBumpsGeneration`, `TestClientNoOpUpdateOwesNothing`,
`TestSpecWriteEnqueuesNothingOnRollback`.

### 3. Schema-version re-stamp

Equal bytes at a *different* spec version pass through the no-op gate and bump the
generation. Bytes in a different shape are not comparable.

Tests: `TestCrossVersionWriteIsNotANoOp`, `TestNoOpWriteStampsUpwardWhileConverging`.

### 4. A stale status write makes the object unsettled again

`UpdateStatus` can report an `observedGeneration` behind the current one. On the
content path the store writes it without a clamp. The object becomes unsettled
again, and the stale status is derived again.

Tests: `TestUpdateStatusChangedStaleGenerationUnsettles`,
`TestUpdateStatusAcceptsStaleGeneration`.

## B. Dependency-derived

There are three mechanisms. The split between them is the whole design.

- Case 5 is a durable record made when an edge is declared.
- Case 6 is a fast scan that covers every ordinary change to a target.
- Case 8 records nothing. It derives staleness from current state, and recovers what
  the other two lost.

Case 7 is a companion to case 5. It records nothing and covers nothing on its own.

### 5. The new-edge stamp

**Record:** `reconcile_owed` on the dependent.

Every `depends_on` edge that `EdgesAdd` creates increments the dependent's
`reconcile_owed`. Self-edges are excluded. The increment is in the same statement
sequence as the edge insert. Thus the two cannot come apart.

The stamp is unconditional. Recorded owed work is the only mechanism that is sound
under every interleaving. An increment that lands while the dependent's own pass is
in flight sits above the count that pass read at load. Thus the load-scoped
decrement cannot consume it.

This covers three races:

- The declare race. The target moved between the caller's read and the declaration.
- The quiet-target declare. The target did not move at all. See case 7.
- The mid-pass third-party declare. Another writer declared during the dependent's
  own pass. This used to lose the wake.

The price is one reconcile for each edge ever created. The edge-new gate bounds it.
See [the ADR](adr/2026-07-29-stamp-every-new-dependency-edge.md).

**Push:** the declaration enqueues the source. `Beehive.signalRequeueNow`/`signalRequeueThrottled` runs on
`Store.AfterCommit`. It is gated on `EdgesAddResult.ReconcileOwedStamped` and routed
by `EdgesAddResult.From`. The route matters because the edge is cross-kind. The
source's kind can differ from the declarer's kind.

**The enqueue is throttled**, so a source whose edge set never converges keeps its
backoff ladder, and a declaration made inside the source's own pass is held to that
object's re-enqueue floor rather than dispatched at once. The stamp is durable, so
the owed pass carries the dependent either way.

The push does not cover two cases. A source whose kind has no reconciler is not
enqueued. A declaration made from another process, or through the embedder's own
`Store`, is not enqueued. Both keep the stamp and wait for the pull.

**Pull:** `reconciler.enqueueReconcileOwed` calls `ReconcileOwedListIDs`, every 30
seconds. `ReconcileOwedDecrement` drains the count in `typedController.reconcile`.
It subtracts the whole count observed at load, not one.

A listing that names an object already parked on its backoff alarm no longer
dispatches it: the alarm absorbs the add, so the ladder holds at every rung. That
is what makes `WithMaxRetryInterval` mean what it says above 30 seconds.

**Restart:** covered. The stamp is durable and is drained at startup without a gate.
A crash between the commit and the dispatch loses the push and keeps the stamp. That
is what the stamp is for.

**Tests:** `TestDependencyRequeueLostAcrossRestart` (run under
`WithStartupFullPass(false)`, so the full pass cannot hide the result),
`TestEdgesAddStampsReconcileOwed`, `TestRefsAddStampsOnlyNewEdge`,
`TestAddDependencyEnqueueRoutesByTheSourcesKind`,
`TestANewEdgeOnAnInFlightSourceRespectsTheBackoff`,
`TestReconcileMidPassDeclareLeavesTheDependentOwed`.

### 6. An ordinary target change

**Record:** a row in `object_writes`. The store appends one row for each committed
write, inside that write's transaction.

**How the waker decides.** The waker reads two tables:

1. `object_writes` says which objects changed.
2. `edges` says which objects depend on them.

`waker.scan` calls `ObjectWritesListSinceAll` above a watermark. The read is
store-wide, not per kind, because a `depends_on` edge can point at a kind that no
per-kind query can name. `dependentsWake` then calls `EdgesGroupIncomingByID` with
`RelationDependsOn`. One query resolves a whole page. Each dependent is enqueued
under its own kind.

**The waker does not read a `resource_version` value to decide anything.** It uses
`resource_version` in one place only: `WHERE resource_version > ? ORDER BY
resource_version LIMIT ?`. The version orders the scan and resumes it. It is not a
predicate. Compare case 8, where a comparison of two versions is the whole test.

**A log row for a create or an update carries no payload.** Thus the waker cannot
filter on what changed. Any row for target T wakes every dependent of T.

**Only `depends_on` edges wake.** An `owned_by` edge wakes nothing.

**Push:** the commit wakes the waker. `signalKindWritten` publishes on
`Store.AfterCommit`, and the waker subscribes across every kind — an edge can point
at one it cannot name. The wake carries nothing: the waker reads its own cursor, so
a burst collapses into one wake and a lost one costs only latency. Wake-driven scans
are floored at 100ms, and dropped while a failed scan is backing off.
See [the ADR](adr/2026-08-05-a-commit-wakes-the-dependency-waker.md).

**Pull:** none. The waker holds no timer while it is idle, so a write this process
did not publish — a second process, or a write issued straight to the `Store` —
reaches its dependents through case 8 rather than here.
See [the ADR](adr/2026-08-05-the-waker-is-wake-driven.md).

A failed page holds the cursor, and the retry — `driver.Backoff` from 100ms up to
the stale-dependents cadence — reads the same range again. A failed edges lookup
does the same. The self-edge is skipped. A wake that arrives during a reconcile
is held by the `workQueue` dirty bit, and `done` dispatches it again.

**Restart:** covered by case 8. This mechanism resumes rather than always reseeding.
`seed` reads a cursor the waker persisted in `driver_cursors` and resumes there,
instead of at `ObjectWritesMaxVersionAll`. Thus a change committed while the process
was down is scanned on the eager first pass back.

Case 8 is still the guarantee. Three things bypass the cursor:

- A store that does not implement `DriverCursorer`.
- The first start of a fresh store.
- A wake that was queued but never delivered. The cursor records what was
  *scanned*, never what was woken.

A dependent stranded in any of those ways is invisible to every owed-work listing,
because its own generation never moved. The cost is time, never divergence.
See [the ADR](adr/2026-07-30-durable-waker-cursor.md).

**What bumps a target's `resource_version`**, and thus wakes its dependents, is
wider than a spec change. The full list is `ObjectsCreate`, `ObjectsUpdateSpec`, the
content and handshake-only paths of `ObjectsUpdateStatus`, `ConditionsSet`,
`ConditionsDelete`, `FinalizersDelete`, `markForDeletion`, and the cascade mark.
`EventsAdd` is the one write that does not bump it. That is by design.

This list is scoped to writes on a target that can still be depended on.
`ObjectsDelete` also draws a version and appends a write-log entry, but a physical
delete removes the row, and `edges.to_id` is `ON DELETE RESTRICT` — so a deleted
target structurally cannot have a live `depends_on` edge pointing at it left to wake.
The write-log entry it leaves is bookkeeping, not a wake; the actual notification for
a delete is the owner push in case 11.

A target of a **client-only kind** is covered. The scan is store-wide for exactly
this reason.

**Tests:** `TestWakerScanWakesDependentsByTheirOwnKind`,
`TestWakerSeedsFromTheStoredCursor`, `TestWakerHoldsTheWatermarkOnLookupFailure`,
`TestWakerSkipsTheSelfEdge`, `TestClientOnlyTargetWakesDependent`.

### 7. The watermark clear on a new edge

This is a companion to case 5. It is not a coverage mechanism of its own.

`EdgesAdd` clears the dependent's `dependency_watermarks` row when it creates a new
`depends_on` edge. A cursor recorded over a smaller dependency set cannot speak for
a target that was just added. If the row stayed, it would report convergence to case
8's scan until the stamped pass runs. An absent row already means stale. Thus
nothing new is recorded.

The clear used to be the only route to a declaration against a quiet target, because
the old stamp was conditional and did not fire there. That route had a hole shaped
like a strand. A third party could declare between a dependent's load and that
dependent's own watermark write. The pass then undid the clear, and that pass never
saw the new target. Case 5's unconditional stamp closes the hole. Recorded owed work
survives a pass. Invalidated derived state does not.
See [the ADR](adr/2026-07-29-stamp-every-new-dependency-edge.md).

**Record:** none. The mechanism *un*-records, inside `sqliteStore.EdgesAdd`, in the
same statement sequence as the edge and the stamp.

**Push and pull:** both are case 5's. Case 8's scan reads the absent row until case
5's pass writes it again.

**Restart:** covered by case 5's stamp. The cleared row, and its absence, are
durable.

**This costs the declarer's own pass nothing, with one exception.** A controller
that declares inside its own `Reconcile` writes the watermark when the pass
succeeds, from the cursor it loaded at. That is sound, because its read of the new
target happened after that load. The exception is the object's *first* `depends_on`
edge. `ReconcileLoad.HasDependencies` was sampled before the edge existed, so the
pass skips the write and the object is found stale one time. This happens once for
each object, and it stops itself.

**Tests:** `TestRefsAddClearsTheDependentsWatermark`,
`TestRefsAddKeepsTheWatermarkOnAReDeclaredEdge`,
`TestReconcileSkipsTheWatermarkWhenTheFirstDependencyIsDeclaredMidPass`.

### 8. A dependency moved and nobody noticed

This is the backstop under case 6. It is the only mechanism here that records
nothing. It asks current state whether each dependent has reconciled against its
targets' latest versions. Thus it recovers a wake lost by any means: a crash, a
startup seed race, a process with no waker, a write nobody published — a second
process, or one issued straight to the `Store` — or a defect in the wake path.

**Record:** `dependency_watermarks.reconciled_against`, written by
`Store.DependencyWatermarksSet`. `typedController.reconcile` writes it on every
successful pass of an object that has dependencies. The value is the write cursor as
of the pass's *load*. It is never the end of the pass.

**Push:** none, and none is possible. This mechanism exists to find what every push
lost. A push form of it would have to record its findings at commit time, which is
the moment it cannot trust. A commit knows the target moved. It does not know which
dependents failed to observe the move, because that answer is a comparison against
each dependent's watermark, and those watermarks move under other transactions.

**Pull:** `staleDependents.sweep` calls `DependentsListStaleSince`, paged to
exhaustion, every 60 seconds. The sweep stamps each finding's `reconcile_owed`
before it enqueues the finding. Thus a finding outlives the queue. The `through`
bound is the mark read before the scan. That bound is what keeps a sweep finite
under sustained writes.

The pass is slower than the waker on purpose. It is the guarantee, not the latency.
The scan is bounded by a cursor over the target `resource_version`, so its cost is
what changed, not the size of the graph. A tick finds nothing to list when nothing
has been issued since the last sweep.

**Restart:** covered. Every process re-derives once. The cursor is process-local and
is never persisted. Thus a wake lost only in memory is found again on the next
start, and a changed kind set needs no special handling.

A *lost watermark write* needs no repair. It leaves the watermark low.
`reconciled_against` is read in one place, and a lower value selects more. Thus a
target change that the reconcile did not observe is above the cursor, and the next
sweep lists it.

Inside one process the cursor moves only when a sweep reaches the end. Thus a failed
page is read again rather than skipped.

**Not covered:** a dependent whose kind has no controller. The scan excludes it,
because there is no loop to enqueue into and it would be stale forever. It is found
on the first pass after its kind is registered. The kind filter applies to the
*dependent* only. A registered dependent of a client-only target is always in scope.

**Tests:** `TestDependencyWakeSurvivesRestart`,
`TestStaleDependentsSweepStartsEveryProcessAtTheBeginning`,
`TestStaleDependentsSweepLeavesADurableFinding`,
`TestALostWatermarkStillFindsAnUnobservedChange`,
`TestReconcileRecordsCursorFromTheLoad`,
`TestDependentsListStaleSinceTreatsMissingWatermarkAsStale`.

## C. Deletion-derived

Cases 9, 10 and 11 share one record and one driver.

**Record:** `deletion_requested_at`.

**Push:** four, all registered-kind only (cases 9, 10, and 11's routes 1 and 2). A
client-only object is marked and left to the sweeper: `deletionAdvance` collects one
directly, and running that from a commit hook would put the whole subtree below it
on the caller's goroutine.

**Pull:** `Beehive.deletionPendingSweep` calls `DeletionRequestsList`, which is
kind-agnostic. `deletionAdvance` routes each result. A registered kind is
**enqueued**, so its controller can clear finalizers. A client-only kind is
collected directly by `gcCollect`. The routing is correctness, not speed.
`gcCollect` cannot clear a finalizer, so calling it on a registered kind would never
make progress.

The GC sweeper runs every 30 seconds. `WithGCInterval` rejects a non-positive value.
Thus every error path has a next tick.

**Restart:** covered. `driver.Run` sweeps once before its first tick.

**Tests:** `TestIntegrationGCResumesDanglingDeleteOnStartup`,
`TestGCSweepDispatchesRegisteredKind`, `TestIntegrationGCSweepsClientOnlyKind`,
`TestWithGCIntervalRejectsNonPositive`.

### 9. A delete request

`Delete` and `DeleteByName` stamp `deletion_requested_at`, then enqueue the object
at commit through `signalDeletionRequested`. The gate is the store's `marked`, so a
retry pushes nothing; the mark is once per object, so the push cannot repeat on a
pass. `DeleteByName` pushes the row the name resolved to.

Tests: `TestDeleteEnqueuesItsOwnObject`, `TestDeleteByNameEnqueuesItsOwnObject`,
`TestRepeatedDeleteEnqueuesOnce`, `TestIntegrationDeleteTriggersReconcile`,
`TestIntegrationDeleteCollectsWithoutThePush` (the pull path, which marks through
the store so no push is issued), `TestDeletionRequestsCreateIsIdempotent`.

### 10. Cascade to owned children

`gcCollect` marks the children with `DeletionRequestsCreateFromOwner`, then enqueues
the ones it marked in a single commit hook. Thus a cascade advances one level per
commit, for as long as the levels are registered kinds; a client-only level costs a
sweep, and the pushes below it wait on that level's own collect.

The gate is `DeletionCascadeChild.Marked` — the guarded `UPDATE`'s own answer, not
"was it already deleting". Without it the push would fire at reconcile rate, since
`gcCollect` reruns after every pass over a deleting object.

The gate is also what makes the push immediate: a mark lands once per child, so
cancelling that child's pending alarm cannot become a repeat. Absorbing it instead
would park the child behind a backoff ladder that can outlast the GC interval — and
the child's own reconcile is what cascades to the level below it, so the subtree
would wait with it. The sweeper's route absorbs the same way, which is why the
ladder, not the tick, is what a stalled cascade waits on.

Tests: `TestCascadePushesEachMarkedChild`, `TestCascadePushesOnlyNewlyMarkedChildren`,
`TestCascadeSkipsClientOnlyChild`, `TestIntegrationGCCascadeDeletesOwnerAndChild`,
`TestCollectCascadesAndBlocksOnChild`,
`TestDeletionRequestsCreateFromOwnerCascadesThenIsNoOp`.

### 11. A blocked collect retries by staying in the listing

A collect is blocked when finalizers are still pending, or when `EdgesHasIncoming`
reports a referrer under RESTRICT. Three routes lead out of one, and **the first two
push**:

1. **The last finalizer was cleared.** `ControllerClient.FinalizersDelete` enqueues
   the object at commit, gated on the store reporting `clearedLast`. →
   [the ADR](adr/2026-08-05-a-cleared-finalizer-pushes-its-own-collect.md).
2. **The last child was removed.** `gcCollect` reads the dying row's `owned_by`
   edges before deleting it — after that the owner is unnameable, since
   `edges.from_id` is `ON DELETE CASCADE` — and enqueues the deletion-pending ones
   at commit. Two filters, both load-bearing: the relation, because
   `EdgesHasIncoming` already discounts a `depends_on` edge from a deletion-pending
   source, so those targets are not blocked; and the owner's own
   `deletion_requested_at`, because a live owner was never blocked and pushing one
   would spin. →
   [the ADR](adr/2026-08-05-a-physical-delete-pushes-its-owner.md).
3. **`DependenciesDelete` dropped the last referrer.** An edge write bumps no
   `resource_version` and appends no write-log entry, so no cursor in the system can
   see it at all.

Route 3 waits for the next sweep; its entry in [`TODO.md`](TODO.md) has the fix.

The push is a probe about *which* referrer went, not a verdict: route 2 pushes every
deletion-pending owner without checking that this child was the last one, and
`gcCollect` re-checks the block itself.

The push is a probe, not a verdict: `gcCollect` re-checks the RESTRICT block, and the
sweep remains the route after a crash.

Every block is temporary by construction. The one block that was not is a finalizer
on a client-only kind, which no `FinalizersDelete` can reach. That is now rejected at
create time. Thus a sweep always has a route to progress.

Tests: `TestFinalizersDeletePushesTheCollect`,
`TestFinalizersDeletePushesNothingOtherwise`,
`TestIntegrationClearedFinalizerCollectsWithoutASweep`,
`TestIntegrationClearedFinalizerCollectsWithoutThePush`,
`TestPhysicalDeletePushesItsOwner`, `TestPhysicalDeletePushBeatsAPendingAlarm`,
`TestPhysicalDeletePushesNoLiveOwner`, `TestPhysicalDeletePushesNothingWhenBlocked`,
`TestPhysicalDeletePushesNothingForAnOrphan`,
`TestPhysicalDeletePushesAcrossKinds`, `TestPhysicalDeleteSkipsClientOnlyOwner`,
`TestPhysicalDeletePushesEveryOwner`, `TestPhysicalDeleteQueuesNoDependsOnTarget`,
`TestIntegrationLastChildCollectsItsOwnerWithoutASweep`,
`TestIntegrationLastChildCollectsItsOwnerWithoutThePush`,
`TestCollectKeepsFinalizedObject`, `TestCollectDeletesOwnerAfterChildGone`,
`TestIntegrationGCDeleteDependencyUnblocksTarget`,
`TestClientCreateRejectsFinalizersOnUnregisteredKind`.

## D. In-memory only

Cases 12, 13 and 14 leave no record. `workQueue.stop` cancels every pending timer at
shutdown.

**Restart: lost.** An object is recovered only if it also carries a durable record.
That is true for the two cases that matter. A reconcile that failed on an
unconverged spec leaves the object unsettled, which is section A. A reconcile that
failed while servicing a wake keeps its `reconcile_owed` count, because
`ReconcileOwedDecrement` runs only on success, which is case 5. A retry for a
*settled* object with neither record has nothing to recover it.

### 12. `Result.RequeueAfter`

`runWorker` calls `workQueue.addAfter`.

A chain of these on a settled object is the one case with no durable record at all.
It does not survive a restart, and no driver brings it back. **This is accepted, not
planned work: beehive will not add a durable form of `RequeueAfter`.** A
`RequeueAfter` chain is a controller's private timer, not a fact about the object,
and persisting it would mean a row per poll on the single connection —
exactly the cost `reconcile_owed` exists to avoid. See [`TODO.md`](TODO.md) for the
full reasoning. The open question left there is narrower: whether a self-polling
controller should be written this way at all, or should own its own ticker and call
`Client.Requeue`, or should enable `WithFullPassInterval` instead.

Tests: `TestReconcilerRequeueAfter`, `TestWorkQueueAddAfterNewestWins`,
`TestTypedControllerReconcileDropsRequeueWhenCollected`.

### 13. Failure backoff

`reconciler.backoffNext` calls `addAfter`. The delay doubles up to
`maxRetryInterval`. Only a successful reconcile clears it.

Tests: `TestReconcilerRequeuesOnError`, `TestReconcilerClearsBackoffOnSuccess`,
`TestReconcilerRequeueBackoffLadder`.

### 14. `Client.Requeue`

`reconciler.requeue` calls `workQueue.requeueNow`, which cancels any pending alarm.
This is the public way to beat a cadence.

Tests: `TestClientRequeue`, `TestClientRequeueNoController`,
`TestWorkQueueRequeueNow`.

## E. Not triggers

Each item below looks like a trigger. None of them is one.

- **The full pass is not a coverage mechanism.** `enqueueAll` calls
  `ObjectsListIDs` and dispatches every object of the kind. It runs once at startup
  under `WithStartupFullPass`, and on a timer under `WithFullPassInterval`. Both are
  off by default. Its job is the one thing no owed-work listing can express:
  re-confirming state that belongs to a *process* rather than to the store. An
  example is a liveness condition that reads "verifying" until a controller in this
  process writes it again. The full pass does dispatch settled objects that other
  mechanisms failed to reach. **Do not lean on that property.** It turns an open gap
  into an intermittent gap, scaled by the object count, and hides the gap from
  anyone reading this document. Tests: `TestDefaultConfigDoesNotFullPass`,
  `TestStartupFullPassDisabledSkipsSettled`, `TestFullPassTickReconcilesSettled`.
- **A status or condition write does not wake that object's own controller.** The
  waker skips the self-edge on purpose. A spec write already leaves the object
  unsettled, and a status write came from the pass that just ran. Test:
  `TestWakerSkipsTheSelfEdge`.
- **`EventsAdd` wakes nothing.** It bumps no object `resource_version`. That is also
  what makes it the one write that is safe inside a dependency cycle.
- **A schedule change wakes nothing.** `SchedulesWatch` reports an in-memory gauge.
  It bumps no generation and no `resource_version`. It is delivered by a push hub
  rather than by a poll. That changes how a subscriber learns of it. It changes
  nothing about what it triggers.
- **The object watch tail wakes no controller.** `signalKindWritten` feeds the watch
  tailers and the dependency waker. The tailer half never enqueues a reconcile; the
  waker half does, and it is case 6. Do not confuse the tailer with the push paths in
  section 1.
- **`EventsWatch` wakes nothing.** It polls the event log for subscribers. It
  enqueues no reconcile.
- **An object of a client-only kind is never reconciled.** It has no reconcile loop.
  An unsettled spec on one is inert. Only its deletion is acted on, by the sweeper.
- **A queued id whose row is gone is a successful no-op**, not a retry. `ObjectsGet`
  returns `ErrNotFound` and the worker drops the item. Test:
  `TestTypedControllerReconcileMissingIDIsTerminal`.
- **An undecodable row is quarantined**, not retried. It is skipped as a successful
  no-op, so the worker drops it. Its `reconcile_owed` count is deliberately left
  standing. Tests: `TestTypedControllerReconcileRawToTypedError`,
  `TestTypedControllerReconcileQuarantineKeepsReconcileOwed`.
