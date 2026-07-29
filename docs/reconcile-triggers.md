# How an object becomes owed a reconcile

Every way an object can come to owe a reconcile pass, what records it, which driver
finds it, and whether that survives a restart. This is a coverage map, not a design
record — the decisions behind it live in [the drivers ADR](adr/2026-07-28-periodic-scan-drivers.md)
and [the stamp-every-new-edge ADR](adr/2026-07-29-stamp-every-new-dependency-edge.md).

It exists because the guarantee is distributed: no single file holds it. A write
leaves a trace in one column, a driver in another file scans for it, and whether a
restart still finds it depends on a third. Reading any one of those in isolation makes
the system look more push-driven than it is.

**Keep it in step with the code.** When you add a way for work to be owed, it belongs
here with its recording site, its finding site, and its restart answer. Gaps are not
listed here — they are in [`TODO.md`](../TODO.md), and linked from the case they
belong to.

## The shape of the answer

Nothing is pushed, and **no write schedules a reconcile**. A write leaves a durable
trace and a periodic driver finds it. There are exactly five traces, plus one
mechanism that has no trace at all:

| Trace | Column | Listed by | Driver |
|---|---|---|---|
| Spec not converged | `generation` vs `observed_generation` | `ObjectsListUnsettledIDs` | owed pass, per kind, 30s |
| Wake owed | `reconcile_owed` | `ReconcileOwedListIDs` | owed pass, per kind, 30s |
| Deletion requested | `deletion_requested_at` | `DeletionRequestsList` | GC sweeper, global, 30s, **cannot be disabled** |
| Anything changed | `resource_version` | `ObjectWritesListSince` | dependency waker, global, 1s |
| A dependency moved | `dependency_watermarks.reconciled_against` vs targets' `resource_version` | `DependentsListStale` | stale-dependents pass, global, 60s, **cannot be disabled** |
| *(none)* | — | — | `workQueue`, in memory only |

The first three and the fifth are re-derived from state, so a restart finds them by
definition. The fourth is a cursor scan, so a restart finds only what is above the
watermark it seeds with — which is why the fifth exists: it is the backstop that makes
the fourth an optimisation. The last does not survive a restart at all.

The fifth has two writers, and the second one only *clears*: declaring a new
`depends_on` edge drops the dependent's watermark, because a cursor recorded over a
smaller dependency set cannot speak for a target just added (case 6b). Nothing is
recorded there — but the same declare records case 5's stamp, which is what
guarantees the dependent a pass whatever else is in flight.

Startup runs exactly three things: `enqueueOwedPass` (`reconciler.run`, before its
workers start), the GC sweeper's eager first sweep, and the stale-dependents pass's
eager first step (both `runDriver`). The first two are unconditional; the third runs
whenever any kind is registered, which is the only case in which it could enqueue
anything. That is the whole of what a restart guarantees.

**The full pass is not in this document, and that is deliberate.** Neither
`WithFullPassInterval` nor `WithStartupFullPass` is on by default, and **no reconcile
may depend on either**: both scale with the object count rather than with what is
outstanding, so a guarantee resting on one holds only while the sweep stays
affordable. A full pass is a re-confirm tool for process-scoped state — see
[section E](#e-not-triggers) — never an answer to "how does this work get found". If
the only answer for some path is "the full pass picks it up", that path is a defect,
and it belongs in [`TODO.md`](../TODO.md) rather than in a coverage column here.

---

## A. Spec-derived

All four cases below leave the same trace and are found the same way, so the coverage
argument is shared:

- **Found by:** `reconciler.enqueueUnsettled` → `ObjectsListUnsettledIDs`
  (`observed_generation IS NULL OR observed_generation < generation`).
- **Normal:** ✅ the owed-pass tick, 30s. Nothing schedules it sooner — a create or
  update returns without touching a queue, which is why the examples call
  `Client.Requeue` rather than turning intervals down.
- **Restart:** ✅ `reconciler.run` calls `enqueueOwedPass` before its workers start, and
  it is *not* gated on `startupFullPass`.
- **Tests:** `TestIntegrationStartupEnqueuesUnsettled`, `TestStartupAlwaysDrainsOwedWork`
  (pins that the drain happens even under `WithStartupFullPass(false)`),
  `TestEnqueueUnsettledEnqueuesReturnedIDs`, `TestObjectsListUnsettledIDs`.

1. **Create** — `Create` / `CreateOrUpdate` / `GetOrCreate` insert with `generation=1`
   and `observed_generation` NULL (`sqliteStore.objectsCreate`). Tests:
   `TestIntegrationCreateTriggersReconcile`, `TestClientGetOrCreateOwesAPassOnlyOnCreate`,
   `TestClientWritesAreOwedOnlyAfterOuterCommit`.
2. **Spec update** — `ObjectsUpdateSpec` bumps `generation`. Suppressed on equal bytes
   at the same schema version, which is what stops a controller re-applying its own
   spec from waking itself forever. Tests: `TestIntegrationUpdateTriggersReconcile`,
   `TestObjectsUpdateSpecBumpsGeneration`, `TestClientNoOpUpdateOwesNothing`,
   `TestSameVersionNoOpWritesNothing`.
3. **Schema-version re-stamp** — equal bytes at a *different* spec version deliberately
   falls through the no-op gate and bumps generation, because bytes in a different
   shape are not comparable. Tests: `TestCrossVersionWriteIsNotANoOp`,
   `TestNoOpWriteStampsUpwardWhileConverging`.
4. **A stale status write re-unsettles** — `UpdateStatus` reporting an
   `observedGeneration` behind the current one writes it unclamped on the content path,
   so the object goes back to unsettled and the stale status gets re-derived. Tests:
   `TestUpdateStatusChangedStaleGenerationUnsettles`, `TestUpdateStatusAcceptsStaleGeneration`.

## B. Dependency-derived

Three mechanisms, and the split between them is the whole design. One is a durable
record made at declare time — every new edge stamps the pass it owes, which is what
no scan can be trusted to see under every interleaving. One is a fast in-memory scan
covering every ordinary change to an existing target. And one records nothing,
re-deriving from state whatever the other two lost.

### 5. The new-edge stamp (durable)

Every `depends_on` edge `EdgesAdd` **creates** (self-edges excluded) increments the
dependent's `reconcile_owed` — in the same statement sequence as the edge insert, so
the two cannot come apart. The stamp is unconditional,
because recorded owed work is the only mechanism sound under every interleaving: an
increment landing while the dependent's own pass is in flight sits above the count
that pass observed at load, so the load-scoped decrement cannot consume it. That is
what covers the declare-race (a target that moved between the caller's read and the
declare), the quiet-target declare (case 6b's shape), and the mid-pass third-party
declare that used to be a strand — with one reconcile per edge ever created as the
price, bounded by the edge-new gate.
→ [ADR](adr/2026-07-29-stamp-every-new-dependency-edge.md)

- **Recorded by:** `sqliteStore.EdgesAdd`, via `ControllerClient.DependenciesAdd`.
- **Found by:** `reconciler.enqueueReconcileOwed` → `ReconcileOwedListIDs`. Drained by
  `ReconcileOwedDecrement` in `typedController.reconcile`, which subtracts the whole
  count observed at load, not one.
- **Normal:** ✅ owed-pass tick.
- **Restart:** ✅ durable and drained unconditionally at startup — a crash between the
  commit and the dispatch loses nothing. This is the case `TestDependencyRequeueLostAcrossRestart`
  pins, deliberately under `WithStartupFullPass(false)` so the full pass cannot heal it
  for unrelated reasons.
- **Tests:** `TestAddDependencyWakesOncePerEdge`, `TestAddDependencyStampRidesRefsAdd`,
  `TestEdgesAddStampsReconcileOwed`, `TestRefsAddStampsOnlyNewEdge`,
  `TestRefsAddStampFailureLeavesNoEdge`, `TestRefsAddEdgeFailureLeavesStamp`,
  `TestAddDependencyNoWakeOnRollback`, `TestDependencyRequeueRaceOnDeclare`,
  `TestDependencyRequeueRaceOnOutOfBandDeclare`, `TestReconcileDecrementsReconcileOwed`,
  `TestReconcileDrainsMultipleOwedPasses`, `TestReconcileOwedSurvivesConcurrentIncrement`,
  `TestReconcileMidPassDeclareLeavesTheDependentOwed`,
  `TestOwedPassTickDispatchesOwedWake`.

### 6. An ordinary target change (in memory)

- **Recorded by:** nothing specific — the target's `resource_version` moving *is* the
  record.
- **Found by:** `waker.scan` → `ObjectWritesListSince` from an in-memory watermark, then
  `dependentsWake` → `EdgesGroupIncomingByID` resolves a whole page in one query and
  enqueues each dependent under its own kind.
- **Normal:** ✅ 1s scan. A failed page or a failed edges lookup holds the cursor so the
  next tick re-reads it; the self-edge is skipped; a wake arriving mid-reconcile is held
  by `workQueue`'s dirty bit and re-dispatched by `done`.
- **Restart:** ✅ **by case 7, not by this mechanism.** `seed` re-reads the store's
  *current* cursor, so a change made while the process was down is never scanned, and a
  settled dependent stranded by one is invisible to every owed-work listing: its own
  generation never moved, and nothing stamped `reconcile_owed`. This mechanism leaves no
  durable trace, **by design** — it is an optimisation over case 7, which re-derives the
  same wake from durable state at its own cadence. A wake lost mid-process to a failed
  lookup or a bug is covered the same way. The cost is latency (one stale-pass interval
  instead of one waker tick), never divergence; there is no fix owed here, and
  `TODO.md` carries none.
- **Tests:** `TestWakerScanWakesDependentsByTheirOwnKind`, `TestWakerSeedsFromTheStoreCursor`,
  `TestWakerRetriesSeedOnTheNextTick`, `TestWakerHoldsTheWatermarkOnScanFailure`,
  `TestWakerHoldsTheWatermarkOnLookupFailure`, `TestWakerPagesTheScan`,
  `TestWakerStopsOnAShortPage`, `TestWakerResolvesEachTargetOnce`,
  `TestWakerSkipsTheSelfEdge`, `TestWakerSkipsUnregisteredKinds`,
  `TestStartWithNoControllersSkipsWaker`, `TestDependencyRequeue`,
  `TestSelfDependentObjectWakesOnSpecChange`.

**What bumps a target's `resource_version`** — i.e. what wakes its dependents — is
broader than "a spec change": `ObjectsCreate`, `ObjectsUpdateSpec`, the content and
handshake-only paths of `ObjectsUpdateStatus`, `ConditionsSet`, `ConditionsDelete`,
`FinalizersDelete`, `markForDeletion`, and the cascade mark. `EventsAdd` is the one
write that does not, by design.

A target of a **client-only kind** is covered too: the waker scans store-wide rather
than per registered kind, precisely because a `depends_on` edge may point at a kind no
per-kind query could name. Tests: `TestClientOnlyTargetWakesDependent`,
`TestClientOnlyTargetCreatedAfterStart`, `TestClientOnlyTargetDeletionUnwedges`.

### 6b. The watermark clear on a new edge (derived-state hygiene)

A companion to case 5, no longer a coverage mechanism of its own. `EdgesAdd` **clears
the dependent's `dependency_watermarks` row** when it creates a new `depends_on`
edge, because a cursor recorded over a smaller dependency set cannot speak for a
target just added — leaving it standing would misreport convergence to case 7's scan
for the window until the stamped pass runs. An absent row already means stale;
nothing new is recorded.

The clear alone used to be what reached a declare against a quiet target — one the
old conditional stamp did not fire on — and it had a strand-shaped hole: a *third party*
declaring between a dependent's load and that dependent's own watermark write had the
clear immediately undone by a pass that never saw the new target. Case 5's
unconditional stamp is what closes that — recorded owed work survives the pass, where
invalidated derived state does not — so this clear is now hygiene for the watermark
invariant rather than anyone's only route to a wake.
→ [ADR](adr/2026-07-29-stamp-every-new-dependency-edge.md)

- **Recorded by:** nothing — it *un*-records, inside `sqliteStore.EdgesAdd`, in the
  same statement sequence as the edge and the stamp.
- **Found by:** case 7's scan, on the absent-row arm (until case 5's pass rewrites
  the row).
- **Normal / Restart:** ✅ carried by case 5's stamp; the cleared row (and its
  absence) is durable too.
- **Costs the declarer's own pass nothing, with one exception:** a controller declaring
  from inside its own `Reconcile` rewrites the watermark when the pass succeeds, from
  the cursor it loaded at — sound because its read of the new target happened after that
  load. The exception is the object's *first* `depends_on` edge, where
  `ReconcileLoad.HasDependencies` was sampled before it existed, so the pass skips the
  write and the object is found stale once. Once per object ever, self-extinguishing,
  and not this mechanism's doing. Test:
  `TestReconcileSkipsTheWatermarkWhenTheFirstDependencyIsDeclaredMidPass`.
- **Tests:** `TestRefsAddClearsTheDependentsWatermark`,
  `TestRefsAddKeepsTheWatermarkOnAReDeclaredEdge`,
  `TestRefsAddKeepsTheWatermarkOnAnOwnerEdge`,
  `TestRefsAddKeepsTheWatermarkOnASelfEdge`,
  `TestReconcileRecordsDependencyWatermarkAfterDeclaringANewEdge`,
  `TestReconcileMidPassDeclareLeavesTheDependentOwed`.

### 7. A dependency moved and nobody noticed (re-derived)

The backstop under case 6, and the only mechanism here that records nothing: it asks
current state whether each dependent has reconciled against its targets' latest
versions, so it recovers a wake lost by *any* means — a crash, a startup seed race, a
process with no waker, a bug in the wake path.

- **Recorded by:** `Store.DependencyWatermarksSet`, from `typedController.reconcile` on
  every successful pass of an object with dependencies. The value is the write cursor
  as of the pass's *load*, never the end of the pass.
- **Found by:** `Beehive.staleDependentsSweep` → `DependentsListStale(kinds, afterID,
  limit)`, paged to exhaustion, enqueuing each dependent under its own kind.
- **Normal:** ✅ 60s. Slower than the waker on purpose: it is the guarantee, not the
  latency. A failed page abandons the sweep and the next tick re-derives the same set,
  since nothing was drained.
- **Restart:** ✅ **derived from current state, so there is nothing to lose.** An
  absent watermark row already means stale, which is also why no backfill was needed
  and why the first start after this landed enqueued the whole dependency graph once.
- **Not covered:** a dependent whose kind has no controller is excluded from the scan
  (there is no loop to enqueue into, and it would be stale forever); it is found on the
  first pass after its kind is registered. The kind filter applies to the *dependent*
  only — a registered dependent of a client-only target is always in scope.
- **Tests:** `TestDependencyWakeSurvivesRestart`,
  `TestStaleDependentsPassEnqueuesStaleDependents`,
  `TestStaleDependentsPassIgnoresUnregisteredKinds`,
  `TestReconcileRecordsDependencyWatermark`,
  `TestReconcileRecordsCursorFromTheLoad`,
  `TestReconcileSkipsDependencyWatermarkWithoutDependencies`,
  `TestReconcileHoldsDependencyWatermarkOnFailure`,
  `TestReconcileHoldsDependencyWatermarkOnUndecodableRow`,
  `TestReconcileWarnsAndContinuesOnCursorWriteFailure`,
  `TestDependentsListStaleFindsMovedTargets`,
  `TestDependentsListStaleTreatsMissingWatermarkAsStale`,
  `TestDependentsListStaleFindsDependentsOfUnregisteredTargets`,
  `TestDependentsListStaleExcludesSelfEdges`, `TestDependentsListStaleFiltersByKind`,
  `TestDependentsListStaleReturnsEachDependentOnce`, `TestDependentsListStalePages`,
  `TestDependencyWatermarksSetGatesOnOutgoingDependsOn`,
  `TestDependencyWatermarksSetNeverRegresses`,
  `TestDependencyWatermarksSetMovesReconciledAtOnlyWithTheCursor`,
  `TestDependencyWatermarksSetSkipsCollectedObject`,
  `TestDependencyWatermarksSetBumpsNoResourceVersion`,
  `TestDependencyWatermarksCascadeOnObjectDelete`,
  `TestObjectsGetForReconcileReturnsTheWriteCursor`,
  `TestObjectsGetForReconcileReportsHasDependencies`.

## C. Deletion-derived

All three cases share one trace and one driver:

- **Found by:** `Beehive.deletionPendingSweep` → `DeletionRequestsList` (kind-agnostic),
  routed by `deletionAdvance`: a registered kind is **enqueued** so its controller can
  clear finalizers, a client-only kind is collected directly by `gcCollect`. The routing
  is correctness, not speed — `gcCollect` cannot clear a finalizer, so calling it on a
  registered kind would make no progress forever.
- **Normal:** ✅ GC sweeper, 30s, and `WithGCInterval` rejects a non-positive value, so
  every error path has a next tick.
- **Restart:** ✅ `runDriver` sweeps eagerly before its first tick. Test:
  `TestIntegrationGCResumesDanglingDeleteOnStartup`.
- **Tests:** `TestGCSweepsOnItsOwnInterval`, `TestGCSweepDispatchesRegisteredKind`,
  `TestIntegrationGCSweepsClientOnlyKind`, `TestIntegrationGCSweepCollectsStandaloneClientOnlyDelete`,
  `TestGCSweepLogsCollectFailure`, `TestWithGCIntervalRejectsNonPositive`.

7. **A delete request** — `Delete` / `DeleteBySlug` stamp `deletion_requested_at` and
   nothing else. Tests: `TestIntegrationDeleteTriggersReconcile`,
   `TestDeletionRequestsCreateIsIdempotent`, `TestRepeatDeletionRequestsCreateDoesNotBumpResourceVersion`.
8. **Cascade to owned children** — `gcCollect` marks them via
   `DeletionRequestsCreateFromOwner` and returns; the mark is what puts them in the next
   sweep's listing, so a cascade advances one level per sweep. Tests:
   `TestIntegrationGCCascadeDeletesOwnerAndChild`, `TestIntegrationGCCascadeWithFullPassDisabled`,
   `TestCollectCascadesAndBlocksOnChild`, `TestDeletionRequestsCreateFromOwnerCascadesThenIsNoOp`.
9. **A blocked collect retries by never leaving the listing** — finalizers still
   pending, or `EdgesHasIncoming` reporting a referrer under RESTRICT. Nothing signals
   the unblocking either: an owner freed by its last child's removal, or by a
   `DependenciesDelete`, is simply still deletion-pending when the next sweep runs.
   Every block is temporary by construction: the one that was not — a finalizer on a
   client-only kind, which no `FinalizersDelete` can reach — is now rejected at create
   (`TestClientCreateRejectsFinalizersOnUnregisteredKind`), so a sweep always has a
   route to progress.
   Tests: `TestCollectKeepsFinalizedObject`, `TestCollectDeletesOwnerAfterChildGone`,
   `TestIntegrationGCDeletesAfterFinalizerCleared`, `TestIntegrationGCDeleteDependencyUnblocksTarget`,
   `TestIntegrationGCBreaksDependencyCycle`, `TestCollectBreaksSelfDependency`.

## D. In-memory only

None of these leave a trace, and `workQueue.stop` cancels every pending timer at
shutdown. **Restart: ❌ lost** — recovered only if the object *also* carries a durable
trace, which is the case for the two that matter: a reconcile that failed on an
unconverged spec is still unsettled (A), and one that failed servicing a wake still
holds its `reconcile_owed` count, because `ReconcileOwedDecrement` runs only on success
(B5). A retry for a *settled* object with neither has nothing to recover it.

10. **`Result.RequeueAfter`** — `runWorker` → `workQueue.addAfter`. Tests:
    `TestReconcilerRequeueAfter`, `TestWorkQueueAddAfter`, `TestWorkQueueAddAfterNewestWins`,
    `TestTypedControllerReconcileDropsRequeueWhenCollected`. A chain of these on a
    settled object is the case with no durable trace at all: it does not survive a
    restart, and no driver brings it back. See [`TODO.md`](../TODO.md) — the open
    question there is whether a self-polling controller should be written this way
    rather than owning its own ticker and calling `Client.Requeue`.
11. **Failure backoff** — `reconciler.backoffNext` → `addAfter`, doubling to
    `maxRetryInterval`, cleared only by a successful reconcile. Tests:
    `TestReconcilerRequeuesOnError`, `TestReconcilerClearsBackoffOnSuccess`,
    `TestReconcilerRequeueBackoffLadder`, `TestNextBackoffDoubles`, `TestNextBackoffCaps`.
12. **`Client.Requeue`** — `reconciler.requeue` → `workQueue.requeueNow`, cancelling any
    pending alarm. The public way to beat a cadence. Tests: `TestClientRequeue`,
    `TestClientRequeueNoController`, `TestReconcilerRequeueNow`, `TestWorkQueueRequeueNow`.
## E. Not triggers

Worth stating, because each looks like one:

- **The full pass is not a coverage mechanism.** `enqueueAll` → `ObjectsListIDs`
  re-dispatches every object of the kind, once at startup under `WithStartupFullPass`
  and periodically under `WithFullPassInterval` — **both off by default**. Its job is
  the one thing no owed-work listing can express: re-confirming state that belongs to
  a *process* rather than to the store, such as a liveness condition reading
  "verifying" until a controller in this process rewrites it. It is not listed as a
  trigger above because nothing may be owed a reconcile *by way of it*. It does
  incidentally re-dispatch settled objects that other mechanisms failed to reach, and
  that is exactly the property not to lean on: it turns an open gap into an
  intermittent one, scaled by object count, and hides it from anyone reading this
  document for what guarantees convergence. Where a gap exists it is named as a gap
  here and tracked in [`TODO.md`](../TODO.md). Tests:
  `TestStartupEnqueuesAllNotJustUnsettled`, `TestStartupFullPassReconcilesSettled`,
  `TestStartupFullPassDisabledSkipsSettled`, `TestFullPassTickReconcilesSettled`,
  `TestDefaultConfigDoesNotFullPass`, `TestSelfDrivenRecovery`.
- **A status or condition write on an object does not wake that object's own
  controller.** The waker skips the self-edge deliberately: a spec write already leaves
  the object unsettled, and a status write came from the pass that just ran. Test:
  `TestWakerSkipsTheSelfEdge`.
- **`EventsAdd` wakes nothing** — it bumps no object `resource_version`, which is also
  what makes it the one write safe inside a dependency cycle.
- **A schedule change wakes nothing** — `SchedulesWatch` reports an in-memory gauge; it
  bumps no generation or `resource_version`.
- **An object of a client-only kind is never reconciled.** It has no reconcile loop, so
  an unsettled spec on one is inert; only its deletion is ever acted on, by the sweeper.
- **A queued id whose row is gone is a no-op success**, not a retry — `ObjectsGet`
  returning `ErrNotFound` drops it. Test: `TestTypedControllerReconcileMissingIDIsTerminal`.
- **An undecodable row is quarantined**, not retried: it is skipped as a no-op success so
  the worker drops it, while its `reconcile_owed` count is deliberately left standing.
  Tests: `TestTypedControllerReconcileRawToTypedError`,
  `TestTypedControllerReconcileQuarantineKeepsReconcileOwed`.
