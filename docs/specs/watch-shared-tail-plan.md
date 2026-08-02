# Watch shared tail: implementation plan

- **Status:** Draft — companion to [the spec](watch-shared-tail.md).
- **Date:** 2026-08-02

## Ground rules

- Red/green: each cycle starts with a failing test, ends with the least code
  that passes it, then a refactor pass with the suite green. Do not write
  code a cycle's test does not force.
- `go test ./...` is green at every commit. New machinery lands beside the
  poll (phases 1–2); the public surface flips onto it only in phase 3; the
  poll is deleted last.
- Whitebox tests in `package beehive`; test files mirror source files; new
  source in `watchtail.go`, tests in `watchtail_test.go`. Synchronize on
  signals, never on sleeps — that rule is what this design finally makes
  easy, because every wake is observable.
- One conventional commit per cycle; suggested subjects are given.

## Decisions fixed for this plan

Open points in the spec, resolved here so the cycles are executable:

- Tailers start lazily, on a kind's first watch, guarded by `bh.mu` (not
  reentrant — the start path resolves nothing through `migratorFor` or
  `reconcilerFor`), under a `tailCtx` created in `New` and cancelled by `stop`
  — running or not. A tailer is not torn down when its subscribers leave.
- A too-old cursor on the tailer closes the fan-out sender and resets it: all
  subscribers end with `ErrWatchTooOld` and resubscribe.
- The tailer and its fan-out hub are non-generic; the hub's value is
  `rawChange`. Subscribers decode.
- The floor tick is a new unexported option, defaulting to 30s.
  `withWatchPollInterval` survives as the events-watch cadence only.
- `LagPolicy` removal moves **after** the surface flip (phase 4). Removing it
  first is a breaking change that buys nothing until conflate is carrying the
  traffic, and an abandoned run leaves the API broken with no gain.

## Phase 1 — the wake hub

### Cycle 1: a create wakes its kind

- **Red:** `TestWakeHubPublishesOnCreate` — register a receiver for a kind
  on the beehive's wake hub, `Create` an object, receive `(rv)` above zero.
  Fails: no hub exists.
- **Green:** the hub on `Beehive` (`gobus/watch`, key `GroupKind`, value rv,
  `Accept: next > prev`), built in `New`; one publish, in `Create`'s commit
  path, through a new `signalObjectWritten(ctx, gk, rv)` beside
  `signalRequeue` — `Store.AfterCommit`, same file, same shape.
- Commit: `feat(watch): add a commit-path wake hub, published on create`

### Cycle 2: every write-log call site wakes

- **Red:** `TestWakeHubPublishesOnEveryWrite` — a table derived from the
  store's three write-log helpers (`appendWriteLog`, `recordObjectWrite`,
  `appendWriteLogDelete`), **not** from the public verbs. Rows, per the
  spec's table: create (bare, by-name, `GetOrCreate`'s create branch), spec
  update, status update, **`ConditionsSet`, `ConditionsDelete`** (both reach
  `bumpObject`), finalizer clear, soft delete, and the GC sweeper's physical
  delete. Each row performs the write and requires a wake on the right kind.
- **Green:** route the remaining verbs through `signalObjectWritten`. This
  table is the emit-discipline guard the spec demands — a new verb's author
  extends it or ships a write watchers see only on the floor tick.
- Commit: `feat(watch): wake the hub from every object write`

### Cycle 3: the owner cascade wakes every kind it touched

- **Red:** `TestWakeHubPublishesPerCascadedKind` — an owner with children of
  two other kinds; **drive the collection explicitly** (`bh.gcCollect` on the
  owner), then require a wake on all three kinds. Do *not* write this as
  "`Delete` on the owner wakes all three":
  `DeletionRequestsCreateFromOwner` is called only from `gcCollect`
  (`gc.go:35`), never from `Delete`, which soft-marks the owner and stops
  there. That test would fail because no cascade ran, or pass incidentally
  once a reconcile loop or the sweeper reached it — either way it would not be
  testing the routing.
- **Green:** `gc.go:35` currently discards the refs
  (`if _, err := bh.store.DeletionRequestsCreateFromOwner(...)`); that discard
  is the bug. Keep them and route a wake per ref, as the new-edge enqueue
  routes by `EdgesAddResult.From`. Its own cycle because it is the one row
  that is not "the caller's gk".
- Commit: `feat(watch): wake every kind the owner cascade marked`

### Cycle 4: a rollback wakes nothing

- **Red:** `TestWakeHubSilentOnRollback` — a write inside a failing `Within`
  publishes no wake.
- **Green:** nothing, if cycle 1 used `AfterCommit` correctly — this test
  pins the property rather than adding code.
- Commit: `test(watch): pin that a rolled-back write publishes no wake`

### Cycle 5: stop closes the wake sender

- **Red:** `TestWakeHubClosesOnStop` — after the beehive stops, a receiver's
  next read reports closed, not a hang. A second case: a beehive that was
  never started still closes.
- **Green:** close the sender (not the hub — `scheduleHub`'s rule) in the
  stop path; cancel `tailCtx` there too, outside the `beehiveRunning` guard.
- Commit: `feat(watch): close the wake sender on stop`

## Phase 2 — the tailer

### Cycle 6: a wake drives one tail step

- **Red:** `TestTailerDeliversOnWake` — start a tailer for a kind with a raw
  conflate receiver attached, write an object, receive its `rawChange`
  without any interval elapsing (failsafe timeout only).
- **Green:** `watchtail.go`: a per-kind, non-generic tailer goroutine
  selecting on its wake receiver and `ctx`, running the existing tail step
  (gate, page, coalesce, batched read) and publishing `rawChange` keyed by
  `ObjectID`. No decode here. Reuse the poll's step logic by extraction, not
  copy.
- Commit: `feat(watch): add a per-kind tailer that tails the log on wake`

### Cycle 7: the tailer registers before it reads its cursor

- **Red:** `TestTailerLosesNoWriteAtStartup` — a write committed between the
  tailer's cursor read and its registration must still be delivered; drive
  the interleaving with a store double that publishes from inside the read.
- **Green:** register the wake receiver first, read the initial cursor
  second.
- Commit: `test(watch): pin the tailer registers before reading its cursor`

### Cycle 8: a burst drains without waiting for another write

- **Red:** `TestTailerDrainsBurstAbovePageCap` — write `tailPageCap + 88`
  objects, then nothing more; all of them are delivered, with no floor tick
  and no further write. Fails today: one wake slot, one 512-entry page, 88
  entries stranded.
- **Green:** the drain loop — re-run until a step returns a short page
  (`len(page) < tailPageCap`); block on the wake only when drained. The page
  length, not a second `ObjectWritesMaxVersion` read: `step` already opens
  with that read as its gate, and a re-read would cost a scalar query per
  wake for an answer the page gives exactly. Add the query-count assertion to
  this test so the cheap condition is pinned, not merely chosen.
- Commit: `fix(watch): drain the log fully before blocking on the wake`

### Cycle 9: the merge table

- **Red:** `TestTailerMergeTable` — with a deliberately unread receiver,
  drive the four reachable pairs: unobserved create then update delivers one
  `Added` with the newest state; update then update delivers the last;
  observed object's delete delivers `Deleted`; **create then delete delivers
  a `Deleted`, not nothing**. The last row is the anti-annihilation case; add
  its motivating test too — `TestWatchSeesDeleteOfSnapshotObject`, where the
  create precedes the subscriber's snapshot and the subscriber has read
  nothing — as the regression that explains why.
- **Green:** the hub's `Merge`: stale guard first (`next.ResourceVersion <=
  prev.ResourceVersion` keeps prev), then the create promotion. Never
  `keep = false`.
- Commit: `feat(watch): coalesce fan-out changes with the merge table`

### Cycle 10: the floor tick and its retry

- **Red:** two tests. `TestTailerFloorTickPicksUpAForeignWrite` — a write
  made through a *second* `Beehive` over the same store is delivered on the
  floor tick, with no wake. `TestTailerRetriesAfterAFailedStep` — a store
  double that fails one step and then succeeds delivers without a further
  write.
- **Green:** the floor ticker in the tailer's select, a new unexported
  interval option defaulting to 30s (tests set it small), and bounded backoff
  on a failed step, capped at the floor.
- Commit: `feat(watch): keep a floor tick behind the wake`

### Cycle 11: the tailer's cursor falls below the horizon

- **Red:** `TestTailerResetsWhenItsCursorIsTrimmed` — hold the tailer's cursor
  still (a store double failing its step), trim the log past it, then let the
  step succeed: every subscriber ends with `ErrWatchTooOld`, and a fresh watch
  afterwards works from a new snapshot.
- **Green:** on `ErrWatchTooOld` from the step, close the fan-out sender and
  reset the tailer. Not: advance the cursor to the horizon (silently drops
  changes for every subscriber), and not: return the error to one subscriber
  (a shared reader has no one subscriber at fault).
- Commit: `feat(watch): reset the tailer when its cursor is trimmed`

### Cycle 12: tailer lifecycle

- **Red:** `TestTailerStartsLazilyAndStopsWithBeehive` — no tailer goroutine
  before the first watch; the first watch starts exactly one per kind under
  concurrent callers; a watch works on a beehive that was never started; stop
  ends the tailer and closes its fan-out sender so subscribers drain and see
  closed; a watch opened after stop returns its snapshot and a closed stream.
- **Green:** lazy start guarded by `bh.mu`, under `tailCtx`; stop wiring.
  **`bh.mu` is not reentrant**, and both `migratorFor` (`beehive.go:381`) and
  `reconcilerFor` (`:388`) take it — the start path must resolve nothing
  through them while holding it. Small hazard, since decode moved to the
  subscribers and the tailer needs neither; leave a one-line comment anyway,
  as `stop` already does for the same trap with `wg.Wait`. Note that a tailer
  is never torn down when its last subscriber leaves — the spec's stated
  trade, not an oversight; do not add idle teardown here.
- Commit: `feat(watch): start tailers lazily and stop them with the beehive`

## Phase 3 — flip the surface

### Cycle 13: WatchList subscribes to the shared tail

- **Red:** `TestWatchListDeliversWithoutPolling` — a `WatchList` delivery
  arrives with no interval elapsed. The existing `WatchList` suite is the
  regression harness and must stay green through the flip, except tests that
  pinned write-order delivery across objects — rewrite those to the relaxed
  contract (each object's latest state, newest wins; a repeated `Added` for a
  snapshot object is legal).
- **Green:** `ObjectChange` gains a public `ResourceVersion`; `WatchList`
  registers a conflate receiver, snapshots, then consumes, decoding each
  `rawChange` with its own migrator and dropping `rv <= floor`. Registration
  before snapshot; the floor is the snapshot's position.
- Commit: `feat(watch)!: WatchList subscribes to the shared tail`

### Cycle 14: two type parameter sets, one tailer

- **Red:** `TestTwoClientsOverOneKindShareATailer` — a typed client and a
  `json.RawMessage` client watch the same `GroupKind` concurrently; both
  receive every write, correctly decoded, and the store sees one read path.
  This is the test that would have caught a typed hub.
- **Green:** nothing new if cycle 6 kept the hub non-generic; this pins it
  before anyone is tempted to fold decode back into the tailer.
- Commit: `test(watch): pin one tailer serves clients of any type params`

### Cycle 15: Watch filters one key

- **Red:** `TestWatchSingleObjectSeesOnlyItsID` — two objects written; the
  single-object watch receives only its id, still without polling.
- **Green:** `Watch` joins through `hub.WithKeyFilter(k == id)`.
- Commit: `feat(watch)!: Watch subscribes through a key filter`

### Cycle 16: loads stay batched

- **Red:** `TestWatchLoadsAreBatchedPerDrain` — with a counting store, a
  burst delivered to a watch with `WithLoads` costs one relation query per
  drained batch, not one per object.
- **Green:** the subscriber drains pending with `TryRecv` before loading
  relations for the drained batch.
- Commit: `feat(watch): batch relation loads per drained batch`

### Cycle 17: resume, paged

- **Red:** `TestWatchResumeReplaysGapThenGoesLive` — write, subscribe with
  `WithResumeFrom` at an older rv, receive the gap then a live write, no
  duplicates across the seam. Plus `TestWatchResumeReplaysBeyondOnePage` — a
  gap larger than `tailPageCap` replays whole. Keep the existing
  `ErrWatchTooOld` tests green: a resume below the horizon still fails, per
  subscriber.
- **Green:** register, replay the log above rv **in a paging loop** on the
  subscriber's goroutine until it reaches the tailer's position, raise the
  floor to the replay's end, consume.
- Commit: `feat(watch): resume replays the log gap before going live`

### Cycle 18: the tailer is one reader

- **Red:** `TestTailerQueryCountConstantInSubscribers` — with a counting
  store double, two `WatchList` subscribers on one kind cost the same reads
  per write as one; a kind with no writes costs one read per floor interval,
  not one per subscriber.
- **Green:** nothing new if phases 2–3 are honest. This lands here, not in
  phase 2: while the public watches still poll, a query count over "two
  subscribers" measures the poll, not the tailer — the plan's own hygiene
  rule ("if a cycle's red test passes before its green change, the cycle is
  mis-scoped") rejects it earlier.
- Commit: `test(watch): pin one shared read path per kind`

## Phase 4 — retire the lag contract

### Cycle 19: delete LagPolicy

- **Red:** compile failure is the red — delete `LagPolicy`, `WithLagPolicy`,
  `ErrWatchLagged`, `maxLagDepth` and their tests; the suite must build and
  pass.
- **Green:** deletions. Conflate's coalescing is already carrying the
  traffic, so the lag story is genuinely gone rather than merely removed. A
  breaking change, taken deliberately and alone so it is reviewable.
- Commit: `feat(watch)!: drop LagPolicy; slow subscribers coalesce instead`

## Phase 5 — demolition and docs

### Cycle 20: delete the poll

- **Red:** nothing references the per-watch poll loop — delete the polling
  `objectStream` body, its `driver.Run` use, and any now-dead helpers; the
  suite stays green. `EventsWatch` keeps `withWatchPollInterval`; its docs
  say events-only.
- **Green:** deletions only. `go vet` confirms no dead exports.
- Commit: `refactor(watch): delete the per-watch poll loop`

### Cycle 21: leak and shutdown sweep

- **Red:** `TestWatchGoroutinesDrainOnStop` — start watches on several
  kinds, stop the beehive, assert every stream closes and no goroutine
  lingers (goleak or a counted-registration double).
- **Green:** whatever the sweep finds; expected to be nothing if phases 1–4
  closed their cycles honestly.
- Commit: `test(watch): pin shutdown drains every watch goroutine`

### Cycle 22: docs

- Write the ADR (two hubs, raw fan-out value, the observation-surface
  argument for relaxing the tick, emit discipline, relaxed ordering); amend
  the drivers ADR (the watch poll keeps its place at a floor cadence, with a
  wake in front, and why the schedule watch's exception does not extend to
  it); trim `docs/TODO.md`; update `CLAUDE.md` and `README.md` watch bullets,
  including the schedule watch's "the one watch that does not poll". Mark the
  spec Accepted and link the ADR.
- Commit: `docs(watch): record the shared-tail design; amend the drivers ADR`

## Cycle hygiene

- If a cycle's red test passes before its green change, the cycle is
  mis-scoped: shrink it, delete it, or move it to a phase where it bites.
- If a green change needs code the test did not force, the test is
  under-specified: strengthen the red first.
- Refactors (extraction of the tail step in cycle 6, file moves) happen
  inside a green suite, in their own commits when large.
