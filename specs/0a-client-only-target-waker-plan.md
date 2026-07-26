# Implementation plan — `0a-client-only-target-waker.md`

Eleven TDD cycles. Each one is **self-contained**: it names a single behavior, has a test
that fails for a stated reason before the change and passes after, and ends with the whole
suite green (`go test -race ./...`, `go vet ./...`). You can stop after any cycle and the
tree is shippable.

No cycle leaves a test red for a later cycle to fix. The three acceptance tests from the
spec's §5 are the red of **cycle 7** — the one cycle where the defect actually closes —
rather than being written up front and left failing across the whole plan.

Cycles 1–6 are preparation that changes no observable behavior; each is small enough to
review in one sitting. Cycle 7 is the behavior change and is unavoidably atomic (the
waker's input type and `Start`'s subscription must move together). Cycles 8–11 are
hardening and cleanup.

Test placement follows the repo convention — a function in `foo.go` is tested in
`foo_test.go` — with one exception noted in cycle 6.

---

## Cycle 1 — the store publishes every change to one global hub

**Goal:** a single conflating hub carries changes of all kinds, alongside the per-kind hubs.

**Red** — `sqlite/watch_test.go`: `TestPublishReachesGlobalHub`. Subscribe a raw
`conflate.Receiver` to `store.changeHub`, write objects of two different kinds, assert both
arrive.
*Fails to compile: no `changeHub` field.*

**Green:**
- `sqliteStore.changeHub *conflate.Hub[ObjectID, RawChange]`, created **eagerly** in `open`
  (`sqlite/sqlite.go:55-65`) beside `hubs`/`eventHubs`, closed in `Close`
  (`sqlite/store.go:76-92`) in the existing `hubMu` section. Eager rather than a
  `hubFor`-style lazy lookup: spec §3.1 — `publish` runs on every object write, and this
  saves it a lock acquisition and a map lookup.
- `publish` (`sqlite/watch.go:226`) also sends to `changeHub`, same `ev.Object.ID` key, same
  `mergeChange`.

**Safe to stop:** nothing subscribes to the new hub yet; the per-kind path is untouched.

---

## Cycle 2 — `WatchChangeRefs` streams blob-free change references

**Goal:** the new store surface exists and delivers ids, not rows.

**Red** — `sqlite/watch_test.go`: `TestWatchChangeRefsStreamsLiveChanges` (one write → one
batch of one `ChangeRef{ID, Added}`), `TestWatchChangeRefsSpansKinds` (one subscription sees
two kinds), `TestWatchChangeRefsSkipsSnapshot` (a pre-existing object produces nothing — the
contract `TestWatchChangesSkipsSnapshot` pins today).
*Fails to compile: no `WatchChangeRefs`.*

**Green:**
- `internal/storeapi`: `ChangeRef{ID ObjectID; Type ChangeType}`,
  `ChangeRefWatcher{ Batches() <-chan []ChangeRef; Close() }`, and
  `Store.WatchChangeRefs(ctx) (ChangeRefWatcher, error)`. **Leave `WatchChanges(gk)` in
  place** — cycle 10 removes it, once nothing calls it.
- `sqlite`: subscribe to `changeHub` with `annihilatingMerge(nil)`; no snapshot, so the
  dedup floor is 0 exactly as `WatchChanges` documents (`sqlite/watch.go:301-308`). Feeder
  goroutine receives one value, projects it to a `ChangeRef` — **this is where `Object` is
  dropped and the blob released** — and sends a one-element slice on an **unbuffered**
  channel, using the existing `send`-with-`wctx`/`s.done` pattern.
- Wire the `afterStream` seam so tests can await goroutine exit without reading `out`.

Batching is *not* in this cycle. One-element slices are the honest minimum that passes.

**Safe to stop:** new surface, no production caller.

---

## Cycle 3 — a burst drains into one batch

**Goal:** the property spec §3.2 exists for — O(bursts), not O(changes).

**Red** — `sqlite/watch_test.go`: `TestWatchChangeRefsBatchesBurst` (park the consumer,
write N distinct objects, then read: **one** batch with all N) and
`TestWatchChangeRefsCapsBatch` (with more than the cap ready, the first batch is exactly the
cap and the rest follow).
*Fails: N batches of one.*

**Green:** after the blocking `RecvContext`, drain with `rx.TryRecv()` until empty or
`changeRefBatchCap`, appending projections. Add the unexported `changeRefBatchCap = 64` with
a comment that it bounds *slice length*, not retained blobs — the memory bound is the
receiver's live-key set, which is now store-wide.

**Safe to stop:** still no production caller.

---

## Cycle 4 — conflation survives the drain

**Goal:** the property that rules out the buffered-channel design.

**Red** — `sqlite/watch_test.go`: `TestWatchChangeRefsCoalescesRepeatWrites` (park the
consumer, write the *same* object N times, read: one batch, **one** entry, latest type) and
`TestWatchChangeRefsAnnihilatesTransient` (create-then-delete an object the consumer never
saw: no entry).

*Expect these to pass on arrival if cycle 3 was implemented correctly — they are
regression pins, not drivers.* Write them anyway and confirm by reverting cycle 3's
`TryRecv` to a buffered-channel drain: `CoalescesRepeatWrites` must then fail with N
entries. That five-minute check is what makes them meaningful; note the result in the
commit message.

**Green:** nothing, if they pass. If they don't, the drain is reading past the receiver.

**Safe to stop:** yes.

---

## Cycle 5 — the stream shuts down cleanly

**Goal:** lifecycle parity with the watchers it sits beside.

**Red** — `sqlite/watch_test.go`: `TestWatchChangeRefsClosedStore` (after `Close` →
`errStoreClosed`), plus an open stream's channel closing on `Close` and on caller ctx
cancel.
*Fails: whichever arm the cycle-2 feeder didn't cover.*

**Green:** the closed-store guard on subscribe and the `s.done` arm in `send`.

**Safe to stop:** yes — this is the last cycle before anything in `beehive` moves.

---

## Cycle 6 — dependents of many targets resolve in one query

**Goal:** the batched wake policy, tested directly, before anything about *streams* changes.

**Placement note:** `wakeDependents` lives in `beehive.go` but its tests are in
`reconciler_test.go`. Keep the new ones beside them; relocating the topic is a separate,
mechanical change if it is wanted at all.

**Red** — `reconciler_test.go`: `TestWakeDependentsBatchOneQuery` (three target ids → one
`GroupIncomingRefsByID` call, every dependent enqueued once),
`TestWakeDependentsBatchDedups` (a repeated id resolves once),
`TestWakeDependentsBatchSkipsSelfEdges` (per-target self-edge skip, the batch form of
`TestWakeDependentsSkipsSelfEdge` :769).
*Fails to compile: no `wakeDependentsBatch`.*

**Green:**
- Implement `fakeStore.GroupIncomingRefsByID` (currently a panic stub) with a call counter.
- `wakeDependentsBatch(ctx, ids []ObjectID)`: one
  `GroupIncomingRefsByID(ctx, ids, RelationDependsOn)`, then per `(targetID, referrers)` the
  existing body — skip `d.ID == targetID`, else `enqueueIfRegistered(d.GroupKind(), d.ID)`.
  Error branch keeps today's semantics verbatim (`ctx.Err() != nil` → silent return, else
  Warn + `resyncKindsNextTick()`), with the message describing the batch.
- `wakeDependents(ctx, id)` becomes a one-element call into it. Temporary — cycle 7 deletes
  it — but it keeps this cycle's diff to one new function and its callers untouched.

**Safe to stop:** no caller sees a difference; the single-id path still works.

---

## Cycle 7 — one store-wide waker (**the defect closes here**)

**Goal:** the spec's actual fix. Atomic by necessity: the waker's input type and `Start`'s
subscription cannot move separately.

**Red** — `reconciler_test.go`, real `sqlite` store, in the style of
`TestDependencyRequeueLostAcrossRestart` (:524). `WithStartupResync(false)`,
`WithResyncInterval(0)`, a catchup interval too long to fire:
1. `TestClientOnlyTargetWakesDependent` — register only D's kind; create T through a
   `Client` for an unregistered kind; `AddDependency(D → T)`; let D settle; write T; assert
   D reconciles.
2. `TestClientOnlyTargetCreatedAfterStart` — same, with T created after `Start`. *The
   discriminating test against the "subscribe per kind present at `Start`" alternative.*
3. `TestClientOnlyTargetDeletionUnwedges` — `Client.Delete(T)`; assert D is woken by the
   tombstone's `Modified`, drops its edge, and T then actually collects (spec §1.1).
4. `TestStartSubscribesOneChangeStream` — three registered kinds, one subscription.

*(1)–(3) fail by timeout: no waker exists for T's kind. (4) fails with 3.*

**Green:**
- Doubles: `fakeStore.WatchChangeRefs` → a never-firing `noopChangeRefWatcher`;
  `fakeChangeRefWatcher` (a `fakeWatcher` twin whose `push` takes `...ChangeRef`);
  `watcherStore` gains the `ChangeRefWatcher` + error it serves. The existing `fakeWatcher`
  stays for the `Client.Watch` adapter tests.
- `runDependencyWaker(ctx, w ChangeRefWatcher)` — the `gk` parameter goes (it existed only
  for the per-kind log line). The loop filters `Added`/`Modified` into an id set and calls
  `wakeDependentsBatch`.
- `Start` (`beehive.go:195-214`): one `WatchChangeRefs(runCtx)`, one failure branch, one
  `bh.wg.Go` — still before the `r.run` loop, which is the ordering constraint `Start`
  already documents.
- Delete single-id `wakeDependents` and port its callers to one-element slices:
  `reconciler_test.go` :769, :784, :812, :882, :2826, :2841, :2998, :3106.
- Port the waker tests: :700, :889, :2863, :2884, and `TestStopDoesNotDeadlockWithActiveWaker`
  (:639) — its `blockingDepsStore` parks inside `ListIncomingRefs` and must move to
  `GroupIncomingRefsByID`. That test is what keeps the "never hold `bh.mu` across a wake"
  invariant honest; do not let it lapse in the port.

**Safe to stop:** yes, and this is the cycle worth stopping at if time runs out — cycles
8–11 are polish over a working fix.

---

## Cycle 8 — no controllers, no subscription

**Red** — `beehive_test.go`: `TestStartWithNoControllersSkipsWaker` — zero registered
controllers ⇒ zero `WatchChangeRefs` calls.
*Fails: one subscription that pays a refs query per change only to reach
`enqueueIfRegistered`'s no-op arm (spec §3.4).*

**Green:** guard the subscription on `len(bh.order) == 0`.

---

## Cycle 9 — failures report the right blast radius

**Red** — `reconciler_test.go`/`beehive_test.go`, asserting on log output as the existing
log tests do: `TestChangeStreamSubscribeFailureEscalates` (with
`watcherStore{changeRefErr: errBoom}`: the warning fires **once**, names the whole-process
consequence, and arms `resyncKindsEveryTick` on **every** reconciler; plus the
`!hasPeriodicPass()` variant) and the stream-ended twin for `beehive.go:346-356`.
*Fails: K warnings with per-kind wording, or one warning still saying "for this kind".*

**Green:** drop `"group"`/`"kind"` from both messages and restate them process-wide (spec
§3.3). Add the two-`Beehive`s-on-one-store note to the waker's doc comment (§3.4) — a
comment, but it belongs in this diff.

---

## Cycle 10 — delete the per-kind stream

**Goal:** remove the now-dead surface. No new test; the cycle is green when the suite is.

- Remove `WatchChanges(ctx, gk)` from `internal/storeapi.Store`, `sqlite`, `fakeStore`,
  `watcherStore`. `Client.Watch`/`WatchList` are a different surface and stay.
- Port or delete its tests (`sqlite/watch_test.go` :435, :441, :464, :1102;
  `sqlite/store_test.go` :1358). Re-read rather than rename: a store-wide stream does not
  filter by kind, so any assertion that another kind's change is *absent* pins the old
  behavior, and nothing can read spec/status off this stream any more. Cycles 2–5 already
  cover the surviving contracts, so several of these delete outright.
- Delete `noopWatcher` if unused. Check whether `watch()`'s `hasSnapshot=false` arm still
  has a caller; if not, simplify the `default:` receiver case and the `seenIDs == nil` path.

---

## Cycle 11 — docs

- `CLAUDE.md`: the waker paragraph ("one waker per registered kind") and the watch-surface
  enumeration both change — one store-wide, batch-drained, blob-free change stream;
  surfaces are `WatchChangeRefs` (internal), `Client.Watch`/`WatchList`, `WatchEvents`,
  `WatchSchedule`.
- `TODO.md`: drop anything superseded.
- `README.md`: only if it documents the per-kind waker.

---

## Commits

One per cycle, each green:

1. `feat(sqlite): publish object changes to a store-wide conflating hub`
2. `feat(store): add WatchChangeRefs, a blob-free change stream`
3. `perf(sqlite): drain change refs in batches`
4. `test(sqlite): pin that batching preserves per-object conflation`
5. `fix(sqlite): close change-ref streams on store close and ctx cancel`
6. `refactor(waker): resolve many targets' dependents in one query`
7. `fix(waker)!: subscribe one store-wide change stream instead of one per kind`
8. `perf(controller): skip the waker when no controllers are registered`
9. `fix(waker): report change-stream failures as process-wide`
10. `refactor(store)!: drop the per-kind WatchChanges stream`
11. `docs: describe the store-wide dependency change stream`

`!` on 7 and 10 marks the `internal/storeapi` contract change, matching the repo's existing
convention (the package is not consumer-importable, but the store contract is the thing
that broke).
