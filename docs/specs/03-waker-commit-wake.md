# A commit wake in front of the dependency waker

**Status:** designed, not built, **unblocked**: piece 0 shipped in gobus v0.6.0 as
`watch.Hub.WatchAcross`, and bumping `go.mod` is all that stands in front of the code.
The shape is settled; two numbers are deliberately left to the build — the value of
`defaultWakeScanMinInterval`, which ships only once the bench in piece 4 has run, and
whether `wakeScanPagesPerPass` drops with it.

## Problem

Case 6 in [`reconcile-triggers.md`](../reconcile-triggers.md) — an ordinary change to
a target waking its dependents — has no push. `waker.scan` is the only route from
"target committed" to "dependents enqueued", and it runs on a 1s tick.

The cost is **one tick per hop**. A chain of depth D settles in about D seconds no
matter how fast the store commits, because each link waits for the next tick to
notice the link before it. This is the largest latency gap in the system, and it is
on the path most controllers actually use.

## What exists

`objectTailer.run` already runs the exact pattern this spec wants: a commit wake in
front, a floor tick behind, one timer re-armed after every pass, and a flag that
drops wakes while the loop is recovering from a failure.

The waker differs from the tailer in three ways that matter:

- It is **store-wide**, not per kind. A `depends_on` edge can point at a client-only
  kind that no per-kind query would name. So it subscribes to every key of a hub the
  tailer subscribes to one key of.
- It is **unconditional**. A tailer runs only while something watches, so its duty
  cycle is demand-scoped. The waker's is not. That is why the throttle is in scope
  here and still deferred for the tailer ([`TODO.md`](../TODO.md)).
- Its cursor is **persisted** in `driver_cursors`, and resuming it is a real
  workload — `wakeScanPagesPerTick` exists so a long resume cannot monopolise the
  single connection. **Rename it `wakeScanPagesPerPass`** in this change: once a pass
  can be wake-driven, "per tick" names a thing that no longer bounds it.

One asymmetry with the tailer, worth stating so nobody copies `drain` wholesale:
**the waker needs no quiet-wake position read.** `drain` pays one
`ObjectWritesMaxVersion` before listing because a quiet wake is common; the waker's
quiet scan is already a single `ObjectWritesListSinceAll` that comes back empty. A
scalar pre-read would buy the same answer for the same one query.

## Proposal

Wake the waker when a write commits, and keep the tick as a floor.

The signal is **not** the changed object's id, and not its kind. It is "something was
written": the waker holds one store-wide cursor and reads `object_writes` to learn
what moved, so a burst of commits collapses into one wake.

Five pieces, in the order they should be built.

### 0. `watch.Hub.WatchAcross` — **shipped in gobus v0.6.0**

```go
func (h *Hub[K, V]) WatchAcross(initial V) *Receiver[K, V]
```

A receiver that watches every key, including keys nobody has published under yet and
keys the consumer cannot name — which is the waker's case exactly, since a `depends_on`
edge may point at a kind with no controller.

The properties this spec relies on, all now stated upstream rather than assumed here:

- **One slot, and the collapse is the contract.** The package doc says it in as many
  words: *"a burst across many keys collapses to a single pending value naming the last
  key to land. That collapse is the point."* A burst across N kinds is one wake.
- **Routing stays exact.** `sendLocked` fans out to `index[k]` and then to `wildcard`
  (`watch.go:459`), so no predicate runs on the commit path. `wildcard` is a second map
  rather than a reserved `index` entry precisely so a wildcard receiver cannot appear
  in the hub's live key set.
- **A wildcard receiver pins no key state**, so `deregisterLocked`'s "a key costs
  nothing once its last watcher has gone" still holds with one registered.
- **`Event.Key` names the key of the value in the slot** — `offerLocked(k, v)` writes
  it as the value lands (`:542`).
- **Registration is still the snapshot.** Both constructors go through one `watch(k,
  wildcard, initial)` (`:269`), so seeding, the pre-closed case, and the ordering that
  makes registration a snapshot cannot drift between them.
- `Accept`, close discipline, and the `Sender.Close`-versus-`Send` promise are
  unchanged. Note `Accept` compares across keys for a wildcard receiver; beehive sets
  none on this hub.

**Remaining action: bump `go.mod` to v0.6.0.** Nothing else here is blocked.

*(The `conflate.Sender.Close` concurrency promise from v0.5.2 is not wasted work — the
doc gap was real and `conflate` remains beehive's object-watch fan-out bus — but this
spec no longer depends on it.)*

### 1. The waker subscribes to every key of the hub that already exists

`kindWriteHub` **does not move**. The tailer keeps its `Watch(gk, …)`, the waker adds
a `WatchAcross(…)`, and `signalKindWritten` — seven call sites: `client.go` (4),
`controller.go` (1), `gc.go` (2) — is untouched. One hub, one `Send`, no swap.

**Why not `conflate`.** An earlier draft of this spec moved the hub to `conflate` and
gave the waker an unfiltered receiver. That works, and gobus's README does route wide
subscriptions there — but for *this* hub it loses on every axis:

| | `watch` + `WatchAcross` | `conflate`, unfiltered |
|---|---|---|
| Send routing | exact index: the receivers for that kind, plus wildcards | **every** receiver, each evaluating a `keep(gk)` closure, under the hub lock |
| The waker's pending signal | one slot — a burst across N kinds is **one** wake | one slot per key — N kinds is N wakes |
| Registration ordering | "registration is the snapshot" | the weaker "no replay" |
| The tailer | untouched | rewritten onto another bus, under a shipped ADR |
| Upstream work | one option | a declared-key index — i.e. making `conflate` watch-like |

`conflate` stays exactly where it belongs, one layer down: the tailer's own
`conflate.Hub[ObjectID, rawChange]` needs per-key queues, a real merge with
annihilation, and `TryRecvAll`'s batch cut. A wake signal wants none of that. The two
packages keep their roles rather than converging.

**One cost survives the reversal**, and it belongs in piece 4's table rather than in a
bullet that reads as reassurance: **a quiet process stops being free.** `watch`'s
`Send` has the same `live.Idle()` fast path, and the waker subscribes for the life of
the process — so the hub is never idle in a running beehive. Today a beehive with no
watches pays nothing per commit; after this change every `signalKindWritten` takes the
hub mutex and offers to one receiver. That is caused by having an unconditional
subscriber, not by which bus carries it, and it is the honest price of the wake.

`kindWriteHub` keeps embedding `watchHub`, so its zero-value guard, its close
discipline, and the `Send`-versus-`stop` promise `watchhub.go:24-30` and
`beehive.go:326-333` already cite all keep working as they are.

The one piece of plumbing: `watchHub` has `watch(k, initial)` and needs a
`watchAcross(initial)` beside it, with the same nil-hub guard returning `ok == false`
— then `kindWriteHub.WatchAcross()` mirrors its existing `Watch(gk)`. A `Beehive` built
field by field therefore still reports `ok == false`, and the waker runs tick-only
there.

**The tailer is not touched at all**, which is the largest single win over the swap
draft: no equivalence argument to make, no shipped ADR to re-open, and
`TestTailerDeliversOnWake`, `TestTailerDrainsBurstAbovePageCap` and
`TestWatchLoadsAreBatchedPerDrain` keep passing because nothing beneath them moved.

**A write that appends an `object_writes` row without reaching `signalKindWritten`
costs latency, never coverage** — the floor tick still lists it. The emit set is
derived from the store's write-log call sites rather than from the public verbs
(see `CLAUDE.md`); confirm it still covers them when building, and if one is missing,
that is a separate fix with its own test.

### 2. The waker's loop

`waker.run` stops using `driver.Run` — it now selects over two sources — and becomes
the tailer's shape:

```go
func (dw *waker) run(ctx context.Context) {
    if len(dw.bh.order) == 0 || dw.bh.wakeInterval <= 0 {
        return
    }
    // Built here, never in New: New constructs the waker (beehive.go:363)
    // *before* it applies options (:365), so a gate in that struct literal would
    // capture the default and silently ignore withWakeScanMinInterval.
    dw.gate = rategate.New[struct{}](dw.bh.wakeScanMinInterval)

    // Every kind, one slot. Registered before the first scan, for
    // newObjectTailer's reason: a write landing between the subscribe and the
    // seed read would be missed by both.
    rx, ok := dw.bh.kindWriteHub.WatchAcross()
    var written <-chan gobus.Event[GroupKind, struct{}] // nil blocks forever: tick only
    if ok {
        defer rx.Close()
        written = rx.Chan()
    }
    timer := time.NewTimer(0) // the eager first pass driver.Run gave us
    defer timer.Stop()
    backingOff := false
    for {
        select {
        case <-ctx.Done():
            return
        case _, open := <-written:
            if !open {
                return // safety net; see below — ctx normally wins
            }
            // Consumed rather than skipped, so the closed arm above stays live.
            if backingOff {
                continue
            }
        case <-timer.C:
        }
        var next time.Duration
        next, backingOff = dw.pass(ctx, dw.now())
        rearm(timer, next)
    }
}

// pass runs one scan under the throttle and reports how long to wait and
// whether wakes are being dropped. Split from run so the rate tests drive it
// with explicit times instead of sleeping.
func (dw *waker) pass(ctx context.Context, now time.Time) (time.Duration, bool) {
    floor := dw.bh.wakeInterval
    if opensAt, held := dw.gate.Allow(gateKey, now); held {
        return opensAt.Sub(now), false // the deferred wake *is* the memory of it
    }
    switch dw.scan(ctx) {
    case scanMore:
        if d := dw.gate.Interval(); d > 0 {
            return d, false // keep draining, at the throttle's rate
        }
        return floor, false
    case scanFailed:
        return floor, true
    }
    return floor, false
}
```

Notes an implementer needs:

- **The gate is built in `run`, and its interval lives on `Beehive`** as
  `wakeScanMinInterval`, beside `wakeInterval`. This is the trap `workQueue.setFloor`
  (`workqueue.go:190`) already exists to dodge: `New` builds the waker before it
  applies options, so anything the waker reads at construction reads the default. A
  gate built in the struct literal would make every throttle test pass against the
  default and pin nothing.
- **`dw.now` is the waker's one clock**, a field defaulting to `time.Now`. Four new
  tests and two existing ones are rate assertions, and the repo forbids sleeps
  ("synchronize on signals, never on sleeps"). `rategate` is already pure — it takes
  `now` — so the impurity is confined to this field. It is read in two places, and
  piece 3 says why: `run` reads it and hands `pass` an explicit `now`, while `persist`
  reads it directly rather than have `scan` grow a parameter for it. Decide this before
  the loop lands; retrofitting it after is worse.
- **`rearm(timer, d)` does not exist yet.** The `Stop`-then-`Reset` pair is inlined at
  `objectswatch.go:434`. Extract it as a package helper and move the tailer onto it in
  the same commit, or this ships a second copy of a pattern that is easy to get subtly
  wrong.
- **`timer`, not `time.Ticker`.** Every pass re-arms it, which is what lets one timer
  serve the floor, the throttle re-arm and the failure delay.
- **`wakeInterval <= 0` still turns the waker off entirely**, wake included. It is the
  documented meaning of the option and one test pins it.
- **A refused wake is not lost.** Re-arming the timer to `opensAt` *is* remembering
  it: the scan that runs then reads its position from the store, so the wake carries
  nothing that could have been dropped.
- **Skip the re-arm when the timer already fires soon enough.** The throttle bounds
  *scans*, not loop iterations. `WatchAcross` collapses across kinds, so a burst over W
  kinds is one wake rather than W — but the single slot refills on every `Send` once
  the feeder has taken the previous value, so under a sustained stream the loop can
  still iterate at roughly commit rate: wake, `pass` refuses at the gate, `rearm` does
  a `Stop`+`Reset`, where the loop previously ran exactly 1/s. Track the deadline the
  timer is armed for and skip the re-arm when it is already at or before the new one.
  Cheap per iteration, but it is unbounded by the one mechanism this spec added to
  bound things, and leaving it unbounded should at least be a decision.
- **`scanMore` re-arms at the throttle interval, and at the floor when the throttle is
  disabled** — the second being exactly today's behaviour. This is the one behaviour
  change beyond latency: a scan that stops at `wakeScanPagesPerPass` no longer waits a
  full second for the next page. It follows from the same argument as the wake, and it
  is what makes a resume finish in throttle intervals rather than seconds.
- **A failure re-arms at the floor and drops wakes until it fires.** The waker needs
  no backoff ladder: the tailer's ladder is capped at its own floor, and the waker's
  floor is 1s, so "wait the floor" *is* the ladder's terminal state. Dropping wakes
  meanwhile is the tailer's argument verbatim — a commit landing during a failed scan
  refills the wake slot, so a live writer would otherwise keep a degraded store
  re-reading as fast as it can fail.

### 3. `scan` reports what happened, and its cursor write gets its own floor

The scan body is unchanged in what it *reads*: seed, cursor, paging, the self-edge
skip, the hold-on-failure behaviour and every log line stay. **One thing it writes
does have to change** — see the sub-section below, which is the only place this spec
touches `persist`.

`scan` stops returning nothing and returns a `scanResult` instead, which is the only
thing the loop can dispatch on:

| Result | When | Loop's response |
|---|---|---|
| `scanIdle` | empty page, short page, or a seed with nothing behind it | floor |
| `scanMore` | stopped at `wakeScanPagesPerPass` with a full page, **or a seed that resumed below the mark** | throttle interval |
| `scanFailed` | a page read, an edges lookup, or a seed failed | floor, wakes dropped |

A shutdown-cancelled read returns `scanFailed`; the loop's `ctx.Done()` arm wins on
the next pass, so it is never observed.

**A resumed seed reports `scanMore`, and this is not a detail.** `seed` already reads
`ObjectWritesMaxVersionAll`, so it knows whether the cursor it resumed from is below
the mark — i.e. whether a backlog exists. Returning `scanIdle` there would make a
restart with a large backlog wait a full floor before drawing page one, which
contradicts piece 2's own claim that a resume finishes in throttle intervals. The
comparison is free; make it. A fresh seed (no stored cursor) starts *at* the mark and
is therefore `scanIdle`.

**Seeding on a wake is still forbidden**, and it needs no guard beyond this table: an
unseeded pass seeds and then waits — it never scans in the same pass — so a failed
seed returns `scanFailed` and a wake cannot turn it into a scan.

#### The cursor write needs a floor of its own

`scan` persists on the way out (`defer dw.persist(ctx)`, `waker.go:169`), gated only
on `watermark > persisted` — which under a sustained write stream is true on **every**
pass. So a 10×-faster loop is a 10×-faster `DriverCursorsSet`, and that is the one
number in this spec that is not a read: a bare statement outside any transaction, on
SQLite's single writer, contending with exactly the commits generating the wakes.
Every other cost here competes for read time; this one competes for the write lock.

**Gate the persist at `wakeInterval`** — a second `rategate.Gate[struct{}]` on the
waker, consulted inside `persist` **before the `persistSkips` decrement**
(`waker.go:204`), not merely before the write. The order is load-bearing: a gate below
the decrement would let refused calls keep burning skips at 10/s, so
`wakePersistRetryCap` would still collapse to ~6s — the exact bug this sub-section
exists to repair. Gate first, then the skip ladder, then the write. The cursor write rate then stays
exactly what it is today no matter how fast the loop turns. This costs nothing that
matters: the cursor is an optimisation over the stale-dependents pass
([ADR](../adr/2026-07-30-durable-waker-cursor.md)), `persist` always writes the
current `dw.watermark` rather than a per-page delta, and a crash with a ≤1s-stale
cursor replays wakes that are idempotent. That is already the exposure today.

**It also repairs a bug this change would otherwise make worse.**
`wakePersistRetryCap = 60` is documented at `waker.go:67` as *"a minute at the default
wake interval"*, but `persistSkips` decrements **per persist call** (`waker.go:204`),
not per second. At 10 passes/s that cap becomes ~6s — the retry backoff for a failing
cursor write would shrink 10× during exactly the outage it exists for. Gating persists
at `wakeInterval` restores one persist attempt per second, so the constant means a
minute again by construction. Leave a one-line invariant comment at the constant
saying the gate — and its position ahead of the decrement — is what keeps it in
seconds; a wall-clock backoff is the alternative and is strictly more machinery for
the same result.

**The clock seam for this gate is `dw.now`, read inside `persist` itself.** `persist`
is reached through `scan(ctx)`, which takes no `now`, and threading one down would
change `scan`'s signature at **34 call sites in `waker_test.go` alone** — for a single
deep consumer. So the rule for the whole waker is: one clock field, `dw.now`, defaulting
to `time.Now`; `run` reads it and hands an explicit `now` to `pass`, because the
loop-level rate tests want to drive passes at chosen instants; `persist` calls it
directly. Two call styles, one seam, and no signature churn.

**This gate is the one change in the spec that breaks existing tests**, and they must be
updated rather than worked around — see Tripwires.

### 4. The throttle

Removing the tick as the only pacing on this path needs a floor between wake-driven
scans, or a sustained write stream holds the single connection at full paging budget.
Two throttles are in play and they are not the same one:

**The cycle rate is already handled.** The work queue's re-enqueue floor
([ADR](../adr/2026-08-04-work-queue-re-enqueue-floor.md)) bounds one object to one
dispatch per interval whatever the wake rate, and `defaultMinRequeueInterval` was set
to `defaultWakeInterval` precisely so this spec could add the wake without changing
the rate at which two mutually dependent objects round trip. **If that constant is
ever raised, re-read this paragraph before shipping the wake.**

**The scan rate is what this spec adds.** Use `internal/rategate`, so the waker's
limit and the tailer's deferred one are the same code rather than two curves that
drift apart. `rategate` is eager on the first admission after a quiet period, which
is precisely the shape [`TODO.md`](../TODO.md) asks for: an idle-to-active transition
pays no added latency, and only a sustained stream is paced.

- Field: `gate *rategate.Gate[struct{}]` on `waker`, one key (`gateKey = struct{}{}`),
  **built in `run`, never in `New`** — see piece 2. A single-key gate does not need the
  eviction queue, and using it anyway is the point: four gate instances — the work
  queue's floor, this one, the persist gate in piece 3, and the tailer's deferred
  one — over one implementation.
- Interval: `Beehive.wakeScanMinInterval`, defaulting to
  `defaultWakeScanMinInterval`, with an unexported `withWakeScanMinInterval`
  dispatching on `*Beehive` like its neighbours; non-positive disables it, which
  `rategate.New(0)` already means.
- **It is a new constant, not a fraction of `wakeInterval`.** Deriving it would tie
  wake latency to the floor, and the floor is the number this spec explicitly leaves
  free to be raised later.

**The starting value is 100ms and it does not ship unmeasured.** An order of magnitude
under the floor buys a ≤100ms chain hop instead of ≤1s. What it costs is **duty
cycle**, and unlike the tailer's deferred throttle this loop runs unconditionally:

| | today | after, worst case |
|---|---|---|
| Passes per second | 1 | 10 |
| Read queries per second | 1 list (+1 edges when non-empty) | ~10 list + ~10 edges |
| **Cursor writes per second** | 1 | **1** — gated at `wakeInterval`; **10 if the gate in piece 3 is skipped** |
| Resume rate | 16 pages/s | 160 pages/s, at startup |
| Per-commit hub cost, nothing watching | nil (`live.Idle()` fast path) | one hub mutex + one offer, always |
| Per-commit fan-out | receivers watching that kind | unchanged — the wildcard receiver adds one, and routing stays indexed |

The TODO's tailer analysis holds here too — cost *per write* falls as the rate rises,
because a page batches — but the fraction of time the loop holds the single
connection is what competes with the writers generating the wakes, and that fraction
is what rises. So:

- **Add `BenchmarkWakerScanRateUnderSustainedWrites` to `waker_bench_test.go`** (which
  exists and this spec would otherwise never touch): a write stream at a fixed rate, a
  wake per commit, measuring passes and queries per second and the connection time
  they consume. **It must count cursor writes separately from reads** — a bench that
  reports one aggregate query number sets `wakeScanMinInterval` from a picture missing
  the only figure that contends for the write lock.
- **The bench sets two numbers, not one.** If it shows the loop starving writers,
  the lever is `wakeScanPagesPerPass`: re-arming 10× more often at 16 pages a pass is
  10× the resume load, while 4 pages a pass at 10× the frequency is the same aggregate
  resume throughput in shorter holds — strictly better for a writer waiting on the
  connection. Decide both together, from the same bench.
- Nothing else in this spec depends on either value.

**`rategate` gains exactly one method, `Allow`**, added with this consumer:

```go
// Allow is OpensAt followed by Admit when k is free: it reports the same
// (opensAt, held) pair and records the admission when held is false. For a
// caller whose test and action are at one point.
func (g *Gate[K]) Allow(k K, now time.Time) (time.Time, bool)
```

The pair keeps `OpensAt`'s polarity so a reader never has to flip it. **`NextAt` —
the earliest-held-key query the earlier draft proposed — is not built here.** The
waker holds one key, so `OpensAt` already answers it, and the rule is that a method
lands with its consumer.

## What must stay true

- **The floor tick stays.** This makes the waker a driver with a wake in front, like
  the watch tail. It does not make it a push path. `wakeInterval` still bounds the
  worst case, a non-positive interval still turns the waker off, and the
  stale-dependents pass is still the guarantee underneath.
- **A wake is an optimisation twice over.** The waker is already an optimisation over
  the stale-dependents pass. A lost wake costs latency against a lost tick, which
  costs latency against a lost sweep. Nothing here is load-bearing, and no durable
  record is added or removed.
- **The cursor still records what was scanned, never what was woken.** A wake that
  fires and finds nothing must not move it further than the scan reached.
- **The waker still resolves reconcilers under `bh.mu`**, and `stop` still cancels
  `runCtx` before `wg.Wait`. Nothing in this change may make the waker hold a lock
  across a channel receive.

## Resolved questions

**Per kind or store-wide? — Store-wide, on the one hub, via `WatchAcross`.** The first
draft said "reuse `kindWriteHub` and discard the key", which was right about the hub
and had no mechanism behind it. Three were considered: a second keyless hub (a second
`Send` per write), swapping the hub to `conflate` (predicate routing on the commit
path, N wakes for N kinds, and a shipped ADR re-opened), and giving `watch` the
wildcard subscription it lacks. The third is the only one that costs nothing on the
write path and leaves the tailer alone — and one slot per receiver, which is `watch`'s
existing invariant, is exactly the collapse a "something changed" subscriber wants.

**Ignore a wake during a drain or a failure? — Yes, and copy the tailer's mechanism as
well as its reasoning.** *"A wake carries no information — the drain reads its
position from the store — so dropping one loses nothing."* The tailer **consumes** the
wake and continues rather than skipping the receive, which is what keeps the
closed-channel arm live. During a drain the throttle does the absorbing; during a
failure `backingOff` does.

**What happens to `wakeInterval`'s default? — Nothing, in this change.** 1s is
load-bearing twice: as this driver's floor and as the value `defaultMinRequeueInterval`
matches, which is what keeps a cycle at exactly the rate it has today. Raising it is a
separate measurement with its own argument, and bundling it here would make that claim
untestable in the same diff.

**Does the hub close race a write's `AfterCommit`? — Already answered, and the answer
still holds.** `stop` closes the sender, not the hub; `gobus/watch.Sender.Close`
promises safety against a concurrent `Send` (`watch.go:322`), so a racing send either
publishes or answers `ErrClosed`, never both. Keeping the hub on `watch` is what keeps
that citation in `watchhub.go:24-30` and `beehive.go:326-333` true without further
work. The waker's closed arm just returns — unlike the
tailer it has no subscribers to hand `ErrStopped` to. It is also, unlike the tailer's,
a **safety net rather than the normal exit**: the waker is in `wg`, which `stop` waits
on before closing anything, so `ctx.Done()` gets there first except when the drain
times out. Keep the arm — a loop with no case for a closed channel spins on it — and
do not describe it as how the waker ends.

## Tripwires

`TestWakerSeedsFromTheWriteLogMax`, `TestWakerSeedsFromTheStoredCursor`,
`TestWakerRetriesSeedOnTheNextTick` and `TestWakerRetriesSeedOnAFailedCursorRead` pin
the seed contract. A wake must not seed, and must not turn a failed seed into a scan.

`TestWakerHoldsTheWatermarkOnLookupFailure`, `TestWakerHoldsTheWatermarkOnScanFailure`,
`TestWakerStopsAtThePageBudget` and `TestWakerResumesAnEnormousBacklog` pin the drain.
All must hold with wakes arriving mid-drain; the first two also pin that a held
watermark is still persisted.

`TestWakerScanWakesDependentsByTheirOwnKind`, `TestWakerSkipsTheSelfEdge` and
`TestClientOnlyTargetWakesDependent` (in `reconciler_test.go`) pin the scan semantics,
which do not change. **The last one does not pin the unfiltered subscription**, though
it is the case that motivates it: it is tick-driven, so it would pass against a
per-kind receiver. `TestWakerWakesOnAWriteToAnyKind` below is the one that pins it.

`TestWakerDisabledByNonPositiveInterval` pins that the option still turns everything
off. `TestStartWithNoControllersSkipsWaker` pins the other early return.

Nothing pins the current wake latency, so nothing fails if the wake silently does not
fire. Nothing pins the current *scan* cadence either, so the scan gate trips no
existing test: every waker test drives `scan`/`seed` directly and none enters `run`.

**The persist gate is the exception, and it is the only part of this spec that edits
existing tests.** `persist` is reached directly by tests that make several calls at one
wall instant, which a 1s gate refuses:

- `TestWakerRetriesPersistOnAFailedWrite` (`waker_test.go:639`) — two back-to-back
  `dw.scan()` calls asserting the second one's write lands (`setCalls == []int64{3}`).
  Gated at one instant, the second is refused and the assertion sees nothing.
- `TestWakerBacksOffAFailingPersist` (`:661`) — 30 scans, `setErr = nil`, 30 more,
  asserting `setCalls == []int64{3}` and `persistFailures == 0`. All 60 land at one
  instant, so the write never lands and the streak never closes.

Both pin exactly the retry semantics the gate is meant to preserve, so **fix them by
advancing `dw.now` by the floor per simulated tick**, which is what "per tick" already
meant in them, and keep every assertion as it stands. A test that passes only because
its assertion was weakened has been deleted, not fixed.

New tests, in `waker_test.go` unless noted. The rate assertions
(`…DropsWakesWhileBackingOff`, `…ThrottlesWakeDrivenScans`, `…FirstWakeAfterAQuietPeriodIsEager`)
drive `pass(ctx, now)` with explicit times — **no sleeps, and no wall-clock
cadence measured through `run`**:

- `TestWakerScansWhenAWriteCommits` — a dependent is enqueued at commit with
  `wakeInterval` set long enough that the tick cannot be the cause. This is the test
  the whole spec exists for.
- `TestWakerWakeDoesNotSeed` — a wake arriving before the first successful seed leaves
  the waker unseeded and scans nothing.
- `TestWakerDropsWakesWhileBackingOff` — a failing scan plus a wake per failure does
  not raise the read rate above the floor.
- `TestWakerThrottlesWakeDrivenScans` — a burst of wakes yields one scan, and the next
  is admitted only after the throttle interval.
- `TestWakerFirstWakeAfterAQuietPeriodIsEager` — the property `rategate` was chosen
  for; a throttle that lost it would pass the previous test and defeat the spec.
- `TestWakerKeepsDrainingPastThePageBudget` — `scanMore` re-arms at the throttle, not
  at the floor.
- `TestWakerRunsWithoutAWriteHub` — a `Beehive` built field by field (zero hub) still
  runs on the tick alone.
- `TestWakerWakesOnAWriteToAnyKind` — **wake-driven**, and the only test that pins the
  wildcard subscription: a commit to a kind the waker never names wakes it, with the
  floor set long enough that the tick cannot be the cause. A waker accidentally
  subscribed per kind must fail here and nowhere else.
- `TestWakerCollapsesABurstAcrossKinds` — writes to several kinds between two reads
  deliver one wake, not one per kind. This is a `WatchAcross` property the beehive relies
  on for its loop-iteration bound, so pin it here rather than only upstream.
- `TestWakerKeepsDrainingAfterAResumedSeed` — a seed that resumed below the mark
  re-arms at the throttle, not the floor.
- `TestWakerPersistsAtMostOncePerFloor` — many passes with a moving watermark yield
  one `DriverCursorsSet` per `wakeInterval`, driven by advancing `dw.now` rather than
  by counting passes. Its companion is the *edit* to `TestWakerBacksOffAFailingPersist`
  above, which must still mean seconds rather than passes once the clock moves.
- `TestWakerClosedHubArmReturns` — the closed arm, driven **directly** by closing the
  hub on a running waker. Not through `stop`: `stop` cancels `runCtx`, then `wg.Wait`s,
  and closes the hub only after (`beehive.go:334`), and the waker *is* in `wg`
  (`:172`) — so `ctx.Done()` always wins and the closed arm is reachable only when the
  drain hits `stopCtx`'s deadline. It is a safety net, not the normal path, which is
  the reverse of the tailer (not in `wg`, closed arm is how it ends). A test routed
  through `stop` would pin nothing.
- The pull-path test the README's rule demands already exists in spirit: the waker
  tests drive `scan` directly, which issues no wake. Keep at least one end-to-end
  waker test that writes through the store rather than through a client verb.

## Docs to update when this ships

- [`reconcile-triggers.md`](../reconcile-triggers.md): case 6 gains a **Push**; the
  section 1 prose *"A target change does not push today"* and its neighbouring claim
  that every push is confined to a registered kind are both now wrong (the wake is
  store-wide, though what it *enqueues* is still per registered kind); the pull-driver
  table's waker row gains "with a commit wake in front", as the watch tail's does; and
  section E's *"`signalKindWritten` … is the only commit-driven push in the system
  today"* note needs correcting — it is already stale after the deletion push shipped.
- [`CLAUDE.md`](../../CLAUDE.md): the driver bullet listing "the dependency waker
  (write log, 1s)" gains the wake, in the same shape as the watch tail's entry; the
  watch bullet's `signalKindWritten` sentence gains its second reader. **This change
  adds no `AfterCommit` user** — the waker rides the existing hook, which is the point
  of piece 1. Separately, the "five users" list is already wrong: `signalKindWritten`
  is itself an `AfterCommit` user (`beehive.go:500`) and is missing from it. Add it as
  the sixth, and say in the commit that the correction is pre-existing, not this
  change's doing.
- [`TODO.md`](../TODO.md): the object-watch-tail throttle entry says the shared limit
  should be built when the two are compared side by side. It ships here — narrow that
  entry to "the tailer has not adopted it", and point it at `rategate.Allow`.
- [`specs/README.md`](README.md): mark item 3 shipped with a link to the ADR, and drop
  the waker row from the latency table.
- [`adr/2026-08-03-watch-shared-tail.md`](../adr/2026-08-03-watch-shared-tail.md): its
  "Commit → tailer" bullet describes the hub as feeding tailers. It now has a second,
  wildcard subscriber that is not a tailer — one line. The bus does not change, so its
  *"`gobus/watch` is a state bus and this is a signal; the fit is close enough"* caveat
  stands as written.
- Write the ADR (`docs/adr/2026-08-__-a-commit-wakes-the-dependency-waker.md`) and
  delete this spec. The rationale it must carry: why the wake subscribes to every key
  of the existing hub rather than getting one of its own, why that meant a wildcard
  subscription in `gobus/watch` rather than a move to `conflate`, what the wake costs
  on the commit path (a hub that is never idle), why the cursor write keeps a floor of
  its own when nothing else does, why the throttle is `rategate` and why its interval
  is not derived from the floor.

## Done when

- A dependent is enqueued when its target's write commits, not on the next tick.
- A chain of depth D propagates in D commits, bounded below by the work queue's
  re-enqueue floor rather than by the waker.
- With the wake disabled — a zero hub, or a store whose writes bypass the signal —
  every waker test above still passes at the tick cadence.
- A restart with a backlog draws its first page at the throttle, not a floor later.
- A sustained write stream cannot drive more than one scan per
  `wakeScanMinInterval`, and a failing store cannot drive more than one per
  `wakeInterval`.
- **The duty cycle is measured, not assumed.** `BenchmarkWakerScanRateUnderSustainedWrites`
  exists, has been run, and `wakeScanMinInterval` and `wakeScanPagesPerPass` are both
  set from it. A resume's load is bounded by that same pair, and the trade it
  represents — denser reads at startup, when the store is busiest — is stated in the
  ADR.
- The rate tests contain no `time.Sleep`, the two edited persist tests keep every
  assertion they have today, and no existing waker test was weakened to accommodate a
  gate.
- **The cursor write rate is unchanged from today** at any wake rate, and
  `wakePersistRetryCap` still means a minute.
- `go.mod` points at gobus v0.6.0, and `kindWriteHub` is still the same hub on the same
  bus — the waker's subscription is the only thing that was added to it.
- The docs listed above are updated in the same change, and this file is gone.
