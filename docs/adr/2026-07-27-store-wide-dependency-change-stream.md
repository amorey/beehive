# One store-wide change stream for the dependency waker

- **Status:** Accepted — implemented in `beehive.go`, `sqlite/watch.go`, and
  `internal/storeapi/storeapi.go`.
- **Date:** 2026-07-27

## Context

`Start` subscribed one dependency waker per **registered** kind:

```go
for _, r := range bh.order {                       // beehive.go, before
    w, err := bh.store.WatchChanges(runCtx, r.gk)
    ...
    bh.wg.Go(func() { bh.runDependencyWaker(runCtx, r.gk, w) })
}
```

`bh.order` is the registered controllers. A `depends_on` edge may point at **any**
object, including one of a *client-only* kind — used through `Client`, never
`Register`ed, and therefore with no reconciler and no entry in `bh.order`. That
shape is ordinary: configuration, secrets, any reference data the application
writes and controllers read.

Changes to such a target reached no waker. Not a dropped wake — **none was
attempted**. Nothing observed the change, so `wakeDependents` never ran, so no
dependent was queued.

The *dependent* side had always been handled: `wakeDependents` routed each
dependent through `enqueueIfRegistered`, so a client-only dependent was ignored
deliberately. The two are not symmetric. An unregistered dependent has nothing to
reconcile; an unregistered target is an ordinary object whose changes other kinds'
controllers have asked to hear about.

### What covered it, and what did not

| | covered it? |
|---|---|
| the dependency waker | no — no waker existed for that kind |
| `ObjectsListUnsettledIDs` (catchup tick) | no — the dependent is settled; its own generation never moved |
| `WakesListPendingIDs` (catchup tick) | only if the edge was declared against an already-moved target, a different case |
| `WithStartupResync` | **yes**, and only at the next process start |

The last row held for a narrow reason: `enqueueAll` (the startup full pass)
deliberately includes already-settled objects, while `enqueueCatchup` —
`ObjectsListUnsettledIDs` + `WakesListPendingIDs` — structurally cannot see a settled
dependent. So the sole cover was a default, not a mechanism, and two supported
configurations remove it: under `WithStartupResync(false)` a dependent of a
client-only target never learned its target moved, for the life of the store.

### The case no configuration recovered

`edges.to_id` is `REFERENCES objects(id) ON DELETE RESTRICT`
(`sqlite/migrations/0001_init.sql:152`), so a target with dependents cannot be
physically removed. `Client.Delete` sets the tombstone and emits `Modified`; the
row stays deletion-pending until every incoming edge is gone, and only a
dependent's own reconcile can drop that edge. With no waker for the target's kind
that `Modified` reached nobody, so the dependents never woke, nothing dropped the
edge, and the GC sweeper retried the row every tick — forever, inside a single
running process. `WithStartupResync` fires at the *next* start, so nothing
recovered it.

## Decision

Replace the per-registered-kind waker with a single store-wide one.

```go
// internal/storeapi — replaces WatchChanges(ctx, gk), which had no other caller
type ObjectWrite struct {
    ID   ObjectID
    Type ChangeType
}

// A concrete Subscription[V]; see the naming ADR for why the three
// stream-specific interfaces collapsed into one.
type ObjectWritesSubscription = Subscription[[]ObjectWrite]

ObjectWritesSubscribe(ctx context.Context) (*ObjectWritesSubscription, error)
```

`Start` runs one waker over one subscription (`dependencyWakerStart`,
`beehive.go:320`). Routing needed no change: it was never keyed on the
subscription — `dependentsWake` (`beehive.go:402`) enqueues each dependent
through `enqueueIfRegistered`, by the *dependent's* own kind.

**It is the only option that cannot go stale.** Any per-kind subscription list has
to be computed from something, and every candidate misses a case: the registered
set (the defect), the kinds present in the store at `Start` (misses a kind whose
first object is created later — an ordinary thing for a client-only kind), or the
kinds currently referenced by an edge (changes on every `DependenciesAdd`, so it is
correct only if that path is never missed). A global stream asks no such question.

The store was already close to it. `objects.id` is one `AUTOINCREMENT` primary key
for the whole table and `ObjectsGet(ctx, id)` takes no `GroupKind`, so a global
conflating hub keyed by `ObjectID` coalesces per object exactly as the per-kind
hubs do. It is an eager field created in `open` and closed in `Close`, not a lazy
`hubFor` entry: there is one and no key to look it up by, and laziness would cost
the publish path — which runs on every object write — a second lock and map
lookup.

### The stream carries identity, not rows

The hub value is `writeSignal{typ, rv}` (`sqlite/watch.go:124`) and the stream
delivers `ObjectWrite{ID, Type}`. This stream sees every write in the process, so
a `*RawObject` sitting in an undelivered slot would pin that row's spec and status
JSON — the same reason `watch()` nils its snapshot slice the moment it is emitted.

Nothing is lost that the payload actually provided. Conflation already meant a
lagging consumer never saw intermediate states: `changeMerge` keeps one value per
id, latest-version-wins, so the row a consumer eventually popped was "current-ish
state", not "the state at the time of the change it is dequeuing". And beehive is
level-triggered — a consumer must re-read to be correct regardless, since the
object can change between publish and dequeue. Payload-free forces the pattern the
architecture already requires. (The wire-facing analogue, `Client.ObjectsWatch`'s
`Change[Spec, Status]`, still carries the object: a remote consumer maintaining a
client-side map cannot cheaply re-read, and a reconciler can and must.)

### The batch is drained at the receiver

Wake resolution costs one edges lookup per changed target. Per-kind, that was one
lookup per change on registered kinds; store-wide it would be one per change in
the *store* — and the new load is precisely the high-write client-only kinds that
motivate the change. The store is `SetMaxOpenConns(1)`, so those reads serialize
against every writer: per-change resolution would let a hot config kind tax every
write in the process.

So the feeder blocks on `RecvContext` for the first value, then drains with
`conflate.Receiver.TryRecv` until empty or `objectChangeBatchCap` (64), and
`dependentsWake` resolves the whole batch in one `EdgesGroupIncomingByID`.
O(bursts) instead of O(changes), with no timer and no accumulation window — in
steady state a batch is one element, and a batch only grows when values were
already waiting.

Draining at the *receiver* rather than through a buffered channel is load-bearing,
not stylistic. Conflation lives in the pending slot: until a value is popped,
another write to the same object merges into it. Any downstream batcher must pop
first, which de-conflates exactly when the consumer is slow — with a buffered
channel, 64 slots become 64 versions of one hot object. `mergePendingChange`
(`sqlite/watch.go:134`) also annihilates unobserved create-then-delete pairs
unconditionally, which it can do because this stream has no snapshot to preserve
deletes for.

### Guard: no controllers, no stream

`Start` skips the subscription entirely when `len(bh.order) == 0`. Every dependent
would land on `enqueueIfRegistered`'s no-op arm, and the stream is not free — it
would pay a edges query per change in the whole store to reach it.

### Alternatives considered

| option | why not |
|---|---|
| Subscribe per kind reachable through a `depends_on` edge | The set is dynamic: an edge declared at runtime introduces a kind nothing is watching, so it needs re-subscription on `DependenciesAdd`. Correct only if that path is never missed. |
| Subscribe per kind present in the store at `Start` | Static and simple, but misses a kind whose first object is created after `Start`. Also costs a stream per kind for kinds nothing depends on. |
| Reject `DependenciesAdd` against a client-only target | Converts a silent half-working case into a loud one, but makes a runtime error of something that mostly works, and the registered set is only known between `Register` and `Start`, so the check cannot sit at the natural place. Worth considering as a *warning*. |
| Keep per-kind wakers, add a global one for unregistered kinds | Two code paths for one job, and "unregistered" is exactly the stale question a global stream avoids. |
| Buffered channel of single changes instead of receiver-side batching | De-conflates on exit from the receiver and (under the old `RawObjectChange` value) pinned blobs. Pinned by `TestWatchObjectChangesCoalescesRepeatWrites`, which fails under that design. |

## Consequences

### Failure is process-wide, and says so

Both waker loss points lost their per-kind scope. A failed subscribe used to cost
one kind's wakes and leave K−1 wakers live; a closed stream used to kill one
kind's. Now either kills dependency wakes for every kind. Correctness survives —
both escalate every catchup tick to a full resync across *every* registered
reconciler, which was already the cross-kind behaviour — but the log messages
dropped their `group`/`kind` attributes and state the process-wide consequence,
because an operator reading "for this kind" would look for a scope that no longer
exists (`TestSubscribeFailureReportsWholeProcess`).

### One waker goroutine is a head-of-line block

K independent wakers became one. A slow `EdgesGroupIncomingByID` — itself queued
behind writers on the single connection — now delays wakes for every kind, where
before it delayed only its own. Batching bounds throughput, not latency: a batch
still waits for the query ahead of it. Accepted deliberately, since the
alternative is the defect. The shape of a fix, if a workload ever shows the stall,
is a small pool of drain goroutines over the one subscription, partitioned by
target id so a kind's wakes stay ordered. Recorded in `TODO.md`; unbuilt.

### Two `Beehive`s on one store observe each other's kinds

The restart tests use that shape. `enqueueIfRegistered` filters correctly, so it
is sound — but it is paid for in edges queries.

### Delete-time state is unrecoverable

`Deleted` fires on the physical delete, so a consumer wanting the object's last
known state cannot re-read it. The per-kind stream carried the real final row for
exactly this. The waker does not care (a gone object has no dependents, and
RESTRICT means it had none), but a future audit or cache-eviction consumer would.
The cheap widening is not the row: `pendingChange` already carries `rv` for
conflation ordering, and exposing it costs one word and pins nothing.

### The store keeps two hubs

`hubs[gk]` (carrying `RawObjectChange`) and `changeHub` (carrying `pendingChange`) are
fed by the same `publish`, so every write pays two `Send`s. One hub keyed
`struct{GroupKind; ObjectID}` would serve all three consumers with conflation
granularity unchanged, but the value types have deliberately diverged — a single
hub means either giving snapshot watchers the projection and re-reading rows, or
giving the waker the blobs back. Recorded in `TODO.md`.

### `watch()` lost a parameter

With `WatchChanges` deleted, `watch()` has no snapshot-less caller: the
`hasSnapshot` flag, its receiver arm, and the nil-`seenIDs` path are gone, and
`transientDropMerge`'s `preserve` is now required rather than nilable. Deleting
that boolean is the clearest sign the split is at the right seam — the store-wide
stream shares none of `watch()`'s snapshot machinery (resource-version high-water
dedup, `seenIDs` type promotion, orphan-tombstone suppression).

## Implementation

Sixteen commits on `spec/client-only-target-waker`, as eleven self-contained
red/green cycles: the global hub, `ObjectWritesSubscribe`, the batch drain, the
conflation pins, stream shutdown, `dependentsWake`, then the single
subscription — where the defect closes — followed by the zero-controllers guard,
the process-wide messages, deleting `WatchChanges`, and docs. A cleanup pass and a
code review followed, which produced the payload-free hub value, the
`ChangeRef` → `ObjectWrite` rename, and the removal of `transientDropMerge`'s
dead branch.

The defect tests are the red of the cycle that closes it, in `reconciler_test.go`:

- `TestClientOnlyTargetWakesDependent` — with every periodic driver disabled, so
  it tests the waker rather than a backstop.
- `TestClientOnlyTargetCreatedAfterStart` — the discriminating case against
  "subscribe per kind present at `Start`".
- `TestClientOnlyTargetDeletionUnwedges` — the RESTRICT stall above.

Four tests describe properties that passed on arrival and were verified by
construction rather than assumed: reverting the `TryRecv` drain to a buffered
channel fails `TestWatchObjectChangesCoalescesRepeatWrites`; dropping the
`s.done` arm hangs the parked-on-send shutdown case; forcing the waker branch open
fails `TestStartWithNoControllersSkipsWaker`; restoring the per-kind log wording
fails both blast-radius tests.

## Related work

A future durable change-history scan would cover the *recovery* half of this
defect as a side effect — a target whose kind has no waker gets scanned like any
other — but not the defect: no wake is delivered, so a dependent converges at the
scan cadence rather than within milliseconds, and the RESTRICT stall is merely
bounded by that cadence instead of by the process lifetime. Waker first, for that
reason.

The unreclaimed `pending_wake` count for client-only *dependents* is the mirror
gap on the other side of the edge, and is still open in `TODO.md`.
