# Dependency targets of client-only kinds get no waker at all

**Status: a defect that exists today, with a recommended fix. Independent — it
depends on nothing else, and nothing else depends on it.**

---

## 1. The defect

`Start` subscribes one dependency waker per **registered** kind (`beehive.go:195-214`):

```go
for _, r := range bh.order {                       // beehive.go
    w, err := bh.store.WatchChanges(runCtx, r.gk)
    ...
    bh.wg.Go(func() { bh.runDependencyWaker(runCtx, r.gk, w) })
}
```

`bh.order` is the registered controllers. A `depends_on` edge may point at **any**
object, including one belonging to a client-only kind — a kind used through `Client`
with no `Register`, and therefore with no reconciler and no entry in `bh.order`.

Changes to such a target reach no waker. Not a dropped wake: **none**. Nothing observes
the change, so `wakeDependents` is never called, so `ListIncomingRefs` never runs, so no
dependent is queued.

Note the asymmetry with the *dependent* side, which is handled correctly:
`wakeDependents` routes each dependent through `enqueueIfRegistered` (`beehive.go:409`),
so a client-only *dependent* is deliberately ignored. There is no equivalent thought
given to a client-only *target*, and the two are not symmetric — an unregistered
dependent has nothing to reconcile, while an unregistered target is an ordinary object
whose changes other kinds' controllers have asked to hear about.

**Nothing in the API warns about it.** `AddDependency` accepts any target id and says
nothing about the target's kind needing a controller. The shape is easy to arrive at:
configuration objects, secrets, or any "reference data" kind that is written by the
application and read by controllers is exactly a client-only target.

### 1.1 The unrecoverable case: deleting a client-only target

The motivating case above is a *slow* dependent. The sharper one is a wedged store, and
no configuration recovers it.

`refs.to_id` is `REFERENCES objects(id) ON DELETE RESTRICT`
(`sqlite/migrations/0001_init.sql:152`), so a target with dependents cannot be physically
removed. `Client.Delete` therefore sets the tombstone and emits `Modified`; the row stays
deletion-pending until every incoming edge is gone, which only its dependents' reconciles
can accomplish. Today that `Modified` reaches no waker, so:

- the dependents are never woken, so nothing drops the edge;
- the target stays deletion-pending, RESTRICT-blocking its own collection;
- the GC sweeper retries it every `WithGCInterval` tick forever, with no way to progress.

`WithStartupResync` does not repair this — it fires at the *next process start*, so a
running process stays wedged indefinitely. This is a stronger motivation than §1's: §1 is
latency that a default happens to cover, this is a permanent stall inside a single run.

## 2. What covers it today, and what does not

| | covers it? |
|---|---|
| the dependency waker | no — no waker exists for that kind |
| `ListUnsettledIDs` (catchup tick) | no — the dependent is settled; its own generation never moved |
| `ListPendingWakeIDs` (catchup tick) | only if the edge was declared against an already-moved target, which is a different case |
| `WithStartupResync` | **yes**, and only at the next process start |

The load-bearing row is the last one, and it holds for a narrow reason: `enqueueAll` (the
startup full pass, `reconciler.go:299-311`) deliberately includes already-settled objects,
while `enqueueCatchup` = `ListUnsettledIDs` + `ListPendingWakeIDs` structurally cannot see
a settled dependent.

So the sole cover is a default, not a mechanism — and it is a default two supported
configurations remove. Under `WithStartupResync(false)`, a dependent of a client-only
target never learns its target moved, for the life of the store. And per §1.1, even with
every default in place, an in-process delete of a client-only target does not recover.

**Severity: a live correctness gap in the dependency mechanism**, for a shape the API
permits without warning.

## 3. Recommended fix: one global change stream, with a batched drain

**Replace the per-registered-kind waker with a single store-wide one, and make the waker
drain in batches.** These are one change, not two: going global multiplies the waker's
per-change store work by the whole write volume of the process, and the batch is what pays
for it (§3.2).

### 3.1 The subscription

Replace the existing method rather than adding a second one — after this fix the per-kind
form has **zero** production callers (`beehive.go:196` is the only one today), and leaving
it on the interface is dead surface. The replacement is store-wide **and** batched, and it
carries change *references*, not rows:

```go
// internal/storeapi — replaces Store.WatchChanges(ctx, gk)

// ObjectChange is everything the waker uses from a change: the id to resolve
// dependents for, and the type it filters on. Deliberately blob-free.
type ObjectChange struct {
    ID   ObjectID
    Type ChangeType
}

type ObjectChangeWatcher interface {
    Batches() <-chan []ObjectChange
    Close()
}

// Store
WatchObjectChanges(ctx context.Context) (ObjectChangeWatcher, error)
```

`Client.Watch`/`WatchList` are untouched; this is the internal object-*change* stream only,
and the waker is its only consumer — so narrowing it to what the waker reads costs nothing
and buys §3.2's memory discipline.

Implementation is the existing `watch` machinery with the per-kind hub swapped for the
global one, no snapshot (as `WatchChanges` already does: `hasSnapshot=false`, so no
`seenIDs` correction and a dedup floor of 0), and a different feeder loop: block on
`rx.Recv`, then drain with `conflate.Receiver.TryRecv` until empty or a bounded batch cap,
projecting each value to `ObjectChange` and sending the slice on an **unbuffered** channel.

`Start` then runs one waker instead of K, and `wakeDependents` needs no change to its
routing at all — it already resolves dependents by edge and routes each through
`enqueueIfRegistered`, so routing by the *dependent's* kind is already correct and already
the only routing that happens.

**Why this shape rather than the biggest one:**

- **It is the only option that cannot go stale.** The set of kinds a `depends_on` edge
  might point at is discovered at runtime, and any per-kind subscription list has to be
  computed from something — the registered set (today's bug), the kinds present in the
  store at `Start` (misses a kind whose first object is written later), or the kinds
  currently referenced by an edge (changes on every `AddDependency`). A global stream
  asks no such question.
- **The store is already close to it.** `sqliteStore.hubFor(gk)` creates a conflating hub
  per kind on first use and the emit path publishes through it. The hub key is `ObjectID`,
  which is globally unique — `objects.id` is one `AUTOINCREMENT` primary key for the whole
  table (`sqlite/migrations/0001_init.sql:11-14`), and `GetObject(ctx, id)` takes no
  `GroupKind` — so a global hub conflates per object exactly as the per-kind hubs do, with
  the same `mergeChange` policy.
- **It collapses more than it adds.** The per-kind subscribe loop, its per-kind failure
  branch, and the `gk` that `runDependencyWaker` carries solely for that branch's log line
  all reduce to one subscription and one failure branch. Against that, §3.1 adds one
  narrow type and one feeder loop, and deletes the per-kind method it replaces.

**The global hub is an eager field, not a lazy `hubFor` entry.** There is exactly one of
it and no key to look up, so create it in `open` and close it in `Close` alongside the two
maps (`sqlite/store.go:76-92`). Lazy creation would cost `publish` a second `hubMu`
acquisition and a second map lookup on **every object write in the process** — the emit
path already takes `hubMu` once for the per-kind hub, and this is the one place where the
global stream taxes writers rather than the waker.

### 3.2 The batched drain (not optional)

Two costs land together, and both are cured by the same change:

- **Query amplification.** Wake resolution costs one `ListIncomingRefs` per change. Today
  that is per change *on registered kinds*; globally it is per change *in the store* — and
  the new load is precisely the high-write client-only kinds §1 cites as motivation. The
  store runs `db.SetMaxOpenConns(1)` (`sqlite/sqlite.go:45`), so the waker's reads
  serialize against every writer in the process: a hot client-only kind would tax every
  write.
- **Lost per-kind isolation.** K waker goroutines collapse to 1, and `runDependencyWaker`
  blocks on `wakeDependents` before taking the next change. A slow refs lookup delays only
  its own kind's wakes today; after the fix it delays every kind's. Conflation keeps this
  correct (the receiver holds the latest change per id, so nothing is lost — the merge is
  `mergeChange`, `sqlite/watch.go`), but the waker exists for *latency*, and the resync
  backstop is off by default.

**Fix:** drain the ready changes, resolve them in one query, then wake.

- `Store.GroupIncomingRefsByID(ctx, toIDs, relation) map[ObjectID][]Referrer`
  (`internal/storeapi/storeapi.go:532`) already exists and is exactly the batched shape;
  the eager `List` loader is its current caller. `ListIncomingRefs` stays for GC.
- The waker receives a `[]ObjectChange` batch, filters to `Added`/`Modified`, issues **one**
  `GroupIncomingRefsByID` and walks the result — self-edge skip and `enqueueIfRegistered`
  per dependent, unchanged. Dedup is free: the ids go into a set.

**The batch is drained at the receiver, not at the channel.** The obvious shape — leave
the `RawChange` channel in place and give it a buffer so a non-blocking drain finds
something — is wrong twice over, and `watch.go` is already explicit about the first:

- **It pins blobs.** `RawChange.Object` is a `*RawObject` carrying spec and status JSON.
  The snapshot loop nils its slice the moment it is emitted precisely because "holding the
  slice would pin every object's spec/status blobs until the watcher closes"
  (`sqlite/watch.go:466`). An N-slot FIFO in front of the one stream that carries the
  store's entire write volume re-introduces exactly that, N blobs deep — and the waker
  reads nothing but the id, so every byte of it is waste.
- **It un-conflates.** A change that has left the receiver is no longer coalescable, so N
  buffered slots can hold N versions of one hot object — the opposite of the property the
  hub exists for, and it defeats the batching too: the batch would be N repeats of one id.

Draining with `TryRecv` inside the feeder keeps conflation intact right up to the handoff
(the hub still holds one latest value per id) and lets the projection to `ObjectChange` throw
the blob away before anything is buffered. What crosses the channel is a slice of two-word
structs, one per distinct object, and the channel itself stays unbuffered. Memory is
bounded by the receiver's live-key set — which is store-wide now, so say it plainly: one
pending `RawChange` per live object in the store, held by a single receiver, plus one
batch slice of ids in flight. That is the honest bound, and it is the same bound the
per-kind hubs already carry in aggregate.

This turns O(changes) round-trips into O(bursts) and converts the lost per-kind isolation
into a batching win. Without it the fix trades a correctness gap for a throughput gap.

### 3.3 Blast radius of the single subscription

Both failure paths become all-or-nothing, and the spec states it rather than discovering
it in review:

- **Subscribe failure** (`beehive.go:198-208`): today a failure escalates and leaves K−1
  wakers live. After, one failure means the process has no dependency wakes at all.
- **Stream ended** (`beehive.go:346-356`): today this kills one kind's wakes. After, it
  kills every kind's.

Correctness survives in both cases — `resyncKindsEveryTick` converts each catchup tick
into a full pass across *every* registered reconciler (`reconciler.go:599`), which is
already the escalation both branches perform. So this is not fatal, but the log messages
must stop saying "for this kind": there is no per-kind consequence any more, and the
`hasPeriodicPass()` wording (which already keys across all kinds) becomes the whole story.

### 3.4 Two guards

- **Zero registered controllers.** `Start` with `len(bh.order) == 0` would open a
  store-wide subscription and pay a refs query per change only to reach
  `enqueueIfRegistered`'s no-op arm. Skip the waker entirely in that case.
- **Two `Beehive`s on one store** (the shape the restart tests use) now each observe the
  other's kinds. `enqueueIfRegistered` filters correctly, so this is sound — but it is
  paid for in refs queries, and it is worth a sentence in the waker's comment so the next
  reader does not read it as a bug.

### 3.5 Ordering

The fan-out ordering `Start` already documents still holds: the waker must be subscribed
and consuming *before* any reconcile loop launches, because a controller's startup
reconcile can modify a target immediately. With one subscription the constraint gets
easier, not harder — but it is still the constraint.

### Alternatives considered

| option | why not |
|---|---|
| Subscribe per kind reachable through a `depends_on` edge | The set is dynamic: an edge declared at runtime can introduce a kind nothing is watching, so it needs re-subscription on `AddDependency`. Correct only if that path is never missed. |
| Subscribe per kind present in the store at `Start` | Static and simple, but misses a kind whose first object is created after `Start`, which is an ordinary thing for a client-only kind. Also costs a stream per kind for kinds nothing depends on. |
| Reject `AddDependency` against a client-only target | Honest, and it converts a silent half-working case into a loud one — but it makes a runtime error out of something that currently *mostly* works, and the registered set is only known between `Register` and `Start`, so the check cannot be made at the natural place. Worth considering as a *warning*, not an error. |
| Keep per-kind wakers and add a global one for unregistered kinds | Two code paths for one job, and the "unregistered" set is exactly the stale question the global stream avoids. |

## 4. Relationship to a future durable change-history scan

A future design may back dependency recovery with a durable cursor scanned from the store's
global write history rather than a live stream (unwritten; tracked in `TODO.md`). That
would cover the *recovery* half of this defect as a side effect — a target whose kind has
no waker gets scanned like any other — but it does **not** fix the defect:

- The live path stays dead: no wake is ever delivered, so a dependent converges at the
  scan cadence instead of within milliseconds.
- A backstop firing on every change to a whole class of targets is the backstop doing the
  waker's job at the waker's frequency, which is not the cost model such a scan would be
  sized against.
- §1.1 is not covered at all if the scan is cadence-driven and the process is wedged
  between ticks — it merely bounds the stall by the cadence instead of the process
  lifetime.

Order the work waker-first: it is the smaller change and it is the one that restores the
latency guarantee.

## 5. Test plan

- **A dependent of a client-only target is woken live.** Register only D's kind; create T
  through `Client` with no `Register`; declare `D depends_on T`; let D converge; then
  change T. With `WithStartupResync(false)` and **every ticker disabled**, assert D
  reconciles. *Every ticker disabled is what makes this a test of the waker rather than of
  a backstop.*
- **Deleting a client-only target unwedges.** Same setup; `Client.Delete(T)`. Assert D is
  woken by the tombstone's `Modified`, and that once D drops its edge the target actually
  collects. *This is §1.1 and it is the test that fails today with no configuration that
  rescues it.*
- **The same as the first test, with the target kind's first object created after
  `Start`.** *This is what fails under the "subscribe per kind present in the store at
  `Start`" alternative, and it is the discriminating test between that option and the
  global stream.*
- **A registered target still behaves exactly as before.** The existing dependency-wake
  tests, unchanged, so the global stream is a superset rather than a swap.
- **One subscription, not K.** Assert the store hands out a single change watcher for a
  `Beehive` with several registered kinds, and **none** for a `Beehive` with zero
  registered controllers (§3.4).
- **A burst resolves in one query.** Change several targets before the waker drains, and
  assert one `GroupIncomingRefsByID` call covers them (count the calls on a store double),
  with every dependent still woken exactly once. *This pins §3.2; without it the batch can
  silently regress to per-change lookups.*
- **Repeated writes to one object collapse to one batch entry.** Write the same target
  several times while the waker is busy, and assert the next batch carries that id once.
  *This is the un-conflation half of §3.2 — it fails under the buffered-channel design and
  passes under the `TryRecv` drain, which is exactly the difference between them.*
- **Subscription failure still degrades loudly.** With the single subscription failing,
  assert the warning fires once and names the whole-process consequence rather than one
  kind's (§3.3).
- **Ordering holds.** A controller whose startup reconcile immediately modifies a target
  must not outrun the waker. The existing ordering guarantee in `Start`, re-asserted
  against the single subscription.

### 5.1 Test doubles to update

Mechanical, but they gate everything above:

- `fakeStore.WatchChanges` (`testutils_test.go:231`) — becomes `WatchObjectChanges(ctx)`,
  defaulting to a never-firing `ObjectChangeWatcher` the way `noopWatcher` does today.
- `watcherStore.WatchChanges` (`testutils_test.go:258`) — same rename; this is the double
  that injects a subscribe failure, so it is what the §3.3 test drives
  (`watcherStore{err: …}`, as `client_test.go:1457` already does for `New`). Its preset
  watcher field changes type, and `fakeWatcher` needs a batch-shaped twin whose `push`
  feeds `[]ObjectChange` — the existing one stays for the `Client.Watch` adapter tests, which
  are unaffected.
- `fakeStore.GroupIncomingRefsByID` — currently a `panic("not implemented")` stub in the
  fill-in-as-needed set; the burst test needs it implemented plus a call counter.
- `sqlite/watch_test.go` and `sqlite/store_test.go` callers of
  `store.WatchChanges(ctx, testGK)` — more than a signature change now: they assert on
  `RawChange` values (`TestWatchChangesSkipsSnapshot`, `TestWatchChangesStreamsLiveAdded`
  and friends) and must move to `[]ObjectChange` batches. Two contracts also genuinely
  changed, so re-read rather than mechanically port: a store-wide stream no longer filters
  by kind, so any assertion that *another kind's* change is absent is asserting the old
  behavior; and no test can inspect an object's spec/status off this stream any more,
  which is the point of §3.2.
