# Watch shared tail, driven by the commit path

- **Status:** Draft — not implemented. Plan:
  [watch-shared-tail-plan.md](watch-shared-tail-plan.md).
- **Date:** 2026-08-02

## Goal

The watch tail has two costs that grow with use:

- **Latency.** A subscriber waits up to `withWatchPollInterval` (1s) after a
  commit, because nothing wakes the tail.
- **Query load.** Each watch tails the log alone. N watches on one kind cost N
  `ObjectWritesMaxVersion` queries per interval, and a busy tick costs N reads
  of the same log page. Apps are expected to hold many simultaneous watches.

This spec replaces the per-watch poll with:

1. **A shared tailer per kind.** One goroutine owns the kind's cursor, runs
   the gate + log page + batched read once, and fans raw rows out in-process.
   Watches become subscribers that decode for themselves. Query load scales
   with watched kinds, not watch count.
2. **A wake from the commit path, on top of a slow floor tick.** The wake
   carries freshness; the floor tick (30s) carries the guarantees a wake
   cannot — a writer this process does not share memory with, a transient read
   failure, a retention trim. See "Why the wake cannot stand alone".

## Why the fast tick can go

Watches are an observation surface, not a control surface. No reconcile
trigger consumes a watch (`docs/reconcile-triggers.md`); controllers, the
drivers, GC and the waker never read it. State lives in the store, and every
guarantee about state — owed reconciles, watermarks, GC — has its own durable
or periodic backstop.

So a lost wake cannot corrupt state, strand a reconcile, or diverge a
controller. It can only leave a subscriber stale. The worst case is a stale
frontend, and the store heals it.

## Why the wake cannot stand alone

The [drivers ADR](../adr/2026-07-28-periodic-scan-drivers.md) sets the bar the
schedule watch was granted its exception under: a poll may go only when its
hub can observe every writer. It then says every store-backed driver fails
that bar by construction, because a second process can always write to the
store.

The watch tail fails it too, and not only in the third-party-`Store` case. A
second process over the same SQLite file, or a second `Beehive` over one
store, writes through *its own* `AfterCommit` hooks. This process's watchers
would never hear about those writes. Today they hear about them on the next
tick, forever. Dropping the tick outright would narrow a public API from "a
watch observes writes to the store" to "a watch observes writes made through
this `Beehive`".

We do not take that narrowing. The tick drops from 1s to a **30s floor**:

- The query-load win is preserved almost whole. A quiet kind costs one read of
  one number every 30s per *kind*, not one per watch per second — for 20
  watches on one kind that is ~0.05 queries/s against 20/s today, over 99% of
  the win.
- Latency is the wake's job and is unaffected: the floor never delays a write
  this process made.
- The floor is also the retry path for a failed tail step, and the path that
  notices a retention trim. Both are otherwise unreachable on a quiet kind.

The emit-discipline table below is still required. The floor bounds staleness
at 30s; it is not a licence to forget a publish.

The watch tail therefore stays in the periodic-scan driver family, at a
relaxed cadence, with a wake in front of it. The drivers ADR gains an
amendment about the cadence, not an exception.

## Non-goals

- The commit path does not carry object state, only (kind, id, op, resource
  version). Changes enter delivery in exactly one place: the tailer's read.
  Cross-goroutine publish order does not matter for this — delivery is
  latest-per-object, and a per-key `resource_version` comparison makes racing
  publishes converge on the newest (see "Ordering"). The reads stay on the
  tailer because the write paths do not hold full rows (the
  [write-shapes ADR](../adr/2026-07-30-store-write-shapes.md) keeps their
  returns narrow), and because resume needs the log regardless.
- No change to `EventsWatch` or to the dependency waker (see "Later
  customers").
- No new contract on the `Store` interface.

## Design

Two hubs, one per direction, each from a package already in the tree:

- **Commit → tailer:** `gobus/watch`, the keyed latest-value gauge bus the
  schedule watch already uses. Key: `GroupKind`. Value: the committed write's
  resource version. Accept: `next > prev`, so stale publishes vanish and a
  burst coalesces into one pending wake. One key per receiver fits: a tailer
  watches one kind. `Sender` is safe to share across committing goroutines.
- **Tailer → subscribers:** `gobus/conflate`, the single-producer keyed
  fan-out bus. This is its stated use case (Kubernetes-style informers). Key:
  `ObjectID`. Value: a raw envelope, not a decoded change (see below). Merge:
  latest wins. A slow subscriber coalesces per object — bounded by the live
  key set — and never blocks the tailer or the other subscribers.

### The fan-out value is raw

The tailer is **non-generic**, and so is the hub it feeds. It publishes:

```go
type rawChange struct {
    ID              ObjectID
    Op              WriteOp        // from the log entry
    ResourceVersion int64          // from the log entry
    Object          *RawObject     // current state; the row image for a delete
}
```

Each subscriber decodes with its own type parameters and its own migrator.

This is forced, not preferred. `NewClient[Spec, Status](bh, gk)`
(`client.go:261`) is free-form: nothing stops two clients with different type
parameters over the same `GroupKind` — a `json.RawMessage` client alongside
the typed one is the obvious case. A typed per-kind hub would either
type-assert into a panic or need a second tailer per type set, losing the
sharing that is the point. A non-generic tailer also keeps CLAUDE.md's rule
that new internal machinery is non-generic.

Query sharing is unaffected: the gate, the log page and the batched
`ObjectsListByIDs` still run once per kind. Decode and quarantine move per
subscriber, which is more correct anyway — one subscriber's undecodable blob
is not another's.

### Wake producer

`Beehive` owns the wake hub, created in `New`. Each beehive-layer write path
publishes its kind and resource version through `Store.AfterCommit` — the
boundary `signalRequeue` uses, for the same reasons: a rollback publishes
nothing, and the wake cannot arrive before the write is readable. On stop,
close the sender, not the hub (`Hub.Close` is a hard tear-down; see
`scheduleHub`).

### Emit discipline

With the tick at 30s, a write path that forgets to publish is a write
subscribers see up to 30s late. Make the obligation structural, not per call
site: route every object mutator's commit through one beehive-layer choke
point (the shape `signalRequeue` already has) that publishes as a side
effect.

**Derive the table from the store's three write-log helpers, never from the
public verbs.** `sqlite/store.go` appends an `object_writes` entry from these
sites:

| site                                       | reached by                                                            | wake key                       |
| ------------------------------------------ | --------------------------------------------------------------------- | ------------------------------ |
| `objectsCreate` (`store.go:593`)           | `Create`, `CreateByName`, `GetOrCreate`'s create branch                | caller's gk                    |
| `updateSpec` (`store.go:1225`)             | `Update`, `UpdateByName`                                               | caller's gk                    |
| `ObjectsUpdateStatus` (`store.go:1287`, `:1299`) | `UpdateStatus`                                                   | caller's gk                    |
| `bumpObject` (`store.go:1412`)             | **`ConditionsSet`, `ConditionsDelete`** (`store.go:1476`, `:1498`)     | caller's gk                    |
| `FinalizersDelete` (`store.go:1715`)       | finalizer clearing, GC                                                 | caller's gk                    |
| `markForDeletion` (`store.go:1767`)        | `Delete`, `DeleteByName`                                               | caller's gk                    |
| `markForDeletion` via `DeletionRequestsCreateFromOwner` (`store.go:1882`) | the owner cascade                        | **per returned `[]ObjectRef`** |
| `objectsDelete` (`store.go:1916`)          | the GC sweeper's physical delete                                       | that object's gk               |

Two rows are the ones a verb-derived table misses, and both matter:

- **`bumpObject`.** Controllers set conditions constantly. A condition write
  that does not wake is a write watchers do not see until the floor tick.
- **The owner cascade.** `DeletionRequestsCreateFromOwner` marks many objects
  across several kinds in one call. One wake for the caller's `gk` is wrong.
  Route by the returned `[]ObjectRef`, exactly as `EdgesAddResult.From`
  already does for the new-edge enqueue.

A third-party `Store` bypasses these hooks. So does a second process over the
same file. The floor tick is what covers both; see "Why the wake cannot stand
alone".

### The tailer

One per kind, started lazily on the kind's first watch. Its context is a
`tailCtx` created in `New`, **not** `Start`'s `runCtx`: watches work today on
a beehive that was never started (see `stop`'s comment that watches are not
counted in `wg`), and the tailer must not change that. `stop` cancels
`tailCtx` whether or not the beehive was running; a watch opened after stop
gets its snapshot and an immediately closed stream.

The tailer selects over its wake receiver, a 30s floor ticker and `ctx`. On
either signal it runs a **drain loop**, not one step:

```
for {
    n := step()             // gate, page, coalesce, batched read, publish; returns the page length
    if n < tailPageCap { break }
}
```

The loop is required, not an optimisation. `poll` reads at most `tailPageCap`
(512) entries and today spills into the next 1s tick. Under a wake, a burst of
600 writes coalesces into **one** pending wake slot; the tailer consumes it,
reads 512, and the remaining 88 would wait for an unrelated future write —
only the publish that races the read survives in the slot. With the floor tick
that stall is 30s rather than unbounded, which is still wrong. Re-run until a
step comes back short; block on the wake only when drained.

The condition is the **page length, not a second gate read**. A short page
means the log is drained, exactly; re-reading `ObjectWritesMaxVersion` would
pay a scalar query per wake for an answer the page already gave — `step`
opens with that read as its gate.

**Ordering constraint:** the tailer registers its wake receiver *before* it
reads its initial cursor. The mirror of the subscriber rule below, and for the
same reason — a write between the read and the registration would otherwise be
lost to both.

A failed step logs and backs off (bounded, capped at the floor interval)
rather than spinning; the cursor does not advance, so nothing is lost.

The tailer resolves nothing through `bh.mu` while starting or stepping. It has
no reason to — decode moved to the subscribers, so it needs neither
`migratorFor` (`beehive.go:381`) nor `reconcilerFor` (`:388`), and both take
`bh.mu`, which is not reentrant. **Lazy start holds `bh.mu`; anything it
resolves through those helpers deadlocks.** Same trap `stop` already carries a
comment about for `wg.Wait`.

### The tailer's own ErrWatchTooOld

The cursor is shared, so a too-old cursor is not a per-subscriber failure. The
step can reach it: a long store outage holds the cursor still (backoff is
capped at the floor, and not advancing is the point), and 24h retention
eventually trims past it.

**Policy: close the fan-out sender, then reset the tailer.** Every subscriber
ends with `ErrWatchTooOld` and resubscribes onto a fresh snapshot. The next
watch starts a new tailer at the current position.

The two tempting alternatives are both wrong. Advancing the cursor to the
horizon silently drops changes from *every* subscriber — the failure mode this
whole design exists to avoid. Returning the error to one subscriber is not
available to a shared reader: there is no one subscriber at fault.

A store-wide tailer over `ObjectWritesListSinceAll` (the dependency waker's
read) is a possible follow-up.

### Subscribers

`Watch` and `WatchList` register a conflate receiver, then take their
snapshot, in that order — conflate has no replay, so registration must come
first — and drop deliveries at or below the snapshot's position. The drop is
one comparison in the subscriber's own loop, and it stays there: the floor is
irreducibly per-receiver state that only the subscriber learns, after
registration, from its own snapshot read. It is sound because
`resource_version` is one log-wide sequence and the snapshot is a consistent
cut at it — at or below the floor *means* "already in the snapshot", for every
key.

The floor comparison needs a resource version on the delivered change.
`ObjectChange` (`client.go:113`) does not carry one today. **Add it as a
public field**, not an internal envelope: callers need it for exactly the
resume and dedup reasons this spec relies on, and `WithResumeFrom` already
takes one with no documented way to obtain it.

Stock conflate suffices — no gobus change. Ordering and the change-type
algebra both live in the hub's `Merge`; see "The merge" below.

A single-object watch uses `WithKeyFilter` on its id, so its memory is bounded
by one key. `WithLoads` runs per subscriber: drain the pending queue with
`TryRecv`, then load relations for the drained batch, so a watch with loads
stays batched and does not become an N+1.

### The merge

Conflate takes the coalescing policy as a `Merge(prev, next)` on the hub. It
runs only when a send lands on a pending slot, which is exactly where a stale
racing send is caught: returning `prev` keeps the pending value and its queue
position. After the guard, the change-type algebra. Ids are never reused, so a
create is always an id's first write; do not handle the unreachable pairs.

| prev     | next     | merged                   |
| -------- | -------- | ------------------------ |
| Added    | Modified | next's state, type Added |
| Modified | Modified | next                     |
| Modified | Deleted  | next                     |
| Added    | Deleted  | next, type Deleted       |

```go
func(prev, next rawChange) (rawChange, bool) {
    if next.ResourceVersion <= prev.ResourceVersion {
        return prev, true // stale racing send: keep what's pending
    }
    if prev.Op == writeOpCreate && next.Op != writeOpDelete {
        next.Op = writeOpCreate // newest state, still a first sighting
    }
    return next, true
}
```

**Nothing annihilates.** An earlier draft dropped an `Added` + `Deleted` pair
on the grounds that the consumer never observed the create. That is wrong: the
floor is per subscriber, but `Merge` is hub-wide. Sequence — subscriber
registers; the tailer publishes `Added@95`; the subscriber's snapshot lands at
100 with the object present; the object is deleted at 105; the merge
annihilates the slot. The subscriber holds the object from its snapshot and is
never told it is gone. That is permanent divergence, not a latency cost, and
the stale guard does not catch it (105 > 95). A `Deleted` for a key the
consumer never saw is a no-op delete at any cache; the residue is much cheaper
than the bug.

The `Added` promotion is blind to the same per-subscriber floor, but benignly:
a subscriber whose snapshot already contains the object gets a duplicate
`Added` for it. **State that in the watch contract** — an `Added` may repeat
for an object the snapshot already carried — rather than paying per-receiver
bookkeeping to suppress it.

The stale guard is defense in depth, not the guarantee. Per key the tailer
cannot send out of order — one goroutine, a strictly advancing cursor,
sequential state read-backs, so payloads are monotone as well as stamps. A
stale send arriving *after* its key's slot was delivered would go through
undetected; the bus keeps no per-key position (that map would grow with every
key ever seen), and the backstop belongs to the consumer that per-key
staleness can hurt — one holding state per object, which therefore already has
a position to compare. Newest-wins at that cache makes the pipeline idempotent
end to end.

### Lifecycle and memory

Conflate frees per key on delivery: the delivered key's queue entry and value
slot both go, so a receiver's memory is bounded by its *undelivered* keys,
never by every key it saw. The explicit obligations are the receivers: a
subscriber closes its conflate receiver when its watch ends (the `defer
rx.Close()` pattern `SchedulesWatch` uses), and a tailer closes its wake
receiver on stop — that hub is keyed by `GroupKind`, so its footprint is
bounded by registered kinds.

**A tailer outlives its last subscriber, deliberately.** An app that watches
50 kinds once holds 50 goroutines and 50 scalar reads per floor interval for
the process's life. That is the accepted trade against restart churn: a watch
that opens and closes repeatedly on one kind would otherwise rebuild the
tailer and its cursor each time, and a tailer torn down mid-step needs a
teardown race resolved for no benefit. Idle teardown is available later if a
kind count ever makes it worth the race; it is not free, so it is not in v1.

Memory moves from one shared page per tick to N receivers × undelivered keys ×
one row image, since each pending slot holds a full spec/status blob. Bounded,
but it is a real shift; see "Acceptance" for the bound to measure.

`gobus/watch` cannot replace conflate on the fan-out side. Keyed per object, a
receiver cannot span a kind or see a future create. Keyed per kind, its
skip-to-latest under lag is safe only for a self-contained value: a skipped
change batch is objects silently never delivered, which breaks the invariant,
and a cursor gauge is safe but carries no data — every subscriber then needs
per-receiver bookkeeping of what it has not seen, which is what conflate is.
Watch is for gauges; this side is a change stream.

### Resume

A resume replays the log above the caller's rv on the subscriber's goroutine,
then raises the floor to the replay's end and consumes live deliveries.

**The replay pages.** Retention is 24h by default, so the gap can far exceed
`tailPageCap`; the replay loops on `ObjectWritesListSince` until it reaches the
tailer's current position. A trim that overtakes the replay mid-way ends that
stream with `ErrWatchTooOld`, per subscriber.

## Ordering

Concurrent commits race their `AfterCommit` hooks, so publishes do not arrive
in `resource_version` order. This does not matter, at either hub:

- **Per object**, both hubs resolve races by comparing resource versions, so
  the newest write wins regardless of publish order — and the tailer reads the
  log, which the store wrote in order, before anything is delivered.
- **Across objects**, order was never load-bearing: beehive is
  level-triggered, and a subscriber acts on current state per object, never on
  a cross-object sequence of changes.

The public contract weakens accordingly: from "changes arrive in write order"
to "each object's latest state arrives once, newest wins; an `Added` may
repeat for an object the snapshot already carried". State that in the watch
godoc.

`ErrWatchTooOld` narrows in the same edit. Today a `LagBlock` subscriber that
stops reading gets it once retention overtakes its cursor. Under conflate a
slow subscriber coalesces and cannot fall behind the horizon at all, so the
sentinel is left with two producers: a resume below the horizon (per
subscriber) and the tailer's own reset (all subscribers at once, above).
Record that alongside the ordering relaxation — it is an improvement, but it
is still a documented sentinel changing meaning.

## Consequences

- **A quiet kind costs one query per 30s, whatever the watch count.** Today it
  costs one per watch per second. The acceptance measurement is query count,
  not tick cost.
- **A single-object watch regresses.** `watchpoll.go` filters by id before the
  batched read, so today a lone `Watch` decodes one row per tick. Sharing the
  tailer makes it pay the kind's whole write volume even when it is the only
  subscriber — the key filter drops changes after the read, not before it. The
  shared win is real above roughly two subscribers per kind; below that this
  is a trade of throughput for latency.
- **Freshness depends on the wake; correctness does not.** A missed publish (a
  bug) leaves subscribers stale for up to 30s. The emit-discipline table is
  its guard.
- **`LagPolicy` may be deletable.** Conflate has no backlog to bound: a slow
  subscriber coalesces instead of lagging. `LagBlock` becomes the natural
  behavior with stronger isolation, and `LagFail` loses its reason to exist.
  Removing it is a breaking API change; take it deliberately, and after the
  surface has flipped onto conflate — not before.
- **`ObjectChange` gains a public `ResourceVersion`.** Additive, and it is what
  makes `WithResumeFrom` usable.
- **`withWatchPollInterval` splits.** `EventsWatch` keeps the 1s cadence; the
  object tail gets the 30s floor as its own unexported option.

## Later customers

- The dependency waker's 1s scan could become wake-driven the same way — but
  the waker feeds reconciles, where the stale-dependents pass is the durable
  backstop, so that change carries a different argument. Separate spec.
- `EventsWatch` gates on `EventsMaxVersion` per object and needs its own key
  space. Out of scope.

## Acceptance

- Write-to-delivery latency: commit-to-wake time plus one tail step, with no
  interval term.
- Quiet-state query count: one read per kind per floor interval, independent of
  watch count.
- Burst: a write burst above `tailPageCap` delivers in full with no further
  write, and without waiting for the floor tick — at one gate read per wake,
  not two.
- Trim: a tailer whose own cursor falls below the horizon ends every
  subscriber with `ErrWatchTooOld`, and the next watch works.
- Emit discipline: a test walks every write-log call site in the table above —
  including the condition verbs and the owner cascade — and asserts each one
  wakes the right kind or kinds.
- Divergence: a delete of an object present in a subscriber's snapshot is
  always delivered, even when the create preceded the snapshot and the
  subscriber has read nothing.
- Memory: an unread subscriber's retained bytes stay bounded by its
  undelivered key count × row size, and do not grow with total writes.
- Tests synchronize on the hubs; the watch tests lose their poll-interval
  timing entirely.

## On acceptance

- Write an ADR recording the two-hub split, the raw fan-out value, the
  observation-surface argument for relaxing (not removing) the tick, the
  relaxed ordering contract, and `ErrWatchTooOld`'s two remaining producers.
- Amend the drivers ADR: the watch poll stays a driver, at a floor cadence,
  with a wake in front — and record why the schedule watch's exception does
  not extend to it.
- Remove the "watches have no push path" item from `docs/TODO.md`.
- Update the watch bullets in `CLAUDE.md` and `README.md`, including the
  schedule-watch bullet's claim that it is "the one watch that does not poll".
