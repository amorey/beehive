# One tailer per kind, woken by the commit path, above a floor tick

- **Status:** Accepted — implemented in `watchtail.go`, `watch.go`,
  `beehive.go`, `client.go`, `controller.go`, `gc.go`.
- **Date:** 2026-08-03

## Context

Each object watch used to tail the write log alone, on
`withWatchPollInterval` (1s). Two costs grew with use:

- **Latency.** A subscriber waited up to a full interval after the commit,
  because nothing woke the tail.
- **Query load.** N watches on one kind cost N position reads per interval, and
  a busy tick cost N reads of the same log page. Applications are expected to
  hold many watches at once.

## Decision

**One tailer per kind, shared by every watch on it, woken by the commit path,
with a slow floor tick behind the wake.**

### Two hubs

- **Commit → tailer** (`gobus/watch`, keyed by `GroupKind`). Every beehive-layer
  write publishes through `Store.AfterCommit`, so a rollback publishes nothing
  and a wake never arrives before the row is readable. The value is a
  process-local counter, **not** the write's resource version: most store writes
  return no version (see the [write-shapes ADR](2026-07-30-store-write-shapes.md)),
  and the tailer reads its own cursor from the store. The value only needs to
  rise, so `Accept: next > prev` drops superseded publishes and a burst
  collapses into one pending wake.
- **Tailer → subscribers** (`gobus/conflate`, keyed by `ObjectID`). A slow
  subscriber coalesces per object and never blocks the tailer or the other
  subscribers.

### The fan-out value is raw

The tailer is non-generic and publishes `rawChange`: the log entry's id, op and
resource version, plus the row. Each subscriber decodes with its own type
parameters and migrator.

This is forced by the API. `NewClient[Spec, Status](bh, gk)` is free-form, so
two clients with different type parameters over one `GroupKind` are legal — a
`json.RawMessage` client beside a typed one. A typed fan-out would either panic
on a type assertion or need one tailer per type set, which loses the sharing.
Query sharing is unaffected: the position check, log page and batched read
still run once per kind. Decode and its error handling move per subscriber,
which is also more correct — a blob one subscriber cannot decode may be fine
for another.

### Emit discipline

With the tick slow, a write path that forgets to publish is a write subscribers
see late. The list of publish sites is derived from the store's three write-log
helpers (`appendWriteLog`, `recordObjectWrite`, `appendWriteLogDelete`), never
from the public verbs, and `TestWakeHubPublishesOnEveryWrite` is the guard. A
table derived from the verbs would miss two rows:

- `bumpObject`, reached by `ConditionsSet` and `ConditionsDelete`. Controllers
  write conditions constantly.
- The owner cascade. `DeletionRequestsCreateFromOwner` marks children across
  several kinds in one call, so the wake is routed by the refs it returns —
  the same way the new-edge enqueue routes by `EdgesAddResult.From` — and
  deduped by kind, so a wide cascade queues one commit hook per kind rather
  than per row.

A deeper alternative exists: an optional `ObjectWritesNotifier` on `Store`,
probed the way `DriverCursorer` and `FreePagesReleaser` are, called from the
three helpers themselves. The bundled store could then never forget to publish,
and the floor tick already degrades correctly for a store that does not
implement it. It is not taken here for two reasons: it puts a watch concern
into the store contract for a guarantee the floor tick already provides, and
`ObjectsDelete(ctx, id)` carries no `GroupKind`, so one site would stay manual
either way. Revisit if the table test ever fails to catch a new verb.

### Why the tick stays, at 30s

The [drivers ADR](2026-07-28-periodic-scan-drivers.md) says a poll may be
removed only when its hub observes every writer. This tail does not qualify: a
second process over the same file, or a second `Beehive` over one store, writes
through its own hooks, and this process's watchers never hear about it.
Dropping the tick would narrow a public API from "a watch observes writes to
the store" to "a watch observes writes made through this `Beehive`". So the
tick drops from 1s to a 30s floor (`withWatchFloorInterval`) instead of going
away:

- Almost all of the query savings remain — one read of one number per kind per
  30s, against one per watch per second.
- Latency comes from the wake, and the floor never delays a local write.
- The floor is also how a failed step retries on a quiet kind, and how a
  retention trim is noticed.

The watch tail therefore stays a periodic-scan driver, at a relaxed cadence,
with a wake in front. Unlike the schedule watch, it gets no exception.

### The drain loop

A step reads at most `tailPageCap` (512) entries. A burst collapses into one
wake, so a tailer that read one page per wake would strand the remainder until
some later write. `run` therefore repeats until a step returns a short page.
The stop condition is the page length, not a second position read: a short
page means the log is drained, exactly, and `step` already opens with the
position check.

### The merge, and why nothing is dropped

`Merge` keeps the pending value when the new send is stale
(`next.ResourceVersion <= prev.ResourceVersion`), then promotes: a run whose
start the subscriber has not seen still reports as a create.

It never returns `keep = false`. Dropping an unread create/delete pair looks
safe and is not: the pending slot is shared by all subscribers, but a snapshot
is per subscriber. Take a subscriber that registers, then snapshots at 100 —
after the create was published at 95, so the object is in the snapshot. If the
delete at 105 cancelled the pending create, that subscriber would hold the
object forever with no correction coming. That is permanent divergence, not a
latency cost. The residue is harmless the other way: a delete for a key a
consumer never saw is a no-op at any cache.

The create promotion cannot see the per-subscriber floor either, but the cost
is benign: a subscriber whose snapshot already held the object gets a repeated
`Added`. That is stated in the contract rather than fixed, because suppressing
it would need per-receiver bookkeeping of every key ever seen.

### Subscribers

A watch registers its fan-out receiver **before** it snapshots — conflate has
no replay — and drops deliveries at or below the snapshot's position. The floor
is sound because `resource_version` is one log-wide sequence and the snapshot
is a consistent cut at it, so "at or below the floor" means "already in the
snapshot" for every key. `ObjectChange` now carries that `ResourceVersion`,
which is also what makes `WithResumeFrom` usable.

A resume checks that its position is still inside the log on the caller's
goroutine, then replays the gap in pages on the stream's goroutine: with a day
of retention the gap can be far more than one page.

A relation load that fails is retried rather than skipped. The old poll could
leave the change in the log and try again next tick; the tailer's cursor has
already moved past it, so the subscriber retries the load on the batch it
holds.

### The tailer's own ErrWatchTooOld

The cursor is shared, so a cursor below the retention horizon is not one
subscriber's failure. The tailer closes its fan-out and ends: every subscriber
gets `ErrWatchTooOld` and resubscribes onto a fresh snapshot, and the next
watch starts a new tailer. Advancing the cursor to the horizon instead would
silently drop changes for every subscriber, and there is no single subscriber
to hand the error to.

## Consequences

- **A quiet kind costs one read per floor interval, whatever the watch count.**
- **A lone single-object watch gets slower.** The key filter runs on the
  fan-out, not before the read, so it pays for its kind's whole write volume.
  The sharing pays off above roughly two subscribers per kind.
- **Ordering weakened**, from "changes arrive in write order" to "each object's
  latest state arrives once, newest wins; an `Added` may repeat for an object
  the snapshot already carried". Order across objects was never load-bearing:
  beehive is level-triggered and a subscriber acts on current state per object.
- **`ErrWatchTooOld` narrowed.** A slow subscriber coalesces and can no longer
  fall behind the horizon, so the sentinel now has two producers: a resume
  below the horizon, and the tailer's reset.
- **`LagPolicy` is gone.** Conflate has no backlog to bound, so `LagFail` had
  nothing to fail on and `LagBlock` became the behavior, with better isolation.
- **A tailer outlives its last subscriber.** An application watching 50 kinds
  once holds 50 goroutines and 50 reads per floor interval for the process's
  life. That is the accepted trade against restart churn and a teardown race.
  Idle teardown stays available if a kind count ever makes it worth it.
- **Memory moved** from one shared page per tick to N receivers × undelivered
  keys × one row image. Bounded by undelivered keys, not by total writes.
- **The watch machinery comes up in `New`, not `Start`**, because watches
  always worked on an unstarted `Beehive`. `stop` tears it down either way.
