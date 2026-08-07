# One tailer per kind, woken by the commit path, above a floor tick

- **Status:** Accepted — implemented in `objectswatch.go`, `beehive.go`,
  `client.go`, `controller.go`, `gc.go`.
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
  and a wake never arrives before the row is readable. The signal carries no
  value — **not** even the write's resource version: most store writes return no
  version (see the [write-shapes ADR](2026-07-30-store-write-shapes.md)), and the
  tailer reads its own cursor from the store. So the hub is
  `watch.Hub[GroupKind, struct{}]` with no `Accept` gate, and a burst collapses
  into one pending wake because a receiver holds one slot, not a queue.

  The hub has a second kind of subscriber since
  [the waker's commit wake](2026-08-05-a-commit-wakes-the-dependency-waker.md):
  one `WatchAcross` receiver, watching every key rather than one. It is not a
  tailer and enqueues reconciles rather than feeding watch subscribers.
  `gobus/watch` is a state bus and this is a signal; the fit is close enough
  (keyed, coalescing, one receiver per kind) that a bus of its own has to earn a
  second caller first.
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
from the public verbs, and `TestKindWriteHubPublishesOnEveryWrite` is the guard. A
table derived from the verbs would miss two rows:

- `bumpObject`, reached by `Conditions().Set` and `Conditions().Delete`. Controllers
  write conditions constantly.
- The owner cascade. `DeletionRequests().CreateFromOwner` marks children across
  several kinds in one call, so the wake is routed by the refs it returns —
  the same way the new-edge enqueue routes by `EdgesAddResult.From` — and
  deduped by kind, so a wide cascade queues one commit hook per kind rather
  than per row.

A deeper alternative exists: an optional `ObjectWritesNotifier` on `Store`,
probed the way the optional capabilities once were, called from the
three helpers themselves. The bundled store could then never forget to publish,
and the floor tick already degrades correctly for a store that does not
implement it. It is not taken here for two reasons: it puts a watch concern
into the store contract for a guarantee the floor tick already provides, and
`Objects().Delete(ctx, id)` carries no `GroupKind`, so one site would stay manual
either way. Revisit if the table test ever fails to catch a new verb.

### Why the tick stays, at 30s

The [drivers ADR](2026-07-28-periodic-scan-drivers.md) says a poll may be
removed only when its hub observes every writer. This tail does not qualify: the
wake rides on a hook that a failed publish, a retention trim, or a read error
can leave the watcher behind on, and the tick is what re-derives from the store
rather than from a signal. So the tick drops from 1s to a 30s floor
(`withWatchFloorInterval`) instead of going away:

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

A failed step logs and backs off, bounded and capped at the floor interval,
rather than spinning; the cursor does not advance, so nothing is lost. **The
commit wake is suppressed while it backs off**, or the backoff is not one: a
commit landing during a failed drain refills the wake slot, so a tailer that
honoured it would re-read a degraded store as fast as it could fail, for as
long as anything kept writing. Dropping a wake loses nothing, since the retry
timer reads the log either way.

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

A resume replays the gap in pages on the stream's goroutine: with a day of
retention the gap can be far more than one page. It reads nothing on the
caller's goroutine — the position is the caller's, and the replay checks the
horizon on every page it reads anyway, so a probe first would cost a round trip
to answer the same question a moment earlier. A position retention has passed
therefore ends the stream rather than the call, which is also the only way a
live stream can report it: `ErrWatchTooOld` has one place to be handled. The
cost is that a store too broken to answer no longer fails a resume
synchronously — the replay retries it, bounded by the caller's context, as it
does for every other read after a subscribe.

A relation load that fails is retried rather than skipped. The old poll could
leave the change in the log and try again next tick; the tailer's cursor has
already moved past it, so the subscriber retries the load on the batch it
holds.

### Tailer lifetime

A tailer runs from its kind's first watch to its last. `tailerFor` hands back a
tailer with a **subscriber lease** taken on it and every caller owes one
`release`; the last release cancels the tailer's context and removes it from
the registry. The tailer's context is its own, not the beehive's, which is what
lets a watch on a `Beehive` that was never `Start`ed end with its caller —
`stop` is reachable only through the closure `Start` returns.

The alternative was to run every tailer until `stop`. It is simpler by one
counter and avoids a teardown race, but it makes tailer lifetime process-global:
an application watching 50 kinds once holds 50 goroutines and 50 reads per floor
interval forever, and an unstarted `Beehive` has no way to stop any of them.

**The teardown race is closed by moving the count only under `tailMu`**, the
same lock the registry moves under, so "registered" and "has subscribers" are
one condition and change together. A watch arriving during a teardown either
takes the lock first, and joins a tailer that is still registered, or takes it
after, and finds the entry gone and starts a fresh one. There is no window in
which a caller can join a tailer that is about to be cancelled. Release
compares identity rather than presence, because a tailer that reset below the
horizon was already replaced in the registry and its last subscriber must not
evict the successor.

**`tailerFor` builds outside `tailMu`**, and the second registry check plus the
discard path for the loser are the price. An earlier cut held the lock across
the build, on the reasoning that the cursor read happens once per kind per
process, so the only cost was that one kind's first watch. That priced the
wrong thing. `tailMu` is process-global and the cursor read parks on the
store's single connection, so holding one across the other stalls every kind's
watch setup — and every `release`, which `tailStream`'s defers run *before*
closing the caller's channel. A transaction holding the connection while its
own goroutine waits for such a channel to close then deadlocks all three ways
round.

The read still cannot move into `run`: the cursor has to be read before
`tailerFor` returns, or a subscriber that snapshots in between falls into the
gap below it.

The registry alone cannot answer "is there a tailer to join". A tailer that
ended below the horizon stays registered while its subscribers hold leases —
they release when they see the failure, which is after they read it — so
`tailerFor` checks health as well as presence. Without that check the
resubscribe an `ErrWatchTooOld` asks for can rejoin the tailer that just
failed and be told the same thing again, which is the recovery path failing
on itself.

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
- **Ordering weakened per object**, from "changes arrive in write order" to
  "each object's latest state arrives once, newest wins; an `Added` may repeat
  for an object the snapshot already carried".

  **Order across objects is not weakened, and the first cut of this design was
  wrong to say it was.** A batch is sorted ascending by resource version before
  delivery (`drainPending`). The reasoning that dropped it — beehive is
  level-triggered, so a subscriber acts on current state per object — holds for
  a subscriber that stays connected and does not hold for one that resumes.
  `ObjectChange.ResourceVersion` is documented as a resume position, so a
  descending pair lets a caller checkpoint the higher version and lose the lower
  change for good: the replay reads above the checkpoint and the live floor
  drops anything at or below it. The fan-out produces such a pair whenever a
  re-written object coalesces in place, keeping its original queue position
  while carrying a newer version.
- **`ErrWatchTooOld` narrowed.** A slow subscriber coalesces and can no longer
  fall behind the horizon, so the sentinel now has two producers: a resume
  below the horizon, and the tailer's reset.
- **`LagPolicy` is gone.** Conflate has no backlog to bound, so `LagFail` had
  nothing to fail on and `LagBlock` became the behavior, with better isolation.
- **A tailer's life is its subscribers'.** A kind watched once costs nothing
  after that watch ends, and a watch on a `Beehive` that was never `Start`ed
  ends with its own context — the caller needs no handle on the control plane
  to stop it. The cost is the teardown race, resolved under `tailMu`; see
  "Tailer lifetime".
- **Memory moved** from one shared page per tick to N receivers × undelivered
  keys × one row image. Bounded by undelivered keys, not by total writes.
- **The watch machinery comes up in `New`, not `Start`**, because watches
  always worked on an unstarted `Beehive`. `stop` closes the wake hub, which
  ends every tailer whose subscribers are still reading; the rest end
  themselves.
