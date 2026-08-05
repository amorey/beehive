# Every driver is a periodic scan of the store, on its own cadence

- **Status:** Accepted — implemented in `internal/driver`, `beehive.go`, `reconciler.go`, `waker.go`, `objectswatch.go`, `eventswatch.go`, `gc.go`, `options.go`. **The object watches gained a commit wake and a 30s floor on 2026-08-03**; they are still scans, not an exception. See [the shared-tail ADR](2026-08-03-watch-shared-tail.md). **The stale-dependents pass gained a cursor on 2026-08-03**; it is still a scan. See [the cursor ADR](2026-08-03-stale-dependents-cursor.md). **The dependency
waker lost its tick on 2026-08-05**; it is the one store-backed driver that is
not a periodic scan. See [the wake-driven ADR](2026-08-05-the-waker-is-wake-driven.md).
- **Date:** 2026-07-28; watch tail and stale-dependents cursor amended 2026-08-03; waker amended 2026-08-05

This record is the *why*. For the case-by-case map of what each driver actually
covers — every trigger, where it is recorded, which driver finds it, whether it
survives a restart, and which tests pin it — see
[reconcile triggers](../reconcile-triggers.md).

## Context

Beehive is level-triggered: a controller reconciles from *current* state, never
from a sequence of changes. So the store already records everything that is owed —
`generation`/`observed_generation` says a spec has not converged,
`reconcile_owed` says a dependency wake is due, `deletion_requested_at` says a
collect is pending, and `resource_version` is a globally monotonic cursor over
every write. The only open question is how that recorded work gets *found*.

## Decision

Nothing **store-backed** is pushed. Every driver over the store is a periodic
scan, and the observable each one
reads is the column the write already moved. A write's durable trace **is** its
notification.

*(Amended 2026-08-05.)* The dependency waker is the one exception, and it is an
exception to the *cadence* rather than to the record: it still reads the write
log and still enqueues from what it finds there, but only a commit makes it
look. What entitles it to that is the stale-dependents pass, a periodic scan
that finds a superset of what the waker finds and cannot be disabled — so the
rule holds where it matters, one level down.

**One value in beehive is pushed, and it is the one that never reaches the
store.** The schedule gauge is the work queue's memory, so no second process and
no embedder can move it: its hub sees every writer that exists, which no
store-backed hub can. It therefore has a push path and no poll at all. The
criterion, so it is an exception and not a loophole: a poll may go only when its
hub can observe every writer, and when the notify is derived from the state
rather than asserted at each site. Every driver in the table below fails the first
test by construction, because a second process can always write to the store. See
[the schedule-watch ADR](2026-07-27-schedule-watch.md).

**A wake in front of a scan is a weaker claim, and the watch tail has one.**
A commit publishes its kind to a process-local hub, so a local write reaches
subscribers without waiting for a tick. That hub sees only this process's
writers, which is exactly why the scan stays: the floor tick covers a second
process over the same file, a failed read, and a retention trim. The tail is a
driver with a wake, not an exception to the rule.
→ [ADR](2026-08-03-watch-shared-tail.md).

| Driver | What it scans | Paced by | Default |
| --- | --- | --- | --- |
| Owed pass | unsettled specs, `reconcile_owed` | `withOwedPassInterval` (per-kind, unexported) | 30s |
| Full pass | every object of the kind | `WithFullPassInterval` (per-kind) | 0 (off) |
| GC sweeper | `deletion_requested_at`, event retention, then free pages | `WithGCInterval` (global) | 30s |
| Dependency waker | `resource_version` above a scan watermark | a commit wake; no tick | — |
| Stale dependents | targets above each dependent's watermark | `withStaleDependentsInterval` (global, unexported) | 60s |
| Client watch | the kind's write log, above one shared cursor | `withWatchFloorInterval` (global, unexported), plus a commit wake | 30s |
| Event watch | one object's event log, diffed against last reported | `withWatchPollInterval` (global, unexported) | 1s |

They are separate cadences because they are separate jobs with sharply different
cost curves, and one interval governing several would mean tuning any of them moves
the rest. They share two loop shapes in `internal/driver`: `driver.Run` (one cadence with
an eager first pass — the GC sweeper, the stale-dependents pass, each event
watch) and `driver.TickerChan`
(a nil channel for a disabled cadence, for the reconciler's select over the owed
*and* full passes). The object tailer and the waker are the loops outside them: each selects over
its wake, a timer and its context, so neither shape fits. Keeping them together is what makes "a non-positive interval
disables this driver" one answer rather than one per driver — for the drivers
built from them. The waker is built from neither and is turned off by a flag of
its own: with no cadence left, both of its intervals are floors, and a floor
switched off means "no floor" rather than "no waker".

### The stale-dependents pass re-derives what the waker may have missed

`DependentsListStaleSince(kinds, pos, through, limit)`, paged to exhaustion on each
step, over the `depends_on` edges of registered kinds: a dependent is stale when a
target sits above the watermark its last successful reconcile recorded. It is the
correctness backstop the waker is an optimisation over, so unlike the waker it
**cannot be disabled**, and unlike every other driver it asks about current state
rather than about a column a write moved — which is exactly why it recovers a wake
lost by any means, including a bug in the wake path or a process that never ran a
waker. Its cadence is set by acceptable staleness after a crash rather than by
cost, since a steady-state pass finds nothing and enqueues nothing. Full reasoning
in [its ADR](2026-07-29-dependency-watermarks.md).

*(Amended 2026-08-03.)* The scan is now bounded by a cursor over target
`resource_version`, so a sweep costs what changed rather than the size of the
graph, and an idle tick is one read. It still asks about current state: the cursor
is process-local and starts at 0, so every process re-derives the whole graph once.
That is what keeps "recovers a wake lost by any means" true. See
[the cursor ADR](2026-08-03-stale-dependents-cursor.md).

### The owed pass drains what the store records as owed

`enqueueOwedPass` = `ObjectsListUnsettledIDs` + `ReconcileOwedListIDs`. Its cost is
bounded by what is actually outstanding, so both listings return nothing in a
converged system. They stay separate rather than unioned in SQL so one failing
still lets the other through, and `enqueueFrom`'s log names which lost its pass.

### The full pass is the only one not bounded by owed work

`enqueueAll` reaches an object nothing recorded as owing anything: process-scoped
state a restart invalidated (a liveness condition reads as "verifying" until a
controller in *this* process rewrites it). It is opt-in because its cost scales
with the object count, and because the startup pass already covers that ground
once per process.

### GC routes rather than collects

`gcSweeperRun` = `deletionPendingSweep` + `eventRetentionSweep`. It is global
because it spans kinds that have no controller at all.

`deletionPendingSweep` **routes**: a registered kind is enqueued, a client-only
kind is collected directly. That is why `DeletionRequestsList` returns
`[]ObjectRef` rather than ids. The routing is load-bearing, not an optimization —
`gcCollect` cannot clear a finalizer (it cascades to owned children and returns
while any remain, because releasing one is the controller's decision), so an object
of a registered kind must be handed to its reconcile loop or it would make no
progress on every sweep, forever. A client-only kind has no loop to enqueue onto,
which is the whole reason the global sweep exists. The routing lives in one place,
`deletionAdvance`, and the sweeper is its only caller, so every collect runs on the
sweeper's goroutine and ctx rather than on whichever caller requested the delete.

### The dependency waker scans a watermark, store-wide

`ObjectWritesListSince(afterRV, limit)`, paged and version-ordered. The waker seeds
its cursor from `ObjectWritesMaxVersion` at startup — so the first scan reports
changes from startup, and everything below the seed is the startup pass's ground —
then pages everything above it through the wake path, advancing per page. A page is
complete up to its own last row, so the cursor advances by that row; a failed scan
or lookup leaves it where it was, and the next pass re-reads exactly what is still
owed.

This is the one driver whose cost is bounded by what *changed* rather than by what
exists, which is why it can run on every commit rather than on a cadence
of its own, and why a dropped wake needs no full pass to repair. A settled dependent
is invisible to every owed-work listing, because its own generation never moved;
re-reading the change that stranded it is the only thing that can find it.

The scan watermark is deliberately **not persisted**, and this is the argument that
chose the mechanism beside it. A scan watermark records *delivery* ("the waker
reached rv N"), while what has to survive a restart is *convergence* ("the dependent
actually reconciled against the new state"). Persisting it would buy half the hole,
leave the twin half open, and charge a write per page on a single connection. So
durability was attached to convergence instead, per dependent and re-derived: see
[the dependency-watermarks ADR](2026-07-29-dependency-watermarks.md). That is what
makes a lost wake here cost latency rather than divergence, and it is why this
driver can stay a pure in-memory optimisation.

**It is store-wide, not per registered kind.** A `depends_on` edge may point at an
object of any kind, including one only ever used through `Client` with no
`Register`, so no per-kind scan could name it. The sharp case is not a slow wake but
a permanent strand: `edges.to_id` is `ON DELETE RESTRICT`, so deleting such a target
only sets a tombstone, and with no dependent woken to drop the edge the row stays
deletion-pending while the sweeper retries it forever. Any per-kind list would have
to be computed from something — the registered set, the kinds present at `Start`,
the kinds currently referenced by an edge — and each of those misses a case. Routing
needs no help from the scan: `dependentsWake` enqueues each dependent by the
dependent's own kind.

Two costs come with that and are accepted. A page resolves its dependents in one
`EdgesGroupIncomingByID` rather than a lookup per change, because the store is
single-connection and the waker's reads serialize against every writer. And the
single waker goroutine is a process-wide head-of-line block: a slow edges query
delays wakes for every kind, where per-kind wakers would delay only their own. Per-
kind wakers are the defect, not the alternative. If it ever bites, the shape is a
small pool of drain goroutines partitioned by target id; unbuilt, and not worth the
concurrency until a workload shows the stall.

### The object watches share one tailer per kind

`Watch` and `WatchList` no longer scan for themselves. One tailer per kind owns
the kind's write-log cursor, and every watch on that kind subscribes to its
fan-out, so reads scale with watched kinds rather than with watch count. A quiet
tick is one read of the kind's log position; a busy one reads the entries above
the cursor and then one batched read of current state. A commit wakes the
tailer, so latency comes from the wake and the floor only bounds what a wake
cannot reach. → [ADR](2026-08-03-watch-shared-tail.md).

The other watches share nothing with it, so read the paragraph
above as being about the object watches alone. `EventsWatch` also compares
`resource_version`, but per run rather than per object, and its gate is **one number
and no second read**: `EventsMaxVersion(ctx, id)`, the high-water mark of that one
object's log, with the blob-bearing `EventsList` paid only when it moves. That number
is cheaper than the listing rather than free — on SQLite it still reads one table row
per run, because no index carries `events.resource_version` under `object_id` — so
this quiet tick is the least indexed of the gated three. There is no
liveness half because there is nothing to find with one — the watch emits no
`Deleted`, since an append-only log has no tombstone, and retention removes runs
silently. The mark spans every category, so a filtered watch can be woken by a run it
will not be shown; that costs a listing that reports nothing, never a missed run.
`SchedulesWatch` is further out still: it holds a `Schedule` rather than a version,
reads memory rather than the store, and emits the gauge itself rather than a change.
So of the four, three are gated and the fourth touches no store at all.

## GC is the one cadence that cannot be disabled

`WithGCInterval` rejects `d <= 0` with `ErrInvalidOption`, checked *before* the
target type-switch: the value is nonsense wherever it was aimed. The reconcile knobs
accept 0 because `Client.Requeue` still drives a pass by hand. Nothing public
triggers a collect, so a sweeper-less `Beehive` would accumulate deletion-pending
rows with no recourse, each `owned_by` edge RESTRICT-blocking its owner's delete.

That invariant is load-bearing *inside* the sweeper too: every failure there is
logged and swallowed on the promise of a next tick, which is only true while a
cadence is guaranteed. A guaranteed cadence is what lets the sweeper treat a failed
`DeletionRequestsList` as latency rather than as a row stranded for the life of the
process.

`gcSweeperRun` still returns early on a non-positive interval — unreachable through
`New`, kept so a `Beehive` assembled field-by-field (the `withoutGCSweeper()` test
helper) has no sweeper instead of panicking in `NewTicker`.

`withWatchPollInterval` is mandatory for a different reason: for `EventsWatch` it
is not a backstop but the delivery mechanism, and a watch that never polls is a
stream that never emits — there is nothing such a value could mean.
`withWatchFloorInterval` is mandatory because it is the object tail's backstop in
the ordinary sense: without it, a write whose wake this process never sees would
leave subscribers stale for good.

## Only two cadences are public

`WithFullPassInterval` and `WithGCInterval` are exported. The owed pass, the
dependency waker, the stale-dependents pass, the watch tail and the event poll
keep their options unexported — `withOwedPassInterval`,
`withWakeScanMinInterval`, `withStaleDependentsInterval`,
`withWatchFloorInterval`, `withWatchPollInterval` — reachable from the package's
own tests and nowhere else.

The split is not "cheap versus expensive"; it is **which cadences a caller has a
reason to move**. The four unexported ones are each bounded by what is outstanding, by what changed,
or by the dependency graph rather than by what exists, so they cost about the same in a large
deployment as in a small one, and shortening them buys latency the store pays for on
every tick forever. Three of the four also cannot be turned off without opening a
correctness hole, which makes an exported knob mostly a way to break the guarantee
that convergence is a property of the system rather than of its configuration. The
one cadence whose cost genuinely scales with the deployment — the full pass — is
exported, and off by default.

`Client.Requeue` is what replaces a shortened interval. It aims latency at one
object, where an interval aims it at every object for the life of the process, and it
is already the documented way to beat a pass. The `examples/` programs are the proof:
they run on production defaults and nudge each object they create, where they
previously turned three cadences down to 50ms.

This narrows a surface that was already shipped, and it is the conservative direction
to narrow in: an interval can be exported later once a caller shows what they need it
for, but an exported interval is a promise about internal driver structure that a
future change — splitting the owed pass, merging the waker into it — would have to
keep.

## Startup is two steps, only one of them a choice

`enqueueOwedPass` runs unconditionally — an object already owed a pass is not a
cheapness knob — and `WithStartupFullPass` (default false) adds the full pass.
Deletion-pending work is not resumed here: the GC sweeper's own unconditional
startup pass routes it.

So the two unconditional steps, the owed drain and the GC sweep, are the whole of
what startup guarantees, and that is deliberate: **no reconcile may depend on either
full pass.** Both scale with the object count rather than with what is outstanding,
which disqualifies them as a correctness mechanism — a system whose convergence rests
on a sweep converges only while the sweep stays affordable. It also makes the two
defaults one decision rather than two: the periodic full pass was always off, and the
startup one now matches it, so a hole cannot hide behind "well, startup catches it".
The dependency waker's restart behaviour used to be the open item here, for exactly
that reason — the pass that covered it was never entitled to. It is now carried by
the stale-dependents pass, whose cost is bounded by the dependency graph rather than
by the object count, which is what makes it eligible to be depended on.

## Writes record what they owe, and only a spec write also starts it

A write's durable record is what puts the object in a driver's listing. A spec write
bumps the generation, which is what the owed pass lists; a delete sets
`deletion_requested_at`, which is what the sweeper lists. The signal and the durable
record are the same write, so a rolled-back transaction leaves nothing behind for
free. That is the invariant, and it is unchanged.

**One write also starts the work it recorded.** A spec write that leaves the row
unsettled enqueues that row through `Store.AfterCommit`, so a `Create` or `Update` no
longer waits up to an owed-pass interval to be seen. The listing still runs and still
finds the same objects, so the enqueue is an optimisation over it rather than a
replacement — a lost enqueue costs latency, never convergence. The gate is the store
reporting that the write *changed* the object, which is what keeps a byte-identical
update from waking a controller that re-applies its own spec. See
[the spec-write-enqueue ADR](2026-07-31-a-spec-write-enqueues-its-own-object.md).

**A delete still schedules nothing.** Nothing registers a collect; the sweeper's
cadence is the whole of that path.

`Store.AfterCommit` therefore has two users: `WithOnCreate`, a caller-facing
guarantee that a create-conditional side effect never fires for a row a rollback
discarded, and the spec-write enqueue above. Both want the same property — fire on a
real commit, and never on a rollback.

## Consequences

- **Latency is a number, per driver, chosen against that driver's cost curve** — two
  of them by the caller, three of them by beehive. A missed tick costs latency and
  nothing else: convergence is not at stake, because the record is still there on the
  next one.
- **Intermediate states are not observable.** A watch coalesces per interval, so an
  object created and deleted inside one interval is reported to nobody, and three
  writes between two polls produce one `Modified` carrying the third.
- **A physical delete draws no `resource_version`.** Versions live on rows, and a
  deleted row has none left to carry, so a scan of the write log cannot report a
  removal. Consumers that must observe one derive it from absence, which is what the
  watch diff does. Nothing else needs to: a target with a live `depends_on` edge
  cannot be deleted at all.
- **The store owns no goroutines.** `Close` closes the database and nothing else. A
  consumer mid-scan simply fails its next query.
- **A scan tells you what *is*, not what happened.** A write-log row is
  `(id, resource_version)` and carries no lifecycle type — the row records the
  version it now holds, not how it got there. Every consumer re-reads current state
  anyway; one that genuinely needed the distinction would have to add a durable
  change log rather than reach for a stream.

## If latency is ever pushed

A push path is admissible only *above* this one: the scans stay the path that
guarantees convergence, and pushing may only make work arrive sooner. These are the
constraints such a layer has to satisfy, listed because each is easy to miss and
expensive to learn.

- **Lag needs a policy, and conflation is a domain decision.** Per-object conflation
  beats a bounded ring plus a lag error and a relist — a slow watcher converges to
  current state, and per-watcher memory is bounded by the live key set rather than by
  write volume — but create-then-delete annihilation, and which tombstone survives a
  coalesce, are beehive's rules to make, not the transport's.
- **Conflation changes what a consumer may gate on.** A create-then-modify that
  coalesces arrives as a single `Added`, so anything keyed on `Modified` alone
  silently misses changes.
- **A stream that opens with a snapshot must dedupe against it.** Changes landing
  between subscribe and read have to be excluded; `resource_version` being one
  store-wide monotonic cursor is what makes that a scalar comparison rather than a
  set difference.
- **Publication order is part of the contract.** If a consumer treats delivered
  versions as a cursor, a reordered publish advances it past an undelivered change.
- **Delivery order is not version order.** Under first-touch delivery a batch's
  newest version says nothing about what is queued behind it, so a cursor advanced
  from a batch skips the backlog; the bound has to come from the receiver's own head.
- **A store-wide stream must not carry `*RawObject`** — an undelivered change would
  pin that row's spec and status blobs for as long as the consumer lags.
- **Anything the store owns, it must also tear down.** Today `Close` has no ordering
  problem to solve; a hub reintroduces one, including what a consumer mid-read sees.
- **Keep it off the correctness path.** The failure that matters is the silent one: a
  push path that dies while the control plane still looks healthy. If the scans remain
  what guarantees convergence, that entire class is a latency regression rather than a
  correctness bug — which is the only reason a push path is worth having.
