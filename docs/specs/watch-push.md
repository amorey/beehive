# Spec: replace the object watches' polls with delta push, and add the
stale-watch pass

Status: draft — phase 4 of [push-conversion](push-conversion.md)
Date: 2026-07-31
Scope: `watchpoll.go`'s two object-watch entry points, the write and
delete paths in the beehive layer, and the public watch contract.
Related: builds on the hub that [wake-push](wake-push.md) introduces, which
must land first. It also adds `ObjectsGetSummary`, a `Store` break, which
used to be scheduled separately and now lands here with its only consumer —
see "The store read the pass needs". The
[drivers ADR](../adr/2026-07-28-periodic-scan-drivers.md) lists the
constraints on a push path; this spec must satisfy each one.

## Open decision: the poll now stays, and three things may be unnecessary

The plan no longer deletes the object-watch poll. That was decided after
this spec was written, and it puts three of its parts in question. Settle
this before implementing.

**The poll can run beside the hub, and I said earlier that it could not.**
That was wrong. `poll` already dedupes on `seen`: it skips a row whose
`resource_version` matches what the stream last reported, and it emits a
`Deleted` only for an id still in `seen`. So one goroutine can select over
the hub receiver and the poll ticker, with both maintaining `seen`. Push
delivers first and records; the next tick sees a match and stays quiet.
This is the same arrangement [events-push](events-push.md) uses, for the
same reason.

**If the poll stays, it is the backstop.** Every tick re-reads the store
and diffs against `seen`, so a delta the hub never received is emitted on
the next tick. That is a complete repair, at the poll interval, with no new
machinery.

So these three parts of this spec exist only to back a stream that has no
poll:

| Part | Still needed if the poll stays? |
| --- | --- |
| The stale-watch pass | No. The poll re-derives every tick. |
| `ObjectsGetSummary` | No. It exists only to feed that pass. |
| `Lagged` | No. Only that pass raised it. It is this plan's one public contract change. |

Dropping all three would leave this plan with no `Store` break left to
make. `EventsMaxVersion` has landed, and `ObjectsGetSummary` — specified
below — is the only remaining reason to touch `Store`. So this decision,
and nothing else, is what decides whether an external backend ever pays for
the push conversion.

That is also why the read is specified in this file rather than in one of
its own: the decision that determines whether it is built at all is at the
top of this page.

**What is lost.** Goal 2 below does not survive: a list stream keeps its
`seen` map and the decoded bodies it holds for tombstones. Push then buys
latency only, not memory.

**What is kept.** Everything in this spec that makes deltas arrive at
commit time: the per-kind hub, the merge and its annihilation rules, the
writer-built deltas, and the subscribe sequence.

The rest of this spec is written as a switchover, with the poll removed.
Read it with that in mind until this is decided.

## Goal

Two goals, and they arrive together:

1. An object watch gets its changes at commit time, and not at the next
   1-second tick.
2. A list watch becomes delta-based: no stream holds the decoded bodies of
   its kind. Today each list stream keeps a `tracked` entry for each object
   it reported, body included, for the life of the stream.

Both `ObjectsWatchList` and `ObjectsWatch` convert together. They share one
poll engine, so converting one alone would mean threading push through that
engine for a single caller. They also share the hub, the merge and the
subscribe sequence, so the second watch is a key filter rather than a
second design.

## Non-goals

- Do not convert `EventsWatch`; that is [events-push](events-push.md),
  which keeps its poll as a backstop. `SchedulesWatch` is already
  converted, push-only, and out of this plan — see the
  [schedule-watch ADR](../adr/2026-07-27-schedule-watch.md). **Do not read it as precedent for
  removing a poll here.** It qualified because its value never reaches the
  store, so it has no writer this process cannot see. Every object watch
  reads store state, where a second process can always write.
- Do not change the snapshot guarantee: a returned stream always carries a
  snapshot, and a failed first read returns an error, not a stream.
- Do not weaken the level-triggered contract: intermediate states stay
  invisible, and a subscriber that reads converges on current state.
- Do not fence writes. A slow subscriber harms only its own stream.

## Design

### Deltas come from the writer

The committing write is the one place where "create", "update" or "delete"
exists as a fact. The write path builds the delta there — the change type
plus the raw row — and hands it to the hub through `Store.AfterCommit`, as
the wake notify does. Each subscriber decodes the row on receipt (see the
delta section below):

- Create and update: the mutator already holds the written row.
- Physical delete: the remover (`gcCollect`, and the id-keyed delete path)
  reads the row before it deletes it, and that read builds the `Deleted`
  tombstone with the object's final state. Do this read only when the kind
  has at least one subscriber, so a system with no watchers pays nothing.
- A deletion *request* is an update (the tombstone fields moved), not a
  `Deleted`. `Deleted` means the row is gone, as today.

Thus no stream needs a `seen` map. Nothing re-derives the change type from
a comparison, so no stream remembers what it reported.

### The hub fans out

One `conflate.Hub[ObjectID, delta]` for each kind that has subscribers, as
the umbrella spec's messaging-backend section describes. A kind with no
subscribers has no hub, so its deltas are discarded before any queue.

- **One key for each object id.** Each subscriber is a `Receiver`, and each
  receiver holds one slot for each id plus an insertion-ordered key queue.
  So the hub conflates by object id and keeps arrival order, which is what
  this spec asked for, and the library rather than beehive owns the
  concurrency.
- **`Send` never blocks**, so one slow subscriber never delays the writer
  or its siblings.
- **Coalescing is bounded by the live key set of one kind**, not by write
  volume. A subscriber that stops reading for a minute holds at most one
  pending delta for each object of its kind.
- **A delivered event has left the receiver's slots**, so a merge can never
  rewrite what a subscriber already saw. A delivered `Added` still gets its
  `Deleted`. This is the library's rule, and it is exactly the rule the
  contract needs.
- **The stream a caller sees is `Receiver.Chan()`**, adapted to
  `<-chan ObjectChange[Spec, Status]`. No gobus type reaches the public
  API.
- **`ObjectsWatch` is the same subscriber with a key filter.**
  `hub.Receiver(hub.WithKeyFilter(func(k ObjectID) bool { return k == id }))`
  drops every other id at enqueue, so a single-object stream holds one slot
  rather than one for each object of its kind. Everything else — the merge,
  the subscribe sequence, `Lagged`, the stale-watch pass — applies
  unchanged.

### The delta the hub carries, and the merge that coalesces it

The hub is per-kind but not generic: it lives beside the other beehive
machinery, below the `Register` boundary, so its value type carries the raw
row and the change type. Each subscriber decodes on receipt with its own
migrator, which is where the migrator and the quarantine path already live.

The drivers ADR says a store-wide stream must not carry `*RawObject`,
because an undelivered change would pin that row's blobs while a consumer
lags. That constraint holds for a store-wide stream. These hubs are
per-kind and conflated, so what a lagging subscriber pins is one row for
each live object of one kind — the same order as the snapshot it would get
if it resubscribed. Record this narrowing in the new ADR. Do not read it as
permission to carry `*RawObject` on a store-wide stream.

The `Merge` is beehive's rule, not the library's. Given a pending change
and a new one for the same id:

| Pending | New | Result |
| --- | --- | --- |
| `Added` | `Modified` | `Added`, with the new row |
| `Added` | `Deleted` | Nothing: `keep == false` |
| `Modified` | `Modified` | `Modified`, with the new row |
| `Modified` | `Deleted` | `Deleted`, with the new row |
| `Deleted` | `Added` | `Added`, with the new row |

Two of these rows are the decisions the drivers ADR reserved for beehive.
The `Added`+`Deleted` row is create-then-delete annihilation: the
subscriber never saw the object, so it is told nothing, which matches the
poll's behaviour today for an object created and deleted inside one tick.
The `Added`+`Modified` row is why a consumer must not gate on `Modified`
alone: a coalesced pair arrives as one `Added`. Say both in the doc
comments.

A `Deleted` followed by an `Added` is a reused name on a new id, so it
cannot happen under one key — ids are never reused. Keep the row anyway, so
the merge is total.

### What `ObjectsWatch` keeps

Two behaviours of the single-object watch are easy to lose in the
conversion. Pin both with tests.

- **An id that does not exist yet streams nothing until it is created.**
  Its snapshot is empty, and the create arrives as an `Added`. The poll got
  this from an empty one-row listing; the hub gets it from an absent key.
- **A foreign id is invisible.** The hub is per-kind and the client is
  kind-scoped, so an id belonging to another kind never enters this kind's
  hub. That is the same fold `scopedRow` does today, obtained structurally
  rather than by a check. Do not add a check that cannot fire; do add the
  test that proves it cannot.

### Subscribe: register, snapshot, dedupe

1. Register the subscriber with the hub (`hub.Receiver()`). Deltas start to
   queue in that receiver's slots.
2. Read the snapshot on the caller's goroutine, and return the stream. A
   failed read returns an error, as today. `ObjectsWatchList` reads
   `ObjectsList`; `ObjectsWatch` reads its one row through `ObjectsGet`,
   folding "missing" and "foreign" to an empty snapshot exactly as
   `scopedRow` does now.
3. Deliver the snapshot, then the queue. Drop a queued delta when the
   snapshot already carries that id at the same or a higher
   `resource_version`. The store-wide monotonic version makes this one
   comparison for each id. The dedupe state is the snapshot's id-to-version
   map, and it is released when the queue drains past the snapshot's
   highest version — it is transient, not held for the stream's life.

### The lag protocol

`conflate` removes the overflow half of this protocol. It has no capacity
argument and never returns `ErrFull`, because coalescing already bounds a
receiver by the live key set. A slow subscriber therefore collapses its
backlog and catches up on current state; it does not lag out. So the one
remaining reason to end a stream is the pass's reset (below).

The reset ends the stream loudly, and in a way the subscriber can act on:

1. Send one final change with the new type `Lagged`, then close the
   channel.
2. The subscriber resubscribes and gets a fresh snapshot. Convergence is
   restored by the snapshot, not by replay.

This adds one value to the public `ChangeType`. A subscriber that ignores
it sees a closed channel, which today means only ctx cancellation; the doc
comments must state the new meaning. This is the one public contract
change in the spec.

Keep `Lagged` even though only the stale-watch pass raises it. It is the
name for "your view was reset, resubscribe", and a subscriber that handles
it handles every future reason for a reset.

### The stale-watch pass

The stale-watch pass is the repair mechanism for the delta streams, and it
is deliberately the twin of the stale-dependents pass: both re-derive
staleness from durable state rather than trusting a delivery, and both
repair without a human. One finds a dependent that has fallen behind its
target; this one finds a stream that has fallen behind the store.
Beehive is embedded, so no operator watches for a missed delta, and a
subscriber's view that diverges would otherwise stay wrong for the life of
the process. The failure it repairs is the one no monitor can see: a write
path that commits and does not hand the hub a delta.

The pass reads `ObjectsGetSummary(ctx, gk)`, which this spec adds: the
highest `resource_version` over the live objects of the kind, and the count
of those objects. "The store read" below holds the signature, the cost and
the naming.

The poll gate the summary replaces is not the same read, so do not describe
the summary as a promotion of it. The gate reads the *store-wide*
`ObjectWritesMaxVersion` plus a per-kind id listing (`ObjectsListIDs`), and
it derives "something vanished" from the ids it holds. The pass holds no
ids, so it needs the pair from the store instead.

The repair cycle:

- The hub keeps, for each kind with subscribers: the highest
  `resource_version` it delivered, and the live count as its deltas moved
  it (add one for `Added`, subtract one for `Deleted`).
- On the backstop cadence (the umbrella spec's last phase sets it, in
  minutes), the pass reads
  `ObjectsGetSummary` for each such kind and compares the pair against the
  hub's pair.
- On agreement: nothing. One pass costs one summary read for each watched
  kind, and no listing.
- On disagreement: end every stream of that kind through the lag protocol,
  and reset the hub's pair from the summary. The resubscribed streams
  re-derive the truth from their fresh snapshots. This close-and-resnapshot
  is the repair, and it completes with nobody watching. Also log the
  mismatch at error level — but only as a debugging trace for the
  embedder. No part of the design depends on a person reading it.

The pass is also why the repair stays cheap. The one alternative that
repairs without a human is the unconditional resync: close and resnapshot
every stream every few minutes, correct or not. That pays a blob-carrying
listing for each healthy stream on each cycle. The summary comparison is
the gate that limits the resync to the case where something is wrong: two
scalars for each watched kind, and the full cost only on a real
mismatch. The poll gate did the same job one level down — it skipped a
listing rather than a resync — so the summary carries that idea forward on
a read of its own after the poll gate goes away.

The count matters here for the same reason the id listing mattered in the
gate: a missed `Deleted` moves no version, and only the count exposes it.

Two limits, so the pass is not described as more than it is:

- It verifies the delivery bookkeeping (versions and counts), not the
  payload content. A delta with the correct version and a wrong body would
  pass. The writer builds each delta from the row it just wrote, so this
  class needs a bug in that one construction site.
- It compares against delivered state, so it must read the summary and the
  hub's pair at a quiet point of the queue, or tolerate skew. Specify the
  exact rule in the implementation: a mismatch must persist across two
  consecutive passes before it fires. A transient skew fires nothing.

### The store read the pass needs

`ObjectsGetSummary` used to live in a spec of its own, with
`EventsMaxVersion`, on the theory that a `Store` break is scheduled rather
than taken when convenient. It lives here now, because this read has
exactly one consumer and the open decision at the top of this spec is what
decides whether it is built at all. See "Why this read is no longer
scheduled separately" below.

Give a caller the pair that describes a kind's current state, with one read
and no blobs:

```go
// ObjectsGetSummary returns the highest resource_version over the live
// objects of kind gk, and the count of those objects. An empty kind
// returns (0, 0, nil).
ObjectsGetSummary(ctx context.Context, gk GroupKind) (rv int64, live int64, err error)
```

"Live" means the row exists. A deletion-pending row is live, because it is
still there and a watch still reports it; only a physical delete removes it
from both numbers. State this in the doc comment, because "live" could
otherwise be read as "not deletion-pending".

**Why the pair, and not one number.** A version alone cannot see a delete.
A removed row draws no `resource_version`, so the maximum over the live
rows can stay put while an object disappears. The count is what exposes
that. The existing watch poll solves the same problem with a per-kind id
listing, and it can do so only because a poll holds the ids it reported. A
caller that holds no ids needs the store to count for it.

**The name.** The shape follows `ObjectsGetMeta`: one read that returns a
small description of something larger. `Get` is the verb because the read
returns one value, which is what the
[naming ADR](../adr/2026-07-27-noun-verb-naming.md) asks for. Do not name
it `ObjectsChangeMark`, which an earlier draft used. "Change" reads there
as a verb, and this read changes nothing.

**Cost, stated honestly.** The read returns two scalars, and
`idx_objects_kind` (`objects("group", kind)`) is what prevents a full table
scan. It is not a constant-time read: a `MAX` over a non-leading column and
a `COUNT` both walk the kind's range, so the read touches one index entry
for each live row of the kind. It is blob-free, and its consumer runs it on
a backstop cadence of minutes rather than on a 1-second tick, which is what
makes the walk acceptable.

If a large kind ever makes that walk measurable, the answer is a covering
index that carries `resource_version`. Do not add that index now. Add it
when a query plan asks for it. `EventsMaxVersion` is the precedent in both
directions: its spec predicted the existing index would serve, the plan
disagreed, and `idx_events_object_rv` was added then rather than
speculatively.

**Do not make it an optional capability.** `FreePagesReleaser` and
`DriverCursorer` are optional because a backend that lacks them costs
latency and nothing else. A backend with no summary read leaves the
stale-watch pass with no backstop, which is a correctness gap, so this
belongs in `Store` itself.

**Test plan for the read**, in `sqlite/store_test.go`:

- **Empty kind:** returns `(0, 0, nil)`, not an error.
- **The pair tracks writes:** a create moves both numbers, an update moves
  the version only, and a physical delete moves the count down while the
  version stays put. That last case is the whole reason the count exists.
- **Kind scope:** a write to another kind moves neither number.
- **Tombstones count as live:** a deletion-pending row still exists, so it
  is counted, and its deletion request moved the version.
- **The double implements it:** `fakeStore` gets a real method, not a
  `panic`, because the stale-watch tests will drive it.

### Why this read is no longer scheduled separately

`Store` is externally implementable, so a new member breaks every backend
outside this repository, and the
[write-shapes ADR](../adr/2026-07-30-store-write-shapes.md) records that
the cost is paid once per break rather than once per method. That is why
the two reads shared a spec.

**That discipline is a post-release discipline, and it was being applied
before the release.** Beehive is not released, so there is no external
backend to break — the same argument the
[amend-in-place ADR](../adr/2026-07-31-amend-the-schema-in-place-until-release.md)
makes about the schema, and it expires at the same moment and for the same
reason. `EventsMaxVersion` landing alone was that discipline correctly not
being followed.

After the first release, group them again. Until then a read lands with the
push path that needs it, so that a read and its only consumer are never
separated by a scheduling rule with nothing left to protect.

One item does not fit this and keeps its own home: `EventsAdd` still takes
the read shape and wants an `EventsAddInput` beside `ObjectsCreateInput`.
It has no push consumer to land with, so it stays in `docs/TODO.md`, which
is where it already lives.

## The switchover, and what it leaves behind

Both object watches stop calling `objectStream` and subscribe to their
kind's hub instead. That happens in one commit, because a poll and a hub
feeding one subscriber stream would deliver every change twice. There is no
overlap period here, unlike the wake and event paths.

This spec deletes nothing. `objectStream`, `poll`, `deletedSince`, `seen`
and `tracked` all stay. Under the open decision at the top of this spec
they keep running as well, and `seen` becomes the shared dedupe state
between the two paths.

## Test plan

Write whitebox tests in `package beehive`. Synchronize on channels and
fakes, and never on sleeps.

- **Immediate delivery:** a commit reaches a subscriber without any tick.
- **Rollback delivers nothing:** a write inside a failed `Within` produces
  no delta.
- **Snapshot dedupe:** a write that lands between register and snapshot is
  delivered exactly once.
- **Slow subscriber:** a subscriber that stops reading gets conflated
  deltas per id, and its siblings are not delayed. A delivered `Added` is
  still followed by its `Deleted`.
- **Annihilation:** an object created and deleted while a subscriber is
  parked is reported to that subscriber not at all, and to a reading
  sibling as `Added` then `Deleted`.
- **Lag:** a stale-watch reset ends the stream with `Lagged` then close; a
  resubscribe converges from the fresh snapshot. There is no
  queue-overflow case to test, because `conflate` has no capacity bound.
- **Teardown ordering:** stopping the beehive closes each sender before it
  closes any hub, and a subscriber parked on `Chan()` sees its channel
  close rather than a hang. Also assert that no receiver is left registered
  after a stream ends, so a long-lived hub pins nothing. `TestMain`'s leak
  check backs this: a stream or hub goroutine that outlives its test fails
  the run.
- **Tombstone body:** a gc-collected object's `Deleted` carries its final
  state, and the pre-delete read happens only when a subscriber exists.
- **Stale-watch pass:** inject a skipped delta with a store double; the
  mismatch fires after two passes, the streams end with `Lagged`, and the hub's
  pair resets. A transient skew fires nothing.
- **Contract regression:** the existing list-watch tests for
  `Added`/`Modified`/`Deleted`, the snapshot guarantee and quarantine
  behaviour pass with at most mechanical changes.
- **Single-object delivery:** a write to the watched id delivers, a write
  to another id of the same kind does not, and the unwanted id occupies no
  slot. Assert the last one at enqueue.
- **`ObjectsWatch` edge cases:** a watch on an id that does not exist yet
  streams nothing and then reports its `Added`; a watch on a foreign id
  streams nothing, ever.

## Open questions

- Whether the pass also covers a kind with zero subscribers (it should
  not: no subscribers, no promise, nothing to repair).
- The pass's cadence: a fixed constant first, an option only if a workload
  asks. There is no queue bound left to choose, because `conflate` bounds a
  receiver by the live key set instead of by a capacity.

Two questions that earlier drafts left open are now closed by the design
above:

- **The quarantine path stays where it is.** The hub carries the raw row,
  so each subscriber decodes on receipt with its own migrator. An
  undecodable row therefore warns and is skipped in the client, for a delta
  and for a snapshot row alike, exactly as it does today.
- **Tombstone memory is gone for list watches.** A list stream no longer
  keeps decoded bodies at all. A lagging subscriber holds at most one raw
  row for each live object of its kind, and only until it reads. The `seen`
  map itself stays, because the poll stays. Under the open decision at the
  top of this spec, tombstone memory is not reclaimed at all.
