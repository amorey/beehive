# Spec: push the event log, beside the poll that stays

Status: draft, less the poll gate. "Make the retained poll cheap" landed in
`1613685`; the push half is still a draft. Phase 1 of
[push-conversion](push-conversion.md), and the pilot for the phases after
it. One landing is scheduled ahead of it —
[new-edge-push](new-edge-push.md), which builds no hub and so takes nothing
away from what this pilot proves.
Date: 2026-07-31
Scope: `controller.go` (`EventsAdd`), `watchpoll.go` (`EventsWatch`), and a
new hub in the beehive layer. No new dependency: `gobus` arrived with the
schedule watch.
Related: [push-conversion](push-conversion.md) holds the policy this spec
inherits, including the messaging backend. The gate in "Make the retained
poll cheap" needed a new `Store` read, `EventsMaxVersion`, and the two
landed together. The [events ADR](../adr/2026-07-27-events-api.md) records
that decision.

## Goal

Deliver an event run to `EventsWatch` at commit time, and keep the poll
running as the backstop. Nothing a caller sees changes except latency.

**This spec is the pilot for the store-backed push paths.** It is first
because it is the cheapest place to learn whether the assumptions in the
later phases hold, not because the event log is the most valuable thing to
speed up.

It is no longer the first hub in the process. The schedule watch was carved
out and built separately in `4c8f607`, so `gobus` is a dependency, per-subscriber
receivers feed public channels, and the teardown ordering has a working
answer. **But it runs on `gobus/watch`, the keyed state bus — not on
`conflate`.** So this is still the first consumer of `conflate` in beehive,
and the first of its coalescing.

What this pilot is first at is therefore two things: **push over a store
write** — the commit hook, the rollback property, and a delta that outlives
the transaction that made it — and **`conflate` itself**. The schedule hub
touches neither: its notify sites are in-memory critical sections with no
transaction anywhere near them, and its bus keeps one slot per watched key
rather than a coalescing queue.

## Why the event log is the pilot

**Push and poll can share one stream here, and it costs no new code.**
`EventsWatch` already dedupes on a map of run id to `resource_version`
(`watchpoll.go`). A pushed run and a polled run carry the same version, so
the existing map suppresses the duplicate. The two paths overlap safely, so
a lost notify costs nothing at all while we learn.

An earlier draft said this was possible here and nowhere else. It is not:
the object watches dedupe on `seen` the same way, which is the open
decision at the top of [watch-push](watch-push.md). What is still true is
that the overlap here is free — the map exists and needs no changes — and
that it makes the pilot safe to get wrong.

**The blast radius is the smallest in the system.** `EventsAdd` is the one
write that bumps no object `resource_version`, so it drives no reconcile
and no dependency wake. A bug in this path costs an observability panel,
never a strand.

**It exercises more of the machinery than the wake hub does.** The wake hub
in [wake-push](wake-push.md) has one receiver, no `Chan()` and an internal
consumer. This has a receiver for each subscriber, fans out to several, and
hands a channel to a caller who may hold it, stop reading it, or leak it.
The schedule watch already proved that shape works; what is new here is
that the values come from committed writes rather than from memory.

**The commit hook gets its hardest test here.** `EventsAdd` is normally
called mid-reconcile, inside the outer `Within` that the reconcile
transaction opens. So "a rollback discards the notify" is exercised by
ordinary use rather than by a contrived test.

**The delta is already built and already thrown away.** `Store.EventsAdd`
returns `(*Event, error)`, and `controllerClientImpl.EventsAdd` discards
the run: `_, err := c.bh.store.EventsAdd(...)`. The push path gives that
return value its first consumer. No new store read, and no new store write.

## What this de-risks, and what it does not

State this plainly, so nobody reads a green pilot as more assurance than it
is.

| Assumption | Tested here? |
| --- | --- |
| `AfterCommit` fires on real commits only, including a savepoint unwind | Yes, in its hardest form |
| A delta built inside a transaction and delivered after it | Yes, and first |
| `conflate`'s coalescing: the pending-slot queue and `Merge` under beehive's load | **No** — the schedule watch runs on `gobus/watch`, which has neither |
| Non-blocking send and close precedence | Answered by the schedule watch: shared `gobus` machinery |
| Per-subscriber receivers and `Chan()` adapted to a public channel | Answered by the schedule watch |
| Teardown ordering: senders before hubs, and no receiver left registered | Answered by the schedule watch |
| The merge policy: annihilation, `Added`+`Deleted`, tombstone bodies | **No** |
| The stale-watch pass and the backstop pattern for a push consumer | **No** — and it may never be built; see below |
| The `Lagged` contract change | **No** — same |
| The snapshot-and-dedupe subscribe sequence | **No** |

Three rows are answered by shipped code rather than by this spec, which is
what carving the schedule watch out bought. They are the receive-side rows —
the machinery `gobus/watch` and `conflate` share. The pilot's remaining jobs
are the transaction boundary and `conflate`'s coalescing, which no shipped
consumer exercises yet.

The last four are absent for structural reasons, not by choice. An event
log is append-only, so it has no delete to annihilate and no tombstone to
carry. `EventsWatch` streams `Event` itself rather than a change, as the
[naming ADR](../adr/2026-07-27-noun-verb-naming.md) requires for a watch
over a log, so there is no `ChangeType` to extend. And `EventsWatch` has no
snapshot guarantee today: it returns its channel at once and takes its
first read on the eager tick inside the goroutine, with no error path for a
failed first read.

**Two of those rows may have nothing to de-risk.** The stale-watch pass and
`Lagged` exist only if [watch-push](watch-push.md) removes the object-watch
poll, which is the open decision at the top of that spec. If the poll
stays, this pilot's arrangement — push and poll into one stream, deduped on
what the stream last reported — is what phase 4 uses too, and these two
rows fall away rather than waiting for a later phase to answer them. That
raises what the pilot is worth. Do not read it as a reason to decide phase
4 either way.

So this pilot de-risks the commit hook. It does not de-risk the watch
semantics, and it no longer has to de-risk the library. That is the right
trade: the merge policy is a table we can argue about on paper, and a
notify that must fire exactly when a transaction commits is not.

## Non-goals

- **Do not remove or weaken the poll.** It is the only backstop the event
  log has. Push-only would make a lost notify a permanently missing event,
  which breaks the umbrella spec's rule that every push consumer has a
  backstop.
- Do not give `EventsWatch` a snapshot guarantee. That is a new contract,
  and it is not this spec's job.
- Do not change what a run is, how runs are grouped, or retention.
- Do not convert `ObjectsWatch`; that is [watch-push](watch-push.md).
  `SchedulesWatch` is already converted and out of this plan.

## The hub

One `conflate.Hub[eventKey, storeapi.Event]` in the beehive layer, beside
the wake hub and separate from it.

```go
type eventKey struct {
    Object ObjectID
    Run    EventID
}
```

- **The key is the pair, not the run id alone.** A subscriber watches one
  object, and `conflate`'s `WithKeyFilter` sees only the key. Putting the
  object id in the key lets an unwanted object be dropped at enqueue, so a
  subscriber never holds a slot for a log it does not watch.
- **The value is the raw run**, as the store returned it. The subscriber
  converts with `eventFromRaw` on receipt. Events are not versioned and
  have no `Migrator`, so there is no decode that can fail and no quarantine
  path — unlike [watch-push](watch-push.md), where the same choice carries
  a migrator.
- **The merge is newest-wins, and never annihilates:**
  `func(prev, next storeapi.Event) (storeapi.Event, bool) { return next, true }`.
  A run extended twice before delivery is delivered once, carrying the
  final count. That is what the poll does between two ticks today, so the
  contract does not move. Nothing returns `keep == false`, because
  retention emits nothing and a run never disappears from the stream.
- **Coalescing is bounded by the runs of the objects a subscriber
  watches**, which is one object. That bound is much tighter than the watch
  hubs get.

## The notify site

Register the notify through `Store.AfterCommit` in the beehive layer's
`EventsAdd` wrapper, carrying the run the store returned. Do not notify
from the store.

The rollback property is the reason. `EventsAdd` self-wraps in `Within` and
usually runs nested inside the reconcile transaction. A notify placed after
the call returns would fire for a run that the outer transaction then rolled
back, and the subscriber would be told about an event that does not exist.
`AfterCommit` discards a hook whose frame unwound, including a savepoint
unwind inside a nested `Within`, so the rule stays simple: notify exactly
when the commit is real.

`controllerClientImpl.EventsAdd`'s doc comment says "Nothing is published:
the row is the record, and an `EventsWatch` poll finds it once the write
commits." Update it. The row is still the record — that part does not
change, and it is what makes the poll a valid backstop.

## Delivery, and the filter that cannot live in the key

`EventsWatch` gains a receiver alongside its poll loop, and selects over
both. Each delivered run goes through the same `seen` map the poll feeds,
so a run the poll already sent at that `resource_version` is dropped, and a
run push delivers first is dropped when the poll re-reads it.

`WithKeyFilter` handles the object. It cannot handle the rest of
`EventQuery` — `Category`, `Type` and `Reason` are fields of the value, not
of the key — so those are applied on receipt, when the receiver hands a run
to the subscriber. The cost is that a subscriber filtering by category
still occupies a slot for every run of its object. That is bounded by one
object's log, so it is acceptable; say so rather than leaving a reader to
wonder.

`Since` and `Limit` need no special handling. A new or extended run is the
newest by `last_at`, so it is inside a `Since` window and inside a newest-N
window by construction.

## Make the retained poll cheap — landed

Shipped in `1613685`, ahead of the push half, as "The gate is severable"
below allowed. The rest of this section records why. The
[events ADR](../adr/2026-07-27-events-api.md) holds the decision.

The poll stays, so it should stop costing what it cost. `EventsWatch` had
no gate at all: every tick ran the full `EventsList` for its object,
selecting every column including the `Detail` blob, once for each
subscriber. It was the most expensive quiet tick in the watch surface.

It now has the gate the object watches have, reading
`EventsMaxVersion(ctx, id)`: the highest
`resource_version` over that object's events, served by the covering index
`idx_events_object_rv`. A tick whose mark has not
moved skips the listing. A log that is still empty reads 0, which matches
the zero cursor, so an object with no events never pays for a listing at
all.

**One scalar is enough here, and a count is not needed.** The object
watches need a count because a delete moves no version and must still be
reported. An event watch reports no deletions at all: a run only appears or
grows, and retention removes runs silently by design. So nothing is missed
by a gate that watches only for a moved version.

**The wrinkle, stated honestly.** `EventsWatch` rebuilds its `seen` map
from each listing on purpose, so that run ids removed by retention are not
held for the stream's life. Under a gate, a quiet tick does not rebuild, so
a trimmed run's id lingers until the next real event moves the mark. That
is memory only, it is bounded by the runs the stream has seen, and any new
event clears it. It was accepted, and the note is in the code where the
rebuild comment lives.

The gate was severable, and it was severed the other way: the gate landed
and the push half did not. The pilot's value does not depend on the gate,
and the gate did not depend on the pilot.

## Test plan

Write whitebox tests in `package beehive`. Synchronize on channels and
fakes, and never on sleeps.

- **Immediate delivery:** an `EventsAdd` reaches a subscriber without any
  tick.
- **Rollback delivers nothing:** an `EventsAdd` inside a `Within` that
  returns an error publishes no run. Nest it inside an outer `Within` too,
  which is the shape a reconcile actually produces.
- **No duplicate:** a run delivered by push is not delivered again by the
  next poll, and the reverse. This is the overlap property the pilot rests
  on, so test both orders.
- **Coalescing:** a run extended N times before delivery arrives once, with
  the final count.
- **Object filter:** a subscriber watching object A holds no slot for
  object B's runs. Assert at enqueue, not at delivery.
- **Query filter:** a subscriber filtering by category receives only that
  category, and its other slots drain rather than accumulate.
- **Fan-out:** two subscribers on one object both receive, and a subscriber
  that stops reading does not delay the other.
- **Teardown:** stopping the beehive closes each sender before its hub, a
  subscriber parked on the channel sees it close, and no receiver is left
  registered afterwards. `TestMain`'s leak check already fails the run if a
  hub goroutine or a stream outlives its test, so a missed teardown is
  caught even by a test that does not assert on it.
- **The poll's own tests pass unchanged.** It is still running. A test that
  needed edits means this spec changed the poll, which it must not — except
  for the gate, which has its own tests. The gate's landing did edit
  `watchpoll_test.go`, so treat the tests as of `1613685` as the baseline.
- **Gate — done:** a quiet tick reads one scalar and no listing; a tick
  after an `EventsAdd` pays the listing. In `watchpoll_test.go`.
- **No gobus type is exported:** no signature in the public API names
  `conflate`.

## Open questions

- Whether the hub should be global or per watched object. Global with a key
  filter is written above, because a per-object hub adds a lifecycle to
  manage for each subscriber. Revisit only if the enqueue-side filtering
  shows up in a profile.
- Whether the poll should later be dropped to a much longer interval rather
  than kept at 1s. That is a backstop-interval decision, so it belongs with
  the last phase of the umbrella spec, not here.
- Whether a future `EventsWatch` snapshot guarantee is worth adding, now
  that a subscriber can register before reading. Out of scope, and worth
  deciding on its own.
