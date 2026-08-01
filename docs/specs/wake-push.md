# Spec: push the dependency wakes, beside the waker that stays

Status: draft — the hub and the dependent wakes. Two pieces have left this
spec because neither needs the hub: the self-enqueue landed in `60b4ea4`,
and the new-edge stamp moved to [new-edge-push](new-edge-push.md). Phase 2
of [push-conversion](push-conversion.md).
Date: 2026-07-31
Scope: a new hub in the beehive layer and the write paths that notify it.
No new dependency: `gobus` arrived with the schedule watch.
Related: the [drivers ADR](../adr/2026-07-28-periodic-scan-drivers.md)
stated that beehive's own machinery registers nothing on `AfterCommit`.
The self-enqueue already changed that, and its
[ADR](../adr/2026-07-31-a-spec-write-enqueues-its-own-object.md) records
the narrowing.

## Goal

A commit that moves an object must wake the dependents of that object at
once, and not at the next 1-second tick.

A commit that changes a spec must enqueue that object at once too. **That
half is done.** It landed on its own, through `Store.AfterCommit` and not
through the hub, because it needed neither the hub nor a new dependency.
See "Enqueue the object itself" below for what shipped and what it means
for the hub.

**This spec adds the push path and deletes nothing.** The waker keeps
scanning beside it, permanently. See "Why the waker stays" below for why
push does not replace it.

## Non-goals

- Do not change the stale-dependents pass. It stays the correctness
  backstop, on its current cadence. The umbrella spec's last phase changes
  the cadence, not this spec.
- Do not change `dependentsWake`'s routing. Each dependent is enqueued by
  the dependent's own kind, as today. Enqueueing the *written* object was
  work beside that, not a change to it, and it has already landed on its
  own path.
- Do not delete the waker, its cursor, `DriverCursorer` or
  `driver_cursors`. Push does not replace the waker; it covers a narrower
  set of writes, faster. See "Why the waker stays".
- Do not fence writes. See the umbrella spec's loud-failure policy.

## Background

The waker today scans `ObjectWritesListSince` from a watermark, on a
1-second tick, in pages. It seeds the watermark at startup, and it persists
the watermark through `DriverCursorer` into the `driver_cursors` table. The
waker is an optimisation over the stale-dependents pass: a lost wake costs
latency until that pass, and never divergence. Push keeps exactly that
contract and removes the scan.

## The hub

Add a small in-process hub to the beehive layer, built on
`gobus/conflate` (see the umbrella spec's messaging-backend section). The
hub is a `conflate.Hub[ObjectID, wakeOf]` with one receiver.

A hub has two sides. A committing write puts an object id in. Something
must take the ids out and act on them. That something is one goroutine,
called the **drain**, because it empties the hub. It does not take one id
at a time. It takes the whole backlog as one batch, so one store call
serves many ids.

This hub is the only one with a drain. The hubs in
[events-push](events-push.md) and [watch-push](watch-push.md) deliver to
subscribers instead: each subscriber owns a receiver, and `gobus` runs the
goroutine that feeds its channel.

The hub is declared like this:

```go
type wakeOf struct {
    GK GroupKind // routing; fixed at insert, and an id is never reused
}

hub := conflate.New[ObjectID, wakeOf](func(prev, next wakeOf) (wakeOf, bool) {
    return next, true
})
```

- **The key is the object id, and the value carries no state.** The wake
  says nothing about what the object now is; the drain re-reads the store,
  which is the level-triggered rule the whole system runs on. The one field
  is routing, not payload. An object's group and kind are fixed at insert
  and its id is never reused, so `GK` cannot go stale, and carrying it
  spares the drain a lookup. A burst of writes to one object still costs
  one wake.
- **The merge is newest-wins, and it makes no decision.** Earlier drafts
  carried a `Self bool` on the value and ORed it here, so that a spec write
  followed by a status write still enqueued the object. That flag is gone:
  the self-enqueue landed outside the hub, on `Store.AfterCommit` at each
  spec-write site, so no obligation rides through a slot and no merge can
  drop one. See "Enqueue the object itself" below.
- **Notify:** `hub.Sender().Send(id, wakeOf{...})`. `Send` never blocks and
  never applies backpressure, so a slow drain cannot stall a commit. The
  sender is safe to share across goroutines, which a commit hook needs. The
  memory is bounded by the number of objects that changed since the last
  drain, and no notify is dropped.
- **Drain:** one goroutine, holding the single receiver. It calls
  `RecvContext(ctx)` for the first id, then `TryRecv` until `ErrEmpty` to
  take the rest of the backlog as one batch. It resolves the dependents of
  every id in that batch in one `EdgesGroupIncomingByID` call, as the
  waker's page does today, and enqueues each through the same wake path
  (`dependentsWake`'s body, without the paging). The written object's own
  pass is not the drain's work: it was scheduled at commit time, before the
  drain saw anything, so a failed dependent resolution cannot cost an
  object its own reconcile.
- **On a store error:** send the failed ids back through the sender, log a
  warning, and wait a short backoff before the next receive. The ids are
  pending again, and a fresh write to the same id coalesces into the retry
  rather than adding to it. This is the same discipline as the waker's held
  watermark. The backoff is required, not optional: re-sent ids are
  immediately receivable, so a drain that retried at once would loop without
  pause against a store that keeps failing.
- **On a panic in the drain goroutine:** crash the process. Do not recover
  and continue, because a dead drain with a healthy process is the silent
  failure. The restart runs the startup recovery, and the stale-dependents
  pass covers the gap. `conflate` does not supervise the drain, so this
  stays beehive's rule.
- **Shutdown:** the hub stops with the beehive's ctx, as the drivers do
  today. Call `Sender.Close()`, not `Hub.Close()`: a sender close is a
  graceful end of stream, so the drain reads its backlog and then gets
  `ErrClosed`, while `Hub.Close` abandons the backlog. The library also
  states that `Hub.Close` must not run at the same time as a `Send`, so
  beehive must stop accepting commits before it tears the hub down. The
  drivers ADR names this ordering as a constraint on any push path; this is
  where beehive answers it. The store still owns no goroutines.
- **Close precedence:** `RecvContext` ranks closed above cancelled above
  value, so a drain that cancels its own ctx still reaches `ErrClosed`
  rather than spinning. Do not add a second stop signal.

## The notify sites

Register the notify through `Store.AfterCommit`, one hook for each write,
carrying the object id. `AfterCommit` is the correct hook because it
already has the two properties the notify needs: a rollback, including a
savepoint unwind inside a nested `Within`, discards the hook, and a write
outside a transaction runs the hook at once. A notify in the mutator
wrapper, after the call returns, does not have the first property: an
outer `Within` that rolls back would leave a notify for a write that never
happened. A false wake is harmless — the dependent reconciles against
current state and settles — but the hook makes the clean rule possible:
notify exactly when the commit is real.

Notify on each write that moves an `objects` row's `resource_version`.
That set includes the spec writes, `UpdateStatus`, `ObjectsCreate` and the
deletion requests — `markForDeletion` bumps the version, and the tombstone
wake matters: it is the strand case the waker's ADR names, where a
deletion-pending target waits for its dependents to drop their edges.

## Enqueue the object itself, not only its dependents — landed

Shipped in `60b4ea4`, ahead of the hub and without it. The
[ADR](../adr/2026-07-31-a-spec-write-enqueues-its-own-object.md) holds the
decision; this section records what changed against the draft below.

The waker wakes an object's *dependents*. Nothing woke the object that was
written, because nothing had ever needed to: a spec write bumps the
generation, and the owed pass listed it on the next tick. That made the
owed pass the **primary** trigger for an ordinary spec write, not a
backstop for one — which is why `CLAUDE.md` still tells the examples to
call `Client.Requeue` after a create.

The umbrella spec's last phase lengthens the owed pass to five minutes. If
nothing pushed an object's own reconcile, that phase would make the most
common latency in the system ten times worse. That is now closed.

**`Client.Create`, `GetOrCreate`'s created branch and both spec updates
enqueue their own object through `Store.AfterCommit`.** One helper,
`signalSpecWritten` in `client.go`, holds the hook. `AfterCommit` gives it
the property the enqueue needs, and the same one the notify sites above
need: a rollback discards it, and so does a savepoint unwind inside a
nested `Within`. The owed pass is unchanged and stays the backstop.

### It did not go through the hub, and it did not need to

The draft below put the self-enqueue on the drain, behind a `Self` flag on
the hub's value. The landing does not, for two reasons.

**It needs no store read.** The drain batches ids so that one
`EdgesGroupIncomingByID` call serves many. A self-enqueue reads nothing, so
batching buys it nothing, and routing it through a queue only adds a place
for it to wait.

**It needed no hub at all.** The self-enqueue was the part of this spec
that priced phase 5, so holding it behind machinery it did not use would
have held the whole plan behind one landing.

The hub keeps the dependent wakes, which do read the store and do batch.
The `Self` flag and the ORing merge are gone from the design above.

### The gate is "this write changed the object", not "the generation moved"

The draft said to self-enqueue only on a write that bumps `generation`.
The landing says the same thing in the store's words rather than the
caller's: both `ObjectsUpdateSpec*` mutators now return a `changed` bool,
true only when the spec was written, and the helper reads that. A create
always changes the object, so it always enqueues.

The two phrasings pick the same writes. The `changed` bool is better
because the caller cannot claim it. A byte-identical `Update` sets it
false, and the store's skip of that write is what stops a controller
re-applying its own spec from waking itself forever.

**Gating on the row being unsettled is the defect to avoid, and it shipped
once before it was fixed.** A failing reconcile never settles the row, so
every no-op write that controller makes would pass such a gate.
`requeueNow` cancels the backoff alarm and marks an in-flight id dirty, so
the object would retry at full speed forever and never reach its ladder.
Pinned by `TestFailingRespecControllerKeepsItsBackoff`. "Unsettled" is what
the object owes; "changed" is what this write did.

### Why a self-enqueue cannot loop

A reconcile ends in `UpdateStatus`, which moves `resource_version`. If a
self-enqueue fired on every version move, the object would schedule its own
next reconcile, forever, on every object in the system. This is the same
defect `docs/TODO.md` records for dependency cycles, and it is why
`dependentsWake` already skips `from_id == to_id`.

The spec gate is what makes it safe, because **no controller write changes
a spec.** `UpdateStatus`, the condition mutators and `FinalizersDelete` all
leave the spec and the generation alone. Only a spec write moves them, and
a spec write comes from the user through `Client`. So the loop cannot
close.

**Dependent wakes still fire on every write that moves
`resource_version`.** That set is unchanged, and it is not the self-enqueue
set. Keep the two apart when the hub lands.

### Deletion is already covered

A deletion request changes no spec, and it does not need to. The GC sweeper
lists `deletion_requested_at` on a cadence the umbrella spec's last phase
leaves at 30s. So a delete keeps the latency it has, and self-enqueues
nothing. Do not add it because it looks symmetric.

### The new-edge stamp — moved out

`EdgesAdd` stamps `reconcile_owed` for every new `depends_on` edge, drained
by the owed pass — the other listing the last phase would slow. It does not
yet enqueue the source object, so a fresh declare still waits for a tick.

It gets the same treatment the spec writes got: an `AfterCommit` hook at
the site, not through the hub. It reads nothing, so the drain buys it
nothing, and it needs no `gobus`. So it left this spec for the same reason
the self-enqueue did, and it now has its own:
**[new-edge-push](new-edge-push.md)**, which holds the routing, the gate
and the test plan.

What that leaves here is the hub and the dependent wakes — the parts that
do read the store and do batch.

## Why the waker stays

Two wake paths run at once, permanently. This is not a migration step.

**Push is faster than the waker but covers less.** The notify is registered
in the beehive layer, above the store. So push sees a write only if that
write went through this process's `Client` or `ControllerClient`. The waker
scans the store, so it sees every write, whatever made it. Two cases where
that difference is real, and neither is a bug:

- The embedder holds the `Store` they constructed and passed to `New`.
  Nothing stops them writing through it directly.
- A second process opens the same database. Its writes never reach this
  process's hub.

The drivers ADR already names the second case: it lists "a process with no
waker" among the ways a wake is lost. Push is per-process by construction,
and no amount of care in the notify sites changes that.

**A duplicate wake is harmless.** Both paths call the same `dependentsWake`
body, which enqueues a dependent by its own kind. A dependent enqueued
twice reconciles once more against current state and settles. That is the
level-triggered contract doing what it exists for.

**The waker is also the cheapest driver in the system.**
`ObjectWritesListSince` reads an index range above the cursor. When nothing
has changed the range is empty and the tick costs one seek. Its cost is
bounded by what changed, which no other backstop's is: the stale-dependents
pass costs about 190 ms at 250,000 edges, and pays it whether or not
anything is stale.

So the three mechanisms layer, each with a different bound:

| Mechanism | Latency | Covers | Cost bounded by |
| --- | --- | --- | --- |
| Push | almost none | writes through this process | what changed |
| Waker | its interval | every write to the store | what changed |
| Stale-dependents pass | its interval | everything, including a broken waker | the dependency graph |

Deleting the waker would leave a five-minute gap after any missed notify,
covered only by the most expensive of the three. Keep it, and leave it on
production defaults.

## Test plan

Write whitebox tests in `package beehive`. Synchronize on channels and
fakes, and never on sleeps.

- **Immediate wake:** a write to a target enqueues its dependent without
  any tick. Wait on the enqueue signal, not on time.
- **Rollback discards the notify:** a write inside a `Within` that returns
  an error wakes nothing.
- **Coalescing:** N writes to one target before the drain runs cause one
  `EdgesGroupIncomingByID` call with that one id. Hold the drain on a
  channel the test closes, rather than on a sleep: `Send` never blocks, so
  the N writes land in the one slot while the drain waits.
- **No gobus type is exported:** the hub stays unexported, and no signature
  in the public API names `conflate`.
- **Error retry:** a failing `EdgesGroupIncomingByID` keeps the ids
  pending; the next drain resolves them. Use a store double that fails
  once.
- **Tombstone wake:** a deletion request on a target wakes the dependent,
  which can then drop its edge. This is the strand case.
- **Self-enqueue on a spec write — done:** an `Update` enqueues that object
  without any tick, with no dependents involved at all. In `client_test.go`.
- **No self-enqueue on a controller write — done:** `UpdateStatus`, a
  condition write and `FinalizersDelete` each enqueue nothing. Asserted
  directly rather than by absence of a loop — a test that merely finishes
  proves nothing about a cycle that is slow.
- **A byte-identical write enqueues nothing — done:** and a failing
  controller that re-applies its own spec keeps its backoff ladder
  (`TestFailingRespecControllerKeepsItsBackoff`).
- **A spec write and a status write in one transaction — done:** the object
  is still enqueued, because the signal is read as the write left it rather
  than as the transaction committed
  (`TestSpecThenStatusInOneTransactionStillEnqueues`). This replaces the
  merge test the `Self` flag needed, which no longer has a flag to test.
- **A new edge enqueues its source:** moved to
  [new-edge-push](new-edge-push.md) with the rest of that work.
- **Client-only kinds:** a write to a kind with no registered reconciler
  enqueues nothing and does not error.
- **Backstop unchanged:** the stale-dependents tests pass without change.
- **The waker's own tests pass unchanged.** It is still running, so a test
  that needed edits means this spec changed the scan, which it must not.
- **The drain ends with the beehive.** `TestMain`'s leak check fails the
  run if it does not, so a drain that misses its stop signal is caught even
  by a test that asserts nothing about teardown.
- **Push works with the waker disabled.** Drive the wake tests with the
  waker's interval turned off through `withDependencyWakeInterval`, so
  every wake they observe is provably the hub's. Without this the tests
  cannot tell which mechanism did the work.

## Open questions

- Where the notify registration lives: in each mutator wrapper in the
  beehive layer, or in one shared helper that the wrappers call. The
  self-enqueue answered this for its own sites — `signalSpecWritten` is one
  shared helper, and `signalCreated` wraps it for the create path — so a
  new mutator cannot forget the hook without also skipping the helper.
  [new-edge-push](new-edge-push.md) generalises that helper to take a
  `GroupKind`, since its site is cross-kind. Use it for the wake notify
  too, once it exists.

One question is closed by the landing above:

- Whether the drain should skip a self-enqueue for an object the reconciler
  has already observed. The drain no longer carries self-enqueues, so there
  is nothing to skip.

## Docs to update when this lands

- `docs/reconcile-triggers.md`: the dependency-wake row gains the commit
  hook as a recording site and the hub as a driver. The scan stays, so add
  a row rather than replacing one. The self-enqueue already added its own
  row there.
- The [drivers ADR](../adr/2026-07-28-periodic-scan-drivers.md) statement
  that beehive's own machinery registers nothing on `AfterCommit`. The
  self-enqueue already narrowed it; the hub narrows what is left.
