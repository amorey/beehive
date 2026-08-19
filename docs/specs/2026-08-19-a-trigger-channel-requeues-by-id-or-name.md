# A trigger channel requeues a kind's objects, by id or by name

- **Status:** Planned
- **Date:** 2026-08-19
- **Issue:** [#127](https://github.com/amorey/beehive/issues/127)

## Decision

Two registration options let an app hand beehive a channel of addresses to
requeue:

```go
beehive.Register(bh, ClusterIdentityGroupKind, identity,
    beehive.WithTriggerByName(names))   // names is <-chan string
beehive.Register(bh, WorkerGroupKind, worker,
    beehive.WithTriggerByID(ids))       // ids is <-chan beehive.ObjectID
```

Each value received is resolved within the kind and requeued, as
`Client.Requeue` would. Beehive owns the receive loop, its rate against the
store, and its place in the lifecycle; the app owns the channel and what it
sends.

This is not new capability — `Client.Requeue` is public and an app can drive it
from a loop of its own. What the options remove is the loop, which is entirely
about beehive rather than about the app's domain, and which is silent when it is
forgotten. Same argument `WithIndividualPassInterval` won.

### It is neither a push nor a pull, and nothing recovers a lost poke

[Reconcile triggers](../reconcile-triggers.md) defines a push as a *write* that
starts the work at commit time, and states that every push has a pull behind it.
A trigger is neither. It is not a write, it uses no `AfterCommit`, and the change
it reports happened outside the store — a file, a cloud API, a probe — so there
is no column to scan and no driver to add.

A dropped poke is therefore recovered only by whatever cadence the kind already
runs, and for a kind whose truth is external that may be no recovery at all.
That is the app's trade to make, and the trade it already makes by calling
`Requeue` from a loop of its own. Beehive states it rather than fixing it: the
options' godoc says a poke is a latency hint whose correctness rests on the
kind's own cadence.

### Why two options rather than one

An ObjectID is allocated by the store, so an external producer cannot hold one:
a file watcher, a cloud API and a probe all speak the app's own names. An
in-process producer — a worker pool, a cache, a feed built from beehive's own
watches — holds ids and would otherwise resolve back to a name it does not use.

Both resolve through one metadata-only statement (below), so neither is the
cheap path; they differ only in which address the producer already has.

## Surface

```go
// WithTriggerByName requeues the object holding each name received on ch. ...
func WithTriggerByName(ch <-chan string) Option

// WithTriggerByID requeues each id received on ch. ...
func WithTriggerByID(ch <-chan ObjectID) Option
```

Both are `Option`, dispatching on `*reconciler` only, and both are meaningful
only at `Register` — a trigger without a kind has nothing to resolve within.
Passed to `New` they are ignored, as every option is at a target it does not
recognise. That is the convention and the spec keeps it, but because these
options have no other meaningful target, the no-op is pinned by a test rather
than left incidental.

A nil channel is rejected with `ErrInvalidOption`, checked before the target
switch. A nil channel blocks forever, so accepting it would register a trigger
that silently never fires — the failure the option exists to kill.

**Repeated options accumulate.** One kind may be driven by several feeds (the
motivating app drives two), and last-wins here would re-create inside beehive
the "notifier nobody remembers to wire up" failure the option exists to kill.
Each channel gets its own goroutine; there is no ordering between them.

**A channel serves one kind.** Handing the same channel to two `Register` calls
makes two goroutines race the receive, and each value lands on an arbitrary one
of the two kinds. Say so in the godoc; it is a plausible mistake once repeated
options accumulate.

## Semantics

| Case | Behaviour |
| --- | --- |
| Name resolves to an object | Requeue it |
| Name resolves to nothing | No-op, logged at debug. Whether a record exists for an address is the app's business and changes under it |
| Empty name | No-op by the same branch. `Objects().GetByName` matches no row; unlike `Client.GetByName` there is no `checkName` in front, so `""` is not `ErrInvalidName` here |
| Id does not exist, or belongs to another kind | No-op, logged at debug |
| Store read fails | Logged at warn, poke dropped. Not retried: a poke is a latency hint, and a retry ladder here would compete with the kind's own |
| Channel closed | Stop servicing that channel and return. A `select` over a closed channel that is not handled is a hot loop |
| Beehive stopping | The receive loop's context is cancelled; see lifecycle |

**Beehive never closes the channel.** The app owns the subscription — in the
motivating app one producer's `Close` ends every subscription in the process, so
ownership has to sit with the app.

**The backoff ladder is preserved**, matching plain `Client.Requeue`: a poke
from an external feed is no evidence that a failing reconcile's cause is gone.
There is no per-poke option slot on a channel, and no registration-level
"trigger resets backoff" is offered — a feed that knows a failure is over is
asserting something about the reconcile rather than about the address, and can
say so with `Requeue(id, WithResetBackoff())`.

## Cost against the store

The store runs on one connection (`SetMaxOpenConns(1)`), which every read shares
with every writer. A trigger is the first read loop driven by a producer outside
this process, so both halves of its cost are the spec's business.

### One statement per resolution, not two

A trigger needs existence and kind. It does not need conditions, which
`Objects().Get` and `Objects().GetByName` attach in a second round trip
(`attachConditions`).

- **By id:** `Objects().GetMeta` — already exactly this read, and its
  `RawObject` carries `Group`/`Kind`, so the kind gate is a comparison in Go
  rather than a query.
- **By name:** add `Objects().GetMetaByName(ctx, gk, name)`. `sqliteStore`
  already has the statement (`getObjectRowByName`, one `SELECT`, served by the
  `UNIQUE ("group", kind, name)` index); it is unexported and has no
  `storeapi` member. The addition is that member and its `fakeStore` stub.

Neither variant may route through `Client.Requeue` or `scopedGet`, which would
pay the conditions query for data no trigger reads.

### The read rate is floored and coalesced

Every other read loop here is throttled to keep the connection available to
writers: the object tailer floors each drain at 100ms, and the waker floors both
its wake and its cursor write. A trigger has no such bound by construction — the
producer sets the rate.

Floor it the way the tailer is floored, with the same shape: **the receive loop
never reads the store.** It accumulates addresses into a set and a floored timer
drains the set, resolving each distinct address once per window
(`internal/rategate`, 100ms, unexported like the other floors). The first drain
after an idle period is eager, so a quiet feed pays no added latency.

This is coalescing, not buffering: the set is bounded by *distinct addresses in
flight*, never by poke count, and nothing is dropped — a hot feed on one address
collapses to one read per window, and a burst across N addresses costs N reads
in one drain rather than N drains.

It also settles backpressure. **Beehive does not drop and does not buffer**: a
send blocks only until the receive goroutine takes it, never behind a store
read, so a producer is never held up by the connection. A producer that must not
block even that far owns its own buffer or its own drop — beehive cannot drop on
its behalf without inventing a buffer size, and has no basis to pick one.

Do **not** justify the floor by the work queue's own absorption. `internal/rategate`
in `workQueue` absorbs the *dispatch*, which is the cheap half; without a floor
here the poke still pays a store read for a dispatch that then gets swallowed.

### Resolution is kind-scoped, and this is load-bearing

`Objects().GetForReconcile` — the reconcile loop's opening read — takes an id
and no `GroupKind`. Every existing enqueue path is kind-routed by construction,
so nothing today can hand a reconciler a foreign id. A trigger channel is the
first place an app hands beehive raw addresses, so it is the first that can.

Both variants therefore resolve kind-scoped before queueing anything, and an
unscoped id trigger would let one kind's controller decode another kind's row.

A poke for an id collected between resolution and dispatch needs no further
guard: `typedController.reconcile` reports `gone`, and the worker forgets the id
rather than scheduling anything.

## Lifecycle

Each trigger goroutine is counted in `bh.wg` and runs under `runCtx`, launched
in `Start` beside the reconcile loops. It ends on whichever comes first: the
channel closing, or `runCtx` being cancelled by `stop`.

**No separate pre-drain phase is needed.** The issue asks that the trigger stop
being serviced before the reconcilers drain; what actually holds is this, and an
implementer should build to it rather than to the queue's `stopped` flag:

- `stop` cancels `runCtx`, which every reconcile worker selects on, so the
  workers return without draining the queue.
- The trigger's own resolve is a store read under the same `runCtx`. Once
  cancelled it fails with the context error and takes the drop path above, so
  nothing new is queued after the cancel except by a read already in flight.
- `workQueue.stop` — which makes `addLocked` a no-op — runs later still, in
  `reconciler.run`'s deferred block after its workers are drained. A poke landing
  between the cancel and that defer is accepted by a live queue. It is harmless,
  because no worker is left to dispatch it, but it is *not* dropped by the
  `stopped` gate and a guard built on that belief would be guarding nothing.

A `Beehive` that is never started never receives: the goroutines launch in
`Start`. Sends before `Start` block, or fill the app's buffer, exactly as they
would against a loop the app wrote.

## Implementation

A new `trigger.go` holds the receive loop, the coalescing set and the floor,
non-generic like every other internal: the option closes over a channel of a
concrete type and the reconciler stores a slice of run funcs. `reconciler` gains
one field for them, set by the options in `Register`; `Start` launches them.

No `internal/driver` shape fits — there is no cadence and no wake, just a
receive, a floor and a resolve. Do not force it into `driver.Run`.

## Tests

In `trigger_test.go`, mirroring `trigger.go`, and `options_test.go` for
validation. Synchronise on signals, never on sleeps.

- A name sent on the channel requeues the object holding it; an id likewise.
- An unknown name, an empty name and an unknown id are all no-ops, and none ends
  the loop.
- An id of another registered kind is a no-op — the foreign-id guard.
- Two pokes for one address inside one window cost one store read; two pokes for
  different addresses in one window cost two, in one drain.
- The first poke after an idle period is not delayed by the floor.
- Two `WithTriggerByName` options on one `Register` both fire.
- A closed channel ends that loop and leaves the kind's other trigger, and its
  periodic passes, running.
- A poke that arrives after `stop` has begun does not resurrect a dispatch, and
  does not delay the drain past the stop context.
- A trigger requeue preserves an object's backoff ladder.
- `WithTriggerByName(nil)` and `WithTriggerByID(nil)` return `ErrInvalidOption`,
  including at a target the option does not recognise (checked before the switch).
- `WithTriggerByName(ch)("unrelated")` is a no-op, pinning the ignored-target
  convention for an option that has no other meaningful target.
- A store read failure is logged and dropped, and the loop keeps serving.

## Documentation on shipping

Fold this into an ADR, add the options to `README.md` and to the option list in
`CLAUDE.md`, and delete this file. Two documents need more than a mention:

**[Reconcile triggers](../reconcile-triggers.md)** is the blocking one. It
claims to list *every* way an object comes to owe a reconcile, and puts "every
push has a pull behind it" at the centre of the design. Resolve it as follows,
which is narrower than it first looks:

- The count at "exactly nine push paths" **stands**. That sentence scopes itself
  to paths using `Store.AfterCommit`, and by the document's own definition a
  push is a write committing. A trigger is neither.
- Amend the invariant to say every push *from a write* has a pull behind it, and
  add a short paragraph to section 1 naming the third thing: an address handed
  in from outside the process, which makes an object *dispatched* without ever
  making it *owed*.
- Add it to **section D (in-memory only)** as case 17, not to section E. E is
  "each item below looks like a trigger; none of them is one", and a trigger
  channel genuinely is one. D is the right home by precedent: case 13, a
  `RequeueAfter` chain on a settled object, already sits there with no durable
  record and no driver behind it. Case 17's restart line is stronger than D's
  preamble, though — D says an object is recovered if it also carries a durable
  record, and here the change is not in the store at all, so nothing re-derives
  it. Update D's "cases 13 through 16" accordingly.

**[The drivers ADR](../adr/2026-07-28-periodic-scan-drivers.md)** gets one
sentence: a trigger channel is not a driver, scans nothing, and has no cadence,
so it is absent from the table deliberately.

## Not in scope

- **A valueless `<-chan struct{}`.** It can only dispatch the whole kind, which
  for a kind whose objects change independently is N dispatches per event.
- **A channel carrying state.** A poke carries an address at most: the pass
  reads current state for itself, because a snapshot handed through a queue was
  written from a state that has already moved on.
- **Caching a name's resolution across windows.** It would drop the by-name read
  to nearly nothing, and a collect-and-recreate inside the window would send the
  poke to the dead id — a loss with no pull behind it to repair it. The
  per-window coalescing above is the bounded form of the same idea.
- **Scoping `Objects().GetForReconcile` by kind.** It would tighten every
  enqueue path, not just this one, but it is a store change with its own reach
  and the trigger does not need it.
