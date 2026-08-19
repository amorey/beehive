# A trigger channel requeues a kind's objects, by id or by name

- **Status:** Accepted — implemented in `trigger.go`, `options.go`, `beehive.go`,
  and `Objects().GetMetaByName` in `internal/storeapi` and `sqlite`.
- **Date:** 2026-08-19

## Context

An app whose truth changes *outside* the store — a file watcher, a cloud API, a
probe — has a feed of addresses and wants each one reconciled promptly.
`Client.Requeue` is public, so the capability was already there; what was
missing was the loop around it, and every app wrote its own.

That loop is entirely about beehive rather than about the app's domain. It has
to end its subscription with the process but outlive the context that bounded
startup, stop on a closed feed rather than spinning on it, treat an address
matching no object as a no-op rather than an error, drop a failed poke rather
than retrying it, and not still be poking a kind whose reconcilers are draining.
None of that is visible in a review of the app's own code, and a notifier nobody
remembers to add to the lifecycle is silent.

## Decision

Two options declare a feed at registration:

```go
beehive.Register(bh, gk, ctl, beehive.WithTriggerByName(names)) // <-chan string
beehive.Register(bh, gk, ctl, beehive.WithTriggerByID(ids))     // <-chan ObjectID
```

Each value received is resolved within the kind and requeued. Beehive owns the
receive loop, its rate against the store, and its place in the lifecycle; the
app owns the channel and what it sends. Repeated options accumulate, so a kind
may declare several feeds, and beehive never closes one.

### It is neither a push nor a pull, and nothing recovers a lost poke

[Reconcile triggers](../reconcile-triggers.md) defines a push as a *write* that
starts the work at commit, and holds that every push has a pull behind it. A
trigger is neither: it is not a write, it uses no `AfterCommit`, and the change
it reports never entered the store, so there is no column to scan and no driver
to add.

A lost poke is therefore recovered only by whatever cadence the kind already
runs, and for a kind whose truth is external that may be no recovery at all.
That is the app's trade, and the one it already made by calling `Requeue` from a
loop of its own; the options' godoc states it rather than pretending otherwise.

It is also why the floor below coalesces instead of dropping. Every other read
loop here may drop what it is holding, because a driver behind it will find the
work again. Nothing is behind this one.

### Two options, because an address is what a producer holds

An ObjectID is allocated by the store, so an external producer cannot hold one —
a file watcher and a probe speak the app's own names. An in-process producer — a
worker pool, a cache, a feed built from beehive's own watches — holds ids and
would otherwise resolve back to a name it does not use. Both resolve through one
metadata-only statement, so neither is the cheap path.

### The resolution is one statement, and it is kind-scoped

A trigger needs existence and kind and nothing else, so it must not route
through `Client.Requeue` or `Objects().Get`/`GetByName`, each of which attaches
conditions in a second round trip. `Objects().GetMeta` was already the read the
id form wants; `Objects().GetMetaByName` was added for the name form, over a
statement `sqlite` already had.

The kind scoping is load-bearing rather than tidy. `Objects().GetForReconcile`
takes a bare id with no `GroupKind`, and every *other* enqueue path is
kind-routed by construction, so nothing before this could hand a reconciler a
foreign id. A trigger is the first place an app hands beehive a raw address, so
an ungated id would let one kind's controller decode another kind's row. The
name form takes its kind from the `WHERE`; the id form compares after the read.

A poke for an id collected between resolution and dispatch needs no further
guard: the pass reports `gone` and the worker forgets it.

### The receive never reads the store

The store runs on one connection, shared with every writer, and a trigger is the
first read loop whose rate is set outside this process. So the loop accumulates
addresses into a set and a floored timer drains it
(`internal/rategate`, 100ms, the same floor the object tailer uses). The first
drain after an idle period is eager, so a quiet feed pays no added latency.

The set is a set: a hot feed on one address costs one read per window, and a
burst across N addresses costs N reads in one drain rather than N drains. That
is coalescing, not buffering — it is bounded by distinct addresses in flight,
never by poke count.

It also answers backpressure. A send blocks only until the receive goroutine
takes it, never behind a store read, so the connection cannot hold up a
producer. A producer that must not block even that far owns its own buffer or
its own drop; beehive cannot drop on its behalf without inventing a buffer size
it has no basis to pick.

### Shutdown needed no new phase

The requirement is that a poke must not arrive at a kind whose reconcilers are
draining. It already holds, and the guarantee is worth stating exactly because
the obvious reading of it is wrong:

- `stop` cancels `runCtx`, which every reconcile worker selects on, so the
  workers return without draining the queue.
- A trigger's resolve runs on the same `runCtx`. Once cancelled it fails with
  the context error and takes the ordinary dropped-poke path.
- `workQueue.stop`, which makes `addLocked` a no-op, runs *later* still — in
  `reconciler.run`'s deferred block, after its workers are drained. A poke
  landing between the cancel and that defer is accepted by a live queue. It is
  harmless because no worker is left to dispatch it, but it is **not** stopped
  by the `stopped` gate, and a guard built on that belief would guard nothing.

## Consequences

The backoff ladder is preserved, matching a plain `Client.Requeue`: a poke from
an external feed is no evidence that a failing reconcile's cause is gone. There
is no per-poke option slot on a channel and no registration-level override — a
feed that knows a failure is over is asserting something about the reconcile
rather than about the address, and can say so with
`Requeue(id, WithResetBackoff())`.

A channel serves one kind. Two `Register` calls sharing one channel race the
receive, and each value lands on an arbitrary one of them.

A nil channel is `ErrInvalidOption`, checked before the target switch: it blocks
forever, so accepting it would register a trigger that silently never fires.
Passed to `New` the options are ignored, as any option is at a target it does
not recognise — pinned by a test, since these have no other meaningful target.

Not taken: a valueless `<-chan struct{}`, which can only dispatch the whole kind
and is N dispatches per event for a kind whose objects change independently; a
channel carrying state, since a snapshot handed through a queue was written from
a state that has already moved on; and caching a name's resolution across
windows, where a collect-and-recreate inside the window would send the poke to a
dead id, with nothing behind it to repair the loss.
