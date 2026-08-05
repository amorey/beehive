# Specs

A spec is work that is designed but not built. It states the problem, the shape of
the change, and what must stay true. It is not a design record: once a spec ships,
its rationale moves to an [ADR](../adr/README.md) and the spec is deleted.

Three documents divide the space. Do not duplicate between them.

| Document | Holds |
|---|---|
| [`reconcile-triggers.md`](../reconcile-triggers.md) | what the system does today. Coverage only. No gaps. |
| [`TODO.md`](../TODO.md) | a known gap we chose not to close, and why. One entry, no plan. |
| `specs/` | a gap we intend to close, with a plan. |

An item moves from `TODO.md` to a spec when we decide to build it. The `TODO.md`
entry then shrinks to a pointer. It does not move to `reconcile-triggers.md` until
the code exists.

## The problem these specs share

Beehive is level-triggered and durable. Every route to a reconcile has a record in
the store and a driver on a timer that finds it. That property is not negotiable and
none of these specs touches it.

What the property does not supply is latency. A timer decides when work starts, so
the interval is the floor, even when the writer and the controller are in one
process and the store already knows the answer at commit time.

Today two writes beat the timer. A spec write enqueues its own object. A new
`depends_on` edge enqueues the edge's source. Both run on `Store.AfterCommit`. Every
other route waits for a tick:

| Route | Latency today | Spec |
|---|---|---|
| A target changed, so its dependents are stale | ≤1s (the waker) | [03](03-waker-commit-wake.md) |
| A delete was requested | ≤30s (the GC sweeper) | [02](02-deletion-push.md) |
| A cascade reached the next level of children | ≤30s per level | [02](02-deletion-push.md) |
| A blocked collect was unblocked | ≤30s | [04](04-finalizer-unblock-push.md) |

The stale-dependents pass is absent from that table on purpose. It runs every 60
seconds and it cannot be pushed. It answers "which dependents failed to observe a
change", and no single commit holds that answer. It stays a timer, and it stays the
backstop under everything below. See case 8 in
[`reconcile-triggers.md`](../reconcile-triggers.md).

## The rule every spec obeys

**Every push has a pull behind it.** A push lives in memory. A crash between the
commit and the dispatch discards it. So a push may only remove latency. It may never
become the sole route to a reconcile, and no spec here adds a durable record or
removes a driver.

The test for each spec is the same: disable the push and the system must still
converge, more slowly. If it does not, the spec is wrong.

## Execution order

The order is a dependency order, not a priority order.

### 1. Work-queue re-enqueue floor — **shipped**

→ [ADR](../adr/2026-08-04-work-queue-re-enqueue-floor.md)

It had to come first, because it removes latency from nothing: the timers these
specs replace are also the only rate limit on a dependency cycle. It shipped the
floor, absorbed a wake into a pending backoff alarm, and built
`internal/rategate` — the one piece of shared machinery in this set, which the
waker's scan limit and the object tail's drain limit both want.

### 2. [Deletion push](02-deletion-push.md)

Cheapest, and the most visible asymmetry in the public API: `Create` reconciles at
commit and `Delete` waits up to 30 seconds, on the same object through the same
controller. The gate the write needs is already returned, and the routing function
already exists.

### 3. [Waker commit wake](03-waker-commit-wake.md)

The largest latency win and the largest change. Dependency propagation costs one
waker tick per hop, so a chain of depth D settles in D seconds. `objectTailer`
already runs the pattern this spec copies: a commit wake in front of a floor tick.

### 4. [Finalizer unblock push](04-finalizer-unblock-push.md)

The last GC interval on the deletion path. Smallest win, and the one with a half
that stays deferred, because an edge write is invisible to every cursor in the
system.

## Out of scope

**Cross-process push.** Every push here is `workQueue`, which is in memory and
process-local. A second process writing the same store gets no pushes at all and
runs at the timer cadences. Closing that needs a notification channel between
processes, which the
[drivers ADR](../adr/2026-07-28-periodic-scan-drivers.md) places above this core
rather than in it. None of these specs improves it, and none of them makes it worse.
