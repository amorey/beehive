# A commit wakes the dependency waker

- **Status:** Accepted — implemented in `waker.go`, `objectswatch.go`,
  `watchhub.go`, `internal/rategate/rategate.go`, `internal/driver/driver.go`.
- **Date:** 2026-08-05

## Context

Case 6 in [`reconcile-triggers.md`](../reconcile-triggers.md) — an ordinary
change to a target waking its dependents — had no push. `waker.scan` was the
only route from "target committed" to "dependents enqueued", and it ran on a 1s
tick.

The cost was **one tick per hop**. A chain of depth D settled in about D seconds
no matter how fast the store committed, because each link waited for the next
tick to notice the one before it. That was the largest latency gap in the
system, on the path most controllers use.

`objectTailer` already ran the pattern: a commit wake in front, a floor tick
behind.

## Decision

**Wake the waker when a write commits, and keep the tick as the floor.**

The signal is not the object's id and not its kind. It is "something was
written": the waker holds one store-wide cursor and reads `object_writes` to
learn what moved.

### The subscription is across every key of the hub that already existed

`kindWriteHub` did not move. A tailer keeps `Watch(gk)`; the waker adds
`WatchAcross()`, which gobus v0.6.0 added for this. `signalKindWritten` is
untouched, so there is one hub, one `Send`, and no new call site.

The rejected alternative was moving the hub to `gobus/conflate`, where a
receiver sees every key and narrows with a filter. It loses on four counts:
`conflate.sendLocked` iterates *every* receiver and evaluates each one's
predicate under the hub lock, where `watch` routes through a key index; a
conflate receiver holds a slot per key, so a burst across N kinds is N wakes
rather than one; its "no replay" is weaker than watch's "registration is the
snapshot"; and it would have rewritten the tailer's wake path under a shipped
ADR. `conflate` stays where it belongs — the tailer's own fan-out, which needs
per-key queues, a merge with annihilation, and `TryRecvAll`'s batch cut.

**One cost is real and unavoidable:** the waker subscribes for the life of the
process, so the hub is never idle and `Send`'s lock-free empty-hub path is never
taken. A beehive with no watches now pays a hub mutex and one offer per commit.
That is the price of an unconditional subscriber, not of the bus.

### The floor tick stays, so this is a driver with a wake in front

`wakeInterval` still bounds the worst case, a non-positive interval still turns
the waker off — wake included — and the stale-dependents pass is still the
guarantee underneath. A wake is an optimisation twice over: the waker is already
an optimisation over that pass, so a lost wake costs latency against a lost
tick, which costs latency against a lost sweep.

The closed-hub arm in `run` is a safety net rather than the normal exit: `stop`
cancels `runCtx` and waits on the WaitGroup the waker is in, closing the hub
only after, so `ctx.Done()` wins except when the drain hits its deadline.

### Two rate limits, both `internal/rategate`

**Scans are floored at `wakeScanMinInterval` (100ms).** Removing the tick as the
only pacing would otherwise let a sustained write stream hold the connection at
the full paging budget. `rategate` is eager after a quiet period, so an
idle-to-active transition pays no added latency and only a sustained stream is
paced. The value is a constant rather than a fraction of `wakeInterval`:
deriving it would tie wake latency to a floor we want free to raise.

**The cursor write is floored at `wakeInterval`.** `scan` persists on the way
out whenever the watermark moved, which under a sustained stream is every pass —
so a 10×-faster loop would have meant a 10×-faster `DriverCursorsSet`. Every
other cost here is a read competing for connection time; this one is a bare
write competing for the write lock, which is what the commits themselves need.
Gating it keeps the cursor write rate exactly what it was. The cursor is an
optimisation over the stale-dependents pass, `persist` writes the current
watermark rather than a delta, and a crash with a ≤1s-stale cursor replays wakes
that are idempotent.

**The gate sits ahead of the retry skip ladder, not just ahead of the write.**
`persistSkips` decrements per persist call, so a gate below it would let refused
passes burn skips at the wake rate and `wakePersistRetryCap` would collapse from
a minute to a few seconds — the backoff shrinking during exactly the outage it
exists for.

### `wakeScanPagesPerPass` drops from 16 to 4

`BenchmarkWakerScanRateUnderSustainedWrites` measures a pass at each of the
three shapes a wake finds. A quiet pass — what most wakes find — costs ~31µs and
one read, so 10 passes a second is 0.03% of the connection. A full-budget pass
cost ~13ms at sixteen pages and ~3ms at four.

Ten passes a second makes that hold the share of the connection a writer waits
behind, so four pages trades peak hold for the same work spread finer: a resume
drains at 40 pages/s against the old 16, holding the connection for ~3ms at a
time rather than ~13ms.

| resume | before (16 pages, 1/s) | 16 pages, 10/s | shipped (4 pages, 10/s) |
|---|---|---|---|
| connection hold per pass | 12.8ms | 12.8ms | 3.2ms |
| duty cycle | 1.3% | 12.8% | 3.2% |
| drain rate | 16 pages/s | 160 pages/s | 40 pages/s |

### A resumed seed reports a backlog

`seed` already reads `ObjectWritesMaxVersionAll`, so it knows whether the cursor
it resumed from sits below the mark. Reporting `scanMore` there costs nothing
and keeps a restart with a backlog from waiting a floor for its first page.

## Consequences

- A dependent is enqueued when its target's write commits. A chain of depth D
  propagates in D commits, bounded below by the work queue's re-enqueue floor
  rather than by the waker.
- `defaultMinRequeueInterval` is what bounds a dependency cycle now that the
  tick does not. It was set to `defaultWakeInterval` for this change; **raising
  either constant needs this ADR re-read first.**
- The waker has one clock, `waker.now`, so the rate tests drive it by hand
  rather than sleeping. `run` reads it and hands `pass` an explicit instant;
  `persist` reads it directly, because threading one through `scan` would have
  changed its signature at 34 test call sites for one deep consumer.
- Gates are built on first use, never in `New`: `New` constructs the waker
  before it applies options, so anything built there captures the defaults and
  silently ignores the option.
- `internal/rategate` gained `Allow`, and `internal/driver` gained `Rearm`, both
  with their consumers. `Rearm` also replaced the tailer's inlined
  `Stop`-then-`Reset`.
