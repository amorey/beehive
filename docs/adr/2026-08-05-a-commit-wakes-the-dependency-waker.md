# A commit wakes the dependency waker

- **Status:** Accepted — implemented in `waker.go`, `objectswatch.go`,
  `watchhub.go`, `internal/rategate/rategate.go`, `internal/driver/driver.go`.
  **The floor tick was removed on 2026-08-05**: the wake is now the waker's only
  cadence. See [the wake-driven ADR](2026-08-05-the-waker-is-wake-driven.md).
- **Date:** 2026-08-05; floor tick removed the same day

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

### The wake is the only cadence

*(Amended 2026-08-05.)* This ADR shipped with `wakeInterval` as the floor behind
the wake. That tick is gone: a pass that drains the log arms nothing, and a
failed scan re-arms `waker.retry` rather than the floor — which is what used to
clear `backingOff`. The stale-dependents pass is still the guarantee underneath,
and it is now the only thing covering a write this process did not publish. See
[the wake-driven ADR](2026-08-05-the-waker-is-wake-driven.md).

The closed-hub arm in `run` is a safety net rather than the normal exit: `stop`
cancels `runCtx` and waits on the WaitGroup the waker is in, closing the hub
only after, so `ctx.Done()` wins except when the drain hits its deadline.

### Two rate limits, both `internal/rategate`

**Scans are floored at `wakeScanMinInterval` (100ms).** Removing the tick as the
only pacing would otherwise let a sustained write stream hold the connection at
the full paging budget. `rategate` is eager after a quiet period, so an
idle-to-active transition pays no added latency and only a sustained stream is
paced. The value is a constant rather than a fraction of the floor: deriving it
would have tied wake latency to a cadence we wanted free to raise.

*(Amended 2026-08-05.)* It was clamped to the floor tick, because a scan floor
above it would have delayed a scan past the bound the tick provided. With no
tick it is the waker's only cadence and the clamp is gone.

**The cursor write is floored at `wakePersistInterval` (1s).** `scan` persists on the way
out whenever the watermark moved, which under a sustained stream is every pass —
so a 10×-faster loop would have meant a 10×-faster `DriverCursorsSet`. Every
other cost here is a read competing for connection time; this one is a bare
write competing for the write lock, which is what the commits themselves need.
Gating it keeps the cursor write rate exactly what it was. The cursor is an
optimisation over the stale-dependents pass, `persist` writes the current
watermark rather than a delta, and a crash with a ≤1s-stale cursor replays wakes
that are idempotent.

**The gate sits ahead of the retry ladder, not just ahead of the write.**
*(Amended 2026-08-05.)* The ladder was a count of persists sat out, which made
its ordering against the gate load-bearing: below it, refused passes would burn
skips at the wake rate. It is a delay now, for the reason in
[the wake-driven ADR](2026-08-05-the-waker-is-wake-driven.md) — a pass costs a
scan, so a ladder counted in passes reads a store that is already failing its
writes — and the ordering is no longer what protects it.

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
  tick does not. It was set to the waker's 1s cadence for this change; **raising
  it needs this ADR re-read first.**
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
