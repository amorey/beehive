# A commit wake in front of the dependency waker

**Status:** not built.

## Problem

Case 6 in [`reconcile-triggers.md`](../reconcile-triggers.md) — an ordinary change to
a target waking its dependents — has no push. `waker.scan` is the only route from
"target committed" to "dependents enqueued", and it runs on a 1s tick.

The cost is **one tick per hop**. A chain of depth D settles in about D seconds no
matter how fast the store commits, because each link waits for the next tick to
notice the link before it. This is the largest latency gap in the system, and it is
on the path most controllers actually use.

The tick also runs unconditionally for the life of the process, so it costs a query
per second whether or not anything changed.

## What exists

`objectTailer` already runs the exact pattern this spec wants: a commit wake in
front, a floor tick behind. `signalKindWritten` publishes the written kind to
`kindWriteHub` on `AfterCommit`, the tailer selects on that wake or on its floor
timer, and a wake with nothing behind it costs one scalar position read.

The waker differs from the tailer in three ways that matter:

- It is **store-wide**, not per kind. A `depends_on` edge can point at a client-only
  kind that no per-kind query would name.
- It is **unconditional**. A tailer runs only while something watches, so its
  duty cycle is demand-scoped. The waker's is not.
- Its cursor is **persisted** in `driver_cursors`, and resuming it is a real
  workload — `wakeScanPagesPerTick` exists so a long resume cannot monopolise the
  single connection.

## Proposal

Wake `waker.scan` when a write commits, and keep the tick as a floor.

The signal is **not** the changed object's id. It is "something was written", the
same reduction `signalKindWritten` makes: the waker holds one store-wide cursor and
reads `object_writes` to learn what moved, so a burst of commits collapses into one
wake. Reuse `kindWriteHub` rather than adding a second hub — every write that bumps a
`resource_version` already publishes to it.

The scan body does not change. Seed, cursor, paging, the self-edge skip and the
hold-on-failure behaviour all stay.

## What must stay true

- **The floor tick stays.** This makes the waker a driver with a wake in front, like
  the watch tail. It does not make it a push path. `wakeInterval` still bounds the
  worst case, a non-positive interval still turns the waker off, and the
  stale-dependents pass is still the guarantee underneath.
- **A wake is an optimisation twice over.** The waker is already an optimisation over
  the stale-dependents pass. A lost wake costs latency against a lost tick, which
  costs latency against a lost sweep. Nothing here is load-bearing.
- **The cursor still records what was scanned, never what was woken.** A wake that
  fires and finds nothing must not move it further than the scan reached.

## The throttle that had to land first

Removing the tick removes the tick's own rate limit on the dependency wake path.
Two mutually dependent objects that write on every pass would round trip as fast as
the store commits, on the connection every other writer shares.

**That is already handled.** The work queue's re-enqueue floor
([ADR](../adr/2026-08-04-work-queue-re-enqueue-floor.md)) bounds one object to one dispatch per interval whatever the wake rate,
and `defaultMinRequeueInterval` was set to `defaultWakeInterval` precisely so this
spec could remove the tick without changing the cycle rate. **If that constant is
ever raised, re-read this section before shipping the wake.**

The waker also needs a throttle of its own, at a different granularity: a minimum
interval between wake-driven scans, so a sustained write stream does not hold the
single connection at full paging budget. This is the same limit the object watch tail
wants (see [`TODO.md`](../TODO.md)), and the trade comes out harder here, because the
waker's loop runs for the life of the process rather than while a caller watches.

**Use `internal/rategate`, which already exists.** The waker is its third consumer
and the simplest: one key, consulted before `scan`, with a `NextAt` re-arming the
tick timer the loop already selects on. That is what makes the three throttles the
same code rather than three curves that drift apart.

`rategate` ships `OpensAt`, `Admit` and `Forget` today, which is what the work queue
needed. **This spec adds `Allow` and `NextAt`** — a driver tests and acts at the same
point, so it wants the combined form, and it needs the earliest-held-key query to arm
one timer over the set. Add them here with their consumers.

## Resolved questions

**Should a wake be ignored during a resume drain? — Yes, and the repo already does
it.** `objectTailer.run` drops commit wakes while backing off, and its comment gives
the argument in full: *"A wake carries no information — the drain reads its position
from the store — so dropping one loses nothing."* A waker already paging at its full
budget is in the same position, so absorb wakes until it catches up. Copy the tailer's
mechanism as well as its reasoning: it **consumes** the wake and then continues,
rather than skipping the receive, which is what keeps the closed-channel arm live.

**Should the wake be per kind or store-wide? — Reuse `kindWriteHub` and discard the
key.** A second hub would mean every write site publishes twice, and
`signalKindWritten` already sits at every write that bumps a `resource_version`. The
waker's scan is store-wide, so the key is simply unread. One difference from the
tailer to handle: `stop` closes that hub, and the tailer reads the close as
`ErrStopped` for its subscribers. The waker has no subscribers, so its closed arm
just returns.

**What happens to `wakeInterval`'s default? — Nothing, in this change.** 1s is now
load-bearing twice: as this driver's floor and as the value
`defaultMinRequeueInterval` matches, which is what keeps a cycle at exactly the rate
it has today. Raising it is a separate measurement with its own argument, and bundling it
here would make that claim untestable in the same diff.

## Tripwires

`TestWakerSeedsFromTheWriteLogMax`, `TestWakerSeedsFromTheStoredCursor`,
`TestWakerRetriesSeedOnTheNextTick` and `TestWakerRetriesSeedOnAFailedCursorRead` pin
the seed contract. A wake must not seed, and must not turn a failed seed into a
scan.

`TestWakerHoldsTheWatermarkOnLookupFailure` and `TestWakerResumesAnEnormousBacklog`
pin the drain. Both must hold with wakes arriving mid-drain.

`TestWakerScanWakesDependentsByTheirOwnKind`, `TestWakerSkipsTheSelfEdge` and
`TestClientOnlyTargetWakesDependent` pin the scan semantics, which do not change.

Nothing pins the current wake latency, so nothing fails if the wake silently does not
fire. Add a test that a dependent is enqueued at commit with the tick interval set
long enough that it cannot be the cause.

## Done when

- A dependent is enqueued when its target's write commits, not on the next tick.
- A chain of depth D propagates in D commits.
- With the wake disabled, every waker test above still passes at the tick cadence.
- Case 6 in [`reconcile-triggers.md`](../reconcile-triggers.md) lists a push, and
  section 1's push-path table and the *"the only commit-driven push"* note in section
  E are corrected.
