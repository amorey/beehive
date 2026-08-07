# The dependency waker is wake-driven and has no tick

- **Status:** Accepted — implemented in `waker.go`, `beehive.go`, `options.go`.
- **Date:** 2026-08-05

## Context

[A commit wakes the dependency waker](2026-08-05-a-commit-wakes-the-dependency-waker.md)
put a push in front of the waker's 1s scan and kept the tick as the floor behind
it. That left the tick a heartbeat with nothing to find. In a single-process
beehive a lost wake is close to impossible by construction — the subscription is
registered before the seed read, the hub closes only at stop, and one slot per
receiver means a burst cannot drop one — so essentially every tick was a quiet
pass: one `ObjectWrites().ListSinceAll` that comes back empty, ~21µs by
`BenchmarkWakerScanRateUnderSustainedWrites`. 86,400 queries a day of insurance
against an event the push path makes very hard to arrange.

The tick also only earned its keep strictly faster than 60s: at or above
`staleDependentsInterval` it is redundant, because the backstop runs at that
cadence and finds a superset of what a cursor replay finds.

## Decision

**Arm a timer only when there is a reason to look again.** `pass` answers
`wakeIdle` after a scan that drained the log, and `run` arms nothing and blocks
on the wake. `scanMore` re-arms at the scan throttle; `scanFailed` re-arms the
retry. The eager first pass stays — it seeds, and a restart resumes from the
persisted cursor there rather than on a tick.

### The failure retry moved first

`backingOff` *drops* arriving wakes, so a degraded store cannot be re-read as
fast as it can fail. The floor timer used to be what cleared it; with no floor,
a failed pass that re-armed nothing would wedge the waker for the life of the
process. `waker.retry` is a `driver.Backoff` — `wakeRetryBase` (100ms), doubling,
capped at `staleDependentsInterval` — the ladder `objectTailer` already uses. It
is conditional rather than periodic, so it does not reintroduce what this
removes, and the cap is where a retry stops being worth making: past it the
backstop has already swept.

### An outstanding cursor write is the second reason to look again

`persist` failing does not fail the scan, so a pass whose log was drained
answers "idle" with the cursor row still behind the watermark. Under the tick
that was retried within a second; wake-driven, the next commit would be the only
retry, and a process that seeds, fails to persist and then stops leaves its
successor to reseed at the mark — skipping everything committed while it was
down, exactly the gap the durable cursor exists to close. So `pass` treats a row below
the watermark as a reason of its own and re-arms for when the write is next
worth attempting. It stops the moment the write lands.

The retry ladder had to become a **delay** for that to be affordable. It counted
persists sat out, which was sound under a tick that was going to run anyway;
wake-driven, the only thing that carries a persist attempt is a pass, and a pass
costs an `ObjectWrites().ListSinceAll`. A count of 60 would mean 60 scans of a store
that is already refusing its writes — the heartbeat this ADR removes, reinstated
in the one condition where the store can least afford it. `persistRetry` is a
`driver.Backoff` from the persist floor to a minute, and `persistWait` re-arms
for whichever of the two pacers is further out.

### Stopping the timer is part of going idle

The loop owns one timer, and `nextTimer` is its whole policy: never push a
pending deadline later, and **stop** what is armed once a pass reports nothing to
come back for. The stop is not bookkeeping — when a throttle timer and a commit
wake become ready together the wake may win the select, and a timer left armed
then drives a pass nobody asked for. The race is not reproducible on demand, so
the rule is a pure function with a table test rather than a loop test.

### Two knobs the tick was standing in for

`wakeInterval` also floored the cursor write, and through it decided what the
persist retry ladder meant in seconds. That floor is now
`wakePersistInterval` (1s, unchanged in value). And `wakeInterval <= 0` used to
mean "no waker at all", which needed to stop being a cadence: `withDependencyWakerOff`
says it directly.

The scan floor is no longer clamped to anything. `wakeScanMinInterval` was
clamped to the tick because a scan floor above the tick would delay a scan past
the bound the tick was there to provide; with no tick it is the waker's only
cadence and stands on its own.

### What this gives up: a wake nobody published

`signalKindWritten` publishes to an in-process hub, so the waker sees a write
made **through this process's `Client`, `ControllerClient` or GC path** and
nothing else. Two writers it no longer hears:

- a second process writing the same store, and
- a write issued straight to the `Store` behind the beehive's back.

**Both are unsupported configurations, not degraded ones** — see
[one process, one Beehive](2026-08-05-one-process-one-beehive-sole-writer.md),
which is what this section is really describing. The tick previously covered
them incidentally, at its cadence. Losing that coverage costs nothing beehive
promises. In practice the stale-dependents pass still finds them at 60s, since
it derives staleness from watermarks rather than replaying a cursor and so finds
a superset of what the waker finds — but that is a property of the backstop, not
a guarantee extended to writes beehive never saw commit.

An opt-in periodic pass, off by default, would restore the incidental coverage.
It is not built: the knob would exist for a deployment shape this package does
not serve, and the honest fix for that shape is a store-backed wake rather than
a tick whose cadence *is* the latency target.

## Consequences

- **An idle beehive issues no waker queries and holds no waker timer.**
  `TestIdleWakerIssuesNoQueries` pins the queries; `TestNextTimer` pins that
  going idle stops whatever was armed.
- **Two conditions re-arm, neither of them periodic**: a failed scan, and a
  cursor write the store has not accepted yet. Both stop on success.
- **A failed scan recovers on its own.** `TestWakerRecoversFromAFailedScanWithoutATick`
  runs with nothing behind the waker at all — no wake is sent — so only the
  retry can produce the second read.
- **`defaultMinRequeueInterval` no longer matches a waker cadence.** It bounds a
  dependency cycle, and the constant it was set against is gone; it stands on
  its own reasoning now (see
  [the floor ADR](2026-08-04-work-queue-re-enqueue-floor.md)).
- **The waker is the first store-backed driver with no periodic scan.** The
  schedule watch was already an exception, but on the grounds that its gauge
  never reaches the store — this one reads the store and still has no tick. See
  the amendment in
  [the drivers ADR](2026-07-28-periodic-scan-drivers.md).
- **A test that changes an object by writing to the `Store` directly must
  publish the wake itself.** The two that could moved onto the in-band write
  (`ControllerClient.ConditionsSet`, which publishes for itself); the two whose
  target is a client-only kind call `bh.signalKindWritten` by hand, because
  `Client` has no conditions write to make.
