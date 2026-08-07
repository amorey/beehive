# The dependency waker subscribes and seeds before Start returns

- **Status:** Accepted — implemented in `waker.go`, `beehive.go`.
- **Date:** 2026-08-06

## Context

The waker promises that a write reaches its dependents on the next commit. Two
things had to be in place before that promise could hold, and neither was when
`Start` returned:

- **The watermark.** `seed` read `ObjectWrites().MaxVersionAll` — and the stored
  cursor — on the waker's own goroutine,
  whenever the runtime first scheduled it. A write committed between `Start`
  returning and that read landed *below* the watermark, and no scan read it.
- **The subscription.** `run` registered with `kindWriteHub.WatchAcross` on that
  same goroutine. Since [the waker went wake-driven](2026-08-05-the-waker-is-wake-driven.md)
  a commit landing before that registration is not merely late: it wakes
  nothing, and the waker sits idle until some later commit arrives.

`Start` returning is exactly the signal a caller waits for before writing, so
the window was easy to hit. [The durable cursor](2026-07-30-durable-waker-cursor.md)
narrowed it — a racing write sits *above* a stored cursor — but it reopened
whenever the seed fell back to the mark: a fresh store, a store with no
no persisted cursor, or a seed whose reads failed.

The cost was latency rather than divergence: the stale-dependents pass derives
staleness from `dependency_watermarks`, and the racing write bumps the target's
`resource_version`, so the next sweep lists the dependent. `docs/TODO.md` carried
this for a week as "worth doing on latency grounds", deferred because the fix
puts store reads inside `Start`'s critical section.

## Decision

**Do both steps synchronously in `Start`, subscribe first.** `waker.prime` takes
the subscription and then seeds; `Start` calls it before it launches any
goroutine. No caller holds the stop func while that runs, so the watermark
precedes every write a caller could make and the subscription precedes every
commit that could wake it.

### A failed seed does not fail Start

The waker is an optimisation over the stale-dependents pass, so a store that
cannot answer the seed read must not stop the control plane coming up. `prime`
returns nothing. A failed seed leaves the waker unseeded exactly as before, and
`run` arms the retry ladder for it.

A `startCtx` cancelled *during* the seed is different — that is the caller
abandoning startup, and `Start` already contracts to answer `start aborted` for
it, so it tears the subscription back down and returns that error.

`Start` also becomes a store *writer*: `seed` ends in `persist`, so a fresh
store takes one `DriverCursorsSet` for the seed point. A store that refuses it
warns and startup continues.

### run keeps its eager first pass

`run` opens with `time.NewTimer(0)` as it always did. That pass used to be the
seed; now the seed is already done, so it is an ordinary scan from the watermark
`prime` took — or, when prime's read failed, the seed retried at once.

It was tempting to skip it: on a caught-up seed it spends an
`ObjectWrites().ListSinceAll` that comes back empty. A decision function answering
"how long before the first pass" from what the seed found was written, reviewed,
and removed again. It bought ~21µs and one scan-floor window of first-wake
latency, once per process, and it cost a second copy of decisions `pass` already
makes — a copy that was wrong twice before it was deleted. The eager pass reaches
the same four answers with no new code: a caught-up scan goes idle, a backlog
drains, a failed seed retries on the backoff ladder, and a refused cursor write
re-arms through `persistWait`.

**A failed seed must still leave `seeded` false**, which takes one line: an
aborted `Start` leaves the Beehive startable, so `prime` clears the flag before
seeding rather than inheriting the previous attempt's. Without it a retried
`Start` whose seed fails scans from a watermark it never read.

## Consequences

**The scheduling half of the seed race is closed.** No write a caller can make
after `Start` returns is below a watermark this process took, or ahead of its
subscription. This supersedes the "narrows to first run only" paragraph in
[the durable-cursor ADR](2026-07-30-durable-waker-cursor.md). The hesitation that
ADR recorded — a synchronous seed now reads *two* rows inside `Start`'s critical
section rather than one — is what this decision accepts.

**A failed seed keeps its own window, and it is the same window.** The answer to
"does a failed seed abort startup" is no, so `run` retries roughly 100ms later —
and with no stored cursor to resume from, that retry reads the mark as of
*then*. A write committed in between is below it and reaches its dependents
through the stale-dependents pass, not through the waker. `backingOff` is why a
commit in that window does not shorten it: an unseeded waker drops wakes until
its retry fires. This is narrower than what this ADR closes — it needs a failed
store read *and* a store with no cursor stored yet — but it is
the same shape, so `docs/TODO.md` keeps an entry for it rather than claiming the
race is gone.

**`Start` blocks on the store, under `bh.mu`.** Two indexed reads and at most
one cursor write. `Register`, `Stop` and a concurrent client write's commit
hooks wait for them. **This does not deadlock**, and the reason is worth
recording: `Within` runs its `AfterCommit` hooks *after* `tx.Commit()`, so the
connection is back in the pool before any hook reaches for `bh.mu`. A hook
waiting on the lock never also holds the connection `prime` is waiting for.

**The waker's fields have a second toucher.** `prime` runs on the caller's
goroutine; `run` on its own. They are ordered by `Start` launching the second
after the first returns, and nothing else may touch them.

### Tests

`TestStartSeedsTheWakerBeforeItReturns` and `TestWakerPrimeSubscribesBeforeItSeeds`
are the deterministic pins, one per half. `TestStartSurvivesAFailedWakerSeed`
pins that a store error is not a startup error, and
`TestWakerRetriesSeedOnTheNextTick` / `TestWakerRetriesSeedOnAFailedCursorRead`
— the tripwires the deleted TODO entry named — still pin the retry path that
answer depends on. `TestStartRePrimesAfterAnAbortedStart` pins the `seeded`
reset.

**There is deliberately no end-to-end test of the race itself.** Any wait
between `Start` and the racing write closes the window under the old code, and
without a wait two triggers that cannot be disabled — `reconciler.run`'s
unconditional owed pass and `staleDependentsRun`'s first sweep — can deliver the
wake instead, so such a test passes for the wrong reason as easily as the right
one. The end-to-end wiring is covered instead by the `TestClientOnlyTarget*` and
`TestDependencyRequeueRace*` tests, which fail outright when `Start` does not
prime.
