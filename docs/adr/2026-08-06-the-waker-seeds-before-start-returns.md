# The dependency waker subscribes and seeds before Start returns

- **Status:** Accepted — implemented in `waker.go`, `beehive.go`.
- **Date:** 2026-08-06

## Context

The waker promises that a write reaches its dependents on the next commit. Two
things had to be in place before that promise could hold, and neither was when
`Start` returned:

- **The watermark.** `seed` read `ObjectWritesMaxVersionAll` — and the stored
  cursor, when the store is a `DriverCursorer` — on the waker's own goroutine,
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
`DriverCursorer`, or a seed whose reads failed.

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

### run opens from what the seed found, not from an eager pass

`run` used to open with `time.NewTimer(0)`, whose only job was to seed. With the
seed already done, an unconditional first pass would spend an
`ObjectWritesListSinceAll` that comes back empty, on the single connection,
exactly when the owed pass and every reconcile loop want it. So `primedWait`
answers from what `prime` left: `scanMore` drains at once, `wakeIdle` arms
nothing, and an unseeded waker climbs the retry ladder.

It ends in the same `persistWait` check `pass` makes, and for the reason
[the wake-driven ADR](2026-08-05-the-waker-is-wake-driven.md) gives: a seed
whose cursor write was refused is caught up, so nothing else would arm, and a
successor that finds no row reseeds at the mark — skipping everything committed
while this process ran. An idle waker that owes a cursor write is not idle.

**The gate is `seeded`, not the primed value.** `scanIdle` is `scanResult`'s
zero value, so a waker nobody primed would otherwise read as caught up and idle
forever, arming nothing and never seeding. Gating on `seeded` collapses that
case into the failed-seed one. A `scanUnprimed` sentinel was rejected: it adds a
value `scan` never returns, for a state `seeded` already names.

That gate only holds if a failed seed really does leave `seeded` false, which
takes one line: an aborted `Start` leaves the Beehive startable, so `prime`
clears the flag before seeding rather than inheriting the previous attempt's.
Without it a retried `Start` whose seed fails reads as caught up and arms no
retry.

One eager query survives by design: a commit landing between the subscribe and
the seed read fills the receiver's slot, so `run` consumes that wake and scans
once, finding nothing. That is the price of subscribing first, and it is one
query on a store already busy at startup.

## Consequences

**The seed race is closed, not narrowed.** This supersedes the "narrows to first
run only" paragraph in [the durable-cursor ADR](2026-07-30-durable-waker-cursor.md),
and the `docs/TODO.md` entry it belonged to is deleted. The hesitation that ADR
recorded — a synchronous seed now reads *two* rows inside `Start`'s critical
section rather than one — is what this decision accepts, and the answer to "does
a failed seed abort startup" is no, which is why the retry-on-a-later-pass path
stays.

**`Start` blocks on the store, under `bh.mu`.** Two indexed reads and at most
one cursor write. `Register`, `Stop` and a concurrent client write's commit
hooks wait for them. **This does not deadlock**, and the reason is worth
recording: `Within` runs its `AfterCommit` hooks *after* `tx.Commit()`, so the
connection is back in the pool before any hook reaches for `bh.mu`. A hook
waiting on the lock never also holds the connection `prime` is waiting for.

**The waker's fields have a second toucher.** `prime` runs on the caller's
goroutine; `run` on its own. They are ordered by `Start` launching the second
after the first returns, and nothing else may touch them.

**A Beehive with no hub now idles after its seed** rather than running one eager
pass. Nothing depended on that pass; the stale-dependents pass covers such a
process as it always did.

### Tests

`TestStartSeedsTheWakerBeforeItReturns` and `TestWakerPrimeSubscribesBeforeItSeeds`
are the deterministic pins, one per half. `TestStartSurvivesAFailedWakerSeed`
pins that a store error is not a startup error, and
`TestWakerRetriesSeedOnTheNextTick` / `TestWakerRetriesSeedOnAFailedCursorRead`
— the tripwires the deleted TODO entry named — still pin the retry path that
answer depends on. `TestWakerPrimedWait` covers the three opening states.

**There is deliberately no end-to-end test of the race itself.** Any wait
between `Start` and the racing write closes the window under the old code, and
without a wait two triggers that cannot be disabled — `reconciler.run`'s
unconditional owed pass and `staleDependentsRun`'s first sweep — can deliver the
wake instead, so such a test passes for the wrong reason as easily as the right
one. The end-to-end wiring is covered instead by the `TestClientOnlyTarget*` and
`TestDependencyRequeueRace*` tests, which fail outright when `Start` does not
prime.
