# The driver cadences are configurable, because every trigger pushes at commit

- **Status:** Accepted — implemented in `options.go`, `beehive.go`,
  `objectswatch.go`, `waker.go`, `sqlite/store.go`.
- **Date:** 2026-08-06

## Context

An embedder running beehive inside a desktop application leaves the process up
all day, mostly idle. Every driver tick is a query on the store's single
connection, and the idle cost is one query per driver per interval: on a
dashboard with a handful of registered kinds, a handful of watched kinds and a
few dozen open event streams, the stock cadences come to roughly one query a
second, forever, with nothing happening.

Nothing about that cost was configurable. `WithGCInterval` and
`WithFullPassInterval` were the only public cadences; the owed pass, the
stale-dependents pass and the watch floor were unexported options only tests
reached.

**What changed underneath is that the ticks stopped being the trigger.** Every
reconcile trigger for a registered kind now pushes at commit — nine paths, all
enumerated in [reconcile-triggers.md](../reconcile-triggers.md) — both watch
families read on a commit wake, and the dependency waker has no tick at all.
The remaining ticks are backstops for a push lost between the commit and the
dispatch, for kinds no push covers, and for a restart. So the question a cadence
answers is no longer "how long until this happens" but "how long until this is
*recovered* when the push that should have carried it was lost".

That reframing is what makes a multi-minute interval a reasonable thing to ask
for. It is not what makes it safe: three constants were doing double duty as
error-recovery bounds, and two per-sweep work budgets were sized against the
default GC cadence rather than against wall-clock time.

## Decision

**Promote `WithOwedPassInterval`, `WithStaleDependentsInterval` and
`WithWatchFloorInterval`, keep every default exactly where it was, and first
decouple everything that would have silently degraded when one was lengthened.**

The defaults are unchanged deliberately: an embedder who has thought about the
trade-off can express it, and one who has not gets what beehive always did. Each
option's godoc states what lengthening it costs in the terms above — recovery of
a lost push, staleness after a lost wake — because "how often the owed pass
runs" does not tell a caller what they are trading.

### None of the three can be disabled

All three reject a non-positive value with `ErrInvalidOption`, matching
`WithGCInterval`. Each is the only mechanism that re-derives its own class of
work: the owed pass is what makes convergence a guarantee, the stale-dependents
pass is what makes a dependency wake one, and the watch floor is what covers a
retention trim. "Rarely" is expressible; "never" is not.

`withOwedPassInterval` stays unexported alongside the public form, without the
floor, because a test that wants to prove a push carried something on its own has
to be able to turn the tick off. The other two already had no such caller.

### Three constants were doing double duty, and two of them had to stop

- **`watchBackoff().Max` was the watch floor.** A 10-minute floor would have
  retried a *failed* tail read at 10 minutes. It now caps at `watchRetryMax`,
  30s — the value the floor used to supply. The old comment argued the cap
  should be "what a healthy quiet kind already costs"; that argument holds only
  while the floor is a number beehive picks, and it is now a number the embedder
  picks for battery.
- **The waker's `retry.Max` was `staleDependentsInterval`.** The reasoning was
  that past the backstop's cadence a retry only re-derives what the backstop has
  already found. True, but the cost is not symmetric: `backingOff` drops every
  wake arriving during a backoff, so a transient store error would wedge the
  waker for the whole interval with nothing able to shorten it. It now caps at
  `wakeRetryMax`, 30s.
- **The waker's `abandonAfter` is still `staleDependentsInterval`, and that is
  correct.** It is not a shared default: the jump to the write log's mark is
  sound *only* because the stale-dependents pass has already swept the range
  being skipped ([ADR](2026-08-05-the-waker-abandons-an-overtaken-drain.md)). A
  longer interval means a longer drain before the argument holds, which is the
  right answer. It is passed by argument with a comment saying so, because the
  next reader will find it beside the retry cap that just moved.

### The GC sweeper's per-sweep budgets scale with its interval

`freePagesPerSweep` (1000) and the store's event-cap budget (256 timelines) were
both sized against a 30s sweep. At 5 minutes they would be a 10× slower
incremental vacuum and — the one item here that is not merely latency — a 10×
slower event trim, so an application writing events continuously would sit over
its configured cap indefinitely.

Both are now derived rather than re-picked: `Beehive.gcBudget` scales a budget
sized against `gcBudgetInterval` (30s) by `gcInterval / gcBudgetInterval`,
floored at 1×. Work per unit time holds, and a shorter interval keeps today's
behaviour by sweeping more often.

The event budget lives in the sqlite package, inside `overCapTimelines`, so
`EventsSweep` takes it as a parameter — a breaking change to `Store`. A
non-positive budget means "the implementation's own", which is what keeps the
sweep meaningful for a caller that is not the GC loop. `ObjectWritesSweep` needs
no equivalent: its work is bounded by the *kind* count, one statement each, not
by a per-sweep row budget.

### What is not configurable, and why

`minRequeueInterval` (1s), `wakeScanMinInterval` (100ms),
`watchScanMinInterval` (100ms) and `wakePersistInterval` (1s) stay unexported.
All four are floors on *active* work: they cost nothing while idle, so raising
one saves no battery and adds user-visible latency. `minRequeueInterval` is
additionally the only bound on a dependency cycle
([TODO](../TODO.md)), so it is not a preference.

## Consequences

**A long GC interval costs a client-only cascade one interval per level.** A
registered kind advances a level per commit; a client-only kind has no push at
all. At 5 minutes a three-level client-only subtree takes fifteen. This is
recorded in `WithGCInterval`'s godoc rather than fixed — the fix would be a push
path, and this change adds none.

**A lengthened cadence lengthens recovery, and nothing else.** Every path it
paces is enumerated in [reconcile-triggers.md](../reconcile-triggers.md), which
now names the option rather than a bare number wherever a cadence appears. An
embedder reading "5 minutes" there should read it as the recovery window for the
push above it.

**The retry ladders no longer track the cadences**, so a store failing under a
battery-tuned beehive recovers at the same rate it always did. Pinned by
`TestWakerRetryCapIsNotTheBackstopsCadence` and the tailer's "a failing drain
settles at the retry cap, not at the floor".

**`Store.EventsSweep` grew a parameter.** External backends do not exist yet
(`Store` is not externally implementable today — see [TODO](../TODO.md)), so the
cost is the in-tree implementation and the test doubles.
