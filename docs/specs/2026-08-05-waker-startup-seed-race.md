# Seed the dependency waker before `Start` returns

- **Status:** Ready to implement — supersedes the "waker's startup seed race"
  entry in [`../TODO.md`](../TODO.md), which is deleted by this change.
- **Date:** 2026-08-06 (problem first recorded 2026-08-05)
- **Branch:** `fix-waker-startup-seed-race`
- **Touches:** `beehive.go`, `waker.go`, `waker_test.go`, `beehive_test.go`,
  `docs/TODO.md`, `docs/reconcile-triggers.md`, `CLAUDE.md`, one new ADR.

## Problem

The waker promises "every write from here on reaches its dependents on the next
commit". Two things have to be in place before that promise can hold, and today
neither is in place when `Start` returns:

1. **The watermark.** `seed` reads `ObjectWritesMaxVersionAll` (and the stored
   cursor, when the store is a `DriverCursorer`) on the waker's own goroutine,
   whenever the runtime first schedules it. A write committed between `Start`
   returning and that read lands *below* the watermark the waker then takes, and
   no scan ever reads it.
2. **The subscription.** `run` registers with `kindWriteHub.WatchAcross` on that
   same goroutine. The waker has no tick
   ([ADR](../adr/2026-08-05-the-waker-is-wake-driven.md)), so a commit landing
   before the subscribe is not merely late — it wakes nothing at all, and the
   waker sits idle until some *later* commit happens to arrive.

`Start` returning is exactly the signal callers wait for before writing, so the
window is trivially easy to hit. It is narrower than it was — with a stored
cursor, a racing write sits *above* the resume point and is scanned normally —
but it reopens fully whenever `seed` falls back to the mark: a fresh store, a
store with no `DriverCursorer`, or a seed whose reads failed (the next pass then
seeds from the mark as of *then*, skipping everything committed in between).

**Impact is latency, not divergence.** A dependent stranded this way is
invisible to every owed-work listing — its own generation never moved, and
nothing stamped `reconcile_owed` — but the stale-dependents pass derives
staleness from `dependency_watermarks` rather than from anything the waker
recorded, and the racing write bumps the target's `resource_version`, so the
next sweep lists it. The cost is up to one `staleDependentsInterval` (60s) of
delay where the waker promises one commit.

## Decision

**Do both startup steps synchronously in `Start`, in the order the wake path
already depends on: subscribe, then seed.** No caller holds the stop func while
that runs, so the watermark provably precedes every write a caller could make,
and the subscription provably precedes every commit that could wake it.

The run goroutine then starts from what priming found rather than from an
unconditional eager pass.

### `Start` does not fail on a failed seed

A store read failing at startup must not abort startup: the waker is an
optimisation and the stale-dependents pass is the guarantee. So priming returns
no error. A failed seed leaves the waker unseeded exactly as today, and the run
loop arms its retry ladder for it — which is the path
`TestWakerRetriesSeedOnTheNextTick` and `TestWakerRetriesSeedOnAFailedCursorRead`
already pin, and which this change must keep alive rather than replace.

A `startCtx` cancelled *during* priming is different: that is the caller
abandoning startup, and `Start` already contracts to answer
`"beehive: start aborted"` for it. `Start` re-checks after priming, tears the
subscription down and returns that error.

**`Start` also becomes a store *writer*.** `seed` ends in `persist`, and on a
fresh store `watermark > persisted == noStoredCursor`, so every `Start` against
a `DriverCursorer` with no row yet issues one synchronous `DriverCursorsSet`.
That write is what keeps a process that seeds and stops without seeing a write
from stranding its successor at the mark, so it belongs in the critical section
with the rest. A store that refuses it produces `persist`'s warn-level log and
nothing more: startup proceeds, the ladder retries, and the cursor being stale
costs latency after a *later* restart. Nothing here can fail `Start`.

### The run loop arms from the primed result

Today `run` opens with `time.NewTimer(0)` — an eager first pass whose only job
was to seed. With the seed already done, an unconditional eager pass would spend
an `ObjectWritesListSinceAll` that comes back empty, on the single connection,
at the moment the owed pass and every reconcile loop want it. So the primed
`scanResult` picks the loop's opening state:

| primed result | opening state |
| --- | --- |
| `scanIdle` (seeded at the mark) | nothing armed, `backingOff` false — the wake-driven steady state |
| `scanMore` (resumed below the mark) | armed at zero: drain the backlog now |
| `scanFailed` (either read failed, or `startCtx` was cancelled) | armed at `retry.Next()`, `backingOff` true |

This is the same dispatch `pass` already returns to the loop, applied one turn
earlier.

**The gate is `dw.seeded`, not the `scanFailed` value.** `scanIdle` is
`scanResult`'s zero value, so a waker that was never primed would otherwise read
as "seeded at the mark" and sit silently idle for the life of the process —
arming nothing, never seeding, never scanning. That is failure mode #1 as a
silent one, and a footgun for any future caller of `run`. So the rule the loop
actually implements is:

- `!dw.seeded` → arm `retry.Next()`, `backingOff` true. Covers both a failed
  seed and a `run` that was never primed.
- `scanMore` → arm zero.
- otherwise → arm nothing.

In production the two conditions coincide (a failed seed always leaves
`seeded` false), so this costs nothing and closes the hole. The alternative — a
`scanUnprimed` sentinel at the head of the `scanResult` iota — was rejected: it
adds a value `scan` never returns, to describe a state `seeded` already names.

### One eager query survives, and that is fine

The table's rationale is "don't spend an empty `ObjectWritesListSinceAll` at
startup", and it does not fully hold: a commit landing between `prime`'s
subscribe and its seed read fills the receiver's one slot, so `run` consumes
that wake and scans — finding nothing, because the seed read already took a mark
above it. Exactly one wasted scan, only on a store busy during startup, and it
is the cost of the subscribe-before-seed ordering rather than a defect in it.
Claim "no *unconditional* eager query", not "no eager query".

## Implementation

### `waker.go`

Two fields, one new method, one changed one.

```go
type waker struct {
	// ...

	// written carries the commit wakes. Registered by prime, before the seed
	// read: the waker has no tick, so a commit landing before the subscribe
	// wakes nothing at all. nil when the Beehive was assembled without a hub,
	// which blocks forever.
	written <-chan gobus.Event[GroupKind, struct{}]
	closeRx func()

	// primed is what the seed in Start found, and the state run's first turn
	// starts from.
	primed scanResult
}
```

**`prime(ctx context.Context)`** — called from `Start`, on the caller's
goroutine, before `run`'s goroutine exists:

- No-op when `len(dw.bh.order) == 0 || dw.bh.wakerOff`, so a disabled waker
  costs no store read and registers no receiver (`run` keeps its own guard: the
  tests drive `run` directly).
- `WatchAcross`, storing `written` and `closeRx`; `ok == false` (zero hub)
  leaves both nil, as today.
- `dw.primed = dw.seed(ctx)`.

**`teardown()`** — closes the receiver if one was registered, idempotent. Used
by `run`'s defer and by `Start`'s abort path.

**`run(ctx)`** — keeps the disabled guard and the `defer dw.teardown()`; drops
the subscribe (now `prime`'s) and replaces the eager timer with the table above:

```go
timer := time.NewTimer(0)
defer timer.Stop()
var armedFor time.Time
backingOff := !dw.seeded // an unseeded waker drops wakes until its retry fires
switch {
case !dw.seeded:
	driver.Rearm(timer, dw.retry.Next())
case dw.primed == scanMore:
	driver.Rearm(timer, 0)
default:
	timer.Stop() // wake-driven from here: nothing armed, nothing queried
}
```

`armedFor` follows from the same branch. Factor the decision into a small pure
helper in `nextTimer`'s style — `primedWait(primed scanResult, seeded bool)
(time.Duration, bool)`, returning `wakeIdle` for the stop case — so the three
rows are unit-testable without running the loop and without a clock. `retry.Next()`
is consumed only on the unseeded path, so the ladder's first rung stays
`wakeRetryBase`.

Field ownership: the doc comment says only the waker goroutine touches these
fields. That stays true in effect — `prime` runs strictly before `bh.wg.Go`
launches `run`, which is a happens-before edge — but the comment must say so
rather than leave the reader to find the exception.

### `beehive.go`

In `Start`, after the `startCtx.Err()` pre-check and before any `bh.wg.Go`:

```go
// Both before the loops launch, and in this order: no caller holds the stop
// func yet, so the watermark precedes every write a caller could make and the
// subscription precedes every commit that could wake it. A failed seed does
// not abort startup — the waker is an optimisation, and the loop retries it.
bh.waker.prime(startCtx)
if err := startCtx.Err(); err != nil {
	bh.waker.teardown()
	cancel()
	return nil, fmt.Errorf("beehive: start aborted: %w", err)
}
```

The comment above the goroutines ("None of the goroutines below need ordering
against each other: the waker's first scan is bounded by a cursor read…") is now
wrong about *why* and must be rewritten: the ordering the waker needed is taken
above, and what is left is that the loops are genuinely independent.

`Start` now blocks on up to two indexed reads (`ObjectWritesMaxVersionAll`, and
`DriverCursorsGet` when the store has one) plus at most one `DriverCursorsSet`
for the seed point. That is the cost this change buys the guarantee with, and it
is bounded by `startCtx`.

**It holds `bh.mu` while it does.** `Register`, `Stop` and any concurrent
client write's commit hooks (`reconcilerFor` takes `bh.mu`) block for the
duration of those round trips. **This does not deadlock**, and the reason is
worth recording because it is not obvious: `Within` runs its `AfterCommit` hooks
*after* `tx.Commit()` (`sqlite/store.go:481-489`), so the connection is back in
the pool before any hook reaches for `bh.mu`. A hook waiting on `bh.mu` is never
also holding the connection `prime` is waiting for, and the cycle does not
close. Keep the priming reads inside the lock rather than "optimising" them out
of it — moving them before `bh.mu` is taken would put them before the
already-started check, and after the state flip they would no longer precede a
caller's first write.

## Tests

Whitebox, in `package beehive`, mirroring source files: `waker_test.go` for the
waker's own units, `beehive_test.go` for what `Start` orders.

New:

- `TestStartSeedsTheWakerBeforeItReturns` — over a store whose mark is
  non-zero: after `Start` returns, `bh.waker.seeded` is true and `watermark`
  equals the mark. The direct assertion that the window is closed.
- `TestStartSubscribesTheWakerBeforeItSeeds` — the store's
  `ObjectWritesMaxVersionAll` hook asserts the receiver is already registered
  (`dw.written != nil`) when the seed read runs. This is the ordering the
  wake-driven design rests on, and it is invisible in any end-to-end test.
- `TestStartSurvivesAFailedWakerSeed` — seed reads error: `Start` returns a stop
  func and no error, the waker is unseeded, `primed == scanFailed`. With the
  store recovered, the loop seeds on its retry rather than staying wedged.
- `TestStartSkipsPrimingADisabledWaker` — `withDependencyWakerOff` (and,
  separately, no registered controllers): zero store reads from priming, no
  receiver registered.
- `TestStartAbortedByACancelledStartCtxDuringTheSeed` — `Start` answers
  `start aborted`, hands back no stop func, and leaves no receiver on the hub.
- `TestWakerRunArmsFromThePrimedResult` — the three rows of the table, driven
  through `primedWait` with a fake clock: idle arms nothing and issues no query,
  `scanMore` drains at once, `scanFailed` waits `wakeRetryBase` and drops the
  wakes arriving meanwhile.
- `TestWakerScansAWriteThatRacedStartsReturn` — the regression this whole change
  is for: over a fresh store with no stored cursor, write a target the instant
  `Start` returns, and assert its dependent is woken by the waker rather than by
  the stale-dependents pass (which the test disables, or outruns).

Must stay green, unchanged: `TestWakerSeedsFromTheWriteLogMax`,
`TestWakerSeedsFromTheStoredCursor`, `TestWakerSeedsFromMaxWithoutAStoredCursor`,
`TestWakerRetriesSeedOnTheNextTick`, `TestWakerRetriesSeedOnAFailedCursorRead`,
`TestWakerPersistsTheSeedBeforeSeeingAnyWrite`, and all of
`TestWakerPassPacesTheLoop`. They call `seed`/`scan`/`pass` directly, and this
change moves the *caller*, not the seed.

### Existing run-driven tests this breaks

Every test that drives `waker.run` directly is affected, because `run` no longer
subscribes and no longer opens with an eager pass. **Seven, all in
`waker_test.go`**; none is rescued by the `!seeded` safety net, since each one
sets `seeded = true` itself or seeds through `run`. Each needs a decision, and
several are pinning behaviour that genuinely changes — say so in the comment
rather than quietly rewriting the assertion.

A `primedWaker(t, store, opts...)` helper in `testutils_test.go` — build,
`prime`, hand back the waker — keeps the rewrite to one line apiece. Note
`wakerOver` builds a `Beehive` literal with a **zero** `kindWriteHub`, so a
waker from it primes with no subscription at all; only the `newTestBeehive`
tests get a live hub.

| test (line) | today | becomes |
| --- | --- | --- |
| `TestWakerScansWhenAWriteCommits` (:158) | waits on `seedProbeStore` for the seed `run` performs, then sends a wake | prime first, then wait on the probe — the comment "the seed read follows the subscribe" now describes `prime`, and that is the ordering the test should assert |
| `TestWakerRunsWithoutAWriteHub` (:218) | `waitClosed(..., "the eager first pass")` | **premise changes**: with no hub and nothing armed, a seeded waker now does *nothing* until cancelled. Re-point it at a store whose stored cursor is below the mark, so `primed == scanMore` drives the drain, and pin "a Beehive without a hub still drains its backlog, then idles forever" |
| `TestIdleWakerIssuesNoQueries` (:240) | waits for the eager pass, then two wakes; `assert.Len(store.pages, 3)` | prime, drop the eager wait, expect **2** pages — and the assertion gets stronger: no query at all until a wake arrives |
| `TestWakerRecoversFromAFailedScanWithoutATick` (:270) | eager pass fails, `healFromCall: 2`, waits for 2 lists | prime, then one wake to cause the first failing scan; the retry is still the only way back. Adjust `healFromCall` for the shifted call count |
| `TestWakerDropsWakesWhileBackingOff` (:297) | comment "past the seed, so the first pass is a scan — and it fails" | prime, send wake #1 to cause the failing scan, wait for it, then send wake #2 and close the hub; `assert.Len(store.pages, 1)` is unchanged and still pins the drop |
| `TestWakerClosedHubArmReturns` (:324) | closes the hub and expects `run` to return | **would hang**: unprimed, `written` is nil and the closed-hub arm is unreachable. Prime first. (Not in the reviewer's list; same root cause.) |
| `TestWakerDisabledByOption` (:862) | `run` returns immediately | unchanged in behaviour, but add the mirror assertion that `prime` also no-ops — the disabled guard now lives in two places and both must hold |

### Suite-wide

`prime` no-ops only when no controllers are registered, so **every `Start` in
the suite with a registered controller now hits the store during `Start`** —
65 `.Start(` call sites across eight test files. `fakeStore.ObjectWritesMaxVersionAll`
already answers `0, nil` (`testutils_test.go:512`) and `fakeStore` is not a
`DriverCursorer`, so the default path is one harmless read. The stub-panic
doubles are the risk: run the **full suite**, not just `waker_test.go` and
`beehive_test.go`, and fill in whatever `fakeStore` method the priming read
lands on.

## Docs

- **New ADR** `docs/adr/2026-08-06-the-waker-seeds-before-start-returns.md`:
  the context is the race and why it was tolerable, the decision is
  subscribe-then-seed inside `Start`, the consequences are a startup that pays
  two reads and a `Start` that still cannot fail on a store error. It supersedes
  the "narrows to first run only" paragraph in the durable-cursor ADR's
  Consequences — say so there rather than editing that ADR's record, and note
  that the two-reads-in-the-critical-section hesitation it recorded is what this
  decision accepts.
- **`docs/adr/README.md`** — index the new ADR next to the other waker entries.
- **`docs/TODO.md`** — delete the "waker's startup seed race" entry
  (`:81-125`). Its closing **Tripwires** paragraph names four tests as the
  constraints on any fix; they are not deleted with it — `TestWakerRetriesSeedOnTheNextTick`
  and `TestWakerRetriesSeedOnAFailedCursorRead` still hold, and the new ADR's
  test list is what supersedes that paragraph. Say so in the ADR rather than
  dropping the reference. The *retention* entry below it stays: a cursor below
  the write log's horizon is a different, still-open hole.
- **`docs/reconcile-triggers.md`** — case 8's list of what it recovers names
  "a startup seed race" (`:449`); that is no longer one of the ways a wake is
  lost. Case 6's "Three things bypass the cursor" (`:369-378`) keeps **all
  three** bullets — the first two are about *resuming* rather than the race, and
  the third ("a wake that was queued but never delivered") is untouched by this
  change. Its "eager first pass back" sentence needs rewording, since the pass
  that drains a resumed backlog is now armed from the primed result.
- **`CLAUDE.md`**, the waker bullet: one sentence that the watermark and the
  subscription are both taken inside `Start`, so no write a caller can make
  after `Start` returns is below the watermark or unwoken, and that a failed
  seed does not abort startup. Link the new ADR.
- This spec file is working material; the ADR is the durable record of the
  decision, and the spec is not maintained after the change lands.

## Out of scope

- The write-log **horizon** gap (a resumed cursor below what retention trimmed)
  — separate `docs/TODO.md` entry, separate fix, unchanged by this.
- Making the waker a guarantee. It stays an optimisation over the
  stale-dependents pass; this closes a startup window in that optimisation and
  changes nothing about the backstop.
- Any ordering between the *other* startup goroutines. They remain independent.
