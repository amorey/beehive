# The object tail floors the gap between wake-driven drains

- **Status:** Accepted — implemented in `objectswatch.go`, `beehive.go`,
  `options.go`.
- **Date:** 2026-08-05

## Context

[One tailer per kind](2026-08-03-watch-shared-tail.md) reads the write log when
a commit wakes it, above a floor tick. Nothing bounded how soon the next drain
could start: a commit landing during a drain refills the wake slot, so the loop
went straight around. Under a sustained write stream to a watched kind the
tailer read as fast as the store could answer, and every read held the single
connection the writers needed.

The cost was never a query count. The position read already gates a quiet wake
behind one scalar, and page batching means a burst of 512 writes is read by one
drain — so the cost *per write* falls as the rate rises. What does not fall is
the fraction of time the tailer is running, and that is what competes with
writers.

Two things were unbounded, and fixing one would have left the problem: how often
a drain starts, and how long one runs.

## Decision

**Take the [dependency waker's](2026-08-05-a-commit-wakes-the-dependency-waker.md)
pacing — a `rategate` floor on drain starts plus a page budget — and none of its
idle bookkeeping.**

The waker is purely wake-driven, so it needs `wakeIdle`, `armedFor` and
`nextTimer` to stop an already-ready timer driving a pass nobody asked for. The
tailer keeps its floor tick and re-arms unconditionally, and a gate refusal
returns the *remaining* window, which shrinks as wakes arrive inside it. So a
commit stream cannot push the deadline out, and the loop needs no memory of what
it armed.

The gate sits at the top of `pass`, so it floors *every* drain start, not only a
wake-driven one: a quiet floor tick takes the slot too, and a commit landing just
after one waits out the rest of the window. That is why the floor tick is the
thing the interval must stay far below — at 30s against 100ms it costs nothing,
but the two are not independent.

`withWatchScanMinInterval` is unexported like every other watch cadence, and
`<= 0` turns the floor off — the floor tick and the wake still cover
correctness, so a disabled throttle is unpaced, not unsound.

### The budget is two pages, and the waker's four does not transfer

A tail page is 512 rows against the waker's 256, and costs a batched object read
and a fan-out on top of the listing.
`BenchmarkTailerDrainRateUnderSustainedWrites` measures ~3.6ms a page, linear in
the budget: 3.6ms at one page, 7.2ms at two, 14.6ms at four.

The interval is anchored first, at 100ms — it is the added latency a busy watch
pays, and a caller who opened a watch asked for latency. The budget is then the
largest whose full-budget drain keeps `D / (D + I)` under 10%: two pages, at
6.7%.

### `pass` reports "finished" as a bool, not a sentinel duration

A `tailStop = -1` would collide with a real answer, since `time.Duration(-1)` is
one nanosecond and a disabled throttle returns its interval verbatim — so
`withWatchScanMinInterval(-1)` plus a backlog would have ended every subscriber
on the kind mid-drain. `waker.pass` has the same latent collision between
`wakeIdle` and `withWakeScanMinInterval(-1)`; it is unreachable today because
only tests set that interval, and it is recorded here rather than fixed.

### The resume path subtracts the drain it just ran

The gate admits before the drain, but the *re-arm* decides the next start and
happens after it. Returning the whole interval there would pace a resume
end-to-start, draining a backlog at `budget / (I + D)` instead of `budget / I` —
a 2× loss when a drain runs as long as the window, on exactly the path the
budget exists to bound. `pass` reads the clock again and re-arms for what is
left. `waker.pass` returns a bare `Interval()` on its `scanMore` path and so is
end-to-start on resume; unchanged, since its budget and cadence were measured
together as they stand.

## Consequences

`BenchmarkWritesUnderWatch` reports both settings in one run. Writers behind one
watched kind go from ~224µs to ~175µs a write (p50 204µs → 161µs), and behind 16
watched kinds from ~332µs to ~186µs (p50 274µs → 162µs) — against a ~175µs
no-watcher baseline. A watched kind now costs its writers close to what an
unwatched one does. The p99 is noisy at this sample size and moves in neither
direction consistently.

A busy watch pays up to 100ms of added latency; an idle-to-write transition pays
none, since `rategate` holds nothing for a key not admitted within the interval.
That trade is acceptable here in a way it would not be for a driver, because a
tailer is demand-scoped: it exists only while something watches.

A retry can be refused by the gate — `watchRetryBase` is 100ms and the gate is
checked first — so the retry ladder is effectively floored at the throttle. The
waker accepts the same.

The tests run with the throttle off (`newTestBeehive` prepends
`withWatchScanMinInterval(0)`, so a test's own option still wins), and the five
watch tests that call `New` directly pass it themselves. This was necessary: the
closing TODO item claimed "a throttle under the tests' failsafe timeouts would
trip none of them", but a 100ms floor sits far above the `fastTick` floor
interval the watch suite runs on, so an unmitigated throttle would have quietly
turned floor-tick tests into throttle tests.
