# Measure what an idle beehive costs

- **Status:** Planned. A measurement, not a change. Wanted before the gating
  specs, which have nothing to prove without it.
- **Date:** 2026-08-20
- **Depends on:** nothing.

## Why

Six drivers run on a cadence whether or not anything changed. Nobody has ever
counted what that costs, so the six gating specs after this one have no baseline
to move and no tripwire to protect.

By inspection, a beehive with five registered kinds and nothing happening issues
roughly this per minute:

| Driver | Cadence | Queries per minute |
|---|---|---|
| owed pass, per kind | 30s | 2 × 5 × 2 = 20 |
| GC: deletion-pending listing | 30s | 2 |
| GC: owed-count reclaim | 30s | 2 (write transactions) |
| GC: free-page sweep | 30s | 4 (two `PRAGMA`s each) |
| stale-dependents pass | 60s | 2 |
| watch tail floor tick, per watched kind | 30s | 2 per kind |

Around forty statements a minute, forever, to learn that nothing happened. Two
of them are write transactions. The number is not alarming on a server; it is the
whole story on the battery-powered deployment `examples/lowpower` exists for.

## The change

A benchmark that counts store calls rather than time:

```go
// BenchmarkIdleDrivers reports the store work a beehive with no traffic does.
// ns/op is not the interesting number here — the counters are.
func BenchmarkIdleDrivers(b *testing.B) { ... }
```

It starts a beehive over a counting store, holds it for a fixed number of driver
cycles with the cadences shrunk, and reports per-driver counts as custom metrics
(`b.ReportMetric`): queries, write transactions, rows scanned.

Cases: 1, 5 and 16 registered kinds; with and without a watch on each kind; with
and without objects in the store (an empty store and a thousand settled objects
scan differently).

## Edge cases the implementer would otherwise guess at

- **Time, not iterations.** These drivers are paced by wall clock. Drive them by
  shrinking every interval to milliseconds and running a fixed number of cycles,
  and report per cycle. `b.N` is the cycle count.

- **Counting belongs in the test store, not in `sqlite`.** Wrap `Store` with a
  counter in `testutils_test.go`. Adding counters to the sqlite store would put
  measurement in the production path.

- **Attribute by driver.** A bare total cannot show which gate worked. Tag calls
  with the driver that made them — a context value set where each loop starts is
  enough, and is test-only.

- **The GC sweeper's write transaction is the number to watch.** It is the only
  write an idle beehive makes, and it is the one most obviously wrong.

- **This benchmark must stay green while every gate lands**, so keep it in terms
  of counts per cycle rather than absolute totals.

## Tests

The benchmark is the test. Add one assertion-shaped test beside it —
`TestIdleBeehiveMakesNoWrites` — for after the gates land. It fails today, which
is the point; skip it with a pointer at the gating specs until
[the owed-count reclaim gate](2026-08-20-gate-the-owed-count-reclaim.md) lands.

## On ship

No ADR. Put the measured table in `docs/TODO.md` beside the page-cache item,
which is the other place that wants to know what the drivers cost at rest.

Name the file `beehive_bench_test.go`, mirroring `beehive.go` per the convention
in `CLAUDE.md`.
