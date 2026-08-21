# Gate the owed-count reclaim

- **Status:** Planned. The smallest change in this set, and the only one that
  removes a write.
- **Date:** 2026-08-20
- **Depends on:** [a write mark per kind](2026-08-20-a-write-mark-per-kind.md).

## Why

`reconcileOwedSweep` runs on every GC tick (`beehive.go:312`):

```sql
UPDATE objects SET reconcile_owed = 0
 WHERE reconcile_owed != 0 AND ("group", kind) NOT IN (VALUES ...)
```

It reclaims owed counts on kinds with no reconcile loop, which nothing else
drains. It runs unconditionally, so **an idle beehive issues a write transaction
twice a minute to update no rows**. That is the only write an idle beehive makes.

Two things stamp `reconcile_owed`: `Edges().Add` and the stale-dependents pass.
Both are in this process, and both know the kind they stamped.

## The change

A flag, set when either producer stamps a kind with no reconciler, cleared by the
sweep that reclaims it:

```go
// clientOnlyOwed is set when a kind with no reconcile loop gets an owed stamp.
// Only Edges().Add and the stale-dependents pass stamp, and both run here, so a
// clear flag means there is nothing to reclaim.
```

The sweep runs when the flag is set, and is skipped when it is not.

## The rule this rests on

The count is redundant. The
[reclaim ADR](../adr/2026-08-05-reclaim-a-client-only-owed-count.md) already
records why: the count is recoverable from the dependency watermark
`Edges().Add` clears, so a cursor-0 sweep in a later process re-derives it. A
skipped reclaim therefore costs nothing at all, not even latency — which makes
this the safest gate in the set.

**A cold process sweeps once.** Seed the flag as set at `Start`, so every process
reclaims whatever its predecessor left behind before it starts trusting the flag.

## Edge cases the implementer would otherwise guess at

- **Registration is frozen after `Start`**, so "has no reconcile loop" cannot
  change under the flag. `bh.registeredKinds()` is stable, which is what makes a
  per-kind decision safe to cache.

- **Set the flag at the stamp, not at the commit.** A rolled-back stamp leaves
  the flag set and costs one extra sweep that reclaims nothing. That is the cheap
  direction to be wrong in; the expensive one is a stamp that never gets swept.

- **The stale-dependents pass stamps `reconcile_owed` for what it finds** before
  enqueuing, and it lists only registered kinds — so in practice it cannot stamp
  a client-only kind. Wire it anyway: the listing's kind filter is a property of
  the call, not of the table, and a later change to either would strand counts
  silently.

- **Clear after the sweep succeeds**, never before. A failed sweep must leave the
  flag set.

## Tests

In `gc_test.go`:

- Two sweeps with no stamp in between issue one `UPDATE`.
- An edge added to a client-only kind's object makes the next sweep reclaim.
- An edge added to a registered kind's object does not.
- A failed sweep is retried on the next tick.
- A fresh beehive over a store holding a stale client-only count reclaims it on
  its first sweep.

In `beehive_bench_test.go`: `TestIdleBeehiveMakesNoWrites` — skipped when
[the idle benchmark](2026-08-20-measure-what-an-idle-beehive-costs.md) landed —
now runs.

## On ship

Point at the ADR [a write mark per kind](2026-08-20-a-write-mark-per-kind.md)
ships. Add two sentences
to [the reclaim ADR](../adr/2026-08-05-reclaim-a-client-only-owed-count.md): the
sweep is now flag-driven, and the redundancy it already argues for is what makes
skipping free.
