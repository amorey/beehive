# Specs

One file per piece of *planned* work, named `YYYY-MM-DD-name.md`. A spec is
written to be handed to whoever implements it: it states the decision, the exact
surface to add, every edge case the implementer would otherwise have to guess
at, and the tests that pin it.

A spec is not an [ADR](../adr/README.md). An ADR describes code that **exists**
and records why it is the way it is; a spec describes code that **does not exist
yet**. When a spec ships, fold whatever still governs live code into an ADR,
update `CLAUDE.md` and `README.md`, and delete the spec — git holds the text.

A spec is also not [`TODO.md`](../TODO.md). `TODO.md` holds gaps we have
decided *not* to close yet, and says what would make them worth doing. A spec is
work we have decided to do.

## In flight

Eighteen, from one audit of what the
[sole-writer constraint](../adr/2026-08-05-one-process-one-beehive-sole-writer.md)
buys and the code does not yet spend. Each is one PR. They are grouped by what
they have in common, not by the order they must land in; the dependencies each
spec names are the real constraint.

**Store mechanics.** No caches, no new invariants. Every one of these that has
been measured came back below its estimate, so price the rest before building
them.

1. [Cache prepared statements](2026-08-20-cache-prepared-statements.md) — ~2–6 µs
   off every statement, so the widest of these. The table serves the **read pool
   only**: −63% on a bare read, the writer untouched. That is what settles the
   question the spec used to turn on — the shared-cursor hazard lives inside a
   transaction on the writer, and the pool rules it out on the reader. One
   benchmark is owed before implementation; the spec says which.
2. [Conclude a pass in one transaction](2026-08-20-conclude-a-pass-in-one-transaction.md)
   — proposed; ~20 µs a pass, and it changes three failure arguments. Unmeasured.

**Idle drivers.** Six loops that query on a cadence whether or not anything
changed. One shared mechanism, then five small gates.

3. [Measure what an idle beehive costs](2026-08-20-measure-what-an-idle-beehive-costs.md)
   — the baseline the rest of this group moves.
4. [A write mark per kind](2026-08-20-a-write-mark-per-kind.md) — the mechanism,
   and the owed pass as its first consumer.
5. [Gate the stale-dependents pass](2026-08-20-gate-the-stale-dependents-pass.md)
6. [The tail answers its floor tick from memory](2026-08-20-the-tail-answers-its-floor-tick-from-memory.md)
    — superseded by 15; do one or the other.
7. [Hold the deletion-pending set in memory](2026-08-20-hold-the-deletion-pending-set-in-memory.md)
8. [Gate the owed-count reclaim](2026-08-20-gate-the-owed-count-reclaim.md) — the
    only write an idle beehive makes.
9. [Gate the retention and free-page sweeps](2026-08-20-gate-the-retention-and-free-page-sweeps.md)

**A pass writes less.** Independent of everything else, and the best ratio in the
set.

10. [A pass skips a condition write it can see is a no-op](2026-08-20-a-pass-skips-a-condition-write-it-can-see-is-a-no-op.md)

**In-memory indexes.** These change what breaking the sole-writer rule costs,
from latency to wrong answers. 11 gates the rest.

11. [Enforce one process, one beehive](2026-08-20-enforce-one-process-one-beehive.md)
    — a decision, not an optimization.
12. [A reverse dependency index](2026-08-20-a-reverse-dependency-index.md)
13. [A repeat AddDependency writes nothing](2026-08-20-a-repeat-add-dependency-writes-nothing.md)
14. [Cache the latest event run](2026-08-20-cache-the-latest-event-run.md)

**A commit publishes what it wrote.** The largest structural change, and the one
that makes the steady state store-free.

15. [A commit signal carries its writes](2026-08-20-a-commit-signal-carries-its-writes.md)
16. [An event signal carries its run](2026-08-20-an-event-signal-carries-its-run.md)
17. [The waker wakes from memory](2026-08-20-the-waker-wakes-from-memory.md)

**Cleanup.**

18. [Collect without a transaction it does not need](2026-08-20-collect-without-a-transaction-it-does-not-need.md)

Three things the audit found and deliberately left without a spec: a name-to-id
map, an object row cache, and dropping the conditions read from a spec write's
return. The first two are poor trades — unbounded memory for one indexed seek, and
a stale-read failure mode this design has been careful to make impossible. The
third needs an API decision first. They belong in [`TODO.md`](../TODO.md) if they
are worth recording at all.

## Closed

**A read that groups is a read transaction** shipped: four grouped reads moved to
the reader, so a watch's opening snapshot stops scaling with write pressure.
→ [ADR](../adr/2026-08-20-a-read-that-groups-is-a-read-transaction.md)

**A spec write writes before it reads** was measured and declined: an `UPDATE`
that matches no row costs more than the `SELECT` it would replace, so folding the
read into the write loses on the converged path — the steady state.
→ [ADR](../adr/2026-08-20-a-spec-write-reads-before-it-writes.md)

**Reserve resource versions in blocks** shipped: the counter is drawn once per
block and versions are handed out from memory, taking ~26 µs off every write.
→ [ADR](../adr/2026-08-20-reserve-resource-versions-in-blocks.md)

**Reads get their own connections** shipped: a writer pool of one and a
read-only pool of N, so reads no longer queue behind writes.
→ [ADR](../adr/2026-08-20-reads-get-their-own-connections.md)

[#126](https://github.com/amorey/beehive/issues/126) produced three, all closed:
[a spec write refuses a deletion-pending
row](../adr/2026-08-19-a-spec-write-refuses-a-deleting-row.md) and [a name-keyed
CreateOrUpdate](../adr/2026-08-19-a-name-keyed-create-or-update.md) shipped, and
the probe between them was [measured and
dropped](../adr/2026-08-19-a-spec-write-takes-its-transaction-unconditionally.md).
