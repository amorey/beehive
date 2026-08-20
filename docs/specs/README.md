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

Twenty-one, from one audit of what the
[sole-writer constraint](../adr/2026-08-05-one-process-one-beehive-sole-writer.md)
buys and the code does not yet spend. Each is one PR. They are grouped by what
they have in common, not by the order they must land in; the dependencies each
spec names are the real constraint.

**Store mechanics.** No caches, no new invariants. Land these first: they are the
widest wins and the least argued.

1. [Cache prepared statements](2026-08-20-cache-prepared-statements.md) — ~6 µs off
   every statement in the system.
2. [A read-only transaction](2026-08-20-a-read-only-transaction.md) — five grouped
   reads stop taking the write lock.
3. [Reserve resource versions in blocks](2026-08-20-reserve-resource-versions-in-blocks.md)
   — ~16 µs off every write.
4. [Conclude a pass in one transaction](2026-08-20-conclude-a-pass-in-one-transaction.md)
   — proposed; changes three failure arguments.
5. [A spec write writes before it reads](2026-08-20-a-spec-write-writes-before-it-reads.md)
   — one statement instead of two on the changed path.

**Idle drivers.** Six loops that query on a cadence whether or not anything
changed. One shared mechanism, then five small gates.

6. [Measure what an idle beehive costs](2026-08-20-measure-what-an-idle-beehive-costs.md)
   — the baseline the rest of this group moves.
7. [A write mark per kind](2026-08-20-a-write-mark-per-kind.md) — the mechanism,
   and the owed pass as its first consumer.
8. [Gate the stale-dependents pass](2026-08-20-gate-the-stale-dependents-pass.md)
9. [The tail answers its floor tick from memory](2026-08-20-the-tail-answers-its-floor-tick-from-memory.md)
   — superseded by 18; do one or the other.
10. [Hold the deletion-pending set in memory](2026-08-20-hold-the-deletion-pending-set-in-memory.md)
11. [Gate the owed-count reclaim](2026-08-20-gate-the-owed-count-reclaim.md) — the
    only write an idle beehive makes.
12. [Gate the retention and free-page sweeps](2026-08-20-gate-the-retention-and-free-page-sweeps.md)

**A pass writes less.** Independent of everything else, and the best ratio in the
set.

13. [A pass skips a condition write it can see is a no-op](2026-08-20-a-pass-skips-a-condition-write-it-can-see-is-a-no-op.md)

**In-memory indexes.** These change what breaking the sole-writer rule costs,
from latency to wrong answers. 14 gates the rest.

14. [Enforce one process, one beehive](2026-08-20-enforce-one-process-one-beehive.md)
    — a decision, not an optimization.
15. [A reverse dependency index](2026-08-20-a-reverse-dependency-index.md)
16. [A repeat AddDependency writes nothing](2026-08-20-a-repeat-add-dependency-writes-nothing.md)
17. [Cache the latest event run](2026-08-20-cache-the-latest-event-run.md)

**A commit publishes what it wrote.** The largest structural change, and the one
that makes the steady state store-free.

18. [A commit signal carries its writes](2026-08-20-a-commit-signal-carries-its-writes.md)
19. [An event signal carries its run](2026-08-20-an-event-signal-carries-its-run.md)
20. [The waker wakes from memory](2026-08-20-the-waker-wakes-from-memory.md)

**Cleanup.**

21. [Collect without a transaction it does not need](2026-08-20-collect-without-a-transaction-it-does-not-need.md)

Three things the audit found and deliberately left without a spec: a name-to-id
map, an object row cache, and dropping the conditions read from a spec write's
return. The first two are poor trades — unbounded memory for one indexed seek, and
a stale-read failure mode this design has been careful to make impossible. The
third needs an API decision first. They belong in [`TODO.md`](../TODO.md) if they
are worth recording at all.

## Closed

[#126](https://github.com/amorey/beehive/issues/126) produced three, all closed:
[a spec write refuses a deletion-pending
row](../adr/2026-08-19-a-spec-write-refuses-a-deleting-row.md) and [a name-keyed
CreateOrUpdate](../adr/2026-08-19-a-name-keyed-create-or-update.md) shipped, and
the probe between them was [measured and
dropped](../adr/2026-08-19-a-spec-write-takes-its-transaction-unconditionally.md).
