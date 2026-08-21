# Gate the retention and free-page sweeps

- **Status:** Planned.
- **Date:** 2026-08-20
- **Depends on:** [a write mark per kind](2026-08-20-a-write-mark-per-kind.md).

## Why

Three of the five GC sweeps query on every tick even when there is provably
nothing to do:

- **`eventRetentionSweep`** — runs its candidate query when a bound is
  configured, whether or not an event has been written since the last sweep.
- **`writeLogRetentionSweep`** — the same, and it is on by default (24h per
  kind), so every beehive pays it.
- **`freePagesSweep`** — reads `PRAGMA page_count` and `PRAGMA freelist_count`
  before its floor test, twice a tick, forever. The freelist can only grow when
  something is deleted.

## The change

Three conditions, all answerable from memory:

| Sweep | Runs when |
|---|---|
| event retention | an event has been added since the last sweep |
| write log retention | a write log entry has been appended since the last sweep |
| free pages | a sweep has deleted rows since the last release |

The first two ride the existing signals — `signalEventsWritten` and
`signalKindWritten`. The third rides the row counts the two retention sweeps and
the deletion sweep already return.

## The rule this rests on

**Retention is also time-based.** `maxAge` trims by age, so a timeline with no
new events still becomes trimmable as the clock moves. Gating purely on "has
anything been written" would freeze a quiet timeline above its age bound forever.

So the gate is: skip only when **nothing has been written since the last sweep
and no age bound is configured**. With `maxAge` set, the sweep runs on its
cadence as it does today. With only a count bound — a ring of N runs per timeline,
or N entries per kind — nothing can become trimmable without a write, and the
gate is exact.

This is the trap in this spec. An implementer who gates both bounds the same way
introduces a silent retention failure that no test with a fast clock would catch.

## Edge cases the implementer would otherwise guess at

- **The free-page floor stays.** `freePagesRelease` already declines under an
  absolute page count and a fraction of the file. The gate sits in front of the
  two `PRAGMA` reads, so the floor is evaluated only when a delete could have
  moved it.

- **`ReclaimSpace` acquires its own connection** (`sqlite/store.go:133`). Gating
  in `freePagesSweep`, above the store, avoids the acquisition entirely.

- **A deleted object frees pages too**, not just a retention trim. Feed the gate
  from `gcCollect`'s physical delete as well as from the two sweeps.

- **`auto_vacuum=NONE` releases nothing.** On such a database the sweep already
  returns 0 forever; the gate makes it stop reading pragmas to find that out.
  Behaviour is unchanged either way.

- **The stale comment.** `freePagesRelease` explains a negative release count
  with "another writer freed more than the drain took". There is no other writer.
  Keep the guard — the two reads are advisory, as `ReclaimSpace`'s own doc says —
  and fix the reason.

## Tests

In `gc_test.go`:

- With a count bound only: two sweeps with no event in between run the candidate
  query once.
- With an age bound: two sweeps with no event in between both run. **This is the
  test that protects the rule above.**
- The write log retention sweep behaves the same way on both bounds.
- No delete since the last release means no `PRAGMA` read; a delete makes the
  next sweep read.
- A collected object un-gates the free-page sweep even when no retention trim
  ran.

## On ship

Point at the ADR [a write mark per kind](2026-08-20-a-write-mark-per-kind.md)
ships. Add the count-bound/age-bound distinction to
[the event retention ADR](../adr/2026-08-06-event-retention-is-a-ring-per-timeline.md);
it is a property of the bounds that ADR defines, and it belongs beside them.
