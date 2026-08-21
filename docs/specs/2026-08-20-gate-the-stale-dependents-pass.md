# Gate the stale-dependents pass

- **Status:** Planned.
- **Date:** 2026-08-20
- **Depends on:** [a write mark per kind](2026-08-20-a-write-mark-per-kind.md).

## Why

The stale-dependents pass runs every 60s and cannot be disabled. Each sweep
costs `GetLatestResourceVersion` plus `Dependencies().ListStaleSince`, a
three-way join over targets, edges and dependents with a `LEFT JOIN` onto the
watermarks (`sqlite/store.go:1106`). It is the most expensive periodic query in
the system.

A dependent can only become stale when its target is written. Beehive writes
every target, so a sweep that follows no write anywhere finds nothing, every
time.

## The change

Before the sweep, ask whether anything has been written since the last sweep that
reached the end of the listing. If not, skip both queries and wait for the next
tick.

The mark is store-wide, not per kind: an edge may point at a kind with no
controller, so any kind's write can make a dependent stale. `writeMarks` gets a
store-wide reading for this and for the waker.

## The rule this rests on

**Skip only from a sweep that finished.** The pass keeps a cursor and pages;
`ListStaleSince` advances it only when a sweep reaches the end. A sweep that
stopped on its page budget has work waiting below the mark, and gating the next
one would strand it. Gate on "the cursor is at the end **and** nothing has been
written since it got there".

**A cold process scans.** The cursor is process-local and never persisted, by
design ([ADR](../adr/2026-08-03-stale-dependents-cursor.md)), so a new process
re-derives from zero. That is unchanged: with no mark, the first sweep runs.

## Edge cases the implementer would otherwise guess at

- **`reconcile_owed` stamps do not move the mark.** This pass stamps what it
  finds before enqueuing. Those are its own writes, and treating them as a reason
  to sweep again would make it run forever.

- **The watermark writes of a pass do not either.** `WatermarkSet` writes
  `dependency_watermarks`, not an object, so it does not reach
  `signalKindWritten`. Confirm that; if it ever does, this gate spins.

- **A new edge must un-gate.** `Edges().Add` clears the dependent's watermark, so
  the dependent is stale against a target nobody wrote. `Edges().Add` does not
  bump a resource version — "a ref is not a field of the object" — so it may not
  move the mark today. It must, for this gate. Check it, and if it does not, move
  the mark explicitly there.

- **Retention does not matter here.** This pass reads current state, not the
  write log, so a trim cannot move its cursor.

## Tests

In `waker_test.go`:

- Two sweeps with no write in between run `ListStaleSince` once.
- A write to a target between sweeps makes the second one sweep.
- A new `depends_on` edge between sweeps makes the second one sweep, with no
  write to either endpoint. This is the test that catches the edge case above.
- A sweep stopped by its page budget is followed by another sweep, gate or no
  gate.
- A fresh beehive over a store with stale dependents sweeps on its first tick.

## On ship

Point at the ADR [a write mark per kind](2026-08-20-a-write-mark-per-kind.md)
ships rather than writing a second one. Amend
[the stale-dependents cursor ADR](../adr/2026-08-03-stale-dependents-cursor.md)
with one paragraph: the cursor is unchanged, and the sweep is now skipped when
nothing could have moved it.

`CLAUDE.md` calls this pass "60s, cannot be disabled". Still true. Add that a
sweep with no write behind it costs nothing.
