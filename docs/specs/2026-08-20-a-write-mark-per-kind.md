# A write mark per kind

- **Status:** Planned. Carries the shared mechanism for the five gating specs
  after it, and its first consumer.
- **Date:** 2026-08-20
- **Depends on:** [the idle benchmark](2026-08-20-measure-what-an-idle-beehive-costs.md),
  which is what shows this works.

## Why

The owed pass runs twice a minute per registered kind and asks the store two
questions:

- `Objects().ListUnsettledIDs(gk)` — anything unsettled?
- `ReconcileOwed().ListIDs(gk)` — anything owed?

Both answers can only change when something writes to that kind. Beehive is the
store's only writer
([ADR](../adr/2026-08-05-one-process-one-beehive-sole-writer.md)), so it already
knows whether anything did. On an idle five-kind beehive that is twenty queries a
minute to learn nothing.

The same knowledge gates four more drivers, so this spec builds it once.

## The change

A small in-memory record of what this process has committed, per kind:

```go
// writeMarks records the highest resource version this process has committed
// per kind, and whether the mark has moved since a given driver last looked.
// Sound only because beehive is the store's only writer: a mark that has not
// moved proves nothing was written.
type writeMarks struct { ... }
```

It is fed where the store already announces a commit — `signalKindWritten`
(`beehive.go:604`) — so there is no new hook and no new call site. Every write
path already goes through it.

A driver asks one question:

```go
// changedSince reports whether gk has been written since mark, and returns the
// mark to pass next time. An unseeded kind always reports true.
func (w *writeMarks) changedSince(gk GroupKind, mark uint64) (changed bool, now uint64)
```

The owed pass calls it and skips both listings when the answer is no.

## The rule this rests on

**A cold process knows nothing and must scan.** The marks start empty, so the
first pass of every kind runs. That is what makes the gate safe: everything the
durable records exist for — a restart resuming owed work, a crash losing an
in-memory enqueue — happens on a process that has no marks yet.

**The cadence does not change.** No driver is removed, no interval moves, no
backstop is weakened. The tick still fires; it just stops asking a question it
knows the answer to.

## Edge cases the implementer would otherwise guess at

- **A counter, not a version.** Use a monotonic counter per kind rather than the
  resource version: the mark only has to answer "did anything happen", and a
  counter cannot be confused with a cursor by a later reader.

- **Mark before the query, compare after.** Read the mark, then run the listing.
  Marking after would drop a write that landed during the scan. This is the
  ordering bug the whole thing dies on, and it needs a comment.

- **`signalKindWritten` fires after commit**, which is the right moment: a
  rolled-back write publishes nothing and must not move the mark.

- **The GC's own writes count.** A cascade marks children of other kinds, and
  `gcCollect` already signals each child's kind. Nothing to add — but the
  implementer should confirm it rather than assume it.

- **The pass client's writes count.** Status, conditions and events all reach
  `signalKindWritten`. An event does not bump the object's version, so it must
  not move the object mark — check what each signal carries before wiring it.

- **Live in `Beehive`, not in the store.** These marks gate drivers, and drivers
  are beehive's. A third-party `Store` is unaffected.

## Tests

In `reconciler_test.go`:

- Two owed passes with no write in between issue the listings once.
- A write between two passes makes the second one list.
- A fresh beehive over a store with unsettled rows lists on its first pass —
  the restart case, and the one that matters.
- A write landing *during* a pass is not swallowed: the next pass lists.
- A write to another kind does not un-gate this one.

In `beehive_bench_test.go`: `BenchmarkIdleDrivers` shows the owed pass at zero
queries per cycle for an idle kind.

## On ship

ADR: **an idle driver does not query**, stating the mechanism, the cold-process
rule, and the mark-before-query ordering. The four gating specs that follow point
at it rather than re-arguing it.

`CLAUDE.md`'s drivers bullet says every driver over the store is a periodic scan.
That is still true; add that a scan whose kind has not been written is skipped.
