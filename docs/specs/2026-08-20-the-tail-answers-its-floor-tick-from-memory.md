# The tail answers its floor tick from memory

- **Status:** Planned. Superseded if
  [a commit signal carries its writes](2026-08-20-a-commit-signal-carries-its-writes.md)
  lands first — do one or the other, not both.
- **Date:** 2026-08-20
- **Depends on:** [a write mark per kind](2026-08-20-a-write-mark-per-kind.md).

## Why

Every watch tailer's drain starts with a position read
(`objectswatch.go:574`):

```go
at, err := t.bh.store.ObjectWrites().MaxVersion(ctx, t.gk)
if at <= t.cursor { return false, nil }
```

That is one query per watched kind per floor tick — twice a minute per kind — and
one per commit wake. It exists to answer "did anything change", which is exactly
what a sole writer already knows.

## The change

The tailer keeps the kind's mark from `writeMarks` beside its cursor. On a tick
or a wake it compares marks first, and only reads `MaxVersion` when the mark has
moved.

The store call stays. It is still the position of record, and the drain still
uses it: what changes is that a quiet tick no longer makes it.

## The rule this rests on

Two things move the position, and beehive does both:

1. **A write**, which reaches `signalKindWritten` and moves the mark.
2. **Retention**, which raises the horizon that `MaxVersion` folds in
   (`sqlite/store.go:3113`) — because a trimmed-empty kind must not read below a
   tail parked higher.

The GC sweeper runs in this process, so a trim moves the mark too. **That is the
part an implementer will miss**: gating only on writes leaves a tailer parked
above a trimmed horizon and blind to it until something is written. `ObjectWrites().Sweep`
must move the mark of every kind it trimmed.

## Edge cases the implementer would otherwise guess at

- **A cold tailer reads.** A tailer built with no mark reads `MaxVersion` once,
  which is also its snapshot position. Unchanged.

- **The floor tick keeps its other job.** It also retries after a failed read.
  A tailer in backoff must tick and retry regardless of the mark — gate the
  position read, not the tick.

- **`ErrWatchTooOld` must still be reachable.** It fires when the cursor falls
  below the horizon. Since a trim moves the mark, the next tick reads and finds
  it. Pin this: it is the failure path that silently stops working if the trim
  rule above is missed.

- **The eager first drain stays eager.** `internal/rategate` lets the first drain
  after a quiet period run immediately; that behaviour is unrelated and untouched.

## Tests

In `objectswatch_test.go`:

- Two floor ticks with no write in between issue no `MaxVersion` call.
- A write between ticks makes the next tick read and deliver.
- A retention trim below a parked tailer's cursor still ends its subscribers with
  `ErrWatchTooOld` — `TestTailerResetsWhenItsCursorIsTrimmed` extended to sit
  through a gated tick first.
- A tailer that failed a read retries on the next tick with no write in between.

## On ship

Point at the ADR [a write mark per kind](2026-08-20-a-write-mark-per-kind.md)
ships. Add the trim-moves-the-mark rule to
[the object tail's throttling ADR](../adr/2026-08-05-the-object-tail-throttles-its-drains.md),
which is where a reader is already thinking about what a tick is for.
