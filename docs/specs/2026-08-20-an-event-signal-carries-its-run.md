# An event signal carries its run

- **Status:** Proposed.
- **Date:** 2026-08-20
- **Depends on:** [a commit signal carries its writes](2026-08-20-a-commit-signal-carries-its-writes.md),
  whose buffer and overflow rule this reuses.

## Why

`signalEventsWritten` wakes an object's event readers with a bare id
(`beehive.go:620`), and each reader then reads the store
(`eventswatch.go:284`):

```go
page, trimmed, err := r.bh.store.Events().ListSince(ctx, r.id, r.cfg.query.Category, r.cursor, r.pageCap)
```

The write that woke them knew exactly what it did: which run it extended or
started, at which version, in which category. It is thrown away and read back.

Unlike the object tail there is no sharing here — one reader per watch, by design,
because the read is already per object and already indexed. So the read is cheap.
It is also entirely avoidable.

## The change

The signal carries the run the write produced: the run's id, category,
`(type, reason)`, the new count and window end, and the version it took. A reader
whose cursor is exactly below it delivers from the signal and advances.

Anything else — a gap, a cursor further behind, a category mismatch, an
overflowed buffer — falls back to `Events().ListSince`, which is today's path.

## The rules this rests on

**A run is a mutable row.** An extend rewrites `count`, `last_at`, `message`,
`detail` and `resource_version` on an existing row. So the signal carries the run
*as it now is*, not a delta, and a reader that has already delivered that run
replaces its copy. This is why the signal can carry state where the object tail's
cannot: the event log is what the reader is streaming, not a pointer at something
to read.

**The stream is unbuffered on purpose.** A consumer that stops reading pins the
cursor, which is what makes `ErrWatchTooOld` reachable live. That must not change:
the signal buffer sits in front of the reader, not in front of the consumer.

## Edge cases the implementer would otherwise guess at

- **The category filter is the reader's, not the writer's.** A signal for a
  category this reader does not watch is dropped by the reader, and must still
  advance nothing.

- **`checkExists` and the horizon are not on the signal.** A reader resuming
  below the horizon is refused with `ErrWatchTooOld`, and a collected object ends
  its streams with `ErrNotFound`. Both come from the store read. A signal-driven
  delivery cannot answer either, so it must only be taken when the reader's cursor
  is exactly one behind — where there is nothing to refuse.

- **`Retention` on the stream is configuration**, not a per-run fact. Unchanged.

- **The floor tick stays.** It covers a failed read and a retention trim, exactly
  as it does for the object tail.

- **`AdminClient.AddEvent`** publishes the same signal, since it goes through the
  same store call.

## Tests

In `eventswatch_test.go`:

- An event reaches a watcher with no `ListSince` call.
- An extend delivers the run with its new count, replacing the reader's copy.
- A reader watching one category ignores another's signal.
- A reader whose cursor is more than one behind falls back and delivers
  everything in order.
- A retention trim below a reader's cursor still ends it with `ErrWatchTooOld`.
- A collected object still ends its readers with `ErrNotFound`.
- A consumer that stops reading still pins the cursor.

## On ship

Fold into
[the commit-signal ADR](2026-08-20-a-commit-signal-carries-its-writes.md) if both
land together; otherwise a short ADR of its own recording the "a run is a mutable
row" argument, which is the part that is different from the object tail.

Amend [the events cursor ADR](../adr/2026-08-05-events-get-a-cursor-and-a-commit-wake.md):
the cursor and the wake are unchanged, and the wake now carries the run.
