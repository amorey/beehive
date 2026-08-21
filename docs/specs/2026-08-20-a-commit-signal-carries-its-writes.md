# A commit signal carries its writes

- **Status:** Proposed. The largest change in this set, and the one that most
  changes the shape of the code.
- **Date:** 2026-08-20
- **Depends on:** one beehive per store, now enforced within the process — see [the sole-writer ADR](../adr/2026-08-05-one-process-one-beehive-sole-writer.md).
  Supersedes
  [the floor-tick gate](2026-08-20-the-tail-answers-its-floor-tick-from-memory.md).

## Why

Today a commit publishes a bare fact: *this kind was written*
(`signalKindWritten`, `beehive.go:604`). Each consumer then reads the store to
learn what changed:

- the kind's watch tailer reads `MaxVersion`, then `ListSince`;
- the dependency waker reads `ListSinceAll`.

Both re-read a write log that this process appended, moments earlier, from data
it had in hand. The signal carries no value deliberately — "a burst of writes to
one kind collapses into one" — and that collapsing is worth keeping. What is not
worth keeping is throwing the entries away and reading them back.

## The change

The signal carries the write log entries the transaction appended:
`(id, resource_version, op)`, per kind.

The tailer's steady state becomes: receive entries, coalesce to the latest per
object, one `Objects().ListByIDs` for current state, publish. No `MaxVersion`, no
`ListSince`.

**The log read stays** — for the snapshot at subscribe, for a resume's replay,
for a tailer that fell behind, and for the floor tick that still covers a failed
read and a retention trim. It stops being the steady-state path and becomes the
recovery path.

## The rules this rests on

**Level-triggered is not negotiable.** The entries route; they do not carry
state. `collectChanges` still reads current state through `ListByIDs`, and a
subscriber still sees the object as it is now, not as it was at the write. If the
entries ever start carrying payloads, this design has become change-triggered and
the whole architecture argument goes with it.

**Overflow falls back, it does not drop.** The buffer is bounded. When it
overflows, the tailer must fall back to reading the log from its cursor — which
is exactly today's path, so the fallback is not new code, it is the old code.
A silent drop here is a lost change with no backstop but the floor tick.

**Ascending delivery survives.** A batch is delivered ascending by resource
version, and a caller checkpoints on that. The entries arrive in commit order and
carry their versions, so the ordering is available — but the merge that coalesces
in place can leave a re-written object at its original queue position with a newer
version, which the drain already handles. Do not lose that handling.

## Edge cases the implementer would otherwise guess at

- **The entries must be captured where the log is appended**, not reconstructed
  by the caller. `appendWriteLog`, `appendWriteLogUpdates` and
  `appendWriteLogDelete` are the three sites, and the last one carries a row
  image the tailer needs for a `Deleted` change.

- **A rollback publishes nothing.** The capture rides the transaction and is
  discarded with it, which is what `AfterCommit` already guarantees. Nested
  frames unwind their own captures.

- **A delete entry's row image is the only state on the signal**, and it must be,
  because the row is gone and cannot be read. That is not a violation of the rule
  above — it is the same exception the log itself makes.

- **The waker subscribes across every kind**, the tailers to one each. One buffer
  per consumer, not one shared: they consume at different rates and a slow tailer
  must not stall the waker.

- **The cursor still advances from the entries.** A tailer that consumed entries
  up to version N must set its cursor to N, or a later fallback re-delivers them.

- **`ownerScoped`'s gate is set before a scoped subscriber registers and is never
  cleared.** Its soundness argument rests on "anything above its snapshot was read
  after the flag was set". Entries that were captured before the flag was set and
  delivered after it must not bypass that. This is subtle and needs its own test.

## Tests

In `objectswatch_test.go`:

- A write reaches a watcher with no `MaxVersion` and no `ListSince` call.
- A burst of writes to one kind is delivered in one batch, ascending.
- Overflowing the buffer falls back to the log and delivers everything, once.
- A delete is delivered with its row image.
- A retention trim below a tailer's cursor still ends its subscribers with
  `ErrWatchTooOld`.
- A subscriber that joins mid-burst gets a snapshot and no duplicate below it.
- An owner-scoped subscriber joining while entries are in flight sees no change
  published without an owner.

## On ship

ADR: **a commit publishes what it wrote**, superseding the "the signal is the
kind, never the object" paragraph in `signalKindWritten`'s doc and the
steady-state half of
[the shared tail ADR](../adr/2026-08-03-watch-shared-tail.md).

`CLAUDE.md`'s watch bullet describes the cursor-and-poll shape at length. It
needs rewriting rather than appending to: the cursor stays, the poll becomes the
recovery path.
