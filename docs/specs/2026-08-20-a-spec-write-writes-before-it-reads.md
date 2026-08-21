# A spec write writes before it reads

- **Status:** Planned.
- **Date:** 2026-08-20
- **Depends on:** nothing.

## Why

`updateSpec` (`sqlite/store.go:1377`) reads the row, compares in Go, then writes:

1. `SELECT` the whole row through the resolve.
2. compare version and bytes.
3. `UPDATE ... RETURNING` the whole row.
4. `SELECT` the conditions to attach to the returned row.

A write that changes something pays both the read and the write. The read exists
for three answers — is the row there, is it deletion-pending, are the bytes
already what we are writing — and SQLite can answer all three in the `WHERE`
clause of the write itself.

This is the mirror of the probe that was
[measured and declined](../adr/2026-08-19-a-spec-write-takes-its-transaction-unconditionally.md).
That one added a read in front of writes that happen anyway, and lost. This one
removes a read from writes that happen, and pays for itself whenever a write
changes something.

## The change

One statement on the common path:

```sql
UPDATE objects
   SET spec = ?, schema_version_spec = ?, generation = generation + 1,
       resource_version = ?, updated_at = ?
 WHERE id = ?
   AND "group" = ? AND kind = ?
   AND deletion_requested_at IS NULL
   AND (spec != ? OR schema_version_spec != ?)
RETURNING <objectColumns>
```

A row comes back: the write happened, and it is the row to return.

No row: read it, and say why. That read is the current resolve, and it decides
between `ErrNotFound`, `ErrWrongKind`, `ErrDeletionPending`, and "converged" —
which returns the stored row with `changed=false`.

## Edge cases the implementer would otherwise guess at

- **The version draw moves.** The version has to be drawn before the `UPDATE`
  that uses it, so a converged write now draws a version it does not use. That is
  a gap in the sequence, which is already allowed
  ([blocks](../adr/2026-08-20-reserve-resource-versions-in-blocks.md)), and the write log
  entry must be appended *after* the `UPDATE` reports a row — not before, or a
  converged write logs a write that never happened. This inverts the current
  order in `recordObjectWrite`, so this path cannot use it.

- **`stampVersion`'s rule must not move into SQL.** It refuses a downward version
  and is the one piece of the predicate with an error of its own. Keep it in Go:
  the fast path passes the already-stamped value and the `WHERE` compares against
  it, and the diagnostic read re-derives the error when the write matched nothing.

- **The deletion refusal keeps its documented order.** The contract on `Store`
  says the refusal is checked "before the schema stamp and the byte compare". In
  one statement there is no order, so the diagnostic read must apply it: a
  deletion-pending row is `ErrDeletionPending` even when the bytes also match.
  There is a test for exactly this; it must keep passing unchanged.

- **A blob comparison in the `WHERE` is a full-column read.** For a large spec
  that is the same read the current `SELECT` does, so it is not new cost. It is
  not cheaper either — the saving is the round trip, not the bytes.

- **`attachConditions` stays.** The returned row must carry conditions, as `Get`
  does. Removing that read is a separate question and belongs in `TODO.md`.

- **Both resolvers.** `UpdateSpec` keys on id, `UpdateSpecByName` on
  `(group, kind, name)`. The name form's `WHERE` replaces the id predicate; the
  kind gate is already in it.

## Tests

In `sqlite/store_test.go`, all of these exist for the current shape and must pass
against the new one:

- A changed write returns the row, `changed=true`, bumps generation once, appends
  exactly one write log entry.
- A converged write returns the stored row, `changed=false`, appends nothing,
  bumps nothing — and now also proves it burned at most one version.
- A deletion-pending row is refused even when the bytes match.
- A wrong-kind id is `ErrWrongKind`; a missing id is `ErrNotFound`.
- A downward schema version is refused, on both the changed and converged paths.

Add: a changed write issues one `UPDATE` and no preceding `SELECT`. Count
statements through a store seam, or the point of the change is untested.

## On ship

ADR: **a spec write writes before it reads**, as a companion to
[the declined probe](../adr/2026-08-19-a-spec-write-takes-its-transaction-unconditionally.md)
rather than a replacement — that ADR's decision (take the transaction
unconditionally) still stands, and this changes only what happens inside it.
Record the new `BenchmarkConvergedSpecWrite` numbers in both.
