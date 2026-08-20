# A spec write writes before it reads

- **Status:** Measured, and **recommended for decline**. It saves 7 µs on a spec
  write that changes something and costs 62 µs on one that does not, and the
  converged write is the steady state. See *What it is worth*. The prototype is
  not kept; git holds it.
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

## What it is worth, measured

A prototype of the one-statement path over the id-keyed resolver, on disk, medians
of fifteen alternating runs a side. It passed the whole suite bar the draw-site
list, so the semantics below are the real ones, not an approximation:

| `BenchmarkConvergedSpecWrite`, `spec=0KB/conditions=0` | now | write-first | |
|---|---|---|---|
| `changed` — the write happens | 148.9 µs | 141.6 µs | **−5%** |
| `txn` — converged, nothing written | 41.4 µs | 103.7 µs | **+151%** |

The parts, measured separately inside a transaction against the real schema:

| | |
|---|---|
| empty `Within` | 5.2 µs |
| the row read this removes | +20.4 µs |
| an `UPDATE` that matches no row | +52.4 µs |

**A write statement that changes nothing is two and a half times the cost of the
read it replaces.** That is the whole result. The read is cheap because it runs on
the writer's warm page cache inside the transaction — far cheaper than the 36 µs a
standalone `Objects().Get` costs on the read pool, which is the figure that made
this look worth doing.

So the trade is 7.3 µs saved per changed write against 62.3 µs added per converged
one: it pays only where changed writes outnumber converged ones by more than
**8.5 to 1**. Beehive is built for the opposite ratio. A controller re-applying its
own spec is the steady state, and the byte compare skipping it is
[what stops that waking the controller forever](../adr/2026-08-18-beehive-owns-the-generation-handshake.md).

Nothing rescues it. The converged path cannot skip the `UPDATE` without the read
that the change exists to remove, and no caller signals which case it is in — a
`ControllerClient` holds a status baseline it can compare in memory
([ADR](../adr/2026-08-19-a-pass-skips-a-status-write-it-can-see-is-a-no-op.md)),
but `UpdateSpec`'s callers are clients and hold nothing.

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

## On decline

ADR: **a spec write reads before it writes**, recording the measurement above
beside its sibling,
[the declined probe](../adr/2026-08-19-a-spec-write-takes-its-transaction-unconditionally.md).
The pair says the same thing from both directions: that probe added a read in
front of writes that happen anyway and lost; this removes a read from writes that
happen and loses on the writes that do not. The number worth keeping is the one
that will tempt the next reader — an `UPDATE` matching no row costs more than the
`SELECT` it would replace.

If it is built anyway, everything below still applies.
