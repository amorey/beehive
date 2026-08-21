# A spec write reads before it writes

- **Status:** Accepted — the shape in `sqlite/store.go` (`updateSpec`), measured
  by `BenchmarkConvergedSpecWrite`. Retires
  `docs/specs/2026-08-20-a-spec-write-writes-before-it-reads.md`, which git holds.
- **Date:** 2026-08-20

## Context

`updateSpec` reads the row, compares in Go, then writes. The read exists for
three answers — is the row there, is it deletion-pending, are the bytes already
what we are writing — and SQLite can answer all three in the `WHERE` clause of
the write itself:

```sql
UPDATE objects
   SET spec = ?, schema_version_spec = ?, generation = generation + 1, ...
 WHERE id = ? AND "group" = ? AND kind = ?
   AND deletion_requested_at IS NULL
   AND (spec != ? OR schema_version_spec != ?)
RETURNING <objectColumns>
```

A row comes back: the write happened, and it is the row to return. No row: read
it, and say which of `ErrNotFound`, `ErrWrongKind`, `ErrDeletionPending`,
`ErrSchemaVersionDowngrade` or "converged" it was.

This is the mirror of the probe
[measured and declined](2026-08-19-a-spec-write-takes-its-transaction-unconditionally.md):
that one added a read in front of writes that happen anyway. This one removes a
read from writes that happen.

## Decision

No. A spec write keeps its read.

A prototype of the statement above, over the id-keyed resolver, on disk, medians
of fifteen alternating runs a side. It passed the whole suite bar the draw-site
list, so these are the real semantics rather than an approximation:

| `BenchmarkConvergedSpecWrite`, `spec=0KB/conditions=0` | now | write-first | |
|---|---|---|---|
| `changed` — the write happens | 148.9 µs | 141.6 µs | −5% |
| `txn` — converged, nothing written | 41.4 µs | 103.7 µs | **+151%** |

The parts, measured separately inside a transaction against the real schema:

| | |
|---|---|
| empty `Within` | 5.2 µs |
| the row read this removes | +20.4 µs |
| an `UPDATE` that matches no row | +52.4 µs |

**A write statement that changes nothing costs two and a half times the read it
would replace.** That is the result, and it is the number to remember: the
intuition that folding a read into a write must be cheaper is what makes this
change look free, and it is wrong on the path that matters.

The read is cheap because it runs on the writer's warm page cache inside the
transaction. A standalone `Objects().Get` on the read pool is 36.7 µs, and
pricing the change against *that* figure is what made it look like a −20% win.
It is the wrong connection.

So: 7.3 µs saved per changed write against 62.3 µs added per converged one. It
pays only where changed writes outnumber converged ones by more than **8.5 to
1**, and beehive is built for the opposite ratio. A controller re-applying its
own spec is the steady state, and the byte compare skipping it is
[what stops that waking the controller forever](2026-08-18-beehive-owns-the-generation-handshake.md).

## Consequences

The converged path cannot skip the `UPDATE` without the read the change existed
to remove, and no caller signals which case it is in. A `ControllerClient` holds
a status baseline and can compare in memory
([ADR](2026-08-19-a-pass-skips-a-status-write-it-can-see-is-a-no-op.md)), but
`UpdateSpec`'s callers are clients and hold nothing.

Read with its sibling, the pair bounds the shape from both sides: a read added in
front of a write that happens anyway is a loss below four converged writes per
changed one, and a read removed from a write that happens is a loss below eight
and a half changed writes per converged one. The transaction and its read both
stay.

What would reopen this: a workload whose spec writes overwhelmingly change
something — a bulk import, or a client that filters converged writes before
calling — or a SQLite that can reject a non-matching `UPDATE` for the cost of the
index seek. The second is the one to watch, since 52 µs for a statement that
writes nothing is the whole of the loss.
