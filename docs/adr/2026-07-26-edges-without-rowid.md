# Declare `edges` as a `WITHOUT ROWID` table

- **Status:** Accepted — implemented in `sqlite/migrations/0001_init.sql`.
- **Date:** 2026-07-26

## Context

`edges` holds the graph edges. Every edge is one row, and every column of that row
is part of the primary key:

```sql
CREATE TABLE edges (
    from_id  INTEGER NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    to_id    INTEGER NOT NULL REFERENCES objects(id) ON DELETE RESTRICT,
    relation TEXT NOT NULL CHECK (relation IN ('owned_by','depends_on')),
    PRIMARY KEY (from_id, to_id, relation)
) STRICT;

CREATE INDEX idx_edges_to ON edges(to_id, relation);
```

`idx_edges_to` answers "who points at X?" — the question behind the dependency
waker, the GC cascade, and the batched relation loaders. As a rowid table, that
answer took two steps: the index held only `to_id`, `relation`, and an internal
rowid, so getting `from_id` — the thing the caller actually wants — meant going
back to the table for the row. Every edge found cost an extra seek.

A second, quieter cost: because `edges` was a rowid table with an explicit
`PRIMARY KEY`, SQLite stored every edge twice — once in the table, once in the
automatic index (`sqlite_autoindex_edges_1`) enforcing the key.

## Decision

Declare the table `WITHOUT ROWID`. Columns, foreign keys, the `CHECK`, the primary
key, and `idx_edges_to` are all unchanged:

```sql
    PRIMARY KEY (from_id, to_id, relation)
) STRICT, WITHOUT ROWID;
```

This addresses both costs with one keyword. The primary key *is* the table, so the
duplicate index disappears. And because a secondary index on such a table
identifies rows by primary key rather than by rowid, `idx_edges_to` implicitly
carries `from_id` — it is really `(to_id, relation, from_id)`. The reverse-edge
lookup becomes covering and never touches the table.

### Alternative considered

Spelling the column out instead:

```sql
CREATE INDEX idx_edges_to ON edges(to_id, relation, from_id);
```

This ties on speed (118 vs 120 ms on the bare probe) — not a coincidence, but the
same index arrived at explicitly. `WITHOUT ROWID` was preferred because it *also*
drops the duplicate storage, and because it costs one keyword rather than a wider
index to maintain.

## Consequences

### Performance

Harness: `modernc.org/sqlite`, `SetMaxOpenConns(1)`, in-memory, 100k objects with
`depends_on` edges, 400-byte specs, `ANALYZE` run, best-of-5.

| query | before | after | delta |
|---|---|---|---|
| `SELECT from_id FROM edges WHERE to_id=? AND relation=?` (edges only) | 166 ms | 108 ms | **−35%** |
| `EdgesListIncoming` (the real shape — joins `objects`) | 379 ms | 305 ms | **−20%** |
| database page count | 21276 | 17576 | **−17%** |

**Quote the −20%, not the −35%, for anything beehive runs.** Every real consumer
joins `objects`, so the per-edge `objects` rowid seek survives and dilutes the
gain. The 35% is the ceiling — what the `edges` half costs once covering —
reachable only by a caller that wants ids and nothing else. The page-count figure
likewise scales with spec payload size: it is the `edges` table roughly halving,
measured against 400-byte specs.

Helped: `EdgesListIncoming` in the dependency waker, the batched
`EdgesGroupIncomingByID` / `EdgesGroupOutgoingByID` loaders, and
`ObjectsListByIncomingEdge`. Also the GC cascade's `EdgesHasIncoming`, but for a
smaller reason — its `edges` side becomes covering, while the
`deletion_requested_at` subquery it also runs rides the partial
`idx_objects_deleting` and was already cheap (3 ms / 200 calls at 50k objects);
don't expect it to move. Not measurably helped: single-object lookups like
`OwnersGet`, where one extra row fetch is lost in the noise. Writes should improve
in principle — one B-tree insert instead of two — but this was not measured.

### Query plans

Nothing observable changed: foreign keys still work (`ON DELETE CASCADE` on
`from_id`, `ON DELETE RESTRICT` on `to_id`), the `CHECK` is unaffected, and every
statement returns the same results in the same order. Plans did move, verified by
running `EXPLAIN QUERY PLAN` over every `edges` statement in the tree before and
after and diffing:

| statement | before | after |
|---|---|---|
| `EdgesListOutgoing` | `COVERING INDEX sqlite_autoindex_edges_1` | `PRIMARY KEY` |
| `EdgesListOutgoingByRelation` | `COVERING INDEX sqlite_autoindex_edges_1` | `PRIMARY KEY` |
| `EdgesAdd` wake-stamp probe | `COVERING INDEX sqlite_autoindex_edges_1` | `PRIMARY KEY` |
| `EdgesDelete` | `INDEX sqlite_autoindex_edges_1` | `PRIMARY KEY` |
| `EdgesListIncoming` | `INDEX idx_edges_to` | `COVERING INDEX idx_edges_to` |
| `ObjectsListByIncomingEdge` | `INDEX idx_edges_to` | `COVERING INDEX idx_edges_to` |
| `EdgesHasIncoming` | `INDEX idx_edges_to` | `COVERING INDEX idx_edges_to` |

The outgoing side losing `COVERING` is a wash by construction, not a regression:
the automatic index those statements used *is* the table now, so the same key
search runs against one B-tree instead of two. `EdgesDelete` was the one statement
not already covering, so it gains outright.

### The covering property is now invisible in the schema

`idx_edges_to`'s covering behaviour depends on the table's storage class, not on
the index definition. Removing `WITHOUT ROWID` later would give the gain back with
that `CREATE INDEX` line looking unchanged. Two things guard against that: the
comment above `idx_edges_to` in `0001_init.sql` says so explicitly, and
`TestDeletionRequestsCreateFromOwnerUsesRefsIndex` (`sqlite/store_test.go`) asserts
`"COVERING INDEX idx_edges_to"` rather than just the index name. That assertion is
the only place in the suite that would notice.

### Applying it to an existing database is not a one-liner

SQLite cannot convert a table between rowid and `WITHOUT ROWID` in place. This
change rode an in-place edit to `0001_init.sql`, which costs nothing:
`sqlite/migrations/` holds exactly one file and TODO.md records that a fresh
database is the only supported upgrade path, so editing the initial migration is
legitimate rather than rewriting applied history.

Should that policy change, converting an existing store means a
create-copy-drop-rename of the whole `edges` table — and `0001_init.sql`'s preamble
declares `PRAGMA foreign_keys = ON` as a store contract, with `edges` sitting
between two FK edges, so the rebuild must run under `PRAGMA foreign_keys = OFF`
and then re-enable it and run `PRAGMA foreign_key_check` before declaring success.
That is the one scenario where this is more than a keyword, which is why it was
worth doing early.

## Implementation

Three commits on `refactor/refs--without-rowid`, built red/green:

1. Tightened `TestDeletionRequestsCreateFromOwnerUsesRefsIndex` to assert `COVERING`. Red,
   failing with `SEARCH r USING INDEX idx_edges_to (to_id=? AND relation=?)`.
2. `) STRICT, WITHOUT ROWID;` plus the migration comments. Green, full suite clean.
3. Reworded the `EdgesAdd` wake-stamp comment in `sqlite/store.go` (and its twin in
   CLAUDE.md), which had described the probe as "a covering probe on the edges
   primary key" — accurate before, wrong once the primary key *is* the table.

No new index, no code change, no new test.

## Related work

The most sensitive consumer would be a durable dependency-wake recovery scan — a
catch-up pass running the reverse-edge probe in a tight loop over a backlog. No
such design exists in this repo; the numbers above are measured on this repo's own
statements. If one lands later it inherits the cheaper probe, and if it reads ids
without joining `objects`, the full 35% rather than the 20%.
