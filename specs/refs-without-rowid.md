# Declare `refs` as a `WITHOUT ROWID` table

**Status: proposed, not built.**

A one-line schema change that makes every "who points at this object?" lookup
cheaper and shrinks the database. It stands on its own — nothing else depends on
it, and it depends on nothing else.

---

## 1. Summary

| | |
|---|---|
| **Recommendation** | Add `WITHOUT ROWID` to the `refs` table definition |
| **Impact** | Reverse-edge lookups stop fetching a table row per edge: **−35%** on the bare `refs` probe, **−20%** on the real `ListIncomingRefs` shape, which joins `objects` and so still pays a rowid seek per edge. The `refs` table also stops storing every edge twice — **−17%** database pages at 400-byte specs (§4). |
| **Cost** | One line in `sqlite/migrations/0001_init.sql`, plus two comment fixes and a one-string tightening of an existing test. No new index, no code change, no new test. |
| **Risk** | Low. Storage layout only — no behaviour, no API, no query changes. |

---

## 2. The problem

`refs` holds the graph edges. Every edge is one row:

```sql
CREATE TABLE refs (
    from_id  INTEGER NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    to_id    INTEGER NOT NULL REFERENCES objects(id) ON DELETE RESTRICT,
    relation TEXT NOT NULL CHECK (relation IN ('owned_by','depends_on')),
    PRIMARY KEY (from_id, to_id, relation)
) STRICT;

CREATE INDEX idx_refs_to ON refs(to_id, relation);
```

`idx_refs_to` answers "who points at X?" — the question behind the dependency
waker, the garbage collector's cascade, and the batched relation loaders.

Today that answer takes two steps. SQLite finds the matching entries in
`idx_refs_to`, but that index stores only `to_id`, `relation`, and an internal
rowid. To get `from_id` — the thing the caller actually wants — it must then go
back to the table and read the row.

So every edge found costs an extra lookup.

There is a second, quieter cost. Because `refs` is a normal rowid table with an
explicit `PRIMARY KEY`, SQLite stores the data twice: once in the rowid table, and
once in an automatic index (`sqlite_autoindex_refs_1`) that enforces the key. Every
edge is written and stored two times.

---

## 3. The change

```sql
CREATE TABLE refs (
    ...unchanged...
    PRIMARY KEY (from_id, to_id, relation)
) STRICT, WITHOUT ROWID;
```

That's it. Columns, foreign keys, the `CHECK`, the primary key, and
`idx_refs_to` all stay exactly as they are.

**Why this fixes both problems at once.** In a `WITHOUT ROWID` table the primary
key *is* the table — there is no separate rowid and no duplicate index. And
because secondary indexes on such a table identify rows by primary key rather than
by rowid, `idx_refs_to` automatically carries `from_id` along with it. The lookup
becomes covering: SQLite reads `from_id` straight out of the index and never
touches the table.

You could get the covering behaviour instead by spelling the column out:

```sql
CREATE INDEX idx_refs_to ON refs(to_id, relation, from_id);
```

Both perform the same (§4). `WITHOUT ROWID` is preferable because it also removes
the duplicate storage, and because it costs one keyword rather than a wider index
to maintain.

---

## 4. Measurements

Harness: `modernc.org/sqlite`, `SetMaxOpenConns(1)`, in-memory, 100k objects with
`depends_on` edges, 400-byte specs, `ANALYZE` run, best-of-5. Two queries, run over
a backlog of changed objects so the probe is exercised many times: the bare
reverse-edge probe, and the shape beehive actually issues.

| query | today | `WITHOUT ROWID` | delta |
|---|---|---|---|
| `SELECT from_id FROM refs WHERE to_id=? AND relation=?` (refs only) | 166 ms | 108 ms | **−35%** |
| `ListIncomingRefs` (the real shape — joins `objects`) | 379 ms | 305 ms | **−20%** |
| database page count | 21276 | 17576 | **−17%** |

The bare probe is where the plan changes outright: `SEARCH r USING INDEX
idx_refs_to` with a row fetch per edge becomes `SEARCH r USING COVERING INDEX
idx_refs_to` with none.

**Quote the −20%, not the −35%, for anything beehive runs.** Every consumer in the
"helped" list below joins `objects`, so the per-edge `objects` rowid seek survives
the change and dilutes the gain. The 35% is the ceiling — what the `refs` half of
the work costs once it is covering — reachable only by a caller that wants ids and
nothing else. Spelling out `idx_refs_to(to_id, relation, from_id)` instead lands in
the same place (118 vs 120 ms on the bare probe); see §5 for why that is arithmetic
rather than coincidence.

The page-count figure likewise scales with spec payload size: it is the `refs`
table roughly halving, measured against 400-byte specs. Wider specs dilute it,
narrower ones amplify it.

### What this does and doesn't speed up

This helps whenever many edges are read at once. It is invisible when only a few
are.

- **Helped:** `ListIncomingRefs` in the dependency waker, the batched
  `GroupIncomingRefsByID` / `GroupOutgoingRefsByID` loaders, and
  `ListIncomingRefObjects`. Also the GC cascade's `HasIncomingRefs`, but for a
  smaller reason than the others: its `refs` side becomes covering, while the
  `deletion_requested_at` subquery it also runs rides the partial
  `idx_objects_deleting` and is already cheap (3 ms / 200 calls at 50k objects).
  Don't expect it to move.
- **Not measurably helped:** single-object lookups like `GetOwner`, where one
  extra row fetch is lost in the noise.

Writes are unaffected in principle — one B-tree insert instead of two (table plus
automatic index) — though this was not separately measured.

---

## 5. Is it safe?

**Requirements are met.** SQLite requires a `WITHOUT ROWID` table to declare an
explicit `PRIMARY KEY`. `refs` does. All three key columns are `NOT NULL`, and
every column in the table is part of the key, so there is no leftover payload.
`STRICT, WITHOUT ROWID` is a legal combination.

**Nothing observable changes.** Foreign keys still work, including
`ON DELETE CASCADE` on `from_id` and `ON DELETE RESTRICT` on `to_id`. The `CHECK`
on `relation` is unaffected. Every existing statement against `refs` — inserts,
deletes, the relation loaders, the semi-join in `ListIncomingRefObjects` — is
unchanged and returns the same results in the same order.

**Plans do change, though, on the outgoing side.** Anyone running `EXPLAIN QUERY
PLAN` after the change will see `ListOutgoingRefs`, `ListOutgoingRefsByRelation`
and `AddRef`'s wake-stamp probe shift from
`SEARCH refs USING COVERING INDEX sqlite_autoindex_refs_1` to
`SEARCH refs USING PRIMARY KEY`. That is a wash by construction, not a regression:
the automatic index those statements used *is* the table now, so the same key
search happens against one B-tree instead of two. The word `COVERING` disappears
because there is no longer a table behind the index to be covering *of* — which is
why the comment on that probe in `sqlite/store.go` needs the edit §6 lists.

**`idx_refs_to` gets wider, and that is the mechanism.** In a `WITHOUT ROWID`
table, a secondary index appends whichever primary-key columns it doesn't already
hold. `idx_refs_to(to_id, relation)` already holds two of the three, so it becomes
exactly `(to_id, relation, from_id)` — the explicit index from §3, arrived at
implicitly. That is why the two alternatives tie at 118 vs 120 ms rather than
happening to land nearby. The extra column per entry is paid for several times over
by dropping `sqlite_autoindex_refs_1`, which held all three columns for every edge.

**The one real constraint is timing.** SQLite cannot convert a table between rowid
and `WITHOUT ROWID` in place. As long as the change rides in
`0001_init.sql` (see §6) that costs nothing. If it ever has to be applied to an
existing database, it becomes a create-copy-drop-rename of the whole `refs`
table — still routine, but no longer a one-liner.

**A note for future readers.** After this change, `idx_refs_to`'s covering
behaviour depends on the table's storage class rather than on the index
definition. Someone removing `WITHOUT ROWID` later would silently give the gain
back with no visible change to the index. §6 requires a comment saying so, and §7
tightens an existing plan assertion so the regression fails a test rather than
going unnoticed.

---

## 6. Migration

**This is an edit to `sqlite/migrations/0001_init.sql`, in place. No new migration
file, no `ALTER TABLE`.**

`sqlite/migrations/` holds exactly one file, and TODO.md records that a fresh
database is the only supported upgrade path — existing stores are recreated rather
than migrated. That is what makes editing the initial migration legitimate instead
of rewriting applied history.

Two edits to the migration:

1. Change the `refs` table's closing line:

```sql
-- was: ) STRICT;
) STRICT, WITHOUT ROWID;
```

2. Extend the comment above `idx_refs_to` to record that it now carries `from_id`
   implicitly, that this is why the probe is covering, and that the property is
   lost if the table's storage class changes. Match the style of the existing
   comment on `idx_objects_deleting`, which documents its covering behaviour the
   same way.

Nothing else in the file changes.

**One comment outside the file goes stale.** `sqlite/store.go:1408-1411` describes
`AddRef`'s wake-stamp probe as "a covering probe on the refs primary key". After
the change its plan is `SEARCH refs USING PRIMARY KEY` (§5) — same cost or better,
but "covering" is the wrong word once the primary key *is* the table. CLAUDE.md
treats that probe as the sole edge-newness test, so the comment is worth keeping
accurate: reword it to say the probe rides the primary-key B-tree directly, and
keep the rest of the paragraph (one statement, no extra round-trip, the only place
edge-newness is decided) as is.

**If the fresh-database policy changes first**, this becomes
`000N_refs_without_rowid.sql` containing a create-copy-drop-rename. That is the
only scenario in which this proposal is more than a one-line change, and it is
worth doing early for exactly that reason. Note it is a little more than a routine
table rebuild: `0001_init.sql`'s preamble declares `PRAGMA foreign_keys = ON` as a
store contract, and `refs` sits between two FK edges, so the rebuild has to run
with `PRAGMA foreign_keys = OFF`, then re-enable it and run `PRAGMA
foreign_key_check` before declaring success.

---

## 7. Tests

**No new test.** This changes storage, not behaviour, so a test asserting the new
layout would be testing SQLite rather than beehive.

What proves it is correct is the existing coverage in `sqlite/store_test.go`: edge
insert and delete, cascade delete through `from_id`, `RESTRICT` blocking a live
target, the batched group readers, and the GC cascade tests that lean on
`HasIncomingRefs`. If those pass unchanged, the change is correct.

**One existing test gets one string tightened.** §5 warns that someone removing
`WITHOUT ROWID` later gives the gain back invisibly. That guard already exists in
skeleton form: `TestMarkOwnedForDeletionUsesRefsIndex`
(`sqlite/store_test.go:1391`) asserts the plan for exactly this probe, today as

```go
assert.Contains(t, plan, "idx_refs_to", ...)
```

Tighten it to `"COVERING INDEX idx_refs_to"`. That is one edit to an existing
assertion and it is precisely the regression guard §5 asks for — no more "testing
SQLite" than the current line is, since what it pins is beehive's index alignment.

Deliverables that are not tests: the migration comment (§6.2) and the `store.go`
comment fix (§6). Without them the property is undocumented and easy to undo.

---

## 8. Relationship to other work

The most sensitive consumer would be a durable dependency-wake recovery scan — a
catch-up pass that runs the reverse-edge probe in a tight loop over a backlog. No
such proposal is in this repo today; the §4 numbers are measured on this repo's own
statements, not borrowed from one. **Nothing here depends on that work landing.**

- If a backlog-scan design lands later, it inherits the cheaper probe — and, if it
  reads ids without joining `objects`, the full 35% rather than the 20%.
- If it doesn't, this change still speeds up the GC cascade, the dependency
  waker, and the relation loaders.

Both edit `0001_init.sql`, so if they land together the edits are combined in one
pass over that file. Order does not matter.
