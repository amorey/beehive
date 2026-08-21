# Bind four rendered tuple sets

- **Status:** Planned. Four statements, one mechanism, one PR.
- **Date:** 2026-08-21
- **Depends on:**
  [bind an id list as JSON](../adr/2026-08-21-bind-an-id-list-as-json.md), whose
  mechanism this is, applied to the shapes it left.

## Why

That ADR took the rendered SQL sites from twelve to six by binding `IN` lists as
JSON. Four of the six render a `VALUES` tuple set instead, and the same
mechanism reaches all four: `json_each` yields rows a `SELECT` widens with `->>`,
so a two-column list becomes

```sql
("group", kind) IN (SELECT value ->> 0, value ->> 1 FROM json_each(?))
```

and a bulk insert takes it from the other side, `INSERT … SELECT … FROM
json_each(?)` instead of `VALUES (…), (…)`.

Measured on this driver and schema, rendered against JSON-and-prepared:

| statement | size | rendered | JSON, prepared | |
|---|---|---|---|---|
| `markManyForDeletionChunk` | 1 id | 49.7 µs | 16.2 µs | **−67%** |
| | 128 ids | 360.3 µs | 153.9 µs | **−57%** |
| `appendWriteLogUpdates` | 1 row | 16.1 µs | 11.9 µs | **−26%** |
| | 128 rows | 840.0 µs | 612.2 µs | **−27%** |
| `ListStaleSince` | 1 kind | 270.2 µs | 221.9 µs | **−18%** |
| | 4 kinds | 288.8 µs | 236.7 µs | **−18%** |
| `reconcileOwedSweepQuery` | 1 kind | 95.8 µs | 85.3 µs | **−13%** |
| | 4 kinds | 117.4 µs | 101.5 µs | **−14%** |

**The spread is the parse's share, and it is worth reading before building.**
Preparing removes the parse and nothing else, so what it saves is the fraction
the parse was. The deletion mark touches a handful of rows, so the parse was most
of its cost. `ListStaleSince` walks a dependency graph and keeps that walk —
which is why its saving is flat in the kind count. Nothing here is a surprise
once stated, but it does mean the two tick-driven conversions are worth
single-digit microseconds a tick, and their real case is structural.

**The win is the preparation, not the shape.** The JSON form adds a virtual-table
scan the `VALUES` form does not have, and is *slower* than the rendered one when
both are unprepared. That is why this spec converts nothing that cannot also
become a field — a conversion that stayed rendered would be a straight
regression.

Measured on a probe, not through the store. Let the store's own benchmarks report
the number.

## The four

### `markManyForDeletionChunk` (`sqlite/store.go:2501`)

The best measured, and the only one whose plan improves:

```
today   MATERIALIZE assigned          JSON    SCAN json_each VIRTUAL TABLE INDEX 1:
        SCAN CONSTANT ROW                     SEARCH objects USING INTEGER PRIMARY KEY (rowid=?)
        SCAN assigned
        SEARCH objects USING INTEGER PRIMARY KEY (rowid=?)
```

A `VALUES` CTE is materialised into an ephemeral table before the join; a
`json_each` scan feeds the join directly.

```sql
WITH assigned(mark_id, mark_rv) AS
     (SELECT value ->> 0, value ->> 1 FROM json_each(?2))
UPDATE objects
   SET deletion_requested_at = ?1, updated_at = ?1, resource_version = assigned.mark_rv
  FROM assigned
 WHERE objects.id = assigned.mark_id AND objects.deletion_requested_at IS NULL
RETURNING objects.id, objects."group", objects.kind, objects.resource_version
```

`RETURNING` stays and so does its reason: the log entries need each row's
identity and the version it took, which `RowsAffected` cannot give. A row the
`IS NULL` guard skips still consumes its version, and that gap stays harmless
because every consumer seeks with `>`. `markChunkSize` (`:2452`) keeps its
meaning — a measured optimum for the batched write, never a parameter ceiling.

### `appendWriteLogUpdates` (`:911`)

**A free function today** — `func appendWriteLogUpdates(ctx, c dbtx, …)` — with
no receiver to reach `s.exec`. It becomes a method, or takes the store.
`markManyForDeletionChunk` is already a method and its `c dbtx` parameter simply
goes unused, so the two are not symmetric here.

`op` and `written_at` are the same for every row in a batch, so only four values
ride the array:

```sql
INSERT INTO object_writes (` + objectWritesColumns + `)
SELECT value ->> 0, value ->> 1, value ->> 2, value ->> 3, ?1, ?2
  FROM json_each(?3)
```

`objectWritesColumns` (`:892`), not the columns spelled out: the const exists to
keep the two log inserts in step and `stmtAppendWriteLogDelete` already uses it
(`statements.go:312`). The array's `[rv, id, group, kind]` order is that const's
order, and must stay so.

No `ON CONFLICT`, so this one does not need the `WHERE true` the conditions
upsert does.

**It is the only one here that carries text.** `group` and `kind` go through
JSON, which cannot represent bytes that are not UTF-8 — `json.Marshal`
substitutes U+FFFD. The values come from `Register`, are identifiers rather than
caller data, and arrive having just been read back out of SQLite by the deletion
mark's `RETURNING`. Stated as an assumption rather than designed around; the
alternative is joining `objects` for two columns per row to protect an identifier
that cannot realistically be malformed. Free text is why
[the conditions upsert](2026-08-21-bind-the-conditions-upsert.md) is a separate
spec.

### `ListStaleSince` (`:1290`)

**The plan is the whole risk.** This is the most carefully tuned read in the
store: its `CROSS JOIN`s pin the join order — without them the planner reads the
whole graph and the cursor buys nothing — and its cursor rides `idx_objects_rv`.
A subquery in the middle of that is exactly the kind of change that quietly costs
a seek. It does not:

```
today   SEARCH t USING COVERING INDEX idx_objects_rv (resource_version>? AND resource_version<?)
        SEARCH e USING COVERING INDEX idx_edges_to (to_id=? AND relation=?)
        SEARCH d USING INTEGER PRIMARY KEY (rowid=?)
        LIST SUBQUERY 1
        SCAN CONSTANT ROW
        CREATE BLOOM FILTER
        SEARCH c USING INTEGER PRIMARY KEY (rowid=?) LEFT-JOIN

JSON    … SCAN json_each VIRTUAL TABLE INDEX 1: … (every other line identical)
```

Both covering-index seeks, the bloom filter, the `LEFT-JOIN` on the watermark and
the join order all survive; only `SCAN CONSTANT ROW` becomes the array scan, and
neither plan has a temp B-tree, so the row-value cursor still delivers its own
order.

`(d."group", d.kind) IN (…)` is a row-value comparison against a two-column
subquery; verified working. The kinds list is not chunked and does not become
chunked — it comes from `Register` calls, not caller data.

### `reconcileOwedSweepQuery` (`:1223`)

The function disappears; it exists only to render. Plan unchanged apart from the
same substitution, and the partial index `idx_objects_reconcile_owed WHERE
reconcile_owed != 0` still drives it.

**The empty-`keep` branch folds into the statement.** It exists today because
`NOT IN (VALUES)` is a syntax error, so a sweep keeping nothing needs a second
query with the clause removed. `NOT IN (SELECT … FROM json_each('[]'))` is valid
and matches every row, which is what an empty `keep` means. Verified.

`NOT IN` against a subquery yielding any NULL returns no rows at all — the
classic trap, and it is not reachable here: `group` and `kind` are `NOT NULL`
and the array is built from `GroupKind` values, so no element can be JSON `null`.
This is the only one of the four with `NOT IN`, so it is the only place the
question is even askable.

## The helper this adds

`jsonList[T ~int64]` marshals a flat list of ids. None of these four is flat:
they need `[[id, rv], …]`, `[["group", "kind"], …]` and `[[rv, id, "group",
"kind"], …]`. **This is the spec's one open design choice**, and it is where the
U+FFFD surface lives — the reason
[the conditions upsert](2026-08-21-bind-the-conditions-upsert.md) is a separate
spec at all.

**Three non-generic helpers, one per shape**, beside `jsonList` and
`conditionTypeList` — the latter being the precedent: it exists because
narrowing `jsonList` to `~int64` left condition types without a path, and it
carries its own doc comment saying what makes its text safe.

```go
func jsonKinds(kinds []storeapi.GroupKind) string        // the two pair sets
func jsonMarkPairs(ids []storeapi.ObjectID, first int64) string
func jsonWriteLogRows(writes []loggedWrite) string
```

Not one widened generic. A `~string` constraint on a shared marshal helper is
exactly what produced the silent condition-type corruption that
`ErrInvalidConditionType` now guards, and the two helpers here that carry text
should say in their own doc comment which text they carry and why it is safe —
`jsonKinds` and `jsonWriteLogRows` both take identifiers from `Register`, never
caller data. A helper that cannot be handed free text cannot lose it.

Each discards `json.Marshal`'s error for `jsonList`'s reason: a slice of
integers and registered kinds cannot fail to marshal.

## The helpers this removes

The chain is one link longer than it looks, and the order matters:

```
placeholders  ←  tupleRows  ←  appendWriteLogUpdates, markManyForDeletionChunk,
                                upsertConditions, kindTuples
                  kindTuples ←  reconcileOwedSweepQuery, ListStaleSince
```

**`kindTuples` (`:3391`) dies in this change**: both its callers are here.

**`tupleRows` (`:3386`) does not.** It loses three of its four callers — the two
converted here plus `kindTuples` — and keeps `upsertConditions`. `placeholders`
(`:3380`) has one caller, `tupleRows`, so it survives too. Both go when
[the conditions upsert](2026-08-21-bind-the-conditions-upsert.md) lands, and
neither goes at all if that spec is declined.

`renderedSQLSites` goes from six entries to two: `sqliteStore.upsertConditions`
and `sqliteEvents.List`. The list is asserted with `ElementsMatch`, so it moves
in the same change or the suite fails. **Its doc comment still says "the twelve
functions"** and has said so since the id-list change took it to six; fix it
here.

## Three of the four are writes

`appendWriteLogUpdates`, `markManyForDeletionChunk` and the owed sweep all
write; only `ListStaleSince` reads. Each of the three needs its entry in
`stmtWrites`, or it is prepared on the read pool, `writeStmt` stops refusing it
in a read frame, and it fails as a driver error on a cold path instead of at
routing. `TestEveryStatementIsClassifiedByItsOwnText` derives the classification
from the statement text and catches a miss, which is the backstop rather than
the reason to be careful.

Two roster entries go stale with this change and neither fails loudly:

- `TestNoWriteBypassesConn` lists `reconcileOwedSweepQuery` as a function that
  "builds a string, executes nothing". The function is deleted here, and a
  stale name is silently ignored by the map lookup — so it rots rather than
  failing.
- `markChunkSize`'s comment ends "and the mark still renders a tuple per id",
  which stops being true the moment the deletion mark converts.

## Traps shared across the four

- **`->>` types by the JSON value.** A JSON string yields TEXT, a number
  INTEGER, `true` the integer 1, `null` an SQL NULL. Nothing here stores a bool,
  and the two pair sets compare TEXT against `NOT NULL` TEXT columns.

- **Numbered parameters must be contiguous.** The deletion mark repeats `?1`
  across two columns, which needs numbering; skipping a number — `?1` and `?3`
  with two arguments — fails at `database/sql` with `missing named argument`,
  not at SQLite. Met while probing that statement.

- **A plan test must read the field the store issues**, not a copy. The text is
  long-lived now, so a test carrying its own string would pass while the store
  ran something else.

## Tests

In `sqlite/store_test.go`:

- Each statement returns or writes exactly what it did before, over an empty
  list, one row, and more than one chunk where the caller chunks.
- **`ListStaleSince` keeps its plan**: both covering-index seeks, the bloom
  filter, no temp B-tree. The most important test here.
- The deletion mark no longer materialises its CTE, and still hands `chunk[i]`
  the version `first+i`; a row already deletion-pending is still skipped and
  reported unmarked.
- The write log records one entry per write, each carrying its own version — a
  batch shares a draw, never a value.
- The owed sweep zeroes outside `keep` and leaves it inside, over an empty
  `keep`, one kind and several; the empty case is the branch that folded.
- `kindTuples` is gone, and `tupleRows`/`placeholders` are not — or
  `staticcheck -checks=all` says otherwise.
- `renderedSQLSites` names two functions, and
  `TestOnlyRenderedSQLLivesInAFunction` still passes.

Benchmark the deletion mark at 1 and 128 ids and the write log append at 1 and
128 rows, against the commit before this one. **No benchmark for the two
tick-driven statements**: their numbers above say the conversion is not a
regression, which is what they were for; 10–50 µs on a 30s or 60s tick does not
earn a permanent benchmark. `ListStaleSince`'s plan test is what has to stay.

## Not in this spec

**[The conditions upsert](2026-08-21-bind-the-conditions-upsert.md)** — same
mechanism and the second-best number, but it carries free text and so needs a
contract decision first. Separate, so four mechanical conversions do not wait on
one API question.

**[The PRAGMA tripwire](2026-08-21-let-the-tripwire-see-a-pragma.md)** — not this
mechanism, and not a conversion: the two constant `PRAGMA`s it covers turn out to
be unpreparable for a reason worth writing down.

**`Events().List`** — measured and declined; see `docs/TODO.md`.
