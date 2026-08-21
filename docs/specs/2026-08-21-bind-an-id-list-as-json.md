# Bind an id list as JSON

- **Status:** Planned. Seven functions render an `IN` list today; bound as one
  JSON parameter their text is constant, so each becomes a prepared statement.
  One of the seven changes its query plan — see *What the plan survives*.
- **Date:** 2026-08-21
- **Depends on:**
  [prepare every constant statement](../adr/2026-08-21-prepare-every-constant-statement.md),
  which is what a constant text is now worth.

## Why

That ADR states a rule the code makes true, not a law: *"text rendered from a
runtime count is not a field."* It is rendered because of how the statement is
written, and an `IN` list is the one shape SQLite lets you write without the
count. Bind the ids as a JSON array and unpack them with `json_each`, and the
text stops varying.

The twelve functions holding rendered SQL pay the residency toll — +9% on an
unprepared execution — with nothing against them. Seven of them are `IN` lists.

Measured against today's rendered list, prepared, on a connection holding 60
resident statements, 1000 rows, seventeen columns, on disk:

| ids | rendered `IN` list | `json_each` | |
|---|---|---|---|
| 1 | 25.1 µs | 16.1 µs | **−36%** |
| 8 | 58.2 µs | 36.0 µs | **−38%** |
| 64 | 223.0 µs | 188.2 µs | **−16%** |
| 100 | 328.0 µs | 286.6 µs | **−13%** |

Small lists win most, which is where the toll bit hardest: the tail pages one
object at a time on a quiet beehive, so `Objects().ListByIDs` with a single id is
its ordinary shape, not its edge case.

That was measured on a probe carrying this driver and these pragmas, not on the
store. Treat it as the reason to build, and let the store's own benchmarks
report the number.

## The form is not a choice

**`WHERE col IN (SELECT value FROM json_each(?))`, never a join against
`json_each`.** The join reads better and changes the plan:

```
edges, IN list today          SEARCH r USING PRIMARY KEY (from_id=?)
                              SEARCH o USING INTEGER PRIMARY KEY (rowid=?)

edges, json_each subquery     SEARCH r USING PRIMARY KEY (from_id=?)
                              LIST SUBQUERY 1
                              SCAN json_each VIRTUAL TABLE INDEX 1:
                              SEARCH o USING INTEGER PRIMARY KEY (rowid=?)

edges, json_each join         SCAN j VIRTUAL TABLE INDEX 1:
                              SEARCH r USING PRIMARY KEY (from_id=?)
                              SEARCH o USING INTEGER PRIMARY KEY (rowid=?)
                              USE TEMP B-TREE FOR ORDER BY
```

Driving from the JSON makes it the outer loop, so the rows arrive in the array's
order and the `ORDER BY` has to sort them. That is the temp B-tree
`TestEdgeListsInheritTheIndexOrder` exists to keep out.

## What the plan survives

**The subquery form preserves the plan a multi-id list gets today. It does not
preserve the one SQLite gives a list of exactly one.** `IN (?)` with a single
element folds into an equality constraint, and a plan built on one value of the
constrained column can carry an `ORDER BY` the index only provides within that
value. `json_each` cannot fold, so that plan is not available to it.

The rule this comes down to: **the sort survives when the `ORDER BY` leads with
the column the `IN` list constrains.** Six of the seven do, at any length.
`unblockedTargetsChunk` does not — it filters `r.from_id` and orders by
`r.to_id`, which the primary key gives only inside one `from_id`:

```
unblocked today, one id     SEARCH r USING PRIMARY KEY (from_id=?)
                            SEARCH o USING INTEGER PRIMARY KEY (rowid=?)

unblocked today, two ids    … + USE TEMP B-TREE FOR ORDER BY

unblocked as json_each      … + USE TEMP B-TREE FOR ORDER BY   ← at any length
```

So today's statement already sorts whenever it is given more than one id; what
converting costs is the single-id case, and that is its ordinary shape —
`requestDeletion` passes a one-element slice on a client delete
(`store.go:2606`).

**Convert it anyway, and measure that case.** The result set is the
deletion-pending targets of one object, usually none and rarely more than a few,
so the sort is over almost nothing while the preparation saves a parse the
rendered statement pays every time. That is an expectation, not a measurement.
If the benchmark disagrees, the fallback is a second field holding
`r.from_id = ?` for the singular call, which would leave that path both prepared
and sorted the way it is today; do not write it before the number asks for it.

**Run that benchmark first, not last.** It is the only measurement in this spec
that can change what gets built: the fallback gives a one-statement function two,
and it would arrive after the plan test had already been written to expect the
sort. Everything else here is mechanical once the pattern is set.

## What changes

Seven functions, each losing its rendered text to a field:

| Function | Becomes | Binds |
|---|---|---|
| `sqliteStore.conditionsByIDsChunk` (`:1161`) | one id | object ids |
| `sqliteReconcileOwed.Stamp` (`:1228`) | one id | object ids |
| `sqliteStore.readImages` (`:1484`) | one id | resource versions |
| `conditionSetLoad` (`:1816`) | one id, and the function goes | condition types |
| `sqliteStore.unblockedTargetsChunk` (`:2633`) | one id | source ids |
| `sqliteStore.edgesByIDsChunk` (`:2943`) | **two** ids | route ids |
| `sqliteObjects.ListByIDs` (`:3405`) | one id | object ids |

`edgesByIDsChunk` takes two because `routeCol`/`joinCol` are the only two column
pairs its callers pass — `("to_id", "from_id")` at `:2918` and
`("from_id", "to_id")` at `:2994`. Each new statement writes its own
`ORDER BY r.<route>, r.<join>` inline, as the rendered version does.
**Not `edgeOrderByReferrer`/`edgeOrderByTarget`** (`:715`): those are single
column, and they stay exactly as they are for the four single-id edge statements
that use them (`statements.go:341-353`).

`conditionSetLoad` disappears entirely: it exists only to interpolate the count.

`renderedSQLSites` goes from twelve entries to six —
`appendWriteLogUpdates`, `reconcileOwedSweepQuery`, `ListStaleSince`,
`upsertConditions`, `Events().List`, `markManyForDeletionChunk`. The list is
asserted with `ElementsMatch`, so it moves in the same change or the suite fails.

## Converting ListByIDs ends the split

`Objects().ListByIDs` is the one that was never in `renderedSQLSites`: it renders
a *fragment* for `listObjectsWhere`. With a constant tail it becomes an ordinary
field through `listObjectsSQL`, and `listObjectsWhere` has no callers left
(`:3414` is the only one). Four edits belong in this change, or CI fails and two
tests assert something that is no longer true:

- **Delete `listObjectsWhere`** (`:1098`). CI runs
  `staticcheck -checks=all`, which flags unused unexported code.
- **Rewrite or delete `TestOnlyTheConstantObjectListingIsPrepared`**
  (`store_test.go:9601`). Its premise is a split with two live sides, and it
  asserts `callSites(t, "listObjectsWhere") == ["sqliteObjects.ListByIDs"]`.
  After this there is one side. What is worth keeping is that both reads return
  the same row.
- **Delete `TestAnOversizedIDListIsReported`** (`store_test.go:9849`). It asserts
  40 000 ids error out past `SQLITE_MAX_VARIABLE_NUMBER`. One bound parameter has
  no such limit, so the branch it reaches is gone with the function.
  `Objects().ListByIDs` loses its arity ceiling — a widening, not a regression,
  and worth saying out loud since a caller could have been relying on the error.
- **Fix the comment on `listObjectsSQL`** (`statements.go:403-404`), which says
  `ListByIDs` "renders its own tail and cannot be prepared, so it keeps
  `listObjectsWhere`". All three clauses stop being true.

## Binding

**`encoding/json` over a `[]int64`**, or a `[]string` for condition types. One
mechanism, not two: hand-rolling the array for the integer cases would be faster
and is not worth a second thing to be wrong.

**One helper, and the error is decided there.** `json.Marshal` of a `[]int64` or
a `[]string` cannot fail — no channels, no funcs, no NaN — so seven call sites
would otherwise each grow an `if err != nil` that no test can reach. Put the
marshal behind one function returning a `string`, and discard the error there
with a line saying why.

**Marshal ids as numbers, never as strings.** `json_each` gives a JSON string
TEXT affinity, and TEXT compared against an INTEGER column matches nothing — a
silent empty result, not an error. `storeapi.ObjectID` is `int64`, so a
`[]int64` marshals correctly by construction; the trap is only reachable by
converting first.

**`readImages` and its caller both change shape.** It takes `versions []any`
(`:1484`) because that is what the placeholder path wanted; it becomes
`[]int64`, and `attachImages` (`:1459`) builds that slice, so its `var deletes
[]any` moves with it.

## Edge cases the implementer would otherwise guess at

- **Keep the empty-list guards.** `IN (SELECT … FROM json_each('[]'))` matches
  nothing, which is the right answer, but the guards return it without a
  statement. `attachImages` early-returns, `ListByIDs` and `Stamp` guard, and the
  chunk loops never run a chunk of zero.

- **`idChunkSize` stays, and its comment stops being true** (`:2921`). It says
  the size is "under SQLite's `SQLITE_MAX_VARIABLE_NUMBER` (32766 in modernc)",
  which was the binding reason while each id was a parameter. One parameter has
  no such ceiling. What still justifies chunking is the result set and the JSON
  this builds: at 30000 ids that array is roughly 600 KB in one bound value,
  where it used to be 30000 small ones. Far under any SQLite limit, and the point
  of the rewritten comment — the number bounds a size, it is not arbitrary.
  Leave it alone.

- **A statement's text is now long-lived, so `EXPLAIN QUERY PLAN` in a test pins
  what actually runs.** The plan tests must read the same field the store issues,
  the way `reconcileOwedSweepQuery`'s already does.

- **`json_each` is core SQLite, not an extension to enable.** `modernc` ships
  3.53.2; JSON1 has been built in since 3.38, so there is no build tag and no
  version gate.

## Tests

In `sqlite/store_test.go`:

- Each converted read returns exactly what it returned before, over an empty
  list, one id, and a list longer than one chunk.
- **The edge listings still inherit the index order.**
  `TestEdgeListsInheritTheIndexOrder` covers this and must be run against the
  prepared text, since the join form passes every behavioural test and fails only
  this one. Its two batch cases hardcode `IN (?,?)` and their own `ORDER BY`
  (`store_test.go:969-978`); they move to `stmtSQL[…]` for the two new statements,
  and their args become one JSON string apiece. Copying the `json_each` text into
  the test by hand would leave exactly the gap the bullet above exists to close.
- The conditions read keeps `ORDER BY object_id, type` without a temp B-tree, for
  the same reason.
- **`unblockedTargetsChunk`'s plan is pinned as it will be, with the sort.** The
  point is that the change is deliberate and visible, not that the plan is
  unchanged.
- An id list binds as numbers: a test that a converted read finds a row by an id
  **above 2⁵³**, not 2³². `json_each` reports `typeof = integer` up to
  9223372036854775807, so the reachable failure is a stringified binding; putting
  the boundary above 2⁵³ also rules out a value parsed as REAL.
- `renderedSQLSites` names six functions, and
  `TestOnlyRenderedSQLLivesInAFunction` still passes.

Benchmark `Objects().ListByIDs` at one id and at 64, on disk, against the commit
before this one — the two ends of the table in *Why*, on the path the tail
actually runs. **And `unblockedTargets` at one id**, which is the only converted
statement whose plan changes.

## Not in this spec

**The `VALUES` tuple sets** — `reconcileOwedSweepQuery`, `ListStaleSince` (both
through `kindTuples`), `appendWriteLogUpdates`, `upsertConditions`,
`markManyForDeletionChunk`. A `(group, kind)` pair set can become
`(d."group", d.kind) IN (SELECT value ->> 0, value ->> 1 FROM json_each(?))`, and
a bulk insert can become `INSERT … SELECT … FROM json_each(?)`. Neither is
measured and neither has its plan checked. Separate spec, after this one has
shown the pattern works on the easy shape.

**`Events().List`.** Its `WHERE` is assembled from optional predicates, so the
constant form is `(?1 IS NULL OR category = ?1)` and a `LIMIT ?` bound to −1.
That is a different technique with a different risk — an `OR` over a bound NULL
can cost the index — and it needs its own plan check.
