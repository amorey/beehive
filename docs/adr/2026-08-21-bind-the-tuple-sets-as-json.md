# Bind the tuple sets as JSON

- **Status:** Accepted — `sqlite/statements.go`, four statements.
  Retires `docs/specs/2026-08-21-bind-four-rendered-tuple-sets.md`, which git
  holds.
- **Date:** 2026-08-21
- **Builds on:** [Bind an id list as JSON](2026-08-21-bind-an-id-list-as-json.md)

## Context

That record took the rendered SQL sites from twelve to six by binding `IN` lists
as JSON. Four of the six rendered a `VALUES` tuple set instead — a pair set for
two predicates, a bulk insert, and an assignment CTE — so their text still varied
with the data and none could be a field.

## Decision

**A tuple set binds as one JSON array of arrays, widened with `->>`.** A pair set
becomes `IN (SELECT value ->> 0, value ->> 1 FROM json_each(?))`; a bulk insert
becomes `INSERT … SELECT … FROM json_each(?)`. Four statements converted: the
owed-count reclaim, the stale-dependents scan, the batched deletion mark and the
batched write-log append.

Measured through the store, two runs each:

| | rendered | prepared | |
|---|---|---|---|
| deletion mark, 1 id | 116 µs | 80 µs | **−31%** |
| deletion mark, 128 ids | 2035 µs | 1684 µs | **−17%** |
| write-log append, 1 row | 40 µs | 35 µs | **−13%** |
| write-log append, 128 rows | 1065 µs | 891 µs | **−16%** |

**A bare-statement probe put the mark at −67%.** The store's own path carries the
transaction, the version draw and the log append around the statement, so the
parse is a smaller share of it than of the statement alone. The probe was right
about direction and wrong about size, which is why the spec asked for these
numbers before the record was written.

The two tick-driven statements are not benchmarked: the reclaim runs on the GC
tick and the scan on the 60s stale-dependents pass, where a probe put them at
−13% and −18% of single-digit-microsecond savings. They converted for the
structure — their kinds come from `Register` and never change, so the rendered
text was constant at runtime and could not be constant at compile time.

**Three non-generic marshal helpers, one per shape.** `jsonKinds`,
`jsonMarkPairs` and `jsonWriteLogRows`, beside the existing `jsonList` and
`conditionTypeList`. Not one widened generic: a `~string` constraint on a shared
helper is what let condition types reach a lossy encoder, and each of these says
in its own comment what it carries. `jsonKinds` and `jsonWriteLogRows` carry
`group` and `kind`, which are identifiers from `Register` rather than caller
data — the assumption that makes JSON lossless for them.

## Consequences

**An empty `keep` stopped being a special case.** The reclaim branched on it
because `NOT IN (VALUES)` is a syntax error. `NOT IN (SELECT … FROM
json_each('[]'))` is valid and matches every row, which is what keeping nothing
means, so the branch and the function that rendered it both went.

**The deletion mark's plan improved.** A `VALUES` CTE materialises into an
ephemeral table before the join; `json_each` feeds the join directly, so
`MATERIALIZE assigned` is gone and the primary-key seek is unchanged.

**`kindTuples` had two callers and both converted**, so it went. `tupleRows` and
`placeholders` did not: `upsertConditions` still renders eight placeholders per
row, and it is a separate decision because it carries free text.

**The classifier could not see a CTE-fronted write.**
`TestEveryStatementIsClassifiedByItsOwnText` anchored on the verb, and the
deletion mark starts `WITH assigned(…) AS (…) UPDATE`. Its regex now allows an
optional `WITH` prefix. Without that it called a write a read, which would have
prepared it on the read pool and turned a read-frame refusal into a driver error
on a cold path.

**`appendWriteLogUpdates` became a method** to reach `s.exec`, which left the
`dbtx` threaded through `markManyForDeletionChunk` with no user. The mark now
takes an explicit `refuseWriteInReadFrame` in its place, ahead of the version
draw so a doomed call burns no versions — the ordering the `s.conn` call used to
provide by accident.

**`renderedSQLSites` is down to two**: `upsertConditions` and `Events().List`.
The first is a contract decision away from converting; the second is measured and
declined, and `docs/TODO.md` says why.
