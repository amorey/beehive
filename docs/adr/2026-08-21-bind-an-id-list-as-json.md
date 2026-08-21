# Bind an id list as JSON

- **Status:** Accepted — `sqlite/statements.go`, seven statements.
  Retires `docs/specs/2026-08-21-bind-an-id-list-as-json.md`, which git holds.
- **Date:** 2026-08-21
- **Builds on:** [Prepare every constant statement at startup](2026-08-21-prepare-every-constant-statement.md)

## Context

That ADR left twelve functions holding SQL rendered from a runtime count, and
seven of them rendered an `IN` list — one `?` per id. Rendered text cannot be a
prepared statement, so each of those seven parsed and planned on every
execution, and paid the residency toll besides.

`IN` is the one shape SQLite lets you write without the count.

## Decision

**An id list binds as one JSON array, unpacked with
`IN (SELECT value FROM json_each(?))`.** The text stops varying, so the
statement becomes an ordinary field. Seven converted: the batched conditions
read, the owed stamp, the write log's row images, the condition set's load, the
unblocked targets, the two batched edge lookups, and `Objects().ListByIDs`.

`json_each` is core SQLite — JSON1 has been built in since 3.38 and `modernc`
ships 3.53.2 — so there is no build tag and no version gate.

Measured on disk, alternating invocations:

| | rendered | JSON | |
|---|---|---|---|
| `Objects().ListByIDs`, one id | 65 µs | 23 µs | **−64%** |
| `Objects().ListByIDs`, 64 ids | 406 µs | 309 µs | **−24%** |
| `unblockedTargets`, one id | 43 µs | 18 µs | **−53%** |

One id wins most, and that is the ordinary shape: the watch tail pages one
object at a time on a quiet beehive, and a client delete asks about one source.

**A subquery, never a join against `json_each`.** Driving from the JSON makes it
the outer loop, so rows arrive in the array's order and every `ORDER BY` has to
sort them — the temp B-tree `TestEdgeListsInheritTheIndexOrder` exists to keep
out. The subquery leaves the index as the driver and adds only the list's own
materialisation.

**Values marshal as numbers, never as strings.** `json_each` gives a JSON string
TEXT affinity, and TEXT compared against an INTEGER column matches nothing — a
silent empty result, not an error. `storeapi.ObjectID` is `int64`, so a slice of
them is correct by construction; the trap is only reachable by converting first.
`TestAnIDListBindsAsNumbers` pins the boundary above 2⁵³, which also rules out a
value parsed as REAL.

**One helper, and the error is decided in it.** `json.Marshal` of a slice of
integers or strings cannot fail — no channels, no funcs, no NaN — so `jsonList`
discards the error rather than growing seven unreachable branches.

## Consequences

**A list of exactly one no longer gets its own plan.** SQLite folds `IN (?)`
into an equality constraint, and a plan built on one value of the constrained
column can carry an `ORDER BY` the index only provides within that value.
`json_each` cannot fold. Six of the seven are unaffected — their `ORDER BY`
leads with the column the list constrains — but `unblockedTargets` filters
`r.from_id` and orders by `r.to_id`, so it now sorts at every length where it
used to sort only above one.

It got 53% faster anyway: the sort is over the deletion-pending targets of a
single object, usually none, while the parse it stopped paying was the whole
statement's. `TestTheUnblockedTargetsReadSorts` pins the plan as it is, so the
trade stays visible rather than looking like a regression that slipped in.

**`Objects().ListByIDs` loses its arity ceiling.** One bound parameter has no
`SQLITE_MAX_VARIABLE_NUMBER` to exceed, so a caller that ignores `idChunkSize`
no longer gets an error. A widening, not a break. `idChunkSize` stays: what it
bounds now is the array built and the rows returned.

**The object listing is no longer split.** `listObjectsWhere` existed for the
one caller that rendered its tail; with that tail constant it had none, so it
went and `listObjects` is the only multi-row read.

**`renderedSQLSites` is down to six**, all of them `VALUES` tuple sets or
optional predicates. Those are separate work: a tuple set can become
`(value ->> 0, value ->> 1)` and a bulk insert an `INSERT … SELECT`, but neither
is measured and neither has its plan checked, and `Events().List` assembles its
`WHERE` from optional predicates, where the constant form risks an `OR` over a
bound NULL costing the index.
