# Bind the conditions upsert

- **Status:** Proposed, not decided. Split out from
  [the tuple sets](../adr/2026-08-21-bind-the-tuple-sets-as-json.md), which share
  its mechanism: this is the one carrying free text, so it needs a contract
  decision before it can be built — see *The question this asks*.
- **Date:** 2026-08-21
- **Depends on:**
  [bind an id list as JSON](../adr/2026-08-21-bind-an-id-list-as-json.md), whose
  mechanism this is, and whose `ErrInvalidConditionType` this would widen. Also
  **on [the tuple sets](../adr/2026-08-21-bind-the-tuple-sets-as-json.md)**,
  which has landed: `tupleRows` has lost its other three callers, so this change
  is what deletes it.

## Why

`upsertConditions` (`sqlite/store.go:1948`) renders eight placeholders per
condition, so a controller writing one condition and one writing three compile
different statements, every pass, forever. It is on the reconcile path: every
`Conditions().Set` that survives the no-op comparison runs it.

Measured on this driver and schema, rendered against JSON-and-prepared:

| conditions | rendered | JSON, prepared | |
|---|---|---|---|
| 1 | 44.9 µs | 22.2 µs | **−51%** |
| 3 | 67.2 µs | 37.1 µs | **−45%** |

**The win is the preparation, not the shape.** Unprepared, the JSON form is
*slower* — 61.3 µs against 44.9 µs at one condition, +36% — because it adds a
virtual-table scan the `VALUES` form does not have. Both numbers above are the
preparation paying that back and more.

Measured on a probe, not through the store.

## The question this asks

Of the five statements sharing this mechanism, this is the one carrying **free
text**. Its columns are `type`, `status`, `reason` and `message`, and `message`
is whatever a controller wrote — commonly an `err.Error()`, which Go does not
guarantee is UTF-8. (`appendWriteLogUpdates` carries `group` and `kind`, but
those are identifiers from `Register`, not caller data.)

JSON has no representation for bytes that are not UTF-8, and `json.Marshal`
substitutes U+FFFD rather than failing. Measured through the converted
statement:

```
message through JSON: "bad<?>byte"     (U+FFFD where the 0xFF was)
```

That is the trap the id-list ADR already met on `type`, where the answer was
`ErrInvalidConditionType` — a gate in `Conditions().Set`, because a type is a
lookup key and a corrupted one never matches the stored row again, so every
`Set` would read the condition as new and wake every watcher. A corrupted
`message` is milder: it is stored, never looked up, and the damage is one
replacement character in text a human reads.

Three of the row's four text columns ride the array ungated: `status`, `reason`
and `message`. `status` is a bare `string` on `storeapi.Condition`
(`storeapi.go:140`) — the root package's `ConditionStatus` names three constants
(`types.go:333`) but nothing enforces them — so it is caller-supplied text in the
same reachability class as the other two.

**So: must a condition's `status`, `reason` and `message` be valid UTF-8?**

If yes, widen the existing gate to cover all three — three more checks in the
loop that already runs, and this conversion is lossless. If no, close this spec:
the store keeps rendering eight placeholders per row, and `tupleRows` stays alive
for it. Gating only `reason` and `message` is not an option worth taking: it
leaves the row half-covered, which is the shape the argument below rejects.

The gate is the recommendation, on three counts. A condition is a report meant
to be read, and all four columns are `TEXT` a human or a machine consumes. The
`type` is already gated, so a rule holding for one column of a row and not the
other three is harder to state than one holding for the row — which is the whole
argument, and it only lands if the widened gate covers every text column rather
than the two that first came to mind. And the
value being protected is already not useful text: a controller storing an
`err.Error()` with a stray byte gets it back faithfully today, but nothing
downstream can render it — so the gate turns a value that is quietly useless into
an error at the write that produced it.

It is still an API change, and that is why this spec asks rather than assumes.

## The change

`object_id`, `transitioned_at` and `updated_at` are the same for every row, so
they stay ordinary parameters and the array carries five values per condition:

```sql
INSERT INTO conditions
       (object_id, type, status, reason, message, liveness, transitioned_at, updated_at)
SELECT ?1, value ->> 0, value ->> 1, value ->> 2, value ->> 3, value ->> 4, ?2, ?2
  FROM json_each(?3) WHERE true
    ON CONFLICT(object_id, type) DO UPDATE SET …
```

### The helper it adds

None of
[the tuple sets](../adr/2026-08-21-bind-the-tuple-sets-as-json.md)' helpers
covers `[[type, status, reason, message, liveness], …]`, so this needs its own —
`jsonConditionRows(conds []storeapi.Condition) string`.

**It is the one exception to that spec's rule** that a marshal helper which
cannot be handed free text cannot lose it. This one can be handed free text, and
what makes it safe is not the helper's shape but the widened gate above. Its doc
comment has to say so, the way `conditionTypeList`'s already does — otherwise the
two specs together state a rule this one silently breaks.

### The rest

`conditionChunkSize` stays, and **its comment must be rewritten** (`:1891`). It
reads "bounds the conditions bound per statement — the gate read and the upsert
both — under SQLite's parameter limit at eight parameters a row." Half of that is
already stale: the gate read binds two parameters since the id-list change. This
makes the rest false. What still justifies the chunk is the size of the batch and
the array it builds, not a parameter count — say that, or the next reader trusts
the old reason.

The statement is a write and needs its `stmtWrites` entry, or it is prepared on
the read pool and fails as a driver error on a cold path rather than at routing.
`TestEveryStatementIsClassifiedByItsOwnText` derives the classification from the
text and catches a miss.

## Traps, all met while probing this

- **`WHERE true` is required, and its absence is a syntax error.** `INSERT …
  SELECT … FROM json_each(?) ON CONFLICT …` does not parse: SQLite cannot tell
  the `ON` of a join from the `ON CONFLICT`. The documented workaround is a
  `WHERE` clause before it. No other converted statement has an upsert, so this
  is the only one that hits it.

- **A repeated `?N` is how one argument fills two columns.** `transitioned_at`
  and `updated_at` both take `?2`. That requires numbered parameters, and the
  numbering must be contiguous — `?1` and `?3` with two arguments fails at
  `database/sql` with `missing named argument`, not at SQLite.

- **`->>` types by the JSON value.** A JSON string yields TEXT, `true` yields
  the integer 1. `liveness` is a Go bool and lands as 0/1 either way, so the
  stored representation does not move.

- **The `ON CONFLICT` semantics survive exactly.** Verified: `transitioned_at`
  holds its prior value on a converged write and moves on a status change, which
  is the `CASE` that column exists for.

## The helpers it removes

**This spec is what finally deletes `tupleRows` and `placeholders`, and nothing
else can.** `tupleRows` (`:3386`) has four callers, not three: this,
`appendWriteLogUpdates`, `markManyForDeletionChunk` and `kindTuples`. The other
three go with
[the tuple sets](../adr/2026-08-21-bind-the-tuple-sets-as-json.md), leaving this
one — so `tupleRows` survives that change and dies with this one. `placeholders`
(`:3380`) has a single caller, `tupleRows`, and follows it.

If this spec is closed, both live on for one call site each. That is the cost of
declining it, and it is most of the reason to take it.

`renderedSQLSites` loses `sqliteStore.upsertConditions`, leaving only
`sqliteEvents.List`. The list is asserted with `ElementsMatch`, so it moves in
the same change or the suite fails.

## Tests

In `sqlite/store_test.go`:

- `transitioned_at` holds across a converged write and moves on a status change;
  `liveness` round-trips as a bool; `updated_at` moves either way.
- One condition, several, and more than one `conditionChunkSize` chunk.
- The widened gate: a `status`, `reason` or `message` that is not UTF-8 is
  refused with the sentinel — one case per column, since a gate covering two of
  the three is the failure this spec exists to avoid — and a valid non-ASCII
  value round-trips through the upsert and back out of `Objects().Get`.
- `renderedSQLSites` no longer names it, and
  `TestOnlyRenderedSQLLivesInAFunction` still passes.

Benchmark `Conditions().Set` at one condition and three, against the commit
before this one — a converged set does not reach this statement, so the benchmark
must write something each pass.
