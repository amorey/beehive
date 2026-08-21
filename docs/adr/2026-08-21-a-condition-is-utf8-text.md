# A condition is UTF-8 text, and its upsert is prepared

- **Status:** Accepted — `sqlite/statements.go`, `Conditions().Set`.
  Retires `docs/specs/2026-08-21-bind-the-conditions-upsert.md`, which git holds.
- **Date:** 2026-08-21
- **Builds on:** [Bind the tuple sets as JSON](2026-08-21-bind-the-tuple-sets-as-json.md)

## Context

`upsertConditions` rendered eight placeholders per condition, so a controller
writing one condition and one writing three compiled different statements every
pass. It was the last `VALUES` tuple set, and the only one carrying **free
text**: `type`, `status`, `reason` and `message`, where `message` is whatever a
controller wrote — commonly an `err.Error()`, which Go does not guarantee is
UTF-8.

JSON has no representation for bytes that are not UTF-8, and `json.Marshal`
substitutes U+FFFD rather than failing. Measured through the converted statement,
a `message` of `"bad\xffbyte"` came back `"bad�byte"`. So the conversion was
not available until the encoding question was answered.

## Decision

**A condition's text must be valid UTF-8, and `Conditions().Set` refuses it
otherwise** with `ErrInvalidCondition`, checked in the loop that already rejects
a duplicate type.

The gate covers all four columns, not the three that were lossy. `type` was
already gated — a corrupted lookup key never matches its stored row again, so
every `Set` would read the condition as new and wake every watcher — and
`status`, `reason` and `message` now join it. A rule holding for one column of a
row and not the other three is harder to state than one holding for the row, and
`status` is a bare `string` on `storeapi.Condition` however typed
`ConditionStatus` is on the client surface.

**`ErrInvalidConditionType` became `ErrInvalidCondition`** with it: a sentinel
reading "condition type must be valid UTF-8" cannot answer for a message.

What this gives up is storing a condition whose text is not text. Faithful
storage of such a value was never worth much — nothing downstream can render it —
and the gate turns it into an error at the write that produced it.

**With the encoding settled, the upsert binds one JSON array** of
`[type, status, reason, message, liveness]` rows. `object_id` and the timestamp
are the batch's, so they stay ordinary parameters.

Measured through the store, two runs each:

| conditions | rendered | prepared | |
|---|---|---|---|
| 1 | 123 µs | 104 µs | **−15%** |
| 3 | 144 µs | 124 µs | **−14%** |

A bare-statement probe put this at −51%. The store's path carries the
transaction, the gate read, the version bump and the write-log append around the
statement, so the parse is a smaller share of it — the same gap the tuple-set
record saw, and the reason both specs asked for store numbers before the record
was written.

## Consequences

**`WHERE true` is load-bearing.** `INSERT … SELECT … FROM json_each(?) ON
CONFLICT …` does not parse: SQLite cannot tell the `ON` of a join from the `ON
CONFLICT`. This is the only prepared statement with an upsert, so it is the only
one that needs the workaround.

**`tupleRows` and `placeholders` are gone.** This was the last of `tupleRows`'
four callers, and `placeholders` had only `tupleRows`. `renderedSQLSites` is down
to one entry, `Events().List`, which is measured as a regression and stays
rendered — see `docs/TODO.md`.

**`conditionChunkSize` stopped being a parameter ceiling.** Its comment cited
"SQLite's parameter limit at eight parameters a row" for both the gate read and
the upsert; the gate read has bound two parameters since the id-list change, and
the upsert now binds three. What the chunk bounds is the array built and the rows
written.

**`conn` is gone.** It was the choke point every data write took its connection
from, and this was its last caller — the coverage gate found it, since the
refusal branch stopped being reachable. What it guarded did not move: `writeStmt`
refuses a read frame at routing, and `refuseWriteInReadFrame` answers for the
sites that need it before an early return. `TestNoWriteBypassesConn` became
`TestNoWriteBypassesTheAccessors`, and `st.tx` is down to four users.

**`jsonConditionRows` is the one marshal helper that can be handed free text**,
and its comment says so. What makes it lossless is not the helper's shape — the
rule the other four rest on — but the gate above.
