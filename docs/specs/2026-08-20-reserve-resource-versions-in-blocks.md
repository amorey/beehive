# Reserve resource versions in blocks

- **Status:** Planned. The first item that spends the sole-writer constraint.
- **Date:** 2026-08-20
- **Depends on:** nothing, but reads better after
  [prepared statements](2026-08-20-cache-prepared-statements.md), which removes
  the part of this cost that is not the page write.

## Why

Every write draws its version with a statement that dirties a page:

```sql
UPDATE resource_version_seq SET value = value + ? WHERE id = 1 RETURNING value
```

Measured on an arm64 sandbox, on disk, inside a transaction:

| | |
|---|---|
| one-row `UPDATE` + `COMMIT` | 20.6 µs |
| the same, plus the sequence draw | 37.8 µs |

**The draw is about 16 µs, roughly 40% of a minimal write.** After the row write
itself it is the most expensive thing on the write path, and every write pays it:
a spec write, a status write, a condition bump, an event, the observed-generation
stamp.

Beehive is the store's only writer
([ADR](../adr/2026-08-05-one-process-one-beehive-sole-writer.md)), so it can draw
a block of versions once and hand them out from memory.

## The change

`sqliteStore` holds an allocator. It reserves `blockSize` versions with the
batched draw that already exists (`advanceResourceVersion(ctx, c, n)`), then
serves from memory until the block is spent.

```go
// versions hands out resource versions from a reserved block. The reservation
// is committed before any version in it is used, so a crash burns the rest of
// the block and the next process starts above it. Versions must be unique and
// increasing; they are not required to be contiguous.
type versions struct {
	mu   sync.Mutex
	next int64 // next version to hand out
	end  int64 // one past the last version in the reserved block
}
```

`nextResourceVersion` and `advanceResourceVersion` keep their signatures and go
through it. When the block is spent they reserve the next one, on the ambient
connection, inside whatever transaction is running.

## Edge cases the implementer would otherwise guess at

- **The reservation must commit before the versions are used.** Reserving inside
  a transaction that then rolls back would hand out versions the sequence row no
  longer covers, and a restart would reuse them. Reserve on the pool, outside any
  ambient transaction, even when called from inside one. This is the one rule the
  whole design rests on.

- **Gaps are fine; going backwards is not.** Every cursor in the system compares
  `>`: the waker's watermark, the tail's position, `ListStaleSince`'s cursor,
  `WatermarkSet`'s monotonic guard. A crash leaving a hole of up to `blockSize`
  costs nothing. Say this in the code, because it is what a reader will doubt.

- **`GetForReconcile` reads the sequence row as its cursor** (`sqlite/store.go:830`,
  the `SELECT value FROM resource_version_seq` subquery). It must read the
  allocator instead, or a pass records a dependency watermark below a version
  already handed out and under-reports staleness. This is the trap: the subquery
  is easy to miss, and the failure is silent.

- **`GetLatestResourceVersion` has the same problem** (`:1013`), and the
  stale-dependents pass uses it as its `through` bound. Reading the allocator's
  `next - 1` is right: it is an upper bound on what any committed write took.

- **A rollback burns versions.** A spec write that rolls back has already taken
  its version. That is true today too — the draw is the first thing in the
  transaction — so nothing changes.

- **`blockSize` is a constant, not an option.** 256 makes the amortized draw
  0.06 µs and bounds a restart's gap at 255. Keep it unexported; there is no
  workload that wants to tune it.

- **`Store` is a public extension point**, so the allocator lives in the sqlite
  store beside the writes, never in `Beehive`.

## Tests

In `sqlite/store_test.go`:

- Versions increase across a block boundary, with `blockSize` shrunk by a test
  var.
- A store closed and reopened over the same file hands out versions above every
  version the first one used, including the unused tail of its block.
- A rolled-back write does not make the next write reuse its version.
- `GetForReconcile`'s cursor is at least the version of the write that preceded
  it — the regression test for the subquery above.
- `GetLatestResourceVersion` is never below a version a committed write took.
- The write log's ordering is unchanged: entries still sort by version in the
  order they committed.

## On ship

ADR: **resource versions are reserved in blocks**. It records the two rules —
reserve outside the ambient transaction, and versions are unique and increasing
but not contiguous — and names the sole-writer ADR as what makes it sound.

`CLAUDE.md`'s store section says nothing about the sequence today. Add the
"unique and increasing, not contiguous" sentence, because it is a property every
cursor in the system depends on.
