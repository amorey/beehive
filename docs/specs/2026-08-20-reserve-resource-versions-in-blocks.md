# Reserve resource versions in blocks

- **Status:** Planned. The first item that spends the sole-writer constraint.
  The refill design below is the second attempt: the first said "reserve outside
  the ambient transaction", which cannot be done. See *Where the refill happens*.
- **Date:** 2026-08-20
- **Depends on:** nothing, but reads better after
  [prepared statements](2026-08-20-cache-prepared-statements.md), which removes
  the part of this cost that is not the page write.

## Why

Every write draws its version with a statement that dirties a page
(`sqlite/store.go:627`):

```sql
UPDATE resource_version_seq SET value = value + ? WHERE id = 1 RETURNING value
```

Measured on an arm64 sandbox, on disk, medians of five runs of 300:

| | |
|---|---|
| one-row `UPDATE` + `COMMIT` | 14.7 µs |
| the same, plus the sequence draw | 40.5 µs |
| **the draw** | **25.8 µs** |

Against the real store, same machine, same method:

| | now | draw removed |
|---|---|---|
| `UpdateStatus` in a `Within` | 95.4 µs | ~69.6 µs (**−27%**) |
| `UpdateSpec` that changes the spec | 175.3 µs | ~149.5 µs (**−15%**) |

Every write pays it exactly once: a spec write, a status write, a condition bump,
an event, the observed-generation stamp. It is the largest single cost on the
write path after the row write itself.

Beehive is the store's only writer
([ADR](../adr/2026-08-05-one-process-one-beehive-sole-writer.md)), so it can draw
a block of versions once and hand them out from memory.

## The invariant this rests on

**Every version draw happens while the writer connection is held.** All six draw
sites sit inside a `Within` — `objectsCreate`, `recordObjectWrite` (five callers),
`Events().Add`, `markForDeletion`, `markManyForDeletion`, `objectsDelete` — and
the writer pool is one connection (`sqlitemigrate.OpenPool(path, 1)`). So write
transactions draw, and commit, in one order. Nothing below is safe without that;
pin it with a test.

## The change

`sqliteStore` holds an allocator:

```go
// versions hands out resource versions from a block reserved by a committed
// write. Versions must be unique and increasing; they are not required to be
// contiguous, so a crash or a rollback may burn the rest of a block.
type versions struct {
	mu        sync.Mutex
	next      int64 // next version to hand out
	end       int64 // one past the last version in the reserved block
	published int64 // highest version handed out by a committed transaction
}
```

`nextResourceVersion` and `advanceResourceVersion` become methods on
`*sqliteStore` and route through it. Every sub-API embeds `*sqliteStore`, so the
call sites change only by receiver.

A draw of `n`:

- If `end - next >= n`, take `[next, next+n-1]` from memory and return. No SQL.
- Otherwise **fall back to today's statement** on the ambient connection, and set
  `next = end` so the block is spent. The fallback must not leave usable versions
  behind it: the table already holds the block's end, so a later memory draw would
  hand out a version *below* one the fallback returned, and versions would go
  backwards.

At `open`, read the sequence row for `published` and reserve the first block.
`open` has no transaction, so it takes the same safe path as a refill — and it is
what keeps the first write of each process off the fallback.

## Where the refill happens

The first draft of this spec said to reserve on the pool, outside any ambient
transaction. **That cannot be done, on two counts:**

- The writer pool is `MaxOpenConns(1)` and the caller is inside a transaction
  holding it. The reservation waits for a connection its own caller will not
  release — a hang, not an error, on the background context the store passes.
  Measured: `context deadline exceeded` after 2.008s against a 2s deadline.
- A second writer pool over the same file does not help: `_txlock=immediate`
  means the ambient transaction holds SQLite's write lock, so the reservation
  gets `SQLITE_BUSY`. This is a SQLite property; no amount of connections fixes
  it.

And reserving *inside* the transaction is unsafe: a rollback unwinds the
sequence write while memory keeps the block, so the next reservation draws from a
lower value and hands out versions already used. Measured: a block reserved
through 256 inside a transaction leaves the row at 0 once it rolls back.

So the refill runs where there is provably no transaction: **at the end of
`Within`, on the outermost path, after `tx.Commit()` succeeds.** `Commit` returns
the connection to the pool, so the refill's own statement autocommits and cannot
be rolled back by anything. Measured: the draw that deadlocks inside the
transaction takes 36 µs one line after the commit.

Two steps, in this order, and the order is the whole point:

1. **Publish**: `published = next - 1`. It must run before the refill, or
   `published` names a version out of the *new* block that no write has taken.
2. **Refill** if the block is spent (`next == end`): draw `blockSize` and set
   `next`/`end` from the result.

Refill only when spent — no low-water mark. A transaction that empties the block
mid-way falls back to SQL for its remaining draws, which is today's cost, and the
next commit refills.

Where in `Within` to put it: after the hook loop. A hook may re-enter `Within` and
draw, and if it does it takes the fallback — correct, just slower, and it keeps the
refill off the path where a hook could be holding a fresh transaction.

## The reader sites read the allocator, not the table

Once a block is reserved, `resource_version_seq.value` is the block **end** —
above what has been handed out. Both sites that read it as a cursor must read
`published` instead, and the failure if they do not is silent divergence, not
over-reporting:

- **`GetForReconcile`** (`sqlite/store.go:882`, the
  `SELECT value FROM resource_version_seq` correlated subquery). Its `Cursor`
  becomes `reconciled_against` on a successful pass, and `ListStaleSince` selects
  on `target.resource_version > reconciled_against`. Stamp the block end and every
  target write in the rest of that block is at or below the watermark: the
  dependent never learns, forever. This is the trap — the subquery is easy to
  miss and nothing fails.
- **`GetLatestResourceVersion`** (`:1065`), the stale-dependents pass's `through`
  bound. The pass moves its cursor to `through` when a sweep reaches the end, so
  a `through` above what has been handed out strands every write in the gap.

`published` is safe for both because draws are serialized by the writer
connection: at any instant every version below the open transaction's first draw
belongs to a committed write, and `published` is exactly that boundary as of the
last commit. It lags — a write is visible to readers for the moment between its
commit and its publish — and lagging over-reports staleness, which is the
harmless direction.

**Read `published` before the row read, not after.** `GetForReconcile` reads the
object on the read pool at some snapshot; a write committing after that snapshot
but before the `published` read would be both unobserved and at or below the
cursor. That is the divergence again, through a different door.

A second, independent reason the table cannot serve: both sites now read through
`s.read(ctx)`, so they see the read pool's snapshot rather than the writer's
state.

## `markForDeletion` assigns its version in SQL

`markForDeletion` (`:2352`) does not call the draw at all — it writes

```sql
resource_version = (SELECT value + 1 FROM resource_version_seq WHERE id = 1)
```

and calls `nextResourceVersion` afterwards to commit the value it took. With a
block reserved the two no longer agree: the marked row takes *block end + 1*
while the allocator is somewhere inside the block, and that version is handed out
again later. Duplicate `resource_version` on `objects` and in the write log, and
every cursor compares `>`, so the second one is dropped silently by the tail and
the waker.

**Draw from the allocator up front and bind it as a parameter.** The comment
there justifies the lazy draw by the counter write a repeat delete would commit
for nothing; a memory draw costs nothing, so the reason is gone. A repeat delete
now burns a version instead, which gaps make free.

## Edge cases the implementer would otherwise guess at

- **Gaps are fine; going backwards is not.** Every cursor in the system compares
  `>`: the waker's watermark, the tail's position, `ListStaleSince`'s cursor,
  `WatermarkSet`'s monotonic guard. A crash leaving a hole of up to `blockSize`
  costs nothing. Say this in the code, because it is what a reader will doubt.

- **`n` larger than what the block has left** takes the fallback, which spends the
  block. `markManyForDeletion` draws `len(ids)`, bounded by `markChunkSize` (128),
  so with `blockSize` at 256 it usually fits and never costs more than today when
  it does not.

- **A rollback burns versions.** True today too — the draw is the first thing in
  the transaction — so nothing changes, except that a rolled-back *refill* is now
  impossible by construction rather than by luck.

- **`blockSize` is a constant, not an option.** 256 amortizes the draw to 0.1 µs
  and bounds a restart's gap at 255. Keep it unexported; there is no workload that
  wants to tune it. A `var` so tests can shrink it.

- **`Store` is a public extension point**, so the allocator lives in the sqlite
  store beside the writes, never in `Beehive`.

## Tests

In `sqlite/store_test.go`:

- Versions increase across a block boundary, with `blockSize` shrunk by the test
  var.
- A store closed and reopened over the same file hands out versions above every
  version the first one used, including the unused tail of its block.
- A rolled-back write does not make the next write reuse its version.
- A transaction that exhausts its block mid-way keeps handing out increasing
  versions — the fallback's monotonicity rule, and the one bug the naive fallback
  has.
- `GetForReconcile`'s cursor is at least the version of the write that preceded
  it, and never above the version of a write it did not observe. The regression
  test for the subquery.
- `GetLatestResourceVersion` is never below a version a committed write took, and
  never above the highest version handed out.
- A repeat `markForDeletion` does not reuse a version, and the marked row's
  version matches its write log entry.
- The write log's ordering is unchanged: entries still sort by version in the
  order they committed.
- **Every draw holds the writer connection** — the invariant above. Cheapest
  structural form: a test that asserts each draw site is reached only with a live
  transaction on the ctx.

**The failure-injection suite needs a plan.** Five `dropSeq` calls, four inline
`DROP TABLE resource_version_seq`, and three `blockResourceVersionDraws` isolate
version-draw error branches by breaking the table. With a block in hand the draw
does not touch it, so those branches go unreachable and the tests fail on
`require.Error` against nil. Setting `blockSize` to 0 makes every draw take the
fallback, which is today's statement, so those tests pass unchanged with one line
of setup. `blockResourceVersionDraws`'s doc describes `markForDeletion`'s draw
specifically and needs rewriting alongside it.

## On ship

ADR: **resource versions are reserved in blocks**. Three rules to record — the
refill runs after the outermost commit and nowhere else, versions are unique and
increasing but not contiguous, and the cursor sites read the allocator rather
than the table — plus the sole-writer ADR and the one-writer-connection fact as
what makes it sound.

`CLAUDE.md`'s store section says nothing about the sequence today. Add the
"unique and increasing, not contiguous" sentence, because it is a property every
cursor in the system depends on.
