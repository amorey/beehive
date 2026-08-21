# Resource versions are reserved in blocks

- **Status:** Accepted
- **Date:** 2026-08-20

## Context

Every write drew its resource version with a statement that dirties a page:

```sql
UPDATE resource_version_seq SET value = value + ? WHERE id = 1 RETURNING value
```

Measured on an arm64 sandbox, on disk, that draw is ~26 µs — the largest cost on
the write path after the row write itself, and every write pays it once.

Beehive is the store's only writer
([ADR](2026-08-05-one-process-one-beehive-sole-writer.md)), so it can draw a block
of versions once and hand them out from memory.

## Decision

`sqliteStore` holds a `versions` allocator (`sqlite/versions.go`). A draw takes
from the reserved block where it fits and falls back to the statement above where
it does not — a refused take spends the block, because the counter already holds
the block's end and anything left behind the fallback would be handed out below
what the fallback returned.

Three rules make it sound.

**The reservation runs where no transaction is open.** A reservation that rolls
back leaves the allocator handing out versions the counter no longer covers, and a
restart reuses them. It cannot run on the pool from inside a transaction either:
the writer is one connection and the caller holds it, so the draw waits for a
connection its own caller will not release — measured, a hang rather than an error.
A second writer pool does not help, because `_txlock=immediate` means the ambient
transaction holds SQLite's write lock. So the reservation runs at `open` and at the
tail of the outermost `Within`, after `tx.Commit()` has returned the connection and
**before the `AfterCommit` hooks** — a hook may open a transaction on that one
connection, and the refill would then queue behind it.
Its failure is swallowed: the commit has already landed, so it cannot be reported,
and the next draw raises it where a caller can act.

**A reservation that lands late is burned.** `refillVersions` runs after `Commit`
has released the connection, so two committing goroutines can both find the block
spent and both draw; a fallback draw inside a transaction can also land between a
refill's draw and its install. The block installed last is therefore not always the
block drawn last, and applying a stale one would hand out a version below one
already taken. `reserve` discards any block starting below `next`, which is the
whole of the fix: uniqueness was never at risk — no two reservations overlap —
only order. Serializing the draws would additionally save the redundant statement
when two collide, and is not worth a second lock for something this rare.

**Versions are unique and increasing, never contiguous.** Every cursor in the
system compares `>`, so a crash or a rollback burning the rest of a block costs
nothing. This is what lets the reservation be coarse, and what lets a guarded
`markForDeletion` draw a version it never stamps.

A burned version is free; a burned version that is also *published* is not quite.
`markForDeletion` draws before the `IS NULL` guard runs, so a mark the guard blocks
still advances the committing transaction's high draw. `GetLatestResourceVersion`
then moves for a delete request that wrote nothing, which defeats the
stale-dependents sweep's `cursor == mark` early-out and costs one real scan.
`requestDeletion`'s probe absorbs the steady-state already-pending case and a mark
that matches no row rolls back without publishing, so only the race between the two
reaches it. One wasted sweep, no correctness consequence — recorded because "gaps
are free" is what a reader will take from the rule above, and this is the corner
where it is not.

**The cursor sites read the allocator, not the counter row.** Once a block is
reserved that row holds the reservation's *end*, above what has been handed out.
`GetForReconcile`'s cursor becomes `reconciled_against`, and `ListStaleSince`
selects on `target.resource_version > reconciled_against`: a cursor above what was
handed out puts every target write in the rest of the block under the watermark,
and the dependent never learns. `GetLatestResourceVersion` bounds the
stale-dependents sweep and strands the same range. Both read `published` — the
highest version a committed transaction took — and `GetForReconcile` samples it
*before* the row read, or a write committing in between would be both unobserved
and at or below the watermark it stamps.

**A commit publishes its own draws, never the allocator's high water mark.**
Publication happens in the tail of `Within`, immediately after `Commit` released
SQLite's write lock and before the `AfterCommit` hooks — those wake the waker,
whose dependent samples the cursor on another goroutine. Another writer can be
mid-transaction by the time a commit publishes, holding a version out of the
block. Publishing `next - 1` would cover it, and a concurrent
`GetForReconcile` would stamp a watermark past a write it never saw, which
`ListStaleSince` then skips for good. So `txState` carries the highest version its
transaction took, and that is what its commit publishes; it is monotonic, because
two commits can reach the tail out of order. `TestEveryDrawSiteIsInsideATransaction`
pins the site list: a draw outside a transaction never publishes, which stalls the
cursor rather than overstating it, but stalls it silently.

`blockSize` is 1024 and unexported. It sits above `markChunkSize` (128) by enough
that a full deletion chunk usually fits — a chunk the block cannot cover takes the
fallback and burns the remainder.

## Consequences

Measured against the real store, blocks-off and blocks-on runs alternated so drift
cancels, medians of fifteen each side: a status write in a `Within` goes 98.0 µs →
81.0 µs (**−17%**) and a spec write that changes the spec 176.4 µs → 151.4 µs
(**−14%**). Repeated on this sandbox the status figure lands between −17% and −19%;
the low end is the one to quote. A deletion cascade is unmoved: its cost is the row writes, and the
`markManyForDeletion` range draw was already one statement for the level.

A restart leaves a gap of up to `blockSize`, which nothing observes.

The counter row no longer tracks draws one for one, so a test counting draws reads
the allocator (`handedOut`) rather than the row. The tests that exercise the
fallback's error branch set `blockSize` to 0, which is the only way to reach it.

This reverses the lazy draw in
[store write shapes](2026-07-30-store-write-shapes.md): `markForDeletion` drew its
version from the counter row inline, which a reserved block makes unsound.
