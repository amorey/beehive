# A read-only transaction

- **Status:** Ready to build. Measured against the shipped read pool and version
  blocks. **The case has moved twice**: the writer-side win it was first justified
  by is gone at the shipping throttle, and the drain-read win that replaced it is
  absorbed by the tailer's scan floor. What survives is the two reads no floor
  covers — a watch's opening snapshot, object or event — where the cost stops
  scaling with write pressure. See *What it is worth*. The prototype is not kept;
  git holds it.
- **Date:** 2026-08-20
- **Depends on:** the read pool, which shipped
  ([ADR](../adr/2026-08-20-reads-get-their-own-connections.md)). A grouped read
  belongs on that pool, and needs a transaction there to see one instant across
  its statements.

## Why

Five places group two or more reads so they describe one instant. They do it
with `Within`, and the DSN sets `_txlock=immediate`, so **a read takes the write
lock**:

- `Events().Snapshot` (`sqlite/store.go:2153`) — the runs and the position.
- `Events().ListSince` (`:2177`) — the page, the horizon and the existence probe.
- `ObjectWrites().ListSince` (`:3208`) — the page, the horizon and the row images.
- `snapshot` (`:3407`) — a listing and the write log's mark.
- `Events().Sweep`'s candidate scan (`:2266`, via `overCapTimelines` at `:2315`).

The third is every watch tailer's page read, on every drain.

Measured on an arm64 sandbox, on disk:

| | |
|---|---|
| empty `BEGIN IMMEDIATE` + `COMMIT` | 10.2 µs |
| empty `BEGIN` (deferred) + `COMMIT` | 7.4 µs |

So about 2.8 µs per grouped read today, and a write lock held for no reason.
The lock costs nothing while the pool is one connection, because everything
already queues there.

On disk only: `OpenMemory`'s DSN carries no `_txlock`, so an in-memory `Within`
is already a deferred `BEGIN` and none of this applies to it. That is a
convenience for the tests and a trap for them too — see the enforcement note
below.

With the read pool it stops being merely wasteful. These five reads belong on the
reader, and the reader is opened `query_only(true)`, which refuses a write lock —
so `IMMEDIATE` there does not work at all. This is what lets those call sites move.

`modernc.org/sqlite` gives us the deferred begin for free: `newTx` applies the
DSN's `_txlock` only when `opts.ReadOnly` is false (`tx.go:18-31`). So
`BeginTx(ctx, &sql.TxOptions{ReadOnly: true})` is a plain `BEGIN` with no DSN
change.

The reader's DSN omits `_txlock` anyway, so `beginMode` is empty there and the
begin would be deferred without the flag. Pass it regardless: it is what states
the intent, and it is what keeps this true if the reader's DSN ever gains one.

## What it is worth, measured

A prototype — `withinRead` over the four read-only sites — passed the whole suite
unchanged, so what follows is the real behaviour rather than an approximation.
On disk, medians of alternating runs a side.

### Where it pays: the reads no floor covers

Two of the four sites run once per watch, on the caller's own latency, with
nothing pacing them. They are where a user feels this.

`Events().Snapshot` — an event watch's start (`eventswatch.go:151`):

| writers | now | with `withinRead` | |
|---|---|---|---|
| 0 | 34.9 µs | 34.1 µs | −2% |
| 1 | 112.6 µs | 47.3 µs | **−58%** |
| 4 | 303.6 µs | 43.1 µs | **−86%** |

`ObjectWrites().Snapshot` — an object watch's initial listing, 200 objects
(`objectswatch.go:220-224`):

| writers | now | with `withinRead` | |
|---|---|---|---|
| 0 | 755.5 µs | 760.2 µs | +1% |
| 1 | 835.8 µs | 860.6 µs | +3% |
| 4 | 1094.1 µs | 852.9 µs | **−22%** |

The percentages matter less than the shape: today these reads **scale with write
pressure**, because `BEGIN IMMEDIATE` puts them in the writers' queue for the one
connection. On the reader they flatten — 34.9 to 43.1 µs across four writers
rather than 34.9 to 303.6. The object listing moves less because 750 µs of it is
the listing itself, not lock waiting, and at one writer it is 3% *worse*: a read
transaction on the reader gives up the writer's warm page cache. That cost is
fixed; the saving grows.

### Where it does not: the read behind the floor

`ObjectWrites().ListSince` is the tailer's drain read, and the drain is floored at
`defaultWatchScanMinInterval`:

| writers | now | with `withinRead` | |
|---|---|---|---|
| 0 | 43.4 µs | 42.3 µs | −2% |
| 1 | 169.7 µs | 119.6 µs | −30% |
| 4 | 380.4 µs | 119.8 µs | −68% |

**Do not lead with this row.** The floor absorbs it: a tailer that finishes its
drain sooner waits out the rest of the window regardless, which is exactly what
the delivery table below shows. What a faster drain leaves behind is a connection
held for less time, and that shows up on the writer side — measured next, at
nothing. `Events().ListSince` is floored the same way
(`eventswatch.go:103`, `rategate.NewSingle`).

### The writer side is now worth nothing

`BenchmarkWritesUnderWatch` at the shipping throttle, which is what an embedder
actually runs:

| | now | with `withinRead` |
|---|---|---|
| no-watcher | 155.6 µs | +1% |
| one-watcher | 157.5 µs | +0% |
| 64-watchers-one-kind | 157.1 µs | +1% |
| 16-kinds-one-watcher-each | 163.3 µs | **−3%** |
| one-owner-scoped-watcher | 157.0 µs | −0% |

**An earlier draft of this spec claimed 2–8% here. It does not reproduce.** That
figure was measured before version blocks shipped, against a more expensive write.
Only the 16-kind row moves at all, and 3% is at the edge of this sandbox's noise.

The reason is the scan floor: `defaultWatchScanMinInterval` caps a tailer at ten
drains a second, so even a 380 µs drain is 3.8 ms of the connection per tailer per
second. Sixteen tailers is 6% of it, and that 6% is the whole of what this change
can give a writer back. Do not build this expecting the write path to move.

With the floor at 0 — `withWatchScanMinInterval`, unexported, so unreachable by an
embedder — the same benchmark moves −7% to −25%. That row is what the writer side
would be worth if drains were not paced, and it is here only to explain the one
above it.

### It does not make watches faster

Delivery latency — commit to the watcher seeing it — is set by the tailer's scan
floor and nothing else. Measured with the earlier prototype, 50 iterations, and
not re-run: the mechanism is the floor, and this change does not touch it.

| throttle | kinds | before | + this |
|---|---|---|---|
| 100ms (default) | 1 | 98.9ms | 99.8ms |
| 100ms (default) | 16 | 99.4ms | 99.8ms |
| 0 | 1 | 0.33ms | 0.34ms |
| 0 | 16 | 5.35ms | 3.90ms |

At the default, sixteen concurrent tailers deliver as fast as one. Anyone who
wants faster delivery should be looking at the floor, not at this.

## The change

A sibling of `Within`, unexported on `sqliteStore` (see *Exported, or not*):

```go
// withinRead runs fn inside a read transaction on the read pool: every read sees
// one snapshot, and no write lock is taken. A write issued inside fn is a
// programming error. The reader pool refuses it with "attempt to write a
// readonly database" — from its query_only pragma, not from ReadOnly below —
// and the error is not wrapped into anything a caller can handle.
//
// A nested withinRead joins the ambient transaction, read or write. Nested inside
// a write transaction it is therefore a write transaction, which is correct: the
// outer one already holds the lock, and its uncommitted writes must be visible.
func (s *sqliteStore) withinRead(ctx context.Context, fn func(ctx context.Context) error) error
```

Implementation is `Within` with four differences: it begins on `s.readDB`,
`BeginTx` gets `&sql.TxOptions{ReadOnly: true}` (which is what makes it a
deferred `BEGIN` — `modernc` applies the DSN's `_txlock` only when `ReadOnly` is
false), `txCount` is not incremented, since it counts write transactions and a
test asserts a fast path took none, and **the version allocator is not settled**.

That last one is newer than this spec and easy to copy in by accident. `Within`
ends with `s.versions.publish(...)` and `s.refillVersions(ctx)`
([ADR](../adr/2026-08-20-reserve-resource-versions-in-blocks.md)). A read
transaction draws no version, so the publish is a no-op — but the refill is a
*write*, on `s.db`, and putting it at the end of a read is both wrong and a way
to make a read block on the writer connection it was moved off. Leave both out.

**A nested call joins by running `fn` on the same ctx, not by taking a
savepoint.** A savepoint is a rollback boundary, and a read has nothing to roll
back. The measured prototype is:

```go
func (s *sqliteStore) withinRead(ctx context.Context, fn func(ctx context.Context) error) error {
	if st := liveTx(ctx); st != nil {
		return fn(ctx) // already grouped, read or write
	}
	tx, err := s.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	...
}
```

Note what the prototype skipped and the real one must not: it dropped the
`AfterCommit` queue. See the hook rule below.

**The hook queue is not one of the differences.** A read transaction still opens
a frame, so `AfterCommit` queues onto it, and a read transaction still commits —
releasing its snapshot. Flush on that commit exactly as `Within` does. Skipping
the flush because "a read has nothing to defer to" drops the hook silently, which
is the one way this can go wrong without failing.

Four of the five call sites switch to it. `Store` is unchanged, so `fakeStore`
needs nothing.

`Events().Sweep` is the fifth and does **not** switch: its candidate scan is a
read but the trim that follows is a write, in the same transaction. Moving the
scan alone would split them across two connections and two snapshots, which is
the one thing the grouping exists to prevent.

## Is it worth building

Yes, on three grounds, none of which is the one this spec was first written on:

- **It settles the export question that
  [cache prepared statements](2026-08-20-cache-prepared-statements.md) turns on.**
  That spec is the widest win in the set and blocked until this one decides. This
  is now the strongest reason, and it is worth being plain about that.
- **Opening a watch stops scaling with write pressure.** −86% on an event watch's
  snapshot under four writers, −22% on an object watch's listing. Both flatten;
  the saving grows with load rather than shrinking.
- **It is the right shape.** Four pure reads taking `BEGIN IMMEDIATE` on the
  writer is wrong on its face and stays wrong however little it costs.

What it is **not** worth building for, all three claimed by earlier drafts and
none surviving measurement: the write path, which does not move at the shipping
throttle; watch delivery latency, which is floor-bound; and the tailer's drain
read, which is behind that same floor. A change measured on a floored path buys
nothing a user can see — the spec applies that test to its own writer-side
number, and it has to survive being turned on the read side too.

Both specs that outranked it are gone — resource version blocks
[shipped](../adr/2026-08-20-reserve-resource-versions-in-blocks.md), and folding
the spec write's read into its `UPDATE` was
[measured and declined](../adr/2026-08-20-a-spec-write-reads-before-it-writes.md) —
so this is first, which the README reflects.

## Exported, or not

[Cache prepared statements](2026-08-20-cache-prepared-statements.md) turns on this
choice, so make it here.

A cached statement is one compiled handle, and two callers sharing one corrupts
both silently. Inside a transaction the connection is already held, so sharing is
possible — unless nothing can reach that transaction concurrently.

- **Unexported**, used only by the four call sites above: all are the store's own
  and single-threaded, so statements inside a read transaction can be cached.
  `ObjectWrites().ListSince` is every watch tailer's page read, so this is the hot
  path.
- **Exported on `Store`**: a caller can fan out inside it, and statements inside a
  read transaction must not be cached.

**Recommendation: keep it unexported** until something outside the store needs
grouped reads. Nothing does today, and exporting costs the tailer's page read for
a capability with no caller. If it is exported later, the caching rule changes
with it — record that dependency in both ADRs rather than in one.

## Edge cases the implementer would otherwise guess at

- **`ReadOnly: true` enforces nothing. `query_only` does.** modernc reads the
  flag only to pick the begin verb (`tx.go:18-31`); it is not a permission. What
  refuses a write inside `withinRead` is the reader pool's `query_only` pragma,
  and **`OpenMemory` has no reader pool** — `readDB` is aliased to `db`, so there
  a write inside a grouped read *succeeds*. Measured, same statement both ways:

  | pool | write inside the read transaction |
  |---|---|
  | `Open` (reader pool) | `attempt to write a readonly database (8)` |
  | `OpenMemory` (aliased) | `<nil>` — it commits |

  Nine call sites in the whole sqlite suite open on disk, against ~380 tests. So
  a stray write inside a grouped read is invisible to almost all of it and breaks
  only in a deployment. Two consequences: the godoc must credit the pragma rather
  than the flag, and the test for the refusal **must be on disk** or it fails
  outright.

- **Nesting a write inside a read is not defended against.** A `Within` reached
  from inside a `withinRead` joins the read transaction, and on disk its writes
  fail at the SQLite level. That is loud and correct where it is loud at all. Do
  not add a runtime check for it.

- **The savepoint machinery is shared.** `txState` does not care which begin verb
  opened the transaction, so nested frames, `sealForCommit` and the unwind rules
  are untouched. Do not fork them.

- **A read transaction holds one of the reader's N connections** for its whole
  life. `WithReadConnections(1)` is legal — `sqlite/sqlite.go:67` rejects only
  below 1 — and at N=1 a snapshot over a large kind blocks every other read for
  its duration: client `Get`s, driver scans, the lot. Not a regression in
  aggregate, since today those same reads queue behind the single writer instead,
  but it moves who waits, and the default of 4 is what keeps it unremarkable.

- **A commit wake can arrive inside an open snapshot.** An autocommit read
  starts a fresh snapshot every statement, so a wake is always seen. Grouped in a
  transaction, a wake arriving mid-read is answered from before that commit. For
  the object tailers the floor tick bounds it. For the dependency waker, which by
  design has no tick, it falls through to the 60s stale-dependents pass — so
  **the waker's reads must not be grouped**, and `ObjectWrites().ListSinceAll`
  (`:1253`) is deliberately not on the list above. Say so where it is not, or
  someone will add it for symmetry.

- **A read transaction pins the WAL**, and this is the change's real operational
  cost, measured in [`TODO.md`](../TODO.md): with autocommit reads only, 2000
  inserts peak at 4.1MB and a checkpoint truncates to nothing; with one open read
  transaction, 25.8MB and the checkpoint comes back busy.

  It is smaller here than that table suggests. **The tailer's drain calls
  `ObjectWrites().ListSince` once per page** (`objectswatch.go:600`), and each call
  is its own transaction, so a snapshot is held for one page read — 120 µs
  measured, not the length of a drain. Do not restructure the drain to hold one
  transaction across its pages; that is what would turn this into the table above.

  `snapshot` (`:3407`) is the exception and the one to watch: it lists a whole kind
  and reads the mark in one transaction, so its hold grows with the kind. If any
  site earns `journal_size_limit`, it is that one.

## Tests

In `sqlite/store_test.go`:

- A `withinRead` that groups two reads sees one snapshot: write between them from
  the same store and the second read must not see it. Drive the write from
  another goroutine through a seam, not by timing.
- A write inside `withinRead` fails, and the failure is the driver's. **On disk**:
  `OpenMemory` aliases the reader to the writer, which has no `query_only`, so in
  memory the write succeeds and this test would fail.
- `withinRead` does not increment `txCount`, and `TestSpecWriteTakesItsTransaction`
  (or whichever test reads that counter) still passes.
- A `withinRead` nested in a `Within` commits with the outer one, and its reads
  see the outer transaction's uncommitted writes.
- `AfterCommit` inside a `withinRead` runs when it commits, not never. Pin it:
  the hook queue is the part most likely to be dropped as "not needed for a
  read", and a dropped hook fails nothing. The measured prototype dropped it.
- On disk, a grouped read runs on the reader while a write transaction is open —
  the property the whole change is for, and one `OpenMemory` cannot show, since
  the reader is the writer there.
- `Events().Sweep` still trims what its scan found, in one transaction on the
  writer.
- The waker's `ListSinceAll` is still ungrouped, so a commit wake mid-scan is
  seen. Without this the next reader groups it for symmetry and the waker's only
  backstop becomes the 60s pass.
- `withinRead` settles no versions: a read transaction neither publishes nor
  refills. Cheapest form is a structural one — `refillVersions` has exactly one
  caller.

Everything above except the last two already exists for `Within` and passed
unchanged against the prototype, so a red suite during the build is a signal, not
a rewrite. The prototype's own gap was the hook queue, which it dropped.

## On ship

ADR: **a read that groups is a read transaction**. It records the one rule that
matters for later readers — `Within` means "I intend to write", and choosing it
for a read is now a mistake rather than a style. Note the modernc detail, because
the whole thing rests on it and it is invisible in our code.

Record the grouped-read table, and record that the writer-side figure an earlier
draft carried did not survive re-measurement: the next person to reach for this
shape will reach for it expecting the write path to move.

The read-pool ADR and this one describe one boundary from two sides; cross-link
them rather than restating either. `CLAUDE.md`'s reads bullet says "a grouped read
still takes the writer, because the five that self-wrap in `Within` have not
moved" — four of the five move here, and that sentence becomes the record of which
one did not.
