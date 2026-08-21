# A read that groups is a read transaction

- **Status:** Accepted — `withinRead` in `sqlite/store.go`, over four call sites.
  Retires `docs/specs/2026-08-20-a-read-only-transaction.md`, which git holds.
- **Date:** 2026-08-20

## Context

Five reads group two or more statements so they describe one instant. They did it
with `Within`, and the writer's DSN sets `_txlock=immediate`, so **a read took the
write lock**. On disk that is a `BEGIN IMMEDIATE` at 10.2 µs against a deferred
`BEGIN` at 7.4 µs, and a lock held for nothing.

With [the read pool](2026-08-20-reads-get-their-own-connections.md) shipped it
stopped being merely wasteful. Those reads belong on the reader, and the reader is
opened `query_only(true)`, which refuses a write lock — so `IMMEDIATE` there does
not work at all.

## Decision

`withinRead` runs fn inside a read transaction on the read pool. Four of the five
sites use it: `Events().Snapshot`, `Events().ListSince`,
`ObjectWrites().ListSince`, and the object listing's `snapshot`.
`Events().Sweep` does **not** — its candidate scan is a read but the trim that
follows is a write in the same transaction, and splitting them across two
snapshots is the one thing the grouping exists to prevent. The waker's
`ObjectWrites().ListSinceAll` stays ungrouped for a different reason: it has no
tick, so a commit wake arriving mid-scan must be seen, and a snapshot would answer
it from before that commit.

**`sql.TxOptions{ReadOnly: true}` is what makes the `BEGIN` deferred and nothing
else.** modernc reads the flag only to pick the begin verb (`tx.go:18-31`); it is
not a permission. This is invisible in our code and the whole change rests on it.

**What refuses a write inside one is the reader pool's `query_only` pragma** — and
`OpenMemory` has no reader pool, so there a write inside a read transaction
*commits*. Measured, same statement both ways: `attempt to write a readonly
database (8)` on disk, `<nil>` in memory. Nine call sites in the sqlite suite open
on disk against ~380 tests, so the pragma is absent almost everywhere the tests
run, so the pragma cannot be the guard.

**`conn` is.** It returns `(dbtx, error)` and refuses a read frame outright, so no
statement is issued at all. That works because of a property the package already
had: **every data write takes its connection from `conn`**, and the only direct
uses of `st.tx` are the savepoint statements. Two structural tests hold that.
`TestNoWriteBypassesConn` lists the functions carrying write SQL and asserts each
takes its connection from `conn` or from a caller that did; both sides are
receiver-qualified, because `Add`, `Delete`, `Set` and `Sweep` each name a write
on more than one sub-API and a bare name would let a new one pass on an existing
one's behalf. `TestTheTransactionHandleHasFiveUsers` covers the way around
`conn`: `st.tx` is reachable from `conn`, `read`, the two statement binders and
the savepoint statements, and nowhere else. A roster of verbs could hold neither — a verb added without going
through `conn` is also a verb nobody adds to a roster.

**`conn` is taken before a write's no-op early return**, not at first use. Several
writes compare first and return without writing when nothing changed; acquiring
after that would report success for a write misplaced inside a read transaction,
in exactly the case where the mistake is hardest to notice.

Note what this is and is not bought by. On disk the pragma is already total —
`query_only` rejects a write whatever Go method issued it, `UPDATE … RETURNING`
included. What `roDBTX` cannot catch is a separate point, about the *type*: it
keeps `QueryContext`, so a `RETURNING` write compiles onto it. The guard in `conn`
exists for `OpenMemory`, and that is the whole of it.

**So the alternative was to fix `OpenMemory` instead**, giving it a `query_only`
reader so both modes enforce identically. Declined, but it is the better long-term
shape and worth revisiting: `file::memory:` is per-connection, so a second pool is
a different and empty database, and the ways around that are a shared-cache DSN —
which trades this divergence for another, with no WAL and different locking — or
bracketing the frame with `PRAGMA query_only` on a pinned connection, which needs
verifying that SQLite honours the flip inside an open transaction.

The cost is a returned error on ~25 write paths that previously could not fail
there, and one write path (`Edges().Add`) folded from three `conn` calls to one.
`deleteWriteLogRows` now takes its `c` from its caller, as `appendWriteLog` did.

**`errWroteInReadTx` is unexported and undocumented on `Store`,** unlike every
other error those methods return. Deliberate: `withinRead` is unexported, so only
this store can build a frame that produces it, and no embedder implementing
`Store` can be in a position to match on it. It is a programming error, not a
condition a caller handles.

**`Within` and `withinRead` share one frame protocol** (`runTx`). Begin, install
the frame, seal, commit, settle, drain the hooks: that ordering is contractual,
and it had two copies with the rules written on one of them. `Within` passes the
version-settling tail; `withinRead` passes none.

**Nested, a read joins on a savepoint like any other frame** — `fr.st.nested`,
exactly as `Within` does. Not for the rollback: a read has nothing to roll back,
and that is the reason the spec gave for skipping it. The savepoint is doing two
other jobs. It is the frame's admission check, which rejects a ctx captured from
an unwound frame; and it is the record that the read is running, which is what
`sealForCommit` refuses to commit over. That second job is a **tripwire, not a
guard**: a transaction ctx belongs to one goroutine (`internal/storeapi`,
`Within`), so an outer transaction committing under a sibling goroutine's grouped
read is a contract violation rather than a case this design owes an answer to.
`ErrConcurrentNestedTx` already says the same of itself.

`s.read(ctx)` on a dead frame still answers from the ambient transaction, as it
always has ([ADR](2026-08-20-reads-get-their-own-connections.md)). That asymmetry
between grouped and ungrouped reads predates this change and is not settled here.

**A read transaction still runs its hooks.** It opens a frame, so `AfterCommit`
queues onto it, and it commits, so the queue is owed. No read path queues one
today; the prototype that skipped the flush is why this is written down.

**Unexported.** A prepared statement is one compiled handle per connection, and
two callers sharing one corrupts both silently. Inside a read transaction the
connection is already held, so sharing is possible unless the body is
single-threaded. What makes it so is the one-goroutine contract, not this; but
unexported is what keeps the contract cheap to hold here, since only this package
can add a caller. Export it later and each new one has to be checked.

## Consequences

Measured on disk, alternating runs a side. The reads no floor covers — a watch's
opening snapshot — stop scaling with write pressure:

| | 0 writers | 1 | 4 |
|---|---|---|---|
| `Events().Snapshot` before | 34.9 µs | 112.6 µs | 303.6 µs |
| after | 34.1 µs | 47.3 µs | **43.1 µs** |
| `ObjectWrites().Snapshot` before | 755.5 µs | 835.8 µs | 1094.1 µs |
| after | 760.2 µs | 860.6 µs | **852.9 µs** |

At one writer the object listing is 3% worse: a read transaction gives up the
writer's warm page cache. That cost is fixed and the saving grows.

**It buys nothing on the write path or on delivery latency**, and two drafts of
the spec claimed otherwise. `BenchmarkWritesUnderWatch` moves 0 to −3% at the
shipping throttle, and watch delivery is set by the tailer's scan floor. The
tailer's own drain read is 68% faster under four writers and that is absorbed by
the same floor. A change measured on a floored path buys nothing a user sees.

**`snapshot` is the one long hold.** Its listing has no bound, so it pins one of
`WithReadConnections` (4 by default) and holds the WAL against a checkpoint for a
whole kind, where the paged reads hold theirs for one page. Those reads used to
take the writer, so they could not queue behind an ordinary reconcile read; now
they can. `WithReadConnections(1)` is legal, and at N=1 a large snapshot blocks
every other read for its duration. What that changes is who waits, not how much.
