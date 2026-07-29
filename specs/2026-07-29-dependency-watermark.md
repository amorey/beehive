# `dependency_watermarks`: re-derive dependency staleness instead of recording each wake

- **Status:** Proposed — not yet implemented.
- **Date:** 2026-07-29
- **Closes:** the three waker items in [`TODO.md`](../TODO.md) — the startup seed
  race, the crash-during-a-waker-outage strand, and the mid-reconcile target change.
- **Supersedes:** the durable *scan*-watermark design, retained below as the
  rejected alternative. Both are watermarks; that one is the waker's single
  store-wide scan position, this one is per dependent.
- **Implements, with changes:** the `observed_cursor` recommendation in `TODO.md`.
- **Breaks `Store`.** See "This is the `Store` break" — two parked `TODO.md` items
  are waiting on exactly this moment.
- **Edits `0001_init.sql` rather than adding a migration.** See "Schema" for the
  precondition that makes that safe.

## Context

Three `TODO.md` items describe the same failure: a dependent D that has *settled* —
its own generation never moved, nothing stamped `reconcile_owed` — is stranded
because the only record that it owed a reconcile was in memory and did not survive a
restart. No owed-work listing can name it afterwards. `WithStartupFullPass(true)`
masks all three and may not be relied on, since a full pass scales with the object
count.

The durable scan-watermark design closes these by making the waker's own record durable:
stamp `reconcile_owed` on every dependent, then advance a persisted cursor in the
same transaction. It works, and its ordering argument is sound.

It has one structural limit. Its guarantee is *"the waker recorded it."*
`reconcile_owed` is a promise the code makes to itself, and a promise can only be
checked against the code that makes it. If the waker had a bug, or was not running —
a process with no registered controllers, a kind between builds — the promise was
never made, and the absence of a stamp is indistinguishable from "nothing was owed."
Draining the count also erases the evidence: once at zero, the store cannot say
whether D converged against T's version 40 or version 12.

This spec records a *measurement* instead. Each dependent remembers the store cursor
it last reconciled against, and staleness becomes a query over current state rather
than a bookkeeping invariant maintained across a crash.

## Decision

**One narrow side table. `dependency_watermarks.reconciled_against` holds the store-wide
write cursor as of the moment that object's last successful reconcile loaded its
state.** A dependent is stale exactly when some target it depends on has moved past
that watermark:

```sql
t.resource_version > c.reconciled_against
```

Three consequences follow, and they are the whole design:

**1. The in-memory waker becomes an optimization, not a guarantee.** It keeps its 1s
latency and its in-memory scan watermark, unchanged. Losing a wake now costs latency until
the next staleness pass, not permanent divergence. Nothing about `waker.go` changes
except its doc comments.

**2. Correctness comes from re-derivation, on a slow cadence.** A new driver — the
stale-dependents pass — runs the query above, paged, and enqueues what it finds. It
recovers a wake lost by *any* means, including means that do not exist yet.

**3. Nothing durable has to be kept in sync.** There is no at-least-once bookkeeping,
no stamp/advance transaction, no ordering constraint between an enqueue and a commit,
no double-stamping to reason about, no count to drain. The watermark is monotonic
and idempotent; writing it twice is writing it once.

### Why the scan cannot lose a dependent

The staleness query joins through `edges.to_id` to read the target's
`resource_version`. That join is total, and by construction rather than by luck:
`edges.to_id` is `ON DELETE RESTRICT`, so a target row always outlives every edge
pointing at it. There is no window in which a dependent's edge survives a collected
target and the join silently drops the row. Nothing in the driver has to handle a
missing target, and no `LEFT JOIN` is needed on that side.

### Why this closes more than the scan watermark does

| Gap | Scan-watermark design | This design |
| --- | --- | --- |
| Seed race (scheduling + failed seed) | ✅ migration-time seed | ✅ nothing to seed |
| Crash during a waker outage | ✅ cursor held and durable | ✅ re-derived |
| Ordinary / mid-reconcile target change | ✅ stamped before the cursor moves | ✅ re-derived |
| Waker not running (client-only process, kind between builds) | ❌ permanent hole | ✅ re-derived |
| A bug in the wake path | ❌ permanent hole | ✅ re-derived |
| Unregistered dependent kinds | ⚠️ stamps leak, never drained | ✅ found on registration, at no standing cost |
| `RequeueAfter` chains across restart | ❌ | ❌ unchanged — different mechanism |
| Dependency cycles ≥ 2 | ❌ | ❌ unchanged, and gains a second driver — see Consequences |

### What `reconcile_owed` is still for

It stays, unchanged, carrying `EdgesAdd`'s caller-versioned stamp. That path closes
the read-then-declare window at owed-pass latency (30s) by riding a write `EdgesAdd`
was making anyway, which is far better than waiting for the stale pass. The two are
complementary: `reconcile_owed` is the cheap fast path for the one case that can be
recorded for free; `dependency_watermarks` is the backstop for every case that cannot.

This spec adds **no** new producer of `reconcile_owed`, so the deliberate "there is
deliberately no standalone increment here" block in `storeapi.go` stays true. The
scan-watermark design would have invalidated it.

## Schema

The new table goes in `sqlite/migrations/0001_init.sql`, after `edges`. There is no
`0002`.

```sql
-- ============================================================
-- dependency_watermarks
-- Per-dependent staleness watermark: the store-wide write cursor
-- (resource_version_seq) as of the moment this object's last
-- successful reconcile loaded its state. A dependent is stale
-- when a target it depends_on has a resource_version above it.
-- ============================================================

-- A side table rather than a column on objects, for one reason: objects rows
-- carry the spec and status JSON inline, and SQLite rewrites the whole record
-- on UPDATE — including overflow pages. Storing eight bytes against a
-- multi-kilobyte row, on every successful reconcile of every dependent, is the
-- most expensive available way to keep a small integer. Here the write touches
-- a three-column row.
--
-- Sparse by construction: a row exists only once a dependent has reconciled,
-- so the table is sized by the dependency graph, not by the object count. An
-- absent row means the same thing a zero cursor would — never reconciled
-- against a known point, therefore stale — which is why the scan LEFT JOINs
-- and needs no backfill.
--
-- A rowid table, unlike edges. object_id is an INTEGER PRIMARY KEY, so it
-- *aliases the rowid*: the table is already one b-tree keyed by object_id with
-- no separate index, and the scan's per-edge probe is a direct rowid seek.
-- WITHOUT ROWID would demote object_id to an ordinary column stored in the
-- record payload, paying a header entry and serial type for a key the rowid
-- form stores as a bare varint. Measured at 200k rows, 4KB pages: 635 pages
-- rowid vs 718 WITHOUT ROWID, +13% for no gain.
--
-- edges is WITHOUT ROWID for the opposite reason, spelled out above: every one
-- of its columns is in the key, so a rowid table would store each edge twice.
-- Here there are non-key columns and an integer key, which is exactly the case
-- the rowid form is best at.
--
-- ON DELETE CASCADE because this is derived state with no claim on the
-- object's lifetime — it disappears with the row, alongside the outgoing edges
-- that cascade for the same reason.
--
-- reconciled_at is observability only; nothing reads it to make a decision. It
-- moves only when reconciled_against does, which the upsert enforces with a
-- single WHERE over both columns rather than by discipline. Two consequences,
-- and both are the same caution 0001's observed_at comment already gives:
--
--   * It is NOT a reconcile heartbeat and cannot be read as "last ran". It
--     stops moving whenever a pass reconciles against a store nobody has
--     written to since the last one — rare when the store is busy, routine
--     when it is quiet. Controller liveness belongs in the events log.
--   * Its coverage is asymmetric: only dependents get a row at all, and only
--     successful passes write one. Anything built on it silently omits every
--     object without dependencies and every failing reconcile.
CREATE TABLE dependency_watermarks (
    object_id          INTEGER PRIMARY KEY REFERENCES objects(id) ON DELETE CASCADE,
    reconciled_against INTEGER NOT NULL, -- resource_version_seq value observed at load
    reconciled_at      INTEGER NOT NULL  -- millis; moves only with reconciled_against
) STRICT;
```

No index beyond the primary key: every access is by `object_id`.

No change to `objects`, to `edges`, or to `reconcile_owed` — its type, its floor, its
partial index or its documented provenance. The `0001_init.sql` claim that "the
in-memory dependency waker is a separate mechanism and leaves nothing here" **remains
true** under this design.

### Why editing `0001` is safe here, and its one hazard

`sqlitemigrate` records `(version, name, applied_at)` and applies everything above
`MAX(version)`. It does **not** checksum applied migrations. So a database that has
already run `0001` will never receive this table, and will fail at runtime with `no
such table: dependency_watermarks` rather than loudly at migrate time.

That is acceptable only because no store predates this change — the schema is
pre-release. The practical cost is that a stale local `.db` file from before this
lands produces a confusing runtime error whose fix is to delete the file. Worth a
line in the commit message.

If any deployed store existed, this would have to be `0002` with a `CREATE TABLE`
instead, and nothing else about the spec would change — the table is additive and
needs no backfill, precisely because an absent row already means "stale".

### No new index on `edges`, and why

An earlier draft added `idx_edges_depends_on ON edges(from_id, to_id) WHERE
relation = 'depends_on'`, justified by "neither existing index leads on `relation`."
That rationale was a non-sequitur: the scan pages by `from_id`, and the primary key
*does* lead on `from_id`. Measured against the 0001 schema on modernc:

```
without the index:  SEARCH e USING PRIMARY KEY (from_id>?)
with the index:     SEARCH e USING COVERING INDEX idx_edges_depends_on (from_id>?)
```

It is already a covering range scan with no row fetch. The index would buy only
skipping `owned_by` entries *during* a scan that runs every few minutes — and it
would cost a second full copy of every `depends_on` edge, because a secondary index
on a `WITHOUT ROWID` table carries the whole primary key in its payload. `(from_id,
to_id)` plus the implicit `(from_id, to_id, relation)` is the table row, byte for
byte. `0001_init.sql`'s own `idx_edges_to` comment spells this out three lines above
where the index would have gone.

Add it only if a measured `owned_by`-dominant graph shows the filtering matters.

## Store surface

Three methods and one small type. Two methods are the new mechanism; the third
replaces the reconcile loop's opening read. No change to `RawObject`.

```go
// DependencyWatermarkSet records cursor as the store-wide write cursor id's reconcile
// observed, for the staleness scan to compare targets against. Upserts, since the
// row appears on a dependent's first successful pass and is raised on later ones.
// Bumps no resource_version — it writes no objects row at all, so a recorded
// reconcile cannot put the object back into the waker's own scan and wake every
// dependent of an object whose only change was that record.
//
// The stored cursor never decreases, and a pass that advances nothing writes
// nothing: the upsert's DO UPDATE carries a WHERE that rejects a non-advancing
// value outright. That makes the write both order-independent — free insurance
// today, since the work queue serialises passes per id — and free of charge on a
// pass that observed no new store state, which is the common case for a polling
// controller in a quiet store.
//
// It also sets reconciled_at, under the same predicate, so the timestamp cannot
// move without the cursor. reconciled_at is observability only; nothing reads it
// to decide anything, and it is not a reconcile heartbeat (see the table comment).
//
// The write gates in SQL on id having at least one outgoing depends_on edge. An
// object with no dependencies can never be found stale, so a row for it is dead
// weight the scan would probe forever. Gating in the statement rather than in the
// caller keeps it to one round trip and no pre-read — and, less obviously, it is
// what keeps the foreign key satisfied when the object is collected mid-pass (see
// the statement below).
//
// Advancing the cursor asserts that this pass observed its dependencies' state as
// of cursor. The store cannot check that, and one caller shape breaks it: a
// controller that swallows a failed read of a dependency and returns nil advances
// past a change it never saw. That is the same unsafe shape as sampling the cursor
// after the reconcile rather than before, and it is the one way this mechanism can
// strand a dependent.
DependencyWatermarkSet(ctx context.Context, id ObjectID, cursor int64) error

// DependentsListStale returns objects of the given kinds that have a depends_on
// edge to a target whose resource_version is above their dependency watermark —
// everything owed a dependency reconcile as of now, re-derived rather than
// recorded. Ordered by id and paged from afterID (pass 0 to start), at most limit
// rows. An empty kinds slice returns nothing.
//
// Filtered by kind for the same reason ReconcileOwedListIDs is: only a registered
// kind has a reconcile loop to enqueue into. Filtering here rather than in the
// driver is what keeps a client-only dependent — which never gets a watermark written
// and is therefore stale forever — from being re-scanned, re-joined and re-paged on
// every pass for the life of the row. A kind that gains a controller in a later
// build appears in the list and is found on the next pass, so nothing is lost.
//
// The kind filter applies to the DEPENDENT and must never be extended to the
// TARGET. A registered object may depend on a client-only one — that is the whole
// reason the waker's scan is store-wide rather than per-kind — so a target's kind
// is irrelevant to whether its dependents are owed a pass. Narrowing the scan to
// edges whose *both* endpoints are registered looks like a free optimisation and
// would silently strand every registered dependent of a client-only target, which
// is the exact failure class this mechanism exists to remove.
//
// A missing dependency_watermarks row counts as stale: an object that has never
// reconciled against a known point cannot have converged, mirroring how
// idx_objects_unsettled treats a NULL observed_generation.
//
// Self-edges are excluded. An object that depends on itself bumps its own
// resource_version whenever its reconcile writes, which would leave it stale against
// itself for an extra pass every time it does any work — the same reason
// waker.dependentsWake skips them.
DependentsListStale(ctx context.Context, kinds []GroupKind, afterID ObjectID, limit int) ([]ObjectRef, error)

// ReconcileLoad is everything one reconcile pass needs from its opening read.
// A struct rather than extra return values: this is the load path most likely to
// grow, and each addition would otherwise be a Store break of its own.
type ReconcileLoad struct {
    Object RawObject
    // Cursor is the store-wide write cursor as of the same statement that read
    // Object — the value to record on success. Reading it here rather than
    // separately saves a round trip on a pool of one connection, and is
    // marginally safer than two statements: it is at or below the true cursor
    // when the controller reads its dependencies, because those reads all happen
    // after this returns.
    Cursor int64
    // HasDependencies reports whether Object had an outgoing depends_on edge at
    // load. It exists only so a reconcile of an object with no dependencies can
    // skip DependencyWatermarkSet entirely and never take the write lock — see that
    // method, and the reconciler's skip rule.
    HasDependencies bool
}

// ObjectsGetForReconcile is the reconcile loop's opening read: the object, the
// write cursor as of the same statement, and whether the object has dependencies.
// Both extra values are correlated subqueries — one over the single-row
// resource_version_seq, one an EXISTS on the edges primary-key prefix — so they
// ride the existing statement rather than adding round trips.
ObjectsGetForReconcile(ctx context.Context, id ObjectID) (ReconcileLoad, error)
```

The staleness query:

```sql
SELECT e.from_id, d."group", d.kind
  FROM edges e
  JOIN objects t ON t.id = e.to_id
  JOIN objects d ON d.id = e.from_id
  LEFT JOIN dependency_watermarks c ON c.object_id = e.from_id
 WHERE e.relation = 'depends_on'
   AND e.from_id != e.to_id
   AND e.from_id > ?
   AND (d."group", d.kind) IN (…)
   AND (c.reconciled_against IS NULL OR t.resource_version > c.reconciled_against)
 GROUP BY e.from_id
 ORDER BY e.from_id
 LIMIT ?
```

`GROUP BY e.from_id`, not `SELECT DISTINCT`. A dependent with several stale targets
must appear once, but `DISTINCT` plans a temp B-tree it does not need — the scan
already arrives in `from_id` order. Measured on the 0001 schema:

```
SELECT DISTINCT … ORDER BY e.from_id   →  3 × SEARCH + USE TEMP B-TREE FOR DISTINCT
SELECT … GROUP BY e.from_id ORDER BY … →  3 × SEARCH
```

Same rows, same order, no temp B-tree.

The write:

```sql
INSERT INTO dependency_watermarks (object_id, reconciled_against, reconciled_at)
SELECT ?, ?, ?
 WHERE EXISTS (SELECT 1 FROM edges WHERE from_id = ? AND relation = 'depends_on')
    ON CONFLICT(object_id) DO UPDATE
   SET reconciled_against = excluded.reconciled_against,
       reconciled_at      = excluded.reconciled_at
 WHERE excluded.reconciled_against > dependency_watermarks.reconciled_against
```

**The `WHERE` on `DO UPDATE` does two jobs, and neither is optional.**

*Monotonicity.* A write arriving out of order cannot regress the cursor and
un-converge a dependent. Nothing produces out-of-order writes today — the work queue
serialises passes per id — and a regression would only over-reconcile, so this buys no
fix. It buys order-independence for one clause, and removes a landmine for the
multi-process case listed under Non-goals.

*No-op suppression.* When the cursor has not advanced, the row is not written at all —
no page dirtied, no WAL frame. Measured on modernc:

```
insert 100                   value=100    changes()=1
advance to 200               value=200    changes()=1
re-apply 200 (no advance)    value=200    changes()=0
regress to 50 (rejected)     value=200    changes()=0
```

This matters most in the case flagged under Consequences as this design's new cost: a
`RequeueAfter` polling controller that declares dependencies. If nothing anywhere in
the store moved between two of its passes, the cursor does not advance and the write
disappears entirely. `MAX(…)` in the `SET` would give monotonicity alone and still
write the row every pass, so the `WHERE` form is strictly better.

It is also what couples `reconciled_at` to `reconciled_against` structurally: one
predicate guards both columns, so the timestamp cannot move on a pass that observed
nothing new. That is deliberate — see the table comment on why it is not a heartbeat.

The `EXISTS` rides the `edges` primary-key prefix, so the gate costs a b-tree probe
and no separate read.

**The gate is also the foreign-key guard, and that is not incidental.** `object_id`
references `objects(id)`, and the reconcile path collects rows mid-pass: `gcCollect`
runs after the controller returns, on the same pass, and can physically remove the
object. If that happens between the load and this write, the object's outgoing edges
have cascaded away with it, so `EXISTS` is false and nothing is inserted. Replacing
the gate with a has-dependencies flag captured at load (the remedy open question 3
floats) removes that protection and turns a racing delete into `FOREIGN KEY
constraint failed`. Any such change has to re-establish the guard some other way.

Cost of the scan is bounded by the `depends_on` edges of registered kinds, with three
primary-key lookups each — not by the object count, which is what separates this from
a full pass. The two-column comparison in the last predicate cannot be indexed by any
means; paging by `from_id` on the primary key is what keeps each page bounded.

The kind list is chunked under `idChunkSize` (`sqlite/store.go:1532`) like every
other id-list query, though a store with enough registered kinds to need it does not
exist.

**Naming.** `DependencyWatermarkSet` stays singular — it sets one object's watermark
— following `ReconcileOwedDecrement`'s cardinality-in-the-verb reading rather than
pluralizing for the table. `DependentsListStale` sits in the dependents family
because it answers a question about objects, not about the table.

The table was `dependency_cursors` through most of this spec's drafting.
**"Watermark" is the better head noun, and the write's own shape is what settles
it.** A cursor is a position, and in most database vocabulary a *live* one — a
handle something is currently advancing. This value is neither: it is a durable mark
left behind by a finished pass, and it is monotonic by enforcement, since the
upsert's `WHERE` rejects any value that does not advance it. "Everything at or below
this is accounted for" is the definition of a high-water mark, so the name now states
a property the code guarantees rather than merely gesturing at a position.

The objection I had held against it — that the waker already owns a "watermark" —
turns out to argue the other way. The two are the *same concept* at different grain
and lifetime: the waker's is one in-memory store-wide scan position, this is one
durable position per dependent. Consistent vocabulary for one concept beats inventing
a second word for it. Where both appear together, qualify: the waker has a **scan**
watermark, a dependent has a **dependency** watermark.

It also removes the last thing "cursor" was still colliding with. `resource_version`
is documented as the store's "write cursor" (`0001_init.sql:48`), so a table of
`cursors` holding `resource_version` values had two unrelated senses of the word one
line apart.

## This is the `Store` break

`type Store = storeapi.Store` (`store.go:25`) is an exported alias, so adding a
method breaks every external implementation. This spec adds three. That is not a
reason to add fewer — it is a reason to check what else is waiting.

Two `TODO.md` items are parked on precisely this event:

- **`ObjectsCreate` takes a `RawObject` and silently drops most of it** (`:405`) —
  "Deferred because `RawObject` is an exported alias, so narrowing the parameter
  breaks an externally implementable `Store`, and it should be done together with the
  return-shape item below rather than as a second separate break."
- **Mutators build a `RawObject` no caller reads** (`:425`) — "Revisit when the next
  `Store` break is on the table anyway."

`ObjectsGetForReconcile` is the smaller version of the same argument, and this spec
takes it: folding the cursor and the dependency flag into the object read is listed
above rather than deferred to a later "if profiling shows it matters", because
deferring it guarantees a second break for a change we already know we want. Its
struct return exists for the same reason — the reconcile load path is the one most
likely to grow again.

**Decided: do not bundle the two `RawObject` items.** Two reasons, and neither is
about their merit:

- The module is at **v0.9.0** with no `/v2`, so under Go's semver rules a v0.x break
  costs external implementers nothing. Amortising breaks is worth very little here,
  which was the whole case for bundling.
- The side table means this spec touches `RawObject` not at all, so there is no
  coupling to exploit. A narrow read method cannot help either item regardless: both
  are write-side — `ObjectsCreate`'s parameter shape and the mutators' return shape.

**But their clock is nearly out.** `TODO.md` parks both on "the next `Store` break" as
a proxy for *before we are locked in*, and the real deadline is v1.0, not the next
break. After v1.0, narrowing `ObjectsCreate`'s parameter stops being a minor-version
inconvenience and becomes a major-version event that realistically never happens.
Sequence the reshape as its own change, on its own merits, before v1.0 — and note
that `TODO.md:418` has not yet chosen between a `CreateObjectInput` and functional
options, which is a design decision that should not be made under time pressure
inside a correctness change.

## Reconciler changes

`typedController.reconcile` loads through the new method and records the cursor.

1. Replace `ObjectsGet` with `ObjectsGetForReconcile`, keeping the `ReconcileLoad`.
2. Reconcile as today.
3. On success, and only if `load.HasDependencies`, record the cursor.

**The skip is an optimisation layered on the SQL gate, never a replacement for it.**
`HasDependencies == false` means there is nothing to write, so the call is skipped and
no write lock is taken — that is the whole point, since most objects in a store
declare no dependencies. When it is true, the gated statement runs unchanged, so the
`EXISTS` still guards the foreign key against a mid-pass `gcCollect`. Removing the
gate in favour of the flag would be unsafe; using the flag to avoid calling into the
gate is not.

Skipping is safe when the flag is stale in the racing direction. If the object gains
a dependency during the pass, it has no watermark row, and an absent row already means
stale — the next stale-dependents pass finds it, and `EdgesAdd`'s caller-versioned
stamp usually finds it sooner. The error is in the over-reconcile direction, which is
the direction this whole design errs in by construction.

**Placement.** The cursor write sits in the `ReconcileOwedDecrement` block
(`reconciler.go:139-152`) — after `Reconcile` returns, **before** `gcCollect`. It goes
under `reconcileErr == nil` **only**, not under the existing `reconcileErr == nil &&
raw.ReconcileOwed != 0`, which is the decrement's condition and unrelated. The two are
independent writes with independent gates: the decrement fires when something was
owed, the cursor write fires when the object has dependencies (gated in SQL).

Ordering it before `gcCollect` is deliberate. `gcCollect` may physically remove the
row on this pass, and while the SQL gate makes the write safe either way (the edges
cascade, `EXISTS` goes false, nothing is inserted), running before the collect keeps
the write meaningful for the ordinary case and keeps the two blocks in the order the
file already reads: bookkeeping for the pass that just ran, then GC follow-up.

**Failure handling.** Mirror `ReconcileOwedDecrement` exactly: log a warning and
continue. A failed cursor write leaves the object stale, so the next pass re-derives
it — returning the error instead would push a self-healing bookkeeping write into the
backoff ladder and retry a whole reconcile over it.

**Both early returns must skip the write**, for the reason each already documents
about `reconcile_owed`:

- **Object gone before reconcile** (`ErrNotFound`): nothing to record.
- **Undecodable row** (the quarantine path): the controller never saw the object, so
  no reconcile happened. Writing the cursor here would silently mark a poison row as
  converged against its dependencies — exactly the discard the quarantine exists to
  avoid.

A failed reconcile leaves the cursor where it was, so the object stays stale and is
found again. That is the self-healing property the whole design rests on, and it
needs no retry bookkeeping.

**Why the cursor must come from the load, not from the end of the pass.** The
recorded cursor must be at or below the true cursor when the controller read its
dependencies. Reading it in the same statement as the object guarantees that, since
every controller read happens after `ObjectsGetForReconcile` returns. A target that moves
during the pass is counted as still-owed and reconciled once more — wasted work,
never lost work. A cursor sampled *after* the controller's reads inverts that: it
lands above a change the pass did not observe, and D is stale with nothing left to
find it. This is the same shape as `ReconcileOwedDecrement`'s
subtract-the-*observed*-count rule.

## The stale-dependents pass

A sixth driver in `drivers.go`'s existing `runDriver` shape: page
`DependentsListStale` over the registered kinds and enqueue each result under its own
`GroupKind`.

It mirrors `waker.run` and `waker.dependentsWake` in two respects that are not
incidental:

- **Early return when `len(bh.order) == 0`.** With no registered controllers there is
  nothing to enqueue, and the kind list would be empty anyway.
- **Resolve reconcilers via `bh.enqueuerForPage()`**, once per page rather than per
  row. Resolving takes the control-plane mutex that `Register`, `migratorFor` and
  `stop` also want, and one page can reach many dependents across a handful of kinds.
  This is the same reason `dependentsWake` is shaped this way (`waker.go:181-185`).

Other properties:

- **Cadence:** `staleDependentsInterval`, unexported option
  `withStaleDependentsInterval` for tests only. It is a correctness backstop and
  **cannot be disabled**, like the GC sweeper.
- **Startup:** covered by `runDriver`'s eager first step, on the driver's own
  goroutine, so it does not block `Start`. It pages, but like `waker.scan` a single
  step pages **to exhaustion** rather than one page per tick — so the first step
  enqueues everything stale, not a slice of it. See "First deploy" below.
- **Default:** `60 * time.Second`. The scan is reads-only, bounded by the
  `depends_on` edges of registered kinds, and pages — so it interleaves with other
  statements rather than holding the connection, and in steady state it finds nothing
  and enqueues nothing. A shorter interval therefore costs scan time and no
  reconciles, which means the value should be set by *acceptable staleness after a
  crash*, not by cost. Five minutes of silent divergence is a long time for a control
  plane whose ordinary wake latency is one second; 60s keeps the backstop in the same
  order as the owed pass and GC sweeper (both 30s) while staying above them. The
  cycle concern does not argue for longer — a cycle is already sustained by the 1s
  waker, against which 60s is noise. Revisit if a realistic `depends_on` edge count
  makes one full scan consume more than roughly 1% of connection time at this
  cadence; that measurement is cheap once `DependentsListStale` exists.

Deliberately unfiltered on `deletion_requested_at`: a finalizing dependent may still
need a pass, and the waker does not filter either.

## Consequences

### The cost, stated plainly

**One narrow-row write per successful reconcile of an object that has dependencies,
and nothing at all for any other reconcile.** The cursor and the dependency flag both
ride `ObjectsGetForReconcile`'s existing statement as correlated subqueries, so the
load costs no extra round trip.

The `HasDependencies` skip is what makes the second half of that claim true, and it
matters more than it looks. The write is an upsert into a three-column table, gated in
SQL, so for an object with no dependencies it would insert nothing — but **"inserts
nothing" is not "costs nothing."** The pool is `MaxOpenConns(1)` with
`_txlock=immediate` in the DSN (`sqlitemigrate.go:39-57`), so an `INSERT` that writes
zero rows still opens a write transaction, takes the write lock, and serialises
against every other statement in the process: client writes, other kinds' reconciles,
the GC sweeper, the waker's scan. Issuing it unconditionally would put that
serialisation on every successful reconcile of every kind, including kinds that
declare no dependencies at all. The skip confines it to dependents.

For a dependent, the remaining cost is one write-lock acquisition per successful
pass, and one narrow row write on the passes that actually advance the cursor —
the upsert's WHERE suppresses the rest. It cannot be merged into `ReconcileOwedDecrement` to
share a transaction, since the two touch different tables.

The side table is what keeps that write narrow. On `objects` the same write would
rewrite a record carrying the `spec` and `status` JSON inline (`0001_init.sql:25-26`),
because SQLite rewrites the whole record on `UPDATE` and the in-place optimization
does not apply when a NULL becomes an integer. Eight bytes would cost a multi-kilobyte
row rewrite plus its overflow pages, on every pass of every dependent.

What the side table costs instead: one extra primary-key probe per edge on the scan,
since the cursor no longer arrives free inside the `d` row lookup. That is a cached
rowid seek on a small table, paid per pass rather than per reconcile.

The case where this is genuinely new cost is **`RequeueAfter` polling loops**. A
controller that returns `RequeueAfter(10s)` reconciles forever, and today a converged
pass writes nothing. Such a controller that *also* declares dependencies now writes
its cursor on every pass. The `HasDependencies` skip is what keeps this from applying
to polling controllers generally — one that declares no dependencies pays nothing.

### The scan cost

O(`depends_on` edges of registered kinds) per pass, at the 60-second cadence.
This is a scan over what *exists* rather than what *changed*, which the
[drivers ADR](../docs/adr/2026-07-28-periodic-scan-drivers.md) warns about — but the
population is the dependency graph, which is exactly the set that can possibly be
stale. That is the tightest bound any re-deriving check can have, and it is
categorically smaller than the full pass's object count.

### First deploy enqueues the whole dependency graph

No object has a watermark row until it reconciles, and an absent row means stale — so on
the first start after this lands, every dependent of a registered kind is stale at
once. Because a driver step pages to exhaustion, the first step enqueues all of them
before it returns; there is no gradual drain.

This is not harmful. The work queue coalesces by id, the per-kind reconcile loops
rate themselves, and the passes are the ones the design wants anyway — each one
records a cursor, so the herd is strictly one-time and self-extinguishing. It is
worth stating because it is the largest single burst of reconcile work this change
ever produces, and it lands on the deploy where it will be least expected.

The same burst occurs for any kind that is registered for the first time in a later
build, bounded by that kind's dependents.

### Client-only dependents

An object whose kind is never registered never gets a watermark row —
`DependencyWatermarkSet` is called only from the reconcile path — so it is stale
*forever*.

Unfiltered, that would be a permanent addition to every pass's working set: rows
re-scanned, re-joined and re-paged on a cadence, for objects nothing can ever
reconcile. It would be strictly worse than `TODO.md`'s already-deferred "a
client-only dependent's `reconcile_owed` is never reclaimed", which is idle bytes.

The `kinds` filter on `DependentsListStale` is what makes the cost zero rather than
permanent, and it keeps the late-registration property: the row is excluded while no
controller exists for it, and included the moment one does. Nothing is stranded by
the exclusion, because there is no reconcile loop to be owed a pass in the first
place — and on registration the object is found twice over, by an absent watermark row
and by any `reconcile_owed` stamp `EdgesAdd` left behind while it was unregistered.

### Client-only *targets* are a different case, and are covered

A registered dependent may point at a client-only target, and that dependent must
still be woken when the target moves. Both mechanisms handle it, and neither by
accident:

- The waker scans the write log store-wide precisely so a `depends_on` edge into an
  unregistered kind is not invisible (`waker.go:24-28`).
- The stale pass joins `objects t` with no predicate on the target's kind — the
  `kinds` filter binds `d` alone.

A client-only target's writes are ordinary object writes and bump `resource_version`
like any other, including `DeletionRequestsCreate` (`sqlite/store.go:1249-1253`), so
a dependent also learns that its client-only dependency has begun finalizing.

The invariant to protect is stated on `DependentsListStale` and pinned by
`TestDependentsListStaleFindsDependentsOfUnregisteredTargets`: the kind filter is on
the dependent, never the target.

### Cycles are unchanged, and this adds a second driver to them

A dependency cycle of length ≥ 2 already reconciles forever (`TODO.md`, first item).
An earlier draft of this spec claimed the loop damps once statuses go byte-stable.
**That is wrong, and `TODO.md` says so in as many words:** a byte-identical
`UpdateStatus` at a generation the object has not settled at, any real condition
write, or `FinalizersDelete` all keep bumping `resource_version`, and "keeping status
byte-stable is no defence."

So two mutually dependent controllers that write on every pass keep re-staling each
other under this design exactly as they do under the waker. This is not a new bug
class, but it is not neutral either: the stale pass **cannot be disabled**, so a
cycle now has a second, unkillable driver sustaining it, where previously turning the
waker off (a test-only configuration) would quiet it.

The fix remains `TODO.md`'s option 2 — a minimum re-enqueue interval per work-queue
item, which bounds cycles of any length without a graph query. This spec does not
depend on it and does not implement it, but it raises the value of doing it.

### Non-goals

- **Pushing latency below the 1s waker scan.** Unchanged; see the drivers ADR.
- **`RequeueAfter` durability.** Untouched, still open in `TODO.md`.
- **Bounding dependency cycles.** See above — raised in value, not addressed.
- **Multiple processes sharing one store.** Not a concern here, unlike the
  scan-watermark design: the cursor is per-object and derived, so concurrent owners would
  over-reconcile rather than lose work. Still out of scope.

## Rejected and deferred

### Rejected: a column on `objects`

The obvious shape, and wrong on write cost. `objects` rows carry `spec` and `status`
inline, SQLite rewrites the whole record on `UPDATE`, and the write happens on every
successful reconcile of every dependent — the highest-frequency writer of the
smallest value in the schema. The side table also keeps `RawObject` untouched, which
matters because `TODO.md:405` is already trying to shrink it, and it removes the
`dependency_watermark` / `resource_version` adjacency, which is a genuine misreading
hazard: they are named as a pair, sit beside a documented pair
(`observed_generation == generation` means applied), and are not comparable.

Its one advantage — the cursor arrives free inside the `d` row lookup the scan
already does — is paid back once per pass, against a write cost paid per reconcile.

### Rejected: the durable scan watermark

Stamp `reconcile_owed` on every dependent the waker wakes, then advance a persisted
watermark in the same transaction, so the cursor is never past a wake that was not
recorded. **Its ordering argument is correct and worth preserving:** at-least-once
falls out of committing the stamps before the cursor moves, and the count-not-flag
property already handles a wake landing mid-reconcile.

Rejected because its guarantee is "the waker recorded it," which leaves a permanent
hole wherever the waker was not running or was wrong, and because making that promise
reliable requires machinery this design does not need at all: a stamp/advance
transaction, id-list chunking under `SQLITE_MAX_VARIABLE_NUMBER` inside that
transaction, deliberate double-stamping on replay, a stamping asymmetry for
unregistered kinds, and a write transaction held on the single connection once a
second.

It is also strictly more expensive in the steady state: it pays writes proportional
to change *events* × dependents, where this design pays one narrow write per
reconcile — proportional to *coalesced* work.

### Deferred: the scan watermark as a startup checkpoint

The scan watermark can come back later as a pure optimization, and the composition is
sound *because* the cursor is the ground truth: a checkpoint that is stale, wrong, or
written by a buggy build costs startup latency and nothing else. It would need no
transaction, no coupling to the stamps, and no atomicity — one small row, written
lazily.

Startup would then have two interchangeable strategies for the same question — replay
the write log from the checkpoint, or re-derive with the scan — and could take the
cheaper one, degrading to the scan whenever the replay exceeds a page budget.

Not in this spec because the cheaper thing should be tried first: the startup scan
already pages and runs in the background, so it does not gate readiness.

### Rejected: `idx_edges_depends_on`

See Schema. The plan is a covering range scan on the primary key without it, and the
index would be a byte-for-byte duplicate of the rows it indexes.

### Rejected: writing the cursor unconditionally

Dropping the edge gate removes one `EXISTS` from the write. Rejected: it would put a
row in the table for every object in the store, including the large majority that
have no dependencies and can never be found stale — turning a table sized by the
dependency graph into one sized by the object count, and adding a probe per row to
every scan.

## Testing

New, beehive-level:

- `TestReconcileRecordsDependencyWatermark` — a successful pass leaves a watermark row for
  an object with dependencies.
- `TestReconcileSkipsDependencyWatermarkWithoutDependencies` — an object with no
  `depends_on` edge never gets a row, and the store never sees the call at all (the
  `HasDependencies` skip, asserted against a fake that records invocations — the point
  is the write lock not taken, which a row-count assertion would not catch).
- `TestReconcileRecordsCursorFromTheLoad` — a target that changes after the load but
  before the controller reads it must leave the dependent stale.
- `TestReconcileHoldsDependencyWatermarkOnFailure` — a failed reconcile leaves the cursor
  unchanged, so the object is found again.
- `TestReconcileHoldsDependencyWatermarkOnUndecodableRow` — the quarantine path records
  nothing, mirroring the `reconcile_owed` assertion already there.
- `TestReconcileWarnsAndContinuesOnCursorWriteFailure` — the failure contract: no
  error escapes into the backoff ladder.
- `TestStaleDependentsPassEnqueuesStaleDependents` — end to end on the new driver.
- `TestStaleDependentsPassIgnoresUnregisteredKinds` — and the late-registration
  property: registering the kind makes the same row appear.
- `TestDependencyWakeSurvivesRestart` — the item-2 end-to-end: the waker's lookup
  fails, the process restarts, the dependent converges with the startup full pass
  **off**. This is `TestDependencyRequeueLostAcrossRestart` (`reconciler_test.go:527`)
  rewritten to assert convergence rather than loss; it is already in that shape.
- `TestSelfDependentObjectDoesNotSelfStale` — the self-edge exclusion.

New, store-level:

- `TestDependentsListStaleFindsMovedTargets`,
  `TestDependentsListStaleTreatsMissingCursorAsStale`,
  `TestDependentsListStaleExcludesSelfEdges`,
  `TestDependentsListStaleFiltersByKind`,
  `TestDependentsListStaleFindsDependentsOfUnregisteredTargets` — the filter applies
  to the dependent only. A registered dependent of a client-only target must be
  returned when that target moves; this is the test that fails if someone narrows the
  scan to edges with two registered endpoints,
  `TestDependentsListStaleReturnsEachDependentOnce` (the `GROUP BY`),
  `TestDependentsListStalePages`.
- `TestDependencyWatermarkSetGatesOnOutgoingDependsOn`.
- `TestDependencyWatermarkSetUpserts` — the second pass overwrites rather than failing.
- `TestDependencyWatermarkSetNeverRegresses` — a lower cursor arriving after a higher one
  leaves the stored value alone (the `DO UPDATE ... WHERE`).
- `TestDependencyWatermarkSetSuppressesNonAdvancingWrites` — re-applying the same cursor
  reports `changes() == 0`, so the row is not rewritten. Asserts the suppression the
  `RequeueAfter` cost argument depends on.
- `TestDependencyWatermarkSetMovesReconciledAtWithTheCursor` — and only with it: a
  suppressed write leaves the timestamp alone, which is what stops it being read as a
  heartbeat.
- `TestDependencyWatermarkSetSkipsCollectedObject` — the FK guard: an id whose row was
  removed since the load writes nothing and returns no error, rather than `FOREIGN KEY
  constraint failed`. This is the test that pins the gate's second job, so it fails
  loudly if open question 3's remedy is applied without a replacement guard.
- `TestDependencyWatermarkSetBumpsNoResourceVersion` — it writes no `objects` row at all.
- `TestDependencyWatermarksCascadeOnObjectDelete` — the row goes with the object.
- `TestObjectsGetForReconcileReturnsTheWriteCursor`.
- `TestObjectsGetForReconcileReportsHasDependencies` — true with an outgoing
  `depends_on` edge, false with only `owned_by`, false with none.

Constrains the implementation — these must keep passing unchanged:

- `TestWakerSeedsFromTheStoreCursor` (`waker_test.go:67`),
  `TestWakerRetriesSeedOnTheNextTick` (`:82`),
  `TestWakerHoldsTheWatermarkOnScanFailure` (`:197`),
  `TestWakerHoldsTheWatermarkOnLookupFailure` (`:215`),
  `TestWakerSkipsUnregisteredKinds` (`:121`). The waker is unchanged; only its doc
  comments move.
- `TestSelfDependentObjectWakesOnSpecChange` (`reconciler_test.go:615`).

## Files

- `sqlite/migrations/0001_init.sql` — the `dependency_watermarks` table (no new
  migration)
- `internal/storeapi/storeapi.go` — three methods and the `ReconcileLoad` type;
  `RawObject` untouched
- `sqlite/store.go` — the three implementations
- `reconciler.go` — load through `ObjectsGetForReconcile`; record on success when
  `HasDependencies`; the two early returns
- `drivers.go` / `beehive.go` — the stale-dependents pass, its interval, its default
- `options.go` — `withStaleDependentsInterval` (unexported, tests only)
- `waker.go` — doc comments only: the type doc and `seed`'s apologia for a lossy
  retry, which is no longer a loss
- `testutils_test.go` — `fakeStore` gains the three methods
- `docs/adr/` — a new ADR for this decision; the
  [drivers ADR](../docs/adr/2026-07-28-periodic-scan-drivers.md) gains the sixth
  driver and keeps its "deliberately not persisted" section, which stays correct
- `docs/reconcile-triggers.md` — §5/§6, and a new entry for the stale-dependents pass
- `TODO.md` — three items resolved; `observed_cursor` becomes this spec; the cycle
  item gains the second-driver note
- `CLAUDE.md` — the driver count (five → six) and the waker bullet

## Resolved during review

All three questions this spec opened are now decided, and are recorded here so the
reasoning is not re-litigated during implementation.

1. **Bundle the two `RawObject` items into this break? No.** The module is at v0.9.0
   with no `/v2`, so a v0.x break costs external implementers nothing and there is
   little to amortise; and the side table leaves `RawObject` untouched, so there is no
   coupling. Both items are write-side, so no read-path method can address them.
   Sequence the reshape separately, before v1.0. See "This is the `Store` break".
2. **The default cadence: 60 seconds.** Set by acceptable staleness after a crash
   rather than by scan cost, since a steady-state pass finds nothing and enqueues
   nothing. See "The stale-dependents pass" for the reasoning and the measurement that
   would move it.
3. **Eliminate the per-reconcile write-lock acquisition: yes, via
   `ReconcileLoad.HasDependencies`.** The earlier concern — that a has-dependencies
   flag would displace the SQL `EXISTS` gate and with it the foreign-key guard — does
   not apply, because the flag *skips the call* rather than replacing the gate. When
   it is true the gated statement runs unchanged. See the reconciler's skip rule.

## Still open

Nothing blocking. Two things to measure after landing, neither of which changes a
design decision:

- Whether the 60s cadence is right for a realistic `depends_on` edge count.
- Whether the first-deploy herd is large enough on a real store to want a paced first
  step rather than a page-to-exhaustion one.
