# Dependency staleness is re-derived from a per-dependent watermark, not recorded per wake

- **Status:** Accepted — implemented in `sqlite/migrations/0001_init.sql`
  (`dependency_watermarks`), `internal/storeapi/storeapi.go`, `sqlite/store.go`,
  `reconciler.go`, `waker.go`, `options.go`.
- **Date:** 2026-07-29

## Context

Three failures shared one shape: a dependent D that has *settled* — its own
generation never moved, nothing stamped `reconcile_owed` — is stranded because the
only record that it owed a reconcile was in memory and did not survive a restart.
No owed-work listing can name it afterwards. The startup full pass masks all three
and may not be relied on, since it scales with the object count.

- The waker seeds on its own goroutine, so a write racing startup can commit below
  the watermark the waker then takes.
- A crash during a waker outage: the waker fails a lookup and holds its cursor, the
  process dies, and the restart re-seeds from the current version.
- A target change landing mid-reconcile is handled correctly in memory by the work
  queue's dirty/processing sets, but the wake exists only in memory.

The obvious fix is to make the waker's own record durable: stamp `reconcile_owed`
on every dependent, then advance a persisted scan cursor in the same transaction.
That works, and its ordering argument is sound — at-least-once falls out of
committing the stamps before the cursor moves.

It has one structural limit. Its guarantee is *"the waker recorded it."*
`reconcile_owed` is a promise the code makes to itself, and a promise can only be
checked against the code that makes it. If the waker had a bug, or was not running
— a process with no registered controllers, a kind between builds — the promise was
never made, and the absence of a stamp is indistinguishable from "nothing was
owed." Draining the count also erases the evidence: once at zero, the store cannot
say whether D converged against T's version 40 or version 12.

## Decision

**Record a measurement instead of a promise.** `dependency_watermarks.reconciled_against`
holds the store-wide write cursor as of the moment an object's last successful
reconcile loaded its state. A dependent is stale exactly when some target it
depends on has moved past that watermark:

```sql
t.resource_version > c.reconciled_against
```

Three consequences follow, and they are the whole design.

**1. The in-memory waker becomes an optimisation, not a guarantee.** It keeps its
1s latency and its in-memory scan watermark, unchanged. Losing a wake now costs
latency until the next staleness pass, not permanent divergence.

**2. Correctness comes from re-derivation, on a slow cadence.** A sixth driver —
the stale-dependents pass, 60s, not disableable — runs that query, paged, and
enqueues what it finds. It recovers a wake lost by *any* means, including means
that do not exist yet.

**3. Nothing durable has to be kept in sync.** There is no at-least-once
bookkeeping, no stamp/advance transaction, no ordering constraint between an
enqueue and a commit, no count to drain. The watermark is monotonic and idempotent;
writing it twice is writing it once.

`reconcile_owed` stays, carrying `Edges().Add`'s stamp (since broadened to
[every new `depends_on` edge](2026-07-29-stamp-every-new-dependency-edge.md)).
The two are complementary: it is the durable record for the declare-time cases,
riding a write `Edges().Add` was making anyway; the watermark is the backstop for a
wake lost after the edge exists. This decision adds **no** new producer of
`reconcile_owed`, so the "there is deliberately no standalone increment here" block
in `storeapi.go` stays true — the scan-watermark design would have invalidated it.

### The write

One narrow-row upsert per successful reconcile of an object that has dependencies,
and nothing at all for any other reconcile. Both extra values the reconciler needs
ride `Objects().GetForReconcile`'s existing statement as correlated subqueries, so the
load costs no extra round trip.

Three parts of it are load-bearing:

- **The `EXISTS` gate on an outgoing `depends_on` edge.** An object with no
  dependencies can never be found stale, so a row for it is dead weight the scan
  would probe forever. It is *also* the foreign-key guard: `gcCollect` runs after
  the controller returns and can physically remove the object, whose edges cascade
  with it — so the gate closes and nothing is written, rather than the write failing.
- **The `WHERE` on `DO UPDATE`.** It makes the stored cursor monotonic, and it
  suppresses a non-advancing write outright — no page dirtied, no WAL frame. That
  second job is what keeps a `RequeueAfter` polling controller that declares
  dependencies from paying a row write per pass in a quiet store. `MAX(…)` in the
  `SET` would give monotonicity alone and still write the row every pass.
- **`ReconcileLoad.HasDependencies`, which skips the *call*, never the gate.** The
  pool is `MaxOpenConns(1)` with `_txlock=immediate`, so an `INSERT` that writes
  zero rows still opens a write transaction and serialises against every other
  statement in the process. Issuing it unconditionally would put that on every
  successful reconcile of every kind. The flag confines it to dependents; when it is
  true the gated statement runs unchanged.

**The cursor comes from the load, never from the end of the pass.** Every read the
controller makes happens after `Objects().GetForReconcile` returns, so the recorded
cursor is at or below the true one when it read its dependencies. A target that
moves during the pass is counted as still-owed and reconciled once more — wasted
work, never lost work. A cursor sampled after the controller's reads inverts that:
it lands above a change the pass did not observe, and D is stale with nothing left
to find it. Same shape as `ReconcileOwed().Decrement`'s subtract-the-*observed*-count
rule.

A failed reconcile, a quarantined undecodable row, and a failed watermark write all
leave the cursor where it was, so the object stays stale and is found again. That
self-healing property is why the write is logged-and-swallowed rather than returned:
returning it would push a bookkeeping write into the backoff ladder.

### A new edge clears the watermark

A cursor measures how much of the store a pass observed *given the dependency set it
had*. Add a target and the measurement no longer covers it — and the failure is
silent, because the scan compares that target's `resource_version` against a
watermark recorded later than it. A target that has simply been sitting there reads
as converged.

So `Edges().Add` deletes `fromID`'s row when it creates a new `depends_on` edge, gated
on the same `NOT EXISTS` as the wake stamp and on the same side of the insert. An
absent row already means stale, so the staleness pass would reconcile the dependent
against its new target even if nothing else did.

What *guarantees* a fresh declare reaches its dependent is no longer this clear but
the wake stamp beside it, which
[now fires for every new `depends_on` edge](2026-07-29-stamp-every-new-dependency-edge.md)
rather than only when the target moved past the caller's claim. The clear stays as
hygiene for the invariant above: a watermark that outlived a growth of its
dependency set would misreport convergence to the scan for the window until the
stamped pass runs and rewrites it honestly.

The cost is nothing in the ordinary case. Re-asserting a dependency set costs
nothing after the first declare, because of the edge-new gate. A controller
declaring from inside its own `Reconcile` mostly pays nothing either: that pass
rewrites the watermark when it succeeds, from the cursor it loaded at — which is
sound for exactly the reason above, since the controller's read of the new target
happened after the load. Self-edges are skipped, matching `Dependencies().ListStaleSince`,
which excludes them.

One case does cost a pass, and it is not this change's doing: an object's **first**
`depends_on` edge. `ReconcileLoad.HasDependencies` is sampled at load, before that
edge existed, so the reconciler skips `Dependencies().WatermarkSet` and leaves no row —
and an absent row means stale. The object is reconciled once more and settles. It is
bounded at once per object ever, self-extinguishing, and in the over-reconcile
direction; the alternative is issuing the gated write on every successful reconcile
of every kind, which is precisely the write-lock acquisition `HasDependencies` exists
to avoid. Pinned by
`TestReconcileSkipsTheWatermarkWhenTheFirstDependencyIsDeclaredMidPass`.

**The residual window this left was a strand, and it is closed.** A *third party*
declaring between a dependent's load and its own watermark write had the clear
immediately undone by that pass, which never saw the new target — the dependent
read as converged with nothing left to re-derive it, permanently if the target
never moved again. The clear could never survive that interleaving, because a pass
in flight legitimately re-derives the state the clear invalidated. Recording owed
work is what closed it:
[the stamp on every new edge](2026-07-29-stamp-every-new-dependency-edge.md) lands
above the count the in-flight pass observed at load, so the load-scoped decrement
cannot consume it. Pinned by `TestReconcileMidPassDeclareLeavesTheDependentOwed`.

### A side table, not a column on `objects`

`objects` rows carry `spec` and `status` inline, SQLite rewrites the whole record on
`UPDATE` — including overflow pages — and this is the highest-frequency writer of
the smallest value in the schema. Eight bytes would cost a multi-kilobyte row
rewrite per pass of every dependent. The side table also leaves `RawObject`
untouched (which `docs/TODO.md` is already trying to shrink) and removes a genuine
misreading hazard: a `dependency_watermark` column beside `resource_version` reads
as a comparable pair, like `observed_generation`/`generation`, and is not one.

What it costs instead: one extra primary-key probe per edge on the scan, since the
cursor no longer arrives free inside the dependent's row lookup. A cached rowid seek
on a small table, paid per pass rather than per reconcile.

### The kind filter is on the dependent, never the target

`Dependencies().ListStaleSince` filters by kind for the same reason `ReconcileOwed().ListIDs`
does: only a registered kind has a reconcile loop to enqueue into. A client-only
dependent never gets a watermark written and is therefore stale forever, so
filtering is what makes it cost zero rather than a permanent addition to every
pass's working set. Nothing is stranded by the exclusion, since there is no loop to
be owed a pass; a kind registered in a later build appears in the list and is found
on the next pass.

Extending that filter to the *target* would be silently wrong. A registered object
may depend on a client-only one — the whole reason the waker's scan is store-wide —
so a target's kind is irrelevant to whether its dependents are owed a pass.
`TestDependencies().ListStaleSinceFindsDependentsOfUnregisteredTargets` is the tripwire.

## Consequences

**First deploy enqueues the whole dependency graph.** No object has a watermark row
until it reconciles, and an absent row means stale, so the first start after this
lands finds every dependent of a registered kind at once — and a driver step pages
to exhaustion, so there is no gradual drain. It is not harmful: the work queue
coalesces by id, the loops rate themselves, and each pass records a watermark, so
the herd is one-time and self-extinguishing. The same burst occurs for any kind
registered for the first time in a later build, bounded by that kind's dependents.

**Scan cost is O(`depends_on` edges of registered kinds) per pass**, with three
rowid seeks each. That is a scan
over what *exists* rather than what changed, which [the drivers
ADR](2026-07-28-periodic-scan-drivers.md) warns about — but the population is the
dependency graph, which is exactly the set that can possibly be stale. It is the
tightest bound any re-deriving check can have, and categorically smaller than the
full pass's object count.

**Cycles are unchanged, and gain a second driver.** Two mutually dependent
controllers that write on every pass keep re-staling each other here exactly as they
do under the waker. Not a new bug class, but not neutral either: the stale pass
cannot be disabled, so a cycle now has a second, unkillable driver sustaining it. The
fix remains `docs/TODO.md`'s minimum re-enqueue interval per work-queue item.

**Not addressed:** `RequeueAfter` durability (a controller's private timer, not a
fact about the object), and pushing latency below the 1s waker scan. Multiple
processes sharing one store stays out of scope, but is no longer a hazard here: the
watermark is per-object and derived, so concurrent owners would over-reconcile
rather than lose work.

### Rejected: the durable scan watermark

Stamp `reconcile_owed` on every dependent the waker wakes, then advance a persisted
watermark in the same transaction. Rejected because its guarantee is "the waker
recorded it," which leaves a permanent hole wherever the waker was not running or
was wrong, and because making that promise reliable needs machinery this design does
not need at all: a stamp/advance transaction, id-list chunking inside it, deliberate
double-stamping on replay, a stamping asymmetry for unregistered kinds, and a write
transaction held on the single connection once a second. It is also strictly more
expensive in the steady state — writes proportional to change *events* × dependents,
where this pays one narrow write per reconcile, proportional to *coalesced* work.

It can come back later as a pure startup optimisation, and the composition is sound
*because* the watermark is the ground truth: a checkpoint that is stale, wrong, or
written by a buggy build costs startup latency and nothing else.

**It did come back, in that narrower form**: the waker now persists its scan
cursor and resumes from it, without any new promise about wakes actually
delivered. → [ADR](2026-07-30-durable-waker-cursor.md)

### Rejected: `idx_edges_depends_on`

*(The plan below is the unbounded `DependentsListStale`, which paged by `from_id`
and is removed — see [the cursor ADR](2026-08-03-stale-dependents-cursor.md). The
conclusion carries: `Dependencies().ListStaleSince` drives from `idx_objects_rv` and
reaches `edges` through `idx_edges_to`, and neither wants this index either.)*

The scan pages by `from_id`, and the `edges` primary key already leads on `from_id`,
so the plan is `SEARCH e USING PRIMARY KEY (from_id>?)` — already a covering range
scan with no row fetch, arriving in the order the `GROUP BY` and `ORDER BY` want. (The
query says `CROSS JOIN` to *get* that plan: left to choose, the planner drives from
`idx_objects_kind` instead, which turns paging to exhaustion into one full scan per
page.) The index would buy only skipping `owned_by` entries
during that range scan, and would cost a second full copy of every `depends_on` edge — a secondary
index on a `WITHOUT ROWID` table carries the whole primary key in its payload, so
`(from_id, to_id)` plus the implicit rest *is* the row, byte for byte. Add it only if
a measured `owned_by`-dominant graph shows the filtering matters.

### Rejected: writing the watermark unconditionally

Dropping the edge gate removes one `EXISTS`. It would also put a row in the table for
every object in the store, turning a table sized by the dependency graph into one
sized by the object count, and adding a probe per row to every scan — while removing
the foreign-key guard described above.
