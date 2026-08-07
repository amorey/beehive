# Reclaim a client-only object's reconcile_owed count

- **Status:** Accepted — implemented in `beehive.go` and `sqlite/store.go`.
- **Date:** 2026-08-05

## Context

`Edges().Add` stamps `reconcile_owed` on a new `depends_on` edge's source, atomically
with the edge. One thing drains it: `ReconcileOwed().Decrement`, called by a controller
after a successful pass.

Edges are deliberately cross-kind, so the source can be an object of a kind with no
registered controller. That row has a producer and no consumer. Its count and its
entry in `idx_objects_reconcile_owed` last as long as the row, and re-creating a
deleted edge stamps again, so it is not bounded by the number of distinct targets
either. Nothing reads the count while the kind stays client-only — it is *unread*,
which is not the same as harmless.

## Decision

Zero the count for kinds with no reconcile loop, on the GC sweeper's existing
cadence. One store verb, `ReconcileOwed().Sweep(ctx, keep)`, called by
`reconcileOwedSweep` with the registered kinds.

**Safe because the count is redundant with the dependency watermark.**

> **Invariant R.** A nonzero `reconcile_owed` never co-exists with a dependency
> watermark high enough to hide that dependent from a sweep starting at cursor 0.

It holds at both producers and at the drain. `Edges().Add` stamps the count and
*deletes* the dependent's watermark row in the same transaction, and
`Dependencies().ListStaleSince` treats a NULL `reconciled_against` as stale, so the
dependent stays listable from cursor 0 for as long as the edge exists.
`ReconcileOwed().Stamp`, the stale pass's producer, stamps exactly the refs that
listing returned *because* their watermark was low, and does not raise it — only a
successful reconcile does. And the drain moves both together. So clearing the count
destroys the prompt record and never the derivable one.

**Which makes "the kind gains a controller later" a corollary, not a second
argument.** `Register` errors once `bh.state != beehiveNew`, so a kind gaining a
controller means a new process, which builds a `staleDependents` at cursor 0 with
that kind now in `sd.kinds`. By R its first sweep re-derives and re-stamps
everything the reclaim dropped. The count is the fast path; the watermark is the
record.

**No option and no driver of its own.** It rides `gcSweeperRun`, which cannot be
disabled, and there is nothing to tune. The one `UPDATE` per tick is unconditional,
unlike the retention sweeps that early-return when unconfigured: a zero-row `UPDATE`
dirties no pages, and a read to avoid the write would cost the same round trip.

## Consequences

The clear is no-emit — no `resource_version`, no `object_writes` entry. It runs
every tick, so emitting would wake every watch tailer and the dependency waker,
which subscribes across every kind rather than one, for a write no consumer can act
on. Pinned by tests at both levels.

It triggers no reconcile, so it earns no entry in
[docs/reconcile-triggers.md](../reconcile-triggers.md) — noted because a reader
will look.

Registered kinds are never touched, so the sweep cannot race a legitimate stamp:
`ReconcileOwed().Decrement` subtracts an observed count and this sets an absolute 0,
but they never address the same row.

The predicate has no equality constraint to drive the planner, so
`idx_objects_reconcile_owed` is a preference rather than a certainty. An EQP
assertion pins it; without the index the sweep is a full scan of `objects` every
tick.

**Under genuine multi-process, one narrow case survives.** Process A stamps and
enqueues in memory, process B without that kind registered zeroes the count, A
loses the enqueue, and A's stale cursor has already moved past the target — that
dependent strands until a restart. Documented rather than closed: the fix is to
state single-process as an API-level contract, which is a decision of its own and
not a complication of this sweep.

### Alternatives considered

- **Gate the stamp instead of reclaiming it.** `DependenciesAdd` does not learn
  `fromID`'s kind until `res.From` comes back *from* the `Edges().Add` call, so gating
  means either a pre-read on a path controllers re-run every pass, or pushing the
  registered-kind set into `EdgesAddInput` and gating in SQL — a kind list threaded
  through every edge write. It also splits the stamp from the edge, which
  `DependenciesAdd` exists to keep indivisible. The sweep needs the same kind list,
  once per sweep instead of once per write.
- **Keep the count, on the grounds that a later controller needs it.** Answered by
  the corollary above: the watermark already carries it.
- **Do nothing.** The cost is storage-only and does not compound. Rejected once the
  redundancy argument made the reclaim cheap to justify — but it is why this is a
  sweep on an existing cadence rather than anything with its own machinery.
