# A dropped dependency pushes the collect it was blocking

- **Status:** Accepted — implemented in `controller.go` and `sqlite/store.go`.
- **Date:** 2026-08-05

## Context

`gcCollect` refuses to remove a deletion-pending row while `EdgesHasIncoming`
reports a referrer under RESTRICT. Three routes lead out of that block, and two
of them push at commit: a cleared finalizer, and the physical delete of the last
child. The third is `ControllerClient.DependenciesDelete` dropping the last live
`depends_on` edge.

Nothing could signal it. An edge write bumps no `resource_version` and appends no
`object_writes` entry — a ref is not a field of the object — so no cursor in the
system can see one. `EdgesAdd` covers itself with a deliberate `reconcile_owed`
stamp; `EdgesDelete` covered nothing, and the target waited out a GC tick.

The deferred fix on record was a monotonic edge epoch. It does not work: a
counter is read on a driver's own schedule, so it can let a tick skip work, but
it cannot make one arrive sooner, and it carries no identity to enqueue. The cost
here is latency, and latency needs a wake.

## Decision

`EdgesDelete` returns an `EdgesDeleteResult`, and `DependenciesDelete` pushes the
target with `signalRequeueNow` when it reports `Unblocked`.

**`Unblocked` is three facts, one field**: an edge was really removed, the target
is deletion-pending, and the source is not. Only the conjunction is read, so only
the conjunction is returned — see the
[write-shapes ADR](2026-07-30-store-write-shapes.md).

- *An edge was removed*, from `RowsAffected`, bounds the push to once per edge
  ever created, matching `EdgesAdd`'s edge-new gate. A controller may call
  `DependenciesDelete` on every pass.
- *The target is deletion-pending*: a live target was never blocked, and
  `requeueNow` bypasses the re-enqueue floor.
- *The source is not*: `EdgesHasIncoming` discounts a `depends_on` edge from a
  deletion-pending source, so dropping one lifts nothing. The natural caller is a
  finalizing dependent releasing its refs during cleanup — the shape that would
  otherwise push on every pass for nothing. Route 2 already treats the symmetric
  filter as load-bearing.

Within those gates the push stays a probe: it does not check that this was the
target's *last* referrer, and `gcCollect` re-checks the block. Buying that check
would cost an `EdgesHasIncoming` on every drop to save a dispatch — and a
dispatch runs the controller's `Reconcile` in full, which is what makes the three
gates above worth their one query.

**Immediate, not throttled.** The target is finalizing, so it already carries the
alarm its own delete push armed; a throttled push would be absorbed. The ladder
tops out at `defaultMaxRetryInterval`, which equals the default GC interval, so
an alarm armed just after a tick pushes the collect past the next one — the
absorbed push would buy nothing over the sweep it was meant to beat.

**Two statements, no transaction of their own.** The `DELETE` runs, and only a
non-zero `RowsAffected` costs the second query, which reads both endpoints in one
row. Wrapping the pair would hold the store's sole connection across a
`BEGIN`…`COMMIT` for a call that is one statement today, on a path a controller
walks every pass. What the transaction would buy is the guarantee that the second
query finds its rows, and every way it can miss them is benign: the sweeper
collected the target in the gap (no row left to collect), the source was
physically deleted (its edge was already discounted), or the target was marked
deletion-pending in the gap (it is blocked-or-not *now*, and the push is a
probe). Nothing un-deletes a row, so the window admits no wrong answer. An
ambient `Within` still takes both statements.

## Consequences

`EdgesDelete`'s signature changes, which is a break in the exported `Store`.

The fan-out is 1→1 and the gates bound it to once per edge, so there is no
teardown burst to coalesce — unlike the physical delete's N→1. The one loop left
is a controller that re-declares and drops an edge against a finalizing target on
every pass; it terminates when that row goes, and queue coalescing bounds it.

A client-only target resolves to no reconciler and falls back to the sweeper, as
every push path does.

**A fourth exit from the same block is still unsignalled.** Marking the last live
referrer deletion-pending lifts the RESTRICT block through the very discount the
source gate above respects, and `signalDeletionRequested` enqueues only the
object it marked. That target waits for the sweep. See `docs/TODO.md`.

### Alternatives considered

- **A monotonic edge epoch**, the direction previously on record. Rejected above:
  wrong instrument for a latency problem.
- **Recording edge writes in `object_writes`.** Rejected on four counts that
  still hold; they are in `docs/TODO.md`.
- **Gating on `EdgesHasIncoming`** so the push fires only for the genuinely last
  referrer. Rejected: a query on every drop to save a dispatch the collect
  already re-derives.
