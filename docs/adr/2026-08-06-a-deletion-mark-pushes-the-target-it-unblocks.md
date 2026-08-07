# A deletion mark pushes the target it unblocks

- **Status:** Accepted — implemented in `sqlite/store.go`, `client.go` and `gc.go`.
- **Date:** 2026-08-06

## Context

`gcCollect` refuses to remove a deletion-pending row while `Edges().HasIncoming`
reports a referrer under RESTRICT. That check discounts a `depends_on` edge
whose source is itself deletion-pending, so marking the last live referrer lifts
the target's block on the spot.

Three routes out of that block already pushed: a cleared finalizer, the physical
delete of the last child, and a dropped `depends_on` edge. This fourth one did
not. `Client.Delete` enqueued only the object it marked, and the cascade only
the children it marked, so an unblocked target waited for the GC sweeper — one
interval of latency, never divergence.

## Decision

A mark reports the targets it unblocked, and its caller pushes them.

`DeletionRequests().Create`/`DeletionRequests().CreateByName` return
`DeletionRequestResult.Unblocked`; `DeletionRequests().CreateFromOwner` returns
`DeletionCascadeResult.Unblocked`, flat across children. `signalDeletionRequested`
pushes the first with `signalRequeueManyNow`; `gcCollect` appends the second to
the children it already pushes, so the cascade keeps one commit hook.

**The read runs inside the mark's transaction.** The alternative was a separate
`Store` read at the call site, and it buys nothing: reaching the store adds an
exported `Store` member either way, which is the same break — and two of them
once the cascade needs the grouped form. Atomicity is then the only axis, and it
decides. `requestDeletion` self-wraps in `Within`, so a call-site read would run
after the mark committed, needing an `ErrNotFound`-skip policy for a target the
sweeper removed in the gap and a "do not fail the delete" rule for every other
error, because the mark is durable and a retry reports `Marked` false. Inside
the transaction a failed read rolls the mark back for a clean retry. The
predicate also omits "and the source is deletion-pending" — it is the mark that
establishes it — which as an exported member would be a precondition a later
caller could violate silently.

**One query, no row fetch.** `unblockedTargets` joins `edges` to `objects` and
returns refs directly. Both sides are covered: `edges` is `WITHOUT ROWID` with
`PRIMARY KEY (from_id, to_id, relation)`, and `idx_objects_deleting` is keyed
`(id, "group", kind)` under a partial `WHERE deletion_requested_at IS NOT NULL`
— exactly the columns selected under exactly this predicate. Keep them aligned:
this is that index's second consumer. Route 2's per-target `Objects().GetMeta` was
the shape to avoid here, since it reads the whole row including both blobs and N
is the object's dependency count, not its owner count.

**Three gates, all in the predicate.**

- *The mark landed.* `Marked` bounds the push to once per object. The cascade
  collects only over the children it marked: one already deleting discounted its
  edges on the pass that marked it, so an ungated read would re-arm the whole
  subtree on every reconcile of the deleting owner.
- *The target is deletion-pending.* A live target was never blocked, and
  `requeueNow` bypasses the re-enqueue floor, so pushing live targets would
  spin.
- *The relation is `depends_on`.* `Edges().HasIncoming`'s discount is specific to
  it — `owned_by` counts until physical removal — so no `owned_by` target can
  have been unblocked by a mark. That is route 2's push.

A self-edge is excluded: the object's own mark already queues it, and the waker
skips `from_id == to_id` for the same reason. A target two sources share is
repeated, which the work queue coalesces.

Within the gates the push is a probe, not a verdict: it does not check that this
was the target's last referrer, and `gcCollect` re-checks the block. Buying that
check would cost an `Edges().HasIncoming` per target to save a dispatch the collect
already re-derives.

**Immediate, not throttled.** The target is finalizing, so it already carries
the alarm its own delete push armed, and a throttled push would be absorbed.

## Consequences

`DeletionRequests().Create`, `DeletionRequests().CreateByName` and
`DeletionRequests().CreateFromOwner` change signature — a break in the exported
`Store`. Result structs rather than more return values, as the
[write-shapes ADR](2026-07-30-store-write-shapes.md) settled for
`EdgesAddResult` and `EdgesDeleteResult`; `DeletionRequests().CreateByName` would
otherwise return four.

`signalRequeueManyNow` registers a hook of its own, so a client delete now takes
two: its own object, then the targets. Outside an ambient `Within` both run
inline. The cascade still takes one, because `gcCollect` merges.

The fan-out is bounded by the source's dependency count, and the `Marked` gate
bounds it to once per object, so there is no teardown burst to coalesce. A
client-only target resolves to no reconciler and falls back to the sweeper, as
every push path does.

This closes the last exit from case 11's block that did not push. What is bought
is latency only: the target stays deletion-pending, which the sweeper lists, and
the sweeper cannot be disabled.

### Alternatives considered

- **Reading at the call site, after the mark commits.** Rejected above: a
  `Store` break either way, so it gives up atomicity for nothing.
- **`Objects().GetMeta` per target, as route 2 does.** Rejected on cost: N+1
  full-row reads including blobs, on an unbounded fan-out, on every delete.
- **A third field on `DeletionCascadeChild`.** Rejected: `gcCollect` merges
  every push into one slice, so nothing reads the refs per child.
- **Gating on `Edges().HasIncoming` for a genuine last-referrer verdict.**
  Rejected: a query per target to save a dispatch the collect re-derives anyway.
