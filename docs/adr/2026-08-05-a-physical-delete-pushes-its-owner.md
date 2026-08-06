# A physical delete pushes the owner it was blocking

- **Status:** Accepted — implemented in `gc.go`.
- **Date:** 2026-08-05

## Context

`gcCollect` refuses to remove a row while `EdgesHasIncoming` reports a referrer
under RESTRICT. For an owner, the referrer is each child's `owned_by` edge, and the
block clears when the last child's row is physically deleted: `edges.from_id` is
`ON DELETE CASCADE`, so the edge goes with the child.

Nothing could signal that. The child's delete appends an `object_writes` entry, but
the entry does not carry the owner's identity and the edge that did is already gone
inside SQLite by the time any consumer reads the log. The owner waited for the GC
sweeper's next tick, and an N-level tree cost N ticks — each level unblocks the one
above it only after it is itself removed.

## Decision

Read the outgoing `owned_by` edges immediately before `ObjectsDelete`, in the same
transaction, and push what they name at commit.

**`owned_by` only.** A row reaching `ObjectsDelete` is always deletion-pending, and
`EdgesHasIncoming` already discounts a `depends_on` edge from a deletion-pending
source. So the dying row's `depends_on` edges were blocking nobody, and pushing
their targets would be noise on the teardown path. `owned_by` is the relation that
always counts, which is why it is the one that has to be signalled.
`EdgesListOutgoingByRelation` is existing surface: no new store method, no counter.

**Immediate, via `signalRequeueManyNow`.** A throttled push would meet the owner's
own pending alarm — armed by its delete push a moment earlier — and be absorbed,
costing the re-enqueue floor per level of the tree, on top of the backoff ladder
that can outlast the GC interval.

**Gated on the owner's `deletion_requested_at`.** Only a deletion-pending owner was
blocked by the dying row, so only it is pushed. The gate is not an optimisation: the
other push paths gate on a store bool that lands once per *object*, where a physical
delete lands once per *row*, and rows are unbounded. Ungated, a controller that
replaces an owned child each pass would drive itself — the owner deletes C1, C1's
collect pushes the owner with the floor bypassed, the owner deletes C2 — at two
reconciles per cycle with nothing damping it. Coalescing bounds a teardown burst; it
does nothing for a steady one-delete-per-pass cycle.

Within that gate the push stays a probe: it does not check that this child was the
*last* referrer, and `gcCollect` re-checks the block itself.

## Consequences

The fan-out is N→1, unlike the cascade's N→N: N children converge on one owner and
only the last can unblock it, so the other N−1 each pay a dispatch on a dying
object. "A physical delete lands once per row" establishes termination, not a bound.

**The bound is the work queue's coalescing, and it is partial.** `requeueNow`
reaches it even though it bypasses the floor: `addLocked` returns early on
`isQueued`, and an id already in flight is left dirty for `done` to re-queue. Repeat
pushes at a queued or processing owner cost a lock and nothing else, so a wide
teardown collapses to roughly one owner dispatch per drain — an owner with 10k
children does not get 10k reconciles. What it does *not* bound is an owner sitting
in backoff: `requeueNow` stops the timer and clears the alarm before `addLocked`,
and `isQueued` is false for an id whose only state is a pending alarm, so a repeat
push there discards the pending wait. The ladder itself survives — `requeueNow` never
reads `backoffFor`, which only a successful reconcile clears — so the delay goes on
doubling while nothing waits it out.

This push is gated on an object other than the one it enqueues, which costs one
`ObjectsGetMeta` per owner — on the delete path only, and owners are normally one.
The create's owner push shares that shape, both bounds and both costs. See
[its ADR](2026-08-05-a-create-pushes-a-deleting-owners-collect.md).

A self `owned_by` edge is unreachable here: `owned_by` is never discounted, so a row
owning itself is blocked forever and never reaches `ObjectsDelete`. It is also not
constructible through the public API, where `WithOwner` is a create option.

Route 3 of case 11 — `DependenciesDelete` dropping the last referrer — is untouched
here, and closed separately by
[its own ADR](2026-08-05-a-dropped-dependency-pushes-its-target.md), which reports
the lifted block from the edge write rather than inferring it from a cursor.

### Alternatives considered

- **Every outgoing relation.** Rejected on the discount above; it would also need
  `EdgesListOutgoing` added to the `Store` interface, where it is not today.
- **No gate at all**, on the grounds that the push is a probe and `gcCollect`
  re-checks. Rejected: it costs an `ObjectsGetMeta` per owner to close a real
  feedback loop, and a live owner was never blocked in the first place.
- **Put the owner id in the write-log row image.** Widens the log's contract — a
  create/update entry deliberately carries no payload — to serve one consumer, and
  still leaves that consumer polling rather than pushed.
