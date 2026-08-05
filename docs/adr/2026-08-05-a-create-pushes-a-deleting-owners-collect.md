# A create under a deleting owner pushes that owner's collect

- **Status:** Accepted — implemented in `client.go`, `sqlite/store.go`.
- **Date:** 2026-08-05

## Context

`WithOwner` writes an `owned_by` edge in the create's transaction. Nothing checked
the owner's lifecycle: `insertObject` read none of it, and `EdgesAdd` verifies only
that both endpoints exist. So a child created against an owner that is already
deletion-pending — and whose cascade has already listed its children — was born live
and unmarked under a finalizing owner. Its edge counts as a live claim in
`EdgesHasIncoming`, which discounts only deletion-pending `depends_on` sources, so
the owner could not be collected.

Nothing else reaches that owner. `EdgesAdd` bumps no `resource_version` and appends
no write-log entry, so no cursor sees the edge; the waker reads only `depends_on`;
and the child's own `gcCollect` returns at once because the child is not finalizing.
The owner waited for `deletionPendingSweep` to re-list it, at which point
`DeletionRequestsCreateFromOwner` — which is built to be re-run — picks the child up.

So this was a latency gap, not a strand: one GC interval, on a sweeper
`WithGCInterval` refuses to disable, and visible as a plainly stuck deletion-pending
owner.

## Decision

Report the target's lifecycle from `EdgesAdd` and push the owner when it is already
deleting. `gcCollect` re-cascades and marks the new child, so the end state is the
sweep's, one interval earlier.

**The gate is the owner's `deletion_requested_at`,** read by the endpoint-existence
join `EdgesAdd` already performs — one widened `SELECT`, no second read, no new
store method. `EdgesAddResult` gains `To` and `ToDeleting`; `To` is what routes the
push, since edges are cross-kind and the owner need not share the child's kind.

**Only a deleting owner is pushed.** A live one was waiting on nothing, and
`requeueNow` bypasses the re-enqueue floor. This is the same gate, for the same
reason, as the physical delete's: both writes land once per *row*, and rows are
unbounded, so an ungated push would let a controller that replaces an owned child
each pass drive its owner.

**Immediate, not throttled.** The owner's own alarm is typically already pending
from its delete push, and a throttled wake would be absorbed by exactly the alarm
this exists to beat.

**Nothing changes semantically.** The child is still created, still returned live,
and still marked by the owner's cascade. There is no new error, no new option, and
no new record — the record is the owner's mark, which the sweeper already reads.

### The gate misses no interleaving

`Within` is `BEGIN IMMEDIATE` on one connection, so writers serialize and the
owner's `gcCollect` transaction is entirely before ours or entirely after:

- **Mark commits before ours begins** → we read it, and push.
- **Mark commits after ours commits** → the cascade that follows it reads our edge.
- **Owner already physically gone** → the endpoint check fails and the create rolls
  back with `ErrNotFound`, as before.

A cascade cannot run *between* our edge insert and our commit, so those are all.

## Consequences

A cascade now advances over a child created after it ran, at commit rather than at
the next sweep. The sweeper remains the pull behind it: a lost push, a client-only
owner, or an owner whose controller is registered in *another process* all fall back
to it, costing one interval and nothing else.

**The feedback-loop bound is partial, and shared with the physical delete.** A
controller that creates a fresh child under a deletion-pending owner on every pass
pushes that owner with the floor bypassed. Coalescing bounds that only while the
owner is queued or in flight — `addLocked` returns early on `isQueued`. It does not
bound an owner sitting in backoff: `requeueNow` stops the timer and clears the alarm
before `addLocked`, and `isQueued` is false for an id whose only state is a pending
alarm. So an owner whose collect keeps failing has its ladder reset by every such
create. This is a property of `requeueNow` rather than of either push, and the
physical delete carries it identically.

### Alternatives considered

- **Reject the create** with a new sentinel. Adds a public failure mode to `Create`
  and `GetOrCreate` and races anyway — the owner can be marked the instant after the
  check — so the sweep backstop would stay regardless. It buys nothing the push does
  not.
- **Create the child already marked.** Self-consistent and needs no new error, but
  it manufactures a deletion-pending object the caller never asked to delete, whose
  spec is then unreachable, and needs a new `ObjectsCreateInput` field to say so.
- **Leave it to the sweeper.** The prior state. Defensible while the fix looked
  expensive; it is one gated `AfterCommit` over a read the store already performs.
- **Bump the edge's `resource_version`** so a cursor could see the declaration.
  Widens what a write log entry means to serve one consumer, and leaves that
  consumer polling rather than pushed.
