# A delete request pushes its own collect

**Status:** not built.

## Problem

The whole deletion path is pull. Cases 9, 10 and 11 in
[`reconcile-triggers.md`](../reconcile-triggers.md) share one record,
`deletion_requested_at`, and one driver, the GC sweeper at 30s.

Two costs follow.

**The public API is asymmetric.** `Create` enqueues its object at commit.
`Delete` stamps the row and schedules nothing — `clientImpl.Delete` says so:
*"Nothing is scheduled: the mark is the signal, and the GC tick is guaranteed."* So
the same object, through the same controller, reconciles in microseconds on create
and in up to 30 seconds on delete. A caller cannot see why.

**A cascade costs one sweep per level.** `gcCollect` marks owned children and
returns; the mark is what puts them in the next sweep's listing. A four-level owner
tree takes about two minutes to collect, and the depth multiplies the interval.

## What exists

Almost all of it.

- `DeletionRequestsCreate` and `DeletionRequestsCreateByName` return a `marked` bool.
  `Delete` and `DeleteByName` already gate `signalKindWritten` on it. That bool is
  the same shape as the `changed` gate the spec-write push uses.
- `deletionAdvance(ctx, gk, id)` already routes one deletion-pending object: a
  registered kind is enqueued so its controller can clear finalizers, a client-only
  kind is collected here.
- `gcCollect`'s cascade already walks the marked children and dedups them per kind,
  to wake each child kind's watch tailer. The same loop can carry the push.

## Proposal

Two pushes, both on `Store.AfterCommit`, both routed by `deletionAdvance`.

1. **The delete request.** `Delete` and `DeleteByName` push the marked object, gated
   on `marked`. An unchanged idempotent retry pushes nothing, which matches the
   watch wake beside it.
2. **The cascade.** `gcCollect` pushes each child it marked, alongside the wake it
   already sends. A cascade then advances one level per commit instead of one level
   per sweep.

## What must stay true

- **The sweeper is unchanged and stays the guarantee.** `WithGCInterval` rejects a
  non-positive value, so every push has a tick behind it. `deletionAdvance` is
  already safe to repeat, and both arms are idempotent.
- **The routing is correctness, not speed.** `gcCollect` cannot clear a finalizer, so
  pushing a registered kind straight into `gcCollect` would never make progress. Push
  through `deletionAdvance` or not at all.

## The one real hazard

**An `AfterCommit` hook runs synchronously, on the committer's goroutine, after the
outer commit.** `Within` flushes its hooks outside any lock and a hook may write to
the store and re-enter `Within`.

For a registered kind that is fine: `deletionAdvance` enqueues and returns.

For a **client-only** kind `deletionAdvance` calls `gcCollect`, which opens a
transaction, cascades to children and may delete the row. Pushing that arm inline
would make `Client.Delete` perform the entire collect — and, through the cascade
push, an entire subtree — before it returns to its caller. That is a new and
surprising cost on a call that is one `UPDATE` today.

Three options:

1. **Push the enqueue arm only.** A registered kind is pushed; a client-only kind
   waits for the sweeper. Smallest change, no new machinery, and it leaves the
   asymmetry in place for exactly the kinds that have no controller to notice it.
   **Recommended.**
2. **Hand the collect to the sweeper's goroutine** through a signal, in the shape
   `signalKindWritten` already uses for the tailer. Correct and uniform, and it costs
   a new wake path on the GC loop.
3. **Push both arms inline.** Rejected. It puts unbounded work on the caller's
   goroutine and makes `Delete`'s cost depend on the subtree below the object.

**Decided: option 1**, with option 2 as the follow-up if a client-only kind is ever
measured to need it. `Within` flushes its hooks on the committer's goroutine, outside
any lock, and explicitly permits a hook to re-enter `Within` — so option 3 would make
`Client.Delete`'s wall time depend on the size of the subtree beneath the object,
which is not a cost a caller can predict from the call. Record the choice on
`Client.Delete`'s godoc: a registered kind reaches its controller at commit, a
client-only kind waits for the sweeper.

## Tripwires

`TestIntegrationGCResumesDanglingDeleteOnStartup`,
`TestGCSweepDispatchesRegisteredKind` and `TestIntegrationGCSweepsClientOnlyKind` pin
the pull path. All three must still pass with the push disabled — that is the "every
push has a pull behind it" test for this spec.

`TestIntegrationGCCascadeDeletesOwnerAndChild` and
`TestCollectCascadesAndBlocksOnChild` pin the cascade. Neither asserts how many
sweeps a level costs, so neither blocks the change.

## Done when

- `Delete` on a registered kind reaches the controller at commit, not at the next
  tick.
- A cascade of depth D collects in D commits rather than D sweeps.
- With the push paths disabled, every test above still passes.
- Case 9 and case 10 in [`reconcile-triggers.md`](../reconcile-triggers.md) list a
  push, and section 1's *"A delete does not push"* is corrected.
