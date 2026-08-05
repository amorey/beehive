# A cleared finalizer pushes its own collect

- **Status:** Accepted — implemented in `controller.go`, `reconciler.go`,
  `workqueue.go`, `internal/storeapi/storeapi.go`, `sqlite/store.go`.
- **Date:** 2026-08-05

## Context

A collect is blocked while finalizers are pending, or while `EdgesHasIncoming`
reports a referrer under RESTRICT. Every block is temporary, and nothing
signalled a route out of one: an object freed by its controller clearing the last
finalizer sat deletion-pending until the next sweep, up to a GC interval later.
That was the last interval on the deletion path after the
[delete request's push](2026-08-04-a-delete-request-pushes-its-own-collect.md)
shipped, and the one a caller is most likely to be watching.

Three routes unblock a collect. Only the cleared finalizer is in process, in Go,
on a call that already knows the object. The last child's removal needs a
reverse-edge lookup before the delete, and `DependenciesDelete` dropping the last
referrer is invisible to every cursor in the system — an edge write bumps no
`resource_version` and appends no write-log entry (see [`TODO.md`](../TODO.md)).

## Decision

`ControllerClient.FinalizersDelete` pushes a collect of its object on
`Store.AfterCommit` when the removal cleared the last finalizer from a
deletion-pending row.

**It routes like every other push**, through `signalRequeueNow`, which resolves
the reconciler in the hook. No exception to the delete-request ADR was needed:
`FinalizersDelete` is folded to `c.gk` and a `ControllerClient` is built only by
`Register`, so the kind is registered by construction and `deletionAdvance` —
whose client-only arm runs `gcCollect` inline — is never on this path.

**The store reports the gate.** `FinalizersDelete` returns `clearedLast`,
computed from the row the write already loaded. It is named for what the write
changed rather than for `unblocked`, because the store cannot say whether the
collect is free: RESTRICT may still hold the row, and `gcCollect` re-checks. The
push is a probe.

**Immediate, not throttled.** Finalizers are set only at create and the list only
shrinks, so clearing the last one is a once-per-object transition and cancelling
a pending alarm can never become a repeat.

**A collected row's queued wake is dropped.** The push lands while its own object
is still in flight, so the queue marked it dirty and `done` re-queued it — one
dispatch that could only read `ErrNotFound`. The worker now learns from the
adapter that the row is `gone` and calls `workQueue.forget`, which clears the
dirty slot and the floor entry before ending processing. Sound because ids are
never reused, so nothing about that id is answerable again. `discard` became
`forget`: it was the same method missing the `clearDirty`.

## Consequences

The push is redundant in the shape both `examples/cascade` controllers use — a
controller clearing its own object's last finalizer during that object's pass,
where the tail `gcCollect` collects it in the same pass. It is load-bearing when
the clear lands outside a pass over the object it frees: on a sibling, or between
a load and a delete request issued from elsewhere in this process. The redundant
case now costs nothing, because that is the dispatch `forget` drops.

Routes 2 and 3 still wait for a sweep. `WithGCInterval` cannot be disabled, so
every block keeps a tick behind it, which is also what covers route 1 after a
crash.

`TestIntegrationClearedFinalizerCollectsWithoutThePush` clears through the store
to keep the pull path pinned, since there is no knob that disables a push.
