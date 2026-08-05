# A cleared finalizer pushes its own collect

**Status:** not built. Needs [02](02-deletion-push.md).

## Problem

Case 11 in [`reconcile-triggers.md`](../reconcile-triggers.md) says it plainly:
*"Nothing signals the unblocking."*

A collect is blocked when finalizers are still pending, or when `EdgesHasIncoming`
reports a referrer under RESTRICT. Every block is temporary by construction, and no
route out of one is signalled. An owner freed by its last child's removal, by a
`DependenciesDelete`, or by its controller clearing its last finalizer is simply
still deletion-pending when the next sweep runs, up to 30 seconds later.

This is the last GC interval left on the deletion path once
[spec 02](02-deletion-push.md) ships, and it is the interval a caller is most likely
to be watching: the object is visibly stuck deletion-pending with nothing to do.

## Three unblocking routes, and only one is cheap

**1. The last finalizer was cleared.** In process, in Go, on a call that already
knows the object. `ControllerClient.FinalizersDelete` calls
`c.wakeAfter(ctx, ...)`, which wakes the watch tailer and nothing else. This is the
one to build.

**2. The last child was removed.** `gcCollect` already notes it: *"Deleting the row
drops its outgoing edges (ON DELETE CASCADE), which may unblock a target the sweeper
retries on its next tick."* Finding that target needs the reverse-edge lookup before
the delete, because after the delete the edges are gone.

**3. `DependenciesDelete` dropped the last referrer.** An edge write bumps no
`resource_version` and appends no write-log entry, so **no cursor in the system can
see it**. This is the edge-invisibility item in [`TODO.md`](../TODO.md), whose only
named consumer is exactly this case.

## Proposal

Build route 1 only.

`FinalizersDelete` pushes a collect of the object when the removal was real, the
object is deletion-pending, and no finalizers remain. Route it through
`deletionAdvance` on `Store.AfterCommit`, the same path
[spec 02](02-deletion-push.md) establishes.

The store reports whether the removal was real — `FinalizersDelete` bumps a
`resource_version` only on one. The remaining-count and the deletion-pending flag are
on the same row the write already touched, so the gate needs no extra read if the
store returns them.

## What must stay true

- **The sweeper is still the answer for the other two routes**, and for route 1 after
  a crash. `WithGCInterval` cannot be disabled, so every block keeps a tick behind
  it.
- **The push is a collect, not a reconcile.** `deletionAdvance` decides which. A
  registered kind is enqueued so its controller runs; a client-only kind is
  collected. A client-only kind cannot hold a clearable finalizer at all — `Create`
  now rejects one — so route 1 only ever reaches the enqueue arm. That sidesteps the
  inline-`gcCollect` hazard spec 02 records.
- **The gate must be all three conditions.** A finalizer cleared on an object that is
  not deleting owes nothing, and pushing there would collect-probe every finalizer
  removal in the system.

## Why routes 2 and 3 stay deferred

Route 2 costs a reverse-edge lookup on the delete path, for one GC tick of latency on
a case that already resolves itself.

Route 3 cannot be built without new store surface. The fix
[`TODO.md`](../TODO.md) names is **one monotonic counter**, not a log: an epoch
bumped by `EdgesAdd` and `EdgesDelete`, shaped like `driver_cursors`. Recording edge
writes in `object_writes` is the wrong fix on four separate counts, all recorded
there. Build the counter when a second consumer appears, or when this latency is
measured to matter — and note that shipping route 1 removes the most common reason
anyone would notice route 3.

Update the `TODO.md` entry when route 1 ships: its "one consumer left" claim narrows
to the two edge-driven routes.

## Tripwires

`TestCollectKeepsFinalizedObject` pins that a finalized object survives a collect.
`TestCollectDeletesOwnerAfterChildGone` and
`TestIntegrationGCDeleteDependencyUnblocksTarget` pin routes 2 and 3, which do not
change — both must still pass at the sweeper cadence.

`TestClientCreateRejectsFinalizersOnUnregisteredKind` is what makes the client-only
arm unreachable. If that rejection is ever relaxed, this spec's routing argument
fails with it.

## Done when

- Clearing the last finalizer on a deletion-pending object collects it at commit.
- Clearing a finalizer on a live object still pushes nothing.
- With the push disabled, every test above still passes.
- Case 11 in [`reconcile-triggers.md`](../reconcile-triggers.md) records which of the
  three routes pushes and which two wait for a tick.
