# A cleared finalizer pushes its own collect

**Status:** ready to build. The deletion push it builds on has
[shipped](../adr/2026-08-04-a-delete-request-pushes-its-own-collect.md).

## Problem

Case 11 in [`reconcile-triggers.md`](../reconcile-triggers.md) says it plainly:
*"Nothing signals the unblocking."*

A collect is blocked when finalizers are still pending, or when `EdgesHasIncoming`
reports a referrer under RESTRICT. Every block is temporary by construction, and no
route out of one is signalled. An owner freed by its last child's removal, by a
`DependenciesDelete`, or by its controller clearing its last finalizer is simply
still deletion-pending when the next sweep runs, up to 30 seconds later.

This is the last GC interval left on the deletion path now that the
[deletion push](../adr/2026-08-04-a-delete-request-pushes-its-own-collect.md) has shipped, and it is the interval a caller is most likely
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
object is deletion-pending, and no finalizers remain, on `Store.AfterCommit`.

### How it routes — settled

Follow [the ADR](../adr/2026-08-04-a-delete-request-pushes-its-own-collect.md):
resolve the reconciler in the hook, via `signalRequeueNow`. No exception is taken and
none is needed. `FinalizersDelete` is a `ControllerClient` verb folded to `c.gk`, and
a `ControllerClient` is built only by `Register` — so the kind is registered by
construction and the resolve always succeeds. `deletionAdvance` is not on this path
at all, so its inline-`gcCollect` arm is never reached and the hazard the ADR records
cannot arise.

That retires the standing argument from
`TestClientCreateRejectsFinalizersOnUnregisteredKind`. The rejection still matters to
case 11 — it is what makes every block temporary — but the routing here no longer
rests on it.

### The gate is one bool from the store

The three conditions are all on the row the write already touched, so the store
evaluates them and returns the answer:

```go
// FinalizersDelete removes finalizer from id's list. […]
// clearedLast reports that this call removed the last finalizer from a
// deletion-pending row.
FinalizersDelete(ctx context.Context, gk GroupKind, id ObjectID, finalizer string) (clearedLast bool, err error)
```

`clearedLast`, not `unblocked`: the store reports what the write changed, per
[the store-write-shapes ADR](../adr/2026-07-30-store-write-shapes.md). Whether the
collect is actually unblocked is not the store's to say — RESTRICT may still hold the
row, which is what "a probe, not a verdict" below means.

One bool rather than a result struct with `Removed`/`Remaining`/`DeletionPending`,
because the caller reads exactly one fact. It follows `DeletionRequestsCreate`'s
`changed` and needs no read the write did not already do: `getObjectRowScoped` loads
the finalizer list and `deletion_requested_at` together, and `removeFinalizer`
already reports whether the removal was real.

The client side becomes:

```go
func (c *controllerClientImpl[Status]) FinalizersDelete(ctx context.Context, id ObjectID, finalizer string) error {
	clearedLast, err := c.bh.store.FinalizersDelete(ctx, c.gk, id, finalizer)
	if err := c.wakeAfter(ctx, err); err != nil {
		return err
	}
	if clearedLast {
		c.bh.signalRequeueNow(ctx, ObjectRef{ID: id, Group: c.gk.Group, Kind: c.gk.Kind})
	}
	return nil
}
```

**Immediate, not throttled.** The gate fires at most once per object: there is no
`FinalizersAdd` verb, finalizers are set only at create, and a list only shrinks — so
the last clear is a once-ever transition and cancelling a pending alarm can never
become a repeat. Same reasoning as the delete request's `marked`.

**The push is a probe, not a verdict.** It gates on the finalizer block only.
`gcCollect` re-checks RESTRICT and may still decline; that block keeps the sweeper
behind it, which is route 2 and route 3.

## Where the push actually wins

The reconcile loop already runs `gcCollect` at the tail of every pass over an object
that was deletion-pending *at load*. So when a controller clears the last finalizer
during that object's own reconcile — the shape both `examples/cascade` controllers
use — the collect happens in the same pass and the push adds nothing.

What it buys is two shapes:

- a controller clearing the finalizer on a **different object of its own kind**
  (a sibling, a leader handing off), which nothing else re-dispatches. A clear
  inside `Within` is this same shape and not a separate one: the tail `gcCollect`
  runs in its own transaction over the controller's committed writes
  (`reconciler.go:129-132`), so a closed `Within` is already visible to it.
- the **delete request landing between the load and the clear**. That is the only
  reachable form of "not deleting at load" — if the object is not deletion-pending
  when the clear commits, `clearedLast` is false and nothing pushes. The pass read
  `deleting` as false, so it skips the tail `gcCollect` entirely. In process the
  delete request's own push already covers it; this push is what covers the request
  arriving from another process, which issues no push at all.

### The cost, and the cheaper fix

In the redundant case the cost is one dispatch of an id that is already gone: the
queue marks the in-flight id dirty (`workqueue.go:184`), `done` re-queues it
(`workqueue.go:342`), the loop's opening read returns `ErrNotFound`, and it logs
*"object gone before reconcile; skipping"*.

That case is not rare — it is the dominant shipped shape, the one both
`examples/cascade` controllers and `gc_test.go:74`/`gc_test.go:105` use. A cascade of
N children pays it N times, and each push also cancels that id's pending floor or
alarm on the way.

Fix it at the source rather than at the gate: **when the tail `gcCollect` reports
`gone`, drop the id's queued state before `done`.** The row is physically deleted and
ids are never reused, so any dirty bit set before or during that pass can only
resolve to `ErrNotFound`. `discard` is the near miss — it calls `done` then
`gate.Forget`, but leaves `dirty` set, and nothing outside a test helper calls it.

This is not the alternative rejected above ("a gate that knows which pass it is
running inside"): it needs no knowledge of the caller, only the collect's own result.
It stands alone and improves the shipped deletion push the same way, so it can land
as its own commit before or after the push. Taking it here, because this spec is what
makes the waste routine. It touches `reconciler.go`, which the rest of the steps do
not — see step 5.

## What must stay true

- **The sweeper is still the answer for the other two routes**, and for route 1 after
  a crash. `WithGCInterval` cannot be disabled, so every block keeps a tick behind
  it.
- **The push is an enqueue, never an inline collect.** It reaches `gcCollect` only
  through the controller's own loop.
- **The gate must be all three conditions.** A finalizer cleared on an object that is
  not deleting owes nothing, and pushing there would collect-probe every finalizer
  removal in the system.
- **The pull path stays pinned by a test that issues no push** — one that clears the
  finalizer through `bh.store.FinalizersDelete` rather than through the controller
  client, then reaches the collect. There is no knob that disables a push.

## Steps

1. **Store contract.** Change the `FinalizersDelete` signature in
   `internal/storeapi/storeapi.go`, doc the `clearedLast` bool, and implement it in
   `sqlite/store.go` — the `Within` body already has every fact; return `false` on
   the `!removed` early return. **`fakeStore.FinalizersDelete` keeps its
   `panic("not implemented: …")`** and only takes the new signature: no test drives
   the fake through this method, and inventing finalizer state in it that nothing
   reads is what the repo's fill-in-as-tests-need-them convention exists to avoid.
2. **Call sites.** Update the five `store.FinalizersDelete` calls in
   `sqlite/store_test.go` for the new return.
3. **Client.** Wire the push in `controller.go` as above.
4. **Audit the tests that clear finalizers.**
   - `gc_test.go:74`, `gc_test.go:105`, `reconciler_test.go:1853`,
     `reconciler_test.go:1925` clear from inside a reconcile of the object itself;
     they will now pass via the push where they may have been passing via the tail
     `gcCollect` or a tick. Keep them, and add the pull-path test rather than
     converting one — the shape `TestIntegrationDeleteCollectsWithoutThePush` took.
   - `controller_test.go:44` and `objectswatch_test.go:1150` clear on **live**
     objects, so `clearedLast` is false and neither changes. Named here because "no
     push on a live object" is a claim worth knowing is already covered incidentally;
     the new test below asserts it directly.
5. **Drop the queued state of a collected id** (independent of steps 1–4). The
   collect's `gone` has to reach the queue: `typedController.reconcile`
   (`reconciler.go:139`) knows it, `runWorker` (`reconciler.go:390`) calls `done`, and
   nothing connects them. Carry it out through the adapter's result and give
   `workQueue` a method that clears `dirty`, forgets the gate entry and ends
   processing — `discard` plus the `clearDirty` it is missing. It must publish the
   gauge report `clearDirty` returns, outside `q.mu`, as every other mutating path
   does; `done` publishes nothing today because it moves no schedule.
6. **Docs**, per "Done when" below: `reconcile-triggers.md` case 11, the `TODO.md`
   edge entry, `CLAUDE.md`'s `AfterCommit` user count, the specs README row and
   execution order, the ADR, and deleting this file.

## Tripwires

`TestCollectKeepsFinalizedObject` pins that a finalized object survives a collect.
`TestCollectDeletesOwnerAfterChildGone` and
`TestIntegrationGCDeleteDependencyUnblocksTarget` pin routes 2 and 3, which do not
change — both must still pass at the sweeper cadence.

`TestClientCreateRejectsFinalizersOnUnregisteredKind` no longer carries the routing
argument, but it still carries case 11's claim that every block is temporary.

## New tests

- The last finalizer cleared on a deletion-pending object collects it at commit,
  with the clear issued from a reconcile of a *different* object of the kind — the
  shape where the tail `gcCollect` cannot be what collected it.
- A finalizer cleared on a live object pushes nothing.
- A non-last finalizer cleared on a deletion-pending object pushes nothing.
- Clearing an absent finalizer pushes nothing (`clearedLast` false on no removal).
- **A rolled-back `Within` pushes nothing**, including the nested-savepoint unwind
  where the caller swallows the error. This is the property `AfterCommit` is supposed
  to give for free and nothing else on this path pins it.
- **An errored call pushes nothing** — `ErrWrongKind` on an id of another kind
  (`controller_test.go:878` has the fixture), which must return before the gate is
  even consulted.
- Pull path: clear through the store, collect through the sweeper.
- Store-level: `clearedLast` is true only for the last-finalizer-on-deleting-row case
  (`sqlite/store_test.go`, table-driven over the four combinations).
- For step 5: a collected id leaves the queue with no further dispatch, even when a
  wake arrived mid-pass — and the schedule watch sees it go.

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

## Done when

- Clearing the last finalizer on a deletion-pending object collects it at commit.
- Clearing a finalizer on a live object still pushes nothing.
- The pull path is still pinned by a test that reaches the collect without
  issuing a push — there is no knob that disables one.
- Case 11 in [`reconcile-triggers.md`](../reconcile-triggers.md) records which of the
  three routes pushes and which two wait for a tick.
- The [`TODO.md`](../TODO.md) edge-invisibility entry narrows: its "one consumer
  left" claim becomes the two edge-driven routes.
- **`CLAUDE.md`'s `Store.AfterCommit` user list reads seven, not six**, with the
  cleared-finalizer enqueue named alongside the delete-request enqueue it shares
  `signalRequeueNow` with. The count is a checked-in invariant; leaving it at six is
  a lie the next reader inherits.
- The [specs README](README.md) latency row is **narrowed, not dropped**: routes 2
  and 3 stay at ≤30s by this spec's own decision, so the Route cell becomes the two
  edge-driven routes and the Spec cell points at the `TODO.md` edge-invisibility
  entry — the same edit shape as that entry's own narrowing. Dropping the row would
  claim a latency nothing closed. Step 4 in its execution order is marked shipped
  with the ADR link.
- The rationale moves to an ADR and this spec is deleted.
