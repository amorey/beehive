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
  to wake each child kind's watch tailer. The same loop can collect the push set —
  but not queue a hook per child, which is the thing that dedup exists to avoid.
- `bh.enqueuerForPage` is already the resolve-once-per-kind enqueuer for exactly this
  shape: many ids across a few kinds, one `bh.mu` take per kind, `nil` cached for a
  client-only kind.
- `signalRequeueNow` and `signalRequeueThrottled` (`beehive.go`) already **are** the
  enqueue arm: each queues an `AfterCommit` hook that resolves `reconcilerFor(gk)`
  and does nothing when the kind is client-only. Their body is `deletionAdvance`'s
  registered arm, minus the `gcCollect` fallthrough. So the decided option below
  needs no new helper.

## Proposal

Two pushes, both on `Store.AfterCommit`, both resolving the reconciler inside the
hook — which is what confines them to registered kinds.

1. **The delete request.** `Delete` and `DeleteByName` push the marked object through
   `signalRequeueNow`, gated on `marked`. An unchanged idempotent retry pushes
   nothing, which matches the watch wake beside it.
2. **The cascade.** `gcCollect` pushes the children it marked, in one hook over
   `enqueuerForPage`, alongside the per-kind wake it already sends. A cascade then
   advances one level per commit instead of one level per sweep.

Both gate on the same thing: **an object is pushed only when this call marked it.**
Step 2's gate does not exist in the store yet, and it plus `DeleteByName`'s id are the
only new surface here.

### Step 0 — two store widenings

Neither is optional; both steps below need an identity the store already has in hand
and currently drops. Each ripples to three places: the `storeapi` interface, the
`sqlite` implementation, and the `fakeStore` double in `testutils_test.go`.

**`DeletionRequestsCreateByName` returns the marked id.** `markForDeletion` already
scans `id, "group", kind, resource_version` off the row it marks (its `RETURNING`
clause feeds the write-log entry), so the id only needs threading back out through
`requestDeletion`. Return `(ObjectID, bool, error)` — an `ObjectID`, not an
`ObjectRef`: the caller holds `c.gk`, and a write returns only what a caller reads.

**`DeletionRequestsCreateFromOwner` reports which children it marked.**
`deletionRequestsCreateFromOwner` already computes `ch.deleting` per child and uses it
to skip the stamp, then throws it away, returning every owned child either way.
Return a purpose-shaped element instead:

```go
// DeletionCascadeChild is one owned child of a cascade. Marked reports whether
// this call stamped it, so a caller can push exactly the children that moved.
type DeletionCascadeChild struct {
    Marked bool
    Ref    ObjectRef
}
```

Do **not** widen `ObjectRef` with the bool. It is a public type alias (`types.go`)
used at every requeue site, where "marked" is meaningless. `EdgesAddResult` is the
precedent: a small result type carrying exactly what the caller follows up on, all of
it falling out of work the write already does.

### Step 1 — `Delete` and `DeleteByName` (`client.go`)

Inside the existing `if marked` block, beside `signalKindWritten`:

```go
if marked {
    c.bh.signalKindWritten(ctx, c.gk)
    c.bh.signalRequeueNow(ctx, ObjectRef{ID: id, Group: c.gk.Group, Kind: c.gk.Kind})
}
```

`Now`, not `Throttled`, for the same reason `Create` is: `marked` is true exactly once
per object, so this cannot repeat on a pass, and a delete carries new information — it
should beat a backoff alarm left by a failing reconcile rather than be absorbed by
one. The cascade takes the throttled path instead (Step 2), where the gate makes the
choice non-load-bearing and the weaker one is the safer default.

`DeleteByName` gets its id from the Step 0 widening. Leaving it pull-only was the
alternative and is rejected: a name delete is the common call in the examples, and it
would re-create the asymmetry one level down.

Replace `Delete`'s *"Nothing is scheduled"* comment with the routing rule, and record
it on the `Client.Delete` godoc: a registered kind reaches its controller at commit,
a client-only kind waits for the sweeper.

### Step 2 — the cascade (`gc.go`)

Two constraints, and the obvious loop violates both.

**Push only the newly-marked children.** Ungated, this fires at *reconcile* rate, not
sweep rate: `gcCollect` runs from `typedController.reconcile` after every pass over a
deleting object, so an owner blocked on a finalizer and retrying under backoff would
re-push its entire child set on every one of those passes. Today that loop pushes
nothing. The Step 0 `Marked` bool is the gate, and it makes the cascade push
structurally identical to Step 1's: exactly once per child per cascade.

**One hook for the whole cascade, not one per child.** The existing per-kind dedup is
there because "a wide cascade would otherwise queue one commit hook per row"; a
`signalRequeue*` call inside the per-child loop reintroduces exactly that, one closure
per row each re-taking `bh.mu` in `reconcilerFor` at flush time. `bh.enqueuerForPage`
is already the resolve-once-per-kind cache for this, so the push is one hook over a
collected slice:

```go
var pushed []ObjectRef
for _, ch := range children {
    if gk := ch.Ref.GroupKind(); !woken[gk] {   // the existing per-kind wake, unchanged
        woken[gk] = true
        bh.signalKindWritten(ctx, gk)
    }
    if ch.Marked {
        pushed = append(pushed, ch.Ref)
    }
}
if len(pushed) > 0 {
    bh.store.AfterCommit(ctx, func(context.Context) {
        enqueue := bh.enqueuerForPage()
        for _, ref := range pushed {
            enqueue(ref.GroupKind(), ref.ID)
        }
    })
}
```

It stays inside the `Within`, so a rollback discards the pushes with the marks.
`enqueuerForPage` enqueues through the ordinary wake path, so a pending alarm absorbs
it — the throttled semantics, reached without the per-child helper.

The wake stays ungated on `Marked`, unchanged: re-waking a kind whose children were
already marked is a spurious wake that costs one position read, and narrowing it is a
separate change with its own tripwires.

### Step 3 — docs

In [`reconcile-triggers.md`](../reconcile-triggers.md):

- §1 *"There are exactly two push paths that cause a reconcile"* — becomes four.
- §1's push table gains both rows.
- §1's *"A delete does not push"* becomes a delete's push and its one exclusion, the
  client-only kind.
- §C's preamble, *"**Push:** none. A delete does not schedule a collect."*
- Cases 9 and 10 list a push and name the gate (`marked` / `Marked`).

In code:

- `gcCollect`'s godoc opens *"No reconcile is woken: every row it touches is
  deletion-pending, and the sweeper's next tick finds it."* Step 2 falsifies that
  sentence; it is the one comment in the tree that has to change with this code.
- `Client.Delete`'s godoc and `clientImpl.Delete`'s *"Nothing is scheduled"* comment
  (Step 1).
- `TestIntegrationGCSweepCollectsStandaloneClientOnlyDelete`'s comment already reads
  *"The delete path itself deliberately only requeues — for a client-only kind that is
  a no-op"*, which describes the world **after** this ships, not today's. Confirm it
  rather than rewrite it.

Delete the [README](README.md) table's two case-02 rows and this file when it ships;
fold the rationale into an ADR only if the client-only exclusion needs one.

## What must stay true

- **The sweeper is unchanged and stays the guarantee.** `WithGCInterval` rejects a
  non-positive value, so every push has a tick behind it. `deletionAdvance` is
  already safe to repeat, and both arms are idempotent.
- **The routing is correctness, not speed.** `gcCollect` cannot clear a finalizer, so
  pushing a registered kind straight into `gcCollect` would never make progress.
- **No commit hook calls `deletionAdvance`.** Its client-only arm *is* `gcCollect`,
  which is the hazard below. The reconciler lookup inside `signalRequeue*` and
  `enqueuerForPage` is the routing; that is the whole of it.
- **A push fires once per object per mark.** Both gates are the store reporting what
  it changed, never "the caller called `Delete`" and never "the row is
  deletion-pending" — the second re-derives the spec-write push's rejected gate and
  fires at reconcile rate, since `gcCollect` runs after every pass over a deleting
  object.

## The one real hazard

**An `AfterCommit` hook runs synchronously, on the committer's goroutine, after the
outer commit.** `Within` flushes its hooks outside any lock and a hook may write to
the store and re-enter `Within`.

For a registered kind that is fine: the hook enqueues and returns.

For a **client-only** kind the collect arm calls `gcCollect`, which opens a
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
which is not a cost a caller can predict from the call.

## Tripwires

`TestIntegrationGCResumesDanglingDeleteOnStartup`,
`TestGCSweepDispatchesRegisteredKind` and `TestIntegrationGCSweepsClientOnlyKind` pin
the pull path. All three must still pass with the push disabled — that is the "every
push has a pull behind it" test for this spec.

`TestIntegrationGCCascadeDeletesOwnerAndChild` and
`TestCollectCascadesAndBlocksOnChild` pin the cascade. Neither asserts how many
sweeps a level costs, so neither blocks the change.

None of the three is defeated by Step 1: `TestGCSweepDispatchesRegisteredKind` marks
through the store's `DeletionRequestsCreate` rather than `client.Delete`, so no push
is issued; `TestIntegrationGCSweepsClientOnlyKind` pushes the owner but still turns on
the client-only child; `TestIntegrationGCSweepCollectsStandaloneClientOnlyDelete` is
unaffected because the client-only push is a no-op.

**One tripwire is weakened rather than broken.** `TestIntegrationDeleteTriggersReconcile`
is case 9's test, and after Step 1 it passes via the push — it stops pinning the
sweeper. Case 9 then has no pull-path test unless one is added (see below).

## Tests to add

In `client_test.go` and `gc_test.go`, mirroring the source files:

- `TestDeleteEnqueuesItsOwnObject` — a delete on a registered kind dispatches with
  the GC interval set far beyond the test's failsafe.
- `TestDeleteByNameEnqueuesItsOwnObject` — the same through the name sibling.
- `TestRepeatedDeleteEnqueuesOnce` — a second `Delete` returns `marked == false` and
  pushes nothing. This is the gate that stops a retry loop re-arming the object.
- `TestDeleteOnClientOnlyKindPushesNothing` — the excluded arm, asserted as a
  non-event: no dispatch, and the sweeper still collects it.
- `TestCascadePushesEachMarkedChild` — a two-level registered tree collects in one
  sweep's worth of commits.
- `TestCascadeSkipsClientOnlyChild` — the excluded arm of the cascade, kept separate:
  it is a different claim from the one above and shares no assertion with it.
- `TestCascadePushesOnlyNewlyMarkedChildren` — a second `gcCollect` over an
  already-marked subtree pushes nothing. This is the reconcile-rate regression, and
  it is the most important test in this list. Arrange a **pending backoff alarm** on
  a child and assert the re-cascade does not disturb it — not a re-enqueue floor,
  which `newWorkQueue` builds as `rategate.New[ObjectID](0)` and is therefore off
  unless a test calls `setFloor`.
- `TestIntegrationDeleteCollectsWithoutThePush` — case 9's replacement pull-path
  test, marking through the store rather than `client.Delete`, as
  `TestGCSweepDispatchesRegisteredKind` already does.

## Done when

- `Delete` on a registered kind reaches the controller at commit, not at the next
  tick.
- A cascade collects in D commits per **contiguous run of registered levels**, rather
  than D sweeps. A client-only level still costs a full sweep, and the pushes below it
  cannot be issued until that level's `gcCollect` runs — option 1 buys nothing for a
  mixed tree at the client-only levels, by construction.
- A re-cascade over an already-marked subtree pushes nothing.
- With the push paths disabled, every test above still passes.
- Case 9 and case 10 in [`reconcile-triggers.md`](../reconcile-triggers.md) list a
  push, and section 1's *"A delete does not push"* is corrected.
