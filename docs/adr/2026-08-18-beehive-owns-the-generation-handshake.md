# Beehive writes observed_generation, from the object it handed to Reconcile

- **Status:** Accepted — implemented in `types.go`, `reconciler.go`,
  `controller.go`, `internal/storeapi/storeapi.go`, `sqlite/store.go`.
- **Date:** 2026-08-18

Supersedes the record on the generation handshake and content no-ops, whose live
parts are folded in below.

## Context

`Generation` increments on every real spec change and `ObservedGeneration`
records what the controller last settled. Who *writes* the second one was the
question. It used to be the controller, through an `observedGeneration` argument
on `UpdateStatus` and a `SetObservedGeneration` verb for a conditions-only pass.
Three things followed:

- Every kind hand-wrote the same branch — status changed, or not — and a kind
  that got it wrong never settled and sat in the owed listing forever. Silently.
- `UpdateStatus` did two unrelated things, so its contract had to say that
  identical bytes write no status *but* still advance the handshake, and that a
  stale generation is dropped on the no-op path *but* written verbatim on the
  content path. That second branch was the only way `observed_generation` could
  move backwards.
- Two error sentinels existed only because a caller could pass a nonsense number.

`Reconcile` also returned `(Result, error)`, and the worker branched on the error
and dropped the `Result` — so the returns were never independent, and a
controller could return a schedule beehive silently ignored.

## Decision

`Reconcile` returns one `ReconcileResult`, and beehive writes the handshake.

- `Settled(d)` — the pass observed the object's current generation, which
  beehive records. Not a claim of health, and no status write required.
- `Unsettled(d)` — a real pass over an object not caught up. Nothing recorded.
  `d == 0` re-dispatches at the work queue's per-object floor.
- `Fail(err)` — the backoff ladder, settling nothing.

The discriminant's zero names no kind, so the zero value is detectable;
`normalize` folds it and `Fail(nil)` into `Fail(ErrInvalidResult)` before
anything reads the result. Every gate then tests **positively** for the kinds it
admits — a negative gate is what makes the zero dangerous, since
`ReconcileResult{}` is not a `Fail` and `!isFail` would admit it and stamp a
generation no pass observed.

`UpdateStatus` loses the argument on both surfaces and
`ControllerClient.SetObservedGeneration` is gone, leaving
`Store.Objects().SetObservedGeneration` the sole handshake writer.

**The stamp is the generation the pass was handed**, read off the loaded row and
never re-read: stamping a mid-pass spec change would mark it seen by a pass that
never read it, and nothing would reconcile it again. The one part here that loses
data if done wrong, and silently —
`TestReconcileStampsTheGenerationItHandedOut` pins it.

**The stamp is gated in memory**, on the `observed_generation` already in hand.
`SetObservedGeneration` opens a `Within` and a scoped `SELECT` before it can
decide the write is a no-op, so an ungated stamp would cost a
BEGIN/SELECT/COMMIT per object per pass on the single connection. The gate
stands in for the store's clamp **only because `observed_generation` is
monotonic**, which holds only because `UpdateStatus`'s unclamped write is gone:
the two changes cannot be separated.

**Order after `Reconcile` returns**: end the pass client, decrement
`reconcile_owed`, write the dependency watermark, stamp, then GC. The stamp
follows the watermark because a crash between them must leave an *unsettled*
object with a low watermark, which only over-reports staleness — never a settled
object whose watermark never landed. It precedes GC because a deleting object can
be collected in the same pass, leaving the stamp writing to a row that is gone.

**A failed stamp is not a failed reconcile.** It warns and leaves the object
unsettled for the listing to re-derive — the same bargain the watermark write
makes. Two errors are silent: a cancelled context is shutdown, and `ErrNotFound`
is another kind's cascade collecting the row between the load and the stamp.

## Consequences

Kept from the superseded record: byte-identical status writes are still skipped,
which stops a controller re-applying its own spec from waking itself forever, and
the no-op gate is still the schema version as well as the bytes.

A pass settling a *new* generation now costs two write-log entries where it cost
one, waking the tailers and the dependency waker twice. Bounded by the same gate:
nothing else writes.

The handshake can no longer join a caller's `Within`. A conditions-only pass
commits its conditions and then its stamp; a crash between them costs one extra
reconcile, which is harmless and level-triggered.

`ErrObservedGenerationFuture` and `ErrInvalidObservedGeneration` leave the public
API for `internal/storeapi`, where tripping one is a beehive bug rather than a
caller's. `ErrInvalidResult` replaces them.

`ObservedAt` now moves only with the handshake, so it means exactly "when the
object settled at `ObservedGeneration`". Still not a liveness signal.
