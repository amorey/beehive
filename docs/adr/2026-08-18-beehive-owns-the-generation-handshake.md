# Beehive writes observed_generation, from the object it handed to Reconcile

- **Status:** Accepted — implemented in `types.go`, `reconciler.go`,
  `controller.go`, `internal/storeapi/storeapi.go`, `sqlite/store.go`.
- **Date:** 2026-08-18

Supersedes the earlier record on the generation handshake and content no-ops,
whose live parts are folded in below.

## Context

`Generation` increments on every real spec change and `ObservedGeneration`
records what the controller last settled. Who *writes* the second one was the
question.

It used to be the controller, through an `observedGeneration` argument on
`UpdateStatus` and a `SetObservedGeneration` verb for a pass whose whole report
is conditions. Three things followed:

- Every kind hand-wrote the same branch — decide whether the status changed, then
  call one verb or the other — and a kind that got it wrong never settled and sat
  in the owed listing forever, re-queued every interval. The failure was silent.
- `UpdateStatus` did two unrelated things, so its contract had to explain that
  identical bytes write no status *but* still advance the handshake, and that a
  stale generation is dropped on the no-op path *but* written verbatim on the
  content path. That second branch was the only way `observed_generation` could
  move backwards.
- Two error sentinels existed only because a caller could pass a number that
  makes no sense.

`Reconcile` also returned `(Result, error)`, and the worker branched on the error
first and dropped the `Result` — so the two returns were never independent, and a
controller could return a schedule beehive silently ignored.

## Decision

`Reconcile` returns one `ReconcileResult`, built by `Settled`, `Unsettled` or
`Fail`, and beehive writes the handshake.

- `Settled(d)` — the pass observed the object's current generation. Beehive
  records it. Not a claim of health, and it does not require a status write.
- `Unsettled(d)` — a real pass over an object that is not caught up. Nothing
  recorded. `d == 0` re-dispatches at the work queue's per-object floor.
- `Fail(err)` — the backoff ladder, settling nothing.

`ReconcileResult`'s discriminant has a zero that names no kind, so the zero value
is detectable; `normalize` folds it and `Fail(nil)` into `Fail(ErrInvalidResult)`
before anything reads the result. Every downstream gate then tests **positively**
for the kinds it admits. A negative gate is what makes the zero value dangerous:
`ReconcileResult{}` is not a `Fail`, so `!isFail` would admit it and stamp a
generation no pass observed.

`UpdateStatus` loses the argument on both the client and the store, and
`ControllerClient.SetObservedGeneration` is gone.
`Store.Objects().SetObservedGeneration` stays as the sole handshake writer.

**The stamp is the generation the pass was handed**, read off the loaded row and
never re-read. A spec change landing mid-pass must stay unobserved: stamping the
newer generation would mark it seen by a pass that never read it, and nothing
would reconcile it again. This is the one part that loses data if done wrong, and
it is silent — `TestReconcileStampsTheGenerationItHandedOut` is what pins it.

**The stamp is gated in memory**, on the `observed_generation` already in hand
from `GetForReconcile`. `Objects().SetObservedGeneration` opens a `Within` and a
scoped `SELECT` before it can decide the write is a no-op, so an ungated stamp
would cost a BEGIN/SELECT/COMMIT per object per pass on the store's single
connection. The gate is equivalent to the store's own clamp **only because
`observed_generation` is monotonic**, which holds only because `UpdateStatus`'s
unclamped write is gone. The two changes cannot be separated.

**Order after `Reconcile` returns**: end the pass client, decrement
`reconcile_owed`, write the dependency watermark, stamp, then GC. The stamp goes
after the watermark because a crash between them must leave an *unsettled* object
with a low watermark — which only over-reports staleness — never a settled object
whose watermark never landed. It goes before GC because a deleting object can be
collected in the same pass, and a stamp afterwards would write to a row that is
gone.

**A failed stamp is not a failed reconcile.** It warns and leaves the object
unsettled, and the unsettled listing re-derives it — the same bargain the
watermark write above it already makes. Two errors are silent: a cancelled
context is shutdown, and `ErrNotFound` is another kind's cascade collecting the
row between the load and the stamp.

## Consequences

Kept from the superseded record: byte-identical status writes are still skipped,
which is what stops a controller re-applying its own spec from waking itself
forever, and the no-op gate is still the schema version as well as the bytes.

A settling status write now costs two write-log entries where it cost one — the
status write and the stamp — so it wakes the tailers and the dependency waker
twice. Bounded by the same gate: only a pass that settles a *new* generation
writes at all.

The handshake can no longer join a caller's `Within`, since it commits after
`Reconcile` returns. A conditions-only pass commits its conditions and then its
stamp; a crash between them costs one extra reconcile, which is harmless and
level-triggered.

`ErrObservedGenerationFuture` and `ErrInvalidObservedGeneration` leave the public
API and stay in `internal/storeapi`, where tripping one is a beehive bug rather
than a caller's. `ErrInvalidResult` replaces them.

`ObservedAt` now moves only with the handshake, so it means exactly "when the
object settled at `ObservedGeneration`". It is still not a liveness signal.
