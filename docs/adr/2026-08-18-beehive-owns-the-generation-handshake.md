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

- `Settled()` — the pass observed the object's current generation, which beehive
  records. Not a claim of health, and no status write required.
- `Unsettled()` — a real pass over an object not caught up. Nothing recorded.
- `Fail(err)` — the backoff ladder, settling nothing.

`RequeueAfter(d)` schedules the next pass, and `Err()` reads a failure back out.

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

### The delay is a method, and its zero means one thing

`Settled` and `Unsettled` took the delay positionally, where `0` was the
opposite instruction on each — nothing scheduled on one, a re-dispatch at the
work queue's floor (1s) on the other. The spelling that read as "no opinion
about scheduling" was, on `Unsettled`, a 1/s poll for as long as the condition
held, which is exactly where `Unsettled` is the right answer: a pass deferring on
something external. On a fleet waiting on one downed dependency, that is one
dispatch per object per second.

So the delay moved to `RequeueAfter(d)`, and an explicit zero means the same
thing on both kinds — dispatch as soon as the floor allows. `requeueSet` is what
separates that zero from a result with no opinion; the two are different
schedules, so the state is not derivable from the duration alone. `RequeueAfter`
never changes the kind, so `ReconcileResult{}.RequeueAfter(d)` is still the
detectable zero value.

**A bare `Unsettled()` schedules its own return**, at the owed pass's interval.
It has to schedule *something*: `Objects().ListUnsettledIDs` gates on
`observed_generation IS NULL OR observed_generation < generation`, so declining
to stamp does not un-settle a row, and an already-converged object — woken by a
dependency, say — that returns `Unsettled()` is in no listing and no other driver
would come back for it. It follows `WithOwedPassInterval` rather than a constant
of its own because it *is* that pass, extended to the objects whose generation
its listing cannot see; `defaultUnsettledRequeue` stands in only when the pass is
disabled.

That interval is an upper bound, not a period. A pending floor alarm outranks it,
and `alarmRequeueAfter` does not absorb an arriving add, so a wake landing inside
the window dispatches on the floor's schedule —
`TestReconcilerBareUnsettledYieldsToAPush` pins it. The alarm belongs to the pass
that set it and nothing on the success path cancels one, so an object enqueued by
the owed pass inside the window and then settling still takes one more pass when
the alarm fires. Idempotent, and pre-existing for any `RequeueAfter`.

`RequeueAfter` on a `Fail` is ignored: the switch tests `!succeeded()` first and
the ladder owns the retry.

### Err, and why there is no Unwrap

`ReconcileResult.err` was unreachable outside the package, so a failure
assertion downstream degraded to equality against the `Fail` the controller
built — which pins the wrapping text into the test — and `errors.Is` on a
sentinel was impossible for a controller wrapping another.

`Err()` normalizes first, so a caller-side `Fail(nil)` or zero value reports the
`ErrInvalidResult` beehive would record rather than a nil.

**No `Unwrap`.** `errors.Is`/`errors.As` take an `error` and consult `Unwrap`
only while walking a chain they were handed, so one here is unreachable unless
`ReconcileResult` implements `error` — which would make `Settled()` a non-nil
`error`. The assertion is `require.ErrorIs(t, res.Err(), sentinel)`.

There is no `String` and no exported discriminant. Equality against `Settled()`
is how a caller asserts the kind, and it prints the unexported `kind` as a
number; that is a failure message rather than a capability, and an exported
discriminant is the honest fix if it ever bites.

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

An object returning a bare `Unsettled()` whose generation really did move now
carries two unsynchronised schedules at the owed pass's rate — the listing and
its own alarm — so it costs roughly two dispatches per interval rather than one.
Against `Unsettled(0)`'s 1/s at the default that is still a ~15× reduction.
