# RequeueAfter and Err on ReconcileResult

- **Status:** Proposed.
- **Date:** 2026-08-18 (against `90b2ab6`)
- **Closes:** [#116](https://github.com/amorey/beehive/issues/116) (the two
  zeros schedule opposite things), [#114](https://github.com/amorey/beehive/issues/114)
  (a failed result's error is unreadable).

## Problem

### The delay is positional, and its zero means two different things

`Settled(d)` and `Unsettled(d)` take the same argument and read identically at
the call site, but `0` is the opposite instruction on each (`reconciler.go:443`):

```go
return beehive.Settled(0)    // nothing scheduled
return beehive.Unsettled(0)  // re-dispatched at the queue's per-object floor — 1s
```

So the spelling that reads as "no opinion about scheduling" is, on one of the
two constructors, a 1/s poll for as long as the condition holds. The hazard
lands exactly where `Unsettled` is the right answer: a pass deferring on
something external ("the config has not been read yet") is genuinely unsettled,
and the obvious spelling of "come back when you can" is the zero. On a fleet
that is one dispatch per object per second while a shared dependency is down —
and the whole fleet is waiting on it at once.

### A failed pass carries an error nothing outside the package can read

`ReconcileResult.err` is unexported (`types.go:245`) with no accessor. beehive
reads it (`reconciler.go:129`, `:172`, `:437`); a caller cannot. Every failure
assertion downstream degrades to equality against the `Fail` the controller
built, which pins the wrapping text into the test:

```go
assert.Equal(t, beehive.Fail(fmt.Errorf("update cluster status: %w", boom)), res)
```

`errors.Is` on a sentinel — `ErrNotFound`, `ErrInvalidResult`, a caller's own —
is unreachable outside the package, which also blocks a controller that wraps
another (a retry or telemetry decorator, a fan-out joining several passes).

## Goals

- The omitted delay means the same thing on `Settled` and on `Unsettled`.
- Neither of today's two schedules becomes unspellable, and neither is the one
  you get by writing the shortest thing.
- A failed result's error is readable, without `ReconcileResult` becoming an
  `error`.

## Non-goals

- Changing what any schedule *does* once chosen: the floor, the backoff ladder
  and the owed pass are untouched.
- A durable form of a result's requeue delay. Still case 13 in
  [`reconcile-triggers.md`](../reconcile-triggers.md), still declined.
- A `String` method. With no argument on the constructors,
  `assert.Equal(t, Settled(), res)` is the only way a caller can assert the
  kind, and it prints `kind:0x2`. That is a failure message, not a capability,
  and the honest fix if it bites is an exported discriminant — a decision to
  make on evidence. Neither issue needs it.
- Making the new unsettled default configurable. `RequeueAfter` is the
  per-call override, and there is no measurement behind a knob.

## Design

### The surface

```go
func Settled() ReconcileResult
func Unsettled() ReconcileResult
func Fail(err error) ReconcileResult

func (r ReconcileResult) RequeueAfter(d time.Duration) ReconcileResult
func (r ReconcileResult) Err() error
```

`RequeueAfter` returns a copy with the delay set; it never changes the kind, so
`ReconcileResult{}.RequeueAfter(d)` stays the zero value and still fails the
pass with `ErrInvalidResult`.

### What each form schedules

| written | next dispatch |
| --- | --- |
| `Settled()` | nothing scheduled |
| `Unsettled()` | after `defaultUnsettledRequeue` (30s) |
| `Settled().RequeueAfter(d)`, `Unsettled().RequeueAfter(d)`, `d > 0` | after `d` |
| `Settled().RequeueAfter(0)`, `Unsettled().RequeueAfter(0)` | as soon as the queue's per-object floor allows |
| `Fail(err)` | the backoff ladder |

The zero now means one thing on both kinds, and it means it at a call site that
says `RequeueAfter` out loud — which is the whole of #116. Today's
`Unsettled(0)` is still available, spelled `Unsettled().RequeueAfter(0)`; the
form a controller reaches for by default no longer polls at 1/s.

A negative `d` is the same as `0`.

### Why bare `Unsettled()` schedules a delay rather than nothing

The tempting answer is that an unsettled object needs no timer because the owed
pass converges it. That is only true when the generation actually moved.
`Objects().ListUnsettledIDs` gates on `observed_generation IS NULL OR
observed_generation < generation`, so *declining to stamp does not un-settle a
row* — case 13b in [`reconcile-triggers.md`](../reconcile-triggers.md) already
records this. An already-converged object woken by a dependency, returning a
bare `Unsettled()`, would be listed by nothing and dispatched by nothing. That
is a strand, and worse than the spin it replaced.

So `Unsettled()` always schedules. 30s is the owed pass's cadence
(`defaultOwedPassInterval`), which is the rate the *durable* half of this
population already converges at, so the new default is not a new pace — it is
the existing one, extended to the case the listing misses. It gets a constant of
its own (`defaultUnsettledRequeue`) rather than reading the option, per the
house rule that a ladder is capped on its own constant.

For an object whose generation really did move, the two schedules coincide in
rate but not in phase: the owed pass lists it every 30s *and* its alarm fires
every 30s, so it costs roughly two dispatches per 30s rather than one. Against
`Unsettled(0)`'s 1/s that is still a ~15× reduction, and the object that gains
the schedule outright — already converged, listed by nothing — is the one this
exists for.

### What the 30s default does not promise

Two properties of the queue the schedule table above should not be read past.
Neither is new, but this change puts both on the default path.

**A pending floor alarm wins.** `alarm.outranks` keeps whichever alarm fires
sooner when either side is `alarmFloor` (`workqueue.go:79`), so an object that
was floor-held when its pass returned dispatches at the floor and the 30s
schedule is dropped. That is the floor's rule working — a held wake never
delays work already scheduled sooner — and it makes the default an upper bound,
not a period.

**A push inside the window is not delayed.** `alarmRequeueAfter` returns false
from `absorbsAdd` (`workqueue.go:68`), so a dependency wake or a spec write
landing inside the 30s dispatches on its own schedule; only the floor paces it.
The new default cannot add latency to a pushed pass.

**A stale alarm can outlive its reason.** The alarm belongs to the pass that set
it, and nothing on the success path cancels one. An *immediate* push clears it
(`workQueue.requeueNow`, `workqueue.go:293`), which covers every
`signalRequeue*Now` caller — but a throttled push and the periodic passes go
through `work.add` (`reconciler.go:212`), which leaves it pending. So an object
that returns `Unsettled()`, gets enqueued by the owed pass inside the window and
then returns `Settled()` still takes one more pass when the alarm fires.
Reconciles are idempotent, so the cost is one spurious pass, and it is
pre-existing for `Settled(d)`; what changes is that every bare `Unsettled()` now
has an alarm.

### `Fail(err).RequeueAfter(d)`

Compiles, and is ignored: `runWorker` checks `!result.succeeded()` first and
takes the backoff ladder. Say so in `RequeueAfter`'s doc comment. Rejecting the
combination in `normalize` was considered and dropped — it turns a controller
building a result through a helper into a failed pass over a field nothing
reads, which is a worse trade than one documented no-op.

### `Err`, and no `Unwrap`

```go
// Err returns the error a failed pass carries, or nil for a successful one.
// The zero value and Fail(nil) report ErrInvalidResult, the same failure
// beehive records for them.
func (r ReconcileResult) Err() error { return r.normalize().err }
```

Reading through `normalize` is what makes `Err` total: a caller-side helper
handed a `Fail(nil)` sees what beehive will see, not nil.

**No `Unwrap`.** `errors.Is`/`errors.As` take an `error` and only consult
`Unwrap` while walking a chain they were already handed, so an `Unwrap` here is
unreachable unless `ReconcileResult` itself implements `error` — which would
make `Settled()` a non-nil `error` and read backwards at every `if err != nil`.
The assertion is `require.ErrorIs(t, res.Err(), boom)`.

## Implementation

`types.go`

- Add `requeueSet bool` beside `requeueAfter`. It is what separates "no
  opinion" from `RequeueAfter(0)`, and the reason the delay cannot stay a bare
  duration.
- Drop the parameter from `Settled`/`Unsettled`; add `RequeueAfter` and `Err`.
  `normalize`, `settles`, `succeeded`, `unsettled` are unchanged.

`beehive.go`, beside `defaultMinRequeueInterval` (`:65`)

```go
// defaultUnsettledRequeue is when a bare Unsettled() comes back. A bare result
// must schedule something: the unsettled listing gates on the generation, so an
// already-converged object that declines to settle is in no listing.
defaultUnsettledRequeue = 30 * time.Second
```

`reconciler.go:436`, the schedule switch — the two new arms are the middle
pair:

```go
case !result.succeeded():
	// unchanged: the backoff ladder
case result.requeueSet && result.requeueAfter > 0:
	r.backoffClear(id)
	r.work.addAfter(id, result.requeueAfter, alarmRequeueAfter)
case result.requeueSet:
	// RequeueAfter(0): the queue's per-object floor paces it.
	r.backoffClear(id)
	r.work.add(id)
case result.unsettled():
	r.backoffClear(id)
	r.work.addAfter(id, defaultUnsettledRequeue, alarmRequeueAfter)
default:
	r.backoffClear(id)
```

`backoffClear` is on every successful arm, as it is today — a success that
skipped it would leave the ladder in `backoffFor` and resume a later failure at
doubled backoff.

The internal returns at `reconciler.go:91`, `:114`, `:116` and `:178` become
`Settled()`.

## Migration

Mechanical, and the compiler finds all of it — the arity change means no call
site compiles unchanged.

| from | to |
| --- | --- |
| `Settled(0)` | `Settled()` |
| `Settled(d)` | `Settled().RequeueAfter(d)` |
| `Unsettled(d)`, `d > 0` | `Unsettled().RequeueAfter(d)` |
| `Unsettled(0)` | `Unsettled().RequeueAfter(0)` *(behaviour preserved)* or `Unsettled()` *(1s → 30s)* |

The last row is the only judgement call. In this repo every `Unsettled(0)` is a
test asserting the floor path, so all of them take the explicit form; the
examples pass non-zero delays already.

Call sites, in descending weight and without counts — the compiler enumerates
them, and a number here would only be a checklist that drifts:
`reconciler_test.go`, `gc_test.go`, `examples/cascade`, `controller_test.go`,
`reconciler.go`, `types_test.go`, `examples/conditions`, then
`examples/{greeting,events,lowpower}`, `waker_test.go`, `testutils_test.go`,
`client_test.go`, `beehive_test.go`.

## Tests

- `TestReconcileResultConstructors` (`types_test.go:167`) — extend for
  `RequeueAfter` setting the delay without touching the kind, and for
  `RequeueAfter(0)` differing from an untouched result.
- `TestReconcileResultNormalizeRejectsUnusableValues` (`:193`) — add
  `ReconcileResult{}.RequeueAfter(time.Minute)`, which must still normalize to
  `ErrInvalidResult`.
- New `TestReconcileResultErrReportsTheFailure` — `Err()` nil on both
  successes, the wrapped error on a `Fail`, `ErrInvalidResult` on `Fail(nil)`
  and on the zero value, and `errors.Is(res.Err(), sentinel)` through a wrap.
- `TestReconcilerSchedulesFromTheResultKind` (`reconciler_test.go:3570`) — the
  table gains a bare-`Unsettled` row landing on `defaultUnsettledRequeue` and a
  `RequeueAfter(0)` row landing on the floor path.
- `TestReconcilerRequeueAfter` (`:261`) — retarget to the chained spelling.
- New `TestReconcilerBareUnsettledYieldsToAPush` — a push inside the 30s window
  dispatches at the floor rather than waiting the alarm out. This is what the
  default rests on: it is an upper bound, not a period.
- New `TestReconcilerBareUnsettledSchedulesItself` — an already-converged object
  (stamped `observed_generation`) returning a bare `Unsettled()` is dispatched
  again, which is the strand this design exists to avoid.

## Docs

- `README.md:332` (the signature block), `:339` (the schedule table), `:343`
  (the `Unsettled(0)` warning, now describing `RequeueAfter(0)`), `:345`, and
  the four snippets at `:49`, `:54`, `:553`, `:568`.
- `docs/reconcile-triggers.md` — case 13 (`:714`) and case 13b (`:728`). 13b
  stops being about the zero and becomes about `RequeueAfter(0)`; its
  observation about the unsettled listing is now load-bearing for the bare
  default and should say so.
- `CLAUDE.md` — the generation-handshake bullet names `Settled`/`Unsettled`/`Fail`.
- On ship: fold into
  [the generation-handshake ADR](../adr/2026-08-18-beehive-owns-the-generation-handshake.md)
  rather than opening a new one — it already owns what a result means — and
  delete this file.
