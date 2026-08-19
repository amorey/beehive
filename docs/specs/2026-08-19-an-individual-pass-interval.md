# An individual pass interval

- **Status:** Planned.
- **Date:** 2026-08-19
- **Issue:** [#122](https://github.com/amorey/beehive/issues/122)

## The gap

A kind whose correctness rests on re-polling — a prober that dials a remote
every few minutes — has two ways to get a periodic pass today, and each gives up
something.

`Result.RequeueAfter` schedules per object, measured from the end of that
object's own pass, so objects spread themselves out. But every return path has
to re-declare it, and a branch that forgets is silent: the object settles,
nothing is armed, and that object stops polling for the life of the process.

`WithFullPassInterval` is declared once at registration, so no return path can
drop it. But it is a ticker that lists the whole kind and enqueues every object
at once. There is no per-object phase and no jitter, so a populous kind gets a
synchronized burst every interval.

Nothing gives both properties: declared once, and spread per object.

## The decision

Add a per-controller option that gives **every object of the kind** a pass
roughly every `d`, with each object's next pass scheduled from the end of its own
last one.

```go
// WithIndividualPassInterval gives every object of the kind a pass roughly
// every d, each object's next pass scheduled from the end of its own last one.
// Default 0 (disabled).
func WithIndividualPassInterval(d time.Duration) Option
```

Two parts make it work, and both are needed:

1. **Re-arm after each pass.** When a pass returns settled and says nothing
   about when to come back, beehive arms that object's next pass `d` from now.
   This is the part that makes the cadence unforgettable and self-spreading.
2. **An admission scan at startup.** A per-object alarm has to be armed by
   something, and an object that never gets a pass never arms one. So at `Start`
   the reconciler lists its kind once and arms every object it finds. One
   listing per *process*, not per tick.

Between them the coverage is total. Objects that exist at boot are admitted by
the scan; objects created afterwards are admitted by the create's own commit
push (`signalCreated` → `signalSpecWritten` → `signalRequeueNow`). Under the
one-process, sole-writer rule there is no third way for an object of a
registered kind to come into being.

### The guarantee, stated exactly

> An object whose pass returns settled without scheduling anything gets its next
> pass about `d` later.

That is narrower than "no object goes longer than `d`", and deliberately so. A
pass that returns `RequeueAfter` sets its own schedule, longer or shorter, and
this option does not clamp it — a controller that knows there is nothing to do
for an hour should be able to say so. A failed pass keeps the backoff ladder.
An `Unsettled` pass keeps the owed-pass cadence, which is shorter anyway.

Document `d` as a default cadence, not a ceiling. If it were a ceiling, the
option would silently override the more specific statement the controller just
made.

### Why not just fix `WithFullPassInterval`

Jittering the full pass spreads the burst but keeps the cost: a `ListIDs` over
the whole kind on every tick, forever. This option pays one listing per process
and then rides the per-object alarm the work queue already keeps.

## Surface

### `options.go`

```go
// WithIndividualPassInterval gives every object of the kind a pass roughly
// every d, measured from the end of each object's own last pass. Default 0
// (disabled).
//
// Use it for a kind that must re-poll something the store cannot see — a remote
// dialled by the reconcile, a probe with no notifier behind it. Unlike a
// RequeueAfter the controller has to re-declare on every return path, this is
// declared once and cannot be dropped by a branch that forgot it; unlike
// WithFullPassInterval it costs no listing per tick and dispatches no
// whole-kind burst.
//
// What it schedules: a pass that returns settled without asking to be requeued.
// A pass that returns RequeueAfter sets its own schedule and is not clamped, a
// failed pass keeps its backoff ladder, and an unsettled one keeps the owed
// pass's cadence. So d is a default cadence, not a ceiling.
//
// Each arming carries jitter, so the interval is a floor plus up to a tenth: a
// pass never fires early, and objects that settled together drift apart. At
// startup the spread is the whole interval — an object's first pass after a
// restart lands within d, which is what keeps a restart from dispatching the
// whole kind at once. Pair it with WithStartupFullPass for a kind that needs
// that first pass promptly instead.
//
// Passed to New it sets the default for all controllers; passed to Register it
// overrides that default for one.
func WithIndividualPassInterval(d time.Duration) Option
```

Dispatches on `*Beehive` and `*reconciler`, like `WithFullPassInterval`. `d <= 0`
disables it; no `ErrInvalidOption`, since "off" is the default and is meaningful.

One unexported companion, so a test has a schedule it can assert on:

```go
// withIndividualPassRand replaces the source of the jitter fraction, which
// returns a value in [0,1). A test passes a constant or a counter so the
// schedule is exact; nothing else may set it.
func withIndividualPassRand(f func() float64) Option
```

It replaces the **randomness**, not the fractions — one seam covers both the
re-arm jitter and the admission offset, which is what keeps a test from having
to disable them separately. Dispatches on `*Beehive` and `*reconciler`, since
`newTestBeehive` passes its options to `New`.

### `beehive.go`

The option is inherited, so it needs a field, a default and a copy. All three,
or the `New` form compiles and silently does nothing.

```go
// beside defaultFullPassInterval
defaultIndividualPassInterval time.Duration = 0

// individualPassJitterFrac is the fraction of the interval added to each
// arming. Not configurable: no caller wants a synchronized herd.
individualPassJitterFrac = 0.1
```

On `Beehive`, beside `fullPassInterval`:

```go
individualPassInterval time.Duration
individualPassRand     func() float64 // seeded in New; a test overrides it
```

`New` seeds `individualPassInterval: defaultIndividualPassInterval` and
`individualPassRand: rand.Float64`, and `Register`'s struct literal copies both
into the reconciler beside `fullPassInterval`.

### `reconciler.go`

New fields, mirroring the two above:

```go
individualPassInterval time.Duration
individualPassRand     func() float64
```

The jitter helper. `individualPassRand` is nil on a reconciler built outside
`Register` — the minimal ones in tests — and a nil source means no jitter, which
is what such a test wants anyway:

```go
// jittered spreads an arming by up to individualPassJitterFrac of d, so two
// objects that settled together do not stay together. Never shortens d.
func (r *reconciler) jittered(d time.Duration) time.Duration {
	return d + time.Duration(r.randFrac()*individualPassJitterFrac*float64(d))
}

// randFrac returns the next jitter fraction in [0,1); zero when no source is
// set, which makes every schedule exact.
func (r *reconciler) randFrac() float64 {
	if r.individualPassRand == nil {
		return 0
	}
	return r.individualPassRand()
}
```

Use `math/rand/v2`'s top-level `rand.Float64` — this is the repo's first
`math/rand` import. It must stay a top-level function or an equivalent
goroutine-safe source: the scan goroutine and every worker call `randFrac`
concurrently, and a shared `*rand.Rand` would need a mutex.

Two changes to the reconcile loop:

**The re-arm.** In the post-pass switch (`reconciler.go:444-467`), the `default:`
branch — settled, nothing requeued — arms the alarm:

```go
default:
    r.backoffClear(id)
    if d := r.individualPassInterval; d > 0 && !gone {
        r.work.addAfter(id, r.jittered(d), alarmRequeueAfter)
    }
```

**The admission scan.** In `run`, when the option is set, one goroutine joins the
worker `WaitGroup`:

- List the kind once with `Objects().ListIDs`.
- Arm each id at `time.Duration(r.randFrac() * float64(d))` — an offset spread
  across the *whole* first interval, not a tenth of it, so the first passes are
  spread rather than fired as a wave.
- On a failed listing, retry on `driver.Backoff{Base: admitRetryBase, Max:
  admitRetryMax}` until it succeeds or `ctx` ends. Log each failure.
- If `startupFullPass` is also set, the offset is zero. An immediate `addAfter`
  is an immediate `add`, so the scan *is* the startup pass for this kind and
  `enqueueAll` is skipped — one listing instead of two.

New constants:

```go
// admitRetryBase/admitRetryMax pace the retry of a failed admission scan. Its
// own ladder, not one of the public cadences: with no periodic tick behind it,
// a scan that fails and is not retried means the kind never polls.
admitRetryBase = 100 * time.Millisecond
admitRetryMax  = 30 * time.Second
```

## Edge cases the implementer would otherwise guess at

- **No new `alarmKind`.** Use `alarmRequeueAfter`. The arming happens at the same
  site and with the same lifecycle as a controller's own `RequeueAfter`, the two
  can never contend (the re-arm runs only when the result set nothing), and the
  existing arbitration is already what this wants: a backoff takes the slot, a
  floor arbitrates by fire time, two controller schedules resolve newest-wins.
  Widen the constant's comment rather than adding a fourth kind.

- **Never arm for a collected object.** Both collected paths return
  `Settled(), true` (`reconciler.go:88-92`, `:176`), so they land in `default:`
  — the branch that arms. That path calls `work.forget(id)` to drop everything
  queued for the id, and an alarm armed after it resurrects a dispatch for a row
  that reads back `ErrNotFound` forever. The `!gone` guard is load-bearing.

- **An admission alarm can fire redundantly.** An add does not clear a
  non-absorbing alarm, so an object admitted at boot and then dispatched by the
  owed pass still has its admission alarm pending. It costs at most one extra
  pass: the re-arm at the end of that dispatch replaces it. Do not add machinery
  to prevent this.

- **A collected object costs one wasted dispatch.** An object collected by the
  global sweeper rather than by its own pass still holds a standing alarm. It
  fires, dispatches, `GetForReconcile` returns `ErrNotFound`, and `forget`
  clears it. One dispatch and one store read per collected object, bounded by
  `d` — not free on a high-churn kind, and the reason to reach for this option
  on kinds whose objects are long-lived.

- **A stopped queue absorbs a late arm.** `addAfterLocked` checks `q.stopped`, so
  a scan goroutine racing teardown is already safe. Join it to the worker
  `WaitGroup` anyway — `run`'s deferred teardown does `wg.Wait()` before
  `work.stop()` — so the ordering is deterministic rather than tolerated.

- **This is not `WithStartupFullPass` and does not imply it.** The scan defers an
  object's first pass by up to `d`; the startup pass exists to re-establish
  in-process state *promptly*. A kind that needs both enables both.

- **A restart stretches one interval to about `2d`.** The last pass before the
  crash may itself have run at the start of its interval, and the admission scan
  then adds up to another `d`. It is one doubled gap per process, not a stall,
  and it is the price of the spread: narrowing the admission window to `w` cuts
  the wait to `w` and raises the peak dispatch rate to `N/w`. That trade is the
  dial `WithStartupFullPass` already sits at the far end of (`w = 0`), so there
  is no second window knob. If the middle of the dial is ever wanted it starts
  unexported, like `withMinRequeueInterval`.

- **Folding in the startup pass changes its timing.** Today `enqueueAll` runs
  synchronously in `run` before the workers start (`reconciler.go:359`). Inside
  the scan goroutine it becomes concurrent with the first dispatches, and it
  gains a retry ladder it does not have today (`enqueueFrom` warns and skips).
  Both are fine — the startup pass's guarantee is *one reconcile per process*,
  not one before any other work — but say so where someone checking against
  [the startup-pass ADR](../adr/2026-08-07-the-startup-pass-may-be-depended-on.md)
  will find it.

- **Standing timers, for the life of the process.** Every object of the kind
  holds an alarm, a `time.Timer` and a `gauge.alarms` entry — hundreds of bytes
  each — where the full pass holds none between ticks. Ordinary at 10⁴ objects,
  worth measuring at 10⁶. If it bites, the replacement is a paged admission
  cursor: list a page per tick and arm it, which bounds the standing timers and
  weakens the coverage from "by `d`" to "eventually". Not now.

- **The schedule watch sees both ends.** Arming N objects publishes N schedule
  changes at boot, and `gauge.finalValues` publishes a final value for every id
  it describes — which, with this option on, is every object of the kind. A
  `WatchSchedule` consumer on a populous kind sees a burst at start *and* at
  stop.

## Tests

In `reconciler_test.go` unless noted, with `withIndividualPassRand(func() float64
{ return 0 })` so every schedule is exact.

- A settled pass that requeues nothing arms the next pass at `d`.
- A settled pass that returns `RequeueAfter(shorter)` uses the shorter delay.
- A settled pass that returns `RequeueAfter(longer)` uses the longer delay — the
  non-clamp, pinned so nobody "fixes" it later.
- A failed pass keeps its backoff delay; the option arms nothing.
- An `Unsettled` pass keeps the owed-pass cadence.
- A pass that reports `gone` arms nothing, and the queue holds no alarm for the
  id afterwards.
- A backoff replaces a pending individual-pass alarm, and the failing object
  keeps its ladder. At the reconcile site, not in `workqueue_test.go`: the
  arbitration itself is `alarmRequeueAfter`'s and is already covered — what is
  new is the re-arm feeding it.
- The cadence survives a return path that declares nothing — the whole point:
  reconcile N times, assert an alarm after each.
- An object created after `Start` is picked up by the create's push and is armed
  when its pass ends.
- Admission: objects settled before `Start`, with nothing owed, each get a pass.
  This is the case the option exists for and the one a re-arm alone misses.
- Admission spread: with a counter-based `withIndividualPassRand` returning
  `0, 0.25, 0.5, …`, the armed offsets are those fractions of `d` — the spread is
  pinned by construction, with no probabilistic assertion.
- Admission retries: a store whose `ListIDs` fails twice and then succeeds still
  admits every object.
- Admission with `WithStartupFullPass` on: every object is dispatched at once and
  `ListIDs` is called once, not twice.
- Jitter: with the rand source returning `1`, the arming is `d` plus a tenth —
  and never less than `d`.
- In `beehive_test.go`: a reconciler registered after
  `New(store, WithIndividualPassInterval(d))` inherits `d`, alongside the
  existing `fullPassInterval` inheritance assertion.
- In `options_test.go`: dispatch at `New`, at `Register`, on an unrelated target,
  and `d <= 0` leaving the feature off.

## On ship

- Fold this into an ADR and delete the spec.
- Add the option to the `CLAUDE.md` driver summary and to `README.md`.
- Add the row to [`docs/reconcile-triggers.md`](../reconcile-triggers.md) — and
  qualify the invariant it opens with. "Every push has a pull behind it" does not
  cover this trigger: it has no durable record, and its only re-derivation is the
  admission scan at `Start`. The honest statement is that the pull behind it runs
  once per process rather than on a cadence, which is sound only because the
  in-memory alarm cannot outlive the process that holds it. Say that in the row,
  not just the row's existence.
- Close [#122](https://github.com/amorey/beehive/issues/122).
