# A per-object cadence is armed by a pass and admitted by a startup scan

- **Status:** Accepted — implemented in `options.go`, `beehive.go`,
  `reconciler.go`.
- **Date:** 2026-08-19

## Context

A kind whose correctness rests on re-polling — a prober that dials a remote, a
controller watching something the store cannot see — had two ways to get a
periodic pass, and each gave up one of the two properties that matter.

`Result.RequeueAfter` schedules per object, measured from the end of that
object's own pass, so objects spread themselves out. But every return path has
to re-declare it, and a branch that forgets is silent: the object settles,
nothing is armed, and that object stops polling for the life of the process. The
branch an author forgot to arm is the branch they also forgot to assert.

`WithFullPassInterval` is declared once at registration, so no return path can
drop it. But it lists the whole kind on every tick and enqueues every object at
once, with no per-object phase and no jitter.

Nothing gave both: declared once, and spread per object.

## Decision

`WithIndividualPassInterval(d)` gives every object of the kind a pass roughly
every `d`, each object's next pass scheduled from the end of its own last one.

Two mechanisms, and both are needed:

- **The re-arm.** `scheduleNext`'s `default:` branch — a pass that settled and
  asked for nothing — arms that object's next pass. This is what makes the
  cadence unforgettable and self-spreading.
- **The admission scan.** A per-object alarm is armed *by* a pass, so an object
  no pass reaches never enters the cadence. `admit` lists the kind once at
  `Start` and arms everything it finds.

Between them the coverage is total: objects present at boot are admitted by the
scan, and objects created afterwards are admitted by the create's own commit
push. Under the sole-writer rule there is no third way for an object of a
registered kind to come into being.

**The guarantee is narrower than the name suggests, deliberately.** An object
whose pass returns settled *without scheduling anything* gets its next pass about
`d` later. A `RequeueAfter` is not clamped — longer or shorter, the controller's
own statement wins — a failure keeps its ladder, and a bare `Unsettled` keeps the
owed pass's cadence. `d` is a default cadence, not a ceiling; clamping would let
a blanket option silently override the specific statement a pass just made.

**Both armings are jittered upward.** The re-arm adds up to
`individualPassJitterFrac` (a tenth) so two objects that settled together drift
apart. The admission offset spreads across the *whole* interval, since a restart
would otherwise re-synchronize the kind. Neither is configurable — no caller
wants a synchronized herd — and the only seam is `withIndividualPassRand`, which
replaces the randomness rather than the fractions so one knob makes every
schedule in a test exact.

**No fourth `alarmKind`.** The re-arm uses `alarmRequeueAfter`. It is armed at
the same site with the same lifecycle as a controller's own `RequeueAfter`, and
the two can never contend, since the re-arm runs only when the result scheduled
nothing. The existing arbitration is already what this wants.

**The admission scan subsumes the startup full pass.** Both list the same kind
for the same reason, so with the individual pass on, the scan drops its offset
and `enqueueAll` is skipped. The startup pass thereby gains a retry ladder it did
not have: `enqueueFrom` warns and skips a failed listing, which is harmless when
a tick will come again and fatal for a scan that runs once.

## Consequences

- **A restart stretches one interval to about `2d`.** The last pass before the
  crash may have run at the start of its interval, and the scan adds up to
  another `d`. Narrowing the admission window to `w` would cut the wait to `w`
  and raise the peak dispatch rate to `N/w` — the same trade
  `WithStartupFullPass` already sits at the far end of (`w = 0`), which is why
  there is no second window knob.
- **Standing timers.** Every object of the kind holds an alarm, a `time.Timer`
  and a `gauge.alarms` entry for the life of the process, where the full pass
  holds none between ticks. Ordinary at 10⁴ objects, worth measuring at 10⁶. The
  replacement, if it ever bites, is a paged admission cursor — which bounds the
  timers and weakens the coverage from "by `d`" to "eventually".
- **A collected object costs one wasted dispatch.** One collected by the global
  sweeper rather than by its own pass still holds an alarm: it fires, dispatches,
  `GetForReconcile` reads `ErrNotFound`, and `forget` clears it. Prefer this
  option on kinds whose objects are long-lived.
- **The schedule watch sees both ends.** The scan publishes a schedule change per
  object at boot, and `gauge.finalValues` publishes one per object at stop.
- **A failed admission scan retries on its own ladder** (`admitRetryBase` to
  `admitRetryMax`), not on one of the public cadences. Nothing else would re-run
  it.
- **`!gone` is load-bearing.** Both collected paths return `Settled(), true`,
  which lands in the branch that arms; arming there resurrects an id `forget`
  just dropped, into a dispatch that can only read `ErrNotFound`.
