# Spec: a new dependency edge enqueues its source at commit time

Status: draft, and **the next landing in the plan**. It is the second
severable piece of phase 2 of [push-conversion](push-conversion.md), after
the self-enqueue. It is scheduled ahead of phase 1 because it is the
smallest landing in the plan and it closes one of the two holds on phase
5's owed-pass number. It builds no hub, so phase 1 is still the pilot for
the push machinery.
Date: 2026-08-01
Scope: `controller.go` (`DependenciesAdd`), and one shared helper in the
beehive layer that `client.go`'s `signalSpecWritten` also calls.
Related: [wake-push](wake-push.md) holds the hub this spec does *not* need,
and named this work under "The new-edge stamp". The
[stamp-every-new-edge ADR](../adr/2026-07-29-stamp-every-new-dependency-edge.md)
holds the durable stamp, which does not change. The
[self-enqueue ADR](../adr/2026-07-31-a-spec-write-enqueues-its-own-object.md)
holds the pattern this spec repeats.

## Goal

A controller declares a dependency. The source object owes a reconcile,
and `EdgesAdd` records that durably. Today nothing schedules it, so the
first reconcile waits for the owed pass — 30 seconds by default.

Enqueue the source when the edge commits. The durable stamp does not
change, and the owed pass stays the backstop.

## Where this sits in the plan

The plan is one sentence: **add a push path beside every poll, and remove
no poll.** A poll scans the store, so it sees every write. A push path sees
only writes that pass through this process. So a poll covers more than its
push path, not less, and it stays as the backstop that makes a lost notify
a latency cost instead of a divergence.

The schedule watch is the one exception, and it is already done
(`4c8f607`): its value
never reaches the store, so it has no writer this process cannot see, and
its poll was removed rather than kept. Nothing in the table below can
follow it.

Six drivers poll. This is where each one stands.

| Poll | Interval | Push path | State |
| --- | --- | --- | --- |
| Owed pass | 30s, per kind | A spec write enqueues its own object | Landed (`60b4ea4`) |
| Owed pass | 30s, per kind | **A new `depends_on` edge enqueues its source** | **This spec** |
| Dependency waker | 1s, global | The wake hub | [wake-push](wake-push.md), unbuilt |
| Watch poll — object watches | 1s, global | The per-kind delta hubs | [watch-push](watch-push.md), unbuilt |
| Watch poll — `EventsWatch` | 1s, global | The event hub | [events-push](events-push.md), unbuilt; its poll gate landed |
| Watch poll — `SchedulesWatch` | — | The schedule hub | **Landed, and its poll is gone** (`4c8f607`). Built separately; see the [schedule-watch ADR](../adr/2026-07-27-schedule-watch.md). The one value with no writer outside this process |
| GC sweeper | 30s, global | None yet | The delete path and the cascade steps push nothing. Not specified, and phase 5 leaves this interval alone until it is |
| Full pass | off | None, by decision | It scales with the object count and nothing may depend on it, so there is no latency to improve |
| Stale-dependents pass | 60s, global | None, ever | It re-derives staleness rather than carrying a delivery. It is what the wake paths are measured against |

The owed pass appears twice because two unrelated traces feed it:
`generation` against `observed_generation`, and `reconcile_owed`. The first
now pushes. This spec is the second. After it lands, the owed pass
backstops a prompt path for every write this process makes, which is what
phase 5 needs before it lengthens the interval.

### Why this is severable, like the self-enqueue was

Phase 2 was written as one landing: the hub, the dependent wakes, the
self-enqueue and this stamp. The self-enqueue left it and shipped alone,
because it needed no hub. This is the same case, and both reasons hold.

- **It needs no store read**, so the drain's batching buys it nothing.
- **It needs no hub**, so it adds no goroutine, no merge and no teardown
  ordering. Holding a 30-second latency behind machinery it does not use is
  the trade the self-enqueue already refused.

What is left in [wake-push](wake-push.md) after this is the hub and the
dependent wakes, which do read the store and do batch. That is the phase's
real content.

## Design

### The store already returns what the hook needs

`EdgesAdd` returns an `EdgesAddResult` that `DependenciesAdd` discards:

```go
type EdgesAddResult struct {
	From                 GroupKind // the source's kind, from the endpoint check
	ReconcileOwedStamped bool      // this call created a new depends_on edge
}
```

Both fields fall out of work `EdgesAdd` already does, so the hook costs no
extra query. This spec gives that return value its first consumer.

`From` is not decoration. **Edges are cross-kind**: a controller may
declare a dependency on another kind's behalf, so `fromID` is not
necessarily the calling controller's kind, and the store routes the durable
stamp by `fromID`'s own kind internally. A hook that requeued through the
`ControllerClient`'s own `GroupKind` would route to the wrong reconciler,
or to none. Route through `res.From`.

### The hook

Register through `Store.AfterCommit`, at the call site, carrying `res.From`
and `fromID`. Do not notify from the store.

`AfterCommit` is the hook for the same reason the self-enqueue uses it: a
rollback discards it, and so does a savepoint unwind inside a nested
`Within`.

**A reconcile opens no transaction of its own**, so state the dominant path
first. `reconcile` calls the controller directly, and each
`ControllerClient` write commits alone (`reconciler.go`, and "Reconcile is
not transactional" in `CLAUDE.md`). So on the ordinary path `EdgesAdd`'s
self-wrap has already committed when the hook is registered, there is no
transaction to defer to, and `AfterCommit` runs the hook **inline and
synchronously on the reconcile goroutine** (`sqlite/store.go`). The enqueue
therefore lands *during* `Reconcile`, not after it returns. That is what
"Why this cannot loop" and "The rate this changes" below reason about, so
it has to be said here rather than assumed.

The rollback case is real but opt-in: a controller that needs several
writes to land together wraps them in `ControllerClient.Within`, and a
`DependenciesAdd` inside that frame must not enqueue if the frame unwinds.
`AfterCommit` covers both shapes with one rule — notify exactly when the
commit is real — which is why it is still the right hook even though the
common path has nothing to defer.

Resolve the reconciler inside the hook rather than at registration. That is
what `signalSpecWritten` does, and both paths have a reason.

On the `Within` path the registration *may* run inside the caller's
transaction, and `bh.mu` is a lock `Register` and `stop` also want. On the
inline path there is no transaction, so the hook takes `bh.mu` on the
reconcile goroutine — which does not deadlock, because `stop` releases
`bh.mu` before `wg.Wait` (`beehive.go`, where the comment already says so
for the waker's sake). Deferring the lookup is safe on both paths; only the
first was stated before.

**Extract one shared helper.** `signalSpecWritten` is the same hook with
the kind fixed to the client's own. Lift the body to a beehive-layer method
that takes the target explicitly, and let `signalSpecWritten` pass its own
kind. This answers the open question [wake-push](wake-push.md) carries
about where a notify registration lives: one place holds the registration,
the reconciler lookup and the requeue, so a second notify site cannot get
those details subtly different.

**It does not stop a future write path from forgetting to notify**, and the
earlier draft claimed it did. A helper is only reached by a caller who
calls it. `EdgesAdd` has exactly two callers today — `controller.go`'s
`depends_on` and `client.go`'s `owned_by`, which stamps nothing and
deliberately enqueues nothing — so the seam that would actually enforce it
is a wrapper around `EdgesAdd` itself, not a shared requeue helper. Do not
build that wrapper for two callers. The honest guard is the umbrella spec's
rule that each new write path adds its notify and its line in
`docs/reconcile-triggers.md`, backed by the owed pass, which lists the
durable stamp whether or not anyone remembered the hook.

Enqueue with `requeueNow`, matching the self-enqueue and `Client.Requeue`'s
default. Do not clear the backoff counter: a new edge is not evidence that
a past failure will not repeat. Read the next section before treating that
as sufficient — **not clearing the counter does not preserve the ladder**
on the path this hook actually takes.

### The gate is `ReconcileOwedStamped`, and nothing else

Enqueue only when the store reports that it stamped. That is true only for
a `depends_on` edge this call *created*, self-edges excluded.

This is the same discipline as the self-enqueue: **read the store's report
of what the write did, never the caller's claim that it wrote.** Gating on
"the caller called `DependenciesAdd`" would enqueue on every re-assertion.
The self-enqueue ADR records what that class of mistake costs — `requeueNow`
cancels the backoff alarm and marks an in-flight id dirty, so a controller
that re-asserts and then fails retries at full speed and never reaches its
ladder.

There is deliberately no second, separately derived answer to "was the edge
new" for the hook to disagree with. `EdgesAddResult` is the one report.

### Why this cannot loop

A level-triggered controller re-asserts its whole dependency set on every
pass. That is the normal shape, and it is why the loop question has to be
answered rather than assumed.

The edge-new gate answers it. Re-asserting an existing set creates no edge,
stamps nothing and enqueues nothing. Only a genuinely new edge fires, so
the bound is **one enqueue per edge ever created** — exactly the bound the
durable stamp already carries, and the same argument the stamp's ADR makes
for it.

A controller that deletes and re-declares its set every pass pays per
re-create. The stamp's ADR recorded that trade as one extra pass each. That
pricing does not carry over to the enqueue — see "The backoff ladder does
not survive a non-converging edge set" below, which is where this spec adds
exposure the stamp did not have.

This is a different question from the one `UpdateStatus` raises. A
self-enqueue on every `resource_version` move would loop because a
reconcile ends in a write that moves it. Nothing in a reconcile creates an
edge that did not exist, once the set is declared.

### The rate this changes, which is not the same as the bound

Termination is not the whole answer. State the rate too.

**The common source is the object being reconciled right now.** A
controller declares its own object's dependencies, so `fromID` is usually
the in-flight id, and the hook runs inline on the reconcile goroutine
before `Reconcile` returns. The object is therefore enqueued again while
its own pass is still running, and reconciles a second time back to back
rather than about 30 seconds later.

**That second pass is not new work.** The increment lands above the count
the pass observed at load, so `ReconcileOwedDecrement` does not consume it
and the owed pass would have listed the object anyway. The push moves the
pass earlier; it does not add one. This is the same property the
[stamp ADR](../adr/2026-07-29-stamp-every-new-dependency-edge.md) relies on.

**What changes is the spacing, and it concentrates at startup.** When a
whole kind declares its edges for the first time, every object of that kind
reconciles twice with no gap between the two, instead of once now and once
at the next owed pass. First-pass volume doubles, and it doubles at the
busiest moment in the process's life. That is bounded and it is correct,
but a reader who sees only "one enqueue per edge ever created" will not
expect it.

### The backoff ladder does not survive a non-converging edge set

This is the sharpest edge in this spec. It is an accepted trade, not an
oversight, so it is written out rather than left to be discovered.

**`requeueNow` on an in-flight id bypasses the backoff.** It marks the id
dirty rather than queueing it (`workqueue.go`). The worker then calls
`work.done(id)` *before* `work.addAfter(id, backoff)` (`reconciler.go`),
and `done` sees the id queued and appends it to `items` at once. The
backoff alarm set on the next line is moot: the id is already dispatchable.
So a failing pass that created an edge retries immediately, at full speed,
however deep its ladder is.

This is the defect `signalSpecWritten`'s doc comment describes and the
`changed` gate exists to prevent, arriving at this site through a different
door.

**The edge-new gate bounds it only when the edge set converges**, which is
the normal shape and is why this is not a general hot loop. Re-asserting a
declared set creates nothing and enqueues nothing, so an ordinary failing
controller keeps its ladder. Two shapes escape that:

- a controller that creates a fresh child per attempt — `GenerateName`
  children, or a run-per-attempt design — and declares a dependency on it;
- a controller that deletes and re-declares its dependency set each pass.

Either one creates a genuinely new edge on every failing pass, so every
failing pass re-dispatches immediately and the ladder is never climbed.

**The trade, stated plainly: accept it, and do not suppress the enqueue.**
The alternative is to skip the enqueue when `fromID` is the object this
reconciler currently has in flight. There is no clean signal for that at
the call site — the hook is cross-kind, so `fromID` may belong to another
kind's queue entirely, where "in flight" is a different queue's state — and
buying it would mean reaching into the work queue from a commit hook. The
cost of the trade falls only on the two shapes above, and it costs CPU
against a controller that is already failing, never divergence. The owed
pass and the stamp are unaffected either way.

Revisit if a real controller hits it. `Client.Requeue`'s existing default
and the self-enqueue share this property, so a fix belongs at `requeueNow`
or at the `done`/`addAfter` ordering, and would then cover all three sites
at once rather than being special-cased here.

### What this does not close

Say both, so the landing is not read as more than it is.

- **A write from outside this process still waits for the pass.** The hook
  is registered in the beehive layer, so a second process on the same
  database, or the embedder writing through the `Store` they constructed,
  pushes nothing. No in-process push path can close this. It is the same
  coverage argument that keeps the dependency waker permanently, and it is
  a reason to set phase 5's interval with care rather than a reason to keep
  30 seconds.
- **A client-only source kind enqueues nothing.** `reconcilerFor` resolves
  to nothing and the hook is a no-op, as it already is for a spec write to
  such a kind. The durable stamp still lands, and nothing in this process
  drains it. That is unchanged by this spec, and it is not this spec's
  gap to close.

## Non-goals

- Do not change the durable stamp, its edge-new gate, or the atomicity of
  the endpoint check, the stamp and the insert. The store keeps that
  guarantee, and an implementation must not split it.
- Do not change the owed pass. It stays the backstop, on its current
  cadence. Phase 5 changes cadences, and only after this lands.
- Do not touch the watermark clear that rides the same edge-new gate. It is
  derived-state hygiene, and it has its own reasoning.
- Do not add an enqueue to `DependenciesDelete`. Dropping an edge can
  unblock a target's deletion, and the GC sweeper already lists that target
  because it is deletion-pending.
- Do not add an enqueue to the `owned_by` edge that `Create` writes. It
  stamps nothing, deliberately.
- Do not build the wake hub. This spec touches no hub at all.

## Test plan

Write whitebox tests in `package beehive`. Synchronize on channels and
fakes, never on sleeps. Drive them with the owed pass turned **off**, or
set so long it cannot fire, through `withOwedPassInterval` — so an enqueue
a test observes is provably the hook's and not the pass's. Turning the pass
*down* is the opposite of what that argument needs.

- **A new edge enqueues its source** without any tick.
- **The durable stamp still lands.** A separate assertion from the one
  above, and it must not race the reconcile the push triggers: that
  reconcile drains the stamp through `ReconcileOwedDecrement`. Read
  `reconcile_owed` from inside the controller, or with the source kind
  unregistered so nothing drains it. The point of the pair is that push was
  added beside the record, not instead of it — but one test asserting both
  halves would be flaky by construction.
- **A re-asserted edge enqueues nothing.** Declare the same dependency
  twice; the second call stamps nothing and schedules nothing. Assert
  directly, not by absence of a loop.
- **A self-edge enqueues nothing**, matching the stamp's own exclusion.
- **Cross-kind routing:** a controller of kind A declares an edge whose
  source is kind B. Kind B's reconciler is enqueued, and kind A's is not.
  This is the test that fails if the hook uses the caller's `GroupKind`,
  and nothing else in the suite would catch that.
- **Rollback enqueues nothing:** a `DependenciesAdd` inside a
  `ControllerClient.Within` that returns an error schedules nothing. Nest
  it inside an outer `Within` too, for the savepoint-unwind case. Neither
  shape is what an ordinary reconcile produces — a reconcile opens no
  transaction, so the hook usually runs inline — so this tests the opt-in
  path, which is the only one that can roll back at all.
- **A client-only source kind** enqueues nothing and returns no error.
- **The backoff ladder survives a *converging* edge set:** a failing
  controller that re-asserts the same dependencies keeps its ladder,
  because the second declare stamps nothing. This is the defect the
  self-enqueue shipped once and fixed, so pin it here rather than trusting
  the shape.
- **A non-converging edge set retries without spacing**, and the test says
  so. A failing controller that creates a new edge every pass re-dispatches
  immediately, because `requeueNow` on the in-flight id beats the backoff
  alarm. Pin the accepted behaviour rather than leaving it unasserted: a
  test that fails here means someone changed the trade in "The backoff
  ladder does not survive a non-converging edge set", which is a decision,
  not a bug fix.

  **Assert the mechanism, not the timing.** "Retries immediately" is a
  claim about the clock, and this suite has no clock to assert on:
  `baseRetryInterval` is an unexported field with no option, and
  `WithMaxRetryInterval` only caps the delay upward, so it cannot lengthen
  the first one. With a 1-second default, a test on a real
  `New`+`Register` beehive — which this hook requires — could only tell
  "immediate" from "backed off" by waiting, which the no-sleeps rule
  forbids. So after the failing pass returns, assert that the id is
  **dispatchable now rather than alarmed**: `scheduleAt(id)` reads as
  due-now, or `gauge.alarmFor(id)` shows the state the trade predicts.
  That is what "the backoff alarm is moot" means, and it needs no clock.

  Do not add a `withBaseRetryInterval` option for this. It would not
  violate the work-queue non-goal — an option is not the queue — but the
  assertion above needs no new surface, and a test knob added for one
  bullet outlives the bullet.
- **The owed pass still drains the stamp** with the hook removed from the
  path — the existing stamp tests pass unchanged. A test that needed edits
  means this spec changed the durable record, which it must not.
- **No goroutine outlives the tests.** `TestMain` enforces it. This spec
  adds none, so the check should be silent; it is named here because the
  umbrella spec makes it every phase's rule.

## Docs to update when this lands

- `docs/reconcile-triggers.md`: the "Wake owed" row gains the commit hook
  beside the owed pass, the way the "Spec not converged" row already reads
  "**and the write's own enqueue**". Section 5, "The new-edge stamp
  (durable)", gains the recording site and the restart answer — a crash
  between the commit and the drain loses the enqueue and keeps the stamp,
  which is the whole point of stamping.
- The [self-enqueue ADR](../adr/2026-07-31-a-spec-write-enqueues-its-own-object.md)
  is the closest existing decision. Extend it rather than writing a second
  ADR that repeats its argument: the gate, the hook and the routing are one
  decision applied at a second site. Record the backoff trade there too —
  it is a consequence of that ADR's own `requeueNow` choice showing up
  where the `changed` gate cannot reach, and it is the one thing in this
  landing a future reader would otherwise read as a bug.
- [wake-push](wake-push.md): strike "The new-edge stamp — still to do", and
  close its open question about the shared helper.
- [push-conversion](push-conversion.md), in two places. The backstop
  table's "Spec writes (landed) and new-edge stamps" row marks this half
  landed. And under "Phases", the first of the two holds on the owed-pass
  number — "The new-edge stamp does not push yet" — is struck, which leaves
  phase 5 held only by writes from outside this process. That is the item
  this landing exists to clear, so it is the one that must not be missed.
- `CLAUDE.md`: `Store.AfterCommit` currently has two users. It gains a
  third.

## Closed questions

- **The shared helper takes an `ObjectRef`.** It already pairs an id with a
  kind, and its doc says what this use needs verbatim — "the `GroupKind`
  needed to route a requeue" — with a `GroupKind()` helper for the lookup.
  It reads well at both call sites, so there is no reason to pass the two
  separately.

## Open questions

- Whether the backoff trade above should instead be fixed at its root, in
  `requeueNow` or in the `done`/`addAfter` ordering. That would cover
  `Client.Requeue` and the self-enqueue at the same time. It is out of
  scope here — this spec must not change the work queue — but it is the
  right place for the fix if a real controller ever hits it.
