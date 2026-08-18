# Reconcile returns one value, and beehive stamps observed_generation

- **Status:** Planned. Tracked in [#110](https://github.com/amorey/beehive/issues/110).
- **Date:** 2026-08-18

Four changes that ship together, because each one on its own leaves the surface
worse than either end state:

1. `Reconcile` returns a single `ReconcileResult` instead of `(Result, error)`.
2. `UpdateStatus` drops its `observedGeneration` argument, on `ControllerClient`
   and on `Store.Objects()` alike.
3. Beehive writes `observed_generation` itself, from the object it handed to
   `Reconcile`, based on what `Reconcile` returned.
4. The `ControllerClient` **passed into `Reconcile`** stops working once that
   call has returned. The one `Register` hands back is unaffected — see
   [Two clients, two lifetimes](#two-clients-two-lifetimes), which is the part of
   this spec most worth disagreeing with.

`ControllerClient.SetObservedGeneration` goes away with them.
`Store.Objects().SetObservedGeneration` **stays** — it becomes the sole handshake
writer, and the thing beehive calls to do the stamp.

## Context

**A controller can return something beehive ignores.**
`(Result{RequeueAfter: time.Minute}, err)` compiles, and the worker
(`reconciler.go:401-412`) branches on the error first and drops the `Result`
entirely. Nothing tells the caller. The two return values are not independent,
and the type says they are.

**Every controller hand-writes the same handshake.** Decide whether the status
changed; then either `UpdateStatus` with the generation, or
`SetObservedGeneration` with the generation. Same code in every kind, and the
failure mode is silent — a kind that forgets it never settles and sits in the
owed listing forever, re-queued every interval.

**`UpdateStatus` does two unrelated things.** The store contract
(`internal/storeapi/storeapi.go:779-793`) has to spell out that identical bytes
write no status "but ObservedGeneration/ObservedAt (and ResourceVersion) still
advance if this reconcile settled a new generation", and separately that a stale
generation "is ignored on the no-op path but written as given on the
content-changed path, rolling the object back to unsettled". That entanglement is
exactly why callers branch, and it is why `README.md:721` has to warn against
settling by re-passing the status you were handed — the no-op gate is the schema
version as well as the bytes.

**Bad generations are expressible.** `ErrObservedGenerationFuture` and
`ErrInvalidObservedGeneration` are exported (`types.go:117-123`) only because a
caller can pass a number that makes no sense. If no caller passes one, neither is
reachable from the public surface.

**The reconcile parameter has no end.** `typedController.client`
(`reconciler.go:50`) is built once at `Register` and handed to every pass, so a
controller that captures it holds something indistinguishable from a live pass
forever.

## Decision

### The surface

```go
type Controller[Spec, Status any] interface {
	Reconcile(ctx context.Context, client ControllerClient[Status], obj *Object[Spec, Status]) ReconcileResult
}

// ReconcileResult has no exported fields; these three build every value.
func Settled(requeueAfter time.Duration) ReconcileResult
func Unsettled(requeueAfter time.Duration) ReconcileResult
func Fail(err error) ReconcileResult
```

| Return | Reconcile succeeded | Stamp `observed_generation` | Decrement `reconcile_owed` / set watermark | Requeue |
| --- | --- | --- | --- | --- |
| `Settled(d)` | yes | yes | yes | after `d`; nothing scheduled if `d == 0` |
| `Unsettled(d)` | yes | no | yes | after `d`; **as soon as the floor allows** if `d == 0` |
| `Fail(err)` | no | no | no | backoff ladder |

`Settled` means "I have fully observed this object's current generation". It does
not mean the object is healthy, and it does not require that the pass wrote any
status. `Unsettled` means "I did real work, and this object is not caught up to
its spec yet". `Fail` replaces returning an error, with today's behaviour
unchanged.

`Result` is deleted. `ControllerClient.UpdateStatus` becomes:

```go
UpdateStatus(ctx context.Context, id ObjectID, status Status) error
```

and `Store.Objects().UpdateStatus` loses the argument the same way, becoming
status bytes and schema version only. `Objects().SetObservedGeneration` is
unchanged and keeps both sentinels; `types.go` stops re-exporting them.

### The zero value is invalid, and says so

`ReconcileResult` carries an unexported `kind uint8` whose zero is *no kind*, so
`ReconcileResult{}` is distinguishable from every constructed value. Returning it
is treated as `Fail(ErrInvalidResult)`: the backoff ladder, which is capped, plus
one Error log naming the kind and id.

The alternatives are both worse. Zero as `Settled(0)` lets a slip silently stamp
a generation no pass observed — the failure this whole spec exists to make
impossible. Zero as `Unsettled(0)` turns a slip into a permanent 1/sec dispatch
loop (see below). A loud, bounded failure is the only reading that cannot corrupt
the handshake or burn the connection.

`Fail(nil)` takes the same path and the same sentinel. It does not panic — a
library should not take the process down for a caller's slip — and it is not
promoted to `Settled(0)`, for the reason above.

### What `Unsettled(0)` actually costs

`Unsettled(0)` re-enqueues through `workQueue.add`, which uses `addThrottled`
(`workqueue.go:148-154`), so the per-id floor applies:
`defaultMinRequeueInterval` is 1s (`beehive.go:68`). `Unsettled(0)` is therefore
a **1/sec poll of one object**, not a spin — and not free either.

It is the right return for a controller that can make progress as soon as it is
called again. It is the wrong return for one waiting on an external event, which
should pass a delay matched to what it is waiting for: `Unsettled(30*time.Second)`
costs 1/30th as much and converges just as fast when the wait is that long.

Issue #110 argues `Unsettled(0)` must requeue immediately because an unsettled
object with nothing scheduled "is always a bug". That framing is too strong and
should not survive into the docs: such an object is picked up by the owed pass
every 30s, which is a slow drip rather than a defect, and the fix on offer —
1/sec forever — is 30× the load. The honest statement is narrower, and it is the
one to document: an unsettled object with nothing scheduled is *usually* not what
the author meant, so `Unsettled(0)` is given the useful meaning rather than the
useless one, and the delay is how you say what you actually want.

### Stamp the generation you handed to Reconcile

Beehive stamps `load.Object.Generation` — the value already in hand from
`GetForReconcile` — and **never** re-reads the row.

This is the one detail that can lose data. A reconcile starts on generation 5; a
user changes the spec mid-pass and the row moves to 6. Stamping the 5 that was
handed out leaves the object unsettled, so it reconciles again and the change is
picked up. Re-reading and stamping 6 would mark a spec change as observed by a
pass that never saw it, and nothing would reconcile again. The bug is silent in
both the store and the tests unless something pins it.

The store's clamp (`advanceObserved`, `sqlite/store.go:1452-1457`) makes this safe
against completion out of order: a slow generation-5 pass finishing after a
generation-6 pass has stamped 6 writes nothing, rather than rolling the object
back to unsettled.

### The stamp is gated in memory, not in the store

`Objects().SetObservedGeneration` opens a `Within` and does a scoped `SELECT`
before `advanceObserved` can decide the write is a no-op
(`sqlite/store.go:1422-1445`). On the store's single connection — contended with
the watch tailers, the waker, GC and every user write — that is a BEGIN, a SELECT
and a COMMIT per object per pass, for a no-op. A controller that reports status
would go from 1 transaction per pass to 2; one that reports nothing goes from 0
to 1; a startup full pass pays it once per object. "Wakes nobody" is true of the
write and false of the transaction, and the first draft of this spec conflated
them.

So beehive gates before calling: skip the store entirely when

```go
load.Object.ObservedGeneration != nil && *load.Object.ObservedGeneration >= load.Object.Generation
```

Both values are already in hand (`RawObject`, `storeapi.go:221-222`), so the
steady state costs **zero** transactions — strictly better than today, where the
controller's own `SetObservedGeneration` pays for the no-op.

This is sound only because **change 2 makes `observed_generation` monotonic**.
Today `UpdateStatus`'s content path writes a stale generation as given, rolling a
converged object back to unsettled, so a value read at load could be higher than
the value at stamp time and the gate would skip a write the store would have
made. Remove that argument and `advanceObserved` is the only writer, and it only
ever increases. The gate and change 2 cannot ship separately; note it where the
gate is implemented, because nothing else in the code will say so.

The store call is still made whenever the gate does not fire, and the store's own
clamp remains the authority — the gate is an optimisation over it, never a
replacement.

### Ordering after Reconcile returns

Exactly this order, and the reasons are not interchangeable:

1. **Normalize the result.** `ReconcileResult{}` (no kind) and `Fail(nil)` both
   become `Fail(ErrInvalidResult)` here, before anything reads the result. See
   [Normalize before you branch](#normalize-before-you-branch) — every gate below
   assumes it has already run.
2. **Invalidate the pass client.** Nothing the controller holds may write past
   the point where beehive starts concluding the pass.
3. **Decrement `reconcile_owed`** (`reconciler.go:112`), when the result is
   `Settled` or `Unsettled`.
4. **Write the dependency watermark** (`reconciler.go:126-131`), on the same
   condition.
5. **Stamp `observed_generation`**, when the result is `Settled`.
6. **The GC block** (`reconciler.go:133-148`).

The stamp goes after the watermark because a crash between them must not leave a
settled object whose watermark never landed: a low watermark on an unsettled
object over-reports staleness and costs a redundant pass, while a settled object
with a stale watermark is a dependent that never re-derives. Only this order has
the safe failure on both sides.

The stamp goes before GC because a deleting object can be collected in the same
pass, and a stamp after `gcCollect` would write to a row that no longer exists —
turning every clean cascade into a warning.

### Normalize before you branch

Every gate in this spec is phrased **positively** — "when the result is `Settled`
or `Unsettled`", never "when the result is not a `Fail`". This is not style. A
negative gate is what makes the zero value dangerous: `ReconcileResult{}` is not
a `Fail`, so `!isFail` admits it, and a slip would decrement the owed count,
write the watermark and stamp `observed_generation` — exactly the silent stamp
the zero-value decision exists to prevent. An earlier draft of this spec said
"replace the `reconcileErr == nil` gates with 'not a `Fail`'", and reintroduced
the bug in one clause.

Two things pin it, and both are required:

- **Normalization happens in `typedController.reconcile`**, immediately after
  `inner.Reconcile` returns and before step 2 — not in `runWorker`. Anything that
  reaches the owed decrement, the watermark or the stamp has already been through
  it, and `runWorker` sees only normalized values.
- **Every gate names the kinds it admits.** Switch on the kind rather than
  testing for the absence of one, so a kind added later fails to compile at each
  site instead of silently joining the success path.

### A failed stamp is not a failed reconcile

Treat it exactly like the watermark write above it: warn, leave the object
unsettled, let the unsettled and owed passes re-derive. Low only over-reports.

Suppress the warning on two errors, not one:

- `ctx.Err() != nil` — shutdown, not a lost pass, as the watermark write already
  does.
- `ErrNotFound` — a cascade from another kind can request and collect this row
  between `GetForReconcile` and the stamp. The GC ordering above only covers a
  collect this pass performed; this covers one performed by anybody else, and it
  is a normal race rather than a fault.

The consequence to document: `Settled(0)` means "converged and nothing
scheduled", not "converged and provably recorded".

### The branches that never reach a controller result

Four returns in `typedController.reconcile` produce a result with no `Reconcile`
behind them. The stamp lives in `typedController.reconcile`, **not** in
`runWorker`, so the worker's reading of `Settled(0)` is purely "clear the backoff
and schedule nothing" — which is what makes the first three safe:

| Site | Today | New |
| --- | --- | --- |
| `:70-74` already collected before the pass | `Result{}, true, nil` | `Settled(0)`, `gone` |
| `:88-96` undecodable **and** deleting | `Result{}, gone, nil` | `Settled(0)`, `gone` as returned by `gcCollect` |
| `:98` undecodable, not deleting | `Result{}, false, nil` | `Settled(0)`, not gone |
| `:139` GC failed after the pass | `cmp.Or(reconcileErr, gcErr)` | `Fail(cmp.Or(resultErr, gcErr))` |
| `:145` collected during the pass | `Result{}, true, nil` | `Settled(0)`, `gone` |

`:98` keeps today's meaning precisely: a quarantined row is a no-op success that
clears the backoff and schedules nothing, and the owed pass re-enqueues it. It
must not become `Unsettled(0)`, which would poll an undecodable row once a second
forever.

`:139` is the one case where a stamp has already committed and the pass then
fails. That is correct and needs no compensation: the object really is settled,
and the backoff retry re-runs a reconcile that will find the stamp in place and
gate on it. Say so, or the next reader will try to roll it back.

`:145` drops the delay and any error, as today — a collected id has nothing left
to schedule, and the worker `forget`s it.

## Two clients, two lifetimes

**`Register` returns a `ControllerClient` and it is not restricted.**
`README.md:67` documents it as the client "for status writes from outside a
reconcile", and `README.md:745` makes it the sanctioned route for "background
work — timers, subscriptions, engines", which "belongs to your application".
`beehive.go:442` returns it, and it is the same `*controllerClientImpl[Status]`
that `typedController.client` holds.

This forces a correction to how change 4 was argued. An earlier draft claimed a
status write landing after a `Settled` is "the one class of staleness this
package cannot recover from". That is wrong: the package *supports* exactly that
write, by design, through the `Register` client, and has since before this spec.
Out-of-band status writes are a documented feature, not a hazard, and any
rationale for change 4 that would also condemn them proves too much.

What survives is narrower, and it is enough. The parameter and the `Register`
return differ in what the caller is entitled to assume:

- The **`Register` client** is app-owned. The app knows it is writing out of
  band; beehive makes no claim about which generation such a write belongs to,
  and never did.
- The **parameter** is pass-owned. It arrives as part of a call that beehive
  concludes with a generation stamp, and a capture of it makes a later write look
  like part of a pass that has already ended. That is the confusion worth
  refusing, and refusing it costs a correct controller nothing.

So change 4 is a scoping rule about one value, not a policy about out-of-band
writes. Restricting the `Register` client too would mean deleting the documented
background-work story and reworking `README.md:67` and `:745`; that is a separate
and much larger decision, and it is not this spec.

**The cost, stated plainly:** two values of the same interface type now behave
differently with nothing in the type to distinguish them, and "the client stops
working when your reconcile returns" is true only of the one that was passed in.
The doc comments on `Register` and on `Controller.Reconcile` both have to say
which is which, and a reader who conflates them gets a runtime error rather than
a compile error.

The obvious alternative — give the parameter its own interface type,
`PassClient[Status]` — **does not buy what it appears to.** Go interface values
with identical method sets are assignable in both directions, so
`var c ControllerClient[S] = passClient` compiles, and a controller stashing its
pass client in a `ControllerClient` field — the exact mistake — still builds. A
rename alone is documentation with extra steps.

To get real enforcement the two types have to actually differ: an unexported
marker method on `PassClient` is the cheap way, or drop a method it genuinely
should not carry. That is a larger change than the rename it looks like — a new
exported interface, a `Controller` signature that no longer mentions
`ControllerClient`, a marker method that appears in godoc and has to be explained,
and a docs pass over every controller example.

**This spec takes the runtime path.** The distinction is documented at both
introduction points, the migration note leads with it, and the
`Register`-client-keeps-working test pins it. A marker-method `PassClient` is a
reasonable follow-up and belongs in its own spec; what it is not is a cheap
substitute for the flag.

### What the restriction is, and what it is not

Every method of the pass client fails with `ErrReconcileReturned` once
`Reconcile` has returned — reads included, not just writes. "The client you were
passed stops working when your reconcile returns" is a rule a caller remembers; a
table of which halves still work is not.

It is a **fail-fast, not a barrier**. Beehive flips the flag and proceeds; it does
not wait for calls already in flight. A goroutine that entered `UpdateStatus` — or
`Within` — just before the flag flipped runs to completion, and its commit may
land either side of the stamp. The doc comment must say so rather than implying a
guarantee the flag cannot give.

One consequence is new even though the hazard is not. A `Within` still open when
the flag flips holds the store's single write connection, and the three writes
beehive now performs after `Reconcile` returns — the owed decrement, the
watermark, the stamp — queue behind it. Previously a straggling `Within` blocked
only its own controller's later writes; now it blocks beehive concluding the
pass. Nothing is lost when it does (the stamp's failure path is already
non-fatal), but it is worth one line beside the fail-fast note: the restriction
reduces how often this happens and does not eliminate it.

### Implementing it

Add an unexported `scopedControllerClient[Status]` wrapping the shared
`*controllerClientImpl[Status]` behind an `atomic.Bool`. Every method loads the
flag and returns `ErrReconcileReturned` when it is clear. The flag must be atomic
rather than a plain bool: the caller it guards against is concurrent by
definition, so a plain read is a data race under `-race` and a real one in
practice.

`typedController.client` changes type from `ControllerClient[Status]` to the
concrete `*controllerClientImpl[Status]`, since the reconcile path now wraps it
and beehive's own stamp calls the unexported handshake method on it directly. It
remains the same value `Register` returns.

`typedController.reconcile` allocates one wrapper per pass, passes it to
`inner.Reconcile`, and clears the flag immediately on return, ahead of steps 2-5
above. The allocation is one small struct against a `GetForReconcile` round trip
in the same function; it does not need pooling.

`gcCollect` is unaffected — it reaches the store directly (`gc.go:35-36`) and
never through a `ControllerClient`, so the GC block after the flag clears keeps
working. The "finalizer clearing routed through the controller" in `CLAUDE.md`
means the controller clears finalizers during its own `Reconcile`.

## Consequences

**A settling status write costs two write-log entries where it cost one.** This
is the broadest cost in the change and it is not specific to conditions. Today
`Objects().UpdateStatus` writes the status bytes and the handshake in one
`UPDATE` under one `recordObjectWrite` (`sqlite/store.go:1531-1546`): one entry,
one resource version, one wake. After the split, the status write appends one
entry and beehive's stamp appends another, so the most common path in the package
— a status-writing controller settling a generation — goes from one entry and one
wake to two. That lands on the dependency waker, every object tailer for the kind
and every subscriber behind them.

It is a fair trade for untangling the two concerns, and it is bounded by the same
gate that makes the steady state cheap: the doubling costs a write only on a pass
that *settles a new generation*, and a converged object re-reporting identical
status still costs zero entries and zero transactions. Say both halves together
wherever this is documented, or the cost reads as unbounded.

**`Within` can no longer include the stamp.** Today a controller whose report is
conditions composes `SetObservedGeneration` with `SetConditions` in one
transaction. After this change the conditions commit inside `Reconcile` and the
stamp commits after it — two transactions where there was one. A crash in
between costs one extra reconcile, which is harmless and level-triggered, but it
is a documented composition going away.

**`ObservedAt`'s meaning tightens.** It moves only when beehive stamps a newly
settled generation, so "when the object settled at `ObservedGeneration`"
(`README.md:723`) becomes exactly true rather than nearly true. Still not a
liveness signal.

**Two exported sentinels go, two arrive.** `ErrObservedGenerationFuture` and
`ErrInvalidObservedGeneration` become unreachable from the public surface and
stay in `internal/storeapi`, where tripping one is a beehive bug rather than a
caller's. `ErrReconcileReturned` and `ErrInvalidResult` replace them in
`types.go`.

**A controller that captured its pass client breaks loudly.** That is the intended
outcome, and it is the only part of this change that turns working code into
failing code rather than non-compiling code.

## Implementation

In `types.go`: delete `Result`; add `ReconcileResult` with an unexported
`kind uint8` (zero = invalid) and a duration/error, plus the three constructors.
Keep it a struct, not an interface — it is a value the worker switches on. Add
`ErrReconcileReturned` and `ErrInvalidResult`; drop the two generation
re-exports.

In `controller.go`: change `Controller.Reconcile`'s signature; drop
`SetObservedGeneration` from the `ControllerClient` interface and keep the impl
as an unexported method (beehive's stamp is its only caller); drop the argument
from `UpdateStatus` and its impl; add `scopedControllerClient[Status]`.

In `reconciler.go`: `controllerAdapter.reconcile` (`:41`) returns
`(ReconcileResult, bool)`; the error folds into the result and `gone` stays. In
`typedController.reconcile`, wrap the client per pass, normalize the result,
then apply the ordering and the branch table above. The `reconcileErr == nil`
gates at `:112` and `:128` become a positive test for `Settled`-or-`Unsettled`,
never `!isFail`. In `runWorker` (`:395-412`), switch on the normalized result:
`Fail` → the backoff ladder, unchanged, logged at Error when the error is
`ErrInvalidResult`; otherwise clear the backoff and either
`addAfter(alarmRequeueAfter)` for a non-zero delay, `add` for `Unsettled(0)`, or
nothing for `Settled(0)`.

In `internal/storeapi` and `sqlite/store.go`: drop `observedGeneration` from
`Objects().UpdateStatus` and delete the two doc paragraphs that existed only to
describe the entanglement. `SetObservedGeneration` and `advanceObserved` keep
their bodies, but three things around them change:

- **`UpdateStatus`'s scoped read narrows from four columns to two.**
  `generation` and `observed_generation` were read only to serve the handshake
  (`sqlite/store.go:1503-1512`); what remains is `schema_version_status, status`
  for the content compare. The comment at `:1500-1502` says "four columns, not
  the row" and has to be updated with it.
- **The unclamped write at `:1541-1546` loses its `observed_generation` column**,
  and the comment at `:1535-1540` explaining why it deliberately bypasses
  `advanceObserved` goes with it. That comment describes the sole non-monotonic
  writer in the store; deleting it is what the in-memory gate's soundness rests
  on, so delete it in the same commit as the gate, not before.
- **Three helpers drop from two callers to one.** `advanceObserved`,
  `checkObservedGeneration` and `checkObservedNotFuture` lose their `UpdateStatus`
  call sites and are left serving `SetObservedGeneration` alone. None reach zero
  callers, so `staticcheck -checks=all` stays quiet — but `advanceObserved`'s
  `dbtx` parameter is now always `s.conn(ctx)` and can be dropped if the
  implementer wants it.

### Answers to the issue's open questions

**`Fail(nil)`** is `ErrInvalidResult`, as above — loud, bounded, never a silent
stamp.

**`Fail` does not take a requeue delay.** The ladder is what keeps a failing
controller off the store's single connection, and nothing needs to override it
yet. Addable later without a break.

**`Settled` and `Unsettled` stay two constructors**, not one with a bool. They
differ in what `0` means, and a bool at the return site hides that.

## Tests

Handshake:

- **The stamped generation is the one handed to `Reconcile`.** Bump the spec from
  inside `Reconcile`, return `Settled(0)`, assert `ObservedGeneration` is the old
  generation and the object is re-dispatched. The data-loss case; it gets a named
  test.
- **`Settled(0)` on a converged object opens no transaction** — not merely "writes
  nothing". Assert on the **call count**: an `objectsOverride` hook that records
  invocations, asserted at zero. A double that merely *fails*
  `SetObservedGeneration` proves nothing here, because a failed stamp is
  non-fatal by design — the pass would succeed whether the gate fired or the
  store was called and warned. Counting is the only assertion that matches the
  test's title.
- **`Settled` on a newly settled generation emits**, and a subscriber waiting on
  the object sees it.
- **`Settled(d>0)` both stamps and schedules** — the two are independent.
- **`Unsettled(d)` does not stamp**, and the object stays in the unsettled
  listing.
- **`Unsettled(0)` re-dispatches at the work-queue floor**, ~1/sec, not a spin.
- **`Settled(0)` schedules nothing** — `WatchSchedule` reports no
  `NextRequeueAt`.
- **`Fail(err)` takes the backoff ladder**, stamps nothing, decrements no owed
  count and writes no watermark.
- **`Unsettled` still decrements `reconcile_owed` and writes the watermark.**
- **`ReconcileResult{}` and `Fail(nil)` both fail with `ErrInvalidResult`** and
  stamp nothing.

Ordering and failure:

- **A failed stamp leaves the object unsettled and does not retry under backoff**
  — one warn, and a later pass from the unsettled listing.
- **A stamp failing with `ErrNotFound` warns not at all**, distinct from the
  general failure above.
- **The watermark write commits before the stamp**, pinned by a store double
  recording call order.
- **A failed watermark write still stamps** — the two are independent, and the
  stamp is not conditional on it.
- **A deleting object collected in the same pass stamps nothing** and logs no
  warning.
- **A `gcCollect` failure after a `Settled` keeps the stamp and takes the
  backoff** — pinning `:139`, where today's error survives the GC block.
- **An undecodable, non-deleting row clears the backoff and schedules nothing**,
  pinning `:98` against an `Unsettled(0)` regression.

Client lifetime:

- **A captured pass client fails after `Reconcile` returns** —
  `ErrReconcileReturned` from a write *and* from a read.
- **The pass client is live for the whole of `Reconcile`**, including inside
  `Within` and a nested `Within`.
- **The `Register` client keeps working during and after a reconcile** — the test
  that pins finding 1's resolution. If the reviewer chooses `PassClient` instead,
  this test inverts and the two clients stop sharing a type.
- **Race-free under `-race`**: a goroutine calling the pass client in a loop while
  the pass returns, asserting only that it never races and ends in
  `ErrReconcileReturned`.

Doubles:

- `fakeObjects` in `testutils_test.go` needs `SetObservedGeneration` filled in
  where it still panics, and `objectsOverride` (`testutils_test.go:794-812`)
  needs a `SetObservedGeneration` hook — the failed-stamp and call-order tests
  above both need to override it.

## Migration

All four are breaking. The first three are mechanical and the compiler finds
every site:

- `return Result{}, nil` → `return Settled(0)`
- `return Result{}, err` → `return Fail(err)`
- `return Result{RequeueAfter: d}, nil` → `return Settled(d)` **or**
  `Unsettled(d)` — see below
- drop the generation argument from every `UpdateStatus` call
- delete every `SetObservedGeneration` call

The third line is the one needing judgment, and it is not rare: a controller that
returned success with a delay while knowing the object was not caught up becomes
`Unsettled(d)`, not `Settled(d)`. Today such a controller usually settles anyway,
because `UpdateStatus` carried the generation and it passed `obj.Generation` on
every path — so the migration changes observable behaviour, correctly: the object
now stays unsettled until it converges, and appears in the unsettled listing
until then.

`examples/conditions/main.go:132` is exactly this case and must be migrated by
hand: it returns `Result{RequeueAfter: 1500ms}, nil` while replicas are still
coming up, having called `UpdateStatus(…, obj.Generation, …)` at `:124`. It
becomes `Unsettled(1500 * time.Millisecond)`, and the example's observable
behaviour changes with it — worth a sentence in the example's own comment, since
demonstrating the settled/unsettled distinction is now part of what it shows.

In-tree the mechanical pass covers `examples/greeting`, `examples/events`,
`examples/lowpower`, `examples/conditions` and `examples/cascade` — nineteen
return sites and five `UpdateStatus` calls, of which one return site needs
judgment. No example captures its pass client.

The fourth change is not caught by the compiler. A controller that hands the
`ControllerClient` it was *passed* to a goroutine outliving the reconcile keeps
building and starts failing at runtime with `ErrReconcileReturned`. The fix is
either the `Register` client, which is what it is for, or the pattern beehive is
built for: keep the result in memory, call `Requeue`, and let the next
`Reconcile` write it. Lead the release notes with this — it is the only change
here a build cannot find for you.

## Docs to update when this ships

- `CLAUDE.md`: the generation-handshake bullet, and the "Reconcile is not
  transactional" bullet — which gains the pass client's lifetime and loses the
  `SetObservedGeneration` composition.
- `README.md`: the `Result` section (`:328`); the `ControllerClient` listing
  (`:696-697`); the handshake prose (`:715-723`), including deleting the
  re-pass-the-status warning at `:721`, which stops being reachable; the
  `Controller` interface (`:741`); the `Requeue` note's reference to
  `Result.RequeueAfter` (`:656`); and both worked examples.
- `README.md:747` specifically — not just "the transaction note". Its last
  sentence says the handshake covers a concurrent spec change because
  "`UpdateStatus` rejects a generation from the future, and an older one leaves
  the object unsettled". After change 2 `UpdateStatus` does neither; the same
  guarantee now comes from beehive stamping the generation it handed out. Rewrite
  the sentence rather than deleting it — the guarantee survives, its mechanism
  does not.
- `README.md:67` and `:745`: the two clients now have different lifetimes and the
  distinction has to be stated where each is introduced.
- `docs/reconcile-triggers.md`: two entries, not one. `Unsettled(0)` is a new
  trigger and belongs in the map; and the stamp is a new write-log producer, so a
  settling pass now emits two entries where it emitted one. Record it as its own
  line with the gate that bounds it, not folded into the handshake's existing
  entry.
- `docs/TODO.md:14-21`: **reword, do not delete.** It explains that
  `SetObservedGeneration` is safe in a dependency cycle because its clamp bounds
  it to one write per generation, and in a cycle no generation moves. That is
  still true of beehive's stamp, which clamps identically — the entry just names
  a verb that is no longer public, and
  `TestSetObservedGenerationWakesDependentsOncePerGeneration` moves with it.
- `docs/adr/2026-07-27-generation-handshake-and-noop-writes.md` is **superseded**,
  not edited. Fold what still governs live code — the generation bump on real
  spec changes, the content no-op — into a new record and delete the old file per
  the ADR README. The pass client's lifetime is a second decision and wants a
  second record; do not fold it into the handshake one.
