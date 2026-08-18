# Register hands back nothing

- **Status:** Ready to implement.
- **Date:** 2026-08-18
- **Base:** `main` at `8a30bc9`; line references are as of it.
- **Commit:** `feat!` — `Register`'s result changes, so every caller breaks at
  compile time. That is the point: this is the change a compiler *can* find,
  unlike the one it follows.

## Problem

Two values of one interface type behave differently, with nothing in the type to
tell them apart. The `ControllerClient` a pass is handed dies when `Reconcile`
returns; the one `Register` hands back lives as long as the process and may
write whenever it likes. The distinction is carried in doc comments, and getting
it wrong is a runtime error rather than a compile error — the wart the
[pass-client record](../adr/2026-08-18-the-pass-client-dies-with-the-pass.md)
disclosed and left standing.

The reason it was left standing was that the long-lived client is the only write
surface an application has outside a pass: `Client` (`client.go`) reads events
and conditions but writes neither, and writes no status at all. So "no
out-of-band status writes" could not be stated as a rule without taking a
capability away.

Now that beehive owns the handshake, take it away. A settled object is a
checkpoint a consumer waits on — `ObservedGeneration == Generation`, then read
status — and a write with no pass behind it moves status underneath that
checkpoint. Nothing re-derives it: no pass is owed, no watermark is low, no
driver lists it. The long-lived client is the one surface from which that write
can be made.

`SetObservedGeneration` needs nothing here. `ControllerClient.SetObservedGeneration`
went with the handshake change; `Store.Objects().SetObservedGeneration`
(`internal/storeapi/storeapi.go:763`) has one caller in this repo
(`controller.go:131`), and reaching it means writing to the store behind a
running `Beehive`, which is
[already unsupported](../adr/2026-08-05-one-process-one-beehive-sole-writer.md).
It stays on the `Store` interface: hiding a method the store must implement
behind a type assertion turns a compile error into a runtime one for anyone
writing a store, which is a worse contract for no gain.

## Decision

**A `ControllerClient` exists only for the duration of one pass.** `Register`
returns `error` alone, and `Reconcile`'s parameter becomes the only way to hold
one.

```go
func Register[Spec, Status any](bh *Beehive, gk GroupKind, c Controller[Spec, Status], opts ...Option) error
```

Inference is unaffected: `Spec` and `Status` are still fixed by the `Controller`
argument.

Both existing errors keep their text and their conditions — registration after
`Start`, and a second controller for one kind.

### Step 2: one client type, not two

Once nothing outside a pass holds a client, the split between
`controllerClientImpl` (built at `Register`, shared) and `scopedControllerClient`
(built per pass, wrapping it) has nothing left to express. Collapse them:

- Delete `scopedControllerClient` and its ~90 lines of delegation
  (`controller.go:263`–end).
- Move `done atomic.Bool` and the `live()` gate onto `controllerClientImpl`, and
  call `live()` first in each of its exported methods — **except `SetCondition`,
  which delegates to `SetConditions` and inherits the gate from it.** Gating both
  is harmless but reads as an oversight in the other direction; one owner, stated.
- `reconciler.go:108` builds one per pass — `&controllerClientImpl[Status]{bh: t.bh, gk: t.gk}` —
  instead of wrapping `t.client`, and `typedController.client` (`reconciler.go:53`)
  goes.
- **`stampObserved` moves to `typedController`.** It is beehive's write, made
  deliberately *after* `end()` (`reconciler.go:147`), so leaving it on the client
  would mean one method that must not consult the gate sitting among fourteen that
  must. Off the client, there is no exception to remember. It keeps its comment
  about `SetObservedGeneration` staying the sole writer of the column.

  **Carry the wake with it.** `stampObserved` ends in `c.wakeAfter(ctx, nil)`
  (`controller.go:136`), which becomes `t.bh.signalKindWritten(ctx, t.gk)`.
  Dropping it costs no test — the nearest,
  `TestSetObservedGenerationWakesDependentsOncePerGeneration`, is store-level and
  covers the write-log entry, not the in-memory signal — and shows up only as
  watch latency up to the floor, which is the failure `wakeAfter`'s own comment
  says nothing else catches.

Allocation is unchanged — one value per pass either way — and the gate's reach
is unchanged. Land it in the same commit: leaving a shared client nothing shares
is the wart this spec exists to remove, one indirection further in.

### What is lost, and what replaces it

| Was, on the `Register` client | Now |
| --- | --- |
| `UpdateStatus` from a goroutine | Keep the result in memory, `Client.Requeue`, write it in the pass |
| `SetCondition`/`SetConditions` | Same |
| `AddDependency`/`DeleteDependency` | Same |
| `DeleteFinalizer` | Same — finalizer work already belongs to a deleting pass |
| `AddEvent` from a goroutine | **No replacement.** See below |
| `Within` over app writes | `Client` writes still group under `Store.Within` |

`AddEvent` is the real casualty and the only one. An event bumps no
`resource_version`, settles nothing and is invisible to the handshake, so the
argument above does not reach it — a background prober appending to an event log
breaks no checkpoint. It goes anyway, because it arrives on the same client, and
carving one method out of the type would restore exactly the "which half still
works" table the pass-client record refused to write.

Record it in [`TODO.md`](../TODO.md) as a gap we chose, not one we missed: *an
application has no way to append to an event log outside a reconcile pass.* The
fix, if it is ever wanted, is `Client.AddEvent` — kind-scoped like the
`ControllerClient` verb, sitting beside the three event reads that already live
there — and it needs its own decision, because `Client` writing anything a
controller owns is a line this package has not crossed.

## Non-goals

- **Renaming `ControllerClient`.** `PassClient` would describe it better now, but
  Go interface values with identical method sets stay assignable both ways, so
  the rename enforces nothing that this change has not already enforced by
  construction. Separate decision, separate commit.
- **A marker method to make the two clients distinguishable.** There is one
  client.
- **Widening `Client`.** No new write verb ships here; see the `TODO.md` entry
  above.
- **Touching the handshake, the store surface, or `ErrReconcileReturned`'s
  semantics.** The error keeps its name, its text and its fail-fast behaviour —
  it simply covers every `ControllerClient` that exists.

## Call sites

**`examples/events/main.go` is the one example that must change shape**, and it
is the honest cost of the decision made visible: its prober is app-owned
background work whose whole job is `AddEvent` (`main.go:83`, `:100`).

Move the probe into `Reconcile`. The controller carries the scripted outcomes and
emits the burst on its first pass; `main` drives the one further probe the resume
half needs with `client.Requeue`, and blocks on the stream for it. Keep the output
shape: the panel, the runs, the resume above the snapshot. The demo is about
aggregation into runs, not about who writes them.

**`main` must wait for the burst, or the demo is flaky.** Today the 50 probe
events are written synchronously before `WatchEvents` reads its snapshot. Written
from `Reconcile` they are written by a reconcile worker: `Create` returns as soon
as the row commits, `main` falls straight through, and the panel renders whatever
happened to land — usually nothing. So the controller closes a
`ready chan struct{}` after the burst and `main` waits on it before watching.
That is the example's own synchronisation, not a beehive guarantee, and it should
say so in one line — a demo that reads a snapshot has to know what it is a
snapshot *of*.

**Rewrite the package doc comment too.** It narrates the prober as "an ordinary
app goroutine, not beehive machinery" and diagrams
`Create(spec) -> prober AddEvent×N -> …` (`main.go:18`–`:25`). That header is the
part of the example that most directly teaches the capability being removed;
leaving it in place would teach the opposite of the change.

**The scripted burst puts sequence state in a controller**, which sits against
the level-triggered principle. It is a demo script, not convergence logic, and
`examples/conditions` already carries per-object state (`online map[ObjectID]int`)
— note it in the example so it reads as a choice rather than an accident.

The four other examples and the README snippet discard the result already
(`_, err =` / `_, _ =`) and lose their `_,`; `cascade` has two `Register` calls.

`Register` has 131 call sites in the test suite; ~30 bind the result. Those get
a `testutils_test.go` helper:

```go
// controllerClientFor returns a live client for gk, standing in for what
// Register used to hand back. Tests are whitebox, so a client is constructible.
func controllerClientFor[Status any](bh *Beehive, gk GroupKind) *controllerClientImpl[Status]
```

The value is identical to the one a pass builds, `done` unset. That is the point
of the helper and its limit: it exercises the surface, not the scoping. The
scoping stays pinned by `TestPassClientGatesEveryMethod`, whose table at
`controller_test.go:1500` constructs the client directly and is unaffected by
step 2 beyond the name it constructs.

Three more sets of sites that binding-count misses, all from step 2:

- **12 `typedController{client: …}` literals** in `reconciler_test.go` (1362,
  1412, 1492, 1534, 1570, 1601, 1636, 1671, 1702, 1755, 1793, 1964) lose the
  field step 2 deletes.
- **`controller_test.go:1500` and `:1508`** construct `scopedControllerClient`
  directly and become the merged type.
- **`client_test.go:3244` and `:3525`** do
  `cc.(*controllerClientImpl[cStatus]).bh`; the helper returns that type already,
  so the assertion goes.

## Tests

- `TestRegisterReturnsOnlyAnError` — a compile-shaped assertion, but the two
  error paths (after `Start`, duplicate kind) need re-pinning against the new
  signature wherever they live today (`beehive_test.go`).
- `TestPassClientStopsWorkingWhenReconcileReturns` (`controller_test.go:1344`)
  loses its `registerClient` half — it currently pins that the captured pass
  client fails *while the Register client still succeeds*, and there is no second
  client to compare against. Rewrite it as what it now pins: the captured client
  fails, and the next pass's client succeeds. **Vary the status value per pass**
  — the controller writes `cStatus{Val: "inside"}` every pass and `UpdateStatus`
  skips a byte-identical write, so the naive port passes for the wrong reason.
  `TestPassClientIsSafeAgainstAConcurrentCaller` and
  `TestPassClientGatesEveryMethod` port unchanged.
- Every test using the helper above should keep asserting what it asserted; this
  is a plumbing change, not a coverage change. A test that quietly loses its
  subject is a bug in the port.
- `go vet ./...` and `staticcheck -checks=all ./...` both matter more than usual
  here: step 2 deletes a type and moves a method, and `-checks=all` is what
  catches an unexported leftover.

## Docs to update

- **New ADR**, `docs/adr/2026-08-18-a-controller-client-exists-only-for-a-pass.md`,
  which **replaces**
  [`2026-08-18-the-pass-client-dies-with-the-pass.md`](../adr/2026-08-18-the-pass-client-dies-with-the-pass.md) —
  fold forward the fail-fast consequence, the in-flight-call caveat and the
  `Within`-holds-the-connection note, drop the "two values of one type" and
  "`PassClient` would not fix it" passages (both moot), and delete the old file
  per the ADR index's replacement rule. Update the index line. The tree holds one
  counterexample — `2026-07-27-noun-verb-naming.md` is still present and still
  listed "(superseded)" — so follow the rule and say in the PR that the deletion
  is deliberate, or a reviewer reads it as inconsistency.
- **`CLAUDE.md`**: the pass-client bullet (line 349) loses its "the client
  `Register` returns is the application's and unrestricted" sentence and gains
  the rule — a `ControllerClient` exists only for a pass — plus the new link.
- **`README.md`**: the quick-start comment (69–73), the `Register` signature and
  the paragraph under it (110–113), and the controller section (759–761), where
  "can get a `ControllerClient` from `Register`" becomes the `Requeue`
  round-trip. The "registering is the only way to get one, which is what keeps
  status writes limited to the kind's owner" claim gets stronger, not weaker:
  reconciling is now the only way.
- **`docs/TODO.md`**: the `AddEvent` gap above.
- `docs/reconcile-triggers.md` needs nothing — it maps triggers, and no trigger
  moves.

## Risks

- **The events example is the only place we learn what this costs.** If its
  rewrite reads as contorted, that is data about the decision, not about the
  example — say so in the PR rather than forcing it through.
- **A test ported to the helper stops testing the rule it was written for.** The
  helper hands out a client that never dies, so any test whose subject was
  lifetime must be rewritten rather than ported.
- **Nothing else in this repo holds a long-lived client**, so the port is
  mechanical. An out-of-tree caller's is not: for them this is the second
  breaking change in a row to the same surface, and it should lead the release
  notes exactly as the first one did.
