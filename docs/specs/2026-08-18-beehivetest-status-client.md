# A beehivetest package writes status and conditions outside a pass

- **Status:** Proposed.
- **Date:** 2026-08-18 (against `a4ebf34`)
- **Closes:** [#115](https://github.com/amorey/beehive/issues/115) (nothing
  outside a reconcile can write status, so a test cannot build a stored object
  that has one).

## Problem

A controller reads another kind's status out of the store, and a fixture has to
put one there. In kstack-app a cache controller reads its owner's
`status.server.uid` to decide whether the cache is the active identity, so the
test needs a stored `Cluster` carrying a status — state, not a scenario.

Nothing can build it. Since `Register` stopped handing back a client and
`ControllerClient` became valid only for the pass it is handed to
([ADR](../adr/2026-08-18-a-controller-client-exists-only-for-a-pass.md)), the
only in-API route to a status write is a live `Reconcile`. So a fixture starts
beehive, registers a stub controller holding a map of statuses to park, requeues
the object, and waits on a probe for the pass to land — about 40 lines, a
running reconcile loop behind assertions about rows, and a synchronisation cue
that flakes rather than fails when it is wrong.

### This is narrower than "testing a controller"

Most controller tests do not need any of it. `Controller.Reconcile` takes a
`ControllerClient` (an exported interface) and the object it acts on, so a test
can call `Reconcile` directly against a fake client and a hand-built object: no
store, no beehive, and the assertion lands on what the pass *decided* rather
than on what a row ended up holding. That is the better test wherever it fits,
and it is what a controller's own status writes should be asserted with.

It stops fitting when the pass reads something the parameters do not carry. A
`Client.Get` on another kind inside `Reconcile` still goes to the store when
`Reconcile` is called directly, so the row has to exist and has to have a
status. The alternative — injecting a narrow reader interface into the
controller and faking it — is legitimate design, and a consumer who takes it
needs nothing from this spec. What it gives up is testing against the real
store, which is most of the reason for embedding one.

Two costs on the direct-`Reconcile` route, for whoever weighs them:
`ControllerClient` is 14 methods to fake, and a hand-built `Object` cannot carry
loaded relations — `loaded` and `owner` are unexported (`types.go:176`), so
`obj.Owner()` answers `ErrNotLoaded` and an external test cannot fix it. The
second is a real gap and a separate one; this spec does not close it.

### What already works, and why it is not the answer

`beehive.Store` is a public alias (`types.go:42`) and the caller constructs the
store, so a fixture *can* write status directly, and it is three lines:

```go
b, err := json.Marshal(ClusterStatus{Server: ServerStatus{UID: "server-1"}})
require.NoError(t, err)
require.NoError(t, store.Objects().UpdateStatus(ctx, clusterGK, obj.ID, b, 0))
```

Verified from a module outside this one, so the `internal/` rule applied as it
does for a consumer. It works, and it is still the wrong thing to document:

1. **It is correct in one lifecycle window only.** `ControllerClient`'s writes
   run through `wakeAfter` (`controller.go:129`), which calls
   `signalKindWritten`. The bare store call skips it, so watch tailers and the
   dependency waker do not hear the write and pick it up on the 30s floor tick
   instead. Before `Start` nothing is listening and this is harmless; after
   `Start` it is exactly the "a `Store` call behind the running `Beehive`'s
   back" that `CLAUDE.md` calls unsupported. The fixture author has to know
   which window they are in.
2. **The `0` is a schema version, and it fails silently.** `stampVersion`
   (`sqlite/store.go:1339`) reads an incoming 0 as "no opinion" and keeps the
   stored tag, so the write succeeds and mis-tags the row: bytes this build
   shaped land under whatever version was already there, and the migrator
   converts them again on the next read
   (`ConvertStatus` runs whenever `0 <= from < SchemaVersionStatus()`). A
   fixture gets a corrupt object back and no error anywhere. The correct value
   is `migratorStatusVersion(bh.migratorFor(gk))` (`types.go:443`), which a
   caller cannot reach.
3. **Conditions are not reachable at all.** `Conditions().Set` takes
   `...storeapi.Condition` (`storeapi.go:859`) and `storeapi` is internal, so a
   consumer cannot construct the argument:

   ```
   cannot use beehive.Condition{…} (value of struct type beehive.Condition)
   as storeapi.Condition value in argument to store.Conditions().Set
   ```

   `Condition` is the only *input* type in the whole `Store` interface with that
   problem; the four unaliased `…Result` types are returns, which a caller
   receives with `:=` and reads without naming.
4. **It pins fixtures to plumbing.** `Objects().UpdateStatus(…, []byte, int)` is
   the store contract, not a user surface.

## Goals

- A fixture writes an object's status and conditions in one typed call, with no
  running reconcile loop, no stub controller and no synchronisation cue.
- The same call is correct before `Start` and while the beehive runs — the wake
  fires and no invariant breaks. Not that the write wins a race with a live
  pass; see below.
- The seam says "not for production" in its name.
- The docs send a reader to the cheapest thing that works, which is usually not
  this package.

## Non-goals

- Re-opening `ControllerClient`. Nothing here hands out a durable one, and the
  client cannot be passed where a `ControllerClient` is expected. Narrowing
  `ControllerClient`'s own signatures so a pass can only write the object it was
  handed is a separate proposal, and this package is unaffected by it: writing an
  arbitrary id is what a fixture is for.
- Writing spec, edges or events. `Client` already writes spec and edges;
  appending to an event log out of band stays the open capability, in
  [`TODO.md`](../TODO.md).
- Making `beehive.Store` externally implementable. Five of its methods name
  internal types, so it is usable but not implementable from outside the module.
  A real question, and a separate one — this spec deliberately does not widen
  the public store surface, and adds no public `Condition` alias.
- Any change to who owns the generation handshake. The client never stamps
  `observed_generation`.
- Making a hand-built `Object` carry loaded relations, which is the other half
  of "call `Reconcile` directly" and is not closed here.

## Design

### The surface

A new package at `github.com/amorey/beehive/beehivetest`:

```go
// Package beehivetest builds store state that only a controller can otherwise
// write. For fixtures; not for production code.
package beehivetest

// NewClient returns a client for gk's status and conditions, valid for as long
// as bh is. Needs no registered controller and no running beehive.
func NewClient[Status any](bh *beehive.Beehive, gk beehive.GroupKind) *Client[Status]

func (c *Client[Status]) DeleteCondition(ctx context.Context, id beehive.ObjectID, conditionType string) error
func (c *Client[Status]) SetCondition(ctx context.Context, id beehive.ObjectID, cond beehive.Condition) error
func (c *Client[Status]) SetConditions(ctx context.Context, id beehive.ObjectID, conds []beehive.Condition) error
func (c *Client[Status]) UpdateStatus(ctx context.Context, id beehive.ObjectID, status Status) error
```

That is `ControllerClient`'s status half entire, minus `AddEvent`.
`DeleteCondition` is here because a fixture that can set a condition and not
clear one cannot build the state a controller reaches by clearing it.

The verbs carry the names `ControllerClient` gives them and mean the same
things, so fixture code reads like controller code:

```go
c := beehivetest.NewClient[ClusterStatus](bh, clusterGK)
require.NoError(t, c.UpdateStatus(ctx, obj.ID, ClusterStatus{Server: ServerStatus{UID: "server-1"}}))
```

Only `Status` is a type parameter. `Spec` is unused by every verb here, so the
client does not take one.

### The name

`Client`, not `ControllerClient`: the behaviour is controller-like, but this is
not a controller's client and must not read as one. It is also the name that
survives narrowing `ControllerClient` to the object under reconcile, which would
leave the two surfaces spelled alike and scoped differently — an id parameter is
the whole point here, since a fixture writes objects it is not reconciling.

`beehive.Client` is the other neighbour, and this is deliberately not that
either. It writes only what the spec/status split reserves for controllers; the
reads and the spec writes stay on `beehive.Client`, which already works in a
fixture. The package qualifier carries the distinction, as `httptest.Server`
does against `http.Server`.

### When to reach for it

In order, cheapest first:

1. **Call `Reconcile` directly** with a fake `ControllerClient` and a hand-built
   object. Asserts what the pass decided. No store, no beehive, no package.
2. **Inject a narrow reader** for the cross-kind reads and fake that too, if the
   controller's dependencies are worth naming as an interface.
3. **This package**, when the test is meant to run against the real store and
   the state it needs is a status. That is the case it exists for; it is not the
   tool for asserting what a controller writes.

The package doc says this, because a seam that is easier to reach than the
cheaper options will be reached for first.

### Why a separate package rather than a method on `Beehive`

`Beehive` has one exported method today (`Start`). A test seam on it would be
public surface a production caller reaches by tab-completion, which is what
[#115](https://github.com/amorey/beehive/issues/115)'s option 2 was trying to
avoid. A package name carries the warning instead — the `net/http/httptest`
arrangement.

The package also sits under `github.com/amorey/beehive/`, so Go's internal rule
lets it import `internal/storeapi` and construct `storeapi.Condition`. That is
what makes conditions reachable with no public alias.

### The seam

`beehivetest` is a different package, so it cannot reach `bh.store`,
`bh.migratorFor` or `bh.signalKindWritten`. `internal/storeapi` cannot import
`beehive` either — `beehive` imports it. So the handoff is a hook in a new
internal package, set by `beehive` at init:

```go
// internal/testseam/testseam.go
package testseam

// Writer is what beehivetest needs from a *beehive.Beehive. The status blob is
// already marshalled: the seam is non-generic on purpose, so the type parameter
// stays in beehivetest.
type Writer interface {
	DeleteCondition(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, conditionType string) error
	SetConditions(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, conds ...storeapi.Condition) error
	UpdateStatus(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, status []byte) error
}

// Open is set by package beehive's init and read by beehivetest. The parameter
// is any because this package cannot import beehive without a cycle.
var Open func(bh any) (Writer, bool)
```

The two methods live on `*Beehive`, unexported, beside the pass client's:

```go
func (bh *Beehive) fixtureUpdateStatus(ctx context.Context, gk GroupKind, id ObjectID, status []byte) error {
	if err := bh.store.Objects().UpdateStatus(ctx, gk, id, status, migratorStatusVersion(bh.migratorFor(gk))); err != nil {
		return err
	}
	bh.signalKindWritten(ctx, gk)
	return nil
}
```

So schema-version resolution and the commit wake stay in `beehive`, next to the
invariants they belong to, and `beehivetest` only marshals and converts.

A hook rather than `func (bh *Beehive) TestWriter() testseam.Writer`, because
the method leaks: an external caller can call a method returning an internal
type without naming that type, and `GroupKind`/`ObjectID` are public aliases
(`types.go:27,30`), so `bh.TestWriter().UpdateStatus(ctx, gk, id, blob)` would
compile outside the module and hand a production caller the raw blob write.

`Open` returning `(Writer, bool)` rather than panicking inside keeps the failure
at the caller: `NewClient` panics with a message naming a nil `*Beehive`, which
is the only way the assertion fails.

`Open` must reject a **typed** nil explicitly — `NewClient(nil, gk)` passes an
interface that is itself non-nil while holding a nil `*Beehive`, so
`bh.(*Beehive)` alone succeeds and the panic arrives later as a nil dereference
inside the first write:

```go
bh, ok := v.(*Beehive)
if !ok || bh == nil {
	return nil, false
}
```

### What the client does and does not do

- **Status.** `json.Marshal`, then `fixtureUpdateStatus`. Same marshalling as
  `ControllerClient.UpdateStatus` (`controller.go:141`), so a status that
  marshals to the stored bytes writes nothing.
- **Conditions.** The same five-field copy the pass client makes
  (`controller.go:160-169`): `Type`, `Status`, `Reason`, `Message`, `Liveness`.
  `Unconfirmed` and the two stamps are set by the store on read and ignored on
  write, so they are not copied. A type named twice is
  `ErrDuplicateConditionType`; an empty slice writes nothing. `DeleteCondition`
  passes straight to `Conditions().Delete`, where a missing condition is a
  no-op.
- **Never `observed_generation`.** The handshake stays beehive's
  ([ADR](../adr/2026-08-18-beehive-owns-the-generation-handshake.md)), so an
  object with a fixture status is still unsettled and the owed pass will
  reconcile it once beehive starts. Documented, because it surprises.
- **A write races a live pass.** On a running beehive, writing the status of an
  object currently being reconciled races that pass's own write, last-writer-wins
  at the store — the ordinary hazard of two writers, not a broken invariant. A
  fixture that parks state on an object its controller is actively settling has
  to sequence that itself.
- **Kind scoping is the store's.** `Objects().UpdateStatus` and
  `Conditions().Set` are already scoped to `gk`: wrong kind is `ErrWrongKind`,
  missing id is `ErrNotFound`. The client adds no check of its own.
- **No lifecycle gate.** The commit wake is what a running beehive needs, and
  the client always emits it, so there is nothing left for a "before `Start`
  only" guard to protect. A stopped beehive fails on the closed store.

## Implementation

1. `internal/testseam/testseam.go` — the `Writer` interface and the `Open` hook.
2. `beehive`: `fixtureUpdateStatus`, `fixtureSetConditions` and
   `fixtureDeleteCondition` on `*Beehive`,
   plus a `fixtureWriter` adapter and the `init` that sets `testseam.Open`. Put
   them in one new file, `testseam.go`, so the seam is one file to read.
3. `beehivetest/beehivetest.go` — `NewClient`, `Client[Status]` and the three
   verbs.

`beehivetest` gets its own `go test` target through the existing `./...`. It is
a real package in the module, so `staticcheck -checks=all` covers it.

## Tests

In `beehivetest/beehivetest_test.go`, package `beehivetest_test` — an external
test package, so the visibility story is exercised rather than assumed. What the
`internal/` rule does to a *consumer* module cannot be tested from in here; the
manual check is in the issue thread.

1. **Status round-trips.** Write, then `Client.Get` reads it back.
2. **The handshake is untouched.** After a write, `ObservedGeneration` is nil and
   `Generation` is unchanged.
3. **Conditions round-trip**, with the store's stamps set and `Unconfirmed`
   false; a duplicate type is `ErrDuplicateConditionType`; an empty slice writes
   nothing (`ResourceVersion` unchanged). `DeleteCondition` removes one and is a
   no-op for a type that is not there.
4. **The schema version comes from the registered `Migrator`.** Reading the
   status back is not enough: the bytes come back identical whether the row was
   tagged 2 or left at its stored version, so the assertion has to be visible in
   the decode. Register a migrator reporting status version 2 whose
   `ConvertStatus` stamps something detectable into the value it converts, write
   through the client, then `Client.Get`: a correctly tagged row does **not**
   run `ConvertStatus`, so an unstamped status proves the tag. Passing 0 leaves
   the stored version, `ConvertStatus` runs, and the stamp appears — so the test
   fails if the client stops resolving the version. `Create` leaves
   `schema_version_status` out of its INSERT (`sqlite/store.go:631`), so a fresh
   row sits at 0 and the first fixture write is the one that distinguishes.
5. **The commit wake fires.** Open a `WatchList` before the write on a *running*
   beehive and require the change inside `testTimeout` (10s). The watch floor is
   30s, so a missing wake times out rather than passing slowly. This is the test
   that justifies the seam.
6. **It works before `Start`**, on a beehive that is never started at all.
7. **Scoping.** Wrong kind is `ErrWrongKind`, missing id is `ErrNotFound`.
8. **Client-only kinds work** — no registered controller anywhere.
9. **A nil `*Beehive` panics** with a message naming the argument — the typed-nil
   case above, which a bare type assertion lets through.

In `testseam_test.go` (package `beehive`), one structural tripwire:
`TestTheTestSeamHasOneProducer`, asserting `testseam.Open` is assigned in
exactly one place in the package, so a second door does not appear quietly.

## Migration

None. Nothing is removed or renamed, and no existing signature changes. A
consumer on the raw store path keeps working; the docs stop pointing there.

## Docs

- **README** — a short "writing status in a test" section that leads with
  calling `Reconcile` directly against a fake `ControllerClient`, then names
  `beehivetest.NewClient` for the case that needs a real stored status, with the
  `observed_generation` note. Nothing about stub controllers.
- **`CLAUDE.md`** — one bullet: the seam, why it is a package rather than a
  method, that it is for cross-kind state read through a real store, and that it
  does not weaken the pass-client invariant.
- **ADR on ship** — a new record; the pass-client ADR gains a line pointing at
  it, since "there is no other way to hold one" now has a named exception for
  fixtures.
- **`docs/specs/README.md`** — in flight now, deleted on ship.
