# A ControllerClient writes only the object of its pass

- **Status:** Planned
- **Date:** 2026-08-18
- **Ships as:** an amendment to
  [A ControllerClient exists only for the pass it is handed to](../adr/2026-08-18-a-controller-client-exists-only-for-a-pass.md)

## Decision

Every `ControllerClient` method drops its `ObjectID`. The client binds the id of
the object its pass was handed, and acts on that object alone.
`AddDependency`/`DeleteDependency` keep one id — the *other* end of the edge.

Two things force it for the writes. The kind scoping stops a controller writing
another *kind*'s row; nothing stops it writing another object of its *own* kind,
which races that object's pass. And beehive concludes a pass by stamping
`observed_generation` on the object it handed out, so a sibling status write is
a status write no handshake covers: it lands, and reports nothing. The
signature currently permits what the design forbids.

The argument is already dead weight. Every call site in `examples/` passes
`obj.ID`, all thirteen of them.

**The dependency verbs are a separate judgement, and this is a real removal.**
A declare on another object's behalf is a documented capability, not an
accident of the signature: `README.md:848` names "a declare made on another
object's behalf while that object's own reconcile is mid-flight" as one of the
three interleavings the unconditional `reconcile_owed` stamp exists to cover.
It goes anyway, because a cross-object declare from a controller that is not the
dependent's own is precisely the race the rest of this change closes. The
[Removed](#removed-with-no-replacement) section states what that costs; nothing
below treats it as a side effect.

## Surface

```go
type ControllerClient[Status any] interface {
	AddDependency(ctx context.Context, toID ObjectID) error
	AddEvent(ctx context.Context, event EventSpec) error
	DeleteCondition(ctx context.Context, conditionType string) error
	DeleteDependency(ctx context.Context, toID ObjectID) error
	DeleteFinalizer(ctx context.Context, finalizer string) error
	GetOwner(ctx context.Context) (ObjectRef, bool, error)
	HasIncomingEdges(ctx context.Context) (bool, error)
	ListDependencies(ctx context.Context) ([]ObjectRef, error)
	ListDependents(ctx context.Context) ([]ObjectRef, error)
	ListOwned(ctx context.Context) ([]ObjectRef, error)
	SetCondition(ctx context.Context, condition Condition) error
	SetConditions(ctx context.Context, conditions []Condition) error
	UpdateStatus(ctx context.Context, status Status) error
	Within(ctx context.Context, fn func(ctx context.Context) error) error
}
```

`controllerClientImpl` gains an `id ObjectID` field, bound by
`newPassClient[Status](bh, gk, id)`. The reconcile loop passes `obj.ID` at
`reconciler.go:122`, where the object is already in hand.

**`TestClient`'s four signatures do not change.** It keeps its `ObjectID`
arguments and builds a bound pass client per call, so there is still one
implementation of every verb:

```go
type TestClient[Status any] struct {
	bh *Beehive
	gk GroupKind
}

func (t *TestClient[Status]) client(id ObjectID) *controllerClientImpl[Status] {
	return newPassClient[Status](t.bh, t.gk, id)
}

func (t *TestClient[Status]) UpdateStatus(ctx context.Context, id ObjectID, status Status) error {
	return t.client(id).UpdateStatus(ctx, status)
}
```

It no longer holds one client, it makes one per call — none of them ever ended.
That falsifies three comments that say it *holds* a client nothing ends:
`controller.go:99` (`newPassClient`'s doc), `testclient.go:24`, and `CLAUDE.md`'s
`TestClient` bullet. All three, plus
[the TestClient ADR](../adr/2026-08-18-a-test-client-writes-status.md), have to
say "builds" instead. The property they were pinning is unchanged: `live()`
passes because only the reconcile loop calls `end()`.

## Removed with no replacement

Deliberate, and each needs an entry in `docs/TODO.md` rather than a mention in
passing:

- **A declare on another object's behalf.** `Edges().Add` with
  `RelationDependsOn` has exactly one non-test caller — `controller.go:232` —
  and `Client` has no `AddDependency`, so binding the source makes the
  dependent's own controller, during its own pass, the only producer of a
  `depends_on` edge in the package. Two consequences follow, and the spec
  accepts both:
  - **A client-only kind can never be the source of an edge.** It can still be a
    *target*: the waker scans the whole write log, so an unregistered kind wakes
    its dependents as before. Only the outgoing direction closes.
  - **`DeleteDependency` binds the same way**, so an edge left in a store by an
    earlier build, whose source is a client-only kind, can never be dropped
    through the package. It pins its target against collection through the
    RESTRICT for good.
- **The unconditional stamp's rationale narrows to two of its three cases.**
  `README.md:848` must strike the mid-flight cross-object declare. The other
  two — a target change landing between the read and the edge's commit, and a
  crash before the wake is serviced — are untouched, so the stamp stays
  unconditional.
  `TestReconcileMidPassDeclareLeavesTheDependentOwed`
  (`reconciler_test.go:3144`) declares through `h.store.Edges().Add` directly
  rather than through the client, so the strand it pins survives the signature
  change and the test needs no edit.
- **Appending to another object's event log during a pass.** `docs/TODO.md:420`
  records the *other* half of this — appending outside a pass at all — and
  proposes the same fix, an id-keyed `Client.AddEvent`. Widen that entry to
  carry both losses; do not cite it as though it already covers this one.
- **Writing another object's status, conditions or finalizers.** No entry: this
  is the race the change exists to close, and there is nothing to restore.

## Edge cases

- **`AddDependency`'s enqueue routes by `c.gk`.** The source is this pass's
  object, so `EdgesAddResult.From` and the caller's kind are now the same value;
  reading the store's report to learn a kind the client already holds would be
  indirection no test can falsify. `DeleteDependency` still routes by `res.To`:
  the target is cross-kind.
- **`EdgesAddResult.From` stays.** It loses its production reader but keeps two
  in test scaffolding — `addEdge` in `testutils_test.go:1521` and
  `sqlite/store_test.go:4234`, which declare cross-kind edges straight to the
  store and need the source's kind for the kind-scoped
  `ReconcileOwed().Decrement`. Threading a `GroupKind` through those helpers'
  166 call sites to delete one field is the worse trade. Adjust the field's doc:
  the beehive caller can now assume its own kind, the scaffolding cannot.
- **`ErrWrongKind` becomes unreachable through `ControllerClient`.** Its last
  public producer is `TestClient`, which still takes an id and does not hide the
  error the way `clientImpl.hideWrongKind` does. Retarget the sentinel's doc
  (`controller.go:25`) and the `AddEvent` sentence on `Client.GetLatestEvent`
  (`client.go:149`). Whether an exported sentinel that only a test helper can
  produce should stay exported is a separate question; out of scope here.
- **`AddDependency(ctx, obj.ID)`** is still expressible and still stamps
  nothing. Self-edges are skipped in the store.
- **Reads narrow to the pass object**, which is what `ControllerClient`'s own
  comment already claims ("a controller reasons about its own object's
  relationships"). They become the lazy path for exactly what a `LoadOption`
  loads eagerly onto `Object`.
- **A controller that needs another object's graph holds a `Client`**, which
  carries `GetOwner`, `ListDependencies`, `ListDependents` and `ListOwned`
  id-keyed. `HasIncomingEdges` has no `Client` counterpart, so it is the one
  read that becomes strictly pass-scoped; note it in `docs/TODO.md` rather than
  widening `Client` here.

## Tests

New, in `controller_test.go`:

- `TestPassClientBindsThePassObject` — two objects of the kind exist; the pass
  writes status and a condition; assert the sibling is untouched. Pins the
  binding, which a wrong-id impl would otherwise pass every other test with.
- `TestAddDependencyDeclaresFromThePassObject` — assert the edge runs
  `obj → toID` and that `reconcile_owed` landed on `obj`.
- `TestDeleteDependencyDropsFromThePassObject`.
- `TestDeleteFinalizerTargetsThePassObject` — including that the cleared-last
  push is routed to `obj`.

Changed:

- The `ErrReconcileReturned` table (`controller_test.go:1507`) keeps every
  method and loses the ids it passes for other objects.
- `TestAddDependencyEnqueueRoutesByTheSourcesKind` (`controller_test.go:804`)
  **is deleted**, with `declareFixture` if nothing else uses it. Its premise is
  the capability struck above; with the source bound, the routing it pins cannot
  be got wrong.
- ~187 call sites across the root test files. `TestClient`'s 13 do not move, and
  remain the coverage for the kind-scoped store writes' `ErrWrongKind` paths.
- No `sqlite` test changes: `EdgesAddResult.From` survives.

## Cycles

Each is red (the test moves to the new signature) then green (the method
follows), and ends with `go vet ./...`, `go test ./...` and a commit.

1. Bind the id on `controllerClientImpl`, thread it through `newPassClient`,
   `reconciler.go` and `TestClient`; narrow `UpdateStatus`.
2. `SetCondition`, `SetConditions`, `DeleteCondition`.
3. `AddEvent`.
4. `DeleteFinalizer`.
5. `AddDependency`, `DeleteDependency`; route by `c.gk`; delete the routing
   test; re-doc `EdgesAddResult.From`.
6. `GetOwner`, `HasIncomingEdges`, `ListDependencies`, `ListDependents`,
   `ListOwned`.
7. Docs: `README.md` — the interface listing at `:716`, the walkthrough call
   sites (`:57`, `:571`), and striking `:848`'s third case; `:784` is
   `TestClient` and does not move. Then all five `examples/`, `CLAUDE.md`, the three `ErrWrongKind` and
   `TestClient` comment sites, `docs/reconcile-triggers.md` (the trigger row at
   `:55` and the test name at `:327`), and `docs/TODO.md` (widen the `AddEvent`
   entry, add the client-only-source and `HasIncomingEdges` entries).
8. ADRs: amend
   [exists only for a pass](../adr/2026-08-18-a-controller-client-exists-only-for-a-pass.md)
   with the binding,
   [a test client writes status](../adr/2026-08-18-a-test-client-writes-status.md)
   with the per-call construction, and
   [stamp every new dependency edge](../adr/2026-07-29-stamp-every-new-dependency-edge.md)
   with the narrowed third case. Update the ADR index title if it changes.
   Delete this spec.

Breaking: the commit type is `feat!`.
