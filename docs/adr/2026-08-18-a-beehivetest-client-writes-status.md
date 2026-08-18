# A beehivetest client writes status and conditions outside a pass

- **Status:** Accepted — implemented in `beehivetest/`, `internal/testseam/`,
  `testseam.go`.
- **Date:** 2026-08-18

## Context

A controller that reads another kind's status needs a stored one to read. Since
a `ControllerClient` exists only for the pass it is handed to
([ADR](2026-08-18-a-controller-client-exists-only-for-a-pass.md)), the only
in-API route to a status write is a live `Reconcile` — so a fixture had to start
beehive, register a stub controller parking statuses, requeue, and wait on a
probe for the pass to land.

Writing through `Store` directly is reachable, since `beehive.Store` is a public
alias and the caller constructs the store, but it is correct in one window only:
it skips the commit wake, so above `Start` it is the store call behind a running
beehive's back. It also passes the status schema version by hand, where 0 means
"keep the stored tag" — the write succeeds and mis-tags the row. Conditions are
not reachable at all: `Conditions().Set` takes `storeapi.Condition`, which an
external package cannot name.

## Decision

`beehivetest.NewClient[Status](bh, gk)` returns a `*beehivetest.Client[Status]`
with `UpdateStatus`, `SetCondition`, `SetConditions` and `DeleteCondition` — the
pass client's status half, minus `AddEvent`. Both paths write through the same
non-generic `kindWriter`, so the wake obligation has one site rather than one per
caller. It needs no registered controller
and no running beehive, and it resolves the schema version and emits the commit
wake itself, so it is correct on both sides of `Start`.

**It is the last resort, not the first.** A controller test that needs only the
object it is handed should call `Reconcile` directly against a fake
`ControllerClient`: no store, no beehive, and the assertion lands on what the
pass decided. This package is for what that cannot cover — a pass reading
another kind's status out of a real store.

**A separate package, not a method on `Beehive`.** A method leaks: an external
caller can call a method returning an internal type without naming it, and
`GroupKind`/`ObjectID` are public aliases, so `bh.TestWriter().UpdateStatus(ctx,
gk, id, blob)` would compile outside the module. The seam is a hook in
`internal/testseam` that `beehive` sets in `init`, which keeps `Beehive`'s
exported surface at `Start` alone and puts the warning in the package name.
`beehivetest` sits under the module path, so it may import `internal/storeapi`
and construct `storeapi.Condition` — which is why conditions work with no public
`Condition` alias and no widening of `Store`.

`Open` takes `any` (the seam cannot import `beehive` without a cycle) and must
reject a **typed** nil: `NewClient(nil, gk)` passes a non-nil interface holding a
nil `*Beehive`, so the assertion alone admits it and the panic would arrive later
as a nil dereference.

## Consequences

The handshake is untouched: nothing here stamps `observed_generation`, so an
object given a fixture status is still unsettled and the owed pass reconciles it
once beehive starts.

On a running beehive a fixture write races the object's own pass, last-writer-
wins at the store. The ordinary hazard of two writers, not a broken invariant; a
fixture parking state on an object its controller is actively settling sequences
that itself.

`AddEvent` is deliberately absent — appending to an event log out of band is the
capability the pass-client change removed rather than relocated, and it stays in
[`TODO.md`](../TODO.md). A hand-built `Object` still cannot carry loaded
relations (`loaded` and `owner` are unexported), so a controller reading
relations off its object is not testable by calling `Reconcile` directly. That
is the other half of the same gap and is not closed here.
