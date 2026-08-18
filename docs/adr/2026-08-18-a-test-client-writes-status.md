# A TestClient writes status and conditions outside a pass

- **Status:** Accepted — implemented in `testclient.go`.
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

`NewTestClient[Status](bh, gk)` returns a `*TestClient[Status]` with
`UpdateStatus`, `SetCondition`, `SetConditions` and `DeleteCondition` — the pass
client's status half, minus `AddEvent`. It needs no registered controller and no
running beehive, and it writes through the same `kindWriter` a pass does, so the
schema version and the commit wake are resolved in one place and it is correct on
both sides of `Start`.

**It is the last resort, not the first.** A controller test that needs only the
object it is handed should call `Reconcile` directly against a fake
`ControllerClient`: no store, no beehive, and the assertion lands on what the
pass decided. `TestClient` is for what that cannot cover — a pass reading another
kind's status out of a real store.

**In package `beehive`, not a `beehivetest` sub-package.** A separate package
would have to reach `bh.store`, `bh.migratorFor` and `bh.kindWriteHub`, none of
which are exported; the only ways across are an exported method returning an
internal type — which an external caller can still call, since `GroupKind` and
`ObjectID` are public aliases — or a hook in an internal package set by `init`.
Both buy exactly one thing over living in the root: the warning sits in a package
name instead of an identifier. Neither prevents production use, because the
constructor is exported either way. `TestClient` carries the warning in the name
it is called by, and costs no seam, no `any` parameter, no typed-nil trap and no
tripwire guarding a second producer. A generic *method* would not work
(`bh.UpdateStatusForTest`), but a package-level generic function does, and
`NewClient` is already one.

## Consequences

The handshake is untouched: nothing here stamps `observed_generation`, so an
object given a fixture status is still unsettled and the owed pass reconciles it
once beehive starts.

On a running beehive a fixture write races the object's own pass, last-writer-
wins at the store. The ordinary hazard of two writers, not a broken invariant; a
fixture parking state on an object its controller is actively settling sequences
that itself.

Two names join the root package's godoc, which is the price of not having a
package name to hide behind. In exchange the `Condition` conversion is shared
with the pass client (`conditionsToRaw`) rather than copied across a package
boundary, which is what a sub-package forced.

`AddEvent` is deliberately absent — appending to an event log out of band is the
capability the pass-client change removed rather than relocated, and it stays in
[`TODO.md`](../TODO.md). A hand-built `Object` still cannot carry loaded
relations (`loaded` and `owner` are unexported), so a controller reading
relations off its object is not testable by calling `Reconcile` directly. That
is the other half of the same gap and is not closed here.
