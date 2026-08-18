# A ControllerClient exists only for the pass it is handed to, and writes only that pass's object

- **Status:** Accepted — implemented in `beehive.go`, `controller.go`,
  `reconciler.go`.
- **Date:** 2026-08-18

## Context

Beehive concludes a pass by stamping the generation it handed out, so `Settled`
is a checkpoint a consumer can wait on: `ObservedGeneration == Generation`, then
read status. A status write with no pass behind it moves status underneath that
checkpoint, and nothing re-derives it — no pass is owed, no watermark is low, no
driver lists it.

Scoping the `Reconcile` parameter alone left the hole open at the other end:
`Register` also returned a `ControllerClient`, and that one never expired. Two
values of one interface type behaved differently with nothing in the type to say
so, and the difference was a runtime error rather than a compile error.

That client was kept because it was the only write surface an application had
outside a pass — `Client` reads events and conditions but writes neither, and
writes no status at all.

Scoping the *lifetime* left the *target* open. Every method took an `ObjectID`,
and the store's kind scoping only stopped another kind's row: a controller could
still write a sibling of its own kind, which races that sibling's own pass and
lands underneath the same checkpoint the lifetime rule protects. Every call site
in `examples/` already passed `obj.ID`.

## Decision

`Register` returns `error`. `Reconcile`'s parameter is the only way to hold a
`ControllerClient`, and every method fails with `ErrReconcileReturned` once the
pass returns.

No method takes the object's id. The client binds it at construction
(`newPassClient`), and the only ids left in the surface name the *other* end of
an edge: `AddDependency(ctx, toID)`, `DeleteDependency(ctx, toID)`. A sibling
write is not refused at runtime — it cannot be written down.

Reads fail too. "The client you were passed stops working when your reconcile
returns" is a rule a caller remembers; a table of which half still works is not.

The flag clears immediately on return, ahead of beehive's own post-reconcile
writes, so the stamp really is the last write of the pass. Atomic because the
caller it guards against is concurrent by definition. One type carries all of
it: the gate sits on `controllerClientImpl`, which is built per pass, and
`stampObserved` moved to `typedController` rather than becoming the one method
that must not consult the gate.

## Consequences

**A fail-fast, not a barrier.** Calls already in flight are not waited for, so a
goroutine past the check — including one inside `Within` — runs to completion and
may commit either side of the stamp. The restriction catches the pattern, not
every instance of the race. A `Within` still open when the flag flips also holds
the single write connection, so beehive's three post-reconcile writes queue behind
it; nothing is lost, because the stamp's failure path is already non-fatal.

**An application cannot append to an event log outside a pass**, and a pass
cannot append to another object's log. That is the one capability removed rather
than relocated: an event settles nothing, so the argument above does not reach
it. It goes because it arrived on the same client, and carving one method out
would restore the which-half-works table. `docs/TODO.md` carries the gap and the
shape of the fix.

**A declare is now something only the dependent's own pass can make.**
`Edges().Add` with `RelationDependsOn` has one non-test caller and `Client` has
no `AddDependency`, so a client-only kind can never be an edge's source, and an
edge whose source is one cannot be dropped through the package at all. What that
costs is the third case the unconditional `reconcile_owed` stamp was written to
cover — a declare made on another object's behalf while that object's own
reconcile is mid-flight. The stamp stays unconditional for the other two.
`docs/TODO.md` carries this one too.

**The reads answer for the pass's object.** `GetOwner`, `ListDependencies`,
`ListDependents` and `ListOwned` have id-keyed twins on `Client`, so a controller
that needs another object's graph holds one. `HasIncomingEdges` does not, and is
now pass-scoped outright.

**`ErrWrongKind` no longer reaches a controller.** The id a pass client binds is
its own kind's by construction, so only a fabricated client can raise it, and
`Client` reports the store's error as `ErrNotFound`. That leaves `TestClient` —
id-keyed, by design — the only surface a caller can get it from.

**The cleared-finalizer push loses one of its two cases.** A clear landing on a
*sibling's* pass is what binding removes. The other survives: `deleting` is
sampled at load, so a pass that started before the delete request skips its tail
`gcCollect` while the store still reports the clear as freeing a
deletion-pending row. [Its record](2026-08-05-a-cleared-finalizer-pushes-its-own-collect.md)
is narrowed to that case rather than retired.

Everything else an application used the long-lived client for is a round trip
through `Client.Requeue`: keep what you learned in memory, ask for a pass, write
it there.

**The mistake is now a compile error.** A captured client still fails at runtime,
but there is no second client to confuse it with, and no signature that hands one
out.
