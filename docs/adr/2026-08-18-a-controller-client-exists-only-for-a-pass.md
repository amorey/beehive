# A ControllerClient exists only for the pass it is handed to

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

## Decision

`Register` returns `error`. `Reconcile`'s parameter is the only way to hold a
`ControllerClient`, and every method fails with `ErrReconcileReturned` once the
pass returns.

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

**An application cannot append to an event log outside a pass.** That is the one
capability removed rather than relocated: an event settles nothing, so the
argument above does not reach it. It goes because it arrived on the same client,
and carving one method out would restore the which-half-works table. `docs/TODO.md`
carries the gap and the shape of the fix.

Everything else an application used the long-lived client for is a round trip
through `Client.Requeue`: keep what you learned in memory, ask for a pass, write
it there.

**The mistake is now a compile error.** A captured client still fails at runtime,
but there is no second client to confuse it with, and no signature that hands one
out.
