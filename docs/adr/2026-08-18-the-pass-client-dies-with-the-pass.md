# The ControllerClient passed to Reconcile stops working when it returns

- **Status:** Accepted — implemented in `controller.go`, `reconciler.go`.
- **Date:** 2026-08-18

## Context

Now that [beehive writes the handshake](2026-08-18-beehive-owns-the-generation-handshake.md),
a pass has a definite end, and `Settled` is a checkpoint a consumer can wait on:
`ObservedGeneration == Generation`, then read status.

A controller that captures the client it was passed and writes from a goroutine
afterwards breaks that checkpoint. Status moves with no pass behind it, so a
consumer that waited for the object to settle reads status the settling pass had
not finished producing. Nothing re-derives it: no pass is owed, no watermark is
low, no driver lists it.

The wider framing — "out-of-band status writes are unsound" — proves too much.
`Register` returns a `ControllerClient` precisely so an application's background
work can write status, documented since before this change. That client is
app-owned, and beehive makes no claim about which generation such a write belongs
to.

## Decision

Scope the *parameter*, not the category. `Reconcile` receives a
`scopedControllerClient` wrapping the shared client behind an `atomic.Bool`;
every method fails with `ErrReconcileReturned` once the pass ends. The client
`Register` returns is untouched.

Reads fail too. "The client you were passed stops working when your reconcile
returns" is a rule a caller remembers; a table of which half still works is not.

The flag clears immediately on return, ahead of beehive's own post-reconcile
writes, so the stamp really is the last write of the pass. Atomic because the
caller it guards against is concurrent by definition.

## Consequences

**A fail-fast, not a barrier.** Calls already in flight are not waited for, so a
goroutine past the check — including one inside `Within` — runs to completion and
may commit either side of the stamp. The restriction catches the pattern, not
every instance of the race. A `Within` still open when the flag flips also holds
the single write connection, so beehive's three post-reconcile writes queue behind
it; nothing is lost, because the stamp's failure path is already non-fatal.

**Two values of one interface type now behave differently**, with nothing in the
type to tell them apart. The doc comments on `Register` and `Controller.Reconcile`
carry the distinction, and conflating them gives a runtime error rather than a
compile error.

A distinct `PassClient[Status]` type would not fix that on its own: Go interface
values with identical method sets are assignable both ways, so
`var c ControllerClient[S] = passClient` still compiles and the same mistake still
builds. Real enforcement needs a marker method or a different method set — a
larger change than the rename it resembles, left for a separate decision.

**The only change in the set a compiler cannot find**, so it leads the release
notes. The fix is the `Register` client, or keeping the result in memory and
calling `Client.Requeue`.
