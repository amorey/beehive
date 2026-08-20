# Conclude a pass in one transaction

- **Status:** Proposed, not decided. The saving is small and the failure
  semantics change; see the question at the end.
- **Date:** 2026-08-20
- **Depends on:** nothing.

## Why

A settled reconcile ends with three separate writes, each its own transaction
(`reconciler.go:140`, `:156`, `:166`):

1. `ReconcileOwed().Decrement` — one `UPDATE`.
2. `Dependencies().WatermarkSet` — one `INSERT ... ON CONFLICT`.
3. `Objects().SetObservedGeneration` — a `Within` holding a read, a version
   draw, a log append and an `UPDATE`.

Three transactions, three commits, for what is one event: this pass concluded.
Grouping them costs one `BEGIN`/`COMMIT` instead of three, about 20 µs a pass on
the numbers in
[the read transaction](../adr/2026-08-20-a-read-that-groups-is-a-read-transaction.md).

## The question to settle first

Today each write fails independently and is backstopped independently. The
comments at each call site lean on that:

- a failed decrement is "left to the backstop rather than retried under backoff";
- a failed watermark write "only over-reports staleness", never strands;
- a failed generation stamp "only leaves the object unsettled".

Grouping them means a failed watermark write also rolls back the generation
stamp, so the object stays unsettled and the whole pass runs again.

That is arguably the better contract — a pass either concludes or it does not,
and there is no state where beehive thinks the object is settled but has
forgotten what it was settled against. It is also a different contract from the
one three ADRs currently describe.

**So: is "the pass concludes atomically" worth re-deriving those three failure
arguments?** If yes, build this. If no, close the spec; 20 µs a pass does not buy
a rewrite of the recovery story.

## The change, if taken

One `Within` in `typedController.reconcile` around the three writes, entered
only when at least one of them will run. The three store calls are unchanged —
they already join an ambient transaction.

```go
// One transaction: a pass concludes or it does not. A partial conclusion would
// leave the object settled against a watermark it never recorded.
err := t.bh.store.Within(ctx, func(ctx context.Context) error { ... })
```

The error handling collapses to one site. It stays a warning, not a failure: a
pass that ran is not undone by failing to record that it ran, and the object is
left unsettled for the next pass to redo.

## Edge cases the implementer would otherwise guess at

- **GC stays outside.** `gcCollect` deliberately "runs in its own transaction
  over the controller's committed writes, so a finalizer the controller just
  cleared is visible". Pulling it in would break that.

- **The controller's own writes stay outside.** They committed before this block
  runs, which is what the `ControllerClient` contract promises.

- **`ctx.Err()` handling moves up.** Three sites check it to tell shutdown from
  fault; there is one now, and it must keep the same distinction.

- **The gate on entering.** A pass with no owed count, no dependencies and an
  unchanged generation writes nothing. Do not open a transaction to discover
  that: check the three conditions first, as the three `if`s already do.

- **`SetObservedGeneration` keeps its own `Within`**, which becomes a savepoint.
  Nothing to change; it is already written to be called either way.

## Tests

In `reconciler_test.go`:

- A settled pass with an owed count and a dependency takes one write
  transaction, not three. Assert on the store's `txCount`.
- A failing watermark write leaves the object unsettled and leaves the owed count
  standing — the new behaviour, pinned.
- A pass that writes nothing takes no transaction.
- A cancelled context during the block is logged as shutdown, not as a fault.
- The existing tests for each of the three writes still pass; if one of them
  asserts an independent failure, it is the test that has to change, and the
  change is the decision above.

## On ship

ADR: **a pass concludes in one transaction**, superseding the independent-failure
paragraphs in
[dependency watermarks](../adr/2026-07-29-dependency-watermarks.md),
[the stale-dependents cursor](../adr/2026-08-03-stale-dependents-cursor.md) and
[the generation handshake](../adr/2026-08-18-beehive-owns-the-generation-handshake.md).
All three describe live code, so each needs an edit rather than a pointer.

`CLAUDE.md`'s "Reconcile is not transactional" bullet needs a qualifier: the
controller's writes still commit one by one, but beehive's own conclusion does
not.
