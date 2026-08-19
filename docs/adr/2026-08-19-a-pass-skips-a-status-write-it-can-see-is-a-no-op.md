# A pass skips a status write it can see is a no-op

- **Status:** Accepted — implemented in `statusbaseline.go`, `controller.go`,
  `reconciler.go`, `sqlite/store.go`.
- **Date:** 2026-08-19

## Context

A controller that reports the same status it reported last time writes nothing,
but finding that out cost a transaction. `Objects().UpdateStatus` reads the
stored bytes, compares them, and skips the write, all inside `Within`.

The cost is not lock contention — one process is the store's sole writer, so the
`BEGIN IMMEDIATE` is uncontended. It is that the pool is one connection, so
BEGIN, SELECT and COMMIT occupy the only connection every other reader and
writer is queued behind, to conclude that nothing changed.

For a level-triggered system that is the common case. One change wakes every
dependent, each re-reads current state, and most find what they found last pass.
Controllers were hand-rolling the check the store already does.

## Decision

**The pass compares in memory first.** It is handed its object's stored status
at dispatch, so `ControllerClient.UpdateStatus` compares the marshalled status
against those bytes and returns without calling the store on a match
(`statusBaseline`, `statusbaseline.go`).

**The store's compare stays, in front of nothing and behind everything.** It is
what keeps `AdminClient` and every direct store caller correct. The in-memory
check must remain a **strict subset** of it: a false negative costs a
transaction, a false positive loses a write silently. The store's predicate
includes `stampVersion`'s "incoming 0 means keep stored", which the pass does not
model — that gap errs safe.

**`Objects().UpdateStatus` reports `changed`.** The pass's `signalKindWritten` is
gated on it, so a status write the store declined no longer wakes every tailer
for the kind and the dependency waker.
`ControllerClient.UpdateStatus` keeps returning `error` alone: nothing in-tree
reads a bool there, and a store write returns only what a caller reads.

**An outstanding write poisons the fast path rather than updating it.**
`AfterCommit` hooks run only at the outermost commit, so inside a controller's
`Within` an advance is invisible to sibling calls — writing `A` then writing back
the loaded value would drop the second write. So `claim` marks a write in flight
and refuses to match while any is outstanding; the commit hook promotes and
clears its own. The hook carries its own bytes rather than reading shared state,
because by commit time a later write may have been issued and failed.

Neither an error nor a store-side no-op promotes. Promoting a `!changed` write at
commit would in fact be sound, but leaving both paths outstanding keeps the
invariant to one sentence: `outstanding` returns to zero only when every issued
write committed.

## Consequences

**A skip cannot report a collected object.** The store's scoped read is what
turns a row collected mid-pass into `ErrNotFound`; a skipped call never reads. A
controller re-reporting an unchanged status for a collected object now gets `nil`.
The reconcile loop already treats a mid-pass collect as a no-op success.

**Soundness rests on `objects.status` having one writer.** The create sets it to
`NULL` and `Objects().UpdateStatus` is the only statement that moves it after
that. `TestObjectStatusIsWrittenInOnePlace` pins it; a backfill or repair verb
that wrote the column would break the fast path silently.

**Not extended to `SetConditions`.** `conditionUnchanged` deliberately refuses to
skip a matching condition when `livenessStale` fires, and that predicate reads
the store's `processStart`, which a pass cannot evaluate.

**Not available to `Client.Update`.** It returns the updated object, so it must
reach the store even when nothing changed. Its no-op is still discovered inside
`updateSpec`'s transaction.

Per write the added cost is one `bytes.Equal`, one mutex, and one closure. A
failed or store-side-no-op write drops that pass back to the old path for the
rest of its life.
