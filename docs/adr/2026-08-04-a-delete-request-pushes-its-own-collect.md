# A delete request pushes its own collect, for a registered kind only

- **Status:** Accepted — implemented in `client.go`, `gc.go`,
  `internal/storeapi/storeapi.go`, `sqlite/store.go`.
- **Date:** 2026-08-04

## Context

The whole deletion path was pull. `Create` enqueued its object at commit;
`Delete` stamped `deletion_requested_at` and scheduled nothing. The same object,
through the same controller, reconciled in microseconds on create and in up to a
GC interval on delete, and a caller could not see why.

A cascade paid the interval per level. `gcCollect` marked owned children and
returned, and the mark was what put them in the next sweep's listing, so a
four-level owner tree took about four sweeps to collect.

## Decision

Two pushes on `Store.AfterCommit`: the delete request enqueues its own object,
and a cascade enqueues the children it marked.

**Both are confined to registered kinds, by routing rather than by a branch.**
Each hook resolves a reconciler and does nothing without one. The alternative —
routing through `deletionAdvance`, which the sweeper uses — collects a
client-only kind inline, and `gcCollect` opens a transaction, cascades to
children and may delete the row. A hook runs on the committer's goroutine after
the outer commit, so `Client.Delete` would have performed the whole subtree's
collect before returning, a cost no caller can predict from the call. A
client-only object is marked and left to the sweeper. Handing that collect to
the sweeper's goroutine through a signal is the follow-up if it is ever measured
to matter.

**Each gate is the store's report of what the write changed.** `Delete` gates on
`marked`, which is true once per object. The cascade gates on
`DeletionCascadeChild.Marked`, which is new surface: the store computed the fact
and discarded it.

`Marked` is reported by the guarded `UPDATE` rather than reconstructed from the
`SELECT` that preceded it. On the sqlite backend the two coincide — `Within` is
`BEGIN IMMEDIATE`, so no writer interleaves — and the write's own answer is used
because it is the source of truth at no cost, and `Store` admits backends that do
not serialize a read against a later write in the same transaction. Gating on "the
row is deletion-pending" is a different matter, and worse than imprecise: `gcCollect` reruns after every
reconcile of a deleting object, so an owner blocked on a finalizer and retrying
under backoff would re-push its entire child set at reconcile rate.

**The cascade queues one hook for the whole set**, over `resolverForPage`. The
per-kind wake beside it is deduped because a wide cascade would otherwise queue
one commit hook per row; a per-child push would have reintroduced exactly that,
each closure re-taking `bh.mu` to resolve the same few kinds.

**Both pushes are immediate** — `signalRequeueNow` and `signalRequeueManyNow` —
because each gate is a write that lands once, so cancelling a pending alarm can
never become a repeat. Throttling the cascade instead would let a child's backoff
alarm absorb the mark, and a ladder can outlast the GC interval; worse, a child's
own reconcile is what cascades to the level below it, so one parked child stalls
the whole subtree. The sweeper's route through `deletionAdvance` is absorbed the
same way, so this is a stall the pull path never beat either.

`DeletionRequestsCreateByName` returns the id it marked, since a name delete has
no id to push. `markForDeletion` already scanned it for the write-log entry.

## Consequences

A cascade advances one level per commit for as long as the levels are registered
kinds. A client-only level still costs a sweep, and the pushes below it cannot be
issued until that level's own collect runs.

`TestIntegrationDeleteTriggersReconcile` now passes via the push and no longer
exercises the sweeper, so `TestIntegrationDeleteCollectsWithoutThePush` marks
through the store to keep the pull path pinned. That is the shape of the rule
here: there is no knob that disables a push, so a pull-path test is one that
issues no push at all.
