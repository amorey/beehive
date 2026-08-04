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

`Marked` is the guarded `UPDATE`'s own answer, never a reconstruction from the
`SELECT` that preceded it. A race may set the flag in between, and then the write
stamps nothing while `!deleting` still claims it did. Gating on "the row is
deletion-pending" is worse than merely imprecise: `gcCollect` reruns after every
reconcile of a deleting object, so an owner blocked on a finalizer and retrying
under backoff would re-push its entire child set at reconcile rate.

**The cascade queues one hook for the whole set**, over `enqueuerForPage`. The
per-kind wake beside it is deduped because a wide cascade would otherwise queue
one commit hook per row; a per-child push would have reintroduced exactly that,
each closure re-taking `bh.mu` to resolve the same few kinds.

The delete request uses `signalRequeueNow` and the cascade the throttled path.
With the gate in place neither can repeat on a pass, so the choice is not
load-bearing; the delete keeps the stronger one because a delete carries new
information and should beat a backoff alarm rather than be absorbed by one.

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
