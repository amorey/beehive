# The dependency waker recovers missed wakes by replaying a resource_version watermark

- **Status:** Accepted — implemented in `beehive.go` (the waker), `sqlite/watch.go`
  and `sqlite/store.go` (the store side).
- **Date:** 2026-07-27
- **Supersedes:** [Dependency-wake failures escalate the catchup tick](2026-07-27-dependency-wake-escalation.md)

## Context

The waker has three loss points: a failed `EdgesGroupIncomingByID`, a closed change
stream, and a failed `ObjectWritesSubscribe`. All three were repaired by escalating a
periodic pass into a full one — and that repair could not run at the default
configuration.

The escalation set `resyncOnce`/`resyncAlways` on every reconciler, and those flags were
read in exactly one place: inside the catchup ticker's case. With `catchupInterval <= 0`
and `resyncInterval <= 0` — and `resyncInterval` already defaults to 0 — the flags were
set and never read. `hasPeriodicPass` detected precisely this condition and reported it to
the operator without fixing it.

The miss was permanent, not slow. A dependent that has settled is invisible to every
owed-work listing, because its own generation never moved, so nothing else ever came back
for it. A failed subscribe took down every dependency wake for every kind for the life of
the process, while the control plane continued to look healthy.

## Decision

Repair by replaying what was missed, not by re-deriving everything.

**The waker holds a watermark** — the highest `resource_version` it has finished
processing. `resource_version` is drawn from a standalone monotonic counter, never reused
even across physical deletes, and indexed by `idx_objects_rv`: a watch cursor. On a
failed lookup or a re-established subscription the waker asks for everything above the
watermark (`ObjectWritesListSince`, paged, rv-ordered), feeds it through the same wake
path, and advances. Cost is O(changes missed), not O(table).

**The waker drives its own recovery.** A cursor says where to resume; something must still
be running to decide to. Nothing was — a failed subscribe returned, a closed stream ended
the loop — so `serve` now keeps a subscription alive for the life of the control plane,
backing off between attempts. This does not remove the tick dependency so much as move
the timer from the reconciler into the waker: one goroutine, O(missed) work, and no
operator knob whose default disables a correctness repair.

**The escalation is deleted**, not kept as a second line. The waker was its only caller.

### The watermark is a low-water mark

It advances only on batches actually processed, never on receipt — and never past the
oldest write still queued behind the batch. That second condition is not optional: the
hub delivers in *first-touch* order, since a re-written object coalesces into the
queue position it already held. So the highest version in a batch says nothing about
what is still queued below it, and taking it as a resume point would step over changes
that were never processed.

Each batch therefore carries the bound (`ObjectWriteBatch.OldestPending`), read from
the receiver's own backlog head via `conflate.Receiver.Peek` at the moment the batch
is assembled. The cursor commits to one below it, or — when the backlog is empty — to
the highest version actually delivered.

An earlier form inferred the bound from the batch coming back *shorter* than the
backend's cap. That starved: a workload touching more than a batch's worth of distinct
objects with no lull made every batch full, so the cursor never moved at all and the
first dropped subscription replayed essentially the whole table. The bound is now
asked for rather than guessed.

A **replay page** commits differently again: it is version-ordered and complete up to
its own last row, so it advances the cursor by *that* row — never by the staged value,
which may sit far above it. Committing the staged value on a replay page would step
over the whole range between, which is exactly the range staging exists to keep out of
the cursor.

On a failed lookup the waker stops consuming and retries from the watermark. That keeps
the cursor a scalar and removes the second hazard by construction: batches do not arrive
version-ordered across a failure either, so advancing on receipt would let a later batch
that succeeded carry the cursor past an earlier one that did not — skipping exactly the
changes the recovery exists to replay, while passing every "it recovers" test.

Stalling the live consumer is safe *because the hub conflates per object*: a paused
consumer's pending set is bounded by the store's live key set, not by churn.

Batches with nothing to wake still advance it. Deletes carry no dependents and the hub
annihilates unobserved transients, so on a delete-heavy store those are most of the
traffic; a cursor that only moved on wakeable changes would trail arbitrarily far behind
and turn the bounded replay into a whole-table scan.

### The cursor comes back from `ObjectWritesSubscribe`

Returning `(subscription, cursor, error)` makes the initialization order
unrepresentable rather than documented — there is no second call to misorder. The hub
receiver is registered before the cursor is read, so a write landing between the two is
either already in the receiver or above the returned value. This is `snapshotAt`'s
argument for the per-kind streams, which reads the objects and the cursor in one
transaction because *"a separate cursor read could span a write the list itself didn't."*

### The ceiling is on the interval, never the attempts

A waker that gave up is the dead waker this replaces, reached by a slower route. The delay
goes through an injectable seam because the retry loop is now the only recovery path, so
tests must drive it — and must do so without waiting on a real interval.

The counter resets on **duration, not on progress**: a stream resets the backoff only if
it outlived the pause that round would have cost. "It delivered something" cannot be the
test — a store that hands out a subscription, sends one batch and closes it again
satisfies that every round, holding the delay at its floor and resubscribing ten times a
second on the connection every writer shares. Duration says what progress cannot: a
stream that ran longer than the retry it would have cost was a working stream, however
quiet, and one that did not was a flap, however busy.

## Consequences

Three properties this rests on, each asserted rather than assumed:

- **`resource_version` is monotonic in commit order, and publication follows commit
  order.** The first half holds because the store is single-connection — the version is
  drawn inside the write transaction, so with a pool of two a transaction could draw 5
  and commit after one that drew 6. The second half does not come for free: `Commit`
  releases the connection before the post-commit flush runs, so a writer at version 100
  could be preempted and publish after one at 101, handing a cursor-keeping consumer a
  version whose predecessor it has not seen. `Within` therefore holds `publishMu` across
  commit-and-publish. Post-commit *hooks* run outside that lock, because a hook may write
  to the store and would otherwise deadlock; only publication had to be ordered.
- **A missing row means no dependent remains.** `edges.to_id` is `ON DELETE RESTRICT`, so
  a target cannot be removed while anything depends on it. It does not guarantee the
  replayed row still exists — a dependent deleted first cascades its own edge away and
  frees the target — but a row that vanished had no dependents left to strand.
- **Coalescing is not loss.** The hub delivers the latest state per object and the waker is
  level-triggered, so replaying "object X changed" once equals replaying it five times.
- **The backlog head holds the lowest pending version.** What makes `Peek` sufficient:
  the queue is in first-touch order, so its head is the earliest-touched key, and under
  version-ordered publication that is also the lowest-versioned one. `writeSignal.firstRV`
  records a slot's creating version and `writeSignalMerge` never advances it, so
  coalescing cannot move the bound. Where publication order fails, the waker notices —
  a delivered write at or below the cursor is proof the cursor over-committed, reported
  at Error with the cursor clamped back. That is a detector, not a repair: it cannot help
  if the stream drops before the late write arrives.

**Not covered: changes made before the first successful subscribe.** If the initial
subscribe fails, no cursor was ever taken, so the only honest resume point is zero —
and replaying from zero is the whole-world pass this design replaces, bought with one
transient error. The waker takes the first cursor it can get and warns that changes
before it are not replayed; the reconciler's startup pass is what covers them, unless
`WithStartupResync(false)` turned it off.

**Not covered: a crash during an outage.** The watermark is in memory, and persisting it
would not fix this — a watermark records *delivery*, while what must survive a restart is
*convergence*. If the waker requeues a dependent, advances past the target, and the process
dies before that reconcile runs, a persisted cursor is already past it. Recorded intent is
unavailable too: `reconcile_owed` cannot be stamped on the dependent at failure time,
because the lookup that failed is the one that would have named it. The fix is per-object
`observed_cursor`, which derives staleness instead of recording intent; see the TODO entry.
Exposure needs `WithStartupResync(false)` *and* a crash, since the startup full pass
otherwise covers it.
