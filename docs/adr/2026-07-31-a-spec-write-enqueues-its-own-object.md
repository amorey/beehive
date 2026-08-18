# A write enqueues the object it made owe a reconcile, gated on what it changed

- **Status:** Accepted — implemented in `client.go` (`signalSpecWritten`,
  `signalCreated`) over a `changed` bool added to both `Objects().UpdateSpec*`
  mutators in `internal/storeapi` and `sqlite/store.go`, and in `controller.go`
  (`DependenciesAdd`) over `EdgesAddResult`. Both go through
  `Beehive.signalRequeueNow`/`signalRequeueThrottled`. Narrows the "Writes schedule nothing" section of
  [the drivers ADR](2026-07-28-periodic-scan-drivers.md), which still governs
  every other write.
- **Date:** 2026-07-31, extended 2026-08-01 to the new-edge stamp

## Context

A spec write bumped `generation` and stopped. The owed pass listed the object on
its next tick, 30 seconds by default, and that pass was therefore the *primary*
trigger for an ordinary `Create` or `Update` rather than a backstop for one.

The cost was the commonest latency in the system, and the workaround was written
into the documentation rather than the defect: every example under `examples/`
calls `Client.Requeue` after a create, because a write scheduled nothing.

It also mispriced the owed pass. An interval that carries the primary path cannot
be lengthened, because lengthening it makes ordinary use proportionally slower.
An interval that only backstops a prompt path prices one thing: how long a bug in
that path may delay an object.

## Decision

**A write that changes an object's spec enqueues that object's own reconcile,
through `Store.AfterCommit`.** `Create`, `GetOrCreate`'s created branch, and both spec
updates go through one helper. The owed pass is unchanged and still lists the same
objects; it is now a backstop.

`AfterCommit` is the hook because it already has the property the enqueue needs: a
rollback discards it, including a savepoint unwind inside a nested `Within`. An
enqueue placed after the call returns would fire for a spec change that an outer
transaction then discarded.

### The gate is "this write changed the object", and nothing else

The store now reports it: both `Objects().UpdateSpec*` mutators return a `changed`
bool beside the row, true only when the spec was written and `Generation` bumped.
A create always changes the object. The
[write-shapes ADR](2026-07-30-store-write-shapes.md) sanctions exactly this shape —
a row plus "a `bool` that someone actually reads" — and the alternative gates are
both defects rather than style differences.

**Gating on "the caller called `Update`" would enqueue a byte-identical write.** The
store skips such a write entirely, and that skip is what stops a controller
re-applying its own spec from waking itself forever (see
[the generation-handshake ADR](2026-08-18-beehive-owns-the-generation-handshake.md)).

**Gating on the row being unsettled has the same defect by a longer route, and this
one was shipped and then fixed.** A failing reconcile never settles the row, so every
no-op write such a controller makes passes an unsettledness gate. The enqueue is then
worse than a scan would be: `requeueNow` cancels the backoff alarm and marks the
in-flight id dirty, so `work.done` makes it dispatchable immediately. A controller
that re-applies its spec and then fails retries at full speed forever and never
reaches its ladder. Measured at 25 reconciles with no delay before the fix; pinned by
`TestFailingRespecControllerKeepsItsBackoff`.

The lesson is worth stating plainly: **"unsettled" is what the object owes, and
"changed" is what this write did.** Only the second is a reason to schedule anything.
The owed pass answers the first, on its own cadence, which is what a backstop is for.

**A status write cannot reach the helper.** `UpdateStatus`, both condition mutators
and `Objects().DeleteFinalizer` leave the generation alone, and none of them is on the
`Client` surface. So the loop that a reconcile could otherwise close — reconcile ends
in a status write, status write enqueues the next reconcile — does not exist. This is
the same hazard `dependentsWake` avoids by skipping `from_id == to_id`, reached by a
different route.

### The signal is read as the write leaves it, not as the transaction commits

A caller that updates the spec and then reports that generation through
`UpdateStatus` in the same outer `Within` commits a *settled* row, which
`Objects().ListUnsettledIDs` would not select — and the enqueue registered by the spec
write still stands, because that write did change the object.

**That is a duplicate, not a defect.** The object is dispatched once more, reconciles
against current state and settles. It is the same direction the rest of the design
errs in, and the work queue coalesces it against anything else pending.

Checking the committed row instead was considered and rejected. It would cost a store
read on every spec write, on the one connection, and it would not deliver what it
appears to: after the commit a read returns *current* state, which a concurrent write
may have moved either way, so the check would be exact against a later moment rather
than against this transaction. It would also fail in the worse direction — reading
"settled" while another transaction had just made the object owed skips an enqueue
for real work, trading a harmless duplicate for a missed prompt.

Pinned by `TestSpecThenStatusInOneTransactionStillEnqueues`.

### What it does not do

The enqueue does not clear the backoff ladder, matching `Client.Requeue`'s default.
`WithResetBackoff` stays the explicit way to ask for that. A new spec is not
evidence that a previous failure will not repeat.

The reconciler is resolved inside the hook rather than at registration time. On the
`Within` path the helper may run inside the caller's transaction, and `bh.mu` is a
lock `Register` and `stop` also want. Outside a transaction the hook runs inline on
the caller's goroutine and takes `bh.mu` there, which does not deadlock because
`stop` releases `bh.mu` before it waits. A client-only kind resolves to nothing and
the hook is a no-op.

## Consequences

- **"Writes schedule nothing" is now narrower, and its argument survives.** That
  section's reasoning was that the durable record and the signal are the same
  write, so a rollback leaves nothing behind for free. That still holds: the
  generation bump is still the record, the owed pass still lists it, and the
  enqueue is an optimisation layered over a listing that keeps running. What
  changed is that a write now also *starts* the work it records. A delete still
  schedules nothing — the GC sweeper lists `deletion_requested_at` on a cadence
  this ADR does not touch.
- **A lost enqueue costs latency, never convergence.** The hook lives in memory, so
  a crash between commit and dispatch discards it. The row is still unsettled, so
  the startup owed pass finds it, and the periodic owed pass finds it in a running
  process. This is the same contract every push path in beehive holds.
- **The owed pass interval becomes a policy number.** It now prices how long a
  missed enqueue may delay an object, not how long ordinary use waits. Lengthening
  it is a separate decision, and this ADR is its precondition.
- **The examples' `Client.Requeue` calls are no longer required for a create.**
  They are left in place, because they still demonstrate the API and still bound
  the case where the enqueue is lost.
- **A write that does not go through this `Beehive` gets no push.** The hook is
  registered in the beehive layer, so a writer holding the `Store` directly, or a
  second process on the same database, is not enqueued here. Both are
  [unsupported](2026-08-05-one-process-one-beehive-sole-writer.md) rather than
  slow — the scans that would eventually pick such a write up are the reason the
  symptom is latency, not the reason the shape is allowed.

## Extension: a new dependency edge enqueues its source

`Edges().Add` increments `reconcile_owed` for every `depends_on` edge it creates, and
that count waited for the owed pass. The same decision applies at that site, so it
is recorded here rather than in a second ADR that repeats the argument.

**The gate, the hook and the routing are one decision applied twice.** The gate is
`EdgesAddResult.ReconcileOwedStamped`, which is the store's report that this call
created the edge — the same discipline as the `changed` bool, and for the same
reason: a caller's claim that it wrote is not evidence that anything moved. A
level-triggered controller re-asserts its whole dependency set on every pass, so
an ungated enqueue would schedule a pass per pass. The hook is `Store.AfterCommit`,
so a rollback or a savepoint unwind discards it.

**Both enqueue their own kind.** A spec write is always to the client's own kind, and
a declare is always from the pass's own object, so an edge — cross-kind at its far
end — still enqueues the source through the client's own reconciler.
`TestDependencyVerbsBindTheSource` pins the source.

### The backoff ladder now survives a non-converging edge set

This was a known cost of `requeueNow`, accepted rather than fixed. The
[re-enqueue floor](2026-08-04-work-queue-re-enqueue-floor.md) closed it.

`requeueNow` on an id that is in flight marks it dirty rather than queueing it.
`runWorker` then calls `work.done(id)` before `work.addAfter(id, backoff)`, and
`done` makes a dirty id dispatchable at once, so the backoff alarm set on the next
line did not hold it. A failing pass that created an edge retried immediately,
however deep its ladder was.

**The new-edge push now goes through `work.add` rather than `requeueNow`.** The
dispatch that started the pass opened the source's re-enqueue floor, so the
declaration is held rather than queued, and the backoff the worker sets a line
later takes the floor's place. The stamp is durable either way, so nothing is
lost: the owed pass still carries the dependent.

**The spec write's own enqueue stays immediate**, and keeps this cost. A spec
write carries new information, so absorbing one into a pending backoff would make
a user's edit to a failing object wait out `maxRetryInterval`. Its own runaway
case — a controller writing a genuinely changing spec every pass — is
self-announcing, because the generation climbs. The `changed` gate above is what
stops the byte-identical case.

Pinned by `TestFailingControllerKeepsItsBackoffWhenItsEdgeSetConverges`,
`TestANewEdgeOnAnInFlightSourceRespectsTheBackoff` and
`TestFailingRespecControllerKeepsItsBackoff`.
