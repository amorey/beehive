# The stale-dependents pass scans from a cursor over target versions

- **Status:** Accepted — implemented in `waker.go` (`staleDependents`),
  `sqlite/store.go` (`DependentsListStaleSince`, `ReconcileOwedStamp`,
  `ResourceVersionsMaxIssued`), `reconciler.go`.
- **Date:** 2026-08-03

## Context

The stale-dependents pass is the correctness backstop behind the dependency
waker. It cannot be disabled. Before this change it re-derived staleness from
current state on every sweep, over the whole dependency graph.

Its cost tracked the edge count, not the change rate. Measured on an in-memory
store, one kind, one converged sweep:

| Objects | `depends_on` edges | One sweep |
| --- | --- | --- |
| 1,000 | 2,000 | 1.5 ms |
| 10,000 | 20,000 | 17 ms |
| 50,000 | 250,000 | 190 ms |

A converged sweep is the worst case. `LIMIT` cannot stop the scan early when
nothing matches, so a healthy system paid the full scan every time.

`docs/TODO.md` recorded this cost against "a five-minute interval", which put it
at 0.06% of one connection. That figure was wrong.
`defaultStaleDependentsInterval` is 60 s, so the true cost was **0.32%**, five
times higher, and the deferral rested on it.

### What a cursor puts at risk

A cursor bounds the scan to targets written since the last sweep. It therefore
never re-examines a dependent that is already stale and whose target has since
gone quiet. Four routes reach that state:

1. **The pass found the dependent and the process stopped before the reconcile
   ran.** The enqueue was in memory. Nothing failed, so nothing was recorded.
2. **The reconcile succeeded and the watermark write failed.** `reconciler.go`
   logged the error and continued, on the stated ground that the pass would
   re-derive it.
3. **`EdgesAdd` cleared the watermark for a new edge whose target is quiet.**
   Already covered by `EdgesAdd`'s own `reconcile_owed` stamp.
4. **The process was killed between a reconcile's owed decrement and its
   watermark write.** These are two statements, not one transaction. The count
   is gone, the object is settled by its status write, and the target may never
   be written again.

The full scan covered routes 1, 2 and 4. A cursor removes that cover. Route 4 is
the hard one: no durable record names that dependent, because the decrement that
removed the record was correct at the time it ran.

## Decision

Add the cursor, and cover each route with a named mechanism. Three mechanisms do
the work, and they are not interchangeable.

### The bounded scan

`DependentsListStaleSince` lists the dependents of targets written above a
cursor, then filters on the watermark as before. It is ordered by
`(target resource_version, target id, dependent id)` and returns the position of
its last row.

The position is a triple because a target with more dependents than one page
must resume **inside** its own fan-out. A cut at a target boundary would drop the
rest of it. Only the first component outlives the sweep; the other two are paging
state.

The `CROSS JOIN`s pin the join order. Without them the planner reads the whole
graph and the cursor saves nothing.
`TestDependentsListStaleSinceDrivesFromTheVersionIndex` holds that.

Each sweep reads a mark **before** it scans, and scans no higher. The mark comes
from `ResourceVersionsMaxIssued`, which reads `resource_version_seq`. It must not
come from `ObjectWritesMaxVersionAll`: retention lowers that value, and an idle
store past its retention window reads 0. A mark that falls compares wrongly
against a stored position.

The mark also makes a sweep finite. A store that takes writes faster than the
caller pages would never reach a short page, so the sweep would never end and the
cursor would never move.

A completed sweep consumed its mark, so the next one resumes at the **next**
version, from the start. A resume at `(mark, 0, 0)` would not do: ids are
positive, so that position still matches every target at the consumed version. A
target sitting exactly there whose dependents have not reconciled yet would have
its whole fan-out listed and stamped again on every sweep.

The cursor moves only when a sweep reaches the end. A failed page abandons the
sweep and holds the cursor, so the next tick reads the same range again. A
re-read is harmless: the stamp accumulates and `ReconcileOwedDecrement` subtracts
the whole count observed at load.

A tick where nothing has been issued since the last sweep skips the listing
entirely, because no target can have moved.

### Mechanism 1: the cursor is process-local, and covers routes 1 and 4

**It is deliberately not persisted. Every process re-derives the graph once.**

The cursor starts at 0, so the first sweep of each process scans from the
beginning and finds every dependent whose watermark is behind its target. That
is the same set the unbounded scan produced.

This is what covers route 4, and it is the only thing that can. A crash between
the decrement and the watermark write leaves nothing durable to read. It also
covers route 1, because a lost in-memory enqueue requires the process to stop,
and a stop is followed by a start.

Re-derivation once per process needs no case analysis. Both routes are produced
only by a crash, and a crash is a restart. The alternative — enumerating every
enqueue path that could lose a wake — is not worth trusting. A dependent woken by
the dependency waker carries no stamp at all. So does one enqueued by its own
spec write, by `Client.Requeue`, or by a `RequeueAfter`. Any future enqueue path
would join them silently.

The cost is one full scan per process: 190 ms at 250,000 edges, which this pass
paid every 60 s before this change. The per-sweep saving is untouched.

### Mechanism 2: the reconciler stamps a lost watermark, and covers route 2

`reconciler.go` now calls `ReconcileOwedStamp` when `DependencyWatermarksSet`
fails. Route 2 is closed by that stamp alone, and by nothing else.

The cursor cannot close it. Route 2 happens inside a process that keeps running,
and that process's cursor has already moved past the target. The dependent would
wait for the next restart.

A cancelled write is a shutdown rather than a lost pass, so the stamp is skipped
when `ctx.Err() != nil`. The stamp would fail the same way, and a report would
fault every clean stop.

### Mechanism 3: the pass stamps what it finds

`ReconcileOwedStamp` increments `reconcile_owed` for a page of refs in one
statement. The pass stamps each page before it enqueues that page, so a crash
between the two costs a spare reconcile, which is idempotent.

**This stamp is not what makes the cursor sound.** Routes 1 and 4 are covered by
mechanism 1, route 2 by mechanism 2, and route 3 by `EdgesAdd`. With the current
`workQueue`, removing this stamp would not strand any dependent: `add` marks an
in-flight id dirty rather than dropping it, `done` re-queues it, and the retry
ladder is unbounded, so the only way to lose a finding is to stop the process.

It is kept for three reasons.

- **A finding is recorded before it is queued.** This is the project's standing
  rule: durable owed work, rather than state that has to be derived again. The
  guarantee then does not rest on the queue's current policy at all. A bounded
  queue, a drop under pressure, or a maximum-retry rule would each turn a lost
  finding into a silent strand, and none of them would fail a test.
- **It is the precondition for ever persisting the cursor.** The merge with the
  dependency waker is the expected end state (see below), and that waker's cursor
  is durable. A persisted cursor removes mechanism 1, at which point this stamp is
  the only cover for route 1.
- **It bounds repair after a crash to the owed pass rather than to the
  re-derivation sweep.** Both run soon after a start, so the gain is small, but
  the owed pass reads an empty partial index and the sweep does not.

The cost is one `UPDATE` for each finding, plus a second wake path for that
finding through the owed pass. The queue folds the duplicate.

## Consequences

- **`reconcile_owed` has three producers, where the contract said it should have
  one.** `storeapi.Store` carried the note "there is deliberately no standalone
  `reconcile_owed` increment… add one only when a producer other than `EdgesAdd`
  exists." This record is that condition being met, twice: the pass and the
  reconciler's watermark fallback.
- **A live process that loses both writes is still exposed.** If
  `DependencyWatermarksSet` fails and the repair stamp fails with it, that
  dependent stays stale until the process restarts. Mechanism 1 repairs a crash,
  not a running process that keeps running. Recorded in `docs/TODO.md`, not done
  here.
- **The reconciler's fallback is best-effort against the store that just
  failed.** This is the one place the change trades a derived guarantee for a
  durable write that might not land. The store cannot own the fallback — a
  compensating write inside `DependencyWatermarksSet` fails for the same reason
  the watermark write did — so the caller is the right level. But the two calls
  share one connection and usually fail together. The deeper fix is to fold the
  owed decrement and the watermark set into one store transaction, so a failure
  leaves `reconcile_owed` standing by construction. That also removes route 4,
  and with it the reason the cursor cannot be persisted.
- **A stamped dependent whose reconcile keeps failing retries on the owed pass's
  cadence, not only on its backoff ladder.** `ReconcileOwedDecrement` is gated on
  success, so a failing dependent keeps the count the pass stamped. The owed pass
  lists it every `owedPassInterval` and calls `work.add`, which consults only the
  queued-now set — an id parked on a backoff alarm is not in it, so the add
  dispatches at once and the alarm later fires into a no-op.

  **Harmless on the defaults, and deliberately left there.**
  `defaultMaxRetryInterval` and `defaultOwedPassInterval` are both 30 s, so every
  rung of the ladder is at or under the owed-pass cadence and the alarm always
  beats the tick. The cost appears only when `WithMaxRetryInterval` is raised
  above `withOwedPassInterval`, where the ladder is effectively capped at the
  owed-pass cadence for any object carrying a standing stamp.

  The mechanism is not new — `EdgesAdd`'s stamp has always left a count standing
  through a failing reconcile — but this change widens the population from
  objects with a non-converging edge set to any dependent the pass has found. It
  is distinct from the `requeueNow` cost recorded in
  [the spec-write ADR](2026-07-31-a-spec-write-enqueues-its-own-object.md), which
  fires immediately rather than on a tick. Recorded in `docs/TODO.md`.
- **The pass and the dependency waker are now the same mechanism.** Both scan
  from a cursor and wake dependents. The waker's own record calls it "an
  optimisation, not a guarantee", and the guarantee it defers to is this pass —
  which now differs from it by a durable stamp and by the cursor's lifetime.
  Merging them is the expected end state and is deliberately not done here.
- **`DependentsListStale`, the unbounded form, is removed.** The cursor form is
  the only staleness listing on `storeapi.Store`. Its tests were ported onto
  `DependentsListStaleSince` through a shared `staleIDs` oracle, and the two that
  only restated the old shape — paging by dependent id, and one row per dependent
  — were replaced: the second by
  `TestDependentsListStaleSinceReturnsAPairPerMovedTarget`, which pins the
  opposite contract. Earlier records name the old method; it was never renamed,
  and `DependentsListStaleSince` is where its contract now lives.
- **A process-local cursor needs no scoping and no repair path.** A persisted one
  would need both. It would have to be keyed by the registered kind set, because
  a cursor earned while a kind had no controller says nothing about that kind's
  dependents. It would also need a way to move backwards, because a monotone
  write can never replace a position above this database's sequence. Both
  disappear when the cursor starts at 0 in every process.
- **A cursor shared by two processes on one database breaks**, because each would
  skip work the other's cursor claimed. The full scan was immune to that by
  construction. This is bounded by the single-writer, single-process constraint
  `driver_cursors` already documents.
- **An idle sweep is now one read**, so the case for an idle gate over this pass
  is gone. A gate keyed on an edge-set counter was specified and is not needed:
  there is no full scan left to skip on a settled store. The edge-set counter
  still has a second consumer, recorded in `docs/TODO.md`.
