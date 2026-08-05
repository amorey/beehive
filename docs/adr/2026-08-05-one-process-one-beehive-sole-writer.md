# One process, one Beehive, and it is the store's only writer

- **Status:** Accepted — a scope constraint; documented in `README.md`, `CLAUDE.md`,
  `docs/reconcile-triggers.md` and amended into the driver ADRs.
- **Date:** 2026-08-05

## Context

Nothing in beehive ever claimed multi-process support, but the documentation kept
reasoning about it. The driver ADRs justify the floor ticks partly by "a second
process writing the same store"; the waker ADRs quantify what "the multi-process
deployment" gives up; `README.md` promised a dependency wake "within a minute in
every case — including one written by another process, or straight to the `Store`
behind this `Beehive`'s back". Read together, those passages describe a supported
configuration with a documented latency penalty.

That configuration does not exist. It is not designed for, not tested, and not
reasoned about anywhere that would make it sound:

- **The work queue, its floors, and the schedule gauge are in-memory and
  per-`Beehive`.** Two processes each dispatch their own copy of the same object,
  and the per-id re-enqueue floor bounds neither the pair nor the cycle it exists
  to bound.
- **The waker's cursor is one row in `driver_cursors`, shared and unscoped.** Two
  wakers consume each other's pages; a change whose only dependent is registered
  in the other process is scanned away.
- **`Register` is process-local.** Each process knows only its own kinds, so
  finalizer clearing, the GC's per-kind backstop, and `WithFinalizers`' eager
  check all answer differently depending on which process is asked.
- **`AfterCommit` publishes to an in-process hub.** Every push path — the spec
  write's own enqueue, the new-edge enqueue, the delete-request and
  cleared-finalizer collects, the physical delete's owner push, the watch
  tailers, the waker's wake — is invisible to any other process by construction.
- **SQLite is one writer at a time.** A second process serialises against the
  first for the whole of every transaction, which is not a shape the store's
  single-connection assumptions were sized for.

The backstops are real, so the failure mode is usually latency rather than
divergence — which is exactly what made the documentation comfortable describing
it as degradation. But "usually latency" across a set of mechanisms that were
never analysed together is not a guarantee, and stating it as one has a cost:
reviewers read the promise, find a mechanism that does not keep it, and file a
bug against a configuration nobody supports.

The same applies to writes issued to the underlying database out of band — from
another tool, or straight to the `Store` behind a running `Beehive`. Such a write
publishes nothing, so it reaches every push path not at all and every pull path
at that pull's cadence.

## Decision

**Beehive supports exactly one process, running exactly one `Beehive`, as the
store's only writer.** Concretely:

1. **One process per store.** A database file is opened by one process at a time.
   Two processes over one file is unsupported.
2. **One `Beehive` per store.** Two `Beehive` values over the same store, even in
   the same process, is the same unsupported configuration — the hubs, queues and
   cursors that would have to be shared are per-`Beehive`, not per-process.
3. **No out-of-band access to the database.** While a `Beehive` is running, every
   write goes through its `Client`, `ControllerClient` or GC path. That excludes
   an external tool writing the file, and it excludes calling the `Store` directly
   behind the running `Beehive` — even though the embedder holds that `Store`,
   having opened it to pass to `New`. Reads out of band are equally outside the
   contract: the store's invariants are maintained across transactions the
   `Beehive` issues, and a reader that does not know them can observe a row
   mid-cascade.

**Sequential use is unaffected**, and is not what this rules out. Stopping a
`Beehive` and starting another over the same store — a restart, a crash, a new
binary — is supported, and is what the durable records exist for: a spec write's
generation bump, `reconcile_owed`, `dependency_watermarks.reconciled_against`,
the write log. The constraint is on *concurrent* access, not on succession.

**This is documented, not enforced.** No lock file, no advisory lock, no
registration row. Enforcement is a defensible thing to add later; it is a
separate decision, and adding it would not change the contract above.

## Consequences

**The floor ticks and the pull passes keep every other justification they had.**
Nothing here removes a driver. The object tail's 30s floor tick still covers a
failed read and a retention trim; the stale-dependents pass still cannot be
disabled, and still recovers a wake lost to a crash, a restart, or a bug in the
wake path. What changes is only that "a second process wrote it" is no longer one
of the cases in the list — so a driver whose whole remaining justification was
that case would now need re-deriving. There is none.

**The dependency-wake guarantee gets narrower and truer.** The promise is a wake
as soon as the target's write commits, and within 60s if that push is lost, for
writes made through this `Beehive`. It is no longer "within a minute in every
case, including a write from another process" — beehive cannot see that write's
commit, and saying so is the point of this record.

**A whole class of review finding is out of scope.** A bug report whose repro
needs two processes, two `Beehive` values over one store, or a write issued
around a running `Beehive`, is answered by this ADR rather than fixed. A report
that needs only a restart is in scope and is a real bug.

**Tests asserting the unsupported shape are removed rather than kept as
documentation.** A green test is a support claim. `TestTailerFloorTickPicksUpAForeignWrite`
built a second `Beehive` over one store and asserted the floor tick picked up its
write; the floor tick's remaining justifications are covered by
`TestTailerRetriesAfterAFailedStep` and `TestTailerResetsWhenItsCursorIsTrimmed`.
Tests that construct a second `Beehive` *after stopping the first* are restart
tests and stay.

### Rejected alternatives

**Enforce it — a lock file or `PRAGMA locking_mode=EXCLUSIVE`.** Turns a
documented constraint into a runtime error, which is strictly better for anyone
who trips it. Not done now because it changes `New`'s failure modes and its
teardown obligations, and because the immediate problem is that the docs claim
the opposite of the truth. Fixing the claim does not depend on it.

**Support it properly — a store-backed wake, scoped cursors, a shared queue.**
This is the honest fix for the deployment shape, and it is a different package.
Every push path would need a store-backed equivalent, the per-id floors would
need to be shared state, and `Register` would need a durable registration. The
[drivers ADR](2026-07-28-periodic-scan-drivers.md) already records that a push
path belongs *above* this core; the same is true of coordination.

**Say nothing and let the backstops carry it.** The status quo. It reads as a
support claim, which is how the review findings that prompted this record got
written.
