# The waker spins on a self-dependency, and on any cycle

**Status: a defect that exists today. The self-edge half has a recommended fix; the
general case is a decision this document frames rather than makes. Independent — it
depends on nothing else in flight, and nothing else depends on it.**

---

## 1. The defect

`wakeDependents` (`beehive.go:394-396`) does not exclude `from_id == to_id`:

```go
func (bh *Beehive) wakeDependents(ctx context.Context, targetID ObjectID) {
    deps, err := bh.store.ListIncomingRefs(ctx, targetID, RelationDependsOn)
    ...
    for _, d := range deps {
        bh.enqueueIfRegistered(d.GroupKind(), d.ID)   // d.ID can be targetID
    }
}
```

An object that depends on itself and writes a change the store emits is therefore
woken by its own `Modified`: the pass writes, the store emits, `runDependencyWaker`
(`beehive.go:364`) calls `wakeDependents`, and the object is enqueued again. The full
path is `scanAndEmit(Modified)` → waker → `enqueueIfRegistered` → `r.enqueue` →
`workQueue.addLocked` (`workqueue.go:69`), which has **no rate limiter and no
already-settled skip**. **Immediately, at full speed** — no tick involved, no backoff,
no second party, and nothing that converges.

Two objects depending on each other do the same thing to one another for the same
reason: A's write wakes B, B's wakes A.

## 2. What bounds it, and what does not

**The trigger is any write the store emits — which is wider than status bytes.**
Four separate write families reach `scanAndEmit(Modified)`, and any one of them
sustains the loop:

- **`UpdateStatus` with changing bytes.** Identical bytes at the row's own schema
  version suppress the *content* write (no `resource_version` bump, no emit). A
  controller that stamps a timestamp, a counter, a retry attempt or a formatted
  message into status is not byte-stable.
- **`UpdateStatus` with identical bytes, at a generation it hasn't settled at.**
  The bullet above is not "byte-stable status is quiet". The content no-op still
  advances `observed_generation`/`observed_at` when they'd move, and that advance
  **does** bump `resource_version` and emit `Modified` (`sqlite/store.go:646` and its
  doc comment; CLAUDE.md documents it as deliberate — anything gating on
  `ObservedGeneration == Generation` would otherwise sit blind). So a byte-stable
  self-dependent controller still takes **one extra wake per generation**: bounded,
  not absent. `markForDeletion` is the same category — it emits once under an
  `IS NULL` guard. Both are bounded emitters that cannot sustain a spin alone but do
  sustain one alongside anything that recurs.
- **Condition writes.** `SetCondition`/`DeleteCondition` route through
  `bumpObjectAndEmit` (`sqlite/store.go:851-865`): a standalone `resource_version`
  bump plus `scanAndEmit(Modified)` for **every non-no-op condition mutation**. A
  controller with perfectly byte-stable status but a condition whose `message` or
  `reason` carries an attempt count or a timestamp spins just as hard. The stale-
  liveness path (`conditionUnchanged` returns false for a condition written by a prior
  process) also breaks the no-op suppression, though only once per process.
- **Finalizer clearing.** `DeleteFinalizer` (`sqlite/store.go:1165`; the absent-
  finalizer no-op returns early at `:1180`, a real removal bumps and emits at
  `:1184-1193`). There is no `AddFinalizer` mutator — finalizers are stamped at
  create — so this family is bounded by the finalizer set and cannot spin on its own;
  it is listed because it emits, so it sustains a cycle for as long as a controller
  has finalizers to clear.

**The converse, because it reads surprising: `RecordEvent` does *not* bump the
object's `resource_version`.** It allocates a version from the same counter for the
*event* row and publishes on the events hub, but emits no object `Modified` — so an
events-only controller does not feed this loop.

That widening is what justifies this section's length: **byte-stable status is not a
defence**, so "our controllers don't rewrite status" does not put a deployment outside
the blast radius. It carries no requirement into §6's core tests, which are unit-level
and agnostic to what caused the change — the write family matters only inside §6's
optional end-to-end bullet, which has to pick one that actually emits.

None of it bounds *what happens* when the loop does fire: uncapped, at reconcile
speed, per object in the cycle, for as long as the process lives.

**The generation handshake does not stop it**, because it is not involved as a brake.
The object is settled the whole time — this is a wake, not an unsettled re-queue — and
`ObservedGeneration == Generation` throughout. The handshake's one contribution runs
the other way: recording it is itself an emitter (bullet 2 above).

**Neither shape is a pathology beehive can dismiss:**

- `AddDependency(ctx, id, id, …)` is accepted with no complaint, and `collect`
  explicitly names "or a self-dependency" when dropping finalizing `depends_on` edges
  (`gc.go:90-92`). The self case is anticipated in the code, not merely permitted by
  accident.
- `DeleteFinalizingDependsOnRefs` exists *precisely* because two finalizing objects
  can point at each other, so mutual edges are a shape the store already reasons
  about.

## 2a. Severity: contention, not background CPU

"Unbounded background work" understates it for this store. Each spin iteration is a
reconcile pass with at least one write, and:

- **The store runs on a single connection** — `db.SetMaxOpenConns(1)` in `OpenMemory`
  (`sqlite/sqlite.go:45`), `sqlitemigrate.OpenPool(path, 1)` in `Open`. Every write
  transaction the spin opens **serializes every other writer in the process** — client
  writes, other kinds' reconciles, the GC sweeper, event retention. This is not one
  goroutine burning cycles off to the side; it is contention on the single resource
  everything in beehive shares.
- **A reconcile worker slot** out of the kind's `concurrency` (`reconciler.go:508`),
  permanently occupied by an object that is already converged.
- **The global monotonic `resource_version` counter**, burned at reconcile speed.
- **A publish through the change hub and a `WatchSchedule` emit per iteration**, so
  every live watcher and schedule subscriber is driven at the same rate.

So the observable symptom is a control plane whose unrelated writes get slow, on a
store that is fully converged and reports nothing wrong: the objects are settled,
their generations match, and nothing is out of sync.

## 3. The self-edge half: fix it

**Skip the self-edge in `wakeDependents`.** One comparison, on a value the loop
already holds:

```go
for _, d := range deps {
    if d.ID == targetID {
        // Self-edge: nothing here is owed a wake. A spec write requeues through
        // wakeAfterCommit; a status or condition write is this object's own pass,
        // which just ran. Waking it re-enqueues at full speed with nothing to
        // converge it — see specs/waker-cycle-spin.md.
        continue
    }
    bh.enqueueIfRegistered(d.GroupKind(), d.ID)
}
```

**Split the two comments by what they're about.** The spin rationale earns its three
lines at the guard. The invariant that makes an id compare mean "the same object" —
`ObjectID` is globally unique across kinds (`objects.id INTEGER PRIMARY KEY
AUTOINCREMENT`, one table for every kind) — is about the function's addressing model,
not this branch: the `enqueueIfRegistered(d.GroupKind(), d.ID)` line carries the same
assumption. It belongs in `wakeDependents`' doc comment, noting that a per-kind id
scheme would need the `GroupKind` compared too. Nine lines inside a `for` body is over
the density line this repo holds.

**It must be `continue`, not `return`.** A target whose dependents include both itself
and other objects must still wake the others; the self-edge's position in
`ListIncomingRefs`' result is not something the caller controls.

**Why this is safe, not merely cheap.** A self-dependency can only mean "requeue me
when I change", and every route by which the object changes already requeues it:

| what changed | what already requeues it |
|---|---|
| its spec, through `Client` | `wakeAfterCommit` enqueues post-commit on the write, and the generation bump leaves it unsettled for `ListUnsettledIDs` |
| its own status/conditions, in its own pass | nothing needs to — the pass just ran, and `Result.RequeueAfter` is how a controller asks for another |
| another object it depends on | that target's own wake, which this guard does not touch |
| the self-edge was just declared | `AddDependency`'s conjunction — the in-memory requeue plus the `pending_wake` stamp inside `AddRef` — fires for a self-edge like any other, exactly once. It runs on the declare path, which never enters `wakeDependents`, so the guard is structurally incapable of touching it and the raced-declare guarantee is unchanged |

That last row is the one entry in the table with no dedicated §6 assertion, and
deliberately: it is untouched by construction rather than by argument, so a test would
be pinning that `AddDependency` doesn't call `wakeDependents`. §6's
`TestAddDependencyAcceptsCycle` does exercise the self-edge declare path incidentally,
which is as much coverage as this row warrants.

**One case the table does not cover, stated rather than glossed.** It assumes the
object's own controller is the only writer of its status and conditions. It isn't:
`Register` hands the application a `ControllerClient`, and an out-of-band
`UpdateStatus`/`SetCondition` from application code is a supported pattern. That write
today wakes a self-dependent object, and after the guard it is woken by **nothing** —
it bumps no generation, so `ListUnsettledIDs` cannot see it, and `observed_generation`
is already recorded. The route for that case is `Client.Requeue` (or
`Result.RequeueAfter`), which is what an out-of-band writer should be using anyway;
this is niche enough that it does not change the recommendation, but the guard's
comment and the release note should say it rather than claim the self-wake is
redundant on every path.

**Put the reason in the code**, not just the guard. Written as a bare `if d.ID ==
targetID { continue }` it reads like a micro-optimisation and invites removal; what it
prevents is an unbounded loop. The comment must not attribute the coverage to the
generation handshake — the handshake requeues nothing; `wakeAfterCommit` and
`ListUnsettledIDs` do.

## 4. The general case: a decision, not a fix

Cycles of length two or more spin for exactly the same reason and are excluded by
nothing. **This document does not fix them.** Two candidate fixes, with the second one
the standard answer:

**(a) Reachability at declare time.** `from_id != to_id` is one comparison on values
the join already holds; excluding cycles of arbitrary length means *reachability* — a
recursive CTE, on the store's single connection. Neither consumer is the right place
either: the waker and any future dependency scan would each need their own copy of the
same question, both on the hot path. That pushes it to the declare surface: should
`AddDependency` reject an edge that closes a cycle? That is a real judgement call, not
an oversight, and it turns on whether beehive wants to support mutual dependencies at
all — given that `DeleteFinalizingDependsOnRefs` currently assumes they exist, the
answer is not obviously yes. Two constraints on whoever takes it up:

- **It is a change to `AddRef`/`AddDependency`'s surface**, which is the boundary the
  in-flight observed-cursor work is explicitly scoped out of touching.
- **A cycle check at declare time is a read, and the declare path is already one
  pre-read away from a documented performance problem.** CLAUDE.md records that a
  per-declare pre-read is what sank an earlier version of the raced-declare guard —
  which is why edge-newness is now a `WHERE … NOT EXISTS` inside `AddRef` rather than a
  lookup before it. A reachability probe is strictly more expensive than the pre-read
  that was rejected.

**(b) A rate limiter / minimum re-enqueue interval on the work queue.** This is what
controller-runtime does for exactly this class, and the spec's earlier draft omitted
it. It would bound **every** cycle length, needs no reachability query, and costs
nothing on the hot path. It does not make a cycle *converge* — it turns "spins at full
speed forever" into "one pass per interval forever" — but that is the same order of
harm already accepted elsewhere for the general case, and it removes the contention
argument in §2a entirely, which is where the actual damage is.

**It is a new mechanism, not a reuse of `addAfter`.** An earlier draft said it could
reuse the existing `addAfter`/`alarms` machinery; that is wrong, and wrong in the
direction that would make someone accept the option on a bad estimate:

- **`addAfter` is newest-wins, and a throttle needs oldest-wins.** `addAfter` stops
  the prior timer (`workqueue.go:120-121`) and `timerFired` drops a superseded alarm
  (`:139`), so under a sustained wake stream every new wake pushes the alarm further
  out and the item **starves, never running**. A minimum-interval limiter wants a
  floor: a per-item "not before" watermark that later wakes clamp *to*, not past.
- **`add`/`addLocked` never consult `alarms` at all**, and the immediate path is the
  one every wake actually takes. The limiter has to intercept `addLocked`, which is
  also where the dirty-set coalescing and the `emitScheduleLocked` transition live.

So the honest cost is a new watermark field and its interaction with the existing
state, plus what the earlier draft already listed: a new unanchored constant, an
interaction with `Result.RequeueAfter` and with backoff to work out, and a behaviour
change for every non-cyclic caller of `add`.

**The recommendation stands at: ship the self-edge guard now, record the general
case.** But the record must weigh (b), not only (a) — a reader who sees only the
reachability option will conclude the general case is expensive to address, and it
isn't necessarily. Whoever takes it up should evaluate (b) first.

Recording the current answer is worth more than half-fixing it — but it has to be
recorded in a form that *trips*, and **no single assertion trips for both candidates**
— which is the trap here, since the obvious ones fail open in different directions. A
"the reconcile count climbs past N" test hangs rather than fails once (b) lands. A
unit-level "the waker wakes both directions" test is worse: it never calls
`AddDependency`, so (a) is invisible to it, and a *first* wake is immediate under any
sane throttle, so (b) is too — it asserts ordinary correct behaviour both fixes
preserve. So §6 names a tripwire per candidate: `TestAddDependencyAcceptsCycle` for
(a), and for (b) the existing work-queue tests asserting the *second* dispatch of the
*same* id is immediate — not the first-add tests, which a watermark leaves green for
exactly the reason that disqualifies the two-cycle test. Plus a TODO.md entry (§7),
which is where this repo records built-and-reverted guards and unbuilt fixes.

## 5. Interaction with the observed-cursor design

There is an in-flight design (recorded in TODO.md as `observed_cursor`, the broad fix
that would delete the waker's escalation paths) which replaces event-driven wakes with
a periodic cursor-comparison scan. It has the same shape in its scan and treats the two
lengths differently. The load-bearing facts, restated inline so this document stands on
its own:

**That scan needs its own self-edge guard.** Its query must carry `r.from_id !=
r.to_id`, because without it a self-dependent object is flagged on every check forever
— *by construction*, not by race: its cursor is sampled at load, so any write its own
pass makes lands above that cursor, making `target.rv > dependent.cursor` true on every
pass with no second party involved. Fixing the waker does **not** remove the need for
that guard; they are two consumers of the same edge and each needs its own.

**That scan has no guard for longer cycles either.** A 2-cycle of writing controllers
would be flagged once per check, forever, on a converged store. That is strictly better
than what the waker does today — once per check, versus at full speed — and is exactly
the bound option (b) above would give the waker.

So: this fix and that design are independent, and neither subsumes the other. If the
observed-cursor design lands as a document, the two cross-references above should
become citations into it; until then they are stated here.

## 6. Test plan

**The core assertions must be unit-level, not end-to-end.** "The object stops being
re-enqueued" is an *absence* of work, and this repo's convention is explicit: no
`time.Sleep` to wait for a goroutine, no polling loops. There is no signal that says
"the waker is done not waking me", so an end-to-end spin test can only be written as a
sleep or a poll, and it would burn a reconcile worker and hammer the single sqlite
connection inside the package that runs every other test, under `-race`.

The shape that works is already in the suite: `TestWakeDependentsListErrorLogs`
(`reconciler_test.go:2693`) builds a bare `&Beehive{store: fake}` and calls
`bh.wakeDependents(ctx, 1)` directly, with `errDepsStore` (`:740`) as the pattern for a
fake that controls `ListIncomingRefs`. Every assertion below that concerns the guard
itself is written that way: a fake returning the edges under test, a `reconcilers` map
holding a real `&reconciler{work: newWorkQueue()}`, a direct `wakeDependents` call, and
an inspection of the queue afterwards. Fully deterministic, no control plane, no spin
to observe.

- **A self-edge does not enqueue.** Fake `ListIncomingRefs` returns a single referrer
  whose `ID` equals `targetID`; assert the queue is empty after `wakeDependents`. This
  is the guard's whole contract and it fails today.
- **The guard is `continue`, not `return`.** Fake returns the self-edge **first**,
  followed by two other dependents — one on the target's kind, one on a second kind, to
  cover the `enqueueIfRegistered` routing. Ordering is under the fake's control here,
  which is why this is a unit test. Two details the recipe depends on: the fake must
  populate `Group`/`Kind` on **every** `Referrer`, since `GroupKind()` is what routes;
  and the second kind means a **second `reconciler` with its own `workQueue`** in
  `bh.reconcilers`, so the assertion inspects *each kind's* queue rather than one — a
  single-queue assertion would silently degrade the routing half into a same-kind test.
- **A 2-cycle still wakes both ways.** `wakeDependents(A)` enqueues B and
  `wakeDependents(B)` enqueues A. This pins the waker's ordinary behaviour around the
  guard. It is **not** the record of the general case — see the tripwire bullet below
  and §7; both candidate fixes leave it green.
- **`AddDependency` accepts a cycle today — the tripwire for candidate (a).**
  `AddDependency(A, B, …)` then `AddDependency(B, A, …)`, both returning nil, against
  the real store. That is precisely the fact option (a) proposes to change, so a
  reachability rejection at declare time fails it immediately. Cheap and deterministic.
  **Add a third call, `AddDependency(A, A, …)`, also asserting nil** — the two calls
  above form a 2-cycle, and neither has `fromID == toID`, so without this the test does
  not cover the self-edge declare path (§3's fourth row) at all. It also independently
  pins §2's load-bearing claim that a self-dependency "is accepted with no complaint",
  which nothing in the suite asserts today.
- **If an end-to-end variant is still wanted, it needs a named barrier.** The waker is
  one goroutine per kind, so publishing a *second* change on that kind and waiting for
  its dependent's wake flushes the waker past the self-change; only after that
  handshake is a reconcile count safe to assert exactly. Do not write it without that
  barrier. The write driving it must be one §2 actually emits — changing status bytes,
  a non-no-op condition write, or a byte-identical `UpdateStatus` at a fresh
  generation.
- **A self-dependent object is still woken by its own spec change.** Write its spec
  through `Client` and assert it reconciles. *Note this test passes even with the guard
  mis-implemented as a filter in `ListIncomingRefs` — the spec write requeues through
  `wakeAfterCommit`, which never consults the refs table. It is a regression guard for
  the wake path, not a detector for that mis-implementation.*
- **The self-edge is still visible to readers.** Assert `ListDependents(id)` returns
  the self-edge after the guard. *This is what actually detects a guard implemented by
  dropping the edge or filtering in `ListIncomingRefs` — that store call backs
  `Client.ListDependents` (`client.go:725`) and the `LoadDependentsBit` eager load
  (`client.go:565`), so filtering there silently changes what callers see.*
- **A self-dependent finalizing object still collects.** `collect` handles this case
  explicitly today (`gc.go:90-92`, which still runs `DeleteFinalizingDependsOnRefs`
  against the edge); assert the guard did not disturb it. *Corrected during
  implementation: this is **not** the other half of the filter mis-implementation, as
  an earlier draft claimed. `collect` reads refs through `HasIncomingRefs` and
  `DeleteFinalizingDependsOnRefs`, never `ListIncomingRefs`, so a self-edge filtered
  out of that call leaves this path green — verified by mutation. It fails only under
  a mutation to its own query. The two tests cover different consumers, and
  `TestClientListDependentsIncludesSelfEdge` is the sole detector of the filter.*
- **Ordinary dependents are unaffected.** The existing dependency-wake tests,
  unchanged.

**Where the tests go.** `TestAddDependencyAcceptsCycle` goes in `controller_test.go`,
next to its eight existing `TestAddDependency*` siblings (`:522`-`:669`) —
`AddDependency` is defined in `controller.go`, so this is what both CLAUDE.md's
mirror-the-source-filename rule and locality say. The waker tests are the awkward case:
`wakeDependents` is defined in `beehive.go`, so the rule says `beehive_test.go`, but
*every* existing test for it lives in `reconciler_test.go` (`:740`, `:749`, `:2693`),
which is also the pattern §6 borrows. **Keep the new ones with their siblings in
`reconciler_test.go`**; the pre-existing divergence is out of scope for this change.
Splitting the waker's tests across two files to satisfy the rule literally is worse
than either placement on its own.

**Which assertion is the tripwire for which fix.** No single test constrains both
candidates, and §4's "recorded in a form that trips" requirement is only met by naming
them separately:

| candidate fix | what trips when it lands |
|---|---|
| (a) reachability at declare time | `TestAddDependencyAcceptsCycle` — it asserts the exact call that would start returning an error |
| (b) minimum re-enqueue interval | the **existing** `TestWorkQueueNoConcurrentDispatch` (`workqueue_test.go:193`) and `TestWorkQueueReaddAfterDone` (`:221`) — both assert the *second* dispatch of the *same* id is immediate, which is exactly the latency a watermark introduces. No new test is needed. |

**Why those two and not the obvious ones.** `TestWorkQueueFIFO` (`:31`),
`TestWorkQueueDedup` (`:53`) and the ready-signal tests (`:67`, `:86`, `:105`) all stay
green under (b): FIFO and the ready tests are first-adds of distinct ids, which
dispatch immediately under any per-item watermark, and Dedup asserts a repeated add
dispatches *once* — which a throttle reinforces rather than breaks. Only a
same-id-second-dispatch assertion bites: `NoConcurrentDispatch` requires `get()` to
succeed immediately after `done(1)` following a re-add during processing, and
`ReaddAfterDone` requires `add(7)` → `get()` to succeed immediately after a prior
`done(7)`. Both fail for any nonzero interval, and both are load-bearing rather than
incidental.

The two-directional `wakeDependents` assertion is **neither**: it never calls
`AddDependency`, so (a) is invisible to it, and a first wake is immediate under any
sane throttle, so (b) is too. It pins ordinary waker behaviour that both fixes
preserve — keep it for that, but it must not be described as the record. Each of the
above carries the comment pointing at the TODO.md entry.

## 7. TODO.md entry (ships with the fix)

The general case must land in TODO.md, not only in a test comment — that file is where
this repo records real defects it chose not to fix, and a reader auditing known gaps
looks there, not in the test suite. Draft:

> - **A dependency cycle of length ≥ 2 spins the waker at full speed** — known, not
>   fixed; the self-edge half *is* fixed. `wakeDependents` skips `from_id == to_id`
>   (see the guard's comment), so a self-dependency no longer re-enqueues itself. Two
>   objects that depend on each other still do: A's emitted write wakes B, B's wakes A,
>   with no rate limiter in `workQueue.addLocked` and no already-settled skip on the
>   dispatch path. Any write family that emits sustains it — changing status bytes, any
>   non-no-op condition write (`bumpObjectAndEmit`), `DeleteFinalizer` — so
>   byte-stable status is not a defence. `RecordEvent` does not, since it bumps no
>   object `resource_version`.
>
>   It costs more than background CPU: the single store connection means the spin's
>   write transactions serialize every other writer in the process, and it holds a reconcile
>   worker slot, burns the global `resource_version` counter, and drives every change
>   watcher and `WatchSchedule` subscriber at reconcile speed — on a store where every
>   object is converged and every generation matches.
>
>   Two candidate fixes. **Reachability at declare time** (reject an edge that closes
>   a cycle in `AddDependency`) is a recursive CTE on the single connection, and the
>   declare path is one pre-read away from the performance problem that sank the
>   earlier raced-declare guard — strictly more expensive than the read that was
>   rejected. **A per-item minimum re-enqueue interval on the work queue** — what
>   controller-runtime does for this class — bounds every cycle length, needs no
>   reachability query, and costs nothing on the hot path; it does not make the cycle
>   converge, but turns "full speed forever" into "one pass per interval forever",
>   removing the contention. It is **not** a reuse of `addAfter`, whose newest-wins
>   alarm would starve the item under a sustained wake stream — it wants an oldest-wins
>   watermark on `addLocked`, which is the path every wake takes. Cost: that watermark,
>   a new unanchored constant, and an interaction with `Result.RequeueAfter` and backoff
>   worked out. Evaluate the rate limiter first.
>
>   Deferred on **fix cost**, not on likelihood: the self-edge is one comparison on
>   values the loop already holds, while the general case is either a recursive CTE on
>   the declare path or a new work-queue primitive. Likelihood points the other way, and
>   the entry should not pretend otherwise — a self-edge requires naming your own id,
>   whereas a mutual dependency is what two independently-written controllers fall into
>   with neither author seeing both halves, which is why `DeleteFinalizingDependsOnRefs`
>   exists at all. Whether beehive should support cycles is also unsettled, which is a
>   reason not to guard hastily.
>
>   Each candidate has its own tripwire, because no one test constrains both.
>   `TestAddDependencyAcceptsCycle` (`controller_test.go`) asserts `AddDependency(A,B)`,
>   `AddDependency(B,A)` and `AddDependency(A,A)` all succeed today — the exact fact (a)
>   would change. For (b), `TestWorkQueueNoConcurrentDispatch` and
>   `TestWorkQueueReaddAfterDone` (`workqueue_test.go:193`, `:221`) are already the
>   tripwire: both assert the *second* dispatch of the *same* id is immediate, which any
>   nonzero interval breaks. The first-add tests (`TestWorkQueueFIFO`,
>   `TestWorkQueueDedup`, the ready-signal tests) do **not** constrain a throttle and
>   must not be cited as if they do. `TestWakeDependentsTwoCycle` pins the waker's
>   both-directions behaviour but is *not* the record — both fixes leave it green.
