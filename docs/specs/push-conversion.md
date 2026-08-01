# Spec: replace the hot polls with commit-time push

Status: draft. Four pieces have landed out of order, none of them a hub:
the self-enqueue (`60b4ea4`), `EventsMaxVersion` (`1613685`), the
`EventsWatch` gate that reads it (`1613685`, `56af29d`) and the new-edge
enqueue ([new-edge-push](new-edge-push.md)). No hub in *this* plan exists
yet. `gobus` is a dependency, brought in by the schedule
watch, which was carved out and built separately on `gobus/watch` — a
different package from the `conflate` every phase below uses. See "Phases".
Date: 2026-07-31
Scope: the architecture of the drivers. This spec is the umbrella: it
holds the policy that every child spec inherits, and nothing else. The
children hold the changes, and [README](README.md) indexes them.
Related: this plan changes the accepted
[drivers ADR](../adr/2026-07-28-periodic-scan-drivers.md). Write a new ADR
when the work lands. Update `docs/reconcile-triggers.md` at each phase.

## Goal

Add push at commit time, so a commit starts the work that a tick starts
today. Latency falls from the poll interval to almost zero.

**No poll is removed by this plan.** Every existing driver keeps running,
and push is added beside it. Push sees only writes that pass through this
process's beehive layer; each poll scans the store and sees every write,
whatever made it. So a poll is not a slower copy of its hub — it covers
more. See [wake-push](wake-push.md), "Why the waker stays", for the full
argument.

**One poll was removed, outside this plan, and the criterion is what keeps
it from being a precedent.** The schedule watch converted to push-only in
its own spec, which was carved out and implemented separately. A poll may
go only when its hub can observe every writer that exists, and when the
notify is derived from the state rather than asserted at each site. The
first requires a value with no writer outside this process — which, in
beehive, means a value that is not in the store at all.

Every poll in *this* plan fails that first test by construction: each one
scans the store, and a second process can always write there. So the
schedule watch is not the first of a series. It was the only candidate, and
it is already done.

## What does not change

- **The periodic backstops stay.** The owed pass, the stale-dependents pass
  and the GC sweeper continue to run. They guarantee convergence. Push only
  makes work arrive sooner. Thus a lost push costs latency, and never
  divergence. This is the crash-safety rule: no strand gaps, while the
  system runs and across restarts.
- **Startup and crash recovery do not change.** The startup owed pass and
  the startup GC pass stay unconditional. A push lost to a crash is found
  there, or by the periodic backstops.
- **The full pass stays off by default.** No reconcile may depend on it.
- **`EventsWatch` keeps its poll as a backstop, and gains push beside it.**
  It can run both paths into one stream, because it dedupes what it
  delivers on `resource_version`. The poll is now gated on
  `EventsMaxVersion`, so a quiet tick costs one scalar. See
  [events-push](events-push.md).
- **Whether the object watches keep their poll is open, and it is phase
  4's decision.** This spec said they could not keep it, on the grounds
  that a poll and a hub would double-deliver a change. **That reason is
  wrong**: `poll` already dedupes on `seen`, so one goroutine can select
  over both paths, exactly as `EventsWatch` does. The open decision at the
  top of [watch-push](watch-push.md) holds the choice, and it decides
  whether the stale-watch pass, `ObjectsGetSummary` and `Lagged` are built
  at all. Nothing in phases 1, 2 or 2a depends on the answer.
- **`SchedulesWatch` is already converted, and is not part of this plan.**
  It is push-only, with no poll and no backstop, done separately in
  `4c8f607` — see the
  [schedule-watch ADR](../adr/2026-07-27-schedule-watch.md). Its value is
  the work queue's memory,
  so it has no writer outside this process and its hub can see everything —
  a property nothing in this plan has. Treat it as landed context: the
  `gobus` dependency exists because of it. It runs on `gobus/watch`, a keyed
  *state* bus, so it exercises the shared receive-side machinery and not
  `conflate`.

## The backstop pattern: re-derive, do not replay

Each push consumer gets a backstop that asks the store what is true now. No
backstop keeps a second copy of the hot path, and no backstop replays
deliveries.

| Push consumer | Backstop | What the backstop reads |
| --- | --- | --- |
| Dependent wakes ([wake-push](wake-push.md)) | Stale-dependents pass | The dependency watermarks |
| Spec writes and new-edge stamps, both landed ([new-edge-push](new-edge-push.md)) | Owed pass | The unsettled specs and `reconcile_owed` |
| Object deltas, one hub per kind ([watch-push](watch-push.md)) | **Open**: the retained object-watch poll, or the stale-watch pass | The poll re-reads and diffs against `seen`; the pass reads a per-kind summary — the highest `resource_version` and the live count |
| Event runs ([events-push](events-push.md)) | The retained `EventsWatch` poll | One scalar, `EventsMaxVersion`; the listing only when it moved |

The schedule gauge is deliberately absent from this table. It has no
backstop at all, which is why it was done separately: its hub sees every
writer that exists, so there is no class of miss to repair. Nothing in this
plan has that property, so nothing in this plan may copy it. See the
[schedule-watch ADR](../adr/2026-07-27-schedule-watch.md) if the exception
needs checking.

The event row's backstop is built. The gate landed before the push path it
backstops, so today it is only a cheaper poll. That is the pattern working
in the order it happens to allow, not a deviation from it.

The object row covers both object watches, because they are two consumers
of one hub rather than two hubs. `ObjectsWatchList` subscribes unfiltered,
and `ObjectsWatch` subscribes with a key filter on its one id. Under either
answer the two are backstopped together: a retained poll runs in the same
goroutine as each stream's receiver, and the pass compares the hub's
per-kind bookkeeping, so a filtered receiver needs no accounting of its own
and a mismatch ends every stream of that kind.

**The object row is the one open row in this table, and only in which
backstop it names.** It gets one either way, so the pattern below holds
whichever way phase 4 decides. What the answer changes is the cost and the
blast radius: a retained poll repairs every tick with no new machinery and
no public contract change, and a stale-watch pass repairs on a cadence of
minutes, needs a new `Store` read, and adds `Lagged` to the public
`ChangeType`. See the open decision at the top of
[watch-push](watch-push.md).

The event row keeps the poll it pushes beside, rather than replacing it.
That is available because that stream dedupes both paths on
`resource_version`. The object watches can do the same on `seen`, which is
what reopened the object row above.

Every row re-derives from something durable, and no row replays a delivery.
That is the property that decides what may be pushed *in this plan*: each
of these values lives in the store, so a backstop can always ask the store
what is true now, and every one of them therefore gets a backstop.

The schedule watch is the one value in beehive that escapes the rule rather
than satisfying it, which is why it was built separately and why it is not
in this table.

The owed and stale-dependents passes exist, and so does the object-watch
poll. The stale-watch pass is the one backstop in this plan that would be
new, and so is the summary it reads: the store has no per-kind summary
today. The poll gate reads the store-wide `ObjectWritesMaxVersion` and a
per-kind id listing (`ObjectsListIDs`) instead.
[watch-push](watch-push.md) holds the pass, the read it needs, and the
decision on whether either is built.

**`ObjectsGetSummary` is the last `Store` break this plan would make.**
`EventsMaxVersion` has landed, and no other phase needs a new member. So
phase 4's open decision is also the decision on whether an external backend
ever pays for the push conversion.

The backstops are what catch the failure that no monitor can see: a code
path that commits and does not notify. A dead goroutine is detectable. A
missing notify call is not. Only a periodic read of the durable state finds
it.

## The loud-failure policy

- **A detectable failure of the push machinery crashes the process.** No
  goroutine of that machinery may panic and be swallowed. This covers the
  wake hub's drain, and it covers any goroutine a hub owns on a
  subscriber's behalf. The restart then runs the startup recovery, which is
  a path we already test. This is the crash-only form of "fail loudly".
- **A subscriber that falls behind is collapsed, not failed.** Conflation
  bounds it by the live key set, so it catches up on current state instead
  of overflowing. This is the level-triggered contract, applied to
  delivery.
- **A subscriber whose view is reset gets a loud, scoped signal** — *if
  any mechanism resets one.* Its stream reports `Lagged` and closes, and
  the subscriber resubscribes for a fresh snapshot. Only the stale-watch
  pass would raise this, so `Lagged` exists only if that pass does. This
  rule is therefore conditional on phase 4's open decision, and it is the
  one rule in this policy that is. A retained object-watch poll repairs a
  stream in place, so it resets nothing and needs no signal. See
  [watch-push](watch-push.md).
- **Every goroutine a phase adds must end when its owner stops.** The test
  binary enforces this: `TestMain` in `testutils_test.go` reads the
  goroutine profile after a passing run and fails on any stack still in
  this module. A drain, a receiver's feeder and a subscriber's adapter each
  count, so every push phase inherits the check whether or not its test
  plan names it. This is not a style rule. A goroutine that outlives its
  test keeps polling for the rest of the binary, and what breaks is some
  later test's failsafe rather than the test that leaked.
- **Do not fence writes.** The system must not stop accepting writes when
  the push machinery degrades. A write fence lets one slow watch subscriber
  stop the full control plane, and the backstops make the fence
  unnecessary: a degraded push path is a latency problem, and the fence
  would turn it into an availability problem.

## The messaging backend

Do not hand-write the hub. Every push phase builds on
[`github.com/amorey/gobus`](https://github.com/amorey/gobus) `v0.4.0`, and
its `conflate` package supplies the exact bus each of them needs: one
producer, many consumers, one slot for each key, and per-key coalescing
through a caller-supplied `Merge`. Beehive keeps the policy — what a key
is, and what a merge does — and the library keeps the concurrency.

**It is already a direct dependency**, and already carries a shipped
consumer — but of its sibling package. The schedule watch runs on
`gobus/watch`, a keyed *state* bus, not on `conflate`. So no phase below
pays the dependency decision, and the receive-side machinery the two share —
per-subscriber receivers, `Chan()`, close precedence, teardown ordering — is
answered by working code rather than by a spec.

**`conflate`'s coalescing is not covered by that.** `Merge`, the pending-slot
queue and annihilation have no shipped consumer in beehive yet. Phase 1 is
where they get one.

`conflate.Hub[K, V]` gives each phase what it would otherwise build:

| What the spec asks for | What `conflate` gives |
| --- | --- |
| A pending set that removes duplicates | One slot for each key |
| "Never block on a subscriber" | `Send` never applies backpressure |
| "Conflate by object id, in arrival order" | Per-key `Merge`, and the key keeps its queue position |
| "Never conflate against what was delivered" | A delivered event has left the slots |
| "Memory bounded by what changed" | Memory bounded by the live key set |
| Create-then-delete annihilation | `Merge` returns `keep == false` |
| A `select`-able stream for each subscriber | `Receiver.Chan()` |

[`github.com/amorey/gochan`](https://github.com/amorey/gochan) is the
lower-level sister library, and beehive already depends on it: the tests
use `gochan/oneshot`. Neither module adds a transitive dependency beyond
`testify`, so this backend costs the embedder two direct modules and no
new indirect ones — both of which it already has.

Two rules for the code that uses it:

- **A gobus type must not reach the public API.** The hub is internal
  machinery. The watch surface keeps returning `<-chan ObjectChange`, as
  the naming ADR requires.
- **Beehive owns the merge, and each merge is a decision this spec states.**
  A merge is where beehive's level-triggered contract is enforced, so no
  merge may be written by a reader who is guessing.

## Phases

Each phase is one spec and one landing. **Every phase adds or switches; a
removal is always its own phase.** That rule is what makes the plan
bisectable: a delivery bug and a cleanup regression never share a commit.

Three pieces landed ahead of their phase, each because it needed neither a
hub nor `gobus`. They are marked below. A phase is still one landing; a
piece that is severable from its phase and blocked on nothing may go first.

The schedule watch is not a phase here. It was carved out and built
separately on `gobus/watch` in `4c8f607`, and this plan assumes it: `gobus`
is in `go.mod`, per-subscriber receivers feed public channels, and the
teardown ordering has a working answer. It does not exercise `conflate`.
See the [schedule-watch ADR](../adr/2026-07-27-schedule-watch.md).

**The phase numbers below are names, not an order.** They are cited from
every child spec, so they do not move. The order is
**2a → 1 → 2 → 4 → 5**: [new-edge-push](new-edge-push.md) goes first
because it is the smallest landing in the plan and it closes one of the two
holds on phase 5's number. It needs no hub and no `gobus`, so it proves
none of the push machinery, and phase 1 is still the pilot for that.

1. **[events-push](events-push.md) — push the event log.** The pilot. Add
   the hub and the commit hook for `EventsAdd`, and keep the poll. **The
   poll gate landed** (`1613685`): a quiet tick reads one scalar. The push
   half is still to do. This is
   a stream where push and poll can safely overlap, because `EventsWatch`
   already dedupes on `resource_version`, so a lost notify costs nothing
   while the machinery is new. It also has the smallest blast radius in the
   system: `EventsAdd` bumps no object `resource_version`, so it drives no
   reconcile and no wake.
2. **[wake-push](wake-push.md) — push the dependency wakes.** Add the wake
   hub and its commit hooks. The waker keeps scanning beside them,
   permanently. **The self-enqueue landed** (`60b4ea4`), on
   `Store.AfterCommit` rather than on the hub: a spec write now enqueues
   its own object. That is the gap that made phase 5 safe, so phase 5 is no
   longer gated on this phase. The new-edge stamp left too, for the same
   reason, and has its own spec:
   **[new-edge-push](new-edge-push.md)**. What remains here is the hub and
   the dependent wakes, which are the parts that read the store and batch.
3. **Dissolved.** This was `store-reads`, which grouped `EventsMaxVersion`
   and `ObjectsGetSummary` so that one `Store` break served both. The
   grouping rule is a post-release one — before the release there is no
   external backend to break, the same argument the
   [amend-in-place ADR](../adr/2026-07-31-amend-the-schema-in-place-until-release.md)
   makes for the schema — so each read now lands with the push path that
   needs it. `EventsMaxVersion` landed with the events poll gate
   (`1613685`); `ObjectsGetSummary` is specified inside phase 4, which is
   also where the decision on whether to build it at all lives. Group them
   again after the first release.
4. **[watch-push](watch-push.md) — push the object watches.** The per-kind
   hub, the merge policy and the writer-built deltas. Both
   `ObjectsWatchList` and `ObjectsWatch` convert together, because they
   share one poll engine, one hub and one subscribe sequence.
   **This phase opens with a decision that has to be settled before it is
   implemented**: whether the poll stays as the backstop, or is removed in
   favour of the stale-watch pass, `ObjectsGetSummary` and `Lagged`. The
   decision is at the top of that spec. Nothing before this phase is held
   by it.
5. **Lengthen the backstop intervals.** With push as the latency path,
   each backstop interval prices one thing only: how long a push bug may
   delay an object. Proposed defaults, to confirm before landing:
   - Owed pass: 30s → 5m.
   - Stale-dependents pass: 60s → 5m.
   - Dependency waker: keep 1s. Its tick is one index seek when nothing
     changed, and it is the only cover for a write this process did not
     make.
   - `SchedulesWatch` poll: already gone, so there is no number to set.
     `4c8f607` removed it, outside this plan.
   - GC sweeper: keep 30s. A cascade advances one step for each sweep, so
     its latency is the depth times the interval. Lengthen this interval
     only after the delete path and the cascade steps also push.

**Phase 5's gate is open for the owed pass, and shut for the rest.** The
gate was the self-enqueue: with nothing pushing an object's own reconcile,
the owed pass was the *primary* trigger for an ordinary spec write, and
lengthening it to five minutes would have made the commonest latency in the
system ten times worse. `60b4ea4` closed that, so the owed pass now
backstops a prompt path and its interval prices only how long a lost
enqueue may delay an object.

One thing still holds the owed-pass number, and it is small. The other is
now closed:

- ~~**The new-edge stamp does not push yet.**~~ **Closed.** A fresh
  `depends_on` edge now enqueues its source when it commits, so a first
  declare no longer waits for the pass. Landed by
  [new-edge-push](new-edge-push.md), which went first in the plan for this
  reason.
- **A write from outside this process pushes nothing.** The enqueue is
  registered in the beehive layer, so a second process, or the embedder
  writing through the `Store` they own, still waits for the pass. That is
  the same coverage argument that keeps the waker, and it is a reason to
  set the number with care rather than a reason to keep 30s.

Phase 5 still has no spec, and it must land last and alone. The numbers are
a policy decision, not a technical one: they set the staleness that a push
bug can cause. Do not lengthen an interval while a push path it backstops
is still new.

## Honest costs

- **Phase 2 trades query code for a dependency plus policy code.** It
  deletes the list-watch gate, the `seen` map and the tombstone memory. The
  queues, the conflation and the fan-out come from `gobus/conflate` rather
  than from new beehive code, so what beehive adds is the `Merge`, the
  delta type, the subscribe sequence and the teardown ordering. The test
  load still moves from SQL plans, which are cheap to pin, to
  interleavings, which are not — but the hardest of those interleavings are
  the library's to test, not beehive's.
- **Two new direct dependencies, and beehive is a library.** Every embedder
  inherits them. `gobus` and `gochan` each add nothing beyond `testify`,
  and beehive already requires `gochan`, so the real cost is one new module
  in the embedder's graph.
- **A library owns part of the contract now.** Conflation semantics,
  close precedence and the delivered-event rule live in `gobus`, so a
  change there can change what a beehive subscriber sees. Pin the version,
  and treat a `gobus` upgrade as a change to the watch contract until the
  release notes prove otherwise.
- **A forgotten notify in a future mutator is a silent bug.** The backstops
  turn it into bounded latency, and the last phase sets that bound.
  Each new write path must add its notify and its line in
  `docs/reconcile-triggers.md`.
