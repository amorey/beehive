# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

`README.md` is the spec. Where the code and the README disagree about a signature,
the code wins. Design rationale lives in [docs/adr](docs/adr/README.md); keep this
file to a summary plus a link, and put anything longer in a new ADR.

## Status

The README spec is implemented end to end and the suite is green. One loose end:
the `fakeStore` double in `testutils_test.go` still `panic`s on many methods; they
get filled in as tests need them.

Every example under `examples/` calls `Client.Requeue` after each create even
though a spec write now enqueues its own object — `Requeue` is what the docs point
callers at, and it covers a lost in-memory enqueue. `lowpower` is the one
exception, and deliberately: it exists to show that the pushes alone carried the
demo with every tick minutes away. `cascade` alone sets `WithGCInterval`, since
collection is what it demonstrates, and `lowpower` sets every public cadence.
Leave the production defaults alone otherwise.

Latency here is a configured interval, not a push path. If a push path is ever
added it belongs *above* this core; the
[drivers ADR](docs/adr/2026-07-28-periodic-scan-drivers.md) lists the constraints.

## Commands

```sh
go build ./...
go vet ./...
staticcheck -checks=all ./...   # CI runs this; -checks=all flags unused unexported code
go run ./examples/greeting/main.go   # the end-to-end smoke target
go run ./examples/events/main.go     # Events API demo: a connection-health panel
go run ./examples/lowpower/main.go   # every public cadence at minutes; pushes carry it
go test ./...
go test -run TestName ./  # single test

# Benchmarks live in *_bench_test.go and never run under `go test`; -bench opts
# them in, and -run '^$' keeps the tests from running alongside them.
go test -run '^$' -bench . -benchtime 2000x -count 3 ./
go test -run '^$' -bench . -benchtime 1x ./   # smoke: compiles and runs each once
```

## Writing standards

1. **Code — simple, idiomatic, easy for a human to follow.** Prefer the boring construction. Match the idiom of the file you are in over the one you would pick on a blank page. Cleverness that needs a comment to survive review is usually the wrong trade.
2. **Comments — terse, necessary, easy for a human to read.** Say what the code cannot: the *why*, the invariant, the trap that made this shape necessary. Never restate what the line already says. A comment justifying a choice the code no longer contains is dead weight — state the current design, don't argue against the alternatives you rejected (that is what `docs/adr/` is for).
3. **Documentation — simple, concise, easy for a human to read.** Lead with what is true now. One idea per sentence.

The failure mode to watch for is a comment addressed to a *reviewer* — someone who just watched the reasoning — rather than to a reader opening the file cold. The tell is a comment that spends its length on the option **not** taken.

A `UserPromptSubmit` hook (`scripts/writing-standards-hook.sh`, wired in `.claude/settings.json`) restates this at the start of every turn, since the rule has to be in mind *before* anything is composed. Keep the script's short form in sync with this section.

## Architecture

Beehive is an embedded, Kubernetes-inspired control plane backed by a durable store.

- **One process, one `Beehive`, and it is the store's only writer.** Two
  processes over one file, two `Beehive` values over one store, and any
  out-of-band access to the database while a `Beehive` runs — an external tool,
  or a `Store` call behind the running `Beehive`'s back — are all **unsupported**,
  not degraded. Restarts are supported and are not what this excludes; the
  constraint is on *concurrent* access. Documented, not enforced. Do not justify
  a driver, a tick or a backstop by "a second process could write the store", and
  do not accept a bug report whose repro needs one. → [ADR](docs/adr/2026-08-05-one-process-one-beehive-sole-writer.md)
- **Nothing store-backed is pushed, and every driver over the store is a
  periodic scan except the waker** (`internal/driver`). Seven drivers: the owed
  pass (unsettled specs plus `reconcile_owed`, per-kind, 30s), the full pass
  (`WithFullPassInterval`, off by default), the GC sweeper (`WithGCInterval`,
  **cannot be disabled**), the dependency waker (write log, **wake-driven, no
  tick**), the stale-dependents pass (60s, **cannot be disabled**), the object
  watch tail and the event watch (both `WithWatchFloorInterval`, 30s, each with
  a commit wake in front). **Five cadences are public** — `WithGCInterval`,
  `WithFullPassInterval`, `WithOwedPassInterval`, `WithStaleDependentsInterval`,
  `WithWatchFloorInterval` — because every trigger pushes at commit, so what a
  tick paces is recovery of a *lost* push rather than latency; only the full pass
  may be disabled, and the defaults are unchanged. The floors on active work
  (`minRequeueInterval`, the two scan floors, `wakePersistInterval`) stay
  unexported, every retry ladder is capped on a constant of its own rather than
  on one of these, and the GC sweeper's per-sweep budgets scale with its
  interval. `Client.Requeue` is the public way to beat a cadence.
  → [ADR](docs/adr/2026-08-06-driver-cadences-are-configurable.md) **No reconcile may depend on
  the *periodic* full pass** — its cost scales with the object count and it repeats
  forever. The **startup** pass is O(objects) once per process, and a kind whose
  reconcile establishes in-process state (a connection, a worker, a liveness
  condition) may depend on it: it guarantees every object of that kind one reconcile
  per process, which no store column can express.
  → [ADR](docs/adr/2026-08-07-the-startup-pass-may-be-depended-on.md) The schedule watch is
  the one push exception (see below); the two watch wakes are not, because their
  floor tick stays. **The waker is the exception to the cadence, not to the
  record**: it still reads the write log, but only a commit makes it look, and
  what entitles it to that is the stale-dependents pass finding a superset of
  what it finds. A write this beehive did not publish reaches its dependents on
  no schedule this package promises — it is out of scope, not slow. Both wakes
  are rate-limited (`internal/rategate`), and the waker's
  cursor write keeps a floor of its own so a faster loop is not a faster write.
  **The subscription and the watermark are both taken inside `Start`**, in that
  order, so no write a caller can make after `Start` returns is below the
  watermark or unheard. A failed seed does not abort startup: the loop retries it
  on the backoff, and with no stored cursor that retry reseeds at the mark as of
  *then* — the one seed window left, and `docs/TODO.md` carries it.
  A drain that pages without a break for one stale-dependents interval **stops and
  jumps to the write log's mark** — the backstop has already swept that range. A
  failed mark read restarts that window rather than retrying per pass, so the bound
  is a window per failed read, not one window.
  → [ADR](docs/adr/2026-07-28-periodic-scan-drivers.md),
  [ADR](docs/adr/2026-08-05-a-commit-wakes-the-dependency-waker.md),
  [ADR](docs/adr/2026-08-05-the-waker-is-wake-driven.md),
  [ADR](docs/adr/2026-08-05-the-waker-abandons-an-overtaken-drain.md),
  [ADR](docs/adr/2026-08-06-the-waker-seeds-before-start-returns.md). Every reconcile trigger
  is mapped in [docs/reconcile-triggers.md](docs/reconcile-triggers.md) — update
  it when you add one.
- **The work queue floors how often one object is dispatched**, and a pending
  backoff or floor alarm absorbs an arriving wake rather than jumping it. The
  floor is per id (`internal/rategate`), so N distinct objects cost nothing and
  one object N times costs N intervals — which is the shape of a dependency
  cycle. It bounds a cycle; it does not converge one. A zero interval turns off
  the floor but **not** the absorption, which is a semantic change of its own.
  → [ADR](docs/adr/2026-08-04-work-queue-re-enqueue-floor.md)
- **Declarative and level-triggered.** Controllers reconcile from *current*
  state, never from a sequence of changes; controllers coordinate through the
  store, never with each other.
- **Dependency staleness is derived from watermarks, and every finding is
  durable.** A successful reconcile stamps
  `dependency_watermarks.reconciled_against` with the write cursor as of its
  *load*; the stale-dependents pass finds every dependent a target has moved
  past, so any lost wake costs latency, never divergence. `Edges().Add` *clears*
  the watermark on a new `depends_on` edge; the `reconcile_owed` stamp on that
  same edge is what guarantees the dependent a pass. A **failed** watermark write
  needs no compensating record: it leaves the watermark low, and low only
  over-reports staleness.
  → [ADR](docs/adr/2026-07-29-dependency-watermarks.md)
- **The stale-dependents pass scans from a cursor over target
  `resource_version`** (`Dependencies().ListStaleSince`), so its cost is what
  changed, not the size of the graph. **The cursor is process-local and never
  persisted**, so every process re-derives once and recovers a wake lost in
  memory. **A lost watermark write is not a strand**: `reconciled_against` is
  read in one place, where a lower value selects more, so a target change the
  reconcile did not observe is above the sweep's cursor and the next sweep
  lists it. What is given up is a re-report for a change already observed. This
  pass *also* stamps `reconcile_owed` for what it finds before enqueuing, so a
  finding outlives the queue; that stamp is what a persisted cursor would need,
  and it is not load-bearing today. The cursor moves only when a sweep reaches
  the end. **`reconcile_owed` has two producers** — `Edges().Add` and this pass;
  the owed pass is its consumer, not a third. → [ADR](docs/adr/2026-08-03-stale-dependents-cursor.md)
- **The dependency waker scans the write log from a watermark**
  (`ObjectWrites().ListSinceAll`, paged, store-wide — an edge can point at a
  client-only kind). Cost is bounded by what changed. **A commit is the only
  thing that wakes it**: an idle waker arms no timer and issues no query, so a
  dependency chain propagates per commit. Two conditions re-arm its one timer,
  neither periodic: a failed scan (`driver.Backoff`, 100ms up to `wakeRetryMax`,
  its own constant and not the backstop's cadence — without it a failed scan
  would wedge, since `backingOff` drops arriving wakes) and a cursor row still
  below the watermark, which would otherwise be retried only by a commit that may
  never come. Going
  idle **stops** the timer, or one already ready drives a pass nobody asked for.
  The cursor persists via `Store.DriverCursors().Set`; it is an optimisation
  over the stale-dependents pass, never a guarantee. **Both store-wide reads
  report the retention horizon** beside their value rather than folded in — the
  abandon jump needs the bare mark — so a cursor below the boundary is warned about
  once instead of skipping silently. The horizon **moves no cursor**: it is a max
  over kinds, and the per-kind count bound trims a chatty kind past entries a quiet
  one still holds, so it proves a loss without bounding an empty range.
  → [ADR](docs/adr/2026-07-30-durable-waker-cursor.md),
  [ADR](docs/adr/2026-08-06-the-waker-sees-a-retention-trim.md),
  [ADR](docs/adr/2026-08-05-a-commit-wakes-the-dependency-waker.md),
  [ADR](docs/adr/2026-08-05-the-waker-is-wake-driven.md)
- **Object writes are recorded in an append-only log** (`object_writes`), one
  entry per committed write, in that write's transaction. A create/update entry
  carries no payload — consumers route by id and read current state; a physical
  delete carries a row image, since nothing survives to be read. The soft delete
  is an ordinary update. Retention is per kind and **bounded by default** (24h),
  unlike the event log, because entries land at reconcile rate; what it trims is
  recorded per kind in `object_writes_horizon`, and that horizon is the resume
  boundary. → [ADR](docs/adr/2026-08-02-object-write-log.md)
- **Client watches return a stream — snapshot, `Changes`, and an `Err()` —
  and subscribe to their kind's shared tailer** (`objectswatch.go`). The
  terminal failure is stored in a `streamFail` slot **before** the channel
  closes, never delivered as a change; `Watch` shares the slot of the list
  stream it adapts, so a copy there would report nil forever. A nil `Err()`
  after the close means the caller's own context ended.
  One tailer per kind owns the cursor, so reads scale with watched kinds, not
  watch count: a quiet read costs one
  `ObjectWrites().MaxVersion` (which folds in the horizon so it only rises — gate on
  `>`, not `!=`), a busy one reads the entries above the cursor and then one
  batched `Objects().ListByIDs`, draining until a page comes back short. A commit
  wakes it (`signalKindWritten`, `AfterCommit`), and the same signal wakes the
  dependency waker, which subscribes across every kind rather than to one; the
  emit table is derived from the store's write-log call sites, **not** from the public verbs — conditions
  reach the log through `bumpObject`, and the owner cascade is routed by the refs
  it returns. The fan-out is non-generic (`rawChange`) because two clients may
  watch one kind with different type parameters; each subscriber decodes and
  drops what its own snapshot already held. Delivery is latest-per-object, so an
  `Added` may repeat for a snapshot object. **A batch is delivered ascending by
  resource version** (`drainPending`), and that is load-bearing rather than
  tidiness: a caller checkpoints a delivered change's version and resumes above
  it, so a version delivered after a higher one would be skipped for good. The
  merge coalesces in place, which leaves a re-written object at its original
  queue position carrying a newer version — the one way the drain sees them out
  of order. The batch's membership is one `TryRecvAll` cut, not a `TryRecv`
  loop: everything pending as of one instant, so a later batch cannot carry a
  lower version. Nothing is dropped in the merge. A cursor **below** the horizon
  (strictly: equality has lost nothing) ends *every* subscriber with
  `ErrWatchTooOld` and resets the tailer. **A tailer runs from its kind's first
  watch to its last**: `tailerFor` hands back a subscriber lease and every
  caller owes one `release`, with the count and the build both under `tailMu` —
  the same lock the registry moves under, which is what closes the teardown
  race. Presence in the registry is not health: a tailer that reset stays there
  until its subscribers release, so `tailerFor` checks both or a resubscribe
  rejoins the tailer that just failed. **Every drain start is floored**
  (`internal/rategate`, 100ms) — a floor tick takes the slot the same as a wake,
  so a commit landing just after one waits out the rest of the window — and one
  drain is bounded by a page budget, so a write stream cannot make a tailer hold
  the single connection away from the writers waking it; the first drain after a
  quiet period is still eager.
  → [ADR](docs/adr/2026-08-03-watch-shared-tail.md),
  [ADR](docs/adr/2026-08-05-the-object-tail-throttles-its-drains.md)
- **`WatchOwnedObjects` scopes a watch to one owner's children, and reads
  ownership from current state.** Never from the log: a create's entry is
  appended *before* its `owned_by` edge, in the same transaction, so a
  denormalised `owner_id` would be NULL on the write that matters most. The
  tailer resolves a page's owners in one `Edges().GroupOutgoingByID` beside the
  batched object read; a collected child has no edges left, so it takes its owner
  off the delete entry's row image. **The lookup's gate (`ownerScoped`) is set
  before a scoped subscriber registers and is never cleared** — a change
  published without an owner while one is live is dropped silently and forever,
  which is also why `decodeChanges` warns on a nil owner it did not expect. It is
  sound for a snapshotting subscriber because anything above its snapshot was
  read after the flag was set, and for a resuming one **only because `replay`
  runs to the head**. Unlike `Watch(id)` there is no key filter: membership is
  what the watch exists to learn. All of it rests on **ownership changing only
  through a logged write to the child** — true by construction, pinned
  structurally by `TestOwnedByIsWrittenInOnePlace`, and the thing a re-parent
  verb would have to preserve. `ListDependents`/`ListDependencies` have no watch
  counterpart for the harder reason: `depends_on` edges are mutable and log
  nothing. → [ADR](docs/adr/2026-08-06-owner-scoped-watches.md)
- **The event watch reads one object's log above a cursor, one reader per
  watch** (`eventswatch.go`). An extend re-samples `events.resource_version`, so
  "runs above the cursor" is exactly what changed and the old `seen`/`EventID`
  diff is gone. `ControllerClient.AddEvent`'s commit wakes it through `eventWriteHub` (keyed by
  id, not kind); the floor tick covers a foreign writer. Nothing is shared — the
  read is already per object and already indexed — so there is no lease
  machinery, and no merge either: the stream is unbuffered, so a consumer that
  stops reading pins the cursor, which is what makes `ErrWatchTooOld` reachable
  live. `Events().Snapshot` reads runs and position in one transaction, because two
  reads either drop a write or deliver it twice. **A read must not imply an
  absence it cannot vouch for**: `events_horizon` records what retention trimmed,
  per `(object, category)` to match the ring cap's partition, and a resume below
  it is refused rather than answered; a *collected* object ends its streams with
  `ErrNotFound`, since its log and its horizon cascade away together. The horizon
  is only as good as the sweep's `last_at` clock, and it errs toward
  over-reporting. **All three reads take an id and are not kind-scoped** —
  `ListEvents`, `GetLatestEvent`, `WatchEvents` — because `events` has no kind
  column to scope by; the reader's probe (`checkExists`) only latches that a row
  is there, which is what keeps "not created yet" apart from "collected". The
  write is the asymmetry: `AddEvent` stays kind-scoped. `WatchEvents` still needs
  a registered controller for the *client's* kind, a property of the caller and
  not of the target.
  → [ADR](docs/adr/2026-08-05-events-get-a-cursor-and-a-commit-wake.md),
  [ADR](docs/adr/2026-08-13-the-event-reads-take-an-id.md)
- **`Spec`/`Status` separation is structural.** Only
  `Controller`/`ControllerClient` writes status.
- **Reconcile is not transactional.** Each `ControllerClient` write commits on
  its own; `Within` groups writes that must land together. Id-keyed writes are
  scoped to the caller's `GroupKind` (`ErrWrongKind`). There is no name-keyed
  upsert — none of `Create`/`GetOrCreate`/`Delete` writes to a row it found. **A
  write's durable record is what a driver lists**: a spec write bumps the
  generation, a delete sets `deletion_requested_at`. A spec write also enqueues
  its own object, gated on the store's `changed` bool — never on the row being
  unsettled; a delete does the same, gated on `marked`. `Store.AfterCommit` has
  twelve users: `WithOnCreate`, the spec-write enqueue, the new-edge enqueue, the
  delete-request enqueue, the cleared-finalizer enqueue, the dropped-dependency
  target push, the create's push of an already-deleting owner (all shared via
  `Beehive.signalRequeueNow` and `signalRequeueThrottled`), the GC cascade's own
  hook, the physical delete's owner push, the delete request's push of the
  targets its mark unblocked (all three `signalRequeueManyNow`),
  `signalKindWritten` — which feeds the watch tailers and the dependency waker —
  and `signalEventsWritten`, which feeds one object's event readers.
  → [ADR](docs/adr/2026-07-27-name-keyed-writes.md),
  [ADR](docs/adr/2026-07-31-a-spec-write-enqueues-its-own-object.md),
  [ADR](docs/adr/2026-08-04-a-delete-request-pushes-its-own-collect.md),
  [ADR](docs/adr/2026-08-05-a-cleared-finalizer-pushes-its-own-collect.md),
  [ADR](docs/adr/2026-08-05-a-physical-delete-pushes-its-owner.md),
  [ADR](docs/adr/2026-08-05-a-dropped-dependency-pushes-its-target.md),
  [ADR](docs/adr/2026-08-05-a-create-pushes-a-deleting-owners-collect.md),
  [ADR](docs/adr/2026-08-06-a-deletion-mark-pushes-the-target-it-unblocks.md)
- **The id is the key everywhere; the name is a lookup.** The bare CRUD verbs
  take an `ObjectID` and act on one incarnation; the `…ByName` siblings act on
  whatever holds the name *now*, resolving and writing in one transaction. The
  name is positional on `Create`/`GetOrCreate` only, where there is no id yet.
  Read-modify-write needs no rule: the read hands back `ID`. Names are required,
  immutable, opaque; `""` is rejected with `ErrInvalidName` in the store itself.
  A taken name (tombstones included) is `ErrNameTaken`; `GenerateName(prefix)`
  builds one and callers bound-retry on the sentinel. Foreign keys stay integer
  ids, which are never reused.
  → [ADR](docs/adr/2026-08-02-id-primary-key-with-byname-siblings.md)
- **A store write takes only what it honours and returns only what a caller
  reads.** `Objects().Create` takes `ObjectsCreateInput`; only it and the
  `Objects().UpdateSpec*` mutators return a row; `Events().Add` takes
  `EventsAddInput`. → [ADR](docs/adr/2026-07-30-store-write-shapes.md)
- **A nested `Within` is a real rollback boundary** (SAVEPOINT): an error
  unwinds that frame's writes and queued hooks even if the caller swallows it. A
  nested `Within` from a sibling goroutine is refused with
  `ErrConcurrentNestedTx`. → [ADR](docs/adr/2026-07-29-nested-within-savepoints.md)
- **The generic boundary is `Register`**, which wraps the typed `Controller` in
  the non-generic `typedController` adapter (`reconciler.go`). Keep new internal
  machinery non-generic.
- **Options dispatch on their target's type** and ignore targets they don't
  recognize, so the same option works at `New`, `Register`, or per call.
- **GC has two backstops**: each reconcile loop runs `gcCollect` for its own
  kind (routing finalizer clearing through the controller), and the global
  sweeper covers client-only kinds. Both are idempotent. A delete request, a
  cascade, a physical delete and a dropped dependency each enqueue at commit for a
  registered kind, so a cascade advances a level per commit and unwinds a level per
  commit; a client-only level still costs a sweep. **A deletion mark also pushes
  the deletion-pending targets it discounted**, since that mark is what lifts
  their RESTRICT — from both the client delete and the cascade's marked children.
  **A create under an owner that is already deleting pushes that owner**, whose
  re-cascade is what marks the new child — gated on the owner's
  `deletion_requested_at`, which `Edges().Add` now reports.
  **The sweeper also reclaims `reconcile_owed` for kinds with no reconcile loop**,
  which nothing else drains — safe because the count is redundant with the
  dependency watermark `Edges().Add` clears, so a cursor-0 sweep in a later process
  re-derives it. The clear is no-emit.
  → [ADR](docs/adr/2026-08-04-a-delete-request-pushes-its-own-collect.md),
  [ADR](docs/adr/2026-08-05-a-physical-delete-pushes-its-owner.md),
  [ADR](docs/adr/2026-08-05-a-dropped-dependency-pushes-its-target.md),
  [ADR](docs/adr/2026-08-05-a-create-pushes-a-deleting-owners-collect.md),
  [ADR](docs/adr/2026-08-06-a-deletion-mark-pushes-the-target-it-unblocks.md),
  [ADR](docs/adr/2026-08-05-reclaim-a-client-only-owed-count.md)
- **The store is `auto_vacuum=INCREMENTAL`**, set on the DSN (SQLite ignores the
  pragma on a non-empty database and inside a transaction — which a migration
  is). The sweeper drains the freelist through `Store.ReclaimSpace`, gated on a
  floor and a fraction of the file. `PRAGMA incremental_vacuum` **must be
  `Exec`'d, never `Query`'d**. → [ADR](docs/adr/2026-07-29-auto-vacuum-incremental.md)
- **The schema is amended in place until the first release**: `sqlite/migrations/`
  holds exactly one file, and `TestTheSchemaIsOneMigration` is the tripwire.
  → [ADR](docs/adr/2026-07-31-amend-the-schema-in-place-until-release.md)
- **The generation handshake is beehive's write, not the controller's.**
  `Generation` increments on every real spec change; `ObservedGeneration` records
  what the controller last settled. Byte-identical writes are skipped, which is
  what stops a controller re-applying its own spec from waking itself forever.
  `Reconcile` returns `Settled`/`Unsettled`/`Fail` — with the next pass scheduled
  by `RequeueAfter`, whose explicit zero is the queue floor on either kind, and a
  bare `Unsettled` scheduling itself at the owed pass's cadence because the
  unsettled listing gates on the generation and would not list it — and beehive stamps `observed_generation`
  from **the object it handed out**, never a fresh read.
  `UpdateStatus` writes status alone, which is what leaves
  `Objects().SetObservedGeneration` the sole writer of that column.
  → [ADR](docs/adr/2026-08-18-beehive-owns-the-generation-handshake.md)
- **A `ControllerClient` exists only for the pass it is handed to, and writes
  only that pass's object**, because beehive concludes a pass by stamping the
  generation it handed out. Every method fails with `ErrReconcileReturned` once
  `Reconcile` returns, and `Register` hands back nothing, so there is no other way
  to hold one. A fail-fast, not a barrier. **No method takes the object's id**:
  the client is bound at construction and the only ids left name the *other* end
  of an edge, so a sibling write — which would race that object's own pass and
  settle nothing — cannot be expressed. `ErrWrongKind` is unreachable from it as a
  result. What that removes rather than relocates is a declare or an event append
  made on another object's behalf, both in `docs/TODO.md`; the reads relocate to
  `Client`, `HasIncomingEdges` excepted. The cleared-finalizer push survives the
  narrowing but nothing depends on it any more — the pass's own tail `gcCollect`
  covers every ordering.
  → [ADR](docs/adr/2026-08-18-a-controller-client-exists-only-for-a-pass.md)
- **`TestClient` writes status and conditions outside a pass**, for the fixture
  a `ControllerClient`'s pass scoping made unbuildable: a controller reading
  *another kind's* status needs a stored one. Last resort, not first — a
  controller test that needs only the object it is handed calls `Reconcile`
  directly against a fake `ControllerClient`. It lives in package `beehive`
  rather than a `beehivetest` sub-package: a sub-package cannot reach `bh.store`
  or the write hub, so it would need a seam, and the seam buys only a package
  name to hide the warning in — the constructor is exported either way. It
  builds, per call, a `ControllerClient` nothing ever ends — `live()` closes only
  when the reconcile loop calls `end()` — so the writes have one implementation,
  and it never stamps `observed_generation`. It keeps its `ObjectID` arguments,
  which is what a bound client cannot take, and is now the only surface that
  returns `ErrWrongKind`.
  → [ADR](docs/adr/2026-08-18-a-test-client-writes-status.md)
- **A downgraded liveness condition says so.** `downgradeLiveness` sets
  `Condition.Unconfirmed` beside the `Unknown` rewrite, because the predicate
  (`UpdatedAt` before `processStart`) is store-internal and a remote consumer
  cannot evaluate it even in principle — so `{Unknown, Liveness}` alone cannot
  distinguish a prior process's unrefreshed report from an assessment that ran and
  came back inconclusive. Read-only: `SetConditions` doesn't copy it and
  `conditionUnchanged` doesn't compare it, so echoing a read condition back stays a
  no-op. Both read paths already funnel through `downgradeLiveness`, so there is no
  third site. `Reason`/`Message`/both stamps stay the pre-downgrade write's — last
  known values, not facts about the `Unknown`.
  → [ADR](docs/adr/2026-08-07-a-downgraded-liveness-condition-says-so.md)
- **Schema-version migration** (`Migrator`): per-kind, on read, at the decode
  boundary; spec and status version independently; an undecodable blob
  quarantines its row rather than killing the read.
  → [ADR](docs/adr/2026-07-27-schema-version-migration.md)
- **Every new `depends_on` edge stamps an owed reconcile** (`reconcile_owed`),
  atomically with the edge inside `Edges().Add`, drained by the owed pass. The
  declaration also enqueues the source at commit, gated on
  `ReconcileOwedStamped` and routed by `EdgesAddResult.From` (the edge is
  cross-kind). The edge push is throttled; the spec write's is not.
  → [ADR](docs/adr/2026-07-29-stamp-every-new-dependency-edge.md),
  [ADR](docs/adr/2026-07-31-a-spec-write-enqueues-its-own-object.md)
- **Secondary lookups are read on request**, never folded into the blob-carrying
  `SELECT`. Eager `LoadOption`s and lazy getters share loaders; accessors return
  `ErrNotLoaded` for a relation nobody asked for.
  → [ADR](docs/adr/2026-07-27-secondary-lookups.md)
- **Events are an append-only log, aggregated into runs** per (object,
  category), extended when `(type, reason)` matches. Reads live on `Client`.
  "Event" means this log and nothing else. `Store.Events().Add` returns `error`
  alone — the watch builds its delta from a cursor, not from the write's result.
  **Retention runs in the GC sweeper and is off by default**: a cap of *runs*
  per timeline, which trims only the timelines a candidate query finds over it
  (bounded per sweep, so it is progressive), plus an optional flat `maxAge`
  cutoff across every timeline. **`EventStream.Retention` reports that
  configuration**, clamped to what the sweeper's `> 0` gates enforce, so a
  consumer bounds its own in-memory list without mirroring the config; a prune
  is still never delivered.
  → [ADR](docs/adr/2026-07-27-events-api.md),
  [ADR](docs/adr/2026-08-05-events-get-a-cursor-and-a-commit-wake.md),
  [ADR](docs/adr/2026-08-06-event-retention-is-a-ring-per-timeline.md)
- **The schedule watch is an in-memory gauge and the one watch with no tick at
  all**: the `workQueue` publishes each move of its `gauge` to a `gobus/watch`
  hub. Sound only because the queue is unexported and process-local and the
  gauge reports every move from one type — give `workQueue` a second writer and
  the poll has to come back. (The object tail also has a wake, but it keeps a
  floor tick, so it is a driver rather than an exception.) Streams end when the
  beehive stops, after the final value.
  → [ADR](docs/adr/2026-07-27-schedule-watch.md)

## Conventions

- **On `Store` the noun names a type; on the client surfaces the method is
  `VerbNoun`.** On `Store` each
  family is a sub-API reached through an accessor (`store.Edges().Add`,
  `store.Objects().ListUnsettledIDs`), so no method carries its family in its
  name. A member with no family behind it sits on the root and carries the noun
  in the name, verb first (`GetLatestResourceVersion`, `ReclaimSpace`). A family is earned by a **protocol worth its own
  noun**, usually a table: `finalizers` is one column and one method, so it went to
  `Objects` (`Objects().DeleteFinalizer`), while `reconcile_owed` and
  `deletion_requested_at` are columns on `objects` that carry four methods each and
  keep families of their own. Cardinality stays
  in the verb (`Get`/`Watch` for one, `List`/`WatchList` for many). On
  `Client`/`ControllerClient` the method is `VerbNoun`: the receiver is already
  its kind, so its own CRUD stays bare (`Get`, `WatchList`) and only its
  secondary nouns are spoken — singular for one, plural for many
  (`SetCondition`, `AddEvent`, `ListEvents`, `GetOwner`). A qualifier naming a
  key you pass trails (`GetByName`); an adjectival one leads
  (`GetLatestEvent`, `HasIncomingEdges`). List interface members
  alphabetically. `Err*`, `With*` and external-interface methods are exempt, as
  are `Object`'s relation accessors. A watch returns a **stream value** whose
  channel field carries what it streams — `NounChange` over a change stream,
  the value itself over a log — plus an `Err()` for why it ended; `WatchSchedule`
  is the exception, a gauge with no failure to report, so it returns the bare
  channel.
  → [ADR](docs/adr/2026-08-07-verb-noun-on-the-client-surfaces.md),
  [ADR](docs/adr/2026-08-13-a-stream-reports-its-failure-beside-itself.md)
- **Whitebox tests**: tests go in `package beehive`, so they reach unexported
  machinery.
- **Test files mirror source files, not features.** Shared helpers and fakes go
  in `testutils_test.go`. Benchmarks mirror the same way but in
  `<source>_bench_test.go`, so a semantics file never carries a load harness.
  No build tag: `go test` already skips benchmarks, and a tag only hides them
  from `go vet` until they stop compiling.
- **Assertions use `stretchr/testify`**: `require` for preconditions, `assert`
  for independent checks.
- **Synchronize on signals, never on sleeps.** The only use of `time` is a
  generous failsafe timeout in a `select`.
- **Comments are terse; the code speaks for itself.** Concretely:
  - Exported identifiers get godoc of 1–3 short sentences stating the contract:
    behavior, error sentinels, scoping (e.g. "Scoped to gk: wrong kind →
    ErrWrongKind, missing id → ErrNotFound"). Not more.
  - Unexported identifiers get a one-line comment only when the name doesn't
    already say it; otherwise no comment.
  - Keep single-line invariant/trap comments the code cannot express — ordering
    constraints, lock rules, gates ("must be Exec'd, never Query'd"; "checked
    before the target switch"). These are the comments worth having.
  - Never write design narration, alternatives considered, history ("this used
    to…"), or cross-reference essays in code. If the why needs a paragraph,
    write an ADR and leave one line: `// See docs/adr/<file>.`
  - Don't restate what the next line does, and don't argue that the code is
    correct — state the constraint and stop.
- **Commits are terse conventional commits, written for a human reader.**
  `type(scope): subject` —
  `feat`/`fix`/`perf`/`refactor`/`test`/`docs`/`chore`, scope only when it adds
  information (`sqlite`, `edges`, `watch`), `!` for a breaking change. Subject
  is imperative, lower-case, no period, ≤72 chars, and says what the change
  does, not how (`feat(edges): enqueue an edge's source when the declaration
  commits`). No body unless the why isn't obvious from the diff — then 1–3
  plain sentences, no bullet lists; rationale longer than that is an ADR the
  body links. Optimise for someone reading `git log`: the subject is the whole
  message for most commits, so spend its budget on what changed, never on
  restating the diff or padding the format.
- **Pull requests follow
  [`.github/pull_request_template.md`](.github/pull_request_template.md)**: keep
  its sections (`Summary` for the why, `Key Changes` for the what, `Checklist`),
  and lead the title with the template's emoji for the change type — 🎣 bug fix,
  🐋 new feature, 📜 documentation, ✨ general improvement.
- **Stubs are explicit**: `panic("not implemented: <name>")`; stub options
  return `nil` and are marked `(stub: not yet wired up)`.
- **Design rationale goes in an ADR**, not here. See
  [docs/adr/README.md](docs/adr/README.md) for the format.
