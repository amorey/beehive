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
callers at, and it covers a lost in-memory enqueue. `cascade` alone sets
`WithGCInterval`, since collection is what it demonstrates. Leave the production
defaults alone otherwise.

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
go test ./...
go test -run TestName ./  # single test

# Benchmarks live in *_bench_test.go and never run under `go test`; -bench opts
# them in, and -run '^$' keeps the tests from running alongside them.
go test -run '^$' -bench . -benchtime 2000x -count 3 ./
go test -run '^$' -bench . -benchtime 1x ./   # smoke: compiles and runs each once
```

## Architecture

Beehive is an embedded, Kubernetes-inspired control plane backed by a durable store.

- **Nothing store-backed is pushed, and every driver over the store is a
  periodic scan except the waker** (`internal/driver`). Seven drivers: the owed
  pass (unsettled specs plus `reconcile_owed`, per-kind, 30s), the full pass
  (`WithFullPassInterval`, off by default), the GC sweeper (`WithGCInterval`,
  **cannot be disabled**), the dependency waker (write log, **wake-driven, no
  tick**), the stale-dependents pass (60s, **cannot be disabled**), the object
  watch tail (`withWatchFloorInterval`, 30s, with a commit wake in front) and
  the event watch poll (1s). Only `WithFullPassInterval` and `WithGCInterval`
  are public; the other cadences are unexported options only tests reach, and
  `Client.Requeue` is the public way to beat one. **No reconcile may depend on
  either full pass** — both scale with the object count. The schedule watch is
  the one push exception (see below); the tail's commit wake is not, because its
  floor tick stays. **The waker is the exception to the cadence, not to the
  record**: it still reads the write log, but only a commit makes it look, and
  what entitles it to that is the stale-dependents pass finding a superset of
  what it finds. So a write this process did not publish — a second process, or
  one issued straight to the `Store` — reaches its dependents in 60s rather than
  on a tick. Both wakes are rate-limited (`internal/rategate`), and the waker's
  cursor write keeps a floor of its own so a faster loop is not a faster write.
  → [ADR](docs/adr/2026-07-28-periodic-scan-drivers.md),
  [ADR](docs/adr/2026-08-05-a-commit-wakes-the-dependency-waker.md),
  [ADR](docs/adr/2026-08-05-the-waker-is-wake-driven.md). Every reconcile trigger
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
  past, so any lost wake costs latency, never divergence. `EdgesAdd` *clears*
  the watermark on a new `depends_on` edge; the `reconcile_owed` stamp on that
  same edge is what guarantees the dependent a pass. A **failed** watermark write
  needs no compensating record: it leaves the watermark low, and low only
  over-reports staleness.
  → [ADR](docs/adr/2026-07-29-dependency-watermarks.md)
- **The stale-dependents pass scans from a cursor over target
  `resource_version`** (`DependentsListStaleSince`), so its cost is what
  changed, not the size of the graph. **The cursor is process-local and never
  persisted**, so every process re-derives once and recovers a wake lost in
  memory. **A lost watermark write is not a strand**: `reconciled_against` is
  read in one place, where a lower value selects more, so a target change the
  reconcile did not observe is above the sweep's cursor and the next sweep
  lists it. What is given up is a re-report for a change already observed. This
  pass *also* stamps `reconcile_owed` for what it finds before enqueuing, so a
  finding outlives the queue; that stamp is what a persisted cursor would need,
  and it is not load-bearing today. The cursor moves only when a sweep reaches
  the end. **`reconcile_owed` has two producers** — `EdgesAdd` and this pass;
  the owed pass is its consumer, not a third. → [ADR](docs/adr/2026-08-03-stale-dependents-cursor.md)
- **The dependency waker scans the write log from a watermark**
  (`ObjectWritesListSinceAll`, paged, store-wide — an edge can point at a
  client-only kind). Cost is bounded by what changed. **A commit is the only
  thing that wakes it**: an idle waker arms no timer and issues no query, so a
  dependency chain propagates per commit. Two conditions re-arm its one timer,
  neither periodic: a failed scan (`driver.Backoff`, 100ms up to the
  stale-dependents cadence — without it a failed scan would wedge, since
  `backingOff` drops arriving wakes) and a cursor row still below the watermark,
  which would otherwise be retried only by a commit that may never come. Going
  idle **stops** the timer, or one already ready drives a pass nobody asked for.
  The cursor persists via the optional `DriverCursorer`; it is an optimisation
  over the stale-dependents pass, never a guarantee.
  → [ADR](docs/adr/2026-07-30-durable-waker-cursor.md),
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
- **Client watches return a snapshot and subscribe to their kind's shared
  tailer** (`objectswatch.go`). One tailer per kind owns the cursor, so reads scale
  with watched kinds, not watch count: a quiet read costs one
  `ObjectWritesMaxVersion` (which folds in the horizon so it only rises — gate on
  `>`, not `!=`), a busy one reads the entries above the cursor and then one
  batched `ObjectsListByIDs`, draining until a page comes back short. A commit
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
  `EventsWatch` still polls and diffs, gated on `EventsMaxVersion`.
  → [ADR](docs/adr/2026-08-03-watch-shared-tail.md),
  [ADR](docs/adr/2026-08-05-the-object-tail-throttles-its-drains.md)
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
  seven users: `WithOnCreate`, the spec-write enqueue, the new-edge enqueue, the
  delete-request enqueue, the cleared-finalizer enqueue (all shared via
  `Beehive.signalRequeueNow` and `signalRequeueThrottled`), the GC cascade's own
  hook, and `signalKindWritten` — which feeds the watch tailers and the
  dependency waker.
  → [ADR](docs/adr/2026-07-27-name-keyed-writes.md),
  [ADR](docs/adr/2026-07-31-a-spec-write-enqueues-its-own-object.md),
  [ADR](docs/adr/2026-08-04-a-delete-request-pushes-its-own-collect.md),
  [ADR](docs/adr/2026-08-05-a-cleared-finalizer-pushes-its-own-collect.md)
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
  reads.** `ObjectsCreate` takes `ObjectsCreateInput`; only it and the
  `ObjectsUpdateSpec*` mutators return a row. `EventsAdd` is a kept exception
  (see `docs/TODO.md`). → [ADR](docs/adr/2026-07-30-store-write-shapes.md)
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
  sweeper covers client-only kinds. Both are idempotent. A delete request and a
  cascade each enqueue at commit for a registered kind, so a cascade advances a
  level per commit; a client-only level still costs a sweep.
  → [ADR](docs/adr/2026-08-04-a-delete-request-pushes-its-own-collect.md)
- **The store is `auto_vacuum=INCREMENTAL`**, set on the DSN (SQLite ignores the
  pragma on a non-empty database and inside a transaction — which a migration
  is). The sweeper drains the freelist through `FreePagesReleaser`, gated on a
  floor and a fraction of the file. `PRAGMA incremental_vacuum` **must be
  `Exec`'d, never `Query`'d**. → [ADR](docs/adr/2026-07-29-auto-vacuum-incremental.md)
- **The schema is amended in place until the first release**: `sqlite/migrations/`
  holds exactly one file, and `TestTheSchemaIsOneMigration` is the tripwire.
  → [ADR](docs/adr/2026-07-31-amend-the-schema-in-place-until-release.md)
- **The generation handshake.** `Generation` increments on every real spec
  change; `ObservedGeneration` records what the controller last settled.
  Byte-identical writes are skipped (except that `UpdateStatus` still advances
  `observed_generation`), which is what stops a controller re-applying its own
  spec from waking itself forever.
  → [ADR](docs/adr/2026-07-27-generation-handshake-and-noop-writes.md)
- **Schema-version migration** (`Migrator`): per-kind, on read, at the decode
  boundary; spec and status version independently; an undecodable blob
  quarantines its row rather than killing the read.
  → [ADR](docs/adr/2026-07-27-schema-version-migration.md)
- **Every new `depends_on` edge stamps an owed reconcile** (`reconcile_owed`),
  atomically with the edge inside `EdgesAdd`, drained by the owed pass. The
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
  category), extended when `(type, reason)` matches. Reads live on `Client`;
  retention runs in the GC sweeper. "Event" means this log and nothing else.
  → [ADR](docs/adr/2026-07-27-events-api.md)
- **The schedule watch is an in-memory gauge and the one watch with no tick at
  all**: the `workQueue` publishes each move of its `gauge` to a `gobus/watch`
  hub. Sound only because the queue is unexported and process-local and the
  gauge reports every move from one type — give `workQueue` a second writer and
  the poll has to come back. (The object tail also has a wake, but it keeps a
  floor tick, so it is a driver rather than an exception.) Streams end when the
  beehive stops, after the final value.
  → [ADR](docs/adr/2026-07-27-schedule-watch.md)

## Conventions

- **Methods are `NounsVerbQualifier`, noun first and plural** (`ObjectsGet`,
  `EdgesListIncoming`): one prefix per family, cardinality in the verb
  (`Get`/`Watch` for one, `List`/`WatchList` for many). Drop the prefix when the
  family is the receiver itself — `Client`'s own CRUD stays bare; on
  `ControllerClient` a column on the object's row is bare (`UpdateStatus`), a
  table of its own is prefixed. List interface members alphabetically. `Err*`,
  `With*` and external-interface methods are exempt. A watch over a change
  stream returns `<-chan NounChange`; a watch over a gauge or a log streams the
  value itself. → [ADR](docs/adr/2026-07-27-noun-verb-naming.md)
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
- **Commits are terse conventional commits.** `type(scope): subject` —
  `feat`/`fix`/`perf`/`refactor`/`test`/`docs`/`chore`, scope only when it adds
  information (`sqlite`, `edges`, `watch`), `!` for a breaking change. Subject
  is imperative, lower-case, no period, ≤72 chars, and says what the change
  does, not how (`feat(edges): enqueue an edge's source when the declaration
  commits`). No body unless the why isn't obvious from the diff — then 1–3
  plain sentences, no bullet lists; rationale longer than that is an ADR the
  body links.
- **Stubs are explicit**: `panic("not implemented: <name>")`; stub options
  return `nil` and are marked `(stub: not yet wired up)`.
- **Design rationale goes in an ADR**, not here. See
  [docs/adr/README.md](docs/adr/README.md) for the format.
