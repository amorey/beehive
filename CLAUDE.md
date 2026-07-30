# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

`README.md` is the spec. Where the code and the README disagree about a signature,
the code wins. Design rationale lives in [docs/adr](docs/adr/README.md); keep this
file to a summary plus a link, and put anything longer in a new ADR.

## Status

The README spec is implemented end to end and the suite is green. One loose end:
the `fakeStore` double in `testutils_test.go` still `panic`s on many methods. They
get filled in as tests need them, so the real `sqlite` store backs most tests.

A write schedules nothing, so a demo that creates an object waits for the owed pass
tick. Every example under `examples/` runs on production defaults and calls
`Client.Requeue` after each create instead — the owed pass is no longer tunable from
outside the package, and `Requeue` is what the docs point callers at. `cascade` is
the exception: it sets `WithGCInterval`, since collection is what it demonstrates.
Leave the production defaults alone otherwise.

Latency here is a configured interval, not a push path. If a push path is ever
added it belongs *above* this core, because the scans are what guarantee
convergence. The [drivers ADR](docs/adr/2026-07-28-periodic-scan-drivers.md) lists
the constraints it would have to meet.

## Commands

```sh
go build ./...
go vet ./...
go run ./examples/greeting/main.go   # the end-to-end smoke target
go run ./examples/events/main.go     # Events API demo: a connection-health panel
go test ./...
go test -run TestName ./  # single test
```

## Architecture

Beehive is an embedded, Kubernetes-inspired control plane backed by a durable store.

- **Nothing is pushed. Every driver is a periodic scan.** A write leaves a durable
  trace, and a driver finds it by scanning the column that moved. `drivers.go` holds
  the two loop shapes: `runDriver` for one cadence, `tickerChan` for a select over
  several. Six drivers: the owed pass (unsettled specs plus `reconcile_owed`,
  per-kind, 30s), the full pass (`WithFullPassInterval`, every object, per-kind, off
  by default), the GC sweeper (`WithGCInterval`, deletion-pending, global, **cannot
  be disabled**), the dependency waker (the write log, global, 1s), the
  stale-dependents pass (dependency watermarks, global, 60s, **cannot be disabled**)
  and the watch poll (the client watches, global, 1s). Startup always runs the owed pass; the
  startup full pass is opt-in (`WithStartupFullPass`), like the periodic one and for
  the same reason — **no reconcile may depend on either full pass**, since both scale
  with the object count. **Only two of the six are publicly configurable** —
  `WithFullPassInterval` and `WithGCInterval`. The other four keep unexported
  options (`withOwedPassInterval`, `withDependencyWakeInterval`,
  `withStaleDependentsInterval`, `withWatchPollInterval`) that only tests reach; `Client.Requeue` is the public way
  to beat a cadence. Examples run on production defaults and nudge with `Requeue`
  rather than turning the drivers up.
  → [ADR](docs/adr/2026-07-28-periodic-scan-drivers.md). Every way an object comes to
  owe a pass, with its recording site, its driver, its restart answer and its tests,
  is mapped in [docs/reconcile-triggers.md](docs/reconcile-triggers.md) — update it
  when you add one.
- **Declarative and level-triggered.** Users write `Spec`; controllers reconcile
  toward it from *current* state, never from a sequence of changes. That is what
  makes scanning enough: a driver only has to notice that something moved, not what
  it was before.
- **Controllers coordinate through the store, never with each other.** A change
  reaches another controller by being scanned, not by being delivered.
- **Dependency staleness is re-derived, not recorded.** Each dependent's successful
  reconcile stamps `dependency_watermarks.reconciled_against` with the store-wide
  write cursor as of its *load* (an absent row means stale), and the stale-dependents
  pass enqueues every dependent a target has moved past. That is what makes a
  dependency wake a guarantee: a wake lost by any means — a crash, a startup seed
  race, a process with no waker, a bug — costs latency until this pass rather than
  permanent divergence, so the waker below is an optimisation over it. The write is
  gated in SQL on an outgoing `depends_on` edge (which is also its foreign-key guard
  against a mid-pass `gcCollect`) and suppressed entirely when the cursor has not
  advanced. `EdgesAdd` is the row's second writer, and only *clears* it: a new
  `depends_on` edge drops the watermark, because a cursor measured over a smaller
  dependency set cannot speak for a target just added. The clear keeps the derived
  state honest; what *guarantees* a fresh declare reaches its dependent is the
  `reconcile_owed` stamp on every new edge (below), which survives the interleaving
  the clear alone did not — a pass already in flight rewriting the cleared row.
  → [ADR](docs/adr/2026-07-29-dependency-watermarks.md)
- **The dependency waker scans from a `resource_version` watermark**
  (`ObjectWritesListSince`, paged). It is store-wide rather than per-kind, because a
  `depends_on` edge can point at a client-only kind that no per-kind query would
  name. Its cost is bounded by what changed rather than by what exists, which is why
  it runs far more often than the other drivers. A failed scan holds the cursor, so
  the next tick re-reads what is still owed. It is also the only driver that can
  find a *settled* dependent, which no owed-work listing sees. The watermark is
  persisted through the optional `DriverCursorer` capability (`driver_cursors`),
  paged and capped at `wakeScanPagesPerTick` per tick so a resumed cursor can't
  monopolise the connection, and abandoned for a fresh seed from
  `ObjectWritesMaxVersion` past `wakeSeedBacklogCap` — a store with no
  `DriverCursorer`, or a first start, seeds from that max exactly as before. This is
  still an optimisation, never a guarantee: the stale-dependents pass below is what
  makes a lost wake, persisted cursor or not, a latency cost rather than a strand.
  → [ADR](docs/adr/2026-07-30-durable-waker-cursor.md)
- **Client watches poll and diff** (`watchpoll.go`). Each stream remembers the
  `resource_version` it last reported and emits `Added`/`Modified`/`Deleted` from the
  comparison. A quiet tick costs one high-water-mark read plus one blob-free liveness
  read (the kind's ids for a list watch, one row for a single-object one); the listing
  that carries specs and statuses is paid only when the mark moved or something it
  tracks vanished. Deletes are found by absence, since a deleted row draws no version.
  `ObjectWritesMaxVersion` is the maximum over live `objects` rows rather than the
  `resource_version_seq` counter, because the event log draws from that counter too —
  a mark taken from it would move for writes no consumer of this pair can be shown.
- **`Spec`/`Status` separation is structural.** The user-facing `Client` has no
  status-write path. Only `Controller`/`ControllerClient` does.
- **Reconcile is not transactional.** Each `ControllerClient` write commits on its
  own. Mutators self-wrap in `Within` and scope id-keyed writes to the caller's
  `GroupKind`, returning `ErrWrongKind` otherwise. Use `ControllerClient.Within` when
  several writes must land together. The slug-keyed writes (`Create`, `GetOrCreate`,
  `DeleteBySlug`) differ only in what they do when the slug is taken, and **none of
  them writes to a row it found** — there is no slug-keyed upsert, so changing an
  existing object is always `Update`, keyed by id. **No write schedules anything**: a spec write bumps the
  generation that the owed pass lists, a delete sets the `deletion_requested_at` that the
  sweeper lists. `Store.AfterCommit` exists for one thing, the `WithOnCreate` hook.
  → [ADR](docs/adr/2026-07-27-slug-keyed-writes.md)
- **A store write takes only what it honours and returns only what a caller reads.**
  `ObjectsCreate` takes an `ObjectsCreateInput`, not the read-shaped `RawObject`
  whose `Status` field it used to drop in silence. And a mutator returns a row only
  where a public `Client` write hands that row to the user, which is `ObjectsCreate`
  and `ObjectsUpdateSpec` alone; the rest return `error`, plus a `bool` that someone
  actually reads. So no write pays a conditions query for a value nobody looks at,
  and the writes that report nothing answer from metadata alone. Tests read a
  post-write row back with `ObjectsGet`.
  → [ADR](docs/adr/2026-07-30-store-write-shapes.md)
- **A nested `Within` is a real rollback boundary.** It runs on a `SAVEPOINT`, so an
  error unwinds that frame's writes and its queued `AfterCommit` hooks even if the
  outer caller swallows it; only the outermost transaction commits. Depth rides on the
  ctx *beside* the transaction state, never under a key of its own — a stale depth
  would ride into a fresh transaction. A nested `Within` from a sibling goroutine is
  refused with `ErrConcurrentNestedTx` (ordinary deep nesting is not that case), and a
  failed unwind poisons the transaction rather than letting it commit. This is what
  retires the class of bug `EdgesAdd` had to solve by *ordering*.
  → [ADR](docs/adr/2026-07-29-nested-within-savepoints.md)
- **The generic boundary is `Register`.** `Register[Spec, Status]` wraps the user's
  typed `Controller` in a `typedController` adapter (`reconciler.go`) that satisfies
  the non-generic `controllerAdapter`. Everything below it — reconciler, work queue,
  store — deals in raw rows and has no type parameters. Keep new internal machinery
  non-generic.
- **Options dispatch on their target's type.** An `Option` type-switches on what it
  is applied to (`*Beehive`, `*reconciler`, …) and ignores targets it doesn't
  recognize, so the same option works at `New`, at `Register`, or per call.
- **GC has two backstops.** Each reconcile loop runs `gcCollect` for its own kind,
  which routes finalizer clearing through the controller. The global sweeper covers
  client-only kinds, whose stranded `owned_by` edges would otherwise RESTRICT-block
  their owner's delete forever. `gcCollect` does nothing while finalizers or
  referrers remain and is safe to repeat, so the overlap is harmless. A cascade
  advances one step per sweep: `gcCollect` marks children and returns, and that mark
  is what puts them in the next sweep's listing.
- **The store is `auto_vacuum=INCREMENTAL`, and the sweeper drains the freelist.**
  The mode is set on `OpenPool`/`OpenMemory`'s DSN, never in a migration: SQLite
  writes it into the file header at the first `CREATE TABLE` and ignores the pragma
  on a non-empty database and inside a transaction, both of which a migration is.
  The GC sweeper's third step releases up to `freePagesPerSweep` pages through the
  optional `FreePagesReleaser`, but only once the freelist passes both an absolute
  floor and a fraction of the file — free pages are what the next inserts would have
  reused, so draining a small one trades work for nothing. `PRAGMA
  incremental_vacuum` frees one page per step and so **must be `Exec`'d, never
  `Query`'d**; the `Query` path releases exactly one page and reports no error.
  → [ADR](docs/adr/2026-07-29-auto-vacuum-incremental.md)
- **The generation handshake.** `Generation` increments on every spec change;
  `ObservedGeneration` records what the controller last settled, and is `nil` until
  the first `UpdateStatus` (which takes the generation explicitly). Mutators skip a
  write whose bytes already match, except that `UpdateStatus` still advances
  `observed_generation`/`observed_at` — and so `resource_version` — when only those
  moved. The byte check is gated on the schema version too. A skipped write leaves
  nothing for a scan to find, which is what stops a controller that re-applies its
  own spec from waking itself forever.
  → [ADR](docs/adr/2026-07-27-generation-handshake-and-noop-writes.md)
- **Schema-version migration** (`Migrator`, `migrator.go`). A per-kind migrator
  converts spec and status blobs up *on read*, at the decode boundary. Writes stamp
  lazily and never downward, and the two columns
  (`schema_version_spec`/`_status`) version independently. A blob that fails to
  decode quarantines its row instead of killing the whole list or stream.
  → [ADR](docs/adr/2026-07-27-schema-version-migration.md)
- **Every new `depends_on` edge stamps an owed reconcile.** `EdgesAdd` increments
  `objects.reconcile_owed` for every `depends_on` edge it *creates* (self-edges
  excluded), in the same statement sequence as the edge, drained by the owed pass.
  The stamp is unconditional on the caller's claim because only recorded owed work
  survives every interleaving — an increment landing mid-pass sits above the count
  that pass observed at load, so the decrement cannot consume it. The edge-new gate
  is what bounds it: re-asserting a dependency set every pass stamps nothing after
  the first. There is no version claim — the parameter the old conditional stamp
  read was removed once nothing conditioned on it.
  → [ADR](docs/adr/2026-07-29-stamp-every-new-dependency-edge.md)
- **Secondary lookups (owner, dependencies, dependents, owned) are read on request**,
  never folded into the `SELECT` that carries the blobs. Eager `LoadOption`s and lazy
  `Client`/`ControllerClient` getters share the same loaders, and accessors return
  `ErrNotLoaded` for a relation nobody asked for. `OwnedObjectsList` is the typed form
  of `OwnedList`. → [ADR](docs/adr/2026-07-27-secondary-lookups.md)
- **Events are an append-only log, aggregated into runs.** One log per object,
  partitioned by `category`. `ControllerClient.EventsAdd` extends the latest run
  when `(type, reason)` matches and appends otherwise. `Detail` goes in typed and
  comes out opaque, and is not versioned. Reads live on `Client`; retention runs in
  the GC sweeper. "Event" means this log and nothing else — the object-change streams
  are named apart. → [ADR](docs/adr/2026-07-27-events-api.md)
- **The schedule watch is an in-memory gauge, not store state.**
  `Client.SchedulesWatch`/`SchedulesGet` report the `workQueue`'s next-requeue time,
  which bumps no generation or `resource_version` and so fires no object watch. It
  lives in the beehive layer; the watch polls `nextRequeueAt` and emits on change.
  → [ADR](docs/adr/2026-07-27-schedule-watch.md)

## Conventions

- **Methods are `NounsVerbQualifier`, noun first and plural** (`ObjectsGet`,
  `EdgesListIncoming`, `DeletionRequestsCreate`): one prefix per family, cardinality
  in the verb (`Get`/`Watch` for one, `List`/`WatchList` for many). Drop the prefix
  when the family is the receiver itself — `Client`'s own CRUD stays bare, and on
  `ControllerClient` the line falls between a column on the object's row
  (`UpdateStatus`, bare) and a table of its own (conditions, events, edges, all
  prefixed). List interface members alphabetically, as godoc renders them. `Err*`,
  `With*` options and external-interface methods are exempt. **A watch over a change
  stream returns `<-chan NounChange`**, never a `…Watcher` interface. A watch over a
  gauge or a log (`SchedulesWatch`, `EventsWatch`) streams the value itself; use a
  `NounChange` only when the consumer has to tell *what happened* from *what it now
  is*. → [ADR](docs/adr/2026-07-27-noun-verb-naming.md)
- **Whitebox tests.** Tests go in `package beehive`, not `beehive_test`, so they can
  reach unexported machinery — the reconcile loop, the adapter and options dispatch
  are the interesting parts, and none of them are exported.
- **Test files mirror source files, not features.** A function in `foo.go` is tested
  in `foo_test.go`, whatever feature it belongs to (edges and conditions live in
  `sqlite/store.go`, so they are tested in `sqlite/store_test.go`). Shared helpers and
  fakes go in `testutils_test.go`. Not every source file needs a test file.
- **Assertions use `stretchr/testify`**: `require` for preconditions that must hold,
  `assert` for independent checks.
- **Synchronize on signals, never on sleeps.** Wait on channels the code or a fake
  closes, or on `ctx.Done()`. The only use of `time` is a generous failsafe timeout in
  a `select`, which turns a hang into a failure. No `time.Sleep` to wait for a
  goroutine, and no polling loops.
- **Comments are short and explain why.** Call out invariants that aren't obvious;
  don't restate what the code says. Match the density in `beehive.go` and
  `reconciler.go`.
- **Stubs are explicit.** Unimplemented methods `panic("not implemented: <name>")`.
  Unimplemented options return `nil` and are marked `(stub: not yet wired up)`.
- **Design rationale goes in an ADR**, not here. See
  [docs/adr/README.md](docs/adr/README.md) for the format.
