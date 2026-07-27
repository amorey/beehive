# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` is the authoritative spec; when code and README disagree on a signature, the code is the current truth. Design rationale lives in [docs/adr](docs/adr/README.md) — keep this file to a summary plus a link, and move anything longer into a new ADR.

## Status

The README spec is implemented end-to-end and the suite is green. One loose end: the `fakeStore` test double in `testutils_test.go` still `panic`s on many methods — they're filled in only as a test needs them, so the real `sqlite` store backs most tests.

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

- **Declarative + level-triggered.** Users write `Spec` (desired state); controllers reconcile toward it from *current* state, not event sequences. Events are a latency optimization; three independent periodic drivers backstop them — `WithCatchupInterval` (owed work, per-kind), `WithResyncInterval` (full pass, per-kind, off by default), `WithGCInterval` (global, **cannot be disabled**). Startup runs catchup unconditionally plus, by default, one full pass. → [ADR](docs/adr/2026-07-27-periodic-reconcile-drivers.md)
- **Coordination through the store, never controller-to-controller.** Controllers read/write the shared store and wake on changes.
- **The dependency waker rides one store-wide change stream**, not one per registered kind — a `depends_on` edge may point at a client-only kind, which no per-kind subscription can name. The stream is blob-free and batch-drained. → [ADR](docs/adr/2026-07-27-store-wide-dependency-change-stream.md)
- **Dependency-wake failures escalate the catchup tick** (not resync, which is off by default), fanning out to every registered kind since edges are cross-kind. → [ADR](docs/adr/2026-07-27-dependency-wake-escalation.md)
- **Watch fan-out conflates per object, so it never lags-as-loss.** A per-kind conflating hub (`conflate` from `github.com/amorey/gobus`) keeps the latest event per object id; no ring, no `ErrLagged`, no relist. A store-wide `changeHub` sits beside the per-kind ones. → [ADR](docs/adr/2026-07-27-conflating-watch-fanout.md)
- **`Spec`/`Status` separation is structural.** The user-facing `Client` has no status-write path; only the `Controller`/`ControllerClient` surface does.
- **Reconcile is not transactional**; each `ControllerClient` write commits on its own (mutators self-wrap in `Within`, and scope id-keyed writes to the caller's `GroupKind` → `ErrWrongKind`). Use `ControllerClient.Within` when several writes must be atomic. The slug-keyed writes (`Create`/`CreateOrUpdate`/`GetOrCreate`/`DeleteBySlug`) differ only in conflict policy, and every client write registers its wake through `Store.AfterCommit`, run after the *outermost* commit. → [ADR](docs/adr/2026-07-27-writes-and-post-commit-wakes.md)
- **Generic-to-non-generic boundary.** `Register[Spec, Status]` wraps the user's typed `Controller` in a `typedController` adapter (`reconciler.go`) satisfying the non-generic `controllerAdapter`. Everything below that line — reconciler, work queue, store — stays free of type parameters and deals in raw rows. Keep new internal machinery non-generic.
- **Options dispatch by target type.** An `Option` type-switches on what it's applied to (`*Beehive`, `*reconciler`, …) and ignores targets it doesn't recognize — so the same option works at `New`, `Register`, or per-object call sites.
- **GC has two backstops.** Each controller's reconcile loop runs `collect` for its own kind (routing finalizer clearing through the controller); the global GC sweeper covers **client-only kinds**, whose stranded `owned_by` edges would otherwise RESTRICT-block their owner's delete forever. `collect` is a no-op while finalizers/referrers remain and idempotent across paths, so the overlap is harmless.
- **Generation/convergence handshake.** `Generation` increments on every spec change; `ObservedGeneration` records what the controller last settled (`nil` until the first `UpdateStatus`, which takes the generation explicitly). Mutators skip a write whose bytes match — except that `UpdateStatus` still advances `observed_generation`/`observed_at` (and does emit) when only those moved, and the no-op is gated on the schema version too. → [ADR](docs/adr/2026-07-27-generation-handshake-and-noop-writes.md)
- **Schema-version migration (`Migrator`, `migrator.go`).** A per-kind migrator converts spec/status blobs up *on read* at the decode boundary; writes stamp lazily and never downward. Per-column versions (`schema_version_spec`/`_status`) make that sound. Decode failures quarantine the row rather than killing a list or stream. → [ADR](docs/adr/2026-07-27-schema-version-migration.md)
- **Declaring a dependency is caller-versioned** (`DependenciesAdd`'s `targetResourceVersion`), closing the read-then-declare window. The requeue fires only when the edge is new *and* the target has moved past the version the decision used; it has a durable twin in `objects.pending_wake`, stamped inside `RefsAdd` before the insert and drained by the reconcile. → [ADR](docs/adr/2026-07-27-caller-versioned-dependencies.md)
- **Secondary lookups (owner / dependencies / dependents / owned)** are read on request, never folded into the blob-bearing `SELECT`. Eager per-call `LoadOption`s and lazy `Client`/`ControllerClient` getters share the loaders; accessors return `ErrNotLoaded` when a relation wasn't requested. `OwnedObjectsList` is the typed counterpart of `OwnedList`. → [ADR](docs/adr/2026-07-27-secondary-lookups.md)
- **Events API (append-only, contiguous-run aggregated).** A per-object log partitioned by `category`; `ControllerClient.EventsRecord` extends the latest run when `(type, reason)` matches and appends otherwise. `Detail` is typed-in/opaque-out and unversioned. Reads live on `Client`; retention runs in the GC sweeper. "Event(s)" is reserved for this log — the object-change streams are named apart. → [ADR](docs/adr/2026-07-27-events-api.md)
- **Schedule watch (in-memory gauge, not the store).** `Client.SchedulesWatch`/`SchedulesGet` expose the `workQueue`'s next-requeue time, which bumps no generation or `resource_version` and so fires no object watch. It lives entirely in the beehive/reconciler layer. → [ADR](docs/adr/2026-07-27-schedule-watch.md)

## Conventions

- **Methods are `NounsVerbQualifier`, with the noun plural** (`ObjectsGet`, `RefsListIncoming`, `DeletionRequestsCreate`) — one prefix per family, cardinality in the verb (`Get`/`Watch` vs `List`/`WatchList`) — **except where the family is already the receiver's own**: `Client`'s own CRUD stays bare, and on `ControllerClient` the line falls between a column on the object's row (`UpdateStatus`, bare) and a table of its own (conditions, events, refs — prefixed). Interface members are listed alphabetically, matching godoc. `Err*`, `With*` options, and external-interface methods are exempt. **A watch over a *change* stream returns `<-chan NounChange` (by value) or a `*NounsSubscription`** — never a bare `…Watcher` interface. A watch that is a **gauge or a log** (`SchedulesWatch`, `EventsWatch`) streams the value itself; it takes a `NounChange` only when the consumer must tell *what happened* from *what it now is*. → [ADR](docs/adr/2026-07-27-noun-verb-naming.md)
- **Whitebox tests.** Put tests in `package beehive` (not `beehive_test`) so they can exercise unexported machinery — the reconcile loop, adapter, and options dispatch are the interesting parts and they're unexported.
- **Tests are organized by origin file, not by topic.** A function defined in `foo.go` is tested in `foo_test.go` — mirror the source filename, regardless of feature (refs and conditions live in `sqlite/store.go`, so they're tested in `sqlite/store_test.go`). Shared helpers and fakes go in `testutils_test.go`. Not every source file needs a test file.
- **Assertions: `stretchr/testify`** (`require` for fatal preconditions, `assert` for independent checks) — already the style in `sqlitemigrate/sqlitemigrate_test.go`.
- **Event-driven, never sleep-paced.** Synchronize on channels (or `ctx.Done()`) that the code/fakes signal; the only use of `time` is a generous failsafe timeout in a `select` that turns a hang into a failure. No `time.Sleep` to "wait for" a goroutine and no polling loops.
- **Comments are short, idiomatic, and human-centered.** Explain *why* and call out non-obvious invariants; don't restate what the code plainly says. Match the density already in `beehive.go`/`reconciler.go`.
- **Stubs are explicit.** Unimplemented methods `panic("not implemented: <name>")`; unimplemented options return `nil` and are marked `(stub: not yet wired up)`.
- **Design rationale goes in an ADR**, not here — see [docs/adr/README.md](docs/adr/README.md) for the format.
