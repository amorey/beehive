# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

The README spec is implemented end-to-end and the suite is green: `example/greeting/main.go` runs to convergence, and the full Client / ControllerClient / Options surfaces, the reconcile loop (concurrency, backoff, resync), conditions, refs, finalizers, the dependency waker, and GC (cascade + finalizer gating + dangling-delete resume) are all wired and tested. `README.md` remains the authoritative spec; when code and README disagree on a signature, the code is the current truth.

One loose end: the `fakeStore` test double in `testutils_test.go` still `panic`s on many methods — they're filled in only as a test needs them, so the real `sqlite` store backs most tests.

## Commands

```sh
go build ./...
go vet ./...
go run ./example/greeting/main.go   # the end-to-end smoke target
go test ./...
go test -run TestName ./  # single test
```

## Architecture

Beehive is an embedded, Kubernetes-inspired control plane backed by a durable store.

- **Declarative + level-triggered.** Users write `Spec` (desired state); controllers reconcile actual state toward it based on *current* state, not event sequences. Events are a latency optimization; periodic resync is the correctness backstop.
- **Coordination through the store, never controller-to-controller.** Controllers read/write the shared store and wake on changes.
- **`Spec`/`Status` separation is structural.** The user-facing `Client` has no status-write path; only the `Controller`/`ControllerClient` surface does.
- **Reconcile is not transactional.** `typedController.reconcile` loads the object and calls `Reconcile` with no enclosing transaction; each `ControllerClient` write commits on its own (a write before a returned error stays committed, and the level loop re-derives from it). Each write is still internally atomic — the store mutators self-wrap in `Within`, and `withinKind` wraps the kind-check + write in one short transaction. A controller that needs several writes atomic wraps them in `ControllerClient.Within` (which is `store.Within`; nested CC writes join via the ctx's `txKey`). The store runs on a single connection (`SetMaxOpenConns(1)`, `_txlock=immediate`), so an open transaction serializes all other writers for its duration — which is why holding one across a whole reconcile was removed in favor of this opt-in.
- **Generic-to-non-generic boundary.** `Register[Spec, Status]` wraps the user's typed `Controller` in a `typedController` adapter (`reconciler.go`) that satisfies the non-generic `controllerAdapter`. Everything below that line — reconciler, work queue, store — stays free of type parameters and deals in raw rows. Keep new internal machinery non-generic; confine generics to the public API and the adapter.
- **Options dispatch by target type.** An `Option` type-switches on what it's applied to (`*Beehive`, `*reconciler`, …) and ignores targets it doesn't recognize — so the same option works at `New`, `Register`, or per-object call sites.
- **GC has two backstops.** Each controller's reconcile loop runs `collect` for its own kind (routing finalizer clearing through the controller). A single global GC sweeper (`runGCSweeper`, started by `Start`) sweeps *every* kind on startup + the resync cadence, so deletion-pending objects of **client-only kinds** (no registered controller) are still collected — otherwise a cascade would strand them and their `owned_by` edge would RESTRICT-block the owner's delete forever. `collect` is a no-op while finalizers/referrers remain and idempotent across paths, so the overlap on registered kinds is harmless.
- **Generation/convergence handshake.** `Object.Generation` increments on every spec change. `Object.ObservedGeneration` records the generation the controller last settled; `nil` until the first `UpdateStatus` call. The reconciler and resync skip objects where `ObservedGeneration == Generation` (already settled). Controllers report which generation they reconciled by passing `obj.Generation` explicitly to `UpdateStatus(ctx, id, observedGeneration, status)` — the store never derives this internally, so callers must always pass the generation of the object they received in `Reconcile`. The store guards the handshake: `UpdateStatus` rejects an `observedGeneration` greater than the row's current generation with `ErrObservedGenerationFuture` (a controller can only have observed a generation that exists). An older value is accepted — that's the normal case where the spec changed mid-reconcile, leaving the object unsettled so it reconciles again.

## Conventions

- **Whitebox tests.** Put tests in `package beehive` (not `beehive_test`) so they can exercise unexported machinery — the reconcile loop, adapter, and options dispatch are the interesting parts and they're unexported.
- **Tests are organized by origin file, not by topic.** A function defined in `foo.go` is tested in `foo_test.go` — mirror the source filename, regardless of feature. For example, refs and conditions live in `sqlite/store.go`, so their tests belong in `sqlite/store_test.go` (not a `refs_test.go`/`conditions_test.go`); `open`/`Open` live in `sqlite/sqlite.go`, so they're tested in `sqlite/sqlite_test.go`. Shared test helpers and fakes that aren't tied to one source file go in `testutils_test.go`. Not every source file needs a test file (e.g. pure type-alias files).
- **Assertions: `stretchr/testify`** (`require` for fatal preconditions, `assert` for independent checks) — already the style in `sqlitemigrate/sqlitemigrate_test.go`.
- **Event-driven, never sleep-paced.** Synchronize on channels (or `ctx.Done()`) that the code/fakes signal; the only use of `time` is a generous failsafe timeout in a `select` that turns a hang into a failure. No `time.Sleep` to "wait for" a goroutine and no polling loops.
- **Comments are short, idiomatic, and human-centered.** Explain *why* and call out non-obvious invariants (e.g. why `Start` takes no context, why a guard exists); don't restate what the code plainly says. Match the density already in `beehive.go`/`reconciler.go`.
- **Stubs are explicit.** Unimplemented methods `panic("not implemented: <name>")`; unimplemented options return `nil` and are marked `(stub: not yet wired up)`.

## Known improvements

- **Filter options for `ListObjects()`.** Several call sites list a whole kind and then filter in Go — e.g. the lag-recovery `relist` in `sqlite/watch.go` (single-object watches already special-case this with a `GetObject`, but other paths don't). Adding filter options to `ListObjects()` (by id set, by deletion-pending, by unsettled, etc.) would push the predicate into SQL: faster (less scanned/marshaled) and more readable (no post-list filtering loops). It would also let related queries like `ListUnsettledIDs`/`ListDeletionPendingIDs` collapse into one filtered list path.

- **Revisit watch behavior on `ErrLagged`.** When a watcher falls behind the ring (`sqlite/watch.go`), it currently self-heals in place: relist current state as `Modified` and synthesize `Deleted` tombstones for ids that vanished during the gap. Worth reconsidering the whole contract — e.g. terminating the watch on lag and forcing the caller to re-list (the k8s `410 Gone` model), which would need a re-subscribe loop around the dependency waker and a public-contract change. Also reconsider the tombstone trade-off (lag-induced `Deleted` carries an empty spec, unlike a normal delete).

- **Optimize the kind-scoping checks.** Both write surfaces guard id-keyed operations against cross-kind access: the user-facing client's `scopedGet` (`client.go`) and the controller client's `checkKind` (`controller.go`) each do a full `GetObject` — loading the whole row plus its assembled conditions — purely to compare `Group`/`Kind`, often immediately before another store call (`UpdateSpec`, `UpdateStatus`, `RequestDeletion`, …) that re-reads the same row. Push the predicate into the write instead: a kind-only lookup (select just `group`/`kind`), or better, fold the kind into the mutating statement's `WHERE` so a foreign id simply matches no rows and the store reports the mismatch — one round trip, no redundant row+conditions marshaling. This would also retire `withinKind`'s short per-call transaction (`controller.go`): with the kind folded into the single mutating statement there's no separate check to keep atomic with the write, so each controller-client write collapses to one autocommit statement. Overlaps with the `ListObjects()` filter-options note: both are about expressing the predicate in SQL rather than re-reading and checking in Go.

- **Revisit the per-watcher id maps.** Each `watch()` goroutine keeps two maps keyed by object id: `live` (the believed-current id set, used to detect gap-deletions) and `snapshotRV` (high-water resource version per id, used to dedup stale ring events). `snapshotRV` is deliberately never pruned (it must retain versions for deleted ids), so it grows with every distinct id the watcher ever sees. Revisit whether this growth is acceptable for long-lived watches over high-churn kinds, and whether the two maps can be reconciled or bounded (e.g. TTL/version-window pruning) without resurrecting deleted objects from stale buffered events.
