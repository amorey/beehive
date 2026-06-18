# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

Work-in-progress. Lower layers are stubbed (methods `panic("not implemented")`); the package is being built top-down, driven by `example/main.go`, which won't fully run until those layers land. `README.md` describes the target API and is ahead of the code — treat it as the spec, the code as the current slice. When code and README disagree on a signature, the code is the current truth.

## Commands

```sh
go build ./...
go vet ./...
go run ./example          # the end-to-end smoke target
go test ./...
go test -run TestName ./  # single test
```

## Architecture

Beehive is an embedded, Kubernetes-inspired control plane backed by a durable store.

- **Declarative + level-triggered.** Users write `Spec` (desired state); controllers reconcile actual state toward it based on *current* state, not event sequences. Events are a latency optimization; periodic resync is the correctness backstop.
- **Coordination through the store, never controller-to-controller.** Controllers read/write the shared store and wake on changes.
- **`Spec`/`Status` separation is structural.** The user-facing `Client` has no status-write path; only the `Controller`/`ControllerClient` surface does.
- **Generic-to-non-generic boundary.** `Register[Spec, Status]` wraps the user's typed `Controller` in a `typedController` adapter (`reconciler.go`) that satisfies the non-generic `controllerAdapter`. Everything below that line — reconciler, work queue, store — stays free of type parameters and deals in raw rows. Keep new internal machinery non-generic; confine generics to the public API and the adapter.
- **Options dispatch by target type.** An `Option` type-switches on what it's applied to (`*Beehive`, `*reconciler`, …) and ignores targets it doesn't recognize — so the same option works at `New`, `Register`, or per-object call sites.

## Conventions

- **Whitebox tests.** Put tests in `package beehive` (not `beehive_test`) so they can exercise unexported machinery — the reconcile loop, adapter, and options dispatch are the interesting parts and they're unexported.
- **Assertions: `stretchr/testify`** (`require` for fatal preconditions, `assert` for independent checks) — already the style in `sqlitemigrate/sqlitemigrate_test.go`.
- **Event-driven, never sleep-paced.** Synchronize on channels (or `ctx.Done()`) that the code/fakes signal; the only use of `time` is a generous failsafe timeout in a `select` that turns a hang into a failure. No `time.Sleep` to "wait for" a goroutine and no polling loops.
- **Comments are short, idiomatic, and human-centered.** Explain *why* and call out non-obvious invariants (e.g. why `Start` takes no context, why a guard exists); don't restate what the code plainly says. Match the density already in `beehive.go`/`reconciler.go`.
- **Stubs are explicit.** Unimplemented methods `panic("not implemented: <name>")`; unimplemented options return `nil` and are marked `(stub: not yet wired up)`.
