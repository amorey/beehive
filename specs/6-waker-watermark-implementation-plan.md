# Implementation plan: watermark recovery for missed dependency wakes

Red/green TDD cycles for
[6-waker-escalation-without-tick.md](6-waker-escalation-without-tick.md). Each cycle is
one commit, small enough to review on its own and to revert without unpicking the next.

**Order rationale.** The three store changes come first (1–3) because everything above
them is unwritable without them, and each is independently testable through the `sqlite`
suite. Then a pure refactor (4) creates somewhere for the state to live. Then the waker
gains behavior one rule at a time (5–8). The escalation is deleted last (9), once nothing
depends on it — deleting it earlier would leave the tree red for several cycles.

**Two cycles are honestly not red/green**, and are labeled so rather than dressed up:
cycle 4 is a pure refactor (tests must stay green, unchanged), and the rv-monotonicity
test in cycle 3 is a guard on an existing invariant, so it passes the moment it is
written. Everything else has a genuine failing test first.

---

## Cycle 1 — `ObjectWrite` carries its `resource_version`

**RED** — `sqlite/watch_test.go`: subscribe to object writes, commit one object, assert
the delivered `ObjectWrite.ResourceVersion` equals the object's `resource_version`. Fails
to compile: no such field.

**GREEN** — add `ResourceVersion int64` to `storeapi.ObjectWrite`; pass `wev.Value.rv`
through at the two construction sites (`sqlite/watch.go:398`, `:409`). `writeSignal`
already carries `rv` and `writeSignalMerge` already merges on `max`, so there is nothing
to compute.

Also assert the merge case: two writes to one object coalesce to the **higher** rv.

> `feat(store): carry resource_version on ObjectWrite`

## Cycle 2 — `ObjectWritesSubscribe` returns the starting cursor

**RED** — `sqlite/watch_test.go`: commit a write, subscribe, assert the returned cursor is
≥ that write's rv; then commit another and assert its delivered rv is > the cursor. Fails
to compile: single return value.

**GREEN** — signature becomes
`ObjectWritesSubscribe(ctx) (*ObjectWritesSubscription, int64, error)`. Register the hub
receiver **before** reading `currentResourceVersion`, so a write landing between the two
is either already in the receiver or above the cursor. Update the one non-test caller
(`beehive.go:324`) and `fakeStore`.

No waker behavior yet — the cursor is read and dropped. That keeps the signature change
reviewable on its own.

> `feat(store)!: return the starting cursor from ObjectWritesSubscribe`

## Cycle 3 — `ObjectIDsListSince` for the replay query

**RED** — `sqlite/store_test.go`: write objects across several kinds, assert the method
returns ids with rv > `afterRV` in rv order, honors `limit`, and is kind-agnostic.

**GREEN** — implement it blob-free over `idx_objects_rv`; add the `fakeStore` entry.

**Plus one guard test (not red)** — `resource_version` is monotonic in commit order. This
passes immediately; it exists so that raising `SetMaxOpenConns` fails *here* rather than
silently skipping changes in production. Label it as pinning the single-connection
assumption.

> `feat(store): add ObjectIDsListSince for cursor-ordered replay`

## Cycle 4 — extract a `waker` struct (pure refactor, no behavior change)

**No new test.** The existing waker tests must pass **unchanged** — that is the check.

Move `dependencyWakerStart` / `dependencyWakerRun` / `dependentsWake` onto a `waker` type
holding the store, logger, requeue path, and (from the next cycle) the watermark. No logic
edits; anything more is a later cycle.

This exists so cycles 5–8 have somewhere to put single-goroutine-owned state, instead of
adding fields to `Beehive` beside the config knobs.

> `refactor(waker): extract the dependency waker into its own type`

## Cycle 5 — track the watermark on the live path

**RED** — the waker consumes two batches; assert its watermark equals the highest rv
consumed. Then feed a **delete-only** batch (which `dependentsWake` early-returns on) and
assert the watermark still advanced. Fails: no watermark exists.

**GREEN** — a plain `int64` field on `waker`, initialized from cycle 2's cursor, advanced
from `max(rv)` of every batch **consumed** — no-ops included.

Nothing reads it yet. This cycle is only about advancing it correctly.

> `feat(waker): track a resource_version watermark on consumed batches`

## Cycle 6 — hold the watermark when a lookup fails

**RED** — the interleaving test: batch A (`X@100`) fails its dependents lookup, batch B
(`Z@150`) would succeed. Assert the watermark does **not** advance past A. Fails today:
the watermark advances on receipt.

**GREEN** — on a failed lookup, stop consuming and hold. The stall is what makes the
cursor a true low-water mark instead of `max(processed)`; the hub's per-object conflation
is what makes stalling safe.

Still no replay — a held watermark with nothing to spend it on. Next cycle spends it.

> `fix(waker): hold the watermark when a dependents lookup fails`

## Cycle 7 — replay from the watermark, paged

**RED** — with N changes missed and M objects in the store, assert replay reads N rows,
not M, and issues more than one page when N exceeds the page size. Also: a target deleted
during the outage does not error the replay.

**GREEN** — replay via `ObjectIDsListSince`, rv-ordered, `LIMIT` per page, advancing the
watermark **per page** (safe here precisely because pages are rv-ordered). Feed each page
through the existing `dependentsWake` path.

Wire it to the failed-lookup retry only. The subscribe/stream-closed paths have no driver
until cycle 8.

> `feat(waker): replay missed changes from the watermark in pages`

## Cycle 8 — the resubscribe loop, with an injectable backoff seam

**RED** — the headline test: `WithCatchupInterval(0)`, `WithResyncInterval(0)`,
`WithStartupResync(false)`, `withoutGCSweeper()`. Fail the subscription, change a target,
assert its **settled** dependent still reconciles. This is the whole gap, and it fails
today with nothing but a log line. Plus: the backoff interval stops growing at the
ceiling, and the loop is **still retrying** after it.

**GREEN** — on subscribe failure or stream close: back off, resubscribe, replay from the
watermark, resume. The ceiling caps the *interval*, never the attempts — an attempt cap
would resurrect the dead waker by a slower route.

The backoff seam is **required, not optional**: `CLAUDE.md` forbids sleep-paced tests, and
this test drives a failure through the loop. Follow `beforeLiveSend` / `afterStream`
(`sqlite/watch.go:388,397`).

> `feat(waker): resubscribe and replay instead of relying on a periodic tick`

## Cycle 9 — delete the escalation machinery

**RED** — assert the `beehive.go:334-335` operator-guidance message no longer appears on
a subscribe failure. Fails while the message exists.

**GREEN** — delete `resyncKindsNextTick` / `resyncKindsEveryTick`, both `hasPeriodicPass`,
`resyncOnce` / `resyncAlways`, `resyncNextTick` / `resyncEveryTick`, `tickResyncs`, and the
self-re-arm at `reconciler.go:568`. Retire the escalation tests
(`TestDroppedWakeEscalatesEveryKind`, `TestFailedEscalatedPassRearmsOneShot`,
`TestSubscribeFailureReportsWholeProcess`, `TestDeadWakerReportsWholeProcess`) or rewrite
them against the replay path.

The catchup and resync ticks themselves **stay** — they are independent features. Only the
escalation layered on top goes.

> `refactor(waker)!: delete the tick escalation now that replay covers its loss points`

## Cycle 10 — docs

Successor ADR marking
`docs/adr/2026-07-27-dependency-wake-escalation.md` superseded; update the `CLAUDE.md`
bullet; close out `TODO.md:95-118`; note on `WithStartupResync(false)` that it opts out of
crash recovery for settled dependents (the residual this design scopes out).

> `docs(adr): record watermark replay superseding the tick escalation`

---

## Checks between cycles

`go build ./... && go vet ./... && go test ./...` green at every commit, plus
`gofmt`. Cycle 4 additionally requires that no existing test file changed.
