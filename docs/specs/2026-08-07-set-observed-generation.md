# A controller settles its generation without writing status

- **Status:** Proposed. Not implemented.
- **Date:** 2026-08-07
- **Issue:** [#98](https://github.com/amorey/beehive/issues/98)

## Problem

An object is unsettled when `observed_generation < generation`. `generation`
moves on a spec write; `observed_generation` moves only in
`Objects().UpdateStatus`. So `UpdateStatus` is the only way a controller can say
"I reconciled this generation".

A controller whose response to a spec change is *only* a condition write can
therefore never settle. `Conditions().Set` bumps `resource_version` — watches and
the dependency waker see it — but deliberately leaves `generation` and
`observed_generation` alone. Nothing closes the handshake. The object sits in
`ListUnsettledIDs` forever and the owed pass re-enqueues it every interval.

It is not a hot loop: the condition write no-ops on the second pass, so each wake
costs a read. But the object is permanently unconverged and permanently in the
owed listing while its controller is doing exactly the right thing, and the
better the controller is at not writing redundant status, the more likely it hits
this.

The concrete case: a child's spec is `{Enabled bool}`; the parent flips it to
`false` to pause the child; the child's controller stops its worker and reports
`Synced=False/Paused` as a condition; its status holds only a liveness timestamp,
which a pause does not change, so there is no `UpdateStatus` call.

There *is* a way to settle such an object today — re-pass the status you were
handed — and it is unsound on a schema-version bump. See the third rejected
option below; it is the strongest argument for this verb, not an afterthought.

## Decision

Add one verb that records the handshake and nothing else, at both layers:

- `Objects().SetObservedGeneration(ctx, gk, id, observedGeneration) error`
- `ControllerClient.SetObservedGeneration(ctx, id, observedGeneration) error`

It is orthogonal to what a controller reports. A pass that reports conditions
composes the two inside `Within`; a pass that legitimately reports nothing at all
calls it alone.

### Why not fold it into the condition setter

Issue #98's option (1) — `SetConditions` takes an `observedGeneration`, applying
`UpdateStatus`'s clamp — is rejected:

- It is a breaking signature change to two verbs that stabilized in #97, against
  one added method.
- `SetConditions` no-ops when every condition already matches, so it would
  inherit `UpdateStatus`'s two-axis rule (content unchanged but the handshake
  advances, at most once per generation, a call identical in both writes
  nothing). That is the subtlest paragraph in the store API and it would exist in
  two places.
- Not every condition write settles. `Progressing=True` mid-pass then
  `Ready=True` at the end is one pass and two condition writes, only the second
  of which settles. A mandatory argument forces the first to say something it
  does not mean, so it would need a "don't touch" sentinel — the tell that the
  concern does not belong on the verb.

### Why not have the reconciler stamp it

Issue #98's option (3) — the reconciler stamps after a successful `Reconcile`,
beside the `ReconcileOwed` decrement and the dependency watermark — is rejected:

- It collides with documented behavior. `UpdateStatus` writes a stale
  `observedGeneration` verbatim on the content-changed path, "rolling the object
  back to unsettled so a later pass re-derives it"
  (`internal/storeapi/storeapi.go:765-767`). A reconciler that stamps after every
  nil return silently overrides a controller that used that deliberately.
- It flips the default for kinds that today opt out of settling by never writing
  status.
- It weakens the claim from "the controller reported on generation N" to "a
  reconcile of generation N returned nil".

### Why not re-pass the status

A condition-only controller *can* settle today: call
`UpdateStatus(ctx, id, gen, obj.Status)` with the status it was handed. Identical
bytes hit the bookkeeping-only branch this spec extracts, so it writes the
handshake and leaves `updated_at` alone. This is the first thing a reviewer will
propose, and it is the reason the verb has to exist rather than merely being
nicer:

**The no-op gate is `stamp == storedVersion && bytes.Equal(...)`**
(`sqlite/store.go:1459`), not `bytes.Equal` alone. On a build where the
migrator's status version rose, the identical bytes carry a *higher* stamp, so
the same call falls through to the content path — which rewrites `status`, moves
`updated_at`, and writes `observedGeneration` **unclamped**
(`sqlite/store.go:1488-1498`). The re-pass idiom is therefore a settle on most
builds and a status rewrite plus a possible roll-back-to-unsettled on the build
after a schema-version bump, with nothing at the call site to say which. That is
a cliff a controller author cannot see and cannot test for.

It is also the wrong shape twice over: it makes a status write out of a pass that
observed no status, and it requires the controller to hold a status value it
otherwise has no use for.

`SetObservedGeneration` has no content and therefore no version gate to fall off.

### Naming

`SetObservedGeneration`, not `MarkObserved`. The client surfaces are `VerbNoun`
(see [the ADR](../adr/2026-08-07-verb-noun-on-the-client-surfaces.md)); the noun
matches the column and `UpdateStatus`'s own parameter name.

It goes on `Objects`, not a family of its own: `observed_generation` is one
column reached by one new method, the same call that sent `finalizers` to
`Objects().DeleteFinalizer`. `ReconcileOwed` and `DeletionRequests` earned
families on four methods each.

It goes on `ControllerClient` only, never `Client`. `Spec`/`Status` separation is
structural.

## Surface

### Store

In `internal/storeapi/storeapi.go`, on `Objects`, alphabetically between
`ListUnsettledIDs` and `UpdateSpec`:

```go
// SetObservedGeneration records observedGeneration as the generation id's
// controller has settled, writing no status: the handshake for a controller
// whose report is conditions, or nothing at all. Advancing it bumps
// ObservedAt and ResourceVersion, leaving UpdatedAt — which tracks content —
// alone. A generation at or below the recorded one writes nothing, so the
// call is idempotent per generation and can never roll a converged object
// back to unsettled.
//
// Scoped to gk: wrong kind → ErrWrongKind, missing id → ErrNotFound.
// observedGeneration above the row's generation → ErrObservedGenerationFuture;
// below 1, which no generation ever is → ErrInvalidObservedGeneration.
// Returns no row.
SetObservedGeneration(ctx context.Context, gk GroupKind, id ObjectID, observedGeneration int64) error
```

Add the sentinel beside `ErrObservedGenerationFuture`:

```go
// ErrInvalidObservedGeneration is returned by a handshake write given a
// generation below 1. generation is NOT NULL DEFAULT 1, so no object ever
// holds one — a zero is an uninitialised caller, not a stale report.
var ErrInvalidObservedGeneration = errors.New("beehive: observed generation is not a generation")
```

**Apply the floor to `UpdateStatus` too.** This is the one behavior change beyond
the new verb, and it is deliberate: without it the two handshake writers disagree
on the same input. Today `UpdateStatus(…, 0, …)` passes the future check and, from
a NULL `observed_generation`, takes the write path — a `resource_version` bump, an
`object_writes` row and a dependent wake, all to settle nothing. Nothing legitimate
passes 0, so what this surfaces is a caller bug; note it in the changelog as such.

### Client

In `controller.go`, on `ControllerClient`, alphabetically between
`SetConditions` and `UpdateStatus`:

```go
// SetObservedGeneration records the generation this reconcile settled without
// writing status, for a controller whose report is conditions or nothing at
// all. Pass the generation of the object you were handed. A generation at or
// below the recorded one writes nothing; one above the object's current
// generation → ErrObservedGenerationFuture. Compose it inside Within to land
// with a SetConditions.
SetObservedGeneration(ctx context.Context, id ObjectID, observedGeneration int64) error
```

## Semantics

Let `stored` be the row's `observed_generation` (NULL = never settled) and `gen`
its `generation`.

| Case | Outcome |
| --- | --- |
| `observedGeneration < 1` | `ErrInvalidObservedGeneration`, no write |
| `observedGeneration > gen` | `ErrObservedGenerationFuture`, no write |
| `stored` valid and `stored >= observedGeneration` | no write, `nil` |
| otherwise (including `stored` NULL) | writes `observed_generation`, `observed_at`, `resource_version`; appends one `object_writes` entry; wakes the kind |
| id of another kind | `ErrWrongKind` |
| id absent | `ErrNotFound` |

Notes the implementer should not have to re-derive:

- **The clamp is unconditional here**, unlike `UpdateStatus`. `UpdateStatus`
  skips the clamp on its content-changed path because a stale reporter just
  overwrote the status and unsettling gets it re-derived. This verb writes no
  content, so there is nothing to justify an unsettle — a stale report is simply
  dropped.
- **`updated_at` does not move.** It tracks content. `observed_at` records the
  handshake. This matches `UpdateStatus`'s bookkeeping-only branch
  (`sqlite/store.go:1469-1482`), and keeps README's promise that `ObservedAt` is
  "when the object settled at `ObservedGeneration`", not a reconcile heartbeat.
- **A deletion-pending row is not special-cased.** `UpdateStatus` does not gate
  on `deletion_requested_at` either; stay symmetric.
- **A generation moving between load and call is safe.** The controller passes
  the generation of the object it loaded, which is at most the current one, so
  the worst case stamps low, leaves the object unsettled, and the owed pass runs
  it again. Errs toward over-reporting, like everything else here.

### Emission

The write appends an `object_writes` entry and bumps `resource_version`, exactly
as `UpdateStatus`'s handshake-only path does. That is deliberate, not incidental:
a watcher waiting for `ObservedGeneration == Generation` has to see the object
converge (README:678 already promises this for `UpdateStatus`).

The cost is that settling wakes dependents that learn nothing new. It is the cost
`UpdateStatus` already pays, and it is bounded: the clamp makes the write happen
at most once per generation, so unlike a condition write it cannot sustain a
dependency cycle — in a cycle no generation moves, so the second call writes
nothing. Add that qualifier where `docs/TODO.md` lists the writes that sustain a
cycle, rather than adding this one to the list, and cite
`TestASettleWakesADependentOnce` from it — the claim is only as good as that
test.

### Composition with `SetConditions`

The intended shape:

```go
err := client.Within(ctx, func(ctx context.Context) error {
    if err := client.SetConditions(ctx, obj.ID, conds); err != nil {
        return err
    }
    return client.SetObservedGeneration(ctx, obj.ID, obj.Generation)
})
```

This commits atomically but produces **two** `resource_version` bumps and two
`object_writes` entries inside the one transaction. Accepted: both entries commit
together, so no subscriber can observe a torn state, and the tail's
latest-per-object merge collapses them into one delivery. Collapsing them into a
single bump is what option (1) was buying, and it is not worth the coupling.
Anyone revisiting this should reach for a store-level batching primitive, not for
an argument on the condition setter.

## Implementation

### `internal/storeapi/storeapi.go`

Add the method to `Objects` as above. No new sentinel — `ErrObservedGenerationFuture`
already exists (line 72); widen its doc comment from "returned by
`Objects().UpdateStatus`" to name both.

### `sqlite/store.go`

`UpdateStatus`'s bookkeeping-only branch (lines 1469-1482) *is* this write.
Extract it and call it from both:

```go
// stampObserved writes the handshake alone: observed_generation and observed_at
// under a fresh resource_version, leaving status and updated_at untouched.
// Callers have proved the row exists in gk and clamped observedGeneration.
func (s *sqliteStore) stampObserved(ctx context.Context, c dbtx, gk storeapi.GroupKind, id storeapi.ObjectID, observedGeneration int64) error
```

`SetObservedGeneration` on `sqliteObjects` then wraps `Within` (the
read-compare-write must be atomic, same as `UpdateStatus`), does one
`selectScoped` for `generation, observed_generation`, applies the floor, the
future check and the `>=` clamp, and calls `stampObserved`. The scoped read is
what yields `ErrWrongKind`/`ErrNotFound`; do not add a second existence query.

**The extraction stops at two call sites.** `UpdateStatus`'s content path
(`sqlite/store.go:1488-1498`) writes `observed_generation` inline, in one
statement with `status`, and **unclamped** — a stale reporter that overwrote the
content is meant to leave the object unsettled so it gets re-derived. It must not
be routed through `stampObserved`: that would be a second write of the same
columns, and it would silently acquire the clamp and delete the behavior. Leave
the comment already at lines 1488-1492 in place and add a line saying the
extraction deliberately skipped this path, or the next reader finishes the
refactor.

### `controller.go`

```go
func (c *controllerClientImpl[Status]) SetObservedGeneration(ctx context.Context, id ObjectID, observedGeneration int64) error {
	return c.wakeAfter(ctx, c.bh.store.Objects().SetObservedGeneration(ctx, c.gk, id, observedGeneration))
}
```

`wakeAfter` is required: the write appends to the object write log.

### `testutils_test.go`

- `fakeObjects.SetObservedGeneration` → `panic("not implemented: fakeStore.Objects().SetObservedGeneration")`, matching the file's convention.
- `objectsOverride.SetObservedGeneration` passthrough with a hook, matching the
  `UpdateStatus` override at line 892.

### Not touched

`reconciler.go` — the reconciler stamps nothing; that is the rejected option (3).
`docs/reconcile-triggers.md` — this adds no trigger. It *removes* a repeating
one, so review that file's owed-pass entry for a line that now reads wrong.

## Tests

Whitebox, `package beehive`, mirroring source files.

`sqlite/store_test.go`:

- `TestSetObservedGenerationSettlesWithoutWritingStatus` — status bytes and
  `schema_version_status` unchanged; `observed_generation`, `observed_at` and
  `resource_version` all move; `updated_at` holds.
- `TestSetObservedGenerationIsIdempotentPerGeneration` — a second call at the
  same generation writes nothing: no `resource_version` bump, no second
  `object_writes` entry.
- `TestSetObservedGenerationClampsAStaleReport` — a report below the stored value
  writes nothing and does not re-unsettle. This is the difference from
  `UpdateStatus`'s content path and is worth its own test.
- `TestSetObservedGenerationRejectsAFutureGeneration` — `ErrObservedGenerationFuture`.
- `TestSetObservedGenerationRejectsANonGeneration` — 0 and a negative →
  `ErrInvalidObservedGeneration`, no write, from both a NULL and a set
  `observed_generation`. Companion in the `UpdateStatus` tests for the floor
  added there.
- `TestUpdateStatusReUseIsUnsoundAcrossAStatusVersionBump` — the rejected
  option, pinned so nobody recommends it in a doc later: identical status bytes
  under a raised `statusVersion` take the content path, move `updated_at`, and
  write a stale `observedGeneration` unclamped. Asserts the cliff exists rather
  than asserting it is good.
- `TestSetObservedGenerationIsScopedToItsKind` — `ErrWrongKind` for another
  kind's id, `ErrNotFound` for an absent one.
- `TestSetObservedGenerationLeavesTheUnsettledListing` — `ListUnsettledIDs`
  contains the id before and not after.
- `TestSetObservedGenerationSettlesFromNull` — a row that never settled.
- Rollback: inside a failing `Within`, the stamp unwinds.

`controller_test.go`:

- `TestAConditionOnlyControllerSettlesItsGeneration` — the issue's repro. A
  controller whose `Reconcile` calls `SetConditions` and `SetObservedGeneration`
  and never `UpdateStatus`; after the pass, `ListUnsettledIDs` no longer contains
  the id.

  **Assert only that listing.** The owed pass drains two independent records —
  `Objects().ListUnsettledIDs` and `ReconcileOwed().ListIDs`
  (`docs/reconcile-triggers.md:126-127`) — and this verb touches only the first.
  An assertion on "the owed pass stops enqueuing" passes or fails on whether the
  fixture happens to carry an owed count from an `Edges().Add`, which is nothing
  to do with this change.
- `TestSettlingLeavesTheOwedCountAlone` — the other half, pinned separately: an
  object with a non-zero `reconcile_owed` still lists after a settle, and the
  count is unchanged.
- `TestAConditionOnlyControllerStaysUnsettledWithoutTheStamp` — the same
  controller minus the stamp stays listed. Pins the gap as deliberate so nobody
  "fixes" it by stamping in the reconciler.
- `TestSetObservedGenerationComposesWithSetConditions` — both land under one
  `Within`; a returned error rolls both back.

`objectswatch_test.go`:

- `TestASettleEmitsToAKindWatch` — a subscriber sees one change carrying the new
  `ObservedGeneration`.

`waker_test.go`:

- `TestASettleWakesADependentOnce` — the emission cost the spec accepts, on the
  side where it lands. A dependent of a settling object is woken; a second settle
  at the same generation wakes nobody, because the clamp made it a no-op.

  This pair is the assertion behind `docs/TODO.md`'s cycle qualifier — that this
  write cannot sustain a cycle the way a condition write can. Without it that
  claim is prose. Reference the test from the TODO entry, the way the
  dependency-cycle entry already cites
  `TestADependencyCycleIsBoundedByTheFloor`.

## Docs to update when this lands

- `README.md` — the `ControllerClient` listing (~line 658) and a paragraph in the
  handshake section (~line 676-686) saying which verb to use when a pass's whole
  report is conditions. It must **not** offer re-passing the status as an
  alternative spelling; see the rejected option.
- `internal/storeapi/storeapi.go:72-74` — widen `ErrObservedGenerationFuture`'s
  comment from "returned by `Objects().UpdateStatus`" to name both writers, and
  add `ErrInvalidObservedGeneration` beside it.
- `CLAUDE.md` — the generation-handshake bullet.
- `docs/adr/2026-07-27-generation-handshake-and-noop-writes.md` — fold in the
  decision and the two rejected options; then delete this spec and its index
  entry per `docs/specs/README.md`.
- `docs/TODO.md` — the dependency-cycle entry, per the qualifier above.
- `examples/conditions/main.go` — optional, and only if it does not muddy the
  example: its controller already calls `UpdateStatus` (line 124), so the gap is
  not visible there today.

## Open questions

None blocking. One worth a decision at review time: whether `Result` should grow
a way to say "settled at generation N" so the reconciler stamps on the
controller's explicit instruction. That is a strictly larger surface than this
verb and can be added later without changing anything here.
