# A create under a deleting owner pushes that owner's collect

- **Status:** Ready to implement. Supersedes the "`Create` accepts a `WithOwner`
  naming an already-deleting owner" entry in [`../TODO.md`](../TODO.md), which this
  change closes and which is deleted as part of it.
- **Date:** 2026-08-05

## Problem

`insertObject` checks nothing about the owner's lifecycle, and `EdgesAdd` verifies
only that both endpoints exist, never that the target is alive. So a child created
against an owner that is already deletion-pending — and whose cascade has already
run past the point of listing its children — is born live and unmarked under a
finalizing owner. Its `owned_by` edge counts as a live claim in `EdgesHasIncoming`,
which discounts only deletion-pending `depends_on` sources, so the owner cannot be
collected.

Nothing else reaches the owner. `EdgesAdd` bumps no `resource_version`, so no scan
of the write log finds the edge; the waker reads only `depends_on`; and the child's
own `gcCollect` returns immediately because the child is not finalizing. The owner
sits deletion-pending until `deletionPendingSweep` re-lists it and `gcCollect`
re-runs `DeletionRequestsCreateFromOwner`, which is built to be re-run and picks the
new child up.

So this is a **latency gap, not a strand**: the exposure is one GC interval, and no
configuration makes it permanent, since `WithGCInterval` rejects a non-positive
value. It is also plainly visible — a stuck deletion-pending owner, not a quietly
stale read.

## Decision

**Push the owner's collect at commit when the create attaches to an owner that is
already deletion-pending.** `gcCollect` re-cascades and marks the new child, so the
end state is exactly what the sweep would have produced, one GC interval earlier.

This is the same shape as the three pushes already in the tree — a delete request,
a cascade, and a physical delete each enqueue what they moved at commit — and the
same N→1 fan-out and owner-side gate as
[`a-physical-delete-pushes-its-owner`](../adr/2026-08-05-a-physical-delete-pushes-its-owner.md).
The pull behind it is unchanged: the GC sweeper, which cannot be turned off.

Rejected alternatives (all three were live in the TODO entry; record them in the
ADR's *Alternatives considered*):

1. *Reject the create with a new sentinel.* Adds a public failure mode to `Create`
   and `GetOrCreate`, and races anyway — the owner can be marked the instant after
   the check, so the sweep backstop stays regardless. It buys nothing the push does
   not, and costs API surface.
2. *Create the child already marked.* Manufactures a deletion-pending object the
   caller never asked to delete, whose spec is immediately unreachable, and needs a
   new `ObjectsCreateInput` field to say so.
3. *Leave it and document the bound.* The status quo. Fine while the gap was only a
   TODO; not worth keeping once the fix is one gated `AfterCommit` over a read the
   store already performs.

**Semantics do not change.** The child is still created, still returned live to the
caller, and is still marked by the owner's cascade. Only the delay shortens. No new
error, no new option, no schema change.

## Correctness of the gate

The gate is the owner's `deletion_requested_at`, read in the same transaction as the
edge insert. There is no interleaving it misses, because `Within` is
`BEGIN IMMEDIATE` on one connection and writers serialize:

- The owner's mark commits **before** our transaction begins → we read
  `deletion_requested_at` non-NULL, gate fires, we push. (If the mark's own push has
  not yet driven the cascade, ours coalesces with it — harmless.)
- The owner's mark commits **after** our transaction commits → the cascade that
  follows the mark reads our `owned_by` edge and marks the child. No push needed.
- The owner is **physically gone** before our transaction begins → `EdgesAdd`'s
  endpoint check finds no row and returns `ErrNotFound`, which rolls the create back.
  Existing behaviour, unchanged here, and listed only so the enumeration is closed.

A cascade cannot run *between* our edge insert and our commit, so those three are all
the cases there are.

## Changes

### 1. `internal/storeapi/storeapi.go` — `EdgesAddResult` reports the target

```go
type EdgesAddResult struct {
	From GroupKind
	// To is the target object's GroupKind, for a caller routing work to the
	// other end.
	To GroupKind
	// ToDeleting reports whether the target was deletion-pending when the edge
	// landed.
	ToDeleting bool
	ReconcileOwedStamped bool
}
```

Both fields fall out of work `EdgesAdd` already does — the endpoint-existence join
already reads the target row — which is what keeps this inside
[the store-write-shapes rule](../adr/2026-07-30-store-write-shapes.md): a write
returns what a caller reads, and these have exactly one consumer, named below.

**Also amend `Store.EdgesAdd`'s interface doc** (same file, the method at the
`EdgesAdd(...)` line), which enumerates what the result reports and would otherwise
describe only `ReconcileOwedStamped`. One sentence: the result also reports the
target's `GroupKind` and whether it was deletion-pending when the edge landed, both
read as part of the endpoint check, for a caller routing work to the other end. The
existing atomicity requirement covers them — they are read inside the same unit.

### 2. `sqlite/store.go` — `EdgesAdd` selects the target's columns

Widen the existing endpoint join; do not add a second read:

```sql
SELECT f."group", f.kind, t."group", t.kind, t.deletion_requested_at
FROM objects f, objects t WHERE f.id = ? AND t.id = ?
```

Scan `deletion_requested_at` into `*int64` (nullable INTEGER) and set
`ToDeleting = delAt != nil`. Populate `To` alongside `From` in the existing
`out = storeapi.EdgesAddResult{…}`. No change to the stamp, the watermark clear, the
insert, or their ordering.

### 3. `client.go` — `insertObject` pushes the deleting owner

`insertObject` currently discards the `EdgesAddResult`. Keep the push here rather
than in `signalCreated`: `signalCreated` is documented as what the *freshly inserted
row* owes, and this is what the *owner* owes; and `insertObject` is the only place
holding the result.

```go
if co.owner != nil {
	res, err := c.bh.store.EdgesAdd(ctx, raw.ID, *co.owner, RelationOwnedBy)
	if err != nil {
		return nil, err
	}
	// An owner whose cascade already ran past this child would otherwise wait
	// for the sweeper. See docs/adr/2026-08-05-a-create-pushes-a-deleting-owner.md.
	if res.ToDeleting {
		c.bh.signalRequeueNow(ctx, ObjectRef{
			ID: *co.owner, Group: res.To.Group, Kind: res.To.Kind,
		})
	}
}
```

Notes that belong in the code as at most one line each, and in the ADR in full:

- **Immediate, not throttled.** The owner's alarm is typically already pending from
  its own delete push, and a throttled wake would be absorbed by it — which is the
  wake we are trying to beat.
- **Routed by `res.To`, not by `c.gk`.** Edges are cross-kind; the owner need not
  share the child's kind.
- **A client-only owner resolves to no reconciler**, so `signalRequeueNow` is a
  no-op there and the sweeper stays the answer — the same confinement every other
  push has.
- **A cross-process owner is the same no-op, for a different reason.** The push is
  process-local: an owner whose kind *is* registered, but in another process,
  resolves to nothing in the process that ran the create, and falls back to that
  other process's sweeper. Distinct from the client-only case — the kind is
  registered somewhere — and worth its own line, since a write issued straight to
  the `Store` or from a second process is a case this codebase treats as real
  throughout.
- **`GetOrCreate`'s found branch is untouched**, since it never reaches
  `insertObject`; create-time options are ignored there by existing contract.
- **Residual feedback loop, accepted — and the bound is partial.** A controller that
  creates a fresh child under a deletion-pending owner on every pass pushes that
  owner with the floor bypassed. Coalescing bounds it only while the owner is queued
  or in flight: `addLocked` returns early on `isQueued`, so repeat pushes there cost
  a lock and nothing else. It does **not** bound an owner sitting in backoff —
  `requeueNow` stops the timer and clears the alarm before `addLocked`
  (`workqueue.go:290`), and `isQueued` is false for an id whose only state is a
  pending alarm. So an owner whose `gcCollect` keeps failing has its backoff ladder
  reset by every such create. State it this way in the ADR; do not claim coalescing
  covers it. **The physical-delete push carries the identical residual** through the
  same `requeueNow`, which is why this is a property of the push primitive rather
  than a reason to reject this design — but the physical-delete ADR's coalescing
  paragraph is equally partial and should be corrected in the same pass.

### 4. Tests

Whitebox, in the file mirroring the source. Signals, not sleeps.

`sqlite/store_test.go`
- `TestEdgesAddReportsTheTarget` — `To` is the target's `GroupKind` (use a target of
  a *different* kind, so a `From`/`To` mix-up fails), and `ToDeleting` is false for a
  live target.
- `TestEdgesAddReportsADeletingTarget` — mark the target, then add the edge;
  `ToDeleting` is true. Assert `ReconcileOwedStamped` is unaffected either way.

`gc_test.go` — note `fast` returns a slice, so every call site spreads it:
`newTestBeehive(t, store, fast(...)...)` (see `gc_test.go:1099`).

- `TestIntegrationCreateUnderADeletingOwnerCollectsItWithoutASweep` — the decisive
  one, modelled on `TestIntegrationLastChildCollectsItsOwnerWithoutASweep`: build
  with `fast(WithFullPassInterval(0), withOwedPassInterval(time.Hour),
  withoutGCSweeper())...`, so a push is the only possible route. Owner plus one
  child holding a finalizer, `Delete` the owner, wait for the first child's deletion
  request (the cascade has now provably run), then `Create` a second child with
  `WithOwner(owner.ID)` and assert via `waitForDeletionRequest` that it is marked.
  Without the push this test hangs; that is the point of it.
- `TestCreateUnderALiveOwnerPushesNothing` — same rig, live owner. **The negative
  needs a quiescence point, not a timeout**: a bare "assert the owner was not
  dispatched" is a sleep in disguise, against the house rule. Take the owner's
  reconcile count after its own create has settled (a create enqueues its own
  object, so the baseline is one, not zero), create the child, then drive a sentinel
  object of the same kind through the *same* queue with `Client.Requeue` and wait on
  its reconcile signal. A push issued by the create would have been queued before
  the sentinel's, so once the sentinel has run the owner's count is final — assert
  it still equals the baseline.
- `TestClientOnlyDeletingOwnerStillHealsOnTheSweep` — owner on an unregistered kind,
  sweeper on. Pins that the push is an optimisation over the pull, never a
  replacement. Two rig constraints, both from existing code: an unregistered kind
  **cannot hold finalizers** (`checkFinalizersClearable` rejects `WithFinalizers`
  there), so hold the owner deletion-pending the other way — a first child on the
  *registered* kind carrying a finalizer keeps its own row alive, and its `owned_by`
  edge RESTRICT-blocks the owner. And `waitForDeletionRequest` is typed to
  `cSpec`/`cStatus` on the child kind, so watch the child kind, not the owner's.

`testutils_test.go`
- `fakeStore.EdgesAdd` returns the zero `EdgesAddResult`, which reads as "live
  owner, no push". Fill in only if a test needs the other branch.

### 5. Docs

- **New ADR** `docs/adr/2026-08-05-a-create-pushes-a-deleting-owners-collect.md`,
  titled *A create under a deleting owner pushes that owner's collect* — filename and
  title deliberately the same sentence, as elsewhere in the directory. House format.
  Context: the race and why nothing else reaches the owner. Decision: the push, the
  gate, immediate-not-throttled, `To`-routed. Consequences: unchanged semantics, the
  push's confinement (client-only, and cross-process), the residual feedback loop
  with the honest bound above, and the three rejected alternatives. Add it to the
  index in `docs/adr/README.md`.
- **`docs/adr/2026-08-05-a-physical-delete-pushes-its-owner.md`** — two corrections
  in the same pass, both now shared with this push: its "The bound is the work
  queue's coalescing" paragraph overstates what `requeueNow` coalesces (it clears a
  pending backoff alarm), and its N→1 fan-out is no longer the only one. Neither
  changes that ADR's decision.
- **`docs/reconcile-triggers.md`** — this is a new push path and the document is a
  coverage map, so it is not optional. Four edits, and the numbering one has to be
  done deliberately:

  1. **The push-paths table** gains a seventh row: *A create under a deleting owner
     enqueues the owner* / `clientImpl.insertObject` / the owner named by
     `WithOwner` / `EdgesAddResult.ToDeleting`. Update the "exactly six push paths"
     sentence and the "five of the six are immediate" count (six of seven).
  2. **The physical-delete paragraph makes two uniqueness claims and the new push
     breaks both.** It is no longer "the one push gated on a different object than
     the one it enqueues" *or* "the one whose fan-out is N→1" — N children created
     under one owner converge on that owner exactly as N deletes do. Rewrite the
     paragraph to say two pushes share both properties, and keep the gate rationale,
     which is the same rationale in both cases: the write lands once per *row*, and
     rows are unbounded.
  3. **Numbering: append as case 12, the last case in section C — do not insert next
     to case 10.** Inserting mid-section shifts 11 and breaks the references at
     lines 66, 365 and 469; appending at the end of C shifts only section D, whose
     three cases become 13 (`Result.RequeueAfter`), 14 (failure backoff) and 15
     (`Client.Requeue`). Fix exactly these references: line 66 (`Cases 1, 5, 9, 10
     and 11` → add 12), line 469 (`Cases 9, 10 and 11 share one record and one
     driver` → 9 to 12; the new case shares both — the owner's
     `deletion_requested_at` and the GC sweeper), line 581 (`Cases 12, 13 and 14` →
     13, 14 and 15). Lines 365 and 651 reference cases 11 and 6 and are unaffected.
  4. **Fix the pre-existing off-by-one at line 79**, which calls `Client.Requeue`
     "case 13" where its own heading numbers it 14. It becomes 15 under this change;
     correct it rather than carrying the error forward.

  The case body itself: record is the owner's `deletion_requested_at` (already
  durable, still true after a restart — the create adds no record of its own), driver
  is the GC sweeper, restart behaviour is a lost push costing one sweep interval,
  which is exactly today's behaviour and so is the thing the push improves on.
- **`CLAUDE.md`** — the `AfterCommit` list moves from eight users to nine; add the
  create's owner push to it and link the new ADR from the GC bullet.
- **`docs/TODO.md`** — delete the "`Create` accepts a `WithOwner` naming an
  already-deleting owner" entry. Its analysis lives in the new ADR now; do not leave
  a stub.
- **`README.md`** — the `WithOwner` line at §"declare owned_by edge" and the
  `GetOrCreate` sharp-edge paragraph both stay accurate as written. No change unless
  we choose to state the bound explicitly; not part of this change.

## Non-goals

- No new error and no new option. `Create` under a deleting owner stays legal.
- No adoption of an existing row's owner on `GetOrCreate`'s found branch — an object
  has at most one owner, and picking a winner is caller policy. Unchanged.
- The `depends_on` version of this race (a target that moves between a dependent's
  read and its `EdgesAdd`) is closed by the new-edge stamp and is not touched here.
- No `resource_version` bump on `EdgesAdd`. A ref is not a field of the object, and
  making the edge visible to the write log is a much larger change that this push
  makes unnecessary.

## Acceptance

- `go build ./... && go vet ./... && staticcheck -checks=all ./...` clean.
- `go test ./...` green, including the three new `gc_test.go` tests and the two new
  store tests.
- With the sweeper stopped, a child created under an already-cascaded owner is
  marked without any tick.
- `docs/reconcile-triggers.md` lists seven push paths and fifteen cases, every
  cross-reference resolves to the case it names (including the corrected line 79),
  and no paragraph claims a property as unique that two pushes now share.
- `docs/TODO.md` no longer lists this gap.
