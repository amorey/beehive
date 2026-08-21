# Collect without a transaction it does not need

- **Status:** Planned. Two small fixes in one function; the smallest PR in this
  set.
- **Date:** 2026-08-20
- **Depends on:** nothing.

## Why

Two things in `gcCollect` (`gc.go:35`) cost more than they need to.

**It opens a write transaction to find out it has nothing to do.** The first
thing inside `Within` is a `GetMeta`, and the next line returns when
`DeletionRequestedAt` is nil. The sweeper only calls it for rows it listed as
deletion-pending, so this is narrow — but a reconcile calls it whenever its own
load said `deleting`, and that load can be stale in the other direction too.

**It reads each owner one at a time.** After the finalizer and RESTRICT checks it
lists the object's `owned_by` edges, then calls `GetMeta` per owner to test
`DeletionRequestedAt` (`gc.go:95-110`). Almost always one owner, so almost always
one extra read — but it is a loop over a store call inside a write transaction,
which is the shape everything else in this codebase avoids.

## The change

**Probe before the transaction.** Read the object's metadata first. Return
immediately when it is not deletion-pending. Open `Within` only when there is work.

**Return the owner's deletion state with the edge.** `Edges().ListOutgoingByRelation`
returns `[]ObjectRef`. Add a sibling that carries the target's
`deletion_requested_at`, so the blocked-owner test is one query. `Edges().Add`
already reports `ToDeleting` for the same reason, so the shape exists.

## The rules this rest on

**A probe outside the transaction can be stale in one direction only.** It says
"not deleting" for a row that has since become deletion-pending. That row is
enqueued by the delete request's own commit push, so a pass is already coming.
It cannot say "deleting" for a row that is not — nothing clears
`deletion_requested_at`.

So the probe is safe, and it is safe for a reason worth writing down: the mark is
one-way.

**The transaction still covers everything after the probe.** The cascade, the
finalizer check, the edge drop, the RESTRICT check and the delete stay in one
transaction. This is not a general loosening.

## Edge cases the implementer would otherwise guess at

- **The probe re-reads inside the transaction.** Do not delete the `GetMeta`
  inside `Within` — the probe is an early return, not a replacement. The state
  must be re-confirmed under the lock, because the cascade below acts on it.

- **`ErrNotFound` from the probe is a success**, as it is today: the row was
  collected by someone else, and `deleted` is false because this call did not do
  it. Check what the callers do with that before changing the shape.

- **The new edge listing is one more `Edges` member**, and `Edges()` takes no
  `GroupKind`, so it stays id-keyed like its siblings. Name it for the qualifier
  it adds, per the naming convention.

- **An owner that is live is not pushed**, because `requeueNow` bypasses the
  floor and a live owner would spin. That gate is the reason the per-owner read
  exists; keep it exactly.

- **The comment in `freePagesRelease`** (`sqlite/store.go:163`) explains a
  negative page count with "another writer freed more than the drain took". There
  is no other writer. Fix it here — it is a one-line docs fix in a neighbouring
  file, and it is not worth its own PR.

## Tests

In `gc_test.go`:

- `gcCollect` on a live object takes no transaction. Assert on `txCount`.
- `gcCollect` on a deletion-pending object with finalizers takes one.
- An object with a deletion-pending owner pushes it, with one edge query and no
  per-owner read.
- An object with a live owner pushes nothing.
- An object collected between the probe and the transaction reports
  `deleted=false` and no error.
- The existing cascade, RESTRICT and finalizer tests pass unchanged.

## On ship

No ADR. Add one line to
[the physical delete's owner push ADR](../adr/2026-08-05-a-physical-delete-pushes-its-owner.md)
recording that the owner's deletion state now rides the edge listing.
