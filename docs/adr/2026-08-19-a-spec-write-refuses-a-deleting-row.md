# A spec write refuses a deletion-pending row

- **Status:** Accepted — implemented in `sqlite/store.go`,
  `internal/storeapi/storeapi.go`, `types.go`.
- **Date:** 2026-08-19

## Context

`updateSpec` resolved the row and wrote it. Nothing looked at
`deletion_requested_at`, so `Client.Update` and `Client.UpdateByName` wrote the
spec of an object being torn down, and reported success.

The write was then discarded. Generation bumped, the row went unsettled and was
enqueued, and the pass that picked it up saw `deleting` and took the `gcCollect`
path instead of reconciling. It was not free either: the write appended to
`object_writes`, waking the kind's watch tailers and the dependency waker for a
change nothing would ever apply.

The burden sat entirely on callers, and `README.md` said so — a caller composing
ensure-then-set "should think about the deletion-pending row before it does".
[#126](https://github.com/amorey/beehive/issues/126) reported the bug that
advice is meant to prevent, from a caller who could not reasonably have been
expected to carry it: the guard is three lines about beehive in the middle of
code about clusters.

## Decision

A spec write to a deletion-pending row writes nothing and returns
`ErrDeletionPending`.

The check sits immediately after the resolve, **before the schema version stamp
and before the byte compare**. The row is going away, so a schema complaint
about it is noise, and the answer to "may I write this object" must not depend
on which of two problems the caller has, nor on the bytes they happen to hold.

`ErrDeletionPending` is a sentinel of its own rather than folded into
`ErrNotFound`, because the two ask for opposite responses:

- `ErrNotFound` — nothing holds the name. **Create it.**
- `ErrDeletionPending` — a dying row holds the name. **You cannot create it**:
  the name stays under `UNIQUE`, so a create answers `ErrNameTaken` until GC
  releases it. Wait.

That distinction is what `GetOrCreate`'s `created=false` flattens, and flattening
it is what produced the reported bug.

The ordering and the no-append are stated on the `Objects` interface, not only in
`sqlite`. `Store` is a public extension point, so a guarantee held by one
implementation is not a guarantee — and the order is observable: it decides which
sentinel a deleting row at a stale schema version answers with, and whether a
byte-identical write to a deleting row is refused or quietly succeeds.

## Consequences

**This is breaking, and the failure mode of not upgrading is loud.** An
unmodified caller propagates the sentinel out of `Reconcile` as a `Fail`, and the
object enters the retry ladder — against a condition that clears on GC's
schedule and that can persist indefinitely behind a finalizer nobody clears. So
the cost of ignoring it is a hot loop against a wall, not a lost write. That is
still the better trade than the silent discard it replaces: the old behaviour
dropped the caller's intent and told them it had succeeded.

For an owned child the handler is a bare `return nil`. [A physical delete pushes
its owner](2026-08-05-a-physical-delete-pushes-its-owner.md), so the parent is
re-run when the child is finally collected, and the replacement is created on
that pass. The sentinel reports "not yet", with the retry already arranged by
machinery that exists.

**It reads as asymmetric with delete, and the asymmetry is deliberate.**
`requestDeletion` is silently idempotent on an already-pending row — `Marked` is
false, no error — where a spec write now fails. A second delete request gets the
state it asked for, so there is nothing to report; a spec write does not, so
staying silent would mean discarding the caller's intent. The two verbs disagree
about the same row because they want different things from it.

**`GetOrCreate`'s found branch is unchanged.** It still returns a
deletion-pending row as-is with `created=false`. That branch writes nothing, so
there is nothing to refuse, and the [name-keyed
writes](2026-07-27-name-keyed-writes.md) contract it belongs to still holds.
