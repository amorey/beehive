# An owner-scoped watch resolves ownership from current state, not from the log

- **Status:** Accepted — implemented in `objectswatch.go`, `sqlite/store.go`.
- **Date:** 2026-08-06

## Context

`OwnedObjectsList` reads one owner's children in a batched query. Its watch
counterpart, `OwnedObjectsListWatch`, has to answer the same question of a
change stream: is this object one of ownerID's children?

The write log records what changed, not what it belonged to. The obvious fix —
denormalise `owner_id` into `object_writes` — is wrong three times over. It
records ownership as of the write, committing the schema on disk to an answer
that a re-parent verb would falsify. It costs an `(owner_id, resource_version)`
index on what will be the largest table in the database. And it fails on the
entry that matters most: `Objects().Create` appends the create entry before
`insertObject` calls `Edges().Add`, so the create would carry `NULL`.

That same ordering is what makes the alternative sound. Both statements are in
one transaction and the tailer only ever reads committed entries, so by the time
an entry is visible its edge is too.

## Decision

**The tailer resolves each page's owners from current state.** One
`Edges().GroupOutgoingByID` over the page's live ids, beside the `Objects().ListByIDs`
it already runs — one statement per drain page, shared by every subscriber on
the kind, not a read per entry. A collected object has no edges left, so it
takes its owner off the delete entry's row image (`RawObject.Owner`), which is
the same reason the image carries conditions: captured there or lost.

**The lookup is gated by `objectTailer.ownerScoped`, which is set before a
scoped subscriber registers and never cleared.** The property to hold is that no
change above a scoped subscriber's floor has a nil `Owner` for a reason other
than being unowned — a nil for any other reason is dropped silently and forever.
It has two legs, because the floor has two sources:

- *Snapshotting subscriber, floor `at`.* A change above `at` was committed after
  the snapshot read `at`, so it was read by a page started after the snapshot,
  hence after the flag was set. Everything at or below `at` is dropped by the
  floor and needs no owner. This holds against a join mid-drain too: page reads
  and commits serialise on the single connection, so a page collected with the
  flag still off holds only entries at or below the joiner's `at`.
- *Resuming subscriber.* Not covered by the above — a resume takes no snapshot,
  and its floor starts at a caller-supplied position that can sit far below a
  change already buffered in its receiver. It is sound because `replay` pages
  until it reads an empty one, so the floor it hands `consume` is the log head as
  of a read that started after the flag was set. **"A replay runs to the head" is
  therefore load-bearing for a correctness property in another function.**

A gate that could clear would break both legs. Never clearing costs one query
per drain on a kind that once had a scoped watch, and a tailer dies with its last
subscriber anyway.

`mergeRawChange` needs no owner handling: it takes `next` wholesale, and `next`
is the newer resolution of a value that cannot change. `prev` and `next` come
from different pages read at different times, so it is immutability doing the
work, not a shared read. That also covers the flag flipping between two pages —
`next` is the one carrying an owner.

Because a nil `Owner` is overloaded — "unowned" and "never looked up" are the
same value — `rawChange` carries `OwnerResolved` beside it, and `decodeChanges`
warns on an unresolved change rather than on a nil one. That distinction is the
signal's whole worth: gated on nil, a kind holding both owned and standalone
objects would warn on every write to a standalone one, and routine noise is
exactly what makes the one signal that would catch a soundness break worthless.
It is the continuous form of the gate argument above.

## The invariant

> **An ownership change is a logged write to the child.**

True today by construction: `owned_by` is written in exactly one place
(`insertObject`, in the create's transaction) and removed only by
`ON DELETE CASCADE` when the child is collected. Both ends append a log entry.
`edges.to_id` is `ON DELETE RESTRICT`, so an owner cannot be collected out from
under a child either.

`TestOwnedByIsWrittenInOnePlace` asserts the structure rather than the
behaviour: "no verb moves an `owned_by` edge" is a claim over an open set, and a
verb added next year is exactly what a behavioural test would miss.

**If re-parenting is ever added, this is the price:** bump the child's
`resource_version` so the change is logged, and deliver it to both owners'
streams — a `Deleted` on the old scope and an `Added` on the new.

## Consequences

- A scoped subscriber takes no `WithKeyFilter`, unlike `Watch(id)`: membership is
  what the watch exists to learn. It holds pending keys for the whole kind, as
  `WatchList` does. A membership-driven filter is not available — the closure
  would run concurrently with the membership it consults, and a new child would
  be filtered out before the subscriber could learn it belongs.
- A scoped resume reads back every object in each page it replays, since there is
  nothing to narrow on before the owner is resolved.
- Delete row images written before this landed carry no owner, so a resume across
  that boundary drops a collected child's `Deleted`. Bounded by the write log's
  24h retention, and pre-release.
- `Store` and `Client` both gained a member. The `Client` break is the more
  expensive: `Store` has no external implementer it could break, and `Client` has
  an obvious one in an embedder's test double.
- `DependentsList`/`DependenciesList` have a superficially similar hole and a
  genuinely harder one: `depends_on` edges are mutable and log nothing, so a
  scoped watch there needs the edge write to become a write to the source.
- **One owner per child is assumed, not enforced.** `edges` permits several
  `owned_by` rows per child, and both the resolved owner and the delete row
  image take the first — as `fetchOwnerRef`, `LoadOwner` and `OwnersGet` already
  do. The typed API cannot create such a child (`WithOwner` sets one field), so
  this matches the readers around it rather than the store's raw capability.
  Making the watch multi-owner-correct on its own would put those semantics in a
  fourth place while `Owner()` still returns one; `docs/TODO.md` carries the
  decision, which is to forbid the state rather than fan out to it.
