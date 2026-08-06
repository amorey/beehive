# Owner-scoped watches

- **Status:** Specified — not implemented.
- **Motivation:** the "Owner-scoped watches" entry in [`docs/TODO.md`](../TODO.md).
- **Date:** 2026-08-06

`OwnedObjectsList` reads one owner's children in a batched query; there is no
watch counterpart, so a subscriber that wants one owner's children watches the
whole kind and filters client-side. This adds `OwnedObjectsWatchList`.

## What is true today

The TODO entry defers this on a blocker — "ownership can change afterwards" —
that no public API can reach:

- An `owned_by` edge is written in exactly one place: `client.go:387`, inside
  `insertObject`, in the create's own transaction, from `WithOwner`. There is no
  re-parent, adopt or disown verb.
- It is removed in exactly one place: `ON DELETE CASCADE` on `edges.from_id`
  (`sqlite/migrations/0001_init.sql:122`) when the child is physically collected.
  `EdgesDelete` is only ever called with `RelationDependsOn` (`controller.go:188`).

So an object's owner is constant for its whole lifetime, and both ends of that
lifetime already append a write-log entry — op 1 on create, op 3 on the collect.
**Ownership already changes only through logged writes to the child**, which is
the property the TODO says would make a logged owner correct by construction. It
holds; it is nowhere written down and nothing pins it.

`depends_on` is the opposite and is not in scope here: `DependenciesAdd` /
`DependenciesRemove` mutate edges freely, and `EdgesAdd` / `EdgesDelete`
deliberately bump nothing (`sqlite/store.go:2278`), so an edge change is
invisible to the tail.

More facts the design rests on:

- Both create paths wrap `insertObject` in `Within` — `Create` (`client.go:317`)
  and `GetOrCreate` (`client.go:451`) — so the create log entry and its owner
  edge commit together on either.
- `edges.to_id` is `ON DELETE RESTRICT` (`0001_init.sql:127`), so an owner can
  never be physically collected while a child's edge points at it. The child's
  own collect is the only way an `owned_by` edge disappears, which is why the
  delete row image is the single gap to close.
- `collectChanges` already reads every live changed row in one batched
  `ObjectsListByIDs` (`objectswatch.go:555`). The owner of the same set is one
  more batched query — `EdgesGroupOutgoingByID`, which `LoadOwner` already uses
  (`client.go:702`). It really is one query: `idChunkSize` is 30000 against a
  `tailPageCap` of 512.
- `Watch(id)` is already a client-side filter over the shared kind tailer
  (`objectswatch.go:106`). Owner scope is the same move with a wider predicate.

## Design

**Resolve ownership from current state, in the tailer. Do not denormalise
`owner_id` into `object_writes`.**

Denormalising is wrong on three counts. It records ownership as of the write,
which the schema would then be committed to on disk. It costs an
`(owner_id, resource_version)` index on what will be the largest table in the
database, for a feature with no consumer yet. And it does not even work for the
entry that matters most: `ObjectsCreate` appends the create entry *before*
`insertObject` calls `EdgesAdd`, so a create entry would carry `NULL`. That same
ordering is why current-state resolution *is* correct — both statements are in
one transaction, and the tailer only ever reads committed log entries, so by the
time an entry is visible its edge is too.

### Live changes: one batched edge read per page

`collectChanges` gains an owner lookup beside the object read: one
`EdgesGroupOutgoingByID(ids, RelationOwnedBy)`, its result attached to each
`rawChange`. One extra statement per drain page, shared by every subscriber on
the kind — not a read per entry.

Pass it `live` — the slice `collectChanges` already builds for
`ObjectsListByIDs` (`objectswatch.go:549`) — not the whole page. A deleted id
cascaded its edges away, so it can never match, and it takes its owner off the
row image anyway.

`rawChange` gains one field:

```go
// Owner is the object's current owner, or nil for an unowned object. Set only
// when the tailer has an owner-scoped subscriber; see ownerScoped.
Owner *ObjectRef
```

It needs no handling in `mergeRawChange`: the merge takes `next` wholesale except
for the coalesced op, and `next` is the newer resolution of a value that cannot
change. `prev` and `next` come from different pages read at different times — so
this is not "the same current state", it is immutability doing the work. It also
covers the flag flipping between the two pages: `next` is the one that carries an
owner.

### The gate, and why it is sticky

The lookup is gated so a kind with no owner-scoped watch pays nothing: an
`ownerScoped atomic.Bool` on `objectTailer`, sampled at the top of each
`collectChanges`.

**The flag is set and never cleared.** A gate that could clear would let a change
be published without an owner while a scoped subscriber was live, and that
subscriber would silently drop it. Sticky plus one ordering rule closes it:
`tailStream` sets the flag *before* registering its receiver and taking its
snapshot or starting its replay.

The property to hold is: **no change above a scoped subscriber's floor has a nil
`Owner` for a reason other than being unowned.** It has two legs, because the
floor has two sources.

*Snapshotting subscriber, floor `at`.* A change above `at` was committed after
the snapshot read `at`, so it was read by a page started after the snapshot, so
it was read after the flag was set, and it carries an owner. Everything at or
below `at` is dropped by the floor and needs no owner. This holds even against a
join mid-drain: page reads and commits serialize on the size-1 pool, so a page
collected with the flag still off holds only entries at or below the joiner's
`at`.

*Resuming subscriber.* This leg is **not** covered by the argument above, and
`WithResumeFrom` is otherwise unchanged by this spec. A resume never takes a
snapshot; its floor starts as a caller-supplied position that can sit
arbitrarily far below a nil-`Owner` change already buffered in its receiver. It
is sound anyway, for a different reason: `replay` pages until it reads an empty
one (`objectswatch.go:875`), so the floor it hands `consume` is the log head as
of a read that started after the flag was set — above anything collected before
it. That makes **"a replay always runs to the head" load-bearing for a
correctness property in a different function**, which it is not today and which
nothing pins.

Both legs go in the ADR, and the resume leg gets its own test.

The cost of never clearing is one query per drain on a kind that once had a
scoped watch, and the tailer dies with its last subscriber anyway.

### Deletes: capture the owner in the row image

By the time a physical delete lands the `owned_by` edge has cascaded away, so a
`Deleted` change has no owner to resolve against. The row image is the only
surviving evidence, which is what it is for — `0001_init.sql:299` says conditions
are captured there "or they are lost", and the owner is the same class of thing.

`RawObject` gains one field:

```go
// Owner is populated only on a delete entry's row image, where the edge has
// already cascaded away; nil on every row read from the objects table, which
// resolves ownership through EdgesListOutgoingByRelation like every other
// secondary lookup.
Owner *ObjectRef `json:"owner,omitempty"`
```

`objectsDelete` (`sqlite/store.go:2164`) reads it with the image it already
takes, before the `DELETE`:

```go
image, err := s.ObjectsGet(ctx, id)          // existing
owners, err := s.EdgesListOutgoingByRelation(ctx, id, RelationOwnedBy)
if len(owners) > 0 { image.Owner = &owners[0] }
```

One extra indexed read per physical collection. The soft delete needs nothing:
it is an ordinary update, the row and its edge are both live, and current-state
resolution covers it.

The rejected alternative is a subscriber-side membership set — "drop a `Deleted`
for an id I never saw added". It is sound for a snapshotting subscriber and
silently wrong for a `WithResumeFrom` one, which never took the snapshot that
would have established membership. Worth keeping as a belt-and-braces filter if
it falls out for free; not the mechanism.

**Upgrade window.** Delete row images written before this lands carry no
`Owner`, so a resume across the boundary drops a collected child's `Deleted`.
Bounded by the write log's 24h default retention, and acceptable pre-release —
recorded because it is a silent drop, not because it needs handling.

### nil Owner is overloaded, so make the bad case loud

`Owner == nil` means both "unowned" and "not resolved", and the whole design
rests on the second never reaching a scoped subscriber above its floor. If it
ever does, that subscriber drops the change forever and nothing says so.

`decodeChanges` therefore warns — beside `warnUndecodable` (`client.go:685`),
which is the same "this should not happen, do not kill the stream" shape — when
a scoped subscriber sees a nil `Owner` above its floor, naming the id and the
version. The scoped watch tests assert through `captureLogger`
(`testutils_test.go:296`) that it never fires. That is the assertion
`TestAScopedWatchJoiningMidDrainSeesItsOwner` is really making, made
continuously rather than at one instant, and it is what would catch a future
change that breaks either leg of the gate proof.

### API

```go
// OwnedObjectsWatchList streams the objects of this client's kind owned by
// ownerID: a snapshot of them now, then every change to one of them. A child
// created under ownerID later arrives as Added, and its collection as Deleted.
// ownerID is not existence-checked and is typically another kind. Same options,
// same errors and the same shared tailer as WatchList.
OwnedObjectsWatchList(ctx context.Context, ownerID ObjectID, opts ...WatchOption) (
    ObjectListSnapshot[Spec, Status], <-chan ObjectChange[Spec, Status], error)
```

A method rather than a `WithOwnerScope` `WatchOption`, for two reasons. It sits
beside `OwnedObjectsList` and reads under the naming convention — `OwnedObjects`
is a family of its own, cardinality in the verb. And an option would be
representable on `Watch(id)`, where it means nothing, forcing either a silent
ignore or a call-time error for a combination that should not be spellable.

`WithResumeFrom` and `WithLoads` compose unchanged — but see the resume leg of
the gate proof above, which is what makes `WithResumeFrom` sound here.

**This breaks `Client`, and that is the more expensive of the two breaks in this
spec.** `Store` has no external implementer it could break (`docs/TODO.md`
records why: four of its signature types have no public alias). `Client` has an
obvious one — a test double in an embedder's own suite — and unlike `Store` it is
the interface the whole package exists to offer. The break is taken because a
watch over one owner's children is not expressible any other way and because
`Client` is pre-release, but it should be listed in the release notes as an
interface addition, not folded in silently the way an implementation change
would be.

**Memory.** A scoped subscriber's receiver takes no `WithKeyFilter` — unlike
`Watch(id)` (`objectswatch.go:615`), there is no key predicate, because
membership is what the watch is trying to learn. So it holds pending keys for
the whole kind, exactly as `WatchList` does. A membership-driven key filter is
not available: the closure runs concurrently with the membership it would
consult, and a newly created child would be filtered out before the subscriber
could learn it belongs.

### Store

One new member, a six-line implementation over the existing `s.snapshot` helper
(`sqlite/store.go:2814`) and the existing `ObjectsListByIncomingEdge`:

```go
// ObjectWritesSnapshotByOwner is ObjectWritesSnapshot for one owner's children:
// the objects of kind gk with an owned_by edge to ownerID, and the log position
// they are consistent at, read in one transaction.
ObjectWritesSnapshotByOwner(ctx context.Context, gk GroupKind, ownerID ObjectID) ([]*RawObject, int64, error)
```

Composing this client-side out of `Store.Within` + the two existing members
would avoid a `Store` break, and is deliberately not done: the two sibling
snapshots keep their transaction in the store, and splitting the third across the
boundary trades a real invariant for a break whose cost `docs/TODO.md` measures
as a population of zero.

**This is a `Store` break, so it is a ride-along window.** Two `TODO.md` entries
are waiting for exactly one — `EventsAddInput`, and the four `storeapi` types
with no public alias — and the second of those is what makes `Store` externally
implementable at all. Decide whether to take them here before landing; the
`EventsAddInput` entry already records that it missed the last window.

## Implementation

In order; each step compiles.

1. **`internal/storeapi/storeapi.go`** — add `RawObject.Owner`, with the godoc
   above. Add `ObjectWritesSnapshotByOwner` to `Store`, alphabetically among the
   `ObjectWrites*` members.
2. **`sqlite/store.go`** — implement `ObjectWritesSnapshotByOwner` over
   `s.snapshot` + `ObjectsListByIncomingEdge`. Populate `image.Owner` in
   `objectsDelete` before the `DELETE`.
3. **`testutils_test.go`** — `fakeStore.ObjectWritesSnapshotByOwner`, panicking
   like its siblings until a test needs it.
4. **`objectswatch.go`** — everything else, including the method itself:
   `WatchList` and `Watch` live here (`objectswatch.go:100`, `:106`), and the
   tests for all of this go in `objectswatch_test.go`.
   - `rawChange.Owner`;
   - `objectTailer.ownerScoped atomic.Bool`;
   - `collectChanges` takes the gate, does the batched edge read over `live` when
     set, and takes a delete entry's owner off `w.Final.Owner` instead;
   - `tailStream` takes an `ownedBy *ObjectID`, sets the tailer's flag **before**
     `hub.Receiver`, snapshots through `ObjectWritesSnapshotByOwner`, and passes
     the scope down to `decodeChanges` and `replay`;
   - `decodeChanges` drops a change whose `Owner` is not `ownedBy`; cheapest
     beside the existing floor check, and independent of it. A nil `Owner` above
     the floor warns before it drops — see above;
   - `replay` filters its page the way `only` already does, but *after*
     `collectChanges` rather than before it: ownership is not in the log entry,
     so there is nothing to filter on until the owner is resolved. Note the cost
     in a comment — a scoped resume reads back every object in the page;
   - `OwnedObjectsWatchList` on `clientImpl`, beside `WatchList`.
5. **`client.go`** — the `OwnedObjectsWatchList` member on the `Client`
   interface, which is here (`client.go:281`), not in `types.go`. Listed
   alphabetically, so beside `OwnedObjectsList`.
6. **Docs** — an ADR, `docs/adr/2026-08-??-owner-scoped-watches-resolve-current-ownership.md`,
   carrying the current-state-not-log decision, the sticky-gate ordering argument
   and the ownership invariant below. Index it in `docs/adr/README.md`, summarise
   it in `CLAUDE.md` beside the watch-shared-tail bullet, shrink the `TODO.md`
   entry to a pointer, and delete this spec.

## The invariant, and its tripwire

Everything above rests on one rule:

> **An ownership change is a logged write to the child.**

True today only by construction, and a future re-parent verb could break it with
nothing failing. It goes in the ADR, and it gets a test.

"No public verb moves an `owned_by` edge" is a claim over an open set and cannot
be asserted behaviourally — a verb added next year is exactly the case a
behavioural test does not cover. `TestTheSchemaIsOneMigration` is the precedent:
assert the *structure* that makes the claim true.

`TestOwnedByIsWrittenInOnePlace` — parse the package and assert that
`EdgesAdd(..., RelationOwnedBy)` has exactly one call site, `insertObject`.
Anything that adds a second has to come here and read why.

If re-parenting is ever added, the ADR states the price: bump the child's
`resource_version` so the change is logged, and deliver it to both owners'
streams — a `Deleted` on the old scope and an `Added` on the new.

## Tests

Whitebox in `objectswatch_test.go`, mirroring the source file. Against the real
store unless noted; synchronise on signals.

- `TestOwnedObjectsWatchListSnapshotsOnlyTheOwnersChildren` — three children over
  two owners; the snapshot holds one owner's two.
- `TestOwnedObjectsWatchListDeliversALaterChild` — create under the owner after
  the snapshot, expect `Added`. This is the case denormalisation gets wrong, and
  it is the create-entry-before-edge ordering that makes it work.
- `TestOwnedObjectsWatchListIgnoresAnotherOwnersChild` — write to a sibling
  owner's child, expect nothing, then write to one of ours and expect only that.
  Guards against a filter that passes everything.
- `TestOwnedObjectsWatchListReportsACollectedChild` — collect a child, expect
  `Deleted` carrying the object. Fails without the row-image owner.
- `TestOwnedObjectsWatchListIgnoresAnUnownedObject` — a child with no owner at
  all, so a nil `Owner` is not treated as a match.
- `TestOwnedObjectsWatchListResumesFromAPosition` — `WithResumeFrom` below the
  head replays only the owner's children, including a collected one.
- `TestAScopedResumeSeesAnOwnerOnEveryLiveChange` — the resume leg of the gate
  proof, which the snapshot tests do not touch. Publish changes with the flag
  off, then attach a scoped resume from a position below them, and assert every
  delivered change carries an owner and the nil-`Owner` warning never fires.
  This is the test that fails if `replay` ever stops running to the head.
- `TestOwnedObjectsWatchListReportsAnUndecodableCollectedChild` — a collected
  child whose spec does not decode. `decodeChanges` quarantines it and still
  reports the `Deleted` with a nil object (`objectswatch.go:828`); the scope
  filter must still pass it, because the owner comes off `rawChange.Owner` and
  not off the object that failed to decode. The one place the two mechanisms
  cross.
- `TestOwnedObjectsWatchListSharesTheKindTailer` — a scoped and a kind-wide watch
  on one kind; assert one tailer, and that both see their own view.
- `TestTheOwnerLookupIsSkippedWithoutAScopedWatch` — a counting store wrapper
  (`countingLoadStore` is the pattern, `objectswatch_test.go:2339`) sees zero
  `EdgesGroupOutgoingByID` calls from a plain `WatchList` drain, and one per page
  once a scoped watch joins.
- `TestAScopedWatchJoiningMidDrainSeesItsOwner` — the ordering argument. Set the
  flag, hold the drain, and assert the joining subscriber's first delivery above
  its snapshot carries an owner.

Benchmark: extend `BenchmarkWritesUnderWatch`
(`objectswatch_bench_test.go`) with a scoped-watch row, so the extra query per
drain has a number beside the table in `docs/TODO.md`.

## Out of scope

- **A dependents/dependencies-scoped watch.** `DependentsList` /
  `DependenciesList` have a superficially similar hole, and it is a genuinely
  harder one: `depends_on` edges are mutable and log nothing, so making a
  scoped watch sound there needs the edge write to become a write to the source.
  That is a separate change, and folding it in is what has kept the TODO entry
  parked.
- **Re-parenting.** Not added here, only priced.
- **A consumer.** The TODO entry says this is worth doing "when someone has a
  real fan-out of children per owner", and nothing in the tree establishes one.
  The work is small and mostly reuse; that is a reason it is cheap, not a reason
  it is needed.
