# Object writes go to an append-only log, and watches tail it

- **Status:** Accepted — implemented in `sqlite/migrations/0001_init.sql`,
  `sqlite/store.go`, `watchpoll.go`, `client.go`, `beehive.go`, `options.go`,
  `waker.go`.
- **Date:** 2026-08-02

## Context

`ObjectWritesListSince` used to read live rows — `SELECT id, resource_version
FROM objects WHERE resource_version > ?`. It carried the name of a log without
being one, and a deleted row was simply absent from it.

Four costs followed. A watch could not see a delete in the cursor, so it kept a
map of every object it had reported, with the decoded body, and probed
`ObjectsListIDs` on each quiet tick to find rows by absence: memory O(objects in
the kind). A watch could not resume, because no durable position survived a
disconnect. A subscriber could not tell when the initial state was complete,
since the snapshot arrived as `Added` changes indistinguishable from later ones.
And the cursor was store-wide, so a write to any kind made every list watch read
every row of its own kind, with blobs.

## Decision

Record every object write in `object_writes`, appended in the write's own
transaction and taking the `resource_version` that write was assigned.

- **The log is a wake-up list, not a source of truth.** Create and update
  entries carry no payload; consumers route by `object_id` and read current
  state. A physical delete is the exception and carries a JSON row image of the
  whole `RawObject` — a `Deleted` change reports a full object, and by
  collection the row and its `ON DELETE CASCADE` conditions are gone. That image
  is the only surviving copy, which is why `TestWriteLogImageCoversRawObject`
  exists.
- **The soft delete is an ordinary update.** `markForDeletion` leaves a live,
  readable, finalizing object, so it appends `WriteUpdate` and subscribers see
  `Modified` with `DeletionRequestedAt` set. Only the GC's physical removal is
  `WriteDelete`. Callers key on `DeletionRequestedAt` to stop using an object and
  on `Deleted` to evict it.
- **Collection draws a `resource_version`.** It did not before — the comment on
  `objectsDelete` said so as intent — but a delete entry needs a version to be
  ordered against the rest of the log. The shared counter therefore moves on
  collection, and the event log draws from it too.
- **Watches return a snapshot and tail the log above it.** The snapshot is
  read on the caller's goroutine, so subscribe-then-act still holds, and its
  `ResourceVersion` is the exact seam: the stream carries changes
  strictly above it. Entries coalesce per object and are delivered in write
  order, keeping delivery level-triggered. A coalesced run that began with a
  create still reports `Added`: the surviving entry is the later update, but the
  object was absent from the snapshot, and a controller stamping status right
  after a create makes that the common case rather than a corner. One batched
  `ObjectsListByIDs` reads what a batch names — per-object reads would be
  serialized round trips on a single connection, which is what made the old full
  listing competitive.
- **Retention is per kind and bounded by default** (`WithWriteLogRetention`,
  24h). Unlike the event log, an entry lands on every object write and a status
  write bumps `resource_version`, so the log grows at reconcile rate whether or
  not a user opts in. The sweep records what it removed per kind in
  `object_writes_horizon`, in the same transaction as the delete: an empty
  horizon reads as "nothing trimmed", so a lagging horizon would let a resume
  succeed against a log with a hole in it.
- **A page, its horizon and its delete images are read atomically**, in one
  transaction. They describe one instant or they are wrong: a sweep landing
  between them can report a horizon above entries the page already captured — a
  terminal failure for a stream that lost nothing — or delete a captured entry's
  row image, leaving a delete with no state to report. The tail treats a missing
  image as a quarantined row rather than dereferencing it, since `Store` is a
  public extension point and a backend that breaks the contract must cost one
  change, not the process.
- **The horizon is the resume boundary, and the test is strictly `<`.** A cursor
  equal to `trimmed_through` has lost nothing — the next unread entry is
  `trimmed_through + 1`. This is not an edge case: a kind that stops writing has
  its whole log age out, and the horizon converges onto exactly where every live
  tail is parked, so `<=` would end every established watcher on every idle kind.
  A real gap ends the stream with `ErrWatchTooOld` on a terminal `Failed` change.
- **`ObjectWritesMaxVersion` folds the horizon in**, so the position only ever
  rises and the tick gate is `>` rather than `!=`. Without the fold, a kind
  trimmed empty reports 0 against a tail parked higher and lists on every tick —
  on the kind that writes least.

The count bound trims one statement per kind rather than one subquery per row.
Keyed on a literal kind the cutoff is uncorrelated, so SQLite evaluates it once
and every step is a covering-index seek; a kind under its cap yields NULL and
matches nothing. Kinds number in the handful where entries number in the
millions, so enumerating kinds is the right axis — the alternative, a window
function partitioned by kind, still numbers every row in the log on every sweep.

`object_writes` is a rowid table: `INTEGER PRIMARY KEY` makes
`resource_version` the rowid, so it is already clustered in cursor order with no
duplicate key index, and delete entries carry blobs that `WITHOUT ROWID` handles
badly. `object_writes_horizon` is `WITHOUT ROWID` for the `edges` reasons.
`idx_object_writes_kind` spells `resource_version` out even though it is the
rowid: the implicit copy sorts last, and relying on it would lose the range seek
the tail is built on.

## Consequences

- Watch memory is O(1). The `seen` map, the tombstone bodies and the liveness
  probe are gone.
- Storage grows with write volume, at reconcile rate. Retention is the control,
  and it also governs how long a collected object's final state persists.
- The waker reads the real log (`*All` variants) and now sees deletes. That
  changes nothing for it: `edges.to_id` is `ON DELETE RESTRICT`, so an object
  with dependents cannot be physically removed at all.
- A single-object watch tails its kind's log and filters, because the log has no
  index under `object_id`. It costs what its kind writes, not what the object
  writes.
- The log requires entries to become visible in `resource_version` order. This
  holds because the SQLite pool is size 1, so writers serialize. `Store` is a
  public extension point, so a backend that assigns versions outside the commit
  lock must serialize them or not implement the log — otherwise cursors gap
  permanently.

### Deferred

Owner-scoped watches. Denormalising `owner_id` into the log would filter on
ownership as of write time, so a re-parented object would keep arriving on its
old owner's stream. Correcting that needs a read per entry, which removes the
benefit. A subscriber that needs it watches the kind and filters.

Snapshot pagination. A paged snapshot must hold one transaction across every
page to stay consistent with its `ResourceVersion`, which on a pool-of-1 store
holds the only connection for the whole listing.
