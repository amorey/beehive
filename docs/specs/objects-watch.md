# Objects watch: a durable write log, a split snapshot, and resume

- **Status:** Proposed. Not implemented. No code in this repository behaves as
  described here.
- **Date:** 2026-08-02
- **Supersedes on acceptance:** the watch half of
  [the periodic-scan drivers ADR](../adr/2026-07-28-periodic-scan-drivers.md).

## Scope

This spec covers the object watches: `Client.ObjectsWatchList` and
`Client.ObjectsWatch`. It does not change `EventsWatch` or `SchedulesWatch`.

## Context

`Store.ObjectWritesListSince` reads live rows:

```sql
SELECT id, resource_version FROM objects WHERE resource_version > ? ORDER BY resource_version
```

There is no write log. The name describes a scan of current state. A deleted row
is absent from that scan, so a delete leaves no record.

Four properties follow from this, and all of them are costs:

1. A watch cannot detect a delete from the cursor. It keeps a `seen` map of every
   object it reported, with the decoded body, and probes `ObjectsListIDs` on each
   quiet tick to find rows by absence. Memory is O(objects in the kind).
2. A watch cannot resume. There is no position that stays valid across a
   disconnect, because the log does not hold what happened.
3. A subscriber cannot tell when the initial state is complete. The snapshot
   arrives as `Added` changes that are identical to later `Added` changes.
4. The cursor `ObjectWritesMaxVersion` is store-wide. A write to any kind makes
   every list watch read every row of its own kind, with blobs.

## Decision

Record object writes in an append-only table. Read the watches from that table.

### Schema

Both tables follow the conventions in the header of `0001_init.sql`: `STRICT`,
`INTEGER` epoch-millisecond timestamps, JSON in `TEXT`, `""` for the core group.

```sql
-- ============================================================
-- object_writes
-- Append-only change log. One entry per committed object write,
-- inserted in that write's transaction and carrying the
-- resource_version it was assigned. The watch tail and the
-- dependency waker read it; retention trims it.
-- ============================================================

-- A rowid table, unlike edges. INTEGER PRIMARY KEY makes resource_version the
-- rowid itself, so the table is already clustered in cursor order with no
-- separate key index — WITHOUT ROWID would add nothing and would hurt: delete
-- entries carry blobs, and WITHOUT ROWID wants rows small against the page size.
CREATE TABLE object_writes (
    -- From resource_version_seq, so it orders against objects.resource_version
    -- and events.resource_version. Never reused. The rowid alias: an append is
    -- an in-order insert at the end of the B-tree, and a cursor scan is
    -- sequential.
    resource_version INTEGER PRIMARY KEY,

    -- NO FOREIGN KEY, deliberately. ON DELETE CASCADE would erase exactly the
    -- delete entries this log exists to record; ON DELETE RESTRICT would make
    -- the log block collection. The id is a routing key, not a reference, and
    -- ids are never reused (objects.id is AUTOINCREMENT).
    object_id INTEGER NOT NULL,

    "group" TEXT NOT NULL,
    kind    TEXT NOT NULL,

    -- 1 create, 2 update, 3 physical delete. The soft delete is an UPDATE:
    -- markForDeletion sets deletion_requested_at and bumps resource_version,
    -- and the object is still live and still readable, so it appends op = 2.
    -- op = 3 is only the GC's physical removal, once finalizers clear.
    op INTEGER NOT NULL CHECK (op IN (1, 2, 3)),

    written_at INTEGER NOT NULL, -- epoch ms; the retention key

    -- Delete entries only, NULL otherwise: a JSON row image of the object as it
    -- was when collected — the whole RawObject, spec and status and their schema
    -- versions included. NOT just the two blobs: a Deleted change reports a full
    -- *Object, and rawToTyped needs name, generation, observed_generation,
    -- observed_at, deletion_requested_at, finalizers, created_at and updated_at
    -- as well. Conditions live in their own ON DELETE CASCADE table and are gone
    -- by collection, so they are serialized in here or they are lost.
    final TEXT
) STRICT;

-- The watch tail: seek by kind, scan in cursor order. object_id and op are in
-- the key to make it covering for create and update entries — without them
-- every entry costs a row fetch into the table that holds the delete blobs,
-- which is the whole cost argument for a blob-free tail.
--
-- resource_version is spelled out even though it is the rowid and every index
-- entry carries the rowid anyway: the implicit copy sorts LAST, so relying on it
-- would order by ("group", kind, object_id, op, resource_version) and lose the
-- range seek the tail is built on. The duplicate costs a few bytes per entry.
CREATE INDEX idx_object_writes_kind
    ON object_writes("group", kind, resource_version, object_id, op);

-- ObjectWritesSweep's maxAge bound. Without it the age sweep is a full scan of
-- the largest table in the database on every GC tick.
CREATE INDEX idx_object_writes_age ON object_writes(written_at);

-- ============================================================
-- object_writes_horizon
-- What retention has removed, per kind. A resume BELOW
-- trimmed_through is refused: the log has a hole under it.
-- The sweep is the only writer.
--
-- WITHOUT ROWID, for the edges reasons: a composite text key that
-- would otherwise be stored twice, tiny rows, and one row per kind
-- rather than per write. Reads are always by full primary key.
-- ============================================================

CREATE TABLE object_writes_horizon (
    "group"         TEXT    NOT NULL,
    kind            TEXT    NOT NULL,
    -- Highest resource_version trimmed for this kind. A cursor sitting exactly
    -- on it has lost nothing: the next unread entry is trimmed_through + 1.
    -- Loss is cursor < trimmed_through, everywhere this value is compared.
    trimmed_through INTEGER NOT NULL
    PRIMARY KEY ("group", kind)
) STRICT, WITHOUT ROWID;
```

Every object write appends one entry in the same transaction as the write
itself. The entry takes the `resource_version` the write was assigned. A write
the store skips as a content no-op appends nothing, because it is not a write.
See [the generation ADR](../adr/2026-07-27-generation-handshake-and-noop-writes.md).

**The physical delete has to start drawing a `resource_version`.** It does not
today. `objectsDelete` (`sqlite/store.go:1717`) removes the row without touching
`resource_version_seq`, and its comment states that as intent: "A delete draws no
resource_version: the row is gone, so there is nothing for a write-log scan to
report — watches derive the tombstone from absence." That reasoning is exactly
what this spec reverses. A delete entry needs a version to be ordered against
every other entry, so collection begins moving the shared counter — which events
also draw from. The comment must be rewritten rather than worked around; read as
it stands, it looks like an invariant to preserve.

The two delete stages append different entries, and conflating them is the
likeliest implementation error. `markForDeletion` (`sqlite/store.go:1577`) sets
`deletion_requested_at`, bumps `resource_version`, and leaves a live, readable,
finalizing object: that is `op = 2`, and it reaches subscribers as `Modified`
with `DeletionRequestedAt` set. Only the GC's physical removal is `op = 3`.
Stamping `3` on the soft delete would report `Deleted` for an object that is
still there.

#### The delete lifecycle in the log

Deletion is asynchronous and multi-step, so one `Client.Delete` produces a
sequence of entries, not one. For an object with two finalizers and one owned
child:

| Step | Writer | Entry | Subscriber sees |
| --- | --- | --- | --- |
| `Delete(name)` | `markForDeletion` | `op = 2` | `Modified`, `DeletionRequestedAt` set |
| child marked | `DeletionRequestsCreateFromOwner` (`gc.go`) | `op = 2` **on the child** | `Modified` on the child's kind |
| finalizer cleared | `FinalizersDelete` | `op = 2` | `Modified` |
| finalizer cleared | `FinalizersDelete` | `op = 2` | `Modified` |
| collected | `ObjectsDelete` | `op = 3`, final row image | `Deleted` |

Three things follow that are easy to miss:

- **The cascade writes to the children, not the parent.** `gcCollect` marks owned
  children inside its own transaction, and each mark is a write to that child's
  row, so it appends the child's entry under the child's `GroupKind`. A cascade
  therefore appears on the children's watches as a wave of `Modified`, then as
  `Deleted` as each child is collected in a later sweep.
- **A sweep that makes no progress appends nothing.** `gcCollect` returns early
  when finalizers remain or `EdgesHasIncoming` reports a referrer, and those
  paths write no row. The log grows with progress, not with sweeps — which is
  what keeps the GC interval off the log's growth rate.
- **Edge writes append nothing at all.** `EdgesAdd`, `EdgesDeleteFinalizingDependsOn`
  and the `ON DELETE CASCADE` from a collected row change no object row and draw
  no `resource_version`. This is an object write log. A watcher that needs to see
  an edge change sees it when the object write that accompanies it lands — the
  `reconcile_owed` stamp on a new edge, for instance.

Because `gcCollect` wraps its whole body in one `Within`, the child marks and the
parent's `op = 3` commit together and can reach the tail in one batch. They are
distinct entries with distinct versions, so coalescing and ordering handle them
normally.

The log is a wake-up list, not a source of truth. A create or update entry
carries no payload. The consumer routes by `object_id` and reads current state,
as the dependency waker does today.

Delete entries are the exception, and they carry more than "the blob". The row
is gone, so nothing can be read back — and `ObjectChange` promises that a
`Deleted` change carries the row's final state as a full `*Object`
(`client.go:81`). `rawToTyped` (`client.go:886`) builds that from a whole
`RawObject`: `Name`, `Generation`, `ObservedGeneration`, `ObservedAt`,
`DeletionRequestedAt`, `Finalizers`, `CreatedAt`, `UpdatedAt` and `Conditions`,
not only `Spec` and `Status`. A delete entry therefore stores a **row image** —
the serialized `RawObject` — and the decode path unmarshals it and hands it to
`rawToTyped` unchanged.

Three of those fields are knowable without storing them, and are stored anyway
so the image needs no reconstruction rules: `ResourceVersion` is the entry's own,
`Finalizers` is empty by definition (collection requires it), and
`DeletionRequestedAt` is non-nil by definition. `Conditions` is the field that
forces the decision: it lives in its own table under `ON DELETE CASCADE`
(`0001_init.sql:92`), so it is destroyed at the moment of collection and exists
afterwards only if this entry captured it.

**Ordering invariant.** The tail requires log entries to become visible in
`resource_version` order. A reader that sees version N must never later see an
unread entry below N, or the cursor steps over it permanently. This holds in the
SQLite store because the pool is size 1 (`sqlite/sqlite.go:36,47`), so writers
serialize and a version is assigned and committed under the same lock. `Store`
is a public extension point, so this is a documented requirement on
implementations, not an incidental property: a backend that assigns versions
outside the commit lock, or commits concurrently, will leave permanent cursor
gaps. Such a backend must serialize version assignment with commit, or not
implement the log.

The schema is amended in place. `sqlite/migrations/` holds one file until the
first release, so this is an edit to `0001_init.sql`.
See [the amend-in-place ADR](../adr/2026-07-31-amend-the-schema-in-place-until-release.md).

### Store API

```go
// ObjectWrite is one log entry.
type ObjectWrite struct {
    ResourceVersion int64
    ID              ObjectID
    Group           string
    Kind            string
    Op              WriteOp // WriteCreate, WriteUpdate, WriteDelete
    WrittenAt       int64
    // Final is the object as it was when collected, set on a WriteDelete entry
    // and nil otherwise. It is a whole row image because that is what a Deleted
    // change reports; the tail passes it to rawToTyped without reading a row.
    Final *RawObject
}
```

Two read families exist, because two consumers need different scopes.

**Kind-scoped, for the watches:**

```go
// ObjectWritesListSince returns gk's log entries above afterRV, in
// resource_version order, at most limit. trimmedThrough is gk's retention
// horizon read in the same transaction as the page: afterRV < trimmedThrough
// means entries were trimmed unread and the caller must resync. Equality is
// fine — the next unread entry is trimmedThrough + 1. Create and update entries
// carry no blob; delete entries carry the object's final state.
ObjectWritesListSince(ctx context.Context, gk GroupKind, afterRV int64, limit int) (page []ObjectWrite, trimmedThrough int64, err error)

// ObjectWritesMaxVersion returns gk's log position: max(highest
// resource_version in gk's log, gk's trimmed_through). It only rises, so
// consumers gate on > cursor.
//
// The max is not cosmetic. Retention lowers the raw log max, so a quiet kind
// whose log fully aged out would report 0 against a tail parked at 100 and
// force a listing on every tick — on the kind that writes least, which is the
// cost this design exists to remove. Folding in trimmed_through keeps the
// position where the tail left it.
ObjectWritesMaxVersion(ctx context.Context, gk GroupKind) (int64, error)

// ObjectWritesSnapshot returns every object of kind gk and the log position the
// listing is complete as of, the same value ObjectWritesMaxVersion reports.
// Both reads run in one transaction, so no write falls between them.
ObjectWritesSnapshot(ctx context.Context, gk GroupKind) ([]*RawObject, int64, error)

// ObjectWritesSnapshotByID is ObjectWritesSnapshot for one object: the row, or
// no rows when id does not exist or belongs to another kind, and gk's log
// position. It reads one row rather than the kind, but reports the KIND's
// position, because the stream that follows tails the kind's log.
ObjectWritesSnapshotByID(ctx context.Context, gk GroupKind, id ObjectID) ([]*RawObject, int64, error)
```

There is no `ObjectWritesResumable`. An earlier draft had one, and
`trimmedThrough` on every page makes it redundant: `WithResumeFrom` learns the
horizon from its own first page, in one round trip instead of two, with no window
between the check and the read in which a sweep can run.

**Store-wide, for the dependency waker.** The waker scans every kind, because a
`depends_on` edge can point at a kind with no controller
(`waker.go:102`, `waker.go:168` call these forms today and keep calling them):

```go
// ObjectWritesListSinceAll is ObjectWritesListSince across every kind.
ObjectWritesListSinceAll(ctx context.Context, afterRV int64, limit int) (page []ObjectWrite, err error)

// ObjectWritesMaxVersionAll is ObjectWritesMaxVersion across every kind.
ObjectWritesMaxVersionAll(ctx context.Context) (int64, error)
```

The store-wide reads return no horizon. The waker's cursor is an optimisation
over the stale-dependents pass, which guarantees the wake regardless
([ADR](../adr/2026-07-30-durable-waker-cursor.md)), so a trimmed entry costs
latency, not correctness. That is the difference between the two families, and
it is why only one of them reports a horizon.

**Batched state read, for the tail:**

```go
// ObjectsListByIDs returns the objects of kind gk whose ids are in ids, in id
// order — creation order, since objects.id is AUTOINCREMENT, NOT the order the
// caller asked for and not resource_version order. Ids that name no object, or
// an object of another kind, are absent: a short result is normal, not an error.
ObjectsListByIDs(ctx context.Context, gk GroupKind, ids []ObjectID) ([]*RawObject, error)
```

Without this the tail issues one `ObjectsGet` per changed object. The pool is
size 1, so those are serialized round trips, and a churny kind would be slower
than today's single `ObjectsList`. This read is what keeps the design a win
rather than a regression.

**The sweep:**

```go
// ObjectWritesSweep trims the log to the retention bounds and returns how many
// entries it deleted. perKind > 0 caps each (group, kind) log to its newest
// perKind entries; maxAge > 0 drops entries written more than maxAge ago. A zero
// bound is skipped. It raises each affected kind's trimmed_through in the same
// transaction that deletes that kind's entries.
ObjectWritesSweep(ctx context.Context, perKind int, maxAge time.Duration) (int, error)
```

The horizon is per kind, so the sweep cannot be one global statement. The
obvious `DELETE FROM object_writes WHERE written_at < ?` deletes across every
kind and learns nothing about which kinds it touched, which leaves
`object_writes_horizon` empty — and an empty horizon reads as "nothing was
trimmed", so every later resume succeeds against a holed log. That is a silent
correctness failure, not a missing optimisation. The sweep must fold the
per-kind maximum of what it removed: `DELETE ... RETURNING "group", kind,
resource_version` reduced per kind, or a loop over the kinds present in the log.
Either way the fold and the `trimmed_through` write share the delete's
transaction.

`idx_object_writes_age` serves the `maxAge` bound. The `perKind` bound is a
per-kind window over newest-first entries, which `idx_object_writes_kind` serves
as a leading prefix — `("group", kind, resource_version)` scanned backwards, with
`object_id` and `op` along for free.

### Client API

The snapshot leaves the stream:

```go
// ObjectsWatchList returns the current state of this client's kind and a stream
// of every change above it. Snapshot.ResourceVersion is the log position the
// snapshot is complete as of. The stream carries changes strictly above that
// position: no overlap, no gap.
ObjectsWatchList(ctx context.Context, opts ...WatchOption) (Snapshot[Spec, Status], <-chan ObjectChange[Spec, Status], error)

// ObjectsWatch is ObjectsWatchList restricted to one object. The snapshot holds
// that object alone, or no objects when the id does not exist or belongs to
// another kind, and it costs a one-row read (ObjectWritesSnapshotByID) rather
// than a listing of the kind. Snapshot.ResourceVersion is still the KIND's log
// position, because the stream tails the kind's log. Takes the same
// WatchOptions.
ObjectsWatch(ctx context.Context, id ObjectID, opts ...WatchOption) (Snapshot[Spec, Status], <-chan ObjectChange[Spec, Status], error)

type Snapshot[Spec, Status any] struct {
    Objects         []*Object[Spec, Status]
    ResourceVersion int64
}
```

A subscriber holds the initial state before it reads the first change. "Am I
synced?" becomes a value, not a guess. The subscribe-then-act guarantee is kept
and is now explicit: the snapshot read runs on the caller's goroutine and its
failure is returned, so a write the caller makes after the call returns is
always in the stream.

`ObjectsWatch(id)` tails the kind's log and drops the entries for other ids,
because the log carries no index under `object_id`. A single-object watch
therefore costs what its kind writes, not what the object writes. That is
acceptable while the tail is covering and blob-free; it is the first thing to
measure if single-object watches become common.

A registered controller is no longer required by either watch. The path needs no
reconciler, so client-only kinds get watches. `SchedulesWatch` keeps its
`ErrNoController`, because a schedule is a reconciler's state.

#### Terminal errors on the stream

A watch can fail after it is established, and the channel must be able to say so.
`ObjectChange` gains a terminal form:

```go
// ChangeType values: Added, Modified, Deleted, Failed.

// ObjectChange reports a change to a watched object. On a Deleted change,
// Object carries the row's final state. On a Failed change, Object is nil and
// Err is non-nil: the stream is over, and a Failed change is always the last
// value before the channel closes.
type ObjectChange[Spec, Status any] struct {
    Type   ChangeType
    Object *Object[Spec, Status]
    Err    error
}
```

Two sentinels reach `Err`:

- `ErrWatchTooOld` — retention removed entries this stream had not read. The
  caller resubscribes without `WithResumeFrom` and resyncs from a new snapshot.
- `ErrWatchLagged` — the subscriber did not keep up and the lag policy is
  failing rather than blocking.

A channel that closes with no `Failed` change ended because the caller's context
ended. That is the one case a reader need not distinguish.

#### Options

`WatchOption` is a distinct option type, not the general `Option`: these are
meaningful only at a watch call, and dispatching them on `*Beehive` or a
controller would silently accept nonsense. `LoadOption`s are accepted alongside
them.

```go
// WithResumeFrom streams changes above rv instead of taking a snapshot.
// Snapshot.Objects is nil and Snapshot.ResourceVersion is rv. Returns
// ErrWatchTooOld when retention has already removed entries above rv.
func WithResumeFrom(rv int64) WatchOption

// WithLagPolicy sets what happens when the subscriber does not keep up.
// LagBlock (the default, and today's behaviour) blocks the tail until the
// subscriber reads. LagFail buffers depth changes and then ends the stream with
// ErrWatchLagged. depth is ignored under LagBlock and must be > 0 under LagFail.
//
// Under LagFail the channel is made with capacity depth+1. The policy fires
// when depth changes are outstanding, and the Failed change goes on that same
// channel — without the reserved slot the terminal send blocks on exactly the
// subscriber that stopped reading, which is what the policy exists to avoid.
// The tail sends the Failed change into the reserved slot and closes without
// blocking.
func WithLagPolicy(p LagPolicy, depth int) WatchOption
```

The `LoadOption`s that `List` accepts apply to the snapshot and to each
delivered batch, through the batched `loadListRelated` path, so a watch does not
become an N+1.

### Delivery

The stream stays a poll. Nothing store-backed is pushed.
See [the drivers ADR](../adr/2026-07-28-periodic-scan-drivers.md). A tick reads
`ObjectWritesMaxVersion(gk)` and lists the log only when that value is **greater
than** the tail's cursor. The gate is `>`, not `!=`, because the position folds
in `trimmed_through` and therefore only rises; the inequality test the current
watch uses is inherited from a live-rows maximum that could step back, and
carrying it over would list on every tick of a fully-trimmed quiet kind. The
cost of a tick is bounded by what changed, not by how many objects exist.

An in-process notify hub may wake a tick early when the writer is in this
process. It is a latency hint only, in the same class as `Client.Requeue`. A
lost signal costs one interval. Correctness rests on the tick.

**A trim under a live stream ends it.** A subscribe-time check cannot cover
this: retention can trim past a running stream's cursor between ticks. So the
gap check lives in the same read as the page. `ObjectWritesListSince` returns
`trimmedThrough` with every page, and the tail compares it against its own
cursor before using the page. `cursor < trimmedThrough` means entries were
trimmed unread, and the tail sends a `Failed` change carrying `ErrWatchTooOld`
and closes. Without this the tail reads an empty page and silently skips the
trimmed changes, which is the exact failure `ErrWatchTooOld` exists to prevent.

The test is strictly `<`. A cursor equal to `trimmedThrough` has lost nothing:
everything trimmed is at or below where the tail already read. This is not
pedantry about a boundary — it is the common case. On a kind that stops writing,
the whole log ages out and `trimmed_through` converges onto exactly the position
every live tail is parked at, so an `<=` test would end every established watcher
on every idle kind.

**Level-triggered delivery.** Within one batch the tail keeps the highest entry
per `object_id`, then reads current state for all of them in one
`ObjectsListByIDs`. A subscriber sees the current object, never a superseded one.
The change type comes from the coalesced entries: a batch containing a create
reports `Added`, a batch whose highest entry is a physical delete reports
`Deleted`, anything else reports `Modified` — including the soft delete, whose
object is still live with `DeletionRequestedAt` set.

**Intra-batch order is by each object's coalesced `resource_version`,** ascending.
`ObjectsListByIDs` returns rows in id order, which is creation order and unrelated
to when the changes happened, so the tail reorders after the read rather than
delivering what the store handed back. Cross-batch order is already the log's.

**An object that vanishes between the two reads is skipped.** The log read and
the state read are separate statements, so a delete can land between them and
`ObjectsListByIDs` returns short. The tail drops those ids and reports nothing
for them. It must not synthesise a `Deleted` and must not fail the tick: the
delete appended its own entry above this batch's cursor, so it arrives as a
`Deleted` change on a later tick, with the final body the log entry carries.
A short result from `ObjectsListByIDs` is therefore normal.

Two consequences follow, and both are contract:

- A subscriber can receive `Deleted` for an object it never saw, when a create
  and a delete land in one interval. It can receive `Modified` for an unknown
  object after a resume. Consumers must tolerate a change for an id they do not
  hold. This is what keeps stream memory at O(1).
- A create and a delete in one interval are still reported. Today they cancel
  and are invisible.

Deletes need no liveness probe. `ObjectsListIDs` leaves the watch path.

**`Deleted` means collected, not requested — and it may never arrive.** A
subscriber learns that an object is going away as a `Modified` change with
`DeletionRequestedAt` set, and learns it is gone as `Deleted` only once the GC
sweeper physically removes the row. The gap between the two is the GC interval
(`defaultGCInterval`, 30s) *plus* however long the object's finalizers take,
which is controller-defined and unbounded. Two cases have no upper bound at all:
a finalizer that never clears, and an object held by `EdgesHasIncoming` under
`ON DELETE RESTRICT` while a referrer survives. Both leave the object visible,
deletion-pending, and un-`Deleted` for as long as the condition holds.

This is today's behaviour, preserved deliberately. The current watch derives
`Deleted` from a row's absence from `ObjectsList`, and a row is absent only after
collection, so the log-based tail reports it at exactly the same moment. It is
also Kubernetes' semantics — `DELETE` sets a deletion timestamp, watchers see
`MODIFIED`, and `DELETED` follows collection — so callers arriving with that model
are not surprised.

The consequence for callers is worth stating plainly in `README.md`, because it
inverts the obvious reading: **a subscriber that wants to stop using an object
must key on `DeletionRequestedAt != nil`, not on the `Deleted` change.**
`Deleted` answers "is the row gone", which is the right question for cache
eviction and the wrong one for "should I still act on this".

### Retention

Retention matches the event log in mechanism and in wording. It does not match
it in default.

```go
// WithWriteLogRetention bounds the object write log, enforced globally by the
// GC sweeper. perKind > 0 caps each (group, kind) log to its newest perKind
// entries — per kind, so a hot kind cannot evict a quiet one; maxAge > 0 drops
// entries written more than maxAge ago. A zero bound is skipped. Meaningful
// only at New.
//
// The default is defaultWriteLogMaxAge (24h) and no count bound. Passing 0 for
// both disables retention and lets the log grow without limit.
func WithWriteLogRetention(perKind int, maxAge time.Duration) Option
```

`Beehive.writeLogRetentionSweep` runs in `gcSweeperRun`, beside
`eventRetentionSweep`, with the same shape: it returns early unless a bound is
set, and a failed sweep is logged and retried on the next tick. Freed pages
reach `freePagesSweep` as they do now.

**Why the default differs from the event log.** Events are written when a
controller chooses to write one. Log entries are written on every object write,
and a status write bumps `resource_version`, so this table grows at *reconcile
rate*, not at user-write rate. A converged object still writes when a controller
re-stamps status. Calling that "the same bargain as the event log" understates it
by roughly an order of magnitude, and an unbounded default would make a long-
running process accumulate a table nobody asked for. The default bounds age; a
deployment that wants long resume windows raises it deliberately.

**24h** is the proposed value, and it is a resume-window decision before it is a
storage one: it is what a subscriber may be disconnected for and still resume
without a full resync. A day covers a process restart, a deploy, and a night
under maintenance. Shorter risks resyncs on ordinary operational pauses; longer
buys little, because a subscriber down for more than a day is usually rebuilding
anyway. The number is worth revisiting once there is a measurement of entries
per object per day under a real controller.

Retention defines the resume horizon. The sweep raises `trimmed_through` for a
kind in the same transaction that deletes that kind's entries, so a resume is
never accepted against a log with a hole in it, and a crash between the two is
not possible.

A retention rule tied to the oldest live watcher was rejected. One stuck
consumer would grow the table without bound. `ErrWatchTooOld` exists so that a
consumer that cannot keep up is told, rather than served.

#### Why this is a second option and not one shared bound

Sharing the mechanism is right; sharing the setting is not. The two bounds
count different things and cost different things when they bite.

- **Different units.** `WithEventRetention`'s count is per `(object, category)`
  timeline. `WithWriteLogRetention`'s is per `(group, kind)`. One number cannot
  mean both, and a user who reads the second as the first will size it wrong by
  the object count of the kind.
- **Different losses.** Trimming events destroys history a user can read:
  `EventsList` will never return those runs again. Trimming the write log
  destroys nothing a user can read — the objects are untouched — and costs only
  the ability to resume cheaply. One is a data-retention policy; the other is a
  resume window.
- **Different reasons to change them.** "Keep a week of event history" and "let a
  subscriber be disconnected for an hour" are unrelated decisions. Coupling them
  forces the larger of the two on both tables.

So the shape, the sweep, the zero-is-skipped rule and the wording stay matched,
and the values stay independent. A combined `WithRetention(struct{...})` was
considered and rejected on the same grounds: it groups two settings that have no
reason to move together, and it would have to name its fields per-timeline and
per-kind anyway.

**One asymmetry falls out, and it is worth knowing.** `events.object_id` is
`ON DELETE CASCADE` (`0001_init.sql:210`), so an object's events die the moment
it is collected. `object_writes` deliberately has no foreign key, so the delete
entry — the one entry that carries the object's final row image — survives
collection for the whole retention window. A collected object therefore leaves a
full copy of itself in the write log after its history is gone, conditions
included, since those are captured in the image precisely because their own table
cascades away. That is the design working
as intended (the entry exists precisely to report a `Deleted` change with a body),
but it means write-log retention, not event retention, is what governs how long
a deleted object's bytes persist. That is a property of the system worth stating
in `README.md` beside the retention option, since nothing else in the API implies
it.

## Consequences

- Watch memory drops from O(objects in the kind) to O(1). The `seen` map and the
  tombstone bodies are removed.
- A quiet tick costs one indexed read of one kind's log. A write to another kind
  no longer forces a listing.
- A busy tick costs one covering index scan plus one `ObjectsListByIDs`, against
  today's one full `ObjectsList`. It is cheaper whenever fewer objects changed
  than the kind holds, and comparable when they all did.
- Storage grows with write volume, at reconcile rate. This is the price of the
  design, and retention is the control.
- The snapshot is still a full listing, with blobs, unpaged. `Snapshot` is where
  a continuation token goes when that becomes the limit. This spec does not add
  one, and the deferral is not free: a paged snapshot must hold one transaction
  across every page to stay consistent with its `ResourceVersion`, and on a
  pool-of-1 store that holds the only connection for the whole listing. The
  alternative is to page without a transaction and reconcile the pages against
  the log, which is a real design and belongs in its own spec.
- `watchpoll.go` loses roughly half its code: `deletedSince`, the `tracked`
  type, and the diffing in `poll`.
- `ObjectsWatchList` and `ObjectsWatch` change signature, and `ObjectChange`
  gains a field. Every caller and example changes with them. This is pre-release,
  so no compatibility shim is needed.
- The dependency waker gains delete entries, and they change nothing. An object
  with incoming `depends_on` edges cannot be physically removed at all:
  `edges.to_id` is `ON DELETE RESTRICT` (`0001_init.sql:127`). By the time a
  delete entry exists for an object, it has no dependents left to wake. The
  waker moves onto the real log for the other reasons in this spec, not for that
  one.

## Collateral updates

The change is not confined to the watch path. It also touches:

- `docs/reconcile-triggers.md` — the waker row cites `ObjectWritesMaxVersion`
  (line 190) and the trigger map must name the log as the source.
- `CLAUDE.md` — the watch bullet describes `ObjectWritesMaxVersion` as a
  "live-rows max" (line 71), which stops being true.
- `testutils_test.go` — `fakeStore` (line 265) needs the new methods, and
  `replayStore` (line 463) serves `ObjectWritesListSince` and
  `ObjectWritesMaxVersion` from fixed rows in the store-wide shape. Both change.
- `README.md` — it is the spec of the public API, and both watch signatures are
  in it. The watch section (line 508) also promises that deleting an object
  right after subscribing makes its `Deleted` guaranteed. That stays true in the
  sense it was written — the change is never *missed* — but it reads as a timing
  promise, and `Deleted` waits on collection, which finalizers and a surviving
  referrer can delay without bound. Worth separating the two claims while the
  section is being rewritten anyway.

  `WithWriteLogRetention` is user-facing, so it needs a line in the options
  reference block (line 655) beside `WithEventRetention` (line 562 covers the
  event bound in prose). The two options have the same shape and opposite
  defaults — events unbounded, the write log 24h — and listed adjacently that
  reads as an inconsistency. The reason belongs in the README, not only here:
  an event is written when a controller chooses to write one, while a log entry
  is written on every object write, so the log grows at reconcile rate whether
  or not the user opts in.
- The `examples/` that watch.
- `waker.go:147` — `resumeWatermark` clamps a stored cursor with
  `min(stored, mark)`, and its comment justifies the clamp by the mark stepping
  back "when the highest-versioned row is deleted". That stops being the reason:
  the log max falls only through retention now. The comment has to be rewritten
  either way. Whether the clamp itself survives is a judgement call — after a
  large trim it drags a restarting waker back to rescan a long slice of log,
  which is wasted work but not a fault, since wakes are idempotent and the
  stale-dependents pass is the guarantee.

## Open questions

None. Both are settled below.

## Rejected: reuse `driver_cursors` for the horizon

`driver_cursors(name, cursor, updated_at)` already stores an
`int64` per name, and `DriverCursorsSet`'s upsert is monotonic — it writes only
when the new value is greater, and dirties no page otherwise
(`sqlite/store.go:2000`). That is exactly the write pattern a horizon wants, so
the question is a fair one. It is still the wrong table, for four reasons, the
first of which is decisive.

**`DriverCursorer` is optional; the horizon cannot be.** It is deliberately not a
member of `Store` — a backend that does not implement it simply loses its scan
position, because a cursor is an optimisation over the stale-dependents pass and
never a guarantee
([ADR](../adr/2026-07-30-durable-waker-cursor.md)). The horizon has the opposite
character: an absent horizon reads as "nothing was trimmed", so every resume
succeeds against a holed log and subscribers silently miss changes. Storing it
behind an optional capability makes watch correctness depend on whether the
backend opted in.

**A cursor and a horizon are different kinds of fact.** A driver cursor is how far
*a reader* has got: private to one driver, reseedable from anywhere, costing only
latency when it is wrong. A horizon is what *the database no longer holds*: a
property of the data that every reader must respect, and that no reader may
advance. Putting them in one table invites a future `DriverCursorsSet` call to
move a horizon.

**The key does not fit.** `driver_cursors` is keyed by a single driver name; the
horizon is keyed by `(group, kind)`. Reuse means encoding the kind into the name
(`"object_writes_horizon/acme.com/Widget"`), which is a composite key smuggled
into a text column. That costs the sweep its set-based
`DELETE ... RETURNING "group", kind` fold — it would write one row per kind
through `DriverCursorsSet` instead — and turns "which kinds have a horizon" into
a `LIKE` prefix scan.

**The table documents a single-writer constraint** that belongs to drivers, not
to the sweep. Keeping the sweep out of it keeps that comment true.

The cost of the decision is one more table, holding one row per kind that has
ever been trimmed. That is the right price.

A last variant, also rejected: deriving the horizon as `MIN(resource_version) - 1`
over the kind's remaining entries and having no table at all. It works until a
kind's log is trimmed empty, at which point an empty result cannot distinguish
"never written" from "entirely trimmed" — the same fully-trimmed quiet kind that
forces `ObjectWritesMaxVersion` to fold `trimmed_through` in. The one case the
derivation cannot cover is the one that matters.

## Settled

**A delete entry stores a full row image.** `Deleted` keeps carrying a whole
`*Object`, exactly as `ObjectChange` promises today, and the `final` column holds
a serialized `RawObject` for the tail to hand to `rawToTyped`.

The alternatives were reducing `Deleted` to an `ObjectRef`, which keeps the log
uniformly payload-free but makes a deleted object's `Conditions` unrecoverable
for every subscriber at once, and storing `spec`/`status` alone, which pays most
of the storage and still breaks the contract silently in the metadata fields.
Neither is worth changing a published guarantee for. The image is bounded by
retention, and a delete is the one write that has nowhere else to put its state.

Two obligations follow from the choice:

- **The image must track `RawObject`.** A column added to `objects` and surfaced
  on `RawObject` has to reach the image, or a `Deleted` change starts reporting a
  zero value for it. Nothing about the write path fails when that is forgotten,
  so it needs a tripwire test in the repository's existing style — reflect over
  `RawObject`'s fields and fail on any the image does not round-trip. That test
  is part of the change, not a follow-up.
- **The image is the only copy.** `Conditions` cascade away with the object
  (`0001_init.sql:92`), so a delete entry that drops them destroys them. It is
  captured in the image for that reason, and the reason belongs in a comment
  where someone will find it before trimming the column.

**The retention key is `written_at`, indexed.** The alternative was a
`resource_version` recorded per sweep, which needs no timestamp column and no
second index. `written_at` wins on matching the event log's wording, which is
what makes the two retention options explainable as one idea; `idx_object_writes_age`
is what makes it affordable.

## Deferred: owner-scoped watches

An earlier draft denormalised `owner_id` into the log and added
`WithOwner(id ObjectID)`, to give `OwnedObjectsList` a watch. Both are out of
the first version.

An entry records the owner at write time, and ownership can change after that.
A filter on the logged value therefore selects on stale ownership: a re-parented
object keeps arriving on its old owner's stream and never appears on its new
one, until it is written again. Correcting that means confirming each entry
against current state, which is a read per entry and removes most of the benefit
the index was there to provide.

The feature is still wanted. It needs its own design, and the choice is between
filtering the tail against current ownership and treating an ownership change as
a write to the child. Neither belongs in the change that lands the log. A
subscriber that needs owner scoping today watches the kind and filters.

## Implementation order

1. The tables, the indexes, and the append inside every object write — including
   making `objectsDelete` draw a `resource_version`, and rewriting the comment
   at `sqlite/store.go:1717` that currently states the opposite as intent. The
   delete entry's row image lands here, with the `RawObject` round-trip tripwire
   beside it. Nothing reads the log yet.
2. `ObjectWritesSweep`, `object_writes_horizon`, `WithWriteLogRetention` and its
   default, and the sweeper hook.
3. The store reads: both scoped families, and `ObjectsListByIDs`. The waker moves
   onto the real log via the store-wide family.
4. `objectStream` rewritten as a log tail, with the gap check and the batched
   state read. `ObjectsWatchList` and `ObjectsWatch` split into snapshot and
   stream. `ObjectChange.Err` and the `Failed` type.
5. `WithResumeFrom`, `LoadOption`s, and `WithLagPolicy`.
