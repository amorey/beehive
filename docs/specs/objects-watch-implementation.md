# Objects watch: implementation plan

- **Status:** Proposed. Nothing here is built.
- **Date:** 2026-08-02
- **Implements:** [objects-watch.md](objects-watch.md)

## How to work through this

Each cycle is one red/green pair and one commit.

1. **Red.** Write the test named in the cycle. Run it. Confirm it fails, and
   confirm it fails for the stated reason — a test that fails because a method
   does not exist has not tested anything yet.
2. **Green.** Make it pass with the smallest change that is honest. No
   speculative structure for a later cycle.
3. **Check.** `gofmt`, `go vet ./...`, `go test ./...`. The whole suite is green
   at the end of every cycle, not only the new test.
4. **Commit.** The message is given per cycle. Amend it if the work turned out
   different; the message describes what landed.

Rules that hold for every cycle:

- Tests are whitebox (`package beehive`, `package sqlite`) and mirror source
  files. Shared fakes go in `testutils_test.go`.
- `require` for preconditions, `assert` for independent checks. Synchronize on
  signals, never on sleeps.
- A cycle that grows past one production file plus its test is a cycle that
  wanted splitting. Split it.
- Two guardrails must stay green throughout: `TestTheSchemaIsOneMigration` (the
  schema is amended in place, never a second migration) and the goroutine-leak
  check.

At the end of the last cycle, run `/simplify` over the whole diff.

---

## Phase 1 — the log records writes

Nothing reads the log in this phase. It is built, filled, and verified in
isolation.

### C1 — the table exists and a create is recorded

**Red.** `sqlite/store_test.go`: `TestObjectsCreateAppendsAWriteLogEntry`.
Create one object, read `object_writes` directly through a
`writeLogEntries(t, store)` helper, and assert exactly one entry whose
`resource_version` equals the created row's, `op` is create, `group`/`kind`
match, and `final` is NULL. Fails because the table does not exist.

**Green.** Add `object_writes` and its two indexes to `0001_init.sql`. Append
in `objectsCreate`.

**Commit.** `feat(sqlite): record object creates in a durable write log`

### C2 — every version-bumping mutator is recorded

**Red.** `sqlite/store_test.go`: `TestObjectWritesRecordEveryVersionBump`,
table-driven over the mutators that assign a `resource_version` —
`ObjectsUpdateSpec*`, `UpdateStatus`, `markForDeletion`, `FinalizersDelete`,
and the `reconcile_owed` stamp. Each case asserts one update entry at the row's
new version. A second case asserts a byte-identical spec write appends nothing,
because a content no-op is not a write.

**Green.** Append in each mutator. Prefer one small helper over a copied INSERT.

**Commit.** `feat(sqlite): record every object write in the log`

### C3 — collection draws a resource_version

**Red.** `sqlite/store_test.go`: `TestObjectsDeleteDrawsAResourceVersion`.
Collect an object, assert `resource_version_seq` advanced. Fails because
`objectsDelete` deliberately does not draw one today.

**Green.** Draw a version in `objectsDelete`. Rewrite the comment at
`sqlite/store.go:1717`, which currently states the opposite as intent. Expect
fallout in tests that assert the counter is still: fix them here, in this cycle,
where the reason is visible.

**Commit.** `feat(sqlite)!: draw a resource_version when an object is collected`

### C4 — collection is recorded with a row image

**Red.** Two tests in `sqlite/store_test.go`:
`TestObjectsDeleteAppendsARowImage` — collect an object that has conditions,
finalizers cleared, and a status; assert the delete entry's `final` decodes to a
`RawObject` equal to the row as it was, conditions included.
`TestWriteLogImageCoversRawObject` — the tripwire: reflect over `RawObject`'s
fields and fail on any field the image does not round-trip. It is what stops a
new column silently reporting a zero value on a `Deleted` change.

**Green.** Add the `final` column. Serialize the row image in `objectsDelete`,
reading conditions before the cascade removes them.

**Commit.** `feat(sqlite): carry a row image on the log's delete entries`

---

## Phase 2 — reading and trimming

### C5 — the log is readable by kind

**Red.** `sqlite/store_test.go`: `TestObjectWritesListSinceScopesToKind` and
`TestObjectWritesMaxVersionScopesToKind`. Write to two kinds; assert each reads
only its own entries, in `resource_version` order, honouring `limit`, and that a
kind with no entries reads 0. `trimmedThrough` comes back 0 — nothing has been
trimmed yet.

**Green.** `ObjectWritesListSince(ctx, gk, afterRV, limit)` returning
`(page, trimmedThrough, err)`, and `ObjectWritesMaxVersion(ctx, gk)`. Both on
`storeapi.Store`. The store-wide forms the waker uses stay as they are.

**Commit.** `feat(sqlite): read the write log per kind`

### C6 — the sweep trims and records what it removed

**Red.** `sqlite/store_test.go`: `TestObjectWritesSweepRecordsThePerKindHorizon`.
Two kinds with entries at known versions; sweep with an age bound that removes
some of each; assert the entries are gone AND that each kind's
`trimmed_through` equals the highest version removed *for that kind*. The second
assertion is the one that fails on the obvious global `DELETE`.

**Green.** Add `object_writes_horizon` and `idx_object_writes_age`. Implement
`ObjectWritesSweep` with the per-kind fold, deleting and raising the horizon in
one transaction.

**Commit.** `feat(sqlite): trim the write log and record its per-kind horizon`

### C7 — a trimmed kind keeps its log position

**Red.** `sqlite/store_test.go`: `TestObjectWritesMaxVersionHoldsTheHorizon`.
Write to a kind, trim its log empty, assert `ObjectWritesMaxVersion` still
reports the last version rather than 0. Fails while the read is a bare `MAX`.

**Green.** Fold `trimmed_through` in: the position is
`max(log max, trimmed_through)`.

**Commit.** `fix(sqlite): hold a fully trimmed kind's log position`

### C8 — retention is configurable and swept

**Red.** `options_test.go`: `TestWithWriteLogRetentionBoundsTheLog` — a Beehive
with a short bound trims on a GC tick. `beehive_test.go`:
`TestWriteLogRetentionDefaultsToADayOfHistory` — the default is non-zero, unlike
the event log's.

**Green.** `WithWriteLogRetention(perKind, maxAge)`, the two fields on
`Beehive`, `defaultWriteLogMaxAge = 24h`, and `writeLogRetentionSweep` in
`gcSweeperRun` beside `eventRetentionSweep`.

**Commit.** `feat: bound the write log from the GC sweeper`

### C9 — a snapshot carries its log position

**Red.** `sqlite/store_test.go`: `TestObjectWritesSnapshotIsConsistent` — the
returned position equals the kind's position at the moment of the listing, and a
write made after the call is above it. Plus
`TestObjectWritesSnapshotByIDReadsOneRow` — one row, but the kind's position.

**Green.** `ObjectWritesSnapshot` and `ObjectWritesSnapshotByID`, both reading
rows and position in one transaction.

**Commit.** `feat(sqlite): snapshot a kind with its log position`

### C10 — objects are readable in one batched query

**Red.** `sqlite/store_test.go`: `TestObjectsListByIDsIsKindScoped` — ids of two
kinds in, only this kind's rows out; a missing id is absent rather than an
error.

**Green.** `ObjectsListByIDs`.

**Commit.** `feat(sqlite): read a batch of objects by id`

---

## Phase 3 — the consumers

### C11 — the waker reads the real log

**Red.** `waker_test.go`: `TestWakerScansTheDurableLog` — an existing waker test
retargeted at the log, plus a case asserting the waker still wakes from a page
that contains a delete entry rather than skipping it.

**Green.** Rename the store-wide reads to `ObjectWritesListSinceAll` /
`ObjectWritesMaxVersionAll` and point them at `object_writes`. Update
`replayStore` in `testutils_test.go`. Reword `resumeWatermark`'s comment: the
mark now steps back through retention, not through a deleted high row.

**Commit.** `refactor(waker): scan the durable write log`

### C12 — the snapshot leaves the stream

**Red.** `watchpoll_test.go`: `TestObjectsWatchListReturnsASnapshot` — the
snapshot holds current state and its `ResourceVersion`, and the channel carries
only changes made afterwards.

**Green.** Change both watch signatures to return `(Snapshot, <-chan
ObjectChange, error)`, taking the snapshot from `ObjectWritesSnapshot`. The
stream is still the existing diff engine, seeded from the snapshot. Public API
change: `README.md` and the `examples/` compile again in this cycle.

**Commit.** `feat!: split the watch snapshot out of its change stream`

### C13 — the stream tails the log

**Red.** `watchpoll_test.go`: `TestObjectStreamTailsTheWriteLog` — a quiet tick
reads the position and does not list; a tick with one changed object reads that
object alone.

**Green.** Replace the listing in `poll` with a log tail plus
`ObjectsListByIDs`. Deletes still come from the `seen` map — that goes in C14.

**Commit.** `perf(watch): tail the write log instead of listing the kind`

### C14 — deletes come from the log

**Red.** `watchpoll_test.go`: `TestDeletedChangeComesFromTheLogImage` — collect
an object and assert the `Deleted` change carries its full final state,
conditions included, with no liveness read having run.

**Green.** Build `Deleted` from the entry's row image. Delete `deletedSince`,
the `tracked` type, and the `seen` map. `ObjectsListIDs` leaves the watch path.

**Commit.** `perf(watch): report deletes from the log's row image`

### C15 — a batch is coalesced and ordered

**Red.** `watchpoll_test.go`: `TestBatchCoalescesToCurrentState` — three writes
to one object inside one interval yield one change carrying current state; and
`TestBatchOrdersByResourceVersion` — two objects changed in a known order arrive
in that order, not in id order.

**Green.** Coalesce by `object_id`, keep the highest entry, reorder after
`ObjectsListByIDs`. Skip an id that `ObjectsListByIDs` does not return: its
delete is a later entry.

**Commit.** `fix(watch): coalesce a batch to current state in write order`

### C16 — a lost stream says so

**Red.** `watchpoll_test.go`: `TestTrimUnderALiveStreamEndsIt` — trim past a
parked stream's cursor and assert a final `Failed` change carrying
`ErrWatchTooOld`, then a closed channel. And
`TestAQuietKindIsNotTornDownByATrim` — the boundary: a cursor exactly at
`trimmed_through` keeps streaming. That second test is the one that catches the
off-by-one.

**Green.** `ObjectChange.Err`, the `Failed` change type, `ErrWatchTooOld`, and
the `cursor < trimmedThrough` check against every page.

**Commit.** `fix(watch): end a stream whose log entries were trimmed`

### C17 — a stream resumes

**Red.** `watchpoll_test.go`: `TestWithResumeFromSkipsTheSnapshot` — resume above
a version and receive only later changes, with no listing; and
`TestResumeBelowTheHorizonIsRefused` — `ErrWatchTooOld` from the call itself.

**Green.** `WatchOption`, `WithResumeFrom`.

**Commit.** `feat(watch): resume a stream from a resource version`

### C18 — watched objects can carry relations

**Red.** `watchpoll_test.go`: `TestWatchAppliesLoadOptions` — a watch with
`LoadOwner()` delivers objects whose accessor returns the owner instead of
`ErrNotLoaded`, with one relation query per batch rather than one per object.

**Green.** Accept `LoadOption`s and route each batch through
`loadListRelated`.

**Commit.** `feat(watch): apply LoadOptions to snapshots and batches`

### C19 — a lagging subscriber can be failed

**Red.** `watchpoll_test.go`: `TestLagFailEndsAStalledSubscriber` — a subscriber
that stops reading gets `ErrWatchLagged` and a closed channel, and the tail does
not block. The reserved `depth+1` slot is what makes the terminal send land.

**Green.** `LagPolicy`, `WithLagPolicy`, the buffered channel.

**Commit.** `feat(watch): fail a lagging subscriber instead of blocking`

### C20 — client-only kinds can be watched

**Red.** `watchpoll_test.go`: `TestWatchNeedsNoController` — a kind with no
registered controller watches normally.

**Green.** Drop the `isRegistered` gate from both watches. `SchedulesWatch`
keeps its `ErrNoController`.

**Commit.** `feat(watch): watch kinds that have no controller`

---

## Phase 4 — the record

### C21 — the docs match the code

No test. Update `README.md` (both signatures, the `Deleted`-means-collected
rule, `WithWriteLogRetention` in the options block with its default explained
against `WithEventRetention`), `CLAUDE.md` (the live-rows-max bullet),
`docs/reconcile-triggers.md` (the waker's source).

**Commit.** `docs: describe the write log and the split watch snapshot`

### C22 — the spec becomes a record

Fold what still governs live code into `docs/adr/<date>-object-write-log.md`,
delete both files in `docs/specs`, and update the ADR index. A directory of live
records beats a directory of archaeology.

**Commit.** `docs(adr): record the object write log decision`

### C23 — simplify

Run `/simplify` over the branch diff. Expect it to find duplicated append calls
from C2, and the seams left where the diff engine was cut out in C13–C15.

**Commit.** `refactor: simplify the write log and watch tail`

---

## Sequencing notes

- **Phases 1 and 2 are independently useful.** The log is durable and swept
  before anything reads it, so a bad tail can be reverted without touching the
  schema.
- **C3 is the one behaviour change with no new API.** It moves the shared
  counter on collection, which events also draw from. Land it alone.
- **C12 is the API break.** Everything before it is additive. Do not bundle it.
- **C13–C15 are one idea in three cycles.** The suite is green after each, but
  the design only reads correctly once C15 lands.
