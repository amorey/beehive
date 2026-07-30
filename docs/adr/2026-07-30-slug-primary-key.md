# The slug is the `Client` API's key; the id is the store's key

- **Status:** Accepted — implemented in `client.go`, `options.go`, `types.go`,
  `internal/storeapi/storeapi.go`, `sqlite/store.go`,
  `sqlite/migrations/0001_init.sql`.
- **Date:** 2026-07-30

## Context

Every object could be named, but nothing required it. `Create` took the slug through
`WithSlug`, an options bag, so "no slug" was representable and the column was
nullable. Everything after the create took an `ObjectID`: `Get`, `Update`, `Delete`.
A caller who names things — which the target application does everywhere — had to
hold an id to do ordinary work, and the slug was reachable only through the two
`…BySlug` methods that had been added where the ergonomics hurt most.

The README defended this with a real property: *"Everything after the create takes an
`ObjectID`, so a delete and recreate under the same slug can't make you act on the
wrong row."* Slugs are reusable by design — a rename is delete+recreate — so a
slug-keyed API can hand a caller a different incarnation than the one it read. That
is a genuine hazard, and any change here has to answer it rather than ignore it.

## Decision

Key the user-facing CRUD surface on the slug, and make the slug required.

- `Create` takes the slug positionally; `WithSlug` is deleted. A required field
  passed through an options bag is the wrong shape — the bag can express an absence
  the argument cannot, which is exactly how a NULL slug got written. Deleting it also
  deletes `GetOrCreate`'s `ErrConflictingOption` branch, which existed only to defend
  against the option contradicting the positional argument, and `ErrConflictingOption`
  itself, which then had no producer.
- `GetBySlug`/`DeleteBySlug` lose the suffix; the id-keyed forms they displace gain
  `ByID`. `Update` gains a slug form and an `UpdateByID` sibling — it had no `BySlug`
  today, and leaving it out would split the CRUD set, letting a caller create, read
  and delete by name but forcing an id to write.
- `slug TEXT NOT NULL`, and `Slug` is a `string` on `Object`, `RawObject` and
  `ObjectsCreateInput`.
- `""` is rejected with `ErrInvalidSlug`, by the writes and the reads alike.

### The ABA contract, stated rather than inferred

> A **slug-keyed** call acts on whatever holds that slug *now*, or reports absence.
> An **id-keyed** call acts on that one incarnation, or returns `ErrNotFound`.

This is not a weakening of the old promise; it is the level-triggered principle
applied to the API surface. "Ensure this child exists" / "remove this child" is a
statement about a *name*, and should re-evaluate against current state on every
reconcile — which is what a slug-keyed call does, and what made `GetOrCreate` and
the slug-keyed delete worth adding in the first place. "Finish what I read a moment
ago" is a statement about an *incarnation*, and that is what an id is for.

Two things make the split safe:

1. **Every slug-keyed write resolves and writes in one transaction.** Never two store
   calls. The residual hazard the store cannot close is a caller's own
   `Get` → mutate → `Update` sequence, so the rule is that **read-modify-write goes
   through `UpdateByID`** — the object `Get` returned carries `ID`. The window is
   narrow anyway: a tombstone holds the slug's `UNIQUE` constraint until GC clears
   finalizers, so opening it takes a full collect *plus* a new create.
2. **The rename cannot fail silently.** `ObjectID` is an alias for `int64` and a slug
   is a `string`, so every un-updated call site is a compile error. This is a property
   of the *types*, not of the design, and it would stop holding if `ObjectID` ever
   became a string-backed type — which is the reason to write it down.

### What is not re-keyed

Nothing below `Client`. Every foreign key stays `INTEGER REFERENCES objects(id)`,
for three independent reasons:

- **ABA safety.** `objects.id` is `AUTOINCREMENT` precisely so ids are never reused,
  which is what makes stale edge targets impossible by construction. Slug-keyed edges
  would let a recreate re-adopt the previous incarnation's edges — and worse, the
  surviving `dependency_watermarks` row was measured against the *old* incarnation's
  cursor, so the stale-dependents pass would read converged for a dependency never
  reconciled against. That is permanent divergence, the exact failure the
  [watermarks ADR](2026-07-29-dependency-watermarks.md) exists to exclude.
- **Key width in the scan-hot tables.** An id is a varint, typically 1–3 bytes; a
  slug key is `(group, kind, slug)`, tens of bytes, on every row of `edges` (twice),
  `conditions`, `events` and `dependency_watermarks`. `edges` is `WITHOUT ROWID`
  because the key *is* the table, and `idx_edges_to` is covering only because it
  carries that key. These are the tables the 1s waker and the 60s stale-dependents
  pass scan.
- **Two deliberate storage tricks.** `dependency_watermarks.object_id INTEGER PRIMARY
  KEY` aliases the rowid; `idx_objects_deleting` is chosen to plan as a plain index
  scan with no row fetch and no sort. A text key forfeits both.

`storeapi.Store` also keeps `ObjectsGetBySlug` / `DeletionRequestsCreateBySlug` /
`ObjectsUpdateSpecBySlug` under those names. There the id really is the key and the
slug methods really are the qualified variant; renaming would invert the convention
against the thing being named.

### Why `ObjectsUpdateSpecBySlug` reads before it writes

The obvious rule — "put the slug in the `WHERE` clause, never resolve then write" —
generalises `DeletionRequestsCreateBySlug` one step too far. A spec write **must**
read the row first: the generation handshake's no-op skip compares stored bytes and
schema version against the incoming ones, and that skip is what stops a controller
re-applying its own spec from waking itself forever. A bare
`UPDATE … WHERE slug = ?` has nothing to compare against.

Atomicity here comes from `Within`, not from statement count. The store runs every
caller through one connection under `BEGIN IMMEDIATE`, so a read and a write in one
transaction are as indivisible as one statement — which `ObjectsUpdateSpec` already
relied on, and `GetOrCreate` before it. So the two spec mutators share one body and
differ only in how they resolve the row, the same shape `requestDeletion` uses for
the delete pair.

A predicate-taking `ObjectsUpdateSpec` was rejected outright: `markForDeletion`'s
`where string` is an unexported `sqliteStore` helper, while `ObjectsUpdateSpec` is on
the public `storeapi.Store` that external, backend-agnostic implementations satisfy.

### The empty slug

`""` is the one rule beehive enforces on an otherwise opaque key. Making the slug
required removes the old spelling of "no name", which leaves `""` as what an unset
configuration field reads as — and under an unvalidated contract every caller whose
config was unset would silently converge on one shared row. Every other malformed
slug at least addresses the row its author meant.

A new sentinel rather than `ErrInvalidOption`, whose godoc scopes it to option
values: the slug is a positional argument on all four writes. The reads reject it
too, because `ErrNotFound` would send the caller hunting for a missing row when what
is missing is a config value, and the slug-keyed delete folds absence to `nil`, where
a silent `nil` is indistinguishable from success.

**`ErrInvalidSlug` lives in `storeapi`, and the store is what enforces it.** A
client-side check alone would make the invariant true of one caller rather than of
the data: `Store` is a public extension point, and a row admitted under `""` is one
no slug-keyed call can address again. `Client` still rejects it up front — that is
what keeps the reads from answering `ErrNotFound` for a bad argument — but the
guarantee is the store's.

`ObjectsCreate` refuses it in Go, *before* the version draw, rather than leaning on
the column's `CHECK (slug <> '')`. A constraint violation arrives as a raw driver
error carrying no sentinel to match on, and its text belongs to the driver; the
contract promises `ErrInvalidSlug`, so the promise is kept where it can be. The
`CHECK` remains as the backstop for writes that never pass through the store — a
migration, or a foreign writer — which is the only case a Go guard cannot cover.

## Consequences

- **The public API breaks for every consumer.** Taken all at once, which is why
  `Update` is pulled in rather than left for a second break.
- **The schema is amended, not migrated.** `0001_init.sql` gains `NOT NULL` in place.
  Safe exactly once — the project is undeployed, and the migrator stores no checksum,
  so it cannot detect an amended migration. Anyone holding a database from an earlier
  checkout must delete it. See [ADR](2026-07-29-auto-vacuum-incremental.md) for the
  related reason a rebuild was not an option: this migrator runs every migration
  inside `BeginTx`, where `PRAGMA foreign_keys=OFF` is silently ignored.
- **Performance is a wash, not a win.** `Get(ctx, slug)` is an index probe on the
  existing `UNIQUE ("group", kind, slug)` plus a row fetch — two b-tree descents
  against one for an id. Nothing here makes lookups faster; the win is ergonomic. Do
  not let this change grow a schema re-keying in pursuit of one.
- **The idempotent delete paths got cheaper**, independently: both no-op outcomes now
  answer from a lock-free probe instead of opening a write transaction. That was a
  deferred TODO whose gate — "revisit if absent-path deletes are hot, or if
  `DeletionRequestsCreate` gets the same treatment" — this change satisfied by
  routing all delete traffic through the slug path and touching both keyings anyway.
- **An in-memory slug→id memo is now safe** but deliberately not taken: the mapping
  is immutable per incarnation, so it could be cached off the write log the waker
  already scans. Gated on a profile — these lookups are already index-backed, and an
  unmeasured cache is a coherence bug waiting for a delete/recreate.
