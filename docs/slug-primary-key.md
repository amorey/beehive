# The slug as the `Client` API's primary key

- **Status:** Accepted — ready to implement. All four gating decisions are settled
  (see Decisions and Readiness); nothing is blocked.
- **Date:** 2026-07-30 (re-verified against `bd77d7d`)
- **Replaces:** one deferred `docs/TODO.md` item (`DeleteBySlug` on an absent slug
  still opens a write transaction, `docs/TODO.md:68-95`).
- **Builds on:** the storeapi write contract, **landed** in `3d9b36a` —
  `ObjectsCreateInput` is the struct Change 1 narrows, and it exists today
  (`internal/storeapi/storeapi.go:245-254`). The prerequisite is satisfied; this spec
  is unblocked.

## Goal

Make every object slug-addressable and key the user-facing CRUD surface on the slug,
under one principle:

> **The slug is the API's key; the id is the store's key.**

A caller who names things — which the target application does everywhere — should
never hold an `ObjectID` to get ordinary work done. The `id` keeps every job it does
today (incarnation identity, foreign-key target, work-queue key, scan ordering) and
stays reachable through `…ByID` siblings for the cases that need an incarnation
rather than a name.

## Non-goals

- **No re-keying of the store.** Every foreign key stays `INTEGER REFERENCES
  objects(id)`. Three separate reasons, all load-bearing:
  - **ABA safety.** `objects.id` is `AUTOINCREMENT` specifically so ids are never
    reused, which is what makes stale edge targets *"impossible by construction. No
    to_uid soft guard or re-adoption machinery needed"*
    (`sqlite/migrations/0001_init.sql:12-15,148-157`). Slugs are reusable **by
    design** — a rename is delete+recreate — so slug-keyed edges would let a recreate
    silently re-adopt the old incarnation's edges. Worse, the surviving
    `dependency_watermarks` row was measured against the *old* incarnation's cursor,
    so the stale-dependents pass would read converged for a dependency never
    reconciled against: permanent divergence, the exact failure the
    [watermarks ADR](adr/2026-07-29-dependency-watermarks.md) exists to
    exclude.
  - **Key width in the scan-hot tables.** An id is a varint, typically 1–3 bytes. A
    slug key is `(group, kind, slug)` — tens of bytes — and it would land on every row
    of `edges` (twice), `conditions`, `events` and `dependency_watermarks`. `edges` is
    `WITHOUT ROWID` precisely because *"the key **is** the table"*
    (`0001_init.sql:161-166`), and `idx_edges_to` is covering only because it carries
    that same primary key (`:172-178`). These are the tables the dependency waker
    (1s), the stale-dependents pass (60s) and cascade GC scan — fewer keys per page
    means deeper trees and more page reads on the loops that define steady-state cost.
  - **Two deliberate storage tricks.** `dependency_watermarks.object_id INTEGER
    PRIMARY KEY` *aliases the rowid*, so the table already is its own b-tree
    (`0001_init.sql:202-205`); and `idx_objects_deleting ON objects(id, "group",
    kind)` is chosen because it is *"already in id order, so it plans as a plain SCAN
    ... USING INDEX with no row fetch and no sort"* (`:80-94`). A text key forfeits
    both.
- **No change to `ControllerClient`.** It stays id-keyed end to end. The reconciler
  dispatches ids out of the `workQueue`, and the `*Object` handed to `Reconcile`
  already carries `ID`, so the controller never has a slug it would rather use.
- **No change to `ObjectRef`.** It is `{ID, Group, Kind}`
  (`internal/storeapi/storeapi.go:277`), and edges are cross-kind: a slug is unique
  only *within* a `GroupKind`, so a slug alone does not address an edge endpoint.
  This is why the whole edge/ref family stays id-keyed (Change 2).
- **The rename stops at the `Client` boundary.** `storeapi.Store` keeps
  `ObjectsGetBySlug` / `DeletionRequestsCreateBySlug` under those names, because the
  store's key really is the id and its slug methods really are the qualified variant.
  Renaming there would invert the naming against the thing being named.
- **Not the in-memory slug→id memo.** Once the slug is required and immutable, the
  mapping is immutable per incarnation and safe to cache off the write log the
  dependency waker already scans. That is the cheapest way to make the indirection
  free, and it touches no schema — but it is a distinct change, gated on a profile.
  See Open questions.
- **The slug stays immutable and stays opaque**, with one decided exception: the
  empty string is rejected. See Decisions.

## Current state (verified)

| fact | site |
| --- | --- |
| `slug TEXT` is nullable, and `UNIQUE ("group", kind, slug)` does not constrain it — *"SQLite NULL != NULL so multiple NULL slugs are allowed"* | `sqlite/migrations/0001_init.sql:23-24`, `:75` |
| `Create` has no slug parameter; the slug arrives as an `Option` | `client.go:43`, `options.go:178-186` |
| `GetOrCreate` already takes the slug positionally. **`CreateOrUpdate` no longer exists** — it was deleted in `3d9b36a`, so the slug-keyed write family is three, not four | `client.go:134`, [store-write-shapes ADR](adr/2026-07-30-store-write-shapes.md#L85-86) |
| `GetOrCreate` rejects `WithSlug` with `ErrConflictingOption` | `client.go:418-421` |
| `Object.Slug` is `*string`, and so is `RawObject.Slug` and `ObjectsCreateInput.Slug` | `types.go:146`, `internal/storeapi/storeapi.go:181`, `:248` |
| `scanObject` decodes the column through `sql.NullString`, so NULL is representable end to end | `sqlite/store.go:2376`, `:2399-2400` |
| The slug is immutable **by absence** — there is no `UpdateSlug`, and no mutator writes the column | verified across `sqlite/store.go` |
| `markForDeletion` takes the caller's whole row predicate as a SQL `where string`, so a new keying is a call-site change rather than a second copy of the statement. It is an **unexported `sqliteStore` helper**, not a `storeapi` shape | `sqlite/store.go:1856-1878` |
| `getObjectRowBySlug` already exists — the helper the deferred TODO fix asks for. It selects `objectColumns`, which **includes the `spec` and `status` blobs** | `sqlite/store.go:801-807`, `objectColumns:637-640` |
| `DeletionRequestsCreateBySlug` opens `Within` before anything is known to match — but **no longer writes the sequence row on a zero-row mark**: `markForDeletion` reads `value + 1` inline and calls `nextResourceVersion` only after `RowsAffected > 0` | `:1919` → `requestDeletion:1890` → `markForDeletion:1856-1878` |
| `requestDeletion` is already shared by both keyings, and already takes its resolver as a `probe func(context.Context) error` — an error-only existence check, not a row re-read | `sqlite/store.go:1880-1904` |
| `DeletionRequestsCreateBySlug`'s probe is already a metadata-only `SELECT 1`, not `getObjectRowBySlug` | `sqlite/store.go:1923-1934` |
| `ObjectsUpdateSpec` is read-compare-write inside `Within`: `getObjectRowScoped` loads the row (kind check *and* no-op compare), then the `UPDATE` is keyed on `id` alone with `RETURNING` | `sqlite/store.go:1223-1283` |
| The edge/ref and event lookups are documented as **not** kind-scoped, and take foreign ids on purpose | `client.go:63-79`, `:186-197` |
| `ControllerClient` is `ControllerClient[Status any]` and has no create path, so the Non-goal holds by construction | `controller.go:44` |
| `ObjectID` is `= int64` (an alias), so slug/id confusion at a call site is a type error, not a silent one | `internal/storeapi/storeapi.go:35` |
| The migrator records versions only — **no checksum** — so amending an applied migration is undetectable | `internal/sqlitemigrate/sqlitemigrate.go:143-152` |
| `foreign_keys` is ON at the DSN, and each migration runs inside `BeginTx` — so `PRAGMA foreign_keys=OFF` *inside* a migration is silently ignored | `internal/sqlitemigrate/sqlitemigrate.go:76`, `runMigration:183-184` |
| Only `0001_init.sql` exists, so a `0002` is the next version and the gap check accepts it | `sqlite/migrations/`, `sqlitemigrate.go:129-130` |
| The README promises the opposite of what Change 2 delivers: *"Everything after the create takes an `ObjectID`, so a delete and recreate under the same slug can't make you act on the wrong row."* | `README.md:348` |

## Change 1 — the slug is required at creation

### Go layer (load-bearing)

```go
// before
Create(ctx context.Context, spec Spec, opts ...Option) (*Object[Spec, Status], error)
func WithSlug(slug string) Option

// after
Create(ctx context.Context, slug string, spec Spec, opts ...Option) (*Object[Spec, Status], error)
// WithSlug is deleted.
```

`WithSlug` goes away rather than becoming redundant. A required field passed through
an options bag is the wrong shape, and the codebase already says so twice: `Create`
is the last slug-keyed write that does *not* take the slug positionally, and
`GetOrCreate` carries a bespoke `ErrConflictingOption` branch (`client.go:454-459`)
that exists only to defend against the option contradicting the parameter. Making the
slug positional everywhere deletes that branch and the `createOptions.slug` field
with it.

Consequent narrowings:

```go
// internal/storeapi — Slug loses its pointer, so there is no longer a
// representable "no slug" input.
type ObjectsCreateInput struct {
	Finalizers  []string
	Slug        string   // was *string
	Spec        []byte
	SpecVersion int
}

// types.go
Slug string   // was *string
```

`Slug string` on the input struct is what actually enforces the invariant: after this
change no code path can write a NULL, because none can express one. `insertObject`
(`client.go:360-372`) already funnels every create through one site, and its doc
comment — *"The slug rides on co, which each caller has already populated from its
single source (`WithSlug` for `Create`, the positional argument for `GetOrCreate`)"* —
collapses to one source.

### The read shape narrows too

With `NOT NULL` landing in `0001` (below) there are no NULL-slug rows at any point in
the project's life, so the read shape is not forced to carry what the column can no
longer hold. **`RawObject.Slug` becomes `string`** alongside the write shape
(`internal/storeapi/storeapi.go:181`), and the pointer disappears from the store
boundary rather than being collapsed at the decode boundary.

`scanObject` (`sqlite/store.go:2376`, `:2399-2400`) drops its `sql.NullString` and
scans straight into `obj.Slug`. That is a real assertion, not just a simplification: a
NULL arriving from the column would now fail the scan rather than decode to `""`, which
is the correct behaviour for a state the schema forbids — it surfaces a foreign writer
or a botched migration as an error on the row that has it, instead of silently
manufacturing an empty slug. It also quarantines rather than kills a listing, since
`scanObject`'s error path is already the quarantine path.

The earlier draft's decode-boundary ambiguity — a NULL row decoding to `Object{Slug:
""}`, indistinguishable from a row genuinely holding the empty slug — had exactly one
source, and both decisions independently remove it: no row can hold NULL, and no write
can express `""`.

### SQL layer

**Decided: the project is undeployed, so amend `0001_init.sql` in place.** `slug TEXT
NOT NULL`, and drop the `NULL != NULL` note from the comment at `:23`. No `0002`, no
table rebuild, no backfill, and no change to `sqlitemigrate`.

This is safe *only* because no database has applied `0001`. The migrator compares
version numbers and stores no checksum (`internal/sqlitemigrate/sqlitemigrate.go:143-152`),
so it cannot detect an amended migration: any database that had already applied `0001`
would keep its nullable column and silently disagree with the source. That is the
whole reason this was a question. It is answered, and the answer is recorded here so a
later reader does not re-derive it from an empty `migrations/` directory.

Two consequences worth stating, because they simplify the rest of the spec:

- **There are no legacy NULL-slug rows, at any point.** The residual the earlier draft
  carried — rows unreachable through `Get(ctx, slug)`, visible only via `List` and
  `GetByID` — does not arise. Neither does the `'auto-' || id` backfill; it is dropped
  from the plan entirely.
- **The read-shape ambiguity disappears with it.** Change 1's decode-boundary question
  (a NULL row decoding to `Slug: ""`, indistinguishable from a genuine empty slug) had
  exactly one source, and the constraint removes it. See the revised section below.

For the record, had databases existed: SQLite cannot add `NOT NULL` to an existing
column, so it needs the table-rebuild dance, and `objects` is the target of four
`REFERENCES` with `ON DELETE CASCADE` (`conditions`, `dependency_watermarks`,
`events`) and `ON DELETE RESTRICT` (`edges.to_id`). The standard procedure requires
`PRAGMA foreign_keys=OFF` **outside** a transaction, and this migrator runs every
migration inside `BeginTx` on a `foreign_keys(on)` connection — where the pragma is
silently ignored, exactly as `PRAGMA auto_vacuum` is
([ADR](adr/2026-07-29-auto-vacuum-incremental.md)). A naive rebuild would
cascade-delete every condition and event and then RESTRICT-fail on `edges`. That path
would have meant teaching `sqlitemigrate` to run a migration outside a transaction.
None of it is needed.

## Change 2 — `X` for the slug, `XByID` for the id

The mechanical rule: a `XBySlug` method loses the suffix, and the id-keyed `X` it
displaces gains `ByID`.

| today | after |
| --- | --- |
| `Create(ctx, spec, opts…)` + `WithSlug` | `Create(ctx, slug, spec, opts…)` |
| `GetOrCreate(ctx, slug, spec, opts…)` | unchanged — already positional, minus the `WithSlug` rejection |
| `GetBySlug(ctx, slug, loads…)` | `Get(ctx, slug, loads…)` |
| `Get(ctx, id, loads…)` | `GetByID(ctx, id, loads…)` |
| `DeleteBySlug(ctx, slug)` | `Delete(ctx, slug)` |
| `Delete(ctx, id)` | `DeleteByID(ctx, id)` |
| `Update(ctx, id, spec)` | `Update(ctx, slug, spec)` **+ new** `UpdateByID(ctx, id, spec)` |

`Update` is the one row that goes beyond the mechanical rule, since it has no
`UpdateBySlug` today. It is in scope because leaving it out splits the CRUD set: a
caller could create, read and delete by name but would have to hold an id to write.
Its slug form must be keyed in SQL, not composed — see Change 3.

**These stay id-keyed, and it is not an oversight:**

- `DependenciesList`, `DependentsList`, `OwnedList`, `OwnersGet`, `OwnedObjectsList`
- `EventsGetLatest`, `EventsList`, `EventsWatch`

Every one is reached through an edge, returns or consumes an `ObjectRef` (id, group,
kind — no slug), and is explicitly documented as *not* kind-scoped: *"Another kind's
id reads that kind's edges, and a missing id reads empty"* (`client.go:186-192`), and
`OwnedObjectsList`'s `ownerID` *"need not be this client's kind — it is the owner,
typically another kind"*. A slug cannot address any of these without a `GroupKind`
beside it, which is what an `ObjectID` already is in one word.

`ObjectsWatch`, `Requeue`, `SchedulesGet` and `SchedulesWatch` are kind-scoped and
*could* take a slug. They are left alone here to keep the change to one coherent
story; see Open questions.

**On the naming convention.** `Client`'s bare CRUD staying bare is already the rule
(`CLAUDE.md`, and the [naming ADR](adr/2026-07-27-noun-verb-naming.md): *"Drop
the prefix when the family is the receiver itself"*), and `ByID` reads as the
qualifier it is — the same shape as `EdgesListIncoming`. Nothing in the convention
needs amending.

**On rename safety.** `Get(ctx, id)` → `Get(ctx, slug)` changes a parameter type
without changing the name, which is normally the dangerous kind of rename. It is safe
here only because `ObjectID = int64` and a slug is a `string`, so every stale call
site is a compile error. Worth an explicit note in the ADR, because it is a property
of the types rather than of the design, and it would quietly stop holding if
`ObjectID` ever became a string-backed type.

## Change 3 — the ABA contract the rename changes

`README.md:348` currently promises: *"Everything after the create takes an
`ObjectID`, so a delete and recreate under the same slug can't make you act on the
wrong row."* Change 2 retires that sentence, and it must be replaced by an explicit
contract rather than left to inference:

> A **slug-keyed** call acts on whatever holds that slug *now*, or reports absence.
> An **id-keyed** call acts on that one incarnation, or returns `ErrNotFound`.

That split is not a compromise; it is the level-triggered principle applied to the
API surface. `ensure this child exists` / `remove this child` is a statement about a
name, and it should re-evaluate against current state on every reconcile — which is
what a slug-keyed call does. `finish what I read a moment ago` is a statement about
an incarnation, and that is what an id is for.

Two requirements follow.

**1. Every slug-keyed write resolves and writes inside one transaction.** The earlier
draft of this spec said "puts the slug in its own `WHERE` clause; never resolve then
write", and generalised that from `DeletionRequestsCreateBySlug` (`README.md:432`:
*"the slug goes into the store's `WHERE` clause rather than being resolved first and
deleted after, so no concurrent collection can retire the row and hand its slug to a
replacement in between"*). **That rule does not transfer to `Update`, and it is not
what the codebase actually asserts.** Two reasons:

- `ObjectsUpdateSpec` **must** read the row before it writes. The generation
  handshake's no-op skip compares the stored bytes and the stored schema version
  against the incoming ones (`sqlite/store.go:1262`), and that comparison is what
  stops a controller re-applying its own spec from waking itself forever
  ([ADR](adr/2026-07-27-generation-handshake-and-noop-writes.md)). A
  single slug-keyed `UPDATE … WHERE slug = ?` has nothing to compare against, so a
  pure-`WHERE` mutator would silently drop the skip — a correctness regression, not a
  style question.
- Atomicity here comes from `Within`, not from statement count. `ObjectsUpdateSpec`
  already says so: *"Within keeps the read-compare-write atomic so a concurrent writer
  can't slip between the no-op check and the update"* (`:1224-1225`). `GetOrCreate`
  rests on the same property. The store runs every caller through one connection under
  `BEGIN IMMEDIATE`, so a read and a write in one `Within` are as indivisible as one
  statement is.

So the correct shape is **`ObjectsUpdateSpecBySlug(ctx, gk, slug, spec, specVersion)
(*RawObject, error)`**, implemented as today's `ObjectsUpdateSpec` body with
`getObjectRowScoped` swapped for `getObjectRowBySlug` and the `UPDATE` still keyed on
the loaded `obj.ID`. Keying the write on the id is safe *because* the read is in the
same transaction — the existing comment already licenses it: *"Keyed on id alone: the
kind boundary came from the scoped read above, in this same transaction"*
(`:1271-1273`). The two mutators then share one body with the resolver as a parameter,
exactly as `requestDeletion` does for the delete pair.

What remains true, and is the real requirement: **it must not be implemented in the
`Client` layer as `Get(slug)` followed by `UpdateByID`,** because those are two
separate store calls and nothing holds a transaction across them.

Two consequences to pin:

- Absence is reported by `getObjectRowBySlug` returning `ErrNotFound` directly — no
  probe is needed, unlike `requestDeletion`, because this mutator reads the row first
  and so has no ambiguous zero-row outcome to resolve.
- There is no `ErrWrongKind` on this path: the kind is in the `WHERE`, so a slug this
  kind does not hold is absent rather than foreign. Same reasoning as
  `DeletionRequestsCreateBySlug`'s probe comment (`:1921-1923`).

**Rejected: a predicate-taking `ObjectsUpdateSpec`.** The earlier draft called this
"the cheaper option", citing `markForDeletion` as precedent. It is not viable.
`markForDeletion` is an unexported `sqliteStore` helper whose `where string` never
crosses a package boundary; `ObjectsUpdateSpec` is on the public `storeapi.Store`
interface, which external implementers satisfy and which is deliberately backend-
agnostic. Passing a raw SQL fragment through it would make every non-SQLite
implementation either parse SQL or fail, and would put a string-typed injection
surface on the store's public contract. Add the named method.

**2. Read-modify-write uses the id.** The residual hazard the WHERE clause cannot
close is a caller's own two-call sequence: `Get(ctx, "prod")` → mutate →
`Update(ctx, "prod", spec)`. If a GC collect and a fresh create land in between, the
write hits a different incarnation, where today's id-keyed `Update` would have
returned `ErrNotFound`. The object returned by `Get` carries `ID`, so the fix is
available at the call site and should be documented as the rule: **read-modify-write
goes through `UpdateByID(ctx, obj.ID, spec)`.** Note also that the window is narrower
than it looks — a tombstone holds the slug's `UNIQUE` constraint until GC clears
finalizers (`README.md:397,429`), so opening it takes a full collect *plus* a new
create.

A `resource_version` CAS parameter would close it in the store instead
(`resource_version` is already documented as a CAS token, `0001_init.sql:48-50`).
Out of scope: `UpdateByID` covers the case with no new surface, and adding optimistic
concurrency is its own design with its own error taxonomy.

## Change 4 — the absent-path probe

Take the deferred fix from `docs/TODO.md:68-95` as part of this change. Its own gate was
*"revisit if a profile shows absent-path deletes are hot, or if
`DeletionRequestsCreate` gets the same treatment"*, and this spec satisfies both
halves by construction — it routes **all** delete traffic through the slug path, and
it touches both keyings anyway.

**The remaining cost is smaller than this spec's first draft claimed, and the
difference matters.** That draft priced the absent path as *"`BEGIN IMMEDIATE`, an
`UPDATE` on the sequence row (`nextResourceVersion`), a zero-row `UPDATE`, a re-read,
and a rollback"*. **The sequence `UPDATE` is already gone.** `markForDeletion` now
reads `value + 1` inline in the `UPDATE` and calls `nextResourceVersion` only after
`RowsAffected() > 0` (`sqlite/store.go:1856-1878`), and the probe reads two columns
rather than a whole row. `docs/TODO.md:79-85` records this as *"two thirds of the
original cost is gone"*.

What is actually left on the absent path: `BEGIN IMMEDIATE`, a zero-row `UPDATE`, a
`SELECT 1` probe, and a rollback. **The write lock and the journal work are the whole
remaining cost** — the extra statements this fix would have saved are the ones already
removed. That absent path is still the steady state of the method: a controller that
idempotently removes a child re-runs the call every reconcile, and exactly one of
those calls ever deletes anything. Taking the write lock every reconcile on a
single-connection store is worth removing on its own; it just should not be sold as a
statement-count win.

**The fix, and why the objection dissolves.** Add a lock-free probe *before* `Within`,
short-circuiting both idempotent outcomes (no such row; row already deletion-pending)
and falling through to the atomic mark otherwise. The item deferred this because
*"its no-op branch would answer outside a transaction where the id-keyed sibling
answers inside one"* — but `requestDeletion` (`sqlite/store.go:1890`) is **already**
the shared body of both keyings, and already takes its resolver as a `probe`
parameter. Putting the pre-probe there gives both keyings the same shape, and there is
no divergence left to object to.

**Use a metadata-only probe, not `getObjectRowBySlug`.** The first draft named
`getObjectRowBySlug` / `getObjectRowScoped`. Both select `objectColumns`
(`sqlite/store.go:637-640`), which carries `spec` and `status` — so they would pull
two JSON blobs off disk to answer a question about `deletion_requested_at`, on the
hottest path in this change and for a value the caller discards. That is a regression
against the very shape the write-shapes ADR just established (*"a write … returns only
what a caller reads"*). Extend the existing `SELECT 1` probe instead:

```sql
SELECT deletion_requested_at IS NOT NULL
  FROM objects WHERE "group" = ? AND kind = ? AND slug = ?
```

one indexed probe on `UNIQUE ("group", kind, slug)`, no row fetch, no blobs. The
id-keyed sibling gets the same treatment against `checkObjectScoped`
(`:718-733`), which already reads columns only.

Cost becomes 1 statement and **no write transaction** for absent, 1 for the
already-pending no-op, and one extra statement on the happy path that actually
deletes.

**Why the probe cannot be wrong.** It is advisory: the fall-through path still runs
the atomic slug-keyed `markForDeletion`, whose `IS NULL` guard re-checks everything.
A row that *appears* between probe and return means the delete simply did not cover a
create that committed after it — the same race any delete has, and the README already
states the limit (*"What it cannot promise, and no implementation could, is that the
slug is still free when the call returns"*, `:431`). A row that *vanishes* between
probe and mark yields the same idempotent `nil`. The probe can produce a redundant
skip, never a wrong write.

## Test plan

New:

- `client_test.go`: `Create` with a slug already held fails on `UNIQUE`, unchanged
  from today — but now reachable without an option, so it needs a test at the new
  signature.
- `client_test.go`: `Get`/`Delete`/`Update` on a slug no row holds. `Get` is
  `ErrNotFound`; `Delete` is `nil` (idempotent); `Update` — **decide and pin**:
  `ErrNotFound` is the consistent answer, since unlike delete there is no
  "already in the desired state" reading of a missing row.
- `client_test.go`: the ABA contract from Change 3, both directions. Create slug
  `s` → delete → clear finalizers → let GC collect → create `s` again, then assert
  `Update(ctx, s, …)` writes the **new** incarnation while `UpdateByID(ctx, oldID, …)`
  returns `ErrNotFound`. This is the single most important test in the change: it is
  the executable form of the promise `README.md:348` used to make, and without it the
  new contract is prose.
- `sqlite/store_test.go`: `ObjectsUpdateSpecBySlug` resolves and writes in one
  transaction — assert against a concurrent-collection interleaving, not just the happy
  path, since a two-store-call implementation would pass a naive test. Plus the no-op
  case: re-writing identical bytes at the same schema version bumps neither
  `generation` nor `resource_version`, the same assertion `ObjectsUpdateSpec` already
  carries. That test is what stops the slug-keyed sibling from being written as a bare
  `UPDATE … WHERE slug = ?` and losing the skip.
- `sqlite/store_test.go`: the Change 4 probe. Absent slug and already-pending row both
  return `changed=false` **and take no write transaction**.

  **The obvious observable does not work.** An earlier draft proposed asserting
  `resource_version_seq` is unmoved across the call, on the grounds that *"without it
  the test passes on the old code"*. It is now the reverse: `markForDeletion` already
  advances the sequence only after a row is stamped, so **the counter is already
  unmoved on both no-op paths** and such a test passes on the unfixed code — precisely
  the failure it was written to prevent. Do not merge Change 4 with that assertion.

  **Decided: count transactions begun.** Add an unexported `atomic.Int64` to
  `sqliteStore` (`sqlite/store.go:46-53`) and increment it at the single `BeginTx`
  site, `Within:571`. The test asserts the count is unchanged across an absent-slug
  and an already-pending delete, and moves by one on the delete that lands.

  Three things make that site the right one:

  - **It is the only one.** `BeginTx` appears exactly once in `package sqlite`, and a
    nested `Within` returns at `:566-569` before reaching it. So "transactions begun"
    is *precisely* the quantity the probe removes — no filtering of nested joins, and
    no risk of the metric drifting from what is being claimed.
  - **Nothing is exported.** `sqlite/store_test.go` is `package sqlite` (whitebox, per
    `CLAUDE.md`), so the test reads the field directly. `storeapi.Store` is untouched,
    and external implementers never see it.
  - **The cost is nil.** An atomic add next to a `BEGIN IMMEDIATE` is unmeasurable.

  **Rejected: asserting on lock acquisition.** Holding a write transaction open on a
  second connection and calling the delete under a short-deadline `ctx` tests the
  property more directly and needs no seam — but distinguishing "blocked" from "not
  blocked" *requires* a wall-clock deadline, which is the sleep-shaped test
  `CLAUDE.md` rules out (*"Synchronize on signals, never on sleeps"*; `time` is for a
  failsafe that turns a hang into a failure, not for the assertion itself). It would
  also need a file-backed pool rather than `OpenMemory`'s single connection. The
  counter is deterministic and conventional; take it.
- `sqlite/store_test.go`: probe-then-mark interleaving — a row that becomes
  deletion-pending between probe and mark still ends `changed=false, err=nil`.
- `sqlite/store_test.go`: a direct `INSERT` with a NULL slug is rejected by the
  constraint. This has to go in at the SQL level, since no Go path can express it any
  more — which is the point: the test is what keeps the `NOT NULL` from being dropped
  as redundant later. No backfill test; there is no backfill.
- `client_test.go`: `""` is rejected with `ErrInvalidSlug` on all four slug-keyed
  writes and on `Get`. Assert `errors.Is`, not equality, and assert it *before* any
  store call — the empty check is caller-input validation and should not depend on
  whether a row happens to exist, for the same reason `GetOrCreate` validates its spec
  and options up front (`client.go`, the "validated eagerly" paragraph).

Rewrites (not mechanical):

- Every `GetOrCreate` test covering the `WithSlug` → `ErrConflictingOption` branch.
  The branch is deleted, so these lose their subject.
- Any test asserting `Object.Slug == nil` for a slugless create. There is no such
  create; these must be re-expressed or dropped.

Mechanical:

- Every `Create(ctx, spec)` call site gains a slug: the four examples plus the test
  suite. Re-derive the call sites with `grep -rn '\.Create(ctx' --include='*.go'`
  rather than trusting line numbers here; the earlier draft's citations predate
  `bd77d7d`.

  The earlier draft said to route test creates through a `mustCreate` helper *"the
  storeapi-write-contract spec introduces"*. **No such helper landed** —
  `grep -rn 'func mustCreate'` is empty. **Decided: introduce it here, as commit 0**,
  in `testutils_test.go` with the other shared helpers. Landing it before the signature
  change is the point — it absorbs the churn once, so commit 1 edits the helper rather
  than every call site, and the next signature change edits it again.
- `Get(ctx, id)` → `GetByID`, `Delete(ctx, id)` → `DeleteByID`, `Update(ctx, id, …)`
  → `UpdateByID` across the suite. Compiler-guided (`int64` vs `string`), so the
  sweep is safe; the risk is only volume.
- `fakeStore` in `testutils_test.go` and the per-test doubles gain the new store
  methods.

## Sequencing

The storeapi write contract has landed (`3d9b36a`), so `ObjectsCreateInput` — the
struct Change 1 narrows — exists today. **There is no remaining prerequisite.**

One branch, merged as one break, four commits plus docs:

0. **`mustCreate` test helper** in `testutils_test.go`. Ordering it first is what keeps
   commit 1 from paying the create churn twice.
1. **Slug required.** `slug TEXT NOT NULL` amended into `0001_init.sql`; `Slug string`
   on `ObjectsCreateInput`, `RawObject` and `Object`; `scanObject` drops its
   `sql.NullString`; positional slug on `Create`; `WithSlug` and `createOptions.slug`
   deleted; `GetOrCreate`'s conflict branch deleted; `""` rejected on the three
   slug-keyed writes. Absorbs the example and test churn.

   The schema and the Go types move together *because* the project is undeployed —
   there is no window in which one is ahead of the other, and no migration to sequence
   against. This is the commit that would have been three under a deployed database.
2. **The rename.** `Get`/`GetByID`, `Delete`/`DeleteByID`, `Update`/`UpdateByID`, and
   `ObjectsUpdateSpecBySlug` behind `Update` — sharing one body with
   `ObjectsUpdateSpec` via the resolver, and carrying the no-op skip. Signature work
   plus one new store method.
3. **The ABA contract.** Godoc on all six methods, and the Change 3 test. No code
   change — this commit exists so the contract has a reviewable diff of its own
   rather than riding invisibly on commit 2.
4. **The absent-path probe.** Change 4, in `requestDeletion`, covering both keyings.

There is no commit 5. The earlier draft held the SQL `NOT NULL` and a backfill back as
optional last steps against a possible deployed database; with the project undeployed
the constraint folds into commit 1 and the backfill is not needed at all.

`go build ./... && go vet ./... && go test ./...` green at each commit; each is
independently revertable.

## Docs

- **Delete** `docs/TODO.md:68-95` (the file is at `docs/TODO.md`, not the repo root).
- **`README.md`** is the spec, and this change contradicts it in eight places, all of
  which move in the same pass. Line numbers verified against `bd77d7d`:
  - `:242` — `Slug *string // nil when created without WithSlug; never auto-generated`.
  - `:312-318` — the `Client` interface listing (`GetOrCreate`, `GetBySlug`,
    `DeleteBySlug`).
  - `:348` — the whole "`Create` leaves the slug unset" paragraph, including the
    `ObjectID`-safety sentence Change 3 replaces, and the `:352` example call.
  - `:356` — the empty-slug warning. Its advice, *"when you mean 'no name' pass no
    `WithSlug` at all rather than `\"\"`"*, becomes impossible to follow; see Open
    questions.
  - `:358-366` — the slug-keyed-writes table and its intro. Note the intro reads *"The
    **two** slug-keyed creates"* (not three, as an earlier draft of this spec claimed):
    the family is `Create`, `GetOrCreate`, `DeleteBySlug`, and `Update` makes it four.
  - `:422-434` — the `DeleteBySlug` section, renamed and extended with the Change 4
    cost note.
  - `:569`, `:581` — the error-handling list and the `WithSlug` option line.
- **`CLAUDE.md`** — the slug-keyed-writes bullet. It currently reads *"The slug-keyed
  writes (`Create`, `GetOrCreate`, `DeleteBySlug`) differ only in what they do when the
  slug is taken"* — an earlier draft of this spec quoted a stale four-member version
  including `CreateOrUpdate`, which was deleted in `3d9b36a`. Both the membership and
  the names change; note also that the sentence's *"differ only in what they do when
  the slug is taken"* stops being true once `Update` joins the family, since `Update`
  requires the slug to be taken.
- **ADRs** — write one, `docs/adr/2026-07-30-slug-primary-key.md`, carrying the whole
  argument: why the API flips and the storage does not, and the id-vs-slug contract
  from Change 3. Add it to `docs/adr/README.md`. Amend
  `docs/adr/2026-07-27-slug-keyed-writes.md` with a forward pointer rather than
  rewriting it — its transaction-boundary reasoning is unchanged and is what Change 3
  builds on.
- **`docs/reconcile-triggers.md`** — verify no change. A create still owes its first
  reconcile by being unsettled, and nothing here touches a recording site; confirm
  rather than assume.

## Risks

**Real risk: this breaks the user-facing surface.** Unlike the storeapi break, which
only external `Store` implementers feel, every consumer of `Client` has to touch
every CRUD call site. That is the accepted cost, and the mitigation is to take it all
at once — which is why `Update` is pulled in (Change 2) rather than left for a second
break, and why the probe (Change 4) rides along instead of waiting for its own
profile.

**Verified non-risk: the rename cannot fail silently.** `ObjectID = int64` and slugs
are strings, so every un-updated `Get`/`Delete`/`Update` call site is a compile error.
This is a property of the types, not the design — flag it in the ADR so a future
change to `ObjectID`'s representation knows it is load-bearing.

**Retired risk: legacy NULL-slug rows.** The earlier draft carried this as a bounded
risk against a deferred SQL constraint. The constraint now lands in `0001` on an
undeployed project, so no such row can exist and nothing has to tolerate one.

**Accepted, and cheap: the schema is amended rather than migrated.** Editing an applied
migration is normally a serious hazard here, because the migrator stores no checksum
and would not notice (`internal/sqlitemigrate/sqlitemigrate.go:143-152`). It is safe
exactly once — while nothing has applied `0001` — and this change spends that. Anyone
holding a database built from a pre-change checkout must delete it rather than expect
an upgrade; that includes stray test fixtures and example scratch files, none of which
are checked in.

**Bounded risk: performance is a wash, not a win.** `Get(ctx, slug)` is an index
probe on the existing `UNIQUE ("group", kind, slug)` plus a row fetch — two b-tree
descents on pages almost certainly in the page cache, against one for an id. Nothing
here makes lookups faster; the win is ergonomic, plus Change 4's removal of a write
transaction from the hot idempotent-delete path. Do not sell this change on lookup
throughput, and do not let it grow a schema re-keying in pursuit of one — see
Non-goals.

## Decisions

**1. `""` is rejected.** *(was Open question 1 — decided)* Today `""` is an ordinary
slug, and the README's advice for "no name" is to pass no `WithSlug`. With the slug
required that escape hatch is gone, so a slug read from unset configuration would
silently point every such caller at one shared object, with no alternative spelling to
recommend.

The three slug-keyed writes — `Create`, `GetOrCreate`, `DeleteBySlug`, and `Update`
once Change 2 adds it — reject `""` with a new **`ErrInvalidSlug`**. A new sentinel
rather than `ErrInvalidOption`, because the slug is now a positional argument on every
one of them, not an option: `ErrInvalidOption`'s own godoc scopes it to *"an option
carrying a value that has no meaning"* (`options.go:39-43`), and reusing it would
misfile the error the moment `WithSlug` is deleted.

Reject on the **reads** too (`Get`, and `DeleteBySlug`'s idempotent path is a write so
it is already covered). A `Get(ctx, "")` can only be the same unset-configuration bug
arriving from the other side, and returning `ErrNotFound` for it would send the caller
looking for a missing row instead of a missing config value.

This is a narrow, deliberate exception to the README's *"beehive does not validate
slugs"* (`README.md:356`), and that sentence needs amending rather than deleting: no
character rules, no length limit, no normalization — one emptiness check. It is the
only footgun this change creates rather than inherits, which is what earns the
exception.

## Open questions

2. **Callers with no natural name.** Removing `WithSlug` removes the ability to create
   an unnamed object, which some callers legitimately want. *Recommendation:* an
   exported helper the caller invokes explicitly (`beehive.GeneratedSlug()`, ULID or
   `kind-<ulid>`), never implicit generation — an auto-slug that the caller did not
   choose is a name nobody can look up, which is the current NULL in a costume.
3. **Should `ObjectsWatch` / `Requeue` / `SchedulesGet` / `SchedulesWatch` take
   slugs?** All four are kind-scoped, so all four could. *Recommendation:* not in this
   change. They are latency and observability surfaces usually reached from an
   `ObjectRef` or from an object already in hand, and adding four more slug forms
   widens the break without completing anything. Revisit once the CRUD flip has real
   usage.
4. ~~**Pay for the SQL `NOT NULL`?**~~ **Decided:** yes, amended into `0001_init.sql`.
   The project is undeployed, so the transaction-free migration path in `sqlitemigrate`
   this would otherwise have needed is not required. See Change 1's SQL layer.
5. **The slug→id memo.** Immutable slugs make a per-kind cache safe off the write log
   the waker already scans, turning every slug lookup into a map hit. *Recommendation:*
   defer, gated on a profile — this spec's slug lookups are already index-backed, and
   an unmeasured cache is a coherence bug waiting for a delete/recreate.

## Readiness

**Ready to implement. No open decisions block it.** Nothing survived re-verification
against `bd77d7d` as a technical obstacle: the one stated prerequisite landed in
`3d9b36a`, the store already has every seam the change needs (`requestDeletion`'s
probe parameter, `getObjectRowBySlug`, `ObjectsUpdateSpec`'s read-compare-write body
to share), and the `ControllerClient` non-goal holds by construction.

The four decisions this spec was waiting on are settled and folded into the plan
above:

| decision | outcome | where it lands |
| --- | --- | --- |
| Empty-string slug | **Rejected**, with a new `ErrInvalidSlug`, on the four slug-keyed writes and on `Get` | Decisions; commit 1 |
| SQL `NOT NULL` | **Amended into `0001_init.sql`** — project undeployed, so no `0002`, no rebuild, no backfill, no `sqlitemigrate` change | Change 1 SQL layer; commit 1 |
| Change 4 test observable | **`atomic.Int64` on `sqliteStore`, incremented at `Within:571`** — the sole `BeginTx`, read whitebox from `package sqlite` | Test plan; commit 4 |
| `mustCreate` | **Introduced here**, in `testutils_test.go` | Test plan; commit 0 |

Two of those simplified the change rather than just unblocking it. The undeployed
answer retired a whole risk (legacy NULL rows), a migration, a backfill, and a
separate-package change to `sqlitemigrate` — and it let `RawObject.Slug` narrow to
`string` alongside the write shape instead of being collapsed at the decode boundary.
Rejecting `""` independently closed the same ambiguity from the other side.

Three corrections from the re-verification pass are worth re-reading if you reviewed
an earlier draft, because they change what gets built rather than only what is cited:
the slug-keyed spec mutator's shape (Change 3, requirement 1 — the pure-`WHERE` form
would have dropped the generation handshake's no-op skip), Change 4's cost model and
probe (the sequence `UPDATE` it claimed to remove was already gone; `getObjectRowBySlug`
would have read two JSON blobs to check one flag), and Change 4's test observable.

Remaining open questions — 2 (a `GeneratedSlug()` helper), 3 (slugs on
`ObjectsWatch`/`Requeue`/`Schedules*`) and 5 (the slug→id memo) — are all deferrals
with standing recommendations. None gates commit 1; revisit after the CRUD flip has
usage.
