# The Store contract is grouped into sub-APIs reached through accessors

- **Status:** Draft — not implemented.
- **Date:** 2026-08-07
- **Issue:** [#93](https://github.com/amorey/beehive/issues/93)

## Why

`Client` is moving to `VerbNoun`. `Store` carries its family in the method name
(`NounsVerb`, 53 methods), so the two public surfaces would document two competing
conventions. `VerbNoun` is wrong for `Store` — `AddEdge`/`ListIncomingEdges` is what the
[naming ADR](../adr/2026-07-27-noun-verb-naming.md) already rejected, and re-adopting it
would scatter each family across godoc again.

Grouping sidesteps the conflict rather than resolving it: with the noun on a **type**,
`Store` has no noun convention left to disagree with. `store.Edges().Add(…)` and
`client.ListEvents(…)` are two mechanisms, not two rules.

Three secondary wins, all recorded as costs in the naming ADR's *Consequences*:

- **No verb in the middle.** `ObjectsListUnsettledIDs` → `Objects().ListUnsettledIDs`.
- **No noun-phrase ambiguity.** `ConditionsSet` reads as "a set of conditions" only
  because a noun sits left of the verb; `Conditions().Set` cannot. It also *unblocks*
  `Events().Record()`, which the ADR notes told the truer story about run extension —
  unblocks, not adopts: D7 keeps renames out of this change.
- **Predicates read correctly.** `EdgesHasIncoming` → `Edges().HasIncoming`, matching
  `strings.HasPrefix`. No exemption needed.

## Decisions already taken

**D1. The grouped interface is the contract, and it is the only one.** `Store` becomes a
grouped interface, aliased publicly as today (`types.go:42`). There is no second flat
interface and no adapter: a backend implements the sub-APIs directly, so `NounsVerb`
disappears from the codebase rather than being relocated.

**D2. No `Flat` seam, and no `Group` adapter.** An earlier draft kept today's 53-method
interface as an implementation seam with `Group(Flat) Store` lifting it, to spare backends
the accessors (issue #93's Q1) and spare the test fakes any churn. Rejected:

- **It costs more code than it saves.** Q1 priced accessors as boilerplate *on top of* the
  53 flat methods. With the flat methods gone it is a relocation, not new code — and with
  embedding the bodies do not change at all:

  ```go
  type sqliteEdges struct{ *sqliteStore }

  func (s *sqliteStore) Edges() storeapi.Edges { return sqliteEdges{s} }

  // was: func (s *sqliteStore) EdgesAdd(…)   body references s.db
  func (s sqliteEdges) Add(…)                 // body unchanged — s.db resolves
                                              // through the embed
  ```

  One struct line and a two-line accessor per family: ~30 lines per backend, once.
  `Group` would have added ~57 forwarders, permanently.

  Bodies are unchanged **except where one store method calls another across
  families** — eight sites in `sqlite/store.go` (1832, 1835, 2401, 2407, 3057, 3065,
  3080, 3096), e.g. `s.EventsList` inside a snapshot and `s.ObjectsGet` inside a
  delete. Each becomes `s.Events().List` and moves during the *callee's* family
  commit, not the caller's, so a family commit touches sqlite files outside its own
  family. They are enumerated by the compiler like any other call site.
- **It taxes the common case.** Every new store method would need two declarations and a
  forwarder, to save a one-time migration.
- **A forwarder wired to the wrong method compiles.** The adapter is untested plumbing
  between two hand-maintained lists.
- **It would make D1 aspirational.** The contract would be grouped while every backend
  implemented flat, so no backend author would ever see the grouped API.

What the seam genuinely bought — a compiling checkpoint per family — is kept by a
*temporary* `unmigrated` interface instead (see Migration), which costs nothing once the
migration ends because it is deleted.

**D3. The optional capabilities become required, and the concept goes away.** Today
`FreePagesReleaser` and `DriverCursorer` are type-asserted on the store (`beehive.go:303`,
`waker.go:241`). An adapter cannot forward that: a wrapper's method set cannot be
conditional on the wrapped value, so the assertion would succeed for a backend lacking the
capability — contradicting the
[auto_vacuum ADR](../adr/2026-07-29-auto-vacuum-incremental.md)'s "a backend without it is
simply not drained". So optionality had to become explicit either way; required is the
simpler end state.

**Both are safe to require, because both already have a contractually valid no-op.**
`FreePagesRelease` is documented as "a report, not a guarantee; a backend may release
fewer, including none" — a backend that reclaims nothing returns `0` and is within
contract today. `DriverCursorsGet`'s `ok=false` is "absence is normal, not an error", and
the waker reseeds from the mark when it sees it — a backend that never persists is
indistinguishable from a cold store, which the
[waker-cursor ADR](../adr/2026-07-30-durable-waker-cursor.md) already calls an
optimisation over the stale-dependents pass rather than a guarantee. Requiring them costs
a non-sqlite backend three trivial methods and changes no semantics.

What this deletes: the `(T, bool)` accessors, the two type assertions, the capability
probing that any adapter would need, and the `FreePagesReleaser`/`DriverCursorer` aliases at
`types.go:44-50`.

- **`DriverCursors()` returns a `DriverCursors` interface** — an ordinary family over the
  `driver_cursors` table, with `Get`/`Set`. Not a capability, not an `-er` type.
- **`ReclaimSpace` flattens onto the root.** It is one store-wide maintenance operation
  over no table, so a sub-API for it would be a family of one with no noun behind it.
  `FreePagesRelease` is renamed in the same pass: "free pages" is sqlite's freelist,
  drained by `PRAGMA incremental_vacuum`, and postgres has no free pages a caller releases
  — only autovacuum. A contract admitting other backends should not be shaped in one
  backend's terms. `maxPages` stays: it is the unit the *caller* budgets in, and a backend
  is free to interpret it.

**D4. Postgres is not planned near-term; if it happens it lives in-repo**
(`postgres/` beside `sqlite/`), so it can import `internal/storeapi` and implement `Store`
directly. Nothing in this spec is shaped around a second backend, and no cost here is
justified by one — sqlite is the only implementation this change has to serve.

**D5. "Backend" is not a type name.** The ADRs use it as prose for a thing implementing
`Store` ("a contract change for backend authors",
[nested-within-savepoints.md:194](../adr/2026-07-29-nested-within-savepoints.md)).
Promoting it to a type would collide with that vocabulary and re-open a settled decision.
`Store` stays the contract's name and is never sqlite-specific.

**D6. The sub-APIs are interfaces, not concrete structs.** With no adapter, nothing forces
this — `Store` could hand back per-family structs and make the accessors free. Interfaces
win on two grounds that hold with one backend: each family stays independently fakeable,
which is what the reworked `fakeStore` is built on (see Testing), and `Store` stays uniform
— an interface whose members are structs invites the question at every family. The
extension-point argument is *not* load-bearing here, per D4.

**D7. No method renames ride along.** The change is structural only: a family's methods
keep their current names minus the prefix. `Events().Record` stays unavailable even though
the [naming ADR](../adr/2026-07-27-noun-verb-naming.md) judged `Record` the truer word,
and the same goes for every other tempting rename. This keeps each family commit a pure
move the compiler can verify, and keeps the diff reviewable as one mechanical shape.
A rename is cheap afterwards and costs nothing to defer.

## Shape

```go
// internal/storeapi

// Store is the durable-store contract beehive depends on. Non-generic; it deals
// only in raw rows.
type Store interface {
    io.Closer

    // unmigrated is embedded only during the migration, then deleted.
    unmigrated

    Conditions() Conditions
    Dependencies() Dependencies
    DeletionRequests() DeletionRequests
    DriverCursors() DriverCursors
    Edges() Edges
    Events() Events
    ObjectWrites() ObjectWrites
    Objects() Objects
    ReconcileOwed() ReconcileOwed

    // GetLatestResourceVersion returns the highest resource version issued
    // store-wide.
    GetLatestResourceVersion(ctx context.Context) (int64, error)
    // ReclaimSpace returns up to maxPages of space freed by deleted rows to
    // the OS and reports how many it released — a report, not a guarantee. A
    // backend that reclaims nothing returns 0.
    ReclaimSpace(ctx context.Context, maxPages int) (int, error)

    Within(ctx context.Context, fn func(ctx context.Context) error) error
    AfterCommit(ctx context.Context, fn func(ctx context.Context))
}

type Edges interface {
    Add(ctx context.Context, fromID, toID ObjectID, relation Relation) (EdgesAddResult, error)
    HasIncoming(ctx context.Context, id ObjectID) (bool, error)
    ListOutgoingByRelation(ctx context.Context, fromID ObjectID, relation Relation) ([]ObjectRef, error)
    // …
}

type DriverCursors interface {
    // Get returns the cursor last persisted for name, or ok=false if none has
    // been. Absence is normal, not an error.
    Get(ctx context.Context, name string) (cursor int64, ok bool, err error)
    // Set persists cursor for name if it is greater than what is stored, and
    // otherwise writes nothing.
    Set(ctx context.Context, name string, cursor int64) error
}

// unmigrated holds the not-yet-grouped declarations, verbatim from today's
// Store. Each family commit moves declarations out of it; the last one deletes
// it. Temporary — it must not outlive the migration.
type unmigrated interface { /* … */ }
```

Transactions are unaffected: `Within` threads the tx through `ctx`
(`storeapi.go:382`, guarded by `ErrStaleTxContext`), so a sub-API method taking `ctx`
joins exactly as a root method does. `Within`/`AfterCommit` stay on the root, which is
correct — they span every family.

An accessor allocates nothing: `sqliteEdges{*sqliteStore}` is pointer-shaped, so it fits
in the interface value's data word directly.

## Family map

All 57 members — today's 53, plus `Close`, plus the three capability methods D3 folds in
(`FreePagesRelease`, `DriverCursorsGet`, `DriverCursorsSet`) — with their home. Nine
families, plus the root. **No family has fewer than two members** — the one-method
families are gone.

| family | table | n | members (after) |
|---|---|---|---|
| `Objects` | `objects` | 15 | Create, **DeleteFinalizer**, Delete, Get, GetByName, GetForReconcile, GetMeta, List, ListByIDs, ListByIncomingEdge, ListIDs, ListUnsettledIDs, UpdateSpec, UpdateSpecByName, UpdateStatus |
| `ObjectWrites` | `object_writes` | 8 | ListSince, ListSinceAll, MaxVersion, MaxVersionAll, Snapshot, SnapshotByID, SnapshotByOwner, Sweep |
| `Edges` | `edges` | 8 | Add, Delete, DeleteFinalizingDependsOn, GroupIncomingByID, GroupOutgoingByID, HasIncoming, ListIncoming, ListOutgoingByRelation |
| `Events` | `events` | 7 | Add, GetLatest, List, ListSince, MaxVersion, Snapshot, Sweep |
| `DeletionRequests` | `objects` | 4 | Create, CreateByName, CreateFromOwner, List |
| `ReconcileOwed` | `objects` | 4 | Decrement, ListIDs, Stamp, Sweep |
| `Conditions` | `conditions` | 2 | Delete, Set |
| `Dependencies` | `dependency_watermarks` | 2 | **ListStaleSince**, **WatermarkSet** |
| `DriverCursors` | `driver_cursors` | 2 | Get, Set |
| root | — | 5 | Within, AfterCommit, Close, **GetLatestResourceVersion**, **ReclaimSpace** |

### What flattened, and why

Beyond `ReclaimSpace`, three members lose their family. The test each time is **does a
table stand behind the noun** — the same line `CLAUDE.md` already draws on
`ControllerClient` between "a column on the object's row" and "a table of its own".

A flattened member has no type to carry its noun, so it carries it in the name, verb
first: `GetLatestResourceVersion`, not `MaxIssuedVersion` or `ResourceVersionsMaxIssued`.
That is `VerbNoun` — the same rule `Client` is moving to, which is the right outcome: the
root of `Store` is the one place with no family noun to put on a type, so it reads like
`Client` rather than like a family.

- **`FinalizersDelete` → `Objects().DeleteFinalizer`.** `finalizers` is a **column** on
  `objects` (`finalizers TEXT NOT NULL DEFAULT '[]'`), not a table — so by the repo's own
  rule it is not a family. It is also already scoped by `gk` and `id` like every other
  `Objects` mutator. (Note: the
  [naming ADR](../adr/2026-07-27-noun-verb-naming.md):34 lists finalizers among things
  "each its own table". Accurate for `ControllerClient`'s surface, wrong about the schema;
  worth a one-line fix either way. `CLAUDE.md` states the rule without the enumeration and
  needs no change.)
- **`ResourceVersionsMaxIssued` → root `GetLatestResourceVersion`.**
  `resource_version_seq` is a single-row counter, not a domain family, and the value is
  store-wide. A `ResourceVersions()` family would be one method over one row.
- **`ReclaimSpace` → root**, per D3. No table at all.

### What stayed a family, against the flattening instinct

- **`DependencyWatermarks` merges with the stale-dependents scan into `Dependencies`.**
  Previously I put `DependentsListStaleSince` in `Edges` and left watermarks as a family
  of one; both were wrong. They are one subsystem — the watermark is written by a
  successful reconcile and read by the scan that finds who a target has moved past — and
  splitting them across two families hides that. `dependency_watermarks` is a real table,
  so the noun is earned. Now two members: `ListStaleSince` and `WatermarkSet`.
- **`DeletionRequests` and `ReconcileOwed` sit on `objects` columns**, so the column test
  would flatten them — but at four members each they would add eight prefixed methods back
  onto the root, which is the outcome this change exists to avoid. The column test breaks
  ties for singletons; it does not override a real family.

## Migration

Each step compiles and passes.

0. **Fold the three capability methods into `Store`** (D3), before any grouping. `Store`
   absorbs `FreePagesRelease` → `ReclaimSpace`, `DriverCursorsGet` and `DriverCursorsSet`;
   the `FreePagesReleaser`/`DriverCursorer` types and their `types.go:44-50` aliases are
   deleted. **This deletes live behaviour, not just two type assertions.** `dw.cursors` is
   nillable and branched on at `waker.go:298`, `:364` and `:563`, feeding
   `resumeWatermark(stored, ok, mark)`; with the methods required, the field stops being
   nillable and those branches go away. The no-capability path is covered today and the
   coverage must be retargeted, not dropped: `cursorStore` (`testutils_test.go:682`) exists
   precisely because `replayStore` implements no `DriverCursorer`, and `waker_test.go:54`,
   `:90`, `:510`, `:1581` plus `beehive_test.go:109` (asserting `freePagesSweep` does not
   panic without the capability) all exercise it. Each retargets to a store whose
   `DriverCursors().Get` returns `ok=false` — same semantics, reached through a value
   instead of a nil check. `sqlite` already implements all three methods.
1. **Introduce the staging shape.** Rename today's interface `unmigrated`, and declare
   `Store` as `io.Closer` + `unmigrated`. Pure rename; nothing else moves.

   The test-double seam does **not** come first, though an earlier draft put it there:
   its override fields are typed on the sub-interfaces (`func(storeapi.Edges) …`), which
   do not exist until a family migrates. Written first it would have to be written flat
   and regrouped later — the churn it exists to avoid. Each family's doubles convert
   inside that family's commit instead.
2. **One family per commit.** *Move* that family's declarations out of `unmigrated` into
   a new sub-interface, shortening each name; add the accessor and the one-line receiver
   struct in `sqlite`; add the family's override struct to the test seam and convert the
   doubles that shadow it; rewrite the call sites — ~74 in the root package, ~1068 in
   tests, plus the 8 cross-family bodies in `sqlite/store.go`, which move with the
   *callee's* family. All compiler-enumerated. Nine commits.
3. **Delete `unmigrated`.** Empty by now; its deletion is the migration's end state. Note
   this step is **manual**: `unmigrated` stays embedded in `Store`, so it is still "used"
   and no linter will flag it. Emptiness is the signal to look, not a tripwire that fires.

No duplication exists at any point: a declaration is either in `unmigrated` or in its
family, never both.

## Testing

There are **two** shadowing idioms in the suite, not one, and they are disjoint:

- **33 embed `fakeStore`** (+7 embed an intermediate fake) — pure fakes, unimplemented
  methods panic.
- **36 embed the `Store` interface** — *decorators*: `wakeProbeStore`, `listProbeStore`,
  `freePagesStore`, `eventSweepStore`, `flakyListStore`, … each wraps a **real** sqlite
  store, shadows one or two methods to fail or count, and delegates the rest.

Grouping breaks the decorators, and it is the accessor indirection that breaks them, not
the naming: `struct{ Store }` + shadowing `EdgesListIncoming` works only while the method
is on the interface. Written naively each decorator needs an accessor override *and* a
sub-interface wrapper — two types where there was one, and `listProbeStore`
(`testutils_test.go:1124`) shadows five methods across four families, so four wrappers.
That is issue #93's Q1 cost resurfacing on the test side, which D2 removed the seam that
paid for.

**So each family commit extends one shared seam that serves both idioms.** A single decorator type
in `testutils_test.go` wraps a `Store` and holds per-family override structs of func
fields; each family's wrapper delegates to the wrapped store when its field is nil. Both
idioms then read as closures over a base:

```go
// decorator, before: a type + a shadowed method, per probe
type wakeProbeStore struct{ Store; … }
func (s *wakeProbeStore) EdgesListIncoming(…) (…) { s.n++; return s.Store.EdgesListIncoming(…) }

// after: same base, one closure
d := decorate(realStore)
d.edges.listIncoming = func(next storeapi.Edges) … { n++; return next.ListIncoming(…) }
```

`fakeStore` gets the same treatment with panic-stubs as the defaults instead of
delegation, so the 33 collapse the same way. Measured: **35 decorator types, 61 overrides,
34 distinct methods**, and the worst case is `pollProbeStore` at 12 methods across four
families — four hand-written wrappers, without the seam.

The ~1068 direct `store.ObjectsCreate(…)` calls in tests (355 root, 713 under `sqlite/`)
change one family at a time inside that family's commit. An earlier draft claimed these
were free; that was true only while `*sqliteStore` kept its flat methods, which is exactly
what the rejected `Flat` seam paid for.

Step 0 edits tests rather than removing them, as above. `waker_bench_test.go:171` is the
one genuine subtraction: it asserts `store.(DriverCursorer)` and skips if absent, and with
the methods required both the assertion and its skip branch go away.

## Open questions

None. The three the draft carried are settled as D6 (interfaces), D7 (no renames) and D4
(postgres deferred, so the required-capability risk in D3 is not worth pricing now —
making a required method optional again is additive if it ever bites).

## Follow-ups

- Amend the [naming ADR](../adr/2026-07-27-noun-verb-naming.md): for `Store` the noun
  lives on a type, so `NounsVerb` no longer governs it at all — except on the root, where
  a member with no family carries its noun in the name, verb first.
- Write the ADR for this decision once implemented; update `CLAUDE.md`'s summary and the
  `Store`-shaped bullets.
- Amend the two ADRs that argue these capabilities *out* of the contract, since D3
  reverses both. The
  [auto_vacuum ADR](../adr/2026-07-29-auto-vacuum-incremental.md) says "reclaiming pages
  is one backend's concern. Putting `FreePagesRelease` in `Store`…"; the
  [waker-cursor ADR](../adr/2026-07-30-durable-waker-cursor.md) says "deliberately not a
  member of `Store`". Both keep their substance — reclaiming is still best-effort, the
  cursor is still an optimisation over the stale-dependents pass — but the mechanism
  changes from "absent from the contract" to "present with a valid no-op".
- Fix `docs/adr/2026-07-27-noun-verb-naming.md:34`, which lists finalizers among things
  "each its own table": they are a column on `objects`. True of `ControllerClient`'s
  surface, wrong about the schema.
- Answer #93 with D1–D5 and close it.
- Separately: `sqlite/sqlite.go:47` sets `SetMaxOpenConns(1)`, and the `Within` contract,
  the SAVEPOINT nesting rules and the
  [sole-writer ADR](../adr/2026-08-05-one-process-one-beehive-sole-writer.md) are written
  around it. A concurrent postgres backend would make those clauses wrong or
  under-specified. Deferred with postgres itself (D4) — worth an issue only if that work
  is scheduled, not now.
