# Spec: rename the public API surface from `NounsVerb` to `VerbNoun`

- **Status:** Proposed — not yet implemented.
- **Date:** 2026-08-07
- **Branch:** `public-api-rename`
- **Scope:** `Client` and `ControllerClient` method names, plus every call site,
  doc and comment that spells one. **Pure rename — no behaviour changes.**

## Why

[`docs/adr/2026-07-27-noun-verb-naming.md`](../adr/2026-07-27-noun-verb-naming.md)
named every method `NounsVerb` so that godoc's alphabetical member list grouped a
type's methods by the family they operate on. It has already been half-retired:
the 2026-08-07 amendment moved `Store` to accessors (`store.Edges().Add`,
`store.Objects().ListUnsettledIDs`), where the noun sits on a type and the method
is a bare verb.

`Client` and `ControllerClient` are the two surfaces the amendment did **not**
reach, and they cannot follow `Store` there — the receiver is already the kind,
so `client.Events().List(ctx, id)` would add an accessor per secondary noun to a
surface whose primary verbs are bare. That leaves them the odd ones out: a user
writes `client.Get(...)` beside `client.EventsList(...)` and
`cc.UpdateStatus(...)` beside `cc.ConditionsSet(...)`, so the same surface reads
verb-first in one line and noun-first in the next.

This spec makes the whole public surface verb-first, which is also Go's house
style (`http.Client.Do`, `sql.DB.QueryRow`, `os.ReadFile`) and what the bare
verbs on both surfaces already are.

## The rule

**`VerbNounQualifier`.** The verb comes first and carries cardinality —
`Get`/`Watch` for one, `List`/`WatchList` for many. The noun names what the verb
acts on and is **singular when the verb acts on one thing** (`AddEvent`,
`GetOwner`), **plural when it returns or streams many** (`ListEvents`,
`ListDependents`). A qualifier that names a **key you pass** trails
(`ByName`, `OrCreate`); an **adjectival** one that is part of naming the thing
sits before the noun (`GetLatestEvent`, `HasIncomingEdges`, `ListOwnedObjects`).

**Omit the noun when it is the receiver's own kind.** Unchanged from the current
rule: `Create`, `Get`, `Update`, `Delete`, `List`, `Watch`, `WatchList` stay
bare on `Client`, and `UpdateStatus`/`Within` stay as they are on
`ControllerClient`. Only secondary nouns take one.

Exempt, also unchanged: `Err*` values, `With*`/`Load*` options, `Object`'s
relation accessors (`Owner()`, `Dependencies()`, `Dependents()`, `Owned()`,
`Events()` — Go's accessor idiom, no verb), type names, and anything satisfying
an external interface.

## Renames

### `Client` (client.go)

| Old | New |
| --- | --- |
| `DependenciesList` | `ListDependencies` |
| `DependentsList` | `ListDependents` |
| `EventsGetLatest` | `GetLatestEvent` |
| `EventsList` | `ListEvents` |
| `EventsWatch` | `WatchEvents` |
| `OwnedList` | `ListOwned` |
| `OwnedObjectsList` | `ListOwnedObjects` |
| `OwnedObjectsListWatch` | `WatchOwnedObjects` |
| `OwnersGet` | `GetOwner` |
| `SchedulesGet` | `GetSchedule` |
| `SchedulesWatch` | `WatchSchedule` |

Unchanged: `Create`, `Delete`, `DeleteByName`, `Get`, `GetByName`,
`GetOrCreate`, `List`, `Requeue`, `Update`, `UpdateByName`, `Watch`,
`WatchList`.

### `ControllerClient` (controller.go)

| Old | New |
| --- | --- |
| `ConditionsDelete` | `DeleteCondition` |
| `ConditionsSet` | `SetCondition` |
| `DependenciesAdd` | `AddDependency` |
| `DependenciesDelete` | `DeleteDependency` |
| `DependenciesList` | `ListDependencies` |
| `DependentsList` | `ListDependents` |
| `EdgesHasIncoming` | `HasIncomingEdges` |
| `EventsAdd` | `AddEvent` |
| `FinalizersDelete` | `DeleteFinalizer` |
| `OwnedList` | `ListOwned` |
| `OwnersGet` | `GetOwner` |

Unchanged: `UpdateStatus`, `Within`.

The four relation reads (`GetOwner`, `ListDependencies`, `ListDependents`,
`ListOwned`) must stay spelled identically on both surfaces, as they are today.

### Judgment calls, recorded

Each of these has a defensible other answer; the implementation must not
re-litigate them silently.

- **`WatchOwnedObjects`, not `WatchListOwnedObjects`.** The rule says the verb
  carries cardinality, and `WatchList` earns its `List` only because its noun is
  elided — there is no plural left to say "many". Once the noun is spoken and
  plural, `List` is redundant. The cost is that the owner-scoped watch no longer
  looks like a sibling of `WatchList` in the name alone; the godoc keeps saying
  so.
- **`HasIncomingEdges`, not `HasIncomingClaims` or `HasDependents`.** This is a
  rename, not a redesign. `Edges` stays the noun for the same reason the ADR
  chose it: the method folds owned children and live dependents together, and
  neither `Dependents` nor `Owned` covers both. `Has` is the verb, so the
  predicate now reads as one.
- **Singular `AddEvent` / `SetCondition` / `AddDependency` / `DeleteFinalizer`.**
  The old rule forced plurals so each family had exactly one prefix. Verb-first
  drops that need — the verb, not the noun, is the grouping key — so the noun
  goes back to matching the argument: one `EventSpec`, one `Condition`, one
  edge, one finalizer string.
- **`GetLatestEvent`, not `GetEventLatest` or `GetLatestEventInCategory`.** An
  adjectival qualifier sits before the noun — it is part of naming the thing,
  not a key you pass — and the `category` argument is documented, not named.
  This is the same carve-out that produces `HasIncomingEdges` and
  `ListOwnedObjects`; only key-naming qualifiers (`ByName`, `OrCreate`) trail.
- **`ListOwned` keeps its bare adjective.** `ListOwnedRefs` would say what comes
  back, but `ListDependencies`/`ListDependents` return `[]ObjectRef` under a bare
  noun too, and `ListOwned` beside `ListOwnedObjects` is exactly the untyped/typed
  pair the README already describes.
- **Types are not renamed.** `EventsAddInput`, `ObjectsCreateInput`,
  `ObjectSnapshot`, `ObjectListSnapshot`, `ObjectChange`, `EventStream`,
  `EventSpec` all stand. A type name is a noun by definition; the convention is
  about methods. `EventsAddInput` is named after `Store.Events().Add` and is out
  of this spec's scope either way.

### Not in scope

- **`Store` and `internal/storeapi`.** Already accessor-based and verb-first as
  of #94. Untouched.
- **Options and loaders.** `WithEventCategory`, `WithEventsResumeFrom`,
  `LoadOwner`, `LoadEvents` etc. are exempt by rule and stay exactly as they are.
- **`Object` accessors.** `Owner()`, `Dependencies()`, `Dependents()`,
  `Owned()`, `Events()` stay.
- **Stale store-side comments.** `sqlite/store_test.go` and
  `internal/storeapi/storeapi_test.go` still spell pre-#94 store names
  (`EventsAdd`, `EventsList`, `EdgesHasIncoming`, `ConditionsSet`) in prose
  comments and in test function names (`TestEventsListSince*`,
  `TestConditionsSetLoadError`, `TestDependentsListStaleSince*`). That is
  pre-existing drift from the store refactor, not this rename. **Leave it**, so
  the diff stays reviewable as one mechanical change. It is **not** currently in
  `docs/TODO.md`, so add an entry for it there as part of this change — one
  line, naming the two files.

## What this costs

Worth stating up front so the ADR can record it honestly: **godoc no longer
groups a type's methods by family.** That was the original rule's whole
argument. After this change `Client`'s alphabetical member list interleaves
families under shared verbs —

```
Create, Delete, DeleteByName, Get, GetByName, GetLatestEvent, GetOrCreate,
GetOwner, GetSchedule, List, ListDependencies, ListDependents, ListEvents,
ListOwned, ListOwnedObjects, Requeue, Update, UpdateByName, Watch, WatchEvents,
WatchList, WatchOwnedObjects, WatchSchedule
```

— so "what can I do with events?" now means scanning for `Event` across four
verbs instead of reading one contiguous block. What is bought back: "how do I
read one thing?" is now the contiguous block, the surface reads uniformly
verb-first instead of switching halfway through a call site, and the
twenty-three members of `Client` are small enough that neither grouping is a
search problem.
The argument that carried `NounsVerb` was a forty-method `Store`, and `Store`
solved it a different way.

## Implementation

Mechanical, one commit. Ordered so the compiler finds every miss.

1. **Rename the declarations.** `Client` (client.go:140) and `ControllerClient`
   (controller.go:38) interface members, and the `clientImpl` /
   `controllerClientImpl` methods that implement them.
2. **Re-sort both interfaces alphabetically** under the new names — the
   convention is that source order matches godoc order, and every rename moves.
   Move each member's doc comment with it.

   One member pair cannot just be moved. `client.go:271` documents `Watch` and
   `WatchList` in **one** comment block above `Watch`, with `WatchList`
   carrying none of its own; the new sort is `Watch`, `WatchEvents`,
   `WatchList`, `WatchOwnedObjects`, so `WatchEvents` lands between them and the
   shared block would silently become `Watch`-only while `WatchList` went
   undocumented. **Split it.** `Watch` keeps the block, with its opening
   sentence narrowed to `Watch` alone and the one-incarnation paragraph intact.
   `WatchList` gets its own comment: what it covers (every object of this
   client's kind), and one sentence saying the snapshot-and-stream guarantees
   are `Watch`'s — the cursor/no-gap contract, the shared tailer,
   latest-per-object delivery, no watch inside a transaction. Do not paraphrase
   those guarantees twice; state them once on `Watch` and point at it.

   Godoc already renders these two apart under the current names, so the shared
   block was only ever readable in source. Do **not** keep the pair adjacent as
   an exception to the sort — that trades a convention that holds everywhere for
   one comment's convenience.

   README's listing is thematically grouped and keeps `Watch`/`WatchList`
   adjacent, so it needs no equivalent split.
3. **Fix call sites** until `go build ./...` is clean. Affected non-test files:
   `client.go`, `controller.go`, `beehive.go`, `eventswatch.go`,
   `objectswatch.go`, `scheduleswatch.go`, `reconciler.go`, `workqueue.go`,
   `options.go`, `types.go`, and `examples/{cascade,conditions,events}/main.go`.
   Several of these are prose comments referring to a public method by name
   (e.g. `workqueue.go:34`, `beehive.go:401`, `options.go:40,64,101,128,140,180`)
   — update those too; the compiler will not.
4. **Fix tests.** `beehive_test.go`, `client_test.go`, `controller_test.go`,
   `eventswatch_test.go`, `gc_test.go`, `objectswatch_test.go`,
   `objectswatch_bench_test.go`, `reconciler_test.go`, `scheduleswatch_test.go`,
   `testutils_test.go`, `types_test.go`, `waker_test.go`, `workqueue_test.go`.
5. **Rename the test functions that carry a renamed method in their name**, so a
   failure still points at the method it covers:
   - `eventswatch_test.go`: `TestEventsWatch*` → `TestWatchEvents*` (24 tests).
   - `objectswatch_test.go`: `TestOwnedObjectsListWatch*` → `TestWatchOwnedObjects*` (9 tests).
   - `controller_test.go`: `TestFinalizersDelete*` → `TestDeleteFinalizer*` (4),
     `TestDependenciesDelete*` → `TestDeleteDependency*` (5).
   - `scheduleswatch_test.go`: `TestSchedulesWatchEmitsOnlyOnChange` →
     `TestWatchScheduleEmitsOnlyOnChange`. The `TestScheduleStream*` names cover
     the stream type, not the method — leave them.
   - `client_test.go`'s `TestClientListOwnedObjects*` already read verb-first;
     leave them.
   Do **not** touch `sqlite/store_test.go` (see *Not in scope*).
6. **Update the docs.**
   - `README.md` — the two interface listings (~lines 355–378 and 640–655) and
     every prose mention, across the Events, Watches, Secondary lookups,
     Schedules, `ControllerClient` and Options sections. `README.md` is the
     spec; it must be exactly right, so work from the counts rather than from a
     read-through — **82 mentions in total**:

     | name | n | | name | n |
     | --- | --- | --- | --- | --- |
     | `EventsAdd` | 10 | | `EventsGetLatest` | 3 |
     | `OwnersGet` | 9 | | `FinalizersDelete` | 3 |
     | `OwnedList` | 9 | | `OwnedObjectsList` | 3 |
     | `EventsWatch` | 8 | | `OwnedObjectsListWatch` | 3 |
     | `DependentsList` | 7 | | `SchedulesGet` | 3 |
     | `EventsList` | 6 | | `DependenciesDelete` | 3 |
     | `DependenciesList` | 6 | | `SchedulesWatch` | 2 |
     | | | | `ConditionsSet` | 2 |
     | | | | `DependenciesAdd` | 2 |
     | | | | `EdgesHasIncoming` | 2 |
     | | | | `ConditionsDelete` | 1 |

     The counts are disjoint (`OwnedObjectsList` does not include
     `OwnedObjectsListWatch`), and README contains no `EventsAddInput`, so
     nothing there needs preserving from the rename.

     README's two interface listings are grouped **thematically**, not
     alphabetically, and stay that way: only source order follows godoc. The two
     will diverge in member order after this change, and that is intended.
   - `docs/reconcile-triggers.md` and `docs/TODO.md` — mentions of renamed
     methods.
   - `CLAUDE.md` — four places, all of them:
     - **line 374, the first bullet of Conventions** ("The noun names a type,
       and the method is a bare verb"): rewrite so it states both halves —
       `Store` reaches families through accessors and the method is a bare verb;
       `Client`/`ControllerClient` are `VerbNoun` with the noun omitted when it
       is the receiver's kind, singular for one and plural for many. Its
       examples `ConditionsSet`/`EventsAdd` become `SetCondition`/`AddEvent`.
     - **line 204**, the owner-scoped-watch bullet under Architecture, which
       spells `OwnedObjectsListWatch` more than once.
     - **line 220**, `DependentsList`/`DependenciesList` "have no watch
       counterpart".
     - **line 226**, `ControllerClient.EventsAdd` in the event-watch bullet.

     `EventsAddInput` at line 279 is a store type and correctly stays.
   - Existing ADRs: **leave their prose as written.** They are dated records and
     several argue *about* the old names. The new ADR carries the pointer.
7. **Write the ADR**: `docs/adr/2026-08-07-verb-noun-on-the-client-surfaces.md`,
   following `docs/adr/README.md`. It **supersedes**
   `2026-07-27-noun-verb-naming.md` for `Client`/`ControllerClient` — that record
   is now fully retired, its `Store` half by the 2026-08-07 amendment and its
   client half by this. Add a superseded-by line to the old ADR's status —
   **and amend its existing 2026-08-07 amendment block** (lines 6–12), whose
   first sentence reads "This record still governs `Client` and
   `ControllerClient`". Left alone, that assertion stands three lines under a
   status saying the opposite. It should now say the record governs neither
   surface, while keeping the reason its names are left as written: several of
   its arguments are about those names. The new
   ADR must carry: the rule as stated above, the judgment calls, and the *What
   this costs* section — the godoc-grouping regression is the reason the old rule
   existed and a future reader must find it argued, not discovered.
   Add the new ADR to `docs/adr/README.md`'s index, and retitle the old entry
   there (line 66, "NounsVerb method naming and the watch return shape") so the
   index does not advertise a superseded rule as current. Rule 2 of that ADR —
   a watch over a change stream returns `<-chan NounChange` — is **not**
   superseded and must be restated in the new ADR, or it is lost with its
   record.

## Verification

```sh
go build ./...
go vet ./...
staticcheck -checks=all ./...
go test ./...
go test -run '^$' -bench . -benchtime 1x ./
go run ./examples/greeting/main.go
go run ./examples/events/main.go
go run ./examples/cascade/main.go
go run ./examples/conditions/main.go
go run ./examples/lowpower/main.go
```

Then, as a rename-completeness check, confirm no old name survives anywhere
outside `sqlite/` and `internal/storeapi/` (whose hits are the pre-existing
store-side drift) and outside `docs/adr/`. The `docs/adr/` exclusion is about
**dated prose left as written** — step 7 does edit two files in that directory,
the new ADR and `README.md`'s index, and neither should spell an old name in a
way this grep would need to catch:

```sh
grep -rnE "\b(EventsList|EventsWatch|EventsGetLatest|OwnersGet|OwnedList|\
OwnedObjectsList|OwnedObjectsListWatch|DependenciesList|DependentsList|SchedulesGet|\
SchedulesWatch|ConditionsSet|ConditionsDelete|EventsAdd|FinalizersDelete|\
DependenciesAdd|DependenciesDelete|EdgesHasIncoming)\b" \
  --include='*.go' --include='*.md' . \
  | grep -vE '^\.?/?(sqlite|internal/storeapi|docs/adr|docs/specs)/|^\.?/?docs/TODO\.md'
```

Anchor the exclusions with `\.?/?` — `grep -r .` emits paths without a `./`
prefix, so `^./sqlite/` silently matches nothing and the check passes on a
directory it never filtered.

`EventsAddInput` is a deliberate survivor of that grep — it is a type, not a
method.

Test count and behaviour must be unchanged: this rename adds no test and removes
none, and no test assertion should change except the identifiers it spells.

## Commit and PR

One commit, breaking:

```
refactor!: name the client surfaces verb-first

Client and ControllerClient were the last NounsVerb surfaces; the store
moved to accessors in #94. Every rename is mechanical and no behaviour
changes.
```

PR title: `✨ Name the public client surfaces verb-first`, body following
`.github/pull_request_template.md`. `Key Changes` should carry the two rename
tables verbatim — this is the change's whole content, and a reviewer needs the
mapping in front of them.
