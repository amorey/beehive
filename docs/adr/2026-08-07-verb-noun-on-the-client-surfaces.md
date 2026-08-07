# The client surfaces are named VerbNoun

- **Status:** Accepted — implemented across `client.go`, `controller.go`, the
  internals, `README.md` and the examples. Supersedes
  [NounsVerb method naming](2026-07-27-noun-verb-naming.md), whose `Store` half
  was already retired by its own 2026-08-07 amendment.
- **Date:** 2026-08-07

## Context

`NounsVerb` existed to make godoc's alphabetical member list group a type's
methods by family, on a `Store` that had grown to forty of them. That store no
longer exists in that shape: since the accessor refactor the noun sits on a type
and the method is a bare verb (`store.Edges().Add`).

`Client` and `ControllerClient` were left as the only `NounsVerb` surfaces, and
they cannot follow `Store` — the receiver is already the kind, so
`client.Events().List(ctx, id)` would add an accessor per secondary noun to a
surface whose primary verbs are bare. The result read two ways at once:
`client.Get(...)` beside `client.EventsList(...)`, `cc.UpdateStatus(...)` beside
`cc.ConditionsSet(...)`.

## Decision

**`VerbNounQualifier`.** The verb leads and carries cardinality — `Get`/`Watch`
for one, `List`/`WatchList` for many. The noun names what the verb acts on,
singular when the verb acts on one thing (`AddEvent`, `GetOwner`) and plural
when it returns or streams many (`ListEvents`, `ListDependents`). A qualifier
naming a **key you pass** trails (`GetByName`, `GetOrCreate`); an **adjectival**
one leads, being part of naming the thing (`GetLatestEvent`,
`HasIncomingEdges`, `ListOwnedObjects`).

**Omit the noun when it is the receiver's own kind** — unchanged. `Create`,
`Get`, `Update`, `Delete`, `List`, `Watch`, `WatchList` stay bare on `Client`;
`UpdateStatus` and `Within` stay as they were.

Exempt, also unchanged: `Err*` values, `With*`/`Load*` options, `Object`'s
relation accessors (`Owner()`, `Dependencies()` — Go's accessor idiom, no verb
left), type names, and anything satisfying an external interface.

**Rule 2 of the superseded record survives unchanged**, and is restated here so
it is not lost with its record: a watch over a change stream returns
`<-chan NounChange` by value, never a `…Watcher` interface — the channel is the
whole handle, and `ctx` cancellation ends and closes it. A watch over a **gauge**
(`WatchSchedule`) or a **log** (`WatchEvents`) streams the value itself, because
"what happened to it" is not a question those consumers can act on. The
`EventChange` gap that record left open is still open, and still a behavioural
change rather than a rename.

### What was renamed

Breaking, with no deprecation shim — pre-release. `Client`:

| Old | New | | Old | New |
| --- | --- | --- | --- | --- |
| `DependenciesList` | `ListDependencies` | | `OwnedObjectsListWatch` | `WatchOwnedObjects` |
| `DependentsList` | `ListDependents` | | `OwnersGet` | `GetOwner` |
| `EventsGetLatest` | `GetLatestEvent` | | `SchedulesGet` | `GetSchedule` |
| `EventsList` | `ListEvents` | | `SchedulesWatch` | `WatchSchedule` |
| `EventsWatch` | `WatchEvents` | | `OwnedList` | `ListOwned` |
| `OwnedObjectsList` | `ListOwnedObjects` | | | |

`ControllerClient`:

| Old | New | | Old | New |
| --- | --- | --- | --- | --- |
| `ConditionsDelete` | `DeleteCondition` | | `EdgesHasIncoming` | `HasIncomingEdges` |
| `ConditionsSet` | `SetCondition` | | `EventsAdd` | `AddEvent` |
| `DependenciesAdd` | `AddDependency` | | `FinalizersDelete` | `DeleteFinalizer` |
| `DependenciesDelete` | `DeleteDependency` | | `OwnedList` | `ListOwned` |
| `DependenciesList` | `ListDependencies` | | `OwnersGet` | `GetOwner` |
| `DependentsList` | `ListDependents` | | | |

Unchanged: `Client`'s `Create`/`Delete`/`DeleteByName`/`Get`/`GetByName`/
`GetOrCreate`/`List`/`Requeue`/`Update`/`UpdateByName`/`Watch`/`WatchList`, and
`ControllerClient`'s `UpdateStatus`/`Within`. The four relation reads are spelled
identically on both surfaces, as before. No type was renamed.

## Consequences

**Godoc no longer groups a type's methods by family.** This is the cost, and it
is the exact benefit the superseded rule was written to buy. `Client` now reads:

```
Create, Delete, DeleteByName, Get, GetByName, GetLatestEvent, GetOrCreate,
GetOwner, GetSchedule, List, ListDependencies, ListDependents, ListEvents,
ListOwned, ListOwnedObjects, Requeue, Update, UpdateByName, Watch, WatchEvents,
WatchList, WatchOwnedObjects, WatchSchedule
```

so "what can I do with events?" means scanning for `Event` across four verbs
instead of reading one block. What is bought back: "how do I read one thing?" is
now the contiguous block, the surface reads uniformly verb-first rather than
switching halfway through a call site, and Go's own libraries read this way.
Twenty-three members is small enough that neither grouping is a search problem —
the argument that carried `NounsVerb` was a forty-method type, and that type
solved it with accessors instead.

`README.md`'s interface listings stay grouped thematically, so they and the
source now differ in member order. Only source follows godoc.

Judgment calls, each with a defensible other answer:

- **`WatchOwnedObjects`, not `WatchListOwnedObjects`.** `WatchList` earns its
  `List` only because its noun is elided; once a plural noun is spoken, `List` is
  redundant. The cost is that the owner-scoped watch no longer looks like a
  sibling of `WatchList` in the name alone.
- **`HasIncomingEdges`.** `Edges` stays the noun for the reason the old record
  chose it: the method folds owned children and live dependents together, and
  neither `Dependents` nor `Owned` covers both. With `Has` in front it finally
  reads as the predicate it is — which the old record named as a thing accessors
  had fixed for `Store` and it could not fix for this surface.
- **The plurals went singular.** `NounsVerb` forced `Conditions`/`Events`/
  `Dependencies` so each family had one prefix. Verb-first makes the verb the
  grouping key, so the noun goes back to matching the argument: one `EventSpec`,
  one `Condition`, one edge, one finalizer.
- **Types were not renamed.** `EventsAddInput`, `ObjectChange`, `EventStream`
  stand. A type name is a noun by definition; this convention is about methods.
- **`Watch` and `WatchList` no longer share a doc comment.** The sort puts
  `WatchEvents` between them, so the shared block would have left `WatchList`
  undocumented. `Watch` keeps the contract and `WatchList` points at it. The
  block was only ever readable in source — godoc has always rendered the two
  apart.
