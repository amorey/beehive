# Methods are named NounsVerb, and every watch returns a change or a value

- **Status:** Accepted — implemented across `internal/storeapi`, `client.go`, `controller.go`, `types.go`, and the internals.
- **Date:** 2026-07-27

> **Amended 2026-08-07.** This record still governs `Client` and
> `ControllerClient`. It no longer governs `Store`, where the noun moved from the
> method name onto a type and each family is reached through an accessor
> (`store.Edges().Add`) — which drops the verb-in-the-middle tax and the
> noun-phrase ambiguity recorded in *Consequences* below, and lets
> `Edges().HasIncoming` read as the predicate it is. See
> [the grouped-Store spec](../specs/2026-08-07-grouped-store-api.md).

## Context

The `Store` interface had grown to forty methods named verb-first: `CreateObject`,
`ListIncomingRefs`, `RequestDeletion`, `DecrementPendingWake`. Godoc lists a type's
methods alphabetically, so verb-first scattered each concern across the page. Deletion
sat under R for `RequestDeletion`, M for `MarkOwnedForDeletion` and L for
`ListAllDeletionPending`, and the two halves of one protocol were thirty methods
apart. Answering "what can I do to an edge?" meant reading the whole list.

The stream surface had a second problem. `Watcher`, `EventWatcher` and
`ObjectChangeWatcher` were three interfaces differing only in the name of their single
accessor — `Changes()`, `Events()`, `Batches()` — which forced one generic
implementation to declare all three names over the same channel and rely on each
instantiation being used through only its own interface.

## Decision

**1. `NounsVerbQualifier`.** The prefix is a plural noun naming the family the method
belongs to, and the verb carries cardinality: `Get`/`Watch` for one, `List`/`WatchList`
for many. Plural rather than singular so each family has exactly one prefix —
`Edges().Add`, `Edges().Delete`, `Edges().ListIncoming`, not `EdgeAdd` beside
`Edges().ListIncoming`.

**Omit the prefix when the family is already the receiver's own.** `Client` is its
kind, so `Create`/`Get`/`Update`/`Delete`/`List`/`Watch`/`WatchList` stay bare; only
its secondary nouns (events, schedule, relations) take one. See the amendment below:
the object watches were listed here as a secondary noun until 2026-08-02. On `ControllerClient` the
line falls between a **column on the object's row** and a **table of its own**:
`UpdateStatus` stays bare, while conditions, events and edges — each its own table —
are prefixed. Finalizers are listed here too, and that is wrong about the schema:
`finalizers` is a column on `objects`. The `ControllerClient` name stands, but the
distinction it is drawn from does not reach it — which is why the store side is
`Objects().DeleteFinalizer`.

`Object`'s relation accessors are the degenerate case: noun-first with no verb left is
Go's accessor idiom, so `GetOwner`/`ListDependencies` became `Owner()`/`Dependencies()`.
The `Get`/`List` cardinality signal moves to the return type, which already carried it.

Exempt: `Err*` values, `With*` options, and anything satisfying an external interface.

One family takes a **singular** prefix: `ReconcileOwed*`, over the
`objects.reconcile_owed` column. The rule exists to give each family one prefix, which
a singular does just as well here, and this family fronts a scalar count whose name it
should match. `ReconcilesOwed*` would satisfy the letter of the rule at every call
site, and `OwedReconciles*` only reads as a noun by inverting the column name. Recorded
rather than left to the reader, because undocumented exceptions are how a convention
erodes.

**2. A watch over a change stream returns `<-chan NounChange`** — never a bare
`…Watcher` interface. The change travels by value: `ObjectChange[Spec, Status]` is a
type tag plus one pointer, so copying it costs nothing worth avoiding, and a pointer
would add a per-change allocation and a `nil` case with no meaning on a delivery
channel. The channel is the whole handle — `ctx` cancellation ends the stream and
closes it — so there is no subscription object to name, hold, or `Close`.

Not every watch is over changes, and the rule does not reach the ones that aren't. A
**gauge** (`SchedulesWatch`) and a **log** (`EventsWatch`) stream the value itself:
there is one current `Schedule`, and "what happened to it" is not a question a
consumer can act on — a `ScheduleChange` would carry a `Type` that never varies. The
wrapper earns its place only where the consumer must tell *what happened* from *what
it now is*, which is the test the next section applies to `Event`.

The store has no streams at all, so no method there takes a stream verb: the write
log reads as an ordinary listing, `ObjectWrites().ListSince` / `ObjectWrites().MaxVersion`
— plain `List`/`Get` verbs under one noun.

## Consequences

Each surface's members are now sorted alphabetically in source, matching how godoc
renders them, so the file and the doc page agree.

The tax is a verb in the middle on qualified lists (`Objects().ListUnsettledIDs`,
`Edges().ListOutgoingByRelation`). The alternative, qualifier-before-verb, reads worse;
it is paid on about six methods.

Five judgment calls worth recording, since each has a defensible other answer:

- **`OwnersGet` for an at-most-one relation.** The prefix names the family (the store
  holds many owners), the verb carries cardinality. Folding all four relations under
  `Edges*` on the client surfaces — `EdgesGetOwner`, `EdgesListDependencies` — would be
  more internally consistent with `Store`, at the cost of the user's vocabulary. The
  relation nouns won on the client; `Store`, which genuinely deals in edges, keeps
  `Edges*`.
- **`Objects().ListByIncomingEdge`** files under `Objects`, away from the `Edges*` family it
  is conceptually adjacent to. It returns objects, not edges, and forcing that question
  is the point — but it is the one method that loses a neighbour.
- **`DeletionRequests*`, and `From` instead of `By` on its cascade.** The family sets
  `deletion_requested_at` and never removes a row — the hard delete is `Objects().Delete`
  — so naming it `Deletions*` gave one word two families, and the one that deletes
  nothing had the better claim on it. Being a column rather than a table is no
  objection here; `ReconcileOwed*` (`objects.reconcile_owed`) is the same shape.
  `Pending` then drops out of the list method, since a request is only ever cleared by
  the delete itself — and out of `ReconcileOwed().ListIDs` for the same reason, the family
  name already saying the row owes something. The cascade breaks the `By…` qualifier pattern (`ByName`, `ByRelation`,
  `ByIncomingEdge`, all naming the key you pass) and reads
  `DeletionRequests().CreateFromOwner`: `By` also means *agency* in English, and unlike a
  name an owner is animate, so `CreateByOwner` invites the wrong reading — the owner
  is what is being deleted, not the actor. `From` keeps the sense of "derived from an
  owner id" and answers the question `CreateOwned` leaves open, whether the owner
  itself gets a request. It does not.
- **`Edges*` for the family, `ObjectRef` for the value.** These were both "ref": the
  store family `Refs*` meant rows in the `refs` table, while the type it returned was
  `Referrer`, aliased publicly as `Ref`. But that type is `{ID, Group, Kind}` — no
  from, no to, no relation — so it cannot express an edge, and `Referrer` was wrong
  about direction besides: `EdgesListOutgoing` returns the objects pointed *at*, and
  `DeletionRequests().List` returns objects with no edge in the picture at all. So the
  value became `ObjectRef` — a reference to an object, the same shape Kubernetes calls
  a reference — and the family became `Edges*`, which is what it always operated on.
  There is no public `Ref` alias beside it: one type wants one name, and every other
  alias in the package (`GroupKind`, `ObjectID`, `Relation`, `ChangeType`)
  re-exports the internal name unchanged.
- **`Events().Add`, not `EventsRecord`.** The rule fixes the shape but not the verb, and
  `EventsRecord` satisfied it — `Record` also said the true thing, that a call may
  extend the latest run instead of inserting a row. It lost on grammar. The verb slot
  in `NounsVerb` is read as a verb only by habit: `Objects().List` is also "a list of
  objects" and `Conditions().Set` "a set of conditions", and what disarms those is that
  `List` and `Set` appear dozens of times each. `Record` appeared once, so it got no
  such help and read as a noun in the register of `EventSpec` and `ObjectRef` — a
  method named like a type. `Add` is a verb only, and it is already this package's
  verb for writing to a table of its own (`Edges().Add`, `DependenciesAdd`), which is
  the same category the prefix rule above puts events in.

  What `Add` gives up is that it overclaims: the common call extends a run and
  inserts nothing. That is tolerable because the surface already reads its write
  verbs as intent rather than as a row count — `DeletionRequests().Create` documents
  that "repeat calls do nothing", and `Edges().Add` stamps only when the edge was new.
  The aggregation moved into the doc comments on both `Events().Add` declarations, which
  is where a caller needs it in prose anyway. `EventsEmit` and `EventsObserve` are
  the other verb-only candidates and both mislead: nothing is pushed here, and
  "observe" is spoken for by the generation handshake.

Left undone: `EventsWatch` still streams a bare `Event`, so a consumer cannot
tell a new run from a count-bump on an existing one — the one place a log does face
rule 2's test and fail it. It wants an `EventChange` there (by value, like
`ObjectChange`) and the information would be a genuine improvement, but that is a behavioural
change to a public surface rather than a rename, so it is not part of this work.

The convention covers every public surface, so a new method has exactly one
defensible name and reviewers argue about the noun rather than the shape.

## Amendments

**2026-08-02 — the object watches lost their prefix.** `ObjectsWatch` and
`ObjectsWatchList` became `Watch` and `WatchList`. Listing them as a secondary noun
was the error: they stream the client's own kind, so the omit-the-prefix rule reaches
them exactly as it reaches `List`. Bare `Watch` beside `EventsWatch` reads the way
bare `List` already reads beside `Events().List`, and the `Get`/`List` cardinality
pairing returns. The `Object` noun stays where it carries information — the return
types `ObjectSnapshot`, `ObjectListSnapshot` and `ObjectChange`.
