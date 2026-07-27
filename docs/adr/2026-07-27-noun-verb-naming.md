# Methods are named NounsVerb, and every watch returns a change or a subscription

- **Status:** Accepted — implemented across `internal/storeapi`, `client.go`, `controller.go`, `types.go`, and the internals.
- **Date:** 2026-07-27

## Context

The `Store` interface had grown to forty methods named verb-first — `CreateObject`,
`ListIncomingRefs`, `RequestDeletion`, `DecrementPendingWake`. Godoc renders a type's
methods alphabetically, so verb-first scatters one concern across the page: deletion
was `RequestDeletion` under R, `MarkOwnedForDeletion` under M and
`ListAllDeletionPending` under L, and the two halves of the `pending_wake` protocol
sat thirty methods apart. Reading "what can I do to an edge?" meant a full scan.

The stream surface had a second problem. `Watcher`, `EventWatcher` and
`ObjectChangeWatcher` were three interfaces differing only in the name of their single
accessor — `Changes()`, `Events()`, `Batches()` — which forced one generic
implementation to declare all three names over the same channel and rely on each
instantiation being used through only its own interface.

## Decision

**1. `NounsVerbQualifier`.** The noun prefix is plural and names the family the method
belongs to; cardinality lives in the verb (`Get`/`Watch` for one, `List`/`WatchList`
for many). Plural rather than singular so a family has exactly one prefix —
`EdgesAdd`/`EdgesDelete`/`EdgesListIncoming`, not `EdgeAdd` beside `EdgesListIncoming`.

**Omit the prefix when the family is already the receiver's own.** `Client` is its
kind, so `Create`/`Get`/`Update`/`Delete`/`List` stay bare; only its secondary nouns
(events, schedule, relations, the object watches) take one. On `ControllerClient` the
line falls between a **column on the object's row** and a **table of its own**:
`UpdateStatus` stays bare, while conditions, finalizers, events and edges — each its
own table — are prefixed.

`Object`'s relation accessors are the degenerate case: noun-first with no verb left is
Go's accessor idiom, so `GetOwner`/`ListDependencies` became `Owner()`/`Dependencies()`.
The `Get`/`List` cardinality signal moves to the return type, which already carried it.

Exempt: `Err*` values, `With*` options, and anything satisfying an external interface.

**2. A watch over a change stream returns `<-chan NounChange` or a
`*NounsSubscription`** — never a bare `…Watcher` interface. The change
travels by value: `ObjectChange[Spec, Status]` is a type tag plus one pointer, so
copying it costs nothing worth avoiding, and a pointer would add a per-change
allocation and a `nil` case with no meaning on a delivery channel. The
subscription is a pointer because it is a handle with identity and a `Close`. The three
interfaces collapsed into one concrete `storeapi.Subscription[V]` with a single
`Changes()` accessor, aliased per stream (`ObjectsSubscription`, `EventsSubscription`,
`ObjectWritesSubscription`); a backend builds one with `NewSubscription`, and `Close`
is idempotent through a `sync.Once`, so the test doubles no longer carry their own.

Not every watch is over changes, and the rule does not reach the ones that aren't. A
**gauge** (`SchedulesWatch`) and a **log** (`EventsWatch`) stream the value itself:
there is one current `Schedule`, and "what happened to it" is not a question a
consumer can act on — a `ScheduleChange` would carry a `Type` that never varies. The
wrapper earns its place only where the consumer must tell *what happened* from *what
it now is*, which is the test the next section applies to `Event`.

`Subscribe` survives as a distinct verb for exactly one method,
`ObjectWritesSubscribe`. It carries no row — the consumer re-reads current state — and
that is a different contract from a watch that hands you the object, so it gets a
different verb rather than a footnote.

## Consequences

Each surface's members are now sorted alphabetically in source, matching how godoc
renders them, so the file and the doc page agree.

The tax is a verb in the middle on qualified lists (`ObjectsListUnsettledIDs`,
`EdgesListOutgoingByRelation`). The alternative, qualifier-before-verb, reads worse;
it is paid on about six methods.

Four judgment calls worth recording, since each has a defensible other answer:

- **`OwnersGet` for an at-most-one relation.** The prefix names the family (the store
  holds many owners), the verb carries cardinality. Folding all four relations under
  `Edges*` on the client surfaces — `EdgesGetOwner`, `EdgesListDependencies` — would be
  more internally consistent with `Store`, at the cost of the user's vocabulary. The
  relation nouns won on the client; `Store`, which genuinely deals in edges, keeps
  `Edges*`.
- **`ObjectsListByIncomingEdge`** files under `Objects`, away from the `Edges*` family it
  is conceptually adjacent to. It returns objects, not edges, and forcing that question
  is the point — but it is the one method that loses a neighbour.
- **`DeletionRequests*`, and `From` instead of `By` on its cascade.** The family sets
  `deletion_requested_at` and never removes a row — the hard delete is `ObjectsDelete`
  — so naming it `Deletions*` gave one word two families, and the one that deletes
  nothing had the better claim on it. Being a column rather than a table is no
  objection here; `Wakes*` (`objects.pending_wake`) is the same shape. `Pending` then
  drops out of the list method, since a request is only ever cleared by the delete
  itself. The cascade breaks the `By…` qualifier pattern (`BySlug`, `ByRelation`,
  `ByIncomingEdge`, all naming the key you pass) and reads
  `DeletionRequestsCreateFromOwner`: `By` also means *agency* in English, and unlike a
  slug an owner is animate, so `CreateByOwner` invites the wrong reading — the owner
  is what is being deleted, not the actor. `From` keeps the sense of "derived from an
  owner id" and answers the question `CreateOwned` leaves open, whether the owner
  itself gets a request. It does not.
- **`Edges*` for the family, `ObjectRef` for the value.** These were both "ref": the
  store family `Refs*` meant rows in the `refs` table, while the type it returned was
  `Referrer`, aliased publicly as `Ref`. But that type is `{ID, Group, Kind}` — no
  from, no to, no relation — so it cannot express an edge, and `Referrer` was wrong
  about direction besides: `EdgesListOutgoing` returns the objects pointed *at*, and
  `DeletionRequestsList` returns objects with no edge in the picture at all. So the
  value became `ObjectRef` — a reference to an object, the same shape Kubernetes calls
  a reference — and the family became `Edges*`, which is what it always operated on.
  The short public alias `Ref` went with them rather than surviving as a synonym: one
  type wants one name, and every other alias in the package (`GroupKind`, `ObjectID`,
  `Relation`, the subscriptions) already re-exports the internal name unchanged. The
  table itself was renamed `refs` → `edges` in `0001_init.sql` rather than migrated:
  nothing has shipped, so there is no deployed database to carry the old name forward.

Left undone: `EventsSubscription` still carries a bare `Event`, so a consumer cannot
tell a new run from a count-bump on an existing one — the one place a log does face
rule 2's test and fail it. It wants an `EventChange` there (by value, like
`ObjectChange`) and the information would be a genuine improvement, but that is a behavioural
change to a public surface rather than a rename, so it is not part of this work.

This is a breaking change to every public surface; it ships as one release. Downstream
consumers pinning an older tag are unaffected until they bump.
