# Methods are named NounsVerb, and every watch returns a change or a subscription

- **Status:** Accepted — implemented across `internal/storeapi`, `client.go`, `controller.go`, `types.go`, and the internals.
- **Date:** 2026-07-27

## Context

The `Store` interface had grown to forty methods named verb-first — `CreateObject`,
`ListIncomingRefs`, `RequestDeletion`, `DecrementPendingWake`. Godoc renders a type's
methods alphabetically, so verb-first scatters one concern across the page: deletion
was `RequestDeletion` under R, `MarkOwnedForDeletion` under M and
`ListAllDeletionPending` under L, and the two halves of the `pending_wake` protocol
sat thirty methods apart. Reading "what can I do to a ref?" meant a full scan.

The stream surface had a second problem. `Watcher`, `EventWatcher` and
`ObjectChangeWatcher` were three interfaces differing only in the name of their single
accessor — `Changes()`, `Events()`, `Batches()` — which forced one generic
implementation to declare all three names over the same channel and rely on each
instantiation being used through only its own interface.

## Decision

**1. `NounsVerbQualifier`.** The noun prefix is plural and names the family the method
belongs to; cardinality lives in the verb (`Get`/`Watch` for one, `List`/`WatchList`
for many). Plural rather than singular so a family has exactly one prefix —
`RefsAdd`/`RefsDelete`/`RefsListIncoming`, not `RefAdd` beside `RefsListIncoming`.

**Omit the prefix when the family is already the receiver's own.** `Client` is its
kind, so `Create`/`Get`/`Update`/`Delete`/`List` stay bare; only its secondary nouns
(events, schedule, relations, the object watches) take one. On `ControllerClient` the
line falls between a **column on the object's row** and a **table of its own**:
`UpdateStatus` stays bare, while conditions, finalizers, events and refs — each its
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
`RefsListOutgoingByRelation`). The alternative, qualifier-before-verb, reads worse;
it is paid on about six methods.

Two judgment calls worth recording, since both have a defensible other answer:

- **`OwnersGet` for an at-most-one relation.** The prefix names the family (the store
  holds many owners), the verb carries cardinality. Folding all four relations under
  `Refs*` on the client surfaces — `RefsGetOwner`, `RefsListDependencies` — would be
  more internally consistent with `Store`, at the cost of the user's vocabulary. The
  relation nouns won on the client; `Store`, which genuinely deals in edges, keeps
  `Refs*`.
- **`ObjectsListByIncomingRef`** files under `Objects`, away from the `Refs*` family it
  is conceptually adjacent to. It returns objects, not refs, and forcing that question
  is the point — but it is the one method that loses a neighbour.

Left undone: `EventsSubscription` still carries a bare `Event`, so a consumer cannot
tell a new run from a count-bump on an existing one — the one place a log does face
rule 2's test and fail it. It wants an `EventChange` there (by value, like
`ObjectChange`) and the information would be a genuine improvement, but that is a behavioural
change to a public surface rather than a rename, so it is not part of this work.

This is a breaking change to every public surface; it ships as one release. Downstream
consumers pinning an older tag are unaffected until they bump.
