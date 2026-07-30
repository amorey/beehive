# Secondary lookups: owner / dependencies / dependents / owned

- **Status:** Accepted — implemented in `client.go`, `types.go`, `sqlite/store.go`.
- **Date:** 2026-07-27 (recorded retroactively)

## Context

Edges are read on request, never folded into the `SELECT` that carries the object's
spec and status. A one-to-many join would repeat that JSON once per edge.

## Decision: two paths, one set of loaders

- **Eager** — `LoadOption`s passed to `Get`, `GetByID`, `List` or
  `OwnedObjectsList`. `resolveLoads` folds them into a `LoadSet` bitset;
  `loadObjectRelated` handles one object and `loadListRelated` a list, running one
  batched query per relation rather than one per object.
- **Lazy** — `Client.OwnersGet` / `DependenciesList` / `DependentsList` /
  `OwnedList`, and the same four on `ControllerClient`, since a `Reconcile` has no
  read call to hang options off.

Both run the same query. Eager just attaches the result to the object and batches it
across a list.

The store primitives are `EdgesListOutgoingByRelation` for a single object, and
`EdgesGroupOutgoingByID` / `EdgesGroupIncomingByID` for many. The batched pair return
`map[id][]ObjectRef` rather than a slice, which is why they are named `Group…ByID`
rather than `List…`; one `edgesByIDs` helper serves both, with the two columns
swapped. The unfiltered `EdgesListOutgoing` stays for GC.

There is no option to set default loads. Per-call plus lazy covers every case without
leaving a way to pay for queries nobody used.

## `owned` is the inverse of `owner`

`OwnersGet` / `LoadOwner` read the *outgoing* `owned_by` edge
(`EdgesListOutgoingByRelation` / `EdgesGroupOutgoingByID`).
`OwnedList` / `LoadOwned` / `Object.Owned` read the *incoming* `owned_by` edges
(`EdgesListIncoming` / `EdgesGroupIncomingByID`) — the owner's children — exactly as
`dependents` inverts `dependencies` over `depends_on`.

`owner` is single (`WithOwner` sets one), so `fetchOwnerRef` takes the first
`owned_by` edge; `owned` is naturally many.

## Accessors gate on what was loaded

The related data lives in **unexported** `Object` fields (`owner` / `dependencies` /
`dependents` / `owned`); callers reach it only through the accessors.
`Object.loaded` (a `LoadSet`) records what was fetched; the accessors return
`ErrNotLoaded` when the relation wasn't requested — so a forgotten `Load*()` fails
loudly instead of looking empty.

**The return type carries cardinality**, so the `Object` accessors need no verb at
all (they are bare nouns — `Owner()`, `Dependencies()`) while the `Client` /
`ControllerClient` lazy lookups spell the same relations with one (`OwnersGet`,
`DependenciesList`):

- `Owner() (ObjectRef, bool, error)` — bool = owner present, folding away ownerless;
  err = not loaded.
- `Dependencies()` / `Dependents()` / `Owned() ([]ObjectRef, error)` — loaded-empty is an
  empty slice plus nil err; no bool, since not-loaded is an error now, not an empty.

## `Client.OwnedObjectsList` is the typed counterpart of `OwnedList`

It returns the decoded `[]*Object[Spec, Status]` children of *this client's kind*,
where `OwnedList` returns untyped refs across every owned kind.

It is deliberately not a fifth lazy lookup. The kind filter and the row read fold
into one store primitive, `ObjectsListByIncomingEdge(gk, toID, relation)`, so neither
the Go-side `ref.Kind` filter nor the `Get` per child that the untyped shape forces on
callers ever happens. Its contract otherwise follows `OwnedList`'s, and it takes
`List`'s `LoadOption`s through the same `loadListRelated` — a list read whose children
could not be eager-loaded would only push the per-child `Get` down a level.

It cannot have a `ControllerClient` twin: `ControllerClient[Status]` has no `Spec`
parameter, so `[]*Object[Spec, Status]` cannot be written there. That is exactly why
that surface's four lookups return untyped refs.

## The store's two multi-row object reads share one predicate seam

`listObjectsWhere(tail, args…)` runs the caller's WHERE fragment **once**, in the
`SELECT` that carries the blobs. The batched conditions read — `conditionsByIDs`,
chunked under `idChunkSize` like `edgesByIDs` — then keys off the ids that returned.

Running the predicate a second time for the conditions would be a skew bug rather than
a shared seam. The two statements are not in one transaction, so a write landing
between them could drop the conditions of a row already scanned. Keying off the ids
also avoids paying for the edges semi-join twice, and an empty result skips the second
round trip entirely.

`ObjectsList` supplies a kind tail. `ObjectsListByIncomingEdge` supplies a kind tail
plus `o.id IN (SELECT from_id FROM edges …)` — a **semi-join, not a join**. Written as
a join, the planner drives from `idx_objects_kind`, which already satisfies
`ORDER BY o.id`, and probes `edges` once per object *of the kind*. Written as
`IN (SELECT …)`, `idx_edges_to` drives instead, so the work scales with the owner's
children rather than with the table.
