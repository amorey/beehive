# Secondary lookups: owner / dependencies / dependents / owned

- **Status:** Accepted — implemented in `client.go`, `types.go`, `sqlite/store.go`.
- **Date:** 2026-07-27 (recorded retroactively)

## Context

ObjectRef edges are read on request, never folded into the object's blob-bearing
`SELECT` — a one-to-many join would re-send the spec/status JSON per edge.

## Decision: two paths, one set of loaders

- **Eager** — per-call `LoadOption`s on `Get` / `GetBySlug` / `List` /
  `OwnedObjectsList` (`resolveLoads` → `LoadSet` bitset; `loadObjectRelated` for
  one object, `loadListRelated` for a `List` — one batched `…ByIDs` query per
  relation, not per object).
- **Lazy** — `Client.OwnersGet` / `DependenciesList` / `DependentsList` /
  `OwnedList`, and the same quartet on `ControllerClient`, since a `Reconcile` has
  no read call site to pass options to.

Both issue the same secondary query; eager just attaches the result and batches
across a list.

Store primitives: the relation-filtered `EdgesListOutgoingByRelation` (single) and
the batched `EdgesGroupOutgoingByID` / `EdgesGroupIncomingByID` (returning a
`map[id][]ObjectRef`, not a slice — hence `Group…ByID`, not `List…`; one shared
`edgesByIDs` helper, routeCol/joinCol swapped). The unfiltered `EdgesListOutgoing`
stays for GC.

There is no standing default-loads option: per-call plus lazy cover every case
without a "queries you didn't use" footgun.

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

It is deliberately not a fifth lazy ref lookup: the kind filter and the row read
fold into one store primitive, `ObjectsListByIncomingEdge(gk, toID, relation)`, so the
Go-side `ref.Kind` filter and the `Get`-per-child the untyped shape forces on
callers never happen. Its contract otherwise tracks `OwnedList`'s (see the godoc),
and it takes `List`'s `LoadOption`s through the same `loadListRelated` — a list read
whose children can't be eager-loaded would just push the per-child `Get` back one
level.

It has no `ControllerClient` twin because it can't: `ControllerClient[Status]`
carries no `Spec` parameter, so `[]*Object[Spec, Status]` is inexpressible there —
which is exactly why that surface's quartet is untyped refs.

## The store's two multi-row object reads share one predicate seam

`listObjectsWhere(tail, args…)`: the blob-bearing `SELECT` runs the internal WHERE
fragment **once**, and the batched conditions read (`conditionsByIDs`, chunked under
`idChunkSize` like `edgesByIDs`) keys off the ids it returned.

Re-running the predicate for the conditions half would be a skew bug, not a shared
seam — the two statements aren't in one transaction, so a concurrent ref/object
write between them could drop the conditions of a row already scanned. Keying off
the ids also avoids paying the edges semi-join twice. An empty result skips the
conditions round-trip.

`ObjectsList` supplies the kind tail; `ObjectsListByIncomingEdge` a kind tail plus
`o.id IN (SELECT from_id FROM edges …)` — a **semi-join, not a join**. Written as a
join, the planner drives from `idx_objects_kind` (which already satisfies
`ORDER BY o.id`) and probes `edges` once per object *of the kind*; `IN (SELECT …)`
lets `idx_edges_to` drive, so the work scales with the owner's children instead of
the table.
