# A reverse dependency index

- **Status:** Planned.
- **Date:** 2026-08-20
- **Depends on:** [enforcement](2026-08-20-enforce-one-process-one-beehive.md).
  An index that does not know about another process's edges drops wakes silently.

## Why

Every page the dependency waker reads costs one edges query
(`waker.go:602`):

```go
byTarget, err := dw.bh.store.Edges().GroupIncomingByID(ctx, ids, RelationDependsOn)
```

It asks "who depends on any of these objects?" for every object that was written.
Most objects are nobody's dependency, so most of those queries return nothing.

`depends_on` edges change in four places, all in this process:
`Edges().Add`, `Edges().Delete`, `Edges().DeleteFinalizingDependsOn`, and the
foreign key cascade when a row is physically deleted.

## The change

`Beehive` holds the reverse index:

```go
// dependents maps a target to the objects that depend on it. Edges only — no
// payloads — so the memory is the graph, not the store.
dependents map[ObjectID][]ObjectRef
```

Built at `Start` from one scan of `edges`, maintained by the four writers above,
and read by `dependentsWake` in place of the query.

## The rule this rests on

**Every edge writer must publish.** Three of the four are explicit store calls
and easy to wire. The fourth is a foreign key cascade — `edges.from_id` is
`ON DELETE CASCADE`, so deleting an object silently removes its outgoing edges,
and the index has to learn about it from `objectsDelete` rather than from the
edge table.

**A missed removal wakes a dead object**, which is harmless: the reconcile finds
`ErrNotFound` and treats it as a no-op success. **A missed addition drops a wake**,
which is not harmless, but the stale-dependents pass still finds it within its
cadence — that pass reads current state and knows nothing about this index. So the
index is an optimization over a backstop, exactly as the waker's own cursor is.

Say that in the ADR. It is what makes the index safe to build.

## Edge cases the implementer would otherwise guess at

- **Self-edges are skipped.** `dependentsWake` already skips `from_id == to_id`,
  and `ListStaleSince` filters them too. Keep them out of the index entirely, so
  the skip cannot be forgotten at one of two read sites.

- **The build at `Start` needs a store call that does not exist.** `Edges()` has
  no "list every `depends_on` edge". Add one, scoped to the relation, and page it:
  a large graph should not be one result set.

- **The build races the first writes.** `Start` already takes the waker's
  subscription and watermark in order, so a write cannot land below the mark or
  go unheard. Build the index inside that same window, before `Start` returns.

- **Memory is the edge count, not the object count.** Two ids and a group/kind
  per edge. Worth stating: a reader will ask, and the answer is what makes this
  different from caching objects.

- **A client-only kind's objects can be targets.** The waker is store-wide by
  design. Index every edge, not only the ones whose dependent has a controller.

## Tests

In `waker_test.go`:

- A page of writes to objects nobody depends on issues no edges query.
- A page containing a target with dependents wakes them, with no query.
- An edge added during a scan is visible to the next page.
- An object collected mid-scan does not wake a dead dependent into anything worse
  than a no-op.
- The FK cascade case: physically delete an object with outgoing edges and assert
  the index no longer holds them. **Without this test the cascade goes unwired and
  nothing fails visibly.**
- A fresh beehive builds the index from the store and wakes correctly on its
  first commit.

## On ship

ADR: **the waker reads a reverse index, not the edge table**, recording the four
writers, the cascade trap, and the "optimization over a backstop" framing.

`CLAUDE.md`'s waker bullet describes the write-log scan. That is unchanged; what
changes is what the scan does with a page.
