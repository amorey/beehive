# The id is the `Client` API's key; the name resolves through `…ByName` siblings

- **Status:** Accepted — implemented in `client.go`, `README.md`.
- **Date:** 2026-08-02
- **Supersedes:** "The name is the `Client` API's key; the id is the store's key"
  (2026-07-30), which keyed the bare CRUD verbs on the name.

## Context

The 2026-07-30 record made the name the key of the user-facing CRUD surface.
`Get`, `Update` and `Delete` took a name. `GetByID`, `UpdateByID` and `DeleteByID`
took an id. That change removed a real problem: a caller had to hold an id to do
ordinary work, and the name column was nullable.

It also created two problems.

**The API became asymmetric.** Only CRUD moved to the name. The rest of `Client`
stayed keyed on the id: `Watch`, `Requeue`, `SchedulesGet`, `SchedulesWatch`,
`Events().List`, `Events().GetLatest`, `EventsWatch`, `OwnersGet`, `DependenciesList`,
`DependentsList`, `OwnedList` and `OwnedObjectsList`. These calls address rows in
`edges` and `events`, or entries in the work queue. All of those are keyed on
`objects.id`, and the 2026-07-30 record keeps them that way for ABA safety and for
key width in the scan-hot tables. Thus a caller read one key from CRUD and needed
the other key for everything else.

**The ABA rule was documentary, not structural.** A name can be reused. A delete
and a recreate can put a different incarnation behind the same name. The record
answered this with a rule: read-modify-write must go through `UpdateByID`. Nothing
enforced the rule. `Get` followed by `Update` compiled, and it was wrong.

## Decision

Key the bare CRUD verbs on the id. Provide `GetByName`, `UpdateByName` and
`DeleteByName` as the name-keyed siblings.

- `Get`, `Update` and `Delete` take an `ObjectID`.
- `GetByName`, `UpdateByName` and `DeleteByName` take a name.
- `Create` and `GetOrCreate` keep the name as a positional argument. This is not
  an inconsistency. A create has no id to take, because the id does not exist
  until the row does.
- `List`, the watches, the graph lookups and the observability calls are
  unchanged. They were already id-keyed.

The ABA contract does not change. Only the default changes.

> A **name-keyed** call acts on whatever holds that name *now*, or reports
> absence. An **id-keyed** call acts on that one incarnation, or returns
> `ErrNotFound`.

The bare verbs now restore the property the pre-2026-07-30 README stated:
everything after the create takes an `ObjectID`, so a delete and recreate under
the same name cannot make a caller act on the wrong row. The safe call is the
call a caller reaches for first. `ByName` is the deliberate opt-out, which is the
correct shape for a hazard.

### Read-modify-write needs no rule

Every read returns an `*Object` that carries `ID`. Thus the natural way to write
a mutation back is `Update(ctx, obj.ID, spec)`. The unsafe composition —
`GetByName`, mutate, `UpdateByName` — must now be written deliberately. The
README states the hazard; the type signatures no longer invite it.

### The level-triggered case still holds

The 2026-07-30 record argued that "ensure this child exists" and "remove this
child" are statements about a name. They must re-evaluate against current state
on every reconcile. That argument is correct, and the `ByName` siblings serve it.
`GetOrCreate` also returns the object, so a controller holds an id for every call
after the create. Thus the cost of this change to a controller is keystrokes, not
semantics.

### Nothing below `Client` changes

`storeapi.Store` already exposes `Objects().GetByName`, `Objects().UpdateSpecByName` and
`DeletionRequests().CreateByName`. The 2026-07-30 record kept those names because
there the id really is the key and the name really is the qualified variant. This
change makes `Client` agree with the store instead of inverting it. Each `ByName`
method maps to the store method of the same name. No schema, no migration, no
store interface change.

Atomicity is unchanged. A `ByName` write resolves and writes inside one
transaction, never two store calls. `Objects().UpdateSpecByName` still reads before
it writes, because the generation handshake compares stored bytes against the
incoming bytes. `Within` supplies the atomicity, not the statement count.

### Watch follows one incarnation

`Watch` godoc promised that an id holding nothing yet reports a nil
`ObjectSnapshot.Object`, and that "its creation arrives as `Added`". That promise
cannot hold. An id is minted by a create that already happened. Ids are
`AUTOINCREMENT` and are never reused, so an id that holds nothing will never come
to hold anything.

The godoc now states the true contract. `Watch` follows one incarnation. A
recreate under the same name is a different id. Thus the stream ends at `Deleted`.

A name-keyed `Watch` was rejected. It would have to re-resolve the name after
each delete, and the poll filters write-log entries by id before the batched
`Objects().ListByIDs` read. Filtering by name instead would make a single-object
watch pay for the whole kind's churn. A name-keyed watch also could not state a
gap-free `ResourceVersion` contract across the re-resolve.

### What is not re-keyed

Nothing below `Client`, for the reasons the 2026-07-30 record gives and this one
keeps:

- **ABA safety.** `objects.id` is `AUTOINCREMENT`, so ids are never reused. Stale
  edge targets are impossible by construction. A name-keyed edge would let a
  recreate adopt the previous incarnation's edges. The surviving
  `dependency_watermarks` row was measured against the old incarnation's cursor,
  so the stale-dependents pass would read converged for a dependency it never
  reconciled against. That is permanent divergence.
- **Key width in the scan-hot tables.** An id is a varint of 1 to 3 bytes. A name
  key is `(group, kind, name)`, which is tens of bytes, on every row of `edges`
  (twice), `conditions`, `events` and `dependency_watermarks`. The 1s waker and
  the 60s stale-dependents pass scan these tables.
- **Two storage tricks.** `dependency_watermarks.object_id INTEGER PRIMARY KEY`
  aliases the rowid. `idx_objects_deleting` plans as a plain index scan with no
  row fetch and no sort. A text key forfeits both.

### The name rules are unchanged

The name is required, immutable and opaque. A rename is delete plus recreate.
`""` is rejected with `ErrInvalidName`, by the writes and the reads alike, and the
store is what enforces it. `Store` is a public extension point, and a row admitted
under `""` is one no name-keyed call can address again. `Client` still rejects it
up front, so the reads do not answer `ErrNotFound` for a bad argument.

A taken name reports `ErrNameTaken`, tombstones included, because a
deletion-pending row still holds the name's `UNIQUE` constraint.
`GenerateName(prefix)` builds a name from a UUIDv7, and callers bound-retry on the
sentinel.

## Consequences

- **The public API breaks again**, one PR after the previous break. This is
  acceptable only because the project is undeployed. Do not take a third pass at
  this surface without a stronger reason than ergonomics.
- **The asymmetry is gone.** One key addresses the whole `Client` surface. The
  `ByName` trio is the only exception, and it is a lookup, not a second keying.
- **`Watch` has no name form.** A caller that holds only a name must call
  `GetByName` first. Under an id-keyed surface this is consistent rather than an
  outlier.
- **`ObjectRef` still carries no `Name`.** The graph lookups return
  `{ID, Group, Kind}`. A caller that wants to display a dependent must read it.
  This is less costly under an id-keyed surface, because the ref's id feeds
  straight back into `Get`. Left as is.
- **Test names were inverted with the API.** `TestClientGetByID` is now
  `TestClientGet`, and the former `TestClientDelete` is now
  `TestClientDeleteByName`. `TestClientNameKeyedWritesFollowTheNameAcrossARecreate`
  keeps its name and still pins both halves of the ABA contract.
