# A name-keyed CreateOrUpdate

- **Status:** Planned.
- **Date:** 2026-08-19
- **Issue:** [#126](https://github.com/amorey/beehive/issues/126)

## The gap

Making a named object hold a spec takes two calls, and there are two ways to
order them. One is correct:

```go
obj, created, err := client.GetOrCreate(ctx, name, spec, beehive.WithOwner(owner))
if err != nil {
    return 0, err
}
if !created {
    obj, err = client.Update(ctx, obj.ID, spec)
}
```

The other loses writes:

```go
obj, err := client.UpdateByName(ctx, name, spec)
if errors.Is(err, beehive.ErrNotFound) {
    obj, _, err = client.GetOrCreate(ctx, name, spec)   // spec unapplied if someone else created it
}
```

`GetOrCreate` is one `Within` around its read and its insert, so the loser of a
concurrent create observes the winner's row — and then writes its spec onto that
id. Start with `UpdateByName` instead and the loser gets `created=false`, no
error, and its spec silently never applied.

Nothing in the API says which ordering is which. `README.md:542` says it in a
sentence, and [#126](https://github.com/amorey/beehive/issues/126) is evidence
that a sentence is not enough. **That is what this verb is for: not the four
lines it saves, but making the correct ordering the only ordering.**

## The decision

```go
// CreateOrUpdate makes the object holding name hold spec, creating it if
// absent. created reports which happened.
CreateOrUpdate(ctx context.Context, name string, spec Spec, opts ...Option) (*Object[Spec, Status], bool, error)
```

One `Within`: `resolveCreate` up front, then resolve by name and

- absent → `insertObject` + `signalCreated`, `created=true`;
- deletion-pending → `ErrDeletionPending`;
- present → `UpdateSpec` on the resolved id, opts ignored.

It introduces no store call and costs what the composition costs.

### Not `Apply`

The issue proposes `Apply`. In a codebase that advertises itself as
Kubernetes-inspired that is a false cognate: apply there means server-side apply
— field managers, per-field ownership, three-way merge. This replaces the whole
spec, so a reader who knows the Kubernetes meaning would be wrong in the
direction that clobbers.

`CreateOrUpdate` says what it does and matches the bare-CRUD convention on
`Client`.

### The deletion-pending answer

`ErrDeletionPending`, inherited from [the
refusal](../adr/2026-08-19-a-spec-write-refuses-a-deleting-row.md) — **not** the
issue's proposal 3, "a tombstoned row returned untouched".

Proposal 3 hands back an object whose spec is not the one the caller passed, with
nothing to distinguish it from success. `GetOrCreate` gets away with that because
`created=false` is a signal; here `created=false` also means "found and updated",
so the two collapse. That is the reported bug reproduced inside the verb built to
prevent it.

## Surface

On the `Client` interface, between `Create` and `Delete`:

```go
// CreateOrUpdate makes whatever holds name hold spec, creating it if absent;
// created says which. The resolve and the write are one transaction.
//
// opts are honoured on create and IGNORED on update, as in GetOrCreate: a
// created=false result says nothing about whether the row matches opts. A
// deletion-pending row is refused with ErrDeletionPending rather than
// rewritten, and its name stays held until GC releases it.
//
// Like GetOrCreate's, created is synchronous inside a caller's Within — route
// create-conditional side effects through WithOnCreate.
CreateOrUpdate(ctx context.Context, name string, spec Spec, opts ...Option) (*Object[Spec, Status], bool, error)
```

`fakeStore` gains whatever this path touches, if it is still panicking on it.

## Edge cases the implementer would otherwise guess at

- **`created` distinguishes create from update, not "did anything change".** An
  update that wrote nothing returns `created=false`, same as one that wrote.
  Callers wanting change detection compare `ResourceVersion`.

- **Options on the update branch are ignored, silently.** Mirrors `GetOrCreate`,
  `WithOwner` included: this verb never re-parents. Warning on ignored opts is a
  different decision, taken for both verbs at once.

- **`resolveCreate` runs before the branch**, so `WithFinalizers` on a kind this
  process cannot clear fails the same way whether or not the row exists.

- **The decode stays inside the transaction**, so a spec that does not round-trip
  rolls the write back and `created` is false. Same rule as `Create`.

## Tests

In `client_test.go`:

- Absent name → creates, `created=true`, opts honoured (`WithOwner` wires the
  edge, `WithOnCreate` fires).
- Present name → updates, `created=false`, and the owner edge is **unchanged**
  when a different `WithOwner` is passed — pins the ignore.
- Present with identical bytes → no `object_writes` entry, no `resource_version`
  bump.
- Deletion-pending → `ErrDeletionPending`, nothing written.
- Wrong kind holding the name → creates, since the resolve is kind-scoped.
- A create that fails to decode rolls back and reports `created=false`.
- Inside a caller's `Within` that later fails: nothing is committed and
  `WithOnCreate` never fires.

## On ship

- Fold into an ADR and narrow the "there is no name-keyed upsert" section of
  [name-keyed writes](../adr/2026-07-27-name-keyed-writes.md) to what still
  governs: `Create` and `GetOrCreate` never write to a row they found.
- `CLAUDE.md`: the "Reconcile is not transactional" bullet says outright that
  there is no name-keyed upsert. Rewrite it, do not append to it.
- `README.md`: the paragraph at line 542 is about the absence of this verb.
- Close [#126](https://github.com/amorey/beehive/issues/126).
