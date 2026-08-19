# A name-keyed CreateOrUpdate

- **Status:** Accepted — implemented in `client.go` (`CreateOrUpdate`).
- **Date:** 2026-08-19

## Context

Making a named object hold a spec took two calls, and there were two ways to
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

Nothing in the API said which ordering was which. The README said it in a
sentence, and [#126](https://github.com/amorey/beehive/issues/126) is evidence
that a sentence is not enough.

## Decision

`Client.CreateOrUpdate(ctx, name, spec, opts...) (*Object, bool, error)`.

**Its reason to exist is not the four lines it saves, but making the correct
ordering the only ordering.** That is why the two branches share one
transaction, and why splitting them would give back exactly what the verb
removes.

`checkName`, `json.Marshal` and `resolveCreate` run outside the transaction —
the marshal because it would otherwise hold the single connection's write lock
across arbitrary user `MarshalJSON` code. Then one `Within` around
`UpdateSpecByName` and, on its `ErrNotFound`, `insertObject`.

Leading with the spec write rather than a resolve keeps the common path to one
row read, and it inherits `ErrDeletionPending` from [the
refusal](2026-08-19-a-spec-write-refuses-a-deleting-row.md) for free. The update
branch sends the same two wakes `client.update` sends, on the same `changed`
gate; the create branch reuses `signalCreated`.

### Not `Apply`

The issue proposed `Apply`. In a codebase that advertises itself as
Kubernetes-inspired that is a false cognate: apply there means server-side apply
— field managers, per-field ownership, three-way merge. This replaces the whole
spec, so a reader who knows the Kubernetes meaning would be wrong in the
direction that clobbers.

### The deletion-pending answer

`ErrDeletionPending`, not the issue's proposal 3, "a tombstoned row returned
untouched". Proposal 3 hands back an object whose spec is not the one the caller
passed, with nothing to distinguish it from success. `GetOrCreate` gets away with
that because `created=false` is a signal; here `created=false` also means "found
and updated", so the two collapse — the reported bug reproduced inside the verb
built to prevent it.

## Consequences

`Create` and `GetOrCreate` still never write to a row they found; that rule
narrows to them rather than disappearing.

**It repairs a poison spec, which `GetOrCreate` cannot.** The update branch
decodes the row the write hands back, carrying the new bytes, so a row whose
stored spec no longer decodes comes back healthy. `GetOrCreate`'s found branch
decodes what it read and surfaces the error. A poison *status* still fails, since
neither verb writes status.

On the update path it is one transaction where the composition is two
(`GetOrCreate`'s and `Update`'s); on the create path the two are equal at one.
The steady state takes `BEGIN IMMEDIATE` unconditionally, like every other spec
write. → [ADR](2026-08-19-a-spec-write-takes-its-transaction-unconditionally.md)

`created` distinguishes create from update, not "did anything change": an update
that wrote nothing returns `created=false`, same as one that wrote. Callers
wanting change detection compare `ResourceVersion`. Options are honoured on
create and ignored on update, `WithOwner` included, so the verb never
re-parents — warning on ignored opts would be a decision for both verbs at once.
