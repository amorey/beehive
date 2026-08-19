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

`checkName`, `json.Marshal` and `resolveCreate` run first, outside the
transaction — the marshal because it would otherwise hold the single
connection's write lock across arbitrary user `MarshalJSON` code, as
`client.update` notes. Then one `Within` around `UpdateSpecByName` and

- it wrote → `created=false`, plus `signalSpecWritten` and `signalKindWritten`
  gated on `changed`, exactly as `client.update` does;
- `ErrNotFound` → `insertObject` + `signalCreated`, `created=true`;
- `ErrDeletionPending` → returned.

Leading with `UpdateSpecByName` rather than resolving first keeps the common
path to one row read, and it is already the atomic resolve-and-write the
composition needs.

It introduces no store call. On the update path it is **one** transaction where
the composition is two — `GetOrCreate`'s and `Update`'s — and on the create path
the two are equal at one. The steady state takes `BEGIN IMMEDIATE`
unconditionally, like every other spec write.
→ [ADR](../adr/2026-08-19-a-spec-write-takes-its-transaction-unconditionally.md)

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

`fakeStore` panics on `UpdateSpec` and `UpdateSpecByName`
(`testutils_test.go:779`, `:783`); both need filling in.

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

- **The update branch owes both wakes.** `signalCreated` fires them for a
  create; the update branch must call `signalSpecWritten` and
  `signalKindWritten` itself, gated on `changed`. Miss the gate and a
  byte-identical write wakes the kind for nothing; miss the calls and a real one
  reaches no watch tailer and no dependency waker.

- **Both branches are in the one transaction**, which is the verb's reason to
  exist. Hoisting the `ErrNotFound` branch out — `UpdateSpecByName` in one
  transaction, the insert in another — rebuilds the race the verb removes. The
  decode-rollback test below is what catches it: a hoisted insert commits before
  the decode fails, so the row survives.

- **It repairs a poison spec, and `GetOrCreate` cannot.** The update branch
  decodes the row `UpdateSpecByName` hands back, which carries the new bytes, so
  a row whose stored spec no longer decodes comes back healthy. `GetOrCreate`'s
  found branch decodes what it read and surfaces the error instead
  (`client.go:457`). A poison *status* still fails, since neither verb writes
  status.

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
- A create that fails to decode rolls back and reports `created=false` — and
  **the row is gone**, which is what pins the insert inside the one transaction.
- Inside a caller's `Within` that later fails: nothing is committed and
  `WithOnCreate` never fires.
- A changed update enqueues the object and wakes the kind; a byte-identical one
  wakes nobody. Nothing else pins the update branch's signals.
- A row whose stored spec no longer decodes is repaired by the update branch,
  where `GetOrCreate` returns the decode error.

## On ship

- Fold into an ADR and narrow the "there is no name-keyed upsert" section of
  [name-keyed writes](../adr/2026-07-27-name-keyed-writes.md) to what still
  governs: `Create` and `GetOrCreate` never write to a row they found.
- `CLAUDE.md`: the "Reconcile is not transactional" bullet says outright that
  there is no name-keyed upsert. Rewrite it, do not append to it.
- `README.md`: the paragraph at line 542 is about the absence of this verb.
- `docs/reconcile-triggers.md`: no new row — both branches reuse
  `signalSpecWritten`/`signalCreated` — but the prose at lines ~246 and ~690
  names `Create` and `GetOrCreate` as the verbs that insert with `generation` 1
  and write the `owned_by` edge inside the insert. Add the create branch to
  both.
- Close [#126](https://github.com/amorey/beehive/issues/126).
