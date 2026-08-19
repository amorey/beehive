# A name-keyed CreateOrUpdate

- **Status:** Proposed, not decided. Blocked on [the deletion
  refusal](../adr/2026-08-19-a-spec-write-refuses-a-deleting-row.md) and [the
  probe](2026-08-19-a-spec-write-probes-before-it-transacts.md), and to be
  re-argued once they ship — most of the reported pain is theirs, not this
  verb's. If the answer then is no, this becomes a `TODO.md` entry, not a file
  here.
- **Date:** 2026-08-19
- **Issue:** [#126](https://github.com/amorey/beehive/issues/126)

## The gap, as it will stand after the other two

[#126](https://github.com/amorey/beehive/issues/126) asks for one call that
makes a named object hold a spec. Today that is fifteen lines, three of them
about beehive rather than about the caller's domain. The refusal removes the
deletion guard; the probe removes the comparison and the reason for it. What is
left is:

```go
obj, err := client.UpdateByName(ctx, name, spec)
if errors.Is(err, beehive.ErrNotFound) {
    obj, _, err = client.GetOrCreate(ctx, name, spec, beehive.WithOwner(owner))
}
if err != nil {
    return 0, fmt.Errorf("apply cluster identity %s: %w", name, err)
}
return obj.ID, nil
```

Against the issue's own sketch of what it wanted, the delta is one `errors.Is`.
That is the honest measure of what this verb is worth, and it is the reason this
spec is proposed rather than planned.

There is one thing the composition cannot do. Between the `ErrNotFound` and the
`GetOrCreate`, a concurrent create can win; `GetOrCreate` then returns that row
with `created=false` and the caller's spec unapplied, with no error. Unlike the
probe's skip this is a genuine anomaly — it matches no serial order in which the
caller ran.

It is also weak grounds on its own. It self-heals: the system is level-triggered,
so the next pass calls `UpdateByName`, finds the row and sets the spec. And for
the reporting caller it is close to unreachable — the work queue dispatches one
pass per object at a time, so two passes of the same parent cannot race, and two
different parents claiming one child name is a naming bug upstream of beehive.

Nor can the caller close it themselves. `Within` is on `ControllerClient` only
(`controller.go:93`), never on `Client`. A caller inside a pass can borrow its
own, at the cost of holding `BEGIN IMMEDIATE` across both calls on a
single-connection store — which is the cost this whole issue is about. A caller
outside a pass has no option at all.

**So the question to settle before building this is which need is real**: the
line count, or the race. If it is the line count, this verb is right. If it is
the race, `Client.Within` is the better answer — more general, and it serves
every multi-call composition instead of this one.

## The decision, if taken

```go
// CreateOrUpdate makes the object holding name hold spec, creating it if
// absent. created reports which happened.
CreateOrUpdate(ctx context.Context, name string, spec Spec, opts ...Option) (*Object[Spec, Status], bool, error)
```

### Not `Apply`

The issue proposes `Apply`. In a codebase that advertises itself as
Kubernetes-inspired that name is a false cognate: apply there means server-side
apply — field managers, per-field ownership, three-way merge. This replaces the
whole spec. A reader who knows the Kubernetes meaning would be wrong in the
direction that silently clobbers.

`CreateOrUpdate` says what it does, matches the bare-CRUD convention on `Client`
(the receiver is already its kind), and is the name this shipped under before.

### It must probe first, like a plain update

The obvious implementation — one `Within` around resolve-then-write — is
**slower in the steady state than the composition it replaces**, because
atomicity means the lock is taken before anything is known, and the steady state
of an ensure call is that nothing needs doing.

So the shape is the probe's, not the transaction's:

1. Probe lock-free. Row present, not deleting, spec and version already
   matching → return it, `created=false`. This is the steady state and it takes
   no write transaction.
2. Otherwise one `Within`: resolve, and
   - absent → `insertObject` + `signalCreated`, `created=true`;
   - deletion-pending → `ErrDeletionPending`;
   - present → `UpdateSpec` on the resolved id, opts ignored.

Which is why this is sequenced last rather than built first: without the probe
it inherits the slow path, and retrofitting the fast path afterwards means
re-deriving the linearization argument inside a verb that also creates.

### The deletion-pending answer

`ErrDeletionPending`, inherited from the refusal — **not** the issue's own
proposal 3, "a tombstoned row returned untouched".

Proposal 3 hands back an object whose spec is not the one the caller passed,
with nothing to distinguish it from success. `GetOrCreate` gets away with that
shape because `created=false` is a signal; here `created=false` also means "found
and updated", so the two collapse. That is the reporter's original bug
reproduced inside the verb built to prevent it.

## Surface

### `client.go`

On the `Client` interface, between `Create` and `Delete`:

```go
// CreateOrUpdate makes whatever holds name hold spec, creating it if absent;
// created says which. The write and the resolve are one transaction, so a
// concurrent create cannot leave spec unapplied — the difference from composing
// UpdateByName with GetOrCreate.
//
// opts are honoured on create and IGNORED on update, as in GetOrCreate: a
// created=false result says nothing about whether the row matches opts. A
// deletion-pending row is refused with ErrDeletionPending rather than rewritten,
// and its name stays held until GC releases it.
//
// A spec whose bytes already match writes nothing and takes no write
// transaction. Like GetOrCreate's, created is synchronous inside a caller's
// Within — route create-conditional side effects through WithOnCreate.
CreateOrUpdate(ctx context.Context, name string, spec Spec, opts ...Option) (*Object[Spec, Status], bool, error)
```

The implementation shares `insertObject`, `signalCreated` and `signalSpecWritten`
with the verbs it sits between; it introduces no new store call.

### `testutils_test.go`

`fakeStore` gains whatever this path touches, if it is still panicking on it.

## Edge cases the implementer would otherwise guess at

- **`created` distinguishes create from update, not "did anything change".** An
  update that wrote nothing returns `created=false`, same as one that wrote.
  Callers wanting change detection compare `ResourceVersion`. Say so; the two
  readings are easy to conflate and only one is supported.

- **Options on the update branch are ignored, silently.** Mirrors `GetOrCreate`,
  including `WithOwner`: this verb never re-parents. Warning on ignored opts
  would be a different decision, taken for both verbs at once, not here.

- **`checkFinalizersClearable` runs up front**, on both branches, as it does for
  `GetOrCreate` — otherwise the same call passes or fails depending on whether
  the row happens to exist.

- **The decode stays inside the transaction**, so a spec that does not
  round-trip rolls the write back and `created` is false. Same rule as `Create`
  and `GetOrCreate`.

- **`Within` on `Client` is a separate decision.** If it is taken, revisit
  whether this verb is still worth having; it exists largely to supply an
  atomicity a plain `Client` cannot express.

## Tests

In `client_test.go`:

- Absent name → creates, `created=true`, opts honoured (`WithOwner` wires the
  edge, `WithOnCreate` fires).
- Present name → updates, `created=false`, and the owner edge is **unchanged**
  when a different `WithOwner` is passed — pins the ignore.
- Present with identical bytes → no `object_writes` entry, no
  `resource_version` bump.
- Deletion-pending → `ErrDeletionPending`, nothing written.
- Wrong kind holding the name → creates, since the resolve is kind-scoped.
- A create that fails to decode rolls back and reports `created=false`.
- Inside a caller's `Within` that later fails: nothing is committed and
  `WithOnCreate` never fires.
- The race the verb exists for, driven by a store seam rather than by timing: a
  concurrent create injected between resolve and insert must not leave the spec
  unapplied. Without this test the verb has no test for its own reason to exist.

## On ship

- Fold into an ADR that supersedes the "there is no name-keyed upsert" section
  of [name-keyed writes](../adr/2026-07-27-name-keyed-writes.md), and carry
  forward what still governs live code — `Create` and `GetOrCreate` still never
  write to a row they found. The rule narrows to those two rather than
  disappearing.
- `CLAUDE.md`: the "Reconcile is not transactional" bullet says outright that
  there is no name-keyed upsert. Rewrite it, do not append to it.
- `README.md`: line 536's paragraph is largely about the absence of this verb.
- Close [#126](https://github.com/amorey/beehive/issues/126).
