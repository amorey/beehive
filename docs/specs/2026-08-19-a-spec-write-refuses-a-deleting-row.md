# A spec write refuses a deletion-pending row

- **Status:** Planned.
- **Date:** 2026-08-19
- **Issue:** [#126](https://github.com/amorey/beehive/issues/126)

## The gap

`updateSpec` (`sqlite/store.go:1373`) resolves the row and writes it. Nothing
looks at `deletion_requested_at`, so `Client.Update` and `Client.UpdateByName`
write the spec of an object being torn down.

The write lands and is then discarded. Generation bumps, the row goes unsettled
and is enqueued, and the pass that picks it up sees `deleting` and takes the
`gcCollect` path instead of reconciling. The spec is never applied, and the row
is collected. The caller is told it succeeded.

It is not free either. The write appends to `object_writes`, so it wakes the
kind's watch tailers and the dependency waker for a change nothing will act on.

Today the whole burden sits on the caller, and `README.md:536` says so outright:
a caller composing ensure-then-set "should think about the deletion-pending row
before it does". [#126](https://github.com/amorey/beehive/issues/126) is the
report of someone who did not, and could not have been expected to — the guard
is three lines about beehive in the middle of code about clusters.

## The decision

A spec write to a deletion-pending row writes nothing and returns
`ErrDeletionPending`.

No caller is served by the current behaviour. Spec is app-written and status is
controller-written, so the only use for writing a dying row's spec is stashing
something for that object's own finalizer to read — which crosses the
spec/status boundary the wrong way, and has `AddEvent` for it.

### Why not fold it into `ErrNotFound`

Because the recoveries are opposites, and the caller must be able to tell them
apart:

- `ErrNotFound` — nothing holds the name. **Create it.**
- `ErrDeletionPending` — a dying row holds the name. **You cannot create it**:
  the name is still under `UNIQUE`, so a create answers `ErrNameTaken` until GC
  releases it. Wait.

That distinction is exactly what `GetOrCreate`'s `created=false` flattens, and
flattening it is what produced the reported bug.

### What the caller does with it

For an owned child — the reporting caller's shape — the handler is a bare
`return nil`. The parent will be re-run when the child is finally collected,
because [a physical delete pushes its
owner](../adr/2026-08-05-a-physical-delete-pushes-its-owner.md), and the create
of the replacement happens on that pass. So the sentinel reports "not yet", with
the retry already arranged by machinery that exists. Say this in the godoc; it
is the difference between a sentinel a caller handles and one a caller fears.

### What an unhandled sentinel does, and why this is breaking

Today the call returns success. After this, a caller that does not handle the
sentinel propagates it out of `Reconcile` as a `Fail`, and the object enters the
retry ladder — on a condition that clears on someone else's schedule, and that
can persist indefinitely behind a finalizer nobody clears. So the failure mode of
not upgrading is a hot loop against a wall, not a lost write.

That is still better than the silent discard it replaces, and it is loud rather
than quiet, which is the direction to err. But it means this ships as `feat!` per
the `CLAUDE.md` commit convention, and both the godoc and the ADR have to state
the unhandled behaviour rather than only the recommended handler.

## Surface

### `internal/storeapi/storeapi.go`

Beside the other sentinels:

```go
// ErrDeletionPending is returned by a write to an object whose deletion has
// been requested. Distinct from ErrNotFound: the row is still there and still
// holds its name, so a create answers ErrNameTaken until GC releases it.
var ErrDeletionPending = errors.New("beehive: object is deletion-pending")
```

The `Objects` interface godoc in this file is the contract every `Store`
implementation is written against, and `README.md` advertises `Store` as a
public extension point that enforces its own rules — so a guarantee stated only
in `sqlite` is not a guarantee. `UpdateSpec` and `UpdateSpecByName` (lines
767–780) each gain a sentence, in the shape `UpdateSpecByName`'s existing "An
implementation MUST resolve and write in one transaction" already sets:

> A deletion-pending row is refused with `ErrDeletionPending`, before the schema
> version is stamped and before the byte compare, and nothing is appended to the
> write log.

Both the ordering and the no-append clause are load-bearing, not prose: [the
probe](2026-08-19-a-spec-write-probes-before-it-transacts.md) makes this refusal
the first check in `specWriteSettled`, which is sound only if it is also the
transaction's first check.

### `types.go`

Re-exported beside `ErrNameTaken`:

```go
// ErrDeletionPending is returned by Update and UpdateByName when the object is
// being torn down. The write is refused rather than applied: a pass on a
// deleting row runs collection, not reconcile, so the spec would be discarded.
//
// Distinct from ErrNotFound, because the answers differ — ErrNotFound means
// create it, ErrDeletionPending means you cannot, since the name stays held
// until GC releases it. A caller whose object is owned can treat it as "not
// yet" and do nothing: a physical delete pushes its owner, so the owner's next
// pass creates the replacement.
var ErrDeletionPending = storeapi.ErrDeletionPending
```

### `sqlite/store.go`

One check in `updateSpec`, immediately after `resolve`:

```go
obj, err := resolve(ctx)
if err != nil {
    return err
}
// A pass on a deleting row runs collection, not reconcile, so a spec written
// here is never applied — and it would still wake every watcher and dependent.
if obj.DeletionRequestedAt != nil {
    return fmt.Errorf("%w: object %d", storeapi.ErrDeletionPending, obj.ID)
}
```

**Before `stampVersion`.** The row is going away, so a schema-version complaint
about it is noise, and the answer must not depend on which of two problems the
caller has.

### `client.go`

No code change — the sentinel passes through `update` untouched, and
`hideWrongKind` does not apply. Godoc only, on `Update` and `UpdateByName` in
the `Client` interface: state the refusal and name the sentinel.

## Edge cases the implementer would otherwise guess at

- **A byte-identical write to a deleting row also refuses.** It would otherwise
  be a silent no-op, and the answer to "may I write this object" must not depend
  on the bytes the caller happens to be holding. The check runs before the
  compare, which it already does by sitting before `stampVersion`.

- **`GetOrCreate`'s found branch does not change.** It still returns a
  deletion-pending row as-is with `created=false`. That branch is a *read* — it
  writes nothing, so there is nothing to refuse — and its contract is pinned by
  the [name-keyed writes ADR](../adr/2026-07-27-name-keyed-writes.md). Do not
  "align" the two; they are answering different questions.

- **Both entry points, one site.** `UpdateSpec` and `UpdateSpecByName` share
  `updateSpec`, so one check covers both. `UpdateSpecByName` reports no
  `ErrWrongKind` (the kind is in the `WHERE`), and that is unchanged.

- **No other caller exists.** `Client.Update`/`UpdateByName` are the only
  callers of the spec mutators in the tree. `ControllerClient` and `AdminClient`
  carry no spec write at all, so neither surface grows an error.

- **Nothing is appended to `object_writes`.** The refusal returns before
  `recordObjectWrite`, so no watcher and no dependent is woken. Assert this — it
  is half the reason for the change.

- **The refusal is not terminal.** It is a level-triggered "not yet": the object
  is collected, the name is released, and the caller's next pass creates the
  replacement. Nothing needs a retry loop.

- **`fakeStore` still panics on both spec mutators** (`testutils_test.go:779`).
  The client tests here run against the real store, so this should stay a
  non-event — but the panic is what a test reaching for the double will hit
  first, and filling it in as tests need it is the standing rule.

## Tests

In `client_test.go`:

- `Update` on a deletion-pending row returns `ErrDeletionPending`, and the row's
  `generation`, `resource_version` and spec bytes are all unchanged afterwards.
- `UpdateByName` likewise.
- A **byte-identical** `Update` on a deletion-pending row also returns
  `ErrDeletionPending` rather than the silent no-op.
- The pair that is the reported bug, in one test: with a deletion-pending row
  holding the name, `Update` gives `ErrDeletionPending` **and** `Create` gives
  `ErrNameTaken`. That pair is the whole contract — the write is refused and the
  replacement cannot be made yet — and it is what a caller reads the sentinel
  against.
- `ErrDeletionPending` does not satisfy `errors.Is(err, ErrNotFound)`. Keep it
  for the documentation value, but do not count it as coverage: two distinct
  `errors.New` values under a single `%w` can only fail if someone deliberately
  wraps both. The paired test above is the real pin.

In `sqlite/store_test.go`:

- `UpdateSpec` and `UpdateSpecByName` both return it.
- No `object_writes` entry is appended by a refused write.
- A schema-version downgrade **on a deleting row** reports
  `ErrDeletionPending`, not `ErrSchemaVersionDowngrade` — pins the check order.

## On ship

- Fold into an ADR and delete this spec. Two things belong in it that are not in
  the diff: the unhandled-sentinel behaviour above, and one sentence on the
  asymmetry with delete — `requestDeletion` (`sqlite/store.go:2407`) is silently
  idempotent on an already-pending row (`Marked=false`, no error) where a spec
  write now errors. Defensible, since a second delete gets the state it asked
  for and a spec write does not, but it is the first question a reader will ask.
- `README.md`: the `Name already held by` table around line 530 is what a caller
  actually reads on this subject, and it covers only `Create` and `GetOrCreate`.
  It needs `Update` too, not just the prose paragraph below it. That paragraph
  (line 536) then loses its "should think about the deletion-pending row"
  warning, which becomes a statement that `Update` refuses it — keep the
  `GetOrCreate` half, whose found branch still hands the row back.
- `CLAUDE.md`: extend the "Reconcile is not transactional" bullet, where the
  durable record of a write is described.
- [`docs/reconcile-triggers.md`](../reconcile-triggers.md): the spec-write push
  row (line 69) gates on "the store reports `changed`". The gate is unchanged,
  but a refused write is a new way to not trigger, and `CLAUDE.md` requires that
  doc to move when a trigger does. One cell.
- Comment on [#126](https://github.com/amorey/beehive/issues/126) with the
  sentinel and the owned-child recovery; it removes one of the reporter's three
  beehive-specific lines.
