# A spec write probes before it transacts

- **Status:** Planned. Sequenced after [a spec write refuses a deleting
  row](../adr/2026-08-19-a-spec-write-refuses-a-deleting-row.md).
- **Date:** 2026-08-19
- **Issue:** [#126](https://github.com/amorey/beehive/issues/126),
  [#125](https://github.com/amorey/beehive/issues/125)

## The gap

A spec write opens `Within` — `BEGIN IMMEDIATE` on a single-connection store —
reads, compares bytes, and usually writes nothing. A converged object pays a
write transaction to be told it is converged.

[#125](https://github.com/amorey/beehive/issues/125) is closed, but what shipped
in it (#129, `statusbaseline.go`) is the **status** half: a pass compares against
the object it was handed and skips. The spec half — #125's "Also worth having" —
is still open, and it is the half a caller reaches through `Client.Update` and
`Client.UpdateByName`.

The consequence is not just cost. Callers avoid the transaction by comparing
specs themselves, and in Go that comparison is wrong the moment a spec holds a
pointer or a slice:

```go
if found.Spec == spec {   // compares addresses, never true
```

That is the bug in [#126](https://github.com/amorey/beehive/issues/126): a spec
embedding a pointer-variant union, a comparison that never fired, and an
`Update` on every pass — invisible, because the store then skipped the
byte-identical write. A caller can only match the store's answer with
`reflect.DeepEqual` or a re-marshal, both worse than what the store already does
on the way in. So the fix is not to help callers compare; it is to make reaching
the store cheap enough that nobody wants to.

### Where the lock is actually taken

**Not in `updateSpec`.** `client.update` (`client.go:518`) wraps the store
mutator in `Within` unconditionally, so that a spec which marshals but does not
decode back rolls the write back:

```go
err = c.bh.store.Within(ctx, func(ctx context.Context) error {
    raw, changed, err := write(ctx, b, migratorSpecVersion(...))
    if err != nil { return err }
    if obj, err = c.decode(raw); err != nil { return err }   // why the Within is there
```

`Client.Update`/`UpdateByName` are the only callers of the spec mutators in the
tree, so `updateSpec`'s own `Within` is always a nested SAVEPOINT
(`sqlite/store.go:497`) and `BEGIN IMMEDIATE` is already held by the time
`resolve` runs. **A probe placed inside `updateSpec` avoids no lock and costs one
`SELECT` per call.** The probe has to run above the client's `Within`, which is
what makes this a change to two packages rather than one function.

## The decision

`client.update` probes lock-free before it opens `Within`. A settled probe
returns the object without a write transaction; anything else falls through to
today's path, unchanged and authoritative.

The predicate needs the stored bytes, `schema_version_spec`,
`deletion_requested_at` and `stampVersion`'s rule — all store-internal — so the
probe is a store read, not a client comparison. Re-deriving the stamp rule above
the `Store` boundary is the drift this whole change exists to prevent.

### The claim this PR has to be reviewed against

**It changes no outcome.** Every answer the probe short-circuits is one the
in-transaction path already establishes, and that path stays exactly as it is, as
the authority.

### Why skipping on a pre-transaction read is sound

The skip condition is *stored bytes equal the bytes I intend to write*. So a
skip is observationally identical to having performed the write at the probe's
instant: it linearizes at the read. A writer landing between the probe and the
would-be write is the undefined last-writer-wins race that exists today —
whoever is later wins, with or without the probe.

This is the distinction from the hazard #125 raised against itself for status,
which I would otherwise be waving away. The status baseline compares against the
object handed out at **dispatch**, which can be arbitrarily stale and may never
be re-read; a false positive there loses a write. A probe is a fresh read by
construction, so it has no staleness to be wrong about. Staleness is the hazard,
and the probe does not have it.

Only spec bytes are compared, and `Client.Update`/`UpdateByName` are the only
spec writers in the tree — so the argument covers every path that reaches this
code.

### The probe is conservative in one direction only

`settled=true` must imply the transaction would write nothing. The converse is
not required: `settled=false` on a row the transaction would also skip costs one
transaction and nothing else. That asymmetry is the same discipline the status
baseline runs under (`statusbaseline.go`) — a false negative costs a
transaction, a false positive loses a write — and it is what lets **every**
error read as `settled=false`.

There is therefore exactly one error rule, and it covers sentinels as well as
transient failures: `ErrNotFound`, `ErrWrongKind`, `ErrDeletionPending`,
`ErrSchemaVersionDowngrade` and a failed read all fall through to the
transaction, which produces the error under the lock. Do not let the probe
answer a refusal. One error path is worth more than a saved transaction on a
path that is already failing.

### What it does *not* buy

The store is `SetMaxOpenConns(1)` (`sqlite/sqlite.go:47`). A lock-free read still
contends for the one connection, so this removes `BEGIN IMMEDIATE` and the write
transaction's duration, **not** contention. The win is that the connection is
held for reads rather than for a write transaction plus its commit. State the
claim that precisely in the ADR and in any benchmark, or the next reader will
measure the wrong thing.

### What it costs

| | today | with the probe |
|---|---|---|
| converged row | `BEGIN IMMEDIATE`, row read, conditions read, `COMMIT` | row read, conditions read |
| changed row | as above plus the write | one extra row read in front |

The extra read on the write path is a full `objectColumns` read, blobs included
— `getObjectRowScoped`/`ByName` select the same columns the transaction will
select again. That is the honest cost, and it is the branch that then does a
generation bump, a write-log append and a wake, so one more read is small beside
it. The branch this change exists for gets strictly fewer round trips.

**Do not push the compare into SQL** to avoid it. `SELECT deletion_requested_at,
schema_version_spec, spec = ?` keeps the blobs off the wire, but the settled path
still has to return a full object to `c.decode`, so it buys a narrow probe at the
price of a second read on the *common* branch — the wrong branch to spend on. The
blob read is paid once where it matters.

## Surface

### `internal/storeapi/storeapi.go`

Two new `Objects` members, in the family's alphabetical order beside
`GetForReconcile`, whose "opening read" idiom they follow:

```go
// GetForSpecWrite is the spec write's opening read: id's row within gk, with
// conditions, and whether writing spec at specVersion would write nothing.
//
// settled=true is a caller's licence to skip UpdateSpec entirely, and an
// implementation MUST NOT report it where UpdateSpec would write, refuse, or
// fail. The converse is free: every refusal — a deletion-pending row, a version
// downgrade — and every read failure reads as settled=false, leaving the write
// the sole authority on them. obj is meaningful only when settled is true.
//
// Scoped to gk: wrong kind → ErrWrongKind, missing id → ErrNotFound.
GetForSpecWrite(ctx context.Context, gk GroupKind, id ObjectID, spec []byte, specVersion int) (obj *RawObject, settled bool, err error)

// GetForSpecWriteByName is GetForSpecWrite keyed by name within gk, ErrNotFound
// if the name is not held (no ErrWrongKind).
GetForSpecWriteByName(ctx context.Context, gk GroupKind, name string, spec []byte, specVersion int) (obj *RawObject, settled bool, err error)
```

`UpdateSpec`'s own godoc does not change: it already carries the no-op contract
and, from [the
refusal](../adr/2026-08-19-a-spec-write-refuses-a-deleting-row.md), the clause
that a caller answering a check ahead of the write must reach the same verdict.
That clause was written for this caller.

Neither member is a transaction. An implementation is free to make them one; the
contract only requires the conservative direction.

### `sqlite/store.go`

The predicate is pure and sits beside `stampVersion`:

```go
// specWriteSettled reports whether a spec write to obj would write nothing.
// Conservative in one direction: false is always safe, true must match what
// updateSpec's transaction below would decide. A refusal or a downgrade is
// false — the transaction reports those.
func specWriteSettled(obj *storeapi.RawObject, spec []byte, specVersion int) bool {
	if obj.DeletionRequestedAt != nil {
		return false
	}
	stamp, err := stampVersion(obj.SpecVersion, specVersion)
	if err != nil {
		return false
	}
	return stamp == obj.SpecVersion && bytes.Equal(obj.Spec, spec)
}
```

The two new members are `resolve` + `attachConditions` + the predicate, sharing
the same resolve closures as `UpdateSpec`/`UpdateSpecByName`. `updateSpec`'s
body does not change: its skip stays written out, because it returns the errors
the predicate collapses. The two are kept honest by a test rather than by
sharing code — see below.

### `client.go`

`update` grows a probe argument beside its `write` argument, and both call sites
supply one:

```go
// The probe runs lock-free, so the steady state — a spec that already matches —
// answers without BEGIN IMMEDIATE. It is advisory: any error, sentinel or not,
// falls through to the write, which re-resolves under the lock and decides.
if raw, settled, err := probe(ctx, b, version); err == nil && settled {
	return c.decode(raw)
}
```

Nothing else moves. `update` already gates its signals on `changed`, so a
settled return wakes nobody — the same as today's in-transaction skip.

### `testutils_test.go`

`fakeObjects` gains both members. `objectsOverride` gains
`getForSpecWrite`/`getForSpecWriteByName` hooks: that is the seam the
failing-probe test needs, and it is the only one — `updateSpec` reads through
`s.conn(ctx)` off `s.db`, with nothing to inject.

## Edge cases the implementer would otherwise guess at

- **A decode failure on the fast path rolls nothing back, because nothing was
  written.** Today's `Within` exists so a spec that does not round-trip undoes
  its write; the settled path has no write, so it returns the decode error and
  the store is untouched. Same observable outcome, one fewer transaction.

- **Inside a caller's `Within`, the probe joins the transaction.** `s.conn(ctx)`
  picks up the live tx, so the probe becomes an extra read under a lock the
  caller already holds — correct, no benefit. Reachable only when an app calls
  `Client.Update` inside a `ControllerClient.Within`. Do not special-case it; a
  branch on "am I in a transaction" buys one `SELECT` and costs a rule.

- **The fast path is two statements, and they can tear.** `resolve` then
  `attachConditions` (`sqlite/store.go:1621`), outside a transaction, so spec and
  conditions can come from different instants. Accept it and say so: the object
  is already a snapshot as of some instant, spec is app-written while conditions
  are controller-written, and the level-triggered model re-reads. If it ever
  matters the fix is one query joining conditions, **not** a transaction — there
  is no read-only `Within` here, since the DSN is `_txlock=immediate`.

- **`settled=false` must never be load-bearing.** No caller may infer "this will
  write" from it. It means only "ask the transaction".

## Tests

In `sqlite/store_test.go`:

- The predicate against the transaction, table-driven over the same rows: for
  each case, `GetForSpecWrite` reporting settled implies `UpdateSpec` reports
  `changed=false`. This is the test that keeps the two from drifting, and it is
  what pays for not sharing the code.
- Settled on identical bytes at the same `schema_version_spec`; **not** settled
  at a different one, so the version gate is not lost.
- Not settled, no error, on a deletion-pending row and on a downgrade — the
  refusals stay with the write.
- `ErrNotFound` and `ErrWrongKind` from both members, id and name.
- Conditions are attached on a settled read, so the row is shaped like the
  transaction's skip rather than like a bare `getObjectRow`.

In `client_test.go`:

- A byte-identical `Update`/`UpdateByName` returns the object, appends no
  `object_writes` entry (`newWriteProbe(...).expectNone()`), and leaves
  `resource_version` alone. Extend the existing no-op coverage.
- A probe that fails still completes the write, via an `objectsOverride` whose
  `getForSpecWrite` returns an error.
- A probe that wrongly reports settled is **not** tested: the contract forbids
  it and no production caller can produce it.

In `sqlite/store_bench_test.go`:

- A no-op `UpdateSpec` benchmark against a converged row, plus the changed-row
  case so the extra read is on the record too. These are the numbers the change
  exists for, and they belong next to the claim they support.

Not worth writing: a test asserting no lock is taken. With one connection there
is nothing to observe from another goroutine — see "what it does not buy".

## On ship

- Fold into an ADR carrying the linearization argument and the "what it does not
  buy" section verbatim. Neither is obvious from the diff.
- `CLAUDE.md`: extend the same bullet [the
  refusal](../adr/2026-08-19-a-spec-write-refuses-a-deleting-row.md) touches.
- `README.md`: `Update`'s no-op paragraph gains the probe, matching how the
  delete verb's lock-free probe is already described around line 602.
- Re-measure [#126](https://github.com/amorey/beehive/issues/126) with the
  reporter. With this and the refusal in, their block is `UpdateByName`, then
  `GetOrCreate` on `ErrNotFound` — no probe, no comparison, no guard.
