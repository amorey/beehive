# A spec write probes before it transacts

- **Status:** Planned. Sequenced after [a spec write refuses a deleting
  row](2026-08-19-a-spec-write-refuses-a-deleting-row.md).
- **Date:** 2026-08-19
- **Issue:** [#126](https://github.com/amorey/beehive/issues/126),
  [#125](https://github.com/amorey/beehive/issues/125)

## The gap

`updateSpec` opens `Within` — `BEGIN IMMEDIATE` on a single-connection store —
reads, compares bytes, and usually writes nothing. A converged object pays a
write transaction to be told it is converged.

[#125](https://github.com/amorey/beehive/issues/125) is closed, but what shipped
in it (#129, `statusbaseline.go`) is the **status** half: a pass compares against
the object it was handed and skips. `updateSpec` is untouched, so the spec half —
#125's "Also worth having" — is still open, and it is the half a caller reaches
through `Client.Update` and `Client.UpdateByName`.

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

## The decision

Run the resolve **once before** `Within`. If it answers the whole question,
return without opening a write transaction.

The probe needs no new query and no new store method: `updateSpec` already takes
`resolve func(context.Context) (*RawObject, error)`, and the row it returns
carries spec bytes, `schema_version_spec` and `deletion_requested_at`. Calling
it outside the transaction is the entire mechanism.

### The claim this PR has to be reviewed against

**It changes no outcome.** Every answer the probe short-circuits is one the
in-transaction path already establishes, and that path stays exactly as it is,
as the authority. The probe is advisory — the same standing `requestDeletion`'s
probe has (`sqlite/store.go:2407`), and this follows its shape deliberately.

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

### What it does *not* buy

The store is `SetMaxOpenConns(1)`. A lock-free read still contends for the one
connection, so this removes `BEGIN IMMEDIATE` and the write transaction's
duration, **not** contention. The win is that the connection is held for a
`SELECT` rather than for a write transaction plus its commit. State the claim
that precisely in the ADR and in any benchmark, or the next reader will measure
the wrong thing.

## Surface

### `sqlite/store.go`

`updateSpec` grows a front end. The body inside `Within` does not change.

```go
// updateSpec is the read-compare-write body both spec mutators share. The probe
// runs first and lock-free, so the steady state — a spec that already matches —
// answers without BEGIN IMMEDIATE. It is advisory: the transaction re-resolves
// and re-compares, and its answer is the one that counts.
func (s *sqliteStore) updateSpec(...) (*storeapi.RawObject, bool, error) {
	if obj, err := resolve(ctx); err == nil {
		if done, res, err := s.specWriteSettled(ctx, obj, spec, specVersion); done {
			return res, false, err
		}
	}
	// ... unchanged Within body
}
```

The predicate is the transaction body's own prefix, factored so the two cannot
drift:

```go
// specWriteSettled reports whether obj already answers a spec write, and with
// what. Exactly the transaction's leading checks, in the same order: a deleting
// row is refused, and a matching spec at a matching version writes nothing.
// done=false means the write has work to do and must go through Within.
func (s *sqliteStore) specWriteSettled(
	ctx context.Context, obj *storeapi.RawObject, spec []byte, specVersion int,
) (done bool, res *storeapi.RawObject, err error) {
```

Three things it must get exactly right, all of them shared with the transaction:

1. The deletion refusal from
   [its spec](2026-08-19-a-spec-write-refuses-a-deleting-row.md), first.
2. `stampVersion`, whose `ErrSchemaVersionDowngrade` is returned here too — it
   is pure, so the fast path can answer it and a downgrade never needs the lock.
3. The compare gated on `stamp == obj.SpecVersion && bytes.Equal(...)`, not on
   bytes alone. Drop the version gate and the fast path and the transaction
   disagree about the same row.

A probe error is **swallowed, not returned**: fall through to the transaction
and let it produce the error under the lock. One error path, and a transient
read failure costs a transaction rather than a spurious failure.

### Nothing else changes

`client.update` already gates its signals on `changed`, so a fast-path return
(`changed=false`) wakes nobody — same as today's in-transaction skip.

The `Objects` interface godoc does **not** change, unlike [the
refusal](2026-08-19-a-spec-write-refuses-a-deleting-row.md), which adds a clause
there. The contract states outcomes, and this changes none — it is an
implementation's choice about when to take its own lock, which no `Store`
implementer is obliged to copy.

## Edge cases the implementer would otherwise guess at

- **Inside a caller's `Within`, the probe joins the transaction.** `s.conn(ctx)`
  picks up the live tx, so the probe becomes an extra read inside a lock the
  caller already holds — correct, no benefit. Do not special-case it; a branch
  on "am I in a transaction" buys one `SELECT` and costs a rule.

- **The fast path returns a torn read.** The skip must return the row with
  conditions attached, as the transaction's skip does via `attachConditions` —
  and outside a transaction that is a second lock-free read at a different
  instant, so spec and conditions can come from different moments. Accept it and
  say so: the object is already a snapshot as of some instant, spec is
  app-written while conditions are controller-written, and the level-triggered
  model re-reads. If it ever matters the fix is one query joining conditions,
  **not** a transaction — there is no read-only `Within` here, since the DSN is
  `_txlock=immediate`.

- **The probe must not skip an error.** `ErrNotFound` and `ErrWrongKind` fall
  through to the transaction rather than short-circuiting, per the swallow rule
  above.

- **`specWriteSettled` is called from two places and must stay one function.**
  The moment the fast path grows its own copy of the compare, the two can
  disagree about a row and the "changes no outcome" claim is void.

## Tests

In `sqlite/store_test.go`:

- A byte-identical `UpdateSpec` returns the row with `changed=false`, appends no
  `object_writes` entry, and leaves `resource_version` alone. Extend the
  existing no-op coverage rather than adding a parallel test.
- Conditions are attached on the fast path — the returned row is shaped like the
  transaction's skip, not like a bare `getObjectRow`.
- Identical bytes at a **different** `schema_version_spec` still write, so the
  version gate is not lost to the fast path.
- A downgrade reports `ErrSchemaVersionDowngrade` from the probe.
- On a deleting row, `ErrDeletionPending` — the fast path inherits the refusal.
- A probe whose read fails still completes the write through the transaction.
  Reach for the existing failing-connection harness in `store_test.go`.

In `sqlite/store_bench_test.go`:

- A no-op `UpdateSpec` benchmark, against a converged row. This is the number
  the change exists for, and it belongs next to the claim it supports.

Not worth writing: a test asserting no lock is taken. With one connection there
is nothing to observe from another goroutine — see "what it does not buy".

## On ship

- Fold into an ADR carrying the linearization argument. That argument is what
  the change rests on, and it is not obvious from the diff.
- `CLAUDE.md`: extend the same bullet
  [the refusal spec](2026-08-19-a-spec-write-refuses-a-deleting-row.md) touches.
- `README.md`: `Update`'s no-op paragraph gains the probe, matching how the
  delete verb's lock-free probe is already described around line 602.
- Re-measure [#126](https://github.com/amorey/beehive/issues/126) with the
  reporter. With this and the refusal in, their block is `UpdateByName`, then
  `GetOrCreate` on `ErrNotFound` — no probe, no comparison, no guard.
