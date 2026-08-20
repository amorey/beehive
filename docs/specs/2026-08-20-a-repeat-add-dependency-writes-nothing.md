# A repeat AddDependency writes nothing

- **Status:** Planned.
- **Date:** 2026-08-20
- **Depends on:** [the reverse dependency index](2026-08-20-a-reverse-dependency-index.md),
  which is where the edge set lives.

## Why

A controller declares its dependencies on every pass. That is the documented
shape — the edges are level-triggered like everything else — so
`AddDependency` is called for edges that already exist, over and over.

Each call costs a full transaction (`sqlite/store.go:2630`):

```
BEGIN IMMEDIATE
  SELECT ... FROM objects f, objects t WHERE f.id = ? AND t.id = ?   -- endpoints
  UPDATE objects SET reconcile_owed = ... WHERE id = ? AND <edge is new>
  DELETE FROM dependency_watermarks WHERE object_id = ? AND <edge is new>
  INSERT INTO edges ... ON CONFLICT DO NOTHING
COMMIT
```

For an edge that already exists, all four statements write nothing — the two
middle ones are gated on `edgeIsNew`, and the insert conflicts. Roughly 70 µs per
declaration per pass to confirm the edge is still there.

## The change

`ControllerClient.AddDependency` asks the index first. An edge already present
returns without touching the store.

The result it must still produce is the problem:

```go
type EdgesAddResult struct {
	To                   GroupKind
	ToDeleting           bool
	ReconcileOwedStamped bool
}
```

`ReconcileOwedStamped` is false for a repeat edge, and `To` is the target's kind,
which the index has. **`ToDeleting` is the one that is not free**: it is the
target's current deletion state, and it changes without touching the edge.

`AddDependency` reads it — a create under a deleting owner pushes that owner's
collect. So the fast path has to answer it, which means the index carries the
target's deletion state and every deletion mark updates it.

That is worth doing anyway:
[the deletion-pending set](2026-08-20-hold-the-deletion-pending-set-in-memory.md)
already maintains exactly this. Read it from there rather than duplicating it.

## The rule this rests on

**An existing edge writes nothing today.** Both writes inside `Edges().Add` are
gated on `edgeIsNew`, and the insert is `ON CONFLICT DO NOTHING`. So the fast path
is not skipping a write — it is skipping the four statements that decide not to
write. The observable behaviour is identical, which is what makes this safe.

## Edge cases the implementer would otherwise guess at

- **A missing endpoint must still be `ErrNotFound`.** The store's endpoint join
  is what produces it. The index knows the edge exists, which proves both
  endpoints existed *then* — and a collected endpoint would have taken the edge
  with it through the FK cascade. So a known edge implies live endpoints, but only
  if the cascade is wired (see the index spec). Lean on it, and say so.

- **`owned_by` is not covered.** `Edges().Add` serves both relations;
  `insertObject` adds the owner edge exactly once per object, so there is no
  repeat to skip. Restrict the fast path to `depends_on`.

- **`AdminClient.AddDependency` is the other caller.** It writes outside a pass,
  including on a stopped beehive where there is no index. It keeps the store path.

- **The push still has to fire.** `AddDependency` calls `signalRequeueThrottled`
  when `ReconcileOwedStamped` is true. A repeat edge stamps nothing, so it pushes
  nothing — which is today's behaviour, since the stamp is gated on `edgeIsNew`.
  Confirm it rather than assuming it.

## Tests

In `controller_test.go`:

- A repeat `AddDependency` takes no transaction. Assert on `txCount`.
- The first `AddDependency` stamps `reconcile_owed`, clears the watermark, and
  pushes.
- A repeat does none of those three, with or without the fast path — the test
  should pass before the change as well as after, which is the proof that
  behaviour is unchanged.
- A repeat against a target that has since been marked for deletion reports
  `ToDeleting`, and the caller's push fires.
- `AddDependency` after `DeleteDependency` of the same edge writes again.
- `AdminClient.AddDependency` still reaches the store.

## On ship

Fold into [the reverse index ADR](2026-08-20-a-reverse-dependency-index.md)'s
record rather than writing a second one — it is the same index answering a second
question.

Amend [the stamp-every-new-edge ADR](../adr/2026-07-29-stamp-every-new-dependency-edge.md)
with one sentence: the stamp is unchanged, and a declaration that stamps nothing
now costs nothing.
