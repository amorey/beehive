# A pass skips a condition write it can see is a no-op

- **Status:** Planned. The same shape as
  [the status skip](../adr/2026-08-19-a-pass-skips-a-status-write-it-can-see-is-a-no-op.md),
  applied to the other half of what a pass reports.
- **Date:** 2026-08-20
- **Depends on:** nothing. Needs no in-memory index and no enforcement, because
  the baseline is the object the pass was handed.

## Why

A controller that reports `Ready=True` on every pass writes the same condition
every pass. `Conditions().Set` handles that correctly and cheaply — it loads the
stored conditions, compares, and writes nothing when nothing changed — but it
still opens a transaction to do it:

```
BEGIN IMMEDIATE  +  conditionSetLoad join  +  COMMIT
```

About 35 µs to conclude that there is nothing to write, on every pass of every
controller that reports conditions.

The pass already holds the answer. `GetForReconcile` attaches the object's
conditions and hands them to the controller, so the comparison the store makes
against the database is one the pass can make in memory.

## The change

`statusbaseline.go` gains a conditions half. `newStatusBaseline` already takes
the loaded `RawObject`; it keeps the loaded conditions too, keyed by type.

`ControllerClient.SetCondition` / `SetConditions` compare against that baseline
first. Every condition unchanged means no store call at all. Anything else falls
through to `Conditions().Set` as today.

On commit, the written conditions are promoted into the baseline, so a second
identical write in the same pass also skips.

## The rules this rests on

These are inherited from the status skip, and all four apply unchanged:

1. **The in-memory comparison must be a strict subset of the store's.** A false
   negative costs a transaction. A false positive loses a write. When in doubt,
   fall through.
2. **A write in flight poisons the skip.** `AfterCommit` runs at the outermost
   commit, so a sibling call inside a `Within` would otherwise match a stale
   baseline. A failed or rolled-back write never promotes.
3. **The skip cannot report a collected object.** A pass whose object was
   collected mid-pass gets `ErrNotFound` from the store today; a skip returns nil.
   That is the same trade the status skip made.
4. **`objects.conditions` must have one writer.** `TestObjectStatusIsWrittenInOnePlace`
   pins the status column; this needs the equivalent for the `conditions` table.

## Edge cases the implementer would otherwise guess at

- **A stale liveness condition must not be skipped.** `conditionUnchanged`
  refuses to suppress a write when `livenessStale` says the stored condition was
  written before this process started — the refresh is what clears the `Unknown`
  downgrade. The in-memory compare must apply the same rule, and it has the same
  `processStart` available.
  **This is the one that will be missed**, and getting it wrong pins a liveness
  condition at `Unknown` forever.

- **`Condition.Unconfirmed` is read-only.** `SetConditions` does not copy it and
  `conditionUnchanged` does not compare it. The baseline must ignore it too, or
  echoing a read condition back stops being a no-op.

- **`transitioned_at` is decided in SQL.** The baseline never has to compute it,
  because a skip writes nothing. Do not try to model it.

- **A batch is all-or-nothing.** `SetConditions` writes several conditions under
  one timestamp and one version bump, because "a reader must never see half of
  it". So skip only when *every* condition in the batch is unchanged; one
  difference sends the whole batch to the store.

- **`DeleteCondition` is not covered.** A delete of an absent condition is
  already a no-op in the store, and knowing the condition is absent needs the same
  baseline — worth adding, but say so explicitly rather than leaving it ambiguous.

- **`AdminClient` holds no baseline**, so its condition writes keep the store
  path. That is what the status half does.

## Tests

In `statusbaseline_test.go` and `controller_test.go`:

- A pass re-asserting an unchanged condition takes no transaction. Assert on
  `txCount`.
- A changed status, reason or message writes.
- A batch with one changed condition writes the whole batch.
- A condition whose stored copy predates `processStart` is written even when
  every field matches — the liveness rule.
- A write inside a caller's `Within` that then fails does not promote, and the
  next write reaches the store.
- Two identical writes in one pass: the second skips.
- A condition written by another client between the load and the write is not
  lost — the store's own compare is still behind it.

## On ship

ADR, or an amendment to
[the status skip ADR](../adr/2026-08-19-a-pass-skips-a-status-write-it-can-see-is-a-no-op.md).
An amendment is better: the rules are identical, and stating them twice invites
them to drift.

`CLAUDE.md`'s bullet on the status skip becomes a bullet about what a pass skips,
covering both.
