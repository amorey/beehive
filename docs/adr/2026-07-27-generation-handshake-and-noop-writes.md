# The generation handshake, and what a content no-op still writes

- **Status:** Accepted — implemented in `sqlite/store.go`, `controller.go`.
- **Date:** 2026-07-27 (recorded retroactively)

## The handshake

`Object.Generation` increments on every spec change. `Object.ObservedGeneration`
records the generation the controller last settled; `nil` until the first
`UpdateStatus` call. The reconciler and the full pass skip objects where
`ObservedGeneration == Generation` (already settled).

Controllers report which generation they reconciled by passing `obj.Generation`
explicitly to `UpdateStatus(ctx, id, observedGeneration, status)` — the store never
derives this internally, so callers must always pass the generation of the object
they received in `Reconcile`.

The store guards it: `UpdateStatus` rejects an `observedGeneration` greater than
the row's current generation with `ErrObservedGenerationFuture` (a controller can
only have observed a generation that exists). An older value is accepted — that's
the normal case where the spec changed mid-reconcile, leaving the object unsettled
so it reconciles again.

The guard is a read-first check, not a `WHERE generation >= ?` clause, and it
fires on the no-op path too: a caller reporting a generation that doesn't exist is
a bug regardless of whether the bytes changed.

## The content no-op splits the two halves of the write

Every mutator skips a write whose bytes already match what is stored:
`ObjectsUpdateSpec`, `FinalizersDelete`, `ConditionsDelete` and `UpdateStatus`. No
`resource_version` bump, no `updated_at`. Otherwise a watch poll would report a change
that didn't happen, and any dependent riding on this kind's status would reconcile for
nothing on every unchanged pass.

`UpdateStatus` is different in one way: it also carries `observed_generation` and
`observed_at`, which record *that the controller ran*, not what it wrote — and
`ObjectsListUnsettledIDs` selects on `observed_generation < generation`. So the no-op
branch still advances those two columns when they would move. Skipping them would
leave a genuinely converged object unsettled and re-queued forever.

That advance **does** bump `resource_version`, identical bytes or not, because the
object just settled at a generation it had not settled at before. Anything waiting for
`ObservedGeneration == Generation` would otherwise stay blind until the next full
pass. It cannot spin a controller that re-applies its own status, because it happens
at most once per generation: the next pass finds the generation already recorded and
takes the silent path.

## The no-op is gated on the schema version, not just the bytes

Comparing bytes only means something when both sides have the same shape, and
converting on read leaves a row tagged at the version it was written in (see
[schema-version migration](2026-07-27-schema-version-migration.md)). A caller at a
*newer* version is handing over a different shape, where identical bytes can mean
different things — a converter might read a field v1 leaves out as the default that v2
writes explicitly. Treating that as a no-op would change what every later read decodes
while reporting `changed=false` and bumping no `resource_version`, so no scan could
tell anything had moved.

Both mutators therefore take the no-op branch only when `stampVersion`'s result equals
the row's current tag. A mismatch falls through to the content write, which stamps and
bumps like any real change. So re-tagging is always visible, and there is no silent
path that writes the version column alone. At worst it costs one extra reconcile per
row per version bump, which a level-triggered loop absorbs.

## The stamp is never downward, on either branch

One rule, `stampVersion(stored, incoming)` — the write-side twin of `convertBlob`,
applied by both `ObjectsUpdateSpec` and `UpdateStatus` to the content no-op *and* the real
content write:

- `incoming == 0` is "no opinion" (kind unversioned, or this build lost the
  migrator): keep the stored tag.
- `0 < incoming < stored` is a genuine downgrade: reject with
  `ErrSchemaVersionDowngrade`. Such a caller could not have decoded the row it's
  writing back (`convertBlob` refuses `from > current`), so it's a bug worth
  surfacing, not a case to clamp silently.
- otherwise stamp `incoming`.

The content branch needs this because `convertBlob` passes a blob through untouched
when the current version is 0, so a build with no migrator can decode v3 bytes and
marshal them straight back. Stamping 0 there would label v3-shaped bytes as
unversioned, and a later build with the migrator restored would see `from < current`
and convert already-converted data instead of getting the downgrade error it is owed.

## `observed_at` is a handshake timestamp, not a reconcile heartbeat

Identical bytes at the row's own schema version, with the generation already recorded,
write nothing at all. So `observed_at` records when the object settled at
`observed_generation` and stops moving once it has. It was never a faithful "last ran"
signal anyway: a reconcile that calls no `UpdateStatus` never moved it either.

Controller liveness belongs in the event log, whose runs bump `last_at` every time.
`Condition.Liveness` is the other case where a timestamp carries meaning, and it breaks
no-op suppression on purpose — `conditionUnchanged` returns false for a liveness
condition left by an earlier process — because that refresh happens once per process,
not once per reconcile.
