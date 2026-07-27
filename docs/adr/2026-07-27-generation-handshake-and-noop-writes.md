# The generation handshake, and what a content no-op still writes

- **Status:** Accepted — implemented in `sqlite/store.go`, `controller.go`.
- **Date:** 2026-07-27 (recorded retroactively)

## The handshake

`Object.Generation` increments on every spec change. `Object.ObservedGeneration`
records the generation the controller last settled; `nil` until the first
`UpdateStatus` call. The reconciler and resync skip objects where
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

Every mutator skips a write whose bytes match what's stored (`ObjectsUpdateSpec`,
`FinalizersDelete`, `ConditionsDelete`, `UpdateStatus`) — no `resource_version` bump,
no `updated_at`, no emit, since a watcher would otherwise see a spurious diff and
any dependent free-riding on this kind's status `Modified` would reconcile for
nothing on each unchanged poll.

But `UpdateStatus` alone also carries `observed_generation` / `observed_at`, which
record *that the controller ran*, not what it wrote, and `ObjectsListUnsettledIDs` keys
off `observed_generation < generation`. So the no-op branch still advances those
two columns when they'd move — skipping them would strand a legitimately converged
object unsettled and re-enqueued forever — and that advance **does** bump
`resource_version` and emit `Modified`, identical bytes or not: the object just
settled at a generation it hadn't settled at before, and anything gating on
`ObservedGeneration == Generation` would otherwise sit blind until the next
resync.

It can't spin a controller re-applying its own status, because it fires at most
once per generation — the repeat poll finds the generation already recorded and
takes the silent path.

## The no-op is gated on the schema version, not just the bytes

The byte compare is only meaningful when both sides are in the same shape, and
convert-on-read leaves a row tagged at the version it was written in (see
[schema-version migration](2026-07-27-schema-version-migration.md)). A caller at a
*newer* version is handing over bytes in a different shape, where equal bytes can
carry different values — a converter reading v1's absent field as a default the v2
shape spells explicitly. Suppressing that as a no-op would change what every later
read decodes while reporting `changed=false`, bumping no `resource_version` and
emitting nothing, so no watcher learns and the client skips the controller wake.

Both mutators therefore take the no-op branch only when `stampVersion`'s result
equals the row's current tag; a mismatch falls through to the content write, which
stamps, bumps and emits like any real change. That subsumes the old silent
re-stamp path (a `restamp` helper writing the version column alone, with no rv bump
and no emit): re-tagging is now always visible, at worst costing one spurious
reconcile per row per version bump, which the level-triggered loop absorbs.

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

The content branch needs it because `convertBlob`'s `current == 0` identity lets an
unversioned build decode v3 bytes untouched and marshal them straight back —
stamping 0 there would label v3-shaped bytes as unversioned, and a later build with
the migrator restored would see `from < current` and convert already-converted data
instead of getting the downgrade error the read path owes it.

## `observed_at` is a handshake timestamp, not a reconcile heartbeat

Identical bytes at the row's own schema version, with the generation already
recorded, writes nothing at all. So `observed_at` records when the object settled
at `observed_generation` and stops ticking once it has — a reconcile that calls no
`UpdateStatus` never moved it either, so it was never a faithful "last ran" signal.

Controller liveness belongs in the events log, whose runs bump `last_at` per poll.
`Condition.Liveness` is the other timestamp-carries-meaning case, and it breaks the
no-op suppression deliberately (`conditionUnchanged` returns false for a
process-stale liveness condition) because that refresh is bounded to once per
process, not once per reconcile.
