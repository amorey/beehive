# Watch fan-out conflates per object instead of lagging-as-loss

- **Status:** Accepted — implemented in `sqlite/watch.go`.
- **Date:** 2026-07-27 (recorded retroactively)

## Context

A watch fan-out has to decide what happens when a receiver falls behind. The
Kubernetes-style answer is a bounded ring plus `ErrLagged` and a relist, which
makes every watcher carry relist machinery and makes memory scale with write
volume.

## Decision

Fan out through a per-kind **conflating hub** (`conflate`, from the external
`github.com/amorey/gobus` library). Each receiver keeps the *latest* event per
object id in first-touch order, and a `Send` for an already-pending id coalesces
into that slot via a merge callback (`changeMerge`).

A slow watcher therefore converges to each object's current state — a delete
carries the store's real final row — instead of dropping events. There is no ring,
no `ErrLagged`, no relist, and no synthesized tombstone, and per-watcher memory is
bounded by the live key set rather than write volume.

The merge function is beehive's domain policy (Added/Modified/Deleted lifecycle,
create-then-delete annihilation); the hub stays generic.

## Consequences

- The subscribe→snapshot race is deduped by the single global
  `snapshotHighWaterRV` scalar — `resource_version` is one monotonic cursor.
- A create-then-modify can coalesce into one `Added`, so the dependency waker must
  wake on `Added` *and* `Modified`.

## The store-wide hub

Alongside the per-kind hubs, `publish` also sends to one store-wide `changeHub`
under the same `ObjectID` key and the same `changeMerge` — `objects.id` is one
`AUTOINCREMENT` PK for the whole table, so a global hub conflates per object
exactly as the per-kind ones do.

It is an eager field created in `open` and closed in `Close`, not a lazy `hubFor`
entry: there is exactly one and no key to look up, and laziness would cost the
publish path a second lock and map lookup on every write. Its consumer is the
dependency waker — see
[one store-wide change stream](2026-07-27-store-wide-dependency-change-stream.md).
