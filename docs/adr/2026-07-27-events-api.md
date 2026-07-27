# Events API: an append-only, contiguous-run aggregated log

- **Status:** Accepted — implemented in `sqlite/store.go`, `sqlite/watch.go`,
  `client.go`, `controller.go`, `sqlite/migrations/0001_init.sql`.
- **Date:** 2026-07-27 (recorded retroactively)

## Decision

A per-object event log (`events` table) partitioned by `category` into independent
timelines.

`ControllerClient.EventsRecord` is the sole writer. Inside one `Within` it probes
the latest run for `(object_id, category)`
(`WHERE object_id=? AND category=? ORDER BY id DESC LIMIT 1`) and either

- *extends* it (`count++`, bump `last_at`, re-sample `message` and `detail`, fresh
  `resource_version`) when `(type, reason)` matches, or
- *appends* a new run otherwise.

That is the "compare-to-latest, don't blind-upsert" rule, which yields a flapping
timeline instead of Kubernetes-style global dedup. The `category` scoping is what
keeps unrelated concerns on one object from shredding each other's runs.

It is the un-collapsed sibling of conditions, which keep only the current run per
type.

## `Detail` is typed in, opaque out

`EventSpec.Detail` (`any`, marshaled by `EventsRecord`) → the opaque `detail` blob
column → `Event.Detail` (`json.RawMessage`), decoded by the free generic helper
`EventDetail[T]`.

Like `Spec` / `Status`, but *unversioned* — no `Migrator`; retention ages old shapes
out — and *sampled* like `message`: not in the run key, so a varying payload never
fragments a run.

`Detail` stays off the generic boundary deliberately: a timeline mixes reasons with
heterogeneous payloads, so a per-`Client` / per-`Event` `Detail` type param can't
express it. The per-event `EventDetail[T]` helper does.

## Reads and retention

Reads live on `Client`: `EventsList` / `EventsGetLatest` / `EventsWatch` (lazy), plus
eager `LoadEvents()` → `Object.Events()`, gated by `LoadEventsBit` in the same
`LoadSet` and returning `ErrNotLoaded` when unrequested.

`EventsWatch` is snapshot-then-live over a conflating hub keyed on **`EventID`, not
object id**, so a run's count-bump conflates into that run's own slot. It publishes
through the same post-commit `eventCollector` → `flush` path as object writes.

Retention runs in `gcSweeperRun`: a per-`(object, category)` cap-N ring plus optional
`maxAge` (`WithEventRetention`). `events.object_id` is `FK … ON DELETE CASCADE`, so
object deletion cascades the log.

Store trio: `EventsRecord` / `EventsList` / `EventsSweep`.

## Naming: "event(s)" is reserved for the log

The other streams are deliberately named apart:

- The dependency waker's live-only object-*change* stream is
  `Store.ObjectWritesSubscribe(ctx)` — store-wide, snapshot-less, returning an
  `*ObjectWritesSubscription` whose `Changes()` channel yields `[]ObjectWrite` (id plus
  `ChangeType` = `Added` / `Modified` / `Deleted`, no row). It replaced the per-kind
  `WatchChanges(gk)`, which had no other caller.
- Typed `ObjectChange[Spec,Status]` values reach users through `Client.ObjectsWatch` /
  `ObjectsWatchList`, riding `ObjectsSubscription.Changes()`.
- `Store.EventsWatch` / `Client.EventsWatch` stream the log's aggregated `Event`s
  over an `EventsSubscription.Changes()` channel.

Every one of them returns a `*storeapi.Subscription[V]` with the single accessor
`Changes()`; see the naming ADR. With `WatchChanges` gone, `watch()` has no
snapshot-less caller left — hence no `hasSnapshot` parameter and an always-present
`seenIDs`.
