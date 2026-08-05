# Events API: an append-only, contiguous-run aggregated log

- **Status:** Accepted — implemented in `sqlite/store.go`, `eventswatch.go`,
  `client.go`, `controller.go`, `sqlite/migrations/0001_init.sql`.
- **Date:** 2026-07-27 (recorded retroactively)

## Decision

A per-object event log (`events` table) partitioned by `category` into independent
timelines.

`ControllerClient.EventsAdd` is the sole writer. Inside one `Within` it probes
the latest run for `(object_id, category)`
(`WHERE object_id=? AND category=? ORDER BY id DESC LIMIT 1`) and either

- *extends* it (`count++`, bump `last_at`, re-sample `message` and `detail`, fresh
  `resource_version`) when `(type, reason)` matches, or
- *appends* a new run otherwise.

That is the "compare to the latest, don't blindly upsert" rule. It produces a
flapping timeline rather than Kubernetes-style global deduplication, and scoping the
comparison to a category is what keeps unrelated concerns on one object from breaking
each other's runs.

It is the long form of conditions, which keep only the current run per type.

## `Detail` is typed in, opaque out

`EventSpec.Detail` (`any`, marshaled by `EventsAdd`) → the opaque `detail` blob
column → `Event.Detail` (`json.RawMessage`), decoded by the free generic helper
`EventDetail[T]`.

It works like `Spec` and `Status` with two differences. It is *unversioned* — no
`Migrator`, because retention ages old shapes out — and it is *sampled* like
`message`, so it is not part of the run key and a payload that varies never splits a
run.

`Detail` stays off the generic boundary deliberately. One timeline mixes reasons whose
payloads have different shapes, which a `Detail` type parameter on `Client` or `Event`
could not express. The per-event `EventDetail[T]` helper can.

## Reads and retention

Reads live on `Client`: `EventsList` / `EventsGetLatest` / `EventsWatch` (lazy), plus
eager `LoadEvents()` → `Object.Events()`, gated by `LoadEventsBit` in the same
`LoadSet` and returning `ErrNotLoaded` when unrequested.

`EventsWatch` **no longer polls and diffs**; it reads the log above a cursor, woken
by the write's own commit, and hands back a snapshot plus a resumable stream. That
half of this ADR is superseded by
[the event-cursor ADR](2026-08-05-events-get-a-cursor-and-a-commit-wake.md), which
also covers the retention horizon a resume is checked against. What stays true here
is the log itself: the run aggregation, the category partition, and `Detail`.

Retention runs in `gcSweeperRun`: a per-`(object, category)` cap-N ring plus optional
`maxAge` (`WithEventRetention`). `events.object_id` is `FK … ON DELETE CASCADE`, so
object deletion cascades the log.

Store set: `EventsAdd` / `EventsGetLatest` / `EventsList` / `EventsListSince` /
`EventsMaxVersion` / `EventsSnapshot` / `EventsSweep`. The store has no event watch:
the watch is a client-side reader over `EventsListSince`, woken by the commit.

## Naming: "event(s)" is reserved for the log

The object-change surfaces are deliberately named apart:

- The dependency waker reads the store's write log through
  `Store.ObjectWritesListSince(ctx, afterRV, limit)` — store-wide, paged, yielding
  `[]ObjectWrite` (id plus `ChangeType` = `Added` / `Modified` / `Deleted`, no row).
- Typed `ObjectChange[Spec,Status]` values reach users through `Client.Watch` /
  `WatchList`, which poll and diff.
- `Client.EventsWatch` streams the log's aggregated `Event`s — the value itself, not
  a `Change`, because an append-only log has nothing for a change type to say. Its
  terminal error lives on the `EventStream` handle for that reason.

See the naming ADR for the return-shape rule each follows.
