# Cache the latest event run

- **Status:** Planned.
- **Date:** 2026-08-20
- **Depends on:** one beehive per store, now enforced within the process — see [the sole-writer ADR](../adr/2026-08-05-one-process-one-beehive-sole-writer.md).
  A cached run key that another writer has moved past extends the wrong run.

## Why

`Events().Add` costs six round trips (`sqlite/store.go:1922`):

```
BEGIN IMMEDIATE
  SELECT "group", kind FROM objects WHERE id = ?          -- kind gate
  UPDATE resource_version_seq ... RETURNING value          -- version
  SELECT id, type, reason FROM events ... ORDER BY id DESC -- the latest run
  UPDATE events ... | INSERT INTO events ...               -- extend or start
COMMIT
```

Two of the four statements exist to look things up that this process wrote and
has not forgotten: the object's kind, and the latest run key for
`(object, category)`.

## The change

A bounded cache keyed by `(ObjectID, category)` holding the latest run's id,
type and reason — the three columns `latestEventKey` returns — plus the object's
group and kind for the gate.

An entry is written by every successful `Events().Add`, so a timeline that is
being appended to stays hot. A miss falls through to today's two reads and
populates the entry.

With [version blocks](../adr/2026-08-20-reserve-resource-versions-in-blocks.md) as well,
an event write goes from six round trips to three: `BEGIN`, the write, `COMMIT`.

## The rules this rests on

**Three things invalidate an entry, and all three run here:**

1. `Events().Sweep` — retention trims runs, and may trim the latest one.
2. Object collection — the log cascades away with its row.
3. `Events().Add` itself, which is the only thing that appends.

**A wrong entry corrupts the log.** Extending a run that retention deleted writes
to a row that no longer exists — harmless, the `UPDATE` matches nothing, but the
event is lost. Extending the wrong run merges two timelines' worth of counts.
Neither is detectable afterwards. So the cache must be invalidated conservatively:
when in doubt, drop the entry and read.

## Edge cases the implementer would otherwise guess at

- **Retention is per `(object, category)`**, matching the ring cap's partition.
  `Events().Sweep` returns what it trimmed; drop those entries. If it does not
  report enough to identify them, drop the whole cache on any sweep that trimmed
  anything — a sweep is rare, and correctness is worth more than the entries.

- **The kind gate is a different lifetime from the run key.** The object's kind
  never changes, but the object can be collected. Keying both in one entry is
  fine as long as collection drops it.

- **`AddEvent` is kind-scoped; the reads are not.** Only the write is gated, so
  only the write needs the cached kind. Do not let the cache leak into
  `ListEvents`, `GetLatestEvent` or `WatchEvents`, which take a bare id by design.

- **Bound the cache.** One entry per active timeline, LRU. An unbounded map keyed
  by object id is the thing this design has otherwise avoided.

- **`AdminClient.AddEvent` writes outside a pass**, including on a stopped
  beehive. The cache lives in the sqlite store, beside the writes, so it serves
  that path too — which is the reason it does not live in `Beehive`.

## Tests

In `sqlite/store_test.go`:

- Two events with the same `(type, reason)` extend one run; the second issues no
  latest-run read.
- A different `(type, reason)` starts a new run and refreshes the entry.
- A retention sweep that trims a timeline makes the next add read again.
- A collected object drops its entry; adding to it is `ErrNotFound`.
- A wrong-kind id is still `ErrWrongKind`, hot cache or cold.
- Two categories on one object keep separate entries.
- The cache evicts under its bound without changing any answer.

## On ship

ADR: **the event log's head is cached**, recording the three invalidation paths
and the "when in doubt, drop" rule.

Amend [the events ADR](../adr/2026-07-27-events-api.md) with a pointer. Its
description of runs and extension is unchanged.
