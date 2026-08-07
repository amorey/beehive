# The event watch reads from a cursor, and a commit wakes it

- **Status:** Accepted — implemented in `eventswatch.go`, `sqlite/store.go`,
  `sqlite/migrations/0001_init.sql`.
- **Date:** 2026-08-05

## Context

`EventsWatch` was the one watch surface that never followed the object watches
onto a cursor. It polled every second, re-listed the object's log when
`Events().MaxVersion` moved, and diffed the listing against a `seen` map keyed by
`EventID`. A subscriber learned of a write on the next tick rather than at
commit, its snapshot arrived through the channel mixed in with everything after
it, and `WithEventRetention` could trim runs out from under it with nothing to
say so.

## Decision

### The log already had a cursor

`events.resource_version` is drawn from `resource_version_seq`, and **an extend
re-samples it** — `Events().Add`'s `UPDATE` takes a fresh version the same way its
`INSERT` does. So every write leaves its run above every version the log has
handed out, and one query is the whole tail:

```sql
SELECT … FROM events WHERE object_id = ? AND resource_version > ? ORDER BY resource_version
```

served by the existing `idx_events_object_rv`. Every row it returns is new or
extended; nothing else can move. `Events().ListSince` is that query, and the `seen`
map, the `EventID` diff and the full re-listing went with it.

### The reader is per watch, not shared

One reader per `EventsWatch`, over its own cursor, with none of `objectTailer`'s
lease machinery. The object tail is shared per kind because `object_writes` has
no per-object index and N watches on one kind would be N scans of one log;
neither holds here — the events read is already per object and already indexed,
and the interesting fan-out (a panel over many objects) is *distinct* ids, which
a shared tailer would not collapse anyway.

The cost is stated rather than hidden: one receiver, one goroutine, one timer and
one gate per stream, and two watches on one id cost two queries per wake. A
consumer that wants one reader for many objects wants a kind-wide event watch,
which this does not build.

### A commit wakes it; the tick stays, at the floor interval

`ControllerClient.EventsAdd` publishes the object id on `Store.AfterCommit`
through `eventWriteHub` — keyed by id, since a kind-wide signal would wake every
event reader of the kind on every write. Ninth user of `AfterCommit`.

This is not a new push exception. The tick stays, because an event written by a
second process or issued straight to the `Store` publishes nothing, and it moved
from the 1s poll to `watchFloorInterval` (30s): once the wake carries the latency
requirement, the tick's only job is foreign writers — the same job the object
tail's floor already does at that cadence. `withWatchPollInterval` and its
default went with the poll they configured. A cross-process write is now seen in
up to 30s rather than 1s, which is the trade the object tail already made.

### Retention gets a horizon, per timeline

A read must not imply an absence it cannot vouch for. Returning the survivors
above a cursor is an unqualified claim that they are *all* the runs above it, and
once retention has been through, that claim is stronger than the store can back —
a trimmed run and a run never written are the same empty result. `events_horizon`
qualifies it: complete above `trimmed_through`, unknown below, and a resume below
it is refused with `ErrWatchTooOld` rather than answered.

Two bounds on what it says. It reports *that* there is a hole, never what was in
it — the only useful response is to resubscribe. And only a resume, or a reader
stalled past retention, can read into the unknown range: a caught-up reader's
cursor sits above everything trimmable.

Keyed `(object_id, category)` to match the ring cap's own partition, which trims
each timeline independently. An object-wide horizon would let a chatty category
refuse every resume on a quiet one — routine, not rare, for exactly the consumer
this is for.

`Events().Sweep` records it **before** each delete, from the same predicate in the
same transaction (`INSERT … SELECT … GROUP BY … ON CONFLICT`). `RETURNING` would
give the same answer while materialising every deleted row in Go and holding a
half-read cursor on the single connection between two statements of one
transaction; this way `RowsAffected` still supplies the returned count.

### A collected object ends its streams

The same rule where the horizon runs out. `events` and `events_horizon` both
cascade off `objects`, so a physical delete takes every unread run *and the
record that they existed*, leaving an empty page and a zero horizon — the read
implying "no events" about an object whose whole log was deleted.
`Events().ListSince` probes `objects` when it finds neither rows nor a horizon and
returns `ErrNotFound`, which ends the stream. Ids are never reused, so nothing is
lost by ending it, and a caller blocked on that channel is waiting for something
that cannot happen.

### The public shape

```go
EventsWatch(ctx, id, opts ...EventOption) (*EventStream, error)
```

`EventStream` carries `Runs` (the snapshot, newest-first), `ResourceVersion` (the
position it was read at), `Events` (the runs above it, oldest-first) and `Err()`.
A handle rather than the object watches' `(snapshot, chan, error)` because a
stream needs somewhere to report the failures that are not the caller's
cancellation, and an event log has no change type to carry one — the naming ADR
is explicit that a watch over a log streams the value itself.

`Events().Snapshot` reads the runs and the position in one transaction: two reads
cannot answer "these runs, as of this position" — whichever order they run in, a
write between them is either delivered twice or dropped. `Event` gained
`ResourceVersion`, without which a caller cannot checkpoint what it was
delivered. Resume rides `EventOption` (`WithEventsResumeFrom`), which grew a
config behind it; a separate option type would have forced two variadics or a
wrapper on every call, where the filters are meaningful on both reads.

## Consequences

Delivery is at commit. A resume replays exactly the gap. Ordering is ascending by
`resource_version` — `ORDER BY` gives it, where the object tail had to sort a
fan-out for it — and it is load-bearing either way: a caller checkpoints a
delivered version and resumes above it.

There is no merge, so a consumer that stops reading blocks the drain and pins the
cursor. That is what makes `ErrWatchTooOld` reachable on a live stream and not
only on a resume, and it is the cheaper contract: a buffer would only move the
threshold.

`Store.Events().Add` returns `error` alone. The push path builds its delta from a
cursor rather than from the write's own result, so the write-shapes exception
`TODO.md` held open for it closes, and both branches lose `RETURNING` and a row
decode with it.

**The horizon is not exact, and the ADR does not claim it is.** Both retention
bounds select by `last_at`, a clock the store already distrusts (`latestEventRun`
orders by `id` for this reason). A backwards step that makes a freshly extended
run look old trims it and slams the horizon to the head, ending every live stream
on that timeline. Note the direction: it over-reports, so the qualification is too
loud rather than absent, and no read implies a completeness it lacks.

See [the events ADR](2026-07-27-events-api.md) for the log itself, and
[the watch tail ADR](2026-08-03-watch-shared-tail.md) for the object-side pattern
this borrows its loop from.
