# Events get a cursor, a snapshot and a commit wake

**Status:** designed, not built. Supersedes the `EventsWatch` entry in
[`TODO.md`](../TODO.md) and resolves the `EventsAdd` return-value entry there.
Amends the [events ADR](../adr/2026-07-27-events-api.md), whose "Reads and
retention" section describes the poll this replaces.

A spec is deleted when it ships; its rationale moves to an ADR.

## Problem

`EventsWatch` is the one watch surface that never followed the object watches
onto a cursor. It polls every second, re-lists the object's log when
`EventsMaxVersion` moved, and diffs the listing against a `seen` map keyed by
`EventID`. Three things follow from that, and all three are visible to a caller
holding both surfaces on one `Client`:

1. **Latency is the interval.** A commit is what the object tail waits on;
   an event subscriber waits out a tick, up to 1s after the write it cares about.
2. **There is no position.** The snapshot arrives *through the channel*, mixed in
   with everything after it, so "am I caught up?" is a guess rather than a value,
   and a dropped connection re-reads the whole log.
3. **Retention is silent.** `WithEventRetention` trims runs out from under a
   reader with nothing to tell it, so a read implies an absence it cannot vouch
   for — a trimmed run and a run never written are the same empty result. An
   object watch refuses that range outright with `ErrWatchTooOld`; see §6.

None of this is unsafe today — an append-only log has no tombstones, so the
poll-and-diff is sound — but (2) and (3) are the ones that block the consumer
this is for: a UI panel or an exporter that reconnects and expects to resume.

## The property the log already has

`events.resource_version` is drawn from `resource_version_seq`, the same sequence
`objects` and `object_writes` use, and **an extend re-samples it** (see
`EventsAdd`: the `UPDATE` sets `resource_version = ?` from `nextResourceVersion`).
So every write to the log — a new run or an extension of the newest one — leaves
that run with an `rv` above every `rv` the log has ever handed out.

That makes one query the whole tail:

```sql
SELECT … FROM events WHERE object_id = ? AND resource_version > ? ORDER BY resource_version
```

served by the existing `idx_events_object_rv`. Every run it returns is either new
or extended; nothing else can move. The `seen` map, the `EventID` diff and the
full re-listing all exist to compute exactly that set, and all three go away.

The one thing the cursor cannot see is a run that was **deleted** by retention,
which is what §"Retention gets a horizon" below is for.

## Shape of the change

### 1. `EventsWatch` returns a snapshot, a position and a stream

```go
// EventStream is a live view of one object's event log.
type EventStream struct {
    // Runs is the snapshot: the runs matching the query as of the subscribe,
    // newest-first like EventsList. Empty on a resume.
    Runs []Event
    // ResourceVersion is the position Runs was read at, and the value to pass
    // back to WithEventsResumeFrom.
    ResourceVersion int64
    // Events delivers runs above ResourceVersion, oldest-first, until ctx ends
    // or the stream fails. Closed exactly once.
    Events <-chan Event

    failed atomic.Pointer[error]
}

// Err reports why the stream ended, after Events is closed: ErrWatchTooOld,
// ErrNotFound (the object was collected), ErrStopped, or nil when the caller's
// own context ended.
func (s *EventStream) Err() error

func (c *clientImpl[Spec, Status]) EventsWatch(
    ctx context.Context, id ObjectID, opts ...EventOption) (*EventStream, error)
```

`Event` gains `ResourceVersion int64` — without it a caller cannot checkpoint a
delivered run, which is the whole point of the resume. It is populated on
`EventsList` and `LoadEvents()` reads too; one decode path, one shape.

**Why a handle rather than the object watches' `(snapshot, chan, error)`.** A
stream needs somewhere to report the two failures that are not the caller's
cancellation (`ErrWatchTooOld`, `ErrStopped`). Object watches carry them on a
`Failed` change, and events have no change type to carry one — the
[naming ADR](../adr/2026-07-27-noun-verb-naming.md) is explicit that a watch over
a log streams the value itself, and the [events ADR](../adr/2026-07-27-events-api.md)
says why: an append-only log has nothing for a change type to say. Adding an
error field to `Event` would put a stream concern on a log row. The handle keeps
`<-chan Event` intact and gives the terminal error one place to live.

### 2. Resume rides `EventOption`, which grows a config

`EventOption` becomes `func(*eventConfig)` over

```go
type eventConfig struct {
    query      storeapi.EventQuery
    resumeFrom *int64
}
```

`resolveEvents` already funnels every call site, so the change is mechanical;
`EventsList` and the eager loader read `.query` and nothing else.

```go
// WithEventsResumeFrom streams the runs above rv instead of taking a snapshot.
// EventsWatch only — EventsList and LoadEvents ignore it, the way an Option
// ignores a target it does not recognise.
func WithEventsResumeFrom(rv int64) EventOption
```

A separate `EventWatchOption` type (the object watches' shape) was rejected: the
filters are meaningful on both reads, and splitting the type would force either
two variadics or a `WithEventFilters(...)` wrapper on every watch call.

`WithEventLimit` now bounds **the snapshot only**; its godoc says so. A limit on a
tail is not expressible — the stream has no end to count back from.

### 3. A commit wakes the reader; the tick stays and slows to 30s

`ControllerClient.EventsAdd` publishes the object id to a new hub on
`Store.AfterCommit`:

```go
// objectEventHub, on Beehive, alongside kindWriteHub.
type objectEventHub struct{ watchHub[ObjectID, struct{}] }

func (bh *Beehive) signalEventsWritten(ctx context.Context, id ObjectID)
```

Keyed by id, not kind: the read is per object, and a kind-wide signal would wake
every event subscriber of that kind on every write. The signal carries no value —
the reader reads its position from the store — so a burst collapses into one
pending slot, exactly as `signalKindWritten` does. Closed in `Stop` next to
`kindWriteHub`.

**This is not a new push exception.** A tick stays, so the event watch is still a
periodic-scan driver with a wake in front — the object tail's shape, not the
schedule watch's. It has to: an event written by a second process, or issued
straight to the `Store`, publishes nothing, and the tick is what covers it.
Ninth user of `AfterCommit`.

**The tick moves from `watchPollInterval` (1s) to `watchFloorInterval` (30s),
and the 1s knob is deleted with it.** Once the wake carries the latency
requirement, the tick's only remaining job is foreign writers — which is exactly
the job the object tail's 30s floor already does, and the cadence it was sized
for. Keeping 1s would be thirty times the query rate for that job, per stream
(§4 says why per stream is the number that matters). So `watchPoll`,
`watchPollInterval`, `defaultWatchPollInterval` and the unexported
`withWatchPollInterval` all go: `EventsWatch` was their only consumer, and
`staticcheck -checks=all` would flag them the moment it stops calling them. The
events reader takes `bh.watchFloor()` and `bh.watchBackoff()` unchanged — one
watch cadence, one retry ladder, both already documented as "what a healthy quiet
kind costs".

This is a **latency regression on the pull path**: an event written by a second
process is seen in up to 30s rather than 1s. That is the trade the object tail
already made, and it is the honest one — a 1s tick advertises a cross-process
latency the system does not otherwise promise anywhere.

### 4. One reader per watch — no shared tailer

Each `EventsWatch` runs its own reader over its own cursor. The object tail is
shared per kind because `object_writes` has no per-object index and N watches on
one kind would otherwise be N scans of the same log; neither holds here. The
events read is already per object and already indexed, and the interesting
fan-out (many objects, one panel) is *distinct* ids, which a shared tailer would
not collapse anyway.

**What one stream costs, stated rather than implied.** A `watch.Receiver` bound
to its id for life (`Close` is the only unwatch), its `Chan()` goroutine, the
reader goroutine, a timer and a `rategate.Single`. Quiet cost is one
`EventsListSince` seek per stream per tick, plus one per wake. The named
consumer — a panel over a hundred objects — is therefore a hundred readers and a
hundred seeks per 30s, against today's one covering `MAX()` per object per
second. It is cheaper than what it replaces at any fan-out, and it is still
linear in streams: a consumer wanting one reader for many objects wants a
different API (a kind-wide event watch), and this spec does not build one.

Two watches on one id cost two queries per wake — accepted, and revisited only
if a consumer appears that opens many streams on **one** object.

**Lifetime: a reader exists only while its watch does.** It is built inside the
`EventsWatch` call and ends with the caller's context, so an object nobody
watches costs no goroutine, no timer and no query — `signalEventsWritten` on an
unwatched id is a map lookup in the hub and a return. Nothing here needs the
tailer's lease machinery (`refs`, `tailMu`, the identity check on eviction, the
"registered is not healthy" rule): those exist because one tailer is shared
between the watches of a kind, and this reader is shared with nobody.

What it does owe is teardown order, LIFO and on every exit path including the
error returns before the stream is handed back: close the wake receiver, then
close `Events`. A `watch.Receiver` is bound to its key for life and `Close` is
the only unwatch, so a missed one leaks the receiver and leaves a slot the hub
keeps filling for a stream nobody is reading. `Stop` closes the hub from the
other side, which is what ends a live stream with `ErrStopped`.

The reader is the object tail's loop with a smaller body:

- select over the wake receiver and a floor timer; wakes are dropped while
  backing off, for the reason `objectTailer.run` records.
- every drain start floored by a `rategate.Single` on `bh.watchScanMinInterval`
  (100ms, shared knob, same rationale: a write stream must not hold the single
  connection away from the writers waking it).
- one drain reads pages of `eventPageCap` (256 — one object's runs, against the
  tail's 512 entries) until a page comes back short, bounded by
  `defaultEventPagesPerDrain` (2).
- `bh.watchBackoff()` as the retry ladder, unchanged and unforked.
- **no `EventsMaxVersion` pre-gate.** The tail pays a scalar read to avoid a
  listing; here the listing *is* the seek — an empty page is the same index probe
  the pre-gate would be — so gating would double the quiet cost to answer the
  question the page already answers. This is the one place the reader deliberately
  does not copy the tail, and a reviewer will ask.
- delivery ascending by `resource_version`, which `ORDER BY` gives for free: one
  reader, one cursor, no fan-out to re-order. Unlike the tail, nothing here has to
  earn it.

**No merge, and that is the interesting consequence.** The tail conflates per
object because a shared hub must not block on one slow subscriber. This reader
has no hub: `Events` is unbuffered, like the object watches' channel, and the
drain sends through `sendOrDone`. So a consumer that stops reading blocks the
drain, which pins the cursor, which is precisely what makes `ErrWatchTooOld`
reachable on a *live* stream rather than only on a resume — the case §6 exists
for. A caller that cannot keep up should stop reading, take the error, and
resubscribe; that is a cheaper contract than a buffer that only moves the
threshold.

### 5. Filters apply in Go, on the way out

`EventsListSince` takes no *filter*. The reader applies Category / Type / Reason /
Since to each page itself and advances the cursor by the **unfiltered** page's
last `rv`. Pushing the predicate into SQL would leave the reader unable to say
how far it scanned without a second return value, and the predicates are four
field comparisons. Same shape as a watch subscriber decoding and dropping for
itself.

It does take the caller's **category**, and that is not a filter on the page —
it selects which horizon the call reports. See §6.

### 6. Retention gets a horizon

**Why: a read must not imply absence it cannot vouch for.** `EventsListSince(id,
cat, C, n)` returning the survivors above `C` is an unqualified claim that those
are *all* the runs above `C`. A trimmed run and a run that was never written are
the same bytes on the wire, so once retention has been through, that claim is
stronger than the store can back. The horizon is what qualifies it: complete
above `trimmed_through`, unknown below. That is a property of the read itself,
not a feature for a particular consumer — it holds whether or not anyone is
exporting.

Two bounds on what it says, both of which the ADR should carry:

- **It reports that there is a hole, not what was in it.** `trimmed_through` is a
  boundary, not an audit trail. The only useful answer to it is to resubscribe
  with a snapshot, which is exactly what `ErrWatchTooOld` already asks for.
- **Only a resume, or a reader stalled past retention, can read into the unknown
  range.** A caught-up live reader's cursor sits above everything trimmable, so
  its survivors-above-`C` claim is sound with no horizon consulted.

The object watches reached the same rule from the harder case — a missed write
leaves a mirror permanently wrong — and their horizon does a second job besides
(it folds into `ObjectWritesMaxVersion` to keep the tail's position monotone)
that has no counterpart here, since §4's reader has no position read at all. The
shared part is only this: neither read may imply completeness it does not have.

New table, amended into `sqlite/migrations/0001_init.sql` in place (pre-release;
`TestTheSchemaIsOneMigration` is the tripwire):

```sql
-- What event retention has removed, per timeline. A resume BELOW
-- trimmed_through is refused: the log has a hole under it. A cursor sitting
-- exactly on it has lost nothing. EventsSweep is the only writer.
--
-- Keyed by (object_id, category) to match the ring cap's own partition: the cap
-- trims each timeline independently, so an object-wide horizon would let a
-- chatty category refuse every resume on a quiet one.
CREATE TABLE events_horizon (
    object_id       INTEGER NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    category        TEXT    NOT NULL,
    trimmed_through INTEGER NOT NULL, -- highest resource_version trimmed here
    PRIMARY KEY (object_id, category)
) STRICT, WITHOUT ROWID;
```

`WITHOUT ROWID` for the `object_writes_horizon` reasons: a composite key a rowid
table would store twice, tiny rows, and reads always by full primary key — or by
its `object_id` prefix, which is the unfiltered watch's `MAX` over the object's
timelines.

`EventsListSince` reports the horizon for the caller's category, or the max
across the object's timelines when the caller filtered on none. So a stream
watching `"connection"` is refused only for a trim in `"connection"`, and an
unfiltered stream is refused for a trim anywhere — which is correct in both
directions rather than conservative in one.

`EventsSweep` keeps its signature, its one transaction and both `Exec`s.
The horizon is computed **before** each delete, from the same predicate, in the
same transaction:

```sql
INSERT INTO events_horizon (object_id, category, trimmed_through)
SELECT object_id, category, MAX(resource_version) FROM events
WHERE <the delete's own predicate>
GROUP BY object_id, category
ON CONFLICT (object_id, category) DO UPDATE SET
  trimmed_through = MAX(trimmed_through, excluded.trimmed_through);
```

then the `DELETE` runs unchanged. No `RETURNING`, so `RowsAffected` still
supplies the returned count, no deleted row is materialised in Go, and no
half-read `sql.Rows` can sit on the store's single connection between two
statements of one transaction. It costs one extra scan of a set the delete is
about to scan anyway.

Retention is **off by default**, unlike the write log's — so the table stays empty
for anyone who has not opted in, and every resume is honoured. That asymmetry is
already recorded in the write-log ADR and does not change here.

**The horizon inherits the sweep's clock dependence.** Both bounds select by
`last_at`, and the store already distrusts that clock — `latestEventRun` orders by
`id` because a backwards step could otherwise name an older run latest. Here the
consequence is sharper than a mis-ordered read: `trimmed_through` is a `MAX(rv)`
over rows chosen by `last_at`, so a backwards step that makes a freshly extended
run look old trims it and slams the horizon up to the head, ending every live
stream on that timeline with `ErrWatchTooOld`. Low frequency, whole-timeline blast
radius, and no cheap fix that does not re-key retention off the clock. Recorded
rather than fixed: the horizon is not exact, and the ADR should not claim it is.

Note which way it fails, though. A skewed clock makes the horizon **over**-report
— streams are told "I might not know" about a range that lost nothing, and they
answer by resubscribing. Under the rule above that is the safe direction: the
qualification is too loud rather than absent, and no read ever implies a
completeness it lacks.

### 7. A collected object ends its streams

This is §6's rule at full scale, and the horizon cannot state it. `events` and
`events_horizon` both cascade off `objects`, so a physical delete takes every
unread run *and the record that they existed* — leaving an empty page and a zero
horizon, which is the read implying "no events here" about an object whose entire
log was deleted. The strongest possible form of the implication §6 exists to
prevent, and the one the table is structurally unable to qualify. The `foreign`
latch compounds it: the loop never re-reads meta, so the stream goes silent
forever.

So this section is not a nicety on top of §6; it is the same rule applied where
§6's mechanism runs out, which is why it needs a sentinel of its own rather than
a horizon value.

**Decision: the stream ends, reported as `ErrNotFound` on `Err()`.** Ids are
never reused, so a collected object's log can never grow again; a caller blocked
on that channel is waiting on something that cannot happen, and the handle now
has somewhere to say so.

Detected in the store, not by a second query from the reader: when
`EventsListSince` finds no rows above `afterRV` **and** no horizon row, it probes
`objects` by primary key and returns `ErrNotFound` if the row is gone. One extra
scalar seek, paid only on a drain that found nothing — quiet drains happen on a
30s tick or on a wake that raced a delete, so the probe is off the hot path. A
page with rows proves the object exists and skips it.

The same answer covers the unresolved-id case the old latch handled by going
silent: while `ObjectsGetMeta` reports "not found", the reader keeps re-checking
each pass, and an id later created under **another** kind ends the stream with
`ErrNotFound` instead of streaming nothing for the life of the process.

## Store surface

One new method, alphabetical in the `Store` interface:

```go
// EventsListSince returns id's runs above afterRV, ascending by
// resource_version, at most limit of them, together with the retention horizon
// (0 when nothing has been trimmed). A run is returned when it was appended or
// extended above afterRV — an extend re-samples resource_version — so the page
// is exactly what changed. Not kind-scoped, and the page is unfiltered: the
// caller filters what it asked for.
//
// category selects which horizon is reported: that timeline's, or the max over
// the object's timelines when nil. ErrNotFound when the page is empty, no
// horizon is recorded and id holds no object — a collected object, whose log
// cascaded away and can never grow again.
EventsListSince(ctx context.Context, id ObjectID, category *string, afterRV int64, limit int) (
    runs []Event, trimmedThrough int64, err error)
```

Shaped after `ObjectWritesListSince`: page and horizon in one call, so the
horizon check costs no extra round trip and cannot be skipped.

```go
// EventsSnapshot returns id's runs matching q and the log position the listing
// is complete as of, read in one transaction so no write falls between them.
// The position is what EventsMaxVersion reports. Not kind-scoped; an unknown id
// reads as no runs at position 0.
EventsSnapshot(ctx context.Context, id ObjectID, q EventQuery) (
    runs []Event, at int64, err error)
```

Shaped after `ObjectWritesSnapshot`, and for its reason: two reads cannot answer
"these runs, as of this position" — whichever order they run in, a write between
them is either delivered twice or dropped. `sqlite`'s existing `snapshot` helper
is the same three lines around `Within`; the query is `EventsList`'s, so the two
share their `WHERE` builder rather than growing a second copy of the filters.

`Store.EventsAdd` **loses its return value** (`error` alone) — the store method
only; `ControllerClient.EventsAdd` already returns `error`, so no public
signature moves here. Its two branches also drop `RETURNING … ` and the
`scanEvent` that reads it, which is where the saving actually is: one fewer row
decode on every event written. The
[write-shapes ADR](../adr/2026-07-30-store-write-shapes.md) always wanted that;
[`TODO.md`](../TODO.md) held the exception open for exactly this push path, whose
condition was "revisit only if an events push path is ruled out for good". It is
not ruled out — it is built here, and it does not read the value: the wake carries
no payload, the reader reads its position from the store, and a value-carrying
wake would be a second delivery path with its own ordering and coalescing rules
against the paged one. The exception closes; delete that `TODO.md` entry with it.

`TODO.md`'s **third** events entry — the one asking `EventsAdd` to take an
`EventsAddInput` rather than a whole `Event`, and its cross-reference further
down — is a different rule about a different half of the signature. It survives
this spec untouched, and the take-shape change is not in scope here.

## Subscribe-time reads

`EventsWatch` does its first reads synchronously, so a caller learns about a bad
resume from the error return rather than from a stream that ends immediately:

1. Register the wake receiver **before** any read — a write landing between the
   two would otherwise be missed by both. (`newObjectTailer` records the same
   invariant.)
2. `ObjectsGetMeta(id)`. Another kind's id → `ErrNotFound` now, rather than
   today's silent-forever stream; the `foreign` latch and its `<-ctx.Done()` limb
   go away. A **missing** id still streams nothing until it is created, so the
   kind check stays in the loop for that case alone — and per §7 it now resolves
   either way rather than latching only on success.
3. Resume: one `EventsListSince(id, category, rv, eventPageCap)`. It carries the horizon,
   so `rv < trimmedThrough` → `ErrWatchTooOld` here. An empty page then costs one
   `EventsMaxVersion`, and `rv > max(maxVersion, trimmedThrough)` →
   `ErrWatchTooNew` — the position did not come from this store, and unreported it
   would hold the cursor above every later run and drop them all silently. (The
   fold against the horizon is what stops retention on the newest run from turning
   a legitimate cursor into a false `ErrWatchTooNew`; `EventsMaxVersion` alone can
   fall.)
4. Snapshot: one `EventsSnapshot(id, q)`. Runs and position from one
   transaction, so the stream that follows starts exactly above what the caller
   already holds: no run is dropped, and none is delivered twice. Two reads
   cannot do that — position-then-list re-delivers, list-then-position drops —
   and the object watches took the same decision to the store rather than
   living with either.

## What must stay true

- **Every push has a pull behind it.** The 30s floor tick is the pull, and a test
  proves it: write an event straight through the `Store`, issuing no wake, and the
  stream must still deliver.
- **A cursor is never advanced past an unread page.** It moves only past a
  successful, delivered page — a failed read costs a retry, not a run.
- **Delivery is ascending by `resource_version`,** within a page and across pages.
- **The snapshot and its position are one read.** A run in `Runs` is never also
  delivered on the stream, and a run written during the subscribe is never lost
  between them. A *re-delivery* is still expected and correct when the run is
  later extended — that is the log moving, not the subscribe leaking.
- **`EventsAdd` still bumps no object `resource_version` and writes no
  `object_writes` entry.** That is what makes an event safe to emit inside a
  dependency cycle, and this spec touches none of it: the new hub is an event hub,
  it wakes no reconcile, and case notes in
  [`reconcile-triggers.md`](../reconcile-triggers.md) ("`EventsAdd` wakes nothing",
  "`EventsWatch` wakes nothing") stay true as written about *reconciles*.
- **No tombstones.** Retention deletion is still not a change; it is reported only
  as a refused resume or a failed stream, never as a delivered value.
- **A caught-up subscriber cannot fall below the horizon** — its cursor is at the
  head, and a trimmed run is one it already delivered. Only a stalled or blocked
  reader can, which is the case `ErrWatchTooOld` exists for.

## Work plan

Each stage builds, vets and passes `go test ./...` on its own.

1. **Store.** `events_horizon` in the migration; `EventsListSince`;
   `EventsSnapshot`; `EventsSweep` writing horizons before each delete.
   `fakeStore` gains the two methods. No behaviour changes above the store yet.
2. **Types and options.** `Event.ResourceVersion` — `storeapi.Event` already
   carries it and `eventFromRaw` drops it, so this is one line in a mapper, not a
   new decode path. Plus `eventConfig` and `WithEventsResumeFrom`.
3. **The reader,** with `EventStream` landing in the same stage. Rewrite
   `eventswatch.go` around the cursor; add `objectEventHub`,
   `signalEventsWritten` and the `EventsAdd` hook; delete the `seen` map, the
   `foreign` limb, and `watchPoll` / `watchPollInterval` /
   `defaultWatchPollInterval` / `withWatchPollInterval` with them.
   `EventStream` cannot ship in stage 2: `failed` would have no writer and `Err`
   no caller, and `staticcheck -checks=all` — which CI runs — flags exactly that.
4. **`Store.EventsAdd` returns `error`,** and both its branches lose `RETURNING`
   and `scanEvent`. Store, sqlite, `fakeStore`.
5. **Docs.** New ADR; amend the events ADR's "Reads and retention"; widen
   `ErrWatchTooOld` / `ErrWatchTooNew`'s godoc, which is written about the write
   log and now has a second producer; put `ErrWatchTooNew` in `EventsWatch`'s own
   godoc, where today it appears only in this spec's prose. README (drivers table
   row — cadence *and* the wake, the Events section's poll paragraph, the
   `Client` interface listing, the `EventOption` note); CLAUDE.md (the events
   bullet, the driver list's event-watch entry, `AfterCommit`'s user count
   8 → 9); `reconcile-triggers.md` (the two "wakes nothing" notes, narrowed to
   reconciles); `TODO.md` (the `EventsWatch` and return-value entries removed,
   the take-shape entry left standing); `examples/events/main.go` switched to the
   stream, since a reconnecting panel is the consumer this is for. Delete this
   spec.

Breaking: `feat(events)!:` — `EventsWatch`'s signature, `Event`'s new field, and
the pull-path cadence moving from 1s to 30s.

## Tripwires and tests

New, in `eventswatch_test.go` unless noted:

- an `EventsAdd` is delivered before a tick could fire (floor interval set long,
  so only a wake can deliver) — the push path.
- an event written straight through the `Store` is delivered on the tick — the
  pull path, with no knob disabling the push.
- the snapshot carries the current runs and a position, and the stream starts
  exactly above it: an event committed while the subscribe is in flight is
  delivered once — in `Runs` or on the channel, never both, never neither.
- a resume delivers exactly the gap, with an empty `Runs` and the position echoed.
- an **extend** re-delivers the run with a higher `ResourceVersion` and a bumped
  `Count`, and is not a second run.
- ordering: a burst of writes across runs arrives ascending by
  `ResourceVersion`.
- filters apply to the tail, and `WithEventLimit` bounds only the snapshot.
- two streams on one id with different filters are each served in full, and one
  of them resuming from its own checkpoint does not disturb the other.
- a foreign id is `ErrNotFound` at subscribe; an id that does not exist yet gives
  an empty snapshot and delivers once it is created and an event lands; an id
  created later under **another** kind ends the stream with `ErrNotFound` rather
  than going silent.
- a collected object ends its live stream with `ErrNotFound` on `Err()`, and a
  resume against a collected id fails the same way at subscribe (§7).
- the cursor does not advance past a send the consumer never took: cancel mid
  batch, resume from the last delivered `ResourceVersion`, and the undelivered
  runs arrive.
- retention: a resume below the horizon is `ErrWatchTooOld` at subscribe; a
  horizon crossing a live stalled stream ends it with the same error on `Err()`.
- the per-timeline horizon: trimming a chatty category does **not** refuse a
  resume on a quiet one of the same object, and an unfiltered resume below either
  is refused. This is the test that pins §6's key choice.
- a position above the head is `ErrWatchTooNew`; a position above a
  `EventsMaxVersion` that retention lowered is **not**.
- `Stop` ends the stream with `ErrStopped`; a caller's own cancel closes it with
  `Err() == nil`.
- lifetime: cancelling the caller's context leaves no reader goroutine and no
  registered receiver behind — including when the subscribe fails at `ErrWatchTooOld`
  / `ErrWatchTooNew` / `ErrNotFound`, which return before any stream exists — and
  a subsequent `EventsAdd` on that id publishes to nobody.
- the drain floor: a burst of commits costs at most one drain per
  `watchScanMinInterval`, mirroring the tailer's throttle test.

Store-side (`sqlite`): `EventsListSince` ordering, paging, per-category vs
object-wide horizon reporting, and `ErrNotFound` on a collected id;
`EventsSnapshot` returning a listing and a position with no write able to land
between them, and honouring the same filters as `EventsList`; `EventsSweep`
recording a per-timeline max, still returning its deleted count from
`RowsAffected`, and cascading with the object; an index-plan test for
`EventsListSince`, mirroring `TestEventsMaxVersionUsesCoveringIndex`;
`TestTheSchemaIsOneMigration` still green.

Existing tests that must change rather than break: everything asserting
`EventsWatch`'s two-value return or its snapshot-through-the-channel behaviour.

## Done when

- A subscriber sees an event at commit time, and still sees one written by
  another process within the tick.
- `EventsWatch` hands back a snapshot, a position and a stream, and a caller can
  reconnect from a checkpoint without re-reading the log.
- **No read implies an absence it cannot vouch for.** A range retention has been
  through is refused rather than answered, and a collected object says
  `ErrNotFound` rather than looking like a quiet log. Bounded by what the
  mechanism can express: the horizon is per timeline, it is only as trustworthy as
  the sweep's clock and errs toward over-reporting (§6), and it says *that* a hole
  exists, never what was in it.
- `Store.EventsAdd` returns `error`, and `TODO.md` carries neither the
  `EventsWatch` entry nor the return-value entry — while its take-shape entry is
  still there, untouched.
