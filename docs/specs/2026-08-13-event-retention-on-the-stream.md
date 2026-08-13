# Report the event retention bound on `EventStream`

## Problem

`WithEventRetention(perTimeline, maxAge)` is configured once at `New`, and the
GC sweeper prunes runs below it. The event watch never reports a prune: it
delivers the snapshot plus what grows above it, which is the right shape for a
log you tail from a position — a trim below the cursor is not a change above it.

The consequence lands on the consumer. A caller that keeps `Runs` in memory and
appends what `Events` delivers accumulates without bound, and slowly holds more
than the store does. To bound its own list it has to know the number the server
enforces, and nothing on `EventStream` says it. Callers hardcode the value to
mirror the server's config; when the config changes, the mirror is silently
wrong and nothing tells them.

## Decision

Report the configured retention on `EventStream`, beside `ResourceVersion`.
Purely additive: no store change, no schema change, no behaviour change on any
existing field.

**This spec is one change and nothing else**: an `EventRetention` type, a field
on `EventStream`, and the copy that fills it — plus their godoc and tests. No
existing line of `eventswatch.go` moves except `WatchEvents`' construction of
the stream. Anything that arrives with it belongs in the *Out of scope* section
below or in its own spec.

This is a *configuration* readout, not a per-stream fact. It says what bound the
sweeper enforces for this process, so a consumer can size its own buffer from
the server's number instead of a copy of it. It does not report what was
trimmed, when, or whether this stream's object was affected — the horizon
(`events_horizon`) already answers "did a resume fall below a trim", with
`ErrWatchTooOld`, and that stays exactly as it is.

**On the stream, not on `Client`.** A `Client`-level accessor would serve
`ListEvents` too and would report the value once rather than per stream, and the
convention would allow it (`GetEventRetention`, a read with no object behind
it). It is rejected for this change: the caller who needs the number is the one
holding a growing list, they are already holding the struct that grows, and a
field on it needs no second call to correlate — the buffer size and the position
it is a buffer of arrive together. It also keeps the "additive field" claim
literally true. If `ListEvents` later turns out to need the same number, the
accessor is the answer *and it does not duplicate this*: the field would be
documented as reading the same configuration, the way `ResourceVersion` and
`Events().List` both speak about one log.

## Surface

In `types.go`:

```go
// EventRetention is the event-log bound the GC sweeper enforces, as configured
// by WithEventRetention. A zero field is that bound unset.
type EventRetention struct {
    // PerTimeline caps each (object, category) timeline to its newest N runs.
    // Zero means no count bound.
    PerTimeline int
    // MaxAge drops runs whose window ended more than MaxAge ago, across every
    // timeline. Zero means no age bound.
    MaxAge time.Duration
}
```

In `eventswatch.go`, one field on `EventStream`:

```go
type EventStream struct {
    Runs            []Event
    ResourceVersion int64
    Events          <-chan Event
    // Retention is the bound the sweeper enforces on this log, so a consumer
    // holding runs in memory can size its own buffer from the server's number.
    // Zero fields mean unbounded. Read-only, and fixed for the process.
    Retention EventRetention
    ...
}
```

A struct rather than two flat `RetentionPerTimeline` / `RetentionMaxAge`
fields: the two bounds are the option's own pair, they are read together, and a
named type is where the "zero means unbounded" contract can live once instead of
on each field. It also gives a third bound, if one is ever added, somewhere to
go without widening `EventStream` again.

## Implementation

`Beehive` already holds `eventRetentionPerTimeline` and `eventRetentionMaxAge`
(`beehive.go`), set by `WithEventRetention` and read only by the sweeper. The
whole change is to copy them onto the stream where `WatchEvents` builds it
(`eventswatch.go`), so both the snapshot path and the resume path carry the
same value:

```go
stream: &EventStream{Retention: EventRetention{
    PerTimeline: max(0, c.bh.eventRetentionPerTimeline),
    MaxAge:      max(0, c.bh.eventRetentionMaxAge),
}},
```

**The `max(0, …)` is required, not defensive.** `WithEventRetention` validates
nothing (`options.go:417`), unlike its neighbours, so
`WithEventRetention(-5, 0)` stores `-5`. The sweeper gates on `> 0` in both
`Beehive.eventRetentionSweep` and `Events().Sweep`, so a negative bound behaves
exactly as unset — and the field must report what is *enforced*, or a consumer
sizing a buffer off `PerTimeline` gets a negative cap out of a struct whose
godoc says zero means unset. Clamping at the copy is what keeps this change
additive. Validating in the option instead — rejecting a negative with
`ErrInvalidOption`, as `WithWatchFloorInterval` does — is defensible and is a
better fix for the underlying gap, but it is a behaviour change to a construction
path this spec otherwise leaves alone: raise it separately, and do not fold it
in here.

Set at construction, before `start`, and never written again — the field is
read-only to the reader goroutine as well, so it needs no synchronisation and
no note in the "fields below the stream are run's alone" comment.

`WithEventRetention` is documented "meaningful only at `New`" and nothing
mutates the two fields after that, so the value a stream reports cannot go stale
relative to the process that produced it. Do not add a setter, and do not
re-read the fields in the reader loop: if retention ever becomes reconfigurable
at runtime, that is a separate decision about how a live stream learns of a
change, not something to leave a seam for now.

## Edge cases the implementer should not have to guess at

- **Zero is unbounded, not "unknown".** A `Beehive` built without the option
  reports `EventRetention{}`, which is the accurate answer: the sweeper skips a
  zero bound and the log is unbounded. There is no third state to encode.
- **A negative bound reports as zero**, per the clamp above — it is unenforced,
  so "unset" is the true answer and not a rounding of one.
- **`PerTimeline` is not a bound on the stream.** It caps one `(object,
  category)` timeline. A watch with no `WithEventCategory` covers every category
  on the object, and categories are open-ended, so `PerTimeline` bounds the
  stream's total only when the watch is scoped to one category. The godoc and
  the README must say this outright, since sizing a buffer at `PerTimeline` on
  an unscoped watch is exactly the mistake this field invites.
- **`WithEventType` / `WithEventReason` / `WithEventLimit` do not change it.**
  Retention is enforced over the whole timeline, not over the filtered view. A
  filtered stream sees at most what an unfiltered one would.
- **The server may transiently sit above the cap.** The sweeper enforces on
  `WithGCInterval`, so a burst lives above `PerTimeline` until the next sweep. A
  consumer capping its own list at exactly `PerTimeline` therefore prunes no
  later than the server and sometimes earlier, which is the safe direction for a
  memory bound. `WithEventRetention`'s godoc already says the burst part — reuse
  its wording rather than writing a second description, and do not try to make
  the numbers agree instant to instant.
- **`MaxAge` is measured on a run's end** (`LastAt`), so a run that keeps being
  extended never ages out. Same wording as `WithEventRetention` — do not invent
  a second description of the same rule.
- **The tests that construct `EventStream{}` directly** (e.g.
  `eventswatch_test.go:606`) keep compiling and need no edit: the new field
  zero-values. Do not add retention assertions to them — they are about other
  things, and the tests below cover this.

## Out of scope

- **`ListEvents` has the same gap** and does not get the field: it returns
  `[]Event`, with nowhere additive to put it, and a one-shot read is not the
  case that accumulates. If it turns out to matter, the answer is a `Client`
  accessor, which is a separate decision.
- **Reporting an actual prune** — a signal that runs were trimmed below the
  cursor. Deliberately not done: the watch's contract is "the snapshot, and what
  grows above it", and a trim event would be the one thing it delivers that is
  not a run. The horizon plus `ErrWatchTooOld` already covers the case where a
  trim can hurt a reader.
- **Object-write-log retention (`WithWriteLogRetention`) on the object
  watches.** The same argument may apply; it is a different surface and a
  different decision. Do not fold it in.

## Tests

In `eventswatch_test.go`, table-driven where it fits:

1. A stream from a `Beehive` built with `WithEventRetention(20, time.Hour)`
   reports `EventRetention{PerTimeline: 20, MaxAge: time.Hour}`.
2. A stream from a `Beehive` built without the option reports the zero
   `EventRetention`.
3. The resume path (`WithEventsResumeFrom`) reports the same value as the
   snapshot path — the field is not tied to `Runs` being populated.
4. Only one of the two bounds set reports only that bound.
5. `WithEventRetention(-5, -time.Hour)` reports the zero `EventRetention` —
   the clamp, pinned.
6. `WithEventRetention(-5, time.Hour)` reports `{0, time.Hour}` — the mixed
   case, which is the one the clamp exists for: `beehive.go:266` skips the sweep
   only when *both* bounds are unset, so this config runs a sweep that enforces
   the age bound and skips the count bound, and the field must say so.

No store-level or sweeper test changes: the sweeper's behaviour is untouched.
That includes the negative-bound cases above — assert what the *field* reports,
not what the sweeper does with the same config. The sweeper's `<= 0` gates are
its own to pin.

## Docs

- Godoc on `EventRetention` and on the field, per the repo's 1–3 sentence rule,
  carrying the "per `(object, category)`, so it bounds the stream only when the
  watch is category-scoped" caveat.
- `README.md`: one sentence in the `WatchEvents` paragraph (~line 664) and a
  cross-reference from the `WithEventRetention` paragraph (~line 672), plus the
  field in the `EventStream` shape if it is spelled out there.
- On ship: fold the rationale into
  [`docs/adr/2026-08-06-event-retention-is-a-ring-per-timeline.md`](../adr/2026-08-06-event-retention-is-a-ring-per-timeline.md)
  as a short section on why the bound is readable from the stream and why a
  prune is still not an event, update `CLAUDE.md`'s events bullet if the
  sentence there needs it, and delete this spec.
