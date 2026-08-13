# The event reads take an id

- **Status:** Accepted — implemented in `eventswatch.go`, `objectswatch.go` and `client.go`.
- **Date:** 2026-08-13

## Context

`Client` has three ways into an object's event log — `ListEvents`,
`GetLatestEvent`, `WatchEvents` — and they disagreed about what the id means.
The first two queried `Events().List`/`GetLatest` by id alone. The third read the
object's row first and returned `ErrNotFound` when its group/kind differed from
the client's. One client, one id, two answers.

The unscoped direction failed quietly, which is what made the split worth
closing rather than tolerating: `ListEvents` on another kind's id handed back
that object's runs and said nothing.

The scoping also reached further than a subscribe-time check. `drain` re-runs the
probe while `!resolved`, and a foreign-kind `ErrNotFound` is terminal, so a watch
opened on an id that did not exist yet was *killed* if that id was later created
under another kind — a caller that never wrote a foreign id down could still lose
a stream to this.

## Decision

All three reads take an `ObjectID` and read that object's log, whatever kind
holds it. `checkKind` becomes `checkExists`: it keeps the `Objects().GetMeta`
read and the `resolved` latch, and drops the comparison.

The `events` table has no group or kind column. The scoping was an extra row read
bolted on top of a per-object log to enforce a partition the storage does not
have, and two of the three reads never paid for it. It also matches the rule the
client already follows for reads *through* an id rather than *of* it — `GetOwner`,
`ListDependencies`, `ListDependents`, `ListOwned` all run their query with no
kind check and document it. `Get`/`GetByName` are kind-scoped because they decode
into the client's `Spec`/`Status`; an `Event` decodes the same for every kind.

**Writes keep the scope.** `ControllerClient.AddEvent` still returns
`ErrWrongKind`, pinned by `TestControllerClientAddEventWrongKind`. Writing an
observation about another kind's object is a controller reaching outside its
own; reading one is not.

**The `resolved` latch stays, and is now the whole job of the probe.**
`Events().ListSince` reports `ErrNotFound` for an id with no row and no horizon,
which is how a *collected* object ends its stream. Without the latch, "not
created yet" and "collected" would be the same answer.

`WatchEvents` still requires a registered controller for the client's own kind.
That is now plainly a property of the caller rather than of the target — a
registered kind's client may watch a client-only kind's object. `docs/TODO.md`
carries what revisiting it would take.

## Consequences

A watch on a foreign id costs a goroutine, a hub subscription and a
`WithWatchFloorInterval` tick for as long as the caller holds it, where it used
to fail at open. That is the feature; it also means a typo'd id is a live watch
rather than an error, and a watch on an unknown id keeps waiting rather than
ending when a foreign object claims it.

The wake path was already cross-kind, which is what makes the unblocked case
usable rather than merely legal: `eventWriteHub` is keyed by `ObjectID` and
`signalEventsWritten` fires from any `AddEvent` commit whatever the writer's
kind. A foreign-id watch is commit-woken, not left on the floor tick.

Read volume is unchanged and one comparison per resolve goes away. `horizonErr`
took a `GroupKind` only to name the log in its message; it now takes a subject
string, as `tooNewErr` already did, and the event reader names the object. With
that, `eventReader.gk` had no remaining use. The object watches pass
`"group/kind"` and their messages are byte-identical.

This is a breaking change for a caller relying on `WatchEvents` returning
`ErrNotFound` for a foreign id, but no documented contract is withdrawn: the
scoping appeared in neither the godoc nor the README, only in
`TestWatchEventsIsKindScoped` and a comment.

### Alternatives considered

- **Scope all three.** Makes three methods pay a row read per call to enforce a
  partition the table does not have, and leaves the caller needing a public kind
  lookup we would then have to add and support. It also answers the reported use
  case — subscribe to an object's events knowing only its id — with "first go
  find out what kind it is".
- **Keep `eventReader.gk` for the error message alone.** The message would name
  the client's kind for a log belonging to another object, which is the exact
  confusion this change exists to remove.
- **A cheaper existence probe than `GetMeta`.** The read happens once per watch,
  and again per pass only while the object is absent. Swapping it also means
  reworking the fake store's `metaRead` hook, which counts `GetMeta`
  specifically and is the only clock an unresolved stream's tests have.
