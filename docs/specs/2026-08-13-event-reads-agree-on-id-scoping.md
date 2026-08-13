# The event reads agree: all three read by id

Status: planned. Target: the next release (breaking, pre-1.0).

## The decision

**`WatchEvents` stops scoping its read to the client's kind.** All three event
reads — `ListEvents`, `GetLatestEvent`, `WatchEvents` — take an `ObjectID` and
read that object's log, whatever kind holds it. Writes are untouched:
`ControllerClient.AddEvent` stays kind-scoped and keeps returning
`ErrWrongKind`.

Today the three disagree. `ListEvents` and `GetLatestEvent` query
`Events().List`/`GetLatest` by id alone; `WatchEvents` calls
`eventReader.checkKind`, which reads the object's row and returns `ErrNotFound`
when the group/kind differs from the client's. Same client, same id, two
answers — and the unscoped direction fails quietly, handing back another
object's events.

They are made to agree in the *unscoped* direction, for three reasons:

- It matches what the events table is. `events` has no group or kind column;
  the scoping in `WatchEvents` is an extra `Objects().GetMeta` read bolted on
  top of a per-object log. Two of the three reads never paid for it.
- It matches the convention the client already has for reads *through* an id
  rather than *of* it: `GetOwner`, `ListDependencies`, `ListDependents`,
  `ListOwned` all run their query with no kind check and document it. `Get` and
  `GetByName` are kind-scoped because they decode into the client's `Spec`/
  `Status`; an `Event` is kind-agnostic and decodes the same for every kind.
- It unblocks the reported use case: one subscription that takes an object id
  and streams its events, without the caller first having to learn the id's
  kind. Object ids are unique across kinds, so the id is enough.

The stricter direction (scope all three) is rejected: it would make three
methods pay an extra row read per call to enforce a partition the table does not
have, and it would leave the caller needing a public kind lookup we would then
have to add and support. Exposing the kind lookup publicly is likewise not part
of this work — under this change nothing needs it.

## Surface changes

No signatures change. Behaviour and godoc change:

| Call | Before | After |
| --- | --- | --- |
| `WatchEvents(ctx, foreignID)` | `ErrNotFound` at open | opens and streams that object's log |
| `WatchEvents(ctx, unknownID)` | opens, waits for the object | unchanged |
| …and that id is later created as another kind | stream **ends** `ErrNotFound` | resolves and streams |
| `WatchEvents` on a collected object | stream ends `ErrNotFound` | unchanged |
| `ListEvents` / `GetLatestEvent` | unscoped | unchanged |
| `AddEvent` | `ErrWrongKind` | unchanged |

The third row is the one break that is not visible at open. `drain` re-runs the
check while `!resolved` (`eventswatch.go:272`) and a foreign-kind `ErrNotFound`
is terminal (`isTerminalWatchErr`), so today a watch opened on an id that does
not exist yet is *killed* if that id is later created as another kind. After the
change it resolves and streams. Both directions are strictly more useful, and
the second is the one a caller could hit without ever having written a foreign
id down.

Two costs, both intended and neither free. A watch on a foreign id now runs a
goroutine, a hub subscription and a `WithWatchFloorInterval` tick for as long as
the caller holds it, where it used to fail at open — that is the feature, but it
means a typo'd id is a live watch rather than an error. And a watch on an
*unknown* id now keeps waiting instead of ending when a foreign object claims
that id: nothing wrong, but nothing tells the caller either.

This is a **breaking change** for a caller that relies on `WatchEvents`
returning `ErrNotFound` for a foreign id — but a cheap one, and no documented
contract is withdrawn: the scoping appears in neither `WatchEvents`' godoc
(`client.go:238`) nor the README's events section. It is pinned only by
`TestWatchEventsIsKindScoped` and by the `checkKind` comment. The README edit
below is therefore an addition, not a correction.

`WatchEvents` keeps requiring a registered controller for the *client's* kind —
decided, not deferred. It is now plainly a property of the client and not of the
target: a registered kind's client may watch a client-only kind's object. Land a
`docs/TODO.md` entry with this change saying so, and what would make revisiting
it worthwhile.

**The wake path is already cross-kind**, which is what makes the unblocked use
case usable rather than merely legal: `eventWriteHub` is keyed by `ObjectID`, and
`signalEventsWritten` (`beehive.go:588`) fires from any `AddEvent` commit
whatever the writer's kind. A foreign-id watch is commit-woken, not left on the
`WithWatchFloorInterval` tick. Read volume is unchanged and the change strictly
removes work: one comparison per resolve.

## Implementation

All of it is in `eventswatch.go`, plus godoc in `client.go`.

1. **`checkKind` becomes `checkExists`.** Keep the `Objects().GetMeta` read and
   the `resolved` latch; drop the group/kind comparison and the `ErrNotFound`
   it raises. `ErrNotFound` from the read still means "not there yet" and still
   returns `nil` with `resolved` unset.

   The latch is load-bearing beyond kind scoping and must stay: `drain` refuses
   to read while `!resolved`, which is what keeps a watch opened ahead of its
   object from ending immediately — `Events().ListSince` reports `ErrNotFound`
   for an id with no row and no horizon, and that is how a *collected* object
   ends its stream. Removing the gate would make "not created yet" and
   "collected" the same answer. Its comment should say so; today it explains the
   kind latch instead.

   **Sweep the phrase while you are there.** "Kind check" wording outlives
   `checkKind` in at least four places: the `resolved` field comment
   (`eventswatch.go:132`), `start`'s doc comment (`:146`), the `metaRead` fixture
   comment in `testutils_test.go`, and the comment on
   `TestWatchEventsWaitsForAnObjectThatDoesNotExistYet` — a test this spec keeps
   unchanged in behaviour but not in prose.

   **Keep `GetMeta` as the probe.** A cheaper existence read is not worth it
   here — the read happens once per watch, and again per pass only while the
   object is absent — and it is not free to swap:
   `TestWatchEventsWaitsForAnObjectThatDoesNotExistYet` synchronises on the fake
   store's `metaRead` hook (`eventswatch_test.go:274`, `testutils_test.go:1754`),
   which counts `GetMeta` specifically. Changing the probe means reworking that
   hook.

2. **Widen `horizonErr` to a subject string; delete `eventReader.gk`.**
   `horizonErr(gk GroupKind, what string, cursor, trimmedThrough int64)`
   (`objectswatch.go:328`) becomes
   `horizonErr(subject, what string, cursor, trimmedThrough int64)`, matching
   `tooNewErr` (`:338`), which already takes one.

   - The two object-watch call sites (`objectswatch.go:603`, `:1019`) pass
     `fmt.Sprintf("%s/%s", gk.Group, gk.Kind)` — they legitimately want the
     kind, and their messages stay byte-identical.
   - The event reader passes `fmt.Sprintf("object %d", r.id)`, which is exactly
     what its own `tooNewErr` call already passes (`eventswatch.go:214`).

   With that, `eventReader.gk` has no remaining use (`:201`, `:296` are the
   only two) and is deleted. The `fmt.Errorf` wrapper in `WatchEvents` keeps
   `c.gk`: it names the caller, not the target.

   The alternative — keep `gk` solely for the message — is rejected because the
   message would then name the client's kind for a log belonging to another
   object, which is the exact confusion this spec exists to remove.

3. **Godoc, anchored on `GetLatestEvent`.** The three reads are not adjacent
   (`GetLatestEvent` `:143`, `ListEvents` `:189`, `WatchEvents` `:238`) — the
   interface is alphabetical. Follow the `GetOwner` precedent (`:162`), which
   sits on the first of its family in interface order and speaks for the rest:
   put the family note on `GetLatestEvent`, naming all three and the `AddEvent`
   asymmetry. `ListEvents` keeps its existing "Reads by id, not kind-scoped"
   line. `WatchEvents` gains that one clause beside "Requires a registered
   controller" — it is where the behaviour changes, so it should not be silent —
   and nothing longer.

## Tests

Whitebox, `package beehive`, in `eventswatch_test.go` unless noted.

- **Replace** `TestWatchEventsIsKindScoped` with
  `TestWatchEventsIsNotKindScoped`: create an object of another kind, add an
  event to it, watch it from this client, assert the snapshot carries the run
  and a later `AddEvent` on that object arrives on the stream.
- **New** `TestTheEventReadsAgreeOnAForeignID` (may live in `client_test.go`):
  one foreign id through `ListEvents`, `GetLatestEvent` and `WatchEvents`; all
  three return the same runs and none errors. This is the test the issue is
  about — it should fail before the change.
- **New**, beside `TestWatchEventsWaitsForAnObjectThatDoesNotExistYet`:
  `TestWatchEventsResolvesAnIDCreatedAsAnotherKind` — open on an id with no row,
  create it under another kind, assert the stream resolves and delivers that
  object's events instead of ending with `ErrNotFound`. This pins the table's
  third row, which is the only break the open-time tests cannot see.
- **Keep, unchanged and passing**:
  `TestWatchEventsWaitsForAnObjectThatDoesNotExistYet` and
  `TestWatchEventsEndsWhenTheObjectIsCollected`. They pin the `resolved` latch
  from both sides.
- **New** `TestWatchEventsEndsWhenAForeignObjectIsCollected`: the collected-object
  ending is about the object, not the kind — watch a foreign id, delete it,
  assert the stream ends with `ErrNotFound`.
- **Unchanged**: `TestControllerClientAddEventWrongKind` (`controller_test.go:344`)
  pins `AddEvent`'s `ErrWrongKind`. It is what keeps the read/write asymmetry
  deliberate rather than accidental; leave it alone.

## Docs to update when it lands

- `README.md`: the events section (around the `ListEvents`/`WatchEvents`
  description, `:672`–`:674`) — an **addition**, since the scoping was never
  documented: say the three reads are by id and the write is kind-scoped. Line
  625's note that the *object* watches are kind-scoped stays true and is worth
  keeping adjacent, since the two watches now differ.
- `docs/TODO.md`: the registered-controller requirement on `WatchEvents` — why
  it stays, and what would make dropping it worth doing. Lands with the change,
  not after it.
- `CLAUDE.md`: the event-watch bullet.
- `docs/adr/2026-08-05-events-get-a-cursor-and-a-commit-wake.md`: amend with the
  scoping decision and its rationale, or add a short ADR that supersedes this
  spec's "The decision" section.
- `docs/specs/README.md`: this is the one spec left on a branch, so the
  branch paragraph goes entirely and the "last spec shipped" line points at
  this change's ADR.
- Delete this spec.
