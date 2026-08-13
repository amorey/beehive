# A stream reports its failure beside itself, not inside it

- **Status:** Accepted — implemented in `objectswatch.go`, `client.go`,
  `types.go`, `internal/storeapi/storeapi.go`.
- **Date:** 2026-08-13

## Context

Beehive ended a stream two ways. The [event watch](2026-08-05-events-get-a-cursor-and-a-commit-wake.md)
returned an `*EventStream` whose `Err()` said why the channel closed. The object
watches returned a snapshot plus a bare channel and reported failure *in band*:
a final `ObjectChange{Type: Failed, Err: …}`, then the close.

The in-band report cost three things. `ObjectChange` was two types in one
struct — on a `Failed` change `ID`, `ResourceVersion` and `Object` were all
meaningless, while `Err` was set nowhere else — so no field could be trusted
before `Type` had been read. `ChangeType` had a value that was not a change.
And forgetting the case was silent: the channel closed immediately after, so a
consumer that never matched `Failed` saw a stream that looked like it ended
normally, with `ErrWatchTooOld`, `ErrWatchTooNew` or `ErrStopped` dropped.

## Decision

**`Watch`, `WatchList` and `WatchOwnedObjects` return a stream value —
`*ObjectStream` or `*ObjectListStream` — carrying the snapshot, `Changes`, and
an `Err()`. `Failed` is deleted.**

`ChangeType` is `Added`/`Modified`/`Deleted`; every field of `ObjectChange` is
meaningful on every value; the snapshot types fold into the streams that carry
them. One shape ends a stream across the library.

This does not claim to make an unread failure loud: an `Err()` nobody calls is
as quiet as a `Failed` nobody matches. What it buys is that the failure is not a
value on the change path, and that events and objects are handled alike.

### The failure slot is shared

`Watch` has no tail of its own — it joins the kind's `tailStream` and adapts the
result down to one object. The snapshot fields are final before `tailStream`
returns, so copying them is sound. **A failure is not**: it is written later,
from the stream goroutine. A copied slot would leave `Watch(...).Err()` nil
forever — every terminal failure lost on the shape most callers reach for first,
which is worse than the `Failed` change it replaced.

So `streamFail` is allocated once per subscribe and **embedded by pointer** in
every stream value built from that call. Embedding it by value re-creates the
bug and still compiles, which is why the `Watch` row of
`TestEveryWatchShapeReportsItsFailureAlike` exists: it fails with a copied slot.

`objectTailer` carries the same slot **by value** — it is never copied, and its
subscribers reach it through the one pointer they hold — which is what makes
`tailer.Err()` and a stream's `Err()` one mechanism rather than two lookalikes.

The alternative — passing the destination stream into `tailStream` — keeps both
structs flat like `EventStream`, but moves the invariant into three call sites
instead of one, and the flatness is invisible (the embedded field is unexported,
so godoc shows a promoted `Err` either way).

## Consequences

- **The failure is stored before `close(Changes)`**, so seeing the close is
  enough to read it. The goroutine's `defer close(out)` is declared first and so
  runs last; `endStream` runs on the body before it returns.
- **Teardown no longer waits on a reader.** The terminal report used to go out
  over the unbuffered channel, so a caller that stopped reading held the sender
  until its own context ended. Storing cannot block.
- **The change path is unchanged, deliberately.** `consume` still sends ordinary
  changes with `sendOrDone` on the caller's context, so an undelivered change
  still parks the sender, and a cancel from there reports a nil `Err()` — the
  caller cancelled. A stream that abandoned undelivered changes to report an
  error about dropping changes would be the wrong trade.
- **A nil `Err()` after the close still means the caller's context ended.** The
  `ctx.Err()` check at the top of `endStream` is load-bearing: it is what a
  supervisor keys on to decide not to resubscribe.
- **A failed initial read still returns a nil stream and an error**, not a
  stream whose `Err()` is set: the snapshot's guarantee is void, so there is
  nothing to subscribe to.
- **`EventStream` keeps its own inline slot.** Converting it to `streamFail` is
  a tidy-up with no caller-visible effect; there is no bug behind it, and its
  `Err` godoc names a different sentinel set (`ErrNotFound` for a collected
  object), which a promoted method would have to carry somewhere else.
- **The stream goroutine captures the slot, not the stream.** Capturing the
  stream would pin its snapshot — every decoded object, with eager relations —
  for the stream's whole life, which the old shape did not do.
- **A dead tailer is now released promptly** on failure, since no subscriber
  parks delivering a terminal change. Presence in the registry still is not
  health — the window between a reset and the last release is real, and
  `tailerFor` still checks both — so that invariant is pinned directly, against
  the registry, rather than by racing a live stream against it.

Breaking for every caller of the three watches; this is pre-1.0 and the
alternative was carrying two shapes and keeping `Failed` alive.
