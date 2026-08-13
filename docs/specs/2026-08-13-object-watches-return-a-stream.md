# Object watches return a stream, and `Failed` goes away

## Problem

Beehive ends a watch two different ways.

`WatchEvents` returns an `*EventStream`: the snapshot, the position it was read
at, the channel, and an `Err()` that says why the channel closed. The failure
lives beside the stream, not in it.

`Watch`, `WatchList` and `WatchOwnedObjects` return a snapshot plus a bare
`<-chan ObjectChange`, and report a failure *in band*: one final change with
`Type: Failed` and the reason in `Err`, then the channel closes.

That in-band report costs three things:

- **`ObjectChange` is two types wearing one struct.** On a `Failed` change `ID`
  is zero, `ResourceVersion` is zero and `Object` is nil — the godoc says so
  field by field — while `Err` is non-nil *only* there. Nothing on the value can
  be trusted before `Type` has been read.
- **The names are untrue.** `ChangeType` has a value that is not a change, and
  `ObjectChange` describes something that is not about an object.
- **Forgetting it is silent.** The channel closes immediately after the `Failed`
  change, so a consumer that never checks `Type` sees a stream that looks like
  it ended normally. `ErrWatchTooOld`, `ErrWatchTooNew` and `ErrStopped` are all
  dropped and nothing complains.

The snapshot-plus-channel design is not the problem and does not change: `Watch`
reading the snapshot synchronously, on the caller's goroutine, before the stream
exists, is what lets a caller subscribe and then act without a race. This spec
is only about how a stream reports that it stopped.

## Decision

Give the three object watches the shape `WatchEvents` already has: each returns
a **pointer to a stream value** carrying the snapshot, the change channel, and
an `Err()` reporting why the channel closed. Then delete `Failed`.

After this change:

- `ChangeType` is `Added`/`Modified`/`Deleted`, and every one of them is a
  change.
- Every field of `ObjectChange` is meaningful on every value, and `Err` is gone
  from it.
- The library has one way to end a stream, and one place a caller reads why.

Note what this does *not* claim about the third problem: an `Err()` a caller
never calls is as quiet as a `Failed` change a caller never matches. What
changes is that the failure is no longer a value on the change path, so no
consumer has to type-check a change before trusting its fields, and the case is
handled the same way for events and for objects. Making an unread failure loud
is a separate change; see "Out of scope".

**One flag day, no deprecated pair.** The issue offers an additive path with the
old methods deprecated; we are not taking it. This is a pre-1.0 library that has
already renamed its whole client surface in one breaking commit
(`refactor!: name the client surfaces verb-first`), and carrying two shapes for
the same watch would leave `Failed` alive — which is most of the point of the
change. Ship it as one `feat!`/`refactor!` commit that also updates every
example.

### The stream types replace the snapshot types

`ObjectSnapshot` and `ObjectListSnapshot` are **folded into** the new stream
types and deleted. Their two fields become fields on the stream, flat, exactly
as `EventStream` carries `Runs` and `ResourceVersion`.

The alternative considered was embedding — `struct { ObjectSnapshot[…]; Changes
…; }` — which keeps the old types and the old field access. Rejected: it leaves
two names for one thing, it does not match `EventStream`, and the snapshot types
would have no remaining users of their own. One shape, one name.

## Surface

In `objectswatch.go`, replacing `ObjectSnapshot`, `ObjectListSnapshot` (which
live in `client.go` today) and `ObjectChange`'s `Err` field. Beside their
machinery, mirroring `EventStream` in `eventswatch.go`; `ObjectChange` stays in
`client.go` with the interface that returns it.

```go
// streamFail is the slot a stream's terminal failure is stored in. A pointer,
// shared: Watch hands back a different value than the goroutine writes through,
// so the slot cannot live in either struct by value.
type streamFail struct {
    failed atomic.Pointer[error]
}

// Err reports why the stream ended, after Changes is closed: ErrWatchTooOld for
// a stream that fell below retention, ErrWatchTooNew for a resume position this
// store never issued, ErrStopped for a Beehive that stopped, or nil when the
// caller's own context ended. Before the close it reports nil, which says
// nothing.
func (f *streamFail) Err() error {
    if f == nil { // a zero-value stream a test built itself
        return nil
    }
    if err := f.failed.Load(); err != nil {
        return *err
    }
    return nil
}

func (f *streamFail) fail(err error) { f.failed.Store(&err) }

// ObjectStream is a live view of one object: its state as of the subscribe, the
// position that state was read at, and the changes above it.
type ObjectStream[Spec, Status any] struct {
    // Object is the current state, or nil when the id holds nothing yet. Nil on
    // a resume.
    Object *Object[Spec, Status]
    // ResourceVersion is the position Object was read at, and the value to hand
    // back to WithResumeFrom.
    ResourceVersion int64
    // Changes delivers the changes above ResourceVersion, ascending by
    // resource version, until ctx ends or the stream fails. Closed exactly once.
    Changes <-chan ObjectChange[Spec, Status]

    *streamFail
}

// ObjectListStream is ObjectStream over many objects: a kind, or one owner's
// children of a kind.
type ObjectListStream[Spec, Status any] struct {
    // Objects is the snapshot. Empty on a resume.
    Objects []*Object[Spec, Status]
    // ResourceVersion is the position Objects is complete as of, and the value
    // to hand back to WithResumeFrom.
    ResourceVersion int64
    // Changes delivers the changes above ResourceVersion, ascending by
    // resource version, until ctx ends or the stream fails. Closed exactly once.
    Changes <-chan ObjectChange[Spec, Status]

    *streamFail
}
```

### The failure slot is shared, and that is the load-bearing part

`Watch` does not get its own stream from the tailer: it joins the kind's
`tailStream` like the other two and adapts what comes back. Today that
adaptation is a **copy** (`objectswatch.go:125-133`) — a fresh `ObjectSnapshot`
built from `list.ResourceVersion` and `list.Objects[0]` — which is sound only
because both fields are final before `tailStream` returns.

A failure is not final: it is written later, from the stream goroutine. Copy the
struct and the goroutine writes into the value the caller never sees, so
`Watch(...).Err()` is nil forever and every terminal failure vanishes on the
single-object watch — a worse version of the bug this spec exists to fix, since
`Failed` at least arrived on the channel.

Hence one `*streamFail`, allocated once in `tailStream` and pointed at by every
stream value derived from that call. Three consequences the implementer must
keep:

- **Embed the pointer, never the struct.** `streamFail` by value re-creates the
  bug the moment `Watch` copies, and it compiles.
- **`fail` is called on the slot, not on a stream.** `endStream` takes the
  `*streamFail`; it never needs to know which shape the caller holds.
- **`Err()` and `fail()` have one implementation and one godoc**, promoted onto
  both types. A fourth entry point cannot forget to wire a slot it does not
  allocate.

The alternative — `tailStream` taking the destination stream as a parameter, so
each entry point owns the value written into — keeps the two structs flat and
identical to `EventStream`. Rejected: it moves the invariant into three call
sites instead of one, and the flatness it buys is invisible (the embedded field
is unexported, so godoc shows promoted `Err` either way).

`EventStream` keeps its own inline `failed` field. Converting it to `streamFail`
is a tidy-up with no caller-visible effect and no bug behind it — leave it, or
do it in its own commit.

`ObjectChange` keeps `Type`, `ID`, `ResourceVersion` and `Object`, loses `Err`,
and loses every "Zero on a `Failed` change" caveat. The one caveat that stays is
the real one: on a `Deleted` change `Object` carries the row's final state, or
nil when that state could not be decoded.

The three methods on `Client`:

```go
Watch(ctx context.Context, id ObjectID, opts ...WatchOption) (*ObjectStream[Spec, Status], error)
WatchList(ctx context.Context, opts ...WatchOption) (*ObjectListStream[Spec, Status], error)
WatchOwnedObjects(ctx context.Context, ownerID ObjectID, opts ...WatchOption) (*ObjectListStream[Spec, Status], error)
```

Pointers, like `*EventStream`: a stream is an identity with a live channel and a
failure yet to be written, not a value to copy around.

In `types.go` and `internal/storeapi/storeapi.go`: delete the `Failed` constant
and its alias. `ChangeType` itself stays where it is, alias included — nothing
about its home changes.

The error sentinels do not change, and neither does which one is reported when.

## Implementation

All of it is in `objectswatch.go` — the new types included — plus the `Client`
interface in `client.go`, `ObjectChange` losing its `Err` field there, and the
constant in `types.go`/`storeapi.go`. No store change, no schema change, no
driver change, no change to the tailer, the fan-out, the merge, the replay
paging, the horizon checks or the retry ladders.

1. **`tailStream`** (`objectswatch.go` ~line 667) allocates one `&streamFail{}`
   and builds the stream value around it: same reads in the same order, same
   `abandon()` on a failed initial read, same `(value, error)` return for a
   snapshot that could not be read. It already returns before the goroutine
   starts, so the caller still gets the snapshot synchronously. Its return type
   becomes `*ObjectListStream[Spec, Status]`; `Watch` adapts that to
   `*ObjectStream` as it adapts the list snapshot today — taking at most one
   object, by the key filter — and **carries the same `*streamFail` pointer
   across**, which is the whole of the fix above.
2. **`endStream`** stops sending and starts storing, and takes the
   `*streamFail` rather than the channel. It keeps its single-site rule and its
   precedence: caller's context ended → store nothing, `Err()` is nil;
   otherwise store the subscriber's own failure if it has one, else
   `tailer.failure()`. The `sendOrDone` call becomes `slot.fail(err)`.
3. **Ordering is the one invariant to get right.** The failure must be stored
   **before** `close(Changes)`, or a consumer that sees the close and calls
   `Err()` reads nil. The goroutine's existing `defer close(out)` is declared
   first and so runs last, and `endStream` is called on the goroutine's body
   before it returns — the ordering holds, but it deserves the one-line comment
   the repo reserves for exactly this kind of constraint.
4. **The slot is `atomic.Pointer[error]`**, copied from `EventStream`. The write
   is on the stream goroutine, the read on the caller's; nothing else touches
   it. Each subscriber gets its own slot, so a tailer-wide failure is stored
   once per subscriber — and, on the `Watch` path, once into the slot the
   returned value shares rather than into a copy of it.
5. **`replay` and `consume` lose their knowledge of `Failed`.** They already
   hand the failure to `endStream` rather than sending it; only the comments that
   say "sends the terminal `Failed` change" need rewording.

Logging is untouched: the tailer keeps its `ErrWatchTooOld` warning, and
`endStream` gains none. See "Out of scope".

**The terminal store cannot block, so teardown no longer waits on a reader.**
Today the `Failed` change goes out over the unbuffered channel, so a caller that
stopped reading holds the sender until its own context ends. A store cannot
block.

Do not overstate this: the *change* path is unaffected. `consume` sends ordinary
changes with `sendOrDone(ctx, out, ch)` on the caller's context
(`objectswatch.go:809`), and a false return there leaves `consume` without
calling `endStream` at all. So with an unread `Changes` and a change already in
flight, a stop still parks the goroutine until the caller cancels — and then
`Err()` is nil, not `ErrStopped`, because a caller cancel is exactly what nil
reports. That is today's behaviour, this spec does not change it, and it should
not: a stream that abandons undelivered changes to report a failure has dropped
data to deliver an error about dropping data.

The replay path differs and an implementer reading both send sites will notice.
`tailStream` passes `work` — the caller's context merged with the tailer's —
into `replay`, so an abandoned replay send returns `ok=false` with a nil
failure, and `endStream` still resolves `ErrStopped` from `tailer.failure()`.
Leave the asymmetry; it is the difference between a bounded catch-up and an
open-ended live stream.

## Edge cases the implementer should not have to guess at

- **A stream built by a test has a nil slot**, and `Err()` must answer nil
  rather than panic — hence the nil check on the receiver. Tests construct
  `EventStream{}` directly today and will do the same here.
- **`Err()` is meaningful only after `Changes` is closed.** Calling it earlier is
  safe (atomic) but answers about an unfinished stream. Same wording as
  `EventStream.Err`.
- **Nil `Err()` after a close still means "your context ended".** That is the
  signal a supervisor keys on to decide not to resubscribe, and it must survive
  the change: the check `if ctx.Err() != nil { return }` at the top of
  `endStream` is load-bearing, not a fast path.
- **Caller cancel and tailer stop racing.** The caller's context wins, as today,
  giving a nil `Err()`. Do not try to report both.
- **`ErrStopped` still requires the merged `work` context.** The goroutine's
  retries run under `ctx` merged with `tailer.ctx` precisely so a store that
  keeps failing past stop cannot hold the stream open and swallow `ErrStopped`.
  Unchanged, and the comment stays.
- **A resume's stream carries no snapshot** — nil `Object`, empty `Objects` —
  and `ResourceVersion` is the caller's own position handed back. Same as today,
  and the godoc on each field must say it.
- **`Watch` on an id that holds nothing** is a nil `Object` on a live stream, not
  `ErrNotFound`. Unchanged.
- **A failed initial read returns a nil stream and an error**, not a stream whose
  `Err()` is set. The distinction is deliberate: the snapshot's guarantee is
  void, so there is nothing to subscribe to. Every early return still owes
  `abandon()` — the receiver leaves the fan-out and the tailer lease is
  released.
- **An undecodable row is still quarantined and logged, never reported.** It is
  not a stream failure, and `decodeChanges` still cannot fail.
- **`ChangeType` keeps its `storeapi` alias.** Only the `Failed` constant is
  deleted, from both files. Confirm nothing in `sqlite/` or elsewhere in
  `internal/` refers to it (nothing does today) — otherwise `staticcheck
  -checks=all` will find it.

## Out of scope

- **The snapshot-plus-channel design.** Not up for revision: the synchronous
  snapshot is the anti-race guarantee callers rely on.
- **`WatchSchedule`.** It returns `<-chan Schedule` — a gauge over process-local
  state with no failure mode to report; it ends when the context ends or the
  beehive stops, and a reader cannot tell those apart by design. Giving it a
  stream type would add an `Err()` that is always nil.
  See [the schedule-watch ADR](../adr/2026-07-27-schedule-watch.md).
- **A `Retention` field on the object streams.** `EventStream.Retention` shipped
  in `5e9679f` and the same argument may carry over to
  `WithWriteLogRetention` — but that is a feature, and folding it into a
  reshaping would mix the two. Note the ordering it implies: this spec's
  `ObjectStream`/`ObjectListStream` are the place such a field would land, so it
  is cheaper after than during.
  → [ADR](../adr/2026-08-06-event-retention-is-a-ring-per-timeline.md)
- **Any change to what the tailer, the merge or the replay do.** This is a
  reshaping of the report, not of the machinery.
- **Logging a terminal failure in `endStream`.** Tempting, because the
  per-subscriber failures (`replay`'s horizon and too-new checks) are logged
  nowhere today and a stream shape does not by itself make an unchecked `Err()`
  loud. It is still a separate change: it adds an output this spec does not
  touch, it applies just as well to `EventStream`, and deciding it here would
  settle it for one watch family only. Raise it on its own once the shapes
  agree.

## Tests

Mostly a mechanical rewrite of `objectswatch_test.go`, since every watch test
touches the return shape. The assertions that must exist afterwards:

1. **A retention trim ends every subscriber on the kind with
   `Err() == ErrWatchTooOld`**, and `Changes` is closed. The current test that
   asserts a `Failed` change becomes this.
2. **`WithResumeFrom` below the horizon** ends the stream with `ErrWatchTooOld`
   from the replay path, and **above the log's head** with `ErrWatchTooNew`.
3. **Stopping the beehive** ends a live stream with `ErrStopped`, after the
   changes already delivered.
4. **Cancelling the caller's context** closes `Changes` and leaves `Err()` nil.
5. **`Err()` is observable the instant `Changes` is closed** — read it directly
   after the receive on the closed channel, with no sleep, which is what pins
   the store-before-close ordering.
6. **A stop with nobody reading `Changes` still sets `Err()`** — the teardown no
   longer waits on a reader. This one is only true with the goroutine parked in
   `rx.RecvContext` and **no change in flight**; write it that way (a quiet
   stream, stopped) or it is flaky, passing or failing on whether a change
   happened to be pending. The blocked-on-a-pending-change case is unchanged
   behaviour and is not what this asserts.
7. **`Watch`'s `Err()` reports the failure, not nil.** The single-object path
   returns a value the goroutine never writes to directly, so this is the
   assertion that pins the shared slot; without it the regression is silent.
8. **All three entry points** — `Watch`, `WatchList`, `WatchOwnedObjects` —
   report a terminal failure identically, table-driven over the constructor.
9. **`Watch`'s stream carries at most one object**, and a resume's stream carries
   none, on both cardinalities.
10. **A failed initial read returns a nil stream and a non-nil error**, and
    releases the tailer — assert the kind has no tailer afterwards, as the
    existing lease tests do.

Beyond `objectswatch_test.go`: **`beehive_test.go:674`** asserts on `Failed` and
`ev.Err` (a second stop must not end the stream) and has to be rewritten to
assert a nil `Err()` instead. `objectswatch_bench_test.go` and
`testutils_test.go` compile against the new shape; the `fakeStore` needs nothing
new.

## Docs and examples

- **Four examples carry a `beehive.Failed` branch**: `greeting` (line 123),
  `lowpower` (144), `conditions` (198) and `cascade` (212 **and** 227 — two
  watches, both). Each drops the branch and checks `stream.Err()` after the
  range instead. `examples/events` has none and needs nothing. These are the reference the issue says consumers copy, so the
  new idiom has to be visible in all of them:

  ```go
  stream, err := client.WatchList(ctx)
  // ...
  for ch := range stream.Changes {
      // ...
  }
  if err := stream.Err(); err != nil { /* ErrWatchTooOld → resubscribe */ }
  ```

- **`README.md`**, all five sites confirmed against the current text: the
  `Failed` constant (356), the `ObjectChange`/`ObjectSnapshot`/`ObjectListSnapshot` block (359–372),
  the `Client` interface block (391–404), the terminal-failure paragraph (604 —
  the whole thing becomes the `Err()` contract), and `WatchOwnedObjects` (632).
  Keep the sentence that a nil `Err()` after a close means the caller's own
  context ended: that is the part supervisors key on.
- **`CLAUDE.md`**: the watch bullets mention neither `Failed` nor the return
  shape today, so check rather than assume; if a sentence names the shape, make
  it say "a stream value with `Err()`" for both watches.
- **On ship**: fold the rationale into a new ADR —
  *object watches report their failure beside the stream* — recording why the
  failure left the change type (one struct cannot be both), why one shape for
  both watch families, and why this went in as a flag day rather than a
  deprecation. Cross-reference it from
  [`docs/adr/2026-08-03-watch-shared-tail.md`](../adr/2026-08-03-watch-shared-tail.md)
  and [`docs/adr/2026-08-05-events-get-a-cursor-and-a-commit-wake.md`](../adr/2026-08-05-events-get-a-cursor-and-a-commit-wake.md),
  then delete this spec.
