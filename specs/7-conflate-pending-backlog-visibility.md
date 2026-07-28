# Add Peek to a conflating receiver

**Repo: `github.com/amorey/gobus`, package `conflate`.** Not beehive — this is an
upstream library change, and a small one. Beehive is the consumer, and the change it
unblocks is specified in
[8-waker-watermark-from-pending-backlog.md](8-waker-watermark-from-pending-backlog.md).

**Status: proposed.** Do this first; spec 8 cannot start until it lands (or until a local
fork exists to prototype against). Beehive currently depends on `gobus v0.1.0`.

---

## Background

`conflate` is a keyed latest-value fan-out bus. Each `Receiver` holds one value slot per
key plus an insertion-ordered queue of keys. A `Send` for a key with no pending slot
appends it at the back; a `Send` for a key already pending coalesces into the existing slot
via `Merge` and leaves its queue position unchanged. Delivery is therefore *first-touch*
order, and memory is bounded by the live key set rather than by write volume.

`Recv`, `RecvContext` and `TryRecv` all **remove** the value they return. There is no way
to look at the head of the queue without consuming it, and the queue state
(`order`, `elems`, `pending` on `Receiver`) is unexported with no accessor.

## The problem

**A consumer cannot ask what is still queued.** For a consumer that reads the values it
receives as a *cursor* — recording how far it has got so it can resume from there after a
dropped subscription rather than re-deriving the world — that is the one thing it needs to
know, and it cannot be inferred from the values delivered.

The reason is first-touch order. Because a re-written key coalesces into the position it
already held, the newest value in a batch says nothing about what sits behind it: a key
bumped to sequence 5000 can be delivered while *other* keys carrying 65–100 are still
queued. A consumer that resumes from 5000 skips those permanently.

Beehive's present workaround infers "the receiver was drained" from the delivered batch
being shorter than its own batch cap. That starves: under a workload touching more than a
batch's worth of distinct keys with no lull, every batch is full, the cursor never
advances, and the first dropped subscription costs a full table pass. It is also off by
one — a drain that fills the batch *exactly* as the receiver empties is indistinguishable
from one that stopped because the batch was full.

## What to add

One method:

```go
// Peek returns the oldest pending event without removing it, and false if nothing is
// pending.
func (rx *Receiver[K, V]) Peek() (gobus.Event[K, V], bool)
```

Read `order.Front()`, look up `pending[k]`, return, under `s.mu`. No new state, no new
option, no new obligation on the caller.

**It replaces an `Empty()` accessor rather than needing one beside it:** `ok == false` is
exactly "nothing pending right now". Note this is *not* the same question as the existing
unexported `drainedLocked`, which folds in `txClosed` — that means "this stream is over",
not "the queue is empty".

### The property worth documenting

For a receiver with a single consumer goroutine — which the package already states is the
intended use — **a `Peek` immediately after a drain reports exactly what remains.** The
only other mutation is a `Send`, and a `Send` either coalesces into an existing slot or
appends at the back; neither can change the front. So the consumer needs no lock held
across drain-then-peek to trust the answer.

That property is the whole reason a plain `Peek` is sufficient here, so it belongs in the
doc comment. State the converse too: with more than one consumer on a receiver, or read
concurrently with another consumer's pop, the answer is a stale observation like any other.

### Why not a sequence/rank API

An earlier draft of this spec proposed `Hub.WithSequence(seq func(V) int64)` plus an
`OldestPending() (int64, bool)`, with the receiver recording a rank at first touch and
never updating it on coalesce. It was rejected: it teaches the bus what a rank is, which
walks back the design principle that coalescing policy lives in the caller's `Merge` so
the bus stays domain-agnostic. It also needed new per-slot state, a new receiver option,
and a documented ordering obligation on the sender.

`Peek` needs none of that. A caller wanting the oldest sequence tracks it **in `V` through
its own `Merge`** — preserving the earlier value's first-touch field while coalescing
whatever else it likes — and reads it off the peeked value. All the domain knowledge stays
with the domain.

## Acceptance criteria

- `Peek` returns the front without consuming it: peek twice, then `TryRecv`, and all three
  report the same event.
- `Peek` reflects coalescing: after a `Send` merges into the front's slot, `Peek` reports
  the merged value.
- `Peek` does not move on coalesce into a *non*-front key — queue position is unchanged by
  `Merge`, so the front must not change either.
- After annihilation (`Merge` returning `keep == false`) of the front's key, `Peek` reports
  the next key.
- After popping the front, `Peek` reports the new front.
- With nothing pending, `Peek` reports `false` — including on a closed sender with an empty
  queue, and including before anything has ever been sent.
- With a **closed sender and a non-empty queue**, `Peek` still reports the front: it is not
  `drained`, and the soft-drain contract says those values are still coming.
- A key the receiver's `WithKeyFilter` rejects is never enqueued, so it never appears.
- No allocation, and no iteration over `pending`.

## Out of scope

- Any change to `Merge`, delivery order, or the memory bound.
- Exposing the pending count or the queue contents. A count invites polling and answers
  nothing the cursor case needs.
- `PeekContext` or a blocking peek. The consumer already has `RecvContext` to wait on.
