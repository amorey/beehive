# Expose a conflating receiver's pending backlog

**Repo: `github.com/amorey/gobus`, package `conflate`.** Not beehive — this is an
upstream library change. Beehive is the consumer, and the change it unblocks is
specified in [8-waker-watermark-from-pending-backlog.md](8-waker-watermark-from-pending-backlog.md).

**Status: proposed.** Do this one first; spec 8 cannot start until it lands (or until a
local fork exists to prototype against). Beehive currently depends on `gobus v0.1.0`.

---

## Background: what conflate is

`conflate` is a keyed latest-value fan-out bus. A `Hub` hands out one `Sender` and any
number of `Receiver`s. Each receiver holds **one value slot per key** plus an
insertion-ordered queue of keys. A `Send` for a key with no pending slot appends the key
at the back; a `Send` for a key that is already pending **coalesces into the existing slot
via `Merge` and leaves the key's queue position unchanged**. So delivery is *first-touch*
order, and a receiver's memory is bounded by the live key set rather than by write volume.

The relevant internals are all unexported (`conflate.go`, `Receiver` struct):

- `order *list.List` — keys in first-touch order
- `elems map[K]*list.Element` — key → its queue element
- `pending map[K]V` — key → latest undelivered value

`enqueueLocked` is where a new slot is appended (the `else` branch) versus coalesced (the
`if` branch). `popLocked` removes from the front. `drainedLocked` reports the terminal
condition, but folds in `txClosed` — it means "this stream is over", not "the queue is
empty right now".

## The problem

**A consumer cannot see anything about the backlog.** There is no accessor for `pending`,
`order`, or their size, and no way to add one from outside: `ReceiverOption`'s parameter
type is unexported specifically so the option set stays closed. A consumer that needs to
reason about what is still queued has no seam at all short of forking.

### Why a consumer needs it

Beehive's dependency waker reads the versions on delivered values as a **resume cursor**:
it records how far it has got, and after a dropped subscription it replays everything
above that point instead of re-deriving the world. For that, it needs to know a version
below which nothing is still queued.

Delivery order makes this impossible to infer from the delivered values alone. Because a
re-written key coalesces into the queue position it already held, the newest version in a
batch says nothing about what sits behind it: an object bumped to version 5000 can ride
in the first batch while versions 65–100 belonging to *other* keys are still queued. Take
5000 as a resume point and those are skipped permanently.

Beehive's present workaround is to infer "the receiver was drained" from the delivered
batch being shorter than the backend's batch cap. That has two defects, and both are
really this gap:

- **It starves.** Under a workload touching more than the batch cap's worth of distinct
  keys with no lull, every batch is full, the cursor never advances, and the first dropped
  subscription replays essentially the whole table — the exact cost the cursor exists to
  avoid. Nothing unsticks it.
- **It is off by one.** A drain that fills the batch *exactly* as the receiver empties is
  indistinguishable from one that stopped because the batch was full.

## What to add

Two accessors. They are one coherent change: the second is a degenerate case of the first
and should not ship on its own.

### 1. The oldest pending sequence

A receiver option supplying a monotonic rank for values, and an accessor for the rank of
the oldest undelivered one:

```go
// Hub.WithSequence supplies a monotonic rank for values on this receiver. The rank is
// recorded when a key is first touched and is NOT updated when Merge coalesces into an
// existing slot.
func (h *Hub[K, V]) WithSequence(seq func(V) int64) ReceiverOption[K, V]

// OldestPending returns the sequence of the oldest undelivered value, and false if
// nothing is pending. Requires WithSequence.
func (rx *Receiver[K, V]) OldestPending() (int64, bool)
```

**This can be O(1), which is the point.** Do not scan `pending` and do not add a heap. If
the rank is recorded at first touch and never updated on coalesce, then insertion order
*is* increasing rank order — so the front of `order` already holds the minimum, and
`OldestPending` is a list-head read under the existing lock.

That holds **only if the sender publishes in increasing rank order**, which is the
caller's obligation and must be stated plainly in the doc comment: a caller who violates
it gets a silently wrong answer rather than an error. (Beehive satisfies it — its store
publishes under a mutex held across commit, so publication order is commit order and the
version counter is monotonic in commit order.)

Sketch of where the state goes:

- `enqueueLocked`, **new-slot branch only**: record `seq(v)` beside the slot.
- `enqueueLocked`, **coalesce branch**: leave the recorded rank alone. This is the
  load-bearing rule — updating it here would make the rank a *latest*-touch rank and
  destroy the ordering that makes the front the minimum.
- `enqueueLocked`, **annihilation branch**: drop the recorded rank with the slot.
- `popLocked`: drop the recorded rank with the slot.
- `OldestPending`: read `order.Front()`'s recorded rank under `s.mu`.

Calling it without `WithSequence` should fail loudly rather than return a plausible zero —
pick whichever of panic or a documented `(0, false)` fits the package's conventions, but
be explicit about the choice.

### 2. The instantaneous empty check

```go
// Empty reports whether this receiver has nothing pending right now. Unlike the
// terminal "drained" condition it says nothing about whether the sender is closed.
func (rx *Receiver[K, V]) Empty() bool
```

`order.Len() == 0` under the lock. Distinct from `drainedLocked`, which folds in
`txClosed`; a consumer asking "is there more to come right now" is not asking "is this
stream over".

Needs no `WithSequence`, and is what a consumer uses when it has no meaningful rank.

## Alternative, if the ordering obligation is unacceptable

If requiring monotonic-rank-in-send-order is too sharp an edge for a general-purpose bus,
the fallback is an unconditional scan:

```go
// Pending calls yield for each undelivered key/value until yield returns false.
func (rx *Receiver[K, V]) Pending(yield func(K, V) bool)
```

Correct with no obligation on the caller, and the caller computes whatever fold it wants.
The cost is real and worth stating in the doc: it walks the whole live key set **under the
bus lock**, which serializes against every `Send` on the hub — worst on exactly the
high-cardinality workload the accessor exists to serve. A consumer would have to call it
on a cadence rather than per batch, which converts "the cursor never advances" into "the
cursor advances on a cadence" rather than fixing it outright.

Prefer option 1. Record this one as considered, with the reason.

## Acceptance criteria

- `OldestPending` returns the rank of the **first-touched** pending key, not the
  lowest-ranked value — assert these differ by coalescing a re-written key to a high rank
  and checking the front still reports its original rank.
- Coalescing does not move a key's reported rank. This is the property the O(1) claim
  rests on; without a test it will be "fixed" into a latest-touch rank later.
- Annihilation (a `Merge` returning `keep == false`) removes the rank along with the slot,
  and `OldestPending` then reports the next key's.
- After popping the front, `OldestPending` reports the new front's rank.
- With nothing pending, `OldestPending` reports `false` and `Empty` reports true.
- `Empty` is false with a queued value even when the sender is closed — it is not
  `drained`.
- Key filters compose: a value the receiver's `WithKeyFilter` rejects is never enqueued,
  so it must not affect either accessor.
- No new allocation on the `Send` path, and `OldestPending` does no iteration. Worth a
  benchmark, since both sit on a hot path shared with every writer.

## Out of scope

- Any change to `Merge`, delivery order, or the memory bound.
- Exposing the pending *count* or the queue contents beyond the two accessors above. A
  count invites polling loops and answers nothing the cursor case needs.
- Making the rank generic over an ordered type parameter. `int64` covers the cursor use
  and avoids a second type parameter on `Hub`; revisit only if a real second consumer
  wants otherwise.
