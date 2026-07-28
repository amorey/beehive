# The watermark's bound comes from the receiver's backlog head

- **Status:** Accepted — implemented in `sqlite/watch.go` (the bound) and `beehive.go`
  (the waker's use of it). Requires `github.com/amorey/gobus` v0.2.0 for
  `conflate.Receiver.Peek`.
- **Date:** 2026-07-28
- **Refines:** [The waker replays a resource_version watermark](2026-07-27-waker-watermark-replay.md),
  which establishes why the watermark exists at all. Read that first.

## Context

The waker's watermark is only sound if it never passes a change that has not been
processed. Deciding where that line is turns out to be the hard part, because the
store-wide stream **cannot answer it from what it delivers**.

The conflating hub coalesces a re-written object into the queue position it already
held, so delivery is in *first-touch* order, not version order. An object bumped to
version 5000 can therefore ride in the first batch while versions 65–100 belonging to
other objects are still queued behind it. The newest version in a batch says nothing
about what sits below it, and `writeSignalMerge` keeps only the max, so an entry's
first-touch version is unrecoverable from the delivered value.

The first implementation inferred the bound from the batch coming back **shorter than
the backend's cap**, which the drain loop produces when it emptied the receiver. That
was wrong in a way no test caught, because it fails on load rather than on error: a
workload touching more than a batch's worth of distinct objects with no lull makes
every batch full, so the cursor never advances at all, stays pinned at the subscribe
cursor for the life of the stream, and the first dropped subscription replays
essentially the whole table — the whole-world pass the watermark exists to replace,
reached without anything failing. It was also off by one: a drain that fills the batch
*exactly* as the receiver empties is indistinguishable from one that stopped because
the batch was full.

## Decision

**Ask the receiver instead of guessing.** `conflate.Receiver.Peek` reports the head of
the backlog without consuming it, and the ordering quantity rides in the value:

- `writeSignal` carries `firstRV`, the version of the write that *created* the slot,
  alongside `rv`, the newest merged into it.
- `changePublish` stamps `firstRV = rv` on every send. This is not optional and is the
  sharp edge of the design: conflate never calls `Merge` on a first touch, so the value
  stored verbatim at the publish site is the only thing that can establish the field.
- `writeSignalMerge` keeps the newest `rv` and the **earliest** `firstRV`. They pull in
  opposite directions — `rv` is the state to go read, `firstRV` is how far back the slot
  reaches — so both are asserted directly rather than through an end-to-end test.
- The backend peeks **right after the drain, on the drain's own goroutine**, and the
  answer travels with the batch it describes as `ObjectWriteBatch.OldestPending`.

The peek is exact, not a sample: a receiver has one consumer, and a concurrent `Send`
either coalesces in place or appends at the back, so neither can move the head. Reading
it later, or from another goroutine, would report a head further along and hand the
consumer a bound above what it was actually given.

The bound is **three-valued**, not two. A closed handle abandons whatever it was
holding, so `ErrClosed` cannot be folded into "nothing pending":

| `OldestPending` | meaning | the waker commits |
|---|---|---|
| `> 0` | oldest write still queued | `OldestPending - 1` |
| `0` | backlog empty | the highest version delivered (`seen`) |
| `< 0` | unreadable (closed handle) | nothing — hold the cursor |

### Why `Peek` rather than a rank API in the bus

The rejected alternative was for conflate to own the ordering quantity: a
`WithSequence(func(V) int64)` receiver option, per-slot rank state, and an
`OldestSequence` accessor. It lost on three counts. It teaches a deliberately
domain-agnostic bus what a rank is, when `Merge` is already the designated per-key
combining policy and can fold the quantity into `V` for free. It bakes `int64` into a
public signature where the consumer's own value type has no such constraint. And it
does not actually remove the ordering obligation below — it only moves it somewhere the
bus can document but still not enforce.

`Peek` is the smaller and more general primitive: one accessor, no option, no new
per-slot state, and the domain knowledge stays with the domain.

## The obligation this rests on

**Publication must be in version order.** The head of the queue is the *earliest-touched*
key; it is the *lowest-versioned* one only if first touches are published in
non-decreasing version order. Coalescing sends are unconstrained — they do not move
queue position — so the constraint is narrower than "ordered sends", but it is real, and
conflate is thread-safe and accepts out-of-order sends by design. Nothing upstream
enforces it.

Out of order it fails silently: with writers at versions 100 and 101, if 101 publishes
first the queue is `[K(first=101), K(first=100)]`, the head reports 101, the waker
commits 100, and `K`'s change at 100 is still queued and unprocessed.

Beehive satisfies it through four links, and all four are load-bearing:

1. `db.SetMaxOpenConns(1)` — commits serialize.
2. The version is drawn inside the write transaction, so version order is commit order.
3. `Within` holds `publishMu` across commit-and-publish, so publication order is commit
   order. (Post-commit *hooks* run after the unlock: a hook is user code that may write
   to the store, and running it under the lock deadlocks. Only publication needed
   ordering.)
4. **Every** store method that emits an object change self-wraps in `Within`. This is
   the fragile link — `ObjectsCreate`, `ObjectsDelete` and
   `DeletionRequestsCreateFromOwner` all had to be changed for it, and a new emitting
   method that publishes inline would break the bound with no failing test nearby.

Note that moving the cursor read under `publishMu` is **not** available as a
simplification: a writer holds the single connection and then takes `publishMu`, so a
subscriber holding `publishMu` while waiting for the connection deadlocks.

## The detector

Because link 4 cannot be enforced, the waker carries a tripwire. After committing
watermark `W`, every later delivery must have `rv > W` — anything still queued has a
first-touch version above `W`, and a replay reads only rows above it. So **a delivery at
or below `W` is proof the cursor passed something undelivered.** One comparison per
batch entry, reported at Error (a broken invariant, not an operating condition).

Three details that are each the difference between a working tripwire and a useless one:

- **The clamp takes `seen` with it.** `seen` is what the empty-backlog branch commits, so
  lowering only the watermark would let the very next batch jump straight back over the
  reserved range with no replay in between — the repair would change nothing.
- **Expected over-delivery is exempt**, via a per-stream grace floor. Two paths hand the
  cursor versions the stream is still about to deliver: subscribing registers the
  receiver *before* reading the cursor (so a write in that window is both buffered and at
  or below it), and a replay reads rows the receiver may already hold. Without the floor,
  a healthy control plane logs an Error per overlapping write and clamps itself into a
  redundant replay each time.
- **A zero version is not evidence.** It carries no cursor information at all, and
  contributes nothing to the cursor either.

It is a detector, not a repair. It cannot help in the window where the cursor
over-commits and the stream drops before the late write arrives.

## Consequences

`storeapi.WriteBatchCap` is gone — it was exported only so a consumer could infer
drained-ness from batch length. The stream now delivers `ObjectWriteBatch` rather than
`[]ObjectWrite`.

**The cursor legitimately lags by the backlog depth.** Under a deep sustained backlog it
sits at the version of the oldest queued write, which may be far behind the newest. That
is correct — those changes genuinely have not been processed — and it is bounded by the
backlog rather than by table size, but it will surprise anyone expecting the cursor to
track the head of the store.

**Order-independent bounds were considered and rejected**, and stay recorded here in case
the detector's residual ever matters. Both compute a real minimum over outstanding
writes rather than using queue position as a proxy for version order, so both survive
arbitrary send order:

- *Conflate maintains the minimum by rank* — a min-heap keyed by a caller-supplied rank,
  O(1) query and O(log n) maintenance. **This is the one to take** if the residual
  becomes unacceptable, because it makes the bound correct rather than merely checked.
- *Beehive shadows the outstanding set* — needs no upstream change at all, since beehive
  is both sender and consumer, but duplicates conflate's bookkeeping, puts a heap push on
  every write's publish path, and must be per-subscription.

Neither is needed while send order is *structurally* ordered rather than incidentally
so: skew requires removing `publishMu` or adding an emitting method outside `Within`,
both conspicuous edits, and the detector catches both.
