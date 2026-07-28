# Take the waker's watermark from the pending backlog, not from batch length

**Repo: beehive.** Depends on
[7-conflate-pending-backlog-visibility.md](7-conflate-pending-backlog-visibility.md)
landing in `github.com/amorey/gobus` first (or on a local fork to prototype against).

**Status: proposed.** Closes the one known correctness-adjacent gap left in the watermark
recovery design recorded in
[docs/adr/2026-07-27-waker-watermark-replay.md](../docs/adr/2026-07-27-waker-watermark-replay.md).
Read that ADR before starting; this spec assumes it.

---

## Background

The **dependency waker** (`waker` in `beehive.go`) rides one store-wide stream of object
writes. When an object changes it looks up everything that declared a `depends_on` edge
pointing at that object and requeues those dependents.

The stream can be lost three ways: a failed dependents lookup, a closed stream, a failed
subscribe. The repair is a **watermark**: the waker records the highest
`resource_version` it has finished processing, and on a loss it replays everything above
that point (`ObjectWritesListSince`, paged) through the same wake path. Cost is O(changes
missed) rather than O(table). That last property is the entire justification for the
design — a dependent that has settled is invisible to every owed-work listing, so the
alternative was a full pass over every object of every kind.

## The problem

**The watermark can stop advancing indefinitely, and then the replay is a full pass.**

The store-wide stream delivers in *first-touch* order (the conflating hub coalesces a
re-written object into the queue position it already held), so the newest version in a
batch says nothing about what is still queued behind it. The waker therefore cannot commit
a batch's high-water mark unless it knows nothing lower is still pending — and its only
present signal for that is the batch coming back **shorter than `writeBatchCap`** (64),
which the backend's drain loop produces when it emptied the receiver
(`sqlite/watch.go:426`).

Under a workload touching more than 64 distinct objects with no lull, every batch is full.
`commitStaged` is then the only rule that ever fires, the watermark stays pinned at the
subscribe cursor for the life of the stream, and the first dropped subscription replays
essentially the whole `objects` table above that cursor — 256 rows per page with an
`EdgesGroupIncomingByID` per page, on the single connection every writer shares. That is
the whole-world pass this design exists to replace, reached without anything failing.

Nothing unsticks it. There is no secondary rule and no bound, and it is silent.

The same inference is also **off by one**: a drain that fills the batch exactly as the
receiver empties is reported as unsafe.

### Why the obvious fixes don't work

Worth knowing before proposing something simpler, because these have been considered and
rejected:

- *"Commit the high when a full batch is followed by a short one"* is already the behaviour
  — the short batch commits `seen`, which includes the earlier full batch's high. The
  problem is a short batch never arriving.
- *A time or count bound* cannot be made sound **as things stand**. `writeSignalMerge`
  keeps only the max version, so an entry's first-touch version is unrecoverable from the
  delivered value, and a key queued behind it can carry an arbitrarily lower version. No
  safe fallback is derivable from what the waker sees today — which is why the fix below
  starts by making that version available rather than by picking a bound.

## What changes

Take the bound from the backlog itself. Spec 7 adds `Receiver.Peek`, which reports the
oldest **undelivered** value without consuming it. Track that value's *first-touch*
version in the signal, and the correct watermark is one below it:

```
watermark = max(watermark, oldestPendingFirstVersion - 1)   // something is pending
watermark = max(watermark, seen)                            // the receiver is empty
```

Every version below `oldestPendingFirstVersion` belongs to a key that is either not pending
at all — so every write to it was delivered and processed — or pending in a slot created
*later* than that write, which means the write went out in an earlier slot that has already
been popped. The only remaining case is annihilation, a create/delete pair the consumer
never saw, and a deleted object has no dependents to wake (`edges.to_id` is
`ON DELETE RESTRICT`, so a depended-upon target cannot be removed). All three are covered.

Note the bound needs **no `min` against `seen`**: holding the cursor back to the highest
version actually delivered would only lose ground for the annihilation case, which needs no
wake. `seen` is still required, but only for the empty-receiver case — which is precisely
today's `commitDrained`.

This **generalizes** the existing rule rather than adding a second one beside it: the
empty-receiver case is exactly today's `commitDrained`. Expect the three-way `commitRule`
(`beehive.go:306-311`) to collapse — the live path stops needing a staged-versus-drained
distinction. `commitOrdered` (replay pages, which are version-ordered and complete) stays,
because it commits by a different quantity. Land whatever shape falls out; do not preserve
the enum for its own sake.

### `writeSignal` carries a first-touch version

`writeSignal` (`sqlite/watch.go`) is `{typ, rv}` and `writeSignalMerge` keeps `max(rv)`.
Add a `firstRV`, set equal to `rv` at publish, which the merge **preserves from `prev`**
while continuing to keep the max in `rv`.

Both are needed, and for opposite reasons:

- `rv` must stay the **latest**, because the delivered `ObjectWrite.ResourceVersion` is
  what advances `seen`. A merged reference whose version trailed the row would hand a
  resuming consumer a cursor behind the state it is about to read. Pinned by
  `TestObjectWritesSubscribeCoalescesRepeatWrites`.
- `firstRV` must stay the **earliest**, because it is the pending bound. Updating it on
  coalesce turns it into a latest-touch version and destroys the ordering the whole design
  rests on.

**Two invariants move into this repo with it**, and both need tests of their own rather
than being implied by an end-to-end assertion:

1. **`writeSignalMerge` never advances `firstRV`.** Previously the bus would have enforced
   this; now a later simplification of the merge could quietly break it, and every
   "the cursor advances" test would still pass.
2. **The queue front holds the *minimum* `firstRV`.** This is what makes `Peek` sufficient,
   and it holds only because publication is commit-ordered and `resource_version` is
   monotonic in commit order — so first-touch order *is* increasing-version order. Beehive
   guarantees both (`publishMu` in `Within`, and
   `TestResourceVersionMonotonicInCommitOrder`), which is why the argument belongs here
   rather than in the bus. Assert the composition, not just the two halves.

### The publish path must stay sequence-ordered

`Peek` reports the *oldest-touched* pending key. That is only the *lowest-versioned* one if
first touches are published in non-decreasing version order — and conflate is thread-safe
and accepts out-of-order sends by design, so nothing upstream enforces this.

Out of order it fails silently and permanently. Two writers at versions 100 and 101: if
101 publishes first, the queue is `[K(first=101), K(first=100)]`, `Peek` reports 101, the
waker commits 100, and `K`'s change *at* 100 is still queued and unprocessed. A later
replay from 100 excludes it, and its dependents never wake.

Beehive satisfies this today: `Within` holds `publishMu` across commit-and-publish so
publication order is commit order, versions are drawn inside the write transaction on a
single connection so version order is commit order, and within one transaction mutators
draw then append to the collector in that same order.

**What this makes load-bearing** is that *every* store method emitting an object change
self-wraps in `Within`. `ObjectsCreate` and `ObjectsDelete` had to be fixed for exactly
this reason; a new emitting method that publishes inline breaks the watermark with no
failing test anywhere near it. Add a guard the next author will trip over — the cheapest is
a test asserting that concurrent direct calls to each public emitting mutator produce
stream versions in increasing order.

### The store carries it through

`Peek` lives on the receiver, which only the `sqlite` backend holds, so the bound has to
reach the waker alongside the batch it describes — beside it in a second channel would
race. The stream item becomes a struct rather than a bare slice, roughly:

```go
type ObjectWriteBatch struct {
	Writes []ObjectWrite
	// OldestPending is the first-touch version of the oldest write still queued behind
	// this batch, or 0 when nothing is. resource_version_seq starts at 0 and is
	// pre-incremented, so no real version is ever 0 and the sentinel is unambiguous.
	OldestPending int64
}
```

Keep it blob-free; that constraint is why `ObjectWrite` exists in its present form (see its
doc comment in `internal/storeapi/storeapi.go`).

**Peek immediately after the drain, in the same goroutine.** That ordering is what makes
the answer exact rather than a stale observation: the receiver has a single consumer, and
the only concurrent mutation is a `Send`, which cannot change the front. Peeking later —
or from another goroutine — can report a higher bound than was true for the batch, which
commits a watermark above what was actually delivered.

### `WriteBatchCap` stops being contract

`storeapi.WriteBatchCap` (`internal/storeapi/storeapi.go:133-143`) was exported *only* so
consumers could infer drained-ness from batch length, and `store.go:77-80` re-exports it
into the beehive package for that one comparison. Both go back to being an implementation
detail of the backend's drain loop. Its doc comment states the inference as part of the
contract; that paragraph is now wrong and must go, along with the ADR's "short batch"
wording.

## Acceptance criteria

The starvation case is the point, so test it first and directly:

- **A stream of nothing but full batches still advances the watermark.** Drive more than
  `WriteBatchCap` distinct objects with a permanently non-empty receiver and assert the
  cursor moves. This fails today and is the whole reason for the change.
- **The cursor never passes an undelivered version.** With a re-written object riding a
  high version in an early batch and lower-versioned objects still queued behind it,
  assert the watermark stays below the lowest queued version — and that after a stream
  drop those objects' dependents are still woken.
- **A full batch that empties the receiver commits.** The off-by-one: it must behave as a
  short batch does.
- **`writeSignalMerge` preserves the earliest `firstRV` and the latest `rv`.** A direct
  unit test on the merge, not an end-to-end one. Both halves: coalescing must not advance
  `firstRV`, and must not hold `rv` back.
- **The peek happens after the drain, in the drain's own goroutine.** Advance the receiver
  between assembly and consumption and assert the committed watermark reflects the state at
  assembly. This is the ordering mistake that makes the fix silently unsound while every
  "it advances" test passes.
- **Replay pages still commit by their own high**, not by `seen` and not by
  oldest-pending. Keep the existing coverage
  (`TestWakerReplayAdvancesByPageNotStagedHigh`) green; the bug it pins is easy to
  reintroduce while reshaping `commitRule`.
- **Out-of-order publication is caught.** Concurrent direct calls to each public emitting
  mutator must produce stream versions in increasing order. This is the invariant `Peek`
  rests on and the one nothing upstream can enforce; without a guard, a future mutator that
  publishes outside `Within` breaks the watermark silently.
- **Replay stays bounded after a long busy stream.** With N changes missed and M objects
  in the store, N ≪ M, assert the replay reads ~N rows. This is the property the gap
  broke, so assert it end-to-end rather than trusting the cursor arithmetic.

Existing waker tests must stay green — in particular the recovery, low-water-mark and
backoff tests in `reconciler_test.go`. Any that assert on batch-length semantics are
testing the mechanism being removed and should be rewritten against the new signal, not
deleted.

The repo requires **100% coverage on library packages** (CI enforces it, measured with
`-coverpkg=./...`) and forbids sleep-paced tests — synchronize on channels the fakes
signal, never `time.Sleep`. The waker has seams for its retry pause and its clock
(`waitRetry`, `now`); set them through the fixture's configure hook **before** `start`,
since the waker reads them on its own goroutine.

## Out of scope

- The restart residual (a crash during an outage strands a settled dependent). Recorded in
  `TODO.md`; the fix is per-object `observed_cursor`, and it is a different mechanism.
- Changes made before the first successful subscribe are deliberately not replayed. See
  the ADR; leave that behaviour and its warning alone.
- Retuning `replayPageCap`. It may deserve it once replays are reliably small, but that is
  a measurement question, not this change.

## Follow-up when this lands

Update the ADR's "The watermark is a low-water mark" section and the `CLAUDE.md`
dependency-waker bullet — both currently describe the short-batch rule. The gobus
dependency in `go.mod` moves to whatever version ships spec 7.
