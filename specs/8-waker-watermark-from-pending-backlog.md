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
- *A time or count bound* cannot be made sound. `writeSignalMerge` keeps the **max**
  version, so an entry's first-touch version is unrecoverable from the delivered value,
  and a key queued behind it can carry an arbitrarily lower version. There is no safe
  fallback derivable from what the waker currently sees.

## What changes

Take the bound from the backlog itself. Spec 7 adds a receiver accessor reporting the
sequence of the **oldest undelivered value**; the correct watermark is one below it:

```
watermark = min(seen, oldestPending - 1)      // when something is pending
watermark = seen                              // when the receiver is empty
```

Every version below `oldestPending` has either been delivered, or been superseded by a
later write to the same key — and if that later write is still pending, its version is at
or above `oldestPending`, so it sits *above* the watermark and a replay picks it up. Both
cases are covered.

Note this **generalizes** the existing rule rather than adding a second one beside it: the
empty-receiver case is exactly today's `commitDrained`. Expect the three-way `commitRule`
(`beehive.go:306-311`) to collapse — the live path stops needing a staged-versus-drained
distinction, and `commitOrdered` (replay pages, which are version-ordered and complete)
stays because it commits by a different quantity. Land whatever shape falls out; do not
preserve the enum for its own sake.

### The store has to carry it

The accessor lives on the receiver, which only the `sqlite` backend holds. So the value
has to reach the waker alongside each batch — the batch type is `[]ObjectWrite`, so this
is a signature or wrapper change on `ObjectWritesSubscribe`'s stream. Pick the shape that
keeps the stream blob-free; that constraint is why `ObjectWrite` exists in its present
form (see its doc comment in `internal/storeapi/storeapi.go`).

Read the accessor **when the batch is assembled**, not when it is consumed: by the time
the waker processes a batch, the receiver has moved on, and a later read would report a
higher oldest-pending than was true for that batch — committing a watermark above what was
actually delivered.

### `WriteBatchCap` stops being contract

`storeapi.WriteBatchCap` (`internal/storeapi/storeapi.go:133-143`) was exported *only* so
consumers could infer drained-ness from batch length, and `store.go:77-80` re-exports it
into the beehive package for that one comparison. Both go back to being an implementation
detail of the backend's drain loop. Its doc comment currently states the inference as part
of the contract; that paragraph is now wrong and must go, along with the ADR's
"short batch" wording.

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
- **The oldest-pending value is read as of batch assembly.** Advance the receiver between
  assembly and consumption and assert the committed watermark reflects the earlier state.
  This is the ordering mistake that makes the fix silently unsound while every "it
  advances" test passes.
- **Replay pages still commit by their own high**, not by `seen` and not by
  oldest-pending. Keep the existing coverage
  (`TestWakerReplayAdvancesByPageNotStagedHigh`) green; the bug it pins is easy to
  reintroduce while reshaping `commitRule`.
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
