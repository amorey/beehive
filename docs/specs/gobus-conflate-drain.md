# Feature request: `conflate.Receiver.Drain`

- **Status:** Draft. This targets the sister library
  `github.com/amorey/gobus`, not this repository. Beehive is the caller that
  wants it.
- **Date:** 2026-08-03
- **Related:** `drainPending` in `objectswatch.go`, and the
  [watch shared tail ADR](../adr/2026-08-03-watch-shared-tail.md).

## What is asked for

One method on `conflate.Receiver[K, V]` that takes everything pending in a
single call, under one acquisition of the hub lock:

```go
func (rx *Receiver[K, V]) Drain() ([]gobus.Event[K, V], error)
```

It is `TryRecv` repeated until empty, made atomic and paid for once.

## Why a loop of `TryRecv` is not the same thing

Beehive's watch subscriber reads a burst as one unit. It calls `RecvContext`
for the first change, then `TryRecv` until the receiver reports empty, then
decodes and delivers the whole batch. Two problems follow, and both are
properties of the loop rather than of beehive.

### The loop is the anti-pattern the library already documents

`Peek`'s doc says it plainly:

> `Peek` takes the same hub lock that serializes the entire `Send` fan-out, so
> polling it in a loop degrades every publisher and every other receiver on the
> bus — call it once per unit of work.

`TryRecv` takes that same lock. A subscriber draining N pending keys acquires
the one hub mutex N times, and every acquisition contends with the fan-out of
every publisher and with every other receiver's reads. The advice for `Peek` is
"call it once per unit of work", and a drain **is** one unit of work — but the
API offers no way to say so. `Drain` is that way.

### The loop has no bound that the reader can see

The loop ends when the receiver reports empty. A producer that publishes
between two `TryRecv` calls postpones that. In beehive the loop terminates
because the producer is store-bound — the tailer publishes at most one page,
then must read the store again before it can publish more — but that is a fact
about beehive's producer, not about the bus. A reviewer reading the loop cannot
see the bound, and was right to ask.

## Why a caller cannot fix this itself

The obvious repair is to cap the loop: pop at most N and leave the rest. **For
a conflate consumer that cap is unsound**, and the reason is worth stating
because it is not obvious.

A conflate receiver's queue is ordered by **first touch**, and a merge leaves a
key at its original position. So queue order carries no relation to any
ordering quantity inside `V`. If the caller stops early it takes an arbitrary
subset of what was pending and leaves the rest for the next batch, and the two
batches interleave in that quantity.

Beehive is a concrete instance. Its values carry a log position, and a caller
checkpoints the position of a delivered change and resumes above it. A pending
queue of `A@12, B@11` — where `A@12` coalesced in place from `A@10` — is
delivered correctly only if both are taken together and sorted. Capping the
drain at one delivers `A@12`, leaves `B@11` for the next batch, and a caller
that checkpoints 12 loses `B@11` for good.

So the caller needs "everything pending as of one instant". That is a
consistent cut, and only the bus can take one: the caller cannot ask how many
keys are pending, and any answer would be stale before it acted on it.

**`Drain` does not order anything.** It returns queue order, which is
first-touch order. Beehive still sorts what it gets. What `Drain` supplies is
the *set* — the guarantee that nothing pending was left behind — which is what
makes sorting sound.

## Semantics

- **Returns everything pending**, in queue order, and empties the queue.
- **One acquisition of `s.mu`**, which is the point.
- **Precedence is `TryRecv`'s, unchanged**: closed beats empty beats value. A
  receiver or hub that is closed returns `ErrClosed`. A sender closed and
  drained returns `ErrClosed`. Nothing pending returns `ErrEmpty`.
- **Partial results are not a case.** Either the receiver is in a state that
  yields values, and `Drain` returns all of them with a nil error, or it
  returns no values and an error. There is no "some values and `ErrClosed`".
- **No `max` parameter.** A cap would give back the split this method exists to
  prevent. It is also unnecessary: conflate already bounds a receiver's memory
  by the live key set rather than by write volume, so "everything pending" is
  bounded by construction — the same property the package README claims for a
  slow receiver.
- **`Chan` interaction is unchanged.** An event already handed to the feeder has
  left the queue, so `Drain` does not see it — the same rule `Peek` states.
- **One consumer goroutine**, as for every other receive path.

An `Append` form (`DrainAppend(dst []gobus.Event[K, V])`) would let a caller in
a loop reuse one buffer. Worth considering, not required: beehive's batch is
short-lived and its allocation is dwarfed by the decode that follows.

## What the change also touches

Per the gobus repository's own rules:

- `README.md`: the `conflate` section, beside `Peek`. The "call it once per unit
  of work" advice on `Peek` should point at `Drain` as the way to honour it when
  the unit of work is a whole burst.
- Tests to 100% coverage on the new path, including the closed-and-drained and
  empty cases, and a test that a `Drain` racing a `Sender.Close` resolves
  closed-beats-value like the other receive paths.
- No conformance row. This is a method on an existing bus, not a new
  architecture, and `gobus.Receiver[K, V]` does not grow a member — `Drain` is
  concrete-type surface, like `Peek`.

## What beehive does until then

Nothing changes. `drainPending` keeps its unbounded `TryRecv` loop, which
terminates for the store-bound reason above, and sorts the result. The loop
carries a comment saying why it must not be capped. If `Drain` lands, the loop
collapses into one call and the sort stays.

The cost of waiting is N lock acquisitions per burst instead of one, on a hub
whose single mutex also serializes the tailer's fan-out. That is a real cost
under write load — see the
[single-connection contention item](../TODO.md) for the measurement harness —
but it has not been shown to matter, and no correctness claim rests on it.
