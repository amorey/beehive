# Expose a conflating receiver's pending backlog

**Repo: `github.com/amorey/gobus`, package `conflate`.** Not beehive — this is an
upstream library change. Beehive is the consumer, and the change it unblocks is
specified in [8-waker-watermark-from-pending-backlog.md](8-waker-watermark-from-pending-backlog.md).

**Status: ready for implementation.** Do this one first; spec 8 cannot start until it lands
(or until a local fork exists to prototype against). Beehive currently depends on
`gobus v0.1.0`. The open design choices this spec once left to the implementer are now
resolved inline (see "Decisions" under each accessor and the "Handoff notes" section); no
further clarification from the author is required to start.

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

The primitive is **two layers**, and separating them is what keeps it from being
beehive-shaped:

- **A default arrival sequence, always on.** Every receiver stamps each key, at first touch,
  with a per-receiver monotonic counter, and `OldestSequence()` reads the stamp of the
  oldest pending key. This needs no option and imposes no obligation on the caller — the bus
  owns the counter, so it is monotonic *by construction*. It is a general
  backlog-observability primitive (head-of-line staleness, stall detection, an in-process
  progress marker), independent of any domain notion of version.
- **An optional override, `WithSequence`,** that replaces the arrival counter as the stamp
  source with a caller function of the value — e.g. a domain version. This is what a durable
  cross-restart cursor (beehive) needs, and it is the **only** layer that carries the
  monotonic-send obligation, because the caller now owns the numbers.

Plus a convenience `Empty()`. All three are one coherent change.

This factoring is the answer to "isn't this over-fit to beehive?": the always-on layer is
general and obligation-free, and the sharp assumption is quarantined in the opt-in override.

### 1. The arrival sequence and `OldestSequence`

```go
// OldestSequence returns the sequence stamp of the oldest undelivered key, and false if
// nothing is pending. By default the stamp is a per-receiver arrival counter assigned at
// first touch; Hub.WithSequence overrides the source. Always available — no option needed.
func (rx *Receiver[K, V]) OldestSequence() (int64, bool)

// Hub.WithSequence overrides a receiver's default arrival counter: the stamp becomes
// seq(v) instead of a monotonic counter. The stamp is recorded when a key is first touched
// and is NOT updated when Merge coalesces into an existing slot.
//
// The caller must serialize stamp assignment with the Send that carries it, so that stamps
// reach the bus in non-decreasing order. A single-goroutine publisher satisfies this. A
// shared Sender called from several goroutines does NOT — even drawing versions from an
// atomic counter — because the winning order at the bus lock is not the version order, and
// a stamp that arrives below one already recorded panics on that Send (see below). If you
// need concurrent publishers with WithSequence, hold one lock across "assign stamp; Send".
func (h *Hub[K, V]) WithSequence(seq func(V) int64) ReceiverOption[K, V]
```

**The stamp is an *arrival sequence*, not a position or an index.** It is assigned once, at
first touch, and frozen: coalescing and the popping of earlier keys never change it. The
default source is a per-receiver counter that increments **only on a new-slot enqueue** — a
coalescing re-send does not advance it, so the counter measures *distinct keys admitted*,
not sends. Because keys are stamped in first-touch order and appended in first-touch order,
the front of `order` always holds the smallest stamp — so `OldestSequence` is the low-water
mark below which the receiver's backlog is empty, and it is a list-head read under the
existing lock. **The default's monotonicity is not a caller obligation and is immune to
concurrent senders**: the counter is assigned under `s.mu` inside `enqueueLocked`, so
whatever order goroutines win the lock in *is* the stamp order. The bus generates the numbers.

**`WithSequence` re-introduces the obligation, and enforces it — but only under serialized
publication.** When a caller overrides the source, the numbers are the caller's, computed
from the value, so the stamp reflects *caller-time* order while the enqueue happens in
*lock-acquisition* order. Those coincide for a single-goroutine publisher, and then the bus
can detect a violation for free — the stamps arrive pre-sorted, so an out-of-order one is a
genuine caller bug — and it panics rather than returning a silently wrong answer. This
reverses the earlier "silent wrong answer is acceptable" stance deliberately: for the
serialized publisher the detection is O(1) and correct, so there is no reason to tolerate the
silent failure.

The critical caveat, and why the obligation is stated as *serialized publication* rather than
loosely as "send order": two goroutines sharing the `Sender` (which is documented safe to
share, `conflate.go:131`), each drawing versions from a shared atomic counter, are each
monotonic — but the one that wins `s.mu` second may carry the *lower* version, and its stamp
arriving below the recorded floor would panic a caller who did nothing wrong. So the contract
is not "publish in increasing order"; it is "hold one lock across *assign-stamp-then-Send*",
which makes lock-arrival order equal version order. Beehive satisfies this exactly — its
store publishes under a mutex held across commit, so commit order, publication order, and
version order are one and the same. A `WithSequence` receiver is therefore **incompatible
with the concurrent-multi-sender pattern**; the always-on default counter is the tool for
that case. Enforcement is scoped to the override; the default never trips it.

**Conventions the override must follow (mirror `WithKeyFilter`/`WithMerge`):**

- `WithSequence` **panics on a nil `seq`**, exactly as `WithKeyFilter` panics on nil `keep`
  and `WithMerge` on nil `Merge`. Message: `"gobus: conflate.Hub.WithSequence requires a
  non-nil seq func"`.
- `seq(v)` is invoked **under the bus lock** at enqueue time, like `keep` and `merge`, so
  its doc comment carries the same "must not call back into the hub" warning.
- Store it on `receiverConfig` (`seq func(V) int64`) and copy it onto the `Receiver` in
  `receiver()`, alongside `keep` and `merge`. `seq == nil` selects the default counter.

**Receiver state.** Add to the `Receiver` struct:

```go
seq     func(V) int64 // override source; nil = default arrival counter
nextSeq int64         // default counter (seq == nil): next stamp, assigned from 1
lastSeq int64         // override floor (seq != nil): highest stamp seen; init math.MinInt64
```

`nextSeq` and `lastSeq` are mutually exclusive — a receiver is fixed at construction as
either default or override — so the struct effectively carries one live `int64`; a reviewer
may merge them, the spec does not require it. Initialize in `receiver()`: `nextSeq = 1`, and
`lastSeq = math.MinInt64` (so the first override stamp never trips the check).

**Start the default counter at 1, not 0.** `OldestSequence` returns `(0, false)` when
empty, so `0` must be an unambiguous "nothing pending" sentinel. If the first key also
stamped `0`, a caller who drops the `ok` — the exact misuse the accessor's `(int64, bool)`
shape guards against — would read a real first key as the empty value. Starting at `1`
makes `0` mean only "empty", at no cost. (The override has no such reserved value; its
stamps are the caller's domain versions.)

**Where the state goes — widen the `elems` value, do not touch the list element.** The
tempting move — boxing the stamp into the `list.Element.Value` alongside the key — is
measurably wrong: interface boxing puts a pointer in the word directly (free) and caches
small ints, but a two-field struct never fits the interface word and *always* heap-allocates.
`testing.AllocsPerRun` on `container/list`: `PushBack(int)` is 1 alloc, `PushBack(struct{k;rank})`
is 2 — a new allocation per new-key `Send`, on the fan-out path under the hub-wide `s.mu`.
Do not do it.

Instead carry the stamp in the `elems` map value, which is stored inline in the map's buckets
and so adds no per-`Send` allocation:

```go
type slot struct {
    e    *list.Element
    rank int64
}
// Receiver.elems changes from map[K]*list.Element to map[K]slot.
```

`slot` is **not** generic — neither field mentions `K` — so `elems map[K]slot` avoids an
unused type parameter and a per-`K` instantiation.

- `enqueueLocked`, **new-slot branch** (the body at `conflate.go:419-420`): compute the
  stamp from the active source, then store the slot. `order.PushBack(k)` still boxes the bare
  key, exactly as today. The panic message interpolates the offending values, since an
  operator hitting it needs them to debug (the nil-policy panics have nothing to interpolate,
  so this is not a convention break); this adds an `fmt` import.

  ```go
  var rank int64
  if rx.seq != nil {
      rank = rx.seq(v)
      if rank < rx.lastSeq {
          panic(fmt.Sprintf("gobus: conflate.WithSequence stamps must be non-decreasing "+
              "under serialized publication: got %d after %d", rank, rx.lastSeq))
      }
      rx.lastSeq = rank
  } else {
      rank = rx.nextSeq
      rx.nextSeq++
  }
  rx.elems[k] = slot{e: rx.order.PushBack(k), rank: rank}
  rx.pending[k] = v
  ```

- `enqueueLocked`, **coalesce branch**: reads the slot as `sl, ok := rx.elems[k]` and uses
  `sl.e` where it used the raw element; the slot is **not** rewritten, so the stamp is
  untouched. "Coalescing must not move the stamp" is thus *structural* — there is no
  stamp-write on this branch to get wrong.
- `enqueueLocked`, **annihilation branch**: `rx.order.Remove(sl.e)`; `delete(rx.elems, k)`
  drops the stamp for free.
- `popLocked` (`conflate.go:426`): **unchanged** — `k := e.Value.(K)` stays, and the existing
  `delete(rx.elems, k)` drops the stamp for free. The stamp never rode in the element, so
  `:431` is not edited at all.
- `OldestSequence`: under `s.mu`, `e := rx.order.Front()`; if `e == nil` return `(0, false)`;
  else `k := e.Value.(K)` and return `rx.elems[k].rank, true` — front key, then one map
  lookup. O(1), a single locked read, and it **never panics**: it works on every receiver,
  since the default source is always present.

This is **allocation-count** neutral against today (`map[K]*list.Element` → `map[K]slot` is
one map write either way — no new heap allocation per `Send`). It is not byte-neutral: the
map value grows from 8 to 16 bytes, i.e. `+8` bytes per live key, on exactly the
high-cardinality receiver this serves. That is clearly the right trade — one word per live
key against the allocation-per-`Send` the element-boxing form would cost — but say
"no new *allocation*", not "no memory cost". No side map, no nil-map guard, and
`popLocked`/`PushBack` are left alone.

**The enforcement panic aborts the fan-out mid-flight — and that is fine.** A violating
override stamp panics inside `enqueueLocked`, which runs inside `sendLocked`'s
`for rx := range s.receivers` loop, so receivers earlier in the loop have already enqueued
and later ones have not. This partial fan-out on panic is exactly the pre-existing behavior
for a panicking `Merge` or `keep`, and it is safe the same way: `Send`/`SendContext` release
`s.mu` through a deferred unlock, so the hub is not left locked. A monotonicity violation is
a caller contract breach — crashing loudly beats a corrupt cursor — so it is treated like
any other panicking user callback. It never fires for a correctly serialized publisher; the
partial fan-out is only reachable by a caller already in breach. Note this in the design,
don't re-solve it.

**Behavior after close — both accessors report raw queue state, undefined once closed.**
`Hub.Close` and `Receiver.Close` close `rx.done` but deliberately do *not* clear `order`/
`pending` (hard tear-down and abandonment, respectively). So a closed receiver still holds
whatever was queued, and these accessors keep reporting it: `Empty` stays `false` and
`OldestSequence` keeps returning a stamp whose value can never now be delivered. That is
acceptable **only because the accessors are defined as instantaneous reads of raw queue
state, not as part of the close/cancel precedence** — folding `rx.done.IsClosed()` in would
drag them into the ordered-verdict machinery the package keeps in one place, buy the cursor
use case nothing, and force a `forTesting*` seam. The doc comments on both accessors must
say plainly: *the result is meaningful only on a live receiver; after any `Close` it
reflects abandoned queue state and should not be consulted.* The consumer already stops
reading at `ErrClosed`/channel-close, so it never calls these post-close in the intended
flow. This "raw state, no close fold-in" decision needs its own coverage test (a `Close`d
receiver with a queued value: `Empty()` is still `false`) to lock the choice in against a
future "fix".

### 2. The instantaneous empty check

```go
// Empty reports whether this receiver has nothing pending right now. Unlike the
// terminal "drained" condition it says nothing about whether the sender is closed.
func (rx *Receiver[K, V]) Empty() bool
```

`order.Len() == 0` under the lock. Distinct from `drainedLocked`, which folds in
`txClosed`; a consumer asking "is there more to come right now" is not asking "is this
stream over".

**`Empty` is sugar over `OldestSequence`, kept for readability.** Since the default arrival
sequence is always present, `OldestSequence`'s `ok == false` is already the empty test — a
consumer *can* write `_, ok := rx.OldestSequence()` and negate it. `Empty` exists because
"is anything pending right now" is a question that shouldn't require the reader to know what
a sequence is, and because it reads cleanly against the `drained` distinction. It is a
genuine convenience, not a second capability; keep it, but the doc should say it equals
`!ok` from `OldestSequence`.

**Keep the test-only `lenForTest`; it is not redundant with `Empty`.** `lenForTest` reports
an exact count, and several existing tests assert one — `assert.Equal(t, 4, …)` then `3` in
`TestBoundedUnderSlowConsumer` (`conflate_test.go:563,566`, where the exact bound *is* the
point), and `== 1` at `:429`, `:475`, `:775`. `Empty` is a boolean and cannot express those.
Since exposing a pending count is deliberately out of scope (see Out of scope), the internal
test-only helper is exactly how that count stays internal — the duplication is the design.
The implementer may optionally repoint the four `== 0` call sites (`:415`, `:425`, `:451`,
`:477`) to `Empty()` so the new accessor gets exercised through existing tests, but must
leave `lenForTest` in place. (It is also named in the test-helper inventory at CLAUDE.md's
Testing section.)

**Both accessors intentionally cannot see an in-flight `Chan` event.** The `Chan` feeder
pops under `s.mu` and then parks on delivery outside it (`conflate.go:685-694`), and the
package already documents that a popped event has left the receiver's slots. So a consumer
reading from `Chan()` can observe `Empty() == true` / `OldestSequence()` false while exactly
one event is still held by the feeder, undelivered. This is sound for the cursor use case —
first-touch stamp order guarantees that in-flight event outranks everything the consumer has
already seen, so treating the queue as empty never rewinds the cursor past unseen work — but
it is not obvious and spec 8 builds on it. This caveat belongs in the **README subsection**
(below), not stacked on the method comment: these accessors describe the receiver's *slots*
and are cleanest for the `Recv`/`TryRecv` consumer that pops on the same goroutine it queries
from; a `Chan` consumer must account for the one event that may be in the feeder.

**The default sequence is per-receiver — not comparable across receivers or restarts.** Two
receivers stamp the "same" send with different arrival numbers, and the counter resets when
a receiver is created. So the default stamp is an *in-process progress marker*, not a durable
cursor: a consumer that needs a stamp comparable across a dropped-and-recreated subscription
(beehive) must use `WithSequence` to map to a real domain version. State this in the README
so nobody mistakes the default counter for a resume token.

**Where each caveat is documented.** Keep the one-line meaning on each method comment and
push the context to the README, so neither comment is buried under qualifications:

- *Post-close raw-state* caveat → **both** method doc comments (`Empty` and
  `OldestSequence`), since it is per-method behavior a caller reads at the call site.
- *`Chan` in-flight event*, *default is per-receiver / not durable*, and *`Empty` equals
  `!ok`* → the **README subsection** under `### Conflate` that this change already requires,
  alongside the monotonic-send obligation on `WithSequence`.

## Alternatives considered (recorded, not implemented)

**A per-send rank argument** — e.g. a conflate-only `SendSeq(k, v, seq int64)` that stamps
at send time instead of the receiver deriving the stamp. Rejected: it is hub-wide, so the
*receiver* has no signal that a domain sequence is in effect and `OldestSequence` could not
tell "no override" from "override is 0"; and mixing `Send`/`SendSeq` records `0` for the
plain path and silently corrupts the ordering. It would be the right shape only if a
consumer's rank were genuinely *external* to the value — not beehive's case, where the
version rides on the value. The `WithSequence` override keeps one enqueue path and one
handle for configure-and-read.

**An unconditional scan** for consumers who cannot meet the override's ordering obligation:

```go
// Pending calls yield for each undelivered key/value until yield returns false.
func (rx *Receiver[K, V]) Pending(yield func(K, V) bool)
```

Correct with no obligation and any fold the caller wants, but it walks the whole live key
set **under the bus lock**, serializing against every `Send` — worst on exactly the
high-cardinality workload the accessor serves, forcing cadence-based polling. This is the
zero-obligation general escape hatch, and it is the **designated extension point**: a second
consumer whose rank is not monotonic-in-send-order is the signal to build this tier, not to
weaken the `WithSequence` override. Not shipped now (YAGNI); recorded so it isn't
re-litigated.

**A generic rank type (`Hub[K, V, S]`).** The one generalization that keeps O(1) — widen
`int64` to an ordered type parameter — costs a third type argument on `New` that every
conflate user pays even without sequencing, and Go won't let it default. Deferring is *not* a
forced future migration: a differently-typed facility can be added later without breaking
`OldestSequence() (int64, bool)`. `int64` now; revisit only for a real second consumer.

## Handoff notes

### Files that change

- `conflate/conflate.go` — the `Receiver` struct (new `seq`, `nextSeq`, `lastSeq` fields;
  `elems` changes type from `map[K]*list.Element` to `map[K]slot`, so there is **no** new
  map and the list element is left as a bare key), the (non-generic) `slot` type,
  `receiverConfig` (new `seq` field), `receiver()` (copy `seq`; init `nextSeq = 1`,
  `lastSeq = math.MinInt64`; `elems` allocation type changes), `WithSequence` (new method,
  mirrors `WithKeyFilter`),
  `enqueueLocked` (stamp source + override enforcement per the design above — note `popLocked`
  is **unchanged**), and the two new accessors `OldestSequence`/`Empty`. Adds a `math` import
  (`math.MinInt64`, or a local `const`) and an `fmt` import (the enforcement panic message).
  Also extend **both** places the composable options are enumerated so neither goes stale: the
  package-doc "Per-receiver policy overrides" paragraph (`conflate.go:48-54`) and the
  `Hub.Receiver` doc-comment example (`conflate.go:193-196`), each to name/show
  `hub.WithSequence(...)`. Finally, add a one-line pointer from `Send`'s doc comment
  (`conflate.go:282-284`) to the `WithSequence` contract: `Send` reads as "never blocks,
  returns an error", but a `WithSequence` receiver makes it a library-originated panic path on
  a monotonicity violation, and a caller should not be surprised by it.
- `conflate/conflate_test.go` — the acceptance-criteria tests below. (`helpers_test.go` and
  its `lenForTest` stay as they are; see the `Empty` section.)
- `README.md` — a short subsection under `### Conflate` documenting the default arrival
  sequence and `OldestSequence`, the `WithSequence` override with its obligation stated as
  **serialized publication** (a shared `Sender` across goroutines does not satisfy it even
  with an atomic counter — the concurrency qualifier, not just "send order"), that
  `WithSequence` is observational and does not change delivery order, that the default counter
  is per-receiver and not a durable cursor, `Empty` equalling `!ok`, and the `Chan`
  in-flight-event caveat. CLAUDE.md requires README to move with any public behavior change,
  so this is not optional.

### Files that deliberately do **not** change

- `gobus.go` — `OldestSequence`, `Empty` and `WithSequence` are **conflate-specific**
  accessors on the concrete `*Receiver`/`*Hub`, not additions to the shared `Sender`/
  `Receiver` interfaces. Do not touch the interface docs.
- `conformance_test.go` — for the same reason, no new row and no new interface method. The
  cross-architecture suite pins close/cancel/value precedence, which this change does not
  affect. (If you find yourself editing it, you have widened the surface beyond this spec.)
- `Merge`, delivery order, `drainedLocked`, the receive paths, and the memory bound — all
  untouched (see Out of scope).

### CI gates to satisfy (all enforced separately by CI)

- **100% coverage on the library package.** Every new exported symbol needs a test, *and*
  every branch: the empty-front `(0, false)` return, the closed-receiver-still-non-empty
  case, both arms of the stamp-source `if rx.seq != nil` in `enqueueLocked`'s new-slot branch
  (a default-counter receiver and a `WithSequence` receiver), and the override monotonicity
  panic all need explicit coverage or the build fails.
- `WithSequence(nil)` panic path needs a test too (mirror the existing
  `WithKeyFilter(nil)`/`WithMerge(nil)` panic tests).
- `gofmt -l .` clean, `go vet ./...` clean, `staticcheck -checks=all ./conflate` clean,
  `go test -race ./...` green.
- Follow the package's test conventions (see CLAUDE.md → Testing): testify `assert`/
  `require`, no magic sleeps, assert whole `Event` values via `assertRecv`, and use the
  `waitParked` helper if a test needs a reader parked before a `Send`. These accessors are
  plain locked reads with no lock-free pre-check, so — unlike the close/cancel paths — they
  need **no** `forTesting*` race seam.

## Acceptance criteria

**Default arrival sequence (no `WithSequence`):**

- A receiver with no option returns increasing stamps for successive new keys: first key →
  `1`, next distinct key → `2`, and `OldestSequence` reports the front's. (First stamp is
  `1`, not `0`, so `0` stays the empty sentinel.)
- The counter advances **only on new keys, not on coalescing sends**: send key A (stamp 1),
  re-send A while pending (coalesce, no new stamp), send key B — B is stamp `2`, not `3`.
  This pins "the default measures distinct keys admitted, not sends".
- `OldestSequence` never panics and needs no option — assert it works on a plain receiver.
- The default counter does **not** trip the override enforcement under concurrent senders:
  a `-race` test with multiple goroutines sharing the `Sender` on a *default* receiver never
  panics (the counter is assigned under `s.mu`). This is the case a `WithSequence` receiver
  cannot serve, so pin the default's immunity.

**`WithSequence` override:**

- `OldestSequence` returns the stamp of the **first-touched** pending key, not the
  lowest-stamped value — assert these differ by coalescing a re-written key to a high stamp
  and checking the front still reports its original stamp.
- Coalescing does not move a key's reported stamp. This is the property the O(1) claim rests
  on; without a test it will be "fixed" into a latest-touch stamp later.
- `seq` is invoked exactly once per first-touch and **never on coalesce**: a `seq` closure
  that counts its calls, N sends of the same key while pending, assert count `== 1`. Guards
  against a refactor that hoists the stamp computation above the `if e, ok := rx.elems[k]`
  branch and runs `seq` on every `Send`.
- **Monotonicity is enforced under serialized publication**: a single-goroutine publisher
  that first-touches a key with a stamp lower than a previously first-touched key's panics on
  that `Send` (`assert.Panics` around the `Send`), and the panic message names both values. A
  non-decreasing sequence, including equal consecutive stamps, does not panic. Drive this from
  one goroutine — the enforcement keys on lock-arrival order, so a *concurrent* out-of-order
  arrival is a legitimate caller, not a test case, and `WithSequence` is documented
  incompatible with the concurrent-sender pattern (do not add it to
  `TestConcurrentSendersAndReceiver`).
- `WithSequence(nil)` panics, matching `WithKeyFilter(nil)`/`WithMerge(nil)`.
- `WithSequence` does **not** change delivery order: events still come out in first-touch
  order identical to the same workload with no override. Assert delivery is unchanged (the
  stamp is observational only).

**Shared by both sources:**

- Annihilation (a `Merge` returning `keep == false`) removes the stamp along with the slot,
  and `OldestSequence` then reports the next key's.
- After popping the front, `OldestSequence` reports the new front's stamp.
- With nothing pending, `OldestSequence` reports `false` and `Empty` reports `true`.
- `Empty` is `false` with a queued value even when the sender is closed — it is not
  `drained`.
- Key filters compose: a value the receiver's `WithKeyFilter` rejects is never enqueued, so
  it advances neither the default counter nor `OldestSequence`/`Empty`.
- On a `Close`d receiver still holding a queued value, `Empty` is `false` and `OldestSequence`
  still returns that value's stamp — pinning the "raw state, no close fold-in" decision.
- `OldestSequence` does no iteration — a list-head read plus one `elems` lookup — and the
  `Send` path adds no allocation beyond today. Pin with `testing.AllocsPerRun`, not a vague
  benchmark: (a) the **coalesce** path is allocation-free (the `elems` slot is untouched);
  and (b) new-key `Send` cost matches today for both a default and a `WithSequence` receiver,
  because `order.PushBack(k)` is unchanged and `elems` is one map write either way
  (`map[K]*list.Element` → `map[K]slot` adds no allocation, only `+8` bytes per live key). The
  list-element allocation on a new key is pre-existing, not new. True as literally stated
  *only* with the `elems`-slot design — the element-boxing form fails it for int and pointer
  keys.

## Out of scope

- Any change to `Merge`, delivery order, or the memory bound.
- Exposing the pending *count* or the queue contents beyond the accessors above. A count
  invites polling loops and answers nothing the cursor case needs. (Note the arrival counter
  is *not* a count — it is a monotonic stamp that never decreases and skips popped keys.)
- Exposing the *current/newest* arrival stamp alongside `OldestSequence`. It would let a
  consumer compute a lag span (`newest − oldest`), but that is a staleness-metric feature, not
  the cursor need; add it only when a consumer actually asks. Recorded so the default counter
  isn't quietly grown into a metrics surface.
- A generic rank type (`Hub[K, V, S]`) and a per-send stamp argument — both covered under
  *Alternatives considered*.
