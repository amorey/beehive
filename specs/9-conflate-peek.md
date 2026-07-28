# Expose a conflating receiver's backlog head with `Peek`

**Repo: `github.com/amorey/gobus`, package `conflate`.** Plus a small consumer-side
obligation in beehive (see "The consumer side").

**Status: proposed — an alternative to
[7-conflate-pending-backlog-visibility.md](7-conflate-pending-backlog-visibility.md).**
Spec 7 and this spec solve the same beehive problem (a watermark that never advances,
[8-waker-watermark-from-pending-backlog.md](8-waker-watermark-from-pending-backlog.md))
and are mutually exclusive. Read spec 7's *Background* and *The problem* sections first —
they are not repeated here. If this spec is adopted, spec 7 is withdrawn and spec 8's
"What changes" section is reworked against `Peek`.

Circulated for beehive-team feedback before implementation; see
"Questions for the beehive team" at the end.

---

## The idea in one paragraph

Spec 7 asks the bus to maintain the ordering quantity: every receiver stamps each key at
first touch, either with a bus-owned arrival counter or — via a new `WithSequence` option —
with a caller function of the value. This spec instead asks the *consumer* to maintain it,
in `V`, using the `Merge` function the bus already has, and adds a single accessor so the
consumer can read the head of the backlog:

```go
// Peek returns the oldest pending event without removing it.
func (rx *Receiver[K, V]) Peek() (gobus.Event[K, V], error)
```

`Merge` is already the hook for "what does it mean to have two undelivered values for this
key". Folding a first-touch version into the merged value is exactly what it is for. The bus
then needs no notion of sequence, rank, or monotonicity at all — it only needs to let a
consumer look at the front of the queue without consuming it.

## What to add

### `Peek`

`Peek` is **`TryRecv` without the pop**, and that is the whole contract:

```go
// Peek returns the oldest pending event without removing it, so a subsequent
// Recv or TryRecv returns the same key. It returns [gobus.ErrEmpty] if nothing
// is pending, or [gobus.ErrClosed] if the receiver or hub is closed (or the
// sender is closed and the pending values have drained) — the same precedence
// [Receiver.TryRecv] applies.
//
// The returned value is the current merged contents of the key's slot: a Send
// that coalesces into it between two Peeks changes what the second Peek
// reports, while the key's queue position (and therefore its identity as the
// head) is unchanged.
func (rx *Receiver[K, V]) Peek() (gobus.Event[K, V], error)
```

Implementation mirrors `TryRecv` (`conflate.go:596-619`) line for line, substituting a new
`peekLocked` for `popLocked`, and including the terminal `deregisterLocked` on
`drainedLocked` — the drained verdict is terminal however it is observed, and CLAUDE.md
requires the tear-down to happen under the lock that decided it.

```go
// peekLocked returns the oldest pending event without removing it. Caller holds s.mu.
func (rx *Receiver[K, V]) peekLocked() (gobus.Event[K, V], bool) {
	e := rx.order.Front()
	if e == nil {
		return gobus.Event[K, V]{}, false
	}
	k := e.Value.(K)
	return gobus.Event[K, V]{Key: k, Value: rx.pending[k]}, true
}
```

**`popLocked` stays exactly as it is** (`conflate.go:426-437`). Refactoring it to delegate
to `peekLocked` would cost an extra `elems` lookup to recover the element it already had, on
the pop path under `s.mu`; the four duplicated lines are the cheaper trade. Do not
"deduplicate" this.

**`Peek` needs a `forTestingBeforePeekLock` seam**, alongside the existing
`forTestingBeforeRecvLock`/`forTestingBeforeTryRecvLock`, because it carries a lock-free
`rx.done.IsClosed()` pre-check that is re-checked under `s.mu`. That is the package
convention (CLAUDE.md → Conventions: "Every such re-check has a `forTesting*` hook"), and
CI's 100% coverage requires the under-lock arm to be reachable deterministically.

### Nothing else

No new option, no new receiver state, no change to `enqueueLocked`, `popLocked`, `Merge`,
delivery order, or the memory bound. No new imports. `elems` stays
`map[K]*list.Element`. The `Send` path is not touched at all.

There is **no `Empty()`**: `Peek` returning `gobus.ErrEmpty` already is the empty test, and
unlike spec 7's `Empty` it distinguishes "empty" from "over" for free. `lenForTest` stays as
it is — several tests assert an exact pending count (`conflate_test.go:429,475,563,566,775`)
and exposing a count publicly remains out of scope.

## The consumer side (beehive)

This is the half of the design that moves out of the library, so it is specified here even
though it lands in the other repo.

1. **`ObjectWrite` gains a `firstVersion int64`** next to `resource_version`.

2. **The producer stamps it on every send**: `firstVersion = resource_version`. This is not
   optional and is the one sharp edge of the design — `Merge` is **never called on a first
   touch**. `enqueueLocked`'s new-slot branch (`conflate.go:418-421`) stores the sent value
   verbatim; `merge` runs only on the coalesce branch (`:405-417`). A design that expects
   `Merge` to establish the field leaves every first-touch value carrying a zero, and the
   watermark is pinned at `-1` forever while every "it advances" test still passes.

3. **`writeSignalMerge` carries it through**:

   ```go
   merged.version      = max(prev.version, next.version)   // unchanged
   merged.firstVersion = prev.firstVersion                 // or min(prev, next), defensively
   ```

   `prev` is the value already in the slot, so `prev.firstVersion` *is* the first-touch
   version. The `min` form is equivalent under version-ordered publication and costs
   nothing; take it if you want the belt-and-braces.

4. **The waker's watermark**, read **when the batch is assembled** (spec 8's ordering
   requirement is unchanged and is still the mistake that makes this silently unsound):

   ```
   ev, err := rx.Peek()
   err == nil            → watermark = min(seen, ev.Value.firstVersion - 1)
   ErrEmpty / ErrClosed  → watermark = seen
   ```

   Note `ErrClosed` is treated as "nothing pending". A drained-and-closed receiver has an
   empty backlog by definition, so the empty rule is the correct one; the stream ending is
   reported separately by the receive path.

5. **The monotonicity invariant is beehive's to assert.** The soundness argument is
   unchanged from spec 7 — the front of `order` is the earliest first-touched key, so under
   version-ordered publication its `firstVersion` is the lowest pending version — but the
   bus no longer checks it. beehive's store already publishes under a mutex held across
   commit, so commit order, publication order and version order are one and the same; assert
   that at the send site if it is worth pinning. See "What this gives up", item 2.

## Why this shape rather than spec 7's

Recorded because spec 7 is a complete, implementation-ready design and this needs to justify
displacing it.

1. **The library change collapses to one accessor.** Spec 7 adds a `slot` type, widens
   `elems` from `map[K]*list.Element` to a 16-byte value (`+8` bytes per live key, on exactly
   the high-cardinality receiver it serves), adds three `Receiver` fields, a `receiverConfig`
   field, a new `Hub` option, an enforcement panic inside `enqueueLocked`, and `fmt`/`math`
   imports — all on the fan-out path under `s.mu`. This adds one method, one unexported
   helper and one test seam, and touches no existing code path.

2. **The rank-type problem disappears.** Spec 7 records `Hub[K, V, S]` (a generic ordered
   rank) as a rejected generalization and bakes `int64` into `OldestSequence`'s signature
   permanently. Here the ordering quantity lives in `V`, is of whatever type the consumer
   likes, and is folded however the consumer likes. The deferred generalization is simply not
   needed.

3. **The monotonic-publication obligation leaves the library.** Spec 7 must document the
   obligation as *serialized publication*, enforce it with a panic that aborts fan-out
   mid-flight, and then declare `WithSequence` incompatible with the shared-`Sender`-across-
   goroutines pattern the package elsewhere documents as safe (`conflate.go:131`). With a
   `Merge` fold the bus makes no ordering claim whatsoever and there is no new panic path out
   of `Send`.

4. **`Peek` is the more general primitive, not the less general one.** Spec 7's always-on
   arrival counter is justified largely as "this isn't beehive-shaped", but it is a
   bus-invented ordinal with no meaning outside the process. `Peek` is domain-free by
   construction, imposes no obligation, and is the O(1) head-only form of the
   `Pending(yield func(K, V) bool)` escape hatch spec 7 records as its designated extension
   point — without that one's whole-key-set walk under `s.mu`.

5. **It uses the bus's own idiom.** `Merge` is the designated per-key combining policy.
   `WithSequence` would be a second, parallel per-key policy callback overlapping that role.

## What this gives up

Stated plainly; these are the trade-offs the beehive team is being asked to weigh.

1. **No general numeric backlog layer.** A consumer whose `V` carries no ordering field gets
   nothing numeric from `Peek` — no "distinct keys admitted" ordinal, no lag span, no
   head-of-line staleness *number*. It can see *what* is at the head, not *how far* the queue
   has advanced. Spec 7's always-on counter provides that; this does not. The question is
   whether that layer has a customer today.

2. **A monotonicity violation is silent instead of loud.** Spec 7 panics when a first-touch
   stamp goes backwards. Nothing here checks, so a version-ordering bug in beehive's
   publisher yields a watermark that is *too high* — precisely the silent-skip failure the
   whole watermark design exists to prevent. Two mitigations: assert the invariant at
   beehive's send site (cheaper, and where the knowledge lives), and note that spec 7's check
   only ever covered the serialized publisher — i.e. the case that was already correct — and
   explicitly could not cover the concurrent one.

3. **No improvement on the front-key argument.** `Peek` reports the front only, so soundness
   still rests on "publication is version-ordered ⇒ the earliest first-touched key holds the
   lowest pending version". That is the same premise `OldestSequence` rests on. No
   regression, but no new robustness either. A consumer that cannot meet it needs the
   `Pending` scan tier, under either design.

4. **A slightly wider surface.** `Peek` hands back the pending value, not an opaque number,
   so it is marginally more inviting to a polling loop. It leaks no new information —
   conflate hands the same values out via `Recv` — but it is a value-returning accessor where
   spec 7 offered a scalar.

5. **The `Chan` in-flight caveat is unchanged and still applies.** The feeder pops under
   `s.mu` and parks on delivery outside it (`conflate.go:685-694`), so a `Chan` consumer can
   observe `Peek() == ErrEmpty` while exactly one event is still held by the feeder,
   undelivered. Sound for the cursor case by the same first-touch argument spec 7 gives — the
   in-flight event outranks everything already seen — but it must be documented. beehive's
   backend drains with `TryRecv` on the assembling goroutine, so it is unaffected.

## Handoff notes

### Files that change

- `conflate/conflate.go` — `peekLocked` (new, next to `popLocked`), `Peek` (new, mirroring
  `TryRecv`), `forTestingBeforePeekLock` on the `Receiver` struct alongside the existing two
  seams and in their shared doc comment. Add one line to the package doc's **Semantics**
  section (`conflate.go:26-60`) noting the backlog head is observable without consuming it.
  Nothing else in the file is edited.
- `conflate/conflate_test.go` — the acceptance-criteria tests below.
- `README.md` — a short subsection under `### Conflate` documenting `Peek`: that it is
  `TryRecv` without the pop and shares its precedence; that the value it returns is the
  current merged slot contents and can change between calls while the head key does not; the
  `Chan` in-flight caveat; and the intended pattern of folding an ordering quantity into `V`
  via `Merge` and reading it from the head (with the "publication order must match that
  quantity's order" premise stated as the consumer's, not the bus's). CLAUDE.md requires
  README to move with any public behavior change.

### Files that deliberately do **not** change

- `gobus.go` — `Peek` is a conflate-specific accessor on the concrete `*Receiver`, not an
  addition to the shared `Receiver` interface. It may generalize to other bus types later;
  adding it to the interface now would oblige every future architecture to have a
  head-of-queue notion. Do not touch the interface docs.
- `conformance_test.go` — no new row and no new interface method, for the same reason. This
  change does not affect close/cancel/value precedence.
- `enqueueLocked`, `popLocked`, `Merge`, delivery order, `drainedLocked`, the receive paths,
  the memory bound, and `helpers_test.go`.

### CI gates

`gofmt -l .` clean, `go vet ./...` clean, `staticcheck -checks=all ./conflate` clean,
`go test -race ./...` green, and **100% coverage on the library package** — which for `Peek`
means every branch: the lock-free `ErrClosed`, the under-lock `ErrClosed` (via the new seam),
the drained `ErrClosed` *including* its deregistration, the `ErrEmpty` empty-front return,
and the success return. Follow the package's test conventions (testify, no magic sleeps,
whole-`Event` assertions via `assertRecv`, `waitParked` where a reader must be parked).

## Acceptance criteria

- **`Peek` does not consume.** After `Peek`, a `TryRecv` returns the identical `Event`, and
  `lenForTest` is unchanged across the `Peek`.
- **`Peek` reports the head, not the newest.** With keys A then B pending, `Peek` returns A;
  after popping A it returns B.
- **`Peek` reflects coalescing without moving the head.** Send A, send B, then re-send A with
  a value that merges to something new: `Peek` still returns key A, now carrying the merged
  value. This is the property the whole consumer-side design rests on — that the head key's
  identity is stable while its value is not.
- **`Peek` sees an annihilation.** A `Merge` returning `keep == false` for the head key makes
  `Peek` report the next key.
- **Empty receiver** → `gobus.ErrEmpty`, zero `Event`.
- **Precedence matches `TryRecv`.** A closed receiver with a value still queued returns
  `ErrClosed`, not the value — pinning that `Peek` is *not* a raw-state read. A closed hub
  likewise. A closed sender with values still pending returns the head (soft drain), and
  returns `ErrClosed` once empty.
- **The drained `ErrClosed` deregisters**, same as `TryRecv` — assert via
  `forTestingReceiverCount`.
- **Close wins the race under the lock.** Using `forTestingBeforePeekLock`, close the
  receiver between the lock-free check and `s.mu`, and assert `ErrClosed`. Mirrors the
  existing `TryRecv` race test.
- **Key filters compose**: a value the receiver's `WithKeyFilter` rejects never becomes the
  head.
- **`Peek` does no iteration and allocates nothing.** Pin with `testing.AllocsPerRun`, not a
  benchmark: `Peek` on a receiver with many pending keys is 0 allocs and O(1) (list-head read
  plus one map lookup).

## Out of scope

- Exposing the pending *count* or the full queue contents. Unchanged from spec 7: a count
  invites polling loops and answers nothing the cursor case needs.
- A `PeekN` / bulk head inspection, and the `Pending(yield)` full scan. `Pending` remains the
  recorded extension point for a consumer whose ordering quantity is not monotonic in
  publication order.
- Adding `Peek` to the shared `gobus.Receiver` interface.
- Any change to `Merge`, delivery order, or the memory bound.

## Questions for the beehive team

1. **Is item 1 under "What this gives up" a real loss?** Does anything want a domain-free
   numeric backlog marker (stall detection, head-of-line staleness metric) that `Peek` cannot
   serve? If yes, spec 7's always-on layer has a customer and this spec is the wrong trade.
2. **Is stamping `firstVersion` on every send acceptable in the store?** It is one field
   assignment at the publication site, but it is a producer obligation the bus cannot check —
   a forgotten stamp is a silently pinned watermark. Would you rather the bus own it (spec 7)
   than have this be a code-review invariant?
3. **Is asserting version-ordered publication at beehive's send site enough**, given the bus
   no longer panics on a violation?
4. **Does `ObjectWrite` gaining a second version field conflict with the blob-free stream
   constraint** its doc comment describes (`internal/storeapi/storeapi.go`)? One `int64`
   should be fine, but it is your invariant.
