# Recover missed dependency wakes from a `resource_version` watermark

**Status: decided, not built.** Independent — the gap it closes exists today at the
default configuration and needs nothing else in this directory.

Replaces the escalation mechanism recorded in
[docs/adr/2026-07-27-dependency-wake-escalation.md](../docs/adr/2026-07-27-dependency-wake-escalation.md).
When this lands, that ADR needs a successor record, the `CLAUDE.md` bullet that
summarizes it needs updating, and `TODO.md:95-118` (the same subject, including the
reverted stride throttle) is resolved rather than deferred — close it out, don't
re-file it.

---

## Terms

- **Dependency waker** — the goroutine that watches one store-wide stream of object
  writes. When an object changes, it looks up everything that declared a `depends_on`
  edge pointing at that object and requeues those *dependents*.
- **Catchup pass** — a periodic pass over objects that still have work owed
  (`WithCatchupInterval`).
- **Full pass / resync** — a periodic pass over *every* object of a kind, settled or
  not (`WithResyncInterval`, off by default).
- **Escalation** — today's repair mechanism. When the waker loses a change, it asks every
  registered reconciler to turn its next periodic pass into a full pass, so the
  dependents it failed to name get re-derived anyway.

## The gap

The waker has three known loss points. All three log at Warn, and all three repair
themselves by escalating a periodic pass into a full one:

| loss point | escalation |
|---|---|
| `EdgesGroupIncomingByID` fails (`beehive.go:415-432`) | `resyncKindsNextTick` — one full pass. It cannot be narrower: the lookup that failed is the one that would have named the dependents. |
| the change stream closes (`beehive.go:370`) | `resyncKindsEveryTick` — changes keep being dropped, not just one |
| `ObjectWritesSubscribe` fails (`beehive.go:330-338`) | `resyncKindsEveryTick`, same reason |

Escalating sets `resyncOnce` or `resyncAlways` on every registered reconciler. But those
flags are read in exactly one place: inside the catchup ticker's `case`
(`reconciler.go:551-568`). So when both `catchupInterval <= 0` and `resyncInterval <= 0`,
nothing ever ticks, nothing ever reads the flags, and the repair never runs.

The code already knows about this condition. `hasPeriodicPass`
(`reconciler.go:259-261`, `beehive.go:130-137`) tests for it, and the subscribe-failure
path even reports it to the operator (`beehive.go:334-335`):

> *"dependency waker subscription failed and there is no periodic pass to fall back on;
> no dependency wakes will be delivered for any kind — drive them with `Client.Requeue`."*

**So the failure is diagnosed and then not repaired.** And the miss is permanent, not
just slow: a dependent that has already settled is invisible to a catchup pass, because
its own generation never moved. Nothing else will ever come back for it.
`beehive.go:425-429` says as much, in the very code that arms the repair it has no way
to spend.

### Why this is worse than "one backstop weaker"

**The blast radius is total.** A failed subscribe takes down *every* dependency wake for
*every* kind, for the whole life of the process — there is only one stream. With no
catchup tick there is no mechanism left that can notice a stale dependent, so the
control plane quietly stops honoring `depends_on` while continuing to look healthy.

**It happens at the default configuration.** `resyncInterval` already defaults to 0, so
the repair rides the catchup tick alone. Setting one more knob to 0 removes it entirely.
And that knob is documented only as *"A value <= 0 disables the catchup tick"* — nothing
hints that a correctness repair depends on it.

## The design

Don't repair by re-deriving everything. Repair by replaying what was missed.

`resource_version` is a globally monotonic counter drawn from `resource_version_seq`. It
is never reused, not even after a physical delete (`0001_init.sql:216-229`), and it is
indexed by `idx_objects_rv`. That makes it a watch cursor — the schema comment calls it
one.

So: the waker remembers the highest `resource_version` it has finished processing. After
a failed lookup, or when a subscription is re-established, it asks the store for every
object with `resource_version > watermark`, runs those through the same
`EdgesGroupIncomingByID` path it uses for live changes, and advances the watermark. Cost
is O(changes missed) — not O(table), and not O(dependents).

### A cursor is not a trigger: the waker needs its own driver

**This is the part that does not exist yet, and it is the bulk of the work.** Nothing
currently survives any of the three loss points: `dependencyWakerStart` returns on
subscribe error (`beehive.go:337`), and `dependencyWakerRun` returns when the stream ends
(`beehive.go:370`). A watermark tells you *where to resume*; something still has to be
alive to decide *to* resume.

- **Failed lookup** — the live loop is still running, so it retries inline. No new
  driver needed.
- **Stream closed / subscribe failed** — there is no stream, and by hypothesis no tick.
  The waker needs a **resubscribe loop**: on either failure, back off, re-subscribe,
  replay from the watermark, resume live consumption.

So the honest claim is *not* "this removes the tick dependency entirely" — it **moves the
timer from the reconciler into the waker**. That is still the trade worth making: one
goroutine instead of a per-kind full pass, O(missed) instead of O(table), and no operator
knob whose default silently disables a correctness repair. But the spec should say it
plainly, because the headline test below is unimplementable without this loop.

**The backoff needs an injectable seam, and this is a hard requirement, not a nicety.**
`CLAUDE.md` forbids sleep-paced tests, and the headline test drives a subscribe failure
*through* the resubscribe loop — so without a seam it has to wait on a real backoff
interval, which is precisely the flaky-on-CI `time.Sleep` the conventions rule out. The
store already has the pattern in `beforeLiveSend` / `afterStream`
(`sqlite/watch.go:388,397`); the new waker struct is the natural home for the waker's
equivalent. This is the difference between a test that synchronizes and a test that
sleeps 200ms.

### The replay query pages

An unbounded replay against a store pinned to `SetMaxOpenConns(1)`
(`sqlite/sqlite.go:45`) blocks every writer in the process for its whole duration and
materializes the entire id set. After a long outage, that is the table — the exact cost
profile this design exists to avoid.

Page it: rv-ordered, `LIMIT` per page, advancing the watermark **per page**. Advancing
per page is safe precisely because pages are rv-ordered — this is the one place ordering
is guaranteed. Size pages in line with `writeBatchCap` (`sqlite/watch.go:369`) so replay
and live traffic have the same cost shape.

### Retry backs off to a ceiling, and never gives up

A lookup failing because the store is unhappy will keep failing, and a tight retry loop
against a single-connection store makes the outage worse. This matters more than it
normally would: after this change, the retry loop is the *only* recovery path.

**"Ceiling" means a cap on the interval, not on the attempts.** The loop retries forever.
An attempt cap would reintroduce exactly the permanent-death failure mode this spec exists
to kill — a waker that has given up is the dead waker of `beehive.go:370`, reached by a
slower route.

### Not a problem — don't "fix" it

`edgesByIDs` already chunks under the bound-parameter limit at `idChunkSize = 30000`
(`sqlite/store.go:1488-1511`), so a large replay batch handed to
`EdgesGroupIncomingByID` is safe as-is. Noted because someone will otherwise add a second
layer of chunking on top.

## Where the watermark lives

### Ownership: one field, one goroutine, no atomic

The watermark is a plain `int64` field on the waker's own state, read and written **only
by the waker goroutine** — including its resubscribe loop, which is that same goroutine.
No mutex, no `atomic.Int64`: reaching for one would imply other goroutines may read it,
which is exactly the invariant to preserve. Nothing outside the waker has any business
knowing the cursor.

There is no waker struct today — it is two methods on `*Beehive` plus a loop — so this
change likely introduces one. Put the watermark there rather than on `Beehive` next to
the config knobs, so its single-goroutine ownership is visible from where it is declared.

### Lifetime: one process, deliberately

The watermark is **in memory and does not survive a restart.** A fresh process
initializes it from `currentResourceVersion` and replays nothing from before the crash.
That is a deliberate boundary, not an oversight — see [Out of scope](#out-of-scope-the-restart-residual)
below.

### The update rules, in one place

1. **Initialize from the cursor `ObjectWritesSubscribe` hands back** — there is no
   ordering rule to follow, because the [signature change](#scope-three-store-changes-not-two)
   makes the bad order unrepresentable. Subscribing *is* how you learn the cursor, so
   there is no second call to misorder. On resubscribe the returned value is **ignored**:
   the waker keeps the watermark it already has, which is the whole point of holding one.

2. **Advance from `max(rv)` of every batch actually consumed — including the no-op
   ones — but only commit it on a batch shorter than `WriteBatchCap`.** A short batch
   is how the backend reports that its drain ended on an empty receiver. Delivery is
   in first-touch order, not version order, so a full batch may have left lower
   versions queued behind it; its high-water mark stages until a short batch confirms
   there is nothing left to step over. `dependentsWake` early-returns when a batch holds no `Added`/`Modified`
   (`beehive.go:410-412`), and `writeSignalMerge` annihilates unobserved transients
   outright (`sqlite/watch.go:135-137`). If those paths don't advance the cursor, a
   delete-heavy store leaves the watermark arbitrarily far behind and "replay is bounded"
   quietly degrades to O(table).

3. **Never advance on receipt** — only on a batch whose dependents lookup completed.

4. **On a failed lookup, stop consuming the stream and retry replay-from-watermark until
   it succeeds.** This is what makes the cursor a genuine low-water mark rather than
   `max(processed)`, and it does so by construction instead of by bookkeeping.

   The hazard it precludes: batches do not arrive rv-ordered across a failure. If batch A
   (`X@100`) fails its lookup and batch B (`Z@150`) succeeds, advancing to 150 drops `X`
   **permanently** — while passing every "it recovers" test. Stalling removes the
   interleaving that makes that possible, so there is no need to track
   `min(failed rv) - 1` and hold there.

   Stalling the live loop is safe *because the hub conflates per object*: a stalled
   consumer's pending set is bounded by the live key set, not by churn
   (`writeSignalMerge`, `sqlite/watch.go:134-141`). A stalled waker costs bounded memory,
   not unbounded.

5. **During replay, advance per page.** Safe because replay pages are rv-ordered — the
   one place ordering is guaranteed. See [the paging rule](#the-replay-query-pages).

### Out of scope: the restart residual

A crash during a waker outage can strand a settled dependent, and **this design does not
fix that.** State it plainly rather than letting the in-memory choice imply otherwise.

The exposure is narrow. `enqueueCatchup` runs unconditionally at startup
(`reconciler.go:498`), but it lists unsettled objects plus `reconcile_owed` stamps, and a
settled dependent has neither — its generation never moved and nothing stamped it.
`enqueueAll` is what would catch it, and that is gated on `startupResync`
(`reconciler.go:504`), which defaults to `true` (`beehive.go:498`). So the hole requires
**`WithStartupResync(false)` *and* a crash mid-outage** — though note that is the same
configuration the headline test uses, which is why it needs saying out loud.

**Persisting this watermark is not the fix.** It records *delivery* ("the waker reached rv
N"), while what has to survive a restart is *convergence* ("dependent D actually
reconciled against X's new state"). Those come apart in the ordinary case: if the waker
requeues D, advances past X, and the process dies before D's reconcile runs, a persisted
cursor is already past X and D is stranded exactly as before. So persisting buys half the
restart hole, leaves the twin half open, and charges a write per consumed batch on a
single-connection store — the cost profile this whole design exists to avoid.

**The recorded right answer is per-object `observed_cursor`** (`TODO.md:786-802`): stamp
the global `resource_version` as of the reconciler's load, advance it on every successful
pass, and enqueue any dependent whose target's version now exceeds it. Durability
attaches to the thing that must converge, so it survives a restart by construction.

And the cheap durable trick is unavailable, which is why this cannot be folded in here:
you cannot stamp `reconcile_owed` on the dependent at failure time, because **the lookup
that failed is the one that would have named the dependents**. Recorded intent needs to
know who to record against; that is precisely the information the failure destroyed.
Derived staleness does not — which is `observed_cursor`'s whole argument over a stamp.

## Scope: three store changes, not two

**1. `ObjectWrite` gains `ResourceVersion int64`.** Cheaper than it looks: `writeSignal`
already carries `rv` and `writeSignalMerge` already merges on `max`
(`sqlite/watch.go:119-141`), so this is passing `wev.Value.rv` through at
`sqlite/watch.go:398` and `:409`. It costs eight bytes and reintroduces none of the
blob-pinning that `ObjectWrite`'s doc comment
(`internal/storeapi/storeapi.go:135-143`) exists to prevent.

**2. A new `Store` method for the replay query** — `ObjectIDsListSince(ctx, afterRV,
limit)` or similar, plus its `fakeStore` entry in `testutils_test.go`.

`ObjectsList` cannot be reused: it is per-kind and returns full `*RawObject`s with blobs,
which reintroduces exactly the memory problem `ObjectWrite` is lean to avoid. The new
method must be **blob-free and kind-agnostic**, returning ids (+rv) only. `idx_objects_rv`
already covers it.

**3. `ObjectWritesSubscribe` returns the starting cursor.**

```go
ObjectWritesSubscribe(ctx) (*ObjectWritesSubscription, int64, error)
```

The waker has no way to read the cursor today, and this is the change that fixes it —
not an extra one. `currentResourceVersion` is unexported, takes an internal `dbtx`, and
needs `s.conn(ctx)`; nothing on the `Store` interface exposes the cursor, and
`Subscription` cannot carry it, since `NewSubscription` is just `(ch, close)`
(`internal/storeapi/storeapi.go:110-112`). The per-kind watch paths only have a high-water
mark because `snapshotAt` returns one (`sqlite/watch.go:448`), and
`ObjectWritesSubscribe` deliberately takes no snapshot.

Returning it **makes the initialization order unrepresentable** rather than documented.
There is no second call to misorder, so the "read the sequence too early and silently
under-deliver" hazard cannot be written. That is the store's own precedent: `snapshotAt`'s
comment argues this exact case for the per-kind streams — *"a separate cursor read could
span a write the list itself didn't, dropping a real event or replaying a snapshotted
one."* Same reasoning, same fix.

Implementation is the mirror of `snapshotAt`'s ordering: register the hub receiver
(`sqlite/watch.go:381`) **before** reading the cursor, so any write landing in between is
either already in the receiver or above the returned value. That over-delivers at worst,
which "coalescing is not loss" covers.

The alternative — an explicit `ResourceVersionCurrent(ctx)` on `Store` — also works, but
then the ordering becomes a rule callers can break, and rule 1 has to grow the paragraph
back. Prefer the signature.

## Three properties make this sound

Assert all three rather than assuming them.

- **`resource_version` is monotonic in commit order.** The whole cursor argument rests on
  this, and it holds only because the store is single-connection: rv is drawn inside the
  write transaction (`nextResourceVersion`, `sqlite/store.go:221-227`). With a pool of 2,
  a transaction could draw rv=5 and commit after one that drew rv=6, and the watermark
  would skip a real change. That is one line of config away from being false and nothing
  today would catch it — **this is the property most worth a test.**
- **A missing row means no dependent remains.** `edges.to_id` is `ON DELETE RESTRICT`
  (`0001_init.sql:156`), so a target cannot be physically removed while anything still
  depends on it. It does *not* guarantee the replayed row still exists — the dependent
  may have been deleted first, cascading its own edge away (`from_id` is
  `ON DELETE CASCADE`, `0001_init.sql:151`) and freeing the target. The conclusion holds,
  but state it as the property you actually get: a row that vanished had no dependents
  left to strand.
- **Coalescing is not loss.** The hub delivers the latest state per object, and the waker
  is level-triggered — it re-reads current state. Replaying "object X changed" once is
  equivalent to replaying it five times.

## The escalation machinery is deleted, not kept

The waker is the **only** non-test caller of it. Once it stops arming the flags, all of
the following is dead and should go in the same change:

- `Beehive.resyncKindsNextTick` / `resyncKindsEveryTick` (`beehive.go:98-124`)
- `Beehive.hasPeriodicPass` and `reconciler.hasPeriodicPass`
- `reconciler.resyncOnce` / `resyncAlways`, `resyncNextTick` / `resyncEveryTick`,
  `tickResyncs`
- the self-re-arm at `reconciler.go:568`, which only matters if something arms the
  one-shot in the first place

All unexported, so this is internal cleanup with no API consequence. Stating it here
because leaving the call to the implementer produces a half-migrated subsystem: flags
that are still set, still read, and no longer load-bearing. The catchup and resync ticks
themselves stay — they are independent features; it is only the *escalation* on top of
them that this replaces.

## Test plan

- **A dropped subscription is repaired with no periodic tick configured.** Run with
  `WithCatchupInterval(0)`, `WithResyncInterval(0)`, `WithStartupResync(false)`,
  `withoutGCSweeper()`. Fail the subscription, change a target, and assert its settled
  dependent still reconciles. *This is the whole gap: today it fails, and the only signal
  is a log line. Note it needs the resubscribe loop — it cannot pass on the watermark
  alone.*
- **`resource_version` is monotonic in commit order.** Pin the single-connection
  assumption the cursor depends on, so raising the pool size fails here rather than
  silently skipping changes in production.
- **A failed dependents lookup is repaired without a full pass.** Assert the repair
  enqueues the dependent and **not** every object of the kind.
- **The watermark does not advance past an unprocessed batch.** Fail the lookup for one
  batch, let the next succeed, and assert the failed batch's targets are still replayed.
  *This is the ordering mistake that makes recovery silently useless while still passing
  every "it recovers" test.*
- **A batch with no wakeable changes still advances the watermark.** Feed a
  delete-only batch, then assert a later replay does not re-read from before it.
  *Without this, "replay is bounded" degrades to O(table) on a delete-heavy store.*
- **Replay is bounded, and pages.** With N changes missed and M objects in the store,
  assert the recovery reads N rows, not M, and that it issues more than one page when N
  exceeds the page size.
- **A target deleted during the outage does not break the replay.** Assert the recovery
  completes rather than erroring on a missing row.
- **Coalesced changes replay once.** Change one target repeatedly during the outage and
  assert the dependent reconciles. Don't assert a per-change count.
- **Retry backs off to a ceiling and keeps trying.** Drive several consecutive failures
  through the seam and assert the interval stops growing at the ceiling — and that the
  loop is still retrying after it. *An attempt cap would pass a naive "it backs off" test
  while resurrecting the dead waker this spec exists to kill.*
- **The escalation is gone.** The `beehive.go:334-335` message variant that tells the
  operator to drive wakes by hand should no longer exist; assert it does not appear on a
  subscribe failure. *If it still does, the repair still depends on a tick.*
- **No test waits on a real backoff interval.** Every test above drives the retry through
  the seam. *Stated as a test-plan constraint because the alternative passes locally and
  flakes on CI.*

## Alternatives considered

**Make the existing escalation independent of the tick.** When `resyncKindsNextTick`
fires and `hasPeriodicPass()` is false, run the pass immediately instead of setting a
flag. Smallest possible diff, preserves today's semantics exactly. It loses because a
dropped subscription then triggers an immediate full-table pass for every kind — the cost
profile `TODO.md:101-108` rejected a stride throttle for trying to soften. Keep it in
reach as the fallback if the `ObjectWrite` change is unwanted.

**Narrow the escalated pass to dependents.** `SELECT DISTINCT from_id FROM edges WHERE
relation='depends_on'`, joined to the kind. The escalation exists only to reach stale
*dependents*, and an object with no outgoing `depends_on` edge cannot be one — so the pass
never needed to cover the whole table. `TODO.md:112-117` records this as the "narrow fix"
and notes it wants a partial index on the dependents side. It loses because it makes the
repair cheaper without making it *correct*: the pass still has to be driven by a tick that
may not exist. It composes with the option above rather than standing alone, and is the
natural second step if that fallback is taken.
