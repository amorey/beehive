# Specs

A spec is a plan for work that is not done yet. An
[ADR](../adr/README.md) records a decision that is already in the code.
When a spec lands, write the ADR and delete or mark the spec.

Each spec is one landing. **A removal is always its own commit**, so a
bisect can tell a new path's bug from a cleanup's regression.

A spec that lands in part is marked in place: the section that shipped
names its commit and its ADR, and keeps its reasoning. Delete a spec only
when all of it has landed.

## The schedule watch, done separately — landed

The schedule watch converted to push in `4c8f607`, and **its poll is
gone**. It was standalone: it depended on no other spec, and it was
implemented on its own branch. Its spec is deleted, as a landed spec is;
the decision lives in the
[schedule-watch ADR](../adr/2026-07-27-schedule-watch.md), under
"Superseded: the poll".

It was carved out because it answers the central question — what backs a
push path up when a notify is lost — differently from everything else. Its
value is the work queue's memory, so no writer exists that its hub cannot
see, and it needs no backstop at all. Nothing else in beehive has that
property. **Do not cite it as precedent for removing another poll.**

The push conversion below assumes it: `gobus` is a direct dependency, a
per-subscriber receiver feeding a public channel is running, and the
teardown ordering has a working answer. Note that it runs on `gobus/watch`,
not `conflate` — so it proves the module and the shared receive-side
machinery, and **not** `conflate`'s coalescing.

## Upstream

Two changes beehive asked `github.com/amorey/gobus` for have landed, and
neither has a spec here any more:

- **The `Send` fast path** (`v0.2.1`). `Send` returns at a lock-free idle
  check when no receiver is live, so an unwatched hub costs one atomic load
  per publish. It removed the need for a subscriber count in the schedule
  watch.
- **The `watch` package** (`v0.3.0`, plus `Peek` in `v0.4.0`). A keyed
  latest-value **state** bus: one slot per watched key, seeded at
  registration with the value the caller just read, with a caller-supplied
  `Accept` deciding which of two values wins. `Peek` reads a receiver's slot
  without taking the value, which is how a test asserts that `Accept`
  rejected something. The schedule watch is built on it.

The correspondence is in `docs/gobus-*.md`, including one request we
withdrew — a per-key watermark in `conflate` — because it would have grown
without bound for an unbounded key space. That withdrawal is what led to
`watch`, where the key set is bounded by declaration.

**`conflate` and `watch` are not interchangeable.** `conflate` is a keyed
*event* bus with coalescing and annihilation, which is what a change stream
needs; `watch` is a keyed *state* bus, which is what a gauge needs. The
object-watch deltas in [watch-push](watch-push.md) want the first. The
schedule gauge wants the second.

## Push conversion

Add commit-time push beside the store-backed polls.
[push-conversion](push-conversion.md) is the umbrella: it holds the
backstop pattern, the loud-failure policy and the messaging backend that
every child inherits. Read it first.

| # | Spec | What it does | State |
| --- | --- | --- | --- |
| 2a | [new-edge-push](new-edge-push.md) | Enqueues an edge's source at commit, so a fresh declare stops waiting for the owed pass. No hub. | **Landed**; see the [ADR](../adr/2026-07-31-a-spec-write-enqueues-its-own-object.md) |
| 1 | [events-push](events-push.md) | Pushes the event log beside its poll. The pilot for push over a store write. | Poll gate landed; push half to do |
| 2 | [wake-push](wake-push.md) | Adds the wake hub and the dependent wakes. | Self-enqueue landed; hub to do |
| 3 | — | Was `store-reads`, which grouped two `Store` reads behind one break. Dissolved: each read now lands with the push path that uses it. | Dissolved |
| 4 | [watch-push](watch-push.md) | Pushes the object-watch deltas. **Has an open decision at the top**: whether the poll stays as the backstop, or goes in favour of the stale-watch pass, `ObjectsGetSummary` and `Lagged`. | Blocked on that decision |
| 5 | — | Lengthens the backstop intervals. No spec; land it last and alone. | To do |

Order: **2a → 1 → 2 → 4 → 5**, and 2a has landed. The numbers are the phase
numbers the umbrella spec uses, and they do not change; the order does.
**Spec 1 is next.**

Spec 2a went first because it was the smallest landing in the plan and
because it closed one of the two holds on spec 5's owed-pass number. It
built no hub and added no goroutine, so it proved none of the push
machinery. Spec 1 is still the pilot for that.

**Spec 4's open decision is the only one left that changes the plan's
shape.** It decides whether the stale-watch pass, `ObjectsGetSummary` and
the `Lagged` contract change are built at all — and so whether this plan
breaks `Store` even once, since `EventsMaxVersion` has landed and no other
phase needs a new member. The umbrella spec carries it in its backstop
table and its loud-failure policy; that spec holds the argument. Nothing
before phase 4 is held by it.

**A `Store` read lands with the push path that needs it.** Spec 3 existed
to group the reads behind one break, on the rule that an interface break is
paid once per break rather than once per method. That rule protects
external backends, and there are none before the first release — the same
reasoning the
[amend-in-place ADR](../adr/2026-07-31-amend-the-schema-in-place-until-release.md)
applies to the schema, expiring at the same moment. Restore the grouping
after the release.

**Three pieces have landed ahead of their spec**, each because it was
severable and blocked on nothing: the self-enqueue (`60b4ea4`),
`EventsMaxVersion` (`1613685`) and the `EventsWatch` poll gate (`1613685`,
`56af29d`). None of them is a push path, so every spec above still holds
its own core.

**Spec 5 was gated on spec 2's self-enqueue, and that gate is open.** Until
a spec write enqueued its own object, the owed pass was the primary trigger
for one rather than a backstop for it, and lengthening it would have made
the commonest latency in the system ten times worse. `60b4ea4` closed that.
Two smaller holds remained on the owed-pass number, and spec 2a closed the
first. **One hold is left**: a write from outside this process, which no
push path can cover. [push-conversion](push-conversion.md) has it under
"Phases".

**No poll is removed by this plan.** Every driver it touches keeps running,
and push is added beside it. A poll scans the store, so it sees writes from
any source; a hub sees only writes that pass through this process. So a
poll covers more than its hub, not less. That reasoning is written out in
[wake-push](wake-push.md), "Why the waker stays". The schedule watch was
the one poll in beehive that was removable, and it is already removed —
outside this plan, in `4c8f607`.

Spec 1 is the pilot. Read its "what this de-risks, and what it does not"
table before treating a green phase 1 as assurance for phase 4. Two rows of
that table are answered by the schedule watch's shipped code — the ones
about receivers, channels and teardown, which `gobus/watch` and `conflate`
share. `conflate`'s own coalescing is not among them, and the pilot still
proves the transaction boundary.

The GC sweeper is the one poll with no push spec at all. Pushing it means
re-architecting the cascade, which advances one step per sweep, rather than
adding a notify. Phase 5 leaves its interval alone for that reason.
