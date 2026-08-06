# Spec: the event retention audit

- **Status:** Draft — nothing implemented. Two decisions need sign-off before
  any code: D0 (what the log is for) and D5 (what a sweep is allowed to cost).
  D3 depends on D5 and cannot be taken on its own.
- **Date:** 2026-08-06
- **Closes:** the `docs/TODO.md` entry *"Event retention has never been audited
  end to end, and its shape is not derivable from the code"*, and decides the
  entry after it, *"Dropping the per-timeline cap in favour of `maxAge` alone"*.
- **Deliverable:** one ADR, one public API rename, a cheaper candidate
  selection in `EventsSweep`, a defaults decision backed by a benchmark, and
  the docs that make the shape derivable. No schema change under the
  recommended branch.

## 1. Why this exists

`WithEventRetention(perObject, maxAge)` is public API, and the partition its
count bound uses is a primary key on disk (`events_horizon`). Both are cheap to
change today — the schema is still amended in place
([ADR](../adr/2026-07-31-amend-the-schema-in-place-until-release.md)) and
nothing has been released — and expensive to change later. The audit is
therefore pre-release work, not cleanup.

The concrete complaint is that the option's shape is not derivable from its
name or its godoc: `perObject` is not per object, it does not count events, and
neither bound is on, so the default configuration is an unbounded log.

**In scope:** the `WithEventRetention` surface, `Store.EventsSweep` semantics
and cost, the `events_horizon` key, the defaults, and the documentation of all
four.

**Out of scope**, each already its own `docs/TODO.md` entry: a kind-wide event
watch, `EventsAddInput`, and the missing public aliases on `Store`. Nothing
here changes the event watch's cursor protocol.

## 2. What is true today

Audited end to end, source of truth for everything below.

| Mechanism | Where | Behaviour |
| --- | --- | --- |
| The option | `options.go:368` | Sets two `Beehive` fields (`beehive.go:102`). Ignored on any other target. |
| The sweep | `beehive.go:238` | No-op unless a bound is positive. Runs from `gcSweeperRun` (`beehive.go:225`) at `gcInterval`, default 30s, which cannot be disabled. A failure is logged and retried next tick. |
| The count bound | `sqlite/store.go:1850` | `ROW_NUMBER() OVER (PARTITION BY object_id, category ORDER BY last_at DESC, id DESC)`, delete `rn > perObject`. Counts **runs**, per **timeline**. No `WHERE` — it ranks the whole table (F9). |
| The age bound | `sqlite/store.go:1866` | `last_at < now-maxAge`, evaluated over the whole `events` table, unpartitioned. `now` is `time.Now()` inside the store. |
| Atomicity | `sqlite/store.go:1849` | Both bounds run in one `Within`, so they see one snapshot and land together — and hold the single write connection for the whole sweep. |
| The horizon | `sqlite/store.go:1884` | Per `(object_id, category)`, raised to `MAX(resource_version)` of what the same predicate matches, written **before** the delete in the same transaction. `EventsSweep` is its only writer. |
| The horizon's readers | `sqlite/store.go:1791`, `:1820` | `eventPage` carries it as a trailing column; `eventHorizon` reads it alone for an empty page. A `nil` category takes `MAX` across the object's timelines, which matches what a `nil` category *reads* (`storeapi.go:148`: every category interleaved). |
| The refusal | `eventswatch.go:284` → `objectswatch.go:230` | `cursor < trimmed_through` ends the stream with `ErrWatchTooOld`. Equality has lost nothing. |
| Cascade | `0001_init.sql:206`, `:259` | Both `events` and `events_horizon` are `ON DELETE CASCADE` on `objects(id)`, so a collected object's log and its horizon go together — which is what lets a stream end with `ErrNotFound` instead of an empty read. |
| What a run is | `sqlite/store.go:1646` | `EventsAdd` extends the newest run of `(object, category)` when `(type, reason)` matches, bumping `count` and re-sampling `message`/`detail`; otherwise it inserts. `count` has no bound. |

## 3. Findings

**F1 — the name is wrong on two axes.** `perObject` is per `(object,
category)`, and it counts runs, not events. Both are stated correctly in the
godoc (`options.go:368`) and the README (`README.md:617`), and in neither case
does the name carry them. A reader who does not already know the category rule
reads the parameter as exactly what it says.

**F2 — runs is the right unit for a storage bound, and the wrong unit for a
reader's expectation.** Rows are what cost disk, and `count` growing inside one
run costs nothing, so bounding runs is correct. But `EventsList`'s limit also
counts runs, and a caller who sets `perObject = 50` expecting 50 events can be
handed 50 runs carrying a million occurrences. This is a documentation
obligation, not a mechanism change.

**F3 — the default is an unbounded log.** Both bounds are off, so
`eventRetentionSweep` returns immediately (`beehive.go:239`) and nothing ever
trims. `WithWriteLogRetention` defaults the other way (24h, `beehive.go:42`),
and `README.md:615` explains the asymmetry by write rate: a log entry lands on
every object write, an event only when a controller writes one. That argument
justifies a *different* default, not the absence of one — a controller that
emits a distinct `(type, reason)` per reconcile appends a run per reconcile,
which is the same rate.

**F4 — `maxAge` is unpartitioned, and never expires an open run.** The
predicate is `last_at`, so a run that keeps being extended is immortal no
matter how old its `first_at` is, and one that stopped being extended takes its
whole history with it at the cutoff. It is a recency window over the table, and
the "a chatty timeline must not evict a quiet one" argument that shapes the
ring does not apply to it — nor need it, since time does not partition. Worth
stating explicitly because the two bounds sit behind one option and read as two
settings of the same kind.

**F5 — the horizon is sound, and over-reports in exactly one way.** Every
deleted `resource_version` is `≤` the horizon written for its timeline, in the
same transaction, before the delete — so a hole can never be served silently.

The over-report is a reader whose query filters on `Type`, `Reason` or `Since`
but **not** `Category`. Those three filter client-side in `eventReader.step`
(`eventswatch.go:292`, via `q.Matches`), while the horizon is scoped only by
category (`sqlite/store.go:1791`), so such a reader is refused for a trim of
runs it would have discarded on arrival. It is the safe direction and it needs
no code; it needs writing down.

A `nil` category is **not** an over-report, and an earlier draft of this spec
had it backwards. `EventQuery.Category == nil` means "every category
interleaved" (`storeapi.go:148`), and `step` passes that same `nil` down, so a
nil-category reader is reading every timeline and a trim anywhere is a real
loss. `MAX` across timelines is exactly right.
`TestEventsWatchHorizonIsPerTimeline` (`eventswatch_test.go:208`) already pins
this, ending on `"an unfiltered resume is refused for a trim anywhere"`.

A second, narrower skew: the ring orders by `last_at DESC, id DESC` while
cursors order by `resource_version`, so same-millisecond extends can leave a
survivor's version below a trimmed one. Also the safe direction, also
documentation.

**F6 — the category partition in retention is inherited, not chosen.**
`category` exists to keep run aggregation from comparing across timelines
(`0001_init.sql:196`). The ring adopted it, and `events_horizon`'s primary key
then adopted it from the ring, because a horizon has to describe what the trim
actually deleted. So the partition is now load-bearing for the watch contract.
It should be re-affirmed deliberately rather than left as an inheritance.

**F7 — enforcement granularity is the GC interval.** Retention is a sweep, not
an invariant: a burst can exceed the cap for up to `gcInterval` (30s default).
Nothing depends on the cap being tight — the horizon only rises, and readers
tolerate a late trim — but "cap N" reads as a hard bound and is not one.

**F8 — the cutoff clock is `time.Now()` inside the store.** No injected clock
exists on `Beehive`, and the existing tests get determinism by writing old
`last_at` values rather than by moving the clock. Recorded so the next reader
does not re-derive it; no change proposed.

**F9 — the ring trim full-scans `events` twice per sweep, and nothing bounds
the delete.** The ranking subquery has no `WHERE`, so it ranks every row in the
table; `trimEvents` then evaluates that same predicate twice, once for the
horizon `INSERT…SELECT` and once for the `DELETE` (`sqlite/store.go:1886`,
`:1895`). Confirmed plan, both statements:

```
SCAN events USING COVERING INDEX idx_events_object_cat
USE TEMP B-TREE FOR GROUP BY
```

Both run inside one `Within`, so the scan holds the single write connection
away from every writer for its duration. Nobody pays this today because the
default is off (F3) — which is precisely why F9 and D3 cannot be decided
separately.

The same problem is solved 800 lines away for the write log.
`ObjectWritesSweep` (`sqlite/store.go:2698`) chooses "one statement per kind,
not one subquery per row… a seek for the cutoff, a range delete below it", and
`writeLogKinds`' comment says why the axis works: "kinds number in the handful
where entries number in the millions". **That does not transfer.** Event
partitions are `(object, category)` — at least one per object — so iterating
partitions the way the write log iterates kinds replaces one scan with a
million seeks. The event ring needs its own answer, which is D5.

## 4. Decisions

### D0 — What is the event log for? *(gate: needed before any code)*

**Recommendation: a bounded ring per timeline, plus an optional recency window.
Keep both bounds.**

The ring is the only bound that holds under a flapping controller. `maxAge`
bounds age, not size: inside the window the log grows with reconcile rate,
where the ring keeps it proportional to the object count. That is the failure
mode an event log meets first.

Accepting this **closes the `docs/TODO.md` entry on dropping the per-timeline
cap as "considered, not taken"**, with this spec's reasoning folded into the
ADR.

*Branch B (events are a recent-activity window, cap dropped)* is a genuinely
smaller system and is specified in §6 so it can be chosen without re-deriving
it. Choose it only on the purpose argument, never as a simplification: what it
removes is mostly the horizon's complexity, and the horizon is what stops a
resume implying an absence it cannot vouch for. Note that branch B also
dissolves F9 and D5, since the age bound is a single indexed predicate.

**D0 gates every mechanism phase, including the rename.** Under branch B the
count parameter is deleted rather than renamed, so Phase 2 would be wasted
work.

### D1 — Does the category partition stay in retention?

**Recommendation: yes, unchanged.** It is what stops a chatty timeline evicting
a quiet one on the same object, and `events_horizon`'s key already matches it,
so a resume is refused per timeline rather than per object. No schema change.

### D2 — Rename `perObject`.

**Recommendation: `perTimeline`, everywhere.** It names the partition the way
`WithWriteLogRetention(perKind, …)` names its own, and the godoc then only has
to add "runs, not occurrences" rather than correct the name. Breaking public
API, and free today.

### D5 — What is a retention sweep allowed to cost? *(gate: needed before D3)*

F9 is the finding this spec previously missed, and D3 rests on it.

**Recommendation: narrow the candidate set before ranking, then scope both
statements per timeline.** Two steps:

1. Select over-cap timelines with an index-only aggregate:
   `SELECT object_id, category FROM events GROUP BY object_id, category HAVING
   COUNT(*) > ?`. `idx_events_object_cat` leads on exactly those two columns,
   so this is expected to ride the index in order with no temp B-tree and no
   table fetch. **The implementer must confirm the plan** and pin it —
   `TestEventsMaxVersionUsesCoveringIndex` is the precedent for a
   plan-asserting test.
2. For each over-cap timeline, call the existing `trimEvents` with a scoped
   predicate — `object_id = ? AND category = ? AND id IN (SELECT id FROM events
   WHERE object_id = ? AND category = ? ORDER BY last_at DESC, id DESC LIMIT -1
   OFFSET ?)`. `trimEvents` already takes a `where` plus args and needs no
   change. Its double evaluation then costs two seeks per over-cap timeline
   instead of two full scans of the table.

Cost becomes one index-only pass plus work proportional to what is actually
over cap, which is the property the write-log trim has and this one does not.

**Also required: a per-sweep candidate budget.** Iterating an unbounded
candidate list inside one `Within` reintroduces the connection-hold F9
describes, just with seeks. Bound it (a constant, in the low hundreds), and
document that retention is progressive across sweeps — sound because the
horizon only ever rises and F7 already says the cap is not tight.

*Held in reserve, if the benchmark says the index-only pass is still too much:*
a per-timeline run counter maintained by `EventsAdd` (a new run increments, an
extend does not), which turns candidate selection into a seek on `count > cap`.
It costs a table, a write-path `UPDATE` in the hot event path, and a drift
risk. Do not build it without a number demanding it.

*Alternative to all of the above:* take D3's "both off" branch and leave the
sweep as it is. That is a legitimate outcome — it is what makes the current
implementation correct today.

### D3 — Should either bound default on? *(depends on D5)*

**Recommendation: the ring on at `defaultEventRetentionPerTimeline = 50`, the
age bound off — and only if D5's rework lands with a benchmark showing the
sweep's cost is proportional to what is over cap rather than to table size.**

A default of 50 runs per timeline keeps a stock beehive bounded by object count,
leaves the age bound as the opt-in that expresses "recent activity only", and
matches the write log in shape (one bound on, one off).

Consequences to accept with it:

- Retention becomes reachable on stock defaults, so `ErrWatchTooOld` can now
  end a stream that no one configured for it. Not a new class of failure — an
  unbuffered stream whose consumer stalls already reaches it — but newly
  reachable without opting in, and the README must say so.
- Every stock beehive runs a sweep every 30s forever. That is the cost D5
  exists to bound and the benchmark exists to price.

*Alternative:* keep both off and document the choice as deliberate. This is the
right answer if D5's rework does not land or its benchmark disappoints —
in which case the audit still closes, because F1–F9 are documented and the
option's shape becomes derivable. **The TODO entry closes on documentation, not
on a default.**

### D4 — The sweep clock.

**No change.** `time.Now()` stays in `EventsSweep`; recorded here so F8 is not
re-litigated.

## 5. Work plan — recommended branch (D0 = ring + window)

No schema change. No change to the watch protocol, the horizon's key, or
`EventsListSince`'s signature.

### Phase 1 — the ADR, first

1. New `docs/adr/2026-08-06-event-retention-is-a-ring-per-timeline.md`, in the
   house format. It must carry: what the log is for (D0); why the count bound
   partitions by category and why the horizon key must match it (D1, F6); why
   the unit is a run (F2); how a sweep selects candidates and why the write
   log's per-kind shape does not transfer (D5, F9); why the ring defaults on
   and the window does not, if D3 lands (D3); that the horizon over-reports for
   a non-category client-side filter and that this is the safe direction (F5);
   and that enforcement is a sweep rather than an invariant (F7). Fold in the
   "dropping the cap" trade from `docs/TODO.md` as the alternative considered.
2. `docs/adr/README.md` — index entry.
3. `docs/adr/2026-07-27-events-api.md:56` — point retention at the new ADR
   instead of restating it.

### Phase 2 — rename (D2)

4. `options.go:374` — `WithEventRetention(perTimeline int, maxAge
   time.Duration)`. Rewrite the godoc to state, in order: the unit is a **run**,
   the partition is `(object, category)`, `maxAge` is a flat cutoff on a run's
   **end**, a zero bound is skipped, and the sweep is the GC sweeper so the cap
   is enforced on its interval (F7).
5. `beehive.go:102` — `eventRetentionPerObject` → `eventRetentionPerTimeline`;
   update `:239` and `:242`.
6. `internal/storeapi/storeapi.go:456` — `EventsSweep(ctx, perTimeline int,
   maxAge time.Duration)`, and its doc comment gains the run/timeline wording.
7. `sqlite/store.go:1846` — parameter rename.

The two `Store` doubles that implement `EventsSweep` — `fakeStore`
(`testutils_test.go:463`) and `eventErrStore` (`client_test.go:2484`) — declare
unnamed parameters, so **neither needs an edit**. Listed so the next reader does
not go looking.

This phase touches `Store`, so it is a break an external backend would pay for.
Two other `docs/TODO.md` entries (`EventsAddInput`, the four missing public
aliases) are waiting to ride along with the next `Store` break, and both are in
the event family. **Take them in the same PR if the team wants them; otherwise
note in the PR that this break was the third opportunity passed on.** Neither
is a prerequisite.

### Phase 3 — the sweep's cost (D5)

8. `sqlite/store.go:1850` — replace the whole-table window function with the
   candidate selection of D5, calling `trimEvents` once per over-cap timeline
   with a scoped predicate. `trimEvents` (`:1884`) is unchanged.
9. Add the per-sweep candidate budget as a package constant beside the sweep,
   with a one-line comment naming what it bounds (the connection hold), not
   what it does.
10. Benchmark first, then change, then benchmark again — see §7.

### Phase 4 — defaults (D3), only if Phase 3's numbers support it

11. `beehive.go:38` block — add `defaultEventRetentionPerTimeline = 50` beside
    `defaultWriteLogMaxAge`, and set it in the constructor at `beehive.go:392`.
12. The no-op guard at `beehive.go:239` is unchanged in shape: an explicit
    `WithEventRetention(0, 0)` must still mean "unbounded", so the default lives
    in the constructor and nowhere else.

### Phase 5 — docs and TODO

13. `README.md:617` — rewrite the `WithEventRetention` paragraph for the new
    signature and, if Phase 4 landed, the new default. Keep the sibling framing
    against `WithWriteLogRetention` at `:615`, whose "defaults the other way"
    sentence is then wrong and must be re-cut.
14. `README.md:611` — if Phase 4 landed, one sentence where
    `WithEventsResumeFrom` is described noting that retention is on by default,
    so `ErrWatchTooOld` is reachable without configuring anything.
15. `README.md:710` — the signature line in the API listing.
16. `CLAUDE.md` — the events bullet gains the retention shape in one clause and
    the ADR link; do not restate the argument there.
17. `docs/TODO.md` — delete the audit entry and the "dropping the cap" entry,
    both settled. If Phase 2 did not take the ride-along breaks, add one
    sentence to the `EventsAddInput` entry recording that this was the third
    passed opportunity, matching the note it already carries about
    `EventsMaxVersion`.
18. `docs/specs/` — delete this file and its index entry.

## 6. Work plan — branch B (D0 = recent-activity window)

Recorded so the branch is choosable, not to be built alongside. If D0 goes this
way, Phases 1 and 5 stand with different content, D5 and F9 dissolve, and the
mechanism work is:

- `sqlite/store.go:1850` — delete the window-function trim. `EventsSweep` loses
  its count parameter, and so do `Store`, the `Beehive` field and the option
  (now one duration). The two test doubles still need no edit (unnamed params),
  but their signatures change arity, so both **do** get touched here.
- `0001_init.sql:259` — `events_horizon` loses `category` from its primary key
  and becomes one row per object. Amend in place;
  `TestTheSchemaIsOneMigration` is the tripwire that keeps it one file.
- `sqlite/store.go:1791`, `:1820` — the horizon reads lose their category
  predicate, and `EventsListSince` (`internal/storeapi/storeapi.go:436`) loses
  its `category` parameter, which exists only to scope the horizon. The page
  itself is already unfiltered — `eventswatch.go:292` filters client-side via
  `q.Matches` — so the reader is unaffected.
- `eventswatch_test.go:208` — `TestEventsWatchHorizonIsPerTimeline` asserts the
  behaviour branch B removes, and must be replaced rather than adjusted.
- `docs/adr/2026-07-31-amend-the-schema-in-place-until-release.md:16` — states
  that "`EventsSweep` ranks within `(object_id, category)`" as part of its
  index argument. Stale under branch B, and easy to miss.
- The double predicate scan the sweep pays halves, since only one predicate
  survives.

What it gives up is stated in F3 and D0: no bound survives a controller that
emits a distinct `(type, reason)` per reconcile. Do not choose it without an
answer to that.

## 7. Tests and benchmarks

Two packages, two conventions. Store-level tests are `package sqlite`
(`sqlite/store_test.go:15`); everything at the `Beehive`/`Client` level is
whitebox `package beehive`. Both use `require` for preconditions and `assert`
for independent checks.

**What already exists** (rename with the parameter, do not rewrite):
`TestSweepEventsCapN` (`sqlite/store_test.go:280`), `TestSweepEventsMaxAge`
(`:337`), `TestSweepEventsExecErrors` (`:899`),
`TestSweepEventsRecordsHorizonPerTimeline` (`:497`),
`TestSweepEventsHorizonOnlyRises` (`:520`), `TestSweepEventRetention`
(`beehive_test.go:32`), `TestEventsWatchHorizonIsPerTimeline`
(`eventswatch_test.go:208`).

**New:**

- `sqlite/store_test.go` — `TestEventsSweepCountsRunsNotOccurrences`: one
  timeline, one `(type, reason)` extended past the cap; nothing is trimmed and
  `count` is unbounded. Pins F2 against a future change that decides to count
  occurrences.
- `sqlite/store_test.go` — `TestEventsSweepAgeBoundSpansCategories`: the age
  bound is table-wide where the ring is per timeline. Pins F4.
- `sqlite/store_test.go` — `TestEventsSweepHorizonCoversEveryTrimmedRun`: after
  a mixed trim, every deleted version is `≤` its timeline's `trimmed_through`.
  Additive to `:497`/`:520`, which pin the partition and the monotonicity but
  not the covering property itself.
- `sqlite/store_test.go` (Phase 3) — `TestEventsSweepSelectsCandidatesByIndex`:
  a plan assertion on the candidate query, modelled on
  `TestEventsMaxVersionUsesCoveringIndex`. This is the test that makes D5's
  premise falsifiable.
- `sqlite/store_test.go` (Phase 3) — `TestEventsSweepIsProgressiveUnderBudget`:
  more over-cap timelines than the budget; one sweep trims some, a second
  finishes, and the horizon never falls.
- `eventswatch_test.go` — `TestEventsWatchHorizonIgnoresClientSideFilters`: a
  reader filtering on `Reason` but not `Category` is refused for a trim of runs
  it would have dropped. Pins the F5 over-report as intended, so a later "fix"
  has to argue with a test.
- `beehive_test.go` (Phase 4 only) — extend `TestSweepEventRetention` with a
  case proving the new default trims with no option set, and that an explicit
  `WithEventRetention(0, 0)` still leaves the log unbounded.
- `options_test.go:66` — `TestWithEventRetentionDispatch` follows the field
  rename.

**Benchmark** — `sqlite/store_bench_test.go`, the first bench file in that
package (the convention is to mirror the source file, which this does):
`BenchmarkEventsSweep`, on disk, over a store with a large event count and a
varying fraction of timelines over cap. The baseline is today's whole-table
ranking; the target is a curve that tracks the over-cap fraction rather than
the table size. **This benchmark is the evidence D3 is gated on**, and its
numbers belong in the ADR and the PR summary, not only in a terminal.

## 8. Acceptance

- [ ] D0 and D5 signed off, and the chosen branch's ADR (Phase 1) merged before
      any mechanism change.
- [ ] `WithEventRetention`'s godoc states unit, partition, cutoff semantics and
      enforcement granularity; no reader has to open `EventsSweep` to learn the
      shape. **This is the close condition for the TODO entry** — D3 landing or
      not does not change it.
- [ ] `BenchmarkEventsSweep` exists, with before/after numbers recorded in the
      ADR, if Phase 3 ran.
- [ ] D3 landed only with those numbers behind it, or was explicitly declined
      in the ADR.
- [ ] Both `docs/TODO.md` retention entries deleted, and this spec deleted.
- [ ] `go build ./... && go vet ./... && staticcheck -checks=all ./... && go test ./...`
      green, plus `go test -run '^$' -bench . -benchtime 1x ./...`.
- [ ] `TestTheSchemaIsOneMigration` still passes if branch B touched the schema.
- [ ] PR follows `.github/pull_request_template.md`, titled with ✨ for the
      recommended branch or 🐋 for branch B.

## 9. Risks

- **The sweep's cost is the main risk of this change, not the API break.**
  Today's ring trim scans the whole table twice per sweep while holding the
  single write connection (F9). Turning it on by default without D5 is a
  straight performance regression on stock defaults; landing D5 wrong turns it
  into a regression that only appears at scale, which is worse. The benchmark
  is the control, and the candidate-selection plan test is the tripwire.
- **The default (D3) deletes data nobody asked to delete**: an unbounded log
  becomes a 50-run ring per timeline. Pre-release, so no migration is owed, but
  it is the one item here with that property. Call it out in the PR summary,
  not only in the README.
- **The `Store` break is the third one this family has taken.** If the
  ride-along entries are skipped again, say so in the PR so the next reader
  sees a decision rather than an oversight.
- **Branch B is one-way**: `events_horizon`'s key is on disk, and the argument
  for putting the category back after a release is much harder than the
  argument for keeping it now.
