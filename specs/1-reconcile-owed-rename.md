# Rename `pending_wake` to `reconcile_owed`

**Status: built** — `7bbd6f0`, `895d827`, `0928321`, `9c0d7dd`, in the four cycles the
test plan implies. Independent — nothing else in `specs/` depends on it, and nothing
here changes behaviour.

Two names landed differently from the table below, both in tests where the mechanical
substitution produced no English: `TestReconcileDrainsMultiplePendingWakes` became
`TestReconcileDrainsMultipleOwedPasses` and
`TestReconcilePendingWakeSurvivesConcurrentWake` became
`TestReconcileOwedSurvivesConcurrentIncrement`.

---

## Why

`pending_wake` counts the passes beehive owes an object. It has exactly one
producer — `EdgesAdd`, on a declare that raced its target — and the name reads as a
boolean flag, which is precisely what it is not (§ "Why a count rather than a flag"
below).

The sharper problem is that "wake" now names two different things in the same
packages, and only one of them is this column:

- the **in-memory dependency waker** (`dependentsWake`, `wakeAfterCommit`, and the
  reconcile-scoped `pendingWakes` collector in `gc.go`) — a live requeue, no storage;
- **this counter** — a durable marker that a pass is owed, drained by the reconcile.

`gc.go`'s `pendingWakes` and `objects.pending_wake` are unrelated and differ only in
case. That is a collision a reader has to resolve by reading both, every time.
Anything that later adds a *second* durable "this object still owes something" marker
(a decode-quarantine stamp is the obvious candidate — quarantining is log-and-skip
today, with no durable marker) inherits the same trap: two markers whose names both
say "wake" is how they get folded back into one column. Renaming now makes the
distinction legible before that happens.

It also keeps a pure rename out of every later diff. That is the whole reason it goes
first: the change touches a column, an index, three store methods, one result field, a
`RawObject` field, several comment blocks and about a dozen test names, and mixed into
a behavioural diff it would bury the part worth reviewing.

## What changes

Nothing but names. Same shape, same producer, same arithmetic, same assertions.

**Schema** (`sqlite/migrations/0001_init.sql`, edited in place — that directory holds
one file and TODO.md records a fresh database as the only supported upgrade path):

```sql
-- was: pending_wake
    reconcile_owed INTEGER NOT NULL DEFAULT 0,

-- was: idx_objects_pending_wake
CREATE INDEX idx_objects_reconcile_owed
    ON objects("group", kind) WHERE reconcile_owed != 0;
```

The index's comment block already names the backstop query by a *stale* method name
(`ListPendingWakeIDs`, renamed to `WakesListPendingIDs` in #31); fix it to the new one
while here. The column's own comment carries a second stale name — "AddDependency
increments it", renamed to `DependenciesAdd` in the same PR — three lines above; fix
both.

One note on the in-place edit, which is otherwise routine (`0001` has been amended
six times; TODO.md records a fresh database as the only supported upgrade path). This
is the **first amendment to rename a column that live queries name**. Every earlier
one added a column or rekeyed an index, both invisible to an already-migrated
database. `sqlitemigrate` records only `(version, name, applied_at)` and no checksum
of the SQL, so an existing v1 database is not re-migrated and not refused — it opens
clean and then fails at *query* time with "no such column: reconcile_owed". That is
the accepted pre-release tradeoff, not a new one, but it is worth knowing the failure
is late and per-query rather than loud at startup.

**Store methods.** The family prefix moves too — the naming ADR
([noun-verb-naming](../docs/adr/2026-07-27-noun-verb-naming.md)) cites `Wakes*` by
name as an example of a family that is a column rather than a table, so leaving the
methods `Wakes*` over a column called `reconcile_owed` would strand that example:

| today | after |
|---|---|
| `WakesIncrement` | `ReconcileOwedIncrement` |
| `WakesDecrement` | `ReconcileOwedDecrement` |
| `WakesListPendingIDs` | `ReconcileOwedListIDs` |

`Pending` drops out of the list method the same way it dropped out of
`DeletionRequestsList`: the family name already says the row owes something, so the
qualifier only repeats it.

The prefix is **singular**, where the convention asks for a plural noun. This is
settled, not a reviewer's choice: the column is a scalar count, the prefix should be
spelled the same way as the column it names, and `ReconcilesOwed*` buys the rule at
the cost of every call site. The one plural that reads as an actual noun,
`OwedReconciles*`, inverts the column name and loses that correspondence. Because
this is a documented convention, the exception gets recorded where the convention
lives — a line in [noun-verb-naming](../docs/adr/2026-07-27-noun-verb-naming.md)
saying the family takes a singular prefix because it names a scalar count. An
undocumented exception is how the convention erodes.

**Do not** rename the dependency waker's
identifiers (`dependentsWake`, `wakeAfterCommit`, `pendingWakes`, `TestWakeDependents*`,
`TestDependencyWaker*`) — they are the other concept, and disentangling them is the
point.

`ReconcileOwedIncrement` **stays off the `storeapi.Store` interface**, exactly as it is
today, and the invariant it encodes survives the rename verbatim: "the declare-race
stamp rides `EdgesAdd`" is a compile-time property rather than something a test has to
police. The paragraph in `storeapi.go` that says so is unchanged apart from the names.

**Go, beyond the store methods:**

- `storeapi.RawObject.PendingWake` → `.ReconcileOwed`, keeping its "store-owned,
  store-assigned, ignored on the way in" doc comment.
- `storeapi.EdgesAddResult.WakeStamped` → `.ReconcileOwedStamped` (`sqlite/store.go`
  sets it; `controller.go` reads it).
- `reconciler.go`: `enqueuePendingWake` → `enqueueReconcileOwed`, its `enqueueFrom`
  label `"pending-wake"` → `"reconcile-owed"`, and the warn message on a failed
  decrement.
- `testutils_test.go`'s `fakeStore` and `listProbeStore` follow; `listProbeStore`'s
  `wakeListed` channel becomes `owedListed`, which also touches its other
  construction site in `gc_test.go`.

**Tests.** Renames only, no assertion changes. Do not assemble this list by hand —
derive it from the sweep in the test plan below, which is what produced the list
here. In `reconciler_test.go`: `TestEnqueuePendingWake`,
`TestCatchupTickEnqueuesPendingWake`,
`TestTypedControllerReconcileQuarantineKeepsPendingWake`,
`TestReconcileDecrementsPendingWake`, `TestReconcileDrainsMultiplePendingWakes`,
`TestReconcilePendingWakeSurvivesConcurrentWake`,
`TestReconcileWakesDecrementErrorIsNonFatal`, the
`reconcilePendingWakeHarness` helper, and the `pendingWakeIDsStore`,
`tickOnlyPendingWakeStore`, `errPendingWakeStore`, `failDecrementPendingWakeStore`
and `owedBadSpecStore` doubles (the last two override `WakesDecrement`; the
`owedWakes`-style method bodies move with the interface). `controller_test.go` calls
`WakesListPendingIDs` and follows the method rename, though no test there needs a new
name. In `sqlite/store_test.go`: `TestPendingWakeCount`,
`TestPendingWakeQueryErrors` and `TestRefsAddStampsPendingWake` (whose `Refs` prefix
is stale too — it covers `EdgesAdd`).

`TestCatchupTickDispatchesOwedWake` already reads "OwedWake" and keeps its name; it
is listed only so a sweep does not read its survival as a miss.

`TestReconcilePendingWakeSurvivesConcurrentWake` keeps its body: it pins the mid-pass
increment that `Decrement(id, observed)` is shaped for, and that shape does not change
here.

## The comment the column carries

The column's comment is the one piece of prose worth getting right, because it is
where the next reader decides whether their new marker belongs in this number. The
existing comment already earns its place: it explains the read-then-declare race, why
this is a count rather than a flag, and why the decrement subtracts the whole observed
count. **Keep all of that** — rename the identifiers inside it, fix the stale
`AddDependency`, and *append* the paragraph below. The count-not-a-flag reasoning
belongs in the schema, where the next reader already has the file open; the
restatement in this document is for the reviewer, and this document is not what they
will be reading in a year.

The paragraph to add:

> This is durable, owed *work*; the in-memory dependency waker is a separate mechanism
> and leaves nothing here. A second durable marker (undecodable rows, say) gets its
> own column and its own cadence — it does not join this count.

The same addition goes in `storeapi.go`, next to the existing "must not leave
separable" wording, which is unchanged and still covers the same write.

## Docs to update in the same commit

- **CLAUDE.md** names `objects.pending_wake` in the caller-versioned-dependencies
  bullet.
- **`docs/adr/2026-07-27-caller-versioned-dependencies.md`** is the heaviest user —
  the column, both methods, the index, and the count-not-a-flag section.
- **`docs/adr/2026-07-27-store-wide-dependency-change-stream.md`** names
  `WakesListPendingIDs` and the unreclaimed count.
- **`docs/adr/2026-07-27-noun-verb-naming.md`** cites `Wakes*`/`pending_wake` as a
  worked example — the precedent for a family that is a column rather than a table,
  and for `Pending` dropping out of a list method. It needs the new names, not
  deletion, plus the singular-prefix exception recorded above.
- **`docs/adr/2026-07-27-periodic-reconcile-drivers.md`** names `WakesListPendingIDs`
  in the catchup-set definition.
- **TODO.md** names the column throughout, including the two open items restated below.

ADRs record a decision as it was taken, so update the names in place and do not
rewrite the reasoning around them.

## Why a count rather than a flag

Unchanged by this document, restated because the rename is the moment someone asks.
`EdgesAdd` runs out of band, so a wake can arrive *while an earlier one is being
reconciled*. Increments that land after the load sit above the observed count, survive
the subtraction, and keep the object owed. A same-valued flag would be cleared by that
pass and then lost to a crash. `Decrement(id, observed)` subtracts the count the pass
loaded, because one pass reads the target's current state and so addresses every wake
outstanding when it started.

## Out of scope, filed separately

**The decrement is not kind-scoped** — `WHERE id = ?` with no group/kind in the
predicate, unlike every other id-keyed write in the store. That is safe today, because
its only caller is the reconciler acting on a row it just loaded for its own kind. Now
filed in TODO.md as its own item; do not fold it in, or this stops being a diff a
reviewer can skim. The rename renames the method and leaves the predicate alone.

**The unreclaimed count on client-only kinds** stays exactly as TODO.md and the
caller-versioned-dependencies ADR describe it: `EdgesAdd` can set the counter on a
dependent whose kind has no controller, nothing drains it (the decrement happens in a
reconcile) and nothing reads it (the list is per-kind and only its own reconciler calls
it). The recorded fix — a cross-kind sweeper, the analogue of the global GC sweeper's
`DeletionRequestsList` — remains right and remains unbuilt. Renaming the column does
not touch it.

## Test plan

No new tests — if a test needed writing here, the change would not be a rename. But a
green suite is the wrong gate for this change: it proves the Go compiles, and the
things most likely to be missed are comments, ADRs, TODO.md and SQL, none of which
compile. Two sweeps stand in for it.

**Nothing old survives.** Zero hits outside this spec and the waker allowlist below:

```sh
grep -rn "pending_wake\|PendingWake\|Wakes\|WakeStamped\|wakeListed" \
  --include="*.go" --include="*.sql" --include="*.md" .
```

**The waker is still there.** This is the real risk — an over-eager sed collapses the
two concepts the change exists to separate, and it collapses them *silently*, because
the result still compiles and still passes. Non-zero hits required:

```sh
grep -rn "dependentsWake\|wakeAfterCommit\|pendingWakes\|TestWakeDependents\|TestDependencyWaker" \
  --include="*.go" .
```

Both sweeps produced the file lists above, and re-running them is how you check those
lists were complete. Then `go build ./... && go vet ./... && go test ./...`.
