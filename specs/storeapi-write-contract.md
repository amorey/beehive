# The `storeapi.Store` write contract

- **Status:** Proposed — not implemented.
- **Date:** 2026-07-29
- **Replaces:** three deferred `TODO.md` items (`ObjectsCreate` takes a `RawObject`;
  mutators build a `RawObject` no caller reads; `ReconcileOwedDecrement` is not
  kind-scoped).

## Goal

Land the three deferred `storeapi.Store` breaks as one break, under one principle:

> **The write path is not the read path.** An input carries only what the write
> accepts, an output carries only what callers read, and every id-keyed write is
> scoped to a `GroupKind`.

All three are deferred for the same reason — `type Store = storeapi.Store` is an
alias, so each one breaks an externally implementable interface. That cost is paid
once here.

## Non-goals

- **No schema change.** Nothing in this bundle touches `sqlite/migrations`. The whole
  diff is Go signatures plus one SQL predicate.
- **No behaviour change** visible to `Client`/`ControllerClient` users. The one new
  error, `ErrWrongKind` from `ReconcileOwedDecrement`, is unreachable from the only
  caller (see Change 3, which is deliberate about *not* introducing a second one).
- **Not the cross-kind `reconcile_owed` sweeper** (`TODO.md`, the item above the
  decrement one). This bundle unblocks it — that sweeper is the second caller whose
  arrival the decrement item names as the trigger — but it stays deferred on its own
  merits.

## Current state (verified)

| fact | site |
| --- | --- |
| `ObjectsCreate` binds 6 of `RawObject`'s 18 fields; `Status` is bound to a literal `NULL` | `sqlite/store.go:653`, INSERT in `objectsCreate` at `:671` |
| Its only production caller sets exactly those 6 | `client.go:366` (`insertObject`) |
| `ObjectsUpdateSpec`'s row **is** read, on both branches | `client.go:423`, `client.go:532` |
| `ObjectsUpdateStatus`'s row is discarded | `controller.go:137` |
| `FinalizersDelete`'s row is discarded | `controller.go:181` |
| `ConditionsSet`'s row is discarded | `controller.go:142` |
| `ConditionsDelete`'s row is discarded | `controller.go:153` |
| `DeletionRequestsCreate`'s row **and** `changed` are discarded | `client.go:862` |
| `DeletionRequestsCreateBySlug`'s row **and** `changed` are discarded | `client.go:880` |
| `ReconcileOwedDecrement`'s UPDATE is keyed `WHERE id = ?`, no kind | `sqlite/store.go:946` |
| Its only caller already holds the `GroupKind` | `reconciler.go:142` (`t.gk`) |

The TODO describes the return-shape waste as per-branch ("the branches whose row
nobody reads"). It is broader than that: for six of the seven mutators the caller
discards the row on **every** branch. `ObjectsUpdateSpec` is the sole exception and
keeps its full return, including on its content no-op, because `Client.Update`
hands that object back to the user.

## Change 1 — `ObjectsCreateInput`

```go
// before
ObjectsCreate(ctx context.Context, obj *RawObject) (*RawObject, error)

// after
ObjectsCreate(ctx context.Context, gk GroupKind, in ObjectsCreateInput) (*RawObject, error)

// internal/storeapi, aliased in store.go beside RawObject
type ObjectsCreateInput struct {
	Finalizers  []string
	Slug        *string
	Spec        []byte
	SpecVersion int
}
```

Named `ObjectsCreateInput`, not `CreateObjectInput`: the house convention is
`NounsVerb…`, `EdgesAddResult` (`internal/storeapi/storeapi.go:241`) is the
precedent, and it sorts beside the method it feeds in godoc.

The kind moves out to a positional `gk` rather than staying inside the struct. Every
other kind-scoped mutator takes one, and `ObjectsCreate` is the only write that
smuggles the kind inside a value; keeping it in the struct would re-create the shape
this change exists to remove.

`ObjectsCreate` keeps returning `*RawObject` — `Client.Create` returns the object to
the user, and the store-assigned id, `resource_version` and timestamps come back on
the `RETURNING` clause that already exists.

**Free saving, no interface change: drop `attachConditions` from the create path.**
`objectsCreate` (`sqlite/store.go:671`) ends in `scanWritten`, which runs the
conditions query — but the id was assigned by that very INSERT, so the row provably
has no conditions. Scan the returned columns and set `Conditions: nil` directly. That
removes one indexed query from every create, `Client.Create` included, which is the
same per-write saving the rest of the break is being bought for.

Also delete the "ignored on a `RawObject` handed to `ObjectsCreate`" clause from the
`ReconcileOwed` godoc (`internal/storeapi/storeapi.go:199`). It documents a drop that
will no longer be expressible.

## Change 2 — narrow the return, don't narrow the godoc

`TODO.md` offers three options: narrow what the godoc promises, add variants
returning nothing, or make the discard explicit at the boundary. Take the one that
removes the `attachConditions` query. A break bought for a documentation change is a
break wasted.

| method | before | after | saving |
| --- | --- | --- | --- |
| `ObjectsCreate` | `(*RawObject, error)` | unchanged — user-facing | conditions query (Change 1) |
| `ObjectsUpdateSpec` | `(*RawObject, bool, error)` | unchanged — user-facing, both branches | none |
| `ConditionsSet` | `(*RawObject, error)` | `error` | conditions query + 17-column scan, **per reconcile pass** |
| `ConditionsDelete` | `(*RawObject, error)` | `error` | same |
| `ObjectsUpdateStatus` | `(*RawObject, error)` | `error` | conditions query + scan |
| `FinalizersDelete` | `(*RawObject, error)` | `error` | conditions query + scan |
| `DeletionRequestsCreate` | `(*RawObject, bool, error)` | `(changed bool, err error)` | conditions query + scan |
| `DeletionRequestsCreateBySlug` | `(*RawObject, bool, error)` | `(changed bool, err error)` | conditions query + scan |

**The condition mutators are the biggest win in the bundle, not a footnote.** Both
discard at `controller.go:142,153`; both route their success path through `bumpObject`
(`sqlite/store.go:1452`) — `UPDATE … RETURNING objectColumns` + `scanWritten` +
`attachConditions` — and their no-op branches call `attachConditions` directly.
`bumpObject` has exactly those two callers (`:1530`, `:1561`), so dropping both
returns collapses it to a bare `Exec`. Controllers set conditions on every pass, so
this is the one saving on the hot path.

**`changed` stays** on the deletion mutators. No production caller reads it, but it
costs no query, it is the documented idempotence contract, and `sqlite/store_test.go`
pins that contract with it. Dropping it would force those tests to re-derive
idempotence from a `resource_version` read — worse tests in exchange for nothing.

**`EventsAdd` is excluded.** It returns `*storeapi.Event` off `RETURNING
eventColumns` (`sqlite/store.go:1624`) — no `attachConditions`, no second query.
Narrowing it to `error` would save nothing and spend API surface, so it keeps its
return. This is the exception that shows the rule is about the discarded *query*, not
about discarded returns as such.

### `sqlite` implementation notes

- `scanWritten` (`sqlite/store.go:642`) splits. The row-returning path keeps
  `attachConditions`; the rest either `Exec` outright or scan only what they still
  report.
- `bumpObject` collapses to a bare `Exec` once both callers stop reading the row.
- `FinalizersDelete` (`sqlite/store.go:1757`): the no-op branch drops its
  `attachConditions` call and returns; the write branch becomes an `Exec`. That is
  safe **only because** the scoped read a few lines above already proved the row
  exists and belongs to `gk` — the `Exec` is not carrying the existence contract.
  Say so in the comment, or a later refactor that drops the "redundant" read will
  silently turn a missing row into success.
- `requestDeletion` (`sqlite/store.go:1829`): the `ErrNotFound` re-read stays — it is
  what disambiguates already-deleting from out-of-scope from gone, i.e. the source of
  `changed` and of `ErrWrongKind`. Only its `attachConditions` goes.
- **`markForDeletion` (`sqlite/store.go:1803`) does not become an `Exec`.** Its
  `ErrNotFound` is the entire input to `requestDeletion`'s three-way resolution, and
  it comes from `scanObject` mapping `sql.ErrNoRows`. Re-deriving that from
  `RowsAffected` routes a load-bearing contract through driver semantics this repo
  has been bitten by once already — the `PRAGMA incremental_vacuum` `Exec`-vs-`Query`
  step trap in `CLAUDE.md`, in the opposite direction. Narrow the clause to
  `RETURNING id` and keep `QueryRow(...).Scan(&id)` with the existing `ErrNoRows` →
  `ErrNotFound` mapping: same saving, no new failure mode.

## Change 3 — kind-scope `ReconcileOwedDecrement`

```go
// before
ReconcileOwedDecrement(ctx context.Context, id ObjectID, observed int64) error

// after
ReconcileOwedDecrement(ctx context.Context, gk GroupKind, id ObjectID, observed int64) error
```

```sql
UPDATE objects SET reconcile_owed = max(reconcile_owed - ?, 0)
WHERE id = ? AND "group" = ? AND kind = ?
```

**A foreign id returns `ErrWrongKind`. A vanished row returns `nil`.** On
`RowsAffected == 0`, resolve with a scoped re-read (`getObjectRowScoped`,
`sqlite/store.go:706` — the idiom `FinalizersDelete` and `requestDeletion` already
use): `ErrWrongKind` if the row exists under another kind, `nil` if it is simply
gone. The re-read runs only on that branch, so the normal path stays one statement.

The asymmetry is the point. `ErrWrongKind` is what moves the kind invariant from
convention into the schema, and it is unreachable from `typedController`, which
passes the kind it loaded the row for one line earlier. `ErrNotFound` is a different
story: **the vanished-row case is reachable in production today**, where it is a
silent no-op. The row can be collected between the reconciler's load and its
decrement — by the `gcCollect` at the end of that same pass, which `reconciler.go`'s
own comment at `:155` already reasons about, or by another process's GC sweeper
concurrently. Propagating `ErrNotFound` would turn a benign race into a periodic
`failed to decrement the reconcile-owed count` warning at `reconciler.go:142`, in a
system whose premise is that a lost wake costs latency rather than correctness.
Nothing to decrement is not an error.

The caller passes `t.gk`. Its existing warn-and-continue error handling is correct
for `ErrWrongKind` and needs no change.

## Test plan

New:

- `sqlite/store_test.go`: a foreign-kind `ReconcileOwedDecrement` returns
  `ErrWrongKind` and leaves the target row's count untouched.
- `sqlite/store_test.go`: a decrement against a deleted row returns `nil`. This is
  the blocking-item contract; without a test it is one "tidy up the error handling"
  refactor away from becoming the warning it exists to prevent.
- `sqlite/store_test.go`: `ErrWrongKind` from `ObjectsUpdateStatus` and from
  `FinalizersDelete`. Once those return only `error`, their scoped re-read is the
  sole remaining consumer of that load and is trivially deletable by a later
  refactor with nothing failing. Same for `ConditionsSet`/`ConditionsDelete` if their
  scoped reads end up in the same position.
- `sqlite/store_test.go`: every `ObjectsCreateInput` field lands on the row — the six
  that used to be honoured, now asserted as the whole contract rather than as a
  subset of a wider struct.

Rewrites (not mechanical):

- The existing `DeletionRequestsCreate`/`BySlug` tests that assert on the returned
  row. They lose their subject and must be re-expressed against `changed` plus a
  follow-up read.
- Any `ConditionsSet`/`ConditionsDelete` test asserting on the returned object,
  same treatment.

Mechanical:

- ~40 `ObjectsCreate(ctx, &RawObject{…})` call sites across `reconciler_test.go`,
  `client_test.go`, `gc_test.go`, `watchpoll_test.go`, `beehive_test.go` and
  `sqlite/store_test.go`. Introduce a `mustCreate` helper in `testutils_test.go`, and
  a sibling in `sqlite/store_test.go` (different package), so the next `Store` break
  is a one-line edit rather than forty.
- `fakeStore` (`testutils_test.go:168,205,208,211`) and the per-test doubles at
  `reconciler_test.go:1316,1765`, `controller_test.go:712`, `client_test.go:1368`.

Watch the compile errors from Change 1 for any test seeding `Status` through
`ObjectsCreate`. Surfacing those is the trap the item exists to close; there should
be none, since the field has never done anything.

## Sequencing

One branch, merged as one break. Three commits plus docs:

1. **`ReconcileOwedDecrement`.** Smallest diff, and it touches `fakeStore` and two
   doubles — so the fixture churn lands before the large mechanical pass.
2. **`ObjectsCreateInput`.** The wide-but-shallow commit, including the
   create-path `attachConditions` drop. Land the `mustCreate` helpers in the same
   commit as the churn they absorb.
3. **Return contract.** Interface, `sqlite` scan paths, six call sites, doubles.

`go build ./... && go vet ./... && go test ./...` green at each commit — no commit
may leave the suite red, since each is independently revertable.

## Docs

- Delete the three items from `TODO.md` (lines ~272–334).
- Add a line to the cross-kind-`reconcile_owed`-sweeper item noting that its stated
  blocker ("revisit when a second caller appears" / the `Store` break) has cleared.
- Write **one** ADR, `docs/adr/2026-07-29-storeapi-write-contract.md`, at landing
  time. All three changes are one argument; three ADRs would fragment it. Add it to
  the `docs/adr/README.md` index.
- Prose that contradicts the new contract, all of which must move in the same pass:
  - `store.go:22` — "Mutators return the freshly written row so callers see the
    store-assigned id, resource_version, and timestamps without a re-read." Exactly
    the promise Change 2 stops making in general.
  - `internal/storeapi/storeapi.go:648` — the `ReconcileOwedDecrement` godoc: add the
    `gk` parameter, and state the `ErrWrongKind`/row-gone-is-`nil` split.
  - `sqlite/store.go:929–931` — "The decrement's contract (cross-kind, no
    resource_version bump…)" asserts as contractual precisely what Change 3 removes.
  - `internal/storeapi/storeapi.go:199` — the `ReconcileOwed` create clause
    (Change 1).

## Risks

**Verified non-risk: the returned row is not on a notification path.** Comments at
`controller.go:135` and `sqlite/store.go:1791` describe mutators "emitting a
Modified" into "its transaction's collector". No such collector exists in `sqlite/`
or `internal/storeapi/` — watches discover writes by polling `resource_version`
(`watchpoll.go`), per the architecture. Dropping the row therefore removes nothing a
watcher depends on. The stale comments should be corrected in the same pass.

**Real risk: external implementers.** Every one of these is a breaking change to a
public interface, by construction. That is the accepted cost; the mitigation is to
take all three at once and never a fourth separately, which is why `ConditionsSet`
and `ConditionsDelete` are promoted into Change 2 rather than deferred to an audit.

**Bounded risk: `ErrWrongKind` on the decrement.** A new error on a path whose caller
logs and continues. Unreachable from `typedController`. If a future caller (the
cross-kind sweeper) wants to reach across kinds deliberately, it needs a different
method, not a laxer predicate — which is the outcome this change is for.
