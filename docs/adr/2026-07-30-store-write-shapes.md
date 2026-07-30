# A store write takes only what it honours and returns only what a caller reads

- **Status:** Accepted — implemented in `internal/storeapi/storeapi.go`, `sqlite/store.go`,
  `client.go`, `controller.go`.
- **Date:** 2026-07-30

## Context

`storeapi.RawObject` mirrors a whole `objects` row. It is the shape every read
returns, and it was also the shape every write spoke in — on both sides of the call.

**On the way in.** `ObjectsCreate` took a `*RawObject`. The `INSERT` bound six of its
eighteen fields — group, kind, name, spec, `schema_version_spec`, finalizers — and
ignored the rest. Some of the twelve are defensible: `ID`, `ResourceVersion`,
`CreatedAt` and `UpdatedAt` are store-assigned, `Generation` starts at 1 and
`ReconcileOwed` at 0. `Status` was the sharp one. Seeding a status on create is a
reasonable thing to try, it is silently discarded, and nothing at the call site says
which fields are honoured and which are decoration.

**On the way out.** Every mutator returned a full `*RawObject`, and the write path
assembled it through `scanWritten`, which calls `attachConditions`. So each write
paid an indexed query on the `conditions` table to build a value that, for six of the
seven mutators, no caller ever read: `controllerClientImpl` discarded the row from
`UpdateStatus`, `ConditionsSet`, `ConditionsDelete` and `FinalizersDelete`, and
`clientImpl.Delete`/`DeleteByID` discarded it from both deletion entry points. The
waste was worst exactly where the least happened — the content no-op branches, which
did no write at all and still assembled a row to report it.

Both halves were deferred for the same reason: `type Store = storeapi.Store` and
`type RawObject = storeapi.RawObject` are exported aliases, so either change breaks
an interface an application could implement outside this repo. That cost is paid once
per break, not once per fix, which is why these landed together — with the
kind-scoping of `ReconcileOwedDecrement`, which had been waiting on the same table.

## Decision

**A write takes a shape built for writing.** `ObjectsCreate(ctx, gk, ObjectsCreateInput)`
replaces the `*RawObject` parameter. The `GroupKind` moves out to its own argument,
matching every other kind-scoped call in the interface, and `ObjectsCreateInput`
carries exactly the four remaining fields the `INSERT` binds: `Finalizers`, `Name`,
`Spec`, `SpecVersion`. Seeding a status is now a compile error rather than a silent
discard.

**A write returns what its caller reads, and nothing more.** `ObjectsCreate` and
`ObjectsUpdateSpec` keep returning the row, because `Client.Create`, `Update` and
`GetOrCreate` hand it to the user, who has no other way to see
the store-assigned id, version and timestamps. Every other mutator returns `error`,
plus a `bool` where whether the write landed is not otherwise derivable
(`DeletionRequestsCreate`, `DeletionRequestsCreateByName`). The `RETURNING`
clauses behind them are gone: `bumpObject`, the finalizer rewrite, both status
writes and `markForDeletion` now `Exec`. Each had already read its row under a kind
scope inside the same transaction, so `RETURNING` was never what proved the row
existed.

`markForDeletion` reports `RowsAffected() > 0` where it used to lean on
`RETURNING`'s `ErrNotFound`. That signal was always ambiguous — guard, scope, or
missing row — and `requestDeletion`'s second read is what disambiguates it. That read
is now a probe returning only an error, and it reads `"group"`/`kind` rather than the
whole row (`checkObjectScoped`), which is where the deletion path's saving comes
from: the already-pending branch, the steady state for an idempotent controller,
answers from metadata alone — no blob fetch, no finalizer unmarshal, no conditions
query. A probe built on the read-path row readers would have kept the conditions
saving and thrown the rest away.

`checkObjectScoped` is the general form of that, not a one-off for the deletion pair:
**a write that reports no row reads no row.** Both condition mutators gate on kind
through it too, which is what stops a corrupt `finalizers` blob from failing a
condition write that never touches finalizers — the inconsistency that gave the rule
away, since `DeletionRequestsCreate` already tolerated such a row.

The same rule applied to the version draw. `markForDeletion` used to call
`nextResourceVersion` — an `UPDATE` on `resource_version_seq` — *before* the `IS NULL`
guard decided whether anything would be stamped, so a repeat delete committed a
counter write and its fsync to stamp nothing. It now reads `value + 1` inline in the
`UPDATE` and advances the counter only once a row was stamped, making the
already-pending path a pure read. That is safe because the two statements sit in the
caller's transaction on a single connection, and because every `where` this helper
takes keys on a unique column, so the subquery can never hand one value to two rows.

**`ObjectsUpdateSpec` loses its `changed` bool**, which is where the two halves of the
rule pull against each other. It passes the derivability test — the returned row has
no before-state, so a caller genuinely cannot reconstruct "was this a no-op" from it —
and fails the one that matters, which is that nobody reads it. Both call sites spelled
it `_`. It survived the first draft of this sweep on the strength of a hypothetical
caller: `CreateOrUpdate`, the one place a follow-up might plausibly have been skipped
on a no-op. `CreateOrUpdate` is deleted in the same change, and with it the only
candidate. What is left is `Client.Update`, which discards it.

So the rule is "what a caller reads", not "what a caller could not otherwise compute";
the second is a reason to *keep* a value someone reads, not a reason to ship one nobody
does. The suppression behaviour is unchanged and still pinned — by generation and
`resource_version` in `TestObjectsUpdateSpecIdenticalSpecIsNoOp` and by
`TestClientNoOpUpdateOwesNothing` at the client layer, which are what the bool was
being cross-checked against anyway.

## Consequences

`attachConditions` is reached from one write path, `scanWritten`, and `scanWritten`
from two mutators. A missing `conditions` table can therefore no longer fail a status
write, a deletion mark, or a condition write — `TestNonConditionWriteAssemblyError`
pins that inversion, having previously asserted the opposite for three of them. For
the same reason `DeletionRequestsCreate` no longer decodes the row it stamps, so an
undecodable blob on that row cannot fail the mark; it still fails the probe, which
is what `TestDeletionRequestsCreateDoesNotScanTheRowItMarks` covers.

Tests that read a mutator's return now re-read with `ObjectsGet`. That is a little
more ceremony per test and it is the honest shape: it asks the store what is stored,
rather than trusting a value the write handed back.

**Rejected: dropping `attachConditions` from the write path while keeping the
`*RawObject` returns.** It looks like the smaller change and it is the wrong one —
`Client.Update` would start handing users an object whose `Conditions` are silently
nil. Also rejected: skipping the assembly per branch, which would make one method's
return shape depend on which branch it took, and differ from the sibling method a few
lines away. The contract was the thing to change, not the branch.

The interface is now asymmetric — two mutators return a row and five do not — which
is a thing to remember. `Store`'s doc comment states the rule and the reason, so the
answer is one place. State it as the invariant rather than as two facts: **a mutator
returns a row iff a public `Client` write returns that object to the user.** That
makes the next mutator's shape decidable instead of a judgement call, and it is why
the line falls where the callers put it rather than somewhere chosen for tidiness.

One consequence worth naming: `ObjectsCreate` returns a row but does *not* assemble
conditions for it, because a condition references an object id and the id was minted
by the `INSERT` that returned the row — so the row provably has none, and `nil` is
what assembling would have produced. `scanWritten` is therefore reached from
`ObjectsUpdateSpec` only.
