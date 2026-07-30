# A store write takes only what it honours and returns only what a caller reads

- **Status:** Accepted — implemented in `internal/storeapi/storeapi.go`, `sqlite/store.go`,
  `client.go`, `controller.go`.
- **Date:** 2026-07-30

## Context

`storeapi.RawObject` mirrors a whole `objects` row. It is the shape every read
returns, and it was also the shape every write spoke in — on both sides of the call.

**On the way in.** `ObjectsCreate` took a `*RawObject`. The `INSERT` bound six of its
eighteen fields — group, kind, slug, spec, `schema_version_spec`, finalizers — and
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
`clientImpl.Delete`/`DeleteBySlug` discarded it from both deletion entry points. The
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
carries exactly the four remaining fields the `INSERT` binds: `Finalizers`, `Slug`,
`Spec`, `SpecVersion`. Seeding a status is now a compile error rather than a silent
discard.

**A write returns what its caller reads, and nothing more.** `ObjectsCreate` and
`ObjectsUpdateSpec` keep returning the row, because `Client.Create`, `Update` and
`GetOrCreate` hand it to the user, who has no other way to see
the store-assigned id, version and timestamps. Every other mutator returns `error`,
plus a `bool` where whether the write landed is not otherwise derivable
(`DeletionRequestsCreate`, `DeletionRequestsCreateBySlug`). The `RETURNING`
clauses behind them are gone: `bumpObject`, the finalizer rewrite, both status
writes and `markForDeletion` now `Exec`. Each had already read its row under a kind
scope inside the same transaction, so `RETURNING` was never what proved the row
existed.

`markForDeletion` reports `RowsAffected() > 0` where it used to lean on
`RETURNING`'s `ErrNotFound`. That signal was always ambiguous — guard, scope, or
missing row — and `requestDeletion`'s second read is what disambiguates it. That read
is now a probe returning only an error, which is where the deletion path's saving
comes from: the already-pending branch, the steady state for an idempotent
controller, no longer decodes a row or queries conditions to answer.

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
answer is one place, and the line falls where the callers put it rather than
somewhere chosen for tidiness.
