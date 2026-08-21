# The tripwire sees a PRAGMA, and what cannot be prepared says so

- **Status:** Accepted — `sqlite/store_test.go`, `sqlite/store.go`.
  Retires `docs/specs/2026-08-21-let-the-tripwire-see-a-pragma.md`, which git
  holds.
- **Date:** 2026-08-21
- **Builds on:** [Prepare every constant statement at startup](2026-08-21-prepare-every-constant-statement.md)

## Context

That record says every statement whose text is constant is prepared at startup,
and `TestOnlyRenderedSQLLivesInAFunction` is what stops a new hot read from being
silently unprepared. It matched `SELECT|INSERT INTO|UPDATE|DELETE FROM`, so it
could not see a `PRAGMA`, a `SAVEPOINT`, a `ROLLBACK TO` or a `RELEASE`.

Six statements sat outside the only check that would have found them — three
pragmas and three savepoint verbs. They were found by hand, which is the failure
this record is about: the rule held, the check could not see the exception.

## Decision

**The tripwire matches pragmas and savepoint verbs too**, and three functions are
exempt with the reason each cannot be prepared carried beside the name:

- `pageCounters` — `PRAGMA page_count` and `PRAGMA freelist_count` are constant,
  and preparing the pair measures −11%. They still cannot be: `freePagesRelease`
  reads them, vacuums, and reads them again on the one connection `ReclaimSpace`
  holds, and `released = free - freeAfter` is exact only because both reads see
  that connection's snapshot. A prepared read routes by ctx and, with no live
  transaction, takes the reader pool — no deadlock, just a subtraction that
  quietly spans snapshots. Preparing against a held `*sql.Conn` is not something
  the statement-set machinery does.
- `freePagesRelease` — `PRAGMA incremental_vacuum(N)` interpolates its budget.
  SQLite takes no bound parameter in a pragma argument: the `?` form fails at
  *prepare*, not at execution.
- `txState.nested` — a savepoint name is not a parameter either, and the names
  are monotonic and unbounded, so there is no statement per name to prepare.

**The savepoint verbs must be the whole literal.** Matched loosely they are
ordinary English, and the pattern is case-insensitive, so `"free pages: release
failed"` would make its function look like it holds SQL. A word boundary does not
help — `release` is a whole word there — so the three verbs anchor to the entire
literal, which is how `savepointStmt` is handed them. That in turn needs the
literal unquoted before matching: `ast.BasicLit.Value` is raw source, and an
anchored pattern would otherwise be comparing against the quote characters.

**An exemption carries its reason or it is not an exemption.** The map is
`name → why`, and `TestEveryUnpreparableExemptionIsLive` fails on an entry that
no longer holds a statement and on one with an empty reason. A stale entry is how
the original hole stayed open; a list that cannot rot is the point of the change.

## Consequences

**Nothing is converted and nothing gets faster.** The −11% on the page counters
is why they are *not* prepared, not a win being claimed. What the change buys is
that the next constant pragma is caught by CI rather than by somebody reading the
file.

**The rule is now stateable in full**: every statement whose text *can* be
constant is prepared. Four statements cannot be — one pragma argument and three
savepoint names, all because SQLite takes no parameter there — and two are
constant but bound to a caller's connection. `Events().List` is the only rendered
statement left, measured as a regression and recorded in `docs/TODO.md`.

**The migrations stay out of scope.** `inspectPackage` walks the `sqlite` package
only, and `internal/sqlitemigrate` runs once per process before the statement
sets exist.
