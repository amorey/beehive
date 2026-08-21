# Let the tripwire see a PRAGMA

- **Status:** Planned. No statement changes and no measurement: this closes a
  hole in an existing check and writes down what cannot be prepared.
- **Date:** 2026-08-21
- **Depends on:**
  [prepare every constant statement at startup](../adr/2026-08-21-prepare-every-constant-statement.md),
  whose rule this enforces.

## Why

That ADR says every statement whose text is constant is prepared at startup, and
`TestOnlyRenderedSQLLivesInAFunction` is what keeps a new hot read from being
silently unprepared. It matches string literals against

```
SELECT\s|INSERT\s+INTO|UPDATE\s+\w|DELETE\s+FROM
```

so it cannot see a `PRAGMA`, and it cannot see `SAVEPOINT`, `ROLLBACK TO` or
`RELEASE` either. Six statements sit outside the only check that would have
found them — three pragmas and three savepoint verbs — and they were found by
hand.

Two of the six have constant text and still cannot be prepared. That is worth
knowing and is currently written down nowhere: the next reader has to re-derive
it, and the reader after that has to re-derive it again.

## The counters look preparable and are not

`pageCounters` (`sqlite/store.go:191`) issues `PRAGMA page_count` and `PRAGMA
freelist_count`. The text never varies, a pragma prepares and re-executes
normally — verified — and preparing the pair measures −11%, about 0.8 µs. Every
surface reading says convert it. Two things say otherwise, and they are the
reason this spec converts nothing.

**The counters must run on the connection that vacuums.** `ReclaimSpace`
(`:151`) takes `s.db.Conn(ctx)` and hands that one connection to
`freePagesRelease`, which reads the counters, runs `PRAGMA
incremental_vacuum(N)`, and reads them again. `released = free - freeAfter` is
exact today because both reads and the vacuum happen on the same connection
under the sole-writer rule. A prepared statement routes by ctx: with no live
transaction, `readStmt` returns the read pool's copy, so the counters would run
on one of four other connections and the subtraction would span snapshots. It
would not deadlock — the routing is fine — it would quietly stop meaning what it
means. Preparing against a held `*sql.Conn` is not something the statement-set
machinery does, and adding it is a design question, not a detail.

**And the seam it would break is deliberate.** `freePagesRelease` exists "split
out so the arithmetic can be tested against a scripted `dbtx`" (`:163`), and
`pageCounters` is a free function taking that `dbtx` for the same reason.
`queryRow` is a `*sqliteStore` method, so converting means a receiver or the
store threaded through — either way the scripted `dbtx` stops reaching the
counters, and the floor-and-fraction test goes with it.

So the counters join the list rather than leaving it.

## The change

**Widen the regex to `PRAGMA`, `SAVEPOINT`, `ROLLBACK TO` and `RELEASE`.**
Pragmas alone are not enough: the savepoint verbs match neither the current
pattern nor a pragma-widened one, so exempting them without widening for them
would leave three entries that can never fire.

**Then exempt three functions, each with its reason.** `sqlLiteralSites` returns
function names, not statements, so the list is short:

- `pageCounters` — constant, but must run on the caller's connection (above).
- `freePagesRelease` — `PRAGMA incremental_vacuum(N)` interpolates its budget.
  SQLite accepts no bound parameter in a pragma argument: `PRAGMA
  incremental_vacuum(?)` fails at *prepare* with ``near "?": syntax error``.
  Verified. Its existing comment argues `Exec` versus `Query`, a different trap;
  the parameter question needs its own line.
- `txState.nested` — holds all three savepoint verbs (`:361`, `:372`, `:380`).
  SQLite accepts no parameter where a savepoint name goes, which `savepointStmt`
  (`:287`) already documents, and the names are monotonic and unbounded, so a
  statement per name is not a design either.

An exemption without a reason is how the original hole was left, so the reason
travels with the entry rather than living in this spec.

The migrations (`internal/sqlitemigrate`) stay out of scope: `inspectPackage`
walks the `sqlite` package only, and they run once per process before the
statement sets exist.

## Tests

In `sqlite/store_test.go`:

- The widened `TestOnlyRenderedSQLLivesInAFunction` passes with exactly those
  three exemptions, and **fails if any is removed** — that is what stops a stale
  entry from accumulating, and it is a stronger check than asserting the list is
  non-empty.
- A new `PRAGMA` literal added to any other function fails the test. Worth an
  explicit case: the hole this closes was invisible precisely because nothing
  exercised it.
- `inspectPackage` still skips `_test.go`, so the five pragmas in the test file
  stay out of scope.

No benchmark and no measurement to report. The −11% above is why the counters
are *not* converted, not a win being claimed.
