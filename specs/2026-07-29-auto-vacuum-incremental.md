# Spec: `auto_vacuum=INCREMENTAL`, drained by the GC sweeper above a floor

- **Status:** Proposed — not yet implemented.
- **Date:** 2026-07-29
- **Revised:** 2026-07-29, after an independent replication of the verification
  numbers against `modernc.org/sqlite` v1.52.0 with the exact `OpenPool` DSN.
- **Retires:** the `auto_vacuum` entry in [TODO.md](../TODO.md) (the deadline item).

A spec, not an ADR: it describes code that does not exist yet. When it lands, fold
the *why* into a dated ADR under [docs/adr](../docs/adr/README.md), add the one-line
summary to `CLAUDE.md`, and delete this file.

## Context

`sqlitemigrate.OpenPool` sets four pragmas and never sets `auto_vacuum`, so every
database file Beehive has ever written is `auto_vacuum=NONE`.

The mode is chosen **before the first table exists**. On a database that already has
one, switching requires a full `VACUUM` rewrite of the file. That makes this the one
open item with a deadline: zero real deployments exist today, so the change costs a
DSN parameter; after the first long-lived deployment, it costs a rewrite of every
file in the field.

Free pages genuinely accumulate. `gcCollect` removes collected object rows and their
edges; `EventsSweep` trims the event log on the GC sweeper's cadence. Nothing is
incorrect — SQLite reuses free pages for later inserts — but a long-lived store whose
object count churns holds its high-water page count for the life of the file.

## Decision

Set `auto_vacuum=INCREMENTAL` in the DSN, and have the GC sweeper release a bounded
number of free pages per tick — but only once the freelist is large enough that
releasing it is not competing with SQLite's own page reuse.

### 1. `INCREMENTAL`, not `FULL`

Both modes pay the same structural cost: pointer-map pages, so a slightly larger file
and a few more page writes per insert. Measured, that cost is **~1 ptrmap page per
~200 pages (≈0.5% file growth)**, with no measurable single-row commit regression
(2000 individual commits: 25.4 ms with `INCREMENTAL`, 25.8 ms with `NONE` — inside
the noise). So the mode itself is nearly free, and the write path is not the thing to
worry about.

The two modes differ in *when* the page-moving happens. `FULL` does it inside every
commit — on the single write connection that every user write and every driver
already serialises through. That is the wrong place for unbounded work in a design
whose write path is the contended resource. `INCREMENTAL` defers it to a
caller-chosen moment with a caller-chosen batch size.

`NONE` (status quo) is rejected because the decision is irreversible-in-practice and
the option is nearly free right now — not because the cost of `NONE` has been
measured.

### 2. The pragma goes in the DSN, not in `0001_init.sql`

It **cannot** go in the migration. `sqlitemigrate.Apply` runs
`CREATE TABLE IF NOT EXISTS schema_migrations` before it applies any migration, so by
the time `0001_init.sql` executes the database is no longer empty and the pragma is a
silent no-op — and `runMigration` executes it inside a transaction, where
`PRAGMA auto_vacuum` is ignored regardless of emptiness.

`OpenPool`'s DSN is where modernc applies it: on connection open, before any table
exists. `OpenMemory` gets the same parameter, so tests exercise the same mode as
production.

**This is a deliberate API-scope decision.** `sqlitemigrate` is an exported,
general-purpose package, so baking the mode into `OpenPool` changes the on-disk
format for every consumer — and only Beehive will ship a drainer, so others get the
0.5% ptrmap overhead against a freelist nobody releases. That is still strictly
better than `NONE` for them (their file can be drained by a later `PRAGMA
incremental_vacuum` or shrunk by a plain `VACUUM` without a format change), and a
`maxConns`-style extra parameter for one pragma would be worse than the opinion.
Take the opinion and **say so in the package doc**: `OpenPool` is a pool with
Beehive's pragmas baked in, not a neutral opener.

### 3. The GC sweeper owns the drain

`gcSweeperRun` gains a third step after `deletionPendingSweep` and
`eventRetentionSweep`. It is the right owner for three reasons:

- It is the one driver that **cannot be disabled** (`WithGCInterval` rejects a
  non-positive interval — with the caveat already noted in `gcSweeperRun`'s comment,
  that a `Beehive` built field-by-field in a test can still reach a non-running
  driver), so the freelist is guaranteed to be drained.
- It is where the free pages are *produced* — row deletes and event trimming.
- It already has the best-effort shape this needs: log the failure, retry next tick.

### 4. Hysteresis: only drain a freelist that has grown past a floor

Draining toward an *empty* freelist every tick would fight SQLite's page reuse. On
exactly the store this is aimed at — steady object churn, with `EventsSweep` trimming
on the same tick — free pages are what the next inserts would have reused for free.
Releasing them means moving pages, writing them into the WAL, re-writing ptrmap, and
then re-growing the file on the next write, all on the single contended write
connection. That is a smaller version of what §1 rejects `FULL` for.

So the drain is gated, not unconditional. Skip it entirely unless the freelist is
above **both**:

- an absolute floor — **256 pages (≈1 MB)**, which keeps a small or quiet file from
  being churned at all; and
- a fraction of the file — **1/8 of `page_count`**, which keeps a large steady-state
  file from being drained continuously just because 1 MB is a rounding error in it.

Above the gate the per-tick cap takes over, so successive ticks walk the freelist
down until it falls back under the gate and the drain stops. That is the hysteresis:
the property we want (a churned file does not hold its high-water mark forever)
without touching the steady state.

Both numbers are SQLite pages — a unit the store-agnostic core has no business
knowing — so the gate lives in the `sqlite` implementation, which is already reading
those pragmas. The sweeper passes only the cap. (The alternative, passing the floor
down from `beehive.go`, puts a page count in a package that otherwise never mentions
pages.)

### 5. The cap is 1000 pages, and the comment shows the arithmetic

Draining costs **~3.7 µs/page**, measured. That gives the cap a derivation instead of
a vibe:

| Cap | Write lock held per tick | Reclaimed per 30s tick | Time to drain 1 GB |
| --- | --- | --- | --- |
| 100 | 0.4 ms | ~400 KB | ~21 hours |
| 1000 | 3.7 ms | ~4 MB | ~2 hours |
| 2000 | 7.4 ms | ~8 MB | ~1 hour |

**1000.** A cap of 100 reclaims slower than a churning store can plausibly produce
free pages, which makes the drain decorative. 3.7 ms of write lock once per 30s is
negligible against the sweep it already runs beside. Cite the µs/page figure in the
constant's comment so the next person can re-derive the choice rather than re-measure
it.

### 6. Plumbing: an optional interface, not a `Store` method

`incremental_vacuum` is a SQLite concept. Adding it to `Store` would force every
implementation — and the `fakeStore` double — to answer a question only one backend
has. Instead `sqlite.sqliteStore` implements an optional interface that
`gcSweeperRun` type-asserts and skips when absent.

### 7. `FreePagesRelease` must `Exec`, never `Query`

`PRAGMA incremental_vacuum` releases **one page per `sqlite3_step`**. Routing it
through `database/sql`'s query path therefore releases only as many pages as rows the
caller consumes:

| Call | Asked | Actually released |
| --- | --- | --- |
| `Query(...)` + drain all rows | 500 | 500 |
| `Query(...)` + `rows.Close()` without iterating | 500 | **1** |
| `Exec(...)` | 500 | 500 |

The signature returns a count, which is exactly what tempts an implementer into
`QueryRow`/`Query`-then-`Close` — and the failure is silent: a one-page-per-30s drain
that looks like it is working. This rule goes in `sqlite/store.go`'s comment at the
call site, not only in this spec, because the test that catches it can drift.

## Changes

| File | Change |
| --- | --- |
| `sqlitemigrate/sqlitemigrate.go` | Add `&_pragma=auto_vacuum(incremental)` to `OpenPool`'s DSN; extend the doc comment's pragma list, say why the mode must be set here rather than in a migration, and state the §2 API-scope opinion (these are Beehive's pragmas). |
| `sqlite/sqlite.go` | Add the same `_pragma` to `OpenMemory`'s DSN, so tests run the production mode. |
| `internal/storeapi/storeapi.go` | Declare the optional interface — `FreePagesRelease(ctx context.Context, maxPages int) (int, error)`, returning pages released. Documented as optional, backend-specific, permitted to release fewer than asked (including zero, when the backend's own floor says the freelist is not worth draining), and returning an **advisory** count. **Not** a member of `Store`. |
| `sqlite/store.go` | Implement `FreePagesRelease` on `sqliteStore`. Take a single `*sql.Conn` for all three statements (read `freelist_count`/`page_count`, gate per §4, `ExecContext` the pragma, re-read), so the delta cannot be skewed by another writer on a larger pool. Carry the §7 `Exec`-not-`Query` comment and the §4 floor constants. Not wrapped in `Within` — it is its own statement and there is nothing to make atomic with it. |
| `beehive.go` | Add `freePagesSweep`; call it from `gcSweeperRun` after the other two sweeps. Type-assert `bh.store` to the optional interface and return when it does not satisfy it. Add the page-cap constant beside `defaultGCInterval`, with the §5 arithmetic in its comment. |
| `README.md` | `README.md:120` — the GC sweep row's "what it scans" cell gains the freelist drain. `README.md:585` — `WithGCInterval`'s one-liner ("collect dead rows + prune the event log") gains it too. The README is the spec; both are invalidated by this change. |
| `TODO.md` | Delete the `auto_vacuum` entry. |
| `docs/adr/2026-07-28-periodic-scan-drivers.md` | The GC sweeper's "What it scans" cell gains the freelist drain, since the driver now does three things. |
| `docs/adr/` + `CLAUDE.md` | New ADR carrying the *why* above; one-line summary plus link in `CLAUDE.md`. Delete this spec file. |

**No new public option.** The cap and the floor are unexported constants, matching how
the other non-tunable driver internals are handled. `WithGCInterval` already governs
the cadence, and a second knob is how a config grows cargo.

**Naming.** `FreePagesRelease` follows `NounsVerbQualifier` — noun first, plural.

## Verification

Measured against `modernc.org/sqlite` v1.52.0 with the exact `OpenPool` DSN; every
claim below was reproduced independently.

- A **new** file opened with `&_pragma=auto_vacuum(incremental)` reports
  `PRAGMA auto_vacuum` = `2` both before and after the first `CREATE TABLE`, with
  `journal_mode=wal` active. The DSN placement does set the mode in the file header.
- Draining honours the cap: `page_count` 2233 → 2133 for `incremental_vacuum(100)`.
- An **existing** `auto_vacuum=NONE` file reopened with the pragma stays at `0`,
  keeps accepting writes, and treats `PRAGMA incremental_vacuum` as a silent no-op
  rather than an error. Shipping this is therefore safe for any file already written;
  it simply does not help those files.
- `PRAGMA incremental_vacuum` also succeeds *inside* an open transaction, so the call
  site is not constrained.
- `file::memory:` with the pragma reports `2`.
- Cost: **~3.7 µs/page** to drain; **~1 ptrmap page per ~200** (0.5% growth); 2000
  single-row commits 25.4 ms (`INCREMENTAL`) vs 25.8 ms (`NONE`).
- Page-per-step behaviour: the `Query`/`Exec` table in §7.

**In WAL mode the main file does not shrink until a checkpoint** — a drained database
can report 2234 pages behind a 16 KB main file. Nothing to fix; it goes in the ADR in
one sentence so nobody files "the drain doesn't work".

## Tests

- `sqlitemigrate`: a store opened by `OpenPool` on a fresh path reports
  `PRAGMA auto_vacuum` = 2 after migrations have created the schema.
- `sqlite/store_test.go`: grow the file, delete the rows, assert `freelist_count` is
  past the §4 gate, call `FreePagesRelease` with a cap below the freelist, and assert
  `0 < dropped <= cap` — **not** `dropped == cap`. The pragma's contract is *at most
  N*; the loose assertion still catches the §7 `Query`-path bug (which drops 1), and
  does not encode an equality the contract never promised.
- `sqlite/store_test.go`: a freelist **below** the gate returns 0 with no error and
  leaves `page_count` unchanged (§4), and an empty freelist does the same.
- `beehive_test.go`: a store double that does **not** implement the optional
  interface is swept without panicking or erroring; one that does has
  `FreePagesRelease` called by the sweeper (signal on a channel the fake closes — no
  sleeps).
- A failing `FreePagesRelease` is logged and swallowed, and the sweep still returns —
  same shape as the existing event-retention assertion.

## Non-goals

- **No `VACUUM`.** Nothing offers to rewrite an existing `NONE` file. That is a
  deployment decision with a full-file-rewrite cost, and no code path should take it
  on a caller's behalf.
- **No WAL checkpoint forcing** to make the main file shrink on a schedule. That is a
  separate cadence decision with its own contention story.
- **No page-cache or `mmap_size` tuning.** A separate TODO entry, deliberately still
  deferred: it is a tuning change with no measurement behind it, whereas this one is
  a deadline.
- **No public knob** for the cap, the floor, or for disabling the drain.

## Known duplication (not fixed here)

`sqlite/sqlite_test.go:56` opens a raw `file::memory:` DSN rather than going through
`OpenMemory`. It is an error-path test that never migrates — it passes an
already-closed DB to `open` so `Apply` fails — so it does not undercut §2's "tests
run the production mode". It is simply the one place the memory DSN is written twice,
and worth knowing about when the DSN next changes.
