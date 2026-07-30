# The store runs auto_vacuum=INCREMENTAL, drained by the GC sweeper above a floor

- **Status:** Accepted — implemented in `internal/sqlitemigrate/sqlitemigrate.go`,
  `sqlite/sqlite.go`, `sqlite/store.go`, `internal/storeapi/storeapi.go`,
  `types.go`, `beehive.go`.
- **Date:** 2026-07-29

## Context

`auto_vacuum` is the one SQLite setting that cannot be changed after the fact. The
mode is written into the file header when the **first table is created**; on a
database that already has one, switching costs a full `VACUUM` rewrite of the file.
Until this decision every file Beehive wrote was `auto_vacuum=NONE`, and would have
stayed there.

Free space genuinely accumulates here. `gcCollect` removes collected object rows and
their edges, and `EventsSweep` trims the event log on the GC sweeper's cadence, so a
long-lived store whose object count churns holds its high-water page count for the
life of the file. Nothing is *incorrect* about that — SQLite reuses free pages for
later inserts — but the file only ever grows.

With no deployments in the field, the decision cost a DSN parameter. After the first
long-lived one it would have cost a rewrite of every file in existence. That
asymmetry, not a measurement, is why it was made now.

## Decision

**`INCREMENTAL`, set on the DSN, drained by the GC sweeper only once the freelist has
grown past a floor.**

### Why `INCREMENTAL` and not `FULL`

Both modes pay the same structural cost — pointer-map pages, measured at about **one
per 200 pages (~0.5% file growth)**, with no measurable per-commit regression (2000
single-row commits: 25.4ms on `INCREMENTAL`, 25.8ms on `NONE`, inside the noise). So
the mode itself is nearly free and the write path is not what to worry about.

They differ in *when* pages get moved. `FULL` does it inside every commit — on the
single write connection every user write and every driver already serialises through.
That is the wrong place for unbounded work in a design whose write path is the
contended resource. `INCREMENTAL` defers it to a moment and a batch size we choose.

### Why the DSN and not a migration

A `PRAGMA auto_vacuum` in `0001_init.sql` would be a silent no-op twice over.
`sqlitemigrate.Apply` creates `schema_migrations` *before* it applies any migration,
so the database is no longer empty by the time `0001_init.sql` runs — and each
migration runs inside a transaction, where the pragma is ignored regardless. On the
DSN, modernc applies it at connection open, while the file is still empty.

On an existing `NONE` database the pragma is likewise a silent no-op, so adding it
could not disturb a file already written.

This makes `sqlitemigrate.OpenPool` an opinionated opener rather than a neutral one:
it decides the on-disk format of every database it opens, and only Beehive ships a
drainer. That is taken deliberately and said so in the package doc — and it is why
the package lives under `internal/`, so the opinion reaches no caller who did not
choose Beehive's storage strategy along with it. A caller that never drains pays only
the 0.5% and can still drain or `VACUUM` later without a format change; the reverse
is not true, which is what settles the direction of the default. A `maxConns`-style
parameter for one pragma would be worse than the opinion.

### Why the GC sweeper owns the drain

It is where the free space is produced — collected rows and trimmed event runs — and
it is the one driver that **cannot be disabled** (`WithGCInterval` rejects a
non-positive interval), so the drain is guaranteed rather than conditional on
configuration. It already has the shape this needs: best-effort, log the failure,
retry next tick. Nothing is incorrect while space is unreclaimed, so there is nothing
to escalate.

### Why there is a floor

Draining toward an *empty* freelist on every tick would fight SQLite's own page
reuse. On exactly the store this is aimed at — steady churn, with event retention
trimming on the same tick — free pages are what the next inserts would have reused
for free. Releasing them means moving pages, writing them to the WAL, re-writing
pointer maps, and then re-growing the file on the next write, all on the one write
connection. That is a smaller version of what `FULL` was rejected for.

So the drain is gated on the freelist exceeding **both** an absolute floor (256
pages, ~1MB) **and** a fraction of the file (1/8). The absolute floor keeps a small
or quiet database from being churned at all; the fraction keeps a large steady-state
one from being drained forever just because 1MB is a rounding error in it. Above the
gate the per-tick cap walks the freelist down over successive ticks until it falls
back under, and then stops. That is the hysteresis: a churned file does not hold its
high-water mark forever, and a steady one is left alone.

The floor lives in the `sqlite` package rather than in the sweeper because both
numbers are SQLite pages — a unit the store-agnostic core has no business knowing.
The sweeper passes only the cap.

### The cap is 1000 pages

Draining costs about **3.7µs a page**, which gives the constant a derivation instead
of a vibe: 1000 pages is ~3.7ms of held write lock once per GC interval, negligible
beside the sweep it runs after, and reclaims ~4MB per 30s tick. A cap of 100 would
give back 400KB a tick and take the better part of a day on a gigabyte of freed
space — slower than a churning store can produce it, which makes the drain
decorative. Neither the cap nor the floor is an option: there is no measurement a
caller could tune them against that the sweeper does not already have.

### Why `FreePagesReleaser` is optional, not part of `Store`

Reclaiming pages is one backend's concern. Putting `FreePagesRelease` in `Store`
would make every implementation — and every test double — answer a question only
SQLite has. It is an optional interface the sweeper type-asserts and skips when
absent, and the contract says an implementation may release fewer pages than asked,
including none, since a backend is entitled to decide free space is worth more kept
than returned.

## Consequences

- **`PRAGMA incremental_vacuum` frees one page per step, so the transport is
  load-bearing.** It must be `Exec`'d. Run through `Query` and closed without
  draining the rows it releases exactly **one** page and returns no error — a drain
  that looks alive and reclaims 4KB a sweep. The rule is a comment at the call site
  as well as here, and `TestFreePagesReleaseDrainsPastTheFloor` asserts
  `1 < released <= cap` specifically to catch it. The assertion is deliberately *at
  most* the cap, never *exactly*: the pragma promises up to N.
- **In WAL mode the main file does not shrink until a checkpoint.** A drained
  database can report thousands of pages behind a 16KB main file. Nothing is wrong;
  the pages are gone from the database and the file follows at the next checkpoint.
  Forcing one is a separate cadence decision with its own contention story and was
  not taken.
- **The released count is advisory.** It is a difference of two reads taken around
  the drain, so on a pool wider than one connection a concurrent writer can skew it.
  All three statements share one `*sql.Conn` so the pool at least cannot scatter
  them, and the count is logged, never acted on.
- **Files written before this change stay `NONE` forever** unless someone pays the
  `VACUUM`. Nothing in the code offers to: rewriting a database file is a deployment
  decision, not one a startup path should take on a caller's behalf. There are no
  such files in the field, which is the whole reason the decision was made now.
- `OpenMemory` carries the same pragma, so tests exercise the production format
  rather than a mode where the drain silently does nothing.
