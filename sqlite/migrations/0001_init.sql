-- Timestamps: INTEGER Unix-epoch milliseconds, UTC.
-- JSON blobs:  TEXT (spec, status, finalizers, event detail).
-- Core group:  empty string "" (never NULL).
-- Requires:    PRAGMA foreign_keys = ON.

-- ============================================================
-- objects
-- One row per GVK-identified object.
-- ============================================================

CREATE TABLE objects (
    -- Incarnation identity. AUTOINCREMENT required: a recycled id would break
    -- ABA safety on delete/recreate. int64 in Go; 0 = not yet persisted.
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- "" = core group, "acme.com" = plugin.
    "group" TEXT NOT NULL,
    kind    TEXT NOT NULL,

    -- The Client API's key. Immutable (a rename is delete+recreate, which is why
    -- edges key on id), unique within (group, kind). NOT NULL and the CHECK make
    -- that total at the column: SQLite NULL != NULL, '' is what unset
    -- configuration reads as, and Store is a public extension point.
    name TEXT NOT NULL CHECK (name <> ''),

    spec   TEXT NOT NULL, -- JSON, user-owned,        HARD / desired state
    status TEXT,          -- JSON, controller-owned,  SOFT / observed state (nullable)

    -- Schema version each blob was last written at; 0 = not versioned. Opaque to
    -- the store; the Migrator converts on read.
    schema_version_spec   INTEGER NOT NULL DEFAULT 0,
    schema_version_status INTEGER NOT NULL DEFAULT 0,

    -- Convergence handshake: generation bumps only on a spec change;
    -- observed_generation == generation means "applied". observed_at is a
    -- handshake timestamp, NOT a reconcile heartbeat — it stops ticking once a
    -- generation is recorded. Controller liveness belongs in the events log.
    generation          INTEGER NOT NULL DEFAULT 1,
    observed_generation INTEGER,
    observed_at         INTEGER,

    -- Global monotonic write cursor: watch cursor, CAS token, no-op suppression
    -- guard (bumped only on a real change).
    resource_version INTEGER NOT NULL,

    -- Async delete: deletion_requested_at set ⇒ finalizing;
    -- row removed only once finalizers clears to [].
    deletion_requested_at INTEGER,
    finalizers            TEXT NOT NULL DEFAULT '[]', -- JSON array of finalizer names

    -- Durable owed passes; 0 = nothing owed. A count, not a flag: a wake landing
    -- mid-pass sits above the observed count and survives the subtraction, and a
    -- successful reconcile subtracts the whole count it observed. The in-memory
    -- waker is separate and leaves nothing here.
    reconcile_owed INTEGER NOT NULL DEFAULT 0,

    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,

    UNIQUE ("group", kind, name)
) STRICT;

CREATE INDEX idx_objects_kind ON objects("group", kind);    -- list / resync a kind
CREATE INDEX idx_objects_rv   ON objects(resource_version); -- watch ordering

-- Finalizing objects, for the global GC sweeper. The key covers the sweeper's
-- exact read (SELECT id, "group", kind ... WHERE deletion_requested_at IS NOT
-- NULL ORDER BY id) — no row fetch, no sort; keep them aligned. The partial
-- WHERE keeps the wider key cheap.
CREATE INDEX idx_objects_deleting
    ON objects(id, "group", kind)
    WHERE deletion_requested_at IS NOT NULL;

-- Objects whose spec has not yet been fully reconciled by a controller.
CREATE INDEX idx_objects_unsettled
    ON objects("group", kind)
    WHERE observed_generation IS NULL OR observed_generation < generation;

-- Objects owed a durable wake (ReconcileOwedListIDs). Separate from unsettled:
-- an object can be spec-converged yet still owe a wake.
CREATE INDEX idx_objects_reconcile_owed
    ON objects("group", kind)
    WHERE reconcile_owed != 0;

-- ============================================================
-- conditions
-- One row per (object, type). Independent writers upsert only
-- their own condition type without clobbering others'.
-- ============================================================

CREATE TABLE conditions (
    object_id INTEGER NOT NULL REFERENCES objects(id) ON DELETE CASCADE,

    type    TEXT NOT NULL, -- e.g. "Ready", "Connected", "Healthy"
    status  TEXT NOT NULL CHECK (status IN ('True', 'False', 'Unknown')),
    reason  TEXT,          -- machine-readable token, e.g. "DialTimeout"
    message TEXT,          -- human-readable detail for the troubleshooting UI

    -- 0 = store-truth (valid across restart); 1 = liveness (valid only in the
    -- writing process — a prior-process write reads as Unknown until re-confirmed).
    liveness INTEGER NOT NULL DEFAULT 0 CHECK (liveness IN (0, 1)),

    transitioned_at INTEGER NOT NULL, -- epoch ms when status last CHANGED
    updated_at      INTEGER NOT NULL, -- epoch ms of last write (also the liveness stamp)

    PRIMARY KEY (object_id, type)
) STRICT;

-- No index beyond the primary key: every read is keyed on object_id, and the PK
-- autoindex already covers that prefix. A separate (object_id) index served
-- nothing and cost a b-tree write per upsert.

-- ============================================================
-- edges
-- Dependency-tree edges. Both endpoints are hard integer FKs
-- into objects(id) — ids are never reused, so stale targets
-- are impossible by construction.
-- ============================================================

CREATE TABLE edges (
    -- dependent / child.  ON DELETE CASCADE: removing the child drops its outgoing edges.
    from_id INTEGER NOT NULL REFERENCES objects(id) ON DELETE CASCADE,

    -- target / owner.  ON DELETE RESTRICT: a target cannot be physically removed while
    -- edges still point at it, and an edge cannot point at a nonexistent object.
    -- No to_uid soft guard or re-adoption machinery needed.
    to_id INTEGER NOT NULL REFERENCES objects(id) ON DELETE RESTRICT,

    -- owned_by   deleting `to` cascades to `from` (foreground, via the GC reconciler)
    -- depends_on `to` going NotReady ⇒ `from` requeued automatically by Beehive
    relation TEXT NOT NULL CHECK (relation IN ('owned_by', 'depends_on')),

    -- WITHOUT ROWID: every column is in the key, so a rowid table would store
    -- each edge twice. Also what makes idx_edges_to covering.
    PRIMARY KEY (from_id, to_id, relation)
) STRICT, WITHOUT ROWID;

-- "Who points at X?". A secondary index on a WITHOUT ROWID table carries the
-- primary key, so this is really (to_id, relation, from_id) and covering —
-- dropping WITHOUT ROWID silently reinstates a row fetch per edge.
CREATE INDEX idx_edges_to ON edges(to_id, relation);

-- ============================================================
-- dependency_watermarks
-- Per-dependent staleness watermark: the store-wide write cursor
-- (resource_version_seq) as of the moment this object's last
-- successful reconcile loaded its state. A dependent is stale
-- when a target it depends_on has a resource_version above it.
-- ============================================================

-- A side table, not a column on objects: SQLite rewrites the whole record on
-- UPDATE, and objects rows carry the blobs inline.
--
-- Sparse by construction: a row exists only once a dependent has reconciled, and
-- an absent row means "never reconciled against a known point, therefore stale"
-- — which is why the scan LEFT JOINs. That absence is load-bearing on the write
-- side too: a new depends_on edge deletes the row (see EdgesAdd).
--
-- A rowid table (unlike edges): object_id aliases the rowid, so the per-edge
-- probe is a direct rowid seek.
--
-- ON DELETE CASCADE: derived state with no claim on the object's lifetime.
--
-- reconciled_at is observability only, and NOT a reconcile heartbeat: it moves
-- only when reconciled_against does (one WHERE guards both), and only
-- successful passes of dependents write it at all.
CREATE TABLE dependency_watermarks (
    object_id          INTEGER PRIMARY KEY REFERENCES objects(id) ON DELETE CASCADE,
    reconciled_against INTEGER NOT NULL, -- resource_version_seq value observed at load
    reconciled_at      INTEGER NOT NULL  -- millis; moves only with reconciled_against
) STRICT;

-- ============================================================
-- driver_cursors
-- Durable scan position for a periodic driver, keyed by driver
-- name. One row today: the dependency waker's write-log
-- watermark (see waker.go). Lets a driver resume where it
-- stopped across a restart instead of reseeding from "now".
-- ============================================================

-- WITHOUT ROWID: the key is TEXT, so a rowid table would store every name twice.
-- name identifies the driver — nothing for a foreign key to reference.
--
-- Single-writer: a second process sharing this database would steal pages from
-- the first's scan. A constraint to keep true, not a gap to close.
--
-- updated_at is guarded by the same WHERE as cursor, so a no-progress tick
-- dirties no page — load-bearing at the waker's cadence.
CREATE TABLE driver_cursors (
    name       TEXT PRIMARY KEY,
    cursor     INTEGER NOT NULL, -- resource_version scanned through, inclusive
    updated_at INTEGER NOT NULL  -- millis; moves only with cursor
) STRICT, WITHOUT ROWID;

-- ============================================================
-- events
-- Append-only per-object log, aggregated into contiguous runs:
-- one row per run of consecutive observations sharing
-- (object_id, category, type, reason). count/last_at grow while
-- the run holds; a key change appends a new row. category
-- partitions the log into independent timelines — a run is only
-- compared against the latest of its own (object_id, category),
-- so interleaved categories never break each other.
-- ============================================================

CREATE TABLE events (
    -- Run identity. AUTOINCREMENT so a retention-swept run's id is never reused.
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    object_id INTEGER NOT NULL REFERENCES objects(id) ON DELETE CASCADE,

    category TEXT NOT NULL DEFAULT '',   -- independent timeline within the object
    type     TEXT NOT NULL CHECK (type IN ('Normal', 'Warning')),
    reason   TEXT NOT NULL,              -- machine-readable token, e.g. "ProbeFailed"
    message  TEXT,                       -- human-readable; sampled (latest occurrence wins)
    detail   TEXT,                       -- opaque JSON payload; sampled (latest occurrence wins)

    count    INTEGER NOT NULL DEFAULT 1, -- occurrences coalesced into this run
    first_at INTEGER NOT NULL,           -- epoch ms of the first occurrence
    last_at  INTEGER NOT NULL,           -- epoch ms of the latest occurrence

    -- Draws from resource_version_seq like objects: the watch cursor / ordering key.
    resource_version INTEGER NOT NULL
) STRICT;

-- Newest-first read order for EventsList and EventsSweep's ring window. id
-- closes the tiebreak, making the order total — without it a limited EventsList
-- sorts the whole timeline. Does NOT serve the append-time probe (see
-- idx_events_latest).
CREATE INDEX idx_events_object_cat
    ON events(object_id, category, last_at DESC, id DESC);

-- The append-time probe: EventsAdd's "newest run" means greatest id (append
-- order, not clock order) — a different sort from the index above, not a prefix,
-- so one key cannot answer both. Without it the probe sorts and table-fetches
-- the whole timeline per event, unbounded with retention off. Extend writes
-- touch none of these columns, so only a new run inserts an entry.
CREATE INDEX idx_events_latest ON events(object_id, category, id DESC);

-- Covers EventsMaxVersion, EventsWatch's quiet-tick gate; it exists for that read
-- alone. Without it: one table fetch per run, past overflow chains.
CREATE INDEX idx_events_object_rv ON events(object_id, resource_version);

-- ============================================================
-- resource_version_seq
-- Monotonic global write cursor, decoupled from the objects table.
-- ============================================================

-- MAX(objects.resource_version) would reuse a version once the highest row is
-- deleted; a standalone counter only ever increments.
CREATE TABLE resource_version_seq (
    id    INTEGER PRIMARY KEY CHECK (id = 1), -- single row, always id = 1
    value INTEGER NOT NULL                    -- last resource_version handed out
) STRICT;

INSERT INTO resource_version_seq (id, value) VALUES (1, 0);
