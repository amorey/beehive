-- Timestamps: INTEGER Unix-epoch milliseconds, UTC.
-- JSON blobs:  TEXT (spec, status, finalizers).
-- Core group:  empty string "" (never NULL).
-- Requires:    PRAGMA foreign_keys = ON.

-- ============================================================
-- objects
-- One row per GVK-identified object.
-- ============================================================

CREATE TABLE objects (
    -- Incarnation identity. AUTOINCREMENT (not plain rowid) is required:
    -- a recycled id would break ABA safety on delete/recreate. int64 in Go;
    -- 0 is the "not yet persisted" sentinel.
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- "" = core group, "acme.com" = plugin.
    "group" TEXT NOT NULL,
    kind    TEXT NOT NULL,

    -- NULL for internally-generated objects; set for user-named objects (e.g. kubeconfig entries).
    -- Immutable — a rename is delete+recreate.
    -- Unique within (group, kind); SQLite NULL != NULL so multiple NULL slugs are allowed.
    slug TEXT,

    spec   TEXT NOT NULL, -- JSON, user-owned,        HARD / desired state
    status TEXT,          -- JSON, controller-owned,  SOFT / observed state (nullable)

    -- Per-column migrator schema versions: the schema version each blob was last
    -- written at. Opaque to the store (like resource_version) — the generic layer's
    -- Migrator converts a blob from its stored version on read. 0 = not versioned
    -- (the kind hasn't opted in), which is why both default to 0.
    schema_version_spec   INTEGER NOT NULL DEFAULT 0,
    schema_version_status INTEGER NOT NULL DEFAULT 0,

    -- Convergence handshake. generation bumps only on a spec change.
    -- observed_generation is the last generation a reconciler finished;
    -- observed_generation == generation means "applied" (spec progress, not liveness).
    -- observed_at gates the SETTLED indicator: a value older than the current process
    -- start (or NULL) surfaces as "verifying" — spec progress is durable, but not yet
    -- re-confirmed by a controller in this process.
    generation          INTEGER NOT NULL DEFAULT 1,
    observed_generation INTEGER,
    observed_at         INTEGER,

    -- Global monotonic write cursor. Used as a watch cursor, CAS token, and no-op
    -- suppression guard (bumped only on a real change).
    -- Distinct from id: id = incarnation identity; resource_version = mutation cursor.
    resource_version INTEGER NOT NULL,

    -- Async delete: deletion_requested_at set ⇒ finalizing;
    -- row removed only once finalizers clears to [].
    deletion_requested_at INTEGER,
    finalizers            TEXT NOT NULL DEFAULT '[]', -- JSON array of finalizer names

    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,

    UNIQUE ("group", kind, slug)
);

CREATE INDEX idx_objects_kind ON objects("group", kind);    -- list / resync a kind
CREATE INDEX idx_objects_rv   ON objects(resource_version); -- watch ordering

CREATE INDEX idx_objects_deleting
    ON objects(deletion_requested_at)
    WHERE deletion_requested_at IS NOT NULL;

-- Objects whose spec has not yet been fully reconciled by a controller.
CREATE INDEX idx_objects_unsettled
    ON objects("group", kind)
    WHERE observed_generation IS NULL OR observed_generation < generation;

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

    -- Writer-declared classification:
    --   0 = store-truth  derived from persisted state; valid as-is across restart
    --   1 = liveness     derived from a live resource; valid only in the writing process
    -- Liveness rows: the read path compares updated_at against process start; a
    -- prior-process write surfaces as Unknown / "verifying" until a controller
    -- re-confirms it (which bumps updated_at). Default is store-truth; liveness is
    -- opt-in by the writer.
    liveness INTEGER NOT NULL DEFAULT 0,

    transitioned_at INTEGER NOT NULL, -- epoch ms when status last CHANGED
    updated_at      INTEGER NOT NULL, -- epoch ms of last write (also the liveness stamp)

    PRIMARY KEY (object_id, type)
);

-- Fetch all conditions for an object (status assembly, cascade delete).
CREATE INDEX idx_conditions_object ON conditions(object_id);

-- ============================================================
-- refs
-- Dependency-tree edges. Both endpoints are hard integer FKs
-- into objects(id) — ids are never reused, so stale targets
-- are impossible by construction.
-- ============================================================

CREATE TABLE refs (
    -- dependent / child.  ON DELETE CASCADE: removing the child drops its outgoing edges.
    from_id INTEGER NOT NULL REFERENCES objects(id) ON DELETE CASCADE,

    -- target / owner.  ON DELETE RESTRICT: a target cannot be physically removed while
    -- edges still point at it, and an edge cannot point at a nonexistent object.
    -- No to_uid soft guard or re-adoption machinery needed.
    to_id INTEGER NOT NULL REFERENCES objects(id) ON DELETE RESTRICT,

    -- owned_by   deleting `to` cascades to `from` (foreground, via the GC reconciler)
    -- depends_on `to` going NotReady ⇒ `from` requeued automatically by Beehive
    relation TEXT NOT NULL CHECK (relation IN ('owned_by', 'depends_on')),

    PRIMARY KEY (from_id, to_id, relation)
);

-- Answers "who points at X?" for cascade-GC and wake-dependents.
CREATE INDEX idx_refs_to ON refs(to_id, relation);

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
    -- Run identity. AUTOINCREMENT so a physically-deleted (retention-swept) run's
    -- id is never reused as a UI row key. int64 in Go.
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    object_id INTEGER NOT NULL REFERENCES objects(id) ON DELETE CASCADE,

    category TEXT NOT NULL DEFAULT '',   -- independent timeline within the object
    type     TEXT NOT NULL CHECK (type IN ('Normal', 'Warning')),
    reason   TEXT NOT NULL,              -- machine-readable token, e.g. "ProbeFailed"
    message  TEXT,                       -- human-readable; sampled (latest occurrence wins)
    detail   BLOB,                       -- opaque JSON payload; sampled (latest occurrence wins)

    count    INTEGER NOT NULL DEFAULT 1, -- occurrences coalesced into this run
    first_at INTEGER NOT NULL,           -- epoch ms of the first occurrence
    last_at  INTEGER NOT NULL,           -- epoch ms of the latest occurrence

    -- Draws from resource_version_seq like objects: the watch cursor / ordering key.
    resource_version INTEGER NOT NULL
);

-- Serves both the append-time "latest run for (object, category)" probe and the
-- newest-first panel query (ORDER BY last_at DESC).
CREATE INDEX idx_events_object_cat ON events(object_id, category, last_at DESC);

-- Watch ordering (mirrors idx_objects_rv).
CREATE INDEX idx_events_rv ON events(resource_version);

-- ============================================================
-- resource_version_seq
-- Monotonic global write cursor, decoupled from the objects table.
-- ============================================================

-- Deriving the next resource_version from MAX(objects.resource_version) lets a
-- version be reused once the highest-versioned row is physically deleted, which
-- breaks its use as a watch cursor / CAS token. A standalone single-row counter
-- only ever increments, regardless of row deletions, so versions are never reused.
CREATE TABLE resource_version_seq (
    id    INTEGER PRIMARY KEY CHECK (id = 1), -- single row, always id = 1
    value INTEGER NOT NULL                    -- last resource_version handed out
);

INSERT INTO resource_version_seq (id, value) VALUES (1, 0);
