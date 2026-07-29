-- Timestamps: INTEGER Unix-epoch milliseconds, UTC.
-- JSON blobs:  TEXT (spec, status, finalizers, event detail).
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
    -- observed_at records *when* the object settled at observed_generation — a
    -- handshake timestamp, not a reconcile heartbeat. It stops ticking once a
    -- generation is recorded (an UpdateStatus that changes neither the bytes nor
    -- the generation writes nothing at all), so it can't be read as "last ran".
    -- Controller liveness belongs in the events log.
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

    -- How many passes beehive owes this object. DependenciesAdd adds one when it
    -- detects its target moved between the caller's read and the declare (the
    -- read-then-declare race), so the wake survives a crash that loses the in-memory
    -- requeue; a successful reconcile subtracts the count it observed. 0 = nothing
    -- owed. A count (not a flag) so a wake owed while an earlier one is being
    -- reconciled is not lost: it lands above the observed count and survives the
    -- subtraction. Subtracting the whole observed count (not 1) is what drains a
    -- multi-wake row in the single pass the backstop schedules for it.
    --
    -- This is durable, owed *work*; the in-memory dependency waker is a separate
    -- mechanism and leaves nothing here. A second durable marker (undecodable rows,
    -- say) gets its own column and its own cadence — it does not join this count.
    reconcile_owed INTEGER NOT NULL DEFAULT 0,

    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,

    UNIQUE ("group", kind, slug)
) STRICT;

CREATE INDEX idx_objects_kind ON objects("group", kind);    -- list / resync a kind
CREATE INDEX idx_objects_rv   ON objects(resource_version); -- watch ordering

-- Finalizing objects, for the global GC sweeper. Keyed on (id, "group", kind)
-- rather than on deletion_requested_at because the sweeper's only query is
--
--   SELECT id, "group", kind FROM objects
--    WHERE deletion_requested_at IS NOT NULL ORDER BY id
--
-- (it needs the kind to route: registered kind -> enqueue for its controller,
-- client-only -> collect directly). This key covers that read and is already in
-- id order, so it plans as a plain `SCAN ... USING INDEX` with no row fetch and
-- no sort; keying on deletion_requested_at instead costs both. The partial WHERE
-- is what keeps the wider key cheap — only finalizing rows are indexed, and
-- id/group/kind are write-once, so entries appear when a delete is requested and
-- vanish when the row is collected, never updated in between.
CREATE INDEX idx_objects_deleting
    ON objects(id, "group", kind)
    WHERE deletion_requested_at IS NOT NULL;

-- Objects whose spec has not yet been fully reconciled by a controller.
CREATE INDEX idx_objects_unsettled
    ON objects("group", kind)
    WHERE observed_generation IS NULL OR observed_generation < generation;

-- Objects owed a durable dependency wake (see reconcile_owed). Separate from the
-- unsettled index because the two are orthogonal: an object can be spec-converged
-- yet still owe a wake. The backstop query (ReconcileOwedListIDs) rides this.
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

    -- Writer-declared classification:
    --   0 = store-truth  derived from persisted state; valid as-is across restart
    --   1 = liveness     derived from a live resource; valid only in the writing process
    -- Liveness rows: the read path compares updated_at against process start; a
    -- prior-process write surfaces as Unknown / "verifying" until a controller
    -- re-confirms it (which bumps updated_at). Default is store-truth; liveness is
    -- opt-in by the writer.
    liveness INTEGER NOT NULL DEFAULT 0 CHECK (liveness IN (0, 1)),

    transitioned_at INTEGER NOT NULL, -- epoch ms when status last CHANGED
    updated_at      INTEGER NOT NULL, -- epoch ms of last write (also the liveness stamp)

    PRIMARY KEY (object_id, type)
) STRICT;

-- Fetch all conditions for an object (status assembly, cascade delete).
CREATE INDEX idx_conditions_object ON conditions(object_id);

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

    -- WITHOUT ROWID: every column is in the key, so a rowid table would store each
    -- edge twice — once in the table, once in the automatic index enforcing this
    -- key. Here the key *is* the table. It also makes idx_edges_to below covering.
    PRIMARY KEY (from_id, to_id, relation)
) STRICT, WITHOUT ROWID;

-- Answers "who points at X?" for cascade-GC and wake-dependents. Reads from_id,
-- which the index appears not to hold — but a secondary index on a WITHOUT ROWID
-- table identifies rows by primary key, so this is really (to_id, relation,
-- from_id) and the probe never touches the table. The covering property lives in
-- the table's storage class, not here: dropping WITHOUT ROWID silently reinstates
-- a row fetch per edge with this line looking unchanged.
CREATE INDEX idx_edges_to ON edges(to_id, relation);

-- ============================================================
-- dependency_watermarks
-- Per-dependent staleness watermark: the store-wide write cursor
-- (resource_version_seq) as of the moment this object's last
-- successful reconcile loaded its state. A dependent is stale
-- when a target it depends_on has a resource_version above it.
-- ============================================================

-- A side table rather than a column on objects, for one reason: objects rows
-- carry the spec and status JSON inline, and SQLite rewrites the whole record
-- on UPDATE — including overflow pages. Storing eight bytes against a
-- multi-kilobyte row, on every successful reconcile of every dependent, is the
-- most expensive available way to keep a small integer. Here the write touches
-- a three-column row.
--
-- Sparse by construction: a row exists only once a dependent has reconciled,
-- so the table is sized by the dependency graph, not by the object count. An
-- absent row means the same thing a zero cursor would — never reconciled
-- against a known point, therefore stale — which is why the scan LEFT JOINs
-- and needs no backfill.
--
-- A rowid table, unlike edges. object_id is an INTEGER PRIMARY KEY, so it
-- *aliases the rowid*: the table is already one b-tree keyed by object_id with
-- no separate index, and the scan's per-edge probe is a direct rowid seek.
-- WITHOUT ROWID would demote object_id to an ordinary column stored in the
-- record payload, paying a header entry and serial type for a key the rowid
-- form stores as a bare varint. edges is WITHOUT ROWID for the opposite reason,
-- spelled out above: every one of its columns is in the key, so a rowid table
-- would store each edge twice. Here there are non-key columns and an integer
-- key, which is exactly the case the rowid form is best at.
--
-- ON DELETE CASCADE because this is derived state with no claim on the
-- object's lifetime — it disappears with the row, alongside the outgoing edges
-- that cascade for the same reason.
--
-- reconciled_at is observability only; nothing reads it to make a decision. It
-- moves only when reconciled_against does, which the upsert enforces with a
-- single WHERE over both columns rather than by discipline. Two consequences,
-- and both are the same caution objects.observed_at already gives above:
--
--   * It is NOT a reconcile heartbeat and cannot be read as "last ran". It
--     stops moving whenever a pass reconciles against a store nobody has
--     written to since the last one — rare when the store is busy, routine
--     when it is quiet. Controller liveness belongs in the events log.
--   * Its coverage is asymmetric: only dependents get a row at all, and only
--     successful passes write one. Anything built on it silently omits every
--     object without dependencies and every failing reconcile.
--
-- No index beyond the primary key: every access is by object_id.
CREATE TABLE dependency_watermarks (
    object_id          INTEGER PRIMARY KEY REFERENCES objects(id) ON DELETE CASCADE,
    reconciled_against INTEGER NOT NULL, -- resource_version_seq value observed at load
    reconciled_at      INTEGER NOT NULL  -- millis; moves only with reconciled_against
) STRICT;

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
    detail   TEXT,                       -- opaque JSON payload; sampled (latest occurrence wins)

    count    INTEGER NOT NULL DEFAULT 1, -- occurrences coalesced into this run
    first_at INTEGER NOT NULL,           -- epoch ms of the first occurrence
    last_at  INTEGER NOT NULL,           -- epoch ms of the latest occurrence

    -- Draws from resource_version_seq like objects: the watch cursor / ordering key.
    resource_version INTEGER NOT NULL
) STRICT;

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
) STRICT;

INSERT INTO resource_version_seq (id, value) VALUES (1, 0);
