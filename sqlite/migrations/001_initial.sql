CREATE TABLE objects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    "group" TEXT NOT NULL,
    kind    TEXT NOT NULL,

    name TEXT,

    current_version TEXT NOT NULL,

    spec   TEXT NOT NULL,
    status TEXT,

    generation          INTEGER NOT NULL DEFAULT 1,
    observed_generation INTEGER,
    observed_at         INTEGER,

    resource_version INTEGER NOT NULL,

    deletion_requested_at INTEGER,
    finalizers            TEXT NOT NULL DEFAULT '[]',

    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,

    UNIQUE ("group", kind, name)
);

CREATE INDEX idx_objects_kind ON objects("group", kind);
CREATE INDEX idx_objects_rv   ON objects(resource_version);

CREATE INDEX idx_objects_deleting
    ON objects(deletion_requested_at)
    WHERE deletion_requested_at IS NOT NULL;

CREATE INDEX idx_objects_unsettled
    ON objects("group", kind)
    WHERE observed_generation IS NULL OR observed_generation < generation;

CREATE INDEX idx_objects_stale_encoding
    ON objects("group", kind, current_version);

CREATE TABLE conditions (
    object_id INTEGER NOT NULL REFERENCES objects(id) ON DELETE CASCADE,

    type    TEXT NOT NULL,
    status  TEXT NOT NULL CHECK (status IN ('True', 'False', 'Unknown')),
    reason  TEXT,
    message TEXT,

    liveness INTEGER NOT NULL DEFAULT 0,

    observed_generation INTEGER,
    last_transition     INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,

    PRIMARY KEY (object_id, type)
);

CREATE INDEX idx_conditions_object ON conditions(object_id);

CREATE TABLE refs (
    from_id INTEGER NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    to_id   INTEGER NOT NULL REFERENCES objects(id) ON DELETE RESTRICT,

    relation TEXT NOT NULL CHECK (relation IN ('owned_by', 'depends_on')),

    PRIMARY KEY (from_id, to_id, relation)
);

CREATE INDEX idx_refs_to ON refs(to_id, relation);
