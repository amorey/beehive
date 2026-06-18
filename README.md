# Beehive

*Beehive is an self-healing, eventually consistent datastore for Go apps that takes inspiration from the stigmergic cooperation of bees in a beehive.*

<img width="435" alt="beehive" src="https://github.com/user-attachments/assets/f5b845df-6ed0-47f3-b1be-69d3f2286d9f" />

## Introduction

Beehive is an embedded control plane for Go apps, backed by a durable store. With Beehive, you define desired state as objects and register controllers that reconcile actual state toward it. The system is self-healing which means it converges on restart, tolerates missed events, and handles cascading dependencies without controllers calling each other. The architecture is heavily influenced by Kubernetes and takes inspiration from the stigmergic cooperation of bees in a beehive.

## Quickstart

```go
package main

import (
  "context"
  "log"
  "time"

  "github.com/amorey/beehive"
  "github.com/amorey/beehive/store"
)

var ClusterGroupKind = beehive.GroupKind{
  Group: "kstack.sh",
  Kind:  "Cluster",
}

type ClusterSpec struct {
  // TODO: define desired state fields
}

type ClusterStatus struct {
  // TODO: define observed state fields
}

type ClusterController struct {
  client beehive.ControllerClient[ClusterStatus]
}

func (cc *ClusterController) Start(ctx context.Context, client beehive.ControllerClient[ClusterStatus]) error {
  cc.client = client
  // TODO: start background workers (e.g. connection pool, watchers)
  return nil
}

func (cc *ClusterController) Stop(ctx context.Context) error {
  // TODO: shut down background workers
  return nil
}

func (cc *ClusterController) Reconcile(ctx context.Context, obj *beehive.Object[ClusterSpec, ClusterStatus]) (beehive.Result, error) {
  // Handle deletion: object is finalizing when DeletionRequestedAt is set.
  // Remove any external resources, then clear the finalizer to allow the row to be deleted.
  if obj.DeletionRequestedAt != nil {
    // TODO: clean up external resources for obj.Spec
    // TODO: remove finalizer: return beehive.Result{}, cc.client.DeleteFinalizer(ctx, obj.ID, "kstack.sh/cluster")
    return beehive.Result{}, nil
  }

  // TODO: reconcile obj.Spec against actual state (e.g. create/update external resources)
  // If the resource is not yet ready, requeue to check again later:
  // return beehive.Result{RequeueAfter: 5 * time.Second}, nil

  // TODO: update observed state
  // return beehive.Result{}, cc.client.UpdateStatus(ctx, obj.ID, ClusterStatus{})

  return beehive.Result{}, nil
}

func main() {
  store, _ := store.OpenSQLite(context.TODO(), "/path/to/beehive.db")
  defer store.Close()

  bh, _ := beehive.New(store)
  beehive.Register(bh, ClusterGroupKind, &ClusterController{})

  if err := bh.Start(); err != nil {
    log.Fatal(err)
  }

  ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
  defer cancel()
  bh.Stop(ctx)
}
```

## Architecture

- **Declarative core.** Users write `spec` (desired state); controllers continuously reconcile actual state toward it. Reconciliation is level-triggered — driven by current state, not event sequences — so the system self-heals on restart and is robust to missed events. A cold start is just a reconcile from persisted desired state.

- **Coordination through the store.** Controllers never call each other. They read/write the shared store and wake on change-events; a periodic resync catches anything dropped. Events are a latency optimization, not a correctness dependency.

- **`spec`/`status` separation.** Only controllers may write `status`. This is structural in the API: the user-facing `Client` surface has no status-write path; only the `Controller` surface does.

- **Internal versioning.** `current_version` in the schema tracks Beehive's own serialization format, not user-defined spec versions. Migrations run automatically on `store.OpenSQLite`. User-defined spec types are opaque to Beehive — versioning of `Spec`/`Status` structs is the application's responsibility.

## API

### Beehive

```go
func New(store store.Store, opts ...Option) (*Beehive, error)
func Register[Spec, Status any](bh *Beehive, gk GroupKind, c Controller[Spec, Status], opts ...Option) error
```

Options are dispatched by caller type — `WithResyncInterval` passed to `New` sets the global default; passed to `Register` it overrides for that controller only. Unrecognised options for a given caller are ignored.

### GroupKind

```go
type GroupKind struct {
    Group string // "" for core group, "acme.com" for plugins
    Kind  string
}
```

### Condition

```go
type ConditionStatus string

const (
    ConditionTrue    ConditionStatus = "True"
    ConditionFalse   ConditionStatus = "False"
    ConditionUnknown ConditionStatus = "Unknown"
)

type Condition struct {
    Type    string
    Status  ConditionStatus
    Reason  string // machine-readable token, e.g. "DialTimeout"
    Message string // human-readable detail
}
```

### Object

```go
type ObjectID = int64

type Object[Spec, Status any] struct {
    ID                  ObjectID
    Group               string
    Kind                string
    Name                *string  // nil for internally-generated objects
    Spec                Spec
    Status              *Status
    Generation          int64
    ObservedGeneration  *int64
    ObservedAt          *time.Time
    ResourceVersion     int64
    DeletionRequestedAt *time.Time
    Finalizers          []string
    CreatedAt           time.Time
    UpdatedAt           time.Time
}
```

### Result

```go
type Result struct {
    RequeueAfter time.Duration // zero means no requeue
}
```

### Client

```go
type WatchEventType string

const (
    WatchEventAdded    WatchEventType = "Added"
    WatchEventModified WatchEventType = "Modified"
    WatchEventDeleted  WatchEventType = "Deleted"
)

type WatchEvent[Spec, Status any] struct {
    Type   WatchEventType
    Object *Object[Spec, Status]
}

type Client[Spec, Status any] interface {
    Create(ctx context.Context, spec Spec, opts ...Option) (*Object[Spec, Status], error)
    Update(ctx context.Context, id ObjectID, spec Spec) (*Object[Spec, Status], error)
    Get(ctx context.Context, id ObjectID) (*Object[Spec, Status], error)
    GetByName(ctx context.Context, name string) (*Object[Spec, Status], error)
    List(ctx context.Context) ([]*Object[Spec, Status], error)
    Delete(ctx context.Context, id ObjectID) error
    Watch(ctx context.Context, id ObjectID) (<-chan WatchEvent[Spec, Status], error)
    WatchList(ctx context.Context) (<-chan WatchEvent[Spec, Status], error)
}

func NewClient[Spec, Status any](bh *Beehive, gk GroupKind) Client[Spec, Status]
```

`Create` generates a name unless `beehive.WithName` is provided. If a name is given and already exists, `Create` fails. All subsequent operations use `ObjectID` — safe against operating on a different incarnation after a delete/recreate. Finalizers and other metadata are set via options:

```go
client := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
obj, _ := client.Create(ctx, ClusterSpec{...}, beehive.WithName("prod-cluster"), beehive.WithFinalizers("kstack.sh/cluster"))
client.Update(ctx, obj.ID, ClusterSpec{...})
```

`Watch` and `WatchList` emit the current state as `Added` events on start, then stream subsequent changes. The channel closes when `ctx` is cancelled.

### ControllerClient

```go
type ControllerClient[Status any] interface {
    UpdateStatus(ctx context.Context, id ObjectID, status Status) error
    SetCondition(ctx context.Context, id ObjectID, condition Condition) error
    DeleteCondition(ctx context.Context, id ObjectID, conditionType string) error
    DeleteFinalizer(ctx context.Context, id ObjectID, finalizer string) error
    AddDependency(ctx context.Context, fromID, toID ObjectID) error
    DeleteDependency(ctx context.Context, fromID, toID ObjectID) error
}
```

### Controller

```go
type Controller[Spec, Status any] interface {
    Start(ctx context.Context, client ControllerClient[Status]) error
    Stop(ctx context.Context) error
    Reconcile(ctx context.Context, obj *Object[Spec, Status]) (Result, error)
}
```

Beehive wraps each `Reconcile` call in a transaction, committing on `nil` error and rolling back on non-nil. All `ControllerClient` calls made with the reconcile `ctx` participate in that transaction automatically. Do all external I/O before writing to the store — holding a write transaction open during a network call blocks all other writers.

A non-nil error triggers an automatic retry with exponential backoff starting at 1s and capped at 30s by default. Configurable per-controller with `WithMaxRetryInterval`.

### Options

```go
type Option interface{ apply(any) }

func WithName(name string) Option                  // set a human-readable name; fails if already exists
func WithFinalizers(f ...string) Option            // declare finalizers before the object is visible to controllers
func WithOwner(id ObjectID) Option                 // declare owned_by edge; owner cannot be deleted while this object exists
func WithResyncInterval(d time.Duration) Option    // override the default resync interval
func WithMaxRetryInterval(d time.Duration) Option  // cap on exponential backoff after Reconcile errors (default: 30s)
```

`WithOwner` sets an `owned_by` edge in `refs` atomically with the `Create` call. When the owner is deleted, Beehive triggers deletion of the child via the GC reconciler.

`AddDependency` and `DeleteDependency` on `ControllerClient` manage `depends_on` edges during reconcile. When a target's conditions change, Beehive automatically requeues the dependent. Both calls participate in the reconcile transaction.

## Schema (SQLite)

Timestamps: `INTEGER` Unix-epoch milliseconds, UTC. `spec`/`status`: JSON in `TEXT`. Core group: `""` (never NULL). All identity/foreign keys: `INTEGER`. Requires `PRAGMA foreign_keys = ON`.

### objects

```sql
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
    -- Unique within (group, kind); SQLite NULL != NULL so multiple NULL names are allowed.
    name TEXT,

    -- Beehive internal serialization version. Bumped by Beehive migrations only;
    -- opaque to user-defined Spec/Status types.
    current_version TEXT NOT NULL,

    spec   TEXT NOT NULL, -- JSON, user-owned,        HARD / desired state
    status TEXT,          -- JSON, controller-owned,  SOFT / observed state (nullable)

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

    UNIQUE ("group", kind, name)
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

-- Objects whose serialization version predates the current Beehive version (need migration).
CREATE INDEX idx_objects_stale_encoding
    ON objects("group", kind, current_version);
```

### conditions

```sql
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

    observed_generation INTEGER,       -- generation this condition was evaluated against
    last_transition     INTEGER NOT NULL, -- epoch ms when status last CHANGED
    updated_at          INTEGER NOT NULL, -- epoch ms of last write (also the liveness stamp)

    PRIMARY KEY (object_id, type)
);

-- Answers "who points at X?" for cascade-GC and wake-dependents.
CREATE INDEX idx_conditions_object ON conditions(object_id);
```

### refs

```sql
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
```
