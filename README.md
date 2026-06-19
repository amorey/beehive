# Beehive

*Beehive is an embedded, self-healing, eventually consistent datastore for Go that takes inspiration from the stigmergic cooperation of bees in a beehive.*

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

func (cc *ClusterController) Start(client beehive.ControllerClient[ClusterStatus]) error {
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
  // return beehive.Result{}, cc.client.UpdateStatus(ctx, obj.ID, obj.Generation, ClusterStatus{})

  return beehive.Result{}, nil
}

func main() {
  store, _ := store.OpenSQLite("/path/to/beehive.db")
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
    Type     string
    Status   ConditionStatus
    Reason   string // machine-readable token, e.g. "DialTimeout"
    Message  string // human-readable detail
    Liveness bool   // see below
}
```

`Liveness` marks a condition derived from a live, in-process resource: it is valid
only within the process that wrote it. A liveness condition written by a prior
process is downgraded to `ConditionUnknown` ("verifying") on read until a controller
re-confirms it. The default (`false`) is durable store-truth that survives restarts.

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
    Conditions          []Condition // per-type observations reported by controllers
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
    UpdateStatus(ctx context.Context, id ObjectID, observedGeneration int64, status Status) error
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
    Start(client ControllerClient[Status]) error
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
