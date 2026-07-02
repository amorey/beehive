# Beehive

*Beehive is an embedded, durable, self-healing control-plane for Go apps that takes inspiration from Kubernetes and the stigmergic cooperation of bees in a beehive.*

<img width="435" alt="beehive" src="https://github.com/user-attachments/assets/f5b845df-6ed0-47f3-b1be-69d3f2286d9f" />

[![Go Reference](https://pkg.go.dev/badge/github.com/amorey/beehive.svg)](https://pkg.go.dev/github.com/amorey/beehive)
![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)

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
  "github.com/amorey/beehive/sqlite"
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

type ClusterController struct{}

func (cc *ClusterController) Reconcile(ctx context.Context, client beehive.ControllerClient[ClusterStatus], obj *beehive.Object[ClusterSpec, ClusterStatus]) (beehive.Result, error) {
  // Handle deletion: object is finalizing when DeletionRequestedAt is set.
  // Remove any external resources, then clear the finalizer to allow the row to be deleted.
  if obj.DeletionRequestedAt != nil {
    // TODO: clean up external resources for obj.Spec
    // TODO: remove finalizer: return beehive.Result{}, client.DeleteFinalizer(ctx, obj.ID, "kstack.sh/cluster")
    return beehive.Result{}, nil
  }

  // TODO: reconcile obj.Spec against actual state (e.g. create/update external resources)
  // If the resource is not yet ready, requeue to check again later:
  // return beehive.Result{RequeueAfter: 5 * time.Second}, nil

  // TODO: update observed state
  // return beehive.Result{}, client.UpdateStatus(ctx, obj.ID, obj.Generation, ClusterStatus{})

  return beehive.Result{}, nil
}

func main() {
  store, _ := sqlite.Open("/path/to/beehive.db")
  defer store.Close()

  bh, _ := beehive.New(store)
  // Register returns the kind's ControllerClient for out-of-band status writes
  // from your own goroutines (background work belongs to the app, not beehive);
  // ignore it if Reconcile is your only writer.
  _, _ = beehive.Register(bh, ClusterGroupKind, &ClusterController{})

  stop, err := bh.Start(context.Background())
  if err != nil {
    log.Fatal(err)
  }

  ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
  defer cancel()
  if err := stop(ctx); err != nil {
    log.Printf("beehive: shutdown did not drain cleanly: %v", err)
  }
}
```

## Architecture

- **Declarative core.** Users write `spec` (desired state); controllers continuously reconcile actual state toward it. Reconciliation is level-triggered — driven by current state, not event sequences — so the system self-heals on restart and is robust to missed events. A cold start is just a reconcile from persisted desired state.

- **Coordination through the store.** Controllers never call each other. They read/write the shared store and wake on change-events; a periodic resync catches anything dropped. Events are a latency optimization, not a correctness dependency.

- **`spec`/`status` separation.** Only controllers may write `status`. This is structural in the API: the user-facing `Client` surface has no status-write path; only the `Controller` surface does.

- **Schema-version migration.** `Spec` and `Status` are opaque JSON, so reshaping a struct would break decode of older rows. A per-kind `Migrator` converts an old blob up *on read*, before unmarshal. Spec and Status version and convert independently; conversion is lazy — re-stamped only when the blob is next written, never by a bulk rewrite.

## API

### Beehive

```go
func New(store Store, opts ...Option) (*Beehive, error)
func Register[Spec, Status any](bh *Beehive, gk GroupKind, c Controller[Spec, Status], opts ...Option) (ControllerClient[Status], error)
```

`Register` returns the kind's `ControllerClient` — the status-write surface — so the embedding application can write status out-of-band (e.g. from its own goroutines) without beehive handing it over via a callback. A `ControllerClient` is obtainable *only* by registering a controller for that kind, which keeps the "only the owning controller writes its status" boundary intact.

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

### Event

Events are a per-object, append-only log of observations, aggregated into runs. Consecutive emissions that share `(Category, Type, Reason)` coalesce into a single `Event` whose `Count` grows and whose `[FirstAt, LastAt]` window widens; a change in any of those fields starts a new run. This is *contiguous-run* aggregation, not global dedup — a value that recurs after a different value starts a fresh run, so a flapping object yields a timeline of alternating runs rather than one ever-growing row. An event log is the un-collapsed sibling of a Condition: where a Condition keeps only the current run per type (its `Status`/`Reason` overwritten in place), the event log keeps the whole history.

```go
type EventType string

const (
    EventNormal  EventType = "Normal"  // ✓
    EventWarning EventType = "Warning" // ✗
)

type EventID = int64

type EventSpec struct {
    Category string    // independent timeline; "" = default
    Type     EventType
    Reason   string    // machine-readable token, e.g. "ProbeFailed"
    Message  string    // human-readable; sampled, not keyed
    Detail   any       // optional payload; marshaled on write; nil = none
}

type Event struct {
    ID       EventID
    ObjectID ObjectID        // object this event is about
    Category string
    Type     EventType
    Reason   string
    Message  string          // latest occurrence's message
    Detail   json.RawMessage // latest occurrence's payload; nil = none
    Count    int             // occurrences in this run (>= 1)
    FirstAt  time.Time       // run start
    LastAt   time.Time       // run end (latest occurrence)
}

// EventDetail unmarshals e.Detail into T.
func EventDetail[T any](e Event) (T, error)
```

`Category` partitions the aggregation domain: each `(object, category)` is an independent timeline, so unrelated concerns on the same object — say connection probes and config sync — never break each other's runs. Both `Category` and `Reason` are free labels the app chooses per emit (`string`, like `Condition.Reason`); declare typed-string constants for a closed, typo-safe vocabulary if you like. `Message` is *sampled*, not keyed: re-emitting the same `(Category, Type, Reason)` with a new message extends the current run and updates the shown message rather than starting a new one.

`Detail` is the machine-readable companion to `Message` — an optional structured payload (`ProbeFailed` might carry `{"endpoint":"10.0.0.1:443","latencyMs":5000}`). It follows the `Spec`/`Status` convention of **typed in, opaque out**: on write it is any JSON-marshalable value (`RecordEvent` marshals it, just as `Create` marshals `Spec`); on read it returns as `json.RawMessage`, decoded on demand with the free generic helper `EventDetail[T](e)` — applied per event with the type its `Reason` implies, so a single timeline can mix reasons carrying different detail shapes without the API becoming generic. Like `Message`, `Detail` is *sampled* (latest occurrence wins) and not part of the run key, so a varying payload never fragments a run — if you need every occurrence's payload retained, that event shouldn't aggregate (use a unique `Reason`). Unlike `Spec`/`Status`, `Detail` is **not** schema-versioned; reshaping it breaks decode of older rows, which is acceptable only because retention ages events out — version inside the payload if you need forward-compatibility.

Only controllers write events — `ControllerClient.RecordEvent` is the sole write path, because events are observations and (like `status`) have no user-facing writer. Reads are on the `Client` (`ListEvents` / `WatchEvents` / `GetLatestEvent`), plus the eager `LoadEvents()` / `Object.ListEvents()` pair that follows the same loaded-gating as the secondary lookups (`ErrNotLoaded` when not requested).

A connection-health panel renders one category's timeline directly — `client.ListEvents(ctx, id, WithEventCategory("connection"))` yields, newest first:

```
10:08:30  ✓ Connected      ×4    10:08:00–10:08:30
10:07:50  ✗ ProbeFailed    ×18   10:05:00–10:07:50   "i/o timeout"
10:04:55  ✓ Connected      ×7    10:03:50–10:04:55
```

where each row is one `Event`: `LastAt` · `Type` · `Reason` · `Count` · `FirstAt–LastAt` · `Message`.

### Object

```go
type ObjectID = int64

type Object[Spec, Status any] struct {
    ID                  ObjectID
    Group               string
    Kind                string
    Slug                *string  // nil for internally-generated objects
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

    // Secondary lookups (owner, dependencies, dependents, owned) are held in
    // unexported fields, populated only for the relations a read requested (see
    // Load options) and reached through the accessors below — never as fields.
}

type Ref = storeapi.Referrer // { ID ObjectID; Group, Kind string }
```

The secondary-lookup data is filled only when the read asked for it. Read it through the accessors, which return `ErrNotLoaded` if the relation wasn't requested — so forgetting the `Load*()` option fails loudly instead of looking empty. The verb tracks cardinality: `Get` for the at-most-one owner, `List` for the zero-or-more relations — matching the `Client`/`ControllerClient` lookups below:

```go
func (o *Object[Spec, Status]) GetOwner() (Ref, bool, error) // bool: an owner exists; err: not loaded
func (o *Object[Spec, Status]) ListDependencies() ([]Ref, error)
func (o *Object[Spec, Status]) ListDependents() ([]Ref, error)
func (o *Object[Spec, Status]) ListOwned() ([]Ref, error)
```

Once loaded, an empty slice (or `GetOwner`'s `ok == false`) means genuinely none. `ErrNotLoaded` is caller misuse — fetch the relation eagerly with the `Load*()` option, or lazily via the `Client`/`ControllerClient` methods below.

### Result

```go
type Result struct {
    RequeueAfter time.Duration // zero means no requeue
}
```

### Client

```go
type ChangeType string

const (
    Added    ChangeType = "Added"
    Modified ChangeType = "Modified"
    Deleted  ChangeType = "Deleted"
)

type Change[Spec, Status any] struct {
    Type   ChangeType
    Object *Object[Spec, Status]
}

type Client[Spec, Status any] interface {
    Create(ctx context.Context, spec Spec, opts ...Option) (*Object[Spec, Status], error)
    Update(ctx context.Context, id ObjectID, spec Spec) (*Object[Spec, Status], error)
    Get(ctx context.Context, id ObjectID, loads ...LoadOption) (*Object[Spec, Status], error)
    GetBySlug(ctx context.Context, slug string, loads ...LoadOption) (*Object[Spec, Status], error)
    List(ctx context.Context, loads ...LoadOption) ([]*Object[Spec, Status], error)
    Delete(ctx context.Context, id ObjectID) error
    Watch(ctx context.Context, id ObjectID) (<-chan Change[Spec, Status], error)
    WatchList(ctx context.Context) (<-chan Change[Spec, Status], error)

    // Lazy secondary lookups — the on-demand counterparts to the Load options.
    GetOwner(ctx context.Context, id ObjectID) (Ref, bool, error)
    ListDependencies(ctx context.Context, id ObjectID) ([]Ref, error)
    ListDependents(ctx context.Context, id ObjectID) ([]Ref, error)
    ListOwned(ctx context.Context, id ObjectID) ([]Ref, error)

    // Event log — per-object, category-partitioned, contiguous-run aggregated.
    ListEvents(ctx context.Context, id ObjectID, opts ...EventOption) ([]Event, error)
    WatchEvents(ctx context.Context, id ObjectID, opts ...EventOption) (<-chan Event, error)
    GetLatestEvent(ctx context.Context, id ObjectID, category string) (Event, bool, error)

    // Reconcile control.
    Requeue(ctx context.Context, id ObjectID, opts ...RequeueOption) error // requeue now; preserves backoff unless WithResetBackoff()
    NextRequeueAt(ctx context.Context, id ObjectID) time.Time              // next scheduled requeue (zero if none)
}

func NewClient[Spec, Status any](bh *Beehive, gk GroupKind) Client[Spec, Status]
```

#### Secondary lookups (owner / dependencies / dependents / owned)

An object's ref edges are fetched on request, two ways:

- **Eager** — pass `LoadOption`s to a read: `Get(ctx, id, LoadOwner())`, `List(ctx, LoadDependencies(), LoadDependents())`. The returned objects carry the data (read via the accessors). On `List` each relation is one batched query, not one per object.
- **Lazy** — call `GetOwner` / `ListDependencies` / `ListDependents` / `ListOwned` when the data is actually needed. These hit the edge query directly and do **not** kind-scope `id` (no validating read in front): a foreign id reads that kind's edges and a missing id reads empty, neither as `ErrNotFound`. Reserve them for ids the client owns.

`ListOwned` (and the eager `LoadOwned()` / `Object.ListOwned()`) is the inverse of `GetOwner` over `owned_by`: it returns the objects a given owner owns, the same way `ListDependents` inverts `ListDependencies` over `depends_on`.

Both issue the same secondary query (edges are a separate indexed lookup, never joined into the object's blob-bearing `SELECT`); eager just attaches the result to the object and batches across a `List`.

#### Reconcile control

`Requeue` requeues an object for immediate reconcile — the manual counterpart to the store-write and dependency-change wakes. It is a **latency hint, not a synchronous run**: it returns once the object is enqueued, and a worker reconciles it on its own schedule. Correctness never depends on it — the periodic resync remains the backstop — so a missed or coalesced requeue is harmless. Use it to promptly re-examine an object after out-of-band state the controller reads has changed.

By default `Requeue` **preserves the object's retry backoff ladder**. A requeue is the ordinary event-driven nudge (config change, dependency update, manual poke) and almost never proves the failing condition is resolved; the only event that proves recovery is a successful reconcile, which already clears backoff. The invariant: **backoff is cleared by a successful reconcile or an explicit `WithResetBackoff()`, never by a plain requeue.** Pass `beehive.WithResetBackoff()` only when the caller knows the failure is resolved and the next retry should restart from the base interval — the analog of controller-runtime's `Forget`. (This mirrors controller-runtime's split between `Add`/`AddAfter`, which requeue without resetting, and `Forget`, which explicitly resets.)

`NextRequeueAt` reports when the reconcile loop has, **in advance, scheduled the object to be requeued**: a pending backoff retry or `RequeueAfter` delay, or now if it is already queued. It returns the **zero time** when no requeue is scheduled.

This is the next *scheduled requeue* — not a prediction of the next reconcile. By design it sees only per-id timers, so it does **not** account for any other wake:

- the **periodic resync** (kind-wide, conditional on the object being unsettled),
- **dependency-change** wakes, **store-write** enqueues, or a `Requeue`.

So the actual next reconcile can be **earlier** than reported, and a **zero time means "nothing scheduled", not "will not reconcile"** — an unsettled object with no pending timer is still picked up by the next resync tick. Treat it as observability, not a guarantee.

`Requeue` validates the id against the client's kind first (`ErrNotFound` for a missing or foreign id), then requires a registered controller (`ErrNoController` for a client-only kind, which has no reconcile loop to schedule against). `NextRequeueAt` does neither: it reads in-memory schedule state directly, so it returns no error — a missing, foreign, or client-only id simply reads as the zero time, indistinguishable from a real object with nothing scheduled. Both are `Client`-only — a controller schedules itself with `Result.RequeueAfter` and influences other objects through the store, never by poking another reconcile loop directly.

`Create` generates a slug unless `beehive.WithSlug` is provided. If a slug is given and already exists, `Create` fails. All subsequent operations use `ObjectID` — safe against operating on a different incarnation after a delete/recreate. Finalizers and other metadata are set via options:

```go
client := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
obj, _ := client.Create(ctx, ClusterSpec{...}, beehive.WithSlug("prod-cluster"), beehive.WithFinalizers("kstack.sh/cluster"))
client.Update(ctx, obj.ID, ClusterSpec{...})
```

`Watch` and `WatchList` emit the current state as `Added` changes on start, then stream subsequent changes as `Change` values. The channel closes when `ctx` is cancelled. Changes are conflated per object: a watcher that falls behind converges to each object's latest state (a delete still carries its final body) rather than seeing every intermediate version — consistent with Beehive's level-triggered model. (The event *log* — `ListEvents`/`WatchEvents` below — is a separate concept: `Change` is an object-change notification, `Event` is a recorded log entry.)

#### Events

`ListEvents` returns an object's runs most-recent-first (`LastAt` descending); `WithEventCategory` narrows to a single timeline, and the other `EventOption`s filter by type/reason/time or cap the count. `WatchEvents` delivers the current recent runs as a snapshot, then streams extends and new runs — matching `Watch`/`WatchList`'s snapshot-then-live contract — conflated on `EventID`, so a subscriber sees one update per run (a count-bump updates the run in place) rather than one per occurrence. `GetLatestEvent` returns the current run in a category; its `bool` folds away the no-events-yet case, like `GetOwner`.

Retention is bounded per `(object, category)` by `WithEventRetention`: a cap-N ring keeps the newest N runs per timeline — so a flapping timeline can't evict a quiet one on the same object — plus an optional global max-age. The global GC sweeper enforces it, and events cascade-delete with their object.

### ControllerClient

```go
type ControllerClient[Status any] interface {
    UpdateStatus(ctx context.Context, id ObjectID, observedGeneration int64, status Status) error
    SetCondition(ctx context.Context, id ObjectID, condition Condition) error
    DeleteCondition(ctx context.Context, id ObjectID, conditionType string) error
    RecordEvent(ctx context.Context, id ObjectID, event EventSpec) error
    DeleteFinalizer(ctx context.Context, id ObjectID, finalizer string) error
    AddDependency(ctx context.Context, fromID, toID ObjectID) error
    DeleteDependency(ctx context.Context, fromID, toID ObjectID) error
    HasIncomingRefs(ctx context.Context, id ObjectID) (bool, error)
    // Lazy secondary lookups, for reading an object's edges during reconcile.
    GetOwner(ctx context.Context, id ObjectID) (Ref, bool, error)
    ListDependencies(ctx context.Context, id ObjectID) ([]Ref, error)
    ListDependents(ctx context.Context, id ObjectID) ([]Ref, error)
    ListOwned(ctx context.Context, id ObjectID) ([]Ref, error)
    Within(ctx context.Context, fn func(ctx context.Context) error) error
}
```

`GetOwner`/`ListDependencies`/`ListDependents`/`ListOwned` mirror the `Client` lazy lookups — a `Reconcile` receives the object directly (no read call site), so it reads related edges through these. `GetOwner` returns the owner via `owned_by`, `ListOwned` the inverse (the owner's children); `ListDependents` is the inverse of `ListDependencies` over `depends_on`. Distinct from `HasIncomingRefs`, which is a GC predicate: it folds in owned children *and* excludes finalizing dependents, so it can't be reconstructed from `ListDependents`.

`HasIncomingRefs` reports whether any object with a live claim still points at `id` — an owned child, or a dependent that is not itself being deleted (a finalizing dependent is excluded, since it's going away too). A finalizer can gate teardown on it — e.g. a controller that owns a shared connection clears its finalizer only once nothing with a live claim references the object, so the connection outlives its last real user.

`RecordEvent` appends an observation to the object's event log — see [Event](#event). Like `SetCondition` it is a scoped, transactional write (kind-folded; `ErrWrongKind` for a foreign id) and composes inside `Within`, so a controller can record an observation and flip a condition in one atomic step.

### Controller

```go
type Controller[Spec, Status any] interface {
    Reconcile(ctx context.Context, client ControllerClient[Status], obj *Object[Spec, Status]) (Result, error)
}
```

A controller owns **no lifecycle** in beehive — it implements only `Reconcile`, which receives the kind's `ControllerClient` as a parameter. Any background work (timers, subscriptions, engines) belongs to the embedding application, which already owns its own lifecycle and obtains a `ControllerClient` from `Register`. Beehive owns the reconcile lifecycle only: the work queue, backoff, resync, dependency wakers, GC, and drain ordering.

`Reconcile` is **not** wrapped in a transaction. Each `ControllerClient` write commits on its own, so a write that lands before `Reconcile` returns an error stays committed — the level loop simply re-derives from the persisted state on the next pass, so make `Reconcile` idempotent. (Each write is still internally atomic, and the `obj` snapshot a concurrent spec change can race is covered by the generation handshake: `UpdateStatus` rejects a future `observedGeneration`, and an older one leaves the object unsettled to reconcile again.)

When several writes must be atomic — all land together or none do — wrap them in `ControllerClient.Within(ctx, func(ctx) error { … })`. Writes made with the inner `ctx` join one transaction that commits on a `nil` return and rolls back on error. That transaction holds the store's single write lock for the whole duration of the function, so keep external I/O outside it — do your I/O first, then open `Within` only around the writes.

A non-nil error triggers an automatic retry with exponential backoff starting at 1s and capped at 30s by default. Configurable per-controller with `WithMaxRetryInterval`.

### Migrator

```go
type Migrator interface {
    SchemaVersionSpec() int                                          // spec version this build writes; 0 = not versioned
    SchemaVersionStatus() int                                        // status version this build writes; 0 = not versioned
    ConvertSpec(from int, raw json.RawMessage) (json.RawMessage, error)
    ConvertStatus(from int, raw json.RawMessage) (json.RawMessage, error)
}
```

Attach a `Migrator` per kind with `WithMigrator` passed to `Register`. The store persists the version each blob was written at in two opaque per-row columns (spec and status). On read, a blob below the current version is run through `ConvertSpec`/`ConvertStatus`; an equal version (or a current version of `0`, "not versioned") passes through; a *greater* version is a downgrade and is rejected as a decode error. `from == 0` is the unversioned baseline, so once a migrator is enabled its converters must handle it.

Conversion is lazy and per-column — a blob is re-stamped only when next written, so a status-only write re-stamps just the status version. A blob that fails to convert, fails to unmarshal, or is a downgrade is a decode failure: `List` and live watches skip-and-log it and continue, while `Get`/`GetBySlug` return the error. A kind with no migrator is unchanged — its columns stay `0`. Only `Register`ed kinds can have a migrator; client-only kinds cannot.

### Options

```go
type Option interface{ apply(any) }

func WithSlug(slug string) Option                  // set a human-readable slug; fails if already exists
func WithFinalizers(f ...string) Option            // declare finalizers before the object is visible to controllers
func WithOwner(id ObjectID) Option                 // declare owned_by edge; owner cannot be deleted while this object exists
func WithResyncInterval(d time.Duration) Option    // override the default resync interval
func WithMaxRetryInterval(d time.Duration) Option  // cap on exponential backoff after Reconcile errors (default: 30s)
func WithMigrator(m Migrator) Option               // attach a schema-version Migrator for the kind (Register only)
func WithEventRetention(perObject int, maxAge time.Duration) Option // event-log retention: per-(object,category) cap-N ring + optional age bound (0 = no age bound)
```

`WithOwner` sets an `owned_by` edge in `refs` atomically with the `Create` call. When the owner is deleted, Beehive triggers deletion of the child via the GC reconciler.

`AddDependency` and `DeleteDependency` on `ControllerClient` manage `depends_on` edges during reconcile. When a target's conditions change, Beehive automatically requeues the dependent. Each commits on its own, or joins a `Within` if the controller opened one.

Read calls take `LoadOption`s (a separate type from `Option`) to eagerly fetch secondary lookups — see [Secondary lookups](#secondary-lookups-owner--dependencies--dependents--owned):

```go
func LoadOwner() LoadOption         // fetch the owner (outgoing owned_by)
func LoadDependencies() LoadOption  // fetch dependencies (outgoing depends_on)
func LoadDependents() LoadOption    // fetch dependents (incoming depends_on)
func LoadOwned() LoadOption         // fetch owned children (incoming owned_by)
func LoadEvents() LoadOption        // fetch the most-recent events (default N per (object,category))
```

`Requeue` takes `RequeueOption`s (also a separate type from `Option`, applying only to `Requeue`) — see [Reconcile control](#reconcile-control):

```go
func WithResetBackoff() RequeueOption   // clear the retry backoff ladder before requeuing (default: preserve it)
```

The event read methods take `EventOption`s (also a separate type from `Option`, applying only to `ListEvents`/`WatchEvents`) — see [Events](#events):

```go
func WithEventCategory(cat string) EventOption  // restrict to a single timeline
func WithEventType(t EventType) EventOption      // only Normal or only Warning
func WithEventReason(reason string) EventOption  // only runs with this reason
func WithEventLimit(n int) EventOption           // cap the number of runs returned / snapshotted
func WithEventsSince(t time.Time) EventOption    // only runs active at or after t
```
