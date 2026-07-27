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
    // TODO: remove finalizer: return beehive.Result{}, client.FinalizersDelete(ctx, obj.ID, "kstack.sh/cluster")
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

- **Coordination through the store.** Controllers never call each other. They read/write the shared store and wake on change-events. Events are a latency optimization: an object whose spec has not converged, or which is owed a recorded dependency wake, is re-derived by the catchup tick and at startup, so a dropped event costs latency rather than convergence. The one case events alone cover is a *settled* object whose state changed for a reason the store did not record — for that, enable `WithResyncInterval` or rely on the next startup pass.

- **`spec`/`status` separation.** Only controllers may write `status`. This is structural in the API: the user-facing `Client` surface has no status-write path; only the `Controller` surface does.

- **Schema-version migration.** `Spec` and `Status` are opaque JSON, so reshaping a struct would break decode of older rows. A per-kind `Migrator` converts an old blob up *on read*, before unmarshal. Spec and Status version and convert independently; conversion is lazy — re-stamped only when the blob is next written, never by a bulk rewrite.

The decisions behind these — and the trade-offs each one closed — are recorded in [docs/adr](docs/adr/README.md), linked from the relevant sections below.

## API

### Beehive

```go
func New(store Store, opts ...Option) (*Beehive, error)
func Register[Spec, Status any](bh *Beehive, gk GroupKind, c Controller[Spec, Status], opts ...Option) (ControllerClient[Status], error)
```

`Register` returns the kind's `ControllerClient` — the status-write surface — so the embedding application can write status out-of-band (e.g. from its own goroutines) without beehive handing it over via a callback. A `ControllerClient` is obtainable *only* by registering a controller for that kind, which keeps the "only the owning controller writes its status" boundary intact.

Options are dispatched by caller type — `WithResyncInterval` passed to `New` sets the global default; passed to `Register` it overrides for that controller only. Unrecognised options for a given caller are ignored. (`WithGCInterval` is the exception: garbage collection is global, spanning kinds that have no controller at all, so it is meaningful only at `New`.)

#### Periodic drivers

Three independent cadences, because they are three different jobs with different costs:

| option | what it re-dispatches | cost scales with | default |
|---|---|---|---|
| `WithCatchupInterval` | work the store *records* as owed — unconverged specs (`observed_generation < generation`) and owed dependency wakes | what is actually outstanding | 30s |
| `WithResyncInterval` | **every** object, converged or not | the object count | 0 (off) |
| `WithGCInterval` | deletion-pending rows, plus event-log retention | rows being deleted | 30s |

The full resync is opt-in because its cost is the only one unbounded by outstanding work, and because it is the only driver that reaches an object nothing recorded as owing anything — process-scoped state a restart invalidated (a liveness condition reads as "verifying" until a controller in *this* process rewrites it), or a dependency wake lost for a reason nothing observed. The startup pass covers that ground once per process.

Disabling a driver is supported for the two reconcile cadences and logged at startup, so a knob left at 0 by accident is visible rather than silent. **GC is the exception: it cannot be disabled** — `WithGCInterval` rejects a non-positive interval with `ErrInvalidOption`. A long interval says "collect rarely"; there is no way to say "never".

→ [ADR: three independent periodic drivers](docs/adr/2026-07-27-periodic-reconcile-drivers.md), for why they were split and why GC alone is mandatory.

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

`Detail` is the machine-readable companion to `Message` — an optional structured payload (`ProbeFailed` might carry `{"endpoint":"10.0.0.1:443","latencyMs":5000}`). It follows the `Spec`/`Status` convention of **typed in, opaque out**: on write it is any JSON-marshalable value (`EventsRecord` marshals it, just as `Create` marshals `Spec`); on read it returns as `json.RawMessage`, decoded on demand with the free generic helper `EventDetail[T](e)` — applied per event with the type its `Reason` implies, so a single timeline can mix reasons carrying different detail shapes without the API becoming generic. Like `Message`, `Detail` is *sampled* (latest occurrence wins) and not part of the run key, so a varying payload never fragments a run — if you need every occurrence's payload retained, that event shouldn't aggregate (use a unique `Reason`). Unlike `Spec`/`Status`, `Detail` is **not** schema-versioned; reshaping it breaks decode of older rows, which is acceptable only because retention ages events out — version inside the payload if you need forward-compatibility.

Only controllers write events — `ControllerClient.EventsRecord` is the sole write path, because events are observations and (like `status`) have no user-facing writer. Reads are on the `Client` (`EventsList` / `EventsWatch` / `EventsGetLatest`), plus the eager `LoadEvents()` / `Object.Events()` pair that follows the same loaded-gating as the secondary lookups (`ErrNotLoaded` when not requested).

A connection-health panel renders one category's timeline directly — `client.EventsList(ctx, id, WithEventCategory("connection"))` yields, newest first:

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
    Slug                *string  // nil when created without WithSlug; never auto-generated
    Spec                Spec
    Status              *Status
    Generation          int64
    ObservedGeneration  *int64
    ObservedAt          *time.Time // when ObservedGeneration was recorded; not a reconcile heartbeat
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

type Ref = storeapi.ObjectRef // { ID ObjectID; Group, Kind string }
```

The secondary-lookup data is filled only when the read asked for it. Read it through the accessors, which return `ErrNotLoaded` if the relation wasn't requested — so forgetting the `Load*()` option fails loudly instead of looking empty. These are bare accessors, with no verb to add: cardinality is in the return type, `(Ref, bool, error)` for the at-most-one owner against `([]Ref, error)` for the rest.

```go
func (o *Object[Spec, Status]) Owner() (Ref, bool, error) // bool: an owner exists; err: not loaded
func (o *Object[Spec, Status]) Dependencies() ([]Ref, error)
func (o *Object[Spec, Status]) Dependents() ([]Ref, error)
func (o *Object[Spec, Status]) Owned() ([]Ref, error)
func (o *Object[Spec, Status]) Events() ([]Event, error)
```

Once loaded, an empty slice (or `Owner`'s `ok == false`) means genuinely none. `ErrNotLoaded` is caller misuse — fetch the relation eagerly with the `Load*()` option, or lazily via the `Client`/`ControllerClient` methods below.

### Result

```go
type Result struct {
    RequeueAfter time.Duration // zero means no requeue
}
```

### Schedule

```go
type Schedule struct {
    NextRequeueAt time.Time // when the object is next due to reconcile; zero = nothing scheduled
    // reserved: a future Trigger/Reason (backoff | success-cadence | manual poke)
}
```

`Schedule` is the value the [scheduling API](#scheduling) reports: an object's **next reconcile time, as a gauge**. It is a struct rather than a bare `time.Time` so fields can be added — e.g. a reschedule **trigger** (backoff vs. success-cadence vs. manual poke), reserved but not yet populated — without a breaking change. `NextRequeueAt` reflects only per-id timers (a pending backoff retry or `RequeueAfter` delay, or now if already queued) and is the zero time when nothing is scheduled.

### Client

```go
type ChangeType string

const (
    Added    ChangeType = "Added"
    Modified ChangeType = "Modified"
    Deleted  ChangeType = "Deleted"
)

type ObjectChange[Spec, Status any] struct {
    Type   ChangeType
    Object *Object[Spec, Status]
}

type Client[Spec, Status any] interface {
    Create(ctx context.Context, spec Spec, opts ...Option) (*Object[Spec, Status], error)
    CreateOrUpdate(ctx context.Context, slug string, spec Spec) (*Object[Spec, Status], error)
    GetOrCreate(ctx context.Context, slug string, spec Spec, opts ...Option) (*Object[Spec, Status], bool, error)
    Update(ctx context.Context, id ObjectID, spec Spec) (*Object[Spec, Status], error)
    Get(ctx context.Context, id ObjectID, loads ...LoadOption) (*Object[Spec, Status], error)
    GetBySlug(ctx context.Context, slug string, loads ...LoadOption) (*Object[Spec, Status], error)
    List(ctx context.Context, loads ...LoadOption) ([]*Object[Spec, Status], error)
    Delete(ctx context.Context, id ObjectID) error
    DeleteBySlug(ctx context.Context, slug string) error // idempotent: absent or already-deleting is a nil no-op
    ObjectsWatch(ctx context.Context, id ObjectID) (<-chan ObjectChange[Spec, Status], error)
    ObjectsWatchList(ctx context.Context) (<-chan ObjectChange[Spec, Status], error)

    // Lazy secondary lookups — the on-demand counterparts to the Load options.
    OwnersGet(ctx context.Context, id ObjectID) (Ref, bool, error)
    DependenciesList(ctx context.Context, id ObjectID) ([]Ref, error)
    DependentsList(ctx context.Context, id ObjectID) ([]Ref, error)
    OwnedList(ctx context.Context, id ObjectID) ([]Ref, error)
    // The typed, kind-scoped form of OwnedList: this kind's decoded children.
    OwnedObjectsList(ctx context.Context, ownerID ObjectID, loads ...LoadOption) ([]*Object[Spec, Status], error)

    // Event log — per-object, category-partitioned, contiguous-run aggregated.
    EventsList(ctx context.Context, id ObjectID, opts ...EventOption) ([]Event, error)
    EventsGetLatest(ctx context.Context, id ObjectID, category string) (Event, bool, error)
    EventsWatch(ctx context.Context, id ObjectID, opts ...EventOption) (<-chan Event, error)

    // Reconcile control.
    Requeue(ctx context.Context, id ObjectID, opts ...RequeueOption) error // requeue now; preserves backoff unless WithResetBackoff()

    // Scheduling — observe the next-requeue time.
    SchedulesGet(ctx context.Context, id ObjectID) (Schedule, error)          // current schedule (zero if nothing scheduled)
    SchedulesWatch(ctx context.Context, id ObjectID) (<-chan Schedule, error) // stream the schedule live as a gauge
}

func NewClient[Spec, Status any](bh *Beehive, gk GroupKind) Client[Spec, Status]
```

#### Writes

`Create` leaves the slug **unset** unless `beehive.WithSlug` is provided — it is `nil`, stored as SQL `NULL`, and nothing is generated for you. NULL slugs don't collide (`NULL != NULL` in SQLite), so any number of slugless objects of a kind coexist; they are reachable by `ObjectID` and `List`, just not by name. If a slug *is* given and already exists, `Create` fails on the `UNIQUE ("group", kind, slug)` constraint. All subsequent operations use `ObjectID` — safe against operating on a different incarnation after a delete/recreate. Finalizers and other metadata are set via options:

```go
client := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
obj, _ := client.Create(ctx, ClusterSpec{...}, beehive.WithSlug("prod-cluster"), beehive.WithFinalizers("kstack.sh/cluster"))
client.Update(ctx, obj.ID, ClusterSpec{...})
```

**A slug is an opaque key, and beehive does not validate it** — no charset rule, no length limit, no normalization. The empty string is therefore a perfectly ordinary slug: a real value under the unique constraint, and distinct from `NULL`. `GetOrCreate(ctx, "", spec)` creates *the* empty-slug object of that kind, and the next caller passing `""` gets that same row back with `created=false` — exactly as two callers passing `"prod"` would. That is the contract rather than a collision bug, but it has a sharp edge worth knowing: a slug derived from configuration is `""` when the config field is unset, which silently keys every such caller to one shared object. Validate slugs at the edge if they come from outside your code, and when you mean "no name" use `nil` — `Create` without `WithSlug` — not `""`.

The three slug-keyed writes differ **only in what they do when the slug is already taken**, and the table holds under concurrency too. `CreateOrUpdate` and `GetOrCreate` wrap their read-and-write in one transaction, so two callers racing on the same slug never both insert: the loser observes the winner's row and updates or returns it. `Create` does no lookup — it inserts — so the loser of the same race fails on `UNIQUE`, exactly as it would against a pre-existing row:

| Slug already held by    | `Create`         | `CreateOrUpdate`     | `GetOrCreate`                         |
| ----------------------- | ---------------- | -------------------- | ------------------------------------- |
| nothing                 | creates          | creates              | creates, `created=true`               |
| a live row              | fails (`UNIQUE`) | updates it to `spec` | returns it untouched, `created=false` |
| a deletion-pending row  | fails (`UNIQUE`) | updates it to `spec` | returns it untouched, `created=false` |

Where `CreateOrUpdate` says "updates it", re-applying the spec the row already holds is a **complete** no-op: no generation bump, no `resource_version` bump, no watch event, and no reconciler wake. That last part matters if a controller re-applies a spec of its own kind on every pass — it converges instead of waking itself in a loop. The same holds for `Update`.

Every write **validates before it commits.** `Create`, `CreateOrUpdate`, `GetOrCreate`, and `Update` decode the written row back into `Spec`/`Status` *inside* the write transaction, so a spec that marshals but does not round-trip — typically an asymmetric `MarshalJSON`/`UnmarshalJSON` — rolls the write back rather than committing a row the process cannot read. **An error from a write therefore means nothing was committed:** no poison row, no reconciler wake, no `UNIQUE` left behind for a retry to trip on, and for `Update`/`CreateOrUpdate` the prior good spec is preserved. `GetOrCreate` in particular returns `created=false` on such an error, since nothing was created. The cost is that the write holds the store's single writer across the decode (`json.Marshal` still runs *before* the transaction). This guards only the write path; a row can still become undecodable *later* — e.g. a schema downgrade — which is a read/reconcile concern handled by quarantine (see [Migrator](#migrator)).

Reach for `GetOrCreate` when a controller must idempotently ensure a child exists **without ever mutating it** — the pattern otherwise open-coded as `GetBySlug` → `Create` → re-`GetBySlug` on conflict, where the fallback path tends to drift from the primary one's checks. Its found branch performs no write at all, so a deletion-pending row comes back as-is with `DeletionRequestedAt` set rather than being resurrected by an `ObjectsUpdateSpec`:

Two surfaces appear in the example below, and they are not interchangeable:
`GetOrCreate` is on `Client` (the child kind's client, built with `NewClient` and
held by the controller), while `EventsRecord` is on the `ControllerClient` that
`Reconcile` is handed for writes about the object being reconciled. `Client` has no
`EventsRecord`, and `ControllerClient` has no `GetOrCreate` — a controller creates
children through a `Client` for that kind.

```go
type ProjectController struct {
    // built once at wiring time:
    //   beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
    clusters beehive.Client[ClusterSpec, ClusterStatus]
}

// Ensure the Cluster this Project owns exists, without ever mutating it. The
// options apply only if this call creates the row — a pre-existing row is
// returned exactly as it is (see the caveat below).
func (p *ProjectController) Reconcile(ctx context.Context, cc beehive.ControllerClient[ProjectStatus], obj *beehive.Object[ProjectSpec, ProjectStatus]) (beehive.Result, error) {
    cluster, created, err := p.clusters.GetOrCreate(ctx, "prod-cluster", ClusterSpec{...},
        beehive.WithOwner(obj.ID), beehive.WithFinalizers("kstack.sh/cluster"))
    if err != nil {
        return beehive.Result{}, err
    }
    if cluster.DeletionRequestedAt != nil {
        // The slug is still held by a tombstone; it is released only once GC clears
        // the row's finalizers. Wait and retry — a replacement cannot be created yet.
        return beehive.Result{RequeueAfter: 5 * time.Second}, nil
    }
    if created {
        // EventsRecord is about obj (this controller's object), not the child.
        if err := cc.EventsRecord(ctx, obj.ID, beehive.EventSpec{
            Category: "lifecycle", Reason: "ClusterCreated",
        }); err != nil {
            return beehive.Result{}, err
        }
    }
    return beehive.Result{}, nil
}
```

`created` reports whether this call inserted the row: on create the `Added` event is emitted and the object enqueued, exactly as with `Create`; returning an existing row emits and enqueues nothing. Both are post-commit — nested in an outer `ControllerClient.Within`, they fire after the *outermost* commit, and not at all if it rolls back (`Create`, `CreateOrUpdate`, and `Update` behave the same way). The return value can't be deferred that way, so `created=true` is provisional until the outer transaction commits; for a side effect that must run only if the row lands, use `WithOnCreate` (below), which is deferred to the same post-commit point as the wake.

The options apply **only on the create branch** (`WithOwner`, `WithFinalizers`, `WithOnCreate`). `WithSlug` is **rejected** with `ErrConflictingOption`: the slug is positional here, so the option can only contradict it, and dropping it silently would put the row under one slug while the caller went looking for it under another. (This is narrower than the general option rule — an inapplicable option is still ignored by design; a contradiction is a caller mistake.)

That last point has a sharp edge worth stating plainly: on the found branch the options are ignored outright, so **`created=false` does not mean "exists and matches your options."** A row created earlier by a path that passed no `WithOwner` comes back with no owner edge, and a caller that assumes otherwise gets a child the GC cascade will never collect when the parent is deleted. If you depend on the owner edge, verify it — `GetOrCreate` then `OwnersGet` (or a `Get(ctx, id, LoadOwner())`) — and reconcile the difference yourself. Beehive deliberately does not adopt the row for you: `owner` is single, so adding the edge to a row that already has a *different* owner would produce a two-owner object, and choosing which owner wins is your policy, not the library's.

`DeleteBySlug` is the remove half of that ensure/remove pair: `GetOrCreate` creates-if-absent, `DeleteBySlug` deletes-if-present, and both are idempotent and tombstone-aware, so a controller that ensures a slug-keyed child on one branch and removes it on another spells each side as a single call. It collapses what is otherwise open-coded as `GetBySlug` → `ErrNotFound`-is-success → `DeletionRequestedAt`-is-a-no-op → `Delete`:

| Slug held by           | `DeleteBySlug`                                              |
| ---------------------- | ----------------------------------------------------------- |
| nothing                | `nil` — already gone                                         |
| a live row             | soft-deletes it (sets `DeletionRequestedAt`), advances GC    |
| a deletion-pending row | no-op — no write, no event — advances GC; `nil`              |

Like `Delete` it soft-deletes and hands the object to the controller to clear its finalizers; physical removal follows once they clear, and only then is the slug released. It is kind-scoped like `GetBySlug` — a slug is per-kind, so another kind's row holding the same slug is simply not found, and reported as success rather than as a wrong-kind error.

The resolve is **atomic with the delete**, in the same sense the table above holds for `CreateOrUpdate`/`GetOrCreate`: the slug is folded into the store's write, not looked up first and deleted after, so no concurrent collection can retire the row and hand the slug to a replacement in between. `nil` therefore means "no object of this kind holds this slug" rather than "the row I happened to resolve is gone." What it does *not* promise — and no implementation could — is that the slug is still free when the call returns: a concurrent `GetOrCreate` may take it the instant the delete commits. As everywhere in Beehive, the next reconcile re-derives from current state.

→ [ADR: slug-keyed writes and post-commit wakes](docs/adr/2026-07-27-writes-and-post-commit-wakes.md), for the transaction boundaries and why every wake runs after the outermost commit.

#### Watching

`ObjectsWatch` and `ObjectsWatchList` emit the current state as `Added` changes on start, then stream subsequent changes as `ObjectChange` values. The channel closes when `ctx` is cancelled. Changes are conflated per object: a watcher that falls behind converges to each object's latest state (a delete still carries its final body) rather than seeing every intermediate version — consistent with Beehive's level-triggered model. (The event *log* — `EventsList`/`EventsWatch` below — is a separate concept: `ObjectChange` is an object-change notification, `Event` is a recorded log entry.)

→ [ADR: watch fan-out conflates per object](docs/adr/2026-07-27-conflating-watch-fanout.md), for why there is no ring, no lag error, and no relist.

#### Secondary lookups (owner / dependencies / dependents / owned)

An object's ref edges are fetched on request, two ways:

- **Eager** — pass `LoadOption`s to a read: `Get(ctx, id, LoadOwner())`, `List(ctx, LoadDependencies(), LoadDependents())`. The returned objects carry the data (read via the accessors). On `List` each relation is one batched query, not one per object.
- **Lazy** — call `OwnersGet` / `DependenciesList` / `DependentsList` / `OwnedList` when the data is actually needed. These hit the edge query directly and do **not** kind-scope `id` (no validating read in front): a foreign id reads that kind's edges and a missing id reads empty, neither as `ErrNotFound`. Reserve them for ids the client owns.

`OwnedList` (and the eager `LoadOwned()` / `Object.Owned()`) is the inverse of `OwnersGet` over `owned_by`: it returns the objects a given owner owns, the same way `DependentsList` inverts `DependenciesList` over `depends_on`.

`OwnedObjectsList(ownerID)` is its typed counterpart: where `OwnedList` returns untyped `Ref`s across *every* owned kind — leaving the caller to filter by `Kind` and `Get` each child through that kind's client — `OwnedObjectsList` returns the fully decoded `*Object[Spec, Status]` children of **this client's kind**, in one store query (the kind filter and the row read are folded into the edge semi-join, so there is no `Get` per child). Same ordering (by id) and same missing-owner behavior as `OwnedList`; deletion-pending children are included, so a caller that wants to skip them checks `DeletionRequestedAt` itself. It takes the same `LoadOption`s as `List`, batched the same way — without them the children carry nothing loaded and their accessors return `ErrNotLoaded`.

Both issue the same secondary query (edges are a separate indexed lookup, never joined into the object's blob-bearing `SELECT`); eager just attaches the result to the object and batches across a `List`.

→ [ADR: secondary lookups](docs/adr/2026-07-27-secondary-lookups.md), for the loader sharing, the accessor naming rule, and the store's semi-join.

#### Reconcile control

`Requeue` requeues an object for immediate reconcile — the manual counterpart to the store-write and dependency-change wakes. It is a **latency hint, not a synchronous run**: it returns once the object is enqueued, and a worker reconciles it on its own schedule. A missed or coalesced requeue is harmless whenever the object is *owed* something the store records, since the catchup tick re-derives that. It is also the supported way to drive reconciles yourself when every periodic driver is disabled. Use it to promptly re-examine an object after out-of-band state the controller reads has changed.

By default `Requeue` **preserves the object's retry backoff ladder**. A requeue is the ordinary event-driven nudge (config change, dependency update, manual poke) and almost never proves the failing condition is resolved; the only event that proves recovery is a successful reconcile, which already clears backoff. The invariant: **backoff is cleared by a successful reconcile or an explicit `WithResetBackoff()`, never by a plain requeue.** Pass `beehive.WithResetBackoff()` only when the caller knows the failure is resolved and the next retry should restart from the base interval — the analog of controller-runtime's `Forget`. (This mirrors controller-runtime's split between `Add`/`AddAfter`, which requeue without resetting, and `Forget`, which explicitly resets.)

`Requeue` validates the id against the client's kind first (`ErrNotFound` for a missing or foreign id), then requires a registered controller (`ErrNoController` for a client-only kind, which has no reconcile loop to schedule against). It is `Client`-only — a controller schedules itself with `Result.RequeueAfter` and influences other objects through the store, never by poking another reconcile loop directly.

#### Scheduling

The scheduling API observes when an object is **next due to reconcile** — a [`Schedule`](#schedule) gauge whose `NextRequeueAt` is a pending backoff retry or `RequeueAfter` delay, or now if the object is already queued, and the zero time when nothing is scheduled.

`SchedulesGet` is the point read. It is a non-blocking, best-effort read of in-memory schedule state — no store lookup, no kind guard — so it returns no error today (the error is reserved for symmetry). A missing, foreign, or client-only-kind id reads as the zero-value `Schedule`, indistinguishable from a real object with nothing scheduled.

`SchedulesWatch` streams that schedule live as a **gauge**: the current value on subscribe, then a new `Schedule` on every (re)schedule — backoff step, `RequeueAfter`, resync or dependency wake, dispatch, or `Requeue`. None of these fire the object `ObjectsWatch`/`ObjectsWatchList` (a reschedule bumps no generation or resource version) and no other signal captures them all, so this is the only way to reliably observe reschedules — e.g. to drive a "next attempt" countdown that stays accurate for an object whose spec/status has stopped changing. Delivery mirrors `EventsWatch`: snapshot-then-live, conflated **per object** so a lagging reader converges to the latest value (it can miss intermediate values but never the current one), and the channel closes when `ctx` is cancelled or the control plane stops. Unlike `SchedulesGet`, `SchedulesWatch` returns `ErrNoController` for a client-only kind — a live stream that can never emit should say so rather than hang — but `id` need not exist: an unscheduled id simply streams the zero `Schedule` until something schedules it.

Both are `Client`-only and read **only per-id timers**, so neither is a prediction of the next reconcile: the actual next reconcile can be **earlier** than reported (the catchup tick — kind-wide, conditional on the object being owed something — plus the full resync, dependency-change wakes and store-write enqueues are not per-id timers), and a **zero `NextRequeueAt` means "nothing scheduled", not "will not reconcile"**. Treat it as observability, not a guarantee.

→ [ADR: the schedule watch](docs/adr/2026-07-27-schedule-watch.md), for why it is an in-memory gauge rather than an event-log surface.

#### Events

`EventsList` returns an object's runs most-recent-first (`LastAt` descending); `WithEventCategory` narrows to a single timeline, and the other `EventOption`s filter by type/reason/time or cap the count. `EventsWatch` delivers the current recent runs as a snapshot, then streams extends and new runs — matching `ObjectsWatch`/`ObjectsWatchList`'s snapshot-then-live contract — conflated on `EventID`, so a subscriber sees one update per run (a count-bump updates the run in place) rather than one per occurrence. `EventsGetLatest` returns the current run in a category; its `bool` folds away the no-events-yet case, like `OwnersGet`.

Retention is bounded per `(object, category)` by `WithEventRetention`: a cap-N ring keeps the newest N runs per timeline — so a flapping timeline can't evict a quiet one on the same object — plus an optional global max-age. The global GC sweeper enforces it, and events cascade-delete with their object.

→ [ADR: the events API](docs/adr/2026-07-27-events-api.md), for the run-aggregation rule, why `Detail` stays off the generic boundary, and the watch-surface naming.

### ControllerClient

```go
type ControllerClient[Status any] interface {
    UpdateStatus(ctx context.Context, id ObjectID, observedGeneration int64, status Status) error
    ConditionsSet(ctx context.Context, id ObjectID, condition Condition) error
    ConditionsDelete(ctx context.Context, id ObjectID, conditionType string) error
    EventsRecord(ctx context.Context, id ObjectID, event EventSpec) error
    FinalizersDelete(ctx context.Context, id ObjectID, finalizer string) error
    DependenciesAdd(ctx context.Context, fromID, toID ObjectID, targetResourceVersion int64) error
    DependenciesDelete(ctx context.Context, fromID, toID ObjectID) error
    EdgesHasIncoming(ctx context.Context, id ObjectID) (bool, error)
    // Lazy secondary lookups, for reading an object's edges during reconcile.
    OwnersGet(ctx context.Context, id ObjectID) (Ref, bool, error)
    DependenciesList(ctx context.Context, id ObjectID) ([]Ref, error)
    DependentsList(ctx context.Context, id ObjectID) ([]Ref, error)
    OwnedList(ctx context.Context, id ObjectID) ([]Ref, error)
    Within(ctx context.Context, fn func(ctx context.Context) error) error
}
```

`UpdateStatus` is a **no-op when the status marshals to the bytes already stored**: no `resource_version` bump and no watch event, exactly as re-applying an unchanged spec is a no-op on the `Client` side. So a controller reports its observed state unconditionally — no hand-rolled equality guard — and a dependent that free-rides on this kind's status changes isn't woken by a poll that found nothing new.

The generation handshake is the exception. `observedGeneration`/`ObservedAt` are recorded even when the content didn't change, so a reconcile that legitimately changed no status still settles the object rather than being re-enqueued by every catchup tick — and that write *does* bump `resource_version` and emit, so a watcher gating on `ObservedGeneration == Generation` sees the object converge. It fires at most once per generation: the next unchanged poll finds the generation already recorded and writes nothing.

So `ObservedAt` records **when the object settled at `ObservedGeneration`**, not when the controller last ran. Don't build a controller-liveness check on it — a reconcile that calls no `UpdateStatus` at all never moved it either. For "when did we last check", record an event: `EventsRecord` extends the current run and bumps its `LastAt` on every poll, which is exactly that signal, retained and rate-shaped.

→ [ADR: the generation handshake and content no-ops](docs/adr/2026-07-27-generation-handshake-and-noop-writes.md), for how the no-op splits the two halves of the write and why it is gated on the schema version.

`OwnersGet`/`DependenciesList`/`DependentsList`/`OwnedList` mirror the `Client` lazy lookups — a `Reconcile` receives the object directly (no read call site), so it reads related edges through these. `OwnersGet` returns the owner via `owned_by`, `OwnedList` the inverse (the owner's children); `DependentsList` is the inverse of `DependenciesList` over `depends_on`. Distinct from `EdgesHasIncoming`, which is a GC predicate: it folds in owned children *and* excludes finalizing dependents, so it can't be reconstructed from `DependentsList`.

`EdgesHasIncoming` reports whether any object with a live claim still points at `id` — an owned child, or a dependent that is not itself being deleted (a finalizing dependent is excluded, since it's going away too). A finalizer can gate teardown on it — e.g. a controller that owns a shared connection clears its finalizer only once nothing with a live claim references the object, so the connection outlives its last real user.

`EventsRecord` appends an observation to the object's event log — see [Event](#event). Like `ConditionsSet` it is a scoped, transactional write (kind-folded; `ErrWrongKind` for a foreign id) and composes inside `Within`, so a controller can record an observation and flip a condition in one atomic step.

### Controller

```go
type Controller[Spec, Status any] interface {
    Reconcile(ctx context.Context, client ControllerClient[Status], obj *Object[Spec, Status]) (Result, error)
}
```

A controller owns **no lifecycle** in beehive — it implements only `Reconcile`, which receives the kind's `ControllerClient` as a parameter. Any background work (timers, subscriptions, engines) belongs to the embedding application, which already owns its own lifecycle and obtains a `ControllerClient` from `Register`. Beehive owns the reconcile lifecycle only: the work queue, backoff, the periodic drivers, dependency wakers, GC, and drain ordering.

`Reconcile` is **not** wrapped in a transaction. Each `ControllerClient` write commits on its own, so a write that lands before `Reconcile` returns an error stays committed — the level loop simply re-derives from the persisted state on the next pass, so make `Reconcile` idempotent. (Each write is still internally atomic, and the `obj` snapshot a concurrent spec change can race is covered by the generation handshake: `UpdateStatus` rejects a future `observedGeneration`, and an older one leaves the object unsettled to reconcile again.)

When several writes must be atomic — all land together or none do — wrap them in `ControllerClient.Within(ctx, func(ctx) error { … })`. Writes made with the inner `ctx` join one transaction that commits on a `nil` return and rolls back on error, `Client` writes included. That transaction holds the store's single write lock for the whole duration of the function, so keep external I/O outside it. Side effects wait for it: the reconciler wake, and for `Delete` the garbage-collection follow-up, run after the outermost commit and are skipped on rollback, so a controller can create or delete children inside `Within` without waking anything at a row that never lands. → [ADR](docs/adr/2026-07-27-writes-and-post-commit-wakes.md)

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

Conversion is lazy and per-column — a blob is re-stamped only when next written, so a status-only write re-stamps just the status version. A blob that fails to convert, fails to unmarshal, or is a downgrade is a decode failure, and each read path handles it in the way that fails safest for it: `List` and live watches skip-and-log the bad row and continue; `Get`/`GetBySlug` return the error; and the **reconcile loop quarantines** it — a row it cannot decode cannot be reconciled and its bytes won't change until someone rewrites the spec, so it logs and treats the pass as a no-op success rather than retrying the same bytes forever under backoff (a deletion-pending row is still collected, since GC needs only the id). Because the catchup tick re-enqueues an unsettled poison row every time, this warning recurs at that cadence — deliberately, so a persistent bad row stays visible rather than logging once and going silent. A kind with no migrator is unchanged — its columns stay `0`. Only `Register`ed kinds can have a migrator; client-only kinds cannot.

→ [ADR: schema-version migration](docs/adr/2026-07-27-schema-version-migration.md), for convert-on-read / stamp-on-write and why stamping is never downward.

### Options

```go
type Option interface{ apply(any) }

func WithSlug(slug string) Option                  // set a human-readable slug; fails if already exists
func WithFinalizers(f ...string) Option            // declare finalizers before the object is visible to controllers
func WithOwner(id ObjectID) Option                 // declare owned_by edge; owner cannot be deleted while this object exists
func WithOnCreate(fn func(ctx context.Context)) Option // run fn after the create commits (Create always; GetOrCreate only when it inserts)
func WithCatchupInterval(d time.Duration) Option   // how often to drain recorded owed work (default: 30s; 0 disables)
func WithResyncInterval(d time.Duration) Option    // how often to re-dispatch EVERY object (default: 0, off)
func WithGCInterval(d time.Duration) Option        // how often to collect dead rows + prune the event log (default: 30s; New only; must be > 0)
func WithStartupResync(enabled bool) Option        // also re-dispatch settled objects once at startup (default: true)
func WithMaxRetryInterval(d time.Duration) Option  // cap on exponential backoff after Reconcile errors (default: 30s)
func WithMigrator(m Migrator) Option               // attach a schema-version Migrator for the kind (Register only)
func WithEventRetention(perObject int, maxAge time.Duration) Option // event-log retention: per-(object,category) cap-N ring + optional age bound (0 = no age bound)
```

`WithOwner` sets an `owned_by` edge in `edges` atomically with the `Create` call. When the owner is deleted, Beehive triggers deletion of the child via the GC reconciler.

`WithOnCreate` is the commit-safe channel for a create-conditional side effect (an external call, an in-memory counter). It is registered on the same post-commit path as the reconciler wake, so it runs once after the *outermost* commit and never on a rollback. `Create` always fires it; `GetOrCreate` fires it only on the create branch, not when it returns an existing row. Prefer it over branching on `GetOrCreate`'s returned `created` bool: that bool is synchronous, so inside a caller's `ControllerClient.Within` it is set before the enclosing transaction commits, and acting on it there fires the side effect for a row a later rollback would discard.

`DependenciesAdd` and `DependenciesDelete` on `ControllerClient` manage `depends_on` edges during reconcile. When a target's conditions change, Beehive automatically requeues the dependent. Each commits on its own, or joins a `Within` if the controller opened one.

The target may be of **any** kind, including one you only ever use through `Client` and never `Register` — configuration, secrets, any reference data your application writes and your controllers read. Beehive observes changes to every object in the store, not only to kinds that have controllers, so such a target wakes its dependents like any other.

`DependenciesAdd` takes `targetResourceVersion`: the `ResourceVersion` of the target *as the decision to depend on it was read*, not a freshly fetched one. A change to the target that lands between that read and the edge's commit would otherwise reach nobody — the waker resolves dependents at the instant of the change, and the edge does not exist yet — so if the target has moved past the version you pass, the dependent is requeued. Pass `0` to skip the check; that is the right value when you declare the edge *before* reading the target, which needs no check.

A version *above* the target's current one is rejected with `ErrTargetResourceVersionFuture` — versions only move forward, so it cannot have come from reading the target. The rejection happens before anything is written, so no edge is declared even if you call it inside your own `Within` and ignore the error.

The requeue fires at most once per edge — it is also gated on the call that creates the edge, and every change after that reaches an edge the waker already sees. So re-asserting your edges on every pass costs nothing after the first, and a stale version costs at most one spurious reconcile rather than a loop. It survives a crash: the same conjunction records the owed wake durably, and the catchup tick drains it.

→ [ADR: caller-versioned dependency declaration](docs/adr/2026-07-27-caller-versioned-dependencies.md), for why both halves are required and how the durable twin is kept atomic with the edge. The waker itself is [one store-wide change stream](docs/adr/2026-07-27-store-wide-dependency-change-stream.md).

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

The event read methods take `EventOption`s (also a separate type from `Option`, applying only to `EventsList`/`EventsWatch`) — see [Events](#events):

```go
func WithEventCategory(cat string) EventOption  // restrict to a single timeline
func WithEventType(t EventType) EventOption      // only Normal or only Warning
func WithEventReason(reason string) EventOption  // only runs with this reason
func WithEventLimit(n int) EventOption           // cap the number of runs returned / snapshotted
func WithEventsSince(t time.Time) EventOption    // only runs active at or after t
```
