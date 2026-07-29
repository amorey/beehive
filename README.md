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

- **Declarative core.** You write `spec`, the desired state. Controllers reconcile actual state toward it, working from current state rather than from a sequence of events. That is what makes the system self-healing: it converges on restart, and a missed event costs nothing because the next pass reads the same state anyway. A cold start is just a reconcile from stored desired state.

- **Coordination through the store.** Controllers never call each other. They read and write the shared store, and a change reaches another controller by being found there rather than delivered to it. Nothing is pushed: every write leaves a durable trace — a bumped generation, an owed-wake count, a deletion mark, a higher `resource_version` — and each driver scans for the trace it cares about. So a missed tick costs latency and nothing else. The record is still there next time.

- **Every driver is a tick.** Reconcile passes, garbage collection, dependency wakes and client watches are all periodic scans, each on its own interval (see [Periodic drivers](#periodic-drivers)). Those intervals are the latency the system runs at; two of them are yours to choose.

- **`spec`/`status` separation.** Only controllers write `status`, and the API enforces it: the user-facing `Client` has no status-write path, only the `Controller` surface does.

- **Schema-version migration.** `Spec` and `Status` are stored as opaque JSON, so reshaping a struct would break decoding of older rows. A per-kind `Migrator` converts an old blob up *on read*, before unmarshal. Spec and status version independently, and conversion is lazy — a row is re-stamped when it is next written, never by a bulk rewrite.

The reasoning behind each of these is recorded in [docs/adr](docs/adr/README.md), linked from the sections below.

## API

### Beehive

```go
func New(store Store, opts ...Option) (*Beehive, error)
func Register[Spec, Status any](bh *Beehive, gk GroupKind, c Controller[Spec, Status], opts ...Option) (ControllerClient[Status], error)
```

`Register` returns the kind's `ControllerClient`, the status-write surface, so your application can write status from its own goroutines without beehive handing it over through a callback. Registering a controller is the only way to get one, which is what keeps status writes limited to the kind's owner.

Where you pass an option decides its scope. `WithFullPassInterval` at `New` sets the default for every kind; at `Register` it overrides that one controller. An option a given call site doesn't recognize is ignored. `WithGCInterval` is global and therefore only meaningful at `New` — garbage collection covers kinds with no controller.

#### Periodic drivers

Every driver is one of these. They run on separate intervals because they are separate jobs with very different costs — a single interval would mean tuning one of them moves the rest:

| driver | what it scans | cost scales with | interval |
|---|---|---|---|
| owed pass | work the store *records* as owed — unconverged specs (`observed_generation < generation`) and owed dependency wakes | what is actually outstanding | 30s, fixed |
| full pass | **every** object of the kind, converged or not | the object count | `WithFullPassInterval`, default 0 (off) |
| GC sweep | deletion-pending rows, plus event-log retention | rows being deleted | `WithGCInterval`, default 30s |
| dependency wake | the write log above a watermark, waking dependents of what moved | what has **changed** since the last scan | 1s, fixed |
| stale dependents | dependents whose targets moved past the watermark their last pass recorded | the dependency graph | 60s, fixed |
| watch poll | current state, for each live `Client` watch | one cheap read per subscriber per tick; a full listing only when something changed | 1s, fixed |

**Only two of the six are configurable.** The other four are what make convergence a property of the system rather than a setting, and each is already bounded by what is outstanding, by what changed, or by the dependency graph rather than by what exists — so there is little to gain by moving them and a correctness hole to fall into by turning them off. If a cadence you need isn't here, that's a gap to report rather than one to work around.

The full pass is opt-in because it is the only driver whose cost is unbounded by outstanding work. It is also the only one that reaches an object the store records nothing about: state that belongs to a process and a restart invalidated, such as a liveness condition, which reads as "verifying" until a controller in *this* process rewrites it. Set it well above the 30s owed pass, which it subsumes.

Both of its cadences are off by default — `WithFullPassInterval` for the periodic one, `WithStartupFullPass` for the once-per-process one — and that is a correctness position, not a cost saving. **Nothing may depend on a full pass to converge.** Work that is genuinely owed is recorded in a column and drained by the owed pass and the GC sweeper, both of which run at every startup no matter how these two are set. Enable a full pass to re-confirm process-scoped state, and for nothing else: a convergence bug it happens to hide is a bug that comes back the moment an embedder turns it off or the object set outgrows what a sweep can carry.

To reconcile something sooner than the next pass, use `Client.Requeue` rather than shortening a cadence: it is a latency hint aimed at one object, where an interval is a cost paid by every object forever. The examples under `examples/` all do this — it is what lets them run on production defaults.

**GC cannot be disabled**: `WithGCInterval` rejects a non-positive interval with `ErrInvalidOption`. A long interval means "collect rarely"; there is no way to say "never". `WithFullPassInterval` can be set to 0, and startup logs when it is off, so a value left at 0 by accident is visible rather than silent.

→ [ADR: every driver is a periodic scan of the store](docs/adr/2026-07-28-periodic-scan-drivers.md), for why the cadences are separate, why GC alone is mandatory, and what each driver's cost is bounded by.

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

`Liveness` marks a condition that describes a live, in-process resource, and so is
only valid inside the process that wrote it. On read, a liveness condition left by an
earlier process is downgraded to `ConditionUnknown` ("verifying") until a controller
confirms it again. The default, `false`, means the condition is durable and survives
restarts.

### Event

Events are a per-object, append-only log of observations, grouped into runs. Consecutive records sharing `(Category, Type, Reason)` merge into one `Event`: its `Count` grows and its `[FirstAt, LastAt]` window widens. Change any of those three fields and a new run starts.

Runs are *consecutive*, not deduplicated globally. A value that comes back after a different one starts a fresh run, so a flapping object produces a timeline of alternating runs rather than one row that grows forever. Think of the log as the long form of a Condition: a Condition keeps only the current run per type, overwriting its `Status` and `Reason` in place, while the log keeps the history.

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

`Category` splits an object's log into independent timelines, one per `(object, category)`, so unrelated concerns — connection probes and config sync, say — never break each other's runs. `Category` and `Reason` are both free-form strings you choose per record, like `Condition.Reason`; declare typed string constants if you want a fixed, typo-proof vocabulary.

`Message` is *sampled*, not part of the run key. Recording the same `(Category, Type, Reason)` with a new message extends the current run and updates the message shown, rather than starting a new run.

`Detail` is the machine-readable companion to `Message`: an optional structured payload, so `ProbeFailed` might carry `{"endpoint":"10.0.0.1:443","latencyMs":5000}`. Like `Spec` and `Status` it goes in **typed and comes out opaque**. On write it is any JSON-marshalable value, which `EventsAdd` marshals. On read it is a `json.RawMessage` you decode when you need it, with `EventDetail[T](e)`. Decoding per event, with the type that event's `Reason` implies, is what lets one timeline mix reasons carrying different payload shapes without making the API generic.

`Detail` is sampled like `Message` — latest occurrence wins, and it is not part of the run key — so a payload that varies never splits a run. If you need every occurrence's payload, that event shouldn't aggregate: give it a unique `Reason`. Unlike `Spec` and `Status`, `Detail` is **not** schema-versioned, so reshaping it breaks decoding of older rows. That is tolerable only because retention ages events out; put a version inside the payload if you need more.

Only controllers write events. `ControllerClient.EventsAdd` is the only write path, because events are observations and, like `status`, have no user-facing writer. Reads live on `Client` (`EventsList`, `EventsWatch`, `EventsGetLatest`), plus the eager `LoadEvents()` / `Object.Events()` pair, which gates on being loaded exactly like the secondary lookups and returns `ErrNotLoaded` otherwise.

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

type ObjectRef = storeapi.ObjectRef // { ID ObjectID; Group, Kind string }
```

Secondary-lookup data is filled in only when the read asked for it, and you read it through the accessors below. They return `ErrNotLoaded` for a relation nobody requested, so forgetting a `Load*()` option fails loudly instead of looking empty. The return type carries the cardinality — `(ObjectRef, bool, error)` for the at-most-one owner, `([]ObjectRef, error)` for the rest — so the accessors need no verb in their names.

```go
func (o *Object[Spec, Status]) Owner() (ObjectRef, bool, error) // bool: an owner exists; err: not loaded
func (o *Object[Spec, Status]) Dependencies() ([]ObjectRef, error)
func (o *Object[Spec, Status]) Dependents() ([]ObjectRef, error)
func (o *Object[Spec, Status]) Owned() ([]ObjectRef, error)
func (o *Object[Spec, Status]) Events() ([]Event, error)
```

Once loaded, an empty slice — or `ok == false` from `Owner` — means there really are none. `ErrNotLoaded` means you forgot to ask: fetch the relation eagerly with a `Load*()` option, or lazily through the `Client`/`ControllerClient` methods below.

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

`Schedule` is what the [scheduling API](#scheduling) reports: an object's **next reconcile time**, as a gauge. It is a struct rather than a bare `time.Time` so fields can be added later without breaking anything — a reschedule trigger, for instance (backoff, success cadence, or manual poke), which is reserved but not yet filled in. `NextRequeueAt` covers per-id timers only: a pending backoff retry or `RequeueAfter` delay, or now if the object is already queued, or the zero time if nothing is scheduled.

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
    OwnersGet(ctx context.Context, id ObjectID) (ObjectRef, bool, error)
    DependenciesList(ctx context.Context, id ObjectID) ([]ObjectRef, error)
    DependentsList(ctx context.Context, id ObjectID) ([]ObjectRef, error)
    OwnedList(ctx context.Context, id ObjectID) ([]ObjectRef, error)
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

`Create` leaves the slug unset unless you pass `beehive.WithSlug`. It stays `nil`, stored as SQL `NULL`, and nothing is generated for you. NULL slugs never collide (`NULL != NULL` in SQLite), so a kind can hold any number of slugless objects; you reach them by `ObjectID` and `List`, just not by name. If you do pass a slug and it is taken, `Create` fails on the `UNIQUE ("group", kind, slug)` constraint. Everything after the create takes an `ObjectID`, so a delete and recreate under the same slug can't make you act on the wrong row. Finalizers and other metadata are options:

```go
client := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
obj, _ := client.Create(ctx, ClusterSpec{...}, beehive.WithSlug("prod-cluster"), beehive.WithFinalizers("kstack.sh/cluster"))
client.Update(ctx, obj.ID, ClusterSpec{...})
```

**A slug is an opaque key and beehive does not validate it** — no character rules, no length limit, no normalization. So the empty string is an ordinary slug: a real value under the unique constraint, and distinct from `NULL`. `GetOrCreate(ctx, "", spec)` creates *the* empty-slug object of that kind, and the next caller passing `""` gets that same row back with `created=false`, exactly as two callers passing `"prod"` would. That is the contract, not a collision bug, but it has a sharp edge: a slug read from configuration is `""` when the field is unset, which quietly points every such caller at one shared object. Validate slugs that come from outside your code, and when you mean "no name" pass no `WithSlug` at all rather than `""`.

The three slug-keyed writes differ **only in what they do when the slug is taken**, and that holds under concurrency. `CreateOrUpdate` and `GetOrCreate` do their read and write in one transaction, so two callers racing on a slug never both insert — the loser sees the winner's row and updates or returns it. `Create` does no lookup at all, so the loser of that race fails on `UNIQUE`, just as it would against a row that was already there:

| Slug already held by    | `Create`         | `CreateOrUpdate`     | `GetOrCreate`                         |
| ----------------------- | ---------------- | -------------------- | ------------------------------------- |
| nothing                 | creates          | creates              | creates, `created=true`               |
| a live row              | fails (`UNIQUE`) | updates it to `spec` | returns it untouched, `created=false` |
| a deletion-pending row  | fails (`UNIQUE`) | updates it to `spec` | returns it untouched, `created=false` |

Where the table says `CreateOrUpdate` "updates it", re-applying the spec the row already holds does nothing at all: no generation bump, no `resource_version` bump, and so nothing for a scan to find — no watch delivery, no reconcile. That matters when a controller re-applies a spec of its own kind on every pass, because the object stays settled instead of owing itself another pass forever. `Update` behaves the same way.

Every write **validates before it commits.** `Create`, `CreateOrUpdate`, `GetOrCreate` and `Update` decode the written row back into `Spec`/`Status` *inside* the transaction. A spec that marshals but does not round-trip — usually a `MarshalJSON`/`UnmarshalJSON` pair that disagree — rolls the write back instead of committing a row this process cannot read. **So an error from a write means nothing was committed:** no unreadable row, nothing added to a driver's listing, no `UNIQUE` left behind for the retry to trip on, and for `Update`/`CreateOrUpdate` the previous spec is still there. `GetOrCreate` returns `created=false` in that case, since nothing was created. The cost is that the write holds the store's single writer across the decode (`json.Marshal` still runs before the transaction opens). This only guards the write path — a row can still become unreadable later, say after a schema downgrade, which the read path handles by quarantining it (see [Migrator](#migrator)).

Use `GetOrCreate` when a controller has to make sure a child exists **without ever changing it**. The alternative is open-coding `GetBySlug` → `Create` → `GetBySlug` again on conflict, where the fallback path tends to drift out of step with the primary one. Its found branch writes nothing, so a deletion-pending row comes back as it is, with `DeletionRequestedAt` set, rather than being resurrected by a spec update:

The example uses two surfaces, and they are not interchangeable. `GetOrCreate` is on
`Client` — here the child kind's client, built with `NewClient` and held by the
controller. `EventsAdd` is on the `ControllerClient` that `Reconcile` receives,
for writes about the object being reconciled. `Client` has no `EventsAdd` and
`ControllerClient` has no `GetOrCreate`: a controller creates children through a
`Client` for their kind.

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
        // EventsAdd is about obj (this controller's object), not the child.
        if err := cc.EventsAdd(ctx, obj.ID, beehive.EventSpec{
            Category: "lifecycle", Reason: "ClusterCreated",
        }); err != nil {
            return beehive.Result{}, err
        }
    }
    return beehive.Result{}, nil
}
```

`created` reports whether this call inserted the row. A new object has a generation nothing has observed yet, so the owed pass picks it up, exactly as it would after `Create`. Returning an existing row writes nothing and so owes nothing. Neither case schedules anything at write time — the row is the record, so a rollback leaves nothing behind.

`created` is returned synchronously, so inside an enclosing `ControllerClient.Within` a `created=true` is provisional until that transaction commits. For a side effect that must run only if the row really lands, use `WithOnCreate` (below), which waits for the outermost commit.

The options apply **only when the call creates the row** (`WithOwner`, `WithFinalizers`, `WithOnCreate`). `WithSlug` is rejected with `ErrConflictingOption`, because the slug is already a positional argument here — the option could only contradict it, and dropping it silently would file the row under one slug while you looked for it under another. (Options that simply don't apply are still ignored, as everywhere else; this one is a caller mistake.)

That has a sharp edge worth stating plainly: since the found branch ignores the options, **`created=false` does not mean "exists and matches your options."** A row created earlier without `WithOwner` comes back with no owner edge, and a caller that assumes otherwise ends up with a child the GC cascade will never collect when the parent goes. If you depend on the owner edge, check it — `GetOrCreate` then `OwnersGet`, or `Get(ctx, id, LoadOwner())` — and fix the difference yourself. Beehive will not adopt the row for you: an object has at most one owner, so adding the edge to a row that already has a different one would give it two, and deciding which owner wins is your policy, not the library's.

`DeleteBySlug` is the other half of the pair: `GetOrCreate` creates if absent, `DeleteBySlug` deletes if present. Both are idempotent and both understand tombstones, so a controller that ensures a slug-keyed child on one branch and removes it on another writes one call for each. It replaces the usual open-coding of `GetBySlug`, treating `ErrNotFound` as success, treating `DeletionRequestedAt` as a no-op, then `Delete`:

| Slug held by           | `DeleteBySlug`                                              |
| ---------------------- | ----------------------------------------------------------- |
| nothing                | `nil` — already gone                                         |
| a live row             | soft-deletes it (sets `DeletionRequestedAt`), advances GC    |
| a deletion-pending row | no-op — no write at all — advances GC; `nil`                 |

Like `Delete`, it marks the object and hands it to the controller to clear its finalizers. The row is removed once they clear, and only then is the slug free again. It is scoped to the kind, like `GetBySlug`: another kind's row holding the same slug is simply not found, which is reported as success rather than as a wrong-kind error.

Looking the slug up is **atomic with the delete** — the slug goes into the store's `WHERE` clause rather than being resolved first and deleted after, so no concurrent collection can retire the row and hand its slug to a replacement in between. A `nil` return means "no object of this kind holds this slug", not "the row I resolved is gone". What it cannot promise, and no implementation could, is that the slug is still free when the call returns: a concurrent `GetOrCreate` may take it the instant the delete commits. As always, the next reconcile works from current state.

→ [ADR: slug-keyed writes](docs/adr/2026-07-27-slug-keyed-writes.md), for the transaction boundaries.

#### Watching

`ObjectsWatch` and `ObjectsWatchList` emit the current state as `Added` changes on start, then stream subsequent changes as `ObjectChange` values. The channel closes when `ctx` is cancelled.

**Do not open a watch inside `Within`.** The read below happens on your goroutine, and the store runs on a single connection — so it waits for the connection your transaction is holding, and the transaction cannot commit until it returns. (This is the general rule for `Within`: pass the ctx you were given to every store call inside it. A watch is the one call that has no right ctx to pass, since its stream must outlive the transaction.)

**Subscribe, then act.** Both read current state *before returning*, so a change you make after subscribing is measured against a snapshot that already exists — delete an object on the next line and its `Deleted` is guaranteed, where a snapshot taken one tick later could miss the object entirely. The cost is one store read on your goroutine, and if that read fails you get the error rather than a stream: a watch with no snapshot could not report that delete, and you would wait for a tombstone that never comes.

Both are **polls, not subscriptions.** Each remembers the `resource_version` it last reported to you and, on each watch-poll tick (1s), sends the difference: a new object is `Added`, a moved version is `Modified`, a row that has gone is `Deleted` and carries its last known state. Two things follow, and both are the level-triggered contract the rest of beehive keeps — you are told what *is*, never what happened:

- **Changes inside one interval collapse together.** Three writes between two polls produce one `Modified` carrying the third. An object created and deleted within a single interval is never reported at all — but not one that existed when you subscribed, since the snapshot above is what it is compared against.
- **Latency is the poll interval**, not the write. A quiet tick is cheap: reading the object write log's high-water mark is one indexed query, and only a mark that moved — or an object that vanished, since deletes draw no version — pays for the full listing. Writing to the event log does not move it, so an object watch stays quiet through a controller that records events on every pass.

A failed poll is logged and skipped rather than fatal, so the stream survives a transient store error instead of ending quietly under a subscriber with no way to notice. That applies to every poll after the first — the first one's failure is returned to you, since there is no earlier state for the stream to fall back on.

Both need a **registered controller** for the kind, as `EventsWatch` does, and both are scoped to it: `ObjectsWatch` on another kind's id streams nothing. The id need not exist yet — an absent object is just an empty listing, and the stream reports it as `Added` once it is created.

(The event *log* below, `EventsList`/`EventsWatch`, is a different thing: an `ObjectChange` says an object changed, an `Event` is a log entry.)

→ [ADR: every driver is a periodic scan of the store](docs/adr/2026-07-28-periodic-scan-drivers.md), for what a poll costs and the constraints any push path above it would have to satisfy.

#### Secondary lookups (owner / dependencies / dependents / owned)

An object's ref edges are fetched on request, two ways:

- **Eager** — pass `LoadOption`s to a read: `Get(ctx, id, LoadOwner())`, `List(ctx, LoadDependencies(), LoadDependents())`. The returned objects carry the data (read via the accessors). On `List` each relation is one batched query, not one per object.
- **Lazy** — call `OwnersGet` / `DependenciesList` / `DependentsList` / `OwnedList` when you actually need the data. These run the edge query directly, with no validating read in front, so they do **not** check the kind: another kind's id returns that kind's edges, and a missing id returns nothing, neither as `ErrNotFound`. Use them for ids the client owns.

`OwnedList` (and the eager `LoadOwned()` / `Object.Owned()`) is the inverse of `OwnersGet` over `owned_by`: it returns the objects a given owner owns, the same way `DependentsList` inverts `DependenciesList` over `depends_on`.

`OwnedObjectsList(ownerID)` is the typed version. `OwnedList` returns untyped `ObjectRef`s across every owned kind, leaving you to filter by `Kind` and `Get` each child through its own client. `OwnedObjectsList` returns decoded `*Object[Spec, Status]` children of **this client's kind** in a single query, because the kind filter and the row read fold into the edge join — no `Get` per child. Ordering (by id) and missing-owner behaviour match `OwnedList`. Deletion-pending children are included, so skip them yourself by checking `DeletionRequestedAt`. It takes the same `LoadOption`s as `List`, batched the same way; without them the children have nothing loaded and their accessors return `ErrNotLoaded`.

Eager and lazy run the same query — edges are always a separate indexed lookup, never joined into the `SELECT` that carries specs and statuses. Eager just attaches the result to the object and batches it across a `List`.

→ [ADR: secondary lookups](docs/adr/2026-07-27-secondary-lookups.md), for the loader sharing, the accessor naming rule, and the store's semi-join.

#### Reconcile control

`Requeue` queues an object for reconcile now, and is the only way to reconcile something without waiting for a tick. It is a **latency hint, not a synchronous run**: it returns once the object is queued, and a worker gets to it on its own schedule. Losing one is harmless whenever the store records that the object is owed a pass, because the owed pass finds it anyway. It is also how you drive reconciles yourself with every periodic driver switched off. Use it to re-examine an object promptly after state the controller reads has changed elsewhere.

By default `Requeue` **keeps the object's retry backoff**. A requeue is an ordinary nudge — a config change, a dependency update, a manual poke — and almost never proves the failure is over. The one thing that does prove it is a successful reconcile, which clears backoff already. So: **backoff is cleared by a successful reconcile or by an explicit `WithResetBackoff()`, never by a plain requeue.** Pass `beehive.WithResetBackoff()` only when you know the failure is resolved and the next retry should start from the base interval. (controller-runtime draws the same line between `Add`/`AddAfter` and `Forget`.)

`Requeue` checks the id against the client's kind first, returning `ErrNotFound` for a missing or foreign id, then requires a registered controller, returning `ErrNoController` for a client-only kind that has no reconcile loop. It is on `Client` only: a controller schedules itself with `Result.RequeueAfter` and reaches other objects through the store, never by poking another reconcile loop.

#### Scheduling

The scheduling API reports when an object is **next due to reconcile**, as a [`Schedule`](#schedule) whose `NextRequeueAt` is a pending backoff retry or `RequeueAfter` delay — or now, if the object is already queued, or the zero time if nothing is scheduled.

`SchedulesGet` is the point read: a non-blocking read of in-memory state, with no store lookup and no kind check, so it returns no error today (the error is reserved for symmetry with the rest of the surface). A missing id, another kind's id and a client-only kind all read as the zero `Schedule`, which looks the same as a real object with nothing scheduled.

`SchedulesWatch` streams the same value as a **gauge**: the current one on subscribe, then a new `Schedule` whenever it changes — a backoff step, a `RequeueAfter`, a pass or dependency wake, a dispatch, a `Requeue`. None of those fire `ObjectsWatch`/`ObjectsWatchList`, since rescheduling bumps no generation or resource version, and no other signal covers them all. So this is the way to watch reschedules — for example to drive a "next attempt" countdown that stays accurate while an object's spec and status sit still. It polls on the same 1s cadence as the object watches and emits only on change, which means it converges on the current value and may skip values in between. The channel closes when `ctx` is cancelled. Unlike `SchedulesGet` it returns `ErrNoController` for a client-only kind, since a stream that can never emit should say so rather than hang, but the id need not exist: an unscheduled id streams the zero `Schedule` until something schedules it.

Both are on `Client` only, and both read **per-id timers only**. Neither predicts the next reconcile: the real one can come **earlier**, because the owed pass, the full pass and the dependency wake are not per-id timers, and **a zero `NextRequeueAt` means "nothing scheduled", not "will not reconcile"**. Treat it as observability, not a guarantee.

→ [ADR: the schedule watch](docs/adr/2026-07-27-schedule-watch.md), for why it is an in-memory gauge rather than an event-log surface.

#### Events

`EventsList` returns an object's runs newest first (by `LastAt`). `WithEventCategory` narrows to one timeline, and the other `EventOption`s filter by type, reason or time, or cap how many come back. `EventsWatch` sends the current runs first, then streams new runs and extensions to existing ones, on the same interval and the same snapshot-then-poll contract as the object watches. Runs are matched by `EventID`, so you see at most one update per run per interval — a count bump updates the run in place instead of arriving as a new one. There are no tombstones, since an append-only log means a run can only appear or grow. `EventsGetLatest` returns the current run in a category, with a `bool` that folds away the no-events-yet case like `OwnersGet` does.

`WithEventRetention` bounds the log per `(object, category)`: a ring that keeps the newest N runs in each timeline, so a flapping timeline can't evict a quiet one on the same object, plus an optional maximum age. The GC sweeper enforces it, and deleting an object deletes its events.

→ [ADR: the events API](docs/adr/2026-07-27-events-api.md), for the run-aggregation rule, why `Detail` stays off the generic boundary, and the watch-surface naming.

### ControllerClient

```go
type ControllerClient[Status any] interface {
    UpdateStatus(ctx context.Context, id ObjectID, observedGeneration int64, status Status) error
    ConditionsSet(ctx context.Context, id ObjectID, condition Condition) error
    ConditionsDelete(ctx context.Context, id ObjectID, conditionType string) error
    EventsAdd(ctx context.Context, id ObjectID, event EventSpec) error
    FinalizersDelete(ctx context.Context, id ObjectID, finalizer string) error
    DependenciesAdd(ctx context.Context, fromID, toID ObjectID, targetResourceVersion int64) error
    DependenciesDelete(ctx context.Context, fromID, toID ObjectID) error
    EdgesHasIncoming(ctx context.Context, id ObjectID) (bool, error)
    // Lazy secondary lookups, for reading an object's edges during reconcile.
    OwnersGet(ctx context.Context, id ObjectID) (ObjectRef, bool, error)
    DependenciesList(ctx context.Context, id ObjectID) ([]ObjectRef, error)
    DependentsList(ctx context.Context, id ObjectID) ([]ObjectRef, error)
    OwnedList(ctx context.Context, id ObjectID) ([]ObjectRef, error)
    Within(ctx context.Context, fn func(ctx context.Context) error) error
}
```

`UpdateStatus` **does nothing when the status marshals to the bytes already stored**. There is no `resource_version` bump, so a watch poll and the dependency waker both find nothing — the same way re-applying an unchanged spec does nothing on the `Client` side. So report observed state unconditionally; you don't need your own equality check, and a dependent riding on this kind's status won't be woken by a pass that found nothing new.

The generation handshake is the exception. `observedGeneration` and `ObservedAt` are recorded even when the content is unchanged, so a reconcile that legitimately changed no status still settles the object instead of being re-queued by every owed-pass tick. That write does bump `resource_version`, so a watcher waiting for `ObservedGeneration == Generation` sees the object converge. It happens at most once per generation: the next unchanged pass finds the generation already recorded and writes nothing.

`ObservedAt` therefore records **when the object settled at `ObservedGeneration`**, not when the controller last ran — don't use it as a liveness check, since a reconcile that never calls `UpdateStatus` never moves it either. For "when did we last look", record an event instead: `EventsAdd` extends the current run and bumps its `LastAt` every time, which is that signal, retained and aggregated.

→ [ADR: the generation handshake and content no-ops](docs/adr/2026-07-27-generation-handshake-and-noop-writes.md), for how the no-op splits the two halves of the write and why it is gated on the schema version.

`OwnersGet`, `DependenciesList`, `DependentsList` and `OwnedList` are the same lazy lookups the `Client` has. `Reconcile` is handed its object directly, with no read call of its own, so these are how it reads related edges. `OwnersGet` returns the owner over `owned_by` and `OwnedList` the reverse, the owner's children; `DependentsList` is the reverse of `DependenciesList` over `depends_on`.

`EdgesHasIncoming` is a different question, used by GC: does anything with a live claim still point at `id`? That means an owned child, or a dependent that is not itself being deleted — one that is going away has no claim. You cannot rebuild it from `DependentsList`, because it folds in owned children as well. A finalizer can wait on it: a controller holding a shared connection clears its finalizer only once nothing with a live claim references the object, so the connection outlives its last real user.

`EventsAdd` adds an observation to the object's event log — see [Event](#event). Adding is not always an insert: repeating the latest run's `(Category, Type, Reason)` extends that run instead of appending a second one, which is what lets a controller report every poll without growing the log per poll. Like `ConditionsSet` it is scoped to the controller's kind (`ErrWrongKind` for another kind's id) and composes inside `Within`, so a controller can record an event and flip a condition together.

### Controller

```go
type Controller[Spec, Status any] interface {
    Reconcile(ctx context.Context, client ControllerClient[Status], obj *Object[Spec, Status]) (Result, error)
}
```

A controller has **no lifecycle** in beehive. It implements `Reconcile` and nothing else, and receives the kind's `ControllerClient` as a parameter. Background work — timers, subscriptions, engines — belongs to your application, which already has its own lifecycle and can get a `ControllerClient` from `Register`. Beehive owns only the reconcile lifecycle: the work queue, backoff, the periodic drivers and shutdown ordering.

`Reconcile` is **not** wrapped in a transaction. Each `ControllerClient` write commits on its own, so a write that lands before `Reconcile` returns an error stays committed. The next pass works from the stored state, so write `Reconcile` to be idempotent. Each write is still atomic on its own, and the generation handshake covers a concurrent spec change racing the `obj` you were handed: `UpdateStatus` rejects a generation from the future, and an older one leaves the object unsettled so it reconciles again.

When several writes must land together or not at all, wrap them in `ControllerClient.Within(ctx, func(ctx) error { … })`. Writes made with the inner `ctx` join one transaction, which commits when the function returns `nil` and rolls back on error — `Client` writes included. That transaction holds the store's single write lock for as long as the function runs, so keep external I/O out of it. Nothing waits on it, because nothing is scheduled: a rolled-back transaction leaves no rows, so no driver can list them. That makes it safe to create or delete children inside `Within`. The one thing deferred past the commit is `WithOnCreate`, which is skipped on rollback. → [ADR](docs/adr/2026-07-27-slug-keyed-writes.md)

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

Attach a `Migrator` per kind by passing `WithMigrator` to `Register`. The store records the version each blob was written at in two per-row columns, one for spec and one for status. On read, a blob below the current version goes through `ConvertSpec`/`ConvertStatus`; an equal version passes through, as does anything when the current version is `0` ("not versioned"); a *higher* version means the data was written by a newer build and is rejected as a decode error. `from == 0` is the unversioned baseline, so once you enable a migrator its converters have to handle it.

Conversion is lazy and per column: a blob is re-stamped when it is next written, so a status-only write re-stamps only the status version.

A blob that fails to convert, fails to unmarshal, or came from a newer build is a decode failure, and each read path handles it in the way that fails safest:

- `List` and the watches skip the bad row, log it and carry on. A watch remembers its version, so it warns once per change rather than once per poll.
- `Get`/`GetBySlug` return the error.
- **The reconcile loop quarantines the row.** It cannot reconcile what it cannot decode, and the bytes will not change until someone rewrites the spec, so it logs and treats the pass as a successful no-op rather than retrying the same bytes forever under backoff. A deletion-pending row is still collected, since GC needs only the id. The owed pass re-queues the unsettled row every tick, so the warning repeats at that interval — deliberately, so a bad row stays visible instead of logging once and going quiet.

A kind with no migrator is untouched; its columns stay `0`. Only registered kinds can have a migrator, so client-only kinds cannot.

→ [ADR: schema-version migration](docs/adr/2026-07-27-schema-version-migration.md), for convert-on-read / stamp-on-write and why stamping is never downward.

### Options

```go
type Option interface{ apply(any) }

func WithSlug(slug string) Option                  // set a human-readable slug; fails if already exists
func WithFinalizers(f ...string) Option            // declare finalizers before the object is visible to controllers; registered kinds only
func WithOwner(id ObjectID) Option                 // declare owned_by edge; owner cannot be deleted while this object exists
func WithOnCreate(fn func(ctx context.Context)) Option // run fn after the create commits (Create always; GetOrCreate only when it inserts)
func WithFullPassInterval(d time.Duration) Option  // how often to re-dispatch EVERY object (default: 0, off)
func WithGCInterval(d time.Duration) Option        // how often to collect dead rows + prune the event log (default: 30s; New only; must be > 0)
func WithStartupFullPass(enabled bool) Option      // also re-dispatch settled objects once at startup (default: false, off)
func WithMaxRetryInterval(d time.Duration) Option  // cap on exponential backoff after Reconcile errors (default: 30s)
func WithMigrator(m Migrator) Option               // attach a schema-version Migrator for the kind (Register only)
func WithEventRetention(perObject int, maxAge time.Duration) Option // event-log retention: per-(object,category) cap-N ring + optional age bound (0 = no age bound)
```

`WithOwner` writes an `owned_by` edge in the same transaction as the `Create`. Deleting the owner then cascades to the child through GC.

`WithFinalizers` is the one create option that needs a kind **this process has registered a controller for**; otherwise the call fails with `ErrInvalidOption`. Only `ControllerClient.FinalizersDelete` can clear a finalizer, and it folds the calling controller's own kind into the write — so a client-only kind's finalizer is removable by nothing, and the row would stay deletion-pending forever while its `owned_by` edge blocks its owner's delete.

The check is **process-local and evaluated at call time**, since the store records no registrations: it also refuses a create issued before this process's own `Register`, and one from a process that never registers the kind even if another over the same store does. Register the kind first, from whichever process creates these rows. It runs before any store work and only when the option is used, so an ordinary create on a client-only kind is unaffected — and like every other create-option check it is eager, so `GetOrCreate` rejects it on the found branch too rather than only when a row is really inserted.

`WithOnCreate` is the safe way to run a side effect only if the row is really created — an external call, an in-memory counter. It waits for the *outermost* commit, so it runs once and never after a rollback; it is the only thing in beehive deferred that way. `Create` always fires it, `GetOrCreate` only when it inserts. Prefer it to branching on `GetOrCreate`'s `created` bool, which is returned synchronously: inside an enclosing `ControllerClient.Within` that bool is set before the transaction commits, so acting on it fires your side effect for a row a rollback may still discard.

`DependenciesAdd` and `DependenciesDelete` manage `depends_on` edges during reconcile. When a target changes, the next dependency-wake scan queues the dependent. Each commits on its own, or joins a `Within` the controller opened.

The target can be **any** kind, including one you only ever use through `Client` and never register — configuration, secrets, any reference data your app writes and your controllers read. The waker scans the whole store's write log rather than only the kinds with controllers, so such a target wakes its dependents like any other.

Every call that **creates** the edge records, durably and atomically with the edge itself, that the dependent owes a reconcile (a count on the row, `reconcile_owed`, drained by the owed pass). That one rule covers every way a declare could otherwise miss: a change to the target landing between your read and the edge's commit, a declare made on another object's behalf while that object's own reconcile is mid-flight, and a crash before the wake is serviced. Re-asserting your edges on every pass costs nothing after the first, because only the call that created the edge records anything — the cost is one reconcile per edge ever created.

`DependenciesAdd` also takes `targetResourceVersion`: the `ResourceVersion` of the target *as you read it when deciding to depend on it*, not one fetched fresh for the call. A version *above* the target's current one is rejected with `ErrTargetResourceVersionFuture`, since versions only move forward and it cannot have come from reading the target — the one wrong value the call can detect, usually a version read from the wrong object. Nothing is written before that check, so no edge is declared even if you call it inside your own `Within` and ignore the error. Pass `0` for "no opinion", which is right when you declare the edge *before* reading the target.

**A dependency wake is a guarantee, not a best effort.** The scan above is fast and lives in memory, so a crash, a restart, or a process that never ran one can drop a wake — and a dependent that has already settled is invisible to every listing of owed work, because its own generation never moved. So beehive records, on each successful reconcile of an object that has dependencies, the store-wide write cursor that pass observed; a slower pass (60s) then enqueues every dependent whose targets have moved past it. Nothing about that is bookkeeping you can lose: it compares current state, so it recovers a wake lost by any means. A failed reconcile records nothing and is therefore found again. What you get is a wake within a second in the ordinary case, and within a minute in every case.

→ [ADR: stamp every new dependency edge](docs/adr/2026-07-29-stamp-every-new-dependency-edge.md), for how the count is kept atomic with the edge and why the stamp is unconditional on the claim. The waker itself is [a periodic scan of the write log](docs/adr/2026-07-28-periodic-scan-drivers.md), and the backstop under it is [dependency watermarks](docs/adr/2026-07-29-dependency-watermarks.md).

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
