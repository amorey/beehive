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
  // Register returns the kind's ControllerClient for status writes from outside
  // a reconcile (background work belongs to the app, not beehive);
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

- **Embedded, single-process.** Beehive runs inside your app, not beside it: no server, no daemon, no network hop. In exchange, a store belongs to one process running one `Beehive`, which is its only writer while it runs. Restarts are supported but *concurrent* access from a separate process is not.

- **Declarative core.** You write `spec`, the desired state. Controllers reconcile actual state toward it, working from current state rather than from a sequence of events. That is what makes the system self-healing: it converges on restart, and a missed event costs nothing because the next pass reads the same state anyway. A cold start is just a reconcile from stored desired state.

- **Coordination through the store.** Controllers never call each other. They read and write the shared store, and a change reaches another controller by being found there rather than delivered to it. Nothing is pushed: every write leaves a durable trace — a bumped generation, an owed-wake count, a deletion mark, a higher `resource_version` — and each driver scans for the trace it cares about. So a missed tick costs latency and nothing else. The record is still there next time.

- **Almost every driver is a tick.** Reconcile passes, garbage collection and client watches are all periodic scans, each on its own interval (see [Drivers](#drivers)). Those intervals are the latency the system runs at; two of them are yours to choose. Object watches also have a commit wake in front of their tick, so a write made through this `Beehive` reaches them without waiting for the tick. Dependency wakes are the one driver with no tick at all: a write made through this `Beehive` wakes them, and a 60s pass over dependency watermarks is what covers a write that did not.

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

#### Drivers

Every driver is one of these. They run on separate intervals because they are separate jobs with very different costs — a single interval would mean tuning one of them moves the rest. One of them has no interval at all: the dependency wake runs when a write commits and not otherwise.

| driver | what it scans | cost scales with | interval |
|---|---|---|---|
| owed pass | work the store *records* as owed — unconverged specs (`observed_generation < generation`) and owed dependency wakes | what is actually outstanding | `WithOwedPassInterval`, default 30s |
| full pass | **every** object of the kind, converged or not | the object count | `WithFullPassInterval`, default 0 (off) |
| GC sweep | deletion-pending rows, event-log retention, then the free space those two leave behind | rows being deleted | `WithGCInterval`, default 30s |
| dependency wake | the write log above a watermark, waking dependents of what moved | what has **changed** since the last scan | none: a commit wakes it |
| stale dependents | dependents whose targets moved past the watermark their last pass recorded | the dependency graph | `WithStaleDependentsInterval`, default 60s |
| watch tail | the write log of each watched kind, once per kind however many watches it has | one cheap read per watched kind per tick, and a commit wakes it before the tick | `WithWatchFloorInterval`, default 30s |
| event watch | one object's event log above a cursor, for each live `WatchEvents` | what the log has grown by, and a commit wakes it before the tick | `WithWatchFloorInterval`, default 30s |

**Every cadence here is configurable, and only the full pass can be switched off.** That is deliberate on both counts. A tick is no longer how work is found — every trigger for a registered kind pushes at commit, and both watch families read on a commit wake — so lengthening one buys a mostly-idle process a much quieter store while costing recovery time on a *lost* push, which is a trade an embedder is entitled to make. Turning one off is not, because each is the only thing that re-derives its own class of work. → [ADR: the driver cadences are configurable](docs/adr/2026-08-06-driver-cadences-are-configurable.md)

The full pass is opt-in because it is the only driver whose cost is unbounded by outstanding work. It is also the only one that reaches an object the store records nothing about: state that belongs to a process and a restart invalidated — a liveness condition that reads as "verifying" until a controller in *this* process rewrites it, but equally a live connection, a running worker, an open watch. Set it well above the owed pass, which it subsumes.

Both cadences are off by default — `WithFullPassInterval` for the periodic one, `WithStartupFullPass` for the once-per-process one — but for different reasons, and only one of them is a correctness position.

**Nothing may depend on the *periodic* full pass to converge.** Its cost is unbounded by outstanding work and it repeats forever, so a convergence bug it hides comes back the moment an embedder lengthens it or the object set outgrows what a sweep can carry. Work genuinely owed is recorded in a column and drained by the owed pass and the GC sweeper, both of which run at every startup no matter how these two are set.

**The startup pass is different: a kind may, and sometimes must, depend on it.** It runs once per process at O(objects) — the owed pass's own worst case — so it is not the unbounded driver the rule above is about. Enable it for a kind whose reconcile establishes *in-process* state, and for nothing else. That splits two ways:

- *Reporting* state — a liveness condition that reads "verifying" until a controller in this process rewrites it. The object is converged either way; only the display is stale.
- *Load-bearing* state — the reconcile opens a connection, starts a worker, holds a watch. Here the object is **not** converged until this process has reconciled it, and no store column can say so: `observed_generation == generation` was written by a process that is gone, so every store-visible measure reads settled.

For the second class the startup pass is the convergence mechanism, and what it guarantees is exactly that: **every object of a kind that enables it is reconciled at least once per process.** Declare it at `Register`, per kind, so the kinds that own in-process state say so and the rest don't pay. → [ADR: the startup full pass may be depended on](docs/adr/2026-08-07-the-startup-pass-may-be-depended-on.md)

To reconcile something sooner than the next pass, use `Client.Requeue` rather than shortening a cadence: it is a latency hint aimed at one object, where an interval is a cost paid by every object forever. The examples under `examples/` all do this — it is what lets them run on production defaults. `examples/lowpower` is the exception, and shows the other side: every cadence at minutes, with the pushes alone carrying the demo.

**Four of the five cadences cannot be disabled**: `WithGCInterval`, `WithOwedPassInterval`, `WithStaleDependentsInterval` and `WithWatchFloorInterval` each reject a non-positive interval with `ErrInvalidOption`. A long interval means "rarely"; there is no way to say "never". `WithFullPassInterval` can be set to 0, and startup logs when it is off, so a value left at 0 by accident is visible rather than silent.

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

    // Set by the store on read, ignored on write.
    Unconfirmed    bool      // Status is a downgrade this process derived, not a write
    TransitionedAt time.Time // when Status last changed
    UpdatedAt      time.Time // when the condition was last written at all
}
```

`TransitionedAt` and `UpdatedAt` are the two clocks a condition carries. `UpdatedAt`
moves on every write that changes the condition — a new `Reason` or `Message` counts,
a byte-identical rewrite does not. `TransitionedAt` moves only when `Status` itself
flips, so "how long has this been Ready" is `time.Since(cond.TransitionedAt)` and
"how fresh is this observation" is `UpdatedAt`. Both are decided by the store, so
whatever you put in the `Condition` you hand to `SetCondition` or `SetConditions` is
discarded.

A liveness condition downgraded to `ConditionUnknown` on read keeps the stamps of the
stored write: the downgrade is derived per process, not written, so `TransitionedAt`
describes the last stored status change rather than the downgrade. `Reason` and
`Message` are the stored write's for the same reason — they say what the condition
last *was*, not what this `Unknown` means.

`Unconfirmed` is how you tell that apart from an `Unknown` a controller in this
process wrote deliberately, having looked and been unable to say. The two are the same
on the wire otherwise, and the rule that separates them — "written before this process
started" — is one only the store can evaluate, so it reports the answer rather than
its inputs. Branch on `Unconfirmed` alone; it is set only by the downgrade, so it
already implies both `ConditionUnknown` and `Liveness`.

```go
switch {
case cond.Unconfirmed:
    fmt.Printf("unconfirmed since restart — last known %s\n", cond.Reason)
case cond.Status == beehive.ConditionUnknown:
    fmt.Printf("cannot tell: %s\n", cond.Message)
default:
    fmt.Printf("%s since %s\n", cond.Status, cond.TransitionedAt)
}
```

That last branch is the trap `Unconfirmed` exists to close: a downgraded condition's
`TransitionedAt` predates the restart, so rendering "since" against it would date a
status this process never established.

`Liveness` marks a condition that describes a live, in-process resource, and so is
only valid inside the process that wrote it. On read, a liveness condition left by an
earlier process is downgraded to `ConditionUnknown` ("verifying") until a controller
confirms it again. The default, `false`, means the condition is durable and survives
restarts. See [the ADR](docs/adr/2026-08-07-a-downgraded-liveness-condition-says-so.md).

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

`Detail` is the machine-readable companion to `Message`: an optional structured payload, so `ProbeFailed` might carry `{"endpoint":"10.0.0.1:443","latencyMs":5000}`. Like `Spec` and `Status` it goes in **typed and comes out opaque**. On write it is any JSON-marshalable value, which `AddEvent` marshals. On read it is a `json.RawMessage` you decode when you need it, with `EventDetail[T](e)`. Decoding per event, with the type that event's `Reason` implies, is what lets one timeline mix reasons carrying different payload shapes without making the API generic.

`Detail` is sampled like `Message` — latest occurrence wins, and it is not part of the run key — so a payload that varies never splits a run. If you need every occurrence's payload, that event shouldn't aggregate: give it a unique `Reason`. Unlike `Spec` and `Status`, `Detail` is **not** schema-versioned, so reshaping it breaks decoding of older rows. That is tolerable only because retention ages events out; put a version inside the payload if you need more.

Only controllers write events. `ControllerClient.AddEvent` is the only write path, because events are observations and, like `status`, have no user-facing writer. Reads live on `Client` (`ListEvents`, `WatchEvents`, `GetLatestEvent`), plus the eager `LoadEvents()` / `Object.Events()` pair, which gates on being loaded exactly like the secondary lookups and returns `ErrNotLoaded` otherwise.

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
    Name                string   // required and immutable; the key the Client API addresses the row by
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

`Schedule` is what the [scheduling API](#scheduling) reports: an object's **next reconcile time**, as a gauge. It is a struct rather than a bare `time.Time` so fields can be added later without breaking anything — a reschedule trigger, for instance (backoff, success cadence, or manual poke), which is reserved but not yet filled in. `NextRequeueAt` covers per-id timers only: a pending backoff retry, a `RequeueAfter` delay, or a re-enqueue floor holding a wake until the object may run again — or now if the object is already queued, or the zero time if nothing is scheduled.

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
    Object *Object[Spec, Status] // nil on a Deleted whose row image no longer decodes
}

type ObjectStream[Spec, Status any] struct {
    Object          *Object[Spec, Status] // nil when the id holds nothing yet
    ResourceVersion int64                 // the log position the snapshot is complete as of
    Changes         <-chan ObjectChange[Spec, Status]
}

type ObjectListStream[Spec, Status any] struct {
    Objects         []*Object[Spec, Status]
    ResourceVersion int64 // the log position the snapshot is complete as of
    Changes         <-chan ObjectChange[Spec, Status]
}

// Err, on either stream, reports why Changes closed: ErrWatchTooOld,
// ErrWatchTooNew, ErrStopped, or nil for the caller's own cancellation.
func (s *ObjectStream[Spec, Status]) Err() error

type Client[Spec, Status any] interface {
    // Creating: the name is positional, because there is no id yet.
    Create(ctx context.Context, name string, spec Spec, opts ...Option) (*Object[Spec, Status], error)
    GetOrCreate(ctx context.Context, name string, spec Spec, opts ...Option) (*Object[Spec, Status], bool, error)

    // Id-keyed: acts on one incarnation, or returns ErrNotFound.
    Update(ctx context.Context, id ObjectID, spec Spec) (*Object[Spec, Status], error)
    Get(ctx context.Context, id ObjectID, loads ...LoadOption) (*Object[Spec, Status], error)
    Delete(ctx context.Context, id ObjectID) error
    List(ctx context.Context, loads ...LoadOption) ([]*Object[Spec, Status], error)

    // Name-keyed: acts on whatever holds the name now.
    UpdateByName(ctx context.Context, name string, spec Spec) (*Object[Spec, Status], error)
    GetByName(ctx context.Context, name string, loads ...LoadOption) (*Object[Spec, Status], error)
    DeleteByName(ctx context.Context, name string) error // idempotent: absent or already-deleting is a nil no-op

    // Watching: a snapshot plus the changes above it. Kind-scoped; no controller
    // needed; follows one incarnation, so an id holding nothing is a nil Object
    // rather than ErrNotFound, and the stream ends at Deleted.
    Watch(ctx context.Context, id ObjectID, opts ...WatchOption) (*ObjectStream[Spec, Status], error)
    WatchList(ctx context.Context, opts ...WatchOption) (*ObjectListStream[Spec, Status], error)

    // Lazy secondary lookups — the on-demand counterparts to the Load options.
    GetOwner(ctx context.Context, id ObjectID) (ObjectRef, bool, error)
    ListDependencies(ctx context.Context, id ObjectID) ([]ObjectRef, error)
    ListDependents(ctx context.Context, id ObjectID) ([]ObjectRef, error)
    ListOwned(ctx context.Context, id ObjectID) ([]ObjectRef, error)
    // The typed, kind-scoped form of ListOwned: this kind's decoded children.
    ListOwnedObjects(ctx context.Context, ownerID ObjectID, loads ...LoadOption) ([]*Object[Spec, Status], error)
    WatchOwnedObjects(ctx context.Context, ownerID ObjectID, opts ...WatchOption) (*ObjectListStream[Spec, Status], error)

    // Event log — per-object, category-partitioned, contiguous-run aggregated.
    ListEvents(ctx context.Context, id ObjectID, opts ...EventOption) ([]Event, error)
    GetLatestEvent(ctx context.Context, id ObjectID, category string) (Event, bool, error)
    WatchEvents(ctx context.Context, id ObjectID, opts ...EventOption) (*EventStream, error)

    // Reconcile control.
    Requeue(ctx context.Context, id ObjectID, opts ...RequeueOption) error // requeue now; preserves backoff unless WithResetBackoff()

    // Scheduling — observe the next-requeue time.
    GetSchedule(ctx context.Context, id ObjectID) (Schedule, error)          // current schedule (zero if nothing scheduled)
    WatchSchedule(ctx context.Context, id ObjectID) (<-chan Schedule, error) // stream the schedule live as a gauge
}

func NewClient[Spec, Status any](bh *Beehive, gk GroupKind) Client[Spec, Status]
```

#### Writes

**The id is the key; the name is how you find it.** Every object is named at
creation — the name is positional on `Create`, because there is no id yet — and it
is unique and immutable thereafter. Everything after the create takes an
`ObjectID`: the same key the store uses for incarnation identity, foreign-key
targets, the work queue and scan ordering. The `…ByName` siblings resolve a name to
whatever holds it now, for callers who have a name and no id. Finalizers and other
metadata are options:

```go
client := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
obj, _ := client.Create(ctx, "prod-cluster", ClusterSpec{...}, beehive.WithFinalizers("kstack.sh/cluster"))
client.Update(ctx, obj.ID, ClusterSpec{...})
```

The name is required and immutable — there is no `UpdateName`, and a rename is
delete+recreate. If it is already taken, `Create` returns `ErrNameTaken`; use
`GetOrCreate` when "already there" is an acceptable outcome. A deletion-pending row
still holds its name, so a tombstone reports `ErrNameTaken` too, until GC clears the
finalizers and removes it.

For objects with no natural name, `beehive.GenerateName(prefix)` returns the prefix
joined to a fresh UUIDv7 — time-ordered, so names sharing a prefix sort by creation.
Nothing generates a name implicitly: a name the caller never chose is a name nobody
can look up, which is the nullable name this API retired. Passing it positionally
keeps the value in your hands, where you can log it or write it into a sibling's spec
before the create:

```go
// "cache-018f3a5c-8b2e-7c3d-a4f5-6b7c8d9e0f10"
obj, err := client.Create(ctx, beehive.GenerateName("cache"), spec)
```

It returns a bare `string`, with no error to handle: the random bytes come from
`crypto/rand.Read`, which is documented never to return an error — it crashes the
program if the OS entropy source fails, which is the right answer for an unusable
source and not something a name helper could improve on.

It is collision-resistant, not collision-proof, and nothing but the store can settle
that atomically — a lookup before the create would be a TOCTOU race. So a caller
generating names should bound-retry on `ErrNameTaken` and no other error. Reaching
the bound means generation is broken, not that you were unlucky.

**Which key you use decides what a call acts on**, and the two answers differ
exactly when a name has been reused:

> A **name-keyed** call acts on whatever holds that name *now*, or reports absence.
> An **id-keyed** call acts on that one incarnation, or returns `ErrNotFound`.

**The id is the key, and the `ByName` siblings are the opt-out.** The bare verbs
take an `ObjectID`, so a delete and recreate under the same name cannot make you act
on the wrong row — the safe thing is what you get by not thinking about it. Reach
for `ByName` when acting on whatever holds the name now is what you actually mean:
"ensure this child exists" / "remove this child" is a statement about a *name*, and
re-evaluating it against current state on every reconcile is the level-triggered
principle, not a compromise.

That is why **read-modify-write needs no rule**. The object a read returns carries
`ID`, so the natural way to write it back is already the incarnation-safe one:

```go
obj, _ := client.GetByName(ctx, "prod-cluster")
obj.Spec.Replicas++
client.Update(ctx, obj.ID, obj.Spec)   // this incarnation, or ErrNotFound
```

Composing `GetByName` → mutate → `UpdateByName` names the row twice, and a GC
collect plus a fresh create in between would land the write on a different
incarnation. The window is narrower than it looks — a tombstone holds the name's
`UNIQUE` constraint until GC clears finalizers, so opening it takes a full collect
*plus* a new create — but it is real, and using the id closes it.

Each `ByName` call is atomic on its own: it resolves and writes in one transaction,
never two store calls. The hazard is only in composing two of them.

**A name is an opaque key and beehive does not validate it** — no character rules, no length limit, no normalization — with exactly one exception: **the empty string is rejected with `ErrInvalidName`**, by the writes and the reads alike. `""` is not a name anyone chooses; it is what an unset configuration field reads as, and treating it as an ordinary name would quietly point every caller whose config was unset at one shared row. Every other malformed name at least addresses the row its author meant. Validate names that come from outside your code; beehive only catches the one case where the mistake is invisible. The store enforces it too, not just the client — `Store` is a public extension point, and a row admitted under `""` is one no name-keyed call could address again.

The two name-keyed creates differ **only in what they do when the name is taken**, and that holds under concurrency. `GetOrCreate` does its read and write in one transaction, so two callers racing on a name never both insert — the loser sees the winner's row and returns it. `Create` does no lookup at all, so the loser of that race fails on `UNIQUE`, just as it would against a row that was already there:

| Name already held by    | `Create`         | `GetOrCreate`                         |
| ----------------------- | ---------------- | ------------------------------------- |
| nothing                 | creates          | creates, `created=true`               |
| a live row              | fails (`UNIQUE`) | returns it untouched, `created=false` |
| a deletion-pending row  | fails (`UNIQUE`) | returns it untouched, `created=false` |

**There is no name-keyed upsert.** Neither create branch ever writes to a row it found, so changing an existing object is always a separate `Update`. A caller that wants ensure-then-set composes `GetOrCreate` with `Update` — on the id it just got back — inside its own `Within` — and should think about the deletion-pending row before it does, since `GetOrCreate` hands that back like any other and the `Update` would write a spec onto an object being torn down.

Re-applying the spec a row already holds does nothing at all: no generation bump, no `resource_version` bump, and so nothing for a scan to find — no watch delivery, no reconcile. That matters when a controller re-applies a spec of its own kind on every pass, because the object stays settled instead of owing itself another pass forever.

Every write **validates before it commits.** `Create`, `GetOrCreate` and `Update` decode the written row back into `Spec`/`Status` *inside* the transaction. A spec that marshals but does not round-trip — usually a `MarshalJSON`/`UnmarshalJSON` pair that disagree — rolls the write back instead of committing a row this process cannot read. **So an error from a write means nothing was committed:** no unreadable row, nothing added to a driver's listing, no `UNIQUE` left behind for the retry to trip on, and for `Update` the previous spec is still there. `GetOrCreate` returns `created=false` in that case, since nothing was created. The cost is that the write holds the store's single writer across the decode (`json.Marshal` still runs before the transaction opens). This only guards the write path — a row can still become unreadable later, say after a schema downgrade, which the read path handles by quarantining it (see [Migrator](#migrator)).

Use `GetOrCreate` when a controller has to make sure a child exists **without ever changing it**. The alternative is open-coding `Get` → `Create` → `Get` again on conflict, where the fallback path tends to drift out of step with the primary one. Its found branch writes nothing, so a deletion-pending row comes back as it is, with `DeletionRequestedAt` set, rather than being resurrected by a spec update:

The example uses two surfaces, and they are not interchangeable. `GetOrCreate` is on
`Client` — here the child kind's client, built with `NewClient` and held by the
controller. `AddEvent` is on the `ControllerClient` that `Reconcile` receives,
for writes about the object being reconciled. `Client` has no `AddEvent` and
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
        // The name is still held by a tombstone; it is released only once GC clears
        // the row's finalizers. Wait and retry — a replacement cannot be created yet.
        return beehive.Result{RequeueAfter: 5 * time.Second}, nil
    }
    if created {
        // AddEvent is about obj (this controller's object), not the child.
        if err := cc.AddEvent(ctx, obj.ID, beehive.EventSpec{
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

The options apply **only when the call creates the row** (`WithOwner`, `WithFinalizers`, `WithOnCreate`). Options that don't apply are ignored, as everywhere else.

That has a sharp edge worth stating plainly: since the found branch ignores the options, **`created=false` does not mean "exists and matches your options."** A row created earlier without `WithOwner` comes back with no owner edge, and a caller that assumes otherwise ends up with a child the GC cascade will never collect when the parent goes. If you depend on the owner edge, check it — `GetOrCreate` then `GetOwner`, or `GetByName(ctx, name, LoadOwner())` — and fix the difference yourself. Beehive will not adopt the row for you: an object has at most one owner, so adding the edge to a row that already has a different one would give it two, and deciding which owner wins is your policy, not the library's.

`DeleteByName` is the other half of the pair: `GetOrCreate` creates if absent, `DeleteByName` deletes if present. Both are idempotent and both understand tombstones, so a controller that ensures a name-keyed child on one branch and removes it on another writes one call for each. It replaces the usual open-coding of `GetByName`, treating `ErrNotFound` as success, treating `DeletionRequestedAt` as a no-op, then deleting:

| Name held by           | `DeleteByName`                                               |
| ---------------------- | ----------------------------------------------------------- |
| nothing                | `nil` — already gone                                         |
| a live row             | soft-deletes it (sets `DeletionRequestedAt`), advances GC    |
| a deletion-pending row | no-op — no write at all — advances GC; `nil`                 |

It marks the object and hands it to the controller to clear its finalizers. The row is removed once they clear, and only then is the name free again. It is scoped to the kind, like `GetByName`: another kind's row holding the same name is simply not found, which is reported as success rather than as a wrong-kind error. `Delete` is the incarnation-keyed sibling, and reports `ErrNotFound` where this folds absence to `nil`.

Both idempotent outcomes — no such row, and a row already deletion-pending — are answered by a lock-free probe **without opening a write transaction**. That is the steady state of the call: a controller that removes a child re-runs it every reconcile, and exactly one of those calls ever deletes anything, so taking the store's single write lock to discover there is nothing to do was the whole cost.

Looking the name up is **atomic with the delete** — the name goes into the store's `WHERE` clause rather than being resolved first and deleted after, so no concurrent collection can retire the row and hand its name to a replacement in between. The probe above does not change that: it is advisory, and the fall-through still runs the atomic mark, whose `deletion_requested_at IS NULL` guard re-checks everything. A `nil` return means "no object of this kind holds this name", not "the row I resolved is gone". What it cannot promise, and no implementation could, is that the name is still free when the call returns: a concurrent `GetOrCreate` may take it the instant the delete commits. As always, the next reconcile works from current state.

→ [ADR: name-keyed writes](docs/adr/2026-07-27-name-keyed-writes.md), for the transaction boundaries.

#### Watching

`Watch` and `WatchList` return a **stream**: the current state, the position it was read at, the `Changes` channel, and an `Err()` saying why that channel closed. The shape matches the call's cardinality — `Watch` returns an `ObjectStream`, whose `Object` is `nil` when the id holds nothing yet, and `WatchList` returns an `ObjectListStream`. `WatchOwnedObjects(ownerID)` is `WatchList` narrowed to one owner's children — see [secondary lookups](#secondary-lookups-owner--dependencies--dependents--owned) below. It is the same shape `WatchEvents` returns, so a stream ends one way across the whole library.

```go
list, err := client.WatchList(ctx)
// list.Objects is current state; list.ResourceVersion is where list.Changes starts.

one, err := client.Watch(ctx, id)
// one.Object is current state, or nil; one.ResourceVersion is where one.Changes starts.

for change := range list.Changes {
    // ...
}
if err := list.Err(); err != nil { /* the stream ended; see below */ }
```

**Do not open a watch inside `Within`.** The read below happens on your goroutine, and the store runs on a single connection — so it waits for the connection your transaction is holding, and the transaction cannot commit until it returns. (This is the general rule for `Within`: pass the ctx you were given to every store call inside it. A watch is the one call that has no right ctx to pass, since its stream must outlive the transaction.)

**Subscribe, then act.** The snapshot is read *before either returns*, so a change you make after subscribing is always in the stream — delete an object on the next line and its `Deleted` will come. If that read fails you get the error rather than a stream, since a watch with no snapshot could not report that delete. The stream carries changes strictly above `ResourceVersion`: no overlap with the snapshot, no gap between them. That is also what makes "have I caught up?" a value rather than a guess — you hold the starting state before you read the first change.

Both **share one reader per kind.** However many watches a kind has, one tailer reads its write-log position, and only a position that moved costs anything more: the entries above the cursor, then one batched read of the objects they name. A commit wakes that tailer, and a floor tick (`WithWatchFloorInterval`, 30s by default) covers what a wake cannot — a failed read, a retention trim. That reader lives exactly as long as the kind has watches — the first one starts it, the last one to end takes it down — so cancelling a watch releases everything it held, on a `Beehive` you started or one you never did. Three things follow, and they are the level-triggered contract the rest of beehive keeps — you are told what *is*, never what happened:

- **Changes collapse together.** Several writes to one object produce one change carrying current state. An object created *and* updated before you read reports `Added`, since it was not in your snapshot — and an `Added` may repeat for an object your snapshot already held, so treat it as "here is this object" rather than "this object is new".
- **Order holds per object, not across objects.** Each object's latest state arrives once, newest wins. Nothing is dropped: a delete always arrives, even for an object you never saw created.
- **Latency is the commit**, not the interval, for writes made through this `Beehive`. A quiet kind reads one indexed number per floor tick. Writing to the event log does not move the position, so an object watch stays quiet through a controller that records events on every pass.

**`Deleted` means collected, not requested.** Deleting an object sets `DeletionRequestedAt` and leaves the row live and readable, so you get a `Modified` with that field set. `Deleted` follows only when the GC sweeper physically removes the row — after its finalizers clear, which is controller-defined and unbounded, and after nothing references it any more. So: key on `DeletionRequestedAt != nil` to stop using an object, and on `Deleted` to evict it from a cache. A `Deleted` arrives even when its row image will not decode — a peer wrote the row at a schema version this binary cannot read, say — with `Object` nil and `ID` set. Nothing later in the log mentions a deleted id, so a `Deleted` withheld here would leave the object in your cache for good.

A failed read is logged and retried rather than fatal, so the stream survives a transient store error. One failure is terminal: if retention trims log entries the tail had not read, every watch on that kind closes with `Err()` reporting `ErrWatchTooOld`, because it cannot continue truthfully. Subscribe again for a fresh snapshot. A `WithResumeFrom` position that retention has already passed arrives the same way, on the stream rather than as an error from the call — so `ErrWatchTooOld` has one place to be handled, not two. Stopping the beehive ends every stream the same way, with `ErrStopped`; unlike `ErrWatchTooOld` that one cannot be answered by subscribing again. So a `Changes` channel that closes with a **nil** `Err()` closed because your own context ended, and a supervisor can key on exactly that to decide whether to resubscribe. `Err()` is set before the close, so reading it the moment the channel closes is enough — and because it is not a value on the stream, a caller that forgets it drops an error rather than mistaking one for a change.

`WatchOption`s tune the rest:

```go
WithResumeFrom(rv int64)       // stream above rv instead of taking a snapshot; a trimmed rv fails the stream, not the call
WithLoads(loads ...LoadOption) // the same eager relations List takes, batched per delivery
```

A slow subscriber stalls its own stream and nothing else: no change is dropped, and no other subscriber waits on it.

Neither watch needs a registered controller — the tail reads the write log, not a reconciler — and both are kind-scoped: `Watch` on another kind's id streams nothing. The id need not exist yet; an absent object is a `nil` `Object`, and its creation arrives as `Added`.

(The event *log* below, `ListEvents`/`WatchEvents`, is a different thing: an `ObjectChange` says an object changed, an `Event` is a log entry.)

→ [ADR: every driver is a periodic scan of the store](docs/adr/2026-07-28-periodic-scan-drivers.md), for what a poll costs and the constraints any push path above it would have to satisfy.

#### Secondary lookups (owner / dependencies / dependents / owned)

An object's ref edges are fetched on request, two ways:

- **Eager** — pass `LoadOption`s to a read: `Get(ctx, id, LoadOwner())`, `List(ctx, LoadDependencies(), LoadDependents())`. The returned objects carry the data (read via the accessors). On `List` each relation is one batched query, not one per object.
- **Lazy** — call `GetOwner` / `ListDependencies` / `ListDependents` / `ListOwned` when you actually need the data. These run the edge query directly, with no validating read in front, so they do **not** check the kind: another kind's id returns that kind's edges, and a missing id returns nothing, neither as `ErrNotFound`. Use them for ids the client owns.

`ListOwned` (and the eager `LoadOwned()` / `Object.Owned()`) is the inverse of `GetOwner` over `owned_by`: it returns the objects a given owner owns, the same way `ListDependents` inverts `ListDependencies` over `depends_on`.

`ListOwnedObjects(ownerID)` is the typed version. `ListOwned` returns untyped `ObjectRef`s across every owned kind, leaving you to filter by `Kind` and `Get` each child through its own client. `ListOwnedObjects` returns decoded `*Object[Spec, Status]` children of **this client's kind** in a single query, because the kind filter and the row read fold into the edge join — no `Get` per child. Ordering (by id) and missing-owner behaviour match `ListOwned`. Deletion-pending children are included, so skip them yourself by checking `DeletionRequestedAt`. It takes the same `LoadOption`s as `List`, batched the same way; without them the children have nothing loaded and their accessors return `ErrNotLoaded`.

`WatchOwnedObjects(ownerID)` is that read as a watch: the same snapshot, then every change to one of that owner's children — a later child as `Added`, its collection as `Deleted`. It takes the same `WatchOption`s as `WatchList` and joins the same per-kind tailer, so scoping costs a watch nothing beyond one batched edge read per drained page. Ownership is resolved from *current* state rather than from the write log, which is why a child created after the snapshot arrives correctly: the create's log entry is appended before its `owned_by` edge, in the same transaction. Like `GetOwner` and `Object.Owner()`, it assumes the one owner `WithOwner` can express.

→ [ADR: owner-scoped watches](docs/adr/2026-08-06-owner-scoped-watches.md), for the ownership invariant the resolution rests on.

Eager and lazy run the same query — edges are always a separate indexed lookup, never joined into the `SELECT` that carries specs and statuses. Eager just attaches the result to the object and batches it across a `List`.

→ [ADR: secondary lookups](docs/adr/2026-07-27-secondary-lookups.md), for the loader sharing, the accessor naming rule, and the store's semi-join.

#### Reconcile control

`Requeue` queues an object for reconcile now, and is the only way to reconcile something without waiting for a tick. It is a **latency hint, not a synchronous run**: it returns once the object is queued, and a worker gets to it on its own schedule. Losing one is harmless whenever the store records that the object is owed a pass, because the owed pass finds it anyway. It is also how you drive reconciles yourself with every periodic driver switched off. Use it to re-examine an object promptly after state the controller reads has changed elsewhere.

By default `Requeue` **keeps the object's retry backoff**. A requeue is an ordinary nudge — a config change, a dependency update, a manual poke — and almost never proves the failure is over. The one thing that does prove it is a successful reconcile, which clears backoff already. So: **backoff is cleared by a successful reconcile or by an explicit `WithResetBackoff()`, never by a plain requeue.** Pass `beehive.WithResetBackoff()` only when you know the failure is resolved and the next retry should start from the base interval. (controller-runtime draws the same line between `Add`/`AddAfter` and `Forget`.)

`Requeue` checks the id against the client's kind first, returning `ErrNotFound` for a missing or foreign id, then requires a registered controller, returning `ErrNoController` for a client-only kind that has no reconcile loop. It is on `Client` only: a controller schedules itself with `Result.RequeueAfter` and reaches other objects through the store, never by poking another reconcile loop.

#### Scheduling

The scheduling API reports when an object is **next due to reconcile**, as a [`Schedule`](#schedule) whose `NextRequeueAt` is a pending backoff retry, a `RequeueAfter` delay, or a re-enqueue floor holding a wake — or now, if the object is already queued, or the zero time if nothing is scheduled.

`GetSchedule` is the point read: a non-blocking read of in-memory state, with no store lookup and no kind check, so it returns no error today (the error is reserved for symmetry with the rest of the surface). A missing id, another kind's id and a client-only kind all read as the zero `Schedule`, which looks the same as a real object with nothing scheduled.

`WatchSchedule` streams the same value as a **gauge**: the current one on subscribe, then a new `Schedule` whenever it changes — a backoff step, a `RequeueAfter`, a wake held by the re-enqueue floor, a pass or dependency wake, a dispatch, a `Requeue`. None of those fire `Watch`/`WatchList`, since rescheduling bumps no generation or resource version, and no other signal covers them all. So this is the way to watch reschedules — for example to drive a "next attempt" countdown that stays accurate while an object's spec and status sit still. It is pushed rather than polled, and emits only on change, which means it converges on the current value and may skip values in between. The channel closes when `ctx` is cancelled. Unlike `GetSchedule` it returns `ErrNoController` for a client-only kind, since a stream that can never emit should say so rather than hang, but the id need not exist: an unscheduled id streams the zero `Schedule` until something schedules it.

Both are on `Client` only, and both read **per-id timers only**. Neither predicts the next reconcile: the real one can come **earlier**, because the owed pass, the full pass and the dependency wake are not per-id timers, and **a zero `NextRequeueAt` means "nothing scheduled", not "will not reconcile"**. Treat it as observability, not a guarantee.

→ [ADR: the schedule watch](docs/adr/2026-07-27-schedule-watch.md), for why it is an in-memory gauge rather than an event-log surface.

#### Events

`ListEvents` returns an object's runs newest first (by `LastAt`). `WithEventCategory` narrows to one timeline, and the other `EventOption`s filter by type, reason or time, or cap how many come back. `GetLatestEvent` returns the current run in a category, with a `bool` that folds away the no-events-yet case like `GetOwner` does.

`WatchEvents` hands back an `EventStream`: `Runs` is the snapshot as of `ResourceVersion`, `Events` streams what the log grows by above it — oldest-first — and `Err()` says why the stream ended once `Events` is closed. It reads the log above a cursor, and an `AddEvent` commit wakes it, so a local write arrives at commit rather than on a tick; the floor (`WithWatchFloorInterval`) is what covers a write another process made. An **extend is not a new run**: the row comes back with a higher `ResourceVersion` and a bumped `Count`, which is what lets you update it in place. There are no tombstones, since a run can only appear or grow.

`Retention` on the stream is the bound the sweeper enforces, as configured by `WithEventRetention` — a readout of that option, not a per-stream fact, so a consumer holding runs in memory bounds its own list from the server's number instead of a copy of it. A prune is still not delivered: the stream is the snapshot and what grows above it. Mind what `PerTimeline` counts — it caps each `(object, category)` timeline, so it bounds one stream's total only when the watch is scoped to a category; an unset (or unenforced) bound reads zero.

`WithEventsResumeFrom(rv)` starts above a position instead of snapshotting, so a reconnecting reader pays for the gap rather than the whole log — checkpoint the `ResourceVersion` of what you were delivered. Two answers end a resume instead of serving it: `ErrWatchTooOld` when retention has taken runs below your position, and `ErrWatchTooNew` when the position is above everything that object's log has held. Both mean "subscribe again without the option". A stream also ends with `ErrNotFound` if the object is collected, because its log cascades away with it — an empty stream would otherwise read as "no events" about an object that no longer exists.

`WithEventLimit` bounds the snapshot only; a tail has no end to count back from. The other filters apply to both, so a run in a category you filtered out is dropped rather than delivered, and costs you nothing.

`WithWriteLogRetention` bounds the object write log, which is what the watches tail and what a resume reads. It looks like `WithEventRetention` and defaults the other way: an event is written when a controller chooses to write one, while a log entry lands on **every** object write — and a status write bumps `resource_version`, so the log grows at reconcile rate whether or not you opt in. Hence a 24h default rather than unbounded. The value is a resume window before it is a storage bound: it is how long a subscriber may be disconnected and still resume instead of resyncing. It also governs how long a collected object's final state lives on, since the delete entry carries the row image a `Deleted` change reports.

`WithEventRetention` bounds the log per `(object, category)`: a ring that keeps the newest N **runs** in each timeline — runs, not occurrences, since an extend grows a run in place — so a flapping timeline can't evict a quiet one on the same object. `maxAge` is the other bound, and it is a different kind of thing: a flat cutoff on a run's *end*, across every timeline, so a run that keeps being extended never ages out. Both are off by default, which leaves the log unbounded; the GC sweeper enforces whichever you set, on its own interval, so a burst can sit above the cap until the next sweep. Deleting an object deletes its events. `EventStream.Retention` reports whichever bounds you set, so a watching consumer does not have to mirror this configuration by hand.

→ [ADR: event retention](docs/adr/2026-08-06-event-retention-is-a-ring-per-timeline.md), for why the cap counts runs per timeline and why neither bound is on by default.

→ [ADR: the events API](docs/adr/2026-07-27-events-api.md), for the run-aggregation rule, why `Detail` stays off the generic boundary, and the watch-surface naming.

### ControllerClient

```go
type ControllerClient[Status any] interface {
    UpdateStatus(ctx context.Context, id ObjectID, observedGeneration int64, status Status) error
    SetObservedGeneration(ctx context.Context, id ObjectID, observedGeneration int64) error
    SetCondition(ctx context.Context, id ObjectID, condition Condition) error
    SetConditions(ctx context.Context, id ObjectID, conditions []Condition) error
    DeleteCondition(ctx context.Context, id ObjectID, conditionType string) error
    AddEvent(ctx context.Context, id ObjectID, event EventSpec) error
    DeleteFinalizer(ctx context.Context, id ObjectID, finalizer string) error
    AddDependency(ctx context.Context, fromID, toID ObjectID) error
    DeleteDependency(ctx context.Context, fromID, toID ObjectID) error
    HasIncomingEdges(ctx context.Context, id ObjectID) (bool, error)
    // Lazy secondary lookups, for reading an object's edges during reconcile.
    GetOwner(ctx context.Context, id ObjectID) (ObjectRef, bool, error)
    ListDependencies(ctx context.Context, id ObjectID) ([]ObjectRef, error)
    ListDependents(ctx context.Context, id ObjectID) ([]ObjectRef, error)
    ListOwned(ctx context.Context, id ObjectID) ([]ObjectRef, error)
    Within(ctx context.Context, fn func(ctx context.Context) error) error
}
```

`UpdateStatus` **does nothing when the status marshals to the bytes already stored**. There is no `resource_version` bump, so a watch and the dependency waker both find nothing — the same way re-applying an unchanged spec does nothing on the `Client` side. So report observed state unconditionally; you don't need your own equality check, and a dependent riding on this kind's status won't be woken by a pass that found nothing new.

The generation handshake is the exception. `observedGeneration` and `ObservedAt` are recorded even when the content is unchanged, so a reconcile that legitimately changed no status still settles the object instead of being re-queued by every owed-pass tick. That write does bump `resource_version`, so a watcher waiting for `ObservedGeneration == Generation` sees the object converge. It happens at most once per generation: the next unchanged pass finds the generation already recorded and writes nothing.

**If your pass reports only conditions — or nothing at all — call `SetObservedGeneration(ctx, id, obj.Generation)`.** `SetCondition` bumps `resource_version` but deliberately leaves the handshake alone, so a controller whose real output is conditions would otherwise never settle and would sit in the owed listing forever, re-queued every interval. This verb records the handshake and writes no status; compose it inside `Within` to land with a `SetConditions`. Unlike `UpdateStatus` it always clamps — a generation at or below the recorded one writes nothing, so it is idempotent per generation and can never roll a converged object back to unsettled. Neither verb accepts a generation below 1 (`ErrInvalidObservedGeneration`); no object holds one.

Do **not** settle by re-passing the status you were handed to `UpdateStatus`. It usually works and is unsound — the no-op gate is the schema version as well as the bytes; the ADR below has the detail.

`ObservedAt` therefore records **when the object settled at `ObservedGeneration`**, not when the controller last ran — don't use it as a liveness check, since a reconcile that never calls `UpdateStatus` never moves it either. For "when did we last look", record an event instead: `AddEvent` extends the current run and bumps its `LastAt` every time, which is that signal, retained and aggregated.

→ [ADR: the generation handshake and content no-ops](docs/adr/2026-07-27-generation-handshake-and-noop-writes.md), for how the no-op splits the two halves of the write and why it is gated on the schema version.

`SetConditions` writes several conditions of one object as **one** write: they land in a single transaction under a single `resource_version` bump, so a watcher never sees a fresh `Connected` beside a stale `Healthy`, and a dependent is woken once for the pass rather than once per condition. Suppression stays per condition — the ones matching what is stored are not rewritten, so their `UpdatedAt` holds — and a batch where every condition matches writes nothing at all, exactly like a single `SetCondition` no-op. Naming a type twice in one call is refused with `ErrDuplicateConditionType` rather than resolved by slice order, and nothing in that batch is written. An empty slice writes nothing.

`SetCondition` is the one-condition spelling of the same write. Reach for `SetConditions` when one pass observes several conditions; both compose inside `Within`, which is what to use when conditions must land with an `UpdateStatus` or a `DeleteCondition` — `Within` gives you the atomicity, and `SetConditions` additionally collapses what would be one version bump and one log entry per condition into one of each.

`GetOwner`, `ListDependencies`, `ListDependents` and `ListOwned` are the same lazy lookups the `Client` has. `Reconcile` is handed its object directly, with no read call of its own, so these are how it reads related edges. `GetOwner` returns the owner over `owned_by` and `ListOwned` the reverse, the owner's children; `ListDependents` is the reverse of `ListDependencies` over `depends_on`.

`HasIncomingEdges` is a different question, used by GC: does anything with a live claim still point at `id`? That means an owned child, or a dependent that is not itself being deleted — one that is going away has no claim. You cannot rebuild it from `ListDependents`, because it folds in owned children as well. A finalizer can wait on it: a controller holding a shared connection clears its finalizer only once nothing with a live claim references the object, so the connection outlives its last real user.

`AddEvent` adds an observation to the object's event log — see [Event](#event). Adding is not always an insert: repeating the latest run's `(Category, Type, Reason)` extends that run instead of appending a second one, which is what lets a controller report every poll without growing the log per poll. Like `SetCondition` it is scoped to the controller's kind (`ErrWrongKind` for another kind's id) and composes inside `Within`, so a controller can record an event and flip a condition together.

### Controller

```go
type Controller[Spec, Status any] interface {
    Reconcile(ctx context.Context, client ControllerClient[Status], obj *Object[Spec, Status]) (Result, error)
}
```

A controller has **no lifecycle** in beehive. It implements `Reconcile` and nothing else, and receives the kind's `ControllerClient` as a parameter. Background work — timers, subscriptions, engines — belongs to your application, which already has its own lifecycle and can get a `ControllerClient` from `Register`. Beehive owns only the reconcile lifecycle: the work queue, backoff, the periodic drivers and shutdown ordering.

`Reconcile` is **not** wrapped in a transaction. Each `ControllerClient` write commits on its own, so a write that lands before `Reconcile` returns an error stays committed. The next pass works from the stored state, so write `Reconcile` to be idempotent. Each write is still atomic on its own, and the generation handshake covers a concurrent spec change racing the `obj` you were handed: `UpdateStatus` rejects a generation from the future, and an older one leaves the object unsettled so it reconciles again.

When several writes must land together or not at all, wrap them in `ControllerClient.Within(ctx, func(ctx) error { … })`. Writes made with the inner `ctx` join one transaction, which commits when the function returns `nil` and rolls back on error — `Client` writes included. That transaction holds the store's single write lock for as long as the function runs, so keep external I/O out of it. Nothing waits on it, because nothing is scheduled: a rolled-back transaction leaves no rows, so no driver can list them. That makes it safe to create or delete children inside `Within`. The one thing deferred past the commit is `WithOnCreate`, which is skipped on rollback. → [ADR](docs/adr/2026-07-27-name-keyed-writes.md)

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

- `List` and the watches skip the bad row, log it and carry on. A watch remembers its version, so it warns once per change rather than once per read.
- `Get`/`GetByName` return the error.
- **The reconcile loop quarantines the row.** It cannot reconcile what it cannot decode, and the bytes will not change until someone rewrites the spec, so it logs and treats the pass as a successful no-op rather than retrying the same bytes forever under backoff. A deletion-pending row is still collected, since GC needs only the id. The owed pass re-queues the unsettled row every tick, so the warning repeats at that interval — deliberately, so a bad row stays visible instead of logging once and going quiet.

A kind with no migrator is untouched; its columns stay `0`. Only registered kinds can have a migrator, so client-only kinds cannot.

→ [ADR: schema-version migration](docs/adr/2026-07-27-schema-version-migration.md), for convert-on-read / stamp-on-write and why stamping is never downward.

### Options

```go
type Option interface{ apply(any) }

func WithFinalizers(f ...string) Option            // declare finalizers before the object is visible to controllers; registered kinds only
func WithOwner(id ObjectID) Option                 // declare owned_by edge; owner cannot be deleted while this object exists
func WithOnCreate(fn func(ctx context.Context)) Option // run fn after the create commits (Create always; GetOrCreate only when it inserts)
func WithFullPassInterval(d time.Duration) Option  // how often to re-dispatch EVERY object (default: 0, off)
func WithOwedPassInterval(d time.Duration) Option  // how often to drain work the store records as owed (default: 30s; must be > 0)
func WithStaleDependentsInterval(d time.Duration) Option // how often to re-derive which dependents a target moved past (default: 60s; New only; must be > 0)
func WithWatchFloorInterval(d time.Duration) Option // how often a watch reads without a commit wake (default: 30s; New only; must be > 0)
func WithGCInterval(d time.Duration) Option        // how often to collect dead rows + prune the event log + release free pages (default: 30s; New only; must be > 0)
func WithStartupFullPass(enabled bool) Option      // also re-dispatch settled objects once at startup (default: false, off)
func WithMaxRetryInterval(d time.Duration) Option  // cap on exponential backoff after Reconcile errors (default: 30s)
func WithMigrator(m Migrator) Option               // attach a schema-version Migrator for the kind (Register only)
func WithEventRetention(perTimeline int, maxAge time.Duration) Option // event-log retention: per-(object,category) cap-N ring of runs + optional age bound (0 = unbounded)
func WithWriteLogRetention(perKind int, maxAge time.Duration) Option // write-log retention: per-(group,kind) cap-N ring + age bound (default: 24h, no count bound)
```

`WithOwner` writes an `owned_by` edge in the same transaction as the `Create`. Deleting the owner then cascades to the child through GC.

`WithFinalizers` is the one create option that needs a kind **this process has registered a controller for**; otherwise the call fails with `ErrInvalidOption`. Only `ControllerClient.DeleteFinalizer` can clear a finalizer, and it folds the calling controller's own kind into the write — so a client-only kind's finalizer is removable by nothing, and the row would stay deletion-pending forever while its `owned_by` edge blocks its owner's delete.

The check is **process-local and evaluated at call time**, since the store records no registrations: it refuses a create issued before this process's own `Register`. Register the kind first. It runs before any store work and only when the option is used, so an ordinary create on a client-only kind is unaffected — and like every other create-option check it is eager, so `GetOrCreate` rejects it on the found branch too rather than only when a row is really inserted.

`WithOnCreate` is the safe way to run a side effect only if the row is really created — an external call, an in-memory counter. It waits for the *outermost* commit, so it runs once and never after a rollback; it is the only thing in beehive deferred that way. `Create` always fires it, `GetOrCreate` only when it inserts. Prefer it to branching on `GetOrCreate`'s `created` bool, which is returned synchronously: inside an enclosing `ControllerClient.Within` that bool is set before the transaction commits, so acting on it fires your side effect for a row a rollback may still discard.

`AddDependency` and `DeleteDependency` manage `depends_on` edges during reconcile. When a target changes, the next dependency-wake scan queues the dependent. Each commits on its own, or joins a `Within` the controller opened.

The target can be **any** kind, including one you only ever use through `Client` and never register — configuration, secrets, any reference data your app writes and your controllers read. The waker scans the whole store's write log rather than only the kinds with controllers, so such a target wakes its dependents like any other.

**Dropping** an edge is what releases a target you were holding open: a deletion-pending object cannot be collected while a live dependent points at it, and `DeleteDependency` collects it as soon as the edge goes, rather than at the next sweep.

Every call that **creates** the edge records, durably and atomically with the edge itself, that the dependent owes a reconcile (a count on the row, `reconcile_owed`, drained by the owed pass). That one rule covers every way a declare could otherwise miss: a change to the target landing between your read and the edge's commit, a declare made on another object's behalf while that object's own reconcile is mid-flight, and a crash before the wake is serviced. Re-asserting your edges on every pass costs nothing after the first, because only the call that created the edge records anything — the cost is one reconcile per edge ever created.

There is nothing else to pass: the call takes no version claim, because nothing conditions on one. An earlier design stamped the wake only when the target had moved past the version the caller read, which made the claim load-bearing and left one interleaving stranded; with the stamp unconditional, a claim would be dead weight in every caller's hands, so it was removed rather than kept as decoration.

**A dependency wake is a guarantee, not a best effort.** The scan above is fast and lives in memory, so a crash or a restart can drop a wake — and a dependent that has already settled is invisible to every listing of owed work, because its own generation never moved. So beehive records, on each successful reconcile of an object that has dependencies, the store-wide write cursor that pass observed; a slower pass (60s) then enqueues every dependent whose targets have moved past it. Nothing about that is bookkeeping you can lose: it compares current state, so it recovers a wake lost by any means. A failed reconcile records nothing and is therefore found again. What you get, for a write made through this `Beehive`, is a wake as soon as the target's write commits, and within a minute even if that wake is lost. A write beehive never saw commit — from another process, or issued straight to the `Store` behind its back — is [outside the supported scope](#scope-one-process-one-beehive-no-out-of-band-access) and carries no wake guarantee at all.

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

The event read methods take `EventOption`s (also a separate type from `Option`, applying only to `ListEvents`/`WatchEvents`) — see [Events](#events). `WithEventsResumeFrom` is the one that means nothing outside `WatchEvents`, and the other reads ignore it:

```go
func WithEventCategory(cat string) EventOption  // restrict to a single timeline
func WithEventType(t EventType) EventOption      // only Normal or only Warning
func WithEventReason(reason string) EventOption  // only runs with this reason
func WithEventLimit(n int) EventOption           // cap the number of runs returned / snapshotted
func WithEventsSince(t time.Time) EventOption    // only runs active at or after t
func WithEventsResumeFrom(rv int64) EventOption  // WatchEvents: stream above rv instead of snapshotting
```
