// Copyright 2026 Andres Morey
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package storeapi defines the storage contract shared between the beehive
// control plane and its store implementations (e.g. beehive/sqlite). It lives
// in internal/ so external consumers cannot depend on it directly; they use the
// type aliases re-exported from the top-level beehive package.
package storeapi

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

// GroupKind identifies a kind of resource. An empty Group denotes the core group.
type GroupKind struct {
	Group string
	Kind  string
}

// ObjectID is the store-assigned unique identifier for an object.
type ObjectID = int64

// EventID is the store-assigned unique identifier for an event run.
type EventID = int64

// ErrNotFound is returned by Store reads when no object matches.
var ErrNotFound = errors.New("beehive: object not found")

// ErrWrongKind is returned by an id-keyed mutator when the target id names an
// object of a different kind than the gk passed to the call. The store folds the
// caller's kind into each write so a foreign id is rejected at the source rather
// than corrupting another kind's row; surfaces translate this as they see fit
// (the user-facing client hides it as ErrNotFound, a controller surfaces it).
var ErrWrongKind = errors.New("beehive: object belongs to a different kind")

// ErrObservedGenerationFuture is returned by UpdateStatus when the caller passes
// an observedGeneration greater than the object's current generation. It signals
// a broken convergence handshake: a controller can only report a generation it
// actually observed in Reconcile, so a future value would falsely mark the object
// settled once its spec later reached that generation.
var ErrObservedGenerationFuture = errors.New("beehive: observed generation exceeds current generation")

// ErrSchemaVersionDowngrade is returned by ObjectsUpdateSpec/UpdateStatus when the
// caller's schema version is non-zero and lower than the one already stamped on
// the row. Zero is the "no opinion" case (the kind is unversioned, or this build
// has no migrator for it) and keeps the stored tag instead of erroring. It is the
// write-side twin of the read path's downgrade refusal: an older versioned build
// must not relabel newer bytes as older, because every later read would then
// convert already-converted data instead of refusing to decode it.
var ErrSchemaVersionDowngrade = errors.New("beehive: stored schema version is newer than this build's")

// ErrTargetResourceVersionFuture is returned by EdgesAdd when the caller's
// claimed version of the target exceeds the target's current one. An object's
// version only moves forward, so a version above the target's own cannot have come
// from reading it — the caller passed some other object's version, or some other
// field. Checked before the insert, so a rejected call writes nothing: the sibling
// of ErrObservedGenerationFuture, and the same argument — a caller can only report
// what it could have observed.
var ErrTargetResourceVersionFuture = errors.New("beehive: target resource version is ahead of the target")

// ChangeType classifies a Change.
type ChangeType string

const (
	Added    ChangeType = "Added"
	Modified ChangeType = "Modified"
	Deleted  ChangeType = "Deleted"
)

// RawObjectChange is the untyped change an ObjectsSubscription delivers. The client decodes it
// into the generic, user-facing ObjectChange[Spec, Status]; the name carries the
// "Raw" prefix (like RawObject) to avoid colliding with that generic type.
type RawObjectChange struct {
	Type   ChangeType
	Object *RawObject
}

// Subscription is a closeable stream of V, the shape every store watch returns.
// Changes yields items until the subscription is closed or its store shuts down,
// at which point the channel closes; Close releases the subscription and is safe
// to call more than once.
//
// It is concrete rather than one interface per stream: the three it replaced
// differed only in the name of their single accessor, which forced the sqlite
// implementation to hang three method names on one channel to satisfy them all.
// A backend builds one with NewSubscription.
type Subscription[V any] struct {
	ch    <-chan V
	close func()
	once  sync.Once
}

// NewSubscription wraps a stream's channel and its release function. close is
// called at most once however many times Close is.
func NewSubscription[V any](ch <-chan V, close func()) *Subscription[V] {
	return &Subscription[V]{ch: ch, close: close}
}

func (s *Subscription[V]) Changes() <-chan V { return s.ch }
func (s *Subscription[V]) Close()            { s.once.Do(s.close) }

// ObjectsSubscription is a subscription to a kind's change stream: the current
// state as Added changes (the snapshot) followed by live changes.
type ObjectsSubscription = Subscription[RawObjectChange]

// EventsSubscription is a subscription to one object's event log: the current
// runs (the snapshot) followed by live runs, each an aggregated Event. Unlike
// ObjectsSubscription there are no tombstones — a run only appears or updates —
// so a lagging subscriber converges to each run's latest count/window.
type EventsSubscription = Subscription[Event]

// ObjectWritesSubscription is a subscription to the store-wide write stream. It
// yields the writes that were ready together, coalesced per object — a burst
// arrives as one slice with one entry per distinct object, so a consumer that
// resolves each entry against the store pays per burst rather than per write.
type ObjectWritesSubscription = Subscription[[]ObjectWrite]

// ObjectWrite is a change stripped to what a consumer that only routes by
// identity needs: which object changed, and how. The id is the object's, not a
// change's — changes are not addressable here — so this is an object reference
// annotated with what happened to it, and a consumer reads current state itself.
// It carries no *RawObject on purpose: the store-wide stream sees every write in
// the process, and holding a row would pin its spec and status blobs for as long
// as the value is undelivered.
type ObjectWrite struct {
	ID   ObjectID
	Type ChangeType

	// ResourceVersion is the version of the write this reference reports — the
	// newest one, where conflation merged several. It is the store-wide cursor (see
	// resource_version_seq), so a consumer that records the highest version it has
	// finished processing can resume from there instead of re-deriving the world.
	// Eight bytes and no row: the blob-pinning above stays avoided.
	ResourceVersion int64
}

// Condition is the untyped form of a single condition row. Status is one of
// "True"/"False"/"Unknown"; Liveness marks a condition derived from a live
// in-process resource (valid only within the writing process — see the read
// path's "verifying" downgrade). The client decodes these into the public,
// generic-free beehive.Condition; the store-only bookkeeping fields
// (TransitionedAt, UpdatedAt) stop at that boundary.
type Condition struct {
	Type           string
	Status         string
	Reason         string
	Message        string
	Liveness       bool
	TransitionedAt time.Time
	UpdatedAt      time.Time
}

// Event is the untyped form of a single event-log row: one contiguous run of
// observations about an object, aggregated by (Category, Type, Reason). Detail is
// opaque JSON the store never inspects. The client decodes these into the public,
// generic-free beehive.Event; the store-only ResourceVersion cursor stops at that
// boundary.
type Event struct {
	ID              EventID
	ObjectID        ObjectID
	Category        string
	Type            string
	Reason          string
	Message         string
	Detail          []byte // opaque JSON payload; nil when none
	Count           int
	FirstAt         time.Time
	LastAt          time.Time
	ResourceVersion int64
}

// EventQuery filters and bounds a EventsList read. The zero value selects every
// run for the object, newest first (by LastAt, then id). The client builds one
// from its EventOptions; the store never sees the option type.
type EventQuery struct {
	// Category, when non-nil, restricts to that exact timeline (including "", the
	// default). Nil returns every category interleaved.
	Category *string
	Type     string    // "" = any type
	Reason   string    // "" = any reason
	Since    time.Time // zero = no lower bound; else runs with LastAt >= Since
	Limit    int       // 0 = no limit; else the newest N runs
}

// RawObject is the untyped row below the generic boundary. Spec and Status are
// opaque JSON bytes; everything else is Beehive-owned metadata that mirrors the
// objects table. The reconciler and client decode Spec/Status into typed
// Object[Spec, Status] values; the Store never inspects them.
type RawObject struct {
	ID     ObjectID
	Group  string
	Kind   string
	Slug   *string
	Spec   []byte // JSON, user-owned
	Status []byte // JSON, controller-owned; nil until first status write
	// SpecVersion and StatusVersion are the per-column schema versions: the
	// migrator schema version each blob was last written at. The store persists
	// and returns them but never interprets them (like ResourceVersion); the
	// generic layer's Migrator converts a blob from its stored version on read.
	SpecVersion         int
	StatusVersion       int
	Generation          int64
	ObservedGeneration  *int64
	ObservedAt          *time.Time
	ResourceVersion     int64
	DeletionRequestedAt *time.Time
	// ReconcileOwed is how many passes beehive owes this object (see the
	// objects.reconcile_owed column). 0 means nothing owed. The reconciler reads it
	// to decide whether to decrement on a successful pass. Store-owned and
	// store-assigned, like ResourceVersion: it is reported on reads and moved only by
	// ReconcileOwedIncrement/ReconcileOwedDecrement, so a value set on a RawObject
	// handed to ObjectsCreate is not persisted (a new object owes nothing).
	ReconcileOwed int64
	Finalizers    []string
	Conditions    []Condition // assembled on reads; nil when the object has none
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Relation is the kind of edge in the edges table. The schema's CHECK constraint
// permits exactly these two values.
type Relation string

const (
	// RelationOwnedBy: deleting the target cascade-deletes the dependent.
	RelationOwnedBy Relation = "owned_by"
	// RelationDependsOn: a change to the target requeues the dependent.
	RelationDependsOn Relation = "depends_on"
)

// EdgesAddResult is what a caller needs to follow up on an edge it declared, all
// of it a by-product of work EdgesAdd already does — no extra query pays for any
// of it.
type EdgesAddResult struct {
	// From is the source object's GroupKind, projected from the endpoint check.
	// Edges are cross-kind, so a caller routing a requeue to fromID cannot assume
	// its own kind.
	From GroupKind
	// ReconcileOwedStamped reports whether this call incremented fromID's reconcile_owed:
	// the edge was new *and* the target had moved past the claimed version. A
	// caller pairing the durable stamp with an in-memory requeue gates the requeue
	// on this rather than recomputing the conjunction, so the two halves cannot
	// drift — and there is deliberately no second, independently derived
	// edge-new report to drift against.
	ReconcileOwedStamped bool
}

// ObjectRef names one object — its id plus the GroupKind needed to route a
// requeue or a GC step to it. It is a reference to an object, not an edge: it
// carries no direction, so the same shape serves both ends of an edge query
// (EdgesListIncoming's sources, EdgesListOutgoing's targets) and the edgeless
// DeletionRequests lists, where it is simply "which object, and what kind".
type ObjectRef struct {
	ID    ObjectID
	Group string
	Kind  string
}

// GroupKind is the kind to route a requeue (or a GC step) to.
func (r ObjectRef) GroupKind() GroupKind {
	return GroupKind{Group: r.Group, Kind: r.Kind}
}

// Store is the durable-store contract Beehive depends on internally. It is
// non-generic and deals only in raw rows: the generic-to-non-generic boundary
// lives one layer up, in the typedController adapter.
//
// Mutators return the freshly written row so callers see the store-assigned
// id, resource_version, and timestamps without a re-read. A nil error therefore
// guarantees a non-nil object, and callers dereference it unguarded — including
// on the idempotent no-op paths, where the row is unchanged but still returned
// (DeletionRequestsCreate and DeletionRequestsCreateBySlug with changed=false). An
// implementation that returns (nil, nil) is broken, not a case to handle.
type Store interface {
	io.Closer

	// Within runs fn inside a single transaction, committing on a nil error and
	// rolling back otherwise. Store calls made with the ctx passed to fn join
	// that transaction; calls made with any other context run standalone.
	Within(ctx context.Context, fn func(ctx context.Context) error) error

	// AfterCommit registers fn to run once the transaction ctx belongs to has
	// committed, after that transaction's buffered watch events are published.
	// If ctx carries no transaction the write has already committed, so fn runs
	// before AfterCommit returns. Hooks run in registration order, and a
	// rolled-back transaction never runs them.
	//
	// Callers use it for side effects that must not be observable before the
	// write is durable and its events are out — waking a reconciler, most of all.
	//
	// fn receives a context detached from the transaction, so a store call it
	// makes opens a fresh transaction instead of joining the committed one.
	// Everything else — deadline, cancellation, values — is inherited, so a hook
	// is still bound to the caller's lifetime.
	//
	// Registering from inside a running hook is allowed and runs the new hook
	// immediately: its transaction has already committed, so "after the commit"
	// is now. This holds even when the hook passes back the transaction context
	// it captured rather than the detached one it was handed.
	//
	// fn must not panic: hooks run in sequence, so a panic aborts the ones
	// registered after it while the transaction stays committed. Nothing
	// recovers here — a panicking hook is a bug in the layer that registered it,
	// and this library lets panics surface (Reconcile is not recovered either).
	AfterCommit(ctx context.Context, fn func(ctx context.Context))

	// ConditionsDelete removes the condition of type condType from id. Removing an
	// existing condition bumps ResourceVersion and emits a Modified event; an
	// absent condition is a no-op. Returns the object with its conditions assembled.
	// Scoped to gk: an id of another kind is rejected with ErrWrongKind, a missing
	// id with ErrNotFound.
	ConditionsDelete(ctx context.Context, gk GroupKind, id ObjectID, condType string) (*RawObject, error)

	// ConditionsSet upserts the condition keyed by (id, cond.Type). A real change
	// bumps the object's ResourceVersion and emits a Modified event; an identical
	// write is a no-op. Returns the object with its conditions assembled. Scoped
	// to gk: an id of another kind is rejected with ErrWrongKind, a missing id
	// with ErrNotFound.
	ConditionsSet(ctx context.Context, gk GroupKind, id ObjectID, cond Condition) (*RawObject, error)

	// DeletionRequestsCreate requests an object's deletion by setting
	// DeletionRequestedAt; the row lingers until its finalizers clear, so this
	// creates the request and never the deletion itself (ObjectsDelete does that).
	// changed is true only when this call was the one that set the flag;
	// repeat calls are idempotent and return changed=false. Scoped to gk: an id
	// of another kind is rejected with ErrWrongKind, a missing id with ErrNotFound.
	DeletionRequestsCreate(ctx context.Context, gk GroupKind, id ObjectID) (obj *RawObject, changed bool, err error)

	// DeletionRequestsCreateBySlug is DeletionRequestsCreate keyed by slug within
	// gk: the slug is folded into the write itself, so the resolve and the stamp
	// are one atomic statement rather than a lookup wrapped in a transaction with
	// the write. Both are race-free; this one costs a round trip less, which a
	// single-connection store feels. Semantics otherwise match
	// DeletionRequestsCreate: changed is true only when this call set the flag, a
	// repeat is idempotent with changed=false, and ErrNotFound means no object of
	// gk holds the slug (there is no ErrWrongKind — a foreign kind's slug is
	// simply not found, since slugs are unique per kind rather than globally).
	DeletionRequestsCreateBySlug(ctx context.Context, gk GroupKind, slug string) (obj *RawObject, changed bool, err error)

	// DeletionRequestsCreateFromOwner is the GC cascade as one command: it requests
	// deletion of every object that owned_by ownerID and returns them all to
	// requeue. It stamps (and emits a Modified for) only children not already
	// deletion-pending, so a re-cascade over an already-deleting subtree is a
	// single read — no per-child write every sweep.
	DeletionRequestsCreateFromOwner(ctx context.Context, ownerID ObjectID) ([]ObjectRef, error)

	// DeletionRequestsList returns every deletion-pending object, of every kind,
	// each row's GroupKind alongside its id. The global GC sweeper is the sole
	// caller and needs the kind to route: an object of a registered kind is
	// enqueued so its controller can clear finalizers (a step collect cannot take),
	// while a client-only kind — which no reconcile loop reaches, and which could
	// otherwise strand and RESTRICT-block an owner's delete forever — is collected
	// directly.
	DeletionRequestsList(ctx context.Context) ([]ObjectRef, error)

	// EventsGetLatest returns the most recent run in id's category timeline, or nil
	// if that timeline has no events. Reads by object id only (not kind-scoped).
	EventsGetLatest(ctx context.Context, id ObjectID, category string) (*Event, error)

	// EventsList returns id's event runs matching q, newest first (by LastAt, then
	// id). The zero EventQuery returns every run for the object. Reads by object id
	// only — not kind-scoped, like the ref-list reads.
	EventsList(ctx context.Context, id ObjectID, q EventQuery) ([]Event, error)

	// EventsRecord records an observation about id in the (id, ev.Category) timeline,
	// aggregating into contiguous runs: if the latest run there shares ev's
	// (Type, Reason) it is extended (Count++, LastAt bumped, Message/Detail
	// re-sampled), else a new run is appended (Count 1). Only ev's
	// Category/Type/Reason/Message/Detail are read; the store assigns the rest and
	// returns the run. The compare is scoped to (id, Category), so an interleaved
	// other-category emission never breaks this run. Scoped to gk: foreign id
	// ErrWrongKind, missing id ErrNotFound.
	EventsRecord(ctx context.Context, gk GroupKind, id ObjectID, ev Event) (*Event, error)

	// EventsSweep trims the event log by retention, returning the number of runs
	// deleted. perObject > 0 caps each (object, category) timeline to its newest
	// perObject runs (a ring, so a flapping timeline can't evict a quiet one);
	// maxAge > 0 drops any run whose LastAt is older than maxAge. A zero bound is
	// skipped. It sweeps every object of every kind — retention is global, not
	// per-kind — so the global GC sweeper calls it once per pass.
	EventsSweep(ctx context.Context, perObject int, maxAge time.Duration) (int, error)

	// FinalizersDelete removes finalizer from id's finalizer list. Removing a
	// present finalizer bumps ResourceVersion and emits a Modified event; a
	// finalizer that isn't on the object is a no-op (no bump, no event). Returns
	// the object with its conditions assembled, or ErrNotFound if id is gone.
	// Scoped to gk: an id of another kind is rejected with ErrWrongKind.
	FinalizersDelete(ctx context.Context, gk GroupKind, id ObjectID, finalizer string) (*RawObject, error)

	// ObjectsCreate inserts a new object. The store assigns ID and
	// ResourceVersion and sets Generation to 1; the caller supplies the rest
	// (Group, Kind, Slug, Spec, Finalizers).
	ObjectsCreate(ctx context.Context, obj *RawObject) (*RawObject, error)

	// ObjectsDelete removes the row outright. Callers must ensure finalizers are
	// empty first; this is the physical delete the GC path performs.
	ObjectsDelete(ctx context.Context, id ObjectID) error

	// ObjectsGet loads an object by id, or returns ErrNotFound.
	ObjectsGet(ctx context.Context, id ObjectID) (*RawObject, error)

	// ObjectsGetBySlug loads the object with the given slug within gk, or returns
	// ErrNotFound.
	ObjectsGetBySlug(ctx context.Context, gk GroupKind, slug string) (*RawObject, error)

	// ObjectsGetMeta is ObjectsGet without the conditions query: the returned
	// Conditions is always nil. Metadata-only callers (GC collect, ref bookkeeping)
	// use it to avoid that extra read. Returns ErrNotFound if no object matches.
	ObjectsGetMeta(ctx context.Context, id ObjectID) (*RawObject, error)

	// ObjectsList returns every object of kind gk, ordered by id.
	ObjectsList(ctx context.Context, gk GroupKind) ([]*RawObject, error)

	// ObjectsListByIncomingEdge is the blob-bearing, kind-scoped form of
	// EdgesListIncoming: the full rows of the objects of kind gk pointing at toID
	// through relation, ordered by id, conditions attached. It resolves the edges
	// and the rows in one query so a typed read of an owner's children of one kind
	// (Client.OwnedObjectsList) costs no Get per child. Objects of other kinds are
	// filtered out; a toID with no matching edge reads empty, never ErrNotFound.
	ObjectsListByIncomingEdge(ctx context.Context, gk GroupKind, toID ObjectID, relation Relation) ([]*RawObject, error)

	// ObjectsListIDs returns the IDs of every object of kind gk, ordered by id. The
	// reconciler uses it to enqueue a full reconcile pass at startup, so
	// process-scoped state (e.g. liveness conditions) is re-confirmed even on
	// objects whose spec is already settled.
	ObjectsListIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error)

	// ObjectsListUnsettledIDs returns the IDs of objects of kind gk whose
	// observed_generation doesn't match generation (not yet converged).
	ObjectsListUnsettledIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error)

	// ObjectsUpdateSpec replaces an object's spec, bumping Generation (a real spec
	// change) and ResourceVersion, and stamps specVersion (the migrator schema
	// version the bytes were written at). Writing spec bytes identical to the
	// stored ones *at the row's own schema version* is an idempotent no-op: no
	// Generation/ResourceVersion bump and no event, so a converged object isn't
	// falsely unsettled. The version qualifier is load-bearing: bytes written at a
	// different schema version are in a different shape, so comparing them says
	// nothing about whether the value changed, and such a write takes the normal
	// path (stamped, bumped, emitted). changed reports
	// which happened — true for a real write, false for the no-op — so callers can
	// keep their own follow-up (a reconciler wake) in step with the store's
	// silence. Scoped to gk: an id of another kind is rejected with ErrWrongKind,
	// a missing id with ErrNotFound.
	ObjectsUpdateSpec(ctx context.Context, gk GroupKind, id ObjectID, spec []byte, specVersion int) (obj *RawObject, changed bool, err error)

	// ObjectsUpdateStatus replaces an object's status, records the generation the
	// controller observed, and stamps statusVersion (the migrator schema version
	// the status bytes were written at). When the bytes differ from the stored
	// ones it bumps ObservedAt, ResourceVersion and UpdatedAt and emits Modified.
	//
	// Status bytes identical to the stored ones *at the row's own schema version*
	// write no status and never touch UpdatedAt. The version qualifier is
	// load-bearing: bytes written at a different schema version are in a different
	// shape, so comparing them says nothing about whether the value changed, and
	// such a write takes the normal path above. Two things still ride the no-op
	// path:
	//
	//   - ObservedGeneration/ObservedAt advance if this reconcile settled a
	//     generation the object hadn't settled at before, so neither the
	//     convergence handshake nor the unsettled resync keying off it is
	//     stranded by a reconcile that changed no status content. That advance
	//     is a real transition, so it does bump ResourceVersion and emit
	//     Modified — a watcher gating on ObservedGeneration == Generation sees
	//     the object converge. It fires at most once per generation.
	//   - Nothing else. A call identical in both respects writes nothing at
	//     all: no ResourceVersion bump, no Modified event. So ObservedAt holds
	//     when ObservedGeneration was recorded and does not tick per reconcile —
	//     it is a handshake timestamp, not a liveness heartbeat.
	//
	// The two paths treat a stale observedGeneration — one at or below the recorded
	// value — differently, and the split is deliberate. On the no-op path it is
	// ignored: with identical bytes it relays strictly less than what is already
	// stored, so recording it would only un-converge a settled object and emit a
	// Modified for nothing. On the content-changed path it is written verbatim,
	// rolling ObservedGeneration back so the object reads as unsettled — the
	// reporter just overwrote the status with content derived from an older spec,
	// and being unsettled is what makes the resync backstop re-derive it. Pinning
	// stale status as converged would leave nothing to revisit it.
	//
	// Scoped to gk: an id of another kind is rejected with ErrWrongKind, a missing
	// id with ErrNotFound. An observedGeneration greater than the row's current
	// generation is rejected with ErrObservedGenerationFuture, no-op or not.
	ObjectsUpdateStatus(ctx context.Context, gk GroupKind, id ObjectID, observedGeneration int64, status []byte, statusVersion int) (*RawObject, error)

	// EdgesAdd inserts a directed (fromID -> toID) edge with the given relation.
	// Idempotent; both endpoints must exist, else ErrNotFound. The edge isn't on
	// the object, so it bumps no version and emits no event.
	//
	// targetResourceVersion is the caller's claimed version of toID — the version
	// its decision to depend on toID was based on — or 0 for no claim. It drives
	// two things, and every implementation owes both:
	//
	// A claim above toID's current version is rejected with
	// ErrTargetResourceVersionFuture *before* the insert, so nothing is written.
	// The rejection cannot depend on a caller unwinding a transaction it may be
	// sharing with writes it means to keep.
	//
	// A claim that toID has already moved past, on an edge this call creates,
	// increments fromID's reconcile_owed — the durable record that a dependency
	// wake is owed — and reports it as EdgesAddResult.ReconcileOwedStamped. That
	// write must land on the same side of the insert as the rejection, and for the
	// same reason: were it a second call after EdgesAdd returned, a caller sharing
	// an ambient transaction could handle the error and commit the edge with no
	// wake, which is precisely the stranded-dependent race the claim exists to
	// close. The endpoint check, the stamp and the insert are therefore one
	// atomic unit, and an implementation must not leave them separable.
	//
	// What the count holds is durable, owed *work*; the in-memory dependency waker
	// is a separate mechanism and leaves nothing here. A second durable marker
	// (undecodable rows, say) gets its own column and its own cadence — it does not
	// join this count.
	//
	// The stamp is unconditional on fromID's kind: the store cannot know which
	// kinds have reconcile loops, and gating would cost the caller a pre-read of
	// fromID's kind on every declare. A kind with no loop never drains its count,
	// and nothing scans it either (ReconcileOwedListIDs is per-kind), so the count is
	// unread — but it is a lasting row and index entry, and re-declaring an edge
	// bumps it again. Reclaiming it wants a cross-kind sweeper (see TODO.md), not a
	// gate here: declining to stamp would lose the wake outright for a kind that
	// gains a controller later.
	EdgesAdd(ctx context.Context, fromID, toID ObjectID, relation Relation, targetResourceVersion int64) (EdgesAddResult, error)

	// EdgesDelete removes the (fromID, toID, relation) edge; an absent edge is a
	// no-op. Like EdgesAdd it bumps no version and emits no event.
	EdgesDelete(ctx context.Context, fromID, toID ObjectID, relation Relation) error

	// EdgesDeleteFinalizingDependsOn removes the depends_on edges pointing at toID
	// whose source object is itself marked for deletion. A finalizing dependent is
	// going away, so its dependency must not keep the target alive: without this,
	// two deletion-pending objects that depend on each other (or a self-dependency)
	// would each hold the other's RESTRICT and never be collected. owned_by edges
	// are left untouched — those clear only when the owned child is physically
	// removed (the foreground cascade).
	EdgesDeleteFinalizingDependsOn(ctx context.Context, toID ObjectID) error

	// EdgesGroupIncomingByID is the batched form of EdgesListIncoming: the inbound
	// referrers for many targets through relation, bucketed by target id. The
	// incoming-edge twin of EdgesGroupOutgoingByID (e.g. eager-loading dependents
	// over a List without an N+1). A target with no referrer is absent.
	EdgesGroupIncomingByID(ctx context.Context, toIDs []ObjectID, relation Relation) (map[ObjectID][]ObjectRef, error)

	// EdgesGroupOutgoingByID is the batched form of EdgesListOutgoingByRelation: it
	// resolves the relation's outgoing targets for many sources, bucketed by source
	// id. A source with no matching edge is absent from the map (never a nil/empty
	// entry), so eager loading over a List avoids an N+1. The implementation may
	// chunk a large id list across several queries; the bucketed result is the same.
	EdgesGroupOutgoingByID(ctx context.Context, fromIDs []ObjectID, relation Relation) (map[ObjectID][]ObjectRef, error)

	// EdgesHasIncoming reports whether any object with a live claim points at id: an
	// owned_by edge, or a depends_on edge from a source that is not itself
	// finalizing. A depends_on edge from a deletion-pending source is ignored —
	// that dependent is going away and no longer has a claim, so it must not gate a
	// finalizer (two mutually dependent finalizing objects would otherwise never
	// see EdgesHasIncoming clear). owned_by always counts: the foreground cascade must
	// wait for the owned child to be physically removed. GC pairs this with
	// EdgesDeleteFinalizingDependsOn, which physically removes the ignored edges
	// before ObjectsDelete so the edges RESTRICT is satisfied.
	EdgesHasIncoming(ctx context.Context, id ObjectID) (bool, error)

	// EdgesListIncoming returns every object pointing at toID through relation, ordered by
	// id (e.g. the dependents to requeue, or the owned children to GC).
	EdgesListIncoming(ctx context.Context, toID ObjectID, relation Relation) ([]ObjectRef, error)

	// EdgesListOutgoing returns the distinct objects that fromID points at through any
	// relation, ordered by id (the inverse of EdgesListIncoming). GC uses it to wake
	// the targets a row was holding open before removing it: deleting fromID drops
	// its outgoing edges (ON DELETE CASCADE), which can unblock a deletion-pending
	// target that RESTRICT was keeping alive.
	EdgesListOutgoing(ctx context.Context, fromID ObjectID) ([]ObjectRef, error)

	// EdgesListOutgoingByRelation is the relation-filtered form of EdgesListOutgoing:
	// the objects fromID points at through exactly relation, ordered by id (e.g. a
	// child's owner via RelationOwnedBy, or its dependencies via RelationDependsOn).
	EdgesListOutgoingByRelation(ctx context.Context, fromID ObjectID, relation Relation) ([]ObjectRef, error)

	// There is deliberately no standalone increment here. Increments are produced by
	// EdgesAdd — its stamp has to be indivisible from the edge insert, so it issues
	// one itself and reports it as EdgesAddResult.ReconcileOwedStamped — and consumed by
	// ReconcileOwedDecrement. An interface increment would be surface no caller could
	// use correctly for the declare path (it cannot be made atomic with the edge)
	// and none uses at all otherwise; leaving it off makes "the stamp rides EdgesAdd"
	// a compile-time property rather than something a test has to police. Add it
	// when a producer other than EdgesAdd exists — the durable-wake half of the
	// dependency-waker item in TODO.md would be one.

	// ReconcileOwedDecrement subtracts observed from id's reconcile_owed (floored at 0),
	// recording that a reconcile serviced every wake it saw. Callers pass the count
	// they loaded, not 1: a single pass reads the target's current state, which
	// addresses all the wakes outstanding when it started, so subtracting 1 would
	// strand the rest as a residual nothing re-enqueues. Increments landing *after*
	// that load are above observed, so they survive the subtraction and stay owed.
	// Bumps no resource_version and emits no event.
	ReconcileOwedDecrement(ctx context.Context, id ObjectID, observed int64) error

	// ReconcileOwedListIDs returns the IDs of objects of kind gk owed a durable
	// dependency wake (reconcile_owed != 0). The reconcile backstop enqueues these so
	// a wake that outlived the process (its in-memory requeue lost to a crash) is
	// serviced on restart, without a spec change to wake it. Orthogonal to
	// ObjectsListUnsettledIDs: an object can be spec-converged yet still owe a wake.
	ReconcileOwedListIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error)

	// ObjectsWatch returns a subscription to the single object id of kind gk: its current
	// state (if any) as an Added snapshot, then live changes filtered to that id.
	ObjectsWatch(ctx context.Context, gk GroupKind, id ObjectID) (*ObjectsSubscription, error)

	// EventsWatch subscribes to id's event log within gk: the runs matching q as a
	// snapshot (oldest-first), then live runs. q filters both the snapshot and the
	// live stream (Limit bounds only the snapshot). Runs conflate per run id, so a
	// lagging subscriber converges to each run's latest state.
	EventsWatch(ctx context.Context, gk GroupKind, id ObjectID, q EventQuery) (*EventsSubscription, error)

	// ObjectsWatchList returns a subscription to every object of kind gk: the current set as
	// an Added snapshot, then all live changes for the kind.
	ObjectsWatchList(ctx context.Context, gk GroupKind) (*ObjectsSubscription, error)

	// ObjectWritesSubscribe returns a subscription to live writes to every kind in
	// the store — no initial snapshot, no rows, no kind filter. Batches of
	// identity, for a consumer that routes by id and reads current state itself.
	//
	// The int64 is the cursor the stream starts from: every write committed before
	// the call is at or below it, and every write the stream delivers is above it.
	// Returned here rather than as a separate read so the two cannot be ordered
	// wrongly — a cursor read before the subscription exists would leave a window
	// whose writes reach neither.
	ObjectWritesSubscribe(ctx context.Context) (*ObjectWritesSubscription, int64, error)
}
