// Package storeapi defines the storage contract shared between the beehive
// control plane and its store implementations (e.g. beehive/sqlite). It lives
// in internal/ so external consumers cannot depend on it directly; they use the
// type aliases re-exported from the top-level beehive package.
package storeapi

import (
	"context"
	"errors"
	"io"
	"time"
)

// GroupKind identifies a kind of resource. An empty Group denotes the core group.
type GroupKind struct {
	Group string
	Kind  string
}

// ObjectID is the store-assigned unique identifier for an object.
type ObjectID = int64

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

// WatchEventType classifies a watch event.
type WatchEventType string

const (
	WatchEventAdded    WatchEventType = "Added"
	WatchEventModified WatchEventType = "Modified"
	WatchEventDeleted  WatchEventType = "Deleted"
)

// RawWatchEvent is the untyped event a Watcher delivers. The client decodes it
// into the generic, user-facing WatchEvent[Spec, Status]; the name carries the
// "Raw" prefix (like RawObject) to avoid colliding with that generic type.
type RawWatchEvent struct {
	Type   WatchEventType
	Object *RawObject
}

// Watcher is a subscription to a kind's change stream. Events yields the current
// state as Added events (the snapshot) followed by live changes, until the
// watcher is closed or its store shuts down, at which point the channel closes.
// Close releases the subscription and is safe to call more than once.
type Watcher interface {
	Events() <-chan RawWatchEvent
	Close()
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

// RawObject is the untyped row below the generic boundary. Spec and Status are
// opaque JSON bytes; everything else is Beehive-owned metadata that mirrors the
// objects table. The reconciler and client decode Spec/Status into typed
// Object[Spec, Status] values; the Store never inspects them.
type RawObject struct {
	ID                  ObjectID
	Group               string
	Kind                string
	Name                *string
	Spec                []byte // JSON, user-owned
	Status              []byte // JSON, controller-owned; nil until first status write
	Generation          int64
	ObservedGeneration  *int64
	ObservedAt          *time.Time
	ResourceVersion     int64
	DeletionRequestedAt *time.Time
	Finalizers          []string
	Conditions          []Condition // assembled on reads; nil when the object has none
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// Relation is the kind of edge in the refs table. The schema's CHECK constraint
// permits exactly these two values.
type Relation string

const (
	// RelationOwnedBy: deleting the target cascade-deletes the dependent.
	RelationOwnedBy Relation = "owned_by"
	// RelationDependsOn: a change to the target requeues the dependent.
	RelationDependsOn Relation = "depends_on"
)

// Referrer is an object pointing at a target through a ref edge, with the
// GroupKind needed to route a requeue. ListIncomingRefs returns these.
type Referrer struct {
	ID    ObjectID
	Group string
	Kind  string
}

// Store is the durable-store contract Beehive depends on internally. It is
// non-generic and deals only in raw rows: the generic-to-non-generic boundary
// lives one layer up, in the typedController adapter.
//
// Mutators return the freshly written row so callers see the store-assigned
// id, resource_version, and timestamps without a re-read.
type Store interface {
	io.Closer

	// Within runs fn inside a single transaction, committing on a nil error and
	// rolling back otherwise. Store calls made with the ctx passed to fn join
	// that transaction; calls made with any other context run standalone.
	Within(ctx context.Context, fn func(ctx context.Context) error) error

	// CreateObject inserts a new object. The store assigns ID and
	// ResourceVersion and sets Generation to 1; the caller supplies the rest
	// (Group, Kind, Name, Spec, Finalizers).
	CreateObject(ctx context.Context, obj *RawObject) (*RawObject, error)

	// GetObject loads an object by id, or returns ErrNotFound.
	GetObject(ctx context.Context, id ObjectID) (*RawObject, error)

	// GetObjectMeta is GetObject without the conditions query: the returned
	// Conditions is always nil. Metadata-only callers (GC collect, ref bookkeeping)
	// use it to avoid that extra read. Returns ErrNotFound if no object matches.
	GetObjectMeta(ctx context.Context, id ObjectID) (*RawObject, error)

	// GetObjectByName loads the object with the given name within gk, or returns
	// ErrNotFound.
	GetObjectByName(ctx context.Context, gk GroupKind, name string) (*RawObject, error)

	// ListObjects returns every object of kind gk, ordered by id.
	ListObjects(ctx context.Context, gk GroupKind) ([]*RawObject, error)

	// ListUnsettledIDs returns the IDs of objects of kind gk whose
	// observed_generation doesn't match generation (not yet converged).
	ListUnsettledIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error)

	// ListDeletionPendingIDs returns the IDs of objects of kind gk that have been
	// marked for deletion (DeletionRequestedAt set) but not yet physically
	// removed. The GC backstop enqueues these so a delete makes progress without a
	// spec change to wake it.
	ListDeletionPendingIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error)

	// ListAllDeletionPendingIDs is ListDeletionPendingIDs across every kind. The
	// global GC sweeper uses it to collect deletion-pending objects of kinds with
	// no registered controller (client-only kinds), which the per-controller
	// backstop never reaches and which could otherwise strand and RESTRICT-block
	// an owner's delete forever.
	ListAllDeletionPendingIDs(ctx context.Context) ([]ObjectID, error)

	// ListIDs returns the IDs of every object of kind gk, ordered by id. The
	// reconciler uses it to enqueue a full reconcile pass at startup, so
	// process-scoped state (e.g. liveness conditions) is re-confirmed even on
	// objects whose spec is already settled.
	ListIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error)

	// UpdateSpec replaces an object's spec, bumping Generation (a real spec
	// change) and ResourceVersion. Writing spec bytes identical to the stored
	// ones is an idempotent no-op: no Generation/ResourceVersion bump and no
	// event, so a converged object isn't falsely unsettled. Scoped to gk: an id
	// of another kind is rejected with ErrWrongKind, a missing id with ErrNotFound.
	UpdateSpec(ctx context.Context, gk GroupKind, id ObjectID, spec []byte) (*RawObject, error)

	// UpdateStatus replaces an object's status and records the generation the
	// controller observed, bumping ObservedAt and ResourceVersion. Scoped to gk:
	// an id of another kind is rejected with ErrWrongKind, a missing id with
	// ErrNotFound.
	UpdateStatus(ctx context.Context, gk GroupKind, id ObjectID, observedGeneration int64, status []byte) (*RawObject, error)

	// SetCondition upserts the condition keyed by (id, cond.Type). A real change
	// bumps the object's ResourceVersion and emits a Modified event; an identical
	// write is a no-op. Returns the object with its conditions assembled. Scoped
	// to gk: an id of another kind is rejected with ErrWrongKind, a missing id
	// with ErrNotFound.
	SetCondition(ctx context.Context, gk GroupKind, id ObjectID, cond Condition) (*RawObject, error)

	// DeleteCondition removes the condition of type condType from id. Removing an
	// existing condition bumps ResourceVersion and emits a Modified event; an
	// absent condition is a no-op. Returns the object with its conditions assembled.
	// Scoped to gk: an id of another kind is rejected with ErrWrongKind, a missing
	// id with ErrNotFound.
	DeleteCondition(ctx context.Context, gk GroupKind, id ObjectID, condType string) (*RawObject, error)

	// DeleteFinalizer removes finalizer from id's finalizer list. Removing a
	// present finalizer bumps ResourceVersion and emits a Modified event; a
	// finalizer that isn't on the object is a no-op (no bump, no event). Returns
	// the object with its conditions assembled, or ErrNotFound if id is gone.
	// Scoped to gk: an id of another kind is rejected with ErrWrongKind.
	DeleteFinalizer(ctx context.Context, gk GroupKind, id ObjectID, finalizer string) (*RawObject, error)

	// RequestDeletion marks an object for deletion by setting
	// DeletionRequestedAt; the row lingers until its finalizers clear.
	// changed is true only when this call was the one that set the flag;
	// repeat calls are idempotent and return changed=false. Scoped to gk: an id
	// of another kind is rejected with ErrWrongKind, a missing id with ErrNotFound.
	RequestDeletion(ctx context.Context, gk GroupKind, id ObjectID) (obj *RawObject, changed bool, err error)

	// DeleteObject removes the row outright. Callers must ensure finalizers are
	// empty first; this is the physical delete the GC path performs.
	DeleteObject(ctx context.Context, id ObjectID) error

	// MarkOwnedForDeletion is the GC cascade as one command: it marks every object
	// that owned_by ownerID for deletion and returns them all to requeue. It stamps
	// (and emits a Modified for) only children not already deletion-pending, so a
	// re-cascade over an already-deleting subtree is a single read — no per-child
	// write every sweep.
	MarkOwnedForDeletion(ctx context.Context, ownerID ObjectID) ([]Referrer, error)

	// AddRef inserts a directed (fromID -> toID) edge with the given relation.
	// Idempotent; both endpoints must exist, else ErrNotFound. The edge isn't on
	// the object, so it bumps no version and emits no event.
	AddRef(ctx context.Context, fromID, toID ObjectID, relation Relation) error

	// DeleteRef removes the (fromID, toID, relation) edge; an absent edge is a
	// no-op. Like AddRef it bumps no version and emits no event.
	DeleteRef(ctx context.Context, fromID, toID ObjectID, relation Relation) error

	// ListIncomingRefs returns every object pointing at toID through relation, ordered by
	// id (e.g. the dependents to requeue, or the owned children to GC).
	ListIncomingRefs(ctx context.Context, toID ObjectID, relation Relation) ([]Referrer, error)

	// ListOutgoingRefs returns the distinct objects that fromID points at through any
	// relation, ordered by id (the inverse of ListIncomingRefs). GC uses it to wake
	// the targets a row was holding open before removing it: deleting fromID drops
	// its outgoing edges (ON DELETE CASCADE), which can unblock a deletion-pending
	// target that RESTRICT was keeping alive.
	ListOutgoingRefs(ctx context.Context, fromID ObjectID) ([]Referrer, error)

	// DeleteFinalizingDependsOnRefs removes the depends_on edges pointing at toID
	// whose source object is itself marked for deletion. A finalizing dependent is
	// going away, so its dependency must not keep the target alive: without this,
	// two deletion-pending objects that depend on each other (or a self-dependency)
	// would each hold the other's RESTRICT and never be collected. owned_by edges
	// are left untouched — those clear only when the owned child is physically
	// removed (the foreground cascade).
	DeleteFinalizingDependsOnRefs(ctx context.Context, toID ObjectID) error

	// HasReferrers reports whether any object with a live claim points at id: an
	// owned_by edge, or a depends_on edge from a source that is not itself
	// finalizing. A depends_on edge from a deletion-pending source is ignored —
	// that dependent is going away and no longer has a claim, so it must not gate a
	// finalizer (two mutually dependent finalizing objects would otherwise never
	// see HasReferrers clear). owned_by always counts: the foreground cascade must
	// wait for the owned child to be physically removed. GC pairs this with
	// DeleteFinalizingDependsOnRefs, which physically removes the ignored edges
	// before DeleteObject so the refs RESTRICT is satisfied.
	HasReferrers(ctx context.Context, id ObjectID) (bool, error)

	// Watch returns a Watcher for the single object id of kind gk: its current
	// state (if any) as an Added snapshot, then live changes filtered to that id.
	Watch(ctx context.Context, gk GroupKind, id ObjectID) (Watcher, error)

	// WatchList returns a Watcher for every object of kind gk: the current set as
	// an Added snapshot, then all live changes for the kind.
	WatchList(ctx context.Context, gk GroupKind) (Watcher, error)

	// WatchEvents returns a Watcher for live changes to gk only — no initial
	// snapshot. Use it when current state is already accounted for elsewhere and
	// only subsequent changes matter (e.g. the dependency waker), to skip the
	// snapshot build that WatchList would do on every subscribe.
	WatchEvents(ctx context.Context, gk GroupKind) (Watcher, error)
}
