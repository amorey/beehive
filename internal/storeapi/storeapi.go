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
// GroupKind needed to route a requeue. ListReferrers returns these.
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

	// GetObjectByName loads the object with the given name within gk, or returns
	// ErrNotFound.
	GetObjectByName(ctx context.Context, gk GroupKind, name string) (*RawObject, error)

	// ListObjects returns every object of kind gk, ordered by id.
	ListObjects(ctx context.Context, gk GroupKind) ([]*RawObject, error)

	// ListUnsettledIDs returns the IDs of objects of kind gk whose
	// observed_generation doesn't match generation (not yet converged).
	ListUnsettledIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error)

	// ListIDs returns the IDs of every object of kind gk, ordered by id. The
	// reconciler uses it to enqueue a full reconcile pass at startup, so
	// process-scoped state (e.g. liveness conditions) is re-confirmed even on
	// objects whose spec is already settled.
	ListIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error)

	// UpdateSpec replaces an object's spec, bumping Generation (a real spec
	// change) and ResourceVersion.
	UpdateSpec(ctx context.Context, id ObjectID, spec []byte) (*RawObject, error)

	// UpdateStatus replaces an object's status and records the generation the
	// controller observed, bumping ObservedAt and ResourceVersion.
	UpdateStatus(ctx context.Context, id ObjectID, observedGeneration int64, status []byte) (*RawObject, error)

	// SetCondition upserts the condition keyed by (id, cond.Type). A real change
	// bumps the object's ResourceVersion and emits a Modified event; an identical
	// write is a no-op. Returns the object with its conditions assembled.
	SetCondition(ctx context.Context, id ObjectID, cond Condition) (*RawObject, error)

	// DeleteCondition removes the condition of type condType from id. Removing an
	// existing condition bumps ResourceVersion and emits a Modified event; an
	// absent condition is a no-op. Returns the object with its conditions assembled.
	DeleteCondition(ctx context.Context, id ObjectID, condType string) (*RawObject, error)

	// RequestDeletion marks an object for deletion by setting
	// DeletionRequestedAt; the row lingers until its finalizers clear.
	// changed is true only when this call was the one that set the flag;
	// repeat calls are idempotent and return changed=false.
	RequestDeletion(ctx context.Context, id ObjectID) (obj *RawObject, changed bool, err error)

	// DeleteObject removes the row outright. Callers must ensure finalizers are
	// empty first; this is the physical delete the GC path performs.
	DeleteObject(ctx context.Context, id ObjectID) error

	// AddRef inserts a directed (fromID -> toID) edge with the given relation.
	// Idempotent; both endpoints must exist, else ErrNotFound. The edge isn't on
	// the object, so it bumps no version and emits no event.
	AddRef(ctx context.Context, fromID, toID ObjectID, relation Relation) error

	// DeleteRef removes the (fromID, toID, relation) edge; an absent edge is a
	// no-op. Like AddRef it bumps no version and emits no event.
	DeleteRef(ctx context.Context, fromID, toID ObjectID, relation Relation) error

	// ListReferrers returns every object pointing at toID through relation, ordered by
	// id (e.g. the dependents to requeue, or the owned children to GC).
	ListReferrers(ctx context.Context, toID ObjectID, relation Relation) ([]Referrer, error)

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
