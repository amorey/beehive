package beehive

import (
	"time"

	"github.com/amorey/beehive/internal/storeapi"
)

// GroupKind identifies a kind of resource. An empty Group denotes the core group.
type GroupKind = storeapi.GroupKind

// ObjectID is the store-assigned unique identifier for an object.
type ObjectID = storeapi.ObjectID

// Object is a single resource: user-owned desired state (Spec) plus
// controller-owned observed state (Status), along with the metadata Beehive
// uses to track convergence and deletion.
type Object[Spec, Status any] struct {
	ID                  ObjectID
	Group               string
	Kind                string
	Slug                *string
	Spec                Spec
	Status              *Status
	Generation          int64      // bumped on every Spec change
	ObservedGeneration  *int64     // Generation the controller last reconciled; nil until first reconcile
	ObservedAt          *time.Time // time of the last successful reconcile
	ResourceVersion     int64      // bumped on every write, for optimistic concurrency
	DeletionRequestedAt *time.Time // set when deletion is requested; object lingers until finalizers clear
	Finalizers          []string
	Conditions          []Condition // per-type observations reported by controllers
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// Result is returned by a controller's Reconcile to influence requeueing.
type Result struct {
	// RequeueAfter requeues the object after the given delay. Zero means no
	// explicit requeue (the object still resyncs on the periodic timer).
	RequeueAfter time.Duration
}

// ConditionStatus is the state of a Condition: True, False, or Unknown.
type ConditionStatus string

const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)

// Condition is a standard observation about an object's state, reported by its
// controller (e.g. type "Ready", status True).
type Condition struct {
	Type    string
	Status  ConditionStatus
	Reason  string
	Message string
	// Liveness marks a condition derived from a live in-process resource: it is
	// valid only within the writing process. The store downgrades a liveness
	// condition written by a prior process to Unknown ("verifying") until a
	// controller re-confirms it. The default (false) is durable store-truth.
	Liveness bool
}
