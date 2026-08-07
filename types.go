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

package beehive

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/amorey/beehive/internal/storeapi"
)

// GroupKind identifies a kind of resource. An empty Group denotes the core group.
type GroupKind = storeapi.GroupKind

// ObjectID is the store-assigned unique identifier for an object.
type ObjectID = storeapi.ObjectID

// StalePos is a position in the stale-dependents scan. See storeapi.StalePos.
type StalePos = storeapi.StalePos

// ObjectRef identifies a related object — an owner, a dependency, or a
// dependent — carrying the GroupKind needed to address it.
type ObjectRef = storeapi.ObjectRef

// Store is the durable-store contract Beehive depends on. It is non-generic and
// deals only in raw rows; the generic boundary lives in the typedController
// adapter. See storeapi.Store for the full contract.
type Store = storeapi.Store

// RawObject is the untyped row below the generic boundary: opaque Spec/Status
// JSON plus Beehive-owned metadata.
type RawObject = storeapi.RawObject

// ObjectsCreateInput is the write shape Objects().Create accepts — only the fields a
// create honours.
type ObjectsCreateInput = storeapi.ObjectsCreateInput

// RawEvent is the untyped event-log row below the generic boundary.
type RawEvent = storeapi.Event

// EventsAddInput is the write shape Events().Add accepts — only the fields a
// recorded observation carries.
type EventsAddInput = storeapi.EventsAddInput

// DeletionCascadeChild is one owned child of a deletion cascade, as
// DeletionRequests().CreateFromOwner reports it.
type DeletionCascadeChild = storeapi.DeletionCascadeChild

// Relation is the kind of edge in the edges table.
type Relation = storeapi.Relation

const (
	RelationOwnedBy   = storeapi.RelationOwnedBy
	RelationDependsOn = storeapi.RelationDependsOn
)

// ObjectWrite is one entry of the object write log.
type ObjectWrite = storeapi.ObjectWrite

// WriteOp is what an ObjectWrite recorded.
type WriteOp = storeapi.WriteOp

// The soft delete is a WriteUpdate: the row is still live and readable, so only
// collection is WriteDelete.
const (
	WriteCreate = storeapi.WriteCreate
	WriteUpdate = storeapi.WriteUpdate
	WriteDelete = storeapi.WriteDelete
)

// ChangeType classifies a Change.
type ChangeType = storeapi.ChangeType

const (
	Added    = storeapi.Added
	Modified = storeapi.Modified
	Deleted  = storeapi.Deleted
	// Failed is terminal: the stream is over and the change carries the reason.
	Failed = storeapi.Failed
)

// ErrNotFound is returned by Store reads when no object matches.
var ErrNotFound = storeapi.ErrNotFound

// ErrInvalidName is returned by name-keyed calls when the name is empty.
var ErrInvalidName = storeapi.ErrInvalidName

// ErrNameTaken is returned by Create when the name is already held, by a live
// row or a deletion-pending one. GetOrCreate returns the existing row instead.
var ErrNameTaken = storeapi.ErrNameTaken

// ErrDuplicateConditionType is returned by SetConditions when one call names a
// condition type twice, whose outcome would otherwise depend on apply order.
var ErrDuplicateConditionType = storeapi.ErrDuplicateConditionType

// ErrStaleTxContext is returned by a nested Within whose ctx is not the
// transaction's live innermost frame. Deep nesting on one goroutine is fine;
// using a ctx from outside the frame you are in is not.
var ErrStaleTxContext = storeapi.ErrStaleTxContext

// ErrConcurrentNestedTx is returned by the outermost Within when a nested frame
// is still open at commit, which can only mean another goroutine holds one.
var ErrConcurrentNestedTx = storeapi.ErrConcurrentNestedTx

// ErrObservedGenerationFuture is returned by a handshake write when the caller
// reports a generation greater than the object's current one.
var ErrObservedGenerationFuture = storeapi.ErrObservedGenerationFuture

// ErrInvalidObservedGeneration is returned by a handshake write given a
// generation below 1, which no object ever holds.
var ErrInvalidObservedGeneration = storeapi.ErrInvalidObservedGeneration

// ErrSchemaVersionDowngrade is returned by Objects().UpdateSpec/UpdateStatus when the
// caller's schema version is lower than the one stamped on the row.
var ErrSchemaVersionDowngrade = storeapi.ErrSchemaVersionDowngrade

// LoadSet is a bitset of secondary lookups (owner, dependencies, dependents,
// owned, events) to fetch alongside an object. The zero value loads nothing.
type LoadSet uint8

const (
	// LoadOwnerBit selects the object's owner (its outgoing owned_by edge).
	LoadOwnerBit LoadSet = 1 << iota
	// LoadDependenciesBit selects the object's dependencies (outgoing depends_on).
	LoadDependenciesBit
	// LoadDependentsBit selects the objects that depend on it (incoming depends_on).
	LoadDependentsBit
	// LoadOwnedBit selects the objects this one owns (incoming owned_by edges).
	LoadOwnedBit
	// LoadEventsBit selects the object's event-log runs.
	LoadEventsBit
)

// Object is a single resource: user-owned desired state (Spec) plus
// controller-owned observed state (Status), along with the metadata Beehive
// uses to track convergence and deletion.
type Object[Spec, Status any] struct {
	ID                  ObjectID
	Group               string
	Kind                string
	Name                string
	Spec                Spec
	Status              *Status
	Generation          int64      // bumped on every Spec write that isn't a no-op
	ObservedGeneration  *int64     // Generation the controller last reconciled; nil until first reconcile
	ObservedAt          *time.Time // when ObservedGeneration was recorded; not a reconcile heartbeat
	ResourceVersion     int64      // bumped on every write
	DeletionRequestedAt *time.Time // set when deletion is requested; object lingers until finalizers clear
	Finalizers          []string
	Conditions          []Condition // per-type observations reported by controllers
	CreatedAt           time.Time
	UpdatedAt           time.Time

	// Related data, populated only for the lookups a read requested. loaded
	// records what was fetched, so the accessors can tell "loaded and empty"
	// from "never asked".
	owner        *ObjectRef
	dependencies []ObjectRef
	dependents   []ObjectRef
	owned        []ObjectRef
	events       []Event
	loaded       LoadSet
}

// ErrNotLoaded is returned by the secondary-lookup accessors when the requested
// relation was not fetched on the read that produced the object.
var ErrNotLoaded = errors.New("beehive: secondary lookup not loaded")

// Owner returns the object's owner. ok reports presence; ErrNotLoaded if
// LoadOwner() was not passed to the read.
func (o *Object[Spec, Status]) Owner() (ObjectRef, bool, error) {
	if o.loaded&LoadOwnerBit == 0 {
		return ObjectRef{}, false, fmt.Errorf("%w: owner (pass LoadOwner())", ErrNotLoaded)
	}
	if o.owner == nil {
		return ObjectRef{}, false, nil
	}
	return *o.owner, true, nil
}

// Dependencies returns the objects this one depends on, or ErrNotLoaded if
// LoadDependencies() was not passed to the read.
func (o *Object[Spec, Status]) Dependencies() ([]ObjectRef, error) {
	if o.loaded&LoadDependenciesBit == 0 {
		return nil, fmt.Errorf("%w: dependencies (pass LoadDependencies())", ErrNotLoaded)
	}
	return o.dependencies, nil
}

// Dependents returns the objects that depend on this one, or ErrNotLoaded if
// LoadDependents() was not passed to the read.
func (o *Object[Spec, Status]) Dependents() ([]ObjectRef, error) {
	if o.loaded&LoadDependentsBit == 0 {
		return nil, fmt.Errorf("%w: dependents (pass LoadDependents())", ErrNotLoaded)
	}
	return o.dependents, nil
}

// Owned returns the objects this one owns, or ErrNotLoaded if LoadOwned() was
// not passed to the read.
func (o *Object[Spec, Status]) Owned() ([]ObjectRef, error) {
	if o.loaded&LoadOwnedBit == 0 {
		return nil, fmt.Errorf("%w: owned (pass LoadOwned())", ErrNotLoaded)
	}
	return o.owned, nil
}

// Events returns the object's event-log runs, newest-first, or ErrNotLoaded if
// LoadEvents() was not passed to the read.
func (o *Object[Spec, Status]) Events() ([]Event, error) {
	if o.loaded&LoadEventsBit == 0 {
		return nil, fmt.Errorf("%w: events (pass LoadEvents())", ErrNotLoaded)
	}
	return o.events, nil
}

// Result is returned by a controller's Reconcile to influence requeueing.
type Result struct {
	// RequeueAfter requeues the object after the given delay. Zero means no
	// explicit requeue (the object is still picked up by the periodic passes).
	RequeueAfter time.Duration
}

// Schedule reports when an object is next due to reconcile. A struct so fields
// can be added without a breaking change.
type Schedule struct {
	// NextRequeueAt is when the reconcile loop has scheduled the object to be
	// requeued, or the zero time when nothing is scheduled. It reflects only
	// per-id timers (backoff, RequeueAfter, an immediate enqueue), not the
	// periodic drivers.
	NextRequeueAt time.Time
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
	// Liveness marks a condition valid only within the writing process. The
	// store downgrades a liveness condition written by a prior process to
	// Unknown until a controller re-confirms it.
	Liveness bool
	// Unconfirmed reports that Status is a downgrade this process derived, not a
	// status anyone wrote: a liveness condition an earlier process left behind,
	// read as Unknown until a controller re-confirms it. Reason, Message and the
	// stamps below are the pre-downgrade write's, so they describe the last known
	// status rather than this Unknown.
	Unconfirmed bool
	// Set by the store on read, ignored on write. A downgraded liveness
	// condition keeps the stored write's stamps.
	TransitionedAt time.Time // when Status last changed
	UpdatedAt      time.Time // when the condition was last written
}

// EventID is the store-assigned unique identifier for an event run.
type EventID = storeapi.EventID

// EventType classifies an event's severity.
type EventType string

const (
	EventNormal  EventType = "Normal"
	EventWarning EventType = "Warning"
)

// EventSpec is the caller-supplied portion of an event, passed to
// ControllerClient.Events().Add. Consecutive emissions sharing (Category, Type,
// Reason) coalesce into one run; Message and Detail are sampled (latest wins).
type EventSpec struct {
	Category string // independent timeline; "" = default
	Type     EventType
	Reason   string // machine-readable token, e.g. "ProbeFailed"
	Message  string // human-readable; sampled, not keyed
	Detail   any    // optional payload; marshaled on write; nil = none
}

// Event is one contiguous run of observations about an object, aggregated by
// (Category, Type, Reason).
type Event struct {
	ID       EventID
	ObjectID ObjectID
	Category string
	Type     EventType
	Reason   string
	Message  string          // latest occurrence's message
	Detail   json.RawMessage // latest occurrence's payload; nil = none
	Count    int             // occurrences in this run (>= 1)
	FirstAt  time.Time       // run start
	LastAt   time.Time       // run end (latest occurrence)

	// ResourceVersion orders the log and is what a watch resumes above. An
	// extend re-samples it, so a run that grew carries a fresh one.
	ResourceVersion int64
}

// EventDetail unmarshals an event's Detail payload into T. An empty Detail
// yields the zero value with a nil error.
func EventDetail[T any](e Event) (T, error) {
	var v T
	if len(e.Detail) == 0 {
		return v, nil
	}
	err := json.Unmarshal(e.Detail, &v)
	return v, err
}

// Migrator upgrades a kind's stored Spec/Status JSON to the shape this build
// expects, at the decode boundary. Spec and Status carry independent versions
// and convert independently; a current version of 0 means "not versioned".
// Conversion is lazy: bytes are upgraded on read and re-stamped only when the
// blob is next written. Register one per kind via WithMigrator.
type Migrator interface {
	// SchemaVersionSpec is the spec schema version this build writes.
	// 0 means spec is not versioned for this kind.
	SchemaVersionSpec() int
	// SchemaVersionStatus is the status schema version this build writes.
	// 0 means status is not versioned for this kind.
	SchemaVersionStatus() int
	// ConvertSpec upgrades spec bytes written at version from to the current
	// version. Called only when 0 <= from < SchemaVersionSpec(); from == 0 is
	// the unversioned baseline and must be handled.
	ConvertSpec(from int, raw json.RawMessage) (json.RawMessage, error)
	// ConvertStatus upgrades status bytes written at version from to the
	// current version; same contract as ConvertSpec.
	ConvertStatus(from int, raw json.RawMessage) (json.RawMessage, error)
}

// migratorSpecVersion is the spec schema version a write should stamp: 0 when
// the kind has no migrator.
func migratorSpecVersion(m Migrator) int {
	if m == nil {
		return 0
	}
	return m.SchemaVersionSpec()
}

// migratorStatusVersion is migratorSpecVersion for the status column.
func migratorStatusVersion(m Migrator) int {
	if m == nil {
		return 0
	}
	return m.SchemaVersionStatus()
}
