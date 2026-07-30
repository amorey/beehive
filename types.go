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

// ObjectRef identifies a related object reached through an edge — an owner, a
// dependency, or a dependent — carrying the GroupKind needed to address it. It
// is a reference to the object, not the edge itself: the store's Edges* family
// deals in edges, and every one of its queries returns this same shape.
type ObjectRef = storeapi.ObjectRef

// Store is the durable-store contract Beehive depends on internally. It is
// non-generic and deals only in raw rows: the generic-to-non-generic boundary
// lives one layer up, in the typedController adapter.
//
// ObjectsCreate and ObjectsUpdateSpec return the row they wrote, so a caller can
// hand the store-assigned id, resource_version and timestamps straight to the user.
// Every other mutator returns only an error — see the contract on storeapi.Store.
type Store = storeapi.Store

// FreePagesReleaser is an optional capability a Store may implement to hand space
// freed by deleted rows back to the operating system. The GC sweeper calls it after
// its other two steps, which are what produce that space; a Store that does not
// implement it is skipped. The sqlite store implements it on top of
// auto_vacuum=INCREMENTAL.
type FreePagesReleaser = storeapi.FreePagesReleaser

// DriverCursorer is an optional capability a Store may implement to persist a
// periodic driver's scan position across restarts. The dependency waker uses it
// when present and falls back to its in-memory-only cursor otherwise. The sqlite
// store implements it on the driver_cursors table.
type DriverCursorer = storeapi.DriverCursorer

// RawObject is the untyped row below the generic boundary. Spec and Status are
// opaque JSON bytes; everything else is Beehive-owned metadata that mirrors the
// objects table. The reconciler and client decode Spec/Status into typed
// Object[Spec, Status] values; the Store never inspects them.
type RawObject = storeapi.RawObject

// ObjectsCreateInput is the narrow write shape ObjectsCreate accepts — only the
// fields a create honours, so the compiler refuses the rest rather than the store
// dropping them.
type ObjectsCreateInput = storeapi.ObjectsCreateInput

// RawEvent is the untyped event-log row below the generic boundary — one
// aggregated run. The client decodes it into the public Event.
type RawEvent = storeapi.Event

// Relation is the kind of edge in the edges table.
type Relation = storeapi.Relation

const (
	RelationOwnedBy   = storeapi.RelationOwnedBy
	RelationDependsOn = storeapi.RelationDependsOn
)

// ObjectWrite names one object whose version is above a scan's cursor, with no
// row attached. It is what a scan of the store's write log returns, which is the
// only change-notification path there is.
type ObjectWrite = storeapi.ObjectWrite

// ChangeType classifies a Change.
type ChangeType = storeapi.ChangeType

const (
	Added    = storeapi.Added
	Modified = storeapi.Modified
	Deleted  = storeapi.Deleted
)

// ErrNotFound is returned by Store reads when no object matches.
var ErrNotFound = storeapi.ErrNotFound

// ErrStaleTxContext is returned by a nested Within whose ctx is not the transaction's
// live innermost frame — another goroutine's frame, an enclosing frame used while
// deeper ones are open, or a frame that already unwound. Deep nesting on one goroutine
// is fine; using a ctx from outside the frame you are in is not.
var ErrStaleTxContext = storeapi.ErrStaleTxContext

// ErrConcurrentNestedTx is returned by the outermost Within when a nested frame is
// still open at the commit, which can only mean another goroutine holds one.
var ErrConcurrentNestedTx = storeapi.ErrConcurrentNestedTx

// ErrObservedGenerationFuture is returned by UpdateStatus when the caller reports
// a generation greater than the object's current one — a convergence-handshake
// violation (a controller must pass the generation it received in Reconcile).
var ErrObservedGenerationFuture = storeapi.ErrObservedGenerationFuture

// ErrSchemaVersionDowngrade is returned by ObjectsUpdateSpec/UpdateStatus when the
// caller's schema version is lower than the one stamped on the row — the
// write-side twin of the read path's refusal to decode data a newer build wrote.
var ErrSchemaVersionDowngrade = storeapi.ErrSchemaVersionDowngrade

// LoadSet is a bitset of secondary lookups (owner, dependencies, dependents,
// owned) to fetch alongside an object. The zero value loads nothing; reads OR in the
// bits a caller selects, and the populated Object records what was fetched so
// the accessors can tell "loaded and empty" from "never asked".
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
	// LoadEventsBit selects the object's event-log runs (see LoadEvents).
	LoadEventsBit
)

// Object is a single resource: user-owned desired state (Spec) plus
// controller-owned observed state (Status), along with the metadata Beehive
// uses to track convergence and deletion.
type Object[Spec, Status any] struct {
	ID                  ObjectID
	Group               string
	Kind                string
	Slug                string
	Spec                Spec
	Status              *Status
	Generation          int64      // bumped on every Spec write not provably a no-op (see ObjectsUpdateSpec)
	ObservedGeneration  *int64     // Generation the controller last reconciled; nil until first reconcile
	ObservedAt          *time.Time // when ObservedGeneration was recorded; not a reconcile heartbeat
	ResourceVersion     int64      // bumped on every write, for optimistic concurrency
	DeletionRequestedAt *time.Time // set when deletion is requested; object lingers until finalizers clear
	Finalizers          []string
	Conditions          []Condition // per-type observations reported by controllers
	CreatedAt           time.Time
	UpdatedAt           time.Time

	// Related data, populated only for the lookups a read requested (see LoadSet).
	// A nil/empty field is ambiguous on its own — which loaded records what was
	// actually fetched, so the OwnersGet/DependenciesList/DependentsList/OwnedList
	// accessors distinguish "loaded and empty" from "never asked". These fields are
	// unexported; reach for the accessors, never the backing storage.
	owner        *ObjectRef  // the owning object, if any
	dependencies []ObjectRef // objects this one depends on
	dependents   []ObjectRef // objects that depend on this one
	owned        []ObjectRef // objects this one owns
	events       []Event     // the object's event-log runs
	loaded       LoadSet
}

// ErrNotLoaded is returned by the secondary-lookup accessors when the requested
// relation was not fetched on the read that produced the object. It marks caller
// misuse — forgetting LoadOwner()/LoadDependencies()/LoadDependents() — not a
// missing object, so it is kept distinct from a present-but-empty result.
var ErrNotLoaded = errors.New("beehive: secondary lookup not loaded")

// Owner returns the object's owner. It errors with ErrNotLoaded if LoadOwner()
// was not passed to the read. Otherwise ok reports presence — false when the
// object has no owner. (Use the lazy Client.Owner to fetch on demand instead.)
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
// LoadDependencies() was not passed to the read. A loaded-but-empty result is an
// empty slice with a nil error.
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

// Owned returns the objects this one owns (its incoming owned_by edges), or
// ErrNotLoaded if LoadOwned() was not passed to the read. A loaded-but-empty
// result is an empty slice with a nil error.
func (o *Object[Spec, Status]) Owned() ([]ObjectRef, error) {
	if o.loaded&LoadOwnedBit == 0 {
		return nil, fmt.Errorf("%w: owned (pass LoadOwned())", ErrNotLoaded)
	}
	return o.owned, nil
}

// Events returns the object's event-log runs, newest-first, or ErrNotLoaded
// if LoadEvents() was not passed to the read. A loaded-but-empty log is an empty
// slice with a nil error. (Use the lazy Client.Events to fetch on demand, or
// to filter/limit.)
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

// Schedule reports when an object is next due to reconcile. It is a struct rather
// than a bare time.Time so fields can be added without a breaking change — a
// reschedule watcher (Client.SchedulesWatch) observes this value as a gauge.
type Schedule struct {
	// NextRequeueAt is when the reconcile loop has scheduled the object to be
	// requeued, or the zero time when nothing is scheduled. It reflects only per-id
	// timers (backoff, RequeueAfter, an immediate enqueue), not any of the periodic
	// drivers. Reported by Client.SchedulesGet and SchedulesWatch.
	NextRequeueAt time.Time
	// Reserved: a future Trigger/Reason enum (backoff vs success-cadence vs manual
	// poke) may be added here. Not populated yet.
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

// EventID is the store-assigned unique identifier for an event run.
type EventID = storeapi.EventID

// EventType classifies an event's severity: Normal (✓) or Warning (✗).
type EventType string

const (
	EventNormal  EventType = "Normal"
	EventWarning EventType = "Warning"
)

// EventSpec is the caller-supplied portion of an event, passed to
// ControllerClient.EventsAdd. It excludes the store-owned run fields (id,
// count, window) so a caller can't set them. Consecutive emissions sharing
// (Category, Type, Reason) coalesce into one run; Message and Detail are sampled
// (latest wins), not part of that key.
type EventSpec struct {
	Category string // independent timeline; "" = default
	Type     EventType
	Reason   string // machine-readable token, e.g. "ProbeFailed"
	Message  string // human-readable; sampled, not keyed
	Detail   any    // optional payload; marshaled on write; nil = none
}

// Event is one contiguous run of observations about an object, aggregated by
// (Category, Type, Reason). Count grows and the [FirstAt, LastAt] window widens
// while the run holds; a change in the key starts a new run.
type Event struct {
	ID       EventID
	ObjectID ObjectID // object this event is about
	Category string
	Type     EventType
	Reason   string
	Message  string          // latest occurrence's message
	Detail   json.RawMessage // latest occurrence's payload; nil = none
	Count    int             // occurrences in this run (>= 1)
	FirstAt  time.Time       // run start
	LastAt   time.Time       // run end (latest occurrence)
}

// EventDetail unmarshals an event's Detail payload into T. An empty Detail
// yields the zero value with a nil error; otherwise it returns the result of
// json.Unmarshal. It is a free generic helper over the non-generic Event, so a
// single timeline can carry reasons with different detail shapes — decode each
// with the type its Reason implies.
func EventDetail[T any](e Event) (T, error) {
	var v T
	if len(e.Detail) == 0 {
		return v, nil
	}
	err := json.Unmarshal(e.Detail, &v)
	return v, err
}

// Migrator upgrades a kind's stored Spec/Status JSON to the shape this build
// expects, at the rawToTyped decode boundary. Beehive stores Spec and Status as
// opaque JSON and records the schema version each blob was written at (per
// column); on read, a blob older than this build's current version is converted
// up before it is unmarshalled, so a consumer can evolve its Spec/Status structs
// without breaking decode of rows written by an earlier build.
//
// Spec and Status carry independent versions and convert independently — a
// status-only write re-stamps only the status version. A current version of 0
// means "not versioned": that blob is never converted (the kind hasn't opted in
// for it). Conversion is lazy: bytes are upgraded on read and re-stamped only
// when the blob is next written, never by a bulk row rewrite.
//
// Register a Migrator per kind via WithMigrator passed to Register.
type Migrator interface {
	// SchemaVersionSpec is the spec schema version this build writes (and
	// converts up to). 0 means spec is not versioned for this kind.
	SchemaVersionSpec() int
	// SchemaVersionStatus is the status schema version this build writes (and
	// converts up to). 0 means status is not versioned for this kind.
	SchemaVersionStatus() int
	// ConvertSpec upgrades spec bytes written at version from to the current spec
	// version. It is called only when 0 <= from < SchemaVersionSpec(); from == 0 is
	// the unversioned baseline (a row written before this kind opted into
	// versioning), so once a migrator is enabled the converter must handle it.
	ConvertSpec(from int, raw json.RawMessage) (json.RawMessage, error)
	// ConvertStatus upgrades status bytes written at version from to the current
	// status version. It is called only when 0 <= from < SchemaVersionStatus();
	// from == 0 is the unversioned baseline the converter must handle (see
	// ConvertSpec).
	ConvertStatus(from int, raw json.RawMessage) (json.RawMessage, error)
}

// migratorSpecVersion is the spec schema version a write should stamp: the
// migrator's current spec version, or 0 when the kind has no migrator. Keeping
// the nil check here lets the write paths stamp unconditionally.
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
