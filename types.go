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

// Ref identifies a related object reached through a ref edge — an owner, a
// dependency, or a dependent — carrying the GroupKind needed to address it. It
// is the same shape the store returns for every edge query.
type Ref = storeapi.Referrer

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

	// Related data, populated only for the lookups a read requested (see LoadSet).
	// A nil/empty field is ambiguous on its own — which loaded records what was
	// actually fetched, so the GetOwner/ListDependencies/ListDependents/ListOwned
	// accessors distinguish "loaded and empty" from "never asked". These fields are
	// unexported; reach for the accessors, never the backing storage.
	owner        *Ref    // the owning object, if any
	dependencies []Ref   // objects this one depends on
	dependents   []Ref   // objects that depend on this one
	owned        []Ref   // objects this one owns
	events       []Event // the object's event-log runs
	loaded       LoadSet
}

// ErrNotLoaded is returned by the secondary-lookup accessors when the requested
// relation was not fetched on the read that produced the object. It marks caller
// misuse — forgetting LoadOwner()/LoadDependencies()/LoadDependents() — not a
// missing object, so it is kept distinct from a present-but-empty result.
var ErrNotLoaded = errors.New("beehive: secondary lookup not loaded")

// GetOwner returns the object's owner. It errors with ErrNotLoaded if LoadOwner()
// was not passed to the read. Otherwise ok reports presence — false when the
// object has no owner. (Use the lazy Client.GetOwner to fetch on demand instead.)
func (o *Object[Spec, Status]) GetOwner() (Ref, bool, error) {
	if o.loaded&LoadOwnerBit == 0 {
		return Ref{}, false, fmt.Errorf("%w: owner (pass LoadOwner())", ErrNotLoaded)
	}
	if o.owner == nil {
		return Ref{}, false, nil
	}
	return *o.owner, true, nil
}

// ListDependencies returns the objects this one depends on, or ErrNotLoaded if
// LoadDependencies() was not passed to the read. A loaded-but-empty result is an
// empty slice with a nil error.
func (o *Object[Spec, Status]) ListDependencies() ([]Ref, error) {
	if o.loaded&LoadDependenciesBit == 0 {
		return nil, fmt.Errorf("%w: dependencies (pass LoadDependencies())", ErrNotLoaded)
	}
	return o.dependencies, nil
}

// ListDependents returns the objects that depend on this one, or ErrNotLoaded if
// LoadDependents() was not passed to the read.
func (o *Object[Spec, Status]) ListDependents() ([]Ref, error) {
	if o.loaded&LoadDependentsBit == 0 {
		return nil, fmt.Errorf("%w: dependents (pass LoadDependents())", ErrNotLoaded)
	}
	return o.dependents, nil
}

// ListOwned returns the objects this one owns (its incoming owned_by edges), or
// ErrNotLoaded if LoadOwned() was not passed to the read. A loaded-but-empty
// result is an empty slice with a nil error.
func (o *Object[Spec, Status]) ListOwned() ([]Ref, error) {
	if o.loaded&LoadOwnedBit == 0 {
		return nil, fmt.Errorf("%w: owned (pass LoadOwned())", ErrNotLoaded)
	}
	return o.owned, nil
}

// ListEvents returns the object's event-log runs, newest-first, or ErrNotLoaded
// if LoadEvents() was not passed to the read. A loaded-but-empty log is an empty
// slice with a nil error. (Use the lazy Client.ListEvents to fetch on demand, or
// to filter/limit.)
func (o *Object[Spec, Status]) ListEvents() ([]Event, error) {
	if o.loaded&LoadEventsBit == 0 {
		return nil, fmt.Errorf("%w: events (pass LoadEvents())", ErrNotLoaded)
	}
	return o.events, nil
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

// EventID is the store-assigned unique identifier for an event run.
type EventID = storeapi.EventID

// EventType classifies an event's severity: Normal (✓) or Warning (✗).
type EventType string

const (
	EventNormal  EventType = "Normal"
	EventWarning EventType = "Warning"
)

// EventSpec is the caller-supplied portion of an event, passed to
// ControllerClient.RecordEvent. It excludes the store-owned run fields (id,
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
