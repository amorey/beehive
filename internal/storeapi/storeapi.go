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

// Package storeapi defines the storage contract between the beehive control
// plane and its store implementations (e.g. beehive/sqlite). It is internal;
// external consumers use the aliases re-exported from the beehive package.
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

// EventID is the store-assigned unique identifier for an event run.
type EventID = int64

// ErrNotFound is returned by Store reads when no object matches.
var ErrNotFound = errors.New("beehive: object not found")

// ErrInvalidName is returned by a write whose name is "". The store, not just
// the client, must refuse it: Store is a public extension point.
var ErrInvalidName = errors.New("beehive: name must not be empty")

// ErrNameTaken is returned by a create whose name is already held within the
// GroupKind, tombstones included. Implementations MUST report this sentinel,
// not the driver's raw constraint error.
var ErrNameTaken = errors.New("beehive: name is already in use for this kind")

// ErrWrongKind is returned by an id-keyed mutator whose target id belongs to a
// different kind than gk.
var ErrWrongKind = errors.New("beehive: object belongs to a different kind")

// ErrStaleTxContext is returned by a nested Within whose ctx is not the
// transaction's live innermost frame: another goroutine's frame, an enclosing
// frame used while deeper ones are open, or a frame that already unwound.
// Backends must refuse rather than serialise. Ordinary deep nesting on one
// goroutine must be accepted.
var ErrStaleTxContext = errors.New("beehive: transaction context is not the live frame")

// ErrConcurrentNestedTx is returned by the outermost Within asked to commit
// while a nested frame is still open — proof of a concurrent goroutine, since
// same-goroutine frames unwind before fn returns.
var ErrConcurrentNestedTx = errors.New("beehive: nested transaction frame still open at commit")

// ErrObservedGenerationFuture is returned by ObjectsUpdateStatus when
// observedGeneration exceeds the object's current generation.
var ErrObservedGenerationFuture = errors.New("beehive: observed generation exceeds current generation")

// ErrSchemaVersionDowngrade is returned by ObjectsUpdateSpec/ObjectsUpdateStatus when the
// caller's schema version is non-zero and lower than the row's. Zero means "no
// opinion" and keeps the stored tag.
var ErrSchemaVersionDowngrade = errors.New("beehive: stored schema version is newer than this build's")

// ChangeType classifies a Change.
type ChangeType string

const (
	Added    ChangeType = "Added"
	Modified ChangeType = "Modified"
	Deleted  ChangeType = "Deleted"
	// Failed is terminal: the stream is over and the change carries the reason.
	Failed ChangeType = "Failed"
)

// ObjectWrite is a write-log row: which object holds a version above the
// cursor, and what that version is. No blobs, no lifecycle type.
type ObjectWrite struct {
	ID ObjectID

	// ResourceVersion is the store-wide cursor value the row now holds.
	ResourceVersion int64

	Group string
	Kind  string
	Op    WriteOp

	// Final is the object as it was when collected, set on a WriteDelete entry
	// and nil otherwise. A live row is read back from objects, so only a delete
	// has nowhere else to put its state.
	Final *RawObject
}

// WriteOp is what an ObjectWrite recorded. The soft delete is a WriteUpdate: the
// row is still live and readable, so only collection is WriteDelete.
type WriteOp int

const (
	WriteCreate WriteOp = 1
	WriteUpdate WriteOp = 2
	WriteDelete WriteOp = 3
)

// Condition is the untyped form of one condition row. Status is "True", "False"
// or "Unknown". Liveness marks an in-process condition, valid only inside the
// process that wrote it. TransitionedAt and UpdatedAt are store bookkeeping.
type Condition struct {
	Type           string
	Status         string
	Reason         string
	Message        string
	Liveness       bool
	TransitionedAt time.Time
	UpdatedAt      time.Time
}

// Event is the untyped form of one event-log row: a run of consecutive
// observations grouped by (Category, Type, Reason). Detail is opaque JSON.
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
// run for the object, newest first (by LastAt, then id).
type EventQuery struct {
	// Category, when non-nil, restricts to that exact timeline ("" included).
	// Nil returns every category interleaved.
	Category *string
	Type     string    // "" = any type
	Reason   string    // "" = any reason
	Since    time.Time // zero = no lower bound; else runs with LastAt >= Since
	Limit    int       // 0 = no limit; else the newest N runs
}

// Matches reports whether e satisfies q's filters. The event watch reads an
// unfiltered page — so its cursor advances by what the log did, not by what the
// caller asked for — and applies this to what it delivers. Limit is not a
// predicate and is not applied here.
//
// Since compares in milliseconds because that is the column's resolution, and a
// store filtering in SQL truncates the bound the same way.
func (q EventQuery) Matches(e Event) bool {
	switch {
	case q.Category != nil && e.Category != *q.Category:
		return false
	case q.Type != "" && e.Type != q.Type:
		return false
	case q.Reason != "" && e.Reason != q.Reason:
		return false
	case !q.Since.IsZero() && e.LastAt.UnixMilli() < q.Since.UnixMilli():
		return false
	}
	return true
}

// RawObject is the untyped row below the generic boundary. Spec and Status are
// opaque JSON; the store never inspects them.
// The json tags are a durable format, not decoration: a delete entry in the
// object write log stores this struct as the row image a Deleted change reports.
// Untagged, the on-disk keys would be Go field names, and renaming one would
// change the format silently — json.Unmarshal leaves an unmatched field zero
// rather than failing.
type RawObject struct {
	ID    ObjectID `json:"id"`
	Group string   `json:"group"`
	Kind  string   `json:"kind"`
	// Name is unique within its GroupKind and never empty on a returned row.
	Name   string `json:"name"`
	Spec   []byte `json:"spec"`   // JSON, user-owned
	Status []byte `json:"status"` // JSON, controller-owned; nil until first status write
	// SpecVersion and StatusVersion are the migrator schema versions each blob
	// was last written at. Persisted and returned, never interpreted.
	SpecVersion         int        `json:"specVersion"`
	StatusVersion       int        `json:"statusVersion"`
	Generation          int64      `json:"generation"`
	ObservedGeneration  *int64     `json:"observedGeneration,omitempty"`
	ObservedAt          *time.Time `json:"observedAt,omitempty"`
	ResourceVersion     int64      `json:"resourceVersion"`
	DeletionRequestedAt *time.Time `json:"deletionRequestedAt,omitempty"`
	// ReconcileOwed is the objects.reconcile_owed count; 0 means none. Store-
	// owned: moved only by EdgesAdd's stamp, ReconcileOwedStamp,
	// ReconcileOwedDecrement and ReconcileOwedSweep.
	ReconcileOwed int64       `json:"reconcileOwed"`
	Finalizers    []string    `json:"finalizers"`
	Conditions    []Condition `json:"conditions"` // assembled on reads; nil when the object has none
	CreatedAt     time.Time   `json:"createdAt"`
	UpdatedAt     time.Time   `json:"updatedAt"`
}

// ReconcileLoad is everything one reconcile pass needs from its opening read.
type ReconcileLoad struct {
	Object RawObject
	// Cursor is the store-wide write cursor as of the same statement that read
	// Object — the value to record on success.
	Cursor int64
	// HasDependencies reports whether Object had an outgoing depends_on edge at
	// load, letting a dependency-free reconcile skip DependencyWatermarksSet.
	HasDependencies bool
}

// Relation is the kind of edge in the edges table. The schema's CHECK
// constraint permits exactly these two values.
type Relation string

const (
	// RelationOwnedBy: deleting the target cascade-deletes the dependent.
	RelationOwnedBy Relation = "owned_by"
	// RelationDependsOn: a change to the target requeues the dependent.
	RelationDependsOn Relation = "depends_on"
)

// ObjectsCreateInput is everything ObjectsCreate accepts. Deliberately not
// RawObject: the rest of a new row is store-assigned or fixed (Generation 1,
// ReconcileOwed 0, Status NULL). See docs/adr/2026-07-30-store-write-shapes.md.
type ObjectsCreateInput struct {
	Finalizers []string
	// Name is required. An implementation MUST reject "" with ErrInvalidName
	// and MUST enforce uniqueness within gk, reporting ErrNameTaken as the
	// sentinel rather than the driver's constraint error.
	Name string
	Spec []byte
	// SpecVersion is the migrator schema version Spec was written at. Status
	// has no counterpart: a new row has no status.
	SpecVersion int
}

// DeletionCascadeChild is one owned child of a cascade. Marked reports whether
// this call stamped it, so a caller can follow up on exactly the children that
// moved; a re-cascade over a deleting subtree returns them all with Marked false.
type DeletionCascadeChild struct {
	Marked bool
	Ref    ObjectRef
}

// EdgesAddResult is what a caller needs to follow up on an edge it declared;
// all of it falls out of work EdgesAdd already does.
type EdgesAddResult struct {
	// From is the source object's GroupKind. Edges are cross-kind, so a caller
	// routing a requeue to fromID cannot assume its own kind.
	From GroupKind
	// ReconcileOwedStamped reports whether this call incremented fromID's
	// reconcile_owed (i.e. created a new non-self depends_on edge).
	ReconcileOwedStamped bool
}

// EdgesDeleteResult is what a caller needs to follow up on an edge it dropped;
// like EdgesAddResult, all of it falls out of work EdgesDelete already does.
type EdgesDeleteResult struct {
	// To is the target's GroupKind, needed to route a requeue to it. Edges are
	// cross-kind. Zero unless Unblocked.
	To GroupKind
	// Unblocked reports that this call removed an edge holding a RESTRICT
	// block: the target is deletion-pending and the source is not. A probe, not
	// a verdict — it does not check that this was the target's last referrer.
	Unblocked bool
}

// ObjectRef names one object: its id plus the GroupKind needed to route a
// requeue or GC step to it. Direction-free, so it serves both ends of an edge
// query and the deletion lists.
type ObjectRef struct {
	ID    ObjectID
	Group string
	Kind  string
}

// StalePos is a position in the stale-dependents scan: the target whose write
// put a dependent in scope, and the dependent reached from it. The zero value
// starts at the beginning.
//
// The fields order the scan in turn, so a page can resume inside one target's
// fan-out. A target with more dependents than one page needs that; cutting at a
// target boundary would drop the rest of them.
type StalePos struct {
	TargetVersion int64
	TargetID      ObjectID
	DependentID   ObjectID
}

// GroupKind is the kind to route a requeue (or a GC step) to.
func (r ObjectRef) GroupKind() GroupKind {
	return GroupKind{Group: r.Group, Kind: r.Kind}
}

// Store is the durable-store contract beehive depends on internally. Non-
// generic; it deals only in raw rows.
//
// A mutator returns a row exactly where a public Client write returns that
// object to the user — today ObjectsCreate and ObjectsUpdateSpec only. Where a row is
// returned, a nil error guarantees it is non-nil (including ObjectsUpdateSpec's
// no-op). Every other mutator returns only an error, plus a bool where the
// outcome is not otherwise derivable and a caller reads it; those writes
// should answer from metadata alone, with no blob or conditions query.
// See docs/adr/2026-07-30-store-write-shapes.md.
type Store interface {
	io.Closer

	// Within runs fn inside a single transaction, committing on nil error and
	// rolling back otherwise. Store calls made with the ctx passed to fn join
	// the transaction — use that ctx for every call fn makes; any other ctx
	// deadlocks a single-connection backend.
	//
	// A nested Within is a rollback boundary: it joins the transaction, only
	// the outermost commits, and an error from the nested fn MUST unwind that
	// fn's writes and queued AfterCommit hooks even if the outer caller
	// swallows the error (sqlite uses SAVEPOINT).
	//
	// A nested Within entered from a goroutine other than the enclosing
	// frame's owner MUST be refused with ErrConcurrentNestedTx, never
	// serialised; deep nesting on one goroutine must be accepted. A transaction
	// ctx belongs to one goroutine — the refusal is a tripwire, not a lock.
	// The commit check is exact: a backend must refuse to commit while any
	// nested frame is still open.
	Within(ctx context.Context, fn func(ctx context.Context) error) error

	// AfterCommit registers fn to run once ctx's transaction has committed.
	// Rule: fn runs iff the transaction committed AND the frame it was
	// registered against did not unwind. No transaction on ctx, or
	// registration after a successful commit (including from inside a running
	// hook), runs fn inline. Hooks run in registration order.
	//
	// fn receives a context detached from the transaction (deadline,
	// cancellation and values are inherited). fn must not panic: hooks run in
	// sequence and nothing recovers.
	AfterCommit(ctx context.Context, fn func(ctx context.Context))

	// ConditionsDelete removes the condition of type condType from id. A real
	// removal bumps ResourceVersion; a missing condition does nothing. Scoped
	// to gk: wrong kind → ErrWrongKind, missing id → ErrNotFound. Returns no row.
	ConditionsDelete(ctx context.Context, gk GroupKind, id ObjectID, condType string) error

	// ConditionsSet inserts or updates the condition keyed by (id, cond.Type).
	// A real change bumps ResourceVersion; an identical write does nothing.
	// Scoped to gk: wrong kind → ErrWrongKind, missing id → ErrNotFound.
	// Returns no row; read conditions back with ObjectsGet.
	ConditionsSet(ctx context.Context, gk GroupKind, id ObjectID, cond Condition) error

	// DeletionRequestsCreate sets DeletionRequestedAt; the row stays until finalizers
	// clear (ObjectsDelete removes it). changed is true only when this call set
	// the flag; repeats return false. Scoped to gk: wrong kind → ErrWrongKind,
	// missing id → ErrNotFound.
	DeletionRequestsCreate(ctx context.Context, gk GroupKind, id ObjectID) (changed bool, err error)

	// DeletionRequestsCreateByName is DeletionRequestsCreate keyed by name within gk, with
	// resolve and mark in one statement. ErrNotFound if no object of gk holds
	// the name (no ErrWrongKind: names are per-kind). id is the row the name
	// held, and is meaningful only when changed.
	DeletionRequestsCreateByName(ctx context.Context, gk GroupKind, name string) (id ObjectID, changed bool, err error)

	// DeletionRequestsCreateFromOwner requests deletion of every object owned by
	// ownerID and returns them all. Writes only to children not already
	// deletion-pending, so repeating over a deleting subtree costs one read.
	DeletionRequestsCreateFromOwner(ctx context.Context, ownerID ObjectID) ([]DeletionCascadeChild, error)

	// DeletionRequestsList returns every deletion-pending object of every
	// kind, each with its GroupKind, so the global GC sweeper can route
	// registered kinds to their controller and collect client-only kinds
	// directly.
	DeletionRequestsList(ctx context.Context) ([]ObjectRef, error)

	// EventsAdd records an observation in the (id, ev.Category) timeline. If
	// the latest run there has the same (Type, Reason) it is extended (Count
	// up, LastAt moved, Message/Detail re-sampled); otherwise a new run is
	// appended with Count 1. Only ev's Category, Type, Reason, Message and
	// Detail are read. Scoped to gk: wrong kind → ErrWrongKind, missing id →
	// ErrNotFound.
	EventsAdd(ctx context.Context, gk GroupKind, id ObjectID, ev Event) error

	// EventsGetLatest returns the most recent run in id's category timeline, or
	// nil if none. Reads by id only (not kind-scoped).
	EventsGetLatest(ctx context.Context, id ObjectID, category string) (*Event, error)

	// EventsList returns id's event runs matching q, newest first (by LastAt,
	// then id). The zero EventQuery returns every run. Not kind-scoped.
	EventsList(ctx context.Context, id ObjectID, q EventQuery) ([]Event, error)

	// EventsListSince returns id's runs above afterRV, oldest first, at most
	// limit of them, with the retention horizon (0 when nothing was trimmed). An
	// extend re-samples ResourceVersion, so the page is exactly what changed. The
	// page is unfiltered and spans every category; category selects only which
	// horizon is reported, nil meaning the max across the object's timelines.
	// ErrNotFound when id holds no object: its log cascaded away with it, so an
	// empty page there is not "no events".
	EventsListSince(ctx context.Context, id ObjectID, category *string, afterRV int64, limit int) (
		[]Event, int64, error)

	// EventsSnapshot returns id's runs matching q and the log position the
	// listing is complete as of, read in one transaction so no write falls
	// between them. The position is what EventsMaxVersion reports — the object's
	// whole log, not the query's — so a filtered watch resumes above what it
	// could not see. Not kind-scoped; an unknown id reads as no runs at 0.
	EventsSnapshot(ctx context.Context, id ObjectID, q EventQuery) ([]Event, int64, error)

	// EventsMaxVersion returns the highest ResourceVersion over id's event
	// runs, 0 when there are none (unknown id included). Spans every category
	// and ignores filters. Not monotonic — retention can lower it — so
	// consumers compare for inequality, not greater-than.
	EventsMaxVersion(ctx context.Context, id ObjectID) (int64, error)

	// EventsSweep trims the event log to the retention bounds and returns how
	// many runs it deleted. perObject > 0 caps each (object, category)
	// timeline to its newest perObject runs; maxAge > 0 drops runs with LastAt
	// older than that. A zero bound is skipped. Global, not per-kind.
	EventsSweep(ctx context.Context, perObject int, maxAge time.Duration) (int, error)

	// FinalizersDelete removes finalizer from id's list. A real removal bumps
	// ResourceVersion; a missing one does nothing. clearedLast reports that this
	// call removed the last finalizer from a deletion-pending row. Scoped to gk:
	// wrong kind → ErrWrongKind, missing id → ErrNotFound. Returns no row.
	FinalizersDelete(ctx context.Context, gk GroupKind, id ObjectID, finalizer string) (clearedLast bool, err error)

	// ObjectsCreate inserts a new object of kind gk. The store assigns ID and
	// ResourceVersion and sets Generation to 1.
	ObjectsCreate(ctx context.Context, gk GroupKind, in ObjectsCreateInput) (*RawObject, error)

	// ObjectsDelete removes the row outright — the physical delete the GC path
	// performs. Callers must ensure finalizers are empty first.
	ObjectsDelete(ctx context.Context, id ObjectID) error

	// ObjectsGet loads an object by id, or returns ErrNotFound.
	ObjectsGet(ctx context.Context, id ObjectID) (*RawObject, error)

	// ObjectsGetByName loads the object with the given name within gk, or
	// returns ErrNotFound.
	ObjectsGetByName(ctx context.Context, gk GroupKind, name string) (*RawObject, error)

	// ObjectsGetForReconcile is the reconcile loop's opening read: the object
	// with conditions, the store-wide write cursor as of the same statement,
	// and whether it has dependencies. ErrNotFound if the row is gone.
	ObjectsGetForReconcile(ctx context.Context, id ObjectID) (ReconcileLoad, error)

	// ObjectsGetMeta is ObjectsGet without the conditions query (Conditions is
	// always nil). ErrNotFound if no object matches.
	ObjectsGetMeta(ctx context.Context, id ObjectID) (*RawObject, error)

	// ObjectsList returns every object of kind gk, ordered by id.
	ObjectsList(ctx context.Context, gk GroupKind) ([]*RawObject, error)

	// ObjectsListByIncomingEdge returns the full objects of kind gk that point
	// at toID through relation, ordered by id, with conditions — edges and
	// rows in one query. Other kinds are filtered out; no matching edge reads
	// empty, not ErrNotFound.
	ObjectsListByIncomingEdge(ctx context.Context, gk GroupKind, toID ObjectID, relation Relation) ([]*RawObject, error)

	// ObjectsListByIDs returns the objects of kind gk whose ids are in ids,
	// ordered by id — creation order, not the caller's order and not
	// resource_version order. An id naming no object, or one of another kind, is
	// absent: a short result is normal, not an error. Callers keep ids to a
	// batch a backend can bind in one statement; the watch tail bounds it by its
	// page cap.
	ObjectsListByIDs(ctx context.Context, gk GroupKind, ids []ObjectID) ([]*RawObject, error)

	// ObjectsListIDs returns the ids of every object of kind gk, ordered by id.
	ObjectsListIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error)

	// ObjectsListUnsettledIDs returns the IDs of objects of kind gk whose
	// observed_generation doesn't match generation (not yet converged).
	ObjectsListUnsettledIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error)

	// ObjectsUpdateSpec replaces an object's spec, bumping Generation and
	// ResourceVersion, and stamps specVersion. Spec bytes identical to the
	// stored ones at the row's own schema version are a no-op: no bump, and
	// changed reports false (bytes at a different schema version always take
	// the write path). changed=true is what callers enqueue a reconcile on.
	// Scoped to gk: wrong kind → ErrWrongKind, missing id → ErrNotFound.
	ObjectsUpdateSpec(ctx context.Context, gk GroupKind, id ObjectID, spec []byte, specVersion int) (obj *RawObject, changed bool, err error)

	// ObjectsUpdateSpecByName is ObjectsUpdateSpec keyed by name within gk, ErrNotFound if
	// the name is not held (no ErrWrongKind). Same no-op and changed
	// semantics. An implementation MUST resolve and write in one transaction:
	// the no-op comparison needs the stored bytes, and a split would let a
	// concurrent collect hand the name to a replacement in between.
	ObjectsUpdateSpecByName(ctx context.Context, gk GroupKind, name string, spec []byte, specVersion int) (obj *RawObject, changed bool, err error)

	// ObjectsUpdateStatus replaces an object's status, records observedGeneration and
	// stamps statusVersion. Changed bytes bump ObservedAt, ResourceVersion and
	// UpdatedAt. Bytes identical at the row's own schema version write no
	// status — but ObservedGeneration/ObservedAt (and ResourceVersion) still
	// advance if this reconcile settled a new generation, at most once per
	// generation; a call identical in both respects writes nothing at all.
	//
	// A stale observedGeneration (at or below the recorded value) is ignored
	// on the no-op path but written as given on the content-changed path,
	// rolling the object back to unsettled so a later pass re-derives it.
	//
	// Scoped to gk: wrong kind → ErrWrongKind, missing id → ErrNotFound.
	// observedGeneration above the row's generation → ErrObservedGenerationFuture.
	// Returns no row.
	ObjectsUpdateStatus(ctx context.Context, gk GroupKind, id ObjectID, observedGeneration int64, status []byte, statusVersion int) error

	// EdgesAdd inserts a directed (fromID -> toID) edge with the given
	// relation. Idempotent; both endpoints must exist or ErrNotFound. Bumps no
	// version.
	//
	// Every new depends_on edge it creates (self-edges excluded) increments
	// fromID's reconcile_owed and reports it as ReconcileOwedStamped. The
	// endpoint check, the stamp and the insert MUST be one atomic unit — a
	// stamp issued separately could be dropped while the edge commits,
	// stranding the dependent. The same edge-new gate also clears fromID's
	// dependency_watermarks row (a watermark recorded over a smaller
	// dependency set cannot speak for a new target). The stamp is
	// unconditional on the caller and independent of whether fromID's kind has
	// a controller. See docs/adr/2026-07-29-stamp-every-new-dependency-edge.md.
	EdgesAdd(ctx context.Context, fromID, toID ObjectID, relation Relation) (EdgesAddResult, error)

	// EdgesDelete removes the (fromID, toID, relation) edge; removing a missing
	// one does nothing. Bumps no version. Reports whether the removal lifted a
	// RESTRICT block, so a caller can push the target's collect.
	EdgesDelete(ctx context.Context, fromID, toID ObjectID, relation Relation) (EdgesDeleteResult, error)

	// EdgesDeleteFinalizingDependsOn removes the depends_on edges pointing at toID
	// whose source is itself marked for deletion, so mutually dependent
	// finalizing objects don't RESTRICT-block each other forever. owned_by
	// edges are left alone.
	EdgesDeleteFinalizingDependsOn(ctx context.Context, toID ObjectID) error

	// EdgesGroupIncomingByID is the batched form of EdgesListIncoming: sources
	// pointing at many targets through relation, grouped by target id. A
	// target with no sources is absent from the map.
	EdgesGroupIncomingByID(ctx context.Context, toIDs []ObjectID, relation Relation) (map[ObjectID][]ObjectRef, error)

	// EdgesGroupOutgoingByID is the batched form of EdgesListOutgoingByRelation: the
	// relation's targets for many sources, grouped by source id. A source with
	// no matching edge is absent from the map. An implementation may split a
	// large id list across queries.
	EdgesGroupOutgoingByID(ctx context.Context, fromIDs []ObjectID, relation Relation) (map[ObjectID][]ObjectRef, error)

	// EdgesHasIncoming reports whether anything with a live claim points at
	// id: an owned_by edge, or a depends_on edge from a source that is not
	// itself finalizing (a deletion-pending dependent no longer counts, or
	// mutually dependent finalizing objects would deadlock). GC pairs this
	// with EdgesDeleteFinalizingDependsOn before ObjectsDelete.
	EdgesHasIncoming(ctx context.Context, id ObjectID) (bool, error)

	// EdgesListIncoming returns every object pointing at toID through
	// relation, ordered by id.
	EdgesListIncoming(ctx context.Context, toID ObjectID, relation Relation) ([]ObjectRef, error)

	// EdgesListOutgoingByRelation returns the objects fromID points at through relation,
	// ordered by id — the inverse of EdgesListIncoming.
	EdgesListOutgoingByRelation(ctx context.Context, fromID ObjectID, relation Relation) ([]ObjectRef, error)

	// ReconcileOwedDecrement subtracts observed from id's reconcile_owed,
	// floored at 0. Callers pass the count they loaded, not 1: one pass
	// answers every wake outstanding at its load, and increments landing after
	// the load survive the subtraction. Bumps no resource_version. Scoped to
	// gk: wrong kind → ErrWrongKind; a row that is simply gone is not an error.
	ReconcileOwedDecrement(ctx context.Context, gk GroupKind, id ObjectID, observed int64) error

	// ReconcileOwedListIDs returns the ids of objects of kind gk with
	// reconcile_owed != 0, so a wake recorded before a crash is serviced on
	// restart. Separate from ObjectsListUnsettledIDs: a settled object can still owe
	// a wake.
	ReconcileOwedListIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error)

	// ReconcileOwedStamp increments reconcile_owed once for each DISTINCT ref, so
	// a finding outlives the in-memory queue. Repeats inside one call fold into
	// that single increment: a caller owed two wakes for one object must make two
	// calls. Sound for the pass, whose listing returns a row per (target,
	// dependent) pair, because one reconcile answers every wake outstanding at its
	// load and ReconcileOwedDecrement subtracts the whole count it observed.
	//
	// An id that is gone is skipped, not reported. Empty refs writes nothing.
	// Bumps no resource_version.
	//
	// Not kind-scoped, unlike ReconcileOwedDecrement: the refs come from the
	// store's own listing, which spans every registered kind in one page.
	ReconcileOwedStamp(ctx context.Context, refs []ObjectRef) error

	// ReconcileOwedSweep zeroes reconcile_owed for every object whose kind is not
	// in keep, and returns how many rows it cleared. An empty keep clears every
	// nonzero row. Bumps no resource_version and appends no write-log entry.
	// See docs/adr/2026-08-05-reclaim-a-client-only-owed-count.md.
	ReconcileOwedSweep(ctx context.Context, keep []GroupKind) (int, error)

	// ResourceVersionsMaxIssued returns the highest resource version issued. It
	// reads the sequence, not a table, so retention cannot lower it. It moves
	// for an event write too, so it is a "did anything change" answer, not a
	// log position to scan from.
	ResourceVersionsMaxIssued(ctx context.Context) (int64, error)

	// DependencyWatermarksSet records cursor as the store-wide write cursor
	// id's reconcile observed. Upserts; the stored cursor never decreases, and
	// a non-advancing value writes nothing. Bumps no resource_version (it
	// writes no objects row). Also sets a reconciled-at timestamp under the
	// same predicate — observability only.
	//
	// The write gates in SQL on id having at least one outgoing depends_on
	// edge: a dependency-free object can never be found stale, and the gate
	// closes safely when the object is collected mid-pass. EdgesAdd is the
	// row's other writer and only clears it.
	// See docs/adr/2026-07-29-dependency-watermarks.md.
	DependencyWatermarksSet(ctx context.Context, id ObjectID, cursor int64) error

	// DependentsListStaleSince returns objects of the given kinds with a
	// depends_on edge to a target whose resource_version is above their
	// dependency watermark, bounded to targets written above after and no higher
	// than through. Ordered by StalePos, at most limit rows, plus the position of
	// the last row to resume from. An empty kinds slice returns nothing. A
	// missing watermark counts as stale; self-edges are excluded. Cost tracks
	// what changed, not the graph.
	//
	// The kind filter applies to the DEPENDENT and MUST NOT be extended to the
	// target: a registered object may depend on a client-only one, and
	// narrowing to registered targets would silently strand its dependents.
	//
	// through is what makes a sweep finite. Without it a store taking writes
	// faster than the caller pages could never reach a short page, so the sweep
	// would never end and its cursor would never move. Targets written above
	// through belong to the next sweep.
	//
	// A dependent appears once per moved target it depends on; stamping and
	// enqueuing are idempotent, so a duplicate costs a pass, not correctness.
	DependentsListStaleSince(ctx context.Context, kinds []GroupKind, after StalePos, through int64, limit int) ([]ObjectRef, StalePos, error)

	// ObjectWritesListSince returns gk's log entries above afterRV in cursor
	// order, at most limit. afterRV < trimmedThrough means entries were trimmed
	// unread and the caller must resync; equality is fine, since the next unread
	// entry is trimmedThrough + 1.
	//
	// An implementation MUST read the page, the horizon and the delete entries'
	// row images atomically — they describe one instant or they are wrong. Read
	// apart, a retention sweep landing between them can report a horizon above
	// entries the page already captured, which reads as unrecoverable loss for a
	// stream that lost nothing, or delete a captured entry's image, leaving a
	// WriteDelete with a nil Final that has no state to report.
	//
	// Every WriteDelete entry returned MUST carry a non-nil Final.
	ObjectWritesListSince(ctx context.Context, gk GroupKind, afterRV int64, limit int) (page []ObjectWrite, trimmedThrough int64, err error)

	// ObjectWritesListSinceAll is ObjectWritesListSince across every kind, for
	// the dependency waker: an edge can point at a kind with no controller. It
	// reports no horizon, because the waker's cursor is an optimisation over the
	// stale-dependents pass rather than a guarantee.
	ObjectWritesListSinceAll(ctx context.Context, afterRV int64, limit int) ([]ObjectWrite, error)

	// ObjectWritesMaxVersion returns gk's log position: every entry for gk is at
	// or below it, and ObjectWritesListSince returns nothing above it.
	ObjectWritesMaxVersion(ctx context.Context, gk GroupKind) (int64, error)

	// ObjectWritesMaxVersionAll is ObjectWritesMaxVersion across every kind. Not
	// monotonic — a delete lowers it — so consumers compare for inequality.
	ObjectWritesMaxVersionAll(ctx context.Context) (int64, error)

	// ObjectWritesSnapshot returns every object of kind gk and the log position
	// the listing is complete as of, read in one transaction so no write falls
	// between them. The position is what ObjectWritesMaxVersion reports.
	ObjectWritesSnapshot(ctx context.Context, gk GroupKind) ([]*RawObject, int64, error)

	// ObjectWritesSnapshotByID is ObjectWritesSnapshot for one object: the row,
	// or no rows when id does not exist or belongs to another kind, and gk's log
	// position — the kind's, because the stream that follows tails the kind.
	ObjectWritesSnapshotByID(ctx context.Context, gk GroupKind, id ObjectID) ([]*RawObject, int64, error)

	// ObjectWritesSweep trims the write log to the retention bounds and returns
	// how many entries it deleted. perKind > 0 caps each (group, kind) log to
	// its newest perKind entries; maxAge > 0 drops entries written more than
	// maxAge ago. A zero bound is skipped. It raises each affected kind's
	// horizon in the same transaction that deletes that kind's entries, so a
	// resume is never accepted against a log with a hole in it.
	ObjectWritesSweep(ctx context.Context, perKind int, maxAge time.Duration) (int, error)
}

// FreePagesReleaser is an optional Store capability: a backend that can hand
// space freed by deleted rows back to the OS implements it, and the GC sweeper
// calls it after collecting. Deliberately not a member of Store — a backend
// without it is simply not drained.
type FreePagesReleaser interface {
	// FreePagesRelease releases up to maxPages of reclaimable space and
	// returns how many were actually released — a report, not a guarantee; a
	// backend may release fewer, including none. Non-positive maxPages
	// releases nothing.
	FreePagesRelease(ctx context.Context, maxPages int) (int, error)
}

// DriverCursorer is an optional Store capability: persistence for a periodic
// driver's scan position, so a restart resumes rather than reseeding from
// "now". Deliberately not a member of Store — it is a latency optimisation
// over the stale-dependents pass, which already guarantees the wake.
type DriverCursorer interface {
	// DriverCursorsGet returns the cursor last persisted for name, or ok=false
	// if none has been yet. Absence is normal, not an error; zero is a
	// legitimate cursor value, so it cannot double as "no cursor".
	DriverCursorsGet(ctx context.Context, name string) (cursor int64, ok bool, err error)

	// DriverCursorsSet persists cursor for name if it is greater than what is
	// stored, and otherwise writes nothing — a non-advancing call must cost no
	// write, since a driver may call this every tick.
	DriverCursorsSet(ctx context.Context, name string, cursor int64) error
}
