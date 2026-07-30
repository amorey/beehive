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

// ErrWrongKind is returned by an id-keyed mutator whose target id belongs to a
// different kind than the gk passed in. The store folds the caller's kind into every
// write, so another kind's id is rejected at the source rather than corrupting that
// kind's row. Surfaces above translate it as they see fit: the user-facing client
// hides it as ErrNotFound, a controller reports it.
var ErrWrongKind = errors.New("beehive: object belongs to a different kind")

// ErrStaleTxContext is returned by a nested Within whose ctx is not the transaction's
// live innermost frame. Three things reach it, and only the first involves concurrency:
//
//   - The ctx belongs to another goroutine's frame, so this one is not at the top of
//     the savepoint stack.
//   - The ctx is an *enclosing* frame's, used while deeper frames are still open —
//     reachable on one goroutine, since an enclosing ctx stays in lexical scope. Before
//     the rollback boundary existed this joined the transaction silently; it is now
//     refused, because a savepoint opened out of stack order unwinds the wrong things.
//   - The ctx belongs to a frame that already unwound. Its writes are gone, and
//     admitting it would let them be written again into a transaction that will
//     commit, while hooks registered on the same ctx are still discarded — the store
//     answering "is this frame alive" two different ways.
//
// The name is about the context, not the caller: "stale" here means "not the frame
// this transaction is currently in", which a single goroutine can produce by holding
// a ctx past its scope.
//
// Backends implementing the boundary with savepoints must refuse rather than serialise
// — holding a lock across fn deadlocks as soon as fn waits on another goroutine that
// also wants the store. Ordinary deep nesting on one goroutine is not this: a
// Client.Create inside a ControllerClient.Within, with the mutator's own self-wrap
// below it, is three frames and must be accepted.
var ErrStaleTxContext = errors.New("beehive: transaction context is not the live frame")

// ErrConcurrentNestedTx is returned by the outermost Within when it is asked to commit
// while a nested frame is still open. Unlike ErrStaleTxContext this one *proves*
// concurrency: nested frames unwind in a defer, so on a single goroutine the stack is
// empty by the time fn returns, and a frame still open can only belong to another
// goroutine. It is the one place the one-goroutine-per-transaction rule is enforced
// exactly rather than sampled.
//
// Committing there would release that frame's savepoint and persist writes it is still
// entitled to roll back, with nothing left that could undo them.
var ErrConcurrentNestedTx = errors.New("beehive: nested transaction frame still open at commit")

// ErrObservedGenerationFuture is returned by UpdateStatus when observedGeneration is
// greater than the object's current generation. A controller can only report a
// generation it actually saw in Reconcile, so a value from the future would mark the
// object settled as soon as its spec caught up — a broken handshake.
var ErrObservedGenerationFuture = errors.New("beehive: observed generation exceeds current generation")

// ErrSchemaVersionDowngrade is returned by ObjectsUpdateSpec/UpdateStatus when the
// caller's schema version is non-zero and lower than the one on the row. Zero means
// "no opinion" — the kind is unversioned, or this build has no migrator for it — and
// keeps the stored tag rather than erroring.
//
// It is the write-side twin of the read path refusing a downgrade: an older versioned
// build must not relabel newer bytes as older, or every later read would convert
// already-converted data instead of refusing to decode it.
var ErrSchemaVersionDowngrade = errors.New("beehive: stored schema version is newer than this build's")

// ChangeType classifies a Change.
type ChangeType string

const (
	Added    ChangeType = "Added"
	Modified ChangeType = "Modified"
	Deleted  ChangeType = "Deleted"
)

// ObjectWrite is a write-log row cut down to what a consumer routing by identity
// needs: which object holds a version above the cursor, and what that version is.
//
// It carries no lifecycle type, because the row records the version it holds now, not
// how it got there. It carries no *RawObject either: a scan covers every kind in the
// store, so carrying rows would load spec and status blobs for writes the consumer
// only needs to route by id.
type ObjectWrite struct {
	ID ObjectID

	// ResourceVersion is the version the row now holds. It is the store-wide cursor
	// (see resource_version_seq), so a consumer that records the highest version it
	// has finished processing resumes from there instead of re-deriving the world.
	ResourceVersion int64
}

// Condition is the untyped form of one condition row. Status is "True", "False" or
// "Unknown". Liveness marks a condition describing a live in-process resource, valid
// only inside the process that wrote it — see the read path's "verifying" downgrade.
// The client decodes these into the public beehive.Condition; the store-only
// bookkeeping fields, TransitionedAt and UpdatedAt, stop at that boundary.
type Condition struct {
	Type           string
	Status         string
	Reason         string
	Message        string
	Liveness       bool
	TransitionedAt time.Time
	UpdatedAt      time.Time
}

// Event is the untyped form of one event-log row: a run of consecutive observations
// about an object, grouped by (Category, Type, Reason). Detail is opaque JSON the
// store never inspects. The client decodes these into the public beehive.Event; the
// store-only ResourceVersion cursor stops at that boundary.
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
	ID    ObjectID
	Group string
	Kind  string
	// Slug is the object's name, unique within its GroupKind. Required, so never
	// empty on a row the store returns.
	Slug   string
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
	// ReconcileOwed is how many passes beehive owes this object (the
	// objects.reconcile_owed column); 0 means none. The reconciler reads it to decide
	// what to subtract after a successful pass. Like ResourceVersion it is
	// store-owned: reported on reads, moved only by ReconcileOwedIncrement and
	// ReconcileOwedDecrement. A new object owes nothing, which is why
	// ObjectsCreateInput has no counterpart field.
	ReconcileOwed int64
	Finalizers    []string
	Conditions    []Condition // assembled on reads; nil when the object has none
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// ReconcileLoad is everything one reconcile pass needs from its opening read. A
// struct rather than extra return values: this is the load path most likely to
// grow, and each addition would otherwise be a Store break of its own.
type ReconcileLoad struct {
	Object RawObject
	// Cursor is the store-wide write cursor as of the same statement that read
	// Object — the value to record on success. Reading it here rather than
	// separately saves a round trip on a pool of one connection, and is marginally
	// safer than two statements: it is at or below the true cursor when the
	// controller reads its dependencies, because those reads all happen after this
	// returns.
	Cursor int64
	// HasDependencies reports whether Object had an outgoing depends_on edge at
	// load. It exists only so a reconcile of an object with no dependencies can skip
	// DependencyWatermarksSet entirely and never take the write lock — see that
	// method, and the reconciler's skip rule.
	HasDependencies bool
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

// ObjectsCreateInput is everything ObjectsCreate accepts from its caller. It is
// deliberately not RawObject: that is the read shape, mirroring a whole row, and
// passing it here offered a caller eighteen fields of which the INSERT bound six —
// a seeded Status being the sharp case, discarded with no error. The rest of a new
// row is store-assigned (ID, ResourceVersion, the timestamps) or fixed (Generation
// starts at 1, ReconcileOwed at 0, Status NULL), so a narrow input lets the
// compiler refuse what the store would otherwise drop.
type ObjectsCreateInput struct {
	Finalizers []string
	// Slug is the object's name and is required: the uniqueness constraint is per
	// kind, and the column is NOT NULL. A value type rather than a pointer because
	// there is no unnamed object to represent — see ErrInvalidSlug for the empty
	// string, which the client rejects before it reaches here.
	Slug string
	Spec []byte
	// SpecVersion is the migrator schema version Spec was written at, stamped onto
	// the row like any other write. Status has no counterpart here: a new row has no
	// status to version.
	SpecVersion int
}

// EdgesAddResult is what a caller needs in order to follow up on an edge it
// declared. All of it falls out of work EdgesAdd already does, so none of it costs an
// extra query.
type EdgesAddResult struct {
	// From is the source object's GroupKind, projected from the endpoint check.
	// Edges are cross-kind, so a caller routing a requeue to fromID cannot assume
	// its own kind.
	From GroupKind
	// ReconcileOwedStamped reports whether this call incremented fromID's
	// reconcile_owed, which happens when the call created a new depends_on edge
	// (self-edges excluded). A caller can read it instead of working the condition
	// out again, and there is deliberately no second, separately derived report of
	// "was the edge new" for it to disagree with.
	ReconcileOwedStamped bool
}

// ObjectRef names one object: its id, plus the GroupKind needed to route a requeue or
// a GC step to it. It refers to an object rather than an edge, and carries no
// direction, so one shape serves both ends of an edge query — EdgesListIncoming's
// sources and EdgesListOutgoing's targets — as well as the DeletionRequests lists,
// where there is no edge at all and it just means "which object, and what kind".
type ObjectRef struct {
	ID    ObjectID
	Group string
	Kind  string
}

// GroupKind is the kind to route a requeue (or a GC step) to.
func (r ObjectRef) GroupKind() GroupKind {
	return GroupKind{Group: r.Group, Kind: r.Kind}
}

// Store is the durable-store contract beehive depends on internally. It has no type
// parameters and deals only in raw rows; the generic boundary is one layer up, in the
// typedController adapter.
//
// **A mutator returns a row exactly where a public Client write returns that object
// to the user**, which today is ObjectsCreate and ObjectsUpdateSpec and nothing else.
// Their callers could not otherwise show the store-assigned id, resource_version and
// timestamps without reading again. Apply that rule to a new mutator rather than
// copying whichever neighbour it resembles.
//
// Where a mutator does return a row, a nil error guarantees it is non-nil, so callers
// dereference it without checking; an implementation that returns (nil, nil) is
// broken, not a case to handle. That holds on ObjectsUpdateSpec's content no-op too,
// where the row is unchanged but still returned.
//
// Every other mutator returns only an error, plus a bool where whether the write
// landed is not otherwise derivable *and a caller reads it*. Returning a row nobody
// reads is not free here: assembling one attaches its conditions, so it costs an
// indexed query per write, and offering it invites a caller to trust a shape the
// store would rather narrow. Callers that want the post-write row read it back.
// Writes that report no row should answer from metadata alone — no blob, no
// conditions — since that is the saving, not the signature.
type Store interface {
	io.Closer

	// Within runs fn inside a single transaction, committing on a nil error and
	// rolling back otherwise. Store calls made with the ctx passed to fn join that
	// transaction.
	//
	// **Use that ctx for every store call fn makes.** A call with any other context
	// asks for a connection of its own, which on a single-connection backend (the
	// sqlite store pins the pool to one) is the connection this transaction is
	// holding — so it waits for a transaction that cannot commit until it returns,
	// and the deadlock ends only when its context is cancelled. That is a property of
	// the backend, not of this contract, but the contract is where a caller reads
	// about it: an implementation with a larger pool merely runs the call
	// concurrently, on a snapshot that does not include fn's uncommitted writes.
	//
	// **A nested Within is a rollback boundary.** A Within whose ctx already carries a
	// transaction joins it rather than opening another, and only the outermost one
	// commits — but an error returned from the nested fn must still unwind that fn's
	// writes and the AfterCommit hooks it queued, leaving the store as it was when the
	// nested call was entered. This holds whatever the outer caller then does with the
	// error, including swallowing it. Without that, no composition of two writes is
	// atomic except by the grace of its callers, and a backend cannot promise a caller
	// that a failed compound operation left nothing behind. The sqlite store implements
	// it with SAVEPOINT.
	//
	// A backend implementing the boundary with a savepoint stack must refuse a nested
	// Within entered from a goroutine other than the one owning the enclosing frame,
	// with ErrConcurrentNestedTx — concurrent pushes interleave, and serialising them
	// deadlocks as soon as fn waits on a goroutine that also wants the store. Ordinary
	// deep nesting on one goroutine is not that case and must be accepted.
	//
	// That refusal covers nested Withins and nothing else, which is the whole of the
	// guarantee: **a transaction ctx belongs to one goroutine.** A single-statement
	// call that joins the transaction without opening a frame of its own cannot be
	// detected this way, so a write issued from a second goroutine while a sibling
	// frame is open may simply be discarded by that frame's unwind. Do not share a
	// transaction ctx across goroutines; the refusal is a tripwire on the common case,
	// not a lock.
	//
	// The commit is the exception, and there the check is exact: a backend must refuse
	// to commit while any nested frame is still open. Nested frames unwind before fn
	// returns on a single goroutine, so a live one at that moment can only belong to
	// another, and committing would release its savepoint — landing writes it may be
	// about to roll back, with no way left to undo them.
	Within(ctx context.Context, fn func(ctx context.Context) error) error

	// AfterCommit registers fn to run once the transaction ctx belongs to has
	// committed. If ctx carries no transaction the write has already committed, so
	// fn runs before AfterCommit returns. Hooks run in registration order.
	//
	// The rule in one line: **fn runs if and only if the transaction it was registered
	// against committed, and the frame it was registered against did not unwind.** A
	// rolled-back transaction never runs its hooks — neither those queued in it, which
	// die with the queue, nor one registered afterwards on a ctx someone kept, which is
	// discarded. Registering against a nested frame that already unwound is discarded
	// the same way, even while the outer transaction is still open and heading for a
	// commit: that frame's writes are gone.
	//
	// The one case that runs inline is a registration arriving after a *successful*
	// commit — from a hook that passed back the ctx it captured, say. There is no queue
	// left to join and the commit it was owed to has happened, so "after the commit" is
	// now.
	//
	// It exists for one thing, and a backend author should judge their effort by that:
	// WithOnCreate, the guarantee that a create-conditional side effect never fires for
	// a row that rolled back. Beehive's own machinery registers nothing here, because
	// reconciles come from periodic passes over what the store records rather than from
	// hooks on the write. So a backend that gets the re-entrant case below subtly wrong
	// breaks that one option and nothing else.
	//
	// fn receives a context detached from the transaction, so a store call it makes
	// opens a fresh transaction instead of joining the committed one. Everything
	// else — deadline, cancellation, values — is inherited, so a hook is still bound
	// to the caller's lifetime.
	//
	// Registering from inside a running hook is allowed and runs the new hook
	// immediately: its transaction has already committed, so "after the commit" is
	// now. This holds even when the hook passes back the transaction context it
	// captured rather than the detached one it was handed.
	//
	// fn must not panic: hooks run in sequence, so a panic aborts the ones
	// registered after it while the transaction stays committed. Nothing recovers
	// here — a panicking hook is a bug in the layer that registered it, and this
	// library lets panics surface (Reconcile is not recovered either).
	AfterCommit(ctx context.Context, fn func(ctx context.Context))

	// ConditionsDelete removes the condition of type condType from id. Removing an
	// existing condition bumps ResourceVersion; deleting one that isn't there does
	// nothing. Scoped to gk: another kind's id is rejected with ErrWrongKind, a
	// missing id with ErrNotFound. It returns no row: see the note above Store.
	ConditionsDelete(ctx context.Context, gk GroupKind, id ObjectID, condType string) error

	// ConditionsSet inserts or updates the condition keyed by (id, cond.Type). A real
	// change bumps the object's ResourceVersion; an identical write does nothing.
	// Scoped to gk: another kind's id is rejected with ErrWrongKind, a missing id is
	// rejected with ErrNotFound. It returns no row: see the note above Store, and
	// read the conditions back with ObjectsGet if you need them.
	ConditionsSet(ctx context.Context, gk GroupKind, id ObjectID, cond Condition) error

	// DeletionRequestsCreate requests an object's deletion by setting
	// DeletionRequestedAt. The row stays until its finalizers clear, so this creates
	// the request, never the deletion itself — ObjectsDelete does that. changed is true
	// only when this call set the flag; repeat calls do nothing and return false.
	// Scoped to gk: another kind's id is rejected with ErrWrongKind, a missing id with
	// ErrNotFound. It returns no row: see the note above Store.
	DeletionRequestsCreate(ctx context.Context, gk GroupKind, id ObjectID) (changed bool, err error)

	// DeletionRequestsCreateBySlug is DeletionRequestsCreate keyed by slug within gk.
	// The slug goes into the write's own WHERE clause, so resolving and marking are one
	// statement rather than a lookup and a write wrapped in a transaction. Both are
	// race-free; this one saves a round trip, which a single-connection store notices.
	//
	// Everything else matches DeletionRequestsCreate: changed is true only when this
	// call set the flag, a repeat does nothing, and ErrNotFound means no object of gk
	// holds the slug. There is no ErrWrongKind here, because slugs are unique per kind,
	// so another kind's slug is simply not found. It returns no row either; a caller
	// that needs the marked object's id resolves the slug itself.
	DeletionRequestsCreateBySlug(ctx context.Context, gk GroupKind, slug string) (changed bool, err error)

	// DeletionRequestsCreateFromOwner is the GC cascade as one command: it requests
	// deletion of every object owned by ownerID and returns them all. It writes only to
	// children that are not already deletion-pending, so cascading again over a subtree
	// that is already deleting costs a single read rather than a write per child per
	// sweep.
	DeletionRequestsCreateFromOwner(ctx context.Context, ownerID ObjectID) ([]ObjectRef, error)

	// DeletionRequestsList returns every deletion-pending object of every kind, each
	// with its GroupKind beside its id. The global GC sweeper is the only caller and
	// needs the kind in order to route: an object of a registered kind is queued so its
	// controller can clear finalizers, which gcCollect cannot do, while a client-only
	// kind is collected directly. Nothing else reaches a client-only kind, and left
	// alone it would strand and RESTRICT-block its owner's delete forever.
	DeletionRequestsList(ctx context.Context) ([]ObjectRef, error)

	// EventsAdd records an observation about id in the (id, ev.Category) timeline,
	// grouping consecutive ones into runs. Adding does not always insert: if the
	// latest run there has the same (Type, Reason) it is extended instead — Count
	// goes up, LastAt moves, Message and Detail are re-sampled. Otherwise a new run
	// is appended with Count 1.
	//
	// Only ev's Category, Type, Reason, Message and Detail are read; the store fills in
	// the rest and returns the run. The comparison is scoped to (id, Category), so an
	// event in another category can't break this run. Scoped to gk: another kind's id
	// gives ErrWrongKind, a missing id ErrNotFound.
	EventsAdd(ctx context.Context, gk GroupKind, id ObjectID, ev Event) (*Event, error)

	// EventsGetLatest returns the most recent run in id's category timeline, or nil
	// if that timeline has no events. Reads by object id only (not kind-scoped).
	EventsGetLatest(ctx context.Context, id ObjectID, category string) (*Event, error)

	// EventsList returns id's event runs matching q, newest first (by LastAt, then
	// id). The zero EventQuery returns every run for the object. Reads by object id
	// only — not kind-scoped, like the ref-list reads.
	EventsList(ctx context.Context, id ObjectID, q EventQuery) ([]Event, error)

	// EventsSweep trims the event log to the retention bounds and returns how many runs
	// it deleted. perObject > 0 caps each (object, category) timeline to its newest
	// perObject runs, per timeline so a flapping one can't evict a quiet one. maxAge > 0
	// drops runs whose LastAt is older than that. A zero bound is skipped. Retention is
	// global rather than per-kind, so the GC sweeper calls this once per pass.
	EventsSweep(ctx context.Context, perObject int, maxAge time.Duration) (int, error)

	// FinalizersDelete removes finalizer from id's finalizer list. Removing one that is
	// there bumps ResourceVersion; removing one that isn't does nothing. ErrNotFound if
	// the row is gone. Scoped to gk: another kind's id is rejected with ErrWrongKind.
	// It returns no row: see the note above Store.
	FinalizersDelete(ctx context.Context, gk GroupKind, id ObjectID, finalizer string) error

	// ObjectsCreate inserts a new object of kind gk. The store assigns ID and
	// ResourceVersion and sets Generation to 1; ObjectsCreateInput carries
	// everything else create accepts, and nothing it doesn't.
	ObjectsCreate(ctx context.Context, gk GroupKind, in ObjectsCreateInput) (*RawObject, error)

	// ObjectsDelete removes the row outright. Callers must ensure finalizers are
	// empty first; this is the physical delete the GC path performs.
	ObjectsDelete(ctx context.Context, id ObjectID) error

	// ObjectsGet loads an object by id, or returns ErrNotFound.
	ObjectsGet(ctx context.Context, id ObjectID) (*RawObject, error)

	// ObjectsGetBySlug loads the object with the given slug within gk, or returns
	// ErrNotFound.
	ObjectsGetBySlug(ctx context.Context, gk GroupKind, slug string) (*RawObject, error)

	// ObjectsGetForReconcile is the reconcile loop's opening read: the object (with
	// its conditions, as ObjectsGet returns it), the store-wide write cursor as of the
	// same statement, and whether the object has dependencies. Both extra values ride
	// the statement that reads the row rather than adding round trips. Returns
	// ErrNotFound like ObjectsGet when the row is gone.
	ObjectsGetForReconcile(ctx context.Context, id ObjectID) (ReconcileLoad, error)

	// ObjectsGetMeta is ObjectsGet without the conditions query, so the returned
	// Conditions is always nil. Callers that only need metadata — GC, edge bookkeeping
	// — use it to skip that read. Returns ErrNotFound if no object matches.
	ObjectsGetMeta(ctx context.Context, id ObjectID) (*RawObject, error)

	// ObjectsList returns every object of kind gk, ordered by id.
	ObjectsList(ctx context.Context, gk GroupKind) ([]*RawObject, error)

	// ObjectsListByIncomingEdge is EdgesListIncoming with the rows attached and scoped
	// to one kind: the full objects of kind gk that point at toID through relation,
	// ordered by id, with conditions. Edges and rows resolve in one query, so reading an
	// owner's children of one kind (Client.OwnedObjectsList) costs no Get per child.
	// Other kinds are filtered out, and a toID with no matching edge reads empty rather
	// than ErrNotFound.
	ObjectsListByIncomingEdge(ctx context.Context, gk GroupKind, toID ObjectID, relation Relation) ([]*RawObject, error)

	// ObjectsListIDs returns the ids of every object of kind gk, ordered by id. The
	// reconciler uses it for a full pass at startup, so state belonging to this process
	// — liveness conditions, say — is re-confirmed even on objects whose spec has
	// already settled.
	ObjectsListIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error)

	// ObjectsListUnsettledIDs returns the IDs of objects of kind gk whose
	// observed_generation doesn't match generation (not yet converged).
	ObjectsListUnsettledIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error)

	// ObjectsUpdateSpec replaces an object's spec, bumping Generation and
	// ResourceVersion, and stamps specVersion — the migrator schema version the bytes
	// were written at.
	//
	// Writing spec bytes identical to the stored ones *at the row's own schema version*
	// does nothing: no Generation or ResourceVersion bump, so a converged object is not
	// falsely unsettled. The version qualifier matters. Bytes written at a different
	// schema version are in a different shape, so comparing them says nothing about
	// whether the value changed, and such a write takes the normal path.
	//
	// Which of the two happened is not reported: a caller who needs to know compares
	// Generation against one it read first.
	//
	// Scoped to gk: another kind's id is rejected with ErrWrongKind, a missing id with
	// ErrNotFound.
	ObjectsUpdateSpec(ctx context.Context, gk GroupKind, id ObjectID, spec []byte, specVersion int) (*RawObject, error)

	// ObjectsUpdateSpecBySlug is ObjectsUpdateSpec keyed by slug within gk: it
	// writes whatever holds the slug now, or returns ErrNotFound. A slug this kind
	// does not hold is absent rather than foreign, so there is no ErrWrongKind — as
	// with DeletionRequestsCreateBySlug.
	//
	// Everything else matches ObjectsUpdateSpec, the content no-op included. An
	// implementation MUST resolve and write within one transaction: the no-op skip
	// needs the stored bytes to compare against, so a resolve-then-write split
	// across two calls would let a concurrent collect hand the slug to a
	// replacement in between.
	ObjectsUpdateSpecBySlug(ctx context.Context, gk GroupKind, slug string, spec []byte, specVersion int) (*RawObject, error)

	// ObjectsUpdateStatus replaces an object's status, records the generation the
	// controller observed, and stamps statusVersion (the migrator schema version
	// the status bytes were written at). When the bytes differ from the stored
	// ones it bumps ObservedAt, ResourceVersion and UpdatedAt.
	//
	// Status bytes identical to the stored ones *at the row's own schema version*
	// write no status and never touch UpdatedAt. The version qualifier is
	// load-bearing: bytes written at a different schema version are in a different
	// shape, so comparing them says nothing about whether the value changed, and
	// such a write takes the normal path above. Two things still ride the no-op
	// path:
	//
	//   - ObservedGeneration and ObservedAt advance if this reconcile settled a
	//     generation the object had not settled at before. Otherwise a reconcile that
	//     changed no status would strand both the handshake and the unsettled
	//     listing that keys off it. That advance is a real transition, so it does
	//     bump ResourceVersion, and a watcher waiting for ObservedGeneration ==
	//     Generation sees the object converge. It happens at most once per generation.
	//   - Nothing else. A call identical in both respects writes nothing at all, not
	//     even a ResourceVersion bump. So ObservedAt records when
	//     ObservedGeneration was recorded, and does not move per reconcile: it is a
	//     handshake timestamp, not a liveness heartbeat.
	//
	// The two paths handle a stale observedGeneration — one at or below the recorded
	// value — differently, on purpose. The no-op path ignores it: with identical bytes
	// it says strictly less than what is already stored, so recording it would only
	// un-converge a settled object for nothing. The content-changed path writes it as
	// given, rolling ObservedGeneration back so the object reads as unsettled. The
	// reporter just overwrote the status with content derived from an older spec, and
	// being unsettled is what makes a later pass re-derive it; marking stale status as
	// converged would leave nothing to revisit it.
	//
	// Scoped to gk: an id of another kind is rejected with ErrWrongKind, a missing
	// id with ErrNotFound. An observedGeneration greater than the row's current
	// generation is rejected with ErrObservedGenerationFuture, no-op or not.
	//
	// It returns no row: see the note above Store on which mutators do.
	ObjectsUpdateStatus(ctx context.Context, gk GroupKind, id ObjectID, observedGeneration int64, status []byte, statusVersion int) error

	// EdgesAdd inserts a directed (fromID -> toID) edge with the given relation. It is
	// idempotent, and both endpoints must exist or it returns ErrNotFound. The edge is
	// not part of the object, so it bumps no version.
	//
	// Every **new depends_on** edge this call creates (self-edges excluded)
	// increments fromID's reconcile_owed — the durable record that a wake is owed —
	// and reports it as EdgesAddResult.ReconcileOwedStamped. The stamp is
	// unconditional because it is the only mechanism sound under every
	// interleaving: an increment landing while fromID's own reconcile is in flight
	// sits above the count that pass observed at load, so it survives the decrement
	// and keeps the object owed — where invalidating derived state (the watermark
	// clear below) is undone by that same pass. The edge-new gate bounds the cost at
	// one extra pass per edge ever created; a caller that deletes and re-declares its
	// set every pass pays per re-create, which is the trade recorded in the ADR.
	// The stamp must land *before* the insert: issued as a separate call after
	// EdgesAdd returned, a caller sharing an ambient transaction could handle its
	// error and commit the edge with no wake — a stranded dependent. So the endpoint
	// check, the stamp and the insert are one atomic unit, and an implementation
	// must not split them.
	//
	// The count holds durable, owed *work*, and nothing else writes to it. A second
	// kind of durable marker — undecodable rows, say — gets its own column and its own
	// cadence rather than joining this count.
	//
	// Creating a new depends_on edge also clears fromID's dependency_watermarks row,
	// on the same side of the insert as the other writes and behind the same
	// edge-new gate, skipping self-edges (which DependentsListStale excludes
	// anyway). A watermark describes how much of the store a dependent has
	// reconciled against, and it was recorded when the dependency set was smaller,
	// so it cannot speak for a target just added — one whose resource_version may
	// sit below it, where DependentsListStale would read converged. The stamp above
	// is what guarantees the dependent a pass; the clear keeps the derived state
	// honest for the window until that pass runs, and an absent row already means
	// stale, so nothing new is recorded.
	//
	// The stamp does not depend on fromID's kind having a controller. The store cannot
	// know which kinds do, and gating would cost the caller a pre-read of fromID's kind
	// on every declare. A kind with no reconcile loop never drains its count, and
	// nothing scans it either, since ReconcileOwedListIDs is per-kind — so the count
	// goes unread, though it is still a lasting column value and index entry, and
	// re-declaring an edge raises it again. Reclaiming those wants a cross-kind sweeper
	// (see docs/TODO.md), not a gate here: refusing to stamp would lose the wake outright
	// for a kind that gains a controller later.
	EdgesAdd(ctx context.Context, fromID, toID ObjectID, relation Relation) (EdgesAddResult, error)

	// EdgesDelete removes the (fromID, toID, relation) edge; removing one that isn't
	// there does nothing. Like EdgesAdd it bumps no version.
	EdgesDelete(ctx context.Context, fromID, toID ObjectID, relation Relation) error

	// EdgesDeleteFinalizingDependsOn removes the depends_on edges pointing at toID
	// whose source is itself marked for deletion. A dependent that is going away must
	// not keep its target alive: without this, two deletion-pending objects that depend
	// on each other — or one that depends on itself — would each hold the other's
	// RESTRICT and never be collected. owned_by edges are left alone, since those clear
	// only when the child is physically removed.
	EdgesDeleteFinalizingDependsOn(ctx context.Context, toID ObjectID) error

	// EdgesGroupIncomingByID is the batched form of EdgesListIncoming: the sources
	// pointing at many targets through relation, grouped by target id. It is the
	// incoming twin of EdgesGroupOutgoingByID, and is what lets a List eager-load
	// dependents without a query per object. A target with no sources is absent from
	// the map.
	EdgesGroupIncomingByID(ctx context.Context, toIDs []ObjectID, relation Relation) (map[ObjectID][]ObjectRef, error)

	// EdgesGroupOutgoingByID is the batched form of EdgesListOutgoingByRelation: the
	// relation's targets for many sources, grouped by source id. A source with no
	// matching edge is absent from the map rather than present with an empty slice, so
	// eager loading over a List needs no query per object. An implementation may split
	// a large id list across several queries; the grouped result is the same.
	EdgesGroupOutgoingByID(ctx context.Context, fromIDs []ObjectID, relation Relation) (map[ObjectID][]ObjectRef, error)

	// EdgesHasIncoming reports whether anything with a live claim points at id: an
	// owned_by edge, or a depends_on edge from a source that is not itself finalizing.
	//
	// A depends_on edge from a deletion-pending source is ignored, because that
	// dependent is going away and no longer has a claim. If it still counted, two
	// mutually dependent finalizing objects would never see this go false. owned_by
	// always counts, since the cascade has to wait for the child to be physically
	// removed. GC pairs this with EdgesDeleteFinalizingDependsOn, which removes the
	// ignored edges before ObjectsDelete so the RESTRICT is satisfied.
	EdgesHasIncoming(ctx context.Context, id ObjectID) (bool, error)

	// EdgesListIncoming returns every object pointing at toID through relation, ordered by
	// id (e.g. the dependents to requeue, or the owned children to GC).
	EdgesListIncoming(ctx context.Context, toID ObjectID, relation Relation) ([]ObjectRef, error)

	// EdgesListOutgoingByRelation returns the objects fromID points at through
	// exactly relation, ordered by id (e.g. a child's owner via RelationOwnedBy, or
	// its dependencies via RelationDependsOn) — the inverse of EdgesListIncoming.
	EdgesListOutgoingByRelation(ctx context.Context, fromID ObjectID, relation Relation) ([]ObjectRef, error)

	// There is deliberately no standalone increment here. EdgesAdd produces them —
	// its stamp must be inseparable from the edge insert, so it issues one itself and
	// reports it as EdgesAddResult.ReconcileOwedStamped — and ReconcileOwedDecrement
	// consumes them. An increment on this interface would be API the declare path
	// could not use correctly, since it cannot be made atomic with the edge, and that
	// nothing else uses. Leaving it off makes "the stamp rides EdgesAdd" true at
	// compile time rather than something a test has to police. Add it when a producer
	// other than EdgesAdd exists.

	// ReconcileOwedDecrement subtracts observed from id's reconcile_owed, floored at 0,
	// recording that a reconcile handled every wake it saw. Callers pass the count they
	// loaded rather than 1: one pass reads the target's current state, which answers
	// every wake outstanding when it started, so subtracting 1 would leave a remainder
	// with nothing to re-queue it. Increments landing *after* that load sit above
	// observed, so they survive the subtraction and keep the object owed. Bumps no
	// resource_version.
	//
	// Scoped to gk, like every other id-keyed mutator: another kind's id is rejected
	// with ErrWrongKind rather than silently draining a wake that kind was owed. A
	// row that is simply gone is not an error — a reconcile's target can be collected
	// between its load and this write, and nothing is left to owe a pass to.
	ReconcileOwedDecrement(ctx context.Context, gk GroupKind, id ObjectID, observed int64) error

	// ReconcileOwedListIDs returns the ids of objects of kind gk that are owed a
	// dependency wake (reconcile_owed != 0). The owed pass queues these, so a wake
	// recorded before a crash is serviced on restart even though no spec changed. It is
	// separate from ObjectsListUnsettledIDs, because an object can be settled on its
	// spec and still owe a wake.
	ReconcileOwedListIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error)

	// DependencyWatermarksSet records cursor as the store-wide write cursor id's
	// reconcile observed, for the staleness scan to compare targets against. Upserts,
	// since the row appears on a dependent's first successful pass and is raised on
	// later ones. Bumps no resource_version — it writes no objects row at all, so a
	// recorded reconcile cannot put the object back into the waker's own scan and wake
	// every dependent of an object whose only change was that record.
	//
	// The stored cursor never decreases, and a pass that advances nothing writes
	// nothing: the upsert rejects a non-advancing value outright. That makes the write
	// both order-independent — free insurance today, since the work queue serialises
	// passes per id — and free of charge on a pass that observed no new store state,
	// which is the common case for a polling controller in a quiet store.
	//
	// It also sets a reconciled-at timestamp under the same predicate, so the timestamp
	// cannot move without the cursor. That timestamp is observability only; nothing
	// reads it to decide anything, and it is not a reconcile heartbeat.
	//
	// The write gates on id having at least one outgoing depends_on edge. An object
	// with no dependencies can never be found stale, so a row for it is dead weight the
	// scan would probe forever. The gate is also what keeps the write safe when the
	// object is collected mid-pass: its edges cascade away with it, so the gate closes
	// and nothing is written, rather than the write failing against a missing row.
	//
	// Advancing the cursor asserts that this pass observed its dependencies' state as
	// of cursor. The store cannot check that, and one caller shape breaks it: a
	// controller that swallows a failed read of a dependency and returns nil advances
	// past a change it never saw. That is the same unsafe shape as sampling the cursor
	// after the reconcile rather than before, and it is the one way this mechanism can
	// strand a dependent.
	//
	// This is not the only writer of the row: EdgesAdd *clears* it when it creates a
	// new depends_on edge, because a cursor recorded over a smaller dependency set
	// cannot speak for a target just added. The reconcile the same call's
	// reconcile_owed stamp guarantees is what closes the interleaving the clear alone
	// left open — a pass in flight rewriting the cleared row from a cursor that never
	// saw the new target.
	DependencyWatermarksSet(ctx context.Context, id ObjectID, cursor int64) error

	// DependentsListStale returns objects of the given kinds that have a depends_on
	// edge to a target whose resource_version is above their dependency watermark —
	// everything owed a dependency reconcile as of now, re-derived rather than
	// recorded. Ordered by id and paged from afterID (pass 0 to start), at most limit
	// rows. An empty kinds slice returns nothing.
	//
	// Filtered by kind for the same reason ReconcileOwedListIDs is: only a registered
	// kind has a reconcile loop to enqueue into. Filtering here rather than in the
	// driver is what keeps a client-only dependent — which never gets a watermark
	// written and is therefore stale forever — from being re-scanned, re-joined and
	// re-paged on every pass for the life of the row. A kind that gains a controller in
	// a later build appears in the list and is found on the next pass, so nothing is
	// lost.
	//
	// The kind filter applies to the DEPENDENT and must never be extended to the
	// TARGET. A registered object may depend on a client-only one — that is the whole
	// reason the waker's scan is store-wide rather than per-kind — so a target's kind
	// is irrelevant to whether its dependents are owed a pass. Narrowing the scan to
	// edges whose *both* endpoints are registered looks like a free optimisation and
	// would silently strand every registered dependent of a client-only target, which
	// is the exact failure class this mechanism exists to remove.
	//
	// A missing watermark counts as stale: an object that has never reconciled against
	// a known point cannot have converged, mirroring how the unsettled index treats a
	// NULL observed_generation.
	//
	// Self-edges are excluded. An object that depends on itself bumps its own
	// resource_version whenever its reconcile writes, which would leave it stale
	// against itself for an extra pass every time it does any work — the same reason
	// the waker skips them.
	DependentsListStale(ctx context.Context, kinds []GroupKind, afterID ObjectID, limit int) ([]ObjectRef, error)

	// ObjectWritesListSince returns the live writes above afterRV in cursor order, at
	// most limit of them. It carries no blobs and spans every kind, for a consumer
	// advancing a watermark over the store's write log. A row gives an id and the
	// version it now holds, never how it got there, since the consumer re-reads current
	// state anyway. A row deleted since afterRV is simply absent rather than an error,
	// so a scan reports what exists and never a tombstone.
	//
	// This is the whole change-notification surface; there is no live stream. A
	// consumer polls on its own interval, and the cursor is what keeps that lossless:
	// resource_version always increases and is never reused, so "everything above the
	// watermark" is exactly what has happened since the last scan, however long ago
	// that was.
	ObjectWritesListSince(ctx context.Context, afterRV int64, limit int) ([]ObjectWrite, error)

	// ObjectWritesMaxVersion returns the high-water mark of the object write log as of
	// now: every object write committed before the call is at or below it, and
	// ObjectWritesListSince returns nothing above it. A consumer that wants to start
	// from "changes from here on" seeds its watermark with this rather than replaying
	// the whole log from zero, and one polling for change compares against it.
	//
	// It is the maximum over live rows, not the version counter, so it is *not*
	// monotonic: deleting the highest-versioned row lowers it. That is sound for both
	// uses — nothing exists at the versions it steps back over, so a seed cannot skip
	// a live write, and a poller that re-reads because the mark moved backwards finds
	// exactly the delete that moved it. It also means a write to some *other* log
	// drawing from the same counter (events do) cannot masquerade here as an object
	// change.
	ObjectWritesMaxVersion(ctx context.Context) (int64, error)
}

// FreePagesReleaser is an optional Store capability: a backend that can hand space
// freed by deleted rows back to the operating system implements it, and the GC
// sweeper calls it after collecting rows and trimming the event log — the two things
// that produce the free space. A Store that does not implement it is simply not
// drained, so this is deliberately NOT a member of Store: reclaiming pages is one
// backend's concern (SQLite's auto_vacuum), and putting it in the contract would
// make every implementation, and every test double, answer a question only that
// backend has.
//
// Implementations may release fewer pages than asked, including none at all — a
// backend is free to decide a small amount of free space is worth more kept than
// returned, since it is space the next writes would have reused without growing the
// file. The returned count is therefore a report, not a guarantee, and is advisory
// besides: it may be measured across concurrent writes. Callers log it.
type FreePagesReleaser interface {
	// FreePagesRelease releases up to maxPages of reclaimable space and returns how
	// many pages were actually released. A non-positive maxPages releases nothing.
	FreePagesRelease(ctx context.Context, maxPages int) (int, error)
}

// DriverCursorer is an optional Store capability: a backend that can persist a
// periodic driver's scan position implements it, and a driver that has one reads
// it at startup to resume where it stopped rather than reseeding from "now". A
// Store that does not implement it is simply not resumed across restarts — the
// dependency waker's original, tested behaviour — so this is deliberately NOT a
// member of Store, for the same reason FreePagesReleaser is not: it is a latency
// optimisation over a mechanism (the stale-dependents pass) that is already
// guaranteed, and every implementation and test double would otherwise have to
// answer a question only some of them need to.
type DriverCursorer interface {
	// DriverCursorsGet returns the cursor last persisted for name, or ok=false if
	// none has been yet. Absence is the normal state on every fresh database, not
	// an error — there is no ErrNotFound here because a driver cursor is not an
	// object, and zero is a legitimate cursor value on an empty store, so it
	// cannot double as "no cursor".
	DriverCursorsGet(ctx context.Context, name string) (cursor int64, ok bool, err error)

	// DriverCursorsSet persists cursor for name if it is greater than what is
	// stored, and otherwise writes nothing — a call that does not advance the
	// cursor must cost no write, since a driver may call this every tick.
	DriverCursorsSet(ctx context.Context, name string, cursor int64) error
}
