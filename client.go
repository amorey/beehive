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
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/amorey/beehive/internal/storeapi"
	"github.com/google/uuid"
)

// ErrNoController is returned by Requeue when the client's kind has no
// registered controller: there is no reconcile loop to schedule against.
var ErrNoController = errors.New("beehive: no controller registered for kind")

// ErrWatchTooOld ends a watch whose unread log entries retention has already
// removed. The stream cannot continue truthfully, so it reports this on a Failed
// change and closes; the caller answers by subscribing again for a fresh
// snapshot.
var ErrWatchTooOld = errors.New("beehive: watch is below the write log's retention horizon")

// GenerateName returns prefix joined to a fresh UUIDv7, for callers whose
// objects have no natural name:
//
//	obj, err := client.Create(ctx, beehive.GenerateName("cache"), spec)
//
// UUIDv7 leads with a millisecond timestamp, so names sharing a prefix sort by
// creation time, and a monotonic counter makes same-process names distinct by
// construction. Collision-resistant, not collision-proof: Create reports
// ErrNameTaken, and a caller generating names should bound-retry on exactly
// that sentinel:
//
//	for range 3 {
//		obj, err := client.Create(ctx, beehive.GenerateName("cache"), spec)
//		if !errors.Is(err, beehive.ErrNameTaken) {
//			return obj, err
//		}
//	}
//
// newUUIDv7 is a seam so tests can exercise the otherwise-unreachable panic.
var newUUIDv7 = uuid.NewV7FromReader

func GenerateName(prefix string) string {
	// crypto/rand.Read never returns an error (it crashes on an unusable OS
	// source), so drawing the bytes here makes the whole path error-free.
	var b [16]byte
	rand.Read(b[:])

	// Routed through uuid so the version/variant nibbles and the monotonic
	// counter are stamped. The error is unreachable, but a dropped error would
	// yield uuid.Nil — every name collapsing to one constant — so it panics
	// with its cause instead.
	id, err := newUUIDv7(bytes.NewReader(b[:]))
	if err != nil {
		panic("beehive: unreachable: UUIDv7 from a 16-byte reader: " + err.Error())
	}
	return prefix + "-" + id.String()
}

// checkName rejects "" before any store work, so the error does not depend on
// whether a row happens to exist. The store refuses "" as well; this is the
// courtesy that keeps reads from answering ErrNotFound for a bad argument.
func checkName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: pass the name the object should be addressable by", ErrInvalidName)
	}
	return nil
}

// Snapshot is a watch's starting state: the objects as they were, and the log
// position they are complete as of. The stream that comes with it carries
// changes strictly above that position.
type Snapshot[Spec, Status any] struct {
	Objects         []*Object[Spec, Status]
	ResourceVersion int64
}

// ObjectChange reports a change to a watched object. On a Deleted change,
// Object carries the row's final state. On a Failed change, Object is nil and
// Err is non-nil: the stream is over, and a Failed change is always the last
// value before the channel closes. A channel that closes with no Failed change
// ended because the caller's context did.
type ObjectChange[Spec, Status any] struct {
	Type   ChangeType
	Object *Object[Spec, Status]
	Err    error
}

// Client is the user-facing API for a single resource kind: creating, reading,
// updating, deleting, and watching objects.
type Client[Spec, Status any] interface {
	// Create inserts a new object under name, which is required and immutable.
	// A name already held by a live or deletion-pending row fails with
	// ErrNameTaken — Create never writes to a row it found; use GetOrCreate
	// when "already there" is acceptable. The new object is unsettled and owed
	// its first reconcile.
	Create(ctx context.Context, name string, spec Spec, opts ...Option) (*Object[Spec, Status], error)
	// Delete soft-deletes whatever holds name now by setting
	// DeletionRequestedAt; the GC sweeper takes it from there. Idempotent: a
	// missing name and an already-pending row both return nil. Kind-scoped.
	Delete(ctx context.Context, name string) error
	// DeleteByID is Delete keyed by incarnation: it acts on that one row, or
	// returns ErrNotFound — an id naming no object was collected out from under
	// the caller, which is worth hearing about.
	DeleteByID(ctx context.Context, id ObjectID) error
	// DependenciesList returns the objects id depends on (outgoing depends_on).
	// The lazy counterpart to LoadDependencies().
	DependenciesList(ctx context.Context, id ObjectID) ([]ObjectRef, error)
	// DependentsList returns the objects that depend on id (incoming
	// depends_on). The lazy counterpart to LoadDependents().
	DependentsList(ctx context.Context, id ObjectID) ([]ObjectRef, error)
	// EventsGetLatest returns the current run in id's category timeline. ok is
	// false (with a nil error) when the timeline is empty.
	EventsGetLatest(ctx context.Context, id ObjectID, category string) (Event, bool, error)

	// EventsList returns id's event-log runs, newest-first, filtered by opts.
	// Reads by id, not kind-scoped. An empty log is an empty slice.
	EventsList(ctx context.Context, id ObjectID, opts ...EventOption) ([]Event, error)
	// EventsWatch streams id's event log: the runs matching opts, then whatever
	// the log grows by, until ctx is cancelled. Requires a registered
	// controller and polls, so a run extended several times within one interval
	// is delivered once, carrying its latest state.
	EventsWatch(ctx context.Context, id ObjectID, opts ...EventOption) (<-chan Event, error)
	// Get loads whatever holds name now, or returns ErrNotFound. Kind-scoped:
	// another kind's row holding the same name is not found.
	Get(ctx context.Context, name string, loads ...LoadOption) (*Object[Spec, Status], error)
	// GetByID is Get keyed by incarnation — the read half of a
	// read-modify-write, whose write half is UpdateByID.
	GetByID(ctx context.Context, id ObjectID, loads ...LoadOption) (*Object[Spec, Status], error)
	// GetOrCreate returns the object with the given name, creating it from spec
	// if absent. It NEVER mutates an existing row: a name held by a live or
	// deletion-pending row is returned as-is with created=false, options
	// ignored — do not read created=false as "exists and matches opts". The
	// read-or-create is atomic, so concurrent creates can't both win.
	//
	// There is no name-keyed upsert: to change an existing row, follow with
	// Update. spec and opts are validated up front even when the row exists, so
	// a caller bug fails regardless of store state.
	//
	// The created bool is synchronous: inside a caller's Within it is set
	// before the transaction commits, so route create-conditional side effects
	// through WithOnCreate instead. A non-nil err means nothing was created —
	// a new row that fails to decode rolls back rather than committing bytes
	// the process can't read.
	GetOrCreate(ctx context.Context, name string, spec Spec, opts ...Option) (*Object[Spec, Status], bool, error)
	List(ctx context.Context, loads ...LoadOption) ([]*Object[Spec, Status], error)
	// ObjectsWatch returns one object's current state, ObjectsWatchList every
	// object of this client's kind, each with a stream of the changes above it:
	// Added/Modified/Deleted until ctx is cancelled. Both require a registered
	// controller and are kind-scoped.
	//
	// The snapshot is read before either returns, on the caller's goroutine, so a
	// caller may subscribe and then act: a change it makes afterwards — including
	// a delete — is always in the stream. Snapshot.ResourceVersion is the log
	// position the snapshot is complete as of, and the stream carries changes
	// strictly above it: no overlap, no gap. A failed snapshot read is returned
	// rather than handed back as a stream whose guarantee is void.
	//
	// Everything after is polled, which bounds latency and collapses changes
	// within one interval. A watch cannot be opened inside a transaction (the
	// read would deadlock on the single connection).
	ObjectsWatch(ctx context.Context, id ObjectID) (Snapshot[Spec, Status], <-chan ObjectChange[Spec, Status], error)
	ObjectsWatchList(ctx context.Context) (Snapshot[Spec, Status], <-chan ObjectChange[Spec, Status], error)
	// OwnedList returns the objects id owns (its incoming owned_by edges). The
	// lazy counterpart to LoadOwned().
	OwnedList(ctx context.Context, id ObjectID) ([]ObjectRef, error)

	// OwnedObjectsList returns the objects owned by ownerID that belong to THIS
	// client's kind, fully decoded — the typed, kind-scoped form of OwnedList,
	// resolved in one query instead of a Get per child. ownerID is typically
	// another kind and is not existence-checked: no children, or no such owner,
	// both read empty. Deletion-pending children are included; undecodable rows
	// are quarantined and logged, as in List. Takes the same LoadOptions as
	// List.
	OwnedObjectsList(ctx context.Context, ownerID ObjectID, loads ...LoadOption) ([]*Object[Spec, Status], error)

	// OwnersGet returns id's owner, if it has one; ok is false with a nil error
	// when it has none. The lazy counterpart to LoadOwner().
	//
	// This and DependenciesList/DependentsList/OwnedList run their edge query
	// directly with no kind check: a foreign id reads that kind's edges and a
	// missing id reads empty — neither returns ErrNotFound. Use them for ids
	// this client owns.
	OwnersGet(ctx context.Context, id ObjectID) (ObjectRef, bool, error)

	// Requeue queues id for reconcile now. A latency hint, not a synchronous
	// run: correctness rests on the periodic drivers. By default it keeps id's
	// retry backoff — a requeue almost never proves a failure is over — pass
	// WithResetBackoff() when it is. Returns ErrNotFound if id does not exist,
	// ErrNoController if the kind has no controller.
	Requeue(ctx context.Context, id ObjectID, opts ...RequeueOption) error
	// SchedulesGet reports when the reconcile loop has scheduled id to be
	// requeued (a pending backoff or RequeueAfter delay; for a queued id, the
	// moment it became due), or the zero Schedule when nothing is scheduled. A
	// non-blocking read of in-memory state: a missing or foreign id, and a
	// client-only kind, all read as the zero Schedule; the error is never
	// returned today.
	//
	// This is the next *scheduled* requeue, not a prediction: the periodic
	// passes and dependency wakes never appear here, so the real next reconcile
	// may be sooner. Treat it as observability; use SchedulesWatch to follow it
	// live.
	SchedulesGet(ctx context.Context, id ObjectID) (Schedule, error)
	// SchedulesWatch streams id's schedule as a gauge: the current value, then
	// a new Schedule whenever it changes. Unlike the other watches it reports
	// in-memory state as the work queue moves it — no polling, emits only on
	// change. The channel closes when ctx is cancelled OR when the Beehive
	// stops (after delivering the final schedule); a reader cannot tell the two
	// apart. A client-only kind returns ErrNoController; id need not exist.
	SchedulesWatch(ctx context.Context, id ObjectID) (<-chan Schedule, error)
	// Update replaces the spec of whatever holds name now, or returns
	// ErrNotFound (a missing row is not "already in the desired state"). A spec
	// whose bytes match what is stored writes nothing at all, so a controller
	// re-applying its own spec does not wake itself forever. For a
	// read-modify-write use UpdateByID, so a collect-and-recreate in between
	// cannot land the write on a different incarnation.
	Update(ctx context.Context, name string, spec Spec) (*Object[Spec, Status], error)
	// UpdateByID is Update keyed by incarnation: it writes that one row, or
	// returns ErrNotFound. The write half of a read-modify-write.
	UpdateByID(ctx context.Context, id ObjectID, spec Spec) (*Object[Spec, Status], error)
}

// NewClient returns a Client for the given resource kind. Spec and Status must
// match the controller registered for gk.
func NewClient[Spec, Status any](bh *Beehive, gk GroupKind) Client[Spec, Status] {
	return &clientImpl[Spec, Status]{bh: bh, gk: gk}
}

type clientImpl[Spec, Status any] struct {
	bh *Beehive
	gk GroupKind
}

// decode turns a store row into the typed object, applying this kind's
// migrator. Every read path routes through it.
func (c *clientImpl[Spec, Status]) decode(raw *RawObject) (*Object[Spec, Status], error) {
	return rawToTyped[Spec, Status](raw, c.bh.migratorFor(c.gk))
}

func (c *clientImpl[Spec, Status]) Create(ctx context.Context, name string, spec Spec, opts ...Option) (*Object[Spec, Status], error) {
	if err := checkName(name); err != nil {
		return nil, err
	}
	b, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	co, err := c.resolveCreate(opts)
	if err != nil {
		return nil, err
	}

	var obj *Object[Spec, Status]
	// Within keeps the insert and its owner ref atomic, so a crash between them
	// can't leave an ownerless child GC would never collect.
	err = c.bh.store.Within(ctx, func(ctx context.Context) error {
		raw, err := c.insertObject(ctx, name, b, co)
		if err != nil {
			return err
		}
		// Decode inside the transaction so a row whose bytes don't round-trip
		// never commits: the error rolls back the insert and its buffered wake.
		// decode is pure over the in-memory raw, and the migrator never runs on
		// a create, so the only failure here is an asymmetric caller codec.
		obj, err = c.decode(raw)
		if err != nil {
			return err
		}
		c.signalCreated(ctx, raw, co)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// resolveCreate folds the create-time options, then applies the one validation
// that needs the control plane — before any store work, so a caller mistake
// never takes the write lock.
func (c *clientImpl[Spec, Status]) resolveCreate(opts []Option) (*createOptions, error) {
	co, err := resolveCreate(opts)
	if err != nil {
		return nil, err
	}
	if err := c.checkFinalizersClearable(co); err != nil {
		return nil, err
	}
	return co, nil
}

// checkFinalizersClearable rejects WithFinalizers on a kind this process has no
// controller for. Such a finalizer is unclearable — FinalizersDelete is
// kind-folded, so no other controller can reach it either — and the row would
// sit deletion-pending forever, RESTRICT-blocking its owner's delete. The check
// is process-local and evaluated at call time (the store tracks no
// registrations), and it fires on GetOrCreate's found branch too: deferring to
// the insert would make the same call pass or fail depending on whether the row
// happens to exist. Gated on the option actually being used, so an ordinary
// create never takes bh.mu.
func (c *clientImpl[Spec, Status]) checkFinalizersClearable(co *createOptions) error {
	if len(co.finalizers) == 0 || c.bh.isRegistered(c.gk) {
		return nil
	}
	return fmt.Errorf("%w: WithFinalizers needs a controller registered for %s/%s in this process to clear them; "+
		"a finalizer no controller here can remove would leave the row deletion-pending forever",
		ErrInvalidOption, c.gk.Group, c.gk.Kind)
}

// insertObject inserts one new row and wires its owner edge; every create path
// shares it. Callers run it inside a Within so the insert and its ref commit
// together.
func (c *clientImpl[Spec, Status]) insertObject(ctx context.Context, name string, spec []byte, co *createOptions) (*RawObject, error) {
	raw, err := c.bh.store.ObjectsCreate(ctx, c.gk, ObjectsCreateInput{
		Name:        name,
		Spec:        spec,
		SpecVersion: migratorSpecVersion(c.bh.migratorFor(c.gk)),
		Finalizers:  co.finalizers,
	})
	if err != nil {
		return nil, err
	}
	// The child owns the edge (child -> owner) so the owner's GC walk finds it.
	// No wake stamp: ownership owes the child no reconcile.
	if co.owner != nil {
		if _, err := c.bh.store.EdgesAdd(ctx, raw.ID, *co.owner, RelationOwnedBy); err != nil {
			return nil, err
		}
	}
	return raw, nil
}

// signalCreated registers what a freshly inserted row owes after the commit:
// the caller's WithOnCreate hook and the row's first reconcile. Both go through
// AfterCommit, so neither fires on a rollback.
func (c *clientImpl[Spec, Status]) signalCreated(ctx context.Context, raw *RawObject, co *createOptions) {
	if co.onCreate != nil {
		c.bh.store.AfterCommit(ctx, co.onCreate)
	}
	// A create always changes the object: there was nothing before it.
	c.signalSpecWritten(ctx, raw.ID)
}

// signalSpecWritten enqueues id's own reconcile once the write that changed its
// spec commits — what makes a spec write prompt rather than a wait for the owed
// pass.
//
// The caller passes only writes that actually CHANGED the object. Gating on
// "the caller called Update" would enqueue byte-identical writes, rebuilding
// the self-wake loop the store's no-op skip exists to prevent — and worse,
// since requeueNow beats the backoff ladder. Gating on the row being unsettled
// has the same defect: a failing reconcile leaves it unsettled forever.
//
// The signal is read as the write leaves it, not as the transaction commits: a
// spec-then-UpdateStatus in one Within commits settled and still enqueues once,
// a harmless duplicate in the direction this design errs throughout (pinned by
// TestSpecThenStatusInOneTransactionStillEnqueues).
func (c *clientImpl[Spec, Status]) signalSpecWritten(ctx context.Context, id ObjectID) {
	c.bh.signalRequeue(ctx, ObjectRef{ID: id, Group: c.gk.Group, Kind: c.gk.Kind})
}

// GetOrCreate returns the row holding name, creating it only when absent. The
// found branch does no write at all. See the Client interface for the contract.
func (c *clientImpl[Spec, Status]) GetOrCreate(ctx context.Context, name string, spec Spec, opts ...Option) (*Object[Spec, Status], bool, error) {
	if err := checkName(name); err != nil {
		return nil, false, err
	}
	b, err := json.Marshal(spec)
	if err != nil {
		return nil, false, err
	}
	co, err := c.resolveCreate(opts)
	if err != nil {
		return nil, false, err
	}
	var obj *Object[Spec, Status]
	var created bool
	// One Within around the read and the insert removes the TOCTOU: the loser
	// of a concurrent create observes the winner's row.
	err = c.bh.store.Within(ctx, func(ctx context.Context) error {
		existing, err := c.bh.store.ObjectsGetByName(ctx, c.gk, name)
		if err == nil {
			// Found: returned as-is, live or deletion-pending. A pre-existing
			// poison row's decode error surfaces with created=false.
			obj, err = c.decode(existing)
			return err
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		raw, err := c.insertObject(ctx, name, b, co)
		if err != nil {
			return err
		}
		// Decode inside the transaction (see Create); created is set only after
		// it succeeds, so a rolled-back create reports created=false.
		obj, err = c.decode(raw)
		if err != nil {
			return err
		}
		created = true
		// Wake and WithOnCreate fire only for a row we just made; returning an
		// existing object is a pure read.
		c.signalCreated(ctx, raw, co)
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return obj, created, nil
}

func (c *clientImpl[Spec, Status]) Update(ctx context.Context, name string, spec Spec) (*Object[Spec, Status], error) {
	if err := checkName(name); err != nil {
		return nil, err
	}
	return c.update(ctx, spec, func(ctx context.Context, b []byte, version int) (*RawObject, bool, error) {
		return c.bh.store.ObjectsUpdateSpecByName(ctx, c.gk, name, b, version)
	})
}

func (c *clientImpl[Spec, Status]) UpdateByID(ctx context.Context, id ObjectID, spec Spec) (*Object[Spec, Status], error) {
	return c.update(ctx, spec, func(ctx context.Context, b []byte, version int) (*RawObject, bool, error) {
		// ObjectsUpdateSpec folds this client's kind into the write;
		// hideWrongKind keeps a foreign id invisible.
		raw, changed, err := c.bh.store.ObjectsUpdateSpec(ctx, c.gk, id, b, version)
		return raw, changed, c.hideWrongKind(err)
	})
}

// update is the body both spec writes share. The marshal stays outside the
// transaction — on a single-connection store it would hold the write lock
// across arbitrary user MarshalJSON code. Wrapping the write in Within lets the
// decode join its transaction, so a spec that doesn't round-trip rolls the
// write back.
func (c *clientImpl[Spec, Status]) update(
	ctx context.Context,
	spec Spec,
	write func(ctx context.Context, b []byte, version int) (*RawObject, bool, error),
) (*Object[Spec, Status], error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	var obj *Object[Spec, Status]
	err = c.bh.store.Within(ctx, func(ctx context.Context) error {
		raw, changed, err := write(ctx, b, migratorSpecVersion(c.bh.migratorFor(c.gk)))
		if err != nil {
			return err
		}
		if obj, err = c.decode(raw); err != nil {
			return err
		}
		if changed {
			c.signalSpecWritten(ctx, raw.ID)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (c *clientImpl[Spec, Status]) GetByID(ctx context.Context, id ObjectID, loads ...LoadOption) (*Object[Spec, Status], error) {
	raw, err := c.scopedGet(ctx, id)
	if err != nil {
		return nil, err
	}
	obj, err := c.decode(raw)
	if err != nil {
		return nil, err
	}
	if err := loadObjectRelated(ctx, c.bh.store, obj, resolveLoads(loads)); err != nil {
		return nil, err
	}
	return obj, nil
}

// scopedGet loads id and confirms it belongs to this client's kind, reporting
// ErrNotFound for a foreign id — a Client serves a single kind, so another
// kind's rows must be invisible through it.
func (c *clientImpl[Spec, Status]) scopedGet(ctx context.Context, id ObjectID) (*RawObject, error) {
	raw, err := c.bh.store.ObjectsGet(ctx, id)
	if err != nil {
		return nil, err
	}
	if raw.Group != c.gk.Group || raw.Kind != c.gk.Kind {
		return nil, ErrNotFound
	}
	return raw, nil
}

// hideWrongKind maps the scoped store writes' ErrWrongKind to ErrNotFound,
// mirroring scopedGet on the read path.
func (c *clientImpl[Spec, Status]) hideWrongKind(err error) error {
	if errors.Is(err, ErrWrongKind) {
		return ErrNotFound
	}
	return err
}

func (c *clientImpl[Spec, Status]) Get(ctx context.Context, name string, loads ...LoadOption) (*Object[Spec, Status], error) {
	if err := checkName(name); err != nil {
		return nil, err
	}
	raw, err := c.bh.store.ObjectsGetByName(ctx, c.gk, name)
	if err != nil {
		return nil, err
	}
	obj, err := c.decode(raw)
	if err != nil {
		return nil, err
	}
	if err := loadObjectRelated(ctx, c.bh.store, obj, resolveLoads(loads)); err != nil {
		return nil, err
	}
	return obj, nil
}

// loadObjectRelated populates the related-data fields named by set on one
// object, recording each fetched lookup in obj.loaded. Batched List has its own
// path (loadListRelated) to avoid an N+1.
func loadObjectRelated[Spec, Status any](ctx context.Context, store Store, obj *Object[Spec, Status], set LoadSet) error {
	if set&LoadOwnerBit != 0 {
		owner, ok, err := fetchOwnerRef(ctx, store, obj.ID)
		if err != nil {
			return err
		}
		if ok {
			obj.owner = &owner
		}
		obj.loaded |= LoadOwnerBit
	}
	if set&LoadDependenciesBit != 0 {
		deps, err := store.EdgesListOutgoingByRelation(ctx, obj.ID, RelationDependsOn)
		if err != nil {
			return err
		}
		obj.dependencies = deps
		obj.loaded |= LoadDependenciesBit
	}
	if set&LoadDependentsBit != 0 {
		dependents, err := store.EdgesListIncoming(ctx, obj.ID, RelationDependsOn)
		if err != nil {
			return err
		}
		obj.dependents = dependents
		obj.loaded |= LoadDependentsBit
	}
	if set&LoadOwnedBit != 0 {
		owned, err := store.EdgesListIncoming(ctx, obj.ID, RelationOwnedBy)
		if err != nil {
			return err
		}
		obj.owned = owned
		obj.loaded |= LoadOwnedBit
	}
	if set&LoadEventsBit != 0 {
		raw, err := store.EventsList(ctx, obj.ID, storeapi.EventQuery{})
		if err != nil {
			return err
		}
		obj.events = eventsFromRaw(raw)
		obj.loaded |= LoadEventsBit
	}
	return nil
}

// fetchOwnerRef resolves id's single owned_by edge; ok is false when there is
// none.
func fetchOwnerRef(ctx context.Context, store Store, id ObjectID) (ObjectRef, bool, error) {
	owners, err := store.EdgesListOutgoingByRelation(ctx, id, RelationOwnedBy)
	if err != nil {
		return ObjectRef{}, false, err
	}
	if len(owners) == 0 {
		return ObjectRef{}, false, nil
	}
	return owners[0], true, nil
}

func (c *clientImpl[Spec, Status]) List(ctx context.Context, loads ...LoadOption) ([]*Object[Spec, Status], error) {
	raws, err := c.bh.store.ObjectsList(ctx, c.gk)
	if err != nil {
		return nil, err
	}
	objs := c.decodeList(raws, "List")
	if err := c.loadListRelated(ctx, objs, resolveLoads(loads)); err != nil {
		return nil, err
	}
	return objs, nil
}

// decodeList decodes a multi-row read, quarantining rather than aborting: one
// undecodable row is skipped and logged so it can't break the whole read.
func (c *clientImpl[Spec, Status]) decodeList(raws []*RawObject, method string) []*Object[Spec, Status] {
	mig := c.bh.migratorFor(c.gk)
	objs := make([]*Object[Spec, Status], 0, len(raws))
	for _, raw := range raws {
		obj, err := rawToTyped[Spec, Status](raw, mig)
		if err != nil {
			c.warnUndecodable(method, raw.ID, err)
			continue
		}
		objs = append(objs, obj)
	}
	return objs
}

// warnUndecodable is the one quarantine log line, shared by every read path
// that skips a poison row; the call site rides in "op" so the line groups.
func (c *clientImpl[Spec, Status]) warnUndecodable(method string, id ObjectID, err error) {
	c.bh.log().Warn("beehive: skipping undecodable object",
		"op", method, "group", c.gk.Group, "kind", c.gk.Kind, "id", id, "err", err)
}

// loadListRelated eager-loads the requested secondary lookups for a whole list
// in one batched store call per relation — the N+1-free counterpart to
// loadObjectRelated.
func (c *clientImpl[Spec, Status]) loadListRelated(ctx context.Context, objs []*Object[Spec, Status], set LoadSet) error {
	if set == 0 || len(objs) == 0 {
		return nil
	}
	ids := make([]ObjectID, len(objs))
	for i, o := range objs {
		ids[i] = o.ID
	}
	if set&LoadOwnerBit != 0 {
		byID, err := c.bh.store.EdgesGroupOutgoingByID(ctx, ids, RelationOwnedBy)
		if err != nil {
			return err
		}
		for _, o := range objs {
			if owners := byID[o.ID]; len(owners) > 0 {
				owner := owners[0]
				o.owner = &owner
			}
			o.loaded |= LoadOwnerBit
		}
	}
	if set&LoadDependenciesBit != 0 {
		byID, err := c.bh.store.EdgesGroupOutgoingByID(ctx, ids, RelationDependsOn)
		if err != nil {
			return err
		}
		for _, o := range objs {
			o.dependencies = byID[o.ID]
			o.loaded |= LoadDependenciesBit
		}
	}
	if set&LoadDependentsBit != 0 {
		byID, err := c.bh.store.EdgesGroupIncomingByID(ctx, ids, RelationDependsOn)
		if err != nil {
			return err
		}
		for _, o := range objs {
			o.dependents = byID[o.ID]
			o.loaded |= LoadDependentsBit
		}
	}
	if set&LoadOwnedBit != 0 {
		byID, err := c.bh.store.EdgesGroupIncomingByID(ctx, ids, RelationOwnedBy)
		if err != nil {
			return err
		}
		for _, o := range objs {
			o.owned = byID[o.ID]
			o.loaded |= LoadOwnedBit
		}
	}
	if set&LoadEventsBit != 0 {
		// Events have no batched store primitive, so this is one query per
		// object — the deliberate exception. Prefer the lazy EventsList for
		// large lists.
		for _, o := range objs {
			raw, err := c.bh.store.EventsList(ctx, o.ID, storeapi.EventQuery{})
			if err != nil {
				return err
			}
			o.events = eventsFromRaw(raw)
			o.loaded |= LoadEventsBit
		}
	}
	return nil
}

// The four lazy ref lookups read their edge query directly, with no scopedGet
// kind guard: that guard was a second, blob-bearing read on a hot path. The
// trade: a foreign id reads that kind's edges and a missing id reads empty —
// silent misuse rather than a clean error.
func (c *clientImpl[Spec, Status]) OwnersGet(ctx context.Context, id ObjectID) (ObjectRef, bool, error) {
	return fetchOwnerRef(ctx, c.bh.store, id)
}

func (c *clientImpl[Spec, Status]) DependenciesList(ctx context.Context, id ObjectID) ([]ObjectRef, error) {
	return c.bh.store.EdgesListOutgoingByRelation(ctx, id, RelationDependsOn)
}

func (c *clientImpl[Spec, Status]) DependentsList(ctx context.Context, id ObjectID) ([]ObjectRef, error) {
	return c.bh.store.EdgesListIncoming(ctx, id, RelationDependsOn)
}

func (c *clientImpl[Spec, Status]) OwnedList(ctx context.Context, id ObjectID) ([]ObjectRef, error) {
	return c.bh.store.EdgesListIncoming(ctx, id, RelationOwnedBy)
}

// The kind filter lives in the store statement's WHERE, so foreign-kind
// children never reach Go.
func (c *clientImpl[Spec, Status]) OwnedObjectsList(ctx context.Context, ownerID ObjectID, loads ...LoadOption) ([]*Object[Spec, Status], error) {
	raws, err := c.bh.store.ObjectsListByIncomingEdge(ctx, c.gk, ownerID, RelationOwnedBy)
	if err != nil {
		return nil, err
	}
	objs := c.decodeList(raws, "OwnedObjectsList")
	if err := c.loadListRelated(ctx, objs, resolveLoads(loads)); err != nil {
		return nil, err
	}
	return objs, nil
}

// reconcilerForObject validates id against this client's kind, then resolves
// the kind's reconciler. scopedGet runs first so a missing or foreign id
// surfaces as ErrNotFound regardless of registration.
func (c *clientImpl[Spec, Status]) reconcilerForObject(ctx context.Context, id ObjectID) (*reconciler, error) {
	if _, err := c.scopedGet(ctx, id); err != nil {
		return nil, err
	}
	r, ok := c.bh.reconcilerFor(c.gk)
	if !ok {
		return nil, ErrNoController
	}
	return r, nil
}

// Requeue requeues id for immediate reconcile, preserving its backoff ladder
// unless WithResetBackoff() is passed.
func (c *clientImpl[Spec, Status]) Requeue(ctx context.Context, id ObjectID, opts ...RequeueOption) error {
	r, err := c.reconcilerForObject(ctx, id)
	if err != nil {
		return err
	}
	r.requeue(id, resolveRequeue(opts).resetBackoff)
	return nil
}

// SchedulesGet reads the in-memory work queue directly: no store lookup, no
// kind guard — a foreign, missing, or client-only id all fold into the zero
// Schedule. See the Client interface for the contract.
func (c *clientImpl[Spec, Status]) SchedulesGet(ctx context.Context, id ObjectID) (Schedule, error) {
	r, ok := c.bh.reconcilerFor(c.gk)
	if !ok {
		return Schedule{}, nil // client-only kind: nothing is ever scheduled
	}
	return r.scheduleAt(id), nil
}

func (c *clientImpl[Spec, Status]) DeleteByID(ctx context.Context, id ObjectID) error {
	// DeletionRequestsCreate bumps resource_version only on a real change, so
	// an idempotent retry triggers no spurious watch diff. Kind-folded;
	// hideWrongKind keeps a foreign id invisible.
	_, err := c.bh.store.DeletionRequestsCreate(ctx, c.gk, id)
	if err = c.hideWrongKind(err); err != nil {
		return err
	}
	// Nothing is scheduled: the mark is the signal, and the GC tick is
	// guaranteed (WithGCInterval refuses to be disabled).
	return nil
}

// Delete is DeleteByID keyed by name; the store resolves and marks in one
// statement.
func (c *clientImpl[Spec, Status]) Delete(ctx context.Context, name string) error {
	if err := checkName(name); err != nil {
		return err
	}
	// ErrNotFound is idempotent success here — nothing of this kind holds the
	// name — the one place a name delete departs from DeleteByID.
	if _, err := c.bh.store.DeletionRequestsCreateByName(ctx, c.gk, name); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil // already gone
		}
		return err
	}
	return nil
}

// EventsList reads id's runs and maps them to public Events. Reads by id, not
// kind-scoped, like the ref lookups.
func (c *clientImpl[Spec, Status]) EventsList(ctx context.Context, id ObjectID, opts ...EventOption) ([]Event, error) {
	raw, err := c.bh.store.EventsList(ctx, id, resolveEvents(opts))
	if err != nil {
		return nil, err
	}
	return eventsFromRaw(raw), nil
}

func (c *clientImpl[Spec, Status]) EventsGetLatest(ctx context.Context, id ObjectID, category string) (Event, bool, error) {
	raw, err := c.bh.store.EventsGetLatest(ctx, id, category)
	if err != nil {
		return Event{}, false, err
	}
	if raw == nil {
		return Event{}, false, nil
	}
	return eventFromRaw(*raw), true, nil
}

// conditionsFromRaw maps the store's raw conditions to the public Condition
// type, dropping storage-only bookkeeping. Returns nil for none.
func conditionsFromRaw(raw []storeapi.Condition) []Condition {
	if len(raw) == 0 {
		return nil
	}
	out := make([]Condition, len(raw))
	for i, c := range raw {
		out[i] = Condition{
			Type:     c.Type,
			Status:   ConditionStatus(c.Status),
			Reason:   c.Reason,
			Message:  c.Message,
			Liveness: c.Liveness,
		}
	}
	return out
}

// eventFromRaw maps a raw event row to the public Event, dropping the
// store-only resource_version cursor.
func eventFromRaw(raw storeapi.Event) Event {
	return Event{
		ID:       raw.ID,
		ObjectID: raw.ObjectID,
		Category: raw.Category,
		Type:     EventType(raw.Type),
		Reason:   raw.Reason,
		Message:  raw.Message,
		Detail:   json.RawMessage(raw.Detail),
		Count:    raw.Count,
		FirstAt:  raw.FirstAt,
		LastAt:   raw.LastAt,
	}
}

// eventsFromRaw maps raw event rows to public Events. Returns nil for none.
func eventsFromRaw(raw []storeapi.Event) []Event {
	if len(raw) == 0 {
		return nil
	}
	out := make([]Event, len(raw))
	for i, r := range raw {
		out[i] = eventFromRaw(r)
	}
	return out
}

// convertBlob upgrades a stored JSON blob from its recorded schema version to
// the build's current one. current == 0 or from == current is identity;
// from > current is a downgrade — an older build reading newer data — refused
// rather than silently truncated.
func convertBlob(from, current int, raw []byte, convert func(int, json.RawMessage) (json.RawMessage, error)) ([]byte, error) {
	switch {
	case current == 0 || from == current:
		return raw, nil
	case from > current:
		return nil, fmt.Errorf("beehive: stored schema version %d is newer than this build's %d", from, current)
	default: // from < current
		return convert(from, raw)
	}
}

// rawToTyped decodes a RawObject into a typed Object[Spec, Status], converting
// each blob up from its stored schema version via m before unmarshalling. A nil
// m means the kind has no migrator and every blob decodes as-is.
func rawToTyped[Spec, Status any](raw *RawObject, m Migrator) (*Object[Spec, Status], error) {
	// The converters are only reached when from < current, never when m is nil.
	var convertSpec, convertStatus func(int, json.RawMessage) (json.RawMessage, error)
	if m != nil {
		convertSpec = m.ConvertSpec
		convertStatus = m.ConvertStatus
	}

	specBytes, err := convertBlob(raw.SpecVersion, migratorSpecVersion(m), raw.Spec, convertSpec)
	if err != nil {
		return nil, err
	}
	var spec Spec
	if err := json.Unmarshal(specBytes, &spec); err != nil {
		return nil, err
	}
	obj := &Object[Spec, Status]{
		ID:                  raw.ID,
		Group:               raw.Group,
		Kind:                raw.Kind,
		Name:                raw.Name,
		Spec:                spec,
		Generation:          raw.Generation,
		ObservedGeneration:  raw.ObservedGeneration,
		ObservedAt:          raw.ObservedAt,
		ResourceVersion:     raw.ResourceVersion,
		DeletionRequestedAt: raw.DeletionRequestedAt,
		Finalizers:          raw.Finalizers,
		Conditions:          conditionsFromRaw(raw.Conditions),
		CreatedAt:           raw.CreatedAt,
		UpdatedAt:           raw.UpdatedAt,
	}
	if raw.Status != nil {
		statusBytes, err := convertBlob(raw.StatusVersion, migratorStatusVersion(m), raw.Status, convertStatus)
		if err != nil {
			return nil, err
		}
		var status Status
		if err := json.Unmarshal(statusBytes, &status); err != nil {
			return nil, err
		}
		obj.Status = &status
	}
	return obj, nil
}
