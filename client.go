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
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/amorey/beehive/internal/storeapi"
)

// ErrNoController is returned by Requeue when the client's kind has no
// registered controller: there is no reconcile loop to schedule against. A
// client-only kind is read/write but never reconciled.
var ErrNoController = errors.New("beehive: no controller registered for kind")

// ObjectChange reports a change to a watched object: what happened (Type) and
// the object it happened to. On a Deleted change Object carries the row's final
// state. It is delivered by value — a type tag plus one pointer — so a consumer
// never has to reason about a nil change.
type ObjectChange[Spec, Status any] struct {
	Type   ChangeType
	Object *Object[Spec, Status]
}

// Client is the user-facing API for a single resource kind: the surface for
// creating, reading, updating, deleting, and watching objects.
type Client[Spec, Status any] interface {
	Create(ctx context.Context, spec Spec, opts ...Option) (*Object[Spec, Status], error)
	CreateOrUpdate(ctx context.Context, slug string, spec Spec) (*Object[Spec, Status], error)
	// Delete soft-deletes the object by setting DeletionRequestedAt. That mark is the
	// whole signal: it puts the row in the GC sweeper's listing, so the next sweep
	// hands it to the controller to clear finalizers, and physical removal follows
	// once they clear. An id naming no object of this kind is ErrNotFound — contrast
	// DeleteBySlug, which folds absence to nil.
	Delete(ctx context.Context, id ObjectID) error
	// DeleteBySlug requests deletion of the object with the given slug. It is
	// idempotent: a slug that matches no object returns nil (already gone), and a
	// row already deletion-pending is a no-op returning nil (as Delete is on a
	// repeated call). Kind-scoped like GetBySlug — a slug is per-kind, so this only
	// ever targets this client's kind. Deletion itself is Delete's semantics: the
	// soft-delete mark, collected on a later sweep.
	//
	// The delete-if-present partner to GetOrCreate's create-if-absent, so an
	// ensure/remove pair is one call on each side.
	DeleteBySlug(ctx context.Context, slug string) error
	// DependenciesList returns the objects id depends on (its outgoing depends_on
	// edges). The lazy counterpart to LoadDependencies().
	DependenciesList(ctx context.Context, id ObjectID) ([]ObjectRef, error)
	// DependentsList returns the objects that depend on id (incoming depends_on).
	// The lazy counterpart to LoadDependents().
	DependentsList(ctx context.Context, id ObjectID) ([]ObjectRef, error)
	// EventsGetLatest returns the current (most-recent) run in id's category timeline.
	// ok reports presence: false (with a nil error) when the timeline is empty.
	EventsGetLatest(ctx context.Context, id ObjectID, category string) (Event, bool, error)

	// EventsList returns id's event-log runs, newest-first, filtered by the given
	// options (see EventOption). Like the ref lookups it reads by id and does not
	// kind-scope: a foreign id reads that object's log. An empty log is an empty slice.
	EventsList(ctx context.Context, id ObjectID, opts ...EventOption) ([]Event, error)
	// EventsWatch streams id's event log: the runs matching opts, then whatever the
	// log grows by, on the returned channel. The channel closes when ctx is cancelled.
	// Like the object watches it requires a registered controller, is scoped to this
	// client's kind, and polls on a fixed interval — so a run extended several
	// times within one interval is delivered once, carrying its latest state.
	EventsWatch(ctx context.Context, id ObjectID, opts ...EventOption) (<-chan Event, error)
	Get(ctx context.Context, id ObjectID, loads ...LoadOption) (*Object[Spec, Status], error)
	GetBySlug(ctx context.Context, slug string, loads ...LoadOption) (*Object[Spec, Status], error)
	// GetOrCreate returns the object with the given slug, creating it from spec if
	// absent. Unlike CreateOrUpdate it NEVER mutates an existing row: a slug held by
	// a live OR deletion-pending row is returned as-is with created=false, so the
	// caller can inspect DeletionRequestedAt and decide whether to wait for GC and
	// retry — a tombstone still holds the slug's UNIQUE constraint, so no replacement
	// can be created until GC clears it. The read-or-create is atomic (a single
	// store.Within transaction), so concurrent reconciles can't both create — one
	// wins, the other observes it with created=false.
	//
	// On create the new object is unsettled and so owed its first reconcile, as with
	// Create; returning an existing row writes nothing and owes nothing.
	//
	// opts apply only on the create branch (WithOwner, WithFinalizers, WithOnCreate).
	// WithSlug is rejected with ErrConflictingOption rather than ignored: the slug is
	// positional here, so the option can only contradict it, and silently dropping it
	// would surface much later as an ErrNotFound on the slug the caller meant to use.
	//
	// The returned created bool is synchronous, so inside a caller's
	// ControllerClient.Within it is set before the enclosing transaction commits: a
	// caller that fires a non-store side effect on created==true acts for a row a
	// later rollback discards. Route create-conditional side effects through
	// WithOnCreate, which runs only after the outermost commit; read created as a
	// correct after-the-fact report, not a pre-commit trigger.
	//
	// A non-nil err means nothing was created: the new row is decoded back into
	// Spec/Status inside the atomic Within, so a spec whose bytes don't round-trip (a
	// marshal/type bug) rolls the insert back rather than committing a row the process
	// can't read. Such an error therefore returns created=false, and a retry does not
	// hit a spurious UNIQUE on a phantom row. (A decode error on the found branch — a
	// pre-existing poison row this call did not write — likewise returns created=false,
	// since it plainly wasn't created here.)
	//
	// On the found branch the options are ignored outright — an existing row that
	// lacks the owner or finalizers you passed keeps lacking them, and created=false
	// is the only signal you get. Do not read created=false as "exists and matches
	// opts": a caller depending on the owner edge (for cascade collection, say) must
	// check it with LoadOwner()/OwnersGet and reconcile the difference itself.
	// Beehive can't adopt the row for you — owner is single, so adding the edge to a
	// row that already has a different owner would produce a two-owner object, and
	// picking a winner is caller policy.
	//
	// spec and opts are nonetheless validated up front, before the slug is read, so
	// an unmarshalable spec or an erroring option fails the call even when the row
	// already exists and neither would have been used. Both are caller bugs, and
	// validating them eagerly keeps the error independent of store state: deferring
	// would make the same call succeed or fail depending on whether the row happens
	// to exist, hiding the bug until GC or a cold start removed it. It also keeps
	// json.Marshal out of the transaction, which on a single-connection store would
	// hold the write lock across arbitrary user MarshalJSON code.
	GetOrCreate(ctx context.Context, slug string, spec Spec, opts ...Option) (*Object[Spec, Status], bool, error)
	List(ctx context.Context, loads ...LoadOption) ([]*Object[Spec, Status], error)
	// ObjectsWatch streams changes to one object, ObjectsWatchList to every object
	// of this client's kind: the current state as Added, then Added/Modified/Deleted
	// as things change, until ctx is cancelled and the channel closes. Both need a
	// registered controller for the kind and are scoped to it, so another kind's id
	// streams nothing, and an id that does not exist yet is simply reported as Added
	// once it appears.
	//
	// Both read current state before returning, so a caller may subscribe and then
	// act: a change it makes afterwards is measured against a snapshot that already
	// exists, and cannot fall into the gap before the first poll. That costs one
	// store read on the subscribing goroutine, and its failure is returned — a
	// stream handed back always holds a snapshot, where one that did not would be a
	// watch with nothing to compare against, silently unable to report the delete
	// the caller is about to make. Every later poll failure costs a tick instead,
	// since the last good poll's state is still there.
	//
	// Everything after that read is polled (see withWatchPollInterval), which is what
	// bounds latency and collapses changes within one interval into the latest state.
	//
	// That read is also why a watch cannot be opened inside a transaction: the store
	// runs on one connection, so the read waits for the connection the transaction
	// holds, and passing the transaction's own ctx instead would tie the stream's
	// life to it. Subscribe outside Within (see ControllerClient.Within).
	ObjectsWatch(ctx context.Context, id ObjectID) (<-chan ObjectChange[Spec, Status], error)
	ObjectsWatchList(ctx context.Context) (<-chan ObjectChange[Spec, Status], error)
	// OwnedList returns the objects id owns (its incoming owned_by edges). The
	// lazy counterpart to LoadOwned().
	OwnedList(ctx context.Context, id ObjectID) ([]ObjectRef, error)

	// OwnedObjectsList returns the objects owned by ownerID that belong to THIS
	// client's kind, fully decoded — the typed, kind-scoped form of OwnedList
	// (which returns untyped Refs across every owned kind, leaving the caller to
	// filter by Kind and Get each child through that kind's client). Ownership is
	// the owned_by edge (child -> owner), so these are ownerID's children of this
	// kind, ordered by id as OwnedList is.
	//
	// ownerID need not be this client's kind — it is the owner, typically another
	// kind — and like OwnedList it is not kind-scoped or existence-checked: an
	// owner with no children of this kind, and an ownerID that doesn't exist, both
	// read empty rather than ErrNotFound. A deletion-pending child is included;
	// whether to skip it is the caller's call (check DeletionRequestedAt).
	// Undecodable rows are quarantined and logged, as in List.
	//
	// It takes the same LoadOptions as List, batched the same way: without them the
	// children come back with nothing loaded and their ref/event accessors return
	// ErrNotLoaded. Pass e.g. LoadOwned() to walk a second level of the tree
	// without a Get per child — the per-child Get this method exists to avoid.
	OwnedObjectsList(ctx context.Context, ownerID ObjectID, loads ...LoadOption) ([]*Object[Spec, Status], error)

	// OwnersGet returns id's owner, if it has one. ok is false with a nil error when
	// it simply has none. This is the lazy counterpart to LoadOwner(): fetch the owner
	// only when you need it.
	//
	// This and DependenciesList/DependentsList/OwnedList run their edge query directly
	// and do not check the kind. Another kind's id reads that kind's edges, and a
	// missing id reads empty; neither returns ErrNotFound. Use them for ids this
	// client owns.
	OwnersGet(ctx context.Context, id ObjectID) (ObjectRef, bool, error)

	// Requeue queues id for reconcile now. It is a latency hint, not a synchronous
	// run: correctness rests on the periodic drivers, not on this call.
	//
	// It is also how you drive reconciles on your own schedule rather than waiting
	// out the periodic passes, which is the point of leaving WithFullPassInterval
	// off: the owed pass still guarantees convergence, and this is what makes it
	// prompt. Store.ObjectsListUnsettledIDs reports what is owed.
	//
	// By default it keeps id's retry backoff. A requeue is an ordinary nudge — a
	// config change, a dependency update, a manual poke — and almost never proves the
	// failure is over. Backoff is cleared by a successful reconcile or by
	// WithResetBackoff(), never by a plain requeue, so pass WithResetBackoff() only
	// when you know the failure is resolved and the next retry should start from the
	// base interval.
	//
	// Returns ErrNotFound if id does not exist, and ErrNoController if the kind has no
	// registered controller.
	Requeue(ctx context.Context, id ObjectID, opts ...RequeueOption) error
	// SchedulesGet reports id's Schedule: when the reconcile loop has scheduled id to
	// be requeued — a pending backoff retry or RequeueAfter delay, or, for an id
	// already queued, the moment it became due — in Schedule.NextRequeueAt, or the
	// zero time if nothing is scheduled. A queued id therefore reads as a time in the
	// past (it is dispatchable now, and has been since then), and that time holds
	// still while it waits, which is what lets SchedulesWatch treat "still queued" as
	// a repeated value rather than a moving one. The Schedule wrapper leaves room for fields to be added later, such
	// as a reschedule trigger.
	//
	// It is a non-blocking read of in-memory state and touches no store, so the error
	// is reserved for symmetry and never returned today. An id that does not exist, or
	// belongs to another kind, is simply unscheduled, and a client-only kind has no
	// reconcile loop at all; both read as the zero Schedule, which looks the same as a
	// real object with nothing scheduled.
	//
	// This is the next *scheduled* requeue, not a prediction of the next reconcile.
	// Nothing that isn't a per-id timer can appear here: not the kind-wide owed and
	// full passes, not the dependency wake, not Requeue. So the real next reconcile
	// may be earlier, and a zero NextRequeueAt means "nothing scheduled", not "will
	// not reconcile". Treat it as observability. Use SchedulesWatch to follow it
	// live.
	SchedulesGet(ctx context.Context, id ObjectID) (Schedule, error)
	// SchedulesWatch streams id's schedule as a gauge: the current value first, then a
	// new Schedule whenever it changes — backoff step, RequeueAfter, a pass or
	// dependency wake, dispatch, or Requeue — none of which the object watches see.
	// The channel closes when ctx is cancelled. Like the object watches it polls on a
	// fixed interval and emits only on change, so it converges to the current
	// value and can skip intermediate ones. Unlike SchedulesGet, a client-only kind
	// returns ErrNoController rather than hang on a stream that can never emit; id need
	// not exist — an unscheduled id streams the zero Schedule until scheduled.
	SchedulesWatch(ctx context.Context, id ObjectID) (<-chan Schedule, error)
	Update(ctx context.Context, id ObjectID, spec Spec) (*Object[Spec, Status], error)
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

// decode turns a store row into the typed object, applying this kind's migrator
// (if any) at the decode boundary. Every read path routes through it so the
// client and the reconciler share one migrator per kind.
func (c *clientImpl[Spec, Status]) decode(raw *RawObject) (*Object[Spec, Status], error) {
	return rawToTyped[Spec, Status](raw, c.bh.migratorFor(c.gk))
}

func (c *clientImpl[Spec, Status]) Create(ctx context.Context, spec Spec, opts ...Option) (*Object[Spec, Status], error) {
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
	// can't leave an ownerless child the GC path would never collect.
	err = c.bh.store.Within(ctx, func(ctx context.Context) error {
		raw, err := c.insertObject(ctx, b, co)
		if err != nil {
			return err
		}
		// Decode inside the transaction so a row whose bytes don't round-trip never
		// commits: returning the error here rolls back the insert and discards the
		// buffered wake, preserving "a Create that returns an error made no change"
		// (no committed poison row, no phantom enqueue). decode is pure over the
		// in-memory raw — no store read — and the migrator never runs on a create
		// (the row is stamped at the current version, so from==current is identity),
		// so the only failure here is an asymmetric caller codec. This is the one
		// spot we accept user unmarshal code inside the tx; marshal stays outside it.
		obj, err = c.decode(raw)
		if err != nil {
			return err
		}
		c.signalCreated(ctx, co)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// resolveCreate folds the create-time options and then applies the one validation
// that needs the control plane rather than the option values alone. It wraps the
// package-level resolveCreate, and runs before any store work for the same reason
// that one does — so a caller mistake never takes the store's write lock.
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
// controller for.
//
// Such a finalizer is unclearable, not merely useless. FinalizersDelete lives on
// ControllerClient and folds the caller's own GroupKind into the store mutator, so no
// other kind's controller can reach the row either — it gets ErrWrongKind for trying.
// gcCollect returns early while any finalizer remains, so the row stays
// deletion-pending forever: re-listed by every GC sweep, making no progress on any of
// them, and its owned_by edge RESTRICT-blocks its owner's delete permanently. That is
// exactly the strand the global sweeper exists to prevent for client-only kinds — the
// sweeper reaches the row, it just has nothing it is allowed to do with it.
//
// **The check is process-local and evaluated at call time**, and both halves are
// limits rather than guarantees. bh.isRegistered answers for *this* Beehive, so a
// create issued from a process that does not register the kind is refused even when
// another process over the same store does register it; and a create issued before
// this process's own Register is refused for the same reason. Neither is checkable in
// the store, which tracks no registrations — the same reason EdgesAdd's wake stamp
// deliberately does not gate on registration. The asymmetry is deliberate: gating
// there would *lose* a wake silently, where refusing here is loud and the caller can
// reorder or register.
//
// It is gated on the option actually being used, so an ordinary create never takes
// bh.mu.
//
// **It fires on GetOrCreate's found branch too**, where no row is created and the
// option would have been ignored. That is deliberate, and it follows the eager
// validation rule GetOrCreate's own doc sets out: deferring to the insert would make
// the same call succeed or fail depending on whether the row happens to exist. This
// check is the worst case for that, because the strand it prevents only happens on a
// create — a caller who deferred would see it pass wherever the row already exists
// and strand the first time it doesn't.
func (c *clientImpl[Spec, Status]) checkFinalizersClearable(co *createOptions) error {
	if len(co.finalizers) == 0 || c.bh.isRegistered(c.gk) {
		return nil
	}
	return fmt.Errorf("%w: WithFinalizers needs a controller registered for %s/%s in this process to clear them; "+
		"a finalizer no controller here can remove would leave the row deletion-pending forever",
		ErrInvalidOption, c.gk.Group, c.gk.Kind)
}

// insertObject inserts one new row of this client's kind and wires its owner
// edge. Every create path shares it, so the row shape, the spec-version stamp,
// and the owner-ref policy live in one place. Callers run it inside a Within:
// the insert and its ref must commit together, or a crash between them leaves an
// ownerless child the GC path would never collect. The slug rides on co, which
// each caller has already populated from its single source (WithSlug for Create,
// the positional argument for CreateOrUpdate/GetOrCreate).
func (c *clientImpl[Spec, Status]) insertObject(ctx context.Context, spec []byte, co *createOptions) (*RawObject, error) {
	raw, err := c.bh.store.ObjectsCreate(ctx, &RawObject{
		Group:       c.gk.Group,
		Kind:        c.gk.Kind,
		Slug:        co.slug,
		Spec:        spec,
		SpecVersion: migratorSpecVersion(c.bh.migratorFor(c.gk)),
		Finalizers:  co.finalizers,
	})
	if err != nil {
		return nil, err
	}
	// The child owns the edge (child -> owner) so the owner's GC walk finds it
	// via EdgesListIncoming(owner, RelationOwnedBy). No version claim and no wake
	// stamp: an owner edge carries no read this call could be racing.
	if co.owner != nil {
		if _, err := c.bh.store.EdgesAdd(ctx, raw.ID, *co.owner, RelationOwnedBy, 0); err != nil {
			return nil, err
		}
	}
	return raw, nil
}

// signalCreated registers what a freshly inserted row owes after the commit: the
// caller's WithOnCreate hook, if there is one. It goes through AfterCommit, so it
// fires after the outermost commit and never on a rollback. Create and GetOrCreate
// share it, keeping the create-side-effect wiring in one place next to insertObject.
//
// A create schedules no reconcile. An object whose spec nothing has observed is
// unsettled, which is exactly what the owed pass lists, and the row is the record
// — so a rollback leaves nothing behind, for free.
func (c *clientImpl[Spec, Status]) signalCreated(ctx context.Context, co *createOptions) {
	if co.onCreate != nil {
		c.bh.store.AfterCommit(ctx, co.onCreate)
	}
}

// CreateOrUpdate idempotently reconciles the object named by slug to spec: it
// updates the existing object carrying that slug, or creates one with that slug
// if none exists. Wrapping the read-then-write in Within makes the upsert atomic,
// so concurrent callers can't both insert the same slug — the second sees the
// first's row and updates instead. Re-applying the same spec is a no-op (ObjectsUpdateSpec
// suppresses the generation bump on equal bytes).
//
// It drives the store mutators directly rather than composing Create/Update so
// one call produces at most one row change, and so the create branch stays
// distinguishable from the update branch for WithOnCreate's sake.
func (c *clientImpl[Spec, Status]) CreateOrUpdate(ctx context.Context, slug string, spec Spec) (*Object[Spec, Status], error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	var obj *Object[Spec, Status]
	err = c.bh.store.Within(ctx, func(ctx context.Context) error {
		existing, err := c.bh.store.ObjectsGetBySlug(ctx, c.gk, slug)
		var raw *RawObject
		switch {
		case err == nil:
			raw, _, err = c.bh.store.ObjectsUpdateSpec(ctx, c.gk, existing.ID, b, migratorSpecVersion(c.bh.migratorFor(c.gk)))
		case errors.Is(err, ErrNotFound):
			// No opts on this surface, so the row carries no finalizers and no owner.
			raw, err = c.insertObject(ctx, b, &createOptions{slug: &slug})
		}
		// A non-NotFound read error falls through both cases with raw unset; err
		// still carries it. Both write branches reassign err.
		if err != nil {
			return err
		}
		obj, err = c.decode(raw)
		return err
	})
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// GetOrCreate returns the row holding slug, creating it only when absent. It is
// the create-if-absent sibling of CreateOrUpdate: the found branch does no write
// at all, so a deletion-pending row comes back with its tombstone intact rather
// than being spuriously bumped back to life. See the Client interface for the
// full contract.
func (c *clientImpl[Spec, Status]) GetOrCreate(ctx context.Context, slug string, spec Spec, opts ...Option) (*Object[Spec, Status], bool, error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return nil, false, err
	}
	co, err := c.resolveCreate(opts)
	if err != nil {
		return nil, false, err
	}
	// The slug is positional here, so WithSlug can only contradict it. Reject rather
	// than silently drop it: the ignored option would otherwise surface much later,
	// as an ErrNotFound on a slug the caller believed it had asked for.
	if co.slug != nil {
		return nil, false, fmt.Errorf("%w: GetOrCreate takes the slug positionally; WithSlug(%q) conflicts with %q",
			ErrConflictingOption, *co.slug, slug)
	}

	var obj *Object[Spec, Status]
	var created bool
	// One Within around the read and the insert is what removes the caller's
	// TOCTOU: the loser of a concurrent create observes the winner's row instead
	// of failing on the slug's UNIQUE constraint.
	err = c.bh.store.Within(ctx, func(ctx context.Context) error {
		existing, err := c.bh.store.ObjectsGetBySlug(ctx, c.gk, slug)
		if err == nil {
			// Found: return the existing row as-is (live or deletion-pending; never
			// mutated). A pre-existing poison row is not ours to roll back, so its
			// decode error surfaces with created=false — it plainly wasn't created here.
			obj, err = c.decode(existing)
			return err
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		// co.slug is nil here — WithSlug was rejected up front — so the positional
		// slug is the row's only name.
		co.slug = &slug
		raw, err := c.insertObject(ctx, b, co)
		if err != nil {
			return err
		}
		// Decode inside the transaction (see Create): a new row whose bytes don't
		// round-trip must never commit, so its decode error rolls the insert back
		// rather than leaving a committed poison row. created is set only after the
		// decode succeeds, so a rolled-back create reports created=false — the bool
		// tracks what actually landed, never a row the transaction discarded.
		obj, err = c.decode(raw)
		if err != nil {
			return err
		}
		created = true
		// Wake and any WithOnCreate hook fire only for a row we just made: returning
		// an existing object is a pure read and must not nudge the reconciler or run
		// create-conditional side effects (which is also why they aren't gated on the
		// returned created bool — see below).
		c.signalCreated(ctx, co)
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	// The returned created bool is synchronous: inside a caller's Within it reports
	// true before the enclosing transaction commits, so a caller that fires a
	// non-store side effect on it can act for a row a later rollback discards. Route
	// such effects through WithOnCreate, which defers to the post-commit flush and
	// so never runs on a rollback. The bool remains a correct after-the-fact report
	// once the outermost transaction commits.
	return obj, created, nil
}

func (c *clientImpl[Spec, Status]) Update(ctx context.Context, id ObjectID, spec Spec) (*Object[Spec, Status], error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	// ObjectsUpdateSpec self-wraps in Within; wrapping it here lets the decode join that
	// same transaction, so a spec that doesn't round-trip rolls the write back (see
	// Create), keeping the prior good spec instead of committing a row the process
	// can't read. Outside a caller's Within this is a standalone tx; nested in one it
	// joins.
	var obj *Object[Spec, Status]
	err = c.bh.store.Within(ctx, func(ctx context.Context) error {
		// ObjectsUpdateSpec folds this client's kind into the write, so a foreign id is
		// rejected at the store (no separate read-then-write to keep atomic);
		// hideWrongKind keeps that foreign id invisible to this single-kind client.
		raw, _, err := c.bh.store.ObjectsUpdateSpec(ctx, c.gk, id, b, migratorSpecVersion(c.bh.migratorFor(c.gk)))
		if err = c.hideWrongKind(err); err != nil {
			return err
		}
		obj, err = c.decode(raw)
		return err
	})
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (c *clientImpl[Spec, Status]) Get(ctx context.Context, id ObjectID, loads ...LoadOption) (*Object[Spec, Status], error) {
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

// scopedGet loads id and confirms it belongs to this client's kind. A Client is
// the surface for a single resource kind, so an id naming an object of another
// kind must be invisible here — reads, updates, and deletes through this client
// must never touch another controller's rows. On the read path ObjectsGet isn't
// kind-scoped, so the client checks here, reporting ErrNotFound for a foreign id.
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

// hideWrongKind keeps a foreign id invisible through this single-kind client: the
// scoped store writes reject another kind's object with ErrWrongKind, which the
// client reports as ErrNotFound (mirrors scopedGet on the read path). Any other
// error passes through unchanged.
func (c *clientImpl[Spec, Status]) hideWrongKind(err error) error {
	if errors.Is(err, ErrWrongKind) {
		return ErrNotFound
	}
	return err
}

func (c *clientImpl[Spec, Status]) GetBySlug(ctx context.Context, slug string, loads ...LoadOption) (*Object[Spec, Status], error) {
	raw, err := c.bh.store.ObjectsGetBySlug(ctx, c.gk, slug)
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

// loadObjectRelated populates the related-data fields named by set on one object,
// recording each fetched lookup in obj.loaded so the accessors can tell loaded
// from absent. Both client reads and the reconcile decode boundary call it.
// Batched List has its own path (loadListRelated) to avoid an N+1.
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

// fetchOwnerRef resolves id's single owned_by edge. Owner is single (WithOwner
// sets one), so the first row is the owner and ok is false when there is none.
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
// un-decodable row — an un-migratable shape, or a blob written by a newer build
// (downgrade) — is skipped and logged so it can't break the whole read. The
// calling method rides along as a field rather than in the message, so the log
// line groups across call sites. The migrator is invariant for the kind, so it
// is resolved once rather than re-locking the registry on every row.
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

// warnUndecodable is the one quarantine log line, shared by every read path that
// skips a poison row rather than failing (decodeList and the watch poll). Keeping
// the message identical and putting the call site in `op` is what lets the line
// group across those paths.
func (c *clientImpl[Spec, Status]) warnUndecodable(method string, id ObjectID, err error) {
	c.bh.log().Warn("beehive: skipping undecodable object",
		"op", method, "group", c.gk.Group, "kind", c.gk.Kind, "id", id, "err", err)
}

// loadListRelated eager-loads the requested secondary lookups for a whole list
// in one batched store call per relation, scattering results back onto each
// object — the N+1-free counterpart to loadObjectRelated. A nil set is a no-op.
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
		// Events have no batched store primitive (unlike the ref relations), so this
		// is one query per object — the deliberate exception to loadListRelated's
		// batching. Each object's log is retention-bounded; for large lists or
		// filtered reads, prefer the lazy Client.EventsList.
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
// kind guard in front: that guard was a second, blob-bearing store read (the
// full objects row plus its conditions) issued purely to validate group/kind on
// a hot path. We trade it for speed, mirroring the ControllerClient quartet,
// which never checked. The cost of the trade: a foreign id reads that other
// kind's edges and a missing id reads empty — neither surfaces as ErrNotFound —
// so passing another kind's id through this single-kind client is silent misuse
// rather than a clean error.
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

// The kind filter lives in the store statement's WHERE, so foreign-kind children
// never reach Go. See the Client interface for the contract.
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

// reconcilerForObject validates id against this client's kind, then resolves the
// kind's reconciler — the shared gate for the schedule-control methods. scopedGet
// runs first so a missing or foreign id surfaces as ErrNotFound regardless of
// registration; only then is a client-only kind reported as ErrNoController.
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
// unless WithResetBackoff() is passed. See the Client interface for the full contract.
func (c *clientImpl[Spec, Status]) Requeue(ctx context.Context, id ObjectID, opts ...RequeueOption) error {
	r, err := c.reconcilerForObject(ctx, id)
	if err != nil {
		return err
	}
	r.requeue(id, resolveRequeue(opts).resetBackoff)
	return nil
}

// SchedulesGet reports id's Schedule. It reads the in-memory work queue directly,
// with no store lookup and no kind guard: a foreign or missing id just isn't in
// this kind's schedule, and a client-only kind has no reconciler — both fold into
// the zero-value Schedule. The error is reserved for symmetry with the rest of the
// surface and is never returned today; ctx is unused (no I/O). See the Client
// interface for the full contract — notably that it does not account for the
// periodic drivers, none of which schedules a per-id timer.
func (c *clientImpl[Spec, Status]) SchedulesGet(ctx context.Context, id ObjectID) (Schedule, error) {
	r, ok := c.bh.reconcilerFor(c.gk)
	if !ok {
		return Schedule{}, nil // client-only kind: nothing is ever scheduled
	}
	at, _ := r.nextRequeueAt(id) // zero time when no requeue is scheduled
	return Schedule{NextRequeueAt: at}, nil
}

func (c *clientImpl[Spec, Status]) Delete(ctx context.Context, id ObjectID) error {
	// DeletionRequestsCreate bumps resource_version only on a real state change — an
	// idempotent retry leaves it untouched, so no watch poll reports a spurious
	// diff. It folds this client's kind into the write, so a foreign id can't be
	// deleted through this client; hideWrongKind keeps that foreign id invisible.
	_, _, err := c.bh.store.DeletionRequestsCreate(ctx, c.gk, id)
	if err = c.hideWrongKind(err); err != nil {
		return err
	}
	// Nothing is scheduled here. The mark is the signal: a deletion-pending row is
	// what the GC sweeper lists, so the next tick hands it to the controller to
	// clear finalizers, or collects it directly for a client-only kind. That cadence
	// is guaranteed — WithGCInterval refuses to be disabled — so the wait is bounded
	// by one tick, and a retry or post-crash Delete lands in the same listing.
	return nil
}

// DeleteBySlug is Delete keyed by a name rather than a handle; the store resolves
// and marks in one statement. See the Client interface for the full contract.
func (c *clientImpl[Spec, Status]) DeleteBySlug(ctx context.Context, slug string) error {
	// ErrNotFound is unambiguous here — nothing of this kind holds the slug, a foreign
	// kind's included — so it is idempotent success rather than a failure to report.
	// The one place a slug delete departs from Delete, which reports a missing id.
	if _, _, err := c.bh.store.DeletionRequestsCreateBySlug(ctx, c.gk, slug); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil // already gone
		}
		return err
	}
	// Nothing scheduled, as in Delete: the mark is what the GC sweeper lists.
	return nil
}

// EventsList reads id's runs and maps them to public Events. It reads by id
// (not kind-scoped), like the ref lookups.
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
// type, dropping the storage-only bookkeeping (last-transition/updated/observed
// generation) that the user-facing type doesn't carry. Returns nil for none.
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

// eventFromRaw maps a raw event row to the public Event, translating the type
// string and detail bytes and dropping the store-only resource_version cursor
// (the watch layer needs it; the user-facing type doesn't).
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

// eventsFromRaw maps a slice of raw event rows to public Events. Returns nil for none.
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

// convertBlob upgrades a stored JSON blob from its recorded schema version
// (from) to the build's current version, returning the bytes to unmarshal. It is
// the per-blob conversion rule shared by spec and status: a current of 0 (the
// kind isn't versioned, or there's no migrator) or from == current is identity;
// from < current runs convert; from > current is a downgrade — an older build
// reading data a newer one wrote — which we refuse rather than silently truncate,
// surfacing as a quarantine signal upstream.
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
// each blob up from its stored schema version via m before unmarshalling (see
// convertBlob). A nil m means the kind has no migrator: both current versions are
// 0, so every blob is decoded as-is — byte-identical to the pre-migrator path.
func rawToTyped[Spec, Status any](raw *RawObject, m Migrator) (*Object[Spec, Status], error) {
	// The current-version "0 if nil" rule is shared with the write paths via these
	// helpers; the converters are only reached when from < current (never when m is
	// nil), so guarding them once here suffices.
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
		Slug:                raw.Slug,
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
