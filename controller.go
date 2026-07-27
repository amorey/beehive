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

	"github.com/amorey/beehive/internal/storeapi"
)

// ErrWrongKind is returned by a ControllerClient write when the target id names
// an object of a different kind than the controller's own. A controller may only
// write status, conditions, and finalizers on objects of its registered kind;
// passing an id from another kind (a dependency, an owner) is a bug that would
// otherwise persist this controller's Status JSON into a foreign row and make
// later typed reads of that kind fail to decode. The store folds the controller's
// kind into each write, turning that silent corruption into a loud, retrying
// reconcile failure. Aliased from storeapi like ErrNotFound.
var ErrWrongKind = storeapi.ErrWrongKind

// ErrTargetResourceVersionFuture is returned by DependenciesAdd when
// targetResourceVersion exceeds the target's current ResourceVersion. An object's
// version only moves forward, so a version above the target's own cannot have come
// from reading it — the caller passed some other object's version, or some other
// field. The declaration is rejected rather than silently ignored: the guard that
// value drives would otherwise never fire, leaving the read-then-declare window
// open with nothing to say so. The store checks it before inserting, so a rejected
// call writes nothing. Aliased from storeapi like ErrNotFound.
var ErrTargetResourceVersionFuture = storeapi.ErrTargetResourceVersionFuture

// Controller is the user-supplied reconcile logic for a resource kind.
// Reconcile is called to drive an object toward its desired state; the client
// is the status-write surface for this controller's kind. Controllers own no
// lifecycle in beehive — any background work belongs to the embedding
// application, which obtains a ControllerClient from Register.
type Controller[Spec, Status any] interface {
	Reconcile(ctx context.Context, client ControllerClient[Status], obj *Object[Spec, Status]) (Result, error)
}

// ControllerClient is the write surface a controller uses to report observed
// state. It only writes Status and metadata — never Spec, which the user owns.
type ControllerClient[Status any] interface {
	ConditionsDelete(ctx context.Context, id ObjectID, conditionType string) error
	ConditionsSet(ctx context.Context, id ObjectID, condition Condition) error
	// DependenciesAdd records that fromID depends on toID, so Beehive requeues
	// fromID when toID changes.
	//
	// targetResourceVersion is the version of toID that the decision to depend on
	// it was based on — the ResourceVersion of the object you read, not toID's
	// current one. Re-reading toID immediately before this call to obtain it
	// defeats the purpose: a version fresher than the read claims to have seen
	// changes the decision did not.
	//
	// It closes the read-then-declare window. A change to toID landing between the
	// read and the declaration reaches nobody — the dependency waker resolves
	// dependents at the instant of the change, and the edge does not exist yet —
	// and the dependent then settles at its own generation on the stale read,
	// where ObjectsListUnsettledIDs structurally cannot see it. So if toID has moved past
	// targetResourceVersion by the time the edge commits, fromID is requeued: it
	// declared a dependency on a version that is already superseded.
	//
	// Pass 0 to skip the check — "no opinion", the correct value when the edge is
	// declared *before* reading toID, which is already race-free (the edge is in
	// place, so the waker covers every later change).
	//
	// A targetResourceVersion above toID's current version is rejected with
	// ErrTargetResourceVersionFuture, and rejected before anything is written, so
	// no edge is declared — including when this call is nested in a caller's
	// Within, where returning an error unwinds nothing on its own. Versions only
	// move forward, so such a value cannot have come from reading toID. That
	// catches the value taken from the wrong object,
	// but not one taken from the right object at the wrong time — a stale version
	// is indistinguishable from an old read, and a freshly re-read one from a
	// decision made this instant. Those stay the caller's contract to honour.
	//
	// The requeue fires at most once per edge: it is gated on this call being the
	// one that created the edge. So a caller that re-asserts its edges each pass
	// costs nothing after the first, and one that passes a stale version — a value
	// cached across passes, say — gets at most one spurious requeue rather than a
	// self-sustaining loop.
	//
	// The cost of that bound is that this cannot repair a wake lost elsewhere. Once
	// the edge exists, delivering toID's changes is the dependency waker's job, and
	// the waker can drop one (a swallowed lookup error, a dead subscription, a
	// process that exits before dispatching — see TODO.md). In that case a later
	// pass re-declaring the edge would have the evidence to notice, and this
	// deliberately does not act on it. Covering it means waking whenever the target
	// moved, which for a caller whose version never advances is an unbounded loop —
	// a worse failure than the miss, and not one the caller can see. Repairing lost
	// wakes belongs to a backstop that derives staleness rather than to a guard that
	// records an intent.
	DependenciesAdd(ctx context.Context, fromID, toID ObjectID, targetResourceVersion int64) error
	DependenciesDelete(ctx context.Context, fromID, toID ObjectID) error
	// DependenciesList returns the objects id depends on (outgoing depends_on).
	DependenciesList(ctx context.Context, id ObjectID) ([]Ref, error)
	// DependentsList returns the objects that depend on id (incoming depends_on).
	DependentsList(ctx context.Context, id ObjectID) ([]Ref, error)
	// EventsRecord appends an observation to id's event log, aggregating into
	// contiguous runs (see EventSpec). Like the other writes it is kind-folded and
	// composes in Within, so a controller can record an event and update a
	// condition atomically.
	EventsRecord(ctx context.Context, id ObjectID, event EventSpec) error
	FinalizersDelete(ctx context.Context, id ObjectID, finalizer string) error
	// OwnedList returns the objects id owns (its incoming owned_by edges).
	OwnedList(ctx context.Context, id ObjectID) ([]Ref, error)
	// OwnersGet returns id's owner, if any (its outgoing owned_by edge). ok reports
	// presence: false with a nil error when the object has no owner. The lazy
	// counterpart to a reconciler's LoadOwner default.
	OwnersGet(ctx context.Context, id ObjectID) (Ref, bool, error)
	// EdgesHasIncoming reports whether any object with a live claim still points at id:
	// an owned child, or a dependent that is not itself being deleted. A dependent
	// that is itself finalizing is excluded — it's going away and no longer has a
	// claim. A finalizer can gate teardown on this: a controller holding a shared
	// resource clears its finalizer only once nothing with a live claim references
	// the object, so the resource outlives its last real user.
	EdgesHasIncoming(ctx context.Context, id ObjectID) (bool, error)
	// UpdateStatus records status and the generation this reconcile observed.
	// Status that marshals to the stored bytes writes nothing: no
	// resource_version bump and no Modified event, so a controller can report
	// unconditionally without waking watchers (or dependents) on an unchanged
	// poll. The exception is the generation handshake — if this reconcile
	// settled a generation the object hadn't settled at before, that advance is
	// recorded and does emit, so watchers see the object converge.
	UpdateStatus(ctx context.Context, id ObjectID, observedGeneration int64, status Status) error
	// Within runs fn inside a single transaction: the ControllerClient writes fn
	// makes (with the ctx passed to it) all commit together on a nil return, or all
	// roll back on error. Reconcile itself is not transactional — each write
	// otherwise commits on its own — so a controller uses Within only for the
	// writes that must be atomic. The transaction holds the store's write lock for
	// fn's whole duration, so keep external I/O outside it.
	Within(ctx context.Context, fn func(ctx context.Context) error) error
}

// controllerClientImpl is the status-writing surface for a controller's kind.
// It is constructed once at Register, passed into each Reconcile, and returned
// to the embedding application so it can write status from its own goroutines.
type controllerClientImpl[Status any] struct {
	bh *Beehive
	gk GroupKind
}

// The store folds the controller's kind into each write below: a foreign id
// (a dependency, an owner) matches no row and is rejected with ErrWrongKind, so
// this controller's status/condition/finalizer writes can never corrupt another
// kind's row. There's no separate kind check to keep atomic with the write, so
// each mutator self-wraps in Within (joining the controller's own Within when
// nested) — the per-write withinKind transaction this used to need is gone. A
// missing id surfaces as ErrNotFound (the store distinguishes it from a foreign id).

func (c *controllerClientImpl[Status]) UpdateStatus(ctx context.Context, id ObjectID, observedGeneration int64, status Status) error {
	b, err := json.Marshal(status)
	if err != nil {
		return err
	}
	// The store's UpdateStatus emits the Modified event into its transaction's
	// collector, so it's published only after the write commits.
	_, err = c.bh.store.ObjectsUpdateStatus(ctx, c.gk, id, observedGeneration, b, migratorStatusVersion(c.bh.migratorFor(c.gk)))
	return err
}

func (c *controllerClientImpl[Status]) ConditionsSet(ctx context.Context, id ObjectID, condition Condition) error {
	_, err := c.bh.store.ConditionsSet(ctx, c.gk, id, storeapi.Condition{
		Type:     condition.Type,
		Status:   string(condition.Status),
		Reason:   condition.Reason,
		Message:  condition.Message,
		Liveness: condition.Liveness,
	})
	return err
}

func (c *controllerClientImpl[Status]) ConditionsDelete(ctx context.Context, id ObjectID, conditionType string) error {
	_, err := c.bh.store.ConditionsDelete(ctx, c.gk, id, conditionType)
	return err
}

// EventsRecord marshals the event's optional Detail (typed-in, opaque-out, like
// Spec/Status) and appends the run through the store, which folds in the
// controller's kind and emits the run into the transaction's collector so it
// publishes to watchers only after the write commits. A nil Detail stays nil (no
// payload); the store aggregates by (Category, Type, Reason).
func (c *controllerClientImpl[Status]) EventsRecord(ctx context.Context, id ObjectID, event EventSpec) error {
	var detail []byte
	if event.Detail != nil {
		var err error
		if detail, err = json.Marshal(event.Detail); err != nil {
			return err
		}
	}
	_, err := c.bh.store.EventsRecord(ctx, c.gk, id, storeapi.Event{
		Category: event.Category,
		Type:     string(event.Type),
		Reason:   event.Reason,
		Message:  event.Message,
		Detail:   detail,
	})
	return err
}

func (c *controllerClientImpl[Status]) FinalizersDelete(ctx context.Context, id ObjectID, finalizer string) error {
	_, err := c.bh.store.FinalizersDelete(ctx, c.gk, id, finalizer)
	return err
}

// DependenciesAdd implements the contract documented on ControllerClient. The
// relation is always "depends_on" (owner edges come from WithOwner at create
// time). Both writes — the edge and the durable wake stamp — live inside EdgesAdd,
// which is atomic on its own, so the Within here is not what makes either safe;
// it is only the seam for this method's own composition, joining a controller's
// Within when nested rather than opening a second transaction.
//
// They are in the store rather than sequenced here precisely because a nested
// Within unwinds nothing: a stamp issued as a second call after EdgesAdd returned
// would leave a caller who handles this method's error free to commit the edge
// without it — a dependent stranded on a stale read, which is the race this
// method exists to close. Ordering inside one store call is the guarantee (see
// EdgesAdd), and WakeStamped reports what it did rather than having the conjunction
// recomputed here, where the two halves could drift apart.
//
// The wake is a conjunction — the edge is new *and* the target moved — and both
// halves are load-bearing: either alone re-fires every pass for some ordinary
// controller, and nothing throttles that (the dispatch path has no already-settled
// skip, and workQueue.addLocked has no rate limiter). TODO.md records the two
// rejected one-sided guards; the interface doc covers what the conjunction gives
// up. The edge-new half costs nothing, riding the stamp statement's own NOT EXISTS.
//
// The requeue is routed by fromID's own GroupKind, not the caller's — the edge is
// deliberately cross-kind, so a controller may declare one on another kind's
// behalf, and enqueuing a foreign id onto the caller's reconciler would decode
// another kind's bytes as this one's Spec. wakeAfterCommit (not the
// reconcile-scoped pendingWakes, which is nil for the out-of-band call) registers
// it post-commit, so it can't reach a controller before the edge it is about, or
// at all if the transaction rolls back. It is in-memory, and deliberately the
// expendable half: a process that dies before it runs still finds the stamp on
// restart, and the backstop that drains pending_wake makes the reconcile happen
// late rather than never.
func (c *controllerClientImpl[Status]) DependenciesAdd(ctx context.Context, fromID, toID ObjectID, targetResourceVersion int64) error {
	return c.bh.store.Within(ctx, func(ctx context.Context) error {
		// The store rejects a version above the target's own before it inserts (see
		// ErrTargetResourceVersionFuture), so a bad claim leaves no edge regardless
		// of whose transaction this is running in.
		res, err := c.bh.store.EdgesAdd(ctx, fromID, toID, RelationDependsOn, targetResourceVersion)
		if err != nil {
			return err
		}
		if res.WakeStamped {
			// The durable half already landed with the edge (pending_wake is a count of
			// outstanding wakes, decremented by the reconcile that services one, so a
			// wake owed mid-pass is not lost — see the pending_wake column). This is the
			// latency half: the in-memory requeue, so the dependent reconciles now rather
			// than waiting for the backstop. It self-gates on registration via
			// enqueueIfRegistered — which is also why the stamp doesn't need gating: an
			// unregistered kind simply owes a count nothing scans.
			c.bh.wakeAfterCommit(ctx, res.From, fromID)
		}
		return nil
	})
}

func (c *controllerClientImpl[Status]) DependenciesDelete(ctx context.Context, fromID, toID ObjectID) error {
	return c.bh.store.Within(ctx, func(ctx context.Context) error {
		if err := c.bh.store.EdgesDelete(ctx, fromID, toID, RelationDependsOn); err != nil {
			return err
		}
		// Removing the edge can unblock toID's physical deletion (refs are RESTRICT).
		// If toID is finalizing, register it for a post-commit re-check so GC removes
		// it without waiting on the resync backstop (which may be disabled). Outside a
		// reconcile there's no collector — nothing to schedule.
		wakes := pendingWakesFrom(ctx)
		if wakes == nil {
			return nil
		}
		target, err := c.bh.store.ObjectsGetMeta(ctx, toID)
		if errors.Is(err, ErrNotFound) {
			return nil // target already gone
		}
		if err != nil {
			return err
		}
		if target.DeletionRequestedAt != nil {
			wakes.targets = append(wakes.targets, Ref{ID: toID, Group: target.Group, Kind: target.Kind})
		}
		return nil
	})
}

// EdgesHasIncoming reports whether anything still claims id. It is a plain read that
// commits on its own; to gate a write on it atomically — e.g. clearing a finalizer
// only if nothing references the object — a controller runs both inside Within, so
// the read and the write share one transaction snapshot.
// OwnersGet/DependenciesList/DependentsList/OwnedList read ref edges directly,
// like EdgesHasIncoming above — no kind-scoping, since a controller reasons about
// its own object's relationships.
func (c *controllerClientImpl[Status]) OwnersGet(ctx context.Context, id ObjectID) (Ref, bool, error) {
	return fetchOwnerRef(ctx, c.bh.store, id)
}

func (c *controllerClientImpl[Status]) DependenciesList(ctx context.Context, id ObjectID) ([]Ref, error) {
	return c.bh.store.EdgesListOutgoingByRelation(ctx, id, RelationDependsOn)
}

func (c *controllerClientImpl[Status]) DependentsList(ctx context.Context, id ObjectID) ([]Ref, error) {
	return c.bh.store.EdgesListIncoming(ctx, id, RelationDependsOn)
}

func (c *controllerClientImpl[Status]) OwnedList(ctx context.Context, id ObjectID) ([]Ref, error) {
	return c.bh.store.EdgesListIncoming(ctx, id, RelationOwnedBy)
}

func (c *controllerClientImpl[Status]) EdgesHasIncoming(ctx context.Context, id ObjectID) (bool, error) {
	return c.bh.store.EdgesHasIncoming(ctx, id)
}

// Within opens a transaction and runs fn under it; the ControllerClient writes fn
// makes commit together on a nil return or roll back on error. Each write's own
// store.Within nests into this one (joining via the ctx's txKey), so they share
// the single transaction rather than autocommitting independently.
//
// Within adds no kind scoping of its own — it takes no id and groups arbitrary
// writes (a controller may legitimately touch other kinds here, e.g. read a
// dependency then clear its own finalizer). The kind boundary is still enforced
// per write: each status/condition/finalizer write folds the controller's kind
// into the store mutator, so grouping them in a transaction never widens what
// this controller can mutate.
func (c *controllerClientImpl[Status]) Within(ctx context.Context, fn func(ctx context.Context) error) error {
	return c.bh.store.Within(ctx, fn)
}
