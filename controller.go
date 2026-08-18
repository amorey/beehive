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
	"sync/atomic"

	"github.com/amorey/beehive/internal/storeapi"
)

// ErrWrongKind is returned by a ControllerClient write whose target id belongs
// to another kind. The store folds the controller's kind into every write, so a
// wrong id fails loudly instead of corrupting another kind's row.
var ErrWrongKind = storeapi.ErrWrongKind

// Controller is the user-supplied reconcile logic for a resource kind.
// Reconcile drives an object toward its desired state; the client is the
// status-write surface for this controller's kind. Build the return with
// Settled, Unsettled or Fail.
type Controller[Spec, Status any] interface {
	Reconcile(ctx context.Context, client ControllerClient[Status], obj *Object[Spec, Status]) ReconcileResult
}

// ControllerClient is the write surface a controller uses to report observed
// state. It writes only Status and metadata — never Spec, which the user owns.
//
// It lives for the one Reconcile it is passed to: afterwards every method
// returns ErrReconcileReturned, and there is no other way to hold one.
type ControllerClient[Status any] interface {
	// AddDependency records that fromID depends on toID, so beehive reconciles
	// fromID again when toID changes. Every call that creates the edge records
	// one owed reconcile for fromID, durably and atomically with the edge, so a
	// declared dependency is a guarantee rather than a subscription.
	// Re-asserting existing edges records nothing.
	AddDependency(ctx context.Context, fromID, toID ObjectID) error
	// AddEvent adds an observation to id's event log. Repeating the latest run's
	// (Category, Type, Reason) extends that run rather than appending, so a
	// controller can report every poll without growing the log per poll.
	AddEvent(ctx context.Context, id ObjectID, event EventSpec) error
	DeleteCondition(ctx context.Context, id ObjectID, conditionType string) error
	DeleteDependency(ctx context.Context, fromID, toID ObjectID) error
	DeleteFinalizer(ctx context.Context, id ObjectID, finalizer string) error
	// GetOwner returns id's owner, if any. ok is false with a nil error when the
	// object has no owner.
	GetOwner(ctx context.Context, id ObjectID) (ObjectRef, bool, error)
	// HasIncomingEdges reports whether any object with a live claim still points
	// at id: an owned child, or a dependent that is not itself being deleted. A
	// finalizer can gate teardown on it.
	HasIncomingEdges(ctx context.Context, id ObjectID) (bool, error)
	// ListDependencies returns the objects id depends on (outgoing depends_on).
	ListDependencies(ctx context.Context, id ObjectID) ([]ObjectRef, error)
	// ListDependents returns the objects that depend on id (incoming depends_on).
	ListDependents(ctx context.Context, id ObjectID) ([]ObjectRef, error)
	// ListOwned returns the objects id owns (its incoming owned_by edges).
	ListOwned(ctx context.Context, id ObjectID) ([]ObjectRef, error)
	// SetCondition writes id's condition of that type. The store stamps
	// TransitionedAt and UpdatedAt; the passed values are ignored.
	SetCondition(ctx context.Context, id ObjectID, condition Condition) error
	// SetConditions writes every one of id's named conditions together, under a
	// single version bump, so a watcher never sees half a pass. A type named
	// twice is refused with ErrDuplicateConditionType; an empty slice writes
	// nothing. Same stamping as SetCondition.
	SetConditions(ctx context.Context, id ObjectID, conditions []Condition) error
	// UpdateStatus records status and nothing else — the handshake is beehive's,
	// recorded by returning Settled. Status that marshals to the stored bytes
	// writes nothing, so a controller can report on every poll.
	UpdateStatus(ctx context.Context, id ObjectID, status Status) error
	// Within runs fn inside a single transaction: writes made with fn's ctx all
	// commit together or roll back on error. Pass fn's ctx to every store call
	// it makes — the store runs on one connection, so any other context
	// deadlocks against the transaction. Watches cannot be opened inside it.
	Within(ctx context.Context, fn func(ctx context.Context) error) error
}

// controllerClientImpl is the ControllerClient one Reconcile is handed. It
// refuses everything once the pass ends, and is a fail-fast rather than a
// barrier: nothing waits for calls already in flight.
// See docs/adr/2026-08-18-a-controller-client-exists-only-for-a-pass.md.
type controllerClientImpl[Status any] struct {
	bh   *Beehive
	gk   GroupKind
	done atomic.Bool
}

// newPassClient builds the client for one pass, and is the only constructor.
func newPassClient[Status any](bh *Beehive, gk GroupKind) *controllerClientImpl[Status] {
	return &controllerClientImpl[Status]{bh: bh, gk: gk}
}

// writes is the client's kind-scoped write core. Derived rather than stored: a
// second copy of bh and gk is a second thing to keep in step.
func (c *controllerClientImpl[Status]) writes() kindWriter { return kindWriter{c.bh, c.gk} }

// Called once, after Reconcile returns and before beehive's own writes.
func (c *controllerClientImpl[Status]) end() { c.done.Store(true) }

// live gates every exported method below, SetCondition excepted: it delegates
// to SetConditions, which owns the gate for both.
func (c *controllerClientImpl[Status]) live() error {
	if c.done.Load() {
		return ErrReconcileReturned
	}
	return nil
}

// The store folds the controller's kind into each write below, so another
// kind's id is rejected with ErrWrongKind and a missing id with ErrNotFound.
// Each mutator self-wraps in Within, joining the controller's own Within when
// nested.

// kindWriter is the non-generic core of a write to one kind: the store call and
// the watch wake that must follow it. The pass client and the beehivetest seam
// both write through it, so the wake obligation is discharged in one place.
type kindWriter struct {
	bh *Beehive
	gk GroupKind
}

// wakeAfter returns a store write's error, waking the kind's watches when the
// write succeeded. Every write that appends to the object write log ends with
// it: forgetting the wake costs staleness rather than a failure, so nothing
// else would catch it — up to the watch floor for a subscriber, and up to the
// stale-dependents pass for a dependent, since the waker has no tick behind it.
//
// AddEvent is the one that does not, and must not: an event bumps no
// resource_version, so it appends no entry for a watch to read.
func (w kindWriter) wakeAfter(ctx context.Context, err error) error {
	if err != nil {
		return err
	}
	w.bh.signalKindWritten(ctx, w.gk)
	return nil
}

// conditionsToRaw is the write-side conversion. Unconfirmed and the two stamps
// are set by the store on read and ignored on write, so they are not carried.
func conditionsToRaw(conditions []Condition) []storeapi.Condition {
	conds := make([]storeapi.Condition, len(conditions))
	for i, condition := range conditions {
		conds[i] = storeapi.Condition{
			Type:     condition.Type,
			Status:   string(condition.Status),
			Reason:   condition.Reason,
			Message:  condition.Message,
			Liveness: condition.Liveness,
		}
	}
	return conds
}

func (w kindWriter) DeleteCondition(ctx context.Context, id ObjectID, conditionType string) error {
	return w.wakeAfter(ctx, w.bh.store.Conditions().Delete(ctx, w.gk, id, conditionType))
}

func (w kindWriter) SetConditions(ctx context.Context, id ObjectID, conds ...storeapi.Condition) error {
	return w.wakeAfter(ctx, w.bh.store.Conditions().Set(ctx, w.gk, id, conds...))
}

func (w kindWriter) UpdateStatus(ctx context.Context, id ObjectID, status []byte) error {
	return w.wakeAfter(ctx, w.bh.store.Objects().UpdateStatus(
		ctx, w.gk, id, status, migratorStatusVersion(w.bh.migratorFor(w.gk))))
}

func (c *controllerClientImpl[Status]) UpdateStatus(ctx context.Context, id ObjectID, status Status) error {
	if err := c.live(); err != nil {
		return err
	}
	b, err := json.Marshal(status)
	if err != nil {
		return err
	}
	return c.writes().UpdateStatus(ctx, id, b)
}

func (c *controllerClientImpl[Status]) SetCondition(ctx context.Context, id ObjectID, condition Condition) error {
	return c.SetConditions(ctx, id, []Condition{condition})
}

func (c *controllerClientImpl[Status]) SetConditions(ctx context.Context, id ObjectID, conditions []Condition) error {
	if err := c.live(); err != nil {
		return err
	}
	if len(conditions) == 0 {
		return nil
	}
	return c.writes().SetConditions(ctx, id, conditionsToRaw(conditions)...)
}

func (c *controllerClientImpl[Status]) DeleteCondition(ctx context.Context, id ObjectID, conditionType string) error {
	if err := c.live(); err != nil {
		return err
	}
	return c.writes().DeleteCondition(ctx, id, conditionType)
}

func (c *controllerClientImpl[Status]) AddEvent(ctx context.Context, id ObjectID, event EventSpec) error {
	if err := c.live(); err != nil {
		return err
	}
	var detail []byte
	if event.Detail != nil {
		var err error
		if detail, err = json.Marshal(event.Detail); err != nil {
			return err
		}
	}
	if err := c.bh.store.Events().Add(ctx, c.gk, id, storeapi.EventsAddInput{
		Category: event.Category,
		Type:     string(event.Type),
		Reason:   event.Reason,
		Message:  event.Message,
		Detail:   detail,
	}); err != nil {
		return err
	}
	c.bh.signalEventsWritten(ctx, id)
	return nil
}

// Clearing the last finalizer on a deleting row pushes the collect it unblocks;
// gcCollect still re-checks the RESTRICT block. See
// docs/adr/2026-08-05-a-cleared-finalizer-pushes-its-own-collect.md.
func (c *controllerClientImpl[Status]) DeleteFinalizer(ctx context.Context, id ObjectID, finalizer string) error {
	if err := c.live(); err != nil {
		return err
	}
	clearedLast, err := c.bh.store.Objects().DeleteFinalizer(ctx, c.gk, id, finalizer)
	if err := c.writes().wakeAfter(ctx, err); err != nil {
		return err
	}
	if clearedLast {
		c.bh.signalRequeueNow(ctx, ObjectRef{ID: id, Group: c.gk.Group, Kind: c.gk.Kind})
	}
	return nil
}

// AddDependency is one store call, not a composition: the edge and the durable
// reconcile-owed stamp are indivisible inside Edges().Add, so an edge can never
// commit without its wake. The enqueue below is the prompt half; the stamp is
// the guarantee. It is gated on the store reporting the edge as new — which
// bounds it to one enqueue per edge ever created — and routed by res.From
// because the edge is cross-kind.
func (c *controllerClientImpl[Status]) AddDependency(ctx context.Context, fromID, toID ObjectID) error {
	if err := c.live(); err != nil {
		return err
	}
	res, err := c.bh.store.Edges().Add(ctx, fromID, toID, RelationDependsOn)
	if err != nil {
		return err
	}
	if res.ReconcileOwedStamped {
		// Throttled: a controller can declare on every pass, and the stamp is
		// durable, so this must not jump the source's backoff ladder.
		c.bh.signalRequeueThrottled(ctx, ObjectRef{ID: fromID, Group: res.From.Group, Kind: res.From.Kind})
	}
	return nil
}

// DeleteDependency drops the edge and pushes toID's collect when the drop
// lifted its RESTRICT block; gcCollect still re-checks it. Routed by res.To,
// because the edge is cross-kind. See
// docs/adr/2026-08-05-a-dropped-dependency-pushes-its-target.md.
func (c *controllerClientImpl[Status]) DeleteDependency(ctx context.Context, fromID, toID ObjectID) error {
	if err := c.live(); err != nil {
		return err
	}
	res, err := c.bh.store.Edges().Delete(ctx, fromID, toID, RelationDependsOn)
	if err != nil {
		return err
	}
	if res.Unblocked {
		c.bh.signalRequeueNow(ctx, ObjectRef{ID: toID, Group: res.To.Group, Kind: res.To.Kind})
	}
	return nil
}

// The ref reads below are plain edge queries with no kind scoping: a controller
// reasons about its own object's relationships. To gate a write on
// HasIncomingEdges atomically, run both inside Within.

func (c *controllerClientImpl[Status]) GetOwner(ctx context.Context, id ObjectID) (ObjectRef, bool, error) {
	if err := c.live(); err != nil {
		return ObjectRef{}, false, err
	}
	return fetchOwnerRef(ctx, c.bh.store, id)
}

func (c *controllerClientImpl[Status]) ListDependencies(ctx context.Context, id ObjectID) ([]ObjectRef, error) {
	if err := c.live(); err != nil {
		return nil, err
	}
	return c.bh.store.Edges().ListOutgoingByRelation(ctx, id, RelationDependsOn)
}

func (c *controllerClientImpl[Status]) ListDependents(ctx context.Context, id ObjectID) ([]ObjectRef, error) {
	if err := c.live(); err != nil {
		return nil, err
	}
	return c.bh.store.Edges().ListIncoming(ctx, id, RelationDependsOn)
}

func (c *controllerClientImpl[Status]) ListOwned(ctx context.Context, id ObjectID) ([]ObjectRef, error) {
	if err := c.live(); err != nil {
		return nil, err
	}
	return c.bh.store.Edges().ListIncoming(ctx, id, RelationOwnedBy)
}

func (c *controllerClientImpl[Status]) HasIncomingEdges(ctx context.Context, id ObjectID) (bool, error) {
	if err := c.live(); err != nil {
		return false, err
	}
	return c.bh.store.Edges().HasIncoming(ctx, id)
}

// Within groups writes into one transaction. It adds no kind scoping of its
// own — each write still folds the controller's kind in — so grouping never
// widens what this controller can mutate.
func (c *controllerClientImpl[Status]) Within(ctx context.Context, fn func(ctx context.Context) error) error {
	if err := c.live(); err != nil {
		return err
	}
	return c.bh.store.Within(ctx, fn)
}
