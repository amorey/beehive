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

// ErrWrongKind is returned by an id-keyed write whose target belongs to another
// kind. The store folds the caller's kind into every write, so a wrong id fails
// loudly instead of corrupting another kind's row.
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
// It is bound to the object its Reconcile was handed and acts on that object
// alone; the ids below are the other end of an edge. It lives for the one
// Reconcile it is passed to: afterwards every method returns
// ErrReconcileReturned, and there is no other way to hold one.
type ControllerClient[Status any] interface {
	// AddDependency records that this pass's object depends on toID, so beehive
	// reconciles it again when toID changes. Every call that creates the edge
	// records one owed reconcile, durably and atomically with the edge, so a
	// declared dependency is a guarantee rather than a subscription.
	// Re-asserting existing edges records nothing.
	AddDependency(ctx context.Context, toID ObjectID) error
	// AddEvent adds an observation to the object's event log. Repeating the
	// latest run's (Category, Type, Reason) extends that run rather than
	// appending, so a controller can report every poll without growing the log
	// per poll.
	AddEvent(ctx context.Context, event EventSpec) error
	DeleteCondition(ctx context.Context, conditionType string) error
	DeleteDependency(ctx context.Context, toID ObjectID) error
	DeleteFinalizer(ctx context.Context, finalizer string) error
	// GetOwner returns the object's owner, if any. ok is false with a nil error
	// when it has none.
	GetOwner(ctx context.Context) (ObjectRef, bool, error)
	// HasIncomingEdges reports whether any object with a live claim still points
	// at this pass's object: an owned child, or a dependent that is not itself
	// being deleted. A finalizer can gate teardown on it.
	HasIncomingEdges(ctx context.Context) (bool, error)
	// ListDependencies returns the objects this one depends on (outgoing
	// depends_on).
	ListDependencies(ctx context.Context) ([]ObjectRef, error)
	// ListDependents returns the objects that depend on this one (incoming
	// depends_on).
	ListDependents(ctx context.Context) ([]ObjectRef, error)
	// ListOwned returns the objects this one owns (its incoming owned_by edges).
	ListOwned(ctx context.Context) ([]ObjectRef, error)
	// SetCondition writes the condition of that type. The store stamps
	// TransitionedAt and UpdatedAt; the passed values are ignored.
	SetCondition(ctx context.Context, condition Condition) error
	// SetConditions writes every named condition together, under a single version
	// bump, so a watcher never sees half a pass. A type named twice is refused
	// with ErrDuplicateConditionType, and one that is not valid UTF-8 with
	// ErrInvalidConditionType; an empty slice writes nothing. Same stamping as
	// SetCondition.
	SetConditions(ctx context.Context, conditions []Condition) error
	// UpdateStatus records status and nothing else — the handshake is beehive's,
	// recorded by returning Settled. Status that marshals to the bytes this pass
	// was loaded with writes nothing and reaches no store, so a controller can
	// report on every poll. That skip reads nothing, so it cannot report an
	// object collected mid-pass: it returns nil where a write returns ErrNotFound.
	UpdateStatus(ctx context.Context, status Status) error
	// Within runs fn inside a single transaction: writes made with fn's ctx all
	// commit together or roll back on error. Pass fn's ctx to every store call
	// it makes. A read on any other ctx does not join the transaction, and reads
	// run on their own connection, so it quietly returns state from before the
	// transaction rather than failing. Watches cannot be opened inside it.
	//
	// fn's ctx belongs to one goroutine. Issuing store calls on it from two at
	// once interleaves them on one prepared statement's cursor: each sees part of
	// the other's rows, and neither reports an error.
	Within(ctx context.Context, fn func(ctx context.Context) error) error
}

// controllerClientImpl is the ControllerClient one Reconcile is handed. It
// refuses everything once the pass ends, and is a fail-fast rather than a
// barrier: nothing waits for calls already in flight.
// See docs/adr/2026-08-18-a-controller-client-exists-only-for-a-pass.md.
type controllerClientImpl[Status any] struct {
	bh   *Beehive
	gk   GroupKind
	id   ObjectID
	done atomic.Bool

	// nil for a client built outside a pass, which then never skips.
	baseline *statusBaseline
}

// newPassClient is the only constructor. The pass ends its client; AdminClient
// builds one per call and ends none, so live() always passes there.
func newPassClient[Status any](bh *Beehive, gk GroupKind, id ObjectID) *controllerClientImpl[Status] {
	return &controllerClientImpl[Status]{bh: bh, gk: gk, id: id}
}

// passClients builds pass clients for one kind, binding an object per call, for
// the surfaces that take an id where a pass does not.
type passClients[Status any] struct {
	bh *Beehive
	gk GroupKind
}

func (p passClients[Status]) at(id ObjectID) *controllerClientImpl[Status] {
	return newPassClient[Status](p.bh, p.gk, id)
}

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

// The store folds the controller's kind into each write below; a missing id is
// ErrNotFound, which is what a row collected mid-pass reads as. Each mutator
// self-wraps in Within, joining the controller's own Within when nested.

// wakeAfter returns a store write's error, waking the kind's watches when the
// write succeeded. Every mutator here that appends to the object write log ends
// with it: forgetting the wake costs staleness rather than a failure, so nothing
// else would catch it — up to the watch floor for a subscriber, and up to the
// stale-dependents pass for a dependent, since the waker has no tick behind it.
//
// Two do not. AddEvent must not: an event bumps no resource_version, so it
// appends no entry for a watch to read. UpdateStatus signals inline instead,
// because it wakes on what the store reported writing rather than on a nil
// error — the two part company when identical bytes are re-reported.
func (c *controllerClientImpl[Status]) wakeAfter(ctx context.Context, err error) error {
	if err != nil {
		return err
	}
	c.bh.signalKindWritten(ctx, c.gk)
	return nil
}

func (c *controllerClientImpl[Status]) UpdateStatus(ctx context.Context, status Status) error {
	if err := c.live(); err != nil {
		return err
	}
	b, err := json.Marshal(status)
	if err != nil {
		return err
	}
	version := c.statusVersion()
	if !c.baseline.claim(b, version) {
		// The store would compare these and write nothing. Its cancellation check
		// goes with it, so make it here: which path a caller takes depends on the
		// bytes, and the two must not disagree about a dead context.
		return ctx.Err()
	}
	changed, err := c.bh.store.Objects().UpdateStatus(ctx, c.gk, c.id, b, version)
	if err != nil || !changed {
		// Neither promotes, so this pass keeps reaching the store.
		return err // nothing written, so no resource_version bump and nothing to wake
	}
	if bl := c.baseline; bl != nil {
		// Promote only at commit: a write rolled back inside the caller's Within
		// never landed.
		c.bh.store.AfterCommit(ctx, func(context.Context) { bl.promote(b, version) })
	}
	c.bh.signalKindWritten(ctx, c.gk)
	return nil
}

// statusVersion is the status schema version this build writes. A pass resolves
// it once, when its baseline is built: migratorFor takes the beehive-wide lock,
// which the skip path must not.
func (c *controllerClientImpl[Status]) statusVersion() int {
	if c.baseline != nil {
		return c.baseline.writeVersion
	}
	return migratorStatusVersion(c.bh.migratorFor(c.gk))
}

func (c *controllerClientImpl[Status]) SetCondition(ctx context.Context, condition Condition) error {
	return c.SetConditions(ctx, []Condition{condition})
}

func (c *controllerClientImpl[Status]) SetConditions(ctx context.Context, conditions []Condition) error {
	if err := c.live(); err != nil {
		return err
	}
	if len(conditions) == 0 {
		return nil
	}
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
	return c.wakeAfter(ctx, c.bh.store.Conditions().Set(ctx, c.gk, c.id, conds...))
}

func (c *controllerClientImpl[Status]) DeleteCondition(ctx context.Context, conditionType string) error {
	if err := c.live(); err != nil {
		return err
	}
	return c.wakeAfter(ctx, c.bh.store.Conditions().Delete(ctx, c.gk, c.id, conditionType))
}

func (c *controllerClientImpl[Status]) AddEvent(ctx context.Context, event EventSpec) error {
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
	if err := c.bh.store.Events().Add(ctx, c.gk, c.id, storeapi.EventsAddInput{
		Category: event.Category,
		Type:     string(event.Type),
		Reason:   event.Reason,
		Message:  event.Message,
		Detail:   detail,
	}); err != nil {
		return err
	}
	c.bh.signalEventsWritten(ctx, c.id)
	return nil
}

// Clearing the last finalizer on a deleting row pushes the collect it unblocks;
// gcCollect still re-checks the RESTRICT block. See
// docs/adr/2026-08-05-a-cleared-finalizer-pushes-its-own-collect.md.
func (c *controllerClientImpl[Status]) DeleteFinalizer(ctx context.Context, finalizer string) error {
	if err := c.live(); err != nil {
		return err
	}
	clearedLast, err := c.bh.store.Objects().DeleteFinalizer(ctx, c.gk, c.id, finalizer)
	if err := c.wakeAfter(ctx, err); err != nil {
		return err
	}
	if clearedLast {
		c.bh.signalRequeueNow(ctx, ObjectRef{ID: c.id, Group: c.gk.Group, Kind: c.gk.Kind})
	}
	return nil
}

// AddDependency is one store call, not a composition: the edge and the durable
// reconcile-owed stamp are indivisible inside Edges().Add, so an edge can never
// commit without its wake. The enqueue below is the prompt half; the stamp is
// the guarantee. It is gated on the store reporting the edge as new, which
// bounds it to one enqueue per edge ever created.
func (c *controllerClientImpl[Status]) AddDependency(ctx context.Context, toID ObjectID) error {
	if err := c.live(); err != nil {
		return err
	}
	res, err := c.bh.store.Edges().Add(ctx, c.id, toID, RelationDependsOn)
	if err != nil {
		return err
	}
	if res.ReconcileOwedStamped {
		// Throttled: a controller can declare on every pass, and the stamp is
		// durable, so this must not jump the source's backoff ladder.
		c.bh.signalRequeueThrottled(ctx, ObjectRef{ID: c.id, Group: c.gk.Group, Kind: c.gk.Kind})
	}
	return nil
}

// DeleteDependency drops the edge and pushes toID's collect when the drop
// lifted its RESTRICT block; gcCollect still re-checks it. Routed by res.To,
// because the edge is cross-kind. See
// docs/adr/2026-08-05-a-dropped-dependency-pushes-its-target.md.
func (c *controllerClientImpl[Status]) DeleteDependency(ctx context.Context, toID ObjectID) error {
	if err := c.live(); err != nil {
		return err
	}
	res, err := c.bh.store.Edges().Delete(ctx, c.id, toID, RelationDependsOn)
	if err != nil {
		return err
	}
	if res.Unblocked {
		c.bh.signalRequeueNow(ctx, ObjectRef{ID: toID, Group: res.To.Group, Kind: res.To.Kind})
	}
	return nil
}

// The ref reads below are plain edge queries with no kind scoping: the id is the
// pass's own, whatever kind the other end belongs to. To gate a write on
// HasIncomingEdges atomically, run both inside Within.

func (c *controllerClientImpl[Status]) GetOwner(ctx context.Context) (ObjectRef, bool, error) {
	if err := c.live(); err != nil {
		return ObjectRef{}, false, err
	}
	return fetchOwnerRef(ctx, c.bh.store, c.id)
}

func (c *controllerClientImpl[Status]) ListDependencies(ctx context.Context) ([]ObjectRef, error) {
	if err := c.live(); err != nil {
		return nil, err
	}
	return c.bh.store.Edges().ListOutgoingByRelation(ctx, c.id, RelationDependsOn)
}

func (c *controllerClientImpl[Status]) ListDependents(ctx context.Context) ([]ObjectRef, error) {
	if err := c.live(); err != nil {
		return nil, err
	}
	return c.bh.store.Edges().ListIncoming(ctx, c.id, RelationDependsOn)
}

func (c *controllerClientImpl[Status]) ListOwned(ctx context.Context) ([]ObjectRef, error) {
	if err := c.live(); err != nil {
		return nil, err
	}
	return c.bh.store.Edges().ListIncoming(ctx, c.id, RelationOwnedBy)
}

func (c *controllerClientImpl[Status]) HasIncomingEdges(ctx context.Context) (bool, error) {
	if err := c.live(); err != nil {
		return false, err
	}
	return c.bh.store.Edges().HasIncoming(ctx, c.id)
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
