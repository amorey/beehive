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

	"github.com/amorey/beehive/internal/storeapi"
)

// ErrWrongKind is returned by a ControllerClient write whose target id belongs
// to another kind. The store folds the controller's kind into every write, so a
// wrong id fails loudly instead of corrupting another kind's row.
var ErrWrongKind = storeapi.ErrWrongKind

// Controller is the user-supplied reconcile logic for a resource kind.
// Reconcile drives an object toward its desired state; the client is the
// status-write surface for this controller's kind.
type Controller[Spec, Status any] interface {
	Reconcile(ctx context.Context, client ControllerClient[Status], obj *Object[Spec, Status]) (Result, error)
}

// ControllerClient is the write surface a controller uses to report observed
// state. It writes only Status and metadata — never Spec, which the user owns.
type ControllerClient[Status any] interface {
	ConditionsDelete(ctx context.Context, id ObjectID, conditionType string) error
	ConditionsSet(ctx context.Context, id ObjectID, condition Condition) error
	// DependenciesAdd records that fromID depends on toID, so beehive reconciles
	// fromID again when toID changes. Every call that creates the edge records
	// one owed reconcile for fromID, durably and atomically with the edge, so a
	// declared dependency is a guarantee rather than a subscription.
	// Re-asserting existing edges records nothing.
	DependenciesAdd(ctx context.Context, fromID, toID ObjectID) error
	DependenciesDelete(ctx context.Context, fromID, toID ObjectID) error
	// DependenciesList returns the objects id depends on (outgoing depends_on).
	DependenciesList(ctx context.Context, id ObjectID) ([]ObjectRef, error)
	// DependentsList returns the objects that depend on id (incoming depends_on).
	DependentsList(ctx context.Context, id ObjectID) ([]ObjectRef, error)
	// EdgesHasIncoming reports whether any object with a live claim still points
	// at id: an owned child, or a dependent that is not itself being deleted. A
	// finalizer can gate teardown on it.
	EdgesHasIncoming(ctx context.Context, id ObjectID) (bool, error)
	// EventsAdd adds an observation to id's event log. Repeating the latest run's
	// (Category, Type, Reason) extends that run rather than appending, so a
	// controller can report every poll without growing the log per poll.
	EventsAdd(ctx context.Context, id ObjectID, event EventSpec) error
	FinalizersDelete(ctx context.Context, id ObjectID, finalizer string) error
	// OwnedList returns the objects id owns (its incoming owned_by edges).
	OwnedList(ctx context.Context, id ObjectID) ([]ObjectRef, error)
	// OwnersGet returns id's owner, if any. ok is false with a nil error when the
	// object has no owner.
	OwnersGet(ctx context.Context, id ObjectID) (ObjectRef, bool, error)
	// UpdateStatus records status and the generation this reconcile observed.
	// Status that marshals to the stored bytes writes nothing, so a controller
	// can report unconditionally without waking watchers on an unchanged poll —
	// except that a newly settled generation is still recorded and does emit.
	UpdateStatus(ctx context.Context, id ObjectID, observedGeneration int64, status Status) error
	// Within runs fn inside a single transaction: writes made with fn's ctx all
	// commit together or roll back on error. Pass fn's ctx to every store call
	// it makes — the store runs on one connection, so any other context
	// deadlocks against the transaction. Watches cannot be opened inside it.
	Within(ctx context.Context, fn func(ctx context.Context) error) error
}

// controllerClientImpl is the status-writing surface for a controller's kind,
// built once at Register.
type controllerClientImpl[Status any] struct {
	bh *Beehive
	gk GroupKind
}

// The store folds the controller's kind into each write below, so another
// kind's id is rejected with ErrWrongKind and a missing id with ErrNotFound.
// Each mutator self-wraps in Within, joining the controller's own Within when
// nested.

// wakeAfter returns a store write's error, waking the kind's watches when the
// write succeeded. Every mutator here that appends to the object write log ends
// with it: forgetting the wake costs a floor tick of staleness rather than a
// failure, so nothing else would catch it.
//
// EventsAdd is the one that does not, and must not: an event bumps no
// resource_version, so it appends no entry for a watch to read.
func (c *controllerClientImpl[Status]) wakeAfter(ctx context.Context, err error) error {
	if err != nil {
		return err
	}
	c.bh.signalKindWritten(ctx, c.gk)
	return nil
}

func (c *controllerClientImpl[Status]) UpdateStatus(ctx context.Context, id ObjectID, observedGeneration int64, status Status) error {
	b, err := json.Marshal(status)
	if err != nil {
		return err
	}
	return c.wakeAfter(ctx, c.bh.store.ObjectsUpdateStatus(
		ctx, c.gk, id, observedGeneration, b, migratorStatusVersion(c.bh.migratorFor(c.gk))))
}

func (c *controllerClientImpl[Status]) ConditionsSet(ctx context.Context, id ObjectID, condition Condition) error {
	return c.wakeAfter(ctx, c.bh.store.ConditionsSet(ctx, c.gk, id, storeapi.Condition{
		Type:     condition.Type,
		Status:   string(condition.Status),
		Reason:   condition.Reason,
		Message:  condition.Message,
		Liveness: condition.Liveness,
	}))
}

func (c *controllerClientImpl[Status]) ConditionsDelete(ctx context.Context, id ObjectID, conditionType string) error {
	return c.wakeAfter(ctx, c.bh.store.ConditionsDelete(ctx, c.gk, id, conditionType))
}

func (c *controllerClientImpl[Status]) EventsAdd(ctx context.Context, id ObjectID, event EventSpec) error {
	var detail []byte
	if event.Detail != nil {
		var err error
		if detail, err = json.Marshal(event.Detail); err != nil {
			return err
		}
	}
	_, err := c.bh.store.EventsAdd(ctx, c.gk, id, storeapi.Event{
		Category: event.Category,
		Type:     string(event.Type),
		Reason:   event.Reason,
		Message:  event.Message,
		Detail:   detail,
	})
	return err
}

// Clearing the last finalizer on a deleting row pushes the collect it unblocks;
// gcCollect still re-checks the RESTRICT block. See
// docs/adr/2026-08-05-a-cleared-finalizer-pushes-its-own-collect.md.
func (c *controllerClientImpl[Status]) FinalizersDelete(ctx context.Context, id ObjectID, finalizer string) error {
	clearedLast, err := c.bh.store.FinalizersDelete(ctx, c.gk, id, finalizer)
	if err := c.wakeAfter(ctx, err); err != nil {
		return err
	}
	if clearedLast {
		c.bh.signalRequeueNow(ctx, ObjectRef{ID: id, Group: c.gk.Group, Kind: c.gk.Kind})
	}
	return nil
}

// DependenciesAdd is one store call, not a composition: the edge and the durable
// reconcile-owed stamp are indivisible inside EdgesAdd, so an edge can never
// commit without its wake. The enqueue below is the prompt half; the stamp is
// the guarantee. It is gated on the store reporting the edge as new — which
// bounds it to one enqueue per edge ever created — and routed by res.From
// because the edge is cross-kind.
func (c *controllerClientImpl[Status]) DependenciesAdd(ctx context.Context, fromID, toID ObjectID) error {
	res, err := c.bh.store.EdgesAdd(ctx, fromID, toID, RelationDependsOn)
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

// DependenciesDelete drops the edge and schedules nothing: a finalizing toID is
// already in the GC sweeper's listing, and the next tick finds the block gone.
func (c *controllerClientImpl[Status]) DependenciesDelete(ctx context.Context, fromID, toID ObjectID) error {
	return c.bh.store.EdgesDelete(ctx, fromID, toID, RelationDependsOn)
}

// The ref reads below are plain edge queries with no kind scoping: a controller
// reasons about its own object's relationships. To gate a write on
// EdgesHasIncoming atomically, run both inside Within.

func (c *controllerClientImpl[Status]) OwnersGet(ctx context.Context, id ObjectID) (ObjectRef, bool, error) {
	return fetchOwnerRef(ctx, c.bh.store, id)
}

func (c *controllerClientImpl[Status]) DependenciesList(ctx context.Context, id ObjectID) ([]ObjectRef, error) {
	return c.bh.store.EdgesListOutgoingByRelation(ctx, id, RelationDependsOn)
}

func (c *controllerClientImpl[Status]) DependentsList(ctx context.Context, id ObjectID) ([]ObjectRef, error) {
	return c.bh.store.EdgesListIncoming(ctx, id, RelationDependsOn)
}

func (c *controllerClientImpl[Status]) OwnedList(ctx context.Context, id ObjectID) ([]ObjectRef, error) {
	return c.bh.store.EdgesListIncoming(ctx, id, RelationOwnedBy)
}

func (c *controllerClientImpl[Status]) EdgesHasIncoming(ctx context.Context, id ObjectID) (bool, error) {
	return c.bh.store.EdgesHasIncoming(ctx, id)
}

// Within groups writes into one transaction. It adds no kind scoping of its
// own — each write still folds the controller's kind in — so grouping never
// widens what this controller can mutate.
func (c *controllerClientImpl[Status]) Within(ctx context.Context, fn func(ctx context.Context) error) error {
	return c.bh.store.Within(ctx, fn)
}
