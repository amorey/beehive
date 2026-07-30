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

// ErrWrongKind is returned by a ControllerClient write whose target id belongs to
// another kind. A controller may only write status, conditions and finalizers on its
// own kind. Passing another kind's id — a dependency, an owner — would otherwise
// store this controller's status JSON in that row and break later typed reads of it.
// The store folds the controller's kind into every write, which turns silent
// corruption into a loud, retrying reconcile failure. Aliased from storeapi, like
// ErrNotFound.
var ErrWrongKind = storeapi.ErrWrongKind

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
	// DependenciesAdd records that fromID depends on toID, so beehive reconciles
	// fromID again when toID changes.
	//
	// Every call that *creates* the edge records one owed reconcile for fromID
	// (self-edges excluded), durably, in the same store call as the edge itself. That
	// is what makes a declared dependency a guarantee rather than a subscription: a
	// change to toID that landed before the declare, a declare made on
	// another object's behalf while that object's own Reconcile is mid-flight, a
	// crash before the wake is serviced — all of them leave the owed count standing,
	// and the owed pass drains it at startup unconditionally. Re-asserting your edges
	// every pass costs nothing after the first, because only the call that created
	// the edge records anything; the cost is one reconcile per edge ever created, and
	// a caller that deletes and re-declares its set pays once per re-create.
	//
	// Once the edge exists, delivering toID's changes is the waker's job, backstopped
	// by the stale-dependents pass, which re-derives any wake the waker loses.
	DependenciesAdd(ctx context.Context, fromID, toID ObjectID) error
	DependenciesDelete(ctx context.Context, fromID, toID ObjectID) error
	// DependenciesList returns the objects id depends on (outgoing depends_on).
	DependenciesList(ctx context.Context, id ObjectID) ([]ObjectRef, error)
	// DependentsList returns the objects that depend on id (incoming depends_on).
	DependentsList(ctx context.Context, id ObjectID) ([]ObjectRef, error)
	// EdgesHasIncoming reports whether any object with a live claim still points at id:
	// an owned child, or a dependent that is not itself being deleted. A dependent
	// that is itself finalizing is excluded — it's going away and no longer has a
	// claim. A finalizer can gate teardown on this: a controller holding a shared
	// resource clears its finalizer only once nothing with a live claim references
	// the object, so the resource outlives its last real user.
	EdgesHasIncoming(ctx context.Context, id ObjectID) (bool, error)
	// EventsAdd adds an observation to id's event log, aggregating into contiguous
	// runs (see EventSpec): repeating the latest run's (Category, Type, Reason)
	// extends that run rather than appending a second one, so a controller can
	// report every poll without growing the log per poll. Like the other writes it
	// is kind-folded and composes in Within, so a controller can record an event and
	// update a condition atomically.
	EventsAdd(ctx context.Context, id ObjectID, event EventSpec) error
	FinalizersDelete(ctx context.Context, id ObjectID, finalizer string) error
	// OwnedList returns the objects id owns (its incoming owned_by edges).
	OwnedList(ctx context.Context, id ObjectID) ([]ObjectRef, error)
	// OwnersGet returns id's owner, if any (its outgoing owned_by edge). ok reports
	// presence: false with a nil error when the object has no owner. The lazy
	// counterpart to a reconciler's LoadOwner default.
	OwnersGet(ctx context.Context, id ObjectID) (ObjectRef, bool, error)
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
	//
	// **Pass fn's ctx to every store call it makes, including reads.** The store runs
	// on one connection, so a call made with any other context waits for the
	// connection this transaction holds and deadlocks against itself. The one call
	// that cannot be made here at all is opening a watch: it reads current state
	// before returning (see Client.ObjectsWatch), and its stream must outlive the
	// transaction, so neither ctx is the right one. Subscribe outside Within.
	Within(ctx context.Context, fn func(ctx context.Context) error) error
}

// controllerClientImpl is the status-writing surface for a controller's kind.
// It is constructed once at Register, passed into each Reconcile, and returned
// to the embedding application so it can write status from its own goroutines.
type controllerClientImpl[Status any] struct {
	bh *Beehive
	gk GroupKind
}

// The store folds the controller's kind into each write below, so another kind's id
// matches no row and is rejected with ErrWrongKind. A controller's status, condition
// and finalizer writes therefore cannot corrupt another kind's row. Because the kind
// check is part of the write, there is no separate check to keep atomic with it: each
// mutator self-wraps in Within, joining the controller's own Within when nested. A
// missing id comes back as ErrNotFound, which the store tells apart from another
// kind's id.

func (c *controllerClientImpl[Status]) UpdateStatus(ctx context.Context, id ObjectID, observedGeneration int64, status Status) error {
	b, err := json.Marshal(status)
	if err != nil {
		return err
	}
	// The store's UpdateStatus emits the Modified event into its transaction's
	// collector, so it's published only after the write commits.
	return c.bh.store.ObjectsUpdateStatus(ctx, c.gk, id, observedGeneration, b, migratorStatusVersion(c.bh.migratorFor(c.gk)))
}

func (c *controllerClientImpl[Status]) ConditionsSet(ctx context.Context, id ObjectID, condition Condition) error {
	return c.bh.store.ConditionsSet(ctx, c.gk, id, storeapi.Condition{
		Type:     condition.Type,
		Status:   string(condition.Status),
		Reason:   condition.Reason,
		Message:  condition.Message,
		Liveness: condition.Liveness,
	})
}

func (c *controllerClientImpl[Status]) ConditionsDelete(ctx context.Context, id ObjectID, conditionType string) error {
	return c.bh.store.ConditionsDelete(ctx, c.gk, id, conditionType)
}

// EventsAdd marshals the event's optional Detail — typed in, opaque out, like
// Spec and Status — and appends the run through the store, which folds in the
// controller's kind. Nothing is published: the row is the record, and an EventsWatch
// poll finds it once the write commits. A nil Detail stays nil, and the store groups
// runs by (Category, Type, Reason).
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

func (c *controllerClientImpl[Status]) FinalizersDelete(ctx context.Context, id ObjectID, finalizer string) error {
	return c.bh.store.FinalizersDelete(ctx, c.gk, id, finalizer)
}

// DependenciesAdd implements the contract documented on ControllerClient. The
// relation is always "depends_on" (owner edges come from WithOwner at create
// time).
//
// It is one store call, not a composition. Both writes — the edge and the durable
// wake stamp — live inside EdgesAdd rather than being sequenced here, so the pair is
// indivisible no matter what any caller does with the error. A nested Within is a
// savepoint boundary now, so a second call sequenced here would in fact unwind with
// the first; the reason to keep them in one store call is that the guarantee then
// rests on the store, not on every caller's Within being a real boundary. The failure
// it forecloses is a stranded dependent: an edge committed with no wake, on a stale
// read nothing re-derives.
//
// The stamp fires on every edge the call creates, gated only on the edge being
// new — which is what stops a level-triggered controller re-asserting its set every
// pass from re-firing, since nothing throttles a wake (the dispatch path has no
// already-settled skip, and workQueue.addLocked has no rate limiter). It is
// unconditional beyond that gate because only recorded owed work is sound under
// every interleaving: increments landing mid-pass survive the load-scoped
// decrement (see the ADR on stamping every new edge). The edge-new gate costs
// nothing, riding the stamp statement's own NOT EXISTS.
//
// Nothing is scheduled here, and the store's EdgesAddResult is discarded for that
// reason: reconcile_owed is a durable count the owed pass drains, routed by
// fromID's own GroupKind inside the store — the edge is deliberately cross-kind, so
// a controller may declare one on another kind's behalf. The count is the whole
// mechanism: it is durable, so a crash between the commit and the pass loses
// nothing.
func (c *controllerClientImpl[Status]) DependenciesAdd(ctx context.Context, fromID, toID ObjectID) error {
	_, err := c.bh.store.EdgesAdd(ctx, fromID, toID, RelationDependsOn)
	return err
}

// DependenciesDelete drops the edge and does nothing else. Dropping it can unblock
// toID's deletion, since edges are RESTRICT, but nothing is scheduled for that: a
// finalizing toID is already in the GC sweeper's listing — being deletion-pending is
// what puts it there — so the next tick retries the collect and finds the block gone.
// That tick is guaranteed, because WithGCInterval cannot be disabled.
func (c *controllerClientImpl[Status]) DependenciesDelete(ctx context.Context, fromID, toID ObjectID) error {
	return c.bh.store.EdgesDelete(ctx, fromID, toID, RelationDependsOn)
}

// EdgesHasIncoming reports whether anything still claims id. It is a plain read that
// commits on its own. To gate a write on it atomically — clearing a finalizer only if
// nothing references the object, say — run both inside Within so the read and the
// write share one transaction.
// OwnersGet/DependenciesList/DependentsList/OwnedList read ref edges directly,
// like EdgesHasIncoming above — no kind-scoping, since a controller reasons about
// its own object's relationships.
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
