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
	UpdateStatus(ctx context.Context, id ObjectID, observedGeneration int64, status Status) error
	SetCondition(ctx context.Context, id ObjectID, condition Condition) error
	DeleteCondition(ctx context.Context, id ObjectID, conditionType string) error
	DeleteFinalizer(ctx context.Context, id ObjectID, finalizer string) error
	AddDependency(ctx context.Context, fromID, toID ObjectID) error
	DeleteDependency(ctx context.Context, fromID, toID ObjectID) error
	// HasIncomingRefs reports whether any object with a live claim still points at id:
	// an owned child, or a dependent that is not itself being deleted. A dependent
	// that is itself finalizing is excluded — it's going away and no longer has a
	// claim. A finalizer can gate teardown on this: a controller holding a shared
	// resource clears its finalizer only once nothing with a live claim references
	// the object, so the resource outlives its last real user.
	HasIncomingRefs(ctx context.Context, id ObjectID) (bool, error)
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
	_, err = c.bh.store.UpdateStatus(ctx, c.gk, id, observedGeneration, b)
	return err
}

func (c *controllerClientImpl[Status]) SetCondition(ctx context.Context, id ObjectID, condition Condition) error {
	_, err := c.bh.store.SetCondition(ctx, c.gk, id, storeapi.Condition{
		Type:     condition.Type,
		Status:   string(condition.Status),
		Reason:   condition.Reason,
		Message:  condition.Message,
		Liveness: condition.Liveness,
	})
	return err
}

func (c *controllerClientImpl[Status]) DeleteCondition(ctx context.Context, id ObjectID, conditionType string) error {
	_, err := c.bh.store.DeleteCondition(ctx, c.gk, id, conditionType)
	return err
}

func (c *controllerClientImpl[Status]) DeleteFinalizer(ctx context.Context, id ObjectID, finalizer string) error {
	_, err := c.bh.store.DeleteFinalizer(ctx, c.gk, id, finalizer)
	return err
}

// AddDependency records that fromID depends on toID, so Beehive requeues fromID
// when toID changes. The relation is always "depends_on" (owner edges come from
// WithOwner at create time). AddRef checks both endpoints exist and then inserts
// the edge as separate statements, so the Within keeps them atomic: a delete
// interleaving between them would otherwise leak a raw FK error instead of
// ErrNotFound. Standalone it is one short transaction; inside a controller's own
// Within it joins that group.
func (c *controllerClientImpl[Status]) AddDependency(ctx context.Context, fromID, toID ObjectID) error {
	return c.bh.store.Within(ctx, func(ctx context.Context) error {
		return c.bh.store.AddRef(ctx, fromID, toID, RelationDependsOn)
	})
}

func (c *controllerClientImpl[Status]) DeleteDependency(ctx context.Context, fromID, toID ObjectID) error {
	return c.bh.store.Within(ctx, func(ctx context.Context) error {
		if err := c.bh.store.DeleteRef(ctx, fromID, toID, RelationDependsOn); err != nil {
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
		target, err := c.bh.store.GetObjectMeta(ctx, toID)
		if errors.Is(err, ErrNotFound) {
			return nil // target already gone
		}
		if err != nil {
			return err
		}
		if target.DeletionRequestedAt != nil {
			wakes.targets = append(wakes.targets, Referrer{ID: toID, Group: target.Group, Kind: target.Kind})
		}
		return nil
	})
}

// HasIncomingRefs reports whether anything still claims id. It is a plain read that
// commits on its own; to gate a write on it atomically — e.g. clearing a finalizer
// only if nothing references the object — a controller runs both inside Within, so
// the read and the write share one transaction snapshot.
func (c *controllerClientImpl[Status]) HasIncomingRefs(ctx context.Context, id ObjectID) (bool, error) {
	return c.bh.store.HasIncomingRefs(ctx, id)
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
