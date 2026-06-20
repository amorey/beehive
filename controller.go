package beehive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/amorey/beehive/internal/storeapi"
)

// ErrWrongKind is returned by a ControllerClient write when the target id names
// an object of a different kind than the controller's own. A controller may only
// write status, conditions, and finalizers on objects of its registered kind;
// passing an id from another kind (a dependency, an owner) is a bug that would
// otherwise persist this controller's Status JSON into a foreign row and make
// later typed reads of that kind fail to decode. The guard turns that silent
// corruption into a loud, retrying reconcile failure.
var ErrWrongKind = errors.New("beehive: object belongs to a different kind")

// Controller is the user-supplied reconcile logic for a resource kind. Start
// receives a ControllerClient for writing status and runs once on registration;
// Reconcile is called to drive an object toward its desired state; Stop tears
// down any background work.
type Controller[Spec, Status any] interface {
	Start(client ControllerClient[Status]) error
	Stop(ctx context.Context) error
	Reconcile(ctx context.Context, obj *Object[Spec, Status]) (Result, error)
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
	// HasReferrers reports whether any object with a live claim still points at id:
	// an owned child, or a dependent that is not itself being deleted. A dependent
	// that is itself finalizing is excluded — it's going away and no longer has a
	// claim. A finalizer can gate teardown on this: a controller holding a shared
	// resource clears its finalizer only once nothing with a live claim references
	// the object, so the resource outlives its last real user.
	HasReferrers(ctx context.Context, id ObjectID) (bool, error)
	// Within runs fn inside a single transaction: the ControllerClient writes fn
	// makes (with the ctx passed to it) all commit together on a nil return, or all
	// roll back on error. Reconcile itself is not transactional — each write
	// otherwise commits on its own — so a controller uses Within only for the
	// writes that must be atomic. The transaction holds the store's write lock for
	// fn's whole duration, so keep external I/O outside it.
	Within(ctx context.Context, fn func(ctx context.Context) error) error
}

// controllerClientImpl is the status-writing surface handed to a controller's
// Start. Its methods are only reached from within a Reconcile call.
type controllerClientImpl[Status any] struct {
	bh *Beehive
	gk GroupKind
}

// checkKind confirms id names an object of this controller's kind before a
// write touches it. A ControllerClient is bound to the single kind it was
// registered for; an id from another kind (a dependency, an owner) must never
// receive this controller's status/condition/finalizer writes, or a later typed
// read of that kind would fail to decode the foreign status. Mirrors the
// user-facing client's scopedGet. Called inside withinKind's transaction, so the
// check shares one snapshot with the write and can't race a concurrent kind change.
func (c *controllerClientImpl[Status]) checkKind(ctx context.Context, id ObjectID) error {
	raw, err := c.bh.store.GetObject(ctx, id)
	if err != nil {
		return err
	}
	if raw.Group != c.gk.Group || raw.Kind != c.gk.Kind {
		return fmt.Errorf("%w: controller %s/%s cannot write to %s/%s object %d",
			ErrWrongKind, c.gk.Group, c.gk.Kind, raw.Group, raw.Kind, id)
	}
	return nil
}

// withinKind runs fn in a short transaction after confirming id belongs to this
// controller's kind, so the check and fn's write share one snapshot and commit
// atomically. Every status/condition/finalizer write funnels through here (the
// ref-edge methods are scoped differently — see AddDependency). Standalone it
// commits on its own; inside a controller's Within it nests and commits with the
// group (see Within for the nesting mechanics).
func (c *controllerClientImpl[Status]) withinKind(ctx context.Context, id ObjectID, fn func(ctx context.Context) error) error {
	return c.bh.store.Within(ctx, func(ctx context.Context) error {
		if err := c.checkKind(ctx, id); err != nil {
			return err
		}
		return fn(ctx)
	})
}

func (c *controllerClientImpl[Status]) UpdateStatus(ctx context.Context, id ObjectID, observedGeneration int64, status Status) error {
	b, err := json.Marshal(status)
	if err != nil {
		return err
	}
	return c.withinKind(ctx, id, func(ctx context.Context) error {
		// The store's UpdateStatus emits the Modified event into the transaction's
		// collector, so it's published only after this write's transaction commits.
		_, err := c.bh.store.UpdateStatus(ctx, id, observedGeneration, b)
		return err
	})
}

func (c *controllerClientImpl[Status]) SetCondition(ctx context.Context, id ObjectID, condition Condition) error {
	return c.withinKind(ctx, id, func(ctx context.Context) error {
		_, err := c.bh.store.SetCondition(ctx, id, storeapi.Condition{
			Type:     condition.Type,
			Status:   string(condition.Status),
			Reason:   condition.Reason,
			Message:  condition.Message,
			Liveness: condition.Liveness,
		})
		return err
	})
}

func (c *controllerClientImpl[Status]) DeleteCondition(ctx context.Context, id ObjectID, conditionType string) error {
	return c.withinKind(ctx, id, func(ctx context.Context) error {
		_, err := c.bh.store.DeleteCondition(ctx, id, conditionType)
		return err
	})
}

func (c *controllerClientImpl[Status]) DeleteFinalizer(ctx context.Context, id ObjectID, finalizer string) error {
	return c.withinKind(ctx, id, func(ctx context.Context) error {
		_, err := c.bh.store.DeleteFinalizer(ctx, id, finalizer)
		return err
	})
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
		target, err := c.bh.store.GetObject(ctx, toID)
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

// HasReferrers reports whether anything still claims id. It is a plain read that
// commits on its own; to gate a write on it atomically — e.g. clearing a finalizer
// only if nothing references the object — a controller runs both inside Within, so
// the read and the write share one transaction snapshot.
func (c *controllerClientImpl[Status]) HasReferrers(ctx context.Context, id ObjectID) (bool, error) {
	return c.bh.store.HasReferrers(ctx, id)
}

// Within opens a transaction and runs fn under it; the ControllerClient writes fn
// makes commit together on a nil return or roll back on error. Each write's own
// store.Within nests into this one (joining via the ctx's txKey), so they share
// the single transaction rather than autocommitting independently.
//
// Within adds no kind scoping of its own — it takes no id and groups arbitrary
// writes (a controller may legitimately touch other kinds here, e.g. read a
// dependency then clear its own finalizer). The kind boundary is still enforced
// per write: each status/condition/finalizer write re-checks via withinKind, so
// grouping them in a transaction never widens what this controller can mutate.
func (c *controllerClientImpl[Status]) Within(ctx context.Context, fn func(ctx context.Context) error) error {
	return c.bh.store.Within(ctx, fn)
}
