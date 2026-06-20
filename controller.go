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
// user-facing client's scopedGet. Called from within Reconcile, so the check
// shares the reconcile transaction (ctx) and can't race a concurrent kind change.
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

func (c *controllerClientImpl[Status]) UpdateStatus(ctx context.Context, id ObjectID, observedGeneration int64, status Status) error {
	if err := c.checkKind(ctx, id); err != nil {
		return err
	}
	b, err := json.Marshal(status)
	if err != nil {
		return err
	}
	// The store's UpdateStatus emits the Modified event into the ambient
	// transaction's collector, so it's published only after the reconcile commits.
	if _, err := c.bh.store.UpdateStatus(ctx, id, observedGeneration, b); err != nil {
		return err
	}
	return nil
}

func (c *controllerClientImpl[Status]) SetCondition(ctx context.Context, id ObjectID, condition Condition) error {
	if err := c.checkKind(ctx, id); err != nil {
		return err
	}
	_, err := c.bh.store.SetCondition(ctx, id, storeapi.Condition{
		Type:     condition.Type,
		Status:   string(condition.Status),
		Reason:   condition.Reason,
		Message:  condition.Message,
		Liveness: condition.Liveness,
	})
	return err
}

func (c *controllerClientImpl[Status]) DeleteCondition(ctx context.Context, id ObjectID, conditionType string) error {
	if err := c.checkKind(ctx, id); err != nil {
		return err
	}
	_, err := c.bh.store.DeleteCondition(ctx, id, conditionType)
	return err
}

func (c *controllerClientImpl[Status]) DeleteFinalizer(ctx context.Context, id ObjectID, finalizer string) error {
	if err := c.checkKind(ctx, id); err != nil {
		return err
	}
	_, err := c.bh.store.DeleteFinalizer(ctx, id, finalizer)
	return err
}

// AddDependency records that fromID depends on toID, so Beehive requeues fromID
// when toID changes. The relation is always "depends_on" (owner edges come from
// WithOwner at create time); inside Reconcile the write joins that transaction.
func (c *controllerClientImpl[Status]) AddDependency(ctx context.Context, fromID, toID ObjectID) error {
	return c.bh.store.AddRef(ctx, fromID, toID, RelationDependsOn)
}

func (c *controllerClientImpl[Status]) DeleteDependency(ctx context.Context, fromID, toID ObjectID) error {
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
}

func (c *controllerClientImpl[Status]) HasReferrers(ctx context.Context, id ObjectID) (bool, error) {
	return c.bh.store.HasReferrers(ctx, id)
}
