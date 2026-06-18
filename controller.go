package beehive

import (
	"context"
	"encoding/json"
)

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
}

// controllerClientImpl is the status-writing surface handed to a controller's
// Start. Its methods are only reached from within a Reconcile call, so they
// remain stubbed until the reconcile + Store-write slice lands.
type controllerClientImpl[Status any] struct {
	bh *Beehive
	gk GroupKind
}

func (c *controllerClientImpl[Status]) UpdateStatus(ctx context.Context, id ObjectID, observedGeneration int64, status Status) error {
	b, err := json.Marshal(status)
	if err != nil {
		return err
	}
	_, err = c.bh.store.UpdateStatus(ctx, id, observedGeneration, b)
	return err
}

func (c *controllerClientImpl[Status]) SetCondition(_ context.Context, _ ObjectID, _ Condition) error {
	panic("not implemented: ControllerClient.SetCondition")
}

func (c *controllerClientImpl[Status]) DeleteCondition(_ context.Context, _ ObjectID, _ string) error {
	panic("not implemented: ControllerClient.DeleteCondition")
}

func (c *controllerClientImpl[Status]) DeleteFinalizer(_ context.Context, _ ObjectID, _ string) error {
	panic("not implemented: ControllerClient.DeleteFinalizer")
}

func (c *controllerClientImpl[Status]) AddDependency(_ context.Context, _ ObjectID, _ ObjectID) error {
	panic("not implemented: ControllerClient.AddDependency")
}

func (c *controllerClientImpl[Status]) DeleteDependency(_ context.Context, _ ObjectID, _ ObjectID) error {
	panic("not implemented: ControllerClient.DeleteDependency")
}
