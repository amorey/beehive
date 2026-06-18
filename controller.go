package beehive

import "context"

type Controller[Spec, Status any] interface {
	Start(ctx context.Context, client ControllerClient[Status]) error
	Stop(ctx context.Context) error
	Reconcile(ctx context.Context, obj *Object[Spec, Status]) (Result, error)
}

type ControllerClient[Status any] interface {
	UpdateStatus(ctx context.Context, id ObjectID, status Status) error
	SetCondition(ctx context.Context, id ObjectID, condition Condition) error
	DeleteCondition(ctx context.Context, id ObjectID, conditionType string) error
	DeleteFinalizer(ctx context.Context, id ObjectID, finalizer string) error
	AddDependency(ctx context.Context, fromID, toID ObjectID) error
	DeleteDependency(ctx context.Context, fromID, toID ObjectID) error
}
