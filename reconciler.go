package beehive

import (
	"context"
	"time"
)

const defaultMaxRetryInterval = 30 * time.Second

// controllerAdapter is the non-generic view of a registered controller. The
// generic Register wraps the user's Controller[Spec, Status] in a concrete
// adapter that closes over Spec/Status, so everything below this line —
// reconciler, work queue, Store — stays free of type parameters and deals in
// raw JSON.
type controllerAdapter interface {
	start() error
	stop(ctx context.Context) error
	reconcile(ctx context.Context, id ObjectID) (Result, error)
}

// typedController adapts a generic Controller[Spec, Status] to the non-generic
// controllerAdapter interface.
type typedController[Spec, Status any] struct {
	gk    GroupKind
	bh    *Beehive
	inner Controller[Spec, Status]
}

func (t *typedController[Spec, Status]) start() error {
	client := &controllerClientImpl[Status]{bh: t.bh, gk: t.gk}
	return t.inner.Start(client)
}

func (t *typedController[Spec, Status]) stop(ctx context.Context) error {
	return t.inner.Stop(ctx)
}

func (t *typedController[Spec, Status]) reconcile(ctx context.Context, id ObjectID) (Result, error) {
	// TODO(next slice): load the raw row from the store, decode it into a
	// typed Object[Spec, Status], invoke t.inner.Reconcile, and persist the
	// result inside the reconcile transaction.
	panic("not implemented: typedController.reconcile")
}

// reconciler drives the reconcile loop for a single registered controller.
// It owns the work queue, exponential backoff, and periodic resync timer.
type reconciler struct {
	gk               GroupKind
	adapter          controllerAdapter
	resyncInterval   time.Duration
	maxRetryInterval time.Duration
}

// run is the per-controller reconcile loop. It exits when ctx is cancelled.
//
// A resyncInterval <= 0 disables the periodic resync entirely: the loop then
// reconciles only in response to events (once the work queue lands), never on a
// timer.
func (r *reconciler) run(ctx context.Context) {
	// time.NewTicker panics on a non-positive interval, so guard it: a disabled
	// resync means no ticker channel to select on.
	var resync <-chan time.Time
	if r.resyncInterval > 0 {
		ticker := time.NewTicker(r.resyncInterval)
		defer ticker.Stop()
		resync = ticker.C
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-resync:
			// TODO(next slice): resync — enumerate this kind's objects and
			// enqueue any that are unsettled, then drain the work queue.
		}
	}
}
