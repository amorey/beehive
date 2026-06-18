package beehive

import (
	"context"
	"time"
)

const (
	defaultMaxRetryInterval  = 30 * time.Second
	defaultBaseRetryInterval = 1 * time.Second
)

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

// reconcile runs the controller in a single transaction: load, reconcile, and
// any controller-client writes (UpdateStatus, …) all commit together, or all
// roll back if Reconcile returns an error.
func (t *typedController[Spec, Status]) reconcile(ctx context.Context, id ObjectID) (Result, error) {
	var result Result
	err := t.bh.store.Within(ctx, func(ctx context.Context) error {
		raw, err := t.bh.store.GetObject(ctx, id)
		if err != nil {
			return err
		}
		obj, err := rawToTyped[Spec, Status](raw)
		if err != nil {
			return err
		}
		var reconcileErr error
		result, reconcileErr = t.inner.Reconcile(ctx, obj)
		return reconcileErr
	})
	return result, err
}

// reconciler drives the reconcile loop for a single registered controller.
// It owns the work queue, exponential backoff, and periodic resync timer.
type reconciler struct {
	gk                GroupKind
	adapter           controllerAdapter
	store             Store
	work              *workQueue
	resyncInterval    time.Duration
	maxRetryInterval  time.Duration
	baseRetryInterval time.Duration // zero falls back to defaultBaseRetryInterval

	// backoffFor tracks the current retry delay per object. Only accessed from
	// the run goroutine, so no mutex is needed.
	backoffFor map[ObjectID]time.Duration
}

// enqueue adds id to the work queue if one is configured.
func (r *reconciler) enqueue(id ObjectID) {
	if r.work != nil {
		r.work.add(id)
	}
}

// nextBackoff returns the next retry delay for id and doubles it for next time,
// capped at maxRetryInterval. Only called from the run goroutine.
func (r *reconciler) nextBackoff(id ObjectID) time.Duration {
	cur := r.backoffFor[id]
	if cur == 0 {
		cur = r.baseRetryInterval
		if cur == 0 {
			cur = defaultBaseRetryInterval
		}
	} else {
		cur *= 2
	}
	if cur > r.maxRetryInterval {
		cur = r.maxRetryInterval
	}
	r.backoffFor[id] = cur
	return cur
}

// clearBackoff resets the retry delay for id after a successful reconcile.
// Only called from the run goroutine.
func (r *reconciler) clearBackoff(id ObjectID) {
	delete(r.backoffFor, id)
}

// run is the per-controller reconcile loop. It exits when ctx is cancelled.
//
// A resyncInterval <= 0 disables the periodic resync entirely: the loop then
// reconciles only in response to events (once the work queue lands), never on a
// timer.
func (r *reconciler) run(ctx context.Context) {
	// Enqueue any objects that weren't settled before this process started.
	// Without this, objects persisted by a previous run would never converge
	// when resync is disabled (WithResyncInterval(0)).
	if r.store != nil {
		if objs, err := r.store.ListObjects(ctx, r.gk); err == nil {
			for _, obj := range objs {
				if obj.ObservedGeneration == nil || *obj.ObservedGeneration != obj.Generation {
					r.enqueue(obj.ID)
				}
			}
		}
	}

	// time.NewTicker panics on a non-positive interval, so guard it: a disabled
	// resync means no ticker channel to select on.
	var resync <-chan time.Time
	if r.resyncInterval > 0 {
		ticker := time.NewTicker(r.resyncInterval)
		defer ticker.Stop()
		resync = ticker.C
	}

	// A nil channel blocks forever in a select, which is the correct no-op
	// when no work queue is configured.
	var workReady <-chan struct{}
	if r.work != nil {
		workReady = r.work.ready
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-workReady:
			if id, ok := r.work.get(); ok {
				result, err := r.adapter.reconcile(ctx, id)
				if err != nil {
					r.work.addAfter(id, r.nextBackoff(id))
				} else {
					r.clearBackoff(id)
					if result.RequeueAfter > 0 {
						r.work.addAfter(id, result.RequeueAfter)
					}
				}
			}
		case <-resync:
			objs, err := r.store.ListObjects(ctx, r.gk)
			if err != nil {
				continue
			}
			for _, obj := range objs {
				if obj.ObservedGeneration == nil || *obj.ObservedGeneration != obj.Generation {
					r.enqueue(obj.ID)
				}
			}
		}
	}
}
