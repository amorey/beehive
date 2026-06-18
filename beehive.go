package beehive

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const defaultResyncInterval = 30 * time.Second

// Beehive is the control plane: it owns the durable store and the set of
// registered controllers, and drives their reconcile loops between Start and
// Stop.
type Beehive struct {
	store          Store
	resyncInterval time.Duration

	mu          sync.Mutex
	reconcilers map[GroupKind]*reconciler
	started     bool
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// Start brings the control plane up: it starts every registered controller and
// launches its reconcile loop. It is an error to start twice.
//
// Start does not take a context: controller startup is assumed to be fast and
// non-blocking. Use Stop to tear the control plane down.
func (bh *Beehive) Start() error {
	bh.mu.Lock()
	defer bh.mu.Unlock()
	if bh.started {
		return fmt.Errorf("beehive: already started")
	}

	// runCtx lives for the lifetime of the control plane and drives the
	// reconcile loops. It is cancelled by Stop.
	runCtx, cancel := context.WithCancel(context.Background())
	bh.cancel = cancel

	// Start controllers first so they can stand up background workers before
	// any reconcile is dispatched.
	started := make([]*reconciler, 0, len(bh.reconcilers))
	for _, r := range bh.reconcilers {
		if err := r.adapter.start(); err != nil {
			// Roll back the controllers we already started, then abort.
			// The reconcile loops were never launched, so there's nothing to
			// drain; we just stop each controller. Cleanup runs unbounded.
			for _, s := range started {
				_ = s.adapter.stop(context.Background())
			}
			cancel()
			return fmt.Errorf("beehive: start controller %s/%s: %w", r.gk.Group, r.gk.Kind, err)
		}
		started = append(started, r)
	}

	// Launch reconcile loops only for controllers we started (ranging `started`,
	// not bh.reconcilers, keeps that invariant structural).
	for _, r := range started {
		bh.wg.Add(1)
		go func(r *reconciler) {
			defer bh.wg.Done()
			r.run(runCtx)
		}(r)
	}

	bh.started = true
	return nil
}

// Stop tears the control plane down: it cancels the reconcile loops, waits for
// them to drain (bounded by ctx), then stops every controller.
func (bh *Beehive) Stop(ctx context.Context) {
	bh.mu.Lock()
	defer bh.mu.Unlock()
	if !bh.started {
		return
	}

	bh.cancel()

	// Wait for reconcile loops to exit, but don't block past ctx.
	done := make(chan struct{})
	go func() {
		bh.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}

	for _, r := range bh.reconcilers {
		_ = r.adapter.stop(ctx)
	}

	bh.started = false
}

// New creates a control plane backed by store s. Register controllers on the
// returned Beehive before calling Start.
func New(s Store, opts ...Option) (*Beehive, error) {
	bh := &Beehive{
		store:          s,
		resyncInterval: defaultResyncInterval,
		reconcilers:    make(map[GroupKind]*reconciler),
	}
	for _, o := range opts {
		if err := o(bh); err != nil {
			return nil, err
		}
	}
	return bh, nil
}

// Register installs controller c for the resource kind gk. It must be called
// before Start, and only once per kind.
func Register[Spec, Status any](bh *Beehive, gk GroupKind, c Controller[Spec, Status], opts ...Option) error {
	bh.mu.Lock()
	defer bh.mu.Unlock()
	if bh.started {
		return fmt.Errorf("beehive: cannot register %s/%s after Start", gk.Group, gk.Kind)
	}
	if _, exists := bh.reconcilers[gk]; exists {
		return fmt.Errorf("beehive: controller already registered for %s/%s", gk.Group, gk.Kind)
	}

	r := &reconciler{
		gk:               gk,
		store:            bh.store,
		work:             newWorkQueue(),
		resyncInterval:   bh.resyncInterval,
		maxRetryInterval: defaultMaxRetryInterval,
		backoffFor:       make(map[ObjectID]time.Duration),
	}
	r.adapter = &typedController[Spec, Status]{gk: gk, bh: bh, inner: c}

	// Per-controller option overrides (e.g. WithResyncInterval, WithMaxRetryInterval).
	for _, o := range opts {
		if err := o(r); err != nil {
			return err
		}
	}

	bh.reconcilers[gk] = r
	return nil
}
