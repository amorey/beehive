package beehive

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/amorey/gochan/broadcast"
)

const defaultResyncInterval = 30 * time.Second

// rawWatchEvent is the untyped event that flows through a GroupKind's broadcast
// hub. clientImpl and controllerClientImpl decode it into WatchEvent[Spec,Status].
type rawWatchEvent struct {
	Type   WatchEventType
	Object *RawObject
}

// watchEventCollector accumulates watch events during a reconcile transaction so
// they are published after commit, not speculatively inside the transaction. A
// fresh collector is created per reconcile call and injected via context so
// concurrent reconcile goroutines each have their own queue.
type watchEventCollector struct {
	events []rawWatchEvent
}

func (c *watchEventCollector) add(ev rawWatchEvent) {
	c.events = append(c.events, ev)
}

// watchCollectorKey is the context key for a *watchEventCollector.
type watchCollectorKey struct{}

type beehiveState uint8

const (
	beehiveNew     beehiveState = iota // registered, not yet started
	beehiveRunning                     // Start succeeded, Stop not yet called
	beehiveStopped                     // Stop was called; instance is permanently unusable
)

// Beehive is the control plane: it owns the durable store and the set of
// registered controllers, and drives their reconcile loops between Start and
// Stop.
type Beehive struct {
	store          Store
	resyncInterval time.Duration
	concurrency    int // default worker count for all controllers; 0/1 = single-threaded

	mu          sync.Mutex
	reconcilers map[GroupKind]*reconciler
	// watchMu guards watchHubs independently of mu so that publishEvent (called
	// from reconcile goroutines) never blocks on the same lock that Stop holds
	// while waiting for those goroutines to drain.
	watchMu   sync.RWMutex
	watchHubs map[GroupKind]*broadcast.Hub[rawWatchEvent]
	state     beehiveState
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// Start brings the control plane up: it starts every registered controller and
// launches its reconcile loop. It is an error to start twice or after Stop.
// Beehive is a one-shot object: once stopped, create a new instance.
//
// Start does not take a context: controller startup is assumed to be fast and
// non-blocking. Use Stop to tear the control plane down.
func (bh *Beehive) Start() error {
	bh.mu.Lock()
	defer bh.mu.Unlock()
	switch bh.state {
	case beehiveStopped:
		return fmt.Errorf("beehive: already stopped; create a new Beehive to restart")
	case beehiveRunning:
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

	bh.state = beehiveRunning
	return nil
}

// Stop tears the control plane down: it cancels the reconcile loops, waits for
// them to drain (bounded by ctx), then stops every controller.
func (bh *Beehive) Stop(ctx context.Context) {
	bh.mu.Lock()
	defer bh.mu.Unlock()
	if bh.state != beehiveRunning {
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

	// Close every hub to signal active watchers that the control plane is
	// stopping. Any reconcile goroutines that outlived the stop deadline will
	// call publishEvent on a closed hub; Send returns an error that is silently
	// dropped, so stale events never escape.
	bh.watchMu.Lock()
	for _, hub := range bh.watchHubs {
		hub.Close()
	}
	bh.watchMu.Unlock()

	for _, r := range bh.reconcilers {
		_ = r.adapter.stop(ctx)
	}

	bh.state = beehiveStopped
}

// New creates a control plane backed by store s. Register controllers on the
// returned Beehive before calling Start.
func New(s Store, opts ...Option) (*Beehive, error) {
	bh := &Beehive{
		store:          s,
		resyncInterval: defaultResyncInterval,
		reconcilers:    make(map[GroupKind]*reconciler),
		watchHubs:      make(map[GroupKind]*broadcast.Hub[rawWatchEvent]),
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
	if bh.state != beehiveNew {
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
		concurrency:      bh.concurrency,
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
	bh.watchMu.Lock()
	bh.watchHubs[gk] = broadcast.New[rawWatchEvent](256)
	bh.watchMu.Unlock()
	return nil
}

// publishEvent sends ev to the broadcast hub for gk. It is a no-op if no hub
// exists for gk (e.g. no controller was registered). Send never blocks.
func (bh *Beehive) publishEvent(gk GroupKind, typ WatchEventType, raw *RawObject) {
	bh.watchMu.RLock()
	hub := bh.watchHubs[gk]
	bh.watchMu.RUnlock()
	if hub != nil {
		_ = hub.Sender().Send(rawWatchEvent{Type: typ, Object: raw})
	}
}

// emitOrCollect delivers ev to active watchers. If ctx carries a
// watchEventCollector (i.e. the call is inside a reconcile transaction), the
// event is queued and published only after commit. Otherwise it is published
// immediately. All ControllerClient write methods should use this helper so
// the two-path dispatch never needs to be repeated.
func (bh *Beehive) emitOrCollect(ctx context.Context, gk GroupKind, ev rawWatchEvent) {
	if coll, ok := ctx.Value(watchCollectorKey{}).(*watchEventCollector); ok {
		coll.add(ev)
	} else {
		bh.publishEvent(gk, ev.Type, ev.Object)
	}
}

// enqueueIfRegistered wakes the reconciler for (gk, id) if one exists.
// It is a no-op when gk has no registered controller (e.g. a client-only kind).
func (bh *Beehive) enqueueIfRegistered(gk GroupKind, id ObjectID) {
	bh.mu.Lock()
	r, ok := bh.reconcilers[gk]
	bh.mu.Unlock()
	if ok {
		r.enqueue(id)
	}
}
