package beehive

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/amorey/beehive/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// listOnlyStore is a fakeStore whose ListObjects returns a fixed slice, used
// to exercise enqueueUnsettled without a real SQLite database.
type listOnlyStore struct {
	fakeStore
	objs []*RawObject
}

func (s *listOnlyStore) ListObjects(_ context.Context, _ GroupKind) ([]*RawObject, error) {
	return s.objs, nil
}

// runInBackground starts r.run and returns a channel closed when it returns.
func runInBackground(r *reconciler, ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.run(ctx)
	}()
	return done
}

func TestRunExitsOnCancelWithResyncDisabled(t *testing.T) {
	// resyncInterval <= 0 means no ticker is created (NewTicker would panic).
	r := &reconciler{resyncInterval: 0}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	cancel()
	waitClosed(t, done, "run to return after cancel")
}

func TestRunExitsOnCancelWithResyncEnabled(t *testing.T) {
	// A long interval that won't fire during the test: the exit is driven by the
	// cancel, not by the ticker, so timing is irrelevant to the assertion.
	r := &reconciler{resyncInterval: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	cancel()
	waitClosed(t, done, "run to return after cancel")
}

// fakeAdapter is a controllerAdapter whose reconcile behaviour is supplied by
// the test via a function field.
type fakeAdapter struct {
	reconcileFn func(ctx context.Context, id ObjectID) (Result, error)
}

func (f *fakeAdapter) start() error                                        { return nil }
func (f *fakeAdapter) stop(_ context.Context) error                        { return nil }
func (f *fakeAdapter) reconcile(ctx context.Context, id ObjectID) (Result, error) {
	return f.reconcileFn(ctx, id)
}

func TestReconcilerRequeuesOnError(t *testing.T) {
	calls := 0
	doneCh := make(chan struct{})
	adapter := &fakeAdapter{
		reconcileFn: func(_ context.Context, _ ObjectID) (Result, error) {
			calls++
			if calls == 1 {
				return Result{}, errors.New("transient")
			}
			close(doneCh)
			return Result{}, nil
		},
	}

	r := &reconciler{
		adapter:           adapter,
		work:              newWorkQueue(),
		resyncInterval:    0,
		maxRetryInterval:  time.Second,
		baseRetryInterval: 5 * time.Millisecond,
		backoffFor:        make(map[ObjectID]time.Duration),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	r.enqueue(1)
	waitClosed(t, doneCh, "successful reconcile after error")
	cancel()
	waitClosed(t, done, "run to exit")
}

func TestReconcilerRequeueAfter(t *testing.T) {
	calls := 0
	doneCh := make(chan struct{})
	adapter := &fakeAdapter{
		reconcileFn: func(_ context.Context, _ ObjectID) (Result, error) {
			calls++
			if calls == 1 {
				return Result{RequeueAfter: 10 * time.Millisecond}, nil
			}
			close(doneCh)
			return Result{}, nil
		},
	}

	r := &reconciler{
		adapter:          adapter,
		work:             newWorkQueue(),
		resyncInterval:   0,
		maxRetryInterval: time.Second,
		backoffFor:       make(map[ObjectID]time.Duration),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	r.enqueue(1)
	waitClosed(t, doneCh, "second reconcile after RequeueAfter")
	cancel()
	waitClosed(t, done, "run to exit")
}

// reconcileCapture is a Controller whose Reconcile sends the received object to
// a channel so the test can inspect it.
type reconcileCapture struct {
	ch chan *Object[tSpec, tStatus]
}

func (c *reconcileCapture) Start(_ ControllerClient[tStatus]) error { return nil }
func (c *reconcileCapture) Stop(_ context.Context) error            { return nil }
func (c *reconcileCapture) Reconcile(_ context.Context, obj *Object[tSpec, tStatus]) (Result, error) {
	c.ch <- obj
	return Result{}, nil
}

// TestEnqueueUnsettledSkipsInFlight verifies that a resync does not re-enqueue
// an object whose reconcile is already in progress.
func TestEnqueueUnsettledSkipsInFlight(t *testing.T) {
	const objID = ObjectID(42)

	block := make(chan struct{})
	started := make(chan struct{})
	var startOnce sync.Once

	adapter := &fakeAdapter{
		reconcileFn: func(_ context.Context, _ ObjectID) (Result, error) {
			startOnce.Do(func() { close(started) })
			<-block
			return Result{}, nil
		},
	}

	r := &reconciler{
		adapter:          adapter,
		store:            &listOnlyStore{objs: []*RawObject{{ID: objID, Generation: 1}}},
		work:             newWorkQueue(),
		resyncInterval:   0,
		maxRetryInterval: time.Second,
		backoffFor:       make(map[ObjectID]time.Duration),
		concurrency:      2,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	r.enqueue(objID)
	waitClosed(t, started, "reconcile to start")

	// Simulate a resync tick while the reconcile is still in-flight.
	r.enqueueUnsettled(ctx)

	r.work.mu.Lock()
	qLen := len(r.work.items)
	r.work.mu.Unlock()
	assert.Equal(t, 0, qLen, "in-flight object must not be re-enqueued by resync")

	close(block)
	cancel()
	waitClosed(t, done, "run to exit")
}

func TestReconcilerConcurrency(t *testing.T) {
	const numObjects = 5
	const workers = 3

	gate := make(chan struct{})
	allStarted := make(chan struct{})
	var closeOnce sync.Once

	var (
		mu          sync.Mutex
		inFlight    int
		maxInFlight int
	)

	adapter := &fakeAdapter{
		reconcileFn: func(_ context.Context, _ ObjectID) (Result, error) {
			mu.Lock()
			inFlight++
			cur := inFlight
			if cur > maxInFlight {
				maxInFlight = cur
			}
			mu.Unlock()

			if cur == workers {
				closeOnce.Do(func() { close(allStarted) })
			}

			<-gate // block until test releases all workers

			mu.Lock()
			inFlight--
			mu.Unlock()
			return Result{}, nil
		},
	}

	r := &reconciler{
		adapter:          adapter,
		work:             newWorkQueue(),
		resyncInterval:   0,
		maxRetryInterval: time.Second,
		backoffFor:       make(map[ObjectID]time.Duration),
		concurrency:      workers,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	for i := ObjectID(1); i <= numObjects; i++ {
		r.enqueue(i)
	}

	waitClosed(t, allStarted, "3 concurrent reconciles to start")
	close(gate) // release all in-flight reconciles

	cancel()
	waitClosed(t, done, "run to exit")

	assert.GreaterOrEqual(t, maxInFlight, workers, "expected at least %d concurrent reconciles", workers)
}

func TestTypedControllerReconcile(t *testing.T) {
	ctx := context.Background()

	s, err := sqlite.OpenMemory()
	require.NoError(t, err)
	defer s.Close()

	specJSON, err := json.Marshal(tSpec{})
	require.NoError(t, err)
	raw, err := s.CreateObject(ctx, &RawObject{Kind: "Widget", Spec: specJSON})
	require.NoError(t, err)

	bh := &Beehive{store: s}
	capCh := make(chan *Object[tSpec, tStatus], 1)
	tc := &typedController[tSpec, tStatus]{
		gk:    GroupKind{Kind: "Widget"},
		bh:    bh,
		inner: &reconcileCapture{ch: capCh},
	}
	result, err := tc.reconcile(ctx, raw.ID)
	require.NoError(t, err)
	assert.Equal(t, Result{}, result)

	select {
	case obj := <-capCh:
		assert.Equal(t, raw.ID, obj.ID)
		assert.Equal(t, raw.Generation, obj.Generation)
		assert.Nil(t, obj.Status)
	default:
		t.Fatal("Reconcile was not called")
	}
}
