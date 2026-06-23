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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/amorey/beehive/internal/storeapi"
	"github.com/amorey/beehive/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unsettledIDsStore is a fakeStore whose ListUnsettledIDs returns a fixed slice
// of IDs, used to exercise enqueueUnsettled without a real SQLite database.
type unsettledIDsStore struct {
	fakeStore
	ids []ObjectID
}

func (s *unsettledIDsStore) ListUnsettledIDs(_ context.Context, _ GroupKind) ([]ObjectID, error) {
	return s.ids, nil
}

// deletionPendingIDsStore is a fakeStore whose ListDeletionPendingIDs returns a
// fixed slice, used to exercise the GC backstop enqueue without a real database.
type deletionPendingIDsStore struct {
	fakeStore
	ids []ObjectID
}

func (s *deletionPendingIDsStore) ListDeletionPendingIDs(_ context.Context, _ GroupKind) ([]ObjectID, error) {
	return s.ids, nil
}

// allIDsStore reports no unsettled objects but a fixed full ID set, modeling a
// settled object that must still be reconciled at startup (e.g. to re-confirm a
// liveness condition after a restart).
type allIDsStore struct {
	fakeStore
	ids []ObjectID
}

func (s *allIDsStore) ListUnsettledIDs(_ context.Context, _ GroupKind) ([]ObjectID, error) {
	return nil, nil
}
func (s *allIDsStore) ListIDs(_ context.Context, _ GroupKind) ([]ObjectID, error) {
	return s.ids, nil
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

// TestReconcilerClearsBackoffOnSuccess verifies the per-id backoff entry created
// by a failing reconcile is removed once the object reconciles successfully —
// including the gone-object case, where reconcile returns nil for a missing row.
// This keeps backoffFor bounded by the set of currently-failing objects rather
// than leaking an entry per object that ever failed.
func TestReconcilerClearsBackoffOnSuccess(t *testing.T) {
	calls := 0
	succeeded := make(chan struct{})
	adapter := &fakeAdapter{
		reconcileFn: func(_ context.Context, _ ObjectID) (Result, error) {
			calls++
			if calls == 1 {
				return Result{}, errors.New("transient") // creates a backoff entry
			}
			// Object is now gone: reconcile reports success (mirrors the
			// ErrNotFound -> nil path), which must clear the backoff entry.
			close(succeeded)
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
	waitClosed(t, succeeded, "retry reconcile to succeed")
	cancel()
	waitClosed(t, done, "run to exit") // worker's clearBackoff has run by now

	r.backoffMu.Lock()
	remaining := len(r.backoffFor)
	r.backoffMu.Unlock()
	assert.Equal(t, 0, remaining, "backoff entry must be cleared after a successful reconcile")
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

func (c *reconcileCapture) Reconcile(_ context.Context, _ ControllerClient[tStatus], obj *Object[tSpec, tStatus]) (Result, error) {
	c.ch <- obj
	return Result{}, nil
}

// TestDependencyRequeue verifies the end-to-end auto-requeue: once D depends_on
// T, an observable change to T requeues D's reconcile — across the store, with
// no controller-to-controller call.
func TestDependencyRequeue(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	bh, err := New(store)
	require.NoError(t, err)

	gk := GroupKind{Kind: "Widget"}
	reconciled := make(chan *Object[tSpec, tStatus], 16)
	// Resync disabled so the dependency waker is the only thing that can requeue
	// an already-settled object — no timer noise.
	_, err = Register(bh, gk, &reconcileCapture{ch: reconciled}, WithResyncInterval(0))
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[tSpec, tStatus](bh, gk)
	target, err := client.Create(ctx, tSpec{})
	require.NoError(t, err)
	dep, err := client.Create(ctx, tSpec{})
	require.NoError(t, err)

	// Drain the two creation-driven reconciles so the channel is quiet before we
	// trigger the dependency path.
	seen := map[ObjectID]bool{}
	for len(seen) < 2 {
		select {
		case obj := <-reconciled:
			seen[obj.ID] = true
		case <-time.After(testTimeout):
			t.Fatal("creation reconciles did not arrive")
		}
	}

	require.NoError(t, store.AddRef(ctx, dep.ID, target.ID, "depends_on"))

	// An observable change to the target must wake the dependent.
	_, err = store.SetCondition(ctx, GroupKind{Group: target.Group, Kind: target.Kind}, target.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	select {
	case obj := <-reconciled:
		assert.Equal(t, dep.ID, obj.ID, "the dependent is the object requeued by the waker")
	case <-time.After(testTimeout):
		t.Fatal("dependent was not requeued after the target changed")
	}
}

// TestStartToleratesWatchError verifies that a dependency-watch subscription
// failure is non-fatal: Start (which now establishes the watch synchronously)
// still succeeds and the controller runs — only the waker is skipped, and the
// controller still resyncs on its own timer.
func TestStartToleratesWatchError(t *testing.T) {
	bh, err := New(&watcherStore{err: errBoom}, WithResyncInterval(0))
	require.NoError(t, err)
	_, err = Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	assert.Equal(t, beehiveRunning, bh.state)
	_ = stop(context.Background())
}

// blockingDepsStore parks the dependency waker inside ListIncomingRefs — after it
// has read a Modified event but before it re-enters Beehive's mutex via
// enqueueIfRegistered — so a test can drive a precise interleaving with Stop.
type blockingDepsStore struct {
	watcherStore
	entered chan struct{} // closed-by-send when the waker reaches ListIncomingRefs
	release chan struct{} // close to let the waker proceed to enqueueIfRegistered
}

func (s *blockingDepsStore) ListIncomingRefs(context.Context, ObjectID, Relation) ([]Referrer, error) {
	s.entered <- struct{}{}
	<-s.release
	// One referrer for an unregistered kind: enough to make the waker re-enter
	// bh.mu via enqueueIfRegistered (the registration check happens after Lock).
	return []Referrer{{ID: 1, Kind: "Widget"}}, nil
}

// TestStopDoesNotDeadlockWithActiveWaker guards the invariant that Stop never
// holds bh.mu while draining the wakers: a waker that re-enters bh.mu via
// enqueueIfRegistered mid-event must not deadlock against Stop, even with an
// unbounded Stop context.
func TestStopDoesNotDeadlockWithActiveWaker(t *testing.T) {
	fw := newFakeWatcher()
	store := &blockingDepsStore{
		watcherStore: watcherStore{w: fw},
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	bh := &Beehive{
		store:       store,
		reconcilers: map[GroupKind]*reconciler{},
		state:       beehiveRunning,
		cancel:      cancel,
	}
	bh.wg.Go(func() { bh.runDependencyWaker(ctx, fw) })

	// Drive the waker to the point where it has consumed a Modified event and is
	// parked just before re-entering bh.mu.
	fw.push(WatchEventModified, &RawObject{ID: 1})
	<-store.entered

	stopped := make(chan struct{})
	go func() {
		_ = bh.stop(context.Background()) // unbounded: a lock held across the wait would hang forever
		close(stopped)
	}()

	// Stop cancels under bh.mu, so ctx.Done means Stop is committed to tearing
	// down. Releasing the waker only now guarantees it contends for bh.mu against
	// a Stop that, in the buggy version, still holds it.
	<-ctx.Done()
	close(store.release)

	select {
	case <-stopped:
	case <-time.After(testTimeout):
		t.Fatal("Stop deadlocked against an active dependency waker")
	}
}

// recordingDepsStore reports ListIncomingRefs calls on a channel and serves a preset
// watcher (via the embedded watcherStore), so a test can observe exactly which
// events drive a wake.
type recordingDepsStore struct {
	watcherStore
	calls chan ObjectID
}

func (s *recordingDepsStore) ListIncomingRefs(_ context.Context, toID ObjectID, _ Relation) ([]Referrer, error) {
	s.calls <- toID
	return nil, nil
}

// TestDependencyWakerWakesOnChange verifies the waker reacts to both Added and
// Modified events. The conflating hub can coalesce a create-then-modify into a
// single Added, so skipping Added would drop the wake; a brand-new object
// usually has no dependents (the lookup is then a cheap no-op), making the
// over-wake harmless. Deleted is still ignored (a gone object has no dependents
// to requeue).
func TestDependencyWakerWakesOnChange(t *testing.T) {
	fw := newFakeWatcher()
	calls := make(chan ObjectID, 1)
	bh := &Beehive{store: &recordingDepsStore{watcherStore: watcherStore{w: fw}, calls: calls}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		bh.runDependencyWaker(ctx, fw)
		close(done)
	}()

	fw.push(WatchEventAdded, &RawObject{ID: 1})
	select {
	case id := <-calls:
		assert.Equal(t, ObjectID(1), id, "Added event wakes dependents (a coalesced create+modify)")
	case <-time.After(testTimeout):
		t.Fatal("Added event did not trigger a wake")
	}

	fw.push(WatchEventModified, &RawObject{ID: 2})
	select {
	case id := <-calls:
		assert.Equal(t, ObjectID(2), id, "Modified event wakes dependents of the changed object")
	case <-time.After(testTimeout):
		t.Fatal("Modified event did not trigger a wake")
	}

	fw.push(WatchEventDeleted, &RawObject{ID: 3})
	select {
	case <-calls:
		t.Fatal("Deleted event triggered a dependents wake")
	case <-time.After(200 * time.Millisecond):
	}

	cancel()
	waitClosed(t, done, "waker to exit")
}

// errDepsStore returns an error from ListIncomingRefs.
type errDepsStore struct{ fakeStore }

func (*errDepsStore) ListIncomingRefs(context.Context, ObjectID, Relation) ([]Referrer, error) {
	return nil, errBoom
}

// TestWakeDependentsListError verifies a failed dependents lookup is swallowed:
// the target still reconciled, and the resync backstop will retry the waking.
func TestWakeDependentsListError(t *testing.T) {
	bh := &Beehive{store: &errDepsStore{}}
	bh.wakeDependents(context.Background(), 1)
}

// TestDependencyWakerStreamEnd verifies the waker exits when its watch stream
// ends (channel closed), not only on context cancellation.
func TestDependencyWakerStreamEnd(t *testing.T) {
	fw := newFakeWatcher()
	bh := &Beehive{store: &watcherStore{w: fw}}

	done := make(chan struct{})
	go func() {
		bh.runDependencyWaker(context.Background(), fw)
		close(done)
	}()

	fw.endStream()
	waitClosed(t, done, "waker to exit on stream end")
}

// TestStartupEnqueuesAllNotJustUnsettled verifies that run's startup enqueue
// reconciles every object, not only unsettled ones. A settled object (empty
// ListUnsettledIDs) must still be reconciled at startup so a controller can
// re-confirm process-scoped state like liveness conditions. With resync
// disabled, the startup enqueue is the only thing that could drive it.
func TestStartupEnqueuesAllNotJustUnsettled(t *testing.T) {
	const objID = ObjectID(7)
	reconciled := make(chan ObjectID, 1)
	adapter := &fakeAdapter{
		reconcileFn: func(_ context.Context, id ObjectID) (Result, error) {
			select {
			case reconciled <- id:
			default:
			}
			return Result{}, nil
		},
	}
	r := &reconciler{
		adapter:          adapter,
		store:            &allIDsStore{ids: []ObjectID{objID}},
		work:             newWorkQueue(),
		resyncInterval:   0,
		maxRetryInterval: time.Second,
		backoffFor:       make(map[ObjectID]time.Duration),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runInBackground(r, ctx)

	select {
	case got := <-reconciled:
		assert.Equal(t, objID, got)
	case <-time.After(testTimeout):
		t.Fatal("settled object was not reconciled at startup")
	}

	cancel()
	waitClosed(t, done, "run to return after cancel")
}

// TestStartupReconcileSkipsSettled verifies that the non-default strategies do
// not reconcile a settled object at startup. allIDsStore reports the object via
// ListIDs but not ListUnsettledIDs, so StartupReconcileUnsettled (empty unsettled
// set) and StartupReconcileNone (no startup pass) both leave it untouched.
func TestStartupReconcileSkipsSettled(t *testing.T) {
	for _, strategy := range []StartupReconcileStrategy{StartupReconcileUnsettled, StartupReconcileNone} {
		t.Run(fmt.Sprintf("strategy=%d", strategy), func(t *testing.T) {
			reconciled := make(chan ObjectID, 1)
			adapter := &fakeAdapter{
				reconcileFn: func(_ context.Context, id ObjectID) (Result, error) {
					select {
					case reconciled <- id:
					default:
					}
					return Result{}, nil
				},
			}
			r := &reconciler{
				adapter:          adapter,
				store:            &allIDsStore{ids: []ObjectID{7}},
				work:             newWorkQueue(),
				resyncInterval:   0,
				maxRetryInterval: time.Second,
				startupReconcile: strategy,
				backoffFor:       make(map[ObjectID]time.Duration),
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := runInBackground(r, ctx)

			select {
			case got := <-reconciled:
				t.Fatalf("settled object %d reconciled despite strategy %d", got, strategy)
			case <-time.After(200 * time.Millisecond):
			}

			cancel()
			waitClosed(t, done, "run to return after cancel")
		})
	}
}

// TestEnqueueNilStoreNoop verifies the enqueue helpers are no-ops (no panic)
// when the reconciler has no store, as in the minimal test reconcilers.
func TestEnqueueNilStoreNoop(t *testing.T) {
	r := &reconciler{}
	r.enqueueUnsettled(context.Background())
	r.enqueueAll(context.Background())
}

// TestEnqueueUnsettledEnqueuesReturnedIDs verifies that enqueueUnsettled enqueues
// exactly the IDs returned by ListUnsettledIDs, in order.
func TestEnqueueUnsettledEnqueuesReturnedIDs(t *testing.T) {
	r := &reconciler{
		store:      &unsettledIDsStore{ids: []ObjectID{42, 99}},
		work:       newWorkQueue(),
		backoffFor: make(map[ObjectID]time.Duration),
	}

	r.enqueueUnsettled(context.Background())

	r.work.mu.Lock()
	items := append([]ObjectID(nil), r.work.items...)
	r.work.mu.Unlock()
	assert.Equal(t, []ObjectID{42, 99}, items)
}

func TestEnqueueDeletionPending(t *testing.T) {
	r := &reconciler{
		store:      &deletionPendingIDsStore{ids: []ObjectID{7, 13}},
		work:       newWorkQueue(),
		backoffFor: make(map[ObjectID]time.Duration),
	}

	r.enqueueDeletionPending(context.Background())

	r.work.mu.Lock()
	items := append([]ObjectID(nil), r.work.items...)
	r.work.mu.Unlock()
	assert.Equal(t, []ObjectID{7, 13}, items)
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
		store:            &unsettledIDsStore{ids: []ObjectID{objID}},
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

// TestReconcilerNoConcurrentReconcileOfSameID hammers a single object with
// re-enqueues while it is mid-reconcile, under multiple workers. The work
// queue's processing-hold must keep any second worker from dispatching the same
// id, so the object is never reconciled by two goroutines at once.
func TestReconcilerNoConcurrentReconcileOfSameID(t *testing.T) {
	const workers = 4
	const objID = ObjectID(1)

	inReconcile := make(chan struct{}) // closed when the first reconcile starts
	release := make(chan struct{})     // unblocks the first reconcile
	var startOnce sync.Once

	var (
		mu        sync.Mutex
		active    int
		maxActive int
	)

	adapter := &fakeAdapter{
		reconcileFn: func(_ context.Context, _ ObjectID) (Result, error) {
			mu.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			first := active == 1 && maxActive == 1
			mu.Unlock()

			if first {
				// Hold the object while the test piles on re-adds; without the
				// processing-hold this is exactly when a second worker would
				// dispatch the same id.
				startOnce.Do(func() { close(inReconcile) })
				<-release
			}

			mu.Lock()
			active--
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

	r.enqueue(objID)
	waitClosed(t, inReconcile, "first reconcile to start")

	for range 50 {
		r.enqueue(objID)
	}

	close(release)
	cancel()
	waitClosed(t, done, "run to exit")

	assert.Equal(t, 1, maxActive, "the same object must never be reconciled by two workers at once")
}

func TestNextBackoffDefaultBase(t *testing.T) {
	// When baseRetryInterval is 0, nextBackoff falls back to defaultBaseRetryInterval.
	r := &reconciler{
		backoffFor:       make(map[ObjectID]time.Duration),
		maxRetryInterval: time.Minute,
		// baseRetryInterval left as zero
	}
	d := r.nextBackoff(1)
	assert.Equal(t, defaultBaseRetryInterval, d)
}

func TestNextBackoffDoubles(t *testing.T) {
	r := &reconciler{
		backoffFor:        make(map[ObjectID]time.Duration),
		maxRetryInterval:  time.Minute,
		baseRetryInterval: 10 * time.Millisecond,
	}
	first := r.nextBackoff(1)
	assert.Equal(t, 10*time.Millisecond, first)
	second := r.nextBackoff(1) // cur != 0, so it doubles
	assert.Equal(t, 20*time.Millisecond, second)
}

func TestNextBackoffCaps(t *testing.T) {
	r := &reconciler{
		backoffFor:        make(map[ObjectID]time.Duration),
		maxRetryInterval:  50 * time.Millisecond,
		baseRetryInterval: 40 * time.Millisecond,
	}
	first := r.nextBackoff(1)
	assert.Equal(t, 40*time.Millisecond, first)
	// 40ms * 2 = 80ms > 50ms cap → capped at 50ms.
	second := r.nextBackoff(1)
	assert.Equal(t, 50*time.Millisecond, second)
}

// listCallStore signals a channel each time ListUnsettledIDs is called, so the
// test can wait for the resync tick to fire without using time.Sleep.
type listCallStore struct {
	fakeStore
	callCh chan struct{}
}

func (s *listCallStore) ListUnsettledIDs(_ context.Context, _ GroupKind) ([]ObjectID, error) {
	select {
	case s.callCh <- struct{}{}:
	default:
	}
	return nil, nil
}

func TestRunResyncsOnTick(t *testing.T) {
	store := &listCallStore{callCh: make(chan struct{}, 10)}
	r := &reconciler{
		store:          store,
		work:           newWorkQueue(),
		resyncInterval: 5 * time.Millisecond,
		backoffFor:     make(map[ObjectID]time.Duration),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	// Drain the initial startup enqueueUnsettled call.
	select {
	case <-store.callCh:
	case <-time.After(testTimeout):
		t.Fatal("initial enqueueUnsettled not called")
	}

	// Wait for at least one resync-tick-driven enqueueUnsettled call.
	select {
	case <-store.callCh:
	case <-time.After(testTimeout):
		t.Fatal("resync tick did not call enqueueUnsettled")
	}

	cancel()
	waitClosed(t, done, "run to return after cancel")
}

func TestRawToTypedSpecUnmarshalError(t *testing.T) {
	_, err := rawToTyped[tSpec, tStatus](&RawObject{Spec: []byte("not-json")}, nil)
	require.Error(t, err)
}

func TestRawToTypedMapsConditions(t *testing.T) {
	specJSON, err := json.Marshal(tSpec{})
	require.NoError(t, err)
	raw := &RawObject{Spec: specJSON, Conditions: []storeapi.Condition{
		{Type: "Ready", Status: "True", Reason: "Up", Message: "ok", Liveness: true},
		{Type: "Healthy", Status: "False"},
	}}

	obj, err := rawToTyped[tSpec, tStatus](raw, nil)
	require.NoError(t, err)
	require.Len(t, obj.Conditions, 2)
	assert.Equal(t, "Ready", obj.Conditions[0].Type)
	assert.Equal(t, ConditionTrue, obj.Conditions[0].Status)
	assert.Equal(t, "Up", obj.Conditions[0].Reason)
	assert.Equal(t, "ok", obj.Conditions[0].Message)
	assert.True(t, obj.Conditions[0].Liveness)
	assert.Equal(t, ConditionFalse, obj.Conditions[1].Status)
}

func TestRawToTypedStatusUnmarshalError(t *testing.T) {
	specJSON, err := json.Marshal(tSpec{})
	require.NoError(t, err)
	_, err = rawToTyped[tSpec, tStatus](&RawObject{Spec: specJSON, Status: []byte("not-json")}, nil)
	require.Error(t, err)
}

// getObjectBadSpecStore is a Store whose GetObject returns a RawObject with
// invalid spec JSON, exercising the rawToTyped error path inside
// typedController.reconcile. Within is inherited from fakeStore (inline passthrough).
type getObjectBadSpecStore struct {
	fakeStore
}

func (s *getObjectBadSpecStore) GetObject(_ context.Context, id ObjectID) (*RawObject, error) {
	return &RawObject{ID: id, Kind: "Widget", Spec: []byte("not-json")}, nil
}

func TestTypedControllerReconcileRawToTypedError(t *testing.T) {
	bh := &Beehive{store: &getObjectBadSpecStore{}}
	inner := &noopController[tSpec, tStatus]{}
	tc := &typedController[tSpec, tStatus]{
		gk:    GroupKind{Kind: "Widget"},
		bh:    bh,
		inner: inner,
	}
	_, err := tc.reconcile(context.Background(), 1)
	require.Error(t, err)
}

// getObjectErrorStore returns an error from GetObject to exercise path A in
// typedController.reconcile (the GetObject error before rawToTyped). Within is
// inherited from fakeStore (inline passthrough).
type getObjectErrorStore struct {
	fakeStore
}

func (s *getObjectErrorStore) GetObject(_ context.Context, _ ObjectID) (*RawObject, error) {
	return nil, errBoom
}

func TestTypedControllerReconcileGetObjectError(t *testing.T) {
	bh := &Beehive{store: &getObjectErrorStore{}}
	inner := &noopController[tSpec, tStatus]{}
	tc := &typedController[tSpec, tStatus]{
		gk:    GroupKind{Kind: "Widget"},
		bh:    bh,
		inner: inner,
	}
	_, err := tc.reconcile(context.Background(), 1)
	require.Error(t, err)
}

// notFoundStore returns ErrNotFound from GetObject, modeling an object that was
// already collected (by a prior pass, a cascade, or the backstop) between its
// enqueue and this reconcile.
type notFoundStore struct {
	fakeStore
}

func (s *notFoundStore) GetObject(_ context.Context, _ ObjectID) (*RawObject, error) {
	return nil, ErrNotFound
}

func TestTypedControllerReconcileMissingIDIsTerminal(t *testing.T) {
	bh := &Beehive{store: &notFoundStore{}}
	tc := &typedController[tSpec, tStatus]{
		gk:    GroupKind{Kind: "Widget"},
		bh:    bh,
		inner: &noopController[tSpec, tStatus]{},
	}
	// A gone object is a no-op success, not a retryable error: returning the error
	// would retry the missing id forever on backoff.
	result, err := tc.reconcile(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, Result{}, result, "no requeue for a vanished object")
}

// notFoundReturningController returns ErrNotFound from its own reconcile logic —
// e.g. an AddDependency to a target that was deleted. That is a real failure to
// retry, not the "queued object already gone" no-op.
type notFoundReturningController struct{}

func (notFoundReturningController) Reconcile(context.Context, ControllerClient[tStatus], *Object[tSpec, tStatus]) (Result, error) {
	return Result{}, ErrNotFound
}

func TestTypedControllerReconcilePropagatesControllerNotFound(t *testing.T) {
	ctx := context.Background()

	s, err := sqlite.OpenMemory()
	require.NoError(t, err)
	defer s.Close()

	specJSON, err := json.Marshal(tSpec{})
	require.NoError(t, err)
	raw, err := s.CreateObject(ctx, &RawObject{Kind: "Widget", Spec: specJSON})
	require.NoError(t, err)

	tc := &typedController[tSpec, tStatus]{
		gk:    GroupKind{Kind: "Widget"},
		bh:    &Beehive{store: s},
		inner: notFoundReturningController{},
	}
	// The object exists; only the controller returned ErrNotFound. It must surface
	// so the worker retries, not be swallowed as a vanished-object no-op.
	_, err = tc.reconcile(ctx, raw.ID)
	require.ErrorIs(t, err, ErrNotFound)
}

// requeueController always asks for a periodic requeue, even while its object is
// finalizing — the pattern that would re-schedule a just-collected id.
type requeueController struct{}

func (requeueController) Reconcile(context.Context, ControllerClient[tStatus], *Object[tSpec, tStatus]) (Result, error) {
	return Result{RequeueAfter: time.Minute}, nil
}

func TestTypedControllerReconcileDropsRequeueWhenCollected(t *testing.T) {
	ctx := context.Background()

	s, err := sqlite.OpenMemory()
	require.NoError(t, err)
	defer s.Close()

	specJSON, err := json.Marshal(tSpec{})
	require.NoError(t, err)
	raw, err := s.CreateObject(ctx, &RawObject{Kind: "Widget", Spec: specJSON})
	require.NoError(t, err)
	_, _, err = s.RequestDeletion(ctx, GroupKind{Kind: "Widget"}, raw.ID)
	require.NoError(t, err)

	tc := &typedController[tSpec, tStatus]{
		gk:    GroupKind{Kind: "Widget"},
		bh:    &Beehive{store: s},
		inner: requeueController{},
	}
	// GC removes the unfinalized, deletion-pending row; the controller's
	// RequeueAfter must be dropped so the worker doesn't reschedule a dead id.
	result, err := tc.reconcile(ctx, raw.ID)
	require.NoError(t, err)
	assert.Equal(t, Result{}, result, "requeue dropped because the row was collected")

	_, err = s.GetObject(ctx, raw.ID)
	require.ErrorIs(t, err, ErrNotFound)
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

// funcController is a test Controller whose Reconcile delegates to fn (given the
// ControllerClient passed into Reconcile). If signal is non-nil it is closed
// once, after fn's first call, so a test can wait for the reconcile to have run.
type funcController struct {
	once   sync.Once
	signal chan struct{}
	fn     func(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error)
}

func (c *funcController) Reconcile(ctx context.Context, client ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
	res, err := c.fn(ctx, client, obj)
	if c.signal != nil {
		c.once.Do(func() { close(c.signal) })
	}
	return res, err
}

// TestReconcilePersistsWritesOnError pins the autocommit model: reconcile no
// longer runs under an enclosing transaction, so a write that committed before
// Reconcile returns an error stays committed. The error still surfaces (the worker
// retries), and the level loop re-derives from the persisted state.
func TestReconcilePersistsWritesOnError(t *testing.T) {
	ctx := context.Background()

	s, err := sqlite.OpenMemory()
	require.NoError(t, err)
	defer s.Close()

	specJSON, err := json.Marshal(cSpec{})
	require.NoError(t, err)
	raw, err := s.CreateObject(ctx, &RawObject{Kind: clientTestGK.Kind, Spec: specJSON})
	require.NoError(t, err)

	bh := &Beehive{store: s}
	tc := &typedController[cSpec, cStatus]{
		gk:     clientTestGK,
		bh:     bh,
		client: &controllerClientImpl[cStatus]{bh: bh, gk: clientTestGK},
		inner: &funcController{fn: func(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
			if err := cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "written"}); err != nil {
				return Result{}, err
			}
			return Result{}, errBoom
		}},
	}

	_, rerr := tc.reconcile(ctx, raw.ID)
	require.ErrorIs(t, rerr, errBoom, "the reconcile error still surfaces for retry")

	got, err := s.GetObject(ctx, raw.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status, "the status write committed despite the reconcile error")
	assert.NotNil(t, got.ObservedGeneration)
}

// TestReconcileRunsGCAfterCommittedWritesOnError guards against stranding: a
// deleting controller clears its last finalizer (which commits on its own) and
// then returns an error. Because the write already landed, GC must still run — the
// now-unblocked deletion-pending row must be collected, not left forever (the
// resync sweeper is disabled here, so the in-reconcile collect is the only driver).
func TestReconcileRunsGCAfterCommittedWritesOnError(t *testing.T) {
	ctx := context.Background()

	s, err := sqlite.OpenMemory()
	require.NoError(t, err)
	defer s.Close()

	specJSON, err := json.Marshal(cSpec{})
	require.NoError(t, err)
	raw, err := s.CreateObject(ctx, &RawObject{
		Kind: clientTestGK.Kind, Spec: specJSON, Finalizers: []string{"f"},
	})
	require.NoError(t, err)
	_, _, err = s.RequestDeletion(ctx, clientTestGK, raw.ID)
	require.NoError(t, err)

	bh := &Beehive{store: s}
	tc := &typedController[cSpec, cStatus]{
		gk:     clientTestGK,
		bh:     bh,
		client: &controllerClientImpl[cStatus]{bh: bh, gk: clientTestGK},
		inner: &funcController{fn: func(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
			if err := cc.DeleteFinalizer(ctx, obj.ID, "f"); err != nil {
				return Result{}, err
			}
			return Result{}, errBoom
		}},
	}

	_, _ = tc.reconcile(ctx, raw.ID)

	_, err = s.GetObject(ctx, raw.ID)
	require.ErrorIs(t, err, ErrNotFound,
		"the committed finalizer clear must let GC collect the row even though reconcile errored")
}

// statusSettingController writes a fixed status on the first Reconcile call and
// closes reconciledCh.
type statusSettingController struct {
	once         sync.Once
	reconciledCh chan struct{}
}

func (c *statusSettingController) Reconcile(ctx context.Context, client ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
	if err := client.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"}); err != nil {
		return Result{}, err
	}
	c.once.Do(func() { close(c.reconciledCh) })
	return Result{}, nil
}

// specEchoController writes cStatus{Val: obj.Spec.Val} on every Reconcile.
// firstDone closes after the first successful reconcile; secondCh closes once a
// reconcile observes generation 2, signalling that the spec update — not merely a
// second reconcile — was seen.
type specEchoController struct {
	firstOnce sync.Once
	once      sync.Once
	firstDone chan struct{}
	secondCh  chan struct{}
}

func (c *specEchoController) Reconcile(ctx context.Context, client ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
	if err := client.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: obj.Spec.Val}); err != nil {
		return Result{}, err
	}
	c.firstOnce.Do(func() { close(c.firstDone) })
	// Gate on the observed generation, not a reconcile count: a duplicate startup
	// reconcile of the original generation (the startup pass can race the Create's
	// own enqueue) must not be mistaken for the update being reconciled.
	if obj.Generation >= 2 {
		c.once.Do(func() { close(c.secondCh) })
	}
	return Result{}, nil
}

// deletionTrackingController signals reconciled after the first successful
// reconcile and deleted when the object's DeletionRequestedAt is set.
type deletionTrackingController struct {
	reconcileOne sync.Once
	deleteOne    sync.Once
	reconciled   chan struct{}
	deleted      chan struct{}
}

func (c *deletionTrackingController) Reconcile(ctx context.Context, client ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
	if obj.DeletionRequestedAt != nil {
		c.deleteOne.Do(func() { close(c.deleted) })
		return Result{}, nil
	}
	if err := client.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"}); err != nil {
		return Result{}, err
	}
	c.reconcileOne.Do(func() { close(c.reconciled) })
	return Result{}, nil
}

func TestIntegrationCreateTriggersReconcile(t *testing.T) {
	ctx := context.Background()

	bh, err := New(newClientTestStore(t), WithResyncInterval(0))
	require.NoError(t, err)

	ctrl := &statusSettingController{reconciledCh: make(chan struct{})}
	_, err = Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)

	waitClosed(t, ctrl.reconciledCh, "first reconcile")

	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status)
	assert.Equal(t, "done", got.Status.Val)
	require.NotNil(t, got.ObservedGeneration)
	assert.Equal(t, obj.Generation, *got.ObservedGeneration)
}

func TestIntegrationUpdateTriggersReconcile(t *testing.T) {
	ctx := context.Background()

	bh, err := New(newClientTestStore(t), WithResyncInterval(0))
	require.NoError(t, err)

	ctrl := &specEchoController{
		firstDone: make(chan struct{}),
		secondCh:  make(chan struct{}),
	}
	_, err = Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "v1"})
	require.NoError(t, err)

	// Wait for the first reconcile before updating, so the update is genuinely a
	// distinct reconcile of generation 2 rather than being coalesced with the
	// create into a single pass.
	waitClosed(t, ctrl.firstDone, "first reconcile")

	_, err = client.Update(ctx, obj.ID, cSpec{Val: "v2"})
	require.NoError(t, err)

	waitClosed(t, ctrl.secondCh, "second reconcile after spec update")

	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status)
	assert.Equal(t, "v2", got.Status.Val)
}

func TestIntegrationDeleteTriggersReconcile(t *testing.T) {
	ctx := context.Background()

	bh, err := New(newClientTestStore(t), WithResyncInterval(0))
	require.NoError(t, err)

	ctrl := &deletionTrackingController{
		reconciled: make(chan struct{}),
		deleted:    make(chan struct{}),
	}
	_, err = Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)

	waitClosed(t, ctrl.reconciled, "first reconcile")

	require.NoError(t, client.Delete(ctx, obj.ID))
	waitClosed(t, ctrl.deleted, "reconcile after deletion requested")
}

// TestIntegrationWritePersistsAcrossReconcileError is the end-to-end counterpart
// of TestReconcilePersistsWritesOnError: a status write made during a reconcile
// that then returns an error stays committed, because reconcile no longer runs
// under a transaction. (To make a group of writes atomic, a controller uses
// ControllerClient.Within — see TestControllerClientWithin.)
func TestIntegrationWritePersistsAcrossReconcileError(t *testing.T) {
	ctx := context.Background()

	bh, err := New(newClientTestStore(t), WithResyncInterval(0))
	require.NoError(t, err)

	ctrl := &funcController{
		signal: make(chan struct{}),
		fn: func(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
			_ = cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "persisted"})
			return Result{}, errBoom
		},
	}
	_, err = Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)

	waitClosed(t, ctrl.signal, "reconcile wrote status before erroring")

	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status, "status write commits even though the reconcile returned an error")
	assert.Equal(t, "persisted", got.Status.Val)
}

// conditionSettingController sets a Ready=True condition on the first Reconcile,
// then closes reconciledCh.
type conditionSettingController struct {
	once         sync.Once
	reconciledCh chan struct{}
}

func (c *conditionSettingController) Reconcile(ctx context.Context, client ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
	if err := client.SetCondition(ctx, obj.ID, Condition{
		Type: "Ready", Status: ConditionTrue, Reason: "Provisioned",
	}); err != nil {
		return Result{}, err
	}
	c.once.Do(func() { close(c.reconciledCh) })
	return Result{}, nil
}

func TestIntegrationSetConditionCommitsAndFlows(t *testing.T) {
	ctx := context.Background()

	bh, err := New(newClientTestStore(t), WithResyncInterval(0))
	require.NoError(t, err)

	ctrl := &conditionSettingController{reconciledCh: make(chan struct{})}
	_, err = Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)

	waitClosed(t, ctrl.reconciledCh, "first reconcile")

	// Flows through Get.
	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	ready := findCondition(got.Conditions, "Ready")
	require.NotNil(t, ready, "condition set in Reconcile must be committed")
	assert.Equal(t, ConditionTrue, ready.Status)
	assert.Equal(t, "Provisioned", ready.Reason)

	// Flows through List.
	list, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.NotNil(t, findCondition(list[0].Conditions, "Ready"))
}

// TestIntegrationConditionPersistsAcrossReconcileError is the condition counterpart
// of TestIntegrationWritePersistsAcrossReconcileError: a condition set during a
// reconcile that then errors stays committed (no enclosing reconcile transaction).
func TestIntegrationConditionPersistsAcrossReconcileError(t *testing.T) {
	ctx := context.Background()

	bh, err := New(newClientTestStore(t), WithResyncInterval(0))
	require.NoError(t, err)

	ctrl := &funcController{
		signal: make(chan struct{}),
		fn: func(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
			_ = cc.SetCondition(ctx, obj.ID, Condition{Type: "Ready", Status: ConditionTrue})
			return Result{}, errBoom
		},
	}
	_, err = Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)

	waitClosed(t, ctrl.signal, "reconcile set condition before erroring")

	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	ready := findCondition(got.Conditions, "Ready")
	require.NotNil(t, ready, "condition commits even though the reconcile returned an error")
	assert.Equal(t, ConditionTrue, ready.Status)
}

func TestIntegrationStartupEnqueuesUnsettled(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)

	// Insert an object before beehive starts (simulating a previous process run).
	specJSON, err := json.Marshal(cSpec{Val: "pre-existing"})
	require.NoError(t, err)
	_, err = store.CreateObject(ctx, &RawObject{Kind: clientTestGK.Kind, Spec: specJSON})
	require.NoError(t, err)

	bh, err := New(store, WithResyncInterval(0))
	require.NoError(t, err)

	ctrl := &statusSettingController{reconciledCh: make(chan struct{})}
	_, err = Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	// Without startup enqueue this would time out (resync is disabled).
	waitClosed(t, ctrl.reconciledCh, "reconcile of pre-existing object at startup")
}
