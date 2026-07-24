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
	"log/slog"
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

// pendingWakeIDsStore is a fakeStore whose ListPendingWakeIDs returns a fixed
// slice, used to exercise the durable-wake backstop enqueue without a real
// database — the sibling of unsettledIDsStore and deletionPendingIDsStore.
type pendingWakeIDsStore struct {
	fakeStore
	ids []ObjectID
}

func (s *pendingWakeIDsStore) ListPendingWakeIDs(context.Context, GroupKind) ([]ObjectID, error) {
	return s.ids, nil
}

// tickOnlyPendingWakeStore reports its owed wakes from the second call onward, so
// the startup enqueue sees an empty set and only a resync tick can supply the IDs.
// That is what makes the tick observable: the two calls are otherwise identical,
// and a test that let the startup pass answer would pass with the tick's enqueue
// deleted.
type tickOnlyPendingWakeStore struct {
	fakeStore
	ids []ObjectID

	mu    sync.Mutex
	calls int
}

func (s *tickOnlyPendingWakeStore) ListPendingWakeIDs(context.Context, GroupKind) ([]ObjectID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls == 1 {
		return nil, nil // the startup pass
	}
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

	require.NoError(t, addRef(ctx, store, dep.ID, target.ID, "depends_on"))

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

// dependentController is the dependent in the read-then-declare repros. Every
// pass reads the target, reports the target's Ready state as that pass saw it,
// and settles at obj.Generation — the settle being what hides a missed wake from
// the resync backstop, since ListUnsettledIDs then sees a converged object.
//
// afterRead, when set, runs between the read and the settle. That is where the
// in-band race lives: the controller declares the edge there, and the test parks
// it to land a change to the target inside the window. Left nil the controller
// only observes, which is the out-of-band spelling — there the declaration is the
// embedding application's, not a reconcile's.
type dependentController struct {
	client   Client[tSpec, tStatus]
	depID    ObjectID
	targetID ObjectID

	observed  chan bool // the target's Ready condition as each dep pass saw it
	afterRead func(ctx context.Context, cc ControllerClient[tStatus], target *Object[tSpec, tStatus]) error
}

func (c *dependentController) Reconcile(ctx context.Context, cc ControllerClient[tStatus], obj *Object[tSpec, tStatus]) (Result, error) {
	if obj.ID != c.depID {
		return Result{}, nil // the target's own reconcile is not under test
	}
	target, err := c.client.Get(ctx, c.targetID)
	if err != nil {
		return Result{}, err
	}
	ready := false
	for _, cond := range target.Conditions {
		if cond.Type == "Ready" {
			ready = cond.Status == ConditionTrue
		}
	}
	if c.afterRead != nil {
		if err := c.afterRead(ctx, cc, target); err != nil {
			return Result{}, err
		}
	}
	// Settling at obj.Generation is what hides a missed wake from the resync
	// backstop: ListUnsettledIDs sees a converged object.
	if err := cc.UpdateStatus(ctx, c.depID, obj.Generation, tStatus{}); err != nil {
		return Result{}, err
	}
	c.observed <- ready
	return Result{}, nil
}

// TestDependencyRequeueRaceOnDeclare pins the read-then-declare race: a change to
// the target that lands after the dependent read it but before AddDependency
// commits reaches nobody — wakeDependents resolves dependents at the instant of
// the change, and the edge did not exist yet. The dependent is left holding a
// stale read with no error, no condition, and (because it settled at its own
// generation) nothing for the resync backstop to notice.
func TestDependencyRequeueRaceOnDeclare(t *testing.T) {
	ctx := context.Background()
	db, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	store := &wakeProbeStore{Store: db, looked: make(chan struct{}, 8)}

	bh, err := New(store)
	require.NoError(t, err)

	gk := GroupKind{Kind: "Widget"}
	ctrl := &dependentController{observed: make(chan bool, 8)}
	var once sync.Once
	readDone := make(chan struct{}) // closed once the first pass has read the target
	proceed := make(chan struct{})  // closed by the test after it changes the target
	ctrl.afterRead = func(ctx context.Context, cc ControllerClient[tStatus], target *Object[tSpec, tStatus]) error {
		// First pass only: park between the read and the declaration so the test can
		// land its change to the target inside the window. Later passes declare
		// straight through, as a level-triggered controller re-asserting its edges.
		once.Do(func() {
			close(readDone)
			<-proceed
		})
		// The version the read above reflects — not a fresh one, which would claim
		// to have seen changes this pass did not.
		return cc.AddDependency(ctx, ctrl.depID, ctrl.targetID, target.ResourceVersion)
	}
	// Resync disabled so the dependency waker is the only thing that can requeue
	// the dependent — the backstop must not paper over the miss.
	_, err = Register(bh, gk, ctrl, WithResyncInterval(0))
	require.NoError(t, err)

	client := NewClient[tSpec, tStatus](bh, gk)
	ctrl.client = client

	// Create before Start so the ids are set before any reconcile can dispatch;
	// the startup pass then drives both objects.
	target, err := client.Create(ctx, tSpec{})
	require.NoError(t, err)
	dep, err := client.Create(ctx, tSpec{})
	require.NoError(t, err)
	ctrl.targetID, ctrl.depID = target.ID, dep.ID
	store.targetID = target.ID

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	// The dependent has read the target and not yet declared the edge.
	select {
	case <-readDone:
	case <-time.After(testTimeout):
		t.Fatal("dependent's first reconcile did not read the target")
	}

	// Change the target inside the window and wait for the waker to resolve its
	// dependents — with no edge yet, that lookup comes back empty and the change
	// is now permanently unclaimed. Only then let the declaration commit.
	store.resetLooked()
	_, err = store.SetCondition(ctx, gk, target.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	store.waitLooked(t)
	close(proceed)

	select {
	case ready := <-ctrl.observed:
		require.False(t, ready, "the first pass read the target before it went Ready")
	case <-time.After(testTimeout):
		t.Fatal("dependent's first reconcile did not finish")
	}

	// The edge is in place now and the target's change is still unobserved, so
	// the dependent must be reconciled again and see Ready.
	select {
	case ready := <-ctrl.observed:
		assert.True(t, ready, "the requeued pass observes the target's change")
	case <-time.After(testTimeout):
		t.Fatal("dependent was never requeued: the target's change landed between its read and AddDependency")
	}
}

// TestDependencyRequeueRaceOnOutOfBandDeclare is the out-of-band mirror of
// TestDependencyRequeueRaceOnDeclare: the same read-then-declare window, but with
// the two halves in different goroutines. The embedding application declares the
// edge through the ControllerClient Register handed it, after its own read of the
// target — so no reconcile is in flight to carry the miss, and the hole is a
// notch wider than the in-band one. In-band, the pass that loses the change at
// least runs to completion around the declaration; here the declaration is the
// only thing that happens, and AddDependency enqueues nothing: the edge appears
// with fromID already settled, so a change that landed before the commit reaches
// nobody and nothing re-derives it. With resync disabled the dependent holds a
// stale read forever, with no error, no condition and no log line.
func TestDependencyRequeueRaceOnOutOfBandDeclare(t *testing.T) {
	ctx := context.Background()
	db, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	store := &wakeProbeStore{Store: db, looked: make(chan struct{}, 8)}

	bh, err := New(store)
	require.NoError(t, err)

	gk := GroupKind{Kind: "Widget"}
	ctrl := &dependentController{observed: make(chan bool, 8)}
	// Resync disabled so the dependency waker is the only thing that can requeue
	// the dependent — the backstop must not paper over the miss.
	cc, err := Register(bh, gk, ctrl, WithResyncInterval(0))
	require.NoError(t, err)

	client := NewClient[tSpec, tStatus](bh, gk)
	ctrl.client = client

	// Create before Start: the waker's watch is events-only, so pre-Start creates
	// emit nothing into it and the only lookup the probe can see is the one the
	// test triggers below.
	target, err := client.Create(ctx, tSpec{})
	require.NoError(t, err)
	dep, err := client.Create(ctx, tSpec{})
	require.NoError(t, err)
	ctrl.targetID, ctrl.depID = target.ID, dep.ID
	store.targetID = target.ID

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	// The startup pass settles the dependent on a not-Ready target. From here on
	// only a requeue can make it look again.
	select {
	case ready := <-ctrl.observed:
		require.False(t, ready, "the startup pass reads the target before it goes Ready")
	case <-time.After(testTimeout):
		t.Fatal("dependent's startup reconcile did not run")
	}

	// The application changes the target and only then declares the edge — the
	// out-of-band spelling of read-then-declare. Waiting for the waker's lookup
	// makes the window deterministic: with no edge yet it comes back empty, so the
	// change is already unclaimed by the time AddDependency commits.
	store.resetLooked()
	_, err = store.SetCondition(ctx, gk, target.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	store.waitLooked(t)
	// target is the application's read of the target, taken before the change
	// above — so the version it carries is the one the decision to depend was
	// based on, and the target has since moved past it.
	require.NoError(t, cc.AddDependency(ctx, dep.ID, target.ID, target.ResourceVersion))

	// The edge is in place and the target's change is still unobserved, so the
	// dependent must be reconciled again and see Ready.
	select {
	case ready := <-ctrl.observed:
		assert.True(t, ready, "the requeued pass observes the target's change")
	case <-time.After(testTimeout):
		t.Fatal("dependent was never requeued: the target changed before the out-of-band AddDependency declared the edge")
	}
}

// TestDependencyRequeueLostAcrossRestart pins the durability half of the
// read-then-declare race: a wake that a process owes but never dispatches is
// gone, because the only record of it was the in-memory work queue.
//
// The two repros above are about *deriving* the wake; this one is about
// surviving it. Its diagnostic value lands once the edge-triggered wake exists:
// at that point the out-of-band repro passes while this one still fails, and the
// failure means exactly one thing — the signal was in-memory only. Until then it
// fails for the same reason they do, which is why all three are skipped together.
//
// The crash is spelled as a stopped work queue rather than a killed process: the
// change and the declaration both commit durably, and the wake they imply lands
// on a queue whose addLocked returns early on q.stopped. From the store's side
// that is indistinguishable from dying between the commit and the dispatch, and
// it needs no goroutine timing to be deterministic.
//
// The restart runs with WithStartupResync(false), which is load-bearing: under the
// default full pass the startup sweep reconciles everything and heals the crash for
// reasons that have nothing to do with dependencies, so the test would prove
// nothing. What must reach the dependent is the owed-work drain, which startup runs
// regardless — a wake that is owed is recorded work, not a re-confirm.
func TestDependencyRequeueLostAcrossRestart(t *testing.T) {
	ctx := context.Background()
	// One store, two control planes: the rows outlive the process, the work queue
	// does not. Owned by the test, since stop leaves the store open (see
	// Beehive.stop) — which is what makes the restart possible.
	db, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	gk := GroupKind{Kind: "Widget"}

	// --- first process: settle the dependent on a not-Ready target ---
	bh1, err := New(db)
	require.NoError(t, err)
	ctrl1 := &dependentController{observed: make(chan bool, 8)}
	// Resync disabled here and on the restart: the wake must be what requeues the
	// dependent, not a timer that happens to sweep it up.
	cc, err := Register(bh1, gk, ctrl1, WithResyncInterval(0))
	require.NoError(t, err)

	client1 := NewClient[tSpec, tStatus](bh1, gk)
	ctrl1.client = client1

	target, err := client1.Create(ctx, tSpec{})
	require.NoError(t, err)
	dep, err := client1.Create(ctx, tSpec{})
	require.NoError(t, err)
	ctrl1.targetID, ctrl1.depID = target.ID, dep.ID

	stop1, err := bh1.Start(ctx)
	require.NoError(t, err)

	select {
	case ready := <-ctrl1.observed:
		require.False(t, ready, "the startup pass reads the target before it goes Ready")
	case <-time.After(testTimeout):
		t.Fatal("dependent's startup reconcile did not run")
	}
	require.NoError(t, stop1(ctx))

	// --- the crash window: both writes commit, the wake reaches nobody ---
	// The target changes and only then is the edge declared, so the change is
	// already unclaimed when the edge appears — the out-of-band race. The
	// ControllerClient outlives the control plane (it holds the store, not the
	// loops), so the declaration commits normally; what it can no longer do is
	// reach a running queue.
	_, err = db.SetCondition(ctx, gk, target.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	require.NoError(t, cc.AddDependency(ctx, dep.ID, target.ID, target.ResourceVersion))

	// --- second process over the same store ---
	bh2, err := New(db)
	require.NoError(t, err)
	ctrl2 := &dependentController{
		observed: make(chan bool, 8),
		depID:    dep.ID,
		targetID: target.ID,
	}
	_, err = Register(bh2, gk, ctrl2,
		WithResyncInterval(0),
		WithStartupResync(false))
	require.NoError(t, err)
	ctrl2.client = NewClient[tSpec, tStatus](bh2, gk)

	stop2, err := bh2.Start(ctx)
	require.NoError(t, err)
	defer stop2(ctx)

	// The edge is durably in place and the target's change is still unobserved, so
	// the new process owes the dependent a reconcile that no live event will
	// deliver — nothing is going to change again.
	select {
	case ready := <-ctrl2.observed:
		assert.True(t, ready, "the recovered pass observes the target's change")
	case <-time.After(testTimeout):
		t.Fatal("dependent was never reconciled after restart: the owed wake died with the process that owed it")
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
	bh.wg.Go(func() { bh.runDependencyWaker(ctx, GroupKind{Kind: "Widget"}, fw) })

	// Drive the waker to the point where it has consumed a Modified event and is
	// parked just before re-entering bh.mu.
	fw.push(Modified, &RawObject{ID: 1})
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
		bh.runDependencyWaker(ctx, GroupKind{Kind: "Widget"}, fw)
		close(done)
	}()

	fw.push(Added, &RawObject{ID: 1})
	select {
	case id := <-calls:
		assert.Equal(t, ObjectID(1), id, "Added event wakes dependents (a coalesced create+modify)")
	case <-time.After(testTimeout):
		t.Fatal("Added event did not trigger a wake")
	}

	fw.push(Modified, &RawObject{ID: 2})
	select {
	case id := <-calls:
		assert.Equal(t, ObjectID(2), id, "Modified event wakes dependents of the changed object")
	case <-time.After(testTimeout):
		t.Fatal("Modified event did not trigger a wake")
	}

	fw.push(Deleted, &RawObject{ID: 3})
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
		bh.runDependencyWaker(context.Background(), GroupKind{Kind: "Widget"}, fw)
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
		adapter: adapter,
		store:   &allIDsStore{ids: []ObjectID{objID}},
		work:    newWorkQueue(),
		// Set explicitly: unlike the strategy enum this replaced, a bool's zero
		// value is the *off* state, so a reconciler built outside Register (as here)
		// does not inherit New's true default.
		startupResync:    true,
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

// recordingController signals every ID it reconciles and settles nothing. It
// exists to observe which objects a startup pass reaches, so it must not write
// status: settling would move the object out of the unsettled set mid-test.
type recordingController struct {
	reconciled chan ObjectID
}

func (c *recordingController) Reconcile(_ context.Context, _ ControllerClient[tStatus], obj *Object[tSpec, tStatus]) (Result, error) {
	select {
	case c.reconciled <- obj.ID:
	default:
	}
	return Result{}, nil
}

// TestSelfDrivenRecovery pins the primitives an embedder uses to drive reconciles
// on its own schedule: Store.ListUnsettledIDs reports exactly the objects owed a
// pass, and Client.Requeue dispatches one. Startup now drains owed work itself, so
// this is no longer the *only* way such an object gets reconciled — but it stays
// pinned as public surface, because a deployment that turns every ticker off still
// needs it for anything that falls behind after startup.
func TestSelfDrivenRecovery(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	gk := GroupKind{Kind: "Widget"}

	raw, err := store.CreateObject(ctx, &RawObject{
		Group: gk.Group, Kind: gk.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	bh, err := New(store, WithCatchupInterval(0), WithResyncInterval(0))
	require.NoError(t, err)
	ctrl := &recordingController{reconciled: make(chan ObjectID, 4)}
	_, err = Register(bh, gk, ctrl, WithStartupResync(false))
	require.NoError(t, err)
	client := NewClient[tSpec, tStatus](bh, gk)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	// Startup drains owed work regardless of the resync choice, so consume that
	// dispatch first — otherwise it, not the requeue below, could satisfy the
	// assertion.
	select {
	case <-ctrl.reconciled:
	case <-time.After(testTimeout):
		t.Fatal("startup did not drain the owed object")
	}

	// The embedder's own backstop, on whatever schedule it likes.
	ids, err := store.ListUnsettledIDs(ctx, gk)
	require.NoError(t, err)
	require.Equal(t, []ObjectID{raw.ID}, ids, "the unconverged row is what ListUnsettledIDs reports")
	require.NoError(t, client.Requeue(ctx, raw.ID))

	select {
	case got := <-ctrl.reconciled:
		assert.Equal(t, raw.ID, got)
	case <-time.After(testTimeout):
		t.Fatal("self-driven requeue never reconciled the object: the documented recovery path does not work")
	}
}

// TestStartupResyncDisabledSkipsSettled is the unit-level twin of the
// store-backed TestStartupResyncReconcilesSettled: allIDsStore reports the object
// via ListIDs but not via any owed-work listing, so with the startup resync off
// nothing enqueues it — the owed-work drain has nothing to drain.
func TestStartupResyncDisabledSkipsSettled(t *testing.T) {
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
		maxRetryInterval: time.Second,
		startupResync:    false,
		backoffFor:       make(map[ObjectID]time.Duration),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runInBackground(r, ctx)

	select {
	case got := <-reconciled:
		t.Fatalf("settled object %d reconciled with the startup resync off", got)
	case <-time.After(200 * time.Millisecond):
	}

	cancel()
	waitClosed(t, done, "run to return after cancel")
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

// errPendingWakeStore fails the durable-wake listing, so a test can drive
// enqueueFrom's skipped-pass branch — the one whose silence used to be
// indistinguishable from "nothing was owed".
type errPendingWakeStore struct {
	fakeStore
}

func (s *errPendingWakeStore) ListPendingWakeIDs(context.Context, GroupKind) ([]ObjectID, error) {
	return nil, errBoom
}

// TestEnqueueFromListErrorSkipsPass pins that a failed lister enqueues nothing and
// survives a reconciler built without a logger — the shape these tests use, and the
// one the new warn would panic on if it reached r.logger directly.
func TestEnqueueFromListErrorSkipsPass(t *testing.T) {
	r := &reconciler{
		store:      &errPendingWakeStore{},
		work:       newWorkQueue(),
		backoffFor: make(map[ObjectID]time.Duration),
	}

	r.enqueuePendingWake(context.Background()) // r.logger is nil: must warn, not panic

	r.work.mu.Lock()
	items := append([]ObjectID(nil), r.work.items...)
	r.work.mu.Unlock()
	assert.Empty(t, items, "a failed list enqueues nothing")
}

// TestEnqueuePendingWake verifies that enqueuePendingWake enqueues exactly the IDs
// returned by ListPendingWakeIDs, in order — the sibling of the test above.
// Only its failed-list branch was covered (TestEnqueueFromListErrorSkipsPass), so
// the helper whose whole purpose is not losing an owed wake was the one of the
// three that could have stopped enqueuing anything without a test noticing.
func TestEnqueuePendingWake(t *testing.T) {
	r := &reconciler{
		store:      &pendingWakeIDsStore{ids: []ObjectID{5, 8}},
		work:       newWorkQueue(),
		backoffFor: make(map[ObjectID]time.Duration),
	}

	r.enqueuePendingWake(context.Background())

	r.work.mu.Lock()
	items := append([]ObjectID(nil), r.work.items...)
	r.work.mu.Unlock()
	assert.Equal(t, []ObjectID{5, 8}, items)
}

// TestCatchupTickEnqueuesPendingWake covers run's *tick* call to
// enqueuePendingWake at the unit level, with no store: the restart test that pins
// durable-wake recovery disables every ticker, so deleting the tick's enqueue left
// the suite green. Owed wakes ride the catchup tick, not resync — a wake is
// recorded work, which is what catchup exists to drain.
//
// A disabled startup resync plus a store that withholds its owed IDs until the second
// listing means neither the startup pass nor any other backstop can be what
// enqueues the object — only a tick can.
func TestCatchupTickEnqueuesPendingWake(t *testing.T) {
	const owedID = ObjectID(21)

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
		store:            &tickOnlyPendingWakeStore{ids: []ObjectID{owedID}},
		work:             newWorkQueue(),
		catchupInterval:  time.Millisecond, // the tick is the code under test
		maxRetryInterval: time.Second,
		startupResync:    false,
		backoffFor:       make(map[ObjectID]time.Duration),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runInBackground(r, ctx)

	select {
	case got := <-reconciled:
		assert.Equal(t, owedID, got)
	case <-time.After(testTimeout):
		t.Fatal("owed wake was never enqueued by a catchup tick")
	}

	cancel()
	waitClosed(t, done, "run to return after cancel")
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

// TestReconcilerRequeueBackoffLadder verifies the resetBackoff intent on the
// reconciler's requeue method: requeue(id, false) leaves the ladder so the next
// failure keeps climbing, while requeue(id, true) (the WithResetBackoff path) restarts
// it from the base interval.
func TestReconcilerRequeueBackoffLadder(t *testing.T) {
	r := &reconciler{
		work:              newWorkQueue(),
		backoffFor:        make(map[ObjectID]time.Duration),
		maxRetryInterval:  time.Minute,
		baseRetryInterval: 10 * time.Millisecond,
	}
	// A failing reconcile climbs the ladder twice: 10ms → 20ms.
	assert.Equal(t, 10*time.Millisecond, r.nextBackoff(1))
	assert.Equal(t, 20*time.Millisecond, r.nextBackoff(1))

	// requeue without reset preserves the ladder, so the next failure continues
	// from where it was: 20ms → 40ms, not back to base.
	r.requeue(1, false)
	assert.Equal(t, 40*time.Millisecond, r.nextBackoff(1), "requeue(reset=false) must not reset the ladder")

	// requeue with reset restarts the ladder from base.
	r.requeue(1, true)
	assert.Equal(t, 10*time.Millisecond, r.nextBackoff(1), "requeue(reset=true) must restart the ladder from base")
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

// TestRunCatchesUpOnTick verifies the catchup ticker keeps firing: the unsettled
// listing runs once at startup and again on every tick. This is the loop-level
// pin; which objects each listing returns is covered by the store-backed tests.
func TestRunCatchesUpOnTick(t *testing.T) {
	store := &listCallStore{callCh: make(chan struct{}, 10)}
	r := &reconciler{
		store:           store,
		work:            newWorkQueue(),
		catchupInterval: 5 * time.Millisecond,
		backoffFor:      make(map[ObjectID]time.Duration),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	// Drain the initial startup enqueueUnsettled call.
	select {
	case <-store.callCh:
	case <-time.After(testTimeout):
		t.Fatal("initial enqueueUnsettled not called")
	}

	// Wait for at least one catchup-tick-driven enqueueUnsettled call.
	select {
	case <-store.callCh:
	case <-time.After(testTimeout):
		t.Fatal("catchup tick did not call enqueueUnsettled")
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

// TestTypedControllerReconcileRawToTypedError pins the quarantine: an undecodable
// row (not deletion-pending) is a no-op success, not a retryable error — returning
// the error would retry the identical bytes forever under backoff, and resync
// re-enqueues it regardless. The controller must not run on a row that never
// decoded.
func TestTypedControllerReconcileRawToTypedError(t *testing.T) {
	bh := &Beehive{store: &getObjectBadSpecStore{}}
	var called bool
	inner := &funcController{fn: func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) (Result, error) {
		called = true
		return Result{}, nil
	}}
	tc := &typedController[cSpec, cStatus]{
		gk:    GroupKind{Kind: "Widget"},
		bh:    bh,
		inner: inner,
	}
	res, err := tc.reconcile(context.Background(), 1)
	require.NoError(t, err, "an undecodable row must not retry forever")
	assert.Equal(t, Result{}, res)
	assert.False(t, called, "Reconcile must not run on a row that failed to decode")
}

// owedBadSpecStore is getObjectBadSpecStore with a wake already owed, and records
// whether the reconcile tried to drain it.
type owedBadSpecStore struct {
	fakeStore
	decremented bool
}

func (s *owedBadSpecStore) GetObject(_ context.Context, id ObjectID) (*RawObject, error) {
	return &RawObject{ID: id, Kind: "Widget", Spec: []byte("not-json"), PendingWake: 2}, nil
}

func (s *owedBadSpecStore) DecrementPendingWake(context.Context, ObjectID, int64) error {
	s.decremented = true
	return nil
}

// TestTypedControllerReconcileQuarantineKeepsPendingWake pins that quarantining an
// undecodable row does not drain its owed wake. The pass never reached the
// controller, so the wake is still owed; draining it would silently discard a real
// obligation and leave the dependent stale with nothing recording it. The count is
// meant to outlive the poison and be serviced by the first pass that can decode —
// so a future refactor must not "fix" this by hoisting the decrement above the
// quarantine return.
func TestTypedControllerReconcileQuarantineKeepsPendingWake(t *testing.T) {
	store := &owedBadSpecStore{}
	bh := &Beehive{store: store}
	tc := &typedController[cSpec, cStatus]{
		gk: GroupKind{Kind: "Widget"},
		bh: bh,
		inner: &funcController{fn: func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) (Result, error) {
			return Result{}, nil
		}},
	}

	_, err := tc.reconcile(context.Background(), 1)
	require.NoError(t, err, "an undecodable row is still a no-op success")
	assert.False(t, store.decremented, "a wake the pass could not service must stay owed")
}

// TestTypedControllerReconcileRawToTypedErrorCollectsDeleting pins the GC leg of
// the quarantine: a deletion-pending, finalizer-free row that can't decode is
// still collected here (collect needs only the id), so it doesn't strand holding
// its slug and owned_by edge waiting for a controller that can never decode it.
func TestTypedControllerReconcileRawToTypedErrorCollectsDeleting(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	gk := GroupKind{Kind: "Widget"}

	// Inject an undecodable row directly (a valid create can always decode), then
	// request its deletion so the reconcile sees a deletion-pending poison row.
	raw, err := store.CreateObject(ctx, &RawObject{Group: gk.Group, Kind: gk.Kind, Spec: []byte("not-json")})
	require.NoError(t, err)
	_, _, err = store.RequestDeletion(ctx, gk, raw.ID)
	require.NoError(t, err)

	var called bool
	inner := &funcController{fn: func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) (Result, error) {
		called = true
		return Result{}, nil
	}}
	tc := &typedController[cSpec, cStatus]{gk: gk, bh: bh, inner: inner}

	res, err := tc.reconcile(ctx, raw.ID)
	require.NoError(t, err)
	assert.Equal(t, Result{}, res)
	assert.False(t, called, "Reconcile must not run on a row that failed to decode")

	_, err = store.GetObject(ctx, raw.ID)
	require.ErrorIs(t, err, ErrNotFound, "the finalizer-free deleting poison row must be collected, not stranded")
}

// undecodableDeletingCollectErrorStore returns an undecodable, deletion-pending
// row from GetObject, and errors from GetObjectMeta so that collect (which reads
// meta first) fails. This exercises the GC-error leg of the quarantine: a poison
// deleting row whose collect fails must surface the error for retry, not swallow
// it as a no-op success.
type undecodableDeletingCollectErrorStore struct {
	fakeStore
}

func (s *undecodableDeletingCollectErrorStore) GetObject(_ context.Context, id ObjectID) (*RawObject, error) {
	deletedAt := time.Unix(1, 0)
	return &RawObject{ID: id, Kind: "Widget", Spec: []byte("not-json"), DeletionRequestedAt: &deletedAt}, nil
}

func (s *undecodableDeletingCollectErrorStore) GetObjectMeta(context.Context, ObjectID) (*RawObject, error) {
	return nil, errBoom
}

func TestTypedControllerReconcileRawToTypedErrorCollectError(t *testing.T) {
	bh := &Beehive{store: &undecodableDeletingCollectErrorStore{}}
	var called bool
	inner := &funcController{fn: func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) (Result, error) {
		called = true
		return Result{}, nil
	}}
	tc := &typedController[cSpec, cStatus]{
		gk:    GroupKind{Kind: "Widget"},
		bh:    bh,
		inner: inner,
	}
	_, err := tc.reconcile(context.Background(), 1)
	require.ErrorIs(t, err, errBoom, "a failed collect on a poison deleting row must surface for retry")
	assert.False(t, called, "Reconcile must not run on a row that failed to decode")
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

// reconcilePendingWakeHarness builds a typedController over a real store, driven
// synchronously so the decrement has run by the time reconcile returns. wrap, if
// non-nil, decorates the store the controller writes through (to inject a failing
// mutator); the returned count always reads the real store underneath it.
// reconcilePendingWakeHarness returns the pieces the durable-wake tests need,
// including owe: seeding an owed wake goes through the concrete store, since
// IncrementPendingWake is deliberately absent from the Store interface (AddRef is
// production's only producer). A closure rather than the store itself because the
// concrete type is unexported in package sqlite and so cannot be named here.
func reconcilePendingWakeHarness(t *testing.T, wrap func(Store) Store) (*typedController[cSpec, cStatus], *funcController, ObjectID, func(*testing.T) int64, func() error) {
	t.Helper()
	ctx := context.Background()
	s, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	specJSON, err := json.Marshal(cSpec{})
	require.NoError(t, err)
	raw, err := s.CreateObject(ctx, &RawObject{Kind: clientTestGK.Kind, Spec: specJSON})
	require.NoError(t, err)

	var store Store = s
	if wrap != nil {
		store = wrap(store)
	}
	bh := &Beehive{store: store}
	inner := &funcController{}
	tc := &typedController[cSpec, cStatus]{
		gk:     clientTestGK,
		bh:     bh,
		client: &controllerClientImpl[cStatus]{bh: bh, gk: clientTestGK},
		inner:  inner,
	}
	count := func(t *testing.T) int64 {
		t.Helper()
		got, err := s.GetObject(ctx, raw.ID)
		require.NoError(t, err)
		return got.PendingWake
	}
	owe := func() error { return s.IncrementPendingWake(ctx, raw.ID) }
	return tc, inner, raw.ID, count, owe
}

// TestReconcileDecrementsPendingWake pins the durable-wake decrement: a successful
// pass services one owed wake (count down by one), and a failed pass leaves the
// count owed for the backstop to retry.
func TestReconcileDecrementsPendingWake(t *testing.T) {
	ctx := context.Background()
	tc, inner, id, count, owe := reconcilePendingWakeHarness(t, nil)

	// Success decrements the owed count to zero.
	inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) (Result, error) {
		return Result{}, nil
	}
	require.NoError(t, owe())
	_, err := tc.reconcile(ctx, id)
	require.NoError(t, err)
	assert.Zero(t, count(t), "a successful pass services the owed wake")

	// A failed pass leaves the count owed for the backstop.
	inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) (Result, error) {
		return Result{}, errBoom
	}
	require.NoError(t, owe())
	_, err = tc.reconcile(ctx, id)
	require.ErrorIs(t, err, errBoom)
	assert.Equal(t, int64(1), count(t), "a failed pass leaves the wake owed")
}

// TestReconcileDrainsMultiplePendingWakes pins that one pass services every wake
// it observed, not just one. A crashed process can leave a count above 1; the
// backstop enqueues that row exactly once (the work queue coalesces), so a pass
// that subtracted only 1 would strand the remainder with nothing to re-enqueue it —
// indefinitely when resync is disabled, and one per tick otherwise. Subtracting the
// observed count drains it in the single pass the backstop scheduled.
func TestReconcileDrainsMultiplePendingWakes(t *testing.T) {
	ctx := context.Background()
	tc, inner, id, count, owe := reconcilePendingWakeHarness(t, nil)

	inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) (Result, error) {
		return Result{}, nil
	}
	// Three wakes owed, as a crashed process would have left them.
	for range 3 {
		require.NoError(t, owe())
	}
	require.Equal(t, int64(3), count(t))

	_, err := tc.reconcile(ctx, id)
	require.NoError(t, err)
	assert.Zero(t, count(t), "one recovery pass drains every wake it observed")
}

// TestReconcilePendingWakeSurvivesConcurrentWake pins the condition the reviewer
// surfaced, and the reason pending_wake is a count rather than a single token: a
// second wake owed *while a reconcile is already servicing an earlier one* must not
// be lost. Under the reverted design (the token was the target's resource_version)
// two wakes for the same unchanged target shared a value, so the reconcile's clear
// matched and dropped the second — and a crash before the in-memory requeue then
// lost it entirely. As a +1/-1 count it cannot: the mid-pass increment outlives the
// pass's subtraction (it lands above the count that pass observed), leaving the
// object owed and re-enqueued by the backstop.
func TestReconcilePendingWakeSurvivesConcurrentWake(t *testing.T) {
	ctx := context.Background()
	tc, inner, id, count, owe := reconcilePendingWakeHarness(t, nil)

	// The pass is servicing one owed wake; a second is owed during it.
	inner.fn = func(ctx context.Context, _ ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
		return Result{}, owe()
	}
	require.NoError(t, owe()) // the wake this pass loads
	_, err := tc.reconcile(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count(t),
		"the wake owed during the pass is not clobbered by the pass's decrement")
}

// failDecrementPendingWakeStore fails the durable-wake decrement while delegating
// the rest, so a test can exercise the reconciler's log-and-continue branch.
type failDecrementPendingWakeStore struct {
	Store
}

func (s *failDecrementPendingWakeStore) DecrementPendingWake(context.Context, ObjectID, int64) error {
	return errBoom
}

// TestReconcileDecrementPendingWakeErrorIsNonFatal pins that a failed decrement does
// not fail the reconcile: the count stays up and the backstop re-enqueues (a
// harmless extra pass), so shadowing the successful reconcile with the decrement
// error would be strictly worse.
func TestReconcileDecrementPendingWakeErrorIsNonFatal(t *testing.T) {
	ctx := context.Background()
	tc, inner, id, count, owe := reconcilePendingWakeHarness(t, func(s Store) Store {
		return &failDecrementPendingWakeStore{Store: s}
	})
	require.NoError(t, owe())
	inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) (Result, error) {
		return Result{}, nil
	}

	_, err := tc.reconcile(ctx, id)
	require.NoError(t, err, "a failed decrement must not fail an otherwise successful reconcile")
	assert.Equal(t, int64(1), count(t), "the count stays owed for the backstop to retry")
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

// deletionTrackingFinalizer gates collection of deletionTrackingController's
// object on the controller. Without it, a finalizer-less object's row is
// collectible the instant deletion is requested, so the global GC sweeper's
// startup pass can remove it before the controller's reconcile observes
// DeletionRequestedAt — the controller is then never woken with deleting=true.
// (Observing deletion at all requires a finalizer; this mirrors that contract.)
const deletionTrackingFinalizer = "test.beehive/deletion-tracking"

// deletionTrackingController signals reconciled after the first successful
// reconcile and deleted when the object's DeletionRequestedAt is set, then
// clears its finalizer so the row can finally be collected.
type deletionTrackingController struct {
	reconcileOne sync.Once
	deleteOne    sync.Once
	reconciled   chan struct{}
	deleted      chan struct{}
}

func (c *deletionTrackingController) Reconcile(ctx context.Context, client ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
	if obj.DeletionRequestedAt != nil {
		c.deleteOne.Do(func() { close(c.deleted) })
		// Clear the finalizer so GC can collect the row now that the deletion has
		// been observed (idempotent: re-clearing a gone finalizer is a no-op).
		if err := client.DeleteFinalizer(ctx, obj.ID, deletionTrackingFinalizer); err != nil {
			return Result{}, err
		}
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
	// The finalizer keeps the row alive until the controller observes the deletion;
	// see deletionTrackingFinalizer.
	obj, err := client.Create(ctx, cSpec{Val: "hello"}, WithFinalizers(deletionTrackingFinalizer))
	require.NoError(t, err)

	waitClosed(t, ctrl.reconciled, "first reconcile")

	require.NoError(t, client.Delete(ctx, obj.ID))
	waitClosed(t, ctrl.deleted, "reconcile after deletion requested")
}

// TestIntegrationWatchScheduleClosesOnStop verifies a live WatchSchedule stream is
// torn down when the control plane stops, even though the subscriber's own context
// stays open: run's teardown closes the schedule hub, which ends the receiver and
// closes the channel. Without that close the stream would hang forever on Background.
func TestIntegrationWatchScheduleClosesOnStop(t *testing.T) {
	ctx := context.Background()

	bh, err := New(newClientTestStore(t), WithResyncInterval(0))
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	// Subscribe with a context that never cancels, so only the control plane's
	// teardown can close the stream.
	ch, err := client.WatchSchedule(ctx, obj.ID)
	require.NoError(t, err)
	recv(t, ch) // drain the snapshot: the stream is live before we stop

	require.NoError(t, stop(ctx))
	assertChanClosed(t, ch)
}

// TestIntegrationWatchScheduleClosesOnCtxCancel verifies cancelling the subscriber's
// own context closes the stream independently of the control plane — the other half
// of the lifecycle contract.
func TestIntegrationWatchScheduleClosesOnCtxCancel(t *testing.T) {
	ctx := context.Background()

	bh, err := New(newClientTestStore(t), WithResyncInterval(0))
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	wctx, cancel := context.WithCancel(ctx)
	ch, err := client.WatchSchedule(wctx, obj.ID)
	require.NoError(t, err)
	recv(t, ch) // drain the snapshot: the stream is live before we cancel

	cancel()
	assertChanClosed(t, ch)
}

// TestMergeSchedule pins the schedule hub's coalescing policy: latest value wins
// and the slot is never annihilated — even the zero (unscheduled) Schedule is a
// real gauge value a subscriber must observe, so keep is always true.
func TestMergeSchedule(t *testing.T) {
	prev := Schedule{NextRequeueAt: time.Unix(1, 0)}

	got, keep := mergeSchedule(prev, Schedule{NextRequeueAt: time.Unix(2, 0)})
	assert.True(t, keep)
	assert.Equal(t, time.Unix(2, 0), got.NextRequeueAt)

	// The unscheduled zero is kept, not annihilated.
	got, keep = mergeSchedule(prev, Schedule{})
	assert.True(t, keep)
	assert.True(t, got.NextRequeueAt.IsZero())
}

// TestWatchScheduleSnapshotSendCtxDone covers the snapshot-send arm exiting on
// context cancellation: no one reads the channel, so the goroutine parks on the
// snapshot send and takes ctx.Done. Exit is awaited via afterWatchSchedule rather
// than reading the channel, which would let the send succeed and mask the arm.
func TestWatchScheduleSnapshotSendCtxDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	r := bh.reconcilers[clientTestGK]
	exited := make(chan struct{})
	r.afterWatchSchedule = func() { close(exited) }

	ch, err := client.WatchSchedule(ctx, obj.ID)
	require.NoError(t, err)

	cancel() // goroutine parks on the snapshot send (no reader) → ctx.Done
	<-exited
	_, ok := <-ch
	assert.False(t, ok, "channel must be closed after the goroutine exits")
}

// TestWatchScheduleLiveSendCtxDone covers the live-send arm exiting on context
// cancellation. A reschedule is buffered before the snapshot is drained, so once
// the goroutine advances past the snapshot RecvContext returns that ready value
// (a pending value beats a cancelled ctx) and parks on the live send with no
// reader, taking ctx.Done.
func TestWatchScheduleLiveSendCtxDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	r := bh.reconcilers[clientTestGK]
	drainQueue(r.work)

	exited := make(chan struct{})
	r.afterWatchSchedule = func() { close(exited) }

	ch, err := client.WatchSchedule(ctx, obj.ID)
	require.NoError(t, err)

	// Buffer a live reschedule before draining the snapshot: it is pending by the
	// time the goroutine reaches the live RecvContext, so it is returned rather
	// than the cancelled ctx observed.
	r.work.addAfter(obj.ID, time.Hour)

	recv(t, ch) // drain the snapshot; goroutine advances to the live send

	cancel() // live send parks with no reader → ctx.Done
	<-exited
	_, ok := <-ch
	assert.False(t, ok, "channel must be closed after the goroutine exits")
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

// TestReconcilerRequeueNow verifies requeueNow cancels any pending delayed retry
// timer and makes the id immediately dispatchable, while preserving the backoff
// ladder (clearing is the caller's separate clearBackoff step).
func TestReconcilerRequeueNow(t *testing.T) {
	r := &reconciler{
		work:              newWorkQueue(),
		maxRetryInterval:  time.Second,
		baseRetryInterval: 5 * time.Millisecond,
		backoffFor:        make(map[ObjectID]time.Duration),
	}
	// Simulate a failed reconcile: a backoff entry and a far-future retry timer.
	seeded := r.nextBackoff(1)
	r.work.addAfter(1, time.Hour)
	require.NotZero(t, r.backoffFor[1], "precondition: backoff seeded")
	require.NotNil(t, r.work.alarms[1], "precondition: retry timer scheduled")

	r.requeueNow(1)

	assert.Equal(t, seeded, r.backoffFor[1], "requeueNow must preserve the backoff entry")
	assert.Nil(t, r.work.alarms[1], "requeueNow must cancel the stale retry timer")

	id, ok := r.work.get()
	require.True(t, ok, "requeueNow must make the id dispatchable now")
	assert.Equal(t, ObjectID(1), id)
}

// TestReconcilerNextRequeueAt verifies nextRequeueAt reports a pending delayed
// add's fire time and reports nothing for an id with no schedule.
func TestReconcilerNextRequeueAt(t *testing.T) {
	r := &reconciler{work: newWorkQueue()}
	r.work.addAfter(1, time.Hour)

	at, ok := r.nextRequeueAt(1)
	require.True(t, ok)
	assert.True(t, at.After(time.Now().Add(time.Minute)), "fire time must be ~1h out, got %s", at)

	_, ok = r.nextRequeueAt(2)
	assert.False(t, ok, "an id with no schedule must report nothing")
}

// TestReconcilerNextRequeueAtNilWork verifies the scheduling methods are safe on
// a reconciler with no work queue (built outside Register, e.g. in tests).
func TestReconcilerNextRequeueAtNilWork(t *testing.T) {
	r := &reconciler{backoffFor: make(map[ObjectID]time.Duration)}
	_, ok := r.nextRequeueAt(1)
	assert.False(t, ok, "nil work queue must report nothing scheduled")
	assert.NotPanics(t, func() { r.requeueNow(1) }, "requeueNow must be nil-work safe")
}

// catchupProbeStore signals the startup listings a catchup test must order itself
// against. Start only *launches* the reconcile loop, and the startup pass drains
// the same owed sets the tick does — so a test that seeds owed work before Start
// (or right after it) is measuring the startup pass, not the tick. Waiting for
// these signals and only then seeding leaves the tick as the sole possible cause.
type catchupProbeStore struct {
	Store
	unsettledListed chan struct{}
	wakeListed      chan struct{}
}

func (s *catchupProbeStore) ListUnsettledIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error) {
	ids, err := s.Store.ListUnsettledIDs(ctx, gk)
	select {
	case s.unsettledListed <- struct{}{}:
	default:
	}
	return ids, err
}

func (s *catchupProbeStore) ListPendingWakeIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error) {
	ids, err := s.Store.ListPendingWakeIDs(ctx, gk)
	select {
	case s.wakeListed <- struct{}{}:
	default:
	}
	return ids, err
}

// wakeStampingStore is the store surface a catchup test needs: the Store contract
// plus IncrementPendingWake, which is deliberately not on Store (see the comment
// on reconcilePendingWakeHarness) but exists on the concrete sqlite store so a
// test can seed an owed wake without staging the whole declare race.
type wakeStampingStore interface {
	Store
	IncrementPendingWake(context.Context, ObjectID) error
}

// newCatchupHarness starts a control plane whose only periodic driver is the
// catchup tick — resync and GC off, no startup spec pass — and returns once the
// startup pass has provably drained both owed sets. Whatever the caller seeds
// after this can only be dispatched by a tick.
func newCatchupHarness(t *testing.T, gk GroupKind, seed func(wakeStampingStore)) (wakeStampingStore, <-chan ObjectID) {
	t.Helper()
	real, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { real.Close() })
	// Seeding before Start lets a test establish a *settled* object without racing
	// the tick, which would otherwise dispatch it for being briefly unsettled.
	if seed != nil {
		seed(real)
	}

	store := &catchupProbeStore{
		Store:           real,
		unsettledListed: make(chan struct{}, 8),
		wakeListed:      make(chan struct{}, 8),
	}
	reconciled := make(chan ObjectID, 4)

	bh, err := New(store, WithResyncInterval(0), WithGCInterval(0),
		WithCatchupInterval(10*time.Millisecond))
	require.NoError(t, err)
	_, err = Register(bh, gk, &recordingController{reconciled: reconciled},
		WithStartupResync(false))
	require.NoError(t, err)

	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { stop(context.Background()) })

	// The startup pass runs enqueuePendingWake unconditionally; the unsettled
	// listing only arrives via a tick with the startup resync off, so waiting on
	// the wake signal is what proves startup is behind us.
	select {
	case <-store.wakeListed:
	case <-time.After(testTimeout):
		t.Fatal("startup pass never listed pending wakes")
	}
	return real, reconciled
}

// TestCatchupTickDispatchesOwedWork pins the catchup ticker: the cheap, frequent
// pass that drains work the store has *recorded* as owed — an unconverged spec
// here — on a cadence of its own.
//
// It is deliberately separate from the resync knob. Draining owed work is bounded
// by what is actually outstanding (indexed listings that return nothing in a
// converged system), while re-confirming every object scales with the object
// count. One interval governing both means tuning either moves the other.
func TestCatchupTickDispatchesOwedWork(t *testing.T) {
	ctx := context.Background()
	gk := GroupKind{Kind: "Widget"}
	real, reconciled := newCatchupHarness(t, gk, nil)

	// An object a prior process left unconverged: written straight through the
	// store, so observed_generation is NULL and nothing has dispatched it.
	raw, err := real.CreateObject(ctx, &RawObject{Group: gk.Group, Kind: gk.Kind, Spec: []byte(`{}`)})
	require.NoError(t, err)

	select {
	case got := <-reconciled:
		assert.Equal(t, raw.ID, got)
	case <-time.After(testTimeout):
		t.Fatal("unsettled object was never dispatched: no catchup tick is draining owed work")
	}
}

// TestCatchupTickDispatchesOwedWake pins the *other* half of the catchup set.
// An object owed a durable dependency wake is settled by definition — that is
// precisely why the unsettled listing cannot see it — so if catchup drained only
// unsettled objects, a wake recorded across a restart would never be delivered.
// The two listings read different columns and need separate coverage.
func TestCatchupTickDispatchesOwedWake(t *testing.T) {
	ctx := context.Background()
	gk := GroupKind{Kind: "Widget"}

	// Seeded before Start and left *settled*, so the unsettled listing can never
	// be what dispatches it.
	var id ObjectID
	real, reconciled := newCatchupHarness(t, gk, func(s wakeStampingStore) {
		raw, err := s.CreateObject(ctx, &RawObject{Group: gk.Group, Kind: gk.Kind, Spec: []byte(`{}`)})
		require.NoError(t, err)
		_, err = s.UpdateStatus(ctx, gk, raw.ID, raw.Generation, []byte(`{}`), 0)
		require.NoError(t, err)
		id = raw.ID
	})

	// Now owed a wake, the way a crash between a target's commit and the
	// dependent's dispatch leaves it.
	require.NoError(t, real.IncrementPendingWake(ctx, id))

	select {
	case got := <-reconciled:
		assert.Equal(t, id, got)
	case <-time.After(testTimeout):
		t.Fatal("object owed a wake was never dispatched: catchup drains only the unsettled half")
	}
}

// newSettledHarness starts a control plane over a real store holding one settled
// object, with the catchup tick and GC off and no startup spec pass. A settled
// object is invisible to every owed-work listing, so nothing but a full pass can
// re-dispatch it — which is exactly what makes it the probe for resync. tune
// configures the reconciler options (i.e. whether resync is on).
func newSettledHarness(t *testing.T, opts ...Option) (ObjectID, <-chan ObjectID) {
	t.Helper()
	ctx := context.Background()
	gk := GroupKind{Kind: "Widget"}

	store, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	raw, err := store.CreateObject(ctx, &RawObject{Group: gk.Group, Kind: gk.Kind, Spec: []byte(`{}`)})
	require.NoError(t, err)
	// Settled before Start: observed_generation == generation, so it is owed
	// nothing and no catchup listing will ever return it.
	_, err = store.UpdateStatus(ctx, gk, raw.ID, raw.Generation, []byte(`{}`), 0)
	require.NoError(t, err)

	reconciled := make(chan ObjectID, 4)
	bh, err := New(store, WithCatchupInterval(0), WithGCInterval(0))
	require.NoError(t, err)
	opts = append(opts, WithStartupResync(false))
	_, err = Register(bh, gk, &recordingController{reconciled: reconciled}, opts...)
	require.NoError(t, err)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { stop(ctx) })
	return raw.ID, reconciled
}

// TestResyncTickReconcilesSettled pins what WithResyncInterval now buys: a pass
// over *every* object, converged ones included. That is the only thing that
// re-confirms process-scoped state a restart invalidated (liveness conditions read
// as "verifying" until this process rewrites them) and the only thing that heals a
// wake lost for a reason nothing recorded — neither is visible to any owed-work
// listing, so no catchup tick can reach it.
func TestResyncTickReconcilesSettled(t *testing.T) {
	id, reconciled := newSettledHarness(t, WithResyncInterval(10*time.Millisecond))

	select {
	case got := <-reconciled:
		assert.Equal(t, id, got)
	case <-time.After(testTimeout):
		t.Fatal("settled object was never re-dispatched: the resync tick is not a full pass")
	}
}

// TestDefaultConfigDoesNotFullPass is the other half of the contract: with no
// resync asked for, nothing re-dispatches a settled object. It guards the *shape*
// — that no other driver quietly grew into a full pass — not the default's value,
// which it cannot see: any default longer than the grace window below looks
// identical from here. TestNewAppliesDefaults pins the value itself.
func TestDefaultConfigDoesNotFullPass(t *testing.T) {
	_, reconciled := newSettledHarness(t)

	select {
	case got := <-reconciled:
		t.Fatalf("settled object %d was re-dispatched: the full pass is not opt-in", got)
	case <-time.After(200 * time.Millisecond):
	}
}

// newStartupHarness starts a control plane with every periodic driver off, so the
// startup pass is the only thing that can dispatch anything. seed runs before
// Start. It returns the ids the controller reconciled, collected until the
// channel is quiet.
func newStartupHarness(t *testing.T, seed func(Store, GroupKind), opts ...Option) []ObjectID {
	t.Helper()
	ctx := context.Background()
	gk := GroupKind{Kind: "Widget"}

	store, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	seed(store, gk)

	reconciled := make(chan ObjectID, 8)
	bh, err := New(store, WithCatchupInterval(0), WithResyncInterval(0), WithGCInterval(0))
	require.NoError(t, err)
	_, err = Register(bh, gk, &recordingController{reconciled: reconciled}, opts...)
	require.NoError(t, err)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { stop(ctx) })

	var got []ObjectID
	for {
		select {
		case id := <-reconciled:
			got = append(got, id)
		case <-time.After(200 * time.Millisecond):
			return got
		}
	}
}

// TestStartupAlwaysDrainsOwedWork pins that startup resumes work the store has
// recorded as owed regardless of the resync choice. Declining it is not a
// cheapness knob — an object a previous process left unconverged, or one owed a
// durable wake, is *already* owed a pass, and with every ticker off nothing else
// will ever run it. The knob governs only the full re-confirm pass below.
func TestStartupAlwaysDrainsOwedWork(t *testing.T) {
	ctx := context.Background()
	var unsettled ObjectID
	got := newStartupHarness(t, func(s Store, gk GroupKind) {
		// Unconverged: observed_generation NULL, as a crash mid-reconcile leaves it.
		raw, err := s.CreateObject(ctx, &RawObject{Group: gk.Group, Kind: gk.Kind, Spec: []byte(`{}`)})
		require.NoError(t, err)
		unsettled = raw.ID
	}, WithStartupResync(false))

	assert.Equal(t, []ObjectID{unsettled}, got,
		"owed work must be resumed even when the full startup pass is declined")
}

// TestStartupResyncReconcilesSettled is the other half: the knob's actual job is
// the *settled* objects, which no owed-work listing can see. That pass is what
// re-confirms process-scoped state a restart invalidated.
func TestStartupResyncReconcilesSettled(t *testing.T) {
	ctx := context.Background()
	var settled ObjectID
	seed := func(s Store, gk GroupKind) {
		raw, err := s.CreateObject(ctx, &RawObject{Group: gk.Group, Kind: gk.Kind, Spec: []byte(`{}`)})
		require.NoError(t, err)
		_, err = s.UpdateStatus(ctx, gk, raw.ID, raw.Generation, []byte(`{}`), 0)
		require.NoError(t, err)
		settled = raw.ID
	}

	t.Run("enabled reconciles it", func(t *testing.T) {
		got := newStartupHarness(t, seed, WithStartupResync(true))
		assert.Equal(t, []ObjectID{settled}, got)
	})

	t.Run("disabled leaves it alone", func(t *testing.T) {
		got := newStartupHarness(t, seed, WithStartupResync(false))
		assert.Empty(t, got, "a settled object is owed nothing")
	})

	t.Run("defaults to enabled", func(t *testing.T) {
		got := newStartupHarness(t, seed)
		assert.Equal(t, []ObjectID{settled}, got, "the safe default holds without the option")
	})
}

// TestDisabledBackstopsAnnounceThemselves pins that turning a periodic driver off
// is visible in the log. Each of these is a supported configuration, so none of
// them is an error — but the failure mode when one is reached by accident (an
// unset config field, a bad duration parse) is silence: work quietly stops being
// re-derived and nothing says so. The level differs by what recourse the operator
// has left.
func TestDisabledBackstopsAnnounceThemselves(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}

	start := func(t *testing.T, level slog.Level, opts ...Option) string {
		t.Helper()
		logger, buf := captureLogger(level)
		opts = append(opts, WithLogger(logger))
		bh, err := New(&fakeStore{}, opts...)
		require.NoError(t, err)
		_, err = Register(bh, gk, &noopController[tSpec, tStatus]{})
		require.NoError(t, err)
		stop, err := bh.Start(context.Background())
		require.NoError(t, err)
		require.NoError(t, stop(context.Background()))
		return buf.String()
	}

	t.Run("catchup off is Info: the caller can still requeue", func(t *testing.T) {
		out := start(t, slog.LevelInfo, WithCatchupInterval(0))
		assert.Contains(t, out, "catchup disabled")
		assert.Contains(t, out, "Requeue", "name the primitive that replaces it")
	})

	t.Run("gc off is Warn: nothing public collects a row", func(t *testing.T) {
		// No public API triggers collect, so a disabled sweeper leaves the operator
		// no way to make deletion progress — a strictly worse position than the
		// other two, and the reason this one is louder.
		out := start(t, slog.LevelWarn, WithGCInterval(0))
		assert.Contains(t, out, "garbage collection disabled")
	})

	t.Run("the defaults say nothing", func(t *testing.T) {
		out := start(t, slog.LevelInfo)
		assert.NotContains(t, out, "disabled",
			"a default configuration must not narrate; resync-off is the default and would be noise")
	})
}

// TestWakeDependentsListErrorLogs pins the first of the dependency waker's silent
// loss points. When the dependents lookup fails, every dependent of that target
// misses that change — and a dependent that has settled is invisible to every
// owed-work listing, so with the full resync off by default the miss is permanent
// rather than slow. Swallowing it silently is what made this a stuck-dependent bug
// instead of a hiccup.
func TestWakeDependentsListErrorLogs(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	bh := &Beehive{store: &errDepsStore{}, logger: logger}

	bh.wakeDependents(context.Background(), 1)

	assert.Contains(t, buf.String(), "dependents lookup failed",
		"a dropped wake must not be silent")
}

// TestDependencyWakerStreamEndLogs pins the second. A closed change stream ends
// the waker for the life of the process — nothing re-subscribes — so every future
// change on that kind reaches no dependent at all. No reachable path closes the
// channel short of store Close today, which makes this latent rather than live;
// unlogged either way.
func TestDependencyWakerStreamEndLogs(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	fw := newFakeWatcher()
	bh := &Beehive{store: &watcherStore{w: fw}, logger: logger}

	done := make(chan struct{})
	go func() {
		bh.runDependencyWaker(context.Background(), GroupKind{Kind: "Widget"}, fw)
		close(done)
	}()

	fw.endStream()
	waitClosed(t, done, "waker to exit on stream end")

	assert.Contains(t, buf.String(), "dependency waker stopped",
		"a dead waker must not be silent")
}

// TestDependencyWakerCancelDoesNotLog is the negative: an ordinary shutdown ends
// every waker, and that is not a loss. Warning on it would put a line per kind in
// every clean stop, training operators to ignore the one message that matters.
func TestDependencyWakerCancelDoesNotLog(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	fw := newFakeWatcher()
	bh := &Beehive{store: &watcherStore{w: fw}, logger: logger}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		bh.runDependencyWaker(ctx, GroupKind{Kind: "Widget"}, fw)
		close(done)
	}()

	cancel()
	waitClosed(t, done, "waker to exit on cancel")

	assert.Empty(t, buf.String(), "a clean shutdown is not a dropped wake")
}

// TestCatchupTickEscalationPredicate covers the tick's full-pass decision
// directly, because the two escalation reasons differ in a way an integration
// test cannot show cheaply: a dead waker is a standing condition, while a dropped
// wake is a one-shot that must be *consumed* by the tick it drives.
//
// It also pins the consumption order. When a standing reason already applies, the
// one-shot must survive rather than be burned — that tick sweeps anyway, and
// spending it there would discard the repair still owed if the standing reason
// later goes away.
func TestCatchupTickEscalationPredicate(t *testing.T) {
	t.Run("no escalation by default", func(t *testing.T) {
		assert.False(t, (&reconciler{}).tickResyncs())
	})

	t.Run("resyncNextTick fires once", func(t *testing.T) {
		r := &reconciler{}
		r.resyncNextTick()
		assert.True(t, r.tickResyncs())
		assert.False(t, r.tickResyncs(), "one-shot: a transient drop must not degrade the process")
	})

	t.Run("resyncEveryTick fires forever", func(t *testing.T) {
		r := &reconciler{}
		r.resyncEveryTick()
		assert.True(t, r.tickResyncs())
		assert.True(t, r.tickResyncs(), "sticky: the waker is gone for the process lifetime")
	})

	t.Run("a standing reason leaves the one-shot armed", func(t *testing.T) {
		r := &reconciler{}
		r.resyncEveryTick()
		r.resyncNextTick()
		assert.True(t, r.tickResyncs())
		assert.True(t, r.resyncOnce.Load(), "not consumed by a tick it did not decide")
	})
}

// TestDroppedWakeEscalatesEveryKind pins the property the first attempt at this
// got wrong. The flags were process-wide but consumed per-reconciler, so a dropped
// wake repaired whichever kind ticked first and silently spent the repair for the
// rest — a guarantee stated in a comment and contradicted by the code.
//
// Cross-kind is forced by the mechanism, not chosen: dependency edges are
// deliberately cross-kind, so a lost wake on one kind strands dependents of any
// kind, and the lookup that failed is what would have named them.
func TestDroppedWakeEscalatesEveryKind(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	r1, r2 := &reconciler{}, &reconciler{}
	bh := &Beehive{store: &errDepsStore{}, logger: logger, order: []*reconciler{r1, r2}}

	bh.wakeDependents(context.Background(), 1)

	assert.Contains(t, buf.String(), "forcing a full resync pass")
	assert.True(t, r1.resyncOnce.Load(), "every kind is armed, not just whichever ticks first")
	assert.True(t, r2.resyncOnce.Load())
	assert.False(t, r1.resyncAlways.Load(), "a transient lookup error is not a permanent escalation")
}

// TestDeadWakerEscalatesEveryKind is the sticky counterpart: a waker whose stream
// ended keeps dropping every future change, so one pass would repair the instant
// of death and strand everything after it.
func TestDeadWakerEscalatesEveryKind(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	fw := newFakeWatcher()
	r1, r2 := &reconciler{}, &reconciler{}
	bh := &Beehive{store: &watcherStore{w: fw}, logger: logger, order: []*reconciler{r1, r2}}

	done := make(chan struct{})
	go func() {
		bh.runDependencyWaker(context.Background(), GroupKind{Kind: "Widget"}, fw)
		close(done)
	}()
	fw.endStream()
	waitClosed(t, done, "waker to exit on stream end")

	assert.Contains(t, buf.String(), "escalating")
	assert.True(t, r1.resyncAlways.Load(), "a dead waker escalates every kind's later ticks")
	assert.True(t, r2.resyncAlways.Load())
}

// TestEscalatedCatchupTickReconcilesSettled closes the loop end to end: an armed
// escalation must actually reach a settled object, which is the only kind of
// object the repair exists for — one that is owed nothing and therefore invisible
// to the catchup listings the tick would otherwise run.
func TestEscalatedCatchupTickReconcilesSettled(t *testing.T) {
	ctx := context.Background()
	gk := GroupKind{Kind: "Widget"}

	store, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	raw, err := store.CreateObject(ctx, &RawObject{Group: gk.Group, Kind: gk.Kind, Spec: []byte(`{}`)})
	require.NoError(t, err)
	_, err = store.UpdateStatus(ctx, gk, raw.ID, raw.Generation, []byte(`{}`), 0)
	require.NoError(t, err)

	reconciled := make(chan ObjectID, 4)
	// Catchup on (it carries the escalation), resync off, no startup pass: only an
	// escalated catchup tick can reach a settled object.
	bh, err := New(store, WithCatchupInterval(10*time.Millisecond),
		WithResyncInterval(0), WithGCInterval(0))
	require.NoError(t, err)
	_, err = Register(bh, gk, &recordingController{reconciled: reconciled},
		WithStartupResync(false))
	require.NoError(t, err)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	select {
	case got := <-reconciled:
		t.Fatalf("settled object %d dispatched before any escalation", got)
	case <-time.After(100 * time.Millisecond):
	}

	bh.resyncKindsNextTick()

	select {
	case got := <-reconciled:
		assert.Equal(t, raw.ID, got)
	case <-time.After(testTimeout):
		t.Fatal("escalation never reached the settled object")
	}
}
