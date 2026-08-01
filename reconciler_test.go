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

// unsettledIDsStore is a fakeStore whose ObjectsListUnsettledIDs returns a fixed slice
// of IDs, used to exercise enqueueUnsettled without a real SQLite database.
type unsettledIDsStore struct {
	fakeStore
	ids []ObjectID
}

func (s *unsettledIDsStore) ObjectsListUnsettledIDs(_ context.Context, _ GroupKind) ([]ObjectID, error) {
	return s.ids, nil
}

// reconcileOwedIDsStore is a fakeStore whose ReconcileOwedListIDs returns a fixed
// slice, used to exercise the durable-wake backstop enqueue without a real
// database — the sibling of unsettledIDsStore.
type reconcileOwedIDsStore struct {
	fakeStore
	ids []ObjectID
}

func (s *reconcileOwedIDsStore) ReconcileOwedListIDs(context.Context, GroupKind) ([]ObjectID, error) {
	return s.ids, nil
}

// tickOnlyReconcileOwedStore reports its owed wakes from the second call onward, so
// the startup enqueue sees an empty set and only a full-pass tick can supply the IDs.
// That is what makes the tick observable: the two calls are otherwise identical,
// and a test that let the startup pass answer would pass with the tick's enqueue
// deleted.
type tickOnlyReconcileOwedStore struct {
	fakeStore
	ids []ObjectID

	mu    sync.Mutex
	calls int
}

func (s *tickOnlyReconcileOwedStore) ReconcileOwedListIDs(context.Context, GroupKind) ([]ObjectID, error) {
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

func (s *allIDsStore) ObjectsListUnsettledIDs(_ context.Context, _ GroupKind) ([]ObjectID, error) {
	return nil, nil
}
func (s *allIDsStore) ObjectsListIDs(_ context.Context, _ GroupKind) ([]ObjectID, error) {
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

func TestRunExitsOnCancelWithFullPassDisabled(t *testing.T) {
	// fullPassInterval <= 0 means no ticker is created (NewTicker would panic).
	r := &reconciler{fullPassInterval: 0}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	cancel()
	waitClosed(t, done, "run to return after cancel")
}

func TestRunExitsOnCancelWithFullPassEnabled(t *testing.T) {
	// A long interval that won't fire during the test: the exit is driven by the
	// cancel, not by the ticker, so timing is irrelevant to the assertion.
	r := &reconciler{fullPassInterval: time.Hour}
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
		fullPassInterval:  0,
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
		fullPassInterval:  0,
		maxRetryInterval:  time.Second,
		baseRetryInterval: 5 * time.Millisecond,
		backoffFor:        make(map[ObjectID]time.Duration),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	r.enqueue(1)
	waitClosed(t, succeeded, "retry reconcile to succeed")
	cancel()
	waitClosed(t, done, "run to exit") // worker's backoffClear has run by now

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
		fullPassInterval: 0,
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

	bh, err := New(store, fast()...)
	require.NoError(t, err)

	gk := GroupKind{Kind: "Widget"}
	reconciled := make(chan *Object[tSpec, tStatus], 16)
	// Full pass disabled so the dependency waker is the only thing that can requeue
	// an already-settled object — no timer noise.
	_, err = Register(bh, gk, &reconcileCapture{ch: reconciled}, WithFullPassInterval(0))
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[tSpec, tStatus](bh, gk)
	target := mustCreate(t, ctx, client, uniqueName(), tSpec{})
	dep := mustCreate(t, ctx, client, uniqueName(), tSpec{})

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

	require.NoError(t, addEdge(ctx, store, dep.ID, target.ID, "depends_on"))

	// An observable change to the target must wake the dependent.
	err = store.ConditionsSet(ctx, GroupKind{Group: target.Group, Kind: target.Kind}, target.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	// Wait for the dependent specifically rather than for "the next reconcile":
	// reconcileCapture never settles its objects, so the owed-pass tick keeps
	// re-dispatching both, and which one arrives next says nothing. What the waker
	// owes is that the dependent arrives at all — nothing else can produce it, since
	// the dependent's own spec has not moved since its creation pass.
	for {
		select {
		case obj := <-reconciled:
			if obj.ID == dep.ID {
				return
			}
		case <-time.After(testTimeout):
			t.Fatal("dependent was not requeued after the target changed")
		}
	}
}

// dependentController is the dependent in the read-then-declare repros. Every
// pass reads the target, reports the target's Ready state as that pass saw it,
// and settles at obj.Generation — the settle being what hides a missed wake from
// the owed-pass backstop, since ObjectsListUnsettledIDs then sees a converged object.
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
	target, err := c.client.GetByID(ctx, c.targetID)
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
	// Settling at obj.Generation is what hides a missed wake from the full pass
	// backstop: ObjectsListUnsettledIDs sees a converged object.
	if err := cc.UpdateStatus(ctx, c.depID, obj.Generation, tStatus{}); err != nil {
		return Result{}, err
	}
	c.observed <- ready
	return Result{}, nil
}

// TestDependencyRequeueRaceOnDeclare pins the read-then-declare race: a change to
// the target that lands after the dependent read it but before DependenciesAdd
// commits reaches nobody — the waker resolves dependents at the instant of
// the change, and the edge did not exist yet. The dependent is left holding a
// stale read with no error, no condition, and (because it settled at its own
// generation) nothing for the owed-pass backstop to notice.
func TestDependencyRequeueRaceOnDeclare(t *testing.T) {
	ctx := context.Background()
	db, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	store := &wakeProbeStore{Store: db, looked: make(chan struct{}, 8)}

	bh, err := New(store, fast()...)
	require.NoError(t, err)

	gk := GroupKind{Kind: "Widget"}
	ctrl := &dependentController{observed: make(chan bool, 8)}
	readDone := newSignal()        // fires once the first pass has read the target
	proceed := make(chan struct{}) // closed by the test after it changes the target
	ctrl.afterRead = func(ctx context.Context, cc ControllerClient[tStatus], target *Object[tSpec, tStatus]) error {
		// First pass only: park between the read and the declaration so the test can
		// land its change to the target inside the window. Later passes declare
		// straight through, as a level-triggered controller re-asserting its edges.
		if readDone.fire() {
			// ctx.Done is the abort half, not decoration: the test closes proceed only
			// on the path where it got what it was waiting for, so a failed wait would
			// otherwise park this worker forever and the deferred stop — which waits on
			// an unbounded context — would hang the binary until the panic timeout,
			// burying the failure it was meant to report.
			select {
			case <-proceed:
			case <-ctx.Done():
			}
		}
		// The version the read above reflects — not a fresh one, which would claim
		// to have seen changes this pass did not.
		return cc.DependenciesAdd(ctx, ctrl.depID, ctrl.targetID)
	}
	// Full pass disabled so the dependency waker is the only thing that can requeue
	// the dependent — the backstop must not paper over the miss.
	_, err = Register(bh, gk, ctrl, WithFullPassInterval(0))
	require.NoError(t, err)

	client := NewClient[tSpec, tStatus](bh, gk)
	ctrl.client = client

	// Create before Start so the ids are set before any reconcile can dispatch;
	// the startup pass then drives both objects.
	target := mustCreate(t, ctx, client, uniqueName(), tSpec{})
	dep := mustCreate(t, ctx, client, uniqueName(), tSpec{})
	ctrl.targetID, ctrl.depID = target.ID, dep.ID
	store.targetID = target.ID

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	// The dependent has read the target and not yet declared the edge.
	readDone.wait(t, "the dependent's first reconcile to read the target")

	// Change the target inside the window and wait for the waker to resolve its
	// dependents — with no edge yet, that lookup comes back empty and the change
	// is now permanently unclaimed. Only then let the declaration commit.
	store.resetLooked()
	err = store.ConditionsSet(ctx, gk, target.ID, storeapi.Condition{Type: "Ready", Status: "True"})
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
		t.Fatal("dependent was never requeued: the target's change landed between its read and DependenciesAdd")
	}
}

// TestDependencyRequeueRaceOnOutOfBandDeclare is the out-of-band mirror of
// TestDependencyRequeueRaceOnDeclare: the same read-then-declare window, but with
// the two halves in different goroutines. The embedding application declares the
// edge through the ControllerClient Register handed it, after its own read of the
// target — so no reconcile is in flight to carry the miss, and the hole is a
// notch wider than the in-band one. In-band, the pass that loses the change at
// least runs to completion around the declaration; here the declaration is the
// only thing that happens, and DependenciesAdd enqueues nothing: the edge appears
// with fromID already settled, so a change that landed before the commit reaches
// nobody and nothing re-derives it. With the full pass disabled the dependent holds a
// stale read forever, with no error, no condition and no log line.
func TestDependencyRequeueRaceOnOutOfBandDeclare(t *testing.T) {
	ctx := context.Background()
	db, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	store := &wakeProbeStore{Store: db, looked: make(chan struct{}, 8)}

	// The stale-dependents pass cannot be disabled, and it would re-derive this
	// dependent's staleness within a tick or two — closing the gap for a reason other
	// than the one under test. Pushing it past the test's own timeout leaves the
	// EdgesAdd stamp as the only thing that can requeue the dependent, which is what
	// this is about.
	bh, err := New(store, fast(withStaleDependentsInterval(time.Hour))...)
	require.NoError(t, err)

	gk := GroupKind{Kind: "Widget"}
	ctrl := &dependentController{observed: make(chan bool, 8)}
	// Full pass disabled so the dependency waker is the only thing that can requeue
	// the dependent — the backstop must not paper over the miss.
	cc, err := Register(bh, gk, ctrl, WithFullPassInterval(0))
	require.NoError(t, err)

	client := NewClient[tSpec, tStatus](bh, gk)
	ctrl.client = client

	// Create before Start: the waker's watch is events-only, so pre-Start creates
	// emit nothing into it and the only lookup the probe can see is the one the
	// test triggers below.
	target := mustCreate(t, ctx, client, uniqueName(), tSpec{})
	dep := mustCreate(t, ctx, client, uniqueName(), tSpec{})
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
	// change is already unclaimed by the time DependenciesAdd commits.
	store.resetLooked()
	err = store.ConditionsSet(ctx, gk, target.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	store.waitLooked(t)
	// target is the application's read of the target, taken before the change
	// above — so the version it carries is the one the decision to depend was
	// based on, and the target has since moved past it.
	require.NoError(t, cc.DependenciesAdd(ctx, dep.ID, target.ID))

	// The edge is in place and the target's change is still unobserved, so the
	// dependent must be reconciled again and see Ready.
	// Earlier passes may have queued observations: the owed pass lists unsettled
	// objects every tick, so a dependent that has not settled yet is legitimately
	// reconciled more than once around startup, and one of those can still be in
	// flight here. Those passes all read the target as it was before the change, so
	// they are skipped rather than asserted on — with the backstop pushed out of
	// reach above, the only pass that can report ready is one the stamp requeued.
	deadline := time.After(testTimeout)
	for observedReady := false; !observedReady; {
		select {
		case ready := <-ctrl.observed:
			observedReady = ready
		case <-deadline:
			t.Fatal("no pass observed the target's change")
		}
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
// The restart runs with WithStartupFullPass(false), which is load-bearing: under the
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
	// Full pass disabled here and on the restart: the wake must be what requeues the
	// dependent, not a timer that happens to sweep it up.
	cc, err := Register(bh1, gk, ctrl1, WithFullPassInterval(0))
	require.NoError(t, err)

	client1 := NewClient[tSpec, tStatus](bh1, gk)
	ctrl1.client = client1

	target := mustCreate(t, ctx, client1, uniqueName(), tSpec{})
	dep := mustCreate(t, ctx, client1, uniqueName(), tSpec{})
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
	// loops), so the declaration commits normally with no running queue to reach.
	err = db.ConditionsSet(ctx, gk, target.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	require.NoError(t, cc.DependenciesAdd(ctx, dep.ID, target.ID))

	// --- second process over the same store ---
	bh2, err := New(db)
	require.NoError(t, err)
	ctrl2 := &dependentController{
		observed: make(chan bool, 8),
		depID:    dep.ID,
		targetID: target.ID,
	}
	_, err = Register(bh2, gk, ctrl2,
		WithFullPassInterval(0),
		WithStartupFullPass(false))
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

// TestSelfDependentObjectWakesOnSpecChange is the guard's safety argument, run
// rather than asserted: a self-dependency can only mean "requeue me when I
// change", and skipping the self-wake is safe because a spec write already
// already leaves the object unsettled, and the owed pass drains that
// independently of any edge.
//
// It cannot detect a guard mis-implemented as a self-edge filter in
// EdgesListIncoming — that path never consults edges, so this test stays green
// while the read API silently loses the edge. TestClientListDependentsIncludesSelfEdge
// is what catches that; this one only pins the wake.
func TestSelfDependentObjectWakesOnSpecChange(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store, fast()...)
	require.NoError(t, err)

	gk := GroupKind{Kind: "Widget"}
	reconciled := make(chan ObjectID, 8)
	// Full pass off: an arriving pass must be the write's own wake, not a tick.
	_, err = Register(bh, gk, &idCapture{ch: reconciled},
		WithFullPassInterval(0), WithConcurrency(1))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, gk)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	require.NoError(t, addEdge(ctx, store, obj.ID, obj.ID, RelationDependsOn))

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { stop(ctx) })
	require.Equal(t, obj.ID, recv(t, reconciled), "startup pass")

	// A changed spec: the write must not be suppressed as an identical-byte no-op,
	// or nothing would wake it and the test would pass for the wrong reason.
	_, err = client.UpdateByID(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)

	assert.Equal(t, obj.ID, recv(t, reconciled), "a spec write wakes it without the self-edge")
}

// idCapture reports the id of each object it reconciles. It is cSpec-typed
// because tSpec is empty, which would make every Update a byte-identical no-op.
type idCapture struct{ ch chan ObjectID }

func (c *idCapture) Reconcile(_ context.Context, _ ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
	c.ch <- obj.ID
	return Result{}, nil
}

// TestStartupEnqueuesAllNotJustUnsettled verifies that run's startup enqueue
// reconciles every object, not only unsettled ones. A settled object (empty
// ObjectsListUnsettledIDs) must still be reconciled at startup so a controller can
// re-confirm process-scoped state like liveness conditions. With the full pass
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
		// Set explicitly: a bool's zero value is the *off* state, so a reconciler
		// built outside Register (as here) does not inherit New's true default.
		startupFullPass:  true,
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
// on its own schedule: Store.ObjectsListUnsettledIDs reports exactly the objects owed a
// pass, and Client.Requeue dispatches one. Startup drains owed work itself, so this
// is not the only way such an object gets reconciled — but it is pinned as public
// surface, because a deployment that turns every ticker off needs it for anything
// that falls behind after startup.
func TestSelfDrivenRecovery(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	gk := GroupKind{Kind: "Widget"}

	raw, err := store.ObjectsCreate(ctx, gk, ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	bh, err := New(store, withOwedPassInterval(0), WithFullPassInterval(0))
	require.NoError(t, err)
	ctrl := &recordingController{reconciled: make(chan ObjectID, 4)}
	_, err = Register(bh, gk, ctrl, WithStartupFullPass(false))
	require.NoError(t, err)
	client := NewClient[tSpec, tStatus](bh, gk)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	// Startup drains owed work regardless of the full-pass choice, so consume that
	// dispatch first — otherwise it, not the requeue below, could satisfy the
	// assertion.
	select {
	case <-ctrl.reconciled:
	case <-time.After(testTimeout):
		t.Fatal("startup did not drain the owed object")
	}

	// The embedder's own backstop, on whatever schedule it likes.
	ids, err := store.ObjectsListUnsettledIDs(ctx, gk)
	require.NoError(t, err)
	require.Equal(t, []ObjectID{raw.ID}, ids, "the unconverged row is what ObjectsListUnsettledIDs reports")
	require.NoError(t, client.Requeue(ctx, raw.ID))

	select {
	case got := <-ctrl.reconciled:
		assert.Equal(t, raw.ID, got)
	case <-time.After(testTimeout):
		t.Fatal("self-driven requeue never reconciled the object: the documented recovery path does not work")
	}
}

// TestStartupFullPassDisabledSkipsSettled is the unit-level twin of the
// store-backed TestStartupFullPassReconcilesSettled: allIDsStore reports the object
// via ObjectsListIDs but not via any owed-work listing, so with the startup full
// pass off
// nothing enqueues it — the owed-work drain has nothing to drain.
func TestStartupFullPassDisabledSkipsSettled(t *testing.T) {
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
	logger, started := loggerSignallingOn(reconcilerStartedMsg)
	r := &reconciler{
		adapter:          adapter,
		store:            &allIDsStore{ids: []ObjectID{7}},
		work:             newWorkQueue(),
		maxRetryInterval: time.Second,
		startupFullPass:  false,
		backoffFor:       make(map[ObjectID]time.Duration),
		logger:           logger,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runInBackground(r, ctx)

	// Barrier rather than a grace period: run logs "reconciler started" only after
	// both startup passes have finished enqueueing, so a sentinel added now sits
	// behind anything they queued in the FIFO work queue. Whichever id the worker
	// reports first therefore answers the question outright — object 7 means the
	// disabled full pass dispatched it anyway.
	waitClosed(t, started, "the startup passes to finish enqueueing")
	const sentinel ObjectID = 99
	r.enqueue(sentinel)

	select {
	case got := <-reconciled:
		assert.Equal(t, sentinel, got, "settled object reconciled with the startup full pass off")
	case <-time.After(testTimeout):
		t.Fatal("the sentinel never reconciled: the loop is not dispatching at all")
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
// exactly the IDs returned by ObjectsListUnsettledIDs, in order.
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

// errReconcileOwedStore fails the durable-wake listing, so a test can drive
// enqueueFrom's skipped-pass branch, whose log is what keeps a lost listing
// distinguishable from "nothing was owed".
type errReconcileOwedStore struct {
	fakeStore
}

func (s *errReconcileOwedStore) ReconcileOwedListIDs(context.Context, GroupKind) ([]ObjectID, error) {
	return nil, errBoom
}

// TestEnqueueFromListErrorSkipsPass pins that a failed lister enqueues nothing and
// survives a reconciler built without a logger — the shape these tests use, and the
// one the new warn would panic on if it reached r.logger directly.
func TestEnqueueFromListErrorSkipsPass(t *testing.T) {
	r := &reconciler{
		store:      &errReconcileOwedStore{},
		work:       newWorkQueue(),
		backoffFor: make(map[ObjectID]time.Duration),
	}

	r.enqueueReconcileOwed(context.Background()) // r.logger is nil: must warn, not panic

	r.work.mu.Lock()
	items := append([]ObjectID(nil), r.work.items...)
	r.work.mu.Unlock()
	assert.Empty(t, items, "a failed list enqueues nothing")
}

// TestEnqueueReconcileOwed verifies that enqueueReconcileOwed enqueues exactly the IDs
// returned by ReconcileOwedListIDs, in order — the sibling of the test above.
// Only its failed-list branch was covered (TestEnqueueFromListErrorSkipsPass), so
// the helper whose whole purpose is not losing an owed wake was the one of the
// three that could have stopped enqueuing anything without a test noticing.
func TestEnqueueReconcileOwed(t *testing.T) {
	r := &reconciler{
		store:      &reconcileOwedIDsStore{ids: []ObjectID{5, 8}},
		work:       newWorkQueue(),
		backoffFor: make(map[ObjectID]time.Duration),
	}

	r.enqueueReconcileOwed(context.Background())

	r.work.mu.Lock()
	items := append([]ObjectID(nil), r.work.items...)
	r.work.mu.Unlock()
	assert.Equal(t, []ObjectID{5, 8}, items)
}

// TestOwedPassTickEnqueuesReconcileOwed covers run's *tick* call to
// enqueueReconcileOwed at the unit level, with no store: the restart test that pins
// durable-wake recovery disables every ticker, so deleting the tick's enqueue left
// the suite green. Owed wakes ride the owed-pass tick, not the full pass — a wake is
// recorded work, which is what the owed pass exists to drain.
//
// A disabled startup full pass plus a store that withholds its owed IDs until
// the second
// listing means neither the startup pass nor any other backstop can be what
// enqueues the object — only a tick can.
func TestOwedPassTickEnqueuesReconcileOwed(t *testing.T) {
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
		store:            &tickOnlyReconcileOwedStore{ids: []ObjectID{owedID}},
		work:             newWorkQueue(),
		owedPassInterval: time.Millisecond, // the tick is the code under test
		maxRetryInterval: time.Second,
		startupFullPass:  false,
		backoffFor:       make(map[ObjectID]time.Duration),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runInBackground(r, ctx)

	select {
	case got := <-reconciled:
		assert.Equal(t, owedID, got)
	case <-time.After(testTimeout):
		t.Fatal("owed wake was never enqueued by a owed-pass tick")
	}

	cancel()
	waitClosed(t, done, "run to return after cancel")
}

// TestEnqueueUnsettledSkipsInFlight verifies that a full pass does not re-enqueue
// an object whose reconcile is already in progress.
func TestEnqueueUnsettledSkipsInFlight(t *testing.T) {
	const objID = ObjectID(42)

	block := make(chan struct{})
	started := newSignal()

	adapter := &fakeAdapter{
		reconcileFn: func(_ context.Context, _ ObjectID) (Result, error) {
			started.fire()
			<-block
			return Result{}, nil
		},
	}

	r := &reconciler{
		adapter:          adapter,
		store:            &unsettledIDsStore{ids: []ObjectID{objID}},
		work:             newWorkQueue(),
		fullPassInterval: 0,
		maxRetryInterval: time.Second,
		backoffFor:       make(map[ObjectID]time.Duration),
		concurrency:      2,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	r.enqueue(objID)
	started.wait(t, "reconcile to start")

	// Simulate a full-pass tick while the reconcile is still in-flight.
	r.enqueueUnsettled(ctx)

	r.work.mu.Lock()
	qLen := len(r.work.items)
	r.work.mu.Unlock()
	assert.Equal(t, 0, qLen, "in-flight object must not be re-enqueued by a full pass")

	close(block)
	cancel()
	waitClosed(t, done, "run to exit")
}

func TestReconcilerConcurrency(t *testing.T) {
	const numObjects = 5
	const workers = 3

	gate := make(chan struct{})
	allStarted := newSignal()

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
				allStarted.fire()
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
		fullPassInterval: 0,
		maxRetryInterval: time.Second,
		backoffFor:       make(map[ObjectID]time.Duration),
		concurrency:      workers,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	for i := ObjectID(1); i <= numObjects; i++ {
		r.enqueue(i)
	}

	allStarted.wait(t, "3 concurrent reconciles to start")
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

	inReconcile := newSignal()     // fires when the first reconcile starts
	release := make(chan struct{}) // unblocks the first reconcile

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
				inReconcile.fire()
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
		fullPassInterval: 0,
		maxRetryInterval: time.Second,
		backoffFor:       make(map[ObjectID]time.Duration),
		concurrency:      workers,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	r.enqueue(objID)
	inReconcile.wait(t, "first reconcile to start")

	for range 50 {
		r.enqueue(objID)
	}

	close(release)
	cancel()
	waitClosed(t, done, "run to exit")

	assert.Equal(t, 1, maxActive, "the same object must never be reconciled by two workers at once")
}

func TestNextBackoffDefaultBase(t *testing.T) {
	// When baseRetryInterval is 0, backoffNext falls back to defaultBaseRetryInterval.
	r := &reconciler{
		backoffFor:       make(map[ObjectID]time.Duration),
		maxRetryInterval: time.Minute,
		// baseRetryInterval left as zero
	}
	d := r.backoffNext(1)
	assert.Equal(t, defaultBaseRetryInterval, d)
}

func TestNextBackoffDoubles(t *testing.T) {
	r := &reconciler{
		backoffFor:        make(map[ObjectID]time.Duration),
		maxRetryInterval:  time.Minute,
		baseRetryInterval: 10 * time.Millisecond,
	}
	first := r.backoffNext(1)
	assert.Equal(t, 10*time.Millisecond, first)
	second := r.backoffNext(1) // cur != 0, so it doubles
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
	assert.Equal(t, 10*time.Millisecond, r.backoffNext(1))
	assert.Equal(t, 20*time.Millisecond, r.backoffNext(1))

	// requeue without reset preserves the ladder, so the next failure continues
	// from where it was: 20ms → 40ms, not back to base.
	r.requeue(1, false)
	assert.Equal(t, 40*time.Millisecond, r.backoffNext(1), "requeue(reset=false) must not reset the ladder")

	// requeue with reset restarts the ladder from base.
	r.requeue(1, true)
	assert.Equal(t, 10*time.Millisecond, r.backoffNext(1), "requeue(reset=true) must restart the ladder from base")
}

func TestNextBackoffCaps(t *testing.T) {
	r := &reconciler{
		backoffFor:        make(map[ObjectID]time.Duration),
		maxRetryInterval:  50 * time.Millisecond,
		baseRetryInterval: 40 * time.Millisecond,
	}
	first := r.backoffNext(1)
	assert.Equal(t, 40*time.Millisecond, first)
	// 40ms * 2 = 80ms > 50ms cap → capped at 50ms.
	second := r.backoffNext(1)
	assert.Equal(t, 50*time.Millisecond, second)
}

// listCallStore signals a channel each time ObjectsListUnsettledIDs is called, so the
// test can wait for the full-pass tick to fire without using time.Sleep.
type listCallStore struct {
	fakeStore
	callCh chan struct{}
}

func (s *listCallStore) ObjectsListUnsettledIDs(_ context.Context, _ GroupKind) ([]ObjectID, error) {
	select {
	case s.callCh <- struct{}{}:
	default:
	}
	return nil, nil
}

// TestRunDrainsOwedWorkOnTick verifies the owed-pass ticker keeps firing: the unsettled
// listing runs once at startup and again on every tick. This is the loop-level
// pin; which objects each listing returns is covered by the store-backed tests.
func TestRunDrainsOwedWorkOnTick(t *testing.T) {
	store := &listCallStore{callCh: make(chan struct{}, 10)}
	r := &reconciler{
		store:            store,
		work:             newWorkQueue(),
		owedPassInterval: 5 * time.Millisecond,
		backoffFor:       make(map[ObjectID]time.Duration),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	// Drain the initial startup enqueueUnsettled call.
	select {
	case <-store.callCh:
	case <-time.After(testTimeout):
		t.Fatal("initial enqueueUnsettled not called")
	}

	// Wait for at least one owed-pass-tick-driven enqueueUnsettled call.
	select {
	case <-store.callCh:
	case <-time.After(testTimeout):
		t.Fatal("owed-pass tick did not call enqueueUnsettled")
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

// getObjectBadSpecStore is a Store whose ObjectsGet returns a RawObject with
// invalid spec JSON, exercising the rawToTyped error path inside
// typedController.reconcile. Within is inherited from fakeStore (inline passthrough).
type getObjectBadSpecStore struct {
	fakeStore
}

func (s *getObjectBadSpecStore) ObjectsGet(_ context.Context, id ObjectID) (*RawObject, error) {
	return &RawObject{ID: id, Kind: "Widget", Spec: []byte("not-json")}, nil
}

func (s *getObjectBadSpecStore) ObjectsGetForReconcile(ctx context.Context, id ObjectID) (storeapi.ReconcileLoad, error) {
	return reconcileLoadOf(s.ObjectsGet(ctx, id))
}

// TestTypedControllerReconcileRawToTypedError pins the quarantine: an undecodable
// row (not deletion-pending) is a no-op success, not a retryable error — returning
// the error would retry the identical bytes forever under backoff, and the full pass
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

func (s *owedBadSpecStore) ObjectsGet(_ context.Context, id ObjectID) (*RawObject, error) {
	return &RawObject{ID: id, Kind: "Widget", Spec: []byte("not-json"), ReconcileOwed: 2}, nil
}

func (s *owedBadSpecStore) ObjectsGetForReconcile(ctx context.Context, id ObjectID) (storeapi.ReconcileLoad, error) {
	return reconcileLoadOf(s.ObjectsGet(ctx, id))
}

func (s *owedBadSpecStore) ReconcileOwedDecrement(context.Context, GroupKind, ObjectID, int64) error {
	s.decremented = true
	return nil
}

// TestTypedControllerReconcileQuarantineKeepsReconcileOwed pins that quarantining an
// undecodable row does not drain its owed wake. The pass never reached the
// controller, so the wake is still owed; draining it would silently discard a real
// obligation and leave the dependent stale with nothing recording it. The count is
// meant to outlive the poison and be serviced by the first pass that can decode —
// so a future refactor must not "fix" this by hoisting the decrement above the
// quarantine return.
func TestTypedControllerReconcileQuarantineKeepsReconcileOwed(t *testing.T) {
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
// its name and owned_by edge waiting for a controller that can never decode it.
func TestTypedControllerReconcileRawToTypedErrorCollectsDeleting(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	gk := GroupKind{Kind: "Widget"}

	// Inject an undecodable row directly (a valid create can always decode), then
	// request its deletion so the reconcile sees a deletion-pending poison row.
	raw, err := store.ObjectsCreate(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte("not-json")})
	require.NoError(t, err)
	_, err = store.DeletionRequestsCreate(ctx, gk, raw.ID)
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

	_, err = store.ObjectsGet(ctx, raw.ID)
	require.ErrorIs(t, err, ErrNotFound, "the finalizer-free deleting poison row must be collected, not stranded")
}

// undecodableDeletingCollectErrorStore returns an undecodable, deletion-pending
// row from ObjectsGet, and errors from ObjectsGetMeta so that collect (which reads
// meta first) fails. This exercises the GC-error leg of the quarantine: a poison
// deleting row whose collect fails must surface the error for retry, not swallow
// it as a no-op success.
type undecodableDeletingCollectErrorStore struct {
	fakeStore
}

func (s *undecodableDeletingCollectErrorStore) ObjectsGet(_ context.Context, id ObjectID) (*RawObject, error) {
	deletedAt := time.Unix(1, 0)
	return &RawObject{ID: id, Kind: "Widget", Spec: []byte("not-json"), DeletionRequestedAt: &deletedAt}, nil
}

func (s *undecodableDeletingCollectErrorStore) ObjectsGetForReconcile(ctx context.Context, id ObjectID) (storeapi.ReconcileLoad, error) {
	return reconcileLoadOf(s.ObjectsGet(ctx, id))
}

func (s *undecodableDeletingCollectErrorStore) ObjectsGetMeta(context.Context, ObjectID) (*RawObject, error) {
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

// getObjectErrorStore returns an error from ObjectsGet to exercise path A in
// typedController.reconcile (the ObjectsGet error before rawToTyped). Within is
// inherited from fakeStore (inline passthrough).
type getObjectErrorStore struct {
	fakeStore
}

func (s *getObjectErrorStore) ObjectsGet(_ context.Context, _ ObjectID) (*RawObject, error) {
	return nil, errBoom
}

func (s *getObjectErrorStore) ObjectsGetForReconcile(ctx context.Context, id ObjectID) (storeapi.ReconcileLoad, error) {
	return reconcileLoadOf(s.ObjectsGet(ctx, id))
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

// notFoundStore returns ErrNotFound from ObjectsGet, modeling an object that was
// already collected (by a prior pass, a cascade, or the backstop) between its
// enqueue and this reconcile.
type notFoundStore struct {
	fakeStore
}

func (s *notFoundStore) ObjectsGet(_ context.Context, _ ObjectID) (*RawObject, error) {
	return nil, ErrNotFound
}

func (s *notFoundStore) ObjectsGetForReconcile(ctx context.Context, id ObjectID) (storeapi.ReconcileLoad, error) {
	return reconcileLoadOf(s.ObjectsGet(ctx, id))
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
// e.g. an DependenciesAdd to a target that was deleted. That is a real failure to
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
	raw, err := s.ObjectsCreate(ctx, GroupKind{Kind: "Widget"}, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
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
	raw, err := s.ObjectsCreate(ctx, GroupKind{Kind: "Widget"}, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)
	_, err = s.DeletionRequestsCreate(ctx, GroupKind{Kind: "Widget"}, raw.ID)
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

	_, err = s.ObjectsGet(ctx, raw.ID)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestTypedControllerReconcile(t *testing.T) {
	ctx := context.Background()

	s, err := sqlite.OpenMemory()
	require.NoError(t, err)
	defer s.Close()

	specJSON, err := json.Marshal(tSpec{})
	require.NoError(t, err)
	raw, err := s.ObjectsCreate(ctx, GroupKind{Kind: "Widget"}, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
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
// ControllerClient passed into Reconcile). If signal is non-nil it fires after
// fn's first call, so a test can wait for the reconcile to have run.
type funcController struct {
	signal *signal
	fn     func(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error)
}

func (c *funcController) Reconcile(ctx context.Context, client ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
	res, err := c.fn(ctx, client, obj)
	if c.signal != nil {
		c.signal.fire()
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
	raw, err := s.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
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

	got, err := s.ObjectsGet(ctx, raw.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status, "the status write committed despite the reconcile error")
	assert.NotNil(t, got.ObservedGeneration)
}

// wrapStore applies a harness's optional store decoration, so the harnesses read
// as "the controller writes through this" while their assertions keep reading the
// real store underneath.
func wrapStore(s Store, wrap func(Store) Store) Store {
	if wrap == nil {
		return s
	}
	return wrap(s)
}

// newSyncController wires a typedController over s with its ControllerClient and a
// funcController the caller scripts. Nothing here starts a loop: reconcile is
// called directly, so a pass's bookkeeping writes have landed by the time it
// returns, which is what lets these tests assert on them without waiting.
func newSyncController(s Store) (*typedController[cSpec, cStatus], *funcController) {
	bh := &Beehive{store: s}
	inner := &funcController{}
	return &typedController[cSpec, cStatus]{
		gk:     clientTestGK,
		bh:     bh,
		client: &controllerClientImpl[cStatus]{bh: bh, gk: clientTestGK},
		inner:  inner,
	}, inner
}

// reconcileOwedHarness builds a typedController over a real store, driven
// synchronously so the decrement has run by the time reconcile returns. wrap, if
// non-nil, decorates the store the controller writes through (to inject a failing
// mutator); the returned count always reads the real store underneath it.
// reconcileOwedHarness returns the pieces the durable-wake tests need,
// including owe: seeding an owed wake goes through the concrete store, since
// ReconcileOwedIncrement is deliberately absent from the Store interface (EdgesAdd is
// production's only producer). A closure rather than the store itself because the
// concrete type is unexported in package sqlite and so cannot be named here.
func reconcileOwedHarness(t *testing.T, wrap func(Store) Store) (*typedController[cSpec, cStatus], *funcController, ObjectID, func(*testing.T) int64, func() error) {
	t.Helper()
	ctx := context.Background()
	s, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	specJSON, err := json.Marshal(cSpec{})
	require.NoError(t, err)
	raw, err := s.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)

	tc, inner := newSyncController(wrapStore(s, wrap))
	count := func(t *testing.T) int64 {
		t.Helper()
		got, err := s.ObjectsGet(ctx, raw.ID)
		require.NoError(t, err)
		return got.ReconcileOwed
	}
	owe := func() error { return s.ReconcileOwedIncrement(ctx, raw.ID) }
	return tc, inner, raw.ID, count, owe
}

// TestReconcileDecrementsReconcileOwed pins the durable-wake decrement: a successful
// pass services one owed wake (count down by one), and a failed pass leaves the
// count owed for the backstop to retry.
func TestReconcileDecrementsReconcileOwed(t *testing.T) {
	ctx := context.Background()
	tc, inner, id, count, owe := reconcileOwedHarness(t, nil)

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

// TestReconcileDrainsMultipleOwedPasses pins that one pass services every wake
// it observed, not just one. A crashed process can leave a count above 1; the
// backstop enqueues that row exactly once (the work queue coalesces), so a pass
// that subtracted only 1 would strand the remainder with nothing to re-enqueue it —
// indefinitely when the full pass is disabled, and one per tick otherwise.
// Subtracting the
// observed count drains it in the single pass the backstop scheduled.
func TestReconcileDrainsMultipleOwedPasses(t *testing.T) {
	ctx := context.Background()
	tc, inner, id, count, owe := reconcileOwedHarness(t, nil)

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

// TestReconcileOwedSurvivesConcurrentIncrement pins why reconcile_owed is a count
// rather than a single token: a second wake owed *while a reconcile is already
// servicing an earlier one* must not be lost. A token carrying a value both wakes
// share would be cleared by the first pass, dropping the second. As a +1/-1 count it
// cannot: the mid-pass increment outlives the pass's subtraction (it lands above the
// count that pass observed), leaving the object owed and re-enqueued by the
// backstop.
func TestReconcileOwedSurvivesConcurrentIncrement(t *testing.T) {
	ctx := context.Background()
	tc, inner, id, count, owe := reconcileOwedHarness(t, nil)

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

// failDecrementReconcileOwedStore fails the durable-wake decrement while delegating
// the rest, so a test can exercise the reconciler's log-and-continue branch.
type failDecrementReconcileOwedStore struct {
	Store
}

func (s *failDecrementReconcileOwedStore) ReconcileOwedDecrement(context.Context, GroupKind, ObjectID, int64) error {
	return errBoom
}

// TestReconcileReconcileOwedDecrementErrorIsNonFatal pins that a failed decrement does
// not fail the reconcile: the count stays up and the backstop re-enqueues (a
// harmless extra pass), so shadowing the successful reconcile with the decrement
// error would be strictly worse.
func TestReconcileReconcileOwedDecrementErrorIsNonFatal(t *testing.T) {
	ctx := context.Background()
	tc, inner, id, count, owe := reconcileOwedHarness(t, func(s Store) Store {
		return &failDecrementReconcileOwedStore{Store: s}
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
// full-pass sweeper is disabled here, so the in-reconcile collect is the only driver).
func TestReconcileRunsGCAfterCommittedWritesOnError(t *testing.T) {
	ctx := context.Background()

	s, err := sqlite.OpenMemory()
	require.NoError(t, err)
	defer s.Close()

	specJSON, err := json.Marshal(cSpec{})
	require.NoError(t, err)
	raw, err := s.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{
		Name:       uniqueName(),
		Spec:       specJSON,
		Finalizers: []string{"f"},
	})
	require.NoError(t, err)
	_, err = s.DeletionRequestsCreate(ctx, clientTestGK, raw.ID)
	require.NoError(t, err)

	bh := &Beehive{store: s}
	tc := &typedController[cSpec, cStatus]{
		gk:     clientTestGK,
		bh:     bh,
		client: &controllerClientImpl[cStatus]{bh: bh, gk: clientTestGK},
		inner: &funcController{fn: func(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
			if err := cc.FinalizersDelete(ctx, obj.ID, "f"); err != nil {
				return Result{}, err
			}
			return Result{}, errBoom
		}},
	}

	_, _ = tc.reconcile(ctx, raw.ID)

	_, err = s.ObjectsGet(ctx, raw.ID)
	require.ErrorIs(t, err, ErrNotFound,
		"the committed finalizer clear must let GC collect the row even though reconcile errored")
}

// statusSettingController writes a fixed status on the first Reconcile call and
// fires reconciled.
type statusSettingController struct {
	reconciled *signal
}

func (c *statusSettingController) Reconcile(ctx context.Context, client ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
	if err := client.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"}); err != nil {
		return Result{}, err
	}
	c.reconciled.fire()
	return Result{}, nil
}

// specEchoController writes cStatus{Val: obj.Spec.Val} on every Reconcile.
// firstDone fires after the first successful reconcile; secondDone fires once a
// reconcile observes generation 2, signalling that the spec update — not merely a
// second reconcile — was seen.
type specEchoController struct {
	firstDone  *signal
	secondDone *signal
}

func (c *specEchoController) Reconcile(ctx context.Context, client ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
	if err := client.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: obj.Spec.Val}); err != nil {
		return Result{}, err
	}
	c.firstDone.fire()
	// Gate on the observed generation, not a reconcile count: a duplicate startup
	// reconcile of the original generation (the startup pass can race the Create's
	// own enqueue) must not be mistaken for the update being reconciled.
	if obj.Generation >= 2 {
		c.secondDone.fire()
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
	reconciled *signal
	deleted    *signal
}

func (c *deletionTrackingController) Reconcile(ctx context.Context, client ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
	if obj.DeletionRequestedAt != nil {
		c.deleted.fire()
		// Clear the finalizer so GC can collect the row now that the deletion has
		// been observed (idempotent: re-clearing a gone finalizer is a no-op).
		if err := client.FinalizersDelete(ctx, obj.ID, deletionTrackingFinalizer); err != nil {
			return Result{}, err
		}
		return Result{}, nil
	}
	if err := client.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"}); err != nil {
		return Result{}, err
	}
	c.reconciled.fire()
	return Result{}, nil
}

func TestIntegrationCreateTriggersReconcile(t *testing.T) {
	ctx := context.Background()

	bh, err := New(newClientTestStore(t), fast(WithFullPassInterval(0))...)
	require.NoError(t, err)

	ctrl := &statusSettingController{reconciled: newSignal()}
	_, err = Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

	ctrl.reconciled.wait(t, "first reconcile")

	got, err := client.GetByID(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status)
	assert.Equal(t, "done", got.Status.Val)
	require.NotNil(t, got.ObservedGeneration)
	assert.Equal(t, obj.Generation, *got.ObservedGeneration)
}

func TestIntegrationUpdateTriggersReconcile(t *testing.T) {
	ctx := context.Background()

	bh, err := New(newClientTestStore(t), fast(WithFullPassInterval(0))...)
	require.NoError(t, err)

	ctrl := &specEchoController{
		firstDone:  newSignal(),
		secondDone: newSignal(),
	}
	_, err = Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "v1"})

	// Wait for the first reconcile before updating, so the update is genuinely a
	// distinct reconcile of generation 2 rather than being coalesced with the
	// create into a single pass.
	ctrl.firstDone.wait(t, "first reconcile")

	_, err = client.UpdateByID(ctx, obj.ID, cSpec{Val: "v2"})
	require.NoError(t, err)

	ctrl.secondDone.wait(t, "second reconcile after spec update")

	got, err := client.GetByID(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status)
	assert.Equal(t, "v2", got.Status.Val)
}

func TestIntegrationDeleteTriggersReconcile(t *testing.T) {
	ctx := context.Background()

	bh, err := New(newClientTestStore(t), fast(WithFullPassInterval(0))...)
	require.NoError(t, err)

	ctrl := &deletionTrackingController{
		reconciled: newSignal(),
		deleted:    newSignal(),
	}
	_, err = Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	// The finalizer keeps the row alive until the controller observes the deletion;
	// see deletionTrackingFinalizer.
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"}, WithFinalizers(deletionTrackingFinalizer))

	ctrl.reconciled.wait(t, "first reconcile")

	require.NoError(t, client.DeleteByID(ctx, obj.ID))
	ctrl.deleted.wait(t, "reconcile after deletion requested")
}

// TestIntegrationWritePersistsAcrossReconcileError is the end-to-end counterpart
// of TestReconcilePersistsWritesOnError: a status write made during a reconcile
// that then returns an error stays committed, because reconcile no longer runs
// under a transaction. (To make a group of writes atomic, a controller uses
// ControllerClient.Within — see TestControllerClientWithin.)
func TestIntegrationWritePersistsAcrossReconcileError(t *testing.T) {
	ctx := context.Background()

	bh, err := New(newClientTestStore(t), fast(WithFullPassInterval(0))...)
	require.NoError(t, err)

	ctrl := &funcController{
		signal: newSignal(),
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
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

	ctrl.signal.wait(t, "reconcile wrote status before erroring")

	got, err := client.GetByID(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status, "status write commits even though the reconcile returned an error")
	assert.Equal(t, "persisted", got.Status.Val)
}

// conditionSettingController sets a Ready=True condition on the first Reconcile,
// then fires reconciled.
type conditionSettingController struct {
	reconciled *signal
}

func (c *conditionSettingController) Reconcile(ctx context.Context, client ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
	if err := client.ConditionsSet(ctx, obj.ID, Condition{
		Type: "Ready", Status: ConditionTrue, Reason: "Provisioned",
	}); err != nil {
		return Result{}, err
	}
	c.reconciled.fire()
	return Result{}, nil
}

func TestIntegrationSetConditionCommitsAndFlows(t *testing.T) {
	ctx := context.Background()

	bh, err := New(newClientTestStore(t), fast(WithFullPassInterval(0))...)
	require.NoError(t, err)

	ctrl := &conditionSettingController{reconciled: newSignal()}
	_, err = Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

	ctrl.reconciled.wait(t, "first reconcile")

	// Flows through Get.
	got, err := client.GetByID(ctx, obj.ID)
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

	bh, err := New(newClientTestStore(t), fast(WithFullPassInterval(0))...)
	require.NoError(t, err)

	ctrl := &funcController{
		signal: newSignal(),
		fn: func(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
			_ = cc.ConditionsSet(ctx, obj.ID, Condition{Type: "Ready", Status: ConditionTrue})
			return Result{}, errBoom
		},
	}
	_, err = Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

	ctrl.signal.wait(t, "reconcile set condition before erroring")

	got, err := client.GetByID(ctx, obj.ID)
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
	_, err = store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)

	bh, err := New(store, WithFullPassInterval(0))
	require.NoError(t, err)

	ctrl := &statusSettingController{reconciled: newSignal()}
	_, err = Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	// Without startup enqueue this would time out (the full pass is disabled).
	ctrl.reconciled.wait(t, "reconcile of pre-existing object at startup")
}

// TestReconcilerRequeueNow verifies requeueNow cancels any pending delayed retry
// timer and makes the id immediately dispatchable, while preserving the backoff
// ladder (clearing is the caller's separate backoffClear step).
func TestReconcilerRequeueNow(t *testing.T) {
	r := &reconciler{
		work:              newWorkQueue(),
		maxRetryInterval:  time.Second,
		baseRetryInterval: 5 * time.Millisecond,
		backoffFor:        make(map[ObjectID]time.Duration),
	}
	// Simulate a failed reconcile: a backoff entry and a far-future retry timer.
	seeded := r.backoffNext(1)
	r.work.addAfter(1, time.Hour)
	require.NotZero(t, r.backoffFor[1], "precondition: backoff seeded")
	require.NotNil(t, r.work.gauge.alarmAt(1), "precondition: retry timer scheduled")

	r.requeueNow(1)

	assert.Equal(t, seeded, r.backoffFor[1], "requeueNow must preserve the backoff entry")
	assert.Nil(t, r.work.gauge.alarmAt(1), "requeueNow must cancel the stale retry timer")

	id, ok := r.work.get()
	require.True(t, ok, "requeueNow must make the id dispatchable now")
	assert.Equal(t, ObjectID(1), id)
}

// TestReconcilerScheduleAt verifies scheduleAt reports a pending delayed add's
// fire time and reports the zero Schedule for an id with no schedule.
func TestReconcilerScheduleAt(t *testing.T) {
	r := &reconciler{work: newWorkQueue()}
	r.work.addAfter(1, time.Hour)

	at := r.scheduleAt(1).Schedule.NextRequeueAt
	require.False(t, at.IsZero())
	assert.True(t, at.After(time.Now().Add(time.Minute)), "fire time must be ~1h out, got %s", at)

	assert.True(t, r.scheduleAt(2).Schedule.NextRequeueAt.IsZero(),
		"an id with no schedule must report nothing")
}

// TestReconcilerScheduleAtNilWork verifies the scheduling methods are safe on a
// reconciler with no work queue (built outside Register, e.g. in tests).
func TestReconcilerScheduleAtNilWork(t *testing.T) {
	r := &reconciler{backoffFor: make(map[ObjectID]time.Duration)}
	assert.True(t, r.scheduleAt(1).Schedule.NextRequeueAt.IsZero(),
		"nil work queue must report nothing scheduled")
	assert.NotPanics(t, func() { r.requeueNow(1) }, "requeueNow must be nil-work safe")
}

// wakeStampingStore is the store surface an owed-pass test needs: the Store contract
// plus ReconcileOwedIncrement, which is deliberately not on Store (see the comment
// on reconcileOwedHarness) but exists on the concrete sqlite store so a
// test can seed an owed wake without staging the whole declare race.
type wakeStampingStore interface {
	Store
	ReconcileOwedIncrement(context.Context, ObjectID) error
}

// newOwedPassHarness starts a control plane whose only periodic driver is the
// owed-pass tick — the full pass and GC off, no startup spec pass — and returns
// once the
// startup pass has provably drained both owed sets. Whatever the caller seeds
// after this can only be dispatched by a tick.
func newOwedPassHarness(t *testing.T, gk GroupKind, seed func(wakeStampingStore)) (wakeStampingStore, <-chan ObjectID) {
	t.Helper()
	real, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { real.Close() })
	// Seeding before Start lets a test establish a *settled* object without racing
	// the tick, which would otherwise dispatch it for being briefly unsettled.
	if seed != nil {
		seed(real)
	}

	store := &listProbeStore{
		Store:           real,
		unsettledListed: make(chan struct{}, 8),
		owedListed:      make(chan struct{}, 8),
	}
	reconciled := make(chan ObjectID, 4)

	bh, err := New(store, WithFullPassInterval(0), withoutGCSweeper(),
		withOwedPassInterval(10*time.Millisecond))
	require.NoError(t, err)
	_, err = Register(bh, gk, &recordingController{reconciled: reconciled},
		WithStartupFullPass(false))
	require.NoError(t, err)

	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { stop(context.Background()) })

	// The startup pass runs enqueueReconcileOwed unconditionally; the unsettled
	// listing only arrives via a tick with the startup full pass off, so waiting on
	// the wake signal is what proves startup is behind us.
	select {
	case <-store.owedListed:
	case <-time.After(testTimeout):
		t.Fatal("startup pass never listed pending wakes")
	}
	return real, reconciled
}

// TestOwedPassTickDispatchesOwedWork pins the owed-pass ticker: the cheap, frequent
// pass that drains work the store has *recorded* as owed — an unconverged spec
// here — on a cadence of its own.
//
// It is deliberately separate from the full-pass knob. Draining owed work is bounded
// by what is actually outstanding (indexed listings that return nothing in a
// converged system), while re-confirming every object scales with the object
// count. One interval governing both means tuning either moves the other.
func TestOwedPassTickDispatchesOwedWork(t *testing.T) {
	ctx := context.Background()
	gk := GroupKind{Kind: "Widget"}
	real, reconciled := newOwedPassHarness(t, gk, nil)

	// An object a prior process left unconverged: written straight through the
	// store, so observed_generation is NULL and nothing has dispatched it.
	raw, err := real.ObjectsCreate(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte(`{}`)})
	require.NoError(t, err)

	select {
	case got := <-reconciled:
		assert.Equal(t, raw.ID, got)
	case <-time.After(testTimeout):
		t.Fatal("unsettled object was never dispatched: no owed-pass tick is draining owed work")
	}
}

// TestOwedPassTickDispatchesOwedWake pins the *other* half of the owed-pass set.
// An object owed a durable dependency wake is settled by definition — that is
// precisely why the unsettled listing cannot see it — so if the owed pass
// drained only
// unsettled objects, a wake recorded across a restart would never be delivered.
// The two listings read different columns and need separate coverage.
func TestOwedPassTickDispatchesOwedWake(t *testing.T) {
	ctx := context.Background()
	gk := GroupKind{Kind: "Widget"}

	// Seeded before Start and left *settled*, so the unsettled listing can never
	// be what dispatches it.
	var id ObjectID
	real, reconciled := newOwedPassHarness(t, gk, func(s wakeStampingStore) {
		raw, err := s.ObjectsCreate(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte(`{}`)})
		require.NoError(t, err)
		err = s.ObjectsUpdateStatus(ctx, gk, raw.ID, raw.Generation, []byte(`{}`), 0)
		require.NoError(t, err)
		id = raw.ID
	})

	// Now owed a wake, the way a crash between a target's commit and the
	// dependent's dispatch leaves it.
	require.NoError(t, real.ReconcileOwedIncrement(ctx, id))

	select {
	case got := <-reconciled:
		assert.Equal(t, id, got)
	case <-time.After(testTimeout):
		t.Fatal("object owed a wake was never dispatched: the owed pass drains only the unsettled half")
	}
}

// newSettledHarness starts a control plane over a real store holding one settled
// object, with the owed-pass tick and GC off and no startup spec pass. A settled
// object is invisible to every owed-work listing, so nothing but a full pass can
// re-dispatch it — which is exactly what makes it the probe for the full pass.
// opts are
// forwarded to Register (i.e. whether the full pass is on).
//
// sentinel is the barrier a negative assertion needs: it waits for the startup
// passes to finish enqueueing, then requeues a second settled object and returns
// its id. Anything startup dispatched is ahead of it in the FIFO queue, so a test
// can read the stream until the sentinel arrives and know it has seen everything.
func newSettledHarness(t *testing.T, opts ...Option) (id ObjectID, reconciled <-chan ObjectID, sentinel func() ObjectID) {
	t.Helper()
	ctx := context.Background()
	gk := GroupKind{Kind: "Widget"}

	store, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	// Settled before Start: observed_generation == generation, so neither is owed
	// anything and no owed-pass listing will ever return them.
	settle := func() ObjectID {
		t.Helper()
		raw, err := store.ObjectsCreate(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte(`{}`)})
		require.NoError(t, err)
		err = store.ObjectsUpdateStatus(ctx, gk, raw.ID, raw.Generation, []byte(`{}`), 0)
		require.NoError(t, err)
		return raw.ID
	}
	probeID, sentinelID := settle(), settle()

	ch := make(chan ObjectID, 4)
	logger, started := loggerSignallingOn(reconcilerStartedMsg)
	bh, err := New(store, withOwedPassInterval(0), withoutGCSweeper(), WithLogger(logger))
	require.NoError(t, err)
	opts = append(opts, WithStartupFullPass(false))
	_, err = Register(bh, gk, &recordingController{reconciled: ch}, opts...)
	require.NoError(t, err)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { stop(ctx) })

	return probeID, ch, func() ObjectID {
		t.Helper()
		waitClosed(t, started, "the startup passes to finish enqueueing")
		bh.reconcilers[gk].requeueNow(sentinelID)
		return sentinelID
	}
}

// TestFullPassTickReconcilesSettled pins what WithFullPassInterval now buys: a pass
// over *every* object, converged ones included. That is the only thing that
// re-confirms process-scoped state a restart invalidated (liveness conditions read
// as "verifying" until this process rewrites them) and the only thing that heals a
// wake lost for a reason nothing recorded — neither is visible to any owed-work
// listing, so no owed-pass tick can reach it.
func TestFullPassTickReconcilesSettled(t *testing.T) {
	id, reconciled, _ := newSettledHarness(t, WithFullPassInterval(10*time.Millisecond))

	select {
	case got := <-reconciled:
		assert.Equal(t, id, got)
	case <-time.After(testTimeout):
		t.Fatal("settled object was never re-dispatched: the full-pass tick is not a full pass")
	}
}

// TestDefaultConfigDoesNotFullPass is the other half of the contract: with no
// full pass asked for, nothing re-dispatches a settled object. It guards the *shape*
// — that no other driver quietly grew into a full pass — not the default's value,
// which it cannot see: any default longer than this test's run looks identical
// from here. TestNewAppliesDefaults pins the value itself.
func TestDefaultConfigDoesNotFullPass(t *testing.T) {
	probe, reconciled, sentinel := newSettledHarness(t)

	// The sentinel is queued behind whatever startup dispatched, so the first id to
	// arrive settles it: the probe means a full pass ran uninvited.
	want := sentinel()
	select {
	case got := <-reconciled:
		assert.NotEqual(t, probe, got, "settled object was re-dispatched: the full pass is not opt-in")
		assert.Equal(t, want, got)
	case <-time.After(testTimeout):
		t.Fatal("the sentinel never reconciled: the loop is not dispatching at all")
	}
}

// newStartupHarness starts a control plane with every periodic driver off, so the
// startup pass is the only thing that can dispatch anything. seed runs before
// Start. It returns the ids the controller reconciled.
//
// The listing is closed by a sentinel rather than by the channel going quiet: a
// settled object seeded after the caller's own is requeued once startup has
// finished enqueueing, so the FIFO work queue hands it to the worker strictly
// last. Reading up to it yields the whole startup dispatch set and nothing more.
func newStartupHarness(t *testing.T, seed func(Store, GroupKind), opts ...Option) []ObjectID {
	t.Helper()
	ctx := context.Background()
	gk := GroupKind{Kind: "Widget"}

	store, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	seed(store, gk)

	sentinel, err := store.ObjectsCreate(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte(`{}`)})
	require.NoError(t, err)
	// Settled, so no startup pass of its own can reach it: the only thing that ever
	// dispatches it is the explicit requeue below.
	err = store.ObjectsUpdateStatus(ctx, gk, sentinel.ID, sentinel.Generation, []byte(`{}`), 0)
	require.NoError(t, err)

	reconciled := make(chan ObjectID, 8)
	logger, started := loggerSignallingOn(reconcilerStartedMsg)
	bh, err := New(store, withOwedPassInterval(0), WithFullPassInterval(0), withoutGCSweeper(), WithLogger(logger))
	require.NoError(t, err)
	_, err = Register(bh, gk, &recordingController{reconciled: reconciled}, opts...)
	require.NoError(t, err)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { stop(ctx) })

	waitClosed(t, started, "the startup passes to finish enqueueing")
	bh.reconcilers[gk].requeueNow(sentinel.ID)

	var got []ObjectID
	for {
		select {
		case id := <-reconciled:
			if id == sentinel.ID {
				return got
			}
			got = append(got, id)
		case <-time.After(testTimeout):
			t.Fatal("the sentinel never reconciled: the loop is not dispatching at all")
		}
	}
}

// TestStartupAlwaysDrainsOwedWork pins that startup resumes work the store has
// recorded as owed regardless of the full-pass choice. Declining it is not a
// cheapness knob — an object a previous process left unconverged, or one owed a
// durable wake, is *already* owed a pass, and with every ticker off nothing else
// will ever run it. The knob governs only the full re-confirm pass below.
func TestStartupAlwaysDrainsOwedWork(t *testing.T) {
	ctx := context.Background()
	var unsettled ObjectID
	got := newStartupHarness(t, func(s Store, gk GroupKind) {
		// Unconverged: observed_generation NULL, as a crash mid-reconcile leaves it.
		raw, err := s.ObjectsCreate(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte(`{}`)})
		require.NoError(t, err)
		unsettled = raw.ID
	}, WithStartupFullPass(false))

	assert.Equal(t, []ObjectID{unsettled}, got,
		"owed work must be resumed even when the full startup pass is declined")
}

// TestStartupFullPassReconcilesSettled is the other half: the knob's actual job is
// the *settled* objects, which no owed-work listing can see. That pass is what
// re-confirms process-scoped state a restart invalidated.
func TestStartupFullPassReconcilesSettled(t *testing.T) {
	ctx := context.Background()
	var settled ObjectID
	seed := func(s Store, gk GroupKind) {
		raw, err := s.ObjectsCreate(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte(`{}`)})
		require.NoError(t, err)
		err = s.ObjectsUpdateStatus(ctx, gk, raw.ID, raw.Generation, []byte(`{}`), 0)
		require.NoError(t, err)
		settled = raw.ID
	}

	t.Run("enabled reconciles it", func(t *testing.T) {
		got := newStartupHarness(t, seed, WithStartupFullPass(true))
		assert.Equal(t, []ObjectID{settled}, got)
	})

	t.Run("disabled leaves it alone", func(t *testing.T) {
		got := newStartupHarness(t, seed, WithStartupFullPass(false))
		assert.Empty(t, got, "a settled object is owed nothing")
	})

	// The default is off, matching WithFullPassInterval. Both full passes scale with
	// the object count, so neither may be something a reconcile depends on — a
	// settled object is owed nothing, and startup owes it nothing back.
	t.Run("defaults to disabled", func(t *testing.T) {
		got := newStartupHarness(t, seed)
		assert.Empty(t, got, "the startup full pass must be opt-in, like the periodic one")
	})
}

// TestDisabledBackstopsAnnounceThemselves pins that turning a periodic driver off
// is visible in the log. Both of these are supported configurations, so neither is
// an error — but the failure mode when one is reached by accident (an unset config
// field, a bad duration parse) is silence: work quietly stops being re-derived and
// nothing says so.
//
// GC is the driver that is *not* here: it cannot be turned off at all, because it
// is the one with no recourse left (see WithGCInterval), so the mistake is reported
// as an error from New rather than a log line nobody reads
// (TestWithGCIntervalRejectsNonPositive).
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

	t.Run("owed pass off is Info: the caller can still requeue", func(t *testing.T) {
		out := start(t, slog.LevelInfo, withOwedPassInterval(0))
		assert.Contains(t, out, "owed pass disabled")
		assert.Contains(t, out, "Requeue", "name the primitive that replaces it")
	})

	t.Run("the defaults say nothing", func(t *testing.T) {
		out := start(t, slog.LevelInfo)
		assert.NotContains(t, out, "disabled",
			"a default configuration must not narrate; full-pass-off is the default and would be noise")
	})
}

// clientOnlyGK is a kind used through Client with no Register: it has no
// reconciler, so nothing in bh.order names it. A depends_on edge may still point
// at one of its objects — configuration, secrets, any "reference data" the
// application writes and controllers read.
var clientOnlyGK = GroupKind{Kind: "Config"}

// newClientOnlyTargetFixture builds the shape the defect lives in: one
// registered kind D, one client-only kind T, an edge D depends_on T, and D
// already settled. Every periodic driver that could paper over a missed wake is
// disabled — no startup full pass, no full-pass tick, and an owed-pass interval far
// beyond the test — so the only thing that can requeue D is the dependency
// waker. The GC sweeper's interval cannot be disabled, so it is set long enough
// to never fire on its own; tests that need a sweep drive it directly.
func newClientOnlyTargetFixture(t *testing.T) (*Beehive, Store, chan *Object[tSpec, tStatus], func()) {
	t.Helper()
	ctx := context.Background()
	store := &seedProbeStore{Store: newClientTestStore(t), seeded: make(chan struct{}, 8)}

	// The dependency waker is the only driver under test here, so it runs fast
	// while everything else is pushed out of the way.
	bh, err := New(store, WithGCInterval(time.Hour), withDependencyWakeInterval(fastTick))
	require.NoError(t, err)
	reconciled := make(chan *Object[tSpec, tStatus], 16)
	_, err = Register(bh, GroupKind{Kind: "Widget"}, &reconcileCapture{ch: reconciled},
		WithFullPassInterval(0),
		withOwedPassInterval(time.Hour),
		WithStartupFullPass(false))
	require.NoError(t, err)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)

	// Wait for the waker to take its cursor before returning. It seeds from the
	// store's current version, so a write made before that read is *below* the
	// watermark and is never scanned — and with the startup pass disabled here,
	// nothing else would find it either. Waiting makes "the waker was watching"
	// a fact rather than a bet on goroutine scheduling.
	waitClosed(t, chanAfter(store.seeded, 1), "the waker to seed its watermark")
	return bh, store, reconciled, func() { _ = stop(ctx) }
}

// seedProbeStore signals when the waker reads the store-wide cursor. In this
// fixture nothing else calls it: there are no client watches, and the reconcile
// drivers list objects rather than versions.
type seedProbeStore struct {
	Store
	seeded chan struct{}
}

func (s *seedProbeStore) ObjectWritesMaxVersion(ctx context.Context) (int64, error) {
	at, err := s.Store.ObjectWritesMaxVersion(ctx)
	probeSignal(s.seeded)
	return at, err
}

// settleFirstPass drives the one reconcile these fixtures cannot get any other
// way. They disable every periodic driver but the waker, so nothing lists a newly
// created object: the startup owed pass ran before it existed, and no write
// schedules a pass. Requeue is the supported way to reconcile by hand in exactly that
// configuration, and using it here is what keeps the baseline from being a race
// with the reconciler goroutine's own startup.
func settleFirstPass(t *testing.T, client Client[tSpec, tStatus], ch chan *Object[tSpec, tStatus], id ObjectID) {
	t.Helper()
	require.NoError(t, client.Requeue(context.Background(), id))
	awaitReconcile(t, ch, id, "the dependent's requeued first pass did not run")
}

// awaitReconcile waits for a reconcile of id, ignoring any others.
func awaitReconcile(t *testing.T, ch chan *Object[tSpec, tStatus], id ObjectID, msg string) {
	t.Helper()
	awaitMatch(t, ch, func(obj *Object[tSpec, tStatus]) bool { return obj.ID == id }, msg)
}

// TestClientOnlyTargetWakesDependent is the defect: a depends_on edge may point
// at an object of a kind with no controller, and a per-registered-kind waker
// never observes it. Not a dropped wake — none is ever attempted, so no amount
// of healthy operation repairs it. With every periodic driver disabled, the
// dependent must still be requeued when its client-only target changes.
func TestClientOnlyTargetWakesDependent(t *testing.T) {
	ctx := context.Background()
	bh, store, reconciled, stop := newClientOnlyTargetFixture(t)
	defer stop()

	depClient := NewClient[tSpec, tStatus](bh, GroupKind{Kind: "Widget"})
	dep := mustCreate(t, ctx, depClient, uniqueName(), tSpec{})
	target := mustCreate(t, ctx, NewClient[tSpec, tStatus](bh, clientOnlyGK), "target-a", tSpec{})
	settleFirstPass(t, depClient, reconciled, dep.ID)
	require.NoError(t, addEdge(ctx, store, dep.ID, target.ID, RelationDependsOn))

	err := store.ConditionsSet(ctx, clientOnlyGK, target.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	awaitReconcile(t, reconciled, dep.ID,
		"the dependent was never woken: its target's kind has no controller, so no waker observed the change")
}

// TestClientOnlyTargetCreatedAfterStart is the same defect for a target whose
// kind has no objects at all when Start runs. It is the discriminating case
// against subscribing per kind *present in the store at Start*: that option
// passes the test above and fails this one, which is an ordinary shape for a
// client-only kind.
func TestClientOnlyTargetCreatedAfterStart(t *testing.T) {
	ctx := context.Background()
	bh, store, reconciled, stop := newClientOnlyTargetFixture(t)
	defer stop()

	depClient := NewClient[tSpec, tStatus](bh, GroupKind{Kind: "Widget"})
	dep := mustCreate(t, ctx, depClient, uniqueName(), tSpec{})
	settleFirstPass(t, depClient, reconciled, dep.ID)

	// The kind's first object is born after Start, so nothing observable at
	// subscribe time could have named it.
	target := mustCreate(t, ctx, NewClient[tSpec, tStatus](bh, clientOnlyGK), "target-b", tSpec{})
	require.NoError(t, addEdge(ctx, store, dep.ID, target.ID, RelationDependsOn))

	err := store.ConditionsSet(ctx, clientOnlyGK, target.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	awaitReconcile(t, reconciled, dep.ID,
		"the dependent was never woken for a target kind whose first object appeared after Start")
}

// TestClientOnlyTargetDeletionUnwedges is the unrecoverable half of the defect.
// edges.to_id is ON DELETE RESTRICT, so a target with dependents cannot be
// physically removed: Delete sets the tombstone and emits Modified, and only the
// dependents' own reconciles can drop the edge that blocks collection. With no
// waker for the target's kind that Modified reaches nobody, so the row stays
// deletion-pending and the GC sweeper retries it forever with no way to
// progress — and unlike the case above, no configuration recovers it inside a
// running process.
func TestClientOnlyTargetDeletionUnwedges(t *testing.T) {
	ctx := context.Background()
	bh, store, reconciled, stop := newClientOnlyTargetFixture(t)
	defer stop()

	widget := GroupKind{Kind: "Widget"}
	depClient := NewClient[tSpec, tStatus](bh, widget)
	dep := mustCreate(t, ctx, depClient, uniqueName(), tSpec{})
	targetClient := NewClient[tSpec, tStatus](bh, clientOnlyGK)
	target := mustCreate(t, ctx, targetClient, uniqueName(), tSpec{})
	settleFirstPass(t, depClient, reconciled, dep.ID)
	require.NoError(t, addEdge(ctx, store, dep.ID, target.ID, RelationDependsOn))

	require.NoError(t, targetClient.DeleteByID(ctx, target.ID))
	awaitReconcile(t, reconciled, dep.ID,
		"the dependent was never woken by its target's tombstone, so nothing can drop the edge that RESTRICT-blocks collection")

	// The wake is only half the story: with the edge dropped, the target must
	// actually collect rather than stay deletion-pending forever.
	require.NoError(t, store.EdgesDelete(ctx, dep.ID, target.ID, RelationDependsOn))
	_, err := bh.gcCollect(ctx, target.ID)
	require.NoError(t, err)
	_, err = store.ObjectsGet(ctx, target.ID)
	assert.ErrorIs(t, err, ErrNotFound, "the target collects once its last dependent edge is gone")
}

// watermarkHarness is a typedController over a real store with one dependent and
// one target, driven synchronously so the watermark write has run by the time
// reconcile returns. wrap, if non-nil, decorates the store the controller writes
// through; the staleness query always reads the real store underneath it.
//
// Staleness is the observable rather than the table itself: what the watermark is
// *for* is whether the stale-dependents pass would find this object again.
type watermarkHarness struct {
	tc     *typedController[cSpec, cStatus]
	inner  *funcController
	store  Store
	dep    ObjectID
	target ObjectID
}

func newWatermarkHarness(t *testing.T, wrap func(Store) Store) *watermarkHarness {
	t.Helper()
	ctx := context.Background()
	s := newClientTestStore(t)

	specJSON, err := json.Marshal(cSpec{})
	require.NoError(t, err)
	target, err := s.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)
	dep, err := s.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)
	require.NoError(t, addEdge(ctx, s, dep.ID, target.ID, RelationDependsOn))

	tc, inner := newSyncController(wrapStore(s, wrap))
	return &watermarkHarness{tc: tc, inner: inner, store: s, dep: dep.ID, target: target.ID}
}

// stale is what the stale-dependents pass would enqueue right now.
func (h *watermarkHarness) stale(t *testing.T) []ObjectID {
	t.Helper()
	refs, err := h.store.DependentsListStale(context.Background(), []GroupKind{clientTestGK}, 0, 100)
	require.NoError(t, err)
	return objectRefIDs(refs)
}

// touchTarget writes the target's spec, so it moves above any watermark recorded
// before now.
func (h *watermarkHarness) touchTarget(t *testing.T, spec string) {
	t.Helper()
	_, _, err := h.store.ObjectsUpdateSpec(context.Background(), clientTestGK, h.target, []byte(spec), 0)
	require.NoError(t, err)
}

// TestReconcileRecordsDependencyWatermark pins the write: a dependent is stale
// until a pass records the cursor it reconciled against, and settles once one
// does.
func TestReconcileRecordsDependencyWatermark(t *testing.T) {
	ctx := context.Background()
	h := newWatermarkHarness(t, nil)
	h.inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) (Result, error) {
		return Result{}, nil
	}
	require.Equal(t, []ObjectID{h.dep}, h.stale(t), "a dependent that never reconciled is stale")

	_, err := h.tc.reconcile(ctx, h.dep)
	require.NoError(t, err)
	assert.Empty(t, h.stale(t), "the pass recorded what it reconciled against")

	h.touchTarget(t, `{"val":"moved"}`)
	assert.Equal(t, []ObjectID{h.dep}, h.stale(t), "and the next change makes it stale again")
}

// A controller that declares a *new* dependency mid-pass costs itself nothing. The
// declare clears the dependent's watermark — which is what makes a third party's
// declare leave the dependent stale, since the new target may sit below a watermark
// the stale scan would otherwise report as converged — but this pass's own write
// lands after it, from the cursor it loaded at. Without that ordering every first
// declare would buy a spurious extra pass, forever, for every controller that
// declares its edges from inside Reconcile.
func TestReconcileRecordsDependencyWatermarkAfterDeclaringANewEdge(t *testing.T) {
	ctx := context.Background()
	h := newWatermarkHarness(t, nil)
	h.inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) (Result, error) {
		return Result{}, nil
	}
	_, err := h.tc.reconcile(ctx, h.dep)
	require.NoError(t, err)
	require.Empty(t, h.stale(t), "settled, with a watermark for the declare below to clear")

	specJSON, err := json.Marshal(cSpec{})
	require.NoError(t, err)
	second, err := h.store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)
	h.inner.fn = func(ctx context.Context, cc ControllerClient[cStatus], _ *Object[cSpec, cStatus]) (Result, error) {
		return Result{}, cc.DependenciesAdd(ctx, h.dep, second.ID)
	}

	_, err = h.tc.reconcile(ctx, h.dep)
	require.NoError(t, err)
	assert.Empty(t, h.stale(t), "the pass that declared the edge also observed the target")
}

// TestReconcileMidPassDeclareLeavesTheDependentOwed pins the close of the last
// strand in "a wake lost by any means costs latency, never divergence": a third
// party declaring a new dependency for an object *while that object's own pass is
// in flight*, against a target that never moves again. The declare clears the
// watermark, but this pass — which never read the new target — rewrites it on
// success from its load cursor, so the stale scan reads converged; the waker sees
// nothing either, since the target never moves. What survives is the declare's
// reconcile_owed stamp: it landed above the count the pass observed at load, so
// the load-scoped decrement cannot consume it, and the owed pass delivers the
// reconcile that actually reads the new target.
func TestReconcileMidPassDeclareLeavesTheDependentOwed(t *testing.T) {
	ctx := context.Background()
	h := newWatermarkHarness(t, nil)
	h.inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) (Result, error) {
		return Result{}, nil
	}
	_, err := h.tc.reconcile(ctx, h.dep)
	require.NoError(t, err)
	require.Empty(t, h.stale(t), "settled, with a watermark for the mid-pass declare to clear")

	// A quiet target, created before the pass loads so the pass's cursor covers its
	// version — the shape that makes the rewritten watermark read as converged.
	specJSON, err := json.Marshal(cSpec{})
	require.NoError(t, err)
	quiet, err := h.store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)

	// The third party declares from outside the pass's client, mid-flight. The
	// target never moves, so only the edge-new stamp can carry this wake.
	h.inner.fn = func(ctx context.Context, _ ControllerClient[cStatus], _ *Object[cSpec, cStatus]) (Result, error) {
		_, err := h.store.EdgesAdd(ctx, h.dep, quiet.ID, RelationDependsOn)
		return Result{}, err
	}
	_, err = h.tc.reconcile(ctx, h.dep)
	require.NoError(t, err)

	// The derived state really is blind here — that blindness was the strand.
	assert.Empty(t, h.stale(t), "the pass rewrote the watermark from a cursor that never saw the new target")

	// The durable record is not: the stamp survived the pass's decrement, so the
	// owed pass still delivers the reconcile that reads the new target.
	got, err := h.store.ObjectsGet(ctx, h.dep)
	require.NoError(t, err)
	assert.Equal(t, int64(1), got.ReconcileOwed, "the mid-pass stamp outlives the load-scoped decrement")
	owed, err := h.store.ReconcileOwedListIDs(ctx, clientTestGK)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{h.dep}, owed, "and the owed listing names the dependent")
}

// The one case that does cost a pass: an object whose *first* depends_on edge is
// declared mid-reconcile. HasDependencies was sampled false at load, so the pass
// skips DependencyWatermarksSet entirely and leaves no row — and an absent row means
// stale. It is the over-reconcile direction, self-extinguishing after one pass, and
// bounded at once per object ever; the alternative is issuing the write on every
// successful reconcile of every kind, which is the write-lock acquisition
// HasDependencies exists to avoid.
func TestReconcileSkipsTheWatermarkWhenTheFirstDependencyIsDeclaredMidPass(t *testing.T) {
	ctx := context.Background()
	s := newClientTestStore(t)
	specJSON, err := json.Marshal(cSpec{})
	require.NoError(t, err)
	dep, err := s.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)
	target, err := s.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)
	tc, inner := newSyncController(s)
	stale := func() []ObjectID {
		refs, err := s.DependentsListStale(ctx, []GroupKind{clientTestGK}, 0, 100)
		require.NoError(t, err)
		return objectRefIDs(refs)
	}

	inner.fn = func(ctx context.Context, cc ControllerClient[cStatus], _ *Object[cSpec, cStatus]) (Result, error) {
		return Result{}, cc.DependenciesAdd(ctx, dep.ID, target.ID)
	}
	_, err = tc.reconcile(ctx, dep.ID)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{dep.ID}, stale(), "no watermark was written, so one more pass is owed")

	// And it settles on that pass, which now loads with the edge in place.
	inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) (Result, error) {
		return Result{}, nil
	}
	_, err = tc.reconcile(ctx, dep.ID)
	require.NoError(t, err)
	assert.Empty(t, stale(), "self-extinguishing: once per object, never repeated")
}

// watermarkProbeStore records every dependency-watermark write, so a test can
// assert on the call rather than on the row. The point of the skip is the write
// lock not taken — on a pool of one connection an INSERT that writes no rows still
// serialises against every other statement — which a row-count assertion could
// not catch.
type watermarkProbeStore struct {
	Store
	sets []ObjectID
	err  error
}

func (s *watermarkProbeStore) DependencyWatermarksSet(ctx context.Context, id ObjectID, cursor int64) error {
	s.sets = append(s.sets, id)
	if s.err != nil {
		return s.err
	}
	return s.Store.DependencyWatermarksSet(ctx, id, cursor)
}

// TestReconcileSkipsDependencyWatermarkWithoutDependencies pins the skip: an
// object with no depends_on edge can never be found stale, so its pass must not
// reach the store at all.
func TestReconcileSkipsDependencyWatermarkWithoutDependencies(t *testing.T) {
	ctx := context.Background()
	var probe *watermarkProbeStore
	h := newWatermarkHarness(t, func(s Store) Store {
		probe = &watermarkProbeStore{Store: s}
		return probe
	})
	h.inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) (Result, error) {
		return Result{}, nil
	}

	_, err := h.tc.reconcile(ctx, h.target)
	require.NoError(t, err)
	assert.Empty(t, probe.sets, "an object with no dependencies never takes the write lock")

	_, err = h.tc.reconcile(ctx, h.dep)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{h.dep}, probe.sets, "a dependent does record one")
}

// TestReconcileRecordsCursorFromTheLoad pins where the cursor comes from. A
// target that moves *during* the pass was not observed by it, so the dependent
// must stay stale: recording a cursor sampled after the controller's reads would
// land above a change the pass never saw, leaving the dependent stranded with
// nothing left to find it. Erring the other way costs one extra pass.
func TestReconcileRecordsCursorFromTheLoad(t *testing.T) {
	ctx := context.Background()
	h := newWatermarkHarness(t, nil)
	h.inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) (Result, error) {
		h.touchTarget(t, `{"val":"moved mid-pass"}`)
		return Result{}, nil
	}

	_, err := h.tc.reconcile(ctx, h.dep)
	require.NoError(t, err)

	assert.Equal(t, []ObjectID{h.dep}, h.stale(t),
		"a change the pass could not have observed leaves the dependent owed another")
}

// TestReconcileHoldsDependencyWatermarkOnFailure pins the self-healing property
// the whole design rests on: a failed pass records nothing, so the object stays
// stale and is found again with no retry bookkeeping of its own.
func TestReconcileHoldsDependencyWatermarkOnFailure(t *testing.T) {
	ctx := context.Background()
	h := newWatermarkHarness(t, nil)
	h.inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) (Result, error) {
		return Result{}, errBoom
	}

	_, err := h.tc.reconcile(ctx, h.dep)
	require.ErrorIs(t, err, errBoom)

	assert.Equal(t, []ObjectID{h.dep}, h.stale(t), "a failed pass leaves the dependent owed one")
}

// TestReconcileHoldsDependencyWatermarkOnUndecodableRow pins the quarantine's
// half of that, mirroring the reconcile_owed assertion beside it: the controller
// never saw the object, so recording a watermark would silently mark a poison row
// as converged against its dependencies — exactly the discard the quarantine
// exists to avoid.
func TestReconcileHoldsDependencyWatermarkOnUndecodableRow(t *testing.T) {
	ctx := context.Background()
	var probe *watermarkProbeStore
	h := newWatermarkHarness(t, func(s Store) Store {
		probe = &watermarkProbeStore{Store: s}
		return probe
	})
	// A valid create always decodes, so the poison bytes go in directly.
	_, _, err := h.store.ObjectsUpdateSpec(ctx, clientTestGK, h.dep, []byte("not-json"), 0)
	require.NoError(t, err)

	_, err = h.tc.reconcile(ctx, h.dep)
	require.NoError(t, err, "an undecodable row is still a no-op success")

	assert.Empty(t, probe.sets, "a pass that never ran records nothing")
	assert.Equal(t, []ObjectID{h.dep}, h.stale(t))
}

// TestReconcileWarnsAndContinuesOnCursorWriteFailure pins the failure contract:
// no error escapes into the backoff ladder over a bookkeeping write that the next
// stale pass re-derives anyway.
func TestReconcileWarnsAndContinuesOnCursorWriteFailure(t *testing.T) {
	ctx := context.Background()
	h := newWatermarkHarness(t, func(s Store) Store {
		return &watermarkProbeStore{Store: s, err: errBoom}
	})
	logger, logs := captureLogger(slog.LevelWarn)
	h.tc.logger = logger
	h.inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) (Result, error) {
		return Result{}, nil
	}

	_, err := h.tc.reconcile(ctx, h.dep)

	require.NoError(t, err, "a failed watermark write must not fail the reconcile")
	assert.Contains(t, logs.String(), "failed to record the dependency watermark")
	assert.Equal(t, []ObjectID{h.dep}, h.stale(t), "and the dependent is still found by the next pass")
}

// TestDependencyWakeSurvivesRestart is the mechanism's reason for existing, end
// to end. A dependent settles against a target, the process stops, and only then
// does the target change — so the wake is owed to a process that no longer exists
// and was never recorded anywhere: the declare-time stamp was drained by the pass
// that settled the dependent, and its own generation never moved (no owed-work
// listing can name it).
//
// The restart runs with the waker off and both full passes off, which is
// load-bearing. The waker cannot help — the change is below any watermark it
// would seed — and a full pass would heal this for reasons that have nothing to
// do with dependencies, so the test would prove nothing. What is left is the
// stale-dependents pass, re-deriving the wake from the durable watermark.
func TestDependencyWakeSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	// One store, two control planes: the rows outlive the process, everything
	// in-memory does not. Owned by the test, since stop leaves the store open.
	db, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	gk := GroupKind{Kind: "Widget"}

	// --- first process: settle the dependent on a not-Ready target ---
	bh1, err := New(db)
	require.NoError(t, err)
	ctrl1 := &dependentController{observed: make(chan bool, 8)}
	cc, err := Register(bh1, gk, ctrl1, WithFullPassInterval(0))
	require.NoError(t, err)

	client1 := NewClient[tSpec, tStatus](bh1, gk)
	ctrl1.client = client1

	target := mustCreate(t, ctx, client1, uniqueName(), tSpec{})
	dep := mustCreate(t, ctx, client1, uniqueName(), tSpec{})
	ctrl1.targetID, ctrl1.depID = target.ID, dep.ID
	// Declared before the change. The declare stamps one owed pass, but the
	// startup reconcile below services and drains it — so by the time the target
	// moves, nothing durable records that a wake is owed: this is the ordinary
	// settled dependency, not the declare-time case the stamp covers.
	require.NoError(t, cc.DependenciesAdd(ctx, dep.ID, target.ID))

	stop1, err := bh1.Start(ctx)
	require.NoError(t, err)
	select {
	case ready := <-ctrl1.observed:
		require.False(t, ready, "the startup pass reads the target before it goes Ready")
	case <-time.After(testTimeout):
		t.Fatal("dependent's startup reconcile did not run")
	}
	require.NoError(t, stop1(ctx))

	// --- the crash window: the target changes with nobody running ---
	err = db.ConditionsSet(ctx, gk, target.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	// --- second process over the same store ---
	bh2, err := New(db, withDependencyWakeInterval(0))
	require.NoError(t, err)
	ctrl2 := &dependentController{
		observed: make(chan bool, 8),
		depID:    dep.ID,
		targetID: target.ID,
	}
	_, err = Register(bh2, gk, ctrl2, WithFullPassInterval(0), WithStartupFullPass(false))
	require.NoError(t, err)
	ctrl2.client = NewClient[tSpec, tStatus](bh2, gk)

	stop2, err := bh2.Start(ctx)
	require.NoError(t, err)
	defer stop2(ctx)

	select {
	case ready := <-ctrl2.observed:
		assert.True(t, ready, "the re-derived pass observes the target's change")
	case <-time.After(testTimeout):
		t.Fatal("dependent was never reconciled after restart: the wake died with the process that owed it")
	}
}
