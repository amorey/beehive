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
	"log/slog"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amorey/beehive/internal/storeapi"
	"github.com/amorey/beehive/sqlite"
	"github.com/amorey/gobus/watch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unsettledIDsStore is a fakeStore whose ObjectsListUnsettledIDs returns a fixed slice
// of IDs, used to exercise enqueueUnsettled without a real SQLite database.
type unsettledIDsStore struct {
	fakeStore
	ids []ObjectID
}

func (s *unsettledIDsStore) Objects() storeapi.Objects {
	return objectsOverride{Objects: s.fakeStore.Objects(), listUnsettledIDs: s.listUnsettledIDsObjects}
}

func (s *unsettledIDsStore) listUnsettledIDsObjects(_ context.Context, _ GroupKind) ([]ObjectID, error) {
	return s.ids, nil
}

// reconcileOwedIDsStore is a fakeStore whose ReconcileOwedListIDs returns a fixed
// slice, used to exercise the durable-wake backstop enqueue without a real
// database — the sibling of unsettledIDsStore.
type reconcileOwedIDsStore struct {
	fakeStore
	ids []ObjectID
}

func (s *reconcileOwedIDsStore) ReconcileOwed() storeapi.ReconcileOwed {
	return owedOverride{ReconcileOwed: s.fakeStore.ReconcileOwed(), listIDs: s.listIDs}
}

func (s *reconcileOwedIDsStore) listIDs(context.Context, GroupKind) ([]ObjectID, error) {
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

func (s *tickOnlyReconcileOwedStore) ReconcileOwed() storeapi.ReconcileOwed {
	return owedOverride{ReconcileOwed: s.fakeStore.ReconcileOwed(), listIDs: s.listIDs}
}

func (s *tickOnlyReconcileOwedStore) listIDs(context.Context, GroupKind) ([]ObjectID, error) {
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
	// failures fails that many ListIDs calls before serving ids, for a listing
	// whose only recovery is its own retry.
	failures int
	// listed, when set, is closed on the first call, so a test can wait for the
	// listing rather than for a delay.
	listed chan struct{}

	mu    sync.Mutex
	calls int
}

func (s *allIDsStore) Objects() storeapi.Objects {
	return objectsOverride{Objects: s.fakeStore.Objects(), listUnsettledIDs: s.listUnsettledIDsObjects, listIDs: s.listIDsObjects}
}

func (s *allIDsStore) listCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *allIDsStore) listUnsettledIDsObjects(_ context.Context, _ GroupKind) ([]ObjectID, error) {
	return nil, nil
}
func (s *allIDsStore) listIDsObjects(_ context.Context, _ GroupKind) ([]ObjectID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls == 1 && s.listed != nil {
		close(s.listed)
	}
	if s.calls <= s.failures {
		return nil, errors.New("list failed")
	}
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

// alarmFor reads an id's pending alarm under the queue's lock: a timer firing
// concurrently writes the same map.
func alarmFor(q *workQueue, id ObjectID) *alarm {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.gauge.alarmFor(id)
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
	reconcileFn func(ctx context.Context, id ObjectID) ReconcileResult
	gone        bool // reported for every id, as a collect would
}

func (f *fakeAdapter) reconcile(ctx context.Context, id ObjectID) (ReconcileResult, bool) {
	return f.reconcileFn(ctx, id), f.gone
}

func TestReconcilerRequeuesOnError(t *testing.T) {
	calls := 0
	doneCh := make(chan struct{})
	adapter := &fakeAdapter{
		reconcileFn: func(_ context.Context, _ ObjectID) ReconcileResult {
			calls++
			if calls == 1 {
				return Fail(errors.New("transient"))
			}
			close(doneCh)
			return Settled()
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

// A push landing mid-pass — the cleared finalizer's own — leaves the id dirty
// while it is in flight. When that pass collected the row, the worker drops it
// rather than paying a dispatch that can only read ErrNotFound. The second
// enqueue is the barrier: one worker dispatches in order, so a re-queued 1 would
// have been seen before 2.
func TestReconcilerDropsAWakeForACollectedObject(t *testing.T) {
	var seen []ObjectID
	reached2 := make(chan struct{})
	adapter := &fakeAdapter{gone: true}
	r := &reconciler{adapter: adapter, work: newWorkQueue(), backoffFor: make(map[ObjectID]time.Duration)}
	adapter.reconcileFn = func(_ context.Context, id ObjectID) ReconcileResult {
		seen = append(seen, id)
		switch id {
		case 1:
			r.work.add(1) // the push, arriving while 1 is still in flight
		case 2:
			close(reached2)
		}
		return Settled()
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	r.enqueue(1)
	r.enqueue(2)
	waitClosed(t, reached2, "the barrier object reconciled")
	cancel()
	waitClosed(t, done, "run to exit")

	assert.Equal(t, []ObjectID{1, 2}, seen, "the collected id is not dispatched again")
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
		reconcileFn: func(_ context.Context, _ ObjectID) ReconcileResult {
			calls++
			if calls == 1 {
				return Fail(errors.New("transient")) // creates a backoff entry
			}
			// Object is now gone: reconcile reports success (mirrors the
			// ErrNotFound -> nil path), which must clear the backoff entry.
			close(succeeded)
			return Settled()
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
		reconcileFn: func(_ context.Context, _ ObjectID) ReconcileResult {
			calls++
			if calls == 1 {
				return Settled().RequeueAfter(10 * time.Millisecond)
			}
			close(doneCh)
			return Settled()
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

	bh := newTestBeehive(t, store, fast()...)

	gk := GroupKind{Kind: "Widget"}
	reconciled := make(chan *Object[tSpec, tStatus], 16)
	// Full pass disabled so the dependency waker is the only thing that can requeue
	// an already-settled object — no timer noise.
	err = Register(bh, gk, &reconcileCapture{ch: reconciled}, WithFullPassInterval(0))
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
	err = store.Conditions().Set(ctx, GroupKind{Group: target.Group, Kind: target.Kind}, target.ID, storeapi.Condition{Type: "Ready", Status: "True"})
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
// only observes, which is the outside-a-reconcile spelling — there the declaration is the
// embedding application's, not a reconcile's.
type dependentController struct {
	client   Client[tSpec, tStatus]
	depID    ObjectID
	targetID ObjectID

	observed  chan bool // the target's Ready condition as each dep pass saw it
	afterRead func(ctx context.Context, cc ControllerClient[tStatus], target *Object[tSpec, tStatus]) error
}

func (c *dependentController) Reconcile(ctx context.Context, cc ControllerClient[tStatus], obj *Object[tSpec, tStatus]) ReconcileResult {
	if obj.ID != c.depID {
		return Settled() // the target's own reconcile is not under test
	}
	target, err := c.client.Get(ctx, c.targetID)
	if err != nil {
		return Fail(err)
	}
	ready := false
	for _, cond := range target.Conditions {
		if cond.Type == "Ready" {
			ready = cond.Status == ConditionTrue
		}
	}
	if c.afterRead != nil {
		if err := c.afterRead(ctx, cc, target); err != nil {
			return Fail(err)
		}
	}
	// Settling at obj.Generation is what hides a missed wake from the full pass
	// backstop: ObjectsListUnsettledIDs sees a converged object.
	if err := cc.UpdateStatus(ctx, tStatus{}); err != nil {
		return Fail(err)
	}
	c.observed <- ready
	return Settled()
}

// TestDependencyRequeueRaceOnDeclare pins the read-then-declare race: a change to
// the target that lands after the dependent read it but before AddDependency
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

	bh := newTestBeehive(t, store, fast()...)

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
		return cc.AddDependency(ctx, ctrl.targetID)
	}
	// Full pass disabled so the dependency waker is the only thing that can requeue
	// the dependent — the backstop must not paper over the miss.
	cc := registerWithClient(t, bh, gk, ctrl, WithFullPassInterval(0))

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
	require.NoError(t, cc.at(target.ID).SetCondition(ctx, Condition{Type: "Ready", Status: ConditionTrue}))
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

// TestDependencyRequeueRaceOnDeclareOutsideReconcile is the mirror of
// TestDependencyRequeueRaceOnDeclare: the same read-then-declare window, but with
// the two halves in different goroutines. The embedding application declares the
// edge through the ControllerClient Register handed it, after its own read of the
// target — so no reconcile is in flight to carry the miss, and the hole is a
// notch wider than the in-band one. In-band, the pass that loses the change at
// least runs to completion around the declaration; here the declaration is the
// only thing that happens, and AddDependency enqueues nothing: the edge appears
// with fromID already settled, so a change that landed before the commit reaches
// nobody and nothing re-derives it. With the full pass disabled the dependent holds a
// stale read forever, with no error, no condition and no log line.
func TestDependencyRequeueRaceOnDeclareOutsideReconcile(t *testing.T) {
	ctx := context.Background()
	db, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	store := &wakeProbeStore{Store: db, looked: make(chan struct{}, 8)}

	// The stale-dependents pass cannot be disabled, and its first sweep of a fresh
	// process scans from 0 — so it can re-derive this dependent's staleness and
	// close the gap for a reason other than the one under test. Pushing it past the
	// test's own timeout leaves the EdgesAdd stamp as the only thing that can
	// requeue the dependent, which is what this is about.
	bh := newTestBeehive(t, store, fast(WithStaleDependentsInterval(time.Hour))...)

	gk := GroupKind{Kind: "Widget"}
	ctrl := &dependentController{observed: make(chan bool, 8)}
	// Full pass disabled so the dependency waker is the only thing that can requeue
	// the dependent — the backstop must not paper over the miss.
	cc := registerWithClient(t, bh, gk, ctrl, WithFullPassInterval(0))

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
	// outside-a-reconcile spelling of read-then-declare. Waiting for the waker's lookup
	// makes the window deterministic: with no edge yet it comes back empty, so the
	// change is already unclaimed by the time AddDependency commits.
	store.resetLooked()
	require.NoError(t, cc.at(target.ID).SetCondition(ctx, Condition{Type: "Ready", Status: ConditionTrue}))
	store.waitLooked(t)
	// target is the application's read of the target, taken before the change
	// above — so the version it carries is the one the decision to depend was
	// based on, and the target has since moved past it.
	require.NoError(t, cc.at(dep.ID).AddDependency(ctx, target.ID))

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
// at that point the outside-a-reconcile repro passes while this one still fails, and the
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
	cc := registerWithClient(t, bh1, gk, ctrl1, WithFullPassInterval(0))

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
	// already unclaimed when the edge appears — the outside-a-reconcile race. The
	// ControllerClient outlives the control plane (it holds the store, not the
	// loops), so the declaration commits normally with no running queue to reach.
	err = db.Conditions().Set(ctx, gk, target.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	require.NoError(t, cc.at(dep.ID).AddDependency(ctx, target.ID))

	// --- the restart: a second process, the first already stopped ---
	bh2, err := New(db)
	require.NoError(t, err)
	ctrl2 := &dependentController{
		observed: make(chan bool, 8),
		depID:    dep.ID,
		targetID: target.ID,
	}
	err = Register(bh2, gk, ctrl2,
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
	bh := newTestBeehive(t, store, fast()...)

	gk := GroupKind{Kind: "Widget"}
	reconciled := make(chan ObjectID, 8)
	// Full pass off: an arriving pass must be the write's own wake, not a tick.
	err := Register(bh, gk, &idCapture{ch: reconciled},
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
	_, err = client.Update(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)

	assert.Equal(t, obj.ID, recv(t, reconciled), "a spec write wakes it without the self-edge")
}

// idCapture reports the id of each object it reconciles. It is cSpec-typed
// because tSpec is empty, which would make every Update a byte-identical no-op.
type idCapture struct{ ch chan ObjectID }

func (c *idCapture) Reconcile(_ context.Context, _ ControllerClient[cStatus], obj *Object[cSpec, cStatus]) ReconcileResult {
	c.ch <- obj.ID
	return Settled()
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
		reconcileFn: func(_ context.Context, id ObjectID) ReconcileResult {
			select {
			case reconciled <- id:
			default:
			}
			return Settled()
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

// Reports the object still unconverged, far enough out that only the test's own
// requeue dispatches it again: these tests drive the unsettled listing, which a
// Settled pass would empty.
func (c *recordingController) Reconcile(_ context.Context, _ ControllerClient[tStatus], obj *Object[tSpec, tStatus]) ReconcileResult {
	select {
	case c.reconciled <- obj.ID:
	default:
	}
	return Unsettled().RequeueAfter(time.Hour)
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

	raw, err := store.Objects().Create(ctx, gk, ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	bh := newTestBeehive(t, store, withOwedPassInterval(0), WithFullPassInterval(0))
	ctrl := &recordingController{reconciled: make(chan ObjectID, 4)}
	err = Register(bh, gk, ctrl, WithStartupFullPass(false))
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
	ids, err := store.Objects().ListUnsettledIDs(ctx, gk)
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
		reconcileFn: func(_ context.Context, id ObjectID) ReconcileResult {
			select {
			case reconciled <- id:
			default:
			}
			return Settled()
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

func (s *errReconcileOwedStore) ReconcileOwed() storeapi.ReconcileOwed {
	return owedOverride{ReconcileOwed: s.fakeStore.ReconcileOwed(), listIDs: s.listIDs}
}

func (s *errReconcileOwedStore) listIDs(context.Context, GroupKind) ([]ObjectID, error) {
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
		reconcileFn: func(_ context.Context, id ObjectID) ReconcileResult {
			select {
			case reconciled <- id:
			default:
			}
			return Settled()
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
		reconcileFn: func(_ context.Context, _ ObjectID) ReconcileResult {
			started.fire()
			<-block
			return Settled()
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
		reconcileFn: func(_ context.Context, _ ObjectID) ReconcileResult {
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
			return Settled()
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
		reconcileFn: func(_ context.Context, _ ObjectID) ReconcileResult {
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
			return Settled()
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

func (s *listCallStore) Objects() storeapi.Objects {
	return objectsOverride{Objects: s.fakeStore.Objects(), listUnsettledIDs: s.listUnsettledIDsObjects}
}

func (s *listCallStore) listUnsettledIDsObjects(_ context.Context, _ GroupKind) ([]ObjectID, error) {
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
	transitioned := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	updated := transitioned.Add(time.Minute)
	raw := &RawObject{Spec: specJSON, Conditions: []storeapi.Condition{
		{Type: "Ready", Status: "True", Reason: "Up", Message: "ok", Liveness: true,
			TransitionedAt: transitioned, UpdatedAt: updated},
		{Type: "Healthy", Status: "False"},
		{Type: "Connected", Status: "Unknown", Reason: "Dialed", Liveness: true, Unconfirmed: true},
	}}

	obj, err := rawToTyped[tSpec, tStatus](raw, nil)
	require.NoError(t, err)
	require.Len(t, obj.Conditions, 3)
	assert.Equal(t, "Ready", obj.Conditions[0].Type)
	assert.Equal(t, ConditionTrue, obj.Conditions[0].Status)
	assert.Equal(t, "Up", obj.Conditions[0].Reason)
	assert.Equal(t, "ok", obj.Conditions[0].Message)
	assert.True(t, obj.Conditions[0].Liveness)
	assert.Equal(t, transitioned, obj.Conditions[0].TransitionedAt)
	assert.Equal(t, updated, obj.Conditions[0].UpdatedAt)
	assert.False(t, obj.Conditions[0].Unconfirmed)
	assert.Equal(t, ConditionFalse, obj.Conditions[1].Status)
	assert.True(t, obj.Conditions[2].Unconfirmed, "the downgrade flag survives the mapping")
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

func (s *getObjectBadSpecStore) Objects() storeapi.Objects {
	return objectsOverride{Objects: s.fakeStore.Objects(), get: s.getObjects, getForReconcile: s.getForReconcileObjects}
}

func (s *getObjectBadSpecStore) getObjects(_ context.Context, id ObjectID) (*RawObject, error) {
	return &RawObject{ID: id, Kind: "Widget", Spec: []byte("not-json")}, nil
}

func (s *getObjectBadSpecStore) getForReconcileObjects(ctx context.Context, id ObjectID) (storeapi.ReconcileLoad, error) {
	return reconcileLoadOf(s.Objects().Get(ctx, id))
}

// TestTypedControllerReconcileRawToTypedError pins the quarantine: an undecodable
// row (not deletion-pending) is a no-op success, not a retryable error — returning
// the error would retry the identical bytes forever under backoff, and the full pass
// re-enqueues it regardless. The controller must not run on a row that never
// decoded.
func TestTypedControllerReconcileRawToTypedError(t *testing.T) {
	bh := &Beehive{store: &getObjectBadSpecStore{}}
	var called bool
	inner := &funcController{fn: func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		called = true
		return Settled()
	}}
	tc := &typedController[cSpec, cStatus]{
		gk:    GroupKind{Kind: "Widget"},
		bh:    bh,
		inner: inner,
	}
	res, _, err := reconcilePass(tc, context.Background(), 1)
	require.NoError(t, err, "an undecodable row must not retry forever")
	assert.Equal(t, Settled(), res)
	assert.False(t, called, "Reconcile must not run on a row that failed to decode")
}

// owedBadSpecStore is getObjectBadSpecStore with a wake already owed, and records
// whether the reconcile tried to drain it.
type owedBadSpecStore struct {
	fakeStore
	decremented bool
}

func (s *owedBadSpecStore) Objects() storeapi.Objects {
	return objectsOverride{Objects: s.fakeStore.Objects(), get: s.getObjects, getForReconcile: s.getForReconcileObjects}
}

func (s *owedBadSpecStore) getObjects(_ context.Context, id ObjectID) (*RawObject, error) {
	return &RawObject{ID: id, Kind: "Widget", Spec: []byte("not-json"), ReconcileOwed: 2}, nil
}

func (s *owedBadSpecStore) getForReconcileObjects(ctx context.Context, id ObjectID) (storeapi.ReconcileLoad, error) {
	return reconcileLoadOf(s.Objects().Get(ctx, id))
}

func (s *owedBadSpecStore) ReconcileOwed() storeapi.ReconcileOwed {
	return owedOverride{ReconcileOwed: s.fakeStore.ReconcileOwed(), decrement: s.decrement}
}

func (s *owedBadSpecStore) decrement(context.Context, GroupKind, ObjectID, int64) error {
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
		inner: &funcController{fn: func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
			return Settled()
		}},
	}

	_, _, err := reconcilePass(tc, context.Background(), 1)
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
	bh := newTestBeehive(t, store)
	gk := GroupKind{Kind: "Widget"}

	// Inject an undecodable row directly (a valid create can always decode), then
	// request its deletion so the reconcile sees a deletion-pending poison row.
	raw, err := store.Objects().Create(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte("not-json")})
	require.NoError(t, err)
	_, err = store.DeletionRequests().Create(ctx, gk, raw.ID)
	require.NoError(t, err)

	var called bool
	inner := &funcController{fn: func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		called = true
		return Settled()
	}}
	tc := &typedController[cSpec, cStatus]{gk: gk, bh: bh, inner: inner}

	res, _, err := reconcilePass(tc, ctx, raw.ID)
	require.NoError(t, err)
	assert.Equal(t, Settled(), res)
	assert.False(t, called, "Reconcile must not run on a row that failed to decode")

	_, err = store.Objects().Get(ctx, raw.ID)
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

func (s *undecodableDeletingCollectErrorStore) Objects() storeapi.Objects {
	return objectsOverride{Objects: s.fakeStore.Objects(), get: s.getObjects, getForReconcile: s.getForReconcileObjects, getMeta: s.getMetaObjects}
}

func (s *undecodableDeletingCollectErrorStore) getObjects(_ context.Context, id ObjectID) (*RawObject, error) {
	deletedAt := time.Unix(1, 0)
	return &RawObject{ID: id, Kind: "Widget", Spec: []byte("not-json"), DeletionRequestedAt: &deletedAt}, nil
}

func (s *undecodableDeletingCollectErrorStore) getForReconcileObjects(ctx context.Context, id ObjectID) (storeapi.ReconcileLoad, error) {
	return reconcileLoadOf(s.Objects().Get(ctx, id))
}

func (s *undecodableDeletingCollectErrorStore) getMetaObjects(context.Context, ObjectID) (*RawObject, error) {
	return nil, errBoom
}

func TestTypedControllerReconcileRawToTypedErrorCollectError(t *testing.T) {
	bh := &Beehive{store: &undecodableDeletingCollectErrorStore{}}
	var called bool
	inner := &funcController{fn: func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		called = true
		return Settled()
	}}
	tc := &typedController[cSpec, cStatus]{
		gk:    GroupKind{Kind: "Widget"},
		bh:    bh,
		inner: inner,
	}
	_, _, err := reconcilePass(tc, context.Background(), 1)
	require.ErrorIs(t, err, errBoom, "a failed collect on a poison deleting row must surface for retry")
	assert.False(t, called, "Reconcile must not run on a row that failed to decode")
}

// deletingCollectErrorStore hands back a deletion-pending row that decodes, so
// the reconcile itself runs and the collect that follows it is what fails.
type deletingCollectErrorStore struct {
	fakeStore
}

func (s *deletingCollectErrorStore) Objects() storeapi.Objects {
	return objectsOverride{Objects: s.fakeStore.Objects(), getForReconcile: s.getForReconcileObjects, getMeta: s.getMetaObjects}
}

func (s *deletingCollectErrorStore) getForReconcileObjects(context.Context, ObjectID) (storeapi.ReconcileLoad, error) {
	deletedAt := time.Unix(1, 0)
	return reconcileLoadOf(&RawObject{
		ID: 1, Kind: "Widget", Spec: []byte(`{}`), DeletionRequestedAt: &deletedAt,
	}, nil)
}

func (s *deletingCollectErrorStore) getMetaObjects(context.Context, ObjectID) (*RawObject, error) {
	return nil, errBoom
}

// A collect that fails after a successful reconcile fails the whole pass: the
// committed writes stand, but the retry is the ladder's, so the controller's own
// delay is dropped rather than competing with it.
func TestTypedControllerReconcileCollectErrorAfterASuccessfulPass(t *testing.T) {
	bh := &Beehive{store: &deletingCollectErrorStore{}}
	var called bool
	inner := &funcController{fn: func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		called = true
		return Settled().RequeueAfter(time.Minute)
	}}
	tc := &typedController[cSpec, cStatus]{
		gk:    GroupKind{Kind: "Widget"},
		bh:    bh,
		inner: inner,
	}

	result, _, err := reconcilePass(tc, context.Background(), 1)
	require.ErrorIs(t, err, errBoom, "a failed collect must surface so the pass is retried")
	assert.True(t, called, "the row decoded, so the controller ran; only the collect failed")
	assert.False(t, result.succeeded(), "the pass failed, whatever the controller returned")
	assert.Zero(t, result.requeueAfter, "the backoff ladder schedules the retry, not the controller")
}

// getObjectErrorStore returns an error from ObjectsGet to exercise path A in
// typedController.reconcile (the ObjectsGet error before rawToTyped). Within is
// inherited from fakeStore (inline passthrough).
type getObjectErrorStore struct {
	fakeStore
}

func (s *getObjectErrorStore) Objects() storeapi.Objects {
	return objectsOverride{Objects: s.fakeStore.Objects(), get: s.getObjects, getForReconcile: s.getForReconcileObjects}
}

func (s *getObjectErrorStore) getObjects(_ context.Context, _ ObjectID) (*RawObject, error) {
	return nil, errBoom
}

func (s *getObjectErrorStore) getForReconcileObjects(ctx context.Context, id ObjectID) (storeapi.ReconcileLoad, error) {
	return reconcileLoadOf(s.Objects().Get(ctx, id))
}

func TestTypedControllerReconcileGetObjectError(t *testing.T) {
	bh := &Beehive{store: &getObjectErrorStore{}}
	inner := &noopController[tSpec, tStatus]{}
	tc := &typedController[tSpec, tStatus]{
		gk:    GroupKind{Kind: "Widget"},
		bh:    bh,
		inner: inner,
	}
	_, _, err := reconcilePass(tc, context.Background(), 1)
	require.Error(t, err)
}

// notFoundStore returns ErrNotFound from ObjectsGet, modeling an object that was
// already collected (by a prior pass, a cascade, or the backstop) between its
// enqueue and this reconcile.
type notFoundStore struct {
	fakeStore
}

func (s *notFoundStore) Objects() storeapi.Objects {
	return objectsOverride{Objects: s.fakeStore.Objects(), get: s.getObjects, getForReconcile: s.getForReconcileObjects}
}

func (s *notFoundStore) getObjects(_ context.Context, _ ObjectID) (*RawObject, error) {
	return nil, ErrNotFound
}

func (s *notFoundStore) getForReconcileObjects(ctx context.Context, id ObjectID) (storeapi.ReconcileLoad, error) {
	return reconcileLoadOf(s.Objects().Get(ctx, id))
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
	result, _, err := reconcilePass(tc, context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, Settled(), result, "no requeue for a vanished object")
}

// notFoundReturningController returns ErrNotFound from its own reconcile logic —
// e.g. an AddDependency to a target that was deleted. That is a real failure to
// retry, not the "queued object already gone" no-op.
type notFoundReturningController struct{}

func (notFoundReturningController) Reconcile(context.Context, ControllerClient[tStatus], *Object[tSpec, tStatus]) ReconcileResult {
	return Fail(ErrNotFound)
}

func TestTypedControllerReconcilePropagatesControllerNotFound(t *testing.T) {
	ctx := context.Background()

	s, err := sqlite.OpenMemory()
	require.NoError(t, err)
	defer s.Close()

	specJSON, err := json.Marshal(tSpec{})
	require.NoError(t, err)
	raw, err := s.Objects().Create(ctx, GroupKind{Kind: "Widget"}, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)

	bh := &Beehive{store: s}
	tc := &typedController[tSpec, tStatus]{
		gk:    GroupKind{Kind: "Widget"},
		bh:    bh,
		inner: notFoundReturningController{},
	}
	// The object exists; only the controller returned ErrNotFound. It must surface
	// so the worker retries, not be swallowed as a vanished-object no-op.
	_, _, err = reconcilePass(tc, ctx, raw.ID)
	require.ErrorIs(t, err, ErrNotFound)
}

// requeueController always asks for a periodic requeue, even while its object is
// finalizing — the pattern that would re-schedule a just-collected id.
type requeueController struct{}

func (requeueController) Reconcile(context.Context, ControllerClient[tStatus], *Object[tSpec, tStatus]) ReconcileResult {
	return Settled().RequeueAfter(time.Minute)
}

func TestTypedControllerReconcileDropsRequeueWhenCollected(t *testing.T) {
	ctx := context.Background()

	s, err := sqlite.OpenMemory()
	require.NoError(t, err)
	defer s.Close()

	specJSON, err := json.Marshal(tSpec{})
	require.NoError(t, err)
	raw, err := s.Objects().Create(ctx, GroupKind{Kind: "Widget"}, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)
	_, err = s.DeletionRequests().Create(ctx, GroupKind{Kind: "Widget"}, raw.ID)
	require.NoError(t, err)

	bh := &Beehive{store: s}
	tc := &typedController[tSpec, tStatus]{
		gk:    GroupKind{Kind: "Widget"},
		bh:    bh,
		inner: requeueController{},
	}
	// GC removes the unfinalized, deletion-pending row; the controller's
	// RequeueAfter must be dropped so the worker doesn't reschedule a dead id.
	result, gone, err := reconcilePass(tc, ctx, raw.ID)
	require.NoError(t, err)
	assert.Equal(t, Settled(), result, "requeue dropped because the row was collected")
	assert.True(t, gone, "the worker is told the row is gone")

	_, err = s.Objects().Get(ctx, raw.ID)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestTypedControllerReconcile(t *testing.T) {
	ctx := context.Background()

	s, err := sqlite.OpenMemory()
	require.NoError(t, err)
	defer s.Close()

	specJSON, err := json.Marshal(tSpec{})
	require.NoError(t, err)
	raw, err := s.Objects().Create(ctx, GroupKind{Kind: "Widget"}, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)

	bh := &Beehive{store: s}
	capCh := make(chan *Object[tSpec, tStatus], 1)
	tc := &typedController[tSpec, tStatus]{
		gk:    GroupKind{Kind: "Widget"},
		bh:    bh,
		inner: &reconcileCapture{ch: capCh},
	}
	result, _, err := reconcilePass(tc, ctx, raw.ID)
	require.NoError(t, err)
	assert.Equal(t, Settled(), result)

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
	fn     func(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) ReconcileResult
}

func (c *funcController) Reconcile(ctx context.Context, client ControllerClient[cStatus], obj *Object[cSpec, cStatus]) ReconcileResult {
	res := c.fn(ctx, client, obj)
	if c.signal != nil {
		c.signal.fire()
	}
	return res
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
	raw, err := s.Objects().Create(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)

	bh := &Beehive{store: s}
	tc := &typedController[cSpec, cStatus]{
		gk: clientTestGK,
		bh: bh,
		inner: &funcController{fn: func(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) ReconcileResult {
			if err := cc.UpdateStatus(ctx, cStatus{Val: "written"}); err != nil {
				return Fail(err)
			}
			return Fail(errBoom)
		}},
	}

	_, _, rerr := reconcilePass(tc, ctx, raw.ID)
	require.ErrorIs(t, rerr, errBoom, "the reconcile error still surfaces for retry")

	got, err := s.Objects().Get(ctx, raw.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status, "the status write committed despite the reconcile error")
	assert.Nil(t, got.ObservedGeneration, "a failed pass settles nothing")
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
		gk:    clientTestGK,
		bh:    bh,
		inner: inner,
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
	s := newClientTestStore(t)

	specJSON, err := json.Marshal(cSpec{})
	require.NoError(t, err)
	raw, err := s.Objects().Create(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)

	tc, inner := newSyncController(wrapStore(s, wrap))
	count := func(t *testing.T) int64 {
		t.Helper()
		got, err := s.Objects().Get(ctx, raw.ID)
		require.NoError(t, err)
		return got.ReconcileOwed
	}
	owe := func() error { return incrementOwed(t, s, raw.ID) }
	return tc, inner, raw.ID, count, owe
}

// TestReconcileDecrementsReconcileOwed pins the durable-wake decrement: a successful
// pass services one owed wake (count down by one), and a failed pass leaves the
// count owed for the backstop to retry.
func TestReconcileDecrementsReconcileOwed(t *testing.T) {
	ctx := context.Background()
	tc, inner, id, count, owe := reconcileOwedHarness(t, nil)

	// Success decrements the owed count to zero.
	inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		return Settled()
	}
	require.NoError(t, owe())
	_, _, err := reconcilePass(tc, ctx, id)
	require.NoError(t, err)
	assert.Zero(t, count(t), "a successful pass services the owed wake")

	// A failed pass leaves the count owed for the backstop.
	inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		return Fail(errBoom)
	}
	require.NoError(t, owe())
	_, _, err = reconcilePass(tc, ctx, id)
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

	inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		return Settled()
	}
	// Three wakes owed, as a crashed process would have left them.
	for range 3 {
		require.NoError(t, owe())
	}
	require.Equal(t, int64(3), count(t))

	_, _, err := reconcilePass(tc, ctx, id)
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
	inner.fn = func(ctx context.Context, _ ControllerClient[cStatus], obj *Object[cSpec, cStatus]) ReconcileResult {
		if err := owe(); err != nil {
			return Fail(err)
		}
		return Settled()
	}
	require.NoError(t, owe()) // the wake this pass loads
	_, _, err := reconcilePass(tc, ctx, id)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count(t),
		"the wake owed during the pass is not clobbered by the pass's decrement")
}

// failDecrementReconcileOwedStore fails the durable-wake decrement while delegating
// the rest, so a test can exercise the reconciler's log-and-continue branch.
type failDecrementReconcileOwedStore struct {
	Store
}

func (s *failDecrementReconcileOwedStore) ReconcileOwed() storeapi.ReconcileOwed {
	return owedOverride{ReconcileOwed: s.Store.ReconcileOwed(), decrement: s.decrement}
}

func (s *failDecrementReconcileOwedStore) decrement(context.Context, GroupKind, ObjectID, int64) error {
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
	inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		return Settled()
	}

	_, _, err := reconcilePass(tc, ctx, id)
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
	raw, err := s.Objects().Create(ctx, clientTestGK, ObjectsCreateInput{
		Name:       uniqueName(),
		Spec:       specJSON,
		Finalizers: []string{"f"},
	})
	require.NoError(t, err)
	_, err = s.DeletionRequests().Create(ctx, clientTestGK, raw.ID)
	require.NoError(t, err)

	bh := &Beehive{store: s}
	tc := &typedController[cSpec, cStatus]{
		gk: clientTestGK,
		bh: bh,
		inner: &funcController{fn: func(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) ReconcileResult {
			if err := cc.DeleteFinalizer(ctx, "f"); err != nil {
				return Fail(err)
			}
			return Fail(errBoom)
		}},
	}

	_, _, _ = reconcilePass(tc, ctx, raw.ID)

	_, err = s.Objects().Get(ctx, raw.ID)
	require.ErrorIs(t, err, ErrNotFound,
		"the committed finalizer clear must let GC collect the row even though reconcile errored")
}

// statusSettingController writes a fixed status on the first Reconcile call and
// fires reconciled.
type statusSettingController struct {
	reconciled *signal
}

func (c *statusSettingController) Reconcile(ctx context.Context, client ControllerClient[cStatus], obj *Object[cSpec, cStatus]) ReconcileResult {
	if err := client.UpdateStatus(ctx, cStatus{Val: "done"}); err != nil {
		return Fail(err)
	}
	c.reconciled.fire()
	return Settled()
}

// specEchoController writes cStatus{Val: obj.Spec.Val} on every Reconcile.
// firstDone fires after the first successful reconcile; secondDone fires once a
// reconcile observes generation 2, signalling that the spec update — not merely a
// second reconcile — was seen.
type specEchoController struct {
	firstDone  *signal
	secondDone *signal
}

func (c *specEchoController) Reconcile(ctx context.Context, client ControllerClient[cStatus], obj *Object[cSpec, cStatus]) ReconcileResult {
	if err := client.UpdateStatus(ctx, cStatus{Val: obj.Spec.Val}); err != nil {
		return Fail(err)
	}
	c.firstDone.fire()
	// Gate on the observed generation, not a reconcile count: a duplicate startup
	// reconcile of the original generation (the startup pass can race the Create's
	// own enqueue) must not be mistaken for the update being reconciled.
	if obj.Generation >= 2 {
		c.secondDone.fire()
	}
	return Settled()
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

func (c *deletionTrackingController) Reconcile(ctx context.Context, client ControllerClient[cStatus], obj *Object[cSpec, cStatus]) ReconcileResult {
	if obj.DeletionRequestedAt != nil {
		c.deleted.fire()
		// Clear the finalizer so GC can collect the row now that the deletion has
		// been observed (idempotent: re-clearing a gone finalizer is a no-op).
		if err := client.DeleteFinalizer(ctx, deletionTrackingFinalizer); err != nil {
			return Fail(err)
		}
		return Settled()
	}
	if err := client.UpdateStatus(ctx, cStatus{Val: "done"}); err != nil {
		return Fail(err)
	}
	c.reconciled.fire()
	return Settled()
}

func TestIntegrationCreateTriggersReconcile(t *testing.T) {
	ctx := context.Background()

	bh := newTestBeehive(t, newClientTestStore(t), fast(WithFullPassInterval(0))...)

	ctrl := &statusSettingController{reconciled: newSignal()}
	err := Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

	ctrl.reconciled.wait(t, "first reconcile")

	// The signal fires inside Reconcile; the stamp lands after it returns.
	got := waitSettled(t, ctx, client, obj.ID)
	require.NotNil(t, got.Status)
	assert.Equal(t, "done", got.Status.Val)
	assert.Equal(t, obj.Generation, *got.ObservedGeneration)
}

func TestIntegrationUpdateTriggersReconcile(t *testing.T) {
	ctx := context.Background()

	bh := newTestBeehive(t, newClientTestStore(t), fast(WithFullPassInterval(0))...)

	ctrl := &specEchoController{
		firstDone:  newSignal(),
		secondDone: newSignal(),
	}
	err := Register(bh, clientTestGK, ctrl)
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

	_, err = client.Update(ctx, obj.ID, cSpec{Val: "v2"})
	require.NoError(t, err)

	ctrl.secondDone.wait(t, "second reconcile after spec update")

	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status)
	assert.Equal(t, "v2", got.Status.Val)
}

func TestIntegrationDeleteTriggersReconcile(t *testing.T) {
	ctx := context.Background()

	bh := newTestBeehive(t, newClientTestStore(t), fast(WithFullPassInterval(0))...)

	ctrl := &deletionTrackingController{
		reconciled: newSignal(),
		deleted:    newSignal(),
	}
	err := Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	// The finalizer keeps the row alive until the controller observes the deletion;
	// see deletionTrackingFinalizer.
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"}, WithFinalizers(deletionTrackingFinalizer))

	ctrl.reconciled.wait(t, "first reconcile")

	require.NoError(t, client.Delete(ctx, obj.ID))
	ctrl.deleted.wait(t, "reconcile after deletion requested")
}

// The pull path under the delete push: Delete enqueues its object, so the test
// above no longer reaches the sweeper. Marking through the store issues no push
// at all, which leaves the GC tick as the only thing that can dispatch this.
func TestIntegrationDeleteCollectsWithoutThePush(t *testing.T) {
	ctx := context.Background()

	store := newClientTestStore(t)
	bh := newTestBeehive(t, store, fast(WithFullPassInterval(0))...)

	ctrl := &deletionTrackingController{
		reconciled: newSignal(),
		deleted:    newSignal(),
	}
	err := Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"}, WithFinalizers(deletionTrackingFinalizer))

	ctrl.reconciled.wait(t, "first reconcile")

	_, err = store.DeletionRequests().Create(ctx, clientTestGK, obj.ID)
	require.NoError(t, err)
	ctrl.deleted.wait(t, "reconcile after the sweeper found the mark")
}

// TestIntegrationWritePersistsAcrossReconcileError is the end-to-end counterpart
// of TestReconcilePersistsWritesOnError: a status write made during a reconcile
// that then returns an error stays committed, because reconcile no longer runs
// under a transaction. (To make a group of writes atomic, a controller uses
// ControllerClient.Within — see TestControllerClientWithin.)
func TestIntegrationWritePersistsAcrossReconcileError(t *testing.T) {
	ctx := context.Background()

	bh := newTestBeehive(t, newClientTestStore(t), fast(WithFullPassInterval(0))...)

	ctrl := &funcController{
		signal: newSignal(),
		fn: func(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) ReconcileResult {
			_ = cc.UpdateStatus(ctx, cStatus{Val: "persisted"})
			return Fail(errBoom)
		},
	}
	err := Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

	ctrl.signal.wait(t, "reconcile wrote status before erroring")

	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status, "status write commits even though the reconcile returned an error")
	assert.Equal(t, "persisted", got.Status.Val)
}

// conditionSettingController sets a Ready=True condition on the first Reconcile,
// then fires reconciled.
type conditionSettingController struct {
	reconciled *signal
}

func (c *conditionSettingController) Reconcile(ctx context.Context, client ControllerClient[cStatus], obj *Object[cSpec, cStatus]) ReconcileResult {
	if err := client.SetCondition(ctx, Condition{
		Type: "Ready", Status: ConditionTrue, Reason: "Provisioned",
	}); err != nil {
		return Fail(err)
	}
	c.reconciled.fire()
	return Settled()
}

func TestIntegrationSetConditionCommitsAndFlows(t *testing.T) {
	ctx := context.Background()

	bh := newTestBeehive(t, newClientTestStore(t), fast(WithFullPassInterval(0))...)

	ctrl := &conditionSettingController{reconciled: newSignal()}
	err := Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

	ctrl.reconciled.wait(t, "first reconcile")

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

	bh := newTestBeehive(t, newClientTestStore(t), fast(WithFullPassInterval(0))...)

	ctrl := &funcController{
		signal: newSignal(),
		fn: func(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) ReconcileResult {
			_ = cc.SetCondition(ctx, Condition{Type: "Ready", Status: ConditionTrue})
			return Fail(errBoom)
		},
	}
	err := Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

	ctrl.signal.wait(t, "reconcile set condition before erroring")

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
	_, err = store.Objects().Create(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)

	bh := newTestBeehive(t, store, WithFullPassInterval(0))

	ctrl := &statusSettingController{reconciled: newSignal()}
	err = Register(bh, clientTestGK, ctrl)
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
	r.work.addAfter(1, time.Hour, alarmRequeueAfter)
	require.NotZero(t, r.backoffFor[1], "precondition: backoff seeded")
	require.NotNil(t, r.work.gauge.alarmFor(1), "precondition: retry timer scheduled")

	r.requeueNow(1)

	assert.Equal(t, seeded, r.backoffFor[1], "requeueNow must preserve the backoff entry")
	assert.Nil(t, r.work.gauge.alarmFor(1), "requeueNow must cancel the stale retry timer")

	id, ok := r.work.get()
	require.True(t, ok, "requeueNow must make the id dispatchable now")
	assert.Equal(t, ObjectID(1), id)
}

// TestReconcilerScheduleAt verifies scheduleAt reports a pending delayed add's
// fire time and reports the zero Schedule for an id with no schedule.
func TestReconcilerScheduleAt(t *testing.T) {
	r := &reconciler{work: newWorkQueue()}
	r.work.addAfter(1, time.Hour, alarmRequeueAfter)

	at := r.scheduleAt(1).NextRequeueAt
	require.False(t, at.IsZero())
	assert.True(t, at.After(time.Now().Add(time.Minute)), "fire time must be ~1h out, got %s", at)

	assert.True(t, r.scheduleAt(2).NextRequeueAt.IsZero(),
		"an id with no schedule must report nothing")
}

// TestReconcilerScheduleAtNilWork verifies the scheduling methods are safe on a
// reconciler with no work queue (built outside Register, e.g. in tests).
func TestReconcilerScheduleAtNilWork(t *testing.T) {
	r := &reconciler{backoffFor: make(map[ObjectID]time.Duration)}
	assert.True(t, r.scheduleAt(1).NextRequeueAt.IsZero(),
		"nil work queue must report nothing scheduled")
	assert.NotPanics(t, func() { r.requeueNow(1) }, "requeueNow must be nil-work safe")
}

// wakeStampingStore is the store surface an owed-pass test needs: the Store
// contract, whose ReconcileOwed() family carries an Increment that is
// deliberately absent from storeapi.ReconcileOwed (see the comment on
// reconcileOwedHarness) but exists on the concrete sqlite family, so a test can
// seed an owed wake without staging the whole declare race.
type wakeStampingStore interface {
	Store
}

// owedIncrementer is the concrete sqlite family's seed-only Increment.
type owedIncrementer interface {
	Increment(context.Context, ObjectID) error
}

// incrementOwed seeds an owed wake through the concrete family.
func incrementOwed(t *testing.T, s Store, id ObjectID) error {
	t.Helper()
	inc, ok := s.ReconcileOwed().(owedIncrementer)
	require.True(t, ok, "the sqlite ReconcileOwed family seeds an owed wake")
	return inc.Increment(context.Background(), id)
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
	err = Register(bh, gk, &recordingController{reconciled: reconciled},
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
	raw, err := real.Objects().Create(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte(`{}`)})
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
		raw, err := s.Objects().Create(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte(`{}`)})
		require.NoError(t, err)
		err = s.Objects().UpdateStatus(ctx, gk, raw.ID, []byte(`{}`), 0)
		require.NoError(t, err)
		id = raw.ID
	})

	// Now owed a wake, the way a crash between a target's commit and the
	// dependent's dispatch leaves it.
	require.NoError(t, incrementOwed(t, real, id))

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
		raw, err := store.Objects().Create(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte(`{}`)})
		require.NoError(t, err)
		settleRow(t, ctx, store, gk, raw.ID)
		return raw.ID
	}
	probeID, sentinelID := settle(), settle()

	ch := make(chan ObjectID, 4)
	logger, started := loggerSignallingOn(reconcilerStartedMsg)
	bh := newTestBeehive(t, store, withOwedPassInterval(0), withoutGCSweeper(), WithLogger(logger))
	opts = append(opts, WithStartupFullPass(false))
	err = Register(bh, gk, &recordingController{reconciled: ch}, opts...)
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

	sentinel, err := store.Objects().Create(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte(`{}`)})
	require.NoError(t, err)
	// Settled, so no startup pass of its own can reach it: the only thing that ever
	// dispatches it is the explicit requeue below.
	settleRow(t, ctx, store, gk, sentinel.ID)

	reconciled := make(chan ObjectID, 8)
	logger, started := loggerSignallingOn(reconcilerStartedMsg)
	bh := newTestBeehive(t, store, withOwedPassInterval(0), WithFullPassInterval(0), withoutGCSweeper(), WithLogger(logger))
	err = Register(bh, gk, &recordingController{reconciled: reconciled}, opts...)
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
		raw, err := s.Objects().Create(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte(`{}`)})
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
		raw, err := s.Objects().Create(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte(`{}`)})
		require.NoError(t, err)
		settleRow(t, ctx, s, gk, raw.ID)
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
		err = Register(bh, gk, &noopController[tSpec, tStatus]{})
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
func newClientOnlyTargetFixture(t *testing.T) (*Beehive, Store, *depObserver, func()) {
	t.Helper()
	ctx := context.Background()
	store := newClientTestStore(t)

	// The dependency waker is the only driver under test here, so its scans run
	// unthrottled while everything else is pushed out of the way. The re-enqueue
	// floor goes too: it absorbs an enqueue into an alarm that fires a second
	// later, which would reach the dependent after the change and prove nothing
	// about what woke it.
	bh := newTestBeehive(t, store, WithGCInterval(time.Hour),
		withWakeScanMinInterval(0), withMinRequeueInterval(0))
	observer := &depObserver{store: store, seen: make(chan depObservation, 64)}
	err := Register(bh, GroupKind{Kind: "Widget"}, observer,
		WithFullPassInterval(0),
		withOwedPassInterval(time.Hour),
		WithStartupFullPass(false))
	require.NoError(t, err)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)

	// No wait for the waker here: Start subscribes and seeds before it returns,
	// so "the waker was watching" is already a fact. With the startup pass
	// disabled, nothing else would find a write it missed.
	return bh, store, observer, func() {
		observer.release() // a test that failed early may still hold one parked
		_ = stop(ctx)
	}
}

// depObservation is one reconcile of the dependent, and the version of the
// target it read while running.
//
// The version is what makes these tests mean anything. A create enqueues its
// own object, so a reconcile of the dependent is always in flight nearby, and
// an assertion that waits for "a reconcile happened" is satisfied by that one —
// every test here passed with its mutation deleted before the version was
// carried through.
//
// release is non-nil exactly when this reconcile is parked, waiting to be let
// go. A reconcile already running when parking was armed carries nil, and must
// not be mistaken for one that can be released.
type depObservation struct {
	id       ObjectID
	targetRV int64
	release  chan struct{}
}

// depObserver reconciles the dependent by reading its target, so a test can
// wait for the reconcile that saw a particular version of it. target is set
// once the object exists and read on the reconcile goroutine.
//
// It can also park a reconcile after that read: see settle.
type depObserver struct {
	store  Store
	target atomic.Int64
	seen   chan depObservation

	// mu guards the parking state. A reconcile takes its channel under the same
	// lock release closes them under, so it either parks on one that will be
	// closed or does not park at all — never on one nobody holds. Every release
	// is a close, so no path here can block.
	mu      sync.Mutex
	parking bool
	parked  map[chan struct{}]struct{}
}

func (c *depObserver) Reconcile(ctx context.Context, _ ControllerClient[tStatus], obj *Object[tSpec, tStatus]) ReconcileResult {
	obs := depObservation{id: obj.ID, release: c.parkChan()}
	if id := ObjectID(c.target.Load()); id != 0 {
		switch raw, err := c.store.Objects().Get(ctx, id); {
		case err == nil:
			obs.targetRV = raw.ResourceVersion
		case !errors.Is(err, ErrNotFound):
			return Fail(err)
		}
	}
	c.seen <- obs
	if obs.release != nil {
		<-obs.release
	}
	return Settled()
}

// parkChan hands this reconcile the channel it will wait on, or nil when
// parking is off.
func (c *depObserver) parkChan() chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.parking {
		return nil
	}
	ch := make(chan struct{})
	c.parked[ch] = struct{}{}
	return ch
}

// unpark lets one parked reconcile finish.
func (c *depObserver) unpark(ch chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, held := c.parked[ch]; held {
		delete(c.parked, ch)
		close(ch)
	}
}

// release stops parking and frees everything parked. Both halves matter: an
// assertion can succeed while a dispatch is still parked, because the
// observation is sent before the park — and a worker left parked blocks the
// beehive's drain until its deadline. Idempotent, so the fixture can call it
// for a test that failed early.
func (c *depObserver) release() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.parking = false
	for ch := range c.parked {
		delete(c.parked, ch)
		close(ch)
	}
}

// settle drives the dependent until nothing else is queued for it and leaves
// the last reconcile parked inside its dispatch. What the test does next
// therefore races nothing: one object is dispatched at a time, so while this
// reconcile is held no other can read the target, and the queue behind it is
// empty. Call release once the change under test has landed.
//
// It returns the target's version as of that moment. An observation above it
// can only have come from a dispatch enqueued after the change.
func (c *depObserver) settle(t *testing.T, ctx context.Context, bh *Beehive, client Client[tSpec, tStatus], id, target ObjectID) int64 {
	t.Helper()
	r, ok := bh.reconcilerFor(GroupKind{Kind: "Widget"})
	require.True(t, ok)

	c.target.Store(int64(target))
	c.mu.Lock()
	c.parking, c.parked = true, map[chan struct{}]struct{}{}
	c.mu.Unlock()

	require.NoError(t, client.Requeue(ctx, id))
	for {
		// Parked, not merely reconciled: a pass already running when parking
		// was armed is holding nothing, and treating it as held would leave the
		// queue free to dispatch again behind the test's back.
		obs := awaitParked(t, c.seen, id)
		if !queuedFor(r.work, id) {
			break // parked, with nothing behind it
		}
		c.unpark(obs.release) // let it finish so the queued one runs and parks
	}

	raw, err := c.store.Objects().Get(ctx, target)
	require.NoError(t, err)
	return raw.ResourceVersion
}

// queuedFor reports whether id is owed another dispatch. Both halves are
// needed: an add that arrives while id is being processed does not queue it, it
// marks the gauge dirty, and done re-queues it from there. Checking only items
// reads a parked reconcile with an add behind it as "nothing pending", which is
// how a test built on this went flaky.
//
// Read under the queue's own lock, and meaningful only while a reconcile of id
// is parked: nothing else can move it then.
func queuedFor(q *workQueue, id ObjectID) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return queuedForLocked(q, id)
}

// queuedForLocked is queuedFor with q.mu already held, for a caller reading it
// alongside another field of the queue in one observation.
func queuedForLocked(q *workQueue, id ObjectID) bool {
	if _, dirty := q.gauge.dirty[id]; dirty {
		return true
	}
	return slices.Contains(q.items, id)
}

// awaitParked waits for a reconcile of id that is parked, and returns it.
func awaitParked(t *testing.T, ch chan depObservation, id ObjectID) depObservation {
	t.Helper()
	for {
		select {
		case obs := <-ch:
			if obs.id == id && obs.release != nil {
				return obs
			}
		case <-time.After(testTimeout):
			t.Fatal("the dependent's requeued pass did not park")
		}
	}
}

// awaitTargetAbove waits for a reconcile of id that read the target at a
// version above rv — that is, one that observed the change under test rather
// than a reconcile queued before it.
func awaitTargetAbove(t *testing.T, ch chan depObservation, id ObjectID, rv int64, msg string) {
	t.Helper()
	awaitObservation(t, ch, msg, func(obs depObservation) bool {
		return obs.id == id && obs.targetRV > rv
	})
}

func awaitObservation(t *testing.T, ch chan depObservation, msg string, want func(depObservation) bool) {
	t.Helper()
	for {
		select {
		case obs := <-ch:
			if want(obs) {
				return
			}
		case <-time.After(testTimeout):
			t.Fatal(msg)
		}
	}
}

// TestClientOnlyTargetWakesDependent is the defect: a depends_on edge may point
// at an object of a kind with no controller, and a per-registered-kind waker
// never observes it. Not a dropped wake — none is ever attempted, so no amount
// of healthy operation repairs it. With every periodic driver disabled, the
// dependent must still be requeued when its client-only target changes.
func TestClientOnlyTargetWakesDependent(t *testing.T) {
	ctx := context.Background()
	bh, store, observer, stop := newClientOnlyTargetFixture(t)
	defer stop()

	depClient := NewClient[tSpec, tStatus](bh, GroupKind{Kind: "Widget"})
	dep := mustCreate(t, ctx, depClient, uniqueName(), tSpec{})
	target := mustCreate(t, ctx, NewClient[tSpec, tStatus](bh, clientOnlyGK), "target-a", tSpec{})
	require.NoError(t, addEdge(ctx, store, dep.ID, target.ID, RelationDependsOn))
	at := observer.settle(t, ctx, bh, depClient, dep.ID, target.ID)

	err := store.Conditions().Set(ctx, clientOnlyGK, target.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	// Client has no conditions write, so this one goes straight to the store —
	// which announces nothing. Publish what an in-band write would have.
	bh.signalKindWritten(ctx, clientOnlyGK)
	observer.release()

	awaitTargetAbove(t, observer.seen, dep.ID, at,
		"the dependent was never woken: its target's kind has no controller, so no waker observed the change")
}

// TestClientOnlyTargetCreatedAfterStart is the same defect for a target whose
// kind has no objects at all when Start runs. It is the discriminating case
// against subscribing per kind *present in the store at Start*: that option
// passes the test above and fails this one, which is an ordinary shape for a
// client-only kind.
func TestClientOnlyTargetCreatedAfterStart(t *testing.T) {
	ctx := context.Background()
	bh, store, observer, stop := newClientOnlyTargetFixture(t)
	defer stop()

	depClient := NewClient[tSpec, tStatus](bh, GroupKind{Kind: "Widget"})
	dep := mustCreate(t, ctx, depClient, uniqueName(), tSpec{})

	// The kind's first object is born after Start, so nothing observable at
	// subscribe time could have named it.
	target := mustCreate(t, ctx, NewClient[tSpec, tStatus](bh, clientOnlyGK), "target-b", tSpec{})
	require.NoError(t, addEdge(ctx, store, dep.ID, target.ID, RelationDependsOn))
	at := observer.settle(t, ctx, bh, depClient, dep.ID, target.ID)

	err := store.Conditions().Set(ctx, clientOnlyGK, target.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	// Client has no conditions write, so this one goes straight to the store —
	// which announces nothing. Publish what an in-band write would have.
	bh.signalKindWritten(ctx, clientOnlyGK)
	observer.release()

	awaitTargetAbove(t, observer.seen, dep.ID, at,
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
	bh, store, observer, stop := newClientOnlyTargetFixture(t)
	defer stop()

	widget := GroupKind{Kind: "Widget"}
	depClient := NewClient[tSpec, tStatus](bh, widget)
	dep := mustCreate(t, ctx, depClient, uniqueName(), tSpec{})
	targetClient := NewClient[tSpec, tStatus](bh, clientOnlyGK)
	target := mustCreate(t, ctx, targetClient, uniqueName(), tSpec{})
	require.NoError(t, addEdge(ctx, store, dep.ID, target.ID, RelationDependsOn))
	at := observer.settle(t, ctx, bh, depClient, dep.ID, target.ID)

	require.NoError(t, targetClient.Delete(ctx, target.ID))
	observer.release()

	awaitTargetAbove(t, observer.seen, dep.ID, at,
		"the dependent was never woken by its target's tombstone, so nothing can drop the edge that RESTRICT-blocks collection")

	// The wake is only half the story: with the edge dropped, the target must
	// actually collect rather than stay deletion-pending forever.
	_, err := store.Edges().Delete(ctx, dep.ID, target.ID, RelationDependsOn)
	require.NoError(t, err)
	_, err = bh.gcCollect(ctx, target.ID)
	require.NoError(t, err)
	_, err = store.Objects().Get(ctx, target.ID)
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
	target, err := s.Objects().Create(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)
	dep, err := s.Objects().Create(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)
	require.NoError(t, addEdge(ctx, s, dep.ID, target.ID, RelationDependsOn))

	tc, inner := newSyncController(wrapStore(s, wrap))
	return &watermarkHarness{tc: tc, inner: inner, store: s, dep: dep.ID, target: target.ID}
}

// stale is what the stale-dependents pass would enqueue right now.
func (h *watermarkHarness) stale(t *testing.T) []ObjectID {
	t.Helper()
	return staleDependentIDs(t, h.store, clientTestGK)
}

// touchTarget writes the target's spec, so it moves above any watermark recorded
// before now.
func (h *watermarkHarness) touchTarget(t *testing.T, spec string) {
	t.Helper()
	_, _, err := h.store.Objects().UpdateSpec(context.Background(), clientTestGK, h.target, []byte(spec), 0)
	require.NoError(t, err)
}

// TestReconcileRecordsDependencyWatermark pins the write: a dependent is stale
// until a pass records the cursor it reconciled against, and settles once one
// does.
func TestReconcileRecordsDependencyWatermark(t *testing.T) {
	ctx := context.Background()
	h := newWatermarkHarness(t, nil)
	h.inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		return Settled()
	}
	require.Equal(t, []ObjectID{h.dep}, h.stale(t), "a dependent that never reconciled is stale")

	_, _, err := reconcilePass(h.tc, ctx, h.dep)
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
	h.inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		return Settled()
	}
	_, _, err := reconcilePass(h.tc, ctx, h.dep)
	require.NoError(t, err)
	require.Empty(t, h.stale(t), "settled, with a watermark for the declare below to clear")

	specJSON, err := json.Marshal(cSpec{})
	require.NoError(t, err)
	second, err := h.store.Objects().Create(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)
	h.inner.fn = func(ctx context.Context, cc ControllerClient[cStatus], _ *Object[cSpec, cStatus]) ReconcileResult {
		if err := cc.AddDependency(ctx, second.ID); err != nil {
			return Fail(err)
		}
		return Settled()
	}

	_, _, err = reconcilePass(h.tc, ctx, h.dep)
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
	h.inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		return Settled()
	}
	_, _, err := reconcilePass(h.tc, ctx, h.dep)
	require.NoError(t, err)
	require.Empty(t, h.stale(t), "settled, with a watermark for the mid-pass declare to clear")

	// A quiet target, created before the pass loads so the pass's cursor covers its
	// version — the shape that makes the rewritten watermark read as converged.
	specJSON, err := json.Marshal(cSpec{})
	require.NoError(t, err)
	quiet, err := h.store.Objects().Create(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)

	// The third party declares from outside the pass's client, mid-flight. The
	// target never moves, so only the edge-new stamp can carry this wake.
	h.inner.fn = func(ctx context.Context, _ ControllerClient[cStatus], _ *Object[cSpec, cStatus]) ReconcileResult {
		if _, err := h.store.Edges().Add(ctx, h.dep, quiet.ID, RelationDependsOn); err != nil {
			return Fail(err)
		}
		return Settled()
	}
	_, _, err = reconcilePass(h.tc, ctx, h.dep)
	require.NoError(t, err)

	// The derived state really is blind here — that blindness was the strand.
	assert.Empty(t, h.stale(t), "the pass rewrote the watermark from a cursor that never saw the new target")

	// The durable record is not: the stamp survived the pass's decrement, so the
	// owed pass still delivers the reconcile that reads the new target.
	got, err := h.store.Objects().Get(ctx, h.dep)
	require.NoError(t, err)
	assert.Equal(t, int64(1), got.ReconcileOwed, "the mid-pass stamp outlives the load-scoped decrement")
	owed, err := h.store.ReconcileOwed().ListIDs(ctx, clientTestGK)
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
	dep, err := s.Objects().Create(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)
	target, err := s.Objects().Create(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)
	tc, inner := newSyncController(s)
	stale := func() []ObjectID { return staleDependentIDs(t, s, clientTestGK) }

	inner.fn = func(ctx context.Context, cc ControllerClient[cStatus], _ *Object[cSpec, cStatus]) ReconcileResult {
		if err := cc.AddDependency(ctx, target.ID); err != nil {
			return Fail(err)
		}
		return Settled()
	}
	_, _, err = reconcilePass(tc, ctx, dep.ID)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{dep.ID}, stale(), "no watermark was written, so one more pass is owed")

	// And it settles on that pass, which now loads with the edge in place.
	inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		return Settled()
	}
	_, _, err = reconcilePass(tc, ctx, dep.ID)
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

func (s *watermarkProbeStore) Dependencies() storeapi.Dependencies {
	return depsOverride{Dependencies: s.Store.Dependencies(), watermarkSet: s.watermarkSet}
}

func (s *watermarkProbeStore) watermarkSet(ctx context.Context, id ObjectID, cursor int64) error {
	s.sets = append(s.sets, id)
	if s.err != nil {
		return s.err
	}
	return s.Store.Dependencies().WatermarkSet(ctx, id, cursor)
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
	h.inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		return Settled()
	}

	_, _, err := reconcilePass(h.tc, ctx, h.target)
	require.NoError(t, err)
	assert.Empty(t, probe.sets, "an object with no dependencies never takes the write lock")

	_, _, err = reconcilePass(h.tc, ctx, h.dep)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{h.dep}, probe.sets, "a dependent does record one")
}

// TestALostWatermarkStillFindsAnUnobservedChange is why a failed watermark write
// needs no compensating record. The write leaves the watermark low, and a low
// watermark only over-reports staleness: a target change this pass did not
// observe is issued above the sweep's cursor, so even a process that keeps
// running finds the dependent. See docs/adr/2026-08-03-stale-dependents-cursor.md.
func TestALostWatermarkStillFindsAnUnobservedChange(t *testing.T) {
	ctx := context.Background()
	var probe *watermarkProbeStore
	h := newWatermarkHarness(t, func(s Store) Store {
		probe = &watermarkProbeStore{Store: s}
		return probe
	})
	h.inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		return Settled()
	}
	// One sweeper for the whole test: a live process, the case no restart repairs.
	sd := sweeperOver(h.store)
	_, _, err := reconcilePass(h.tc, ctx, h.dep)
	require.NoError(t, err)
	sd.sweep(ctx)
	require.Empty(t, h.stale(t), "converged: the watermark is current and the cursor is past it")

	// The watermark write fails for a pass that could not have observed the change
	// its own controller triggered.
	probe.err = errBoom
	h.inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		h.touchTarget(t, `{"val":"moved mid-pass"}`)
		return Settled()
	}
	_, _, err = reconcilePass(h.tc, ctx, h.dep)
	require.NoError(t, err, "the reconcile succeeded; only the watermark write failed")
	raw, err := h.store.Objects().Get(ctx, h.dep)
	require.NoError(t, err)
	require.Zero(t, raw.ReconcileOwed, "nothing durable names the dependent")

	sd.sweep(ctx)

	raw, err = h.store.Objects().Get(ctx, h.dep)
	require.NoError(t, err)
	assert.EqualValues(t, 1, raw.ReconcileOwed,
		"the same process finds it: the unobserved change is above its cursor")
}

// TestALostWatermarkCostsOnlyAnObservedChange pins what the lost write does give
// up. Once the target goes quiet, a cursor-bound sweep stops re-reporting the
// dependent — but every change still below that cursor is one this pass already
// observed, so the pass it gives up is a redundant one.
func TestALostWatermarkCostsOnlyAnObservedChange(t *testing.T) {
	ctx := context.Background()
	var probe *watermarkProbeStore
	h := newWatermarkHarness(t, func(s Store) Store {
		probe = &watermarkProbeStore{Store: s}
		return probe
	})
	var observed int64
	h.inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		target, err := h.store.Objects().Get(ctx, h.target)
		require.NoError(t, err)
		observed = target.ResourceVersion
		return Settled()
	}
	sd := sweeperOver(h.store)
	_, _, err := reconcilePass(h.tc, ctx, h.dep)
	require.NoError(t, err)
	sd.sweep(ctx)

	h.touchTarget(t, `{"val":"moved"}`)
	sd.sweep(ctx)
	require.NoError(t, h.store.ReconcileOwed().Decrement(ctx, clientTestGK, h.dep, 1),
		"drain the finding, as the reconcile it dispatched would")

	probe.err = errBoom
	_, _, err = reconcilePass(h.tc, ctx, h.dep)
	require.NoError(t, err)

	target, err := h.store.Objects().Get(ctx, h.target)
	require.NoError(t, err)
	require.Equal(t, target.ResourceVersion, observed,
		"the pass whose watermark was lost observed the target's latest version")

	sd.sweep(ctx)

	raw, err := h.store.Objects().Get(ctx, h.dep)
	require.NoError(t, err)
	assert.Zero(t, raw.ReconcileOwed,
		"not re-reported, and nothing is owed: the only change below the cursor was observed")
}

// TestReconcileIsQuietWhenShutdownLosesTheWatermark separates the two reasons the
// watermark write fails. Stop cancels the ctx the pass runs on, so a reconcile in
// flight loses the write for no fault of its own — reporting it would put a WARN
// on every clean shutdown of any object with dependencies.
func TestReconcileIsQuietWhenShutdownLosesTheWatermark(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	logger, logs := captureLogger(slog.LevelWarn)
	h := newWatermarkHarness(t, func(s Store) Store {
		return &watermarkProbeStore{Store: s, err: errBoom}
	})
	h.tc.logger = logger
	// Cancel inside the pass: the load and the reconcile succeed, and only the
	// bookkeeping that follows meets a dead context.
	h.inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		cancel()
		return Settled()
	}

	_, _, err := reconcilePass(h.tc, ctx, h.dep)
	require.NoError(t, err)

	assert.Empty(t, logs.String(), "a cancelled write is shutdown, not a lost pass")
}

// TestReconcileRecordsCursorFromTheLoad pins where the cursor comes from. A
// target that moves *during* the pass was not observed by it, so the dependent
// must stay stale: recording a cursor sampled after the controller's reads would
// land above a change the pass never saw, leaving the dependent stranded with
// nothing left to find it. Erring the other way costs one extra pass.
func TestReconcileRecordsCursorFromTheLoad(t *testing.T) {
	ctx := context.Background()
	h := newWatermarkHarness(t, nil)
	h.inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		h.touchTarget(t, `{"val":"moved mid-pass"}`)
		return Settled()
	}

	_, _, err := reconcilePass(h.tc, ctx, h.dep)
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
	h.inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		return Fail(errBoom)
	}

	_, _, err := reconcilePass(h.tc, ctx, h.dep)
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
	_, _, err := h.store.Objects().UpdateSpec(ctx, clientTestGK, h.dep, []byte("not-json"), 0)
	require.NoError(t, err)

	_, _, err = reconcilePass(h.tc, ctx, h.dep)
	require.NoError(t, err, "an undecodable row is still a no-op success")

	assert.Empty(t, probe.sets, "a pass that never ran records nothing")
	assert.Equal(t, []ObjectID{h.dep}, h.stale(t))
}

// TestReconcileWarnsAndContinuesOnCursorWriteFailure pins the failure contract:
// no error escapes into the backoff ladder over a bookkeeping write, and the
// unwritten watermark leaves the dependent stale rather than settled.
func TestReconcileWarnsAndContinuesOnCursorWriteFailure(t *testing.T) {
	ctx := context.Background()
	h := newWatermarkHarness(t, func(s Store) Store {
		return &watermarkProbeStore{Store: s, err: errBoom}
	})
	logger, logs := captureLogger(slog.LevelWarn)
	h.tc.logger = logger
	h.inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		return Settled()
	}

	_, _, err := reconcilePass(h.tc, ctx, h.dep)

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
	cc := registerWithClient(t, bh1, gk, ctrl1, WithFullPassInterval(0))

	client1 := NewClient[tSpec, tStatus](bh1, gk)
	ctrl1.client = client1

	target := mustCreate(t, ctx, client1, uniqueName(), tSpec{})
	dep := mustCreate(t, ctx, client1, uniqueName(), tSpec{})
	ctrl1.targetID, ctrl1.depID = target.ID, dep.ID
	// Declared before the change. The declare stamps one owed pass, but the
	// startup reconcile below services and drains it — so by the time the target
	// moves, nothing durable records that a wake is owed: this is the ordinary
	// settled dependency, not the declare-time case the stamp covers.
	require.NoError(t, cc.at(dep.ID).AddDependency(ctx, target.ID))

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
	err = db.Conditions().Set(ctx, gk, target.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	// --- the restart: a second process, the first already stopped ---
	bh2, err := New(db, withDependencyWakerOff())
	require.NoError(t, err)
	ctrl2 := &dependentController{
		observed: make(chan bool, 8),
		depID:    dep.ID,
		targetID: target.ID,
	}
	err = Register(bh2, gk, ctrl2, WithFullPassInterval(0), WithStartupFullPass(false))
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

// cycleController writes a changing status on every pass, so each pass bumps
// its object's resource_version and wakes whatever depends on it.
type cycleController struct {
	calls      atomic.Int64
	first, hot *signal
}

func (c *cycleController) Reconcile(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) ReconcileResult {
	n := c.calls.Add(1)
	if n >= hotLoopCalls {
		c.hot.fire()
	}
	c.first.fire()
	if err := cc.UpdateStatus(ctx, cStatus{Val: fmt.Sprint(n)}); err != nil {
		return Fail(err)
	}
	return Settled()
}

// Two objects that depend on each other reconcile forever: each pass wakes the
// other and no generation ever moves, so nothing reports a problem. The
// re-enqueue floor is what bounds the loop — see the cycle item in docs/TODO.md,
// which this does not fix, only rate-limits.
//
// The waker scans unthrottled here, so the wake path is not the limiter.
func TestADependencyCycleIsBoundedByTheFloor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctrl := &cycleController{first: newSignal(), hot: newSignal()}
	bh := newTestBeehive(t, newClientTestStore(t),
		withWakeScanMinInterval(0),
		withMinRequeueInterval(hotLoopWindow))
	cc := registerWithClient(t, bh, clientTestGK, ctrl)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	a := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	b := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "b"})
	require.NoError(t, cc.at(a.ID).AddDependency(ctx, b.ID))
	require.NoError(t, cc.at(b.ID).AddDependency(ctx, a.ID))

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(context.Background())

	requireNoHotLoop(t, ctrl.first, ctrl.hot, &ctrl.calls,
		"a dependency cycle must be floored, not run at wake speed")
}

// The worker reads each kind for scheduling alone.
func TestReconcilerSchedulesFromTheResultKind(t *testing.T) {
	t.Run("RequeueAfter(0) re-dispatches", func(t *testing.T) {
		calls := 0
		doneCh := make(chan struct{})
		adapter := &fakeAdapter{
			reconcileFn: func(_ context.Context, _ ObjectID) ReconcileResult {
				calls++
				if calls == 1 {
					return Unsettled().RequeueAfter(0)
				}
				close(doneCh)
				return Settled()
			},
		}
		r := &reconciler{adapter: adapter, work: newWorkQueue(), backoffFor: make(map[ObjectID]time.Duration)}
		ctx, cancel := context.WithCancel(context.Background())
		done := runInBackground(r, ctx)

		r.enqueue(1)
		waitClosed(t, doneCh, "the second reconcile Unsettled().RequeueAfter(0) asked for")
		cancel()
		waitClosed(t, done, "run to exit")
	})

	t.Run("RequeueAfter(0) waits out the work queue's floor", func(t *testing.T) {
		second := make(chan struct{})
		var once sync.Once
		calls := 0
		adapter := &fakeAdapter{
			reconcileFn: func(_ context.Context, _ ObjectID) ReconcileResult {
				calls++
				if calls > 1 {
					once.Do(func() { close(second) })
					return Settled()
				}
				return Unsettled().RequeueAfter(0)
			},
		}
		q := newWorkQueue()
		q.setFloor(60 * time.Millisecond)
		r := &reconciler{adapter: adapter, work: q, backoffFor: make(map[ObjectID]time.Duration)}
		ctx, cancel := context.WithCancel(context.Background())
		done := runInBackground(r, ctx)
		defer func() { cancel(); waitClosed(t, done, "run to exit") }()

		start := time.Now()
		r.enqueue(1)
		waitClosed(t, second, "the floored re-dispatch")
		// Without the floor this returns immediately and the re-dispatch is a spin.
		assert.GreaterOrEqual(t, time.Since(start), 60*time.Millisecond)
	})

	t.Run("a settled RequeueAfter(0) re-dispatches too", func(t *testing.T) {
		calls := 0
		doneCh := make(chan struct{})
		adapter := &fakeAdapter{
			reconcileFn: func(_ context.Context, _ ObjectID) ReconcileResult {
				calls++
				if calls == 1 {
					return Settled().RequeueAfter(0)
				}
				close(doneCh)
				return Settled()
			},
		}
		r := &reconciler{adapter: adapter, work: newWorkQueue(), backoffFor: make(map[ObjectID]time.Duration)}
		ctx, cancel := context.WithCancel(context.Background())
		done := runInBackground(r, ctx)

		r.enqueue(1)
		waitClosed(t, doneCh, "the second reconcile Settled().RequeueAfter(0) asked for")
		cancel()
		waitClosed(t, done, "run to exit")
	})

	t.Run("a bare Settled schedules nothing", func(t *testing.T) {
		reconciled := make(chan struct{}, 4)
		adapter := &fakeAdapter{
			reconcileFn: func(_ context.Context, _ ObjectID) ReconcileResult {
				reconciled <- struct{}{}
				return Settled()
			},
		}
		r := &reconciler{adapter: adapter, work: newWorkQueue(), backoffFor: make(map[ObjectID]time.Duration)}
		ctx, cancel := context.WithCancel(context.Background())
		done := runInBackground(r, ctx)
		defer func() { cancel(); waitClosed(t, done, "run to exit") }()

		r.enqueue(1)
		<-reconciled
		select {
		case <-reconciled:
			t.Fatal("a bare Settled scheduled a second reconcile")
		case <-time.After(50 * time.Millisecond):
		}
	})
}

// waitScheduleBeyond blocks until id's schedule sits further out than d, which
// is how a test tells an alarm apart from a floored re-dispatch — scheduleAt
// races the worker, so the hub is the signal.
func waitScheduleBeyond(t *testing.T, ctx context.Context, rx *watch.Receiver[ObjectID, gaugeValue], d time.Duration, what string) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()
	for {
		ev, err := rx.RecvContext(waitCtx)
		require.NoError(t, err, what)
		if ev.Value.Schedule.NextRequeueAt.After(time.Now().Add(d)) {
			return
		}
	}
}

// A bare Unsettled must schedule its own return: the unsettled listing gates on
// the generation, so an object that declines to settle without having moved its
// generation is in no listing and no other driver would come back for it.
func TestReconcilerBareUnsettledSchedulesItself(t *testing.T) {
	adapter := &fakeAdapter{
		reconcileFn: func(_ context.Context, _ ObjectID) ReconcileResult {
			return Unsettled()
		},
	}
	q := newWorkQueue()
	r := &reconciler{adapter: adapter, work: q, backoffFor: make(map[ObjectID]time.Duration)}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)
	defer func() { cancel(); waitClosed(t, done, "run to exit") }()

	rx, _ := q.watchSchedule(1)
	defer rx.Close()
	r.enqueue(1)

	// The pass publishes a due-now and a dispatched-zero on the way; what proves
	// the alarm is a schedule further out than the floor could ever be.
	waitScheduleBeyond(t, ctx, rx, 5*time.Second,
		"a bare Unsettled scheduled nothing further out than the floor")
}

// The bare Unsettled alarm is the owed pass extended to the objects its listing
// misses, so it follows that pass's configured cadence: an embedder who asked for
// a quiet store does not get a 30s ping per unsettled object anyway.
func TestReconcilerBareUnsettledFollowsTheOwedPassCadence(t *testing.T) {
	adapter := &fakeAdapter{
		reconcileFn: func(_ context.Context, _ ObjectID) ReconcileResult {
			return Unsettled()
		},
	}
	q := newWorkQueue()
	r := &reconciler{
		adapter:          adapter,
		work:             q,
		backoffFor:       make(map[ObjectID]time.Duration),
		owedPassInterval: time.Hour,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)
	defer func() { cancel(); waitClosed(t, done, "run to exit") }()

	rx, _ := q.watchSchedule(1)
	defer rx.Close()
	r.enqueue(1)

	waitScheduleBeyond(t, ctx, rx, 30*time.Minute,
		"the alarm must follow the owed pass, not the default")
}

// The 30s default is an upper bound, not a period: alarmRequeueAfter does not
// absorb an arriving add, so a wake landing inside the window dispatches on the
// floor's schedule instead of waiting the alarm out.
func TestReconcilerBareUnsettledYieldsToAPush(t *testing.T) {
	first, second := make(chan struct{}), make(chan struct{})
	var onceFirst, onceSecond sync.Once
	calls := 0
	adapter := &fakeAdapter{
		reconcileFn: func(_ context.Context, _ ObjectID) ReconcileResult {
			calls++
			if calls == 1 {
				onceFirst.Do(func() { close(first) })
				return Unsettled()
			}
			onceSecond.Do(func() { close(second) })
			return Settled()
		},
	}
	q := newWorkQueue()
	q.setFloor(60 * time.Millisecond)
	r := &reconciler{adapter: adapter, work: q, backoffFor: make(map[ObjectID]time.Duration)}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)
	defer func() { cancel(); waitClosed(t, done, "run to exit") }()

	rx, _ := q.watchSchedule(1)
	defer rx.Close()
	r.enqueue(1)
	waitClosed(t, first, "the first reconcile")

	// Push only once the alarm is really pending, or a fast dispatch would prove
	// nothing about outranking it.
	waitScheduleBeyond(t, ctx, rx, 5*time.Second, "the bare Unsettled alarm")

	r.enqueue(1)
	waitClosed(t, second, "the pushed reconcile, which must not wait out the 30s alarm")
}

// An unusable result takes the backoff ladder, never the success path — a
// negative gate ("not a Fail") would let the zero value through. The adapter
// normalizes first, so what arrives carries ErrInvalidResult and is logged.
func TestReconcilerTreatsAnUnusableResultAsAFailure(t *testing.T) {
	logger, logged := loggerSignallingOn("controller returned an unusable result")
	calls := 0
	doneCh := make(chan struct{})
	adapter := &fakeAdapter{
		reconcileFn: func(_ context.Context, _ ObjectID) ReconcileResult {
			calls++
			if calls == 1 {
				return ReconcileResult{}.normalize()
			}
			close(doneCh)
			return Settled()
		},
	}
	r := &reconciler{
		adapter:           adapter,
		work:              newWorkQueue(),
		maxRetryInterval:  time.Second,
		baseRetryInterval: 5 * time.Millisecond,
		backoffFor:        make(map[ObjectID]time.Duration),
		logger:            logger,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	r.enqueue(1)
	waitClosed(t, logged, "the unusable result to be logged at Error")
	waitClosed(t, doneCh, "the backoff retry after an unusable result")
	cancel()
	waitClosed(t, done, "run to exit")
}

// Pins that normalize runs at the adapter boundary, before any gate reads the
// result.
func TestReconcileNormalizesAnUnusableControllerReturn(t *testing.T) {
	ctx := context.Background()
	s := newClientTestStore(t)

	specJSON, err := json.Marshal(cSpec{})
	require.NoError(t, err)
	obj, err := s.Objects().Create(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)

	tc, inner := newSyncController(s)
	inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		return ReconcileResult{}
	}
	result, _, err := reconcilePass(tc, ctx, obj.ID)
	require.ErrorIs(t, err, ErrInvalidResult)
	assert.False(t, result.succeeded())

	got, err := s.Objects().Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Nil(t, got.ObservedGeneration, "an unusable result settles nothing")
}

// The generation beehive handed to Reconcile, never a fresh read: stamping a
// mid-pass spec change would mark it seen by a pass that never read it, and
// nothing would reconcile it again.
func TestReconcileStampsTheGenerationItHandedOut(t *testing.T) {
	ctx := context.Background()
	s := newClientTestStore(t)

	specJSON, err := json.Marshal(cSpec{})
	require.NoError(t, err)
	obj, err := s.Objects().Create(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)

	tc, inner := newSyncController(s)
	var handed int64
	inner.fn = func(ctx context.Context, _ ControllerClient[cStatus], obj *Object[cSpec, cStatus]) ReconcileResult {
		handed = obj.Generation
		// The mid-pass spec change.
		next, err := json.Marshal(cSpec{Val: "changed"})
		require.NoError(t, err)
		_, _, err = s.Objects().UpdateSpec(ctx, clientTestGK, obj.ID, next, 0)
		require.NoError(t, err)
		return Settled()
	}
	_, _, err = reconcilePass(tc, ctx, obj.ID)
	require.NoError(t, err)

	got, err := s.Objects().Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.ObservedGeneration)
	assert.Equal(t, handed, *got.ObservedGeneration, "the generation the pass was handed, not a fresh read")
	assert.Less(t, *got.ObservedGeneration, got.Generation, "the mid-pass change is still owed a reconcile")
	assert.Contains(t, unsettledIDs(t, s), obj.ID)
}

// Gated on the generation already loaded, so a converged object costs no store
// call at all — not merely no row write.
func TestReconcileConvergedPassMakesNoStampCall(t *testing.T) {
	ctx := context.Background()
	s := newClientTestStore(t)

	specJSON, err := json.Marshal(cSpec{})
	require.NoError(t, err)
	obj, err := s.Objects().Create(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)

	var calls int
	probe := &objectsOverrideStore{Store: s}
	probe.override.setObservedGen = func(ctx context.Context, gk GroupKind, id ObjectID, gen int64) (bool, error) {
		calls++
		return s.Objects().SetObservedGeneration(ctx, gk, id, gen)
	}

	tc, inner := newSyncController(probe)
	inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		return Settled()
	}

	_, _, err = reconcilePass(tc, ctx, obj.ID)
	require.NoError(t, err)
	require.Equal(t, 1, calls, "the first pass settles a new generation, so it calls the store")

	_, _, err = reconcilePass(tc, ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, 1, calls, "a converged pass must not reach the store at all")
}

// Unsettled records no generation, so the object stays in the unsettled
// listing.
func TestReconcileUnsettledDoesNotStamp(t *testing.T) {
	ctx := context.Background()
	s := newClientTestStore(t)

	specJSON, err := json.Marshal(cSpec{})
	require.NoError(t, err)
	obj, err := s.Objects().Create(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)

	tc, inner := newSyncController(s)
	inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		return Unsettled().RequeueAfter(0)
	}
	_, _, err = reconcilePass(tc, ctx, obj.ID)
	require.NoError(t, err)

	got, err := s.Objects().Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Nil(t, got.ObservedGeneration, "an unsettled pass records nothing")
	assert.Contains(t, unsettledIDs(t, s), obj.ID)
}

// A failed stamp leaves the object unsettled for the listing to re-derive.
// Failing the pass would retry one that already committed its writes.
func TestReconcileFailedStampDoesNotFailThePass(t *testing.T) {
	ctx := context.Background()
	s := newClientTestStore(t)

	specJSON, err := json.Marshal(cSpec{})
	require.NoError(t, err)
	obj, err := s.Objects().Create(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: specJSON})
	require.NoError(t, err)

	probe := &objectsOverrideStore{Store: s}
	probe.override.setObservedGen = func(context.Context, GroupKind, ObjectID, int64) (bool, error) {
		return false, errBoom
	}
	tc, inner := newSyncController(probe)
	inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		return Settled()
	}

	result, _, err := reconcilePass(tc, ctx, obj.ID)
	require.NoError(t, err, "a failed stamp must not fail the pass")
	assert.True(t, result.succeeded())
	assert.Contains(t, unsettledIDs(t, s), obj.ID, "left unsettled for the next pass")
}

// A crash between them must leave an unsettled object with a low watermark,
// which only over-reports staleness — never a settled object whose watermark
// never landed.
func TestReconcileWritesTheWatermarkBeforeTheStamp(t *testing.T) {
	ctx := context.Background()
	var order []string
	h := newWatermarkHarness(t, func(s Store) Store {
		return &orderProbeStore{Store: s, record: func(s string) { order = append(order, s) }}
	})
	h.inner.fn = func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		return Settled()
	}
	_, _, err := reconcilePass(h.tc, ctx, h.dep)
	require.NoError(t, err)

	assert.Equal(t, []string{"watermark", "stamp"}, order)
}

// The point of the option: a return path that declares nothing still comes
// back. Each pass arms the next, so N passes follow one enqueue.
func TestReconcilerIndividualPassRearmsASettledObject(t *testing.T) {
	calls := 0
	doneCh := make(chan struct{})
	adapter := &fakeAdapter{
		reconcileFn: func(_ context.Context, _ ObjectID) ReconcileResult {
			calls++
			if calls == 3 {
				close(doneCh)
			}
			return Settled()
		},
	}

	r := &reconciler{
		adapter:                adapter,
		work:                   newWorkQueue(),
		individualPassInterval: 5 * time.Millisecond,
		maxRetryInterval:       time.Second,
		backoffFor:             make(map[ObjectID]time.Duration),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	r.enqueue(1)
	waitClosed(t, doneCh, "third reconcile from the individual pass")
	cancel()
	waitClosed(t, done, "run to exit")
}

// Off by default: a settled pass with the option unset arms nothing.
func TestReconcilerNoIndividualPassArmsNothing(t *testing.T) {
	adapter := &fakeAdapter{reconcileFn: func(_ context.Context, _ ObjectID) ReconcileResult { return Settled() }}
	r := &reconciler{adapter: adapter, work: newWorkQueue(), backoffFor: make(map[ObjectID]time.Duration)}

	t.Cleanup(r.work.stop)

	r.scheduleNext(1, Settled())

	assert.Nil(t, alarmFor(r.work, 1))
}

// A collected object comes back Settled, which is the branch that arms. Arming
// it would resurrect an id forget just dropped, into a dispatch that can only
// read ErrNotFound. Id 2 is the barrier: one worker dispatches in order, so 1's
// pass is over by the time 2's runs.
func TestReconcilerIndividualPassArmsNothingForACollectedObject(t *testing.T) {
	reached2 := make(chan struct{})
	adapter := &fakeAdapter{gone: true}
	r := &reconciler{
		adapter:                adapter,
		work:                   newWorkQueue(),
		individualPassInterval: time.Minute,
		backoffFor:             make(map[ObjectID]time.Duration),
	}
	adapter.reconcileFn = func(_ context.Context, id ObjectID) ReconcileResult {
		if id == 2 {
			close(reached2)
		}
		return Settled()
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	r.enqueue(1)
	r.enqueue(2)
	waitClosed(t, reached2, "the barrier object reconciled")
	cancel()
	waitClosed(t, done, "run to exit")

	assert.Nil(t, alarmFor(r.work, 1))
}

// What the individual pass must not override. It is a default cadence, not a
// ceiling: a controller that scheduled its own pass — sooner or later — keeps
// it, and a failure keeps its ladder.
func TestReconcilerIndividualPassYieldsToTheResult(t *testing.T) {
	const d = time.Minute

	tests := []struct {
		name   string
		result ReconcileResult
		want   time.Duration
		kind   alarmKind
	}{
		{"a sooner RequeueAfter wins", Settled().RequeueAfter(time.Second), time.Second, alarmRequeueAfter},
		{"a later RequeueAfter wins too", Settled().RequeueAfter(time.Hour), time.Hour, alarmRequeueAfter},
		{"a failure keeps its ladder", Fail(errors.New("boom")), time.Minute, alarmBackoff},
		{"unsettled keeps the owed cadence", Unsettled(), defaultOwedPassInterval, alarmRequeueAfter},
		{"settled with nothing owed takes the individual pass", Settled(), d, alarmRequeueAfter},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &reconciler{
				work:                   newWorkQueue(),
				individualPassInterval: d,
				owedPassInterval:       defaultOwedPassInterval,
				maxRetryInterval:       time.Hour,
				baseRetryInterval:      time.Minute,
				backoffFor:             make(map[ObjectID]time.Duration),
			}
			// Every delay here is minutes out, and stop cancels what is left:
			// an alarm firing after its test lands in whichever test is running.
			t.Cleanup(r.work.stop)

			r.scheduleNext(1, tc.result)

			a := alarmFor(r.work, 1)
			require.NotNil(t, a)
			assert.Equal(t, tc.kind, a.kind)
			assert.InDelta(t, tc.want, time.Until(a.fireAt), float64(time.Second))
		})
	}
}

// The jitter only ever lengthens: a pass never fires early, and two objects
// that settled in the same moment drift apart.
func TestReconcilerIndividualPassJitters(t *testing.T) {
	const d = time.Hour

	tests := []struct {
		name string
		frac float64
		want time.Duration
	}{
		{"no source is exact", 0, d},
		{"the top of the range adds a tenth", 1, d + d/10},
		{"half the range adds a twentieth", 0.5, d + d/20},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &reconciler{
				individualPassInterval: d,
				individualPassRand:     func() float64 { return tc.frac },
			}

			assert.Equal(t, tc.want, d+r.spread(d, individualPassJitterFrac))
		})
	}
}

// The case the option exists for: objects settled by a previous process, owing
// nothing, that no other trigger would ever wake.
func TestReconcilerIndividualPassAdmitsColdObjects(t *testing.T) {
	seen := make(chan ObjectID, 3)
	adapter := &fakeAdapter{
		reconcileFn: func(_ context.Context, id ObjectID) ReconcileResult {
			seen <- id
			return Settled()
		},
	}

	r := &reconciler{
		gk:                     GroupKind{Kind: "Widget"},
		adapter:                adapter,
		store:                  &allIDsStore{ids: []ObjectID{1, 2, 3}},
		work:                   newWorkQueue(),
		individualPassInterval: time.Hour,
		individualPassRand:     func() float64 { return 0 }, // no offset, so the scan dispatches at once
		backoffFor:             make(map[ObjectID]time.Duration),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	got := map[ObjectID]bool{}
	for range 3 {
		select {
		case id := <-seen:
			got[id] = true
		case <-time.After(testTimeout):
			t.Fatalf("timed out waiting for the admission scan; saw %v", got)
		}
	}
	assert.Equal(t, map[ObjectID]bool{1: true, 2: true, 3: true}, got)

	cancel()
	waitClosed(t, done, "run to exit")
}

// The scan spreads its armings across the whole first interval, so a restart
// does not dispatch the kind at once.
func TestReconcilerIndividualPassAdmissionSpreads(t *testing.T) {
	var n int
	fracs := []float64{0, 0.25, 0.5}
	r := &reconciler{
		gk:                     GroupKind{Kind: "Widget"},
		adapter:                &fakeAdapter{reconcileFn: func(context.Context, ObjectID) ReconcileResult { return Settled() }},
		store:                  &allIDsStore{ids: []ObjectID{1, 2, 3}},
		work:                   newWorkQueue(),
		individualPassInterval: time.Hour,
		individualPassRand:     func() float64 { f := fracs[n]; n++; return f },
		backoffFor:             make(map[ObjectID]time.Duration),
	}

	t.Cleanup(r.work.stop)

	require.NoError(t, r.admitAll(context.Background(), time.Hour))

	// Id 1 drew 0 and is due now; the rest are spread over the interval.
	assert.Nil(t, alarmFor(r.work, 1), "a zero offset dispatches rather than arming")
	for id, want := range map[ObjectID]time.Duration{2: time.Hour / 4, 3: time.Hour / 2} {
		a := alarmFor(r.work, id)
		require.NotNil(t, a, "id %d", id)
		assert.InDelta(t, want, time.Until(a.fireAt), float64(time.Second), "id %d", id)
	}
}

// The scan's only recovery is its own retry.
func TestReconcilerIndividualPassAdmissionRetries(t *testing.T) {
	seen := make(chan ObjectID, 1)
	adapter := &fakeAdapter{
		reconcileFn: func(_ context.Context, id ObjectID) ReconcileResult {
			seen <- id
			return Settled()
		},
	}
	store := &allIDsStore{ids: []ObjectID{7}, failures: 2}

	r := &reconciler{
		gk:                     GroupKind{Kind: "Widget"},
		adapter:                adapter,
		store:                  store,
		work:                   newWorkQueue(),
		individualPassInterval: time.Hour,
		individualPassRand:     func() float64 { return 0 },
		backoffFor:             make(map[ObjectID]time.Duration),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	select {
	case id := <-seen:
		assert.Equal(t, ObjectID(7), id)
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for the admission scan to succeed")
	}
	assert.Equal(t, 3, store.listCalls(), "two failures then the success")

	cancel()
	waitClosed(t, done, "run to exit")
}

// The startup pass and the admission scan list the same kind for the same
// reason. With both on, the scan drops its offset and does the work once.
func TestReconcilerIndividualPassSubsumesTheStartupPass(t *testing.T) {
	seen := make(chan ObjectID, 2)
	adapter := &fakeAdapter{
		reconcileFn: func(_ context.Context, id ObjectID) ReconcileResult {
			seen <- id
			return Settled()
		},
	}
	store := &allIDsStore{ids: []ObjectID{1, 2}}

	r := &reconciler{
		gk:                     GroupKind{Kind: "Widget"},
		adapter:                adapter,
		store:                  store,
		work:                   newWorkQueue(),
		startupFullPass:        true,
		individualPassInterval: time.Hour,
		// A source that would spread the armings; the startup pass overrides it.
		individualPassRand: func() float64 { return 0.5 },
		backoffFor:         make(map[ObjectID]time.Duration),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	for range 2 {
		select {
		case <-seen:
		case <-time.After(testTimeout):
			t.Fatal("timed out waiting for the startup dispatch")
		}
	}
	cancel()
	waitClosed(t, done, "run to exit")

	// Counted after the loop exits, so the scan goroutine has finished either
	// way and the count is not a race.
	assert.Equal(t, 1, store.listCalls(), "the scan is the startup pass, not a second listing")
}

// The scan retries until it succeeds or the reconciler stops. run waits on it,
// so a scan that ignored the cancel would hold a stopped beehive open.
func TestReconcilerIndividualPassAdmissionStopsWithTheReconciler(t *testing.T) {
	const alwaysFails = math.MaxInt
	store := &allIDsStore{ids: []ObjectID{1}, failures: alwaysFails, listed: make(chan struct{})}

	r := &reconciler{
		gk:                     GroupKind{Kind: "Widget"},
		adapter:                &fakeAdapter{reconcileFn: func(context.Context, ObjectID) ReconcileResult { return Settled() }},
		store:                  store,
		work:                   newWorkQueue(),
		individualPassInterval: time.Hour,
		backoffFor:             make(map[ObjectID]time.Duration),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(r, ctx)

	waitClosed(t, store.listed, "the first admission attempt")
	cancel()
	waitClosed(t, done, "run to exit while the scan is still retrying")
}

// The scan runs beside the workers, so it can reach an id whose startup pass
// already scheduled itself. A boot-time offset must not displace what that pass
// asked for.
func TestReconcilerIndividualPassAdmissionYieldsToALivePass(t *testing.T) {
	r := &reconciler{
		gk:                     GroupKind{Kind: "Widget"},
		store:                  &allIDsStore{ids: []ObjectID{1, 2, 3}},
		work:                   newWorkQueue(),
		individualPassInterval: time.Hour,
		individualPassRand:     func() float64 { return 0.5 },
		maxRetryInterval:       time.Hour,
		baseRetryInterval:      time.Minute,
		backoffFor:             make(map[ObjectID]time.Duration),
	}
	t.Cleanup(r.work.stop)
	// 1 asked to come back soon, 2 failed and is on its ladder; 3 never ran.
	r.scheduleNext(1, Settled().RequeueAfter(time.Second))
	r.scheduleNext(2, Fail(errors.New("boom")))

	require.NoError(t, r.admitAll(context.Background(), time.Hour))

	one := alarmFor(r.work, 1)
	require.NotNil(t, one)
	assert.InDelta(t, time.Second, time.Until(one.fireAt), float64(time.Second), "the controller's own schedule survives")
	two := alarmFor(r.work, 2)
	require.NotNil(t, two)
	assert.Equal(t, alarmBackoff, two.kind, "the failure keeps its ladder")
	three := alarmFor(r.work, 3)
	require.NotNil(t, three)
	assert.InDelta(t, time.Hour/2, time.Until(three.fireAt), float64(time.Second), "an unscheduled id is admitted")
}

// RequeueAfter(0) means "call me as soon as the floor allows", so it dispatches
// rather than arming — the individual pass must not turn that into a delay.
func TestReconcilerIndividualPassYieldsToRequeueAfterZero(t *testing.T) {
	r := &reconciler{
		work:                   newWorkQueue(),
		individualPassInterval: time.Hour,
		backoffFor:             make(map[ObjectID]time.Duration),
	}
	t.Cleanup(r.work.stop)

	r.scheduleNext(1, Settled().RequeueAfter(0))

	assert.Nil(t, alarmFor(r.work, 1), "no alarm: the id is dispatchable now")
	id, ok := r.work.get()
	require.True(t, ok)
	assert.Equal(t, ObjectID(1), id)
}

// A reconciler with a store and no work queue is the shape log()'s guard exists
// for. Listing for it enqueues nothing rather than panicking.
func TestReconcilerEnqueueWithoutAWorkQueue(t *testing.T) {
	r := &reconciler{gk: GroupKind{Kind: "Widget"}, store: &allIDsStore{ids: []ObjectID{1}}}

	assert.NotPanics(t, func() { r.enqueueAll(context.Background()) })
	assert.NotPanics(t, func() {
		require.NoError(t, r.admitAll(context.Background(), time.Hour))
	})
}
