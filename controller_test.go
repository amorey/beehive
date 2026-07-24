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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/beehive/internal/storeapi"
)

func TestControllerClientDeleteFinalizer(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)

	cc, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "hello"}, WithFinalizers("a", "b"))
	require.NoError(t, err)

	require.NoError(t, cc.DeleteFinalizer(ctx, obj.ID, "a"))
	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, got.Finalizers, "finalizer removed via ControllerClient")
}

// TestWriteStampsSchemaVersions verifies the lazy stamp-on-write half of the
// migrator model: with a migrator registered, a spec write (Create) stamps the
// migrator's current spec version and a controller status write stamps its
// current status version — each column independently. A kind with no migrator
// stamps 0 for both, the backward-compatible default.
func TestWriteStampsSchemaVersions(t *testing.T) {
	ctx := context.Background()

	t.Run("registered migrator stamps current versions", func(t *testing.T) {
		store := newClientTestStore(t)
		bh, err := New(store)
		require.NoError(t, err)

		cc, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{},
			WithMigrator(&fakeMigrator{specVersion: 4, statusVersion: 9}))
		require.NoError(t, err)

		client := NewClient[cSpec, cStatus](bh, clientTestGK)
		obj, err := client.Create(ctx, cSpec{Val: "hello"})
		require.NoError(t, err)

		// Spec write (Create) stamped the spec version; status untouched (still 0).
		raw, err := store.GetObject(ctx, obj.ID)
		require.NoError(t, err)
		assert.Equal(t, 4, raw.SpecVersion, "Create stamps the migrator's spec version")
		assert.Equal(t, 0, raw.StatusVersion, "no status written yet")

		// Controller status write stamps the status version, spec unchanged.
		require.NoError(t, cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"}))
		raw, err = store.GetObject(ctx, obj.ID)
		require.NoError(t, err)
		assert.Equal(t, 4, raw.SpecVersion, "status write must not touch spec version")
		assert.Equal(t, 9, raw.StatusVersion, "UpdateStatus stamps the migrator's status version")
	})

	t.Run("no migrator stamps 0 (backward compatible)", func(t *testing.T) {
		store := newClientTestStore(t)
		bh, err := New(store)
		require.NoError(t, err)

		cc, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
		require.NoError(t, err)

		client := NewClient[cSpec, cStatus](bh, clientTestGK)
		obj, err := client.Create(ctx, cSpec{Val: "hello"})
		require.NoError(t, err)
		require.NoError(t, cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"}))

		raw, err := store.GetObject(ctx, obj.ID)
		require.NoError(t, err)
		assert.Zero(t, raw.SpecVersion, "no migrator => spec version stays 0")
		assert.Zero(t, raw.StatusVersion, "no migrator => status version stays 0")
	})
}

func TestControllerClientUpdateStatus(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)

	cc, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	// Create an object and update its status via the ControllerClient.
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)

	err = cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"})
	require.NoError(t, err)

	// Status must now be visible through the client.
	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status)
	assert.Equal(t, "done", got.Status.Val)
	require.NotNil(t, got.ObservedGeneration)
	assert.Equal(t, obj.Generation, *got.ObservedGeneration)
}

// TestControllerClientUpdateStatusNoOpIsSilent pins the property downstream
// controllers rely on: reporting the same status again produces no Modified
// frame on the watch, so a dependent that free-rides on a status change isn't
// woken by an unchanged poll. A controller can therefore report unconditionally
// instead of hand-rolling an equality guard.
func TestControllerClientUpdateStatusNoOpIsSilent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	cc, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)
	require.NoError(t, cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"}))

	ch, err := client.WatchList(ctx)
	require.NoError(t, err)
	select { // snapshot
	case ev := <-ch:
		require.Equal(t, Added, ev.Type)
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for the snapshot event")
	}

	// Same status: silent.
	require.NoError(t, cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"}))
	select {
	case ev := <-ch:
		t.Fatalf("unchanged status must not emit, got %v", ev.Type)
	case <-time.After(100 * time.Millisecond):
	}

	// A real change still flows.
	require.NoError(t, cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "changed"}))
	select {
	case ev := <-ch:
		assert.Equal(t, Modified, ev.Type)
		require.NotNil(t, ev.Object.Status)
		assert.Equal(t, "changed", ev.Object.Status.Val)
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for the changed-status event")
	}
}

// TestControllerClientWithin verifies the opt-in atomicity surface: writes made
// inside Within commit together on a nil return and roll back together on error,
// with the nested ControllerClient writes joining the one transaction.
func TestControllerClientWithin(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	cc := &controllerClientImpl[cStatus]{bh: bh, gk: clientTestGK}
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	// Rollback: an error from fn discards every write it made.
	sentinel := errors.New("boom")
	err = cc.Within(ctx, func(ctx context.Context) error {
		if err := cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "rolled-back"}); err != nil {
			return err
		}
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)
	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Nil(t, got.Status, "writes inside a Within that errored must roll back")

	// Commit: a nil return persists every write atomically.
	require.NoError(t, cc.Within(ctx, func(ctx context.Context) error {
		if err := cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "committed"}); err != nil {
			return err
		}
		return cc.SetCondition(ctx, obj.ID, Condition{Type: "Ready", Status: ConditionTrue})
	}))
	got, err = client.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status)
	assert.Equal(t, "committed", got.Status.Val)
	assert.NotNil(t, findCondition(got.Conditions, "Ready"))
}

// RecordEvent writes an aggregated run through the store, marshaling EventSpec's
// Detail; the run reads back with the mapped fields and a decodable payload.
func TestControllerClientRecordEvent(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	cc := &controllerClientImpl[cStatus]{bh: bh, gk: clientTestGK}
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	require.NoError(t, cc.RecordEvent(ctx, obj.ID, EventSpec{
		Category: "connection", Type: EventWarning, Reason: "ProbeFailed",
		Message: "i/o timeout", Detail: probeDetail{Endpoint: "h:443", LatencyMs: 5000},
	}))

	run, err := bh.store.GetLatestEvent(ctx, obj.ID, "connection")
	require.NoError(t, err)
	require.NotNil(t, run)
	assert.Equal(t, "Warning", run.Type)
	assert.Equal(t, "ProbeFailed", run.Reason)
	assert.Equal(t, "i/o timeout", run.Message)
	assert.Equal(t, 1, run.Count)

	detail, err := EventDetail[probeDetail](eventFromRaw(*run))
	require.NoError(t, err)
	assert.Equal(t, probeDetail{Endpoint: "h:443", LatencyMs: 5000}, detail)
}

// RecordEvent surfaces a Detail that cannot be JSON-marshaled, before touching
// the store.
func TestControllerClientRecordEventMarshalError(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)
	cc := &controllerClientImpl[cStatus]{bh: bh, gk: clientTestGK}

	err = cc.RecordEvent(context.Background(), 1, EventSpec{Detail: make(chan int)})
	assert.Error(t, err, "an unmarshalable Detail fails the write")
}

// RecordEvent is kind-folded like the other writes: a controller may not record
// events on an object of another kind.
func TestControllerClientRecordEventWrongKind(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	other := &controllerClientImpl[tStatus]{bh: bh, gk: GroupKind{Kind: "Other"}}
	err = other.RecordEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "X"})
	assert.ErrorIs(t, err, ErrWrongKind)
}

// RecordEvent composes in Within: a run recorded inside a transaction that later
// errors is rolled back with the rest.
func TestControllerClientRecordEventWithinRollback(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	cc := &controllerClientImpl[cStatus]{bh: bh, gk: clientTestGK}
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	sentinel := errors.New("boom")
	err = cc.Within(ctx, func(ctx context.Context) error {
		if err := cc.RecordEvent(ctx, obj.ID, EventSpec{Category: "c", Type: EventNormal, Reason: "Started"}); err != nil {
			return err
		}
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)

	run, err := bh.store.GetLatestEvent(ctx, obj.ID, "c")
	require.NoError(t, err)
	assert.Nil(t, run, "a RecordEvent inside a rolled-back Within must not persist")
}

func TestControllerClientSetAndDeleteCondition(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)

	cc, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)

	require.NoError(t, cc.SetCondition(ctx, obj.ID, Condition{Type: "Ready", Status: ConditionTrue}))
	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, findCondition(got.Conditions, "Ready"))

	require.NoError(t, cc.DeleteCondition(ctx, obj.ID, "Ready"))
	got, err = client.Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Nil(t, findCondition(got.Conditions, "Ready"), "condition removed via ControllerClient")
}

func TestControllerClientAddAndDeleteDependency(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)

	cc, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	from, err := client.Create(ctx, cSpec{Val: "from"})
	require.NoError(t, err)
	to, err := client.Create(ctx, cSpec{Val: "to"})
	require.NoError(t, err)

	require.NoError(t, cc.AddDependency(ctx, from.ID, to.ID, to.ResourceVersion))
	deps, err := bh.store.ListIncomingRefs(ctx, to.ID, RelationDependsOn)
	require.NoError(t, err)
	assert.Equal(t, []Referrer{{ID: from.ID, Group: clientTestGK.Group, Kind: clientTestGK.Kind}}, deps)

	require.NoError(t, cc.DeleteDependency(ctx, from.ID, to.ID))
	deps, err = bh.store.ListIncomingRefs(ctx, to.ID, RelationDependsOn)
	require.NoError(t, err)
	assert.Empty(t, deps, "edge removed via ControllerClient")
}

// addRefTxTrackingStore records whether the ref insert ran inside a Within call,
// so a test can assert AddDependency wraps its endpoint check + insert in one
// transaction. Accessed only from the test goroutine, so it needs no locking.
type addRefTxTrackingStore struct {
	Store
	depth      int
	addRefInTx bool
}

func (s *addRefTxTrackingStore) Within(ctx context.Context, fn func(context.Context) error) error {
	s.depth++
	defer func() { s.depth-- }()
	return s.Store.Within(ctx, fn)
}

func (s *addRefTxTrackingStore) AddRef(ctx context.Context, fromID, toID ObjectID, relation Relation, targetRV int64) (storeapi.AddRefResult, error) {
	s.addRefInTx = s.depth > 0
	return s.Store.AddRef(ctx, fromID, toID, relation, targetRV)
}

// TestControllerClientAddDependencyIsTransactional pins that AddDependency runs its
// endpoint existence check and the ref insert in one transaction (like
// DeleteDependency). AddRef checks then inserts as separate statements, so without
// the transaction a delete interleaving between them would leak a raw FK error
// instead of the store's ErrNotFound contract.
func TestControllerClientAddDependencyIsTransactional(t *testing.T) {
	ctx := context.Background()
	tracking := &addRefTxTrackingStore{Store: newClientTestStore(t)}
	bh, err := New(tracking)
	require.NoError(t, err)

	cc := &controllerClientImpl[cStatus]{bh: bh, gk: clientTestGK}
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	from, err := client.Create(ctx, cSpec{Val: "from"})
	require.NoError(t, err)
	to, err := client.Create(ctx, cSpec{Val: "to"})
	require.NoError(t, err)

	require.NoError(t, cc.AddDependency(ctx, from.ID, to.ID, to.ResourceVersion))
	assert.True(t, tracking.addRefInTx,
		"AddDependency must wrap its endpoint check + insert in one transaction")
}

// declareFixture is the shared setup for the targetResourceVersion tests: a
// running control plane, a dependent, and a target.
//
// The two live in *different kinds* on purpose. The edge is deliberately
// cross-kind, so cc — the ControllerClient the tests declare through — belongs to
// the target's kind, not the dependent's: every test therefore exercises the
// routing rule (the requeue follows fromID's own GroupKind, not the caller's) by
// construction, and a wake misrouted to the declarer's reconciler shows up as a
// dependent that was never woken. It also keeps reconciled free of the target's
// own passes, so the channel carries only what these tests are asserting about.
type declareFixture struct {
	cc          ControllerClient[tStatus] // the target kind's client: a foreign kind to dep
	store       Store
	targetGK    GroupKind
	depClient   Client[tSpec, tStatus] // creates the barrier object, see requireNotRequeued
	dep, target *Object[tSpec, tStatus]
	// witness is a second dependent of dep's kind whose edge to the target exists
	// from the start. It is how moveTarget knows the waker has finished with a
	// change: the waker requeues from its own lookup's results, so the witness
	// reconciling is an effect that cannot precede that lookup — and it proves dep
	// was absent from it. Watching the lookup itself cannot show that; a probe on
	// ListIncomingRefs sees a call, not which change it is for, so one already in
	// flight is indistinguishable from the one under test.
	witness    *Object[tSpec, tStatus]
	reconciled chan *Object[tSpec, tStatus] // dep's kind only
}

func newDeclareFixture(t *testing.T) *declareFixture {
	t.Helper()
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)

	depGK := GroupKind{Kind: "Dependent"}
	f := &declareFixture{
		store:      store,
		targetGK:   GroupKind{Kind: "Target"},
		reconciled: make(chan *Object[tSpec, tStatus], 8),
	}
	// Resync disabled: a requeue that arrives must be the declaration's doing.
	// Single-threaded so dispatch order is the queue's FIFO order, which is what
	// makes requireNotRequeued's barrier exact. That is the default; pinning it
	// here keeps the barrier's precondition visible rather than inherited.
	_, err = Register(bh, depGK, &reconcileCapture{ch: f.reconciled},
		WithResyncInterval(0), WithConcurrency(1))
	require.NoError(t, err)
	f.cc, err = Register(bh, f.targetGK, &noopController[tSpec, tStatus]{}, WithResyncInterval(0))
	require.NoError(t, err)

	f.depClient = NewClient[tSpec, tStatus](bh, depGK)
	f.dep, err = f.depClient.Create(ctx, tSpec{})
	require.NoError(t, err)
	f.witness, err = f.depClient.Create(ctx, tSpec{})
	require.NoError(t, err)
	f.target, err = NewClient[tSpec, tStatus](bh, f.targetGK).Create(ctx, tSpec{})
	require.NoError(t, err)
	// Declared straight through the store: the witness's edge is scaffolding, not a
	// use of the guard under test.
	require.NoError(t, addRef(ctx, store, f.witness.ID, f.target.ID, RelationDependsOn))

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { stop(ctx) })

	// Drain both creation reconciles, so anything arriving afterwards is
	// attributable to what the test does next.
	seen := map[ObjectID]bool{}
	for len(seen) < 2 {
		seen[recv(t, f.reconciled).ID] = true
	}
	require.Equal(t, map[ObjectID]bool{f.dep.ID: true, f.witness.ID: true}, seen)
	return f
}

// moveTarget changes the target and returns the version it held before, i.e. the
// one a decision taken before the change was based on. It returns once the waker
// has resolved this change with no edge to the dependent yet, so the change is
// unclaimed and every later requeue of the dependent is the declaration's doing.
func (f *declareFixture) moveTarget(t *testing.T) int64 {
	t.Helper()
	before := f.target.ResourceVersion
	_, err := f.store.SetCondition(context.Background(), f.targetGK, f.target.ID,
		storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	require.Equal(t, f.witness.ID, recv(t, f.reconciled).ID,
		"the waker resolved this change, and reached only the witness")
	return before
}

// requireRequeued asserts the dependent was requeued.
func (f *declareFixture) requireRequeued(t *testing.T) {
	t.Helper()
	assert.Equal(t, f.dep.ID, recv(t, f.reconciled).ID)
}

// requireNotRequeued asserts the dependent was not requeued — an absence, proven
// with a barrier rather than a deadline. Creating a fresh object of the
// dependent's kind enqueues it strictly after any wake the declaration owed:
// AddDependency's post-commit hook has already run by the time it returns, and
// the queue is FIFO over a single worker. So the dependent, if it had been woken,
// must be dispatched before the barrier — and seeing the barrier first proves it
// never was, with no waiting on the clock.
func (f *declareFixture) requireNotRequeued(t *testing.T) {
	t.Helper()
	barrier, err := f.depClient.Create(context.Background(), tSpec{})
	require.NoError(t, err)
	assert.Equal(t, barrier.ID, recv(t, f.reconciled).ID,
		"the dependent was requeued ahead of the barrier; no wake was owed")
}

// TestAddDependencyWakesWhenTargetMovedSinceRead pins the fix for the
// read-then-declare race: a target that moved past the version the caller read
// requeues the dependent, because the change landed while no edge existed to
// carry it. The declaration goes through the target kind's ControllerClient (see
// declareFixture), so this also pins that the requeue routes by fromID's own
// kind rather than the declarer's.
func TestAddDependencyWakesWhenTargetMovedSinceRead(t *testing.T) {
	f := newDeclareFixture(t)
	asRead := f.moveTarget(t)
	require.NoError(t, f.cc.AddDependency(context.Background(), f.dep.ID, f.target.ID, asRead))
	f.requireRequeued(t)
}

// TestAddDependencyNoWakeWhenTargetUnmoved is the anti-spin case, and the reason
// the check is on the target's version rather than on the edge being new: a
// level-triggered controller re-asserts its edges every pass, and nothing
// throttles a self-sustaining requeue. See the rejected wake-when-the-edge-is-new
// guard in TODO.md.
func TestAddDependencyNoWakeWhenTargetUnmoved(t *testing.T) {
	f := newDeclareFixture(t)
	ctx := context.Background()
	// Re-assert the same edge repeatedly with a current version, as a controller
	// converging on an unchanging target does.
	for range 3 {
		require.NoError(t, f.cc.AddDependency(ctx, f.dep.ID, f.target.ID, f.target.ResourceVersion))
	}
	f.requireNotRequeued(t)
}

// TestAddDependencyRejectsFutureResourceVersion pins the one wrong value the call
// can actually detect. Versions come from one global cursor, so any other object's
// is a plausible-looking int64 — but the target's own only moves forward, so a
// version above it cannot have been read from the target. Left to stand it would
// disable the guard silently and permanently (the comparison can never fire), so
// it is rejected, and the edge rolls back with the transaction rather than
// persisting with an inert guard.
func TestAddDependencyRejectsFutureResourceVersion(t *testing.T) {
	f := newDeclareFixture(t)
	ctx := context.Background()

	err := f.cc.AddDependency(ctx, f.dep.ID, f.target.ID, f.target.ResourceVersion+1)
	require.ErrorIs(t, err, ErrTargetResourceVersionFuture)

	refs, err := f.store.ListIncomingRefs(ctx, f.target.ID, RelationDependsOn)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{f.witness.ID}, referrerIDs(refs), "a rejected declaration leaves no edge")
	f.requireNotRequeued(t)

	// The target's own current version is the boundary, and is accepted.
	require.NoError(t, f.cc.AddDependency(ctx, f.dep.ID, f.target.ID, f.target.ResourceVersion))
}

// TestAddDependencyRejectsFutureResourceVersionNested is the rejection's harder
// case: nested in a caller's Within, whose error the caller swallows. A nested
// Within is a bare fn(ctx) with no transaction of its own, so returning the error
// unwinds nothing — only the caller propagating it can roll anything back. The
// edge must therefore never be written in the first place.
func TestAddDependencyRejectsFutureResourceVersionNested(t *testing.T) {
	f := newDeclareFixture(t)
	ctx := context.Background()

	err := f.cc.Within(ctx, func(ctx context.Context) error {
		if err := f.cc.AddDependency(ctx, f.dep.ID, f.target.ID, f.target.ResourceVersion+1); err != nil {
			return nil // the caller logs and carries on; the outer tx still commits
		}
		return nil
	})
	require.NoError(t, err)

	refs, err := f.store.ListIncomingRefs(ctx, f.target.ID, RelationDependsOn)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{f.witness.ID}, referrerIDs(refs), "a rejected declaration must leave no edge, committed or not")
}

// TestAddDependencyStaleResourceVersionWakesAtMostOnce pins the wake's
// once-per-edge bound against the caller who gets it wrong. A version that never
// advances — cached across passes, or read once and reused — compares as "moved"
// forever, so gating on that alone would re-fire every pass, and nothing throttles
// the result. The edge-new half of the conjunction is what stops it: the second
// declaration inserts nothing, so it wakes nothing, however stale the version.
func TestAddDependencyStaleResourceVersionWakesAtMostOnce(t *testing.T) {
	f := newDeclareFixture(t)
	ctx := context.Background()
	stale := f.moveTarget(t)

	// First declaration: the edge is new and the target moved, so this is the
	// requeue the guard exists for.
	require.NoError(t, f.cc.AddDependency(ctx, f.dep.ID, f.target.ID, stale))
	f.requireRequeued(t)

	// Every later pass re-asserts the same edge with the same stale version.
	for range 3 {
		require.NoError(t, f.cc.AddDependency(ctx, f.dep.ID, f.target.ID, stale))
	}
	f.requireNotRequeued(t)
}

// TestAddDependencyZeroResourceVersionSkipsCheck pins the sentinel: 0 is "no
// opinion", the correct value when the edge is declared before the target is
// read. It must not mean "wake unconditionally" — that would reproduce the spin
// above in any caller that passes the zero value.
func TestAddDependencyZeroResourceVersionSkipsCheck(t *testing.T) {
	f := newDeclareFixture(t)
	f.moveTarget(t)
	require.NoError(t, f.cc.AddDependency(context.Background(), f.dep.ID, f.target.ID, 0))
	f.requireNotRequeued(t)
}

// TestAddDependencyNoWakeOnRollback pins that the wake is registered post-commit:
// a declaration rolled back by the controller's enclosing transaction never
// happened, so there is no edge to have missed a change and nothing to requeue.
func TestAddDependencyNoWakeOnRollback(t *testing.T) {
	f := newDeclareFixture(t)
	ctx := context.Background()
	asRead := f.moveTarget(t)

	err := f.cc.Within(ctx, func(ctx context.Context) error {
		if err := f.cc.AddDependency(ctx, f.dep.ID, f.target.ID, asRead); err != nil {
			return err
		}
		return errBoom
	})
	require.ErrorIs(t, err, errBoom)

	refs, err := f.store.ListIncomingRefs(ctx, f.target.ID, RelationDependsOn)
	require.NoError(t, err)
	require.Equal(t, []ObjectID{f.witness.ID}, referrerIDs(refs), "the rolled-back declaration left no edge")
	f.requireNotRequeued(t)
}

func TestControllerClientHasIncomingRefs(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)

	cc, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	owner, err := client.Create(ctx, cSpec{Val: "owner"})
	require.NoError(t, err)
	child, err := client.Create(ctx, cSpec{Val: "child"}, WithOwner(owner.ID))
	require.NoError(t, err)

	has, err := cc.HasIncomingRefs(ctx, owner.ID)
	require.NoError(t, err)
	assert.True(t, has, "owner is referenced by the child")

	has, err = cc.HasIncomingRefs(ctx, child.ID)
	require.NoError(t, err)
	assert.False(t, has, "nothing references the child")
}

// TestControllerClientWritesScopedToKind verifies that a ControllerClient's
// status/condition/finalizer writes refuse an id belonging to another kind: a
// controller for "Widget" must not be able to persist its Status (or mutate
// conditions/finalizers) on a "Gadget" row, which would corrupt that kind's
// rows. AddDependency/HasIncomingRefs are intentionally cross-kind and not guarded.
func TestControllerClientWritesScopedToKind(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	cc, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{}) // controller for "Widget"
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	// A "Gadget" is a foreign kind to this controller. Give it a finalizer so the
	// DeleteFinalizer attempt has a target to (fail to) remove.
	gadgets := NewClient[cSpec, cStatus](bh, GroupKind{Kind: "Gadget"})
	gadget, err := gadgets.Create(ctx, cSpec{Val: "v1"}, WithFinalizers("f"))
	require.NoError(t, err)

	require.ErrorIs(t, cc.UpdateStatus(ctx, gadget.ID, 1, cStatus{Val: "hijacked"}), ErrWrongKind)
	require.ErrorIs(t, cc.SetCondition(ctx, gadget.ID, Condition{Type: "Ready", Status: ConditionTrue}), ErrWrongKind)
	require.ErrorIs(t, cc.DeleteCondition(ctx, gadget.ID, "Ready"), ErrWrongKind)
	require.ErrorIs(t, cc.DeleteFinalizer(ctx, gadget.ID, "f"), ErrWrongKind)

	// The Gadget is untouched: no status, no conditions, finalizer intact.
	got, err := gadgets.Get(ctx, gadget.ID)
	require.NoError(t, err)
	assert.Nil(t, got.Status, "foreign status write rejected")
	assert.Empty(t, got.Conditions, "foreign condition write rejected")
	assert.Equal(t, []string{"f"}, got.Finalizers, "foreign finalizer write rejected")
}

// failHasIncomingRefsStore returns an error from HasIncomingRefs.
type failHasIncomingRefsStore struct {
	fakeStore
}

func (s *failHasIncomingRefsStore) HasIncomingRefs(context.Context, ObjectID) (bool, error) {
	return false, errBoom
}

func TestControllerClientHasIncomingRefsStoreError(t *testing.T) {
	bh, err := New(&failHasIncomingRefsStore{})
	require.NoError(t, err)
	cc := &controllerClientImpl[tStatus]{bh: bh, gk: GroupKind{Kind: "T"}}
	_, err = cc.HasIncomingRefs(context.Background(), 1)
	require.ErrorIs(t, err, errBoom)
}

// failAddRefStore returns an error from the ref insert.
type failAddRefStore struct {
	fakeStore
}

func (s *failAddRefStore) AddRef(context.Context, ObjectID, ObjectID, Relation, int64) (storeapi.AddRefResult, error) {
	return storeapi.AddRefResult{}, errBoom
}

func TestControllerClientAddDependencyStoreError(t *testing.T) {
	bh, err := New(&failAddRefStore{})
	require.NoError(t, err)
	cc := &controllerClientImpl[tStatus]{bh: bh, gk: GroupKind{Kind: "T"}}
	err = cc.AddDependency(context.Background(), 1, 2, 0)
	require.ErrorIs(t, err, errBoom)
}

// kindTStore runs Within inline and answers GetObject with a row of kind "T", so
// tests reach the write path under test. Embed it in a double that overrides the
// specific write.
type kindTStore struct {
	fakeStore
}

func (s *kindTStore) Within(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}
func (s *kindTStore) GetObject(_ context.Context, id ObjectID) (*RawObject, error) {
	return &RawObject{ID: id, Kind: "T"}, nil
}

// failUpdateStatusStore returns an error from UpdateStatus.
type failUpdateStatusStore struct {
	kindTStore
}

func (s *failUpdateStatusStore) UpdateStatus(_ context.Context, _ GroupKind, _ ObjectID, _ int64, _ []byte, _ int) (*RawObject, error) {
	return nil, errBoom
}

// errStatusMarshaler is a Status type whose JSON marshaling always fails.
type errStatusMarshaler struct{}

func (errStatusMarshaler) MarshalJSON() ([]byte, error) { return nil, errBoom }

func TestControllerClientUpdateStatusMarshalError(t *testing.T) {
	bh, err := New(&kindTStore{})
	require.NoError(t, err)
	cc := &controllerClientImpl[errStatusMarshaler]{bh: bh, gk: GroupKind{Kind: "T"}}
	err = cc.UpdateStatus(context.Background(), 1, 1, errStatusMarshaler{})
	require.Error(t, err)
}

func TestControllerClientUpdateStatusStoreError(t *testing.T) {
	bh, err := New(&failUpdateStatusStore{})
	require.NoError(t, err)
	cc := &controllerClientImpl[tStatus]{bh: bh, gk: GroupKind{Kind: "T"}}
	err = cc.UpdateStatus(context.Background(), 1, 1, tStatus{})
	require.Error(t, err)
}

// failDeleteRefStore returns an error from DeleteRef (Within runs fn inline).
type failDeleteRefStore struct {
	fakeStore
}

func (s *failDeleteRefStore) DeleteRef(context.Context, ObjectID, ObjectID, Relation) error {
	return errBoom
}

// TestControllerClientDeleteDependencyDeleteRefError covers the DeleteRef failure
// branch: the edge removal itself fails, so the whole DeleteDependency errors.
func TestControllerClientDeleteDependencyDeleteRefError(t *testing.T) {
	bh, err := New(&failDeleteRefStore{})
	require.NoError(t, err)
	cc := &controllerClientImpl[tStatus]{bh: bh, gk: GroupKind{Kind: "T"}}
	err = cc.DeleteDependency(context.Background(), 1, 2)
	require.ErrorIs(t, err, errBoom)
}

// metaDeleteDepStore lets a DeleteDependency test control what GetObjectMeta
// returns after the edge is dropped. DeleteRef succeeds; the rest defaults to the
// fakeStore (Within inline, no-ops).
type metaDeleteDepStore struct {
	fakeStore
	meta    *RawObject
	metaErr error
}

func (s *metaDeleteDepStore) DeleteRef(context.Context, ObjectID, ObjectID, Relation) error {
	return nil
}
func (s *metaDeleteDepStore) GetObjectMeta(context.Context, ObjectID) (*RawObject, error) {
	return s.meta, s.metaErr
}

// TestControllerClientDeleteDependencyTargetGone covers the post-edge re-check
// when the target is already gone: GetObjectMeta reports ErrNotFound, which is
// swallowed (nothing to wake). The wake collector must be present to reach it.
func TestControllerClientDeleteDependencyTargetGone(t *testing.T) {
	bh, err := New(&metaDeleteDepStore{metaErr: ErrNotFound})
	require.NoError(t, err)
	cc := &controllerClientImpl[tStatus]{bh: bh, gk: GroupKind{Kind: "T"}}

	wakes := &pendingWakes{}
	ctx := withPendingWakes(context.Background(), wakes)
	require.NoError(t, cc.DeleteDependency(ctx, 1, 2))
	assert.Empty(t, wakes.targets, "a gone target schedules no wake")
}

// TestControllerClientDeleteDependencyMetaError covers GetObjectMeta failing with
// a non-ErrNotFound error after the edge is dropped: it propagates out.
func TestControllerClientDeleteDependencyMetaError(t *testing.T) {
	bh, err := New(&metaDeleteDepStore{metaErr: errBoom})
	require.NoError(t, err)
	cc := &controllerClientImpl[tStatus]{bh: bh, gk: GroupKind{Kind: "T"}}

	ctx := withPendingWakes(context.Background(), &pendingWakes{})
	err = cc.DeleteDependency(ctx, 1, 2)
	require.ErrorIs(t, err, errBoom)
}

// TestControllerClientDeleteDependencyWakesFinalizingTarget covers the happy wake
// path: the freed target is itself finalizing, so it's appended to the collector
// for a post-commit GC re-check.
func TestControllerClientDeleteDependencyWakesFinalizingTarget(t *testing.T) {
	now := time.Now()
	meta := &RawObject{ID: 2, Group: "g", Kind: "K", DeletionRequestedAt: &now}
	bh, err := New(&metaDeleteDepStore{meta: meta})
	require.NoError(t, err)
	cc := &controllerClientImpl[tStatus]{bh: bh, gk: GroupKind{Kind: "T"}}

	wakes := &pendingWakes{}
	ctx := withPendingWakes(context.Background(), wakes)
	require.NoError(t, cc.DeleteDependency(ctx, 1, 2))
	assert.Equal(t, []Referrer{{ID: 2, Group: "g", Kind: "K"}}, wakes.targets,
		"a finalizing freed target is scheduled for a GC re-check")
}

// TestControllerClientDeleteDependencyTargetAliveNotFinalizing covers the case
// where the freed target still exists and is not finalizing: nothing is scheduled
// to wake (it's a live object, GC has no interest in it).
func TestControllerClientDeleteDependencyTargetAliveNotFinalizing(t *testing.T) {
	meta := &RawObject{ID: 2, Group: "g", Kind: "K"} // DeletionRequestedAt nil
	bh, err := New(&metaDeleteDepStore{meta: meta})
	require.NoError(t, err)
	cc := &controllerClientImpl[tStatus]{bh: bh, gk: GroupKind{Kind: "T"}}

	wakes := &pendingWakes{}
	ctx := withPendingWakes(context.Background(), wakes)
	require.NoError(t, cc.DeleteDependency(ctx, 1, 2))
	assert.Empty(t, wakes.targets, "a live, non-finalizing target schedules no wake")
}

// TestControllerClientDeleteDependencyNoWakesOutsideReconcile covers the early
// return when there's no collector on the ctx (called outside a reconcile):
// GetObjectMeta is never reached, so even a panicking GetObjectMeta is fine.
func TestControllerClientDeleteDependencyNoWakesOutsideReconcile(t *testing.T) {
	bh, err := New(&metaDeleteDepStore{metaErr: errBoom})
	require.NoError(t, err)
	cc := &controllerClientImpl[tStatus]{bh: bh, gk: GroupKind{Kind: "T"}}

	// No withPendingWakes: pendingWakesFrom(ctx) is nil, so it returns before the
	// GetObjectMeta call that would otherwise fail.
	require.NoError(t, cc.DeleteDependency(context.Background(), 1, 2))
}

func TestControllerClientReadRefs(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	cc, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	owner, err := client.Create(ctx, cSpec{Val: "owner"})
	require.NoError(t, err)
	child, err := client.Create(ctx, cSpec{Val: "child"}, WithOwner(owner.ID))
	require.NoError(t, err)
	require.NoError(t, addRef(ctx, store, child.ID, owner.ID, RelationDependsOn))

	ref, ok, err := cc.GetOwner(ctx, child.ID)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, owner.ID, ref.ID)

	deps, err := cc.ListDependencies(ctx, child.ID)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{owner.ID}, refObjectIDs(deps))

	dependents, err := cc.ListDependents(ctx, owner.ID)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{child.ID}, refObjectIDs(dependents))

	owned, err := cc.ListOwned(ctx, owner.ID)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{child.ID}, refObjectIDs(owned))
}
