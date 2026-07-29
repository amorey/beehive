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

	require.NoError(t, cc.FinalizersDelete(ctx, obj.ID, "a"))
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
		raw, err := store.ObjectsGet(ctx, obj.ID)
		require.NoError(t, err)
		assert.Equal(t, 4, raw.SpecVersion, "Create stamps the migrator's spec version")
		assert.Equal(t, 0, raw.StatusVersion, "no status written yet")

		// Controller status write stamps the status version, spec unchanged.
		require.NoError(t, cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"}))
		raw, err = store.ObjectsGet(ctx, obj.ID)
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

		raw, err := store.ObjectsGet(ctx, obj.ID)
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
	bh, err := New(newClientTestStore(t), fast()...)
	require.NoError(t, err)

	cc, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)
	require.NoError(t, cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"}))

	ch, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)
	select { // snapshot
	case ev := <-ch:
		require.Equal(t, Added, ev.Type)
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for the snapshot event")
	}

	// Same status: silent. Checked at the mechanism rather than by waiting out a
	// grace period on the channel — the watch emits off resource_version, so a
	// no-op write that leaves it alone is one the poller cannot see, whenever it
	// happens to look. The channel assertion below is the second half: a stray
	// frame for this write would have to arrive before the real change's.
	before, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NoError(t, cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"}))
	after, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, before.ResourceVersion, after.ResourceVersion,
		"an unchanged status bumped resource_version, which is what the watch emits on")

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
		return cc.ConditionsSet(ctx, obj.ID, Condition{Type: "Ready", Status: ConditionTrue})
	}))
	got, err = client.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status)
	assert.Equal(t, "committed", got.Status.Val)
	assert.NotNil(t, findCondition(got.Conditions, "Ready"))
}

// EventsRecord writes an aggregated run through the store, marshaling EventSpec's
// Detail; the run reads back with the mapped fields and a decodable payload.
func TestControllerClientRecordEvent(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	cc := &controllerClientImpl[cStatus]{bh: bh, gk: clientTestGK}
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	require.NoError(t, cc.EventsRecord(ctx, obj.ID, EventSpec{
		Category: "connection", Type: EventWarning, Reason: "ProbeFailed",
		Message: "i/o timeout", Detail: probeDetail{Endpoint: "h:443", LatencyMs: 5000},
	}))

	run, err := bh.store.EventsGetLatest(ctx, obj.ID, "connection")
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

// EventsRecord surfaces a Detail that cannot be JSON-marshaled, before touching
// the store.
func TestControllerClientRecordEventMarshalError(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)
	cc := &controllerClientImpl[cStatus]{bh: bh, gk: clientTestGK}

	err = cc.EventsRecord(context.Background(), 1, EventSpec{Detail: make(chan int)})
	assert.Error(t, err, "an unmarshalable Detail fails the write")
}

// EventsRecord is kind-folded like the other writes: a controller may not record
// events on an object of another kind.
func TestControllerClientRecordEventWrongKind(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	other := &controllerClientImpl[tStatus]{bh: bh, gk: GroupKind{Kind: "Other"}}
	err = other.EventsRecord(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "X"})
	assert.ErrorIs(t, err, ErrWrongKind)
}

// EventsRecord composes in Within: a run recorded inside a transaction that later
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
		if err := cc.EventsRecord(ctx, obj.ID, EventSpec{Category: "c", Type: EventNormal, Reason: "Started"}); err != nil {
			return err
		}
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)

	run, err := bh.store.EventsGetLatest(ctx, obj.ID, "c")
	require.NoError(t, err)
	assert.Nil(t, run, "a EventsRecord inside a rolled-back Within must not persist")
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

	require.NoError(t, cc.ConditionsSet(ctx, obj.ID, Condition{Type: "Ready", Status: ConditionTrue}))
	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, findCondition(got.Conditions, "Ready"))

	require.NoError(t, cc.ConditionsDelete(ctx, obj.ID, "Ready"))
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

	require.NoError(t, cc.DependenciesAdd(ctx, from.ID, to.ID, to.ResourceVersion))
	deps, err := bh.store.EdgesListIncoming(ctx, to.ID, RelationDependsOn)
	require.NoError(t, err)
	assert.Equal(t, []ObjectRef{{ID: from.ID, Group: clientTestGK.Group, Kind: clientTestGK.Kind}}, deps)

	require.NoError(t, cc.DependenciesDelete(ctx, from.ID, to.ID))
	deps, err = bh.store.EdgesListIncoming(ctx, to.ID, RelationDependsOn)
	require.NoError(t, err)
	assert.Empty(t, deps, "edge removed via ControllerClient")
}

// TestAddDependencyAcceptsCycle records that beehive lets a caller declare a
// cycle. It is the tripwire for the deferred fix in TODO.md's cycle entry: the
// candidate that rejects a cycle-closing edge at declare time would make one of
// these calls start returning an error, so whoever builds it trips a test that
// states today's answer rather than discovering it.
//
// The waker's own cycle test cannot serve here — it drives edges through a fake,
// so a declare-time check is invisible to it.
//
// A zero targetResourceVersion is "no opinion", which keeps this about acceptance
// of the edge and out of the raced-declare guard's way.
func TestAddDependencyAcceptsCycle(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)

	gk := GroupKind{Kind: "Widget"}
	cc, err := Register(bh, gk, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)
	client := NewClient[tSpec, tStatus](bh, gk)
	a, err := client.Create(ctx, tSpec{})
	require.NoError(t, err)
	b, err := client.Create(ctx, tSpec{})
	require.NoError(t, err)

	require.NoError(t, cc.DependenciesAdd(ctx, a.ID, b.ID, 0))
	require.NoError(t, cc.DependenciesAdd(ctx, b.ID, a.ID, 0), "a cycle-closing edge is accepted today")
	require.NoError(t, cc.DependenciesAdd(ctx, a.ID, a.ID, 0), "and so is a self-edge")
}

// declareFixture is the shared setup for the targetResourceVersion tests: a
// dependent, a target, and a control plane that is deliberately NOT started.
//
// Not started because the thing under test is now durable rather than in-flight.
// A declaration that decides a wake is owed records it in reconcile_owed, and a
// running reconcile loop would drain that count out from under the assertion —
// so the tests read the count directly, which is both the mechanism and the only
// thing that survives a process restart. That the owed-pass tick then turns the
// count into a reconcile is covered end to end by
// TestDependencyRequeueRaceOnDeclare.
//
// The two objects live in *different kinds* on purpose. The edge is deliberately
// cross-kind, so cc — the ControllerClient the tests declare through — belongs to
// the target's kind, not the dependent's: every test therefore exercises the
// routing rule (the stamp follows fromID's own GroupKind, not the caller's) by
// construction, and a stamp misrouted to the declarer's kind shows up as a
// dependent that owes nothing.
type declareFixture struct {
	cc          ControllerClient[tStatus] // the target kind's client: a foreign kind to dep
	store       Store
	targetGK    GroupKind
	depGK       GroupKind
	dep, target *Object[tSpec, tStatus]
	// witness is a second dependent of dep's kind whose edge to the target exists
	// from the start, so the "a rejected declaration leaves no edge" assertions can
	// name what the edge listing should still contain.
	witness *Object[tSpec, tStatus]
}

func newDeclareFixture(t *testing.T) *declareFixture {
	t.Helper()
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)

	f := &declareFixture{
		store:    store,
		targetGK: GroupKind{Kind: "Target"},
		depGK:    GroupKind{Kind: "Dependent"},
	}
	_, err = Register(bh, f.depGK, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)
	f.cc, err = Register(bh, f.targetGK, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	depClient := NewClient[tSpec, tStatus](bh, f.depGK)
	f.dep, err = depClient.Create(ctx, tSpec{})
	require.NoError(t, err)
	f.witness, err = depClient.Create(ctx, tSpec{})
	require.NoError(t, err)
	f.target, err = NewClient[tSpec, tStatus](bh, f.targetGK).Create(ctx, tSpec{})
	require.NoError(t, err)
	// Declared straight through the store: the witness's edge is scaffolding, not a
	// use of the guard under test, and going through the store leaves it unstamped.
	require.NoError(t, addEdge(ctx, store, f.witness.ID, f.target.ID, RelationDependsOn))

	return f
}

// moveTarget changes the target and returns the version it held before, i.e. the
// one a decision taken before the change was based on.
func (f *declareFixture) moveTarget(t *testing.T) int64 {
	t.Helper()
	before := f.target.ResourceVersion
	_, err := f.store.ConditionsSet(context.Background(), f.targetGK, f.target.ID,
		storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	return before
}

// owed returns the dependent kind's owed-wake listing — what the owed-pass tick
// would drain.
func (f *declareFixture) owed(t *testing.T) []ObjectID {
	t.Helper()
	ids, err := f.store.ReconcileOwedListIDs(context.Background(), f.depGK)
	require.NoError(t, err)
	return ids
}

// requireOwed asserts the declaration recorded a wake for the dependent.
func (f *declareFixture) requireOwed(t *testing.T) {
	t.Helper()
	assert.Equal(t, []ObjectID{f.dep.ID}, f.owed(t))
}

// owedCount returns how many wakes the dependent owes — the count itself, not
// just its presence in the listing, so a test can tell "stamped once" from
// "stamped again on every pass".
func (f *declareFixture) owedCount(t *testing.T) int64 {
	t.Helper()
	meta, err := f.store.ObjectsGetMeta(context.Background(), f.dep.ID)
	require.NoError(t, err)
	return meta.ReconcileOwed
}

// requireNotOwed asserts the declaration recorded nothing. It is an exact
// assertion on the whole listing, not a deadline: the count is durable, so its
// absence needs no waiting to prove.
func (f *declareFixture) requireNotOwed(t *testing.T) {
	t.Helper()
	assert.Empty(t, f.owed(t), "no wake was owed")
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
	require.NoError(t, f.cc.DependenciesAdd(context.Background(), f.dep.ID, f.target.ID, asRead))
	f.requireOwed(t)
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
		require.NoError(t, f.cc.DependenciesAdd(ctx, f.dep.ID, f.target.ID, f.target.ResourceVersion))
	}
	f.requireNotOwed(t)
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

	err := f.cc.DependenciesAdd(ctx, f.dep.ID, f.target.ID, f.target.ResourceVersion+1)
	require.ErrorIs(t, err, ErrTargetResourceVersionFuture)

	refs, err := f.store.EdgesListIncoming(ctx, f.target.ID, RelationDependsOn)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{f.witness.ID}, objectRefIDs(refs), "a rejected declaration leaves no edge")
	f.requireNotOwed(t)

	// The target's own current version is the boundary, and is accepted.
	require.NoError(t, f.cc.DependenciesAdd(ctx, f.dep.ID, f.target.ID, f.target.ResourceVersion))
}

// TestAddDependencyStampRidesRefsAdd pins that the durable stamp is not a second
// store call sequenced after the edge. Were it one, a caller nested in its own
// Within could handle this method's error and commit the edge with no wake — a
// dependent stranded on a stale read, the very race the version claim closes —
// because a nested Within is a bare fn(ctx) that unwinds nothing.
//
// That the stamp *cannot* be a second call is now structural: the Store interface
// carries no standalone increment, so nothing on this path could issue one. What
// remains to check is the other half — that folding it into EdgesAdd actually stamps —
// which is what the two assertions below do: edge and wake land together.
func TestAddDependencyStampRidesRefsAdd(t *testing.T) {
	ctx := context.Background()
	real := newClientTestStore(t)

	bh, err := New(real)
	require.NoError(t, err)
	gk := GroupKind{Kind: "Widget"}
	cc, err := Register(bh, gk, &noopController[tSpec, tStatus]{}, WithFullPassInterval(0))
	require.NoError(t, err)

	client := NewClient[tSpec, tStatus](bh, gk)
	dep, err := client.Create(ctx, tSpec{})
	require.NoError(t, err)
	target, err := client.Create(ctx, tSpec{})
	require.NoError(t, err)

	stale := target.ResourceVersion
	_, err = real.ConditionsSet(ctx, gk, target.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	// Conjunction fires: new edge, target moved past stale.
	require.NoError(t, cc.DependenciesAdd(ctx, dep.ID, target.ID, stale))

	refs, err := real.EdgesListIncoming(ctx, target.ID, RelationDependsOn)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{dep.ID}, objectRefIDs(refs), "the edge landed")

	owed, err := real.ReconcileOwedListIDs(ctx, gk)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{dep.ID}, owed, "and the stamp landed with it, inside EdgesAdd")
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
		if err := f.cc.DependenciesAdd(ctx, f.dep.ID, f.target.ID, f.target.ResourceVersion+1); err != nil {
			return nil // the caller logs and carries on; the outer tx still commits
		}
		return nil
	})
	require.NoError(t, err)

	refs, err := f.store.EdgesListIncoming(ctx, f.target.ID, RelationDependsOn)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{f.witness.ID}, objectRefIDs(refs), "a rejected declaration must leave no edge, committed or not")
	// The stamp is written inside EdgesAdd with the edge, so "no edge" and "no
	// stamp" have to hold together — this is the half a caller who swallows the
	// error could otherwise commit on its own.
	f.requireNotOwed(t)
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
	require.NoError(t, f.cc.DependenciesAdd(ctx, f.dep.ID, f.target.ID, stale))
	f.requireOwed(t)

	require.EqualValues(t, 1, f.owedCount(t))

	// Every later pass re-asserts the same edge with the same stale version. The
	// count is what makes this exact: a re-fire would be invisible in the listing
	// (already there from the first) but shows up here immediately.
	for range 3 {
		require.NoError(t, f.cc.DependenciesAdd(ctx, f.dep.ID, f.target.ID, stale))
	}
	assert.EqualValues(t, 1, f.owedCount(t), "the edge is no longer new, so no later pass stamps again")
}

// TestAddDependencyZeroResourceVersionSkipsCheck pins the sentinel: 0 is "no
// opinion", the correct value when the edge is declared before the target is
// read. It must not mean "wake unconditionally" — that would reproduce the spin
// above in any caller that passes the zero value.
func TestAddDependencyZeroResourceVersionSkipsCheck(t *testing.T) {
	f := newDeclareFixture(t)
	f.moveTarget(t)
	require.NoError(t, f.cc.DependenciesAdd(context.Background(), f.dep.ID, f.target.ID, 0))
	f.requireNotOwed(t)
}

// TestAddDependencyNoWakeOnRollback pins that the wake is registered post-commit:
// a declaration rolled back by the controller's enclosing transaction never
// happened, so there is no edge to have missed a change and nothing to requeue.
func TestAddDependencyNoWakeOnRollback(t *testing.T) {
	f := newDeclareFixture(t)
	ctx := context.Background()
	asRead := f.moveTarget(t)

	err := f.cc.Within(ctx, func(ctx context.Context) error {
		if err := f.cc.DependenciesAdd(ctx, f.dep.ID, f.target.ID, asRead); err != nil {
			return err
		}
		return errBoom
	})
	require.ErrorIs(t, err, errBoom)

	refs, err := f.store.EdgesListIncoming(ctx, f.target.ID, RelationDependsOn)
	require.NoError(t, err)
	require.Equal(t, []ObjectID{f.witness.ID}, objectRefIDs(refs), "the rolled-back declaration left no edge")
	f.requireNotOwed(t)
}

func TestControllerClientHasIncomingEdges(t *testing.T) {
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

	has, err := cc.EdgesHasIncoming(ctx, owner.ID)
	require.NoError(t, err)
	assert.True(t, has, "owner is referenced by the child")

	has, err = cc.EdgesHasIncoming(ctx, child.ID)
	require.NoError(t, err)
	assert.False(t, has, "nothing references the child")
}

// TestControllerClientWritesScopedToKind verifies that a ControllerClient's
// status/condition/finalizer writes refuse an id belonging to another kind: a
// controller for "Widget" must not be able to persist its Status (or mutate
// conditions/finalizers) on a "Gadget" row, which would corrupt that kind's
// rows. DependenciesAdd/EdgesHasIncoming are intentionally cross-kind and not guarded.
func TestControllerClientWritesScopedToKind(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	cc, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{}) // controller for "Widget"
	require.NoError(t, err)
	// "Gadget" gets a controller of its own so the finalizer below is legal to
	// create. It is foreign to cc either way — that is the point of the test — and
	// having its own controller is what makes ErrWrongKind the *only* thing standing
	// between cc and the row.
	gadgetGK := GroupKind{Kind: "Gadget"}
	registerNoop[cSpec, cStatus](t, bh, gadgetGK)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	// Give the Gadget a finalizer so the FinalizersDelete attempt has a target to
	// (fail to) remove.
	gadgets := NewClient[cSpec, cStatus](bh, gadgetGK)
	gadget, err := gadgets.Create(ctx, cSpec{Val: "v1"}, WithFinalizers("f"))
	require.NoError(t, err)

	require.ErrorIs(t, cc.UpdateStatus(ctx, gadget.ID, 1, cStatus{Val: "hijacked"}), ErrWrongKind)
	require.ErrorIs(t, cc.ConditionsSet(ctx, gadget.ID, Condition{Type: "Ready", Status: ConditionTrue}), ErrWrongKind)
	require.ErrorIs(t, cc.ConditionsDelete(ctx, gadget.ID, "Ready"), ErrWrongKind)
	require.ErrorIs(t, cc.FinalizersDelete(ctx, gadget.ID, "f"), ErrWrongKind)

	// The Gadget is untouched: no status, no conditions, finalizer intact.
	got, err := gadgets.Get(ctx, gadget.ID)
	require.NoError(t, err)
	assert.Nil(t, got.Status, "foreign status write rejected")
	assert.Empty(t, got.Conditions, "foreign condition write rejected")
	assert.Equal(t, []string{"f"}, got.Finalizers, "foreign finalizer write rejected")
}

// failEdgesHasIncomingStore returns an error from EdgesHasIncoming.
type failEdgesHasIncomingStore struct {
	fakeStore
}

func (s *failEdgesHasIncomingStore) EdgesHasIncoming(context.Context, ObjectID) (bool, error) {
	return false, errBoom
}

func TestControllerClientHasIncomingRefsStoreError(t *testing.T) {
	bh, err := New(&failEdgesHasIncomingStore{})
	require.NoError(t, err)
	cc := &controllerClientImpl[tStatus]{bh: bh, gk: GroupKind{Kind: "T"}}
	_, err = cc.EdgesHasIncoming(context.Background(), 1)
	require.ErrorIs(t, err, errBoom)
}

// failEdgesAddStore returns an error from the ref insert.
type failEdgesAddStore struct {
	fakeStore
}

func (s *failEdgesAddStore) EdgesAdd(context.Context, ObjectID, ObjectID, Relation, int64) (storeapi.EdgesAddResult, error) {
	return storeapi.EdgesAddResult{}, errBoom
}

func TestControllerClientAddDependencyStoreError(t *testing.T) {
	bh, err := New(&failEdgesAddStore{})
	require.NoError(t, err)
	cc := &controllerClientImpl[tStatus]{bh: bh, gk: GroupKind{Kind: "T"}}
	err = cc.DependenciesAdd(context.Background(), 1, 2, 0)
	require.ErrorIs(t, err, errBoom)
}

// kindTStore runs Within inline and answers ObjectsGet with a row of kind "T", so
// tests reach the write path under test. Embed it in a double that overrides the
// specific write.
type kindTStore struct {
	fakeStore
}

func (s *kindTStore) Within(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}
func (s *kindTStore) ObjectsGet(_ context.Context, id ObjectID) (*RawObject, error) {
	return &RawObject{ID: id, Kind: "T"}, nil
}

// failUpdateStatusStore returns an error from UpdateStatus.
type failUpdateStatusStore struct {
	kindTStore
}

func (s *failUpdateStatusStore) ObjectsUpdateStatus(_ context.Context, _ GroupKind, _ ObjectID, _ int64, _ []byte, _ int) (*RawObject, error) {
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

// failEdgesDeleteStore returns an error from EdgesDelete (Within runs fn inline).
type failEdgesDeleteStore struct {
	fakeStore
}

func (s *failEdgesDeleteStore) EdgesDelete(context.Context, ObjectID, ObjectID, Relation) error {
	return errBoom
}

// TestControllerClientDeleteDependencyDeleteRefError covers the EdgesDelete failure
// branch: the edge removal itself fails, so the whole DependenciesDelete errors.
func TestControllerClientDeleteDependencyDeleteRefError(t *testing.T) {
	bh, err := New(&failEdgesDeleteStore{})
	require.NoError(t, err)
	cc := &controllerClientImpl[tStatus]{bh: bh, gk: GroupKind{Kind: "T"}}
	err = cc.DependenciesDelete(context.Background(), 1, 2)
	require.ErrorIs(t, err, errBoom)
}

func TestControllerClientReadEdges(t *testing.T) {
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
	require.NoError(t, addEdge(ctx, store, child.ID, owner.ID, RelationDependsOn))

	ref, ok, err := cc.OwnersGet(ctx, child.ID)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, owner.ID, ref.ID)

	deps, err := cc.DependenciesList(ctx, child.ID)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{owner.ID}, objectRefIDs(deps))

	dependents, err := cc.DependentsList(ctx, owner.ID)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{child.ID}, objectRefIDs(dependents))

	owned, err := cc.OwnedList(ctx, owner.ID)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{child.ID}, objectRefIDs(owned))
}
