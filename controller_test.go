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
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/beehive/internal/storeapi"
)

func TestControllerClientDeleteFinalizer(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)

	cc := registerWithClient(t, bh, clientTestGK, &noopController[cSpec, cStatus]{})
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"}, WithFinalizers("a", "b"))

	require.NoError(t, cc.at(obj.ID).DeleteFinalizer(ctx, "a"))
	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, got.Finalizers, "finalizer removed via ControllerClient")
}

// Clearing the last finalizer is the one route out of a finalizer-blocked
// collect, so it pushes rather than waiting out a GC tick.
func TestDeleteFinalizerPushesTheCollect(t *testing.T) {
	ctx := context.Background()
	_, client, cc, r := specWriteFixture(t)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"}, WithFinalizers("f"))
	require.NoError(t, client.Delete(ctx, obj.ID))
	drainQueue(r.work)

	require.NoError(t, cc.at(obj.ID).DeleteFinalizer(ctx, "f"))
	assert.Equal(t, []ObjectID{obj.ID}, queuedIDs(r.work))
}

// Every neighbour of that transition owes nothing. Pushing on a live object in
// particular would collect-probe every finalizer removal in the system.
func TestDeleteFinalizerPushesNothingOtherwise(t *testing.T) {
	tests := []struct {
		name       string
		finalizers []string
		deleting   bool
		remove     string
	}{
		{"live object", []string{"f"}, false, "f"},
		{"finalizers remain", []string{"f", "g"}, true, "f"},
		{"absent finalizer", []string{"f"}, true, "missing"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			_, client, cc, r := specWriteFixture(t)

			obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"}, WithFinalizers(tt.finalizers...))
			if tt.deleting {
				require.NoError(t, client.Delete(ctx, obj.ID))
			}
			drainQueue(r.work)

			require.NoError(t, cc.at(obj.ID).DeleteFinalizer(ctx, tt.remove))
			assert.Empty(t, queuedIDs(r.work))
		})
	}
}

// The push rides AfterCommit, so an unwound frame discards it with its writes —
// including a nested frame whose error the caller swallows, where the outer
// transaction still commits.
func TestDeleteFinalizerPushesNothingWhenRolledBack(t *testing.T) {
	ctx := context.Background()
	_, client, cc, r := specWriteFixture(t)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"}, WithFinalizers("f"))
	require.NoError(t, client.Delete(ctx, obj.ID))
	drainQueue(r.work)

	pass := cc.at(obj.ID)
	require.NoError(t, pass.Within(ctx, func(ctx context.Context) error {
		err := pass.Within(ctx, func(ctx context.Context) error {
			require.NoError(t, pass.DeleteFinalizer(ctx, "f"))
			return errBoom
		})
		require.ErrorIs(t, err, errBoom)
		return nil // swallowed: the outer frame commits
	}))

	assert.Empty(t, queuedIDs(r.work))
	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"f"}, got.Finalizers, "the savepoint unwound the removal too")
}

// A rejected write pushes nothing: the kind check fires before the gate is even
// consulted.
func TestDeleteFinalizerPushesNothingOnWrongKind(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	cc := registerWithClient(t, bh, clientTestGK, &noopController[cSpec, cStatus]{})
	gadgetGK := GroupKind{Kind: "Gadget"}
	registerNoop[cSpec, cStatus](t, bh, gadgetGK)
	gadgetR := mustReconciler(t, bh, gadgetGK)

	gadgets := NewClient[cSpec, cStatus](bh, gadgetGK)
	gadget := mustCreate(t, ctx, gadgets, uniqueName(), cSpec{Val: "v1"}, WithFinalizers("f"))
	require.NoError(t, gadgets.Delete(ctx, gadget.ID))
	drainQueue(gadgetR.work)

	require.ErrorIs(t, cc.at(gadget.ID).DeleteFinalizer(ctx, "f"), ErrWrongKind)
	assert.Empty(t, queuedIDs(gadgetR.work))
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

		cc := registerWithClient(t, bh, clientTestGK, &noopController[cSpec, cStatus]{},
			WithMigrator(&fakeMigrator{specVersion: 4, statusVersion: 9}))

		client := NewClient[cSpec, cStatus](bh, clientTestGK)
		obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

		// Spec write (Create) stamped the spec version; status untouched (still 0).
		raw, err := store.Objects().Get(ctx, obj.ID)
		require.NoError(t, err)
		assert.Equal(t, 4, raw.SpecVersion, "Create stamps the migrator's spec version")
		assert.Equal(t, 0, raw.StatusVersion, "no status written yet")

		// Controller status write stamps the status version, spec unchanged.
		require.NoError(t, cc.at(obj.ID).UpdateStatus(ctx, cStatus{Val: "done"}))
		raw, err = store.Objects().Get(ctx, obj.ID)
		require.NoError(t, err)
		assert.Equal(t, 4, raw.SpecVersion, "status write must not touch spec version")
		assert.Equal(t, 9, raw.StatusVersion, "UpdateStatus stamps the migrator's status version")
	})

	t.Run("no migrator stamps 0 (backward compatible)", func(t *testing.T) {
		store := newClientTestStore(t)
		bh, err := New(store)
		require.NoError(t, err)

		cc := registerWithClient(t, bh, clientTestGK, &noopController[cSpec, cStatus]{})

		client := NewClient[cSpec, cStatus](bh, clientTestGK)
		obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})
		require.NoError(t, cc.at(obj.ID).UpdateStatus(ctx, cStatus{Val: "done"}))

		raw, err := store.Objects().Get(ctx, obj.ID)
		require.NoError(t, err)
		assert.Zero(t, raw.SpecVersion, "no migrator => spec version stays 0")
		assert.Zero(t, raw.StatusVersion, "no migrator => status version stays 0")
	})
}

func TestControllerClientUpdateStatus(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)

	cc := registerWithClient(t, bh, clientTestGK, &noopController[cSpec, cStatus]{})
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	// Create an object and update its status via the ControllerClient.
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

	err = cc.at(obj.ID).UpdateStatus(ctx, cStatus{Val: "done"})
	require.NoError(t, err)

	// Status must now be visible through the client.
	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status)
	assert.Equal(t, "done", got.Status.Val)
	assert.Nil(t, got.ObservedGeneration, "a status write is not a handshake write")
}

// TestControllerClientUpdateStatusNoOpIsSilent pins the property downstream
// controllers rely on: reporting the same status again produces no Modified
// frame on the watch, so a dependent that free-rides on a status change isn't
// woken by an unchanged poll. A controller can therefore report unconditionally
// instead of hand-rolling an equality guard.
func TestControllerClientUpdateStatusNoOpIsSilent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bh := newTestBeehive(t, newClientTestStore(t), fast()...)

	cc := registerWithClient(t, bh, clientTestGK, &noopController[cSpec, cStatus]{})

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})
	require.NoError(t, cc.at(obj.ID).UpdateStatus(ctx, cStatus{Val: "done"}))

	stream, err := client.WatchList(ctx)
	require.NoError(t, err)
	require.Len(t, stream.Objects, 1)

	// Same status: silent. Checked at the mechanism rather than by waiting out a
	// grace period on the channel — the watch emits off resource_version, so a
	// no-op write that leaves it alone is one the poller cannot see, whenever it
	// happens to look. The channel assertion below is the second half: a stray
	// frame for this write would have to arrive before the real change's.
	before, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NoError(t, cc.at(obj.ID).UpdateStatus(ctx, cStatus{Val: "done"}))
	after, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, before.ResourceVersion, after.ResourceVersion,
		"an unchanged status bumped resource_version, which is what the watch emits on")

	// A real change still flows.
	require.NoError(t, cc.at(obj.ID).UpdateStatus(ctx, cStatus{Val: "changed"}))
	select {
	case ev := <-stream.Changes:
		assert.Equal(t, Modified, ev.Type)
		require.NotNil(t, ev.Object.Status)
		assert.Equal(t, "changed", ev.Object.Status.Val)
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for the changed-status event")
	}
}

// TestPassClientBindsThePassObject pins the binding: the client acts on the
// object its pass was handed, never on a sibling of the same kind. A client that
// bound the wrong id would pass every single-object test in this file.
func TestPassClientBindsThePassObject(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})
	sibling := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})

	cc := newPassClient[cStatus](bh, clientTestGK, obj.ID, nil)
	require.NoError(t, cc.UpdateStatus(ctx, cStatus{Val: "mine"}))
	require.NoError(t, cc.SetCondition(ctx, Condition{Type: "Ready", Status: ConditionTrue}))
	require.NoError(t, cc.AddEvent(ctx, EventSpec{Category: "c", Type: EventNormal, Reason: "Started"}))

	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status)
	assert.Equal(t, "mine", got.Status.Val)
	assert.NotNil(t, findCondition(got.Conditions, "Ready"))

	untouched, err := client.Get(ctx, sibling.ID)
	require.NoError(t, err)
	assert.Nil(t, untouched.Status, "a sibling of the same kind is not the pass's object")
	assert.Empty(t, untouched.Conditions)
	run, err := bh.store.Events().GetLatest(ctx, sibling.ID, "c")
	require.NoError(t, err)
	assert.Nil(t, run, "the sibling's event log too")
}

// TestControllerClientWithin verifies the opt-in atomicity surface: writes made
// inside Within commit together on a nil return and roll back together on error,
// with the nested ControllerClient writes joining the one transaction.
func TestControllerClientWithin(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	cc := passClients[cStatus]{bh: bh, gk: clientTestGK}
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})

	// Rollback: an error from fn discards every write it made.
	sentinel := errors.New("boom")
	err := cc.at(obj.ID).Within(ctx, func(ctx context.Context) error {
		if err := cc.at(obj.ID).UpdateStatus(ctx, cStatus{Val: "rolled-back"}); err != nil {
			return err
		}
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)
	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Nil(t, got.Status, "writes inside a Within that errored must roll back")

	// Commit: a nil return persists every write atomically.
	require.NoError(t, cc.at(obj.ID).Within(ctx, func(ctx context.Context) error {
		if err := cc.at(obj.ID).UpdateStatus(ctx, cStatus{Val: "committed"}); err != nil {
			return err
		}
		return cc.at(obj.ID).SetCondition(ctx, Condition{Type: "Ready", Status: ConditionTrue})
	}))
	got, err = client.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status)
	assert.Equal(t, "committed", got.Status.Val)
	assert.NotNil(t, findCondition(got.Conditions, "Ready"))
}

// AddEvent writes an aggregated run through the store, marshaling EventSpec's
// Detail; the run reads back with the mapped fields and a decodable payload.
func TestControllerClientAddEvent(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	cc := passClients[cStatus]{bh: bh, gk: clientTestGK}
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})

	require.NoError(t, cc.at(obj.ID).AddEvent(ctx, EventSpec{
		Category: "connection", Type: EventWarning, Reason: "ProbeFailed",
		Message: "i/o timeout", Detail: probeDetail{Endpoint: "h:443", LatencyMs: 5000},
	}))

	run, err := bh.store.Events().GetLatest(ctx, obj.ID, "connection")
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

// AddEvent surfaces a Detail that cannot be JSON-marshaled, before touching
// the store.
func TestControllerClientAddEventMarshalError(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{})
	cc := passClients[cStatus]{bh: bh, gk: clientTestGK}

	err := cc.at(1).AddEvent(context.Background(), EventSpec{Detail: make(chan int)})
	assert.Error(t, err, "an unmarshalable Detail fails the write")
}

// AddEvent is kind-folded like the other writes: a controller may not record
// events on an object of another kind.
func TestControllerClientAddEventWrongKind(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})

	other := passClients[tStatus]{bh: bh, gk: GroupKind{Kind: "Other"}}
	err := other.at(obj.ID).AddEvent(ctx, EventSpec{Type: EventNormal, Reason: "X"})
	assert.ErrorIs(t, err, ErrWrongKind)
}

// AddEvent composes in Within: a run recorded inside a transaction that later
// errors is rolled back with the rest.
func TestControllerClientAddEventWithinRollback(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	cc := passClients[cStatus]{bh: bh, gk: clientTestGK}
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})

	sentinel := errors.New("boom")
	err := cc.at(obj.ID).Within(ctx, func(ctx context.Context) error {
		if err := cc.at(obj.ID).AddEvent(ctx, EventSpec{Category: "c", Type: EventNormal, Reason: "Started"}); err != nil {
			return err
		}
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)

	run, err := bh.store.Events().GetLatest(ctx, obj.ID, "c")
	require.NoError(t, err)
	assert.Nil(t, run, "an AddEvent inside a rolled-back Within must not persist")
}

func TestControllerClientSetAndDeleteCondition(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)

	cc := registerWithClient(t, bh, clientTestGK, &noopController[cSpec, cStatus]{})
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

	require.NoError(t, cc.at(obj.ID).SetCondition(ctx, Condition{Type: "Ready", Status: ConditionTrue}))
	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, findCondition(got.Conditions, "Ready"))

	require.NoError(t, cc.at(obj.ID).DeleteCondition(ctx, "Ready"))
	got, err = client.Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Nil(t, findCondition(got.Conditions, "Ready"), "condition removed via ControllerClient")
}

// SetConditions is the batch a controller reaches for when one pass observes
// several conditions: every one lands, and the object moves one version, so a
// watcher cannot see the pass half-applied.
func TestControllerClientSetConditions(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)

	cc := registerWithClient(t, bh, clientTestGK, &noopController[cSpec, cStatus]{})
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

	require.NoError(t, cc.at(obj.ID).SetConditions(ctx, []Condition{
		{Type: "Connected", Status: ConditionTrue, Reason: "Dialed"},
		{Type: "Healthy", Status: ConditionFalse, Reason: "ProbeFailed"},
	}))

	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	connected := findCondition(got.Conditions, "Connected")
	healthy := findCondition(got.Conditions, "Healthy")
	require.NotNil(t, connected)
	require.NotNil(t, healthy)
	assert.Equal(t, ConditionTrue, connected.Status)
	assert.Equal(t, ConditionFalse, healthy.Status)
	assert.Equal(t, obj.ResourceVersion+1, got.ResourceVersion, "the batch draws one version")

	// A type named twice would apply in whichever order the caller happened to
	// build the slice, so it is refused rather than resolved.
	assert.ErrorIs(t, cc.at(obj.ID).SetConditions(ctx, []Condition{
		{Type: "Ready", Status: ConditionTrue},
		{Type: "Ready", Status: ConditionFalse},
	}), ErrDuplicateConditionType)

	require.NoError(t, cc.at(obj.ID).SetConditions(ctx, nil))
	after, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, got.ResourceVersion, after.ResourceVersion,
		"a refused batch and an empty one both write nothing")
	assert.Nil(t, findCondition(after.Conditions, "Ready"))
}

// A controller whose whole report is conditions has no status write to carry the
// handshake and needs none: returning Settled is what records the generation.
func TestConditionsOnlyControllerSettlesByReturningSettled(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)

	reconciled := make(chan struct{}, 4)
	inner := &funcController{fn: func(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) ReconcileResult {
		if err := cc.SetCondition(ctx, Condition{Type: "Synced", Status: ConditionFalse, Reason: "Paused"}); err != nil {
			return Fail(err)
		}
		select {
		case reconciled <- struct{}{}:
		default:
		}
		return Settled()
	}}
	err := Register(bh, clientTestGK, inner)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

	select {
	case <-reconciled:
	case <-time.After(testTimeout):
		t.Fatal("the create's own enqueue never reconciled")
	}
	got := waitSettled(t, ctx, client, obj.ID)
	assert.Nil(t, got.Status, "the handshake writes no status")
	require.NotNil(t, findCondition(got.Conditions, "Synced"), "the pass's real report")

	unsettled, err := store.Objects().ListUnsettledIDs(ctx, clientTestGK)
	require.NoError(t, err)
	assert.NotContains(t, unsettled, obj.ID)
}

func TestControllerClientAddAndDeleteDependency(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)

	cc := registerWithClient(t, bh, clientTestGK, &noopController[cSpec, cStatus]{})
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	from := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "from"})
	to := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "to"})

	require.NoError(t, cc.at(from.ID).AddDependency(ctx, to.ID))
	deps, err := bh.store.Edges().ListIncoming(ctx, to.ID, RelationDependsOn)
	require.NoError(t, err)
	assert.Equal(t, []ObjectRef{{ID: from.ID, Group: clientTestGK.Group, Kind: clientTestGK.Kind}}, deps)

	require.NoError(t, cc.at(from.ID).DeleteDependency(ctx, to.ID))
	deps, err = bh.store.Edges().ListIncoming(ctx, to.ID, RelationDependsOn)
	require.NoError(t, err)
	assert.Empty(t, deps, "edge removed via ControllerClient")
}

// TestAddDependencyAcceptsCycle records that beehive lets a caller declare a
// cycle. It is the tripwire for the deferred fix in docs/TODO.md's cycle entry: the
// candidate that rejects a cycle-closing edge at declare time would make one of
// these calls start returning an error, so whoever builds it trips a test that
// states today's answer rather than discovering it.
//
// The waker's own cycle test cannot serve here — it drives edges through a fake,
// so a declare-time check is invisible to it.
func TestAddDependencyAcceptsCycle(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)

	gk := GroupKind{Kind: "Widget"}
	cc := registerWithClient(t, bh, gk, &noopController[tSpec, tStatus]{})
	client := NewClient[tSpec, tStatus](bh, gk)
	a := mustCreate(t, ctx, client, uniqueName(), tSpec{})
	b := mustCreate(t, ctx, client, uniqueName(), tSpec{})

	require.NoError(t, cc.at(a.ID).AddDependency(ctx, b.ID))
	require.NoError(t, cc.at(b.ID).AddDependency(ctx, a.ID), "a cycle-closing edge is accepted today")
	require.NoError(t, cc.at(a.ID).AddDependency(ctx, a.ID), "and so is a self-edge")
}

// declareFixture is the shared setup for the dependency-declare tests: a
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
// The two objects live in *different kinds* on purpose: the edge is deliberately
// cross-kind, while cc — the client the tests declare through — is the
// dependent's own, which is the only thing that can declare for it.
type declareFixture struct {
	cc          passClients[tStatus] // the dependent's own client
	bh          *Beehive
	store       Store
	targetGK    GroupKind
	depGK       GroupKind
	dep, target *Object[tSpec, tStatus]
	// witness is a second dependent of dep's kind whose edge to the target exists
	// from the start, so an assertion that a declaration left no edge can name what
	// the listing should still contain rather than merely expecting it empty.
	witness *Object[tSpec, tStatus]
}

func newDeclareFixture(t *testing.T) *declareFixture {
	t.Helper()
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)

	f := &declareFixture{
		bh:       bh,
		store:    store,
		targetGK: GroupKind{Kind: "Target"},
		depGK:    GroupKind{Kind: "Dependent"},
	}
	err := Register(bh, f.targetGK, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)
	f.cc = registerWithClient(t, bh, f.depGK, &noopController[tSpec, tStatus]{})

	depClient := NewClient[tSpec, tStatus](bh, f.depGK)
	f.dep = mustCreate(t, ctx, depClient, "dep", tSpec{})
	f.witness = mustCreate(t, ctx, depClient, "witness", tSpec{})
	f.target = mustCreate(t, ctx, NewClient[tSpec, tStatus](bh, f.targetGK), "target", tSpec{})
	// Declared straight through the store: the witness's edge is scaffolding, not a
	// use of the guard under test, and going through the store leaves it unstamped.
	require.NoError(t, addEdge(ctx, store, f.witness.ID, f.target.ID, RelationDependsOn))

	return f
}

// moveTarget changes the target, so a declaration that follows is one whose read
// of it predates the change.
func (f *declareFixture) moveTarget(t *testing.T) {
	t.Helper()
	err := f.store.Conditions().Set(context.Background(), f.targetGK, f.target.ID,
		storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
}

// owed returns the dependent kind's owed-wake listing — what the owed-pass tick
// would drain.
func (f *declareFixture) owed(t *testing.T) []ObjectID {
	t.Helper()
	ids, err := f.store.ReconcileOwed().ListIDs(context.Background(), f.depGK)
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
	meta, err := f.store.Objects().GetMeta(context.Background(), f.dep.ID)
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

// queued returns what gk's work queue holds. Register builds the queue, so an
// enqueue is observable with no driver running: these tests assert that the
// declaration queued the object, not that a later pass found it.
func (f *declareFixture) queued(t *testing.T, gk GroupKind) []ObjectID {
	t.Helper()
	return queuedIDs(mustReconciler(t, f.bh, gk).work)
}

func mustReconciler(t *testing.T, bh *Beehive, gk GroupKind) *reconciler {
	t.Helper()
	r, ok := bh.reconcilerFor(gk)
	require.True(t, ok, "the kind must be registered to have a queue")
	return r
}

// sameKindFixture is one registered kind holding a source and a target, which is
// the simple shape of a declaration. newDeclareFixture is the cross-kind one.
type sameKindFixture struct {
	cc          passClients[tStatus]
	r           *reconciler
	dep, target *Object[tSpec, tStatus]
}

// newSameKindFixture leaves the work queue empty. Each create enqueues its own
// object, so draining here is what makes the queue afterwards hold the
// declaration's work and nothing else.
func newSameKindFixture(t *testing.T) *sameKindFixture {
	t.Helper()
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	gk := GroupKind{Kind: "Widget"}
	cc := registerWithClient(t, bh, gk, &noopController[tSpec, tStatus]{})

	client := NewClient[tSpec, tStatus](bh, gk)
	f := &sameKindFixture{
		cc:     cc,
		r:      mustReconciler(t, bh, gk),
		dep:    mustCreate(t, ctx, client, uniqueName(), tSpec{}),
		target: mustCreate(t, ctx, client, uniqueName(), tSpec{}),
	}
	drainQueue(f.r.work)
	return f
}

func (f *sameKindFixture) queued() []ObjectID { return queuedIDs(f.r.work) }

// TestAddDependencyWakesOncePerEdge pins the declare-time guarantee and its
// bound together. The guarantee: the call that creates the edge records one owed
// reconcile, durably, so a declare reaches its dependent whatever the target was
// doing — including a change that landed between the caller's read and the
// declare, which no scan could carry because the edge did not exist yet. The
// declaration goes through the target kind's ControllerClient (see
// declareFixture), so this also pins that the wake routes by fromID's own kind
// rather than the declarer's. The bound: only the edge-creating call stamps, so a
// level-triggered controller re-asserting its set every pass — and nothing
// throttles a self-sustaining requeue — adds nothing after the first, even after
// the target moves again.
func TestAddDependencyWakesOncePerEdge(t *testing.T) {
	f := newDeclareFixture(t)
	ctx := context.Background()
	// The target moved after the caller's read and before the declare: the case a
	// write-log scan structurally cannot see.
	f.moveTarget(t)

	require.NoError(t, f.cc.at(f.dep.ID).AddDependency(ctx, f.target.ID))
	f.requireOwed(t)
	require.EqualValues(t, 1, f.owedCount(t))

	// Every later pass re-asserts the same edge, as a converging controller does.
	// The count is what makes this exact: a re-fire would be invisible in the
	// listing (already there from the first) but shows up here immediately.
	for range 3 {
		require.NoError(t, f.cc.at(f.dep.ID).AddDependency(ctx, f.target.ID))
	}
	require.EqualValues(t, 1, f.owedCount(t), "the edge is no longer new, so no later declare stamps again")

	// Nor does the target moving make a re-declare stamp: once the edge exists,
	// delivering changes is the waker's and the stale pass's job.
	f.moveTarget(t)
	require.NoError(t, f.cc.at(f.dep.ID).AddDependency(ctx, f.target.ID))
	assert.EqualValues(t, 1, f.owedCount(t), "one wake per edge created, not per declare")
}

// TestAddDependencyStampRidesRefsAdd pins that the durable stamp is not a second
// store call sequenced after the edge, but a write inside EdgesAdd itself — so the
// edge and its wake are indivisible in the store rather than by grace of the caller's
// transaction being a real rollback boundary.
//
// That the stamp *cannot* be a second call is now structural: the Store interface
// carries no standalone increment, so nothing on this path could issue one. What
// remains to check is the other half — that folding it into EdgesAdd actually stamps —
// which is what the two assertions below do: edge and wake land together.
func TestAddDependencyStampRidesRefsAdd(t *testing.T) {
	ctx := context.Background()
	real := newClientTestStore(t)

	bh := newTestBeehive(t, real)
	gk := GroupKind{Kind: "Widget"}
	cc := registerWithClient(t, bh, gk, &noopController[tSpec, tStatus]{}, WithFullPassInterval(0))

	client := NewClient[tSpec, tStatus](bh, gk)
	dep := mustCreate(t, ctx, client, uniqueName(), tSpec{})
	target := mustCreate(t, ctx, client, uniqueName(), tSpec{})

	require.NoError(t, cc.at(dep.ID).AddDependency(ctx, target.ID))

	refs, err := real.Edges().ListIncoming(ctx, target.ID, RelationDependsOn)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{dep.ID}, objectRefIDs(refs), "the edge landed")

	owed, err := real.ReconcileOwed().ListIDs(ctx, gk)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{dep.ID}, owed, "and the stamp landed with it, inside EdgesAdd")
}

// TestAddDependencyEnqueuesItsSource pins the latency this closes. The durable
// stamp records that the source owes a reconcile, but nothing scheduled it, so the
// first pass waited for the owed-pass tick. The declaration now enqueues the source
// when the edge commits.
//
// The source and the target share a kind here, which is the simple case. The
// cross-kind case has its own test, because it is the one that fails if the enqueue
// routes by the caller's kind.
func TestAddDependencyEnqueuesItsSource(t *testing.T) {
	ctx := context.Background()
	f := newSameKindFixture(t)

	require.NoError(t, f.cc.at(f.dep.ID).AddDependency(ctx, f.target.ID))
	assert.Equal(t, []ObjectID{f.dep.ID}, f.queued(), "the new edge queues its source")
}

// TestAddDependencyEnqueuesOnlyWhatItStamped pins the gate. The enqueue reads the
// store's report of what the write did — EdgesAddResult.ReconcileOwedStamped — and
// never the fact that the caller called AddDependency.
//
// The gate is what bounds the enqueue. A level-triggered controller re-asserts its
// whole dependency set on every pass, so an enqueue per call would schedule a pass
// per pass, forever. Worse, requeueNow on an id that is in flight makes it
// dispatchable at once, so a failing controller would retry at full speed and never
// climb its backoff ladder. Only a genuinely new edge stamps, so only a genuinely
// new edge queues, and the bound is one enqueue per edge ever created.
//
// A self-edge stamps nothing for the same reason the store excludes it: an object
// that depends on itself owes no wake from itself.
func TestAddDependencyEnqueuesOnlyWhatItStamped(t *testing.T) {
	ctx := context.Background()
	f := newSameKindFixture(t)
	require.NoError(t, f.cc.at(f.dep.ID).AddDependency(ctx, f.target.ID))
	drainQueue(f.r.work)

	// The edge exists now, so every later declare of it stamps nothing.
	for range 3 {
		require.NoError(t, f.cc.at(f.dep.ID).AddDependency(ctx, f.target.ID))
	}
	assert.Empty(t, f.queued(), "a re-asserted edge is not new, so it queues nothing")

	require.NoError(t, f.cc.at(f.dep.ID).AddDependency(ctx, f.dep.ID))
	assert.Empty(t, f.queued(), "a self-edge stamps nothing, so it queues nothing")
}

// TestAddDependencyEnqueuesNothingOnRollback pins that the enqueue rides
// AfterCommit. A declaration the caller's transaction discards never happened, so
// there is nothing to schedule.
//
// Neither shape here is what an ordinary reconcile produces: a reconcile opens no
// transaction, so the hook usually runs inline and there is nothing to unwind. Both
// shapes are opt-in, and they are the only ones that can roll back at all. The
// nested case is the savepoint unwind, where the inner frame's hooks must go even
// though the outer frame commits nothing.
func TestAddDependencyEnqueuesNothingOnRollback(t *testing.T) {
	runCommitRollback(t, func(t *testing.T, commit bool) {
		f := newDeclareFixture(t)
		ctx := context.Background()
		drainQueue(mustReconciler(t, f.bh, f.depGK).work)

		err := f.cc.at(f.dep.ID).Within(ctx, func(ctx context.Context) error {
			return f.cc.at(f.dep.ID).Within(ctx, func(ctx context.Context) error {
				if err := f.cc.at(f.dep.ID).AddDependency(ctx, f.target.ID); err != nil {
					return err
				}
				if commit {
					return nil
				}
				return errBoom
			})
		})
		if commit {
			require.NoError(t, err)
			assert.Equal(t, []ObjectID{f.dep.ID}, f.queued(t, f.depGK), "a committed declaration queues")
			return
		}
		require.ErrorIs(t, err, errBoom)
		assert.Empty(t, f.queued(t, f.depGK), "a rolled-back declaration queues nothing")
	})
}

// TestAddDependencyOnAClientOnlyKindEnqueuesNothing pins that a source whose kind
// has no reconciler is not an error. The stamp still lands, and nothing in this
// process drains it — which is unchanged by the enqueue, and is the client-only
// kind's existing gap rather than one this path opens.
func TestAddDependencyOnAClientOnlyKindEnqueuesNothing(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)
	targetGK := GroupKind{Kind: "Target"}
	cc := registerWithClient(t, bh, targetGK, &noopController[tSpec, tStatus]{})

	clientOnly := GroupKind{Kind: "NoController"}
	dep := mustCreate(t, ctx, NewClient[tSpec, tStatus](bh, clientOnly), uniqueName(), tSpec{})
	target := mustCreate(t, ctx, NewClient[tSpec, tStatus](bh, targetGK), uniqueName(), tSpec{})

	require.NoError(t, cc.at(dep.ID).AddDependency(ctx, target.ID), "an unroutable enqueue is not an error")

	owed, err := store.ReconcileOwed().ListIDs(ctx, clientOnly)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{dep.ID}, owed, "the durable stamp still lands")
}

// redeclareController fails on every pass and re-asserts the same dependency each
// time, which is the converging shape a level-triggered controller has. It fires
// first on its first failing pass and hot once it has run enough times to prove
// the backoff was bypassed.
//
// The target succeeds and is left alone. It shares this controller, so failing it
// too would put a second backoff ladder under one counter, and the test could not
// say which object's passes it had counted.
type redeclareController struct {
	target     ObjectID
	calls      atomic.Int64
	first, hot *signal
}

func (c *redeclareController) Reconcile(ctx context.Context, cc ControllerClient[tStatus], obj *Object[tSpec, tStatus]) ReconcileResult {
	if obj.ID == c.target {
		return Settled()
	}
	if c.calls.Add(1) >= hotLoopCalls {
		c.hot.fire()
	}
	c.first.fire()
	_ = cc.AddDependency(ctx, c.target)
	return Fail(errBoom)
}

// TestFailingControllerKeepsItsBackoffWhenItsEdgeSetConverges pins the bound the
// gate buys. A controller that re-asserts the same dependency on every failing pass
// creates no edge after the first, so it stamps nothing, queues nothing, and climbs
// its backoff ladder as it would with no declaration at all.
//
// This is the shape almost every controller has. The shape that escapes it has its
// own test below.
func TestFailingControllerKeepsItsBackoffWhenItsEdgeSetConverges(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t), withoutGCSweeper())
	gk := GroupKind{Kind: "Widget"}
	ctrl := &redeclareController{first: newSignal(), hot: newSignal()}
	err := Register(bh, gk, ctrl)
	require.NoError(t, err)
	client := NewClient[tSpec, tStatus](bh, gk)

	target := mustCreate(t, ctx, client, uniqueName(), tSpec{})
	ctrl.target = target.ID

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(context.Background())

	_ = mustCreate(t, ctx, client, uniqueName(), tSpec{})

	requireNoHotLoop(t, ctrl.first, ctrl.hot, &ctrl.calls,
		"a re-asserted edge must not requeue past the backoff")
}

// TestANewEdgeOnAnInFlightSourceRespectsTheBackoff closes what the
// stamp-every-new-edge ADR pinned as an accepted cost: a source creating a
// genuinely new edge on every failing pass used to lose its backoff ladder,
// because the push went through requeueNow, which marks an in-flight id dirty
// and work.done then makes it dispatchable ahead of the alarm.
//
// The push now goes through work.add, so the re-enqueue floor opened by the
// dispatch holds it, and the backoff the worker sets a line later takes the
// floor's place. The stamp is durable either way, so nothing is lost — the owed
// pass still carries the dependent.
//
// The assertion is on the mechanism and not on the clock. "Retries on the
// ladder" is a claim about time, and this suite has no clock to assert on, so
// the test drives the worker's own sequence — get, declare, done, addAfter —
// and asserts the id is not dispatchable while an alarm holds it.
func TestANewEdgeOnAnInFlightSourceRespectsTheBackoff(t *testing.T) {
	ctx := context.Background()
	f := newSameKindFixture(t)

	// Take the id, as a worker does before it runs the controller. The dispatch
	// opens the source's floor.
	f.r.work.add(f.dep.ID)
	got, ok := f.r.work.get()
	require.True(t, ok)
	require.Equal(t, f.dep.ID, got)

	// The controller declares a new dependency and then fails.
	require.NoError(t, f.cc.at(f.dep.ID).AddDependency(ctx, f.target.ID))

	// runWorker releases the id and only then sets the backoff.
	f.r.work.done(f.dep.ID)
	f.r.work.addAfter(f.dep.ID, time.Hour, alarmBackoff)

	assert.Empty(t, f.queued(), "the declaration must not jump the ladder")
	at := f.r.work.scheduleAt(f.dep.ID).NextRequeueAt
	assert.True(t, at.After(time.Now().Add(time.Minute)),
		"the id is on the backoff ladder, got %s", at)
}

// TestAddDependencyNoWakeOnRollback pins that the wake is registered post-commit:
// a declaration rolled back by the controller's enclosing transaction never
// happened, so there is no edge to have missed a change and nothing to requeue.
func TestAddDependencyNoWakeOnRollback(t *testing.T) {
	f := newDeclareFixture(t)
	ctx := context.Background()

	err := f.cc.at(f.dep.ID).Within(ctx, func(ctx context.Context) error {
		if err := f.cc.at(f.dep.ID).AddDependency(ctx, f.target.ID); err != nil {
			return err
		}
		return errBoom
	})
	require.ErrorIs(t, err, errBoom)

	refs, err := f.store.Edges().ListIncoming(ctx, f.target.ID, RelationDependsOn)
	require.NoError(t, err)
	require.Equal(t, []ObjectID{f.witness.ID}, objectRefIDs(refs), "the rolled-back declaration left no edge")
	f.requireNotOwed(t)
}

func TestControllerClientHasIncomingEdges(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)

	cc := registerWithClient(t, bh, clientTestGK, &noopController[cSpec, cStatus]{})
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	owner := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "owner"})
	child := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "child"}, WithOwner(owner.ID))

	has, err := cc.at(owner.ID).HasIncomingEdges(ctx)
	require.NoError(t, err)
	assert.True(t, has, "owner is referenced by the child")

	has, err = cc.at(child.ID).HasIncomingEdges(ctx)
	require.NoError(t, err)
	assert.False(t, has, "nothing references the child")
}

// TestControllerClientWritesScopedToKind verifies that a ControllerClient's
// status/condition/finalizer writes refuse an id belonging to another kind: a
// controller for "Widget" must not be able to persist its Status (or mutate
// conditions/finalizers) on a "Gadget" row, which would corrupt that kind's
// rows. AddDependency/HasIncomingEdges are intentionally cross-kind and not guarded.
func TestControllerClientWritesScopedToKind(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	cc := registerWithClient(t, bh, clientTestGK, &noopController[cSpec, cStatus]{}) // controller for "Widget"
	// "Gadget" gets a controller of its own so the finalizer below is legal to
	// create. It is foreign to cc either way — that is the point of the test — and
	// having its own controller is what makes ErrWrongKind the *only* thing standing
	// between cc and the row.
	gadgetGK := GroupKind{Kind: "Gadget"}
	registerNoop[cSpec, cStatus](t, bh, gadgetGK)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	// Give the Gadget a finalizer so the DeleteFinalizer attempt has a target to
	// (fail to) remove.
	gadgets := NewClient[cSpec, cStatus](bh, gadgetGK)
	gadget := mustCreate(t, ctx, gadgets, uniqueName(), cSpec{Val: "v1"}, WithFinalizers("f"))

	require.ErrorIs(t, cc.at(gadget.ID).UpdateStatus(ctx, cStatus{Val: "hijacked"}), ErrWrongKind)
	require.ErrorIs(t, cc.at(gadget.ID).SetCondition(ctx, Condition{Type: "Ready", Status: ConditionTrue}), ErrWrongKind)
	require.ErrorIs(t, cc.at(gadget.ID).DeleteCondition(ctx, "Ready"), ErrWrongKind)
	require.ErrorIs(t, cc.at(gadget.ID).DeleteFinalizer(ctx, "f"), ErrWrongKind)

	// The Gadget is untouched: no status, no conditions, finalizer intact.
	got, err := gadgets.Get(ctx, gadget.ID)
	require.NoError(t, err)
	assert.Nil(t, got.Status, "foreign status write rejected")
	assert.Empty(t, got.Conditions, "foreign condition write rejected")
	assert.Equal(t, []string{"f"}, got.Finalizers, "foreign finalizer write rejected")
}

// failEdgesHasIncomingStore returns an error from HasIncomingEdges.
type failEdgesHasIncomingStore struct {
	fakeStore
}

func (s *failEdgesHasIncomingStore) Edges() storeapi.Edges {
	return edgesOverride{Edges: s.fakeStore.Edges(), hasIncoming: s.hasIncomingEdges}
}

func (s *failEdgesHasIncomingStore) hasIncomingEdges(context.Context, ObjectID) (bool, error) {
	return false, errBoom
}

func TestControllerClientHasIncomingRefsStoreError(t *testing.T) {
	bh := newTestBeehive(t, &failEdgesHasIncomingStore{})
	cc := passClients[tStatus]{bh: bh, gk: GroupKind{Kind: "T"}}
	_, err := cc.at(1).HasIncomingEdges(context.Background())
	require.ErrorIs(t, err, errBoom)
}

// failEdgesAddStore returns an error from the ref insert.
type failEdgesAddStore struct {
	fakeStore
}

func (s *failEdgesAddStore) Edges() storeapi.Edges {
	return edgesOverride{Edges: s.fakeStore.Edges(), add: s.addEdges}
}

func (s *failEdgesAddStore) addEdges(context.Context, ObjectID, ObjectID, Relation) (storeapi.EdgesAddResult, error) {
	return storeapi.EdgesAddResult{}, errBoom
}

func TestControllerClientAddDependencyStoreError(t *testing.T) {
	bh := newTestBeehive(t, &failEdgesAddStore{})
	cc := passClients[tStatus]{bh: bh, gk: GroupKind{Kind: "T"}}
	err := cc.at(1).AddDependency(context.Background(), 2)
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
func (s *kindTStore) Objects() storeapi.Objects {
	return objectsOverride{Objects: s.fakeStore.Objects(), get: s.getObjects}
}

func (s *kindTStore) getObjects(_ context.Context, id ObjectID) (*RawObject, error) {
	return &RawObject{ID: id, Kind: "T"}, nil
}

// failUpdateStatusStore returns an error from UpdateStatus.
type failUpdateStatusStore struct {
	kindTStore
}

func (s *failUpdateStatusStore) Objects() storeapi.Objects {
	return objectsOverride{Objects: s.kindTStore.Objects(), updateStatus: s.updateStatus}
}

func (s *failUpdateStatusStore) updateStatus(_ context.Context, _ GroupKind, _ ObjectID, _ []byte, _ int) (bool, error) {
	return false, errBoom
}

// errStatusMarshaler is a Status type whose JSON marshaling always fails.
type errStatusMarshaler struct{}

func (errStatusMarshaler) MarshalJSON() ([]byte, error) { return nil, errBoom }

func TestControllerClientUpdateStatusMarshalError(t *testing.T) {
	bh := newTestBeehive(t, &kindTStore{})
	cc := passClients[errStatusMarshaler]{bh: bh, gk: GroupKind{Kind: "T"}}
	err := cc.at(1).UpdateStatus(context.Background(), errStatusMarshaler{})
	require.Error(t, err)
}

func TestControllerClientUpdateStatusStoreError(t *testing.T) {
	bh := newTestBeehive(t, &failUpdateStatusStore{})
	cc := passClients[tStatus]{bh: bh, gk: GroupKind{Kind: "T"}}
	err := cc.at(1).UpdateStatus(context.Background(), tStatus{})
	require.Error(t, err)
}

// failEdgesDeleteStore returns an error from EdgesDelete (Within runs fn inline).
type failEdgesDeleteStore struct {
	fakeStore
}

func (s *failEdgesDeleteStore) Edges() storeapi.Edges {
	return edgesOverride{Edges: s.fakeStore.Edges(), delete: s.deleteEdges}
}

func (s *failEdgesDeleteStore) deleteEdges(context.Context, ObjectID, ObjectID, Relation) (storeapi.EdgesDeleteResult, error) {
	return storeapi.EdgesDeleteResult{}, errBoom
}

// TestControllerClientDeleteDependencyDeleteRefError covers the EdgesDelete failure
// branch: the edge removal itself fails, so the whole DeleteDependency errors.
func TestControllerClientDeleteDependencyDeleteRefError(t *testing.T) {
	bh := newTestBeehive(t, &failEdgesDeleteStore{})
	cc := passClients[tStatus]{bh: bh, gk: GroupKind{Kind: "T"}}
	err := cc.at(1).DeleteDependency(context.Background(), 2)
	require.ErrorIs(t, err, errBoom)
}

// Dropping the last live referrer is one of the routes out of a RESTRICT-blocked
// collect, so it pushes rather than waiting out a GC tick.
func TestDeleteDependencyPushesTheBlockedTarget(t *testing.T) {
	ctx := context.Background()
	_, client, cc, r := specWriteFixture(t)

	target := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "target"})
	dependent := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "dependent"})
	require.NoError(t, cc.at(dependent.ID).AddDependency(ctx, target.ID))
	require.NoError(t, client.Delete(ctx, target.ID))
	drainQueue(r.work)

	require.NoError(t, cc.at(dependent.ID).DeleteDependency(ctx, target.ID))
	assert.Equal(t, []ObjectID{target.ID}, queuedIDs(r.work))
}

// The three gates that decline the push: a live target, an edge that was never
// there, and a dependent already finalizing (whose edge blocks nothing).
func TestDeleteDependencyPushesNothingOtherwise(t *testing.T) {
	tests := []struct {
		name            string
		declare         bool
		deleteTarget    bool
		deleteDependent bool
	}{
		{name: "live target", declare: true},
		{name: "missing edge", deleteTarget: true},
		{name: "finalizing dependent", declare: true, deleteTarget: true, deleteDependent: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			_, client, cc, r := specWriteFixture(t)

			target := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "target"})
			dependent := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "dependent"})
			if tt.declare {
				require.NoError(t, cc.at(dependent.ID).AddDependency(ctx, target.ID))
			}
			if tt.deleteTarget {
				require.NoError(t, client.Delete(ctx, target.ID))
			}
			if tt.deleteDependent {
				require.NoError(t, client.Delete(ctx, dependent.ID))
			}
			drainQueue(r.work)

			require.NoError(t, cc.at(dependent.ID).DeleteDependency(ctx, target.ID))
			assert.Empty(t, queuedIDs(r.work))
		})
	}
}

// The edge is cross-kind, so the push routes by the target's kind rather than
// the controller's own.
func TestDeleteDependencyPushesAcrossKinds(t *testing.T) {
	ctx := context.Background()
	bh, client, cc, depR := specWriteFixture(t)
	targetGK := GroupKind{Kind: "DropTarget"}
	registerNoop[cSpec, cStatus](t, bh, targetGK)
	targetR := mustReconciler(t, bh, targetGK)
	targetClient := NewClient[cSpec, cStatus](bh, targetGK)

	target := mustCreate(t, ctx, targetClient, uniqueName(), cSpec{Val: "target"})
	dependent := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "dependent"})
	require.NoError(t, cc.at(dependent.ID).AddDependency(ctx, target.ID))
	require.NoError(t, targetClient.Delete(ctx, target.ID))
	drainQueue(depR.work)
	drainQueue(targetR.work)

	require.NoError(t, cc.at(dependent.ID).DeleteDependency(ctx, target.ID))
	assert.Equal(t, []ObjectID{target.ID}, queuedIDs(targetR.work), "the target's own kind is queued")
	assert.Empty(t, queuedIDs(depR.work), "the dependent's kind is not")
}

// The target is finalizing, so it already carries an alarm from its own delete
// push; an absorbed push would wait out the ladder the sweep was going to beat.
func TestDeleteDependencyPushBeatsAPendingAlarm(t *testing.T) {
	ctx := context.Background()
	_, client, cc, r := specWriteFixture(t)

	target := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "target"})
	dependent := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "dependent"})
	require.NoError(t, cc.at(dependent.ID).AddDependency(ctx, target.ID))
	require.NoError(t, client.Delete(ctx, target.ID))
	drainQueue(r.work)
	// Long enough that the alarm firing on its own would be the test hanging,
	// not the assertion passing.
	r.work.addAfter(target.ID, time.Hour, alarmBackoff)

	require.NoError(t, cc.at(dependent.ID).DeleteDependency(ctx, target.ID))
	assert.Equal(t, []ObjectID{target.ID}, queuedIDs(r.work), "the drop beats the backoff alarm")
}

// A client-only target resolves to no reconciler; the sweeper stays its route.
func TestDeleteDependencySkipsClientOnlyTarget(t *testing.T) {
	ctx := context.Background()
	bh, client, cc, r := specWriteFixture(t)
	targetClient := NewClient[cSpec, cStatus](bh, GroupKind{Kind: "UnregisteredTarget"})

	target := mustCreate(t, ctx, targetClient, uniqueName(), cSpec{Val: "target"})
	dependent := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "dependent"})
	require.NoError(t, cc.at(dependent.ID).AddDependency(ctx, target.ID))
	require.NoError(t, targetClient.Delete(ctx, target.ID))
	drainQueue(r.work)

	require.NoError(t, cc.at(dependent.ID).DeleteDependency(ctx, target.ID))
	assert.Empty(t, queuedIDs(r.work))
}

func TestControllerClientReadEdges(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)
	cc := registerWithClient(t, bh, clientTestGK, &noopController[cSpec, cStatus]{})

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	owner := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "owner"})
	child := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "child"}, WithOwner(owner.ID))
	require.NoError(t, addEdge(ctx, store, child.ID, owner.ID, RelationDependsOn))

	ref, ok, err := cc.at(child.ID).GetOwner(ctx)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, owner.ID, ref.ID)

	deps, err := cc.at(child.ID).ListDependencies(ctx)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{owner.ID}, objectRefIDs(deps))

	dependents, err := cc.at(owner.ID).ListDependents(ctx)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{child.ID}, objectRefIDs(dependents))

	owned, err := cc.at(owner.ID).ListOwned(ctx)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{child.ID}, objectRefIDs(owned))
}

// UpdateStatus writes status and nothing else, so no status write can roll a
// converged object back to unsettled.
func TestUpdateStatusDoesNotTouchTheHandshake(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)
	cc := registerWithClient(t, bh, clientTestGK, &noopController[cSpec, cStatus]{})
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

	require.NoError(t, cc.at(obj.ID).UpdateStatus(ctx, cStatus{Val: "reported"}))

	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, "reported", got.Status.Val)
	assert.Nil(t, got.ObservedGeneration, "a status write settles nothing")
	assert.Contains(t, unsettledIDs(t, store), obj.ID)
}

// The stamp is a write like any other and wakes the kind's watches. Pinned with
// the floor tick parked, so only the wake can deliver.
func TestObservedGenerationStampWakesTheKindsWatches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bh := newTestBeehive(t, newClientTestStore(t), fast(WithWatchFloorInterval(time.Hour))...)

	var settle atomic.Bool
	ctrl := &funcController{fn: func(context.Context, ControllerClient[cStatus], *Object[cSpec, cStatus]) ReconcileResult {
		if settle.Load() {
			return Settled()
		}
		return Unsettled().RequeueAfter(time.Hour)
	}}
	err := Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

	// Subscribed before the object can settle, so the stamp is the only write
	// left to deliver.
	stream, err := client.WatchList(ctx)
	require.NoError(t, err)
	settle.Store(true)
	require.NoError(t, client.Requeue(ctx, obj.ID))

	ev := recv(t, stream.Changes)
	require.NotNil(t, ev.Object.ObservedGeneration)
	assert.Equal(t, ev.Object.Generation, *ev.Object.ObservedGeneration)
}

// The client passed into Reconcile stops working when that call returns: a write
// arriving later moves status with no pass behind it.
func TestPassClientStopsWorkingWhenReconcileReturns(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)

	var (
		captured ControllerClient[cStatus]
		passes   int
	)
	ran := make(chan struct{}, 4)
	inner := &funcController{fn: func(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) ReconcileResult {
		passes++
		if passes == 1 {
			// Only the first pass's client is captured, so the test reads it
			// after that pass has been signalled and nothing writes it again.
			captured = cc
		}
		// A value per pass: UpdateStatus skips a byte-identical write, so a
		// constant would make the second pass's write indistinguishable from no
		// write at all.
		val := fmt.Sprintf("inside-%d", passes)
		// Live for the whole of Reconcile, Within included.
		if err := cc.Within(ctx, func(ctx context.Context) error {
			return cc.UpdateStatus(ctx, cStatus{Val: val})
		}); err != nil {
			return Fail(err)
		}
		select {
		case ran <- struct{}{}:
		default:
		}
		return Settled()
	}}
	require.NoError(t, Register(bh, clientTestGK, inner))
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})
	select {
	case <-ran:
	case <-time.After(testTimeout):
		t.Fatal("the create's own enqueue never reconciled")
	}
	require.NotNil(t, captured)

	t.Run("a write fails", func(t *testing.T) {
		assert.ErrorIs(t, captured.UpdateStatus(ctx, cStatus{Val: "late"}), ErrReconcileReturned)
	})

	t.Run("a read fails too", func(t *testing.T) {
		_, _, err := captured.GetOwner(ctx)
		assert.ErrorIs(t, err, ErrReconcileReturned, "the whole surface stops, not just the writes")
	})

	t.Run("the write really did not land", func(t *testing.T) {
		got, err := client.Get(ctx, obj.ID)
		require.NoError(t, err)
		require.NotNil(t, got.Status)
		assert.Equal(t, "inside-1", got.Status.Val)
	})

	// The restriction is on the pass, not on the kind: the next pass gets a
	// working client for the same object.
	t.Run("the next pass writes", func(t *testing.T) {
		require.NoError(t, client.Requeue(ctx, obj.ID))
		select {
		case <-ran:
		case <-time.After(testTimeout):
			t.Fatal("the requeue never reconciled")
		}
		got, err := client.Get(ctx, obj.ID)
		require.NoError(t, err)
		assert.Equal(t, "inside-2", got.Status.Val)
	})
}

// end() and every method gate go through the same atomic. Needs -race to mean
// much; without it, it still pins that the calls end in ErrReconcileReturned.
func TestPassClientIsSafeAgainstAConcurrentCaller(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)

	var (
		wg      sync.WaitGroup
		lateErr atomic.Value
	)
	ran := make(chan struct{}, 4)
	inner := &funcController{fn: func(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) ReconcileResult {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Runs across the return: some calls land before end(), some after.
			for i := 0; i < 50; i++ {
				if err := cc.UpdateStatus(ctx, cStatus{Val: "late"}); err != nil {
					lateErr.Store(err)
					return
				}
			}
		}()
		select {
		case ran <- struct{}{}:
		default:
		}
		return Settled()
	}}
	err := Register(bh, clientTestGK, inner)
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})
	select {
	case <-ran:
	case <-time.After(testTimeout):
		t.Fatal("the create's own enqueue never reconciled")
	}
	wg.Wait()

	if err, ok := lateErr.Load().(error); ok {
		assert.ErrorIs(t, err, ErrReconcileReturned, "the only failure a late call may take")
	}
}

// Every method, in both states. Table-driven over the whole surface because the
// rule is the surface: a method added without a gate is what this pins.
func TestPassClientGatesEveryMethod(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)
	err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})
	dep := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "dep"})

	calls := []struct {
		name string
		call func(cc ControllerClient[cStatus]) error
	}{
		{"AddDependency", func(cc ControllerClient[cStatus]) error { return cc.AddDependency(ctx, dep.ID) }},
		{"AddEvent", func(cc ControllerClient[cStatus]) error {
			return cc.AddEvent(ctx, EventSpec{Category: "lifecycle", Reason: "Probed"})
		}},
		{"DeleteCondition", func(cc ControllerClient[cStatus]) error { return cc.DeleteCondition(ctx, "Synced") }},
		{"DeleteDependency", func(cc ControllerClient[cStatus]) error { return cc.DeleteDependency(ctx, dep.ID) }},
		{"DeleteFinalizer", func(cc ControllerClient[cStatus]) error {
			return cc.DeleteFinalizer(ctx, "kstack.sh/none")
		}},
		{"GetOwner", func(cc ControllerClient[cStatus]) error { _, _, err := cc.GetOwner(ctx); return err }},
		{"HasIncomingEdges", func(cc ControllerClient[cStatus]) error { _, err := cc.HasIncomingEdges(ctx); return err }},
		{"ListDependencies", func(cc ControllerClient[cStatus]) error { _, err := cc.ListDependencies(ctx); return err }},
		{"ListDependents", func(cc ControllerClient[cStatus]) error { _, err := cc.ListDependents(ctx); return err }},
		{"ListOwned", func(cc ControllerClient[cStatus]) error { _, err := cc.ListOwned(ctx); return err }},
		{"SetCondition", func(cc ControllerClient[cStatus]) error {
			return cc.SetCondition(ctx, Condition{Type: "Synced", Status: ConditionTrue})
		}},
		{"SetConditions", func(cc ControllerClient[cStatus]) error {
			return cc.SetConditions(ctx, []Condition{{Type: "Ready", Status: ConditionTrue}})
		}},
		{"UpdateStatus", func(cc ControllerClient[cStatus]) error { return cc.UpdateStatus(ctx, cStatus{Val: "v"}) }},
		{"Within", func(cc ControllerClient[cStatus]) error {
			return cc.Within(ctx, func(context.Context) error { return nil })
		}},
	}

	// The table is the surface, so pin it to the interface: a method added
	// without a row here would go ungated and nothing else would fail.
	iface := reflect.TypeFor[ControllerClient[cStatus]]()
	named := make(map[string]bool, len(calls))
	for _, c := range calls {
		named[c.name] = true
	}
	require.Len(t, named, iface.NumMethod(), "every ControllerClient method needs a row")
	for i := range iface.NumMethod() {
		assert.True(t, named[iface.Method(i).Name], "ungated method missing from the table")
	}

	live := newPassClient[cStatus](bh, clientTestGK, obj.ID, nil)
	for _, c := range calls {
		t.Run(c.name+" runs while the pass runs", func(t *testing.T) {
			// Reaching the store is the assertion; whether it likes the arguments is not.
			assert.NotErrorIs(t, c.call(live), ErrReconcileReturned)
		})
	}

	ended := newPassClient[cStatus](bh, clientTestGK, obj.ID, nil)
	ended.end()
	for _, c := range calls {
		t.Run(c.name+" refuses once the pass has ended", func(t *testing.T) {
			assert.ErrorIs(t, c.call(ended), ErrReconcileReturned)
		})
	}
}

// countingStatusStore counts the status writes that reach the store, so a test
// can tell a skipped write from one the store compared and declined.
type countingStatusStore struct {
	Store
	statusWrites atomic.Int64
}

func (s *countingStatusStore) Objects() storeapi.Objects {
	inner := s.Store.Objects()
	return objectsOverride{
		Objects: inner,
		updateStatus: func(ctx context.Context, gk GroupKind, id ObjectID, status []byte, v int) (bool, error) {
			s.statusWrites.Add(1)
			return inner.UpdateStatus(ctx, gk, id, status, v)
		},
	}
}

// newStatusBaselineFixture stores one object and returns a pass client bound to
// it, carrying the baseline the reconcile loop would have handed over.
func newStatusBaselineFixture(t *testing.T) (
	context.Context, *countingStatusStore, *Beehive, *controllerClientImpl[cStatus], ObjectID,
) {
	t.Helper()
	ctx := context.Background()
	store := &countingStatusStore{Store: newClientTestStore(t)}
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})
	return ctx, store, bh, passClientAt(t, ctx, bh, store, obj.ID), obj.ID
}

// passClientAt builds the client a pass would get for id: bound to the object,
// carrying the status bytes as currently stored.
func passClientAt(
	t *testing.T, ctx context.Context, bh *Beehive, store Store, id ObjectID,
) *controllerClientImpl[cStatus] {
	t.Helper()
	raw, err := store.Objects().Get(ctx, id)
	require.NoError(t, err)
	return newPassClient[cStatus](bh, clientTestGK, id, newStatusBaseline(raw.Status, raw.StatusVersion))
}

// The bytes the pass was handed are what the store holds, so a status equal to
// them is a write the store would decline. Skip it without the transaction.
func TestUpdateStatusSkipsWhatThePassCanSeeIsANoOp(t *testing.T) {
	ctx, store, bh, pass, id := newStatusBaselineFixture(t)

	// A first status differs from the stored NULL, so it is written. Pins that
	// the empty baseline is "no bytes stored", not "no baseline".
	require.NoError(t, pass.UpdateStatus(ctx, cStatus{Val: "done"}))
	assert.EqualValues(t, 1, store.statusWrites.Load(), "a first status is a write")

	// Same bytes again, this time against the baseline its own write promoted.
	require.NoError(t, pass.UpdateStatus(ctx, cStatus{Val: "done"}))
	assert.EqualValues(t, 1, store.statusWrites.Load(), "promotion must re-enable the skip")

	// A fresh pass loads those bytes as its baseline and skips from the start.
	next := passClientAt(t, ctx, bh, store, id)
	require.NoError(t, next.UpdateStatus(ctx, cStatus{Val: "done"}))
	assert.EqualValues(t, 1, store.statusWrites.Load(), "a load-time match skips too")
}

// The write-loss regression: a pass that writes A and then writes back the value
// it was loaded with must reach the store the second time.
func TestUpdateStatusBaselineAdvancesWithItsOwnWrites(t *testing.T) {
	ctx, store, bh, _, id := newStatusBaselineFixture(t)

	// Stored state, and the baseline a pass now loads.
	admin := NewAdminClient[cStatus](bh, clientTestGK)
	require.NoError(t, admin.UpdateStatus(ctx, id, cStatus{Val: "loaded"}))
	pass := passClientAt(t, ctx, bh, store, id)
	before := store.statusWrites.Load()

	require.NoError(t, pass.UpdateStatus(ctx, cStatus{Val: "other"}))
	require.NoError(t, pass.UpdateStatus(ctx, cStatus{Val: "loaded"}))
	assert.EqualValues(t, before+2, store.statusWrites.Load(),
		"the second write differs from what is stored and must not be skipped")

	raw, err := store.Objects().Get(ctx, id)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Val":"loaded"}`, string(raw.Status), "the pass's last word is what is stored")
}

// AdminClient is handed no object, so it holds no baseline: every call reaches
// the store, and none of the baseline methods may panic on the nil.
func TestAdminClientHasNoBaseline(t *testing.T) {
	ctx, store, bh, _, id := newStatusBaselineFixture(t)

	admin := NewAdminClient[cStatus](bh, clientTestGK)
	require.NoError(t, admin.UpdateStatus(ctx, id, cStatus{Val: "same"}))
	require.NoError(t, admin.UpdateStatus(ctx, id, cStatus{Val: "same"}))
	assert.EqualValues(t, 2, store.statusWrites.Load(), "no baseline, no skip")
}

// AfterCommit hooks run at the outermost commit, so inside a controller's own
// transaction no write has promoted yet. The arm is what stops the second call
// matching the stale load-time baseline and dropping the pass's last word.
func TestUpdateStatusInsideWithinDoesNotSkipOnAStaleBaseline(t *testing.T) {
	ctx, store, bh, _, id := newStatusBaselineFixture(t)

	admin := NewAdminClient[cStatus](bh, clientTestGK)
	require.NoError(t, admin.UpdateStatus(ctx, id, cStatus{Val: "loaded"}))
	pass := passClientAt(t, ctx, bh, store, id)

	require.NoError(t, pass.Within(ctx, func(ctx context.Context) error {
		if err := pass.UpdateStatus(ctx, cStatus{Val: "other"}); err != nil {
			return err
		}
		return pass.UpdateStatus(ctx, cStatus{Val: "loaded"})
	}))

	raw, err := store.Objects().Get(ctx, id)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Val":"loaded"}`, string(raw.Status),
		"the last write in the transaction is what stands")
}

// The promote hook carries its own bytes, so a later write that failed cannot
// be promoted by an earlier write's hook.
func TestUpdateStatusFailedWriteDoesNotPromoteAnEarlierOne(t *testing.T) {
	ctx := context.Background()
	store := &failSecondStatusWriteStore{Store: newClientTestStore(t)}
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

	raw, err := store.Objects().Get(ctx, obj.ID)
	require.NoError(t, err)
	pass := newPassClient[cStatus](bh, clientTestGK, obj.ID, newStatusBaseline(raw.Status, raw.StatusVersion))

	_ = pass.Within(ctx, func(ctx context.Context) error {
		require.NoError(t, pass.UpdateStatus(ctx, cStatus{Val: "first"}))
		store.failNext.Store(true)
		require.Error(t, pass.UpdateStatus(ctx, cStatus{Val: "second"}))
		store.failNext.Store(false)
		return nil // the controller swallows it
	})

	// "second" never reached the store, so writing it now must not be skipped.
	require.NoError(t, pass.UpdateStatus(ctx, cStatus{Val: "second"}))
	raw, err = store.Objects().Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Val":"second"}`, string(raw.Status))
}

// A failed write stores nothing, so the bytes it carried must not become the
// baseline. With no other write outstanding there is nothing else to keep the
// fast path off, so this is the case that a promote on the error path loses.
func TestUpdateStatusFailedWriteDoesNotPromoteItsOwnBytes(t *testing.T) {
	ctx := context.Background()
	store := &failSecondStatusWriteStore{Store: newClientTestStore(t)}
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

	raw, err := store.Objects().Get(ctx, obj.ID)
	require.NoError(t, err)
	pass := newPassClient[cStatus](bh, clientTestGK, obj.ID, newStatusBaseline(raw.Status, raw.StatusVersion))

	store.failNext.Store(true)
	require.Error(t, pass.UpdateStatus(ctx, cStatus{Val: "attempted"}))
	store.failNext.Store(false)

	require.NoError(t, pass.UpdateStatus(ctx, cStatus{Val: "attempted"}))
	raw, err = store.Objects().Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Val":"attempted"}`, string(raw.Status),
		"the retry must reach the store: the first attempt stored nothing")
}

// failSecondStatusWriteStore fails the status writes made while failNext is set.
type failSecondStatusWriteStore struct {
	Store
	failNext atomic.Bool
}

func (s *failSecondStatusWriteStore) Objects() storeapi.Objects {
	inner := s.Store.Objects()
	return objectsOverride{
		Objects: inner,
		updateStatus: func(ctx context.Context, gk GroupKind, id ObjectID, status []byte, v int) (bool, error) {
			if s.failNext.Load() {
				return false, errBoom
			}
			return inner.UpdateStatus(ctx, gk, id, status, v)
		},
	}
}

// A rolled-back write never landed, so the baseline must not claim it.
func TestUpdateStatusRolledBackWriteDoesNotPromote(t *testing.T) {
	ctx, store, bh, _, id := newStatusBaselineFixture(t)
	pass := passClientAt(t, ctx, bh, store, id)

	require.Error(t, pass.Within(ctx, func(ctx context.Context) error {
		if err := pass.UpdateStatus(ctx, cStatus{Val: "rolled-back"}); err != nil {
			return err
		}
		return errBoom
	}))

	require.NoError(t, pass.UpdateStatus(ctx, cStatus{Val: "rolled-back"}))
	raw, err := store.Objects().Get(ctx, id)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Val":"rolled-back"}`, string(raw.Status),
		"the write must reach the store, not match a baseline that never committed")
}

// A skip never reads, so it cannot notice that the row was collected mid-pass.
// Documented behavior: the write would have written nothing either way.
func TestUpdateStatusSkipOnACollectedObject(t *testing.T) {
	ctx, store, bh, pass, id := newStatusBaselineFixture(t)

	require.NoError(t, pass.UpdateStatus(ctx, cStatus{Val: "done"}))
	next := passClientAt(t, ctx, bh, store, id)

	require.NoError(t, store.Objects().Delete(ctx, id))
	before := store.statusWrites.Load()

	assert.NoError(t, next.UpdateStatus(ctx, cStatus{Val: "done"}),
		"a skip answers from the baseline and never learns the row is gone")
	assert.Equal(t, before, store.statusWrites.Load())
}

// A status write the store declined to make bumps no resource_version, so waking
// the kind's tailers and the dependency waker for it is work with nothing behind
// it. AdminClient is the way in: it holds no baseline, so both calls reach the
// store and the second is a store-side no-op.
func TestUpdateStatusNoOpWakesNothing(t *testing.T) {
	ctx, _, admin, obj := newAdminClientFixture(t)

	require.NoError(t, admin.UpdateStatus(ctx, obj.ID, tcStatus{Server: tcServerStatus{UID: "server-1"}}))

	rx, _ := admin.bh.kindWriteHub.Watch(tcGK)
	defer rx.Close()

	require.NoError(t, admin.UpdateStatus(ctx, obj.ID, tcStatus{Server: tcServerStatus{UID: "server-1"}}))
	_, err := rx.TryRecv()
	assert.Error(t, err, "identical bytes wrote nothing, so nothing should be woken")

	require.NoError(t, admin.UpdateStatus(ctx, obj.ID, tcStatus{Server: tcServerStatus{UID: "server-2"}}))
	ev, err := rx.RecvContext(ctx)
	require.NoError(t, err, "a real status change still wakes")
	assert.Equal(t, tcGK, ev.Key)
}
