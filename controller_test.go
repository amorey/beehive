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
)

// capturingController saves the ControllerClient it receives in Start so the
// test can call UpdateStatus directly.
type capturingController struct {
	clientCh chan ControllerClient[cStatus]
}

func newCapturingController() *capturingController {
	return &capturingController{clientCh: make(chan ControllerClient[cStatus], 1)}
}

func (c *capturingController) Start(client ControllerClient[cStatus]) error {
	c.clientCh <- client
	return nil
}

func (c *capturingController) Stop(_ context.Context) error { return nil }

func (c *capturingController) Reconcile(_ context.Context, _ *Object[cSpec, cStatus]) (Result, error) {
	return Result{}, nil
}

func TestControllerClientDeleteFinalizer(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)

	ctrl := newCapturingController()
	require.NoError(t, Register(bh, clientTestGK, ctrl))
	require.NoError(t, bh.Start())
	defer bh.Stop(ctx)

	var cc ControllerClient[cStatus]
	select {
	case cc = <-ctrl.clientCh:
	case <-time.After(2 * time.Second):
		t.Fatal("controller Start was not called")
	}

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "hello"}, WithFinalizers("a", "b"))
	require.NoError(t, err)

	require.NoError(t, cc.DeleteFinalizer(ctx, obj.ID, "a"))
	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, got.Finalizers, "finalizer removed via ControllerClient")
}

func TestControllerClientUpdateStatus(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)

	ctrl := newCapturingController()
	require.NoError(t, Register(bh, clientTestGK, ctrl))
	require.NoError(t, bh.Start())
	defer bh.Stop(ctx)

	// Receive the ControllerClient that was passed to Start.
	var cc ControllerClient[cStatus]
	select {
	case cc = <-ctrl.clientCh:
	default:
		t.Fatal("controller Start was not called")
	}

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

func TestControllerClientSetAndDeleteCondition(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)

	ctrl := newCapturingController()
	require.NoError(t, Register(bh, clientTestGK, ctrl))
	require.NoError(t, bh.Start())
	defer bh.Stop(ctx)

	var cc ControllerClient[cStatus]
	select {
	case cc = <-ctrl.clientCh:
	case <-time.After(2 * time.Second):
		t.Fatal("controller Start was not called")
	}

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

	ctrl := newCapturingController()
	require.NoError(t, Register(bh, clientTestGK, ctrl))
	require.NoError(t, bh.Start())
	defer bh.Stop(ctx)

	var cc ControllerClient[cStatus]
	select {
	case cc = <-ctrl.clientCh:
	case <-time.After(2 * time.Second):
		t.Fatal("controller Start was not called")
	}

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	from, err := client.Create(ctx, cSpec{Val: "from"})
	require.NoError(t, err)
	to, err := client.Create(ctx, cSpec{Val: "to"})
	require.NoError(t, err)

	require.NoError(t, cc.AddDependency(ctx, from.ID, to.ID))
	deps, err := bh.store.ListIncomingRefs(ctx, to.ID, RelationDependsOn)
	require.NoError(t, err)
	assert.Equal(t, []Referrer{{ID: from.ID, Group: clientTestGK.Group, Kind: clientTestGK.Kind}}, deps)

	require.NoError(t, cc.DeleteDependency(ctx, from.ID, to.ID))
	deps, err = bh.store.ListIncomingRefs(ctx, to.ID, RelationDependsOn)
	require.NoError(t, err)
	assert.Empty(t, deps, "edge removed via ControllerClient")
}

// addRefTxTrackingStore records whether AddRef ran inside a Within call, so a test
// can assert AddDependency wraps its endpoint check + insert in one transaction.
// Accessed only from the test goroutine, so it needs no locking.
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

func (s *addRefTxTrackingStore) AddRef(ctx context.Context, fromID, toID ObjectID, relation Relation) error {
	s.addRefInTx = s.depth > 0
	return s.Store.AddRef(ctx, fromID, toID, relation)
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

	require.NoError(t, cc.AddDependency(ctx, from.ID, to.ID))
	assert.True(t, tracking.addRefInTx,
		"AddDependency must wrap its endpoint check + insert in one transaction")
}

func TestControllerClientHasIncomingRefs(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)

	ctrl := newCapturingController()
	require.NoError(t, Register(bh, clientTestGK, ctrl))
	require.NoError(t, bh.Start())
	defer bh.Stop(ctx)

	var cc ControllerClient[cStatus]
	select {
	case cc = <-ctrl.clientCh:
	case <-time.After(2 * time.Second):
		t.Fatal("controller Start was not called")
	}

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

	ctrl := newCapturingController()
	require.NoError(t, Register(bh, clientTestGK, ctrl)) // controller for "Widget"
	require.NoError(t, bh.Start())
	defer bh.Stop(ctx)

	var cc ControllerClient[cStatus]
	select {
	case cc = <-ctrl.clientCh:
	case <-time.After(2 * time.Second):
		t.Fatal("controller Start was not called")
	}

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

// failAddRefStore returns an error from AddRef.
type failAddRefStore struct {
	fakeStore
}

func (s *failAddRefStore) AddRef(context.Context, ObjectID, ObjectID, Relation) error {
	return errBoom
}

func TestControllerClientAddDependencyStoreError(t *testing.T) {
	bh, err := New(&failAddRefStore{})
	require.NoError(t, err)
	cc := &controllerClientImpl[tStatus]{bh: bh, gk: GroupKind{Kind: "T"}}
	err = cc.AddDependency(context.Background(), 1, 2)
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

func (s *failUpdateStatusStore) UpdateStatus(_ context.Context, _ GroupKind, _ ObjectID, _ int64, _ []byte) (*RawObject, error) {
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
