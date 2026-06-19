package beehive

import (
	"context"
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

func TestControllerClientStubsPanic(t *testing.T) {
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

	require.Panics(t, func() { _ = cc.DeleteFinalizer(ctx, 1, "finalizer") })
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
	deps, err := bh.store.ListReferrers(ctx, to.ID, RelationDependsOn)
	require.NoError(t, err)
	assert.Equal(t, []Referrer{{ID: from.ID, Group: clientTestGK.Group, Kind: clientTestGK.Kind}}, deps)

	require.NoError(t, cc.DeleteDependency(ctx, from.ID, to.ID))
	deps, err = bh.store.ListReferrers(ctx, to.ID, RelationDependsOn)
	require.NoError(t, err)
	assert.Empty(t, deps, "edge removed via ControllerClient")
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

// failUpdateStatusStore returns an error from UpdateStatus.
type failUpdateStatusStore struct {
	fakeStore
}

func (s *failUpdateStatusStore) Within(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}
func (s *failUpdateStatusStore) UpdateStatus(_ context.Context, _ ObjectID, _ int64, _ []byte) (*RawObject, error) {
	return nil, errBoom
}

// errStatusMarshaler is a Status type whose JSON marshaling always fails.
type errStatusMarshaler struct{}

func (errStatusMarshaler) MarshalJSON() ([]byte, error) { return nil, errBoom }

func TestControllerClientUpdateStatusMarshalError(t *testing.T) {
	bh, err := New(&fakeStore{})
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
