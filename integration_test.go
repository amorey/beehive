package beehive_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// statusSettingController stores the ControllerClient from Start, then on the
// first Reconcile call writes a fixed status and closes reconciledCh.
type statusSettingController struct {
	mu           sync.Mutex
	client       beehive.ControllerClient[cStatus]
	once         sync.Once
	reconciledCh chan struct{}
}

func (c *statusSettingController) Start(client beehive.ControllerClient[cStatus]) error {
	c.mu.Lock()
	c.client = client
	c.mu.Unlock()
	return nil
}

func (c *statusSettingController) Stop(_ context.Context) error { return nil }

func (c *statusSettingController) Reconcile(ctx context.Context, obj *beehive.Object[cSpec, cStatus]) (beehive.Result, error) {
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if err := client.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"}); err != nil {
		return beehive.Result{}, err
	}
	c.once.Do(func() { close(c.reconciledCh) })
	return beehive.Result{}, nil
}

func waitForCh(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// specEchoController writes cStatus{Val: obj.Spec.Val} on every Reconcile and
// closes secondCh once after the second call.
type specEchoController struct {
	mu       sync.Mutex
	client   beehive.ControllerClient[cStatus]
	count    int
	once     sync.Once
	secondCh chan struct{}
}

func (c *specEchoController) Start(client beehive.ControllerClient[cStatus]) error {
	c.mu.Lock()
	c.client = client
	c.mu.Unlock()
	return nil
}
func (c *specEchoController) Stop(_ context.Context) error { return nil }
func (c *specEchoController) Reconcile(ctx context.Context, obj *beehive.Object[cSpec, cStatus]) (beehive.Result, error) {
	c.mu.Lock()
	client := c.client
	c.count++
	n := c.count
	c.mu.Unlock()
	if err := client.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: obj.Spec.Val}); err != nil {
		return beehive.Result{}, err
	}
	if n >= 2 {
		c.once.Do(func() { close(c.secondCh) })
	}
	return beehive.Result{}, nil
}

// deletionTrackingController signals reconciled after the first successful
// reconcile and deleted when the object's DeletionRequestedAt is set.
type deletionTrackingController struct {
	mu           sync.Mutex
	client       beehive.ControllerClient[cStatus]
	reconcileOne sync.Once
	deleteOne    sync.Once
	reconciled   chan struct{}
	deleted      chan struct{}
}

func (c *deletionTrackingController) Start(client beehive.ControllerClient[cStatus]) error {
	c.mu.Lock()
	c.client = client
	c.mu.Unlock()
	return nil
}
func (c *deletionTrackingController) Stop(_ context.Context) error { return nil }
func (c *deletionTrackingController) Reconcile(ctx context.Context, obj *beehive.Object[cSpec, cStatus]) (beehive.Result, error) {
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if obj.DeletionRequestedAt != nil {
		c.deleteOne.Do(func() { close(c.deleted) })
		return beehive.Result{}, nil
	}
	if err := client.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"}); err != nil {
		return beehive.Result{}, err
	}
	c.reconcileOne.Do(func() { close(c.reconciled) })
	return beehive.Result{}, nil
}

// rollbackTestController verifies transaction rollback: on the first Reconcile
// it writes status then returns an error; on the second it records whether the
// write was rolled back (obj.Status == nil) and closes doneCh.
type rollbackTestController struct {
	mu         sync.Mutex
	client     beehive.ControllerClient[cStatus]
	count      int
	once       sync.Once
	sawNilStat bool
	doneCh     chan struct{}
}

func (c *rollbackTestController) Start(client beehive.ControllerClient[cStatus]) error {
	c.mu.Lock()
	c.client = client
	c.mu.Unlock()
	return nil
}
func (c *rollbackTestController) Stop(_ context.Context) error { return nil }
func (c *rollbackTestController) Reconcile(ctx context.Context, obj *beehive.Object[cSpec, cStatus]) (beehive.Result, error) {
	c.mu.Lock()
	client := c.client
	c.count++
	n := c.count
	c.mu.Unlock()
	if n == 1 {
		_ = client.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "should-be-rolled-back"})
		return beehive.Result{}, errors.New("intentional error")
	}
	c.once.Do(func() {
		c.mu.Lock()
		c.sawNilStat = (obj.Status == nil)
		c.mu.Unlock()
		close(c.doneCh)
	})
	return beehive.Result{}, nil
}

func TestIntegrationCreateTriggersReconcile(t *testing.T) {
	ctx := context.Background()

	bh, err := beehive.New(newClientTestStore(t), beehive.WithResyncInterval(0))
	require.NoError(t, err)

	ctrl := &statusSettingController{reconciledCh: make(chan struct{})}
	require.NoError(t, beehive.Register(bh, clientTestGK, ctrl))
	require.NoError(t, bh.Start())
	defer bh.Stop(ctx)

	client := beehive.NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)

	waitForCh(t, ctrl.reconciledCh, "first reconcile")

	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status)
	assert.Equal(t, "done", got.Status.Val)
	require.NotNil(t, got.ObservedGeneration)
	assert.Equal(t, obj.Generation, *got.ObservedGeneration)
}

func TestIntegrationUpdateTriggersReconcile(t *testing.T) {
	ctx := context.Background()

	bh, err := beehive.New(newClientTestStore(t), beehive.WithResyncInterval(0))
	require.NoError(t, err)

	ctrl := &specEchoController{secondCh: make(chan struct{})}
	require.NoError(t, beehive.Register(bh, clientTestGK, ctrl))
	require.NoError(t, bh.Start())
	defer bh.Stop(ctx)

	client := beehive.NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "v1"})
	require.NoError(t, err)

	_, err = client.Update(ctx, obj.ID, cSpec{Val: "v2"})
	require.NoError(t, err)

	waitForCh(t, ctrl.secondCh, "second reconcile after spec update")

	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status)
	assert.Equal(t, "v2", got.Status.Val)
}

func TestIntegrationDeleteTriggersReconcile(t *testing.T) {
	ctx := context.Background()

	bh, err := beehive.New(newClientTestStore(t), beehive.WithResyncInterval(0))
	require.NoError(t, err)

	ctrl := &deletionTrackingController{
		reconciled: make(chan struct{}),
		deleted:    make(chan struct{}),
	}
	require.NoError(t, beehive.Register(bh, clientTestGK, ctrl))
	require.NoError(t, bh.Start())
	defer bh.Stop(ctx)

	client := beehive.NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)

	waitForCh(t, ctrl.reconciled, "first reconcile")

	require.NoError(t, client.Delete(ctx, obj.ID))
	waitForCh(t, ctrl.deleted, "reconcile after deletion requested")
}

func TestIntegrationTransactionRollback(t *testing.T) {
	ctx := context.Background()

	// Use a short resync so the second reconcile fires before the 1s backoff.
	bh, err := beehive.New(newClientTestStore(t), beehive.WithResyncInterval(10*time.Millisecond))
	require.NoError(t, err)

	ctrl := &rollbackTestController{doneCh: make(chan struct{})}
	require.NoError(t, beehive.Register(bh, clientTestGK, ctrl))
	require.NoError(t, bh.Start())
	defer bh.Stop(ctx)

	client := beehive.NewClient[cSpec, cStatus](bh, clientTestGK)
	_, err = client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)

	waitForCh(t, ctrl.doneCh, "second reconcile (after rollback)")

	ctrl.mu.Lock()
	ok := ctrl.sawNilStat
	ctrl.mu.Unlock()
	assert.True(t, ok, "status must have been rolled back when first reconcile errored")
}

func TestIntegrationStartupEnqueuesUnsettled(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)

	// Insert an object before beehive starts (simulating a previous process run).
	specJSON, err := json.Marshal(cSpec{Val: "pre-existing"})
	require.NoError(t, err)
	_, err = store.CreateObject(ctx, &beehive.RawObject{Kind: clientTestGK.Kind, Spec: specJSON})
	require.NoError(t, err)

	bh, err := beehive.New(store, beehive.WithResyncInterval(0))
	require.NoError(t, err)

	ctrl := &statusSettingController{reconciledCh: make(chan struct{})}
	require.NoError(t, beehive.Register(bh, clientTestGK, ctrl))
	require.NoError(t, bh.Start())
	defer bh.Stop(ctx)

	// Without startup enqueue this would time out (resync is disabled).
	waitForCh(t, ctrl.reconciledCh, "reconcile of pre-existing object at startup")
}
