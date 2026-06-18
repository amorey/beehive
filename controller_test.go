package beehive_test

import (
	"context"
	"testing"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// capturingController saves the ControllerClient it receives in Start so the
// test can call UpdateStatus directly.
type capturingController struct {
	clientCh chan beehive.ControllerClient[cStatus]
}

func newCapturingController() *capturingController {
	return &capturingController{clientCh: make(chan beehive.ControllerClient[cStatus], 1)}
}

func (c *capturingController) Start(client beehive.ControllerClient[cStatus]) error {
	c.clientCh <- client
	return nil
}

func (c *capturingController) Stop(_ context.Context) error { return nil }

func (c *capturingController) Reconcile(_ context.Context, _ *beehive.Object[cSpec, cStatus]) (beehive.Result, error) {
	return beehive.Result{}, nil
}

func TestControllerClientUpdateStatus(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := beehive.New(store)
	require.NoError(t, err)

	ctrl := newCapturingController()
	require.NoError(t, beehive.Register(bh, clientTestGK, ctrl))
	require.NoError(t, bh.Start())
	defer bh.Stop(ctx)

	// Receive the ControllerClient that was passed to Start.
	var cc beehive.ControllerClient[cStatus]
	select {
	case cc = <-ctrl.clientCh:
	default:
		t.Fatal("controller Start was not called")
	}

	// Create an object and update its status via the ControllerClient.
	client := beehive.NewClient[cSpec, cStatus](bh, clientTestGK)
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
