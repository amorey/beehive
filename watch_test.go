package beehive_test

import (
	"context"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recvWatch waits for the next event on ch, failing the test if none arrives
// within the failsafe timeout.
func recvWatch[S, T any](t *testing.T, ch <-chan beehive.WatchEvent[S, T]) beehive.WatchEvent[S, T] {
	t.Helper()
	select {
	case evt, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed unexpectedly")
		}
		return evt
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watch event")
		panic("unreachable")
	}
}

// assertChanClosed fails the test if ch does not close within the failsafe timeout.
func assertChanClosed[S, T any](t *testing.T, ch <-chan beehive.WatchEvent[S, T]) {
	t.Helper()
	// Drain any buffered events, then expect close.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for watch channel to close")
		}
	}
}

// watchTestBH builds a Beehive with a real SQLite store and a registered
// controller for clientTestGK. No Start is needed for client-side event tests.
func watchTestBH(t *testing.T) (*beehive.Beehive, beehive.Client[cSpec, cStatus]) {
	t.Helper()
	bh, err := beehive.New(newClientTestStore(t))
	require.NoError(t, err)
	ctrl := newWatchFakeController()
	require.NoError(t, beehive.Register(bh, clientTestGK, ctrl))
	client := beehive.NewClient[cSpec, cStatus](bh, clientTestGK)
	return bh, client
}

// watchFakeController is a minimal controller that captures the ControllerClient
// handed to it during Start, for tests that need to call UpdateStatus directly.
type watchFakeController struct {
	clientCh chan beehive.ControllerClient[cStatus]
}

func newWatchFakeController() *watchFakeController {
	return &watchFakeController{clientCh: make(chan beehive.ControllerClient[cStatus], 1)}
}

func (c *watchFakeController) Start(cc beehive.ControllerClient[cStatus]) error {
	c.clientCh <- cc
	return nil
}
func (c *watchFakeController) Stop(_ context.Context) error { return nil }
func (c *watchFakeController) Reconcile(_ context.Context, _ *beehive.Object[cSpec, cStatus]) (beehive.Result, error) {
	return beehive.Result{}, nil
}

// TestWatchListReceivesAddedOnCreate verifies that WatchList delivers a
// WatchEventAdded when an object is created.
func TestWatchListReceivesAddedOnCreate(t *testing.T) {
	ctx := context.Background()
	_, client := watchTestBH(t)

	ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	obj, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)

	evt := recvWatch(t, ch)
	assert.Equal(t, beehive.WatchEventAdded, evt.Type)
	assert.Equal(t, obj.ID, evt.Object.ID)
	assert.Equal(t, "hello", evt.Object.Spec.Val)
}

// TestWatchListReceivesModifiedOnUpdate verifies that WatchList delivers a
// WatchEventModified when an object's spec is updated.
func TestWatchListReceivesModifiedOnUpdate(t *testing.T) {
	ctx := context.Background()
	_, client := watchTestBH(t)

	// Subscribe before creating so the snapshot is empty and the first event is
	// the Modified from the Update, not an Added from the snapshot.
	ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	obj, err := client.Create(ctx, cSpec{Val: "v1"})
	require.NoError(t, err)
	// Drain the Added event from Create.
	recvWatch(t, ch)

	_, err = client.Update(ctx, obj.ID, cSpec{Val: "v2"})
	require.NoError(t, err)

	evt := recvWatch(t, ch)
	assert.Equal(t, beehive.WatchEventModified, evt.Type)
	assert.Equal(t, obj.ID, evt.Object.ID)
	assert.Equal(t, "v2", evt.Object.Spec.Val)
}

// TestWatchListReceivesModifiedOnDelete verifies that WatchList delivers a
// WatchEventModified (not Deleted) when deletion is requested, because the
// object still exists in the store with DeletionRequestedAt set.
func TestWatchListReceivesModifiedOnDelete(t *testing.T) {
	ctx := context.Background()
	_, client := watchTestBH(t)

	ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	obj, err := client.Create(ctx, cSpec{})
	require.NoError(t, err)
	// Drain the Added event from Create.
	recvWatch(t, ch)

	require.NoError(t, client.Delete(ctx, obj.ID))

	evt := recvWatch(t, ch)
	assert.Equal(t, beehive.WatchEventModified, evt.Type)
	assert.Equal(t, obj.ID, evt.Object.ID)
	assert.NotNil(t, evt.Object.DeletionRequestedAt)
}

// TestWatchListNoEventOnIdempotentDelete verifies that a second Delete call for
// an already-pending-deletion object emits no additional watch event.
func TestWatchListNoEventOnIdempotentDelete(t *testing.T) {
	ctx := context.Background()
	_, client := watchTestBH(t)

	ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	obj, err := client.Create(ctx, cSpec{})
	require.NoError(t, err)
	recvWatch(t, ch) // drain Added

	require.NoError(t, client.Delete(ctx, obj.ID))
	recvWatch(t, ch) // drain first Modified

	// Second Delete is idempotent; no new event should arrive.
	require.NoError(t, client.Delete(ctx, obj.ID))
	select {
	case evt, ok := <-ch:
		if ok {
			t.Fatalf("unexpected event on idempotent delete: %v", evt)
		}
	case <-time.After(100 * time.Millisecond):
		// correct — nothing arrived
	}
}

// TestWatchReceivesOnlyMatchingID verifies that Watch(id) filters out events
// for other objects.
func TestWatchReceivesOnlyMatchingID(t *testing.T) {
	ctx := context.Background()
	_, client := watchTestBH(t)

	obj1, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)
	obj2, err := client.Create(ctx, cSpec{Val: "b"})
	require.NoError(t, err)

	ch, err := client.Watch(ctx, obj1.ID)
	require.NoError(t, err)

	// Drain the initial snapshot Added event for obj1.
	snap := recvWatch(t, ch)
	assert.Equal(t, beehive.WatchEventAdded, snap.Type)
	assert.Equal(t, obj1.ID, snap.Object.ID)

	// Update obj2 first — this event must not appear on ch.
	_, err = client.Update(ctx, obj2.ID, cSpec{Val: "b2"})
	require.NoError(t, err)

	// Update obj1 — this must appear.
	_, err = client.Update(ctx, obj1.ID, cSpec{Val: "a2"})
	require.NoError(t, err)

	evt := recvWatch(t, ch)
	assert.Equal(t, obj1.ID, evt.Object.ID)
	assert.Equal(t, "a2", evt.Object.Spec.Val)
}

// TestWatchListClosesOnCtxCancel verifies that the watch channel is closed when
// the context is cancelled.
func TestWatchListClosesOnCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, client := watchTestBH(t)

	ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	cancel()
	assertChanClosed(t, ch)
}

// TestWatchClosesOnCtxCancel verifies that Watch(id) channel closes on ctx cancel.
func TestWatchClosesOnCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, client := watchTestBH(t)

	obj, err := client.Create(context.Background(), cSpec{})
	require.NoError(t, err)

	ch, err := client.Watch(ctx, obj.ID)
	require.NoError(t, err)

	cancel()
	assertChanClosed(t, ch)
}

// TestWatchReceivesModifiedOnStatusUpdate verifies that WatchList delivers a
// WatchEventModified when the controller calls UpdateStatus.
func TestWatchReceivesModifiedOnStatusUpdate(t *testing.T) {
	ctx := context.Background()

	ctrl := newWatchFakeController()
	// Re-register with our capturing controller.
	// watchTestBH already registered one; we need a fresh beehive for this test.
	bh2, err := beehive.New(newClientTestStore(t))
	require.NoError(t, err)
	require.NoError(t, beehive.Register(bh2, clientTestGK, ctrl))
	client2 := beehive.NewClient[cSpec, cStatus](bh2, clientTestGK)

	require.NoError(t, bh2.Start())
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		bh2.Stop(stopCtx)
	}()

	// Capture the ControllerClient from the Start callback.
	var cc beehive.ControllerClient[cStatus]
	select {
	case cc = <-ctrl.clientCh:
	case <-time.After(2 * time.Second):
		t.Fatal("controller Start never called")
	}

	obj, err := client2.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	// Subscribe after create: the snapshot emits Added(obj) first, then we
	// expect Modified from UpdateStatus.
	ch, err := client2.WatchList(ctx)
	require.NoError(t, err)

	// Drain the initial snapshot Added event.
	snap := recvWatch(t, ch)
	assert.Equal(t, beehive.WatchEventAdded, snap.Type)
	assert.Equal(t, obj.ID, snap.Object.ID)

	require.NoError(t, cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"}))

	evt := recvWatch(t, ch)
	assert.Equal(t, beehive.WatchEventModified, evt.Type)
	assert.Equal(t, obj.ID, evt.Object.ID)
	require.NotNil(t, evt.Object.Status)
	assert.Equal(t, "done", evt.Object.Status.Val)
}

// TestWatchListInitialSnapshot verifies that WatchList emits Added events for
// objects that already exist in the store at subscription time.
func TestWatchListInitialSnapshot(t *testing.T) {
	ctx := context.Background()
	_, client := watchTestBH(t)

	a, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)
	b, err := client.Create(ctx, cSpec{Val: "b"})
	require.NoError(t, err)

	ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	// Two snapshot Added events must arrive, one per existing object.
	seen := map[beehive.ObjectID]string{}
	for range 2 {
		evt := recvWatch(t, ch)
		assert.Equal(t, beehive.WatchEventAdded, evt.Type)
		seen[evt.Object.ID] = evt.Object.Spec.Val
	}
	assert.Equal(t, "a", seen[a.ID])
	assert.Equal(t, "b", seen[b.ID])
}

// TestWatchInitialSnapshot verifies that Watch(id) emits an Added event for an
// object that already exists in the store at subscription time.
func TestWatchInitialSnapshot(t *testing.T) {
	ctx := context.Background()
	_, client := watchTestBH(t)

	obj, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)

	ch, err := client.Watch(ctx, obj.ID)
	require.NoError(t, err)

	evt := recvWatch(t, ch)
	assert.Equal(t, beehive.WatchEventAdded, evt.Type)
	assert.Equal(t, obj.ID, evt.Object.ID)
	assert.Equal(t, "hello", evt.Object.Spec.Val)
}

// TestStartAfterStopErrors verifies that Beehive is a one-shot object: calling
// Start after Stop returns an error instead of silently reusing closed hubs.
func TestStartAfterStopErrors(t *testing.T) {
	ctx := context.Background()
	bh, err := beehive.New(newClientTestStore(t))
	require.NoError(t, err)
	require.NoError(t, beehive.Register(bh, clientTestGK, newWatchFakeController()))

	require.NoError(t, bh.Start())
	stopCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	bh.Stop(stopCtx)
	cancel()

	err = bh.Start()
	require.Error(t, err, "Start after Stop must return an error")
}

// TestWatchListErrForUnregisteredKind verifies that WatchList returns an error
// (not a panic) when no controller is registered for the given GroupKind.
func TestWatchListErrForUnregisteredKind(t *testing.T) {
	ctx := context.Background()
	bh, err := beehive.New(newClientTestStore(t))
	require.NoError(t, err)

	unknownGK := beehive.GroupKind{Kind: "Unknown"}
	client := beehive.NewClient[cSpec, cStatus](bh, unknownGK)

	_, err = client.WatchList(ctx)
	require.Error(t, err)

	_, err = client.Watch(ctx, 0)
	require.Error(t, err)
}
