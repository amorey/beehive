package beehive

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errBoom = errors.New("boom")

func TestNewAppliesDefaults(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)
	assert.Equal(t, defaultResyncInterval, bh.resyncInterval)
	assert.NotNil(t, bh.reconcilers)
}

func TestNewPropagatesOptionError(t *testing.T) {
	_, err := New(&fakeStore{}, func(any) error { return errBoom })
	require.ErrorIs(t, err, errBoom)
}

func TestRegisterStoresReconciler(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)

	gk := GroupKind{Kind: "Widget"}
	require.NoError(t, Register(bh, gk, newFakeController()))

	r, ok := bh.reconcilers[gk]
	require.True(t, ok, "reconciler should be registered under its GroupKind")
	assert.Equal(t, gk, r.gk)
	assert.Equal(t, defaultResyncInterval, r.resyncInterval, "inherits the Beehive default")
	assert.Equal(t, defaultMaxRetryInterval, r.maxRetryInterval)
}

func TestRegisterRejectsDuplicate(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)

	gk := GroupKind{Kind: "Widget"}
	require.NoError(t, Register(bh, gk, newFakeController()))
	require.Error(t, Register(bh, gk, newFakeController()))
}

func TestRegisterRejectedAfterStart(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)
	require.NoError(t, bh.Start())
	defer bh.Stop(context.Background())

	require.Error(t, Register(bh, GroupKind{Kind: "Widget"}, newFakeController()))
}

func TestRegisterPerControllerOverride(t *testing.T) {
	// Global default set at New; one controller overrides it, another inherits.
	bh, err := New(&fakeStore{}, WithResyncInterval(10*time.Second))
	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, bh.resyncInterval)

	overridden := GroupKind{Kind: "Overridden"}
	require.NoError(t, Register(bh, overridden, newFakeController(),
		WithResyncInterval(2*time.Second), WithMaxRetryInterval(7*time.Second)))

	inherited := GroupKind{Kind: "Inherited"}
	require.NoError(t, Register(bh, inherited, newFakeController()))

	assert.Equal(t, 2*time.Second, bh.reconcilers[overridden].resyncInterval)
	assert.Equal(t, 7*time.Second, bh.reconcilers[overridden].maxRetryInterval)
	assert.Equal(t, 10*time.Second, bh.reconcilers[inherited].resyncInterval,
		"controller without an override inherits the Beehive default")
}

func TestStartStopLifecycle(t *testing.T) {
	// Disable resync so the reconcile loop just blocks on ctx until Stop.
	bh, err := New(&fakeStore{}, WithResyncInterval(0))
	require.NoError(t, err)

	fc := newFakeController()
	require.NoError(t, Register(bh, GroupKind{Kind: "Widget"}, fc))

	require.NoError(t, bh.Start())
	waitClosed(t, fc.startedCh, "controller Start")
	assert.Equal(t, beehiveRunning, bh.state)

	bh.Stop(context.Background())
	waitClosed(t, fc.stoppedCh, "controller Stop")
	assert.Equal(t, 1, fc.startCount())
	assert.Equal(t, 1, fc.stopCount())
	assert.Equal(t, beehiveStopped, bh.state)
}

func TestStartRejectsSecondStart(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)

	require.NoError(t, bh.Start())
	defer bh.Stop(context.Background())
	require.Error(t, bh.Start())
}

func TestStopWithoutStartIsNoOp(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)

	fc := newFakeController()
	require.NoError(t, Register(bh, GroupKind{Kind: "Widget"}, fc))

	bh.Stop(context.Background()) // never started: must not panic or stop controllers
	assert.Equal(t, 0, fc.stopCount())
}

func TestStopReturnsWithExpiredContext(t *testing.T) {
	bh, err := New(&fakeStore{}, WithResyncInterval(0))
	require.NoError(t, err)

	fc := newFakeController()
	require.NoError(t, Register(bh, GroupKind{Kind: "Widget"}, fc))
	require.NoError(t, bh.Start())
	waitClosed(t, fc.startedCh, "controller Start")

	// An already-expired ctx caps the drain wait. Stop must still return (the
	// test completing proves no hang) and still stop the controllers.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	bh.Stop(ctx)

	assert.Equal(t, 1, fc.stopCount())
	assert.Equal(t, beehiveStopped, bh.state)
}

// createBadJSONStore returns bad JSON from CreateObject so rawToTyped fails.
type createBadJSONStore struct {
	fakeStore
}

func (s *createBadJSONStore) CreateObject(_ context.Context, _ *RawObject) (*RawObject, error) {
	return &RawObject{ID: 1, Spec: []byte("not-json")}, nil
}

// errorCreateObjectStore returns an error from CreateObject.
type errorCreateObjectStore struct {
	fakeStore
}

func (s *errorCreateObjectStore) CreateObject(_ context.Context, _ *RawObject) (*RawObject, error) {
	return nil, errBoom
}

// updateBadJSONStore returns bad JSON from UpdateSpec so rawToTyped fails.
type updateBadJSONStore struct {
	fakeStore
}

func (s *updateBadJSONStore) UpdateSpec(_ context.Context, _ ObjectID, _ []byte) (*RawObject, error) {
	return &RawObject{ID: 1, Spec: []byte("not-json")}, nil
}

// errorUpdateSpecStore returns an error from UpdateSpec.
type errorUpdateSpecStore struct {
	fakeStore
}

func (s *errorUpdateSpecStore) UpdateSpec(_ context.Context, _ ObjectID, _ []byte) (*RawObject, error) {
	return nil, errBoom
}

// errorListObjectsStore returns an error from ListObjects.
type errorListObjectsStore struct {
	fakeStore
}

func (s *errorListObjectsStore) ListObjects(_ context.Context, _ GroupKind) ([]*RawObject, error) {
	return nil, errBoom
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

func TestClientCreateStoreError(t *testing.T) {
	bh, err := New(&errorCreateObjectStore{})
	require.NoError(t, err)
	client := NewClient[tSpec, tStatus](bh, GroupKind{Kind: "Widget"})
	_, err = client.Create(context.Background(), tSpec{})
	require.Error(t, err)
}

func TestClientCreateRawToTypedError(t *testing.T) {
	bh, err := New(&createBadJSONStore{})
	require.NoError(t, err)
	client := NewClient[tSpec, tStatus](bh, GroupKind{Kind: "Widget"})
	_, err = client.Create(context.Background(), tSpec{})
	require.Error(t, err)
}

func TestClientUpdateStoreError(t *testing.T) {
	bh, err := New(&errorUpdateSpecStore{})
	require.NoError(t, err)
	client := NewClient[tSpec, tStatus](bh, GroupKind{Kind: "Widget"})
	_, err = client.Update(context.Background(), 1, tSpec{})
	require.Error(t, err)
}

func TestClientUpdateRawToTypedError(t *testing.T) {
	bh, err := New(&updateBadJSONStore{})
	require.NoError(t, err)
	client := NewClient[tSpec, tStatus](bh, GroupKind{Kind: "Widget"})
	_, err = client.Update(context.Background(), 1, tSpec{})
	require.Error(t, err)
}

// TestClientWatchPropagatesStoreError verifies the client surfaces an error
// returned by the store's Watch/WatchList (e.g. a failed snapshot load).
func TestClientWatchPropagatesStoreError(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	bh, err := New(&watcherStore{err: errBoom})
	require.NoError(t, err)
	require.NoError(t, Register(bh, gk, newFakeController()))

	client := NewClient[tSpec, tStatus](bh, gk)
	_, err = client.Watch(context.Background(), 1)
	require.ErrorIs(t, err, errBoom)
	_, err = client.WatchList(context.Background())
	require.ErrorIs(t, err, errBoom)
}

func TestClientListStoreError(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	bh, err := New(&errorListObjectsStore{})
	require.NoError(t, err)
	client := NewClient[tSpec, tStatus](bh, gk)
	_, err = client.List(context.Background())
	require.Error(t, err)
}

func TestClientListRawToTypedError(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	bh, err := New(&badJSONStore{gk: gk})
	require.NoError(t, err)
	client := NewClient[tSpec, tStatus](bh, gk)
	_, err = client.List(context.Background())
	require.Error(t, err)
}

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

// TestStartRollsBackStartedController exercises the rollback loop body in Start:
// the good controller, registered first, starts; the bad one then fails, and the
// good one must be rolled back. Start iterates in registration order, so this is
// deterministic.
func TestStartRollsBackStartedController(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)

	good := newFakeController()
	bad := &fakeController{startedCh: make(chan struct{}), stoppedCh: make(chan struct{}), startErr: errBoom}

	require.NoError(t, Register(bh, GroupKind{Kind: "Good"}, good))
	require.NoError(t, Register(bh, GroupKind{Kind: "Bad"}, bad))

	require.ErrorIs(t, bh.Start(), errBoom)

	// good started before bad failed, so it must have been rolled back.
	assert.Equal(t, 1, good.startCount())
	assert.Equal(t, 1, good.stopCount(), "a started controller must be rolled back")
	// bad's Start failed, so it is never added to the started set and not stopped.
	assert.Equal(t, 0, bad.stopCount())
}

func TestRegisterPropagatesOptionError(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)
	err = Register(bh, GroupKind{Kind: "Widget"}, newFakeController(), func(any) error { return errBoom })
	require.ErrorIs(t, err, errBoom)
}

// badJSONStore is a fakeStore whose ListObjects returns a RawObject with invalid
// spec JSON, used to drive the rawToTyped error path inside client.List.
type badJSONStore struct {
	fakeStore
	gk GroupKind
}

func (s *badJSONStore) ListObjects(_ context.Context, _ GroupKind) ([]*RawObject, error) {
	return []*RawObject{{ID: 1, Group: s.gk.Group, Kind: s.gk.Kind, Spec: []byte("not-json")}}, nil
}

// newWatchClient registers gk with a fake controller (so the client-side
// isRegistered check passes) and returns a client backed by store.
func newWatchClient(t *testing.T, store Store, gk GroupKind) Client[tSpec, tStatus] {
	t.Helper()
	bh, err := New(store)
	require.NoError(t, err)
	require.NoError(t, Register(bh, gk, newFakeController()))
	return NewClient[tSpec, tStatus](bh, gk)
}

// TestClientAdaptWatcherConversionError verifies a raw event whose Spec is
// invalid JSON closes the typed channel rather than emitting a bad WatchEvent.
func TestClientAdaptWatcherConversionError(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	w := newFakeWatcher()
	client := newWatchClient(t, &watcherStore{w: w}, gk)

	ch, err := client.WatchList(context.Background())
	require.NoError(t, err)

	w.push(WatchEventModified, &RawObject{ID: 1, Spec: []byte("not-json")})

	select {
	case _, ok := <-ch:
		assert.False(t, ok, "channel must close on rawToTyped error")
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for channel to close")
	}
}

// TestClientAdaptWatcherForwardsThenClosesOnCancel verifies a decodable event is
// forwarded as a typed WatchEvent, and cancelling the context closes the channel.
func TestClientAdaptWatcherForwardsThenClosesOnCancel(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	w := newFakeWatcher()
	client := newWatchClient(t, &watcherStore{w: w}, gk)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	w.push(WatchEventAdded, &RawObject{ID: 1, Spec: []byte(`{}`)})
	select {
	case evt, ok := <-ch:
		require.True(t, ok)
		assert.Equal(t, WatchEventAdded, evt.Type)
		assert.EqualValues(t, 1, evt.Object.ID)
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for forwarded event")
	}

	cancel()
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "channel must close on ctx cancel")
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for channel to close")
	}
}

// TestClientAdaptWatcherSendParkCtxDone covers the adapter exiting on ctx
// cancellation while parked sending a typed event: an event is delivered to the
// adapter but never read downstream, then the context is cancelled.
func TestClientAdaptWatcherSendParkCtxDone(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	w := newFakeWatcher()
	client := newWatchClient(t, &watcherStore{w: w}, gk)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	// push returns once the adapter has taken the event; with no reader on ch it
	// then parks on its inner send. Cancelling makes that send take the ctx.Done
	// arm. Synchronize on the goroutine's exit (Close) rather than reading ch:
	// a read here could satisfy the pending send and race the closed-vs-delivered
	// outcome (notably under -race).
	w.push(WatchEventAdded, &RawObject{ID: 1, Spec: []byte(`{}`)})
	cancel()
	select {
	case <-w.closed:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for adapter goroutine to exit")
	}

	select {
	case _, ok := <-ch:
		assert.False(t, ok, "channel must close when ctx is cancelled mid-send")
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for channel to close")
	}
}

// TestClientAdaptWatcherClosesWhenStreamEnds verifies the typed channel closes
// when the underlying store watcher's stream ends.
func TestClientAdaptWatcherClosesWhenStreamEnds(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	w := newFakeWatcher()
	client := newWatchClient(t, &watcherStore{w: w}, gk)

	ch, err := client.WatchList(context.Background())
	require.NoError(t, err)

	w.endStream()
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "channel must close when the watcher stream ends")
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for channel to close")
	}
}

func TestStartRollsBackOnFailure(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)

	good := newFakeController()
	bad := newFakeController()
	bad.startErr = errBoom
	require.NoError(t, Register(bh, GroupKind{Kind: "Good"}, good))
	require.NoError(t, Register(bh, GroupKind{Kind: "Bad"}, bad))

	require.ErrorIs(t, bh.Start(), errBoom)
	assert.Equal(t, beehiveNew, bh.state)

	// Map iteration order is randomized, so we can't say whether `good` started
	// before `bad` failed. Assert the order-independent invariant instead: any
	// controller that started successfully must have been rolled back (Stopped).
	if good.startCount() > 0 {
		assert.Equal(t, 1, good.stopCount(), "a started controller must be rolled back")
	}
	// The controller whose Start failed is never added to the started set, so it
	// is not stopped.
	assert.Equal(t, 0, bad.stopCount())
}
