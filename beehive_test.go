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

// errorGetObjectWatchStore returns an error from GetObject to exercise Watch's
// non-ErrNotFound error path.
type errorGetObjectWatchStore struct {
	fakeStore
}

func (s *errorGetObjectWatchStore) GetObject(_ context.Context, _ ObjectID) (*RawObject, error) {
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

func TestClientWatchSnapshotGetObjectError(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	bh, err := New(&errorGetObjectWatchStore{})
	require.NoError(t, err)
	require.NoError(t, Register(bh, gk, newFakeController()))

	client := NewClient[tSpec, tStatus](bh, gk)
	_, err = client.Watch(context.Background(), 1)
	require.Error(t, err)
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

// dedupListStore blocks ListObjects until listProceed is closed, so the test
// can publish live events BEFORE the snapshot is read, triggering the
// ResourceVersion-dedup continue in watchFiltered's live-event loop.
type dedupListStore struct {
	fakeStore
	listStarted chan struct{}
	listProceed chan struct{}
	listResult  []*RawObject
}

func (s *dedupListStore) ListObjects(_ context.Context, _ GroupKind) ([]*RawObject, error) {
	select {
	case <-s.listStarted: // already closed
	default:
		close(s.listStarted)
	}
	<-s.listProceed
	return s.listResult, nil
}

func TestWatchFilteredDedupContinue(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	const objID ObjectID = 1
	const rv int64 = 42

	store := &dedupListStore{
		listStarted: make(chan struct{}),
		listProceed: make(chan struct{}),
		listResult:  []*RawObject{{ID: objID, Spec: []byte(`{}`), ResourceVersion: rv}},
	}

	bh, err := New(store)
	require.NoError(t, err)
	require.NoError(t, Register(bh, gk, newFakeController()))

	client := NewClient[tSpec, tStatus](bh, gk)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// WatchList is called in a background goroutine because loadSnapshot
	// (which calls store.ListObjects) is synchronous and will block until we
	// signal listProceed.
	var ch <-chan WatchEvent[tSpec, tStatus]
	watchDone := make(chan struct{})
	go func() {
		var watchErr error
		ch, watchErr = client.WatchList(ctx)
		if watchErr != nil {
			t.Errorf("WatchList error: %v", watchErr)
		}
		close(watchDone)
	}()

	// Wait for the WatchList goroutine to enter ListObjects.
	select {
	case <-store.listStarted:
	case <-time.After(testTimeout):
		t.Fatal("ListObjects not called")
	}

	// Publish a live event with rv=42 into the hub while WatchList is blocked
	// on snapshot load. The hub receiver (created before ListObjects is called)
	// captures this event in its buffer.
	bh.publishEvent(gk, WatchEventAdded, &RawObject{ID: objID, Spec: []byte(`{}`), ResourceVersion: rv})

	// Release snapshot load. Snapshot = [{objID, rv=42}], snapshotRV[objID]=42.
	// The background goroutine then processes the buffered event:
	//   rv=42 <= snapshotRV[objID]=42 → dedup → continue.
	close(store.listProceed)

	// Wait for WatchList to return.
	select {
	case <-watchDone:
	case <-time.After(testTimeout):
		t.Fatal("WatchList did not return")
	}

	// Drain the snapshot Added event.
	select {
	case evt, ok := <-ch:
		if !ok {
			t.Fatal("channel closed unexpectedly before snapshot event")
		}
		assert.Equal(t, WatchEventAdded, evt.Type)
		assert.Equal(t, objID, evt.Object.ID)
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for snapshot event")
	}

	// The buffered event (rv=42) was deduped. Cancel to clean up.
	cancel()
	deadline := time.After(testTimeout)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("channel did not close after cancel")
		}
	}
}

// TestWatchFilteredCtxDoneSnapshotSend covers the case <-ctx.Done() branch in
// the snapshot item send select. Cancel the context before the snapshot load
// completes so ctx.Done is already fired when the streaming goroutine tries to
// send the first snapshot item.
func TestWatchFilteredCtxDoneSnapshotSend(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}

	store := &dedupListStore{
		listStarted: make(chan struct{}),
		listProceed: make(chan struct{}),
		listResult:  []*RawObject{{ID: 1, Spec: []byte(`{}`), ResourceVersion: 1}},
	}

	bh, err := New(store)
	require.NoError(t, err)
	require.NoError(t, Register(bh, gk, newFakeController()))

	client := NewClient[tSpec, tStatus](bh, gk)
	ctx, cancel := context.WithCancel(context.Background())

	var ch <-chan WatchEvent[tSpec, tStatus]
	watchDone := make(chan struct{})
	go func() {
		var watchErr error
		ch, watchErr = client.WatchList(ctx)
		if watchErr != nil {
			t.Errorf("WatchList error: %v", watchErr)
		}
		close(watchDone)
	}()

	// Wait for ListObjects to be called.
	select {
	case <-store.listStarted:
	case <-time.After(testTimeout):
		t.Fatal("ListObjects not called")
	}

	// Cancel BEFORE releasing the snapshot load so that ctx.Done is already
	// closed when the streaming goroutine tries to send the snapshot item.
	// No reader on the output channel yet → the only ready arm is ctx.Done.
	cancel()
	close(store.listProceed)

	// WatchList returns the channel.
	select {
	case <-watchDone:
	case <-time.After(testTimeout):
		t.Fatal("WatchList did not return")
	}

	// Channel must close (goroutine exits via ctx.Done in snapshot send select).
	deadline := time.After(testTimeout)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for channel to close")
		}
	}
}

// TestWatchFilteredCtxDoneLiveEventSend covers the case <-ctx.Done() branch in
// the live-event send select. The goroutine is in the live-event loop with a
// buffered event; cancelling ctx before a reader consumes the event makes the
// goroutine take ctx.Done instead of the send arm.
func TestWatchFilteredCtxDoneLiveEventSend(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}

	// dedupListStore with no objects so the snapshot is empty and WatchList
	// returns synchronously, leaving the goroutine in the live-event loop.
	store := &dedupListStore{
		listStarted: make(chan struct{}),
		listProceed: make(chan struct{}),
		listResult:  nil, // empty snapshot
	}

	bh, err := New(store)
	require.NoError(t, err)
	require.NoError(t, Register(bh, gk, newFakeController()))

	client := NewClient[tSpec, tStatus](bh, gk)
	ctx, cancel := context.WithCancel(context.Background())

	var ch <-chan WatchEvent[tSpec, tStatus]
	watchDone := make(chan struct{})
	go func() {
		var watchErr error
		ch, watchErr = client.WatchList(ctx)
		if watchErr != nil {
			t.Errorf("WatchList error: %v", watchErr)
		}
		close(watchDone)
	}()

	// Wait for ListObjects to start.
	select {
	case <-store.listStarted:
	case <-time.After(testTimeout):
		t.Fatal("ListObjects not called")
	}

	// Publish a live event into the hub buffer while WatchList is blocked.
	// After WatchList returns, the goroutine will pick up this buffered event
	// and try to send it to the output channel.
	bh.publishEvent(gk, WatchEventAdded, &RawObject{ID: 1, Spec: []byte(`{}`), ResourceVersion: 1})

	// Cancel ctx BEFORE releasing the snapshot. When the goroutine tries to
	// send the live event: ctx.Done is already ready, and (before the drain
	// loop below starts) there is no reader on ch → takes ctx.Done → exits.
	cancel()
	close(store.listProceed)

	select {
	case <-watchDone:
	case <-time.After(testTimeout):
		t.Fatal("WatchList did not return")
	}

	// Drain any items that were sent, then wait for close.
	deadline := time.After(testTimeout)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for channel to close")
		}
	}
}

func TestWatchFilteredSnapshotLoadError(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	bh, err := New(&errorListObjectsStore{})
	require.NoError(t, err)
	require.NoError(t, Register(bh, gk, newFakeController()))

	client := NewClient[tSpec, tStatus](bh, gk)
	_, err = client.WatchList(context.Background())
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

// badJSONStore is a fakeStore whose ListObjects returns a RawObject with
// invalid spec JSON, used to drive the rawToTyped error path inside watchFiltered.
type badJSONStore struct {
	fakeStore
	gk GroupKind
}

func (s *badJSONStore) ListObjects(_ context.Context, _ GroupKind) ([]*RawObject, error) {
	return []*RawObject{{ID: 1, Group: s.gk.Group, Kind: s.gk.Kind, Spec: []byte("not-json")}}, nil
}

func TestWatchFilteredSnapshotConversionError(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	bh, err := New(&badJSONStore{gk: gk})
	require.NoError(t, err)
	require.NoError(t, Register(bh, gk, newFakeController()))

	client := NewClient[tSpec, tStatus](bh, gk)
	ch, err := client.WatchList(context.Background())
	require.NoError(t, err)

	// rawToTyped fails on the snapshot item → goroutine exits → channel closes.
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to close due to rawToTyped error in snapshot")
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for channel to close")
	}
}

// TestWatchFilteredLiveEventConversionError covers the rawToTyped error branch
// in watchFiltered's live-event loop: a hub event carrying invalid Spec JSON
// must close the channel rather than emit a WatchEvent with a nil Object.
func TestWatchFilteredLiveEventConversionError(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	bh, err := New(&fakeStore{}) // empty snapshot via fakeStore.ListObjects
	require.NoError(t, err)
	require.NoError(t, Register(bh, gk, newFakeController()))

	client := NewClient[tSpec, tStatus](bh, gk)
	ch, err := client.WatchList(context.Background())
	require.NoError(t, err)

	// Publish a live event with bad Spec JSON. resource_version > 0 clears the
	// snapshot dedup guard, so the goroutine reaches rawToTyped, which fails.
	bh.publishEvent(gk, WatchEventModified, &RawObject{ID: 1, Spec: []byte("not-json"), ResourceVersion: 1})

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to close due to rawToTyped error on live event")
		}
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
