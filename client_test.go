package beehive

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/amorey/beehive/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type cSpec struct{ Val string }
type cStatus struct{ Val string }

var clientTestGK = GroupKind{Kind: "Widget"}

func newClientTestStore(t *testing.T) Store {
	t.Helper()
	s, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

// errMarshaler is a type whose JSON marshaling always fails, used to exercise
// the json.Marshal error paths in Create and Update.
type errMarshaler struct{}

func (errMarshaler) MarshalJSON() ([]byte, error) { return nil, errors.New("cannot marshal") }

func TestClientCreateMarshalError(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[errMarshaler, cStatus](bh, clientTestGK)
	_, err = client.Create(ctx, errMarshaler{})
	require.Error(t, err)
}

func TestClientUpdateMarshalError(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[errMarshaler, cStatus](bh, clientTestGK)
	_, err = client.Update(ctx, 1, errMarshaler{})
	require.Error(t, err)
}

func TestClientCreate(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)
	assert.NotZero(t, obj.ID)
	assert.Equal(t, clientTestGK.Group, obj.Group)
	assert.Equal(t, clientTestGK.Kind, obj.Kind)
	assert.Equal(t, int64(1), obj.Generation)
	assert.Nil(t, obj.Status)
	assert.Equal(t, "hello", obj.Spec.Val)
}

func TestClientCreateWithOptions(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	// An owner must exist before a child can ref it.
	owner, err := client.Create(ctx, cSpec{Val: "owner"})
	require.NoError(t, err)

	child, err := client.Create(ctx, cSpec{Val: "child"},
		WithName("child-1"),
		WithFinalizers("cleanup-a", "cleanup-b"),
		WithOwner(owner.ID))
	require.NoError(t, err)

	require.NotNil(t, child.Name)
	assert.Equal(t, "child-1", *child.Name)
	assert.Equal(t, []string{"cleanup-a", "cleanup-b"}, child.Finalizers)

	// Name is persisted and looked up via GetByName.
	got, err := client.GetByName(ctx, "child-1")
	require.NoError(t, err)
	assert.Equal(t, child.ID, got.ID)
	assert.Equal(t, []string{"cleanup-a", "cleanup-b"}, got.Finalizers)

	// The owner ref is recorded child -> owner, so the owner sees the child.
	refs, err := store.ListReferrers(ctx, owner.ID, RelationOwnedBy)
	require.NoError(t, err)
	require.Len(t, refs, 1)
	assert.Equal(t, child.ID, refs[0].ID)
}

func TestClientGet(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	created, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)

	got, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, created.ID, got.ID)
	assert.Equal(t, "hello", got.Spec.Val)
	assert.Nil(t, got.Status)
}

func TestClientGetByName(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, err = client.GetByName(ctx, "nonexistent")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestClientList(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	a, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)
	b, err := client.Create(ctx, cSpec{Val: "b"})
	require.NoError(t, err)

	list, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, list, 2)
	assert.Equal(t, a.ID, list[0].ID)
	assert.Equal(t, b.ID, list[1].ID)
}

func TestClientUpdate(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	created, err := client.Create(ctx, cSpec{Val: "v1"})
	require.NoError(t, err)

	updated, err := client.Update(ctx, created.ID, cSpec{Val: "v2"})
	require.NoError(t, err)
	assert.Equal(t, created.ID, updated.ID)
	assert.Equal(t, int64(2), updated.Generation)
	assert.Equal(t, "v2", updated.Spec.Val)
}

func TestClientGetNotFound(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, err = client.Get(ctx, 999)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestClientGetByNameFound(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)

	// Create a named object via the store directly (client.Create uses nil name).
	specJSON, err := json.Marshal(cSpec{Val: "hello"})
	require.NoError(t, err)
	raw, err := store.CreateObject(ctx, &RawObject{
		Group: clientTestGK.Group, Kind: clientTestGK.Kind,
		Name: new("myobj"), Spec: specJSON,
	})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	got, err := client.GetByName(ctx, "myobj")
	require.NoError(t, err)
	assert.Equal(t, raw.ID, got.ID)
	assert.Equal(t, "hello", got.Spec.Val)
}

func TestClientWatchNonExistentID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, client := watchTestBH(t)

	// Watch a non-existent ID: the snapshot loader returns (nil, nil) via the
	// ErrNotFound path, yielding an empty snapshot and an open channel.
	ch, err := client.Watch(ctx, 9999)
	require.NoError(t, err)

	// Cancel ctx — channel must close cleanly (no events, just the cancel).
	cancel()
	assertChanClosed(t, ch)
}

func TestClientDeleteNotFound(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	err = client.Delete(ctx, 999)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestClientDelete(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	created, err := client.Create(ctx, cSpec{})
	require.NoError(t, err)

	err = client.Delete(ctx, created.ID)
	require.NoError(t, err)

	// object still present (no finalizers cleared), but marked for deletion
	got, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	assert.NotNil(t, got.DeletionRequestedAt)
}

// TestClientIDOpsScopedToKind verifies that ID-based operations on a Client are
// confined to that client's kind: an id naming an object of another kind is
// invisible (Get/Update/Delete all report ErrNotFound) and the foreign object is
// left untouched, never updated or marked for deletion through the wrong client.
func TestClientIDOpsScopedToKind(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	widgets := NewClient[cSpec, cStatus](bh, GroupKind{Kind: "Widget"})
	gadgets := NewClient[cSpec, cStatus](bh, GroupKind{Kind: "Gadget"})

	w, err := widgets.Create(ctx, cSpec{Val: "v1"})
	require.NoError(t, err)

	// The Gadget client must not see or mutate the Widget by its id.
	_, err = gadgets.Get(ctx, w.ID)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = gadgets.Update(ctx, w.ID, cSpec{Val: "hijacked"})
	require.ErrorIs(t, err, ErrNotFound)
	err = gadgets.Delete(ctx, w.ID)
	require.ErrorIs(t, err, ErrNotFound)

	// The Widget is unchanged: original spec, no deletion request.
	got, err := widgets.Get(ctx, w.ID)
	require.NoError(t, err)
	assert.Equal(t, "v1", got.Spec.Val)
	assert.Equal(t, int64(1), got.Generation)
	assert.Nil(t, got.DeletionRequestedAt)
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

// GetObject satisfies the client's pre-write kind check (scopedGet) with a row of
// the test's "Widget" kind, so the update reaches UpdateSpec.
func (s *updateBadJSONStore) GetObject(_ context.Context, id ObjectID) (*RawObject, error) {
	return &RawObject{ID: id, Kind: "Widget"}, nil
}

func (s *updateBadJSONStore) UpdateSpec(_ context.Context, _ ObjectID, _ []byte) (*RawObject, error) {
	return &RawObject{ID: 1, Spec: []byte("not-json")}, nil
}

// errorUpdateSpecStore returns an error from UpdateSpec.
type errorUpdateSpecStore struct {
	fakeStore
}

// GetObject lets scopedGet's kind check pass so the update reaches UpdateSpec.
func (s *errorUpdateSpecStore) GetObject(_ context.Context, id ObjectID) (*RawObject, error) {
	return &RawObject{ID: id, Kind: "Widget"}, nil
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

// recvWatch waits for the next event on ch, failing the test if none arrives
// within the failsafe timeout.
func recvWatch[S, T any](t *testing.T, ch <-chan WatchEvent[S, T]) WatchEvent[S, T] {
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
func assertChanClosed[S, T any](t *testing.T, ch <-chan WatchEvent[S, T]) {
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
func watchTestBH(t *testing.T) (*Beehive, Client[cSpec, cStatus]) {
	t.Helper()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	ctrl := newWatchFakeController()
	require.NoError(t, Register(bh, clientTestGK, ctrl))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	return bh, client
}

// watchFakeController is a minimal controller that captures the ControllerClient
// handed to it during Start, for tests that need to call UpdateStatus directly.
type watchFakeController struct {
	clientCh chan ControllerClient[cStatus]
}

func newWatchFakeController() *watchFakeController {
	return &watchFakeController{clientCh: make(chan ControllerClient[cStatus], 1)}
}

func (c *watchFakeController) Start(cc ControllerClient[cStatus]) error {
	c.clientCh <- cc
	return nil
}
func (c *watchFakeController) Stop(_ context.Context) error { return nil }
func (c *watchFakeController) Reconcile(_ context.Context, _ *Object[cSpec, cStatus]) (Result, error) {
	return Result{}, nil
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
	assert.Equal(t, WatchEventAdded, evt.Type)
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
	assert.Equal(t, WatchEventModified, evt.Type)
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
	assert.Equal(t, WatchEventModified, evt.Type)
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
	assert.Equal(t, WatchEventAdded, snap.Type)
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
	bh2, err := New(newClientTestStore(t))
	require.NoError(t, err)
	require.NoError(t, Register(bh2, clientTestGK, ctrl))
	client2 := NewClient[cSpec, cStatus](bh2, clientTestGK)

	require.NoError(t, bh2.Start())
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		bh2.Stop(stopCtx)
	}()

	// Capture the ControllerClient from the Start callback.
	var cc ControllerClient[cStatus]
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
	assert.Equal(t, WatchEventAdded, snap.Type)
	assert.Equal(t, obj.ID, snap.Object.ID)

	require.NoError(t, cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"}))

	evt := recvWatch(t, ch)
	assert.Equal(t, WatchEventModified, evt.Type)
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
	seen := map[ObjectID]string{}
	for range 2 {
		evt := recvWatch(t, ch)
		assert.Equal(t, WatchEventAdded, evt.Type)
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
	assert.Equal(t, WatchEventAdded, evt.Type)
	assert.Equal(t, obj.ID, evt.Object.ID)
	assert.Equal(t, "hello", evt.Object.Spec.Val)
}

// TestStartAfterStopErrors verifies that Beehive is a one-shot object: calling
// Start after Stop returns an error instead of silently reusing closed hubs.
func TestStartAfterStopErrors(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	require.NoError(t, Register(bh, clientTestGK, newWatchFakeController()))

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
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	unknownGK := GroupKind{Kind: "Unknown"}
	client := NewClient[cSpec, cStatus](bh, unknownGK)

	_, err = client.WatchList(ctx)
	require.Error(t, err)

	_, err = client.Watch(ctx, 0)
	require.Error(t, err)
}
