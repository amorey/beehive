package beehive

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/amorey/beehive/internal/storeapi"
)

// testTimeout is a failsafe only: a select that waits this long has hung, so we
// fail rather than block forever. Tests never rely on it to pace anything.
const testTimeout = 2 * time.Second

// tSpec / tStatus are placeholder payload types. The lifecycle tests never
// inspect them; they exist only to satisfy the generic signatures.
type (
	tSpec   struct{}
	tStatus struct{}
)

// fakeStore is a no-op Store. New only stashes the store, so Close is never
// reached by these tests, but we record it anyway for completeness.
type fakeStore struct {
	mu     sync.Mutex
	closed bool
}

func (s *fakeStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// The lifecycle tests never reach the store's read/write surface (no reconcile
// is dispatched and no client call is made), so these satisfy the interface
// without behavior. A test that needs real store semantics uses sqlite instead.
func (s *fakeStore) Within(context.Context, func(context.Context) error) error {
	panic("not implemented: fakeStore.Within")
}
func (s *fakeStore) CreateObject(context.Context, *RawObject) (*RawObject, error) {
	panic("not implemented: fakeStore.CreateObject")
}
func (s *fakeStore) GetObject(context.Context, ObjectID) (*RawObject, error) {
	panic("not implemented: fakeStore.GetObject")
}
func (s *fakeStore) GetObjectByName(context.Context, GroupKind, string) (*RawObject, error) {
	panic("not implemented: fakeStore.GetObjectByName")
}
func (s *fakeStore) ListObjects(context.Context, GroupKind) ([]*RawObject, error) {
	return nil, nil
}
func (s *fakeStore) ListUnsettledIDs(context.Context, GroupKind) ([]ObjectID, error) {
	return nil, nil
}
func (s *fakeStore) UpdateSpec(context.Context, ObjectID, []byte) (*RawObject, error) {
	panic("not implemented: fakeStore.UpdateSpec")
}
func (s *fakeStore) UpdateStatus(context.Context, ObjectID, int64, []byte) (*RawObject, error) {
	panic("not implemented: fakeStore.UpdateStatus")
}
func (s *fakeStore) RequestDeletion(context.Context, ObjectID) (*RawObject, bool, error) {
	panic("not implemented: fakeStore.RequestDeletion")
}
func (s *fakeStore) DeleteObject(context.Context, ObjectID) error {
	panic("not implemented: fakeStore.DeleteObject")
}

// Watch/WatchList default to a noopWatcher (never fires, no-op Close) rather
// than panicking, so client tests that only exercise the snapshot or
// registration error paths reach their target without each fake overriding them.
func (s *fakeStore) Watch(context.Context, GroupKind, ObjectID) (Watcher, error) {
	return noopWatcher{}, nil
}
func (s *fakeStore) WatchList(context.Context, GroupKind) (Watcher, error) {
	return noopWatcher{}, nil
}

// noopWatcher is a Watcher whose event stream never fires; Close is a no-op.
type noopWatcher struct{}

func (noopWatcher) Events() <-chan storeapi.RawWatchEvent { return nil }
func (noopWatcher) Close()                                {}

// watcherStore is a fakeStore whose Watch/WatchList return a preset Watcher and
// error, so client-layer tests can drive the typed-adapter goroutine directly.
type watcherStore struct {
	fakeStore
	w   Watcher
	err error
}

func (s *watcherStore) Watch(context.Context, GroupKind, ObjectID) (Watcher, error) {
	return s.w, s.err
}
func (s *watcherStore) WatchList(context.Context, GroupKind) (Watcher, error) {
	return s.w, s.err
}

// fakeWatcher is a controllable Watcher: push feeds a raw event, endStream ends
// the stream, and Close signals the adapter goroutine's exit. It backs the
// client adaptWatcher tests.
type fakeWatcher struct {
	ch        chan storeapi.RawWatchEvent
	closed    chan struct{}
	closeOnce sync.Once
}

func newFakeWatcher() *fakeWatcher {
	return &fakeWatcher{ch: make(chan storeapi.RawWatchEvent), closed: make(chan struct{})}
}

func (w *fakeWatcher) Events() <-chan storeapi.RawWatchEvent { return w.ch }

// Close (called by adaptWatcher's defer on exit) closes closed, letting tests
// synchronize on goroutine exit instead of reading Events — which could itself
// satisfy a pending send and race the outcome.
func (w *fakeWatcher) Close() { w.closeOnce.Do(func() { close(w.closed) }) }

// push delivers a raw event to the adapter goroutine.
func (w *fakeWatcher) push(typ WatchEventType, obj *RawObject) {
	w.ch <- storeapi.RawWatchEvent{Type: typ, Object: obj}
}

// endStream closes the event channel, signalling the stream has ended.
func (w *fakeWatcher) endStream() { close(w.ch) }

// fakeController is a test double for Controller. It counts Start/Stop calls and
// closes channels when they happen, so tests synchronize on those events
// instead of sleeping. Reconcile is never dispatched yet, so it's a no-op.
type fakeController struct {
	startErr error // if set, Start fails (to exercise start rollback)

	mu         sync.Mutex
	startCalls int
	stopCalls  int

	startedCh chan struct{} // closed after the first successful Start
	stoppedCh chan struct{} // closed on the first Stop
}

func newFakeController() *fakeController {
	return &fakeController{
		startedCh: make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
}

func (f *fakeController) Start(_ ControllerClient[tStatus]) error {
	f.mu.Lock()
	f.startCalls++
	first := f.startCalls == 1
	f.mu.Unlock()
	if f.startErr != nil {
		return f.startErr
	}
	if first {
		close(f.startedCh)
	}
	return nil
}

func (f *fakeController) Stop(_ context.Context) error {
	f.mu.Lock()
	f.stopCalls++
	first := f.stopCalls == 1
	f.mu.Unlock()
	if first {
		close(f.stoppedCh)
	}
	return nil
}

func (f *fakeController) Reconcile(_ context.Context, _ *Object[tSpec, tStatus]) (Result, error) {
	return Result{}, nil
}

func (f *fakeController) startCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.startCalls
}

func (f *fakeController) stopCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopCalls
}

// waitClosed blocks until ch is closed, failing the test if that takes longer
// than the failsafe timeout (i.e. the expected event never happened).
func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(testTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}
