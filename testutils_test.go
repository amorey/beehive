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
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/amorey/beehive/internal/storeapi"
)

// errBoom is a sentinel error shared by tests that exercise error-propagation
// paths (option failures, store failures, controller reconcile errors).
var errBoom = errors.New("boom")

// fakeMigrator is a configurable Migrator test double. The version methods
// return the configured ints; the converters delegate to the supplied funcs, or
// act as identity when nil. Shared by the registry, conversion, and stamping
// tests.
type fakeMigrator struct {
	specVersion   int
	statusVersion int
	convertSpec   func(from int, raw json.RawMessage) (json.RawMessage, error)
	convertStatus func(from int, raw json.RawMessage) (json.RawMessage, error)
}

func (m *fakeMigrator) SchemaVersionSpec() int   { return m.specVersion }
func (m *fakeMigrator) SchemaVersionStatus() int { return m.statusVersion }

func (m *fakeMigrator) ConvertSpec(from int, raw json.RawMessage) (json.RawMessage, error) {
	if m.convertSpec != nil {
		return m.convertSpec(from, raw)
	}
	return raw, nil
}

func (m *fakeMigrator) ConvertStatus(from int, raw json.RawMessage) (json.RawMessage, error) {
	if m.convertStatus != nil {
		return m.convertStatus(from, raw)
	}
	return raw, nil
}

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
// Within runs fn inline with the same context: the fake has no real transaction,
// so "standalone" and "joined" collapse to a direct call. This lets client code
// that wraps writes in Within reach the overridden mutators below.
func (s *fakeStore) Within(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}
func (s *fakeStore) CreateObject(context.Context, *RawObject) (*RawObject, error) {
	panic("not implemented: fakeStore.CreateObject")
}
func (s *fakeStore) GetObject(context.Context, ObjectID) (*RawObject, error) {
	panic("not implemented: fakeStore.GetObject")
}
func (s *fakeStore) GetObjectMeta(context.Context, ObjectID) (*RawObject, error) {
	panic("not implemented: fakeStore.GetObjectMeta")
}
func (s *fakeStore) GetObjectBySlug(context.Context, GroupKind, string) (*RawObject, error) {
	panic("not implemented: fakeStore.GetObjectBySlug")
}
func (s *fakeStore) ListObjects(context.Context, GroupKind) ([]*RawObject, error) {
	return nil, nil
}
func (s *fakeStore) ListIDs(context.Context, GroupKind) ([]ObjectID, error) {
	return nil, nil
}
func (s *fakeStore) ListUnsettledIDs(context.Context, GroupKind) ([]ObjectID, error) {
	return nil, nil
}
func (s *fakeStore) ListDeletionPendingIDs(context.Context, GroupKind) ([]ObjectID, error) {
	return nil, nil
}
func (s *fakeStore) ListAllDeletionPendingIDs(context.Context) ([]ObjectID, error) {
	return nil, nil
}
func (s *fakeStore) UpdateSpec(context.Context, GroupKind, ObjectID, []byte, int) (*RawObject, error) {
	panic("not implemented: fakeStore.UpdateSpec")
}
func (s *fakeStore) UpdateStatus(context.Context, GroupKind, ObjectID, int64, []byte, int) (*RawObject, error) {
	panic("not implemented: fakeStore.UpdateStatus")
}
func (s *fakeStore) DeleteFinalizer(context.Context, GroupKind, ObjectID, string) (*RawObject, error) {
	panic("not implemented: fakeStore.DeleteFinalizer")
}
func (s *fakeStore) RequestDeletion(context.Context, GroupKind, ObjectID) (*RawObject, bool, error) {
	panic("not implemented: fakeStore.RequestDeletion")
}
func (s *fakeStore) SetCondition(context.Context, GroupKind, ObjectID, storeapi.Condition) (*RawObject, error) {
	panic("not implemented: fakeStore.SetCondition")
}
func (s *fakeStore) DeleteCondition(context.Context, GroupKind, ObjectID, string) (*RawObject, error) {
	panic("not implemented: fakeStore.DeleteCondition")
}
func (s *fakeStore) DeleteObject(context.Context, ObjectID) error {
	panic("not implemented: fakeStore.DeleteObject")
}
func (s *fakeStore) MarkOwnedForDeletion(context.Context, ObjectID) ([]storeapi.Referrer, error) {
	panic("not implemented: fakeStore.MarkOwnedForDeletion")
}
func (s *fakeStore) AddRef(context.Context, ObjectID, ObjectID, Relation) error {
	panic("not implemented: fakeStore.AddRef")
}
func (s *fakeStore) DeleteRef(context.Context, ObjectID, ObjectID, Relation) error {
	panic("not implemented: fakeStore.DeleteRef")
}
func (s *fakeStore) ListIncomingRefs(context.Context, ObjectID, Relation) ([]storeapi.Referrer, error) {
	return nil, nil
}
func (s *fakeStore) ListOutgoingRefs(context.Context, ObjectID) ([]storeapi.Referrer, error) {
	return nil, nil
}
func (s *fakeStore) DeleteFinalizingDependsOnRefs(context.Context, ObjectID) error {
	return nil
}
func (s *fakeStore) HasIncomingRefs(context.Context, ObjectID) (bool, error) {
	return false, nil
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
func (s *fakeStore) WatchEvents(context.Context, GroupKind) (Watcher, error) {
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
func (s *watcherStore) WatchEvents(context.Context, GroupKind) (Watcher, error) {
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

// noopController is a no-op test double for Controller, used wherever a test
// needs a registered controller but never exercises its reconcile behaviour.
// Tests that need a ControllerClient obtain it from Register's return value.
type noopController[Spec, Status any] struct{}

func (noopController[Spec, Status]) Reconcile(_ context.Context, _ ControllerClient[Status], _ *Object[Spec, Status]) (Result, error) {
	return Result{}, nil
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

// findCondition returns the condition of the given type, or nil.
func findCondition(conds []Condition, condType string) *Condition {
	for i := range conds {
		if conds[i].Type == condType {
			return &conds[i]
		}
	}
	return nil
}
