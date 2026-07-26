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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
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

// captureLogger returns a logger that records everything at or above level into
// the returned buffer, for tests asserting that a code path announces itself.
func captureLogger(level slog.Level) (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: level})), &buf
}

// withoutGCSweeper stops the global GC sweeper from starting, for tests that are
// measuring some other driver's listings and want the sweeper's own enqueues out of
// the picture. It sets the field directly because WithGCInterval rejects a
// non-positive interval: production has no way to run without a sweeper (nothing
// public collects a row, so disabling it strands deletion-pending rows for good),
// and this is a test-only escape hatch, not that configuration being supported.
func withoutGCSweeper() Option {
	return func(target any) error {
		if bh, ok := target.(*Beehive); ok {
			bh.gcInterval = 0
		}
		return nil
	}
}

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

// AfterCommit runs inline: the fake never opens a transaction, so there is no
// commit to wait for.
func (s *fakeStore) AfterCommit(ctx context.Context, fn func(context.Context)) { fn(ctx) }
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
func (s *fakeStore) ListAllDeletionPending(context.Context) ([]storeapi.Referrer, error) {
	return nil, nil
}
func (s *fakeStore) ListPendingWakeIDs(context.Context, GroupKind) ([]ObjectID, error) {
	return nil, nil
}
func (s *fakeStore) DecrementPendingWake(context.Context, ObjectID, int64) error {
	panic("not implemented: fakeStore.DecrementPendingWake")
}
func (s *fakeStore) UpdateSpec(context.Context, GroupKind, ObjectID, []byte, int) (*RawObject, bool, error) {
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
func (s *fakeStore) RequestDeletionBySlug(context.Context, GroupKind, string) (*RawObject, bool, error) {
	panic("not implemented: fakeStore.RequestDeletionBySlug")
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
func (s *fakeStore) RecordEvent(context.Context, GroupKind, ObjectID, RawEvent) (*RawEvent, error) {
	panic("not implemented: fakeStore.RecordEvent")
}
func (s *fakeStore) ListEvents(context.Context, ObjectID, storeapi.EventQuery) ([]RawEvent, error) {
	panic("not implemented: fakeStore.ListEvents")
}
func (s *fakeStore) GetLatestEvent(context.Context, ObjectID, string) (*RawEvent, error) {
	panic("not implemented: fakeStore.GetLatestEvent")
}
func (s *fakeStore) SweepEvents(context.Context, int, time.Duration) (int, error) {
	panic("not implemented: fakeStore.SweepEvents")
}
func (s *fakeStore) AddRef(context.Context, ObjectID, ObjectID, Relation, int64) (storeapi.AddRefResult, error) {
	panic("not implemented: fakeStore.AddRef")
}
func (s *fakeStore) DeleteRef(context.Context, ObjectID, ObjectID, Relation) error {
	panic("not implemented: fakeStore.DeleteRef")
}
func (s *fakeStore) ListIncomingRefs(context.Context, ObjectID, Relation) ([]storeapi.Referrer, error) {
	return nil, nil
}
func (s *fakeStore) ListIncomingRefObjects(context.Context, GroupKind, ObjectID, Relation) ([]*RawObject, error) {
	return nil, nil
}
func (s *fakeStore) GroupIncomingRefsByID(context.Context, []ObjectID, Relation) (map[ObjectID][]storeapi.Referrer, error) {
	return nil, nil
}
func (s *fakeStore) ListOutgoingRefs(context.Context, ObjectID) ([]storeapi.Referrer, error) {
	return nil, nil
}
func (s *fakeStore) ListOutgoingRefsByRelation(context.Context, ObjectID, Relation) ([]storeapi.Referrer, error) {
	return nil, nil
}
func (s *fakeStore) GroupOutgoingRefsByID(context.Context, []ObjectID, Relation) (map[ObjectID][]storeapi.Referrer, error) {
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
func (s *fakeStore) WatchChanges(context.Context, GroupKind) (Watcher, error) {
	return noopWatcher{}, nil
}
func (s *fakeStore) WatchChangeRefs(context.Context) (storeapi.ChangeRefWatcher, error) {
	return noopChangeRefWatcher{}, nil
}
func (s *fakeStore) WatchEvents(context.Context, GroupKind, ObjectID, storeapi.EventQuery) (EventWatcher, error) {
	panic("not implemented: fakeStore.WatchEvents")
}

// noopWatcher is a Watcher whose event stream never fires; Close is a no-op.
type noopWatcher struct{}

func (noopWatcher) Changes() <-chan storeapi.RawChange { return nil }
func (noopWatcher) Close()                             {}

// noopChangeRefWatcher is noopWatcher's store-wide-stream twin.
type noopChangeRefWatcher struct{}

func (noopChangeRefWatcher) Batches() <-chan []storeapi.ChangeRef { return nil }
func (noopChangeRefWatcher) Close()                               {}

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
func (s *watcherStore) WatchChanges(context.Context, GroupKind) (Watcher, error) {
	return s.w, s.err
}

// fakeWatcher is a controllable Watcher: push feeds a raw event, endStream ends
// the stream, and Close signals the adapter goroutine's exit. It backs the
// client adaptWatcher tests.
type fakeWatcher struct {
	ch        chan storeapi.RawChange
	closed    chan struct{}
	closeOnce sync.Once
}

func newFakeWatcher() *fakeWatcher {
	return &fakeWatcher{ch: make(chan storeapi.RawChange), closed: make(chan struct{})}
}

func (w *fakeWatcher) Changes() <-chan storeapi.RawChange { return w.ch }

// Close (called by adaptWatcher's defer on exit) closes closed, letting tests
// synchronize on goroutine exit instead of reading Changes — which could itself
// satisfy a pending send and race the outcome.
func (w *fakeWatcher) Close() { w.closeOnce.Do(func() { close(w.closed) }) }

// push delivers a raw event to the adapter goroutine.
func (w *fakeWatcher) push(typ ChangeType, obj *RawObject) {
	w.ch <- storeapi.RawChange{Type: typ, Object: obj}
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

// drainQueue removes every dispatchable item from q (get + done, so nothing is
// left holding a processing slot), leaving the queue empty — used by tests that
// need a clean queue after a create-time enqueue.
func drainQueue(q *workQueue) {
	for id, ok := q.get(); ok; id, ok = q.get() {
		q.done(id)
	}
}

// runCommitRollback runs body as two subtests, "commit" (commit=true) and
// "rollback" (commit=false), for the many wake-ordering tests that assert one
// thing on a committed outer transaction and the opposite on a rolled-back one.
func runCommitRollback(t *testing.T, body func(t *testing.T, commit bool)) {
	t.Helper()
	t.Run("commit", func(t *testing.T) { body(t, true) })
	t.Run("rollback", func(t *testing.T) { body(t, false) })
}

// queuedIDs snapshots q's dispatchable items, for tests that assert on which
// objects a write woke. Reading items directly (rather than draining) leaves the
// queue untouched, so a test can check it repeatedly.
func queuedIDs(q *workQueue) []ObjectID {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]ObjectID(nil), q.items...)
}

// reconcileCapture is a Controller that reports every object it reconciles on a
// channel, so a test can assert which objects a write or a wake requeued.
type reconcileCapture struct {
	ch chan *Object[tSpec, tStatus]
}

func (c *reconcileCapture) Reconcile(_ context.Context, _ ControllerClient[tStatus], obj *Object[tSpec, tStatus]) (Result, error) {
	c.ch <- obj
	return Result{}, nil
}

// addRef declares an edge for test scaffolding: it discards the endpoint metadata
// AddRef reports and passes no version claim (0), so the common
// require.NoError(t, addRef(...)) shape stays a one-liner. Tests that assert on
// the AddRefResult, or on the version guard, call the method directly.
func addRef(ctx context.Context, store Store, from, to ObjectID, relation Relation) error {
	_, err := store.AddRef(ctx, from, to, relation, 0)
	return err
}

// refObjectIDs projects a Ref/Referrer slice to its ids, for assertions that care
// which objects are on the far end of an edge rather than how they were reached.
// Ref and Referrer are both aliases of storeapi.Referrer, so one projection serves
// the owner/dependency lookups and the incoming-ref lookups alike.
func refObjectIDs(refs []Ref) []ObjectID {
	var ids []ObjectID
	for _, r := range refs {
		ids = append(ids, r.ID)
	}
	return ids
}

// wakeProbeStore signals every time someone asks who depends on targetID. The
// race is between the *waker's lookup* and the edge's commit, not between the
// change and the commit, so a test that wants the window deterministically has to
// wait for this rather than assume the waker is done.
//
// It is keyed on (toID, relation), not on the caller — the same lookups are also
// reached from ListDependents and the LoadDependents eager path, and nothing here
// can tell those from the waker. So a token means "somebody looked", and a test
// that wants "the waker looked" must resetLooked immediately before the write it
// expects the waker to react to. Reading the target's dependents from a test's own
// assertions bypasses the probe (use the embedded Store) rather than feeding it a
// token that a later waitLooked would mistake for the waker's.
type wakeProbeStore struct {
	Store
	targetID ObjectID
	looked   chan struct{} // one send per targetID depends_on lookup
}

func (s *wakeProbeStore) ListIncomingRefs(ctx context.Context, toID ObjectID, relation Relation) ([]Referrer, error) {
	refs, err := s.Store.ListIncomingRefs(ctx, toID, relation)
	if toID == s.targetID {
		s.note(relation)
	}
	return refs, err
}

// GroupIncomingRefsByID is the waker's own lookup (it resolves a whole batch of
// changed targets in one query), so the probe has to cover it too — otherwise a
// test waiting on "the waker looked" would wait forever.
func (s *wakeProbeStore) GroupIncomingRefsByID(ctx context.Context, toIDs []ObjectID, relation Relation) (map[ObjectID][]Referrer, error) {
	refs, err := s.Store.GroupIncomingRefsByID(ctx, toIDs, relation)
	if slices.Contains(toIDs, s.targetID) {
		s.note(relation)
	}
	return refs, err
}

// note records one depends_on lookup for the target. Non-blocking: it runs on
// the waker's goroutine, and a full buffer means no test is waiting. Blocking
// there would park the waker inside the store and hang the reconciler — a
// timeout in some unrelated test rather than a failure here.
func (s *wakeProbeStore) note(relation Relation) {
	if relation != RelationDependsOn {
		return
	}
	select {
	case s.looked <- struct{}{}:
	default:
	}
}

// resetLooked discards lookups recorded so far, so the next waitLooked can only
// be satisfied by one that happens after this point. Call it immediately before
// the write whose wake is under observation; without it a leftover token lets
// waitLooked return before the waker has run, and the test then races the very
// thing it means to pin.
func (s *wakeProbeStore) resetLooked() {
	for {
		select {
		case <-s.looked:
		default:
			return
		}
	}
}

// waitLooked blocks until targetID's dependents are resolved (see resetLooked).
// With no edge declared yet that lookup comes back empty, so the change that
// triggered it is unclaimed and any later requeue is attributable to the
// declaration under test.
func (s *wakeProbeStore) waitLooked(t *testing.T) {
	t.Helper()
	waitClosed(t, s.looked, "the dependency waker's lookup")
}

// listProbeStore wraps a Store and signals each of the periodic listings a test
// may need to order itself against: the reconciler's two owed-work listings and
// the GC sweeper's cross-kind pass. Start only *launches* those loops, and their
// startup passes drain the same sets a tick does — so a test that seeds work
// around Start is racing them, and would pass or fail on timing rather than on
// the behavior under test. Waiting for the relevant signal and only then seeding
// leaves the driver under test as the sole possible cause.
//
// A nil channel means that listing isn't observed, so each test names only the
// ones it orders against.
type listProbeStore struct {
	Store
	unsettledListed chan struct{} // ListUnsettledIDs (per-kind)
	wakeListed      chan struct{} // ListPendingWakeIDs (per-kind)
	gcSwept         chan struct{} // ListAllDeletionPending (global)
}

// probeSignal reports one listing. The send is non-blocking so a late pass after
// the test stops reading never wedges the goroutine that made it.
func probeSignal(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (s *listProbeStore) ListUnsettledIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error) {
	ids, err := s.Store.ListUnsettledIDs(ctx, gk)
	probeSignal(s.unsettledListed)
	return ids, err
}

func (s *listProbeStore) ListPendingWakeIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error) {
	ids, err := s.Store.ListPendingWakeIDs(ctx, gk)
	probeSignal(s.wakeListed)
	return ids, err
}

func (s *listProbeStore) ListAllDeletionPending(ctx context.Context) ([]storeapi.Referrer, error) {
	rows, err := s.Store.ListAllDeletionPending(ctx)
	probeSignal(s.gcSwept)
	return rows, err
}
