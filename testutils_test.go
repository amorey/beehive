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
	"sync/atomic"
	"testing"
	"time"

	"github.com/amorey/beehive/internal/storeapi"
	"github.com/amorey/gochan/oneshot"
	"github.com/stretchr/testify/require"
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

// fastTick is the cadence every integration test runs its drivers at. Nothing is
// pushed — a reconcile after a write, a collect after a delete and a dependency wake
// all arrive on a tick — so a test observes a write propagate within a tick or two,
// never immediately. The production defaults (seconds to tens of seconds) would
// simply time these out.
//
// Short enough that a handful of ticks fit inside testTimeout, long enough that
// the drivers are not hammering the store's single connection while the test does
// its own reads.
const fastTick = 2 * time.Millisecond

// staleDependentsTick paces the stale-dependents backstop in tests, and is
// deliberately slower than fastTick. That pass is the one driver whose purpose is a
// slow cadence — it re-derives what the fast paths missed — so running it at every
// other driver's rate makes every integration test pay for its three-join query
// hundreds of times a second, on the single connection all of them share, to
// observe a backstop it is not testing. At ten ticks a test that does exercise it
// still has a hundred-fold margin inside testTimeout, and production's ratio (60s
// against the waker's 1s) is far wider than this.
const staleDependentsTick = 10 * fastTick

// fast bundles the tick intervals an integration test needs, plus whatever else
// the caller passes. Kept as one bundle so a test reads as "run the drivers
// fast" rather than as five numbers each test picked for itself; a test that
// means to disable a specific driver appends its own option, which wins by
// arriving later.
func fast(opts ...Option) []Option {
	return append([]Option{
		withOwedPassInterval(fastTick),
		WithGCInterval(fastTick),
		withDependencyWakeInterval(fastTick),
		withStaleDependentsInterval(staleDependentsTick),
		withWatchPollInterval(fastTick),
	}, opts...)
}

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
//
// It satisfies the nested-rollback-boundary contract only vacuously — there is no
// transaction, so there is nothing to unwind — while the overridden mutators really
// do mutate in-memory maps, and those mutations will not unwind. **No test may use
// fakeStore to exercise that guarantee.** The savepoint behaviour is tested against
// the real store, in sqlite/store_test.go.
func (s *fakeStore) Within(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

// AfterCommit runs inline: the fake never opens a transaction, so there is no
// commit to wait for.
func (s *fakeStore) AfterCommit(ctx context.Context, fn func(context.Context)) { fn(ctx) }
func (s *fakeStore) ObjectsCreate(context.Context, *RawObject) (*RawObject, error) {
	panic("not implemented: fakeStore.ObjectsCreate")
}
func (s *fakeStore) ObjectsGet(context.Context, ObjectID) (*RawObject, error) {
	panic("not implemented: fakeStore.ObjectsGet")
}
func (s *fakeStore) ObjectsGetForReconcile(context.Context, ObjectID) (storeapi.ReconcileLoad, error) {
	panic("not implemented: fakeStore.ObjectsGetForReconcile")
}
func (s *fakeStore) ObjectsGetMeta(context.Context, ObjectID) (*RawObject, error) {
	panic("not implemented: fakeStore.ObjectsGetMeta")
}
func (s *fakeStore) ObjectsGetBySlug(context.Context, GroupKind, string) (*RawObject, error) {
	panic("not implemented: fakeStore.ObjectsGetBySlug")
}
func (s *fakeStore) ObjectsList(context.Context, GroupKind) ([]*RawObject, error) {
	return nil, nil
}
func (s *fakeStore) ObjectsListIDs(context.Context, GroupKind) ([]ObjectID, error) {
	return nil, nil
}
func (s *fakeStore) ObjectsListUnsettledIDs(context.Context, GroupKind) ([]ObjectID, error) {
	return nil, nil
}
func (s *fakeStore) DeletionRequestsList(context.Context) ([]storeapi.ObjectRef, error) {
	return nil, nil
}

// DependentsListStale answers empty like the listings above it, rather than
// panicking: the stale-dependents driver runs in every Beehive that has
// controllers, so a panic would make the fake unusable for anything calling Start.
func (s *fakeStore) DependentsListStale(context.Context, []GroupKind, ObjectID, int) ([]storeapi.ObjectRef, error) {
	return nil, nil
}
func (s *fakeStore) ReconcileOwedListIDs(context.Context, GroupKind) ([]ObjectID, error) {
	return nil, nil
}
func (s *fakeStore) ReconcileOwedDecrement(context.Context, GroupKind, ObjectID, int64) error {
	panic("not implemented: fakeStore.ReconcileOwedDecrement")
}
func (s *fakeStore) ObjectsUpdateSpec(context.Context, GroupKind, ObjectID, []byte, int) (*RawObject, bool, error) {
	panic("not implemented: fakeStore.ObjectsUpdateSpec")
}
func (s *fakeStore) ObjectsUpdateStatus(context.Context, GroupKind, ObjectID, int64, []byte, int) (*RawObject, error) {
	panic("not implemented: fakeStore.UpdateStatus")
}
func (s *fakeStore) FinalizersDelete(context.Context, GroupKind, ObjectID, string) (*RawObject, error) {
	panic("not implemented: fakeStore.FinalizersDelete")
}
func (s *fakeStore) DeletionRequestsCreate(context.Context, GroupKind, ObjectID) (*RawObject, bool, error) {
	panic("not implemented: fakeStore.DeletionRequestsCreate")
}
func (s *fakeStore) DeletionRequestsCreateBySlug(context.Context, GroupKind, string) (*RawObject, bool, error) {
	panic("not implemented: fakeStore.DeletionRequestsCreateBySlug")
}
func (s *fakeStore) ConditionsSet(context.Context, GroupKind, ObjectID, storeapi.Condition) (*RawObject, error) {
	panic("not implemented: fakeStore.ConditionsSet")
}
func (s *fakeStore) ConditionsDelete(context.Context, GroupKind, ObjectID, string) (*RawObject, error) {
	panic("not implemented: fakeStore.ConditionsDelete")
}
func (s *fakeStore) ObjectsDelete(context.Context, ObjectID) error {
	panic("not implemented: fakeStore.ObjectsDelete")
}
func (s *fakeStore) DeletionRequestsCreateFromOwner(context.Context, ObjectID) ([]storeapi.ObjectRef, error) {
	panic("not implemented: fakeStore.DeletionRequestsCreateFromOwner")
}
func (s *fakeStore) EventsAdd(context.Context, GroupKind, ObjectID, RawEvent) (*RawEvent, error) {
	panic("not implemented: fakeStore.EventsAdd")
}
func (s *fakeStore) EventsList(context.Context, ObjectID, storeapi.EventQuery) ([]RawEvent, error) {
	panic("not implemented: fakeStore.EventsList")
}
func (s *fakeStore) EventsGetLatest(context.Context, ObjectID, string) (*RawEvent, error) {
	panic("not implemented: fakeStore.EventsGetLatest")
}
func (s *fakeStore) EventsSweep(context.Context, int, time.Duration) (int, error) {
	panic("not implemented: fakeStore.EventsSweep")
}
func (s *fakeStore) EdgesAdd(context.Context, ObjectID, ObjectID, Relation) (storeapi.EdgesAddResult, error) {
	panic("not implemented: fakeStore.EdgesAdd")
}
func (s *fakeStore) EdgesDelete(context.Context, ObjectID, ObjectID, Relation) error {
	panic("not implemented: fakeStore.EdgesDelete")
}
func (s *fakeStore) EdgesListIncoming(context.Context, ObjectID, Relation) ([]storeapi.ObjectRef, error) {
	return nil, nil
}
func (s *fakeStore) ObjectsListByIncomingEdge(context.Context, GroupKind, ObjectID, Relation) ([]*RawObject, error) {
	return nil, nil
}
func (s *fakeStore) EdgesGroupIncomingByID(context.Context, []ObjectID, Relation) (map[ObjectID][]storeapi.ObjectRef, error) {
	return nil, nil
}
func (s *fakeStore) EdgesListOutgoing(context.Context, ObjectID) ([]storeapi.ObjectRef, error) {
	return nil, nil
}
func (s *fakeStore) EdgesListOutgoingByRelation(context.Context, ObjectID, Relation) ([]storeapi.ObjectRef, error) {
	return nil, nil
}
func (s *fakeStore) EdgesGroupOutgoingByID(context.Context, []ObjectID, Relation) (map[ObjectID][]storeapi.ObjectRef, error) {
	return nil, nil
}
func (s *fakeStore) EdgesDeleteFinalizingDependsOn(context.Context, ObjectID) error {
	return nil
}
func (s *fakeStore) EdgesHasIncoming(context.Context, ObjectID) (bool, error) {
	return false, nil
}

func (s *fakeStore) DependencyWatermarksSet(context.Context, ObjectID, int64) error {
	panic("not implemented: fakeStore.DependencyWatermarksSet")
}

func (s *fakeStore) ObjectWritesListSince(context.Context, int64, int) ([]storeapi.ObjectWrite, error) {
	panic("not implemented: fakeStore.ObjectWritesListSince")
}
func (s *fakeStore) ObjectWritesMaxVersion(context.Context) (int64, error) {
	// Zero rather than a panic: every Beehive whose waker runs seeds from this, so a
	// panic would make the fake unusable for anything that calls Start.
	return 0, nil
}

// depsStore serves a per-target dependent set from the waker's batched lookup
// and records what it was asked, so a test can control the exact edges — and
// their order — that the waker walks, and assert that a batch of targets costs
// one query rather than one per target.
type depsStore struct {
	fakeStore
	deps  map[ObjectID][]ObjectRef
	err   error
	calls atomic.Int64
	seen  [][]ObjectID // the id slices each call was asked to resolve
}

func (s *depsStore) EdgesGroupIncomingByID(_ context.Context, toIDs []ObjectID, _ Relation) (map[ObjectID][]ObjectRef, error) {
	s.calls.Add(1)
	if s.err != nil {
		return nil, s.err
	}
	s.seen = append(s.seen, slices.Clone(toIDs))
	out := make(map[ObjectID][]ObjectRef, len(toIDs))
	for _, id := range toIDs {
		if deps, ok := s.deps[id]; ok {
			out[id] = deps
		}
	}
	return out, nil
}

// changedAt is changed with explicit resource versions, for the watermark: every
// row in the write log carries the version it was last written at.
func changedAt(versions ...int64) []ObjectWrite {
	refs := make([]ObjectWrite, 0, len(versions))
	for i, rv := range versions {
		refs = append(refs, ObjectWrite{ID: ObjectID(i + 1), ResourceVersion: rv})
	}
	return refs
}

// replayStore serves ObjectWritesListSince from a fixed set of rows, recording the
// cursor and limit of every page it was asked for. It is the whole of what the
// waker can see, so a test scripts a scan by setting rows and reads back what the
// waker asked for.
type replayStore struct {
	depsStore
	rows    []ObjectWrite // every live row, in version order
	seed    int64         // what ObjectWritesMaxVersion reports
	pages   [][2]int64    // (afterRV, limit) per call
	read    int           // rows actually served, across every page
	listed  *signal       // fires on the first page request, when set
	err     error
	seedErr error
}

func (s *replayStore) ObjectWritesMaxVersion(context.Context) (int64, error) {
	if s.seedErr != nil {
		return 0, s.seedErr
	}
	return s.seed, nil
}

// cursors returns the afterRV of every scan so far, which is how a test sees
// whether the watermark moved.
func (s *replayStore) cursors() []int64 {
	out := make([]int64, 0, len(s.pages))
	for _, p := range s.pages {
		out = append(out, p[0])
	}
	return out
}

func (s *replayStore) ObjectWritesListSince(_ context.Context, afterRV int64, limit int) ([]ObjectWrite, error) {
	s.pages = append(s.pages, [2]int64{afterRV, int64(limit)})
	if s.listed != nil {
		s.listed.fire()
	}
	if s.err != nil {
		return nil, s.err
	}
	var out []ObjectWrite
	for _, r := range s.rows {
		if r.ResourceVersion > afterRV && len(out) < limit {
			out = append(out, r)
		}
	}
	s.read += len(out)
	return out, nil
}

// replayRows builds count live rows at versions 1..count.
func replayRows(count int) []ObjectWrite {
	rows := make([]ObjectWrite, 0, count)
	for i := 1; i <= count; i++ {
		rows = append(rows, ObjectWrite{ID: ObjectID(i), ResourceVersion: int64(i)})
	}
	return rows
}

// noopController is a no-op test double for Controller, used wherever a test
// needs a registered controller but never exercises its reconcile behaviour.
// Tests that need a ControllerClient obtain it from Register's return value.
type noopController[Spec, Status any] struct{}

func (noopController[Spec, Status]) Reconcile(_ context.Context, _ ControllerClient[Status], _ *Object[Spec, Status]) (Result, error) {
	return Result{}, nil
}

// registerNoop registers a do-nothing controller for gk, making the kind count as
// registered without standing up any reconcile behaviour. Tests that create rows
// WithFinalizers need it: a finalizer on a client-only kind is rejected at create,
// because nothing in the process could ever clear it (see clientImpl.resolveCreate).
// It registers only — nothing starts — so a fixture that never calls Start is
// otherwise unchanged.
func registerNoop[Spec, Status any](t *testing.T, bh *Beehive, gk GroupKind) {
	t.Helper()
	_, err := Register(bh, gk, &noopController[Spec, Status]{})
	require.NoError(t, err)
}

// signal is a one-shot notification from a test fake to the test: a callback
// that may run many times calls fire, and the test awaits it with wait. Firing
// is idempotent by contract — oneshot reports a second Send as ErrClosed, where
// a second close of a channel would panic — so a fake needs no sync.Once beside
// its signal.
type signal struct {
	tx *oneshot.Sender[struct{}]
	rx *oneshot.Receiver[struct{}]
}

func newSignal() *signal {
	tx, rx := oneshot.New[struct{}]()
	return &signal{tx: tx, rx: rx}
}

// fire signals, whether or not the test is waiting yet: the value is held in the
// slot until wait takes it. Repeat calls are no-ops, and the returned bool says
// which call this was — true only for the one that signalled, so a callback that
// also has first-time-only work to do can gate it on that instead of on a
// sync.Once of its own.
func (s *signal) fire() bool { return s.tx.Send(struct{}{}) == nil }

// wait blocks until fire has run, failing the test after the failsafe timeout.
// The receiver's channel yields the value and is then closed, so a second wait
// on the same signal returns immediately rather than hanging.
func (s *signal) wait(t *testing.T, what string) {
	t.Helper()
	waitClosed(t, s.rx.Chan(), what)
}

// chanAfter returns a channel closed once n values have arrived on ch, for a test
// that needs "the poll ran again" as something to wait on rather than assume.
func chanAfter(ch <-chan struct{}, n int) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range n {
			<-ch
		}
	}()
	return done
}

// drainProbe empties a probe channel, so a later wait is answered by a signal the
// test caused rather than by one still buffered from earlier. Generic because the
// same "discard what happened before now" step applies to a probe's tokens and to
// a controller's report of which ids it reconciled.
func drainProbe[T any](ch <-chan T) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// closedWhenDrained returns a channel closed once ch is closed, discarding
// whatever ch still holds. A watch closes its channel on cancellation, so this is
// how a test waits for the stream to end without caring what was in flight.
func closedWhenDrained[V any](ch <-chan V) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range ch {
		}
	}()
	return done
}

// awaitMatch waits for a value satisfying match, discarding the ones that do not:
// a driver reports every object it reconciled, and the one under test is not
// necessarily the next. It is the failsafe-timeout counterpart of recv for a
// stream a test has to filter.
func awaitMatch[T any](t *testing.T, ch <-chan T, match func(T) bool, what string) {
	t.Helper()
	deadline := time.After(testTimeout)
	for {
		select {
		case v := <-ch:
			if match(v) {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		}
	}
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

// reconcilerStartedMsg is the record run logs once its startup passes have
// enqueued everything they are going to and its workers are up. Tests key a
// barrier off it rather than off the wall clock.
const reconcilerStartedMsg = "reconciler started"

// messageSignalHandler closes ch the first time a record with the given message
// is logged, and discards everything else. WithAttrs/WithGroup return the same
// handler on purpose: Register decorates the reconciler's logger with group/kind
// attrs, and the signal has to survive that.
type messageSignalHandler struct {
	msg  string
	once sync.Once
	ch   chan struct{}
}

func (h *messageSignalHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *messageSignalHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Message == h.msg {
		h.once.Do(func() { close(h.ch) })
	}
	return nil
}

func (h *messageSignalHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *messageSignalHandler) WithGroup(string) slog.Handler      { return h }

// loggerSignallingOn returns a logger and a channel closed the first time msg is
// logged through it. It is how a test waits on a point *inside* the loop instead
// of guessing at a duration: the log line is emitted at a known place in run, so
// it orders the test's next action against everything the loop did before it.
func loggerSignallingOn(msg string) (*slog.Logger, <-chan struct{}) {
	h := &messageSignalHandler{msg: msg, ch: make(chan struct{})}
	return slog.New(h), h.ch
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

// unsettledIDs is what the owed pass would find owed for the client test kind.
// It is the observable a write leaves behind now that no write schedules anything
// itself: a spec change bumps the generation, and this listing is what notices.
func unsettledIDs(t *testing.T, store Store) []ObjectID {
	t.Helper()
	ids, err := store.ObjectsListUnsettledIDs(context.Background(), clientTestGK)
	require.NoError(t, err)
	return ids
}

// drainQueue removes every dispatchable item from q (get + done, so nothing is
// left holding a processing slot), leaving the queue empty.
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

// addEdge declares an edge for test scaffolding: it discards the endpoint metadata
// EdgesAdd reports, drains the owed-wake stamp
// every new depends_on edge now records — scaffolding wants the edge to exist, not
// the reconcile it buys — so the common require.NoError(t, addEdge(...)) shape
// stays a one-liner with no side effects on the owed listings. Tests that assert
// on the EdgesAddResult or the stamp call the method directly.
func addEdge(ctx context.Context, store Store, from, to ObjectID, relation Relation) error {
	res, err := store.EdgesAdd(ctx, from, to, relation)
	if err != nil {
		return err
	}
	if res.ReconcileOwedStamped {
		// The decrement is kind-scoped, and scaffolding declares edges across kinds;
		// read from's own kind back rather than making every call site name it.
		obj, err := store.ObjectsGetMeta(ctx, from)
		if err != nil {
			return err
		}
		return store.ReconcileOwedDecrement(ctx, GroupKind{Group: obj.Group, Kind: obj.Kind}, from, 1)
	}
	return nil
}

// objectRefIDs projects an ObjectRef slice to its ids, for assertions that care
// which objects are on the far end of an edge rather than how they were reached.
// One projection serves the owner/dependency lookups and the incoming-edge
// lookups alike, since every Edges* query returns this same shape.
func objectRefIDs(refs []ObjectRef) []ObjectID {
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
// reached from DependentsList and the LoadDependents eager path, and nothing here
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

func (s *wakeProbeStore) EdgesListIncoming(ctx context.Context, toID ObjectID, relation Relation) ([]ObjectRef, error) {
	refs, err := s.Store.EdgesListIncoming(ctx, toID, relation)
	if toID == s.targetID {
		s.note(relation)
	}
	return refs, err
}

// EdgesGroupIncomingByID is the waker's own lookup (it resolves a whole batch of
// changed targets in one query), so the probe has to cover it too — otherwise a
// test waiting on "the waker looked" would wait forever.
func (s *wakeProbeStore) EdgesGroupIncomingByID(ctx context.Context, toIDs []ObjectID, relation Relation) (map[ObjectID][]ObjectRef, error) {
	refs, err := s.Store.EdgesGroupIncomingByID(ctx, toIDs, relation)
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
	unsettledListed chan struct{} // ObjectsListUnsettledIDs (per-kind)
	owedListed      chan struct{} // ReconcileOwedListIDs (per-kind)
	gcSwept         chan struct{} // DeletionRequestsList (global)
	staleListed     chan struct{} // DependentsListStale (global)
	watermarkSet    chan struct{} // DependencyWatermarksSet (per successful dependent pass)

	// mu guards staleKinds alone. The other fields are channels, but this one is
	// written by the stale-dependents driver on its own goroutine and read by the
	// test while that driver is still ticking.
	mu         sync.Mutex
	staleKinds [][]GroupKind
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

func (s *listProbeStore) ObjectsListUnsettledIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error) {
	ids, err := s.Store.ObjectsListUnsettledIDs(ctx, gk)
	probeSignal(s.unsettledListed)
	return ids, err
}

func (s *listProbeStore) ReconcileOwedListIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error) {
	ids, err := s.Store.ReconcileOwedListIDs(ctx, gk)
	probeSignal(s.owedListed)
	return ids, err
}

func (s *listProbeStore) DeletionRequestsList(ctx context.Context) ([]storeapi.ObjectRef, error) {
	rows, err := s.Store.DeletionRequestsList(ctx)
	probeSignal(s.gcSwept)
	return rows, err
}

// reconcileLoadOf wraps a scripted row as the reconcile loop's opening read, for
// the fakes that serve one. The loop reads through ObjectsGetForReconcile, so a
// double keeps its row in its ObjectsGet and delegates here; the cursor and the
// dependency flag stay zero, which is "no dependencies, nothing to record".
func reconcileLoadOf(obj *RawObject, err error) (storeapi.ReconcileLoad, error) {
	if err != nil {
		return storeapi.ReconcileLoad{}, err
	}
	return storeapi.ReconcileLoad{Object: *obj}, nil
}

func (s *listProbeStore) DependentsListStale(ctx context.Context, kinds []GroupKind, afterID ObjectID, limit int) ([]storeapi.ObjectRef, error) {
	refs, err := s.Store.DependentsListStale(ctx, kinds, afterID, limit)
	s.mu.Lock()
	s.staleKinds = append(s.staleKinds, slices.Clone(kinds))
	s.mu.Unlock()
	probeSignal(s.staleListed)
	return refs, err
}

// kindsAsked snapshots what the stale-dependents listings have been asked for.
func (s *listProbeStore) kindsAsked() [][]GroupKind {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.staleKinds)
}

// DependencyWatermarksSet signals *after* the write, so a test can order a change
// to a target against the dependent's pass having already recorded what it
// observed — without which the change might land under the pass and be recorded
// as seen.
func (s *listProbeStore) DependencyWatermarksSet(ctx context.Context, id ObjectID, cursor int64) error {
	err := s.Store.DependencyWatermarksSet(ctx, id, cursor)
	probeSignal(s.watermarkSet)
	return err
}
