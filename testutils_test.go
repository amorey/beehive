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
	"fmt"
	"log/slog"
	"math"
	"os"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amorey/beehive/internal/storeapi"
	"github.com/amorey/gochan/oneshot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMain runs the package's tests and then fails the binary if any beehive
// goroutine outlived them.
//
// The leak it catches is silent by construction. A watch stream ends only when
// its context is cancelled — a failed read costs one tick and the poller retries
// — so a stream started on an uncancellable context keeps polling its store
// every fastTick for the rest of the run, and every test that follows pays for
// it. The test that leaked still passes; what fails is some later test's
// failsafe, on a loaded machine, once in a while. This turns that into a
// deterministic failure in the run that caused it.
//
// It reports only on a run that otherwise passed. A failing test has its own
// message, and one that failed part-way through is entitled to leave goroutines
// behind — the leak report on top of it would be noise pointing at a cause it
// does not have.
func TestMain(m *testing.M) {
	code := m.Run()
	if code == 0 {
		if stacks := lingeringGoroutines(); stacks != "" {
			fmt.Fprintf(os.Stderr, "\ngoroutines outlived the tests that started them:\n\n%s\n"+
				"A watch stream is the usual source: see watchTestClient.\n", stacks)
			code = 1
		}
	}
	os.Exit(code)
}

// leakSettleAttempts and leakSettleWait bound how long lingeringGoroutines waits
// for a goroutine that is already on its way out. Cancellation is asynchronous:
// a stream cancelled in a t.Cleanup is unblocked, not gone, and the profile can
// still show it. A real leak is never gone, so the wait costs a passing run at
// most one nap and a failing one nothing it did not deserve.
const (
	leakSettleAttempts = 20
	leakSettleWait     = 10 * time.Millisecond
)

// lingeringGoroutines returns the stacks of every goroutine still running
// beehive code, or "" when there are none.
//
// The retry is the one deliberate exception to "synchronize on signals, never on
// sleeps": the goroutines under observation are leaked ones, so by definition
// nobody holds a signal to wait on and there is nothing to select over. Polling
// the profile is the only reading available.
func lingeringGoroutines() string {
	for attempt := range leakSettleAttempts {
		if attempt > 0 {
			time.Sleep(leakSettleWait)
		}
		var buf bytes.Buffer
		// 1: one record per distinct stack, carrying a count — so N copies of a
		// leaked poller report as one stack rather than N pages of the same frames.
		if err := pprof.Lookup("goroutine").WriteTo(&buf, 1); err != nil {
			return "" // no profile is not evidence of a leak
		}
		ours := beehiveStacks(buf.String())
		if ours == "" {
			return ""
		}
		if attempt == leakSettleAttempts-1 {
			return ours
		}
	}
	return ""
}

// beehiveStacks keeps the records of a goroutine profile that name this module
// or the bus it streams schedules over, which is what makes the report
// actionable and what keeps it quiet about runtime and database/sql goroutines
// nobody here can end. The goroutine doing the looking names the module too, so
// it is excluded by the frame it is standing in.
//
// gobus is in the list as insurance. The WatchSchedule stream reads its
// receiver with RecvContext, which starts no goroutine of its own, so today
// every frame it could leak is beehive's. A switch to Receiver.Chan would run a
// feeder goroutine whose frames are all in gobus, and the leak this package is
// most likely to grow — a receiver abandoned without Close — would be invisible
// without this.
func beehiveStacks(profile string) string {
	var ours []string
	for _, record := range strings.Split(profile, "\n\n") {
		if strings.Contains(record, "beehive.lingeringGoroutines") {
			continue
		}
		if strings.Contains(record, "github.com/amorey/beehive") ||
			strings.Contains(record, "github.com/amorey/gobus") {
			ours = append(ours, strings.TrimSpace(record))
		}
	}
	return strings.Join(ours, "\n\n")
}

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
//
// Generous on purpose, and paid only by a run that was going to fail anyway. CI
// runs the suite under -race at GOMAXPROCS=1, where every driver in every live
// beehive competes for the one processor with the test goroutine — so the gap
// between "a tick took longer than usual" and "this has hung" is orders of
// magnitude wider there than on a developer's machine. A failsafe tight enough
// to be reached by mere load reports the hang that never happened, which is the
// one thing it must not do.
const testTimeout = 10 * time.Second

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
		withWakeScanMinInterval(0),
		withMinRequeueInterval(fastTick),
		WithStaleDependentsInterval(staleDependentsTick),
		WithWatchFloorInterval(fastTick),
	}, opts...)
}

// parked is fast with every periodic pass stopped, so a push is the only thing
// left that can dispatch — which is what a "…WithoutASweep" test asserts.
func parked(opts ...Option) []Option {
	return fast(append([]Option{
		WithFullPassInterval(0),
		withOwedPassInterval(time.Hour),
		WithStaleDependentsInterval(time.Hour),
		withDependencyWakerOff(),
		withoutGCSweeper(),
	}, opts...)...)
}

// drainRecv discards whatever a bus receiver is holding, so the next Recv proves
// something published after the drain.
func drainRecv[E any, R interface{ TryRecv() (E, error) }](rx R) {
	for {
		if _, err := rx.TryRecv(); err != nil {
			return
		}
	}
}

// newTestBeehive is New with the test's own error handling. A watch tailer ends
// with its last subscriber, so a test that watches owes no teardown here — the
// watch's own context is what ends it.
// The tail's throttle is off unless a test asks for it: it is far above
// fastTick, so an enabled throttle would refuse the floor ticks the watch tests
// deliver on. Prepended, so a test's own option still wins.
func newTestBeehive(t *testing.T, store Store, opts ...Option) *Beehive {
	t.Helper()
	bh, err := New(store, append([]Option{withWatchScanMinInterval(0)}, opts...)...)
	require.NoError(t, err)
	return bh
}

// tailerCount reads the live tailer count under the lock the refcount moves
// under, since a subscriber releases its lease on its own goroutine.
func tailerCount(bh *Beehive) int {
	bh.tailMu.Lock()
	defer bh.tailMu.Unlock()
	return len(bh.tailers)
}

// fakeClock is a clock a test drives by hand, for the wall-clock rate limits
// the suite must not sleep on.
type fakeClock struct{ at time.Time }

func (c *fakeClock) now() time.Time          { return c.at }
func (c *fakeClock) advance(d time.Duration) { c.at = c.at.Add(d) }

// fakeClockOn points now at a fakeClock and returns it. Every driver that
// paces itself takes its clock this way, so they share one epoch.
func fakeClockOn(now *func() time.Time) *fakeClock {
	clk := &fakeClock{at: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	*now = clk.now
	return clk
}

// mustCreate creates one object and fails the test if the create errors — the
// shape of the great majority of test creates, which only want a row to exist
// before they assert on something else.
//
// It exists so that a change to Create's signature is a change to one function
// rather than to every test that needed a row. Tests that assert *on the create
// itself* (a UNIQUE conflict, an option error, a marshal failure) call Create
// directly and should keep doing so: the error is their subject, and this helper
// would swallow it.
func mustCreate[Spec, Status any](
	t *testing.T,
	ctx context.Context,
	c Client[Spec, Status],
	name string,
	spec Spec,
	opts ...Option,
) *Object[Spec, Status] {
	t.Helper()
	obj, err := c.Create(ctx, name, spec, opts...)
	require.NoError(t, err)
	return obj
}

// captureLogger returns a logger that records everything at or above level into
// the returned buffer, for tests asserting that a code path announces itself.
func captureLogger(level slog.Level) (*slog.Logger, *safeBuffer) {
	buf := &safeBuffer{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: level})), buf
}

// safeBuffer is a bytes.Buffer a logger and a test can hold at once, which
// captureLogger needs wherever the code under test still has loops running.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *safeBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
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
	id     int64
}

// Identity is per value, since two fakeStores are two databases. Lazy so the
// zero value works: every fixture builds one with &fakeStore{}.
func (s *fakeStore) Identity() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.id == 0 {
		s.id = fakeStoreIDs.Add(1)
	}
	return "fake:" + strconv.FormatInt(s.id, 10)
}

var fakeStoreIDs atomic.Int64

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

// GetLatestResourceVersion answers 0: the stale-dependents driver reads it every
// tick, so a panic would break every Start.
func (s *fakeStore) GetLatestResourceVersion(context.Context) (int64, error) {
	return 0, nil
}

func (s *fakeStore) Dependencies() storeapi.Dependencies { return fakeDependencies{} }

// fakeDependencies is fakeStore's dependency family. ListStaleSince answers
// empty rather than panicking: the stale-dependents driver runs in every Beehive
// that has controllers, so a panic would make the fake unusable for anything
// calling Start.
type fakeDependencies struct{}

func (fakeDependencies) ListStaleSince(_ context.Context, _ []GroupKind, after StalePos, _ int64, _ int) ([]storeapi.ObjectRef, StalePos, error) {
	return nil, after, nil
}

func (fakeDependencies) WatermarkSet(context.Context, ObjectID, int64) error {
	panic("not implemented: fakeStore.Dependencies().WatermarkSet")
}

// cursorsOverride replaces the hooks that are set and delegates the rest.
type cursorsOverride struct {
	storeapi.DriverCursors
	set func(context.Context, string, int64) error
}

func (o cursorsOverride) Set(ctx context.Context, name string, cursor int64) error {
	if o.set != nil {
		return o.set(ctx, name, cursor)
	}
	return o.DriverCursors.Set(ctx, name, cursor)
}

// depsOverride replaces the hooks that are set on a real Dependencies and
// delegates the rest, so a probe needs no wrapper type of its own.
type depsOverride struct {
	storeapi.Dependencies
	listStaleSince func(context.Context, []GroupKind, StalePos, int64, int) ([]storeapi.ObjectRef, StalePos, error)
	watermarkSet   func(context.Context, ObjectID, int64) error
}

func (d depsOverride) ListStaleSince(ctx context.Context, kinds []GroupKind, after StalePos, through int64, limit int) ([]storeapi.ObjectRef, StalePos, error) {
	if d.listStaleSince != nil {
		return d.listStaleSince(ctx, kinds, after, through, limit)
	}
	return d.Dependencies.ListStaleSince(ctx, kinds, after, through, limit)
}

func (d depsOverride) WatermarkSet(ctx context.Context, id ObjectID, cursor int64) error {
	if d.watermarkSet != nil {
		return d.watermarkSet(ctx, id, cursor)
	}
	return d.Dependencies.WatermarkSet(ctx, id, cursor)
}

func (s *fakeStore) DeletionRequests() storeapi.DeletionRequests { return fakeDeletionRequests{} }

// fakeDeletionRequests is fakeStore's deletion-request family. List answers
// empty rather than panicking: the GC sweeper runs in every Beehive.
type fakeDeletionRequests struct{}

func (fakeDeletionRequests) Create(context.Context, GroupKind, ObjectID) (storeapi.DeletionRequestResult, error) {
	panic("not implemented: fakeStore.DeletionRequests().Create")
}

func (fakeDeletionRequests) CreateByName(context.Context, GroupKind, string) (storeapi.DeletionRequestResult, error) {
	panic("not implemented: fakeStore.DeletionRequests().CreateByName")
}

func (fakeDeletionRequests) CreateFromOwner(context.Context, ObjectID) (storeapi.DeletionCascadeResult, error) {
	panic("not implemented: fakeStore.DeletionRequests().CreateFromOwner")
}

func (fakeDeletionRequests) List(context.Context) ([]storeapi.ObjectRef, error) { return nil, nil }

// delReqOverride replaces the hooks that are set and delegates the rest.
type delReqOverride struct {
	storeapi.DeletionRequests
	createByName    func(context.Context, GroupKind, string) (storeapi.DeletionRequestResult, error)
	createFromOwner func(context.Context, ObjectID) (storeapi.DeletionCascadeResult, error)
	list            func(context.Context) ([]storeapi.ObjectRef, error)
}

func (o delReqOverride) CreateByName(ctx context.Context, gk GroupKind, name string) (storeapi.DeletionRequestResult, error) {
	if o.createByName != nil {
		return o.createByName(ctx, gk, name)
	}
	return o.DeletionRequests.CreateByName(ctx, gk, name)
}

func (o delReqOverride) CreateFromOwner(ctx context.Context, id ObjectID) (storeapi.DeletionCascadeResult, error) {
	if o.createFromOwner != nil {
		return o.createFromOwner(ctx, id)
	}
	return o.DeletionRequests.CreateFromOwner(ctx, id)
}

func (o delReqOverride) List(ctx context.Context) ([]storeapi.ObjectRef, error) {
	if o.list != nil {
		return o.list(ctx)
	}
	return o.DeletionRequests.List(ctx)
}

func (s *fakeStore) ReconcileOwed() storeapi.ReconcileOwed { return fakeReconcileOwed{} }

// fakeReconcileOwed is fakeStore's owed-count family.
type fakeReconcileOwed struct{}

func (fakeReconcileOwed) Decrement(context.Context, GroupKind, ObjectID, int64) error {
	panic("not implemented: fakeStore.ReconcileOwed().Decrement")
}

func (fakeReconcileOwed) ListIDs(context.Context, GroupKind) ([]ObjectID, error) {
	return nil, nil
}

func (fakeReconcileOwed) Stamp(context.Context, []storeapi.ObjectRef) error {
	return nil
}

func (fakeReconcileOwed) Sweep(context.Context, []GroupKind) (int, error) {
	return 0, nil
}

// owedOverride replaces the hooks that are set and delegates the rest.
type owedOverride struct {
	storeapi.ReconcileOwed
	listIDs   func(context.Context, GroupKind) ([]ObjectID, error)
	decrement func(context.Context, GroupKind, ObjectID, int64) error
	stamp     func(context.Context, []storeapi.ObjectRef) error
	sweep     func(context.Context, []GroupKind) (int, error)
}

func (o owedOverride) ListIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error) {
	if o.listIDs != nil {
		return o.listIDs(ctx, gk)
	}
	return o.ReconcileOwed.ListIDs(ctx, gk)
}

func (o owedOverride) Decrement(ctx context.Context, gk GroupKind, id ObjectID, observed int64) error {
	if o.decrement != nil {
		return o.decrement(ctx, gk, id, observed)
	}
	return o.ReconcileOwed.Decrement(ctx, gk, id, observed)
}

func (o owedOverride) Stamp(ctx context.Context, refs []storeapi.ObjectRef) error {
	if o.stamp != nil {
		return o.stamp(ctx, refs)
	}
	return o.ReconcileOwed.Stamp(ctx, refs)
}

func (o owedOverride) Sweep(ctx context.Context, keep []GroupKind) (int, error) {
	if o.sweep != nil {
		return o.sweep(ctx, keep)
	}
	return o.ReconcileOwed.Sweep(ctx, keep)
}

func (s *fakeStore) Edges() storeapi.Edges { return fakeEdges{} }

// fakeEdges is fakeStore's edge family. The listings answer empty rather than
// panicking: the drivers walk edges in every Beehive.
type fakeEdges struct{}

func (fakeEdges) Add(context.Context, ObjectID, ObjectID, Relation) (storeapi.EdgesAddResult, error) {
	panic("not implemented: fakeStore.Edges().Add")
}

func (fakeEdges) Delete(context.Context, ObjectID, ObjectID, Relation) (storeapi.EdgesDeleteResult, error) {
	panic("not implemented: fakeStore.Edges().Delete")
}

func (fakeEdges) ListIncoming(context.Context, ObjectID, Relation) ([]storeapi.ObjectRef, error) {
	return nil, nil
}

func (fakeEdges) GroupIncomingByID(context.Context, []ObjectID, Relation) (map[ObjectID][]storeapi.ObjectRef, error) {
	return nil, nil
}

func (fakeEdges) ListOutgoingByRelation(context.Context, ObjectID, Relation) ([]storeapi.ObjectRef, error) {
	return nil, nil
}

func (fakeEdges) GroupOutgoingByID(context.Context, []ObjectID, Relation) (map[ObjectID][]storeapi.ObjectRef, error) {
	return nil, nil
}

func (fakeEdges) DeleteFinalizingDependsOn(context.Context, ObjectID) error {
	return nil
}

func (fakeEdges) HasIncoming(context.Context, ObjectID) (bool, error) {
	return false, nil
}

// edgesOverride replaces the hooks that are set and delegates the rest.
type edgesOverride struct {
	storeapi.Edges
	add                       func(context.Context, ObjectID, ObjectID, Relation) (storeapi.EdgesAddResult, error)
	delete                    func(context.Context, ObjectID, ObjectID, Relation) (storeapi.EdgesDeleteResult, error)
	deleteFinalizingDependsOn func(context.Context, ObjectID) error
	groupIncomingByID         func(context.Context, []ObjectID, Relation) (map[ObjectID][]storeapi.ObjectRef, error)
	groupOutgoingByID         func(context.Context, []ObjectID, Relation) (map[ObjectID][]storeapi.ObjectRef, error)
	hasIncoming               func(context.Context, ObjectID) (bool, error)
	listIncoming              func(context.Context, ObjectID, Relation) ([]storeapi.ObjectRef, error)
	listOutgoingByRelation    func(context.Context, ObjectID, Relation) ([]storeapi.ObjectRef, error)
}

func (o edgesOverride) Add(ctx context.Context, from, to ObjectID, rel Relation) (storeapi.EdgesAddResult, error) {
	if o.add != nil {
		return o.add(ctx, from, to, rel)
	}
	return o.Edges.Add(ctx, from, to, rel)
}

func (o edgesOverride) Delete(ctx context.Context, from, to ObjectID, rel Relation) (storeapi.EdgesDeleteResult, error) {
	if o.delete != nil {
		return o.delete(ctx, from, to, rel)
	}
	return o.Edges.Delete(ctx, from, to, rel)
}

func (o edgesOverride) DeleteFinalizingDependsOn(ctx context.Context, to ObjectID) error {
	if o.deleteFinalizingDependsOn != nil {
		return o.deleteFinalizingDependsOn(ctx, to)
	}
	return o.Edges.DeleteFinalizingDependsOn(ctx, to)
}

func (o edgesOverride) GroupIncomingByID(ctx context.Context, ids []ObjectID, rel Relation) (map[ObjectID][]storeapi.ObjectRef, error) {
	if o.groupIncomingByID != nil {
		return o.groupIncomingByID(ctx, ids, rel)
	}
	return o.Edges.GroupIncomingByID(ctx, ids, rel)
}

func (o edgesOverride) GroupOutgoingByID(ctx context.Context, ids []ObjectID, rel Relation) (map[ObjectID][]storeapi.ObjectRef, error) {
	if o.groupOutgoingByID != nil {
		return o.groupOutgoingByID(ctx, ids, rel)
	}
	return o.Edges.GroupOutgoingByID(ctx, ids, rel)
}

func (o edgesOverride) HasIncoming(ctx context.Context, id ObjectID) (bool, error) {
	if o.hasIncoming != nil {
		return o.hasIncoming(ctx, id)
	}
	return o.Edges.HasIncoming(ctx, id)
}

func (o edgesOverride) ListIncoming(ctx context.Context, to ObjectID, rel Relation) ([]storeapi.ObjectRef, error) {
	if o.listIncoming != nil {
		return o.listIncoming(ctx, to, rel)
	}
	return o.Edges.ListIncoming(ctx, to, rel)
}

func (o edgesOverride) ListOutgoingByRelation(ctx context.Context, from ObjectID, rel Relation) ([]storeapi.ObjectRef, error) {
	if o.listOutgoingByRelation != nil {
		return o.listOutgoingByRelation(ctx, from, rel)
	}
	return o.Edges.ListOutgoingByRelation(ctx, from, rel)
}

func (s *fakeStore) ObjectWrites() storeapi.ObjectWrites { return fakeObjectWrites{} }

// fakeObjectWrites is fakeStore's write-log family.
type fakeObjectWrites struct{}

func (fakeObjectWrites) ListSince(context.Context, GroupKind, int64, int) ([]storeapi.ObjectWrite, int64, error) {
	panic("not implemented: fakeStore.ObjectWrites().ListSince")
}

func (fakeObjectWrites) ListSinceAll(context.Context, int64, int) ([]storeapi.ObjectWrite, int64, error) {
	// Empty rather than a panic: Start seeds the waker, so its eager first pass
	// scans rather than seeding, and every Beehive whose waker runs reaches this.
	return nil, 0, nil
}

func (fakeObjectWrites) MaxVersion(context.Context, GroupKind) (int64, error) {
	panic("not implemented: fakeStore.ObjectWrites().MaxVersion")
}

func (fakeObjectWrites) MaxVersionAll(context.Context) (int64, int64, error) {
	// Zero rather than a panic: every Beehive whose waker runs seeds from this, so a
	// panic would make the fake unusable for anything that calls Start.
	return 0, 0, nil
}

func (fakeObjectWrites) Snapshot(context.Context, GroupKind) ([]*RawObject, int64, error) {
	panic("not implemented: fakeStore.ObjectWrites().Snapshot")
}

func (fakeObjectWrites) SnapshotByID(context.Context, GroupKind, ObjectID) ([]*RawObject, int64, error) {
	panic("not implemented: fakeStore.ObjectWrites().SnapshotByID")
}

func (fakeObjectWrites) SnapshotByOwner(context.Context, GroupKind, ObjectID) ([]*RawObject, int64, error) {
	panic("not implemented: fakeStore.ObjectWrites().SnapshotByOwner")
}

func (fakeObjectWrites) Sweep(context.Context, int, time.Duration) (int, error) {
	// Zero rather than a panic: write-log retention is on by default, so every
	// Beehive whose GC sweeper ticks reaches this.
	return 0, nil
}

// writesOverride replaces the hooks that are set and delegates the rest.
type writesOverride struct {
	storeapi.ObjectWrites
	listSince     func(context.Context, GroupKind, int64, int) ([]storeapi.ObjectWrite, int64, error)
	listSinceAll  func(context.Context, int64, int) ([]storeapi.ObjectWrite, int64, error)
	maxVersion    func(context.Context, GroupKind) (int64, error)
	maxVersionAll func(context.Context) (int64, int64, error)
	snapshot      func(context.Context, GroupKind) ([]*RawObject, int64, error)
	snapshotByID  func(context.Context, GroupKind, ObjectID) ([]*RawObject, int64, error)
}

func (o writesOverride) ListSince(ctx context.Context, gk GroupKind, after int64, limit int) ([]storeapi.ObjectWrite, int64, error) {
	if o.listSince != nil {
		return o.listSince(ctx, gk, after, limit)
	}
	return o.ObjectWrites.ListSince(ctx, gk, after, limit)
}

func (o writesOverride) ListSinceAll(ctx context.Context, after int64, limit int) ([]storeapi.ObjectWrite, int64, error) {
	if o.listSinceAll != nil {
		return o.listSinceAll(ctx, after, limit)
	}
	return o.ObjectWrites.ListSinceAll(ctx, after, limit)
}

func (o writesOverride) MaxVersion(ctx context.Context, gk GroupKind) (int64, error) {
	if o.maxVersion != nil {
		return o.maxVersion(ctx, gk)
	}
	return o.ObjectWrites.MaxVersion(ctx, gk)
}

func (o writesOverride) MaxVersionAll(ctx context.Context) (int64, int64, error) {
	if o.maxVersionAll != nil {
		return o.maxVersionAll(ctx)
	}
	return o.ObjectWrites.MaxVersionAll(ctx)
}

func (o writesOverride) Snapshot(ctx context.Context, gk GroupKind) ([]*RawObject, int64, error) {
	if o.snapshot != nil {
		return o.snapshot(ctx, gk)
	}
	return o.ObjectWrites.Snapshot(ctx, gk)
}

func (o writesOverride) SnapshotByID(ctx context.Context, gk GroupKind, id ObjectID) ([]*RawObject, int64, error) {
	if o.snapshotByID != nil {
		return o.snapshotByID(ctx, gk, id)
	}
	return o.ObjectWrites.SnapshotByID(ctx, gk, id)
}

func (s *fakeStore) Objects() storeapi.Objects { return fakeObjects{} }

// fakeObjects is fakeStore's objects family. The listings answer empty rather
// than panicking: the drivers list objects in every Beehive.
type fakeObjects struct{}

func (fakeObjects) Create(context.Context, GroupKind, ObjectsCreateInput) (*RawObject, error) {
	panic("not implemented: fakeStore.Objects().Create")
}

func (fakeObjects) Delete(context.Context, ObjectID) error {
	panic("not implemented: fakeStore.Objects().Delete")
}

func (fakeObjects) Get(context.Context, ObjectID) (*RawObject, error) {
	panic("not implemented: fakeStore.Objects().Get")
}

func (fakeObjects) GetByName(context.Context, GroupKind, string) (*RawObject, error) {
	panic("not implemented: fakeStore.Objects().GetByName")
}

func (fakeObjects) GetForReconcile(context.Context, ObjectID) (storeapi.ReconcileLoad, error) {
	panic("not implemented: fakeStore.Objects().GetForReconcile")
}

func (fakeObjects) GetMeta(context.Context, ObjectID) (*RawObject, error) {
	panic("not implemented: fakeStore.Objects().GetMeta")
}

func (fakeObjects) GetMetaByName(context.Context, GroupKind, string) (*RawObject, error) {
	panic("not implemented: fakeStore.Objects().GetMetaByName")
}

func (fakeObjects) ListByIDs(context.Context, GroupKind, []ObjectID) ([]*RawObject, error) {
	panic("not implemented: fakeStore.Objects().ListByIDs")
}

func (fakeObjects) ListByIncomingEdge(context.Context, GroupKind, ObjectID, Relation) ([]*RawObject, error) {
	return nil, nil
}

func (fakeObjects) ListIDs(context.Context, GroupKind) ([]ObjectID, error) {
	return nil, nil
}

func (fakeObjects) ListUnsettledIDs(context.Context, GroupKind) ([]ObjectID, error) {
	return nil, nil
}

// Reports no write, so a pass over the fake wakes nobody.
func (fakeObjects) SetObservedGeneration(context.Context, GroupKind, ObjectID, int64) (bool, error) {
	return false, nil
}

func (fakeObjects) UpdateSpec(context.Context, GroupKind, ObjectID, []byte, int) (*RawObject, bool, error) {
	panic("not implemented: fakeStore.Objects().UpdateSpec")
}

func (fakeObjects) UpdateSpecByName(context.Context, GroupKind, string, []byte, int) (*RawObject, bool, error) {
	panic("not implemented: fakeStore.Objects().UpdateSpecByName")
}

func (fakeObjects) UpdateStatus(context.Context, GroupKind, ObjectID, []byte, int) (bool, error) {
	panic("not implemented: fakeStore.Objects().UpdateStatus")
}

func (fakeObjects) List(context.Context, GroupKind) ([]*RawObject, error) {
	return nil, nil
}

func (fakeObjects) DeleteFinalizer(context.Context, GroupKind, ObjectID, string) (bool, error) {
	panic("not implemented: fakeStore.Objects().DeleteFinalizer")
}

// objectsOverride replaces the hooks that are set and delegates the rest.
type objectsOverride struct {
	storeapi.Objects
	create             func(context.Context, GroupKind, storeapi.ObjectsCreateInput) (*RawObject, error)
	get                func(context.Context, ObjectID) (*RawObject, error)
	getForReconcile    func(context.Context, ObjectID) (storeapi.ReconcileLoad, error)
	getMeta            func(context.Context, ObjectID) (*RawObject, error)
	getMetaByName      func(context.Context, GroupKind, string) (*RawObject, error)
	list               func(context.Context, GroupKind) ([]*RawObject, error)
	listByIDs          func(context.Context, GroupKind, []ObjectID) ([]*RawObject, error)
	listByIncomingEdge func(context.Context, GroupKind, ObjectID, Relation) ([]*RawObject, error)
	listIDs            func(context.Context, GroupKind) ([]ObjectID, error)
	listUnsettledIDs   func(context.Context, GroupKind) ([]ObjectID, error)
	delete             func(context.Context, ObjectID) error
	updateSpec         func(context.Context, GroupKind, ObjectID, []byte, int) (*RawObject, bool, error)
	getByName          func(context.Context, GroupKind, string) (*RawObject, error)
	updateStatus       func(context.Context, GroupKind, ObjectID, []byte, int) (bool, error)
	setObservedGen     func(context.Context, GroupKind, ObjectID, int64) (bool, error)
}

func (o objectsOverride) SetObservedGeneration(ctx context.Context, gk GroupKind, id ObjectID, gen int64) (bool, error) {
	if o.setObservedGen != nil {
		return o.setObservedGen(ctx, gk, id, gen)
	}
	return o.Objects.SetObservedGeneration(ctx, gk, id, gen)
}

func (o objectsOverride) Create(ctx context.Context, gk GroupKind, in storeapi.ObjectsCreateInput) (*RawObject, error) {
	if o.create != nil {
		return o.create(ctx, gk, in)
	}
	return o.Objects.Create(ctx, gk, in)
}

func (o objectsOverride) Delete(ctx context.Context, id ObjectID) error {
	if o.delete != nil {
		return o.delete(ctx, id)
	}
	return o.Objects.Delete(ctx, id)
}

func (o objectsOverride) Get(ctx context.Context, id ObjectID) (*RawObject, error) {
	if o.get != nil {
		return o.get(ctx, id)
	}
	return o.Objects.Get(ctx, id)
}

func (o objectsOverride) GetForReconcile(ctx context.Context, id ObjectID) (storeapi.ReconcileLoad, error) {
	if o.getForReconcile != nil {
		return o.getForReconcile(ctx, id)
	}
	return o.Objects.GetForReconcile(ctx, id)
}

func (o objectsOverride) GetMeta(ctx context.Context, id ObjectID) (*RawObject, error) {
	if o.getMeta != nil {
		return o.getMeta(ctx, id)
	}
	return o.Objects.GetMeta(ctx, id)
}

func (o objectsOverride) GetMetaByName(ctx context.Context, gk GroupKind, name string) (*RawObject, error) {
	if o.getMetaByName != nil {
		return o.getMetaByName(ctx, gk, name)
	}
	return o.Objects.GetMetaByName(ctx, gk, name)
}

func (o objectsOverride) List(ctx context.Context, gk GroupKind) ([]*RawObject, error) {
	if o.list != nil {
		return o.list(ctx, gk)
	}
	return o.Objects.List(ctx, gk)
}

func (o objectsOverride) ListByIDs(ctx context.Context, gk GroupKind, ids []ObjectID) ([]*RawObject, error) {
	if o.listByIDs != nil {
		return o.listByIDs(ctx, gk, ids)
	}
	return o.Objects.ListByIDs(ctx, gk, ids)
}

func (o objectsOverride) ListByIncomingEdge(ctx context.Context, gk GroupKind, to ObjectID, rel Relation) ([]*RawObject, error) {
	if o.listByIncomingEdge != nil {
		return o.listByIncomingEdge(ctx, gk, to, rel)
	}
	return o.Objects.ListByIncomingEdge(ctx, gk, to, rel)
}

func (o objectsOverride) ListIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error) {
	if o.listIDs != nil {
		return o.listIDs(ctx, gk)
	}
	return o.Objects.ListIDs(ctx, gk)
}

func (o objectsOverride) ListUnsettledIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error) {
	if o.listUnsettledIDs != nil {
		return o.listUnsettledIDs(ctx, gk)
	}
	return o.Objects.ListUnsettledIDs(ctx, gk)
}

func (o objectsOverride) UpdateSpec(ctx context.Context, gk GroupKind, id ObjectID, spec []byte, v int) (*RawObject, bool, error) {
	if o.updateSpec != nil {
		return o.updateSpec(ctx, gk, id, spec, v)
	}
	return o.Objects.UpdateSpec(ctx, gk, id, spec, v)
}

func (o objectsOverride) GetByName(ctx context.Context, gk GroupKind, name string) (*RawObject, error) {
	if o.getByName != nil {
		return o.getByName(ctx, gk, name)
	}
	return o.Objects.GetByName(ctx, gk, name)
}

func (o objectsOverride) UpdateStatus(ctx context.Context, gk GroupKind, id ObjectID, status []byte, version int) (bool, error) {
	if o.updateStatus != nil {
		return o.updateStatus(ctx, gk, id, status, version)
	}
	return o.Objects.UpdateStatus(ctx, gk, id, status, version)
}

func (s *fakeStore) Conditions() storeapi.Conditions { return fakeConditions{} }

// fakeConditions is fakeStore's conditions family.
type fakeConditions struct{}

func (fakeConditions) Set(context.Context, GroupKind, ObjectID, ...storeapi.Condition) error {
	panic("not implemented: fakeStore.Conditions().Set")
}

func (fakeConditions) Delete(context.Context, GroupKind, ObjectID, string) error {
	panic("not implemented: fakeStore.Conditions().Delete")
}

func (s *fakeStore) Events() storeapi.Events { return fakeEvents{} }

// fakeEvents is fakeStore's event-log family. Sweep answers zero rather than
// panicking: the GC sweeper runs in every Beehive.
type fakeEvents struct{}

func (fakeEvents) Add(context.Context, GroupKind, ObjectID, EventsAddInput) error {
	panic("not implemented: fakeStore.Events().Add")
}

func (fakeEvents) GetLatest(context.Context, ObjectID, string) (*RawEvent, error) {
	panic("not implemented: fakeStore.Events().GetLatest")
}

func (fakeEvents) List(context.Context, ObjectID, storeapi.EventQuery) ([]RawEvent, error) {
	panic("not implemented: fakeStore.Events().List")
}

func (fakeEvents) ListSince(context.Context, ObjectID, *string, int64, int) ([]RawEvent, int64, error) {
	panic("not implemented: fakeStore.Events().ListSince")
}

func (fakeEvents) MaxVersion(context.Context, ObjectID) (int64, error) {
	panic("not implemented: fakeStore.Events().MaxVersion")
}

func (fakeEvents) Snapshot(context.Context, ObjectID, storeapi.EventQuery) ([]RawEvent, int64, error) {
	panic("not implemented: fakeStore.Events().Snapshot")
}

func (fakeEvents) Sweep(context.Context, int, time.Duration, int) (int, error) {
	panic("not implemented: fakeStore.Events().Sweep")
}

// eventsOverride replaces the hooks that are set and delegates the rest.
type eventsOverride struct {
	storeapi.Events
	getLatest  func(context.Context, ObjectID, string) (*RawEvent, error)
	list       func(context.Context, ObjectID, storeapi.EventQuery) ([]RawEvent, error)
	listSince  func(context.Context, ObjectID, *string, int64, int) ([]RawEvent, int64, error)
	maxVersion func(context.Context, ObjectID) (int64, error)
	snapshot   func(context.Context, ObjectID, storeapi.EventQuery) ([]RawEvent, int64, error)
	sweep      func(context.Context, int, time.Duration, int) (int, error)
}

func (o eventsOverride) GetLatest(ctx context.Context, id ObjectID, category string) (*RawEvent, error) {
	if o.getLatest != nil {
		return o.getLatest(ctx, id, category)
	}
	return o.Events.GetLatest(ctx, id, category)
}

func (o eventsOverride) List(ctx context.Context, id ObjectID, q storeapi.EventQuery) ([]RawEvent, error) {
	if o.list != nil {
		return o.list(ctx, id, q)
	}
	return o.Events.List(ctx, id, q)
}

func (o eventsOverride) ListSince(ctx context.Context, id ObjectID, cat *string, afterRV int64, limit int) ([]RawEvent, int64, error) {
	if o.listSince != nil {
		return o.listSince(ctx, id, cat, afterRV, limit)
	}
	return o.Events.ListSince(ctx, id, cat, afterRV, limit)
}

func (o eventsOverride) MaxVersion(ctx context.Context, id ObjectID) (int64, error) {
	if o.maxVersion != nil {
		return o.maxVersion(ctx, id)
	}
	return o.Events.MaxVersion(ctx, id)
}

func (o eventsOverride) Snapshot(ctx context.Context, id ObjectID, q storeapi.EventQuery) ([]RawEvent, int64, error) {
	if o.snapshot != nil {
		return o.snapshot(ctx, id, q)
	}
	return o.Events.Snapshot(ctx, id, q)
}

func (o eventsOverride) Sweep(ctx context.Context, perTimeline int, maxAge time.Duration, capBudget int) (int, error) {
	if o.sweep != nil {
		return o.sweep(ctx, perTimeline, maxAge, capBudget)
	}
	return o.Events.Sweep(ctx, perTimeline, maxAge, capBudget)
}

// ReclaimSpace reclaims nothing, which the contract permits. The GC sweeper
// calls it every tick, so a panic here would reach every Beehive.
func (s *fakeStore) ReclaimSpace(context.Context, int) (int, error) {
	return 0, nil
}

func (s *fakeStore) DriverCursors() storeapi.DriverCursors { return noopDriverCursors{} }

// noopDriverCursors persists nothing, which the contract permits: ok=false is
// "none stored yet", so a waker over it reseeds from the write log's max every
// time. cursorStore is the persisting double.
type noopDriverCursors struct{}

func (noopDriverCursors) Get(context.Context, string) (int64, bool, error) { return 0, false, nil }
func (noopDriverCursors) Set(context.Context, string, int64) error         { return nil }

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

func (s *depsStore) Edges() storeapi.Edges {
	return edgesOverride{Edges: s.fakeStore.Edges(), groupIncomingByID: s.groupIncomingByIDEdges}
}

func (s *depsStore) groupIncomingByIDEdges(_ context.Context, toIDs []ObjectID, _ Relation) (map[ObjectID][]ObjectRef, error) {
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

// seedProbe answers the waker's seed read itself — the wrapped store serves
// everything else — and reports what it was asked. onRead runs inside the read,
// for a test that needs to act at that instant.
type seedProbe struct {
	Store
	mark    int64
	trimmed int64
	err     error
	onRead  func()
	reads   int
}

func (s *seedProbe) ObjectWrites() storeapi.ObjectWrites {
	return writesOverride{ObjectWrites: s.Store.ObjectWrites(), maxVersionAll: s.maxVersionAllObjectWrites}
}

func (s *seedProbe) maxVersionAllObjectWrites(context.Context) (int64, int64, error) {
	s.reads++
	if s.onRead != nil {
		s.onRead()
	}
	return s.mark, s.trimmed, s.err
}

// replayStore serves ObjectWritesListSinceAll from a fixed set of rows, recording the
// cursor and limit of every page it was asked for. It is the whole of what the
// waker can see, so a test scripts a scan by setting rows and reads back what the
// waker asked for.
type replayStore struct {
	depsStore
	rows    []ObjectWrite // every live row, in version order
	seed    int64         // the mark ObjectWritesMaxVersionAll reports
	trimmed int64         // the retention horizon both reads report
	marks   int           // ObjectWritesMaxVersionAll calls
	pages   [][2]int64    // (afterRV, limit) per call
	read    int           // rows actually served, across every page
	lists   chan struct{} // one token per page request, when set
	err     error
	seedErr error

	// failFromCall makes err apply only from the given call onward (1-indexed),
	// so a test can script an early page succeeding before a later one fails —
	// proving what happened before the failure is not thrown away. Zero (the
	// default) means err, when set, applies to every call, as before this field
	// existed.
	failFromCall int
	// healFromCall stops err applying from the given call onward, for a test
	// that scripts a store recovering from an outage.
	healFromCall int
}

func (s *replayStore) ObjectWrites() storeapi.ObjectWrites {
	return writesOverride{ObjectWrites: s.depsStore.ObjectWrites(), maxVersionAll: s.maxVersionAllObjectWrites, listSinceAll: s.listSinceAllObjectWrites}
}

func (s *replayStore) maxVersionAllObjectWrites(context.Context) (int64, int64, error) {
	s.marks++
	if s.seedErr != nil {
		return 0, 0, s.seedErr
	}
	return s.seed, s.trimmed, nil
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

// failing says whether this call — the one already recorded in pages — is
// scripted to fail.
func (s *replayStore) failing() bool {
	switch {
	case s.err == nil:
		return false
	case s.failFromCall > 0 && len(s.pages) < s.failFromCall:
		return false
	case s.healFromCall > 0 && len(s.pages) >= s.healFromCall:
		return false
	}
	return true
}

func (s *replayStore) listSinceAllObjectWrites(_ context.Context, afterRV int64, limit int) ([]ObjectWrite, int64, error) {
	s.pages = append(s.pages, [2]int64{afterRV, int64(limit)})
	probeSignal(s.lists)
	if s.failing() {
		return nil, 0, s.err
	}
	out := make([]ObjectWrite, 0, min(limit, len(s.rows)))
	for _, r := range s.rows {
		if len(out) == limit {
			break
		}
		if r.ResourceVersion > afterRV {
			out = append(out, r)
		}
	}
	s.read += len(out)
	if len(out) == 0 {
		return out, 0, nil // an empty page carries no horizon, as the store's does not
	}
	return out, s.trimmed, nil
}

// replayRows builds count live rows at versions 1..count.
func replayRows(count int) []ObjectWrite {
	rows := make([]ObjectWrite, 0, count)
	for i := 1; i <= count; i++ {
		rows = append(rows, ObjectWrite{ID: ObjectID(i), ResourceVersion: int64(i)})
	}
	return rows
}

// cursorStore gives replayStore a real cursor table, so a waker test can script
// what a durable store already holds for the waker's cursor name and observe what
// it writes. A plain *replayStore persists nothing and always reports ok=false —
// that is what exercises the no-persistence fallback — so this is a distinct type
// rather than a field toggle.
type cursorStore struct {
	replayStore
	stored map[string]int64 // what DriverCursorsGet reports; nil means nothing stored
	getErr error
	setErr error

	// setCalls holds the cursor values DriverCursorsSet *stored*, in order, and
	// setAttempts counts every call including the ones setErr failed — which is
	// what a test asserting on retry backoff has to count, since a failed write
	// stores nothing but still costs the round trip.
	setCalls    []int64
	setAttempts int
}

func (s *cursorStore) DriverCursors() storeapi.DriverCursors { return cursorStoreCursors{s} }

// cursorStoreCursors is cursorStore's scripted cursor table.
type cursorStoreCursors struct{ *cursorStore }

func (s cursorStoreCursors) Get(_ context.Context, name string) (int64, bool, error) {
	if s.getErr != nil {
		return 0, false, s.getErr
	}
	v, ok := s.stored[name]
	return v, ok, nil
}

func (s cursorStoreCursors) Set(_ context.Context, name string, cursor int64) error {
	s.setAttempts++
	if s.setErr != nil {
		return s.setErr
	}
	s.setCalls = append(s.setCalls, cursor)
	if s.stored == nil {
		s.stored = map[string]int64{}
	}
	if cursor > s.stored[name] {
		s.stored[name] = cursor
	}
	return nil
}

// noopController is a no-op test double for Controller, used wherever a test
// needs a registered controller but never exercises its reconcile behaviour.
// Tests that need a ControllerClient build one with registerWithClient.
type noopController[Spec, Status any] struct{}

// Unsettled, and far enough out that nothing re-dispatches inside a test: a
// Settled pass would stamp the generation, a real write these tests do not expect.
func (noopController[Spec, Status]) Reconcile(_ context.Context, _ ControllerClient[Status], _ *Object[Spec, Status]) ReconcileResult {
	return Unsettled().RequeueAfter(time.Hour)
}

// registerNoop registers a do-nothing controller for gk, making the kind count as
// registered without standing up any reconcile behaviour. Tests that create rows
// WithFinalizers need it: a finalizer on a client-only kind is rejected at create,
// because nothing in the process could ever clear it (see clientImpl.resolveCreate).
// It registers only — nothing starts — so a fixture that never calls Start is
// otherwise unchanged.
func registerNoop[Spec, Status any](t *testing.T, bh *Beehive, gk GroupKind) {
	t.Helper()
	require.NoError(t, Register(bh, gk, &noopController[Spec, Status]{}))
}

// registerWithClient registers c for gk and returns pass clients for it,
// standing in for the pair Register used to return. Live, so it exercises the
// write surface but not the scoping — that belongs to the pass-client tests.
func registerWithClient[Spec, Status any](
	t *testing.T, bh *Beehive, gk GroupKind, c Controller[Spec, Status], opts ...Option,
) passClients[Status] {
	t.Helper()
	require.NoError(t, Register(bh, gk, c, opts...))
	return passClients[Status]{bh: bh, gk: gk}
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
	ids, err := store.Objects().ListUnsettledIDs(context.Background(), clientTestGK)
	require.NoError(t, err)
	return ids
}

// drainQueue empties q, discarding rather than dispatching: a later add in the
// test must not be held by a floor this loop opened.
func drainQueue(q *workQueue) {
	for id, ok := q.get(); ok; id, ok = q.get() {
		q.forget(id)
	}
}

// hotLoopCalls is how many reconciles of one object prove that a backoff ladder
// was bypassed. The ladder starts at defaultBaseRetryInterval and doubles, so a
// handful of passes inside hotLoopWindow is the ladder working. The failure mode
// it separates from that is unbounded, not merely faster.
const hotLoopCalls = 25

// hotLoopWindow is how long a hot loop is given to prove itself. It is a failsafe
// and not a synchronisation point: hot firing is what fails the test, and this
// only bounds the wait for it.
const hotLoopWindow = 500 * time.Millisecond

// requireNoHotLoop fails if hot fired, which a controller does once it has run
// hotLoopCalls times. There is no clock to assert on instead: baseRetryInterval
// has no option, and WithMaxRetryInterval only caps upward — so a retry loop
// running at full speed is told from one climbing its ladder by counting passes
// inside a fixed window.
//
// It waits on first before it opens that window, and that wait is what stops the
// count from being a bound with nothing under it. A window on its own passes when
// the controller never ran at all, so a broken Start, a broken Register or a
// broken enqueue would leave this green. first fires on the controller's own first
// pass, so the assertion below reads "it ran, and then it did not run away".
func requireNoHotLoop(t *testing.T, first, hot *signal, calls *atomic.Int64, msg string) {
	t.Helper()
	first.wait(t, "the first reconcile: nothing dispatched the object at all")
	select {
	case <-hot.rx.Chan():
		t.Fatalf("hot loop: %d reconciles, so the backoff ladder was bypassed: %s", calls.Load(), msg)
	case <-time.After(hotLoopWindow):
		assert.Less(t, calls.Load(), int64(hotLoopCalls), msg)
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

// awaitQueueIdle blocks until id is neither queued nor in flight, so a following
// step cannot be served by a pass that was already running. Both halves are read
// in one lock hold: released between them, the worker moving id from items to
// processing would read as idle.
func awaitQueueIdle(t *testing.T, q *workQueue, id ObjectID) {
	t.Helper()
	require.Eventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		_, busy := q.processing[id]
		return !busy && !queuedForLocked(q, id)
	}, testTimeout, time.Millisecond, "object %d never went idle", id)
}

// reconcileCapture is a Controller that reports every object it reconciles on a
// channel, so a test can assert which objects a write or a wake requeued.
type reconcileCapture struct {
	ch chan *Object[tSpec, tStatus]
}

func (c *reconcileCapture) Reconcile(_ context.Context, _ ControllerClient[tStatus], obj *Object[tSpec, tStatus]) ReconcileResult {
	c.ch <- obj
	return Settled()
}

// addEdge declares an edge for test scaffolding: it drains the owed-wake stamp
// every new depends_on edge now records — scaffolding wants the edge to exist, not
// the reconcile it buys — so the common require.NoError(t, addEdge(...)) shape
// stays a one-liner with no side effects on the owed listings. Tests that assert
// on the EdgesAddResult or the stamp call the method directly.
func addEdge(ctx context.Context, store Store, from, to ObjectID, relation Relation) error {
	res, err := store.Edges().Add(ctx, from, to, relation)
	if err != nil {
		return err
	}
	if !res.ReconcileOwedStamped {
		return nil
	}
	// The decrement is kind-scoped and scaffolding declares edges across kinds, so
	// the source's own kind is needed here; the edge just written names it.
	sources, err := store.Edges().ListIncoming(ctx, to, relation)
	if err != nil {
		return err
	}
	for _, src := range sources {
		if src.ID == from {
			return store.ReconcileOwed().Decrement(ctx, src.GroupKind(), from, 1)
		}
	}
	return fmt.Errorf("addEdge: %d is missing from the edge it just wrote", from)
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

// staleDependentIDs is what the stale-dependents pass would find right now: the
// listing from the beginning, bounded above by everything issued, deduped.
//
// The listing returns one row per (target, dependent) pair, so a dependent with
// two moved targets appears twice. The sweep folds that in the queue, and these
// assertions are about which objects are owed a pass, not how many rows say so.
func staleDependentIDs(t *testing.T, store Store, gk GroupKind) []ObjectID {
	t.Helper()
	refs, _, err := store.Dependencies().ListStaleSince(
		context.Background(), []GroupKind{gk}, StalePos{}, math.MaxInt64, 100)
	require.NoError(t, err)
	seen := make(map[ObjectID]struct{}, len(refs))
	var ids []ObjectID
	for _, r := range refs {
		if _, dup := seen[r.ID]; dup {
			continue
		}
		seen[r.ID] = struct{}{}
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

func (s *wakeProbeStore) Edges() storeapi.Edges {
	return edgesOverride{Edges: s.Store.Edges(), listIncoming: s.listIncomingEdges, groupIncomingByID: s.groupIncomingByIDEdges}
}

func (s *wakeProbeStore) listIncomingEdges(ctx context.Context, toID ObjectID, relation Relation) ([]ObjectRef, error) {
	refs, err := s.Store.Edges().ListIncoming(ctx, toID, relation)
	if toID == s.targetID {
		s.note(relation)
	}
	return refs, err
}

// EdgesGroupIncomingByID is the waker's own lookup (it resolves a whole batch of
// changed targets in one query), so the probe has to cover it too — otherwise a
// test waiting on "the waker looked" would wait forever.
func (s *wakeProbeStore) groupIncomingByIDEdges(ctx context.Context, toIDs []ObjectID, relation Relation) (map[ObjectID][]ObjectRef, error) {
	refs, err := s.Store.Edges().GroupIncomingByID(ctx, toIDs, relation)
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
	staleListed     chan struct{} // Dependencies().ListStaleSince (global)
	watermarkSet    chan struct{} // Dependencies().WatermarkSet (per successful dependent pass)

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

func (s *listProbeStore) Objects() storeapi.Objects {
	return objectsOverride{Objects: s.Store.Objects(), listUnsettledIDs: s.listUnsettledIDsObjects}
}

func (s *listProbeStore) listUnsettledIDsObjects(ctx context.Context, gk GroupKind) ([]ObjectID, error) {
	ids, err := s.Store.Objects().ListUnsettledIDs(ctx, gk)
	probeSignal(s.unsettledListed)
	return ids, err
}

func (s *listProbeStore) ReconcileOwed() storeapi.ReconcileOwed {
	return owedOverride{ReconcileOwed: s.Store.ReconcileOwed(), listIDs: s.probeOwedListIDs}
}

func (s *listProbeStore) probeOwedListIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error) {
	ids, err := s.Store.ReconcileOwed().ListIDs(ctx, gk)
	probeSignal(s.owedListed)
	return ids, err
}

func (s *listProbeStore) DeletionRequests() storeapi.DeletionRequests {
	return delReqOverride{DeletionRequests: s.Store.DeletionRequests(), list: s.probeDelReqList}
}

func (s *listProbeStore) probeDelReqList(ctx context.Context) ([]storeapi.ObjectRef, error) {
	rows, err := s.Store.DeletionRequests().List(ctx)
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

func (s *listProbeStore) Dependencies() storeapi.Dependencies {
	return depsOverride{
		Dependencies:   s.Store.Dependencies(),
		listStaleSince: s.probeListStaleSince,
		watermarkSet:   s.probeWatermarkSet,
	}
}

// probeListStaleSince is the form the sweep calls, so it carries the probe.
func (s *listProbeStore) probeListStaleSince(ctx context.Context, kinds []GroupKind, after StalePos, through int64, limit int) ([]storeapi.ObjectRef, StalePos, error) {
	refs, next, err := s.Store.Dependencies().ListStaleSince(ctx, kinds, after, through, limit)
	s.mu.Lock()
	s.staleKinds = append(s.staleKinds, slices.Clone(kinds))
	s.mu.Unlock()
	probeSignal(s.staleListed)
	return refs, next, err
}

// kindsAsked snapshots what the stale-dependents listings have been asked for.
func (s *listProbeStore) kindsAsked() [][]GroupKind {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.staleKinds)
}

// probeWatermarkSet signals *after* the write, so a test can order a change to a
// target against the dependent's pass having already recorded what it observed —
// without which the change might land under the pass and be recorded as seen.
func (s *listProbeStore) probeWatermarkSet(ctx context.Context, id ObjectID, cursor int64) error {
	err := s.Store.Dependencies().WatermarkSet(ctx, id, cursor)
	probeSignal(s.watermarkSet)
	return err
}

// uniqueName returns a name no other test row holds, for the tests that seed rows
// through the store directly rather than through Client.Create. Names are required
// and unique per kind, but these tests assert on versions, edges and watermarks —
// never on the name — so naming each row by hand would be noise.
func uniqueName() string {
	return fmt.Sprintf("test-obj-%d", nameSeq.Add(1))
}

var nameSeq atomic.Int64

// pollProbeStore signals each time the watch surface reads, so a test can wait for
// a poll it expects rather than assuming one has happened. Errors are injected per
// call site, which is what lets a test drive one failure branch at a time.
type pollProbeStore struct {
	Store
	// polled fires once per object-watch tick. It hangs off the store-wide cursor
	// read because that is the one call every tick makes: the write-log probe and the
	// listings past it are exactly what a quiet tick skips.
	polled chan struct{}
	// listed fires after a listing returns, so a test can cancel knowing the read
	// already succeeded and the goroutine is on its way to the send.
	listed chan struct{}
	// eventsListed is the event reader's equivalent of tailed, and eventsFailed
	// its failure counterpart; metaRead covers the existence probe a pass makes
	// while the id is still unassigned, which is the only clock an unresolved
	// stream has.
	eventsListed chan struct{}
	metaRead     chan struct{}
	eventsFailed chan struct{}
	// byIDs records the id batches the tail read, so a test can assert it read
	// what changed rather than the whole kind.
	byIDs chan []ObjectID
	// tailed fires after the tail's own listing of the write log returns.
	tailed chan struct{}
	// forceTrimmed overrides the horizon the tail's listing reports, so a test
	// can sit exactly on the boundary without staging real retention;
	// forceEventTrimmed is its counterpart for the event log.
	forceTrimmed      atomic.Int64
	forceEventTrimmed atomic.Int64
	listErr           atomic.Bool
	listIDsErr        atomic.Bool
	getErr            atomic.Bool
	eventsErr         atomic.Bool
	eventsSnapErr     atomic.Bool
	markErr           atomic.Bool
	metaErr           atomic.Bool
}

func (s *pollProbeStore) ObjectWrites() storeapi.ObjectWrites {
	return writesOverride{ObjectWrites: s.Store.ObjectWrites(), maxVersion: s.maxVersionObjectWrites, snapshot: s.snapshotObjectWrites, snapshotByID: s.snapshotByIDObjectWrites, listSince: s.listSinceObjectWrites}
}

func (s *pollProbeStore) maxVersionObjectWrites(ctx context.Context, gk GroupKind) (int64, error) {
	at, err := s.Store.ObjectWrites().MaxVersion(ctx, gk)
	probeSignal(s.polled)
	return at, err
}

// The snapshot reads are the watch's first read, so they carry the same failure
// injection and the same signal as the listing they replaced.
func (s *pollProbeStore) snapshotObjectWrites(ctx context.Context, gk GroupKind) ([]*RawObject, int64, error) {
	if s.listErr.Load() {
		return nil, 0, errBoom
	}
	out, at, err := s.Store.ObjectWrites().Snapshot(ctx, gk)
	probeSignal(s.listed)
	return out, at, err
}

func (s *pollProbeStore) snapshotByIDObjectWrites(ctx context.Context, gk GroupKind, id ObjectID) ([]*RawObject, int64, error) {
	if s.getErr.Load() {
		return nil, 0, errBoom
	}
	return s.Store.ObjectWrites().SnapshotByID(ctx, gk, id)
}

// ObjectWritesListSince is the tail's own listing: it carries listErr and signals
// after the read, which is the seam the cancellation tests need — past it the only
// thing left that can observe a cancelled context is the send itself.
func (s *pollProbeStore) listSinceObjectWrites(ctx context.Context, gk GroupKind, afterRV int64, limit int) ([]ObjectWrite, int64, error) {
	if s.listErr.Load() {
		return nil, 0, errBoom
	}
	page, trimmed, err := s.Store.ObjectWrites().ListSince(ctx, gk, afterRV, limit)
	if forced := s.forceTrimmed.Load(); forced > 0 {
		trimmed = forced
	}
	probeSignal(s.tailed)
	return page, trimmed, err
}

// ObjectsList signals *after* the read returns.
func (s *pollProbeStore) Objects() storeapi.Objects {
	return objectsOverride{Objects: s.Store.Objects(), list: s.listObjects, listByIDs: s.listByIDsObjects, listIDs: s.listIDsObjects, get: s.getObjects, getMeta: s.getMetaObjects}
}

func (s *pollProbeStore) listObjects(ctx context.Context, gk GroupKind) ([]*RawObject, error) {
	if s.listErr.Load() {
		return nil, errBoom
	}
	out, err := s.Store.Objects().List(ctx, gk)
	probeSignal(s.listed)
	return out, err
}

func (s *pollProbeStore) listByIDsObjects(ctx context.Context, gk GroupKind, ids []ObjectID) ([]*RawObject, error) {
	if s.getErr.Load() {
		return nil, errBoom
	}
	out, err := s.Store.Objects().ListByIDs(ctx, gk, ids)
	select {
	case s.byIDs <- ids:
	default:
	}
	return out, err
}

func (s *pollProbeStore) listIDsObjects(ctx context.Context, gk GroupKind) ([]ObjectID, error) {
	if s.listIDsErr.Load() {
		return nil, errBoom
	}
	return s.Store.Objects().ListIDs(ctx, gk)
}

func (s *pollProbeStore) getObjects(ctx context.Context, id ObjectID) (*RawObject, error) {
	if s.getErr.Load() {
		return nil, errBoom
	}
	return s.Store.Objects().Get(ctx, id)
}

// ObjectsGetMeta is the event watch's per-tick read, so it signals on every call —
// error or not — which is what a test waiting out a failing phase needs.
func (s *pollProbeStore) getMetaObjects(ctx context.Context, id ObjectID) (*RawObject, error) {
	defer probeSignal(s.metaRead)
	if s.metaErr.Load() {
		return nil, errBoom
	}
	return s.Store.Objects().GetMeta(ctx, id)
}

// EventsListSince is the event reader's own page read: it carries eventsErr and
// signals after the read, which is the seam the cancellation test needs — past
// it the only thing left that can observe a cancelled context is the send.
func (s *pollProbeStore) Events() storeapi.Events {
	return eventsOverride{
		Events:     s.Store.Events(),
		listSince:  s.eventsListSince,
		maxVersion: s.eventsMaxVersion,
		snapshot:   s.eventsSnapshot,
	}
}

func (s *pollProbeStore) eventsListSince(ctx context.Context, id ObjectID, category *string, afterRV int64, limit int) ([]RawEvent, int64, error) {
	if s.eventsErr.Load() {
		probeSignal(s.eventsFailed)
		return nil, 0, errBoom
	}
	page, trimmed, err := s.Store.Events().ListSince(ctx, id, category, afterRV, limit)
	if forced := s.forceEventTrimmed.Load(); forced > 0 {
		trimmed = forced
	}
	probeSignal(s.eventsListed)
	return page, trimmed, err
}

// EventsSnapshot and EventsMaxVersion are the reads only the subscribe path
// makes, each with its own fault so a test can drive one at a time.
func (s *pollProbeStore) eventsSnapshot(ctx context.Context, id ObjectID, q storeapi.EventQuery) ([]RawEvent, int64, error) {
	if s.eventsSnapErr.Load() {
		return nil, 0, errBoom
	}
	return s.Store.Events().Snapshot(ctx, id, q)
}

func (s *pollProbeStore) eventsMaxVersion(ctx context.Context, id ObjectID) (int64, error) {
	if s.markErr.Load() {
		return 0, errBoom
	}
	return s.Store.Events().MaxVersion(ctx, id)
}

// watchFixture wires a Beehive with one registered kind over a probe store — the
// shape all three watch surfaces' tests need, which is why it lives here rather
// than with any one of them. The ControllerClient comes back because the event
// watches need something that can write to a log.
func watchFixture(t *testing.T) (*pollProbeStore, *Beehive, Client[cSpec, cStatus], passClients[cStatus]) {
	t.Helper()
	return watchFixtureWith(t)
}

// foreignObject registers a second kind on bh and creates one object of it, for
// the reads that take an id whatever kind holds it. The ControllerClient comes
// back because only a controller writes that object's events.
func foreignObject(t *testing.T, ctx context.Context, bh *Beehive) (ObjectID, passClients[cStatus]) {
	t.Helper()
	gk := GroupKind{Kind: "Other"}
	cc := registerWithClient(t, bh, gk, &noopController[cSpec, cStatus]{})
	obj := mustCreate(t, ctx, NewClient[cSpec, cStatus](bh, gk), uniqueName(), cSpec{Val: "foreign"})
	return obj.ID, cc
}

// watchFixtureWith is watchFixture with extra options on the beehive.
func watchFixtureWith(t *testing.T, opts ...Option) (*pollProbeStore, *Beehive, Client[cSpec, cStatus], passClients[cStatus]) {
	t.Helper()
	store := &pollProbeStore{
		Store:        newClientTestStore(t),
		polled:       make(chan struct{}, 256),
		listed:       make(chan struct{}, 256),
		eventsListed: make(chan struct{}, 256),
		metaRead:     make(chan struct{}, 256),
		eventsFailed: make(chan struct{}, 256),
		byIDs:        make(chan []ObjectID, 256),
		tailed:       make(chan struct{}, 256),
	}
	bh := newTestBeehive(t, store, fast(opts...)...)
	cc := registerWithClient(t, bh, clientTestGK, &noopController[cSpec, cStatus]{})
	return store, bh, NewClient[cSpec, cStatus](bh, clientTestGK), cc
}

// reconcilePass splits the failure back out of the result, the shape most
// reconcile tests assert on.
func reconcilePass(a controllerAdapter, ctx context.Context, id ObjectID) (ReconcileResult, bool, error) {
	result, gone := a.reconcile(ctx, id)
	return result, gone, result.err
}

// objectsOverrideStore swaps one kind's Objects sub-API for a hooked one, so a
// test can count or fail a single store call without a whole fake.
type objectsOverrideStore struct {
	Store
	override objectsOverride
}

func (s *objectsOverrideStore) Objects() storeapi.Objects {
	if s.override.Objects == nil {
		s.override.Objects = s.Store.Objects()
	}
	return s.override
}

// orderProbeStore records the order of the two post-reconcile writes whose
// ordering is load-bearing: the watermark must commit before the stamp.
type orderProbeStore struct {
	Store
	record func(string)
}

func (s *orderProbeStore) Objects() storeapi.Objects {
	return objectsOverride{
		Objects: s.Store.Objects(),
		setObservedGen: func(ctx context.Context, gk GroupKind, id ObjectID, gen int64) (bool, error) {
			s.record("stamp")
			return s.Store.Objects().SetObservedGeneration(ctx, gk, id, gen)
		},
	}
}

func (s *orderProbeStore) Dependencies() storeapi.Dependencies {
	return depsOverride{
		Dependencies: s.Store.Dependencies(),
		watermarkSet: func(ctx context.Context, id ObjectID, cursor int64) error {
			s.record("watermark")
			return s.Store.Dependencies().WatermarkSet(ctx, id, cursor)
		},
	}
}

// settleRow records id's current generation as observed, for tests needing a
// settled row without a reconcile loop to produce one.
func settleRow(t *testing.T, ctx context.Context, store Store, gk GroupKind, id ObjectID) {
	t.Helper()
	raw, err := store.Objects().GetMeta(ctx, id)
	require.NoError(t, err)
	_, err = store.Objects().SetObservedGeneration(ctx, gk, id, raw.Generation)
	require.NoError(t, err)
}

// waitSettled waits for beehive's generation stamp and returns the settled
// object. The stamp lands after Reconcile returns, so a controller's own signal
// fires before it and cannot be waited on in its place.
func waitSettled(t *testing.T, ctx context.Context, client Client[cSpec, cStatus], id ObjectID) *Object[cSpec, cStatus] {
	t.Helper()
	var got *Object[cSpec, cStatus]
	require.Eventually(t, func() bool {
		o, err := client.Get(ctx, id)
		if err != nil || o.ObservedGeneration == nil {
			return false
		}
		got = o
		return *o.ObservedGeneration == o.Generation
	}, testTimeout, time.Millisecond, "beehive to record the generation the pass settled")
	return got
}
