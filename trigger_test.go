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
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/amorey/beehive/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var triggerGK = GroupKind{Kind: "Trigger"}

// newTriggerReconciler builds a reconciler over a real store with nothing
// running but its work queue, so a test observes exactly what a trigger queued.
func newTriggerReconciler(t *testing.T) *reconciler {
	t.Helper()
	store, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })

	r := &reconciler{gk: triggerGK, store: store, work: newWorkQueue()}
	t.Cleanup(r.work.stop)
	return r
}

// triggerObject creates one object of the trigger kind and returns it.
func triggerObject(t *testing.T, r *reconciler, name string) *RawObject {
	t.Helper()
	obj, err := r.store.Objects().Create(context.Background(), r.gk,
		ObjectsCreateInput{Name: name, Spec: []byte(`{}`)})
	require.NoError(t, err)
	return obj
}

// runTrigger starts trig and returns a channel closed when it has returned.
func runTrigger(ctx context.Context, trig *trigger) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		trig.run(ctx)
	}()
	return done
}

// waitQueued waits for the queue to signal and takes what it holds.
func waitQueued(t *testing.T, q *workQueue) ObjectID {
	t.Helper()
	waitClosedOrValue(t, q)
	id, ok := q.get()
	require.True(t, ok, "the queue signalled but held nothing")
	return id
}

func waitClosedOrValue(t *testing.T, q *workQueue) {
	t.Helper()
	select {
	case <-q.ready:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for the trigger to queue an object")
	}
}

func TestTriggerByIDRequeuesTheObject(t *testing.T) {
	r := newTriggerReconciler(t)
	obj := triggerObject(t, r, "one")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan ObjectID)
	done := runTrigger(ctx, &trigger{r: r, ids: ch})

	ch <- obj.ID
	assert.Equal(t, obj.ID, waitQueued(t, r.work))

	cancel()
	waitClosed(t, done, "the trigger loop to return")
}

// Objects().GetForReconcile takes a bare id, so a foreign id reaching the queue
// would hand one kind's row to another kind's controller. Every other enqueue
// path is kind-routed by construction; a trigger is the first that takes an
// address from outside, so it gates the kind itself.
func TestTriggerByIDIgnoresAForeignKind(t *testing.T) {
	r := newTriggerReconciler(t)
	mine := triggerObject(t, r, "mine")
	foreign, err := r.store.Objects().Create(context.Background(), GroupKind{Kind: "Other"},
		ObjectsCreateInput{Name: "theirs", Spec: []byte(`{}`)})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan ObjectID)
	done := runTrigger(ctx, &trigger{r: r, ids: ch})

	ch <- foreign.ID
	ch <- ObjectID(9999) // and one that names nothing at all
	ch <- mine.ID
	assert.Equal(t, mine.ID, waitQueued(t, r.work))
	_, ok := r.work.get()
	assert.False(t, ok, "only this kind's id may be queued")

	cancel()
	waitClosed(t, done, "the trigger loop to return")
}

func TestTriggerByNameRequeuesTheObject(t *testing.T) {
	r := newTriggerReconciler(t)
	obj := triggerObject(t, r, "one")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan string)
	done := runTrigger(ctx, &trigger{r: r, names: ch})

	ch <- "one"
	assert.Equal(t, obj.ID, waitQueued(t, r.work))

	cancel()
	waitClosed(t, done, "the trigger loop to return")
}

// The app owns the subscription, so a closed channel is how it says it is done.
// Beehive never closes one, and an unhandled close would spin the select.
func TestTriggerStopsOnAClosedChannel(t *testing.T) {
	r := newTriggerReconciler(t)

	ch := make(chan ObjectID)
	done := runTrigger(context.Background(), &trigger{r: r, ids: ch})

	close(ch)
	waitClosed(t, done, "the trigger loop to return on a closed channel")
}

// A failed read says so and drops the poke: a retry ladder here would compete
// with the kind's own cadence, which is the correctness behind every poke.
func TestTriggerLogsAFailedReadAndKeepsServing(t *testing.T) {
	r := newTriggerReconciler(t)
	obj := triggerObject(t, r, "one")

	logger, logs := captureLogger(slog.LevelWarn)
	r.logger = logger

	fail := true
	real := r.store
	r.store = &objectsOverrideStore{Store: real, override: objectsOverride{
		getMeta: func(ctx context.Context, id ObjectID) (*RawObject, error) {
			if fail {
				fail = false
				return nil, errors.New("boom")
			}
			return real.Objects().GetMeta(ctx, id)
		},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan ObjectID)
	done := runTrigger(ctx, &trigger{r: r, ids: ch})

	ch <- obj.ID
	ch <- obj.ID
	assert.Equal(t, obj.ID, waitQueued(t, r.work), "the loop serves the poke after the failure")
	assert.Contains(t, logs.String(), "trigger", "a failed read must name itself")

	cancel()
	waitClosed(t, done, "the trigger loop to return")
}

// The floor keeps a producer-driven read loop off the single connection, and
// what it holds back is coalesced rather than dropped: a trigger has no pull
// behind it, so a dropped poke is never re-derived. Three pokes for one address
// inside one window cost one read, and a distinct address joins the same drain.
func TestTriggerCoalescesInsideTheFloor(t *testing.T) {
	r := newTriggerReconciler(t)
	a := triggerObject(t, r, "a")
	b := triggerObject(t, r, "b")

	reads := make(chan ObjectID, 8)
	real := r.store
	r.store = &objectsOverrideStore{Store: real, override: objectsOverride{
		getMeta: func(ctx context.Context, id ObjectID) (*RawObject, error) {
			reads <- id
			return real.Objects().GetMeta(ctx, id)
		},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan ObjectID)
	done := runTrigger(ctx, &trigger{r: r, ids: ch, floor: 50 * time.Millisecond})

	// The first poke is eager: an idle feed pays no added latency.
	ch <- a.ID
	// These land inside that window. The three for a collapse to one read, and
	// b rides the same drain.
	ch <- a.ID
	ch <- a.ID
	ch <- a.ID
	ch <- b.ID

	got := []ObjectID{waitRead(t, reads), waitRead(t, reads), waitRead(t, reads)}
	slices.Sort(got)
	assert.Equal(t, []ObjectID{a.ID, a.ID, b.ID}, got,
		"the eager read, then one drain holding each address once")

	cancel()
	waitClosed(t, done, "the trigger loop to return")
}

// Start launches the declared feeds; parked leaves the trigger the only thing
// that can dispatch once the startup pass has settled the object.
func TestTriggerDispatchesAfterStart(t *testing.T) {
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store, parked()...)

	passes := make(chan ObjectID, 4)
	ctl := &funcController{fn: func(_ context.Context, _ ControllerClient[cStatus], obj *Object[cSpec, cStatus]) ReconcileResult {
		passes <- obj.ID
		return Settled()
	}}
	ch := make(chan string)
	registerWithClient(t, bh, clientTestGK, ctl, WithTriggerByName(ch))

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, "one", cSpec{})

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })

	require.Equal(t, obj.ID, waitRead(t, passes), "the startup pass settles it first")

	sendName(t, ch, "one")
	assert.Equal(t, obj.ID, waitRead(t, passes), "the trigger dispatched a settled object")
}

// The app owns the channel and beehive never closes it, so stop must not wait
// on a producer: an idle feed with nothing to say still drains promptly.
func TestTriggerDoesNotHoldUpStop(t *testing.T) {
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store, parked()...)

	ch := make(chan string)
	registerWithClient(t, bh, clientTestGK, &noopController[cSpec, cStatus]{}, WithTriggerByName(ch))

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	stop, err := bh.Start(ctx)
	require.NoError(t, err)

	require.NoError(t, stop(ctx))

	// The feed outlives the beehive that read it; a poke now reaches nobody.
	select {
	case ch <- "one":
		t.Fatal("a stopped beehive must not still be servicing the feed")
	default:
	}
}

// sendName pokes a running feed, or fails the test rather than blocking on a
// beehive that is not servicing it.
func sendName(t *testing.T, ch chan<- string, name string) {
	t.Helper()
	select {
	case ch <- name:
	case <-time.After(testTimeout):
		t.Fatalf("timed out sending %q: nothing is servicing the feed", name)
	}
}

// waitRead takes one recorded store read, or fails the test.
func waitRead(t *testing.T, reads <-chan ObjectID) ObjectID {
	t.Helper()
	select {
	case id := <-reads:
		return id
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for the trigger to read the store")
		return 0
	}
}

// A name the kind does not hold is the app's business, not an error: whether a
// record exists for an address changes under the producer. "" is the same
// branch — the ErrInvalidName check lives on Client, above the store.
func TestTriggerByNameIgnoresAnUnknownName(t *testing.T) {
	r := newTriggerReconciler(t)
	obj := triggerObject(t, r, "one")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan string)
	done := runTrigger(ctx, &trigger{r: r, names: ch})

	ch <- "no-such-name"
	ch <- ""
	// The known name behind them proves the loop survived both and that
	// neither queued anything: one signal, one id.
	ch <- "one"
	assert.Equal(t, obj.ID, waitQueued(t, r.work))
	_, ok := r.work.get()
	assert.False(t, ok, "an unresolvable name must queue nothing")

	cancel()
	waitClosed(t, done, "the trigger loop to return")
}
