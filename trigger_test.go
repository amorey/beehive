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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var triggerGK = GroupKind{Kind: "Trigger"}

// newTriggerReconciler builds a reconciler over a real store with nothing
// running but its work queue, so a test observes exactly what a trigger queued.
func newTriggerReconciler(t *testing.T) *reconciler {
	t.Helper()
	r := &reconciler{
		gk:               triggerGK,
		store:            newClientTestStore(t),
		work:             newWorkQueue(),
		maxRetryInterval: time.Minute,
		backoffFor:       make(map[ObjectID]time.Duration),
	}
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

// startTrigger runs trig for the rest of the test, ending it on cleanup. The
// returned channel closes when the loop has returned.
func startTrigger(t *testing.T, trig *trigger) <-chan struct{} {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		trig.run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		waitClosed(t, done, "the trigger loop to return")
	})
	return done
}

// waitQueued waits for the queue to signal and takes what it holds.
func waitQueued(t *testing.T, q *workQueue) ObjectID {
	t.Helper()
	waitClosed(t, q.ready, "the trigger to queue an object")
	id, ok := q.get()
	require.True(t, ok, "the queue signalled but held nothing")
	return id
}

func TestTriggerByIDRequeuesTheObject(t *testing.T) {
	r := newTriggerReconciler(t)
	obj := triggerObject(t, r, "one")

	ch := make(chan ObjectID)
	startTrigger(t, &trigger{r: r, ids: ch})

	ch <- obj.ID
	assert.Equal(t, obj.ID, waitQueued(t, r.work))
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

	ch := make(chan ObjectID)
	startTrigger(t, &trigger{r: r, ids: ch})

	ch <- foreign.ID
	ch <- ObjectID(9999) // and one that names nothing at all
	ch <- mine.ID
	assert.Equal(t, mine.ID, waitQueued(t, r.work))
	_, ok := r.work.get()
	assert.False(t, ok, "only this kind's id may be queued")
}

func TestTriggerByNameRequeuesTheObject(t *testing.T) {
	r := newTriggerReconciler(t)
	obj := triggerObject(t, r, "one")

	ch := make(chan string)
	startTrigger(t, &trigger{r: r, names: ch})

	ch <- "one"
	assert.Equal(t, obj.ID, waitQueued(t, r.work))
}

// The app owns the subscription, so a closed channel is how it says it is done.
// Beehive never closes one, and an unhandled close would spin the select.
func TestTriggerStopsOnAClosedChannel(t *testing.T) {
	ids := make(chan ObjectID)
	names := make(chan string)
	for _, tc := range []struct {
		name  string
		trig  func(*reconciler) *trigger
		close func()
	}{
		{"by id", func(r *reconciler) *trigger { return &trigger{r: r, ids: ids} }, func() { close(ids) }},
		{"by name", func(r *reconciler) *trigger { return &trigger{r: r, names: names} }, func() { close(names) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			done := startTrigger(t, tc.trig(newTriggerReconciler(t)))
			tc.close()
			waitClosed(t, done, "the trigger loop to return on a closed channel")
		})
	}
}

// A close ends the receives, not the addresses already taken off the channel.
// The floor holds a poke back on the promise that it is coalesced rather than
// dropped, and nothing re-derives one, so the last window has to drain.
func TestTriggerDrainsWhatTheFloorHeldWhenTheChannelCloses(t *testing.T) {
	for _, tc := range []struct {
		name string
		poke func(t *testing.T, r *reconciler, eager, held *RawObject)
	}{
		{"by id", func(t *testing.T, r *reconciler, eager, held *RawObject) {
			ch := make(chan ObjectID)
			startTrigger(t, &trigger{r: r, ids: ch, floor: 200 * time.Millisecond})
			ch <- eager.ID
			ch <- held.ID
			close(ch)
		}},
		{"by name", func(t *testing.T, r *reconciler, eager, held *RawObject) {
			ch := make(chan string)
			startTrigger(t, &trigger{r: r, names: ch, floor: 200 * time.Millisecond})
			ch <- eager.Name
			ch <- held.Name
			close(ch)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newTriggerReconciler(t)
			eager := triggerObject(t, r, "eager")
			held := triggerObject(t, r, "held")

			// The first poke admits the gate, so the second is inside the window.
			tc.poke(t, r, eager, held)

			assert.Equal(t, eager.ID, waitQueued(t, r.work), "the eager poke")
			assert.Equal(t, held.ID, waitQueued(t, r.work), "the poke the floor was holding")
		})
	}
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

	ch := make(chan ObjectID)
	startTrigger(t, &trigger{r: r, ids: ch})

	ch <- obj.ID
	ch <- obj.ID
	assert.Equal(t, obj.ID, waitQueued(t, r.work), "the loop serves the poke after the failure")
	assert.Contains(t, logs.String(), "trigger", "a failed read must name itself")
}

// A poke is an address, not a claim that a failure is over, so it leaves the
// retry ladder where a plain Client.Requeue would leave it.
func TestTriggerPreservesTheBackoffLadder(t *testing.T) {
	r := newTriggerReconciler(t)
	obj := triggerObject(t, r, "one")
	ladder := r.backoffNext(obj.ID)

	ch := make(chan ObjectID)
	startTrigger(t, &trigger{r: r, ids: ch})

	ch <- obj.ID
	require.Equal(t, obj.ID, waitQueued(t, r.work))

	r.backoffMu.Lock()
	defer r.backoffMu.Unlock()
	assert.Equal(t, ladder, r.backoffFor[obj.ID], "a trigger must not reset the ladder")
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

	ch := make(chan ObjectID)
	startTrigger(t, &trigger{r: r, ids: ch, floor: 200 * time.Millisecond})

	// The first poke is eager: an idle feed pays no added latency.
	ch <- a.ID
	// These land inside that window. The three for a collapse to one read, and
	// b rides the same drain.
	ch <- a.ID
	ch <- a.ID
	ch <- a.ID
	ch <- b.ID

	got := []ObjectID{recv(t, reads), recv(t, reads), recv(t, reads)}
	slices.Sort(got)
	assert.Equal(t, []ObjectID{a.ID, a.ID, b.ID}, got,
		"the eager read, then one drain holding each address once")
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

	require.Equal(t, obj.ID, recv(t, passes), "the startup pass settles it first")

	sendName(t, ch, "one")
	assert.Equal(t, obj.ID, recv(t, passes), "the trigger dispatched a settled object")
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

// A name the kind does not hold is the app's business, not an error: whether a
// record exists for an address changes under the producer. "" is the same
// branch — the ErrInvalidName check lives on Client, above the store.
func TestTriggerByNameIgnoresAnUnknownName(t *testing.T) {
	r := newTriggerReconciler(t)
	obj := triggerObject(t, r, "one")

	ch := make(chan string)
	startTrigger(t, &trigger{r: r, names: ch})

	ch <- "no-such-name"
	ch <- ""
	// The known name behind them proves the loop survived both and that
	// neither queued anything: one signal, one id.
	ch <- "one"
	assert.Equal(t, obj.ID, waitQueued(t, r.work))
	_, ok := r.work.get()
	assert.False(t, ok, "an unresolvable name must queue nothing")
}
