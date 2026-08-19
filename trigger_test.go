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
