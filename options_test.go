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
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithFullPassIntervalDispatch(t *testing.T) {
	bh := &Beehive{}
	require.NoError(t, WithFullPassInterval(5*time.Second)(bh))
	assert.Equal(t, 5*time.Second, bh.fullPassInterval)

	r := &reconciler{}
	require.NoError(t, WithFullPassInterval(3*time.Second)(r))
	assert.Equal(t, 3*time.Second, r.fullPassInterval)

	// A target the option doesn't recognize is silently ignored.
	require.NoError(t, WithFullPassInterval(time.Second)("unrelated"))
}

// resolveEvents folds the per-call EventOptions into one EventQuery; the empty
// set is the zero query (every run for the object).
func TestResolveEvents(t *testing.T) {
	since := time.Now()
	q := resolveEvents([]EventOption{
		WithEventCategory("connection"),
		WithEventType(EventWarning),
		WithEventReason("ProbeFailed"),
		WithEventLimit(5),
		WithEventsSince(since),
	})
	require.NotNil(t, q.Category)
	assert.Equal(t, "connection", *q.Category)
	assert.Equal(t, "Warning", q.Type)
	assert.Equal(t, "ProbeFailed", q.Reason)
	assert.Equal(t, 5, q.Limit)
	assert.Equal(t, since, q.Since)

	empty := resolveEvents(nil)
	assert.Nil(t, empty.Category, "no category filter unless requested")
	assert.Zero(t, empty.Limit)
}

func TestWithEventRetentionDispatch(t *testing.T) {
	bh := &Beehive{}
	require.NoError(t, WithEventRetention(50, time.Hour)(bh))
	assert.Equal(t, 50, bh.eventRetentionPerObject)
	assert.Equal(t, time.Hour, bh.eventRetentionMaxAge)

	// Retention is global (Beehive-level); other targets ignore it.
	require.NoError(t, WithEventRetention(9, time.Minute)(&reconciler{}))
	require.NoError(t, WithEventRetention(9, time.Minute)("unrelated"))
}

func TestWithMaxRetryIntervalDispatch(t *testing.T) {
	r := &reconciler{}
	require.NoError(t, WithMaxRetryInterval(9*time.Second)(r))
	assert.Equal(t, 9*time.Second, r.maxRetryInterval)

	// Only reconcilers carry a max retry interval; on a Beehive it's a no-op.
	require.NoError(t, WithMaxRetryInterval(9*time.Second)(&Beehive{}))

	// A non-positive cap is ignored so it can't busy-loop the reconciler; the
	// existing value (here the default) is left untouched.
	r = &reconciler{maxRetryInterval: defaultMaxRetryInterval}
	require.NoError(t, WithMaxRetryInterval(0)(r))
	assert.Equal(t, defaultMaxRetryInterval, r.maxRetryInterval)
	require.NoError(t, WithMaxRetryInterval(-1*time.Second)(r))
	assert.Equal(t, defaultMaxRetryInterval, r.maxRetryInterval)
}

func TestWithConcurrencyDispatch(t *testing.T) {
	bh := &Beehive{}
	require.NoError(t, WithConcurrency(4)(bh))
	assert.Equal(t, 4, bh.concurrency)

	r := &reconciler{}
	require.NoError(t, WithConcurrency(2)(r))
	assert.Equal(t, 2, r.concurrency)

	// A target the option doesn't recognize is silently ignored.
	require.NoError(t, WithConcurrency(1)("unrelated"))
}

func TestWithStartupFullPassDispatch(t *testing.T) {
	bh := &Beehive{startupFullPass: true}
	require.NoError(t, WithStartupFullPass(false)(bh))
	assert.False(t, bh.startupFullPass)

	r := &reconciler{}
	require.NoError(t, WithStartupFullPass(true)(r))
	assert.True(t, r.startupFullPass)

	// A target the option doesn't recognize is silently ignored.
	require.NoError(t, WithStartupFullPass(true)("unrelated"))
}

func TestOwedPassIntervalOptionDispatch(t *testing.T) {
	bh := &Beehive{}
	require.NoError(t, withOwedPassInterval(5*time.Second)(bh))
	assert.Equal(t, 5*time.Second, bh.owedPassInterval)

	r := &reconciler{}
	require.NoError(t, withOwedPassInterval(time.Minute)(r))
	assert.Equal(t, time.Minute, r.owedPassInterval)

	// A target the option doesn't recognize is silently ignored.
	require.NoError(t, withOwedPassInterval(time.Second)("unrelated"))
}

func TestWithGCIntervalDispatch(t *testing.T) {
	bh := &Beehive{}
	require.NoError(t, WithGCInterval(5*time.Second)(bh))
	assert.Equal(t, 5*time.Second, bh.gcInterval)

	// GC is global: a reconciler is not a target it recognizes, and neither is
	// anything else.
	r := &reconciler{}
	require.NoError(t, WithGCInterval(time.Minute)(r))
	require.NoError(t, WithGCInterval(time.Second)("unrelated"))
}

// TestWithGCIntervalRejectsNonPositive pins the one interval that cannot be turned
// off. The reconcile knobs accept 0 as "off" because Client.Requeue still drives a
// pass by hand, but nothing public triggers collect — so a sweeper-less Beehive
// accumulates deletion-pending rows with no recourse, each RESTRICT-blocking its
// owner's delete. Every swallowed error inside a sweep also assumes a next tick to
// retry on, which is only true while this holds.
func TestWithGCIntervalRejectsNonPositive(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		t.Run(d.String(), func(t *testing.T) {
			bh := &Beehive{gcInterval: time.Minute}
			err := WithGCInterval(d)(bh)
			require.ErrorIs(t, err, ErrInvalidOption)
			assert.Contains(t, err.Error(), "WithGCInterval", "name the option that was misused")
			assert.Equal(t, time.Minute, bh.gcInterval, "a rejected option must not have written")

			// Rejected wherever it is aimed: the value is nonsense independent of target,
			// so a misdirected call must not carry the mistake silently.
			require.ErrorIs(t, WithGCInterval(d)(&reconciler{}), ErrInvalidOption)
			require.ErrorIs(t, WithGCInterval(d)("unrelated"), ErrInvalidOption)

			// And it surfaces from New, which is where a real caller meets it.
			_, err = New(&fakeStore{}, WithGCInterval(d))
			require.ErrorIs(t, err, ErrInvalidOption)
		})
	}
}

// TestWatchPollIntervalRejectsNonPositive pins the other mandatory interval,
// which is mandatory for a different reason than GC's. The watch poll is not a
// backstop but the delivery mechanism itself, so a watch that never polls is a
// stream that never emits — there is nothing such a value could mean.
func TestWatchPollIntervalRejectsNonPositive(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		t.Run(d.String(), func(t *testing.T) {
			bh := &Beehive{watchPollInterval: time.Minute}
			err := withWatchPollInterval(d)(bh)
			require.ErrorIs(t, err, ErrInvalidOption)
			assert.Contains(t, err.Error(), "withWatchPollInterval", "name the option that was misused")
			assert.Equal(t, time.Minute, bh.watchPollInterval, "a rejected option must not have written")

			// Checked before the target switch, like WithGCInterval: a value that means
			// nothing at one call site means nothing at any of them.
			require.ErrorIs(t, withWatchPollInterval(d)(&reconciler{}), ErrInvalidOption)
			require.ErrorIs(t, withWatchPollInterval(d)("unrelated"), ErrInvalidOption)

			_, err = New(&fakeStore{}, withWatchPollInterval(d))
			require.ErrorIs(t, err, ErrInvalidOption)
		})
	}
}

// TestWatchFloorIntervalRejectsNonPositive pins that the floor cannot be
// disabled. A disabled floor would still deliver this process's own writes —
// the wake covers those — but silently drop what only the floor covers: a
// second writer over the store, a failed step, a retention trim.
func TestWatchFloorIntervalRejectsNonPositive(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		t.Run(d.String(), func(t *testing.T) {
			bh := &Beehive{watchFloorInterval: time.Minute}
			err := withWatchFloorInterval(d)(bh)
			require.ErrorIs(t, err, ErrInvalidOption)
			assert.Contains(t, err.Error(), "withWatchFloorInterval", "name the option that was misused")
			assert.Equal(t, time.Minute, bh.watchFloorInterval, "a rejected option must not have written")

			// Checked before the target switch, like the two above it.
			require.ErrorIs(t, withWatchFloorInterval(d)(&reconciler{}), ErrInvalidOption)
			require.ErrorIs(t, withWatchFloorInterval(d)("unrelated"), ErrInvalidOption)

			_, err = New(&fakeStore{}, withWatchFloorInterval(d))
			require.ErrorIs(t, err, ErrInvalidOption)
		})
	}
}

// TestStaleDependentsIntervalRejectsNonPositive pins the third mandatory
// interval, and the one with the strongest claim to be mandatory: the
// stale-dependents pass is what makes a dependency wake a guarantee rather than a
// best effort, so nothing else re-derives an owed wake if it never runs. "Rarely"
// is expressible, "never" is not.
func TestStaleDependentsIntervalRejectsNonPositive(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		t.Run(d.String(), func(t *testing.T) {
			bh := &Beehive{staleDependentsInterval: time.Minute}
			err := withStaleDependentsInterval(d)(bh)
			require.ErrorIs(t, err, ErrInvalidOption)
			assert.Contains(t, err.Error(), "withStaleDependentsInterval", "name the option that was misused")
			assert.Equal(t, time.Minute, bh.staleDependentsInterval, "a rejected option must not have written")

			// Checked before the target switch, like the two above it.
			require.ErrorIs(t, withStaleDependentsInterval(d)(&reconciler{}), ErrInvalidOption)
			require.ErrorIs(t, withStaleDependentsInterval(d)("unrelated"), ErrInvalidOption)

			_, err = New(&fakeStore{}, withStaleDependentsInterval(d))
			require.ErrorIs(t, err, ErrInvalidOption)
		})
	}
}

func TestWithLoggerDispatch(t *testing.T) {
	l := slog.New(slog.DiscardHandler)

	bh := &Beehive{}
	require.NoError(t, WithLogger(l)(bh))
	assert.Same(t, l, bh.logger)

	r := &reconciler{}
	require.NoError(t, WithLogger(l)(r))
	assert.Same(t, l, r.logger)

	// A nil logger is a valid value (disables logging) and a target the option
	// doesn't recognize is silently ignored.
	require.NoError(t, WithLogger(nil)(bh))
	assert.Nil(t, bh.logger)
	require.NoError(t, WithLogger(l)("unrelated"))
}

func TestWithLogLevelDispatch(t *testing.T) {
	bh := &Beehive{}
	require.NoError(t, WithLogLevel(slog.LevelWarn)(bh))
	assert.Equal(t, slog.LevelWarn, bh.logLevel)

	r := &reconciler{}
	require.NoError(t, WithLogLevel(slog.LevelError)(r))
	assert.Equal(t, slog.LevelError, r.logLevel)

	// A target the option doesn't recognize is silently ignored.
	require.NoError(t, WithLogLevel(slog.LevelInfo)("unrelated"))
}

// The create-time metadata options apply to a *createOptions target and are
// inert on anything else (so they're harmless if passed to New/Register).
func TestCreateOptionsDispatch(t *testing.T) {
	co := &createOptions{}
	require.NoError(t, WithFinalizers("a", "b")(co))
	require.NoError(t, WithOwner(42)(co))

	assert.Equal(t, []string{"a", "b"}, co.finalizers)
	require.NotNil(t, co.owner)
	assert.Equal(t, ObjectID(42), *co.owner)

	// A target the options don't recognize is silently ignored.
	for _, o := range []Option{WithFinalizers("a"), WithOwner(7)} {
		require.NoError(t, o(&Beehive{}))
	}
}

// resolveLoads ORs the selected LoadOptions into a single LoadSet.
func TestResolveLoads(t *testing.T) {
	// No options -> nothing loaded.
	assert.Equal(t, LoadSet(0), resolveLoads(nil))

	// Options OR together.
	assert.Equal(t, LoadOwnerBit|LoadDependenciesBit,
		resolveLoads([]LoadOption{LoadOwner(), LoadDependencies()}))

	// A repeated selector is idempotent.
	assert.Equal(t, LoadOwnerBit, resolveLoads([]LoadOption{LoadOwner(), LoadOwner()}))
}

func TestWithWriteLogRetentionDispatch(t *testing.T) {
	bh := &Beehive{}
	require.NoError(t, WithWriteLogRetention(50, time.Hour)(bh))
	assert.Equal(t, 50, bh.writeLogRetentionPerKind)
	assert.Equal(t, time.Hour, bh.writeLogRetentionMaxAge)

	// Retention is global (Beehive-level); other targets ignore it.
	require.NoError(t, WithWriteLogRetention(9, time.Minute)(&reconciler{}))
	require.NoError(t, WithWriteLogRetention(9, time.Minute)("unrelated"))
}

func TestWithMinRequeueIntervalDispatch(t *testing.T) {
	bh := &Beehive{}
	require.NoError(t, withMinRequeueInterval(time.Minute)(bh))
	assert.Equal(t, time.Minute, bh.minRequeueInterval)

	r := &reconciler{work: newWorkQueue()}
	require.NoError(t, withMinRequeueInterval(time.Second)(r))
	r.work.gate.Admit(1, time.Now())
	_, held := r.work.gate.OpensAt(1, time.Now())
	assert.True(t, held, "the option must reach the queue's gate")

	// A target the option doesn't recognize is silently ignored.
	require.NoError(t, withMinRequeueInterval(time.Second)("unrelated"))
}

// Register seeds the queue's floor from the New-level default, and a per-kind
// option overrides it.
func TestRegisterBuildsTheQueuesGateFromTheResolvedInterval(t *testing.T) {
	bh := newTestBeehive(t, newClientTestStore(t), withMinRequeueInterval(time.Hour))
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{}, withMinRequeueInterval(time.Minute))
	require.NoError(t, err)

	r, ok := bh.reconcilerFor(clientTestGK)
	require.True(t, ok)

	r.work.gate.Admit(1, time.Now())
	opensAt, held := r.work.gate.OpensAt(1, time.Now())
	require.True(t, held, "the queue's gate must carry the resolved interval")
	assert.True(t, opensAt.Before(time.Now().Add(2*time.Minute)), "got the New-level interval, not Register's")
}
