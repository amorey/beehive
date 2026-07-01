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

func TestWithResyncIntervalDispatch(t *testing.T) {
	bh := &Beehive{}
	require.NoError(t, WithResyncInterval(5*time.Second)(bh))
	assert.Equal(t, 5*time.Second, bh.resyncInterval)

	r := &reconciler{}
	require.NoError(t, WithResyncInterval(3*time.Second)(r))
	assert.Equal(t, 3*time.Second, r.resyncInterval)

	// A target the option doesn't recognize is silently ignored.
	require.NoError(t, WithResyncInterval(time.Second)("unrelated"))
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

func TestWithStartupReconcileStrategyDispatch(t *testing.T) {
	bh := &Beehive{}
	require.NoError(t, WithStartupReconcileStrategy(StartupReconcileUnsettled)(bh))
	assert.Equal(t, StartupReconcileUnsettled, bh.startupReconcile)

	r := &reconciler{}
	require.NoError(t, WithStartupReconcileStrategy(StartupReconcileNone)(r))
	assert.Equal(t, StartupReconcileNone, r.startupReconcile)

	// A target the option doesn't recognize is silently ignored.
	require.NoError(t, WithStartupReconcileStrategy(StartupReconcileAll)("unrelated"))
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
	require.NoError(t, WithSlug("widget")(co))
	require.NoError(t, WithFinalizers("a", "b")(co))
	require.NoError(t, WithOwner(42)(co))

	require.NotNil(t, co.slug)
	assert.Equal(t, "widget", *co.slug)
	assert.Equal(t, []string{"a", "b"}, co.finalizers)
	require.NotNil(t, co.owner)
	assert.Equal(t, ObjectID(42), *co.owner)

	// A target the options don't recognize is silently ignored.
	for _, o := range []Option{WithSlug("x"), WithFinalizers("a"), WithOwner(7)} {
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
