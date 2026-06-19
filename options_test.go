package beehive

import (
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

func TestWithMaxRetryIntervalDispatch(t *testing.T) {
	r := &reconciler{}
	require.NoError(t, WithMaxRetryInterval(9*time.Second)(r))
	assert.Equal(t, 9*time.Second, r.maxRetryInterval)

	// Only reconcilers carry a max retry interval; on a Beehive it's a no-op.
	require.NoError(t, WithMaxRetryInterval(9*time.Second)(&Beehive{}))
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

// The metadata options are wired into the API but not yet implemented; they
// should accept any target without erroring.
func TestStubOptionsAreInert(t *testing.T) {
	for _, o := range []Option{WithName("x"), WithFinalizers("a", "b"), WithOwner(42)} {
		require.NoError(t, o(&Beehive{}))
	}
}
