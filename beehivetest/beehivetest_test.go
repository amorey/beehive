package beehivetest_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/beehive/beehivetest"
	"github.com/amorey/beehive/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type clusterSpec struct {
	Region string
}

type serverStatus struct {
	UID string
}

type clusterStatus struct {
	Server serverStatus
}

var clusterGK = beehive.GroupKind{Group: "kstack", Kind: "Cluster"}

// newBeehive returns an unstarted beehive over a fresh in-memory store.
func newBeehive(t *testing.T, opts ...beehive.Option) *beehive.Beehive {
	t.Helper()
	store, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	bh, err := beehive.New(store, opts...)
	require.NoError(t, err)
	return bh
}

func TestUpdateStatusRoundTrips(t *testing.T) {
	ctx := context.Background()
	bh := newBeehive(t)
	objects := beehive.NewClient[clusterSpec, clusterStatus](bh, clusterGK)

	obj, err := objects.Create(ctx, "prod", clusterSpec{Region: "us-east-1"})
	require.NoError(t, err)

	c := beehivetest.NewClient[clusterStatus](bh, clusterGK)
	require.NoError(t, c.UpdateStatus(ctx, obj.ID, clusterStatus{Server: serverStatus{UID: "server-1"}}))

	got, err := objects.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status)
	assert.Equal(t, "server-1", got.Status.Server.UID)

	// The handshake stays beehive's: a fixture status leaves the object unsettled.
	assert.Nil(t, got.ObservedGeneration)
	assert.Equal(t, obj.Generation, got.Generation)
}

// stubController registers a kind without reconciling anything.
type stubController struct{}

func (stubController) Reconcile(context.Context, beehive.ControllerClient[clusterStatus], *beehive.Object[clusterSpec, clusterStatus]) beehive.ReconcileResult {
	return beehive.Settled()
}

// statusV2Migrator reports status version 2 and marks whatever it converts, so
// a test can see whether a row was tagged at the current version or below it.
type statusV2Migrator struct{}

func (statusV2Migrator) SchemaVersionSpec() int   { return 0 }
func (statusV2Migrator) SchemaVersionStatus() int { return 2 }

func (statusV2Migrator) ConvertSpec(_ int, raw json.RawMessage) (json.RawMessage, error) {
	return raw, nil
}

func (statusV2Migrator) ConvertStatus(_ int, _ json.RawMessage) (json.RawMessage, error) {
	return json.RawMessage(`{"Server":{"UID":"converted"}}`), nil
}

func TestUpdateStatusStampsTheMigratorsVersion(t *testing.T) {
	ctx := context.Background()
	bh := newBeehive(t)
	require.NoError(t, beehive.Register[clusterSpec, clusterStatus](
		bh, clusterGK, stubController{}, beehive.WithMigrator(statusV2Migrator{})))
	objects := beehive.NewClient[clusterSpec, clusterStatus](bh, clusterGK)

	obj, err := objects.Create(ctx, "prod", clusterSpec{Region: "us-east-1"})
	require.NoError(t, err)

	c := beehivetest.NewClient[clusterStatus](bh, clusterGK)
	require.NoError(t, c.UpdateStatus(ctx, obj.ID, clusterStatus{Server: serverStatus{UID: "server-1"}}))

	// A row tagged at the migrator's version is not converted on read. Created
	// rows sit at 0, so a write that fails to stamp leaves ConvertStatus to run.
	got, err := objects.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status)
	assert.Equal(t, "server-1", got.Status.Server.UID)
}

func TestUpdateStatusWakesTheWatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// No controller: a reconcile pass stamping the generation would wake the
	// tailer itself, and this test is about the fixture write's own wake.
	bh := newBeehive(t)
	objects := beehive.NewClient[clusterSpec, clusterStatus](bh, clusterGK)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })

	obj, err := objects.Create(ctx, "prod", clusterSpec{Region: "us-east-1"})
	require.NoError(t, err)

	stream, err := objects.WatchList(ctx)
	require.NoError(t, err)

	c := beehivetest.NewClient[clusterStatus](bh, clusterGK)
	require.NoError(t, c.UpdateStatus(ctx, obj.ID, clusterStatus{Server: serverStatus{UID: "server-1"}}))

	// The watch floor is 30s, so without a commit wake this times out rather
	// than arriving late.
	for {
		select {
		case change, ok := <-stream.Changes:
			require.True(t, ok, "stream closed: %v", stream.Err())
			if change.Object != nil && change.Object.Status != nil {
				assert.Equal(t, "server-1", change.Object.Status.Server.UID)
				return
			}
		case <-ctx.Done():
			t.Fatal("no change delivered: the write did not wake the tailer")
		}
	}
}

func TestSetConditions(t *testing.T) {
	ctx := context.Background()
	bh := newBeehive(t)
	objects := beehive.NewClient[clusterSpec, clusterStatus](bh, clusterGK)
	c := beehivetest.NewClient[clusterStatus](bh, clusterGK)

	obj, err := objects.Create(ctx, "prod", clusterSpec{Region: "us-east-1"})
	require.NoError(t, err)

	t.Run("a condition round-trips with the store's stamps", func(t *testing.T) {
		require.NoError(t, c.SetCondition(ctx, obj.ID, beehive.Condition{
			Type:    "Ready",
			Status:  beehive.ConditionTrue,
			Reason:  "Probed",
			Message: "the probe answered",
		}))

		got, err := objects.Get(ctx, obj.ID)
		require.NoError(t, err)
		require.Len(t, got.Conditions, 1)
		cond := got.Conditions[0]
		assert.Equal(t, "Ready", cond.Type)
		assert.Equal(t, beehive.ConditionTrue, cond.Status)
		assert.Equal(t, "Probed", cond.Reason)
		assert.False(t, cond.Unconfirmed)
		assert.False(t, cond.UpdatedAt.IsZero(), "the store stamps on write")
	})

	t.Run("a type named twice is refused", func(t *testing.T) {
		err := c.SetConditions(ctx, obj.ID, []beehive.Condition{
			{Type: "Dup", Status: beehive.ConditionTrue},
			{Type: "Dup", Status: beehive.ConditionFalse},
		})
		assert.ErrorIs(t, err, beehive.ErrDuplicateConditionType)
	})

	t.Run("no conditions writes nothing", func(t *testing.T) {
		before, err := objects.Get(ctx, obj.ID)
		require.NoError(t, err)
		require.NoError(t, c.SetConditions(ctx, obj.ID, nil))
		after, err := objects.Get(ctx, obj.ID)
		require.NoError(t, err)
		assert.Equal(t, before.ResourceVersion, after.ResourceVersion)
	})
}

func TestDeleteCondition(t *testing.T) {
	ctx := context.Background()
	bh := newBeehive(t)
	objects := beehive.NewClient[clusterSpec, clusterStatus](bh, clusterGK)
	c := beehivetest.NewClient[clusterStatus](bh, clusterGK)

	obj, err := objects.Create(ctx, "prod", clusterSpec{Region: "us-east-1"})
	require.NoError(t, err)
	require.NoError(t, c.SetCondition(ctx, obj.ID, beehive.Condition{
		Type: "Ready", Status: beehive.ConditionTrue,
	}))

	require.NoError(t, c.DeleteCondition(ctx, obj.ID, "Ready"))
	got, err := objects.Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Empty(t, got.Conditions)

	t.Run("a missing condition is a no-op", func(t *testing.T) {
		before, err := objects.Get(ctx, obj.ID)
		require.NoError(t, err)
		require.NoError(t, c.DeleteCondition(ctx, obj.ID, "NeverSet"))
		after, err := objects.Get(ctx, obj.ID)
		require.NoError(t, err)
		assert.Equal(t, before.ResourceVersion, after.ResourceVersion)
	})
}
