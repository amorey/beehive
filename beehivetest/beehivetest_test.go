package beehivetest_test

import (
	"context"
	"testing"

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
