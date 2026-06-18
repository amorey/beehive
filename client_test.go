package beehive_test

import (
	"context"
	"testing"

	"github.com/amorey/beehive"
	"github.com/amorey/beehive/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type cSpec   struct{ Val string }
type cStatus struct{ Val string }

var clientTestGK = beehive.GroupKind{Kind: "Widget"}

func newClientTestStore(t *testing.T) beehive.Store {
	t.Helper()
	s, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

func TestClientCreate(t *testing.T) {
	ctx := context.Background()
	bh, err := beehive.New(newClientTestStore(t))
	require.NoError(t, err)

	client := beehive.NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)
	assert.NotZero(t, obj.ID)
	assert.Equal(t, clientTestGK.Group, obj.Group)
	assert.Equal(t, clientTestGK.Kind, obj.Kind)
	assert.Equal(t, int64(1), obj.Generation)
	assert.Nil(t, obj.Status)
	assert.Equal(t, "hello", obj.Spec.Val)
}

func TestClientGet(t *testing.T) {
	ctx := context.Background()
	bh, err := beehive.New(newClientTestStore(t))
	require.NoError(t, err)

	client := beehive.NewClient[cSpec, cStatus](bh, clientTestGK)
	created, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)

	got, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, created.ID, got.ID)
	assert.Equal(t, "hello", got.Spec.Val)
	assert.Nil(t, got.Status)
}

func TestClientGetByName(t *testing.T) {
	ctx := context.Background()
	bh, err := beehive.New(newClientTestStore(t))
	require.NoError(t, err)

	client := beehive.NewClient[cSpec, cStatus](bh, clientTestGK)
	_, err = client.GetByName(ctx, "nonexistent")
	require.ErrorIs(t, err, beehive.ErrNotFound)
}

func TestClientList(t *testing.T) {
	ctx := context.Background()
	bh, err := beehive.New(newClientTestStore(t))
	require.NoError(t, err)

	client := beehive.NewClient[cSpec, cStatus](bh, clientTestGK)
	a, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)
	b, err := client.Create(ctx, cSpec{Val: "b"})
	require.NoError(t, err)

	list, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, list, 2)
	assert.Equal(t, a.ID, list[0].ID)
	assert.Equal(t, b.ID, list[1].ID)
}

func TestClientUpdate(t *testing.T) {
	ctx := context.Background()
	bh, err := beehive.New(newClientTestStore(t))
	require.NoError(t, err)

	client := beehive.NewClient[cSpec, cStatus](bh, clientTestGK)
	created, err := client.Create(ctx, cSpec{Val: "v1"})
	require.NoError(t, err)

	updated, err := client.Update(ctx, created.ID, cSpec{Val: "v2"})
	require.NoError(t, err)
	assert.Equal(t, created.ID, updated.ID)
	assert.Equal(t, int64(2), updated.Generation)
	assert.Equal(t, "v2", updated.Spec.Val)
}

func TestClientDelete(t *testing.T) {
	ctx := context.Background()
	bh, err := beehive.New(newClientTestStore(t))
	require.NoError(t, err)

	client := beehive.NewClient[cSpec, cStatus](bh, clientTestGK)
	created, err := client.Create(ctx, cSpec{})
	require.NoError(t, err)

	err = client.Delete(ctx, created.ID)
	require.NoError(t, err)

	// object still present (no finalizers cleared), but marked for deletion
	got, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	assert.NotNil(t, got.DeletionRequestedAt)
}
