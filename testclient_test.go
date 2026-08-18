package beehive

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type tcSpec struct {
	Region string
}

type tcServerStatus struct {
	UID string
}

type tcStatus struct {
	Server tcServerStatus
}

var tcGK = GroupKind{Group: "kstack", Kind: "Cluster"}

// newTestClientFixture is the shared preamble: an unstarted beehive, one stored
// object, and both clients over it.
func newTestClientFixture(t *testing.T) (context.Context, Client[tcSpec, tcStatus], *TestClient[tcStatus], *Object[tcSpec, tcStatus]) {
	t.Helper()
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	objects := NewClient[tcSpec, tcStatus](bh, tcGK)
	obj, err := objects.Create(ctx, "prod", tcSpec{Region: "us-east-1"})
	require.NoError(t, err)
	return ctx, objects, NewTestClient[tcStatus](bh, tcGK), obj
}

func TestTestClientUpdateStatusRoundTrips(t *testing.T) {
	ctx, objects, c, obj := newTestClientFixture(t)
	require.NoError(t, c.UpdateStatus(ctx, obj.ID, tcStatus{Server: tcServerStatus{UID: "server-1"}}))

	got, err := objects.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status)
	assert.Equal(t, "server-1", got.Status.Server.UID)

	// The handshake stays beehive's: a fixture status leaves the object unsettled.
	assert.Nil(t, got.ObservedGeneration)
	assert.Equal(t, obj.Generation, got.Generation)
}

// tcStatusV2Migrator reports status version 2 and marks whatever it converts, so
// a test can see whether a row was tagged at the current version or below it.
type tcStatusV2Migrator struct{}

func (tcStatusV2Migrator) SchemaVersionSpec() int   { return 0 }
func (tcStatusV2Migrator) SchemaVersionStatus() int { return 2 }

func (tcStatusV2Migrator) ConvertSpec(_ int, raw json.RawMessage) (json.RawMessage, error) {
	return raw, nil
}

func (tcStatusV2Migrator) ConvertStatus(_ int, _ json.RawMessage) (json.RawMessage, error) {
	return json.RawMessage(`{"Server":{"UID":"converted"}}`), nil
}

type tcStubController struct{}

func (tcStubController) Reconcile(context.Context, ControllerClient[tcStatus], *Object[tcSpec, tcStatus]) ReconcileResult {
	return Settled()
}

func TestTestClientStampsTheMigratorsVersion(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	require.NoError(t, Register[tcSpec, tcStatus](bh, tcGK, tcStubController{}, WithMigrator(tcStatusV2Migrator{})))
	objects := NewClient[tcSpec, tcStatus](bh, tcGK)

	obj, err := objects.Create(ctx, "prod", tcSpec{Region: "us-east-1"})
	require.NoError(t, err)

	c := NewTestClient[tcStatus](bh, tcGK)
	require.NoError(t, c.UpdateStatus(ctx, obj.ID, tcStatus{Server: tcServerStatus{UID: "server-1"}}))

	// A row tagged at the migrator's version is not converted on read. Created
	// rows sit at 0, so a write that fails to stamp leaves ConvertStatus to run.
	got, err := objects.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Status)
	assert.Equal(t, "server-1", got.Status.Server.UID)
}

func TestTestClientWakesTheWatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// No controller: a reconcile pass stamping the generation would wake the
	// tailer itself, and this test is about the fixture write's own wake.
	bh := newTestBeehive(t, newClientTestStore(t))
	objects := NewClient[tcSpec, tcStatus](bh, tcGK)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })

	obj, err := objects.Create(ctx, "prod", tcSpec{Region: "us-east-1"})
	require.NoError(t, err)

	stream, err := objects.WatchList(ctx)
	require.NoError(t, err)

	c := NewTestClient[tcStatus](bh, tcGK)
	require.NoError(t, c.UpdateStatus(ctx, obj.ID, tcStatus{Server: tcServerStatus{UID: "server-1"}}))

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

func TestTestClientSetConditions(t *testing.T) {
	ctx, objects, c, obj := newTestClientFixture(t)

	t.Run("a condition round-trips with the store's stamps", func(t *testing.T) {
		require.NoError(t, c.SetCondition(ctx, obj.ID, Condition{
			Type:    "Ready",
			Status:  ConditionTrue,
			Reason:  "Probed",
			Message: "the probe answered",
		}))

		got, err := objects.Get(ctx, obj.ID)
		require.NoError(t, err)
		require.Len(t, got.Conditions, 1)
		cond := got.Conditions[0]
		assert.Equal(t, "Ready", cond.Type)
		assert.Equal(t, ConditionTrue, cond.Status)
		assert.Equal(t, "Probed", cond.Reason)
		assert.False(t, cond.Unconfirmed)
		assert.False(t, cond.UpdatedAt.IsZero(), "the store stamps on write")
	})

	t.Run("a type named twice is refused", func(t *testing.T) {
		err := c.SetConditions(ctx, obj.ID, []Condition{
			{Type: "Dup", Status: ConditionTrue},
			{Type: "Dup", Status: ConditionFalse},
		})
		assert.ErrorIs(t, err, ErrDuplicateConditionType)
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

func TestTestClientDeleteCondition(t *testing.T) {
	ctx, objects, c, obj := newTestClientFixture(t)
	require.NoError(t, c.SetCondition(ctx, obj.ID, Condition{Type: "Ready", Status: ConditionTrue}))

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

func TestTestClientScoping(t *testing.T) {
	// Not the fixture: the foreign-kind case needs a second client over the same
	// beehive.
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	objects := NewClient[tcSpec, tcStatus](bh, tcGK)
	c := NewTestClient[tcStatus](bh, tcGK)

	obj, err := objects.Create(ctx, "prod", tcSpec{Region: "us-east-1"})
	require.NoError(t, err)

	t.Run("a foreign kind is refused", func(t *testing.T) {
		other := NewTestClient[tcStatus](bh, GroupKind{Group: "kstack", Kind: "Cache"})
		assert.ErrorIs(t, other.UpdateStatus(ctx, obj.ID, tcStatus{}), ErrWrongKind)
	})

	t.Run("a missing id is not found", func(t *testing.T) {
		assert.ErrorIs(t, c.UpdateStatus(ctx, ObjectID(9999), tcStatus{}), ErrNotFound)
	})

	t.Run("deleting a condition is scoped the same way", func(t *testing.T) {
		assert.ErrorIs(t, c.DeleteCondition(ctx, ObjectID(9999), "Ready"), ErrNotFound)
	})
}

// tcUnmarshalableStatus stands in for a Status a caller cannot serialise.
type tcUnmarshalableStatus struct{}

func (tcUnmarshalableStatus) MarshalJSON() ([]byte, error) { return nil, errTCNoJSON }

var errTCNoJSON = errors.New("this status does not marshal")

func TestTestClientReportsAMarshalFailure(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	objects := NewClient[tcSpec, tcUnmarshalableStatus](bh, tcGK)

	obj, err := objects.Create(ctx, "prod", tcSpec{Region: "us-east-1"})
	require.NoError(t, err)

	c := NewTestClient[tcUnmarshalableStatus](bh, tcGK)
	assert.ErrorIs(t, c.UpdateStatus(ctx, obj.ID, tcUnmarshalableStatus{}), errTCNoJSON)
}
