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
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/amorey/beehive/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type cSpec struct{ Val string }
type cStatus struct{ Val string }

var clientTestGK = GroupKind{Kind: "Widget"}

// TestRawToTypedConversion exercises the per-blob schema-version conversion rule
// rawToTyped applies before unmarshalling. Spec and Status convert independently,
// each from its own stored version against the migrator's current version.
func TestRawToTypedConversion(t *testing.T) {
	const origSpec = `{"Val":"origspec"}`
	const origStatus = `{"Val":"origstatus"}`

	// poison converters error if called — used to prove a path that should skip
	// conversion never invokes the converter.
	poisonSpec := func(int, json.RawMessage) (json.RawMessage, error) { return nil, errBoom }
	// transform converters rewrite the blob so the decoded Val proves conversion ran.
	transformTo := func(val string) func(int, json.RawMessage) (json.RawMessage, error) {
		return func(int, json.RawMessage) (json.RawMessage, error) {
			return json.RawMessage(`{"Val":"` + val + `"}`), nil
		}
	}
	// convertFromZero only succeeds when invoked with from == 0, so a "converted"
	// result proves the unversioned baseline reaches the converter as 0.
	convertFromZero := func(from int, _ json.RawMessage) (json.RawMessage, error) {
		if from != 0 {
			return nil, errBoom
		}
		return json.RawMessage(`{"Val":"converted"}`), nil
	}

	tests := []struct {
		name       string
		migrator   Migrator
		raw        *RawObject
		wantSpec   string
		wantStatus string // "" => expect nil Status
		wantErr    bool
	}{
		{
			name:     "current 0 skips conversion even when from != 0",
			migrator: &fakeMigrator{specVersion: 0, convertSpec: poisonSpec},
			raw:      &RawObject{Spec: []byte(origSpec), SpecVersion: 5},
			wantSpec: "origspec",
		},
		{
			name:     "from == current is identity",
			migrator: &fakeMigrator{specVersion: 2, convertSpec: poisonSpec},
			raw:      &RawObject{Spec: []byte(origSpec), SpecVersion: 2},
			wantSpec: "origspec",
		},
		{
			name:     "from < current converts and the result is what unmarshals",
			migrator: &fakeMigrator{specVersion: 2, convertSpec: transformTo("converted")},
			raw:      &RawObject{Spec: []byte(origSpec), SpecVersion: 1},
			wantSpec: "converted",
		},
		{
			// from == 0 (the unversioned baseline: a row written before the kind opted
			// into versioning) is still < current, so the converter is invoked with 0.
			name:     "from 0 with current > 0 converts (unversioned baseline)",
			migrator: &fakeMigrator{specVersion: 2, convertSpec: convertFromZero},
			raw:      &RawObject{Spec: []byte(origSpec), SpecVersion: 0},
			wantSpec: "converted",
		},
		{
			name:     "from > current is a downgrade error",
			migrator: &fakeMigrator{specVersion: 2, convertSpec: poisonSpec},
			raw:      &RawObject{Spec: []byte(origSpec), SpecVersion: 3},
			wantErr:  true,
		},
		{
			// Spec decodes fine (unversioned), but the status blob is a downgrade —
			// exercises the status convert-error path independently of spec.
			name:     "status downgrade errors after spec decodes",
			migrator: &fakeMigrator{statusVersion: 2},
			raw:      &RawObject{Spec: []byte(origSpec), Status: []byte(origStatus), StatusVersion: 3},
			wantErr:  true,
		},
		{
			name:     "nil migrator is identity",
			migrator: nil,
			raw:      &RawObject{Spec: []byte(origSpec), SpecVersion: 5},
			wantSpec: "origspec",
		},
		{
			name: "spec and status convert independently",
			migrator: &fakeMigrator{
				specVersion: 2, statusVersion: 2,
				convertSpec:   transformTo("specconv"),
				convertStatus: poisonSpec, // status is already current, must not be called
			},
			raw: &RawObject{
				Spec: []byte(origSpec), SpecVersion: 1, // converts
				Status: []byte(origStatus), StatusVersion: 2, // identity
			},
			wantSpec:   "specconv",
			wantStatus: "origstatus",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			obj, err := rawToTyped[cSpec, cStatus](tc.raw, tc.migrator)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantSpec, obj.Spec.Val)
			if tc.wantStatus == "" {
				assert.Nil(t, obj.Status)
			} else {
				require.NotNil(t, obj.Status)
				assert.Equal(t, tc.wantStatus, obj.Status.Val)
			}
		})
	}
}

// TestListSkipsUndecodableRows verifies quarantine on the List path: a single
// row whose stored spec bytes don't unmarshal is skipped and logged rather than
// failing the whole list. The poison row is written first (lower id) so List
// must skip it before reaching the good one.
func TestListSkipsUndecodableRows(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)

	// No migrator: convertBlob is identity, so the bad bytes reach json.Unmarshal,
	// which fails — exactly the shape-mismatch case the migrator seam guards.
	_, err = store.CreateObject(ctx, &RawObject{
		Group: clientTestGK.Group, Kind: clientTestGK.Kind, Spec: []byte(`not json`),
	})
	require.NoError(t, err)
	good, err := store.CreateObject(ctx, &RawObject{
		Group: clientTestGK.Group, Kind: clientTestGK.Kind, Spec: []byte(`{"Val":"good"}`),
	})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	objs, err := client.List(ctx)
	require.NoError(t, err, "a poison row must not fail the whole list")
	require.Len(t, objs, 1, "only the decodable row is returned")
	assert.Equal(t, good.ID, objs[0].ID)
	assert.Equal(t, "good", objs[0].Spec.Val)
}

// TestWatchListSkipsUndecodableRows verifies quarantine on the watch path: a
// poison object in the snapshot is skipped and the stream stays alive to deliver
// the good object, rather than the watcher silently closing on the first decode
// failure. The poison row is created first so it is processed before the good one.
func TestWatchListSkipsUndecodableRows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	_, err = store.CreateObject(ctx, &RawObject{
		Group: clientTestGK.Group, Kind: clientTestGK.Kind, Spec: []byte(`not json`),
	})
	require.NoError(t, err)
	good, err := store.CreateObject(ctx, &RawObject{
		Group: clientTestGK.Group, Kind: clientTestGK.Kind, Spec: []byte(`{"Val":"good"}`),
	})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	select {
	case ev, ok := <-ch:
		require.True(t, ok, "stream must stay open past the poison row")
		require.NotNil(t, ev.Object)
		assert.Equal(t, good.ID, ev.Object.ID, "the good object flows even though the poison one preceded it")
		assert.Equal(t, "good", ev.Object.Spec.Val)
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for the good object's event")
	}
}

func newClientTestStore(t *testing.T) Store {
	t.Helper()
	s, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

// errMarshaler is a type whose JSON marshaling always fails, used to exercise
// the json.Marshal error paths in Create and Update.
type errMarshaler struct{}

func (errMarshaler) MarshalJSON() ([]byte, error) { return nil, errors.New("cannot marshal") }

func TestClientCreateMarshalError(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[errMarshaler, cStatus](bh, clientTestGK)
	_, err = client.Create(ctx, errMarshaler{})
	require.Error(t, err)
}

func TestClientUpdateMarshalError(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[errMarshaler, cStatus](bh, clientTestGK)
	_, err = client.Update(ctx, 1, errMarshaler{})
	require.Error(t, err)
}

// TestClientCreateOptionError verifies Create propagates an error returned by a
// per-call Option (before any store write), so a bad option fails fast.
func TestClientCreateOptionError(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	// An option that fails when applied to the create-options target.
	badOpt := func(target any) error {
		if _, ok := target.(*createOptions); ok {
			return errBoom
		}
		return nil
	}

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, err = client.Create(ctx, cSpec{Val: "x"}, badOpt)
	require.ErrorIs(t, err, errBoom)
}

func TestClientCreate(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)
	assert.NotZero(t, obj.ID)
	assert.Equal(t, clientTestGK.Group, obj.Group)
	assert.Equal(t, clientTestGK.Kind, obj.Kind)
	assert.Equal(t, int64(1), obj.Generation)
	assert.Nil(t, obj.Status)
	assert.Equal(t, "hello", obj.Spec.Val)
}

func TestClientCreateWithOptions(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	// An owner must exist before a child can ref it.
	owner, err := client.Create(ctx, cSpec{Val: "owner"})
	require.NoError(t, err)

	child, err := client.Create(ctx, cSpec{Val: "child"},
		WithSlug("child-1"),
		WithFinalizers("cleanup-a", "cleanup-b"),
		WithOwner(owner.ID))
	require.NoError(t, err)

	require.NotNil(t, child.Slug)
	assert.Equal(t, "child-1", *child.Slug)
	assert.Equal(t, []string{"cleanup-a", "cleanup-b"}, child.Finalizers)

	// Slug is persisted and looked up via GetBySlug.
	got, err := client.GetBySlug(ctx, "child-1")
	require.NoError(t, err)
	assert.Equal(t, child.ID, got.ID)
	assert.Equal(t, []string{"cleanup-a", "cleanup-b"}, got.Finalizers)

	// The owner ref is recorded child -> owner, so the owner sees the child.
	refs, err := store.ListIncomingRefs(ctx, owner.ID, RelationOwnedBy)
	require.NoError(t, err)
	require.Len(t, refs, 1)
	assert.Equal(t, child.ID, refs[0].ID)
}

func TestClientGet(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	created, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)

	got, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, created.ID, got.ID)
	assert.Equal(t, "hello", got.Spec.Val)
	assert.Nil(t, got.Status)
}

func TestClientGetBySlug(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, err = client.GetBySlug(ctx, "nonexistent")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestClientList(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
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
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	created, err := client.Create(ctx, cSpec{Val: "v1"})
	require.NoError(t, err)

	updated, err := client.Update(ctx, created.ID, cSpec{Val: "v2"})
	require.NoError(t, err)
	assert.Equal(t, created.ID, updated.ID)
	assert.Equal(t, int64(2), updated.Generation)
	assert.Equal(t, "v2", updated.Spec.Val)
}

func TestClientCreateOrUpdateCreates(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.CreateOrUpdate(ctx, "w1", cSpec{Val: "a"})
	require.NoError(t, err)
	assert.NotZero(t, obj.ID)
	require.NotNil(t, obj.Slug)
	assert.Equal(t, "w1", *obj.Slug)
	assert.Equal(t, int64(1), obj.Generation)
	assert.Equal(t, "a", obj.Spec.Val)

	got, err := client.GetBySlug(ctx, "w1")
	require.NoError(t, err)
	assert.Equal(t, obj.ID, got.ID)
}

func TestClientCreateOrUpdateUpdates(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	created, err := client.CreateOrUpdate(ctx, "w1", cSpec{Val: "a"})
	require.NoError(t, err)

	updated, err := client.CreateOrUpdate(ctx, "w1", cSpec{Val: "b"})
	require.NoError(t, err)
	assert.Equal(t, created.ID, updated.ID)
	assert.Equal(t, int64(2), updated.Generation)
	assert.Equal(t, "b", updated.Spec.Val)
}

func TestClientCreateOrUpdateIdempotent(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	first, err := client.CreateOrUpdate(ctx, "w1", cSpec{Val: "a"})
	require.NoError(t, err)
	assert.Equal(t, int64(1), first.Generation)

	// Re-applying the same spec is a no-op: no generation bump.
	second, err := client.CreateOrUpdate(ctx, "w1", cSpec{Val: "a"})
	require.NoError(t, err)
	assert.Equal(t, first.ID, second.ID)
	assert.Equal(t, int64(1), second.Generation)
}

func TestClientCreateOrUpdateMarshalError(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[errMarshaler, cStatus](bh, clientTestGK)
	_, err = client.CreateOrUpdate(ctx, "w1", errMarshaler{})
	require.Error(t, err)
}

func TestClientCreateOrUpdateStoreError(t *testing.T) {
	ctx := context.Background()
	bh, err := New(&slugErrorStore{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, err = client.CreateOrUpdate(ctx, "w1", cSpec{Val: "a"})
	require.ErrorIs(t, err, errBoom)
}

func TestClientCreateOrUpdateRawToTypedError(t *testing.T) {
	ctx := context.Background()
	bh, err := New(&createOrUpdateBadJSONStore{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, err = client.CreateOrUpdate(ctx, "w1", cSpec{Val: "a"})
	require.Error(t, err)
}

func TestClientGetNotFound(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, err = client.Get(ctx, 999)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestClientGetBySlugFound(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)

	// Create a named object via the store directly (client.Create uses nil slug).
	specJSON, err := json.Marshal(cSpec{Val: "hello"})
	require.NoError(t, err)
	raw, err := store.CreateObject(ctx, &RawObject{
		Group: clientTestGK.Group, Kind: clientTestGK.Kind,
		Slug: new("myobj"), Spec: specJSON,
	})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	got, err := client.GetBySlug(ctx, "myobj")
	require.NoError(t, err)
	assert.Equal(t, raw.ID, got.ID)
	assert.Equal(t, "hello", got.Spec.Val)
}

func TestClientWatchNonExistentID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, client := watchTestBH(t)

	// Watch a non-existent ID: the snapshot loader returns (nil, nil) via the
	// ErrNotFound path, yielding an empty snapshot and an open channel.
	ch, err := client.Watch(ctx, 9999)
	require.NoError(t, err)

	// Cancel ctx — channel must close cleanly (no events, just the cancel).
	cancel()
	assertChanClosed(t, ch)
}

func TestClientDeleteNotFound(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	err = client.Delete(ctx, 999)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestClientDelete(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	created, err := client.Create(ctx, cSpec{})
	require.NoError(t, err)

	err = client.Delete(ctx, created.ID)
	require.NoError(t, err)

	// object still present (no finalizers cleared), but marked for deletion. The
	// default resync is enabled, so the client-only object isn't collected
	// synchronously by Delete — the idle sweeper is its backstop.
	got, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	assert.NotNil(t, got.DeletionRequestedAt)
}

// TestClientIDOpsScopedToKind verifies that ID-based operations on a Client are
// confined to that client's kind: an id naming an object of another kind is
// invisible (Get/Update/Delete all report ErrNotFound) and the foreign object is
// left untouched, never updated or marked for deletion through the wrong client.
func TestClientIDOpsScopedToKind(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	widgets := NewClient[cSpec, cStatus](bh, GroupKind{Kind: "Widget"})
	gadgets := NewClient[cSpec, cStatus](bh, GroupKind{Kind: "Gadget"})

	w, err := widgets.Create(ctx, cSpec{Val: "v1"})
	require.NoError(t, err)

	// The Gadget client must not see or mutate the Widget by its id.
	_, err = gadgets.Get(ctx, w.ID)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = gadgets.Update(ctx, w.ID, cSpec{Val: "hijacked"})
	require.ErrorIs(t, err, ErrNotFound)
	err = gadgets.Delete(ctx, w.ID)
	require.ErrorIs(t, err, ErrNotFound)

	// The Widget is unchanged: original spec, no deletion request.
	got, err := widgets.Get(ctx, w.ID)
	require.NoError(t, err)
	assert.Equal(t, "v1", got.Spec.Val)
	assert.Equal(t, int64(1), got.Generation)
	assert.Nil(t, got.DeletionRequestedAt)
}

// TestRawToTypedDecodesNullSpecTombstone pins the contract the lag-recovery
// tombstone relies on: a Deleted tombstone carries a JSON null spec (see
// sqlite/watch.go), and the typed watch decodes every event's spec through
// rawToTyped. null must decode into an arbitrary Spec as the zero value — here a
// scalar Spec, for which the old "{}" tombstone would fail to unmarshal and
// silently close the watch.
func TestRawToTypedDecodesNullSpecTombstone(t *testing.T) {
	// Scalar (non-object) spec: json.Unmarshal of "{}" into this errors.
	type scalarSpec = string

	tombstone := &RawObject{ID: 7, Kind: "Widget", Spec: []byte("null")}
	obj, err := rawToTyped[scalarSpec, cStatus](tombstone, nil)
	require.NoError(t, err)
	assert.Equal(t, ObjectID(7), obj.ID)
	assert.Equal(t, "", obj.Spec) // zero value, no Status to decode

	// Guard the premise: the previous "{}" tombstone would have failed here,
	// which is exactly the silent-close bug the null spec avoids.
	_, err = rawToTyped[scalarSpec, cStatus](&RawObject{ID: 7, Kind: "Widget", Spec: []byte("{}")}, nil)
	require.Error(t, err)
}

// createBadJSONStore returns bad JSON from CreateObject so rawToTyped fails.
type createBadJSONStore struct {
	fakeStore
}

func (s *createBadJSONStore) CreateObject(_ context.Context, _ *RawObject) (*RawObject, error) {
	return &RawObject{ID: 1, Spec: []byte("not-json")}, nil
}

// errorCreateObjectStore returns an error from CreateObject.
type errorCreateObjectStore struct {
	fakeStore
}

func (s *errorCreateObjectStore) CreateObject(_ context.Context, _ *RawObject) (*RawObject, error) {
	return nil, errBoom
}

// updateBadJSONStore returns bad JSON from UpdateSpec so rawToTyped fails.
type updateBadJSONStore struct {
	fakeStore
}

func (s *updateBadJSONStore) UpdateSpec(_ context.Context, _ GroupKind, _ ObjectID, _ []byte, _ int) (*RawObject, error) {
	return &RawObject{ID: 1, Spec: []byte("not-json")}, nil
}

// errorUpdateSpecStore returns an error from UpdateSpec.
type errorUpdateSpecStore struct {
	fakeStore
}

func (s *errorUpdateSpecStore) UpdateSpec(_ context.Context, _ GroupKind, _ ObjectID, _ []byte, _ int) (*RawObject, error) {
	return nil, errBoom
}

// slugErrorStore returns a non-NotFound error from GetObjectBySlug, driving
// CreateOrUpdate's default (read-error) branch.
type slugErrorStore struct {
	fakeStore
}

func (s *slugErrorStore) GetObjectBySlug(_ context.Context, _ GroupKind, _ string) (*RawObject, error) {
	return nil, errBoom
}

// createOrUpdateBadJSONStore drives CreateOrUpdate's rawToTyped error path: the
// slug is absent (NotFound) so the create branch runs, and CreateObject returns
// undecodable spec bytes.
type createOrUpdateBadJSONStore struct {
	fakeStore
}

func (s *createOrUpdateBadJSONStore) GetObjectBySlug(_ context.Context, _ GroupKind, _ string) (*RawObject, error) {
	return nil, ErrNotFound
}

func (s *createOrUpdateBadJSONStore) CreateObject(_ context.Context, _ *RawObject) (*RawObject, error) {
	return &RawObject{ID: 1, Spec: []byte("not-json")}, nil
}

// errorListObjectsStore returns an error from ListObjects.
type errorListObjectsStore struct {
	fakeStore
}

func (s *errorListObjectsStore) ListObjects(_ context.Context, _ GroupKind) ([]*RawObject, error) {
	return nil, errBoom
}

// badJSONStore is a fakeStore whose ListObjects returns a RawObject with invalid
// spec JSON, used to drive the rawToTyped error path inside client.List.
type badJSONStore struct {
	fakeStore
	gk GroupKind
}

func (s *badJSONStore) ListObjects(_ context.Context, _ GroupKind) ([]*RawObject, error) {
	return []*RawObject{{ID: 1, Group: s.gk.Group, Kind: s.gk.Kind, Spec: []byte("not-json")}}, nil
}

// newWatchClient registers gk with a fake controller (so the client-side
// isRegistered check passes) and returns a client backed by store.
func newWatchClient(t *testing.T, store Store, gk GroupKind) Client[tSpec, tStatus] {
	t.Helper()
	bh, err := New(store)
	require.NoError(t, err)
	_, err = Register(bh, gk, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)
	return NewClient[tSpec, tStatus](bh, gk)
}

func TestClientCreateStoreError(t *testing.T) {
	bh, err := New(&errorCreateObjectStore{})
	require.NoError(t, err)
	client := NewClient[tSpec, tStatus](bh, GroupKind{Kind: "Widget"})
	_, err = client.Create(context.Background(), tSpec{})
	require.Error(t, err)
}

func TestClientCreateRawToTypedError(t *testing.T) {
	bh, err := New(&createBadJSONStore{})
	require.NoError(t, err)
	client := NewClient[tSpec, tStatus](bh, GroupKind{Kind: "Widget"})
	_, err = client.Create(context.Background(), tSpec{})
	require.Error(t, err)
}

func TestClientUpdateStoreError(t *testing.T) {
	bh, err := New(&errorUpdateSpecStore{})
	require.NoError(t, err)
	client := NewClient[tSpec, tStatus](bh, GroupKind{Kind: "Widget"})
	_, err = client.Update(context.Background(), 1, tSpec{})
	require.Error(t, err)
}

func TestClientUpdateRawToTypedError(t *testing.T) {
	bh, err := New(&updateBadJSONStore{})
	require.NoError(t, err)
	client := NewClient[tSpec, tStatus](bh, GroupKind{Kind: "Widget"})
	_, err = client.Update(context.Background(), 1, tSpec{})
	require.Error(t, err)
}

// TestClientWatchPropagatesStoreError verifies the client surfaces an error
// returned by the store's Watch/WatchList (e.g. a failed snapshot load).
func TestClientWatchPropagatesStoreError(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	bh, err := New(&watcherStore{err: errBoom})
	require.NoError(t, err)
	_, err = Register(bh, gk, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	client := NewClient[tSpec, tStatus](bh, gk)
	_, err = client.Watch(context.Background(), 1)
	require.ErrorIs(t, err, errBoom)
	_, err = client.WatchList(context.Background())
	require.ErrorIs(t, err, errBoom)
}

func TestClientListStoreError(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	bh, err := New(&errorListObjectsStore{})
	require.NoError(t, err)
	client := NewClient[tSpec, tStatus](bh, gk)
	_, err = client.List(context.Background())
	require.Error(t, err)
}

// TestClientListRawToTypedError verifies List quarantines an un-decodable row
// (skip-and-log) instead of failing the whole list: badJSONStore returns one row
// whose Spec is invalid JSON, so List returns no error and an empty result.
func TestClientListRawToTypedError(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	bh, err := New(&badJSONStore{gk: gk})
	require.NoError(t, err)
	client := NewClient[tSpec, tStatus](bh, gk)
	objs, err := client.List(context.Background())
	require.NoError(t, err, "a poison row is skipped, not fatal")
	assert.Empty(t, objs, "the only row was un-decodable, so none are returned")
}

// TestClientAdaptWatcherConversionError verifies a raw event whose Spec is
// invalid JSON is skipped (quarantined) and the stream stays open, rather than
// closing the typed channel. A following good event still flows.
func TestClientAdaptWatcherConversionError(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	w := newFakeWatcher()
	client := newWatchClient(t, &watcherStore{w: w}, gk)

	ch, err := client.WatchList(context.Background())
	require.NoError(t, err)

	w.push(WatchEventModified, &RawObject{ID: 1, Spec: []byte("not-json")})
	w.push(WatchEventAdded, &RawObject{ID: 2, Spec: []byte(`{}`)})

	select {
	case evt, ok := <-ch:
		require.True(t, ok, "stream must stay open past the poison event")
		assert.EqualValues(t, 2, evt.Object.ID, "the good event flows after the skipped one")
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for the good event")
	}
}

// TestClientAdaptWatcherForwardsThenClosesOnCancel verifies a decodable event is
// forwarded as a typed WatchEvent, and cancelling the context closes the channel.
func TestClientAdaptWatcherForwardsThenClosesOnCancel(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	w := newFakeWatcher()
	client := newWatchClient(t, &watcherStore{w: w}, gk)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	w.push(WatchEventAdded, &RawObject{ID: 1, Spec: []byte(`{}`)})
	select {
	case evt, ok := <-ch:
		require.True(t, ok)
		assert.Equal(t, WatchEventAdded, evt.Type)
		assert.EqualValues(t, 1, evt.Object.ID)
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for forwarded event")
	}

	cancel()
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "channel must close on ctx cancel")
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for channel to close")
	}
}

// TestClientAdaptWatcherSendParkCtxDone covers the adapter exiting on ctx
// cancellation while parked sending a typed event: an event is delivered to the
// adapter but never read downstream, then the context is cancelled.
func TestClientAdaptWatcherSendParkCtxDone(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	w := newFakeWatcher()
	client := newWatchClient(t, &watcherStore{w: w}, gk)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	// push returns once the adapter has taken the event; with no reader on ch it
	// then parks on its inner send. Cancelling makes that send take the ctx.Done
	// arm. Synchronize on the goroutine's exit (Close) rather than reading ch:
	// a read here could satisfy the pending send and race the closed-vs-delivered
	// outcome (notably under -race).
	w.push(WatchEventAdded, &RawObject{ID: 1, Spec: []byte(`{}`)})
	cancel()
	select {
	case <-w.closed:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for adapter goroutine to exit")
	}

	select {
	case _, ok := <-ch:
		assert.False(t, ok, "channel must close when ctx is cancelled mid-send")
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for channel to close")
	}
}

// TestClientAdaptWatcherClosesWhenStreamEnds verifies the typed channel closes
// when the underlying store watcher's stream ends.
func TestClientAdaptWatcherClosesWhenStreamEnds(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	w := newFakeWatcher()
	client := newWatchClient(t, &watcherStore{w: w}, gk)

	ch, err := client.WatchList(context.Background())
	require.NoError(t, err)

	w.endStream()
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "channel must close when the watcher stream ends")
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for channel to close")
	}
}

// recvWatch waits for the next event on ch, failing the test if none arrives
// within the failsafe timeout.
func recvWatch[S, T any](t *testing.T, ch <-chan WatchEvent[S, T]) WatchEvent[S, T] {
	t.Helper()
	select {
	case evt, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed unexpectedly")
		}
		return evt
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watch event")
		panic("unreachable")
	}
}

// assertChanClosed fails the test if ch does not close within the failsafe timeout.
func assertChanClosed[S, T any](t *testing.T, ch <-chan WatchEvent[S, T]) {
	t.Helper()
	// Drain any buffered events, then expect close.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for watch channel to close")
		}
	}
}

// watchTestBH builds a Beehive with a real SQLite store and a registered
// controller for clientTestGK. No Start is needed for client-side event tests.
func watchTestBH(t *testing.T) (*Beehive, Client[cSpec, cStatus]) {
	t.Helper()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	return bh, client
}

// TestWatchListReceivesAddedOnCreate verifies that WatchList delivers a
// WatchEventAdded when an object is created.
func TestWatchListReceivesAddedOnCreate(t *testing.T) {
	ctx := context.Background()
	_, client := watchTestBH(t)

	ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	obj, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)

	evt := recvWatch(t, ch)
	assert.Equal(t, WatchEventAdded, evt.Type)
	assert.Equal(t, obj.ID, evt.Object.ID)
	assert.Equal(t, "hello", evt.Object.Spec.Val)
}

// TestWatchListReceivesModifiedOnUpdate verifies that WatchList delivers a
// WatchEventModified when an object's spec is updated.
func TestWatchListReceivesModifiedOnUpdate(t *testing.T) {
	ctx := context.Background()
	_, client := watchTestBH(t)

	// Subscribe before creating so the snapshot is empty and the first event is
	// the Modified from the Update, not an Added from the snapshot.
	ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	obj, err := client.Create(ctx, cSpec{Val: "v1"})
	require.NoError(t, err)
	// Drain the Added event from Create.
	recvWatch(t, ch)

	_, err = client.Update(ctx, obj.ID, cSpec{Val: "v2"})
	require.NoError(t, err)

	evt := recvWatch(t, ch)
	assert.Equal(t, WatchEventModified, evt.Type)
	assert.Equal(t, obj.ID, evt.Object.ID)
	assert.Equal(t, "v2", evt.Object.Spec.Val)
}

// TestWatchListReceivesModifiedOnDelete verifies that WatchList delivers a
// WatchEventModified (not Deleted) when deletion is requested, because the
// object still exists in the store with DeletionRequestedAt set.
func TestWatchListReceivesModifiedOnDelete(t *testing.T) {
	ctx := context.Background()
	_, client := watchTestBH(t)

	ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	obj, err := client.Create(ctx, cSpec{})
	require.NoError(t, err)
	// Drain the Added event from Create.
	recvWatch(t, ch)

	require.NoError(t, client.Delete(ctx, obj.ID))

	evt := recvWatch(t, ch)
	assert.Equal(t, WatchEventModified, evt.Type)
	assert.Equal(t, obj.ID, evt.Object.ID)
	assert.NotNil(t, evt.Object.DeletionRequestedAt)
}

// TestWatchListNoEventOnIdempotentDelete verifies that a second Delete call for
// an already-pending-deletion object emits no additional watch event.
func TestWatchListNoEventOnIdempotentDelete(t *testing.T) {
	ctx := context.Background()
	_, client := watchTestBH(t)

	ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	obj, err := client.Create(ctx, cSpec{})
	require.NoError(t, err)
	recvWatch(t, ch) // drain Added

	require.NoError(t, client.Delete(ctx, obj.ID))
	recvWatch(t, ch) // drain first Modified

	// Second Delete is idempotent; no new event should arrive.
	require.NoError(t, client.Delete(ctx, obj.ID))
	select {
	case evt, ok := <-ch:
		if ok {
			t.Fatalf("unexpected event on idempotent delete: %v", evt)
		}
	case <-time.After(100 * time.Millisecond):
		// correct — nothing arrived
	}
}

// TestWatchReceivesOnlyMatchingID verifies that Watch(id) filters out events
// for other objects.
func TestWatchReceivesOnlyMatchingID(t *testing.T) {
	ctx := context.Background()
	_, client := watchTestBH(t)

	obj1, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)
	obj2, err := client.Create(ctx, cSpec{Val: "b"})
	require.NoError(t, err)

	ch, err := client.Watch(ctx, obj1.ID)
	require.NoError(t, err)

	// Drain the initial snapshot Added event for obj1.
	snap := recvWatch(t, ch)
	assert.Equal(t, WatchEventAdded, snap.Type)
	assert.Equal(t, obj1.ID, snap.Object.ID)

	// Update obj2 first — this event must not appear on ch.
	_, err = client.Update(ctx, obj2.ID, cSpec{Val: "b2"})
	require.NoError(t, err)

	// Update obj1 — this must appear.
	_, err = client.Update(ctx, obj1.ID, cSpec{Val: "a2"})
	require.NoError(t, err)

	evt := recvWatch(t, ch)
	assert.Equal(t, obj1.ID, evt.Object.ID)
	assert.Equal(t, "a2", evt.Object.Spec.Val)
}

// TestWatchListClosesOnCtxCancel verifies that the watch channel is closed when
// the context is cancelled.
func TestWatchListClosesOnCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, client := watchTestBH(t)

	ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	cancel()
	assertChanClosed(t, ch)
}

// TestWatchClosesOnCtxCancel verifies that Watch(id) channel closes on ctx cancel.
func TestWatchClosesOnCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, client := watchTestBH(t)

	obj, err := client.Create(context.Background(), cSpec{})
	require.NoError(t, err)

	ch, err := client.Watch(ctx, obj.ID)
	require.NoError(t, err)

	cancel()
	assertChanClosed(t, ch)
}

// TestWatchReceivesModifiedOnStatusUpdate verifies that WatchList delivers a
// WatchEventModified when the controller calls UpdateStatus.
func TestWatchReceivesModifiedOnStatusUpdate(t *testing.T) {
	ctx := context.Background()

	// watchTestBH already registered one; we need a fresh beehive for this test.
	bh2, err := New(newClientTestStore(t))
	require.NoError(t, err)
	cc, err := Register(bh2, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client2 := NewClient[cSpec, cStatus](bh2, clientTestGK)

	stop, err := bh2.Start(context.Background())
	require.NoError(t, err)
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = stop(stopCtx)
	}()

	obj, err := client2.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	// Subscribe after create: the snapshot emits Added(obj) first, then we
	// expect Modified from UpdateStatus.
	ch, err := client2.WatchList(ctx)
	require.NoError(t, err)

	// Drain the initial snapshot Added event.
	snap := recvWatch(t, ch)
	assert.Equal(t, WatchEventAdded, snap.Type)
	assert.Equal(t, obj.ID, snap.Object.ID)

	require.NoError(t, cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"}))

	evt := recvWatch(t, ch)
	assert.Equal(t, WatchEventModified, evt.Type)
	assert.Equal(t, obj.ID, evt.Object.ID)
	require.NotNil(t, evt.Object.Status)
	assert.Equal(t, "done", evt.Object.Status.Val)
}

// TestWatchListInitialSnapshot verifies that WatchList emits Added events for
// objects that already exist in the store at subscription time.
func TestWatchListInitialSnapshot(t *testing.T) {
	ctx := context.Background()
	_, client := watchTestBH(t)

	a, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)
	b, err := client.Create(ctx, cSpec{Val: "b"})
	require.NoError(t, err)

	ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	// Two snapshot Added events must arrive, one per existing object.
	seen := map[ObjectID]string{}
	for range 2 {
		evt := recvWatch(t, ch)
		assert.Equal(t, WatchEventAdded, evt.Type)
		seen[evt.Object.ID] = evt.Object.Spec.Val
	}
	assert.Equal(t, "a", seen[a.ID])
	assert.Equal(t, "b", seen[b.ID])
}

// TestWatchInitialSnapshot verifies that Watch(id) emits an Added event for an
// object that already exists in the store at subscription time.
func TestWatchInitialSnapshot(t *testing.T) {
	ctx := context.Background()
	_, client := watchTestBH(t)

	obj, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)

	ch, err := client.Watch(ctx, obj.ID)
	require.NoError(t, err)

	evt := recvWatch(t, ch)
	assert.Equal(t, WatchEventAdded, evt.Type)
	assert.Equal(t, obj.ID, evt.Object.ID)
	assert.Equal(t, "hello", evt.Object.Spec.Val)
}

// TestStartAfterStopErrors verifies that Beehive is a one-shot object: calling
// Start after Stop returns an error instead of silently reusing closed hubs.
func TestStartAfterStopErrors(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	stopCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	_ = stop(stopCtx)
	cancel()

	_, err = bh.Start(ctx)
	require.Error(t, err, "Start after Stop must return an error")
}

// TestWatchListErrForUnregisteredKind verifies that WatchList returns an error
// (not a panic) when no controller is registered for the given GroupKind.
func TestWatchListErrForUnregisteredKind(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	unknownGK := GroupKind{Kind: "Unknown"}
	client := NewClient[cSpec, cStatus](bh, unknownGK)

	_, err = client.WatchList(ctx)
	require.Error(t, err)

	_, err = client.Watch(ctx, 0)
	require.Error(t, err)
}

func TestClientGetOwner(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	owner, err := client.Create(ctx, cSpec{Val: "owner"})
	require.NoError(t, err)
	child, err := client.Create(ctx, cSpec{Val: "child"}, WithOwner(owner.ID))
	require.NoError(t, err)

	got, ok, err := client.GetOwner(ctx, child.ID)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, Ref{ID: owner.ID, Group: clientTestGK.Group, Kind: clientTestGK.Kind}, got)

	// An ownerless object reports absence, not an error.
	_, ok, err = client.GetOwner(ctx, owner.ID)
	require.NoError(t, err)
	assert.False(t, ok)

	// A missing id is not kind-validated (no scopedGet guard): it reads as
	// ownerless rather than ErrNotFound — the speed-for-isolation trade.
	_, ok, err = client.GetOwner(ctx, 99999)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestClientListDependenciesAndDependents(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	a, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)
	b, err := client.Create(ctx, cSpec{Val: "b"})
	require.NoError(t, err)
	c, err := client.Create(ctx, cSpec{Val: "c"})
	require.NoError(t, err)

	// a depends on b and c.
	require.NoError(t, store.AddRef(ctx, a.ID, b.ID, RelationDependsOn))
	require.NoError(t, store.AddRef(ctx, a.ID, c.ID, RelationDependsOn))

	deps, err := client.ListDependencies(ctx, a.ID)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{b.ID, c.ID}, refObjectIDs(deps))

	// b's dependents include a.
	dependents, err := client.ListDependents(ctx, b.ID)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{a.ID}, refObjectIDs(dependents))

	// No edges -> empty, no error.
	none, err := client.ListDependencies(ctx, b.ID)
	require.NoError(t, err)
	assert.Empty(t, none)
}

func TestClientListOwned(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	owner, err := client.Create(ctx, cSpec{Val: "owner"})
	require.NoError(t, err)
	c1, err := client.Create(ctx, cSpec{Val: "c1"}, WithOwner(owner.ID))
	require.NoError(t, err)
	c2, err := client.Create(ctx, cSpec{Val: "c2"}, WithOwner(owner.ID))
	require.NoError(t, err)

	owned, err := client.ListOwned(ctx, owner.ID)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{c1.ID, c2.ID}, refObjectIDs(owned))

	// A child owns nothing -> empty, no error.
	none, err := client.ListOwned(ctx, c1.ID)
	require.NoError(t, err)
	assert.Empty(t, none)
}

func refObjectIDs(refs []Ref) []ObjectID {
	var ids []ObjectID
	for _, r := range refs {
		ids = append(ids, r.ID)
	}
	return ids
}

func TestClientGetWithLoadOwner(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	owner, err := client.Create(ctx, cSpec{Val: "owner"})
	require.NoError(t, err)
	child, err := client.Create(ctx, cSpec{Val: "child"}, WithOwner(owner.ID))
	require.NoError(t, err)

	// Without the selector the owner is not loaded — accessing it errors.
	plain, err := client.Get(ctx, child.ID)
	require.NoError(t, err)
	_, _, err = plain.GetOwner()
	assert.ErrorIs(t, err, ErrNotLoaded, "owner not loaded without LoadOwner()")

	// With it, the owner is populated in the same read.
	got, err := client.Get(ctx, child.ID, LoadOwner())
	require.NoError(t, err)
	ref, ok, err := got.GetOwner()
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, owner.ID, ref.ID)

	// GetBySlug honours selectors too.
	_, err = client.Create(ctx, cSpec{Val: "slugged"}, WithSlug("s1"), WithOwner(owner.ID))
	require.NoError(t, err)
	bySlug, err := client.GetBySlug(ctx, "s1", LoadOwner())
	require.NoError(t, err)
	ref, ok, err = bySlug.GetOwner()
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, owner.ID, ref.ID)
}

// countingStore wraps a real store to count the batched owner lookup, proving
// eager List fans out one store call rather than one per object.
type countingStore struct {
	Store
	outgoingByIDs int
	incomingByIDs int
}

func (s *countingStore) GroupOutgoingRefsByID(ctx context.Context, ids []ObjectID, rel Relation) (map[ObjectID][]Referrer, error) {
	s.outgoingByIDs++
	return s.Store.GroupOutgoingRefsByID(ctx, ids, rel)
}

func (s *countingStore) GroupIncomingRefsByID(ctx context.Context, ids []ObjectID, rel Relation) (map[ObjectID][]Referrer, error) {
	s.incomingByIDs++
	return s.Store.GroupIncomingRefsByID(ctx, ids, rel)
}

func TestClientListWithLoadOwnerBatches(t *testing.T) {
	ctx := context.Background()
	store := &countingStore{Store: newClientTestStore(t)}
	bh, err := New(store)
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	owner, err := client.Create(ctx, cSpec{Val: "owner"})
	require.NoError(t, err)
	const n = 5
	for i := 0; i < n; i++ {
		_, err := client.Create(ctx, cSpec{Val: fmt.Sprintf("child-%d", i)}, WithOwner(owner.ID))
		require.NoError(t, err)
	}

	objs, err := client.List(ctx, LoadOwner())
	require.NoError(t, err)

	var withOwner int
	for _, o := range objs {
		ref, ok, err := o.GetOwner()
		require.NoError(t, err)
		if ok {
			assert.Equal(t, owner.ID, ref.ID)
			withOwner++
		}
	}
	assert.Equal(t, n, withOwner, "every child's owner populated")
	assert.Equal(t, 1, store.outgoingByIDs, "owner load batched into one store call, not N")
}

func TestClientLoadsOwned(t *testing.T) {
	ctx := context.Background()
	store := &countingStore{Store: newClientTestStore(t)}
	bh, err := New(store)
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	owner, err := client.Create(ctx, cSpec{Val: "owner"})
	require.NoError(t, err)
	const n = 3
	var childIDs []ObjectID
	for i := 0; i < n; i++ {
		c, err := client.Create(ctx, cSpec{Val: fmt.Sprintf("child-%d", i)}, WithOwner(owner.ID))
		require.NoError(t, err)
		childIDs = append(childIDs, c.ID)
	}

	// Without the selector the owned set is not loaded — accessing it errors.
	plain, err := client.Get(ctx, owner.ID)
	require.NoError(t, err)
	_, err = plain.ListOwned()
	assert.ErrorIs(t, err, ErrNotLoaded, "owned not loaded without LoadOwned()")

	// Single-object path populates the owner's children.
	got, err := client.Get(ctx, owner.ID, LoadOwned())
	require.NoError(t, err)
	owned, err := got.ListOwned()
	require.NoError(t, err)
	assert.Equal(t, childIDs, refObjectIDs(owned))

	// A child owns nothing: loaded but empty.
	leaf, err := client.Get(ctx, childIDs[0], LoadOwned())
	require.NoError(t, err)
	owned, err = leaf.ListOwned()
	require.NoError(t, err, "loaded even though empty")
	assert.Empty(t, owned)

	// Batched List path fans out one incoming-edge store call, not one per object.
	store.incomingByIDs = 0
	objs, err := client.List(ctx, LoadOwned())
	require.NoError(t, err)
	byID := map[ObjectID]*Object[cSpec, cStatus]{}
	for _, o := range objs {
		byID[o.ID] = o
	}
	owned, err = byID[owner.ID].ListOwned()
	require.NoError(t, err)
	assert.Equal(t, childIDs, refObjectIDs(owned))
	assert.Equal(t, 1, store.incomingByIDs, "owned load batched into one store call, not N")
}

func TestClientGetLoadsDependenciesAndDependents(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	a, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)
	b, err := client.Create(ctx, cSpec{Val: "b"})
	require.NoError(t, err)
	require.NoError(t, store.AddRef(ctx, a.ID, b.ID, RelationDependsOn)) // a depends on b

	got, err := client.Get(ctx, a.ID, LoadDependencies(), LoadDependents())
	require.NoError(t, err)
	deps, err := got.ListDependencies()
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{b.ID}, refObjectIDs(deps))
	dependents, err := got.ListDependents()
	require.NoError(t, err, "loaded even though empty")
	assert.Empty(t, dependents)

	got, err = client.Get(ctx, b.ID, LoadDependents())
	require.NoError(t, err)
	dependents, err = got.ListDependents()
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{a.ID}, refObjectIDs(dependents))
}

func TestClientListBatchesDependenciesAndDependents(t *testing.T) {
	ctx := context.Background()
	store := &countingStore{Store: newClientTestStore(t)}
	bh, err := New(store)
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	a, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)
	b, err := client.Create(ctx, cSpec{Val: "b"})
	require.NoError(t, err)
	require.NoError(t, store.AddRef(ctx, a.ID, b.ID, RelationDependsOn))

	objs, err := client.List(ctx, LoadDependencies(), LoadDependents())
	require.NoError(t, err)
	byID := map[ObjectID]*Object[cSpec, cStatus]{}
	for _, o := range objs {
		byID[o.ID] = o
	}

	deps, err := byID[a.ID].ListDependencies()
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{b.ID}, refObjectIDs(deps))
	dependents, err := byID[b.ID].ListDependents()
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{a.ID}, refObjectIDs(dependents))

	assert.Equal(t, 1, store.outgoingByIDs, "dependencies batched into one call")
	assert.Equal(t, 1, store.incomingByIDs, "dependents batched into one call")
}

// refErrorStore wraps a real store but errors on every ref-edge lookup, driving
// the error branches of the eager loaders (single + batched) and fetchOwnerRef.
type refErrorStore struct {
	Store
}

func (refErrorStore) ListOutgoingRefsByRelation(context.Context, ObjectID, Relation) ([]Referrer, error) {
	return nil, errBoom
}
func (refErrorStore) ListIncomingRefs(context.Context, ObjectID, Relation) ([]Referrer, error) {
	return nil, errBoom
}
func (refErrorStore) GroupOutgoingRefsByID(context.Context, []ObjectID, Relation) (map[ObjectID][]Referrer, error) {
	return nil, errBoom
}
func (refErrorStore) GroupIncomingRefsByID(context.Context, []ObjectID, Relation) (map[ObjectID][]Referrer, error) {
	return nil, errBoom
}

func TestEagerLoadStoreErrorsPropagate(t *testing.T) {
	ctx := context.Background()
	store := &refErrorStore{Store: newClientTestStore(t)}
	bh, err := New(store)
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	obj, err := client.Create(ctx, cSpec{Val: "x"}, WithSlug("x1"))
	require.NoError(t, err)

	loads := []LoadOption{LoadOwner(), LoadDependencies(), LoadDependents(), LoadOwned()}
	// Single-object path: each relation's store error surfaces through Get/GetBySlug.
	for _, l := range loads {
		_, err := client.Get(ctx, obj.ID, l)
		require.ErrorIs(t, err, errBoom)
	}
	_, err = client.GetBySlug(ctx, "x1", LoadOwner())
	require.ErrorIs(t, err, errBoom)

	// Batched path: each relation's store error surfaces through List.
	for _, l := range loads {
		_, err := client.List(ctx, l)
		require.ErrorIs(t, err, errBoom)
	}
}

func TestClientLazyRefsMissingIDReadsEmpty(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	// The lazy lookups drop the scopedGet kind guard for speed, so a missing id
	// reads as empty rather than ErrNotFound (matching the ControllerClient quartet).
	deps, err := client.ListDependencies(ctx, 99999)
	require.NoError(t, err)
	assert.Empty(t, deps)
	dependents, err := client.ListDependents(ctx, 99999)
	require.NoError(t, err)
	assert.Empty(t, dependents)
	owned, err := client.ListOwned(ctx, 99999)
	require.NoError(t, err)
	assert.Empty(t, owned)
}

// getBadJSONStore returns an undecodable spec from the scoped Get/GetBySlug
// reads, driving the decode-error branch of Client.Get / Client.GetBySlug.
type getBadJSONStore struct {
	fakeStore
	gk GroupKind
}

func (s *getBadJSONStore) GetObject(context.Context, ObjectID) (*RawObject, error) {
	return &RawObject{ID: 1, Group: s.gk.Group, Kind: s.gk.Kind, Spec: []byte("not-json")}, nil
}
func (s *getBadJSONStore) GetObjectBySlug(context.Context, GroupKind, string) (*RawObject, error) {
	return &RawObject{ID: 1, Group: s.gk.Group, Kind: s.gk.Kind, Spec: []byte("not-json")}, nil
}

func TestGetDecodeError(t *testing.T) {
	ctx := context.Background()
	bh, err := New(&getBadJSONStore{gk: clientTestGK})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	_, err = client.Get(ctx, 1)
	require.Error(t, err)
	_, err = client.GetBySlug(ctx, "any")
	require.Error(t, err)
}

// TestClientRequeueNotFound verifies Requeue reports ErrNotFound
// for an id that does not exist, before reaching any reconciler.
func TestClientRequeueNotFound(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	err = client.Requeue(ctx, 999)
	assert.ErrorIs(t, err, ErrNotFound)
}

// TestClientRequeueNoController verifies Requeue reports
// ErrNoController for a client-only kind: the object exists but no reconciler is
// registered to enqueue it on.
func TestClientRequeueNoController(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	err = client.Requeue(ctx, obj.ID)
	assert.ErrorIs(t, err, ErrNoController)
}

// TestClientRequeue verifies that Requeue always enqueues the id, that a plain
// Requeue preserves the backoff ladder, and that Requeue(WithResetBackoff()) clears it.
func TestClientRequeue(t *testing.T) {
	tests := []struct {
		name string
		opts []RequeueOption
		// kept reports whether the seeded backoff entry should survive the requeue.
		kept bool
	}{
		{name: "default preserves backoff", opts: nil, kept: true},
		{name: "WithResetBackoff clears backoff", opts: []RequeueOption{WithResetBackoff()}, kept: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			bh, err := New(newClientTestStore(t))
			require.NoError(t, err)
			_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
			require.NoError(t, err)

			client := NewClient[cSpec, cStatus](bh, clientTestGK)
			obj, err := client.Create(ctx, cSpec{Val: "x"})
			require.NoError(t, err)

			r := bh.reconcilers[clientTestGK]
			// Drain the enqueue Create produced, and seed a backoff entry so the
			// requeue's effect on the ladder is observable.
			drainQueue(r.work)
			seeded := r.nextBackoff(obj.ID)
			require.NotZero(t, seeded, "precondition: backoff seeded")

			require.NoError(t, client.Requeue(ctx, obj.ID, tt.opts...))

			if tt.kept {
				assert.Equal(t, seeded, r.backoffFor[obj.ID], "plain Requeue must preserve the backoff ladder")
			} else {
				assert.Zero(t, r.backoffFor[obj.ID], "Requeue with WithResetBackoff must clear backoff")
			}
			id, ok := r.work.get()
			require.True(t, ok, "Requeue must enqueue the id")
			assert.Equal(t, obj.ID, id)
		})
	}
}

// TestClientNextRequeueAtScheduled verifies NextRequeueAt returns a pending
// delayed reconcile's fire time.
func TestClientNextRequeueAtScheduled(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	// Drain the create-time enqueue so only the future schedule remains; otherwise
	// the id is queued-now and NextRequeueAt correctly reports "due now" instead.
	r := bh.reconcilers[clientTestGK]
	drainQueue(r.work)
	r.work.addAfter(obj.ID, time.Hour)

	at := client.NextRequeueAt(ctx, obj.ID)
	assert.True(t, at.After(time.Now().Add(time.Minute)), "fire time must be ~1h out, got %s", at)
}

// TestClientNextRequeueAtUnscheduled verifies NextRequeueAt returns the zero
// time (and no error) when nothing is firmly scheduled for the id.
func TestClientNextRequeueAtUnscheduled(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	// Drain the create-time enqueue so nothing is pending.
	r := bh.reconcilers[clientTestGK]
	drainQueue(r.work)

	at := client.NextRequeueAt(ctx, obj.ID)
	assert.True(t, at.IsZero(), "unscheduled id must report the zero time, got %s", at)
}

// TestClientNextRequeueAtUnknownID verifies NextRequeueAt reads in-memory
// schedule state without a store lookup: an id that does not exist (or belongs
// to another kind) is simply unscheduled, so it reports the zero time and no
// error rather than ErrNotFound.
func TestClientNextRequeueAtUnknownID(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	at := client.NextRequeueAt(ctx, 999)
	assert.True(t, at.IsZero(), "unknown id must report the zero time, got %s", at)
}

// TestClientNextRequeueAtNoController verifies a client-only kind (no registered
// controller, hence no reconcile loop to schedule against) reports the zero time
// rather than an error.
func TestClientNextRequeueAtNoController(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	at := client.NextRequeueAt(ctx, obj.ID)
	assert.True(t, at.IsZero(), "client-only kind must report the zero time, got %s", at)
}
