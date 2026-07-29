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

	"github.com/amorey/beehive/internal/storeapi"
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
	_, err = store.ObjectsCreate(ctx, &RawObject{
		Group: clientTestGK.Group, Kind: clientTestGK.Kind, Spec: []byte(`not json`),
	})
	require.NoError(t, err)
	good, err := store.ObjectsCreate(ctx, &RawObject{
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

	_, err = store.ObjectsCreate(ctx, &RawObject{
		Group: clientTestGK.Group, Kind: clientTestGK.Kind, Spec: []byte(`not json`),
	})
	require.NoError(t, err)
	good, err := store.ObjectsCreate(ctx, &RawObject{
		Group: clientTestGK.Group, Kind: clientTestGK.Kind, Spec: []byte(`{"Val":"good"}`),
	})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	ch, err := client.ObjectsWatchList(ctx)
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

// A finalizer on a kind with no controller is unclearable, not merely useless:
// FinalizersDelete is a ControllerClient method folded to the caller's own kind, so
// nothing in the process can remove it, gcCollect returns early while it stands,
// and the row's owned_by edge RESTRICT-blocks its owner's delete forever. Rejecting
// at create is the only point where the mistake is still cheap — the symptom
// otherwise surfaces much later, and as an unrelated-looking stuck delete on the
// *owner*.
func TestClientCreateRejectsFinalizersOnUnregisteredKind(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	_, err = client.Create(ctx, cSpec{Val: "a"}, WithFinalizers("cleanup"))

	require.ErrorIs(t, err, ErrInvalidOption)
	assert.Contains(t, err.Error(), "WithFinalizers")
	// Rejected before any store work, so there is no row to collect either.
	objs, listErr := client.List(ctx)
	require.NoError(t, listErr)
	assert.Empty(t, objs, "the create wrote nothing")
}

// GetOrCreate takes the same options, so it makes the same check — on the branch
// that would actually insert and on the one that would not, since resolving
// happens before the slug lookup.
func TestClientGetOrCreateRejectsFinalizersOnUnregisteredKind(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	_, created, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "a"}, WithFinalizers("cleanup"))

	require.ErrorIs(t, err, ErrInvalidOption)
	assert.False(t, created)
}

// The check is gated on the option being used, so an ordinary create on a
// client-only kind stays legal — client-only kinds are a supported shape, and only
// the finalizer makes one uncollectable.
func TestClientCreateWithoutFinalizersAllowsUnregisteredKind(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	obj, err := client.Create(ctx, cSpec{Val: "a"})

	require.NoError(t, err)
	assert.Empty(t, obj.Finalizers)
}

func TestClientCreateWithOptions(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	registerNoop[cSpec, cStatus](t, bh, clientTestGK) // WithFinalizers below needs it

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
	refs, err := store.EdgesListIncoming(ctx, owner.ID, RelationOwnedBy)
	require.NoError(t, err)
	require.Len(t, refs, 1)
	assert.Equal(t, child.ID, refs[0].ID)
}

func TestClientCreateOwnerRefError(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	// The owner must exist: the ref's foreign key rejects a dangling owner, and
	// Within rolls the half-made child back with it.
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, err = client.Create(ctx, cSpec{Val: "child"}, WithOwner(9999))
	require.Error(t, err)

	objs, err := client.List(ctx)
	require.NoError(t, err)
	assert.Empty(t, objs)
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

func TestClientGetOrCreateCreates(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, created, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "a"})
	require.NoError(t, err)
	assert.True(t, created)
	assert.NotZero(t, obj.ID)
	require.NotNil(t, obj.Slug)
	assert.Equal(t, "w1", *obj.Slug)
	assert.Equal(t, int64(1), obj.Generation)
	assert.Equal(t, "a", obj.Spec.Val)

	got, err := client.GetBySlug(ctx, "w1")
	require.NoError(t, err)
	assert.Equal(t, obj.ID, got.ID)
}

func TestClientGetOrCreateReturnsExisting(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	first, _, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "a"})
	require.NoError(t, err)

	// A different spec must not touch the existing row: no update, no bump.
	second, created, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "b"})
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, first.ID, second.ID)
	assert.Equal(t, first.Generation, second.Generation)
	assert.Equal(t, "a", second.Spec.Val)
}

func TestClientGetOrCreateReturnsDeletionPending(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	registerNoop[cSpec, cStatus](t, bh, clientTestGK) // WithFinalizers below needs it

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	// The finalizer keeps the tombstone around after Delete, so the slug is still
	// held by a deletion-pending row when GetOrCreate runs.
	orig, err := client.Create(ctx, cSpec{Val: "a"}, WithSlug("w1"), WithFinalizers("test/hold"))
	require.NoError(t, err)
	require.NoError(t, client.Delete(ctx, orig.ID))

	obj, created, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "b"})
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, orig.ID, obj.ID)
	assert.NotNil(t, obj.DeletionRequestedAt)
}

// GetOrCreate's create branch leaves the new object unsettled — which is what the
// owed pass drains — while its found branch writes nothing at all, so an
// existing row keeps whatever settled state it had. That asymmetry is the whole
// contract: GetOrCreate never mutates a row it did not create.
func TestClientGetOrCreateOwesAPassOnlyOnCreate(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, created, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "a"})
	require.NoError(t, err)
	require.True(t, created)
	assert.Equal(t, []ObjectID{obj.ID}, unsettledIDs(t, store), "a new object is owed its first pass")

	// Settle it, so the found branch below starts from "nothing owed".
	_, err = store.ObjectsUpdateStatus(ctx, clientTestGK, obj.ID, 1, []byte(`{}`), 0)
	require.NoError(t, err)
	require.Empty(t, unsettledIDs(t, store), "precondition: settled")

	_, created, err = client.GetOrCreate(ctx, "w1", cSpec{Val: "b"})
	require.NoError(t, err)
	require.False(t, created)
	assert.Empty(t, unsettledIDs(t, store), "returning an existing row writes nothing, so it owes nothing")
}

// badDecodeSpec marshals to valid JSON that does not unmarshal back into the
// struct, standing in for the marshal/type-param mismatch that makes a freshly
// written row undecodable on read-back.
type badDecodeSpec struct{ Val string }

func (badDecodeSpec) MarshalJSON() ([]byte, error) { return []byte(`"not-an-object"`), nil }

// TestClientGetOrCreateRollsBackOnDecodeError pins validate-before-commit: the new
// row is decoded inside the Within, so bytes that don't round-trip roll the insert
// back instead of committing a poison row. The call returns (nil, false, err) —
// created=false because nothing landed — and the slug is left free, so a retry does
// not hit a spurious UNIQUE on a phantom row.
func TestClientGetOrCreateRollsBackOnDecodeError(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	gk := GroupKind{Kind: "BadDecode"}
	client := NewClient[badDecodeSpec, cStatus](bh, gk)

	obj, created, err := client.GetOrCreate(ctx, "w1", badDecodeSpec{Val: "a"})
	require.Error(t, err, "the new row's bytes must fail to decode")
	assert.Nil(t, obj)
	assert.False(t, created, "a rolled-back create must report created=false")

	// Nothing committed: the slug is still absent, so a second attempt takes the
	// create branch again (not the found branch) and likewise rolls back.
	_, err = store.ObjectsGetBySlug(ctx, gk, "w1")
	require.ErrorIs(t, err, ErrNotFound, "the poison row must not have committed")
	_, created, err = client.GetOrCreate(ctx, "w1", badDecodeSpec{Val: "b"})
	require.Error(t, err)
	assert.False(t, created, "still nothing created")
}

// TestClientCreateRollsBackOnDecodeError is Create's counterpart: an undecodable
// new row is rolled back inside the Within, so the row never commits and the
// post-commit wake (discarded on rollback) never enqueues the reconciler. Register
// without Start leaves a queue nothing drains, so a stray enqueue would be visible.
func TestClientCreateRollsBackOnDecodeError(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	gk := GroupKind{Kind: "BadDecode"}
	_, err = Register(bh, gk, &noopController[badDecodeSpec, cStatus]{})
	require.NoError(t, err)
	r, ok := bh.reconcilerFor(gk)
	require.True(t, ok)

	client := NewClient[badDecodeSpec, cStatus](bh, gk)
	obj, err := client.Create(ctx, badDecodeSpec{Val: "a"}, WithSlug("w1"))
	require.Error(t, err, "the new row's bytes must fail to decode")
	assert.Nil(t, obj)
	assert.Empty(t, queuedIDs(r.work), "a rolled-back create must not wake the reconciler")

	_, err = store.ObjectsGetBySlug(ctx, gk, "w1")
	require.ErrorIs(t, err, ErrNotFound, "the poison row must not have committed")
}

// conditionalBadSpec round-trips normally, but when Bad is set it marshals to bytes
// that won't unmarshal back — letting a test hold a good row and then attempt an
// undecodable update to it, which badDecodeSpec (always poison) can't express.
type conditionalBadSpec struct {
	Val string
	Bad bool
}

func (s conditionalBadSpec) MarshalJSON() ([]byte, error) {
	if s.Bad {
		return []byte(`"not-an-object"`), nil
	}
	type alias conditionalBadSpec // avoid recursing back into MarshalJSON
	return json.Marshal(alias(s))
}

// TestClientUpdateRollsBackOnDecodeError pins validate-before-commit on Update: an
// update to a spec that marshals but does not round-trip is decoded inside the tx,
// so it rolls back — the prior good spec (and generation) survive, and no wake fires.
func TestClientUpdateRollsBackOnDecodeError(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	gk := GroupKind{Kind: "CondBad"}
	_, err = Register(bh, gk, &noopController[conditionalBadSpec, cStatus]{})
	require.NoError(t, err)
	r, ok := bh.reconcilerFor(gk)
	require.True(t, ok)
	client := NewClient[conditionalBadSpec, cStatus](bh, gk)

	orig, err := client.Create(ctx, conditionalBadSpec{Val: "good"})
	require.NoError(t, err)
	drainQueue(r.work)
	require.Empty(t, queuedIDs(r.work), "precondition: queue drained")

	_, err = client.Update(ctx, orig.ID, conditionalBadSpec{Val: "bad", Bad: true})
	require.Error(t, err, "the new spec's bytes must fail to decode")
	assert.Empty(t, queuedIDs(r.work), "a rolled-back update must not wake the reconciler")

	got, err := client.Get(ctx, orig.ID)
	require.NoError(t, err, "the prior good spec must still decode")
	assert.Equal(t, "good", got.Spec.Val, "the spec update must have rolled back")
	assert.Equal(t, orig.Generation, got.Generation, "no generation bump on a rolled-back update")
}

// TestClientCreateOrUpdateRollsBackOnDecodeError pins validate-before-commit on both
// of CreateOrUpdate's branches: an undecodable update keeps the prior good spec, and
// an undecodable create leaves the slug free (no committed poison row).
func TestClientCreateOrUpdateRollsBackOnDecodeError(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	gk := GroupKind{Kind: "CondBad"}
	client := NewClient[conditionalBadSpec, cStatus](bh, gk)

	// Update branch: an existing good row, updated to an undecodable spec, rolls back.
	orig, err := client.CreateOrUpdate(ctx, "w1", conditionalBadSpec{Val: "good"})
	require.NoError(t, err)
	_, err = client.CreateOrUpdate(ctx, "w1", conditionalBadSpec{Val: "bad", Bad: true})
	require.Error(t, err)
	got, err := client.GetBySlug(ctx, "w1")
	require.NoError(t, err, "the prior good spec must still decode")
	assert.Equal(t, "good", got.Spec.Val, "the update must have rolled back")
	assert.Equal(t, orig.Generation, got.Generation, "no generation bump on a rolled-back update")

	// Create branch: an absent slug written with an undecodable spec rolls back,
	// leaving the slug free for a later good create.
	_, err = client.CreateOrUpdate(ctx, "w2", conditionalBadSpec{Val: "bad", Bad: true})
	require.Error(t, err)
	_, err = store.ObjectsGetBySlug(ctx, gk, "w2")
	require.ErrorIs(t, err, ErrNotFound, "the poison row must not have committed")
}

// TestClientWithOnCreateFiresOnlyOnCreate pins WithOnCreate to the create branch:
// Create always runs it, GetOrCreate runs it when it inserts but not when it
// returns an existing row.
func TestClientWithOnCreateFiresOnlyOnCreate(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	var calls int
	onCreate := WithOnCreate(func(context.Context) { calls++ })

	_, err = client.Create(ctx, cSpec{Val: "a"}, WithSlug("c1"), onCreate)
	require.NoError(t, err)
	assert.Equal(t, 1, calls, "Create must run onCreate")

	_, created, err := client.GetOrCreate(ctx, "g1", cSpec{Val: "a"}, onCreate)
	require.NoError(t, err)
	require.True(t, created)
	assert.Equal(t, 2, calls, "GetOrCreate create branch must run onCreate")

	_, created, err = client.GetOrCreate(ctx, "g1", cSpec{Val: "b"}, onCreate)
	require.NoError(t, err)
	require.False(t, created)
	assert.Equal(t, 2, calls, "GetOrCreate found branch must not run onCreate")
}

// TestClientWithOnCreateFiresOnlyAfterOuterCommit is why WithOnCreate exists: it
// is the commit-safe channel for create-conditional side effects. Nested in a
// ControllerClient.Within it must not run until the outer transaction commits, and
// must not run at all on rollback — unlike the synchronous created bool, which is
// already set inside the transaction.
func TestClientWithOnCreateFiresOnlyAfterOuterCommit(t *testing.T) {
	writes := []struct {
		name  string
		write func(ctx context.Context, c Client[cSpec, cStatus], onCreate Option) error
	}{
		{"Create", func(ctx context.Context, c Client[cSpec, cStatus], onCreate Option) error {
			_, err := c.Create(ctx, cSpec{Val: "b"}, onCreate)
			return err
		}},
		{"GetOrCreate", func(ctx context.Context, c Client[cSpec, cStatus], onCreate Option) error {
			_, _, err := c.GetOrCreate(ctx, "new", cSpec{Val: "b"}, onCreate)
			return err
		}},
	}

	for _, w := range writes {
		t.Run(w.name, func(t *testing.T) {
			runCommitRollback(t, func(t *testing.T, commit bool) {
				ctx := context.Background()
				bh, err := New(newClientTestStore(t))
				require.NoError(t, err)
				client := NewClient[cSpec, cStatus](bh, clientTestGK)

				var calls int
				onCreate := WithOnCreate(func(context.Context) { calls++ })

				cc := &controllerClientImpl[cStatus]{bh: bh, gk: clientTestGK}
				err = cc.Within(ctx, func(ctx context.Context) error {
					require.NoError(t, w.write(ctx, client, onCreate))
					assert.Zero(t, calls, "onCreate must wait for the outer commit")
					if !commit {
						return errBoom
					}
					return nil
				})

				if commit {
					require.NoError(t, err)
					assert.Equal(t, 1, calls, "a committed create must run onCreate")
				} else {
					require.ErrorIs(t, err, errBoom)
					assert.Zero(t, calls, "a rolled-back create must not run onCreate")
				}
			})
		})
	}
}

// TestClientWritesAreOwedOnlyAfterOuterCommit pins what a write leaves behind for
// the owed pass to find. Nothing is scheduled at write time any more: a spec
// write bumps the generation, which is exactly what makes the object unsettled,
// and the unsettled listing is what the owed-pass tick drains.
//
// The rollback case is the point. A write nested in ControllerClient.Within joins
// that transaction, so a rolled-back write must leave no trace at all — and here
// that is structural rather than something the client has to remember to skip:
// the row is gone, so no listing can name it.
func TestClientWritesAreOwedOnlyAfterOuterCommit(t *testing.T) {
	writes := []struct {
		name  string
		write func(ctx context.Context, c Client[cSpec, cStatus], seeded ObjectID) error
	}{
		{"Create", func(ctx context.Context, c Client[cSpec, cStatus], _ ObjectID) error {
			_, err := c.Create(ctx, cSpec{Val: "b"})
			return err
		}},
		{"CreateOrUpdate", func(ctx context.Context, c Client[cSpec, cStatus], _ ObjectID) error {
			_, err := c.CreateOrUpdate(ctx, "new", cSpec{Val: "b"})
			return err
		}},
		{"GetOrCreate", func(ctx context.Context, c Client[cSpec, cStatus], _ ObjectID) error {
			_, _, err := c.GetOrCreate(ctx, "new", cSpec{Val: "b"})
			return err
		}},
		{"Update", func(ctx context.Context, c Client[cSpec, cStatus], seeded ObjectID) error {
			_, err := c.Update(ctx, seeded, cSpec{Val: "b"})
			return err
		}},
	}

	for _, w := range writes {
		t.Run(w.name, func(t *testing.T) {
			runCommitRollback(t, func(t *testing.T, commit bool) {
				ctx := context.Background()
				store := newClientTestStore(t)
				bh, err := New(store)
				require.NoError(t, err)
				// Registered but never started, so nothing drains what the writes owe.
				_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
				require.NoError(t, err)

				client := NewClient[cSpec, cStatus](bh, clientTestGK)
				seeded, err := client.Create(ctx, cSpec{Val: "a"}, WithSlug("seed"))
				require.NoError(t, err)
				// Settle the seed so its own unconverged spec doesn't mask the write's.
				_, err = store.ObjectsUpdateStatus(ctx, clientTestGK, seeded.ID, 1, []byte(`{}`), 0)
				require.NoError(t, err)
				require.Empty(t, unsettledIDs(t, store), "precondition: nothing owed")

				cc := &controllerClientImpl[cStatus]{bh: bh, gk: clientTestGK}
				err = cc.Within(ctx, func(ctx context.Context) error {
					require.NoError(t, w.write(ctx, client, seeded.ID))
					if !commit {
						return errBoom
					}
					return nil
				})

				if commit {
					require.NoError(t, err)
					assert.NotEmpty(t, unsettledIDs(t, store), "a committed write leaves the object owed a pass")
				} else {
					require.ErrorIs(t, err, errBoom)
					assert.Empty(t, unsettledIDs(t, store), "a rolled-back write leaves nothing owed")
				}
			})
		})
	}
}

// TestClientNoOpUpdateOwesNothing pins the follow-up to a real row change.
// ObjectsUpdateSpec suppresses an identical-bytes write entirely — no generation
// bump, no resource_version bump — so a settled object stays settled and no
// listing reports it. It also closes a spin: a controller that idempotently
// re-applies its own kind's spec each pass would otherwise re-owe itself forever.
func TestClientNoOpUpdateOwesNothing(t *testing.T) {
	writes := []struct {
		name  string
		write func(ctx context.Context, c Client[cSpec, cStatus], id ObjectID) error
	}{
		{"Update", func(ctx context.Context, c Client[cSpec, cStatus], id ObjectID) error {
			_, err := c.Update(ctx, id, cSpec{Val: "a"})
			return err
		}},
		{"CreateOrUpdate", func(ctx context.Context, c Client[cSpec, cStatus], _ ObjectID) error {
			_, err := c.CreateOrUpdate(ctx, "w1", cSpec{Val: "a"})
			return err
		}},
	}

	for _, w := range writes {
		t.Run(w.name, func(t *testing.T) {
			ctx := context.Background()
			store := newClientTestStore(t)
			bh, err := New(store)
			require.NoError(t, err)
			_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
			require.NoError(t, err)

			client := NewClient[cSpec, cStatus](bh, clientTestGK)
			obj, err := client.Create(ctx, cSpec{Val: "a"}, WithSlug("w1"))
			require.NoError(t, err)
			// Settle it, so anything the writes below owe is theirs.
			_, err = store.ObjectsUpdateStatus(ctx, clientTestGK, obj.ID, 1, []byte(`{}`), 0)
			require.NoError(t, err)
			require.Empty(t, unsettledIDs(t, store), "precondition: nothing owed")

			require.NoError(t, w.write(ctx, client, obj.ID))
			assert.Empty(t, unsettledIDs(t, store), "re-applying the identical spec leaves it settled")

			// A real change still unsettles it, so the suppression is scoped to the no-op.
			_, err = client.Update(ctx, obj.ID, cSpec{Val: "b"})
			require.NoError(t, err)
			assert.Equal(t, []ObjectID{obj.ID}, unsettledIDs(t, store), "a real spec change is owed a pass")
		})
	}
}

// TestClientDeleteAdvancesGCOnlyAfterOuterCommit covers Delete, whose follow-up is
// a collect rather than a reconcile. It must wait for the outer commit for the same
// reason every other write's wake does: a reconciler woken mid-transaction would
// either read a row whose tombstone is not visible yet, or be woken for a deletion
// the caller then rolls back.
func TestClientDeleteIsCollectableOnlyAfterOuterCommit(t *testing.T) {
	runCommitRollback(t, func(t *testing.T, commit bool) {
		ctx := context.Background()
		store := newClientTestStore(t)
		bh, err := New(store)
		require.NoError(t, err)
		_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
		require.NoError(t, err)

		client := NewClient[cSpec, cStatus](bh, clientTestGK)
		obj, err := client.Create(ctx, cSpec{Val: "a"}, WithFinalizers("test/hold"))
		require.NoError(t, err)

		cc := &controllerClientImpl[cStatus]{bh: bh, gk: clientTestGK}
		err = cc.Within(ctx, func(ctx context.Context) error {
			require.NoError(t, client.Delete(ctx, obj.ID))
			if !commit {
				return errBoom
			}
			return nil
		})

		pending, listErr := store.DeletionRequestsList(ctx)
		require.NoError(t, listErr)
		if commit {
			require.NoError(t, err)
			require.Len(t, pending, 1, "a committed delete is in the sweeper's listing")
			assert.Equal(t, obj.ID, pending[0].ID)
		} else {
			require.ErrorIs(t, err, errBoom)
			assert.Empty(t, pending, "a rolled-back delete leaves nothing for the sweeper to find")
		}
	})
}

func TestClientGetOrCreateWithOwner(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	owner, err := client.Create(ctx, cSpec{Val: "owner"})
	require.NoError(t, err)

	child, created, err := client.GetOrCreate(ctx, "child-1", cSpec{Val: "child"}, WithOwner(owner.ID))
	require.NoError(t, err)
	require.True(t, created)

	owned, err := client.OwnedList(ctx, owner.ID)
	require.NoError(t, err)
	require.Len(t, owned, 1)
	assert.Equal(t, child.ID, owned[0].ID)
}

func TestClientGetOrCreateWithFinalizers(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	registerNoop[cSpec, cStatus](t, bh, clientTestGK) // WithFinalizers below needs it

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, created, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "a"},
		WithFinalizers("cleanup-a", "cleanup-b"))
	require.NoError(t, err)
	require.True(t, created)
	assert.Equal(t, []string{"cleanup-a", "cleanup-b"}, obj.Finalizers)

	got, err := client.GetBySlug(ctx, "w1")
	require.NoError(t, err)
	assert.Equal(t, []string{"cleanup-a", "cleanup-b"}, got.Finalizers)
}

func TestClientGetOrCreateMarshalError(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[errMarshaler, cStatus](bh, clientTestGK)
	_, created, err := client.GetOrCreate(ctx, "w1", errMarshaler{})
	require.Error(t, err)
	assert.False(t, created)
}

func TestClientGetOrCreateStoreError(t *testing.T) {
	ctx := context.Background()
	bh, err := New(&slugErrorStore{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, created, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "a"})
	require.ErrorIs(t, err, errBoom)
	assert.False(t, created)
}

func TestClientGetOrCreateOptionError(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	badOpt := func(any) error { return errBoom }

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, created, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "a"}, badOpt)
	require.ErrorIs(t, err, errBoom)
	assert.False(t, created)
}

func TestClientGetOrCreateRejectsWithSlug(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	// The slug is positional, so WithSlug can only contradict it — a silent drop
	// would land the row under "w1" and strand a later GetBySlug("other").
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, created, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "a"}, WithSlug("other"))
	require.ErrorIs(t, err, ErrConflictingOption)
	assert.False(t, created)

	// The call is rejected before any write: neither slug exists afterwards.
	_, err = client.GetBySlug(ctx, "w1")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = client.GetBySlug(ctx, "other")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestClientGetOrCreateCreateError(t *testing.T) {
	ctx := context.Background()
	bh, err := New(&createErrorStore{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, created, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "a"})
	require.ErrorIs(t, err, errBoom)
	assert.False(t, created)
}

func TestClientGetOrCreateRawToTypedError(t *testing.T) {
	ctx := context.Background()
	bh, err := New(&createOrUpdateBadJSONStore{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, created, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "a"})
	require.Error(t, err)
	// Decode runs inside the Within now, so an undecodable new row rolls back rather
	// than committing; created is set only after a successful decode, so it reports
	// false for the row the transaction discarded.
	assert.False(t, created)
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
	raw, err := store.ObjectsCreate(ctx, &RawObject{
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
	ch, err := client.ObjectsWatch(ctx, 9999)
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
	// the default full pass is enabled, so the client-only object isn't collected
	// synchronously by Delete — the idle sweeper is its backstop.
	got, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	assert.NotNil(t, got.DeletionRequestedAt)
}

func TestClientDeleteBySlugDeletes(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, err = client.Create(ctx, cSpec{}, WithSlug("w1"))
	require.NoError(t, err)

	require.NoError(t, client.DeleteBySlug(ctx, "w1"))

	// As in TestClientDelete, the object lingers marked for deletion rather than
	// being collected synchronously.
	got, err := client.GetBySlug(ctx, "w1")
	require.NoError(t, err)
	assert.NotNil(t, got.DeletionRequestedAt)
}

func TestClientDeleteBySlugNotFoundIsNil(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	assert.NoError(t, client.DeleteBySlug(ctx, "never-created"))
}

func TestClientDeleteBySlugIdempotent(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, err = client.Create(ctx, cSpec{}, WithSlug("w1"))
	require.NoError(t, err)

	require.NoError(t, client.DeleteBySlug(ctx, "w1"))
	assert.NoError(t, client.DeleteBySlug(ctx, "w1"))
}

// A row held deletion-pending by a finalizer must absorb a second DeleteBySlug
// as a pure no-op: no error, and no second state change for watchers to see.
func TestClientDeleteBySlugAlreadyDeleting(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	registerNoop[cSpec, cStatus](t, bh, clientTestGK) // WithFinalizers below needs it

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{}, WithSlug("w1"), WithFinalizers("test/hold"))
	require.NoError(t, err)

	require.NoError(t, client.Delete(ctx, obj.ID))
	pending, err := client.GetBySlug(ctx, "w1")
	require.NoError(t, err)
	require.NotNil(t, pending.DeletionRequestedAt)

	require.NoError(t, client.DeleteBySlug(ctx, "w1"))

	got, err := client.GetBySlug(ctx, "w1")
	require.NoError(t, err)
	assert.Equal(t, pending.ID, got.ID)
	assert.Equal(t, pending.DeletionRequestedAt, got.DeletionRequestedAt)
	// DeletionRequestsCreate reports no change, so no write and no Modified event: the
	// resource_version is the tell.
	assert.Equal(t, pending.ResourceVersion, got.ResourceVersion)
}

// the sweeper's registered-kind branch: the object must be handed to its controller
// to clear finalizers, the one part of Delete's tail DeleteBySlug still runs itself
// now that the store resolves and marks in one statement. A slug that matches no
// row must wake nobody.
func TestClientDeleteBySlugMarksForCollection(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{}, WithSlug("w1"), WithFinalizers("test/hold"))
	require.NoError(t, err)

	require.NoError(t, client.DeleteBySlug(ctx, "w1"))
	pending, err := store.DeletionRequestsList(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1, "the mark is the whole signal: it puts the row in the sweeper's listing")
	assert.Equal(t, obj.ID, pending[0].ID)

	require.NoError(t, client.DeleteBySlug(ctx, "absent"))
	pending, err = store.DeletionRequestsList(ctx)
	require.NoError(t, err)
	assert.Len(t, pending, 1, "an unresolved slug marks nothing")
}

// A slug is per-kind, so another kind's row holding the same slug is invisible:
// DeleteBySlug reports success (nothing of this kind to delete) and leaves it be.
func TestClientDeleteBySlugKindScoped(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	widgets := NewClient[cSpec, cStatus](bh, clientTestGK)
	gadgets := NewClient[cSpec, cStatus](bh, GroupKind{Kind: "Gadget"})

	w, err := widgets.Create(ctx, cSpec{Val: "v1"}, WithSlug("shared"))
	require.NoError(t, err)

	require.NoError(t, gadgets.DeleteBySlug(ctx, "shared"))

	got, err := widgets.Get(ctx, w.ID)
	require.NoError(t, err)
	assert.Nil(t, got.DeletionRequestedAt, "the Widget must be untouched")
}

func TestClientDeleteBySlugStoreError(t *testing.T) {
	ctx := context.Background()
	bh, err := New(&requestDeletionBySlugErrorStore{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	require.ErrorIs(t, client.DeleteBySlug(ctx, "w1"), errBoom)
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

// createBadJSONStore returns bad JSON from ObjectsCreate so rawToTyped fails.
type createBadJSONStore struct {
	fakeStore
}

func (s *createBadJSONStore) ObjectsCreate(_ context.Context, _ *RawObject) (*RawObject, error) {
	return &RawObject{ID: 1, Spec: []byte("not-json")}, nil
}

// errorObjectsCreateStore returns an error from ObjectsCreate.
type errorObjectsCreateStore struct {
	fakeStore
}

func (s *errorObjectsCreateStore) ObjectsCreate(_ context.Context, _ *RawObject) (*RawObject, error) {
	return nil, errBoom
}

// updateBadJSONStore returns bad JSON from ObjectsUpdateSpec so rawToTyped fails.
type updateBadJSONStore struct {
	fakeStore
}

func (s *updateBadJSONStore) ObjectsUpdateSpec(_ context.Context, _ GroupKind, _ ObjectID, _ []byte, _ int) (*RawObject, bool, error) {
	return &RawObject{ID: 1, Spec: []byte("not-json")}, true, nil
}

// errorUpdateSpecStore returns an error from ObjectsUpdateSpec.
type errorUpdateSpecStore struct {
	fakeStore
}

func (s *errorUpdateSpecStore) ObjectsUpdateSpec(_ context.Context, _ GroupKind, _ ObjectID, _ []byte, _ int) (*RawObject, bool, error) {
	return nil, false, errBoom
}

// slugErrorStore returns a non-NotFound error from ObjectsGetBySlug, driving
// CreateOrUpdate's default (read-error) branch.
type slugErrorStore struct {
	fakeStore
}

func (s *slugErrorStore) ObjectsGetBySlug(_ context.Context, _ GroupKind, _ string) (*RawObject, error) {
	return nil, errBoom
}

// requestDeletionBySlugErrorStore fails the slug-keyed deletion request.
type requestDeletionBySlugErrorStore struct {
	fakeStore
}

func (s *requestDeletionBySlugErrorStore) DeletionRequestsCreateBySlug(_ context.Context, _ GroupKind, _ string) (*RawObject, bool, error) {
	return nil, false, errBoom
}

// createOrUpdateBadJSONStore drives CreateOrUpdate's rawToTyped error path: the
// slug is absent (NotFound) so the create branch runs, and ObjectsCreate returns
// undecodable spec bytes.
type createOrUpdateBadJSONStore struct {
	fakeStore
}

func (s *createOrUpdateBadJSONStore) ObjectsGetBySlug(_ context.Context, _ GroupKind, _ string) (*RawObject, error) {
	return nil, ErrNotFound
}

func (s *createOrUpdateBadJSONStore) ObjectsCreate(_ context.Context, _ *RawObject) (*RawObject, error) {
	return &RawObject{ID: 1, Spec: []byte("not-json")}, nil
}

// createErrorStore drives the create branch's write-error path: it borrows
// createOrUpdateBadJSONStore's absent-slug lookup so the insert runs, but fails
// the insert instead of returning an undecodable row.
type createErrorStore struct {
	createOrUpdateBadJSONStore
}

func (s *createErrorStore) ObjectsCreate(_ context.Context, _ *RawObject) (*RawObject, error) {
	return nil, errBoom
}

// errorListObjectsStore returns an error from ObjectsList.
type errorListObjectsStore struct {
	fakeStore
}

func (s *errorListObjectsStore) ObjectsList(_ context.Context, _ GroupKind) ([]*RawObject, error) {
	return nil, errBoom
}

// badJSONStore is a fakeStore whose ObjectsList returns a RawObject with invalid
// spec JSON, used to drive the rawToTyped error path inside client.List.
type badJSONStore struct {
	fakeStore
	gk GroupKind
}

func (s *badJSONStore) ObjectsList(_ context.Context, _ GroupKind) ([]*RawObject, error) {
	return []*RawObject{{ID: 1, Group: s.gk.Group, Kind: s.gk.Kind, Spec: []byte("not-json")}}, nil
}

func TestClientCreateStoreError(t *testing.T) {
	bh, err := New(&errorObjectsCreateStore{})
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

// recv waits for the next value on ch, failing the test if none arrives within
// the failsafe timeout.
func recv[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed unexpectedly")
		}
		return v
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for a watch value")
		panic("unreachable")
	}
}

// assertChanClosed fails the test if ch does not close within the failsafe timeout.
func assertChanClosed[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	// Drain any buffered values, then expect close.
	deadline := time.After(testTimeout)
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
	bh, err := New(newClientTestStore(t), fast()...)
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	return bh, client
}

// TestWatchListReceivesAddedOnCreate verifies that WatchList delivers a
// Added when an object is created.
func TestWatchListReceivesAddedOnCreate(t *testing.T) {
	ctx := context.Background()
	_, client := watchTestBH(t)

	ch, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)

	obj, err := client.Create(ctx, cSpec{Val: "hello"})
	require.NoError(t, err)

	evt := recv(t, ch)
	assert.Equal(t, Added, evt.Type)
	assert.Equal(t, obj.ID, evt.Object.ID)
	assert.Equal(t, "hello", evt.Object.Spec.Val)
}

// TestWatchListReceivesModifiedOnUpdate verifies that WatchList delivers a
// Modified when an object's spec is updated.
func TestWatchListReceivesModifiedOnUpdate(t *testing.T) {
	ctx := context.Background()
	_, client := watchTestBH(t)

	// Subscribe before creating so the snapshot is empty and the first event is
	// the Modified from the Update, not an Added from the snapshot.
	ch, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)

	obj, err := client.Create(ctx, cSpec{Val: "v1"})
	require.NoError(t, err)
	// Drain the Added event from Create.
	recv(t, ch)

	_, err = client.Update(ctx, obj.ID, cSpec{Val: "v2"})
	require.NoError(t, err)

	evt := recv(t, ch)
	assert.Equal(t, Modified, evt.Type)
	assert.Equal(t, obj.ID, evt.Object.ID)
	assert.Equal(t, "v2", evt.Object.Spec.Val)
}

// TestWatchListReceivesModifiedOnDelete verifies that WatchList delivers a
// Modified (not Deleted) when deletion is requested, because the
// object still exists in the store with DeletionRequestedAt set.
func TestWatchListReceivesModifiedOnDelete(t *testing.T) {
	ctx := context.Background()
	_, client := watchTestBH(t)

	ch, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)

	obj, err := client.Create(ctx, cSpec{})
	require.NoError(t, err)
	// Drain the Added event from Create.
	recv(t, ch)

	require.NoError(t, client.Delete(ctx, obj.ID))

	evt := recv(t, ch)
	assert.Equal(t, Modified, evt.Type)
	assert.Equal(t, obj.ID, evt.Object.ID)
	assert.NotNil(t, evt.Object.DeletionRequestedAt)
}

// TestWatchListNoEventOnIdempotentDelete verifies that a second Delete call for
// an already-pending-deletion object emits no additional watch event.
func TestWatchListNoEventOnIdempotentDelete(t *testing.T) {
	ctx := context.Background()
	_, client := watchTestBH(t)

	ch, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)

	obj, err := client.Create(ctx, cSpec{})
	require.NoError(t, err)
	recv(t, ch) // drain Added

	require.NoError(t, client.Delete(ctx, obj.ID))
	recv(t, ch) // drain first Modified

	// Second Delete is idempotent. Pinned at the mechanism first: the watch emits
	// off resource_version, so a write that leaves it alone is invisible to the
	// poller no matter when it looks.
	before, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	require.NoError(t, client.Delete(ctx, obj.ID))
	after, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, before.ResourceVersion, after.ResourceVersion,
		"an idempotent delete bumped resource_version, which is what the watch emits on")

	// And on the stream: a fresh object gives the poller something it *must* report,
	// so anything the idempotent delete emitted has to show up at or before that
	// frame. Reading until the Added arrives therefore sees every stray there is.
	other, err := client.Create(ctx, cSpec{})
	require.NoError(t, err)
	evt := recv(t, ch)
	require.Equal(t, other.ID, evt.Object.ID, "unexpected event on idempotent delete: %v", evt.Type)
	assert.Equal(t, Added, evt.Type)
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

	ch, err := client.ObjectsWatch(ctx, obj1.ID)
	require.NoError(t, err)

	// Drain the initial snapshot Added event for obj1.
	snap := recv(t, ch)
	assert.Equal(t, Added, snap.Type)
	assert.Equal(t, obj1.ID, snap.Object.ID)

	// Update obj2 first — this event must not appear on ch.
	_, err = client.Update(ctx, obj2.ID, cSpec{Val: "b2"})
	require.NoError(t, err)

	// Update obj1 — this must appear.
	_, err = client.Update(ctx, obj1.ID, cSpec{Val: "a2"})
	require.NoError(t, err)

	evt := recv(t, ch)
	assert.Equal(t, obj1.ID, evt.Object.ID)
	assert.Equal(t, "a2", evt.Object.Spec.Val)
}

// TestWatchListClosesOnCtxCancel verifies that the watch channel is closed when
// the context is cancelled.
func TestWatchListClosesOnCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, client := watchTestBH(t)

	ch, err := client.ObjectsWatchList(ctx)
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

	ch, err := client.ObjectsWatch(ctx, obj.ID)
	require.NoError(t, err)

	cancel()
	assertChanClosed(t, ch)
}

// TestWatchReceivesModifiedOnStatusUpdate verifies that WatchList delivers a
// Modified when the controller calls UpdateStatus.
func TestWatchReceivesModifiedOnStatusUpdate(t *testing.T) {
	ctx := context.Background()

	// watchTestBH already registered one; we need a fresh beehive for this test.
	bh2, err := New(newClientTestStore(t), fast()...)
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
	ch, err := client2.ObjectsWatchList(ctx)
	require.NoError(t, err)

	// Drain the initial snapshot Added event.
	snap := recv(t, ch)
	assert.Equal(t, Added, snap.Type)
	assert.Equal(t, obj.ID, snap.Object.ID)

	require.NoError(t, cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"}))

	evt := recv(t, ch)
	assert.Equal(t, Modified, evt.Type)
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

	ch, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)

	// Two snapshot Added events must arrive, one per existing object.
	seen := map[ObjectID]string{}
	for range 2 {
		evt := recv(t, ch)
		assert.Equal(t, Added, evt.Type)
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

	ch, err := client.ObjectsWatch(ctx, obj.ID)
	require.NoError(t, err)

	evt := recv(t, ch)
	assert.Equal(t, Added, evt.Type)
	assert.Equal(t, obj.ID, evt.Object.ID)
	assert.Equal(t, "hello", evt.Object.Spec.Val)
}

// TestStartAfterStopErrors verifies that Beehive is a one-shot object: calling
// Start after Stop returns an error instead of silently re-driving a torn-down
// control plane.
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

	_, err = client.ObjectsWatchList(ctx)
	require.Error(t, err)

	_, err = client.ObjectsWatch(ctx, 0)
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

	got, ok, err := client.OwnersGet(ctx, child.ID)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, ObjectRef{ID: owner.ID, Group: clientTestGK.Group, Kind: clientTestGK.Kind}, got)

	// An ownerless object reports absence, not an error.
	_, ok, err = client.OwnersGet(ctx, owner.ID)
	require.NoError(t, err)
	assert.False(t, ok)

	// A missing id is not kind-validated (no scopedGet guard): it reads as
	// ownerless rather than ErrNotFound — the speed-for-isolation trade.
	_, ok, err = client.OwnersGet(ctx, 99999)
	require.NoError(t, err)
	assert.False(t, ok)
}

// TestClientListDependentsIncludesSelfEdge guards the wake guard against being
// re-implemented one layer down. Skipping the self-edge is the waker's policy,
// not the store's: filtering from_id = to_id out of EdgesListIncoming would also
// suppress the wake, and would look like a tidier fix, but that call backs the
// read API — so a self-dependency would silently vanish from DependentsList and
// from the LoadDependents eager load. GC would not notice (it reads edges through
// EdgesHasIncoming and EdgesDeleteFinalizingDependsOn, not this call), which is
// what makes the mis-implementation quiet: only the read surface changes.
func TestClientListDependentsIncludesSelfEdge(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	a, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)
	require.NoError(t, addEdge(ctx, store, a.ID, a.ID, RelationDependsOn))

	dependents, err := client.DependentsList(ctx, a.ID)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{a.ID}, objectRefIDs(dependents), "a self-dependency is still a dependency")

	got, err := client.Get(ctx, a.ID, LoadDependents())
	require.NoError(t, err)
	loaded, err := got.Dependents()
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{a.ID}, objectRefIDs(loaded), "and the eager load sees it too")
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
	require.NoError(t, addEdge(ctx, store, a.ID, b.ID, RelationDependsOn))
	require.NoError(t, addEdge(ctx, store, a.ID, c.ID, RelationDependsOn))

	deps, err := client.DependenciesList(ctx, a.ID)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{b.ID, c.ID}, objectRefIDs(deps))

	// b's dependents include a.
	dependents, err := client.DependentsList(ctx, b.ID)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{a.ID}, objectRefIDs(dependents))

	// No edges -> empty, no error.
	none, err := client.DependenciesList(ctx, b.ID)
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

	owned, err := client.OwnedList(ctx, owner.ID)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{c1.ID, c2.ID}, objectRefIDs(owned))

	// A child owns nothing -> empty, no error.
	none, err := client.OwnedList(ctx, c1.ID)
	require.NoError(t, err)
	assert.Empty(t, none)
}

// ownedObjectsFixture builds an owner of kind Owner plus children of two kinds:
// two Widgets and one Gadget, all owned by that owner — the multi-kind shape
// OwnedObjectsList has to filter down. It returns the two clients, the owner id,
// and the widget children in id order.
func ownedObjectsFixture(t *testing.T) (context.Context, Client[cSpec, cStatus], Client[cSpec, cStatus], ObjectID, []*Object[cSpec, cStatus]) {
	t.Helper()
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	// One consumer of this fixture creates a child WithFinalizers, which is legal
	// only on a registered kind.
	registerNoop[cSpec, cStatus](t, bh, clientTestGK)

	owners := NewClient[cSpec, cStatus](bh, GroupKind{Kind: "Owner"})
	widgets := NewClient[cSpec, cStatus](bh, clientTestGK)
	gadgets := NewClient[cSpec, cStatus](bh, GroupKind{Kind: "Gadget"})

	owner, err := owners.Create(ctx, cSpec{Val: "owner"})
	require.NoError(t, err)
	w1, err := widgets.Create(ctx, cSpec{Val: "w1"}, WithOwner(owner.ID))
	require.NoError(t, err)
	w2, err := widgets.Create(ctx, cSpec{Val: "w2"}, WithOwner(owner.ID))
	require.NoError(t, err)
	_, err = gadgets.Create(ctx, cSpec{Val: "g1"}, WithOwner(owner.ID))
	require.NoError(t, err)

	return ctx, owners, widgets, owner.ID, []*Object[cSpec, cStatus]{w1, w2}
}

func TestClientListOwnedObjectsReturnsTypedChildren(t *testing.T) {
	ctx, _, widgets, ownerID, children := ownedObjectsFixture(t)

	got, err := widgets.OwnedObjectsList(ctx, ownerID)
	require.NoError(t, err)
	require.Len(t, got, 2, "the Gadget child belongs to another kind")
	// Ordered by id, decoded, and kind-scoped: no Gadget in sight.
	assert.Equal(t, []ObjectID{children[0].ID, children[1].ID}, []ObjectID{got[0].ID, got[1].ID})
	assert.Equal(t, "w1", got[0].Spec.Val)
	assert.Equal(t, "w2", got[1].Spec.Val)
	assert.Equal(t, clientTestGK.Kind, got[0].Kind)
}

// TestClientListOwnedObjectsLoads covers the LoadOptions passing through to the
// same batched loader List uses: without them the children are unloaded.
func TestClientListOwnedObjectsLoads(t *testing.T) {
	ctx, _, widgets, ownerID, _ := ownedObjectsFixture(t)

	bare, err := widgets.OwnedObjectsList(ctx, ownerID)
	require.NoError(t, err)
	require.Len(t, bare, 2)
	_, _, err = bare[0].Owner()
	assert.ErrorIs(t, err, ErrNotLoaded, "no load option -> nothing loaded")

	got, err := widgets.OwnedObjectsList(ctx, ownerID, LoadOwner())
	require.NoError(t, err)
	require.Len(t, got, 2)
	for _, child := range got {
		owner, ok, err := child.Owner()
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, ownerID, owner.ID)
	}
}

func TestClientListOwnedObjectsEmpty(t *testing.T) {
	ctx, _, widgets, _, children := ownedObjectsFixture(t)

	// A child owns nothing, so it has no children of this kind.
	got, err := widgets.OwnedObjectsList(ctx, children[0].ID)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestClientListOwnedObjectsIncludesDeletionPending(t *testing.T) {
	ctx, owners, widgets, _, _ := ownedObjectsFixture(t)

	// A second owner, so the fixture's children don't crowd the assertion.
	owner, err := owners.Create(ctx, cSpec{Val: "owner2"})
	require.NoError(t, err)
	// The finalizer holds the row after Delete, leaving it deletion-pending.
	child, err := widgets.Create(ctx, cSpec{Val: "w1"},
		WithOwner(owner.ID), WithFinalizers("test/hold"))
	require.NoError(t, err)
	require.NoError(t, widgets.Delete(ctx, child.ID))

	got, err := widgets.OwnedObjectsList(ctx, owner.ID)
	require.NoError(t, err)
	require.Len(t, got, 1, "deletion-pending children are included, as in OwnedList")
	assert.Equal(t, child.ID, got[0].ID)
	assert.NotNil(t, got[0].DeletionRequestedAt)
}

func TestClientListOwnedObjectsUnknownOwner(t *testing.T) {
	ctx, _, widgets, _, _ := ownedObjectsFixture(t)

	// Like OwnedList, it reads edges: a missing owner is empty, not ErrNotFound.
	got, err := widgets.OwnedObjectsList(ctx, 99999)
	require.NoError(t, err)
	assert.Empty(t, got)
}

// ownedObjectsErrorStore errors on the batched owned-children read.
type ownedObjectsErrorStore struct {
	fakeStore
}

func (*ownedObjectsErrorStore) ObjectsListByIncomingEdge(context.Context, GroupKind, ObjectID, Relation) ([]*RawObject, error) {
	return nil, errBoom
}

func TestClientListOwnedObjectsStoreError(t *testing.T) {
	ctx := context.Background()
	bh, err := New(&ownedObjectsErrorStore{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	_, err = client.OwnedObjectsList(ctx, 1)
	require.ErrorIs(t, err, errBoom)
}

// ownedObjectsBadJSONStore returns one undecodable row alongside a good one,
// driving OwnedObjectsList' quarantine branch.
type ownedObjectsBadJSONStore struct {
	fakeStore
	gk GroupKind
}

func (s *ownedObjectsBadJSONStore) ObjectsListByIncomingEdge(context.Context, GroupKind, ObjectID, Relation) ([]*RawObject, error) {
	return []*RawObject{
		{ID: 1, Group: s.gk.Group, Kind: s.gk.Kind, Spec: []byte("not-json")},
		{ID: 2, Group: s.gk.Group, Kind: s.gk.Kind, Spec: []byte(`{"Val":"ok"}`)},
	}, nil
}

func TestClientListOwnedObjectsQuarantinesUndecodable(t *testing.T) {
	ctx := context.Background()
	bh, err := New(&ownedObjectsBadJSONStore{gk: clientTestGK})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	got, err := client.OwnedObjectsList(ctx, 1)
	require.NoError(t, err, "one bad row is skipped, not fatal")
	require.Len(t, got, 1)
	assert.Equal(t, ObjectID(2), got[0].ID)
}

// ownedObjectsLoadErrorStore returns a decodable child but fails the batched ref
// read, driving OwnedObjectsList' eager-load error branch.
type ownedObjectsLoadErrorStore struct {
	fakeStore
	gk GroupKind
}

func (s *ownedObjectsLoadErrorStore) ObjectsListByIncomingEdge(context.Context, GroupKind, ObjectID, Relation) ([]*RawObject, error) {
	return []*RawObject{{ID: 2, Group: s.gk.Group, Kind: s.gk.Kind, Spec: []byte(`{"Val":"ok"}`)}}, nil
}

func (*ownedObjectsLoadErrorStore) EdgesGroupOutgoingByID(context.Context, []ObjectID, Relation) (map[ObjectID][]ObjectRef, error) {
	return nil, errBoom
}

func TestClientListOwnedObjectsLoadError(t *testing.T) {
	ctx := context.Background()
	bh, err := New(&ownedObjectsLoadErrorStore{gk: clientTestGK})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	_, err = client.OwnedObjectsList(ctx, 1, LoadOwner())
	require.ErrorIs(t, err, errBoom)
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
	_, _, err = plain.Owner()
	assert.ErrorIs(t, err, ErrNotLoaded, "owner not loaded without LoadOwner()")

	// With it, the owner is populated in the same read.
	got, err := client.Get(ctx, child.ID, LoadOwner())
	require.NoError(t, err)
	ref, ok, err := got.Owner()
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, owner.ID, ref.ID)

	// GetBySlug honours selectors too.
	_, err = client.Create(ctx, cSpec{Val: "slugged"}, WithSlug("s1"), WithOwner(owner.ID))
	require.NoError(t, err)
	bySlug, err := client.GetBySlug(ctx, "s1", LoadOwner())
	require.NoError(t, err)
	ref, ok, err = bySlug.Owner()
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

func (s *countingStore) EdgesGroupOutgoingByID(ctx context.Context, ids []ObjectID, rel Relation) (map[ObjectID][]ObjectRef, error) {
	s.outgoingByIDs++
	return s.Store.EdgesGroupOutgoingByID(ctx, ids, rel)
}

func (s *countingStore) EdgesGroupIncomingByID(ctx context.Context, ids []ObjectID, rel Relation) (map[ObjectID][]ObjectRef, error) {
	s.incomingByIDs++
	return s.Store.EdgesGroupIncomingByID(ctx, ids, rel)
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
		ref, ok, err := o.Owner()
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
	_, err = plain.Owned()
	assert.ErrorIs(t, err, ErrNotLoaded, "owned not loaded without LoadOwned()")

	// Single-object path populates the owner's children.
	got, err := client.Get(ctx, owner.ID, LoadOwned())
	require.NoError(t, err)
	owned, err := got.Owned()
	require.NoError(t, err)
	assert.Equal(t, childIDs, objectRefIDs(owned))

	// A child owns nothing: loaded but empty.
	leaf, err := client.Get(ctx, childIDs[0], LoadOwned())
	require.NoError(t, err)
	owned, err = leaf.Owned()
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
	owned, err = byID[owner.ID].Owned()
	require.NoError(t, err)
	assert.Equal(t, childIDs, objectRefIDs(owned))
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
	require.NoError(t, addEdge(ctx, store, a.ID, b.ID, RelationDependsOn)) // a depends on b

	got, err := client.Get(ctx, a.ID, LoadDependencies(), LoadDependents())
	require.NoError(t, err)
	deps, err := got.Dependencies()
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{b.ID}, objectRefIDs(deps))
	dependents, err := got.Dependents()
	require.NoError(t, err, "loaded even though empty")
	assert.Empty(t, dependents)

	got, err = client.Get(ctx, b.ID, LoadDependents())
	require.NoError(t, err)
	dependents, err = got.Dependents()
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{a.ID}, objectRefIDs(dependents))
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
	require.NoError(t, addEdge(ctx, store, a.ID, b.ID, RelationDependsOn))

	objs, err := client.List(ctx, LoadDependencies(), LoadDependents())
	require.NoError(t, err)
	byID := map[ObjectID]*Object[cSpec, cStatus]{}
	for _, o := range objs {
		byID[o.ID] = o
	}

	deps, err := byID[a.ID].Dependencies()
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{b.ID}, objectRefIDs(deps))
	dependents, err := byID[b.ID].Dependents()
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{a.ID}, objectRefIDs(dependents))

	assert.Equal(t, 1, store.outgoingByIDs, "dependencies batched into one call")
	assert.Equal(t, 1, store.incomingByIDs, "dependents batched into one call")
}

// edgeErrorStore wraps a real store but errors on every ref-edge lookup, driving
// the error branches of the eager loaders (single + batched) and fetchOwnerRef.
type edgeErrorStore struct {
	Store
}

func (edgeErrorStore) EdgesListOutgoingByRelation(context.Context, ObjectID, Relation) ([]ObjectRef, error) {
	return nil, errBoom
}
func (edgeErrorStore) EdgesListIncoming(context.Context, ObjectID, Relation) ([]ObjectRef, error) {
	return nil, errBoom
}
func (edgeErrorStore) EdgesGroupOutgoingByID(context.Context, []ObjectID, Relation) (map[ObjectID][]ObjectRef, error) {
	return nil, errBoom
}
func (edgeErrorStore) EdgesGroupIncomingByID(context.Context, []ObjectID, Relation) (map[ObjectID][]ObjectRef, error) {
	return nil, errBoom
}

func TestEagerLoadStoreErrorsPropagate(t *testing.T) {
	ctx := context.Background()
	store := &edgeErrorStore{Store: newClientTestStore(t)}
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
	deps, err := client.DependenciesList(ctx, 99999)
	require.NoError(t, err)
	assert.Empty(t, deps)
	dependents, err := client.DependentsList(ctx, 99999)
	require.NoError(t, err)
	assert.Empty(t, dependents)
	owned, err := client.OwnedList(ctx, 99999)
	require.NoError(t, err)
	assert.Empty(t, owned)
}

// getBadJSONStore returns an undecodable spec from the scoped Get/GetBySlug
// reads, driving the decode-error branch of Client.Get / Client.GetBySlug.
type getBadJSONStore struct {
	fakeStore
	gk GroupKind
}

func (s *getBadJSONStore) ObjectsGet(context.Context, ObjectID) (*RawObject, error) {
	return &RawObject{ID: 1, Group: s.gk.Group, Kind: s.gk.Kind, Spec: []byte("not-json")}, nil
}
func (s *getBadJSONStore) ObjectsGetBySlug(context.Context, GroupKind, string) (*RawObject, error) {
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
			seeded := r.backoffNext(obj.ID)
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

// TestClientGetScheduleUnknownID verifies SchedulesGet reads in-memory schedule
// state without a store lookup: an id that does not exist (or belongs to another
// kind) is simply unscheduled, so it reads as the zero Schedule with a nil error
// rather than ErrNotFound.
func TestClientGetScheduleUnknownID(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	s, err := client.SchedulesGet(ctx, 999)
	require.NoError(t, err)
	assert.True(t, s.NextRequeueAt.IsZero(), "unknown id must read as the zero Schedule, got %s", s.NextRequeueAt)
}

// TestClientGetScheduleScheduled verifies SchedulesGet returns a Schedule carrying
// the pending delayed reconcile's fire time in NextRequeueAt.
func TestClientGetScheduleScheduled(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	// Drain the create-time enqueue so only the future schedule remains.
	r := bh.reconcilers[clientTestGK]
	drainQueue(r.work)
	r.work.addAfter(obj.ID, time.Hour)

	s, err := client.SchedulesGet(ctx, obj.ID)
	require.NoError(t, err)
	assert.True(t, s.NextRequeueAt.After(time.Now().Add(time.Minute)),
		"fire time must be ~1h out, got %s", s.NextRequeueAt)
}

// TestClientGetScheduleUnscheduled verifies SchedulesGet returns the zero-value
// Schedule (and no error) when nothing is scheduled for the id.
func TestClientGetScheduleUnscheduled(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	r := bh.reconcilers[clientTestGK]
	drainQueue(r.work)

	s, err := client.SchedulesGet(ctx, obj.ID)
	require.NoError(t, err)
	assert.True(t, s.NextRequeueAt.IsZero(), "unscheduled id must carry the zero time, got %s", s.NextRequeueAt)
}

// TestClientGetScheduleNoController verifies SchedulesGet folds a client-only kind
// (no reconcile loop to schedule against) into the zero Schedule and a nil error,
// degrading gracefully rather than erroring like the SchedulesWatch live stream.
func TestClientGetScheduleNoController(t *testing.T) {
	ctx := context.Background()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	s, err := client.SchedulesGet(ctx, obj.ID)
	require.NoError(t, err)
	assert.True(t, s.NextRequeueAt.IsZero(), "client-only kind must carry the zero time, got %s", s.NextRequeueAt)
}

// TestClientWatchScheduleSnapshot verifies SchedulesWatch delivers the current
// schedule on subscribe: an id with a pending future requeue emits that fire time
// first, before any live change.
func TestClientWatchScheduleSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	// Drain the create-time enqueue and schedule a future requeue before watching.
	r := bh.reconcilers[clientTestGK]
	drainQueue(r.work)
	r.work.addAfter(obj.ID, time.Hour)

	ch, err := client.SchedulesWatch(ctx, obj.ID)
	require.NoError(t, err)

	s := recv(t, ch)
	assert.True(t, s.NextRequeueAt.After(time.Now().Add(time.Minute)),
		"snapshot must carry the pending ~1h fire time, got %s", s.NextRequeueAt)
}

// TestClientWatchScheduleLive verifies SchedulesWatch streams reschedules live: the
// snapshot, then a future fire time, then due-now, then the unscheduled zero as the
// id moves through addAfter → requeueNow → dispatch.
func TestClientWatchScheduleLive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bh, err := New(newClientTestStore(t), fast()...)
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	r := bh.reconcilers[clientTestGK]
	drainQueue(r.work)

	ch, err := client.SchedulesWatch(ctx, obj.ID)
	require.NoError(t, err)

	// Snapshot: nothing scheduled after the drain.
	snap := recv(t, ch)
	assert.True(t, snap.NextRequeueAt.IsZero(), "snapshot must be unscheduled, got %s", snap.NextRequeueAt)

	// A future requeue: emits the fire time.
	r.work.addAfter(obj.ID, time.Hour)
	future := recv(t, ch)
	assert.True(t, future.NextRequeueAt.After(time.Now().Add(time.Minute)),
		"reschedule must emit the ~1h fire time, got %s", future.NextRequeueAt)

	// requeueNow: flips to due-now.
	r.work.requeueNow(obj.ID)
	now := recv(t, ch)
	require.False(t, now.NextRequeueAt.IsZero(), "due-now must carry a non-zero time")
	assert.False(t, now.NextRequeueAt.After(time.Now()), "requeueNow must emit due-now, got %s", now.NextRequeueAt)

	// Dispatch: the id leaves the queue, so the schedule clears to the zero time.
	_, ok := r.work.get()
	require.True(t, ok)
	cleared := recv(t, ch)
	assert.True(t, cleared.NextRequeueAt.IsZero(), "dispatch must emit the unscheduled zero, got %s", cleared.NextRequeueAt)
}

// TestClientWatchScheduleNoController verifies SchedulesWatch rejects a client-only
// kind with ErrNoController: a live stream that can never emit should say so, unlike
// the point-read SchedulesGet which degrades to the zero Schedule.
func TestClientWatchScheduleNoController(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	_, err = client.SchedulesWatch(ctx, obj.ID)
	assert.ErrorIs(t, err, ErrNoController)
}

// eventErrStore wraps a real store, failing only the event paths so object ops
// still work — drives the client's event error branches.
type eventErrStore struct {
	Store
}

func (eventErrStore) EventsList(context.Context, ObjectID, storeapi.EventQuery) ([]RawEvent, error) {
	return nil, errBoom
}
func (eventErrStore) EventsGetLatest(context.Context, ObjectID, string) (*RawEvent, error) {
	return nil, errBoom
}
func (eventErrStore) EventsSweep(context.Context, int, time.Duration) (int, error) {
	return 0, errBoom
}

// A store error from any client event read (including the eager LoadEvents paths)
// propagates to the caller.
func TestClientEventReadsPropagateStoreError(t *testing.T) {
	ctx := context.Background()
	bh, err := New(eventErrStore{newClientTestStore(t)})
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	_, err = client.EventsList(ctx, obj.ID)
	assert.ErrorIs(t, err, errBoom)
	_, _, err = client.EventsGetLatest(ctx, obj.ID, "c")
	assert.ErrorIs(t, err, errBoom)
	_, err = client.Get(ctx, obj.ID, LoadEvents())
	assert.ErrorIs(t, err, errBoom, "eager LoadEvents on Get")
	_, err = client.List(ctx, LoadEvents())
	assert.ErrorIs(t, err, errBoom, "eager LoadEvents on List")
}

// A client EventsList on an object with no runs returns an empty slice (the
// eventsFromRaw nil branch).
func TestClientListEventsEmpty(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	got, err := client.EventsList(ctx, obj.ID)
	require.NoError(t, err)
	assert.Empty(t, got)
}

// The motivating use case, end-to-end through the public API: a flapping cluster's
// connection-probe outcomes emitted via ControllerClient.EventsRecord render as the
// aggregated, newest-first timeline the health panel shows.
func TestEventsConnectionPanelTimeline(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	cc, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	cluster, err := client.Create(ctx, cSpec{Val: "prod"})
	require.NoError(t, err)

	// The prober emits one event per probe; identical consecutive outcomes coalesce.
	emit := func(typ EventType, reason, msg string, detail any, n int) {
		for i := 0; i < n; i++ {
			require.NoError(t, cc.EventsRecord(ctx, cluster.ID, EventSpec{
				Category: "connection", Type: typ, Reason: reason, Message: msg, Detail: detail,
			}))
		}
	}
	// A flapping cluster, oldest → newest.
	emit(EventNormal, "Connected", "", nil, 16)
	emit(EventWarning, "TLSHandshake", "x509: certificate expired", nil, 5)
	emit(EventNormal, "Connected", "", nil, 7)
	emit(EventWarning, "ProbeFailed", "i/o timeout", probeDetail{Endpoint: "10.0.0.1:443", LatencyMs: 5000}, 18)
	emit(EventNormal, "Connected", "", nil, 4)

	panel, err := client.EventsList(ctx, cluster.ID, WithEventCategory("connection"))
	require.NoError(t, err)

	type row struct {
		typ    EventType
		reason string
		count  int
	}
	want := []row{
		{EventNormal, "Connected", 4},
		{EventWarning, "ProbeFailed", 18},
		{EventNormal, "Connected", 7},
		{EventWarning, "TLSHandshake", 5},
		{EventNormal, "Connected", 16},
	}
	require.Len(t, panel, len(want), "one run per contiguous outcome, newest-first")
	for i, w := range want {
		assert.Equal(t, w.typ, panel[i].Type, "row %d type", i)
		assert.Equal(t, w.reason, panel[i].Reason, "row %d reason", i)
		assert.Equal(t, w.count, panel[i].Count, "row %d count", i)
		assert.False(t, panel[i].LastAt.Before(panel[i].FirstAt), "row %d window", i)
	}

	// Failure runs carry their sampled message and structured detail.
	assert.Equal(t, "i/o timeout", panel[1].Message)
	detail, err := EventDetail[probeDetail](panel[1])
	require.NoError(t, err)
	assert.Equal(t, 5000, detail.LatencyMs)
	assert.Equal(t, "x509: certificate expired", panel[3].Message)

	// The panel header — current state of the connection timeline.
	latest, ok, err := client.EventsGetLatest(ctx, cluster.ID, "connection")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "Connected", latest.Reason)
	assert.Equal(t, 4, latest.Count)
}

// Get(LoadEvents()) eager-loads the object's runs onto Object.Events(); without
// it the accessor reports ErrNotLoaded.
func TestClientGetLoadsEvents(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)
	_, err = store.EventsRecord(ctx, clientTestGK, obj.ID, RawEvent{Category: "c", Type: "Warning", Reason: "ProbeFailed"})
	require.NoError(t, err)

	plain, err := client.Get(ctx, obj.ID)
	require.NoError(t, err)
	_, err = plain.Events()
	assert.ErrorIs(t, err, ErrNotLoaded, "not loaded without LoadEvents()")

	loaded, err := client.Get(ctx, obj.ID, LoadEvents())
	require.NoError(t, err)
	evs, err := loaded.Events()
	require.NoError(t, err)
	require.Len(t, evs, 1)
	assert.Equal(t, "ProbeFailed", evs[0].Reason)
	assert.Equal(t, EventWarning, evs[0].Type)
}

// List(LoadEvents()) attaches each object's own runs.
func TestClientListLoadsEvents(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	a, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)
	b, err := client.Create(ctx, cSpec{Val: "b"})
	require.NoError(t, err)
	_, err = store.EventsRecord(ctx, clientTestGK, a.ID, RawEvent{Category: "c", Type: "Normal", Reason: "AOK"})
	require.NoError(t, err)
	_, err = store.EventsRecord(ctx, clientTestGK, b.ID, RawEvent{Category: "c", Type: "Warning", Reason: "BBad"})
	require.NoError(t, err)

	objs, err := client.List(ctx, LoadEvents())
	require.NoError(t, err)
	byReason := map[ObjectID]string{}
	for _, o := range objs {
		evs, err := o.Events()
		require.NoError(t, err)
		require.Len(t, evs, 1, "each object gets its own log")
		byReason[o.ID] = evs[0].Reason
	}
	assert.Equal(t, "AOK", byReason[a.ID])
	assert.Equal(t, "BBad", byReason[b.ID])
}

// EventsList returns an object's runs as public Events, newest-first, with the
// query options resolved into the store filter.
func TestClientListEvents(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	rec := func(cat, typ, reason string) {
		_, err := store.EventsRecord(ctx, clientTestGK, obj.ID, RawEvent{Category: cat, Type: typ, Reason: reason})
		require.NoError(t, err)
	}
	rec("connection", "Warning", "ProbeFailed")
	rec("connection", "Normal", "Connected")
	rec("sync", "Normal", "Synced")

	all, err := client.EventsList(ctx, obj.ID)
	require.NoError(t, err)
	require.Len(t, all, 3)
	assert.Equal(t, "Synced", all[0].Reason, "newest-first across categories")

	conn, err := client.EventsList(ctx, obj.ID, WithEventCategory("connection"))
	require.NoError(t, err)
	require.Len(t, conn, 2)
	assert.Equal(t, EventNormal, conn[0].Type, "newest connection run mapped to EventType")
	assert.Equal(t, "Connected", conn[0].Reason)
	assert.Equal(t, EventWarning, conn[1].Type)
}

// EventsGetLatest returns the current run in a category with ok=true, or the zero
// Event with ok=false when the timeline is empty.
func TestClientGetLatestEvent(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	_, err = store.EventsRecord(ctx, clientTestGK, obj.ID, RawEvent{Category: "connection", Type: "Normal", Reason: "Connected"})
	require.NoError(t, err)

	got, ok, err := client.EventsGetLatest(ctx, obj.ID, "connection")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "Connected", got.Reason)

	_, ok, err = client.EventsGetLatest(ctx, obj.ID, "nope")
	require.NoError(t, err)
	assert.False(t, ok, "empty timeline is ok=false")
}

// EventsWatch streams live runs as public Events and, like Watch, requires a
// registered controller.
func TestClientWatchEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newClientTestStore(t)
	bh, err := New(store)
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "x"})
	require.NoError(t, err)

	ch, err := client.EventsWatch(ctx, obj.ID)
	require.NoError(t, err)

	_, err = store.EventsRecord(ctx, clientTestGK, obj.ID, RawEvent{Category: "c", Type: "Warning", Reason: "ProbeFailed"})
	require.NoError(t, err)

	select {
	case ev := <-ch:
		assert.Equal(t, "ProbeFailed", ev.Reason)
		assert.Equal(t, EventWarning, ev.Type)
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for event")
	}

	unregistered := NewClient[cSpec, cStatus](bh, GroupKind{Kind: "Unregistered"})
	_, err = unregistered.EventsWatch(ctx, obj.ID)
	assert.Error(t, err, "EventsWatch requires a registered controller")
}

// eventFromRaw maps the store's raw event row to the public Event, translating
// the type string and detail bytes and dropping the store-only resource_version.
func TestEventFromRaw(t *testing.T) {
	first := time.Now().UTC().Add(-time.Minute)
	last := first.Add(time.Minute)
	raw := storeapi.Event{
		ID:              42,
		ObjectID:        7,
		Category:        "connection",
		Type:            "Warning",
		Reason:          "ProbeFailed",
		Message:         "i/o timeout",
		Detail:          []byte(`{"latencyMs":5000}`),
		Count:           18,
		FirstAt:         first,
		LastAt:          last,
		ResourceVersion: 99,
	}

	e := eventFromRaw(raw)
	assert.Equal(t, EventID(42), e.ID)
	assert.Equal(t, ObjectID(7), e.ObjectID)
	assert.Equal(t, "connection", e.Category)
	assert.Equal(t, EventWarning, e.Type)
	assert.Equal(t, "ProbeFailed", e.Reason)
	assert.Equal(t, "i/o timeout", e.Message)
	assert.Equal(t, json.RawMessage(`{"latencyMs":5000}`), e.Detail)
	assert.Equal(t, 18, e.Count)
	assert.Equal(t, first, e.FirstAt)
	assert.Equal(t, last, e.LastAt)
}
