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
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"log/slog"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amorey/beehive/internal/storeapi"
	"github.com/google/uuid"
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
	bh := newTestBeehive(t, store)

	// No migrator: convertBlob is identity, so the bad bytes reach json.Unmarshal,
	// which fails — exactly the shape-mismatch case the migrator seam guards.
	_, err := store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`not json`),
	})
	require.NoError(t, err)
	good, err := store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`{"Val":"good"}`),
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
	bh := newTestBeehive(t, store)
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	_, err = store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`not json`),
	})
	require.NoError(t, err)
	good, err := store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`{"Val":"good"}`),
	})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	snap, _, err := client.WatchList(ctx)
	require.NoError(t, err)

	require.Len(t, snap.Objects, 1, "the poison row is quarantined, not fatal")
	assert.Equal(t, good.ID, snap.Objects[0].ID, "the good object survives the poison one")
	assert.Equal(t, "good", snap.Objects[0].Spec.Val)
}

func newClientTestStore(t *testing.T) Store {
	t.Helper()
	s := newStore(t)
	return s
}

// errMarshaler is a type whose JSON marshaling always fails, used to exercise
// the json.Marshal error paths in Create and Update.
type errMarshaler struct{}

func (errMarshaler) MarshalJSON() ([]byte, error) { return nil, errors.New("cannot marshal") }

func TestClientCreateMarshalError(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[errMarshaler, cStatus](bh, clientTestGK)
	_, err := client.Create(ctx, "bad-marshal", errMarshaler{})
	require.Error(t, err)
}

func TestClientUpdateMarshalError(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[errMarshaler, cStatus](bh, clientTestGK)
	_, err := client.Update(ctx, 1, errMarshaler{})
	require.Error(t, err)
}

// TestClientCreateOptionError verifies Create propagates an error returned by a
// per-call Option (before any store write), so a bad option fails fast.
func TestClientCreateOptionError(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	// An option that fails when applied to the create-options target.
	badOpt := func(target any) error {
		if _, ok := target.(*createOptions); ok {
			return errBoom
		}
		return nil
	}

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, err := client.Create(ctx, "bad-opt", cSpec{Val: "x"}, badOpt)
	require.ErrorIs(t, err, errBoom)
}

func TestClientCreate(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, "hello-1", cSpec{Val: "hello"})
	assert.NotZero(t, obj.ID)
	assert.Equal(t, clientTestGK.Group, obj.Group)
	assert.Equal(t, clientTestGK.Kind, obj.Kind)
	assert.Equal(t, int64(1), obj.Generation)
	assert.Nil(t, obj.Status)
	assert.Equal(t, "hello", obj.Spec.Val)
	assert.Equal(t, "hello-1", obj.Name, "the name is required, so a created row always has one")
}

// The name's UNIQUE constraint is unchanged by the move to a positional
// argument — it was reachable only through WithName before, so it needs a test
// at the new signature.
func TestClientCreateRejectsTakenName(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	mustCreate(t, ctx, client, "taken", cSpec{Val: "first"})

	_, err := client.Create(ctx, "taken", cSpec{Val: "second"})

	require.ErrorIs(t, err, ErrNameTaken,
		"a name already held fails rather than returning the existing row, and says so matchably")
}

// A tombstone keeps the name reserved: the name is not free until GC clears
// finalizers and removes the row. Callers retrying on ErrNameTaken need that to be
// the same error, or a delete-then-recreate looks like a different failure.
func TestClientCreateReportsNameTakenByATombstone(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	registerNoop[cSpec, cStatus](t, bh, clientTestGK) // WithFinalizers below needs it

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	// The finalizer keeps the tombstone around after Delete, so the name is still
	// held when Create runs.
	mustCreate(t, ctx, client, "doomed", cSpec{Val: "first"}, WithFinalizers("test/hold"))
	require.NoError(t, client.DeleteByName(ctx, "doomed"))

	_, err := client.Create(ctx, "doomed", cSpec{Val: "second"})

	require.ErrorIs(t, err, ErrNameTaken, "a deletion-pending row still holds its name")
}

// The empty name is the one footgun making the name required creates rather than
// inherits. Before, "no name" was spelled by passing no WithName; now every write
// names something, and a name read from unset configuration is "" — which under
// the old contract was an ordinary name, so every such caller would silently share
// one row. Rejecting it is a deliberate, narrow exception to "beehive does not
// validate names".
func TestClientRejectsEmptyName(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	t.Run("Create", func(t *testing.T) {
		_, err := client.Create(ctx, "", cSpec{Val: "a"})
		require.ErrorIs(t, err, ErrInvalidName)
	})
	t.Run("GetOrCreate", func(t *testing.T) {
		_, created, err := client.GetOrCreate(ctx, "", cSpec{Val: "a"})
		require.ErrorIs(t, err, ErrInvalidName)
		assert.False(t, created)
	})
	t.Run("Update", func(t *testing.T) {
		_, err := client.UpdateByName(ctx, "", cSpec{Val: "a"})
		require.ErrorIs(t, err, ErrInvalidName)
	})
	t.Run("Get", func(t *testing.T) {
		// Not ErrNotFound: that would send the caller hunting for a missing row
		// when what is missing is a config value.
		_, err := client.GetByName(ctx, "")
		require.ErrorIs(t, err, ErrInvalidName)
	})
	t.Run("Delete", func(t *testing.T) {
		// Delete folds absence to nil, so a silent nil here would be the worst
		// possible answer — indistinguishable from a successful no-op.
		require.ErrorIs(t, client.DeleteByName(ctx, ""), ErrInvalidName)
	})
}

// Rejection is caller-input validation, so it must not depend on store state:
// the same call has to fail whether or not a row happens to exist, or the bug
// stays hidden until a cold start or a GC sweep removes the row.
func TestClientRejectsEmptyNameBeforeAnyStoreWork(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	// An unmarshalable spec would also fail this call — assert the name is checked
	// first, so the caller hears about the argument they actually got wrong.
	bad := NewClient[errMarshaler, cStatus](bh, clientTestGK)
	_, err := bad.Create(ctx, "", errMarshaler{})
	require.ErrorIs(t, err, ErrInvalidName)

	after, err := client.List(ctx)
	require.NoError(t, err)
	assert.Empty(t, after, "a rejected create writes nothing")
}

// The id is the key everywhere, so the bare CRUD verbs take the id and the
// ByName siblings resolve. This pins both halves at once, because the
// interesting property is that they address different things.
func TestClientCRUDIsIDKeyedWithByNameSiblings(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	created := mustCreate(t, ctx, client, "prod", cSpec{Val: "a"})

	byName, err := client.GetByName(ctx, "prod")
	require.NoError(t, err)
	byID, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, created.ID, byName.ID)
	assert.Equal(t, created.ID, byID.ID)

	// Absence splits the same way it always has: a name folds to nil (idempotent),
	// an id reports ErrNotFound.
	require.NoError(t, client.DeleteByName(ctx, "no-such-name"))
	require.ErrorIs(t, client.Delete(ctx, 99999), ErrNotFound)

	require.NoError(t, client.DeleteByName(ctx, "prod"))
	got, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	assert.NotNil(t, got.DeletionRequestedAt, "the name-keyed delete marked the row")
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
	bh := newTestBeehive(t, newClientTestStore(t))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	_, err := client.Create(ctx, "unclearable", cSpec{Val: "a"}, WithFinalizers("cleanup"))

	require.ErrorIs(t, err, ErrInvalidOption)
	assert.Contains(t, err.Error(), "WithFinalizers")
	// Rejected before any store work, so there is no row to collect either.
	objs, listErr := client.List(ctx)
	require.NoError(t, listErr)
	assert.Empty(t, objs, "the create wrote nothing")
}

// GetOrCreate takes the same options, so it makes the same check — on the branch
// that would actually insert and on the one that would not, since resolving
// happens before the name lookup.
func TestClientGetOrCreateRejectsFinalizersOnUnregisteredKind(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	_, created, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "a"}, WithFinalizers("cleanup"))

	require.ErrorIs(t, err, ErrInvalidOption)
	assert.False(t, created)
}

// It fires on the found branch too, where no row is created and the option would
// have been ignored. That is the eager-validation rule GetOrCreate documents:
// deferring to the insert would make the same call pass wherever the row already
// exists and strand the first time it does not.
func TestClientGetOrCreateRejectsFinalizersOnTheFoundBranch(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, err := client.Create(ctx, "w1", cSpec{Val: "a"})
	require.NoError(t, err, "the row exists, so the create branch is not reached")

	_, created, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "b"}, WithFinalizers("cleanup"))

	require.ErrorIs(t, err, ErrInvalidOption)
	assert.False(t, created)
}

// The check is gated on the option being used, so an ordinary create on a
// client-only kind stays legal — client-only kinds are a supported shape, and only
// the finalizer makes one uncollectable.
func TestClientCreateWithoutFinalizersAllowsUnregisteredKind(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	obj, err := client.Create(ctx, "owned", cSpec{Val: "a"})

	require.NoError(t, err)
	assert.Empty(t, obj.Finalizers)
}

func TestClientCreateWithOptions(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)
	registerNoop[cSpec, cStatus](t, bh, clientTestGK) // WithFinalizers below needs it

	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	// An owner must exist before a child can ref it.
	owner := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "owner"})

	child, err := client.Create(ctx, "child-1", cSpec{Val: "child"},
		WithFinalizers("cleanup-a", "cleanup-b"),
		WithOwner(owner.ID))
	require.NoError(t, err)

	assert.Equal(t, "child-1", child.Name)
	assert.Equal(t, []string{"cleanup-a", "cleanup-b"}, child.Finalizers)

	// Name is persisted and looked up via Get.
	got, err := client.GetByName(ctx, "child-1")
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
	bh := newTestBeehive(t, newClientTestStore(t))

	// The owner must exist: the ref's foreign key rejects a dangling owner, and
	// Within rolls the half-made child back with it.
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, err := client.Create(ctx, "orphan", cSpec{Val: "child"}, WithOwner(9999))
	require.Error(t, err)

	objs, err := client.List(ctx)
	require.NoError(t, err)
	assert.Empty(t, objs)
}

func TestClientGet(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	created := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

	got, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, created.ID, got.ID)
	assert.Equal(t, "hello", got.Spec.Val)
	assert.Nil(t, got.Status)
}

func TestClientGetAbsentName(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, err := client.GetByName(ctx, "nonexistent")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestClientList(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	a := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	b := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "b"})

	list, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, list, 2)
	assert.Equal(t, a.ID, list[0].ID)
	assert.Equal(t, b.ID, list[1].ID)
}

func TestClientUpdate(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	created := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "v1"})

	updated, err := client.Update(ctx, created.ID, cSpec{Val: "v2"})
	require.NoError(t, err)
	assert.Equal(t, created.ID, updated.ID)
	assert.Equal(t, int64(2), updated.Generation)
	assert.Equal(t, "v2", updated.Spec.Val)
}

func TestClientGetOrCreateCreates(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, created, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "a"})
	require.NoError(t, err)
	assert.True(t, created)
	assert.NotZero(t, obj.ID)
	assert.Equal(t, "w1", obj.Name)
	assert.Equal(t, int64(1), obj.Generation)
	assert.Equal(t, "a", obj.Spec.Val)

	got, err := client.GetByName(ctx, "w1")
	require.NoError(t, err)
	assert.Equal(t, obj.ID, got.ID)
}

func TestClientGetOrCreateReturnsExisting(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

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
	bh := newTestBeehive(t, newClientTestStore(t))
	registerNoop[cSpec, cStatus](t, bh, clientTestGK) // WithFinalizers below needs it

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	// The finalizer keeps the tombstone around after Delete, so the name is still
	// held by a deletion-pending row when GetOrCreate runs.
	orig := mustCreate(t, ctx, client, "w1", cSpec{Val: "a"}, WithFinalizers("test/hold"))
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
	bh := newTestBeehive(t, store)
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, created, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "a"})
	require.NoError(t, err)
	require.True(t, created)
	assert.Equal(t, []ObjectID{obj.ID}, unsettledIDs(t, store), "a new object is owed its first pass")

	// Settle it, so the found branch below starts from "nothing owed".
	err = store.ObjectsUpdateStatus(ctx, clientTestGK, obj.ID, 1, []byte(`{}`), 0)
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
// created=false because nothing landed — and the name is left free, so a retry does
// not hit a spurious UNIQUE on a phantom row.
func TestClientGetOrCreateRollsBackOnDecodeError(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)
	gk := GroupKind{Kind: "BadDecode"}
	client := NewClient[badDecodeSpec, cStatus](bh, gk)

	obj, created, err := client.GetOrCreate(ctx, "w1", badDecodeSpec{Val: "a"})
	require.Error(t, err, "the new row's bytes must fail to decode")
	assert.Nil(t, obj)
	assert.False(t, created, "a rolled-back create must report created=false")

	// Nothing committed: the name is still absent, so a second attempt takes the
	// create branch again (not the found branch) and likewise rolls back.
	_, err = store.ObjectsGetByName(ctx, gk, "w1")
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
	bh := newTestBeehive(t, store)
	gk := GroupKind{Kind: "BadDecode"}
	_, err := Register(bh, gk, &noopController[badDecodeSpec, cStatus]{})
	require.NoError(t, err)
	r, ok := bh.reconcilerFor(gk)
	require.True(t, ok)

	client := NewClient[badDecodeSpec, cStatus](bh, gk)
	obj, err := client.Create(ctx, "w1", badDecodeSpec{Val: "a"})
	require.Error(t, err, "the new row's bytes must fail to decode")
	assert.Nil(t, obj)
	assert.Empty(t, queuedIDs(r.work), "a rolled-back create must not wake the reconciler")

	_, err = store.ObjectsGetByName(ctx, gk, "w1")
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
	bh := newTestBeehive(t, store)
	gk := GroupKind{Kind: "CondBad"}
	_, err := Register(bh, gk, &noopController[conditionalBadSpec, cStatus]{})
	require.NoError(t, err)
	r, ok := bh.reconcilerFor(gk)
	require.True(t, ok)
	client := NewClient[conditionalBadSpec, cStatus](bh, gk)

	orig := mustCreate(t, ctx, client, uniqueName(), conditionalBadSpec{Val: "good"})
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

// TestClientWithOnCreateFiresOnlyOnCreate pins WithOnCreate to the create branch:
// Create always runs it, GetOrCreate runs it when it inserts but not when it
// returns an existing row.
func TestClientWithOnCreateFiresOnlyOnCreate(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	var calls int
	onCreate := WithOnCreate(func(context.Context) { calls++ })

	mustCreate(t, ctx, client, "c1", cSpec{Val: "a"}, onCreate)
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
			_, err := c.Create(ctx, "hooked", cSpec{Val: "b"}, onCreate)
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
			_, err := c.Create(ctx, "rolled-back", cSpec{Val: "b"})
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
				seeded := mustCreate(t, ctx, client, "seed", cSpec{Val: "a"})
				// Settle the seed so its own unconverged spec doesn't mask the write's.
				err = store.ObjectsUpdateStatus(ctx, clientTestGK, seeded.ID, 1, []byte(`{}`), 0)
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
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, "w1", cSpec{Val: "a"})
	// Settle it, so anything the write below owes is its own.
	err = store.ObjectsUpdateStatus(ctx, clientTestGK, obj.ID, 1, []byte(`{}`), 0)
	require.NoError(t, err)
	require.Empty(t, unsettledIDs(t, store), "precondition: nothing owed")

	_, err = client.Update(ctx, obj.ID, cSpec{Val: "a"})
	require.NoError(t, err)
	assert.Empty(t, unsettledIDs(t, store), "re-applying the identical spec leaves it settled")

	// A real change still unsettles it, so the suppression is scoped to the no-op.
	_, err = client.Update(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{obj.ID}, unsettledIDs(t, store), "a real spec change is owed a pass")
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
		obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"}, WithFinalizers("test/hold"))

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
	bh := newTestBeehive(t, store)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	owner := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "owner"})

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
	bh := newTestBeehive(t, newClientTestStore(t))
	registerNoop[cSpec, cStatus](t, bh, clientTestGK) // WithFinalizers below needs it

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, created, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "a"},
		WithFinalizers("cleanup-a", "cleanup-b"))
	require.NoError(t, err)
	require.True(t, created)
	assert.Equal(t, []string{"cleanup-a", "cleanup-b"}, obj.Finalizers)

	got, err := client.GetByName(ctx, "w1")
	require.NoError(t, err)
	assert.Equal(t, []string{"cleanup-a", "cleanup-b"}, got.Finalizers)
}

func TestClientGetOrCreateMarshalError(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[errMarshaler, cStatus](bh, clientTestGK)
	_, created, err := client.GetOrCreate(ctx, "w1", errMarshaler{})
	require.Error(t, err)
	assert.False(t, created)
}

func TestClientGetOrCreateStoreError(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, &nameErrorStore{})

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, created, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "a"})
	require.ErrorIs(t, err, errBoom)
	assert.False(t, created)
}

func TestClientGetOrCreateOptionError(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	badOpt := func(any) error { return errBoom }

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, created, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "a"}, badOpt)
	require.ErrorIs(t, err, errBoom)
	assert.False(t, created)
}

func TestClientGetOrCreateCreateError(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, &createErrorStore{})

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, created, err := client.GetOrCreate(ctx, "w1", cSpec{Val: "a"})
	require.ErrorIs(t, err, errBoom)
	assert.False(t, created)
}

func TestClientGetOrCreateRawToTypedError(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, &getOrCreateBadJSONStore{})

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
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, err := client.Get(ctx, 999)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestClientWatchNonExistentID(t *testing.T) {
	parent, client := watchTestClient(t)
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	// Watch a non-existent ID: the snapshot loader returns (nil, nil) via the
	// ErrNotFound path, yielding an empty snapshot and an open channel.
	_, ch, err := client.Watch(ctx, 9999)
	require.NoError(t, err)

	// Cancel ctx — channel must close cleanly (no events, just the cancel).
	cancel()
	assertChanClosed(t, ch)
}

func TestClientDeleteNotFound(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	err := client.Delete(ctx, 999)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestClientDelete(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	created := mustCreate(t, ctx, client, uniqueName(), cSpec{})

	err := client.Delete(ctx, created.ID)
	require.NoError(t, err)

	// object still present (no finalizers cleared), but marked for deletion. The
	// the default full pass is enabled, so the client-only object isn't collected
	// synchronously by Delete — the idle sweeper is its backstop.
	got, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	assert.NotNil(t, got.DeletionRequestedAt)
}

func TestClientDeleteByName(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	mustCreate(t, ctx, client, "w1", cSpec{})

	require.NoError(t, client.DeleteByName(ctx, "w1"))

	// As in TestClientDelete, the object lingers marked for deletion rather than
	// being collected synchronously.
	got, err := client.GetByName(ctx, "w1")
	require.NoError(t, err)
	assert.NotNil(t, got.DeletionRequestedAt)
}

func TestClientDeleteByNameNotFoundIsNil(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	assert.NoError(t, client.DeleteByName(ctx, "never-created"))
}

func TestClientDeleteByNameIdempotent(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	mustCreate(t, ctx, client, "w1", cSpec{})

	require.NoError(t, client.DeleteByName(ctx, "w1"))
	assert.NoError(t, client.DeleteByName(ctx, "w1"))
}

// A row held deletion-pending by a finalizer must absorb a second Delete
// as a pure no-op: no error, and no second state change for watchers to see.
func TestClientDeleteAlreadyDeleting(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	registerNoop[cSpec, cStatus](t, bh, clientTestGK) // WithFinalizers below needs it

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, "w1", cSpec{}, WithFinalizers("test/hold"))

	require.NoError(t, client.Delete(ctx, obj.ID))
	pending, err := client.GetByName(ctx, "w1")
	require.NoError(t, err)
	require.NotNil(t, pending.DeletionRequestedAt)

	require.NoError(t, client.DeleteByName(ctx, "w1"))

	got, err := client.GetByName(ctx, "w1")
	require.NoError(t, err)
	assert.Equal(t, pending.ID, got.ID)
	assert.Equal(t, pending.DeletionRequestedAt, got.DeletionRequestedAt)
	// DeletionRequestsCreate reports no change, so no write and no Modified event: the
	// resource_version is the tell.
	assert.Equal(t, pending.ResourceVersion, got.ResourceVersion)
}

// the sweeper's registered-kind branch: the object must be handed to its controller
// to clear finalizers, the one part of Delete's tail Delete still runs itself
// now that the store resolves and marks in one statement. A name that matches no
// row must wake nobody.
func TestClientDeleteMarksForCollection(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, "w1", cSpec{}, WithFinalizers("test/hold"))

	require.NoError(t, client.DeleteByName(ctx, "w1"))
	pending, err := store.DeletionRequestsList(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1, "the mark is the whole signal: it puts the row in the sweeper's listing")
	assert.Equal(t, obj.ID, pending[0].ID)

	require.NoError(t, client.DeleteByName(ctx, "absent"))
	pending, err = store.DeletionRequestsList(ctx)
	require.NoError(t, err)
	assert.Len(t, pending, 1, "an unresolved name marks nothing")
}

// A name is per-kind, so another kind's row holding the same name is invisible:
// Delete reports success (nothing of this kind to delete) and leaves it be.
func TestClientDeleteKindScoped(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	widgets := NewClient[cSpec, cStatus](bh, clientTestGK)
	gadgets := NewClient[cSpec, cStatus](bh, GroupKind{Kind: "Gadget"})

	w := mustCreate(t, ctx, widgets, "shared", cSpec{Val: "v1"})

	require.NoError(t, gadgets.DeleteByName(ctx, "shared"))

	got, err := widgets.Get(ctx, w.ID)
	require.NoError(t, err)
	assert.Nil(t, got.DeletionRequestedAt, "the Widget must be untouched")
}

func TestClientDeleteStoreError(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, &requestDeletionByNameErrorStore{})

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	require.ErrorIs(t, client.DeleteByName(ctx, "w1"), errBoom)
}

// TestClientIDOpsScopedToKind verifies that ID-based operations on a Client are
// confined to that client's kind: an id naming an object of another kind is
// invisible (Get/Update/Delete all report ErrNotFound) and the foreign object is
// left untouched, never updated or marked for deletion through the wrong client.
func TestClientIDOpsScopedToKind(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	widgets := NewClient[cSpec, cStatus](bh, GroupKind{Kind: "Widget"})
	gadgets := NewClient[cSpec, cStatus](bh, GroupKind{Kind: "Gadget"})

	w := mustCreate(t, ctx, widgets, uniqueName(), cSpec{Val: "v1"})

	// The Gadget client must not see or mutate the Widget by its id.
	_, err := gadgets.Get(ctx, w.ID)
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

func (s *createBadJSONStore) ObjectsCreate(_ context.Context, _ GroupKind, _ ObjectsCreateInput) (*RawObject, error) {
	return &RawObject{ID: 1, Spec: []byte("not-json")}, nil
}

// errorObjectsCreateStore returns an error from ObjectsCreate.
type errorObjectsCreateStore struct {
	fakeStore
}

func (s *errorObjectsCreateStore) ObjectsCreate(_ context.Context, _ GroupKind, _ ObjectsCreateInput) (*RawObject, error) {
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

// nameErrorStore returns a non-NotFound error from ObjectsGetByName, driving
// GetOrCreate's read-error branch.
type nameErrorStore struct {
	fakeStore
}

func (s *nameErrorStore) ObjectsGetByName(_ context.Context, _ GroupKind, _ string) (*RawObject, error) {
	return nil, errBoom
}

// requestDeletionByNameErrorStore fails the name-keyed deletion request.
type requestDeletionByNameErrorStore struct {
	fakeStore
}

func (s *requestDeletionByNameErrorStore) DeletionRequestsCreateByName(_ context.Context, _ GroupKind, _ string) (storeapi.DeletionRequestResult, error) {
	return storeapi.DeletionRequestResult{}, errBoom
}

// getOrCreateBadJSONStore drives GetOrCreate's rawToTyped error path: the
// name is absent (NotFound) so the create branch runs, and ObjectsCreate returns
// undecodable spec bytes.
type getOrCreateBadJSONStore struct {
	fakeStore
}

func (s *getOrCreateBadJSONStore) ObjectsGetByName(_ context.Context, _ GroupKind, _ string) (*RawObject, error) {
	return nil, ErrNotFound
}

func (s *getOrCreateBadJSONStore) ObjectsCreate(_ context.Context, _ GroupKind, _ ObjectsCreateInput) (*RawObject, error) {
	return &RawObject{ID: 1, Spec: []byte("not-json")}, nil
}

// createErrorStore drives the create branch's write-error path: it borrows
// getOrCreateBadJSONStore's absent-name lookup so the insert runs, but fails
// the insert instead of returning an undecodable row.
type createErrorStore struct {
	getOrCreateBadJSONStore
}

func (s *createErrorStore) ObjectsCreate(_ context.Context, _ GroupKind, _ ObjectsCreateInput) (*RawObject, error) {
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
	bh := newTestBeehive(t, &errorObjectsCreateStore{})
	client := NewClient[tSpec, tStatus](bh, GroupKind{Kind: "Widget"})
	_, err := client.Create(context.Background(), "closed-a", tSpec{})
	require.Error(t, err)
}

func TestClientCreateRawToTypedError(t *testing.T) {
	bh := newTestBeehive(t, &createBadJSONStore{})
	client := NewClient[tSpec, tStatus](bh, GroupKind{Kind: "Widget"})
	_, err := client.Create(context.Background(), "closed-b", tSpec{})
	require.Error(t, err)
}

func TestClientUpdateStoreError(t *testing.T) {
	bh := newTestBeehive(t, &errorUpdateSpecStore{})
	client := NewClient[tSpec, tStatus](bh, GroupKind{Kind: "Widget"})
	_, err := client.Update(context.Background(), 1, tSpec{})
	require.Error(t, err)
}

func TestClientUpdateRawToTypedError(t *testing.T) {
	bh := newTestBeehive(t, &updateBadJSONStore{})
	client := NewClient[tSpec, tStatus](bh, GroupKind{Kind: "Widget"})
	_, err := client.Update(context.Background(), 1, tSpec{})
	require.Error(t, err)
}

func TestClientListStoreError(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	bh := newTestBeehive(t, &errorListObjectsStore{})
	client := NewClient[tSpec, tStatus](bh, gk)
	_, err := client.List(context.Background())
	require.Error(t, err)
}

// TestClientListRawToTypedError verifies List quarantines an un-decodable row
// (skip-and-log) instead of failing the whole list: badJSONStore returns one row
// whose Spec is invalid JSON, so List returns no error and an empty result.
func TestClientListRawToTypedError(t *testing.T) {
	gk := GroupKind{Kind: "Widget"}
	bh := newTestBeehive(t, &badJSONStore{gk: gk})
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

// watchTestClient builds a Beehive with a real SQLite store and a registered
// controller for clientTestGK. No Start is needed for client-side watch tests.
//
// It returns the context those watches must run on, cancelled when the test
// ends. Cancelling is the *only* thing that stops a stream: a store closed under
// a poll is a read failure, which the poller logs and retries on the next tick.
// A stream left on context.Background() therefore outlives its test and keeps
// polling a closed store every fastTick for the rest of the binary — eight of
// them accumulated here once, and the drag starved a later test's failsafe on a
// single-proc race build.
func watchTestClient(t *testing.T) (context.Context, Client[cSpec, cStatus]) {
	t.Helper()
	bh := newTestBeehive(t, newClientTestStore(t), fast()...)
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	// Registered after the store's own close, so it runs before it: the streams
	// are cancelled first, and none of them sees the closed store at all.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx, NewClient[cSpec, cStatus](bh, clientTestGK)
}

// TestWatchListReceivesAddedOnCreate verifies that WatchList delivers a
// Added when an object is created.
func TestWatchListReceivesAddedOnCreate(t *testing.T) {
	ctx, client := watchTestClient(t)

	_, ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

	evt := recv(t, ch)
	assert.Equal(t, Added, evt.Type)
	assert.Equal(t, obj.ID, evt.Object.ID)
	assert.Equal(t, "hello", evt.Object.Spec.Val)
}

// TestWatchListReceivesModifiedOnUpdate verifies that WatchList delivers a
// Modified when an object's spec is updated.
func TestWatchListReceivesModifiedOnUpdate(t *testing.T) {
	ctx, client := watchTestClient(t)

	// Subscribe before creating so the snapshot is empty and the first event is
	// the Modified from the Update, not an Added from the snapshot.
	_, ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "v1"})
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
	ctx, client := watchTestClient(t)

	_, ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{})
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
	ctx, client := watchTestClient(t)

	_, ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{})
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
	other := mustCreate(t, ctx, client, uniqueName(), cSpec{})
	evt := recv(t, ch)
	require.Equal(t, other.ID, evt.Object.ID, "unexpected event on idempotent delete: %v", evt.Type)
	assert.Equal(t, Added, evt.Type)
}

// TestWatchReceivesOnlyMatchingID verifies that Watch(id) filters out events
// for other objects.
func TestWatchReceivesOnlyMatchingID(t *testing.T) {
	ctx, client := watchTestClient(t)

	obj1 := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	obj2 := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "b"})

	snap, ch, err := client.Watch(ctx, obj1.ID)
	require.NoError(t, err)

	// The snapshot holds obj1 and nothing else; the stream carries what follows.
	require.NotNil(t, snap.Object)
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
	parent, client := watchTestClient(t)
	ctx, cancel := context.WithCancel(parent)

	_, ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	cancel()
	assertChanClosed(t, ch)
}

// TestWatchClosesOnCtxCancel verifies that Watch(id) channel closes on ctx cancel.
func TestWatchClosesOnCtxCancel(t *testing.T) {
	parent, client := watchTestClient(t)
	ctx, cancel := context.WithCancel(parent)

	obj, err := client.Create(parent, "decoded", cSpec{})
	require.NoError(t, err)

	_, ch, err := client.Watch(ctx, obj.ID)
	require.NoError(t, err)

	cancel()
	assertChanClosed(t, ch)
}

// TestWatchReceivesModifiedOnStatusUpdate verifies that WatchList delivers a
// Modified when the controller calls UpdateStatus.
func TestWatchReceivesModifiedOnStatusUpdate(t *testing.T) {
	// Cancelled at the end of the test: the watch below runs on it, and a stream
	// left on an uncancellable context polls on past the test (see watchTestClient).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A fresh beehive rather than watchTestClient's, since this test needs the
	// ControllerClient that registration returns.
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

	obj := mustCreate(t, ctx, client2, uniqueName(), cSpec{Val: "x"})

	// Subscribe after create: the object is in the snapshot, and the stream
	// carries the Modified that UpdateStatus makes next.
	snap, ch, err := client2.WatchList(ctx)
	require.NoError(t, err)
	require.Len(t, snap.Objects, 1)
	assert.Equal(t, obj.ID, snap.Objects[0].ID)

	require.NoError(t, cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"}))

	evt := recv(t, ch)
	assert.Equal(t, Modified, evt.Type)
	assert.Equal(t, obj.ID, evt.Object.ID)
	require.NotNil(t, evt.Object.Status)
	assert.Equal(t, "done", evt.Object.Status.Val)
}

// TestWatchListInitialSnapshot verifies that WatchList emits Added events for
// objects that already exist in the store at subscription time. They come back
// as the Snapshot, not as Added changes.
func TestWatchListInitialSnapshot(t *testing.T) {
	ctx, client := watchTestClient(t)

	a := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	b := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "b"})

	snap, _, err := client.WatchList(ctx)
	require.NoError(t, err)

	seen := map[ObjectID]string{}
	for _, obj := range snap.Objects {
		seen[obj.ID] = obj.Spec.Val
	}
	assert.Equal(t, "a", seen[a.ID])
	assert.Equal(t, "b", seen[b.ID])
}

// TestWatchInitialSnapshot verifies that Watch(id) returns an object that
// already exists in the store at subscription time.
func TestWatchInitialSnapshot(t *testing.T) {
	ctx, client := watchTestClient(t)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})

	snap, _, err := client.Watch(ctx, obj.ID)
	require.NoError(t, err)

	require.NotNil(t, snap.Object)
	assert.Equal(t, obj.ID, snap.Object.ID)
	assert.Equal(t, "hello", snap.Object.Spec.Val)
}

// TestStartAfterStopErrors verifies that Beehive is a one-shot object: calling
// Start after Stop returns an error instead of silently re-driving a torn-down
// control plane.
func TestStartAfterStopErrors(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
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
// Watching a kind nobody registered is not an error: the tail reads the write
// log, so it needs no reconciler. An empty kind simply streams nothing.
func TestWatchListWorksForAnUnregisteredKind(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bh := newTestBeehive(t, newClientTestStore(t))

	unknownGK := GroupKind{Kind: "Unknown"}
	client := NewClient[cSpec, cStatus](bh, unknownGK)

	snap, _, err := client.WatchList(ctx)
	require.NoError(t, err)
	assert.Empty(t, snap.Objects)

	one, _, err := client.Watch(ctx, 0)
	require.NoError(t, err)
	assert.Nil(t, one.Object)
}

func TestClientGetOwner(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	owner := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "owner"})
	child := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "child"}, WithOwner(owner.ID))

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
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	a := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
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
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	a := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	b := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "b"})
	c := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "c"})

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
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	owner := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "owner"})
	c1 := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "c1"}, WithOwner(owner.ID))
	c2 := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "c2"}, WithOwner(owner.ID))

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
	bh := newTestBeehive(t, newClientTestStore(t))
	// One consumer of this fixture creates a child WithFinalizers, which is legal
	// only on a registered kind.
	registerNoop[cSpec, cStatus](t, bh, clientTestGK)

	owners := NewClient[cSpec, cStatus](bh, GroupKind{Kind: "Owner"})
	widgets := NewClient[cSpec, cStatus](bh, clientTestGK)
	gadgets := NewClient[cSpec, cStatus](bh, GroupKind{Kind: "Gadget"})

	owner := mustCreate(t, ctx, owners, uniqueName(), cSpec{Val: "owner"})
	w1 := mustCreate(t, ctx, widgets, uniqueName(), cSpec{Val: "w1"}, WithOwner(owner.ID))
	w2 := mustCreate(t, ctx, widgets, uniqueName(), cSpec{Val: "w2"}, WithOwner(owner.ID))
	mustCreate(t, ctx, gadgets, uniqueName(), cSpec{Val: "g1"}, WithOwner(owner.ID))

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
	owner := mustCreate(t, ctx, owners, uniqueName(), cSpec{Val: "owner2"})
	// The finalizer holds the row after Delete, leaving it deletion-pending.
	child, err := widgets.Create(ctx, "w1", cSpec{Val: "w1"},
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

// An owner-scoped watch resolves ownership from current state, which is sound
// only because ownership never changes under it: an owned_by edge is written at
// create and removed only when the child is collected. "No verb moves one" is a
// claim over an open set and cannot be asserted behaviourally, so this asserts
// the structure that makes it true. A second call site is where a re-parent verb
// would appear; see docs/adr/2026-08-06-owner-scoped-watches.md before adding
// one.
func TestOwnedByIsWrittenInOnePlace(t *testing.T) {
	// The whole module, not just this package: Store is implemented in sqlite/,
	// where an owned_by write would be just as invisible to a scoped watch.
	var sites []string
	require.NoError(t, filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		var fn string
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.FuncDecl:
				fn = node.Name.Name
			case *ast.CallExpr:
				// Delete as well as Add: both are public Store members taking any
				// relation, so either can move an owned_by edge.
				sel, ok := node.Fun.(*ast.SelectorExpr)
				if !ok || len(node.Args) == 0 {
					return true
				}
				if sel.Sel.Name != "EdgesAdd" && sel.Sel.Name != "EdgesDelete" {
					return true
				}
				if relationName(node.Args[len(node.Args)-1]) == "RelationOwnedBy" {
					sites = append(sites, path+":"+fn)
				}
			}
			return true
		})
		return nil
	}))

	assert.Equal(t, []string{"client.go:insertObject"}, sites)
}

// relationName reads the relation constant out of a call argument, spelled bare
// inside this package and qualified as storeapi.X everywhere else.
func relationName(arg ast.Expr) string {
	switch e := arg.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return e.Sel.Name
	}
	return ""
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
	bh := newTestBeehive(t, &ownedObjectsErrorStore{})
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	_, err := client.OwnedObjectsList(ctx, 1)
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
	bh := newTestBeehive(t, &ownedObjectsBadJSONStore{gk: clientTestGK})
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
	bh := newTestBeehive(t, &ownedObjectsLoadErrorStore{gk: clientTestGK})
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	_, err := client.OwnedObjectsList(ctx, 1, LoadOwner())
	require.ErrorIs(t, err, errBoom)
}

func TestClientGetWithLoadOwner(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	owner := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "owner"})
	child := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "child"}, WithOwner(owner.ID))

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

	// Get honours selectors too.
	mustCreate(t, ctx, client, "s1", cSpec{Val: "named"}, WithOwner(owner.ID))
	byName, err := client.GetByName(ctx, "s1", LoadOwner())
	require.NoError(t, err)
	ref, ok, err = byName.Owner()
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
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	owner := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "owner"})
	const n = 5
	for i := 0; i < n; i++ {
		mustCreate(t, ctx, client, fmt.Sprintf("child-%d", i), cSpec{Val: fmt.Sprintf("child-%d", i)}, WithOwner(owner.ID))
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
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	owner := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "owner"})
	const n = 3
	var childIDs []ObjectID
	for i := 0; i < n; i++ {
		c := mustCreate(t, ctx, client, fmt.Sprintf("child-%d", i), cSpec{Val: fmt.Sprintf("child-%d", i)}, WithOwner(owner.ID))
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
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	a := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	b := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "b"})
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
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	a := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	b := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "b"})
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
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	obj := mustCreate(t, ctx, client, "x1", cSpec{Val: "x"})

	loads := []LoadOption{LoadOwner(), LoadDependencies(), LoadDependents(), LoadOwned()}
	// Single-object path: each relation's store error surfaces through Get/GetByName.
	for _, l := range loads {
		_, err := client.Get(ctx, obj.ID, l)
		require.ErrorIs(t, err, errBoom)
	}
	_, err := client.GetByName(ctx, "x1", LoadOwner())
	require.ErrorIs(t, err, errBoom)

	// Batched path: each relation's store error surfaces through List.
	for _, l := range loads {
		_, err := client.List(ctx, l)
		require.ErrorIs(t, err, errBoom)
	}
}

func TestClientLazyRefsMissingIDReadsEmpty(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
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

// getBadJSONStore returns an undecodable spec from the scoped and name-keyed
// reads, driving the decode-error branch of Client.Get / Client.GetByName.
type getBadJSONStore struct {
	fakeStore
	gk GroupKind
}

func (s *getBadJSONStore) ObjectsGet(context.Context, ObjectID) (*RawObject, error) {
	return &RawObject{ID: 1, Group: s.gk.Group, Kind: s.gk.Kind, Spec: []byte("not-json")}, nil
}
func (s *getBadJSONStore) ObjectsGetByName(context.Context, GroupKind, string) (*RawObject, error) {
	return &RawObject{ID: 1, Group: s.gk.Group, Kind: s.gk.Kind, Spec: []byte("not-json")}, nil
}

func TestGetDecodeError(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, &getBadJSONStore{gk: clientTestGK})
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	_, err := client.Get(ctx, 1)
	require.Error(t, err)
	_, err = client.GetByName(ctx, "any")
	require.Error(t, err)
}

// TestClientRequeueNotFound verifies Requeue reports ErrNotFound
// for an id that does not exist, before reaching any reconciler.
func TestClientRequeueNotFound(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	err := client.Requeue(ctx, 999)
	assert.ErrorIs(t, err, ErrNotFound)
}

// TestClientRequeueNoController verifies Requeue reports
// ErrNoController for a client-only kind: the object exists but no reconciler is
// registered to enqueue it on.
func TestClientRequeueNoController(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})

	err := client.Requeue(ctx, obj.ID)
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
			obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})

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
	bh := newTestBeehive(t, newClientTestStore(t))
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
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
	bh := newTestBeehive(t, newClientTestStore(t))
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})

	// Drain the create-time enqueue so only the future schedule remains.
	r := bh.reconcilers[clientTestGK]
	drainQueue(r.work)
	r.work.addAfter(obj.ID, time.Hour, alarmRequeueAfter)

	s, err := client.SchedulesGet(ctx, obj.ID)
	require.NoError(t, err)
	assert.True(t, s.NextRequeueAt.After(time.Now().Add(time.Minute)),
		"fire time must be ~1h out, got %s", s.NextRequeueAt)
}

// TestClientGetScheduleUnscheduled verifies SchedulesGet returns the zero-value
// Schedule (and no error) when nothing is scheduled for the id.
func TestClientGetScheduleUnscheduled(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})

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
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})

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
	bh := newTestBeehive(t, newClientTestStore(t))
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})

	// Drain the create-time enqueue and schedule a future requeue before watching.
	r := bh.reconcilers[clientTestGK]
	drainQueue(r.work)
	r.work.addAfter(obj.ID, time.Hour, alarmRequeueAfter)

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
	bh := newTestBeehive(t, newClientTestStore(t), fast()...)
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})

	r := bh.reconcilers[clientTestGK]
	drainQueue(r.work)

	ch, err := client.SchedulesWatch(ctx, obj.ID)
	require.NoError(t, err)

	// Snapshot: nothing scheduled after the drain.
	snap := recv(t, ch)
	assert.True(t, snap.NextRequeueAt.IsZero(), "snapshot must be unscheduled, got %s", snap.NextRequeueAt)

	// A future requeue: emits the fire time.
	r.work.addAfter(obj.ID, time.Hour, alarmRequeueAfter)
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
	bh := newTestBeehive(t, newClientTestStore(t))

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})

	_, err := client.SchedulesWatch(ctx, obj.ID)
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
	bh := newTestBeehive(t, eventErrStore{newClientTestStore(t)})
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})

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
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})

	got, err := client.EventsList(ctx, obj.ID)
	require.NoError(t, err)
	assert.Empty(t, got)
}

// The motivating use case, end-to-end through the public API: a flapping cluster's
// connection-probe outcomes emitted via ControllerClient.EventsAdd render as the
// aggregated, newest-first timeline the health panel shows.
func TestEventsConnectionPanelTimeline(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)
	cc, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	cluster := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "prod"})

	// The prober emits one event per probe; identical consecutive outcomes coalesce.
	emit := func(typ EventType, reason, msg string, detail any, n int) {
		for i := 0; i < n; i++ {
			require.NoError(t, cc.EventsAdd(ctx, cluster.ID, EventSpec{
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
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})
	err := store.EventsAdd(ctx, clientTestGK, obj.ID, EventsAddInput{Category: "c", Type: "Warning", Reason: "ProbeFailed"})
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
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	a := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	b := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "b"})
	err := store.EventsAdd(ctx, clientTestGK, a.ID, EventsAddInput{Category: "c", Type: "Normal", Reason: "AOK"})
	require.NoError(t, err)
	err = store.EventsAdd(ctx, clientTestGK, b.ID, EventsAddInput{Category: "c", Type: "Warning", Reason: "BBad"})
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
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})

	rec := func(cat, typ, reason string) {
		err := store.EventsAdd(ctx, clientTestGK, obj.ID, EventsAddInput{Category: cat, Type: typ, Reason: reason})
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
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})

	err := store.EventsAdd(ctx, clientTestGK, obj.ID, EventsAddInput{Category: "connection", Type: "Normal", Reason: "Connected"})
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
// registered controller. The write here goes straight through the Store, so it
// publishes no wake: what delivers it is the floor tick, which is the pull path
// every push in this system is allowed to be a shortcut for.
func TestClientWatchEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store, fast()...)
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "x"})

	stream, err := client.EventsWatch(ctx, obj.ID)
	require.NoError(t, err)

	err = store.EventsAdd(ctx, clientTestGK, obj.ID, EventsAddInput{Category: "c", Type: "Warning", Reason: "ProbeFailed"})
	require.NoError(t, err)

	ev := recv(t, stream.Events)
	assert.Equal(t, "ProbeFailed", ev.Reason)
	assert.Equal(t, EventWarning, ev.Type)

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

// UpdateByName joins the ByName family: leaving it out would split the CRUD
// set, letting a caller read and delete by name but forcing an id to write.
func TestClientUpdateIsIDKeyedWithByNameSibling(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	created := mustCreate(t, ctx, client, "prod", cSpec{Val: "v1"})

	byName, err := client.UpdateByName(ctx, "prod", cSpec{Val: "v2"})
	require.NoError(t, err)
	assert.Equal(t, created.ID, byName.ID)
	assert.Equal(t, "v2", byName.Spec.Val)
	assert.Equal(t, int64(2), byName.Generation, "a real spec change bumps generation")

	byID, err := client.Update(ctx, created.ID, cSpec{Val: "v3"})
	require.NoError(t, err)
	assert.Equal(t, "v3", byID.Spec.Val)
}

// Unlike Delete, a missing row is not "already in the desired state" — there is
// nothing to write the spec onto — so Update reports absence both ways.
func TestClientUpdateAbsentNameIsNotFound(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	_, err := client.UpdateByName(ctx, "no-such-name", cSpec{Val: "v1"})
	require.ErrorIs(t, err, ErrNotFound)
}

// A name is unique only within a GroupKind, so the name-keyed update must fold
// the kind into its own lookup rather than trusting the caller's.
func TestClientUpdateIsKindScoped(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)
	widgets := NewClient[cSpec, cStatus](bh, GroupKind{Kind: "Widget"})
	gadgets := NewClient[cSpec, cStatus](bh, GroupKind{Kind: "Gadget"})
	w := mustCreate(t, ctx, widgets, "shared", cSpec{Val: "widget"})

	_, err := gadgets.UpdateByName(ctx, "shared", cSpec{Val: "gadget"})
	require.ErrorIs(t, err, ErrNotFound, "another kind's row holding the same name is not this client's to write")

	unchanged, err := widgets.Get(ctx, w.ID)
	require.NoError(t, err)
	assert.Equal(t, "widget", unchanged.Spec.Val)
}

// The ABA contract, both directions. The bare verbs keep the README's promise —
// everything after the create takes an ObjectID, so a delete and recreate under
// the same name can't make you act on the wrong row — and the ByName siblings
// are the opt-out. A split is only real if both halves are pinned:
//
//	a name-keyed call acts on whatever holds that name NOW, or reports absence;
//	an id-keyed call acts on that ONE incarnation, or returns ErrNotFound.
//
// Without this test the contract is prose, and a resolve-then-write regression in
// either mutator would pass everything else in the suite.
func TestClientNameKeyedWritesFollowTheNameAcrossARecreate(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	first := mustCreate(t, ctx, client, "prod", cSpec{Val: "first"})

	// Retire the first incarnation completely. Until GC collects it the tombstone
	// still holds the name's UNIQUE constraint, so the recreate below could not
	// even happen — which is what makes this window narrow in practice.
	require.NoError(t, client.DeleteByName(ctx, "prod"))
	collected, err := bh.gcCollect(ctx, first.ID)
	require.NoError(t, err)
	require.True(t, collected, "the row must be physically gone before the name is free")

	second := mustCreate(t, ctx, client, "prod", cSpec{Val: "second"})
	require.NotEqual(t, first.ID, second.ID, "AUTOINCREMENT never reuses an id")

	// The name follows the name to the live incarnation.
	updated, err := client.UpdateByName(ctx, "prod", cSpec{Val: "third"})
	require.NoError(t, err)
	assert.Equal(t, second.ID, updated.ID, "the name-keyed write landed on whatever holds the name now")

	// The id still means the incarnation it always meant, which no longer exists.
	_, err = client.Update(ctx, first.ID, cSpec{Val: "resurrect"})
	require.ErrorIs(t, err, ErrNotFound, "the id-keyed write refuses to act on a collected row")

	_, err = client.Get(ctx, first.ID)
	require.ErrorIs(t, err, ErrNotFound)

	// The delete pair splits the same way: the name deletes the live row, the dead
	// id reports absence rather than silently succeeding.
	require.ErrorIs(t, client.Delete(ctx, first.ID), ErrNotFound)
	require.NoError(t, client.DeleteByName(ctx, "prod"))
	live, err := client.Get(ctx, second.ID)
	require.NoError(t, err)
	assert.NotNil(t, live.DeletionRequestedAt, "the name-keyed delete marked the live incarnation")
}

// resolveProbeStore records whether anything asked it to resolve a name to a row.
// A name-keyed write must not be composed as "look the name up, then write by the
// id it returned": those are two store calls with no transaction across them, so a
// concurrent collect could retire the row and hand its name to a replacement in
// between, landing the write on an incarnation the caller never named.
type resolveProbeStore struct {
	Store
	resolved atomic.Bool
}

func (s *resolveProbeStore) ObjectsGetByName(ctx context.Context, gk GroupKind, name string) (*RawObject, error) {
	s.resolved.Store(true)
	return s.Store.ObjectsGetByName(ctx, gk, name)
}

// The single-threaded ABA test above cannot catch a resolve-then-write Update:
// with no concurrent collect to interleave, composing the two calls lands on the
// same row and every assertion passes. So pin the shape directly — the name goes
// to the store's own name-keyed mutator, which resolves and writes inside one
// transaction.
func TestClientUpdateDoesNotResolveTheNameSeparately(t *testing.T) {
	ctx := context.Background()
	probe := &resolveProbeStore{Store: newClientTestStore(t)}
	bh := newTestBeehive(t, probe)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	mustCreate(t, ctx, client, "prod", cSpec{Val: "v1"})
	probe.resolved.Store(false)

	_, err := client.UpdateByName(ctx, "prod", cSpec{Val: "v2"})
	require.NoError(t, err)

	assert.False(t, probe.resolved.Load(),
		"Update resolved the name through a separate store read; it must go through ObjectsUpdateSpecByName, which resolves and writes in one transaction")
}

// The generate-and-retry loop GenerateName's doc recommends, exercised against a
// real collision. Forced rather than waited for: a genuine UUIDv7 collision is
// ~10^-15, so the only way this loop is ever executed is a stubbed generator — which
// is also the failure it exists to insure against, since generation degenerating is
// far likelier than luck running out.
func TestGenerateNameRetryLoopSurvivesACollision(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	// Something already holds the name the generator is about to produce.
	mustCreate(t, ctx, client, "cache-fixed", cSpec{Val: "incumbent"})

	// A generator that hands out that taken name twice before recovering.
	names := []string{"cache-fixed", "cache-fixed", "cache-unique"}
	var i int
	next := func() string {
		s := names[i]
		i++
		return s
	}

	var last *Object[cSpec, cStatus]
	for range len(names) {
		obj, err := client.Create(ctx, next(), cSpec{Val: "v"})
		if errors.Is(err, ErrNameTaken) {
			continue
		}
		require.NoError(t, err)
		last = obj
		break
	}
	require.NotNil(t, last, "the loop must recover once the generator stops repeating")

	// Two rows, not three: the collided create landed nothing.
	all, err := client.List(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 2)
	assert.Equal(t, "cache-unique", last.Name)
}

// The README/godoc example, compiled so it cannot drift into not building.
func TestGenerateNameDocExampleCompiles(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	obj, err := client.Create(ctx, GenerateName("cache"), cSpec{Val: "v"})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(obj.Name, "cache-"))
}

// GenerateName is for callers with no natural name. It is deliberately explicit —
// nothing generates a name for you — because a name the caller never chose is one
// nobody can look up, which is the NULL name in a costume.
func TestGenerateNameKeepsThePrefixAndIsUnique(t *testing.T) {
	const n = 10_000
	seen := make(map[string]struct{}, n)
	for range n {
		name := GenerateName("cache")
		require.True(t, strings.HasPrefix(name, "cache-"), "the prefix is the caller's, kept verbatim: %q", name)
		require.Len(t, name, len("cache-")+36, "a UUIDv7 suffix is 36 characters")
		_, dup := seen[name]
		require.False(t, dup, "generated a name twice: %q", name)
		seen[name] = struct{}{}
	}
}

// UUIDv7 leads with a 48-bit millisecond timestamp, so names sharing a prefix sort
// lexicographically by creation time. google/uuid's NewV7 also carries a monotonic
// 12-bit sub-millisecond counter, which makes that ordering strict within a process
// rather than merely likely — two calls in the same millisecond still order.
func TestGenerateNameSortsByCreationTime(t *testing.T) {
	var prev string
	for range 1000 {
		name := GenerateName("cache")
		if prev != "" {
			require.Greater(t, name, prev, "each name must sort after the one before it")
		}
		prev = name
	}
}

// The empty prefix is allowed: the result is still a valid, non-empty name, so
// there is nothing for ErrInvalidName to catch. A caller who wants no prefix should
// not have to invent one.
func TestGenerateNameAcceptsAnEmptyPrefix(t *testing.T) {
	name := GenerateName("")
	assert.NotEmpty(t, name)
	assert.NoError(t, checkName(name))
}

// GenerateName's error branch is unreachable in production — io.ReadFull of 16
// bytes from a 16-byte reader cannot come up short — but the branch has to stay,
// because the value behind a swallowed error is uuid.Nil, and every name collapsing
// to one constant would surface as ErrNameTaken on every create after the first. So
// pin that it panics rather than returning that constant.
func TestGenerateNamePanicsRatherThanReturningTheNilUUID(t *testing.T) {
	orig := newUUIDv7
	t.Cleanup(func() { newUUIDv7 = orig })
	newUUIDv7 = func(io.Reader) (uuid.UUID, error) { return uuid.Nil, errBoom }

	assert.PanicsWithValue(t,
		"beehive: unreachable: UUIDv7 from a 16-byte reader: "+errBoom.Error(),
		func() { GenerateName("cache") },
		"a swallowed error would make every name identical")
}

// specWriteFixture wires a Beehive with one registered kind and does not start it.
// Register builds the work queue, so an enqueue is observable with no driver
// running at all — which is the point: these tests assert that the *write* queued
// the object, not that some pass later found it.
func specWriteFixture(t *testing.T) (*Beehive, Client[cSpec, cStatus], ControllerClient[cStatus], *reconciler) {
	t.Helper()
	bh := newTestBeehive(t, newClientTestStore(t))
	cc, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	return bh, NewClient[cSpec, cStatus](bh, clientTestGK), cc, mustReconciler(t, bh, clientTestGK)
}

// settle drives the generation handshake to "converged" and empties the queue, so
// a following test step starts from a row that owes nothing.
func settle(t *testing.T, ctx context.Context, cc ControllerClient[cStatus], r *reconciler, obj *Object[cSpec, cStatus]) {
	t.Helper()
	require.NoError(t, cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{Val: "done"}))
	drainQueue(r.work)
	require.Empty(t, queuedIDs(r.work), "settle must leave the queue empty")
}

// A create enqueues its own first reconcile, so a controller runs against a new
// object without waiting for the owed pass.
func TestCreateEnqueuesItsFirstReconcile(t *testing.T) {
	ctx := context.Background()
	_, client, _, r := specWriteFixture(t)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	assert.Equal(t, []ObjectID{obj.ID}, queuedIDs(r.work), "a create queues the new object")
}

// A live owner is queued by nothing: it was never waiting on this child, and
// requeueNow bypasses the re-enqueue floor, so waking every owner a create names
// would let a controller that replaces a child each pass drive its owner.
func TestCreateUnderALiveOwnerQueuesOnlyTheChild(t *testing.T) {
	ctx := context.Background()
	_, client, cc, r := specWriteFixture(t)

	owner := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "owner"})
	settle(t, ctx, cc, r, owner)

	child := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "child"}, WithOwner(owner.ID))
	assert.Equal(t, []ObjectID{child.ID}, queuedIDs(r.work), "the live owner owes nothing")
}

// A deletion-pending owner's cascade may already have run past this child, and
// nothing else would find it before the sweeper: no version moves for an edge,
// and the child's own collect returns at once because the child is not deleting.
func TestCreateUnderADeletingOwnerQueuesTheOwner(t *testing.T) {
	ctx := context.Background()
	_, client, cc, r := specWriteFixture(t)

	owner := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "owner"})
	settle(t, ctx, cc, r, owner)
	require.NoError(t, client.Delete(ctx, owner.ID))
	drainQueue(r.work) // spend the delete's own push

	child := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "child"}, WithOwner(owner.ID))
	assert.ElementsMatch(t, []ObjectID{child.ID, owner.ID}, queuedIDs(r.work),
		"the owner needs a re-cascade to find the new child")
}

// GetOrCreate queues only when it actually created the row: the found branch is a
// pure read and must not nudge the reconciler.
func TestGetOrCreateEnqueuesOnlyWhenItCreates(t *testing.T) {
	ctx := context.Background()
	_, client, cc, r := specWriteFixture(t)
	name := uniqueName()

	obj, created, err := client.GetOrCreate(ctx, name, cSpec{Val: "a"})
	require.NoError(t, err)
	require.True(t, created)
	assert.Equal(t, []ObjectID{obj.ID}, queuedIDs(r.work), "the created branch queues")

	settle(t, ctx, cc, r, obj)
	_, created, err = client.GetOrCreate(ctx, name, cSpec{Val: "b"})
	require.NoError(t, err)
	require.False(t, created)
	assert.Empty(t, queuedIDs(r.work), "the found branch writes nothing and queues nothing")
}

// A spec change enqueues the object at once, which is the latency this closes: the
// owed pass would otherwise be the first thing to list it.
func TestUpdateEnqueuesTheObject(t *testing.T) {
	ctx := context.Background()
	_, client, cc, r := specWriteFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	settle(t, ctx, cc, r, obj)

	updated, err := client.Update(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)
	require.Greater(t, updated.Generation, obj.Generation, "a real spec change bumps the generation")
	assert.Equal(t, []ObjectID{obj.ID}, queuedIDs(r.work), "a spec change queues the object")
}

// The gate is the row's settledness, not the fact that Update was called. A
// byte-identical write skips the store write, so the generation does not move and
// the row stays settled — and enqueueing it anyway is how a controller that
// re-applies its own spec would wake itself forever.
func TestNoOpUpdateOnASettledObjectEnqueuesNothing(t *testing.T) {
	ctx := context.Background()
	_, client, cc, r := specWriteFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	settle(t, ctx, cc, r, obj)

	same, err := client.Update(ctx, obj.ID, cSpec{Val: "a"})
	require.NoError(t, err)
	require.Equal(t, obj.Generation, same.Generation, "identical bytes must not bump the generation")
	assert.Empty(t, queuedIDs(r.work), "a settled row that did not move owes no pass")
}

// A delete enqueues its own object, so the controller gets to clear finalizers
// without waiting out a GC tick — the asymmetry with Create that this closes.
func TestDeleteEnqueuesItsOwnObject(t *testing.T) {
	ctx := context.Background()
	_, client, cc, r := specWriteFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	settle(t, ctx, cc, r, obj)

	require.NoError(t, client.Delete(ctx, obj.ID))
	assert.Equal(t, []ObjectID{obj.ID}, queuedIDs(r.work), "the delete queues the object")
}

// The name sibling pushes the same way; its id comes from the resolve the mark
// already did.
func TestDeleteByNameEnqueuesItsOwnObject(t *testing.T) {
	ctx := context.Background()
	_, client, cc, r := specWriteFixture(t)
	name := uniqueName()
	obj := mustCreate(t, ctx, client, name, cSpec{Val: "a"})
	settle(t, ctx, cc, r, obj)

	require.NoError(t, client.DeleteByName(ctx, name))
	assert.Equal(t, []ObjectID{obj.ID}, queuedIDs(r.work), "the delete queues the row the name held")
}

// EdgesHasIncoming discounts a depends_on edge from a deletion-pending source, so
// marking the referrer lifts the target's RESTRICT then and there. Without this
// push the target waits out a GC interval.
func TestDeleteRequestPushesTheBlockedTarget(t *testing.T) {
	ctx := context.Background()
	_, client, cc, r := specWriteFixture(t)
	target := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "target"})
	dependent := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "dependent"})
	require.NoError(t, cc.DependenciesAdd(ctx, dependent.ID, target.ID))
	require.NoError(t, client.Delete(ctx, target.ID))
	drainQueue(r.work)

	require.NoError(t, client.Delete(ctx, dependent.ID))
	assert.ElementsMatch(t, []ObjectID{dependent.ID, target.ID}, queuedIDs(r.work),
		"the mark queues itself and the target it unblocked")
}

// The name sibling resolves to the same row, so it pushes the same target.
func TestDeleteByNamePushesTheBlockedTarget(t *testing.T) {
	ctx := context.Background()
	_, client, cc, r := specWriteFixture(t)
	target := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "target"})
	name := uniqueName()
	dependent := mustCreate(t, ctx, client, name, cSpec{Val: "dependent"})
	require.NoError(t, cc.DependenciesAdd(ctx, dependent.ID, target.ID))
	require.NoError(t, client.Delete(ctx, target.ID))
	drainQueue(r.work)

	require.NoError(t, client.DeleteByName(ctx, name))
	assert.ElementsMatch(t, []ObjectID{dependent.ID, target.ID}, queuedIDs(r.work))
}

// A live target was never blocked, and requeueNow bypasses the floor: pushing one
// would let a controller deleting a dependent each pass spin.
func TestDeleteRequestPushesNoLiveTarget(t *testing.T) {
	ctx := context.Background()
	_, client, cc, r := specWriteFixture(t)
	target := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "target"})
	dependent := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "dependent"})
	require.NoError(t, cc.DependenciesAdd(ctx, dependent.ID, target.ID))
	drainQueue(r.work)

	require.NoError(t, client.Delete(ctx, dependent.ID))
	assert.Equal(t, []ObjectID{dependent.ID}, queuedIDs(r.work), "only the object it marked")
}

// owned_by always counts until physical removal, so marking a child lifts nothing
// on its owner — that is route 2's push, not this one.
func TestDeleteRequestPushesNoOwnedByTarget(t *testing.T) {
	ctx := context.Background()
	_, client, _, r := specWriteFixture(t)
	owner := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "owner"})
	child := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "child"}, WithOwner(owner.ID))
	require.NoError(t, client.Delete(ctx, owner.ID))
	drainQueue(r.work)

	require.NoError(t, client.Delete(ctx, child.ID))
	assert.Equal(t, []ObjectID{child.ID}, queuedIDs(r.work), "an owned_by target is not unblocked")
}

// The repeat stamps nothing, so it has nothing to report: the push rides the mark.
func TestRepeatedDeleteRequestPushesOnce(t *testing.T) {
	ctx := context.Background()
	_, client, cc, r := specWriteFixture(t)
	target := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "target"})
	dependent := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "dependent"})
	require.NoError(t, cc.DependenciesAdd(ctx, dependent.ID, target.ID))
	require.NoError(t, client.Delete(ctx, target.ID))
	require.NoError(t, client.Delete(ctx, dependent.ID))
	drainQueue(r.work)

	require.NoError(t, client.Delete(ctx, dependent.ID))
	assert.Empty(t, queuedIDs(r.work), "the repeat marked nothing, so it queues nothing")
}

// The gate is the store's report that this call stamped the row. Without it a
// caller retrying Delete would re-arm the object on every attempt.
func TestRepeatedDeleteEnqueuesOnce(t *testing.T) {
	ctx := context.Background()
	_, client, cc, r := specWriteFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	settle(t, ctx, cc, r, obj)

	require.NoError(t, client.Delete(ctx, obj.ID))
	drainQueue(r.work)

	require.NoError(t, client.Delete(ctx, obj.ID))
	assert.Empty(t, queuedIDs(r.work), "the repeat stamped nothing, so it queues nothing")
}

// The enqueue rides AfterCommit, so an outer transaction that rolls back discards
// it along with the write. Without that, a caller would see a reconcile for a spec
// change that never landed.
func TestSpecWriteEnqueuesNothingOnRollback(t *testing.T) {
	runCommitRollback(t, func(t *testing.T, commit bool) {
		ctx := context.Background()
		_, client, cc, r := specWriteFixture(t)
		obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
		settle(t, ctx, cc, r, obj)

		errRollback := errors.New("rollback")
		err := cc.Within(ctx, func(ctx context.Context) error {
			if _, err := client.Update(ctx, obj.ID, cSpec{Val: "b"}); err != nil {
				return err
			}
			if commit {
				return nil
			}
			return errRollback
		})
		if commit {
			require.NoError(t, err)
			assert.Equal(t, []ObjectID{obj.ID}, queuedIDs(r.work), "a committed spec change queues")
			return
		}
		require.ErrorIs(t, err, errRollback)
		assert.Empty(t, queuedIDs(r.work), "a rolled-back spec change queues nothing")
	})
}

// A client-only kind has no reconciler to enqueue into. The hook resolves to
// nothing and the write succeeds, rather than erroring or panicking.
func TestSpecWriteOnAClientOnlyKindEnqueuesNothing(t *testing.T) {
	ctx := context.Background()
	bh := newTestBeehive(t, newClientTestStore(t))
	clientOnly := NewClient[cSpec, cStatus](bh, GroupKind{Kind: "NoController"})

	obj, err := clientOnly.Create(ctx, uniqueName(), cSpec{Val: "a"})
	require.NoError(t, err)
	_, err = clientOnly.Update(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)
}

// Composing a spec write and the matching status write in one outer transaction
// commits a settled row, and still enqueues it. The gate reads the row as the spec
// write left it — unsettled at that moment — so the enqueue is registered before the
// status write settles the row.
//
// This is a duplicate, not a defect: the object is dispatched once more, reconciles
// against current state and settles. Pinning it here because the alternative is
// worse. Checking the committed row would cost a store read on every spec write, and
// after the commit a read returns *current* state rather than what this transaction
// wrote — so it would still not be exact, and it would fail in the direction that
// skips an enqueue for work another transaction had just made owed.
func TestSpecThenStatusInOneTransactionStillEnqueues(t *testing.T) {
	ctx := context.Background()
	_, client, cc, r := specWriteFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	settle(t, ctx, cc, r, obj)

	require.NoError(t, cc.Within(ctx, func(ctx context.Context) error {
		updated, err := client.Update(ctx, obj.ID, cSpec{Val: "b"})
		if err != nil {
			return err
		}
		return cc.UpdateStatus(ctx, obj.ID, updated.Generation, cStatus{Val: "done"})
	}))

	// The committed row is settled, so the owed pass would not list it...
	bh := client.(*clientImpl[cSpec, cStatus]).bh
	unsettled, err := bh.store.ObjectsListUnsettledIDs(ctx, clientTestGK)
	require.NoError(t, err)
	assert.Empty(t, unsettled, "the committed row is settled")

	// ...and the enqueue stands anyway, from the moment the spec write left it.
	assert.Equal(t, []ObjectID{obj.ID}, queuedIDs(r.work),
		"the gate reads the row this write produced, so composition enqueues a duplicate")
}

// respecController re-applies its own byte-identical spec on every reconcile and
// then fails. CLAUDE.md names this shape directly: the store's no-op skip is what
// stops a controller re-applying its own spec from waking itself forever, and the
// spec-write enqueue must not defeat it.
type respecController struct {
	client     Client[cSpec, cStatus]
	calls      atomic.Int64
	first, hot *signal
}

func (c *respecController) Reconcile(ctx context.Context, _ ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
	if c.calls.Add(1) >= hotLoopCalls {
		c.hot.fire()
	}
	c.first.fire()
	_, _ = c.client.Update(ctx, obj.ID, obj.Spec) // identical bytes: the store skips it
	return Result{}, errBoom
}

// A failing controller that re-writes its own spec must stay on its backoff ladder.
//
// The enqueue is gated on the store reporting that it *changed* the object, not on
// the row being unsettled. A failing reconcile leaves the row unsettled forever, so
// an unsettledness gate would fire on every one of these no-op writes — and the
// enqueue is worse than a scan would be, because requeueNow cancels the backoff
// alarm and marks the in-flight id dirty, so work.done redispatches it at once. The
// result is a retry loop at full speed that never reaches the ladder.
func TestFailingRespecControllerKeepsItsBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t), withoutGCSweeper())
	ctrl := &respecController{first: newSignal(), hot: newSignal()}
	_, err := Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	ctrl.client = client

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(context.Background())

	_ = mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	requireNoHotLoop(t, ctrl.first, ctrl.hot, &ctrl.calls,
		"a no-op write from a failing reconcile must not requeue past the backoff")
}

// The gate is "this write changed the object", not "the row is unsettled". A no-op
// write on a row that is unsettled for some other reason enqueues nothing.
func TestNoOpUpdateOnAnUnsettledObjectEnqueuesNothing(t *testing.T) {
	ctx := context.Background()
	_, client, _, r := specWriteFixture(t)

	// Never settled: observed_generation is NULL, so ObjectsListUnsettledIDs lists it.
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	drainQueue(r.work)
	require.Empty(t, queuedIDs(r.work))

	same, err := client.Update(ctx, obj.ID, cSpec{Val: "a"})
	require.NoError(t, err)
	require.Equal(t, obj.Generation, same.Generation, "identical bytes must not bump the generation")

	unsettled, err := client.(*clientImpl[cSpec, cStatus]).bh.store.ObjectsListUnsettledIDs(ctx, clientTestGK)
	require.NoError(t, err)
	require.Equal(t, []ObjectID{obj.ID}, unsettled, "the row is still unsettled")
	assert.Empty(t, queuedIDs(r.work), "an unsettled row is not a reason to enqueue a write that changed nothing")
}

// TestPollFailedSeparatesShutdownFromFailure pins the two answers a failed read
// gets, directly rather than through a stream. Reaching them from a live watch
// means cancelling a context while a read is in flight, which is a race the test
// would sometimes lose — and a coverage gate turns a lost race into a red build
// with no defect behind it.
//
// The distinction itself is the point: a store error is a fault worth reporting
// and worth one more tick, while a cancelled context is this stream shutting down
// and neither. Warning there would put a line in the log on every clean
// unsubscribe.
func TestPollFailedSeparatesShutdownFromFailure(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	c := &clientImpl[cSpec, cStatus]{bh: &Beehive{logger: logger}, gk: clientTestGK}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, c.pollFailed(ctx, "watch", errBoom), "a cancelled read ends the loop")
	assert.Empty(t, buf.String(), "shutdown is not a fault to report")

	assert.True(t, c.pollFailed(context.Background(), "watch", errBoom),
		"a store error costs the tick, not the stream")
	assert.Contains(t, buf.String(), "watch poll failed")
	assert.Contains(t, buf.String(), errBoom.Error())
}

// sendOrDone reports the send it could not make. Without it a subscriber that
// stopped reading would wedge its own poll goroutine, which then never observes
// the cancellation that was meant to release it.
func TestSendOrDoneReportsACancelledSend(t *testing.T) {
	out := make(chan int) // unbuffered, and nobody is reading

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, sendOrDone(ctx, out, 1), "a cancelled context abandons the send")

	// A reader waiting is the other half: the send lands and is reported as landed.
	got := make(chan int, 1)
	go func() { got <- <-out }()
	assert.True(t, sendOrDone(context.Background(), out, 7), "a send with a reader lands")
	assert.Equal(t, 7, <-got)
}

// Stream tests. These go through SchedulesWatch and never Peek: the stream
// goroutine is the receiver's consumer, so a Peek racing it proves nothing.
//
// Every one of them runs with the poll turned off, so a value a test observes is
// provably the hub's. Without that they would pass on the poll alone and say
// nothing about push.
