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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// GetOwner errors with ErrNotLoaded when unloaded; once loaded, ok reports
// presence and folds away the loaded-but-ownerless case.
func TestObjectGetOwner(t *testing.T) {
	owner := Ref{ID: 7, Kind: "Cluster"}

	t.Run("not loaded errors", func(t *testing.T) {
		var o Object[struct{}, struct{}]
		_, _, err := o.GetOwner()
		assert.ErrorIs(t, err, ErrNotLoaded)
	})

	t.Run("loaded, no owner", func(t *testing.T) {
		o := Object[struct{}, struct{}]{loaded: LoadOwnerBit}
		got, ok, err := o.GetOwner()
		require.NoError(t, err)
		assert.False(t, ok, "loaded but ownerless is not present")
		assert.Equal(t, Ref{}, got)
	})

	t.Run("loaded with owner", func(t *testing.T) {
		o := Object[struct{}, struct{}]{loaded: LoadOwnerBit, owner: &owner}
		got, ok, err := o.GetOwner()
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, owner, got)
	})
}

// ListDependencies errors with ErrNotLoaded when unloaded; a loaded-but-empty
// result is an empty slice with a nil error.
func TestObjectListDependencies(t *testing.T) {
	deps := []Ref{{ID: 1}, {ID: 2}}

	t.Run("not loaded errors", func(t *testing.T) {
		var o Object[struct{}, struct{}]
		_, err := o.ListDependencies()
		assert.ErrorIs(t, err, ErrNotLoaded)
	})

	t.Run("loaded, empty", func(t *testing.T) {
		o := Object[struct{}, struct{}]{loaded: LoadDependenciesBit, dependencies: []Ref{}}
		got, err := o.ListDependencies()
		require.NoError(t, err, "loaded-empty is not an error")
		assert.Empty(t, got)
	})

	t.Run("loaded, non-empty", func(t *testing.T) {
		o := Object[struct{}, struct{}]{loaded: LoadDependenciesBit, dependencies: deps}
		got, err := o.ListDependencies()
		require.NoError(t, err)
		assert.Equal(t, deps, got)
	})
}

func TestObjectListDependents(t *testing.T) {
	dependents := []Ref{{ID: 3}}

	var unloaded Object[struct{}, struct{}]
	_, err := unloaded.ListDependents()
	assert.ErrorIs(t, err, ErrNotLoaded)

	o := Object[struct{}, struct{}]{loaded: LoadDependentsBit, dependents: dependents}
	got, err := o.ListDependents()
	require.NoError(t, err)
	assert.Equal(t, dependents, got)
}

func TestObjectListOwned(t *testing.T) {
	owned := []Ref{{ID: 4}, {ID: 5}}

	var unloaded Object[struct{}, struct{}]
	_, err := unloaded.ListOwned()
	assert.ErrorIs(t, err, ErrNotLoaded)

	empty := Object[struct{}, struct{}]{loaded: LoadOwnedBit, owned: []Ref{}}
	got, err := empty.ListOwned()
	require.NoError(t, err, "loaded-empty is not an error")
	assert.Empty(t, got)

	o := Object[struct{}, struct{}]{loaded: LoadOwnedBit, owned: owned}
	got, err = o.ListOwned()
	require.NoError(t, err)
	assert.Equal(t, owned, got)
}
