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

package sqlite

import (
	"context"
	"testing"

	"github.com/amorey/beehive/internal/storeapi"
)

// BenchmarkWithinNestedMutators isolates what the savepoint boundary costs: an
// outer Within enclosing several self-wrapping mutators, which is the shape
// gcCollect and the owed pass run on every tick. Each nested frame adds a SAVEPOINT
// and a RELEASE, and modernc.org/sqlite compiles each statement fresh, so this is
// where the prepare cost would show up if it mattered.
//
// Deliberately self-contained (no helpers from store_test.go) so it can be checked
// out against a pre-savepoint tree unchanged and the two runs compared.
func BenchmarkWithinNestedMutators(b *testing.B) {
	store, err := OpenMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	gk := storeapi.GroupKind{Kind: "Bench"}

	for b.Loop() {
		err := store.Within(ctx, func(ctx context.Context) error {
			for range 8 {
				if _, err := store.ObjectsCreate(ctx, &storeapi.RawObject{
					Group: gk.Group, Kind: gk.Kind, Spec: []byte(`{}`),
				}); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
