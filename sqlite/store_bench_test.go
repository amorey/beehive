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
	"fmt"
	"path/filepath"
	"testing"

	"github.com/amorey/beehive"
	"github.com/amorey/beehive/internal/storeapi"
	"github.com/stretchr/testify/require"
)

// BenchmarkEventsSweep measures what enforcing the cap costs against log size
// and against how much of the log is over it — the over-cap=0 rows are what a
// sweeper on a converged log pays every tick, forever.
//
// The store is on disk, not OpenMemory: the read is what is being measured and
// only a file database pages it in.
func BenchmarkEventsSweep(b *testing.B) {
	for _, timelines := range []int{512, 4096} {
		for _, overCap := range []int{0, 64} {
			b.Run(fmt.Sprintf("timelines=%d/over-cap=%d", timelines, overCap), func(b *testing.B) {
				benchEventsSweep(b, timelines, overCap)
			})
		}
	}
}

// benchEventsSweep seeds timelines runs at the cap, pushes overCap of them past
// it, and sweeps. The trim is idempotent after the first iteration, so the
// steady state is what repeats.
func benchEventsSweep(b *testing.B, timelines, overCap int) {
	ctx := context.Background()

	store, err := Open(filepath.Join(b.TempDir(), "bench.db"))
	require.NoError(b, err)
	defer store.Close()

	const perTimeline = 4
	ids := make([]storeapi.ObjectID, timelines)
	for i := range ids {
		obj, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
			Name: fmt.Sprintf("bench-%d", i),
			Spec: []byte(`{}`),
		})
		require.NoError(b, err)
		ids[i] = obj.ID

		// A distinct reason per event, or the run extends instead of appending.
		for r := range perTimeline {
			require.NoError(b, store.Events().Add(ctx, testGK, obj.ID, storeapi.EventsAddInput{
				Category: "c", Type: "Normal", Reason: fmt.Sprintf("R%d", r),
			}))
		}
	}
	for i := range overCap {
		require.NoError(b, store.Events().Add(ctx, testGK, ids[i], storeapi.EventsAddInput{
			Category: "c", Type: "Normal", Reason: "Extra",
		}))
	}

	b.ResetTimer()
	for range b.N {
		_, err := store.Events().Sweep(ctx, perTimeline, 0, 0)
		require.NoError(b, err)
	}
}

// BenchmarkDeletionCascade measures a cascade against the width of one level.
// The first-cascade rows carry the marks, so the slope between widths is the
// per-child cost of the batched stamp — the number markChunkSize is tuned
// against. The re-cascade rows are the control: an already-deleting level is a
// lone SELECT that never reaches the mark, so the difference between the two is
// how much of the width is marking and how much is the child lookup.
//
// The store is on disk, not OpenMemory: the counter row is the one contended
// row in the schema and only a file database makes its writes cost anything.
// The narrow rows carry b.StopTimer's overhead in wall clock, though not in the
// reported ns/op — the revive between iterations is untimed.
func BenchmarkDeletionCascade(b *testing.B) {
	for _, children := range []int{1, 16, 256} {
		for _, pass := range []struct {
			name     string
			deleting bool
		}{{"first", false}, {"re-cascade", true}} {
			b.Run(fmt.Sprintf("children=%d/%s", children, pass.name), func(b *testing.B) {
				benchDeletionCascade(b, children, pass.deleting)
			})
		}
	}
}

// benchDeletionCascade builds one owner over children owned rows and cascades to
// it. When deleting, the subtree is marked once up front and every iteration is
// the re-cascade; otherwise each iteration is reset to live first, untimed.
func benchDeletionCascade(b *testing.B, children int, deleting bool) {
	ctx := context.Background()

	store, err := Open(filepath.Join(b.TempDir(), "bench.db"))
	require.NoError(b, err)
	defer store.Close()

	owner, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name: "owner",
		Spec: []byte(`{}`),
	})
	require.NoError(b, err)
	for i := range children {
		child, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
			Name: fmt.Sprintf("child-%d", i),
			Spec: []byte(`{}`),
		})
		require.NoError(b, err)
		_, err = store.EdgesAdd(ctx, child.ID, owner.ID, storeapi.RelationOwnedBy)
		require.NoError(b, err)
	}

	// Untimed, and the assertion is the point: it pins that each variant measures
	// the pass it claims to, so a cascade that silently stopped marking would show
	// up as a speedup rather than a bug. The loop below revives before its first
	// timed cascade, so this leaves the subtree marked for either variant.
	res, err := store.DeletionRequests().CreateFromOwner(ctx, owner.ID)
	require.NoError(b, err)
	require.Len(b, res.Children, children)
	for _, ch := range res.Children {
		require.True(b, ch.Marked, "the first cascade marks every child")
	}

	b.ResetTimer()
	for range b.N {
		if !deleting {
			b.StopTimer()
			reviveChildren(b, store)
			b.StartTimer()
		}
		if _, err := store.DeletionRequests().CreateFromOwner(ctx, owner.ID); err != nil {
			b.Fatal(err)
		}
	}
}

// reviveChildren clears the deletion clock behind the store's back so the next
// iteration measures a first cascade again. Deliberately not a re-create: the
// row count, the ids and the child lookup's plan all stay fixed, so every timed
// iteration does identical work. It draws no version, so `objects` is untouched
// otherwise; `object_writes` still grows by one entry per marked child per
// iteration, which is the one thing that drifts across a run.
func reviveChildren(b *testing.B, store *sqliteStore) {
	b.Helper()
	_, err := store.db.ExecContext(context.Background(),
		`UPDATE objects SET deletion_requested_at = NULL WHERE deletion_requested_at IS NOT NULL`)
	require.NoError(b, err)
}
