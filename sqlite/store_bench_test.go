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

// BenchmarkEventsSweep measures what the GC sweeper pays to enforce the event
// cap, as a function of how much of the log is actually over it.
//
// The question is whether the sweep costs the size of the table or the size of
// what it trims. The steady-state case is the one that matters for a default:
// a sweeper running every 30s forever spends almost all of its runs finding
// nothing to do, so "nothing over cap" is the cost a stock beehive pays.
//
// The store is on disk, not OpenMemory, because the scan is what is being
// measured and only a file database pages it in.
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
	b.Helper()
	ctx := context.Background()

	store, err := Open(filepath.Join(b.TempDir(), "bench.db"))
	require.NoError(b, err)
	defer store.Close()

	const cap = 4
	ids := make([]storeapi.ObjectID, timelines)
	for i := range ids {
		obj, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
			Name: fmt.Sprintf("bench-%d", i),
			Spec: []byte(`{}`),
		})
		require.NoError(b, err)
		ids[i] = obj.ID

		// A distinct reason per event, or the run extends instead of appending.
		for r := range cap {
			require.NoError(b, store.EventsAdd(ctx, testGK, obj.ID, storeapi.EventsAddInput{
				Category: "c", Type: "Normal", Reason: fmt.Sprintf("R%d", r),
			}))
		}
	}
	for i := range overCap {
		require.NoError(b, store.EventsAdd(ctx, testGK, ids[i], storeapi.EventsAddInput{
			Category: "c", Type: "Normal", Reason: "Extra",
		}))
	}

	b.ResetTimer()
	for range b.N {
		_, err := store.EventsSweep(ctx, cap, 0)
		require.NoError(b, err)
	}
}
