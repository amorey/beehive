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
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/amorey/beehive/sqlite"
	"github.com/stretchr/testify/require"
)

// BenchmarkWritesUnderWatch measures what a live watch costs the write path.
//
// The premise under test: the commit wake makes tailer reads write-rate-driven
// rather than time-driven, and every one of those reads takes the store's only
// connection (OpenPool sets MaxOpenConns to 1). So the question is not what a
// drain costs on its own but whether writers slow down when a tailer is reading
// beside them.
//
// The store is on disk, not OpenMemory: the contention is over one connection
// either way, but only a file database carries the WAL and the fsync that make
// a writer's wait observable.
//
// The beehive is never started, so no driver runs and the tailers are the only
// readers competing with the writes. That isolates this PR's cost; it is not a
// model of a running beehive.
func BenchmarkWritesUnderWatch(b *testing.B) {
	cases := []struct {
		name     string
		kinds    int
		watchers int  // per kind
		scoped   bool // watch one owner's children rather than the kind
	}{
		// The baseline: no tailer exists, so the wake hits the idle fast path.
		{"no-watcher", 1, 0, false},
		// One tailer reading beside the writer. The delta from the baseline is
		// the whole cost of the wake.
		{"one-watcher", 1, 1, false},
		// The PR's central claim: watches on a kind share one tailer, so this
		// should cost what one-watcher costs, not 64 times it.
		{"64-watchers-one-kind", 1, 64, false},
		// Read load scales with watched kinds. Writes round-robin, so each kind
		// is written a sixteenth as often but every kind holds a tailer.
		{"16-kinds-one-watcher-each", 16, 1, false},
		// An owner-scoped watch arms the tailer's owner lookup, so every drain
		// costs one batched edge read on top. The delta from one-watcher is that
		// read.
		{"one-owner-scoped-watcher", 1, 1, true},
	}
	// Both throttle settings in one run, so the writer-side effect of the floor
	// is a comparison rather than a checkout of the parent commit.
	for _, throttle := range []time.Duration{0, defaultWatchScanMinInterval} {
		b.Run("throttle="+throttle.String(), func(b *testing.B) {
			for _, tc := range cases {
				b.Run(tc.name, func(b *testing.B) {
					benchWritesUnderWatch(b, tc.kinds, tc.watchers, tc.scoped, throttle)
				})
			}
		})
	}
}

func benchWritesUnderWatch(b *testing.B, kinds, watchersPerKind int, scoped bool, throttle time.Duration) {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := sqlite.Open(filepath.Join(b.TempDir(), "bench.db"))
	require.NoError(b, err)
	defer store.Close()

	bh, err := New(store, withWatchScanMinInterval(throttle))
	require.NoError(b, err)

	clients := make([]Client[cSpec, cStatus], kinds)
	ids := make([]ObjectID, kinds)
	var watchers sync.WaitGroup
	for k := range kinds {
		gk := GroupKind{Kind: "Bench" + strconv.Itoa(k)}
		clients[k] = NewClient[cSpec, cStatus](bh, gk)

		var opts []Option
		var ownerID ObjectID
		if scoped {
			owner, err := clients[k].Create(ctx, "owner", cSpec{Val: "0"})
			require.NoError(b, err)
			ownerID = owner.ID
			opts = append(opts, WithOwner(ownerID))
		}
		obj, err := clients[k].Create(ctx, "bench", cSpec{Val: "0"}, opts...)
		require.NoError(b, err)
		ids[k] = obj.ID

		for range watchersPerKind {
			var ch <-chan ObjectChange[cSpec, cStatus]
			var err error
			if scoped {
				_, ch, err = clients[k].WatchOwnedObjects(ctx, ownerID)
			} else {
				var stream *ObjectListStream[cSpec, cStatus]
				if stream, err = clients[k].WatchList(ctx); err == nil {
					ch = stream.Changes
				}
			}
			require.NoError(b, err)
			watchers.Add(1)
			// Drain, or the fan-out coalesces into a slot nobody empties and
			// the benchmark measures a backlog rather than a write path.
			go func() {
				defer watchers.Done()
				for range ch {
				}
			}()
		}
	}

	// Each write must change the spec: a byte-identical write is skipped and
	// appends no log entry, so a constant spec would benchmark the no-op path.
	latencies := make([]time.Duration, b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := i % kinds
		start := time.Now()
		_, err := clients[k].Update(ctx, ids[k], cSpec{Val: strconv.Itoa(i)})
		latencies[i] = time.Since(start)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	slices.Sort(latencies)
	b.ReportMetric(float64(latencies[len(latencies)*99/100])/float64(time.Microsecond), "p99-us")
	b.ReportMetric(float64(latencies[len(latencies)/2])/float64(time.Microsecond), "p50-us")

	cancel()
	watchers.Wait()
}

// BenchmarkTailerDrainRateUnderSustainedWrites measures what one wake-driven
// drain costs. That is what the throttle and the page budget are set against:
// the throttle bounds drains per second, and a drain's cost times that rate is
// the share of the single connection the tailer holds away from the writers
// generating the wakes.
//
// The duty cycle a pair (budget, interval) buys is drain/(drain+interval), so
// the budget is picked as the largest whose full-budget drain leaves that under
// the target. The backlogs are the three shapes a wake finds: quiet is the
// common one — a scalar read and no listing — and full-budget is a resume,
// where the drain stops at the budget and the loop re-arms at the throttle.
func BenchmarkTailerDrainRateUnderSustainedWrites(b *testing.B) {
	ctx := context.Background()
	for _, budget := range []int{1, 2, 4, 8} {
		b.Run("budget="+strconv.Itoa(budget), func(b *testing.B) {
			for _, backlog := range []struct {
				name string
				rows int
			}{
				{"quiet", 0},
				{"one-page", tailPageCap},
				{"full-budget", budget * tailPageCap},
			} {
				b.Run(backlog.name, func(b *testing.B) {
					store, err := sqlite.Open(filepath.Join(b.TempDir(), "bench.db"))
					require.NoError(b, err)
					defer store.Close()

					bh, err := New(store, withWatchScanMinInterval(0))
					require.NoError(b, err)
					tailer, err := newObjectTailer(ctx, bh, clientTestGK)
					require.NoError(b, err)
					defer tailer.close()
					tailer.pagesPerDrain = budget

					rx := tailer.hub.Receiver()
					defer rx.Close()
					go func() {
						for {
							if _, err := rx.RecvContext(ctx); err != nil {
								return
							}
						}
					}()

					spec, err := json.Marshal(cSpec{})
					require.NoError(b, err)
					for i := range backlog.rows {
						_, err := store.Objects().Create(ctx, clientTestGK, ObjectsCreateInput{
							Name: "backlog-" + strconv.Itoa(i),
							Spec: spec,
						})
						require.NoError(b, err)
					}
					resume := tailer.cursor

					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						// Timed: one int64 store is far below the noise, and
						// StopTimer/StartTimer would cost two stop-the-worlds a drain.
						tailer.cursor = resume
						if _, err := tailer.drain(ctx); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
