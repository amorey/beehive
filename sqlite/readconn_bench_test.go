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
	"sync"
	"testing"

	"github.com/amorey/beehive/internal/storeapi"
	"github.com/stretchr/testify/require"
)

// BenchmarkReadUnderWrites measures a bare read while writes are in flight,
// which is what the read pool exists for: the drivers scan on a cadence forever
// and every one of those reads used to sit in the writers' queue.
//
// BenchmarkWritesUnderWatch does not move on this change, and cannot: a watch
// tailer reads through ObjectWrites().ListSince, which self-wraps in Within and
// so still takes the writer. That is the grouped-read spec's to fix.
//
// On disk, since OpenMemory has no read pool.
func BenchmarkReadUnderWrites(b *testing.B) {
	for _, writers := range []int{0, 1, 4} {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			store, err := Open(filepath.Join(b.TempDir(), "bench.db"))
			require.NoError(b, err)
			defer store.Close()
			ctx := context.Background()

			obj, err := store.Objects().Create(ctx, testGK, storeapi.ObjectsCreateInput{
				Name: "read-target", Spec: []byte(`{}`),
			})
			require.NoError(b, err)

			targets := make([]storeapi.ObjectID, writers)
			for i := range targets {
				o, err := store.Objects().Create(ctx, testGK, storeapi.ObjectsCreateInput{
					Name: fmt.Sprintf("write-target-%d", i), Spec: []byte(`{}`),
				})
				require.NoError(b, err)
				targets[i] = o.ID
			}

			stop := make(chan struct{})
			var wg sync.WaitGroup
			for _, id := range targets {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for n := 0; ; n++ {
						select {
						case <-stop:
							return
						default:
						}
						status := fmt.Appendf(nil, `{"n":%d}`, n)
						if _, err := store.Objects().UpdateStatus(ctx, testGK, id, status, 0); err != nil {
							return
						}
					}
				}()
			}

			b.ResetTimer()
			for b.Loop() {
				if _, err := store.Objects().GetMeta(ctx, obj.ID); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			close(stop)
			wg.Wait()
		})
	}
}
