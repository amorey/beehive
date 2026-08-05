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

package rategate

import (
	"strconv"
	"testing"
	"time"
)

// The work queue's gate is the cardinality that matters: interval 1s, key
// ObjectID (int64), one admission per dispatch. Every case below drives that
// shape — the single-key case the waker's gates exercise costs the same at any
// live-set size and so measures none of it.
const benchInterval = time.Second

// benchLiveKeys are the distinct-keys-per-interval points worth pinning: the
// rate a single store connection can plausibly feed, and an order past it.
var benchLiveKeys = []int{1, 1_000, 10_000}

// BenchmarkGateAdmitDistinctKeys admits N distinct keys per interval forever,
// which is the steady state the work queue's gate sits in: every admission
// finds one entry expired and so drives an eviction round.
//
// The clock advances interval/n per admission, so the live set holds at n and
// each Admit pops exactly one entry. That is the case where a per-eviction
// compaction copies the whole live tail.
func BenchmarkGateAdmitDistinctKeys(b *testing.B) {
	for _, n := range benchLiveKeys {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			g := New[int64](benchInterval)
			step := benchInterval / time.Duration(n)
			now := base

			// Fill to the steady-state live set before timing.
			for i := range n {
				g.Admit(int64(i), now)
				now = now.Add(step)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				g.Admit(int64(n+i), now)
				now = now.Add(step)
			}
		})
	}
}

// BenchmarkGateAllowHeldKey measures the refusal path against a full live set:
// the gate's most common call, since a held key is what the floor is for.
func BenchmarkGateAllowHeldKey(b *testing.B) {
	for _, n := range benchLiveKeys {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			g := New[int64](benchInterval)
			for i := range n {
				g.Admit(int64(i), base)
			}
			now := base.Add(benchInterval / 2)

			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if _, held := g.Allow(int64(i%n), now); !held {
					b.Fatal("key must be held")
				}
			}
		})
	}
}

// BenchmarkGateAdmitBurstThenDrain admits a full live set at one instant and
// then drains it in one eviction round, which is what a kind that dispatches a
// batch and goes quiet leaves behind.
func BenchmarkGateAdmitBurstThenDrain(b *testing.B) {
	for _, n := range benchLiveKeys {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				g := New[int64](benchInterval)
				for i := range n {
					g.Admit(int64(i), base)
				}
				g.Admit(-1, base.Add(2*benchInterval))
			}
		})
	}
}
