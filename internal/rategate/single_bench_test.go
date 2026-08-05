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
	"testing"
	"time"
)

// The pair below is the evidence for Single existing: the same single-key
// workload the waker and the object tail run, against the keyed Gate they used
// to run it on. Both paths are the hot one — a wake either admits or is
// refused — and neither allocates, so what is left is the map and the eviction
// queue Single does not have.

func BenchmarkSingleAdmit(b *testing.B) {
	b.Run("single", func(b *testing.B) {
		s := NewSingle(benchInterval)
		now := base

		b.ReportAllocs()
		for range b.N {
			s.Admit(now)
			now = now.Add(benchInterval)
		}
	})
	b.Run("gate", func(b *testing.B) {
		g := New[struct{}](benchInterval)
		now := base

		b.ReportAllocs()
		for range b.N {
			g.Admit(struct{}{}, now)
			now = now.Add(benchInterval)
		}
	})
}

func BenchmarkSingleAllowHeld(b *testing.B) {
	const inside = time.Millisecond

	b.Run("single", func(b *testing.B) {
		s := NewSingle(benchInterval)
		s.Admit(base)

		b.ReportAllocs()
		for range b.N {
			_, _ = s.Allow(base.Add(inside))
		}
	})
	b.Run("gate", func(b *testing.B) {
		g := New[struct{}](benchInterval)
		g.Admit(struct{}{}, base)

		b.ReportAllocs()
		for range b.N {
			_, _ = g.Allow(struct{}{}, base.Add(inside))
		}
	})
}
