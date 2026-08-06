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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// base is an arbitrary fixed instant. Every test drives the clock by hand, so
// none of them sleeps.
var base = time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)

func TestGateAdmitsAQuietKey(t *testing.T) {
	g := New[int](time.Second)

	_, held := g.OpensAt(1, base)
	assert.False(t, held, "a key never admitted is free")

	g.Admit(1, base)
	_, held = g.OpensAt(1, base.Add(time.Second))
	assert.False(t, held, "the window is closed at exactly interval later")
}

func TestGateHoldsAKeyInsideItsWindow(t *testing.T) {
	g := New[int](time.Second)
	g.Admit(1, base)

	opensAt, held := g.OpensAt(1, base.Add(200*time.Millisecond))
	require.True(t, held)
	assert.Equal(t, base.Add(time.Second), opensAt)

	_, held = g.OpensAt(2, base.Add(200*time.Millisecond))
	assert.False(t, held, "the hold is per key")
}

func TestGateOpensAtRecordsNothing(t *testing.T) {
	g := New[int](time.Second)
	g.Admit(1, base)

	for range 5 {
		_, _ = g.OpensAt(1, base.Add(500*time.Millisecond))
	}

	_, held := g.OpensAt(1, base.Add(time.Second))
	assert.False(t, held, "querying must not extend the window")
}

func TestGateAllowAdmitsAndThenHolds(t *testing.T) {
	g := New[int](time.Second)

	_, held := g.Allow(1, base)
	require.False(t, held, "a free key is allowed")

	opensAt, held := g.Allow(1, base.Add(200*time.Millisecond))
	assert.True(t, held, "and allowing it recorded the admission")
	assert.Equal(t, base.Add(time.Second), opensAt)
}

func TestGateAllowRecordsNothingWhenHeld(t *testing.T) {
	g := New[int](time.Second)
	g.Admit(1, base)

	for range 5 {
		_, _ = g.Allow(1, base.Add(500*time.Millisecond))
	}

	_, held := g.Allow(1, base.Add(time.Second))
	assert.False(t, held, "a refused Allow must not extend the window")
}

func TestGateZeroIntervalHoldsNothing(t *testing.T) {
	g := New[int](0)
	g.Admit(1, base)

	_, held := g.OpensAt(1, base)
	assert.False(t, held)
}

func TestGateEvictsExpiredKeys(t *testing.T) {
	g := New[int](time.Second)
	for i := range 100 {
		g.Admit(i, base)
	}
	require.Len(t, g.admitted, 100)

	// One admission past every window is enough: eviction is amortised onto Admit.
	g.Admit(1000, base.Add(2*time.Second))

	assert.Len(t, g.admitted, 1, "expired keys must not accumulate")
}

// Re-admitting a still-held key appends a second eviction entry while the first
// is live. Popping the first must not free the key.
func TestGateReAdmissionDoesNotFreeAHeldKey(t *testing.T) {
	g := New[int](time.Second)
	g.Admit(1, base)
	g.Admit(1, base.Add(900*time.Millisecond)) // re-admitted, now held to base+1.9s

	// base+1.1s is past the first entry's expiry and inside the second's window.
	g.Admit(2, base.Add(1100*time.Millisecond)) // drives eviction

	opensAt, held := g.OpensAt(1, base.Add(1100*time.Millisecond))
	require.True(t, held, "the stale entry must not free a re-admitted key")
	assert.Equal(t, base.Add(1900*time.Millisecond), opensAt)
}

// The eviction queue is bounded by the admissions inside one interval, not by
// the number of admissions: a steady one-in-one-out stream must not grow it.
func TestGateEvictionQueueStaysBounded(t *testing.T) {
	const live = 10
	g := New[int](time.Second)
	step := time.Second / live
	now := base
	for i := range live {
		g.Admit(i, now)
		now = now.Add(step)
	}

	for i := range 10_000 {
		g.Admit(live+i, now)
		now = now.Add(step)
		require.LessOrEqual(t, len(g.order)-g.head, live+1, "live entries")
		require.LessOrEqual(t, len(g.order), 4*live, "queue including the evicted head")
	}
	assert.LessOrEqual(t, len(g.admitted), live+1)
}

// Compaction moves the live entries down and must clear what it left behind:
// for a pointer-bearing K those slots would otherwise keep evicted keys
// reachable, which is the retention the eviction exists to prevent.
func TestGateCompactionClearsEvictedKeys(t *testing.T) {
	const live = 4
	g := New[*int](time.Second)
	step := time.Second / live
	now := base
	keys := make([]*int, 0, 4*live)
	admit := func() {
		k := new(int)
		*k = len(keys)
		keys = append(keys, k)
		g.Admit(k, now)
		now = now.Add(step)
	}
	for range live {
		admit()
	}
	// Enough admissions to drive at least one compaction round.
	for range 3 * live {
		admit()
	}
	require.Equal(t, 0, g.head, "the queue must have compacted")

	tail := g.order[len(g.order):cap(g.order)]
	require.NotEmpty(t, tail, "the backing array must have slots past len to check")
	for i, e := range tail {
		assert.Nil(t, e.key, "slot %d past len holds an evicted key", i)
	}
}

// OpensAt records nothing, but evicting is not recording: a caller that reads
// and stops admitting must still get its memory back.
func TestGateOpensAtEvictsExpiredKeys(t *testing.T) {
	g := New[int](time.Second)
	for i := range 100 {
		g.Admit(i, base)
	}
	require.Len(t, g.admitted, 100)

	_, held := g.OpensAt(500, base.Add(2*time.Second))

	assert.False(t, held)
	assert.Empty(t, g.admitted, "a read past every window reclaims")
}

// A burst that drains hands its backing arrays back; nothing else does, since
// neither a map nor a slice shrinks on its own.
func TestGateDrainedBurstReleasesItsMemory(t *testing.T) {
	g := New[int](time.Second)
	for i := range shrinkAt + 1 {
		g.Admit(i, base)
	}
	require.Greater(t, cap(g.order), shrinkAt)

	g.Admit(-1, base.Add(2*time.Second))

	assert.LessOrEqual(t, cap(g.order), 2, "the drained queue is not retained")
	assert.Len(t, g.admitted, 1, "and the gate still works after")
	_, held := g.OpensAt(-1, base.Add(2*time.Second))
	assert.True(t, held)
}

// A disabled gate allocates nothing at all: the work queue builds one before it
// knows whether a floor is configured.
func TestGateZeroIntervalAllocatesNothing(t *testing.T) {
	g := New[int](0)
	g.Admit(1, base)
	_, _ = g.Allow(1, base)
	g.Forget(1)

	assert.Nil(t, g.admitted)
	assert.Nil(t, g.order)
}

// A now that goes backwards strands an entry behind an unexpired head, so
// eviction cannot reach it. That costs retention and nothing else: the key still
// reads free the moment its own window closes, and the next round past the head
// reclaims it.
func TestGateBackwardsNowRetainsButNeverHolds(t *testing.T) {
	g := New[int](time.Second)
	g.Admit(1, base.Add(2*time.Second))
	g.Admit(2, base) // backwards: queued behind an entry that outlives it

	_, held := g.OpensAt(2, base.Add(1500*time.Millisecond))
	require.False(t, held, "past its own window, the stranded key is free")
	assert.Contains(t, g.admitted, 2, "but eviction has not reached it")

	g.Admit(3, base.Add(3*time.Second)) // past the head: the strand is reachable now

	assert.NotContains(t, g.admitted, 2, "and then it is reclaimed")
}

// Instants are kept relative to the first one the Gate saw, and time.Time.Sub
// saturates at ±292 years. A caller that anchors the Gate far from the instants
// it then uses must still get a held key held: the floor may lose precision at
// that separation, but it must not read a just-admitted key as free.
func TestGateHoldsAcrossASaturatingBase(t *testing.T) {
	for _, tc := range []struct {
		name       string
		anchor, at time.Time
	}{
		{"base far below", time.Time{}, base},
		{"base far above", base.AddDate(4000, 0, 0), base},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := New[int](time.Second)
			_, _ = g.OpensAt(1, tc.anchor) // anchors base

			g.Admit(1, tc.at)

			_, held := g.OpensAt(1, tc.at.Add(200*time.Millisecond))
			assert.True(t, held, "a key admitted 200ms ago is held")
		})
	}
}

// Re-anchoring bounds a stamp to ±292 years but not away from the edge: an
// instant just inside the boundary still overflows when the interval is added
// to it, and a wrapped window reads a just-admitted key as free.
func TestGateHoldsAtTheEdgeOfTheStampRange(t *testing.T) {
	g := New[int](time.Second)
	_, _ = g.OpensAt(1, base) // anchors base
	edge := base.Add(math.MaxInt64 - 1)

	g.Admit(1, edge)

	_, held := g.OpensAt(1, edge)
	assert.True(t, held, "a key admitted at this instant is held")
}

// The mirror: an eviction deadline computed just inside the low edge underflows,
// and a wrapped deadline reads every live entry as expired.
func TestGateEvictionHoldsAtTheEdgeOfTheStampRange(t *testing.T) {
	g := New[int](time.Second)
	_, _ = g.OpensAt(1, base) // anchors base
	edge := base.Add(-(math.MaxInt64 - 1))

	g.Admit(1, edge)
	g.Admit(2, edge) // drives eviction at the low edge

	_, held := g.OpensAt(1, edge)
	assert.True(t, held, "eviction must not free a key admitted at this instant")
}

// The same saturation must not run the other way either: an eviction deadline
// that underflows would drop every live key at once.
func TestGateSaturatingBaseDoesNotEvictLiveKeys(t *testing.T) {
	g := New[int](time.Second)
	_, _ = g.OpensAt(1, base.AddDate(4000, 0, 0)) // anchors base far above

	g.Admit(1, base)
	g.Admit(2, base.Add(time.Millisecond)) // drives eviction

	_, held := g.OpensAt(1, base.Add(2*time.Millisecond))
	assert.True(t, held, "eviction must not free a key inside its window")
}

func TestGateForgetReleasesAKey(t *testing.T) {
	g := New[int](time.Second)
	g.Admit(1, base)

	g.Forget(1)

	_, held := g.OpensAt(1, base)
	assert.False(t, held)
}

// Forget leaves an orphan in the eviction queue; it must not free a key
// admitted again afterwards.
func TestGateForgetThenReAdmitStaysHeld(t *testing.T) {
	g := New[int](time.Second)
	g.Admit(1, base)
	g.Forget(1)
	g.Admit(1, base.Add(500*time.Millisecond))

	g.Admit(2, base.Add(1100*time.Millisecond)) // drives eviction past the orphan

	_, held := g.OpensAt(1, base.Add(1100*time.Millisecond))
	assert.True(t, held)
}
