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
