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

func TestSingleAdmitsWhenQuiet(t *testing.T) {
	s := NewSingle(time.Second)
	assert.Equal(t, time.Second, s.Interval())

	_, held := s.OpensAt(base)
	assert.False(t, held, "one never admitted is free")

	s.Admit(base)
	_, held = s.OpensAt(base.Add(time.Second))
	assert.False(t, held, "the window is closed at exactly interval later")
}

func TestSingleHoldsInsideItsWindow(t *testing.T) {
	s := NewSingle(time.Second)
	s.Admit(base)

	opensAt, held := s.OpensAt(base.Add(200 * time.Millisecond))
	require.True(t, held)
	assert.Equal(t, base.Add(time.Second), opensAt)
}

func TestSingleOpensAtRecordsNothing(t *testing.T) {
	s := NewSingle(time.Second)
	s.Admit(base)

	for range 5 {
		_, _ = s.OpensAt(base.Add(500 * time.Millisecond))
	}

	_, held := s.OpensAt(base.Add(time.Second))
	assert.False(t, held, "querying must not extend the window")
}

func TestSingleAllowAdmitsAndThenHolds(t *testing.T) {
	s := NewSingle(time.Second)

	_, held := s.Allow(base)
	require.False(t, held, "a free Single is allowed")

	opensAt, held := s.Allow(base.Add(200 * time.Millisecond))
	assert.True(t, held, "and allowing it recorded the admission")
	assert.Equal(t, base.Add(time.Second), opensAt)
}

func TestSingleAllowRecordsNothingWhenHeld(t *testing.T) {
	s := NewSingle(time.Second)
	s.Admit(base)

	for range 5 {
		_, _ = s.Allow(base.Add(500 * time.Millisecond))
	}

	_, held := s.Allow(base.Add(time.Second))
	assert.False(t, held, "a refused Allow must not extend the window")
}

func TestSingleZeroIntervalHoldsNothing(t *testing.T) {
	s := NewSingle(0)
	s.Admit(base)

	_, held := s.OpensAt(base)
	assert.False(t, held)
}

// The waker re-admits while still held: the window must move with it, or the
// floor would be measured from the first admission of a busy streak.
func TestSingleReAdmissionExtendsTheWindow(t *testing.T) {
	s := NewSingle(time.Second)
	s.Admit(base)
	s.Admit(base.Add(900 * time.Millisecond))

	opensAt, held := s.OpensAt(base.Add(1100 * time.Millisecond))
	require.True(t, held)
	assert.Equal(t, base.Add(1900*time.Millisecond), opensAt)
}

// The zero time is a deadline like any other, so it cannot double as "never
// admitted": an admission that lands one interval before it must still hold.
func TestSingleHoldsAcrossTheZeroTime(t *testing.T) {
	s := NewSingle(time.Second)
	at := time.Time{}.Add(-time.Second)
	s.Admit(at)

	opensAt, held := s.OpensAt(at)
	require.True(t, held, "a just-admitted Single must not read as free")
	assert.Equal(t, time.Time{}, opensAt)
}

// A now that goes backwards holds longer than the interval asks, which is the
// direction a floor may fail in; what it must never do is read a just-admitted
// Single as free.
func TestSingleBackwardsNowHoldsRatherThanOpens(t *testing.T) {
	s := NewSingle(time.Second)
	s.Admit(base.Add(2 * time.Second))

	_, held := s.OpensAt(base)
	assert.True(t, held, "a backwards now must not open the gate")
}
