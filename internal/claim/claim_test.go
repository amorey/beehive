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

package claim

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTakeHoldsOneKeyAtATime(t *testing.T) {
	var s Set

	a, ok := s.Take("a")
	require.True(t, ok, "the zero value is ready to use")
	_, ok = s.Take("a")
	assert.False(t, ok)
	_, ok = s.Take("b")
	assert.True(t, ok, "a second key is unaffected")

	s.Drop(a)
	_, ok = s.Take("a")
	assert.True(t, ok, "the drop released it")
}

// The zero Held is what a holder that never claimed carries, so dropping it
// must release nothing.
func TestDropIgnoresTheZeroHeld(t *testing.T) {
	var s Set
	held, ok := s.Take("a")
	require.True(t, ok)

	s.Drop(Held{})
	_, ok = s.Take("a")
	assert.False(t, ok, "the zero Held released someone else's claim")

	s.Drop(held)
	_, ok = s.Take("a")
	assert.True(t, ok)
}

// A stale Held names a claim that has since ended, which is what frees every
// holder from tracking whether its own release already ran.
func TestDropIgnoresAStaleHeld(t *testing.T) {
	var s Set
	first, ok := s.Take("a")
	require.True(t, ok)
	s.Drop(first)

	second, ok := s.Take("a")
	require.True(t, ok)

	s.Drop(first)
	_, ok = s.Take("a")
	assert.False(t, ok, "a stale Held evicted the claim that replaced it")

	s.Drop(second)
	_, ok = s.Take("a")
	assert.True(t, ok)
}

func TestTakeIsSafeForConcurrentUse(t *testing.T) {
	var s Set
	var taken sync.WaitGroup
	wins := make(chan struct{}, 8)

	for range 8 {
		taken.Go(func() {
			if _, ok := s.Take("a"); ok {
				wins <- struct{}{}
			}
		})
	}
	taken.Wait()
	close(wins)
	assert.Len(t, wins, 1, "exactly one caller holds the key")
}
