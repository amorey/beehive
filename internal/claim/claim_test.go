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

	require.True(t, s.Take("a"), "the zero value is ready to use")
	assert.False(t, s.Take("a"))
	assert.True(t, s.Take("b"), "a second key is unaffected")

	s.Drop("a")
	assert.True(t, s.Take("a"), "the drop released it")
}

// Dropping a key nobody took is a no-op rather than a panic.
func TestDropIsSafeWithoutATake(t *testing.T) {
	var s Set
	s.Drop("a")
	assert.True(t, s.Take("a"))
}

func TestTakeIsSafeForConcurrentUse(t *testing.T) {
	var s Set
	var taken sync.WaitGroup
	wins := make(chan struct{}, 8)

	for range 8 {
		taken.Go(func() {
			if s.Take("a") {
				wins <- struct{}{}
			}
		})
	}
	taken.Wait()
	close(wins)
	assert.Len(t, wins, 1, "exactly one caller holds the key")
}
