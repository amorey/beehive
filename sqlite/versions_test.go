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
	"testing"

	"github.com/stretchr/testify/assert"
)

// A block covers takes that fit and refuses the one that does not, and a refused
// take spends what is left: the fallback draws from the table, which already sits
// above the block, so a later take from the same block would go backwards.
func TestVersionBlockCoversWhatFits(t *testing.T) {
	t.Run("takes in order", func(t *testing.T) {
		v := versions{next: 1, end: 5}
		hi, ok := v.take(2)
		assert.True(t, ok)
		assert.Equal(t, int64(2), hi)

		hi, ok = v.take(2)
		assert.True(t, ok)
		assert.Equal(t, int64(4), hi)

		_, ok = v.take(1)
		assert.False(t, ok, "the block is spent")
	})

	t.Run("a refused take spends the block", func(t *testing.T) {
		v := versions{next: 1, end: 5}
		_, ok := v.take(9)
		assert.False(t, ok)

		_, ok = v.take(1)
		assert.False(t, ok, "a version left behind the fallback would go backwards")
	})

	t.Run("record carries the fallback's draw", func(t *testing.T) {
		v := versions{next: 1, end: 1}
		v.record(40)
		v.publish()
		assert.Equal(t, int64(40), v.latest())
	})

	t.Run("publish reports only what a commit took", func(t *testing.T) {
		v := versions{next: 1, end: 5}
		v.publish()
		assert.Equal(t, int64(0), v.latest(), "nothing has been handed out")

		_, _ = v.take(2)
		assert.Equal(t, int64(0), v.latest(), "drawn, not committed")
		v.publish()
		assert.Equal(t, int64(2), v.latest())
	})
}
