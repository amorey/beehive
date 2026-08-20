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

import "sync"

// blockSize is how many resource versions one reservation covers. A var so tests
// can shrink it; 0 or less turns blocks off and sends every draw to the table.
var blockSize = 256

// versions hands out resource versions from a block reserved by a committed
// write. Versions must be unique and increasing; they are not required to be
// contiguous, so a crash or a rollback burns the rest of a block.
//
// See docs/specs/2026-08-20-reserve-resource-versions-in-blocks.md.
type versions struct {
	mu        sync.Mutex
	next      int64 // next version to hand out
	end       int64 // one past the last version in the reserved block
	published int64 // highest version handed out by a committed transaction
}

// take hands out n versions, reporting the highest. ok is false when the block
// cannot cover n, and the block is spent either way: the table already holds the
// block's end, so a version left behind the caller's fallback would be handed out
// below one the fallback returned.
func (v *versions) take(n int) (int64, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.end-v.next < int64(n) {
		v.next = v.end
		return 0, false
	}
	v.next += int64(n)
	return v.next - 1, true
}

// record takes the highest version a fallback draw returned, so publish and the
// next block both continue above it.
func (v *versions) record(hi int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.next, v.end = hi+1, hi+1
}

// publish marks everything handed out so far as committed. Called after the
// outermost commit and before the refill, or it names a version out of the new
// block that no write has taken.
func (v *versions) publish() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.published = v.next - 1
}

// latest is the highest version a committed write took. It lags a write by the
// moment between its commit and its publish, which over-reports staleness — the
// harmless direction. See the spec for why the sequence row cannot answer this.
func (v *versions) latest() int64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.published
}

// reserve claims a block, using hi as the highest version the draw returned.
func (v *versions) reserve(hi int64, n int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.next, v.end = hi-int64(n)+1, hi+1
}

// spent reports whether the block has nothing left, which is what asks for a refill.
func (v *versions) spent() bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.next >= v.end
}
