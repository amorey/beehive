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

// blockSize is how many resource versions one reservation covers. Above
// markChunkSize by enough that a full deletion chunk usually fits: a chunk the
// block cannot cover takes the fallback and burns the remainder. A var so tests
// can shrink it; 0 or less turns blocks off and sends every draw to the table.
var blockSize = 1024

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
// cannot cover n, and a refused take spends the block: the counter already holds
// the block's end, so anything left behind the caller's fallback would be handed
// out below what the fallback returned.
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
func (v *versions) record(hi int64) { v.reserve(hi, 0) }

// settle publishes everything handed out and reports whether a refill is owed.
// One call, because publishing must precede the reservation: after it, next-1
// names a version out of the new block that no write has taken.
func (v *versions) settle() bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.published = v.next - 1
	return v.next >= v.end
}

// latest is the highest version a committed write took. It lags a write by the
// moment between its commit and its publish, which over-reports staleness — the
// harmless direction. See the spec for why the sequence row cannot answer this.
func (v *versions) latest() int64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.published
}

// reserve claims the block a draw of n ending at hi covers, unless the allocator
// has already moved past it. next never decreases: two refills can be in flight at
// once, and a fallback draw can land between a refill's draw and its install, so
// the block installed last is not the block drawn last. A stale one is burned
// rather than applied — it would hand out a version below one already taken.
func (v *versions) reserve(hi int64, n int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	lo := hi - int64(n) + 1
	if lo < v.next {
		return
	}
	v.next, v.end = lo, hi+1
}
