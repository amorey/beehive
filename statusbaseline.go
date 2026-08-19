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

package beehive

import (
	"bytes"
	"sync"
)

// statusBaseline is what the store holds for one pass's object, as far as that
// pass knows: the bytes it was loaded with, advanced by its own committed
// writes. UpdateStatus compares against it to skip a write the store would
// decline. Sound only because objects.status has a single writer while a pass
// runs, which TestObjectStatusIsWrittenInOnePlace pins.
// See docs/adr/2026-08-19-a-pass-skips-a-status-write-it-can-see-is-a-no-op.md.
type statusBaseline struct {
	mu     sync.Mutex
	status []byte
	// version is what the stored bytes carry, writeVersion what this build
	// stamps. They differ for a blob migrated on read, which decodes equal and
	// must still be rewritten.
	version      int
	writeVersion int

	// outstanding counts writes issued but not known to have committed. While it
	// is non-zero the stored bytes are unknown here, so every write reaches the
	// store; a write that fails or rolls back never promotes, so the pass stays
	// on the slow path from then on.
	outstanding int
}

func newStatusBaseline(raw *RawObject, writeVersion int) *statusBaseline {
	return &statusBaseline{status: raw.Status, version: raw.StatusVersion, writeVersion: writeVersion}
}

// claim reports whether a write of status at version must reach the store, and
// records it in flight when it must. It answers false only where the store's own
// compare would write nothing, and must stay a strict subset of that: a false
// negative costs a transaction, a false positive loses the write. A nil baseline
// claims everything.
func (b *statusBaseline) claim(status []byte, version int) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.outstanding == 0 && b.version == version && bytes.Equal(b.status, status) {
		return false
	}
	b.outstanding++
	return true
}

// promote records a committed write. The bytes travel with the hook rather than
// being read from here: it runs at the outermost commit, by which time a later
// write may have been issued and failed.
func (b *statusBaseline) promote(status []byte, version int) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.status, b.version = status, version
	b.outstanding--
}
