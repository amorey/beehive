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

// Package claim holds exclusive claims on keys, so one key has one holder at a
// time within the process.
package claim

import "sync"

// Held is what Take hands back, and what Drop releases. Its zero value holds
// nothing, so a holder that never claimed needs no flag of its own.
type Held struct {
	key string
	// tok distinguishes this claim from a later one on the same key. Never zero
	// for a real claim.
	tok uint64
}

// Set is a set of held keys. The zero value is ready to use, and a Set is safe
// for concurrent use.
type Set struct {
	mu   sync.Mutex
	m    map[string]uint64
	next uint64
}

// Take claims k, reporting false if it is already held.
func (s *Set) Take(k string) (Held, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, held := s.m[k]; held {
		return Held{}, false
	}
	if s.m == nil {
		s.m = make(map[string]uint64)
	}
	s.next++
	s.m[k] = s.next
	return Held{key: k, tok: s.next}, true
}

// Drop releases h, and does nothing if h is not the claim now held. Callers owe
// no bookkeeping for that: a second Drop, or one racing a Take that already
// re-claimed the key, cannot release the claim it finds.
func (s *Set) Drop(h Held) {
	if h.tok == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m[h.key] == h.tok {
		delete(s.m, h.key)
	}
}
