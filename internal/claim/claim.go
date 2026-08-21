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

// Set is a set of held keys. The zero value is ready to use, and a Set is safe
// for concurrent use.
//
// An interface type argument satisfies comparable but is not strictly
// comparable: a key whose dynamic type cannot be a map key panics here, as it
// would in any map. Callers holding interface keys reject those before the
// first Take.
type Set[K comparable] struct {
	mu sync.Mutex
	m  map[K]struct{}
}

// Take claims k, reporting false if it is already held.
func (s *Set[K]) Take(k K) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, held := s.m[k]; held {
		return false
	}
	if s.m == nil {
		s.m = make(map[K]struct{})
	}
	s.m[k] = struct{}{}
	return true
}

// Drop releases k. Owed exactly once per successful Take: a second Drop would
// release whatever took k in between, so a holder records its own claim and
// clears that record here.
func (s *Set[K]) Drop(k K) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, k)
}
