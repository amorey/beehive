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
type Set struct {
	mu sync.Mutex
	m  map[string]struct{}
}

// Take claims k, reporting false if it is already held.
func (s *Set) Take(k string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, held := s.m[k]; held {
		return false
	}
	if s.m == nil {
		s.m = make(map[string]struct{})
	}
	s.m[k] = struct{}{}
	return true
}

// Drop releases k. Owed exactly once per successful Take: a second Drop would
// release whatever took k in between, so a holder that may release twice
// records its own claim and checks that record first.
func (s *Set) Drop(k string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, k)
}
