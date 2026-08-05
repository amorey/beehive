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

import "time"

// Single is Gate for a caller with one thing to hold: same interval, same
// methods without the key. One instant is all it keeps, so there is no map, no
// eviction queue and nothing to reclaim.
type Single struct {
	interval time.Duration
	// opensAt is when the last admission's window closes, and admitted says one
	// happened. The zero time is a deadline like any other, so it cannot carry
	// that on its own.
	opensAt  time.Time
	admitted bool
}

// NewSingle builds a Single with a constant interval. A non-positive interval
// holds nothing, so a caller disables the gate without branching.
func NewSingle(interval time.Duration) *Single { return &Single{interval: interval} }

// Interval reports the hold this Single applies.
func (s *Single) Interval() time.Duration { return s.interval }

// OpensAt reports when the caller may next act; the second result is false when
// it is free now. It records no admission, so it never extends a window.
func (s *Single) OpensAt(now time.Time) (time.Time, bool) {
	if !s.admitted || !now.Before(s.opensAt) {
		return time.Time{}, false
	}
	return s.opensAt, true
}

// Allow is OpensAt followed by Admit when the caller is free: same (opensAt,
// held) pair, and the admission is recorded when held is false.
func (s *Single) Allow(now time.Time) (time.Time, bool) {
	if opensAt, held := s.OpensAt(now); held {
		return opensAt, true
	}
	s.Admit(now)
	return time.Time{}, false
}

// Admit records that the caller acted at now, holding it until
// now.Add(interval).
func (s *Single) Admit(now time.Time) {
	if s.interval <= 0 {
		return
	}
	s.opensAt, s.admitted = now.Add(s.interval), true
}
