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

// Package rategate holds a key for a fixed interval after it acts. It owns no
// timer and starts no goroutine: the caller arms its own. Not safe for
// concurrent use; the caller's lock covers it.
package rategate

import "time"

// Gate holds each key for a fixed interval after it acts. A key not admitted
// within the interval is free, so an idle-to-active transition adds no delay.
type Gate[K comparable] struct {
	interval time.Duration
	admitted map[K]time.Time
	// order is admitted's eviction queue. The interval is constant, so
	// admission order is expiry order and popping the front is enough.
	order []entry[K]
}

// entry is one admission's place in the eviction queue: which key, and when it
// was admitted.
type entry[K comparable] struct {
	key K
	at  time.Time
}

// New builds a Gate with a constant interval. A non-positive interval holds
// nothing, so a caller disables the gate without branching.
func New[K comparable](interval time.Duration) *Gate[K] {
	return &Gate[K]{interval: interval, admitted: make(map[K]time.Time)}
}

// Interval reports the hold this Gate applies.
func (g *Gate[K]) Interval() time.Duration { return g.interval }

// OpensAt reports when k may next act; the second result is false when k is
// free now. It records nothing, for a caller whose test and action are at
// different points.
func (g *Gate[K]) OpensAt(k K, now time.Time) (time.Time, bool) {
	// No guard on interval: Admit records nothing when it is non-positive.
	at, ok := g.admitted[k]
	if !ok {
		return time.Time{}, false
	}
	opensAt := at.Add(g.interval)
	if !now.Before(opensAt) {
		return time.Time{}, false
	}
	return opensAt, true
}

// Admit records that k acted at now, holding it until now.Add(interval). It
// also evicts, so the map stays bounded by the keys admitted within one
// interval.
func (g *Gate[K]) Admit(k K, now time.Time) {
	if g.interval <= 0 {
		return
	}
	g.evict(now)
	g.admitted[k] = now
	g.order = append(g.order, entry[K]{key: k, at: now})
}

// evict pops expired entries off the front of the queue, dropping each key from
// the map only if the map's own record has expired too — a key re-admitted
// while held has a fresher record there, and its newer entry evicts it later.
func (g *Gate[K]) evict(now time.Time) {
	i := 0
	for ; i < len(g.order); i++ {
		e := g.order[i]
		if now.Before(e.at.Add(g.interval)) {
			break // expiry order: nothing after this expired either
		}
		if at, ok := g.admitted[e.key]; ok && !now.Before(at.Add(g.interval)) {
			delete(g.admitted, e.key)
		}
	}
	if i > 0 {
		g.order = append(g.order[:0], g.order[i:]...)
	}
}

// Forget drops k's record, for a caller that discards work rather than acting
// on it. Its queue entry stays behind; evict finds the key gone and skips it.
func (g *Gate[K]) Forget(k K) {
	delete(g.admitted, k)
}
