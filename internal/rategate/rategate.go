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
// timer and starts no goroutine: the caller arms its own. Every method mutates,
// reads included, so the lock the caller covers it with must be an exclusive
// one. Instants given to one gate should be non-decreasing; going backwards
// costs retention, never a wrong answer.
//
// Two shapes: Gate holds a key space, and Single holds one instant for a caller
// with one thing to floor — same methods without the key, and no map or
// eviction queue behind them.
package rategate

import (
	"math"
	"time"
)

// shrinkAt is the queue capacity past which draining to empty hands the map and
// the queue back rather than keeping them. Neither a map nor a slice's backing
// array shrinks on its own, so a one-off burst would otherwise be retained for
// the life of the Gate.
const shrinkAt = 1024

// Gate holds each key for a fixed interval after it acts. A key not admitted
// within the interval is free, so an idle-to-active transition adds no delay.
type Gate[K comparable] struct {
	interval time.Duration
	// base anchors the instants below, which are nanoseconds since the first
	// time this Gate was given. An int64 is half a time.Time and carries no
	// pointer, so a comparable key leaves the Gate with nothing to trace.
	base  time.Time
	based bool

	admitted map[K]int64
	// order is admitted's eviction queue, live from head. The interval is
	// constant, so a non-decreasing now makes admission order expiry order and
	// popping the front is enough; one that goes backwards strands an entry
	// behind an unexpired head, which retains its key — never holds it — until
	// that head expires.
	order []entry[K]
	// head is order's first live entry. Eviction slides it instead of copying
	// the live tail, which is what keeps an eviction round amortised O(1).
	head int
}

// entry is one admission's place in the eviction queue: which key, and when it
// was admitted.
type entry[K comparable] struct {
	key K
	at  int64
}

// New builds a Gate with a constant interval. A non-positive interval holds
// nothing, so a caller disables the gate without branching.
func New[K comparable](interval time.Duration) *Gate[K] {
	// The map is built on first admission: a disabled gate allocates nothing.
	return &Gate[K]{interval: interval}
}

// Interval reports the hold this Gate applies.
func (g *Gate[K]) Interval() time.Duration { return g.interval }

// OpensAt reports when k may next act; the second result is false when k is
// free now. It records no admission, so it never extends a window — but it does
// evict and it does anchor, so it needs the same exclusive lock every other
// method does. For a caller whose test and action are at different points.
func (g *Gate[K]) OpensAt(k K, now time.Time) (time.Time, bool) {
	// No guard on interval: Admit records nothing when it is non-positive.
	n := g.stamp(now)
	// Evicting is not recording: it drops only keys that already read as free,
	// and it is the one thing that reclaims for a caller who has gone quiet.
	g.evict(n)
	at, ok := g.admitted[k]
	if !ok {
		return time.Time{}, false
	}
	opensAt := addInterval(at, int64(g.interval))
	if n >= opensAt {
		return time.Time{}, false
	}
	return g.base.Add(time.Duration(opensAt)), true
}

// Allow is OpensAt followed by Admit when k is free: same (opensAt, held)
// pair, and the admission is recorded when held is false. For a caller whose
// test and action are at one point.
func (g *Gate[K]) Allow(k K, now time.Time) (time.Time, bool) {
	if opensAt, held := g.OpensAt(k, now); held {
		return opensAt, true
	}
	g.Admit(k, now)
	return time.Time{}, false
}

// Admit records that k acted at now, holding it until now.Add(interval). It
// also evicts, so the map stays bounded by the keys admitted within one
// interval.
func (g *Gate[K]) Admit(k K, now time.Time) {
	if g.interval <= 0 {
		return
	}
	n := g.stamp(now)
	g.evict(n)
	if g.admitted == nil {
		g.admitted = make(map[K]int64)
	}
	g.admitted[k] = n
	g.order = append(g.order, entry[K]{key: k, at: n})
}

// Forget drops k's record, for a caller that discards work rather than acting
// on it. Its queue entry stays behind; evict finds the key gone and skips it.
func (g *Gate[K]) Forget(k K) {
	delete(g.admitted, k)
}

// stamp converts now to this Gate's internal instant, anchoring base on the
// first one it sees.
//
// time.Time.Sub saturates at ±292 years rather than reporting that it could
// not answer, and two instants that both saturate are indistinguishable — which
// would read a just-admitted key as free. So a saturating Sub re-anchors
// instead of returning the extreme.
func (g *Gate[K]) stamp(now time.Time) int64 {
	if !g.based {
		g.base, g.based = now, true
		return 0
	}
	switch d := now.Sub(g.base); d {
	case math.MaxInt64, math.MinInt64:
		// now is further from base than a Duration can carry, so every record
		// is from an era this Gate cannot measure against: expired if it is
		// behind, meaningless if it is ahead. Drop them and start from now.
		g.base, g.admitted, g.order, g.head = now, nil, nil, 0
		return 0
	default:
		return int64(d)
	}
}

// addInterval is at+d saturated high, and subInterval is now-d saturated low.
// Both take d >= 0. Wrapping either way opens the gate — a held key reads free,
// or eviction drops a live one — which is the one direction a floor must not
// fail in.
func addInterval(at, d int64) int64 {
	if sum := at + d; sum >= at {
		return sum
	}
	return math.MaxInt64
}

func subInterval(now, d int64) int64 {
	if diff := now - d; diff <= now {
		return diff
	}
	return math.MinInt64
}

// evict pops expired entries off the front of the queue, dropping each key from
// the map only if the map's own record has expired too — a key re-admitted
// while held has a fresher record there, and its newer entry evicts it later.
func (g *Gate[K]) evict(now int64) {
	if g.interval <= 0 {
		return // nothing was ever recorded; keeps subInterval's d >= 0 true
	}
	deadline := subInterval(now, int64(g.interval)) // at or before this: expired
	for ; g.head < len(g.order); g.head++ {
		e := g.order[g.head]
		if e.at > deadline {
			break // expiry order: nothing after this expired either
		}
		if at, ok := g.admitted[e.key]; ok && at <= deadline {
			delete(g.admitted, e.key)
		}
	}
	switch {
	case g.head == 0:
	case g.head == len(g.order) && cap(g.order) > shrinkAt:
		// Admit is the only writer of both, so an entirely expired queue means
		// an empty map: a drained burst hands back what it grew.
		g.order, g.admitted, g.head = nil, nil, 0
	case g.head*2 >= len(g.order):
		n := copy(g.order, g.order[g.head:])
		// The copy leaves the evicted keys in the slots past n, where a
		// pointer-bearing K would keep them reachable until something
		// overwrites them. Reclaiming is the point; clear them.
		clear(g.order[n:len(g.order)])
		g.order, g.head = g.order[:n], 0
	}
}
