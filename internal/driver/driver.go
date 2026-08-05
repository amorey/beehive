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

// Package driver holds the two periodic-scan loop shapes, plus the timer and
// backoff primitives a wake-driven loop builds its own shape from. In both scan
// shapes a non-positive interval means the driver is off; a wake-driven loop
// says that some other way, since its intervals are floors rather than cadences.
package driver

import (
	"context"
	"time"
)

// Run runs step once eagerly, then on every tick, until ctx is cancelled or
// step returns false. A non-positive interval turns the driver off, first pass
// included.
func Run(ctx context.Context, every time.Duration, step func(context.Context) bool) {
	if every <= 0 {
		return
	}
	if !step(ctx) {
		return
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !step(ctx) {
				return
			}
		}
	}
}

// TickerChan returns a driver's tick channel and its stop func, for a loop
// that selects over several intervals at once. A non-positive interval returns
// a nil channel, which blocks forever in a select.
func TickerChan(d time.Duration) (<-chan time.Time, func()) {
	if d <= 0 {
		return nil, func() {}
	}
	t := time.NewTicker(d)
	return t.C, t.Stop
}

// Rearm resets t to fire d from now, for a wake-driven loop that re-arms one
// timer after every pass. Safe on a timer that is still running, one that has
// fired, and one whose value was never received: since Go 1.23 the timer
// channel is unbuffered, so Reset cannot leave a stale value behind.
func Rearm(t *time.Timer, d time.Duration) {
	t.Stop()
	t.Reset(d)
}

// Backoff is the retry delay a wake-driven driver uses when a step fails: it
// doubles from Base and is capped at Max, which is the driver's own floor
// cadence. The zero value is unusable — Base must be positive.
type Backoff struct {
	Base time.Duration
	Max  time.Duration
	cur  time.Duration
}

// Wait blocks for the next delay and reports whether it elapsed; false means
// ctx ended. It advances the delay, so a caller must Reset after a success.
func (b *Backoff) Wait(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(b.Next()):
		return true
	}
}

// Next returns the next delay and doubles it for the call after, saturating at
// Max.
//
// It saturates rather than doubling past Max because the doubling overflows: 37
// doublings of a 100ms base pass time.Duration's range, and a negative cur reads
// as uninitialized below — which would drop a driver back to Base part way
// through the outage the backoff exists for.
func (b *Backoff) Next() time.Duration {
	if b.cur <= 0 {
		b.cur = b.Base
	}
	d := min(b.cur, b.Max)
	if b.cur < b.Max/2 {
		b.cur *= 2
	} else {
		b.cur = b.Max
	}
	return d
}

// Reset returns the delay to Base; call it after a step that worked.
func (b *Backoff) Reset() { b.cur = b.Base }
