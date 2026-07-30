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

// Package driver holds the two periodic-scan loop shapes every beehive driver is
// built from. Nothing in beehive is pushed, so every driver is one of these two.
// They live together so "what does a non-positive interval mean" is answered once:
// the driver is off, and turning it off is a no-op rather than a panic in
// time.NewTicker. Neither shape takes a beehive type, which is why they sit below
// the main package rather than inside it.
package driver

import (
	"context"
	"time"
)

// Run runs step once, then on every tick, until ctx is cancelled or step
// returns false. It is the shape of every single-interval driver: the GC sweeper, the
// dependency waker, each client watch. Running eagerly matters for all three — a
// subscriber should not wait an interval for its first values, and a restart should
// not wait one before collecting.
//
// A non-positive interval turns the driver off, first pass included. "Never run this"
// is the only sensible reading of an interval that never elapses, and running once
// would make a disabled driver fire one more time than an enabled one that was
// stopped immediately.
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

// TickerChan returns a driver's tick channel and its stop func. It is Run's
// counterpart for a loop that selects over several intervals at once — the reconciler
// runs the owed pass and the full pass in one select — where no single step
// function fits. A
// non-positive interval returns a nil channel, which blocks forever in a select and
// so is exactly the right no-op.
func TickerChan(d time.Duration) (<-chan time.Time, func()) {
	if d <= 0 {
		return nil, func() {}
	}
	t := time.NewTicker(d)
	return t.C, t.Stop
}
