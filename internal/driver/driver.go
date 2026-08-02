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

// Package driver holds the two periodic-scan loop shapes every beehive driver
// is built from. In both, a non-positive interval means the driver is off.
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
