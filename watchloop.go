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
	"context"
	"time"

	"github.com/amorey/beehive/internal/driver"
)

// runWatchLoop drives a watch that a commit wakes and a floor tick backs up:
// the object tail and the event reader, which differ in what they drain, not in
// how they are paced. pass reports how long to wait, whether to drop the wakes
// arriving meanwhile, and whether the watch is finished. stopped runs when the
// wake hub closes, which only Stop does.
//
// A wake is dropped while pass reports backing off. A wake carries no
// information — a drain reads its position from the store — so dropping one
// loses nothing, and honouring it would void the backoff exactly when it is
// needed: a commit landing during a failed drain refills the wake slot, so a
// live writer would keep a degraded store re-reading as fast as it can fail.
// Generic over the wake's element type only because the two hubs deliver
// different envelopes; the value itself is never read.
func runWatchLoop[W any](
	ctx context.Context,
	wake <-chan W,
	floor time.Duration,
	stopped func(),
	pass func(backingOff bool) (next time.Duration, stillBackingOff, done bool),
) {
	timer := time.NewTimer(floor)
	defer timer.Stop()

	backingOff := false
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-wake:
			if !ok {
				stopped()
				return
			}
			// Consumed rather than ignored, so the closed arm above stays live.
			if backingOff {
				continue
			}
		case <-timer.C:
		}

		next, stillBackingOff, done := pass(backingOff)
		if done {
			return
		}
		backingOff = stillBackingOff
		driver.Rearm(timer, next)
	}
}
