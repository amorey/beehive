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
)

// This file is what the three watch surfaces share. Each has its own file and
// its own mechanism: objectswatch.go subscribes to a per-kind tail that a
// commit wakes, eventswatch.go polls an object's event log and diffs it, and
// scheduleswatch.go reads an in-memory gauge. All three are level-triggered —
// a subscriber is told the latest state, never the states it missed.

// sendOrDone delivers v unless ctx is cancelled first, and reports whether it
// landed. Cancellation is checked first, on its own: once a reader parks on
// out, both select arms are ready and Go picks at random — a subscriber that
// gave up must not be handed one more value.
func sendOrDone[V any](ctx context.Context, out chan<- V, v V) bool {
	select {
	case <-ctx.Done():
		return false
	default:
	}
	select {
	case out <- v:
		return true
	case <-ctx.Done():
		return false
	}
}

// pollFailed logs a failed poll and says whether to keep going: a transient
// store error costs one tick, not the stream; a cancelled context is shutdown.
func (c *clientImpl[Spec, Status]) pollFailed(ctx context.Context, what string, err error, args ...any) bool {
	if ctx.Err() != nil {
		return false
	}
	c.bh.log().WarnContext(ctx, "beehive: "+what+" poll failed; retrying on the next tick",
		append([]any{"group", c.gk.Group, "kind", c.gk.Kind, "err", err}, args...)...)
	return true
}
