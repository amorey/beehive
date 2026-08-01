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

import "github.com/amorey/gobus/watch"

// scheduleHub carries the work queue's gauge to the SchedulesWatch subscribers
// of one kind.
//
// gobus/watch is a keyed latest-value *state* bus: one slot for each watched
// key, seeded at registration with the value the caller has just read. That is
// the right shape because SchedulesWatch is a gauge — it streams the value
// itself rather than a change. Its sibling gobus/conflate is an event bus with
// coalescing and annihilation, which is what a change stream wants and this one
// does not.
//
// The hub lives beside the reconciler that owns the queue, and Register builds
// both. A beehive-level hub would need the kind threaded through every queue
// operation, and it would widen the send lock's blast radius from one kind's
// queue to the whole process — which matters because a single subscriber puts
// every publish for that hub on the locked path.
type scheduleHub = watch.Hub[ObjectID, stamped]

// newScheduleHub builds the hub with the rule that makes a reordered publish
// safe.
//
// A publish happens after workQueue.mu is released, so two moves can unlock in
// one order and reach Send in the other. Accept compares the queue's own order
// rather than trusting arrival: whichever send runs second sees the first's
// value as prev, so the slot settles the same way either way.
//
// The rule runs once for each receiver, against that receiver's own slot. Two
// streams on one id can be seeded at different moments, so a value can be new
// for one and old for the other, and a hub-wide answer would be wrong for one of
// them. It also runs against the value passed to Watch, which is what rejects a
// publish that predates a subscriber's snapshot.
//
// Accept runs under the bus lock and must not take a lock a caller may hold
// while calling Watch, Send or a Close: Watch is expressly safe under the
// queue's lock, so an Accept that took that lock would invert the two orders and
// deadlock. This one reads its two arguments and nothing else.
func newScheduleHub() *scheduleHub {
	return watch.New[ObjectID](watch.WithAccept(
		func(prev, next stamped) bool { return next.Seq > prev.Seq },
	))
}
