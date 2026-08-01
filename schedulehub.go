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
// scheduleSender is what workQueue needs in order to publish a move and to
// register a subscriber. The queue holds this rather than the concrete hub, so a
// test can record what it published without standing up a hub and a receiver to
// observe it.
type scheduleSender interface {
	Send(id ObjectID, s stamped) error
	Watch(id ObjectID, initial stamped) *watch.Receiver[ObjectID, stamped]
	Close()
}

// The hub lives beside the queue it observes, and newWorkQueue builds both. A
// beehive-level hub would need the kind threaded through every queue operation,
// and it would widen the send lock's blast radius from one kind's queue to the
// whole process — which matters because a single subscriber puts every publish
// for that hub on the locked path.
//
// scheduleHub adapts watch.Hub to scheduleSender. Close is the *sender's* close,
// never the hub's: Hub.Close is hard tear-down with no drain, so a receiver that
// had not yet read the final value would lose it on a timing coin flip.
type scheduleHub struct {
	hub *watch.Hub[ObjectID, stamped]
}

func (h scheduleHub) Send(id ObjectID, s stamped) error { return h.hub.Sender().Send(id, s) }

func (h scheduleHub) Watch(id ObjectID, initial stamped) *watch.Receiver[ObjectID, stamped] {
	return h.hub.Watch(id, initial)
}

func (h scheduleHub) Close() { h.hub.Sender().Close() }

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
func newScheduleHub() scheduleHub {
	return scheduleHub{hub: watch.New[ObjectID](watch.WithAccept(
		func(prev, next stamped) bool { return next.Seq > prev.Seq },
	))}
}

// watchSchedule registers a receiver for id, seeded with id's current schedule,
// in one critical section.
//
// The single critical section is the whole of the correctness here. Watch calls
// no caller code, so it is safe under this lock, and seeding it with the value
// read under the same lock closes the subscribe race in both directions:
//
//   - a move whose critical section ran *before* this read is already in the
//     seed, and its later publish carries a Seq at or below it, so Accept
//     rejects it and the subscriber sees no duplicate;
//   - a move whose critical section runs *after* finds the receiver registered,
//     and its Seq exceeds the seed, so nothing is lost.
//
// The bus does not deliver the seed back — it is the caller's own argument — so
// the caller reports it as the stream's first value and reads the receiver for
// what supersedes it.
// It returns the seed as well as the receiver. A caller that re-read the gauge
// afterwards would take a second critical section and reopen the race this one
// closes.
func (q *workQueue) watchSchedule(id ObjectID) (*watch.Receiver[ObjectID, stamped], stamped) {
	q.mu.Lock()
	defer q.mu.Unlock()
	cur := q.gauge.at(id)
	return q.schedules.Watch(id, cur), cur
}

// closeHub ends every schedule stream of this kind. Call it after stop, so the
// final values are already published and each receiver reads its last one before
// its stream ends.
func (q *workQueue) closeHub() {
	q.schedules.Close()
}
