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

import "time"

// stamped is a schedule plus the order it moved in.
//
// Seq is assigned under workQueue.mu, so it is the *queue's* order and not the
// order two publishes happen to reach the bus in. That distinction is the whole
// reason it exists: a publish happens after the queue lock is released, so two
// moves can unlock in one order and be sent in the other. The hub's Accept rule
// compares Seq, so whichever send arrives second sees the other as prev and the
// slot settles the same way either way.
type stamped struct {
	Schedule Schedule
	Seq      uint64
}

// keyed is one id's move, for the mutator that moves many ids at once.
type keyed struct {
	ID ObjectID
	stamped
}

// gauge owns the two maps SchedulesWatch reports on. Nothing outside it touches
// them, so a queue operation cannot move the schedule without calling a method
// here — and every method that moves it says so.
//
// That is not a proof: it is one small type whose five mutators each return a
// report their caller must consume, plus the tests in gauge_test.go that drive
// all of them. It is what stands in for the backstop this stream does not have,
// so keep the surface small and keep every mutator reporting.
//
// A future change that gives workQueue a second writer — an exported handle, a
// shared queue, a durable schedule — breaks the argument entirely and the poll
// would have to come back.
//
// The caller holds workQueue.mu for every method here, including the reads.
type gauge struct {
	// dirty maps a queued id to the moment it became due. The value is what makes
	// the schedule a gauge rather than a clock: at answers with it, so an id that
	// sits queued behind a slow reconcile reports the same time on every read,
	// where time.Now() would differ on each one and make SchedulesWatch emit
	// forever (see the schedule-watch ADR: a repeated value must be impossible).
	dirty  map[ObjectID]time.Time // queued (in items) and awaiting dispatch
	alarms map[ObjectID]*alarm    // pending delayed adds (addAfter), keyed by id
	seq    uint64
}

func newGauge() *gauge {
	return &gauge{
		dirty:  make(map[ObjectID]time.Time),
		alarms: make(map[ObjectID]*alarm),
	}
}

// scheduleLocked is the gauge's whole reading rule: an id queued for immediate
// dispatch is due now, otherwise a pending alarm names its fire time, otherwise
// nothing is scheduled.
//
// dirty is consulted first because "due now" is the truthful answer for an id
// that holds both — it is dispatchable, and the alarm is a later fallback rather
// than the next event. That ordering is why setAlarm on a dirty id reports no
// move: the alarm is real, but invisible.
func (g *gauge) scheduleLocked(id ObjectID) Schedule {
	if at, ok := g.dirty[id]; ok {
		return Schedule{NextRequeueAt: at}
	}
	if a := g.alarms[id]; a != nil {
		return Schedule{NextRequeueAt: a.fireAt}
	}
	return Schedule{}
}

// at reports id's schedule without moving anything.
//
// It returns no ok. The zero Schedule already means "nothing scheduled", which
// is a real value this watch delivers rather than an absence, so a second
// result would carry nothing a caller could act on — both callers of the old
// nextRequeueAt discarded it.
func (g *gauge) at(id ObjectID) stamped {
	return stamped{Schedule: g.scheduleLocked(id), Seq: g.seq}
}

// report stamps a move, or reports nothing when the observable schedule did not
// change. Every mutator below routes its answer through here, so "did a watcher
// see this" is decided in one place rather than at each site.
func (g *gauge) report(id ObjectID, before Schedule) (stamped, bool) {
	after := g.scheduleLocked(id)
	if after == before {
		return stamped{}, false
	}
	g.seq++
	return stamped{Schedule: after, Seq: g.seq}, true
}

// markDirty queues id for immediate dispatch. It is a no-op for an id already
// queued, which is what keeps a burst of adds to one id at one reported move.
func (g *gauge) markDirty(id ObjectID) (stamped, bool) {
	if _, ok := g.dirty[id]; ok {
		return stamped{}, false
	}
	before := g.scheduleLocked(id)
	g.dirty[id] = time.Now()
	return g.report(id, before)
}

// dirtyAt reports whether id is queued for immediate dispatch.
func (g *gauge) dirtyAt(id ObjectID) bool {
	_, ok := g.dirty[id]
	return ok
}

// clearDirty drops id's queued-now slot. The id then reads as its pending alarm,
// or as unscheduled when it has none.
func (g *gauge) clearDirty(id ObjectID) (stamped, bool) {
	before := g.scheduleLocked(id)
	delete(g.dirty, id)
	return g.report(id, before)
}

// setAlarm records a pending delayed add. It reports nothing when id is already
// dirty, because at reads dirty first and the alarm is therefore invisible.
func (g *gauge) setAlarm(id ObjectID, a *alarm) (stamped, bool) {
	before := g.scheduleLocked(id)
	g.alarms[id] = a
	return g.report(id, before)
}

// alarmAt returns id's pending alarm, or nil. It is how the queue tests whether
// a fired timer is still the current schedule without reaching into the maps.
func (g *gauge) alarmAt(id ObjectID) *alarm { return g.alarms[id] }

// clearAlarm drops id's pending alarm without stopping its timer — the caller
// owns that, because only the caller knows whether the timer is the one firing.
func (g *gauge) clearAlarm(id ObjectID) (stamped, bool) {
	before := g.scheduleLocked(id)
	delete(g.alarms, id)
	return g.report(id, before)
}

// clearAllAlarms stops every pending timer, drops every alarm, and reports the
// ids whose observable schedule moved. Shutdown calls it, and the reports are the
// last values a subscriber sees.
//
// An id that is also dirty is *not* reported: it reads as due now before and
// after, since at consults dirty first. Reporting it would publish a Seq bump
// for a change no watcher can see — harmless, but it would break the rule every
// other mutator keeps.
func (g *gauge) clearAllAlarms() []keyed {
	var moved []keyed
	for id, a := range g.alarms {
		a.timer.Stop()
		before := g.scheduleLocked(id)
		delete(g.alarms, id)
		if s, ok := g.report(id, before); ok {
			moved = append(moved, keyed{ID: id, stamped: s})
		}
	}
	return moved
}
