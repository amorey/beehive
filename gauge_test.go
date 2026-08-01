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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pendingAlarm builds an alarm as addAfter does. A real alarm always carries a
// live timer, and clearAllAlarms stops it, so one built without a timer is not a
// state the gauge ever holds.
func pendingAlarm(in time.Duration) *alarm {
	return &alarm{timer: time.NewTimer(in), fireAt: time.Now().Add(in)}
}

// The gauge is what SchedulesWatch reports, and the whole push design rests on
// one property: a mutator that moves the observable schedule says so, and one
// that does not stays quiet. These tests hold that line without a bus, a queue
// or a stream in the way.

func TestGaugeMarkDirtyReports(t *testing.T) {
	g := newGauge()

	got, moved := g.markDirty(7)
	require.True(t, moved, "an id that becomes due now moves the gauge")
	assert.False(t, got.Schedule.NextRequeueAt.IsZero(), "due-now carries the moment it became due")
	assert.NotZero(t, got.Seq)

	// Already dirty: addLocked would return early, and so does the gauge.
	_, moved = g.markDirty(7)
	assert.False(t, moved, "a second markDirty changes nothing observable")
}

func TestGaugeSetAlarmReports(t *testing.T) {
	g := newGauge()
	a := pendingAlarm(time.Hour)

	got, moved := g.setAlarm(7, a)
	require.True(t, moved)
	assert.Equal(t, a.fireAt, got.Schedule.NextRequeueAt)
}

// at reads dirty before alarms, so an alarm on an id already queued for
// immediate dispatch changes nothing a watcher can see.
func TestGaugeSetAlarmOnDirtyIDReportsNothing(t *testing.T) {
	g := newGauge()
	_, moved := g.markDirty(7)
	require.True(t, moved)

	_, moved = g.setAlarm(7, pendingAlarm(time.Hour))
	assert.False(t, moved, "the id already reads as due now")
}

func TestGaugeClearDirtyReports(t *testing.T) {
	g := newGauge()
	_, moved := g.markDirty(7)
	require.True(t, moved)

	got, moved := g.clearDirty(7)
	require.True(t, moved, "dispatch leaves the id unscheduled")
	assert.True(t, got.Schedule.NextRequeueAt.IsZero())

	_, moved = g.clearDirty(7)
	assert.False(t, moved, "clearing what is already clear moves nothing")
}

// Dispatching an id that also holds a future alarm reports the alarm, not the
// zero schedule: the id is still scheduled, just not now.
func TestGaugeClearDirtyFallsBackToTheAlarm(t *testing.T) {
	g := newGauge()
	a := pendingAlarm(time.Hour)
	g.setAlarm(7, a)
	g.markDirty(7)

	got, moved := g.clearDirty(7)
	require.True(t, moved)
	assert.Equal(t, a.fireAt, got.Schedule.NextRequeueAt)
}

func TestGaugeClearAlarmReports(t *testing.T) {
	g := newGauge()
	g.setAlarm(7, pendingAlarm(time.Hour))

	got, moved := g.clearAlarm(7)
	require.True(t, moved)
	assert.True(t, got.Schedule.NextRequeueAt.IsZero())

	_, moved = g.clearAlarm(7)
	assert.False(t, moved, "clearing an absent alarm moves nothing")
}

// Shutdown clears every alarm, but an id that is also dirty reads as due-now
// before and after, so it must not be reported. Reporting it would publish a
// Seq bump for a schedule nobody can see change.
func TestGaugeClearAllAlarmsSkipsADirtyID(t *testing.T) {
	g := newGauge()
	g.setAlarm(1, pendingAlarm(time.Hour))
	g.setAlarm(2, pendingAlarm(time.Hour))
	g.markDirty(2) // 2 now reads as due now, so its alarm is invisible

	got := g.clearAllAlarms()

	require.Len(t, got, 1, "only the id whose observable schedule moved")
	assert.Equal(t, ObjectID(1), got[0].ID)
	assert.True(t, got[0].Schedule.NextRequeueAt.IsZero())
}

// Seq is the queue's order, so it advances on every reported move and never
// on a quiet one. Accept compares it, so a stalled Seq would let a stale
// publish win.
func TestGaugeSeqAdvancesOnlyOnAReportedMove(t *testing.T) {
	g := newGauge()

	first, moved := g.markDirty(1)
	require.True(t, moved)
	second, moved := g.markDirty(2)
	require.True(t, moved)
	assert.Greater(t, second.Seq, first.Seq)

	_, moved = g.markDirty(1) // already dirty
	require.False(t, moved)
	third, moved := g.markDirty(3)
	require.True(t, moved)
	assert.Equal(t, second.Seq+1, third.Seq, "a quiet call consumed no Seq")
}

func TestGaugeAtReportsTheCurrentSchedule(t *testing.T) {
	g := newGauge()
	assert.True(t, g.at(7).Schedule.NextRequeueAt.IsZero(), "an unknown id is unscheduled")

	a := pendingAlarm(time.Hour)
	g.setAlarm(7, a)
	assert.Equal(t, a.fireAt, g.at(7).Schedule.NextRequeueAt)

	// at does not move the gauge, so it does not consume a Seq.
	before := g.at(7).Seq
	assert.Equal(t, before, g.at(7).Seq)
}
