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

package driver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	fastTick    = 2 * time.Millisecond
	testTimeout = 2 * time.Second
)

// Rearm's whole job is that a timer already fired, or fired and was drained,
// still fires again at the new delay.
func TestRearmFiresAtTheNewDelay(t *testing.T) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	Rearm(timer, fastTick)
	select {
	case <-timer.C:
	case <-time.After(testTimeout):
		t.Fatal("rearmed timer never fired")
	}

	// Again, on a timer that has already fired and been drained.
	Rearm(timer, fastTick)
	select {
	case <-timer.C:
	case <-time.After(testTimeout):
		t.Fatal("rearmed timer never fired a second time")
	}
}

// A step that reports false ends the driver, whichever tick it happens on. The
// watches rely on it to stop a poll goroutine whose subscriber has gone away, and
// that decision is almost never made on the first pass.
func TestRunStopsWhenAStepReportsFalse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	var calls int
	go func() {
		defer close(done)
		Run(ctx, fastTick, func(context.Context) bool {
			calls++
			return calls < 3 // the third tick asks to stop
		})
	}()

	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Run to return after a step reported false")
	}
	assert.Equal(t, 3, calls, "the driver runs no further step once one reports false")
}

// A non-positive interval disables a driver outright, first pass included. Running
// once would make "disabled" fire more often than an enabled driver stopped
// immediately, which is the reading no caller could want.
// The eager first step is the one that can end a driver before any ticker
// exists: a step that answers "nothing here will ever change" must not leave a
// ticker running behind it.
func TestRunStopsWhenTheFirstStepReportsFalse(t *testing.T) {
	var calls int
	Run(context.Background(), testTimeout, func(context.Context) bool {
		calls++
		return false
	})
	assert.Equal(t, 1, calls, "the eager step ran and nothing ticked after it")
}

func TestRunDoesNothingWhenDisabled(t *testing.T) {
	var calls int
	Run(context.Background(), 0, func(context.Context) bool {
		calls++
		return true
	})
	assert.Zero(t, calls, "a non-positive interval runs no step at all")
}

// A cancelled wait returns at once and reports that the delay never elapsed.
// Delays are capped at a driver's floor cadence, so a Wait that slept its full
// interval would hold shutdown for that long in every retrying loop.
func TestBackoffWaitReportsACancelledWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	b := Backoff{Base: testTimeout, Max: testTimeout}
	done := make(chan bool, 1)
	go func() { done <- b.Wait(ctx) }()

	select {
	case elapsed := <-done:
		assert.False(t, elapsed, "a cancelled wait must not report its delay as elapsed")
	case <-time.After(testTimeout):
		t.Fatal("Wait slept its full delay instead of returning on cancellation")
	}
}

// A prolonged outage must not walk the delay off the end of time.Duration.
// Doubling past Max overflows cur to a negative, which Next reads as
// uninitialized and answers with Base — dropping a failing driver back to its
// shortest retry part way through the outage, and doing it again every time the
// ladder climbs back. 37 doublings of a 100ms base is roughly 15 minutes at the
// cap, so this is reachable rather than theoretical.
func TestBackoffSaturatesRatherThanOverflowing(t *testing.T) {
	const maxDelay = 30 * time.Second
	b := Backoff{Base: 100 * time.Millisecond, Max: maxDelay}

	var prev time.Duration
	for i := range 200 { // well past the 37 that overflow
		d := b.Next()
		assert.Positive(t, d, "call %d", i+1)
		assert.LessOrEqual(t, d, maxDelay, "call %d", i+1)
		assert.GreaterOrEqual(t, d, prev, "call %d went backwards", i+1)
		prev = d
	}
	assert.Equal(t, maxDelay, b.Next(), "saturated at Max")

	// Reset still returns the ladder to the bottom, saturation notwithstanding.
	b.Reset()
	assert.Equal(t, 100*time.Millisecond, b.Next())
}

// The ladder below Max is unchanged by saturation: it doubles from Base and the
// last step before the cap is the one that overshoots it.
func TestBackoffDoublesUpToMax(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond, Max: 30 * time.Second}
	var got []time.Duration
	for range 11 {
		got = append(got, b.Next())
	}
	assert.Equal(t, []time.Duration{
		100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond,
		800 * time.Millisecond, 1600 * time.Millisecond, 3200 * time.Millisecond,
		6400 * time.Millisecond, 12800 * time.Millisecond, 25600 * time.Millisecond,
		30 * time.Second, 30 * time.Second,
	}, got)
}

// A Backoff whose Max was never set must still back off. Max is a driver's own
// floor cadence, and a wake-driven driver may have none — min(cur, 0) would
// otherwise hand back a zero delay forever, which is a hot loop against
// whatever just failed.
func TestBackoffWithoutAMaxFallsBackToBase(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond}

	for range 5 {
		assert.Equal(t, 100*time.Millisecond, b.Next(), "no ceiling means no growth, never zero")
	}
}
