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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A step that reports false ends the driver, whichever tick it happens on. The
// watches rely on it to stop a poll goroutine whose subscriber has gone away, and
// that decision is almost never made on the first pass.
func TestRunDriverStopsWhenAStepReportsFalse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	var calls int
	go func() {
		defer close(done)
		runDriver(ctx, fastTick, func(context.Context) bool {
			calls++
			return calls < 3 // the third tick asks to stop
		})
	}()

	waitClosed(t, done, "runDriver to return after a step reported false")
	assert.Equal(t, 3, calls, "the driver runs no further step once one reports false")
}

// A non-positive interval disables a driver outright, first pass included. Running
// once would make "disabled" fire more often than an enabled driver stopped
// immediately, which is the reading no caller could want.
func TestRunDriverDoesNothingWhenDisabled(t *testing.T) {
	var calls int
	runDriver(context.Background(), 0, func(context.Context) bool {
		calls++
		return true
	})
	assert.Zero(t, calls, "a non-positive interval runs no step at all")
}

// The watch surface falls back to the default interval for a Beehive assembled
// field by field, as tests do. New itself cannot produce one: withWatchPollInterval
// rejects a non-positive value.
func TestWatchPollFallsBackToTheDefault(t *testing.T) {
	assert.Equal(t, defaultWatchPollInterval, (&Beehive{}).watchPoll(),
		"an unset interval reads as the default rather than as disabled")

	bh, err := New(&fakeStore{}, withWatchPollInterval(fastTick))
	require.NoError(t, err)
	assert.Equal(t, fastTick, bh.watchPoll(), "a configured interval is used as given")
}

// sendOrDone reports the send it could not make. Without it a subscriber that
// stopped reading would wedge its own poll goroutine, which then never observes
// the cancellation that was meant to release it.
func TestSendOrDoneReportsACancelledSend(t *testing.T) {
	out := make(chan int) // unbuffered, and nobody is reading

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, sendOrDone(ctx, out, 1), "a cancelled context abandons the send")

	// A reader waiting is the other half: the send lands and is reported as landed.
	got := make(chan int, 1)
	go func() { got <- <-out }()
	assert.True(t, sendOrDone(context.Background(), out, 7), "a send with a reader lands")
	assert.Equal(t, 7, <-got)
}
