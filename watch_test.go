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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPollFailedSeparatesShutdownFromFailure pins the two answers a failed read
// gets, directly rather than through a stream. Reaching them from a live watch
// means cancelling a context while a read is in flight, which is a race the test
// would sometimes lose — and a coverage gate turns a lost race into a red build
// with no defect behind it.
//
// The distinction itself is the point: a store error is a fault worth reporting
// and worth one more tick, while a cancelled context is this stream shutting down
// and neither. Warning there would put a line in the log on every clean
// unsubscribe.
func TestPollFailedSeparatesShutdownFromFailure(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	c := &clientImpl[cSpec, cStatus]{bh: &Beehive{logger: logger}, gk: clientTestGK}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, c.pollFailed(ctx, "watch", errBoom), "a cancelled read ends the loop")
	assert.Empty(t, buf.String(), "shutdown is not a fault to report")

	assert.True(t, c.pollFailed(context.Background(), "watch", errBoom),
		"a store error costs the tick, not the stream")
	assert.Contains(t, buf.String(), "watch poll failed")
	assert.Contains(t, buf.String(), errBoom.Error())
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

// Stream tests. These go through SchedulesWatch and never Peek: the stream
// goroutine is the receiver's consumer, so a Peek racing it proves nothing.
//
// Every one of them runs with the poll turned off, so a value a test observes is
// provably the hub's. Without that they would pass on the poll alone and say
// nothing about push.
