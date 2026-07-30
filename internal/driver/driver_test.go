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
func TestRunDoesNothingWhenDisabled(t *testing.T) {
	var calls int
	Run(context.Background(), 0, func(context.Context) bool {
		calls++
		return true
	})
	assert.Zero(t, calls, "a non-positive interval runs no step at all")
}
