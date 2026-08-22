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

	"github.com/amorey/gobus"
	"github.com/amorey/gobus/watch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The zero hub is inert rather than fatal: a Beehive built field by field has
// one, and reconcile paths publish through it. Registering is the exception,
// since a receiver has to be tied to a hub.
func TestZeroWatchHubIsInert(t *testing.T) {
	var h watchHub[GroupKind, struct{}]

	assert.NoError(t, h.send(clientTestGK, struct{}{}))
	assert.NotPanics(t, h.Close)
	assert.NotPanics(t, h.Close, "and again")

	rx, ok := h.watch(clientTestGK)
	assert.False(t, ok)
	assert.Nil(t, rx)

	rx, ok = h.watchFrom(clientTestGK, struct{}{})
	assert.False(t, ok)
	assert.Nil(t, rx)

	rx, ok = h.watchAcross()
	assert.False(t, ok)
	assert.Nil(t, rx)
}

// Close ends the sender and not the hub. The difference is observable: a hub
// close abandons a receiver's unread value, where a sender close drains it
// first — and only the sender close is safe against a concurrent send.
func TestWatchHubCloseDrainsTheLastValue(t *testing.T) {
	h := watchHub[GroupKind, int]{hub: watch.New[GroupKind, int]()}
	rx, ok := h.watch(clientTestGK)
	require.True(t, ok)
	defer rx.Close()

	require.NoError(t, h.send(clientTestGK, 7))
	h.Close()

	ev, err := rx.Recv()
	require.NoError(t, err, "the value published before the close was abandoned")
	assert.Equal(t, 7, ev.Value)

	_, err = rx.Recv()
	assert.ErrorIs(t, err, gobus.ErrClosed, "and the stream ends after it")
}
