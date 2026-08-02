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

// A create publishes its kind's new log position, so a tailer never has to poll
// to learn that the kind moved.
func TestWakeHubPublishesOnCreate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	rx := bh.wakes.Watch(clientTestGK)
	defer rx.Close()

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, "w1", cSpec{})

	ev, err := rx.RecvContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, obj.ResourceVersion, ev.Value)
}
