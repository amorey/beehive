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
	"github.com/amorey/gobus/watch"
)

// wakeHub carries a committed write's log position from the commit path to the
// kind's tailer. Keyed by GroupKind and latest-value: a burst coalesces into one
// pending wake, and a publish that lands mid-read waits in the slot. Close
// closes the sender, never the hub — scheduleHub's rule.
type wakeHub struct {
	hub *watch.Hub[GroupKind, int64]
}

func newWakeHub() wakeHub {
	return wakeHub{hub: watch.New[GroupKind](watch.WithAccept(
		func(prev, next int64) bool { return next > prev },
	))}
}

func (h wakeHub) Send(gk GroupKind, rv int64) error { return h.hub.Sender().Send(gk, rv) }

// Watch registers a receiver for gk, seeded at zero: the tailer reads its own
// starting cursor from the store, not from the hub.
func (h wakeHub) Watch(gk GroupKind) *watch.Receiver[GroupKind, int64] {
	return h.hub.Watch(gk, 0)
}

func (h wakeHub) Close() { h.hub.Sender().Close() }
