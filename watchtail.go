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
	"sync/atomic"

	"github.com/amorey/gobus/watch"
)

// wakeHub tells a kind's tailer that the kind moved. Keyed by GroupKind and
// latest-value: a burst coalesces into one pending wake, and a publish that
// lands mid-read waits in the slot. Close closes the sender, never the hub —
// scheduleHub's rule.
//
// The value is a process-local tick, not the write's resource version: most
// store writes return no version (see the write-shapes ADR), and the tailer
// reads its own cursor from the store anyway. All the value has to do is rise,
// so Accept can drop a publish the hub has already superseded.
type wakeHub struct {
	hub *watch.Hub[GroupKind, int64]
	seq *atomic.Int64
}

func newWakeHub() wakeHub {
	return wakeHub{
		hub: watch.New[GroupKind](watch.WithAccept(
			func(prev, next int64) bool { return next > prev },
		)),
		seq: new(atomic.Int64),
	}
}

// Send is a no-op on the zero hub, which is what a Beehive assembled without
// New has — the same courtesy bh.log() extends to an unresolved logger.
func (h wakeHub) Send(gk GroupKind) error {
	if h.hub == nil {
		return nil
	}
	return h.hub.Sender().Send(gk, h.seq.Add(1))
}

// Watch registers a receiver for gk, seeded at zero: the tailer reads its own
// starting cursor from the store, not from the hub.
func (h wakeHub) Watch(gk GroupKind) *watch.Receiver[GroupKind, int64] {
	return h.hub.Watch(gk, 0)
}

// Close is a no-op on the zero hub; see Send.
func (h wakeHub) Close() {
	if h.hub != nil {
		h.hub.Sender().Close()
	}
}
