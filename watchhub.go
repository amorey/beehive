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

import "github.com/amorey/gobus/watch"

// watchHub holds what every gobus/watch hub in this package does the same way,
// so the rules below are stated once rather than per hub. The two callers —
// kindWriteHub and the work queue's scheduleHub — differ in key, value and
// accept rule, and share nothing else.
//
// **Close closes the sender, never the hub**, and both halves of that matter.
// Hub.Close is a hard tear-down with no drain, so a receiver can lose its last
// value on a timing race. And Sender.Close is the one gobus permits to run
// concurrently with a Send, which the commit wake relies on: nothing fences an
// application's writes against Stop.
//
// The zero value is usable and inert. A Beehive built field by field rather
// than by New has one, and reconcile paths reach it there.
//
// Generic over the bus's own key and value, which is not the Spec/Status
// boundary Register draws — the wrapped *watch.Hub is generic either way.
type watchHub[K comparable, V any] struct {
	hub *watch.Hub[K, V]
}

// send publishes v under k, and is a no-op on the zero hub — the same rule
// bh.log() applies to an unresolved logger.
func (h watchHub[K, V]) send(k K, v V) error {
	if h.hub == nil {
		return nil
	}
	return h.hub.Sender().Send(k, v)
}

// watch registers a receiver for k, seeded with initial. ok is false on the
// zero hub: unlike send and Close this cannot no-op, since a receiver has to be
// tied to a hub, so the caller turns it into an error rather than a nil
// dereference.
func (h watchHub[K, V]) watch(k K, initial V) (*watch.Receiver[K, V], bool) {
	if h.hub == nil {
		return nil, false
	}
	return h.hub.Watch(k, initial), true
}

// watchAcross registers a receiver for every key. One slot like any other
// receiver, so a burst across keys collapses to one value. Zero hub as in
// watch.
func (h watchHub[K, V]) watchAcross(initial V) (*watch.Receiver[K, V], bool) {
	if h.hub == nil {
		return nil, false
	}
	return h.hub.WatchAcross(initial), true
}

// Close ends the sender; see the type's doc for why it is never the hub.
// Idempotent, and a no-op on the zero hub.
func (h watchHub[K, V]) Close() {
	if h.hub != nil {
		h.hub.Sender().Close()
	}
}
