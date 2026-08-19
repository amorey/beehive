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
	"errors"
	"time"

	"github.com/amorey/beehive/internal/rategate"
)

// trigger is one feed declared by WithTriggerByID or WithTriggerByName: a
// channel of addresses the app resolves within the kind and requeues. Exactly
// one of the two channels is set, and which one also selects the resolution.
type trigger struct {
	r     *reconciler
	ids   <-chan ObjectID
	names <-chan string
	// floor is the minimum gap between two drains, keeping a producer-driven
	// read loop off the single connection; <= 0 turns it off.
	floor time.Duration
	// now sources the floor's clock; nil means time.Now.
	now func() time.Time
}

// addr is one address received on a trigger channel: an id, or a name when the
// feed is name-keyed.
type addr struct {
	id   ObjectID
	name string
}

// run services the feed until ctx ends or the app closes the channel. A nil
// channel blocks forever, which is what leaves the unused half of the select
// silent.
//
// The receive never reads the store: an address joins pending and a floored
// drain resolves it, so a producer is held up by another receive at most, never
// by the connection. pending is a set, so a hot feed on one address costs one
// read per window; nothing is dropped, because no driver re-derives a poke.
func (t *trigger) run(ctx context.Context) {
	now := t.now
	if now == nil {
		now = time.Now
	}
	gate := rategate.NewSingle(t.floor)
	pending := make(map[addr]struct{})

	var timer *time.Timer
	var opened <-chan time.Time
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case id, ok := <-t.ids:
			if !ok {
				return
			}
			pending[addr{id: id}] = struct{}{}
		case name, ok := <-t.names:
			if !ok {
				return
			}
			pending[addr{name: name}] = struct{}{}
		case <-opened:
			opened = nil
		}

		if len(pending) == 0 {
			continue
		}
		opensAt, held := gate.Allow(now())
		if held {
			// One timer for the whole window: re-arming per arrival would
			// push the drain out for as long as the feed keeps talking.
			if opened == nil {
				timer = time.NewTimer(opensAt.Sub(now()))
				opened = timer.C
			}
			continue
		}
		t.drain(ctx, pending)
	}
}

// drain resolves everything accumulated since the last one and empties the set.
func (t *trigger) drain(ctx context.Context, pending map[addr]struct{}) {
	for a := range pending {
		t.poke(ctx, a)
	}
	clear(pending)
}

// poke resolves a within the kind and queues what it found. An address that
// resolves to nothing is the app's business; a failed read is dropped rather
// than retried, since the kind's own cadence is what a poke is a hint against.
func (t *trigger) poke(ctx context.Context, a addr) {
	obj, err := t.resolve(ctx, a)
	if errors.Is(err, ErrNotFound) {
		t.r.log().DebugContext(ctx, "trigger address matched no object; skipping",
			"id", a.id, "name", a.name)
		return
	}
	if err != nil {
		t.r.log().WarnContext(ctx, "trigger failed to resolve an address; this poke is dropped",
			"id", a.id, "name", a.name, "err", err)
		return
	}
	t.r.requeueNow(obj.ID)
}

// resolve reads existence and kind, and nothing else: a trigger never looks at
// an object's conditions. The name form takes its kind from the WHERE; the id
// form must gate it here, or a foreign id would reach GetForReconcile, which
// takes a bare id and would hand the row to this kind's controller.
func (t *trigger) resolve(ctx context.Context, a addr) (*RawObject, error) {
	if t.names != nil {
		return t.r.store.Objects().GetMetaByName(ctx, t.r.gk, a.name)
	}
	obj, err := t.r.store.Objects().GetMeta(ctx, a.id)
	if err != nil {
		return nil, err
	}
	if obj.Group != t.r.gk.Group || obj.Kind != t.r.gk.Kind {
		return nil, ErrNotFound
	}
	return obj, nil
}
