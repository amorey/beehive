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
	"log/slog"
	"time"

	"github.com/amorey/beehive/internal/driver"
	"github.com/amorey/beehive/internal/rategate"
)

// trigger is one feed declared by WithTriggerByID or WithTriggerByName: a
// channel of addresses to resolve within the kind and requeue. Exactly one of
// the two channels is set, and which one also selects the resolution.
// See docs/adr/2026-08-19-a-trigger-channel-requeues-by-id-or-name.md.
type trigger struct {
	r     *reconciler
	ids   <-chan ObjectID
	names <-chan string
	// floor is the minimum gap between two drains; <= 0 turns it off.
	floor time.Duration
}

// addr is one address received on a trigger channel: an id, or a name when the
// feed is name-keyed.
type addr struct {
	id   ObjectID
	name string
}

// run services the feed until ctx ends or the app closes the channel. A nil
// channel blocks forever, which is what silences the unused half of the select.
//
// The receive never reads the store: an address joins pending and a floored
// drain resolves it. pending is a set, so a hot feed on one address costs one
// read per window, and nothing is dropped — no driver re-derives a poke.
func (t *trigger) run(ctx context.Context) {
	gate := rategate.NewSingle(t.floor)
	pending := make(map[addr]struct{})
	// One timer, armed only while the floor holds addresses back. Since Go 1.23
	// Stop leaves no stale value to receive.
	timer := time.NewTimer(0)
	timer.Stop()
	defer timer.Stop()
	armed := false

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
		case <-timer.C:
			armed = false
		}

		now := time.Now()
		if opensAt, held := gate.Allow(now); held {
			if !armed {
				driver.Rearm(timer, opensAt.Sub(now))
				armed = true
			}
			continue
		}
		// Disarm before draining, or the timer fires on an empty pending.
		timer.Stop()
		armed = false
		for a := range pending {
			t.poke(ctx, a)
		}
		clear(pending)
	}
}

// poke resolves a within the kind and queues what it found. An address that
// resolves to nothing is the app's business; a failed read is dropped rather
// than retried, since the kind's own cadence is what a poke is a hint against.
func (t *trigger) poke(ctx context.Context, a addr) {
	obj, err := t.resolve(ctx, a)
	if errors.Is(err, ErrNotFound) {
		t.r.log().DebugContext(ctx, "trigger address matched no object; skipping", t.key(a))
		return
	}
	if err != nil {
		t.r.log().WarnContext(ctx, "trigger failed to resolve an address; this poke is dropped",
			t.key(a), "err", err)
		return
	}
	t.r.requeueNow(obj.ID)
}

// key names the half of a that this feed populates.
func (t *trigger) key(a addr) slog.Attr {
	if t.names != nil {
		return slog.String("name", a.name)
	}
	return slog.Int64("id", int64(a.id))
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
