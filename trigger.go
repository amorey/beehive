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

import "context"

// trigger is one feed declared by WithTriggerByID or WithTriggerByName: a
// channel of addresses the app resolves within the kind and requeues. Exactly
// one of the two channels is set, and which one also selects the resolution.
type trigger struct {
	r     *reconciler
	ids   <-chan ObjectID
	names <-chan string
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
func (t *trigger) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case id, ok := <-t.ids:
			if !ok {
				return
			}
			t.poke(ctx, addr{id: id})
		case name, ok := <-t.names:
			if !ok {
				return
			}
			t.poke(ctx, addr{name: name})
		}
	}
}

// poke resolves a within the kind and queues what it found.
func (t *trigger) poke(ctx context.Context, a addr) {
	obj, err := t.resolve(ctx, a)
	if err != nil {
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
