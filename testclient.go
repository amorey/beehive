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
	"encoding/json"
)

// TestClient writes one kind's status and conditions outside a reconcile pass,
// for fixtures; not for production code. It exists for one case: a controller
// that reads another kind's status out of a real store, which a test has no
// other way to write.
// See docs/adr/2026-08-18-a-test-client-writes-status.md.
type TestClient[Status any] struct {
	w kindWriter
}

// NewTestClient returns a TestClient for gk. Needs no registered controller and
// no running beehive.
func NewTestClient[Status any](bh *Beehive, gk GroupKind) *TestClient[Status] {
	return &TestClient[Status]{kindWriter{bh, gk}}
}

// DeleteCondition removes id's condition of that type. A missing condition is a
// no-op. Same contract as ControllerClient.DeleteCondition.
func (c *TestClient[Status]) DeleteCondition(ctx context.Context, id ObjectID, conditionType string) error {
	return c.w.DeleteCondition(ctx, id, conditionType)
}

// SetCondition writes id's condition of that type. Same contract as
// ControllerClient.SetCondition: the store stamps the times.
func (c *TestClient[Status]) SetCondition(ctx context.Context, id ObjectID, condition Condition) error {
	return c.SetConditions(ctx, id, []Condition{condition})
}

// SetConditions writes every named condition together. Same contract as
// ControllerClient.SetConditions: one version bump, a type named twice is
// ErrDuplicateConditionType, an empty slice writes nothing.
func (c *TestClient[Status]) SetConditions(ctx context.Context, id ObjectID, conditions []Condition) error {
	if len(conditions) == 0 {
		return nil
	}
	return c.w.SetConditions(ctx, id, conditionsToRaw(conditions)...)
}

// UpdateStatus records status for id. Never stamps observed_generation: the
// handshake stays beehive's, so an object given a fixture status is still
// unsettled.
func (c *TestClient[Status]) UpdateStatus(ctx context.Context, id ObjectID, status Status) error {
	b, err := json.Marshal(status)
	if err != nil {
		return err
	}
	return c.w.UpdateStatus(ctx, id, b)
}
