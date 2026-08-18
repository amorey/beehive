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

// TestClient writes one kind's status and conditions outside a reconcile pass,
// for fixtures; not for production code. It exists for one case: a controller
// that reads another kind's status out of a real store, which a test has no
// other way to write.
//
// It writes through a ControllerClient nothing ever ends, so every verb below
// means exactly what it means during a pass.
// See docs/adr/2026-08-18-a-test-client-writes-status.md.
type TestClient[Status any] struct {
	passClients[Status]
}

// NewTestClient returns a TestClient for gk. Needs no registered controller and
// no running beehive.
func NewTestClient[Status any](bh *Beehive, gk GroupKind) *TestClient[Status] {
	return &TestClient[Status]{passClients[Status]{bh: bh, gk: gk}}
}

// DeleteCondition removes id's condition of that type. See
// ControllerClient.DeleteCondition.
func (t *TestClient[Status]) DeleteCondition(ctx context.Context, id ObjectID, conditionType string) error {
	return t.at(id).DeleteCondition(ctx, conditionType)
}

// SetCondition writes id's condition of that type. See
// ControllerClient.SetCondition.
func (t *TestClient[Status]) SetCondition(ctx context.Context, id ObjectID, condition Condition) error {
	return t.at(id).SetCondition(ctx, condition)
}

// SetConditions writes every named condition together. See
// ControllerClient.SetConditions.
func (t *TestClient[Status]) SetConditions(ctx context.Context, id ObjectID, conditions []Condition) error {
	return t.at(id).SetConditions(ctx, conditions)
}

// UpdateStatus records status for id. See ControllerClient.UpdateStatus. Never
// stamps observed_generation: the handshake stays beehive's, so an object given
// a fixture status is still unsettled.
func (t *TestClient[Status]) UpdateStatus(ctx context.Context, id ObjectID, status Status) error {
	return t.at(id).UpdateStatus(ctx, status)
}
