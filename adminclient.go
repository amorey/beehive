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

// AdminClient writes what only a reconcile pass can otherwise write — status,
// conditions, finalizers, events and dependency edges — for one kind, from
// outside a pass. Two uses: a test fixture, and maintenance on a stopped
// beehive, where a data migration, a backfill or a wedged object has to be
// written by hand. Not for reconcile logic, which holds a ControllerClient
// bound to the object it was handed.
//
// Every verb takes an id and is scoped to gk: another kind's id is ErrWrongKind.
// It writes through a ControllerClient nothing ever ends, so each verb means
// exactly what it means during a pass.
//
// Run it with beehive stopped. A write while beehive runs races that object's
// own pass and the later write wins; a write from a second process is never
// supported at all.
// See docs/adr/2026-08-18-an-admin-client-writes-outside-a-pass.md.
type AdminClient[Status any] struct {
	passClients[Status]
}

// NewAdminClient returns an AdminClient for gk. Needs no registered controller
// and no running beehive.
func NewAdminClient[Status any](bh *Beehive, gk GroupKind) *AdminClient[Status] {
	return &AdminClient[Status]{passClients[Status]{bh: bh, gk: gk}}
}

// AddDependency records that fromID depends on toID. See
// ControllerClient.AddDependency.
func (a *AdminClient[Status]) AddDependency(ctx context.Context, fromID, toID ObjectID) error {
	if err := a.scope(ctx, fromID); err != nil {
		return err
	}
	return a.at(fromID).AddDependency(ctx, toID)
}

// AddEvent adds an observation to id's event log. See
// ControllerClient.AddEvent.
func (a *AdminClient[Status]) AddEvent(ctx context.Context, id ObjectID, event EventSpec) error {
	return a.at(id).AddEvent(ctx, event)
}

// DeleteCondition removes id's condition of that type. See
// ControllerClient.DeleteCondition.
func (a *AdminClient[Status]) DeleteCondition(ctx context.Context, id ObjectID, conditionType string) error {
	return a.at(id).DeleteCondition(ctx, conditionType)
}

// DeleteDependency drops the fromID→toID edge. See
// ControllerClient.DeleteDependency. This is the only way to drop an edge whose
// source has no controller.
func (a *AdminClient[Status]) DeleteDependency(ctx context.Context, fromID, toID ObjectID) error {
	if err := a.scope(ctx, fromID); err != nil {
		return err
	}
	return a.at(fromID).DeleteDependency(ctx, toID)
}

// DeleteFinalizer removes id's finalizer, which is how a wedged object is
// unstuck. See ControllerClient.DeleteFinalizer.
func (a *AdminClient[Status]) DeleteFinalizer(ctx context.Context, id ObjectID, finalizer string) error {
	return a.at(id).DeleteFinalizer(ctx, finalizer)
}

// SetCondition writes id's condition of that type. See
// ControllerClient.SetCondition.
func (a *AdminClient[Status]) SetCondition(ctx context.Context, id ObjectID, condition Condition) error {
	return a.at(id).SetCondition(ctx, condition)
}

// SetConditions writes every named condition together. See
// ControllerClient.SetConditions.
func (a *AdminClient[Status]) SetConditions(ctx context.Context, id ObjectID, conditions []Condition) error {
	return a.at(id).SetConditions(ctx, conditions)
}

// UpdateStatus records status for id. See ControllerClient.UpdateStatus. Never
// stamps observed_generation: the handshake stays beehive's, so an object given
// a status here is still unsettled and the owed pass reconciles it. Holding no
// pass, it holds no loaded status either, so every call reaches the store and
// the no-op is the store's own — ErrNotFound included.
func (a *AdminClient[Status]) UpdateStatus(ctx context.Context, id ObjectID, status Status) error {
	return a.at(id).UpdateStatus(ctx, status)
}

// scope stands in for the kind folding every other verb gets from the store:
// Edges() takes no GroupKind, so a foreign source would write its edge and route
// the enqueue to this client's reconciler.
func (a *AdminClient[Status]) scope(ctx context.Context, id ObjectID) error {
	raw, err := a.bh.store.Objects().Get(ctx, id)
	if err != nil {
		return err
	}
	if raw.Group != a.gk.Group || raw.Kind != a.gk.Kind {
		return ErrWrongKind
	}
	return nil
}
