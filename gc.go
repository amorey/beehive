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

// gcCollect advances garbage collection for one object, in its own transaction.
// It is a no-op unless the object is finalizing. For a finalizing object it
// cascades deletion to owned children, then removes the row once no finalizers
// and no incoming edges remain.
//
// No reconcile is woken: every row it touches is deletion-pending, and the
// sweeper's next tick finds it. Watches are a different matter and are woken —
// both the cascade's marks and the physical delete are the last the log will
// say about those rows, so a subscriber that missed them would wait out a floor
// tick for a change nothing else will report. The cascade wakes once per child
// kind, since it is cross-kind.
func (bh *Beehive) gcCollect(ctx context.Context, id ObjectID) (deleted bool, err error) {
	err = bh.store.Within(ctx, func(ctx context.Context) error {
		obj, err := bh.store.ObjectsGetMeta(ctx, id)
		if err != nil {
			return err
		}
		if obj.DeletionRequestedAt == nil {
			return nil
		}

		// Mark owned children for deletion; the mark puts them in the sweeper's listing.
		// The children span kinds, so wake each child's own kind — deduped per
		// kind: a wide cascade would otherwise queue one commit hook per row
		// for wakes that coalesce anyway.
		children, err := bh.store.DeletionRequestsCreateFromOwner(ctx, id)
		if err != nil {
			return err
		}
		woken := make(map[GroupKind]bool, len(children))
		for _, ch := range children {
			if gk := ch.Ref.GroupKind(); !woken[gk] {
				woken[gk] = true
				bh.signalKindWritten(ctx, gk)
			}
		}

		// The controller hasn't finished cleanup.
		if len(obj.Finalizers) > 0 {
			return nil
		}
		// Drop depends_on edges from finalizing dependents first, or two
		// deletion-pending objects that depend on each other would hold each
		// other's RESTRICT forever. owned_by edges are left for the cascade.
		if err := bh.store.EdgesDeleteFinalizingDependsOn(ctx, id); err != nil {
			return err
		}
		// Still referenced: RESTRICT forbids the delete. A later sweep retries
		// once the referrer is gone.
		referenced, err := bh.store.EdgesHasIncoming(ctx, id)
		if err != nil {
			return err
		}
		if referenced {
			return nil
		}

		// Deleting the row drops its outgoing edges (ON DELETE CASCADE), which may
		// unblock a target the sweeper retries on its next tick.
		if err := bh.store.ObjectsDelete(ctx, id); err != nil {
			return err
		}
		bh.signalKindWritten(ctx, GroupKind{Group: obj.Group, Kind: obj.Kind})
		deleted = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return deleted, nil
}
