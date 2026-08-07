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
// The children this call marks and the owners its delete unblocks are enqueued
// at commit; everything else it touched waits for the sweeper's next tick. See
// docs/adr/2026-08-04-a-delete-request-pushes-its-own-collect.md and
// docs/adr/2026-08-05-a-physical-delete-pushes-its-owner.md.
//
// Watches are woken separately, and on a wider gate than the push: once per
// child kind returned, marked or not. A mark and the physical delete are the
// last the log will say about those rows, so a subscriber that missed either
// would wait out a floor tick for a change nothing else reports — where a wake
// for an unmarked child costs one position read that finds nothing. The wake is
// per kind rather than per child because it is cross-kind and coalesces anyway.
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
		// for wakes that coalesce anyway. Ungated on Marked, unlike the push: a
		// re-cascade's wake reads one position and finds nothing.
		cascade, err := bh.store.DeletionRequests().CreateFromOwner(ctx, id)
		if err != nil {
			return err
		}
		woken := make(map[GroupKind]bool, len(cascade.Children))
		var pushed []ObjectRef
		for _, ch := range cascade.Children {
			if gk := ch.Ref.GroupKind(); !woken[gk] {
				woken[gk] = true
				bh.signalKindWritten(ctx, gk)
			}
			// Gated on Marked: gcCollect reruns after every reconcile of a
			// deleting object, and an ungated push would re-arm the subtree on
			// each of them.
			if ch.Marked {
				pushed = append(pushed, ch.Ref)
			}
		}
		// Marking a child discounts its depends_on edges, which lifts the
		// RESTRICT on any deletion-pending target. One hook for both.
		pushed = append(pushed, cascade.Unblocked...)
		bh.signalRequeueManyNow(ctx, pushed)

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

		// Read before the delete: edges.from_id is ON DELETE CASCADE. owned_by
		// only: EdgesHasIncoming discounts depends_on from a deleting source.
		owners, err := bh.store.EdgesListOutgoingByRelation(ctx, id, RelationOwnedBy)
		if err != nil {
			return err
		}
		// Only a deletion-pending owner was blocked by this row; pushing a live one
		// would spin, since requeueNow bypasses the re-enqueue floor.
		var blocked []ObjectRef
		for _, owner := range owners {
			meta, err := bh.store.ObjectsGetMeta(ctx, owner.ID)
			if err != nil {
				return err
			}
			if meta.DeletionRequestedAt != nil {
				blocked = append(blocked, owner)
			}
		}
		if err := bh.store.ObjectsDelete(ctx, id); err != nil {
			return err
		}
		bh.signalKindWritten(ctx, GroupKind{Group: obj.Group, Kind: obj.Kind})
		bh.signalRequeueManyNow(ctx, blocked)
		deleted = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return deleted, nil
}
