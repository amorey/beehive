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

// gcCollect is the garbage-collection step for one object. It runs after that
// object's Reconcile returns (see typedController.reconcile) and on the global sweep
// (see deletionPendingSweep), in its own transaction, and does nothing unless the
// object is finalizing.
//
// For a finalizing object, two things happen:
//
//   - Cascade: every object owned by this one is marked for deletion too, so
//     deleting an owner tears its children down with it.
//   - Delete: once the object has no finalizers left and nothing still references
//     it, the row is removed. ON DELETE RESTRICT on the edges table makes that
//     order mandatory, since an owner cannot be removed while a child still points
//     at it, and ON DELETE CASCADE on the child side means removing the last child
//     drops the edge that was blocking the owner. The GC sweep picks the freed
//     owner up next time round.
//
// Both happen in one transaction, so the cascade writes and the delete commit
// together.
//
// Nothing is woken here — neither the cascaded children nor the targets a removed row
// was holding open. All of them are deletion-pending rows, so the sweeper's own
// listing finds them on its next tick. That is the only collector a client-only kind
// has anyway, and it is guaranteed, because WithGCInterval cannot be disabled. Waking
// them here would save one tick of latency on a multi-level cascade, at the cost of
// running recursive deletes on the caller's goroutine and ctx, where a caller that
// cancels right after its commit could abandon the cascade half-done.
func (bh *Beehive) gcCollect(ctx context.Context, id ObjectID) (deleted bool, err error) {
	err = bh.store.Within(ctx, func(ctx context.Context) error {
		obj, err := bh.store.ObjectsGetMeta(ctx, id)
		if err != nil {
			return err
		}
		// Not finalizing: nothing to collect.
		if obj.DeletionRequestedAt == nil {
			return nil
		}

		// Cascade deletion to owned children (see DeletionRequestsCreateFromOwner for
		// the steady-state single-read path). Marking them is all that is needed: the
		// mark is what puts them in the sweeper's listing.
		if _, err := bh.store.DeletionRequestsCreateFromOwner(ctx, id); err != nil {
			return err
		}

		// Finalizers still pending: the controller hasn't finished cleanup.
		if len(obj.Finalizers) > 0 {
			return nil
		}
		// A dependent that's itself finalizing has no claim on us: drop those
		// depends_on edges before the referrer gate, or two deletion-pending objects
		// that depend on each other (or a self-dependency) would each hold the
		// other's RESTRICT forever. owned_by edges are left for the cascade.
		if err := bh.store.EdgesDeleteFinalizingDependsOn(ctx, id); err != nil {
			return err
		}
		// Still referenced (owned children or live dependents): RESTRICT forbids the
		// delete. Leave the row; once the referrer is gone a later sweep gets here
		// again and the delete succeeds.
		referenced, err := bh.store.EdgesHasIncoming(ctx, id)
		if err != nil {
			return err
		}
		if referenced {
			return nil
		}

		// Removing this row drops its outgoing edges (ON DELETE CASCADE), which may
		// unblock a deletion-pending target RESTRICT was holding. That target is
		// already in the sweeper's listing — it is deletion-pending, which is how it
		// got here — so the next tick retries it and finds the block gone.
		if err := bh.store.ObjectsDelete(ctx, id); err != nil {
			return err
		}
		deleted = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return deleted, nil
}
