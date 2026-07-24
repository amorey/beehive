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

// pendingWakes collects ref targets to requeue after Reconcile returns. A
// ControllerClient call that frees a target (DeleteDependency) registers it here;
// typedController.reconcile drains it once Reconcile returns (the freeing write has
// already committed). It rides on the context so the long-lived, shared
// ControllerClient holds no per-reconcile state, and a single reconcile's Reconcile
// runs on one goroutine, so the slice needs no locking.
type pendingWakes struct {
	targets []Referrer
}

type pendingWakesKey struct{}

// withPendingWakes attaches a fresh collector to ctx for one reconcile.
func withPendingWakes(ctx context.Context, w *pendingWakes) context.Context {
	return context.WithValue(ctx, pendingWakesKey{}, w)
}

// pendingWakesFrom returns the collector for the current reconcile, or nil when
// called outside one (e.g. a ControllerClient used directly in a test).
func pendingWakesFrom(ctx context.Context) *pendingWakes {
	w, _ := ctx.Value(pendingWakesKey{}).(*pendingWakes)
	return w
}

// collect is the garbage-collection step for a single object, run after its
// controller's Reconcile returns (see typedController.reconcile) and on the global
// GC sweep (see sweepDeletionPending). It runs in its own transaction. It is a no-op
// unless the object is finalizing.
//
// Two things happen for a finalizing object:
//
//   - Cascade: every object that owns_by this one is itself marked for deletion
//     and requeued, so deleting an owner tears its children down with it.
//   - Physical delete: once the object has no finalizers left AND nothing still
//     references it, its row is removed. The refs table's ON DELETE RESTRICT
//     makes that ordering mandatory — an owner cannot be removed while a child
//     still points at it — and ON DELETE CASCADE on the child side means
//     removing the last child drops the edge that was blocking the owner. The
//     freed owner is re-examined by the global GC sweep.
//
// The whole step runs in one transaction so the cascade writes and the delete
// commit together; the watch events they emit publish only on commit.
func (bh *Beehive) collect(ctx context.Context, id ObjectID) (deleted bool, err error) {
	// toWake accumulates objects to requeue after the transaction commits: the
	// cascaded children, plus (when the row is removed) the targets it was holding
	// open. Waking post-commit means a rollback never leaves a phantom enqueue,
	// matching the dependency waker's post-commit pattern.
	var toWake []Referrer
	err = bh.store.Within(ctx, func(ctx context.Context) error {
		obj, err := bh.store.GetObjectMeta(ctx, id)
		if err != nil {
			return err
		}
		// Not finalizing: nothing to collect.
		if obj.DeletionRequestedAt == nil {
			return nil
		}

		// Cascade deletion to owned children, requeuing them all (see
		// MarkOwnedForDeletion for the steady-state single-read path).
		children, err := bh.store.MarkOwnedForDeletion(ctx, id)
		if err != nil {
			return err
		}
		toWake = append(toWake, children...)

		// Finalizers still pending: the controller hasn't finished cleanup.
		if len(obj.Finalizers) > 0 {
			return nil
		}
		// A dependent that's itself finalizing has no claim on us: drop those
		// depends_on edges before the referrer gate, or two deletion-pending objects
		// that depend on each other (or a self-dependency) would each hold the
		// other's RESTRICT forever. owned_by edges are left for the cascade.
		if err := bh.store.DeleteFinalizingDependsOnRefs(ctx, id); err != nil {
			return err
		}
		// Still referenced (owned children or live dependents): RESTRICT forbids the
		// delete. Leave the row; a referrer's own removal will wake us (below).
		referenced, err := bh.store.HasIncomingRefs(ctx, id)
		if err != nil {
			return err
		}
		if referenced {
			return nil
		}

		// Removing this row drops its outgoing edges (ON DELETE CASCADE), which may
		// unblock a deletion-pending target RESTRICT was holding. Capture those
		// targets before the delete so we can wake them — the event-driven path that
		// lets a cascade finish without waiting on the next GC sweep.
		referents, err := bh.store.ListOutgoingRefs(ctx, id)
		if err != nil {
			return err
		}
		if err := bh.store.DeleteObject(ctx, id); err != nil {
			return err
		}
		toWake = append(toWake, referents...)
		deleted = true
		return nil
	})
	if err != nil {
		return false, err
	}
	for _, w := range toWake {
		bh.advanceGC(ctx, w.GroupKind(), w.ID)
	}
	return deleted, nil
}

// advanceGC moves (gk, id) forward toward collection after a deletion-related
// change, once the caller's transaction (if any) has committed.
//
// The post-commit hook is what makes it safe to call from inside a transaction —
// Client.Delete nested in a controller's ControllerClient.Within, or collect's own
// toWake loop when collect itself is nested. Running inline there would wake a
// reconciler for an uncommitted deletion: a phantom wake if the caller rolls back,
// and otherwise a controller reading a row whose tombstone is not visible yet.
// Deferring costs nothing when there is no transaction: AfterCommit runs the hook
// inline.
//
// Collection stays correct across the deferral because collect is level-triggered:
// it re-reads DeletionRequestedAt, so if the caller rolled the deletion back there
// is simply nothing to collect.
//
// Now that advanceGCNow only requeues, this is mechanically what wakeAfterCommit
// does. The two are kept as separate names because they answer separate questions —
// a spec write owes the object a reconcile, a deletion owes it a collect — and
// advanceGCNow is where the second one's answer changes if it ever grows past a
// requeue (a durable stamp, say, as pending_wake is for dependency wakes).
func (bh *Beehive) advanceGC(ctx context.Context, gk GroupKind, id ObjectID) {
	bh.store.AfterCommit(ctx, func(context.Context) {
		bh.advanceGCNow(gk, id)
	})
}

// wakeAfterCommit schedules a reconciler wake for gk/id once the ambient
// transaction commits and its watch events are out — the wake sibling of
// advanceGC, kept non-generic here rather than on the typed client. Registering
// on the store rather than enqueuing after Within is what keeps the ordering
// under nesting: a caller may have wrapped the write in ControllerClient.Within,
// and the nested Within returns while that outer transaction is still open — so a
// wake issued there could reach a controller before the spec event (letting a
// Modified overtake the Added), or at all for a row the outer transaction then
// rolls back. Outside a transaction AfterCommit runs it inline.
func (bh *Beehive) wakeAfterCommit(ctx context.Context, gk GroupKind, id ObjectID) {
	bh.store.AfterCommit(ctx, func(context.Context) { bh.enqueueIfRegistered(gk, id) })
}

// advanceGCNow is advanceGC's body, run with no transaction in flight. A
// registered kind is requeued so its own reconcile loop runs collect; a
// client-only kind has no loop to requeue onto, and the global sweeper's next
// tick is its backstop — which is exactly what enqueueIfRegistered's no-op arm
// leaves it to.
//
// That backstop is unconditional, which is why this needs no second arm: the
// sweeper always has a cadence (WithGCInterval rejects a non-positive interval),
// so the wait is bounded by one tick. Collecting inline here instead would run a
// recursive cascade of physical deletes on the caller's goroutine and ctx — so a
// caller cancelling right after its commit could abandon the cascade mid-flight,
// with nothing scheduled to resume it.
func (bh *Beehive) advanceGCNow(gk GroupKind, id ObjectID) {
	bh.enqueueIfRegistered(gk, id)
}
