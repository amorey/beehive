package beehive

import "context"

// pendingWakes collects ref targets to requeue after a reconcile transaction
// commits. A ControllerClient call that frees a target (DeleteDependency)
// registers it here; typedController.reconcile drains it post-commit. It rides
// on the context so the long-lived, shared ControllerClient holds no
// per-reconcile state, and a single reconcile's Reconcile runs on one goroutine,
// so the slice needs no locking.
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
// controller reconcile commits (see typedController.reconcile) and on the
// deletion-pending resync backstop. It is a no-op unless the object is
// finalizing.
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
//     freed owner is re-examined by the deletion-pending resync backstop.
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
		obj, err := bh.store.GetObject(ctx, id)
		if err != nil {
			return err
		}
		// Not finalizing: nothing to collect.
		if obj.DeletionRequestedAt == nil {
			return nil
		}

		// Cascade deletion to owned children.
		children, err := bh.store.ListReferrers(ctx, id, RelationOwnedBy)
		if err != nil {
			return err
		}
		for _, c := range children {
			if _, _, err := bh.store.RequestDeletion(ctx, c.ID); err != nil {
				return err
			}
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
		referenced, err := bh.store.HasReferrers(ctx, id)
		if err != nil {
			return err
		}
		if referenced {
			return nil
		}

		// Removing this row drops its outgoing edges (ON DELETE CASCADE), which may
		// unblock a deletion-pending target RESTRICT was holding. Capture those
		// targets before the delete so we can wake them — the event-driven path that
		// lets a cascade finish without waiting on the resync backstop.
		referents, err := bh.store.ListReferents(ctx, id)
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
		bh.enqueueIfRegistered(GroupKind{Group: w.Group, Kind: w.Kind}, w.ID)
	}
	return deleted, nil
}
