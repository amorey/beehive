package beehive

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/amorey/beehive/internal/storeapi"
)

// WatchEvent reports a change to a watched object.
type WatchEvent[Spec, Status any] struct {
	Type   WatchEventType
	Object *Object[Spec, Status]
}

// Client is the user-facing API for a single resource kind: the surface for
// creating, reading, updating, deleting, and watching objects.
type Client[Spec, Status any] interface {
	Create(ctx context.Context, spec Spec, opts ...Option) (*Object[Spec, Status], error)
	Update(ctx context.Context, id ObjectID, spec Spec) (*Object[Spec, Status], error)
	Get(ctx context.Context, id ObjectID) (*Object[Spec, Status], error)
	GetByName(ctx context.Context, name string) (*Object[Spec, Status], error)
	List(ctx context.Context) ([]*Object[Spec, Status], error)
	Delete(ctx context.Context, id ObjectID) error
	Watch(ctx context.Context, id ObjectID) (<-chan WatchEvent[Spec, Status], error)
	WatchList(ctx context.Context) (<-chan WatchEvent[Spec, Status], error)
}

// NewClient returns a Client for the given resource kind. Spec and Status must
// match the controller registered for gk.
func NewClient[Spec, Status any](bh *Beehive, gk GroupKind) Client[Spec, Status] {
	return &clientImpl[Spec, Status]{bh: bh, gk: gk}
}

type clientImpl[Spec, Status any] struct {
	bh *Beehive
	gk GroupKind
}

func (c *clientImpl[Spec, Status]) Create(ctx context.Context, spec Spec, opts ...Option) (*Object[Spec, Status], error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	co := &createOptions{}
	for _, o := range opts {
		if err := o(co); err != nil {
			return nil, err
		}
	}

	var raw *RawObject
	// Within keeps the insert and its owner ref atomic, so a crash between them
	// can't leave an ownerless child the GC path would never collect.
	err = c.bh.store.Within(ctx, func(ctx context.Context) error {
		raw, err = c.bh.store.CreateObject(ctx, &RawObject{
			Group:      c.gk.Group,
			Kind:       c.gk.Kind,
			Name:       co.name,
			Spec:       b,
			Finalizers: co.finalizers,
		})
		if err != nil {
			return err
		}
		// The child owns the edge (child -> owner) so the owner's GC walk finds it
		// via ListReferrers(owner, RelationOwnedBy).
		if co.owner != nil {
			return c.bh.store.AddRef(ctx, raw.ID, *co.owner, RelationOwnedBy)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	obj, err := rawToTyped[Spec, Status](raw)
	if err != nil {
		return nil, err
	}
	// The store emitted the Added event inside CreateObject, before this enqueue,
	// so a fast controller can't produce a Modified event ahead of the Added.
	c.bh.enqueueIfRegistered(c.gk, raw.ID)
	return obj, nil
}

func (c *clientImpl[Spec, Status]) Update(ctx context.Context, id ObjectID, spec Spec) (*Object[Spec, Status], error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	raw, err := c.bh.store.UpdateSpec(ctx, id, b)
	if err != nil {
		return nil, err
	}
	obj, err := rawToTyped[Spec, Status](raw)
	if err != nil {
		return nil, err
	}
	c.bh.enqueueIfRegistered(c.gk, raw.ID)
	return obj, nil
}

func (c *clientImpl[Spec, Status]) Get(ctx context.Context, id ObjectID) (*Object[Spec, Status], error) {
	raw, err := c.bh.store.GetObject(ctx, id)
	if err != nil {
		return nil, err
	}
	return rawToTyped[Spec, Status](raw)
}

func (c *clientImpl[Spec, Status]) GetByName(ctx context.Context, name string) (*Object[Spec, Status], error) {
	raw, err := c.bh.store.GetObjectByName(ctx, c.gk, name)
	if err != nil {
		return nil, err
	}
	return rawToTyped[Spec, Status](raw)
}

func (c *clientImpl[Spec, Status]) List(ctx context.Context) ([]*Object[Spec, Status], error) {
	raws, err := c.bh.store.ListObjects(ctx, c.gk)
	if err != nil {
		return nil, err
	}
	objs := make([]*Object[Spec, Status], 0, len(raws))
	for _, raw := range raws {
		obj, err := rawToTyped[Spec, Status](raw)
		if err != nil {
			return nil, err
		}
		objs = append(objs, obj)
	}
	return objs, nil
}

func (c *clientImpl[Spec, Status]) Delete(ctx context.Context, id ObjectID) error {
	// RequestDeletion emits the Modified event itself, and only on a real state
	// change — an idempotent retry carries the same resource_version, so emitting
	// would show watchers a spurious diff.
	if _, _, err := c.bh.store.RequestDeletion(ctx, id); err != nil {
		return err
	}
	// Always enqueue: a retry or post-crash Delete must still hand the
	// deletion-pending object to the controller to clear finalizers.
	c.bh.enqueueIfRegistered(c.gk, id)
	return nil
}

func (c *clientImpl[Spec, Status]) WatchList(ctx context.Context) (<-chan WatchEvent[Spec, Status], error) {
	if !c.bh.isRegistered(c.gk) {
		return nil, fmt.Errorf("beehive: no controller registered for %s/%s", c.gk.Group, c.gk.Kind)
	}
	w, err := c.bh.store.WatchList(ctx, c.gk)
	if err != nil {
		return nil, err
	}
	return c.adaptWatcher(ctx, w), nil
}

func (c *clientImpl[Spec, Status]) Watch(ctx context.Context, id ObjectID) (<-chan WatchEvent[Spec, Status], error) {
	if !c.bh.isRegistered(c.gk) {
		return nil, fmt.Errorf("beehive: no controller registered for %s/%s", c.gk.Group, c.gk.Kind)
	}
	w, err := c.bh.store.Watch(ctx, c.gk, id)
	if err != nil {
		return nil, err
	}
	return c.adaptWatcher(ctx, w), nil
}

// adaptWatcher decodes a store Watcher's raw events (the snapshot's Added events
// followed by live changes — the store owns snapshotting, dedup, and id
// filtering) into typed WatchEvents. It forwards on the returned channel until
// ctx is cancelled, the watcher's stream ends, or an event fails to decode; the
// channel closes and the watcher is released on exit.
func (c *clientImpl[Spec, Status]) adaptWatcher(ctx context.Context, w Watcher) <-chan WatchEvent[Spec, Status] {
	out := make(chan WatchEvent[Spec, Status])
	go func() {
		defer close(out)
		defer w.Close()
		for {
			select {
			case ev, ok := <-w.Events():
				if !ok {
					return
				}
				obj, err := rawToTyped[Spec, Status](ev.Object)
				if err != nil {
					return
				}
				select {
				case out <- WatchEvent[Spec, Status]{Type: ev.Type, Object: obj}:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// conditionsFromRaw maps the store's raw conditions to the public Condition
// type, dropping the storage-only bookkeeping (last-transition/updated/observed
// generation) that the user-facing type doesn't carry. Returns nil for none.
func conditionsFromRaw(raw []storeapi.Condition) []Condition {
	if len(raw) == 0 {
		return nil
	}
	out := make([]Condition, len(raw))
	for i, c := range raw {
		out[i] = Condition{
			Type:     c.Type,
			Status:   ConditionStatus(c.Status),
			Reason:   c.Reason,
			Message:  c.Message,
			Liveness: c.Liveness,
		}
	}
	return out
}

// rawToTyped decodes a RawObject into a typed Object[Spec, Status].
func rawToTyped[Spec, Status any](raw *RawObject) (*Object[Spec, Status], error) {
	var spec Spec
	if err := json.Unmarshal(raw.Spec, &spec); err != nil {
		return nil, err
	}
	obj := &Object[Spec, Status]{
		ID:                  raw.ID,
		Group:               raw.Group,
		Kind:                raw.Kind,
		Name:                raw.Name,
		Spec:                spec,
		Generation:          raw.Generation,
		ObservedGeneration:  raw.ObservedGeneration,
		ObservedAt:          raw.ObservedAt,
		ResourceVersion:     raw.ResourceVersion,
		DeletionRequestedAt: raw.DeletionRequestedAt,
		Finalizers:          raw.Finalizers,
		Conditions:          conditionsFromRaw(raw.Conditions),
		CreatedAt:           raw.CreatedAt,
		UpdatedAt:           raw.UpdatedAt,
	}
	if raw.Status != nil {
		var status Status
		if err := json.Unmarshal(raw.Status, &status); err != nil {
			return nil, err
		}
		obj.Status = &status
	}
	return obj, nil
}
