package beehive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// WatchEventType classifies a WatchEvent.
type WatchEventType string

const (
	WatchEventAdded    WatchEventType = "Added"
	WatchEventModified WatchEventType = "Modified"
	WatchEventDeleted  WatchEventType = "Deleted"
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

func (c *clientImpl[Spec, Status]) Create(ctx context.Context, spec Spec, _ ...Option) (*Object[Spec, Status], error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	raw, err := c.bh.store.CreateObject(ctx, &RawObject{
		Group: c.gk.Group,
		Kind:  c.gk.Kind,
		Spec:  b,
	})
	if err != nil {
		return nil, err
	}
	obj, err := rawToTyped[Spec, Status](raw)
	if err != nil {
		return nil, err
	}
	// Publish before enqueuing: a fast controller must not produce a Modified
	// event before watchers see the Added event that preceded it.
	c.bh.publishEvent(c.gk, WatchEventAdded, raw)
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
	c.bh.publishEvent(c.gk, WatchEventModified, raw)
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
	raw, changed, err := c.bh.store.RequestDeletion(ctx, id)
	if err != nil {
		return err
	}
	// Only emit a watch event for a real state change; idempotent retries
	// carry the same resource_version so watchers would see a spurious diff.
	if changed {
		c.bh.publishEvent(c.gk, WatchEventModified, raw)
	}
	// Always enqueue: a retry or post-crash Delete must still hand the
	// deletion-pending object to the controller to clear finalizers.
	c.bh.enqueueIfRegistered(c.gk, id)
	return nil
}

func (c *clientImpl[Spec, Status]) WatchList(ctx context.Context) (<-chan WatchEvent[Spec, Status], error) {
	return c.watchFiltered(ctx,
		func(ctx context.Context) ([]*RawObject, error) {
			return c.bh.store.ListObjects(ctx, c.gk)
		},
		nil)
}

func (c *clientImpl[Spec, Status]) Watch(ctx context.Context, id ObjectID) (<-chan WatchEvent[Spec, Status], error) {
	return c.watchFiltered(ctx,
		func(ctx context.Context) ([]*RawObject, error) {
			raw, err := c.bh.store.GetObject(ctx, id)
			if errors.Is(err, ErrNotFound) {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
			return []*RawObject{raw}, nil
		},
		func(oid ObjectID) bool { return oid == id })
}

// watchFiltered creates a watch channel that first emits current store state as
// Added events (the snapshot), then streams live events from the hub.
//
// The hub receiver is created before the snapshot is loaded so that any events
// published during the load are buffered and not lost. After the snapshot,
// hub events whose resource version is already covered by the snapshot are
// dropped to prevent duplicates. filter, if non-nil, is applied to live hub
// events; snapshot rows are already pre-filtered by loadSnapshot. The channel
// closes when ctx is cancelled, the hub shuts down, or the receiver lags.
func (c *clientImpl[Spec, Status]) watchFiltered(
	ctx context.Context,
	loadSnapshot func(context.Context) ([]*RawObject, error),
	filter func(ObjectID) bool,
) (<-chan WatchEvent[Spec, Status], error) {
	c.bh.watchMu.RLock()
	hub := c.bh.watchHubs[c.gk]
	c.bh.watchMu.RUnlock()
	if hub == nil {
		return nil, fmt.Errorf("beehive: no watch hub for %s/%s", c.gk.Group, c.gk.Kind)
	}
	// Subscribe before loading the snapshot so live events published during the
	// load are buffered in the receiver and not silently dropped.
	rx := hub.Receiver()
	snapshot, err := loadSnapshot(ctx)
	if err != nil {
		rx.Close()
		return nil, err
	}
	// Record each snapshot object's resource version. Hub events that committed
	// during the snapshot load may appear in both the snapshot and the buffer;
	// any event with rv ≤ the snapshot rv for that object is already covered.
	snapshotRV := make(map[ObjectID]int64, len(snapshot))
	for _, raw := range snapshot {
		snapshotRV[raw.ID] = raw.ResourceVersion
	}
	out := make(chan WatchEvent[Spec, Status])
	go func() {
		defer close(out)
		defer rx.Close()
		// Emit snapshot as Added events before streaming live hub events.
		for _, raw := range snapshot {
			obj, convErr := rawToTyped[Spec, Status](raw)
			if convErr != nil {
				return
			}
			select {
			case out <- WatchEvent[Spec, Status]{Type: WatchEventAdded, Object: obj}:
			case <-ctx.Done():
				return
			}
		}
		for {
			ev, err := rx.RecvContext(ctx)
			if err != nil {
				return
			}
			if ev.Object.ResourceVersion <= snapshotRV[ev.Object.ID] {
				continue // already represented by snapshot
			}
			if filter != nil && !filter(ev.Object.ID) {
				continue
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
		}
	}()
	return out, nil
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
