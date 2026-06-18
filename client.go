package beehive

import (
	"context"
	"encoding/json"
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
	c.bh.mu.Lock()
	r, ok := c.bh.reconcilers[c.gk]
	c.bh.mu.Unlock()
	if ok {
		r.enqueue(raw.ID)
	}
	return rawToTyped[Spec, Status](raw)
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
	c.bh.mu.Lock()
	r, ok := c.bh.reconcilers[c.gk]
	c.bh.mu.Unlock()
	if ok {
		r.enqueue(raw.ID)
	}
	return rawToTyped[Spec, Status](raw)
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
	_, err := c.bh.store.RequestDeletion(ctx, id)
	if err != nil {
		return err
	}
	c.bh.mu.Lock()
	r, ok := c.bh.reconcilers[c.gk]
	c.bh.mu.Unlock()
	if ok {
		r.enqueue(id)
	}
	return nil
}

func (c *clientImpl[Spec, Status]) Watch(_ context.Context, _ ObjectID) (<-chan WatchEvent[Spec, Status], error) {
	panic("not implemented")
}

func (c *clientImpl[Spec, Status]) WatchList(_ context.Context) (<-chan WatchEvent[Spec, Status], error) {
	panic("not implemented")
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
