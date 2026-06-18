package beehive

import "context"

type WatchEventType string

const (
	WatchEventAdded    WatchEventType = "Added"
	WatchEventModified WatchEventType = "Modified"
	WatchEventDeleted  WatchEventType = "Deleted"
)

type WatchEvent[Spec, Status any] struct {
	Type   WatchEventType
	Object *Object[Spec, Status]
}

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

func NewClient[Spec, Status any](bh *Beehive, gk GroupKind) Client[Spec, Status] {
	return &clientImpl[Spec, Status]{bh: bh, gk: gk}
}

type clientImpl[Spec, Status any] struct {
	bh *Beehive
	gk GroupKind
}

func (c *clientImpl[Spec, Status]) Create(_ context.Context, _ Spec, _ ...Option) (*Object[Spec, Status], error) {
	panic("not implemented")
}

func (c *clientImpl[Spec, Status]) Update(_ context.Context, _ ObjectID, _ Spec) (*Object[Spec, Status], error) {
	panic("not implemented")
}

func (c *clientImpl[Spec, Status]) Get(_ context.Context, _ ObjectID) (*Object[Spec, Status], error) {
	panic("not implemented")
}

func (c *clientImpl[Spec, Status]) GetByName(_ context.Context, _ string) (*Object[Spec, Status], error) {
	panic("not implemented")
}

func (c *clientImpl[Spec, Status]) List(_ context.Context) ([]*Object[Spec, Status], error) {
	panic("not implemented")
}

func (c *clientImpl[Spec, Status]) Delete(_ context.Context, _ ObjectID) error {
	panic("not implemented")
}

func (c *clientImpl[Spec, Status]) Watch(_ context.Context, _ ObjectID) (<-chan WatchEvent[Spec, Status], error) {
	panic("not implemented")
}

func (c *clientImpl[Spec, Status]) WatchList(_ context.Context) (<-chan WatchEvent[Spec, Status], error) {
	panic("not implemented")
}
