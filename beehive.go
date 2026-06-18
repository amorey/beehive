package beehive

import (
	"context"
	"time"
)

const defaultResyncInterval = 30 * time.Second

type Beehive struct {
	store          Store
	resyncInterval time.Duration
}

func New(s Store, opts ...Option) (*Beehive, error) {
	bh := &Beehive{
		store:          s,
		resyncInterval: defaultResyncInterval,
	}
	for _, o := range opts {
		if err := o(bh); err != nil {
			return nil, err
		}
	}
	return bh, nil
}

func Register[Spec, Status any](bh *Beehive, gk GroupKind, c Controller[Spec, Status], opts ...Option) error {
	panic("not implemented")
}

func (bh *Beehive) Start() error {
	panic("not implemented")
}

func (bh *Beehive) Stop(ctx context.Context) {
	panic("not implemented")
}
