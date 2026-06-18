package beehive

import "time"

type Option func(target any) error

func WithName(name string) Option {
	return func(target any) error {
		return nil
	}
}

func WithFinalizers(f ...string) Option {
	return func(target any) error {
		return nil
	}
}

func WithOwner(id ObjectID) Option {
	return func(target any) error {
		return nil
	}
}

func WithResyncInterval(d time.Duration) Option {
	return func(target any) error {
		switch t := target.(type) {
		case *Beehive:
			t.resyncInterval = d
		case *reconciler:
			t.resyncInterval = d
		}
		return nil
	}
}

func WithMaxRetryInterval(d time.Duration) Option {
	return func(target any) error {
		if t, ok := target.(*reconciler); ok {
			t.maxRetryInterval = d
		}
		return nil
	}
}
