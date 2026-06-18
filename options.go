package beehive

import "time"

// Option configures a target — a Beehive, a reconciler, or a per-object
// operation — depending on where it is passed. Each option type-switches on the
// targets it understands and ignores the rest.
type Option func(target any) error

// WithName sets the object's unique name. (stub: not yet wired up)
func WithName(name string) Option {
	return func(target any) error {
		return nil
	}
}

// WithFinalizers attaches finalizers that must be cleared before an object is
// deleted. (stub: not yet wired up)
func WithFinalizers(f ...string) Option {
	return func(target any) error {
		return nil
	}
}

// WithOwner records an owning object, so the child is cleaned up with its owner.
// (stub: not yet wired up)
func WithOwner(id ObjectID) Option {
	return func(target any) error {
		return nil
	}
}

// WithResyncInterval sets the periodic resync interval for a controller. A
// value <= 0 disables periodic resync, leaving the controller event-driven
// only.
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
