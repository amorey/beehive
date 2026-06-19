package beehive

import "time"

// Option configures a target — a Beehive, a reconciler, or a per-object
// operation — depending on where it is passed. Each option type-switches on the
// targets it understands and ignores the rest.
type Option func(target any) error

// StartupReconcileStrategy selects which objects a controller reconciles once at
// startup. The zero value is StartupReconcileAll, so the safe default holds for a
// controller that never sets it.
type StartupReconcileStrategy int

const (
	// StartupReconcileAll reconciles every object at startup, settled or not. The
	// full pass re-confirms process-scoped state such as liveness conditions,
	// which read as "verifying" after a restart until a controller rewrites them.
	StartupReconcileAll StartupReconcileStrategy = iota
	// StartupReconcileUnsettled reconciles only objects whose spec has not yet
	// converged — cheaper, but leaves process-scoped state unconfirmed until some
	// other event wakes the object.
	StartupReconcileUnsettled
	// StartupReconcileNone does no startup reconcile at all, leaving the periodic
	// resync (and live events) as the only drivers.
	StartupReconcileNone
)

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

// WithStartupReconcileStrategy sets which objects a controller reconciles at
// startup (see StartupReconcileStrategy). The default is StartupReconcileAll.
// Passed to New it sets the default for all controllers; passed to Register it
// overrides that default for one.
func WithStartupReconcileStrategy(s StartupReconcileStrategy) Option {
	return func(target any) error {
		switch t := target.(type) {
		case *Beehive:
			t.startupReconcile = s
		case *reconciler:
			t.startupReconcile = s
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

// WithConcurrency sets the number of concurrent worker goroutines for a
// controller. When passed to New it becomes the default for all controllers;
// when passed to Register it overrides that default for a single controller.
// A value <= 1 means single-threaded (the default).
func WithConcurrency(n int) Option {
	return func(target any) error {
		switch t := target.(type) {
		case *Beehive:
			t.concurrency = n
		case *reconciler:
			t.concurrency = n
		}
		return nil
	}
}
