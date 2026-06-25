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

import (
	"log/slog"
	"time"
)

// Option configures a target — a Beehive, a reconciler, or a per-object
// operation — depending on where it is passed. Each option type-switches on the
// targets it understands and ignores the rest.
type Option func(target any) error

// LoadOption selects a secondary lookup to fetch alongside an object on a read.
// It is distinct from Option: it applies only to read call sites (Get/GetBySlug/
// List), composing into a LoadSet. Lazy fetching is the alternative — omit the
// selector and call Client.GetOwner/ListDependencies when the data is needed.
type LoadOption func(*LoadSet)

// LoadOwner selects the object's owner (its outgoing owned_by edge).
func LoadOwner() LoadOption {
	return func(s *LoadSet) { *s |= LoadOwnerBit }
}

// LoadDependencies selects the objects this one depends on (outgoing depends_on).
func LoadDependencies() LoadOption {
	return func(s *LoadSet) { *s |= LoadDependenciesBit }
}

// LoadDependents selects the objects that depend on this one (incoming depends_on).
func LoadDependents() LoadOption {
	return func(s *LoadSet) { *s |= LoadDependentsBit }
}

// LoadOwned selects the objects this one owns (its incoming owned_by edges).
func LoadOwned() LoadOption {
	return func(s *LoadSet) { *s |= LoadOwnedBit }
}

// resolveLoads folds the per-call selectors into a single LoadSet.
func resolveLoads(opts []LoadOption) LoadSet {
	var set LoadSet
	for _, o := range opts {
		o(&set)
	}
	return set
}

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

// createOptions collects the per-object settings the create-time options apply.
// Client.Create builds one, runs the options against it, and folds the result
// into the new row (slug/finalizers) and its owner ref.
type createOptions struct {
	slug       *string
	finalizers []string
	owner      *ObjectID
}

// WithSlug sets the object's unique slug, looked up later via GetBySlug.
func WithSlug(slug string) Option {
	return func(target any) error {
		if t, ok := target.(*createOptions); ok {
			t.slug = &slug
		}
		return nil
	}
}

// WithFinalizers attaches finalizers that must be cleared before an object is
// physically deleted.
func WithFinalizers(f ...string) Option {
	return func(target any) error {
		if t, ok := target.(*createOptions); ok {
			t.finalizers = f
		}
		return nil
	}
}

// WithOwner records an owning object, so the child is cleaned up with its owner.
func WithOwner(id ObjectID) Option {
	return func(target any) error {
		if t, ok := target.(*createOptions); ok {
			t.owner = &id
		}
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

// WithMigrator registers a Migrator for the controller's kind, supplying the
// schema-version conversion applied to stored Spec/Status JSON on read (see
// Migrator). It is meaningful only at Register — a migrator is per-kind, and
// Register installs it into the shared registry that both the user-facing client
// and the reconciler decode through. Passed anywhere else it is ignored.
func WithMigrator(m Migrator) Option {
	return func(target any) error {
		if t, ok := target.(*reconciler); ok {
			t.migrator = m
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

// WithLogger routes beehive's internal logging through l. Pass a logger whose
// slog.Handler wraps your logging library — zap, zerolog, logrus, and logr all
// ship slog bridges — to forward beehive's logs into it. A nil logger disables
// logging entirely, which is the default.
//
// Passed to New it sets the logger for the control plane and the default for all
// controllers; passed to Register it overrides that default for one controller.
func WithLogger(l *slog.Logger) Option {
	return func(target any) error {
		switch t := target.(type) {
		case *Beehive:
			t.logger = l
		case *reconciler:
			t.logger = l
		}
		return nil
	}
}

// WithLogLevel sets the minimum level beehive emits, layered on top of whatever
// the logger's own handler already filters. It lets callers quiet beehive down
// without building a leveled handler; pass a very high level to silence it while
// keeping the logger wired up. Has no effect without WithLogger (the discard
// logger emits nothing regardless).
//
// Passed to New it applies to the control plane and is the default for all
// controllers; passed to Register it overrides that default for one controller.
func WithLogLevel(level slog.Level) Option {
	return func(target any) error {
		switch t := target.(type) {
		case *Beehive:
			t.logLevel = level
		case *reconciler:
			t.logLevel = level
		}
		return nil
	}
}

// WithMaxRetryInterval caps the exponential backoff between failed reconciles
// for a controller (the default is defaultMaxRetryInterval). A value <= 0 is
// ignored, keeping the default: a zero or negative cap would clamp every retry
// delay to it and busy-loop the reconciler the instant it keeps returning an
// error, which is never what a caller wants.
func WithMaxRetryInterval(d time.Duration) Option {
	return func(target any) error {
		if t, ok := target.(*reconciler); ok && d > 0 {
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
