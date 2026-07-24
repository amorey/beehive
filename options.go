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
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/amorey/beehive/internal/storeapi"
)

// Option configures a target — a Beehive, a reconciler, or a per-object
// operation — depending on where it is passed. Each option type-switches on the
// targets it understands and ignores the rest.
type Option func(target any) error

// ErrConflictingOption reports an option that contradicts an argument the call
// already carries — passing WithSlug to a method that takes the slug positionally,
// for instance. It is distinct from an option being *inapplicable*: an option
// aimed at a target it doesn't understand is ignored by design (see Option), while
// a contradiction is a caller mistake whose effect would otherwise be invisible.
var ErrConflictingOption = errors.New("beehive: option conflicts with an explicit argument")

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

// LoadEvents selects the object's event-log runs, read via Object.ListEvents().
// It loads the object's current runs (bounded by retention); for filtered or
// bounded reads use the lazy Client.ListEvents with EventOptions instead.
func LoadEvents() LoadOption {
	return func(s *LoadSet) { *s |= LoadEventsBit }
}

// resolveLoads folds the per-call selectors into a single LoadSet.
func resolveLoads(opts []LoadOption) LoadSet {
	var set LoadSet
	for _, o := range opts {
		o(&set)
	}
	return set
}

// RequeueOption configures a Client.Requeue call. Like LoadOption it is distinct
// from Option: it applies only to Requeue.
type RequeueOption func(*requeueOptions)

type requeueOptions struct {
	resetBackoff bool
}

// WithResetBackoff makes a Requeue clear the object's retry backoff ladder so its
// next failure retries from the base interval. Pass it only when the caller has
// proof the failure condition is resolved. A plain Requeue preserves the ladder:
// backoff is cleared by a successful reconcile or an explicit WithResetBackoff, never
// by merely being asked to try again.
func WithResetBackoff() RequeueOption {
	return func(o *requeueOptions) { o.resetBackoff = true }
}

// resolveRequeue folds the per-call options into a single requeueOptions.
func resolveRequeue(opts []RequeueOption) requeueOptions {
	var o requeueOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// EventOption filters a Client.ListEvents / WatchEvents read. Like LoadOption and
// RequeueOption it is distinct from Option: it applies only to the event reads,
// composing into the store's event query.
type EventOption func(*storeapi.EventQuery)

// WithEventCategory restricts a read to a single timeline. The category "" is the
// default timeline (distinct from "no filter", which is the absence of this option).
func WithEventCategory(category string) EventOption {
	return func(q *storeapi.EventQuery) { q.Category = &category }
}

// WithEventType restricts a read to one severity (Normal or Warning).
func WithEventType(t EventType) EventOption {
	return func(q *storeapi.EventQuery) { q.Type = string(t) }
}

// WithEventReason restricts a read to runs with the given reason.
func WithEventReason(reason string) EventOption {
	return func(q *storeapi.EventQuery) { q.Reason = reason }
}

// WithEventLimit caps a read to the newest n runs. It bounds only the snapshot of
// a WatchEvents subscription, not its live stream.
func WithEventLimit(n int) EventOption {
	return func(q *storeapi.EventQuery) { q.Limit = n }
}

// WithEventsSince restricts a read to runs still active at or after t (LastAt >= t).
func WithEventsSince(t time.Time) EventOption {
	return func(q *storeapi.EventQuery) { q.Since = t }
}

// resolveEvents folds the per-call options into a single store event query.
func resolveEvents(opts []EventOption) storeapi.EventQuery {
	var q storeapi.EventQuery
	for _, o := range opts {
		o(&q)
	}
	return q
}

// StartupReconcileStrategy selects which objects a controller reconciles once at
// startup. The zero value is StartupReconcileAll, so the safe default holds for a
// controller that never sets it.
//
// It governs the spec-convergence pass only. In-progress deletions and objects owed
// a durable dependency wake are resumed at startup under every strategy, including
// StartupReconcileNone: those are store bookkeeping rather than spec convergence —
// a half-deleted row RESTRICT-blocks its owner until collected, and an owed wake was
// explicitly stamped — so an embedder driving its own reconciles is not expected to
// know about them.
type StartupReconcileStrategy int

const (
	// StartupReconcileAll reconciles every object at startup, settled or not. The
	// full pass re-confirms process-scoped state such as liveness conditions,
	// which read as "verifying" after a restart until a controller rewrites them.
	StartupReconcileAll StartupReconcileStrategy = iota
	// StartupReconcileUnsettled reconciles only objects whose spec has not yet
	// converged — cheaper, but leaves process-scoped state unconfirmed until some
	// other event wakes the object. This is the pass that resumes an object a
	// previous process left unconverged (crashed mid-reconcile, or created and
	// never settled), so it is the cheapest strategy that still recovers on restart.
	StartupReconcileUnsettled
	// StartupReconcileNone does no startup reconcile at all, leaving live events
	// and the periodic resync as the only drivers.
	//
	// Combined with a disabled resync (WithResyncInterval(0)) it leaves no automatic
	// driver for spec convergence whatsoever: an object a previous process left
	// unconverged is *not* resumed, and its spec is never actuated unless something
	// requeues it. That combination is supported, for an embedder that wants to
	// drive reconciles on its own schedule rather than beehive's — the store's
	// ListUnsettledIDs reports exactly the objects owed a pass, and Client.Requeue
	// dispatches one:
	//
	//	ids, err := store.ListUnsettledIDs(ctx, gk)
	//	for _, id := range ids {
	//		err := client.Requeue(ctx, id)
	//	}
	//
	// Because the failure mode when it is reached by accident is silent, a
	// controller starting in that configuration logs a warning naming it. Choose
	// StartupReconcileUnsettled instead if you did not mean to take over recovery.
	StartupReconcileNone
)

// createOptions collects the per-object settings the create-time options apply.
// Client.Create builds one, runs the options against it, and folds the result
// into the new row (slug/finalizers) and its owner ref.
type createOptions struct {
	slug       *string
	finalizers []string
	owner      *ObjectID
	onCreate   func(context.Context)
}

// resolveCreate folds the create-time options into one createOptions, the
// counterpart to resolveLoads/resolveRequeue/resolveEvents for the create
// family. It errors on the first option that does, so a bad option fails the
// call before any store work.
func resolveCreate(opts []Option) (*createOptions, error) {
	co := &createOptions{}
	for _, o := range opts {
		if err := o(co); err != nil {
			return nil, err
		}
	}
	return co, nil
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

// WithOnCreate registers fn to run once, on the caller's ctx, only if this call
// actually inserts a new row — and only after the outermost transaction it runs
// in commits. Create always fires it; GetOrCreate fires it on the create branch
// but not when it returns an existing row.
//
// It is the commit-safe channel for create-conditional side effects. The
// GetOrCreate created bool is synchronous, so inside a caller's
// ControllerClient.Within it reports true before the enclosing transaction
// commits — act on it for a non-store side effect (an external call, an
// in-memory counter) and a later rollback leaves that effect fired for a row
// that never landed. fn is deferred through the same post-commit path as the
// reconcile wake (Store.AfterCommit), so a rollback simply never runs it. Put
// such side effects here rather than gating them on the returned bool.
func WithOnCreate(fn func(ctx context.Context)) Option {
	return func(target any) error {
		if t, ok := target.(*createOptions); ok {
			t.onCreate = fn
		}
		return nil
	}
}

// WithResyncInterval sets the periodic resync interval for a controller. A
// value <= 0 disables periodic resync, leaving the controller event-driven
// only.
//
// The resync is what re-derives spec staleness (observed_generation < generation)
// after startup, so disabling it alongside StartupReconcileNone leaves no automatic
// driver for convergence at all — supported, but the embedder then owns recovery.
// See StartupReconcileNone for the recipe and the warning that configuration logs.
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

// WithEventRetention bounds the per-object event log, enforced globally by the GC
// sweeper on the startup + resync cadence. perObject > 0 caps each
// (object, category) timeline to its newest perObject runs — a ring, so a flapping
// timeline can't evict a quiet one; maxAge > 0 drops runs whose window ended more
// than maxAge ago. A zero bound is skipped, and both zero (the default) leaves the
// log unbounded. Retention is global, so it is meaningful only at New; passed
// elsewhere it is ignored.
func WithEventRetention(perObject int, maxAge time.Duration) Option {
	return func(target any) error {
		if t, ok := target.(*Beehive); ok {
			t.eventRetentionPerObject = perObject
			t.eventRetentionMaxAge = maxAge
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
