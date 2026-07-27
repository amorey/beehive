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
	"fmt"
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

// ErrInvalidOption reports an option carrying a value that has no meaning — a
// non-positive GC interval, for instance. Unlike ErrConflictingOption it is about
// the argument alone, so it is returned regardless of the target the option lands
// on: a value that means nothing at one call site means nothing at any of them.
var ErrInvalidOption = errors.New("beehive: option value is invalid")

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

// LoadEvents selects the object's event-log runs, read via Object.Events().
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

// WithCatchupInterval sets how often a controller drains work the store has
// recorded as owed: objects whose spec has not converged
// (observed_generation < generation) and objects owed a durable dependency wake.
// A value <= 0 disables the catchup tick.
//
// It is separate from WithResyncInterval because the two scale differently. The
// owed set is bounded by what is actually outstanding — indexed listings that
// return nothing in a converged system — while a full pass scales with the object
// count. One interval governing both would mean tuning either moves the other.
//
// Passed to New it sets the default for all controllers; passed to Register it
// overrides that default for one.
func WithCatchupInterval(d time.Duration) Option {
	return func(target any) error {
		switch t := target.(type) {
		case *Beehive:
			t.catchupInterval = d
		case *reconciler:
			t.catchupInterval = d
		}
		return nil
	}
}

// WithResyncInterval sets how often a controller re-dispatches *every* object it
// owns, converged or not. The default is 0, which disables it.
//
// This is the expensive pass, and the only one that reaches an object nothing has
// recorded as owing work: process-scoped state a restart invalidated (liveness
// conditions read as "verifying" until this process rewrites them), and a
// dependency wake lost for a reason nothing observed. Both are invisible to
// WithCatchupInterval, whose listings are driven by columns.
//
// It is opt-in because its cost scales with the object count rather than with what
// is outstanding, and because the two cheaper drivers already cover convergence:
// the catchup tick drains recorded work, and the startup pass re-confirms
// everything once per process. Reach for this when the gap until the next restart
// is itself too long — then set it well above WithCatchupInterval, since a full
// pass subsumes the catchup set.
//
// Note for callers upgrading: this option previously paced the owed-work tick,
// which is now WithCatchupInterval. A call left unchanged still compiles and now
// buys a full pass at that cadence — likely more work than intended, and the
// catchup tick keeps running at its own default regardless.
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

// WithGCInterval sets how often the global GC sweeper runs: it collects
// deletion-pending objects (of every kind, including ones with no registered
// controller) and applies event-log retention.
//
// It is separate from the reconcile intervals on purpose. Removing dead rows and
// re-dispatching live ones are different jobs with different costs, and a single
// interval for both means tuning one moves the other. GC is also global rather
// than per-kind — the sweeper covers kinds no controller watches — so this is
// meaningful only at New; passed elsewhere it is ignored.
//
// Unlike the reconcile intervals, it cannot be disabled: d <= 0 is rejected with
// ErrInvalidOption. Those two knobs pace work that has another way through —
// Client.Requeue drives a reconcile by hand — but nothing on the public surface
// triggers collect, so a sweeper-less Beehive would let deletion-pending rows
// accumulate with no recourse, each one's owned_by edge RESTRICT-blocking its
// owner's own deletion. It is also the only cross-kind driver: a client-only kind
// has no reconcile loop to fall back on. A long interval expresses "collect
// rarely"; there is no supported way to express "never".
func WithGCInterval(d time.Duration) Option {
	return func(target any) error {
		// Checked before the target switch: the value is nonsense wherever it was
		// aimed, and reporting that only for the target that happens to consume it
		// would let a misdirected call carry the mistake silently.
		if d <= 0 {
			return fmt.Errorf("%w: WithGCInterval needs a positive interval, got %s", ErrInvalidOption, d)
		}
		if t, ok := target.(*Beehive); ok {
			t.gcInterval = d
		}
		return nil
	}
}

// WithEventRetention bounds the per-object event log, enforced globally by the GC
// sweeper on its own cadence (startup pass + WithGCInterval). perObject > 0 caps each
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

// WithStartupResync sets whether a controller re-dispatches *every* object once
// at startup, converged ones included. The default is true.
//
// The pass re-confirms process-scoped state that a restart invalidated — liveness
// conditions, for instance, read as "verifying" until a controller in this process
// rewrites them — which no owed-work listing can see, because nothing in the store
// records it as outstanding.
//
// It does not govern work that *is* recorded as owed. An object whose spec has not
// converged, and one owed a durable dependency wake, are resumed at startup either
// way: they are already owed a pass, and declining them is a correctness hole
// rather than a saving. In-progress deletions are likewise resumed, by the GC
// sweeper's own startup pass.
//
// Set it false for a large object set where the re-confirm is not worth its cost,
// or where the embedder drives its own reconciles. Passed to New it sets the
// default for all controllers; passed to Register it overrides that default for
// one.
func WithStartupResync(enabled bool) Option {
	return func(target any) error {
		switch t := target.(type) {
		case *Beehive:
			t.startupResync = enabled
		case *reconciler:
			t.startupResync = enabled
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
