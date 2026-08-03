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

// ErrInvalidOption reports an option value that has no meaning (e.g. a
// non-positive GC interval). It is about the argument alone, so it is returned
// regardless of the target — distinct from an option being inapplicable, which
// is ignored by design.
var ErrInvalidOption = errors.New("beehive: option value is invalid")

// LoadOption selects a secondary lookup to fetch alongside an object on a read
// (Get/GetByName/List). The lazy alternative: omit it and call
// Client.OwnersGet/DependenciesList when the data is needed.
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

// LoadEvents selects the object's event-log runs, read via Object.Events(). For
// filtered or bounded reads use the lazy Client.EventsList instead.
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

// RequeueOption configures a Client.Requeue call.
type RequeueOption func(*requeueOptions)

type requeueOptions struct {
	resetBackoff bool
}

// WithResetBackoff makes a Requeue clear the object's retry backoff ladder.
// Pass it only when the failure condition is known to be resolved; a plain
// Requeue preserves the ladder.
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

// EventOption filters a Client.EventsList / EventsWatch read.
type EventOption func(*storeapi.EventQuery)

// WithEventCategory restricts a read to a single timeline. The category "" is
// the default timeline (distinct from "no filter", the absence of this option).
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

// WithEventLimit caps a read to the newest n runs. On EventsWatch it bounds
// every poll, so the stream reports only runs inside that window.
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
type createOptions struct {
	finalizers []string
	owner      *ObjectID
	onCreate   func(context.Context)
}

// resolveCreate folds the create-time options into one createOptions, erroring
// on the first option that does so a bad option fails before any store work.
func resolveCreate(opts []Option) (*createOptions, error) {
	co := &createOptions{}
	for _, o := range opts {
		if err := o(co); err != nil {
			return nil, err
		}
	}
	return co, nil
}

// WithFinalizers attaches finalizers that must be cleared before an object is
// physically deleted.
//
// It requires a controller registered for the kind in this process, and the
// call is rejected with ErrInvalidOption otherwise: only
// ControllerClient.FinalizersDelete can clear a finalizer, so one no controller
// here can remove would leave the row deletion-pending forever, RESTRICT-
// blocking its owner's delete. The check is process-local and evaluated at call
// time — the store tracks no registrations — so register the kind first, from
// whichever process creates these rows.
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

// WithOnCreate registers fn to run once, on the caller's ctx, only if the call
// actually inserts a new row — and only after the outermost transaction
// commits (Store.AfterCommit), so a rollback never runs it. Use it for
// create-conditional side effects instead of GetOrCreate's returned bool, which
// inside a caller's Within reports true before the transaction commits.
func WithOnCreate(fn func(ctx context.Context)) Option {
	return func(target any) error {
		if t, ok := target.(*createOptions); ok {
			t.onCreate = fn
		}
		return nil
	}
}

// withOwedPassInterval sets how often a controller drains work the store
// records as owed (unconverged specs, owed dependency wakes); <= 0 disables the
// tick. Unexported: the owed pass is what makes convergence a guarantee, and
// its cost is bounded by what is outstanding. Only tests reach it; callers get
// WithFullPassInterval and Client.Requeue.
func withOwedPassInterval(d time.Duration) Option {
	return func(target any) error {
		switch t := target.(type) {
		case *Beehive:
			t.owedPassInterval = d
		case *reconciler:
			t.owedPassInterval = d
		}
		return nil
	}
}

// WithFullPassInterval sets how often a controller re-dispatches *every* object
// it owns, converged or not. Default 0 (disabled).
//
// This is the expensive pass, and the only one that reaches an object nothing
// has recorded as owing work — process-scoped state a restart invalidated, or a
// wake lost for a reason nothing observed. It is opt-in because its cost scales
// with the object count, and convergence is already covered by the owed pass
// and the startup pass. It does not pace the owed-work tick.
func WithFullPassInterval(d time.Duration) Option {
	return func(target any) error {
		switch t := target.(type) {
		case *Beehive:
			t.fullPassInterval = d
		case *reconciler:
			t.fullPassInterval = d
		}
		return nil
	}
}

// WithGCInterval sets how often the global GC sweeper runs: collecting
// deletion-pending objects of every kind, applying event-log retention, and
// releasing freed space. Meaningful only at New.
//
// Unlike WithFullPassInterval it cannot be disabled: d <= 0 is rejected with
// ErrInvalidOption. Nothing on the public surface triggers collect, so a
// sweeper-less Beehive would strand deletion-pending rows with no recourse. A
// long interval expresses "collect rarely"; there is no "never".
func WithGCInterval(d time.Duration) Option {
	return func(target any) error {
		// Checked before the target switch: the value is nonsense wherever it
		// was aimed.
		if d <= 0 {
			return fmt.Errorf("%w: WithGCInterval needs a positive interval, got %s", ErrInvalidOption, d)
		}
		if t, ok := target.(*Beehive); ok {
			t.gcInterval = d
		}
		return nil
	}
}

// withDependencyWakeInterval sets how often the dependency waker scans the
// write log. Global (a depends_on edge may point at a kind with no controller)
// and meaningful only at New. Unexported: it is the cheapest driver and already
// the shortest cadence, so there is nothing to tune it toward. d <= 0 disables
// it, costing only latency — the reconcile_owed stamp and the stale-dependents
// pass still cover correctness.
func withDependencyWakeInterval(d time.Duration) Option {
	return func(target any) error {
		if t, ok := target.(*Beehive); ok {
			t.wakeInterval = d
		}
		return nil
	}
}

// withStaleDependentsInterval sets how often the stale-dependents pass
// re-derives which dependents a dependency has moved under. Global and
// meaningful only at New. Unexported because it is the backstop that makes a
// dependency wake a guarantee — tuning it would be tuning how long a lost wake
// goes unnoticed. Like WithGCInterval it cannot be disabled: d <= 0 is rejected
// with ErrInvalidOption, because nothing else re-derives.
func withStaleDependentsInterval(d time.Duration) Option {
	return func(target any) error {
		if d <= 0 {
			return fmt.Errorf("%w: withStaleDependentsInterval needs a positive interval, got %s", ErrInvalidOption, d)
		}
		if t, ok := target.(*Beehive); ok {
			t.staleDependentsInterval = d
		}
		return nil
	}
}

// withWatchPollInterval sets how often EventsWatch polls. Global and meaningful
// only at New; the object watches subscribe to their kind's tail instead (see
// withWatchFloorInterval), and SchedulesWatch takes no tick at all. Unexported: watch latency and resolution are part of the streams'
// documented contract. It is both the latency a subscriber sees and the
// resolution — changes within one interval coalesce, and an object created and
// deleted inside one is never reported. Cannot be disabled: d <= 0 is rejected
// with ErrInvalidOption, since a watch that never polls is a stream that never
// delivers.
func withWatchPollInterval(d time.Duration) Option {
	return func(target any) error {
		if d <= 0 {
			return fmt.Errorf("%w: withWatchPollInterval needs a positive interval, got %s", ErrInvalidOption, d)
		}
		if t, ok := target.(*Beehive); ok {
			t.watchPollInterval = d
		}
		return nil
	}
}

// withWatchFloorInterval sets how often a kind's tailer reads the log without a
// wake. The wake carries freshness; this floor covers what a wake cannot — a
// writer this process does not share memory with, a step that failed, a
// retention trim. Global and meaningful only at New, unexported for the same
// reason as withWatchPollInterval. Cannot be disabled: d <= 0 is rejected with
// ErrInvalidOption.
func withWatchFloorInterval(d time.Duration) Option {
	return func(target any) error {
		if d <= 0 {
			return fmt.Errorf("%w: withWatchFloorInterval needs a positive interval, got %s", ErrInvalidOption, d)
		}
		if t, ok := target.(*Beehive); ok {
			t.watchFloorInterval = d
		}
		return nil
	}
}

// WithEventRetention bounds the per-object event log, enforced globally by the
// GC sweeper. perObject > 0 caps each (object, category) timeline to its newest
// perObject runs — per timeline, so a flapping one can't evict a quiet one;
// maxAge > 0 drops runs whose window ended more than maxAge ago. A zero bound
// is skipped; both zero (the default) leaves the log unbounded. Meaningful only
// at New.
func WithEventRetention(perObject int, maxAge time.Duration) Option {
	return func(target any) error {
		if t, ok := target.(*Beehive); ok {
			t.eventRetentionPerObject = perObject
			t.eventRetentionMaxAge = maxAge
		}
		return nil
	}
}

// WithWriteLogRetention bounds the object write log, enforced globally by the
// GC sweeper. perKind > 0 caps each (group, kind) log to its newest perKind
// entries — per kind, so a hot kind cannot evict a quiet one; maxAge > 0 drops
// entries written more than maxAge ago. A zero bound is skipped.
//
// The default is defaultWriteLogMaxAge and no count bound; both zero leaves the
// log unbounded, which also leaves every resume window unbounded. Retention is
// what defines that window: a stream cannot resume below what has been trimmed.
// Meaningful only at New.
func WithWriteLogRetention(perKind int, maxAge time.Duration) Option {
	return func(target any) error {
		if t, ok := target.(*Beehive); ok {
			t.writeLogRetentionPerKind = perKind
			t.writeLogRetentionMaxAge = maxAge
		}
		return nil
	}
}

// WithMigrator registers a Migrator for the controller's kind, applied to
// stored Spec/Status JSON on read. Meaningful only at Register, which installs
// it into the registry both the client and the reconciler decode through.
func WithMigrator(m Migrator) Option {
	return func(target any) error {
		if t, ok := target.(*reconciler); ok {
			t.migrator = m
		}
		return nil
	}
}

// WithStartupFullPass sets whether a controller re-dispatches *every* object
// once at startup, converged ones included. Default false.
//
// The pass re-confirms process-scoped state a restart invalidated — liveness
// conditions read as "verifying" until this process rewrites them — which no
// owed-work listing can see. Enable it for that, and that only: no reconcile
// may depend on it, since a pass that scales with the object count cannot be
// what guarantees convergence. Work genuinely owed is recorded and resumed at
// startup regardless.
//
// Passed to New it sets the default for all controllers; passed to Register it
// overrides that default for one.
func WithStartupFullPass(enabled bool) Option {
	return func(target any) error {
		switch t := target.(type) {
		case *Beehive:
			t.startupFullPass = enabled
		case *reconciler:
			t.startupFullPass = enabled
		}
		return nil
	}
}

// WithLogger routes beehive's internal logging through l (zap, zerolog, logrus
// and logr all ship slog bridges). A nil logger disables logging entirely,
// which is the default. Passed to New it sets the control plane's logger and
// the default for all controllers; passed to Register it overrides one.
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

// WithLogLevel sets the minimum level beehive emits, on top of whatever the
// logger's own handler filters. No effect without WithLogger. Passed to New it
// applies to the control plane and is the default for all controllers; passed
// to Register it overrides one.
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
// for a controller. A value <= 0 is ignored, keeping the default — a
// non-positive cap would busy-loop the reconciler on a persistent error.
func WithMaxRetryInterval(d time.Duration) Option {
	return func(target any) error {
		if t, ok := target.(*reconciler); ok && d > 0 {
			t.maxRetryInterval = d
		}
		return nil
	}
}

// WithConcurrency sets the number of concurrent worker goroutines for a
// controller; <= 1 means single-threaded (the default). Passed to New it is the
// default for all controllers; passed to Register it overrides one.
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
