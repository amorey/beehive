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
// Client.GetOwner/ListDependencies when the data is needed.
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
// filtered or bounded reads use the lazy Client.ListEvents instead.
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

// EventOption configures a Client.ListEvents / WatchEvents read.
type EventOption func(*eventConfig)

// eventConfig is what the event options fold into: the store query every read
// takes, plus the stream position only a watch reads.
type eventConfig struct {
	query storeapi.EventQuery
	// resumeFrom is the position to stream above, or nil to take a snapshot.
	resumeFrom *int64
}

// WithEventCategory restricts a read to a single timeline. The category "" is
// the default timeline (distinct from "no filter", the absence of this option).
func WithEventCategory(category string) EventOption {
	return func(c *eventConfig) { c.query.Category = &category }
}

// WithEventType restricts a read to one severity (Normal or Warning).
func WithEventType(t EventType) EventOption {
	return func(c *eventConfig) { c.query.Type = string(t) }
}

// WithEventReason restricts a read to runs with the given reason.
func WithEventReason(reason string) EventOption {
	return func(c *eventConfig) { c.query.Reason = reason }
}

// WithEventLimit caps a read to the newest n runs. On WatchEvents it bounds the
// snapshot only: a tail has no end to count back from.
func WithEventLimit(n int) EventOption {
	return func(c *eventConfig) { c.query.Limit = n }
}

// WithEventsSince restricts a read to runs still active at or after t (LastAt >= t).
func WithEventsSince(t time.Time) EventOption {
	return func(c *eventConfig) { c.query.Since = t }
}

// WithEventsResumeFrom streams the runs above rv instead of taking a snapshot.
// WatchEvents only — the other reads ignore it, the way an Option ignores a
// target it does not recognise. A position retention has passed ends the stream
// with ErrWatchTooOld, answered by subscribing again without this option.
func WithEventsResumeFrom(rv int64) EventOption {
	return func(c *eventConfig) { c.resumeFrom = &rv }
}

// resolveEvents folds the per-call options into one config.
func resolveEvents(opts []EventOption) eventConfig {
	var c eventConfig
	for _, o := range opts {
		o(&c)
	}
	return c
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
// ControllerClient.DeleteFinalizer can clear a finalizer, so one no controller
// here can remove would leave the row deletion-pending forever, RESTRICT-
// blocking its owner's delete. The check is process-local and evaluated at call
// time — the store tracks no registrations — so register the kind first.
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

// WithOwedPassInterval sets how often a controller drains work the store
// records as owed: unconverged specs and owed dependency wakes. Default 30s.
// Dispatches at New and at Register, so one kind can differ from the rest.
//
// Every trigger for a registered kind pushes at commit, so this is not the
// latency of a local write. What lengthening it costs is how long a *lost* push
// waits — a crash between the commit and the dispatch, a stamp made while no
// reconciler was registered for the source's kind — plus the first drain after
// a restart, which runs at startup regardless. See
// docs/adr/2026-08-06-driver-cadences-are-configurable.md.
//
// Cannot be disabled: d <= 0 is rejected with ErrInvalidOption. Unlike
// WithFullPassInterval, this pass is what makes convergence a guarantee rather
// than an optimisation, and its cost is bounded by what is outstanding rather
// than by the object count — so "rarely" is expressible and "never" is not.
func WithOwedPassInterval(d time.Duration) Option {
	return func(target any) error {
		// Checked before the target switch: the value is nonsense wherever it
		// was aimed.
		if d <= 0 {
			return fmt.Errorf("%w: WithOwedPassInterval needs a positive interval, got %s", ErrInvalidOption, d)
		}
		return withOwedPassInterval(d)(target)
	}
}

// withOwedPassInterval is WithOwedPassInterval without the floor, so a test can
// disable the tick outright and watch a push carry the work on its own.
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

// withMinRequeueInterval floors the gap between two dispatches of one object;
// <= 0 turns the floor off. Unexported: it exists to bound a dependency cycle,
// not to be tuned, and Client.Requeue is the supported way to beat a cadence.
func withMinRequeueInterval(d time.Duration) Option {
	return func(target any) error {
		switch t := target.(type) {
		case *Beehive:
			t.minRequeueInterval = d
		case *reconciler:
			t.work.setFloor(d)
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

// WithIndividualPassInterval gives every object of the kind a pass roughly
// every d, measured from the end of each object's own last pass. Default 0
// (disabled).
//
// It schedules a pass that returned settled without asking to be requeued;
// every other result keeps its own schedule, so d is a default cadence rather
// than a ceiling. Armings are jittered upward, and a scan at startup spreads
// the first pass of each object across d — pair it with WithStartupFullPass for
// a kind that needs that first pass promptly.
// See docs/adr/2026-08-19-an-individual-pass-interval.md.
//
// Passed to New it sets the default for all controllers; passed to Register it
// overrides that default for one.
func WithIndividualPassInterval(d time.Duration) Option {
	return func(target any) error {
		switch t := target.(type) {
		case *Beehive:
			t.individualPassInterval = d
		case *reconciler:
			t.individualPassInterval = d
		}
		return nil
	}
}

// WithTriggerByID requeues each id received on ch, as Client.Requeue would. An
// id naming nothing, or an object of another kind, is a no-op. Meaningful only
// at Register.
//
// A poke is a latency hint and nothing records it: unlike every push a write
// makes, no driver re-derives it, so correctness rests on the kind's own
// cadence. Beehive never closes ch, a closed ch stops that feed, and a channel
// serves one kind — sharing one across two Register calls races the receive.
// Repeated options accumulate, so a kind may declare several feeds. Retry
// backoff is preserved, as a plain Requeue preserves it.
func WithTriggerByID(ch <-chan ObjectID) Option {
	return func(target any) error {
		// Checked before the target switch: a nil channel blocks forever, so it
		// is nonsense wherever it was aimed.
		if ch == nil {
			return fmt.Errorf("%w: WithTriggerByID needs a non-nil channel", ErrInvalidOption)
		}
		if t, ok := target.(*reconciler); ok {
			t.triggers = append(t.triggers, &trigger{r: t, ids: ch})
		}
		return nil
	}
}

// WithTriggerByName requeues the object holding each name received on ch, as
// Client.Requeue would. A name matching nothing — "" included — is a no-op,
// since whether a record exists for an address is the app's business and
// changes under it. Meaningful only at Register.
//
// Carries the same contract as WithTriggerByID.
func WithTriggerByName(ch <-chan string) Option {
	return func(target any) error {
		if ch == nil {
			return fmt.Errorf("%w: WithTriggerByName needs a non-nil channel", ErrInvalidOption)
		}
		if t, ok := target.(*reconciler); ok {
			t.triggers = append(t.triggers, &trigger{r: t, names: ch})
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
//
// A deletion cascade over *registered* kinds advances a level per commit, so
// this is not its latency. A client-only level has no push at all and costs one
// interval, so a subtree of client-only kinds takes one interval per level.
// The sweeper's per-sweep work budgets scale with d, so a longer interval trims
// and reclaims proportionally more rather than at a lower rate. See
// docs/adr/2026-08-06-driver-cadences-are-configurable.md.
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

// withDependencyWakerOff turns the dependency waker off. Global and meaningful
// only at New. Unexported: it costs only latency — the reconcile_owed stamp and
// the stale-dependents pass still cover correctness — but nothing else replaces
// the waker's cadence.
func withDependencyWakerOff() Option {
	return func(target any) error {
		if t, ok := target.(*Beehive); ok {
			t.wakerOff = true
		}
		return nil
	}
}

// withWakeScanMinInterval floors the gap between two wake-driven scans of the
// write log; <= 0 turns the floor off. Global and meaningful only at New.
// Unexported: it trades dependency-wake latency against how much of the single
// connection the waker holds under a sustained write stream, which is a
// measurement rather than a preference.
func withWakeScanMinInterval(d time.Duration) Option {
	return func(target any) error {
		if t, ok := target.(*Beehive); ok {
			t.wakeScanMinInterval = d
		}
		return nil
	}
}

// WithStaleDependentsInterval sets how often the stale-dependents pass
// re-derives which dependents a dependency has moved under. Default 60s. Global
// and meaningful only at New.
//
// The dependency waker propagates a target change per commit, so this is not
// dependency latency. What lengthening it costs is how long a dependent stays
// stale when that wake was *lost* — a crash before the scan, a failed seed, a
// write no waker cursor covers — and it is the only thing that re-derives, so a
// long value is a long window. It also paces the waker's abandon jump, which
// hands a range to this pass on the argument that it has already swept it.
//
// Cannot be disabled: d <= 0 is rejected with ErrInvalidOption.
func WithStaleDependentsInterval(d time.Duration) Option {
	return func(target any) error {
		if d <= 0 {
			return fmt.Errorf("%w: WithStaleDependentsInterval needs a positive interval, got %s", ErrInvalidOption, d)
		}
		if t, ok := target.(*Beehive); ok {
			t.staleDependentsInterval = d
		}
		return nil
	}
}

// withWatchScanMinInterval floors the gap between two wake-driven drains of a
// kind's write log; <= 0 turns the floor off. Global and meaningful only at New.
// Unexported: it trades watch latency against how much of the single connection
// a tailer holds under a sustained write stream, which is a measurement rather
// than a preference.
func withWatchScanMinInterval(d time.Duration) Option {
	return func(target any) error {
		if t, ok := target.(*Beehive); ok {
			t.watchScanMinInterval = d
		}
		return nil
	}
}

// WithWatchFloorInterval sets how often a watch reads without a wake — a kind's
// tailer, and an object's event reader. Default 30s. Global and meaningful only
// at New.
//
// Both watch families read on a commit wake, so this is not delivery latency
// for anything this Beehive writes. What lengthening it costs is staleness for
// what a wake cannot cover: a retention trim, a step that failed after its
// retry ladder gave up, and a write by a second writer over the same store —
// which is an unsupported deployment, not a slow one. A failed read still
// retries on its own ladder, capped in seconds, whatever this is set to.
//
// Cannot be disabled: d <= 0 is rejected with ErrInvalidOption.
func WithWatchFloorInterval(d time.Duration) Option {
	return func(target any) error {
		if d <= 0 {
			return fmt.Errorf("%w: WithWatchFloorInterval needs a positive interval, got %s", ErrInvalidOption, d)
		}
		if t, ok := target.(*Beehive); ok {
			t.watchFloorInterval = d
		}
		return nil
	}
}

// WithEventRetention bounds the event log, enforced globally by the GC sweeper.
// perTimeline > 0 caps each (object, category) timeline to its newest
// perTimeline runs — runs, not occurrences, since an extend grows a run in
// place; maxAge > 0 drops runs whose window ended more than maxAge ago, across
// every timeline. A zero bound is skipped; both zero (the default) leaves the
// log unbounded. The sweeper enforces the cap on its own interval, so a burst
// can sit above it until the next sweep. Meaningful only at New.
func WithEventRetention(perTimeline int, maxAge time.Duration) Option {
	return func(target any) error {
		if t, ok := target.(*Beehive); ok {
			t.eventRetentionPerTimeline = perTimeline
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
// Enable it for a kind whose reconcile establishes in-process state — a live
// connection, a running worker, a liveness condition — which a restart
// invalidated and no owed-work listing can see: the store reads settled,
// because observed_generation was written by a process that is gone.
//
// Unlike the periodic full pass, a kind may depend on this one. It costs
// O(objects) once per process, and for a controller whose reconcile is what
// opens the connection or starts the worker it is the only thing that
// reconverges the object after a restart. What it guarantees: every object of
// a kind that enables it is reconciled at least once per process.
//
// Passed to New it sets the default for all controllers; passed to Register it
// overrides that default for one — prefer Register, so the declaration names
// the kinds that actually own in-process state.
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
//
// The cap bounds the retry rate outright: a wake arriving while the object sits
// on its backoff alarm is absorbed by that alarm rather than dispatched.
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
