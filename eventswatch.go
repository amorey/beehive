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
	"sync/atomic"
	"time"

	"github.com/amorey/beehive/internal/driver"
	"github.com/amorey/beehive/internal/rategate"
	"github.com/amorey/beehive/internal/storeapi"
	"github.com/amorey/gobus/watch"
)

// This file is the event watch: one reader per watch, over its own cursor into
// one object's log. An EventsAdd commit wakes it, and the floor tick covers a
// write this process did not publish. Nothing is shared, so none of the object
// tail's lease machinery appears here — the read is already per object and
// already indexed.

// eventPageCap caps one read of the log: one object's runs, against the object
// tail's 512 write-log entries.
const eventPageCap = 256

// eventPagesPerDrain bounds one drain, so a resume after a long gap cannot
// monopolise the single connection. The rest is read by the next drain.
const eventPagesPerDrain = 2

// EventStream is a live view of one object's event log: the runs matching the
// query as of the subscribe, the position they were read at, and what the log
// grows by after it.
type EventStream struct {
	// Runs is the snapshot, newest-first like EventsList. Empty on a resume.
	Runs []Event
	// ResourceVersion is the position Runs was read at, and the value to hand
	// back to WithEventsResumeFrom.
	ResourceVersion int64
	// Events delivers the runs above ResourceVersion, oldest-first, until ctx
	// ends or the stream fails. Closed exactly once.
	Events <-chan Event

	failed atomic.Pointer[error]
}

// Err reports why the stream ended, after Events is closed: ErrWatchTooOld,
// ErrNotFound for a collected object, ErrStopped, or nil when the caller's own
// context ended.
func (s *EventStream) Err() error {
	if err := s.failed.Load(); err != nil {
		return *err
	}
	return nil
}

func (s *EventStream) fail(err error) { s.failed.Store(&err) }

// eventWriteHub tells an object's event readers that its log moved. Keyed by id,
// not by kind: the read is per object, so a kind-wide signal would wake every
// event reader of that kind on every write. The signal carries no value — a
// reader reads its position from the store — so a burst collapses into one
// pending slot, as kindWriteHub's does.
type eventWriteHub struct {
	watchHub[ObjectID, struct{}]
}

func newEventWriteHub() eventWriteHub {
	return eventWriteHub{watchHub[ObjectID, struct{}]{hub: watch.New[ObjectID, struct{}]()}}
}

func (h eventWriteHub) Send(id ObjectID) error { return h.send(id, struct{}{}) }

// Watch registers a receiver for id. Registration is the baseline and the bus
// never delivers it back, so a receiver reads only writes that follow it.
func (h eventWriteHub) Watch(id ObjectID) (*watch.Receiver[ObjectID, struct{}], bool) {
	return h.watch(id, struct{}{})
}

// EventsWatch streams id's event log. See the Client interface for the contract.
func (c *clientImpl[Spec, Status]) EventsWatch(ctx context.Context, id ObjectID, opts ...EventOption) (*EventStream, error) {
	if !c.bh.isRegistered(c.gk) {
		return nil, fmt.Errorf("beehive: no controller registered for %s/%s", c.gk.Group, c.gk.Kind)
	}
	// Registered BEFORE any read: a write landing between the two would
	// otherwise be missed by both.
	written, ok := c.bh.eventWriteHub.Watch(id)
	if !ok {
		return nil, errors.New("beehive: a watch needs a Beehive built by New")
	}
	r := &eventReader{
		bh:      c.bh,
		gk:      c.gk,
		id:      id,
		cfg:     resolveEvents(opts),
		written: written,
		out:     make(chan Event),
		stream:  &EventStream{},
		gate:    rategate.NewSingle(c.bh.watchScanMinInterval),
		retry:   c.bh.watchBackoff(),
		floor:   c.bh.watchFloor(),
		now:     time.Now,
	}
	if err := r.start(ctx); err != nil {
		written.Close() // owed by every path that returns without a stream
		return nil, fmt.Errorf("beehive: event watch on %s/%s: %w", c.gk.Group, c.gk.Kind, err)
	}
	go r.run(ctx)
	return r.stream, nil
}

// eventReader tails one object's log for one watch. Every field below the
// stream is run's alone once it starts.
type eventReader struct {
	bh      *Beehive
	gk      GroupKind
	id      ObjectID
	cfg     eventConfig
	written *watch.Receiver[ObjectID, struct{}]
	out     chan Event
	stream  *EventStream

	cursor int64
	// resolved latches the kind check: group and kind are fixed at insert and
	// ids are never reused, so once the id resolves the answer cannot change.
	// Only "not there yet" stays unresolved — the id can still be created later.
	resolved bool
	gate     *rategate.Single
	retry    driver.Backoff
	floor    time.Duration
	now      func() time.Time
}

// start makes the reads a caller learns about synchronously: the kind check, and
// then either the snapshot or the resume's own first look at the log.
func (r *eventReader) start(ctx context.Context) error {
	if _, err := r.checkKind(ctx); err != nil {
		return err
	}
	if r.cfg.resumeFrom == nil {
		runs, at, err := r.bh.store.EventsSnapshot(ctx, r.id, r.cfg.query)
		if err != nil {
			return err
		}
		r.stream.Runs, r.stream.ResourceVersion = eventsFromRaw(runs), at
	} else {
		at := *r.cfg.resumeFrom
		if err := r.checkResume(ctx, at); err != nil {
			return err
		}
		r.stream.ResourceVersion = at
	}
	r.cursor = r.stream.ResourceVersion
	r.stream.Events = r.out
	return nil
}

// checkKind scopes the read to this client's kind, as the object watches do. An
// id that holds no object yet is ordinary for a watch opened ahead of the thing
// it is about, and reports notYet; another kind's id is ErrNotFound for good,
// since an id belongs to one kind for life.
func (r *eventReader) checkKind(ctx context.Context) (notYet bool, err error) {
	raw, err := r.bh.store.ObjectsGetMeta(ctx, r.id)
	if errors.Is(err, ErrNotFound) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if raw.Group != r.gk.Group || raw.Kind != r.gk.Kind {
		return false, fmt.Errorf("%w: %d belongs to another kind", ErrNotFound, r.id)
	}
	r.resolved = true
	return false, nil
}

// checkResume refuses a position the log cannot serve: below the horizon it has
// a hole under it, and above the head it did not come from this store —
// unreported, the second holds the cursor above every later run and drops them
// all silently.
func (r *eventReader) checkResume(ctx context.Context, at int64) error {
	runs, trimmed, err := r.bh.store.EventsListSince(ctx, r.id, r.cfg.query.Category, at, eventPageCap)
	if err != nil {
		return err
	}
	if err := horizonErr(r.gk, "the event resume", at, trimmed); err != nil {
		return err
	}
	if len(runs) > 0 {
		return nil
	}
	// Only a resume at or beyond the head gets this far, so no resume with real
	// work to do pays for this read. The horizon is folded in because retention
	// on the newest run lowers the mark.
	head, err := r.bh.store.EventsMaxVersion(ctx, r.id)
	if err != nil {
		return err
	}
	if head = max(head, trimmed); at > head {
		return fmt.Errorf("%w: object %d resumed at %d, past its log's %d",
			ErrWatchTooNew, r.id, at, head)
	}
	return nil
}

// run tails the log until the caller lets go. A commit wakes it; the floor tick
// covers what a wake cannot.
func (r *eventReader) run(ctx context.Context) {
	defer close(r.out)
	defer r.written.Close()

	timer := time.NewTimer(r.floor)
	defer timer.Stop()

	// backingOff drops commit wakes until the retry timer fires, for the reason
	// objectTailer.run records: a wake carries no information, and honouring one
	// would let a live writer keep a degraded store re-reading as fast as it can
	// fail.
	backingOff := false
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-r.written.Chan():
			if !ok {
				// Only Stop closes the hub — the caller letting go comes through ctx
				// above — so this is shutdown, and saying so is what keeps a closed
				// stream distinguishable from a cancelled one.
				r.stream.fail(ErrStopped)
				return
			}
			if backingOff {
				continue
			}
		case <-timer.C:
		}

		next, stillBackingOff, done := r.pass(ctx, r.now(), backingOff)
		if done {
			return
		}
		backingOff = stillBackingOff
		driver.Rearm(timer, next)
	}
}

// pass is one turn of the loop: drain, then report how long to wait, whether to
// drop the wakes arriving meanwhile, and whether the stream is finished.
func (r *eventReader) pass(ctx context.Context, now time.Time, backingOff bool) (next time.Duration, stillBackingOff, done bool) {
	// A refused wake is remembered by the re-arm: the drain that runs then reads
	// its position from the store.
	if opensAt, held := r.gate.Allow(now); held {
		return opensAt.Sub(now), backingOff, false
	}

	more, err := r.drain(ctx)
	switch {
	case err == nil:
	case ctx.Err() != nil:
		return 0, false, true // the caller let go mid-read or mid-send
	case isTerminalWatchErr(err):
		// Something no later read can take back: retention passed the cursor, or
		// the object was collected and its log went with it.
		r.stream.fail(err)
		return 0, false, true
	default:
		r.bh.log().Warn("event watch step failed; retrying", "id", r.id, "err", err)
		return r.retry.Next(), true, false
	}

	r.retry.Reset()
	if !more {
		return r.floor, false, false
	}
	// Minus the drain that just ran, since the gate opened before it: re-arming
	// for a whole interval would pace a backlog end-to-start.
	return max(0, r.gate.Interval()-r.now().Sub(now)), false, false
}

// isTerminalWatchErr reports whether an error ends a stream rather than costing
// it a retry.
func isTerminalWatchErr(err error) bool {
	return errors.Is(err, ErrWatchTooOld) || errors.Is(err, ErrNotFound)
}

// drain reads pages until one comes back short or the budget is spent, so a
// burst that collapsed into one wake is read in full but a backlog is not. The
// bool reports whether the budget stopped it with work still above the cursor.
func (r *eventReader) drain(ctx context.Context) (more bool, err error) {
	if !r.resolved {
		notYet, err := r.checkKind(ctx)
		if err != nil || notYet {
			return false, err
		}
	}
	for range eventPagesPerDrain {
		n, err := r.step(ctx)
		if err != nil {
			return false, err
		}
		if n < eventPageCap {
			return false, nil
		}
	}
	return true, nil
}

// step reads one page of the log above the cursor, delivers what the caller
// asked for, and returns the page length.
func (r *eventReader) step(ctx context.Context) (int, error) {
	page, trimmed, err := r.bh.store.EventsListSince(ctx, r.id, r.cfg.query.Category, r.cursor, eventPageCap)
	if err != nil {
		return 0, err
	}
	if err := horizonErr(r.gk, "the event stream", r.cursor, trimmed); err != nil {
		return 0, err
	}
	for _, raw := range page {
		if !matchesEventQuery(r.cfg.query, raw) {
			continue
		}
		if !sendOrDone(ctx, r.out, eventFromRaw(raw)) {
			return 0, ctx.Err()
		}
	}
	// Moved only past a delivered page: a failed read or an abandoned send
	// leaves the runs owed.
	if len(page) > 0 {
		r.cursor = page[len(page)-1].ResourceVersion
	}
	return len(page), nil
}

// matchesEventQuery applies the caller's filters to a run the page carried. The
// page itself is unfiltered, so the cursor advances by what the log did rather
// than by what this watch asked for.
func matchesEventQuery(q storeapi.EventQuery, e RawEvent) bool {
	switch {
	case q.Category != nil && e.Category != *q.Category:
		return false
	case q.Type != "" && e.Type != q.Type:
		return false
	case q.Reason != "" && e.Reason != q.Reason:
		return false
	case !q.Since.IsZero() && e.LastAt.Before(q.Since):
		return false
	}
	return true
}
