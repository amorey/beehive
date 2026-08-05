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
	"github.com/amorey/gobus/watch"
)

// This file is the event watch: one reader per watch, over its own cursor into
// one object's log, woken by the write's own commit.
// See docs/adr/2026-08-05-events-get-a-cursor-and-a-commit-wake.md.

const (
	defaultEventPageCap = 256
	// defaultEventPagesPerDrain bounds one drain, so a resume after a long gap
	// cannot monopolise the single connection. The rest is read by the next drain.
	defaultEventPagesPerDrain = 2
)

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
// event reader of that kind on every write.
type eventWriteHub struct {
	signalHub[ObjectID]
}

func newEventWriteHub() eventWriteHub { return eventWriteHub{newSignalHub[ObjectID]()} }

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

		pageCap:       defaultEventPageCap,
		pagesPerDrain: defaultEventPagesPerDrain,
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
	// pageCap and pagesPerDrain bound one drain; fields so a test can reach the
	// budget without staging a full page.
	pageCap       int
	pagesPerDrain int
}

// start makes the reads a caller learns about synchronously: the kind check, and
// then either the snapshot or the resume's own first look at the log.
func (r *eventReader) start(ctx context.Context) error {
	if err := r.checkKind(ctx); err != nil {
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

// checkKind scopes the read to this client's kind, as the object watches do, and
// sets resolved when it answers. An id that holds no object yet leaves it unset
// rather than failing — ordinary for a watch opened ahead of the thing it is
// about — where another kind's id is ErrNotFound for good, an id belonging to
// one kind for life.
func (r *eventReader) checkKind(ctx context.Context) error {
	raw, err := r.bh.store.ObjectsGetMeta(ctx, r.id)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if raw.Group != r.gk.Group || raw.Kind != r.gk.Kind {
		return fmt.Errorf("%w: %d belongs to another kind", ErrNotFound, r.id)
	}
	r.resolved = true
	return nil
}

// checkResume refuses a position the log cannot serve: below the horizon it has
// a hole under it, and above the head it did not come from this store —
// unreported, the second holds the cursor above every later run and drops them
// all silently.
func (r *eventReader) checkResume(ctx context.Context, at int64) error {
	// One row: this asks whether anything is above the position, and run reads
	// the page itself.
	runs, trimmed, err := r.bh.store.EventsListSince(ctx, r.id, r.cfg.query.Category, at, 1)
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
	return tooNewErr(fmt.Sprintf("object %d", r.id), at, max(head, trimmed))
}

// run tails the log until the caller lets go. A commit wakes it; the floor tick
// covers what a wake cannot.
func (r *eventReader) run(ctx context.Context) {
	// LIFO: the receiver leaves the hub, then the caller sees the stream end.
	defer close(r.out)
	defer r.written.Close()

	runWatchLoop(ctx, r.written.Chan(), r.floor,
		func() { r.stream.fail(ErrStopped) },
		func(backingOff bool) (time.Duration, bool, bool) {
			return r.pass(ctx, r.now(), backingOff)
		})
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
	// Minus the drain that just ran, since the gate opened before it.
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
		if err := r.checkKind(ctx); err != nil || !r.resolved {
			return false, err
		}
	}
	// max: a zero budget would read nothing and still report more.
	for range max(1, r.pagesPerDrain) {
		n, err := r.step(ctx)
		if err != nil {
			return false, err
		}
		if n < r.pageCap {
			return false, nil
		}
	}
	return true, nil
}

// step reads one page of the log above the cursor, delivers what the caller
// asked for, and returns the page length.
func (r *eventReader) step(ctx context.Context) (int, error) {
	page, trimmed, err := r.bh.store.EventsListSince(ctx, r.id, r.cfg.query.Category, r.cursor, r.pageCap)
	if err != nil {
		return 0, err
	}
	if err := horizonErr(r.gk, "the event stream", r.cursor, trimmed); err != nil {
		return 0, err
	}
	for _, raw := range page {
		if !r.cfg.query.Matches(raw) {
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
