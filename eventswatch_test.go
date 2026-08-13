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
	"log/slog"
	"testing"
	"time"

	"github.com/amorey/beehive/internal/rategate"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The snapshot is the runs as of a position, and the stream starts exactly
// above it: an event committed while the subscribe is in flight is delivered
// once, on one side or the other, never twice and never neither.
func TestWatchEventsSnapshotCarriesItsPosition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Probing"}))

	stream, err := client.WatchEvents(ctx, obj.ID)
	require.NoError(t, err)
	require.Len(t, stream.Runs, 1, "the runs already in the log come back as a snapshot")
	assert.Equal(t, "Probing", stream.Runs[0].Reason)
	assert.Equal(t, stream.Runs[0].ResourceVersion, stream.ResourceVersion)

	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Connected"}))
	got := recv(t, stream.Events)
	assert.Equal(t, "Connected", got.Reason, "only what the snapshot did not hold")
	assert.Greater(t, got.ResourceVersion, stream.ResourceVersion)
}

// An extend is not a new run: the same row comes back with a fresh version and a
// bumped count, which is what lets a caller update it in place.
func TestWatchEventsRedeliversAnExtendedRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	stream, err := client.WatchEvents(ctx, obj.ID)
	require.NoError(t, err)
	require.Empty(t, stream.Runs, "an empty log snapshots empty")

	probe := EventSpec{Type: EventWarning, Reason: "ProbeFailed"}
	require.NoError(t, cc.AddEvent(ctx, obj.ID, probe))
	first := recv(t, stream.Events)
	assert.Equal(t, 1, first.Count)

	require.NoError(t, cc.AddEvent(ctx, obj.ID, probe))
	second := recv(t, stream.Events)
	assert.Equal(t, first.ID, second.ID, "the same run, not a new one")
	assert.Equal(t, 2, second.Count)
	assert.Greater(t, second.ResourceVersion, first.ResourceVersion)
}

// Runs are delivered oldest-first, and that is a contract rather than tidiness:
// a caller checkpoints a delivered version and resumes above it, so a version
// delivered after a higher one would be skipped for good.
func TestWatchEventsDeliversAscending(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	stream, err := client.WatchEvents(ctx, obj.ID)
	require.NoError(t, err)

	for _, reason := range []string{"R1", "R2", "R3"} {
		require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: reason}))
	}
	var last int64
	for _, want := range []string{"R1", "R2", "R3"} {
		got := recv(t, stream.Events)
		assert.Equal(t, want, got.Reason)
		assert.Greater(t, got.ResourceVersion, last, "ascending by resource version")
		last = got.ResourceVersion
	}
}

// Filters apply to the tail as well as to the snapshot, and the cursor still
// advances by what the log did — so a run in a filtered-out category costs
// nothing and hides nothing.
func TestWatchEventsFiltersTheTail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	stream, err := client.WatchEvents(ctx, obj.ID, WithEventCategory("probe"))
	require.NoError(t, err)

	for range 3 {
		require.NoError(t, cc.AddEvent(ctx, obj.ID,
			EventSpec{Category: "other", Type: EventNormal, Reason: "Noise"}))
	}
	require.NoError(t, cc.AddEvent(ctx, obj.ID,
		EventSpec{Category: "probe", Type: EventNormal, Reason: "Connected"}))

	got := recv(t, stream.Events)
	assert.Equal(t, "Connected", got.Reason, "another timeline's runs are dropped, not delivered")
}

// WithEventLimit bounds the snapshot only: a tail has no end to count back from.
func TestWatchEventsLimitBoundsTheSnapshotOnly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	for _, reason := range []string{"R1", "R2", "R3"} {
		require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: reason}))
	}

	stream, err := client.WatchEvents(ctx, obj.ID, WithEventLimit(1))
	require.NoError(t, err)
	require.Len(t, stream.Runs, 1, "the newest run only")

	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "R4"}))
	assert.Equal(t, "R4", recv(t, stream.Events).Reason, "the tail is not capped")
}

// A resume streams the gap and nothing else, so a caller reconnecting from a
// checkpoint pays for what it missed rather than for the whole log.
func TestWatchEventsResumesFromAPosition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Seen"}))

	first, err := client.WatchEvents(ctx, obj.ID)
	require.NoError(t, err)
	checkpoint := first.ResourceVersion

	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Missed"}))

	resumed, err := client.WatchEvents(ctx, obj.ID, WithEventsResumeFrom(checkpoint))
	require.NoError(t, err)
	assert.Empty(t, resumed.Runs, "a resume takes no snapshot")
	assert.Equal(t, checkpoint, resumed.ResourceVersion, "the position is carried back")
	assert.Equal(t, "Missed", recv(t, resumed.Events).Reason, "the gap, and only the gap")
}

// A resume above everything the log has held did not come from this store.
// Unreported it would hold the cursor above every later run and drop them all.
func TestWatchEventsResumeAboveTheHeadIsRefused(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Probing"}))

	_, err := client.WatchEvents(ctx, obj.ID, WithEventsResumeFrom(1<<40))
	assert.ErrorIs(t, err, ErrWatchTooNew)
}

// Retention takes runs a resuming caller never saw, and the horizon is what
// turns that from an empty answer into a refusal.
func TestWatchEventsResumeBelowTheHorizonIsRefused(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	// Checkpointed over an empty log, so the run trimmed below is one this
	// caller never saw — the gap the horizon exists to report.
	stream, err := client.WatchEvents(ctx, obj.ID)
	require.NoError(t, err)
	checkpoint := stream.ResourceVersion

	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Old"}))
	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Newer"}))
	_, err = store.Events().Sweep(ctx, 1, 0, 0) // "Old" goes, unread
	require.NoError(t, err)

	_, err = client.WatchEvents(ctx, obj.ID, WithEventsResumeFrom(checkpoint))
	assert.ErrorIs(t, err, ErrWatchTooOld)
}

// Type, reason and time filter client-side, so the horizon cannot account for
// them: a resume is refused for a trim of runs the filter would have dropped.
// Over-reporting is the safe direction — the alternative is vouching for an
// absence the store never checked.
func TestWatchEventsHorizonIgnoresClientSideFilters(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	stream, err := client.WatchEvents(ctx, obj.ID, WithEventReason("Wanted"))
	require.NoError(t, err)
	checkpoint := stream.ResourceVersion

	for _, reason := range []string{"Unwanted", "Newer"} {
		require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: reason}))
	}
	_, err = store.Events().Sweep(ctx, 1, 0, 0) // "Unwanted" goes — a run this reader filtered out
	require.NoError(t, err)

	_, err = client.WatchEvents(ctx, obj.ID, WithEventReason("Wanted"), WithEventsResumeFrom(checkpoint))
	assert.ErrorIs(t, err, ErrWatchTooOld)
}

// A trim on one timeline says nothing about another: the horizon is keyed the
// way the ring cap partitions, so a chatty category cannot refuse a quiet one.
func TestWatchEventsHorizonIsPerTimeline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Category: "quiet", Type: EventNormal, Reason: "Q1"}))

	quiet, err := client.WatchEvents(ctx, obj.ID, WithEventCategory("quiet"))
	require.NoError(t, err)
	checkpoint := quiet.ResourceVersion

	for _, reason := range []string{"C1", "C2"} {
		require.NoError(t, cc.AddEvent(ctx, obj.ID,
			EventSpec{Category: "chatty", Type: EventNormal, Reason: reason}))
	}
	_, err = store.Events().Sweep(ctx, 1, 0, 0) // trims chatty's older run, quiet keeps its one
	require.NoError(t, err)

	_, err = client.WatchEvents(ctx, obj.ID, WithEventCategory("quiet"), WithEventsResumeFrom(checkpoint))
	assert.NoError(t, err, "a trim on another timeline must not refuse this one")

	_, err = client.WatchEvents(ctx, obj.ID, WithEventsResumeFrom(checkpoint))
	assert.ErrorIs(t, err, ErrWatchTooOld, "an unfiltered resume is refused for a trim anywhere")
}

// An event watch opened before its object exists waits for it rather than
// erroring: the existence probe needs a row to read, and "not there yet" is
// ordinary for a watch opened ahead of the thing it is about.
func TestWatchEventsWaitsForAnObjectThatDoesNotExistYet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, cc := watchFixture(t)
	first := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	// The id the store will assign next: no row holds it yet, so the existence
	// probe finds nothing on every pass.
	next := first.ID + 1
	stream, err := client.WatchEvents(ctx, next)
	require.NoError(t, err)
	waitClosed(t, chanAfter(store.metaRead, 2), "passes while the id is unassigned")

	later := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "b"})
	require.Equal(t, next, later.ID, "the store assigns ids in order")
	require.NoError(t, cc.AddEvent(ctx, later.ID, EventSpec{Type: EventNormal, Reason: "Started"}))

	assert.Equal(t, "Started", recv(t, stream.Events).Reason, "the stream picks the object up once it exists")
}

// An event watch reads by id, not by kind: events carry no kind of their own, so
// the client's kind says who is asking, not whose log this is.
func TestWatchEventsIsNotKindScoped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, bh, client, _ := watchFixture(t)
	foreign, otherCC := foreignObject(t, ctx, bh)
	require.NoError(t, otherCC.AddEvent(ctx, foreign, EventSpec{Type: EventNormal, Reason: "Started"}))

	stream, err := client.WatchEvents(ctx, foreign)
	require.NoError(t, err)
	require.Len(t, stream.Runs, 1, "the snapshot carries the foreign object's log")
	assert.Equal(t, "Started", stream.Runs[0].Reason)

	require.NoError(t, otherCC.AddEvent(ctx, foreign, EventSpec{Type: EventNormal, Reason: "Probing"}))
	assert.Equal(t, "Probing", recv(t, stream.Events).Reason, "and what it grows by after")
}

// The three reads answer for the same id. WatchEvents used to be the odd one,
// which is the disagreement this pins.
func TestTheEventReadsAgreeOnAForeignID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, bh, client, _ := watchFixture(t)
	foreign, otherCC := foreignObject(t, ctx, bh)
	require.NoError(t, otherCC.AddEvent(ctx, foreign, EventSpec{Type: EventNormal, Reason: "Started"}))

	runs, err := client.ListEvents(ctx, foreign)
	require.NoError(t, err)
	latest, ok, err := client.GetLatestEvent(ctx, foreign, "")
	require.NoError(t, err)
	require.True(t, ok)
	stream, err := client.WatchEvents(ctx, foreign)
	require.NoError(t, err)

	assert.Equal(t, runs, stream.Runs, "the watch snapshot is the list")
	assert.Equal(t, runs[0], latest, "and the latest is its newest run")
}

// An id with no row yet is probed on every pass, and the kind that later claims
// it is not the watch's business: the stream resolves whoever it turns out to be.
func TestWatchEventsResolvesAnIDCreatedAsAnotherKind(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, bh, client, _ := watchFixture(t)
	first := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	next := first.ID + 1
	stream, err := client.WatchEvents(ctx, next)
	require.NoError(t, err)
	waitClosed(t, chanAfter(store.metaRead, 2), "passes while the id is unassigned")

	foreign, otherCC := foreignObject(t, ctx, bh)
	require.Equal(t, next, foreign, "the store assigns ids in order")
	require.NoError(t, otherCC.AddEvent(ctx, foreign, EventSpec{Type: EventNormal, Reason: "Started"}))

	assert.Equal(t, "Started", recv(t, stream.Events).Reason, "the stream resolves rather than ending")
}

// The collected-object ending is about the object, not the kind.
func TestWatchEventsEndsWhenAForeignObjectIsCollected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, bh, client, _ := watchFixture(t)
	foreign, otherCC := foreignObject(t, ctx, bh)
	require.NoError(t, otherCC.AddEvent(ctx, foreign, EventSpec{Type: EventNormal, Reason: "Probing"}))

	stream, err := client.WatchEvents(ctx, foreign)
	require.NoError(t, err)

	require.NoError(t, store.Objects().Delete(ctx, foreign))
	waitClosed(t, closedWhenDrained(stream.Events), "the stream to end with its object")
	assert.ErrorIs(t, stream.Err(), ErrNotFound)
}

// A collected object's log cascaded away with it, so an empty page there is not
// "no events" — the stream ends and says which.
func TestWatchEventsEndsWhenTheObjectIsCollected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Probing"}))

	stream, err := client.WatchEvents(ctx, obj.ID)
	require.NoError(t, err)

	require.NoError(t, store.Objects().Delete(ctx, obj.ID))
	waitClosed(t, closedWhenDrained(stream.Events), "the stream to end with its object")
	assert.ErrorIs(t, stream.Err(), ErrNotFound)
}

// Every read the reader makes can fail, and any one of them costs a retry rather
// than the stream — the contract the object watches keep.
func TestWatchEventsSurvivesReadFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, bh, client, cc := watchFixture(t)
	logger, buf := captureLogger(slog.LevelWarn)
	bh.logger = logger

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	stream, err := client.WatchEvents(ctx, obj.ID)
	require.NoError(t, err)

	store.eventsErr.Store(true)
	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Probing"}))
	waitClosed(t, chanAfter(store.eventsFailed, 2), "passes while the log read fails")
	store.eventsErr.Store(false)

	// A failed page does not advance the cursor, so the run it missed is still
	// owed: both arrive, oldest first.
	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Recovered"}))
	assert.Equal(t, "Probing", recv(t, stream.Events).Reason, "the run the failed read missed is not lost")
	assert.Equal(t, "Recovered", recv(t, stream.Events).Reason, "the stream outlived the failure")
	assert.Contains(t, buf.String(), "event watch step failed", "the retried passes are reported")
}

// A send is abandoned on cancellation like any other, so a subscriber that stops
// reading cannot strand the goroutine reading on its behalf.
func TestWatchEventsAbandonsASendOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store, _, client, cc := watchFixture(t)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	stream, err := client.WatchEvents(ctx, obj.ID)
	require.NoError(t, err)

	// Nobody reads the stream. Waiting for the log read to return puts the
	// cancellation after it and before the send, the only place left to observe it.
	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Probing"}))
	waitClosed(t, chanAfter(store.eventsListed, 1), "the log read that found the run")
	cancel()

	waitClosed(t, closedWhenDrained(stream.Events), "the stream to close on cancellation")
	assert.NoError(t, stream.Err(), "a caller's own cancellation is not a failure")
}

// Stopping the beehive ends every event stream, and says so: a subscriber has to
// be able to tell shutdown from its own cancellation.
func TestWatchEventsEndsOnStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, bh, client, _ := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	stream, err := client.WatchEvents(ctx, obj.ID)
	require.NoError(t, err)

	require.NoError(t, bh.stop(context.Background()))
	waitClosed(t, closedWhenDrained(stream.Events), "the stream to end with the beehive")
	assert.ErrorIs(t, stream.Err(), ErrStopped)
}

// A commit delivers on its own wake, with the floor tick set far beyond the
// test timeout: what arrives could not have come from a tick.
func TestWatchEventsDeliversWithoutTicking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t), WithWatchFloorInterval(time.Hour))
	cc, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	stream, err := client.WatchEvents(ctx, obj.ID)
	require.NoError(t, err)

	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Probing"}))
	assert.Equal(t, "Probing", recv(t, stream.Events).Reason)
}

// Two streams on one object are independent: each holds its own cursor, so one
// resuming from its own checkpoint tells the other nothing.
func TestWatchEventsStreamsAreIndependent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	all, err := client.WatchEvents(ctx, obj.ID)
	require.NoError(t, err)
	probes, err := client.WatchEvents(ctx, obj.ID, WithEventCategory("probe"))
	require.NoError(t, err)

	require.NoError(t, cc.AddEvent(ctx, obj.ID,
		EventSpec{Category: "probe", Type: EventNormal, Reason: "Probing"}))
	assert.Equal(t, "Probing", recv(t, all.Events).Reason)
	assert.Equal(t, "Probing", recv(t, probes.Events).Reason)

	resumed, err := client.WatchEvents(ctx, obj.ID, WithEventsResumeFrom(all.ResourceVersion))
	require.NoError(t, err)
	assert.Equal(t, "Probing", recv(t, resumed.Events).Reason, "the resume replays its own gap")

	require.NoError(t, cc.AddEvent(ctx, obj.ID,
		EventSpec{Category: "probe", Type: EventNormal, Reason: "Connected"}))
	assert.Equal(t, "Connected", recv(t, all.Events).Reason, "the live streams carry on")
	assert.Equal(t, "Connected", recv(t, probes.Events).Reason)
}

// A stream reports the retention the sweeper enforces, so a consumer holding
// runs in memory sizes its own list from the server's number rather than a copy
// of it. Configuration, not a per-stream fact: an unset bound reads zero.
func TestWatchEventsReportsTheRetentionBound(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
		want EventRetention
	}{
		{"both bounds", []Option{WithEventRetention(20, time.Hour)}, EventRetention{PerTimeline: 20, MaxAge: time.Hour}},
		{"unset", nil, EventRetention{}},
		{"count only", []Option{WithEventRetention(20, 0)}, EventRetention{PerTimeline: 20}},
		{"age only", []Option{WithEventRetention(0, time.Hour)}, EventRetention{MaxAge: time.Hour}},
		// A negative bound is unenforced — the sweeper gates on > 0 — so the
		// field reports it as unset rather than handing back a negative cap.
		{"negative", []Option{WithEventRetention(-5, -time.Hour)}, EventRetention{}},
		{"negative count, real age", []Option{WithEventRetention(-5, time.Hour)}, EventRetention{MaxAge: time.Hour}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			_, _, client, _ := watchFixtureWith(t, tc.opts...)
			obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

			stream, err := client.WatchEvents(ctx, obj.ID)
			require.NoError(t, err)
			assert.Equal(t, tc.want, stream.Retention)
		})
	}
}

// The bound is configuration, not a property of the snapshot, so a resume
// reports it too — the reader that resumes is exactly the one holding a list
// across reconnects.
func TestWatchEventsResumeReportsTheRetentionBound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, client, cc := watchFixtureWith(t, WithEventRetention(20, time.Hour))
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Probing"}))

	snap, err := client.WatchEvents(ctx, obj.ID)
	require.NoError(t, err)
	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Connected"}))

	resumed, err := client.WatchEvents(ctx, obj.ID, WithEventsResumeFrom(snap.ResourceVersion))
	require.NoError(t, err)
	require.Empty(t, resumed.Runs, "a resume snapshots nothing")
	assert.Equal(t, snap.Retention, resumed.Retention)
	assert.Equal(t, EventRetention{PerTimeline: 20, MaxAge: time.Hour}, resumed.Retention)
}

// The cursor moves only past a delivered page, so a run abandoned mid-send is
// still owed: a new stream resuming from the caller's last checkpoint gets it.
func TestWatchEventsKeepsAnUndeliveredRunOwed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store, _, client, cc := watchFixture(t)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	stream, err := client.WatchEvents(ctx, obj.ID)
	require.NoError(t, err)
	checkpoint := stream.ResourceVersion

	// Nobody reads the stream, so this run is read from the log but never
	// delivered; cancelling after the read leaves the send abandoned.
	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Undelivered"}))
	waitClosed(t, chanAfter(store.eventsListed, 1), "the log read that found the run")
	cancel()
	waitClosed(t, closedWhenDrained(stream.Events), "the abandoned stream to close")

	resumeCtx, cancelResume := context.WithCancel(context.Background())
	defer cancelResume()
	resumed, err := client.WatchEvents(resumeCtx, obj.ID, WithEventsResumeFrom(checkpoint))
	require.NoError(t, err)
	assert.Equal(t, "Undelivered", recv(t, resumed.Events).Reason)
}

// A stream exists only while its watch does: cancelling it leaves no reader and
// no registered receiver behind, so an object nobody watches costs nothing.
func TestWatchEventsLeavesNothingBehind(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, bh, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	streamCtx, endStream := context.WithCancel(ctx)
	stream, err := client.WatchEvents(streamCtx, obj.ID)
	require.NoError(t, err)
	endStream()
	waitClosed(t, closedWhenDrained(stream.Events), "the stream to end with its context")

	// The receiver went with it, so the hub has nobody to publish to. A leaked
	// one would hold a filled slot and TestMain would report its goroutine.
	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Unheard"}))
	assert.NoError(t, bh.eventWriteHub.Send(obj.ID))
}

// The snapshot and the tail agree on a sub-millisecond Since. last_at holds
// milliseconds, so both sides truncate the bound: comparing at full precision
// on one side only would drop from the stream a run the snapshot delivered.
func TestWatchEventsSinceAgreesAtSubMillisecond(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Probing"}))

	opened, err := client.WatchEvents(ctx, obj.ID)
	require.NoError(t, err)
	require.Len(t, opened.Runs, 1)
	// Inside the run's own millisecond, above it by less than the column holds.
	since := opened.Runs[0].LastAt.Add(500 * time.Microsecond)

	filtered, err := client.WatchEvents(ctx, obj.ID, WithEventsSince(since))
	require.NoError(t, err)
	assert.Len(t, filtered.Runs, 1, "the snapshot's SQL truncates the bound")

	// The same run down the tail, which a resume below it replays.
	resumed, err := client.WatchEvents(ctx, obj.ID, WithEventsSince(since), WithEventsResumeFrom(0))
	require.NoError(t, err)
	assert.Equal(t, "Probing", recv(t, resumed.Events).Reason, "and the tail agrees with it")
}

// Every read the subscribe makes can fail, and each fails the call rather than
// handing back a stream that ends immediately.
func TestWatchEventsReportsSubscribeReadFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Probing"}))

	t.Run("existence probe", func(t *testing.T) {
		store.metaErr.Store(true)
		defer store.metaErr.Store(false)
		_, err := client.WatchEvents(ctx, obj.ID)
		assert.ErrorIs(t, err, errBoom)
	})

	t.Run("snapshot", func(t *testing.T) {
		store.eventsSnapErr.Store(true)
		defer store.eventsSnapErr.Store(false)
		_, err := client.WatchEvents(ctx, obj.ID)
		assert.ErrorIs(t, err, errBoom)
	})

	t.Run("resume page", func(t *testing.T) {
		store.eventsErr.Store(true)
		defer store.eventsErr.Store(false)
		_, err := client.WatchEvents(ctx, obj.ID, WithEventsResumeFrom(0))
		assert.ErrorIs(t, err, errBoom)
	})

	t.Run("resume head", func(t *testing.T) {
		// Only a resume with nothing above it reads the mark at all.
		store.markErr.Store(true)
		defer store.markErr.Store(false)
		_, err := client.WatchEvents(ctx, obj.ID, WithEventsResumeFrom(1<<40))
		assert.ErrorIs(t, err, errBoom)
	})
}

// Retention overtaking a live stream ends it: the reader's cursor is below what
// the log can still serve, so continuing would skip runs silently.
func TestWatchEventsEndsWhenRetentionPassesALiveStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	stream, err := client.WatchEvents(ctx, obj.ID)
	require.NoError(t, err)

	// Forced rather than swept, so the horizon crosses this reader's cursor at a
	// moment the test picks; the next wake is what makes it look.
	store.forceEventTrimmed.Store(1 << 40)
	require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Probing"}))

	waitClosed(t, closedWhenDrained(stream.Events), "the stream to end below the horizon")
	assert.ErrorIs(t, stream.Err(), ErrWatchTooOld)
}

// A watch needs the hub New builds: Send and Close no-op on the zero value, but
// a receiver has to be tied to a hub, so the subscribe reports it.
func TestWatchEventsOnABeehiveNotBuiltByNewFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Registered by hand: Register itself needs the maps New builds, and what is
	// under test is the hub, not the registry.
	bh := &Beehive{
		store:       newClientTestStore(t),
		reconcilers: map[GroupKind]*reconciler{clientTestGK: {}},
	}
	// The siblings still tolerate the zero hub, which is what makes Watch the
	// odd one.
	require.NoError(t, bh.eventWriteHub.Send(1))
	require.NotPanics(t, bh.eventWriteHub.Close)

	_, err := NewClient[cSpec, cStatus](bh, clientTestGK).WatchEvents(ctx, 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "built by New")
}

// The drain floor paces a reader the same way the object tail's does: a pass
// inside the window reads nothing and reports when the window opens, and a
// drain that spends its page budget re-arms for the remainder rather than for a
// whole interval.
func TestEventReaderPacesItsDrains(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, bh, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	for _, reason := range []string{"R1", "R2"} {
		require.NoError(t, cc.AddEvent(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: reason}))
	}

	clk := &fakeClock{at: time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)}
	r := &eventReader{
		bh: bh, id: obj.ID,
		out:    make(chan Event, 8), // buffered: nothing here reads the stream
		stream: &EventStream{},
		gate:   rategate.NewSingle(time.Second),
		retry:  bh.watchBackoff(),
		floor:  time.Minute,
		now:    clk.now,
		// One run per page and one page per drain, so two runs are one page more
		// than the budget covers.
		pageCap: 1, pagesPerDrain: 1,
		resolved: true,
	}

	next, backingOff, done := r.pass(ctx, clk.now(), false)
	require.False(t, done)
	assert.False(t, backingOff)
	assert.Equal(t, time.Second, next, "budget spent with more above the cursor: the throttle's remainder")

	next, _, done = r.pass(ctx, clk.now(), false)
	require.False(t, done)
	assert.Equal(t, time.Second, next, "a pass inside the window waits it out")

	clk.advance(time.Second)
	next, _, done = r.pass(ctx, clk.now(), false)
	require.False(t, done)
	assert.Equal(t, time.Second, next, "a full page is proof of more, so the pacing holds")
	assert.Len(t, r.out, 2, "both runs delivered, one drain each")

	clk.advance(time.Second)
	next, _, done = r.pass(ctx, clk.now(), false)
	require.False(t, done)
	assert.Equal(t, r.floor, next, "a short page is the drained signal: back to the floor")
}
