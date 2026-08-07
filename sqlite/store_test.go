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

package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/beehive/internal/storeapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testGK = beehive.GroupKind{Group: "", Kind: "Greeting"}

func newTestStore(t *testing.T) beehive.Store {
	t.Helper()
	store, err := OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })
	return store
}

// newEventObject creates a bare object of testGK and returns its id, for the
// EventsAdd tests to hang events off.
func newEventObject(t *testing.T, store beehive.Store) storeapi.ObjectID {
	t.Helper()
	return newRefObject(t, store).ID
}

// addEvent records an event and returns the run it landed in, which is what the
// store no longer hands back.
func addEvent(t *testing.T, store beehive.Store, id storeapi.ObjectID, in storeapi.EventsAddInput) *storeapi.Event {
	t.Helper()
	require.NoError(t, store.Events().Add(context.Background(), testGK, id, in))
	run, err := store.Events().GetLatest(context.Background(), id, in.Category)
	require.NoError(t, err)
	require.NotNil(t, run)
	return run
}

// ageRun pushes a run's window end into the past, which is what maxAge reads.
// Direct SQL: the store owns the clock, so there is nothing to inject.
func ageRun(t *testing.T, store beehive.Store, run int64, by time.Duration) {
	t.Helper()
	_, err := store.(*sqliteStore).db.ExecContext(context.Background(),
		`UPDATE events SET last_at = ? WHERE id = ?`, toMillis(time.Now().UTC().Add(-by)), run)
	require.NoError(t, err)
}

// A first emission starts a run: count 1, a collapsed window, an assigned id and
// resource_version.
func TestAddEventStartsRun(t *testing.T) {
	store := newTestStore(t)
	id := newEventObject(t, store)

	e := addEvent(t, store, id, storeapi.EventsAddInput{
		Category: "connection", Type: "Warning", Reason: "ProbeFailed",
		Message: "i/o timeout", Detail: []byte(`{"attempt":1}`),
	})
	assert.NotZero(t, e.ID)
	assert.Equal(t, id, e.ObjectID)
	assert.Equal(t, "connection", e.Category)
	assert.Equal(t, "Warning", e.Type)
	assert.Equal(t, "ProbeFailed", e.Reason)
	assert.Equal(t, "i/o timeout", e.Message)
	assert.JSONEq(t, `{"attempt":1}`, string(e.Detail))
	assert.Equal(t, 1, e.Count)
	assert.Equal(t, e.FirstAt, e.LastAt, "a fresh run's window is a point")
	assert.NotZero(t, e.ResourceVersion)
}

// Re-emitting the same (Category, Type, Reason) extends the latest run in place:
// same row, Count grows, the window start is preserved while its end advances,
// and Message/Detail are re-sampled to the latest occurrence.
func TestAddEventExtendsRun(t *testing.T) {
	store := newTestStore(t)
	id := newEventObject(t, store)

	first := addEvent(t, store, id, storeapi.EventsAddInput{
		Category: "connection", Type: "Warning", Reason: "ProbeFailed",
		Message: "timeout", Detail: []byte(`{"n":1}`),
	})
	second := addEvent(t, store, id, storeapi.EventsAddInput{
		Category: "connection", Type: "Warning", Reason: "ProbeFailed",
		Message: "still down", Detail: []byte(`{"n":2}`),
	})

	assert.Equal(t, first.ID, second.ID, "same run extended, not a new row")
	assert.Equal(t, 2, second.Count)
	assert.Equal(t, first.FirstAt, second.FirstAt, "window start preserved")
	assert.False(t, second.LastAt.Before(first.FirstAt), "window end advances")
	assert.Equal(t, "still down", second.Message, "message re-sampled")
	assert.JSONEq(t, `{"n":2}`, string(second.Detail), "detail re-sampled")
	assert.Greater(t, second.ResourceVersion, first.ResourceVersion)
}

// A change in Reason or Type (the run key besides Category) appends a fresh run
// rather than extending — the contiguous-run boundary.
func TestAddEventNewRunOnKeyChange(t *testing.T) {
	store := newTestStore(t)
	id := newEventObject(t, store)

	base := func(typ, reason string) storeapi.EventsAddInput {
		return storeapi.EventsAddInput{Category: "connection", Type: typ, Reason: reason}
	}

	a := addEvent(t, store, id, base("Warning", "ProbeFailed"))

	b := addEvent(t, store, id, base("Warning", "TLSHandshake"))
	assert.NotEqual(t, a.ID, b.ID, "reason change starts a new run")
	assert.Equal(t, 1, b.Count)

	c := addEvent(t, store, id, base("Normal", "TLSHandshake"))
	assert.NotEqual(t, b.ID, c.ID, "type change starts a new run")
	assert.Equal(t, 1, c.Count)
}

// Aggregation is scoped per (object, category): an emission in one category never
// breaks a run in another, even when the two are interleaved.
func TestAddEventCategoriesIndependent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	conn := storeapi.EventsAddInput{Category: "connection", Type: "Warning", Reason: "ProbeFailed"}
	sync := storeapi.EventsAddInput{Category: "sync", Type: "Normal", Reason: "Synced"}

	conn1 := addEvent(t, store, id, conn)
	require.NoError(t, store.Events().Add(ctx, testGK, id, sync)) // interleaved other category
	conn2 := addEvent(t, store, id, conn)

	assert.Equal(t, conn1.ID, conn2.ID, "interleaved other-category event must not break this run")
	assert.Equal(t, 2, conn2.Count)
}

// EventsAdd is kind-scoped like the other id-keyed mutators: a foreign id is
// ErrWrongKind, a missing id is ErrNotFound, and neither writes a row.
func TestAddEventScoped(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	ev := storeapi.EventsAddInput{Category: "connection", Type: "Normal", Reason: "OK"}

	err := store.Events().Add(ctx, beehive.GroupKind{Kind: "Other"}, id, ev)
	assert.ErrorIs(t, err, storeapi.ErrWrongKind)

	err = store.Events().Add(ctx, testGK, 999999, ev)
	assert.ErrorIs(t, err, storeapi.ErrNotFound)
}

// EventsList returns an object's runs newest-first (by last_at, then id as the
// same-millisecond tiebreak).
func TestListEventsOrdersNewestFirst(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	rec := func(typ, reason string) {
		require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{Category: "connection", Type: typ, Reason: reason}))
	}
	rec("Normal", "Connected")     // A
	rec("Warning", "TLSHandshake") // B
	rec("Normal", "Connected")     // C (new run: key changed from B)
	rec("Warning", "ProbeFailed")  // D

	got, err := store.Events().List(ctx, id, storeapi.EventQuery{})
	require.NoError(t, err)
	require.Len(t, got, 4)
	assert.Equal(t, "ProbeFailed", got[0].Reason)  // D, newest
	assert.Equal(t, "Connected", got[1].Reason)    // C
	assert.Equal(t, "TLSHandshake", got[2].Reason) // B
	assert.Equal(t, "Connected", got[3].Reason)    // A, oldest
}

// EventQuery narrows a EventsList read by category/type/reason/since and caps it
// by limit; the zero query returns every run for the object.
func TestListEventsFilters(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	rec := func(cat, typ, reason string) {
		require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{Category: cat, Type: typ, Reason: reason}))
	}
	rec("connection", "Warning", "ProbeFailed")
	rec("connection", "Normal", "Connected")
	rec("sync", "Normal", "Synced")

	t.Run("category restricts to one timeline", func(t *testing.T) {
		cat := "connection"
		got, err := store.Events().List(ctx, id, storeapi.EventQuery{Category: &cat})
		require.NoError(t, err)
		require.Len(t, got, 2)
		for _, e := range got {
			assert.Equal(t, "connection", e.Category)
		}
	})
	t.Run("nil category returns all timelines", func(t *testing.T) {
		got, err := store.Events().List(ctx, id, storeapi.EventQuery{})
		require.NoError(t, err)
		assert.Len(t, got, 3)
	})
	t.Run("type", func(t *testing.T) {
		got, err := store.Events().List(ctx, id, storeapi.EventQuery{Type: "Warning"})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "ProbeFailed", got[0].Reason)
	})
	t.Run("reason", func(t *testing.T) {
		got, err := store.Events().List(ctx, id, storeapi.EventQuery{Reason: "Synced"})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "sync", got[0].Category)
	})
	t.Run("limit takes the newest N", func(t *testing.T) {
		got, err := store.Events().List(ctx, id, storeapi.EventQuery{Limit: 2})
		require.NoError(t, err)
		assert.Len(t, got, 2)
	})
	t.Run("since bounds by last_at", func(t *testing.T) {
		got, err := store.Events().List(ctx, id, storeapi.EventQuery{Since: time.Now().UTC().Add(time.Hour)})
		require.NoError(t, err)
		assert.Empty(t, got, "a future lower bound excludes every run")

		got, err = store.Events().List(ctx, id, storeapi.EventQuery{Since: time.Now().UTC().Add(-time.Hour)})
		require.NoError(t, err)
		assert.Len(t, got, 3, "a past lower bound includes every run")
	})
}

// EventsGetLatest returns the current run in a category timeline, or nil when that
// timeline is empty.
func TestGetLatestEvent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	got, err := store.Events().GetLatest(ctx, id, "connection")
	require.NoError(t, err)
	assert.Nil(t, got, "empty timeline is nil, not an error")

	rec := func(cat, typ, reason string) {
		require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{Category: cat, Type: typ, Reason: reason}))
	}
	rec("connection", "Warning", "ProbeFailed")
	rec("connection", "Normal", "Connected")
	rec("sync", "Normal", "Synced")

	got, err = store.Events().GetLatest(ctx, id, "connection")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "Connected", got.Reason, "the current (newest) run for the category")

	got, err = store.Events().GetLatest(ctx, id, "sync")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "Synced", got.Reason, "scoped to the category")

	got, err = store.Events().GetLatest(ctx, id, "nope")
	require.NoError(t, err)
	assert.Nil(t, got, "unknown category is nil")
}

// EventsSweep caps each timeline to the newest perTimeline runs.
func TestSweepEventsCapN(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	for _, r := range []string{"R1", "R2", "R3", "R4"} { // 4 distinct runs
		require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{Category: "c", Type: "Normal", Reason: r}))
	}

	deleted, err := store.Events().Sweep(ctx, 2, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, deleted)

	got, err := store.Events().List(ctx, id, storeapi.EventQuery{})
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "R4", got[0].Reason, "newest kept")
	assert.Equal(t, "R3", got[1].Reason)
}

// The cap-N ring partitions by (object, category): a flapping timeline can't
// evict a quiet one, on the same object or a different one.
func TestSweepEventsCapNPartitions(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	a := newEventObject(t, store)
	b := newEventObject(t, store)

	rec := func(id storeapi.ObjectID, cat, reason string) {
		require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{Category: cat, Type: "Normal", Reason: reason}))
	}
	rec(a, "connection", "F1") // object a, connection flaps 3 runs
	rec(a, "connection", "F2")
	rec(a, "connection", "F3")
	rec(a, "sync", "S1")            // object a, sync has 1 run
	rec(b, "connection", "OtherC1") // object b, its own timeline

	_, err := store.Events().Sweep(ctx, 1, 0, 0)
	require.NoError(t, err)

	conn := "connection"
	sync := "sync"
	aConn, err := store.Events().List(ctx, a, storeapi.EventQuery{Category: &conn})
	require.NoError(t, err)
	require.Len(t, aConn, 1)
	assert.Equal(t, "F3", aConn[0].Reason, "flapping timeline keeps its newest")

	aSync, err := store.Events().List(ctx, a, storeapi.EventQuery{Category: &sync})
	require.NoError(t, err)
	require.Len(t, aSync, 1, "quiet timeline survives the flap on the same object")
	assert.Equal(t, "S1", aSync[0].Reason)

	bConn, err := store.Events().List(ctx, b, storeapi.EventQuery{Category: &conn})
	require.NoError(t, err)
	require.Len(t, bConn, 1, "another object's timeline is independent")
	assert.Equal(t, "OtherC1", bConn[0].Reason)
}

// EventsSweep drops runs whose window ended more than maxAge ago.
func TestSweepEventsMaxAge(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	old := addEvent(t, store, id, storeapi.EventsAddInput{Category: "c", Type: "Normal", Reason: "Old"})
	require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{Category: "c", Type: "Warning", Reason: "New"}))

	ageRun(t, store, old.ID, 2*time.Hour)

	deleted, err := store.Events().Sweep(ctx, 0, time.Hour, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, deleted)

	got, err := store.Events().List(ctx, id, storeapi.EventQuery{})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "New", got[0].Reason, "the run within maxAge is kept")
}

// The cap counts runs, not occurrences: an extend grows a run in place, so a
// timeline repeating one (type, reason) never reaches the cap however many
// events it records.
func TestSweepEventsCapNCountsRuns(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	for range 5 {
		require.NoError(t, store.Events().Add(ctx, testGK, id,
			storeapi.EventsAddInput{Category: "c", Type: "Normal", Reason: "Same"}))
	}

	deleted, err := store.Events().Sweep(ctx, 1, 0, 0)
	require.NoError(t, err)
	assert.Zero(t, deleted, "one run, at the cap")

	got, err := store.Events().List(ctx, id, storeapi.EventQuery{})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, 5, got[0].Count, "the occurrences are unbounded")
}

// maxAge is a cutoff over the whole table where the cap partitions: a quiet
// timeline the cap would keep still loses runs that aged out.
func TestSweepEventsMaxAgeSpansTimelines(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	quiet := addEvent(t, store, id, storeapi.EventsAddInput{Category: "quiet", Type: "Normal", Reason: "Q1"})
	chatty := addEvent(t, store, id, storeapi.EventsAddInput{Category: "chatty", Type: "Normal", Reason: "C1"})
	ageRun(t, store, quiet.ID, 2*time.Hour)
	ageRun(t, store, chatty.ID, 2*time.Hour)

	deleted, err := store.Events().Sweep(ctx, 0, time.Hour, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, deleted, "both timelines aged out, cap or no cap")
}

// A sweep trims at most eventCapBudget timelines, so one sweep cannot hold the
// write connection for an unbounded backlog. What it leaves is picked up by the
// next sweep, and the horizon only rises in between.
func TestSweepEventsCapIsProgressive(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	timelines := eventCapBudget + 2
	for c := range timelines {
		for _, r := range []string{"R1", "R2"} {
			require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{
				Category: strconv.Itoa(c), Type: "Normal", Reason: r,
			}))
		}
	}

	deleted, err := store.Events().Sweep(ctx, 1, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, eventCapBudget, deleted, "one sweep trims up to the budget")

	deleted, err = store.Events().Sweep(ctx, 1, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, deleted, "the next sweep finishes the backlog")

	deleted, err = store.Events().Sweep(ctx, 1, 0, 0)
	require.NoError(t, err)
	assert.Zero(t, deleted, "nothing left over cap")
}

// The horizon covers every run a sweep removed: each deleted version is at or
// below its own timeline's trimmed_through, so no trimmed run sits above the
// mark a resume is checked against.
func TestSweepEventsHorizonCoversEveryTrimmedRun(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	byRV := map[int64]string{} // version → category, for what the sweep removes
	for _, cat := range []string{"a", "b"} {
		for _, r := range []string{"R1", "R2", "R3"} {
			e := addEvent(t, store, id, storeapi.EventsAddInput{Category: cat, Type: "Normal", Reason: r})
			byRV[e.ResourceVersion] = cat
		}
	}
	// Age one run so both bounds trim in the same sweep.
	aged := addEvent(t, store, id, storeapi.EventsAddInput{Category: "c", Type: "Normal", Reason: "Old"})
	byRV[aged.ResourceVersion] = "c"
	ageRun(t, store, aged.ID, 2*time.Hour)

	_, err := store.Events().Sweep(ctx, 1, time.Hour, 0)
	require.NoError(t, err)

	survived := map[int64]bool{}
	runs, err := store.Events().List(ctx, id, storeapi.EventQuery{})
	require.NoError(t, err)
	for _, r := range runs {
		survived[r.ResourceVersion] = true
	}
	for rv, cat := range byRV {
		if survived[rv] {
			continue
		}
		assert.LessOrEqual(t, rv, eventHorizon(t, store, id, cat),
			"trimmed run %d in %q sits above its horizon", rv, cat)
	}
}

// EventsListSince pages the log above a cursor, oldest-first. An extend
// re-samples resource_version, so a run that grew comes back as itself.
func TestEventsListSinceIsTheTail(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	first := addEvent(t, store, id, storeapi.EventsAddInput{Category: "c", Type: "Normal", Reason: "R1"})
	second := addEvent(t, store, id, storeapi.EventsAddInput{Category: "c", Type: "Warning", Reason: "R2"})

	got, _, err := store.Events().ListSince(ctx, id, nil, 0, 10)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "R1", got[0].Reason, "oldest first")
	assert.Equal(t, "R2", got[1].Reason)

	got, _, err = store.Events().ListSince(ctx, id, nil, first.ResourceVersion, 10)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "R2", got[0].Reason, "the cursor excludes what it has seen")

	// An extend lifts R2 above the cursor that already covered it.
	extended := addEvent(t, store, id, storeapi.EventsAddInput{Category: "c", Type: "Warning", Reason: "R2"})
	got, _, err = store.Events().ListSince(ctx, id, nil, second.ResourceVersion, 10)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, second.ID, got[0].ID, "the same run, not a new one")
	assert.Equal(t, 2, got[0].Count)
	assert.Equal(t, extended.ResourceVersion, got[0].ResourceVersion)

	got, _, err = store.Events().ListSince(ctx, id, nil, 0, 1)
	require.NoError(t, err)
	assert.Len(t, got, 1, "limit bounds the page")
}

// EventsSnapshot is EventsList plus the position it is complete as of, so a
// watch can start its tail exactly above what it already holds.
func TestEventsSnapshotCarriesItsPosition(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	runs, at, err := store.Events().Snapshot(ctx, id, storeapi.EventQuery{})
	require.NoError(t, err)
	assert.Empty(t, runs)
	assert.Zero(t, at, "an empty log reads position 0")

	require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{Category: "connection", Type: "Normal", Reason: "R1"}))
	last := addEvent(t, store, id, storeapi.EventsAddInput{Category: "sync", Type: "Warning", Reason: "R2"})

	runs, at, err = store.Events().Snapshot(ctx, id, storeapi.EventQuery{})
	require.NoError(t, err)
	require.Len(t, runs, 2)
	assert.Equal(t, "R2", runs[0].Reason, "newest first, like EventsList")
	assert.Equal(t, last.ResourceVersion, at)

	tail, _, err := store.Events().ListSince(ctx, id, nil, at, 10)
	require.NoError(t, err)
	assert.Empty(t, tail, "the tail above the position holds nothing the snapshot did")

	// The position spans every category even when the listing is filtered: it is
	// the log's, not the query's, or a filtered watch would resume too low.
	conn := "connection"
	runs, at, err = store.Events().Snapshot(ctx, id, storeapi.EventQuery{Category: &conn})
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Equal(t, "R1", runs[0].Reason)
	assert.Equal(t, last.ResourceVersion, at)
}

// The page is unfiltered — the caller filters — but category selects which
// horizon the call reports.
func TestEventsListSinceReportsTheHorizon(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	for _, r := range []string{"C1", "C2"} {
		require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{Category: "connection", Type: "Normal", Reason: r}))
	}
	require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{Category: "sync", Type: "Normal", Reason: "S1"}))
	_, err := store.Events().Sweep(ctx, 1, 0, 0)
	require.NoError(t, err)
	trimmed := eventHorizon(t, store, id, "connection")
	require.NotZero(t, trimmed)

	conn, sync := "connection", "sync"
	page, at, err := store.Events().ListSince(ctx, id, &conn, 0, 10)
	require.NoError(t, err)
	assert.Equal(t, trimmed, at, "the watched timeline's horizon")
	assert.Len(t, page, 2, "the page spans every category")

	_, at, err = store.Events().ListSince(ctx, id, &sync, 0, 10)
	require.NoError(t, err)
	assert.Zero(t, at, "a timeline that lost nothing")

	_, at, err = store.Events().ListSince(ctx, id, nil, 0, 10)
	require.NoError(t, err)
	assert.Equal(t, trimmed, at, "unfiltered: the max across timelines")
}

// A collected object's log cascaded away with it, so an empty page there is not
// "no events" — it is "this object is gone", and saying so is the point.
func TestEventsListSinceReportsACollectedObject(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	page, at, err := store.Events().ListSince(ctx, id, nil, 0, 10)
	require.NoError(t, err, "an object with no events yet is not gone")
	assert.Empty(t, page)
	assert.Zero(t, at)

	require.NoError(t, store.ObjectsDelete(ctx, id))
	_, _, err = store.Events().ListSince(ctx, id, nil, 0, 10)
	assert.ErrorIs(t, err, storeapi.ErrNotFound)
}

// eventHorizon reads a timeline's recorded horizon, 0 when none.
func eventHorizon(t *testing.T, store beehive.Store, id storeapi.ObjectID, category string) int64 {
	t.Helper()
	var rv sql.NullInt64
	err := store.(*sqliteStore).db.QueryRowContext(context.Background(),
		`SELECT trimmed_through FROM events_horizon WHERE object_id = ? AND category = ?`,
		id, category).Scan(&rv)
	if errors.Is(err, sql.ErrNoRows) {
		return 0
	}
	require.NoError(t, err)
	return rv.Int64
}

// A sweep records what it removed, per (object, category) — the ring cap's own
// partition — so a trim on a chatty timeline says nothing about a quiet one.
func TestSweepEventsRecordsHorizonPerTimeline(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	var trimmed int64
	for _, r := range []string{"C1", "C2", "C3"} {
		e := addEvent(t, store, id, storeapi.EventsAddInput{Category: "connection", Type: "Normal", Reason: r})
		if r == "C2" {
			trimmed = e.ResourceVersion // the highest version the cap will drop
		}
	}
	require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{Category: "sync", Type: "Normal", Reason: "S1"}))

	deleted, err := store.Events().Sweep(ctx, 1, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, deleted, "the count survives the horizon write")

	assert.Equal(t, trimmed, eventHorizon(t, store, id, "connection"))
	assert.Zero(t, eventHorizon(t, store, id, "sync"), "a timeline that lost nothing has no horizon")
}

// The horizon only rises: a later sweep that trims older runs leaves it alone.
func TestSweepEventsHorizonOnlyRises(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	for _, r := range []string{"R1", "R2", "R3"} {
		require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{Category: "c", Type: "Normal", Reason: r}))
	}
	_, err := store.Events().Sweep(ctx, 1, 0, 0)
	require.NoError(t, err)
	high := eventHorizon(t, store, id, "c")
	require.NotZero(t, high)

	// Age the survivor and sweep it: it is older, so the horizon must not move down.
	s := store.(*sqliteStore)
	_, err = s.db.ExecContext(ctx, `UPDATE events SET resource_version = 1, last_at = ? WHERE object_id = ?`,
		toMillis(time.Now().UTC().Add(-2*time.Hour)), id)
	require.NoError(t, err)
	_, err = store.Events().Sweep(ctx, 0, time.Hour, 0)
	require.NoError(t, err)

	assert.Equal(t, high, eventHorizon(t, store, id, "c"))
}

// A horizon row cascades with its object, like the log it describes.
func TestDeleteObjectCascadesEventHorizon(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	for _, r := range []string{"R1", "R2"} {
		require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{Category: "c", Type: "Normal", Reason: r}))
	}
	_, err := store.Events().Sweep(ctx, 1, 0, 0)
	require.NoError(t, err)
	require.NotZero(t, eventHorizon(t, store, id, "c"))

	require.NoError(t, store.ObjectsDelete(ctx, id))
	assert.Zero(t, eventHorizon(t, store, id, "c"))
}

// Deleting an object cascade-deletes its event log (FK ON DELETE CASCADE).
func TestDeleteObjectCascadesEvents(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{Category: "c", Type: "Normal", Reason: "X"}))
	require.NoError(t, store.ObjectsDelete(ctx, id))

	got, err := store.Events().List(ctx, id, storeapi.EventQuery{})
	require.NoError(t, err)
	assert.Empty(t, got, "events cascade-delete with their object")
}

func dropEventsTable(t *testing.T, store *sqliteStore) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(), `DROP TABLE events`)
	require.NoError(t, err)
}

// breakEventRowRead makes events.first_at NULL for every existing run so any
// later full-row read fails inside Scan: first_at scans into a non-nullable
// int64, and "converting NULL to int64" is a scan error. Dropping and re-adding
// the column (rather than just dropping it) keeps the column present so the
// SELECT still prepares — the fault surfaces per row in the scan loop, not at
// QueryContext. STRICT + NOT NULL rules out the old trick of storing
// unconvertible text in the INTEGER column (STRICT rejects the write outright).
func breakEventRowRead(t *testing.T, store *sqliteStore) {
	t.Helper()
	ctx := context.Background()
	_, err := store.db.ExecContext(ctx, `ALTER TABLE events DROP COLUMN first_at`)
	require.NoError(t, err)
	_, err = store.db.ExecContext(ctx, `ALTER TABLE events ADD COLUMN first_at INTEGER`)
	require.NoError(t, err)
}

// EventsAdd surfaces store faults from each of its steps.
func TestAddEventStoreErrors(t *testing.T) {
	ctx := context.Background()
	ev := storeapi.EventsAddInput{Category: "c", Type: "Normal", Reason: "R"}

	t.Run("resource_version_seq missing", func(t *testing.T) {
		store := newRawStore(t)
		id := newEventObject(t, store)
		dropSeq(t, store)
		err := store.Events().Add(ctx, testGK, id, ev)
		require.Error(t, err)
	})

	t.Run("latest-run probe fails", func(t *testing.T) {
		store := newRawStore(t)
		id := newEventObject(t, store)
		dropEventsTable(t, store)
		err := store.Events().Add(ctx, testGK, id, ev)
		require.Error(t, err)
	})
}

// EventsList surfaces a query fault and a per-row scan fault.
func TestListEventsStoreErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("query fails", func(t *testing.T) {
		store := newRawStore(t)
		id := newEventObject(t, store)
		dropEventsTable(t, store)
		_, err := store.Events().List(ctx, id, storeapi.EventQuery{})
		require.Error(t, err)
	})

	t.Run("row fails to scan", func(t *testing.T) {
		store := newRawStore(t)
		id := newEventObject(t, store)
		require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{Category: "c", Type: "Normal", Reason: "R"}))
		breakEventRowRead(t, store)
		_, err := store.Events().List(ctx, id, storeapi.EventQuery{})
		require.Error(t, err)
	})
}

// EventsMaxVersion is the event watch's gate: the highest resource_version over one
// object's runs, spanning every category, 0 for an object with no runs at all, and
// scoped to the object asked for.
func TestEventsMaxVersion(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)
	other := newEventObject(t, store)

	quiet, err := store.Events().MaxVersion(ctx, id)
	require.NoError(t, err)
	assert.Zero(t, quiet, "an object with no runs has no mark")

	unknown, err := store.Events().MaxVersion(ctx, 999999)
	require.NoError(t, err)
	assert.Zero(t, unknown, "an id with no row reads the same as an empty log")

	add := func(id storeapi.ObjectID, category, reason string) int64 {
		t.Helper()
		ev := addEvent(t, store, id, storeapi.EventsAddInput{
			Category: category, Type: "Normal", Reason: reason})
		return ev.ResourceVersion
	}

	first := add(id, "connection", "Connected")
	got, err := store.Events().MaxVersion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, first, got)

	// Another category's run counts: the mark spans the object's whole log, which is
	// why a filtered watch can be woken by a run it will not be shown.
	elsewhere := add(id, "sync", "Synced")
	got, err = store.Events().MaxVersion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, elsewhere, got, "the newest run of any category is the mark")

	// Extending a run advances its version, so the mark moves without a new row.
	extended := add(id, "sync", "Synced")
	got, err = store.Events().MaxVersion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, extended, got, "an extended run moves the mark")

	// Another object's log is invisible here.
	add(other, "connection", "Connected")
	got, err = store.Events().MaxVersion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, extended, got, "the mark is scoped to one object")
}

// The mark falls when retention removes the newest run, so a consumer must compare
// for inequality rather than growth — the same rule ObjectWritesMaxVersion keeps.
func TestEventsMaxVersionFallsWhenTheNewestRunGoes(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	for _, reason := range []string{"A", "B"} {
		err := store.Events().Add(ctx, testGK, id,
			storeapi.EventsAddInput{Category: "c", Type: "Normal", Reason: reason})
		require.NoError(t, err)
	}
	before, err := store.Events().MaxVersion(ctx, id)
	require.NoError(t, err)

	deleted, err := store.Events().Sweep(ctx, 1, 0, 0)
	require.NoError(t, err)
	require.Equal(t, 1, deleted, "the older run is swept, leaving the newest")

	after, err := store.Events().MaxVersion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, before, after, "sweeping an older run leaves the mark where it was")

	// Now age the survivor out, so retention takes the run the mark points at: the
	// mark falls back to the empty-log zero.
	_, err = store.db.ExecContext(ctx, `UPDATE events SET last_at = 0`)
	require.NoError(t, err)
	deleted, err = store.Events().Sweep(ctx, 0, time.Hour, 0)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	after, err = store.Events().MaxVersion(ctx, id)
	require.NoError(t, err)
	assert.Zero(t, after, "an emptied log reads as no mark at all")
}

// EventsMaxVersion surfaces a query fault rather than reporting a quiet log.
func TestEventsMaxVersionStoreError(t *testing.T) {
	store := newRawStore(t)
	id := newEventObject(t, store)
	dropEventsTable(t, store)
	_, err := store.Events().MaxVersion(context.Background(), id)
	require.Error(t, err)
}

// The cap's candidate query runs on every sweep, over every timeline, so it must
// ride an index rather than sort. Two indexes lead on (object_id, category) and
// either will do; what must not appear is a TEMP B-TREE, which means the group
// by is sorting the whole table, or a table scan, which means reading every
// blob to count rows.
func TestEventsSweepSelectsCandidatesByIndex(t *testing.T) {
	plan := queryPlan(t, newTestStore(t).(*sqliteStore), eventCapCandidates, 1, 1)
	assert.Contains(t, plan, "COVERING INDEX", "plan:\n"+plan)
	assert.NotContains(t, plan, "TEMP B-TREE", "plan:\n"+plan)
}

// EventsMaxVersion is the gate EventsWatch pays on every quiet tick, so it must be
// answered from idx_events_object_rv alone. COVERING is the whole assertion: with
// only idx_events_object_cat the plan still names an index, but resource_version is
// not in it, so the read costs a table-btree lookup for each of the object's runs.
// That index exists for this one query — nothing else would notice it going.
func TestEventsMaxVersionUsesCoveringIndex(t *testing.T) {
	store := newTestStore(t).(*sqliteStore)
	ctx := context.Background()

	rows, err := store.db.QueryContext(ctx,
		`EXPLAIN QUERY PLAN SELECT MAX(resource_version) FROM events WHERE object_id = ?`, int64(1))
	require.NoError(t, err)
	defer rows.Close()

	var plan string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notused, &detail))
		plan += detail + "\n"
	}
	require.NoError(t, rows.Err())
	assert.Contains(t, plan, "COVERING INDEX idx_events_object_rv",
		"the watch gate must read the index alone:\n"+plan)
}

// queryPlan returns the EXPLAIN QUERY PLAN text for q, one step per line.
func queryPlan(t *testing.T, store *sqliteStore, q string, args ...any) string {
	t.Helper()
	rows, err := store.db.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+q, args...)
	require.NoError(t, err)
	defer rows.Close()

	var plan string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notused, &detail))
		plan += detail + "\n"
	}
	require.NoError(t, rows.Err())
	return plan
}

// The two (object_id, category, …) indexes answer opposite sort orders, and each
// one's whole purpose is to keep a sort out of a plan. A temp B-tree in either is
// the regression: it means the query fell back to the other key and is scanning
// the object's entire timeline — unbounded, since event retention is off by
// default — to produce what the right index delivers in order.
func TestEventsIndexesKeepSortsOutOfPlans(t *testing.T) {
	store := newTestStore(t).(*sqliteStore)

	// EventsAdd's run-boundary probe, once per event, ordered by append order.
	plan := queryPlan(t, store,
		`SELECT id, type, reason FROM events WHERE object_id = ? AND category = ?
		 ORDER BY id DESC LIMIT 1`, int64(1), "c")
	assert.Contains(t, plan, "idx_events_latest",
		"the append probe must ride the id-ordered index:\n"+plan)
	assert.NotContains(t, plan, "TEMP B-TREE",
		"the append probe must not sort the timeline to find one run:\n"+plan)

	// EventsList's panel read. The id tiebreak is what makes this sort-free: stop
	// the key at last_at and a limited read sorts the whole timeline for page one.
	plan = queryPlan(t, store,
		`SELECT `+eventColumns+` FROM events WHERE object_id = ? AND category = ?
		 ORDER BY last_at DESC, id DESC LIMIT ?`, int64(1), "c", 50)
	assert.Contains(t, plan, "idx_events_object_cat",
		"the panel read must ride the last_at-ordered index:\n"+plan)
	assert.NotContains(t, plan, "TEMP B-TREE",
		"the panel read must arrive in order, LIMIT included:\n"+plan)
}

// The edge listings inherit their sort from the index they already probe, and
// only a plan can show it — ordering on the edge column and on the joined o.id it
// equals return the same rows. See edgeOrderByReferrer for why one streams and the
// other sorts.
//
// These build their queries from the same two constants store.go does, so editing
// either one to name o.id fails here rather than silently reinstating a temp
// B-tree per matched edge. The chunked variant orders on columns it is
// parameterized by, so it is pinned as written.
func TestEdgeListsInheritTheIndexOrder(t *testing.T) {
	store := newTestStore(t).(*sqliteStore)
	owned := string(storeapi.RelationOwnedBy)

	for _, tc := range []struct {
		name  string
		query string
		args  []any
	}{
		{"incoming", `
			SELECT o.id, o."group", o.kind
			FROM edges r JOIN objects o ON o.id = r.from_id
			WHERE r.to_id = ? AND r.relation = ?` + edgeOrderByReferrer, []any{int64(1), owned}},
		{"cascade children", `
			SELECT o.id, o."group", o.kind, o.deletion_requested_at
			FROM edges r JOIN objects o ON o.id = r.from_id
			WHERE r.to_id = ? AND r.relation = ?` + edgeOrderByReferrer, []any{int64(1), owned}},
		{"outgoing by relation", `
			SELECT o.id, o."group", o.kind
			FROM edges r JOIN objects o ON o.id = r.to_id
			WHERE r.from_id = ? AND r.relation = ?` + edgeOrderByTarget, []any{int64(1), owned}},
		{"incoming batch", `
			SELECT r.to_id, o.id, o."group", o.kind
			FROM edges r JOIN objects o ON o.id = r.from_id
			WHERE r.to_id IN (?,?) AND r.relation = ?
			ORDER BY r.to_id, r.from_id`, []any{int64(1), int64(2), owned}},
		{"outgoing batch", `
			SELECT r.from_id, o.id, o."group", o.kind
			FROM edges r JOIN objects o ON o.id = r.to_id
			WHERE r.from_id IN (?,?) AND r.relation = ?
			ORDER BY r.from_id, r.to_id`, []any{int64(1), int64(2), owned}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := queryPlan(t, store, tc.query, tc.args...)
			assert.NotContains(t, plan, "ORDER BY",
				"the edge index already delivers this order:\n"+plan)
		})
	}

	// EdgesListOutgoing keeps one temp B-tree, for DISTINCT — it collapses an
	// object reached through both relations, which no index can do. The ORDER BY
	// is still free, so the plan must name DISTINCT and nothing else.
	plan := queryPlan(t, store, `
		SELECT DISTINCT o.id, o."group", o.kind
		FROM edges r JOIN objects o ON o.id = r.to_id
		WHERE r.from_id = ?`+edgeOrderByTarget, int64(1))
	assert.Contains(t, plan, "TEMP B-TREE FOR DISTINCT", plan)
	assert.NotContains(t, plan, "ORDER BY",
		"only DISTINCT should need a sort here:\n"+plan)
}

// conditions carries no index of its own: PRIMARY KEY (object_id, type) already
// builds one over exactly the prefix every read keys on. A conditions(object_id)
// index would be a strict prefix of that key — chosen by nothing, written on every
// upsert. This pins the reason it is absent rather than leaving it to look like an
// oversight; the cascade probe is the one plan that might have wanted it.
func TestConditionsReadsRideThePrimaryKey(t *testing.T) {
	store := newTestStore(t).(*sqliteStore)

	for _, tc := range []struct{ name, query string }{
		{"list one object", `SELECT ` + conditionColumns + ` FROM conditions WHERE object_id = ? ORDER BY type`},
		{"get one type", `SELECT ` + conditionColumns + ` FROM conditions WHERE object_id = ? AND type = 'Ready'`},
		{"cascade delete", `DELETE FROM objects WHERE id = ?`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := queryPlan(t, store, tc.query, int64(1))
			assert.Contains(t, plan, "sqlite_autoindex_conditions_1",
				"conditions must be reached through its primary key:\n"+plan)
		})
	}
}

// ConditionsSet's kind gate and its no-op comparison key on the same object, so
// they are one read rather than two round trips on the single connection — and the
// join has to ride a primary key on each side or the fold costs more than it saves.
func TestConditionSetLoadsTheGateAndTheConditionTogether(t *testing.T) {
	store := newTestStore(t).(*sqliteStore)

	plan := queryPlan(t, store, conditionSetLoad, "Ready", int64(1))
	assert.Contains(t, plan, "USING INTEGER PRIMARY KEY",
		"objects must be reached by rowid:\n"+plan)
	assert.Contains(t, plan, "sqlite_autoindex_conditions_1",
		"conditions must be reached through its primary key:\n"+plan)
}

// EventsGetLatest surfaces a scan fault on the current run.
func TestGetLatestEventScanError(t *testing.T) {
	ctx := context.Background()
	store := newRawStore(t)
	id := newEventObject(t, store)
	require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{Category: "c", Type: "Normal", Reason: "R"}))
	breakEventRowRead(t, store)
	_, err := store.Events().GetLatest(ctx, id, "c")
	require.Error(t, err)
}

// EventsSweep surfaces a delete fault from either retention bound.
func TestSweepEventsExecErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("cap-N delete fails", func(t *testing.T) {
		store := newRawStore(t)
		newEventObject(t, store)
		dropEventsTable(t, store)
		_, err := store.Events().Sweep(ctx, 1, 0, 0)
		require.Error(t, err)
	})

	t.Run("max-age delete fails", func(t *testing.T) {
		store := newRawStore(t)
		newEventObject(t, store)
		dropEventsTable(t, store)
		_, err := store.Events().Sweep(ctx, 0, time.Hour, 0)
		require.Error(t, err)
	})

	t.Run("candidate row fails to scan", func(t *testing.T) {
		store := newRawStore(t)
		breakTimelineScan(t, store)
		_, err := store.Events().Sweep(ctx, 1, 0, 0)
		require.Error(t, err)
	})
}

// breakTimelineScan replaces events with a table holding a non-numeric
// object_id, so the cap's candidate query still runs and every row it returns
// fails in the scan loop rather than at QueryContext. The column is indexed, so
// STRICT and the index rule out mutating it in place the way breakEventRowRead
// does; two rows share a timeline so the HAVING clause selects it.
func breakTimelineScan(t *testing.T, store *sqliteStore) {
	t.Helper()
	ctx := context.Background()
	_, err := store.db.ExecContext(ctx, `DROP TABLE events`)
	require.NoError(t, err)
	_, err = store.db.ExecContext(ctx, `
		CREATE TABLE events (object_id TEXT NOT NULL, category TEXT NOT NULL);
		INSERT INTO events (object_id, category) VALUES ('x', 'c'), ('x', 'c')`)
	require.NoError(t, err)
}

// blockEventDeletes makes any DELETE on events fail, so the sweep's horizon
// write lands and only the delete behind it faults.
func blockEventDeletes(t *testing.T, store *sqliteStore) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(),
		`CREATE TRIGGER events_no_delete BEFORE DELETE ON events
		 BEGIN SELECT RAISE(ABORT, 'blocked'); END`)
	require.NoError(t, err)
}

// EventsSweep surfaces a delete fault that the horizon write did not see.
func TestSweepEventsDeleteFailsAfterTheHorizon(t *testing.T) {
	ctx := context.Background()
	store := newRawStore(t)
	id := newEventObject(t, store)
	for _, r := range []string{"R1", "R2"} {
		require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{Category: "c", Type: "Normal", Reason: r}))
	}
	blockEventDeletes(t, store)

	_, err := store.Events().Sweep(ctx, 1, 0, 0)
	require.Error(t, err)

	// The horizon write rolls back with it: the transaction is what keeps a
	// horizon from claiming a trim that never happened.
	assert.Zero(t, eventHorizon(t, store, id, "c"))
}

// EventsSnapshot and EventsListSince surface a fault from either half of the
// read they wrap.
func TestEventsReadsSurfaceStoreErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("snapshot listing fails", func(t *testing.T) {
		store := newRawStore(t)
		id := newEventObject(t, store)
		dropEventsTable(t, store)
		_, _, err := store.Events().Snapshot(ctx, id, storeapi.EventQuery{})
		require.Error(t, err)
	})

	t.Run("page fails", func(t *testing.T) {
		store := newRawStore(t)
		id := newEventObject(t, store)
		dropEventsTable(t, store)
		_, _, err := store.Events().ListSince(ctx, id, nil, 0, 10)
		require.Error(t, err)
	})

	t.Run("page row fails to scan", func(t *testing.T) {
		store := newRawStore(t)
		id := newEventObject(t, store)
		require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{Category: "c", Type: "Normal", Reason: "R"}))
		breakEventRowRead(t, store)
		_, _, err := store.Events().ListSince(ctx, id, nil, 0, 10)
		require.Error(t, err)
	})
}

// A non-positive limit reads nothing rather than reaching SQLite as an
// unbounded LIMIT -1, the same as the write log's tail.
func TestEventsListSinceRejectsANonPositiveLimit(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	id := newEventObject(t, store)
	require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{Category: "c", Type: "Normal", Reason: "R"}))

	page, trimmed, err := store.Events().ListSince(ctx, id, nil, 0, 0)

	require.NoError(t, err)
	assert.Empty(t, page)
	assert.Zero(t, trimmed)
}

// A recorded horizon proves the object was there, so an empty page above the
// head is answered without the existence probe behind it.
func TestEventsListSinceAboveTheHeadIsQuiet(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	id := newEventObject(t, store)
	for _, r := range []string{"R1", "R2"} {
		require.NoError(t, store.Events().Add(ctx, testGK, id, storeapi.EventsAddInput{Category: "c", Type: "Normal", Reason: r}))
	}
	_, err := store.Events().Sweep(ctx, 1, 0, 0)
	require.NoError(t, err)

	page, trimmed, err := store.Events().ListSince(ctx, id, nil, 1<<40, 10)
	require.NoError(t, err)
	assert.Empty(t, page)
	assert.NotZero(t, trimmed, "the horizon still comes back on an empty page")

	// And it is still the caller's timeline that answers, not the object's: an
	// empty page reads the horizon on its own, where a page carries it along.
	quiet := "quiet"
	_, trimmed, err = store.Events().ListSince(ctx, id, &quiet, 1<<40, 10)
	require.NoError(t, err)
	assert.Zero(t, trimmed, "a timeline that lost nothing")
}

// A broken horizon table fails the read rather than reporting 0, which would
// say "nothing was trimmed" on a store that cannot answer — the one thing the
// horizon exists to prevent. The page's own subquery reads it too, so that is
// where a dropped table is caught.
func TestEventsListSinceSurfacesAHorizonFault(t *testing.T) {
	ctx := context.Background()
	store := newRawStore(t)
	id := newEventObject(t, store)
	_, err := store.db.ExecContext(ctx, `DROP TABLE events_horizon`)
	require.NoError(t, err)

	_, _, err = store.Events().ListSince(ctx, id, nil, 0, 10)
	require.Error(t, err)
}

// The name is required, and the schema is what says so. No Go path can express a
// NULL name any more — ObjectsCreateInput.Name is a string — so this reaches past
// the store to the column itself. That is the point: without it the NOT NULL reads
// as redundant with the Go type and invites removal, when in fact it is the only
// thing standing between a foreign writer (or a hand-run migration) and a row no
// name-keyed call can ever address.
func TestObjectsNameIsNotNullable(t *testing.T) {
	store := newTestStore(t)
	db := store.(*sqliteStore).db

	_, err := db.ExecContext(context.Background(),
		`INSERT INTO objects ("group", kind, name, spec, generation, resource_version, created_at, updated_at)
		 VALUES (?, ?, NULL, ?, 1, 1, 0, 0)`,
		testGK.Group, testGK.Kind, `{}`)

	// Assert the row was refused, not how the driver spells the refusal: there is no
	// sentinel to match on for a raw INSERT that bypasses the store, and the message
	// text belongs to the driver.
	require.Error(t, err, "the column rejects NULL")
	assertNoObjectRows(t, store)
}

// And the empty string, which is the same hole from the other side: "" is what
// unset configuration reads as, so a row admitted under it is one every such caller
// would collide on and no name-keyed call could address. Client rejects it too, but
// Store is a public extension point, so the guarantee has to be the store's.
//
// The error must be the documented sentinel. A CHECK violation surfaces as a raw
// driver error carrying nothing a caller can match on, so the store refuses "" in Go
// before the INSERT and the constraint stands only as the backstop for writes that
// bypass the store.
func TestObjectsCreateRejectsTheEmptyName(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name: "",
		Spec: []byte(`{}`),
	})

	require.ErrorIs(t, err, storeapi.ErrInvalidName)
	assertNoObjectRows(t, store)
}

// The CHECK is the backstop, and it has to hold against SQL the store never saw.
// Driven raw for the same reason the NULL case is, and asserted the same way.
func TestObjectsNameColumnRejectsTheEmptyStringInSQL(t *testing.T) {
	store := newTestStore(t)
	db := store.(*sqliteStore).db

	_, err := db.ExecContext(context.Background(),
		`INSERT INTO objects ("group", kind, name, spec, generation, resource_version, created_at, updated_at)
		 VALUES (?, ?, '', ?, 1, 1, 0, 0)`,
		testGK.Group, testGK.Kind, `{}`)

	require.Error(t, err, "the column rejects the empty string")
	assertNoObjectRows(t, store)
}

// assertNoObjectRows reports that nothing of testGK landed — the driver-independent
// half of a constraint assertion, where the error text is the driver's to change.
func assertNoObjectRows(t *testing.T, store beehive.Store) {
	t.Helper()
	ids, err := store.ObjectsListIDs(context.Background(), testGK)
	require.NoError(t, err)
	assert.Empty(t, ids, "the refused row must not have landed")
}

func TestObjectsCreateAssignsIdentity(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	obj, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name: "world",
		Spec: []byte(`{"name":"world"}`),
	})
	require.NoError(t, err)

	assert.NotZero(t, obj.ID)
	assert.EqualValues(t, 1, obj.Generation, "generation starts at 1")
	assert.NotZero(t, obj.ResourceVersion)
	assert.Nil(t, obj.Status, "status is nil until first write")
	assert.Nil(t, obj.ObservedGeneration)
	assert.Empty(t, obj.Finalizers)
	assert.False(t, obj.CreatedAt.IsZero())
	assert.Equal(t, obj.CreatedAt, obj.UpdatedAt)
	assert.Equal(t, "world", obj.Name)
	// The kind comes from the positional gk, not from a field of the input.
	assert.Equal(t, testGK.Group, obj.Group)
	assert.Equal(t, testGK.Kind, obj.Kind)
}

func TestObjectsCreatePersistsFinalizers(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	want := []string{"kstack.sh/cluster", "kstack.sh/dns"}
	created, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name:       "guarded",
		Spec:       []byte(`{}`),
		Finalizers: want,
	})
	require.NoError(t, err)
	assert.Equal(t, want, created.Finalizers)

	// Round-trips through the JSON column, not just the returned struct.
	reloaded, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, want, reloaded.Finalizers)
}

func TestGetByIdAndName(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name: "world",
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	byID, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, created.ID, byID.ID)

	byName, err := store.ObjectsGetByName(ctx, testGK, "world")
	require.NoError(t, err)
	assert.Equal(t, created.ID, byName.ID)
}

func TestGetNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.ObjectsGet(ctx, 999)
	assert.ErrorIs(t, err, beehive.ErrNotFound)

	_, err = store.ObjectsGetByName(ctx, testGK, "nope")
	assert.ErrorIs(t, err, beehive.ErrNotFound)
}

func TestDuplicateNameRejected(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	mk := func() error {
		_, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
			Name: "dup",
			Spec: []byte(`{}`),
		})
		return err
	}
	require.NoError(t, mk())

	// The sentinel, not a raw driver error. A caller that generates names has to be
	// able to tell "that name is taken, try another" from "the disk is full", and the
	// UNIQUE violation arrives as a modernc *sqlite.Error whose text is the driver's
	// to change.
	require.ErrorIs(t, mk(), storeapi.ErrNameTaken,
		"second create with same name should report the sentinel")
}

// A create that fails on the name must leave nothing behind — the caller's retry
// depends on it, and self-wrapping already rolls the whole statement pair back.
func TestDuplicateNameCreateLandsNothing(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	first, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name: "dup",
		Spec: []byte(`{"v":1}`),
	})
	require.NoError(t, err)

	_, err = store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name: "dup",
		Spec: []byte(`{"v":2}`),
	})
	require.ErrorIs(t, err, storeapi.ErrNameTaken)

	ids, err := store.ObjectsListIDs(ctx, testGK)
	require.NoError(t, err)
	assert.Equal(t, []storeapi.ObjectID{first.ID}, ids, "the losing create left no row")
}

func TestObjectsUpdateSpecBumpsGeneration(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`{"v":1}`),
	})
	require.NoError(t, err)

	updated, _, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"v":2}`), 0)
	require.NoError(t, err)

	assert.EqualValues(t, 2, updated.Generation, "spec change bumps generation")
	assert.Greater(t, updated.ResourceVersion, created.ResourceVersion)
	assert.JSONEq(t, `{"v":2}`, string(updated.Spec))
}

// TestObjectsUpdateSpecIdenticalSpecIsNoOp verifies that re-writing the same spec bytes
// doesn't bump generation or resource_version: an idempotent update must not
// falsely unsettle a converged object or churn the watch cursor.
func TestObjectsUpdateSpecIdenticalSpecIsNoOp(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`{"v":1}`),
	})
	require.NoError(t, err)

	// Settle the object so observed_generation == generation; an idempotent
	// update must leave it settled.
	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{}`), 0))
	settled, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	again, _, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"v":1}`), 0)
	require.NoError(t, err)

	assert.EqualValues(t, created.Generation, again.Generation, "identical spec must not bump generation")
	assert.Equal(t, settled.ResourceVersion, again.ResourceVersion, "identical spec must not bump resource_version")
	require.NotNil(t, again.ObservedGeneration)
	assert.EqualValues(t, again.Generation, *again.ObservedGeneration, "object stays settled after a no-op update")
	// No watcher churn: an idempotent update emits no Modified event.
	probe.expectNone()
}

func TestUpdateStatusRecordsObservedGeneration(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created := newRefObject(t, store)

	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 0))
	updated, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)

	require.NotNil(t, updated.ObservedGeneration)
	assert.EqualValues(t, created.Generation, *updated.ObservedGeneration)
	assert.EqualValues(t, created.Generation, updated.Generation, "status write must not bump generation")
	require.NotNil(t, updated.ObservedAt)
	assert.Greater(t, updated.ResourceVersion, created.ResourceVersion)
	assert.JSONEq(t, `{"msg":"hi"}`, string(updated.Status))
}

// TestSchemaVersionColumnsRoundTrip verifies the opaque per-column schema
// versions: they default to 0, ObjectsCreate persists the caller-set spec version
// (status is nil at create, so its version stays 0), and the version args to
// ObjectsUpdateSpec/UpdateStatus persist and read back independently.
func TestSchemaVersionColumnsRoundTrip(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Defaults: a created object with no spec version set reports 0/0.
	plain := newRefObject(t, store)
	assert.Zero(t, plain.SpecVersion, "spec version defaults to 0")
	assert.Zero(t, plain.StatusVersion, "status version defaults to 0")

	// ObjectsCreate persists the caller-set spec version; status stays 0 (nil at create).
	created, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name:        "v",
		Spec:        []byte(`{}`),
		SpecVersion: 3,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 3, created.SpecVersion)
	assert.Zero(t, created.StatusVersion)

	reread, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.EqualValues(t, 3, reread.SpecVersion, "spec version survives a re-read")
	assert.Zero(t, reread.StatusVersion)

	// UpdateStatus stamps only the status version, leaving spec untouched.
	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{}`), 7))
	withStatus, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.EqualValues(t, 3, withStatus.SpecVersion, "status write must not touch spec version")
	assert.EqualValues(t, 7, withStatus.StatusVersion)

	// ObjectsUpdateSpec stamps only the spec version, leaving status untouched.
	withSpec, _, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"x":1}`), 5)
	require.NoError(t, err)
	assert.EqualValues(t, 5, withSpec.SpecVersion)
	assert.EqualValues(t, 7, withSpec.StatusVersion, "spec write must not touch status version")
}

func TestUpdateStatusRejectsFutureGeneration(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created := newRefObject(t, store)

	// created.Generation is 1; reporting generation 5 is impossible to have seen.
	err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation+4, []byte(`{"msg":"hi"}`), 0)
	require.ErrorIs(t, err, beehive.ErrObservedGenerationFuture)

	// The rejected write must not have landed.
	reread, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.Nil(t, reread.ObservedGeneration, "rejected status write must not record observed generation")
	assert.Empty(t, reread.Status, "rejected status write must not store status")
}

func TestUpdateStatusAcceptsStaleGeneration(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created := newRefObject(t, store)

	bumped, _, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"x":1}`), 0)
	require.NoError(t, err)
	require.EqualValues(t, 2, bumped.Generation)

	// Controller reports it reconciled the now-stale generation 1.
	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{}`), 0))
	updated, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	require.NotNil(t, updated.ObservedGeneration)
	assert.EqualValues(t, created.Generation, *updated.ObservedGeneration)
	assert.Less(t, *updated.ObservedGeneration, updated.Generation,
		"stale observed generation must leave the object unsettled")
}

// TestUpdateStatusIdenticalStatusIsNoOp verifies the content no-op: re-writing
// the same status bytes must not bump resource_version or updated_at, and must
// not emit — downstream controllers wake dependents off a status Modified, so a
// spurious one is a wake storm on every unchanged poll.
func TestUpdateStatusIdenticalStatusIsNoOp(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created := newRefObject(t, store)

	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 0))
	first, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 0))
	again, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)

	assert.Equal(t, first.ResourceVersion, again.ResourceVersion, "identical status must not bump resource_version")
	assert.Equal(t, first.UpdatedAt, again.UpdatedAt, "identical status must not touch updated_at")
	assert.JSONEq(t, `{"msg":"hi"}`, string(again.Status))
	probe.expectNone()
}

func TestUpdateStatusChangedStatusWrites(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created := newRefObject(t, store)

	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 0))
	first, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"bye"}`), 0))
	again, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)

	assert.Greater(t, again.ResourceVersion, first.ResourceVersion, "a real status change bumps resource_version")
	assert.JSONEq(t, `{"msg":"bye"}`, string(again.Status))
	probe.expectWrite()
}

// The future-generation guard is a caller-bug check, not a write guard: it must
// fire on the no-op path too, where there are no new bytes to reject.
func TestUpdateStatusNoOpStillRejectsFutureGeneration(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created := newRefObject(t, store)

	err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 0)
	require.NoError(t, err)

	err = store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation+4, []byte(`{"msg":"hi"}`), 0)
	require.ErrorIs(t, err, beehive.ErrObservedGenerationFuture)
}

// Scoping is unchanged on both branches: a foreign id is ErrWrongKind and a
// missing id ErrNotFound whether or not the status bytes would change.
func TestUpdateStatusScopedOnBothBranches(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created := newRefObject(t, store)
	err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 0)
	require.NoError(t, err)

	for _, status := range [][]byte{[]byte(`{"msg":"hi"}`), []byte(`{"msg":"bye"}`)} {
		err = store.ObjectsUpdateStatus(ctx, beehive.GroupKind{Kind: "Other"}, created.ID, created.Generation, status, 0)
		assert.ErrorIs(t, err, beehive.ErrWrongKind)

		err = store.ObjectsUpdateStatus(ctx, testGK, 999999, 1, status, 0)
		assert.ErrorIs(t, err, beehive.ErrNotFound)
	}
}

// TestUpdateStatusNoOpAdvancesObservedGeneration pins the design decision: a
// content no-op still advances observed_generation/observed_at. The handshake
// records that the controller ran, not what it wrote — stranding it would leave
// the object unsettled and re-enqueued by every full pass. The advance is a real
// transition (the object just settled at a new generation), so it bumps
// resource_version and emits, or a watcher gating on convergence would sit
// blind until the next full pass. The repeat call, already settled, is silent.
func TestUpdateStatusNoOpAdvancesObservedGeneration(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created := newRefObject(t, store)

	err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 0)
	require.NoError(t, err)

	// New spec, same status: the reconcile observed generation 2 but wrote no
	// new content.
	bumped, _, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"x":1}`), 0)
	require.NoError(t, err)
	require.EqualValues(t, 2, bumped.Generation)

	probe := newWriteProbe(t, store)

	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, bumped.Generation, []byte(`{"msg":"hi"}`), 0))
	again, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)

	require.NotNil(t, again.ObservedGeneration)
	assert.EqualValues(t, bumped.Generation, *again.ObservedGeneration,
		"a content no-op still records the generation the controller observed")
	assert.Greater(t, again.ResourceVersion, bumped.ResourceVersion,
		"settling at a new generation is a real transition, so it bumps resource_version")
	assert.Equal(t, bumped.UpdatedAt, again.UpdatedAt, "the handshake write doesn't touch updated_at")
	// The convergence is observable: a consumer scanning the write log finds the
	// row and re-reads it at the generation the controller settled.
	assert.Equal(t, again.ResourceVersion, probe.expectWrite().ResourceVersion)
	reread, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	require.NotNil(t, reread.ObservedGeneration)
	assert.EqualValues(t, bumped.Generation, *reread.ObservedGeneration)

	// It really settled: the owed-pass backstop no longer sees it.
	unsettled, err := store.ObjectsListUnsettledIDs(ctx, testGK)
	require.NoError(t, err)
	assert.NotContains(t, unsettled, created.ID)

	// And a second identical call, now with the generation already recorded,
	// writes nothing at all.
	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, bumped.Generation, []byte(`{"msg":"hi"}`), 0))
	third, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, again.ObservedAt, third.ObservedAt, "no observed_at churn once the generation is recorded")
	assert.Equal(t, again.ResourceVersion, third.ResourceVersion)
	probe.expectNone()
}

// TestUpdateStatusNoOpKeepsNewerObservedGeneration pins the handshake as
// forward-only. Two reconciles can be in flight for one object and the older can
// commit last; a content no-op reporting a generation already covered by a newer
// recorded one must stay silent, not write observed_generation backwards —
// regressing it would re-unsettle a converged object for the owed-pass backstop and
// emit a Modified that wakes every dependent.
func TestUpdateStatusNoOpKeepsNewerObservedGeneration(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created := newRefObject(t, store)

	bumped, _, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"x":1}`), 0)
	require.NoError(t, err)
	require.EqualValues(t, 2, bumped.Generation)

	// The newer reconcile lands first and settles the object at generation 2.
	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, bumped.Generation, []byte(`{"msg":"hi"}`), 0))
	settled, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	require.NotNil(t, settled.ObservedGeneration)
	require.EqualValues(t, bumped.Generation, *settled.ObservedGeneration)

	probe := newWriteProbe(t, store)

	// The straggler, still holding generation 1, reports identical status.
	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 0))
	late, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)

	require.NotNil(t, late.ObservedGeneration)
	assert.EqualValues(t, bumped.Generation, *late.ObservedGeneration,
		"a stale report must not roll the handshake back")
	assert.Equal(t, settled.ResourceVersion, late.ResourceVersion, "nothing moved, so no version bump")
	assert.Equal(t, settled.ObservedAt, late.ObservedAt)
	probe.expectNone()

	// The object is still converged as far as the owed-pass backstop is concerned.
	unsettled, err := store.ObjectsListUnsettledIDs(ctx, testGK)
	require.NoError(t, err)
	assert.NotContains(t, unsettled, created.ID)
}

// TestUpdateStatusChangedStaleGenerationUnsettles is the content-changed
// counterpart, and pins the opposite behavior on purpose. Here the stale reporter
// overwrote the status with content derived from an older spec, so its generation
// is written back verbatim: the object goes unsettled and the owed-pass backstop
// re-derives the content. Clamping it forward — correct on the no-op path, where
// identical bytes mean there is nothing to heal — would pin stale status as
// converged with nothing left to revisit it.
func TestUpdateStatusChangedStaleGenerationUnsettles(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created := newRefObject(t, store)

	bumped, _, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"x":1}`), 0)
	require.NoError(t, err)
	require.EqualValues(t, 2, bumped.Generation)

	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, bumped.Generation, []byte(`{"msg":"hi"}`), 0))
	settled, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	require.NotNil(t, settled.ObservedGeneration)
	require.EqualValues(t, bumped.Generation, *settled.ObservedGeneration)

	// The straggler, still holding generation 1, writes different status.
	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"stale"}`), 0))
	late, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)

	assert.JSONEq(t, `{"msg":"stale"}`, string(late.Status), "the status content lands")
	require.NotNil(t, late.ObservedGeneration)
	assert.EqualValues(t, created.Generation, *late.ObservedGeneration,
		"the stale generation is recorded verbatim, unlike on the no-op path")
	assert.Greater(t, late.ResourceVersion, settled.ResourceVersion, "a content write is a real transition")

	// The point of not clamping: the object is unsettled again, so the full pass
	// backstop re-reconciles it and the stale content gets re-derived.
	unsettled, err := store.ObjectsListUnsettledIDs(ctx, testGK)
	require.NoError(t, err)
	assert.Contains(t, unsettled, created.ID)
}

// TestCrossVersionWriteIsNotANoOp pins the version gate on the content no-op.
// Convert-on-read leaves a row tagged at the version it was written in, so bytes
// arriving from a caller at a *newer* version are in a different shape: equal
// bytes there can carry different values, and the byte compare that decides the
// no-op is meaningless across that gap. Suppressing such a write would change
// what every later read decodes while reporting changed=false, bumping no
// resource_version and emitting nothing — so no watcher learns and the client
// skips the controller wake. A version mismatch therefore takes the content path:
// it stamps, bumps, and emits, exactly like any other real write.
func TestCrossVersionWriteIsNotANoOp(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name:        uniqueName(),
		Spec:        []byte(`{"v":1}`),
		SpecVersion: 1,
	})
	require.NoError(t, err)
	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 1))
	settled, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	// Same status bytes, newer status schema version: a real write, announced.
	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 2))
	statusStamped, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, 2, statusStamped.StatusVersion, "the newer status version lands")
	assert.Equal(t, 1, statusStamped.SpecVersion, "and leaves the spec version alone")
	assert.Greater(t, statusStamped.ResourceVersion, settled.ResourceVersion,
		"a shape change is watch-visible even with identical bytes")
	probe.expectWrite()

	// Same spec bytes, newer spec schema version: likewise a real write, so the
	// generation bump wakes the controller to re-derive from the reinterpreted spec.
	specStamped, _, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"v":1}`), 3)
	require.NoError(t, err)
	assert.Equal(t, 3, specStamped.SpecVersion, "the newer spec version lands")
	assert.Equal(t, 2, specStamped.StatusVersion, "and leaves the status version alone")
	assert.Greater(t, specStamped.Generation, created.Generation, "and unsettles the object")
	assert.Greater(t, specStamped.ResourceVersion, statusStamped.ResourceVersion)
	probe.expectWrite()

	// Both stamps survive a re-read.
	reread, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, 3, reread.SpecVersion)
	assert.Equal(t, 2, reread.StatusVersion)
}

// TestSameVersionNoOpWritesNothing is the other side of the gate: identical bytes
// at the version the row already carries stay silent, since there is genuinely
// nothing to relay.
func TestSameVersionNoOpWritesNothing(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name:        uniqueName(),
		Spec:        []byte(`{"v":1}`),
		SpecVersion: 1,
	})
	require.NoError(t, err)
	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 1))
	settled, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 1))
	again, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, settled.ResourceVersion, again.ResourceVersion, "no resource_version bump")

	sameSpec, _, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"v":1}`), 1)
	require.NoError(t, err)
	assert.EqualValues(t, created.Generation, sameSpec.Generation, "no generation bump")
	assert.Equal(t, settled.ResourceVersion, sameSpec.ResourceVersion, "no resource_version bump")

	probe.expectNone()
}

// TestNoOpWritesNeverStampSchemaVersionDownward pins the direction of the
// re-stamp. On a content no-op the stored bytes are the ones staying put, so
// they're at the row's version, not the caller's — an older build (or one that
// lost the kind's migrator, reporting 0) re-applying identical content must not
// relabel newer data as older. If it did, the newer build would read from <
// current and convert already-converted bytes instead of getting the downgrade
// error the read path owes it.
func TestNoOpWritesNeverStampSchemaVersionDownward(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name:        uniqueName(),
		Spec:        []byte(`{"v":3}`),
		SpecVersion: 3,
	})
	require.NoError(t, err)
	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 3))
	settled, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, 3, settled.StatusVersion)

	probe := newWriteProbe(t, store)

	// A build that lost the kind's migrator (reporting 0) has no version opinion:
	// the write goes through and leaves the tag alone.
	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 0))
	stale, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, 3, stale.StatusVersion, "a no-op status write never stamps backwards")

	staleSpec, _, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"v":3}`), 0)
	require.NoError(t, err)
	assert.Equal(t, 3, staleSpec.SpecVersion, "a no-op spec write never stamps backwards (0 = no migrator)")

	// The row is untouched: same versions on re-read, nothing announced.
	reread, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, 3, reread.SpecVersion)
	assert.Equal(t, 3, reread.StatusVersion)
	assert.Equal(t, settled.ResourceVersion, reread.ResourceVersion)
	probe.expectNone()
}

// TestNoOpWriteStampsUpwardWhileConverging covers the crossing case: the
// convergence branch (identical bytes, new observed generation) emits, and its
// stamp obeys the same upward-only rule.
func TestNoOpWriteStampsUpwardWhileConverging(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name:        uniqueName(),
		Spec:        []byte(`{}`),
		SpecVersion: 3,
	})
	require.NoError(t, err)
	err = store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 3)
	require.NoError(t, err)

	bumped, _, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"x":1}`), 3)
	require.NoError(t, err)

	// Converging at the new generation with identical status and no version opinion.
	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, bumped.Generation, []byte(`{"msg":"hi"}`), 0))
	got, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, 3, got.StatusVersion, "the convergence write doesn't stamp backwards either")
	require.NotNil(t, got.ObservedGeneration)
	assert.EqualValues(t, bumped.Generation, *got.ObservedGeneration)
}

// TestWriteRejectsSchemaVersionDowngrade pins the other half of the stamp rule,
// on *both* branches. A non-zero version below the row's is a real, wrong opinion
// — not the "no migrator" 0 — and the read path already refuses to decode such a
// row (from > current), so the caller could not have obtained the object it is
// writing back. Clamping it silently, or letting the content path stamp it down,
// would leave newer bytes labelled older and make every later read convert
// already-converted data.
func TestWriteRejectsSchemaVersionDowngrade(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name:        uniqueName(),
		Spec:        []byte(`{"v":3}`),
		SpecVersion: 3,
	})
	require.NoError(t, err)
	err = store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 3)
	require.NoError(t, err)

	// Content no-op and real content change, spec and status alike.
	_, _, err = store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"v":3}`), 1)
	require.ErrorIs(t, err, beehive.ErrSchemaVersionDowngrade)
	_, _, err = store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"v":9}`), 1)
	require.ErrorIs(t, err, beehive.ErrSchemaVersionDowngrade)
	err = store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 1)
	require.ErrorIs(t, err, beehive.ErrSchemaVersionDowngrade)
	err = store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"bye"}`), 1)
	require.ErrorIs(t, err, beehive.ErrSchemaVersionDowngrade)

	// Nothing landed: the row still holds its v3 bytes at v3.
	reread, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, 3, reread.SpecVersion)
	assert.Equal(t, 3, reread.StatusVersion)
	assert.JSONEq(t, `{"v":3}`, string(reread.Spec))
	assert.JSONEq(t, `{"msg":"hi"}`, string(reread.Status))
}

// TestContentWriteWithNoMigratorKeepsSchemaVersion is the finding this rule was
// written for: a build with no migrator (version 0) writing *changed* bytes must
// not zero the row's tag. convertBlob treats current == 0 as identity, so such a
// build decodes v3 bytes untouched and marshals them back — stamping 0 would make
// a later build with the migrator restored convert v3 bytes from 0.
func TestContentWriteWithNoMigratorKeepsSchemaVersion(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name:        uniqueName(),
		Spec:        []byte(`{"v":3}`),
		SpecVersion: 3,
	})
	require.NoError(t, err)
	err = store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 3)
	require.NoError(t, err)

	updated, _, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"v":4}`), 0)
	require.NoError(t, err)
	require.Greater(t, updated.Generation, created.Generation, "precondition: a real content write")
	assert.Equal(t, 3, updated.SpecVersion, "a content write with no migrator keeps the stored version")

	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, created.ID, updated.Generation, []byte(`{"msg":"bye"}`), 0))
	settled, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, 3, settled.StatusVersion, "same on the status half")
}

func TestListObjects(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	for _, n := range []string{"a", "b", "c"} {
		_, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
			Name: n,
			Spec: []byte(`{}`),
		})
		require.NoError(t, err)
	}
	// A different kind must not leak into the list.
	_, err := store.ObjectsCreate(ctx, beehive.GroupKind{Group: "", Kind: "Other"}, beehive.ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	list, err := store.ObjectsList(ctx, testGK)
	require.NoError(t, err)
	require.Len(t, list, 3)

	var names []string
	for _, o := range list {
		names = append(names, o.Name)
	}
	assert.Equal(t, []string{"a", "b", "c"}, names, "ordered by id")
}

func TestResourceVersionIsMonotonic(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	a, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name: "a",
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	b, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name: "b",
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	assert.Greater(t, b.ResourceVersion, a.ResourceVersion, "each create takes the next cursor value")

	// A later mutation advances the global cursor past every prior write.
	updated, _, err := store.ObjectsUpdateSpec(ctx, testGK, a.ID, []byte(`{"v":2}`), 0)
	require.NoError(t, err)
	assert.Greater(t, updated.ResourceVersion, b.ResourceVersion)
}

func TestResourceVersionNotReusedAfterDelete(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	a, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name: "a",
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	b, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name: "b",
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	// Delete the highest-versioned row, then write again. The cursor must not
	// fall back to b's version — it only ever moves forward.
	require.NoError(t, store.ObjectsDelete(ctx, b.ID))

	updated, _, err := store.ObjectsUpdateSpec(ctx, testGK, a.ID, []byte(`{"v":2}`), 0)
	require.NoError(t, err)
	assert.Greater(t, updated.ResourceVersion, b.ResourceVersion,
		"a deleted row's resource_version must never be reused")
}

func TestRepeatDeletionRequestsCreateDoesNotBumpResourceVersion(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created := newRefObject(t, store)

	res, err := store.DeletionRequests().Create(ctx, testGK, created.ID)
	require.NoError(t, err)
	assert.True(t, res.Marked, "first call is a real change")
	first, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.Greater(t, first.ResourceVersion, created.ResourceVersion,
		"the first request is a real change and bumps the cursor")

	// A repeat request changes no deletion state, so it must be a no-op: same
	// resource_version, same updated_at, no spurious watch/CAS churn.
	res, err = store.DeletionRequests().Create(ctx, testGK, created.ID)
	require.NoError(t, err)
	assert.False(t, res.Marked, "repeat call is an idempotent no-op")
	second, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, first.ResourceVersion, second.ResourceVersion,
		"an idempotent repeat must not bump resource_version")
	assert.Equal(t, first.UpdatedAt, second.UpdatedAt)
}

// seqValue reads the global write cursor's counter directly. Tests use it to tell
// "this write consumed a version" from "the row's version did not move", which the
// object row alone cannot distinguish: a drawn-but-unused value leaves no trace on it.
func seqValue(t *testing.T, store *sqliteStore) int64 {
	t.Helper()
	var v int64
	require.NoError(t, store.db.QueryRowContext(context.Background(),
		`SELECT value FROM resource_version_seq WHERE id = 1`).Scan(&v))
	return v
}

// A mark stamps the row with exactly the version it then commits to the counter, and
// a mark blocked by the IS NULL guard draws nothing at all. The second half is the
// point: the version is drawn lazily, so the already-pending path — the steady state
// for a controller that idempotently deletes a child — writes no counter page and
// leaves no gap in the cursor.
func TestDeletionMarkDrawsAVersionOnlyWhenItStamps(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)

	before := seqValue(t, store)
	res, err := store.DeletionRequests().Create(ctx, testGK, obj.ID)
	require.NoError(t, err)
	require.True(t, res.Marked)

	marked, err := store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)
	after := seqValue(t, store)
	assert.Equal(t, before+1, after, "a stamped mark consumes exactly one version")
	assert.Equal(t, after, marked.ResourceVersion,
		"the row carries the value the counter committed, not one beside it")

	// The repeat is blocked by the guard, so nothing is drawn and no gap appears.
	res, err = store.DeletionRequests().Create(ctx, testGK, obj.ID)
	require.NoError(t, err)
	require.False(t, res.Marked)
	assert.Equal(t, after, seqValue(t, store), "a guard-blocked mark draws no version")

	// Same for a mark that matches no row at all, via the other keying.
	_, err = store.DeletionRequests().CreateByName(ctx, testGK, "no-such-name")
	require.ErrorIs(t, err, beehive.ErrNotFound)
	assert.Equal(t, after, seqValue(t, store), "a mark that matches nothing draws none either")
}

// ObjectsGetMeta returns the same row as ObjectsGet but skips assembling
// conditions, which the metadata-only GC and edge callers never read.
func TestGetObjectMetaSkipsConditions(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created := newRefObject(t, store)
	err := store.Conditions().Set(ctx, testGK, created.ID,
		storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	full, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	require.Len(t, full.Conditions, 1, "ObjectsGet assembles conditions")

	meta, err := store.ObjectsGetMeta(ctx, created.ID)
	require.NoError(t, err)
	assert.Nil(t, meta.Conditions, "ObjectsGetMeta must not assemble conditions")
	// Otherwise the same row: id and version match the conditions-laden read.
	assert.Equal(t, full.ID, meta.ID)
	assert.Equal(t, full.ResourceVersion, meta.ResourceVersion)
}

// DeletionRequestsCreateFromOwner marks every owned child for deletion and returns them all;
// a re-cascade over already-deleting children writes nothing (the O(1) steady
// state) yet still returns them for requeue.
func TestDeletionRequestsCreateFromOwnerCascadesThenIsNoOp(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	mk := func() storeapi.ObjectID { return newEventObject(t, store) }
	owner, childA, childB := mk(), mk(), mk()
	require.NoError(t, addEdge(ctx, store, childA, owner, beehive.RelationOwnedBy))
	require.NoError(t, addEdge(ctx, store, childB, owner, beehive.RelationOwnedBy))

	// Probe from here, so each cascade's writes are isolated from the setup's.
	probe := newWriteProbe(t, store)

	// First cascade marks both children (one write each) and returns both.
	got, err := store.DeletionRequests().CreateFromOwner(ctx, owner)
	require.NoError(t, err)
	require.Len(t, got.Children, 2)
	assert.True(t, got.Children[0].Marked && got.Children[1].Marked, "this call stamped both")
	probe.expectWrites(2)
	a1, err := store.ObjectsGetMeta(ctx, childA)
	require.NoError(t, err)
	require.NotNil(t, a1.DeletionRequestedAt, "child A marked for deletion")
	b1, err := store.ObjectsGetMeta(ctx, childB)
	require.NoError(t, err)
	require.NotNil(t, b1.DeletionRequestedAt, "child B marked for deletion")

	// Second cascade over the now-deleting children: still returns both, but writes
	// nothing — no resource_version churn, and nothing new in the write log.
	got2, err := store.DeletionRequests().CreateFromOwner(ctx, owner)
	require.NoError(t, err)
	require.Len(t, got2.Children, 2)
	assert.False(t, got2.Children[0].Marked || got2.Children[1].Marked, "the repeat stamped neither")
	probe.expectNone()
	a2, err := store.ObjectsGetMeta(ctx, childA)
	require.NoError(t, err)
	assert.Equal(t, a1.ResourceVersion, a2.ResourceVersion, "no re-mark, no rv churn")
	b2, err := store.ObjectsGetMeta(ctx, childB)
	require.NoError(t, err)
	assert.Equal(t, b1.ResourceVersion, b2.ResourceVersion)
}

// The delete push gates on the store's report that *this* call stamped the row,
// so a losing racer must report false. The probe is outside the transaction, so
// racers can clear it together and meet at the guarded UPDATE.
func TestDeletionRequestsCreateStampsOnceUnderConcurrency(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	// Released together, so every racer clears requestDeletion's pre-transaction
	// probe before any of them commits. Staggered, the probe filters the losers
	// and the guarded UPDATE never has to.
	const racers = 4
	start := make(chan struct{})
	var stamped atomic.Int32
	var wg sync.WaitGroup
	for range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := store.DeletionRequests().Create(ctx, testGK, id)
			assert.NoError(t, err)
			if res.Marked {
				stamped.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	assert.Equal(t, int32(1), stamped.Load(), "exactly one caller stamped the row")
}

// DeletionRequestsCreateFromOwner's child lookup must ride the idx_edges_to index, not scan
// the edges table — that index alignment is the point of the single-query cascade.
// COVERING is asserted too: edges is WITHOUT ROWID, so idx_edges_to implicitly
// carries from_id and the probe never touches the table. That property lives in
// the table's storage class, not the index definition, so dropping WITHOUT ROWID
// would give it back with nothing in the schema looking different — this is the
// only place that would notice.
func TestDeletionRequestsCreateFromOwnerUsesRefsIndex(t *testing.T) {
	store := newTestStore(t).(*sqliteStore)
	ctx := context.Background()

	rows, err := store.db.QueryContext(ctx, `
		EXPLAIN QUERY PLAN
		SELECT o.id, o."group", o.kind, o.deletion_requested_at
		FROM edges r JOIN objects o ON o.id = r.from_id
		WHERE r.to_id = ? AND r.relation = ?
		ORDER BY o.id`, int64(1), string(storeapi.RelationOwnedBy))
	require.NoError(t, err)
	defer rows.Close()

	var plan string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notused, &detail))
		plan += detail + "\n"
	}
	require.NoError(t, rows.Err())
	assert.Contains(t, plan, "COVERING INDEX idx_edges_to",
		"child lookup must use idx_edges_to as a covering index:\n"+plan)
}

func TestDeleteFinalizerRemovesOneAndEmits(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name:       uniqueName(),
		Spec:       []byte(`{}`),
		Finalizers: []string{"a", "b"},
	})
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	// Removing a present finalizer is a real change: only that finalizer drops,
	// resource_version bumps, and watchers see a Modified event.
	_, err = store.FinalizersDelete(ctx, testGK, created.ID, "a")
	require.NoError(t, err)
	got, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, got.Finalizers)
	assert.Greater(t, got.ResourceVersion, created.ResourceVersion)

	assert.Equal(t, got.ResourceVersion, probe.expectWrite().ResourceVersion,
		"the finalizer removal is observable in the write log")

	// Persisted, not just reflected in the returned struct.
	reloaded, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, reloaded.Finalizers)
}

func TestDeleteFinalizerAbsentIsNoOp(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name:       uniqueName(),
		Spec:       []byte(`{}`),
		Finalizers: []string{"a"},
	})
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	// Removing a finalizer that isn't present changes nothing: the list is intact,
	// resource_version is unbumped, and no event fires (a watcher would otherwise
	// see a spurious diff).
	_, err = store.FinalizersDelete(ctx, testGK, created.ID, "missing")
	require.NoError(t, err)
	got, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, got.Finalizers)
	assert.Equal(t, created.ResourceVersion, got.ResourceVersion)
	probe.expectNone()
}

func TestDeleteFinalizerMissingObject(t *testing.T) {
	store := newTestStore(t)
	_, err := store.FinalizersDelete(context.Background(), testGK, 999, "a")
	assert.ErrorIs(t, err, beehive.ErrNotFound)
}

// clearedLast is the gate behind the collect push, so it must be true for the one
// transition that frees a blocked collect and false for every neighbour of it.
func TestDeleteFinalizerReportsClearedLast(t *testing.T) {
	tests := []struct {
		name       string
		finalizers []string
		deleting   bool
		remove     string
		want       bool
	}{
		{"last on a deleting object", []string{"a"}, true, "a", true},
		{"not the last on a deleting object", []string{"a", "b"}, true, "a", false},
		{"last on a live object", []string{"a"}, false, "a", false},
		{"absent on a deleting object", []string{"a"}, true, "missing", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newTestStore(t)
			ctx := context.Background()

			created, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
				Name:       uniqueName(),
				Spec:       []byte(`{}`),
				Finalizers: tt.finalizers,
			})
			require.NoError(t, err)
			if tt.deleting {
				_, err := store.DeletionRequests().Create(ctx, testGK, created.ID)
				require.NoError(t, err)
			}

			clearedLast, err := store.FinalizersDelete(ctx, testGK, created.ID, tt.remove)
			require.NoError(t, err)
			assert.Equal(t, tt.want, clearedLast)
		})
	}
}

func TestListOutgoingRefs(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	from := newRefObject(t, store)
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	require.NoError(t, addEdge(ctx, store, from.ID, a.ID, beehive.RelationOwnedBy))
	require.NoError(t, addEdge(ctx, store, from.ID, b.ID, beehive.RelationDependsOn))
	// A second edge to the same target via another relation must not duplicate it.
	require.NoError(t, addEdge(ctx, store, from.ID, a.ID, beehive.RelationDependsOn))

	refs, err := store.EdgesListOutgoing(ctx, from.ID)
	require.NoError(t, err)
	var ids []beehive.ObjectID
	for _, r := range refs {
		ids = append(ids, r.ID)
	}
	assert.Equal(t, []beehive.ObjectID{a.ID, b.ID}, ids, "distinct targets, ordered by id")

	// An object that points at nothing has no referents.
	refs, err = store.EdgesListOutgoing(ctx, a.ID)
	require.NoError(t, err)
	assert.Empty(t, refs)
}

func TestListOutgoingRefsByRelation(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	from := newRefObject(t, store)
	owner := newRefObject(t, store)
	dep := newRefObject(t, store)

	require.NoError(t, addEdge(ctx, store, from.ID, owner.ID, beehive.RelationOwnedBy))
	require.NoError(t, addEdge(ctx, store, from.ID, dep.ID, beehive.RelationDependsOn))

	owned, err := store.Edges().ListOutgoingByRelation(ctx, from.ID, beehive.RelationOwnedBy)
	require.NoError(t, err)
	assert.Equal(t, []beehive.ObjectID{owner.ID}, refIDs(owned), "only the owned_by target")

	deps, err := store.Edges().ListOutgoingByRelation(ctx, from.ID, beehive.RelationDependsOn)
	require.NoError(t, err)
	assert.Equal(t, []beehive.ObjectID{dep.ID}, refIDs(deps), "only the depends_on target")

	none, err := store.Edges().ListOutgoingByRelation(ctx, owner.ID, beehive.RelationOwnedBy)
	require.NoError(t, err)
	assert.Empty(t, none, "no matching edges")
}

func TestGroupOutgoingRefsByID(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	owner := newRefObject(t, store)
	childA := newRefObject(t, store)
	childB := newRefObject(t, store)
	loner := newRefObject(t, store) // owns nothing through owned_by

	require.NoError(t, addEdge(ctx, store, childA.ID, owner.ID, beehive.RelationOwnedBy))
	require.NoError(t, addEdge(ctx, store, childB.ID, owner.ID, beehive.RelationOwnedBy))
	// A depends_on edge the relation filter must exclude.
	require.NoError(t, addEdge(ctx, store, childA.ID, loner.ID, beehive.RelationDependsOn))

	got, err := store.Edges().GroupOutgoingByID(ctx,
		[]beehive.ObjectID{childA.ID, childB.ID, loner.ID}, beehive.RelationOwnedBy)
	require.NoError(t, err)
	assert.Equal(t, []beehive.ObjectID{owner.ID}, refIDs(got[childA.ID]))
	assert.Equal(t, []beehive.ObjectID{owner.ID}, refIDs(got[childB.ID]))
	_, ok := got[loner.ID]
	assert.False(t, ok, "a source with no matching edge is absent from the map")

	empty, err := store.Edges().GroupOutgoingByID(ctx, nil, beehive.RelationOwnedBy)
	require.NoError(t, err)
	assert.Empty(t, empty, "empty input short-circuits to an empty map")
}

// TestGroupOutgoingRefsByIDChunks shrinks the chunk size so a modest id list
// spans several queries, proving edgesByIDs stays under SQLite's bound-parameter
// limit and merges every chunk's rows into one map.
func TestGroupOutgoingRefsByIDChunks(t *testing.T) {
	defer func(n int) { idChunkSize = n }(idChunkSize)
	idChunkSize = 2 // 5 ids -> 3 chunks (2, 2, 1)

	store := newRawStore(t)
	ctx := context.Background()
	owner := newRefObject(t, store)
	var children []beehive.ObjectID
	for i := 0; i < 5; i++ {
		c := newRefObject(t, store)
		require.NoError(t, addEdge(ctx, store, c.ID, owner.ID, beehive.RelationOwnedBy))
		children = append(children, c.ID)
	}

	got, err := store.Edges().GroupOutgoingByID(ctx, children, beehive.RelationOwnedBy)
	require.NoError(t, err)
	require.Len(t, got, len(children), "every id resolved across all chunks")
	for _, id := range children {
		assert.Equal(t, []beehive.ObjectID{owner.ID}, refIDs(got[id]))
	}
}

func TestDeleteFinalizingDependsOnRefs(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	target := newRefObject(t, store)
	deletingDep := newRefObject(t, store)
	liveDep := newRefObject(t, store)
	owned := newRefObject(t, store)

	require.NoError(t, addEdge(ctx, store, deletingDep.ID, target.ID, beehive.RelationDependsOn))
	require.NoError(t, addEdge(ctx, store, liveDep.ID, target.ID, beehive.RelationDependsOn))
	require.NoError(t, addEdge(ctx, store, owned.ID, target.ID, beehive.RelationOwnedBy))
	// A self-dependency the GC must also be able to clear.
	require.NoError(t, addEdge(ctx, store, target.ID, target.ID, beehive.RelationDependsOn))

	// The target and the finalizing dependent and the owned child are deleting;
	// the live dependent is not.
	for _, id := range []beehive.ObjectID{target.ID, deletingDep.ID, owned.ID} {
		_, err := store.DeletionRequests().Create(ctx, testGK, id)
		require.NoError(t, err)
	}

	require.NoError(t, store.Edges().DeleteFinalizingDependsOn(ctx, target.ID))

	// depends_on edges from finalizing sources (including the self-edge) are gone.
	assert.Equal(t, 0, countEdges(t, store, deletingDep.ID, target.ID, "depends_on"))
	assert.Equal(t, 0, countEdges(t, store, target.ID, target.ID, "depends_on"))
	// A live dependent's edge is preserved — it still legitimately blocks deletion.
	assert.Equal(t, 1, countEdges(t, store, liveDep.ID, target.ID, "depends_on"))
	// owned_by is never touched here; it clears only when the child is removed.
	assert.Equal(t, 1, countEdges(t, store, owned.ID, target.ID, "owned_by"))
}

func TestHasIncomingRefs(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	owner := newRefObject(t, store)
	child := newRefObject(t, store)

	has, err := store.Edges().HasIncoming(ctx, owner.ID)
	require.NoError(t, err)
	assert.False(t, has, "no edges yet")

	require.NoError(t, addEdge(ctx, store, child.ID, owner.ID, beehive.RelationOwnedBy))

	has, err = store.Edges().HasIncoming(ctx, owner.ID)
	require.NoError(t, err)
	assert.True(t, has, "owner is referenced by the child")

	has, err = store.Edges().HasIncoming(ctx, child.ID)
	require.NoError(t, err)
	assert.False(t, has, "child is the source, not a target")
}

func TestHasIncomingRefsIgnoresFinalizingDependent(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	target := newRefObject(t, store)
	dep := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, dep.ID, target.ID, beehive.RelationDependsOn))

	// A live dependent has a claim: it counts.
	has, err := store.Edges().HasIncoming(ctx, target.ID)
	require.NoError(t, err)
	assert.True(t, has)

	// Once the dependent is itself finalizing, its claim is void — it's going away.
	_, err = store.DeletionRequests().Create(ctx, testGK, dep.ID)
	require.NoError(t, err)
	has, err = store.Edges().HasIncoming(ctx, target.ID)
	require.NoError(t, err)
	assert.False(t, has, "a finalizing dependent does not count as a referrer")

	// But a finalizing owned child still counts: the foreground cascade must wait
	// for it to be physically removed.
	child := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, child.ID, target.ID, beehive.RelationOwnedBy))
	_, err = store.DeletionRequests().Create(ctx, testGK, child.ID)
	require.NoError(t, err)
	has, err = store.Edges().HasIncoming(ctx, target.ID)
	require.NoError(t, err)
	assert.True(t, has, "a finalizing owned child still blocks deletion")
}

// Marking a dependent lifts the RESTRICT its depends_on edge held, through the
// discount above, so the mark reports the target it unblocked.
func TestDeletionRequestReportsTheTargetItUnblocks(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	target := newRefObject(t, store)
	dep := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, dep.ID, target.ID, beehive.RelationDependsOn))
	_, err := store.DeletionRequests().Create(ctx, testGK, target.ID)
	require.NoError(t, err)

	res, err := store.DeletionRequests().Create(ctx, testGK, dep.ID)
	require.NoError(t, err)
	require.True(t, res.Marked)
	assert.Equal(t, []beehive.ObjectID{target.ID}, refIDs(res.Unblocked))
}

// A cascade marks children the same way a delete marks one object, so it lifts
// the same RESTRICTs and owes the same report — one query for the whole level.
func TestCascadeReportsTheTargetsItsChildrenUnblock(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	owner := newRefObject(t, store)
	child := newRefObject(t, store)
	target := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, child.ID, owner.ID, beehive.RelationOwnedBy))
	require.NoError(t, addEdge(ctx, store, child.ID, target.ID, beehive.RelationDependsOn))
	_, err := store.DeletionRequests().Create(ctx, testGK, target.ID)
	require.NoError(t, err)

	res, err := store.DeletionRequests().CreateFromOwner(ctx, owner.ID)
	require.NoError(t, err)
	require.Len(t, res.Children, 1)
	assert.Equal(t, []beehive.ObjectID{target.ID}, refIDs(res.Unblocked))

	// The re-cascade marks nothing, so it lifted nothing.
	res, err = store.DeletionRequests().CreateFromOwner(ctx, owner.ID)
	require.NoError(t, err)
	assert.Empty(t, res.Unblocked, "a child already deleting discounted its edge long ago")
}

// A cascade level is unbounded, so the read chunks; every chunk's targets have to
// survive into the one result.
func TestCascadeReportsUnblockedTargetsAcrossChunks(t *testing.T) {
	defer func(n int) { idChunkSize = n }(idChunkSize)
	idChunkSize = 2 // 5 children -> 3 chunks (2, 2, 1)

	store := newRawStore(t)
	ctx := context.Background()
	owner := newRefObject(t, store)
	var targets []beehive.ObjectID
	for range 5 {
		child, target := newRefObject(t, store), newRefObject(t, store)
		require.NoError(t, addEdge(ctx, store, child.ID, owner.ID, beehive.RelationOwnedBy))
		require.NoError(t, addEdge(ctx, store, child.ID, target.ID, beehive.RelationDependsOn))
		_, err := store.DeletionRequests().Create(ctx, testGK, target.ID)
		require.NoError(t, err)
		targets = append(targets, target.ID)
	}

	res, err := store.DeletionRequests().CreateFromOwner(ctx, owner.ID)
	require.NoError(t, err)
	assert.ElementsMatch(t, targets, refIDs(res.Unblocked))
}

// A self-edge blocks nothing — the object's own mark already queues it — so
// reporting it would push the same object twice, as the waker's skip avoids.
func TestDeletionRequestReportsNoSelfEdge(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, obj.ID, obj.ID, beehive.RelationDependsOn))

	res, err := store.DeletionRequests().Create(ctx, testGK, obj.ID)
	require.NoError(t, err)
	require.True(t, res.Marked)
	assert.Empty(t, res.Unblocked)
}

func TestMutatorsReturnNotFoundForMissingTarget(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	const missing beehive.ObjectID = 999

	ops := map[string]func() error{
		"ObjectsUpdateSpec": func() error {
			_, _, err := store.ObjectsUpdateSpec(ctx, testGK, missing, []byte(`{}`), 0)
			return err
		},
		"UpdateStatus": func() error {
			err := store.ObjectsUpdateStatus(ctx, testGK, missing, 1, []byte(`{}`), 0)
			return err
		},
		"DeletionRequestsCreate": func() error {
			_, err := store.DeletionRequests().Create(ctx, testGK, missing)
			return err
		},
		// Keyed by a name no row holds, so here ErrNotFound carries its full meaning:
		// nothing of this kind is named that.
		"DeletionRequestsCreateByName": func() error {
			_, err := store.DeletionRequests().CreateByName(ctx, testGK, "never-created")
			return err
		},
	}
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			assert.ErrorIs(t, op(), beehive.ErrNotFound)
		})
	}
}

func TestDeletionRequestsCreateIsIdempotent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created := newRefObject(t, store)

	_, err := store.DeletionRequests().Create(ctx, testGK, created.ID)
	require.NoError(t, err)
	first, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	require.NotNil(t, first.DeletionRequestedAt)

	_, err = store.DeletionRequests().Create(ctx, testGK, created.ID)
	require.NoError(t, err)
	second, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	require.NotNil(t, second.DeletionRequestedAt)
	assert.Equal(t, *first.DeletionRequestedAt, *second.DeletionRequestedAt,
		"deletion timestamp is stamped once and not moved by requeues")
}

// The first call marks the row the name names; the repeat reports changed=false and
// stamps nothing — same timestamp, same resource_version, so no watch churn. Neither
// hands back a row: the caller resolves the name itself if it needs the id.
func TestDeletionRequestsCreateByNameIsIdempotent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name: "w1",
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	res, err := store.DeletionRequests().CreateByName(ctx, testGK, "w1")
	require.NoError(t, err)
	require.True(t, res.Marked, "this call set the flag")
	first, err := store.ObjectsGetByName(ctx, testGK, "w1")
	require.NoError(t, err)
	require.NotNil(t, first.DeletionRequestedAt, "the name's own row is the one marked")

	res, err = store.DeletionRequests().CreateByName(ctx, testGK, "w1")
	require.NoError(t, err)
	assert.False(t, res.Marked, "the repeat changed nothing")
	second, err := store.ObjectsGetByName(ctx, testGK, "w1")
	require.NoError(t, err)
	require.NotNil(t, second.DeletionRequestedAt)
	assert.Equal(t, *first.DeletionRequestedAt, *second.DeletionRequestedAt,
		"the deletion timestamp is stamped once")
	assert.Equal(t, first.ResourceVersion, second.ResourceVersion,
		"a no-op must not bump the watch cursor")
}

// The id is what lets a caller push the object it just marked; a name delete has
// no id of its own. It is meaningful only when changed, which is why the repeat
// below asserts nothing about it.
func TestDeletionRequestsCreateByNameReturnsTheMarkedID(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name: "w1",
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	res, err := store.DeletionRequests().CreateByName(ctx, testGK, "w1")
	require.NoError(t, err)
	require.True(t, res.Marked)
	assert.Equal(t, created.ID, res.ID, "the id of the row the name held")

	res, err = store.DeletionRequests().CreateByName(ctx, testGK, "w1")
	require.NoError(t, err)
	assert.False(t, res.Marked)
}

// Names are unique per kind, not globally, so another kind's row holding the same
// name is simply absent here — ErrNotFound, never ErrWrongKind, and untouched.
func TestDeletionRequestsCreateByNameIsKindScoped(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	otherGK := beehive.GroupKind{Group: "", Kind: "Other"}

	other, err := store.ObjectsCreate(ctx, otherGK, beehive.ObjectsCreateInput{
		Name: "shared",
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	_, err = store.DeletionRequests().CreateByName(ctx, testGK, "shared")
	assert.ErrorIs(t, err, beehive.ErrNotFound)
	assert.NotErrorIs(t, err, beehive.ErrWrongKind)

	got, err := store.ObjectsGet(ctx, other.ID)
	require.NoError(t, err)
	assert.Nil(t, got.DeletionRequestedAt, "the other kind's row is untouched")
}

func TestDeleteObject(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created := newRefObject(t, store)

	require.NoError(t, store.ObjectsDelete(ctx, created.ID))

	_, err := store.ObjectsGet(ctx, created.ID)
	assert.ErrorIs(t, err, beehive.ErrNotFound)

	assert.ErrorIs(t, store.ObjectsDelete(ctx, created.ID), beehive.ErrNotFound,
		"deleting a missing row reports not found")
}

func TestWithinCommitsAndRollsBack(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Commit: writes inside a successful Within are visible afterward.
	var committedID beehive.ObjectID
	require.NoError(t, store.Within(ctx, func(ctx context.Context) error {
		obj, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
			Name: "committed",
			Spec: []byte(`{}`),
		})
		if err != nil {
			return err
		}
		committedID = obj.ID
		return nil
	}))
	_, err := store.ObjectsGet(ctx, committedID)
	assert.NoError(t, err)

	// Rollback: a non-nil error discards every write in the transaction.
	sentinel := errors.New("boom")
	err = store.Within(ctx, func(ctx context.Context) error {
		_, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
			Name: "rolledback",
			Spec: []byte(`{}`),
		})
		require.NoError(t, err)
		return sentinel
	})
	assert.ErrorIs(t, err, sentinel)
	_, err = store.ObjectsGetByName(ctx, testGK, "rolledback")
	assert.ErrorIs(t, err, beehive.ErrNotFound, "rolled-back write must not persist")
}

func TestAfterCommit(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Outside a transaction there is nothing to wait for: the hook runs inline.
	ran := false
	store.AfterCommit(ctx, func(context.Context) { ran = true })
	assert.True(t, ran, "hook outside a transaction must run inline")

	// Inside one it waits for the commit — including through a nested Within,
	// which joins the outer transaction's collector.
	var order []string
	require.NoError(t, store.Within(ctx, func(ctx context.Context) error {
		return store.Within(ctx, func(ctx context.Context) error {
			obj, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
				Name: "hooked",
				Spec: []byte(`{}`),
			})
			require.NoError(t, err)
			store.AfterCommit(ctx, func(hookCtx context.Context) {
				order = append(order, "first")
				// The hook's ctx is detached from the (now committed) transaction, so
				// this read runs on the pool and the write below opens a fresh
				// transaction instead of joining a dead *sql.Tx.
				_, err := store.ObjectsGet(hookCtx, obj.ID)
				assert.NoError(t, err, "hook must see the committed row")
				assert.NoError(t, store.Within(hookCtx, func(hookCtx context.Context) error {
					_, _, err := store.ObjectsUpdateSpec(hookCtx, testGK, obj.ID, []byte(`{"a":1}`), 0)
					return err
				}), "a hook must be able to open its own transaction")
			})
			store.AfterCommit(ctx, func(context.Context) { order = append(order, "second") })
			assert.Empty(t, order, "hooks must not run before the outer commit")
			return nil
		})
	}))
	assert.Equal(t, []string{"first", "second"}, order, "hooks run in registration order")

	// A hook that chains more post-commit work through the transaction ctx it
	// captured — rather than the detached one it was handed — must still run it.
	// flush has already drained the collector by then, so buffering would drop it.
	var chained []string
	require.NoError(t, store.Within(ctx, func(txCtx context.Context) error {
		store.AfterCommit(txCtx, func(context.Context) {
			chained = append(chained, "first")
			store.AfterCommit(txCtx, func(context.Context) {
				chained = append(chained, "second")
			})
		})
		return nil
	}))
	assert.Equal(t, []string{"first", "second"}, chained, "a hook registered from a hook must run")

	// addHook's took-ownership return covers the late registration that arrives
	// after the hooks were taken: it runs the hook inline rather than queueing it
	// where nothing will drain it. No test drives that arm directly — reaching it
	// needs a ctx holding the drained txState *without* its committed transaction,
	// and every mutator that could carry one either fails on the dead tx first or
	// self-wraps in Within, which installs a fresh txState.

	// A rolled-back transaction discards its hooks along with its writes.
	sentinel := errors.New("boom")
	var rolledBack bool
	err := store.Within(ctx, func(ctx context.Context) error {
		store.AfterCommit(ctx, func(context.Context) { rolledBack = true })
		return sentinel
	})
	assert.ErrorIs(t, err, sentinel)
	assert.False(t, rolledBack, "a rolled-back transaction must not run its hooks")
}

// createIn creates a bare object of testGK on ctx, joining whatever transaction
// ctx carries. The savepoint tests use names to tell writes apart after the fact.
func createIn(t *testing.T, store beehive.Store, ctx context.Context, name string) {
	t.Helper()
	_, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name: name,
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)
}

// committed reports whether name is in the committed state, read outside any
// transaction.
func committed(t *testing.T, store beehive.Store, name string) bool {
	t.Helper()
	_, err := store.ObjectsGetByName(context.Background(), testGK, name)
	if errors.Is(err, beehive.ErrNotFound) {
		return false
	}
	require.NoError(t, err)
	return true
}

// TestWithinNestedErrorUnwindsItsOwnWrites is the whole feature in one test: the
// outer caller *swallows* the nested error, so nothing but the nested Within
// itself can be what rolled the nested write back.
func TestWithinNestedErrorUnwindsItsOwnWrites(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	sentinel := errors.New("boom")

	require.NoError(t, store.Within(ctx, func(ctx context.Context) error {
		createIn(t, store, ctx, "outer")
		err := store.Within(ctx, func(ctx context.Context) error {
			createIn(t, store, ctx, "nested")
			return sentinel
		})
		assert.ErrorIs(t, err, sentinel)
		return nil // swallowed: the outer transaction still commits
	}))

	assert.True(t, committed(t, store, "outer"), "the outer write must commit")
	assert.False(t, committed(t, store, "nested"), "the nested write must have unwound")
}

// TestWithinNestedErrorDiscardsOnlyItsOwnHooks pins the hook watermark: the hook
// list is append-only and drains at the outermost commit, so an unwind that did
// not truncate it would fire WithOnCreate for a row that rolled back.
func TestWithinNestedErrorDiscardsOnlyItsOwnHooks(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	sentinel := errors.New("boom")

	var ran []string
	require.NoError(t, store.Within(ctx, func(ctx context.Context) error {
		store.AfterCommit(ctx, func(context.Context) { ran = append(ran, "before") })
		err := store.Within(ctx, func(ctx context.Context) error {
			store.AfterCommit(ctx, func(context.Context) { ran = append(ran, "nested") })
			return sentinel
		})
		assert.ErrorIs(t, err, sentinel)
		store.AfterCommit(ctx, func(context.Context) { ran = append(ran, "after") })
		return nil
	}))

	assert.Equal(t, []string{"before", "after"}, ran,
		"the nested frame's hook must be discarded, and only it")
}

// TestWithinNestedSiblingsAreIndependent: a rewind must not take a later sibling
// with it, which is what a savepoint per frame buys over one shared marker.
func TestWithinNestedSiblingsAreIndependent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	sentinel := errors.New("boom")

	require.NoError(t, store.Within(ctx, func(ctx context.Context) error {
		err := store.Within(ctx, func(ctx context.Context) error {
			createIn(t, store, ctx, "first")
			return sentinel
		})
		assert.ErrorIs(t, err, sentinel)
		return store.Within(ctx, func(ctx context.Context) error {
			createIn(t, store, ctx, "second")
			return nil
		})
	}))

	assert.False(t, committed(t, store, "first"), "the failed sibling must have unwound")
	assert.True(t, committed(t, store, "second"), "the later sibling must commit")
}

// TestWithinNestedUnwindsToTheRightDepth: an unwind at depth 2 leaves depth 1's
// writes intact. Ordinary nesting runs this deep in production — a
// ControllerClient.Within around a Client.Create around ObjectsCreate's self-wrap
// is three frames — so this is the normal case, not an edge.
func TestWithinNestedUnwindsToTheRightDepth(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	sentinel := errors.New("boom")

	require.NoError(t, store.Within(ctx, func(ctx context.Context) error {
		return store.Within(ctx, func(ctx context.Context) error {
			createIn(t, store, ctx, "depth1")
			err := store.Within(ctx, func(ctx context.Context) error {
				createIn(t, store, ctx, "depth2")
				return sentinel
			})
			assert.ErrorIs(t, err, sentinel)
			return nil
		})
	}))

	assert.True(t, committed(t, store, "depth1"), "depth 1 must be untouched by depth 2's unwind")
	assert.False(t, committed(t, store, "depth2"), "depth 2 must have unwound")
}

// TestWithinFailedUnwindPoisonsTheTransaction: if the unwind itself fails the
// transaction is in an unknown state, so it must neither accept further nested work
// nor commit. Silently committing after a failed unwind is the exact failure the
// savepoint boundary exists to make impossible.
func TestWithinFailedUnwindPoisonsTheTransaction(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	sentinel := errors.New("boom")

	var refused error
	err := store.Within(ctx, func(ctx context.Context) error {
		createIn(t, store, ctx, "before")

		nerr := store.Within(ctx, func(ctx context.Context) error {
			// Pop this frame's savepoint out from under its own unwind, so the
			// ROLLBACK TO fails with "no such savepoint". That is the shape SQLITE_FULL,
			// SQLITE_IOERR and SQLITE_NOMEM produce for real: they roll the whole
			// transaction back, savepoint stack included, and a ROLLBACK TO naming a
			// savepoint that no longer exists is what fails next.
			fr, ok := txFrom(ctx)
			require.True(t, ok)
			_, err := fr.st.tx.ExecContext(ctx, savepointStmt("RELEASE", fr.st.savepoints))
			require.NoError(t, err)
			return sentinel
		})
		require.ErrorIs(t, nerr, sentinel, "the caller's own error must still surface")

		// Swallowed, as a careless caller would. Further nested work must be refused
		// rather than accumulate on a transaction in unknown state.
		_, refused = store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
			Name: "after",
			Spec: []byte(`{}`),
		})
		return nil
	})

	require.Error(t, err, "a poisoned transaction must not commit, even on a clean return")
	assert.Error(t, refused, "a poisoned transaction must refuse further nested Withins")
	assert.False(t, committed(t, store, "before"), "the whole transaction rolls back")
	assert.False(t, committed(t, store, "after"))
}

// TestWithinOnAClosedTransactionOpensAFreshOne covers the ctx AfterCommit
// deliberately keeps alive: the contract supports a hook passing back the
// transaction ctx it captured rather than the detached one it was handed, and that
// ctx still carries a txState whose *sql.Tx is committed.
//
// It must be driven from inside the hook. Doing it after the outer Within returns
// would exercise only the deferred close and would pass even if closed were set too
// late — and the hook-drain window is precisely the window the contract promises.
func TestWithinOnAClosedTransactionOpensAFreshOne(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	var nestedErr, readErr error
	require.NoError(t, store.Within(ctx, func(txCtx context.Context) error {
		createIn(t, store, txCtx, "seed")
		store.AfterCommit(txCtx, func(context.Context) {
			nestedErr = store.Within(txCtx, func(freshCtx context.Context) error {
				createIn(t, store, freshCtx, "from-hook")
				return nil
			})
			// conn has to agree with Within, or the same ctx takes a fresh
			// transaction one way and a dead one the other.
			_, readErr = store.ObjectsGetByName(txCtx, testGK, "seed")
		})
		return nil
	}))

	assert.NoError(t, nestedErr, "a Within on a closed transaction ctx must open a fresh one")
	assert.True(t, committed(t, store, "from-hook"), "and the write it carries must land")
	assert.NoError(t, readErr, "a bare read on that ctx must fall back to the pool")
}

// TestWithinRefusesAConcurrentNestedFrame: savepoints are a stack, so two
// goroutines nesting on one transaction can interleave such that one's ROLLBACK TO
// discards work the other already released. Refuse rather than serialise — holding a
// lock across fn deadlocks the moment fn waits on another goroutine that also wants
// the store.
//
// Its companion is TestWithinNestedUnwindsToTheRightDepth: ordinary deep nesting on
// one goroutine must stay accepted, which is what makes this a concurrency check
// rather than a depth limit.
func TestWithinRefusesAConcurrentNestedFrame(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	tried := make(chan struct{})
	var sibling error

	require.NoError(t, store.Within(ctx, func(txCtx context.Context) error {
		return store.Within(txCtx, func(context.Context) error {
			// Started from inside the first nested frame, so it is provably in flight;
			// the frame then waits on the channel rather than on a clock.
			go func() {
				defer close(tried)
				sibling = store.Within(txCtx, func(context.Context) error { return nil })
			}()
			select {
			case <-tried:
			case <-time.After(10 * time.Second):
				t.Error("timed out waiting for the sibling goroutine")
			}
			return nil
		})
	}))

	assert.ErrorIs(t, sibling, beehive.ErrStaleTxContext,
		"a second goroutine nesting on the same transaction must be refused: its ctx is "+
			"not the live frame, whoever won the race")
}

// TestWithinFailedSavepointDoesNotPoison: a SAVEPOINT that never lands pushed
// nothing on the SQLite side, so the transaction's state is still known and ordinary
// error handling applies. Poison is for a failed *unwind*, where it is not.
func TestWithinFailedSavepointDoesNotPoison(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	err := store.Within(ctx, func(txCtx context.Context) error {
		fr, ok := txFrom(txCtx)
		require.True(t, ok)
		// Kill the transaction under the frame without latching txState.closed, so
		// Within still takes the nested branch and the SAVEPOINT is what fails.
		require.NoError(t, fr.st.tx.Rollback())

		nerr := store.Within(txCtx, func(context.Context) error {
			t.Error("fn must not run when its savepoint could not be opened")
			return nil
		})
		assert.Error(t, nerr, "a failed SAVEPOINT must surface")
		assert.NoError(t, fr.st.poisonErr(), "but it must not poison the transaction")
		return nerr
	})
	assert.Error(t, err)
}

// TestWithinFailedReleasePoisons is the success-path half of the poison rule: fn
// returned cleanly, so there is nothing to unwind, but the RELEASE that pops the
// savepoint still failed and the stack is no longer what we think it is.
func TestWithinFailedReleasePoisons(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	err := store.Within(ctx, func(txCtx context.Context) error {
		nerr := store.Within(txCtx, func(inner context.Context) error {
			fr, ok := txFrom(inner)
			require.True(t, ok)
			require.NoError(t, fr.st.tx.Rollback())
			return nil // clean return; the RELEASE below is what fails
		})
		assert.Error(t, nerr, "a failed RELEASE must surface even on a clean return")

		fr, ok := txFrom(txCtx)
		require.True(t, ok)
		assert.Error(t, fr.st.poisonErr(), "and must poison the transaction")
		return nerr
	})
	assert.Error(t, err)
}

// TestAfterCommitOnAFinishedTransaction: a hook runs if and only if the transaction
// it was registered against committed. "The transaction is over" is not the question
// — whether it committed is — so the outcome is retained, not just the fact that it
// closed.
//
// A rolled-back outermost transaction is the same event as a nested frame unwinding,
// one level up (see TestAfterCommitOnAnUnwoundFrameIsDiscarded), and gets the same
// answer. Consistency with conn and Within, which treat a closed ctx as carrying no
// transaction, does not extend here: falling back to the pool for a *read* is
// harmless, firing a side effect for writes that are gone is the failure AfterCommit
// exists to prevent.
func TestAfterCommitOnAFinishedTransaction(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	sentinel := errors.New("boom")

	for _, tc := range []struct {
		name    string
		outcome error
		wantRun bool
	}{
		{"committed", nil, true},
		{"rolled back", sentinel, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var captured context.Context
			err := store.Within(ctx, func(txCtx context.Context) error {
				captured = txCtx
				return tc.outcome
			})
			require.ErrorIs(t, err, tc.outcome)

			ran := false
			store.AfterCommit(captured, func(context.Context) { ran = true })
			assert.Equal(t, tc.wantRun, ran)
		})
	}
}

// TestWithinNestedUnwindsAfterContextCancellation: fn's ctx is the caller's, and a
// caller may hand a nested frame a cancellable child. ExecContext returns before it
// runs a statement on a canceled ctx, so an unwind issued on fn's own ctx would skip
// the ROLLBACK TO entirely — leaving the frame's writes applied and poisoning a
// transaction that is otherwise perfectly healthy. The savepoint statements must
// therefore outlive fn's ctx.
func TestWithinNestedUnwindsAfterContextCancellation(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	sentinel := errors.New("boom")

	require.NoError(t, store.Within(ctx, func(txCtx context.Context) error {
		createIn(t, store, txCtx, "outer")

		childCtx, cancel := context.WithCancel(txCtx)
		defer cancel()
		err := store.Within(childCtx, func(nestedCtx context.Context) error {
			createIn(t, store, nestedCtx, "nested")
			cancel() // the caller's own ctx dies mid-frame
			return sentinel
		})
		assert.ErrorIs(t, err, sentinel)
		return nil // swallowed, exactly as the boundary contract allows
	}))

	assert.True(t, committed(t, store, "outer"), "the outer transaction must still commit")
	assert.False(t, committed(t, store, "nested"), "and the nested frame must still have unwound")
}

// TestWithinNestedUnwindKeepsHooksFromEnclosingFrames: a hook queued by an enclosing
// frame is not this frame's to discard, and slice position cannot tell them apart.
// The enclosing ctx is in lexical scope inside the nested fn, so a single goroutine
// reaches this without any concurrency — which is what makes it worth defending,
// since sharing a transaction ctx across goroutines is out of contract anyway.
func TestWithinNestedUnwindKeepsHooksFromEnclosingFrames(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	sentinel := errors.New("boom")

	var ran []string
	require.NoError(t, store.Within(ctx, func(txCtx context.Context) error {
		err := store.Within(txCtx, func(context.Context) error {
			// Registered against the *enclosing* frame while this one is in flight, so
			// it lands above this frame's position while belonging to the frame below.
			store.AfterCommit(txCtx, func(context.Context) { ran = append(ran, "enclosing") })
			return sentinel
		})
		assert.ErrorIs(t, err, sentinel)
		return nil
	}))

	assert.Equal(t, []string{"enclosing"}, ran,
		"the enclosing frame's hook must survive a nested frame's unwind")
}

// TestWithinNestedPanicPoisonsWhenRecovered: the outermost deferred tx.Rollback only
// covers a panic that *escapes* Within. A caller that recovers inside its own fn and
// returns nil leaves the abandoned frame's savepoint open, and COMMIT releases every
// open savepoint — landing the writes of a frame that never completed.
func TestWithinNestedPanicPoisonsWhenRecovered(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	err := store.Within(ctx, func(txCtx context.Context) error {
		func() {
			defer func() { _ = recover() }()
			_ = store.Within(txCtx, func(nestedCtx context.Context) error {
				createIn(t, store, nestedCtx, "panicked")
				panic("boom")
			})
		}()
		return nil // recovered, and carrying on as if nothing happened
	})

	require.Error(t, err, "a frame abandoned by a panic must not be allowed to commit")
	assert.False(t, committed(t, store, "panicked"), "its writes must not land")
}

// TestAfterCommitOnAnUnwoundFrameIsDiscarded: dropping a frame's queued hooks at the
// unwind is not enough, because the frame's ctx can outlive the frame. A registration
// arriving afterwards would be queued fresh and ride the outer commit, firing a hook
// for writes that were rolled back — the precise failure the boundary exists to
// prevent, arriving through the back door.
//
// No goroutine needed: capturing the nested ctx into an enclosing variable reaches it
// on one goroutine, which is the only discipline the contract supports anyway.
func TestAfterCommitOnAnUnwoundFrameIsDiscarded(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	sentinel := errors.New("boom")

	ran := false
	require.NoError(t, store.Within(ctx, func(txCtx context.Context) error {
		var captured context.Context
		err := store.Within(txCtx, func(nested context.Context) error {
			captured = nested
			createIn(t, store, nested, "unwound")
			return sentinel
		})
		require.ErrorIs(t, err, sentinel)

		store.AfterCommit(captured, func(context.Context) { ran = true })
		return nil // swallowed; the outer transaction still commits
	}))

	assert.False(t, ran, "a hook registered against an unwound frame must never run")
	assert.False(t, committed(t, store, "unwound"))
}

// TestWithinRefusesToCommitWhileANestedFrameIsOpen closes the gap the depth check
// alone leaves. A goroutine entering a nested Within while no frame happens to be
// open passes the depth==height test legitimately; if the outer fn then returns
// before that frame finishes, COMMIT releases its still-open savepoint and lands
// writes the frame may be about to roll back — and by then nothing can undo them.
//
// Every nested frame pops in a defer, so on one goroutine height is always 0 by the
// time fn returns. A nonzero height at the commit is therefore proof the ctx was
// shared, which makes it a sound thing to refuse on.
func TestWithinRefusesToCommitWhileANestedFrameIsOpen(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})

	err := store.Within(ctx, func(txCtx context.Context) error {
		go func() {
			defer close(done)
			// Errors here are the point of the test, not a failure of it: this frame
			// outlives its transaction. require would also be illegal off the test
			// goroutine.
			_ = store.Within(txCtx, func(nested context.Context) error {
				_, _ = store.ObjectsCreate(nested, testGK, beehive.ObjectsCreateInput{
					Name: "orphan",
					Spec: []byte(`{}`),
				})
				close(entered)
				<-release
				return errors.New("too late to matter")
			})
		}()
		<-entered
		return nil // returns while the child's frame is still open
	})

	close(release)
	<-done

	require.ErrorIs(t, err, beehive.ErrConcurrentNestedTx,
		"committing under an open frame must be refused")
	assert.False(t, committed(t, store, "orphan"), "and the orphaned frame's write must not land")
}

// TestTxStateSealForCommit drives the state machine directly. The window it closes —
// between observing an empty frame stack and issuing COMMIT — is two adjacent
// statements, so it is not deterministically reachable through the public path; what
// *is* testable is that admission and the commit check share one lock, and that the
// door stays shut once closed. Whitebox tests are the convention here precisely so
// this kind of invariant can be pinned.
func TestTxStateSealForCommit(t *testing.T) {
	t.Run("shuts the door on later frames", func(t *testing.T) {
		st := &txState{}
		require.NoError(t, st.sealForCommit())

		_, err := st.pushSavepoint(0, 0)
		assert.ErrorIs(t, err, beehive.ErrStaleTxContext,
			"a frame arriving after the seal must be refused, not released by the commit")
	})

	t.Run("refuses while a frame is open", func(t *testing.T) {
		st := &txState{}
		_, err := st.pushSavepoint(0, 0)
		require.NoError(t, err)

		assert.ErrorIs(t, st.sealForCommit(), beehive.ErrConcurrentNestedTx)
	})

	t.Run("surfaces poison ahead of either", func(t *testing.T) {
		st := &txState{}
		st.poison(assert.AnError)

		assert.ErrorIs(t, st.sealForCommit(), assert.AnError)
	})
}

// TestWithinRefusesAContextFromAnUnwoundFrame: depth is reusable, so it cannot be the
// whole admission test. Once a frame unwinds, a later sibling restores the height it
// had, and a ctx captured from the dead frame matches again — on one goroutine, with
// no concurrency anywhere.
//
// Admitting it would split the store's view of that frame: Within and conn would treat
// it as live and let its rolled-back work commit, while addHook still finds its id
// dead and discards hooks registered on the same ctx. A committed write whose
// WithOnCreate never fires is the inverse of the guarantee this feature exists for.
func TestWithinRefusesAContextFromAnUnwoundFrame(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	sentinel := errors.New("boom")

	require.NoError(t, store.Within(ctx, func(txCtx context.Context) error {
		var captured context.Context
		err := store.Within(txCtx, func(nested context.Context) error {
			captured = nested
			return sentinel
		})
		require.ErrorIs(t, err, sentinel)

		// A live sibling at the same depth: the dead ctx's depth now matches the
		// height again, which is all admission used to look at.
		return store.Within(txCtx, func(context.Context) error {
			_, createErr := store.ObjectsCreate(captured, testGK, beehive.ObjectsCreateInput{
				Name: "revived",
				Spec: []byte(`{}`),
			})
			assert.ErrorIs(t, createErr, beehive.ErrStaleTxContext)
			return nil
		})
	}))

	assert.False(t, committed(t, store, "revived"), "a dead frame's ctx must not write")
}

// TestTxStateUnwindFrameCoalesces: dead ranges are scanned linearly by every
// admission and every hook registration, so an outer unwind absorbs the ranges of
// frames opened inside it rather than letting the list grow once per unwind. A long
// outer Within swallowing many nested errors would otherwise pay O(N²) under the
// transaction mutex.
func TestTxStateUnwindFrameCoalesces(t *testing.T) {
	t.Run("absorbs the ranges of frames opened inside it", func(t *testing.T) {
		st := &txState{}
		outer, err := st.pushSavepoint(0, 0)
		require.NoError(t, err)
		inner, err := st.pushSavepoint(1, outer)
		require.NoError(t, err)

		st.unwindFrame(inner)
		st.popSavepoint()
		require.Equal(t, []idRange{{lo: inner, hi: inner}}, st.dead)

		st.unwindFrame(outer)
		assert.Equal(t, []idRange{{lo: outer, hi: inner}}, st.dead,
			"the outer unwind must absorb the inner range, not stack another on it")
	})

	t.Run("keeps the ranges of earlier siblings", func(t *testing.T) {
		st := &txState{}
		first, err := st.pushSavepoint(0, 0)
		require.NoError(t, err)
		st.unwindFrame(first)
		st.popSavepoint()

		second, err := st.pushSavepoint(0, 0)
		require.NoError(t, err)
		st.unwindFrame(second)

		assert.Equal(t, []idRange{{lo: first, hi: first}, {lo: second, hi: second}}, st.dead,
			"a sibling was never inside this frame, so its range must survive")
	})
}

func TestObjectsListUnsettledIDs(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	otherGK := beehive.GroupKind{Group: "", Kind: "Other"}

	// settled: ObservedGeneration == Generation — must NOT appear
	settled := newRefObject(t, store)
	err := store.ObjectsUpdateStatus(ctx, testGK, settled.ID, settled.Generation, []byte(`{}`), 0)
	require.NoError(t, err)

	// unsettled: ObservedGeneration is nil — must appear
	nilObs := newRefObject(t, store)

	// unsettled: ObservedGeneration < Generation (spec changed after reconcile) — must appear
	stale := newRefObject(t, store)
	err = store.ObjectsUpdateStatus(ctx, testGK, stale.ID, stale.Generation, []byte(`{}`), 0)
	require.NoError(t, err)
	_, _, err = store.ObjectsUpdateSpec(ctx, testGK, stale.ID, []byte(`{"updated":true}`), 0)
	require.NoError(t, err)

	// different kind — must NOT appear
	_, err = store.ObjectsCreate(ctx, otherGK, beehive.ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	ids, err := store.ObjectsListUnsettledIDs(ctx, testGK)
	require.NoError(t, err)
	assert.Equal(t, []beehive.ObjectID{nilObs.ID, stale.ID}, ids)

	// ObjectsListIDs returns every object of the kind, settled or not, ordered by id.
	all, err := store.ObjectsListIDs(ctx, testGK)
	require.NoError(t, err)
	assert.Equal(t, []beehive.ObjectID{settled.ID, nilObs.ID, stale.ID}, all)
}

func TestListIDsQueryError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	_, err := store.ObjectsListIDs(context.Background(), testGK)
	require.Error(t, err)
}

func TestNestedWithinJoinsOuterTransaction(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// The inner Within returns nil — if it opened and committed its own
	// transaction the write would survive the outer rollback below.
	sentinel := errors.New("outer boom")
	err := store.Within(ctx, func(ctx context.Context) error {
		if err := store.Within(ctx, func(ctx context.Context) error {
			_, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
				Name: "nested",
				Spec: []byte(`{}`),
			})
			return err
		}); err != nil {
			return err
		}
		return sentinel
	})
	assert.ErrorIs(t, err, sentinel)

	_, err = store.ObjectsGetByName(ctx, testGK, "nested")
	assert.ErrorIs(t, err, beehive.ErrNotFound,
		"nested Within joins the outer tx, so the outer rollback discards its write")
}

// writeProbe answers "what would a consumer have seen?" for these tests: opened
// before a write, asked afterwards. It holds a cursor into the write log and reports
// the rows above it, which is the whole of the change-notification surface — nothing
// is pushed, so "an observer sees this write" means "this write is above the
// cursor".
//
// It reports versions and ids, never rows: the log carries no body and no
// lifecycle type, so a test that needs the written content re-reads it. That is
// the point of the probe rather than a shim — it makes the tests assert what a
// real consumer can actually learn.
type writeProbe struct {
	t     *testing.T
	store beehive.Store
	rv    int64
}

// newWriteProbe seeds a probe at the store's current cursor, so it reports only
// what lands after this call.
func newWriteProbe(t *testing.T, store beehive.Store) *writeProbe {
	t.Helper()
	rv, _, err := store.ObjectWritesMaxVersionAll(context.Background())
	require.NoError(t, err)
	return &writeProbe{t: t, store: store, rv: rv}
}

// writes returns everything above the cursor without moving it.
func (p *writeProbe) writes() []storeapi.ObjectWrite {
	p.t.Helper()
	got, _, err := p.store.ObjectWritesListSinceAll(context.Background(), p.rv, 100)
	require.NoError(p.t, err)
	return got
}

// expectWrite asserts exactly one write landed above the cursor, advances past
// it, and returns it.
func (p *writeProbe) expectWrite() storeapi.ObjectWrite {
	p.t.Helper()
	got := p.writes()
	require.Len(p.t, got, 1, "expected exactly one observable write")
	p.rv = got[0].ResourceVersion
	return got[0]
}

// expectWrites asserts n writes landed above the cursor and advances past them.
func (p *writeProbe) expectWrites(n int) []storeapi.ObjectWrite {
	p.t.Helper()
	got := p.writes()
	require.Len(p.t, got, n, "expected exactly %d observable writes", n)
	p.rv = got[len(got)-1].ResourceVersion
	return got
}

// expectNone asserts the write was a true no-op: nothing above the cursor, so no
// consumer can ever learn of it.
func (p *writeProbe) expectNone() {
	p.t.Helper()
	assert.Empty(p.t, p.writes(), "a no-op write must leave nothing for a consumer to find")
}

// newRawStore returns a *sqliteStore directly so tests can close store.db to
// force database errors on subsequent calls.
func newRawStore(t *testing.T) *sqliteStore {
	t.Helper()
	store, err := OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() }) // Close is a no-op after db.Close()
	return store
}

// insertBadFinalizersRow inserts a row with invalid finalizers JSON directly so
// scanObject's json.Unmarshal step fails when the row is read back.
func insertBadFinalizersRow(t *testing.T, store *sqliteStore, gk beehive.GroupKind) beehive.ObjectID {
	t.Helper()
	ctx := context.Background()
	res, err := store.db.ExecContext(ctx, `
		INSERT INTO objects ("group", kind, name, spec, finalizers, generation, resource_version, created_at, updated_at)
		VALUES (?, ?, ?, '{}', 'not-valid-json', 1, 999999, 0, 0)`,
		gk.Group, gk.Kind, uniqueName())
	require.NoError(t, err)
	id, err := res.LastInsertId()
	require.NoError(t, err)
	return beehive.ObjectID(id)
}

// hideObjectColumn renames a column out from under the statements that name it,
// so any read still selecting it fails to prepare. The probe for a column whose
// content cannot be made undecodable the way finalizers can.
func hideObjectColumn(t *testing.T, store *sqliteStore, column string) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(),
		fmt.Sprintf(`ALTER TABLE objects RENAME COLUMN %s TO %s_hidden`, column, column))
	require.NoError(t, err)
}

func TestWithinBeginTxError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	err := store.Within(context.Background(), func(context.Context) error { return nil })
	require.Error(t, err)
}

func TestObjectsCreateDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	_, err := store.ObjectsCreate(context.Background(), testGK, beehive.ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`{}`),
	})
	require.Error(t, err)
}

func TestListObjectsQueryError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	_, err := store.ObjectsList(context.Background(), testGK)
	require.Error(t, err)
}

func TestListObjectsScanError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	// Create a valid object, then corrupt its finalizers so scanObject fails.
	created := newRefObject(t, store)

	_, err := store.db.ExecContext(ctx,
		`UPDATE objects SET finalizers = 'not-valid-json' WHERE id = ?`, created.ID)
	require.NoError(t, err)

	_, err = store.ObjectsList(ctx, testGK)
	require.Error(t, err)
}

func TestObjectsListUnsettledIDsQueryError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	_, err := store.ObjectsListUnsettledIDs(context.Background(), testGK)
	require.Error(t, err)
}

// reconcileOwed reads an object's owed-wake count off the row.
func reconcileOwed(t *testing.T, store *sqliteStore, id beehive.ObjectID) int64 {
	t.Helper()
	obj, err := store.ObjectsGet(context.Background(), id)
	require.NoError(t, err)
	return obj.ReconcileOwed
}

// TestReconcileOwedCount exercises the owed-wake counter: a fresh row owes nothing,
// ReconcileOwedIncrement raises the count (visible on the object and via
// ReconcileOwedListIDs), and ReconcileOwedDecrement subtracts the count a pass
// observed — draining the row fully rather than leaving a residual — while
// increments beyond that observed count survive, and flooring at 0.
func TestReconcileOwedCount(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	newRefObject(t, store) // a second row that owes nothing, so the list stays scoped

	// A fresh object owes no wake.
	require.Zero(t, a.ReconcileOwed)
	ids, err := store.ReconcileOwed().ListIDs(ctx, testGK)
	require.NoError(t, err)
	assert.Empty(t, ids)

	// Two wakes owed (e.g. a second stamped while the first was still owed).
	require.NoError(t, store.ReconcileOwedIncrement(ctx, a.ID))
	require.NoError(t, store.ReconcileOwedIncrement(ctx, a.ID))
	assert.Equal(t, int64(2), reconcileOwed(t, store, a.ID), "the count is read back off the row")

	ids, err = store.ReconcileOwed().ListIDs(ctx, testGK)
	require.NoError(t, err)
	assert.Equal(t, []beehive.ObjectID{a.ID}, ids, "only the owed row is listed (b owes nothing)")

	// A pass that observed both services both: subtracting the observed count
	// drains the row in one go, leaving nothing for the backstop to re-enqueue.
	require.NoError(t, store.ReconcileOwed().Decrement(ctx, testGK, a.ID, 2))
	assert.Zero(t, reconcileOwed(t, store, a.ID), "subtracting the observed count drains the row")
	ids, err = store.ReconcileOwed().ListIDs(ctx, testGK)
	require.NoError(t, err)
	assert.Empty(t, ids, "drained row leaves the partial index")

	// An increment beyond what a pass observed survives that pass's subtraction.
	require.NoError(t, store.ReconcileOwedIncrement(ctx, a.ID)) // observed by the pass
	require.NoError(t, store.ReconcileOwedIncrement(ctx, a.ID)) // lands during the pass
	require.NoError(t, store.ReconcileOwed().Decrement(ctx, testGK, a.ID, 1))
	assert.Equal(t, int64(1), reconcileOwed(t, store, a.ID), "the later increment stays owed")

	// Subtracting more than is owed floors at 0 rather than going negative.
	require.NoError(t, store.ReconcileOwed().Decrement(ctx, testGK, a.ID, 5))
	assert.Zero(t, reconcileOwed(t, store, a.ID), "subtraction floors at 0")
}

// TestReconcileOwedDecrementIsKindScoped pins the kind boundary in the predicate
// rather than in caller discipline: another kind's id is refused outright, and the
// count it names is left for the pass that actually owes it.
func TestReconcileOwedDecrementIsKindScoped(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	require.NoError(t, store.ReconcileOwedIncrement(ctx, a.ID))

	otherGK := beehive.GroupKind{Group: "", Kind: "Other"}
	err := store.ReconcileOwed().Decrement(ctx, otherGK, a.ID, 1)

	assert.ErrorIs(t, err, beehive.ErrWrongKind)
	assert.Equal(t, int64(1), reconcileOwed(t, store, a.ID), "a foreign kind drains nothing")
}

// TestReconcileOwedDecrementVanishedRowIsNotAnError pins the other half of the
// RowsAffected == 0 split. A reconcile's target can be collected between its load
// and this write — by the gcCollect at the end of that same pass, or by another
// process's sweeper — and there is nothing left to owe a wake to. Reporting that
// would put a periodic warning on a benign race.
func TestReconcileOwedDecrementVanishedRowIsNotAnError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	require.NoError(t, store.ReconcileOwedIncrement(ctx, a.ID))
	require.NoError(t, store.ObjectsDelete(ctx, a.ID))

	assert.NoError(t, store.ReconcileOwed().Decrement(ctx, testGK, a.ID, 1))
}

// clientOnlyGK is a kind no reconcile loop covers, so its owed count is the
// reclaim's target.
var clientOnlyGK = beehive.GroupKind{Kind: "ClientOnly"}

func newKindObject(t *testing.T, store beehive.Store, gk beehive.GroupKind) *beehive.RawObject {
	t.Helper()
	obj, err := store.ObjectsCreate(context.Background(), gk, beehive.ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	return obj
}

// TestReconcileOwedSweepSkipsKeptKinds pins the reclaim's predicate: a kind with
// no reconcile loop has its count zeroed, a kind in keep is left alone.
func TestReconcileOwedSweepSkipsKeptKinds(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	kept := newRefObject(t, store)
	loose := newKindObject(t, store, clientOnlyGK)
	require.NoError(t, store.ReconcileOwedIncrement(ctx, kept.ID))
	require.NoError(t, store.ReconcileOwedIncrement(ctx, loose.ID))

	_, err := store.ReconcileOwed().Sweep(ctx, []beehive.GroupKind{testGK})
	require.NoError(t, err)

	assert.Equal(t, int64(1), reconcileOwed(t, store, kept.ID), "a kind with a reconcile loop keeps its count")
	assert.Zero(t, reconcileOwed(t, store, loose.ID), "a client-only kind's count is reclaimed")
}

// TestReconcileOwedSweepCountsRowsCleared pins the return value the sweeper logs:
// rows cleared, not rows scanned, and 0 once there is nothing left to reclaim.
func TestReconcileOwedSweepCountsRowsCleared(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	for range 3 {
		obj := newKindObject(t, store, clientOnlyGK)
		require.NoError(t, store.ReconcileOwedIncrement(ctx, obj.ID))
	}
	newKindObject(t, store, clientOnlyGK) // owes nothing, so it is not counted

	cleared, err := store.ReconcileOwed().Sweep(ctx, []beehive.GroupKind{testGK})
	require.NoError(t, err)
	assert.Equal(t, 3, cleared)

	cleared, err = store.ReconcileOwed().Sweep(ctx, []beehive.GroupKind{testGK})
	require.NoError(t, err)
	assert.Zero(t, cleared, "a second sweep finds nothing to clear")
}

// TestReconcileOwedSweepWithNoKeptKinds pins the empty-keep arm, which drops the
// NOT IN clause rather than emitting an empty one.
func TestReconcileOwedSweepWithNoKeptKinds(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newKindObject(t, store, clientOnlyGK)
	require.NoError(t, store.ReconcileOwedIncrement(ctx, a.ID))
	require.NoError(t, store.ReconcileOwedIncrement(ctx, b.ID))

	cleared, err := store.ReconcileOwed().Sweep(ctx, nil)
	require.NoError(t, err)

	assert.Equal(t, 2, cleared)
	assert.Zero(t, reconcileOwed(t, store, a.ID))
	assert.Zero(t, reconcileOwed(t, store, b.ID))
}

// TestReconcileOwedSweepIsNoEmit pins the reclaim out of the change stream. It
// runs on every GC tick, so emitting would wake every tailer and the dependency
// waker for a write no consumer can act on.
func TestReconcileOwedSweepIsNoEmit(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newKindObject(t, store, clientOnlyGK)
	require.NoError(t, store.ReconcileOwedIncrement(ctx, obj.ID))
	before, err := store.ResourceVersionsMaxIssued(ctx)
	require.NoError(t, err)
	writesBefore, _, err := store.ObjectWritesListSinceAll(ctx, 0, 100)
	require.NoError(t, err)

	_, err = store.ReconcileOwed().Sweep(ctx, nil)
	require.NoError(t, err)

	after, err := store.ResourceVersionsMaxIssued(ctx)
	require.NoError(t, err)
	assert.Equal(t, before, after, "the reclaim issues no resource version")
	writesAfter, _, err := store.ObjectWritesListSinceAll(ctx, 0, 100)
	require.NoError(t, err)
	assert.Len(t, writesAfter, len(writesBefore), "the reclaim appends no write-log entry")
}

// Without the partial index the reclaim is a full scan of objects on every GC
// tick, and the predicate carries no equality constraint to force the choice.
func TestReconcileOwedSweepUsesThePartialIndex(t *testing.T) {
	store := newTestStore(t).(*sqliteStore)

	for _, tc := range []struct {
		name string
		keep []storeapi.GroupKind
	}{
		{"keeping kinds", []storeapi.GroupKind{testGK}},
		{"keeping none", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, args := reconcileOwedSweepQuery(tc.keep)
			plan := queryPlan(t, store, q, args...)
			assert.Contains(t, plan, "idx_objects_reconcile_owed",
				"the reclaim must read the partial index, not scan objects:\n"+plan)
		})
	}
}

func TestReconcileOwedSweepQueryError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	_, err := store.ReconcileOwed().Sweep(context.Background(), nil)
	require.Error(t, err)
}

// TestReconcileOwedStampRecordsFindings pins the second producer of owed work.
// The stale-dependents pass enqueues in memory, and a restart loses that; the
// stamp is what survives.
func TestReconcileOwedStampRecordsFindings(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	require.NoError(t, store.ReconcileOwed().Stamp(ctx, nil), "no refs writes nothing")

	refs := []beehive.ObjectRef{
		{ID: a.ID, Group: testGK.Group, Kind: testGK.Kind},
		{ID: b.ID, Group: testGK.Group, Kind: testGK.Kind},
	}
	require.NoError(t, store.ReconcileOwed().Stamp(ctx, refs))
	assert.Equal(t, int64(1), reconcileOwed(t, store, a.ID))
	assert.Equal(t, int64(1), reconcileOwed(t, store, b.ID))

	// Two sweeps that both found the dependent owe two wakes. One pass drains
	// both, because the decrement subtracts the count it observed.
	require.NoError(t, store.ReconcileOwed().Stamp(ctx, refs[:1]))
	assert.Equal(t, int64(2), reconcileOwed(t, store, a.ID))
}

// TestReconcileOwedStampFoldsRepeatedRefs pins the fold the contract states.
// DependentsListStaleSince returns a row per (target, dependent) pair, so a
// dependent with two moved targets reaches one page twice; IN matches its row
// once. Sound because one reconcile answers every wake outstanding at its load,
// and the decrement subtracts the whole count it observed — but it is a contract
// a per-ref caller has to know about, not an accident of the statement.
func TestReconcileOwedStampFoldsRepeatedRefs(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	ref := beehive.ObjectRef{ID: a.ID, Group: testGK.Group, Kind: testGK.Kind}

	require.NoError(t, store.ReconcileOwed().Stamp(ctx, []beehive.ObjectRef{ref, ref, ref}))

	assert.Equal(t, int64(1), reconcileOwed(t, store, a.ID), "one increment, not three")
}

// TestReconcileOwedStampSkipsVanishedRows: a dependent can be collected between
// the listing that found it and the stamp. There is nothing left to owe a wake
// to, which is not a fault.
func TestReconcileOwedStampSkipsVanishedRows(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)
	require.NoError(t, store.ObjectsDelete(ctx, a.ID))

	err := store.ReconcileOwed().Stamp(ctx, []beehive.ObjectRef{
		{ID: a.ID, Group: testGK.Group, Kind: testGK.Kind},
		{ID: b.ID, Group: testGK.Group, Kind: testGK.Kind},
	})

	assert.NoError(t, err)
	assert.Equal(t, int64(1), reconcileOwed(t, store, b.ID), "the surviving ref is still stamped")
}

// TestResourceVersionsMaxIssuedNeverFalls pins the difference from
// ObjectWritesMaxVersionAll, which reads a table retention trims. A cursor that
// falls would compare wrongly against a stored position, so the stale pass reads
// the sequence instead.
func TestResourceVersionsMaxIssuedNeverFalls(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	newRefObject(t, store)

	issued, err := store.ResourceVersionsMaxIssued(ctx)
	require.NoError(t, err)
	require.Positive(t, issued, "a create takes a version")

	ageOutWriteLog(t, store)

	logged, _, err := store.ObjectWritesMaxVersionAll(ctx)
	require.NoError(t, err)
	require.Zero(t, logged, "the log is empty, so its max is back to 0")

	after, err := store.ResourceVersionsMaxIssued(ctx)
	require.NoError(t, err)
	assert.Equal(t, issued, after, "the sequence is unmoved by retention")
}

func TestReconcileOwedQueryErrors(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	ctx := context.Background()

	_, err := store.ReconcileOwed().ListIDs(ctx, testGK)
	require.Error(t, err)
	require.Error(t, store.ReconcileOwedIncrement(ctx, 1))
	require.Error(t, store.ReconcileOwed().Decrement(ctx, testGK, 1, 1))
}

func TestObjectsUpdateSpecDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	_, _, err := store.ObjectsUpdateSpec(context.Background(), testGK, 1, []byte(`{}`), 0)
	require.Error(t, err)
}

func TestUpdateStatusDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	err := store.ObjectsUpdateStatus(context.Background(), testGK, 1, 1, []byte(`{}`), 0)
	require.Error(t, err)
}

func TestDeletionRequestsCreateDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	_, err := store.DeletionRequests().Create(context.Background(), testGK, 1)
	require.Error(t, err)
}

// The mark's own UPDATE is a separate failure from the version draw ahead of it: the
// draw writes resource_version_seq, so a trigger scoped to objects lets it through
// and fails only the statement under test.
func TestDeletionRequestsCreateMarkError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	blockObjectUpdates(t, store)

	_, err := store.DeletionRequests().Create(ctx, testGK, obj.ID)
	require.Error(t, err)
}

// The condition mutators and EventsAdd gate on kind, which is metadata, so they
// must not decode the row to do it. A corrupt finalizers blob is the probe: it
// fails every full-row read in the store, and none of these writes touches
// finalizers. Sibling of TestDeletionRequestsCreateReadsNoBlobOnEitherBranch —
// same rule, same class of write, and before the gate was narrowed these
// disagreed about it.
func TestGatedWritesReadNoBlobToGateOnKind(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	id := insertBadFinalizersRow(t, store, testGK)

	require.NoError(t, store.Conditions().Set(ctx, testGK, id,
		storeapi.Condition{Type: "Ready", Status: "True"}))
	require.NoError(t, store.Conditions().Delete(ctx, testGK, id, "Ready"))
	require.NoError(t, store.Events().Add(ctx, testGK, id,
		storeapi.EventsAddInput{Category: "c", Type: "Normal", Reason: "R"}))

	// The gate still reports scope and existence, which is all it reads for.
	assert.ErrorIs(t, store.Conditions().Set(ctx, beehive.GroupKind{Kind: "Other"}, id,
		storeapi.Condition{Type: "Ready", Status: "True"}), beehive.ErrWrongKind)
	assert.ErrorIs(t, store.Conditions().Delete(ctx, testGK, 999999, "Ready"), beehive.ErrNotFound)
	assert.ErrorIs(t, store.Events().Add(ctx, beehive.GroupKind{Kind: "Other"}, id,
		storeapi.EventsAddInput{Category: "c", Type: "Normal", Reason: "R"}), beehive.ErrWrongKind)
}

// A status write reads six columns of the row it writes, and neither the spec nor
// the finalizer list is one of them. Both probes at once: a finalizers blob that
// fails to decode and a spec column no statement can name.
func TestUpdateStatusReadsNeitherSpecNorFinalizers(t *testing.T) {
	ctx := context.Background()
	store := newRawStore(t)
	id := insertBadFinalizersRow(t, store, testGK) // generation 1, no status, unsettled
	hideObjectColumn(t, store, "spec")

	status := []byte(`{"msg":"hi"}`)
	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, id, 0, status, 0),
		"the content branch")
	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, id, 1, status, 0),
		"identical bytes, handshake advancing")
	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, id, 1, status, 0),
		"identical bytes at a recorded generation: nothing written")

	var obs int64
	var stored string
	require.NoError(t, store.db.QueryRowContext(ctx,
		`SELECT observed_generation, status FROM objects WHERE id = ?`, id).Scan(&obs, &stored))
	assert.Equal(t, int64(1), obs)
	assert.JSONEq(t, `{"msg":"hi"}`, stored)

	// The gates still answer from the columns it does read.
	assert.ErrorIs(t, store.ObjectsUpdateStatus(ctx, beehive.GroupKind{Kind: "Other"}, id, 1, status, 0),
		beehive.ErrWrongKind)
	assert.ErrorIs(t, store.ObjectsUpdateStatus(ctx, testGK, id, 99, status, 0),
		beehive.ErrObservedGenerationFuture)
}

// Clearing a finalizer needs the list and whether the object is deletion-pending,
// and nothing else off the row. The spec and status columns are hidden to say so:
// the write still lands and clearedLast still reports the transition.
func TestDeleteFinalizerReadsNoBlobBesidesTheList(t *testing.T) {
	ctx := context.Background()
	store := newRawStore(t)

	created, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name:       uniqueName(),
		Spec:       []byte(`{}`),
		Finalizers: []string{"a", "b"},
	})
	require.NoError(t, err)
	_, err = store.DeletionRequests().Create(ctx, testGK, created.ID)
	require.NoError(t, err)
	hideObjectColumn(t, store, "spec")
	hideObjectColumn(t, store, "status")

	clearedLast, err := store.FinalizersDelete(ctx, testGK, created.ID, "a")
	require.NoError(t, err)
	assert.False(t, clearedLast, "b is still held")

	clearedLast, err = store.FinalizersDelete(ctx, testGK, created.ID, "b")
	require.NoError(t, err)
	assert.True(t, clearedLast, "the last finalizer off a deleting object")

	var finalizers string
	require.NoError(t, store.db.QueryRowContext(ctx,
		`SELECT finalizers FROM objects WHERE id = ?`, created.ID).Scan(&finalizers))
	assert.JSONEq(t, `[]`, finalizers)

	// The gates still answer from the columns it does read.
	_, err = store.FinalizersDelete(ctx, beehive.GroupKind{Kind: "Other"}, created.ID, "a")
	assert.ErrorIs(t, err, beehive.ErrWrongKind)
	_, err = store.FinalizersDelete(ctx, testGK, 999999, "a")
	assert.ErrorIs(t, err, beehive.ErrNotFound)
}

// The finalizer list is the one blob this write does read, so an undecodable one
// fails it. Stated as its own case because the alternative — treating a list that
// will not decode as empty — would clear finalizers off a row nobody can read.
func TestDeleteFinalizerRefusesAnUndecodableList(t *testing.T) {
	store := newRawStore(t)
	id := insertBadFinalizersRow(t, store, testGK)

	_, err := store.FinalizersDelete(context.Background(), testGK, id, "a")
	require.Error(t, err)
}

// The counter bump is the one statement in a deletion mark that runs after the row
// was already stamped, so it is a distinct failure from the mark itself. Blocking it
// must roll the whole thing back rather than leaving a row stamped with a version the
// cursor never committed.
func TestDeletionRequestsCreateVersionDrawError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	blockResourceVersionDraws(t, store)

	_, err := store.DeletionRequests().Create(ctx, testGK, obj.ID)
	require.Error(t, err)

	reloaded, err := store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)
	assert.Nil(t, reloaded.DeletionRequestedAt, "the mark rolled back with the draw")
}

// selectScoped is the one place the kind gate lives, so its scope errors and its
// column binding are pinned here rather than through each caller.
func TestSelectScopedGatesAndReadsNamedColumns(t *testing.T) {
	ctx := context.Background()
	store := newRawStore(t)
	obj := newRefObject(t, store)

	var gen int64
	var deletionAt sql.NullInt64
	require.NoError(t, store.selectScoped(ctx, testGK, obj.ID,
		`generation, deletion_requested_at`, &gen, &deletionAt))
	assert.Equal(t, obj.Generation, gen)
	assert.False(t, deletionAt.Valid)

	// No columns: the gate alone, which is what checkObjectScoped wants.
	require.NoError(t, store.selectScoped(ctx, testGK, obj.ID, ``))
	assert.ErrorIs(t, store.selectScoped(ctx, testGK, 999999, ``), beehive.ErrNotFound)
	assert.ErrorIs(t, store.selectScoped(ctx, beehive.GroupKind{Kind: "Other"}, obj.ID, ``),
		beehive.ErrWrongKind)
}

// checkObjectScoped resolves a zero-row mark or decrement, so it only ever runs
// after a statement that already succeeded — no fault-injection path reaches it with
// a broken connection. Called directly, which is what whitebox tests are for.
func TestCheckObjectScopedDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	require.Error(t, store.checkObjectScoped(context.Background(), testGK, 1))
}

// Neither branch of a deletion mark decodes the row's blobs — not the UPDATE that
// stamps it (the old RETURNING clause did) and not the probe that resolves a
// zero-row mark (which reads "group"/kind only). An undecodable finalizers column is
// the sharpest way to say so: it fails every full-row read in the store, so if
// either branch still did one, this would error.
func TestDeletionRequestsCreateReadsNoBlobOnEitherBranch(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	// A row with bad finalizers JSON and no deletion_requested_at, so the guard
	// matches and the UPDATE stamps it.
	id := insertBadFinalizersRow(t, store, testGK)

	res, err := store.DeletionRequests().Create(ctx, testGK, id)
	require.NoError(t, err, "the mark binds no blob column and reads none back")
	assert.True(t, res.Marked)

	// The repeat takes the probe, which answers already-pending from metadata alone.
	res, err = store.DeletionRequests().Create(ctx, testGK, id)
	require.NoError(t, err, "the probe resolves the no-op without decoding the row")
	assert.False(t, res.Marked, "the repeat stamps nothing")

	// The probe still reports scope, which is the one thing it must read to answer.
	_, err = store.DeletionRequests().Create(ctx, beehive.GroupKind{Kind: "Other"}, id)
	assert.ErrorIs(t, err, beehive.ErrWrongKind)
}

func TestDeleteObjectDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	err := store.ObjectsDelete(context.Background(), 1)
	require.Error(t, err)
}

func TestScanObjectBadFinalizersJSON(t *testing.T) {
	store := newRawStore(t)
	id := insertBadFinalizersRow(t, store, testGK)

	_, err := store.ObjectsGet(context.Background(), id)
	require.Error(t, err)
}

func TestWithinNestedCommitError(t *testing.T) {
	// A nested Within with a non-nil error from fn propagates through the outer.
	store := newRawStore(t)
	ctx := context.Background()

	sentinel := errors.New("inner error")
	err := store.Within(ctx, func(ctx context.Context) error {
		return store.Within(ctx, func(context.Context) error {
			return sentinel
		})
	})
	assert.ErrorIs(t, err, sentinel)
}

// newRefObject creates a bare object of testGK and returns it. Refs only need
// ids, so no name/spec detail matters.
func newRefObject(t *testing.T, store beehive.Store) *beehive.RawObject {
	t.Helper()
	return newKindObject(t, store, testGK)
}

// refIDs projects an ObjectRef slice to its ids for order-sensitive assertions.
func refIDs(refs []beehive.ObjectRef) []beehive.ObjectID {
	var ids []beehive.ObjectID
	for _, r := range refs {
		ids = append(ids, r.ID)
	}
	return ids
}

// countEdges reads the edges table directly to assert edge presence.
func countEdges(t *testing.T, store *sqliteStore, from, to beehive.ObjectID, relation string) int {
	t.Helper()
	var n int
	require.NoError(t, store.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM edges WHERE from_id = ? AND to_id = ? AND relation = ?`,
		from, to, relation).Scan(&n))
	return n
}

// addEdge declares an edge for test scaffolding: it discards the EdgesAddResult
// and drains the owed-wake stamp every new depends_on edge records — scaffolding
// wants the edge to exist, not the reconcile it buys — keeping the common
// require.NoError(t, addEdge(...)) shape a one-liner with no side effects on the
// owed listings. Tests that assert on the result or the stamp call
// store.EdgesAdd directly.
func addEdge(ctx context.Context, store beehive.Store, from, to beehive.ObjectID, relation beehive.Relation) error {
	res, err := store.Edges().Add(ctx, from, to, relation)
	if err != nil {
		return err
	}
	if res.ReconcileOwedStamped {
		// The decrement is kind-scoped and scaffolding declares edges across kinds, so
		// the source's own kind is needed here — and res.From already carries it,
		// projected from the endpoint check EdgesAdd had to do anyway.
		return store.ReconcileOwed().Decrement(ctx, res.From, from, 1)
	}
	return nil
}

// dropEdge is EdgesDelete for a caller that only needs the edge gone.
func dropEdge(ctx context.Context, store beehive.Store, from, to beehive.ObjectID, relation beehive.Relation) error {
	_, err := store.Edges().Delete(ctx, from, to, relation)
	return err
}

func TestRefsAddInsertsRow(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	require.NoError(t, addEdge(ctx, store, a.ID, b.ID, "depends_on"))
	assert.Equal(t, 1, countEdges(t, store, a.ID, b.ID, "depends_on"))
}

func TestRefsAddIdempotent(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	require.NoError(t, addEdge(ctx, store, a.ID, b.ID, "depends_on"))
	require.NoError(t, addEdge(ctx, store, a.ID, b.ID, "depends_on"))
	assert.Equal(t, 1, countEdges(t, store, a.ID, b.ID, "depends_on"), "re-adding an identical edge is a no-op")
}

func TestRefsAddNonexistentEndpoint(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)

	err := addEdge(ctx, store, a.ID, 9999, "depends_on")
	assert.ErrorIs(t, err, beehive.ErrNotFound, "missing to_id yields ErrNotFound")
	assert.Equal(t, 0, countEdges(t, store, a.ID, 9999, "depends_on"))

	err = addEdge(ctx, store, 9999, a.ID, "depends_on")
	assert.ErrorIs(t, err, beehive.ErrNotFound, "missing from_id yields ErrNotFound")
	assert.Equal(t, 0, countEdges(t, store, 9999, a.ID, "depends_on"))
}

// TestRefsAddReportsEndpoints pins what the endpoint check reports back: both
// endpoints' GroupKinds. The edge is cross-kind, so a caller routing work to
// either end cannot assume its own kind, and it must come from the same
// round-trip as the insert. The two ends are different kinds here, so confusing
// them fails.
func TestRefsAddReportsEndpoints(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	otherGK := beehive.GroupKind{Group: "", Kind: "Other"}
	a := newRefObject(t, store)
	b := newKindObject(t, store, otherGK)

	res, err := store.Edges().Add(ctx, a.ID, b.ID, "depends_on")
	require.NoError(t, err)
	assert.Equal(t, testGK, res.From, "fromID's kind")
	assert.Equal(t, otherGK, res.To, "toID's kind")
	assert.False(t, res.ToDeleting, "a live target")
	assert.Equal(t, 1, countEdges(t, store, a.ID, b.ID, "depends_on"), "this call created the edge")

	res, err = store.Edges().Add(ctx, a.ID, b.ID, "depends_on")
	require.NoError(t, err)
	assert.Equal(t, testGK, res.From, "re-declare reports it too")
	assert.Equal(t, otherGK, res.To)
	assert.Equal(t, 1, countEdges(t, store, a.ID, b.ID, "depends_on"), "the edge already existed; the insert was a no-op")
}

// TestRefsAddReportsADeletingTarget pins the lifecycle bit an owner edge needs:
// a child created under an owner whose cascade already ran must push that owner,
// and this is what tells the caller to.
func TestRefsAddReportsADeletingTarget(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	child := newRefObject(t, store)
	owner := newRefObject(t, store)

	mark, err := store.DeletionRequests().Create(ctx, testGK, owner.ID)
	require.NoError(t, err)
	require.True(t, mark.Marked)

	res, err := store.Edges().Add(ctx, child.ID, owner.ID, "owned_by")
	require.NoError(t, err)
	assert.True(t, res.ToDeleting, "the owner is deletion-pending")
	assert.False(t, res.ReconcileOwedStamped, "owned_by still stamps nothing")
}

// moveTarget writes to the object so its resource_version advances, for tests
// that need a target to have changed since an earlier read.
func moveTarget(t *testing.T, store *sqliteStore, id beehive.ObjectID) {
	t.Helper()
	err := store.Conditions().Set(context.Background(), testGK,
		id, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
}

// TestEdgesAddStampsReconcileOwed covers the stamp's gate: every depends_on edge
// the call creates records one owed wake on fromID, while an owner edge and a
// self-edge record nothing. Unconditional is the property that closes the mid-pass
// declare strand — a stamp survives the load-scoped decrement where the watermark
// clear does not — so the plain-declare arm is the one that must stamp.
func TestEdgesAddStampsReconcileOwed(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	// A new depends_on edge buys the one pass that reads the target, on fromID and
	// only fromID.
	res, err := store.Edges().Add(ctx, a.ID, b.ID, "depends_on")
	require.NoError(t, err)
	assert.True(t, res.ReconcileOwedStamped, "a new depends_on edge stamps")
	assert.Equal(t, int64(1), reconcileOwed(t, store, a.ID), "the stamp is on the dependent, not the target")
	assert.Zero(t, reconcileOwed(t, store, b.ID))

	// An owner edge is not a dependency: no wake is owed for it.
	o := newRefObject(t, store)
	res, err = store.Edges().Add(ctx, o.ID, b.ID, "owned_by")
	require.NoError(t, err)
	assert.False(t, res.ReconcileOwedStamped, "owned_by stamps nothing")
	assert.Zero(t, reconcileOwed(t, store, o.ID))

	// A self-edge stamps nothing: an object's own pass always reads its current
	// self, so there is no wake to deliver — matching every scan, which skips it.
	s := newRefObject(t, store)
	res, err = store.Edges().Add(ctx, s.ID, s.ID, "depends_on")
	require.NoError(t, err)
	assert.False(t, res.ReconcileOwedStamped, "a self-edge stamps nothing")
	assert.Zero(t, reconcileOwed(t, store, s.ID))
}

// TestRefsAddStampsOnlyNewEdge pins the edge-new gate against the statement that
// carries it, the stamp's own NOT EXISTS. Re-declaring an existing edge must not
// stamp — otherwise a controller that re-asserts its dependency set re-fires a
// wake every pass, unthrottled.
func TestRefsAddStampsOnlyNewEdge(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	res, err := store.Edges().Add(ctx, a.ID, b.ID, "depends_on")
	require.NoError(t, err)
	require.True(t, res.ReconcileOwedStamped)

	res, err = store.Edges().Add(ctx, a.ID, b.ID, "depends_on")
	require.NoError(t, err)
	assert.False(t, res.ReconcileOwedStamped, "the edge was already there, so the stamp is suppressed")
	assert.Equal(t, int64(1), reconcileOwed(t, store, a.ID), "still the one wake owed")
}

// probeEdgeInsertOwed records from_id's reconcile_owed at the instant the edge row
// lands. That instant is the only place the ordering inside EdgesAdd is observable:
// once the transaction commits, a stamp written before the insert and one written
// after are indistinguishable.
func probeEdgeInsertOwed(t *testing.T, store *sqliteStore) func() int64 {
	t.Helper()
	ctx := context.Background()
	_, err := store.db.ExecContext(ctx, `CREATE TABLE edge_insert_probe (owed INTEGER)`)
	require.NoError(t, err)
	_, err = store.db.ExecContext(ctx, `
		CREATE TRIGGER edge_insert_probe AFTER INSERT ON edges
		BEGIN
			INSERT INTO edge_insert_probe(owed)
			SELECT reconcile_owed FROM objects WHERE id = NEW.from_id;
		END`)
	require.NoError(t, err)

	return func() int64 {
		t.Helper()
		var owed int64
		require.NoError(t, store.db.QueryRowContext(ctx,
			`SELECT owed FROM edge_insert_probe`).Scan(&owed))
		return owed
	}
}

// TestRefsAddStampsBeforeTheInsert pins the ordering directly, which no other test
// does: the savepoint boundary now unwinds both writes together, so every
// after-the-fact assertion reads the same under either order and an insert-then-stamp
// refactor would go green. The ordering is still load-bearing wherever the boundary
// does not apply, and there the residual flips from a self-draining spurious wake to a
// permanently stranded dependent.
func TestRefsAddStampsBeforeTheInsert(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)
	owedAtInsert := probeEdgeInsertOwed(t, store)

	_, err := store.Edges().Add(ctx, a.ID, b.ID, "depends_on")
	require.NoError(t, err)

	assert.EqualValues(t, 1, owedAtInsert(),
		"the wake must already be stamped on the row when the edge lands")
}

// TestRefsAddStampFailureLeavesNoEdge is one half of the ordering guarantee: the
// stamp fails, and the insert that would have followed it never runs, so there is no
// edge without a wake. RAISE(ABORT) undoes the statement rather than the transaction,
// which is what keeps the ambient transaction committable and puts this in the band
// where ordering — not the savepoint boundary — is what holds.
//
// The boundary covers the common failures now, so this is defence for the case where
// it does not apply. The other half, that the stamp really is issued first, is pinned
// from inside the transaction by TestRefsAddStampsBeforeTheInsert: from out here both
// writes are simply present, and either order looks the same.
func TestRefsAddStampFailureLeavesNoEdge(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)
	// The stamp is the only UPDATE on objects EdgesAdd issues, so blocking those
	// fails it while leaving the endpoint read and the edges insert alone. RAISE(ABORT)
	// undoes the statement, not the transaction, so the outer transaction below is
	// still committable — exactly the failure band where ordering, rather than
	// rollback, is what protects the caller.
	blockObjectUpdates(t, store)

	err := store.Within(ctx, func(ctx context.Context) error {
		if _, err := store.Edges().Add(ctx, a.ID, b.ID, "depends_on"); err != nil {
			return nil // the caller logs and carries on; the outer tx still commits
		}
		return assert.AnError // EdgesAdd must not have succeeded
	})
	require.NoError(t, err)

	assert.Equal(t, 0, countEdges(t, store, a.ID, b.ID, "depends_on"),
		"a failed stamp must leave no edge, committed or not")
	assert.Zero(t, reconcileOwed(t, store, a.ID))
}

// TestRefsAddEdgeFailureUnwindsTheStamp is the other side of the ordering tradeoff.
// Stamp first, insert second admits a reverse residual: the insert aborts, the
// caller swallows EdgesAdd's error and commits the ambient transaction, and a wake
// is owed for an edge that does not exist.
//
// That residual is now gone, and this test is what pins it gone. EdgesAdd self-wraps
// in Within, and a nested Within is a savepoint boundary, so the aborted insert
// unwinds the stamp issued ahead of it — the caller's swallow commits neither. What
// used to be "a self-draining spurious wake" is simply nothing.
//
// The ordering argument it used to illustrate is unaffected and still governs
// EdgesAdd: stamp-then-insert is chosen because the opposite leaves an edge with no
// wake, which nothing re-derives and ObjectsListUnsettledIDs cannot see. Savepoints
// make the failure atomic; they do not make the ordering arbitrary, and this test
// would go back to pinning a spurious wake if the self-wrap were ever removed.
func TestRefsAddEdgeFailureUnwindsTheStamp(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)
	blockEdgeInserts(t, store)

	err := store.Within(ctx, func(ctx context.Context) error {
		if _, err := store.Edges().Add(ctx, a.ID, b.ID, "depends_on"); err != nil {
			return nil // the caller logs and carries on; the outer tx still commits
		}
		return assert.AnError // EdgesAdd must not have succeeded
	})
	require.NoError(t, err)

	assert.Equal(t, 0, countEdges(t, store, a.ID, b.ID, "depends_on"), "the edge did not land")
	assert.Zero(t, reconcileOwed(t, store, a.ID),
		"and neither did the stamp: EdgesAdd's savepoint unwound it with the failed insert")
}

// blockWatermarkDeletes makes every DELETE from dependency_watermarks abort,
// isolating EdgesAdd's watermark clear from the stamp before it and the insert
// after it. The trigger only fires on a row that actually matches, so a test using
// it has to give the dependent a watermark first.
func blockWatermarkDeletes(t *testing.T, store *sqliteStore) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(), `
		CREATE TRIGGER block_watermark_deletes BEFORE DELETE ON dependency_watermarks
		BEGIN SELECT RAISE(ABORT, 'blocked'); END`)
	require.NoError(t, err)
}

// TestRefsAddWatermarkClearFailurePropagates covers the clear's error branch. It
// sits between the stamp and the insert, so a failure there must abort the call
// rather than be swallowed: the edge is what a later pass would read the target
// through, and the residual has to stay on the harmless side of the ordering — a
// stamp with no edge, never an edge with a stale watermark still standing.
func TestRefsAddWatermarkClearFailurePropagates(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	old := newRefObject(t, store)
	a := newDependentObject(t, store, old.ID) // the edge the watermark write gates on
	b := newRefObject(t, store)
	require.NoError(t, store.Dependencies().WatermarkSet(ctx, a.ID, 42))
	blockWatermarkDeletes(t, store)

	_, err := store.Edges().Add(ctx, a.ID, b.ID, "depends_on")
	require.Error(t, err)

	assert.Equal(t, 0, countEdges(t, store, a.ID, b.ID, "depends_on"),
		"the insert runs after the clear, so it never happened")
	against, _, ok := readWatermark(t, store, a.ID)
	require.True(t, ok, "the blocked delete left the row")
	assert.Equal(t, int64(42), against)
}

func TestRefsAddNoVersionBumpNoEvent(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	probe := newWriteProbe(t, store)

	require.NoError(t, addEdge(ctx, store, a.ID, b.ID, "depends_on"))
	probe.expectNone()

	gotA, err := store.ObjectsGet(ctx, a.ID)
	require.NoError(t, err)
	assert.Equal(t, a.ResourceVersion, gotA.ResourceVersion, "a ref edge does not bump the from object")
	gotB, err := store.ObjectsGet(ctx, b.ID)
	require.NoError(t, err)
	assert.Equal(t, b.ResourceVersion, gotB.ResourceVersion, "a ref edge does not bump the to object")
}

func TestDeleteRefRemovesRow(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	require.NoError(t, addEdge(ctx, store, a.ID, b.ID, "depends_on"))
	require.NoError(t, dropEdge(ctx, store, a.ID, b.ID, "depends_on"))
	assert.Equal(t, 0, countEdges(t, store, a.ID, b.ID, "depends_on"))
}

func TestDeleteRefAbsentNoop(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	// No edge exists, and a nonexistent endpoint, are both silent no-ops.
	require.NoError(t, dropEdge(ctx, store, a.ID, b.ID, "depends_on"))
	require.NoError(t, dropEdge(ctx, store, a.ID, 9999, "depends_on"))

	probe := newWriteProbe(t, store)

	require.NoError(t, dropEdge(ctx, store, a.ID, b.ID, "depends_on"))
	probe.expectNone()
}

// markDeleting stamps deletion_requested_at, which is what makes an object
// collectable and what the EdgesDelete gates read on both endpoints.
func markDeleting(t *testing.T, store *sqliteStore, id beehive.ObjectID) {
	t.Helper()
	res, err := store.DeletionRequests().Create(context.Background(), testGK, id)
	require.NoError(t, err)
	require.True(t, res.Marked)
}

func TestDeleteRefReportsTheUnblockedTarget(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	dependent := newRefObject(t, store)
	target := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, dependent.ID, target.ID, "depends_on"))
	markDeleting(t, store, target.ID)

	res, err := store.Edges().Delete(ctx, dependent.ID, target.ID, "depends_on")
	require.NoError(t, err)
	assert.True(t, res.Unblocked, "a live dependent's edge was RESTRICT-blocking the target's collect")
	assert.Equal(t, testGK, res.To, "the push needs the target's kind; edges are cross-kind")
}

func TestDeleteRefReportsNothingForAMissingEdge(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	dependent := newRefObject(t, store)
	target := newRefObject(t, store)
	markDeleting(t, store, target.ID)

	// Never declared, and declared-then-dropped: neither removes anything, so
	// neither can have lifted a block.
	res, err := store.Edges().Delete(ctx, dependent.ID, target.ID, "depends_on")
	require.NoError(t, err)
	assert.False(t, res.Unblocked)

	require.NoError(t, addEdge(ctx, store, dependent.ID, target.ID, "depends_on"))
	require.NoError(t, dropEdge(ctx, store, dependent.ID, target.ID, "depends_on"))
	res, err = store.Edges().Delete(ctx, dependent.ID, target.ID, "depends_on")
	require.NoError(t, err)
	assert.False(t, res.Unblocked, "the second drop removed nothing")
}

func TestDeleteRefReportsNothingForALiveTarget(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	dependent := newRefObject(t, store)
	target := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, dependent.ID, target.ID, "depends_on"))

	res, err := store.Edges().Delete(ctx, dependent.ID, target.ID, "depends_on")
	require.NoError(t, err)
	assert.False(t, res.Unblocked, "a live target has no collect to unblock")
}

func TestDeleteRefReportsNothingForADeletingSource(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	dependent := newRefObject(t, store)
	target := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, dependent.ID, target.ID, "depends_on"))
	markDeleting(t, store, target.ID)
	markDeleting(t, store, dependent.ID)

	res, err := store.Edges().Delete(ctx, dependent.ID, target.ID, "depends_on")
	require.NoError(t, err)
	assert.False(t, res.Unblocked,
		"EdgesHasIncoming already discounts this edge, so dropping it unblocks nothing")
}

// owned_by is never discounted by EdgesHasIncoming, so the source-side condition
// behind Unblocked does not describe it. The edge still goes.
func TestDeleteRefReportsNothingForAnOwnedByEdge(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	child := newRefObject(t, store)
	owner := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, child.ID, owner.ID, beehive.RelationOwnedBy))
	markDeleting(t, store, owner.ID)

	res, err := store.Edges().Delete(ctx, child.ID, owner.ID, beehive.RelationOwnedBy)
	require.NoError(t, err)
	assert.False(t, res.Unblocked)
	assert.Equal(t, 0, countEdges(t, store, child.ID, owner.ID, string(beehive.RelationOwnedBy)))
}

// The probe runs after the DELETE has already landed, so it can find an endpoint
// gone. Nothing is left to push, and that is not an error. Foreign keys are off
// for the insert because the schema is what normally makes this unreachable.
func TestDeleteRefReportsNothingWhenAnEndpointIsGone(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	_, err := store.db.ExecContext(ctx, `PRAGMA foreign_keys=off`)
	require.NoError(t, err)
	_, err = store.db.ExecContext(ctx,
		`INSERT INTO edges (from_id, to_id, relation) VALUES (9001, 9002, 'depends_on')`)
	require.NoError(t, err)
	_, err = store.db.ExecContext(ctx, `PRAGMA foreign_keys=on`)
	require.NoError(t, err)

	res, err := store.Edges().Delete(ctx, 9001, 9002, "depends_on")
	require.NoError(t, err)
	assert.False(t, res.Unblocked)
	assert.Equal(t, 0, countEdges(t, store, 9001, 9002, "depends_on"), "the edge still goes")
}

// A failed probe is reported even though the DELETE is already durable here:
// inside an ambient Within the caller's rollback unwinds it, and a retry then
// pushes properly. Renaming objects fails the probe and nothing before it.
func TestDeleteRefProbeError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, a.ID, b.ID, "depends_on"))
	_, err := store.db.ExecContext(ctx, `ALTER TABLE objects RENAME TO objects_hidden`)
	require.NoError(t, err)

	_, err = store.Edges().Delete(ctx, a.ID, b.ID, "depends_on")
	assert.Error(t, err)
}

func TestDeleteRefJoinsTransaction(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, a.ID, b.ID, "depends_on"))

	sentinel := errors.New("rollback")
	err := store.Within(ctx, func(ctx context.Context) error {
		if err := dropEdge(ctx, store, a.ID, b.ID, "depends_on"); err != nil {
			return err
		}
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)
	assert.Equal(t, 1, countEdges(t, store, a.ID, b.ID, "depends_on"), "the rollback restores the edge")
}

func TestRefsAddJoinsTransaction(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	require.NoError(t, store.Within(ctx, func(ctx context.Context) error {
		return addEdge(ctx, store, a.ID, b.ID, "depends_on")
	}))
	assert.Equal(t, 1, countEdges(t, store, a.ID, b.ID, "depends_on"), "edge is committed with the transaction")
}

func TestRefsAddRollback(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	sentinel := errors.New("rollback")
	err := store.Within(ctx, func(ctx context.Context) error {
		if err := addEdge(ctx, store, a.ID, b.ID, "depends_on"); err != nil {
			return err
		}
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)
	assert.Equal(t, 0, countEdges(t, store, a.ID, b.ID, "depends_on"), "the edge rolled back with the transaction")
}

func TestRefsListIncoming(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)
	c := newRefObject(t, store)

	require.NoError(t, addEdge(ctx, store, a.ID, c.ID, "depends_on"))
	require.NoError(t, addEdge(ctx, store, b.ID, c.ID, "depends_on"))
	// An owned_by edge to c must not show up under a depends_on query.
	require.NoError(t, addEdge(ctx, store, a.ID, c.ID, "owned_by"))

	deps, err := store.Edges().ListIncoming(ctx, c.ID, "depends_on")
	require.NoError(t, err)
	require.Equal(t, []beehive.ObjectRef{
		{ID: a.ID, Group: testGK.Group, Kind: testGK.Kind},
		{ID: b.ID, Group: testGK.Group, Kind: testGK.Kind},
	}, deps)

	none, err := store.Edges().ListIncoming(ctx, a.ID, "depends_on")
	require.NoError(t, err)
	assert.Empty(t, none, "a target with no dependents returns an empty slice, not an error")
}

func TestObjectsListByIncomingRef(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	otherGK := beehive.GroupKind{Kind: "Other"}

	owner := newRefObject(t, store)
	// Two children of testGK plus one of another kind, all owned by owner.
	c2 := newRefObject(t, store)
	c1 := newRefObject(t, store)
	foreign, err := store.ObjectsCreate(ctx, otherGK, beehive.ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	for _, child := range []*beehive.RawObject{c1, c2, foreign} {
		require.NoError(t, addEdge(ctx, store, child.ID, owner.ID, "owned_by"))
	}
	// A depends_on edge into owner must not surface under an owned_by query.
	dep := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, dep.ID, owner.ID, "depends_on"))

	err = store.Conditions().Set(ctx, testGK, c1.ID,
		storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	got, err := store.ObjectsListByIncomingEdge(ctx, testGK, owner.ID, "owned_by")
	require.NoError(t, err)
	require.Len(t, got, 2, "the foreign-kind child and the depends_on referrer are excluded")
	// Ordered by id (c2 was created first), with full rows and conditions attached.
	assert.Equal(t, []beehive.ObjectID{c2.ID, c1.ID}, []beehive.ObjectID{got[0].ID, got[1].ID})
	assert.Equal(t, []byte(`{}`), []byte(got[0].Spec))
	assert.Empty(t, got[0].Conditions)
	require.Len(t, got[1].Conditions, 1)
	assert.Equal(t, "Ready", got[1].Conditions[0].Type)

	none, err := store.ObjectsListByIncomingEdge(ctx, testGK, c1.ID, "owned_by")
	require.NoError(t, err)
	assert.Empty(t, none, "an owner with no children of this kind reads empty")

	missing, err := store.ObjectsListByIncomingEdge(ctx, testGK, 99999, "owned_by")
	require.NoError(t, err)
	assert.Empty(t, missing, "a nonexistent owner reads empty, not ErrNotFound")
}

func TestObjectsListByIncomingRefDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.ObjectsListByIncomingEdge(context.Background(), testGK, 1, "owned_by")
	require.Error(t, err)
}

// TestRefsAddEndpointReadDBError covers the endpoint read failing for a
// reason that is not "no such row" — dropped table rather than missing endpoint,
// so it must surface the driver error rather than ErrNotFound. BeginTx still
// succeeds here, unlike closing the db, which fails before the read is reached.
func TestRefsAddEndpointReadDBError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)
	dropObjects(t, store)

	_, err := store.Edges().Add(ctx, a.ID, b.ID, "depends_on")
	require.Error(t, err)
	assert.NotErrorIs(t, err, beehive.ErrNotFound, "a dropped table is not a missing endpoint")
}

// TestRefsAddInsertDBError covers the insert failing after the endpoint
// read succeeded: the edges table is gone, so both endpoints resolve and only the
// write fails.
func TestRefsAddInsertDBError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)
	_, err := store.db.ExecContext(ctx, `DROP TABLE edges`)
	require.NoError(t, err)

	_, err = store.Edges().Add(ctx, a.ID, b.ID, "depends_on")
	require.Error(t, err)
}

func TestRefsAddDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	require.Error(t, addEdge(context.Background(), store, 1, 2, "depends_on"))
}

func TestDeleteRefDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	require.Error(t, dropEdge(context.Background(), store, 1, 2, "depends_on"))
}

func TestRefsListIncomingDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.Edges().ListIncoming(context.Background(), 1, "depends_on")
	require.Error(t, err)
}

// newConditionObject creates a bare object to hang conditions on.
func newConditionObject(t *testing.T, store beehive.Store, name string) *beehive.RawObject {
	t.Helper()
	obj, err := store.ObjectsCreate(context.Background(), testGK, beehive.ObjectsCreateInput{
		Name: name,
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	return obj
}

// findCondition returns the condition of the given type, or nil.
func findCondition(conds []storeapi.Condition, condType string) *storeapi.Condition {
	for i := range conds {
		if conds[i].Type == condType {
			return &conds[i]
		}
	}
	return nil
}

func TestSetConditionReadBack(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "ready-obj")

	require.NoError(t, store.Conditions().Set(ctx, testGK, obj.ID, storeapi.Condition{
		Type: "Ready", Status: "True", Reason: "Provisioned", Message: "all good",
	}))
	got, err := store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)

	cond := findCondition(got.Conditions, "Ready")
	require.NotNil(t, cond, "Ready condition must be present on the returned object")
	assert.Equal(t, "True", cond.Status)
	assert.Equal(t, "Provisioned", cond.Reason)
	assert.Equal(t, "all good", cond.Message)
}

func TestConditionsSurfaceOnReads(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "multi-read")

	err := store.Conditions().Set(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	// A second, independent type must coexist without clobbering the first.
	err = store.Conditions().Set(ctx, testGK, obj.ID, storeapi.Condition{Type: "Healthy", Status: "False", Reason: "Degraded"})
	require.NoError(t, err)

	assertBoth := func(t *testing.T, conds []storeapi.Condition) {
		t.Helper()
		ready := findCondition(conds, "Ready")
		healthy := findCondition(conds, "Healthy")
		require.NotNil(t, ready)
		require.NotNil(t, healthy)
		assert.Equal(t, "True", ready.Status)
		assert.Equal(t, "False", healthy.Status)
		assert.Equal(t, "Degraded", healthy.Reason)
	}

	byID, err := store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)
	assertBoth(t, byID.Conditions)

	byName, err := store.ObjectsGetByName(ctx, testGK, "multi-read")
	require.NoError(t, err)
	assertBoth(t, byName.Conditions)

	list, err := store.ObjectsList(ctx, testGK)
	require.NoError(t, err)
	require.Len(t, list, 1)
	assertBoth(t, list[0].Conditions)
}

func TestSetConditionTransitionedAt(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "transition")

	err := store.Conditions().Set(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True", Reason: "A"})
	require.NoError(t, err)

	// Backdate transitioned_at to a known sentinel so we can prove preservation
	// (not a same-millisecond coincidence) and detect a fresh overwrite.
	const sentinel = int64(12345)
	backdate := func() {
		_, err := store.db.ExecContext(ctx,
			`UPDATE conditions SET transitioned_at = ? WHERE object_id = ? AND type = 'Ready'`, sentinel, obj.ID)
		require.NoError(t, err)
	}

	// Same status, different reason: transitioned_at is preserved at the sentinel.
	backdate()
	require.NoError(t, store.Conditions().Set(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True", Reason: "B"}))
	got, err := store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, time.UnixMilli(sentinel).UTC(), findCondition(got.Conditions, "Ready").TransitionedAt,
		"same status keeps transitioned_at")

	// Status change: transitioned_at advances to the write's fresh stamp.
	backdate()
	require.NoError(t, store.Conditions().Set(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "False", Reason: "C"}))
	got, err = store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)
	changed := findCondition(got.Conditions, "Ready")
	assert.True(t, changed.TransitionedAt.After(time.UnixMilli(sentinel).UTC()),
		"status change advances transitioned_at past the sentinel")
	assert.Equal(t, changed.TransitionedAt, changed.UpdatedAt,
		"status change stamps transitioned_at = updated_at")
}

func TestSetConditionEmitsAndBumpsResourceVersion(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "watched")

	probe := newWriteProbe(t, store)

	require.NoError(t, store.Conditions().Set(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"}))
	got, err := store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)
	assert.Greater(t, got.ResourceVersion, obj.ResourceVersion, "a condition change bumps resource_version")

	assert.Equal(t, got.ResourceVersion, probe.expectWrite().ResourceVersion,
		"the condition write is observable in the write log")
}

func TestSetConditionNoOpSuppressed(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "noop")

	require.NoError(t, store.Conditions().Set(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True", Reason: "Up"}))
	first, err := store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	// An identical write changes nothing: no resource_version bump, no event.
	require.NoError(t, store.Conditions().Set(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True", Reason: "Up"}))
	again, err := store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, first.ResourceVersion, again.ResourceVersion, "identical condition write is a no-op")
	probe.expectNone()
}

func TestDeleteCondition(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "deletable")

	err := store.Conditions().Set(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	err = store.Conditions().Set(ctx, testGK, obj.ID, storeapi.Condition{Type: "Healthy", Status: "True"})
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	require.NoError(t, store.Conditions().Delete(ctx, testGK, obj.ID, "Ready"))
	got, err := store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)
	assert.Nil(t, findCondition(got.Conditions, "Ready"), "Ready removed")
	require.NotNil(t, findCondition(got.Conditions, "Healthy"), "Healthy untouched")

	assert.Equal(t, got.ResourceVersion, probe.expectWrite().ResourceVersion,
		"the condition removal is observable in the write log")
}

func TestDeleteConditionAbsentIsNoOp(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "absent")

	probe := newWriteProbe(t, store)

	require.NoError(t, store.Conditions().Delete(ctx, testGK, obj.ID, "Ready"))
	got, err := store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, obj.ResourceVersion, got.ResourceVersion, "deleting an absent condition is a no-op")
	probe.expectNone()
}

// TestNonConditionWritesPreserveConditions verifies that mutators which don't touch
// conditions leave them intact, and that ObjectsUpdateSpec — the one that still
// returns a row — returns them assembled, matching Get/List. Otherwise an Update
// result after a spec change would show Conditions == nil.
func TestNonConditionWritesPreserveConditions(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "preserve")
	err := store.Conditions().Set(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	// UpdateStatus returns no row, so there is nothing here to assemble; it must
	// still leave the condition alone, and still emit.
	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, obj.ID, obj.Generation, []byte(`{"v":1}`), 0))
	updated, err := store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, findCondition(updated.Conditions, "Ready"), "a status write must not disturb conditions")
	probe.expectWrite()

	// ObjectsUpdateSpec too.
	spec, _, err := store.ObjectsUpdateSpec(ctx, testGK, obj.ID, []byte(`{"s":1}`), 0)
	require.NoError(t, err)
	require.NotNil(t, findCondition(spec.Conditions, "Ready"), "ObjectsUpdateSpec result carries conditions")
	probe.expectWrite()

	// DeletionRequestsCreate (the row persists; conditions still exist).
	_, err = store.DeletionRequests().Create(ctx, testGK, obj.ID)
	require.NoError(t, err)
	del, err := store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, findCondition(del.Conditions, "Ready"), "a deletion mark must not disturb conditions")
	probe.expectWrite()
}

// TestOnlyUpdateSpecCanFailAssemblingConditions drops the conditions table so the
// post-write condition assembly fails, covering that error branch in scanWritten.
// ObjectsUpdateSpec is the only mutator left that reaches it — the writes that no
// longer return a row assemble nothing, so a missing conditions table cannot fail
// them, which is what the two require.NoErrors here pin.
func TestOnlyUpdateSpecCanFailAssemblingConditions(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "assembly-error")

	_, err := store.db.ExecContext(ctx, `DROP TABLE conditions`)
	require.NoError(t, err)

	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, obj.ID, obj.Generation, []byte(`{}`), 0),
		"a status write does not read conditions, so a missing table cannot fail it")
	_, err = store.DeletionRequests().Create(ctx, testGK, obj.ID)
	require.NoError(t, err, "nor does a deletion mark")

	// Both of ObjectsUpdateSpec's branches assemble conditions, by two different
	// routes, so both fail here. A changed spec writes and scans the row back
	// (scanWritten); an identical one returns the row it read (attachConditions
	// directly). newConditionObject's spec is `{}`, so the order matters: the
	// changed write has to come first for the second call to be the no-op.
	_, _, err = store.ObjectsUpdateSpec(ctx, testGK, obj.ID, []byte(`{"x":1}`), 0)
	require.Error(t, err, "the write path assembles through scanWritten")

	_, _, err = store.ObjectsUpdateSpec(ctx, testGK, obj.ID, []byte(`{"x":1}`), 0)
	require.Error(t, err, "the content no-op assembles onto the row it read")
}

func TestDeleteObjectCascadesConditions(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "cascade")

	err := store.Conditions().Set(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	require.NoError(t, store.ObjectsDelete(ctx, obj.ID))

	var count int
	require.NoError(t, store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM conditions WHERE object_id = ?`, obj.ID).Scan(&count))
	assert.Zero(t, count, "ON DELETE CASCADE removes the object's condition rows")
}

func TestLivenessDowngradedToUnknownBeforeProcessStart(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "liveness")

	// A liveness condition and a store-truth condition, both written "now".
	err := store.Conditions().Set(ctx, testGK, obj.ID,
		storeapi.Condition{Type: "Connected", Status: "True", Liveness: true})
	require.NoError(t, err)
	err = store.Conditions().Set(ctx, testGK, obj.ID,
		storeapi.Condition{Type: "Provisioned", Status: "True"})
	require.NoError(t, err)

	// Simulate a process that started AFTER both writes: the liveness condition
	// is no longer re-confirmed by this process, so it reads as "verifying".
	store.processStart = time.Now().Add(time.Hour)

	got, err := store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)
	live := findCondition(got.Conditions, "Connected")
	truth := findCondition(got.Conditions, "Provisioned")
	require.NotNil(t, live)
	require.NotNil(t, truth)
	assert.Equal(t, "Unknown", live.Status, "stale liveness condition downgrades to Unknown")
	assert.Equal(t, "True", truth.Status, "store-truth condition is unaffected")
}

func TestStaleLivenessReConfirmRefreshes(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "reconfirm")

	err := store.Conditions().Set(ctx, testGK, obj.ID,
		storeapi.Condition{Type: "Connected", Status: "True", Reason: "Dialed", Liveness: true})
	require.NoError(t, err)

	// Backdate the write to before processStart so it reads as stale "verifying".
	_, err = store.db.ExecContext(ctx,
		`UPDATE conditions SET updated_at = 0 WHERE object_id = ? AND type = 'Connected'`, obj.ID)
	require.NoError(t, err)
	got, err := store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)
	require.Equal(t, "Unknown", findCondition(got.Conditions, "Connected").Status, "precondition: reads as verifying")

	// Re-confirming the identical condition must NOT be suppressed as a no-op: the
	// write has to refresh updated_at so the condition is valid in this process
	// again, otherwise it stays downgraded to Unknown forever.
	err = store.Conditions().Set(ctx, testGK, obj.ID,
		storeapi.Condition{Type: "Connected", Status: "True", Reason: "Dialed", Liveness: true})
	require.NoError(t, err)

	got, err = store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, "True", findCondition(got.Conditions, "Connected").Status,
		"re-confirmed liveness condition is no longer downgraded")
}

func TestSetConditionObjectNotFound(t *testing.T) {
	store := newTestStore(t)
	err := store.Conditions().Set(context.Background(), testGK, 999999, storeapi.Condition{
		Type: "Ready", Status: "True",
	})
	assert.ErrorIs(t, err, beehive.ErrNotFound)
}

func TestSetConditionDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	err := store.Conditions().Set(context.Background(), testGK, 1, storeapi.Condition{Type: "Ready", Status: "True"})
	require.Error(t, err)
}

func TestSetConditionInvalidStatusRejected(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "bad-status")

	// The conditions.status CHECK constraint rejects anything outside the enum,
	// surfacing as an error from the upsert.
	err := store.Conditions().Set(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "Bogus"})
	require.Error(t, err)
}

func TestDeleteConditionDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	err := store.Conditions().Delete(context.Background(), testGK, 1, "Ready")
	require.Error(t, err)
}

// breakConditionRowRead inserts a valid condition row for objID, then makes
// conditions.transitioned_at NULL so any later full-row read of it fails inside
// Scan: transitioned_at scans into a non-nullable int64, and "converting NULL to
// int64" is a scan error. Dropping and re-adding the column (rather than just
// dropping it) keeps the column present so the SELECT still prepares — the fault
// surfaces per row in the scan loop, not at QueryContext. STRICT + NOT NULL rules
// out the old trick of storing unconvertible text in the INTEGER column.
func breakConditionRowRead(t *testing.T, store *sqliteStore, objID storeapi.ObjectID) {
	t.Helper()
	ctx := context.Background()
	_, err := store.db.ExecContext(ctx, `
		INSERT INTO conditions (object_id, type, status, transitioned_at, updated_at)
		VALUES (?, 'Ready', 'True', 0, 0)`, objID)
	require.NoError(t, err)
	_, err = store.db.ExecContext(ctx, `ALTER TABLE conditions DROP COLUMN transitioned_at`)
	require.NoError(t, err)
	_, err = store.db.ExecContext(ctx, `ALTER TABLE conditions ADD COLUMN transitioned_at INTEGER`)
	require.NoError(t, err)
}

// TestConditionAssemblyError corrupts a condition row so the read-path scan
// fails, exercising the conditions-assembly error branches in ObjectsGet (via
// loadConditions), ObjectsList (via conditionsByIDs) and ObjectsGetForReconcile,
// which attaches conditions like ObjectsGet and must surface the failure rather
// than hand a reconcile an object missing them.
func TestConditionAssemblyError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "corrupt")

	breakConditionRowRead(t, store, obj.ID)

	_, err := store.ObjectsGet(ctx, obj.ID)
	require.Error(t, err, "ObjectsGet surfaces a conditions scan error")

	_, err = store.ObjectsList(ctx, testGK)
	require.Error(t, err, "ObjectsList surfaces a conditions scan error")

	_, err = store.ObjectsGetForReconcile(ctx, obj.ID)
	require.Error(t, err, "ObjectsGetForReconcile surfaces a conditions scan error")
}

// TestConditionResourceVersionError drops the resource_version sequence so the
// post-write version bump fails. It covers that error branch in both
// ConditionsSet and ConditionsDelete, and asserts the write is atomic with the
// bump: when the bump fails, the condition change is rolled back rather than
// left applied without a version bump or watch event.
func TestConditionResourceVersionError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "rv-error")

	err := store.Conditions().Set(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	_, err = store.db.ExecContext(ctx, `DROP TABLE resource_version_seq`)
	require.NoError(t, err)

	// A real change whose version bump fails: the whole call rolls back.
	err = store.Conditions().Set(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "False"})
	require.Error(t, err)
	got, err := store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)
	ready := findCondition(got.Conditions, "Ready")
	require.NotNil(t, ready, "rolled-back ConditionsSet must not delete the prior condition")
	assert.Equal(t, "True", ready.Status, "rolled-back ConditionsSet must not apply the changed status")

	// A delete whose version bump fails likewise rolls back, leaving the row.
	err = store.Conditions().Delete(ctx, testGK, obj.ID, "Ready")
	require.Error(t, err)
	got, err = store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)
	assert.NotNil(t, findCondition(got.Conditions, "Ready"), "rolled-back ConditionsDelete must leave the condition in place")
}

// ConditionsSet's load is one statement over two tables, so a fault in either
// fails the write before any of it lands. The conditions table goes rather than a
// row corrupted: the load scans every condition column as nullable — it has to,
// they are NULL whenever there is no condition — so no row content can fail it.
func TestConditionsSetLoadError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "condload-broken")

	_, err := store.db.ExecContext(ctx, `DROP TABLE conditions`)
	require.NoError(t, err)

	err = store.Conditions().Set(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "False"})
	require.Error(t, err)
}

// dropSeq removes the resource_version sequence so the next nextResourceVersion
// call (an UPDATE ... RETURNING) fails, while ordinary object reads still work —
// isolating each mutator's version-bump error branch.
func dropSeq(t *testing.T, store *sqliteStore) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(), `DROP TABLE resource_version_seq`)
	require.NoError(t, err)
}

// dropConditions removes the conditions table while the connection stays open, so
// a DELETE/INSERT against it fails inside an already-open transaction.
func dropConditions(t *testing.T, store *sqliteStore) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(), `DROP TABLE conditions`)
	require.NoError(t, err)
}

// dropObjects removes the objects table mid-connection so a scoped read inside an
// open transaction fails (BeginTx still succeeds, unlike closing the db).
func dropObjects(t *testing.T, store *sqliteStore) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(), `DROP TABLE objects`)
	require.NoError(t, err)
}

// TestListObjectsConditionsChunks shrinks the chunk size so a modest result set
// spans several conditions queries, proving conditionsByIDs stays under SQLite's
// bound-parameter limit and merges every chunk into one map.
func TestListObjectsConditionsChunks(t *testing.T) {
	defer func(n int) { idChunkSize = n }(idChunkSize)
	idChunkSize = 2 // 5 objects -> 3 chunks (2, 2, 1)

	store := newRawStore(t)
	ctx := context.Background()
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		obj := newConditionObject(t, store, "chunked-"+name)
		err := store.Conditions().Set(ctx, testGK, obj.ID,
			storeapi.Condition{Type: "Ready", Status: "True"})
		require.NoError(t, err)
	}

	got, err := store.ObjectsList(ctx, testGK)
	require.NoError(t, err)
	require.Len(t, got, 5)
	for _, obj := range got {
		assert.NotNil(t, findCondition(obj.Conditions, "Ready"),
			"object %d kept its condition across chunks", obj.ID)
	}
}

func TestConditionsByIDsQueryError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.conditionsByIDs(context.Background(), []storeapi.ObjectID{1})
	require.Error(t, err)
}

// TestObjectsUpdateSpecResourceVersionError covers ObjectsUpdateSpec's nextResourceVersion
// branch: the scoped read succeeds and the spec differs, then the version bump
// fails because the sequence table is gone.
func TestObjectsUpdateSpecResourceVersionError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	dropSeq(t, store)

	_, _, err := store.ObjectsUpdateSpec(ctx, testGK, obj.ID, []byte(`{"changed":true}`), 0)
	require.Error(t, err)
}

// TestUpdateStatusResourceVersionError covers UpdateStatus's nextResourceVersion
// branch (its first statement inside Within).
func TestUpdateStatusResourceVersionError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	dropSeq(t, store)

	err := store.ObjectsUpdateStatus(ctx, testGK, obj.ID, obj.Generation, []byte(`{}`), 0)
	require.Error(t, err)
}

// blockObjectUpdates makes every UPDATE against objects abort while SELECTs keep
// working, isolating the mutators' UPDATE branch — they read the row first, so
// dropping the table would fail earlier.
func blockObjectUpdates(t *testing.T, store *sqliteStore) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(), `
		CREATE TRIGGER block_object_updates BEFORE UPDATE ON objects
		BEGIN SELECT RAISE(ABORT, 'blocked'); END`)
	require.NoError(t, err)
}

// blockResourceVersionDraws makes advancing the write cursor abort while leaving
// reads of it alone — a BEFORE UPDATE trigger does not fire for the `SELECT value + 1`
// subquery. That isolates the lazy draw markForDeletion runs *after* stamping a row,
// which no whole-connection failure can reach.
func blockResourceVersionDraws(t *testing.T, store *sqliteStore) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(), `
		CREATE TRIGGER block_rv_draws BEFORE UPDATE ON resource_version_seq
		BEGIN SELECT RAISE(ABORT, 'blocked'); END`)
	require.NoError(t, err)
}

// blockWriteLogInserts makes every INSERT into object_writes abort, isolating the
// append that follows a stamp the row already took.
func blockWriteLogInserts(t *testing.T, store *sqliteStore) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(), `
		CREATE TRIGGER block_write_log_inserts BEFORE INSERT ON object_writes
		BEGIN SELECT RAISE(ABORT, 'blocked'); END`)
	require.NoError(t, err)
}

// blockEdgeInserts makes every INSERT into edges abort while leaving the endpoint
// read and the stamp UPDATE alone, isolating EdgesAdd's final statement.
func blockEdgeInserts(t *testing.T, store *sqliteStore) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(), `
		CREATE TRIGGER block_edge_inserts BEFORE INSERT ON edges
		BEGIN SELECT RAISE(ABORT, 'blocked'); END`)
	require.NoError(t, err)
}

// TestObjectsUpdateSpecCrossVersionUpdateError covers ObjectsUpdateSpec's UPDATE branch reached
// through the version gate: identical spec bytes at a higher schema version skip
// the no-op and run the content write, which fails here.
func TestObjectsUpdateSpecCrossVersionUpdateError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	blockObjectUpdates(t, store)

	_, _, err := store.ObjectsUpdateSpec(ctx, testGK, obj.ID, obj.Spec, 1)
	require.Error(t, err)
}

// TestUpdateStatusCrossVersionUpdateError is the status twin: identical status at
// the recorded generation, but a higher schema version, so the version gate sends
// it down the content write rather than suppressing it.
func TestUpdateStatusCrossVersionUpdateError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	status := []byte(`{"ok":true}`)
	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, obj.ID, obj.Generation, status, 1))
	blockObjectUpdates(t, store)

	// obj.Generation is the generation the call above settled at, so re-reading the
	// row to recover it would only echo the argument.
	err := store.ObjectsUpdateStatus(ctx, testGK, obj.ID, obj.Generation, status, 2)
	require.Error(t, err)
}

// TestUpdateStatusHandshakeResourceVersionError covers the nextResourceVersion
// branch on the content-no-op path: the bytes match but the handshake advances to
// a generation the object hadn't settled at, so the version bump still runs.
func TestUpdateStatusHandshakeResourceVersionError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	status := []byte(`{"ok":true}`)
	// Settle at generation 0 first, so the repeat call at the object's real
	// generation is an unsettled no-op rather than the already-settled path.
	err := store.ObjectsUpdateStatus(ctx, testGK, obj.ID, 0, status, 0)
	require.NoError(t, err)
	dropSeq(t, store)

	err = store.ObjectsUpdateStatus(ctx, testGK, obj.ID, obj.Generation, status, 0)
	require.Error(t, err)
}

// TestDeleteConditionScopedReadError covers ConditionsDelete's scoped-read error
// branch: BeginTx succeeds, but the objects table is gone so the read fails.
func TestDeleteConditionScopedReadError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "del-cond-read")
	dropObjects(t, store)

	err := store.Conditions().Delete(ctx, testGK, obj.ID, "Ready")
	require.Error(t, err)
}

// TestDeleteConditionDeleteExecError covers ConditionsDelete's DELETE-exec error
// branch: the object read succeeds but the conditions table is gone.
func TestDeleteConditionDeleteExecError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "del-cond-exec")
	dropConditions(t, store)

	err := store.Conditions().Delete(ctx, testGK, obj.ID, "Ready")
	require.Error(t, err)
}

// TestDeleteFinalizerResourceVersionError covers FinalizersDelete's
// nextResourceVersion branch: a present finalizer is removed (a real change),
// then the version bump fails.
func TestDeleteFinalizerResourceVersionError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name:       uniqueName(),
		Spec:       []byte(`{}`),
		Finalizers: []string{"f"},
	})
	require.NoError(t, err)
	dropSeq(t, store)

	_, err = store.FinalizersDelete(ctx, testGK, obj.ID, "f")
	require.Error(t, err)
}

// TestDeletionRequestsCreateResourceVersionError covers markForDeletion's
// nextResourceVersion branch, reached via DeletionRequestsCreate on a live object whose
// version bump fails.
func TestDeletionRequestsCreateResourceVersionError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	dropSeq(t, store)

	_, err := store.DeletionRequests().Create(ctx, testGK, obj.ID)
	require.Error(t, err)
}

// TestDeletionRequestsCreateFromOwnerQueryError covers the child-lookup query error.
func TestDeletionRequestsCreateFromOwnerQueryError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.DeletionRequests().CreateFromOwner(context.Background(), 1)
	require.Error(t, err)
}

// The three statements a cascade's mark rests on — the range draw, the batched
// stamp, and the log append that must land with it. Each is reported rather than
// swallowed, and each leaves the level unstamped: a swallowed append in
// particular is what would make a write invisible to every watch.
func TestDeletionRequestsCreateFromOwnerMarkErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		block func(*testing.T, *sqliteStore)
	}{
		{"range draw", dropSeq},
		{"batched stamp", blockObjectUpdates},
		{"log append", blockWriteLogInserts},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newRawStore(t)
			ctx := context.Background()
			owner := newRefObject(t, store)
			child := newRefObject(t, store)
			require.NoError(t, addEdge(ctx, store, child.ID, owner.ID, storeapi.RelationOwnedBy))
			tc.block(t, store)

			_, err := store.DeletionRequests().CreateFromOwner(ctx, owner.ID)
			require.Error(t, err)

			meta, err := store.ObjectsGetMeta(ctx, child.ID)
			require.NoError(t, err)
			assert.Nil(t, meta.DeletionRequestedAt, "a failed mark rolls its stamp back with it")
		})
	}
}

// A failed read leaves the mark unpushed, and every chunk after it unread.
func TestUnblockedTargetsDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.unblockedTargets(context.Background(), []storeapi.ObjectID{1})
	require.Error(t, err)
}

func TestListOutgoingRefsDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.EdgesListOutgoing(context.Background(), 1)
	require.Error(t, err)
}

func TestHasIncomingRefsDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.Edges().HasIncoming(context.Background(), 1)
	require.Error(t, err)
}

func TestRefsGroupIncomingByID(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	target := newRefObject(t, store)
	depA := newRefObject(t, store)
	depB := newRefObject(t, store)
	loner := newRefObject(t, store) // points at target via owned_by, not depends_on

	require.NoError(t, addEdge(ctx, store, depA.ID, target.ID, beehive.RelationDependsOn))
	require.NoError(t, addEdge(ctx, store, depB.ID, target.ID, beehive.RelationDependsOn))
	require.NoError(t, addEdge(ctx, store, loner.ID, target.ID, beehive.RelationOwnedBy))

	got, err := store.Edges().GroupIncomingByID(ctx,
		[]beehive.ObjectID{target.ID, depA.ID}, beehive.RelationDependsOn)
	require.NoError(t, err)
	assert.Equal(t, []beehive.ObjectID{depA.ID, depB.ID}, refIDs(got[target.ID]))
	_, ok := got[depA.ID]
	assert.False(t, ok, "a target with no inbound depends_on is absent from the map")

	empty, err := store.Edges().GroupIncomingByID(ctx, nil, beehive.RelationDependsOn)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

func TestRefsByIDsDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	ctx := context.Background()
	_, err := store.Edges().GroupOutgoingByID(ctx, []beehive.ObjectID{1}, beehive.RelationOwnedBy)
	require.Error(t, err)
	_, err = store.Edges().GroupIncomingByID(ctx, []beehive.ObjectID{1}, beehive.RelationDependsOn)
	require.Error(t, err)
}

func TestListOutgoingRefsByRelationDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.Edges().ListOutgoingByRelation(context.Background(), 1, beehive.RelationOwnedBy)
	require.Error(t, err)
}

func TestDeletionRequestsList(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)
	_ = newRefObject(t, store) // not deletion-pending

	for _, id := range []beehive.ObjectID{a.ID, b.ID} {
		_, err := store.DeletionRequests().Create(ctx, testGK, id)
		require.NoError(t, err)
	}

	// A deleting object of another kind: it must appear too, tagged with its own
	// kind. This listing is deliberately cross-kind — it is the GC sweeper's, and
	// the sweeper's whole reason to exist is the kinds no controller watches.
	otherGK := beehive.GroupKind{Group: "", Kind: "Other"}
	other, err := store.ObjectsCreate(ctx, otherGK, beehive.ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	_, err = store.DeletionRequests().Create(ctx, otherGK, other.ID)
	require.NoError(t, err)

	rows, err := store.DeletionRequests().List(ctx)
	require.NoError(t, err)
	// The kind rides along so the sweeper can route on it: a registered kind is
	// enqueued for its controller, a client-only kind collected directly.
	assert.Equal(t, []storeapi.ObjectRef{
		{ID: a.ID, Group: testGK.Group, Kind: testGK.Kind},
		{ID: b.ID, Group: testGK.Group, Kind: testGK.Kind},
		{ID: other.ID, Group: otherGK.Group, Kind: otherGK.Kind},
	}, rows, "every finalizing object, of every kind, each with its own kind")
}

func TestDeletionRequestsListDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	ctx := context.Background()
	_, err := store.DeletionRequests().List(ctx)
	require.Error(t, err)
}

func TestScopedMutatorWrongKind(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store) // kind = testGK
	other := beehive.GroupKind{Kind: "Other"}
	err := store.ObjectsUpdateStatus(ctx, other, obj.ID, 0, []byte(`{}`), 0)
	require.ErrorIs(t, err, beehive.ErrWrongKind)
}

// ObjectWritesListSince replays what a stalled consumer missed: live rows above a
// cursor, in cursor order, bounded by limit. Kind-agnostic on purpose — a
// depends_on edge may point at a kind with no controller, so a per-kind query
// could not name every target whose change was dropped.
func TestObjectWritesListSinceAll(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	otherGK := beehive.GroupKind{Kind: "Other"}

	mk := func(gk beehive.GroupKind) *beehive.RawObject {
		o, err := store.ObjectsCreate(ctx, gk, beehive.ObjectsCreateInput{
			Name: uniqueName(),
			Spec: []byte(`{}`),
		})
		require.NoError(t, err)
		return o
	}
	first := mk(testGK)
	second := mk(otherGK) // a different kind: it must still come back
	third := mk(testGK)

	// Everything above the first object's version, so `first` is excluded: the
	// cursor is what the consumer already processed, not where it wants to start.
	got, _, err := store.ObjectWritesListSinceAll(ctx, first.ResourceVersion, 10)
	require.NoError(t, err)
	assert.Equal(t, []storeapi.ObjectWrite{
		{ID: second.ID, ResourceVersion: second.ResourceVersion,
			Group: otherGK.Group, Kind: otherGK.Kind, Op: storeapi.WriteCreate},
		{ID: third.ID, ResourceVersion: third.ResourceVersion,
			Group: testGK.Group, Kind: testGK.Kind, Op: storeapi.WriteCreate},
	}, got, "cursor-ordered, exclusive of afterRV, spanning kinds")

	// A limit truncates from the low end, so the caller can page forward by taking
	// the last row's version as its next cursor.
	page, _, err := store.ObjectWritesListSinceAll(ctx, first.ResourceVersion, 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	assert.Equal(t, second.ID, page[0].ID, "the oldest missed change comes first")

	next, _, err := store.ObjectWritesListSinceAll(ctx, page[0].ResourceVersion, 1)
	require.NoError(t, err)
	require.Len(t, next, 1)
	assert.Equal(t, third.ID, next[0].ID, "paging forward from the last row's version")

	// Caught up: nothing above the newest version.
	none, _, err := store.ObjectWritesListSinceAll(ctx, third.ResourceVersion, 10)
	require.NoError(t, err)
	assert.Empty(t, none)
}

// A row deleted during the outage is simply absent from the replay rather than
// erroring it. Per edges.to_id's ON DELETE RESTRICT a target cannot be removed
// while anything depends on it, and from_id's CASCADE means a dependent deleted
// first took its own edge with it — so a row that vanished has no dependents left
// to strand.
// A collection is a log entry like any other. Reading live rows made a delete
// invisible — the waker could not see one at all, and a watch had to find it by
// absence.
func TestObjectWritesListSinceAllReportsDeletes(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	base := newRefObject(t, store)
	gone := newRefObject(t, store)
	require.NoError(t, store.ObjectsDelete(ctx, gone.ID))

	got, _, err := store.ObjectWritesListSinceAll(ctx, base.ResourceVersion, 10)
	require.NoError(t, err)
	require.Len(t, got, 2, "the create and the collection of the second object")
	assert.Equal(t, storeapi.WriteCreate, got[0].Op)
	assert.Equal(t, gone.ID, got[1].ID)
	assert.Equal(t, storeapi.WriteDelete, got[1].Op)
	assert.Nil(t, got[1].Final,
		"no row image: this read routes by id and reads current state, so decoding one would be pure cost")
}

func TestObjectWritesListSinceAllDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, _, err := store.ObjectWritesListSinceAll(context.Background(), 0, 10)
	require.Error(t, err)
}

// The page carries the horizon, so a consumer learns from the same read that its
// cursor is below what retention removed. An empty page carries no rows and so
// reports 0 — ObjectWritesMaxVersionAll is what answers the boundary alone.
func TestObjectWritesListSinceAllReportsTheHorizon(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	old := newRefObject(t, store)
	ageOutWriteLog(t, store)
	fresh := newRefObject(t, store)

	page, trimmed, err := store.ObjectWritesListSinceAll(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, page, 1, "only the write that survived the trim")
	assert.Equal(t, fresh.ID, page[0].ID)
	assert.Equal(t, old.ResourceVersion, trimmed, "and the boundary it was read above")

	empty, trimmed, err := store.ObjectWritesListSinceAll(ctx, fresh.ResourceVersion, 10)
	require.NoError(t, err)
	require.Empty(t, empty)
	assert.Zero(t, trimmed, "no rows to carry it")
}

// The horizon is store-wide, so it is the deepest trim over any kind: the waker's
// cursor is store-wide too, and an entry trimmed from any kind is one it never read.
func TestObjectWritesListSinceAllHorizonIsTheMaxAcrossKinds(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	otherGK := beehive.GroupKind{Kind: "Other"}

	newRefObject(t, store) // testGK, trimmed first and so the shallower horizon
	ageOutWriteLog(t, store)
	deeper, err := store.ObjectsCreate(ctx, otherGK, beehive.ObjectsCreateInput{
		Name: uniqueName(), Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	ageOutWriteLog(t, store)
	newRefObject(t, store) // a live row, so the page has something to carry the horizon

	_, trimmed, err := store.ObjectWritesListSinceAll(ctx, 0, 10)
	require.NoError(t, err)
	assert.Equal(t, deeper.ResourceVersion, trimmed, "the deeper of the two kinds")

	_, markTrimmed, err := store.ObjectWritesMaxVersionAll(ctx)
	require.NoError(t, err)
	assert.Equal(t, trimmed, markTrimmed, "both reads agree")
}

// resource_version is monotonic in commit order, which is what makes it usable as
// a resume cursor at all. It holds because the store runs on a single connection:
// the version is drawn inside the write transaction, so with a pool of two a
// transaction could draw 5 and commit after one that drew 6, and a consumer
// resuming from 6 would skip a real change. This test is a guard on that
// assumption, not on new behavior — raising SetMaxOpenConns should fail here
// rather than silently dropping wakes in production.
func TestResourceVersionMonotonicInCommitOrder(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	var prev int64
	for range 20 {
		obj, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
			Name: uniqueName(),
			Spec: []byte(`{}`),
		})
		require.NoError(t, err)
		require.Greater(t, obj.ResourceVersion, prev,
			"each committed write draws a strictly higher version than the one before it")
		prev = obj.ResourceVersion
	}

	// And the sequence never hands back a version already used, even after the
	// highest-versioned row is physically deleted — the counter is standalone, not
	// MAX(objects.resource_version).
	require.NoError(t, store.ObjectsDelete(ctx, 20))
	after := newRefObject(t, store)
	assert.Greater(t, after.ResourceVersion, prev, "a delete cannot make the cursor regress")
}

// A non-positive limit is rejected rather than passed through: SQLite reads a
// negative LIMIT as unbounded, which is the opposite of what such a caller asked
// for, and the preallocation below it would panic on a negative capacity.
func TestObjectWritesListSinceRejectsNonPositiveLimit(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	newRefObject(t, store)

	for _, limit := range []int{0, -1} {
		got, _, err := store.ObjectWritesListSinceAll(ctx, 0, limit)
		require.NoError(t, err)
		assert.Empty(t, got, "limit %d asks for nothing, not for everything", limit)
	}
}

// ObjectsCreate draws the resource version inside its own transaction, so a
// failure there aborts the insert rather than committing an unversioned row.
// Dropping the sequence is the only way in: a closed database fails at BeginTx
// instead, before the version is ever drawn.
//
// There is no delete counterpart: a delete draws no version at all. The row it
// would have stamped is gone, so there is nothing left for a scan of the write
// log to report — removals are derived from a row's absence, not from a version.
func TestObjectWriteVersionDrawFailureAborts(t *testing.T) {
	ctx := context.Background()
	store := newRawStore(t)
	_, err := store.db.ExecContext(ctx, `DROP TABLE resource_version_seq`)
	require.NoError(t, err)
	_, err = store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name: uniqueName(), Spec: []byte(`{}`)})
	require.Error(t, err)
}

// DeletionRequestsCreateFromOwner stamps several children, each drawing its own
// version, so it has to hold one transaction across the lot: a scan of the write
// log reads versions as a cursor, and a cascade whose rows landed out of order
// would let a consumer advance past one it never saw. The in-tree caller already
// wraps it; this pins the public entry point for the callers Store's surface admits.
func TestDeletionRequestsCreateFromOwnerWritesInVersionOrder(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	mk := func() storeapi.ObjectID { return newEventObject(t, store) }
	owner := mk()
	for range 3 {
		require.NoError(t, addEdge(ctx, store, mk(), owner, beehive.RelationOwnedBy))
	}

	probe := newWriteProbe(t, store)

	got, err := store.DeletionRequests().CreateFromOwner(ctx, owner)
	require.NoError(t, err)
	require.Len(t, got.Children, 3)

	// Every version the cascade wrote, as a scan returns them.
	var versions []int64
	for _, w := range probe.expectWrites(3) {
		versions = append(versions, w.ResourceVersion)
	}
	assert.IsIncreasing(t, versions, "the cascade's own writes must not overtake each other")
}

// The level shares one draw, so the hazard is sharing one *value*: the write log
// orders on it, and two children at the same version are two changes a consumer
// cannot sequence. Each child must land on its own value out of the range, and
// the range must be exactly as wide as the level — a draw of N that stamps N rows
// leaves no gap for the waker to warn about.
func TestCascadeGivesEachChildItsOwnVersionOutOfOneDraw(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	mk := func() storeapi.ObjectID { return newEventObject(t, store) }
	owner := mk()
	children := make([]storeapi.ObjectID, 0, 4)
	for range 4 {
		child := mk()
		require.NoError(t, addEdge(ctx, store, child, owner, beehive.RelationOwnedBy))
		children = append(children, child)
	}

	before := seqValue(t, store)
	probe := newWriteProbe(t, store)
	got, err := store.DeletionRequests().CreateFromOwner(ctx, owner)
	require.NoError(t, err)
	require.Len(t, got.Children, len(children))

	assert.Equal(t, before+int64(len(children)), seqValue(t, store),
		"one draw of N, not N draws and not one value shared")

	// The version on each child's row, and the version its log entry claims.
	var rowVersions, logVersions []int64
	for _, id := range children {
		meta, err := store.ObjectsGetMeta(ctx, id)
		require.NoError(t, err)
		rowVersions = append(rowVersions, meta.ResourceVersion)
	}
	for _, w := range probe.expectWrites(len(children)) {
		logVersions = append(logVersions, w.ResourceVersion)
	}
	assert.IsIncreasing(t, rowVersions, "every child took a distinct value, in cascade order")
	assert.Equal(t, rowVersions, logVersions, "each entry claims the version its own row took")
	assert.Equal(t, before+1, rowVersions[0], "the range starts where the counter stood")
}

// A level wider than one chunk still numbers straight through: the draw is for
// the whole level, so a chunk boundary must not restart or overlap the range.
func TestCascadeNumbersChildrenAcrossMarkChunks(t *testing.T) {
	defer func(n int) { markChunkSize = n }(markChunkSize)
	markChunkSize = 2 // 5 children -> 3 chunks (2, 2, 1)

	store := newRawStore(t)
	ctx := context.Background()

	mk := func() storeapi.ObjectID { return newEventObject(t, store) }
	owner := mk()
	for range 5 {
		require.NoError(t, addEdge(ctx, store, mk(), owner, beehive.RelationOwnedBy))
	}

	before := seqValue(t, store)
	probe := newWriteProbe(t, store)
	got, err := store.DeletionRequests().CreateFromOwner(ctx, owner)
	require.NoError(t, err)
	require.Len(t, got.Children, 5)
	for _, ch := range got.Children {
		assert.True(t, ch.Marked, "every child in every chunk is stamped")
	}

	var versions []int64
	for _, w := range probe.expectWrites(5) {
		versions = append(versions, w.ResourceVersion)
	}
	assert.Equal(t, []int64{before + 1, before + 2, before + 3, before + 4, before + 5}, versions,
		"one contiguous range across the chunk boundaries")
}

// The batch assigns a version per candidate before the guard runs, so a row that
// turns out to be pending leaves its value unused. The gap is the accepted cost —
// consumers seek with `>` — but the row must not be reported marked and must not
// reach the log. Driven through the unexported call because the cascade filters
// pending children out before it, so the guard is unreachable from above.
func TestMarkManyForDeletionSkipsAPendingRowAndLeavesAGap(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	live, pending := newEventObject(t, store), newEventObject(t, store)
	_, err := store.DeletionRequests().Create(ctx, testGK, pending)
	require.NoError(t, err)

	before := seqValue(t, store)
	probe := newWriteProbe(t, store)

	var marked map[storeapi.ObjectID]bool
	require.NoError(t, store.Within(ctx, func(ctx context.Context) error {
		marked, err = store.markManyForDeletion(ctx, []storeapi.ObjectID{pending, live})
		return err
	}))

	assert.False(t, marked[pending], "the IS NULL guard, not the caller's read, decides")
	assert.True(t, marked[live])
	assert.Equal(t, before+2, seqValue(t, store), "both candidates drew; only one stamped")

	w := probe.expectWrite()
	assert.Equal(t, live, w.ID)
	assert.Equal(t, before+2, w.ResourceVersion, "the live row kept its own assigned value")
}

// An empty candidate set must not draw: a re-cascade over an already-deleting
// A candidate set the guard rejects entirely stamps nothing, so the batch append
// has nothing to write — and must say so rather than emit a VALUES list with no
// rows, which is not valid SQL. The draw still happened, so the whole range is a
// gap.
func TestMarkManyForDeletionLogsNothingWhenEveryRowIsGuarded(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	var pending []storeapi.ObjectID
	for range 2 {
		id := newEventObject(t, store)
		_, err := store.DeletionRequests().Create(ctx, testGK, id)
		require.NoError(t, err)
		pending = append(pending, id)
	}

	before := seqValue(t, store)
	probe := newWriteProbe(t, store)

	var marked map[storeapi.ObjectID]bool
	require.NoError(t, store.Within(ctx, func(ctx context.Context) error {
		var err error
		marked, err = store.markManyForDeletion(ctx, pending)
		return err
	}))

	assert.Empty(t, marked, "the guard rejected every candidate")
	assert.Equal(t, before+2, seqValue(t, store), "the range was drawn before the guard ran")
	assert.Empty(t, probe.writes(), "nothing stamped, nothing logged")
}

// subtree is the steady state a controller re-runs every reconcile, and a draw
// there is a counter write to stamp nothing.
func TestMarkManyForDeletionDrawsNothingForNoCandidates(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	before := seqValue(t, store)

	marked, err := store.markManyForDeletion(ctx, nil)
	require.NoError(t, err)
	assert.Empty(t, marked)
	assert.Equal(t, before, seqValue(t, store))
}

// The cascade's own listing failure. Reached directly because the exported wrapper
// opens a transaction first, so a closed database now fails at BeginTx instead.
func TestDeletionRequestsCreateFromOwnerListError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.deletionRequestsCreateFromOwner(context.Background(), 1)
	require.Error(t, err)
}

// A failed COMMIT is reported to the caller rather than swallowed. The rollback
// deferred inside Within is a no-op by then, so nothing else can report it: a
// caller that saw a nil error here would believe writes landed that did not.
func TestWithinReportsACommitFailure(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	err := store.Within(ctx, func(ctx context.Context) error {
		// End the transaction underneath database/sql, so its own Commit finds no
		// transaction to commit. This is the one failure Within cannot rule out by
		// construction: the statement it runs last is the one it cannot check first.
		_, err := store.conn(ctx).ExecContext(ctx, `ROLLBACK`)
		return err
	})
	require.Error(t, err, "a failed commit must reach the caller")
}

// Deleting a row that is already gone reports ErrNotFound rather than succeeding
// silently. GC leans on that: two collectors racing the same object both call
// ObjectsDelete, and the loser has to be able to tell "I collected it" from
// "somebody else already did".
func TestObjectsDeleteMissingRowIsNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	obj := newRefObject(t, store)
	require.NoError(t, store.ObjectsDelete(ctx, obj.ID))

	assert.ErrorIs(t, store.ObjectsDelete(ctx, obj.ID), storeapi.ErrNotFound,
		"the second delete finds no row")
	assert.ErrorIs(t, store.ObjectsDelete(ctx, obj.ID+404), storeapi.ErrNotFound,
		"an id that never existed reads the same way")
}

// A physical delete fails while another object still points at the row. That
// RESTRICT is what the GC ordering rests on — a child has to be removed before its
// owner — so the error has to reach the caller rather than being read as success.
func TestObjectsDeleteRefusesAReferencedRow(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	target := newRefObject(t, store)
	dependent := newRefObject(t, store)
	_, err := store.Edges().Add(ctx, dependent.ID, target.ID, beehive.RelationDependsOn)
	require.NoError(t, err)

	err = store.ObjectsDelete(ctx, target.ID)
	require.Error(t, err, "the edge's RESTRICT must block the delete")
	assert.NotErrorIs(t, err, storeapi.ErrNotFound, "the row is there; the constraint is what failed")

	// Dropping the edge releases it, which is the order GC drives: the referrer goes
	// first, and the row it was holding open becomes collectable.
	require.NoError(t, dropEdge(ctx, store, dependent.ID, target.ID, beehive.RelationDependsOn))
	assert.NoError(t, store.ObjectsDelete(ctx, target.ID))
}

// newDependentObject creates an object of testGK with an outgoing depends_on
// edge to target, which is the shape every dependency-watermark test needs: the
// write gates on that edge existing.
func newDependentObject(t *testing.T, store beehive.Store, target beehive.ObjectID) *beehive.RawObject {
	t.Helper()
	obj := newRefObject(t, store)
	require.NoError(t, addEdge(context.Background(), store, obj.ID, target, beehive.RelationDependsOn))
	return obj
}

// readWatermark returns the stored watermark row for id, or ok=false when the
// object has none. It reads the table directly because nothing on the Store
// surface exposes it — DependentsListStaleSince consumes it in a join, and a test
// asserting on the write itself needs the columns.
func readWatermark(t *testing.T, store *sqliteStore, id beehive.ObjectID) (against, at int64, ok bool) {
	t.Helper()
	err := store.db.QueryRowContext(context.Background(),
		`SELECT reconciled_against, reconciled_at FROM dependency_watermarks WHERE object_id = ?`,
		id).Scan(&against, &at)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, false
	}
	require.NoError(t, err)
	return against, at, true
}

// A dependent's first successful pass leaves the cursor it observed.
func TestDependencyWatermarksSetRecordsTheCursor(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	target := newRefObject(t, store)
	dep := newDependentObject(t, store, target.ID)

	require.NoError(t, store.Dependencies().WatermarkSet(ctx, dep.ID, 42))

	against, at, ok := readWatermark(t, store, dep.ID)
	require.True(t, ok, "a dependent's pass records a watermark")
	assert.Equal(t, int64(42), against)
	assert.NotZero(t, at)
}

// The write gates on an outgoing depends_on edge: an object that can never be
// found stale must not get a row the scan would probe forever. owned_by is not
// a dependency, so it does not open the gate either.
func TestDependencyWatermarksSetGatesOnOutgoingDependsOn(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	owner := newRefObject(t, store)

	lone := newRefObject(t, store)
	require.NoError(t, store.Dependencies().WatermarkSet(ctx, lone.ID, 7))
	_, _, ok := readWatermark(t, store, lone.ID)
	assert.False(t, ok, "an object with no edges gets no row")

	owned := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, owned.ID, owner.ID, beehive.RelationOwnedBy))
	require.NoError(t, store.Dependencies().WatermarkSet(ctx, owned.ID, 7))
	_, _, ok = readWatermark(t, store, owned.ID)
	assert.False(t, ok, "owned_by is not a dependency")
}

// A later pass raises the same row rather than failing on the primary key.
func TestDependencyWatermarksSetUpserts(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	dep := newDependentObject(t, store, newRefObject(t, store).ID)

	require.NoError(t, store.Dependencies().WatermarkSet(ctx, dep.ID, 10))
	require.NoError(t, store.Dependencies().WatermarkSet(ctx, dep.ID, 20))

	against, _, ok := readWatermark(t, store, dep.ID)
	require.True(t, ok)
	assert.Equal(t, int64(20), against)
}

// The FK guard: gcCollect can remove the object between the load and this write,
// taking its edges with it. The gate then finds no edge and nothing is inserted,
// so a racing delete costs a no-op rather than "FOREIGN KEY constraint failed".
func TestDependencyWatermarksSetSkipsCollectedObject(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	dep := newDependentObject(t, store, newRefObject(t, store).ID)
	require.NoError(t, store.ObjectsDelete(ctx, dep.ID))

	require.NoError(t, store.Dependencies().WatermarkSet(ctx, dep.ID, 5))

	_, _, ok := readWatermark(t, store, dep.ID)
	assert.False(t, ok)
}

// It writes no objects row at all, so recording a reconcile cannot put the object
// back into the waker's scan and wake every dependent of it.
func TestDependencyWatermarksSetBumpsNoResourceVersion(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	dep := newDependentObject(t, store, newRefObject(t, store).ID)
	probe := newWriteProbe(t, store)

	require.NoError(t, store.Dependencies().WatermarkSet(ctx, dep.ID, 9))

	probe.expectNone()
}

// Derived state with no claim on the object's lifetime: the row goes with it.
func TestDependencyWatermarksCascadeOnObjectDelete(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	dep := newDependentObject(t, store, newRefObject(t, store).ID)
	require.NoError(t, store.Dependencies().WatermarkSet(ctx, dep.ID, 3))

	require.NoError(t, store.ObjectsDelete(ctx, dep.ID))

	_, _, ok := readWatermark(t, store, dep.ID)
	assert.False(t, ok)
}

// stampWatermarkAt back-dates id's reconciled_at to a sentinel, so a later
// assertion can tell a suppressed write (the sentinel survives) from one that
// rewrote the row.
func stampWatermarkAt(t *testing.T, store *sqliteStore, id beehive.ObjectID, at int64) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(),
		`UPDATE dependency_watermarks SET reconciled_at = ? WHERE object_id = ?`, at, id)
	require.NoError(t, err)
}

// A write arriving out of order cannot regress the cursor and un-converge a
// dependent that has already reconciled against a later point.
func TestDependencyWatermarksSetNeverRegresses(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	dep := newDependentObject(t, store, newRefObject(t, store).ID)

	require.NoError(t, store.Dependencies().WatermarkSet(ctx, dep.ID, 20))
	require.NoError(t, store.Dependencies().WatermarkSet(ctx, dep.ID, 5))

	against, _, ok := readWatermark(t, store, dep.ID)
	require.True(t, ok)
	assert.Equal(t, int64(20), against, "a lower cursor leaves the stored value alone")
}

// reconciled_at is guarded by the same predicate as the cursor, so a pass that
// observed no new store state writes nothing at all — no page dirtied, and no
// timestamp that could be misread as a reconcile heartbeat.
func TestDependencyWatermarksSetMovesReconciledAtOnlyWithTheCursor(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	dep := newDependentObject(t, store, newRefObject(t, store).ID)
	const sentinel = int64(1)

	require.NoError(t, store.Dependencies().WatermarkSet(ctx, dep.ID, 10))
	stampWatermarkAt(t, store, dep.ID, sentinel)

	require.NoError(t, store.Dependencies().WatermarkSet(ctx, dep.ID, 10))
	_, at, ok := readWatermark(t, store, dep.ID)
	require.True(t, ok)
	assert.Equal(t, sentinel, at, "re-applying the same cursor rewrites nothing")

	require.NoError(t, store.Dependencies().WatermarkSet(ctx, dep.ID, 11))
	against, at, ok := readWatermark(t, store, dep.ID)
	require.True(t, ok)
	assert.Equal(t, int64(11), against)
	assert.Greater(t, at, sentinel, "an advancing cursor carries the timestamp with it")
}

// readDriverCursor returns the stored cursor row for name, or ok=false when no
// row exists. It reads the table directly, mirroring readWatermark: nothing on
// the Store surface exposes updated_at, which the suppression test needs.
func readDriverCursor(t *testing.T, store *sqliteStore, name string) (cursor, updatedAt int64, ok bool) {
	t.Helper()
	err := store.db.QueryRowContext(context.Background(),
		`SELECT cursor, updated_at FROM driver_cursors WHERE name = ?`, name).Scan(&cursor, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, false
	}
	require.NoError(t, err)
	return cursor, updatedAt, true
}

// stampDriverCursorAt back-dates name's updated_at to a sentinel, so a later
// assertion can tell a suppressed write (the sentinel survives) from one that
// rewrote the row.
func stampDriverCursorAt(t *testing.T, store *sqliteStore, name string, at int64) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(),
		`UPDATE driver_cursors SET updated_at = ? WHERE name = ?`, at, name)
	require.NoError(t, err)
}

// A driver that has never run yet finds nothing, and that is the normal state
// on every fresh database — not an error, which is why DriverCursorsGet reports
// it as ok=false rather than through ErrNotFound (see the spec on the ok bool).
func TestDriverCursorsGetReportsAbsence(t *testing.T) {
	store := newRawStore(t)

	_, ok, err := store.DriverCursors().Get(context.Background(), "dependency_waker")
	require.NoError(t, err)
	assert.False(t, ok, "no driver has ever persisted a cursor here")
}

// The basic round trip: what Set writes, Get reads back.
func TestDriverCursorsSetThenGetRoundTrips(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	require.NoError(t, store.DriverCursors().Set(ctx, "dependency_waker", 42))

	cursor, ok, err := store.DriverCursors().Get(ctx, "dependency_waker")
	require.NoError(t, err)
	require.True(t, ok)
	assert.EqualValues(t, 42, cursor)
}

// A write arriving out of order cannot regress the cursor, the same guarantee
// DependencyWatermarksSet gives its own cursor and for the same reason: an
// out-of-order write must not un-scan history the stored cursor already covers.
func TestDriverCursorsSetNeverRegresses(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	require.NoError(t, store.DriverCursors().Set(ctx, "dependency_waker", 20))
	require.NoError(t, store.DriverCursors().Set(ctx, "dependency_waker", 5))

	// The public getter, not readDriverCursor: unlike the watermark table, the
	// Store surface exposes exactly what this asserts on.
	cursor, ok, err := store.DriverCursors().Get(ctx, "dependency_waker")
	require.NoError(t, err)
	require.True(t, ok)
	assert.EqualValues(t, 20, cursor, "a lower cursor leaves the stored value alone")
}

// updated_at is guarded by the same predicate as the cursor, so a tick that made
// no progress dirties no page — the property the waker's idle-store cost model
// depends on.
func TestDriverCursorsSetSuppressesTheWriteWhenTheCursorDoesNotAdvance(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	const sentinel = int64(1)

	require.NoError(t, store.DriverCursors().Set(ctx, "dependency_waker", 10))
	stampDriverCursorAt(t, store, "dependency_waker", sentinel)

	require.NoError(t, store.DriverCursors().Set(ctx, "dependency_waker", 10))
	_, updatedAt, ok := readDriverCursor(t, store, "dependency_waker")
	require.True(t, ok)
	assert.Equal(t, sentinel, updatedAt, "re-applying the same cursor rewrites nothing")

	require.NoError(t, store.DriverCursors().Set(ctx, "dependency_waker", 11))
	cursor, updatedAt, ok := readDriverCursor(t, store, "dependency_waker")
	require.True(t, ok)
	assert.EqualValues(t, 11, cursor)
	assert.Greater(t, updatedAt, sentinel, "an advancing cursor carries the timestamp with it")
}

// A read that fails for any reason other than "no such row" is an error, not an
// absence: reporting ok=false would tell the waker to seed from the write log's
// max, silently discarding a cursor that is still there.
func TestDriverCursorsGetDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	_, ok, err := store.DriverCursors().Get(context.Background(), "dependency_waker")
	require.Error(t, err)
	assert.False(t, ok, "a failed read reports no cursor, but as an error rather than an absence")
}

// Zero is a legitimate cursor — it is what an empty write log reports — so the
// upsert has to create the row for it rather than read it as nothing worth
// storing. The monotone guard applies only where a row already exists, which is
// what makes the waker's seed write land on a fresh store.
func TestDriverCursorsSetStoresAZeroCursor(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	require.NoError(t, store.DriverCursors().Set(ctx, "dependency_waker", 0))

	cursor, ok, err := store.DriverCursors().Get(ctx, "dependency_waker")
	require.NoError(t, err)
	require.True(t, ok, "a zero cursor still creates the row")
	assert.Zero(t, cursor)
}

// Two names are two rows: a second driver's cursor must not collide with or
// clamp against the first's.
func TestDriverCursorsSetKeysByName(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	require.NoError(t, store.DriverCursors().Set(ctx, "dependency_waker", 10))
	require.NoError(t, store.DriverCursors().Set(ctx, "some_other_driver", 999))

	cursor, ok, err := store.DriverCursors().Get(ctx, "dependency_waker")
	require.NoError(t, err)
	require.True(t, ok)
	assert.EqualValues(t, 10, cursor, "a second driver's cursor leaves this one alone")
}

// cursorNow is the store-wide write cursor as a reconcile's load would observe
// it — what a dependent records as its watermark.
func cursorNow(t *testing.T, store beehive.Store) int64 {
	t.Helper()
	rv, _, err := store.ObjectWritesMaxVersionAll(context.Background())
	require.NoError(t, err)
	return rv
}

// staleIDs is the staleness listing over testGK from the beginning, unbounded
// above, projected to ids and deduped. The listing returns one row per
// (target, dependent) pair, so a dependent with two moved targets appears twice;
// these assertions are about which objects are owed a pass.
func staleIDs(t *testing.T, store beehive.Store) []beehive.ObjectID {
	t.Helper()
	return dedupeIDs(refIDs(staleRefs(t, store, testGK)))
}

// staleRefs is staleIDs without the projection, for the tests that assert on the
// rows themselves.
func staleRefs(t *testing.T, store beehive.Store, kinds ...beehive.GroupKind) []beehive.ObjectRef {
	t.Helper()
	refs, _, err := store.Dependencies().ListStaleSince(context.Background(),
		kinds, beehive.StalePos{}, math.MaxInt64, 100)
	require.NoError(t, err)
	return refs
}

func dedupeIDs(ids []beehive.ObjectID) []beehive.ObjectID {
	seen := make(map[beehive.ObjectID]struct{}, len(ids))
	var out []beehive.ObjectID
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// A dependent is stale exactly while a target it depends on sits above its
// watermark, and converges out of the listing once it records a later one.
func TestDependentsListStaleSinceFindsMovedTargets(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	target := newRefObject(t, store)
	dep := newDependentObject(t, store, target.ID)
	require.NoError(t, store.Dependencies().WatermarkSet(ctx, dep.ID, cursorNow(t, store)))
	require.Empty(t, staleIDs(t, store), "nothing has moved since the watermark")

	moveTarget(t, store, target.ID)
	assert.Equal(t, []beehive.ObjectID{dep.ID}, staleIDs(t, store))

	require.NoError(t, store.Dependencies().WatermarkSet(ctx, dep.ID, cursorNow(t, store)))
	assert.Empty(t, staleIDs(t, store), "a pass that observed the change settles it")
}

// TestDependentsListStaleSincePagesInsideAFanOut pins the cursor form's order and
// its resume point. The scan drives from targets written above the cursor, so a
// target with more dependents than one page has to resume inside its own fan-out
// — cutting at a target boundary would drop the rest of it.
func TestDependentsListStaleSincePagesInsideAFanOut(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	target := newRefObject(t, store)
	a := newDependentObject(t, store, target.ID)
	b := newDependentObject(t, store, target.ID)
	c := newDependentObject(t, store, target.ID)

	refs, pos, err := store.Dependencies().ListStaleSince(ctx, []beehive.GroupKind{testGK}, beehive.StalePos{}, markNow(t, store), 2)
	require.NoError(t, err)
	require.Equal(t, []beehive.ObjectID{a.ID, b.ID}, refIDs(refs), "the cap cuts mid fan-out")

	refs, _, err = store.Dependencies().ListStaleSince(ctx, []beehive.GroupKind{testGK}, pos, markNow(t, store), 2)
	require.NoError(t, err)
	assert.Equal(t, []beehive.ObjectID{c.ID}, refIDs(refs), "the next page resumes inside the same target")
}

// markNow is the pre-scan mark a sweep would read.
func markNow(t *testing.T, store *sqliteStore) int64 {
	t.Helper()
	mark, err := store.ResourceVersionsMaxIssued(context.Background())
	require.NoError(t, err)
	return mark
}

// TestDependentsListStaleSinceStopsAtTheMark is what makes a sweep finite. A
// target written after the sweep read its mark is above the bound and belongs to
// the next sweep. Without it a store taking writes faster than the sweep pages
// never reaches a short page, so the sweep never ends and its cursor never moves.
func TestDependentsListStaleSinceStopsAtTheMark(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	kinds := []beehive.GroupKind{testGK}
	target := newRefObject(t, store)
	dep := newDependentObject(t, store, target.ID)
	mark := markNow(t, store)

	refs, _, err := store.Dependencies().ListStaleSince(ctx, kinds, beehive.StalePos{}, mark, 100)
	require.NoError(t, err)
	require.Equal(t, []beehive.ObjectID{dep.ID}, refIDs(refs), "in scope as of the mark")

	// A write landing while the sweep runs.
	moveTarget(t, store, target.ID)

	refs, _, err = store.Dependencies().ListStaleSince(ctx, kinds, beehive.StalePos{}, mark, 100)
	require.NoError(t, err)
	assert.Empty(t, refs, "the target moved above the mark, so this sweep leaves it")

	refs, _, err = store.Dependencies().ListStaleSince(ctx, kinds, beehive.StalePos{}, markNow(t, store), 100)
	require.NoError(t, err)
	assert.Equal(t, []beehive.ObjectID{dep.ID}, refIDs(refs), "and the next sweep picks it up")
}

// TestDependentsListStaleSinceIsEmptyWithoutKindsOrLimit: no kinds means no
// reconcile loop to enqueue into, and a non-positive limit asks for nothing.
// Both answer without reading, and hand the cursor back unmoved so a caller
// cannot mistake the empty answer for progress.
func TestDependentsListStaleSinceIsEmptyWithoutKindsOrLimit(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	newDependentObject(t, store, newRefObject(t, store).ID)
	after := beehive.StalePos{TargetVersion: 7, TargetID: 2, DependentID: 3}

	refs, pos, err := store.Dependencies().ListStaleSince(ctx, nil, after, markNow(t, store), 100)
	require.NoError(t, err)
	assert.Empty(t, refs, "no kinds, nothing to enqueue into")
	assert.Equal(t, after, pos)

	refs, pos, err = store.Dependencies().ListStaleSince(ctx, []beehive.GroupKind{testGK}, after, markNow(t, store), 0)
	require.NoError(t, err)
	assert.Empty(t, refs, "a non-positive limit asks for nothing")
	assert.Equal(t, after, pos)
}

// TestDependentsListStaleSinceQueryError: a read that fails is an error, never
// an empty page — the sweep holds its cursor on it, where an empty page would
// let the cursor move past a range nobody read.
func TestDependentsListStaleSinceQueryError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	_, _, err := store.Dependencies().ListStaleSince(context.Background(),
		[]beehive.GroupKind{testGK}, beehive.StalePos{}, 9000, 10)

	assert.Error(t, err)
}

// TestDependentsListStaleSinceDrivesFromTheVersionIndex is the cost assertion.
// The cursor only pays off if the scan seeks targets through idx_objects_rv; a
// plan that starts anywhere else reads the whole graph again and the cursor buys
// nothing. The CROSS JOINs in the query are what hold this.
func TestDependentsListStaleSinceDrivesFromTheVersionIndex(t *testing.T) {
	store := newRawStore(t)
	newDependentObject(t, store, newRefObject(t, store).ID)

	plan := queryPlan(t, store, `
		SELECT t.resource_version, t.id, e.from_id, d."group", d.kind
		  FROM objects t
		  CROSS JOIN edges e ON e.to_id = t.id AND e.relation = 'depends_on'
		  CROSS JOIN objects d ON d.id = e.from_id
		  LEFT JOIN dependency_watermarks c ON c.object_id = e.from_id
		 WHERE (t.resource_version, t.id, e.from_id) > (?, ?, ?)
		   AND t.resource_version <= ?
		   AND e.from_id != e.to_id
		   AND (d."group", d.kind) IN (VALUES (?, ?))
		   AND (c.reconciled_against IS NULL OR t.resource_version > c.reconciled_against)
		 ORDER BY t.resource_version, t.id, e.from_id
		 LIMIT ?`,
		int64(0), int64(0), int64(0), int64(9000), testGK.Group, testGK.Kind, 10)

	assert.Contains(t, plan, "idx_objects_rv", "the scan must seek targets by version:\n"+plan)
	assert.NotContains(t, plan, "SCAN t", "and must not read every object:\n"+plan)
}

// TestDependentsListStaleSinceSkipsConvergedAndSpentPositions covers the two ways
// the cursor form returns nothing: the cursor is already past every target, and
// the dependent has observed the target it depends on. The first is what makes an
// idle sweep cost one indexed range read.
func TestDependentsListStaleSinceSkipsConvergedAndSpentPositions(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	target := newRefObject(t, store)
	dep := newDependentObject(t, store, target.ID)
	kinds := []beehive.GroupKind{testGK}

	refs, pos, err := store.Dependencies().ListStaleSince(ctx, kinds, beehive.StalePos{}, markNow(t, store), 100)
	require.NoError(t, err)
	require.Equal(t, []beehive.ObjectID{dep.ID}, refIDs(refs), "no watermark counts as stale")

	refs, _, err = store.Dependencies().ListStaleSince(ctx, kinds, pos, markNow(t, store), 100)
	require.NoError(t, err)
	assert.Empty(t, refs, "the scan does not re-read the row it just returned")

	require.NoError(t, store.Dependencies().WatermarkSet(ctx, dep.ID, cursorNow(t, store)))
	refs, _, err = store.Dependencies().ListStaleSince(ctx, kinds, beehive.StalePos{}, markNow(t, store), 100)
	require.NoError(t, err)
	assert.Empty(t, refs, "a dependent that observed its target is not returned")
}

// A *new* depends_on edge invalidates the dependent's watermark, which the
// declare drops: a cursor recorded over a smaller dependency set cannot speak for
// a target just added — one whose resource_version may sit below it, where the
// stale scan would read converged. The wake stamp is what guarantees the
// dependent a pass; the clear keeps the derived state honest until that pass
// rewrites it.
func TestRefsAddClearsTheDependentsWatermark(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	target := newRefObject(t, store) // never moves again
	dep := newDependentObject(t, store, newRefObject(t, store).ID)
	require.NoError(t, store.Dependencies().WatermarkSet(ctx, dep.ID, cursorNow(t, store)))
	require.Empty(t, staleIDs(t, store), "the dependent has converged")

	// A third party declares the edge on the dependent's behalf.
	require.NoError(t, addEdge(ctx, store, dep.ID, target.ID, beehive.RelationDependsOn))

	_, _, ok := readWatermark(t, store, dep.ID)
	assert.False(t, ok, "the watermark no longer describes the dependency set")
	assert.Equal(t, []beehive.ObjectID{dep.ID}, staleIDs(t, store),
		"so the next stale-dependents pass reconciles against the new target")
}

// Gated on the same edge-new test as the wake stamp, so a controller that
// re-asserts its dependency set every pass pays for the first declare and nothing
// after it. Without the gate this would re-stale the dependent on every pass,
// which is the loop the edge-new gate exists to avoid.
func TestRefsAddKeepsTheWatermarkOnAReDeclaredEdge(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	target := newRefObject(t, store)
	dep := newDependentObject(t, store, target.ID)
	require.NoError(t, store.Dependencies().WatermarkSet(ctx, dep.ID, cursorNow(t, store)))

	require.NoError(t, addEdge(ctx, store, dep.ID, target.ID, beehive.RelationDependsOn))

	_, _, ok := readWatermark(t, store, dep.ID)
	assert.True(t, ok, "re-asserting an existing edge changes nothing")
	assert.Empty(t, staleIDs(t, store))
}

// An owner edge carries no staleness: the watermark tracks depends_on targets, and
// the cascade reaches children through the GC sweeper rather than through a wake.
func TestRefsAddKeepsTheWatermarkOnAnOwnerEdge(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	owner := newRefObject(t, store)
	dep := newDependentObject(t, store, newRefObject(t, store).ID)
	require.NoError(t, store.Dependencies().WatermarkSet(ctx, dep.ID, cursorNow(t, store)))

	require.NoError(t, addEdge(ctx, store, dep.ID, owner.ID, beehive.RelationOwnedBy))

	_, _, ok := readWatermark(t, store, dep.ID)
	assert.True(t, ok)
	assert.Empty(t, staleIDs(t, store))
}

// A self-edge is excluded from the staleness scan, so clearing for one would drop a
// watermark that edge could never re-derive — the object would just be stale
// against its other targets for a pass, for nothing.
func TestRefsAddKeepsTheWatermarkOnASelfEdge(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	dep := newDependentObject(t, store, newRefObject(t, store).ID)
	require.NoError(t, store.Dependencies().WatermarkSet(ctx, dep.ID, cursorNow(t, store)))

	require.NoError(t, addEdge(ctx, store, dep.ID, dep.ID, beehive.RelationDependsOn))

	_, _, ok := readWatermark(t, store, dep.ID)
	assert.True(t, ok)
	assert.Empty(t, staleIDs(t, store))
}

// A dependent that has never reconciled against a known point cannot have
// converged, mirroring how the unsettled index treats a NULL observed_generation
// — and it is what makes the table need no backfill.
func TestDependentsListStaleSinceTreatsMissingWatermarkAsStale(t *testing.T) {
	store := newRawStore(t)
	dep := newDependentObject(t, store, newRefObject(t, store).ID)

	assert.Equal(t, []beehive.ObjectID{dep.ID}, staleIDs(t, store))
}

// A self-dependent object would be stale against itself for an extra pass every
// time its own reconcile writes anything, so the edge is excluded — the same
// reason the waker skips it.
func TestDependentsListStaleSinceExcludesSelfEdges(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, obj.ID, obj.ID, beehive.RelationDependsOn))
	require.Empty(t, staleIDs(t, store))

	// The case the exclusion is for: its own reconcile writes, which raises its own
	// resource_version above any watermark it just recorded.
	moveTarget(t, store, obj.ID)

	assert.Empty(t, staleIDs(t, store), "an object cannot be stale against itself")
}

// Only a registered kind has a reconcile loop to enqueue into, so a client-only
// dependent — stale forever, since nothing ever writes it a watermark — is left
// out rather than re-scanned on every pass. Registering the kind later is all it
// takes for the same row to appear.
func TestDependentsListStaleSinceFiltersByKind(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	other := beehive.GroupKind{Kind: "Gadget"}
	target := newRefObject(t, store)
	dep, err := store.ObjectsCreate(ctx, other, beehive.ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	require.NoError(t, addEdge(ctx, store, dep.ID, target.ID, beehive.RelationDependsOn))

	assert.Empty(t, staleIDs(t, store), "a kind with no controller is not listed")

	assert.Equal(t, []beehive.ObjectID{dep.ID}, refIDs(staleRefs(t, store, testGK, other)),
		"and appears once its kind is registered")
}

// The kind filter binds the dependent alone. A registered object may depend on a
// client-only target — the whole reason the waker's scan is store-wide — so
// narrowing the scan to edges with two registered endpoints would silently strand
// every dependent of a client-only target. This is the test that fails if someone
// does that.
func TestDependentsListStaleSinceFindsDependentsOfUnregisteredTargets(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	target, err := store.ObjectsCreate(ctx, beehive.GroupKind{Group: "", Kind: "Gadget"}, beehive.ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	dep := newDependentObject(t, store, target.ID)
	require.NoError(t, store.Dependencies().WatermarkSet(ctx, dep.ID, cursorNow(t, store)))
	require.Empty(t, staleIDs(t, store))

	_, _, err = store.ObjectsUpdateSpec(ctx, beehive.GroupKind{Kind: "Gadget"}, target.ID, []byte(`{"v":2}`), 0)
	require.NoError(t, err)

	assert.Equal(t, []beehive.ObjectID{dep.ID}, staleIDs(t, store),
		"a registered dependent of a client-only target is still owed a pass")
}

// A dependent with several stale targets is returned once per target, not once
// overall: a row is a (target, dependent) pair, because the resume position needs
// both. The sweep stamps and enqueues, and both fold a duplicate — so this is a
// contract the caller must tolerate, not one the query should hide with a
// GROUP BY it cannot afford.
func TestDependentsListStaleSinceReturnsAPairPerMovedTarget(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	first, second := newRefObject(t, store), newRefObject(t, store)
	dep := newDependentObject(t, store, first.ID)
	require.NoError(t, addEdge(ctx, store, dep.ID, second.ID, beehive.RelationDependsOn))

	assert.Equal(t, []beehive.ObjectID{dep.ID, dep.ID}, refIDs(staleRefs(t, store, testGK)),
		"one row for each target above the dependent's watermark")
	assert.Equal(t, []beehive.ObjectID{dep.ID}, staleIDs(t, store), "and one object owed a pass")
}

// The reconcile load carries the write cursor as of the same statement that read
// the object, which is the value a successful pass records: every dependency read
// the controller makes happens after this returns, so the cursor is at or below
// the true one when it does.
func TestObjectsGetForReconcileReturnsTheWriteCursor(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)

	load, err := store.ObjectsGetForReconcile(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, obj.ID, load.Object.ID)
	assert.Equal(t, cursorNow(t, store), load.Cursor)

	moveTarget(t, store, obj.ID)
	load, err = store.ObjectsGetForReconcile(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, cursorNow(t, store), load.Cursor, "the cursor tracks the store, not the object")
}

// HasDependencies is the flag that lets a reconcile of an object with no
// dependencies skip the watermark write entirely and never take the write lock.
// owned_by is not a dependency.
func TestObjectsGetForReconcileReportsHasDependencies(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	target := newRefObject(t, store)

	dep := newDependentObject(t, store, target.ID)
	load, err := store.ObjectsGetForReconcile(ctx, dep.ID)
	require.NoError(t, err)
	assert.True(t, load.HasDependencies)

	owned := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, owned.ID, target.ID, beehive.RelationOwnedBy))
	load, err = store.ObjectsGetForReconcile(ctx, owned.ID)
	require.NoError(t, err)
	assert.False(t, load.HasDependencies, "owned_by is not a dependency")

	load, err = store.ObjectsGetForReconcile(ctx, target.ID)
	require.NoError(t, err)
	assert.False(t, load.HasDependencies)
}

// It is a Get, so a collected id is ErrNotFound — the reconcile loop's
// "already gone" skip reads it exactly as it read ObjectsGet's.
func TestObjectsGetForReconcileReportsMissingObject(t *testing.T) {
	store := newRawStore(t)

	_, err := store.ObjectsGetForReconcile(context.Background(), 404)
	assert.ErrorIs(t, err, storeapi.ErrNotFound)
}

// The load carries the object's conditions, like ObjectsGet: a controller reads
// them from the object it is handed.
func TestObjectsGetForReconcileAttachesConditions(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	err := store.Conditions().Set(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	load, err := store.ObjectsGetForReconcile(ctx, obj.ID)
	require.NoError(t, err)
	require.Len(t, load.Object.Conditions, 1)
	assert.Equal(t, "Ready", load.Object.Conditions[0].Type)
}

// The write log's high-water mark is the maximum over live objects rows, not the
// version counter behind them. The two differ exactly where it matters: the event
// log draws from the same counter, so a mark taken from the counter would move for
// a write no ObjectWritesListSince consumer can ever be shown.
func TestObjectWritesMaxVersionIgnoresEventWrites(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	before := cursorNow(t, store)
	require.Equal(t, obj.ResourceVersion, before, "the mark is the newest object write")

	err := store.Events().Add(ctx, testGK, obj.ID, storeapi.EventsAddInput{
		Type: "Normal", Reason: "Probed",
	})
	require.NoError(t, err)

	assert.Equal(t, before, cursorNow(t, store), "an event write is not an object write")
	writes, _, err := store.ObjectWritesListSinceAll(ctx, before, 10)
	require.NoError(t, err)
	assert.Empty(t, writes, "and the listing agrees: nothing above the mark")
}

// An empty store has no objects to take a maximum over, which reads as 0 — the
// same value a consumer starting from scratch would use, so there is nothing to
// special-case.
func TestObjectWritesMaxVersionOnAnEmptyStore(t *testing.T) {
	assert.Zero(t, cursorNow(t, newRawStore(t)))
}

// The mark is not monotonic: removing the highest-versioned row lowers it. That is
// sound for both of its uses — nothing exists at the versions it steps back over,
// so a seeded watermark cannot skip a live write, and a poller that re-reads
// because the mark moved finds exactly the delete that moved it.
// Collection raises the mark rather than lowering it: the delete is an entry of
// its own, above the row it removed. Reading live rows made the mark step back
// here, which is what the waker's clamp was written for.
func TestObjectWritesMaxVersionAllRisesOnCollection(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	newRefObject(t, store)
	second := newRefObject(t, store)
	require.Equal(t, second.ResourceVersion, cursorNow(t, store))

	require.NoError(t, store.ObjectsDelete(ctx, second.ID))

	assert.Greater(t, cursorNow(t, store), second.ResourceVersion)
}

// ageOutWriteLog backdates every log entry and sweeps it, which is what an idle
// store past its retention window reaches on its own.
func ageOutWriteLog(t *testing.T, store *sqliteStore) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(), `UPDATE object_writes SET written_at = 0`)
	require.NoError(t, err)
	_, err = store.ObjectWritesSweep(context.Background(), 0, time.Hour)
	require.NoError(t, err)
}

// The mark falls when retention trims, so on its own it cannot tell a caught-up
// consumer from one whose history was deleted. The horizon beside it can: a
// resume below it lost entries.
func TestObjectWritesMaxVersionAllReportsTheHorizon(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)

	at, trimmed, err := store.ObjectWritesMaxVersionAll(ctx)
	require.NoError(t, err)
	require.Equal(t, obj.ResourceVersion, at)
	assert.Zero(t, trimmed, "nothing has been trimmed")

	ageOutWriteLog(t, store)

	at, trimmed, err = store.ObjectWritesMaxVersionAll(ctx)
	require.NoError(t, err)
	assert.Zero(t, at, "the log is empty, so its bare max is back to 0")
	assert.Equal(t, obj.ResourceVersion, trimmed, "the horizon is the only record of what it held")
}

// ---------------------------------------------------------------------------
// ReclaimSpace
// ---------------------------------------------------------------------------

// pageCounts reads the two pragmas the drain is judged by.
func pageCounts(t *testing.T, store *sqliteStore) (pages, free int) {
	t.Helper()
	require.NoError(t, store.db.QueryRow(`PRAGMA page_count`).Scan(&pages))
	require.NoError(t, store.db.QueryRow(`PRAGMA freelist_count`).Scan(&free))
	return pages, free
}

// churnStore grows the database with fat spec blobs and then deletes every row,
// leaving a freelist well past the drain floor — the state a long-lived store
// reaches through ordinary object churn plus event retention.
func churnStore(t *testing.T, store *sqliteStore) {
	t.Helper()
	ctx := context.Background()
	spec := make([]byte, 4096)
	for i := range spec {
		spec[i] = 'x'
	}
	blob := append(append([]byte(`{"v":"`), spec...), []byte(`"}`)...)
	for i := 0; i < 600; i++ {
		_, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
			Name: uniqueName(),
			Spec: blob,
		})
		require.NoError(t, err)
	}
	_, err := store.db.ExecContext(ctx, `DELETE FROM objects`)
	require.NoError(t, err)

	pages, free := pageCounts(t, store)
	require.Greater(t, free, freePagesFloor, "test setup should leave a freelist past the floor")
	require.Greater(t, free, pages/freePagesFloorDivisor, "test setup should leave a freelist past the fraction gate")
}

// A freelist past the gate is drained, and never by more than the cap. The
// assertion is deliberately "at most the cap", not "exactly": the pragma promises
// up to N pages. It still pins the bug that matters — PRAGMA incremental_vacuum
// frees one page per step, so an implementation that goes through Query and closes
// the rows without draining them releases exactly 1.
func TestReclaimSpaceDrainsPastTheFloor(t *testing.T) {
	store := newRawStore(t)
	churnStore(t, store)
	before, freeBefore := pageCounts(t, store)

	released, err := store.ReclaimSpace(context.Background(), 50)
	require.NoError(t, err)

	after, freeAfter := pageCounts(t, store)
	assert.Greater(t, released, 1, "one page released means the pragma was stepped once, not Exec'd")
	assert.LessOrEqual(t, released, 50)
	assert.Equal(t, freeBefore-freeAfter, released, "released should be the drop in the freelist")
	assert.Equal(t, before-after, released, "released pages should leave the file")
}

// Below either half of the gate the drain does nothing at all: free pages are what
// the next insert would have reused, so releasing them just to re-grow the file is
// work traded for nothing. A fresh store is the small-freelist case.
func TestReclaimSpaceSkipsASmallFreelist(t *testing.T) {
	store := newRawStore(t)
	before, _ := pageCounts(t, store)

	released, err := store.ReclaimSpace(context.Background(), 1000)
	require.NoError(t, err)

	after, _ := pageCounts(t, store)
	assert.Zero(t, released)
	assert.Equal(t, before, after)
}

// The gate is hysteresis, not a one-shot: repeated ticks walk a churned freelist
// down until it falls back under the floor, and then stop. Without the stop the
// sweeper would fight page reuse on every tick forever.
func TestReclaimSpaceStopsOnceUnderTheFloor(t *testing.T) {
	store := newRawStore(t)
	churnStore(t, store)
	ctx := context.Background()

	// Generous bound: each pass takes up to 200 pages off a freelist a few
	// thousand long, so a converging drain is done well inside this.
	var last int
	for i := 0; i < 100; i++ {
		n, err := store.ReclaimSpace(ctx, 200)
		require.NoError(t, err)
		if n == 0 {
			last = i
			break
		}
		require.Less(t, i, 99, "drain never fell back under the floor")
	}
	assert.Greater(t, last, 0, "the churned store should have taken at least one pass")

	_, free := pageCounts(t, store)
	pages, _ := pageCounts(t, store)
	assert.True(t, free <= freePagesFloor || free <= pages/freePagesFloorDivisor,
		"drain should stop with the freelist back under the gate, got %d free of %d pages", free, pages)
}

// A dead pool surfaces as an error rather than a silent zero — the sweeper logs it
// and retries on the next tick.
func TestReclaimSpaceErrorsOnAClosedStore(t *testing.T) {
	store := newRawStore(t)
	require.NoError(t, store.Close())

	_, err := store.ReclaimSpace(context.Background(), 100)
	assert.Error(t, err)
}

// scriptedDBTX answers the two counter pragmas from a scripted list and otherwise
// delegates to a real connection, so the drain can be driven into states SQLite will
// not produce on demand: a fault on a specific read, a failing vacuum, and a freelist
// that grew between the two measurements. The reads still go through a real driver —
// the script only chooses the value or the error — so a scan that stops compiling
// against *sql.Row is still a test failure.
type scriptedDBTX struct {
	inner      dbtx
	values     []int // one per counter read, in call order
	reads      int
	readErrAt  int   // 1-based read that errors instead of answering (0 = never)
	execErr    error // non-nil makes the vacuum fail
	execCalled bool
}

func (s *scriptedDBTX) QueryRowContext(ctx context.Context, _ string, _ ...any) *sql.Row {
	s.reads++
	if s.reads == s.readErrAt {
		// A real driver error, not a fabricated one: the column does not exist.
		return s.inner.QueryRowContext(ctx, `SELECT no_such_column`)
	}
	return s.inner.QueryRowContext(ctx, `SELECT ?`, s.values[s.reads-1])
}

func (s *scriptedDBTX) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	s.execCalled = true
	if s.execErr != nil {
		return nil, s.execErr
	}
	return s.inner.ExecContext(ctx, q, args...)
}

func (s *scriptedDBTX) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return s.inner.QueryContext(ctx, q, args...)
}

// A drain past the gate, driven by the script: 900 free pages of 1000, then 400 after
// the vacuum.
func scripted(t *testing.T, values []int) *scriptedDBTX {
	t.Helper()
	return &scriptedDBTX{inner: newRawStore(t).db, values: values}
}

// Nothing to do, and nothing read: the sweeper passes a positive cap, but a store
// built field by field in a test need not.
func TestReclaimSpaceIgnoresANonPositiveCap(t *testing.T) {
	store := newRawStore(t)
	churnStore(t, store)
	before, _ := pageCounts(t, store)

	released, err := store.ReclaimSpace(context.Background(), 0)
	require.NoError(t, err)

	after, _ := pageCounts(t, store)
	assert.Zero(t, released)
	assert.Equal(t, before, after, "a non-positive cap should not drain a freelist that is past the floor")
}

// Each of the three reads and the vacuum itself can fail mid-drain. All four are the
// same answer — give up and report — because the sweeper's next tick retries and
// nothing is incorrect in the meantime.
func TestReclaimSpaceReportsMidDrainFaults(t *testing.T) {
	ctx := context.Background()

	t.Run("page_count read", func(t *testing.T) {
		c := scripted(t, []int{1000, 900})
		c.readErrAt = 1
		_, err := freePagesRelease(ctx, c, 100)
		require.ErrorContains(t, err, "read page_count")
		assert.False(t, c.execCalled, "a failed measurement must not drain")
	})

	t.Run("freelist_count read", func(t *testing.T) {
		c := scripted(t, []int{1000, 900})
		c.readErrAt = 2
		_, err := freePagesRelease(ctx, c, 100)
		require.ErrorContains(t, err, "read freelist_count")
		assert.False(t, c.execCalled, "a failed measurement must not drain")
	})

	t.Run("the vacuum itself", func(t *testing.T) {
		c := scripted(t, []int{1000, 900})
		c.execErr = errors.New("disk is angry")
		_, err := freePagesRelease(ctx, c, 100)
		require.ErrorContains(t, err, "incremental_vacuum")
	})

	t.Run("the read back", func(t *testing.T) {
		c := scripted(t, []int{1000, 900, 1000})
		c.readErrAt = 3
		_, err := freePagesRelease(ctx, c, 100)
		require.ErrorContains(t, err, "read page_count")
		assert.True(t, c.execCalled, "the drain ran; only the measurement of it failed")
	})
}

// The count is a difference of two reads taken around the drain, so on a pool wider
// than one connection another writer can free pages between them and leave the
// freelist longer than it started. Report nothing rather than a negative count: the
// number is advisory and only ever logged.
func TestReclaimSpaceClampsAGrowingFreelist(t *testing.T) {
	c := scripted(t, []int{1000, 900, 1000, 950})

	released, err := freePagesRelease(context.Background(), c, 100)

	require.NoError(t, err)
	assert.Zero(t, released)
	assert.True(t, c.execCalled)
}

// uniqueName returns a name no other test row holds. Names are required and
// unique per kind, but the great majority of store tests only need a row to
// exist — they assert on versions, edges or sweeps, never on the name. Naming
// each one by hand would be noise, and reusing a literal collides on UNIQUE.
func uniqueName() string {
	return fmt.Sprintf("test-obj-%d", nameSeq.Add(1))
}

var nameSeq atomic.Int64

// The name-keyed spec write must carry everything the id-keyed one does. The
// no-op skip is the part most at risk: written as a bare UPDATE ... WHERE name =
// ? it would have nothing to compare against and would bump generation on every
// re-apply, which is what stops a controller re-applying its own spec from waking
// itself forever.
func TestObjectsUpdateSpecByNameSkipsANoOp(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	created, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name: "prod",
		Spec: []byte(`{"v":1}`),
	})
	require.NoError(t, err)

	same, _, err := store.ObjectsUpdateSpecByName(ctx, testGK, "prod", []byte(`{"v":1}`), 0)
	require.NoError(t, err)
	assert.Equal(t, created.Generation, same.Generation, "identical bytes bump no generation")
	assert.Equal(t, created.ResourceVersion, same.ResourceVersion, "and draw no resource_version")

	changed, _, err := store.ObjectsUpdateSpecByName(ctx, testGK, "prod", []byte(`{"v":2}`), 0)
	require.NoError(t, err)
	assert.Equal(t, created.Generation+1, changed.Generation)
	assert.Greater(t, changed.ResourceVersion, created.ResourceVersion)
}

// The kind rides in the WHERE, so a name this kind does not hold is absent rather
// than foreign — there is no ErrWrongKind to tell apart, as with the name-keyed
// delete.
func TestObjectsUpdateSpecByNameIsKindScoped(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	other := beehive.GroupKind{Kind: "Other"}
	_, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name: "shared",
		Spec: []byte(`{"v":1}`),
	})
	require.NoError(t, err)

	_, _, err = store.ObjectsUpdateSpecByName(ctx, other, "shared", []byte(`{"v":2}`), 0)
	require.ErrorIs(t, err, storeapi.ErrNotFound)

	_, _, err = store.ObjectsUpdateSpecByName(ctx, testGK, "absent", []byte(`{"v":2}`), 0)
	require.ErrorIs(t, err, storeapi.ErrNotFound)
}

// The idempotent delete paths must not take a write transaction. Absence is the
// steady state of a name-keyed delete — a controller that removes a child re-runs
// the call every reconcile, and exactly one of those calls ever deletes anything —
// so BEGIN IMMEDIATE on every one of them takes the store's single write lock to
// discover there is nothing to do.
//
// The observable has to be the transaction itself. resource_version_seq will not
// serve: markForDeletion already draws the version only after a row is stamped, so
// the sequence is unmoved on both no-op paths and such an assertion passes on the
// unfixed code. Counting transactions begun is exact, because Within's BeginTx is
// the only one in the package and a nested Within returns before reaching it.
func TestDeletionRequestsNoOpPathsTakeNoWriteTransaction(t *testing.T) {
	store := newTestStore(t)
	raw := store.(*sqliteStore)
	ctx := context.Background()
	created, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
		Name: "prod",
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	t.Run("absent name", func(t *testing.T) {
		before := raw.txCount.Load()
		res, err := store.DeletionRequests().CreateByName(ctx, testGK, "no-such-name")
		require.ErrorIs(t, err, storeapi.ErrNotFound)
		assert.False(t, res.Marked)
		assert.Equal(t, before, raw.txCount.Load(), "an absent name answered from a lock-free read")
	})

	t.Run("absent id", func(t *testing.T) {
		before := raw.txCount.Load()
		_, err := store.DeletionRequests().Create(ctx, testGK, 99999)
		require.ErrorIs(t, err, storeapi.ErrNotFound)
		assert.Equal(t, before, raw.txCount.Load())
	})

	t.Run("the delete that lands does take one", func(t *testing.T) {
		before := raw.txCount.Load()
		res, err := store.DeletionRequests().CreateByName(ctx, testGK, "prod")
		require.NoError(t, err)
		assert.True(t, res.Marked)
		assert.Equal(t, before+1, raw.txCount.Load())
	})

	t.Run("already pending", func(t *testing.T) {
		before := raw.txCount.Load()
		res, err := store.DeletionRequests().CreateByName(ctx, testGK, "prod")
		require.NoError(t, err)
		assert.False(t, res.Marked, "already deletion-pending is an idempotent no-op")
		assert.Equal(t, before, raw.txCount.Load())

		res, err = store.DeletionRequests().Create(ctx, testGK, created.ID)
		require.NoError(t, err)
		assert.False(t, res.Marked)
		assert.Equal(t, before, raw.txCount.Load())
	})
}

// The pre-probe is advisory, so the fall-through has to resolve a row that moved
// after it. A probe that reports "live" and a mark that stamps nothing is exactly
// that interleaving — a concurrent delete landed in between — and it must come out
// as the idempotent no-op, not as an error.
//
// Driven through requestDeletion directly with a scripted probe: the branch exists
// for a race, and scripting it is what makes the assertion deterministic rather
// than dependent on two goroutines meeting.
func TestRequestDeletionResolvesARowThatMovedAfterTheProbe(t *testing.T) {
	store := newTestStore(t).(*sqliteStore)
	ctx := context.Background()

	// live on the pre-probe, deletion-pending by the time the mark finds no row.
	var calls int
	probe := func(context.Context) (bool, error) {
		calls++
		return calls > 1, nil
	}

	res, err := store.requestDeletion(ctx, probe, `id = ?`, 99999)

	require.NoError(t, err, "the row was collected or marked by someone else; that is success")
	assert.False(t, res.Marked, "this call stamped nothing")
	assert.Equal(t, 2, calls, "the probe ran again inside the transaction to resolve the zero-row mark")
}

// And the same interleaving where the row was physically collected rather than
// marked still surfaces as absence.
func TestRequestDeletionReportsARowCollectedAfterTheProbe(t *testing.T) {
	store := newTestStore(t).(*sqliteStore)
	ctx := context.Background()

	var calls int
	probe := func(context.Context) (bool, error) {
		calls++
		if calls == 1 {
			return false, nil // live
		}
		return false, storeapi.ErrNotFound // gone
	}

	res, err := store.requestDeletion(ctx, probe, `id = ?`, 99999)

	require.ErrorIs(t, err, storeapi.ErrNotFound)
	assert.False(t, res.Marked)
}

// asNameTaken translates only the UNIQUE violation; every other failure has to pass
// through untouched, or a caller's retry-on-ErrNameTaken loop would spin on a
// permanent error. Driven with the table gone, so the INSERT fails with a different
// SQLite code.
func TestObjectsCreateLeavesANonUniqueErrorAlone(t *testing.T) {
	store := newRawStore(t)
	dropObjects(t, store)

	_, err := store.ObjectsCreate(context.Background(), testGK, beehive.ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`{}`),
	})

	require.Error(t, err)
	assert.NotErrorIs(t, err, storeapi.ErrNameTaken, "a missing table is not a taken name")
}

// probeDeletionByName's read failure is distinct from its no-rows branch: absent is
// idempotent success, but a broken read must surface, or a delete would report
// "already gone" for a store it could not question.
func TestDeletionRequestsCreateByNameSurfacesAProbeReadError(t *testing.T) {
	store := newRawStore(t)
	dropObjects(t, store)

	res, err := store.DeletionRequests().CreateByName(context.Background(), testGK, "whatever")

	require.Error(t, err)
	assert.NotErrorIs(t, err, storeapi.ErrNotFound, "a broken read is not an absent row")
	assert.False(t, res.Marked)
}

// writeLogEntry is one object_writes row, read straight from the table so the
// log's tests do not depend on the read API that lands later.
type writeLogEntry struct {
	ResourceVersion int64
	ObjectID        beehive.ObjectID
	Group           string
	Kind            string
	Op              int
	WrittenAt       int64
	Final           sql.NullString
}

func writeLogEntries(t *testing.T, store beehive.Store) []writeLogEntry {
	t.Helper()
	rows, err := store.(*sqliteStore).db.QueryContext(context.Background(),
		`SELECT resource_version, object_id, "group", kind, op, written_at, final
		   FROM object_writes ORDER BY resource_version`)
	require.NoError(t, err)
	defer rows.Close()

	var out []writeLogEntry
	for rows.Next() {
		var e writeLogEntry
		require.NoError(t, rows.Scan(&e.ResourceVersion, &e.ObjectID, &e.Group,
			&e.Kind, &e.Op, &e.WrittenAt, &e.Final))
		out = append(out, e)
	}
	require.NoError(t, rows.Err())
	return out
}

// A create appends one log entry at the row's own resource_version.
func TestObjectsCreateAppendsAWriteLogEntry(t *testing.T) {
	store := newTestStore(t)

	obj := newRefObject(t, store)

	entries := writeLogEntries(t, store)
	require.Len(t, entries, 1)
	assert.Equal(t, obj.ResourceVersion, entries[0].ResourceVersion)
	assert.Equal(t, obj.ID, entries[0].ObjectID)
	assert.Equal(t, testGK.Group, entries[0].Group)
	assert.Equal(t, testGK.Kind, entries[0].Kind)
	assert.Equal(t, writeOpCreate, entries[0].Op)
	assert.NotZero(t, entries[0].WrittenAt)
}

// Every write that bumps an objects row appends an update entry at that row's
// new version. The negative cases matter as much: a content no-op writes
// nothing at all, and an event draws a version without touching an objects row,
// so it must leave the object log alone — a watch gated on this log must not
// wake for a controller that records an event per reconcile.
func TestObjectWritesRecordEveryVersionBump(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		// setup replaces the default bare object when the case needs a
		// differently shaped one; it must create exactly one.
		setup func(t *testing.T, store beehive.Store) *beehive.RawObject
		write func(t *testing.T, store beehive.Store, obj *beehive.RawObject)
		logs  bool
	}{
		{
			name: "spec update",
			write: func(t *testing.T, store beehive.Store, obj *beehive.RawObject) {
				_, _, err := store.ObjectsUpdateSpec(ctx, testGK, obj.ID, []byte(`{"a":1}`), 0)
				require.NoError(t, err)
			},
			logs: true,
		},
		{
			name: "status update",
			write: func(t *testing.T, store beehive.Store, obj *beehive.RawObject) {
				require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, obj.ID, obj.Generation, []byte(`{"b":2}`), 0))
			},
			logs: true,
		},
		{
			name: "condition set",
			write: func(t *testing.T, store beehive.Store, obj *beehive.RawObject) {
				require.NoError(t, store.Conditions().Set(ctx, testGK, obj.ID, storeapi.Condition{
					Type: "Ready", Status: "True", Reason: "Settled",
				}))
			},
			logs: true,
		},
		{
			name: "finalizer cleared",
			setup: func(t *testing.T, store beehive.Store) *beehive.RawObject {
				obj, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
					Name: uniqueName(), Spec: []byte(`{}`), Finalizers: []string{"f"},
				})
				require.NoError(t, err)
				return obj
			},
			write: func(t *testing.T, store beehive.Store, obj *beehive.RawObject) {
				_, err := store.FinalizersDelete(ctx, testGK, obj.ID, "f")
				require.NoError(t, err)
			},
			logs: true,
		},
		{
			name: "deletion requested",
			write: func(t *testing.T, store beehive.Store, obj *beehive.RawObject) {
				_, err := store.DeletionRequests().Create(ctx, testGK, obj.ID)
				require.NoError(t, err)
			},
			logs: true,
		},
		{
			name: "byte-identical spec write",
			write: func(t *testing.T, store beehive.Store, obj *beehive.RawObject) {
				_, changed, err := store.ObjectsUpdateSpec(ctx, testGK, obj.ID, obj.Spec, obj.SpecVersion)
				require.NoError(t, err)
				require.False(t, changed, "precondition: the store must skip this write")
			},
		},
		{
			name: "event appended",
			write: func(t *testing.T, store beehive.Store, obj *beehive.RawObject) {
				err := store.Events().Add(ctx, testGK, obj.ID, storeapi.EventsAddInput{
					Category: "c", Type: "Normal", Reason: "R",
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newTestStore(t)
			newObject := tt.setup
			if newObject == nil {
				newObject = newRefObject
			}
			obj := newObject(t, store)
			require.Len(t, writeLogEntries(t, store), 1, "precondition: the create is logged")

			tt.write(t, store, obj)

			entries := writeLogEntries(t, store)
			if !tt.logs {
				assert.Len(t, entries, 1, "no objects row was written, so nothing is logged")
				return
			}
			require.Len(t, entries, 2)
			after, err := store.ObjectsGetMeta(ctx, obj.ID)
			require.NoError(t, err)
			assert.Equal(t, writeOpUpdate, entries[1].Op)
			assert.Equal(t, obj.ID, entries[1].ObjectID)
			assert.Equal(t, after.ResourceVersion, entries[1].ResourceVersion,
				"the entry carries the version the row now holds")
		})
	}
}

// Collection draws a resource_version. The row is gone, so nothing in objects
// carries it — the write log's delete entry does, and it needs a version to
// order against every other entry.
func TestObjectsDeleteDrawsAResourceVersion(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	before := seqValue(t, store.(*sqliteStore))

	require.NoError(t, store.ObjectsDelete(ctx, obj.ID))

	assert.Greater(t, seqValue(t, store.(*sqliteStore)), before)
}

// The delete entry carries the object as it was, conditions included. Nothing
// else can: the row is gone and the conditions cascaded with it, so a Deleted
// change has no other source for the body it promises.
func TestObjectsDeleteAppendsARowImage(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	require.NoError(t, store.ObjectsUpdateStatus(ctx, testGK, obj.ID, obj.Generation, []byte(`{"b":2}`), 0))
	require.NoError(t, store.Conditions().Set(ctx, testGK, obj.ID,
		storeapi.Condition{Type: "Ready", Status: "True"}))
	before, err := store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)

	require.NoError(t, store.ObjectsDelete(ctx, obj.ID))

	entries := writeLogEntries(t, store)
	last := entries[len(entries)-1]
	assert.Equal(t, writeOpDelete, last.Op)
	assert.Equal(t, obj.ID, last.ObjectID)
	require.True(t, last.Final.Valid, "a delete carries the row image")

	var image storeapi.RawObject
	require.NoError(t, json.Unmarshal([]byte(last.Final.String), &image))
	// ResourceVersion is the one field the image cannot match: the row held the
	// version of its last update, and the entry holds the delete's own.
	before.ResourceVersion = image.ResourceVersion
	assert.Equal(t, *before, image)
	assert.Len(t, image.Conditions, 1, "conditions cascade away with the row")
}

// The image must round-trip every RawObject field. A column added to objects and
// surfaced on RawObject but missed here would report a zero value on a Deleted
// change, and nothing in the write path would fail.
func TestWriteLogImageCoversRawObject(t *testing.T) {
	full := storeapi.RawObject{
		ID: 7, Group: "acme.com", Kind: "Widget", Name: "w",
		Spec: []byte(`{"a":1}`), Status: []byte(`{"b":2}`),
		SpecVersion: 3, StatusVersion: 4,
		Generation: 5, ObservedGeneration: ptr(int64(5)),
		ObservedAt: ptr(time.UnixMilli(1).UTC()), ResourceVersion: 9,
		DeletionRequestedAt: ptr(time.UnixMilli(2).UTC()), ReconcileOwed: 1,
		Finalizers: []string{"f"},
		Conditions: []storeapi.Condition{{Type: "Ready", Status: "True"}},
		Owner:      &beehive.ObjectRef{ID: 8, Group: "acme.com", Kind: "Gadget"},
		CreatedAt:  time.UnixMilli(3).UTC(), UpdatedAt: time.UnixMilli(4).UTC(),
	}
	v := reflect.ValueOf(full)
	for i := range v.NumField() {
		require.False(t, v.Field(i).IsZero(),
			"%s: give every field a value, or this test cannot detect losing it",
			v.Type().Field(i).Name)
	}

	encoded, err := json.Marshal(full)
	require.NoError(t, err)
	var back storeapi.RawObject
	require.NoError(t, json.Unmarshal(encoded, &back))

	assert.Equal(t, full, back)
}

// ptr is the address of a literal, for the pointer fields on RawObject.
func ptr[T any](v T) *T { return &v }

// The tail reads one kind. Another kind's writes must not move it, or every
// watch pays a listing for traffic it can never be shown.
func TestObjectWritesListSinceScopesToKind(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	otherGK := beehive.GroupKind{Group: "acme.com", Kind: "Widget"}
	mine := newRefObject(t, store)
	_, err := store.ObjectsCreate(ctx, otherGK, beehive.ObjectsCreateInput{
		Name: uniqueName(), Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	page, trimmed, err := store.ObjectWritesListSince(ctx, testGK, 0, 10)
	require.NoError(t, err)
	require.Len(t, page, 1, "only this kind's entries")
	assert.Equal(t, mine.ID, page[0].ID)
	assert.Equal(t, mine.ResourceVersion, page[0].ResourceVersion)
	assert.Equal(t, storeapi.WriteCreate, page[0].Op)
	assert.Zero(t, trimmed, "nothing has been trimmed")

	page, _, err = store.ObjectWritesListSince(ctx, testGK, mine.ResourceVersion, 10)
	require.NoError(t, err)
	assert.Empty(t, page, "afterRV is exclusive")
}

// The tick gate is per kind for the same reason the listing is.
func TestObjectWritesMaxVersionScopesToKind(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	otherGK := beehive.GroupKind{Group: "acme.com", Kind: "Widget"}
	mine := newRefObject(t, store)

	at, err := store.ObjectWritesMaxVersion(ctx, testGK)
	require.NoError(t, err)
	assert.Equal(t, mine.ResourceVersion, at)

	_, err = store.ObjectsCreate(ctx, otherGK, beehive.ObjectsCreateInput{
		Name: uniqueName(), Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	again, err := store.ObjectWritesMaxVersion(ctx, testGK)
	require.NoError(t, err)
	assert.Equal(t, at, again, "another kind's write does not move this kind's position")

	empty, err := store.ObjectWritesMaxVersion(ctx, beehive.GroupKind{Kind: "Nothing"})
	require.NoError(t, err)
	assert.Zero(t, empty)
}

// backdateWriteLogEntry ages one entry so retention can see it, instead of
// sleeping for it.
func backdateWriteLogEntry(t *testing.T, store beehive.Store, rv int64, age time.Duration) {
	t.Helper()
	_, err := store.(*sqliteStore).db.ExecContext(context.Background(),
		`UPDATE object_writes SET written_at = ? WHERE resource_version = ?`,
		toMillis(time.Now().UTC().Add(-age)), rv)
	require.NoError(t, err)
}

// The sweep records what it removed, per kind. A single global DELETE trims the
// rows and learns nothing about which kinds it touched, which leaves the horizon
// empty — and an empty horizon reads as "nothing was trimmed", so every later
// resume succeeds against a log with a hole in it.
func TestObjectWritesSweepRecordsThePerKindHorizon(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	otherGK := beehive.GroupKind{Group: "acme.com", Kind: "Widget"}

	staleA := newRefObject(t, store)
	oldA := newRefObject(t, store)
	oldB, err := store.ObjectsCreate(ctx, otherGK, beehive.ObjectsCreateInput{
		Name: uniqueName(), Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	keptA := newRefObject(t, store)
	keptB, err := store.ObjectsCreate(ctx, otherGK, beehive.ObjectsCreateInput{
		Name: uniqueName(), Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	backdateWriteLogEntry(t, store, staleA.ResourceVersion, time.Hour)
	backdateWriteLogEntry(t, store, oldA.ResourceVersion, time.Hour)
	backdateWriteLogEntry(t, store, oldB.ResourceVersion, time.Hour)

	deleted, err := store.ObjectWritesSweep(ctx, 0, 30*time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 3, deleted, "entries removed, not kinds touched")

	pageA, trimmedA, err := store.ObjectWritesListSince(ctx, testGK, 0, 10)
	require.NoError(t, err)
	require.Len(t, pageA, 1)
	assert.Equal(t, keptA.ResourceVersion, pageA[0].ResourceVersion)
	assert.Equal(t, oldA.ResourceVersion, trimmedA, "the horizon is the highest version removed for this kind")

	pageB, trimmedB, err := store.ObjectWritesListSince(ctx, otherGK, 0, 10)
	require.NoError(t, err)
	require.Len(t, pageB, 1)
	assert.Equal(t, keptB.ResourceVersion, pageB[0].ResourceVersion)
	assert.Equal(t, oldB.ResourceVersion, trimmedB, "each kind carries its own horizon")
}

// The count bound is a per-kind ring, so a hot kind cannot evict a quiet one.
func TestObjectWritesSweepCapsEachKind(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	otherGK := beehive.GroupKind{Group: "acme.com", Kind: "Widget"}
	for range 3 {
		newRefObject(t, store)
	}
	quiet, err := store.ObjectsCreate(ctx, otherGK, beehive.ObjectsCreateInput{
		Name: uniqueName(), Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	_, err = store.ObjectWritesSweep(ctx, 2, 0)
	require.NoError(t, err)

	pageA, _, err := store.ObjectWritesListSince(ctx, testGK, 0, 10)
	require.NoError(t, err)
	assert.Len(t, pageA, 2, "the busy kind is capped at its newest two")
	pageB, _, err := store.ObjectWritesListSince(ctx, otherGK, 0, 10)
	require.NoError(t, err)
	require.Len(t, pageB, 1, "the quiet kind keeps its only entry")
	assert.Equal(t, quiet.ResourceVersion, pageB[0].ResourceVersion)
}

// A kind whose log aged out entirely must keep its position. Reporting 0 against
// a tail parked at the last version would make the gate fire on every tick —
// forever, on the kind that writes least.
func TestObjectWritesMaxVersionHoldsTheHorizon(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	backdateWriteLogEntry(t, store, obj.ResourceVersion, time.Hour)

	deleted, err := store.ObjectWritesSweep(ctx, 0, 30*time.Minute)
	require.NoError(t, err)
	require.Equal(t, 1, deleted, "precondition: the log is now empty for this kind")

	at, err := store.ObjectWritesMaxVersion(ctx, testGK)
	require.NoError(t, err)
	assert.Equal(t, obj.ResourceVersion, at)
}

// The snapshot and its position are read together, so a stream that resumes at
// that position sees every write made after the listing and none made before it.
func TestObjectWritesSnapshotIsConsistent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	first := newRefObject(t, store)
	second := newRefObject(t, store)

	rows, at, err := store.ObjectWritesSnapshot(ctx, testGK)
	require.NoError(t, err)
	assert.Len(t, rows, 2)
	position, err := store.ObjectWritesMaxVersion(ctx, testGK)
	require.NoError(t, err)
	assert.Equal(t, position, at)
	assert.GreaterOrEqual(t, at, second.ResourceVersion)

	later := newRefObject(t, store)
	assert.Greater(t, later.ResourceVersion, at, "a write after the listing is above its position")

	page, _, err := store.ObjectWritesListSince(ctx, testGK, at, 10)
	require.NoError(t, err)
	require.Len(t, page, 1, "the stream picks up exactly what the snapshot missed")
	assert.Equal(t, later.ID, page[0].ID)
	assert.NotEqual(t, first.ID, page[0].ID)
}

// The one-object snapshot reads one row but reports the KIND's position: the
// stream that follows it tails the kind's log.
func TestObjectWritesSnapshotByIDReadsOneRow(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	mine := newRefObject(t, store)
	newRefObject(t, store)

	rows, at, err := store.ObjectWritesSnapshotByID(ctx, testGK, mine.ID)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, mine.ID, rows[0].ID)
	position, err := store.ObjectWritesMaxVersion(ctx, testGK)
	require.NoError(t, err)
	assert.Equal(t, position, at, "the kind's position, not this row's version")

	foreign, _, err := store.ObjectWritesSnapshotByID(ctx, beehive.GroupKind{Kind: "Other"}, mine.ID)
	require.NoError(t, err)
	assert.Empty(t, foreign, "another kind cannot see this row")
}

// The owner-scoped snapshot reads one owner's children and reports the KIND's
// position, for the same reason the by-id one does: the stream that follows it
// tails the kind's log.
func TestObjectWritesSnapshotByOwnerReadsOneOwnersChildren(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	owner := newRefObject(t, store)
	other := newRefObject(t, store)
	mine := newRefObject(t, store)
	theirs := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, mine.ID, owner.ID, beehive.RelationOwnedBy))
	require.NoError(t, addEdge(ctx, store, theirs.ID, other.ID, beehive.RelationOwnedBy))

	rows, at, err := store.ObjectWritesSnapshotByOwner(ctx, testGK, owner.ID)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, mine.ID, rows[0].ID)
	position, err := store.ObjectWritesMaxVersion(ctx, testGK)
	require.NoError(t, err)
	assert.Equal(t, position, at, "the kind's position, not this row's version")

	foreign, _, err := store.ObjectWritesSnapshotByOwner(ctx, beehive.GroupKind{Kind: "Other"}, owner.ID)
	require.NoError(t, err)
	assert.Empty(t, foreign, "another kind cannot see this row")
}

// An owner with no children, and an id that is no owner at all, both read empty
// rather than erroring: a watch may be opened before the children exist.
func TestObjectWritesSnapshotByOwnerFoldsAbsence(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	childless := newRefObject(t, store)

	rows, at, err := store.ObjectWritesSnapshotByOwner(ctx, testGK, childless.ID)
	require.NoError(t, err)
	assert.Empty(t, rows)
	assert.NotZero(t, at)

	rows, _, err = store.ObjectWritesSnapshotByOwner(ctx, testGK, 404)
	require.NoError(t, err)
	assert.Empty(t, rows)
}

// The tail reads the objects one batch named, in one query. A short result is
// normal: an id collected between the log read and this one is simply absent,
// and its delete arrives as a later entry.
func TestObjectsListByIDsIsKindScoped(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	otherGK := beehive.GroupKind{Group: "acme.com", Kind: "Widget"}
	mine := newRefObject(t, store)
	alsoMine := newRefObject(t, store)
	foreign, err := store.ObjectsCreate(ctx, otherGK, beehive.ObjectsCreateInput{
		Name: uniqueName(), Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	got, err := store.ObjectsListByIDs(ctx, testGK, []beehive.ObjectID{
		alsoMine.ID, mine.ID, foreign.ID, 9999,
	})
	require.NoError(t, err)
	require.Len(t, got, 2, "another kind's row and a missing id are absent, not errors")
	assert.Equal(t, mine.ID, got[0].ID)
	assert.Equal(t, alsoMine.ID, got[1].ID)

	empty, err := store.ObjectsListByIDs(ctx, testGK, nil)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

// The horizon comes back with the page it belongs to, on both paths: carried by
// the page's own statement when there are rows, and read on its own when there
// are none. Reading it separately from a non-empty page would let a sweep landing
// in between report a horizon above entries the page already captured, ending a
// stream that lost nothing.
func TestObjectWritesListSinceCarriesTheHorizon(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	old := newRefObject(t, store)
	kept := newRefObject(t, store)
	backdateWriteLogEntry(t, store, old.ResourceVersion, time.Hour)
	_, err := store.ObjectWritesSweep(ctx, 0, 30*time.Minute)
	require.NoError(t, err)

	page, trimmed, err := store.ObjectWritesListSince(ctx, testGK, 0, 10)
	require.NoError(t, err)
	require.Len(t, page, 1, "precondition: one entry survived")
	assert.Equal(t, kept.ResourceVersion, page[0].ResourceVersion)
	assert.Equal(t, old.ResourceVersion, trimmed, "carried by the page's statement")

	empty, trimmed, err := store.ObjectWritesListSince(ctx, testGK, kept.ResourceVersion, 10)
	require.NoError(t, err)
	require.Empty(t, empty)
	assert.Equal(t, old.ResourceVersion, trimmed, "and read on its own when the page is empty")
}

// A delete entry with a NULL image is a broken invariant, not a row to hand back
// half-built: the contract promises every WriteDelete carries its final state,
// and a caller that trusts that would drop the change and advance its cursor
// past it, losing the delete for good.
func TestObjectWritesListSinceRefusesAnImagelessDelete(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	require.NoError(t, store.ObjectsDelete(ctx, obj.ID))
	_, err := store.(*sqliteStore).db.ExecContext(ctx,
		`UPDATE object_writes SET final = NULL WHERE op = ?`, writeOpDelete)
	require.NoError(t, err)

	_, _, err = store.ObjectWritesListSince(ctx, testGK, 0, 10)

	require.Error(t, err, "a delete with no image must not be returned as success")
	assert.Contains(t, err.Error(), "row image")
}

// dropWriteLog removes the write log, so any write that must record itself
// fails. The log is not optional: a write nobody can see is worse than a write
// that failed.
func dropWriteLog(t *testing.T, store *sqliteStore) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(), `DROP TABLE object_writes`)
	require.NoError(t, err)
}

// Every mutator fails when it cannot record itself, rather than committing a
// write no watch will ever report.
func TestWritesFailWhenTheWriteLogIsGone(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name  string
		write func(t *testing.T, store *sqliteStore, obj *beehive.RawObject) error
	}{
		{
			name: "create",
			write: func(t *testing.T, store *sqliteStore, _ *beehive.RawObject) error {
				_, err := store.ObjectsCreate(ctx, testGK, beehive.ObjectsCreateInput{
					Name: uniqueName(), Spec: []byte(`{}`),
				})
				return err
			},
		},
		{
			name: "spec update",
			write: func(t *testing.T, store *sqliteStore, obj *beehive.RawObject) error {
				_, _, err := store.ObjectsUpdateSpec(ctx, testGK, obj.ID, []byte(`{"a":1}`), 0)
				return err
			},
		},
		{
			name: "deletion request",
			write: func(t *testing.T, store *sqliteStore, obj *beehive.RawObject) error {
				_, err := store.DeletionRequests().Create(ctx, testGK, obj.ID)
				return err
			},
		},
		{
			name: "collection",
			write: func(t *testing.T, store *sqliteStore, obj *beehive.RawObject) error {
				return store.ObjectsDelete(ctx, obj.ID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newRawStore(t)
			obj := newRefObject(t, store)
			dropWriteLog(t, store)

			require.Error(t, tt.write(t, store, obj))
		})
	}
}

// A page carrying a delete comes back with its row image attached, in the same
// transaction that read the page.
func TestObjectWritesListSinceAttachesImages(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	kept := newRefObject(t, store)
	gone := newRefObject(t, store)
	require.NoError(t, store.ObjectsDelete(ctx, gone.ID))

	page, _, err := store.ObjectWritesListSince(ctx, testGK, 0, 10)
	require.NoError(t, err)
	require.Len(t, page, 3, "two creates and a collection")
	assert.Nil(t, page[0].Final, "a create carries no image")
	assert.Equal(t, kept.ID, page[0].ID)
	last := page[len(page)-1]
	require.Equal(t, storeapi.WriteDelete, last.Op)
	require.NotNil(t, last.Final)
	assert.Equal(t, gone.Name, last.Final.Name)
}

// The image carries the owner, which the edge cannot: it cascades away with the
// row, so an owner-scoped watch has nowhere else to learn a collected child's
// owner from.
func TestObjectsDeleteImageCarriesTheOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	owner := newRefObject(t, store)
	child := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, child.ID, owner.ID, beehive.RelationOwnedBy))
	require.NoError(t, store.ObjectsDelete(ctx, child.ID))

	page, _, err := store.ObjectWritesListSince(ctx, testGK, 0, 10)
	require.NoError(t, err)
	last := page[len(page)-1]
	require.Equal(t, storeapi.WriteDelete, last.Op)
	require.NotNil(t, last.Final.Owner)
	assert.Equal(t, owner.ID, last.Final.Owner.ID)
}

// The owner read is part of assembling the image, so a delete that cannot make
// it fails rather than recording a collected child as ownerless — an absence a
// scoped watch would believe.
func TestObjectsDeleteFailsWhenTheOwnerReadFails(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	_, err := store.db.ExecContext(ctx, `DROP TABLE edges`)
	require.NoError(t, err)

	require.Error(t, store.ObjectsDelete(ctx, obj.ID))
}

// An unowned object's image says so, rather than leaving a caller to guess
// whether the owner was absent or unread.
func TestObjectsDeleteImageLeavesAnUnownedObjectsOwnerNil(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	require.NoError(t, store.ObjectsDelete(ctx, obj.ID))

	page, _, err := store.ObjectWritesListSince(ctx, testGK, 0, 10)
	require.NoError(t, err)
	last := page[len(page)-1]
	require.NotNil(t, last.Final)
	assert.Nil(t, last.Final.Owner)
}

// A non-positive limit reads nothing rather than reaching SQLite as an unbounded
// LIMIT -1.
func TestObjectWritesListSinceRejectsANonPositiveLimit(t *testing.T) {
	store := newTestStore(t)
	newRefObject(t, store)

	page, trimmed, err := store.ObjectWritesListSince(context.Background(), testGK, 0, 0)

	require.NoError(t, err)
	assert.Empty(t, page)
	assert.Zero(t, trimmed)
}

// The reads and the sweep surface a broken store rather than reporting an empty
// log, which a tail would read as "nothing changed".
func TestWriteLogReadsSurfaceADBError(t *testing.T) {
	ctx := context.Background()
	tests := map[string]func(store *sqliteStore) error{
		"list since": func(store *sqliteStore) error {
			_, _, err := store.ObjectWritesListSince(ctx, testGK, 0, 10)
			return err
		},
		"max version": func(store *sqliteStore) error {
			_, err := store.ObjectWritesMaxVersion(ctx, testGK)
			return err
		},
		"snapshot": func(store *sqliteStore) error {
			_, _, err := store.ObjectWritesSnapshot(ctx, testGK)
			return err
		},
		"snapshot by id": func(store *sqliteStore) error {
			_, _, err := store.ObjectWritesSnapshotByID(ctx, testGK, 1)
			return err
		},
		"sweep": func(store *sqliteStore) error {
			_, err := store.ObjectWritesSweep(ctx, 1, time.Hour)
			return err
		},
	}

	for name, read := range tests {
		t.Run(name, func(t *testing.T) {
			store := newRawStore(t)
			store.db.Close()

			require.Error(t, read(store))
		})
	}
}

// The page read surfaces a broken log rather than reporting an empty page, which
// a tail would take as "nothing changed" and advance past.
func TestObjectWritesListSinceSurfacesABrokenLog(t *testing.T) {
	store := newRawStore(t)
	newRefObject(t, store)
	dropWriteLog(t, store)

	_, _, err := store.ObjectWritesListSince(context.Background(), testGK, 0, 10)

	require.Error(t, err)
}

// A missing or foreign id reads as no rows, not as an error: the watch it backs
// streams nothing until the id exists.
func TestObjectWritesSnapshotByIDFoldsAbsence(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)

	missing, at, err := store.ObjectWritesSnapshotByID(ctx, testGK, 9999)
	require.NoError(t, err)
	assert.Empty(t, missing)
	assert.Equal(t, obj.ResourceVersion, at, "still the kind's position")

	foreign, _, err := store.ObjectWritesSnapshotByID(ctx,
		beehive.GroupKind{Kind: "Other"}, obj.ID)
	require.NoError(t, err)
	assert.Empty(t, foreign)
}

// A corrupt row image fails the read rather than surfacing a delete entry the
// caller cannot build a change from.
func TestObjectWritesListSinceRefusesAnUndecodableImage(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	require.NoError(t, store.ObjectsDelete(ctx, obj.ID))
	_, err := store.(*sqliteStore).db.ExecContext(ctx,
		`UPDATE object_writes SET final = 'not json' WHERE op = ?`, writeOpDelete)
	require.NoError(t, err)

	_, _, err = store.ObjectWritesListSince(ctx, testGK, 0, 10)

	require.Error(t, err)
	assert.NotContains(t, err.Error(), "row image", "this one failed to decode, not to exist")
}

// The image read surfaces a broken store rather than reporting deletes with no
// state to report.
func TestReadImagesSurfacesADBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	_, err := store.readImages(context.Background(), []any{int64(1)})

	require.Error(t, err)
}

// Collection needs a version for its log entry, so a store that cannot draw one
// fails the delete rather than removing the row unrecorded.
func TestObjectsDeleteFailsWithoutAVersionToDraw(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	_, err := store.db.ExecContext(ctx, `DROP TABLE resource_version_seq`)
	require.NoError(t, err)

	require.Error(t, store.ObjectsDelete(ctx, obj.ID))
}

// The sweep surfaces a broken log and a broken horizon table separately: the
// first cannot delete, the second cannot record what it deleted.
func TestObjectWritesSweepSurfacesBrokenTables(t *testing.T) {
	ctx := context.Background()

	t.Run("no log to trim", func(t *testing.T) {
		store := newRawStore(t)
		newRefObject(t, store)
		dropWriteLog(t, store)

		_, err := store.ObjectWritesSweep(ctx, 1, 0)
		require.Error(t, err)
	})

	// The age bound trims without enumerating kinds, so it reaches the delete on
	// its own path. Covered explicitly rather than left to whichever background
	// sweeper happens to meet a torn-down store first.
	t.Run("no log to age out", func(t *testing.T) {
		store := newRawStore(t)
		newRefObject(t, store)
		dropWriteLog(t, store)

		_, err := store.ObjectWritesSweep(ctx, 0, time.Hour)
		require.Error(t, err)
	})

	t.Run("no horizon to record", func(t *testing.T) {
		store := newRawStore(t)
		newRefObject(t, store)
		newRefObject(t, store)
		_, err := store.db.ExecContext(ctx, `DROP TABLE object_writes_horizon`)
		require.NoError(t, err)

		_, err = store.ObjectWritesSweep(ctx, 1, 0)
		require.Error(t, err, "trimming without recording the horizon would let a resume cross a hole")
	})
}

// The snapshot surfaces a failed listing rather than reporting an empty kind,
// which a subscriber would read as "nothing exists yet".
func TestObjectWritesSnapshotSurfacesAFailedListing(t *testing.T) {
	store := newRawStore(t)
	newRefObject(t, store)
	dropObjects(t, store)

	_, _, err := store.ObjectWritesSnapshot(context.Background(), testGK)

	require.Error(t, err)
}

// The count bound records its horizon per kind too, and trims each kind against
// its own cap. The trim runs one statement per kind, so a kind under its cap
// must come through untouched and with no horizon of its own — a shared cutoff
// would let a busy kind's trim strand a quiet kind's subscribers.
func TestObjectWritesSweepCapsEachKindWithItsOwnHorizon(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	otherGK := beehive.GroupKind{Group: "acme.com", Kind: "Widget"}

	busy := make([]*beehive.RawObject, 0, 3)
	for range 3 {
		busy = append(busy, newRefObject(t, store))
	}
	quiet, err := store.ObjectsCreate(ctx, otherGK, beehive.ObjectsCreateInput{
		Name: uniqueName(), Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	deleted, err := store.ObjectWritesSweep(ctx, 2, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, deleted, "only the busy kind's oldest entry")

	page, trimmed, err := store.ObjectWritesListSince(ctx, testGK, 0, 10)
	require.NoError(t, err)
	require.Len(t, page, 2)
	assert.Equal(t, busy[0].ResourceVersion, trimmed, "the busy kind carries its own horizon")

	page, trimmed, err = store.ObjectWritesListSince(ctx, otherGK, 0, 10)
	require.NoError(t, err)
	require.Len(t, page, 1)
	assert.Equal(t, quiet.ResourceVersion, page[0].ResourceVersion)
	assert.Zero(t, trimmed, "the quiet kind was never trimmed, so its horizon never moved")
}
