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
	"errors"
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
// EventsRecord tests to hang events off.
func newEventObject(t *testing.T, store beehive.Store) storeapi.ObjectID {
	t.Helper()
	obj, err := store.ObjectsCreate(context.Background(), &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	return obj.ID
}

// A first emission starts a run: count 1, a collapsed window, an assigned id and
// resource_version.
func TestRecordEventStartsRun(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	e, err := store.EventsRecord(ctx, testGK, id, storeapi.Event{
		Category: "connection", Type: "Warning", Reason: "ProbeFailed",
		Message: "i/o timeout", Detail: []byte(`{"attempt":1}`),
	})
	require.NoError(t, err)
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
func TestRecordEventExtendsRun(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	first, err := store.EventsRecord(ctx, testGK, id, storeapi.Event{
		Category: "connection", Type: "Warning", Reason: "ProbeFailed",
		Message: "timeout", Detail: []byte(`{"n":1}`),
	})
	require.NoError(t, err)
	second, err := store.EventsRecord(ctx, testGK, id, storeapi.Event{
		Category: "connection", Type: "Warning", Reason: "ProbeFailed",
		Message: "still down", Detail: []byte(`{"n":2}`),
	})
	require.NoError(t, err)

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
func TestRecordEventNewRunOnKeyChange(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	base := func(typ, reason string) storeapi.Event {
		return storeapi.Event{Category: "connection", Type: typ, Reason: reason}
	}

	a, err := store.EventsRecord(ctx, testGK, id, base("Warning", "ProbeFailed"))
	require.NoError(t, err)

	b, err := store.EventsRecord(ctx, testGK, id, base("Warning", "TLSHandshake"))
	require.NoError(t, err)
	assert.NotEqual(t, a.ID, b.ID, "reason change starts a new run")
	assert.Equal(t, 1, b.Count)

	c, err := store.EventsRecord(ctx, testGK, id, base("Normal", "TLSHandshake"))
	require.NoError(t, err)
	assert.NotEqual(t, b.ID, c.ID, "type change starts a new run")
	assert.Equal(t, 1, c.Count)
}

// Aggregation is scoped per (object, category): an emission in one category never
// breaks a run in another, even when the two are interleaved.
func TestRecordEventCategoriesIndependent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	conn := storeapi.Event{Category: "connection", Type: "Warning", Reason: "ProbeFailed"}
	sync := storeapi.Event{Category: "sync", Type: "Normal", Reason: "Synced"}

	conn1, err := store.EventsRecord(ctx, testGK, id, conn)
	require.NoError(t, err)
	_, err = store.EventsRecord(ctx, testGK, id, sync) // interleaved other-category event
	require.NoError(t, err)
	conn2, err := store.EventsRecord(ctx, testGK, id, conn)
	require.NoError(t, err)

	assert.Equal(t, conn1.ID, conn2.ID, "interleaved other-category event must not break this run")
	assert.Equal(t, 2, conn2.Count)
}

// EventsRecord is kind-scoped like the other id-keyed mutators: a foreign id is
// ErrWrongKind, a missing id is ErrNotFound, and neither writes a row.
func TestRecordEventScoped(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	ev := storeapi.Event{Category: "connection", Type: "Normal", Reason: "OK"}

	_, err := store.EventsRecord(ctx, beehive.GroupKind{Kind: "Other"}, id, ev)
	assert.ErrorIs(t, err, storeapi.ErrWrongKind)

	_, err = store.EventsRecord(ctx, testGK, 999999, ev)
	assert.ErrorIs(t, err, storeapi.ErrNotFound)
}

// EventsList returns an object's runs newest-first (by last_at, then id as the
// same-millisecond tiebreak).
func TestListEventsOrdersNewestFirst(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	rec := func(typ, reason string) {
		_, err := store.EventsRecord(ctx, testGK, id, storeapi.Event{Category: "connection", Type: typ, Reason: reason})
		require.NoError(t, err)
	}
	rec("Normal", "Connected")     // A
	rec("Warning", "TLSHandshake") // B
	rec("Normal", "Connected")     // C (new run: key changed from B)
	rec("Warning", "ProbeFailed")  // D

	got, err := store.EventsList(ctx, id, storeapi.EventQuery{})
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
		_, err := store.EventsRecord(ctx, testGK, id, storeapi.Event{Category: cat, Type: typ, Reason: reason})
		require.NoError(t, err)
	}
	rec("connection", "Warning", "ProbeFailed")
	rec("connection", "Normal", "Connected")
	rec("sync", "Normal", "Synced")

	t.Run("category restricts to one timeline", func(t *testing.T) {
		cat := "connection"
		got, err := store.EventsList(ctx, id, storeapi.EventQuery{Category: &cat})
		require.NoError(t, err)
		require.Len(t, got, 2)
		for _, e := range got {
			assert.Equal(t, "connection", e.Category)
		}
	})
	t.Run("nil category returns all timelines", func(t *testing.T) {
		got, err := store.EventsList(ctx, id, storeapi.EventQuery{})
		require.NoError(t, err)
		assert.Len(t, got, 3)
	})
	t.Run("type", func(t *testing.T) {
		got, err := store.EventsList(ctx, id, storeapi.EventQuery{Type: "Warning"})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "ProbeFailed", got[0].Reason)
	})
	t.Run("reason", func(t *testing.T) {
		got, err := store.EventsList(ctx, id, storeapi.EventQuery{Reason: "Synced"})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "sync", got[0].Category)
	})
	t.Run("limit takes the newest N", func(t *testing.T) {
		got, err := store.EventsList(ctx, id, storeapi.EventQuery{Limit: 2})
		require.NoError(t, err)
		assert.Len(t, got, 2)
	})
	t.Run("since bounds by last_at", func(t *testing.T) {
		got, err := store.EventsList(ctx, id, storeapi.EventQuery{Since: time.Now().UTC().Add(time.Hour)})
		require.NoError(t, err)
		assert.Empty(t, got, "a future lower bound excludes every run")

		got, err = store.EventsList(ctx, id, storeapi.EventQuery{Since: time.Now().UTC().Add(-time.Hour)})
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

	got, err := store.EventsGetLatest(ctx, id, "connection")
	require.NoError(t, err)
	assert.Nil(t, got, "empty timeline is nil, not an error")

	rec := func(cat, typ, reason string) {
		_, err := store.EventsRecord(ctx, testGK, id, storeapi.Event{Category: cat, Type: typ, Reason: reason})
		require.NoError(t, err)
	}
	rec("connection", "Warning", "ProbeFailed")
	rec("connection", "Normal", "Connected")
	rec("sync", "Normal", "Synced")

	got, err = store.EventsGetLatest(ctx, id, "connection")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "Connected", got.Reason, "the current (newest) run for the category")

	got, err = store.EventsGetLatest(ctx, id, "sync")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "Synced", got.Reason, "scoped to the category")

	got, err = store.EventsGetLatest(ctx, id, "nope")
	require.NoError(t, err)
	assert.Nil(t, got, "unknown category is nil")
}

// EventsSweep caps each timeline to the newest perObject runs.
func TestSweepEventsCapN(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	for _, r := range []string{"R1", "R2", "R3", "R4"} { // 4 distinct runs
		_, err := store.EventsRecord(ctx, testGK, id, storeapi.Event{Category: "c", Type: "Normal", Reason: r})
		require.NoError(t, err)
	}

	deleted, err := store.EventsSweep(ctx, 2, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, deleted)

	got, err := store.EventsList(ctx, id, storeapi.EventQuery{})
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
		_, err := store.EventsRecord(ctx, testGK, id, storeapi.Event{Category: cat, Type: "Normal", Reason: reason})
		require.NoError(t, err)
	}
	rec(a, "connection", "F1") // object a, connection flaps 3 runs
	rec(a, "connection", "F2")
	rec(a, "connection", "F3")
	rec(a, "sync", "S1")            // object a, sync has 1 run
	rec(b, "connection", "OtherC1") // object b, its own timeline

	_, err := store.EventsSweep(ctx, 1, 0)
	require.NoError(t, err)

	conn := "connection"
	sync := "sync"
	aConn, err := store.EventsList(ctx, a, storeapi.EventQuery{Category: &conn})
	require.NoError(t, err)
	require.Len(t, aConn, 1)
	assert.Equal(t, "F3", aConn[0].Reason, "flapping timeline keeps its newest")

	aSync, err := store.EventsList(ctx, a, storeapi.EventQuery{Category: &sync})
	require.NoError(t, err)
	require.Len(t, aSync, 1, "quiet timeline survives the flap on the same object")
	assert.Equal(t, "S1", aSync[0].Reason)

	bConn, err := store.EventsList(ctx, b, storeapi.EventQuery{Category: &conn})
	require.NoError(t, err)
	require.Len(t, bConn, 1, "another object's timeline is independent")
	assert.Equal(t, "OtherC1", bConn[0].Reason)
}

// EventsSweep drops runs whose window ended more than maxAge ago.
func TestSweepEventsMaxAge(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	old, err := store.EventsRecord(ctx, testGK, id, storeapi.Event{Category: "c", Type: "Normal", Reason: "Old"})
	require.NoError(t, err)
	_, err = store.EventsRecord(ctx, testGK, id, storeapi.Event{Category: "c", Type: "Warning", Reason: "New"})
	require.NoError(t, err)

	// Age the first run's window into the past directly — no clock injection needed.
	s := store.(*sqliteStore)
	_, err = s.db.ExecContext(ctx, `UPDATE events SET last_at = ? WHERE id = ?`,
		toMillis(time.Now().UTC().Add(-2*time.Hour)), old.ID)
	require.NoError(t, err)

	deleted, err := store.EventsSweep(ctx, 0, time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 1, deleted)

	got, err := store.EventsList(ctx, id, storeapi.EventQuery{})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "New", got[0].Reason, "the run within maxAge is kept")
}

// Deleting an object cascade-deletes its event log (FK ON DELETE CASCADE).
func TestDeleteObjectCascadesEvents(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	_, err := store.EventsRecord(ctx, testGK, id, storeapi.Event{Category: "c", Type: "Normal", Reason: "X"})
	require.NoError(t, err)
	require.NoError(t, store.ObjectsDelete(ctx, id))

	got, err := store.EventsList(ctx, id, storeapi.EventQuery{})
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

// EventsRecord surfaces store faults from each of its steps.
func TestRecordEventStoreErrors(t *testing.T) {
	ctx := context.Background()
	ev := storeapi.Event{Category: "c", Type: "Normal", Reason: "R"}

	t.Run("resource_version_seq missing", func(t *testing.T) {
		store := newRawStore(t)
		id := newEventObject(t, store)
		dropSeq(t, store)
		_, err := store.EventsRecord(ctx, testGK, id, ev)
		require.Error(t, err)
	})

	t.Run("latest-run probe fails", func(t *testing.T) {
		store := newRawStore(t)
		id := newEventObject(t, store)
		dropEventsTable(t, store)
		_, err := store.EventsRecord(ctx, testGK, id, ev)
		require.Error(t, err)
	})

	t.Run("written row fails to scan", func(t *testing.T) {
		store := newRawStore(t)
		id := newEventObject(t, store)
		_, err := store.EventsRecord(ctx, testGK, id, ev)
		require.NoError(t, err)
		breakEventRowRead(t, store)
		// Same key → the key-only probe still reads the run (it ignores first_at),
		// so EXTEND updates it and RETURNINGs the full row whose now-NULL first_at
		// → scan fails.
		_, err = store.EventsRecord(ctx, testGK, id, ev)
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
		_, err := store.EventsList(ctx, id, storeapi.EventQuery{})
		require.Error(t, err)
	})

	t.Run("row fails to scan", func(t *testing.T) {
		store := newRawStore(t)
		id := newEventObject(t, store)
		_, err := store.EventsRecord(ctx, testGK, id, storeapi.Event{Category: "c", Type: "Normal", Reason: "R"})
		require.NoError(t, err)
		breakEventRowRead(t, store)
		_, err = store.EventsList(ctx, id, storeapi.EventQuery{})
		require.Error(t, err)
	})
}

// EventsGetLatest surfaces a scan fault on the current run.
func TestGetLatestEventScanError(t *testing.T) {
	ctx := context.Background()
	store := newRawStore(t)
	id := newEventObject(t, store)
	_, err := store.EventsRecord(ctx, testGK, id, storeapi.Event{Category: "c", Type: "Normal", Reason: "R"})
	require.NoError(t, err)
	breakEventRowRead(t, store)
	_, err = store.EventsGetLatest(ctx, id, "c")
	require.Error(t, err)
}

// EventsSweep surfaces a delete fault from either retention bound.
func TestSweepEventsExecErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("cap-N delete fails", func(t *testing.T) {
		store := newRawStore(t)
		newEventObject(t, store)
		dropEventsTable(t, store)
		_, err := store.EventsSweep(ctx, 1, 0)
		require.Error(t, err)
	})

	t.Run("max-age delete fails", func(t *testing.T) {
		store := newRawStore(t)
		newEventObject(t, store)
		dropEventsTable(t, store)
		_, err := store.EventsSweep(ctx, 0, time.Hour)
		require.Error(t, err)
	})
}

func TestObjectsCreateAssignsIdentity(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	obj, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group,
		Kind:  testGK.Kind,
		Slug:  new("world"),
		Spec:  []byte(`{"name":"world"}`),
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
	require.NotNil(t, obj.Slug)
	assert.Equal(t, "world", *obj.Slug)
}

func TestObjectsCreatePersistsFinalizers(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	want := []string{"kstack.sh/cluster", "kstack.sh/dns"}
	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Slug: new("guarded"),
		Spec: []byte(`{}`), Finalizers: want,
	})
	require.NoError(t, err)
	assert.Equal(t, want, created.Finalizers)

	// Round-trips through the JSON column, not just the returned struct.
	reloaded, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, want, reloaded.Finalizers)
}

func TestGetByIdAndSlug(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Slug: new("world"),
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	byID, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, created.ID, byID.ID)

	byName, err := store.ObjectsGetBySlug(ctx, testGK, "world")
	require.NoError(t, err)
	assert.Equal(t, created.ID, byName.ID)
}

func TestGetNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.ObjectsGet(ctx, 999)
	assert.ErrorIs(t, err, beehive.ErrNotFound)

	_, err = store.ObjectsGetBySlug(ctx, testGK, "nope")
	assert.ErrorIs(t, err, beehive.ErrNotFound)
}

func TestDuplicateSlugRejected(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	mk := func() error {
		_, err := store.ObjectsCreate(ctx, &beehive.RawObject{
			Group: testGK.Group, Kind: testGK.Kind, Slug: new("dup"),
			Spec: []byte(`{}`),
		})
		return err
	}
	require.NoError(t, mk())
	assert.Error(t, mk(), "second create with same slug should violate UNIQUE")
}

func TestUnnamedObjectsCoexist(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	mk := func() *beehive.RawObject {
		obj, err := store.ObjectsCreate(ctx, &beehive.RawObject{
			Group: testGK.Group, Kind: testGK.Kind, // Slug nil
			Spec: []byte(`{}`),
		})
		require.NoError(t, err)
		assert.Nil(t, obj.Slug)
		return obj
	}
	// SQLite treats NULL != NULL, so multiple unnamed objects are allowed.
	a, b := mk(), mk()
	assert.NotEqual(t, a.ID, b.ID)
}

func TestObjectsUpdateSpecBumpsGeneration(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{"v":1}`),
	})
	require.NoError(t, err)

	updated, changed, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"v":2}`), 0)
	require.NoError(t, err)

	assert.True(t, changed, "a real spec change reports changed")
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

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{"v":1}`),
	})
	require.NoError(t, err)

	// Settle the object so observed_generation == generation; an idempotent
	// update must leave it settled.
	settled, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{}`), 0)
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	again, changed, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"v":1}`), 0)
	require.NoError(t, err)

	assert.False(t, changed, "the no-op must report changed=false so callers can skip their follow-up")
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

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	updated, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 0)
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
	plain, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	assert.Zero(t, plain.SpecVersion, "spec version defaults to 0")
	assert.Zero(t, plain.StatusVersion, "status version defaults to 0")

	// ObjectsCreate persists the caller-set spec version; status stays 0 (nil at create).
	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Slug: new("v"),
		Spec: []byte(`{}`), SpecVersion: 3,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 3, created.SpecVersion)
	assert.Zero(t, created.StatusVersion)

	reread, err := store.ObjectsGet(ctx, created.ID)
	require.NoError(t, err)
	assert.EqualValues(t, 3, reread.SpecVersion, "spec version survives a re-read")
	assert.Zero(t, reread.StatusVersion)

	// UpdateStatus stamps only the status version, leaving spec untouched.
	withStatus, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{}`), 7)
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

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	// created.Generation is 1; reporting generation 5 is impossible to have seen.
	_, err = store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation+4, []byte(`{"msg":"hi"}`), 0)
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

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	bumped, _, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"x":1}`), 0)
	require.NoError(t, err)
	require.EqualValues(t, 2, bumped.Generation)

	// Controller reports it reconciled the now-stale generation 1.
	updated, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{}`), 0)
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

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	first, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 0)
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	again, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 0)
	require.NoError(t, err)

	assert.Equal(t, first.ResourceVersion, again.ResourceVersion, "identical status must not bump resource_version")
	assert.Equal(t, first.UpdatedAt, again.UpdatedAt, "identical status must not touch updated_at")
	assert.JSONEq(t, `{"msg":"hi"}`, string(again.Status))
	probe.expectNone()
}

func TestUpdateStatusChangedStatusWrites(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	first, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 0)
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	again, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"bye"}`), 0)
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

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	_, err = store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 0)
	require.NoError(t, err)

	_, err = store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation+4, []byte(`{"msg":"hi"}`), 0)
	require.ErrorIs(t, err, beehive.ErrObservedGenerationFuture)
}

// Scoping is unchanged on both branches: a foreign id is ErrWrongKind and a
// missing id ErrNotFound whether or not the status bytes would change.
func TestUpdateStatusScopedOnBothBranches(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	_, err = store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 0)
	require.NoError(t, err)

	for _, status := range [][]byte{[]byte(`{"msg":"hi"}`), []byte(`{"msg":"bye"}`)} {
		_, err = store.ObjectsUpdateStatus(ctx, beehive.GroupKind{Kind: "Other"}, created.ID, created.Generation, status, 0)
		assert.ErrorIs(t, err, beehive.ErrWrongKind)

		_, err = store.ObjectsUpdateStatus(ctx, testGK, 999999, 1, status, 0)
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

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	_, err = store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 0)
	require.NoError(t, err)

	// New spec, same status: the reconcile observed generation 2 but wrote no
	// new content.
	bumped, _, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"x":1}`), 0)
	require.NoError(t, err)
	require.EqualValues(t, 2, bumped.Generation)

	probe := newWriteProbe(t, store)

	again, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, bumped.Generation, []byte(`{"msg":"hi"}`), 0)
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
	third, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, bumped.Generation, []byte(`{"msg":"hi"}`), 0)
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

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	bumped, _, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"x":1}`), 0)
	require.NoError(t, err)
	require.EqualValues(t, 2, bumped.Generation)

	// The newer reconcile lands first and settles the object at generation 2.
	settled, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, bumped.Generation, []byte(`{"msg":"hi"}`), 0)
	require.NoError(t, err)
	require.NotNil(t, settled.ObservedGeneration)
	require.EqualValues(t, bumped.Generation, *settled.ObservedGeneration)

	probe := newWriteProbe(t, store)

	// The straggler, still holding generation 1, reports identical status.
	late, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 0)
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

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	bumped, _, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"x":1}`), 0)
	require.NoError(t, err)
	require.EqualValues(t, 2, bumped.Generation)

	settled, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, bumped.Generation, []byte(`{"msg":"hi"}`), 0)
	require.NoError(t, err)
	require.NotNil(t, settled.ObservedGeneration)
	require.EqualValues(t, bumped.Generation, *settled.ObservedGeneration)

	// The straggler, still holding generation 1, writes different status.
	late, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"stale"}`), 0)
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

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{"v":1}`), SpecVersion: 1,
	})
	require.NoError(t, err)
	settled, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 1)
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	// Same status bytes, newer status schema version: a real write, announced.
	statusStamped, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 2)
	require.NoError(t, err)
	assert.Equal(t, 2, statusStamped.StatusVersion, "the newer status version lands")
	assert.Equal(t, 1, statusStamped.SpecVersion, "and leaves the spec version alone")
	assert.Greater(t, statusStamped.ResourceVersion, settled.ResourceVersion,
		"a shape change is watch-visible even with identical bytes")
	probe.expectWrite()

	// Same spec bytes, newer spec schema version: likewise, and changed=true so the
	// client wakes the controller to re-derive from the reinterpreted spec.
	specStamped, changed, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"v":1}`), 3)
	require.NoError(t, err)
	assert.True(t, changed, "a shape change is a spec change — the caller's bytes mean something new")
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

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{"v":1}`), SpecVersion: 1,
	})
	require.NoError(t, err)
	settled, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 1)
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	again, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 1)
	require.NoError(t, err)
	assert.Equal(t, settled.ResourceVersion, again.ResourceVersion, "no resource_version bump")

	sameSpec, changed, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"v":1}`), 1)
	require.NoError(t, err)
	assert.False(t, changed)
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

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{"v":3}`), SpecVersion: 3,
	})
	require.NoError(t, err)
	settled, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 3)
	require.NoError(t, err)
	require.Equal(t, 3, settled.StatusVersion)

	probe := newWriteProbe(t, store)

	// A build that lost the kind's migrator (reporting 0) has no version opinion:
	// the write goes through and leaves the tag alone.
	stale, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 0)
	require.NoError(t, err)
	assert.Equal(t, 3, stale.StatusVersion, "a no-op status write never stamps backwards")

	staleSpec, changed, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"v":3}`), 0)
	require.NoError(t, err)
	assert.False(t, changed)
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

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`), SpecVersion: 3,
	})
	require.NoError(t, err)
	_, err = store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 3)
	require.NoError(t, err)

	bumped, _, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"x":1}`), 3)
	require.NoError(t, err)

	// Converging at the new generation with identical status and no version opinion.
	got, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, bumped.Generation, []byte(`{"msg":"hi"}`), 0)
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

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{"v":3}`), SpecVersion: 3,
	})
	require.NoError(t, err)
	_, err = store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 3)
	require.NoError(t, err)

	// Content no-op and real content change, spec and status alike.
	_, _, err = store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"v":3}`), 1)
	require.ErrorIs(t, err, beehive.ErrSchemaVersionDowngrade)
	_, _, err = store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"v":9}`), 1)
	require.ErrorIs(t, err, beehive.ErrSchemaVersionDowngrade)
	_, err = store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 1)
	require.ErrorIs(t, err, beehive.ErrSchemaVersionDowngrade)
	_, err = store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"bye"}`), 1)
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

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{"v":3}`), SpecVersion: 3,
	})
	require.NoError(t, err)
	_, err = store.ObjectsUpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`), 3)
	require.NoError(t, err)

	updated, changed, err := store.ObjectsUpdateSpec(ctx, testGK, created.ID, []byte(`{"v":4}`), 0)
	require.NoError(t, err)
	require.True(t, changed)
	assert.Equal(t, 3, updated.SpecVersion, "a content write with no migrator keeps the stored version")

	settled, err := store.ObjectsUpdateStatus(ctx, testGK, created.ID, updated.Generation, []byte(`{"msg":"bye"}`), 0)
	require.NoError(t, err)
	assert.Equal(t, 3, settled.StatusVersion, "same on the status half")
}

func TestListObjects(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	for _, n := range []string{"a", "b", "c"} {
		_, err := store.ObjectsCreate(ctx, &beehive.RawObject{
			Group: testGK.Group, Kind: testGK.Kind, Slug: new(n),
			Spec: []byte(`{}`),
		})
		require.NoError(t, err)
	}
	// A different kind must not leak into the list.
	_, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: "", Kind: "Other", Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	list, err := store.ObjectsList(ctx, testGK)
	require.NoError(t, err)
	require.Len(t, list, 3)

	var names []string
	for _, o := range list {
		require.NotNil(t, o.Slug)
		names = append(names, *o.Slug)
	}
	assert.Equal(t, []string{"a", "b", "c"}, names, "ordered by id")
}

func TestResourceVersionIsMonotonic(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	a, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Slug: new("a"), Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	b, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Slug: new("b"), Spec: []byte(`{}`),
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

	a, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Slug: new("a"), Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	b, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Slug: new("b"), Spec: []byte(`{}`),
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

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	first, changed, err := store.DeletionRequestsCreate(ctx, testGK, created.ID)
	require.NoError(t, err)
	assert.True(t, changed, "first call is a real change")
	assert.Greater(t, first.ResourceVersion, created.ResourceVersion,
		"the first request is a real change and bumps the cursor")

	// A repeat request changes no deletion state, so it must be a no-op: same
	// resource_version, same updated_at, no spurious watch/CAS churn.
	second, changed, err := store.DeletionRequestsCreate(ctx, testGK, created.ID)
	require.NoError(t, err)
	assert.False(t, changed, "repeat call is an idempotent no-op")
	assert.Equal(t, first.ResourceVersion, second.ResourceVersion,
		"an idempotent repeat must not bump resource_version")
	assert.Equal(t, first.UpdatedAt, second.UpdatedAt)
}

// ObjectsGetMeta returns the same row as ObjectsGet but skips assembling
// conditions, which the metadata-only GC and edge callers never read.
func TestGetObjectMetaSkipsConditions(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	_, err = store.ConditionsSet(ctx, testGK, created.ID,
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

	mk := func() storeapi.ObjectID {
		o, err := store.ObjectsCreate(ctx, &beehive.RawObject{
			Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
		})
		require.NoError(t, err)
		return o.ID
	}
	owner, childA, childB := mk(), mk(), mk()
	require.NoError(t, addEdge(ctx, store, childA, owner, beehive.RelationOwnedBy))
	require.NoError(t, addEdge(ctx, store, childB, owner, beehive.RelationOwnedBy))

	// Probe from here, so each cascade's writes are isolated from the setup's.
	probe := newWriteProbe(t, store)

	// First cascade marks both children (one write each) and returns both.
	got, err := store.DeletionRequestsCreateFromOwner(ctx, owner)
	require.NoError(t, err)
	require.Len(t, got, 2)
	probe.expectWrites(2)
	a1, err := store.ObjectsGetMeta(ctx, childA)
	require.NoError(t, err)
	require.NotNil(t, a1.DeletionRequestedAt, "child A marked for deletion")
	b1, err := store.ObjectsGetMeta(ctx, childB)
	require.NoError(t, err)
	require.NotNil(t, b1.DeletionRequestedAt, "child B marked for deletion")

	// Second cascade over the now-deleting children: still returns both, but writes
	// nothing — no resource_version churn, and nothing new in the write log.
	got2, err := store.DeletionRequestsCreateFromOwner(ctx, owner)
	require.NoError(t, err)
	require.Len(t, got2, 2)
	probe.expectNone()
	a2, err := store.ObjectsGetMeta(ctx, childA)
	require.NoError(t, err)
	assert.Equal(t, a1.ResourceVersion, a2.ResourceVersion, "no re-mark, no rv churn")
	b2, err := store.ObjectsGetMeta(ctx, childB)
	require.NoError(t, err)
	assert.Equal(t, b1.ResourceVersion, b2.ResourceVersion)
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

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
		Finalizers: []string{"a", "b"},
	})
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	// Removing a present finalizer is a real change: only that finalizer drops,
	// resource_version bumps, and watchers see a Modified event.
	got, err := store.FinalizersDelete(ctx, testGK, created.ID, "a")
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

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
		Finalizers: []string{"a"},
	})
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	// Removing a finalizer that isn't present changes nothing: the list is intact,
	// resource_version is unbumped, and no event fires (a watcher would otherwise
	// see a spurious diff).
	got, err := store.FinalizersDelete(ctx, testGK, created.ID, "missing")
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

	owned, err := store.EdgesListOutgoingByRelation(ctx, from.ID, beehive.RelationOwnedBy)
	require.NoError(t, err)
	assert.Equal(t, []beehive.ObjectID{owner.ID}, refIDs(owned), "only the owned_by target")

	deps, err := store.EdgesListOutgoingByRelation(ctx, from.ID, beehive.RelationDependsOn)
	require.NoError(t, err)
	assert.Equal(t, []beehive.ObjectID{dep.ID}, refIDs(deps), "only the depends_on target")

	none, err := store.EdgesListOutgoingByRelation(ctx, owner.ID, beehive.RelationOwnedBy)
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

	got, err := store.EdgesGroupOutgoingByID(ctx,
		[]beehive.ObjectID{childA.ID, childB.ID, loner.ID}, beehive.RelationOwnedBy)
	require.NoError(t, err)
	assert.Equal(t, []beehive.ObjectID{owner.ID}, refIDs(got[childA.ID]))
	assert.Equal(t, []beehive.ObjectID{owner.ID}, refIDs(got[childB.ID]))
	_, ok := got[loner.ID]
	assert.False(t, ok, "a source with no matching edge is absent from the map")

	empty, err := store.EdgesGroupOutgoingByID(ctx, nil, beehive.RelationOwnedBy)
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

	got, err := store.EdgesGroupOutgoingByID(ctx, children, beehive.RelationOwnedBy)
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
		_, _, err := store.DeletionRequestsCreate(ctx, testGK, id)
		require.NoError(t, err)
	}

	require.NoError(t, store.EdgesDeleteFinalizingDependsOn(ctx, target.ID))

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

	has, err := store.EdgesHasIncoming(ctx, owner.ID)
	require.NoError(t, err)
	assert.False(t, has, "no edges yet")

	require.NoError(t, addEdge(ctx, store, child.ID, owner.ID, beehive.RelationOwnedBy))

	has, err = store.EdgesHasIncoming(ctx, owner.ID)
	require.NoError(t, err)
	assert.True(t, has, "owner is referenced by the child")

	has, err = store.EdgesHasIncoming(ctx, child.ID)
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
	has, err := store.EdgesHasIncoming(ctx, target.ID)
	require.NoError(t, err)
	assert.True(t, has)

	// Once the dependent is itself finalizing, its claim is void — it's going away.
	_, _, err = store.DeletionRequestsCreate(ctx, testGK, dep.ID)
	require.NoError(t, err)
	has, err = store.EdgesHasIncoming(ctx, target.ID)
	require.NoError(t, err)
	assert.False(t, has, "a finalizing dependent does not count as a referrer")

	// But a finalizing owned child still counts: the foreground cascade must wait
	// for it to be physically removed.
	child := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, child.ID, target.ID, beehive.RelationOwnedBy))
	_, _, err = store.DeletionRequestsCreate(ctx, testGK, child.ID)
	require.NoError(t, err)
	has, err = store.EdgesHasIncoming(ctx, target.ID)
	require.NoError(t, err)
	assert.True(t, has, "a finalizing owned child still blocks deletion")
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
			_, err := store.ObjectsUpdateStatus(ctx, testGK, missing, 1, []byte(`{}`), 0)
			return err
		},
		"DeletionRequestsCreate": func() error {
			_, _, err := store.DeletionRequestsCreate(ctx, testGK, missing)
			return err
		},
		// Keyed by a slug no row holds, so here ErrNotFound carries its full meaning:
		// nothing of this kind is named that.
		"DeletionRequestsCreateBySlug": func() error {
			_, _, err := store.DeletionRequestsCreateBySlug(ctx, testGK, "never-created")
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

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	first, _, err := store.DeletionRequestsCreate(ctx, testGK, created.ID)
	require.NoError(t, err)
	require.NotNil(t, first.DeletionRequestedAt)

	second, _, err := store.DeletionRequestsCreate(ctx, testGK, created.ID)
	require.NoError(t, err)
	require.NotNil(t, second.DeletionRequestedAt)
	assert.Equal(t, *first.DeletionRequestedAt, *second.DeletionRequestedAt,
		"deletion timestamp is stamped once and not moved by requeues")
}

// The first call marks and resolves the slug to its row; the repeat is the no-op,
// returning the row so the caller can still advance GC but stamping nothing — same
// timestamp, same resource_version, so no watch churn.
func TestDeletionRequestsCreateBySlugIsIdempotent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Slug: new("w1"), Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	first, changed, err := store.DeletionRequestsCreateBySlug(ctx, testGK, "w1")
	require.NoError(t, err)
	require.True(t, changed, "this call set the flag")
	require.Equal(t, created.ID, first.ID, "the slug resolved to its row")
	require.NotNil(t, first.DeletionRequestedAt)

	second, changed, err := store.DeletionRequestsCreateBySlug(ctx, testGK, "w1")
	require.NoError(t, err)
	assert.False(t, changed, "the repeat changed nothing")
	require.NotNil(t, second.DeletionRequestedAt)
	assert.Equal(t, *first.DeletionRequestedAt, *second.DeletionRequestedAt,
		"the deletion timestamp is stamped once")
	assert.Equal(t, first.ResourceVersion, second.ResourceVersion,
		"a no-op must not bump the watch cursor")
}

// Slugs are unique per kind, not globally, so another kind's row holding the same
// slug is simply absent here — ErrNotFound, never ErrWrongKind, and untouched.
func TestDeletionRequestsCreateBySlugIsKindScoped(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	otherGK := beehive.GroupKind{Group: "", Kind: "Other"}

	other, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: otherGK.Group, Kind: otherGK.Kind, Slug: new("shared"), Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	_, _, err = store.DeletionRequestsCreateBySlug(ctx, testGK, "shared")
	assert.ErrorIs(t, err, beehive.ErrNotFound)
	assert.NotErrorIs(t, err, beehive.ErrWrongKind)

	got, err := store.ObjectsGet(ctx, other.ID)
	require.NoError(t, err)
	assert.Nil(t, got.DeletionRequestedAt, "the other kind's row is untouched")
}

func TestDeleteObject(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	require.NoError(t, store.ObjectsDelete(ctx, created.ID))

	_, err = store.ObjectsGet(ctx, created.ID)
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
		obj, err := store.ObjectsCreate(ctx, &beehive.RawObject{
			Group: testGK.Group, Kind: testGK.Kind, Slug: new("committed"),
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
		_, err := store.ObjectsCreate(ctx, &beehive.RawObject{
			Group: testGK.Group, Kind: testGK.Kind, Slug: new("rolledback"),
			Spec: []byte(`{}`),
		})
		require.NoError(t, err)
		return sentinel
	})
	assert.ErrorIs(t, err, sentinel)
	_, err = store.ObjectsGetBySlug(ctx, testGK, "rolledback")
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
			obj, err := store.ObjectsCreate(ctx, &beehive.RawObject{
				Group: testGK.Group, Kind: testGK.Kind, Slug: new("hooked"),
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

func TestObjectsListUnsettledIDs(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	otherGK := beehive.GroupKind{Group: "", Kind: "Other"}

	// settled: ObservedGeneration == Generation — must NOT appear
	settled, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	_, err = store.ObjectsUpdateStatus(ctx, testGK, settled.ID, settled.Generation, []byte(`{}`), 0)
	require.NoError(t, err)

	// unsettled: ObservedGeneration is nil — must appear
	nilObs, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	// unsettled: ObservedGeneration < Generation (spec changed after reconcile) — must appear
	stale, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	_, err = store.ObjectsUpdateStatus(ctx, testGK, stale.ID, stale.Generation, []byte(`{}`), 0)
	require.NoError(t, err)
	_, _, err = store.ObjectsUpdateSpec(ctx, testGK, stale.ID, []byte(`{"updated":true}`), 0)
	require.NoError(t, err)

	// different kind — must NOT appear
	_, err = store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: otherGK.Group, Kind: otherGK.Kind, Spec: []byte(`{}`),
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
			_, err := store.ObjectsCreate(ctx, &beehive.RawObject{
				Group: testGK.Group, Kind: testGK.Kind, Slug: new("nested"),
				Spec: []byte(`{}`),
			})
			return err
		}); err != nil {
			return err
		}
		return sentinel
	})
	assert.ErrorIs(t, err, sentinel)

	_, err = store.ObjectsGetBySlug(ctx, testGK, "nested")
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
	rv, err := store.ObjectWritesMaxVersion(context.Background())
	require.NoError(t, err)
	return &writeProbe{t: t, store: store, rv: rv}
}

// writes returns everything above the cursor without moving it.
func (p *writeProbe) writes() []storeapi.ObjectWrite {
	p.t.Helper()
	got, err := p.store.ObjectWritesListSince(context.Background(), p.rv, 100)
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
		INSERT INTO objects ("group", kind, spec, finalizers, generation, resource_version, created_at, updated_at)
		VALUES (?, ?, '{}', 'not-valid-json', 1, 999999, 0, 0)`,
		gk.Group, gk.Kind)
	require.NoError(t, err)
	id, err := res.LastInsertId()
	require.NoError(t, err)
	return beehive.ObjectID(id)
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

	_, err := store.ObjectsCreate(context.Background(), &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
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
	created, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	_, err = store.db.ExecContext(ctx,
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
	ids, err := store.ReconcileOwedListIDs(ctx, testGK)
	require.NoError(t, err)
	assert.Empty(t, ids)

	// Two wakes owed (e.g. a second stamped while the first was still owed).
	require.NoError(t, store.ReconcileOwedIncrement(ctx, a.ID))
	require.NoError(t, store.ReconcileOwedIncrement(ctx, a.ID))
	assert.Equal(t, int64(2), reconcileOwed(t, store, a.ID), "the count is read back off the row")

	ids, err = store.ReconcileOwedListIDs(ctx, testGK)
	require.NoError(t, err)
	assert.Equal(t, []beehive.ObjectID{a.ID}, ids, "only the owed row is listed (b owes nothing)")

	// A pass that observed both services both: subtracting the observed count
	// drains the row in one go, leaving nothing for the backstop to re-enqueue.
	require.NoError(t, store.ReconcileOwedDecrement(ctx, a.ID, 2))
	assert.Zero(t, reconcileOwed(t, store, a.ID), "subtracting the observed count drains the row")
	ids, err = store.ReconcileOwedListIDs(ctx, testGK)
	require.NoError(t, err)
	assert.Empty(t, ids, "drained row leaves the partial index")

	// An increment beyond what a pass observed survives that pass's subtraction.
	require.NoError(t, store.ReconcileOwedIncrement(ctx, a.ID)) // observed by the pass
	require.NoError(t, store.ReconcileOwedIncrement(ctx, a.ID)) // lands during the pass
	require.NoError(t, store.ReconcileOwedDecrement(ctx, a.ID, 1))
	assert.Equal(t, int64(1), reconcileOwed(t, store, a.ID), "the later increment stays owed")

	// Subtracting more than is owed floors at 0 rather than going negative.
	require.NoError(t, store.ReconcileOwedDecrement(ctx, a.ID, 5))
	assert.Zero(t, reconcileOwed(t, store, a.ID), "subtraction floors at 0")
}

func TestReconcileOwedQueryErrors(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	ctx := context.Background()

	_, err := store.ReconcileOwedListIDs(ctx, testGK)
	require.Error(t, err)
	require.Error(t, store.ReconcileOwedIncrement(ctx, 1))
	require.Error(t, store.ReconcileOwedDecrement(ctx, 1, 1))
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

	_, err := store.ObjectsUpdateStatus(context.Background(), testGK, 1, 1, []byte(`{}`), 0)
	require.Error(t, err)
}

func TestDeletionRequestsCreateDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	_, _, err := store.DeletionRequestsCreate(context.Background(), testGK, 1)
	require.Error(t, err)
}

func TestDeletionRequestsCreateScanError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	// Insert a row with bad finalizers JSON and no deletion_requested_at.
	// DeletionRequestsCreate will UPDATE it (WHERE deletion_requested_at IS NULL matches),
	// the RETURNING clause gives us the row, and scanObject fails on bad finalizers.
	id := insertBadFinalizersRow(t, store, testGK)

	_, _, err := store.DeletionRequestsCreate(ctx, testGK, id)
	require.Error(t, err)
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
	obj, err := store.ObjectsCreate(context.Background(), &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	return obj
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

// addEdge declares an edge for test scaffolding: it discards the EdgesAddResult and
// passes no version claim (0), keeping the common require.NoError(t, addEdge(...))
// shape a one-liner. Tests that assert on the result or the version guard call
// store.EdgesAdd directly.
func addEdge(ctx context.Context, store beehive.Store, from, to beehive.ObjectID, relation beehive.Relation) error {
	_, err := store.EdgesAdd(ctx, from, to, relation, 0)
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

// TestRefsAddReportsEndpoints pins the one thing the endpoint check reports back:
// the source's GroupKind. The edge is cross-kind, so a caller routing a wake to
// fromID cannot assume its own kind, and it must come from the same round-trip as
// the insert. The target's resource_version is read on this path too, but is
// consumed inside EdgesAdd rather than reported — TestEdgesAddStampsReconcileOwed
// covers it, by observing the stamp it decides.
func TestRefsAddReportsEndpoints(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	res, err := store.EdgesAdd(ctx, a.ID, b.ID, "depends_on", 0)
	require.NoError(t, err)
	assert.Equal(t, beehive.GroupKind{Group: testGK.Group, Kind: testGK.Kind}, res.From, "fromID's kind")
	assert.Equal(t, 1, countEdges(t, store, a.ID, b.ID, "depends_on"), "this call created the edge")

	res, err = store.EdgesAdd(ctx, a.ID, b.ID, "depends_on", 0)
	require.NoError(t, err)
	assert.Equal(t, beehive.GroupKind{Group: testGK.Group, Kind: testGK.Kind}, res.From, "re-declare reports it too")
	assert.Equal(t, 1, countEdges(t, store, a.ID, b.ID, "depends_on"), "the edge already existed; the insert was a no-op")
}

// TestRefsAddRejectsFutureTargetVersion pins that the version claim is
// checked before the insert, not after: a rejected call must leave no edge on its
// own, since its caller may be sharing a transaction it fully intends to commit.
func TestRefsAddRejectsFutureTargetVersion(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	_, err := store.EdgesAdd(ctx, a.ID, b.ID, "depends_on", b.ResourceVersion+1)
	assert.ErrorIs(t, err, beehive.ErrTargetResourceVersionFuture)
	assert.Equal(t, 0, countEdges(t, store, a.ID, b.ID, "depends_on"), "a rejected claim writes nothing")

	// The target's own current version is the boundary, and is accepted.
	_, err = store.EdgesAdd(ctx, a.ID, b.ID, "depends_on", b.ResourceVersion)
	require.NoError(t, err)
	assert.Equal(t, 1, countEdges(t, store, a.ID, b.ID, "depends_on"))
}

// moveTarget writes to b so its resource_version advances past what a caller read,
// which is the "target moved" half of the wake conjunction.
func moveTarget(t *testing.T, store *sqliteStore, id beehive.ObjectID) {
	t.Helper()
	_, err := store.ConditionsSet(context.Background(), testGK,
		id, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
}

// TestEdgesAddStampsReconcileOwed covers the conjunction EdgesAdd evaluates on the
// caller's behalf: the stamp lands only when the edge is new *and* the target has
// moved past the claimed version. Each half is withdrawn in turn, and each one
// alone suppresses the stamp. It doubles as the coverage for the target's
// resource_version, which EdgesAdd reads but does not report — the stamp is the only
// way that read is observable.
func TestEdgesAddStampsReconcileOwed(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)
	stale := b.ResourceVersion
	moveTarget(t, store, b.ID)

	// A zero claim is "no opinion" — what an edge declared before reading the
	// target passes — so there is nothing to have raced.
	res, err := store.EdgesAdd(ctx, a.ID, b.ID, "depends_on", 0)
	require.NoError(t, err)
	assert.False(t, res.ReconcileOwedStamped, "no version claim, nothing to have raced")
	assert.Zero(t, reconcileOwed(t, store, a.ID))

	// A claim the target has not moved past: the caller's read is still current.
	c := newRefObject(t, store)
	current, err := store.ObjectsGet(ctx, b.ID)
	require.NoError(t, err)
	res, err = store.EdgesAdd(ctx, c.ID, b.ID, "depends_on", current.ResourceVersion)
	require.NoError(t, err)
	assert.False(t, res.ReconcileOwedStamped, "the target has not moved past the claim")
	assert.Zero(t, reconcileOwed(t, store, c.ID))

	// Both halves hold: the stamp lands, on fromID.
	e := newRefObject(t, store)
	res, err = store.EdgesAdd(ctx, e.ID, b.ID, "depends_on", stale)
	require.NoError(t, err)
	assert.True(t, res.ReconcileOwedStamped)
	assert.Equal(t, int64(1), reconcileOwed(t, store, e.ID), "the stamp is on the dependent, not the target")
	assert.Zero(t, reconcileOwed(t, store, b.ID))
}

// TestRefsAddStampsOnlyNewEdge pins the edge-new half against the statement that
// carries it, the stamp's own NOT EXISTS. Re-declaring an existing edge must not
// stamp, however far the target has moved — otherwise a controller that clears and
// re-declares its dependency set re-fires a wake every pass, unthrottled.
func TestRefsAddStampsOnlyNewEdge(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	stale := b.ResourceVersion
	moveTarget(t, store, b.ID)
	res, err := store.EdgesAdd(ctx, a.ID, b.ID, "depends_on", stale)
	require.NoError(t, err)
	require.True(t, res.ReconcileOwedStamped)

	// Re-declare against an even staler claim, with the target moved again.
	moveTarget(t, store, b.ID)
	res, err = store.EdgesAdd(ctx, a.ID, b.ID, "depends_on", stale)
	require.NoError(t, err)
	assert.False(t, res.ReconcileOwedStamped, "the edge was already there, so the stamp is suppressed")
	assert.Equal(t, int64(1), reconcileOwed(t, store, a.ID), "still the one wake owed")
}

// TestRefsAddStampFailureLeavesNoEdge is the ordering guarantee itself. The stamp
// is a write, so it must land on the same side of the insert as the version
// rejection: a nested Within is a bare fn(ctx) with no transaction of its own, so
// a caller that handles EdgesAdd's error — here by swallowing it and committing the
// ambient transaction anyway — unwinds nothing. Were the stamp sequenced after the
// insert, that caller would commit an edge with no wake, stranding the dependent
// on a stale read where ObjectsListUnsettledIDs cannot see it. Running last, the insert
// simply never happens.
func TestRefsAddStampFailureLeavesNoEdge(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)
	stale := b.ResourceVersion
	moveTarget(t, store, b.ID)
	// The stamp is the only UPDATE on objects EdgesAdd issues, so blocking those
	// fails it while leaving the endpoint read and the edges insert alone. RAISE(ABORT)
	// undoes the statement, not the transaction, so the outer transaction below is
	// still committable — exactly the failure band where ordering, rather than
	// rollback, is what protects the caller.
	blockObjectUpdates(t, store)

	err := store.Within(ctx, func(ctx context.Context) error {
		if _, err := store.EdgesAdd(ctx, a.ID, b.ID, "depends_on", stale); err != nil {
			return nil // the caller logs and carries on; the outer tx still commits
		}
		return assert.AnError // EdgesAdd must not have succeeded
	})
	require.NoError(t, err)

	assert.Equal(t, 0, countEdges(t, store, a.ID, b.ID, "depends_on"),
		"a failed stamp must leave no edge, committed or not")
	assert.Zero(t, reconcileOwed(t, store, a.ID))
}

// TestRefsAddEdgeFailureLeavesStamp is the other side of the ordering tradeoff, and
// pins that it fails the way the design claims rather than merely asserting it in a
// comment. Stamp first, insert second means the reverse residual is possible: the
// insert aborts, the caller swallows EdgesAdd's error and commits the ambient
// transaction, and a wake is owed for an edge that does not exist.
//
// That is the deliberately chosen direction. This residual is self-correcting — the
// count is drained by the next reconcile of that object (TestReconcileDecrementsReconcileOwed
// drains exactly such an edgeless wake), costing one spurious no-op pass — whereas
// the opposite ordering leaves an edge with no wake, which nothing re-derives and
// ObjectsListUnsettledIDs cannot see. One is a wasted reconcile; the other is a permanently
// stale dependent.
func TestRefsAddEdgeFailureLeavesStamp(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)
	stale := b.ResourceVersion
	moveTarget(t, store, b.ID)
	blockEdgeInserts(t, store)

	err := store.Within(ctx, func(ctx context.Context) error {
		if _, err := store.EdgesAdd(ctx, a.ID, b.ID, "depends_on", stale); err != nil {
			return nil // the caller logs and carries on; the outer tx still commits
		}
		return assert.AnError // EdgesAdd must not have succeeded
	})
	require.NoError(t, err)

	assert.Equal(t, 0, countEdges(t, store, a.ID, b.ID, "depends_on"), "the edge did not land")
	assert.Equal(t, int64(1), reconcileOwed(t, store, a.ID),
		"the stamp did, and stands as a self-draining spurious wake rather than a lost one")
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
	require.NoError(t, store.EdgesDelete(ctx, a.ID, b.ID, "depends_on"))
	assert.Equal(t, 0, countEdges(t, store, a.ID, b.ID, "depends_on"))
}

func TestDeleteRefAbsentNoop(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	// No edge exists, and a nonexistent endpoint, are both silent no-ops.
	require.NoError(t, store.EdgesDelete(ctx, a.ID, b.ID, "depends_on"))
	require.NoError(t, store.EdgesDelete(ctx, a.ID, 9999, "depends_on"))

	probe := newWriteProbe(t, store)

	require.NoError(t, store.EdgesDelete(ctx, a.ID, b.ID, "depends_on"))
	probe.expectNone()
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

	deps, err := store.EdgesListIncoming(ctx, c.ID, "depends_on")
	require.NoError(t, err)
	require.Equal(t, []beehive.ObjectRef{
		{ID: a.ID, Group: testGK.Group, Kind: testGK.Kind},
		{ID: b.ID, Group: testGK.Group, Kind: testGK.Kind},
	}, deps)

	none, err := store.EdgesListIncoming(ctx, a.ID, "depends_on")
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
	foreign, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: otherGK.Group, Kind: otherGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	for _, child := range []*beehive.RawObject{c1, c2, foreign} {
		require.NoError(t, addEdge(ctx, store, child.ID, owner.ID, "owned_by"))
	}
	// A depends_on edge into owner must not surface under an owned_by query.
	dep := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, dep.ID, owner.ID, "depends_on"))

	_, err = store.ConditionsSet(ctx, testGK, c1.ID,
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

	_, err := store.EdgesAdd(ctx, a.ID, b.ID, "depends_on", 0)
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

	_, err = store.EdgesAdd(ctx, a.ID, b.ID, "depends_on", 0)
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
	require.Error(t, store.EdgesDelete(context.Background(), 1, 2, "depends_on"))
}

func TestRefsListIncomingDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.EdgesListIncoming(context.Background(), 1, "depends_on")
	require.Error(t, err)
}

// newConditionObject creates a bare object to hang conditions on.
func newConditionObject(t *testing.T, store beehive.Store, name string) *beehive.RawObject {
	t.Helper()
	obj, err := store.ObjectsCreate(context.Background(), &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Slug: new(name), Spec: []byte(`{}`),
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

	got, err := store.ConditionsSet(ctx, testGK, obj.ID, storeapi.Condition{
		Type: "Ready", Status: "True", Reason: "Provisioned", Message: "all good",
	})
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

	_, err := store.ConditionsSet(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	// A second, independent type must coexist without clobbering the first.
	_, err = store.ConditionsSet(ctx, testGK, obj.ID, storeapi.Condition{Type: "Healthy", Status: "False", Reason: "Degraded"})
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

	byName, err := store.ObjectsGetBySlug(ctx, testGK, "multi-read")
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

	_, err := store.ConditionsSet(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True", Reason: "A"})
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
	got, err := store.ConditionsSet(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True", Reason: "B"})
	require.NoError(t, err)
	assert.Equal(t, time.UnixMilli(sentinel).UTC(), findCondition(got.Conditions, "Ready").TransitionedAt,
		"same status keeps transitioned_at")

	// Status change: transitioned_at advances to the write's fresh stamp.
	backdate()
	got, err = store.ConditionsSet(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "False", Reason: "C"})
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

	got, err := store.ConditionsSet(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	assert.Greater(t, got.ResourceVersion, obj.ResourceVersion, "a condition change bumps resource_version")

	assert.Equal(t, got.ResourceVersion, probe.expectWrite().ResourceVersion,
		"the condition write is observable in the write log")
}

func TestSetConditionNoOpSuppressed(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "noop")

	first, err := store.ConditionsSet(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True", Reason: "Up"})
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	// An identical write changes nothing: no resource_version bump, no event.
	again, err := store.ConditionsSet(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True", Reason: "Up"})
	require.NoError(t, err)
	assert.Equal(t, first.ResourceVersion, again.ResourceVersion, "identical condition write is a no-op")
	probe.expectNone()
}

func TestDeleteCondition(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "deletable")

	_, err := store.ConditionsSet(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	_, err = store.ConditionsSet(ctx, testGK, obj.ID, storeapi.Condition{Type: "Healthy", Status: "True"})
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	got, err := store.ConditionsDelete(ctx, testGK, obj.ID, "Ready")
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

	got, err := store.ConditionsDelete(ctx, testGK, obj.ID, "Ready")
	require.NoError(t, err)
	assert.Equal(t, obj.ResourceVersion, got.ResourceVersion, "deleting an absent condition is a no-op")
	probe.expectNone()
}

// TestNonConditionWritesPreserveConditions verifies that mutators which don't
// touch conditions still return — and emit — the object with its existing
// conditions assembled, matching Get/List. Otherwise an Update result or a
// Modified watch event after a status/spec change would show Conditions == nil.
func TestNonConditionWritesPreserveConditions(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "preserve")
	_, err := store.ConditionsSet(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	probe := newWriteProbe(t, store)

	// UpdateStatus return + emitted event both carry the existing condition.
	updated, err := store.ObjectsUpdateStatus(ctx, testGK, obj.ID, obj.Generation, []byte(`{"v":1}`), 0)
	require.NoError(t, err)
	require.NotNil(t, findCondition(updated.Conditions, "Ready"), "UpdateStatus result carries conditions")
	probe.expectWrite()

	// ObjectsUpdateSpec too.
	spec, _, err := store.ObjectsUpdateSpec(ctx, testGK, obj.ID, []byte(`{"s":1}`), 0)
	require.NoError(t, err)
	require.NotNil(t, findCondition(spec.Conditions, "Ready"), "ObjectsUpdateSpec result carries conditions")
	probe.expectWrite()

	// DeletionRequestsCreate (the row persists; conditions still exist).
	del, _, err := store.DeletionRequestsCreate(ctx, testGK, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, findCondition(del.Conditions, "Ready"), "DeletionRequestsCreate result carries conditions")
	probe.expectWrite()
}

// TestNonConditionWriteAssemblyError drops the conditions table so the
// post-write condition assembly fails, covering that error branch in the shared
// scanWritten (UpdateStatus/ObjectsUpdateSpec) and in DeletionRequestsCreate.
func TestNonConditionWriteAssemblyError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "assembly-error")

	_, err := store.db.ExecContext(ctx, `DROP TABLE conditions`)
	require.NoError(t, err)

	_, err = store.ObjectsUpdateStatus(ctx, testGK, obj.ID, obj.Generation, []byte(`{}`), 0)
	require.Error(t, err)
	_, _, err = store.ObjectsUpdateSpec(ctx, testGK, obj.ID, []byte(`{}`), 0)
	require.Error(t, err)
	_, _, err = store.DeletionRequestsCreate(ctx, testGK, obj.ID)
	require.Error(t, err)
}

func TestDeleteObjectCascadesConditions(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "cascade")

	_, err := store.ConditionsSet(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
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
	_, err := store.ConditionsSet(ctx, testGK, obj.ID,
		storeapi.Condition{Type: "Connected", Status: "True", Liveness: true})
	require.NoError(t, err)
	_, err = store.ConditionsSet(ctx, testGK, obj.ID,
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

	_, err := store.ConditionsSet(ctx, testGK, obj.ID,
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
	_, err = store.ConditionsSet(ctx, testGK, obj.ID,
		storeapi.Condition{Type: "Connected", Status: "True", Reason: "Dialed", Liveness: true})
	require.NoError(t, err)

	got, err = store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, "True", findCondition(got.Conditions, "Connected").Status,
		"re-confirmed liveness condition is no longer downgraded")
}

func TestSetConditionObjectNotFound(t *testing.T) {
	store := newTestStore(t)
	_, err := store.ConditionsSet(context.Background(), testGK, 999999, storeapi.Condition{
		Type: "Ready", Status: "True",
	})
	assert.ErrorIs(t, err, beehive.ErrNotFound)
}

func TestSetConditionDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.ConditionsSet(context.Background(), testGK, 1, storeapi.Condition{Type: "Ready", Status: "True"})
	require.Error(t, err)
}

func TestSetConditionInvalidStatusRejected(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "bad-status")

	// The conditions.status CHECK constraint rejects anything outside the enum,
	// surfacing as an error from the upsert.
	_, err := store.ConditionsSet(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "Bogus"})
	require.Error(t, err)
}

func TestDeleteConditionDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.ConditionsDelete(context.Background(), testGK, 1, "Ready")
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
// loadConditions) and ObjectsList (via conditionsByIDs).
func TestConditionAssemblyError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "corrupt")

	breakConditionRowRead(t, store, obj.ID)

	_, err := store.ObjectsGet(ctx, obj.ID)
	require.Error(t, err, "ObjectsGet surfaces a conditions scan error")

	_, err = store.ObjectsList(ctx, testGK)
	require.Error(t, err, "ObjectsList surfaces a conditions scan error")
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

	_, err := store.ConditionsSet(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	_, err = store.db.ExecContext(ctx, `DROP TABLE resource_version_seq`)
	require.NoError(t, err)

	// A real change whose version bump fails: the whole call rolls back.
	_, err = store.ConditionsSet(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "False"})
	require.Error(t, err)
	got, err := store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)
	ready := findCondition(got.Conditions, "Ready")
	require.NotNil(t, ready, "rolled-back ConditionsSet must not delete the prior condition")
	assert.Equal(t, "True", ready.Status, "rolled-back ConditionsSet must not apply the changed status")

	// A delete whose version bump fails likewise rolls back, leaving the row.
	_, err = store.ConditionsDelete(ctx, testGK, obj.ID, "Ready")
	require.Error(t, err)
	got, err = store.ObjectsGet(ctx, obj.ID)
	require.NoError(t, err)
	assert.NotNil(t, findCondition(got.Conditions, "Ready"), "rolled-back ConditionsDelete must leave the condition in place")
}

func TestGetConditionScanError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "getcond-corrupt")

	breakConditionRowRead(t, store, obj.ID)

	// The object row reads fine, but ConditionsSet's getCondition pre-read hits the
	// unreadable row and fails before any write.
	_, err := store.ConditionsSet(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "False"})
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
		_, err := store.ConditionsSet(ctx, testGK, obj.ID,
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

	_, err := store.ObjectsUpdateStatus(ctx, testGK, obj.ID, obj.Generation, []byte(`{}`), 0)
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
	settled, err := store.ObjectsUpdateStatus(ctx, testGK, obj.ID, obj.Generation, status, 1)
	require.NoError(t, err)
	blockObjectUpdates(t, store)

	_, err = store.ObjectsUpdateStatus(ctx, testGK, obj.ID, *settled.ObservedGeneration, status, 2)
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
	_, err := store.ObjectsUpdateStatus(ctx, testGK, obj.ID, 0, status, 0)
	require.NoError(t, err)
	dropSeq(t, store)

	_, err = store.ObjectsUpdateStatus(ctx, testGK, obj.ID, obj.Generation, status, 0)
	require.Error(t, err)
}

// TestDeleteConditionScopedReadError covers ConditionsDelete's scoped-read error
// branch: BeginTx succeeds, but the objects table is gone so the read fails.
func TestDeleteConditionScopedReadError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "del-cond-read")
	dropObjects(t, store)

	_, err := store.ConditionsDelete(ctx, testGK, obj.ID, "Ready")
	require.Error(t, err)
}

// TestDeleteConditionDeleteExecError covers ConditionsDelete's DELETE-exec error
// branch: the object read succeeds but the conditions table is gone.
func TestDeleteConditionDeleteExecError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "del-cond-exec")
	dropConditions(t, store)

	_, err := store.ConditionsDelete(ctx, testGK, obj.ID, "Ready")
	require.Error(t, err)
}

// TestDeleteFinalizerResourceVersionError covers FinalizersDelete's
// nextResourceVersion branch: a present finalizer is removed (a real change),
// then the version bump fails.
func TestDeleteFinalizerResourceVersionError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
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

	_, _, err := store.DeletionRequestsCreate(ctx, testGK, obj.ID)
	require.Error(t, err)
}

// TestDeletionRequestsCreateFromOwnerQueryError covers the child-lookup query error.
func TestDeletionRequestsCreateFromOwnerQueryError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.DeletionRequestsCreateFromOwner(context.Background(), 1)
	require.Error(t, err)
}

// TestDeletionRequestsCreateFromOwnerChildMarkError covers the per-child markForDeletion
// error branch: an owned, not-yet-deleting child exists, but the version bump in
// markForDeletion fails (sequence dropped) with a non-ErrNotFound error.
func TestDeletionRequestsCreateFromOwnerChildMarkError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	owner := newRefObject(t, store)
	child := newRefObject(t, store)
	require.NoError(t, addEdge(ctx, store, child.ID, owner.ID, storeapi.RelationOwnedBy))
	dropSeq(t, store)

	_, err := store.DeletionRequestsCreateFromOwner(ctx, owner.ID)
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
	_, err := store.EdgesHasIncoming(context.Background(), 1)
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

	got, err := store.EdgesGroupIncomingByID(ctx,
		[]beehive.ObjectID{target.ID, depA.ID}, beehive.RelationDependsOn)
	require.NoError(t, err)
	assert.Equal(t, []beehive.ObjectID{depA.ID, depB.ID}, refIDs(got[target.ID]))
	_, ok := got[depA.ID]
	assert.False(t, ok, "a target with no inbound depends_on is absent from the map")

	empty, err := store.EdgesGroupIncomingByID(ctx, nil, beehive.RelationDependsOn)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

func TestRefsByIDsDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	ctx := context.Background()
	_, err := store.EdgesGroupOutgoingByID(ctx, []beehive.ObjectID{1}, beehive.RelationOwnedBy)
	require.Error(t, err)
	_, err = store.EdgesGroupIncomingByID(ctx, []beehive.ObjectID{1}, beehive.RelationDependsOn)
	require.Error(t, err)
}

func TestListOutgoingRefsByRelationDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.EdgesListOutgoingByRelation(context.Background(), 1, beehive.RelationOwnedBy)
	require.Error(t, err)
}

func TestDeletionRequestsList(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)
	_ = newRefObject(t, store) // not deletion-pending

	for _, id := range []beehive.ObjectID{a.ID, b.ID} {
		_, _, err := store.DeletionRequestsCreate(ctx, testGK, id)
		require.NoError(t, err)
	}

	// A deleting object of another kind: it must appear too, tagged with its own
	// kind. This listing is deliberately cross-kind — it is the GC sweeper's, and
	// the sweeper's whole reason to exist is the kinds no controller watches.
	otherGK := beehive.GroupKind{Group: "", Kind: "Other"}
	other, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: otherGK.Group, Kind: otherGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	_, _, err = store.DeletionRequestsCreate(ctx, otherGK, other.ID)
	require.NoError(t, err)

	rows, err := store.DeletionRequestsList(ctx)
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
	_, err := store.DeletionRequestsList(ctx)
	require.Error(t, err)
}

func TestScopedMutatorWrongKind(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store) // kind = testGK
	other := beehive.GroupKind{Kind: "Other"}
	_, err := store.ObjectsUpdateStatus(ctx, other, obj.ID, 0, []byte(`{}`), 0)
	require.ErrorIs(t, err, beehive.ErrWrongKind)
}

// ObjectWritesListSince replays what a stalled consumer missed: live rows above a
// cursor, in cursor order, bounded by limit. Kind-agnostic on purpose — a
// depends_on edge may point at a kind with no controller, so a per-kind query
// could not name every target whose change was dropped.
func TestObjectWritesListSince(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	otherGK := beehive.GroupKind{Kind: "Other"}

	mk := func(gk beehive.GroupKind) *beehive.RawObject {
		o, err := store.ObjectsCreate(ctx, &beehive.RawObject{
			Group: gk.Group, Kind: gk.Kind, Spec: []byte(`{}`),
		})
		require.NoError(t, err)
		return o
	}
	first := mk(testGK)
	second := mk(otherGK) // a different kind: it must still come back
	third := mk(testGK)

	// Everything above the first object's version, so `first` is excluded: the
	// cursor is what the consumer already processed, not where it wants to start.
	got, err := store.ObjectWritesListSince(ctx, first.ResourceVersion, 10)
	require.NoError(t, err)
	assert.Equal(t, []storeapi.ObjectWrite{
		{ID: second.ID, ResourceVersion: second.ResourceVersion},
		{ID: third.ID, ResourceVersion: third.ResourceVersion},
	}, got, "cursor-ordered, exclusive of afterRV, spanning kinds")

	// A limit truncates from the low end, so the caller can page forward by taking
	// the last row's version as its next cursor.
	page, err := store.ObjectWritesListSince(ctx, first.ResourceVersion, 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	assert.Equal(t, second.ID, page[0].ID, "the oldest missed change comes first")

	next, err := store.ObjectWritesListSince(ctx, page[0].ResourceVersion, 1)
	require.NoError(t, err)
	require.Len(t, next, 1)
	assert.Equal(t, third.ID, next[0].ID, "paging forward from the last row's version")

	// Caught up: nothing above the newest version.
	none, err := store.ObjectWritesListSince(ctx, third.ResourceVersion, 10)
	require.NoError(t, err)
	assert.Empty(t, none)
}

// A row deleted during the outage is simply absent from the replay rather than
// erroring it. Per edges.to_id's ON DELETE RESTRICT a target cannot be removed
// while anything depends on it, and from_id's CASCADE means a dependent deleted
// first took its own edge with it — so a row that vanished has no dependents left
// to strand.
func TestObjectWritesListSinceSkipsDeletedRows(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	base, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	gone, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	require.NoError(t, store.ObjectsDelete(ctx, gone.ID))

	got, err := store.ObjectWritesListSince(ctx, base.ResourceVersion, 10)
	require.NoError(t, err)
	assert.Empty(t, got, "the deleted row is absent, not an error")
}

func TestObjectWritesListSinceDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.ObjectWritesListSince(context.Background(), 0, 10)
	require.Error(t, err)
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
		obj, err := store.ObjectsCreate(ctx, &beehive.RawObject{
			Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
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
	after, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	assert.Greater(t, after.ResourceVersion, prev, "a delete cannot make the cursor regress")
}

// A non-positive limit is rejected rather than passed through: SQLite reads a
// negative LIMIT as unbounded, which is the opposite of what such a caller asked
// for, and the preallocation below it would panic on a negative capacity.
func TestObjectWritesListSinceRejectsNonPositiveLimit(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	_, err := store.ObjectsCreate(ctx, &beehive.RawObject{Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`)})
	require.NoError(t, err)

	for _, limit := range []int{0, -1} {
		got, err := store.ObjectWritesListSince(ctx, 0, limit)
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
	_, err = store.ObjectsCreate(ctx, &beehive.RawObject{Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`)})
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

	mk := func() storeapi.ObjectID {
		o, err := store.ObjectsCreate(ctx, &beehive.RawObject{
			Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
		})
		require.NoError(t, err)
		return o.ID
	}
	owner := mk()
	for range 3 {
		require.NoError(t, addEdge(ctx, store, mk(), owner, beehive.RelationOwnedBy))
	}

	probe := newWriteProbe(t, store)

	got, err := store.DeletionRequestsCreateFromOwner(ctx, owner)
	require.NoError(t, err)
	require.Len(t, got, 3)

	// Every version the cascade wrote, as a scan returns them.
	var versions []int64
	for _, w := range probe.expectWrites(3) {
		versions = append(versions, w.ResourceVersion)
	}
	assert.IsIncreasing(t, versions, "the cascade's own writes must not overtake each other")
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

	obj, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
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

	target, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	dependent, err := store.ObjectsCreate(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	_, err = store.EdgesAdd(ctx, dependent.ID, target.ID, beehive.RelationDependsOn, 0)
	require.NoError(t, err)

	err = store.ObjectsDelete(ctx, target.ID)
	require.Error(t, err, "the edge's RESTRICT must block the delete")
	assert.NotErrorIs(t, err, storeapi.ErrNotFound, "the row is there; the constraint is what failed")

	// Dropping the edge releases it, which is the order GC drives: the referrer goes
	// first, and the row it was holding open becomes collectable.
	require.NoError(t, store.EdgesDelete(ctx, dependent.ID, target.ID, beehive.RelationDependsOn))
	assert.NoError(t, store.ObjectsDelete(ctx, target.ID))
}
