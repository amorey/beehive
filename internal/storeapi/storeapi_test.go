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

package storeapi

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestObjectRefGroupKind pins the projection every requeue/GC caller routes on:
// the two kind columns an ObjectRef carries, and nothing else — the id is the
// payload, not part of the routing key.
func TestObjectRefGroupKind(t *testing.T) {
	r := ObjectRef{ID: 7, Group: "apps", Kind: "Widget"}
	assert.Equal(t, GroupKind{Group: "apps", Kind: "Widget"}, r.GroupKind())

	// The empty group is the core group, not a missing value: it must survive the
	// projection rather than being normalized away.
	core := ObjectRef{ID: 8, Kind: "Widget"}
	assert.Equal(t, GroupKind{Kind: "Widget"}, core.GroupKind())
}

// EventQuery.Matches is the predicate the event watch applies to a page the
// store returned unfiltered, so it has to answer exactly as EventsList's WHERE
// clause does — including the millisecond truncation, since that is the
// column's resolution.
func TestEventQueryMatches(t *testing.T) {
	at := time.UnixMilli(1_000).UTC()
	run := Event{Category: "connection", Type: "Warning", Reason: "ProbeFailed", LastAt: at}
	category := func(s string) *string { return &s }

	assert.True(t, EventQuery{}.Matches(run), "the zero query selects every run")

	assert.True(t, EventQuery{Category: category("connection")}.Matches(run))
	assert.False(t, EventQuery{Category: category("sync")}.Matches(run))
	assert.True(t, EventQuery{Type: "Warning"}.Matches(run))
	assert.False(t, EventQuery{Type: "Normal"}.Matches(run))
	assert.True(t, EventQuery{Reason: "ProbeFailed"}.Matches(run))
	assert.False(t, EventQuery{Reason: "Synced"}.Matches(run))

	assert.True(t, EventQuery{Since: at}.Matches(run), "Since is inclusive")
	assert.False(t, EventQuery{Since: at.Add(time.Millisecond)}.Matches(run))
	assert.True(t, EventQuery{Since: at.Add(500 * time.Microsecond)}.Matches(run),
		"a bound inside the run's own millisecond truncates onto it, as the SQL does")

	// Limit bounds a listing, not a run: it is not a predicate.
	assert.True(t, EventQuery{Limit: 1}.Matches(run))
}
