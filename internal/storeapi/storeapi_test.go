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

	"github.com/stretchr/testify/assert"
)

// TestReferrerGroupKind pins the projection every requeue/GC caller routes on:
// the two kind columns a Referrer carries, and nothing else — the id is the
// payload, not part of the routing key.
func TestReferrerGroupKind(t *testing.T) {
	r := Referrer{ID: 7, Group: "apps", Kind: "Widget"}
	assert.Equal(t, GroupKind{Group: "apps", Kind: "Widget"}, r.GroupKind())

	// The empty group is the core group, not a missing value: it must survive the
	// projection rather than being normalized away.
	core := Referrer{ID: 8, Kind: "Widget"}
	assert.Equal(t, GroupKind{Kind: "Widget"}, core.GroupKind())
}
