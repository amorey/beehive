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
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveLoggerNilDisables(t *testing.T) {
	// A nil logger resolves to the shared discard logger: never nil, never emits.
	got := resolveLogger(nil, nil)
	require.NotNil(t, got)
	assert.Same(t, discardLogger, got)

	// A level on a disabled logger is still a no-op (discard ignores everything).
	assert.Same(t, discardLogger, resolveLogger(nil, slog.LevelDebug))
}

func TestResolveLoggerNoLevelPassesThrough(t *testing.T) {
	l := slog.New(slog.DiscardHandler)
	// Without a level override the logger is returned unwrapped.
	assert.Same(t, l, resolveLogger(l, nil))
}

func TestResolveLoggerLevelFilters(t *testing.T) {
	var buf bytes.Buffer
	// Underlying handler at Debug so the levelHandler is the only gate.
	base := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	log := resolveLogger(base, slog.LevelWarn)
	ctx := context.Background()

	assert.False(t, log.Enabled(ctx, slog.LevelInfo), "info is below the warn floor")
	assert.True(t, log.Enabled(ctx, slog.LevelWarn), "warn is at the floor")

	log.Info("dropped")
	log.Warn("kept")

	out := buf.String()
	assert.NotContains(t, out, "dropped")
	assert.Contains(t, out, "kept")
}

// The level wrapper must preserve attrs/groups attached after resolution, since
// Register tags the resolved logger with group/kind via With.
func TestLevelHandlerPreservesAttrs(t *testing.T) {
	var buf bytes.Buffer
	base := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	log := resolveLogger(base, slog.LevelInfo).With("kind", "Widget")
	log.Info("hello", "id", 7)

	out := buf.String()
	assert.True(t, strings.Contains(out, "kind=Widget"), "attr from With survived the wrapper: %q", out)
	assert.Contains(t, out, "id=7")
}
