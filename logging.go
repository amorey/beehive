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
)

// discardLogger is the resolved logger when logging is disabled (the default).
// Using slog.DiscardHandler rather than a nil *slog.Logger lets every call site
// log unconditionally, with no nil checks.
var discardLogger = slog.New(slog.DiscardHandler)

// resolveLogger turns the user-supplied (possibly nil) logger and optional
// minimum level into a concrete, never-nil *slog.Logger. A nil logger means
// logging is disabled. A non-nil level wraps the handler so records below it are
// dropped, layered on top of whatever the handler itself already filters.
func resolveLogger(l *slog.Logger, level slog.Leveler) *slog.Logger {
	if l == nil {
		return discardLogger
	}
	if level == nil {
		return l
	}
	return slog.New(&levelHandler{level: level, inner: l.Handler()})
}

// levelHandler drops records below a minimum level before delegating to inner.
// It exists so WithLogLevel can quiet beehive down without the caller having to
// build a leveled handler around their own logging library.
type levelHandler struct {
	level slog.Leveler
	inner slog.Handler
}

func (h *levelHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= h.level.Level() && h.inner.Enabled(ctx, l)
}

func (h *levelHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.inner.Handle(ctx, r)
}

func (h *levelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &levelHandler{level: h.level, inner: h.inner.WithAttrs(attrs)}
}

func (h *levelHandler) WithGroup(name string) slog.Handler {
	return &levelHandler{level: h.level, inner: h.inner.WithGroup(name)}
}
