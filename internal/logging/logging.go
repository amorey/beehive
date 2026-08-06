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

// Package logging resolves the user-supplied logger into the never-nil,
// optionally level-gated *slog.Logger the rest of beehive logs through.
package logging

import (
	"context"
	"log/slog"
)

// Discard is the resolved logger when logging is disabled (the default), so
// call sites log unconditionally with no nil checks.
var Discard = slog.New(slog.DiscardHandler)

// Resolve turns the user-supplied (possibly nil) logger and optional minimum
// level into a never-nil *slog.Logger. A nil logger disables logging; a
// non-nil level drops records below it, on top of the handler's own filtering.
func Resolve(l *slog.Logger, level slog.Leveler) *slog.Logger {
	if l == nil {
		return Discard
	}
	if level == nil {
		return l
	}
	return slog.New(&levelHandler{level: level, inner: l.Handler()})
}

// levelHandler drops records below a minimum level before delegating to
// inner; it backs beehive.WithLogLevel.
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
