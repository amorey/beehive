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

// Package sqlite provides a durable, SQLite-backed implementation of the
// beehive Store. It holds no in-memory fan-out — consumers scan the write log
// from a watermark.
package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"time"

	"github.com/amorey/beehive/internal/sqlitemigrate"
	_ "modernc.org/sqlite"
)

//go:embed migrations
var migrations embed.FS

// ErrInvalidOption reports an option value that has no meaning. Local to this
// package: the store must not import the control plane.
var ErrInvalidOption = errors.New("beehive/sqlite: option value is invalid")

// defaultReadConnections is a guess, and only the first connection is load
// bearing — one already keeps reads out of the writers' queue. More helps only
// readers that genuinely overlap, which the drivers do: the owed pass runs per
// kind, the tailers per watched kind.
const defaultReadConnections = 4

// Option configures a store at Open. Deliberately not beehive's option type:
// the embedder builds the store and hands it to beehive.New, so beehive's
// option machinery never sees one.
type Option func(*openOptions)

type openOptions struct{ readConns int }

// WithReadConnections sets how many connections serve reads. This bounds read
// concurrency, not total connections — the writer is always one. Below 1 is
// ErrInvalidOption.
//
// Ignored by OpenMemory, which cannot open its database twice.
func WithReadConnections(n int) Option {
	return func(o *openOptions) { o.readConns = n }
}

func resolveOpen(opts []Option) (openOptions, error) {
	o := openOptions{readConns: defaultReadConnections}
	for _, opt := range opts {
		opt(&o)
	}
	if o.readConns < 1 {
		return o, fmt.Errorf("%w: read connections must be at least 1, got %d", ErrInvalidOption, o.readConns)
	}
	return o, nil
}

// Open opens (or creates) a Beehive SQLite database at path, running any
// pending schema migrations before returning. Reads that are not inside a
// transaction run on their own pool; see WithReadConnections.
func Open(path string, opts ...Option) (*sqliteStore, error) {
	o, err := resolveOpen(opts)
	if err != nil {
		return nil, err
	}
	s, err := open(sqlitemigrate.OpenPool(path, 1))
	if err != nil {
		return nil, err
	}
	// After open: migrations run on the writer, and a reader opened first would
	// hold a schema that does not exist yet.
	s.readDB = sqlitemigrate.OpenReadPool(path, o.readConns)
	return s, nil
}

// OpenMemory opens a Beehive SQLite database in memory. Intended for testing;
// data is lost when the store is closed.
//
// No read pool: file::memory: is per-connection, so a second pool would be a
// different and empty database. Reads run on the writer, and the split is
// covered by on-disk tests alone.
//
// auto_vacuum matches OpenPool: it cannot change after the first table exists,
// so a test database on another mode would silently skip ReclaimSpace.
func OpenMemory() (*sqliteStore, error) {
	// sql.Open only fails on an unregistered driver; modernc is blank-imported.
	db, _ := sql.Open("sqlite", "file::memory:?_pragma=foreign_keys(on)&_pragma=auto_vacuum(incremental)")
	db.SetMaxOpenConns(1)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return open(db)
}

func open(db *sql.DB) (*sqliteStore, error) {
	if _, err := sqlitemigrate.Apply(context.Background(), db, migrations, "migrations"); err != nil {
		db.Close()
		return nil, err
	}
	return &sqliteStore{
		db: db,
		// Truncated to ms to match condition timestamps: a sub-ms processStart would
		// wrongly flag a condition written in the process's first millisecond.
		processStart: fromMillis(toMillis(time.Now().UTC())),
	}, nil
}
