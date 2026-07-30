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
// beehive Store: rows, edges, the event log, the write cursor every change
// notification is derived from, and schema migrations. It holds no in-memory
// fan-out — consumers scan the write log from a watermark.
package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"time"

	"github.com/amorey/beehive/internal/sqlitemigrate"
	_ "modernc.org/sqlite"
)

//go:embed migrations
var migrations embed.FS

// Open opens (or creates) a Beehive SQLite database at path,
// running any pending schema migrations before returning.
func Open(path string) (*sqliteStore, error) {
	return open(sqlitemigrate.OpenPool(path, 1))
}

// OpenMemory opens a Beehive SQLite database in memory.
// Intended for testing; data is lost when the store is closed.
//
// auto_vacuum matches OpenPool so tests run the production on-disk format — it is
// the one pragma that cannot be changed after the first table exists, so a test
// database on a different mode would silently not exercise FreePagesRelease.
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
		// Truncate to milliseconds to match condition timestamps' precision: the
		// liveness "verifying" check compares a ms-truncated updated_at against
		// processStart, so a sub-ms processStart would wrongly flag a condition
		// written in the same millisecond the process started.
		processStart: fromMillis(toMillis(time.Now().UTC())),
	}, nil
}
