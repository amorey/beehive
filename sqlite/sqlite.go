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
	"runtime"
	"time"

	"github.com/amorey/beehive/internal/sqlitemigrate"
	_ "modernc.org/sqlite"
)

//go:embed migrations
var migrations embed.FS

// readPoolConns caps the read pool.
func readPoolConns() int {
	return min(4, runtime.GOMAXPROCS(0))
}

// Open opens (or creates) a Beehive SQLite database at path,
// running any pending schema migrations before returning.
//
// Reads that are not inside a transaction run on a second, query_only pool, so
// they do not queue behind writes. See docs/adr/2026-08-06-a-read-pool-beside-the-write-pool.md.
func Open(path string) (*sqliteStore, error) {
	return open(
		sqlitemigrate.OpenPool(path, sqlitemigrate.PoolOptions{MaxConns: 1}),
		sqlitemigrate.OpenPool(path, sqlitemigrate.PoolOptions{MaxConns: readPoolConns(), Reader: true}),
	)
}

// warm opens every connection in db, so none of them attaches later while a
// writer holds the database — which blocks until that write commits. Runs once
// the schema exists and before any writer.
func warm(ctx context.Context, db *sql.DB) error {
	conns := make([]*sql.Conn, 0, db.Stats().MaxOpenConnections)
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	for range cap(conns) {
		c, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		conns = append(conns, c)
	}
	return nil
}

// OpenMemory opens a Beehive SQLite database in memory. Intended for testing;
// data is lost when the store is closed.
//
// auto_vacuum matches OpenPool: it cannot change after the first table exists,
// so a test database on another mode would silently skip FreePagesRelease.
// file::memory: is per-connection, so a second pool here would be a different,
// empty database: the read pool is the write pool.
func OpenMemory() (*sqliteStore, error) {
	// sql.Open only fails on an unregistered driver; modernc is blank-imported.
	db, _ := sql.Open("sqlite", "file::memory:?_pragma=foreign_keys(on)&_pragma=auto_vacuum(incremental)")
	db.SetMaxOpenConns(1)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return open(db, db)
}

// Only the write pool runs Apply; read may alias write, and is warmed once the
// schema exists and before any writer.
func open(write, read *sql.DB) (*sqliteStore, error) {
	ctx := context.Background()
	s := &sqliteStore{
		db:     write,
		readDB: read,
		// Truncated to ms to match condition timestamps: a sub-ms processStart would
		// wrongly flag a condition written in the process's first millisecond.
		processStart: fromMillis(toMillis(time.Now().UTC())),
	}
	if _, err := sqlitemigrate.Apply(ctx, write, migrations, "migrations"); err != nil {
		s.Close()
		return nil, err
	}
	if read != write {
		if err := warm(ctx, read); err != nil {
			s.Close()
			return nil, err
		}
	}
	return s, nil
}
