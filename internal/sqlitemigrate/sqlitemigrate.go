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

// Package sqlitemigrate is a tiny, forward-only SQL migration runner for
// SQLite. A caller embeds numbered `*.sql` files and hands them to Apply,
// which records progress in a schema_migrations table. No down-migrations;
// each migration runs in its own transaction, so a crash mid-upgrade resumes
// from the last committed version. A DB written by a newer binary is refused.
package sqlitemigrate

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// OpenPool opens a modernc-sqlite pool at path with Beehive's PRAGMAs baked
// into the DSN — WAL, 5s busy_timeout, synchronous=NORMAL, foreign_keys on,
// auto_vacuum=INCREMENTAL, immediate txlock. maxConns caps the pool: 1 for a
// writer pool, larger for a WAL reader pool. Run Apply against the result.
// See docs/adr/2026-07-29-auto-vacuum-incremental.md.
func OpenPool(path string, maxConns int) *sql.DB {
	// auto_vacuum MUST be set on the DSN, never in a migration: SQLite ignores
	// the pragma on a non-empty database and inside a transaction, both of
	// which a migration is. On an existing NONE database it is a silent no-op.
	dsn := "file:" + path +
		"?_pragma=journal_mode(WAL)" +
		"&_pragma=busy_timeout(5000)" +
		"&_pragma=synchronous(NORMAL)" +
		"&_pragma=foreign_keys(on)" +
		"&_pragma=auto_vacuum(incremental)" +
		"&_txlock=immediate"
	// sql.Open only fails on an unregistered driver; modernc is blank-imported.
	db, _ := sql.Open("sqlite", dsn)
	db.SetMaxOpenConns(maxConns)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return db
}

// migration is one numbered SQL file. Version comes from the filename's
// leading digits (0001_init.sql -> 1): the ordering and the schema_migrations
// row id.
type migration struct {
	version int
	name    string
	sql     string
}

// loadMigrations reads and validates the numbered `*.sql` files under dir in
// fsys, returning them in version order. Versions must be unique and gap-free.
func loadMigrations(fsys fs.FS, dir string) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations dir %q: %w", dir, err)
	}
	var out []migration
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		// NNNN_description.sql; a non-numeric prefix is a packaging bug.
		base := e.Name()
		underscore := strings.IndexByte(base, '_')
		if underscore <= 0 {
			return nil, fmt.Errorf("migration %q has no version prefix", base)
		}
		v, err := strconv.Atoi(base[:underscore])
		if err != nil {
			return nil, fmt.Errorf("migration %q has non-numeric version: %w", base, err)
		}
		b, err := fs.ReadFile(fsys, dir+"/"+base)
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", base, err)
		}
		out = append(out, migration{version: v, name: base, sql: string(b)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	// Unique and gap-free, so a missing file is caught at startup.
	for i, m := range out {
		if m.version != i+1 {
			return nil, fmt.Errorf("migration version gap: expected %d, got %d (%s)", i+1, m.version, m.name)
		}
	}
	return out, nil
}

// Apply brings db up to the latest migration in fsys under dir, each file in
// its own transaction, recorded in schema_migrations. Returns the highest
// version present after the call. A DB whose recorded version is newer than
// the embedded set is refused.
func Apply(ctx context.Context, db *sql.DB, fsys fs.FS, dir string) (int, error) {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    INTEGER PRIMARY KEY,
		name       TEXT NOT NULL,
		applied_at INTEGER NOT NULL
	)`); err != nil {
		return 0, fmt.Errorf("create schema_migrations: %w", err)
	}

	var current int
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&current); err != nil {
		return 0, fmt.Errorf("read schema_migrations: %w", err)
	}

	migs, err := loadMigrations(fsys, dir)
	if err != nil {
		return current, err
	}
	if len(migs) == 0 {
		return current, nil
	}

	// Refuse to open a DB written by a newer binary. Downgrading would
	// otherwise silently truncate columns the new schema relies on.
	latest := migs[len(migs)-1].version
	if current > latest {
		return current, fmt.Errorf("database schema version %d is newer than binary supports (%d)", current, latest)
	}

	for _, m := range migs {
		if m.version <= current {
			continue
		}
		if err := runMigration(ctx, db, m); err != nil {
			return current, fmt.Errorf("migration %s: %w", m.name, err)
		}
		current = m.version
	}
	return current, nil
}

func runMigration(ctx context.Context, db *sql.DB, m migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, m.sql); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO schema_migrations(version, name, applied_at) VALUES(?, ?, ?)`,
		m.version, m.name, time.Now().UnixMilli(),
	); err != nil {
		return err
	}
	return tx.Commit()
}
