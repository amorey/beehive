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

// Package sqlitemigrate is a tiny, forward-only SQL migration runner for SQLite
// databases. A caller embeds its numbered `*.sql` files and hands them to Apply;
// the runner records progress in a schema_migrations table and brings the DB up to
// the latest version. Beehive's sqlite store is the only caller.
//
// It is deliberately minimal — no down-migrations, no external dependency. Each
// migration runs in its own transaction so a crash mid-upgrade leaves the DB at
// the last committed version and the next start resumes from there. A DB written
// by a newer binary is refused rather than truncated.
//
// It sits below the public surface because OpenPool is not a neutral opener: it
// decides the on-disk format (see auto_vacuum there), which is a choice only
// Beehive's storage strategy earns the right to make.
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

// OpenPool opens a modernc-sqlite connection pool at path with standard PRAGMAs
// baked into the DSN — WAL journal, 5s busy_timeout, synchronous=NORMAL,
// foreign_keys on, auto_vacuum=INCREMENTAL, and immediate txlock so writes grab
// the lock up front. maxConns caps the pool: pass 1 for a writer pool (writes
// serialize at the pool instead of fighting at the SQLite layer), or a larger
// value for a WAL reader pool. Callers run Apply against the returned pool to
// migrate it.
//
// These are Beehive's pragmas, not a neutral set: this is an opinionated opener,
// and auto_vacuum in particular decides the on-disk format of every database it
// opens. That opinion is what keeps this package internal — nobody outside gets it
// imposed on them. INCREMENTAL costs about one pointer-map page per 200 (~0.5% file
// growth) and nothing measurable per commit; in exchange a database whose row
// count churns can be made to give its pages back, which auto_vacuum=NONE can
// never do without a full VACUUM rewrite. A caller that never drains the
// freelist pays only the 0.5%, and can still drain or VACUUM later without a
// format change — the reverse is not true, which is why the default leans this way.
func OpenPool(path string, maxConns int) *sql.DB {
	// _pragma values are URL-encoded; modernc parses them and applies on each
	// new connection.
	//
	// auto_vacuum MUST be set here rather than in a migration. SQLite writes the
	// mode into the file header when the first table is created, and ignores the
	// pragma both on a non-empty database and inside a transaction — and Apply
	// creates schema_migrations before it runs migration 0001, each migration in
	// its own transaction. So a `PRAGMA auto_vacuum` in a .sql file is a silent
	// no-op twice over. On the DSN, modernc applies it at connection open, while
	// the database is still empty. On an existing NONE database it is likewise a
	// silent no-op, so adding it cannot disturb a file already written.
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

// migration is one numbered SQL file. Version comes from the leading digits of
// the filename (e.g. 0001_init.sql -> 1) and is used both for ordering and as
// the row id in schema_migrations.
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
		// Filenames look like NNNN_description.sql. Pull the leading digits as
		// the version; anything non-numeric is a packaging bug, not user input,
		// so surface it loudly.
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
	// Versions must be unique and gap-free so a missing file in a release is
	// caught at startup, not after the schema is half-applied.
	for i, m := range out {
		if m.version != i+1 {
			return nil, fmt.Errorf("migration version gap: expected %d, got %d (%s)", i+1, m.version, m.name)
		}
	}
	return out, nil
}

// Apply brings db up to the latest migration found in fsys under dir. Files are
// NNNN_name.sql, applied in version order, each in its own transaction,
// recorded in a schema_migrations(version, name, applied_at) table. Returns the
// highest version present in the DB on disk after the call. A crash mid-upgrade
// leaves the DB at the last committed version and the next call resumes from
// there. A DB whose recorded version is newer than the embedded set is refused.
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
