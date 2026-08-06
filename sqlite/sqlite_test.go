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

package sqlite

import (
	"database/sql"
	"errors"
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpen verifies that the file-based Open creates a database at the given
// path, applies all migrations, and exposes the expected tables.
func TestOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	store, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })

	for _, table := range []string{"objects", "conditions", "edges", "resource_version_seq", "schema_migrations"} {
		assert.True(t, tableExists(t, store.db, table), "table %q should exist after migration", table)
	}
}

// TestOpenMemoryAppliesMigrations checks the open path end to end: the in-memory
// store opens, migrations run, and the expected schema exists.
func TestOpenMemoryAppliesMigrations(t *testing.T) {
	store, err := OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })

	for _, table := range []string{"objects", "conditions", "edges", "resource_version_seq", "schema_migrations"} {
		assert.True(t, tableExists(t, store.db, table), "table %q should exist after migration", table)
	}
}

// TestOpenMemorySetsAutoVacuum keeps the in-memory store on the same on-disk
// format as production, so the tests that exercise FreePagesRelease are exercising
// the mode Open actually ships.
func TestOpenMemorySetsAutoVacuum(t *testing.T) {
	store, err := OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })

	var mode int
	require.NoError(t, store.db.QueryRow(`PRAGMA auto_vacuum`).Scan(&mode))
	assert.Equal(t, 2, mode) // 2 = INCREMENTAL
}

// TestOpenApplyError covers the error path in open() by passing a closed *sql.DB
// to open so Apply fails and both pools are closed inside open.
func TestOpenApplyError(t *testing.T) {
	// Pass a DB that has already been closed — Apply will fail to create tables.
	db, err := sql.Open("sqlite", "file::memory:?_pragma=foreign_keys(on)")
	require.NoError(t, err)
	db.Close()

	_, err = open(db, db)
	require.Error(t, err)
}

// TestOpenBuildsAReadPool: on disk the read pool is a second, query_only pool
// that sees committed data and refuses writes.
func TestOpenBuildsAReadPool(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	store, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })

	require.NotSame(t, store.db, store.readDB)

	_, err = store.db.Exec(`UPDATE resource_version_seq SET value = 7 WHERE id = 1`)
	require.NoError(t, err)
	var v int64
	require.NoError(t, store.readDB.QueryRow(`SELECT value FROM resource_version_seq WHERE id = 1`).Scan(&v))
	assert.Equal(t, int64(7), v)

	_, err = store.readDB.Exec(`UPDATE resource_version_seq SET value = 9 WHERE id = 1`)
	assert.Error(t, err, "the read pool must refuse a write")
}

// TestOpenMemoryAliasesTheReadPool: file::memory: is per-connection, so a second
// pool there would be a different, empty database.
func TestOpenMemoryAliasesTheReadPool(t *testing.T) {
	store, err := OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })
	assert.Same(t, store.db, store.readDB)
}

// TestCloseClosesBothPools, including the aliased in-memory case, where the
// second close must not report on a handle already closed.
func TestCloseClosesBothPools(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	store, err := Open(path)
	require.NoError(t, err)
	require.NoError(t, store.Close())
	assert.Error(t, store.db.Ping())
	assert.Error(t, store.readDB.Ping())

	mem, err := OpenMemory()
	require.NoError(t, err)
	require.NoError(t, mem.Close())
	assert.NoError(t, mem.Close(), "Close is idempotent")
}

// The schema is amended in place until the first release, so `sqlite/migrations/`
// holds exactly one file — see the amend-in-place ADR. This is the tripwire on that
// policy: a second file means the schema has to be readable as a history instead,
// and every in-place edit becomes a change that reaches fresh databases only.
func TestTheSchemaIsOneMigration(t *testing.T) {
	entries, err := fs.ReadDir(migrations, "migrations")
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	assert.Equal(t, []string{"0001_init.sql"}, names,
		"a second migration retires the amend-in-place policy; read its ADR before adding one")
}

func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var got string
	err := db.QueryRow(
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, name,
	).Scan(&got)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	require.NoError(t, err)
	return true
}
