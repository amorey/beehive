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
	"sync"
	"testing"

	"github.com/amorey/beehive/internal/sqlitemigrate"
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
// format as production, so the tests that exercise ReclaimSpace are exercising
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
// to open so Apply fails and the DB is closed inside open.
func TestOpenApplyError(t *testing.T) {
	// Pass a DB that has already been closed — Apply will fail to create tables.
	db, err := sql.Open("sqlite", "file::memory:?_pragma=foreign_keys(on)")
	require.NoError(t, err)
	db.Close()

	_, err = open(db, nil)
	require.Error(t, err)
}

// The reader is prepared last, so a read pool that cannot serve one fails the
// constructor rather than leaving a store whose reads have no statement.
func TestOpenReportsAFailedReaderPreparation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.db")

	_, err := open(sqlitemigrate.OpenPool(path, 1), func() *sql.DB {
		closed := sqlitemigrate.OpenReadPool(path, 1)
		closed.Close()
		return closed
	})
	require.Error(t, err)
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

func TestOpenSizesTheReadPool(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
		want int
	}{
		{"default", nil, defaultReadConnections},
		{"explicit", []Option{WithReadConnections(2)}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := Open(filepath.Join(t.TempDir(), "b.db"), tc.opts...)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, store.Close()) })
			assert.Equal(t, tc.want, store.readDB.Stats().MaxOpenConnections)
		})
	}

	for _, n := range []int{0, -1} {
		_, err := Open(filepath.Join(t.TempDir(), "bad.db"), WithReadConnections(n))
		assert.ErrorIs(t, err, ErrInvalidOption, "n = %d", n)
	}
}

// file::memory: is per-connection, so a second pool would be a different and
// empty database. Reads run on the writer instead, and the option is a no-op.
func TestOpenMemoryAliasesTheReadPool(t *testing.T) {
	store, err := OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })
	assert.Same(t, store.db, store.readDB)
}

func TestCloseClosesBothPools(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "b.db"))
	require.NoError(t, err)
	require.NoError(t, store.Close())

	assert.Error(t, store.readDB.Ping(), "the read pool should be closed too")
	assert.NoError(t, store.Close(), "Close is idempotent")
}

// A failed migration is reported rather than half-opened, and no read pool is
// built behind it — the reader opens only after the writer has migrated.
func TestOpenReportsAMigrationFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "newer.db")
	db := sqlitemigrate.OpenPool(path, 1)
	_, err := db.Exec(`CREATE TABLE schema_migrations (
		version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at INTEGER NOT NULL)`)
	require.NoError(t, err)
	// A version this binary does not have: Apply refuses to downgrade.
	_, err = db.Exec(`INSERT INTO schema_migrations VALUES (9999, 'from a newer binary', 0)`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store, err := Open(path)
	require.Error(t, err)
	assert.Nil(t, store)
	assert.Contains(t, err.Error(), "newer than binary supports")
}

// One process opens a store once. The path is the key, so two stores over one
// file collide and two over different files do not.
func TestOpenRefusesAPathAlreadyOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.db")

	first, err := Open(path)
	require.NoError(t, err)

	_, err = Open(path)
	require.ErrorIs(t, err, ErrAlreadyOpen)

	require.NoError(t, first.Close())
	reopened, err := Open(path)
	require.NoError(t, err, "Close releases the path")
	t.Cleanup(func() { assert.NoError(t, reopened.Close()) })
}

// A path that cannot be made absolute fails the open. Registering the raw path
// would weaken the key and skipping registration would disable the check, so
// neither silent answer is available.
func TestOpenReportsAnUnresolvablePath(t *testing.T) {
	fail := func(string) (string, error) { return "", errors.New("no working directory") }

	_, err := Open(filepath.Join(t.TempDir(), "b.db"), withAbs(fail))
	require.ErrorContains(t, err, "no working directory")
	require.ErrorContains(t, err, "beehive/sqlite:", "every error out of Open names the package")
}

// Every file::memory: store is its own database, so there is no path to collide
// on — which is what keeps the suite running.
func TestOpenMemoryStoresDoNotCollide(t *testing.T) {
	first, err := OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, first.Close()) })

	second, err := OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, second.Close()) })
}

// A disk store is named by its file and a memory store by a token, since every
// file::memory: is a database of its own. Two stores over one database report
// one identity, which is what the control plane keys its claim on.
func TestIdentityNamesTheDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.db")
	store, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })
	assert.Equal(t, path, store.Identity(), "t.TempDir is absolute already")

	first, err := OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, first.Close()) })
	second, err := OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, second.Close()) })

	assert.NotEqual(t, first.Identity(), second.Identity())
	assert.NotEmpty(t, first.Identity())
}

// Close is idempotent and may be concurrent, so the claim must be released by
// exactly one caller: a second release would hand the path away from whatever
// reopened it.
func TestCloseReleasesTheClaimOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.db")
	store, err := Open(path)
	require.NoError(t, err)

	var closing sync.WaitGroup
	for range 4 {
		closing.Go(func() { assert.NoError(t, store.Close()) })
	}
	closing.Wait()

	reopened, err := Open(path)
	require.NoError(t, err, "the closes released the path")
	t.Cleanup(func() { assert.NoError(t, reopened.Close()) })

	require.NoError(t, store.Close())
	_, err = Open(path)
	assert.ErrorIs(t, err, ErrAlreadyOpen,
		"a late Close from the old store must not evict the one that reopened it")
}
