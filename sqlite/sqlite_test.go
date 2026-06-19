package sqlite

import (
	"database/sql"
	"errors"
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

	for _, table := range []string{"objects", "conditions", "refs", "resource_version_seq", "schema_migrations"} {
		assert.True(t, tableExists(t, store.db, table), "table %q should exist after migration", table)
	}
}

// TestOpenMemoryAppliesMigrations checks the open path end to end: the in-memory
// store opens, migrations run, and the expected schema exists.
func TestOpenMemoryAppliesMigrations(t *testing.T) {
	store, err := OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })

	for _, table := range []string{"objects", "conditions", "refs", "resource_version_seq", "schema_migrations"} {
		assert.True(t, tableExists(t, store.db, table), "table %q should exist after migration", table)
	}
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
