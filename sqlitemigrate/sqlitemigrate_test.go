package sqlitemigrate

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"
)

// failReadFileFS wraps an fs.FS but fails to open any path ending in .sql, so
// loadMigrations' fs.ReadFile call errors deterministically. Embedding the
// fs.FS interface (not a concrete type) hides any ReadFileFS/ReadDirFS the inner
// FS implements, funnelling fs.ReadFile and fs.ReadDir through Open — directory
// listing still succeeds, only the file read fails.
type failReadFileFS struct {
	fs.FS
}

func (f failReadFileFS) Open(name string) (fs.File, error) {
	if strings.HasSuffix(name, ".sql") {
		return nil, &fs.PathError{Op: "open", Path: name, Err: errors.New("simulated read failure")}
	}
	return f.FS.Open(name)
}

// openDB opens a fresh on-disk SQLite database in a temp dir. A file (not
// :memory:) so the single *sql.DB can span multiple connections without losing
// the schema, matching how the runner is used in production.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

// fsWith writes name->sql pairs into a temp "migrations" dir and returns an
// fs.FS rooted at the temp dir, so Apply(..., fsys, "migrations") reads them.
func fsWith(t *testing.T, files map[string]string) fs.FS {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "migrations")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	for name, body := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600))
	}
	return os.DirFS(root)
}

func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var n int
	err := db.QueryRow(
		`SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?`, name,
	).Scan(&n)
	require.NoError(t, err)
	return n > 0
}

func TestApplyRunsInOrderAndRecords(t *testing.T) {
	db := openDB(t)
	fsys := fsWith(t, map[string]string{
		"0001_first.sql":  `CREATE TABLE alpha (id INTEGER PRIMARY KEY);`,
		"0002_second.sql": `CREATE TABLE beta (id INTEGER PRIMARY KEY);`,
	})

	v, err := Apply(context.Background(), db, fsys, "migrations")
	require.NoError(t, err)
	require.Equal(t, 2, v)
	require.True(t, tableExists(t, db, "alpha"))
	require.True(t, tableExists(t, db, "beta"))

	var versions string
	require.NoError(t, db.QueryRow(
		`SELECT group_concat(version, ',') FROM (SELECT version FROM schema_migrations ORDER BY version)`,
	).Scan(&versions))
	require.Equal(t, "1,2", versions)
}

func TestApplyIsIdempotent(t *testing.T) {
	db := openDB(t)
	fsys := fsWith(t, map[string]string{
		"0001_first.sql": `CREATE TABLE alpha (id INTEGER PRIMARY KEY);`,
	})

	v, err := Apply(context.Background(), db, fsys, "migrations")
	require.NoError(t, err)
	require.Equal(t, 1, v)

	// A second call must be a no-op: it would error ("table alpha already
	// exists") if it re-ran the migration instead of skipping it.
	v, err = Apply(context.Background(), db, fsys, "migrations")
	require.NoError(t, err)
	require.Equal(t, 1, v)
}

func TestApplyResumesAfterPartial(t *testing.T) {
	db := openDB(t)

	v, err := Apply(context.Background(), db, fsWith(t, map[string]string{
		"0001_first.sql": `CREATE TABLE alpha (id INTEGER PRIMARY KEY);`,
	}), "migrations")
	require.NoError(t, err)
	require.Equal(t, 1, v)

	// Now a newer binary ships an additional migration; only v2 should run.
	v, err = Apply(context.Background(), db, fsWith(t, map[string]string{
		"0001_first.sql":  `CREATE TABLE alpha (id INTEGER PRIMARY KEY);`,
		"0002_second.sql": `CREATE TABLE beta (id INTEGER PRIMARY KEY);`,
	}), "migrations")
	require.NoError(t, err)
	require.Equal(t, 2, v)
	require.True(t, tableExists(t, db, "beta"))
}

func TestApplyRejectsNewerThanBinary(t *testing.T) {
	db := openDB(t)

	// Simulate a DB written by a newer binary: schema at v2 already.
	_, err := Apply(context.Background(), db, fsWith(t, map[string]string{
		"0001_first.sql":  `CREATE TABLE alpha (id INTEGER PRIMARY KEY);`,
		"0002_second.sql": `CREATE TABLE beta (id INTEGER PRIMARY KEY);`,
	}), "migrations")
	require.NoError(t, err)

	// This (older) binary only knows v1 — refuse rather than truncate.
	_, err = Apply(context.Background(), db, fsWith(t, map[string]string{
		"0001_first.sql": `CREATE TABLE alpha (id INTEGER PRIMARY KEY);`,
	}), "migrations")
	require.Error(t, err)
}

func TestApplyRejectsVersionGap(t *testing.T) {
	db := openDB(t)
	_, err := Apply(context.Background(), db, fsWith(t, map[string]string{
		"0001_first.sql": `CREATE TABLE alpha (id INTEGER PRIMARY KEY);`,
		"0003_third.sql": `CREATE TABLE gamma (id INTEGER PRIMARY KEY);`,
	}), "migrations")
	require.Error(t, err, "a gap in version numbers must be caught at startup")
}

func TestApplyRejectsBadFilename(t *testing.T) {
	db := openDB(t)
	_, err := Apply(context.Background(), db, fsWith(t, map[string]string{
		"init.sql": `CREATE TABLE alpha (id INTEGER PRIMARY KEY);`,
	}), "migrations")
	require.Error(t, err, "a file without a numeric version prefix is a packaging bug")
}

func TestOpenPool(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db := OpenPool(path, 1)
	t.Cleanup(func() { db.Close() })
	require.NoError(t, db.Ping())
}

func TestLoadMigrationsReadDirError(t *testing.T) {
	// An FS that has no "migrations" directory.
	fsys := fstest.MapFS{}
	_, err := loadMigrations(fsys, "migrations")
	require.Error(t, err)
}

func TestLoadMigrationsSkipsNonSQLAndDirs(t *testing.T) {
	// A directory entry that is a subdirectory and a non-SQL file should be
	// silently skipped; only .sql files are loaded.
	fsys := fsWith(t, map[string]string{
		"0001_first.sql": `CREATE TABLE alpha (id INTEGER PRIMARY KEY);`,
		"README.txt":     `not a migration`,
	})
	// Also inject a subdirectory inside "migrations" via fstest.MapFS layering.
	// fsWith uses os.DirFS which would show real dirs; the simplest approach is
	// to just verify the non-SQL file is ignored by the successful apply.
	db := openDB(t)
	v, err := Apply(context.Background(), db, fsys, "migrations")
	require.NoError(t, err)
	require.Equal(t, 1, v) // only one .sql file was applied
}

func TestLoadMigrationsNonNumericVersion(t *testing.T) {
	fsys := fsWith(t, map[string]string{
		"abc_first.sql": `CREATE TABLE alpha (id INTEGER PRIMARY KEY);`,
	})
	db := openDB(t)
	_, err := Apply(context.Background(), db, fsys, "migrations")
	require.Error(t, err, "a file with a non-numeric version prefix should be rejected")
}

func TestLoadMigrationsReadFileError(t *testing.T) {
	// The directory lists a migration file, but reading it fails. failReadFileFS
	// simulates the read error in pure Go so the test doesn't depend on OS
	// permission bits (which don't deny reads for root or on some platforms).
	base := fstest.MapFS{
		"migrations/0001_first.sql": {Data: []byte(`CREATE TABLE alpha (id INTEGER PRIMARY KEY);`)},
	}
	db := openDB(t)
	_, err := Apply(context.Background(), db, failReadFileFS{base}, "migrations")
	require.Error(t, err)
}

func TestApplyCreateTableError(t *testing.T) {
	// Closing the db before Apply prevents CREATE TABLE schema_migrations.
	db := openDB(t)
	db.Close()
	_, err := Apply(context.Background(), db, fsWith(t, map[string]string{}), "migrations")
	require.Error(t, err)
}

func TestApplyEmptyMigrations(t *testing.T) {
	// A migrations dir with no .sql files hits the early-return path (len==0).
	db := openDB(t)
	fsys := fsWith(t, map[string]string{})
	v, err := Apply(context.Background(), db, fsys, "migrations")
	require.NoError(t, err)
	require.Equal(t, 0, v)
}

func TestRunMigrationBadSQL(t *testing.T) {
	db := openDB(t)
	_, err := Apply(context.Background(), db, fsWith(t, map[string]string{
		"0001_bad.sql": `THIS IS NOT VALID SQL !!!`,
	}), "migrations")
	require.Error(t, err)
}

func TestRunMigrationBeginTxError(t *testing.T) {
	db := openDB(t)
	db.Close()
	err := runMigration(context.Background(), db, migration{version: 1, name: "0001_test.sql", sql: `SELECT 1`})
	require.Error(t, err)
}

func TestRunMigrationSchemaInsertError(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()

	// Pre-create schema_migrations and insert version 1 so the INSERT in
	// runMigration fails with a UNIQUE constraint violation.
	_, err := db.ExecContext(ctx, `CREATE TABLE schema_migrations (
		version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at INTEGER NOT NULL
	)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO schema_migrations(version, name, applied_at) VALUES(1, 'existing', 0)`)
	require.NoError(t, err)

	err = runMigration(ctx, db, migration{version: 1, name: "0001_test.sql", sql: `SELECT 1`})
	require.Error(t, err)
}

func TestApplyQuerySchemaVersionError(t *testing.T) {
	// Pre-create a schema_migrations table with the wrong schema so that
	// Apply's CREATE TABLE IF NOT EXISTS is a no-op (table already exists)
	// but SELECT COALESCE(MAX(version), 0) fails because 'version' is absent.
	db := openDB(t)
	ctx := context.Background()

	_, err := db.ExecContext(ctx, `CREATE TABLE schema_migrations (wrong_col TEXT)`)
	require.NoError(t, err)

	_, err = Apply(ctx, db, fsWith(t, map[string]string{}), "migrations")
	require.Error(t, err)
}
