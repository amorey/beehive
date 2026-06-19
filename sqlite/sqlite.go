package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"time"

	"github.com/amorey/beehive/internal/storeapi"
	"github.com/amorey/beehive/sqlitemigrate"
	"github.com/amorey/gochan/broadcast"
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
func OpenMemory() (*sqliteStore, error) {
	db, err := sql.Open("sqlite", "file::memory:?_pragma=foreign_keys(on)")
	if err != nil {
		panic(err) // impossible: modernc sqlite is always registered via blank import
	}
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
		db:   db,
		hubs: make(map[storeapi.GroupKind]*broadcast.Hub[storeapi.RawWatchEvent]),
		done: make(chan struct{}),
	}, nil
}
