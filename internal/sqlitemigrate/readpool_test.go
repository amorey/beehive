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

package sqlitemigrate

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seeded returns a path to a database holding one table with one row.
func seeded(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "r.db")
	db := OpenPool(path, 1)
	defer db.Close()
	_, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO t VALUES (1, 'a')`)
	require.NoError(t, err)
	return path
}

func TestOpenReadPoolReads(t *testing.T) {
	db := OpenReadPool(seeded(t), 2)
	defer db.Close()

	var v string
	require.NoError(t, db.QueryRow(`SELECT v FROM t WHERE id = 1`).Scan(&v))
	assert.Equal(t, "a", v)
}

func TestOpenReadPoolRefusesWrites(t *testing.T) {
	db := OpenReadPool(seeded(t), 2)
	defer db.Close()

	_, err := db.Exec(`INSERT INTO t VALUES (2, 'b')`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "readonly", "want a write refusal, not a lock timeout")
}

// The reader must not inherit _txlock=immediate: BEGIN IMMEDIATE takes a write
// lock, which query_only refuses, so every transaction on it would fail.
func TestOpenReadPoolBeginsDeferred(t *testing.T) {
	db := OpenReadPool(seeded(t), 2)
	defer db.Close()

	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{})
	require.NoError(t, err, "a plain BEGIN must work on the reader")
	var v string
	require.NoError(t, tx.QueryRow(`SELECT v FROM t WHERE id = 1`).Scan(&v))
	require.NoError(t, tx.Commit())
}
