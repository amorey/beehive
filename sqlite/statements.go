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
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// stmtID names a statement whose text is constant. A call site names the id, not
// a preparation of it: which preparation runs depends on the ctx, which the site
// has not passed yet. See stmtFor.
type stmtID int

const (
	stmtGetObjectRow stmtID = iota
	stmtIncrementOwed
	stmtCheckObjectExists
	stmtGetObjectRowByName
	stmtGetForReconcile
	stmtListUnsettledIDs
	stmtListIDs
	// The scoped reads: one per column list selectScoped is asked for.
	stmtScopedGate
	stmtScopedDeletion
	stmtScopedGeneration
	stmtScopedStatus
	stmtScopedFinalizers
	stmtListObjects
	stmtListObjectsByIncomingEdge
	stmtInsertObject
	stmtSetObservedGeneration
	stmtUpdateStatus
	stmtListDeletionRequests
	stmtListOwedIDs
	stmtLoadConditions
	stmtDeleteCondition
	stmtGetDriverCursor
	stmtSetDriverCursor
	stmtWriteLogMaxVersionAll
	stmtWriteLogListSinceAll
	stmtWriteLogMaxVersion
	stmtWriteLogTrimmedThrough
	stmtWriteLogKinds
	stmtLatestEventRun
	stmtLatestEventKey
	stmtEventsMaxVersion

	numStmts
)

// stmtSQL is every prepared statement's text, indexed by stmtID.
var stmtSQL = [numStmts]string{
	stmtGetObjectRow:  `SELECT ` + objectColumns + ` FROM objects WHERE id = ?`,
	stmtIncrementOwed: `UPDATE objects SET reconcile_owed = reconcile_owed + 1 WHERE id = ?`,

	stmtCheckObjectExists:  `SELECT 1 FROM objects WHERE id = ?`,
	stmtGetObjectRowByName: `SELECT ` + objectColumns + ` FROM objects WHERE "group" = ? AND kind = ? AND name = ?`,
	stmtGetForReconcile: `
		SELECT ` + objectColumns + `,
		       EXISTS (SELECT 1 FROM edges
		                WHERE from_id = objects.id AND relation = 'depends_on')
		  FROM objects WHERE id = ?`,
	stmtListUnsettledIDs: `SELECT id FROM objects
		 WHERE "group" = ? AND kind = ?
		   AND (observed_generation IS NULL OR observed_generation < generation)
		 ORDER BY id`,
	stmtListIDs: `SELECT id FROM objects WHERE "group" = ? AND kind = ? ORDER BY id`,

	stmtListObjects: listObjectsSQL(`WHERE o."group" = ? AND o.kind = ?`),
	stmtListObjectsByIncomingEdge: listObjectsSQL(`
		WHERE o.id IN (SELECT from_id FROM edges WHERE to_id = ? AND relation = ?)
		  AND o."group" = ? AND o.kind = ?`),

	stmtInsertObject: `
		INSERT INTO objects
			("group", kind, name, spec, status, schema_version_spec,
			 generation, resource_version, finalizers, created_at, updated_at)
		VALUES (?, ?, ?, ?, NULL, ?, 1, ?, ?, ?, ?)
		RETURNING ` + objectColumns,
	stmtSetObservedGeneration: `
		UPDATE objects
		SET observed_generation = ?, observed_at = ?, resource_version = ?
		WHERE id = ?`,
	stmtUpdateStatus: `
		UPDATE objects
		SET status = ?, schema_version_status = ?, resource_version = ?, updated_at = ?
		WHERE id = ?`,

	stmtListDeletionRequests: `SELECT id, "group", kind FROM objects
		 WHERE deletion_requested_at IS NOT NULL ORDER BY id`,
	stmtListOwedIDs: `SELECT id FROM objects
		 WHERE "group" = ? AND kind = ? AND reconcile_owed != 0
		 ORDER BY id`,
	stmtLoadConditions:  `SELECT ` + conditionColumns + ` FROM conditions WHERE object_id = ? ORDER BY type`,
	stmtDeleteCondition: `DELETE FROM conditions WHERE object_id = ? AND type = ?`,
	stmtGetDriverCursor: `SELECT cursor FROM driver_cursors WHERE name = ?`,
	stmtSetDriverCursor: `
		INSERT INTO driver_cursors (name, cursor, updated_at) VALUES (?, ?, ?)
		    ON CONFLICT(name) DO UPDATE
		   SET cursor = excluded.cursor, updated_at = excluded.updated_at
		 WHERE excluded.cursor > driver_cursors.cursor`,

	stmtWriteLogMaxVersionAll: `SELECT coalesce((SELECT MAX(resource_version) FROM object_writes), 0), ` +
		writeLogHorizonAll,
	stmtWriteLogListSinceAll: `
		SELECT ` + writeLogColumns + `, ` + writeLogHorizonAll + `
		  FROM object_writes
		 WHERE resource_version > ? ORDER BY resource_version LIMIT ?`,
	stmtWriteLogMaxVersion: `
		SELECT max(
			coalesce((SELECT MAX(resource_version) FROM object_writes
			           WHERE "group" = ? AND kind = ?), 0),
			coalesce((SELECT trimmed_through FROM object_writes_horizon
			           WHERE "group" = ? AND kind = ?), 0))`,
	stmtWriteLogTrimmedThrough: `
		SELECT trimmed_through FROM object_writes_horizon
		 WHERE "group" = ? AND kind = ?`,
	stmtWriteLogKinds: `SELECT DISTINCT "group", kind FROM object_writes`,

	stmtLatestEventRun: `SELECT ` + eventColumns + ` FROM events WHERE object_id = ? AND category = ?
		 ORDER BY id DESC LIMIT 1`,
	stmtLatestEventKey: `SELECT id, type, reason FROM events WHERE object_id = ? AND category = ?
		 ORDER BY id DESC LIMIT 1`,
	stmtEventsMaxVersion: `SELECT MAX(resource_version) FROM events WHERE object_id = ?`,

	stmtScopedGate:       scopedSQL(``),
	stmtScopedDeletion:   scopedSQL(`deletion_requested_at`),
	stmtScopedGeneration: scopedSQL(`generation, observed_generation`),
	stmtScopedStatus:     scopedSQL(`schema_version_status, status`),
	stmtScopedFinalizers: scopedSQL(`finalizers, deletion_requested_at IS NOT NULL`),
}

// listObjectsSQL builds the shared multi-row object read. Objects().ListByIDs
// renders its own tail and cannot be prepared, so it keeps listObjectsWhere.
func listObjectsSQL(tail string) string {
	return `SELECT ` + objectColumns + ` FROM objects o ` + tail + ` ORDER BY o.id`
}

// scopedSQL builds selectScoped's read: the kind gate's two columns, plus
// whatever the caller scans beside them.
func scopedSQL(cols string) string {
	if cols == "" {
		return `SELECT "group", kind FROM objects WHERE id = ?`
	}
	return `SELECT "group", kind, ` + cols + ` FROM objects WHERE id = ?`
}

// stmtWrites marks the ids that write, which are prepared on the writer alone.
// Preparing one on the read pool succeeds and fails only on execution, so the
// nil slot is the only representation open can check.
var stmtWrites = [numStmts]bool{
	stmtIncrementOwed:         true,
	stmtInsertObject:          true,
	stmtSetObservedGeneration: true,
	stmtUpdateStatus:          true,
	stmtDeleteCondition:       true,
	stmtSetDriverCursor:       true,
}

// stmtSet is one pool's preparations.
type stmtSet [numStmts]*sql.Stmt

// stmtFor returns id bound to the connection ctx selects: the transaction's own
// connection while one is live, else the pool the statement was prepared on. A
// nil slot is a routing bug and is reported here, not at execution.
func (s *sqliteStore) stmtFor(ctx context.Context, id stmtID) (*sql.Stmt, error) {
	s.stmtUses.Add(1)
	st := liveTx(ctx)
	switch {
	case st == nil:
		if stmtWrites[id] {
			return s.writeStmts[id], nil
		}
		return s.readStmts[id], nil
	case st.readOnly:
		// As conn does, and for the same reason: refused before any statement runs.
		if stmtWrites[id] {
			return nil, errWroteInReadTx
		}
		return st.tx.StmtContext(ctx, s.readStmts[id]), nil
	default:
		return st.tx.StmtContext(ctx, s.writeStmts[id]), nil
	}
}

// prepareStatements fills both sets. It must run after the migrations, and after
// s.readDB is assigned: a statement prepared before either names a schema or a
// pool that is not there yet.
func (s *sqliteStore) prepareStatements(ctx context.Context) error {
	for id := stmtID(0); id < numStmts; id++ {
		ps, err := s.db.PrepareContext(ctx, stmtSQL[id])
		if err != nil {
			return fmt.Errorf("prepare %d on the writer: %w", id, err)
		}
		s.writeStmts[id] = ps

		if stmtWrites[id] {
			continue
		}
		if s.readDB == s.db {
			s.readStmts[id] = ps
			continue
		}
		if s.readStmts[id], err = s.readDB.PrepareContext(ctx, stmtSQL[id]); err != nil {
			return fmt.Errorf("prepare %d on the reader: %w", id, err)
		}
	}
	return s.warmReadStatements(ctx)
}

// warmReadStatements compiles every read statement on the reader's other
// connections: PrepareContext compiled each on one already, and the rest compile
// at first use otherwise. The writer is one connection, so it is skipped.
//
// Every connection is held until the last is out, or the pool hands back the one
// just released and a single connection is warmed N times. The count comes from
// readConns: an exhausted pool blocks in Conn rather than reporting itself full.
//
// Arguments are not passed. The statement compiles before the argument count is
// checked, and that error is not ErrBadConn, so nothing retries — which is what
// keeps this from needing an argument list per statement. Reads only: an argless
// write binding no placeholders would run.
func (s *sqliteStore) warmReadStatements(ctx context.Context) error {
	if s.readDB == s.db {
		return nil
	}
	held := make([]*sql.Conn, 0, s.readConns)
	defer func() {
		for _, c := range held {
			c.Close()
		}
	}()
	for range s.readConns {
		conn, err := s.readDB.Conn(ctx)
		if err != nil {
			return fmt.Errorf("warm the read pool: %w", err)
		}
		held = append(held, conn)
		for id := stmtID(0); id < numStmts; id++ {
			if stmtWrites[id] {
				continue
			}
			// The bind fails; the compile it is preceded by is the point.
			if rows, err := conn.QueryContext(ctx, stmtSQL[id]); err == nil {
				rows.Close()
			}
		}
		s.stmtWarmed++
	}
	return nil
}

// closeStatements releases both sets, freeing the driver's compiled programs.
// Idempotent, as Close is; a set aliased to the other is closed once.
func (s *sqliteStore) closeStatements() error {
	var err error
	for id := stmtID(0); id < numStmts; id++ {
		if s.readStmts[id] != nil && s.readStmts[id] != s.writeStmts[id] {
			err = errors.Join(err, s.readStmts[id].Close())
		}
		if s.writeStmts[id] != nil {
			err = errors.Join(err, s.writeStmts[id].Close())
		}
		s.readStmts[id], s.writeStmts[id] = nil, nil
	}
	return err
}
