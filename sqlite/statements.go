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
	stmtEdgesListIncoming
	stmtEdgesListOutgoing
	stmtEdgesListOutgoingByRelation
	stmtEdgesHasIncoming
	stmtEdgesDeleteFinalizingDependsOn
	stmtDecrementOwed
	stmtWatermarkSet
	stmtProbeDeletionByName
	stmtOverCapTimelines
	stmtEdgeEndpointsForAdd
	stmtStampOwedForNewEdge
	stmtClearWatermarkForNewEdge
	stmtInsertEdge
	stmtDeleteEdge
	stmtEdgeEndpointsForDelete
	stmtAppendWriteLog
	stmtAppendWriteLogDelete
	stmtDrawResourceVersions
	stmtBumpObject
	stmtWriteLogPage
	stmtEventPage
	stmtEventHorizon
	stmtEventHorizonByCategory
	stmtRaiseWriteLogHorizon
	// trimEvents runs two statements over one predicate: the horizon raise, then
	// the delete. One pair per predicate, kept adjacent so an edit cannot split
	// them.
	stmtRaiseEventHorizonByAge
	stmtTrimEventsByAge
	stmtRaiseEventHorizonOverCap
	stmtTrimEventsOverCap
	stmtUpdateSpec
	stmtExtendEventRun
	stmtInsertEventRun

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

	stmtEdgesListIncoming: `
		SELECT o.id, o."group", o.kind
		FROM edges r JOIN objects o ON o.id = r.from_id
		WHERE r.to_id = ? AND r.relation = ?` + edgeOrderByReferrer,
	stmtEdgesListOutgoing: `
		SELECT DISTINCT o.id, o."group", o.kind
		FROM edges r JOIN objects o ON o.id = r.to_id
		WHERE r.from_id = ?` + edgeOrderByTarget,
	stmtEdgesListOutgoingByRelation: `
		SELECT o.id, o."group", o.kind
		FROM edges r JOIN objects o ON o.id = r.to_id
		WHERE r.from_id = ? AND r.relation = ?` + edgeOrderByTarget,
	stmtEdgesHasIncoming: `
		SELECT EXISTS(
			SELECT 1 FROM edges r
			WHERE r.to_id = ?
			  AND NOT (r.relation = ? AND r.from_id IN
			           (SELECT id FROM objects WHERE deletion_requested_at IS NOT NULL)))`,
	stmtEdgesDeleteFinalizingDependsOn: `
		DELETE FROM edges
		WHERE to_id = ? AND relation = ?
		  AND from_id IN (SELECT id FROM objects WHERE deletion_requested_at IS NOT NULL)`,

	stmtDecrementOwed: `UPDATE objects SET reconcile_owed = max(reconcile_owed - ?, 0)
		 WHERE id = ? AND "group" = ? AND kind = ?`,
	stmtWatermarkSet: `
		INSERT INTO dependency_watermarks (object_id, reconciled_against, reconciled_at)
		SELECT ?, ?, ?
		 WHERE EXISTS (SELECT 1 FROM edges WHERE from_id = ? AND relation = 'depends_on')
		    ON CONFLICT(object_id) DO UPDATE
		   SET reconciled_against = excluded.reconciled_against,
		       reconciled_at      = excluded.reconciled_at
		 WHERE excluded.reconciled_against > dependency_watermarks.reconciled_against`,
	stmtProbeDeletionByName: `SELECT deletion_requested_at FROM objects
		 WHERE "group" = ? AND kind = ? AND name = ?`,
	stmtOverCapTimelines: eventCapCandidates,

	stmtEdgeEndpointsForAdd: `
			SELECT t."group", t.kind, t.deletion_requested_at
			FROM objects f, objects t WHERE f.id = ? AND t.id = ?`,
	stmtStampOwedForNewEdge: `
				UPDATE objects SET reconcile_owed = reconcile_owed + 1
				WHERE id = ? AND ` + edgeIsNew,
	stmtClearWatermarkForNewEdge: `
				DELETE FROM dependency_watermarks
				 WHERE object_id = ? AND ` + edgeIsNew,
	stmtInsertEdge: `
			INSERT INTO edges (from_id, to_id, relation) VALUES (?, ?, ?)
			ON CONFLICT(from_id, to_id, relation) DO NOTHING`,
	stmtDeleteEdge: `DELETE FROM edges WHERE from_id = ? AND to_id = ? AND relation = ?`,
	stmtEdgeEndpointsForDelete: `
		SELECT t."group", t.kind,
		       t.deletion_requested_at IS NOT NULL AND f.deletion_requested_at IS NULL
		FROM objects t, objects f WHERE t.id = ? AND f.id = ?`,

	stmtAppendWriteLog: `INSERT INTO object_writes (` + objectWritesColumns + `) VALUES (?, ?, ?, ?, ?, ?)`,
	stmtAppendWriteLogDelete: `
		INSERT INTO object_writes (resource_version, object_id, "group", kind, op, written_at, final)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
	stmtDrawResourceVersions: `UPDATE resource_version_seq SET value = value + ? WHERE id = 1 RETURNING value`,

	stmtBumpObject: `
		UPDATE objects SET resource_version = ?, updated_at = ?
		WHERE id = ?`,

	stmtWriteLogPage: `
		SELECT ` + writeLogColumns + `,
		       coalesce((SELECT trimmed_through FROM object_writes_horizon
		                  WHERE "group" = ? AND kind = ?), 0)
		  FROM object_writes
		 WHERE "group" = ? AND kind = ? AND resource_version > ?
		 ORDER BY resource_version LIMIT ?`,
	stmtEventPage: `
		SELECT ` + eventColumns + `,
		       coalesce((SELECT MAX(trimmed_through) FROM events_horizon
		                  WHERE object_id = ?1 AND (?2 IS NULL OR category = ?2)), 0)
		  FROM events
		 WHERE object_id = ?1 AND resource_version > ?3
		 ORDER BY resource_version LIMIT ?4`,
	stmtEventHorizon:           `SELECT MAX(trimmed_through) FROM events_horizon WHERE object_id = ?`,
	stmtEventHorizonByCategory: `SELECT MAX(trimmed_through) FROM events_horizon WHERE object_id = ? AND category = ?`,
	stmtRaiseWriteLogHorizon: `
			INSERT INTO object_writes_horizon ("group", kind, trimmed_through)
			VALUES (?, ?, ?)
			    ON CONFLICT("group", kind) DO UPDATE SET trimmed_through = excluded.trimmed_through
			 WHERE excluded.trimmed_through > object_writes_horizon.trimmed_through`,

	stmtRaiseEventHorizonByAge:   raiseEventHorizonSQL(eventTrimByAge),
	stmtTrimEventsByAge:          `DELETE FROM events WHERE ` + eventTrimByAge,
	stmtRaiseEventHorizonOverCap: raiseEventHorizonSQL(eventTrimOverCap),
	stmtTrimEventsOverCap:        `DELETE FROM events WHERE ` + eventTrimOverCap,

	stmtUpdateSpec: `
			UPDATE objects
			SET spec = ?, schema_version_spec = ?, generation = generation + 1,
			    resource_version = ?, updated_at = ?
			WHERE id = ?
			RETURNING ` + objectColumns,
	stmtExtendEventRun: `
				UPDATE events SET count = count + 1, last_at = ?, message = ?,
					detail = ?, resource_version = ?
				WHERE id = ?`,
	stmtInsertEventRun: `
			INSERT INTO events
				(object_id, category, type, reason, message, detail,
				 count, first_at, last_at, resource_version)
			VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)`,

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

// The two predicates trimEvents runs over. The outer key predicate is what keeps
// both of each pair's statements on a seek.
const (
	eventTrimByAge   = `last_at < ?`
	eventTrimOverCap = `object_id = ? AND category = ? AND id IN (
			SELECT id FROM events WHERE object_id = ? AND category = ?
			 ORDER BY last_at DESC, id DESC LIMIT -1 OFFSET ?)`
)

// raiseEventHorizonSQL records what a trim over where is about to remove. It runs
// before the delete, from the same predicate in the same transaction.
func raiseEventHorizonSQL(where string) string {
	return `
		INSERT INTO events_horizon (object_id, category, trimmed_through)
		SELECT object_id, category, MAX(resource_version) FROM events
		 WHERE ` + where + `
		 GROUP BY object_id, category
		    ON CONFLICT(object_id, category) DO UPDATE SET trimmed_through = excluded.trimmed_through
		 WHERE excluded.trimmed_through > events_horizon.trimmed_through`
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

	stmtEdgesDeleteFinalizingDependsOn: true,
	stmtDecrementOwed:                  true,
	stmtWatermarkSet:                   true,
	stmtStampOwedForNewEdge:            true,
	stmtClearWatermarkForNewEdge:       true,
	stmtInsertEdge:                     true,
	stmtDeleteEdge:                     true,
	stmtAppendWriteLog:                 true,
	stmtAppendWriteLogDelete:           true,
	stmtDrawResourceVersions:           true,
	stmtBumpObject:                     true,
	stmtRaiseWriteLogHorizon:           true,
	stmtRaiseEventHorizonByAge:         true,
	stmtTrimEventsByAge:                true,
	stmtRaiseEventHorizonOverCap:       true,
	stmtTrimEventsOverCap:              true,
	stmtUpdateSpec:                     true,
	stmtExtendEventRun:                 true,
	stmtInsertEventRun:                 true,
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

// prepareWriteStatements fills the writer's set, reads included: a read issued
// inside a write transaction runs on the writer's connection. It must run after
// the migrations and before the version seed, which draws through it.
func (s *sqliteStore) prepareWriteStatements(ctx context.Context) error {
	for id := stmtID(0); id < numStmts; id++ {
		ps, err := s.db.PrepareContext(ctx, stmtSQL[id])
		if err != nil {
			return fmt.Errorf("prepare %d on the writer: %w", id, err)
		}
		s.writeStmts[id] = ps
	}
	return nil
}

// prepareReadStatements fills the reader's set, which holds no write: preparing
// one against query_only succeeds and fails only on execution, so the nil slot is
// the only representation open can check. It must run after s.readDB is
// assigned, or every read binds to the writer.
func (s *sqliteStore) prepareReadStatements(ctx context.Context) error {
	for id := stmtID(0); id < numStmts; id++ {
		if stmtWrites[id] {
			continue
		}
		if s.readDB == s.db {
			s.readStmts[id] = s.writeStmts[id]
			continue
		}
		var err error
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
