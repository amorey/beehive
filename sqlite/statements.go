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
	// The single-object reads.
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

	// The multi-row object reads.
	stmtListObjects
	stmtListObjectsByIncomingEdge
	stmtListObjectsByIDs

	// The object writes.
	stmtInsertObject
	stmtSetObservedGeneration
	stmtUpdateStatus
	stmtUpdateSpec
	stmtBumpObject
	stmtSetFinalizers
	stmtDeleteObject

	// The deletion mark and what reads it.
	stmtMarkForDeletionByID
	stmtMarkForDeletionByName
	stmtProbeDeletionByName
	stmtListDeletionRequests

	// reconcile_owed.
	stmtListOwedIDs
	stmtDecrementOwed
	stmtStampOwed

	stmtLoadConditions
	stmtConditionsByIDs
	stmtConditionSetLoad
	stmtDeleteCondition

	stmtGetDriverCursor
	stmtSetDriverCursor

	// The write log and its horizon.
	stmtWriteLogMaxVersionAll
	stmtWriteLogListSinceAll
	stmtWriteLogMaxVersion
	stmtWriteLogTrimmedThrough
	stmtWriteLogKinds
	stmtWriteLogPage
	stmtWriteLogImages
	stmtAppendWriteLog
	stmtAppendWriteLogDelete
	stmtRaiseWriteLogHorizon
	// deleteWriteLogRows runs one of two predicates, both RETURNING.
	stmtTrimWriteLogByAge
	stmtTrimWriteLogOverCap

	// The event log and its horizon.
	stmtLatestEventRun
	stmtLatestEventKey
	stmtEventsMaxVersion
	stmtEventPage
	stmtEventHorizon
	stmtExtendEventRun
	stmtInsertEventRun
	stmtOverCapTimelines
	// trimEvents runs two statements over one predicate: the horizon raise, then
	// the delete. One pair per predicate, kept adjacent so an edit cannot split
	// them.
	stmtRaiseEventHorizonByAge
	stmtTrimEventsByAge
	stmtRaiseEventHorizonOverCap
	stmtTrimEventsOverCap

	// The edges, and the dependency watermark that moves with them.
	stmtEdgesListIncoming
	stmtEdgesListOutgoing
	stmtEdgesListOutgoingByRelation
	// The batched edge lookups, one per direction.
	stmtEdgesGroupIncoming
	stmtEdgesGroupOutgoing
	stmtUnblockedTargets
	stmtListOwnedChildren
	stmtEdgesHasIncoming
	stmtEdgesDeleteFinalizingDependsOn
	stmtEdgeEndpointsForAdd
	stmtEdgeEndpointsForDelete
	stmtInsertEdge
	stmtDeleteEdge
	stmtStampOwedForNewEdge
	stmtClearWatermarkForNewEdge
	stmtWatermarkSet

	stmtDrawResourceVersions

	numStmts
)

// stmtSQL is every prepared statement's text, indexed by stmtID.
var stmtSQL = [numStmts]string{
	stmtGetObjectRow: `
		SELECT ` + objectColumns + `
		  FROM objects
		 WHERE id = ?`,
	stmtIncrementOwed: `
		UPDATE objects
		   SET reconcile_owed = reconcile_owed + 1
		 WHERE id = ?`,
	stmtCheckObjectExists: `
		SELECT 1
		  FROM objects
		 WHERE id = ?`,
	stmtGetObjectRowByName: `
		SELECT ` + objectColumns + `
		  FROM objects
		 WHERE "group" = ? AND kind = ? AND name = ?`,
	stmtGetForReconcile: `
		SELECT ` + objectColumns + `,
		       EXISTS (SELECT 1 FROM edges
		                WHERE from_id = objects.id AND relation = 'depends_on')
		  FROM objects
		 WHERE id = ?`,
	stmtListUnsettledIDs: `
		SELECT id
		  FROM objects
		 WHERE "group" = ? AND kind = ?
		   AND (observed_generation IS NULL OR observed_generation < generation)
		 ORDER BY id`,
	stmtListIDs: `
		SELECT id
		  FROM objects
		 WHERE "group" = ? AND kind = ?
		 ORDER BY id`,

	stmtScopedGate:       scopedSQL(``),
	stmtScopedDeletion:   scopedSQL(`deletion_requested_at`),
	stmtScopedGeneration: scopedSQL(`generation, observed_generation`),
	stmtScopedStatus:     scopedSQL(`schema_version_status, status`),
	stmtScopedFinalizers: scopedSQL(`finalizers, deletion_requested_at IS NOT NULL`),

	stmtListObjects: listObjectsSQL(`
		 WHERE o."group" = ? AND o.kind = ?`),
	stmtListObjectsByIncomingEdge: listObjectsSQL(`
		 WHERE o.id IN (SELECT from_id FROM edges WHERE to_id = ? AND relation = ?)
		   AND o."group" = ? AND o.kind = ?`),
	stmtListObjectsByIDs: listObjectsSQL(`
		 WHERE o.id IN (SELECT value FROM json_each(?))
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
	stmtUpdateSpec: `
		UPDATE objects
		   SET spec = ?, schema_version_spec = ?, generation = generation + 1,
		       resource_version = ?, updated_at = ?
		 WHERE id = ?
		RETURNING ` + objectColumns,
	stmtBumpObject: `
		UPDATE objects
		   SET resource_version = ?, updated_at = ?
		 WHERE id = ?`,
	stmtSetFinalizers: `
		UPDATE objects
		   SET finalizers = ?, resource_version = ?, updated_at = ?
		 WHERE id = ?`,
	stmtDeleteObject: `
		DELETE FROM objects
		 WHERE id = ?`,

	stmtMarkForDeletionByID:   markForDeletionSQL(`id = ? AND "group" = ? AND kind = ?`),
	stmtMarkForDeletionByName: markForDeletionSQL(`"group" = ? AND kind = ? AND name = ?`),
	stmtProbeDeletionByName: `
		SELECT deletion_requested_at
		  FROM objects
		 WHERE "group" = ? AND kind = ? AND name = ?`,
	stmtListDeletionRequests: `
		SELECT id, "group", kind
		  FROM objects
		 WHERE deletion_requested_at IS NOT NULL
		 ORDER BY id`,

	stmtListOwedIDs: `
		SELECT id
		  FROM objects
		 WHERE "group" = ? AND kind = ? AND reconcile_owed != 0
		 ORDER BY id`,
	stmtDecrementOwed: `
		UPDATE objects
		   SET reconcile_owed = max(reconcile_owed - ?, 0)
		 WHERE id = ? AND "group" = ? AND kind = ?`,
	stmtStampOwed: `
		UPDATE objects
		   SET reconcile_owed = reconcile_owed + 1
		 WHERE id IN (SELECT value FROM json_each(?))`,

	stmtLoadConditions: `
		SELECT ` + conditionColumns + `
		  FROM conditions
		 WHERE object_id = ?
		 ORDER BY type`,
	// Conditions().Set's kind gate and its no-op comparisons, keyed on the same
	// object. The condition columns are NULL when the object holds none of the
	// types; status is NOT NULL wherever a row exists, so it marks presence.
	// transitioned_at is absent deliberately: the upsert decides it in SQL.
	stmtConditionSetLoad: `
		SELECT o."group", o.kind, c.type, c.status, c.reason, c.message, c.liveness, c.updated_at
		  FROM objects o
		  LEFT JOIN conditions c
		         ON c.object_id = o.id
		        AND c.type IN (SELECT value FROM json_each(?))
		 WHERE o.id = ?`,
	stmtConditionsByIDs: `
		SELECT ` + conditionColumns + `
		  FROM conditions
		 WHERE object_id IN (SELECT value FROM json_each(?))
		 ORDER BY object_id, type`,
	stmtDeleteCondition: `
		DELETE FROM conditions
		 WHERE object_id = ? AND type = ?`,

	stmtGetDriverCursor: `
		SELECT cursor
		  FROM driver_cursors
		 WHERE name = ?`,
	stmtSetDriverCursor: `
		INSERT INTO driver_cursors (name, cursor, updated_at)
		VALUES (?, ?, ?)
		    ON CONFLICT(name) DO UPDATE
		   SET cursor = excluded.cursor, updated_at = excluded.updated_at
		 WHERE excluded.cursor > driver_cursors.cursor`,

	stmtWriteLogMaxVersionAll: `
		SELECT coalesce((SELECT MAX(resource_version) FROM object_writes), 0), ` +
		writeLogHorizonAll,
	stmtWriteLogListSinceAll: `
		SELECT ` + writeLogColumns + `, ` + writeLogHorizonAll + `
		  FROM object_writes
		 WHERE resource_version > ?
		 ORDER BY resource_version LIMIT ?`,
	stmtWriteLogMaxVersion: `
		SELECT max(
		       coalesce((SELECT MAX(resource_version) FROM object_writes
		                  WHERE "group" = ? AND kind = ?), 0),
		       coalesce((SELECT trimmed_through FROM object_writes_horizon
		                  WHERE "group" = ? AND kind = ?), 0))`,
	stmtWriteLogTrimmedThrough: `
		SELECT trimmed_through
		  FROM object_writes_horizon
		 WHERE "group" = ? AND kind = ?`,
	stmtWriteLogKinds: `
		SELECT DISTINCT "group", kind
		  FROM object_writes`,
	stmtWriteLogPage: `
		SELECT ` + writeLogColumns + `,
		       coalesce((SELECT trimmed_through FROM object_writes_horizon
		                  WHERE "group" = ? AND kind = ?), 0)
		  FROM object_writes
		 WHERE "group" = ? AND kind = ? AND resource_version > ?
		 ORDER BY resource_version LIMIT ?`,
	stmtWriteLogImages: `
		SELECT resource_version, final FROM object_writes
		 WHERE resource_version IN (SELECT value FROM json_each(?))`,
	stmtAppendWriteLog: `
		INSERT INTO object_writes (` + objectWritesColumns + `)
		VALUES (?, ?, ?, ?, ?, ?)`,
	stmtAppendWriteLogDelete: `
		INSERT INTO object_writes
		       (resource_version, object_id, "group", kind, op, written_at, final)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
	stmtRaiseWriteLogHorizon: `
		INSERT INTO object_writes_horizon ("group", kind, trimmed_through)
		VALUES (?, ?, ?)
		    ON CONFLICT("group", kind) DO UPDATE SET trimmed_through = excluded.trimmed_through
		 WHERE excluded.trimmed_through > object_writes_horizon.trimmed_through`,

	stmtTrimWriteLogByAge: trimWriteLogSQL(`written_at < ?`),
	stmtTrimWriteLogOverCap: trimWriteLogSQL(`"group" = ? AND kind = ? AND resource_version <= (
		       SELECT resource_version FROM object_writes
		        WHERE "group" = ? AND kind = ?
		        ORDER BY resource_version DESC LIMIT 1 OFFSET ?)`),

	stmtLatestEventRun: `
		SELECT ` + eventColumns + `
		  FROM events
		 WHERE object_id = ? AND category = ?
		 ORDER BY id DESC LIMIT 1`,
	stmtLatestEventKey: `
		SELECT id, type, reason
		  FROM events
		 WHERE object_id = ? AND category = ?
		 ORDER BY id DESC LIMIT 1`,
	stmtEventsMaxVersion: `
		SELECT MAX(resource_version)
		  FROM events
		 WHERE object_id = ?`,
	stmtEventPage: `
		SELECT ` + eventColumns + `,
		       coalesce((SELECT MAX(trimmed_through) FROM events_horizon
		                  WHERE object_id = ?1 AND (?2 IS NULL OR category = ?2)), 0)
		  FROM events
		 WHERE object_id = ?1 AND resource_version > ?3
		 ORDER BY resource_version LIMIT ?4`,
	// The optional category rides a numbered parameter, as stmtEventPage's does.
	stmtEventHorizon: `
		SELECT MAX(trimmed_through)
		  FROM events_horizon
		 WHERE object_id = ?1 AND (?2 IS NULL OR category = ?2)`,
	stmtExtendEventRun: `
		UPDATE events
		   SET count = count + 1, last_at = ?, message = ?,
		       detail = ?, resource_version = ?
		 WHERE id = ?`,
	stmtInsertEventRun: `
		INSERT INTO events
		       (object_id, category, type, reason, message, detail,
		        count, first_at, last_at, resource_version)
		VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)`,
	stmtOverCapTimelines: eventCapCandidates,

	stmtRaiseEventHorizonByAge:   raiseEventHorizonSQL(eventTrimByAge),
	stmtTrimEventsByAge:          `DELETE FROM events WHERE ` + eventTrimByAge,
	stmtRaiseEventHorizonOverCap: raiseEventHorizonSQL(eventTrimOverCap),
	stmtTrimEventsOverCap:        `DELETE FROM events WHERE ` + eventTrimOverCap,

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
	stmtEdgesGroupIncoming: `
		SELECT r.to_id, o.id, o."group", o.kind
		  FROM edges r JOIN objects o ON o.id = r.from_id
		 WHERE r.to_id IN (SELECT value FROM json_each(?)) AND r.relation = ?
		 ORDER BY r.to_id, r.from_id`,
	stmtEdgesGroupOutgoing: `
		SELECT r.from_id, o.id, o."group", o.kind
		  FROM edges r JOIN objects o ON o.id = r.to_id
		 WHERE r.from_id IN (SELECT value FROM json_each(?)) AND r.relation = ?
		 ORDER BY r.from_id, r.to_id`,
	// This one sorts: its ORDER BY does not lead with the column the IN list
	// constrains, so no index delivers it. See TestTheUnblockedTargetsReadSorts.
	stmtUnblockedTargets: `
		SELECT o.id, o."group", o.kind
		  FROM edges r JOIN objects o ON o.id = r.to_id
		 WHERE r.from_id IN (SELECT value FROM json_each(?)) AND r.relation = ?
		   AND o.deletion_requested_at IS NOT NULL
		   AND o.id <> r.from_id` + edgeOrderByTarget,
	stmtListOwnedChildren: `
		SELECT o.id, o."group", o.kind, o.deletion_requested_at
		  FROM edges r JOIN objects o ON o.id = r.from_id
		 WHERE r.to_id = ? AND r.relation = ?` + edgeOrderByReferrer,
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
	stmtEdgeEndpointsForAdd: `
		SELECT t."group", t.kind, t.deletion_requested_at
		  FROM objects f, objects t
		 WHERE f.id = ? AND t.id = ?`,
	stmtEdgeEndpointsForDelete: `
		SELECT t."group", t.kind,
		       t.deletion_requested_at IS NOT NULL AND f.deletion_requested_at IS NULL
		  FROM objects t, objects f
		 WHERE t.id = ? AND f.id = ?`,
	stmtInsertEdge: `
		INSERT INTO edges (from_id, to_id, relation)
		VALUES (?, ?, ?)
		    ON CONFLICT(from_id, to_id, relation) DO NOTHING`,
	stmtDeleteEdge: `
		DELETE FROM edges
		 WHERE from_id = ? AND to_id = ? AND relation = ?`,
	stmtStampOwedForNewEdge: `
		UPDATE objects
		   SET reconcile_owed = reconcile_owed + 1
		 WHERE id = ? AND ` + edgeIsNew,
	stmtClearWatermarkForNewEdge: `
		DELETE FROM dependency_watermarks
		 WHERE object_id = ? AND ` + edgeIsNew,
	stmtWatermarkSet: `
		INSERT INTO dependency_watermarks (object_id, reconciled_against, reconciled_at)
		SELECT ?, ?, ?
		 WHERE EXISTS (SELECT 1 FROM edges WHERE from_id = ? AND relation = 'depends_on')
		    ON CONFLICT(object_id) DO UPDATE
		   SET reconciled_against = excluded.reconciled_against,
		       reconciled_at      = excluded.reconciled_at
		 WHERE excluded.reconciled_against > dependency_watermarks.reconciled_against`,

	stmtDrawResourceVersions: `
		UPDATE resource_version_seq
		   SET value = value + ?
		 WHERE id = 1
		RETURNING value`,
}

// listObjectsSQL builds the shared multi-row object read: one field per tail.
func listObjectsSQL(tail string) string {
	return `
		SELECT ` + objectColumns + `
		  FROM objects o ` + tail + `
		 ORDER BY o.id`
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
		SELECT object_id, category, MAX(resource_version)
		  FROM events
		 WHERE ` + where + `
		 GROUP BY object_id, category
		    ON CONFLICT(object_id, category) DO UPDATE SET trimmed_through = excluded.trimmed_through
		 WHERE excluded.trimmed_through > events_horizon.trimmed_through`
}

// markForDeletionSQL builds the soft delete over where. RETURNING, not
// RowsAffected: the write log entry needs the row's identity, and where is a
// predicate rather than a known id.
func markForDeletionSQL(where string) string {
	return `
		UPDATE objects
		   SET deletion_requested_at = ?,
		       resource_version = ?,
		       updated_at = ?
		 WHERE (` + where + `) AND deletion_requested_at IS NULL
		RETURNING id, "group", kind`
}

// trimWriteLogSQL builds the log delete over where. RETURNING because the caller
// raises each affected kind's horizon to the highest version it removed there.
func trimWriteLogSQL(where string) string {
	return `
		DELETE FROM object_writes
		 WHERE ` + where + `
		RETURNING "group", kind, resource_version`
}

// scopedSQL builds selectScoped's read: the kind gate's two columns, plus
// whatever the caller scans beside them.
func scopedSQL(cols string) string {
	if cols != "" {
		cols = `, ` + cols
	}
	return `
		SELECT "group", kind` + cols + `
		  FROM objects
		 WHERE id = ?`
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
	stmtStampOwed:                      true,
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
	stmtMarkForDeletionByID:            true,
	stmtMarkForDeletionByName:          true,
	stmtSetFinalizers:                  true,
	stmtDeleteObject:                   true,
	stmtTrimWriteLogByAge:              true,
	stmtTrimWriteLogOverCap:            true,
}

// stmtSet is one pool's preparations.
type stmtSet [numStmts]*sql.Stmt

// readStmt returns a read's preparation, bound to the connection ctx selects. It
// cannot fail: a read is prepared on both pools, so no route reaches an empty
// slot, and a read is never refused. Passing a write is a programming error,
// pinned by TestEachAccessorTakesItsOwnKind.
func (s *sqliteStore) readStmt(ctx context.Context, id stmtID) *sql.Stmt {
	st := liveTx(ctx)
	if st == nil {
		return s.readStmts[id]
	}
	if st.readOnly {
		return st.tx.StmtContext(ctx, s.readStmts[id])
	}
	// A read inside a write transaction runs on the writer's connection, or it
	// reads from before that transaction's own writes.
	return st.tx.StmtContext(ctx, s.writeStmts[id])
}

// writeStmt returns a write's preparation, bound to the connection ctx selects.
// A write always takes the writer's, since the reader has no slot for one — and
// on a read frame it takes none: refused here, before any statement runs, as
// conn does and for the same reason. Passing a read is a programming error,
// pinned by TestEachAccessorTakesItsOwnKind.
func (s *sqliteStore) writeStmt(ctx context.Context, id stmtID) (*sql.Stmt, error) {
	st := liveTx(ctx)
	if st != nil && st.readOnly {
		return nil, errWroteInReadTx
	}
	return bindStmt(ctx, st, s.writeStmts[id]), nil
}

// stmt is readStmt or writeStmt by what the id does, for the shapes below that
// carry either.
func (s *sqliteStore) stmt(ctx context.Context, id stmtID) (*sql.Stmt, error) {
	if stmtWrites[id] {
		return s.writeStmt(ctx, id)
	}
	return s.readStmt(ctx, id), nil
}

// exec, query and queryRow issue id on the connection ctx selects. They are the
// shapes every call site wants; the accessors above are for the few that need
// the statement itself, to hoist it out of a loop.
//
// exec is the write shape: only a write reports a Result nobody reads a row from.
func (s *sqliteStore) exec(ctx context.Context, id stmtID, args ...any) (sql.Result, error) {
	ps, err := s.writeStmt(ctx, id)
	if err != nil {
		return nil, err
	}
	return ps.ExecContext(ctx, args...)
}

func (s *sqliteStore) query(ctx context.Context, id stmtID, args ...any) (*sql.Rows, error) {
	// stmt, not readStmt: deleteWriteLogRows issues a DELETE ... RETURNING through
	// here, so this shape carries either kind.
	ps, err := s.stmt(ctx, id)
	if err != nil {
		return nil, err
	}
	return ps.QueryContext(ctx, args...)
}

// queryRow returns a scanner rather than (*sql.Row, error), so a routing failure
// surfaces at Scan exactly as a query failure already does.
func (s *sqliteStore) queryRow(ctx context.Context, id stmtID, args ...any) scanner {
	ps, err := s.stmt(ctx, id)
	if err != nil {
		return errScanner{err}
	}
	return ps.QueryRowContext(ctx, args...)
}

// errScanner carries a routing failure to the Scan that was going to happen.
type errScanner struct{ err error }

func (e errScanner) Scan(...any) error { return e.err }

// bindStmt binds ps to st's connection, or hands back the pool's own where there
// is no transaction. ps is never nil: a write is refused above before it can
// reach the reader's empty slot, and a read is prepared on both pools.
func bindStmt(ctx context.Context, st *txState, ps *sql.Stmt) *sql.Stmt {
	if st == nil {
		return ps
	}
	return st.tx.StmtContext(ctx, ps)
}

// prepareWriteStatements fills the writer's set, reads included: a read issued
// inside a write transaction runs on the writer's connection. It must run after
// the migrations and before the version seed, which draws through it.
func (s *sqliteStore) prepareWriteStatements(ctx context.Context) error {
	for id := range numStmts {
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
	for id := range numStmts {
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
	return nil
}

// closeStatements releases both sets, freeing the driver's compiled programs.
// Idempotent, as Close is, because Stmt.Close is; a set aliased to the other is
// closed once.
//
// It clears no slot. The sets are written once, while the store is still the
// constructor's, and read without a lock by every caller after that — so a write
// here would race them all, and a goroutine outliving Close would find nil
// rather than a closed statement to report.
func (s *sqliteStore) closeStatements() error {
	var err error
	for id := range numStmts {
		if s.readStmts[id] != nil && s.readStmts[id] != s.writeStmts[id] {
			err = errors.Join(err, s.readStmts[id].Close())
		}
		if s.writeStmts[id] != nil {
			err = errors.Join(err, s.writeStmts[id].Close())
		}
	}
	return err
}
