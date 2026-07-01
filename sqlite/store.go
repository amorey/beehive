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
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/amorey/beehive/internal/conflate"
	"github.com/amorey/beehive/internal/storeapi"
)

type sqliteStore struct {
	db *sql.DB

	// processStart stamps when this store opened. Liveness conditions written by a
	// prior process (updated_at older than this) read as Unknown ("verifying")
	// until a controller re-confirms them in this process.
	processStart time.Time

	// hubs fan watch events out to subscribers, one conflating hub per GroupKind,
	// created lazily on first use. hubMu guards the maps and the closed flag.
	hubMu sync.RWMutex
	hubs  map[storeapi.GroupKind]*conflate.Hub[storeapi.ObjectID, storeapi.RawWatchEvent]
	// eventHubs fan the event log out, one per GroupKind, keyed by run so a run's
	// count-bumps conflate while distinct runs stay separate (see eventKey).
	eventHubs map[storeapi.GroupKind]*conflate.Hub[eventKey, storeapi.Event]
	closed    bool
	// done is closed by Close to wake watcher goroutines that are parked on a
	// send (closing the hub only wakes those parked on a receive).
	done chan struct{}

	// beforeSnapshot, if non-nil, runs after a watcher subscribes to its hub but
	// before it loads the snapshot. Tests set it to publish an event into that
	// window to exercise the resource-version dedup; nil in production.
	beforeSnapshot func()

	// afterStream, if non-nil, runs after a watcher's goroutine has closed its
	// output channel and exited. Tests use it to await exit without reading the
	// output (which would race the goroutine's send/cancel selection); nil in
	// production.
	afterStream func()
}

// Close terminates every active watcher — whether parked on a receive (closing
// the hub wakes it) or on a send (closing done wakes it) — so their Events
// channels close, then closes the database. It is idempotent; after Close the
// store is unusable.
func (s *sqliteStore) Close() error {
	s.hubMu.Lock()
	if !s.closed {
		s.closed = true
		close(s.done)
		for _, h := range s.hubs {
			h.Close()
		}
		for _, h := range s.eventHubs {
			h.Close()
		}
		s.hubs = nil
		s.eventHubs = nil
	}
	s.hubMu.Unlock()
	return s.db.Close()
}

// txKey carries an in-flight *sql.Tx through the context so that Store calls
// made with the ctx passed to Within join that transaction.
type txKey struct{}

// dbtx is the subset of *sql.DB and *sql.Tx the object queries use, so the same
// code path runs both standalone and inside a Within transaction.
type dbtx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// conn returns the ambient transaction if ctx carries one, else the pool.
func (s *sqliteStore) conn(ctx context.Context) dbtx {
	if tx, ok := ctx.Value(txKey{}).(*sql.Tx); ok {
		return tx
	}
	return s.db
}

// Within runs fn inside a single transaction. A nested Within (ctx already
// carries a tx) joins the outer transaction rather than opening a new one.
//
// Read-modify-write atomicity rests on the DSN's _txlock=immediate: BeginTx
// issues BEGIN IMMEDIATE, so a transaction holds the sole WAL write lock from
// BEGIN through Commit, before its first read. No other writer can commit in
// between, so a compare-then-write (UpdateSpec's no-op suppression, SetCondition,
// DeleteFinalizer, …) can't act on a stale snapshot, independent of pool size.
// This only covers compound writes routed through Within; a read then a separate
// write on the bare pool is not atomic, so keep multi-statement mutations here.
//
// Watch events that mutators emit during the transaction are buffered in a
// tx-scoped collector and published only after Commit — and as the very last
// step, so "emit before commit" is structurally impossible. A nested Within
// reuses the outer collector, so there is a single flush at the outermost
// commit; on rollback (an fn error or a failed Commit) the buffer is discarded
// and watchers never see the rolled-back writes.
func (s *sqliteStore) Within(ctx context.Context, fn func(ctx context.Context) error) error {
	if _, ok := ctx.Value(txKey{}).(*sql.Tx); ok {
		return fn(ctx) // nested: joins the outer tx and its collector
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	coll := &eventCollector{}
	ctx = context.WithValue(ctx, txKey{}, tx)
	ctx = context.WithValue(ctx, collectorKey{}, coll)
	defer tx.Rollback() // no-op once Commit succeeds; rolls back on any early return
	if err := fn(ctx); err != nil {
		return err // collector discarded, nothing published
	}
	// Flush only on a clean commit, and as the very last step: a failed commit
	// discards the buffer, and there is no later step that could fail after a
	// successful one — so watchers never see writes that didn't land.
	err = tx.Commit()
	if err == nil {
		s.flush(coll)
	}
	return err
}

// objectColumns is the canonical select list; scanObject reads them in order.
const objectColumns = `id, "group", kind, slug, spec, status,
	schema_version_spec, schema_version_status,
	generation, observed_generation, observed_at, resource_version,
	deletion_requested_at, finalizers, created_at, updated_at`

// nextResourceVersion advances and returns the global write cursor. It draws
// from a standalone counter (not MAX(objects.resource_version)) so that
// physically deleting the highest-versioned row can never make the cursor
// regress and hand out a reused version.
func nextResourceVersion(ctx context.Context, c dbtx) (int64, error) {
	var rv int64
	err := c.QueryRowContext(ctx,
		`UPDATE resource_version_seq SET value = value + 1 WHERE id = 1 RETURNING value`).Scan(&rv)
	return rv, err
}

// currentResourceVersion reads the global write cursor without advancing it. Read
// in the same transaction as a snapshot, it is the exact resource version that
// snapshot reflects: every write committed at or below it is included, every
// later write is not.
func currentResourceVersion(ctx context.Context, c dbtx) (int64, error) {
	var rv int64
	err := c.QueryRowContext(ctx,
		`SELECT value FROM resource_version_seq WHERE id = 1`).Scan(&rv)
	return rv, err
}

// scanAndEmit scans a mutator's RETURNING row, assembles its conditions, and on
// success emits a watch event of typ for the written object. Mutators share it,
// so both the returned object and its watch event carry the full conditions set
// regardless of which column the write touched, matching Get/List.
func (s *sqliteStore) scanAndEmit(ctx context.Context, typ storeapi.WatchEventType, sc scanner) (*storeapi.RawObject, error) {
	obj, err := scanObject(sc)
	if err != nil {
		return nil, err
	}
	if _, err := s.attachConditions(ctx, obj); err != nil {
		return nil, err
	}
	s.emit(ctx, typ, obj)
	return obj, nil
}

func (s *sqliteStore) CreateObject(ctx context.Context, obj *storeapi.RawObject) (*storeapi.RawObject, error) {
	finalizers := marshalFinalizers(obj.Finalizers)
	c := s.conn(ctx)
	rv, err := nextResourceVersion(ctx, c)
	if err != nil {
		return nil, err
	}
	now := toMillis(time.Now().UTC())

	// RETURNING hands back the freshly written row — including the assigned id —
	// in the same statement, so there's no follow-up read.
	row := c.QueryRowContext(ctx, `
		INSERT INTO objects
			("group", kind, slug, spec, status, schema_version_spec,
			 generation, resource_version, finalizers, created_at, updated_at)
		VALUES (?, ?, ?, ?, NULL, ?, 1, ?, ?, ?, ?)
		RETURNING `+objectColumns,
		obj.Group, obj.Kind, obj.Slug, obj.Spec, obj.SpecVersion,
		rv, finalizers, now, now)
	return s.scanAndEmit(ctx, storeapi.WatchEventAdded, row)
}

// getObjectRow reads the objects row without assembling conditions. Internal
// callers that don't need the conditions (existence checks, pre-write reads) use
// it to avoid the extra per-object conditions query GetObject would run.
func (s *sqliteStore) getObjectRow(ctx context.Context, id storeapi.ObjectID) (*storeapi.RawObject, error) {
	row := s.conn(ctx).QueryRowContext(ctx,
		`SELECT `+objectColumns+` FROM objects WHERE id = ?`, id)
	return scanObject(row)
}

// getObjectRowScoped loads id's bare row (no conditions) and confirms it belongs
// to gk. Returns ErrNotFound if the row is gone, ErrWrongKind if it names another
// kind. It replaces the full GetObject + Go-side compare the client/controller
// used to do purely to enforce the kind boundary, dropping the conditions marshal.
func (s *sqliteStore) getObjectRowScoped(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID) (*storeapi.RawObject, error) {
	obj, err := s.getObjectRow(ctx, id)
	if err != nil {
		return nil, err
	}
	if obj.Group != gk.Group || obj.Kind != gk.Kind {
		return nil, fmt.Errorf("%w: object %d is %s/%s, not %s/%s",
			storeapi.ErrWrongKind, id, obj.Group, obj.Kind, gk.Group, gk.Kind)
	}
	return obj, nil
}

// objectInKind reports whether id exists and belongs to gk. A missing id
// (ErrNotFound) or foreign id (ErrWrongKind) reports false without erroring —
// used to scope a read to gk while treating "not this kind's object" as empty
// rather than a failure; other read errors propagate.
func (s *sqliteStore) objectInKind(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID) (bool, error) {
	if _, err := s.getObjectRowScoped(ctx, gk, id); err != nil {
		if errors.Is(err, storeapi.ErrNotFound) || errors.Is(err, storeapi.ErrWrongKind) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *sqliteStore) GetObject(ctx context.Context, id storeapi.ObjectID) (*storeapi.RawObject, error) {
	obj, err := s.getObjectRow(ctx, id)
	if err != nil {
		return nil, err
	}
	return s.attachConditions(ctx, obj)
}

// GetObjectMeta is getObjectRow exposed across the store boundary: id's row with
// no conditions assembled, for metadata-only callers (GC collect, ref checks).
func (s *sqliteStore) GetObjectMeta(ctx context.Context, id storeapi.ObjectID) (*storeapi.RawObject, error) {
	return s.getObjectRow(ctx, id)
}

func (s *sqliteStore) GetObjectBySlug(ctx context.Context, gk storeapi.GroupKind, slug string) (*storeapi.RawObject, error) {
	row := s.conn(ctx).QueryRowContext(ctx,
		`SELECT `+objectColumns+` FROM objects WHERE "group" = ? AND kind = ? AND slug = ?`,
		gk.Group, gk.Kind, slug)
	obj, err := scanObject(row)
	if err != nil {
		return nil, err
	}
	return s.attachConditions(ctx, obj)
}

func (s *sqliteStore) ListObjects(ctx context.Context, gk storeapi.GroupKind) ([]*storeapi.RawObject, error) {
	rows, err := s.conn(ctx).QueryContext(ctx,
		`SELECT `+objectColumns+` FROM objects WHERE "group" = ? AND kind = ? ORDER BY id`,
		gk.Group, gk.Kind)
	if err != nil {
		return nil, err
	}
	out, err := scanObjects(rows)
	if err != nil {
		return nil, err
	}
	rows.Close() // free the connection before the conditions query (single-conn pool)

	// One batched query for the whole kind avoids an N+1 per-object lookup.
	byID, err := s.loadConditionsForKind(ctx, gk)
	if err != nil {
		return nil, err
	}
	for _, obj := range out {
		obj.Conditions = byID[obj.ID]
	}
	return out, nil
}

// loadConditionsForKind returns every condition for objects of kind gk, grouped
// by object id, ordered by type within each object.
func (s *sqliteStore) loadConditionsForKind(ctx context.Context, gk storeapi.GroupKind) (map[storeapi.ObjectID][]storeapi.Condition, error) {
	// Columns qualified to c.* (object_id/type/status order matches
	// conditionColumns) because the JOIN with objects makes bare status ambiguous.
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT c.object_id, c.type, c.status, c.reason, c.message, c.liveness,
		       c.transitioned_at, c.updated_at
		FROM conditions c
		JOIN objects o ON o.id = c.object_id
		WHERE o."group" = ? AND o.kind = ?
		ORDER BY c.object_id, c.type`, gk.Group, gk.Kind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byID := make(map[storeapi.ObjectID][]storeapi.Condition)
	for rows.Next() {
		id, cond, err := scanCondition(rows)
		if err != nil {
			return nil, err
		}
		s.downgradeLiveness(&cond)
		byID[id] = append(byID[id], cond)
	}
	return byID, rows.Err()
}

func (s *sqliteStore) ListUnsettledIDs(ctx context.Context, gk storeapi.GroupKind) ([]storeapi.ObjectID, error) {
	rows, err := s.conn(ctx).QueryContext(ctx,
		`SELECT id FROM objects
		 WHERE "group" = ? AND kind = ?
		   AND (observed_generation IS NULL OR observed_generation < generation)
		 ORDER BY id`,
		gk.Group, gk.Kind)
	if err != nil {
		return nil, err
	}
	return scanIDs(rows)
}

func (s *sqliteStore) ListDeletionPendingIDs(ctx context.Context, gk storeapi.GroupKind) ([]storeapi.ObjectID, error) {
	// Matches the partial index idx ... WHERE deletion_requested_at IS NOT NULL.
	rows, err := s.conn(ctx).QueryContext(ctx,
		`SELECT id FROM objects
		 WHERE "group" = ? AND kind = ? AND deletion_requested_at IS NOT NULL
		 ORDER BY id`,
		gk.Group, gk.Kind)
	if err != nil {
		return nil, err
	}
	return scanIDs(rows)
}

func (s *sqliteStore) ListAllDeletionPendingIDs(ctx context.Context) ([]storeapi.ObjectID, error) {
	// Kind-agnostic: same partial index as ListDeletionPendingIDs, no group/kind
	// filter, so the global GC sweeper sees every finalizing object.
	rows, err := s.conn(ctx).QueryContext(ctx,
		`SELECT id FROM objects WHERE deletion_requested_at IS NOT NULL ORDER BY id`)
	if err != nil {
		return nil, err
	}
	return scanIDs(rows)
}

func (s *sqliteStore) ListIDs(ctx context.Context, gk storeapi.GroupKind) ([]storeapi.ObjectID, error) {
	rows, err := s.conn(ctx).QueryContext(ctx,
		`SELECT id FROM objects WHERE "group" = ? AND kind = ? ORDER BY id`,
		gk.Group, gk.Kind)
	if err != nil {
		return nil, err
	}
	return scanIDs(rows)
}

// scanIDs collects the single id column of a SELECT id query, closing rows.
func scanIDs(rows *sql.Rows) ([]storeapi.ObjectID, error) {
	defer rows.Close()
	var ids []storeapi.ObjectID
	for rows.Next() {
		var id storeapi.ObjectID
		_ = rows.Scan(&id) // INTEGER PRIMARY KEY into int64 never errors
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *sqliteStore) UpdateSpec(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, spec []byte, specVersion int) (*storeapi.RawObject, error) {
	// Within keeps the read-compare-write atomic so a concurrent writer can't slip
	// between the no-op check and the update.
	var result *storeapi.RawObject
	err := s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		// Scoped read enforces the kind boundary (ErrWrongKind for a foreign id)
		// while doubling as the no-op compare's load — no separate kind check.
		obj, err := s.getObjectRowScoped(ctx, gk, id)
		if err != nil {
			return err
		}
		// Identical spec: nothing changed, so don't bump generation/resource_version
		// or emit. A bump would falsely unsettle a converged object and trigger a
		// needless reconcile, and the event would show watchers a spurious diff
		// (mirrors RequestDeletion's idempotent no-op).
		if bytes.Equal(obj.Spec, spec) {
			result, err = s.attachConditions(ctx, obj)
			return err
		}
		rv, err := nextResourceVersion(ctx, c)
		if err != nil {
			return err
		}
		// A real spec change bumps generation so the convergence handshake notices.
		row := c.QueryRowContext(ctx, `
			UPDATE objects
			SET spec = ?, schema_version_spec = ?, generation = generation + 1,
			    resource_version = ?, updated_at = ?
			WHERE id = ?
			RETURNING `+objectColumns,
			spec, specVersion, rv, toMillis(time.Now().UTC()), id)
		result, err = s.scanAndEmit(ctx, storeapi.WatchEventModified, row)
		return err
	})
	return result, err
}

func (s *sqliteStore) UpdateStatus(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, observedGeneration int64, status []byte, statusVersion int) (*storeapi.RawObject, error) {
	// Within keeps the rv-bump and the write atomic now that the controller's
	// retired withinKind no longer provides the transaction (mutators self-wrap).
	var result *storeapi.RawObject
	err := s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		rv, err := nextResourceVersion(ctx, c)
		if err != nil {
			return err
		}
		now := toMillis(time.Now().UTC())
		// The kind and generation >= ? guards both live in the WHERE so the happy
		// path stays a single statement. A foreign id matches no row (scoped out);
		// the generation guard rejects a future observedGeneration — a controller
		// can only have observed a generation that exists, and recording a future
		// one would falsely settle the object once its spec caught up. An older
		// value is fine — the normal case where the spec changed mid-reconcile.
		row := c.QueryRowContext(ctx, `
			UPDATE objects
			SET status = ?, schema_version_status = ?, observed_generation = ?, observed_at = ?,
			    resource_version = ?, updated_at = ?
			WHERE id = ? AND generation >= ? AND "group" = ? AND kind = ?
			RETURNING `+objectColumns,
			status, statusVersion, observedGeneration, now, rv, now, id, observedGeneration, gk.Group, gk.Kind)
		obj, err := s.scanAndEmit(ctx, storeapi.WatchEventModified, row)
		if errors.Is(err, storeapi.ErrNotFound) {
			// No row matched: the object is gone, names another kind, or the guard
			// rejected a future generation. Re-read (no conditions) to return a
			// precise error instead of a misleading ErrNotFound.
			cur, gerr := s.getObjectRowScoped(ctx, gk, id)
			if gerr != nil {
				return gerr // genuinely gone (ErrNotFound) or wrong kind (ErrWrongKind)
			}
			return fmt.Errorf("%w: reported %d, current is %d (object %d)",
				storeapi.ErrObservedGenerationFuture, observedGeneration, cur.Generation, id)
		}
		result = obj
		return err
	})
	return result, err
}

// conditionColumns is the canonical select list for a condition row; scanCondition
// reads them in order. object_id leads so the same scan serves both the
// single-object read and the batched by-kind read (which groups on it).
const conditionColumns = `object_id, type, status, reason, message, liveness,
	transitioned_at, updated_at`

// loadConditions returns id's conditions, ordered by type for a stable view.
func (s *sqliteStore) loadConditions(ctx context.Context, id storeapi.ObjectID) ([]storeapi.Condition, error) {
	rows, err := s.conn(ctx).QueryContext(ctx,
		`SELECT `+conditionColumns+` FROM conditions WHERE object_id = ? ORDER BY type`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []storeapi.Condition
	for rows.Next() {
		_, cond, err := scanCondition(rows)
		if err != nil {
			return nil, err
		}
		s.downgradeLiveness(&cond)
		out = append(out, cond)
	}
	return out, rows.Err()
}

// scanCondition decodes one condition row (conditionColumns order), returning its
// object id alongside the condition. The liveness downgrade is applied by the
// read-path callers, not here, so getCondition's no-op comparison sees stored truth.
func scanCondition(sc scanner) (storeapi.ObjectID, storeapi.Condition, error) {
	var (
		id             storeapi.ObjectID
		cond           storeapi.Condition
		reason         sql.NullString
		message        sql.NullString
		liveness       bool
		transitionedAt int64
		updatedAt      int64
	)
	if err := sc.Scan(&id, &cond.Type, &cond.Status, &reason, &message, &liveness,
		&transitionedAt, &updatedAt); err != nil {
		return 0, storeapi.Condition{}, err
	}
	cond.Reason = reason.String
	cond.Message = message.String
	cond.Liveness = liveness
	cond.TransitionedAt = fromMillis(transitionedAt)
	cond.UpdatedAt = fromMillis(updatedAt)
	return id, cond, nil
}

// livenessStale reports whether cond is a liveness condition last written before
// this process started: such a condition is only valid in the process that wrote
// it, so until a controller re-confirms it (bumping updated_at) it reads as
// "verifying". Store-truth conditions are never stale.
func (s *sqliteStore) livenessStale(cond *storeapi.Condition) bool {
	return cond.Liveness && cond.UpdatedAt.Before(s.processStart)
}

// downgradeLiveness applies the "verifying" rule on the read path: a stale
// liveness condition surfaces as Unknown. Applied only when assembling conditions
// for callers — not in getCondition, whose no-op comparison must see the actually
// stored status.
func (s *sqliteStore) downgradeLiveness(cond *storeapi.Condition) {
	if s.livenessStale(cond) {
		cond.Status = "Unknown"
	}
}

// attachConditions loads obj's conditions onto it, returning obj for chaining.
func (s *sqliteStore) attachConditions(ctx context.Context, obj *storeapi.RawObject) (*storeapi.RawObject, error) {
	conds, err := s.loadConditions(ctx, obj.ID)
	if err != nil {
		return nil, err
	}
	obj.Conditions = conds
	return obj, nil
}

// getCondition returns id's condition of type condType, or nil if absent.
func (s *sqliteStore) getCondition(ctx context.Context, id storeapi.ObjectID, condType string) (*storeapi.Condition, error) {
	row := s.conn(ctx).QueryRowContext(ctx,
		`SELECT `+conditionColumns+` FROM conditions WHERE object_id = ? AND type = ?`, id, condType)
	_, cond, err := scanCondition(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &cond, nil
}

// bumpObjectAndEmit advances id's resource_version and emits a Modified event for
// the assembled object. The condition mutators share it: a condition lives in its
// own table, so the version bump that wakes watchers can't be folded into the
// semantic write and is a separate UPDATE.
func (s *sqliteStore) bumpObjectAndEmit(ctx context.Context, c dbtx, id storeapi.ObjectID) (*storeapi.RawObject, error) {
	rv, err := nextResourceVersion(ctx, c)
	if err != nil {
		return nil, err
	}
	row := c.QueryRowContext(ctx, `
		UPDATE objects SET resource_version = ?, updated_at = ?
		WHERE id = ?
		RETURNING `+objectColumns, rv, toMillis(time.Now().UTC()), id)
	return s.scanAndEmit(ctx, storeapi.WatchEventModified, row)
}

// conditionUnchanged reports whether an existing condition already matches the
// proposed write — the no-op case that skips the write, the resource_version
// bump, and the emit.
func (s *sqliteStore) conditionUnchanged(existing *storeapi.Condition, want storeapi.Condition) bool {
	if existing == nil {
		return false
	}
	// A stale liveness condition (written by a prior process) reads as "verifying"
	// until its updated_at advances past processStart. A re-confirmation with
	// identical fields must therefore NOT be suppressed — letting the write through
	// refreshes updated_at and clears the downgrade; skipping it would leave the
	// condition pinned to Unknown forever.
	if s.livenessStale(existing) {
		return false
	}
	return existing.Status == want.Status &&
		existing.Reason == want.Reason &&
		existing.Message == want.Message &&
		existing.Liveness == want.Liveness
}

func (s *sqliteStore) SetCondition(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, cond storeapi.Condition) (*storeapi.RawObject, error) {
	// Within keeps the condition write and the object's version bump atomic: it
	// opens a transaction when called standalone and joins the caller's when
	// nested (the reconcile path), so a crash between the two statements can't
	// leave a changed condition with an unbumped resource_version.
	var result *storeapi.RawObject
	err := s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		// Scoped read confirms the object exists and belongs to gk first: yields a
		// clean ErrNotFound/ErrWrongKind rather than a foreign-key violation or a
		// cross-kind write from the conditions insert.
		obj, err := s.getObjectRowScoped(ctx, gk, id)
		if err != nil {
			return err
		}
		// No-op suppression: an identical condition carries the same resource_version,
		// so emitting would show watchers a spurious diff (mirrors RequestDeletion).
		existing, err := s.getCondition(ctx, id, cond.Type)
		if err != nil {
			return err
		}
		if s.conditionUnchanged(existing, cond) {
			result, err = s.attachConditions(ctx, obj)
			return err
		}
		now := toMillis(time.Now().UTC())
		if _, err := c.ExecContext(ctx, `
			INSERT INTO conditions
				(object_id, type, status, reason, message, liveness,
				 transitioned_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(object_id, type) DO UPDATE SET
				status = excluded.status, reason = excluded.reason,
				message = excluded.message, liveness = excluded.liveness,
				-- transitioned_at tracks when status last CHANGED: keep the prior value
				-- unless the status differs from what's stored.
				transitioned_at = CASE WHEN conditions.status <> excluded.status
					THEN excluded.transitioned_at ELSE conditions.transitioned_at END,
				updated_at = excluded.updated_at`,
			id, cond.Type, cond.Status, cond.Reason, cond.Message, cond.Liveness,
			now, now); err != nil {
			return err
		}
		// A condition change bumps the object's resource_version so watchers wake.
		result, err = s.bumpObjectAndEmit(ctx, c, id)
		return err
	})
	return result, err
}

func (s *sqliteStore) DeleteCondition(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, condType string) (*storeapi.RawObject, error) {
	// Within keeps the delete and the version bump atomic (see SetCondition).
	var result *storeapi.RawObject
	err := s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		// Scoped read enforces the kind boundary up front (symmetric with
		// SetCondition); the conditions table carries no group/kind to fold into
		// the DELETE, so the gate is the object read.
		obj, err := s.getObjectRowScoped(ctx, gk, id)
		if err != nil {
			return err
		}
		res, err := c.ExecContext(ctx,
			`DELETE FROM conditions WHERE object_id = ? AND type = ?`, id, condType)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected() // modernc caches the count; RowsAffected never errors
		// Absent condition: nothing changed, so don't bump resource_version or emit
		// — a watcher would otherwise see a spurious diff. Return the object we
		// already read, with its conditions assembled.
		if n == 0 {
			result, err = s.attachConditions(ctx, obj)
			return err
		}
		result, err = s.bumpObjectAndEmit(ctx, c, id)
		return err
	})
	return result, err
}

// eventColumns is the canonical select list for an event row; scanEvent reads
// them in order.
const eventColumns = `id, object_id, category, type, reason, message, detail,
	count, first_at, last_at, resource_version`

// scanEvent decodes one event row in eventColumns order. message is "" when
// NULL; detail is opaque JSON bytes, nil when NULL.
func scanEvent(sc scanner) (*storeapi.Event, error) {
	var e storeapi.Event
	var message sql.NullString
	var firstMs, lastMs int64
	if err := sc.Scan(&e.ID, &e.ObjectID, &e.Category, &e.Type, &e.Reason,
		&message, &e.Detail, &e.Count, &firstMs, &lastMs, &e.ResourceVersion); err != nil {
		return nil, err
	}
	e.Message = message.String
	e.FirstAt = fromMillis(firstMs)
	e.LastAt = fromMillis(lastMs)
	return &e, nil
}

// latestEventRun returns the full newest run for (id, category), or nil if that
// timeline is empty. GetLatestEvent returns it as-is; RecordEvent probes it for the
// run key (only this run is ever extended, which scopes aggregation per category).
func (s *sqliteStore) latestEventRun(ctx context.Context, id storeapi.ObjectID, category string) (*storeapi.Event, error) {
	row := s.conn(ctx).QueryRowContext(ctx,
		`SELECT `+eventColumns+` FROM events WHERE object_id = ? AND category = ?
		 ORDER BY id DESC LIMIT 1`, id, category)
	e, err := scanEvent(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (s *sqliteStore) RecordEvent(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, ev storeapi.Event) (*storeapi.Event, error) {
	// Within serializes the read-latest-then-write (via _txlock=immediate) so the
	// run-boundary decision can't race, and joins the caller's tx when nested.
	var result *storeapi.Event
	err := s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		// Scoped read enforces the kind boundary (ErrNotFound/ErrWrongKind), like
		// SetCondition — the events table carries no group/kind to fold in.
		if _, err := s.getObjectRowScoped(ctx, gk, id); err != nil {
			return err
		}
		rv, err := nextResourceVersion(ctx, c)
		if err != nil {
			return err
		}
		now := toMillis(time.Now().UTC())

		latest, err := s.latestEventRun(ctx, id, ev.Category)
		if err != nil {
			return err
		}
		var row *sql.Row
		if latest != nil && latest.Type == ev.Type && latest.Reason == ev.Reason {
			// Extend: bump count and window end, re-sample message/detail, advance rv.
			row = c.QueryRowContext(ctx, `
				UPDATE events SET count = count + 1, last_at = ?, message = ?,
					detail = ?, resource_version = ?
				WHERE id = ?
				RETURNING `+eventColumns, now, ev.Message, ev.Detail, rv, latest.ID)
		} else {
			// New run (empty timeline or key changed): count 1, point window.
			row = c.QueryRowContext(ctx, `
				INSERT INTO events
					(object_id, category, type, reason, message, detail,
					 count, first_at, last_at, resource_version)
				VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
				RETURNING `+eventColumns,
				id, ev.Category, ev.Type, ev.Reason, ev.Message, ev.Detail, now, now, rv)
		}
		result, err = scanEvent(row)
		if err != nil {
			return err
		}
		// Publish the resulting run to event-log watchers — buffered in the tx
		// collector and published after commit, like the object mutators' emit.
		s.emitEvent(ctx, gk, result)
		return nil
	})
	return result, err
}

// scanEvents decodes all rows of a query into a value slice, closing rows.
func scanEvents(rows *sql.Rows) ([]storeapi.Event, error) {
	defer rows.Close()
	var out []storeapi.Event
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

func (s *sqliteStore) ListEvents(ctx context.Context, id storeapi.ObjectID, q storeapi.EventQuery) ([]storeapi.Event, error) {
	where := []string{"object_id = ?"}
	args := []any{id}
	if q.Category != nil {
		where = append(where, "category = ?")
		args = append(args, *q.Category)
	}
	if q.Type != "" {
		where = append(where, "type = ?")
		args = append(args, q.Type)
	}
	if q.Reason != "" {
		where = append(where, "reason = ?")
		args = append(args, q.Reason)
	}
	if !q.Since.IsZero() {
		where = append(where, "last_at >= ?")
		args = append(args, toMillis(q.Since))
	}
	// Newest first; id breaks same-millisecond last_at ties deterministically.
	query := `SELECT ` + eventColumns + ` FROM events WHERE ` +
		strings.Join(where, " AND ") + ` ORDER BY last_at DESC, id DESC`
	if q.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, q.Limit)
	}
	rows, err := s.conn(ctx).QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return scanEvents(rows)
}

func (s *sqliteStore) GetLatestEvent(ctx context.Context, id storeapi.ObjectID, category string) (*storeapi.Event, error) {
	return s.latestEventRun(ctx, id, category)
}

func (s *sqliteStore) SweepEvents(ctx context.Context, perObject int, maxAge time.Duration) (int, error) {
	var total int64
	// One transaction so both bounds see the same snapshot and land together.
	err := s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		if perObject > 0 {
			// Rank each run within its (object, category) timeline newest-first and
			// drop everything past the cap — the per-timeline ring.
			res, err := c.ExecContext(ctx, `
				DELETE FROM events WHERE id IN (
					SELECT id FROM (
						SELECT id, ROW_NUMBER() OVER (
							PARTITION BY object_id, category
							ORDER BY last_at DESC, id DESC) AS rn
						FROM events
					) WHERE rn > ?
				)`, perObject)
			if err != nil {
				return err
			}
			n, _ := res.RowsAffected()
			total += n
		}
		if maxAge > 0 {
			cutoff := toMillis(time.Now().UTC().Add(-maxAge))
			res, err := c.ExecContext(ctx, `DELETE FROM events WHERE last_at < ?`, cutoff)
			if err != nil {
				return err
			}
			n, _ := res.RowsAffected()
			total += n
		}
		return nil
	})
	return int(total), err
}

func (s *sqliteStore) DeleteFinalizer(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, finalizer string) (*storeapi.RawObject, error) {
	// Within keeps the read-modify-write of the finalizer list atomic: it opens a
	// transaction standalone and joins the caller's on the reconcile path, so a
	// concurrent writer can't slip between the load and the rewrite.
	var result *storeapi.RawObject
	err := s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		// Scoped read enforces the kind boundary while loading the finalizer list.
		obj, err := s.getObjectRowScoped(ctx, gk, id)
		if err != nil {
			return err
		}
		remaining, removed := removeFinalizer(obj.Finalizers, finalizer)
		// Absent finalizer: nothing changed, so don't bump resource_version or emit
		// — a watcher would otherwise see a spurious diff (mirrors DeleteCondition).
		if !removed {
			result, err = s.attachConditions(ctx, obj)
			return err
		}
		rv, err := nextResourceVersion(ctx, c)
		if err != nil {
			return err
		}
		row := c.QueryRowContext(ctx, `
			UPDATE objects SET finalizers = ?, resource_version = ?, updated_at = ?
			WHERE id = ?
			RETURNING `+objectColumns,
			marshalFinalizers(remaining), rv, toMillis(time.Now().UTC()), id)
		result, err = s.scanAndEmit(ctx, storeapi.WatchEventModified, row)
		return err
	})
	return result, err
}

// markForDeletion stamps id's deletion clock and emits a Modified, once: the
// `IS NULL` guard makes a repeat a no-op (changed=false, ErrNotFound) so retries
// don't churn the watch cursor. extraWhere folds an extra guard into the statement
// (e.g. the kind scope). The row persists (deletion is async via finalizers), so
// the emitted object still carries its conditions, matching Get/List. Runs on the
// ambient connection — callers wrap it in Within to make rv-bump/write/emit atomic.
func (s *sqliteStore) markForDeletion(ctx context.Context, id storeapi.ObjectID, extraWhere string, extraArgs ...any) (*storeapi.RawObject, bool, error) {
	c := s.conn(ctx)
	rv, err := nextResourceVersion(ctx, c)
	if err != nil {
		return nil, false, err
	}
	now := toMillis(time.Now().UTC())
	args := append([]any{now, rv, now, id}, extraArgs...)
	row := c.QueryRowContext(ctx, `
		UPDATE objects
		SET deletion_requested_at = ?, resource_version = ?, updated_at = ?
		WHERE id = ? AND deletion_requested_at IS NULL`+extraWhere+`
		RETURNING `+objectColumns, args...)
	obj, err := s.scanAndEmit(ctx, storeapi.WatchEventModified, row)
	if err != nil {
		return nil, false, err // ErrNotFound = no transition (guard/extraWhere/missing)
	}
	return obj, true, nil
}

func (s *sqliteStore) RequestDeletion(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID) (*storeapi.RawObject, bool, error) {
	// Within keeps the rv-bump, write, and emit atomic now that callers no longer
	// always wrap RequestDeletion (mutators self-wrap; nested it joins the caller's
	// transaction — e.g. the GC cascade). The kind is folded in so a foreign id
	// matches no row.
	var result *storeapi.RawObject
	var changed bool
	err := s.Within(ctx, func(ctx context.Context) error {
		obj, ch, err := s.markForDeletion(ctx, id, ` AND "group" = ? AND kind = ?`, gk.Group, gk.Kind)
		if errors.Is(err, storeapi.ErrNotFound) {
			// Zero rows: already deleting (the no-op), another kind, or gone. The scoped
			// re-read distinguishes them and returns the current row for the no-op case.
			cur, rerr := s.getObjectRowScoped(ctx, gk, id)
			if rerr != nil {
				return rerr // ErrNotFound (gone) or ErrWrongKind
			}
			result, err = s.attachConditions(ctx, cur)
			return err
		}
		if err != nil {
			return err
		}
		result, changed = obj, ch
		return nil
	})
	return result, changed, err
}

// MarkOwnedForDeletion cascades deletion to ownerID's owned children. One indexed
// pass over the owned_by edge (idx_refs_to) reads each child's deletion state;
// markForDeletion then stamps only those not already deleting. So a re-cascade over
// an already-deleting subtree (the steady-state resync) is a lone SELECT — no
// writes, no events. It returns every owned child for requeue, deleting or not.
func (s *sqliteStore) MarkOwnedForDeletion(ctx context.Context, ownerID storeapi.ObjectID) ([]storeapi.Referrer, error) {
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT o.id, o."group", o.kind, o.deletion_requested_at
		FROM refs r JOIN objects o ON o.id = r.from_id
		WHERE r.to_id = ? AND r.relation = ?
		ORDER BY o.id`, ownerID, string(storeapi.RelationOwnedBy))
	if err != nil {
		return nil, err
	}
	type child struct {
		ref      storeapi.Referrer
		deleting bool
	}
	var children []child
	for rows.Next() {
		var ch child
		var delAt *int64
		// id/group/kind (INTEGER/TEXT NOT NULL) and deletion_requested_at (nullable
		// INTEGER -> *int64) all scan without error.
		_ = rows.Scan(&ch.ref.ID, &ch.ref.Group, &ch.ref.Kind, &delAt)
		ch.deleting = delAt != nil
		children = append(children, ch)
	}
	// rows.Err() can't report a late failure here: the modernc driver buffers the
	// whole result set on the first Next, so any query error already surfaced above.
	_ = rows.Err()
	rows.Close() // free the single-conn pool before the per-child writes below

	out := make([]storeapi.Referrer, 0, len(children))
	for _, ch := range children {
		out = append(out, ch.ref)
		if ch.deleting {
			continue // already deletion-pending: nothing to stamp
		}
		// A race could have set the flag since the SELECT; markForDeletion's guard
		// then returns ErrNotFound — benign here.
		if _, _, err := s.markForDeletion(ctx, ch.ref.ID, ""); err != nil &&
			!errors.Is(err, storeapi.ErrNotFound) {
			return nil, err
		}
	}
	return out, nil
}

func (s *sqliteStore) DeleteObject(ctx context.Context, id storeapi.ObjectID) error {
	c := s.conn(ctx)
	rv, err := nextResourceVersion(ctx, c)
	if err != nil {
		return err
	}
	// RETURNING hands back the row being removed so we can publish a Deleted
	// event for it; a zero-row delete scans to ErrNotFound, as before. The
	// object's conditions are cascade-deleted by this statement, so the Deleted
	// event carries none — the object no longer exists to assemble them from.
	row := c.QueryRowContext(ctx,
		`DELETE FROM objects WHERE id = ? RETURNING `+objectColumns, id)
	obj, err := scanObject(row)
	if err != nil {
		return err
	}
	// The row is gone, so stamp the event with a fresh resource_version: watchers
	// drop events at or below their snapshot's version, and the row's last
	// version may already sit in a snapshot, which would swallow the Deleted.
	obj.ResourceVersion = rv
	s.emit(ctx, storeapi.WatchEventDeleted, obj)
	return nil
}

// AddRef inserts a (from_id, to_id, relation) edge. It neither bumps
// resource_version nor emits — a ref is not a field of the object, so watchers
// would see no diff — and joins the ambient transaction (if any) via conn.
func (s *sqliteStore) AddRef(ctx context.Context, fromID, toID storeapi.ObjectID, relation storeapi.Relation) error {
	// Confirm both endpoints exist for a clean ErrNotFound over a raw FK
	// violation — in one round-trip, and without loading the row blobs.
	var fromOK, toOK bool
	if err := s.conn(ctx).QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM objects WHERE id = ?), EXISTS(SELECT 1 FROM objects WHERE id = ?)`,
		fromID, toID).Scan(&fromOK, &toOK); err != nil {
		return err
	}
	if !fromOK || !toOK {
		return storeapi.ErrNotFound
	}
	_, err := s.conn(ctx).ExecContext(ctx, `
		INSERT INTO refs (from_id, to_id, relation) VALUES (?, ?, ?)
		ON CONFLICT(from_id, to_id, relation) DO NOTHING`,
		fromID, toID, string(relation))
	return err
}

// DeleteRef removes a (from_id, to_id, relation) edge; an absent edge is a
// silent no-op. Like AddRef it bumps nothing and joins the ambient transaction.
func (s *sqliteStore) DeleteRef(ctx context.Context, fromID, toID storeapi.ObjectID, relation storeapi.Relation) error {
	_, err := s.conn(ctx).ExecContext(ctx,
		`DELETE FROM refs WHERE from_id = ? AND to_id = ? AND relation = ?`,
		fromID, toID, string(relation))
	return err
}

// ListIncomingRefs returns the objects pointing at toID through relation, joining refs
// to objects so each carries the GroupKind needed to route a requeue.
func (s *sqliteStore) ListIncomingRefs(ctx context.Context, toID storeapi.ObjectID, relation storeapi.Relation) ([]storeapi.Referrer, error) {
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT o.id, o."group", o.kind
		FROM refs r JOIN objects o ON o.id = r.from_id
		WHERE r.to_id = ? AND r.relation = ?
		ORDER BY o.id`, toID, string(relation))
	if err != nil {
		return nil, err
	}
	return scanReferrers(rows)
}

// GroupIncomingRefsByID resolves ListIncomingRefs for many targets at once,
// bucketed by target id — the incoming twin of GroupOutgoingRefsByID. It routes
// by r.to_id and joins the source side (r.from_id).
func (s *sqliteStore) GroupIncomingRefsByID(ctx context.Context, toIDs []storeapi.ObjectID, relation storeapi.Relation) (map[storeapi.ObjectID][]storeapi.Referrer, error) {
	return s.refsByIDs(ctx, toIDs, relation, "to_id", "from_id")
}

// refsByIDsChunkSize bounds how many ids refsByIDs binds in a single query, kept
// under SQLite's SQLITE_MAX_VARIABLE_NUMBER (32766 in modernc) with room for the
// relation parameter — otherwise a large List eager-load would fail with "too
// many SQL variables". A var, not a const, so tests can shrink it to exercise the
// multi-chunk merge without seeding tens of thousands of rows.
var refsByIDsChunkSize = 30000

// refsByIDs is the shared batched edge lookup behind GroupIncomingRefsByID and
// GroupOutgoingRefsByID: it filters refs by routeCol IN (ids), joins objects on
// the opposite endpoint joinCol, and buckets each referrer under its routeCol
// value. routeCol/joinCol are fixed internal column names (never user input), so
// concatenating them is injection-safe. The id list is chunked under the bound-
// parameter limit (see refsByIDsChunkSize); each chunk merges into the same map,
// and a routeCol value with no matching edge never appears.
func (s *sqliteStore) refsByIDs(ctx context.Context, ids []storeapi.ObjectID, relation storeapi.Relation, routeCol, joinCol string) (map[storeapi.ObjectID][]storeapi.Referrer, error) {
	out := make(map[storeapi.ObjectID][]storeapi.Referrer, len(ids))
	for start := 0; start < len(ids); start += refsByIDsChunkSize {
		end := start + refsByIDsChunkSize
		if end > len(ids) {
			end = len(ids)
		}
		if err := s.refsByIDsChunk(ctx, ids[start:end], relation, routeCol, joinCol, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// refsByIDsChunk runs refsByIDs for one chunk of ids, merging rows into out. It
// closes its result set before returning so the next chunk's query can run on the
// single-connection store (which permits one open result set at a time).
func (s *sqliteStore) refsByIDsChunk(ctx context.Context, ids []storeapi.ObjectID, relation storeapi.Relation, routeCol, joinCol string, out map[storeapi.ObjectID][]storeapi.Referrer) error {
	args := make([]any, 0, len(ids)+1)
	placeholders := make([]string, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args = append(args, id)
	}
	args = append(args, string(relation))
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT r.`+routeCol+`, o.id, o."group", o.kind
		FROM refs r JOIN objects o ON o.id = r.`+joinCol+`
		WHERE r.`+routeCol+` IN (`+strings.Join(placeholders, ",")+`) AND r.relation = ?
		ORDER BY r.`+routeCol+`, o.id`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var route storeapi.ObjectID
		var d storeapi.Referrer
		// All columns are INTEGER/TEXT NOT NULL; the scan never fails (see scanReferrers).
		_ = rows.Scan(&route, &d.ID, &d.Group, &d.Kind)
		out[route] = append(out[route], d)
	}
	return rows.Err()
}

// ListOutgoingRefs returns the distinct objects fromID points at (any relation),
// the inverse of ListIncomingRefs. DISTINCT collapses an object reached through
// more than one relation (e.g. both owned_by and depends_on) to a single row.
func (s *sqliteStore) ListOutgoingRefs(ctx context.Context, fromID storeapi.ObjectID) ([]storeapi.Referrer, error) {
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT DISTINCT o.id, o."group", o.kind
		FROM refs r JOIN objects o ON o.id = r.to_id
		WHERE r.from_id = ?
		ORDER BY o.id`, fromID)
	if err != nil {
		return nil, err
	}
	return scanReferrers(rows)
}

// ListOutgoingRefsByRelation returns the objects fromID points at through the
// given relation, ordered by id — the relation-filtered form of
// ListOutgoingRefs. No DISTINCT is needed: (from_id, to_id, relation) is unique,
// so a fixed relation can reach each target at most once.
func (s *sqliteStore) ListOutgoingRefsByRelation(ctx context.Context, fromID storeapi.ObjectID, relation storeapi.Relation) ([]storeapi.Referrer, error) {
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT o.id, o."group", o.kind
		FROM refs r JOIN objects o ON o.id = r.to_id
		WHERE r.from_id = ? AND r.relation = ?
		ORDER BY o.id`, fromID, string(relation))
	if err != nil {
		return nil, err
	}
	return scanReferrers(rows)
}

// GroupOutgoingRefsByID resolves ListOutgoingRefsByRelation for many sources at
// once, bucketed by source id. It routes by r.from_id and joins the target side
// (r.to_id).
func (s *sqliteStore) GroupOutgoingRefsByID(ctx context.Context, fromIDs []storeapi.ObjectID, relation storeapi.Relation) (map[storeapi.ObjectID][]storeapi.Referrer, error) {
	return s.refsByIDs(ctx, fromIDs, relation, "from_id", "to_id")
}

// scanReferrers collects an (id, group, kind) SELECT into Referrers, closing rows
// on return. Like scanObjects it ends in `return out, rows.Err()`: the id/group/
// kind columns are INTEGER/TEXT NOT NULL scanned into int64/string, which never
// fails, and modernc's buffered result set leaves rows.Err clean after a good
// query — so the tail error is reported in one statement, not a dead branch.
func scanReferrers(rows *sql.Rows) ([]storeapi.Referrer, error) {
	defer rows.Close()
	var out []storeapi.Referrer
	for rows.Next() {
		var d storeapi.Referrer
		// id (INTEGER) -> int64 and group/kind (TEXT NOT NULL) -> string never fail.
		_ = rows.Scan(&d.ID, &d.Group, &d.Kind)
		out = append(out, d)
	}
	return out, rows.Err()
}

// DeleteFinalizingDependsOnRefs removes depends_on edges into toID whose source
// is itself deletion-pending, breaking the deadlock where mutually dependent (or
// self-dependent) finalizing objects each hold the other's RESTRICT. Like
// DeleteRef it bumps no version and emits no event.
func (s *sqliteStore) DeleteFinalizingDependsOnRefs(ctx context.Context, toID storeapi.ObjectID) error {
	_, err := s.conn(ctx).ExecContext(ctx, `
		DELETE FROM refs
		WHERE to_id = ? AND relation = ?
		  AND from_id IN (SELECT id FROM objects WHERE deletion_requested_at IS NOT NULL)`,
		toID, string(storeapi.RelationDependsOn))
	return err
}

// HasIncomingRefs reports whether any object with a live claim points at id: an
// owned_by edge, or a depends_on edge from a source that is not itself
// finalizing. A depends_on edge from a deletion-pending source is ignored — that
// dependent is going away and no longer has a claim, so it must not gate a
// finalizer (HasIncomingRefs would otherwise never clear when two finalizing
// objects depend on each other). owned_by always counts: the foreground cascade
// must wait for the owned child to be physically removed.
func (s *sqliteStore) HasIncomingRefs(ctx context.Context, id storeapi.ObjectID) (bool, error) {
	var exists int
	err := s.conn(ctx).QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM refs r
			WHERE r.to_id = ?
			  AND NOT (r.relation = ? AND r.from_id IN
			           (SELECT id FROM objects WHERE deletion_requested_at IS NOT NULL)))`,
		id, string(storeapi.RelationDependsOn)).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists == 1, nil
}

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

func scanObject(sc scanner) (*storeapi.RawObject, error) {
	var (
		obj         storeapi.RawObject
		slug        sql.NullString
		status      []byte
		observedGen sql.NullInt64
		observedAt  sql.NullInt64
		deletionAt  sql.NullInt64
		finalizers  []byte
		createdAt   int64
		updatedAt   int64
	)
	err := sc.Scan(
		&obj.ID, &obj.Group, &obj.Kind, &slug, &obj.Spec, &status,
		&obj.SpecVersion, &obj.StatusVersion,
		&obj.Generation, &observedGen, &observedAt, &obj.ResourceVersion,
		&deletionAt, &finalizers, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, storeapi.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	if slug.Valid {
		obj.Slug = &slug.String
	}
	obj.Status = status // nil for a NULL column; bytes once a status is written
	if observedGen.Valid {
		obj.ObservedGeneration = &observedGen.Int64
	}
	if observedAt.Valid {
		obj.ObservedAt = millisPtr(observedAt.Int64)
	}
	if deletionAt.Valid {
		obj.DeletionRequestedAt = millisPtr(deletionAt.Int64)
	}
	if err := json.Unmarshal(finalizers, &obj.Finalizers); err != nil {
		return nil, err
	}
	obj.CreatedAt = fromMillis(createdAt)
	obj.UpdatedAt = fromMillis(updatedAt)
	return &obj, nil
}

// scanObjects collects every row of an objectColumns SELECT, closing rows on
// return. It ends in `return out, rows.Err()` so the post-iteration error is a
// single tail statement rather than a separate, effectively-unreachable branch
// (the modernc driver materializes the result set on the first Next, so neither
// a trailing rows.Err nor a second-row scan can fail after a clean query).
func scanObjects(rows *sql.Rows) ([]*storeapi.RawObject, error) {
	defer rows.Close()
	var out []*storeapi.RawObject
	for rows.Next() {
		obj, err := scanObject(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, obj)
	}
	return out, rows.Err()
}

// removeFinalizer returns f without target and whether target was present. A
// missing target leaves the slice untouched (removed=false), which the caller
// treats as a no-op.
func removeFinalizer(f []string, target string) (remaining []string, removed bool) {
	remaining = make([]string, 0, len(f))
	for _, x := range f {
		if x == target {
			removed = true
			continue
		}
		remaining = append(remaining, x)
	}
	return remaining, removed
}

func marshalFinalizers(f []string) []byte {
	if f == nil {
		// The column defaults to '[]'; keep the same shape on explicit insert.
		return []byte("[]")
	}
	b, _ := json.Marshal(f) // marshaling []string never errors
	return b
}

func toMillis(t time.Time) int64 { return t.UnixMilli() }

func fromMillis(ms int64) time.Time { return time.UnixMilli(ms).UTC() }

func millisPtr(ms int64) *time.Time {
	t := fromMillis(ms)
	return &t
}
