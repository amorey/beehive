package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/amorey/beehive/internal/storeapi"
	"github.com/amorey/gochan/broadcast"
)

type sqliteStore struct {
	db *sql.DB

	// processStart stamps when this store opened. Liveness conditions written by a
	// prior process (updated_at older than this) read as Unknown ("verifying")
	// until a controller re-confirms them in this process.
	processStart time.Time

	// hubs fan watch events out to subscribers, one broadcast hub per GroupKind,
	// created lazily on first use. hubMu guards the map and the closed flag.
	hubMu  sync.RWMutex
	hubs   map[storeapi.GroupKind]*broadcast.Hub[storeapi.RawWatchEvent]
	closed bool
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
		s.hubs = nil
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
const objectColumns = `id, "group", kind, name, spec, status,
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

// scanAndEmit scans a mutator's RETURNING row, assembles its conditions, and on
// success emits a watch event of typ for the written object. Every mutator that
// returns the freshly written row shares it, so a returned object — and the watch
// event — is fully assembled (conditions included) regardless of which column the
// write touched, matching Get/List.
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
			("group", kind, name, spec, status,
			 generation, resource_version, finalizers, created_at, updated_at)
		VALUES (?, ?, ?, ?, NULL, 1, ?, ?, ?, ?)
		RETURNING `+objectColumns,
		obj.Group, obj.Kind, obj.Name, obj.Spec,
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

func (s *sqliteStore) GetObject(ctx context.Context, id storeapi.ObjectID) (*storeapi.RawObject, error) {
	obj, err := s.getObjectRow(ctx, id)
	if err != nil {
		return nil, err
	}
	return s.attachConditions(ctx, obj)
}

func (s *sqliteStore) GetObjectByName(ctx context.Context, gk storeapi.GroupKind, name string) (*storeapi.RawObject, error) {
	row := s.conn(ctx).QueryRowContext(ctx,
		`SELECT `+objectColumns+` FROM objects WHERE "group" = ? AND kind = ? AND name = ?`,
		gk.Group, gk.Kind, name)
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
	defer rows.Close()

	var out []*storeapi.RawObject
	for rows.Next() {
		obj, err := scanObject(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, obj)
	}
	if err := rows.Err(); err != nil {
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
		if err := rows.Scan(&id); err != nil {
			panic(err) // INTEGER PRIMARY KEY into int64 never errors
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *sqliteStore) UpdateSpec(ctx context.Context, id storeapi.ObjectID, spec []byte) (*storeapi.RawObject, error) {
	c := s.conn(ctx)
	rv, err := nextResourceVersion(ctx, c)
	if err != nil {
		return nil, err
	}
	// A real spec change bumps generation so the convergence handshake notices.
	row := c.QueryRowContext(ctx, `
		UPDATE objects
		SET spec = ?, generation = generation + 1, resource_version = ?, updated_at = ?
		WHERE id = ?
		RETURNING `+objectColumns,
		spec, rv, toMillis(time.Now().UTC()), id)
	return s.scanAndEmit(ctx, storeapi.WatchEventModified, row)
}

func (s *sqliteStore) UpdateStatus(ctx context.Context, id storeapi.ObjectID, observedGeneration int64, status []byte) (*storeapi.RawObject, error) {
	c := s.conn(ctx)
	rv, err := nextResourceVersion(ctx, c)
	if err != nil {
		return nil, err
	}
	now := toMillis(time.Now().UTC())
	row := c.QueryRowContext(ctx, `
		UPDATE objects
		SET status = ?, observed_generation = ?, observed_at = ?,
		    resource_version = ?, updated_at = ?
		WHERE id = ?
		RETURNING `+objectColumns,
		status, observedGeneration, now, rv, now, id)
	return s.scanAndEmit(ctx, storeapi.WatchEventModified, row)
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

func (s *sqliteStore) SetCondition(ctx context.Context, id storeapi.ObjectID, cond storeapi.Condition) (*storeapi.RawObject, error) {
	// Within keeps the condition write and the object's version bump atomic: it
	// opens a transaction when called standalone and joins the caller's when
	// nested (the reconcile path), so a crash between the two statements can't
	// leave a changed condition with an unbumped resource_version.
	var result *storeapi.RawObject
	err := s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		// Confirm the object exists first: this yields a clean ErrNotFound rather
		// than a foreign-key violation from the conditions insert.
		obj, err := s.getObjectRow(ctx, id)
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

func (s *sqliteStore) DeleteCondition(ctx context.Context, id storeapi.ObjectID, condType string) (*storeapi.RawObject, error) {
	// Within keeps the delete and the version bump atomic (see SetCondition).
	var result *storeapi.RawObject
	err := s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		res, err := c.ExecContext(ctx,
			`DELETE FROM conditions WHERE object_id = ? AND type = ?`, id, condType)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		// Absent condition: nothing changed, so don't bump resource_version or emit
		// — a watcher would otherwise see a spurious diff. (The object itself may
		// also be gone, in which case GetObject reports ErrNotFound.)
		if n == 0 {
			result, err = s.GetObject(ctx, id)
			return err
		}
		result, err = s.bumpObjectAndEmit(ctx, c, id)
		return err
	})
	return result, err
}

func (s *sqliteStore) DeleteFinalizer(ctx context.Context, id storeapi.ObjectID, finalizer string) (*storeapi.RawObject, error) {
	// Within keeps the read-modify-write of the finalizer list atomic: it opens a
	// transaction standalone and joins the caller's on the reconcile path, so a
	// concurrent writer can't slip between the load and the rewrite.
	var result *storeapi.RawObject
	err := s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		obj, err := s.getObjectRow(ctx, id)
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

func (s *sqliteStore) RequestDeletion(ctx context.Context, id storeapi.ObjectID) (*storeapi.RawObject, bool, error) {
	c := s.conn(ctx)
	rv, err := nextResourceVersion(ctx, c)
	if err != nil {
		return nil, false, err
	}
	now := toMillis(time.Now().UTC())
	// Only the first request is a real change: the `IS NULL` guard stamps the
	// deletion clock and bumps resource_version exactly once, so retries and
	// requeues don't churn the watch cursor for an idempotent no-op.
	row := c.QueryRowContext(ctx, `
		UPDATE objects
		SET deletion_requested_at = ?, resource_version = ?, updated_at = ?
		WHERE id = ? AND deletion_requested_at IS NULL
		RETURNING `+objectColumns,
		now, rv, now, id)
	obj, err := scanObject(row)
	if errors.Is(err, storeapi.ErrNotFound) {
		// Zero rows: either the object is already deleting (the no-op we just
		// skipped) or the id doesn't exist. GetObject distinguishes them. No
		// event: an idempotent no-op carries the same resource_version, so a
		// watcher would otherwise see a spurious diff.
		obj, err = s.GetObject(ctx, id)
		return obj, false, err
	}
	if err != nil {
		return nil, false, err
	}
	// The row persists (deletion is async via finalizers), so it still has its
	// conditions: assemble them so the result and event match Get/List.
	if _, err := s.attachConditions(ctx, obj); err != nil {
		return nil, false, err
	}
	s.emit(ctx, storeapi.WatchEventModified, obj)
	return obj, true, nil
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
// would see no diff — and joins the ambient reconcile transaction via conn.
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

// ListReferrers returns the objects pointing at toID through relation, joining refs
// to objects so each carries the GroupKind needed to route a requeue.
func (s *sqliteStore) ListReferrers(ctx context.Context, toID storeapi.ObjectID, relation storeapi.Relation) ([]storeapi.Referrer, error) {
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT o.id, o."group", o.kind
		FROM refs r JOIN objects o ON o.id = r.from_id
		WHERE r.to_id = ? AND r.relation = ?
		ORDER BY o.id`, toID, string(relation))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []storeapi.Referrer
	for rows.Next() {
		var d storeapi.Referrer
		if err := rows.Scan(&d.ID, &d.Group, &d.Kind); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// ListReferents returns the distinct objects fromID points at (any relation),
// the inverse of ListReferrers. DISTINCT collapses an object reached through
// more than one relation (e.g. both owned_by and depends_on) to a single row.
func (s *sqliteStore) ListReferents(ctx context.Context, fromID storeapi.ObjectID) ([]storeapi.Referrer, error) {
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT DISTINCT o.id, o."group", o.kind
		FROM refs r JOIN objects o ON o.id = r.to_id
		WHERE r.from_id = ?
		ORDER BY o.id`, fromID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []storeapi.Referrer
	for rows.Next() {
		var d storeapi.Referrer
		if err := rows.Scan(&d.ID, &d.Group, &d.Kind); err != nil {
			return nil, err
		}
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

// HasReferrers reports whether any object with a live claim points at id: an
// owned_by edge, or a depends_on edge from a source that is not itself
// finalizing. A depends_on edge from a deletion-pending source is ignored — that
// dependent is going away and no longer has a claim, so it must not gate a
// finalizer (HasReferrers would otherwise never clear when two finalizing
// objects depend on each other). owned_by always counts: the foreground cascade
// must wait for the owned child to be physically removed.
func (s *sqliteStore) HasReferrers(ctx context.Context, id storeapi.ObjectID) (bool, error) {
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
		name        sql.NullString
		status      []byte
		observedGen sql.NullInt64
		observedAt  sql.NullInt64
		deletionAt  sql.NullInt64
		finalizers  []byte
		createdAt   int64
		updatedAt   int64
	)
	err := sc.Scan(
		&obj.ID, &obj.Group, &obj.Kind, &name, &obj.Spec, &status,
		&obj.Generation, &observedGen, &observedAt, &obj.ResourceVersion,
		&deletionAt, &finalizers, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, storeapi.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	if name.Valid {
		obj.Name = &name.String
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
	b, err := json.Marshal(f)
	if err != nil {
		panic(err) // json.Marshal([]string) never errors
	}
	return b
}

func toMillis(t time.Time) int64 { return t.UnixMilli() }

func fromMillis(ms int64) time.Time { return time.UnixMilli(ms).UTC() }

func millisPtr(ms int64) *time.Time {
	t := fromMillis(ms)
	return &t
}
