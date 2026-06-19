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

// scanAndEmit scans a mutator's RETURNING row and, on success, emits a watch
// event of typ for the written object. The create/update mutators share it
// since each returns the freshly written row.
func (s *sqliteStore) scanAndEmit(ctx context.Context, typ storeapi.WatchEventType, sc scanner) (*storeapi.RawObject, error) {
	obj, err := scanObject(sc)
	if err != nil {
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

func (s *sqliteStore) GetObject(ctx context.Context, id storeapi.ObjectID) (*storeapi.RawObject, error) {
	row := s.conn(ctx).QueryRowContext(ctx,
		`SELECT `+objectColumns+` FROM objects WHERE id = ?`, id)
	return scanObject(row)
}

func (s *sqliteStore) GetObjectByName(ctx context.Context, gk storeapi.GroupKind, name string) (*storeapi.RawObject, error) {
	row := s.conn(ctx).QueryRowContext(ctx,
		`SELECT `+objectColumns+` FROM objects WHERE "group" = ? AND kind = ? AND name = ?`,
		gk.Group, gk.Kind, name)
	return scanObject(row)
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
	return out, rows.Err()
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
	// event for it; a zero-row delete scans to ErrNotFound, as before.
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
