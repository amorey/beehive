package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/amorey/beehive/internal/storeapi"
)

type sqliteStore struct {
	db *sql.DB
}

func (s *sqliteStore) Close() error {
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
func (s *sqliteStore) Within(ctx context.Context, fn func(ctx context.Context) error) error {
	if _, ok := ctx.Value(txKey{}).(*sql.Tx); ok {
		return fn(ctx)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() // no-op once Commit succeeds; rolls back on any early return
	if err := fn(context.WithValue(ctx, txKey{}, tx)); err != nil {
		return err
	}
	return tx.Commit()
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

func (s *sqliteStore) CreateObject(ctx context.Context, obj *storeapi.RawObject) (*storeapi.RawObject, error) {
	finalizers, err := marshalFinalizers(obj.Finalizers)
	if err != nil {
		return nil, err
	}
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
	return scanObject(row)
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
	return scanObject(row)
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
	return scanObject(row)
}

func (s *sqliteStore) RequestDeletion(ctx context.Context, id storeapi.ObjectID) (*storeapi.RawObject, error) {
	c := s.conn(ctx)
	rv, err := nextResourceVersion(ctx, c)
	if err != nil {
		return nil, err
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
		// Zero rows means either the object is already deleting (the no-op we
		// just skipped) or the id doesn't exist. GetObject distinguishes them:
		// it returns the unchanged row, or ErrNotFound.
		return s.GetObject(ctx, id)
	}
	return obj, err
}

func (s *sqliteStore) DeleteObject(ctx context.Context, id storeapi.ObjectID) error {
	res, err := s.conn(ctx).ExecContext(ctx, `DELETE FROM objects WHERE id = ?`, id)
	if err != nil {
		return err
	}
	return requireAffected(res)
}

// requireAffected turns a zero-row update/delete into ErrNotFound.
func requireAffected(res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return storeapi.ErrNotFound
	}
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

func marshalFinalizers(f []string) ([]byte, error) {
	if f == nil {
		// The column defaults to '[]'; keep the same shape on explicit insert.
		return []byte("[]"), nil
	}
	return json.Marshal(f)
}

func toMillis(t time.Time) int64 { return t.UnixMilli() }

func fromMillis(ms int64) time.Time { return time.UnixMilli(ms).UTC() }

func millisPtr(ms int64) *time.Time {
	t := fromMillis(ms)
	return &t
}
