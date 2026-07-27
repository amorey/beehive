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

	"github.com/amorey/beehive/internal/storeapi"
	"github.com/amorey/gobus/conflate"
)

type sqliteStore struct {
	db *sql.DB

	// processStart stamps when this store opened. Liveness conditions written by a
	// prior process (updated_at older than this) read as Unknown ("verifying")
	// until a controller re-confirms them in this process.
	processStart time.Time

	// hubs fan watch events out to subscribers, one conflating hub per GroupKind,
	// created lazily on first use. hubMu guards the maps, writeHub and the
	// closed flag.
	hubMu sync.RWMutex
	hubs  map[storeapi.GroupKind]*conflate.Hub[storeapi.ObjectID, storeapi.RawObjectChange]
	// writeHub is the store-wide twin of hubs: every object change, of every kind,
	// keyed by the same globally unique ObjectID, carrying the projection rather
	// than the row (see writeSignal). Created eagerly in open — there is exactly one
	// and no key to look it up by, so making it lazy would cost the publish path
	// (which runs on every object write) a second lock and map lookup.
	writeHub *conflate.Hub[storeapi.ObjectID, writeSignal]
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

	// beforeLiveSend, if non-nil, runs after a watcher's goroutine has taken a
	// live value off its receiver and decided to deliver it, but before it parks
	// on the send. Tests cancel from here to reach the send's ctx.Done arm: the
	// receiver ranks cancellation above a pending value, so cancelling from
	// outside wakes the receive instead and never reaches the send; nil in
	// production.
	beforeLiveSend func()
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
		s.writeHub.Close()
		s.hubs = nil
		s.eventHubs = nil
	}
	s.hubMu.Unlock()
	return s.db.Close()
}

// txKey carries the in-flight transaction through the context so that Store
// calls made with the ctx passed to Within join it.
type txKey struct{}

// txState is everything a transaction puts on the context. The connection and
// the buffer travel as one value on purpose: "am I inside a transaction?" and
// "where do I buffer until it commits?" are the same question, and answering
// them from two independent context keys would let a future path install one
// without the other — a ctx that looks transactional to conn but standalone to
// AfterCommit would fire wakes mid-transaction, the exact race the buffering
// exists to prevent.
type txState struct {
	tx   *sql.Tx
	coll *eventCollector
}

// txFrom returns the ambient transaction state, if any.
func txFrom(ctx context.Context) (*txState, bool) {
	st, ok := ctx.Value(txKey{}).(*txState)
	return st, ok
}

// dbtx is the subset of *sql.DB and *sql.Tx the object queries use, so the same
// code path runs both standalone and inside a Within transaction.
type dbtx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// conn returns the ambient transaction if ctx carries one, else the pool.
func (s *sqliteStore) conn(ctx context.Context) dbtx {
	if st, ok := txFrom(ctx); ok {
		return st.tx
	}
	return s.db
}

// Within runs fn inside a single transaction. A nested Within (ctx already
// carries a tx) joins the outer transaction rather than opening a new one.
//
// Read-modify-write atomicity rests on the DSN's _txlock=immediate: BeginTx
// issues BEGIN IMMEDIATE, so a transaction holds the sole WAL write lock from
// BEGIN through Commit, before its first read. No other writer can commit in
// between, so a compare-then-write (ObjectsUpdateSpec's no-op suppression, SetCondition,
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
	if _, ok := txFrom(ctx); ok {
		return fn(ctx) // nested: joins the outer tx and its collector
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	coll := &eventCollector{}
	ctx = context.WithValue(ctx, txKey{}, &txState{tx: tx, coll: coll})
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

// AfterCommit defers fn to the outermost transaction's post-commit flush, right
// after that transaction's watch events go out. A nested Within joins the outer
// collector, so a hook registered deep inside a controller's Within still waits
// for the outer commit — which is the point: a wake published before the commit
// can send a reconciler at a row that isn't visible (or never lands at all).
//
// Outside a transaction there is nothing to wait for — the write has committed
// and emitted already — so fn runs inline on the caller's own ctx. So does a
// registration that arrives too late to be buffered: a hook holding the tx ctx it
// was registered on (instead of the detached one it is handed) can call back in
// here after flush has drained the collector, and "run after the commit" is
// satisfied by running now, not by queueing where nothing will look again.
func (s *sqliteStore) AfterCommit(ctx context.Context, fn func(context.Context)) {
	st, ok := txFrom(ctx)
	if !ok {
		fn(ctx) // nothing to defer to, and nothing to strip
		return
	}
	// Strip the transaction before handing the ctx on: by the time the hook runs
	// that *sql.Tx is committed, so a store call joining it would fail outright.
	// A hook that writes gets a fresh transaction, which is the only thing it could
	// have meant. Everything else on the ctx is inherited.
	hookCtx := context.WithValue(ctx, txKey{}, nil)
	if st.coll.addHook(func() { fn(hookCtx) }) {
		return
	}
	fn(hookCtx)
}

// objectColumns is the canonical select list; scanObject reads them in order.
const objectColumns = `id, "group", kind, slug, spec, status,
	schema_version_spec, schema_version_status,
	generation, observed_generation, observed_at, resource_version,
	deletion_requested_at, pending_wake, finalizers, created_at, updated_at`

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
func (s *sqliteStore) scanAndEmit(ctx context.Context, typ storeapi.ChangeType, sc scanner) (*storeapi.RawObject, error) {
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

func (s *sqliteStore) ObjectsCreate(ctx context.Context, obj *storeapi.RawObject) (*storeapi.RawObject, error) {
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
		obj.Group, obj.Kind, obj.Slug, jsonText(obj.Spec), obj.SpecVersion,
		rv, jsonText(finalizers), now, now)
	return s.scanAndEmit(ctx, storeapi.Added, row)
}

// getObjectRow reads the objects row without assembling conditions. Internal
// callers that don't need the conditions (existence checks, pre-write reads) use
// it to avoid the extra per-object conditions query ObjectsGet would run.
func (s *sqliteStore) getObjectRow(ctx context.Context, id storeapi.ObjectID) (*storeapi.RawObject, error) {
	row := s.conn(ctx).QueryRowContext(ctx,
		`SELECT `+objectColumns+` FROM objects WHERE id = ?`, id)
	return scanObject(row)
}

// getObjectRowScoped loads id's bare row (no conditions) and confirms it belongs
// to gk. Returns ErrNotFound if the row is gone, ErrWrongKind if it names another
// kind. It replaces the full ObjectsGet + Go-side compare the client/controller
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

func (s *sqliteStore) ObjectsGet(ctx context.Context, id storeapi.ObjectID) (*storeapi.RawObject, error) {
	obj, err := s.getObjectRow(ctx, id)
	if err != nil {
		return nil, err
	}
	return s.attachConditions(ctx, obj)
}

// ObjectsGetMeta is getObjectRow exposed across the store boundary: id's row with
// no conditions assembled, for metadata-only callers (GC collect, ref checks).
func (s *sqliteStore) ObjectsGetMeta(ctx context.Context, id storeapi.ObjectID) (*storeapi.RawObject, error) {
	return s.getObjectRow(ctx, id)
}

// getObjectRowBySlug is getObjectRow keyed by slug within gk: the bare row, no
// conditions assembled. The slug-keyed sibling of getObjectRowScoped — no
// ErrWrongKind, since the kind is in the WHERE rather than checked after the read.
func (s *sqliteStore) getObjectRowBySlug(ctx context.Context, gk storeapi.GroupKind, slug string) (*storeapi.RawObject, error) {
	row := s.conn(ctx).QueryRowContext(ctx,
		`SELECT `+objectColumns+` FROM objects WHERE "group" = ? AND kind = ? AND slug = ?`,
		gk.Group, gk.Kind, slug)
	return scanObject(row)
}

func (s *sqliteStore) ObjectsGetBySlug(ctx context.Context, gk storeapi.GroupKind, slug string) (*storeapi.RawObject, error) {
	obj, err := s.getObjectRowBySlug(ctx, gk, slug)
	if err != nil {
		return nil, err
	}
	return s.attachConditions(ctx, obj)
}

func (s *sqliteStore) ObjectsList(ctx context.Context, gk storeapi.GroupKind) ([]*storeapi.RawObject, error) {
	return s.listObjectsWhere(ctx, `WHERE o."group" = ? AND o.kind = ?`, gk.Group, gk.Kind)
}

// ObjectsListByIncomingRef returns the full rows of the objects pointing at toID
// through relation, restricted to kind gk — the blob-bearing form of
// RefsListIncoming (which returns bare id/GroupKind referrers). Resolving the
// edges in the statement is what saves the caller a Get per child.
//
// The edge is a semi-join, not a join: written as a join the planner drives from
// idx_objects_kind (which already delivers ORDER BY o.id) and probes refs once
// per object *of the kind*. IN (SELECT …) lets idx_refs_to drive instead, so the
// work scales with the owner's children rather than the whole table.
func (s *sqliteStore) ObjectsListByIncomingRef(ctx context.Context, gk storeapi.GroupKind, toID storeapi.ObjectID, relation storeapi.Relation) ([]*storeapi.RawObject, error) {
	return s.listObjectsWhere(ctx, `
		WHERE o.id IN (SELECT from_id FROM refs WHERE to_id = ? AND relation = ?)
		  AND o."group" = ? AND o.kind = ?`,
		toID, string(relation), gk.Group, gk.Kind)
}

// listObjectsWhere is the shared multi-row object read: the rows matching tail,
// ordered by id, each with its conditions attached. The predicate runs once, in
// the blob-bearing SELECT; the conditions are then fetched by the ids it
// returned (see conditionsByIDs). tail is a fixed internal fragment, never user
// input, so concatenating it is injection-safe; only its bound arguments come
// from the caller.
func (s *sqliteStore) listObjectsWhere(ctx context.Context, tail string, args ...any) ([]*storeapi.RawObject, error) {
	rows, err := s.conn(ctx).QueryContext(ctx,
		`SELECT `+objectColumns+` FROM objects o `+tail+` ORDER BY o.id`, args...)
	if err != nil {
		return nil, err
	}
	// scanObjects closes rows on return, which is what frees the single-connection
	// pool for the conditions query below — no explicit Close needed here.
	out, err := scanObjects(rows)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		// Nothing to attach to. conditionsByIDs is already a no-op on an empty id
		// list (it binds no query at all), so this only makes the skip obvious.
		return out, nil
	}

	// One batched query for the whole result set avoids an N+1 per-object lookup.
	ids := make([]storeapi.ObjectID, len(out))
	for i, obj := range out {
		ids[i] = obj.ID
	}
	byID, err := s.conditionsByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	for _, obj := range out {
		obj.Conditions = byID[obj.ID]
	}
	return out, nil
}

// conditionsByIDs returns the conditions of the given objects, grouped by object
// id and ordered by type within each object — the conditions half of
// listObjectsWhere. Kept a separate query because the two can't be one:
// conditions are a per-object fan-out, and folding them into the blob-bearing
// SELECT would re-send each row's spec/status per condition.
//
// It keys off the ids already scanned rather than re-running the object
// predicate. The two statements are not in one transaction, so a re-run could
// match a different set — a concurrent ref or object write between them would
// silently drop the conditions of a row we are about to return. Keying off the
// ids also avoids paying the predicate (a refs semi-join, for
// ObjectsListByIncomingRef) twice. The list is chunked under the bound-parameter
// limit (see idChunkSize).
func (s *sqliteStore) conditionsByIDs(ctx context.Context, ids []storeapi.ObjectID) (map[storeapi.ObjectID][]storeapi.Condition, error) {
	byID := make(map[storeapi.ObjectID][]storeapi.Condition, len(ids))
	for start := 0; start < len(ids); start += idChunkSize {
		if err := s.conditionsByIDsChunk(ctx, ids[start:min(start+idChunkSize, len(ids))], byID); err != nil {
			return nil, err
		}
	}
	return byID, nil
}

// conditionsByIDsChunk runs conditionsByIDs for one chunk of ids, merging rows
// into out. It closes its result set before returning so the next chunk's query
// can run on the single-connection store.
func (s *sqliteStore) conditionsByIDsChunk(ctx context.Context, ids []storeapi.ObjectID, out map[storeapi.ObjectID][]storeapi.Condition) error {
	args := make([]any, len(ids))
	placeholders := make([]string, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	rows, err := s.conn(ctx).QueryContext(ctx,
		`SELECT `+conditionColumns+` FROM conditions
		 WHERE object_id IN (`+strings.Join(placeholders, ",")+`)
		 ORDER BY object_id, type`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		id, cond, err := scanCondition(rows)
		if err != nil {
			return err
		}
		s.downgradeLiveness(&cond)
		out[id] = append(out[id], cond)
	}
	return rows.Err()
}

func (s *sqliteStore) ObjectsListUnsettledIDs(ctx context.Context, gk storeapi.GroupKind) ([]storeapi.ObjectID, error) {
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

func (s *sqliteStore) DeletionsListPending(ctx context.Context) ([]storeapi.Referrer, error) {
	// Kind-agnostic: no group/kind filter, so the global GC sweeper sees every
	// finalizing object. The kind rides along because the sweeper routes on it
	// (see the Store doc), and idx_objects_deleting is keyed to match — partial on
	// deletion_requested_at IS NOT NULL, over (id, "group", kind) — so this is a
	// covering scan already in id order: no row fetch, no sort. Keep the column
	// list and the index key in step; dropping either group/kind or the ORDER BY
	// out of alignment with it silently costs a table fetch or a temp B-tree.
	rows, err := s.conn(ctx).QueryContext(ctx,
		`SELECT id, "group", kind FROM objects
		 WHERE deletion_requested_at IS NOT NULL ORDER BY id`)
	if err != nil {
		return nil, err
	}
	return scanReferrers(rows)
}

func (s *sqliteStore) WakesListPendingIDs(ctx context.Context, gk storeapi.GroupKind) ([]storeapi.ObjectID, error) {
	// Matches the partial index idx_objects_pending_wake WHERE pending_wake != 0.
	rows, err := s.conn(ctx).QueryContext(ctx,
		`SELECT id FROM objects
		 WHERE "group" = ? AND kind = ? AND pending_wake != 0
		 ORDER BY id`,
		gk.Group, gk.Kind)
	if err != nil {
		return nil, err
	}
	return scanIDs(rows)
}

// WakesIncrement and WakesDecrement are single no-emit UPDATEs on the
// owed-wake count. The decrement's contract (cross-kind, no resource_version bump,
// why it takes the observed count) is on storeapi.Store; the subtraction floors at
// 0 with max() so it can never drive the count negative.
//
// The increment is deliberately *not* on that interface: production wakes are
// produced by RefsAdd, whose stamp must be indivisible from the edge insert, so the
// declare path cannot route through a separate call and no other producer exists
// yet. It stays here as the standalone form — reachable on the concrete store, so
// tests can seed a count without staging the whole declare race — and is where a
// future non-edge producer (see the dependency-waker item in TODO.md) would hook in.
func (s *sqliteStore) WakesIncrement(ctx context.Context, id storeapi.ObjectID) error {
	_, err := s.conn(ctx).ExecContext(ctx,
		`UPDATE objects SET pending_wake = pending_wake + 1 WHERE id = ?`, id)
	return err
}

func (s *sqliteStore) WakesDecrement(ctx context.Context, id storeapi.ObjectID, observed int64) error {
	_, err := s.conn(ctx).ExecContext(ctx,
		`UPDATE objects SET pending_wake = max(pending_wake - ?, 0) WHERE id = ?`, observed, id)
	return err
}

func (s *sqliteStore) ObjectsListIDs(ctx context.Context, gk storeapi.GroupKind) ([]storeapi.ObjectID, error) {
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

// stampVersion resolves the schema version a write should leave on the row, given
// what's stored and what the caller reports. It is the write-side twin of
// convertBlob and applies to *both* branches of ObjectsUpdateSpec/UpdateStatus — the
// content no-op and the real content write — because the tag records the shape the
// bytes are in, and a wrong tag corrupts every later read the same way regardless
// of whether the bytes moved.
//
// An incoming 0 means "no opinion": the kind isn't versioned, or this build lost
// the migrator. convertBlob reads that as identity rather than converting, so the
// write must leave the stored tag alone rather than zero it — a build that can't
// interpret the version has no business relabelling data as unversioned, and
// stamping 0 over a v3 row makes a later re-registered migrator convert v3 bytes
// from 0. Below the stored version (non-zero) is a genuine downgrade: refuse it,
// exactly as the read path refuses from > current. Such a caller could not have
// decoded the row it is writing back, so this is a bug worth surfacing, not a
// case to clamp silently.
func stampVersion(stored, incoming int) (int, error) {
	switch {
	case incoming == 0:
		return stored, nil
	case incoming < stored:
		return 0, fmt.Errorf("%w: stored %d, this build's %d",
			storeapi.ErrSchemaVersionDowngrade, stored, incoming)
	default:
		return incoming, nil
	}
}

func (s *sqliteStore) ObjectsUpdateSpec(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, spec []byte, specVersion int) (*storeapi.RawObject, bool, error) {
	// Within keeps the read-compare-write atomic so a concurrent writer can't slip
	// between the no-op check and the update.
	var result *storeapi.RawObject
	var changed bool
	err := s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		// Scoped read enforces the kind boundary (ErrWrongKind for a foreign id)
		// while doubling as the no-op compare's load — no separate kind check.
		obj, err := s.getObjectRowScoped(ctx, gk, id)
		if err != nil {
			return err
		}
		// Never downward — see stampVersion. A build that can't read this row's
		// version can't be trusted to retag it either.
		stamp, err := stampVersion(obj.SpecVersion, specVersion)
		if err != nil {
			return err
		}
		// Identical spec *at the same schema version*: nothing changed, so don't
		// bump generation/resource_version or emit. A bump would falsely unsettle a
		// converged object and trigger a needless reconcile, and the event would show
		// watchers a spurious diff (mirrors DeletionsRequest's idempotent no-op).
		//
		// The version gate is what makes the byte compare meaningful. Convert-on-read
		// leaves old rows tagged at the version they were written in, so a caller at a
		// newer version hands us bytes in a *different shape* — equal bytes there can
		// carry different values (a converter reading v1's absent field as a default
		// the v2 shape spells explicitly). Suppressing that as a no-op would change
		// what every later read decodes while reporting changed=false, bumping no
		// resource_version and emitting nothing, so no watcher learns and the client
		// skips the controller wake. When the shapes disagree we can't compare, so we
		// fall through and write it as the real change it may be — generation bump
		// included. That is the point, not a side effect: bumping resource_version
		// while leaving generation would re-stamp a row that stays settled
		// (ObservedGeneration == Generation), so neither the reconciler nor
		// ObjectsListUnsettledIDs would ever re-derive status against the new shape. Worst
		// case a re-stamp costs one generation bump and one spurious reconcile per
		// row per version bump — at most once per row, since the next write compares
		// equal — which the level-triggered loop absorbs.
		if stamp == obj.SpecVersion && bytes.Equal(obj.Spec, spec) {
			result, err = s.attachConditions(ctx, obj)
			return err
		}
		rv, err := nextResourceVersion(ctx, c)
		if err != nil {
			return err
		}
		// A real spec change bumps generation so the convergence handshake notices.
		// Keyed on id alone: the kind boundary came from the scoped read above, in
		// this same transaction, and group/kind are write-once at insert. Keep the
		// read if you move this statement.
		row := c.QueryRowContext(ctx, `
			UPDATE objects
			SET spec = ?, schema_version_spec = ?, generation = generation + 1,
			    resource_version = ?, updated_at = ?
			WHERE id = ?
			RETURNING `+objectColumns,
			jsonText(spec), stamp, rv, toMillis(time.Now().UTC()), id)
		result, err = s.scanAndEmit(ctx, storeapi.Modified, row)
		changed = err == nil
		return err
	})
	return result, changed, err
}

// ObjectsUpdateStatus skips the status write when the incoming bytes equal the stored
// ones, mirroring ObjectsUpdateSpec/DeleteFinalizer/DeleteCondition: no resource_version
// bump, no updated_at touch, no Modified event — a watcher would otherwise see a
// spurious diff, and downstream controllers that wake dependents off a status
// Modified would reconcile for nothing on every unchanged health poll.
//
// The convergence handshake is the one thing a content no-op must still carry:
// observed_generation/observed_at record *that the controller ran*, not what it
// wrote, and ObjectsListUnsettledIDs (the resync backstop) keys off observed_generation
// < generation. Leaving it behind on an identical-status reconcile would strand
// the object unsettled forever, re-enqueued every resync. And that advance is a
// real transition — the object just settled at a new generation — so it bumps
// resource_version and emits Modified even though the bytes didn't move: anything
// gating on ObservedGeneration == Generation would otherwise wait for the next
// resync to learn the object converged. It can't spin a controller re-applying
// its own status, because it fires at most once per generation; the repeat poll
// takes the already-settled path below.
//
// The no-op is gated on the schema version matching, not just the bytes: bytes
// written in a different shape aren't comparable, so a caller at a newer version
// takes the content path even when the bytes look identical. Identical status at
// the same version, with the generation already recorded, writes nothing at all.
func (s *sqliteStore) ObjectsUpdateStatus(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, observedGeneration int64, status []byte, statusVersion int) (*storeapi.RawObject, error) {
	var result *storeapi.RawObject
	// Within keeps the read-compare-write atomic so a concurrent writer can't slip
	// between the no-op check and the update.
	err := s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		// Scoped read enforces the kind boundary (ErrWrongKind for a foreign id)
		// while doubling as the no-op compare's load — no separate kind check.
		obj, err := s.getObjectRowScoped(ctx, gk, id)
		if err != nil {
			return err
		}
		// Was the WHERE generation >= ? guard: a controller can only have observed a
		// generation that exists, and recording a future one would falsely settle
		// the object once its spec caught up. An older value is fine — the normal
		// case where the spec changed mid-reconcile. The guard fires on the no-op
		// path too; a stale-ahead observedGeneration is a caller bug either way.
		if obj.Generation < observedGeneration {
			return fmt.Errorf("%w: reported %d, current is %d (object %d)",
				storeapi.ErrObservedGenerationFuture, observedGeneration, obj.Generation, id)
		}
		// Never downward — see stampVersion. A build that can't read this row's
		// version can't be trusted to retag it either.
		stamp, err := stampVersion(obj.StatusVersion, statusVersion)
		if err != nil {
			return err
		}
		if stamp == obj.StatusVersion && bytes.Equal(obj.Status, status) {
			// Content no-op: write only the bookkeeping, and only if it would
			// actually move.
			//
			// The version gate is what makes the byte compare meaningful — see
			// ObjectsUpdateSpec for the argument. A caller at a newer schema version holds
			// bytes in a different shape, so equal bytes aren't equal values; that
			// case falls through to the content write below, which stamps, bumps
			// resource_version and emits. It writes the reported generation verbatim
			// like any content write: there is something to relay, so the settled
			// clamp below doesn't apply.
			//
			// >=, not ==: with no content to write, a report at or below the recorded
			// generation is nothing new to record. Treating a stale one as an advance
			// would roll observed_generation backwards, re-unsettling a converged
			// object for ObjectsListUnsettledIDs and emitting a Modified that wakes every
			// dependent — all to relay strictly less than what's already stored. The
			// changed-status path below deliberately does *not* clamp: there the
			// stale reporter overwrote the status content, so unsettling the object
			// is what gets that content re-derived. Identical bytes means there is
			// nothing to heal, which is what makes suppressing it free here.
			settled := obj.ObservedGeneration != nil && *obj.ObservedGeneration >= observedGeneration
			if settled {
				result, err = s.attachConditions(ctx, obj)
				return err
			}
			// The handshake advanced: the object settled at a generation it hadn't
			// settled at before. That's watch-visible even with identical bytes.
			// updated_at still tracks content and stays put — observed_at is what
			// records the handshake. schema_version_status isn't written: this branch
			// only runs when the stamp already equals the stored version, so
			// re-stamping happens on the content path below, never here.
			rv, err := nextResourceVersion(ctx, c)
			if err != nil {
				return err
			}
			row := c.QueryRowContext(ctx, `
				UPDATE objects
				SET observed_generation = ?, observed_at = ?, resource_version = ?
				WHERE id = ?
				RETURNING `+objectColumns,
				observedGeneration, toMillis(time.Now().UTC()), rv, id)
			result, err = s.scanAndEmit(ctx, storeapi.Modified, row)
			return err
		}
		rv, err := nextResourceVersion(ctx, c)
		if err != nil {
			return err
		}
		now := toMillis(time.Now().UTC())
		// observedGeneration is written verbatim, unclamped — deliberately, unlike
		// the no-op path above. A reporter behind the recorded generation just
		// overwrote the status with content derived from an older spec, and letting
		// its generation land is what marks the object unsettled so the resync
		// backstop re-derives that content. Clamping here would pin stale status as
		// converged, and nothing would ever revisit it.
		//
		// Keyed on id alone, like ObjectsUpdateSpec's: the kind boundary and the
		// generation guard both came from the scoped read above, in this same
		// transaction, and group/kind are write-once at insert. Keep the read if you
		// move this statement.
		row := c.QueryRowContext(ctx, `
			UPDATE objects
			SET status = ?, schema_version_status = ?, observed_generation = ?, observed_at = ?,
			    resource_version = ?, updated_at = ?
			WHERE id = ?
			RETURNING `+objectColumns,
			jsonText(status), stamp, observedGeneration, now, rv, now, id)
		result, err = s.scanAndEmit(ctx, storeapi.Modified, row)
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
	return s.scanAndEmit(ctx, storeapi.Modified, row)
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

func (s *sqliteStore) ConditionsSet(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, cond storeapi.Condition) (*storeapi.RawObject, error) {
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
		// so emitting would show watchers a spurious diff (mirrors DeletionsRequest).
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

func (s *sqliteStore) ConditionsDelete(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, condType string) (*storeapi.RawObject, error) {
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
// timeline is empty. GetLatestEvent returns it as-is.
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

// latestEventKey returns just the run key (id, type, reason) of the newest run
// for (id, category), or ok=false if that timeline is empty. RecordEvent needs
// only the key to decide extend-vs-append, so it deliberately does not decode the
// full row (unlike GetLatestEvent): probing the columns it is about to discard
// would let a decode fault in an older run mask — rather than surface through —
// the write RecordEvent is about to make.
func (s *sqliteStore) latestEventKey(ctx context.Context, id storeapi.ObjectID, category string) (evID storeapi.EventID, typ, reason string, ok bool, err error) {
	row := s.conn(ctx).QueryRowContext(ctx,
		`SELECT id, type, reason FROM events WHERE object_id = ? AND category = ?
		 ORDER BY id DESC LIMIT 1`, id, category)
	err = row.Scan(&evID, &typ, &reason)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", "", false, nil
	}
	if err != nil {
		return 0, "", "", false, err
	}
	return evID, typ, reason, true, nil
}

func (s *sqliteStore) EventsRecord(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, ev storeapi.Event) (*storeapi.Event, error) {
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

		latestID, latestType, latestReason, hasLatest, err := s.latestEventKey(ctx, id, ev.Category)
		if err != nil {
			return err
		}
		var row *sql.Row
		if hasLatest && latestType == ev.Type && latestReason == ev.Reason {
			// Extend: bump count and window end, re-sample message/detail, advance rv.
			row = c.QueryRowContext(ctx, `
				UPDATE events SET count = count + 1, last_at = ?, message = ?,
					detail = ?, resource_version = ?
				WHERE id = ?
				RETURNING `+eventColumns, now, ev.Message, jsonText(ev.Detail), rv, latestID)
		} else {
			// New run (empty timeline or key changed): count 1, point window.
			row = c.QueryRowContext(ctx, `
				INSERT INTO events
					(object_id, category, type, reason, message, detail,
					 count, first_at, last_at, resource_version)
				VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
				RETURNING `+eventColumns,
				id, ev.Category, ev.Type, ev.Reason, ev.Message, jsonText(ev.Detail), now, now, rv)
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

func (s *sqliteStore) EventsList(ctx context.Context, id storeapi.ObjectID, q storeapi.EventQuery) ([]storeapi.Event, error) {
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

func (s *sqliteStore) EventsGetLatest(ctx context.Context, id storeapi.ObjectID, category string) (*storeapi.Event, error) {
	return s.latestEventRun(ctx, id, category)
}

func (s *sqliteStore) EventsSweep(ctx context.Context, perObject int, maxAge time.Duration) (int, error) {
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

func (s *sqliteStore) FinalizersDelete(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, finalizer string) (*storeapi.RawObject, error) {
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
			jsonText(marshalFinalizers(remaining)), rv, toMillis(time.Now().UTC()), id)
		result, err = s.scanAndEmit(ctx, storeapi.Modified, row)
		return err
	})
	return result, err
}

// markForDeletion stamps the deletion clock of the row named by key and emits a
// Modified, once: the `IS NULL` guard makes a repeat a no-op (changed=false,
// ErrNotFound) so retries don't churn the watch cursor. where is the caller's whole
// row predicate, keying and scope together — `id = ?` plus a kind scope, or the
// group/kind/slug triple — so a new keying is a call-site change rather than a
// second copy of this statement. It is parenthesized before the guard is appended,
// so a disjunctive key can't bind loosely enough to escape the IS NULL and re-stamp
// an already-deleting row. Like refsByIDs' column names it is a compile-time
// fragment, never user input; only whereArgs carry values. The row persists
// (deletion is async via finalizers), so the emitted object still carries its
// conditions, matching Get/List. Runs on the ambient connection — callers wrap it
// in Within to make rv-bump/write/emit atomic.
func (s *sqliteStore) markForDeletion(ctx context.Context, where string, whereArgs ...any) (*storeapi.RawObject, bool, error) {
	c := s.conn(ctx)
	rv, err := nextResourceVersion(ctx, c)
	if err != nil {
		return nil, false, err
	}
	now := toMillis(time.Now().UTC())
	args := append([]any{now, rv, now}, whereArgs...)
	row := c.QueryRowContext(ctx, `
		UPDATE objects
		SET deletion_requested_at = ?, resource_version = ?, updated_at = ?
		WHERE (`+where+`) AND deletion_requested_at IS NULL
		RETURNING `+objectColumns, args...)
	obj, err := s.scanAndEmit(ctx, storeapi.Modified, row)
	if err != nil {
		return nil, false, err // ErrNotFound = no transition (guard/where/missing)
	}
	return obj, true, nil
}

// requestDeletion is the mark-or-reread protocol behind both deletion entry points.
// markForDeletion's ErrNotFound is ambiguous — already deleting, out of scope, or
// gone — so reread resolves it on the caller's own key and supplies the current row
// for the no-op case, which callers still need in order to advance GC. Within keeps
// the rv-bump, write, and emit atomic now that callers no longer always wrap these
// (mutators self-wrap; nested they join the caller's transaction — e.g. the GC
// cascade).
func (s *sqliteStore) requestDeletion(
	ctx context.Context,
	reread func(context.Context) (*storeapi.RawObject, error),
	where string, whereArgs ...any,
) (*storeapi.RawObject, bool, error) {
	var result *storeapi.RawObject
	var changed bool
	err := s.Within(ctx, func(ctx context.Context) error {
		obj, ch, err := s.markForDeletion(ctx, where, whereArgs...)
		if errors.Is(err, storeapi.ErrNotFound) {
			cur, rerr := reread(ctx)
			if rerr != nil {
				return rerr
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

// DeletionsRequest marks id within gk. The kind is folded into the write, so a
// foreign id matches no row and the re-read reports ErrWrongKind.
func (s *sqliteStore) DeletionsRequest(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID) (*storeapi.RawObject, bool, error) {
	return s.requestDeletion(ctx,
		func(ctx context.Context) (*storeapi.RawObject, error) { return s.getObjectRowScoped(ctx, gk, id) },
		`id = ? AND "group" = ? AND kind = ?`, id, gk.Group, gk.Kind)
}

// DeletionsRequestBySlug marks the gk row holding slug. The slug rides in the
// UPDATE's own WHERE the way the kind does for DeletionsRequest, so the resolve and
// the mark are one statement: atomic, and a round trip cheaper than the alternative
// of wrapping a ObjectsGetBySlug + DeletionsRequest pair in a Within — which matters
// on a store that runs every caller through one connection.
func (s *sqliteStore) DeletionsRequestBySlug(ctx context.Context, gk storeapi.GroupKind, slug string) (*storeapi.RawObject, bool, error) {
	return s.requestDeletion(ctx,
		func(ctx context.Context) (*storeapi.RawObject, error) { return s.getObjectRowBySlug(ctx, gk, slug) },
		`"group" = ? AND kind = ? AND slug = ?`, gk.Group, gk.Kind, slug)
}

// DeletionsMarkOwned cascades deletion to ownerID's owned children. One indexed
// pass over the owned_by edge (idx_refs_to) reads each child's deletion state;
// markForDeletion then stamps only those not already deleting. So a re-cascade over
// an already-deleting subtree (the steady-state resync) is a lone SELECT — no
// writes, no events. It returns every owned child for requeue, deleting or not.
func (s *sqliteStore) DeletionsMarkOwned(ctx context.Context, ownerID storeapi.ObjectID) ([]storeapi.Referrer, error) {
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
		if _, _, err := s.markForDeletion(ctx, `id = ?`, ch.ref.ID); err != nil &&
			!errors.Is(err, storeapi.ErrNotFound) {
			return nil, err
		}
	}
	return out, nil
}

func (s *sqliteStore) ObjectsDelete(ctx context.Context, id storeapi.ObjectID) error {
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
	s.emit(ctx, storeapi.Deleted, obj)
	return nil
}

// RefsAdd inserts a (from_id, to_id, relation) edge, stamping an owed dependency
// wake when the caller's version claim says the target moved under it (see
// storeapi.Store.RefsAdd for the contract, and ControllerClient.AddDependency for
// what the claim means). It neither bumps resource_version nor emits — a ref is
// not a field of the object, so watchers would see no diff.
//
// It self-wraps in Within like the other mutators, so the endpoint check, the
// wake stamp and the insert are one atomic unit however it is called. That is
// what makes the claim decidable at all: read as a separate statement, a write to
// the target landing between the version read and the insert would be invisible
// both here and to the dependency waker (the edge is not yet inserted) — which is the
// very window AddDependency exists to close. Relying on the caller to supply the
// transaction, or on sqlite serializing writers on one connection, would leave
// that as an unstated precondition of the guard.
//
// The insert is deliberately the *last* write: every fallible step precedes the
// edge coming into existence. A nested Within is a bare fn(ctx) with no
// transaction of its own, so an error returned from here unwinds nothing — a
// caller sharing an ambient transaction and handling the error would commit
// whatever already landed. Ordering is therefore the only guarantee available,
// and it points the residual failure the harmless way: a stamp with no edge is
// one spurious owed wake, which costs a no-op reconcile and drains back to zero,
// where an edge with no stamp is a dependent stranded on a stale read that
// ObjectsListUnsettledIDs structurally cannot see.
func (s *sqliteStore) RefsAdd(ctx context.Context, fromID, toID storeapi.ObjectID, relation storeapi.Relation, targetResourceVersion int64) (storeapi.RefsAddResult, error) {
	var out storeapi.RefsAddResult
	err := s.Within(ctx, func(ctx context.Context) error {
		// One round-trip, and without loading the row blobs. Joining the two rows
		// rather than projecting each column as its own scalar subquery keeps this at
		// one rowid seek per endpoint — SQLite does no common subexpression
		// elimination, so "group" and kind as separate subqueries would seek the same
		// row twice. Either endpoint missing yields no row at all, which is the clean
		// ErrNotFound over a raw FK violation.
		var group, kind string
		var targetRV int64
		err := s.conn(ctx).QueryRowContext(ctx, `
			SELECT f."group", f.kind, t.resource_version
			FROM objects f, objects t WHERE f.id = ? AND t.id = ?`,
			fromID, toID).Scan(&group, &kind, &targetRV)
		if errors.Is(err, sql.ErrNoRows) {
			return storeapi.ErrNotFound
		}
		if err != nil {
			return err
		}
		// Before the insert, so a rejected claim writes nothing at all. After it, the
		// rollback would be the caller's to perform — and a caller nested in its own
		// Within has no inner transaction to unwind, so a swallowed error would leave
		// the edge behind.
		if targetResourceVersion > targetRV {
			return storeapi.ErrTargetResourceVersionFuture
		}
		// The durable wake stamp, on the same side of the insert as the rejection
		// above and for the same reason. Its NOT EXISTS is the edge-new test, a probe
		// straight down the refs primary key — which, refs being WITHOUT ROWID, is
		// the table itself, so it is one statement with no extra round-trip. And it
		// is the *only* place edge-newness is decided, so there is no second
		// derivation of it to fall out of agreement with.
		var stamped bool
		if targetResourceVersion > 0 && targetRV > targetResourceVersion {
			res, err := s.conn(ctx).ExecContext(ctx, `
				UPDATE objects SET pending_wake = pending_wake + 1
				WHERE id = ? AND NOT EXISTS (
					SELECT 1 FROM refs WHERE from_id = ? AND to_id = ? AND relation = ?)`,
				fromID, fromID, toID, string(relation))
			if err != nil {
				return err
			}
			// The error is discarded as at the other RowsAffected sites — modernc caches
			// the count and cannot fail here, and a branch this driver can never take is
			// untestable dead code. Worth knowing if the driver ever changes: this site is
			// the one where a wrong count is not a wrong return value but a silently
			// skipped wake, and a lost dependency wake is permanent and invisible.
			n, _ := res.RowsAffected()
			stamped = n > 0
		}
		if _, err := s.conn(ctx).ExecContext(ctx, `
			INSERT INTO refs (from_id, to_id, relation) VALUES (?, ?, ?)
			ON CONFLICT(from_id, to_id, relation) DO NOTHING`,
			fromID, toID, string(relation)); err != nil {
			return err
		}
		out = storeapi.RefsAddResult{
			From:        storeapi.GroupKind{Group: group, Kind: kind},
			WakeStamped: stamped,
		}
		return nil
	})
	if err != nil {
		return storeapi.RefsAddResult{}, err
	}
	return out, nil
}

// RefsDelete removes a (from_id, to_id, relation) edge; an absent edge is a
// silent no-op. Like RefsAdd it bumps nothing and joins the ambient transaction.
func (s *sqliteStore) RefsDelete(ctx context.Context, fromID, toID storeapi.ObjectID, relation storeapi.Relation) error {
	_, err := s.conn(ctx).ExecContext(ctx,
		`DELETE FROM refs WHERE from_id = ? AND to_id = ? AND relation = ?`,
		fromID, toID, string(relation))
	return err
}

// RefsListIncoming returns the objects pointing at toID through relation, joining refs
// to objects so each carries the GroupKind needed to route a requeue.
func (s *sqliteStore) RefsListIncoming(ctx context.Context, toID storeapi.ObjectID, relation storeapi.Relation) ([]storeapi.Referrer, error) {
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

// RefsGroupIncomingByID resolves RefsListIncoming for many targets at once,
// bucketed by target id — the incoming twin of RefsGroupOutgoingByID. It routes
// by r.to_id and joins the source side (r.from_id).
func (s *sqliteStore) RefsGroupIncomingByID(ctx context.Context, toIDs []storeapi.ObjectID, relation storeapi.Relation) (map[storeapi.ObjectID][]storeapi.Referrer, error) {
	return s.refsByIDs(ctx, toIDs, relation, "to_id", "from_id")
}

// idChunkSize bounds how many ids the batched by-id reads (refsByIDs,
// conditionsByIDs) bind in a single query, kept under SQLite's
// SQLITE_MAX_VARIABLE_NUMBER (32766 in modernc) with room for the extra
// parameters — otherwise a large List would fail with "too many SQL variables".
// A var, not a const, so tests can shrink it to exercise the multi-chunk merge
// without seeding tens of thousands of rows.
var idChunkSize = 30000

// refsByIDs is the shared batched edge lookup behind RefsGroupIncomingByID and
// RefsGroupOutgoingByID: it filters refs by routeCol IN (ids), joins objects on
// the opposite endpoint joinCol, and buckets each referrer under its routeCol
// value. routeCol/joinCol are fixed internal column names (never user input), so
// concatenating them is injection-safe. The id list is chunked under the bound-
// parameter limit (see idChunkSize); each chunk merges into the same map,
// and a routeCol value with no matching edge never appears.
func (s *sqliteStore) refsByIDs(ctx context.Context, ids []storeapi.ObjectID, relation storeapi.Relation, routeCol, joinCol string) (map[storeapi.ObjectID][]storeapi.Referrer, error) {
	out := make(map[storeapi.ObjectID][]storeapi.Referrer, len(ids))
	for start := 0; start < len(ids); start += idChunkSize {
		end := min(start+idChunkSize, len(ids))
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

// RefsListOutgoing returns the distinct objects fromID points at (any relation),
// the inverse of RefsListIncoming. DISTINCT collapses an object reached through
// more than one relation (e.g. both owned_by and depends_on) to a single row.
func (s *sqliteStore) RefsListOutgoing(ctx context.Context, fromID storeapi.ObjectID) ([]storeapi.Referrer, error) {
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

// RefsListOutgoingByRelation returns the objects fromID points at through the
// given relation, ordered by id — the relation-filtered form of
// RefsListOutgoing. No DISTINCT is needed: (from_id, to_id, relation) is unique,
// so a fixed relation can reach each target at most once.
func (s *sqliteStore) RefsListOutgoingByRelation(ctx context.Context, fromID storeapi.ObjectID, relation storeapi.Relation) ([]storeapi.Referrer, error) {
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

// RefsGroupOutgoingByID resolves RefsListOutgoingByRelation for many sources at
// once, bucketed by source id. It routes by r.from_id and joins the target side
// (r.to_id).
func (s *sqliteStore) RefsGroupOutgoingByID(ctx context.Context, fromIDs []storeapi.ObjectID, relation storeapi.Relation) (map[storeapi.ObjectID][]storeapi.Referrer, error) {
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

// RefsDeleteFinalizingDependsOn removes depends_on edges into toID whose source
// is itself deletion-pending, breaking the deadlock where mutually dependent (or
// self-dependent) finalizing objects each hold the other's RESTRICT. Like
// RefsDelete it bumps no version and emits no event.
func (s *sqliteStore) RefsDeleteFinalizingDependsOn(ctx context.Context, toID storeapi.ObjectID) error {
	_, err := s.conn(ctx).ExecContext(ctx, `
		DELETE FROM refs
		WHERE to_id = ? AND relation = ?
		  AND from_id IN (SELECT id FROM objects WHERE deletion_requested_at IS NOT NULL)`,
		toID, string(storeapi.RelationDependsOn))
	return err
}

// RefsHasIncoming reports whether any object with a live claim points at id: an
// owned_by edge, or a depends_on edge from a source that is not itself
// finalizing. A depends_on edge from a deletion-pending source is ignored — that
// dependent is going away and no longer has a claim, so it must not gate a
// finalizer (RefsHasIncoming would otherwise never clear when two finalizing
// objects depend on each other). owned_by always counts: the foreground cascade
// must wait for the owned child to be physically removed.
func (s *sqliteStore) RefsHasIncoming(ctx context.Context, id storeapi.ObjectID) (bool, error) {
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
		&deletionAt, &obj.PendingWake, &finalizers, &createdAt, &updatedAt)
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

// jsonText binds a JSON blob into a TEXT column. A []byte binds as a BLOB, which
// a STRICT table rejects in a TEXT column, so hand the driver a string instead; a
// nil blob stays NULL (e.g. an unwritten status).
func jsonText(b []byte) any {
	if b == nil {
		return nil
	}
	return string(b)
}

func toMillis(t time.Time) int64 { return t.UnixMilli() }

func fromMillis(ms int64) time.Time { return time.UnixMilli(ms).UTC() }

func millisPtr(ms int64) *time.Time {
	t := fromMillis(ms)
	return &t
}
