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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/amorey/beehive/internal/storeapi"
)

type sqliteStore struct {
	db *sql.DB

	// processStart stamps when this store opened. Liveness conditions written by a
	// prior process (updated_at older than this) read as Unknown ("verifying")
	// until a controller re-confirms them in this process.
	processStart time.Time
}

// Close closes the database. It is idempotent (database/sql's Close is), and
// after it the store is unusable. There is nothing else to tear down: the store
// owns no goroutines and no fan-out, so a consumer scanning the write log simply
// starts failing its next scan.
func (s *sqliteStore) Close() error {
	return s.db.Close()
}

// txKey carries the in-flight transaction frame through the context so that Store
// calls made with the ctx passed to Within join it.
type txKey struct{}

// txFrame is one Within frame: the transaction state every frame shares, plus this
// frame's depth in the savepoint stack.
//
// The depth travels *with* the state, never under a key of its own. A separate key
// would be sticky — it survives everything that does not explicitly clear it, and
// installing a transaction only installs txKey — so a ctx carrying a stale depth
// from a finished transaction would install a fresh txState at height 0 while still
// reporting nonzero depth, and the first nested call inside it would look like a
// concurrent frame. AfterCommit's hook ctx is exactly such a ctx: it strips txKey and
// nothing else. Folding the two into one value makes installing a transaction reset
// the depth by construction, for the same reason txState keeps tx and hooks together.
type txFrame struct {
	st    *txState
	depth int
}

// txState is what a transaction puts on the context: the connection every store
// call made with that ctx joins, plus the hooks owed its commit.
//
// The two travel as one value on purpose — "am I inside a transaction?" and
// "where do I defer until it commits?" are the same question, and answering them
// from two independent context keys would let a future path install one without
// the other.
type txState struct {
	tx *sql.Tx

	// mu guards hooks against a Within whose fn fans store calls across goroutines
	// on the tx ctx. flushed latches the list closed: a hook that holds the tx ctx
	// it was registered on can reach AfterCommit again after commit, and appending
	// there would be a silent drop.
	mu      sync.Mutex
	hooks   []func()
	flushed bool

	// closed latches once the transaction is over, by commit or by rollback. A ctx
	// carrying this txState outlives the transaction — AfterCommit's contract lets a
	// hook pass back the tx ctx it captured — so both Within and conn consult it and
	// degrade together, treating the ctx as carrying no transaction rather than a
	// dead one. flushed is no substitute: a rolled-back transaction never sets it.
	closed bool

	// savepoints counts the savepoints this transaction has opened, ever. It names
	// them; it is not a stack height. Monotonic on purpose: a depth-indexed name is
	// reused after an unwind, and ROLLBACK TO on a duplicate name rewinds to the most
	// recent match — correct, but it leaves the next reader proving it.
	savepoints int64

	// height is the current savepoint stack depth, which a nested frame's ctx depth
	// must match to be the rightful next frame. Unlike savepoints it goes back down.
	height int

	// poisoned latches the first failed unwind. After one, the transaction's state is
	// unknown, so it must neither take further nested work nor commit.
	//
	// Not paranoia: SQLite rolls the whole transaction back — savepoint stack
	// included — on SQLITE_FULL, SQLITE_IOERR and SQLITE_NOMEM, and a ROLLBACK TO
	// issued after one of those genuinely fails, because the savepoint it names is
	// gone. The common errors are not in that class: SQLITE_BUSY and constraint
	// violations abort a statement, not the transaction, so savepoints behave
	// normally through them.
	poisoned error
}

// poison latches err as the first failed unwind. A later one adds nothing: the
// state was already unknown.
func (st *txState) poison(err error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.poisoned == nil {
		st.poisoned = err
	}
}

func (st *txState) poisonErr() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.poisoned
}

// savepointStmt builds "<verb> bh_sp_<n>". The name is interpolated rather than
// bound because SQLite accepts no parameter where a savepoint name goes; n is an
// int64 this package owns, so there is nothing to escape. AppendInt over a stack
// array keeps this off fmt, which matters because modernc.org/sqlite compiles each
// statement fresh and these are the most trivial statements we issue.
func savepointStmt(verb string, n int64) string {
	var buf [40]byte
	b := append(buf[:0], verb...)
	b = append(b, " bh_sp_"...)
	b = strconv.AppendInt(b, n, 10)
	return string(b)
}

// pushSavepoint admits a new nested frame, reserving its savepoint name and the hook
// watermark to unwind to. All under mu, since a Within fn may fan store calls across
// goroutines — which is also what depth guards against: a frame whose ctx depth does
// not match the live stack height was entered from somewhere that does not own the
// top of the stack.
func (st *txState) pushSavepoint(depth int) (name int64, mark int, err error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.poisoned != nil {
		return 0, 0, st.poisoned
	}
	if depth != st.height {
		return 0, 0, storeapi.ErrConcurrentNestedTx
	}
	st.height++
	st.savepoints++
	return st.savepoints, len(st.hooks), nil
}

// popSavepoint restores the stack height. It runs on every exit path from an admitted
// frame, including one whose SAVEPOINT failed: leaving the height raised would give
// every later sibling on this transaction a spurious ErrConcurrentNestedTx.
func (st *txState) popSavepoint() {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.height > 0 {
		st.height--
	}
}

// truncateHooks drops every hook queued since mark, so an unwind takes the frame's
// deferred side effects with its writes — otherwise a WithOnCreate registered inside
// a rolled-back frame still fires at the outermost commit, for a row that is gone.
//
// flushed needs no consideration here: takeHooks sets it only after the outermost
// commit, and a nested frame can only be in flight before that, so the list is
// provably still open.
func (st *txState) truncateHooks(mark int) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if mark <= len(st.hooks) {
		st.hooks = st.hooks[:mark]
	}
}

// nested runs fn inside a SAVEPOINT on the ambient transaction, so an error fn
// returns unwinds fn's writes and its queued hooks whatever the outer caller then
// does with that error — including swallowing it. The outermost transaction is
// still the only thing that commits, and a savepoint adds no fsync, so durability
// is unchanged.
//
// Nothing here recovers, and nothing balances the stack on a panic: a panic
// unwinding through this skips the RELEASE, and the outermost Within's deferred
// tx.Rollback discards the whole transaction, savepoint stack included. A recover
// here would turn a panic into a half-committed transaction.
func (st *txState) nested(ctx context.Context, depth int, fn func(ctx context.Context) error) error {
	// pushSavepoint refuses outright once an unwind has failed. The outermost check
	// is what guarantees the rollback; this one keeps a caller that swallowed the
	// poison error from piling writes onto a transaction in unknown state.
	name, mark, err := st.pushSavepoint(depth)
	if err != nil {
		return err
	}
	defer st.popSavepoint()
	ctx = context.WithValue(ctx, txKey{}, &txFrame{st: st, depth: depth + 1})
	if _, err := st.tx.ExecContext(ctx, savepointStmt("SAVEPOINT", name)); err != nil {
		// Nothing was pushed on the SQLite side, so the state is still known and the
		// caller's ordinary error handling is the right answer. No poison.
		return err
	}
	ferr := fn(ctx)
	if ferr != nil {
		st.truncateHooks(mark)
		if _, err := st.tx.ExecContext(ctx, savepointStmt("ROLLBACK TO", name)); err != nil {
			st.poison(err)
			return errors.Join(ferr, err)
		}
	}
	// RELEASE pops the savepoint on both outcomes: ROLLBACK TO rewinds to it but
	// leaves it on the stack, so without this the stack would grow for the life of the
	// transaction. errors.Join drops a nil ferr, so the success path returns the
	// RELEASE error alone.
	if _, err := st.tx.ExecContext(ctx, savepointStmt("RELEASE", name)); err != nil {
		st.poison(err)
		return errors.Join(ferr, err)
	}
	return ferr
}

// hookDisposition is what addHook decided to do with a hook. The two non-queue
// outcomes are genuinely different and must not be collapsed: one transaction
// committed and one did not.
type hookDisposition int

const (
	hookQueued  hookDisposition = iota // waits for the outermost commit
	hookRunNow                         // that commit already happened; "after" it is now
	hookDiscard                        // the transaction rolled back; the hook must never run
)

// addHook queues fn and reports what became of it. Queueing where nothing will look
// again is a silent drop, which is what this return exists to prevent.
func (st *txState) addHook(fn func()) hookDisposition {
	st.mu.Lock()
	defer st.mu.Unlock()
	switch {
	case st.flushed:
		// The commit ran and drained the queue, so run it now. takeHooks sets flushed
		// before any hook executes, which is what makes a hook registered from inside
		// a hook land here rather than in the discard arm below.
		return hookRunNow
	case st.closed:
		// Closed without ever flushing: the transaction rolled back. Discarding is the
		// point, not a leak — running it would fire a WithOnCreate for a row that never
		// landed, which is the one guarantee AfterCommit exists to provide.
		return hookDiscard
	}
	st.hooks = append(st.hooks, fn)
	return hookQueued
}

// close latches the transaction closed, by either outcome. Idempotent, so the
// commit path can call it eagerly and the deferred call still covers the rest.
func (st *txState) close() {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.closed = true
}

func (st *txState) isClosed() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.closed
}

// takeHooks drains the queue and latches it closed.
func (st *txState) takeHooks() []func() {
	st.mu.Lock()
	defer st.mu.Unlock()
	hooks := st.hooks
	st.hooks, st.flushed = nil, true
	return hooks
}

// txFrom returns the ambient transaction frame, if any.
func txFrom(ctx context.Context) (*txFrame, bool) {
	fr, ok := ctx.Value(txKey{}).(*txFrame)
	return fr, ok
}

// dbtx is the subset of *sql.DB and *sql.Tx the object queries use, so the same
// code path runs both standalone and inside a Within transaction.
type dbtx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// conn returns the ambient transaction if ctx carries a live one, else the pool. A
// closed txState degrades to the pool for the same reason Within opens a fresh
// transaction on one: the ctx outlives its transaction, and half of it behaving as
// "the transaction is over" would be worse than neither half doing so.
func (s *sqliteStore) conn(ctx context.Context) dbtx {
	if fr, ok := txFrom(ctx); ok && !fr.st.isClosed() {
		return fr.st.tx
	}
	return s.db
}

// Within runs fn inside a single transaction. A nested Within (ctx already
// carries a tx) joins the outer transaction rather than opening a new one.
//
// Read-modify-write atomicity rests on the DSN's _txlock=immediate: BeginTx
// issues BEGIN IMMEDIATE, so a transaction holds the sole WAL write lock from
// BEGIN through Commit, before its first read. No other writer can commit in
// between, so a compare-then-write (ObjectsUpdateSpec's no-op suppression, ConditionsSet,
// FinalizersDelete, …) can't act on a stale snapshot, independent of pool size.
// This only covers compound writes routed through Within; a read then a separate
// write on the bare pool is not atomic, so keep multi-statement mutations here.
//
// Nothing is published on commit: a write becomes visible by landing in the
// table, and consumers find it by scanning resource_version (see
// ObjectWritesListSince). A rolled-back transaction is therefore invisible for
// free — its rows are gone and its versions were never committed — with no buffer
// to discard and no ordering to hold between the commit and a publication.
//
// AfterCommit hooks are the one thing deferred to the commit, and they run only
// on a clean one. A nested Within joins the outer transaction's queue, so a hook
// registered deep inside a caller's Within still waits for the outermost commit.
//
// A nested Within is a real rollback boundary: it runs fn inside a SAVEPOINT, so an
// error fn returns unwinds fn's own writes and its own queued hooks even if the
// outer caller swallows that error. Without it, any multi-write composition would be
// atomic only by the grace of its callers.
func (s *sqliteStore) Within(ctx context.Context, fn func(ctx context.Context) error) error {
	if fr, ok := txFrom(ctx); ok && !fr.st.isClosed() {
		// nested: a savepoint on the outer tx, joining its hook queue
		return fr.st.nested(ctx, fr.depth, fn)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	st := &txState{tx: tx}
	defer st.close() // covers the rollback and early-return paths
	ctx = context.WithValue(ctx, txKey{}, &txFrame{st: st})
	defer tx.Rollback() // no-op once Commit succeeds; rolls back on any early return
	if err := fn(ctx); err != nil {
		return err // hooks discarded, nothing ran
	}
	// A failed unwind somewhere inside leaves state we cannot reason about, so a
	// clean return from fn is not enough to commit on. The deferred Rollback is what
	// actually discards it.
	if err := st.poisonErr(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// Before draining, not in the deferred close: the hooks below run inside this
	// function, so a close deferred to its return would still read false for the
	// whole window in which a hook can hand its captured tx ctx back to the store.
	st.close()
	// After the commit and outside any lock: a hook is caller code and may write to
	// the store, which would re-enter Within.
	for _, hook := range st.takeHooks() {
		hook()
	}
	return nil
}

// AfterCommit defers fn to the outermost transaction's commit. Outside a
// transaction there is nothing to wait for — the write has landed already — so fn
// runs inline on the caller's own ctx. So does a registration that arrives too
// late to be queued: a hook holding the tx ctx it was registered on can call back
// in here after the commit drained the queue, and "run after the commit" is
// satisfied by running now, not by queueing where nothing will look again.
func (s *sqliteStore) AfterCommit(ctx context.Context, fn func(context.Context)) {
	fr, ok := txFrom(ctx)
	if !ok {
		fn(ctx) // nothing to defer to, and nothing to strip
		return
	}
	st := fr.st
	// Strip the transaction before handing the ctx on: by the time the hook runs
	// that *sql.Tx is committed, so a store call joining it would fail outright.
	// A hook that writes gets a fresh transaction, which is the only thing it could
	// have meant. Everything else on the ctx is inherited.
	hookCtx := context.WithValue(ctx, txKey{}, nil)
	switch st.addHook(func() { fn(hookCtx) }) {
	case hookRunNow:
		fn(hookCtx)
	case hookDiscard:
		// Deliberately nothing: a rolled-back transaction never runs its hooks.
	}
}

// objectColumns is the canonical select list; scanObject reads them in order.
const objectColumns = `id, "group", kind, slug, spec, status,
	schema_version_spec, schema_version_status,
	generation, observed_generation, observed_at, resource_version,
	deletion_requested_at, reconcile_owed, finalizers, created_at, updated_at`

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

// scanWritten scans a mutator's RETURNING row and assembles its conditions.
// Mutators share it, so the returned object carries the full conditions set
// regardless of which column the write touched, matching Get/List.
func (s *sqliteStore) scanWritten(ctx context.Context, sc scanner) (*storeapi.RawObject, error) {
	obj, err := scanObject(sc)
	if err != nil {
		return nil, err
	}
	if _, err := s.attachConditions(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *sqliteStore) ObjectsCreate(ctx context.Context, obj *storeapi.RawObject) (*storeapi.RawObject, error) {
	// Self-wrapping keeps the version draw and the insert atomic: a create is two
	// statements (nextResourceVersion, then INSERT ... RETURNING), and a scan of the
	// write log orders rows by the version they were stamped with. Store is public,
	// so this cannot rest on every caller happening to be inside a transaction
	// already; a nested Within is free.
	var created *storeapi.RawObject
	err := s.Within(ctx, func(ctx context.Context) error {
		var err error
		created, err = s.objectsCreate(ctx, obj)
		return err
	})
	if err != nil {
		return nil, err
	}
	return created, nil
}

func (s *sqliteStore) objectsCreate(ctx context.Context, obj *storeapi.RawObject) (*storeapi.RawObject, error) {
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
	return s.scanWritten(ctx, row)
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
// kind. It reads the bare row rather than a full ObjectsGet because the kind
// boundary is all it enforces, which drops the conditions marshal.
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

func (s *sqliteStore) ObjectsGet(ctx context.Context, id storeapi.ObjectID) (*storeapi.RawObject, error) {
	obj, err := s.getObjectRow(ctx, id)
	if err != nil {
		return nil, err
	}
	return s.attachConditions(ctx, obj)
}

// ObjectsGetForReconcile is the reconcile loop's opening read (see the contract on
// storeapi.Store). The cursor and the dependency flag are correlated subqueries —
// one over the single-row resource_version_seq, one an EXISTS on the edges
// primary-key prefix — so they ride the row read rather than adding round trips on
// a pool of one connection.
func (s *sqliteStore) ObjectsGetForReconcile(ctx context.Context, id storeapi.ObjectID) (storeapi.ReconcileLoad, error) {
	var load storeapi.ReconcileLoad
	row := s.conn(ctx).QueryRowContext(ctx, `
		SELECT `+objectColumns+`,
		       (SELECT value FROM resource_version_seq WHERE id = 1),
		       EXISTS (SELECT 1 FROM edges
		                WHERE from_id = objects.id AND relation = 'depends_on')
		  FROM objects WHERE id = ?`, id)
	obj, err := scanObject(row, &load.Cursor, &load.HasDependencies)
	if err != nil {
		return storeapi.ReconcileLoad{}, err
	}
	if _, err := s.attachConditions(ctx, obj); err != nil {
		return storeapi.ReconcileLoad{}, err
	}
	load.Object = *obj
	return load, nil
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

// ObjectsListByIncomingEdge returns the full rows of the objects pointing at toID
// through relation, restricted to kind gk — the blob-bearing form of
// EdgesListIncoming (which returns bare id/GroupKind referrers). Resolving the
// edges in the statement is what saves the caller a Get per child.
//
// The edge is a semi-join, not a join: written as a join the planner drives from
// idx_objects_kind (which already delivers ORDER BY o.id) and probes edges once
// per object *of the kind*. IN (SELECT …) lets idx_edges_to drive instead, so the
// work scales with the owner's children rather than the whole table.
func (s *sqliteStore) ObjectsListByIncomingEdge(ctx context.Context, gk storeapi.GroupKind, toID storeapi.ObjectID, relation storeapi.Relation) ([]*storeapi.RawObject, error) {
	return s.listObjectsWhere(ctx, `
		WHERE o.id IN (SELECT from_id FROM edges WHERE to_id = ? AND relation = ?)
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
// ids also avoids paying the predicate (an edges semi-join, for
// ObjectsListByIncomingEdge) twice. The list is chunked under the bound-parameter
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

func (s *sqliteStore) DeletionRequestsList(ctx context.Context) ([]storeapi.ObjectRef, error) {
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
	return scanObjectRefs(rows)
}

func (s *sqliteStore) ReconcileOwedListIDs(ctx context.Context, gk storeapi.GroupKind) ([]storeapi.ObjectID, error) {
	// Matches the partial index idx_objects_reconcile_owed WHERE reconcile_owed != 0.
	rows, err := s.conn(ctx).QueryContext(ctx,
		`SELECT id FROM objects
		 WHERE "group" = ? AND kind = ? AND reconcile_owed != 0
		 ORDER BY id`,
		gk.Group, gk.Kind)
	if err != nil {
		return nil, err
	}
	return scanIDs(rows)
}

// ReconcileOwedIncrement and ReconcileOwedDecrement are single no-emit UPDATEs on the
// owed-wake count. The decrement's contract (cross-kind, no resource_version bump,
// why it takes the observed count) is on storeapi.Store; the subtraction floors at
// 0 with max() so it can never drive the count negative.
//
// The increment is deliberately *not* on that interface: production wakes are
// produced by EdgesAdd, whose stamp must be indivisible from the edge insert, so the
// declare path cannot route through a separate call and no other producer exists
// yet. It stays here as the standalone form — reachable on the concrete store, so
// tests can seed a count without staging the whole declare race — and is where a
// future non-edge producer (see the dependency-waker item in TODO.md) would hook in.
func (s *sqliteStore) ReconcileOwedIncrement(ctx context.Context, id storeapi.ObjectID) error {
	_, err := s.conn(ctx).ExecContext(ctx,
		`UPDATE objects SET reconcile_owed = reconcile_owed + 1 WHERE id = ?`, id)
	return err
}

func (s *sqliteStore) ReconcileOwedDecrement(ctx context.Context, id storeapi.ObjectID, observed int64) error {
	_, err := s.conn(ctx).ExecContext(ctx,
		`UPDATE objects SET reconcile_owed = max(reconcile_owed - ?, 0) WHERE id = ?`, observed, id)
	return err
}

// DependentsListStale re-derives which dependents are owed a pass (see the
// contract on storeapi.Store). The join through edges.to_id is total by
// construction rather than by luck: to_id is ON DELETE RESTRICT, so a target row
// always outlives every edge pointing at it and there is no window in which a
// dependent's edge survives a collected target. Nothing here has to handle a
// missing target, and no LEFT JOIN is needed on that side — only the watermark,
// whose absence is what "never reconciled against a known point" looks like.
//
// **The CROSS JOINs pin the join order, and the query is superlinear without
// them.** SQLite treats CROSS JOIN as "do not reorder" and nothing else, so the
// results are unchanged. Left to choose, the planner drives from
// idx_objects_kind — every object of a registered kind, probing edges per row —
// and, because that index is keyed on (group, kind, rowid) rather than on
// from_id, it cannot stream the GROUP BY and materialises a temp B-tree of every
// remaining match *before* LIMIT applies. Paging to exhaustion then costs one
// full scan per page. Driving from edges instead gives the bound this method
// claims. Measured on the 0001 schema, modernc:
//
//	SEARCH e USING PRIMARY KEY (from_id>?)
//	SEARCH d USING INTEGER PRIMARY KEY (rowid=?)
//	SEARCH t USING INTEGER PRIMARY KEY (rowid=?)
//	SEARCH c USING INTEGER PRIMARY KEY (rowid=?) LEFT-JOIN
//
// So: one covering range scan over the depends_on edges above afterID, three
// rowid seeks each, and no sort at all — the scan already arrives in from_id
// order, which is what lets LIMIT stop it after `limit` groups. Cost is bounded
// by the dependency graph rather than by the object count, which is what
// separates this from a full pass, and the two-column comparison in the last
// predicate — which no index can serve — is only ever evaluated on rows that scan
// already reached.
//
// GROUP BY, not SELECT DISTINCT: a dependent with several stale targets must
// appear once, and with the scan in from_id order the grouping is free, where
// DISTINCT plans a temp B-tree for itself and a second one for the ORDER BY.
//
// The kinds list is not chunked under idChunkSize as the id-list reads are. Those
// take caller data of unbounded size; this takes the registered-kind set, which
// comes from Register calls in code — a store with enough kinds to exhaust
// SQLITE_MAX_VARIABLE_NUMBER cannot be written.
func (s *sqliteStore) DependentsListStale(ctx context.Context, kinds []storeapi.GroupKind, afterID storeapi.ObjectID, limit int) ([]storeapi.ObjectRef, error) {
	if len(kinds) == 0 || limit <= 0 {
		return nil, nil
	}
	args := make([]any, 0, len(kinds)*2+2)
	args = append(args, afterID)
	placeholders := make([]string, len(kinds))
	for i, gk := range kinds {
		placeholders[i] = "(?, ?)"
		args = append(args, gk.Group, gk.Kind)
	}
	args = append(args, limit)
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT e.from_id, d."group", d.kind
		  FROM edges e
		  CROSS JOIN objects d ON d.id = e.from_id
		  CROSS JOIN objects t ON t.id = e.to_id
		  LEFT JOIN dependency_watermarks c ON c.object_id = e.from_id
		 WHERE e.relation = 'depends_on'
		   AND e.from_id != e.to_id
		   AND e.from_id > ?
		   AND (d."group", d.kind) IN (VALUES `+strings.Join(placeholders, ", ")+`)
		   AND (c.reconciled_against IS NULL OR t.resource_version > c.reconciled_against)
		 GROUP BY e.from_id
		 ORDER BY e.from_id
		 LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	return scanObjectRefs(rows)
}

// DependencyWatermarksSet upserts id's dependency watermark (see the contract on
// storeapi.Store). The EXISTS rides the edges primary-key prefix, so the
// has-dependencies gate costs a b-tree probe and no separate read — and, since a
// collected object's edges cascade away with it, the same probe is what keeps the
// foreign key satisfied when gcCollect removes the row mid-pass.
//
// The WHERE on DO UPDATE does two jobs, and neither is optional. It makes the
// stored cursor monotonic, so an out-of-order write cannot regress it and
// un-converge a dependent. And when the cursor has not advanced it suppresses the
// write outright — no page dirtied, no WAL frame — which is what keeps a
// RequeueAfter polling controller that declares dependencies from paying a row
// write per pass in a store nobody else is writing to. MAX(…) in the SET would give
// monotonicity alone and still rewrite the row every pass.
//
// It is also what couples reconciled_at to reconciled_against structurally: one
// predicate guards both columns, so the timestamp cannot move on a pass that
// observed nothing new (see the table comment on why it is not a heartbeat).
func (s *sqliteStore) DependencyWatermarksSet(ctx context.Context, id storeapi.ObjectID, cursor int64) error {
	_, err := s.conn(ctx).ExecContext(ctx, `
		INSERT INTO dependency_watermarks (object_id, reconciled_against, reconciled_at)
		SELECT ?, ?, ?
		 WHERE EXISTS (SELECT 1 FROM edges WHERE from_id = ? AND relation = 'depends_on')
		    ON CONFLICT(object_id) DO UPDATE
		   SET reconciled_against = excluded.reconciled_against,
		       reconciled_at      = excluded.reconciled_at
		 WHERE excluded.reconciled_against > dependency_watermarks.reconciled_against`,
		id, cursor, toMillis(time.Now().UTC()), id)
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

// ObjectWritesMaxVersion reads the high-water mark of the object write log: the
// highest resource_version any live objects row holds. Covered by idx_objects_rv,
// so it is an index-max lookup rather than a scan, and NULL (an empty store) reads
// as 0.
//
// It reads the objects rows rather than resource_version_seq, which is the same
// number only when nothing else draws from that sequence — and the event log does.
// A consumer of this pair (this and ObjectWritesListSince, which selects objects)
// must not see the cursor move for a write it can never be shown: an EventsAdd
// bumping the sequence would otherwise read as "something changed", and a
// controller recording an event per reconcile would make that permanent.
func (s *sqliteStore) ObjectWritesMaxVersion(ctx context.Context) (int64, error) {
	var rv sql.NullInt64
	err := s.conn(ctx).QueryRowContext(ctx,
		`SELECT MAX(resource_version) FROM objects`).Scan(&rv)
	return rv.Int64, err
}

// ObjectWritesListSince returns the writes above afterRV: live rows, in cursor
// order, at most limit of them. Blob-free — it selects no spec or status, and is
// covered by idx_objects_rv — because its consumers route by id and read current
// state themselves.
//
// This is how a change reaches anyone: there is no push. A consumer holds a
// watermark, scans on its own cadence, and advances it, which is lossless however
// long the gap between scans, since resource_version is globally monotonic and
// never reused.
//
// Kind-agnostic: a depends_on edge may point at a kind with no controller, so a
// per-kind query could not name every target whose change matters. Ordered by
// resource_version so the caller pages by taking the last row's version as its
// next cursor.
//
// A row deleted since afterRV is simply absent (see the type's own note on why
// that cannot strand a dependent).
func (s *sqliteStore) ObjectWritesListSince(ctx context.Context, afterRV int64, limit int) ([]storeapi.ObjectWrite, error) {
	if limit <= 0 {
		// An exported entry point: a non-positive limit would reach SQLite as
		// "LIMIT -1" (unbounded, the opposite of what was asked) and a negative one
		// would panic in make below.
		return nil, nil
	}
	rows, err := s.conn(ctx).QueryContext(ctx,
		`SELECT id, resource_version FROM objects
		 WHERE resource_version > ? ORDER BY resource_version LIMIT ?`,
		afterRV, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// Capped: limit is the caller's, and a large one must not preallocate for rows
	// the store may not have.
	writes := make([]storeapi.ObjectWrite, 0, min(limit, 1024))
	for rows.Next() {
		var w storeapi.ObjectWrite
		_ = rows.Scan(&w.ID, &w.ResourceVersion) // two INTEGER columns into int64 never error
		writes = append(writes, w)
	}
	return writes, rows.Err()
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
		// Identical spec *at the same schema version*: nothing changed, so don't bump
		// generation/resource_version. A bump would falsely unsettle a converged object
		// and trigger a needless reconcile, and it would show a watch poll a spurious
		// diff (mirrors DeletionRequestsCreate's idempotent no-op).
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
		result, err = s.scanWritten(ctx, row)
		changed = err == nil
		return err
	})
	return result, changed, err
}

// ObjectsUpdateStatus skips the status write when the incoming bytes equal the stored
// ones, mirroring ObjectsUpdateSpec/FinalizersDelete/ConditionsDelete: no resource_version
// bump and no updated_at touch — a watch poll would otherwise report a spurious
// Modified, and the dependency waker, which scans exactly that version, would wake
// every dependent on each unchanged health poll.
//
// The convergence handshake is the one thing a content no-op must still carry:
// observed_generation/observed_at record *that the controller ran*, not what it
// wrote, and ObjectsListUnsettledIDs (the owed-pass backstop) keys off
// observed_generation
// < generation. Leaving it behind on an identical-status reconcile would strand
// the object unsettled forever, re-enqueued every full pass. And that advance is a
// real transition — the object just settled at a new generation — so it bumps
// resource_version even though the bytes didn't move: anything
// gating on ObservedGeneration == Generation would otherwise wait for the next
// owed pass to learn the object converged. It can't spin a controller re-applying
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
			result, err = s.scanWritten(ctx, row)
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
		// its generation land is what marks the object unsettled so the full pass
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
		result, err = s.scanWritten(ctx, row)
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

// bumpObject advances id's resource_version and returns the assembled object. The
// condition mutators share it: a condition lives in its own table, so the version
// bump — which is what makes the change visible to a scan of the write log — can't
// be folded into the semantic write and is a separate UPDATE.
func (s *sqliteStore) bumpObject(ctx context.Context, c dbtx, id storeapi.ObjectID) (*storeapi.RawObject, error) {
	rv, err := nextResourceVersion(ctx, c)
	if err != nil {
		return nil, err
	}
	row := c.QueryRowContext(ctx, `
		UPDATE objects SET resource_version = ?, updated_at = ?
		WHERE id = ?
		RETURNING `+objectColumns, rv, toMillis(time.Now().UTC()), id)
	return s.scanWritten(ctx, row)
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
		// No-op suppression: an identical condition leaves resource_version where it is,
		// so no scan finds anything (mirrors DeletionRequestsCreate).
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
		// A condition change bumps the object's resource_version, which is what a watch
		// poll and the dependency waker both look at.
		result, err = s.bumpObject(ctx, c, id)
		return err
	})
	return result, err
}

func (s *sqliteStore) ConditionsDelete(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, condType string) (*storeapi.RawObject, error) {
	// Within keeps the delete and the version bump atomic (see ConditionsSet).
	var result *storeapi.RawObject
	err := s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		// Scoped read enforces the kind boundary up front (symmetric with
		// ConditionsSet); the conditions table carries no group/kind to fold into
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
		// Absent condition: nothing changed, so don't bump resource_version — a watch
		// poll would otherwise report a spurious diff. Return the object we already
		// read, with its conditions assembled.
		if n == 0 {
			result, err = s.attachConditions(ctx, obj)
			return err
		}
		result, err = s.bumpObject(ctx, c, id)
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
// timeline is empty. EventsGetLatest returns it as-is.
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
// for (id, category), or ok=false if that timeline is empty. EventsAdd needs
// only the key to decide extend-vs-append, so it deliberately does not decode the
// full row (unlike EventsGetLatest): probing the columns it is about to discard
// would let a decode fault in an older run mask — rather than surface through —
// the write EventsAdd is about to make.
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

func (s *sqliteStore) EventsAdd(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, ev storeapi.Event) (*storeapi.Event, error) {
	// Within serializes the read-latest-then-write (via _txlock=immediate) so the
	// run-boundary decision can't race, and joins the caller's tx when nested.
	var result *storeapi.Event
	err := s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		// Scoped read enforces the kind boundary (ErrNotFound/ErrWrongKind), like
		// ConditionsSet — the events table carries no group/kind to fold in.
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
		return err
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
		// Absent finalizer: nothing changed, so don't bump resource_version — a watch
		// poll would otherwise report a spurious diff (mirrors ConditionsDelete).
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
		result, err = s.scanWritten(ctx, row)
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
// an already-deleting row. Like edgesByIDs' column names it is a compile-time
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
	obj, err := s.scanWritten(ctx, row)
	if err != nil {
		return nil, false, err // ErrNotFound = no transition (guard/where/missing)
	}
	return obj, true, nil
}

// requestDeletion is the mark-or-reread protocol behind both deletion entry points.
// markForDeletion's ErrNotFound is ambiguous — already deleting, out of scope, or
// gone — so reread resolves it on the caller's own key and supplies the current row
// for the no-op case. Within keeps the rv-bump and the write atomic whether or not
// a caller wrapped this: the mutators self-wrap, and nested they join the caller's
// transaction — e.g. the GC cascade.
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

// DeletionRequestsCreate marks id within gk. The kind is folded into the write, so a
// foreign id matches no row and the re-read reports ErrWrongKind.
func (s *sqliteStore) DeletionRequestsCreate(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID) (*storeapi.RawObject, bool, error) {
	return s.requestDeletion(ctx,
		func(ctx context.Context) (*storeapi.RawObject, error) { return s.getObjectRowScoped(ctx, gk, id) },
		`id = ? AND "group" = ? AND kind = ?`, id, gk.Group, gk.Kind)
}

// DeletionRequestsCreateBySlug marks the gk row holding slug. The slug rides in the
// UPDATE's own WHERE the way the kind does for DeletionRequestsCreate, so the resolve and
// the mark are one statement: atomic, and a round trip cheaper than the alternative
// of wrapping a ObjectsGetBySlug + DeletionRequestsCreate pair in a Within — which matters
// on a store that runs every caller through one connection.
func (s *sqliteStore) DeletionRequestsCreateBySlug(ctx context.Context, gk storeapi.GroupKind, slug string) (*storeapi.RawObject, bool, error) {
	return s.requestDeletion(ctx,
		func(ctx context.Context) (*storeapi.RawObject, error) { return s.getObjectRowBySlug(ctx, gk, slug) },
		`"group" = ? AND kind = ? AND slug = ?`, gk.Group, gk.Kind, slug)
}

// DeletionRequestsCreateFromOwner cascades deletion to ownerID's owned children. One indexed
// pass over the owned_by edge (idx_edges_to) reads each child's deletion state;
// markForDeletion then stamps only those not already deleting. So a re-cascade over
// an already-deleting subtree (the steady-state case) is a lone SELECT — no
// writes, no events. It returns every owned child for requeue, deleting or not.
func (s *sqliteStore) DeletionRequestsCreateFromOwner(ctx context.Context, ownerID storeapi.ObjectID) ([]storeapi.ObjectRef, error) {
	// Self-wrapped for the same reason as ObjectsCreate and ObjectsDelete: it stamps
	// several children, each drawing a version and publishing, and publication is in
	// commit order only inside Within. Outside one, two of this loop's own writes can
	// reach the store-wide stream in the wrong order — which corrupts the backlog
	// bound a consumer reads as a cursor. The in-tree caller already wraps it, so this
	// is for the external ones Store's public surface admits.
	var out []storeapi.ObjectRef
	err := s.Within(ctx, func(ctx context.Context) error {
		var err error
		out, err = s.deletionRequestsCreateFromOwner(ctx, ownerID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *sqliteStore) deletionRequestsCreateFromOwner(ctx context.Context, ownerID storeapi.ObjectID) ([]storeapi.ObjectRef, error) {
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT o.id, o."group", o.kind, o.deletion_requested_at
		FROM edges r JOIN objects o ON o.id = r.from_id
		WHERE r.to_id = ? AND r.relation = ?
		ORDER BY o.id`, ownerID, string(storeapi.RelationOwnedBy))
	if err != nil {
		return nil, err
	}
	type child struct {
		ref      storeapi.ObjectRef
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

	out := make([]storeapi.ObjectRef, 0, len(children))
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
	// Self-wrapped for the same reason as ObjectsCreate: the delete cascades to
	// conditions, events and edges, and those go together or not at all.
	return s.Within(ctx, func(ctx context.Context) error { return s.objectsDelete(ctx, id) })
}

// objectsDelete physically removes the row. A delete draws no resource_version:
// versions live on rows, and this one is gone, so there is nothing left for a
// scan of the write log to report. Consumers that must observe removals — the
// client's watch surface — diff their own snapshot and derive the tombstone from
// the row's absence.
func (s *sqliteStore) objectsDelete(ctx context.Context, id storeapi.ObjectID) error {
	// A zero-row delete scans to ErrNotFound, which is how callers learn the row
	// was already collected. The object's conditions, events and edges are
	// cascade-deleted by this statement.
	res, err := s.conn(ctx).ExecContext(ctx, `DELETE FROM objects WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected() // modernc caches the count; RowsAffected never errors
	if n == 0 {
		return storeapi.ErrNotFound
	}
	return nil
}

// edgeIsNew is the "this call is the one creating the edge" test, as a WHERE
// fragment over (from_id, to_id, relation) — a probe straight down the edges
// primary key, which is the table itself since edges is WITHOUT ROWID, so a
// statement carrying it costs no extra round trip and no pre-read. Two of
// EdgesAdd's statements are gated on it and both must decide it identically; it
// is a const so they cannot drift, which is the property the alternative — one
// pre-read feeding both — would have bought with a round trip.
const edgeIsNew = `NOT EXISTS (
	SELECT 1 FROM edges WHERE from_id = ? AND to_id = ? AND relation = ?)`

// EdgesAdd inserts a (from_id, to_id, relation) edge, stamping an owed dependency
// wake for every new depends_on edge it creates (see storeapi.Store.EdgesAdd for
// the contract). It does not bump resource_version — a ref is not a field of the
// object, so no watch poll would see a diff anyway.
//
// It self-wraps in Within like the other mutators, so the endpoint check, the
// wake stamp and the insert are one atomic unit however it is called, rather than
// relying on the caller to supply the transaction or on sqlite serializing
// writers on one connection.
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
func (s *sqliteStore) EdgesAdd(ctx context.Context, fromID, toID storeapi.ObjectID, relation storeapi.Relation) (storeapi.EdgesAddResult, error) {
	var out storeapi.EdgesAddResult
	err := s.Within(ctx, func(ctx context.Context) error {
		// One round-trip, and without loading the row blobs. Joining the two rows
		// rather than projecting each column as its own scalar subquery keeps this at
		// one rowid seek per endpoint — SQLite does no common subexpression
		// elimination, so "group" and kind as separate subqueries would seek the same
		// row twice. Either endpoint missing yields no row at all, which is the clean
		// ErrNotFound over a raw FK violation.
		var group, kind string
		err := s.conn(ctx).QueryRowContext(ctx, `
			SELECT f."group", f.kind
			FROM objects f, objects t WHERE f.id = ? AND t.id = ?`,
			fromID, toID).Scan(&group, &kind)
		if errors.Is(err, sql.ErrNoRows) {
			return storeapi.ErrNotFound
		}
		if err != nil {
			return err
		}
		// The durable wake stamp, before the insert (see the func doc for why the
		// ordering carries the guarantee). Its NOT EXISTS is the edge-new test, a probe
		// straight down the edges primary key — which, edges being WITHOUT ROWID, is
		// the table itself, so it is one statement with no extra round-trip.
		//
		// Every new depends_on edge stamps: recording owed work is
		// the only mechanism that survives every interleaving, because a stamp landing
		// mid-pass sits above the count the pass observed at load and so survives the
		// decrement. The watermark clear below cannot say the same — a dependent whose
		// own pass is in flight rewrites the row this call just cleared, from a cursor
		// that never saw the new target, and a target that never moves again is then
		// never reconciled against. The
		// edge-new test is what bounds the cost at one extra pass per edge ever
		// created. Self-edges are skipped, as every scan skips them: an object's own
		// pass always reads its current self, so there is nothing a self-wake could
		// deliver.
		// One predicate for both writes below, so a future edit to either gate cannot
		// silently split the stamp from the clear.
		newDependency := relation == storeapi.RelationDependsOn && fromID != toID
		var stamped bool
		if newDependency {
			res, err := s.conn(ctx).ExecContext(ctx, `
				UPDATE objects SET reconcile_owed = reconcile_owed + 1
				WHERE id = ? AND `+edgeIsNew,
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
		// A new depends_on edge also invalidates the dependent's staleness watermark,
		// so drop it: the row was recorded over a smaller dependency set, so it cannot
		// speak for a target just added, and an absent row already means "never
		// reconciled against a known point". The stamp above is what *guarantees* the
		// dependent a pass — this clear can be undone by a pass already in flight,
		// which is why it stopped being the covering mechanism — but leaving a false
		// watermark standing would still misreport convergence to DependentsListStale
		// for the window until the owed pass runs, so the derived state is kept honest
		// alongside the durable record.
		//
		// Gated on the same edge-new test as the stamp, so re-asserting a dependency
		// set every pass still costs nothing after the first. Self-edges are skipped to
		// match DependentsListStale, which excludes them: clearing for one would drop a
		// watermark no scan will ever re-derive from that edge.
		if newDependency {
			if _, err := s.conn(ctx).ExecContext(ctx, `
				DELETE FROM dependency_watermarks
				 WHERE object_id = ? AND `+edgeIsNew,
				fromID, fromID, toID, string(relation)); err != nil {
				return err
			}
		}
		if _, err := s.conn(ctx).ExecContext(ctx, `
			INSERT INTO edges (from_id, to_id, relation) VALUES (?, ?, ?)
			ON CONFLICT(from_id, to_id, relation) DO NOTHING`,
			fromID, toID, string(relation)); err != nil {
			return err
		}
		out = storeapi.EdgesAddResult{
			From:                 storeapi.GroupKind{Group: group, Kind: kind},
			ReconcileOwedStamped: stamped,
		}
		return nil
	})
	if err != nil {
		return storeapi.EdgesAddResult{}, err
	}
	return out, nil
}

// EdgesDelete removes a (from_id, to_id, relation) edge; an absent edge is a
// silent no-op. Like EdgesAdd it bumps nothing and joins the ambient transaction.
func (s *sqliteStore) EdgesDelete(ctx context.Context, fromID, toID storeapi.ObjectID, relation storeapi.Relation) error {
	_, err := s.conn(ctx).ExecContext(ctx,
		`DELETE FROM edges WHERE from_id = ? AND to_id = ? AND relation = ?`,
		fromID, toID, string(relation))
	return err
}

// EdgesListIncoming returns the objects pointing at toID through relation, joining edges
// to objects so each carries the GroupKind needed to route a requeue.
func (s *sqliteStore) EdgesListIncoming(ctx context.Context, toID storeapi.ObjectID, relation storeapi.Relation) ([]storeapi.ObjectRef, error) {
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT o.id, o."group", o.kind
		FROM edges r JOIN objects o ON o.id = r.from_id
		WHERE r.to_id = ? AND r.relation = ?
		ORDER BY o.id`, toID, string(relation))
	if err != nil {
		return nil, err
	}
	return scanObjectRefs(rows)
}

// EdgesGroupIncomingByID resolves EdgesListIncoming for many targets at once,
// bucketed by target id — the incoming twin of EdgesGroupOutgoingByID. It routes
// by r.to_id and joins the source side (r.from_id).
func (s *sqliteStore) EdgesGroupIncomingByID(ctx context.Context, toIDs []storeapi.ObjectID, relation storeapi.Relation) (map[storeapi.ObjectID][]storeapi.ObjectRef, error) {
	return s.edgesByIDs(ctx, toIDs, relation, "to_id", "from_id")
}

// idChunkSize bounds how many ids the batched by-id reads (edgesByIDs,
// conditionsByIDs) bind in a single query, kept under SQLite's
// SQLITE_MAX_VARIABLE_NUMBER (32766 in modernc) with room for the extra
// parameters — otherwise a large List would fail with "too many SQL variables".
// A var, not a const, so tests can shrink it to exercise the multi-chunk merge
// without seeding tens of thousands of rows.
var idChunkSize = 30000

// edgesByIDs is the shared batched edge lookup behind EdgesGroupIncomingByID and
// EdgesGroupOutgoingByID: it filters edges by routeCol IN (ids), joins objects on
// the opposite endpoint joinCol, and buckets each referrer under its routeCol
// value. routeCol/joinCol are fixed internal column names (never user input), so
// concatenating them is injection-safe. The id list is chunked under the bound-
// parameter limit (see idChunkSize); each chunk merges into the same map,
// and a routeCol value with no matching edge never appears.
func (s *sqliteStore) edgesByIDs(ctx context.Context, ids []storeapi.ObjectID, relation storeapi.Relation, routeCol, joinCol string) (map[storeapi.ObjectID][]storeapi.ObjectRef, error) {
	out := make(map[storeapi.ObjectID][]storeapi.ObjectRef, len(ids))
	for start := 0; start < len(ids); start += idChunkSize {
		end := min(start+idChunkSize, len(ids))
		if err := s.edgesByIDsChunk(ctx, ids[start:end], relation, routeCol, joinCol, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// edgesByIDsChunk runs edgesByIDs for one chunk of ids, merging rows into out. It
// closes its result set before returning so the next chunk's query can run on the
// single-connection store (which permits one open result set at a time).
func (s *sqliteStore) edgesByIDsChunk(ctx context.Context, ids []storeapi.ObjectID, relation storeapi.Relation, routeCol, joinCol string, out map[storeapi.ObjectID][]storeapi.ObjectRef) error {
	args := make([]any, 0, len(ids)+1)
	placeholders := make([]string, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args = append(args, id)
	}
	args = append(args, string(relation))
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT r.`+routeCol+`, o.id, o."group", o.kind
		FROM edges r JOIN objects o ON o.id = r.`+joinCol+`
		WHERE r.`+routeCol+` IN (`+strings.Join(placeholders, ",")+`) AND r.relation = ?
		ORDER BY r.`+routeCol+`, o.id`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var route storeapi.ObjectID
		var d storeapi.ObjectRef
		// All columns are INTEGER/TEXT NOT NULL; the scan never fails (see scanObjectRefs).
		_ = rows.Scan(&route, &d.ID, &d.Group, &d.Kind)
		out[route] = append(out[route], d)
	}
	return rows.Err()
}

// EdgesListOutgoing returns the distinct objects fromID points at (any relation),
// the inverse of EdgesListIncoming. DISTINCT collapses an object reached through
// more than one relation (e.g. both owned_by and depends_on) to a single row.
func (s *sqliteStore) EdgesListOutgoing(ctx context.Context, fromID storeapi.ObjectID) ([]storeapi.ObjectRef, error) {
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT DISTINCT o.id, o."group", o.kind
		FROM edges r JOIN objects o ON o.id = r.to_id
		WHERE r.from_id = ?
		ORDER BY o.id`, fromID)
	if err != nil {
		return nil, err
	}
	return scanObjectRefs(rows)
}

// EdgesListOutgoingByRelation returns the objects fromID points at through the
// given relation, ordered by id — the relation-filtered form of
// EdgesListOutgoing. No DISTINCT is needed: (from_id, to_id, relation) is unique,
// so a fixed relation can reach each target at most once.
func (s *sqliteStore) EdgesListOutgoingByRelation(ctx context.Context, fromID storeapi.ObjectID, relation storeapi.Relation) ([]storeapi.ObjectRef, error) {
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT o.id, o."group", o.kind
		FROM edges r JOIN objects o ON o.id = r.to_id
		WHERE r.from_id = ? AND r.relation = ?
		ORDER BY o.id`, fromID, string(relation))
	if err != nil {
		return nil, err
	}
	return scanObjectRefs(rows)
}

// EdgesGroupOutgoingByID resolves EdgesListOutgoingByRelation for many sources at
// once, bucketed by source id. It routes by r.from_id and joins the target side
// (r.to_id).
func (s *sqliteStore) EdgesGroupOutgoingByID(ctx context.Context, fromIDs []storeapi.ObjectID, relation storeapi.Relation) (map[storeapi.ObjectID][]storeapi.ObjectRef, error) {
	return s.edgesByIDs(ctx, fromIDs, relation, "from_id", "to_id")
}

// scanObjectRefs collects an (id, group, kind) SELECT into ObjectRefs, closing rows
// on return. Like scanObjects it ends in `return out, rows.Err()`: the id/group/
// kind columns are INTEGER/TEXT NOT NULL scanned into int64/string, which never
// fails, and modernc's buffered result set leaves rows.Err clean after a good
// query — so the tail error is reported in one statement, not a dead branch.
func scanObjectRefs(rows *sql.Rows) ([]storeapi.ObjectRef, error) {
	defer rows.Close()
	var out []storeapi.ObjectRef
	for rows.Next() {
		var d storeapi.ObjectRef
		// id (INTEGER) -> int64 and group/kind (TEXT NOT NULL) -> string never fail.
		_ = rows.Scan(&d.ID, &d.Group, &d.Kind)
		out = append(out, d)
	}
	return out, rows.Err()
}

// EdgesDeleteFinalizingDependsOn removes depends_on edges into toID whose source
// is itself deletion-pending, breaking the deadlock where mutually dependent (or
// self-dependent) finalizing objects each hold the other's RESTRICT. Like
// EdgesDelete it bumps no version.
func (s *sqliteStore) EdgesDeleteFinalizingDependsOn(ctx context.Context, toID storeapi.ObjectID) error {
	_, err := s.conn(ctx).ExecContext(ctx, `
		DELETE FROM edges
		WHERE to_id = ? AND relation = ?
		  AND from_id IN (SELECT id FROM objects WHERE deletion_requested_at IS NOT NULL)`,
		toID, string(storeapi.RelationDependsOn))
	return err
}

// EdgesHasIncoming reports whether any object with a live claim points at id: an
// owned_by edge, or a depends_on edge from a source that is not itself
// finalizing. A depends_on edge from a deletion-pending source is ignored — that
// dependent is going away and no longer has a claim, so it must not gate a
// finalizer (EdgesHasIncoming would otherwise never clear when two finalizing
// objects depend on each other). owned_by always counts: the foreground cascade
// must wait for the owned child to be physically removed.
func (s *sqliteStore) EdgesHasIncoming(ctx context.Context, id storeapi.ObjectID) (bool, error) {
	var exists int
	err := s.conn(ctx).QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM edges r
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

// scanObject reads objectColumns into a RawObject. extra binds any columns the
// statement appended *after* objectColumns, for a caller that rides its own values
// on the row read (see ObjectsGetForReconcile); the positional coupling is why
// extras go last in both places.
func scanObject(sc scanner, extra ...any) (*storeapi.RawObject, error) {
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
	dest := []any{
		&obj.ID, &obj.Group, &obj.Kind, &slug, &obj.Spec, &status,
		&obj.SpecVersion, &obj.StatusVersion,
		&obj.Generation, &observedGen, &observedAt, &obj.ResourceVersion,
		&deletionAt, &obj.ReconcileOwed, &finalizers, &createdAt, &updatedAt,
	}
	err := sc.Scan(append(dest, extra...)...)
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
