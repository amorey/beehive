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
	"sync/atomic"
	"time"

	"github.com/amorey/beehive/internal/storeapi"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// Compile-time interface checks. The optional capabilities are acquired by type
// assertion with the failure discarded, so a drifted signature would silently
// turn the feature off rather than fail to build.
var (
	_ storeapi.Store             = (*sqliteStore)(nil)
	_ storeapi.FreePagesReleaser = (*sqliteStore)(nil)
	_ storeapi.DriverCursorer    = (*sqliteStore)(nil)
)

// object_writes.op. The soft delete is an ordinary update: the row is still
// live and readable, so only the GC's physical removal is writeOpDelete.
const (
	writeOpCreate = 1
	writeOpUpdate = 2
	writeOpDelete = 3
)

type sqliteStore struct {
	db *sql.DB

	// txCount counts transactions begun; a nested Within (savepoint) does not add.
	// Test-only, to assert a fast path answered without BEGIN IMMEDIATE.
	txCount atomic.Int64

	// processStart: liveness conditions written before it read as Unknown until
	// re-confirmed in this process.
	processStart time.Time
}

// Close closes the database. Idempotent; the store owns no goroutines, so there
// is nothing else to tear down.
func (s *sqliteStore) Close() error {
	return s.db.Close()
}

// Drain floor: release only past both an absolute size and a share of the file.
// Free pages are what the next inserts would have reused, so draining a small
// freelist trades work for nothing.
const (
	freePagesFloor        = 256 // pages, ~1MB at the default 4KB page size
	freePagesFloorDivisor = 8   // ...and at least 1/8 of the file
)

// FreePagesRelease hands up to maxPages of the freelist back to the OS with
// PRAGMA incremental_vacuum, reporting how many pages left the file. It releases
// nothing, without error, under the drain floor or on an auto_vacuum=NONE
// database. The count is advisory: a difference of two reads, logged, never
// acted on.
//
// The pragma frees one page per step, so it must be Exec'd, never Query'd —
// Query releases exactly one page and reports no error.
func (s *sqliteStore) FreePagesRelease(ctx context.Context, maxPages int) (int, error) {
	if maxPages <= 0 {
		return 0, nil
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("free pages: acquire conn: %w", err)
	}
	defer conn.Close()
	return freePagesRelease(ctx, conn, maxPages)
}

// freePagesRelease is FreePagesRelease once the connection is in hand, split out
// so the arithmetic can be tested against a scripted dbtx.
func freePagesRelease(ctx context.Context, c dbtx, maxPages int) (int, error) {
	pages, free, err := pageCounters(ctx, c)
	if err != nil {
		return 0, err
	}
	if free <= freePagesFloor || free <= pages/freePagesFloorDivisor {
		return 0, nil
	}

	// Exec, not Query — see above.
	if _, err := c.ExecContext(ctx, `PRAGMA incremental_vacuum(`+strconv.Itoa(maxPages)+`)`); err != nil {
		return 0, fmt.Errorf("free pages: incremental_vacuum: %w", err)
	}

	_, freeAfter, err := pageCounters(ctx, c)
	if err != nil {
		return 0, err
	}
	released := free - freeAfter
	if released < 0 {
		// Another writer freed more than the drain took; don't report negative.
		return 0, nil
	}
	return released, nil
}

// pageCounters reads page_count and freelist_count off one connection.
func pageCounters(ctx context.Context, c dbtx) (pages, free int, err error) {
	if err := c.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pages); err != nil {
		return 0, 0, fmt.Errorf("free pages: read page_count: %w", err)
	}
	if err := c.QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&free); err != nil {
		return 0, 0, fmt.Errorf("free pages: read freelist_count: %w", err)
	}
	return pages, free, nil
}

// txKey carries the in-flight transaction frame through the context so that Store
// calls made with the ctx passed to Within join it.
type txKey struct{}

// txFrame is one Within frame: the shared transaction state plus this frame's
// depth in the savepoint stack.
//
// Depth travels *with* the state, never under a key of its own — a stale depth
// from a finished transaction would otherwise ride into a fresh one
// (AfterCommit's hook ctx strips only txKey).
type txFrame struct {
	st    *txState
	depth int
	// id is this frame's savepoint number, 0 for the outermost; it records which
	// frame owns a queued hook, so an unwind drops only its own and its
	// descendants'.
	id int64
}

// txState is what a transaction puts on the context: the connection every store
// call made with that ctx joins, plus the hooks owed its commit. One value so
// neither half can be installed without the other.
type txState struct {
	tx *sql.Tx

	// mu guards against a Within fn fanning calls across goroutines; AfterCommit
	// and bare reads stay legal concurrently.
	mu    sync.Mutex
	hooks []queuedHook

	// closed latches once the transaction is over, by either outcome. A ctx
	// carrying this txState outlives the transaction, so every consumer degrades
	// on it together: fresh transaction, pool, inline hook.
	closed bool

	// committed records *how* it ended; only flush sets it. A hook runs iff its
	// transaction committed, so "over" and "over and durable" differ.
	committed bool

	// sealed latches just before the commit; pushSavepoint refuses on it. Not
	// closed: a hook arriving between seal and drain must still queue and run.
	sealed bool

	// dead is the id ranges of frames that unwound: a late registration on a dead
	// frame's ctx must be refused, not queued fresh for the outer commit.
	dead []idRange

	// savepoints counts savepoints ever opened — it names them, not a stack
	// height. Monotonic so ROLLBACK TO never hits a reused name.
	savepoints int64

	// height is the current savepoint stack depth, which a nested frame's ctx
	// depth must match.
	height int

	// poisoned latches the first failed unwind: state unknown, so no further
	// nested work and no commit. Real: SQLITE_FULL/IOERR/NOMEM roll back the whole
	// transaction, savepoints included, so a later ROLLBACK TO genuinely fails.
	poisoned error
}

// poison latches err as the first failed unwind.
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

// savepointStmt builds "<verb> bh_sp_<n>". Interpolated because SQLite accepts
// no parameter where a savepoint name goes; n is package-owned, nothing to escape.
func savepointStmt(verb string, n int64) string {
	var buf [40]byte
	b := append(buf[:0], verb...)
	b = append(b, " bh_sp_"...)
	b = strconv.AppendInt(b, n, 10)
	return string(b)
}

// pushSavepoint admits a new nested frame under mu, reserving its savepoint
// name. A frame whose ctx depth does not match the live height was entered from
// somewhere that does not own the top of the stack.
func (st *txState) pushSavepoint(depth int, caller int64) (name int64, err error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.poisoned != nil {
		return 0, st.poisoned
	}
	// The caller's own frame is checked as well as the depth: depth is reusable
	// after an unwind, and a ctx captured from the dead frame would match again.
	if st.sealed || depth != st.height || st.deadLocked(caller) {
		return 0, storeapi.ErrStaleTxContext
	}
	st.height++
	st.savepoints++
	return st.savepoints, nil
}

// sealForCommit closes the transaction to new frames and reports whether
// committing is safe — both under the lock that admits frames, so nothing slips
// between the check and the commit. A live frame here can only belong to another
// goroutine.
func (st *txState) sealForCommit() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.poisoned != nil {
		return st.poisoned
	}
	if st.height != 0 {
		return storeapi.ErrConcurrentNestedTx
	}
	st.sealed = true
	return nil
}

// popSavepoint restores the stack height. It runs on every exit path from an
// admitted frame, including one whose SAVEPOINT failed.
func (st *txState) popSavepoint() {
	st.mu.Lock()
	defer st.mu.Unlock()
	// Unconditional: every push pairs with exactly one deferred pop.
	st.height--
}

// nested runs fn inside a SAVEPOINT on the ambient transaction, so an error fn
// returns unwinds fn's writes and its queued hooks even if the outer caller
// swallows it. Only the outermost transaction commits.
//
// Nothing here recovers: a panic through this frame leaves the savepoint open,
// and COMMIT would release it and land the writes — so an abandoned frame
// poisons, the one state that survives a recover.
func (st *txState) nested(ctx context.Context, depth int, caller int64, fn func(ctx context.Context) error) error {
	name, err := st.pushSavepoint(depth, caller)
	if err != nil {
		return err
	}
	settled := false
	defer func() {
		st.popSavepoint()
		if !settled {
			st.poison(errAbandonedFrame)
		}
	}()
	if _, err := st.tx.ExecContext(ctx, savepointStmt("SAVEPOINT", name)); err != nil {
		// Nothing was pushed on the SQLite side; state is still known. No poison.
		settled = true
		return err
	}
	// The unwind must outlive fn's ctx: a caller may cancel inside fn, and
	// ExecContext skips a statement on a canceled ctx — silently skipping the unwind.
	cleanupCtx := context.WithoutCancel(ctx)
	ferr := fn(context.WithValue(ctx, txKey{}, &txFrame{st: st, depth: depth + 1, id: name}))
	if ferr != nil {
		st.unwindFrame(name)
		if _, err := st.tx.ExecContext(cleanupCtx, savepointStmt("ROLLBACK TO", name)); err != nil {
			settled = true // already poisoned, with a better error than the defer's
			st.poison(err)
			return errors.Join(ferr, err)
		}
	}
	// RELEASE pops the savepoint on both outcomes: ROLLBACK TO rewinds to it but
	// leaves it on the stack. errors.Join drops a nil ferr.
	if _, err := st.tx.ExecContext(cleanupCtx, savepointStmt("RELEASE", name)); err != nil {
		settled = true // as above: the specific failure beats the generic one
		st.poison(err)
		return errors.Join(ferr, err)
	}
	settled = true
	return ferr
}

// errAbandonedFrame poisons a transaction whose nested frame exited without
// settling its savepoint (a panic); COMMIT would land that frame's writes.
var errAbandonedFrame = errors.New("beehive: nested transaction frame abandoned without unwinding")

// queuedHook is a hook plus the frame that registered it — ownership recorded,
// not inferred from slice position, since concurrent appends interleave.
type queuedHook struct {
	owner int64
	fn    func()
}

// idRange is a frame that unwound plus every frame opened inside it: ids are
// monotonic and a frame only opens while its parent is live.
type idRange struct{ lo, hi int64 }

// hookDisposition: a hook runs iff its transaction committed and its frame did
// not unwind; the three outcomes are that rule's branches.
type hookDisposition int

const (
	hookQueued  hookDisposition = iota // waits for the outermost commit
	hookRunNow                         // that commit happened already; "after" it is now
	hookDiscard                        // the writes it was owed to are gone; it must never run
)

// addHook queues fn and reports what became of it — queueing where nothing will
// look again would be a silent drop.
func (st *txState) addHook(owner int64, fn func()) hookDisposition {
	st.mu.Lock()
	defer st.mu.Unlock()
	// Checked before closed: a rolled-back frame never runs its hooks.
	if st.deadLocked(owner) {
		return hookDiscard
	}
	if st.closed {
		if !st.committed {
			// Rolled back: the writes are gone, so nothing owed the commit may fire.
			return hookDiscard
		}
		// Committed and drained: "after the commit" is now.
		return hookRunNow
	}
	st.hooks = append(st.hooks, queuedHook{owner: owner, fn: fn})
	return hookQueued
}

// close latches the transaction closed, by either outcome. Idempotent.
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

// flush latches closed *and committed* and drains the queue in one critical
// section, so no registration lands between close and drain. Only the commit
// path calls it, which is what makes committed trustworthy.
func (st *txState) flush() []queuedHook {
	st.mu.Lock()
	defer st.mu.Unlock()
	hooks := st.hooks
	st.hooks, st.closed, st.committed = nil, true, true
	return hooks
}

// unwindFrame discards the hooks owned by frame name and its descendants and
// marks that id range dead; enclosing frames' hooks keep their place and order.
func (st *txState) unwindFrame(name int64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	kept := st.hooks[:0]
	for _, h := range st.hooks {
		if h.owner < name {
			kept = append(kept, h)
		}
	}
	st.hooks = kept
	// Ranges from inner frames are subsumed by the one about to be appended.
	live := st.dead[:0]
	for _, r := range st.dead {
		if r.lo < name {
			live = append(live, r)
		}
	}
	st.dead = append(live, idRange{lo: name, hi: st.savepoints})
}

// deadLocked reports whether id belongs to a frame that unwound. Callers hold mu.
func (st *txState) deadLocked(id int64) bool {
	for _, r := range st.dead {
		if id >= r.lo && id <= r.hi {
			return true
		}
	}
	return false
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

// conn returns the ambient transaction if ctx carries a live one, else the pool.
// A closed txState degrades to the pool — the ctx outlives its transaction, so a
// write issued on it commits standalone rather than failing with sql.ErrTxDone.
// Hooks should use the detached ctx AfterCommit hands them.
func (s *sqliteStore) conn(ctx context.Context) dbtx {
	if fr, ok := txFrom(ctx); ok && !fr.st.isClosed() {
		return fr.st.tx
	}
	return s.db
}

// Within runs fn inside a single transaction. A nested Within joins the outer
// transaction on a SAVEPOINT — a real rollback boundary: an error fn returns
// unwinds its own writes and queued hooks even if the outer caller swallows it.
//
// Read-modify-write atomicity rests on the DSN's _txlock=immediate: BEGIN
// IMMEDIATE holds the sole write lock from before the first read, so a
// compare-then-write cannot act on a stale snapshot. Only writes routed through
// Within get this — keep multi-statement mutations here.
//
// AfterCommit hooks run only on a clean outermost commit.
func (s *sqliteStore) Within(ctx context.Context, fn func(ctx context.Context) error) error {
	if fr, ok := txFrom(ctx); ok && !fr.st.isClosed() {
		// nested: a savepoint on the outer tx, joining its hook queue
		return fr.st.nested(ctx, fr.depth, fr.id, fn)
	}

	s.txCount.Add(1)
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
	// A clean fn return is not enough: a failed unwind leaves unknown state, and a
	// frame still open belongs to another goroutine — COMMIT would release its
	// savepoint and land its writes. Both are checked, and the door shut on new
	// frames, in one critical section; the deferred Rollback does the discarding.
	if err := st.sealForCommit(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// flush latches closed before the hooks below run — a close deferred to return
	// would read false while a hook can hand its captured tx ctx back to the store.
	hooks := st.flush()
	// Outside any lock: a hook may write to the store and re-enter Within.
	for _, hook := range hooks {
		hook.fn()
	}
	return nil
}

// AfterCommit defers fn to the outermost transaction's commit. Outside a
// transaction — or once the commit has drained the queue — fn runs inline:
// "after the commit" is satisfied by running now.
func (s *sqliteStore) AfterCommit(ctx context.Context, fn func(context.Context)) {
	fr, ok := txFrom(ctx)
	if !ok {
		fn(ctx) // nothing to defer to, and nothing to strip
		return
	}
	st := fr.st
	// Strip the transaction: by hook time the *sql.Tx is committed, so a store
	// call joining it would fail. A hook that writes gets a fresh transaction.
	hookCtx := context.WithValue(ctx, txKey{}, nil)
	switch st.addHook(fr.id, func() { fn(hookCtx) }) {
	case hookRunNow:
		fn(hookCtx)
	case hookDiscard:
		// Deliberately nothing: the frame rolled back, so its writes are gone.
	}
}

// objectColumns is the canonical select list; scanObject reads them in order.
const objectColumns = `id, "group", kind, name, spec, status,
	schema_version_spec, schema_version_status,
	generation, observed_generation, observed_at, resource_version,
	deletion_requested_at, reconcile_owed, finalizers, created_at, updated_at`

// The sort order of every edge listing. Order on the edge column, never the
// joined o.id it equals: the edge index already arrives in that order, while
// ORDER BY o.id plans a temp B-tree (see TestEdgeListsInheritTheIndexOrder).
const (
	edgeOrderByReferrer = "\n\t\tORDER BY r.from_id" // incoming: who points at this
	edgeOrderByTarget   = "\n\t\tORDER BY r.to_id"   // outgoing: what this points at
)

// nextResourceVersion advances and returns the global write cursor. A standalone
// counter, not MAX(objects.resource_version): deleting the highest-versioned row
// must never regress the cursor and hand out a reused version.
func nextResourceVersion(ctx context.Context, c dbtx) (int64, error) {
	var rv int64
	err := c.QueryRowContext(ctx,
		`UPDATE resource_version_seq SET value = value + 1 WHERE id = 1 RETURNING value`).Scan(&rv)
	return rv, err
}

// scanWritten scans a mutator's RETURNING row and attaches its conditions,
// matching Get/List.
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

func (s *sqliteStore) ObjectsCreate(ctx context.Context, gk storeapi.GroupKind, in storeapi.ObjectsCreateInput) (*storeapi.RawObject, error) {
	// Self-wrapped: the version draw and the insert must be atomic, and Store is
	// public, so this cannot rest on the caller already being in a transaction.
	var created *storeapi.RawObject
	err := s.Within(ctx, func(ctx context.Context) error {
		var err error
		created, err = s.objectsCreate(ctx, gk, in)
		return err
	})
	if err != nil {
		return nil, err
	}
	return created, nil
}

func (s *sqliteStore) objectsCreate(ctx context.Context, gk storeapi.GroupKind, in storeapi.ObjectsCreateInput) (*storeapi.RawObject, error) {
	// Refused here, not left to the column's CHECK: the contract promises
	// ErrInvalidName, and a driver constraint error carries no sentinel. Before the
	// version draw, so a refused create burns no resource_version.
	if in.Name == "" {
		return nil, fmt.Errorf("%w: pass the name the object should be addressable by", storeapi.ErrInvalidName)
	}
	finalizers := marshalFinalizers(in.Finalizers)
	c := s.conn(ctx)
	rv, err := nextResourceVersion(ctx, c)
	if err != nil {
		return nil, err
	}
	now := toMillis(time.Now().UTC())

	// RETURNING hands back the written row, assigned id included — no follow-up read.
	row := c.QueryRowContext(ctx, `
		INSERT INTO objects
			("group", kind, name, spec, status, schema_version_spec,
			 generation, resource_version, finalizers, created_at, updated_at)
		VALUES (?, ?, ?, ?, NULL, ?, 1, ?, ?, ?, ?)
		RETURNING `+objectColumns,
		gk.Group, gk.Kind, in.Name, jsonText(in.Spec), in.SpecVersion,
		rv, jsonText(finalizers), now, now)
	// scanObject, not scanWritten: the id did not exist before this statement, so
	// the row provably has no conditions.
	obj, err := scanObject(row)
	if err != nil {
		return nil, asNameTaken(err)
	}
	if err := appendWriteLog(ctx, c, obj.ID, gk, writeOpCreate, rv, now); err != nil {
		return nil, err
	}
	return obj, nil
}

// appendWriteLog records one committed object write. Callers pass the version the
// write took, so the entry orders against the row it describes.
func appendWriteLog(ctx context.Context, c dbtx, id storeapi.ObjectID, gk storeapi.GroupKind, op int, rv, now int64) error {
	_, err := c.ExecContext(ctx, `
		INSERT INTO object_writes (resource_version, object_id, "group", kind, op, written_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		rv, id, gk.Group, gk.Kind, op, now)
	return err
}

// asNameTaken translates the UNIQUE violation on ("group", kind, name) into
// ErrNameTaken, leaving every other error alone. The name is the only uniqueness
// a create can hit, so the code alone identifies it.
func asNameTaken(err error) error {
	var serr *sqlite.Error
	if errors.As(err, &serr) && serr.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE {
		return fmt.Errorf("%w: %w", storeapi.ErrNameTaken, err)
	}
	return err
}

// probeObjectScoped answers "does id exist, is it gk's, and is it
// deletion-pending?" from three columns — no blobs, no finalizer unmarshal. Same
// errors as a scoped read: ErrNotFound, ErrWrongKind.
func (s *sqliteStore) probeObjectScoped(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID) (deletionPending bool, err error) {
	var group, kind string
	var deletionAt sql.NullInt64
	err = s.conn(ctx).QueryRowContext(ctx,
		`SELECT "group", kind, deletion_requested_at FROM objects WHERE id = ?`, id).
		Scan(&group, &kind, &deletionAt)
	if errors.Is(err, sql.ErrNoRows) {
		return false, storeapi.ErrNotFound // bare, like scanObject's
	}
	if err != nil {
		return false, err
	}
	if group != gk.Group || kind != gk.Kind {
		return false, fmt.Errorf("%w: object %d is %s/%s, not %s/%s",
			storeapi.ErrWrongKind, id, group, kind, gk.Group, gk.Kind)
	}
	return deletionAt.Valid, nil
}

// checkObjectScoped is probeObjectScoped for callers that only need the gate.
func (s *sqliteStore) checkObjectScoped(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID) error {
	_, err := s.probeObjectScoped(ctx, gk, id)
	return err
}

// getObjectRow reads the objects row without assembling conditions.
func (s *sqliteStore) getObjectRow(ctx context.Context, id storeapi.ObjectID) (*storeapi.RawObject, error) {
	row := s.conn(ctx).QueryRowContext(ctx,
		`SELECT `+objectColumns+` FROM objects WHERE id = ?`, id)
	return scanObject(row)
}

// getObjectRowScoped loads id's bare row (no conditions) and confirms it belongs
// to gk: ErrNotFound if gone, ErrWrongKind if it names another kind.
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

// ObjectsGetForReconcile is the reconcile loop's opening read (see the contract
// on storeapi.Store). The cursor and the dependency flag are correlated
// subqueries, riding the row read rather than adding round trips.
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

// ObjectsGetMeta is getObjectRow across the store boundary: no conditions
// assembled, for metadata-only callers.
func (s *sqliteStore) ObjectsGetMeta(ctx context.Context, id storeapi.ObjectID) (*storeapi.RawObject, error) {
	return s.getObjectRow(ctx, id)
}

// getObjectRowByName is getObjectRow keyed by name within gk. No ErrWrongKind:
// the kind is in the WHERE.
func (s *sqliteStore) getObjectRowByName(ctx context.Context, gk storeapi.GroupKind, name string) (*storeapi.RawObject, error) {
	row := s.conn(ctx).QueryRowContext(ctx,
		`SELECT `+objectColumns+` FROM objects WHERE "group" = ? AND kind = ? AND name = ?`,
		gk.Group, gk.Kind, name)
	return scanObject(row)
}

func (s *sqliteStore) ObjectsGetByName(ctx context.Context, gk storeapi.GroupKind, name string) (*storeapi.RawObject, error) {
	obj, err := s.getObjectRowByName(ctx, gk, name)
	if err != nil {
		return nil, err
	}
	return s.attachConditions(ctx, obj)
}

func (s *sqliteStore) ObjectsList(ctx context.Context, gk storeapi.GroupKind) ([]*storeapi.RawObject, error) {
	return s.listObjectsWhere(ctx, `WHERE o."group" = ? AND o.kind = ?`, gk.Group, gk.Kind)
}

// ObjectsListByIncomingEdge returns the full rows of the objects pointing at
// toID through relation, restricted to gk — the blob-bearing EdgesListIncoming.
//
// The edge is a semi-join, not a join: IN (SELECT …) lets idx_edges_to drive, so
// the work scales with the referrers rather than every object of the kind.
func (s *sqliteStore) ObjectsListByIncomingEdge(ctx context.Context, gk storeapi.GroupKind, toID storeapi.ObjectID, relation storeapi.Relation) ([]*storeapi.RawObject, error) {
	return s.listObjectsWhere(ctx, `
		WHERE o.id IN (SELECT from_id FROM edges WHERE to_id = ? AND relation = ?)
		  AND o."group" = ? AND o.kind = ?`,
		toID, string(relation), gk.Group, gk.Kind)
}

// listObjectsWhere is the shared multi-row object read: rows matching tail,
// ordered by id, conditions attached. tail is a fixed internal fragment, never
// user input; only its bound arguments come from the caller.
func (s *sqliteStore) listObjectsWhere(ctx context.Context, tail string, args ...any) ([]*storeapi.RawObject, error) {
	rows, err := s.conn(ctx).QueryContext(ctx,
		`SELECT `+objectColumns+` FROM objects o `+tail+` ORDER BY o.id`, args...)
	if err != nil {
		return nil, err
	}
	// scanObjects closes rows, freeing the single connection for the conditions query.
	out, err := scanObjects(rows)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return out, nil
	}

	// One batched query avoids an N+1 per-object lookup.
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

// conditionsByIDs returns the given objects' conditions, grouped by object id
// and ordered by type. A separate query: folding conditions into the blob SELECT
// would re-send each row's spec/status per condition. It keys off the already
// scanned ids because the two statements share no transaction — re-running the
// predicate could match a different set. Chunked under idChunkSize.
func (s *sqliteStore) conditionsByIDs(ctx context.Context, ids []storeapi.ObjectID) (map[storeapi.ObjectID][]storeapi.Condition, error) {
	byID := make(map[storeapi.ObjectID][]storeapi.Condition, len(ids))
	for start := 0; start < len(ids); start += idChunkSize {
		if err := s.conditionsByIDsChunk(ctx, ids[start:min(start+idChunkSize, len(ids))], byID); err != nil {
			return nil, err
		}
	}
	return byID, nil
}

// conditionsByIDsChunk runs one chunk, merging rows into out; it closes its
// result set so the next chunk can run on the single connection.
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
	// Kind-agnostic, so the global sweeper sees every finalizing object; the kind
	// rides along for routing. idx_objects_deleting covers exactly this column
	// list and order — keep them in step, or the plan silently gains a row fetch
	// or a temp B-tree.
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

// ReconcileOwedIncrement and ReconcileOwedDecrement are single no-emit UPDATEs
// on the owed-wake count; the decrement floors at 0 (contract on storeapi.Store).
//
// The increment is deliberately not on that interface: production wakes come
// from EdgesAdd, whose stamp must be indivisible from the edge insert. It stays
// here so tests can seed a count.
func (s *sqliteStore) ReconcileOwedIncrement(ctx context.Context, id storeapi.ObjectID) error {
	_, err := s.conn(ctx).ExecContext(ctx,
		`UPDATE objects SET reconcile_owed = reconcile_owed + 1 WHERE id = ?`, id)
	return err
}

// ReconcileOwedDecrement folds the kind into the UPDATE, so a foreign id matches
// no row; the disambiguating read is paid only when nothing was written.
func (s *sqliteStore) ReconcileOwedDecrement(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, observed int64) error {
	res, err := s.conn(ctx).ExecContext(ctx,
		`UPDATE objects SET reconcile_owed = max(reconcile_owed - ?, 0)
		 WHERE id = ? AND "group" = ? AND kind = ?`, observed, id, gk.Group, gk.Kind)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil || n > 0 {
		return err
	}
	// No row: another kind's id, or one collected since. Only the first is a fault.
	if err := s.checkObjectScoped(ctx, gk, id); err != nil && !errors.Is(err, storeapi.ErrNotFound) {
		return err
	}
	return nil
}

// DependentsListStale re-derives which dependents are owed a pass (see the
// contract on storeapi.Store). to_id is ON DELETE RESTRICT, so a target always
// outlives its edges — only the watermark side needs a LEFT JOIN.
//
// The CROSS JOINs pin the join order; the query is superlinear without them.
// Left to choose, the planner drives from idx_objects_kind and materialises a
// temp B-tree before LIMIT — one full scan per page. Driving from edges bounds
// the cost by the dependency graph, not the object count.
//
// GROUP BY, not DISTINCT: with the scan in from_id order the grouping is free,
// where DISTINCT plans two temp B-trees. The kinds list is not chunked: it comes
// from Register calls, not caller data.
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
// storeapi.Store). The EXISTS gate rides the edges primary-key prefix, and is
// also what keeps the foreign key satisfied when gcCollect removes the row
// mid-pass.
//
// The WHERE on DO UPDATE is load-bearing twice: it keeps the stored cursor
// monotonic, and it suppresses a no-advance write outright — no page dirtied —
// so a polling dependent pays no row write per pass. One predicate guards both
// columns, so reconciled_at cannot move without reconciled_against.
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

// ObjectWritesMaxVersion reads the high-water mark of the object write log: the highest
// resource_version any live objects row holds (covered by idx_objects_rv; an
// empty store reads 0).
//
// It reads objects, not resource_version_seq: the event log draws from that
// sequence too, and a consumer of this pair must not see the cursor move for a
// write it can never be shown.
func (s *sqliteStore) ObjectWritesMaxVersion(ctx context.Context) (int64, error) {
	var rv sql.NullInt64
	err := s.conn(ctx).QueryRowContext(ctx,
		`SELECT MAX(resource_version) FROM objects`).Scan(&rv)
	return rv.Int64, err
}

// ObjectWritesListSince returns the writes above afterRV: live rows, in cursor order,
// at most limit of them. Blob-free and covered by idx_objects_rv — consumers
// route by id and read current state themselves. Kind-agnostic, since a
// depends_on edge may point at a kind with no controller. A row deleted since
// afterRV is simply absent.
func (s *sqliteStore) ObjectWritesListSince(ctx context.Context, afterRV int64, limit int) ([]storeapi.ObjectWrite, error) {
	if limit <= 0 {
		// Would reach SQLite as "LIMIT -1" (unbounded) or panic in make below.
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
	// Capped: a large caller limit must not preallocate for rows the store may not have.
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

// stampVersion resolves the schema version a write leaves on the row. Incoming 0
// means "no opinion": leave the stored tag alone — a build that can't interpret
// the version must not relabel data as unversioned. Below the stored version is
// a genuine downgrade and refused: such a caller could not have decoded the row
// it is writing back.
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

// ObjectsUpdateSpec replaces id's spec within gk. See updateSpec for the shape.
func (s *sqliteStore) ObjectsUpdateSpec(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, spec []byte, specVersion int) (*storeapi.RawObject, bool, error) {
	// The scoped read enforces the kind boundary while doubling as the compare's load.
	return s.updateSpec(ctx, spec, specVersion, func(ctx context.Context) (*storeapi.RawObject, error) {
		return s.getObjectRowScoped(ctx, gk, id)
	})
}

// ObjectsUpdateSpecByName replaces the spec of whatever holds name within gk. No
// ErrWrongKind: the kind is in the WHERE, so a foreign name is simply absent.
func (s *sqliteStore) ObjectsUpdateSpecByName(ctx context.Context, gk storeapi.GroupKind, name string, spec []byte, specVersion int) (*storeapi.RawObject, bool, error) {
	return s.updateSpec(ctx, spec, specVersion, func(ctx context.Context) (*storeapi.RawObject, error) {
		return s.getObjectRowByName(ctx, gk, name)
	})
}

// updateSpec is the read-compare-write body both spec mutators share. The read
// is required: the no-op skip compares stored bytes, and a bare UPDATE has
// nothing to compare against. Within (BEGIN IMMEDIATE, one connection) is what
// makes keying the UPDATE on the resolved id safe.
func (s *sqliteStore) updateSpec(
	ctx context.Context,
	spec []byte,
	specVersion int,
	resolve func(context.Context) (*storeapi.RawObject, error),
) (*storeapi.RawObject, bool, error) {
	var result *storeapi.RawObject
	var changed bool
	err := s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		obj, err := resolve(ctx)
		if err != nil {
			return err
		}
		// Never downward — see stampVersion.
		stamp, err := stampVersion(obj.SpecVersion, specVersion)
		if err != nil {
			return err
		}
		// Identical spec *at the same schema version*: write nothing — a bump would
		// falsely unsettle a converged object and show watchers a spurious diff.
		// The version gate is what makes the byte compare meaningful: bytes in a
		// different shape can carry different values, so shape disagreement falls
		// through as the real change it may be, generation bump included — at most
		// one spurious reconcile per row per version bump.
		if stamp == obj.SpecVersion && bytes.Equal(obj.Spec, spec) {
			result, err = s.attachConditions(ctx, obj)
			return err // changed stays false: nothing was written
		}
		rv, err := nextResourceVersion(ctx, c)
		if err != nil {
			return err
		}
		// A real spec change bumps generation. Keyed on id alone: the kind boundary
		// came from the resolve above, in this same transaction — keep the read if
		// you move this statement.
		row := c.QueryRowContext(ctx, `
			UPDATE objects
			SET spec = ?, schema_version_spec = ?, generation = generation + 1,
			    resource_version = ?, updated_at = ?
			WHERE id = ?
			RETURNING `+objectColumns,
			jsonText(spec), stamp, rv, toMillis(time.Now().UTC()), obj.ID)
		result, err = s.scanWritten(ctx, row)
		changed = err == nil
		return err
	})
	return result, changed, err
}

// ObjectsUpdateStatus skips the status write when the incoming bytes equal the stored
// ones at the same schema version: no resource_version bump, so no spurious
// watch diff or dependent wake. A content no-op that advances the generation
// handshake still writes observed_generation/observed_at and bumps
// resource_version — settling at a new generation is a real transition, and it
// fires at most once per generation. Identical status with the generation
// already recorded writes nothing at all.
func (s *sqliteStore) ObjectsUpdateStatus(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, observedGeneration int64, status []byte, statusVersion int) error {
	// Within keeps the read-compare-write atomic.
	return s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		// Scoped read enforces the kind boundary while doubling as the compare's load.
		obj, err := s.getObjectRowScoped(ctx, gk, id)
		if err != nil {
			return err
		}
		// A controller can only have observed a generation that exists; recording a
		// future one would falsely settle the object once its spec caught up. An
		// older value is fine (spec changed mid-reconcile).
		if obj.Generation < observedGeneration {
			return fmt.Errorf("%w: reported %d, current is %d (object %d)",
				storeapi.ErrObservedGenerationFuture, observedGeneration, obj.Generation, id)
		}
		// Never downward — see stampVersion.
		stamp, err := stampVersion(obj.StatusVersion, statusVersion)
		if err != nil {
			return err
		}
		if stamp == obj.StatusVersion && bytes.Equal(obj.Status, status) {
			// Content no-op: write only the bookkeeping, and only if it would move.
			// >=, not ==: a report at or below the recorded generation would roll
			// observed_generation backwards, re-unsettling a converged object. (The
			// content path below deliberately does not clamp: there the stale
			// reporter overwrote the content, and unsettling gets it re-derived.)
			settled := obj.ObservedGeneration != nil && *obj.ObservedGeneration >= observedGeneration
			if settled {
				return nil
			}
			// The handshake advanced — watch-visible even with identical bytes.
			// updated_at tracks content and stays put; observed_at records the
			// handshake.
			rv, err := nextResourceVersion(ctx, c)
			if err != nil {
				return err
			}
			// No RETURNING: no row reported, and the scoped read proved existence.
			_, err = c.ExecContext(ctx, `
				UPDATE objects
				SET observed_generation = ?, observed_at = ?, resource_version = ?
				WHERE id = ?`,
				observedGeneration, toMillis(time.Now().UTC()), rv, id)
			return err
		}
		rv, err := nextResourceVersion(ctx, c)
		if err != nil {
			return err
		}
		now := toMillis(time.Now().UTC())
		// observedGeneration lands verbatim, unclamped: a stale reporter just
		// overwrote the status, and its generation marking the object unsettled is
		// what gets that content re-derived. Keyed on id alone: the kind boundary
		// came from the scoped read in this transaction — keep the read if you move
		// this statement.
		_, err = c.ExecContext(ctx, `
			UPDATE objects
			SET status = ?, schema_version_status = ?, observed_generation = ?, observed_at = ?,
			    resource_version = ?, updated_at = ?
			WHERE id = ?`,
			jsonText(status), stamp, observedGeneration, now, rv, now, id)
		return err
	})
}

// conditionColumns is the canonical select list for a condition row;
// scanCondition reads them in order. object_id leads so one scan serves both the
// single-object and batched reads.
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

// scanCondition decodes one condition row (conditionColumns order). The liveness
// downgrade is applied by read-path callers, not here, so getCondition's no-op
// comparison sees stored truth.
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
// this process started — valid only in the writing process, so it reads as
// "verifying" until re-confirmed. Store-truth conditions are never stale.
func (s *sqliteStore) livenessStale(cond *storeapi.Condition) bool {
	return cond.Liveness && cond.UpdatedAt.Before(s.processStart)
}

// downgradeLiveness surfaces a stale liveness condition as Unknown on the read
// path — not in getCondition, whose no-op comparison must see stored truth.
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

// bumpObject advances id's resource_version — the visibility half of the
// condition mutators, whose semantic write lives in another table.
func (s *sqliteStore) bumpObject(ctx context.Context, c dbtx, id storeapi.ObjectID) error {
	rv, err := nextResourceVersion(ctx, c)
	if err != nil {
		return err
	}
	_, err = c.ExecContext(ctx, `
		UPDATE objects SET resource_version = ?, updated_at = ?
		WHERE id = ?`, rv, toMillis(time.Now().UTC()), id)
	return err
}

// conditionUnchanged reports whether an existing condition already matches the
// proposed write — the no-op case.
func (s *sqliteStore) conditionUnchanged(existing *storeapi.Condition, want storeapi.Condition) bool {
	if existing == nil {
		return false
	}
	// A stale liveness re-confirmation must NOT be suppressed: the write refreshes
	// updated_at and clears the downgrade; skipping it would pin Unknown forever.
	if s.livenessStale(existing) {
		return false
	}
	return existing.Status == want.Status &&
		existing.Reason == want.Reason &&
		existing.Message == want.Message &&
		existing.Liveness == want.Liveness
}

func (s *sqliteStore) ConditionsSet(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, cond storeapi.Condition) error {
	// Within keeps the condition write and the object's version bump atomic.
	return s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		// Metadata-only gate: clean ErrNotFound/ErrWrongKind instead of an FK
		// violation, and no row read on a path that reports none.
		if err := s.checkObjectScoped(ctx, gk, id); err != nil {
			return err
		}
		// No-op suppression: an identical condition leaves resource_version alone.
		existing, err := s.getCondition(ctx, id, cond.Type)
		if err != nil {
			return err
		}
		if s.conditionUnchanged(existing, cond) {
			return nil
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
		// A condition change bumps resource_version — what watch polls and the
		// dependency waker look at.
		return s.bumpObject(ctx, c, id)
	})
}

func (s *sqliteStore) ConditionsDelete(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, condType string) error {
	// Within keeps the delete and the version bump atomic (see ConditionsSet).
	return s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		// Same metadata-only gate as ConditionsSet.
		if err := s.checkObjectScoped(ctx, gk, id); err != nil {
			return err
		}
		res, err := c.ExecContext(ctx,
			`DELETE FROM conditions WHERE object_id = ? AND type = ?`, id, condType)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected() // modernc caches the count; RowsAffected never errors
		// Absent condition: nothing changed, no bump.
		if n == 0 {
			return nil
		}
		return s.bumpObject(ctx, c, id)
	})
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
// timeline is empty.
//
// ORDER BY id, not last_at: last_at moves on every extend, so a backwards clock
// step could name an older run latest — and EventsAdd would extend a run the log
// has moved past. idx_events_latest serves this order.
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

// latestEventKey returns just the run key of the newest run for (id, category),
// ok=false on an empty timeline. EventsAdd needs only the key; decoding columns
// it would discard would let a decode fault mask the write.
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
	// Within serializes read-latest-then-write so the run-boundary decision can't race.
	var result *storeapi.Event
	err := s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		// Scoped read enforces the kind boundary; events carries no group/kind to fold in.
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

// EventsMaxVersion reads the high-water mark of id's event log — a covering seek
// on idx_events_object_rv, NULL reading as 0. That index exists for this read
// alone; without it the plan fetches one table row per run, past overflow chains
// (TestEventsMaxVersionUsesCoveringIndex pins the plan).
func (s *sqliteStore) EventsMaxVersion(ctx context.Context, id storeapi.ObjectID) (int64, error) {
	var rv sql.NullInt64
	err := s.conn(ctx).QueryRowContext(ctx,
		`SELECT MAX(resource_version) FROM events WHERE object_id = ?`, id).Scan(&rv)
	return rv.Int64, err
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
			// Rank runs newest-first per (object, category) and drop past the cap.
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

func (s *sqliteStore) FinalizersDelete(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, finalizer string) error {
	// Within keeps the read-modify-write of the finalizer list atomic.
	return s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		// Scoped read enforces the kind boundary while loading the finalizer list.
		obj, err := s.getObjectRowScoped(ctx, gk, id)
		if err != nil {
			return err
		}
		remaining, removed := removeFinalizer(obj.Finalizers, finalizer)
		// Absent finalizer: nothing changed, no bump.
		if !removed {
			return nil
		}
		rv, err := nextResourceVersion(ctx, c)
		if err != nil {
			return err
		}
		// No RETURNING: no row reported, and the scoped read proved existence.
		_, err = c.ExecContext(ctx, `
			UPDATE objects SET finalizers = ?, resource_version = ?, updated_at = ?
			WHERE id = ?`,
			jsonText(marshalFinalizers(remaining)), rv, toMillis(time.Now().UTC()), id)
		return err
	})
}

// markForDeletion stamps the deletion clock of the row named by where, once: the
// IS NULL guard makes a repeat a no-op. where is a compile-time fragment, never
// user input, parenthesized before the guard so a disjunctive key cannot escape
// the IS NULL. It reports only whether it stamped; requestDeletion's probe
// disambiguates a zero-row result (guard, scope or missing).
//
// The version is drawn lazily — calling nextResourceVersion first would make
// every repeat delete commit a counter write to stamp nothing. The inline
// `value + 1` matches the later draw exactly: same transaction, one connection.
// The subquery tolerates a multi-row match only because every where here keys on
// a unique column.
func (s *sqliteStore) markForDeletion(ctx context.Context, where string, whereArgs ...any) (bool, error) {
	c := s.conn(ctx)
	now := toMillis(time.Now().UTC())
	args := append([]any{now, now}, whereArgs...)
	res, err := c.ExecContext(ctx, `
		UPDATE objects
		SET deletion_requested_at = ?,
		    resource_version = (SELECT value + 1 FROM resource_version_seq WHERE id = 1),
		    updated_at = ?
		WHERE (`+where+`) AND deletion_requested_at IS NULL`, args...)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected() // modernc caches the count; RowsAffected never errors
	if n == 0 {
		return false, nil
	}
	// Commit the value the row above just took; same transaction, same connection.
	if _, err := nextResourceVersion(ctx, c); err != nil {
		return false, err
	}
	return true, nil
}

// probeDeletionByName is probeObjectScoped keyed by name; a name this kind does
// not hold is absent, not foreign. Saves the blob copies and finalizer unmarshal
// of a full row read.
func (s *sqliteStore) probeDeletionByName(ctx context.Context, gk storeapi.GroupKind, name string) (bool, error) {
	var deletionAt sql.NullInt64
	err := s.conn(ctx).QueryRowContext(ctx,
		`SELECT deletion_requested_at FROM objects WHERE "group" = ? AND kind = ? AND name = ?`,
		gk.Group, gk.Kind, name).Scan(&deletionAt)
	if errors.Is(err, sql.ErrNoRows) {
		return false, storeapi.ErrNotFound
	}
	if err != nil {
		return false, err
	}
	return deletionAt.Valid, nil
}

// requestDeletion is the probe-then-mark protocol behind both deletion entry
// points. The probe runs first and lock-free, so the idempotent outcomes —
// absent, or already pending, the steady state — answer without BEGIN IMMEDIATE.
// It is advisory: the mark's IS NULL guard re-checks everything, and a zero-row
// mark re-probes inside the transaction to tell a foreign id from a collected one.
func (s *sqliteStore) requestDeletion(
	ctx context.Context,
	probe func(context.Context) (pending bool, err error),
	where string, whereArgs ...any,
) (bool, error) {
	if pending, err := probe(ctx); err != nil || pending {
		return false, err
	}
	var changed bool
	err := s.Within(ctx, func(ctx context.Context) error {
		var err error
		if changed, err = s.markForDeletion(ctx, where, whereArgs...); err != nil || changed {
			return err
		}
		_, err = probe(ctx)
		return err
	})
	return changed, err
}

// DeletionRequestsCreate marks id within gk. The kind is folded into the write, so a
// foreign id matches no row and the probe reports ErrWrongKind.
func (s *sqliteStore) DeletionRequestsCreate(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID) (bool, error) {
	return s.requestDeletion(ctx,
		func(ctx context.Context) (bool, error) { return s.probeObjectScoped(ctx, gk, id) },
		`id = ? AND "group" = ? AND kind = ?`, id, gk.Group, gk.Kind)
}

// DeletionRequestsCreateByName marks the gk row holding name; the resolve and the mark
// are one statement.
func (s *sqliteStore) DeletionRequestsCreateByName(ctx context.Context, gk storeapi.GroupKind, name string) (bool, error) {
	return s.requestDeletion(ctx,
		func(ctx context.Context) (bool, error) { return s.probeDeletionByName(ctx, gk, name) },
		`"group" = ? AND kind = ? AND name = ?`, gk.Group, gk.Kind, name)
}

// DeletionRequestsCreateFromOwner cascades deletion to ownerID's owned children,
// returning every owned child for requeue, deleting or not. A re-cascade over an
// already-deleting subtree is a lone SELECT.
func (s *sqliteStore) DeletionRequestsCreateFromOwner(ctx context.Context, ownerID storeapi.ObjectID) ([]storeapi.ObjectRef, error) {
	// Self-wrapped: several children each draw a version, and publication is in
	// commit order only inside Within.
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
		WHERE r.to_id = ? AND r.relation = ?`+edgeOrderByReferrer, ownerID, string(storeapi.RelationOwnedBy))
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
		// All columns scan without error (NOT NULL, or nullable INTEGER -> *int64).
		_ = rows.Scan(&ch.ref.ID, &ch.ref.Group, &ch.ref.Kind, &delAt)
		ch.deleting = delAt != nil
		children = append(children, ch)
	}
	// modernc buffers the whole result set on the first Next; no late failure.
	_ = rows.Err()
	rows.Close() // free the single-conn pool before the per-child writes below

	out := make([]storeapi.ObjectRef, 0, len(children))
	for _, ch := range children {
		out = append(out, ch.ref)
		if ch.deleting {
			continue // already deletion-pending: nothing to stamp
		}
		// A race may have set the flag since the SELECT; the guard then stamps
		// nothing — benign, so only the error matters.
		if _, err := s.markForDeletion(ctx, `id = ?`, ch.ref.ID); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *sqliteStore) ObjectsDelete(ctx context.Context, id storeapi.ObjectID) error {
	// Self-wrapped: the cascade to conditions, events and edges goes together or
	// not at all.
	return s.Within(ctx, func(ctx context.Context) error { return s.objectsDelete(ctx, id) })
}

// objectsDelete physically removes the row. A delete draws no resource_version:
// the row is gone, so there is nothing for a write-log scan to report — watches
// derive the tombstone from absence.
func (s *sqliteStore) objectsDelete(ctx context.Context, id storeapi.ObjectID) error {
	// Zero rows means already collected: ErrNotFound. Conditions, events and edges cascade.
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

// edgeIsNew is the "this call creates the edge" test as a WHERE fragment — a
// probe down the edges primary key (the table itself, WITHOUT ROWID). A const so
// the two EdgesAdd statements gated on it cannot drift.
const edgeIsNew = `NOT EXISTS (
	SELECT 1 FROM edges WHERE from_id = ? AND to_id = ? AND relation = ?)`

// EdgesAdd inserts a (from_id, to_id, relation) edge, stamping an owed dependency
// wake for every new depends_on edge it creates (see storeapi.Store.EdgesAdd). It
// does not bump resource_version — a ref is not a field of the object. It
// self-wraps in Within, so the endpoint check, stamp and insert are one atomic
// unit however it is called.
//
// The insert is deliberately the *last* write, so any residual failure lands the
// harmless way: a stamp with no edge is one spurious wake that drains; an edge
// with no stamp is a dependent stranded on a stale read that ObjectsListUnsettledIDs
// structurally cannot see.
func (s *sqliteStore) EdgesAdd(ctx context.Context, fromID, toID storeapi.ObjectID, relation storeapi.Relation) (storeapi.EdgesAddResult, error) {
	var out storeapi.EdgesAddResult
	err := s.Within(ctx, func(ctx context.Context) error {
		// One round-trip, no blobs. A join, not scalar subqueries: SQLite does no
		// CSE, so separate subqueries would seek the same row twice. A missing
		// endpoint yields no row — clean ErrNotFound over an FK violation.
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
		// The durable wake stamp, before the insert (see the func doc). Only
		// recorded owed work survives every interleaving: a stamp landing mid-pass
		// sits above the count the pass observed at load, so the decrement cannot
		// consume it. The edge-new test bounds the cost at one pass per edge ever
		// created; self-edges are skipped as every scan skips them. One predicate
		// for both writes below, so an edit cannot split the stamp from the clear.
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
			// modernc caches the count and cannot fail here. Worth knowing if the
			// driver changes: a wrong count here is a silently skipped wake,
			// permanent and invisible.
			n, _ := res.RowsAffected()
			stamped = n > 0
		}
		// A new depends_on edge also drops the dependent's watermark: it was
		// recorded over a smaller dependency set and cannot speak for the new
		// target. The stamp above is the guarantee — this clear can be undone by a
		// pass in flight — but a false watermark would misreport convergence to
		// DependentsListStale until the owed pass. Same edge-new gate; self-edges
		// skipped to match DependentsListStale.
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
		WHERE r.to_id = ? AND r.relation = ?`+edgeOrderByReferrer, toID, string(relation))
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

// idChunkSize bounds the ids bound per batched query, under SQLite's
// SQLITE_MAX_VARIABLE_NUMBER (32766 in modernc). A var so tests can shrink it to
// exercise the multi-chunk merge.
var idChunkSize = 30000

// edgesByIDs is the shared batched edge lookup: edges filtered by routeCol IN
// (ids), joined on joinCol, bucketed by routeCol. The column names are fixed
// internal strings, never user input. Chunked under idChunkSize.
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

// edgesByIDsChunk runs one chunk, merging rows into out; it closes its result
// set so the next chunk can run on the single connection.
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
		ORDER BY r.`+routeCol+`, r.`+joinCol, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var route storeapi.ObjectID
		var d storeapi.ObjectRef
		// INTEGER/TEXT NOT NULL columns; the scan never fails.
		_ = rows.Scan(&route, &d.ID, &d.Group, &d.Kind)
		out[route] = append(out[route], d)
	}
	return rows.Err()
}

// EdgesListOutgoing returns the distinct objects fromID points at (any
// relation); DISTINCT collapses an object reached through several relations.
func (s *sqliteStore) EdgesListOutgoing(ctx context.Context, fromID storeapi.ObjectID) ([]storeapi.ObjectRef, error) {
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT DISTINCT o.id, o."group", o.kind
		FROM edges r JOIN objects o ON o.id = r.to_id
		WHERE r.from_id = ?`+edgeOrderByTarget, fromID)
	if err != nil {
		return nil, err
	}
	return scanObjectRefs(rows)
}

// EdgesListOutgoingByRelation returns the objects fromID points at through relation. No
// DISTINCT needed: (from_id, to_id, relation) is unique.
func (s *sqliteStore) EdgesListOutgoingByRelation(ctx context.Context, fromID storeapi.ObjectID, relation storeapi.Relation) ([]storeapi.ObjectRef, error) {
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT o.id, o."group", o.kind
		FROM edges r JOIN objects o ON o.id = r.to_id
		WHERE r.from_id = ? AND r.relation = ?`+edgeOrderByTarget, fromID, string(relation))
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

// scanObjectRefs collects an (id, group, kind) SELECT into ObjectRefs, closing
// rows. The columns never fail to scan, and modernc's buffered result set leaves
// rows.Err clean after a good query.
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

// EdgesDeleteFinalizingDependsOn removes depends_on edges into toID whose source is
// itself deletion-pending, breaking the mutual-RESTRICT deadlock between
// finalizing objects. Bumps no version.
func (s *sqliteStore) EdgesDeleteFinalizingDependsOn(ctx context.Context, toID storeapi.ObjectID) error {
	_, err := s.conn(ctx).ExecContext(ctx, `
		DELETE FROM edges
		WHERE to_id = ? AND relation = ?
		  AND from_id IN (SELECT id FROM objects WHERE deletion_requested_at IS NOT NULL)`,
		toID, string(storeapi.RelationDependsOn))
	return err
}

// EdgesHasIncoming reports whether any object with a live claim points at id. A
// depends_on edge from a deletion-pending source is ignored — otherwise two
// finalizing objects depending on each other would never clear. owned_by always
// counts: the foreground cascade waits for physical removal.
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

// DriverCursorsGet reads name's persisted cursor (see storeapi.DriverCursorer). A
// missing row is ok=false: absence is the ordinary first-run state, not a fault.
func (s *sqliteStore) DriverCursorsGet(ctx context.Context, name string) (int64, bool, error) {
	var cursor int64
	err := s.conn(ctx).QueryRowContext(ctx,
		`SELECT cursor FROM driver_cursors WHERE name = ?`, name).Scan(&cursor)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return cursor, true, nil
}

// DriverCursorsSet upserts name's persisted cursor (see storeapi.DriverCursorer).
// The WHERE on DO UPDATE keeps the cursor monotonic and suppresses a no-advance
// write outright — no page dirtied on a quiet tick.
func (s *sqliteStore) DriverCursorsSet(ctx context.Context, name string, cursor int64) error {
	_, err := s.conn(ctx).ExecContext(ctx, `
		INSERT INTO driver_cursors (name, cursor, updated_at) VALUES (?, ?, ?)
		    ON CONFLICT(name) DO UPDATE
		   SET cursor = excluded.cursor, updated_at = excluded.updated_at
		 WHERE excluded.cursor > driver_cursors.cursor`,
		name, cursor, toMillis(time.Now().UTC()))
	return err
}

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

// scanObject reads objectColumns into a RawObject. extra binds columns the
// statement appended after objectColumns (positional — extras go last in both
// places).
func scanObject(sc scanner, extra ...any) (*storeapi.RawObject, error) {
	var (
		obj         storeapi.RawObject
		status      []byte
		observedGen sql.NullInt64
		observedAt  sql.NullInt64
		deletionAt  sql.NullInt64
		finalizers  []byte
		createdAt   int64
		updatedAt   int64
	)
	dest := []any{
		&obj.ID, &obj.Group, &obj.Kind, &obj.Name, &obj.Spec, &status,
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

// scanObjects collects every row of an objectColumns SELECT, closing rows.
// modernc materializes the result set on the first Next, so the tail rows.Err is
// the only reachable error site.
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

// removeFinalizer returns f without target and whether target was present.
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
