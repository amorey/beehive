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

var _ storeapi.Store = (*sqliteStore)(nil)

// sqliteConditions is the conditions family. Embedding the store keeps every
// method body unchanged: s.db still resolves.
type sqliteConditions struct{ *sqliteStore }

func (s *sqliteStore) Conditions() storeapi.Conditions { return sqliteConditions{s} }

type sqliteReconcileOwed struct{ *sqliteStore }

func (s *sqliteStore) ReconcileOwed() storeapi.ReconcileOwed { return sqliteReconcileOwed{s} }

type sqliteDeletionRequests struct{ *sqliteStore }

func (s *sqliteStore) DeletionRequests() storeapi.DeletionRequests { return sqliteDeletionRequests{s} }

type sqliteEvents struct{ *sqliteStore }

func (s *sqliteStore) Events() storeapi.Events { return sqliteEvents{s} }

type sqliteEdges struct{ *sqliteStore }

func (s *sqliteStore) Edges() storeapi.Edges { return sqliteEdges{s} }

type sqliteObjectWrites struct{ *sqliteStore }

func (s *sqliteStore) ObjectWrites() storeapi.ObjectWrites { return sqliteObjectWrites{s} }

type sqliteObjects struct{ *sqliteStore }

func (s *sqliteStore) Objects() storeapi.Objects { return sqliteObjects{s} }

type sqliteDependencies struct{ *sqliteStore }

func (s *sqliteStore) Dependencies() storeapi.Dependencies { return sqliteDependencies{s} }

type sqliteDriverCursors struct{ *sqliteStore }

func (s *sqliteStore) DriverCursors() storeapi.DriverCursors { return sqliteDriverCursors{s} }

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

// ReclaimSpace hands up to maxPages of the freelist back to the OS with
// PRAGMA incremental_vacuum, reporting how many pages left the file. It releases
// nothing, without error, under the drain floor or on an auto_vacuum=NONE
// database. The count is advisory: a difference of two reads, logged, never
// acted on.
//
// The pragma frees one page per step, so it must be Exec'd, never Query'd —
// Query releases exactly one page and reports no error.
func (s *sqliteStore) ReclaimSpace(ctx context.Context, maxPages int) (int, error) {
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

// freePagesRelease is ReclaimSpace once the connection is in hand, split out
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

// nextResourceVersion advances and returns the global write cursor.
func nextResourceVersion(ctx context.Context, c dbtx) (int64, error) {
	return advanceResourceVersion(ctx, c, 1)
}

// advanceResourceVersion advances the cursor by n and returns the highest value
// drawn, so the range taken is [value-n+1, value]. n must be positive. A
// standalone counter, not MAX(objects.resource_version): deleting the
// highest-versioned row must never regress the cursor and hand out a reused
// version.
func advanceResourceVersion(ctx context.Context, c dbtx, n int) (int64, error) {
	var rv int64
	err := c.QueryRowContext(ctx,
		`UPDATE resource_version_seq SET value = value + ? WHERE id = 1 RETURNING value`, n).Scan(&rv)
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

func (s sqliteObjects) Create(ctx context.Context, gk storeapi.GroupKind, in storeapi.ObjectsCreateInput) (*storeapi.RawObject, error) {
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

// recordObjectWrite draws the version an object write will take and logs it in
// one step, returning the version and the timestamp the caller stamps its row
// with. Bound together deliberately: drawing a version without logging it is
// exactly the mistake that makes a write invisible to every watch, and the
// caller cannot write its row without the value this returns.
//
// For callers that know the row up front. objectsCreate and markForDeletion
// learn the id from their own RETURNING, and objectsDelete needs the row image,
// so those three call appendWriteLog directly.
func (s *sqliteStore) recordObjectWrite(
	ctx context.Context,
	c dbtx,
	gk storeapi.GroupKind,
	id storeapi.ObjectID,
	op int,
) (rv, now int64, err error) {
	if rv, err = nextResourceVersion(ctx, c); err != nil {
		return 0, 0, err
	}
	now = toMillis(time.Now().UTC())
	if err = appendWriteLog(ctx, c, id, gk, op, rv, now); err != nil {
		return 0, 0, err
	}
	return rv, now, nil
}

// objectWritesColumns is the log's insert column list, less the delete-only final.
const objectWritesColumns = `resource_version, object_id, "group", kind, op, written_at`

// appendWriteLog records one committed object write. Callers pass the version the
// write took, so the entry orders against the row it describes.
func appendWriteLog(ctx context.Context, c dbtx, id storeapi.ObjectID, gk storeapi.GroupKind, op int, rv, now int64) error {
	_, err := c.ExecContext(ctx,
		`INSERT INTO object_writes (`+objectWritesColumns+`) VALUES (?, ?, ?, ?, ?, ?)`,
		rv, id, gk.Group, gk.Kind, op, now)
	return err
}

// loggedWrite is one row of a batched write log append: the identity and the
// version a single write took.
type loggedWrite struct {
	id storeapi.ObjectID
	gk storeapi.GroupKind
	rv int64
}

// appendWriteLogUpdates records one update entry per write in a single INSERT.
// Each carries the version its own row took: a batch shares a draw, never a value.
func appendWriteLogUpdates(ctx context.Context, c dbtx, writes []loggedWrite, now int64) error {
	if len(writes) == 0 {
		return nil
	}
	args := make([]any, 0, len(writes)*6)
	for _, w := range writes {
		// The soft delete is an update: the row is still live and readable.
		args = append(args, w.rv, w.id, w.gk.Group, w.gk.Kind, writeOpUpdate, now)
	}
	_, err := c.ExecContext(ctx,
		`INSERT INTO object_writes (`+objectWritesColumns+`) VALUES `+tupleRows(len(writes), 6), args...)
	return err
}

// appendWriteLogDelete records a collection, carrying the row image a Deleted
// change reports. image is stamped with the delete's own version: it is the
// object's last, and the row that held the previous one no longer exists.
func appendWriteLogDelete(ctx context.Context, c dbtx, image *storeapi.RawObject, rv, now int64) error {
	image.ResourceVersion = rv
	final, _ := json.Marshal(image) // plain data: no channel, func or cyclic field can fail it
	_, err := c.ExecContext(ctx, `
		INSERT INTO object_writes (resource_version, object_id, "group", kind, op, written_at, final)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		rv, image.ID, image.Group, image.Kind, writeOpDelete, now, string(final))
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

// selectScoped reads id's row through the kind gate, binding only the columns
// the caller names into dest, in order; empty cols is the gate alone. The one
// place ErrNotFound and ErrWrongKind are decided for a write that needs part of
// a row rather than all of it.
func (s *sqliteStore) selectScoped(
	ctx context.Context,
	gk storeapi.GroupKind,
	id storeapi.ObjectID,
	cols string,
	dest ...any,
) error {
	q := `SELECT "group", kind FROM objects WHERE id = ?`
	if cols != "" {
		q = `SELECT "group", kind, ` + cols + ` FROM objects WHERE id = ?`
	}
	var group, kind string
	err := s.conn(ctx).QueryRowContext(ctx, q, id).
		Scan(append([]any{&group, &kind}, dest...)...)
	if errors.Is(err, sql.ErrNoRows) {
		return storeapi.ErrNotFound // bare, like scanObject's
	}
	if err != nil {
		return err
	}
	return gateKind(gk, id, group, kind)
}

// gateKind reports whether the row's own group/kind is gk's: ErrWrongKind if not.
func gateKind(gk storeapi.GroupKind, id storeapi.ObjectID, group, kind string) error {
	if group != gk.Group || kind != gk.Kind {
		return fmt.Errorf("%w: object %d is %s/%s, not %s/%s",
			storeapi.ErrWrongKind, id, group, kind, gk.Group, gk.Kind)
	}
	return nil
}

// probeObjectScoped answers "does id exist, is it gk's, and is it
// deletion-pending?" from three columns — no blobs, no finalizer unmarshal. Same
// errors as a scoped read: ErrNotFound, ErrWrongKind.
func (s *sqliteStore) probeObjectScoped(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID) (deletionPending bool, err error) {
	var deletionAt sql.NullInt64
	if err := s.selectScoped(ctx, gk, id, `deletion_requested_at`, &deletionAt); err != nil {
		return false, err
	}
	return deletionAt.Valid, nil
}

// checkObjectExists is probeObjectScoped without the kind gate: ErrNotFound, or
// nil.
func (s *sqliteStore) checkObjectExists(ctx context.Context, id storeapi.ObjectID) error {
	var one int
	err := s.conn(ctx).QueryRowContext(ctx, `SELECT 1 FROM objects WHERE id = ?`, id).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return storeapi.ErrNotFound // bare, like scanObject's
	}
	return err
}

// checkObjectScoped is probeObjectScoped for callers that only need the gate.
func (s *sqliteStore) checkObjectScoped(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID) error {
	return s.selectScoped(ctx, gk, id, ``)
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
	if err := gateKind(gk, id, obj.Group, obj.Kind); err != nil {
		return nil, err
	}
	return obj, nil
}

func (s sqliteObjects) Get(ctx context.Context, id storeapi.ObjectID) (*storeapi.RawObject, error) {
	obj, err := s.getObjectRow(ctx, id)
	if err != nil {
		return nil, err
	}
	return s.attachConditions(ctx, obj)
}

// ObjectsGetForReconcile is the reconcile loop's opening read (see the contract
// on storeapi.Store). The cursor and the dependency flag are correlated
// subqueries, riding the row read rather than adding round trips.
func (s sqliteObjects) GetForReconcile(ctx context.Context, id storeapi.ObjectID) (storeapi.ReconcileLoad, error) {
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
func (s sqliteObjects) GetMeta(ctx context.Context, id storeapi.ObjectID) (*storeapi.RawObject, error) {
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

func (s sqliteObjects) GetByName(ctx context.Context, gk storeapi.GroupKind, name string) (*storeapi.RawObject, error) {
	obj, err := s.getObjectRowByName(ctx, gk, name)
	if err != nil {
		return nil, err
	}
	return s.attachConditions(ctx, obj)
}

func (s sqliteObjects) List(ctx context.Context, gk storeapi.GroupKind) ([]*storeapi.RawObject, error) {
	return s.listObjectsWhere(ctx, `WHERE o."group" = ? AND o.kind = ?`, gk.Group, gk.Kind)
}

// ObjectsListByIncomingEdge returns the full rows of the objects pointing at
// toID through relation, restricted to gk — the blob-bearing EdgesListIncoming.
//
// The edge is a semi-join, not a join: IN (SELECT …) lets idx_edges_to drive, so
// the work scales with the referrers rather than every object of the kind.
func (s sqliteObjects) ListByIncomingEdge(ctx context.Context, gk storeapi.GroupKind, toID storeapi.ObjectID, relation storeapi.Relation) ([]*storeapi.RawObject, error) {
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

func (s sqliteObjects) ListUnsettledIDs(ctx context.Context, gk storeapi.GroupKind) ([]storeapi.ObjectID, error) {
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

func (s sqliteDeletionRequests) List(ctx context.Context) ([]storeapi.ObjectRef, error) {
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

func (s sqliteReconcileOwed) ListIDs(ctx context.Context, gk storeapi.GroupKind) ([]storeapi.ObjectID, error) {
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

// ResourceVersionsMaxIssued reads the sequence itself (contract on
// storeapi.Store). One row, always present: the migration seeds it.
func (s *sqliteStore) ResourceVersionsMaxIssued(ctx context.Context) (int64, error) {
	var rv int64
	err := s.conn(ctx).QueryRowContext(ctx,
		`SELECT value FROM resource_version_seq WHERE id = 1`).Scan(&rv)
	return rv, err
}

// Stamp stamps a page of findings in one statement (contract on
// storeapi.Store). A missing id matches no row, which is how a vanished
// dependent is skipped; a repeated id matches its row once, which is the fold
// the contract requires.
func (s sqliteReconcileOwed) Stamp(ctx context.Context, refs []storeapi.ObjectRef) error {
	if len(refs) == 0 {
		return nil
	}
	args := make([]any, len(refs))
	for i, ref := range refs {
		args[i] = ref.ID
	}
	_, err := s.conn(ctx).ExecContext(ctx,
		`UPDATE objects SET reconcile_owed = reconcile_owed + 1
		  WHERE id IN (`+placeholders(len(refs))+`)`, args...)
	return err
}

// reconcileOwedSweepQuery builds the reclaim. The test that pins its query plan
// calls this too, so the plan it pins is the one that runs.
func reconcileOwedSweepQuery(keep []storeapi.GroupKind) (string, []any) {
	// Matches the partial index idx_objects_reconcile_owed WHERE reconcile_owed != 0.
	q := `UPDATE objects SET reconcile_owed = 0 WHERE reconcile_owed != 0`
	if len(keep) == 0 {
		return q, nil // NOT IN (VALUES) is a syntax error, and nothing is kept
	}
	values, args := kindTuples(keep)
	return q + ` AND ("group", kind) NOT IN (VALUES ` + values + `)`, args
}

// ReconcileOwedSweep zeroes the owed count outside keep in one no-emit UPDATE
// (contract on storeapi.Store).
func (s sqliteReconcileOwed) Sweep(ctx context.Context, keep []storeapi.GroupKind) (int, error) {
	q, args := reconcileOwedSweepQuery(keep)
	res, err := s.conn(ctx).ExecContext(ctx, q, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected() // modernc caches the count; RowsAffected never errors
	return int(n), nil
}

// ReconcileOwedIncrement and ReconcileOwedDecrement are single no-emit UPDATEs
// on the owed-wake count; the decrement floors at 0 (contract on storeapi.Store).
//
// The increment is deliberately not on that interface: production wakes come
// from EdgesAdd, whose stamp must be indivisible from the edge insert, and from
// ReconcileOwedStamp. It stays here so tests can seed a count.
func (s *sqliteStore) ReconcileOwedIncrement(ctx context.Context, id storeapi.ObjectID) error {
	_, err := s.conn(ctx).ExecContext(ctx,
		`UPDATE objects SET reconcile_owed = reconcile_owed + 1 WHERE id = ?`, id)
	return err
}

// ReconcileOwedDecrement folds the kind into the UPDATE, so a foreign id matches
// no row; the disambiguating read is paid only when nothing was written.
func (s sqliteReconcileOwed) Decrement(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, observed int64) error {
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

// DependentsListStaleSince re-derives which dependents are owed a pass, bounded
// by a cursor over target versions (see the contract on storeapi.Store). to_id is
// ON DELETE RESTRICT, so a target always outlives its edges — only the watermark
// side needs a LEFT JOIN.
//
// The CROSS JOINs pin the join order: targets, then incoming edges, then
// dependents. Without them the planner reads the whole graph and the cursor
// buys nothing.
//
// No GROUP BY: a row is one (target, dependent) pair, and the position needs
// both to resume. The kinds list is not chunked; it comes from Register calls,
// not caller data.
func (s sqliteDependencies) ListStaleSince(ctx context.Context, kinds []storeapi.GroupKind, after storeapi.StalePos, through int64, limit int) ([]storeapi.ObjectRef, storeapi.StalePos, error) {
	if len(kinds) == 0 || limit <= 0 {
		return nil, after, nil
	}
	values, kindArgs := kindTuples(kinds)
	args := make([]any, 0, len(kinds)*2+5)
	args = append(args, after.TargetVersion, after.TargetID, after.DependentID, through)
	args = append(args, kindArgs...)
	args = append(args, limit)
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT t.resource_version, t.id, e.from_id, d."group", d.kind
		  FROM objects t
		  CROSS JOIN edges e ON e.to_id = t.id AND e.relation = 'depends_on'
		  CROSS JOIN objects d ON d.id = e.from_id
		  LEFT JOIN dependency_watermarks c ON c.object_id = e.from_id
		 WHERE (t.resource_version, t.id, e.from_id) > (?, ?, ?)
		   AND t.resource_version <= ?
		   AND e.from_id != e.to_id
		   AND (d."group", d.kind) IN (VALUES `+values+`)
		   AND (c.reconciled_against IS NULL OR t.resource_version > c.reconciled_against)
		 ORDER BY t.resource_version, t.id, e.from_id
		 LIMIT ?`, args...)
	if err != nil {
		return nil, after, err
	}
	defer rows.Close()

	var refs []storeapi.ObjectRef
	pos := after
	for rows.Next() {
		var ref storeapi.ObjectRef
		// INTEGER -> int64 and TEXT NOT NULL -> string never fail, as in scanObjectRefs.
		_ = rows.Scan(&pos.TargetVersion, &pos.TargetID, &ref.ID, &ref.Group, &ref.Kind)
		pos.DependentID = ref.ID
		refs = append(refs, ref)
	}
	// On error the caller discards both, so a half-advanced pos costs nothing.
	return refs, pos, rows.Err()
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
func (s sqliteDependencies) WatermarkSet(ctx context.Context, id storeapi.ObjectID, cursor int64) error {
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

func (s sqliteObjects) ListIDs(ctx context.Context, gk storeapi.GroupKind) ([]storeapi.ObjectID, error) {
	rows, err := s.conn(ctx).QueryContext(ctx,
		`SELECT id FROM objects WHERE "group" = ? AND kind = ? ORDER BY id`,
		gk.Group, gk.Kind)
	if err != nil {
		return nil, err
	}
	return scanIDs(rows)
}

// ObjectWritesMaxVersionAll reads the write log's high-water mark across every
// kind, covered by the log's primary key, plus the retention horizon beside it.
// An empty log reads 0.
//
// The mark reads object_writes, not resource_version_seq: the event log draws
// from that sequence too, and a consumer of this pair must not see the cursor
// move for a write it can never be shown. Retention is the only thing that
// lowers it, and the horizon is what says so — one statement, since the caller
// that compares them needs both at one instant.
func (s sqliteObjectWrites) MaxVersionAll(ctx context.Context) (int64, int64, error) {
	var at, trimmed int64
	err := s.conn(ctx).QueryRowContext(ctx,
		`SELECT coalesce((SELECT MAX(resource_version) FROM object_writes), 0), `+writeLogHorizonAll).
		Scan(&at, &trimmed)
	return at, trimmed, err
}

// ObjectWritesListSinceAll returns the log entries above afterRV across every
// kind, in cursor order, at most limit of them, with the retention horizon
// carried as a trailing column every row repeats. Kind-agnostic, since a
// depends_on edge may point at a kind with no controller. No row images: the
// waker routes by id and reads current state, so decoding a collected object
// only to discard it is pure cost.
//
// One statement, and deliberately no transaction — this is the waker's whole
// quiet pass, which runs per commit.
func (s sqliteObjectWrites) ListSinceAll(ctx context.Context, afterRV int64, limit int) ([]storeapi.ObjectWrite, int64, error) {
	if limit <= 0 {
		// Would reach SQLite as "LIMIT -1" (unbounded) or panic in make below.
		return nil, 0, nil
	}
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT `+writeLogColumns+`, `+writeLogHorizonAll+`
		  FROM object_writes
		 WHERE resource_version > ? ORDER BY resource_version LIMIT ?`,
		afterRV, limit)
	if err != nil {
		return nil, 0, err
	}
	return scanWriteLog(rows, limit)
}

// writeLogHorizonAll is the store-wide retention horizon as a scalar subquery:
// the deepest trim over any kind, 0 when nothing has been trimmed.
const writeLogHorizonAll = `coalesce((SELECT MAX(trimmed_through) FROM object_writes_horizon), 0)`

// writeLogColumns is the canonical select list for a log entry; scanWriteLog
// reads them in order. Exactly the columns idx_object_writes_kind carries, so a
// page is answered from the index alone — final is deliberately absent, since
// selecting it forces a table row fetch for EVERY entry, not only the rare
// delete that has one.
const writeLogColumns = `resource_version, object_id, "group", kind, op`

// scanWriteLog collects log entries and the retention horizon the query carries
// as their trailing column. Row images are attached separately, by the one
// caller that reports them.
func scanWriteLog(rows *sql.Rows, limit int) ([]storeapi.ObjectWrite, int64, error) {
	defer rows.Close()
	writes := make([]storeapi.ObjectWrite, 0, writeLogPageCap(limit))
	var trimmed int64
	for rows.Next() {
		var w storeapi.ObjectWrite
		// Six declared columns into their own types; a STRICT schema cannot
		// surprise them.
		_ = rows.Scan(&w.ResourceVersion, &w.ID, &w.Group, &w.Kind, &w.Op, &trimmed)
		writes = append(writes, w)
	}
	return writes, trimmed, rows.Err()
}

// writeLogPage reads one page of gk's log with the retention horizon carried as
// a trailing column, which every row repeats. One function, so a broken read is
// one error rather than a query branch and a scan branch that cannot both happen.
func (s *sqliteStore) writeLogPage(ctx context.Context, gk storeapi.GroupKind, afterRV int64, limit int) ([]storeapi.ObjectWrite, int64, error) {
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT `+writeLogColumns+`,
		       coalesce((SELECT trimmed_through FROM object_writes_horizon
		                  WHERE "group" = ? AND kind = ?), 0)
		  FROM object_writes
		 WHERE "group" = ? AND kind = ? AND resource_version > ?
		 ORDER BY resource_version LIMIT ?`,
		gk.Group, gk.Kind, gk.Group, gk.Kind, afterRV, limit)
	if err != nil {
		return nil, 0, err
	}
	return scanWriteLog(rows, limit)
}

// writeLogPageCap caps the preallocation: a large caller limit must not
// preallocate for rows the store may not have.
func writeLogPageCap(limit int) int { return min(limit, 1024) }

// attachImages fills Final on the delete entries in page, in one query, and does
// nothing when the page has none. op identifies them without reading the blob,
// which is why it is in the covering index.
func (s *sqliteStore) attachImages(ctx context.Context, page []storeapi.ObjectWrite) error {
	var deletes []any
	for _, w := range page {
		if w.Op == storeapi.WriteDelete {
			deletes = append(deletes, w.ResourceVersion)
		}
	}
	if len(deletes) == 0 {
		return nil
	}
	images, err := s.readImages(ctx, deletes)
	if err != nil {
		return err
	}
	// Read back inside the caller's transaction, so every delete in the page is
	// in images: the reachable violation is the NULL readImages rejects.
	for i := range page {
		if page[i].Op == storeapi.WriteDelete {
			page[i].Final = images[page[i].ResourceVersion]
		}
	}
	return nil
}

// readImages decodes the row images stored against the given versions.
func (s *sqliteStore) readImages(ctx context.Context, versions []any) (map[int64]*storeapi.RawObject, error) {
	rows, err := s.conn(ctx).QueryContext(ctx,
		`SELECT resource_version, final FROM object_writes
		  WHERE resource_version IN (`+placeholders(len(versions))+`)`, versions...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	images := make(map[int64]*storeapi.RawObject, len(versions))
	for rows.Next() {
		var rv int64
		var final sql.NullString
		_ = rows.Scan(&rv, &final) // an INTEGER and a nullable TEXT, both declared
		if !final.Valid {
			// The append path writes the image with the entry, so a NULL here is a
			// broken invariant. Failing the read costs one tick; returning the
			// entry without its image costs the delete itself, because the caller
			// drops the change it cannot build and advances its cursor past it.
			return nil, fmt.Errorf("beehive: write log entry %d is a delete with no row image", rv)
		}
		image := &storeapi.RawObject{}
		if err := json.Unmarshal([]byte(final.String), image); err != nil {
			return nil, err
		}
		images[rv] = image
	}
	return images, rows.Err()
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
func (s sqliteObjects) UpdateSpec(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, spec []byte, specVersion int) (*storeapi.RawObject, bool, error) {
	// The scoped read enforces the kind boundary while doubling as the compare's load.
	return s.updateSpec(ctx, spec, specVersion, func(ctx context.Context) (*storeapi.RawObject, error) {
		return s.getObjectRowScoped(ctx, gk, id)
	})
}

// ObjectsUpdateSpecByName replaces the spec of whatever holds name within gk. No
// ErrWrongKind: the kind is in the WHERE, so a foreign name is simply absent.
func (s sqliteObjects) UpdateSpecByName(ctx context.Context, gk storeapi.GroupKind, name string, spec []byte, specVersion int) (*storeapi.RawObject, bool, error) {
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
		gk := storeapi.GroupKind{Group: obj.Group, Kind: obj.Kind}
		rv, now, err := s.recordObjectWrite(ctx, c, gk, obj.ID, writeOpUpdate)
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
			jsonText(spec), stamp, rv, now, obj.ID)
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
func (s sqliteObjects) UpdateStatus(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, observedGeneration int64, status []byte, statusVersion int) error {
	// Within keeps the read-compare-write atomic.
	return s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		// Scoped read enforces the kind boundary while doubling as the compare's load
		// — four columns, not the row: the spec blob and the finalizer list are no
		// part of a status write.
		var (
			generation    int64
			observedGen   sql.NullInt64
			storedVersion int
			storedStatus  []byte
		)
		if err := s.selectScoped(ctx, gk, id,
			`generation, observed_generation, schema_version_status, status`,
			&generation, &observedGen, &storedVersion, &storedStatus); err != nil {
			return err
		}
		// A controller can only have observed a generation that exists; recording a
		// future one would falsely settle the object once its spec caught up. An
		// older value is fine (spec changed mid-reconcile).
		if generation < observedGeneration {
			return fmt.Errorf("%w: reported %d, current is %d (object %d)",
				storeapi.ErrObservedGenerationFuture, observedGeneration, generation, id)
		}
		// Never downward — see stampVersion.
		stamp, err := stampVersion(storedVersion, statusVersion)
		if err != nil {
			return err
		}
		if stamp == storedVersion && bytes.Equal(storedStatus, status) {
			// Content no-op: write only the bookkeeping, and only if it would move.
			// >=, not ==: a report at or below the recorded generation would roll
			// observed_generation backwards, re-unsettling a converged object. (The
			// content path below deliberately does not clamp: there the stale
			// reporter overwrote the content, and unsettling gets it re-derived.)
			settled := observedGen.Valid && observedGen.Int64 >= observedGeneration
			if settled {
				return nil
			}
			// The handshake advanced — watch-visible even with identical bytes.
			// updated_at tracks content and stays put; observed_at records the
			// handshake.
			rv, now, err := s.recordObjectWrite(ctx, c, gk, id, writeOpUpdate)
			if err != nil {
				return err
			}
			// No RETURNING: no row reported, and the scoped read proved existence.
			_, err = c.ExecContext(ctx, `
				UPDATE objects
				SET observed_generation = ?, observed_at = ?, resource_version = ?
				WHERE id = ?`,
				observedGeneration, now, rv, id)
			return err
		}
		rv, now, err := s.recordObjectWrite(ctx, c, gk, id, writeOpUpdate)
		if err != nil {
			return err
		}
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
// downgrade is applied by read-path callers, not here, so the write path's no-op
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
// path — never on the write path, whose no-op comparison must see stored truth.
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

// conditionSetLoad answers ConditionsSet's kind gate and its no-op comparison in
// one statement, keyed on the same object. The condition columns are NULL when
// there is none; status is NOT NULL wherever a row exists, so it marks presence.
// transitioned_at is absent deliberately: the upsert decides it in SQL.
const conditionSetLoad = `
	SELECT o."group", o.kind, c.status, c.reason, c.message, c.liveness, c.updated_at
	  FROM objects o
	  LEFT JOIN conditions c ON c.object_id = o.id AND c.type = ?
	 WHERE o.id = ?`

// loadForConditionSet runs conditionSetLoad: the scope error, then the stored
// condition or nil. Stored truth, undowngraded — see downgradeLiveness.
func (s *sqliteStore) loadForConditionSet(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, condType string) (*storeapi.Condition, error) {
	var (
		group, kind             string
		status, reason, message sql.NullString
		liveness                sql.NullBool
		updatedAt               sql.NullInt64
	)
	err := s.conn(ctx).QueryRowContext(ctx, conditionSetLoad, condType, id).
		Scan(&group, &kind, &status, &reason, &message, &liveness, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, storeapi.ErrNotFound // bare, like scanObject's
	}
	if err != nil {
		return nil, err
	}
	if err := gateKind(gk, id, group, kind); err != nil {
		return nil, err
	}
	if !status.Valid {
		return nil, nil
	}
	return &storeapi.Condition{
		Type:      condType,
		Status:    status.String,
		Reason:    reason.String,
		Message:   message.String,
		Liveness:  liveness.Bool,
		UpdatedAt: fromMillis(updatedAt.Int64),
	}, nil
}

// bumpObject advances id's resource_version — the visibility half of the
// condition mutators, whose semantic write lives in another table.
func (s *sqliteStore) bumpObject(ctx context.Context, c dbtx, gk storeapi.GroupKind, id storeapi.ObjectID) error {
	rv, now, err := s.recordObjectWrite(ctx, c, gk, id, writeOpUpdate)
	if err != nil {
		return err
	}
	_, err = c.ExecContext(ctx, `
		UPDATE objects SET resource_version = ?, updated_at = ?
		WHERE id = ?`, rv, now, id)
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

func (s sqliteConditions) Set(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, cond storeapi.Condition) error {
	// Within keeps the condition write and the object's version bump atomic.
	return s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		// One read: a metadata-only gate — clean ErrNotFound/ErrWrongKind instead of
		// an FK violation, no blob decoded — joined to the stored condition, since
		// no-op suppression needs it and both key on the same object.
		existing, err := s.loadForConditionSet(ctx, gk, id, cond.Type)
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
		return s.bumpObject(ctx, c, gk, id)
	})
}

func (s sqliteConditions) Delete(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, condType string) error {
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
		return s.bumpObject(ctx, c, gk, id)
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
	if err := scanEventInto(sc, &e); err != nil {
		return nil, err
	}
	return &e, nil
}

// scanEventInto decodes one event row into e, with trailing destinations for a
// query that carries extra columns past eventColumns.
func scanEventInto(sc scanner, e *storeapi.Event, extra ...any) error {
	var message sql.NullString
	var firstMs, lastMs int64
	dest := append([]any{&e.ID, &e.ObjectID, &e.Category, &e.Type, &e.Reason,
		&message, &e.Detail, &e.Count, &firstMs, &lastMs, &e.ResourceVersion}, extra...)
	if err := sc.Scan(dest...); err != nil {
		return err
	}
	e.Message = message.String
	e.FirstAt = fromMillis(firstMs)
	e.LastAt = fromMillis(lastMs)
	return nil
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

func (s sqliteEvents) Add(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, in storeapi.EventsAddInput) error {
	// Within serializes read-latest-then-write so the run-boundary decision can't race.
	return s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		// Metadata-only gate: events carries no group/kind to fold in, and an event
		// write reports no row, so it reads none.
		if err := s.checkObjectScoped(ctx, gk, id); err != nil {
			return err
		}
		rv, err := nextResourceVersion(ctx, c)
		if err != nil {
			return err
		}
		now := toMillis(time.Now().UTC())

		latestID, latestType, latestReason, hasLatest, err := s.latestEventKey(ctx, id, in.Category)
		if err != nil {
			return err
		}
		if hasLatest && latestType == in.Type && latestReason == in.Reason {
			// Extend: bump count and window end, re-sample message/detail, advance rv.
			_, err = c.ExecContext(ctx, `
				UPDATE events SET count = count + 1, last_at = ?, message = ?,
					detail = ?, resource_version = ?
				WHERE id = ?`, now, in.Message, jsonText(in.Detail), rv, latestID)
			return err
		}
		// New run (empty timeline or key changed): count 1, point window.
		_, err = c.ExecContext(ctx, `
			INSERT INTO events
				(object_id, category, type, reason, message, detail,
				 count, first_at, last_at, resource_version)
			VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)`,
			id, in.Category, in.Type, in.Reason, in.Message, jsonText(in.Detail), now, now, rv)
		return err
	})
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

func (s sqliteEvents) List(ctx context.Context, id storeapi.ObjectID, q storeapi.EventQuery) ([]storeapi.Event, error) {
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

// EventsSnapshot lists id's runs and reads its log position in one transaction:
// two reads cannot answer "these runs, as of this position" — whichever order
// they run in, a write between them is either delivered twice or dropped.
func (s sqliteEvents) Snapshot(
	ctx context.Context, id storeapi.ObjectID, q storeapi.EventQuery,
) ([]storeapi.Event, int64, error) {
	var runs []storeapi.Event
	var at int64
	err := s.Within(ctx, func(ctx context.Context) error {
		var err error
		if runs, err = s.Events().List(ctx, id, q); err != nil {
			return err
		}
		at, err = s.Events().MaxVersion(ctx, id)
		return err
	})
	if err != nil {
		return nil, 0, err
	}
	return runs, at, nil
}

// EventsListSince pages id's log above afterRV on idx_events_object_rv.
//
// Self-wrapped for ObjectWritesListSince's reason: the page, the horizon and the
// existence probe must describe one instant, or a sweep landing between them
// reports a horizon above rows the page already carried.
func (s sqliteEvents) ListSince(
	ctx context.Context, id storeapi.ObjectID, category *string, afterRV int64, limit int,
) ([]storeapi.Event, int64, error) {
	if limit <= 0 {
		return nil, 0, nil // "LIMIT -1" is unbounded in SQLite
	}
	var runs []storeapi.Event
	var trimmed int64
	err := s.Within(ctx, func(ctx context.Context) error {
		var err error
		if runs, trimmed, err = s.eventPage(ctx, id, category, afterRV, limit); err != nil {
			return err
		}
		if len(runs) > 0 {
			return nil // the rows carried the horizon subquery
		}
		// No rows carried it, so read it on its own. A horizon proves the object
		// was there; without one, an empty page still has to tell "quiet" from
		// "collected" — a log that cascaded with its row is not "no events".
		if trimmed, err = s.eventHorizon(ctx, id, category); err != nil || trimmed > 0 {
			return err
		}
		return s.checkObjectExists(ctx, id)
	})
	if err != nil {
		return nil, 0, err
	}
	return runs, trimmed, nil
}

// eventPage reads one page of id's log above afterRV with the horizon carried as
// a trailing column, which every row repeats — one statement rather than two for
// the common case. An empty page carries nothing, so its caller reads the
// horizon on its own.
func (s *sqliteStore) eventPage(
	ctx context.Context, id storeapi.ObjectID, category *string, afterRV int64, limit int,
) ([]storeapi.Event, int64, error) {
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT `+eventColumns+`,
		       coalesce((SELECT MAX(trimmed_through) FROM events_horizon
		                  WHERE object_id = ?1 AND (?2 IS NULL OR category = ?2)), 0)
		  FROM events
		 WHERE object_id = ?1 AND resource_version > ?3
		 ORDER BY resource_version LIMIT ?4`, id, category, afterRV, limit)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var runs []storeapi.Event
	var trimmed int64
	for rows.Next() {
		var e storeapi.Event
		if err := scanEventInto(rows, &e, &trimmed); err != nil {
			return nil, 0, err
		}
		runs = append(runs, e)
	}
	return runs, trimmed, rows.Err()
}

// eventHorizon is what retention removed from id's category, or from its highest
// timeline when category is nil. No row means nothing has been trimmed.
func (s *sqliteStore) eventHorizon(ctx context.Context, id storeapi.ObjectID, category *string) (int64, error) {
	where, args := `object_id = ?`, []any{id}
	if category != nil {
		where, args = where+` AND category = ?`, append(args, *category)
	}
	var rv sql.NullInt64
	err := s.conn(ctx).QueryRowContext(ctx,
		`SELECT MAX(trimmed_through) FROM events_horizon WHERE `+where, args...).Scan(&rv)
	return rv.Int64, err
}

// EventsMaxVersion reads the high-water mark of id's event log — a covering seek
// on idx_events_object_rv, NULL reading as 0. That index exists for this read
// alone; without it the plan fetches one table row per run, past overflow chains
// (TestEventsMaxVersionUsesCoveringIndex pins the plan).
func (s sqliteEvents) MaxVersion(ctx context.Context, id storeapi.ObjectID) (int64, error) {
	var rv sql.NullInt64
	err := s.conn(ctx).QueryRowContext(ctx,
		`SELECT MAX(resource_version) FROM events WHERE object_id = ?`, id).Scan(&rv)
	return rv.Int64, err
}

func (s sqliteEvents) GetLatest(ctx context.Context, id storeapi.ObjectID, category string) (*storeapi.Event, error) {
	return s.latestEventRun(ctx, id, category)
}

func (s sqliteEvents) Sweep(ctx context.Context, perTimeline int, maxAge time.Duration, capBudget int) (int, error) {
	if capBudget <= 0 {
		capBudget = eventCapBudget
	}
	var total int
	// One transaction so both bounds see the same snapshot and land together.
	err := s.Within(ctx, func(ctx context.Context) error {
		if perTimeline > 0 {
			n, err := s.trimEventsToCap(ctx, perTimeline, capBudget)
			if err != nil {
				return err
			}
			total += n
		}
		if maxAge > 0 {
			n, err := s.trimEvents(ctx, `last_at < ?`, toMillis(time.Now().UTC().Add(-maxAge)))
			if err != nil {
				return err
			}
			total += n
		}
		return nil
	})
	return total, err
}

// eventCapCandidates lists the timelines over the cap, newest-first order
// unneeded. Grouped on the columns idx_events_object_cat leads with, so it rides
// the index rather than sorting the table.
const eventCapCandidates = `
	SELECT object_id, category FROM events
	 GROUP BY object_id, category HAVING COUNT(*) > ?
	 LIMIT ?`

// eventCapBudget bounds the timelines one sweep trims when the caller names no
// budget of its own: the scoped statements are seeks, but a backlog of them
// still holds the write connection. The rest waits for the next sweep. A caller
// on a longer cadence asks for more, so the trim rate does not fall with it.
// See docs/adr/2026-08-06-event-retention-is-a-ring-per-timeline.md.
const eventCapBudget = 256

// timeline is one (object, category) partition of the event log.
type timeline struct {
	id       storeapi.ObjectID
	category string
}

// overCapTimelines reads the candidates in full: the trims that follow run on the
// same connection, so the cursor must be closed before they start.
func (s *sqliteStore) overCapTimelines(ctx context.Context, perTimeline, capBudget int) ([]timeline, error) {
	rows, err := s.conn(ctx).QueryContext(ctx, eventCapCandidates, perTimeline, capBudget)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	over := make([]timeline, 0, capBudget)
	for rows.Next() {
		var t timeline
		if err := rows.Scan(&t.id, &t.category); err != nil {
			return nil, err
		}
		over = append(over, t)
	}
	return over, rows.Err()
}

// trimEventsToCap deletes each over-cap timeline's oldest runs, one scoped
// statement per timeline.
func (s *sqliteStore) trimEventsToCap(ctx context.Context, perTimeline, capBudget int) (int, error) {
	over, err := s.overCapTimelines(ctx, perTimeline, capBudget)
	if err != nil {
		return 0, err
	}

	var total int
	for _, t := range over {
		// Newest-first, skip the cap, delete the rest. The outer key predicate is
		// what keeps both of trimEvents' statements on a seek.
		n, err := s.trimEvents(ctx, `object_id = ? AND category = ? AND id IN (
			SELECT id FROM events WHERE object_id = ? AND category = ?
			 ORDER BY last_at DESC, id DESC LIMIT -1 OFFSET ?)`,
			t.id, t.category, t.id, t.category, perTimeline)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// trimEvents deletes the runs matching where and raises each affected timeline's
// horizon to the highest version it removed there.
//
// The horizon is recorded BEFORE the delete, from the same predicate in the same
// transaction: RETURNING would give the same answer, but at the cost of
// materialising every deleted run and holding a half-read cursor on the single
// connection between two statements of one transaction.
func (s *sqliteStore) trimEvents(ctx context.Context, where string, args ...any) (int, error) {
	c := s.conn(ctx)
	if _, err := c.ExecContext(ctx, `
		INSERT INTO events_horizon (object_id, category, trimmed_through)
		SELECT object_id, category, MAX(resource_version) FROM events
		 WHERE `+where+`
		 GROUP BY object_id, category
		    ON CONFLICT(object_id, category) DO UPDATE SET trimmed_through = excluded.trimmed_through
		 WHERE excluded.trimmed_through > events_horizon.trimmed_through`, args...); err != nil {
		return 0, err
	}
	res, err := c.ExecContext(ctx, `DELETE FROM events WHERE `+where, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s sqliteObjects) DeleteFinalizer(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, finalizer string) (bool, error) {
	var clearedLast bool
	// Within keeps the read-modify-write of the finalizer list atomic.
	err := s.Within(ctx, func(ctx context.Context) error {
		c := s.conn(ctx)
		// Scoped read enforces the kind boundary while loading the finalizer list —
		// the list and the deletion flag are the whole of what this write reads, and
		// the flag is wanted as a bool, so the column never leaves SQLite as a clock.
		var (
			raw             []byte
			deletionPending bool
		)
		if err := s.selectScoped(ctx, gk, id,
			`finalizers, deletion_requested_at IS NOT NULL`, &raw, &deletionPending); err != nil {
			return err
		}
		var held []string
		if err := json.Unmarshal(raw, &held); err != nil {
			return err
		}
		remaining, removed := removeFinalizer(held, finalizer)
		// Absent finalizer: nothing changed, no bump.
		if !removed {
			return nil
		}
		clearedLast = len(remaining) == 0 && deletionPending
		rv, now, err := s.recordObjectWrite(ctx, c, gk, id, writeOpUpdate)
		if err != nil {
			return err
		}
		// No RETURNING: no row reported, and the scoped read proved existence.
		_, err = c.ExecContext(ctx, `
			UPDATE objects SET finalizers = ?, resource_version = ?, updated_at = ?
			WHERE id = ?`,
			jsonText(marshalFinalizers(remaining)), rv, now, id)
		return err
	})
	if err != nil {
		return false, err
	}
	return clearedLast, nil
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
func (s *sqliteStore) markForDeletion(ctx context.Context, where string, whereArgs ...any) (storeapi.ObjectID, bool, error) {
	c := s.conn(ctx)
	now := toMillis(time.Now().UTC())
	args := append([]any{now, now}, whereArgs...)
	// RETURNING, not RowsAffected: the write log entry needs the row's identity,
	// and the where here is a predicate rather than a known id.
	row := c.QueryRowContext(ctx, `
		UPDATE objects
		SET deletion_requested_at = ?,
		    resource_version = (SELECT value + 1 FROM resource_version_seq WHERE id = 1),
		    updated_at = ?
		WHERE (`+where+`) AND deletion_requested_at IS NULL
		RETURNING id, "group", kind, resource_version`, args...)
	var id storeapi.ObjectID
	var gk storeapi.GroupKind
	var rv int64
	err := row.Scan(&id, &gk.Group, &gk.Kind, &rv)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	// Commit the value the row above just took; same transaction, same connection.
	if _, err := nextResourceVersion(ctx, c); err != nil {
		return 0, false, err
	}
	// The soft delete is an update: the row is still live and readable.
	if err := appendWriteLog(ctx, c, id, gk, writeOpUpdate, rv, now); err != nil {
		return 0, false, err
	}
	return id, true, nil
}

// markChunkSize bounds the ids bound per batched deletion mark: a measured
// optimum, not a parameter-limit ceiling like idChunkSize. A var so tests can
// shrink it. See docs/adr/2026-07-30-store-write-shapes.md.
var markChunkSize = 128

// markManyForDeletion is markForDeletion over a set: it stamps every id whose
// clock is still NULL, drawing one version range for the whole set rather than
// one per row. Each row still takes its own value out of the range — the write
// log orders on it — assigned in the order ids arrives. Reports the ids it
// stamped, which is the write's own answer and not the caller's read. An empty
// set draws nothing, which is what keeps a re-cascade a lone SELECT.
// See docs/adr/2026-07-30-store-write-shapes.md.
func (s *sqliteStore) markManyForDeletion(ctx context.Context, ids []storeapi.ObjectID) (map[storeapi.ObjectID]bool, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	c := s.conn(ctx)
	now := toMillis(time.Now().UTC())
	end, err := advanceResourceVersion(ctx, c, len(ids))
	if err != nil {
		return nil, err
	}
	first := end - int64(len(ids)) + 1
	marked := make(map[storeapi.ObjectID]bool, len(ids))
	for start := 0; start < len(ids); start += markChunkSize {
		chunk := ids[start:min(start+markChunkSize, len(ids))]
		if err := s.markManyForDeletionChunk(ctx, c, chunk, first+int64(start), now, marked); err != nil {
			return nil, err
		}
	}
	return marked, nil
}

// markManyForDeletionChunk stamps one chunk, handing chunk[i] version first+i.
// The assignment rides a joined VALUES list, never a CASE over the id, which is
// quadratic in the chunk. A row the IS NULL guard skips leaves its assigned
// version unused; the resulting gap is harmless, since consumers seek with `>`.
// See docs/adr/2026-07-30-store-write-shapes.md.
func (s *sqliteStore) markManyForDeletionChunk(
	ctx context.Context,
	c dbtx,
	ids []storeapi.ObjectID,
	first, now int64,
	marked map[storeapi.ObjectID]bool,
) error {
	args := make([]any, 0, len(ids)*2+2)
	for i, id := range ids {
		args = append(args, id, first+int64(i))
	}
	args = append(args, now, now)
	// RETURNING, not RowsAffected: the log entries need each row's identity and
	// the version it actually took.
	// Drained and closed before the insert below, which needs the single conn.
	stamped, err := scanLoggedWrites(c.QueryContext(ctx,
		`WITH assigned(mark_id, mark_rv) AS (VALUES `+tupleRows(len(ids), 2)+`)
		UPDATE objects SET deletion_requested_at = ?, updated_at = ?, resource_version = assigned.mark_rv
		FROM assigned
		WHERE objects.id = assigned.mark_id AND objects.deletion_requested_at IS NULL
		RETURNING objects.id, objects."group", objects.kind, objects.resource_version`, args...))
	if err != nil {
		return err
	}
	for _, w := range stamped {
		marked[w.id] = true
	}
	return appendWriteLogUpdates(ctx, c, stamped, now)
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
) (storeapi.DeletionRequestResult, error) {
	if pending, err := probe(ctx); err != nil || pending {
		return storeapi.DeletionRequestResult{}, err
	}
	var res storeapi.DeletionRequestResult
	err := s.Within(ctx, func(ctx context.Context) error {
		var err error
		if res.ID, res.Marked, err = s.markForDeletion(ctx, where, whereArgs...); err != nil {
			return err
		}
		if !res.Marked {
			_, err = probe(ctx)
			return err
		}
		// Inside the mark's transaction: the discount that lifts the block is
		// the mark itself, so the read cannot see a state the mark did not make.
		res.Unblocked, err = s.unblockedTargets(ctx, []storeapi.ObjectID{res.ID})
		return err
	})
	if err != nil {
		return storeapi.DeletionRequestResult{}, err
	}
	return res, nil
}

// unblockedTargets returns the deletion-pending objects fromIDs point at through
// depends_on. Sound only for ids this transaction just marked: that mark is what
// makes EdgesHasIncoming discount the edge and lift the target's RESTRICT. A self
// edge is excluded — the object's own mark already queues it — and a target two
// sources share is repeated, which the work queue coalesces. Chunked under
// idChunkSize.
func (s *sqliteStore) unblockedTargets(ctx context.Context, fromIDs []storeapi.ObjectID) ([]storeapi.ObjectRef, error) {
	var out []storeapi.ObjectRef
	for start := 0; start < len(fromIDs); start += idChunkSize {
		refs, err := s.unblockedTargetsChunk(ctx, fromIDs[start:min(start+idChunkSize, len(fromIDs))])
		if err != nil {
			return nil, err
		}
		out = append(out, refs...)
	}
	return out, nil
}

func (s *sqliteStore) unblockedTargetsChunk(ctx context.Context, fromIDs []storeapi.ObjectID) ([]storeapi.ObjectRef, error) {
	args := make([]any, 0, len(fromIDs)+1)
	for _, id := range fromIDs {
		args = append(args, id)
	}
	args = append(args, string(storeapi.RelationDependsOn))
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT o.id, o."group", o.kind
		FROM edges r JOIN objects o ON o.id = r.to_id
		WHERE r.from_id IN (`+placeholders(len(fromIDs))+`) AND r.relation = ?
		  AND o.deletion_requested_at IS NOT NULL
		  AND o.id <> r.from_id`+edgeOrderByTarget, args...)
	if err != nil {
		return nil, err
	}
	return scanObjectRefs(rows)
}

// DeletionRequestsCreate marks id within gk. The kind is folded into the write, so a
// foreign id matches no row and the probe reports ErrWrongKind.
func (s sqliteDeletionRequests) Create(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID) (storeapi.DeletionRequestResult, error) {
	return s.requestDeletion(ctx,
		func(ctx context.Context) (bool, error) { return s.probeObjectScoped(ctx, gk, id) },
		`id = ? AND "group" = ? AND kind = ?`, id, gk.Group, gk.Kind)
}

// DeletionRequestsCreateByName marks the gk row holding name; the resolve and the mark
// are one statement, which is where the returned id comes from.
func (s sqliteDeletionRequests) CreateByName(ctx context.Context, gk storeapi.GroupKind, name string) (storeapi.DeletionRequestResult, error) {
	return s.requestDeletion(ctx,
		func(ctx context.Context) (bool, error) { return s.probeDeletionByName(ctx, gk, name) },
		`"group" = ? AND kind = ? AND name = ?`, gk.Group, gk.Kind, name)
}

// DeletionRequestsCreateFromOwner cascades deletion to ownerID's owned children,
// returning every owned child, deleting or not, each flagged with whether this
// call stamped it. A re-cascade over an already-deleting subtree is a lone SELECT.
func (s sqliteDeletionRequests) CreateFromOwner(ctx context.Context, ownerID storeapi.ObjectID) (storeapi.DeletionCascadeResult, error) {
	// Self-wrapped: several children each draw a version, and publication is in
	// commit order only inside Within.
	var res storeapi.DeletionCascadeResult
	err := s.Within(ctx, func(ctx context.Context) error {
		var err error
		res, err = s.deletionRequestsCreateFromOwner(ctx, ownerID)
		return err
	})
	if err != nil {
		return storeapi.DeletionCascadeResult{}, err
	}
	return res, nil
}

func (s *sqliteStore) deletionRequestsCreateFromOwner(ctx context.Context, ownerID storeapi.ObjectID) (storeapi.DeletionCascadeResult, error) {
	rows, err := s.conn(ctx).QueryContext(ctx, `
		SELECT o.id, o."group", o.kind, o.deletion_requested_at
		FROM edges r JOIN objects o ON o.id = r.from_id
		WHERE r.to_id = ? AND r.relation = ?`+edgeOrderByReferrer, ownerID, string(storeapi.RelationOwnedBy))
	if err != nil {
		return storeapi.DeletionCascadeResult{}, err
	}
	type child struct {
		ref      storeapi.ObjectRef
		deleting bool
	}
	var children []child
	var candidates []storeapi.ObjectID
	for rows.Next() {
		var ch child
		var delAt *int64
		// All columns scan without error (NOT NULL, or nullable INTEGER -> *int64).
		_ = rows.Scan(&ch.ref.ID, &ch.ref.Group, &ch.ref.Kind, &delAt)
		ch.deleting = delAt != nil
		children = append(children, ch)
		if !ch.deleting {
			candidates = append(candidates, ch.ref.ID)
		}
	}
	// modernc buffers the whole result set on the first Next; no late failure.
	_ = rows.Err()
	rows.Close() // free the single-conn pool before the writes below

	// One draw and one UPDATE for the level, not one per child.
	marked, err := s.markManyForDeletion(ctx, candidates)
	if err != nil {
		return storeapi.DeletionCascadeResult{}, err
	}

	res := storeapi.DeletionCascadeResult{Children: make([]storeapi.DeletionCascadeChild, 0, len(children))}
	var markedIDs []storeapi.ObjectID
	for _, ch := range children {
		// Marked is the UPDATE's own answer, which here matches !ch.deleting: the
		// SELECT above and the marks share one BEGIN IMMEDIATE transaction.
		// Reported from the write anyway — it is the source of truth, it costs
		// nothing, and Store admits backends that do not serialize the two.
		stamped := marked[ch.ref.ID]
		if stamped {
			markedIDs = append(markedIDs, ch.ref.ID)
		}
		res.Children = append(res.Children, storeapi.DeletionCascadeChild{Marked: stamped, Ref: ch.ref})
	}
	// One query for the level, and only for the children this call marked: a
	// child already deleting discounted its edges on the pass that marked it.
	// Tail call: the caller zeroes res on error.
	res.Unblocked, err = s.unblockedTargets(ctx, markedIDs)
	return res, err
}

func (s sqliteObjects) Delete(ctx context.Context, id storeapi.ObjectID) error {
	// Self-wrapped: the cascade to conditions, events and edges goes together or
	// not at all.
	return s.Within(ctx, func(ctx context.Context) error { return s.objectsDelete(ctx, id) })
}

// objectsDelete physically removes the row. It draws a resource_version even
// though no row survives to hold it: the write log's delete entry takes it, and
// without one the entry could not be ordered against the rest of the log. The
// counter is shared with the event log, so collection moves that too.
func (s *sqliteStore) objectsDelete(ctx context.Context, id storeapi.ObjectID) error {
	c := s.conn(ctx)
	// Read before the DELETE: the conditions cascade with the row, so this is the
	// last moment the image can be assembled.
	image, err := s.Objects().Get(ctx, id)
	if err != nil {
		return err
	}
	// The owner edge cascades with the row, so the image is the last place it can
	// be recorded — an owner-scoped watch reads a collected child's owner here.
	owners, err := s.Edges().ListOutgoingByRelation(ctx, id, storeapi.RelationOwnedBy)
	if err != nil {
		return err
	}
	if len(owners) > 0 {
		image.Owner = &owners[0]
	}
	// Conditions, events and edges cascade. No zero-row check: the read above
	// already returned ErrNotFound for an id this transaction cannot see.
	if _, err := c.ExecContext(ctx, `DELETE FROM objects WHERE id = ?`, id); err != nil {
		return err
	}
	rv, err := nextResourceVersion(ctx, c)
	if err != nil {
		return err
	}
	if err := appendWriteLogDelete(ctx, c, image, rv, toMillis(time.Now().UTC())); err != nil {
		return err
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
func (s sqliteEdges) Add(ctx context.Context, fromID, toID storeapi.ObjectID, relation storeapi.Relation) (storeapi.EdgesAddResult, error) {
	var out storeapi.EdgesAddResult
	err := s.Within(ctx, func(ctx context.Context) error {
		// One round-trip, no blobs. A join, not scalar subqueries: SQLite does no
		// CSE, so separate subqueries would seek the same row twice. A missing
		// endpoint yields no row — clean ErrNotFound over an FK violation.
		var from, to storeapi.GroupKind
		var toDeletedAt *int64
		err := s.conn(ctx).QueryRowContext(ctx, `
			SELECT f."group", f.kind, t."group", t.kind, t.deletion_requested_at
			FROM objects f, objects t WHERE f.id = ? AND t.id = ?`,
			fromID, toID).Scan(&from.Group, &from.Kind, &to.Group, &to.Kind, &toDeletedAt)
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
		// DependentsListStaleSince until the owed pass. Same edge-new gate;
		// self-edges skipped to match that listing.
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
			From:                 from,
			To:                   to,
			ToDeleting:           toDeletedAt != nil,
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
// silent no-op. Like EdgesAdd it bumps nothing and joins the ambient
// transaction. Unblocked reports that the removal lifted a RESTRICT block: the
// edge was there, the target is deletion-pending and the source is not — the
// last condition because EdgesHasIncoming already discounts an edge from a
// deletion-pending source.
func (s sqliteEdges) Delete(ctx context.Context, fromID, toID storeapi.ObjectID, relation storeapi.Relation) (storeapi.EdgesDeleteResult, error) {
	res, err := s.conn(ctx).ExecContext(ctx,
		`DELETE FROM edges WHERE from_id = ? AND to_id = ? AND relation = ?`,
		fromID, toID, string(relation))
	if err != nil {
		return storeapi.EdgesDeleteResult{}, err
	}
	// modernc caches the count and cannot fail here; a wrong count would
	// silently skip the caller's push.
	if n, _ := res.RowsAffected(); n == 0 {
		return storeapi.EdgesDeleteResult{}, nil
	}
	// Unblocked is a depends_on verdict: the source-side discount below is the one
	// EdgesHasIncoming gives that relation and no other.
	if relation != storeapi.RelationDependsOn {
		return storeapi.EdgesDeleteResult{}, nil
	}
	// Both endpoints in one row, as EdgesAdd does. No transaction of its own. The
	// gap costs at most a push: a source marked deletion-pending inside it reads
	// as "was already discounted", and the target waits for the sweep. See
	// docs/adr/2026-08-05-a-dropped-dependency-pushes-its-target.md.
	//
	// A failure here is reported, not swallowed. Inside an ambient Within the
	// caller's rollback unwinds the DELETE, so a retry re-runs cleanly and
	// pushes. Outside one the DELETE stands and the retry removes nothing, so
	// the report costs the push, not the collect: the sweeper is the route, and
	// it cannot be turned off.
	var to storeapi.GroupKind
	var unblocked int
	err = s.conn(ctx).QueryRowContext(ctx, `
		SELECT t."group", t.kind,
		       t.deletion_requested_at IS NOT NULL AND f.deletion_requested_at IS NULL
		FROM objects t, objects f WHERE t.id = ? AND f.id = ?`,
		toID, fromID).Scan(&to.Group, &to.Kind, &unblocked)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = nil // an endpoint went in the gap: nothing left to push
		}
		return storeapi.EdgesDeleteResult{}, err
	}
	if unblocked == 0 {
		return storeapi.EdgesDeleteResult{}, nil
	}
	return storeapi.EdgesDeleteResult{To: to, Unblocked: true}, nil
}

// EdgesListIncoming returns the objects pointing at toID through relation, joining edges
// to objects so each carries the GroupKind needed to route a requeue.
func (s sqliteEdges) ListIncoming(ctx context.Context, toID storeapi.ObjectID, relation storeapi.Relation) ([]storeapi.ObjectRef, error) {
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
func (s sqliteEdges) GroupIncomingByID(ctx context.Context, toIDs []storeapi.ObjectID, relation storeapi.Relation) (map[storeapi.ObjectID][]storeapi.ObjectRef, error) {
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
func (s sqliteEdges) ListOutgoingByRelation(ctx context.Context, fromID storeapi.ObjectID, relation storeapi.Relation) ([]storeapi.ObjectRef, error) {
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
func (s sqliteEdges) GroupOutgoingByID(ctx context.Context, fromIDs []storeapi.ObjectID, relation storeapi.Relation) (map[storeapi.ObjectID][]storeapi.ObjectRef, error) {
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

// scanLoggedWrites scans a mutator's RETURNING rows into the shape a batched
// write log append takes. Takes the query's error alongside its rows: modernc
// runs an UPDATE ... RETURNING to completion at QueryContext, so a failed stamp
// arrives here rather than out of rows.Err().
func scanLoggedWrites(rows *sql.Rows, err error) ([]loggedWrite, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []loggedWrite
	for rows.Next() {
		var w loggedWrite
		// id (INTEGER), group/kind (TEXT NOT NULL) and resource_version (INTEGER
		// NOT NULL) never fail.
		_ = rows.Scan(&w.id, &w.gk.Group, &w.gk.Kind, &w.rv)
		out = append(out, w)
	}
	return out, rows.Err()
}

// EdgesDeleteFinalizingDependsOn removes depends_on edges into toID whose source is
// itself deletion-pending, breaking the mutual-RESTRICT deadlock between
// finalizing objects. Bumps no version.
func (s sqliteEdges) DeleteFinalizingDependsOn(ctx context.Context, toID storeapi.ObjectID) error {
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
func (s sqliteEdges) HasIncoming(ctx context.Context, id storeapi.ObjectID) (bool, error) {
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

// DriverCursorsGet reads name's persisted cursor (see storeapi.Store). A
// missing row is ok=false: absence is the ordinary first-run state, not a fault.
func (s sqliteDriverCursors) Get(ctx context.Context, name string) (int64, bool, error) {
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

// DriverCursorsSet upserts name's persisted cursor (see storeapi.Store).
// The WHERE on DO UPDATE keeps the cursor monotonic and suppresses a no-advance
// write outright — no page dirtied on a quiet tick.
func (s sqliteDriverCursors) Set(ctx context.Context, name string, cursor int64) error {
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

// ObjectWritesListSince reads gk's log entries above afterRV, covered by
// idx_object_writes_kind except where a delete entry's image is fetched.
//
// Self-wrapped, because the contract is atomic and this takes more than one
// statement: the page, the horizon and the delete images must all describe the
// same instant. A retention sweep landing between them would either report a
// horizon above entries the page already captured — a terminal ErrWatchTooOld
// for a stream that lost nothing — or delete a captured entry's row image out
// from under the caller.
//
// The horizon still rides the page's own statement as a scalar subquery, which
// costs nothing and keeps the empty-page case honest.
func (s sqliteObjectWrites) ListSince(ctx context.Context, gk storeapi.GroupKind, afterRV int64, limit int) ([]storeapi.ObjectWrite, int64, error) {
	if limit <= 0 {
		// Would reach SQLite as "LIMIT -1" (unbounded) or panic in make below.
		return nil, 0, nil
	}
	var writes []storeapi.ObjectWrite
	var trimmed int64
	err := s.Within(ctx, func(ctx context.Context) error {
		var err error
		if writes, trimmed, err = s.writeLogPage(ctx, gk, afterRV, limit); err != nil {
			return err
		}
		if len(writes) == 0 {
			// No rows carried the subquery, so read it on its own.
			trimmed, err = s.trimmedThrough(ctx, gk)
			return err
		}
		return s.attachImages(ctx, writes)
	})
	if err != nil {
		return nil, 0, err
	}
	return writes, trimmed, nil
}

// trimmedThrough is gk's retention horizon; no row means nothing has been
// trimmed, which is 0 rather than an error.
func (s *sqliteStore) trimmedThrough(ctx context.Context, gk storeapi.GroupKind) (int64, error) {
	var trimmed int64
	err := s.conn(ctx).QueryRowContext(ctx, `
		SELECT trimmed_through FROM object_writes_horizon
		 WHERE "group" = ? AND kind = ?`, gk.Group, gk.Kind).Scan(&trimmed)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return trimmed, err
}

// ObjectWritesMaxVersion reads gk's log position, covered by
// idx_object_writes_kind. The horizon is folded in because retention lowers the
// log's own maximum: a kind trimmed empty would report 0 against a tail parked
// higher and list on every tick. Folded, the position only ever rises, which is
// why the tail gates on > rather than !=.
func (s sqliteObjectWrites) MaxVersion(ctx context.Context, gk storeapi.GroupKind) (int64, error) {
	// One statement, not two: this is a watch's entire quiet-tick budget, and
	// both halves are covering-index seeks that fold for free.
	var at int64
	err := s.conn(ctx).QueryRowContext(ctx, `
		SELECT max(
			coalesce((SELECT MAX(resource_version) FROM object_writes
			           WHERE "group" = ? AND kind = ?), 0),
			coalesce((SELECT trimmed_through FROM object_writes_horizon
			           WHERE "group" = ? AND kind = ?), 0))`,
		gk.Group, gk.Kind, gk.Group, gk.Kind).Scan(&at)
	return at, err
}

// ObjectWritesSweep trims the log and records what it removed. The delete and
// the horizon raise share a transaction: a horizon that lagged its deletes would
// read as "nothing trimmed" and let a resume succeed against a hole.
//
// RETURNING, not a bare DELETE: the horizon is per kind, so the sweep has to
// learn which kinds it touched and how far.
func (s sqliteObjectWrites) Sweep(ctx context.Context, perKind int, maxAge time.Duration) (int, error) {
	var deleted int
	err := s.Within(ctx, func(ctx context.Context) error {
		if maxAge > 0 {
			n, err := s.trimWriteLog(ctx, `written_at < ?`,
				toMillis(time.Now().UTC().Add(-maxAge)))
			if err != nil {
				return err
			}
			deleted += n
		}
		if perKind > 0 {
			// One statement per kind, not one subquery per row. Keyed on a literal
			// kind, the cutoff is uncorrelated, so it is evaluated once and every
			// step rides idx_object_writes_kind: a seek for the cutoff, a range
			// delete below it. A kind under its cap yields NULL, and
			// `resource_version <= NULL` matches nothing, so it costs one seek.
			kinds, err := s.writeLogKinds(ctx)
			if err != nil {
				return err
			}
			for _, gk := range kinds {
				n, err := s.trimWriteLog(ctx, `"group" = ? AND kind = ? AND resource_version <= (
					SELECT resource_version FROM object_writes
					 WHERE "group" = ? AND kind = ?
					 ORDER BY resource_version DESC LIMIT 1 OFFSET ?)`,
					gk.Group, gk.Kind, gk.Group, gk.Kind, perKind)
				if err != nil {
					return err
				}
				deleted += n
			}
		}
		return nil
	})
	return deleted, err
}

// writeLogKinds lists the kinds present in the log. An index-only scan, and the
// only step of the count trim that is not a seek — kinds number in the handful
// where entries number in the millions, so it is the right axis to iterate.
func (s *sqliteStore) writeLogKinds(ctx context.Context) ([]storeapi.GroupKind, error) {
	rows, err := s.conn(ctx).QueryContext(ctx,
		`SELECT DISTINCT "group", kind FROM object_writes`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var kinds []storeapi.GroupKind
	for rows.Next() {
		var gk storeapi.GroupKind
		_ = rows.Scan(&gk.Group, &gk.Kind) // two declared TEXT columns
		kinds = append(kinds, gk)
	}
	return kinds, rows.Err()
}

// deleteWriteLogRows deletes the entries matching where and reports the highest
// version removed per kind, with the total. Closes its rows before returning, so
// the horizon writes that follow get the single connection back.
func (s *sqliteStore) deleteWriteLogRows(ctx context.Context, where string, args ...any) (map[storeapi.GroupKind]int64, int, error) {
	rows, err := s.conn(ctx).QueryContext(ctx,
		`DELETE FROM object_writes WHERE `+where+`
		 RETURNING "group", kind, resource_version`, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	highest := map[storeapi.GroupKind]int64{}
	var deleted int
	for rows.Next() {
		var k storeapi.GroupKind
		var rv int64
		_ = rows.Scan(&k.Group, &k.Kind, &rv) // two TEXT columns and an INTEGER
		highest[k] = max(highest[k], rv)
		deleted++
	}
	return highest, deleted, rows.Err()
}

// trimWriteLog deletes the entries matching where and raises each affected
// kind's horizon to the highest version it removed there.
func (s *sqliteStore) trimWriteLog(ctx context.Context, where string, args ...any) (int, error) {
	c := s.conn(ctx)
	highest, deleted, err := s.deleteWriteLogRows(ctx, where, args...)
	if err != nil {
		return 0, err
	}
	for k, rv := range highest {
		if _, err := c.ExecContext(ctx, `
			INSERT INTO object_writes_horizon ("group", kind, trimmed_through)
			VALUES (?, ?, ?)
			    ON CONFLICT("group", kind) DO UPDATE SET trimmed_through = excluded.trimmed_through
			 WHERE excluded.trimmed_through > object_writes_horizon.trimmed_through`,
			k.Group, k.Kind, rv); err != nil {
			return 0, err
		}
	}
	return deleted, nil
}

// ObjectWritesSnapshot lists gk and reads its log position in one transaction.
// Separately they could straddle a write: a row listed after the position was
// read would never reach the stream, since the stream starts above it.
func (s sqliteObjectWrites) Snapshot(ctx context.Context, gk storeapi.GroupKind) ([]*storeapi.RawObject, int64, error) {
	return s.snapshot(ctx, gk, func(ctx context.Context) ([]*storeapi.RawObject, error) {
		return s.Objects().List(ctx, gk)
	})
}

// ObjectWritesSnapshotByID reads one row rather than the kind, and folds both
// "not there" cases — missing and foreign — into an empty result.
func (s sqliteObjectWrites) SnapshotByID(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID) ([]*storeapi.RawObject, int64, error) {
	return s.snapshot(ctx, gk, func(ctx context.Context) ([]*storeapi.RawObject, error) {
		obj, err := s.Objects().Get(ctx, id)
		if errors.Is(err, storeapi.ErrNotFound) {
			return nil, nil
		}
		if err != nil || obj.Group != gk.Group || obj.Kind != gk.Kind {
			return nil, err
		}
		return []*storeapi.RawObject{obj}, nil
	})
}

// ObjectWritesSnapshotByOwner reads one owner's children of gk. The listing is
// already kind-scoped, so a foreign owner simply matches nothing.
func (s sqliteObjectWrites) SnapshotByOwner(ctx context.Context, gk storeapi.GroupKind, ownerID storeapi.ObjectID) ([]*storeapi.RawObject, int64, error) {
	return s.snapshot(ctx, gk, func(ctx context.Context) ([]*storeapi.RawObject, error) {
		return s.Objects().ListByIncomingEdge(ctx, gk, ownerID, storeapi.RelationOwnedBy)
	})
}

func (s *sqliteStore) snapshot(
	ctx context.Context,
	gk storeapi.GroupKind,
	list func(context.Context) ([]*storeapi.RawObject, error),
) ([]*storeapi.RawObject, int64, error) {
	var rows []*storeapi.RawObject
	var at int64
	err := s.Within(ctx, func(ctx context.Context) error {
		var err error
		if rows, err = list(ctx); err != nil {
			return err
		}
		at, err = s.ObjectWrites().MaxVersion(ctx, gk)
		return err
	})
	if err != nil {
		return nil, 0, err
	}
	return rows, at, nil
}

// ObjectsListByIDs reads one batch of ids in one query. The tail calls it once
// per batch rather than ObjectsGet per changed object: the pool is size 1, so
// those would be serialized round trips and a churny kind would cost more than
// the full listing this design replaced.
func (s sqliteObjects) ListByIDs(ctx context.Context, gk storeapi.GroupKind, ids []storeapi.ObjectID) ([]*storeapi.RawObject, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(ids)+2)
	for _, id := range ids {
		args = append(args, id)
	}
	args = append(args, gk.Group, gk.Kind)
	return s.listObjectsWhere(ctx,
		`WHERE o.id IN (`+placeholders(len(ids))+`) AND o."group" = ? AND o.kind = ?`, args...)
}

// placeholders builds "?, ?, ?" for an IN list of n values.
func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

// tupleRows builds "(?, ?), (?, ?)" for a VALUES list of rows tuples of cols
// each. Zero rows yields an empty string, which no caller may emit.
func tupleRows(rows, cols int) string {
	return strings.TrimSuffix(strings.Repeat("("+placeholders(cols)+"), ", rows), ", ")
}

// kindTuples builds a VALUES list of kinds with the args to fill it.
func kindTuples(kinds []storeapi.GroupKind) (string, []any) {
	args := make([]any, 0, len(kinds)*2)
	for _, gk := range kinds {
		args = append(args, gk.Group, gk.Kind)
	}
	return tupleRows(len(kinds), 2), args
}
