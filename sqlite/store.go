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
	"unicode/utf8"

	"github.com/amorey/beehive/internal/storeapi"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// Compile-time checks, one per family: without them a method landing on the wrong
// receiver still satisfies Store through some other family's promotion.
var (
	_ storeapi.Store            = (*sqliteStore)(nil)
	_ storeapi.Conditions       = sqliteConditions{}
	_ storeapi.DeletionRequests = sqliteDeletionRequests{}
	_ storeapi.Dependencies     = sqliteDependencies{}
	_ storeapi.DriverCursors    = sqliteDriverCursors{}
	_ storeapi.Edges            = sqliteEdges{}
	_ storeapi.Events           = sqliteEvents{}
	_ storeapi.ObjectWrites     = sqliteObjectWrites{}
	_ storeapi.Objects          = sqliteObjects{}
	_ storeapi.ReconcileOwed    = sqliteReconcileOwed{}
)

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

	// identity names the database: the absolute path, or a token for a memory
	// store, whose file::memory: genuinely is a database of its own.
	identity string

	// readDB serves reads that are not inside a transaction. Aliased to db where
	// the database cannot be opened twice (see OpenMemory).
	readDB *sql.DB

	// versions hands out resource versions from a reserved block.
	versions versions

	// readStmts and writeStmts hold every constant statement, prepared once per
	// pool. Written once by the two preparations, read through the two accessors,
	// and never written again — see closeStatements.
	readStmts  stmtSet
	writeStmts stmtSet

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
	// Readers first: they hold snapshots the writer's checkpoint waits on. The
	// writer closes whatever happened above it, so a failed reader cannot leak it.
	// Statements first: closing them frees the driver's compiled programs.
	err := s.closeStatements()
	if s.readDB != s.db {
		err = errors.Join(err, s.readDB.Close())
	}
	return errors.Join(err, s.db.Close())
}

// Identity is the absolute path, or a token for a memory store.
func (s *sqliteStore) Identity() string { return s.identity }

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

	// Exec, not Query — see above. Interpolated, not bound: a pragma argument
	// takes no parameter, and the ? form fails at prepare rather than execution.
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

// pageCounters reads page_count and freelist_count off one connection. Both are
// constant and neither is prepared: freePagesRelease subtracts them across its
// vacuum, so they must run on the connection it holds, and a prepared read would
// route to the reader pool.
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

	// mu keeps this state consistent. It is not a licence to fan out: a
	// transaction ctx belongs to one goroutine (internal/storeapi, Within), and
	// two goroutines sharing a prepared statement interleave on one cursor.
	mu    sync.Mutex
	hooks []queuedHook

	// closed latches once the transaction is over, by either outcome. A ctx
	// carrying this txState outlives the transaction, so every consumer degrades
	// on it together: fresh transaction, pool, inline hook.
	closed bool

	// committed records *how* it ended; only flush sets it. A hook runs iff its
	// transaction committed, so "over" and "over and durable" differ.
	committed bool

	// readOnly marks a frame opened by withinRead. conn refuses one; see there.
	readOnly bool

	// drawn is the highest resource version this transaction has taken, which is
	// what its commit publishes. Nested frames share it, rolled-back draws
	// included: a burned version belongs to no write, so publishing over it is safe.
	drawn int64

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

// roDBTX is dbtx without ExecContext, and read returns it so a write issued the
// ordinary way does not compile onto the reader. A runtime check could not
// stand in: inside a transaction read hands back that transaction, where a
// write would commit on the writer and look correct.
//
// It is not airtight: a write with RETURNING is issued through QueryContext or
// QueryRowContext, which this interface keeps, and several writes are — the
// create, the spec write and both deletion marks among them. conn is where a
// write is actually refused; this type only keeps the ordinary ones off the
// reader at compile time.
type roDBTX interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// refuseWriteInReadFrame reports whether a write may be issued on ctx. Callers
// take it for the refusal alone, before a compare that may return without
// writing: writeStmt cannot stand in there, since it is only reached once a
// statement is actually issued.
func (s *sqliteStore) refuseWriteInReadFrame(ctx context.Context) error {
	if st := liveTx(ctx); st != nil && st.readOnly {
		return errWroteInReadTx
	}
	return nil
}

// read returns the connection a read-only statement runs on: the ambient
// transaction while it is live, else the read pool.
//
// Returning the transaction is the whole safety property, not an optimisation.
// A read issued inside a transaction on any other connection sees the database
// as of before that transaction's own uncommitted writes — and unlike today's
// deadlock, that failure is silent.
func (s *sqliteStore) read(ctx context.Context) roDBTX {
	if st := liveTx(ctx); st != nil {
		return st.tx
	}
	return s.readDB
}

// liveTx returns the ctx's transaction frame while it is still open. A ctx
// outlives its transaction, so a closed frame reads as none: a statement issued
// on it commits standalone rather than failing with sql.ErrTxDone.
func liveTx(ctx context.Context) *txState {
	if fr, ok := txFrom(ctx); ok && !fr.st.isClosed() {
		return fr.st
	}
	return nil
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
	return s.runTx(ctx, s.db, nil, fn, func(ctx context.Context, st *txState) {
		// Both before the hooks. Publishing after them lets the waker's dependent
		// sample the cursor below its target's version; refilling after them puts
		// the draw behind whatever transaction a hook has opened on the connection
		// this commit just released.
		s.versions.publish(st.highestDraw())
		s.refillVersions(ctx)
	})
}

// errWroteInReadTx reports a write attempted inside withinRead. Not a sentinel
// callers handle: it is a programming error, and no statement runs.
var errWroteInReadTx = errors.New("beehive/sqlite: write inside a read transaction")

// runTx is the frame protocol both Within and withinRead run: begin, install the
// frame, seal, commit, settle, then drain the hooks the commit owed. settle runs
// only on a clean commit and may be nil.
//
// The ordering here is contractual. Nothing outside this function may reorder it.
func (s *sqliteStore) runTx(
	ctx context.Context,
	db *sql.DB,
	opts *sql.TxOptions,
	fn func(ctx context.Context) error,
	settle func(ctx context.Context, st *txState),
) error {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	st := &txState{tx: tx, readOnly: opts != nil && opts.ReadOnly}
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
	if settle != nil {
		settle(ctx, st)
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

// noteDraw records a resource version this transaction took.
func (st *txState) noteDraw(rv int64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if rv > st.drawn {
		st.drawn = rv
	}
}

// highestDraw is the highest version this transaction took, 0 if it took none.
func (st *txState) highestDraw() int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.drawn
}

// withinRead runs fn inside a read transaction on the read pool: every read fn
// makes sees one snapshot, and no write lock is taken. A write inside fn is
// refused by conn, which is what marking the frame readOnly buys.
//
// ReadOnly does one thing: modernc applies the DSN's _txlock only when it is
// false, which is what makes the BEGIN deferred.
func (s *sqliteStore) withinRead(ctx context.Context, fn func(ctx context.Context) error) error {
	// Nested, a read joins on a savepoint like any other frame. Not for the
	// rollback — a read has nothing to roll back — but because the frame is what
	// records that this read is running: without it the outer transaction seals
	// and commits from under a sibling goroutine's grouped read.
	if fr, ok := txFrom(ctx); ok && !fr.st.isClosed() {
		return fr.st.nested(ctx, fr.depth, fr.id, fn)
	}
	// No settle: a read draws no version, and the refill would be a write on the
	// connection this call exists to stay off.
	return s.runTx(ctx, s.readDB, &sql.TxOptions{ReadOnly: true}, fn, nil)
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
func (s *sqliteStore) nextResourceVersion(ctx context.Context) (int64, error) {
	return s.advanceResourceVersion(ctx, 1)
}

// advanceResourceVersion advances the cursor by n and returns the highest value
// drawn, so the range taken is [value-n+1, value]. n must be positive. Served
// from the reserved block where it fits, and from the table otherwise.
func (s *sqliteStore) advanceResourceVersion(ctx context.Context, n int) (int64, error) {
	hi, ok := s.versions.take(n)
	if !ok {
		var err error
		if hi, err = s.drawResourceVersions(ctx, n); err != nil {
			return 0, err
		}
		s.versions.record(hi)
	}
	// The transaction publishes its own draws at commit. A draw outside one is
	// never published, which stalls the cursor rather than overstating it.
	if st := liveTx(ctx); st != nil {
		st.noteDraw(hi)
	}
	return hi, nil
}

// drawResourceVersions advances the counter by n and returns the highest value
// drawn. A standalone counter, not MAX(objects.resource_version): deleting the
// highest-versioned row must never regress the cursor and hand out a reused
// version.
func (s *sqliteStore) drawResourceVersions(ctx context.Context, n int) (int64, error) {
	var rv int64
	err := s.queryRow(ctx, stmtDrawResourceVersions, n).Scan(&rv)
	return rv, err
}

// refillVersions waits no longer than this for the writer connection. Matches the
// DSN's busy_timeout, which bounds the lock wait but not the pool wait. Expiring
// costs one fallback draw. A var so tests can shrink it.
var refillTimeout = 5 * time.Second

// refillVersions reserves the next block. It must run where no transaction is
// open: a reservation that rolls back leaves the allocator handing out versions
// the counter no longer covers.
//
// The draw is outside the allocator's lock. It waits for the writer connection,
// which a sibling transaction may hold while itself waiting to take a version.
func (s *sqliteStore) refillVersions(ctx context.Context) {
	if blockSize <= 0 || !s.versions.spent() {
		return
	}
	// Detached from the caller's deadline, since a cancelled refill leaves the block
	// spent and puts every later write back on the fallback draw — but bounded, or a
	// hook that left a sibling transaction open holds this commit for that
	// transaction's whole life waiting on the one writer connection.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), refillTimeout)
	defer cancel()
	// The frame goes too, not just the deadline: settle runs after the commit but
	// before flush latches the frame closed, so routing by ctx would draw on a
	// transaction that is already over.
	hi, err := s.drawResourceVersions(context.WithValue(ctx, txKey{}, nil), blockSize)
	if err != nil {
		// Swallowed: the commit already landed, so this cannot be reported. The
		// block stays spent and the next draw raises it where a caller can act.
		return
	}
	s.versions.reserve(hi, blockSize)
}

// seedVersions puts the allocator above every version the previous process used.
// The reservation doubles as the read: the draw reports the block's end, and what
// sits below it is where that process left the counter.
func (s *sqliteStore) seedVersions(ctx context.Context) error {
	n := max(blockSize, 0)
	hi, err := s.drawResourceVersions(ctx, n)
	if err != nil {
		return err
	}
	s.versions.reserve(hi, n)
	// The counter's value before the draw: where the previous process left it.
	s.versions.publish(hi - int64(n))
	return nil
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
	// conn for its refusal alone: a write in a read frame must be refused before
	// the draw, so a refused create burns no resource_version.
	if err := s.refuseWriteInReadFrame(ctx); err != nil {
		return nil, err
	}
	rv, err := s.nextResourceVersion(ctx)
	if err != nil {
		return nil, err
	}
	now := toMillis(time.Now().UTC())

	// RETURNING hands back the written row, assigned id included — no follow-up read.
	row := s.queryRow(ctx, stmtInsertObject,
		gk.Group, gk.Kind, in.Name, jsonText(in.Spec), in.SpecVersion,
		rv, jsonText(finalizers), now, now)
	// scanObject, not scanWritten: the id did not exist before this statement, so
	// the row provably has no conditions.
	obj, err := scanObject(row)
	if err != nil {
		return nil, asNameTaken(err)
	}
	if err := s.appendWriteLog(ctx, obj.ID, gk, writeOpCreate, rv, now); err != nil {
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
	gk storeapi.GroupKind,
	id storeapi.ObjectID,
	op int,
) (rv, now int64, err error) {
	if rv, err = s.nextResourceVersion(ctx); err != nil {
		return 0, 0, err
	}
	now = toMillis(time.Now().UTC())
	if err = s.appendWriteLog(ctx, id, gk, op, rv, now); err != nil {
		return 0, 0, err
	}
	return rv, now, nil
}

// objectWritesColumns is the log's insert column list, less the delete-only final.
const objectWritesColumns = `resource_version, object_id, "group", kind, op, written_at`

// appendWriteLog records one committed object write. Callers pass the version the
// write took, so the entry orders against the row it describes.
func (s *sqliteStore) appendWriteLog(ctx context.Context, id storeapi.ObjectID, gk storeapi.GroupKind, op int, rv, now int64) error {
	_, err := s.exec(ctx, stmtAppendWriteLog, rv, id, gk.Group, gk.Kind, op, now)
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
func (s *sqliteStore) appendWriteLogUpdates(ctx context.Context, writes []loggedWrite, now int64) error {
	if len(writes) == 0 {
		return nil
	}
	// The soft delete is an update: the row is still live and readable.
	_, err := s.exec(ctx, stmtAppendWriteLogUpdates, writeOpUpdate, now, jsonWriteLogRows(writes))
	return err
}

// appendWriteLogDelete records a collection, carrying the row image a Deleted
// change reports. image is stamped with the delete's own version: it is the
// object's last, and the row that held the previous one no longer exists.
func (s *sqliteStore) appendWriteLogDelete(ctx context.Context, image *storeapi.RawObject, rv, now int64) error {
	ps, err := s.writeStmt(ctx, stmtAppendWriteLogDelete)
	if err != nil {
		return err
	}
	image.ResourceVersion = rv
	final, _ := json.Marshal(image) // plain data: no channel, func or cyclic field can fail it
	_, err = ps.ExecContext(ctx,
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
	stmt stmtID,
	dest ...any,
) error {
	ps := s.readStmt(ctx, stmt)
	var group, kind string
	err := ps.QueryRowContext(ctx, id).
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
	if err := s.selectScoped(ctx, gk, id, stmtScopedDeletion, &deletionAt); err != nil {
		return false, err
	}
	return deletionAt.Valid, nil
}

// checkObjectExists is probeObjectScoped without the kind gate: ErrNotFound, or
// nil.
func (s *sqliteStore) checkObjectExists(ctx context.Context, id storeapi.ObjectID) error {
	var one int
	err := s.queryRow(ctx, stmtCheckObjectExists, id).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return storeapi.ErrNotFound // bare, like scanObject's
	}
	return err
}

// checkObjectScoped is probeObjectScoped for callers that only need the gate.
func (s *sqliteStore) checkObjectScoped(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID) error {
	return s.selectScoped(ctx, gk, id, stmtScopedGate)
}

// getObjectRow reads the objects row without assembling conditions.
func (s *sqliteStore) getObjectRow(ctx context.Context, id storeapi.ObjectID) (*storeapi.RawObject, error) {
	return scanObject(s.queryRow(ctx, stmtGetObjectRow, id))
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

// Objects().GetForReconcile is the reconcile loop's opening read (see the contract
// on storeapi.Store). The dependency flag is a correlated subquery, riding the row
// read rather than adding a round trip.
//
// The cursor is read before the row, never after: a write committing in between
// would be both unobserved by this load and at or below the watermark it stamps.
func (s sqliteObjects) GetForReconcile(ctx context.Context, id storeapi.ObjectID) (storeapi.ReconcileLoad, error) {
	load := storeapi.ReconcileLoad{Cursor: s.versions.latest()}
	obj, err := scanObject(s.queryRow(ctx, stmtGetForReconcile, id), &load.HasDependencies)
	if err != nil {
		return storeapi.ReconcileLoad{}, err
	}
	if _, err := s.attachConditions(ctx, obj); err != nil {
		return storeapi.ReconcileLoad{}, err
	}
	load.Object = *obj
	return load, nil
}

// Objects().GetMeta is getObjectRow across the store boundary: no conditions
// assembled, for metadata-only callers.
func (s sqliteObjects) GetMeta(ctx context.Context, id storeapi.ObjectID) (*storeapi.RawObject, error) {
	return s.getObjectRow(ctx, id)
}

// Objects().GetMetaByName is getObjectRowByName across the store boundary: no
// conditions assembled, for metadata-only callers.
func (s sqliteObjects) GetMetaByName(ctx context.Context, gk storeapi.GroupKind, name string) (*storeapi.RawObject, error) {
	return s.getObjectRowByName(ctx, gk, name)
}

// getObjectRowByName is getObjectRow keyed by name within gk. No ErrWrongKind:
// the kind is in the WHERE.
func (s *sqliteStore) getObjectRowByName(ctx context.Context, gk storeapi.GroupKind, name string) (*storeapi.RawObject, error) {
	return scanObject(s.queryRow(ctx, stmtGetObjectRowByName, gk.Group, gk.Kind, name))
}

func (s sqliteObjects) GetByName(ctx context.Context, gk storeapi.GroupKind, name string) (*storeapi.RawObject, error) {
	obj, err := s.getObjectRowByName(ctx, gk, name)
	if err != nil {
		return nil, err
	}
	return s.attachConditions(ctx, obj)
}

func (s sqliteObjects) List(ctx context.Context, gk storeapi.GroupKind) ([]*storeapi.RawObject, error) {
	return s.listObjects(ctx, stmtListObjects, gk.Group, gk.Kind)
}

// Objects().ListByIncomingEdge returns the full rows of the objects pointing at
// toID through relation, restricted to gk — the blob-bearing Edges().ListIncoming.
//
// The edge is a semi-join, not a join: IN (SELECT …) lets idx_edges_to drive, so
// the work scales with the referrers rather than every object of the kind.
func (s sqliteObjects) ListByIncomingEdge(ctx context.Context, gk storeapi.GroupKind, toID storeapi.ObjectID, relation storeapi.Relation) ([]*storeapi.RawObject, error) {
	return s.listObjects(ctx, stmtListObjectsByIncomingEdge,
		toID, string(relation), gk.Group, gk.Kind)
}

// listObjects is the shared multi-row object read: rows matching stmt's tail,
// ordered by id, conditions attached.
func (s *sqliteStore) listObjects(ctx context.Context, stmt stmtID, args ...any) ([]*storeapi.RawObject, error) {
	ps := s.readStmt(ctx, stmt)
	rows, err := ps.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	return s.attachConditionsToRows(ctx, rows)
}

// attachConditionsToRows scans an object listing and fills its conditions.
func (s *sqliteStore) attachConditionsToRows(ctx context.Context, rows *sql.Rows) ([]*storeapi.RawObject, error) {
	// scanObjects closes rows, releasing their connection before the conditions query.
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
// result set so the next chunk reuses that connection rather than taking
// another from the read pool.
func (s *sqliteStore) conditionsByIDsChunk(ctx context.Context, ids []storeapi.ObjectID, out map[storeapi.ObjectID][]storeapi.Condition) error {
	rows, err := s.query(ctx, stmtConditionsByIDs, jsonList(ids))
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
	rows, err := s.query(ctx, stmtListUnsettledIDs, gk.Group, gk.Kind)
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
	rows, err := s.query(ctx, stmtListDeletionRequests)
	if err != nil {
		return nil, err
	}
	return scanObjectRefs(rows)
}

func (s sqliteReconcileOwed) ListIDs(ctx context.Context, gk storeapi.GroupKind) ([]storeapi.ObjectID, error) {
	// Matches the partial index idx_objects_reconcile_owed WHERE reconcile_owed != 0.
	rows, err := s.query(ctx, stmtListOwedIDs, gk.Group, gk.Kind)
	if err != nil {
		return nil, err
	}
	return scanIDs(rows)
}

// GetLatestResourceVersion reports the highest version a committed write took
// (contract on storeapi.Store). A cursor above what was handed out would strand
// every write in the gap.
func (s *sqliteStore) GetLatestResourceVersion(ctx context.Context) (int64, error) {
	return s.versions.latest(), nil
}

// Stamp records a page of findings in one statement (contract on
// storeapi.Store). A missing id matches no row, which is how a vanished
// dependent is skipped; a repeated id matches its row once, which is the fold
// the contract requires.
func (s sqliteReconcileOwed) Stamp(ctx context.Context, refs []storeapi.ObjectRef) error {
	if len(refs) == 0 {
		return nil
	}
	ids := make([]storeapi.ObjectID, len(refs))
	for i, ref := range refs {
		ids[i] = ref.ID
	}
	_, err := s.exec(ctx, stmtStampOwed, jsonList(ids))
	return err
}

// ReconcileOwed().Sweep zeroes the owed count outside keep in one no-emit UPDATE
// (contract on storeapi.Store).
func (s sqliteReconcileOwed) Sweep(ctx context.Context, keep []storeapi.GroupKind) (int, error) {
	res, err := s.exec(ctx, stmtOwedSweep, jsonKinds(keep))
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected() // modernc caches the count; RowsAffected never errors
	return int(n), nil
}

// Increment and Decrement are single no-emit UPDATEs
// on the owed-wake count; the decrement floors at 0 (contract on storeapi.Store).
//
// The increment is deliberately not on that interface: production wakes come
// from Edges().Add, whose stamp must be indivisible from the edge insert, and from
// ReconcileOwed().Stamp. It stays here so tests can seed a count.
func (s sqliteReconcileOwed) Increment(ctx context.Context, id storeapi.ObjectID) error {
	_, err := s.exec(ctx, stmtIncrementOwed, id)
	return err
}

// Decrement folds the kind into the UPDATE, so a foreign id matches
// no row; the disambiguating read is paid only when nothing was written.
func (s sqliteReconcileOwed) Decrement(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, observed int64) error {
	res, err := s.exec(ctx, stmtDecrementOwed, observed, id, gk.Group, gk.Kind)
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

// Dependencies().ListStaleSince re-derives which dependents are owed a pass, bounded
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
	rows, err := s.query(ctx, stmtListStaleSince,
		after.TargetVersion, after.TargetID, after.DependentID, through, jsonKinds(kinds), limit)
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

// Dependencies().WatermarkSet upserts id's dependency watermark (see the contract on
// storeapi.Store). The EXISTS gate rides the edges primary-key prefix, and is
// also what keeps the foreign key satisfied when gcCollect removes the row
// mid-pass.
//
// The WHERE on DO UPDATE is load-bearing twice: it keeps the stored cursor
// monotonic, and it suppresses a no-advance write outright — no page dirtied —
// so a polling dependent pays no row write per pass. One predicate guards both
// columns, so reconciled_at cannot move without reconciled_against.
func (s sqliteDependencies) WatermarkSet(ctx context.Context, id storeapi.ObjectID, cursor int64) error {
	_, err := s.exec(ctx, stmtWatermarkSet, id, cursor, toMillis(time.Now().UTC()), id)
	return err
}

func (s sqliteObjects) ListIDs(ctx context.Context, gk storeapi.GroupKind) ([]storeapi.ObjectID, error) {
	rows, err := s.query(ctx, stmtListIDs, gk.Group, gk.Kind)
	if err != nil {
		return nil, err
	}
	return scanIDs(rows)
}

// ObjectWrites().MaxVersionAll reads the write log's high-water mark across every
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
	err := s.queryRow(ctx, stmtWriteLogMaxVersionAll).Scan(&at, &trimmed)
	return at, trimmed, err
}

// ObjectWrites().ListSinceAll returns the log entries above afterRV across every
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
	rows, err := s.query(ctx, stmtWriteLogListSinceAll, afterRV, limit)
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
	rows, err := s.query(ctx, stmtWriteLogPage, gk.Group, gk.Kind, gk.Group, gk.Kind, afterRV, limit)
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
	// Nil until a delete turns up: a page holds none in the common case, and
	// this runs on every page both the waker and the tailers read.
	var deletes []int64
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
func (s *sqliteStore) readImages(ctx context.Context, versions []int64) (map[int64]*storeapi.RawObject, error) {
	rows, err := s.query(ctx, stmtWriteLogImages, jsonList(versions))
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

// Objects().UpdateSpec replaces id's spec within gk. See updateSpec for the shape.
func (s sqliteObjects) UpdateSpec(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, spec []byte, specVersion int) (*storeapi.RawObject, bool, error) {
	// The scoped read enforces the kind boundary while doubling as the compare's load.
	return s.updateSpec(ctx, spec, specVersion, func(ctx context.Context) (*storeapi.RawObject, error) {
		return s.getObjectRowScoped(ctx, gk, id)
	})
}

// Objects().UpdateSpecByName replaces the spec of whatever holds name within gk. No
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
		// Before the converged return below: this reports a programming error, and
		// a write that happens to be converged must not swallow it.
		// conn for its refusal, before the compares below return without writing.
		if err := s.refuseWriteInReadFrame(ctx); err != nil {
			return err
		}
		obj, err := resolve(ctx)
		if err != nil {
			return err
		}
		// Checked before the version stamp and the byte compare: a pass on a
		// deleting row runs collection, so a spec written here is discarded, and
		// the write would still wake every watcher and dependent on the way.
		if obj.DeletionRequestedAt != nil {
			return fmt.Errorf("%w: object %d", storeapi.ErrDeletionPending, obj.ID)
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
		rv, now, err := s.recordObjectWrite(ctx, gk, obj.ID, writeOpUpdate)
		if err != nil {
			return err
		}
		// A real spec change bumps generation. Keyed on id alone: the kind boundary
		// came from the resolve above, in this same transaction — keep the read if
		// you move this statement.
		row := s.queryRow(ctx, stmtUpdateSpec, jsonText(spec), stamp, rv, now, obj.ID)
		result, err = s.scanWritten(ctx, row)
		changed = err == nil
		return err
	})
	return result, changed, err
}

func (s sqliteObjects) SetObservedGeneration(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, observedGeneration int64) (settled bool, err error) {
	if err := checkObservedGeneration(observedGeneration); err != nil {
		return false, err
	}
	// Within keeps the read-compare-write atomic.
	err = s.Within(ctx, func(ctx context.Context) error {
		var (
			generation  int64
			observedGen sql.NullInt64
		)
		if err := s.selectScoped(ctx, gk, id, stmtScopedGeneration, &generation, &observedGen); err != nil {
			return err
		}
		if err := checkObservedNotFuture(observedGeneration, generation, id); err != nil {
			return err
		}
		// No content to re-derive, so a stale report is dropped rather than rolling
		// a converged object back to unsettled — the opposite of what UpdateStatus's
		// content path does.
		settled, err = s.advanceObserved(ctx, gk, id, observedGen, observedGeneration)
		return err
	})
	return settled && err == nil, err
}

// advanceObserved writes the handshake alone — observed_generation and
// observed_at under a fresh resource_version, leaving updated_at, which tracks
// content — and reports whether it wrote. A report at or below the recorded
// generation would roll a converged object back to unsettled, so it writes
// nothing. Callers have proved the row exists in gk.
func (s *sqliteStore) advanceObserved(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, recorded sql.NullInt64, observedGeneration int64) (bool, error) {
	if recorded.Valid && recorded.Int64 >= observedGeneration {
		return false, nil
	}
	if err := s.refuseWriteInReadFrame(ctx); err != nil {
		return false, err
	}
	rv, now, err := s.recordObjectWrite(ctx, gk, id, writeOpUpdate)
	if err != nil {
		return false, err
	}
	// No RETURNING: no row reported, and the caller's scoped read proved existence.
	_, err = s.exec(ctx, stmtSetObservedGeneration, observedGeneration, now, rv, id)
	return err == nil, err
}

// checkObservedNotFuture rejects a generation the object has not reached: a
// controller can only have observed one that exists.
func checkObservedNotFuture(observedGeneration, generation int64, id storeapi.ObjectID) error {
	if generation < observedGeneration {
		return fmt.Errorf("%w: reported %d, current is %d (object %d)",
			storeapi.ErrObservedGenerationFuture, observedGeneration, generation, id)
	}
	return nil
}

// checkObservedGeneration rejects a generation no object can hold.
func checkObservedGeneration(observedGeneration int64) error {
	if observedGeneration < 1 {
		return fmt.Errorf("%w: reported %d", storeapi.ErrInvalidObservedGeneration, observedGeneration)
	}
	return nil
}

// Objects().UpdateStatus skips the write when the incoming bytes equal the stored
// ones at the same schema version: no resource_version bump, so no spurious
// watch diff or dependent wake. It touches no part of the handshake.
func (s sqliteObjects) UpdateStatus(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, status []byte, statusVersion int) (bool, error) {
	var changed bool
	// Within keeps the read-compare-write atomic.
	err := s.Within(ctx, func(ctx context.Context) error {
		// Before the no-op compare below: see updateSpec.
		if err := s.refuseWriteInReadFrame(ctx); err != nil {
			return err
		}
		// Scoped read enforces the kind boundary while doubling as the compare's
		// load — two columns, not the row.
		var (
			storedVersion int
			storedStatus  []byte
		)
		if err := s.selectScoped(ctx, gk, id, stmtScopedStatus,
			&storedVersion, &storedStatus); err != nil {
			return err
		}
		// Never downward — see stampVersion.
		stamp, err := stampVersion(storedVersion, statusVersion)
		if err != nil {
			return err
		}
		// A pass shadows this compare in memory to skip the transaction entirely
		// (statusBaseline.claim). Loosening this one widens that one: keep the
		// in-memory skip a subset, or it starts dropping writes this would make.
		if stamp == storedVersion && bytes.Equal(storedStatus, status) {
			return nil // no version bump, so no watch diff and no wake
		}
		rv, now, err := s.recordObjectWrite(ctx, gk, id, writeOpUpdate)
		if err != nil {
			return err
		}
		// Keyed on id alone: the kind boundary came from the scoped read in this
		// transaction — keep the read if you move this statement.
		_, err = s.exec(ctx, stmtUpdateStatus,
			jsonText(status), stamp, rv, now, id)
		changed = err == nil
		return err
	})
	if err != nil {
		return false, err // a failed commit wrote nothing, whatever the closure saw
	}
	return changed, nil
}

// conditionColumns is the canonical select list for a condition row;
// scanCondition reads them in order. object_id leads so one scan serves both the
// single-object and batched reads.
const conditionColumns = `object_id, type, status, reason, message, liveness,
	transitioned_at, updated_at`

// loadConditions returns id's conditions, ordered by type for a stable view.
func (s *sqliteStore) loadConditions(ctx context.Context, id storeapi.ObjectID) ([]storeapi.Condition, error) {
	rows, err := s.query(ctx, stmtLoadConditions, id)
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
// Unconfirmed is what tells the two Unknowns apart; nothing else on the wire does.
// See docs/adr/2026-08-07-a-downgraded-liveness-condition-says-so.md.
func (s *sqliteStore) downgradeLiveness(cond *storeapi.Condition) {
	if s.livenessStale(cond) {
		cond.Status = "Unknown"
		cond.Unconfirmed = true
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

// loadForConditionSet runs stmtConditionSetLoad over the conditions being written:
// the scope error, then whichever of their types are stored, keyed by type.
// Stored truth, undowngraded — see downgradeLiveness. Chunked under
// conditionChunkSize, like the upsert it feeds.
func (s *sqliteStore) loadForConditionSet(
	ctx context.Context,
	gk storeapi.GroupKind,
	id storeapi.ObjectID,
	conds []storeapi.Condition,
) (map[string]storeapi.Condition, error) {
	existing := make(map[string]storeapi.Condition, len(conds))
	for start := 0; start < len(conds); start += conditionChunkSize {
		chunk := conds[start:min(start+conditionChunkSize, len(conds))]
		if err := s.loadForConditionSetChunk(ctx, gk, id, chunk, existing); err != nil {
			return nil, err
		}
	}
	return existing, nil
}

// loadForConditionSetChunk runs one chunk, merging rows into out; it closes its
// result set so the next chunk can run on the transaction's connection.
func (s *sqliteStore) loadForConditionSetChunk(
	ctx context.Context,
	gk storeapi.GroupKind,
	id storeapi.ObjectID,
	conds []storeapi.Condition,
	out map[string]storeapi.Condition,
) error {
	types := make([]string, len(conds))
	for i, cond := range conds {
		types[i] = cond.Type
	}
	rows, err := s.query(ctx, stmtConditionSetLoad, conditionTypeList(types), id)
	if err != nil {
		return err
	}
	defer rows.Close()
	var gated bool
	for rows.Next() {
		var (
			group, kind                       string
			condType, status, reason, message sql.NullString
			liveness                          sql.NullBool
			updatedAt                         sql.NullInt64
		)
		if err := rows.Scan(&group, &kind, &condType, &status, &reason, &message,
			&liveness, &updatedAt); err != nil {
			return err
		}
		if !gated {
			if err := gateKind(gk, id, group, kind); err != nil {
				return err
			}
			gated = true
		}
		if !status.Valid { // LEFT JOIN miss: the object holds none of these types
			continue
		}
		out[condType.String] = storeapi.Condition{
			Type:      condType.String,
			Status:    status.String,
			Reason:    reason.String,
			Message:   message.String,
			Liveness:  liveness.Bool,
			UpdatedAt: fromMillis(updatedAt.Int64),
		}
	}
	// No row at all means no object: the LEFT JOIN keeps one even with no
	// conditions. A read fault outranks it — it proves nothing about the object.
	if err = rows.Err(); err == nil && !gated {
		err = storeapi.ErrNotFound // bare, like scanObject's
	}
	return err
}

// bumpObject advances id's resource_version — the visibility half of the
// condition mutators, whose semantic write lives in another table.
func (s *sqliteStore) bumpObject(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID) error {
	rv, now, err := s.recordObjectWrite(ctx, gk, id, writeOpUpdate)
	if err != nil {
		return err
	}
	_, err = s.exec(ctx, stmtBumpObject, rv, now, id)
	return err
}

// conditionUnchanged reports whether a stored condition already matches the
// proposed write — the no-op case.
func (s *sqliteStore) conditionUnchanged(existing map[string]storeapi.Condition, want storeapi.Condition) bool {
	stored, ok := existing[want.Type]
	if !ok {
		return false
	}
	// A stale liveness re-confirmation must NOT be suppressed: the write refreshes
	// updated_at and clears the downgrade; skipping it would pin Unknown forever.
	if s.livenessStale(&stored) {
		return false
	}
	return stored.Status == want.Status &&
		stored.Reason == want.Reason &&
		stored.Message == want.Message &&
		stored.Liveness == want.Liveness
}

// conditionChunkSize bounds the conditions carried per statement — the gate read
// and the upsert both. Each binds one JSON array now, so what it bounds is the
// array built and the rows written, not a parameter count. A var so tests can
// shrink it.
var conditionChunkSize = 512

func (s sqliteConditions) Set(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, conds ...storeapi.Condition) error {
	if len(conds) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(conds))
	for _, cond := range conds {
		// Every column below reaches the store through JSON, which substitutes
		// U+FFFD for bytes that are not UTF-8 rather than failing.
		for _, text := range []string{cond.Type, cond.Status, cond.Reason, cond.Message} {
			if !utf8.ValidString(text) {
				return fmt.Errorf("%w: %q", storeapi.ErrInvalidCondition, text)
			}
		}
		if seen[cond.Type] {
			return fmt.Errorf("%w: %q", storeapi.ErrDuplicateConditionType, cond.Type)
		}
		seen[cond.Type] = true
	}
	// Within keeps the condition writes and the object's version bump atomic.
	return s.Within(ctx, func(ctx context.Context) error {
		// Before the no-op return below: see updateSpec.
		if err := s.refuseWriteInReadFrame(ctx); err != nil {
			return err
		}
		// One read: a metadata-only gate — clean ErrNotFound/ErrWrongKind instead of
		// an FK violation, no blob decoded — joined to the stored conditions of the
		// types being written, since no-op suppression needs them and all key on the
		// same object.
		existing, err := s.loadForConditionSet(ctx, gk, id, conds)
		if err != nil {
			return err
		}
		changed := make([]storeapi.Condition, 0, len(conds))
		for _, cond := range conds {
			if !s.conditionUnchanged(existing, cond) {
				changed = append(changed, cond)
			}
		}
		if len(changed) == 0 {
			return nil
		}
		if err := s.upsertConditions(ctx, id, changed); err != nil {
			return err
		}
		// A condition change bumps resource_version — what watch polls and the
		// dependency waker look at. One bump for the batch: a caller writing several
		// conditions is reporting one pass, and a reader must never see half of it.
		return s.bumpObject(ctx, gk, id)
	})
}

// upsertConditions writes conds, all under one timestamp: they are one pass, so
// splitting their clocks would date the same observation differently.
func (s *sqliteStore) upsertConditions(
	ctx context.Context,
	id storeapi.ObjectID,
	conds []storeapi.Condition,
) error {
	now := toMillis(time.Now().UTC())
	for start := 0; start < len(conds); start += conditionChunkSize {
		chunk := conds[start:min(start+conditionChunkSize, len(conds))]
		if _, err := s.exec(ctx, stmtUpsertConditions, id, now, jsonConditionRows(chunk)); err != nil {
			return err
		}
	}
	return nil
}

// jsonConditionRows marshals conditions as one JSON array of
// [type, status, reason, message, liveness] rows.
//
// The one marshal helper that can be handed free text: message is whatever a
// controller wrote. What makes it lossless is not the helper but the gate in
// Conditions().Set, which refuses text that is not UTF-8 — JSON substitutes
// U+FFFD for such bytes rather than failing.
func jsonConditionRows(conds []storeapi.Condition) string {
	rows := make([][5]any, len(conds))
	for i, c := range conds {
		rows[i] = [5]any{c.Type, c.Status, c.Reason, c.Message, c.Liveness}
	}
	out, _ := json.Marshal(rows)
	return string(out)
}

func (s sqliteConditions) Delete(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, condType string) error {
	// Within keeps the delete and the version bump atomic (see Conditions().Set).
	return s.Within(ctx, func(ctx context.Context) error {
		if err := s.refuseWriteInReadFrame(ctx); err != nil {
			return err
		}
		// Same metadata-only gate as Conditions().Set.
		if err := s.checkObjectScoped(ctx, gk, id); err != nil {
			return err
		}
		res, err := s.exec(ctx, stmtDeleteCondition, id, condType)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected() // modernc caches the count; RowsAffected never errors
		// Absent condition: nothing changed, no bump.
		if n == 0 {
			return nil
		}
		return s.bumpObject(ctx, gk, id)
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
// step could name an older run latest — and Events().Add would extend a run the log
// has moved past. idx_events_latest serves this order.
func (s *sqliteStore) latestEventRun(ctx context.Context, id storeapi.ObjectID, category string) (*storeapi.Event, error) {
	e, err := scanEvent(s.queryRow(ctx, stmtLatestEventRun, id, category))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return e, nil
}

// latestEventKey returns just the run key of the newest run for (id, category),
// ok=false on an empty timeline. Events().Add needs only the key; decoding columns
// it would discard would let a decode fault mask the write.
func (s *sqliteStore) latestEventKey(ctx context.Context, id storeapi.ObjectID, category string) (evID storeapi.EventID, typ, reason string, ok bool, err error) {
	err = s.queryRow(ctx, stmtLatestEventKey, id, category).Scan(&evID, &typ, &reason)
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
		if err := s.refuseWriteInReadFrame(ctx); err != nil {
			return err
		}
		// Metadata-only gate: events carries no group/kind to fold in, and an event
		// write reports no row, so it reads none.
		if err := s.checkObjectScoped(ctx, gk, id); err != nil {
			return err
		}
		rv, err := s.nextResourceVersion(ctx)
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
			_, err := s.exec(ctx, stmtExtendEventRun, now, in.Message, jsonText(in.Detail), rv, latestID)
			return err
		}
		// New run (empty timeline or key changed): count 1, point window.
		_, err = s.exec(ctx, stmtInsertEventRun,
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
	rows, err := s.read(ctx).QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return scanEvents(rows)
}

// Events().Snapshot lists id's runs and reads its log position in one transaction:
// two reads cannot answer "these runs, as of this position" — whichever order
// they run in, a write between them is either delivered twice or dropped.
func (s sqliteEvents) Snapshot(
	ctx context.Context, id storeapi.ObjectID, q storeapi.EventQuery,
) ([]storeapi.Event, int64, error) {
	var runs []storeapi.Event
	var at int64
	err := s.withinRead(ctx, func(ctx context.Context) error {
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

// Events().ListSince pages id's log above afterRV on idx_events_object_rv.
//
// Self-wrapped for ObjectWrites().ListSince's reason: the page, the horizon and the
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
	err := s.withinRead(ctx, func(ctx context.Context) error {
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
	rows, err := s.query(ctx, stmtEventPage, id, category, afterRV, limit)
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
	ps := s.readStmt(ctx, stmtEventHorizon)
	var rv sql.NullInt64
	err := ps.QueryRowContext(ctx, id, category).Scan(&rv)
	return rv.Int64, err
}

// Events().MaxVersion reads the high-water mark of id's event log — a covering seek
// on idx_events_object_rv, NULL reading as 0. That index exists for this read
// alone; without it the plan fetches one table row per run, past overflow chains
// (TestEventsMaxVersionUsesCoveringIndex pins the plan).
func (s sqliteEvents) MaxVersion(ctx context.Context, id storeapi.ObjectID) (int64, error) {
	ps := s.readStmt(ctx, stmtEventsMaxVersion)
	var rv sql.NullInt64
	err := ps.QueryRowContext(ctx, id).Scan(&rv)
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
			raise, del, err := s.trimStmts(ctx, stmtRaiseEventHorizonByAge, stmtTrimEventsByAge)
			if err != nil {
				return err
			}
			n, err := s.trimEvents(ctx, raise, del,
				toMillis(time.Now().UTC().Add(-maxAge)))
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
	rows, err := s.query(ctx, stmtOverCapTimelines, perTimeline, capBudget)
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

	// Resolved once, not per timeline: inside a transaction each resolution
	// allocates a statement bound to it, and this loop runs to capBudget.
	raise, del, err := s.trimStmts(ctx, stmtRaiseEventHorizonOverCap, stmtTrimEventsOverCap)
	if err != nil {
		return 0, err
	}

	var total int
	for _, t := range over {
		// Newest-first, skip the cap, delete the rest. The outer key predicate is
		// what keeps both of trimEvents' statements on a seek.
		n, err := s.trimEvents(ctx, raise, del,
			t.id, t.category, t.id, t.category, perTimeline)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// trimStmts resolves a trim's horizon raise and its delete together, so a caller
// looping over timelines resolves them once.
func (s *sqliteStore) trimStmts(ctx context.Context, raise, del stmtID) (*sql.Stmt, *sql.Stmt, error) {
	raisePS, err := s.writeStmt(ctx, raise)
	if err != nil {
		return nil, nil, err
	}
	// Both are writes on one frame, so the check above answers for this one too.
	delPS, err := s.writeStmt(ctx, del)
	return raisePS, delPS, err
}

// trimEvents deletes the runs matching where and raises each affected timeline's
// horizon to the highest version it removed there.
//
// The horizon is recorded BEFORE the delete, from the same predicate in the same
// transaction: RETURNING would give the same answer, but at the cost of
// materialising every deleted run and holding a half-read cursor on the single
// connection between two statements of one transaction.
func (s *sqliteStore) trimEvents(ctx context.Context, raise, del *sql.Stmt, args ...any) (int, error) {
	if _, err := raise.ExecContext(ctx, args...); err != nil {
		return 0, err
	}
	res, err := del.ExecContext(ctx, args...)
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
		// conn for its refusal, before the absent-finalizer return below.
		if err := s.refuseWriteInReadFrame(ctx); err != nil {
			return err
		}
		// Scoped read enforces the kind boundary while loading the finalizer list —
		// the list and the deletion flag are the whole of what this write reads, and
		// the flag is wanted as a bool, so the column never leaves SQLite as a clock.
		var (
			raw             []byte
			deletionPending bool
		)
		if err := s.selectScoped(ctx, gk, id, stmtScopedFinalizers,
			&raw, &deletionPending); err != nil {
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
		rv, now, err := s.recordObjectWrite(ctx, gk, id, writeOpUpdate)
		if err != nil {
			return err
		}
		// No RETURNING: no row reported, and the scoped read proved existence.
		_, err = s.exec(ctx, stmtSetFinalizers, jsonText(marshalFinalizers(remaining)), rv, now, id)
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
// The version is drawn before the stamp, so a mark blocked by the guard burns one.
// Gaps are free: every cursor compares `>`.
func (s *sqliteStore) markForDeletion(ctx context.Context, stmt stmtID, whereArgs ...any) (storeapi.ObjectID, bool, error) {
	// conn for its refusal, before the draw below burns a version.
	if err := s.refuseWriteInReadFrame(ctx); err != nil {
		return 0, false, err
	}
	rv, err := s.nextResourceVersion(ctx)
	if err != nil {
		return 0, false, err
	}
	now := toMillis(time.Now().UTC())
	row := s.queryRow(ctx, stmt, append([]any{now, rv, now}, whereArgs...)...)
	var id storeapi.ObjectID
	var gk storeapi.GroupKind
	if err := row.Scan(&id, &gk.Group, &gk.Kind); errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	} else if err != nil {
		return 0, false, err
	}
	// The soft delete is an update: the row is still live and readable.
	if err := s.appendWriteLog(ctx, id, gk, writeOpUpdate, rv, now); err != nil {
		return 0, false, err
	}
	return id, true, nil
}

// markChunkSize bounds the ids per batched deletion mark: a measured optimum for
// the write, never a parameter ceiling. A var so tests can shrink it.
// See docs/adr/2026-07-30-store-write-shapes.md.
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
	// Ahead of the draw, so a doomed call burns no versions.
	if err := s.refuseWriteInReadFrame(ctx); err != nil {
		return nil, err
	}
	now := toMillis(time.Now().UTC())
	end, err := s.advanceResourceVersion(ctx, len(ids))
	if err != nil {
		return nil, err
	}
	first := end - int64(len(ids)) + 1
	marked := make(map[storeapi.ObjectID]bool, len(ids))
	for start := 0; start < len(ids); start += markChunkSize {
		chunk := ids[start:min(start+markChunkSize, len(ids))]
		if err := s.markManyForDeletionChunk(ctx, chunk, first+int64(start), now, marked); err != nil {
			return nil, err
		}
	}
	return marked, nil
}

// markManyForDeletionChunk stamps one chunk, handing chunk[i] version first+i.
// See docs/adr/2026-07-30-store-write-shapes.md.
func (s *sqliteStore) markManyForDeletionChunk(
	ctx context.Context,
	ids []storeapi.ObjectID,
	first, now int64,
	marked map[storeapi.ObjectID]bool,
) error {
	// Drained and closed before the insert below, which needs the single conn.
	stamped, err := scanLoggedWrites(s.query(ctx, stmtMarkManyForDeletion, now, jsonMarkPairs(ids, first)))
	if err != nil {
		return err
	}
	for _, w := range stamped {
		marked[w.id] = true
	}
	return s.appendWriteLogUpdates(ctx, stamped, now)
}

// probeDeletionByName is probeObjectScoped keyed by name; a name this kind does
// not hold is absent, not foreign. Saves the blob copies and finalizer unmarshal
// of a full row read.
func (s *sqliteStore) probeDeletionByName(ctx context.Context, gk storeapi.GroupKind, name string) (bool, error) {
	ps := s.readStmt(ctx, stmtProbeDeletionByName)
	var deletionAt sql.NullInt64
	err := ps.QueryRowContext(ctx, gk.Group, gk.Kind, name).Scan(&deletionAt)
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
	stmt stmtID, whereArgs ...any,
) (storeapi.DeletionRequestResult, error) {
	if pending, err := probe(ctx); err != nil || pending {
		return storeapi.DeletionRequestResult{}, err
	}
	var res storeapi.DeletionRequestResult
	err := s.Within(ctx, func(ctx context.Context) error {
		var err error
		if res.ID, res.Marked, err = s.markForDeletion(ctx, stmt, whereArgs...); err != nil {
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
// makes Edges().HasIncoming discount the edge and lift the target's RESTRICT. A self
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
	rows, err := s.query(ctx, stmtUnblockedTargets,
		jsonList(fromIDs), string(storeapi.RelationDependsOn))
	if err != nil {
		return nil, err
	}
	return scanObjectRefs(rows)
}

// DeletionRequests().Create marks id within gk. The kind is folded into the write, so a
// foreign id matches no row and the probe reports ErrWrongKind.
func (s sqliteDeletionRequests) Create(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID) (storeapi.DeletionRequestResult, error) {
	return s.requestDeletion(ctx,
		func(ctx context.Context) (bool, error) { return s.probeObjectScoped(ctx, gk, id) },
		stmtMarkForDeletionByID, id, gk.Group, gk.Kind)
}

// DeletionRequests().CreateByName marks the gk row holding name; the resolve and the mark
// are one statement, which is where the returned id comes from.
func (s sqliteDeletionRequests) CreateByName(ctx context.Context, gk storeapi.GroupKind, name string) (storeapi.DeletionRequestResult, error) {
	return s.requestDeletion(ctx,
		func(ctx context.Context) (bool, error) { return s.probeDeletionByName(ctx, gk, name) },
		stmtMarkForDeletionByName, gk.Group, gk.Kind, name)
}

// DeletionRequests().CreateFromOwner cascades deletion to ownerID's owned children,
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
	rows, err := s.query(ctx, stmtListOwnedChildren, ownerID, string(storeapi.RelationOwnedBy))
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
	if _, err := s.exec(ctx, stmtDeleteObject, id); err != nil {
		return err
	}
	rv, err := s.nextResourceVersion(ctx)
	if err != nil {
		return err
	}
	if err := s.appendWriteLogDelete(ctx, image, rv, toMillis(time.Now().UTC())); err != nil {
		return err
	}
	return nil
}

// edgeIsNew is the "this call creates the edge" test as a WHERE fragment — a
// probe down the edges primary key (the table itself, WITHOUT ROWID). A const so
// the two Edges().Add statements gated on it cannot drift.
const edgeIsNew = `NOT EXISTS (
	SELECT 1 FROM edges WHERE from_id = ? AND to_id = ? AND relation = ?)`

// Edges().Add inserts a (from_id, to_id, relation) edge, stamping an owed dependency
// wake for every new depends_on edge it creates (see storeapi.Store.Edges().Add). It
// does not bump resource_version — a ref is not a field of the object. It
// self-wraps in Within, so the endpoint check, stamp and insert are one atomic
// unit however it is called.
//
// The insert is deliberately the *last* write, so any residual failure lands the
// harmless way: a stamp with no edge is one spurious wake that drains; an edge
// with no stamp is a dependent stranded on a stale read that Objects().ListUnsettledIDs
// structurally cannot see.
func (s sqliteEdges) Add(ctx context.Context, fromID, toID storeapi.ObjectID, relation storeapi.Relation) (storeapi.EdgesAddResult, error) {
	var out storeapi.EdgesAddResult
	err := s.Within(ctx, func(ctx context.Context) error {
		// One round-trip, no blobs. A join, not scalar subqueries: SQLite does no
		// CSE, so separate subqueries would seek the same row twice. A missing
		// endpoint yields no row — clean ErrNotFound over an FK violation.
		var to storeapi.GroupKind
		endpoints := s.readStmt(ctx, stmtEdgeEndpointsForAdd)
		var toDeletedAt *int64
		err := endpoints.QueryRowContext(ctx, fromID, toID).
			Scan(&to.Group, &to.Kind, &toDeletedAt)
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
			res, err := s.exec(ctx, stmtStampOwedForNewEdge, fromID, fromID, toID, string(relation))
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
		// Dependencies().ListStaleSince until the owed pass. Same edge-new gate;
		// self-edges skipped to match that listing.
		if newDependency {
			if _, err := s.exec(ctx, stmtClearWatermarkForNewEdge, fromID, fromID, toID, string(relation)); err != nil {
				return err
			}
		}
		if _, err := s.exec(ctx, stmtInsertEdge, fromID, toID, string(relation)); err != nil {
			return err
		}
		out = storeapi.EdgesAddResult{
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

// Edges().Delete removes a (from_id, to_id, relation) edge; an absent edge is a
// silent no-op. Like Edges().Add it bumps nothing and joins the ambient
// transaction. Unblocked reports that the removal lifted a RESTRICT block: the
// edge was there, the target is deletion-pending and the source is not — the
// last condition because Edges().HasIncoming already discounts an edge from a
// deletion-pending source.
func (s sqliteEdges) Delete(ctx context.Context, fromID, toID storeapi.ObjectID, relation storeapi.Relation) (storeapi.EdgesDeleteResult, error) {
	res, err := s.exec(ctx, stmtDeleteEdge, fromID, toID, string(relation))
	if err != nil {
		return storeapi.EdgesDeleteResult{}, err
	}
	// modernc caches the count and cannot fail here; a wrong count would
	// silently skip the caller's push.
	if n, _ := res.RowsAffected(); n == 0 {
		return storeapi.EdgesDeleteResult{}, nil
	}
	// Unblocked is a depends_on verdict: the source-side discount below is the one
	// Edges().HasIncoming gives that relation and no other.
	if relation != storeapi.RelationDependsOn {
		return storeapi.EdgesDeleteResult{}, nil
	}
	// Both endpoints in one row, as Edges().Add does. No transaction of its own. The
	// gap costs at most a push: a source marked deletion-pending inside it reads
	// as "was already discounted", and the target waits for the sweep. See
	// docs/adr/2026-08-05-a-dropped-dependency-pushes-its-target.md.
	//
	// A failure here is reported, not swallowed. Inside an ambient Within the
	// caller's rollback unwinds the DELETE, so a retry re-runs cleanly and
	// pushes. Outside one the DELETE stands and the retry removes nothing, so
	// the report costs the push, not the collect: the sweeper is the route, and
	// it cannot be turned off.
	endpoints := s.readStmt(ctx, stmtEdgeEndpointsForDelete)
	var to storeapi.GroupKind
	var unblocked int
	err = endpoints.QueryRowContext(ctx, toID, fromID).Scan(&to.Group, &to.Kind, &unblocked)
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

// Edges().ListIncoming returns the objects pointing at toID through relation, joining edges
// to objects so each carries the GroupKind needed to route a requeue.
func (s sqliteEdges) ListIncoming(ctx context.Context, toID storeapi.ObjectID, relation storeapi.Relation) ([]storeapi.ObjectRef, error) {
	rows, err := s.query(ctx, stmtEdgesListIncoming, toID, string(relation))
	if err != nil {
		return nil, err
	}
	return scanObjectRefs(rows)
}

// Edges().GroupIncomingByID resolves Edges().ListIncoming for many targets at once,
// bucketed by target id — the incoming twin of Edges().GroupOutgoingByID. It routes
// by r.to_id and joins the source side (r.from_id).
func (s sqliteEdges) GroupIncomingByID(ctx context.Context, toIDs []storeapi.ObjectID, relation storeapi.Relation) (map[storeapi.ObjectID][]storeapi.ObjectRef, error) {
	return s.edgesByIDs(ctx, toIDs, relation, stmtEdgesGroupIncoming)
}

// idChunkSize bounds the ids a batched query carries. The ids are one JSON
// parameter now, so what it bounds is the array built and the rows returned, not
// a parameter count. A var so tests can shrink it to exercise the multi-chunk
// merge.
var idChunkSize = 30000

// edgesByIDs is the shared batched edge lookup: edges filtered by stmt's route
// column, joined on the other, bucketed by the route column stmt selects first.
// Chunked under idChunkSize.
func (s *sqliteStore) edgesByIDs(ctx context.Context, ids []storeapi.ObjectID, relation storeapi.Relation, stmt stmtID) (map[storeapi.ObjectID][]storeapi.ObjectRef, error) {
	out := make(map[storeapi.ObjectID][]storeapi.ObjectRef, len(ids))
	for start := 0; start < len(ids); start += idChunkSize {
		end := min(start+idChunkSize, len(ids))
		if err := s.edgesByIDsChunk(ctx, ids[start:end], relation, stmt, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// edgesByIDsChunk runs one chunk, merging rows into out; it closes its result
// set so the next chunk reuses that connection rather than taking another from
// the read pool.
func (s *sqliteStore) edgesByIDsChunk(ctx context.Context, ids []storeapi.ObjectID, relation storeapi.Relation, stmt stmtID, out map[storeapi.ObjectID][]storeapi.ObjectRef) error {
	rows, err := s.query(ctx, stmt, jsonList(ids), string(relation))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var route storeapi.ObjectID
		var d storeapi.ObjectRef
		// stmt must select the route column first — that is what buckets here.
		// INTEGER/TEXT NOT NULL columns; the scan never fails.
		_ = rows.Scan(&route, &d.ID, &d.Group, &d.Kind)
		out[route] = append(out[route], d)
	}
	return rows.Err()
}

// ListOutgoing returns the distinct objects fromID points at (any
// relation); DISTINCT collapses an object reached through several relations.
func (s sqliteEdges) ListOutgoing(ctx context.Context, fromID storeapi.ObjectID) ([]storeapi.ObjectRef, error) {
	rows, err := s.query(ctx, stmtEdgesListOutgoing, fromID)
	if err != nil {
		return nil, err
	}
	return scanObjectRefs(rows)
}

// Edges().ListOutgoingByRelation returns the objects fromID points at through relation. No
// DISTINCT needed: (from_id, to_id, relation) is unique.
func (s sqliteEdges) ListOutgoingByRelation(ctx context.Context, fromID storeapi.ObjectID, relation storeapi.Relation) ([]storeapi.ObjectRef, error) {
	rows, err := s.query(ctx, stmtEdgesListOutgoingByRelation, fromID, string(relation))
	if err != nil {
		return nil, err
	}
	return scanObjectRefs(rows)
}

// Edges().GroupOutgoingByID resolves Edges().ListOutgoingByRelation for many sources at
// once, bucketed by source id. It routes by r.from_id and joins the target side
// (r.to_id).
func (s sqliteEdges) GroupOutgoingByID(ctx context.Context, fromIDs []storeapi.ObjectID, relation storeapi.Relation) (map[storeapi.ObjectID][]storeapi.ObjectRef, error) {
	return s.edgesByIDs(ctx, fromIDs, relation, stmtEdgesGroupOutgoing)
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

// Edges().DeleteFinalizingDependsOn removes depends_on edges into toID whose source is
// itself deletion-pending, breaking the mutual-RESTRICT deadlock between
// finalizing objects. Bumps no version.
func (s sqliteEdges) DeleteFinalizingDependsOn(ctx context.Context, toID storeapi.ObjectID) error {
	_, err := s.exec(ctx, stmtEdgesDeleteFinalizingDependsOn, toID, string(storeapi.RelationDependsOn))
	return err
}

// Edges().HasIncoming reports whether any object with a live claim points at id. A
// depends_on edge from a deletion-pending source is ignored — otherwise two
// finalizing objects depending on each other would never clear. owned_by always
// counts: the foreground cascade waits for physical removal.
func (s sqliteEdges) HasIncoming(ctx context.Context, id storeapi.ObjectID) (bool, error) {
	var exists int
	err := s.queryRow(ctx, stmtEdgesHasIncoming, id, string(storeapi.RelationDependsOn)).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists == 1, nil
}

// DriverCursors().Get reads name's persisted cursor (see storeapi.Store). A
// missing row is ok=false: absence is the ordinary first-run state, not a fault.
func (s sqliteDriverCursors) Get(ctx context.Context, name string) (int64, bool, error) {
	var cursor int64
	err := s.queryRow(ctx, stmtGetDriverCursor, name).Scan(&cursor)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return cursor, true, nil
}

// DriverCursors().Set upserts name's persisted cursor (see storeapi.Store).
// The WHERE on DO UPDATE keeps the cursor monotonic and suppresses a no-advance
// write outright — no page dirtied on a quiet tick.
func (s sqliteDriverCursors) Set(ctx context.Context, name string, cursor int64) error {
	_, err := s.exec(ctx, stmtSetDriverCursor, name, cursor, toMillis(time.Now().UTC()))
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

// ObjectWrites().ListSince reads gk's log entries above afterRV, covered by
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
	err := s.withinRead(ctx, func(ctx context.Context) error {
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
	err := s.queryRow(ctx, stmtWriteLogTrimmedThrough, gk.Group, gk.Kind).Scan(&trimmed)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return trimmed, err
}

// ObjectWrites().MaxVersion reads gk's log position, covered by
// idx_object_writes_kind. The horizon is folded in because retention lowers the
// log's own maximum: a kind trimmed empty would report 0 against a tail parked
// higher and list on every tick. Folded, the position only ever rises, which is
// why the tail gates on > rather than !=.
func (s sqliteObjectWrites) MaxVersion(ctx context.Context, gk storeapi.GroupKind) (int64, error) {
	// One statement, not two: this is a watch's entire quiet-tick budget, and
	// both halves are covering-index seeks that fold for free.
	var at int64
	err := s.queryRow(ctx, stmtWriteLogMaxVersion, gk.Group, gk.Kind, gk.Group, gk.Kind).Scan(&at)
	return at, err
}

// ObjectWrites().Sweep trims the log and records what it removed. The delete and
// the horizon raise share a transaction: a horizon that lagged its deletes would
// read as "nothing trimmed" and let a resume succeed against a hole.
//
// RETURNING, not a bare DELETE: the horizon is per kind, so the sweep has to
// learn which kinds it touched and how far.
func (s sqliteObjectWrites) Sweep(ctx context.Context, perKind int, maxAge time.Duration) (int, error) {
	var deleted int
	err := s.Within(ctx, func(ctx context.Context) error {
		if maxAge > 0 {
			n, err := s.trimWriteLog(ctx, stmtTrimWriteLogByAge,
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
				n, err := s.trimWriteLog(ctx, stmtTrimWriteLogOverCap,
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
	rows, err := s.query(ctx, stmtWriteLogKinds)
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
// the horizon writes that follow get the transaction's connection back.
func (s *sqliteStore) deleteWriteLogRows(ctx context.Context, stmt stmtID, args ...any) (map[storeapi.GroupKind]int64, int, error) {
	// A write id, not a read: a DELETE ... RETURNING is a write however it is
	// issued, so query is what refuses it on a read frame.
	rows, err := s.query(ctx, stmt, args...)
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
func (s *sqliteStore) trimWriteLog(ctx context.Context, stmt stmtID, args ...any) (int, error) {
	highest, deleted, err := s.deleteWriteLogRows(ctx, stmt, args...)
	if err != nil {
		return 0, err
	}
	// One statement per kind, and kinds are a handful: resolving inside the loop
	// costs a bound statement each, and buys the refusal deleteWriteLogRows made.
	for k, rv := range highest {
		if _, err := s.exec(ctx, stmtRaiseWriteLogHorizon, k.Group, k.Kind, rv); err != nil {
			return 0, err
		}
	}
	return deleted, nil
}

// ObjectWrites().Snapshot lists gk and reads its log position in one transaction.
// Separately they could straddle a write: a row listed after the position was
// read would never reach the stream, since the stream starts above it.
func (s sqliteObjectWrites) Snapshot(ctx context.Context, gk storeapi.GroupKind) ([]*storeapi.RawObject, int64, error) {
	return s.snapshot(ctx, gk, func(ctx context.Context) ([]*storeapi.RawObject, error) {
		return s.Objects().List(ctx, gk)
	})
}

// ObjectWrites().SnapshotByID reads one row rather than the kind, and folds both
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

// ObjectWrites().SnapshotByOwner reads one owner's children of gk. The listing is
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
	// The longest-held read transaction in the store: list has no bound, so this
	// pins one of WithReadConnections' connections, and the WAL against a
	// checkpoint, for the whole kind. The paged reads hold theirs for one page.
	err := s.withinRead(ctx, func(ctx context.Context) error {
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

// Objects().ListByIDs reads one batch of ids in one query. The tail calls it once
// per batch rather than Objects().Get per changed object: a churny kind would
// otherwise cost a round trip per object, more than the full listing this design
// replaced.
func (s sqliteObjects) ListByIDs(ctx context.Context, gk storeapi.GroupKind, ids []storeapi.ObjectID) ([]*storeapi.RawObject, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	return s.listObjects(ctx, stmtListObjectsByIDs, jsonList(ids), gk.Group, gk.Kind)
}

// jsonWriteLogRows marshals a batch of log entries as one JSON array, in
// objectWritesColumns' order. group and kind are identifiers from Register, not
// caller data, so JSON carries them whole.
func jsonWriteLogRows(writes []loggedWrite) string {
	rows := make([][4]any, len(writes))
	for i, w := range writes {
		rows[i] = [4]any{w.rv, int64(w.id), w.gk.Group, w.gk.Kind}
	}
	out, _ := json.Marshal(rows)
	return string(out)
}

// jsonMarkPairs marshals the deletion mark's assignments as one JSON array of
// [id, version] pairs: chunk[i] takes first+i. Integers only, so marshalling
// cannot fail.
func jsonMarkPairs(ids []storeapi.ObjectID, first int64) string {
	pairs := make([][2]int64, len(ids))
	for i, id := range ids {
		pairs[i] = [2]int64{int64(id), first + int64(i)}
	}
	out, _ := json.Marshal(pairs)
	return string(out)
}

// jsonKinds marshals kinds as one JSON array of [group, kind] pairs, for a
// `(group, kind) IN (SELECT value ->> 0, value ->> 1 FROM json_each(?))` set.
// The values come from Register, never from caller data, so they are identifiers
// and marshalling them cannot lose bytes JSON has no room for.
func jsonKinds(kinds []storeapi.GroupKind) string {
	pairs := make([][2]string, len(kinds))
	for i, gk := range kinds {
		pairs[i] = [2]string{gk.Group, gk.Kind}
	}
	out, _ := json.Marshal(pairs)
	return string(out)
}

// jsonList marshals an IN list's ids as one JSON array, for
// `IN (SELECT value FROM json_each(?))`. Numbers stay numbers: json_each gives a
// JSON string TEXT affinity, which matches no INTEGER column. Integers cannot
// fail to marshal, so the error is discarded.
func jsonList[T ~int64](values []T) string {
	out, _ := json.Marshal(values)
	return string(out)
}

// conditionTypeList is jsonList for condition types, which are text and need
// their own path: JSON cannot represent a byte sequence that is not UTF-8, and
// json.Marshal substitutes U+FFFD instead of failing. The lookup would then miss
// the raw bytes the upsert stored, and every Set would read the condition as
// new. Conditions().Set refuses such a type, which is what makes this lossless.
func conditionTypeList(types []string) string {
	out, _ := json.Marshal(types)
	return string(out)
}
