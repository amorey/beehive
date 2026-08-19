# A pass skips a status write it can see is a no-op

- **Status:** Planned.
- **Date:** 2026-08-19
- **Issue:** [#125](https://github.com/amorey/beehive/issues/125)

## The problem

A controller that reports the same status it reported last time writes nothing —
but finding that out costs a transaction. `Objects().UpdateStatus`
(`sqlite/store.go:1491`) reads the stored bytes, compares them, and skips the
write, all inside `Within`.

The cost is not lock contention. One process is the store's sole writer, so the
`BEGIN IMMEDIATE` is uncontended. The cost is that the pool is one connection
(`OpenPool(path, 1)`, `sqlite/sqlite.go:36`), so BEGIN, SELECT and COMMIT occupy
the only connection every other reader and writer is queued behind — to conclude
that nothing changed.

For a level-triggered system that is the common case, not the rare one. One
change wakes every dependent, each dependent re-reads current state, and most of
them find exactly what they found last pass. Controllers end up guarding the
call by hand — comparing the status they are about to write against the one on
the object they were handed — and each hand-rolled guard is a copy of the check
the store already does.

The pass already holds the answer. `GetForReconcile` read the stored status
bytes at dispatch, and the `ControllerClient` is bound to that one object. So the
pass can compare in memory and return without touching the store at all.

## The decision

`ControllerClient.UpdateStatus` compares the marshalled status against the bytes
the pass was loaded with. On a match it returns `nil` without calling the store.
On anything else it calls the store as it does today.

`Objects().UpdateStatus` keeps its own compare, unchanged. The in-memory check
goes **in front of** the store's, never instead of it: the store's compare is
what keeps every other caller correct, including `AdminClient` and anything that
reaches the store directly.

The two checks are not equally safe, which is why only one of them moves. A
stale `observed_generation` stamp leaves an object unsettled and it gets
reconciled again. A wrongly skipped status write is just gone, and nothing
re-runs to notice. That is affordable here only because the baseline comes from
this process's own load of this object, under the
[sole-writer rule](../adr/2026-08-05-one-process-one-beehive-sole-writer.md), and
because the store still checks behind us.

### What a load-time baseline actually rests on

The sole-writer rule bounds other *processes*. What bounds this one is that the
`status` column has a single writer inside the store: the create sets it to
`NULL` (`sqlite/store.go:634`) and `Objects().UpdateStatus`'s `UPDATE`
(`sqlite/store.go:1522`) is the only statement that ever moves it after that.
Nothing else — no condition write, no generation stamp, no GC path — can change
the column under a live pass.

That is the invariant the fast path is built on, and a second writer added later
(a backfill, a repair verb) would break it silently. Pin it structurally — by
scanning SQL text, not the Go AST; see the test list for the shape and its
limits.

## Surface

`ControllerClient` and `AdminClient` are unchanged. The one exported-surface
change is internal to the module: `Objects().UpdateStatus` reports whether it
wrote (see "The store reports whether it wrote").

### The baseline

`controllerClientImpl` gains the status the pass was handed:

```go
// statusBaseline is what the store holds for the pass's object, as far as this
// client knows: the bytes it was loaded with, advanced by its own committed
// writes. UpdateStatus compares against it to skip a write that would change
// nothing. The version is half the comparison — a blob migrated on read decodes
// equal but must still be rewritten to be stamped at the new version.
type statusBaseline struct {
	mu      sync.Mutex
	bytes   []byte
	version int

	// outstanding counts writes issued but not yet known to have committed. While
	// it is non-zero the store's state is unknown to us, so matches reports false
	// and every write reaches the store. A write that fails or rolls back never
	// decrements, which leaves the pass on the slow path — today's cost, and the
	// only safe answer without a rollback hook.
	outstanding int
}
```

- `arm()` increments `outstanding`.
- `promote(b []byte, version int)` sets `bytes`/`version` **from its arguments**
  and decrements.
- `matches(b, version)` requires `outstanding == 0` before comparing.

`promote` must take the bytes it is promoting, never read a shared slot — see
the trap below.

`newPassClient` takes a `*statusBaseline`. `reconcile` (`reconciler.go:122`)
passes one built from `raw.Status` and `raw.StatusVersion`; `passClients.at`
(`controller.go:118`) passes `nil`, so `AdminClient` — which is handed no object —
never skips.

A `nil` baseline is the only way to say "no baseline". Do not test `len(bytes) == 0`:
a stored `NULL` status is a legitimate baseline that a first write must be
compared against.

**All three methods must be nil-receiver no-ops** — `matches`, `arm` and
`promote`. `AdminClient` never skips, so it reaches `arm` before every store call
and `promote` after every successful one; a `matches`-only nil check leaves a
nil-pointer panic on every `AdminClient` status write.

### The check

```go
func (c *controllerClientImpl[Status]) UpdateStatus(ctx context.Context, status Status) error {
	if err := c.live(); err != nil {
		return err
	}
	b, err := json.Marshal(status)
	if err != nil {
		return err
	}
	version := migratorStatusVersion(c.bh.migratorFor(c.gk))
	if c.baseline.matches(b, version) {
		return nil // the store would compare these and write nothing
	}
	c.baseline.arm() // every later call reaches the store until this promotes
	changed, err := c.bh.store.Objects().UpdateStatus(ctx, c.gk, c.id, b, version)
	if err != nil || !changed {
		// Neither case promotes: the arm stands and the pass keeps reaching the
		// store. See "Neither an error nor a store-side no-op promotes".
		return err
	}
	// Promote at commit, carrying b: a write rolled back inside the caller's
	// Within never landed, and a baseline advanced ahead of the store would skip
	// the rewrite.
	c.bh.store.AfterCommit(ctx, func(context.Context) { c.baseline.promote(b, version) })
	return c.wakeAfter(ctx, nil)
}
```

All three baseline methods take the mutex.

## Edge cases the implementer would otherwise have to guess at

**The baseline must advance on every write that lands.** `UpdateStatus(A)`
followed by `UpdateStatus(original)` in one pass: without advancing, the second
call matches the load-time bytes, skips, and leaves the stored status at `A`. The
write is silently lost.

**Advancing at commit is not enough on its own — this is the trap.**
`AfterCommit` hooks run only at the outermost commit (`sqlite/store.go:521-531`),
and a nested `Within` joins the outer queue rather than running its own. So
inside a controller's transaction the advance is invisible to every sibling call:

```go
client.Within(ctx, func(ctx context.Context) error {
    client.UpdateStatus(ctx, A)  // writes A; queues the advance on the outer commit
    client.UpdateStatus(ctx, S)  // baseline is still the load-time S -> skips
    return nil
})
```

That commits with `A` stored while the controller's last word was `S` — the write
loss the previous rule was supposed to prevent, reintroduced by the fix for it.

The two rules are reconciled by making an outstanding write *poison* the fast
path rather than update it. `arm` marks a write in flight at call time; `matches`
returns false while any is outstanding; the commit hook promotes and clears its
own. In the sequence above the second call sees an outstanding write and reaches
the store, which is exactly today's behavior.

A rollback never promotes, so the pass stays on the slow path for the rest of its
life. That costs transactions and loses nothing, and it needs no rollback hook —
which the store does not offer.

**The marker must be a count, and the hook must carry its own bytes.** A single
shared "pending bytes" slot has the same bug one level down, because the hook
would promote whatever the slot holds at commit rather than what it was
registered for, and those come apart the moment a later write fails:

```go
client.Within(ctx, func(ctx context.Context) error {
    client.UpdateStatus(ctx, A)  // writes A, queues a hook, pending = A
    client.UpdateStatus(ctx, B)  // pending = B; the store call fails
    return nil                   // controller swallows the error
})
```

`B`'s frame unwinds and queues no hook, but the slot now reads `B`. The outer
commit runs `A`'s hook, which promotes `B`. The baseline says `B`, the store
holds `A`, and a later `UpdateStatus(A)` is skipped — silent loss again.

With a counter and a hook that carries `b`, the same sequence arms twice and
promotes once, so `outstanding` never returns to zero and the pass stays
poisoned. Hooks run in registration order (`sqlite/store.go:527-530`), so when
several do promote, the last one to run is the last write that actually
committed.

**Neither an error nor a store-side no-op promotes**, and the reason is
simplicity rather than necessity. Promoting a `!changed` write *at commit* would
in fact be sound: the hook runs only if the transaction committed, and a
committed `!changed` means the stored bytes really do equal `b`. What must never
happen is promoting synchronously, which "promote at commit" already forbids.

The rule is worth keeping anyway. Leaving both paths outstanding means the
counter's invariant is one sentence — `outstanding` returns to zero only when
every issued write committed — with no case analysis over what the store
reported. The two mechanisms stay independent, and the branch that does not
promote is the branch that does not need reasoning about.

Do not write a test for it. A rollback discards every hook regardless, so a
correct implementation and one that promotes on `!changed` both leave the
counter non-zero and both poison the pass; nothing observable separates them.

**Compare bytes, not decoded values.** A status blob written at an older schema
version is migrated on read, so the value the controller hands back can decode
equal to the stored one while the stored bytes are still at the old version.
Comparing `raw.Status` and `raw.StatusVersion` lets the version-stamping rewrite
through. Two facts make the byte compare stable, and both should stay true:
`rawToTyped` (`client.go:990`) does not mutate `raw`, and `jsonText`
(`sqlite/store.go:3021`) stores blobs verbatim as TEXT with no `json()`
normalization.

**The fast path must be a strict subset of the store's skip, never equal to
it.** The store's predicate is `stampVersion(stored, v) == stored &&
bytes.Equal(...)`, and `stampVersion` (`sqlite/store.go:1339`) treats an incoming
`0` as "keep stored" — so with no migrator (`migratorStatusVersion(nil) == 0`,
`types.go:443`) against a row stored at version 2, the store skips where the
fast path does not. That direction is free: a wasted transaction, which is
today's cost. The invariant to hold is the other direction — an in-memory match
must imply the store would have skipped. When the store's compare changes, re-check
that implication; do not try to keep the two predicates identical.

**A skip wakes nothing, and that is correct.** No write, no `resource_version`
bump, nothing for a watch to see. Do not call `wakeAfter` on the skip path.

**A skip cannot report a collected object.** Today the store's scoped read
(`selectScoped`, `sqlite/store.go:754`) is what turns a row collected mid-pass
into `ErrNotFound`. A skipped call never reads, so a controller re-reporting an
unchanged status for an object that has since been collected gets `nil` where it
used to get `ErrNotFound`.

Accept this, and say so in the godoc: `UpdateStatus` promises that the stored
status matches what was passed, and for a collected object nothing needed
writing either way. The reconcile loop already treats a mid-pass collect as a
no-op success (`reconciler.go:88`), so nothing in-tree depends on the error.

The alternative — a lock-free `SELECT 1 FROM objects WHERE id = ?` before the
skip — would keep the error exactly, at the cost of the thing this change is for:
one indexed lookup still takes the store's single connection. Take it only if a
controller turns up that needs the signal.

**Concurrent `UpdateStatus` from one pass is survivable, not ordered.** The
mutex keeps the baseline's fields consistent; it does not make two goroutines'
writes meaningful. Two concurrent writes promote in `AfterCommit` registration
order, so the baseline can settle on the value that is not stored, and a later
write of the stored value is then skipped — where today the same race is merely
last-writer-wins. Survivable means no torn state and no panic. A controller that
needs a defined outcome serializes its own writes.

**Do not extend this to `SetConditions`.** `conditionUnchanged`
(`sqlite/store.go:1714`) deliberately refuses to skip a condition that matches,
when `livenessStale` fires — the write is what refreshes `updated_at` and clears
the downgrade. That predicate reads the store's `processStart`, which a pass
cannot evaluate. Conditions stay store-side.

**`AdminClient` is unaffected**, by construction: it builds a client per call
with a `nil` baseline, so every call reaches the store.

## The skip is unconditional: no option, no second verb

`UpdateStatus` always takes the fast path when it can. There is no
`WithSkipUnchanged()` and no `UpdateStatusIfChanged` beside it.

The postcondition is the same either way — the stored status is what you passed.
Two spellings that differ only in how much work happens underneath is a choice
every controller author would have to make, on no information, on the most
common write in the system. And an opt-out is an admission that the fast path is
not trusted; if it is not safe to have on, the fix is to make it safe, not to
make it a flag.

`Client.Update` cannot take this shape at all, whatever it is spelled: it
returns the updated object, so it must reach the store even when nothing
changed.

## The store reports whether it wrote

`Objects().UpdateStatus` (`internal/storeapi/storeapi.go:785`) returns
`(changed bool, err error)`: `false` on the byte-and-version match it already
detects, `true` when it wrote. `internal/storeapi` is internal, so this breaks
no user code.

There is a caller for it today. `wakeAfter` (`controller.go:145`) fires
`signalKindWritten` after every successful status write — including the ones
where the store compared bytes and wrote nothing. That wakes every watch tailer
for the kind and the dependency waker, for a write that did not happen. The
store's compare skips the write to avoid "no spurious watch diff or dependent
wake" (`sqlite/store.go:1489`), and the layer above wakes regardless, because the
store never told it. Gating a signal on what actually changed is the convention
everywhere else — `signalRequeueNow`'s own doc says so, and the spec write and
`AddDependency` both do it.

Without this the fast path also leaves two paths disagreeing: a skip wakes
nothing, a store-side no-op wakes everything. Same outcome, different behavior,
decided by which of two identical comparisons ran.

So: the wake is gated on `changed`, and the baseline promotes only when `changed`
is true — a store-side no-op wrote nothing, so there is nothing new to remember.

**`ControllerClient.UpdateStatus` keeps returning `error` alone.** Its bool would
be a second, public, breaking change: a `_,` at six example call sites, roughly
110 test call sites, 59 doc mentions, and in every user's controller — for a
value nothing in-tree reads. The wake gate needs the store's bool, not this one.
Add it later if a caller turns up; a store write returns only what a caller reads.

## Out of scope

`Client.Update`'s no-op is discovered inside `updateSpec`'s transaction
(`sqlite/store.go:1352`), and the caller does not hold the object, so there is no
in-memory baseline to compare against. The fix there would be a store-side
probe-then-transact, like `requestDeletion` (`sqlite/store.go:2394`). It is worth
having, but it is a weaker case: with one connection, a probe read still holds
the thing every other caller is queued behind. All it saves is BEGIN and COMMIT
on a transaction that writes nothing. Decide it on a measurement, in its own spec.

## Unrelated bug found while specifying this

`OpenMemory` (`sqlite/sqlite.go:46`) builds its DSN by hand and omits
`_txlock=immediate`, so every transaction in the test suite is deferred and never
takes the write lock up front. Fix it separately; do not fold it into this change.

## Test-double work this implies

`fakeObjects.UpdateStatus` (`testutils_test.go:783`) is a `panic` stub with no
compare and no version handling, and `objectsOverride.updateStatus`
(`testutils_test.go:905`) carries the old `error`-only hook signature. Both need
doing before the fake-store tests below can be written.

The fake must report `changed == true` when it writes. A stub that returns a bare
`false` leaves the client permanently poisoned, which every test that asserts
"a store call happened" would still pass — only the promotion test would fail,
and it would look like a bug in the fast path rather than in the double.

The fake cannot host every test here. `fakeStore.Within` runs `fn` inline and
`fakeStore.AfterCommit` runs hooks inline (`testutils_test.go:349-355`), and the
comment there is explicit: no test may use `fakeStore` to exercise the rollback
guarantee. Anything that depends on real transaction boundaries goes against a
real store, which package `beehive` already does (`client_test.go:224`).

## Tests

Most of the list below asserts that a store call *happens*, and a fast path that
arms once and never promotes satisfies every one of them. That is exactly how the
two bugs in this design failed. **`promote` needs a test of its own**, and it is
the first one to write.

Against the fake store, in `controller_test.go`, counting calls to
`Objects().UpdateStatus`:

- **Promotion happens.** `UpdateStatus(T)` where `T != S`, then `UpdateStatus(T)`
  again, outside any `Within`: exactly one store call. Nothing else in this file
  distinguishes a working fast path from one that is permanently poisoned.
- **The same status is not written.** A pass whose object loads with status `S`
  calls `UpdateStatus(S)`: zero store calls, `nil` returned.
- **A changed status is written.** `UpdateStatus(T)` where `T != S`: one store call.
- **A first status is written.** An object loaded with a `NULL` status: one store
  call, proving the `nil`-baseline check is not a `len == 0` check.
- **The baseline advances.** `UpdateStatus(A)` then `UpdateStatus(S)`, outside any
  `Within`: two store calls, the second carrying `S`. The write-loss regression.
- **`AdminClient` always reaches the store**, and panics on none of the three
  nil-baseline methods. Two identical `UpdateStatus` calls on a stopped beehive:
  two store calls.
- **The gate still comes first.** `UpdateStatus` after `Reconcile` returns is
  `ErrReconcileReturned`, matching bytes or not.
- **A collected object returns nil on a skip.** Object collected mid-pass, same
  status re-reported: `nil`, zero store calls. Pins the documented change rather
  than leaving it to be discovered.

Against a real store, in `controller_test.go`:

- **An outstanding write poisons the fast path.** Inside one `Within`:
  `UpdateStatus(A)` then `UpdateStatus(S)`. Assert the stored status is `S`.
  Without the arm it stores `A`.
- **A failed write does not promote an earlier one.** Inside one `Within`:
  `UpdateStatus(A)`, then `UpdateStatus(B)` where the store call fails, error
  swallowed. After the commit, `UpdateStatus(B)` again: it reaches the store and
  `B` is stored. With a shared pending slot the baseline reads `B` and the write
  is skipped.
- **A rolled-back write does not advance the baseline.** A `Within` that writes
  `A` and returns an error, then `UpdateStatus(A)` again: the second call reaches
  the store and `A` is stored.
- **A migrated status is rewritten.** Object stored at status version 1, migrator
  at 2, controller hands back a value that decodes equal: one store call, and the
  row ends at version 2.
- **A store-side no-op wakes nothing.** Bypass the baseline via `AdminClient`,
  write identical bytes twice: no `signalKindWritten` on the second. The wake the
  fast path would otherwise hide.

In `sqlite/store_test.go`:

- **The store reports what it did.** `Objects().UpdateStatus` with new bytes:
  `true`. Identical bytes at the same version: `false`. Identical bytes at a
  higher version: `true`, because the stamp is a write.
- **`objects.status` has one writer.** The invariant the load-time baseline rests
  on, asserted structurally. `TestOwnedByIsWrittenInOnePlace` (`client_test.go:1996`)
  is the precedent but not the shape: it walks the Go AST for `Edges().Add`/`Delete`
  call expressions, and a column named inside a SQL string literal is invisible to
  a call walk. Scan the SQL text in package `sqlite` instead: collect the statements
  that write `objects`, and assert the set mentioning `status` is exactly the
  create's `INSERT` and `UpdateStatus`'s `UPDATE`. Say in the test what it cannot
  see — a statement assembled by concatenation, or a future `INSERT ... SELECT`.

## Cost

Per write: one `bytes.Equal`, one mutex, one closure. The only behavioral cost is
that a failed or store-side-no-op write drops that pass back to today's path for
the rest of its life. Nothing gets slower than it is now.

## When it ships

Fold into an ADR, add the one-line summary to the `ControllerClient` bullet in
`CLAUDE.md`, and delete this file.
