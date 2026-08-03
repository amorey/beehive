# An in-memory edges epoch and idle gate, with no durable footprint

- **Status:** Proposed — not implemented.
- **Date:** 2026-08-03

## 1. Purpose

This document specifies the smallest form of the stale-dependents idle gate: an
in-memory edge-set counter and an in-memory baseline.

It removes one cost only. The sweep on an idle store does 190 ms of work at
250,000 edges and finds nothing. This gate skips it.

It deliberately does **not** remove the sweep after a process start. That
saving needs a durable baseline and a durable counter, which this document
rejects: see section 2.

## 2. Why this form and not a durable one

A durable gate would also skip the first sweep after a process start. It needs
an `edges_epoch` table, a baseline in `driver_cursors` scoped by a hash of the
registered kind set, and two required `Store` members.

That is rejected on two counts.

**It is expensive to unbuild.** Before the first release a table and an
interface member are free to remove. After a release, the table needs a
migration to drop, and removing an interface member is a breaking change.

**The saving it adds is the one that disappears first.**
[`stale-dependents-cursor.md`](stale-dependents-cursor.md) removes the need for
any idle gate. After it lands, an idle sweep is one empty indexed range query,
a restart resumes from a cursor rather than rescanning, and the epoch has no
consumer left. The durable form would then be a table and two dead interface
members that cannot be removed.

This form is worth building either way, because **it is deleted in an afternoon
and leaves nothing behind.**

## 3. Decision

Hold the epoch in memory on the store. Hold the baseline in memory in
`staleDependentsRun`. Add no table, no migration, and **no required `Store`
member at all**.

## 4. The gate's two values

Both values reach the driver through one optional capability:

```go
// IdleGateReader is an optional Store capability: the two values the
// stale-dependents idle gate compares. Nothing else reads them. A store that
// omits it disables the gate, and the sweep runs on every tick.
type IdleGateReader interface {
	// EdgesEpoch is a process-local counter bumped by every committed edge
	// write that changed the edge set. A restart resets it, which costs one
	// unconditional sweep.
	EdgesEpoch() uint64

	// ResourceVersionsMaxIssued is the highest resource version the store has
	// issued. It reads resource_version_seq, not any table, so it never falls.
	ResourceVersionsMaxIssued(ctx context.Context) (int64, error)
}
```

**Optional is the whole point.** Section 10 depends on it: nothing here touches
the required `Store` surface, so the entire implementation deletes without a
breaking change. `DriverCursorer` and `FreePagesReleaser` set the precedent —
an optional capability is how this codebase carries a value that only an
optimisation reads.

The two methods are folded into one interface because one driver reads both and
nothing else reads either. Split them when a second consumer appears.

The epoch is an `atomic.Uint64` on `sqliteStore`. Atomic rather than plain: the
writes come from the caller's goroutine and the read comes from the driver's.
The single-writer constraint covers the writes, not the race with the reader.
It returns no error because the value comes from memory.

### 4.1 The bump

`EdgesAdd`, `EdgesDelete` and `EdgesDeleteFinalizingDependsOn` bump it.

**The bump must be gated on the write changing the edge set**, and all three
sites take that signal from `RowsAffected`.

- `EdgesAdd`'s final statement is already
  `INSERT … ON CONFLICT(from_id, to_id, relation) DO NOTHING`. Its
  `RowsAffected` is an exact "the edge set changed" for every relation, at no
  extra cost.
- `EdgesDelete` and `EdgesDeleteFinalizingDependsOn` discard `RowsAffected`
  today and must read it.

**Do not widen `edgeIsNew` to every relation.** It is an index probe, so
extending it past `depends_on` buys a third seek for a fact the insert already
reports. Leave it as the `depends_on` gate it is; for that relation `stamped`
already carries the same signal.

Reading `RowsAffected` here is consistent with what `EdgesAdd` already does for
its `reconcile_owed` stamp, including the existing note that modernc caches the
count and cannot fail at that call.

This rule is what makes the gate useful, not what makes it correct. A
controller re-declares its edge set on every reconcile. Without the gate on a
real change, that controller bumps every pass and the baseline never matches.

### 4.2 The bump must run on the commit path

**Register the bump with `Store.AfterCommit`. Do not bump inline.**

An inline bump is visible before the edge is. That opens an interleaving the
gate cannot survive:

1. `EdgesAdd` bumps the epoch to `E+1`. Its transaction has not committed.
2. A tick reads `version = V` and `epoch = E+1`, sweeps, does not see the
   edge, and finds nothing.
3. The baseline is set to `(V, E+1)`. The edge then commits. Nothing in
   `EdgesAdd` draws a resource version — the `reconcile_owed` stamp and the
   watermark clear are both plain `UPDATE`s — so `V` does not move either.
4. The next tick reads `(V, E+1)`, matches, and skips.

`AfterCommit` closes it exactly. Its contract is that `fn` runs if and only if
the transaction committed and the frame it was registered against did not
unwind, and that it runs inline when ctx carries no transaction. Thus:

- Inside `EdgesAdd`'s self-wrap, the bump lands at commit, after the edge is
  visible.
- Inside an ambient `Within`, it lands at that transaction's commit.
- In `EdgesDelete` called with no transaction, the `Exec` has already
  autocommitted, and the hook runs inline. Still correct.
- A rolled-back frame runs no hook, so a rollback bumps nothing. The counter
  and the edge set stay in step.

This is why the in-memory form needs no rule about `EdgesDelete` self-wrapping
in `Within`. `EdgesDelete`'s missing `Within` remains a real asymmetry against
`EdgesAdd`; it is out of scope here and belongs in `docs/TODO.md`.

A crash between commit and hook is not a case: the process is gone, and the
counter and the baseline go with it.

### 4.3 The cascade needs no bump

`edges.from_id` is `ON DELETE CASCADE`, so SQLite removes outgoing edges with
no Go code on the path. That cascade fires only when an object is collected,
and a collect takes a resource version. Thus the version half of the pair moves
and the gate does not skip.

## 5. The version

`ResourceVersionsMaxIssued` reads `resource_version_seq`. It is a member of the
optional `IdleGateReader` of section 4, not of `Store`.

**Do not use `ObjectWritesMaxVersionAll`.** Its own godoc records why:
"Retention is the only thing that lowers it." The age trim in
`ObjectWritesSweep` is `written_at < cutoff` with no floor, so a store idle for
the retention period loses every row and the value reads 0 — on exactly the
store this gate targets.

A falling version is harmless here in a way it is not in the durable form,
because an in-memory baseline is overwritten rather than guarded by
set-if-greater. It is still wrong to gate on a value that moves for a reason
unrelated to staleness, so the sequence remains the right source.

## 6. Gate algorithm

`staleDependentsRun` holds `baseline` (a version, an epoch, and a set flag). If
the store is no `IdleGateReader`, the gate is off and every tick sweeps.
Otherwise each tick runs:

1. Read `version` from `ResourceVersionsMaxIssued`.
2. Read `epoch` from `EdgesEpoch`.
3. If the baseline is set and equals both, return. Do no listing.
4. Otherwise run `staleDependentsSweep`, which reports how many rows it
   enqueued and whether it completed.
5. If the sweep completed and enqueued zero rows, set the baseline to the
   `version` and `epoch` read at steps 1 and 2.

Step 5 must use the values read **before** the sweep. Any write during the
sweep then moves one of them above what is recorded, so the next tick cannot
skip on account of a write the sweep may have missed.

That argument needs both halves to move only at commit, and each half earns it
differently:

- The version is drawn inside the write's transaction, so a value the sweep
  cannot see is not yet in the sequence the tick read.
- The epoch earns it from section 4.2 alone. An inline bump would break this
  step, not merely blunt it.

A read error at step 1 must be logged and treated as "do not skip". The gate is
an optimisation and must fail open.

`staleDependentsSweep` returns nothing today. It must report the enqueued count
and whether it completed. A failed page abandons the sweep, which is not
completion, and must not set the baseline.

No kinds fingerprint is needed. The durable form needs one because a stored
baseline can outlive the kind set that produced it. An in-memory baseline
cannot: `bh.order` is frozen at `Start`, and the baseline dies with the
process.

## 7. Why the baseline is set only on an empty sweep

The sweep enqueues into the in-memory `workQueue`, and `docs/reconcile-triggers.md`
section D records that those enqueues are lost on restart.

A baseline meaning "a sweep ran here" would let a restart skip a sweep whose
findings were lost. A baseline meaning "the stale set was empty here" cannot,
because an empty sweep loses nothing.

This rule matters less here than in the durable form, since an in-memory
baseline does not survive a restart at all. It is kept because it costs nothing
in the case the gate targets — an idle store's sweeps find nothing and record —
and because it is the rule the durable form needs, so the two cannot diverge.

## 8. Soundness

The gate is sound if every event that can **create** staleness moves either
`resource_version_seq` or the epoch. Events that remove staleness do not
matter; they can only make a skipped sweep more correct.

| Event that creates staleness | What moves |
| --- | --- |
| A target's `resource_version` rises | `resource_version_seq` |
| A new `depends_on` edge, and its watermark clear | the epoch |
| An edge dropped by `DependenciesDelete` | the epoch |
| A new object, which has no watermark yet | `resource_version_seq` |

Every row reading `resource_version_seq` holds because the sequence is what
issues a `resource_version`, and no object write lands without taking one.

A collect creates no staleness and is not in the table: `edges.to_id` is
`ON DELETE RESTRICT`, so a target cannot be collected while anything depends on
it, and the `from_id` cascade only removes edges. The soft delete is an
ordinary update that takes a resource version, so it is row 1.

## 9. Cost

A tick costs one store read plus one atomic load. The 190 ms sweep is skipped
whenever nothing has changed since an empty sweep.

A busy store never skips, and pays the one extra read per tick. A store whose
version moves constantly while nothing is stale — a controller emitting events
on settled objects — also never skips, and pays nothing beyond that read. There
is no write on any path.

## 9.1 Test doubles

Three doubles implement `DependentsListStale` and are touched by this change:
`fakeStore` (`testutils_test.go`), `staleListErrorStore` and `listProbeStore`
(`waker_test.go` and `testutils_test.go`). `fakeStore` also needs the
`IdleGateReader` members, or must deliberately omit them to exercise the
no-capability fallback — `replayStore` omits `DriverCursorer` for exactly that
reason today, and the same split applies here.

`listProbeStore` is what the "does no listing" assertions count through.

## 10. Deletion

This document's implementation is removed whole when
[`stale-dependents-cursor.md`](stale-dependents-cursor.md) lands: the
`IdleGateReader` interface, the counter field, the three bumps, and the gate in
`staleDependentsRun`.

**Nothing outlives the delete.** Every piece is either unexported or a member
of an optional capability, so removing it is not a breaking change even after a
release. That is the reason section 4 folds the version read into
`IdleGateReader` instead of putting it on `Store`: it is what makes this
document safe to implement without knowing when the cursor work is scheduled.
