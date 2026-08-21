# Enforce one process, one beehive

- **Status:** Proposed, and it is a decision rather than an optimization. It
  gates the four specs after it.
- **Date:** 2026-08-20

## Why now

[The sole-writer ADR](../adr/2026-08-05-one-process-one-beehive-sole-writer.md)
says the constraint is documented, not enforced, and lists enforcement as
"defensible to add later". Later is now, and the reason is that the specs after
this one change what breaking the rule costs.

Today a second writer costs latency. The queues, floors and cursors are
per-`Beehive`, so two of them duplicate work and confuse each other's cursors —
but every durable record is still correct, and every backstop still converges.

With an in-memory reverse dependency index, an edge-existence set, an event run
cache and a commit signal that carries its own writes, a second writer costs
**wrong answers**: wakes that never fire because the index does not know about
the other process's edges, condition writes skipped against a stale baseline,
event runs extended against a cached key that is no longer the latest.

That is a change of category, and it is not one to discover in production.

## The decision to take

**Beehive enforces what it can see. Isolating the process is the embedder's.**

The rule has three violations, and they are not one problem:

| Violation | Owner |
| --- | --- |
| Two `Beehive` values over one `Store` | beehive — a registry keyed by the `Store` |
| Two `Store` values over one path, one process | `sqlite` — a registry keyed by the path |
| Two processes over one file | **the embedder** |

The first two are invisible to anyone but beehive: one process, one address
space, and in the first case a single connection. They are also the mistakes
people actually make — a test helper, a wiring error, a second `New` on a store
that was already handed to one. Both cost a map and a mutex.

The third is a deployment fact, and beehive is the wrong place for it.

### Why the cross-process case goes out

**Beehive cannot do it reliably.** Any lock it could take rests on `fcntl`,
which over NFS needs a working lock daemon and over some network and overlay
filesystems is advisory-only. A shared volume with two replicas is precisely the
deployment that motivates a lock and precisely where one silently stops working.

**The embedder is better equipped.** A single-replica Deployment, a Kubernetes
Lease with real leader election, a systemd unit, a supervisor that already
guarantees one instance. Whatever beehive shipped would be a weaker duplicate of
a mechanism they run already, and one they cannot see or reason about.

**It is where this codebase has put coordination twice before.** The
[drivers ADR](../adr/2026-07-28-periodic-scan-drivers.md) records that a push
path belongs above this core; the sole-writer ADR records that supporting the
multi-process shape properly is a different package. Cross-process exclusion is
coordination.

So beehive states the requirement and does not prescribe a mechanism. No lock
file, no advisory lock, no helper — `README.md` says one process must own the
store and leaves how to that.

### What enforcement actually buys

Not correctness. The in-memory indexes in the specs after this one are correct
if and only if there is one writer, whether or not anything checks. Enforcement
converts a silent wrong answer into a loud startup failure, for the two
violations where beehive can raise one.

## The surface to add

Nothing on `Store`. Two package-level registries and one sentinel each.

**In `beehive`.** `Start` registers `bh.store` in a package-level
`map[Store]*Beehive` under a package mutex, and `stop` deregisters. A store
already registered fails the start with `ErrStoreInUse`, in `types.go` beside
`ErrNotLoaded` — the file's other locally-defined sentinel. Not in the
`ErrNotFound`-through-`ErrSchemaVersionDowngrade` run above it: those are all
re-exports of `storeapi`, and this has no store counterpart.

A sentinel rather than a plain error, unlike the `already started` and
`already stopped` errors it returns beside: those report misuse of *this*
`Beehive`, and a caller holding one already knows which it is. This one reports
that a *different* `Beehive` owns the store, which is the fact a caller cannot
otherwise recover — and the two are indistinguishable if both are plain.

**In `sqlite`.** `Open` registers the absolute path in a package-level set and
`Close` deregisters. A path already open is `ErrAlreadyOpen`, a new
package-local sentinel beside `ErrInvalidOption`.

`sqliteStore` gains a `path` field, set by `Open` and left empty by
`OpenMemory`, because `Close` has nothing else to deregister with. An empty
`path` skips the deregistration.

## Edge cases the implementer would otherwise guess at

- **Register at `Start`, not at `New`.** `New` touches no store. Two `Beehive`
  values constructed over one store, only one of them started, is not a
  violation.

- **Where in `Start`.** Immediately after the `beehiveStopped`/`beehiveRunning`
  switch, so a double `Start` reports the existing error rather than this one,
  and well before `bh.waker.prime` — the registration must precede the first
  store read. **Return `ErrStoreInUse` directly, not through `abort`**, which
  wraps as "start aborted" and would frame a start that never began work.

- **`abort` must deregister.** Every failure path in `Start` runs through
  `abort`, which today tears down the waker and cancels `runCtx`. It gains the
  deregistration, or a failed start locks the store out for the life of the
  process.

- **`stop` deregisters after the drain, and a blown deadline does not skip it.**
  Both halves are a choice, and both cost something.

  *After* the drain, because `stop` flips `bh.state` and releases `bh.mu` before
  `wg.Wait()` — deregistering with the state flip would let a second `Start`
  succeed while the first beehive's loops are still draining.

  *Even on a `drainErr`*, which is the sharper one: `stop`'s own comment says
  that on a blown deadline "the loops are still running", so this hands the store
  to a second `Beehive` while the first may still be writing — exactly the
  corruption this spec gates the later specs against. It is still the right
  trade, because `bh.cancel()` has already run: those loops are cancelled and
  ending, whereas holding the registration would lock the store out for the life
  of the process over a caller's deadline being a second too short. **Log at warn
  when a `drainErr` deregisters**, since it is the one case where the guarantee
  lapses and nothing else records it.

- **Deregister by identity, not by state.** `if registry[store] == bh { delete(…) }`.
  Gating on "was running" happens to be correct, but a bare
  `delete(registry, bh.store)` after `stop`'s early return lets a never-started
  third `Beehive` evict a running one's registration. The identity check is free
  and removes the need to reason about it at all.

- **`stop` deregisters exactly once.** It returns early when another call already
  owns the teardown; that call is the one that deregisters.

- **Many beehives in one process is the normal case, and stays untouched.**
  Both registries are keyed — by the `Store` and by the path — never a global
  flag. N `Beehive` values over N distinct stores do not see each other, which is
  what the whole test suite is, parallel or not. Only a *shared* store or path
  collides.

- **The `Store` key must be comparable.** `Store` is an interface, so a
  non-comparable implementation panics as a map key. Both real stores are
  pointers and so is `fakeStore`; a decorator that wraps a store — `seedProbe` in
  `waker_test.go` — keys as itself, so two beehives over one store through two
  different wrappers evade the check. A test-only limit, worth one line and not
  worth closing.

- **Register the path in `Open`, around the call to `open`.** Claim it before
  `open` runs and drop it on any error `open` returns — one site, and the only
  shape that is neither racy nor brittle.

  **A `filepath.Abs` failure fails the `Open`.** It returns an error, and the two
  silent answers are both wrong: registering the raw path weakens the key, and
  skipping registration disables the check. Failing is also the least code.

  Registering *after* `open` succeeds would leave `Apply` and `seedVersions`
  outside the check, so two concurrent `Open` on one path would both migrate and
  both seed — the two most destructive things `open` does.

  Unwinding inside `open` instead would have to touch four failure paths that do
  not unwind alike: the migration, the write preparation and the seed each call
  `db.Close()` directly, and only the read preparation calls `s.Close()`. A
  deregistration living in `Close` would be reached by one of the four. Claiming
  in `Open` makes which unwind ran irrelevant.

- **`OpenMemory` registers nothing.** Each `file::memory:` store is its own
  database, so there is no path to collide on. The `Store` registry still covers
  it, which is what keeps the two-beehive case enforced in the whole test suite.

- **`Close` deregisters even when it returns an error.** It joins the errors
  from several closes, and a reader-pool failure must not leave the path claimed
  for the life of the process. Same point as the `drainErr` above, and the same
  reason.

- **Symlinks and bind mounts.** The path registry keys on `filepath.Abs`, so two
  names for one file evade it. A guardrail, not a boundary; do not reach for
  `EvalSymlinks` and the questions it opens.

- **`AdminClient` on a stopped beehive is unaffected**, and must stay so: the
  registries key on a running `Beehive` and an open store, and maintenance uses
  neither.

- **Do not enforce out-of-band writes.** The ADR's third clause — no writes to
  the database around a running beehive — stays documented only. Nothing can
  detect it, and pretending otherwise would be worse than saying so.

- **`Start`'s failure modes change**, which the ADR names as the reason this was
  deferred. Document it in `Start`'s godoc and in `README.md`'s startup section.

## Tests

In `beehive_test.go`:

- Two `Beehive` values over one `Store`: the second `Start` is `ErrStoreInUse`,
  and not the `already started` error its neighbour returns.
- Stop the first, start the second over the same store: succeeds. The restart
  case must stay easy — and it is already covered:
  `TestDependencyWakeSurvivesRestart` and
  `TestDependencyRequeueLostAcrossRestart` each start two beehives over one
  `OpenMemory` store, sequentially, and pass only if `stop` deregisters. They
  are the regression test for the teardown path; the new test is for the error.
- Two beehives over two stores, both running: unaffected. One case, since this
  is what every other test in the suite already exercises.
- A `Start` that fails after registering leaves the store startable. `abort`
  does not set `bh.state`, so a retry is a supported path and not a contrivance.
- A stop that blew its deadline still deregistered — a second `Start` over that
  store succeeds — and logged at warn.
- `OpenMemory` stores do not lock each other out, so the suite still runs.

In `sqlite/`:

- A second `Open` on one path is `ErrAlreadyOpen`; after `Close`, it succeeds.
  The existing sequential double-open tests (`versions_test.go`,
  `sqlite_test.go`) already close in between and must stay green untouched.
- An `Open` that fails leaves the path openable. Two existing tests already fail
  an open and reopen the path — `TestOpenReportsAMissingCounter` (a failed seed)
  and `TestOpenFailsOnAStatementItCannotPrepare` (a failed preparation) — and
  they fail inside `open` at different points. Both must stay green; between them
  they are the regression test for the unwind, which is why the claim belongs in
  `Open` and not in any of them.
- `OpenMemory` twice: no path, no collision, both usable.

## On ship

Amend [the sole-writer ADR](../adr/2026-08-05-one-process-one-beehive-sole-writer.md)
rather than writing a new one: it already frames enforcement as its own rejected
alternative, and the honest edit is to record the split — enforced within the
process, delegated across it, with the reason for each. Its "a lock file or
`PRAGMA locking_mode=EXCLUSIVE`" alternative stays rejected, and now has a better
reason than the one it carries: not that it would change `New`'s failure modes,
but that beehive cannot make the guarantee reliably and the embedder can.

`README.md` and `CLAUDE.md` both state the constraint as documented-not-enforced.
Both change, and both keep saying it for the two cases that stay that way: an
out-of-band writer, and a second process. `README.md` gains a sentence putting
process isolation on the embedder, without prescribing how.
