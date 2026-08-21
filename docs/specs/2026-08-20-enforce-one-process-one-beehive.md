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

Refuse to open a store another process is already using, and refuse a second
`Beehive` over one store in this process.

Two mechanisms, and the choice between them is the substance of this spec:

**A lock table row.** A row in a `beehive_lock` table holding a process id, a
host, and a heartbeat timestamp. `Start` claims it, `stop` releases it, and a
stale claim (heartbeat older than some multiple of the refresh) is taken over.
Portable, inspectable, and it says who holds the lock — which is what a person
debugging this actually wants.

**`PRAGMA locking_mode=EXCLUSIVE`.** SQLite refuses the second opener outright.
No heartbeat, no staleness rule, no takeover logic. But it holds the lock for the
life of the connection, it is incompatible with a read pool on the same file (see
the read-pool item in [`TODO.md`](../TODO.md)), and the error a second process
gets is `SQLITE_BUSY` with no indication of who holds it.

**Recommendation: the lock row.** The read-pool split is on the roadmap, and a
pragma that forecloses it buys less than it costs. A stale-claim takeover is the
only hard part, and a generous timeout plus a clear error message handles the
crash case honestly.

The in-process half is separate and easy: a package-level registry keyed by the
store's file path, so a second `New` over one store fails immediately with a
clear error rather than waiting on a lock the same process holds.

## Edge cases the implementer would otherwise guess at

- **A crash leaves the row claimed.** The heartbeat is what distinguishes a crash
  from a live holder. Choose the timeout so a stopped-the-world pause cannot look
  like a crash — a minute is generous and still leaves a restart usable.

- **`OpenMemory` cannot participate.** `file::memory:` is per-connection, so each
  store is its own database. Skip the lock there, and say why in one line.

- **`Store` is opened by the embedder and passed to `New`.** The lock is claimed
  at `Start` and released at `stop`, not at `Open`: a store opened for a migration
  or for `AdminClient` maintenance on a stopped beehive must not be refused.

- **`New`'s failure modes change**, which the ADR names as the reason this was
  deferred. `Start` gains a claim error; document it in `Start`'s godoc and in
  `README.md`'s startup section.

- **Teardown obligations change too.** A `Beehive` that fails to release its
  claim on stop leaves the next start waiting out the timeout. Release must be on
  the same path as the rest of teardown, and it must run even when stop fails.

- **Do not enforce out-of-band writes.** The ADR's third clause — no writes to
  the database around a running beehive — stays documented only. Nothing can
  detect it, and pretending otherwise would be worse than saying so.

## Tests

In `beehive_test.go`:

- Two `Beehive` values over one store: the second `Start` fails, with an error
  that names the holder.
- Stop the first, start the second: succeeds. The restart case must stay easy.
- A claim whose heartbeat has expired is taken over, and the takeover is logged
  at warn.
- A `Beehive` that fails to stop cleanly still releases its claim.
- `AdminClient` against a stopped beehive's store works, unchanged.
- `OpenMemory` stores do not lock each other out, so the suite still runs.

## On ship

Amend [the sole-writer ADR](../adr/2026-08-05-one-process-one-beehive-sole-writer.md)
rather than writing a new one: it already frames this as its own rejected
alternative, and the honest edit is to move it from rejected to taken, with the
reason (the specs that follow) recorded.

`README.md` and `CLAUDE.md` both state the constraint as documented-not-enforced.
Both change.
