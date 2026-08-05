# Architecture decision records

One file per decision, named `YYYY-MM-DD-name.md`, opening with:

```markdown
# <the decision, as a sentence>

- **Status:** Accepted — implemented in `<files>`.
- **Date:** YYYY-MM-DD

## Context
## Decision
## Consequences   (optional; alternatives considered go here too)
```

`CLAUDE.md` stays lean by holding only a one- or two-sentence summary of each
decision plus a link here. When a design discussion grows past a couple of
sentences in `CLAUDE.md`, move it into a new ADR and leave the summary behind.
Every record here describes code that exists. When a decision is replaced, fold
whatever still governs live code into the new record and delete the old file — git
holds the previous text, and a directory of live records is worth more than a
directory of archaeology.

## Index

- [One process, one Beehive, and it is the store's only writer](2026-08-05-one-process-one-beehive-sole-writer.md)
- [The work queue floors how often one object is dispatched](2026-08-04-work-queue-re-enqueue-floor.md)
- [A delete request pushes its own collect, for a registered kind only](2026-08-04-a-delete-request-pushes-its-own-collect.md)
- [A cleared finalizer pushes its own collect](2026-08-05-a-cleared-finalizer-pushes-its-own-collect.md)
- [A physical delete pushes the owner it was blocking](2026-08-05-a-physical-delete-pushes-its-owner.md)
- [A commit wakes the dependency waker](2026-08-05-a-commit-wakes-the-dependency-waker.md)
- [The dependency waker is wake-driven and has no tick](2026-08-05-the-waker-is-wake-driven.md)
- [Object writes go to an append-only log, and watches tail it](2026-08-02-object-write-log.md)
- [One tailer per kind, woken by the commit path, above a floor tick](2026-08-03-watch-shared-tail.md)
- [The object tail floors the gap between wake-driven drains](2026-08-05-the-object-tail-throttles-its-drains.md)
- [The event watch reads from a cursor, and a commit wakes it](2026-08-05-events-get-a-cursor-and-a-commit-wake.md)
- [The stale-dependents pass scans from a cursor over target versions](2026-08-03-stale-dependents-cursor.md)
- [edges WITHOUT ROWID](2026-07-26-edges-without-rowid.md)
- [auto_vacuum=INCREMENTAL, drained by the GC sweeper](2026-07-29-auto-vacuum-incremental.md)
- [Every driver is a periodic scan of the store](2026-07-28-periodic-scan-drivers.md)
- [A spec write enqueues its own object](2026-07-31-a-spec-write-enqueues-its-own-object.md)
- [Dependency watermarks: re-derived staleness](2026-07-29-dependency-watermarks.md)
- [The dependency waker persists its scan cursor](2026-07-30-durable-waker-cursor.md)
- [Store write shapes: narrow in, narrow out](2026-07-30-store-write-shapes.md)
- [The id is the Client API's key; the name resolves through ByName siblings](2026-08-02-id-primary-key-with-byname-siblings.md)
- [Name-keyed writes](2026-07-27-name-keyed-writes.md)
- [The generation handshake and content no-ops](2026-07-27-generation-handshake-and-noop-writes.md)
- [Schema-version migration](2026-07-27-schema-version-migration.md)
- [The schema is amended in place until release](2026-07-31-amend-the-schema-in-place-until-release.md)
- [Every new depends_on edge stamps a durable owed reconcile](2026-07-29-stamp-every-new-dependency-edge.md)
- [A nested Within is a rollback boundary (SAVEPOINT)](2026-07-29-nested-within-savepoints.md)
- [Secondary lookups (owner/dependencies/dependents/owned)](2026-07-27-secondary-lookups.md)
- [Events API](2026-07-27-events-api.md)
- [Schedule watch](2026-07-27-schedule-watch.md)
- [NounsVerb method naming and the watch return shape](2026-07-27-noun-verb-naming.md)
