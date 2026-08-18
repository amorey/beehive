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

Records here describe code that exists. Work that is only planned goes in
[`docs/specs`](../specs/README.md) and folds back into a record here when it
ships.

`CLAUDE.md` stays lean by holding only a one- or two-sentence summary of each
decision plus a link here. When a design discussion grows past a couple of
sentences in `CLAUDE.md`, move it into a new ADR and leave the summary behind.
Every record here describes code that exists. When a decision is replaced, fold
whatever still governs live code into the new record and delete the old file — git
holds the previous text, and a directory of live records is worth more than a
directory of archaeology.

Records dated before 2026-08-07 spell the pre-rename `Client`/`ControllerClient`
method names (`EventsAdd`, `OwnersGet`, `EventsWatch`, …). That prose is left as
written; see [the VerbNoun record](2026-08-07-verb-noun-on-the-client-surfaces.md)
for the mapping.

## Index

- [One process, one Beehive, and it is the store's only writer](2026-08-05-one-process-one-beehive-sole-writer.md)
- [The work queue floors how often one object is dispatched](2026-08-04-work-queue-re-enqueue-floor.md)
- [A delete request pushes its own collect, for a registered kind only](2026-08-04-a-delete-request-pushes-its-own-collect.md)
- [A cleared finalizer pushes its own collect](2026-08-05-a-cleared-finalizer-pushes-its-own-collect.md)
- [A physical delete pushes the owner it was blocking](2026-08-05-a-physical-delete-pushes-its-owner.md)
- [A dropped dependency pushes the collect it was blocking](2026-08-05-a-dropped-dependency-pushes-its-target.md)
- [A create under a deleting owner pushes that owner's collect](2026-08-05-a-create-pushes-a-deleting-owners-collect.md)
- [A deletion mark pushes the target it unblocks](2026-08-06-a-deletion-mark-pushes-the-target-it-unblocks.md)
- [A commit wakes the dependency waker](2026-08-05-a-commit-wakes-the-dependency-waker.md)
- [The dependency waker is wake-driven and has no tick](2026-08-05-the-waker-is-wake-driven.md)
- [The dependency waker subscribes and seeds before Start returns](2026-08-06-the-waker-seeds-before-start-returns.md)
- [The dependency waker abandons a drain the stale-dependents pass has overtaken](2026-08-05-the-waker-abandons-an-overtaken-drain.md)
- [The dependency waker sees a retention trim under its cursor](2026-08-06-the-waker-sees-a-retention-trim.md)
- [Object writes go to an append-only log, and watches tail it](2026-08-02-object-write-log.md)
- [One tailer per kind, woken by the commit path, above a floor tick](2026-08-03-watch-shared-tail.md)
- [A stream reports its failure beside itself, not inside it](2026-08-13-a-stream-reports-its-failure-beside-itself.md)
- [The object tail floors the gap between wake-driven drains](2026-08-05-the-object-tail-throttles-its-drains.md)
- [An owner-scoped watch resolves ownership from current state](2026-08-06-owner-scoped-watches.md)
- [The event watch reads from a cursor, and a commit wakes it](2026-08-05-events-get-a-cursor-and-a-commit-wake.md)
- [The event reads take an id](2026-08-13-the-event-reads-take-an-id.md)
- [The stale-dependents pass scans from a cursor over target versions](2026-08-03-stale-dependents-cursor.md)
- [Reclaim a client-only object's reconcile_owed count](2026-08-05-reclaim-a-client-only-owed-count.md)
- [edges WITHOUT ROWID](2026-07-26-edges-without-rowid.md)
- [auto_vacuum=INCREMENTAL, drained by the GC sweeper](2026-07-29-auto-vacuum-incremental.md)
- [Every driver is a periodic scan of the store](2026-07-28-periodic-scan-drivers.md)
- [The driver cadences are configurable, because every trigger pushes at commit](2026-08-06-driver-cadences-are-configurable.md)
- [The startup full pass may be depended on; the periodic one may not](2026-08-07-the-startup-pass-may-be-depended-on.md)
- [A spec write enqueues its own object](2026-07-31-a-spec-write-enqueues-its-own-object.md)
- [Dependency watermarks: re-derived staleness](2026-07-29-dependency-watermarks.md)
- [The dependency waker persists its scan cursor](2026-07-30-durable-waker-cursor.md)
- [Store write shapes: narrow in, narrow out](2026-07-30-store-write-shapes.md)
- [The id is the Client API's key; the name resolves through ByName siblings](2026-08-02-id-primary-key-with-byname-siblings.md)
- [Name-keyed writes](2026-07-27-name-keyed-writes.md)
- [Beehive owns the generation handshake](2026-08-18-beehive-owns-the-generation-handshake.md)
- [The pass client dies with the pass](2026-08-18-the-pass-client-dies-with-the-pass.md)
- [A downgraded liveness condition says so](2026-08-07-a-downgraded-liveness-condition-says-so.md)
- [Schema-version migration](2026-07-27-schema-version-migration.md)
- [The schema is amended in place until release](2026-07-31-amend-the-schema-in-place-until-release.md)
- [Every new depends_on edge stamps a durable owed reconcile](2026-07-29-stamp-every-new-dependency-edge.md)
- [A nested Within is a rollback boundary (SAVEPOINT)](2026-07-29-nested-within-savepoints.md)
- [Secondary lookups (owner/dependencies/dependents/owned)](2026-07-27-secondary-lookups.md)
- [Events API](2026-07-27-events-api.md)
- [Event retention is a ring per timeline, and it is off by default](2026-08-06-event-retention-is-a-ring-per-timeline.md)
- [Schedule watch](2026-07-27-schedule-watch.md)
- [The client surfaces are named VerbNoun](2026-08-07-verb-noun-on-the-client-surfaces.md)
- [NounsVerb method naming and the watch return shape](2026-07-27-noun-verb-naming.md) (superseded)
