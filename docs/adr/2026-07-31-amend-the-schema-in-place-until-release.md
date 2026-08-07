# The schema is amended in place until the first release; numbered migrations start after it

- **Status:** Accepted — implemented in `sqlite/migrations/0001_init.sql`, pinned by
  `TestTheSchemaIsOneMigration` (`sqlite/sqlite_test.go`).
- **Date:** 2026-07-31

## Context

`sqlite/migrations/` holds one file, `0001_init.sql`, and every schema change so far
was an edit to it. That was never written down as a policy. It was stated in passing
inside [the edges ADR](2026-07-26-edges-without-rowid.md), which pointed at
`docs/TODO.md` for the record — and `docs/TODO.md` no longer held one.

`docs/TODO.md` then held a change that made the question real. `idx_events_rv` served
no query: nothing ordered or filtered on `events.resource_version` alone. `Events().List`
sorts by `(last_at DESC, id DESC)`, `Events().Sweep` ranks within `(object_id, category)`
and deletes by `last_at`, and `EventsWatch` compares versions row by row from a
listing it already holds. Meanwhile `Events().MaxVersion` — the gate `EventsWatch` reads
on every quiet tick — wanted an index that did not exist, `(object_id,
resource_version)`, and fell back to `idx_events_object_cat`, which does not carry the
column. One index to drop and one to add, deferred on this question rather than on
doubt about the indexes. Several other deferred items want a schema change too, so
the question had to be answered once.

## Decision

**Amend `0001_init.sql` in place. Numbered migrations start at the first release.**
Beehive is not deployed, so no database exists that an edit could strand: a fresh
database is the only upgrade path, and it is a supported one because there is nothing
to upgrade from. `0001_init.sql` now creates `idx_events_object_rv ON events(object_id,
resource_version)` where it created `idx_events_rv`, and the comment says what the
index is for rather than what it mirrors.

`Events().MaxVersion` plans as `SEARCH events USING COVERING INDEX idx_events_object_rv
(object_id=?)` — the equality prefix selects the object's runs and the maximum sits at
the tail of that range, so a quiet watch tick reads the index and never the table.

What this buys is that **the schema is one file a reader can read**. A numbered set
records the same schema as a history, and answering "what does this column look like
now" means replaying it. That is the right trade once a database exists that has to be
carried forward, and the wrong one while the only cost of a change is rebuilding a
development database.

`internal/sqlitemigrate` stays. It already applies files in version order, each in its
own transaction, recorded in `schema_migrations`, and refuses a database newer than the
binary. Nothing about this decision needs it changed — it needs it *unused past 0001*,
which is a different thing from absent.

## Consequences

- **An edit to `0001_init.sql` never reaches a database that has already applied it.**
  `Apply` skips every version at or below the recorded one. That is exactly why the
  policy expires at release, and why `TestTheSchemaIsOneMigration` fails the moment a
  second file appears: adding one is the act that retires this record, so it should
  follow a deliberate read of this ADR rather than a habit.
- **`0002` is the first migration after release, not the first schema change.** Every
  amendment made before then is already in `0001`, so a released binary and a fresh
  database agree by construction.
- **A storage-class change is cheap now and expensive later.** The `WITHOUT ROWID`
  conversion in [the edges ADR](2026-07-26-edges-without-rowid.md) is the example:
  SQLite cannot convert a table in place, so as a forward migration it is a
  create-copy-drop-rename under `PRAGMA foreign_keys = OFF`, with a
  `PRAGMA foreign_key_check` before declaring success. Decisions of that shape belong
  before release, which is the reason that one was made early.
- **Anyone holding a development database rebuilds it after a schema edit.** There is
  no version bump to notice, so a stale file fails at the first query against a changed
  table rather than at open. Delete it and reopen.
