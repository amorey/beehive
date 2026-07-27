# Schema-version migration: convert on read, stamp on write

- **Status:** Accepted — implemented in `migrator.go`, `sqlite/store.go`,
  `client.go`, `reconciler.go`.
- **Date:** 2026-07-27 (recorded retroactively)

## Context

Spec and Status are opaque JSON, so a consumer that reshapes a struct breaks
decode of old rows.

## Decision

A per-kind `Migrator` (`SchemaVersionSpec/Status()`, `ConvertSpec/Status(from,
raw)`) converts a blob up *on read*, at the `rawToTyped` decode boundary, before
unmarshal.

### Per-column versions

The `objects` table carries `schema_version_spec` and `schema_version_status`
(both `NOT NULL DEFAULT 0`), opaque ints the store persists and returns but never
interprets — exactly like `resource_version`.

That is what makes lazy convert-on-read sound: a status-only write re-stamps only
`schema_version_status`, and each blob converts independently from its own stored
version. There is no "rewrite the whole row" rule.

### Convert on read

`convertBlob(from, current, …)`:

- `current == 0` (kind not versioned, or nil migrator) or `from == current` —
  identity.
- `from < current` — run the converter.
- `from > current` — a downgrade (older build reading newer data); error.

### Stamp on write

Lazily: `Create` / `CreateOrUpdate` / `Update` stamp `SchemaVersionSpec()` (via
`migratorSpecVersion`, 0 if nil), `ControllerClient.UpdateStatus` stamps
`SchemaVersionStatus()` — the *separate* status-write client, easy to miss.
Condition and finalizer mutators write other rows and carry no version.

The never-downward stamping rule and its interaction with the content no-op live
in [the generation handshake ADR](2026-07-27-generation-handshake-and-noop-writes.md).

### gk-keyed registry on `*Beehive`

`WithMigrator(m)` passed to `Register` installs into `bh.migrators[gk]`. Both
decode paths — the user-facing `clientImpl.decode` and the reconciler's
`typedController.reconcile` — resolve the same migrator via `bh.migratorFor(gk)`,
so a kind can't be migrated on one path but not the other.

*Limitation:* a client-only kind (no `Register`) can't attach a migrator.

### Quarantine

A convert error, a downgrade, or a post-convert `json.Unmarshal` error are all
decode failures. `List` and `adaptWatcher` (the live watch) skip-and-log the bad
row and continue rather than aborting the whole list or killing the stream;
`Get` / `GetBySlug` still return the error.

## Consequences

A kind with no migrator behaves byte-identically to before: columns stay 0, no
conversion runs.
