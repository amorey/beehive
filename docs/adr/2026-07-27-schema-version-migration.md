# Schema-version migration: convert on read, stamp on write

- **Status:** Accepted — implemented in `migrator.go`, `sqlite/store.go`,
  `client.go`, `reconciler.go`.
- **Date:** 2026-07-27 (recorded retroactively)

## Context

Spec and Status are stored as opaque JSON, so reshaping one of those structs breaks
decoding of older rows.

## Decision

A per-kind `Migrator` (`SchemaVersionSpec/Status()`, `ConvertSpec/Status(from,
raw)`) converts a blob up *on read*, at the `rawToTyped` decode boundary, before
unmarshal.

### Per-column versions

The `objects` table carries `schema_version_spec` and `schema_version_status`, both
`NOT NULL DEFAULT 0`. They are opaque ints the store persists and returns but never
interprets, exactly like `resource_version`.

Two columns is what makes lazy conversion sound. A status-only write re-stamps only
`schema_version_status`, and each blob converts from its own stored version. Nothing
ever has to rewrite the whole row.

### Convert on read

`convertBlob(from, current, …)`:

- `current == 0` (kind not versioned, or nil migrator) or `from == current` —
  identity.
- `from < current` — run the converter.
- `from > current` — a downgrade (older build reading newer data); error.

### Stamp on write

Lazily. `Create`, `GetOrCreate` and `Update` stamp `SchemaVersionSpec()`, through
`migratorSpecVersion`, which is 0 when there is no migrator.
`ControllerClient.UpdateStatus` stamps `SchemaVersionStatus()` — note that this is the
separate status-write surface, which is easy to miss. The condition and finalizer
mutators write other tables and carry no version.

The never-downward stamping rule and its interaction with the content no-op live
in [the generation handshake ADR](2026-07-27-generation-handshake-and-noop-writes.md).

### gk-keyed registry on `*Beehive`

`WithMigrator(m)` passed to `Register` installs into `bh.migrators[gk]`. Both
decode paths — the user-facing `clientImpl.decode` and the reconciler's
`typedController.reconcile` — resolve the same migrator via `bh.migratorFor(gk)`,
so a kind can't be migrated on one path but not the other.

*Limitation:* a client-only kind (no `Register`) can't attach a migrator.

### Quarantine

A failed conversion, a downgrade and a failed `json.Unmarshal` after conversion are
all decode failures. `List` and the watch reads in `watch.go` log the bad row and
skip it rather than failing the whole list or ending the stream. `Get` and
`GetByName` still return the error.

## Consequences

A kind with no migrator is unaffected: its columns stay 0 and no conversion runs.
