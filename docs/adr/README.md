# Architecture decision records

One file per decision, named `YYYY-MM-DD-slug.md`, opening with:

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
Records are not rewritten as the design evolves beyond them — supersede them with
a new record and mark the old one `Superseded by <file>`.

## Index

- [refs WITHOUT ROWID](2026-07-26-refs-without-rowid.md)
- [One store-wide change stream for the dependency waker](2026-07-27-store-wide-dependency-change-stream.md)
- [Three independent periodic drivers](2026-07-27-periodic-reconcile-drivers.md)
- [Dependency-wake failures escalate the catchup tick](2026-07-27-dependency-wake-escalation.md)
- [Watch fan-out conflates per object](2026-07-27-conflating-watch-fanout.md)
- [Slug-keyed writes and post-commit wakes](2026-07-27-writes-and-post-commit-wakes.md)
- [The generation handshake and content no-ops](2026-07-27-generation-handshake-and-noop-writes.md)
- [Schema-version migration](2026-07-27-schema-version-migration.md)
- [Caller-versioned dependency declaration](2026-07-27-caller-versioned-dependencies.md)
- [Secondary lookups (owner/dependencies/dependents/owned)](2026-07-27-secondary-lookups.md)
- [Events API](2026-07-27-events-api.md)
- [Schedule watch](2026-07-27-schedule-watch.md)
- [NounsVerb method naming and the watch return shape](2026-07-27-noun-verb-naming.md)
