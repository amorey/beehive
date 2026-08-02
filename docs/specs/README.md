# Specs

One file per proposed design, named `<name>.md`, opening with:

```markdown
# <the design, as a sentence>

- **Status:** Proposed | Accepted | Implemented | Withdrawn
- **Date:** YYYY-MM-DD
```

A spec describes code that does not exist yet. An
[ADR](../adr/README.md) describes code that does. That is the whole
difference, and it decides which directory a document belongs in.

A spec states the design in enough detail to build from: the schema, the
signatures, the semantics a caller can rely on, and what the design costs. It
records open questions rather than hiding them.

When a spec is built, fold what still governs live code into an ADR and delete
the spec. Git holds the previous text. A directory of live proposals is worth
more than a directory of archaeology.

`README.md` at the repository root stays the spec of what Beehive *is*. This
directory holds proposals for what it should become.

## Index

- [Objects watch: a durable write log, a split snapshot, and resume](objects-watch.md)
