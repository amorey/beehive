# Specs

Design documents for work that is agreed but not yet built. One file per piece
of work, named for the thing rather than the date — a spec is a plan, not a
record.

A spec earns a file when the design has to be settled before the code can be
written: a new public API, an invariant the implementation will lean on, or a
choice with a live alternative. Anything smaller is a `docs/TODO.md` entry.

```markdown
# <the thing being built>

- **Status:** Specified — not implemented.
- **Motivation:** the `docs/TODO.md` entry or issue this comes from.

## What is true today   (with file:line evidence)
## Design
## Implementation
## Tests
## Out of scope
```

A spec's life ends when the code lands. At that point fold whatever still
governs the code into an ADR under [`docs/adr`](../adr/README.md), shrink the
`TODO.md` entry to a pointer, and delete the spec — git holds the plan, and a
directory of live plans is worth more than a directory of finished ones.

## Index

- [Owner-scoped watches](owner-scoped-watches.md)
