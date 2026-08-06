# Specs

Design write-ups for work that has not been built. One file per piece of work,
named `YYYY-MM-DD-name.md`, opening with:

```markdown
# <the change, as a sentence>

- **Status:** Proposed | Accepted | Superseded — <one clause>
- **Date:** YYYY-MM-DD
- **Tracks:** <the TODO entry or issue this comes from>
```

A spec is not an ADR. An [ADR](../adr/README.md) records a decision about code
that exists; a spec proposes code that does not, and says what would make it
worth building, what could go wrong, and how it would be measured. When a spec
ships, fold whatever still governs the code into an ADR and delete the spec —
git holds the previous text.

Every spec here should be reachable from the [`TODO.md`](../TODO.md) entry it
came from.

## Index

Empty — the read-pool spec shipped as
[an ADR](../adr/2026-08-06-a-read-pool-beside-the-write-pool.md).
