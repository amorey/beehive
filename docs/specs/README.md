# Specs

One file per piece of *planned* work, named `YYYY-MM-DD-name.md`. A spec is
written to be handed to whoever implements it: it states the decision, the exact
surface to add, every edge case the implementer would otherwise have to guess
at, and the tests that pin it.

A spec is not an [ADR](../adr/README.md). An ADR describes code that **exists**
and records why it is the way it is; a spec describes code that **does not exist
yet**. When a spec ships, fold whatever still governs live code into an ADR,
update `CLAUDE.md` and `README.md`, and delete the spec — git holds the text.

A spec is also not [`TODO.md`](../TODO.md). `TODO.md` holds gaps we have
decided *not* to close yet, and says what would make them worth doing. A spec is
work we have decided to do.

Two specs sit on their own branches and are not indexed here until they land:
one gives the object watches the `EventStream` shape, the other drops
`WatchEvents`' kind check.

The last spec shipped as
[`EventStream.Retention`](../adr/2026-08-06-event-retention-is-a-ring-per-timeline.md).
