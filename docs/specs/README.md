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

One spec sits on its own branch and is not indexed here until it lands: it
drops `WatchEvents`' kind check.

The last spec shipped as
[the object watches' stream shape](../adr/2026-08-13-a-stream-reports-its-failure-beside-itself.md).
