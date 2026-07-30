# Specs

Designs for work that does not exist yet. One file per change, `slug.md`, opening
with a `Status:` line and the files it would touch.

The split against [docs/adr](../docs/adr/README.md) is tense: an ADR records a
decision about code that *exists*, so a spec is what an ADR cannot be. When a
spec lands, fold whatever still governs the live code into an ADR and delete the
spec — git holds the text, and a spec left beside the implementation it describes
becomes a second source of truth that drifts.

A spec that is abandoned is deleted too, with the reason moved to `TODO.md` if
the gap it addressed is still real.

## Index

- [A durable scan cursor for the dependency waker](durable-waker-cursor.md) —
  persist the waker's `resource_version` watermark so a restart resumes where it
  stopped instead of skipping the interval it was down for.
