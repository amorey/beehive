# The waker wakes from memory

- **Status:** Proposed. The end point the three waker ADRs were already heading
  toward.
- **Date:** 2026-08-20
- **Depends on:** [the reverse dependency index](2026-08-20-a-reverse-dependency-index.md)
  and [a commit signal carries its writes](2026-08-20-a-commit-signal-carries-its-writes.md).
  Neither is optional: this spec is the two of them meeting.

## Why

The dependency waker is already wake-driven — an idle waker arms no timer and
issues no query. What it still does on every wake is read back the write log this
process just wrote (`waker.go:482`), and then ask the edge table who depends on
what it found (`:602`).

With the entries arriving on the signal and the reverse index answering the
second question, a dependency wake becomes: commit → entries → index lookup →
enqueue. **No store read at all.**

`CLAUDE.md` says of the waker: "a commit is the only thing that wakes it". After
this it is also the only thing it needs.

## The change

`scanPages` gains a memory path. When the entries above the watermark are all in
the buffer, `dependentsWake` runs against them and the index, and the watermark
advances. Otherwise — a gap, an overflow, an unseeded waker, a resume — the
existing paged drain runs, unchanged.

Everything durable stays exactly as it is:

- the persisted cursor and its floor (`wakePersistInterval`);
- the seed at `Start`, its retry, and the reseed rule;
- the paged drain, its page budget and its backoff;
- the abandon-if-overtaken jump and its window;
- the retention horizon reporting.

None of that is the steady state any more. All of it is still the recovery path,
and the recovery path is what those ADRs are about.

## The rules this rests on

**The watermark still advances only past what was processed.** A memory-driven
wake must move the watermark exactly as a page-driven one does, and persist on the
same floor. Otherwise a restart re-scans a range that was already woken — harmless,
but it makes the cursor meaningless, and the cursor is what the abandon logic
measures against.

**The stale-dependents pass is still the guarantee.** The waker was already an
optimization over it ("it is an optimisation over the stale-dependents pass, never
a guarantee"). Making it faster does not change that, and every failure mode here
lands in the same place: a wake arrives late, from a pass that reads current state.

**A gap means fall back.** The buffer's versions must be contiguous with the
watermark for the memory path to be taken. Any hole and it reads. Do not try to
patch a hole from the store and continue in memory; take the whole drain.

## Edge cases the implementer would otherwise guess at

- **The waker subscribes across every kind**, including kinds with no controller,
  because an edge can point at one. The buffer it consumes must be store-wide, not
  the per-kind one the tailers use.

- **The retention horizon still has to be reported.** `noteTrim` and
  `noteTrimIdle` exist so a trim below the cursor is warned about once. A waker
  that never reads the store never sees the horizon. Keep the floor on which it
  checks — this is the one periodic read that must survive, and it should be rare
  rather than removed.

- **Self-edges are skipped**, as they already are in `dependentsWake` and in the
  index.

- **`drainSince` measures an unbroken drain.** A memory-driven wake is not a
  drain and must not start that clock, or the abandon jump fires against a waker
  that is keeping up perfectly.

- **The seed still runs before `Start` returns**, and until it has, every wake
  takes the store path. `dw.seeded` already gates this.

## Tests

In `waker_test.go`:

- A commit wakes a dependent with no store read at all. Assert on a counting
  store.
- A gap in the buffer falls back to the paged drain and delivers everything.
- An overflowed buffer does the same.
- The watermark after a memory-driven wake equals the watermark after the
  equivalent page-driven one — run both and compare.
- A restart resumes from the persisted cursor and re-derives nothing it already
  woke.
- The abandon jump still fires for a genuine backlog, and does not fire for a
  fast memory path.
- A retention trim below the cursor is still warned about.

## On ship

ADR: **the waker wakes from memory**, superseding the steady-state half of
[the wake-driven ADR](../adr/2026-08-05-the-waker-is-wake-driven.md) and pointing
at [the durable cursor ADR](../adr/2026-07-30-durable-waker-cursor.md),
[the abandon ADR](../adr/2026-08-05-the-waker-abandons-an-overtaken-drain.md) and
[the seed ADR](../adr/2026-08-06-the-waker-seeds-before-start-returns.md) as
unchanged — they describe the recovery path, and the recovery path is what they
were always about. Say that explicitly; it is the most useful sentence for the
next reader.

`CLAUDE.md`'s waker bullet is long and describes the scan as the mechanism.
Rewrite it around the index and the signal, with the scan named as the resume.
