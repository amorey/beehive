# A spec write takes its transaction unconditionally

- **Status:** Accepted — the shape in `client.go` (`update`) and `sqlite/store.go`
  (`updateSpec`), measured by `BenchmarkConvergedSpecWrite`.
- **Date:** 2026-08-19

## Context

A spec write opens `Within` — `BEGIN IMMEDIATE` on a single-connection store —
reads, compares bytes, and usually writes nothing. So a converged object pays a
write transaction to be told it is converged, and
[#125](https://github.com/amorey/beehive/issues/125) asked for the same skip the
status half got in #129: probe first, and take the lock only when there is work.

The lock is not taken where it looks like it is. `client.update` wraps the store
mutator in `Within` unconditionally, so that a spec which marshals but does not
decode back rolls its write back, and `Client.Update`/`UpdateByName` are the only
callers of the spec mutators. `updateSpec`'s own `Within` is therefore always a
nested SAVEPOINT. A probe inside the store avoids nothing; it would have to run
above the client's transaction, as a `Store` read — the predicate needs the
stored bytes, `schema_version_spec`, `deletion_requested_at` and `stampVersion`'s
rule, and re-deriving that rule above the `Store` boundary is the drift the
change existed to prevent.

That priced the change at two new `Objects` members, a contract clause making
them conservative in one direction, a `fakeStore` fill-in, an `objectsOverride`
seam, and a test pinning the predicate against a transaction it cannot share code
with.

## Decision

No probe. A spec write takes its transaction whether or not it will write.

`BenchmarkConvergedSpecWrite` measures what that costs. Medians of five runs of
3000 iterations, on disk:

| | txn | no-txn | saving | changed | resolve |
|---|---|---|---|---|---|
| spec 0KB, 0 conditions | 41.3µs | 35.7µs | 5.6µs (14%) | 165.5µs | 21.4µs |
| spec 0KB, 4 conditions | 48.4 | 42.6 | 5.8 (12%) | 173.1 | 21.4 |
| spec 8KB, 0 conditions | 54.6 | 45.6 | 9.0 (16%) | 163.6 | 31.0 |
| spec 8KB, 4 conditions | 61.1 | 52.8 | 8.3 (14%) | 174.6 | 30.9 |

The saving is `BEGIN IMMEDIATE` plus a `COMMIT` that fsyncs nothing, so it is
flat in spec size and condition count. The cost is the `resolve` column: a probe
that finds work adds that read in front of a write that happens anyway. Break-even
is about **four converged writes per changed one**, and below that ratio the probe
is a loss.

Six microseconds does not pay for the surface. At the rate that motivated
[#126](https://github.com/amorey/beehive/issues/126) — an `Update` per pass over
a thousand objects on the 30s owed pass, so ~33 calls a second — it saves 0.2ms
per second of the one connection. Reaching a rate where it registers means
~10k converged writes a second, at which point 41µs each saturates the connection
on its own.

## Consequences

The pointer-comparison trap that
[#126](https://github.com/amorey/beehive/issues/126) reported stands unaddressed
by this decision, and deliberately: `if found.Spec == spec` compares addresses
and never fires, but the store's own byte compare catches the write, so the cost
of getting it wrong is the 41µs above. What that report also carried — a spec
written to a deletion-pending row — was the real defect and shipped separately.
→ [ADR](2026-08-19-a-spec-write-refuses-a-deleting-row.md)

**Folding the conditions read into the object read was measured and is also a
loss.** `GetMeta` at 21.4µs against `Get` at 35.7µs made the second query look
like the larger lever, but a `LEFT JOIN` returning both in one statement ran
43.7µs against 35.4µs with no conditions and 53.1µs against 42.3µs with four.
That harness scanned into `any`, which costs more per column than a typed scan,
so it bounds the win loosely rather than tightly — but it would have to recover
8–11µs across twenty columns to break even. The benchmark is not kept: the store
has no such query to regress.

What would reopen this: a workload whose converged spec writes are frequent
enough for 6µs to register, or a change that makes the transaction itself
dearer — a durability pragma that fsyncs an empty commit would move the saving by
orders of magnitude and is the one plausible route.
