# A downgraded liveness condition says so

- **Status:** Accepted — implemented in `sqlite/store.go`, `internal/storeapi/storeapi.go`, `types.go` and `client.go`.
- **Date:** 2026-08-07

## Context

A liveness condition is valid only inside the process that wrote it, so the read path
downgrades one an earlier process left behind: `livenessStale(cond)` is
`cond.Liveness && cond.UpdatedAt.Before(s.processStart)`, and `downgradeLiveness`
rewrites `Status` to `Unknown` when it holds.

That rewrite produced a `{Status: Unknown, Liveness: true}` indistinguishable from a
condition a controller in *this* process wrote as `Unknown` — an assessment that ran
and came back inconclusive. The two mean different things to a reader, and nothing on
the wire separated them. `processStart` is unexported and process-local, so no
consumer could recompute the predicate; a consumer in another process could not even
in principle, since the answer is about *this* process's lifetime.

The stamps make it more than a missing hint. `TransitionedAt` and `UpdatedAt` are the
stored write's, deliberately — they carry "last known Connected, since X", which is
the useful part of a downgrade and the reason not to rewrite them. But that leaves a
condition asserting `Status: Unknown` beside a `TransitionedAt` from before the
restart: a transition this process never made. A reader rendering "disconnected since
{TransitionedAt}" gets a pre-restart time against a status nobody established, and
had no way to detect the case and caveat it.

`Reason` survives the downgrade, so a consumer can lean on it — a downgraded
`Connected` reads `Unknown/Connected` while a never-probed one reads
`Unknown/Connecting`. That works only while a kind's reason vocabulary keeps its
legitimate `Unknown` from colliding with its last-known status, which is a naming
convention downstream, not a property this API guarantees.

## Decision

`Condition.Unconfirmed bool`, set by `downgradeLiveness` beside the status rewrite —
the one place the predicate is already evaluated and, until now, thrown away.

Read-only, like the stamps: `SetConditions` maps the caller's `Condition` field by
field into `storeapi.Condition` and never copies it, and `conditionUnchanged`
enumerates the compared fields, so a caller echoing a read condition back into
`Conditions().Set` is still a no-op rather than a write that would refresh
`updated_at` and clear the downgrade it just observed. Pinned by
`TestUnconfirmedIsIgnoredOnWrite`.

No column, no migration, no write path. Both read paths already funnel through
`downgradeLiveness` — `loadConditions` for a single object and `conditionsByIDsChunk`
for the batched list — so watches and every list verb inherit it, and there is no
third site to keep in step.

**`Unconfirmed`, not `Stale`.** The fact is narrow: this `Status` was synthesized by
the read path rather than written by anyone. `Stale` reads as a general "this data is
old" and invites use for the several other kinds of staleness in this package —
a dependency watermark, a watch cursor, a retention horizon — none of which it means.
`Unconfirmed` also matches the existing "until a controller re-confirms it" wording on
`Liveness`, and names the state a UI actually renders.

## Consequences

`Unconfirmed` implies `Status == ConditionUnknown` and `Liveness`, since only
`downgradeLiveness` sets it. The converse fails in both directions, which is the whole
point: an `Unknown` liveness condition may be either, and a consumer branches on the
flag alone rather than on the triple.

`Reason`, `Message` and both stamps continue to describe the *pre-downgrade* write, so
they are last-known values rather than facts about the `Unknown`. That was already
true and already documented; the flag is what lets a reader act on it — render "last
known Connected" instead of a bare `Unknown`, and suppress a "since" line it now knows
is measuring the wrong thing.

Nothing that already worked breaks. A consumer keying on the preserved `Reason` — the
existing workaround — keeps working, since the downgrade still preserves it. The flag
removes the need to arrange a reason vocabulary so the ambiguity cannot arise; it does
not forbid one.

### Alternatives considered

- **Expose `processStart` and let consumers apply the predicate.** Pushes a
  store-internal rule into every consumer, and the rule does not survive the process
  boundary the issue is actually about: a remote reader comparing its own clock to
  another process's start time is comparing the wrong two things. The store is the
  only party that can answer, so it should answer rather than publish its inputs.
- **Stamp a reserved `Reason` on the downgrade.** Destroys the last-known reason,
  which is the part consumers currently depend on to tell what the condition said
  before the restart. It trades one ambiguity for the loss of the information that
  makes a downgrade worth rendering at all.
- **Rewrite `TransitionedAt` to the downgrade.** Makes the condition internally
  consistent by discarding "since when" entirely, and would mean writing a derived
  per-process value into a field documented as the stored write's. The inconsistency
  is better resolved by explaining the status than by erasing the history.
- **Leave it to `Reason` conventions.** What is in place today. It holds by
  coincidence of spelling in the consumers that exist, and any kind whose legitimate
  `Unknown` shares a reason with its last-known status has no recourse. A one-field
  answer at the point the predicate already runs is cheaper than a convention every
  consumer must independently maintain.
