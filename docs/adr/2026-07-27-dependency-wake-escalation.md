# Dependency-wake failures escalate the catchup tick

- **Status:** Superseded by
  [2026-07-27-waker-watermark-replay.md](2026-07-27-waker-watermark-replay.md). The
  escalation could not run at the default configuration — its flags were read only
  inside the catchup ticker's case, and `resyncInterval` already defaults to 0 — so
  the waker now replays missed changes from a `resource_version` watermark and the
  machinery below is deleted. Kept as the record of what was tried.
- **Date:** 2026-07-27 (recorded retroactively)

## Context

The dependency waker (see
[one store-wide change stream](2026-07-27-store-wide-dependency-change-stream.md))
can lose wakes in three ways. A lost wake leaves a settled dependent that no
listing can name: `ObjectsListUnsettledIDs` structurally cannot see it, and with resync
off by default nothing else reaches it.

## Decision

All three loss points log at Warn and arm an escalation:

| loss point | escalation |
|---|---|
| `dependentsWake`' failed `EdgesGroupIncomingByID` | `resyncNextTick` — one full pass |
| a closed change stream | `resyncEveryTick` |
| a failed subscription | `resyncEveryTick` |

The single-failure case gets one pass and cannot be narrower: the lookup that
failed is what would have named the dependents. The other two keep dropping
changes rather than having dropped one, so they escalate every later tick.

The latter two are **process-wide** now that there is one stream: a single failure
kills dependency wakes for every kind, and both messages say so rather than naming
a kind that no longer scopes anything (`TestSubscribeFailureReportsWholeProcess`,
`TestDeadWakerReportsWholeProcess`).

### The escalation rides the catchup ticker, not resync

Resync is off by default — a repair hung off an opt-in knob would be dead where it
is needed most.

### The one-shot is spent on a pass that ran

`reconciler.tickResyncs()` = `resyncAlways.Load() || resyncOnce.Swap(false)`, whose
`||` short-circuit *is* the rule that a standing reason leaves the one-shot armed.

`tickResyncs` consumes the one-shot before the listing, so `enqueueAll` /
`enqueueFrom` report success, and the catchup arm re-arms via `resyncNextTick()`
when the full pass could not list. Otherwise a transient `ObjectsListIDs` failure would
swallow the repair permanently (`TestFailedEscalatedPassRearmsOneShot`).

### Escalation fans out to every registered kind

`Beehive.resyncKindsNextTick` / `EveryTick` hit **every** reconciler. Edges are
cross-kind, so escalating one kind repairs one arbitrary kind and silently spends
the repair for the rest — a bug an earlier attempt shipped;
`TestDroppedWakeEscalatesEveryKind` registers two kinds to pin it.

The primitives are named domain-agnostically — "one full pass" / "full passes from
now on" — so `beehive.go` owns the waker policy and the reconciler stays free of
it. The subscribe-failure message reports which situation the operator is in,
keyed on `hasPeriodicPass()` across *all* kinds to match the escalation's scope.
