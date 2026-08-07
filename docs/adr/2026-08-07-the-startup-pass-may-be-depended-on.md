# The startup full pass may be depended on; the periodic one may not

- **Status:** Accepted — documented in `README.md`, `options.go`, `beehive.go`,
  `reconciler.go`, `docs/reconcile-triggers.md`.
- **Date:** 2026-08-07

## Context

The two full-pass cadences shipped under one rule: *no reconcile may depend on
either*, on the grounds that a pass scaling with the object count cannot be what
guarantees convergence. The same paragraphs then told an embedder to enable one
of them to re-confirm process-scoped state — which is a dependency, stated one
sentence after it was forbidden.

The contradiction is not cosmetic, because "process-scoped state" is two things
and they differ in whether losing them costs convergence:

- **Reporting state.** A liveness condition that reads "verifying" until a
  controller in this process rewrites it. The object is converged either way;
  only the display is stale. The rule holds fine for this case, which is the one
  the docs imagined.
- **Load-bearing state.** The reconcile *is* what establishes the thing: it
  opens the connection, starts the worker, holds the watch. After a restart the
  object is not converged in any sense a user would recognise, and **no store
  column can say so** — `observed_generation == generation`, written by a process
  that no longer exists, so every store-visible measure reads settled. The owed
  pass, by construction, cannot see it.

The concrete case is a Kubernetes desktop application that registers a `Cluster`
kind whose reconcile resolves credentials, dials the API server and holds a
liveness watch open, plus a `ClusterCache` kind whose reconcile starts a sync
engine. None of it survives a restart and none of it is recorded. Without
`WithStartupFullPass(true)` a settled `Cluster` comes back unconnected and stays
that way until an unrelated event happens to wake it. Enabling it per kind at
`Register` is exactly right, and by the docs it was the forbidden thing.

The two cadences also differ in kind, not only in degree:

| | cost | repeated |
|---|---|---|
| `WithFullPassInterval` | unbounded by outstanding work | forever, at the embedder's cadence |
| `WithStartupFullPass` | O(objects), once | once per process |

The stated justification is aimed at the periodic pass and is right about it.
Applied to the startup pass it proves too much: the owed pass's own worst case is
also O(objects), and a process start is an event the embedder controls rather
than a recurring tax.

## Decision

**Split the rule. Nothing may depend on the periodic full pass; a kind may, and
sometimes must, depend on the startup pass.**

What the startup pass guarantees, stated as a guarantee rather than as a
convenience: **every object of a kind that enables it is reconciled at least once
per process.** That is real owed work — process start owes every object of the
kind a pass — for which the store simply has no column.

Both defaults stay `false`, and both remain opt-in per kind at `Register`. The
periodic pass keeps its absolute rule and its reasoning intact: its cost is
unbounded by outstanding work and it repeats forever, so a convergence bug it
hides comes back the moment an embedder lengthens it or the object set outgrows
what a sweep can carry.

`docs/reconcile-triggers.md` keeps the periodic pass out of its coverage table —
"the full pass finds it" is still a defect for every path in that document — but
now names the startup pass as the trigger for the one class of work no durable
record expresses.

**Scope: documentation only.** `WithStartupFullPass` already does the right
thing, is already per kind at `Register`, and `enqueueAll` is the right
mechanism. What was wrong is what the option said about itself.

## Consequences

**An embedder can now write down what it was already doing.** A controller whose
reconcile holds in-process state declares `WithStartupFullPass(true)` at
`Register`, and a later reader following the rule literally will not remove it.

**The periodic pass's rule got stronger by getting narrower.** It no longer has
to absorb the one legitimate dependency, so "if the only answer is *the full pass
finds it*, that path is a defect" holds without an exception to explain.

### Considered and not taken

**A rename — `WithProcessScopedState()` at `Register`.** It would say *why*
rather than *what*, and would let the periodic-pass rule stay absolute with no
exception. It is an API break for a naming gain, the option is already per kind,
and the mechanism underneath stays `enqueueAll` either way.

**Stamping `reconcile_owed` for every row at boot** — the "make it real owed
work" version. N writes replacing N queue pushes, and a crash mid-startup just
re-runs the pass next boot, so the durability buys nothing.
