# TODO

Deferred work, and why. An item belongs here when it is a real defect or gap we
chose not to fix yet — not a wishlist. Each one says what would make it worth doing,
so the next reader can tell "we decided against this" from "nobody thought of it".

Once we decide to build one, the entry here shrinks to a pointer at the work, and
moves to [`reconcile-triggers.md`](reconcile-triggers.md) once the code exists.

- **A dependency cycle of length ≥ 2 never converges** — rate-limited, not fixed.
  The self-edge case *is* fixed: `dependentsWake` skips `from_id == to_id`. Two
  objects that depend on each other still wake each other forever. A's write wakes
  B, B's wakes A, and no generation ever moves, so nothing reports a problem.

  Almost any write sustains it: changed status bytes, any real condition write, or
  `DeleteFinalizer`. Only `AddEvent` is safe, because it bumps no object
  `resource_version`. Beehive's own generation stamp is safe for a different
  reason: `Objects().SetObservedGeneration` clamps, which bounds it to one write
  per generation, and in a cycle no generation moves — so the second settle writes
  nothing. A byte-identical `UpdateStatus` is now safe too, since it no longer
  carries the handshake and so writes nothing at all. Pinned by
  `TestSetObservedGenerationWakesDependentsOncePerGeneration`.

  **The contention is gone.** The work queue's re-enqueue floor bounds the loop to
  one round trip per `defaultMinRequeueInterval`, whatever the wake rate, so it no
  longer queues write transactions ahead of every other writer. Pinned by
  `TestADependencyCycleIsBoundedByTheFloor`, which runs 25 reconciles in 30ms with
  the floor off. See [the ADR](adr/2026-08-04-work-queue-re-enqueue-floor.md).

  **What is left is that it never stops.** A reconcile worker is occupied once per
  interval forever, `resource_version` numbers are consumed forever, and every
  watch sees the pair change forever — quietly, since every object is converged
  and every generation matches.

  **The remaining fix is to reject cycles when the edge is declared**, which needs
  a recursive CTE on the single connection in `AddDependency` — strictly more
  expensive than the pre-read that already sank an earlier declare-time guard. It
  is also still open whether beehive should support cycles at all, which is a
  reason not to guard hastily. Deferred on that question rather than on cost now
  that the contention is bounded.

  Tripwire: `TestAddDependencyAcceptsCycle` asserts that cycle-closing and
  self edges are both accepted today — exactly what such a guard would change.

- **A failed seed still reseeds at the mark as of its retry** — the remainder of
  the startup seed race, after
  [seeding moved into `Start`](adr/2026-08-06-the-waker-seeds-before-start-returns.md)
  closed the scheduling half. `prime` cannot fail startup, so a failed read leaves
  the waker unseeded and `run` retries on the backoff (100ms, doubling). With a
  stored cursor the retry resumes there and nothing is skipped; **without one — a
  fresh store, or a store that persists no cursor — it reads
  `ObjectWritesMaxVersionAll` as of *then*, and a write committed in between is
  below it.** `backingOff` is why a commit cannot shorten the window: an unseeded
  waker drops wakes until its retry fires.

  Latency, not divergence, for the usual reason: the stale-dependents pass derives
  staleness from `dependency_watermarks` and the racing write bumps the target's
  `resource_version`, so the next sweep lists the dependent. The cost is up to one
  `staleDependentsInterval` where the waker promises one commit, on a path that
  needs a store read to fail during startup.

  **The fix is to make a failed seed hand its window to the backstop** — force one
  stale-dependents sweep once the seed lands, rather than trying to reconstruct a
  mark nobody read. Not done because the trigger is a failed read on a cold path
  and the backstop already covers it.

  Tripwires: `TestWakerRetriesSeedOnTheNextTick` and
  `TestWakerRetriesSeedOnAFailedCursorRead` pin that a failed seed leaves the
  waker unseeded and scanning nothing — the behaviour any fix here must keep.

- **A `RequeueAfter` chain does not survive a restart** — known, not fixed. The
  dependency-wake half is closed: staleness is re-derived from
  `dependency_watermarks`, so a wake lost with the process that owed it is found again
  by the stale-dependents pass. This is the self-scheduling half, and nothing
  re-derives it, because there is nothing in the store to re-derive it from.

  A controller that keeps itself running with `Result.RequeueAfter` — polling an
  external system, re-checking a lease — leaves *no durable trace at all* once its
  object has settled. It is not unsettled, it is not `reconcile_owed`, it is not
  deletion-pending. The schedule lives only in `workQueue.alarms`, and `stop` cancels
  every one of them. So the chain restarts only if something re-dispatches the object
  cold, and on stock defaults nothing does: the owed drain finds nothing owed, and both
  full passes are off. The controller simply stops running, with every object
  converged and no error anywhere. Unlike a stranded dependency wake it does not even
  heal at the *next* restart, because every restart is equally silent.

  **No durable fix is proposed, because durability is the wrong frame for it.** A
  `RequeueAfter` is a controller's private timer, not a fact about the object, and
  persisting it would mean writing a row per poll on the single connection — the cost
  `reconcile_owed` exists to avoid.

  **The real question this raises is whether a self-polling controller should be
  expressed this way at all.** `RequeueAfter` is sound as a *retry* — a bounded
  follow-up to a pass that has more to do — and unsound as a process's only heartbeat,
  because a heartbeat that exists solely in one process's memory is not a property of
  the system. An embedder that needs periodic re-derivation has two honest options:
  own the ticker itself and call `Client.Requeue`, which is explicit about the schedule
  living in the application, or enable `WithFullPassInterval`, which is explicit about
  paying per object. Until that is written down, `RequeueAfter`'s doc reads as though
  it were a durable schedule. Documenting the boundary is the actual deliverable here,
  not a mechanism.

- **A controller that never calls `UpdateStatus` is permanently unsettled** — known,
  not fixed. `observed_generation` is NULL until the first `UpdateStatus`, and
  `ObjectsListUnsettledIDs` lists `observed_generation IS NULL OR observed_generation <
  generation`. A controller with no meaningful status to write — one whose whole job is
  a side effect elsewhere — therefore never settles a single object, and the owed pass
  re-enqueues every one of them on every tick, forever.

  It is not a hot loop (the owed-pass cadence bounds it) and it is not incorrect: the
  object genuinely has not reported convergence. But it is indistinguishable at a
  glance from a healthy system, it scales with the object count rather than with what
  changed — the one property the owed pass is designed not to have — and it defeats
  every gate on `ObservedGeneration == Generation`, including a caller waiting for one.

  **This is a documentation gap before it is a code one.** The handshake ADR says
  `ObservedGeneration` is nil until the first `UpdateStatus`; what it does not say is
  that *never* calling it is a supported-looking way to opt out of convergence
  entirely. A code fix would mean either a no-status escape hatch (an explicit
  "settled, nothing to report" call, which is `UpdateStatus` with a zero value and so
  adds surface for nothing) or having the reconciler record the handshake itself on a
  clean return — which would settle objects the controller never actually converged,
  and is a much larger change to what the generation handshake means. Neither is worth
  it against saying so plainly where controllers are introduced.

- **Four types in `Store`'s signatures have no public alias, so the interface is not
  externally implementable today** — known, not fixed, and recorded because the rest
  of this file reasons about the cost of breaking external backends as though they
  exist.

  `Store` is aliased into the root package, and most of what its methods mention is
  aliased alongside it (`RawObject`, `ObjectRef`, `ObjectsCreateInput`,
  `EventsAddInput`, `RawEvent`, `DeletionCascadeChild`). Four are not: `Condition`,
  `EdgesAddResult`, `EventQuery` and `ReconcileLoad` live only in
  `internal/storeapi`, which an external module cannot import. A backend outside
  this module cannot write those signatures at all, so it cannot satisfy the
  interface — which makes "this break costs external backends" an argument about a
  population of zero.

  Three are a one-line alias each. `Condition` is not: the root package already
  exports a richer `Condition` for the typed API, so the alias needs a distinct name
  — `RawCondition`, exactly as `RawEvent` already resolves the same collision for
  `Event`.

  Not done here because it is API surface unrelated to the change that surfaced it,
  and because it is worth deciding deliberately: aliasing these promotes every field
  of four store-shaped structs to public API, which is a commitment the internal
  package currently avoids. Revisit when an external backend is actually attempted,
  or fold into the next break that touches these types. `EventsAddInput` (2026-08-06)
  was not that break: it added an alias rather than touching any of these four.

- **`List` in a method name may be saying what the return type already says** —
  proposed, not decided, and pre-release or never. `ListOwned` → `Owned`,
  `ListOwnedObjects` → `OwnedObjects`, `ListEvents` → `Events`, and on the store
  `Edges().ListIncoming` → `Edges().Incoming`: twenty distinct method names
  across `Client`, `ControllerClient` and `Store`.

  Only the cardinality verbs are in question. `WatchOwnedObjects` and the other
  watches keep theirs — `Watch` says what the method *does*, not how many it
  returns, and a bare `OwnedObjects` could not say it streams.

  **The argument is already in the naming ADR, applied to a different surface.**
  `Object`'s relation accessors dropped their verbs — `OwnersGet`/`DependenciesList`
  became `Owner()`/`Dependencies()` — because "the `Get`/`List` cardinality signal
  moves to the return type, which already carried it"
  ([ADR](adr/2026-07-27-noun-verb-naming.md)). A plural noun returning a slice has
  said "many" twice before `List` says it a third time.

  What has to be answered is why that reasoning stopped at `Object`. The honest
  distinction is that those are pure accessors over already-loaded state, while
  `Client.OwnedObjects(ctx, id)` takes a context and issues a query — and a bare
  noun on a method that does I/O reads like a field, which hides the cost at the
  call site. That is the case to make or reject, and it is not obviously wrong
  either way.

  **The bare `List` cannot follow.** `Client`'s own CRUD omits the noun, so
  `List` has none to fall back on, and `WatchList` cannot become `Watch` —
  taken by the single-object watch. The rule would have to read "drop the verb
  where a noun is spoken, keep it where the noun is the receiver's own kind",
  which is a second exception stacked on the omit-the-noun one. Whether that is
  one convention or two is the thing to settle before touching a single name.

  Two more loose ends. Whether `Get` goes too (`GetOwner` → `Owner`), which reads
  well but gives the client the same spelling as `Object.Owner()` for a different
  operation — and if it stays, cardinality is signalled asymmetrically. And a few
  names get worse rather than better: `ObjectsListIDs` → `ObjectsIDs` wants to be
  `ObjectIDs`, which breaks the plural-prefix rule to fix the reading.

  Worth doing before the first release or not at all: every one is public API, the
  rename is the entire cost, and it is the kind of change that never gets cheaper.

- **`incoming == 0` conflates "no migrator" with "unversioned", so an old build can
  launder reshaped bytes under the stored schema version** — known, not fixed.
  Explore a `WithSchemaVersion(n)` option that lets a kind declare its schema
  version independently of registering a `Migrator`.

    **How it happens.** `convertBlob` passes a blob through untouched when the current
  version is 0, so a build with no migrator decodes a v3 row as-is. If that build's
  struct is the *older* shape, `json.Unmarshal` quietly drops the v3-only fields. The
  write-back then reports `incoming == 0`, `stampVersion` keeps the stored tag, and
  old-shaped bytes end up labelled v3. A later v3 reader sees a matching version, skips
  conversion, and misreads them. `stampVersion` is where the mislabelling happens, but
  it is not where the information was lost.

  **The obvious guard doesn't work.** Rejecting a content change when
  `stored > 0 && incoming == 0` sounds right, but `incoming == 0` means "no migrator
  registered", not "old struct", and those come apart constantly. Registering a
  `Migrator` is optional, so a build carrying the current struct with no converter yet
  writes faithful v3 bytes and reports 0. A client-only kind cannot attach a migrator
  at all, so an application driving the DB purely through `Client` reports 0 by
  construction. The benign case dominates, and the guard would wedge those writers
  permanently — every reconcile erroring — to defend against a mixed-binary rollback.

  **Nor can the store choose a better tag.** Changed bytes do not imply a reshape; an
  ordinary `Update` changes bytes too. Stamping the stored version is wrong when the
  round trip was lossy, and stamping 0 is wrong when it was faithful, because a
  restored build would then re-convert already-converted data. There is no third answer
  available at that call site, which is what makes this a missing-signal problem rather
  than a stamping bug.

  **Hence `WithSchemaVersion(n)`**: let a kind declare "my shape is v3" without
  shipping a converter, and let client-only kinds do it too. Then `incoming == 0`
  really does mean unversioned, and the guard above becomes sound.

  The read path deserves the same pass at the same time. If an unversioned build cannot
  be trusted with a v3 row, then `from > 0 && current == 0` is arguably the same
  "older build reading newer data" downgrade as `from > current`, and refusing it in
  `convertBlob` stops the lossy decode before it can become a write. Blocking only the
  write leaves a process that can observe but not act — defensible, but it should be
  one deliberate decision across both sides.

  Deferred because it needs a new public option and a read-path policy change together,
  and because the corruption requires two builds with different struct shapes sharing
  one database file. Revisit before the first real `Migrator` consumer ships a v2, or
  the first time a rollback across a schema bump has to be supported.

- **A held read transaction stops the WAL being checkpointed** — measured, not
  fixed, and not reachable yet. Reads have their own pool now, and a connection
  sitting idle in it holds no snapshot: only an open read transaction does, and
  nothing opens one today. On disk, 2000 4KB inserts with autocommit reads
  alongside:

  | readers | WAL peak | `wal_checkpoint(TRUNCATE)` | WAL after |
  | --- | --- | --- | --- |
  | autocommit only | 4.1MB | busy=0 | 0MB |
  | one open read transaction | 25.8MB | busy=1 | 25.8MB |

  So the exposure arrives with grouped reads, which hold a snapshot across
  several statements — the same change that brings the stale-snapshot hazard,
  and for the same reason. A reader parked on an old snapshot pins the WAL at
  its high-water mark, and neither `ReclaimSpace` nor `auto_vacuum` touches the
  WAL; both work on the main file.

  Two levers when it matters, neither worth adding blind: `journal_size_limit`
  on the writer's DSN, which bounds what the file is left at *after* a
  checkpoint succeeds, and a periodic `wal_checkpoint(TRUNCATE)` in the GC
  sweeper, which needs a moment with no reader holding a snapshot. Adding
  either now would be a pragma with no number behind it — the thing the page
  cache item below already declines to do. Revisit with grouped reads, and
  bound how long one may page for.

- **The page cache and `mmap_size` are untuned, and it is unclear whether tuning them
  buys anything here** — known, not fixed. `OpenPool` sets five pragmas and leaves
  SQLite's stock ~2MB cache and disabled memory mapping alone.

  One thing makes this less obviously a win than the standard advice suggests: we run
  on `modernc.org/sqlite`, a pure-Go translation rather than a cgo binding, so whether
  `mmap_size` maps anything at all there is **unverified** — that is the first thing to
  check, and if it is a no-op the item halves.

  The other used to be that the store was one connection, so a larger cache could not
  be shared the way the advice assumes. Reads have their own pool now, and the cache is
  per connection, so the sizing question is real and the cost is per reader.

  Those scans are the one real argument for it. The owed pass, the stale-dependents
  pass, the waker and the watch floor all re-read the same indexes on a fixed cadence
  forever, which is exactly the working set a page cache is for. Deferred because it is
  a tuning change with no measurement behind it, and adding pragmas we cannot show a
  number for is how a config grows cargo. Revisit with a benchmark on a store large
  enough that the scans miss cache — until then there is nothing to compare against.

- **Nothing writes down why `synchronous=NORMAL` is safe for us** — a documentation gap,
  not a defect. The pragma's usual summary is that WAL plus `NORMAL` cannot corrupt the
  database, which is true and is not the property we actually depend on: a power loss
  can also lose the *most recently committed* transactions. Stated plainly, that reads
  as a direct contradiction of the crash-safety the whole `reconcile_owed` design exists
  to provide.

  It is safe for a specific reason, and the reason is the interesting part. The loss is
  transactionally consistent at the tail: `EdgesAdd` stamps `reconcile_owed` in the same
  statement sequence as the edge it stamps for, so a lost commit takes the edge *and*
  its stamp together. There is no interleaving in which the durable trace survives and
  the work it owes does not — which is precisely the strand the stamp-every-new-edge ADR
  argues against. The same holds for a spec write and the generation it bumps.

  So the durability floor is "we may lose the last few commits entirely", never "we may
  lose the wake but keep the write". That distinction is load-bearing and lives nowhere
  — `OpenPool`'s comment lists the pragma without it. It belongs next to the
  [stamp-every-new-edge ADR](docs/adr/2026-07-29-stamp-every-new-dependency-edge.md)'s
  interleaving argument, which is where a reader is already thinking about exactly this.
  Deferred only because it is prose; it is cheap and should be written the next time
  that ADR is touched. If the invariant ever stops holding — a design that records owed
  work in a *different* transaction from the change that owes it — the answer is
  `synchronous=FULL`, and that is the tripwire to watch for.

- **A dependent- or dependency-scoped watch does not exist** — the half of the
  owner-scoped work that was not taken. Owner scope shipped
  ([ADR](adr/2026-08-06-owner-scoped-watches.md)) by resolving ownership from
  current state, which is sound only because an `owned_by` edge is written at
  create and removed at collect, both of them logged writes to the child.

  `depends_on` has neither property: `AddDependency`/`DeleteDependency`
  mutate edges freely, and `EdgesAdd`/`EdgesDelete` bump nothing, so an edge
  change is invisible to the tail. A scoped watch there needs the edge write to
  become a write to its source first — which is a change to what the write log
  means, not a filter over it.

  Worth doing when someone has a real fan-out of dependents per target. Settle
  the edge-write question before anything else: it is the whole of the work.

- **Nothing stops a child having two owners, and the typed API cannot express
  one that does** — proposed, not decided, and worth deciding pre-release
  because only one of the two directions is reversible.

  `edges` keys on `(from_id, to_id, relation)`, so a row can carry any number of
  `owned_by` edges. Nothing in the typed API makes one: `WithOwner` sets a single
  field, last-wins, and `insertObject` writes one edge. Only a direct
  `Store.EdgesAdd` reaches the state, and `TestPhysicalDeletePushesEveryOwner` is
  the one place that does.

  **The read surface already chose single-owner.** `fetchOwnerRef` resolves "id's
  single `owned_by` edge" and returns `owners[0]`; `LoadOwner` does the same;
  `Object.Owner()` and `GetOwner` each return one `ObjectRef`.
  `WatchOwnedObjects` and the delete row image follow them
  ([ADR](adr/2026-08-06-owner-scoped-watches.md)). The one multi-owner-correct
  read is `ListOwnedObjects`, and only by construction — it asks who points at an
  owner rather than collapsing a child's edges. So the state is representable,
  half-honoured, and unreachable through the public API: the worst of the three.

  **The fix is a constraint, not more fan-out.** A partial unique index —
  `CREATE UNIQUE INDEX ... ON edges(from_id) WHERE relation = 'owned_by'` — costs
  nothing at write time, cannot be bypassed by a raw `EdgesAdd`, and the schema is
  [amended in place](adr/2026-07-31-amend-the-schema-in-place-until-release.md)
  until release; `EdgesAdd` maps the violation to a sentinel as it already does
  for `ErrNameTaken`. The alternative — making every reader multi-owner-correct —
  means turning `Owner()`, `GetOwner` and `LoadOwner` into slices, which widens
  public API to support a state that public API cannot create.

  **Order is the argument.** Forbidding now and allowing later is backward
  compatible; allowing now and forbidding later is not.

  What it retires: `gc.go`'s owner fan-out becomes one lookup rather than a loop
  filtering every owner for deletion-pending, `TestPhysicalDeletePushesEveryOwner`
  goes, and the `[0]` in three readers stops being a silent choice.

  Deliberately left as is until then: the scoped watch and `objectsDelete` both
  take the first owner. Fixing those two alone would place multi-owner semantics
  in a fourth spot while `Owner()` still returns one.

- **A kind-wide event watch does not exist, so a panel over N objects runs N
  readers** — known, not fixed. `WatchEvents` is per object, and each stream is
  its own receiver, goroutine, timer and gate over its own cursor (see
  [the ADR](adr/2026-08-05-events-get-a-cursor-and-a-commit-wake.md)). That is
  cheaper than the poll it replaced at any fan-out, and still linear in streams.

  A shared reader would need what the object tail has — one cursor per kind and a
  fan-out — which the events log cannot serve today: it is indexed by object, not
  by kind, so a kind-wide tail has no seek to ride. Revisit when a consumer holds
  enough streams for the goroutine count to matter, and expect it to need an index
  before it needs an API.

- **`WatchEvents` requires a registered controller for the client's own kind**,
  which since [the reads take an id](adr/2026-08-13-the-event-reads-take-an-id.md)
  is plainly a property of the caller and not of the target: a registered kind's
  client may watch a client-only kind's object, but a client-only kind's client
  may watch nothing at all. Deliberate, not deferred — the requirement is one
  line and nothing in the read path needs a reconciler.

  Dropping it would mean deciding what a watch on a beehive with no controllers
  is for, and `NewClient` is the surface that would have to answer. What would
  make it worth doing: a consumer that only observes — a panel, an exporter —
  built against a beehive whose kinds it does not implement. Until one exists,
  registering the kind is a one-line workaround and the requirement documents
  itself.

- **Nothing can append to an event log except the object's own pass** — removed
  deliberately, in two steps, both under
  [the pass-scoped client](adr/2026-08-18-a-controller-client-exists-only-for-a-pass.md).
  `AddEvent` lives on the `ControllerClient`, which exists only for the pass it is
  handed to, so a background prober — an ordinary app goroutine watching a
  connection — has no way to record what it saw. It has to hold the outcome in
  memory and call `Client.Requeue`, which is a round trip through the work queue
  for a write that settles nothing. Binding the client then took the second half:
  a pass can no longer append to a *related* object's log either, so a controller
  that observes a child reports it on its own timeline or not at all.
  `AdminClient.AddEvent` does not answer this: it is correct on a stopped
  beehive, and a prober is running.

  The argument that took the client away does not reach `AddEvent`: an event
  bumps no `resource_version` and is invisible to the handshake, so an out-of-band
  event breaks no settled checkpoint. It went because it arrived on the same
  client, and carving one method out would restore the which-half-works table that
  scoping the client exists to avoid.

  The fix is `Client.AddEvent`, id-keyed and kind-scoped, sitting beside the three
  event reads already there. It closes both halves at once. What it needs first is a
  decision, not code: `Client` writing something only a controller may write
  crosses a line this package has held, and "only controllers write events"
  (`README.md`) would have to become a sentence about passes instead. What would
  make it worth doing: an application whose observations genuinely arrive between
  passes — a prober, a subscription — rather than during one.

- **Only a dependent's own pass can declare or drop its `depends_on` edges** —
  the other half of binding the client to its pass object. `Edges().Add` with
  `RelationDependsOn` has one non-test caller, and `Client` has no
  `AddDependency`, so a kind with no controller can never be the *source* of an
  edge (it is still fine as a target: the waker scans the whole write log). The
  drop binds the same way, so an edge left in a store by an earlier build, whose
  source is a client-only kind, cannot be removed through the package at all — it
  pins its target against collection through the RESTRICT — until an operator
  drops it with `AdminClient`, which is the answer for a wedged store but not for
  an application. What would make this worth closing at the application level: a
  caller that declares dependencies for objects it does not reconcile, on a
  running beehive. The fix is the same shape as the `AddEvent` one — id-keyed
  edge verbs on `Client` — and runs into the same question about what `Client`
  may write.

- **`HasIncomingEdges` has no `Client` counterpart.** The other four edge reads
  moved to the pass's own object with an id-keyed twin already on `Client`; this
  one did not, so "does anything with a live claim still point at this object?"
  is answerable only from inside that object's pass. It folds owned children in
  with dependents, so it cannot be rebuilt from `ListDependents`. What would make
  it worth adding: a caller outside a pass that has to make the GC-blocking
  question itself, rather than gating a finalizer on it.

- **The store-side tests still spell pre-accessor method names in prose and test
  names** — `sqlite/store_test.go` and `internal/storeapi/storeapi_test.go` carry
  `EventsAdd`, `EventsList`, `ConditionsSet`, `EdgesHasIncoming` in comments, and
  test names like `TestEventsListSince*`, `TestConditionsSetLoadError` and
  `TestDependentsListStaleSince*`. Drift from the accessor refactor, left out of
  the verb-first rename so that diff stayed one mechanical change. Nothing reads
  wrong, but the names now point at methods that exist on neither surface.

- **The read pool sets two pragmas, and the tuning ones are not among them.**
  `OpenReadPool`'s DSN carries `busy_timeout` and `query_only`; neither pool sets
  `cache_size` or `temp_store`. Each connection keeps its own page cache, about
  2MB by default, so N readers doing repeated indexed lookups over the same pages
  each re-fetch what a larger cache would hold — the shape every driver loop has.
  `mmap_size` is the other usual candidate, and it needs checking rather than
  assuming: `modernc` is a pure-Go translation, and whether its VFS implements
  memory-mapped I/O at all is not something to take on faith from the C docs.

  What would make it worth doing: a number. This is one line of DSN against the
  whole of [preparing every constant statement](adr/2026-08-21-prepare-every-constant-statement.md),
  which took 66% off a bare read — so it is worth pricing on that work's
  benchmarks, which measure the same paths. Deferred on measurement, not on doubt.

- **Nothing ever runs `PRAGMA optimize`.** SQLite recommends it for long-lived
  connections: it runs `ANALYZE` where the statistics have drifted, which is what
  keeps the planner choosing the indexes this store's queries are shaped around.
  `TestEdgeListsInheritTheIndexOrder` and `TestEventsIndexesKeepSortsOutOfPlans`
  assert the plan is right on a fresh database; neither says the planner still has
  the statistics to find it after a million writes.

  It is a write — `sqlite_stat1` — so it belongs on the writer, and the GC sweeper
  already has the cadence and the per-sweep budget to hang it on. What would make
  it worth doing: a plan that measurably degrades on a large, long-lived store.
  Nobody has run one that long, which is the actual gap here.
