# Architecture boundaries

Quiescence should stay aggressively modular. The model semantics are deliberately smaller than any one implementation of them.

## Semantic invariants

These are the pieces callers should be able to rely on:

1. **Everything observable is a node.** Configuration, reaction time, thresholds, rates and controller state are not privileged metadata when they can be modeled as facts.
2. **A node has canonical observable state.** Downstream propagation is based on changes to that state, not changes to evaluator working values.
3. **Dependencies are semantic and dynamic.** If an evaluator did not read a fact, that fact is not currently a dependency.
4. **Time is causal, not a polling cadence.** Work is scheduled because an event, certificate expiry or dependency change can matter.
5. **Cycles are first-class.** Positive-delay cycles are ordinary temporal behavior. Same-time cycles must either settle under the selected solver or report instability.
6. **Future knowledge is optional.** Correct state at `now` is mandatory; forecast depth is an optimization that may degrade gracefully under a budget.
7. **External interaction occurs only at quiescent boundaries.** Other threads/domains enqueue causal input; the graph owner imports it between settled states. Nobody observes or mutates a half-stabilized graph.

Everything else is replaceable.

## Layer 1: canonical observable policy

Numeric facts use decimal quantization plus last-digit hysteresis:

```text
working value -> quantizer/hysteresis -> integer observable bucket
```

Today the working numeric value is commonly `f64`. That is not part of graph equality, serialization or dependency semantics. The runtime also supports exact atom nodes: a `u64` canonical handle participates directly in equality/dependency semantics and is intended for immutable/interned structured snapshots whose contents live in a domain store.

Future policies may use:

- `f32` when sufficient;
- `f64`;
- interval estimates with progressive refinement;
- integer/fixed-point evaluators;
- exact rational or arbitrary-precision fallback near boundaries;
- domain-specific solvers that emit a canonical bucket.

The important contract is that an evaluator either determines the correct observable bucket or explicitly reports that more precision/refinement is required.

Not every fact is numeric. Exact atom nodes store a canonical `u64` identity with ordinary exact equality. The intended use is a stable handle into an interned, immutable, or persistent domain store rather than a lossy encoding of structured state. This lets inventories, item stacks, block states, topology snapshots, and other structured resources participate in the same dependency and causal-transition machinery without making Quiescence depend on their domain types.

The canonical-state contract is therefore broader than numeric quantization: a node exposes a stable comparable value, currently either a numeric bucket or an exact atom. Future canonical representations may be added without changing causal scheduling semantics.

## Layer 2: evaluation and reactions

A derived-node evaluator reads nodes through an evaluation context and produces a candidate result plus temporal information. Reads form the current dependency set.

A reaction uses the same read-tracking context, but its output is a forecasted state transition: an optional causal delay plus an atomic batch of numeric and/or exact-atom writes to state/input nodes. Reaction dependencies are dynamic just like derived-node dependencies. If any dependency changes before the forecasted transition, the reaction is reevaluated and the old generation-stamped forecast becomes stale.

This keeps evaluation pure while making side effects explicit and inspectable. A reaction does not mutate graph state while deciding what should happen; the runtime applies its write batch only when the causal boundary is reached.

This distinction is important for recovered game logic. A furnace, machine controller, cooldown, scheduled block update, stochastic callback handler, or network state machine generally has sparse semantic boundaries even when the source implementation polls it every tick. The graph should store facts such as absolute deadlines and epochs, then derive continuously changing presentation values such as progress bars from `(state, now)` on demand. It should not manufacture periodic wakeups merely to imitate a historical tick loop.

Dynamic dependencies can also change by temporal phase. A cooldown reaction can depend only on its deadline while waiting, then begin reading inventory/topology snapshots at expiry; if it is blocked, those snapshots become the dependencies that can wake it. This is a direct way to avoid both polling and irrelevant invalidations.

Dense spatial work remains outside this abstraction when the graph adds no semantic leverage. Bulk block breaking, mesh/physics inner loops, and similar hot paths can stay specialized while their sparse boundary interactions enter the graph.

## External interaction boundary

The runtime has one mutable owner. External producers communicate through a thread-safe interaction queue, not by borrowing `Graph`. An interaction is either an atomic batch of canonical input writes or an explicit callback delivery with optional source token. Each interaction carries either an explicit causal timestamp or `QuiescentNow`, which resolves to the graph owner's time when that boundary is imported.

`Graph::is_quiescent()` is stronger than "the dirty queue is empty": no event may still be due at the current simulation instant. `Graph::quiesce()` processes all such work. `Graph::process_interactions()` quiesces before and after draining the ingress queue, which establishes a simple ownership rule for background execution:

1. only the worker mutates/advances the graph;
2. producers enqueue interactions from any thread;
3. each drain observes a finite prefix of the ingress queue;
4. same-time consequences settle before the worker publishes state;
5. readers consume an immutable quiescent publication of canonical node values and committed transitions.

When the owner has no internal causal event it chooses to advance toward, `InteractionInbox::wait_for_pending()` provides a blocking wakeup rather than requiring queue polling. `QuiescentPublication` combines the stable `QuiescentSnapshot` with transition records accumulated since the previous publication; publishing drains those records while all evaluator/runtime machinery remains worker-owned. The snapshot is not intended to capture evaluator code, causal queues or other runtime machinery.

Internal causal scheduling remains separate. Forecasts and schedule-once callbacks are outputs of graph computation and never leave/re-enter through the external ingress merely for architectural symmetry.

## Layer 3: dependency storage and propagation

The prototype uses per-node `BTreeSet`s because they make correctness easy to inspect. This is intentionally disposable.

The current exact-atom prototype stores node state as a numeric-or-atom enum. In a same-load three-run smoke comparison against `7a45dc9`, the median 100k-node full-propagation path moved from about 21.7 ms to 22.6 ms (roughly 4%); sparse cutoff latency remained dominated by noise. If exact atoms remain sparse, moving their payloads into a side arena is an obvious way to remove that common numeric-node storage/cache tax without changing semantics.

Likely later representations include packed adjacency lists, generation-stamped edge arenas, small-vector fast paths, SCC-local layouts and compiled stable regions.

The invariant is only:

> unchanged canonical output does not propagate.

## Layer 4: temporal scheduling

The prototype uses one binary heap of scheduled events. The API distinguishes two causal meanings:

- **Forecasts** are generation-stamped predictions derived from current dependencies. If a dependency changes before the forecast fires, the old event becomes stale.
- **Scheduled callbacks** are keyed schedule-once wakeups. A producer may target itself or a distinct callback reaction. Once accepted they survive dependency changes; another request with the same `(target reaction, key)` is ignored until the callback is dequeued. Same-time callbacks order by lower numeric priority and then insertion sequence. The key is intentionally domain-neutral: a Minecraft adapter can intern `(position, block)` while another domain can choose a different identity. Explicit targets matter when source code schedules a different handler, such as a neighbor-change path enqueueing a later block-update callback.
- **External causal events** are unkeyed reaction wakeups injected by a source adapter. They also survive dependency changes, but duplicates are preserved: selecting the same target twice is two callbacks. Event-only callback reactions are the preferred target when the handler must read current state only at invocation time: they are dormant during initial settle and their reads are deliberately not registered as reactive dependencies. When invoked by a keyed scheduled callback, `EvalContext::scheduled_key()` exposes the exact queued key so an adapter can revalidate or dispatch against the source identity that actually fired. Duplicate-preserving external events may instead carry an opaque per-event token through `EvalContext::external_token()`; the token is payload identity, not a deduplication key. This is the right boundary for scheduled game callbacks and dense spatial/random schedulers whose RNG state, iteration order, or selection machinery should remain outside the graph.

This distinction matters because rescheduling a prediction when its assumptions change is correct, moving an already-pending vanilla block tick merely because a neighbor changed is not, and deduplicating two independently emitted random opportunities is also not. The API should not require the current heap/set implementation.

Possible replacements include calendar queues, timing wheels, hierarchical queues, per-region queues or specialized indexes for certificate expiry.

A stale prediction may be left in a queue and discarded cheaply when its generation no longer matches.

### Lazy temporal windows

Some recovered behavior depends on bounded history rather than a future deadline. An event window stores exact `(time, key)` records and answers keyed counts at the current causal time. Recording history is part of a reaction transition, so it commits alongside ordinary state writes. Expiration is lazy: a record simply stops contributing once its age exceeds the configured duration, and old front entries are physically pruned on later records. No expiry event is created merely to maintain bookkeeping.

This deliberately is not a normal reactive dependency yet. Reading a window does not wake a reaction when wall time alone makes a record stale; the caller must already have a causal event at which the history can matter. That matches the first recovered use case, redstone-torch burnout, where the source only checks/prunes history during scheduled torch callbacks. If a future corpus case requires autonomous "wake exactly when this window count decays" semantics, that should be added as an explicit stronger contract rather than silently imposed on every history record.

## Layer 5: cycle solving

The generic fallback is bounded same-time microstepping. It exists because failure must be finite and diagnosable.

Future SCC strategies can be selected independently:

- repeated-state / finite-cycle detection;
- monotone least-fixed-point iteration;
- analytical solvers;
- constraint solvers;
- periodic-region recognition;
- rate/flow solvers;
- domain-specific solver plugins.

A solver may optimize an SCC but must preserve the same observable semantics.

## Layer 6: forecasting

Execution at `now` and speculative lookahead are separate responsibilities.

The target runtime should maintain a forecast frontier opportunistically. Running out of speculative budget should shrink the horizon, not make the real-time simulation incorrect or stall merely because it failed to precompute far enough ahead.

## Why the layers matter

The first implementation is intentionally simple enough to throw pieces away. Quiescence is the computation model, not a commitment to `f64`, `BTreeSet`, `BinaryHeap`, one SCC algorithm, or any particular DSL.
