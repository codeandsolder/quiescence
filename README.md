# Quiescence

**Temporal incremental computation for graphs of evolving facts.**

Quiescence is an experimental Rust runtime for systems where most state is predictable, most dependencies are quiet most of the time, and repeatedly polling everything is the wrong computational model.

The core rule is simple:

> A node is an observable fact. Recompute only when causality says its result might change, and stop propagation as soon as its observable value did not change.

The runtime is intentionally domain-neutral. Industrial/factory simulation and background spacecraft/resource simulation are intended first users, not concepts in the API.

## Current prototype

- Per-node decimal quantization for numeric facts.
- Built-in hysteresis measured in least-significant decimal digits.
- Exact atom nodes for canonical interned/snapshot identities such as immutable resource-state handles.
- `f64` numeric working values behind the quantization boundary; canonical observable state is either an integer bucket or an exact atom.
- Dynamic dependency discovery during evaluation.
- Equality cutoff: unchanged canonical values do not wake dependants.
- Delayed outputs; delay can itself be read from another node.
- Future input events and generation-stamped predicted events.
- Explicit reaction-only reevaluation deadlines (`ReactionOutput::reevaluate_after`) when reaching a causal boundary matters but no state should be written yet.
- Keyed schedule-once reaction wakeups for callback semantics that must survive dependency changes. Duplicate `(reaction, key)` requests preserve the first pending deadline; same-time callbacks order by lower numeric priority, then insertion order.
- Unkeyed external reaction events for dense or stochastic source adapters. They preserve duplicate callbacks, survive dependency changes, and use the same causal time/priority ordering without forcing source RNG state into the graph.
- Cyclic graphs as a normal case.
- Bounded same-time stabilization with explicit `CalculationInstability` instead of an infinite loop.
- Validity timers for nodes that know when they next need reconsideration.

The numerical layer is deliberately replaceable. Point `f64` evaluation is merely the first implementation; bounded/refinable estimates can be added without changing graph semantics. Exact atoms are deliberately much simpler: an atom is already canonical graph-observable identity and compares exactly.

## Numerical semantics

A numeric node declares `decimal_places` and `hysteresis_lsd`. Working arithmetic may use floating point, but floating point is not graph-observable state. Graph equality, serialization and propagation decisions use the canonical integer bucket.

A one-LSD hysteresis widens the retention region by one last digit before a node moves into a new bucket. This makes the chosen information resolution part of the model and gives the runtime a natural early-termination boundary.

## Exact atom semantics

An atom node stores an exact `u64` canonical identity. It is intended for interned values or handles into an immutable/persistent domain store: inventory snapshots, item-stack identities, block-state snapshots, topology versions, and similar facts that should participate in dependencies and transactions but should never be coerced through floating point.

The graph does not interpret the handle. Equal handles mean equal observable state; changing a handle invalidates readers exactly like changing a numeric bucket. Reactions can atomically commit mixed numeric and atom writes, so a domain transaction can publish a new canonical snapshot together with counters, deadlines, or rates without exposing an intermediate state.

## Cycles

Cycles are normal. A zero-time cycle is evaluated at one simulation instant until it becomes quiescent. A configurable microstep budget is the generic safety net; exhausting it reports calculation instability. Positive delays naturally break same-time causal cycles.

Later layers should add SCC classification, repeated-state detection, specialized monotone/fixed-point solvers and stable-region compilation without weakening the bounded generic fallback.

## Workspace

- `quiescence-core`: time, IDs, estimates and quantization.
- `quiescence-runtime`: dependency graph, event queue and stabilization.
- `examples/boiler`: an AE-fed-boiler-sized demo where reaction time is itself a node.

Run the demo:

```console
RUSTC_WRAPPER=/usr/local/bin/sccache cargo run -p quiescence-boiler-demo
```

Near-term targets are interval/refinement evaluators, SCC/repeated-state analysis, a budgeted forecast frontier, stable-region collapse, and explanation tracing (*why did this node wake?*).

The fast path is not faster arithmetic. It is **proving that no arithmetic needs to happen**.

### Sparse-cutoff smoke benchmark

A deliberately simple release-mode smoke test builds a 100,001-node chain, then compares a source change that dies at the first quantized node with one that propagates through the entire chain:

```console
RUSTC_WRAPPER=/usr/local/bin/sccache cargo run --release -p quiescence-runtime --example sparse_cutoff
```

This is not yet a stable benchmark suite; it exists to catch catastrophic runtime regressions while the graph representation is still changing rapidly.

## Sparse reactions

Read-tracked reactions can forecast generation-stamped, atomic state transitions. Dependency changes invalidate stale forecasts, allowing tick-shaped source logic to execute at causal boundaries instead of polling. See `quiescence-runtime/examples/vanilla_furnace.rs` for a furnace-shaped deadline example and `quiescence-runtime/examples/vanilla_hopper.rs` for a recovered cooldown shape where inventory snapshot handles are not dependencies during cooldown, become dependencies only once the hopper is ready-but-blocked, and commit atomically with the next deadline after a successful transfer. Dense stochastic selectors can remain adapter-owned and inject each selected callback through `Graph::schedule_reaction_event`; duplicate selections remain distinct events. `quiescence-runtime/examples/vanilla_redstone_torch.rs` shows a third temporal shape: a lazy keyed event window records callbacks transactionally and evaluates an 8-events-in-60-ticks threshold without scheduling expiry wakeups.
