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
- Keyed schedule-once reaction wakeups for callback semantics that must survive dependency changes. A reaction can schedule either itself or an explicit target reaction; duplicate `(target reaction, key)` requests preserve the first pending deadline, and same-time callbacks order by lower numeric priority then insertion order.
- Event-only callbacks for source handlers that should run only when explicitly scheduled. They do not run during initial settle and their state reads do not become future reactive subscriptions. Keyed schedule-once invocations expose their triggering key; duplicate-preserving external events may independently carry an opaque source token such as a dense-selector position identity.
- A thread-safe external interaction ingress. Foreground/engine code can enqueue atomic state batches or tokenized callback deliveries without owning `Graph`; the graph owner imports them only at quiescent boundaries. `QuiescentNow` resolves to the owner's current simulation time at import, while explicit timestamps remain causal future events.
- Explicit quiescent observable snapshots. A snapshot is available only when no work is dirty and no event is due at `now`, so a background graph owner can publish stable canonical node state without exposing a half-settled transition.
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

## External interactions and background ownership

`Graph` is intentionally single-owner mutable state. External systems should not share or lock the graph itself. Instead, `interaction_channel()` splits the boundary into a cloneable `InteractionSender` and an `InteractionInbox` owned beside the graph:

```text
foreground / network / dense world code
                 │
                 ▼
         InteractionSender
                 │  timestamped state batch or callback token
                 ▼
       ┌── quiescent boundary ──┐
       │     Graph owner        │
       │ process_interactions() │
       └──────────┬─────────────┘
                  ▼
          causal settle/events
                  ▼
        QuiescentSnapshot
```

A sender may live on any thread. `Graph::process_interactions()` first finishes already-due causal work, drains exactly the interactions visible at that boundary, processes every newly due same-time consequence, and returns quiescent again. Interactions enqueued concurrently after the drain remain for the next boundary. This makes foreground and background execution the same semantic path rather than two simulation modes.

State batches are validated and become one event, so dependants cannot observe a half-applied external update. Callback interactions preserve duplicates and optional source tokens. Explicit future timestamps are imported into the ordinary causal event queue; `QuiescentNow` is resolved by the graph owner and therefore does not require another thread to read or synchronize the simulation clock.

`QuiescentSnapshot` is deliberately an **observable-state projection**, not a serialized graph/runtime image. It contains canonical node values plus the current and next event times. Evaluator closures, dependency indexes, histories, interners and the future event queue stay with the graph owner. A UI/game thread can therefore read immutable snapshots while a worker continues to own and advance the simulation.

The older direct mutation/scheduling methods remain useful as low-level graph controls and tests, but domain adapters should use interaction ingress at external boundaries so moving a graph between foreground and background ownership requires no semantic rewrite.

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

Read-tracked reactions can forecast generation-stamped, atomic state transitions. Dependency changes invalidate stale forecasts, allowing tick-shaped source logic to execute at causal boundaries instead of polling. See `quiescence-runtime/examples/vanilla_furnace.rs` for a furnace-shaped deadline example and `quiescence-runtime/examples/vanilla_hopper.rs` for a recovered cooldown shape where inventory snapshot handles are not dependencies during cooldown, become dependencies only once the hopper is ready-but-blocked, and commit atomically with the next deadline after a successful transfer. Dense stochastic selectors can remain adapter-owned and enqueue each selected callback through the external interaction ingress; duplicate selections remain distinct events. `quiescence-runtime/examples/vanilla_redstone_torch.rs` shows a third temporal shape: a lazy keyed event window records callbacks transactionally and evaluates an 8-events-in-60-ticks threshold without scheduling expiry wakeups.
