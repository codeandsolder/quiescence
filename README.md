# Quiescence

**Temporal incremental computation for graphs of evolving facts.**

Quiescence is an experimental Rust runtime for systems where most state is predictable, most dependencies are quiet most of the time, and repeatedly polling everything is the wrong computational model.

The core rule is simple:

> A node is an observable fact. Recompute only when causality says its result might change, and stop propagation as soon as its observable value did not change.

The runtime is intentionally domain-neutral. Industrial/factory simulation and background spacecraft/resource simulation are intended first users, not concepts in the API.

## Current prototype

- Per-node decimal quantization.
- Built-in hysteresis measured in least-significant decimal digits.
- `f64` working values behind the quantization boundary; canonical observable state is an integer bucket.
- Dynamic dependency discovery during evaluation.
- Equality cutoff: unchanged buckets do not wake dependants.
- Delayed outputs; delay can itself be read from another node.
- Future input events and generation-stamped predicted events.
- Cyclic graphs as a normal case.
- Bounded same-time stabilization with explicit `CalculationInstability` instead of an infinite loop.
- Validity timers for nodes that know when they next need reconsideration.

The numerical layer is deliberately replaceable. Point `f64` evaluation is merely the first implementation; bounded/refinable estimates can be added without changing graph semantics.

## Numerical semantics

A numeric node declares `decimal_places` and `hysteresis_lsd`. Working arithmetic may use floating point, but floating point is not graph-observable state. Graph equality, serialization and propagation decisions use the canonical integer bucket.

A one-LSD hysteresis widens the retention region by one last digit before a node moves into a new bucket. This makes the chosen information resolution part of the model and gives the runtime a natural early-termination boundary.

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
