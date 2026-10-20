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

Everything else is replaceable.

## Layer 1: observable numeric policy

The first implementation uses decimal quantization plus last-digit hysteresis:

```text
working value -> quantizer/hysteresis -> integer observable bucket
```

Today the working value is commonly `f64`. That is not part of graph equality, serialization or dependency semantics.

Future policies may use:

- `f32` when sufficient;
- `f64`;
- interval estimates with progressive refinement;
- integer/fixed-point evaluators;
- exact rational or arbitrary-precision fallback near boundaries;
- domain-specific solvers that emit a canonical bucket.

The important contract is that an evaluator either determines the correct observable bucket or explicitly reports that more precision/refinement is required.

## Layer 2: evaluation

An evaluator reads nodes through an evaluation context and produces a candidate result plus temporal information. Reads form the current dependency set.

Evaluation is conceptually pure with respect to graph state. Side effects belong in explicit transition/application layers rather than hidden inside an evaluator.

## Layer 3: dependency storage and propagation

The prototype uses per-node `BTreeSet`s because they make correctness easy to inspect. This is intentionally disposable.

Likely later representations include packed adjacency lists, generation-stamped edge arenas, small-vector fast paths, SCC-local layouts and compiled stable regions.

The invariant is only:

> unchanged canonical output does not propagate.

## Layer 4: temporal scheduling

The prototype uses one binary heap of scheduled events with generation stamps. The API should not require that implementation.

Possible replacements include calendar queues, timing wheels, hierarchical queues, per-region queues or specialized indexes for certificate expiry.

A stale prediction may be left in a queue and discarded cheaply when its generation no longer matches.

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
