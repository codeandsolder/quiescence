# Demand-rooted Quiescence native JIT experiment

This is a standalone experiment, not a production runtime change. It takes a
typed, topologically ordered expression DAG, backwards-slices the DAG from
one demanded Boolean output, and emits a single native function via Cranelift.

The exact same graph is also registered with the existing Quiescence API, one
derived evaluator per arithmetic or comparison expression. The native kernel is
benchmarked both directly and when wrapped in one real Quiescence derived
node (which retains normal dependency tracking, canonical output cutoffs,
settlement and transition logging).

## Run

    cargo +nightly run --manifest-path experiments/output-slice-jit/Cargo.toml \
      --release --offline \
      --target-dir /home/jan/build/quiescence/output-slice-jit/target \
      --config 'build.rustc-wrapper="/usr/local/bin/sccache"'

The benchmark uses exact integer-valued floating-point additions with
zero-decimal quantization, and verifies reference/fused/direct output
equivalence across 251 different sensor configurations. It then compares
medians from five 201-update runs per graph, counting nanoseconds per input
mutation including normal graph settlement and transition drain for both
graph modes.

An optional dead branch is attached to the same input but is not reachable
from the output. Reference Quiescence still schedules its nodes on that
input change; the compiler excludes them. The direct JIT measurement excludes
scheduler work and is a kernel-only lower bound, not a comparable end-to-end
runtime.

## Limitations

- The typed IR contains fixed numeric inputs, f64 addition, comparisons and
  OR; it does not yet support arbitrary Rust code or branch-dependent reads.
- Intermediate canonical equality cutoffs happen to be equivalent because
  the benchmark calculations are exactly representable integers. In a
  general implementation, intermediate quantization, history, and delayed
  effects must be represented as optimization boundaries.
- Compilation and machine-code memory mapping use a native JIT, not sandboxed
  WASM. This prototype must not execute untrusted user code.
- The generated function's lifetime is explicitly bound to its owning
  JITModule. Two unavoidable unsafe casts are confined to the prototype;
  the production runtime remains safe Rust.
- Results demonstrate the benefit and cost of fine-grained node fusion on
  synthetic graphs, not a prediction of KSP's overall simulation speed.

## Measured results on Acer (2026-11-23)

Release build, same production Quiescence runtime, 5 rounds of 201 input
updates. The benchmark mutates the temperature input and drains all
transition records after each update in both Quiescence modes.

| DAG shape | Baseline ns/update | Fused in Quiescence ns/update | Direct JIT ns/update | Speedup |
|---|---:|---:|---:|---:|
| Chain 16 | 997 | 143 | 10 | 7.0x |
| Chain 64 | 3,231 | 169 | 38 | 19.1x |
| Chain 256 | 11,896 | 346 | 211 | 34.3x |
| Chain 1024 | 38,404 | 880 | 742 | 43.6x |
| Chain 256 + 1024 dead nodes | 46,745 | 298 | 182 | 157x |
| Fanout 16 | 2,238 | 165 | 14 | 13.5x |
| Fanout 64 | 9,132 | 221 | 45 | 41.3x |
| Fanout 256 | 33,869 | 412 | 250 | 82.1x |

The fanout rows are from a second run, after that graph shape was added.
The same second run reproduced the chain speedup range (7.7x–179x).
Absolute times fluctuate under concurrent Acer workloads; these are
indicative, not laboratory-controlled. Median native Cranelift compilation
per graph was mostly below 4 ms, with cold-start and concurrent-load outliers
up to 23 ms. This excludes the Cargo toolchain build.

Important: reference graph snapshots expose all intermediate nodes.
Discarding them is only valid under an explicit input/output contract that
marks those nodes private and unobservable. The differential tests compare
the declared output, not the full internal transition log. General semantic
equivalence needs a more expressive observable-state model.
