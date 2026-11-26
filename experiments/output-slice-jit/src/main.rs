//! Standalone experimental demand-rooted Quiescence graph -> Cranelift compiler.
//! Normal execution remains in quiescence-runtime. Native JIT code is a derived
//! evaluator; the existing scheduler retains input tracking and event semantics.
use cranelift_codegen::ir::{
    AbiParam, InstBuilder, MemFlagsData, Value, condcodes::FloatCC, types,
};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module, default_libcall_names};
use quiescence_core::{NodeId, Quantization};
use quiescence_runtime::{EvalContext, EvalError, EvalOutput, Graph, RuntimeConfig};
use std::{
    error::Error,
    hint::black_box,
    time::{Duration, Instant},
};

const INPUTS: usize = 5;
const SAMPLES: usize = 201;
const ROUNDS: usize = 5;
const DEFAULTS: [f64; INPUTS] = [0.0, 100.0, 40.0, 10.0, 1.0];

#[derive(Clone, Copy)]
enum Op {
    Input(usize),
    Const(f64),
    Add(usize, usize),
    Gt(usize, usize),
    Lt(usize, usize),
    Or(usize, usize),
}
impl Op {
    fn deps(self) -> Option<(usize, usize)> {
        match self {
            Self::Add(a, b) | Self::Gt(a, b) | Self::Lt(a, b) | Self::Or(a, b) => Some((a, b)),
            _ => None,
        }
    }
}
struct Plan {
    ops: Vec<Op>,
    root: usize,
}
impl Plan {
    fn push(&mut self, op: Op) -> usize {
        let i = self.ops.len();
        self.ops.push(op);
        i
    }
    fn build(depth: usize, dead: usize) -> Self {
        let mut p = Self {
            ops: (0..INPUTS).map(Op::Input).collect(),
            root: 0,
        };
        let one = p.push(Op::Const(1.0));
        let mut tail = 0;
        for _ in 0..depth {
            tail = p.push(Op::Add(tail, one));
        }
        let threshold = p.push(Op::Const(100.0 + depth as f64));
        let hot = p.push(Op::Gt(tail, threshold));
        let low = p.push(Op::Lt(2, 3));
        let fuel_missing = p.push(Op::Lt(4, one));
        let alarm = p.push(Op::Or(hot, low));
        p.root = p.push(Op::Or(alarm, fuel_missing));
        let mut unused = 0;
        for _ in 0..dead {
            unused = p.push(Op::Add(unused, one));
        }
        p
    }
    fn fanout(width: usize) -> Self {
        let mut p = Self {
            ops: (0..INPUTS).map(Op::Input).collect(),
            root: 0,
        };
        let one = p.push(Op::Const(1.0));
        let mut sum = 0;
        let mut total_offsets = 0.0;
        for idx in 0..width {
            let offset = ((idx % 19) + 1) as f64;
            total_offsets += offset;
            let constant = p.push(Op::Const(offset));
            let term = p.push(Op::Add(0, constant));
            sum = p.push(Op::Add(sum, term));
        }
        let threshold = p.push(Op::Const((width + 1) as f64 * 100.0 + total_offsets));
        let hot = p.push(Op::Gt(sum, threshold));
        let low_pressure = p.push(Op::Lt(2, 3));
        let no_fuel = p.push(Op::Lt(4, one));
        let alarm = p.push(Op::Or(hot, low_pressure));
        p.root = p.push(Op::Or(alarm, no_fuel));
        p
    }

    fn live(&self) -> Vec<bool> {
        let mut live = vec![false; self.ops.len()];
        let mut stack = vec![self.root];
        while let Some(i) = stack.pop() {
            if std::mem::replace(&mut live[i], true) {
                continue;
            }
            if let Some((a, b)) = self.ops[i].deps() {
                stack.push(a);
                stack.push(b);
            }
        }
        live
    }
}

struct Compiled {
    _module: JITModule, // owns executable memory
    entry: extern "C" fn(*const f64) -> f64,
    live_ops: usize,
    used_inputs: Vec<usize>,
}
impl Compiled {
    fn new(p: &Plan) -> Result<Self, Box<dyn Error>> {
        let live = p.live();
        let used_inputs: Vec<_> = p
            .ops
            .iter()
            .enumerate()
            .filter_map(|(i, op)| {
                if live[i] {
                    if let Op::Input(x) = op {
                        Some(*x)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        let mut module = JITModule::new(JITBuilder::new(default_libcall_names())?);
        let mut ctx = module.make_context();
        let mut sig = module.make_signature();
        sig.params
            .push(AbiParam::new(module.target_config().pointer_type()));
        sig.returns.push(AbiParam::new(types::F64));
        let func = module.declare_function("output", Linkage::Local, &sig)?;
        ctx.func.signature = sig;
        let mut fbctx = FunctionBuilderContext::new();
        {
            let mut b = FunctionBuilder::new(&mut ctx.func, &mut fbctx);
            let block = b.create_block();
            b.append_block_params_for_function_params(block);
            b.switch_to_block(block);
            let ptr = b.block_params(block)[0];
            let mut values: Vec<Option<Value>> = vec![None; p.ops.len()];
            for (i, op) in p.ops.iter().enumerate() {
                if !live[i] {
                    continue;
                }
                let get = |idx: usize| values[idx].ok_or("operand has not been compiled");
                let v = match *op {
                    Op::Input(x) => b.ins().load(
                        types::F64,
                        MemFlagsData::new(),
                        ptr,
                        i32::try_from(x * size_of::<f64>())?,
                    ),
                    Op::Const(x) => b.ins().f64const(x),
                    Op::Add(a, z) => {
                        let x = get(a)?;
                        let y = get(z)?;
                        b.ins().fadd(x, y)
                    }
                    Op::Gt(a, z) | Op::Lt(a, z) => {
                        let x = get(a)?;
                        let y = get(z)?;
                        let cond = if matches!(op, Op::Gt(..)) {
                            FloatCC::GreaterThan
                        } else {
                            FloatCC::LessThan
                        };
                        let test = b.ins().fcmp(cond, x, y);
                        let one = b.ins().f64const(1.0);
                        let zero = b.ins().f64const(0.0);
                        b.ins().select(test, one, zero)
                    }
                    Op::Or(a, z) => {
                        let x = get(a)?;
                        let y = get(z)?;
                        let zero = b.ins().f64const(0.0);
                        let t1 = b.ins().fcmp(FloatCC::NotEqual, x, zero);
                        let t2 = b.ins().fcmp(FloatCC::NotEqual, y, zero);
                        let t = b.ins().bor(t1, t2);
                        let one = b.ins().f64const(1.0);
                        b.ins().select(t, one, zero)
                    }
                };
                values[i] = Some(v);
            }
            let output = values[p.root].ok_or("root unreachable")?;
            b.ins().return_(&[output]);
            b.seal_all_blocks();
            b.finalize(module.target_config());
        }
        module.define_function(func, &mut ctx)?;
        module.clear_context(&mut ctx);
        module.finalize_definitions()?;
        let ptr = module.get_finalized_function(func);
        // SAFETY: the function was declared with this exact ABI and the module
        // remains owned by Compiled. Every invocation passes a valid f64 array.
        let entry =
            unsafe { std::mem::transmute::<*const u8, extern "C" fn(*const f64) -> f64>(ptr) };
        Ok(Self {
            _module: module,
            entry,
            live_ops: live.iter().filter(|&&v| v).count(),
            used_inputs,
        })
    }
    fn eval(&self, inputs: &[f64; INPUTS]) -> f64 {
        (self.entry)(inputs.as_ptr())
    }
}
#[derive(Clone, Copy)]
enum Source {
    Const(f64),
    Node(NodeId),
}
impl Source {
    fn read(self, c: &mut EvalContext<'_>) -> Result<f64, EvalError> {
        match self {
            Self::Const(x) => Ok(x),
            Self::Node(n) => c.read_f64(n),
        }
    }
}
fn graph(p: &Plan) -> Result<(Graph, [NodeId; INPUTS], NodeId), Box<dyn Error>> {
    let mut g = Graph::new(RuntimeConfig {
        same_time_microstep_budget: p.ops.len() + 64,
    });
    let q = Quantization::new(0, 0)?;
    let mut sources: Vec<Source> = Vec::new();
    let mut inputs = [None; INPUTS];
    for op in &p.ops {
        let src = match *op {
            Op::Input(i) => {
                let id = g.add_input(format!("input-{i}"), q, DEFAULTS[i])?;
                inputs[i] = Some(id);
                Source::Node(id)
            }
            Op::Const(x) => Source::Const(x),
            Op::Add(a, b) | Op::Gt(a, b) | Op::Lt(a, b) | Op::Or(a, b) => {
                let a = sources[a];
                let b = sources[b];
                let kind = *op;
                let id = g.add_derived("op", q, 0.0, move |c| {
                    let x = a.read(c)?;
                    let y = b.read(c)?;
                    let out = match kind {
                        Op::Add(..) => x + y,
                        Op::Gt(..) => f64::from(u8::from(x > y)),
                        Op::Lt(..) => f64::from(u8::from(x < y)),
                        Op::Or(..) => f64::from(u8::from(x != 0.0 || y != 0.0)),
                        _ => unreachable!(),
                    };
                    Ok(EvalOutput::immediate(out))
                })?;
                Source::Node(id)
            }
        };
        sources.push(src);
    }
    g.settle()?;
    let root = match sources[p.root] {
        Source::Node(id) => id,
        _ => return Err("constant root".into()),
    };
    Ok((g, inputs.map(|id| id.expect("input registered")), root))
}
fn jit_graph(j: &Compiled) -> Result<(Graph, [NodeId; INPUTS], NodeId), Box<dyn Error>> {
    let mut g = Graph::new(RuntimeConfig::default());
    let q = Quantization::new(0, 0)?;
    let mut inputs = [None; INPUTS];
    for (i, slot) in inputs.iter_mut().enumerate() {
        *slot = Some(g.add_input(format!("input-{i}"), q, DEFAULTS[i])?);
    }
    let inputs = inputs.map(|id| id.expect("input registered"));
    let used = j.used_inputs.clone();
    let pointer = j.entry as usize;
    let root = g.add_derived("jit-output", q, 0.0, move |c| {
        let mut values = [0.0; INPUTS];
        for &i in &used {
            values[i] = c.read_f64(inputs[i])?;
        }
        // SAFETY: the Compiled JIT module outlives this graph; pointer points
        // to an externally constructed (f64 pointer)->f64 native function.
        let entry: extern "C" fn(*const f64) -> f64 = unsafe { std::mem::transmute(pointer) };
        Ok(EvalOutput::immediate(entry(values.as_ptr())))
    })?;
    g.settle()?;
    Ok((g, inputs, root))
}
fn values(step: usize) -> [f64; INPUTS] {
    [
        (step % 230) as f64,
        100.0,
        if step.is_multiple_of(17) { 2.0 } else { 40.0 },
        10.0,
        if step.is_multiple_of(23) { 0.0 } else { 1.0 },
    ]
}
fn verify(
    a: &mut Graph,
    ai: &[NodeId; INPUTS],
    ao: NodeId,
    b: &mut Graph,
    bi: &[NodeId; INPUTS],
    bo: NodeId,
    j: &Compiled,
) -> Result<(), Box<dyn Error>> {
    for step in 0..=250 {
        let inputs = values(step);
        for (idx, &value) in inputs.iter().enumerate() {
            a.set_input_now(ai[idx], value)?;
            b.set_input_now(bi[idx], value)?;
        }
        let x = a.value_f64(ao)?;
        let y = b.value_f64(bo)?;
        let z = j.eval(&inputs);
        if x != y || x != z {
            return Err(format!("sample {step}: {x} != {y} != {z}").into());
        }
        black_box(a.take_transitions());
        black_box(b.take_transitions());
    }
    Ok(())
}
fn median(mut samples: [Duration; ROUNDS]) -> Duration {
    samples.sort_unstable();
    samples[ROUNDS / 2]
}
fn time_graph(
    g: &mut Graph,
    inputs: &[NodeId; INPUTS],
    root: NodeId,
) -> Result<(Duration, u64), Box<dyn Error>> {
    let mut results = [Duration::ZERO; ROUNDS];
    let mut checksum = 0;
    for result in &mut results {
        let start = Instant::now();
        for i in 1..=SAMPLES {
            g.set_input_now(inputs[0], black_box((i % 230) as f64))?;
            checksum += u64::from(g.value_f64(root)? != 0.0);
            black_box(g.take_transitions());
        }
        *result = start.elapsed();
    }
    Ok((median(results), checksum))
}
fn time_jit(j: &Compiled) -> (Duration, u64) {
    let mut results = [Duration::ZERO; ROUNDS];
    let mut checksum = 0;
    let mut inputs = DEFAULTS;
    for result in &mut results {
        let start = Instant::now();
        for i in 1..=SAMPLES {
            inputs[0] = black_box((i % 230) as f64);
            checksum += u64::from(black_box(j.eval(black_box(&inputs))) != 0.0);
        }
        *result = start.elapsed();
    }
    (median(results), checksum)
}
fn measure(label: &str, p: Plan) -> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    let jit = Compiled::new(&p)?;
    let compilation = start.elapsed();
    let (mut original, oi, oo) = graph(&p)?;
    let (mut fused, fi, fo) = jit_graph(&jit)?;
    verify(&mut original, &oi, oo, &mut fused, &fi, fo, &jit)?;
    let (mut original, oi, oo) = graph(&p)?;
    let (mut fused, fi, fo) = jit_graph(&jit)?;
    let (base, sum1) = time_graph(&mut original, &oi, oo)?;
    let (inside, sum2) = time_graph(&mut fused, &fi, fo)?;
    let (direct, sum3) = time_jit(&jit);
    if sum1 != sum2 || sum1 != sum3 {
        return Err("checksum mismatch".into());
    }
    let ns = |time: Duration| time.as_secs_f64() * 1e9 / SAMPLES as f64;
    println!(
        "scenario={label} total={} live={} compile_ms={:.2} baseline_ns={:.1} jit_graph_ns={:.1} jit_direct_ns={:.1} gain={:.1}x checksum={sum1}",
        p.ops.len(),
        jit.live_ops,
        compilation.as_secs_f64() * 1e3,
        ns(base),
        ns(inside),
        ns(direct),
        ns(base) / ns(inside)
    );
    Ok(())
}
fn main() -> Result<(), Box<dyn Error>> {
    for (depth, dead) in [(16, 0), (64, 0), (256, 0), (1024, 0), (256, 1024)] {
        measure(
            &format!("chain:{depth}+dead:{dead}"),
            Plan::build(depth, dead),
        )?;
    }
    for width in [16, 64, 256] {
        measure(&format!("fanout:{width}"), Plan::fanout(width))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn demand_slicing_excludes_unused_branch() {
        let p = Plan::build(17, 200);
        let live = p.live();
        // Four live inputs, two constants, 17 adds and five gates.
        assert_eq!(live.iter().filter(|&&v| v).count(), 17 + 11);
        assert_eq!(live.len(), 17 + 12 + 200);
    }

    #[test]
    fn jit_and_fused_quiescence_match_reference() -> Result<(), Box<dyn Error>> {
        for p in [
            Plan::build(0, 0),
            Plan::build(7, 31),
            Plan::build(64, 0),
            Plan::fanout(7),
            Plan::fanout(64),
        ] {
            let jit = Compiled::new(&p)?;
            let (mut reference, ri, ro) = graph(&p)?;
            let (mut optimized, oi, oo) = jit_graph(&jit)?;
            verify(&mut reference, &ri, ro, &mut optimized, &oi, oo, &jit)?;
        }
        Ok(())
    }
}
