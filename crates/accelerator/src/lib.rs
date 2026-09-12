//! Offline buffer operations with deterministic central-processor references and optional
//! ahead-of-time NVIDIA CUDA execution. The engine remains the financial authority.
//!
//! Public operation arguments follow the preserved kernel ABIs; output pointers become
//! owned result buffers. No operation compiles source at runtime.

// The long signatures preserve the source buffer/scalar contracts for extraction parity.
#![allow(clippy::too_many_arguments)]

pub mod bootstrap;
#[cfg(feature = "cuda")]
pub mod cuda;
pub mod portfolio;
pub mod repair;
pub mod search;

use std::time::{Duration, Instant};

/// Kernel symbols and byte-preserved source text in module build order.
pub const KERNEL_SOURCES: [(&str, &str); 13] = [
    (
        "score_bucket_plans_cap1",
        include_str!("../kernels/score_bucket_plans_cap1.cu"),
    ),
    (
        "score_bucket_plans_cap1_dual",
        include_str!("../kernels/score_bucket_plans_cap1_dual.cu"),
    ),
    (
        "score_bucket_plans_cap1_basic",
        include_str!("../kernels/score_bucket_plans_cap1_basic.cu"),
    ),
    (
        "score_bucket_plans_cap1_basic_dual",
        include_str!("../kernels/score_bucket_plans_cap1_basic_dual.cu"),
    ),
    (
        "score_bucket_plans_cap1_sparse",
        include_str!("../kernels/score_bucket_plans_cap1_sparse.cu"),
    ),
    (
        "score_bucket_plans_cap1_basic_sparse",
        include_str!("../kernels/score_bucket_plans_cap1_basic_sparse.cu"),
    ),
    (
        "score_bucket_plans_cap1_sparse_dual",
        include_str!("../kernels/score_bucket_plans_cap1_sparse_dual.cu"),
    ),
    (
        "score_bucket_plans_cap1_basic_sparse_dual",
        include_str!("../kernels/score_bucket_plans_cap1_basic_sparse_dual.cu"),
    ),
    (
        "reconstruct_signal_masks_cap1",
        include_str!("../kernels/reconstruct_signal_masks_cap1.cu"),
    ),
    (
        "bootstrap_path_metrics",
        include_str!("../kernels/bootstrap_path_metrics.cu"),
    ),
    (
        "replay_capacity",
        include_str!("../kernels/replay_capacity.cu"),
    ),
    ("path_drawdown", include_str!("../kernels/path_drawdown.cu")),
    (
        "replay_policies",
        include_str!("../kernels/replay_policies.cu"),
    ),
];

/// The native device binary compiled at build time.
#[cfg(feature = "cuda")]
pub const MODULE_CUBIN: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/module.cubin"));
/// Compiler, flags, architecture, and source order for the embedded binary.
#[cfg(feature = "cuda")]
pub const MODULE_BUILD: &str = include_str!(concat!(env!("OUT_DIR"), "/module.json"));

/// Explicit execution selection; device failures never select the CPU implicitly.
pub enum Backend {
    /// Always available, dependency-free kernel reference.
    Cpu,
    /// An opened device with its module and default stream.
    #[cfg(feature = "cuda")]
    Cuda(cuda::Device),
}

impl Backend {
    /// Opens the requested device, or names the missing build feature.
    pub fn cuda(ordinal: usize) -> Result<Self, String> {
        #[cfg(feature = "cuda")]
        {
            cuda::Device::open(ordinal).map(Self::Cuda)
        }
        #[cfg(not(feature = "cuda"))]
        {
            Err(format!(
                "device {ordinal}: CUDA requires the `cuda` feature"
            ))
        }
    }
}

/// Synchronized operation intervals. CPU execution has zero transfer/allocation fields.
#[derive(Debug, Clone, Copy, Default)]
pub struct Timings {
    /// Upload and output allocation through stream synchronization.
    pub upload: Duration,
    /// Execution through stream synchronization.
    pub execute: Duration,
    /// Download through stream synchronization.
    pub download: Duration,
    /// Bytes in device buffers used by this operation (including resident inputs).
    pub allocated_bytes: usize,
}

/// Output buffers together with measured intervals, excluded from result identity.
#[derive(Debug)]
pub struct Measured<T> {
    /// Raw kernel outputs in their declared layout.
    pub output: T,
    /// Transfer and execution measurements.
    pub timings: Timings,
}

fn cpu<T>(run: impl FnOnce() -> T) -> Measured<T> {
    let start = Instant::now();
    let output = run();
    Measured {
        output,
        timings: Timings {
            execute: start.elapsed(),
            ..Timings::default()
        },
    }
}

fn count(kernel: &str, argument: &str, value: i32) -> Result<usize, String> {
    usize::try_from(value)
        .map_err(|_| format!("{kernel}: {argument} must be nonnegative, got {value}"))
}

fn length(kernel: &str, argument: &str, actual: usize, expected: usize) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "{kernel}: {argument} length {actual}, expected {expected}"
        ))
    }
}

fn product(kernel: &str, argument: &str, left: usize, right: usize) -> Result<usize, String> {
    left.checked_mul(right)
        .filter(|&n| n <= isize::MAX as usize)
        .ok_or_else(|| format!("{kernel}: {argument} shape overflows addressable length"))
}

#[cfg(test)]
mod tests;
