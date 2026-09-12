//! Bootstrap kernel contracts and operation-ordered central-processor references.
//!
//! `paths` is simulation-major `[simulations][trade_count]` f64 net returns.
//! Each simulation starts equity and peak at zero and emits maximum drawdown,
//! longest strictly underwater run, and the number of negative complete windows.
//! Sliding windows add the new value before subtracting the expired value.
//! Sampling and summary quantiles belong to the caller.

use crate::{Backend, Measured, count, length, product};

impl Paths<'_> {
    pub(crate) fn validate(&self) -> Result<(), String> {
        let k = "bootstrap_path_metrics";
        let simulations = count(k, "simulations", self.simulations)?;
        let trades = count(k, "trade_count", self.trade_count)?;
        count(k, "rolling_horizon", self.rolling_horizon)?;
        length(
            k,
            "paths",
            self.paths.len(),
            product(k, "paths", simulations, trades)?,
        )
    }

    fn reference(&self) -> Metrics {
        let mut result = Metrics {
            max_drawdowns: Vec::new(),
            longest_underwater: Vec::new(),
            negative_rolling: Vec::new(),
        };
        for simulation in 0..self.simulations as usize {
            let path =
                &self.paths[simulation * self.trade_count as usize..][..self.trade_count as usize];
            let (mut equity, mut peak, mut max_drawdown, mut rolling_sum) = (0.0, 0.0, 0.0, 0.0);
            let (mut underwater_run, mut max_underwater_run, mut negative_windows) = (0, 0, 0);
            for (index, &value) in path.iter().enumerate() {
                equity += value;
                if equity >= peak {
                    peak = equity;
                    underwater_run = 0;
                } else {
                    underwater_run += 1;
                    if underwater_run > max_underwater_run {
                        max_underwater_run = underwater_run;
                    }
                }
                let drawdown = peak - equity;
                if drawdown > max_drawdown {
                    max_drawdown = drawdown;
                }
                rolling_sum += value;
                if index >= self.rolling_horizon as usize {
                    rolling_sum -= path[index - self.rolling_horizon as usize];
                }
                if index + 1 >= self.rolling_horizon as usize && rolling_sum < 0.0 {
                    negative_windows += 1;
                }
            }
            result.max_drawdowns.push(max_drawdown);
            result.longest_underwater.push(max_underwater_run);
            result.negative_rolling.push(negative_windows);
        }
        result
    }
}

/// Raw output buffers of `bootstrap_path_metrics`, in kernel ABI order.
#[derive(Debug, Clone, PartialEq)]
pub struct Metrics {
    /// The `max_drawdowns` output, one element per launched item.
    pub max_drawdowns: Vec<f64>,
    /// The `longest_underwater` output, one element per launched item.
    pub longest_underwater: Vec<i64>,
    /// The `negative_rolling` output, one element per launched item.
    pub negative_rolling: Vec<i64>,
}

#[derive(Clone, Copy)]
pub(crate) struct Paths<'a> {
    pub paths: &'a [f64],
    pub simulations: i32,
    pub trade_count: i32,
    pub rolling_horizon: i32,
}

/// Executes `bootstrap_path_metrics` with the original typed buffers and scalars.
/// Output pointer arguments become owned buffers; shapes are checked before dispatch.
pub fn bootstrap_path_metrics(
    backend: &Backend,
    paths: &[f64],
    simulations: i32,
    trade_count: i32,
    rolling_horizon: i32,
) -> Result<Measured<Metrics>, String> {
    let input = Paths {
        paths,
        simulations,
        trade_count,
        rolling_horizon,
    };
    input.validate()?;
    match backend {
        Backend::Cpu => Ok(crate::cpu(|| input.reference())),
        #[cfg(feature = "cuda")]
        Backend::Cuda(device) => device.bootstrap_path_metrics(input),
    }
}
