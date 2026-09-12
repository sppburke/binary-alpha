//! Portfolio kernel contracts and operation-ordered central-processor references.
//!
//! Capacity masks are `[portfolio][candidate]`; accepted flags are
//! `[portfolio][event]`. Events remain in caller-supplied chronology. Eligible,
//! selected events compact positions with `due > entry`, then enforce total and
//! same-expiry limits using exactly 64 local slots. Skipped events do not compact.
//!
//! Path returns are observation-major `[observation][portfolio]` f32 values.
//! Each is converted to f64 before equity accumulation; drawdown starts from a
//! zero peak, and ulcer index is the square root of mean squared drawdown.
//! Return aggregation and portfolio selection belong to the caller.

use crate::{Backend, Measured, count, length, product};

impl Capacity<'_> {
    pub(crate) fn validate(&self) -> Result<(), String> {
        let k = "replay_capacity";
        let portfolios = count(k, "portfolio_count", self.portfolio_count)?;
        let candidates = count(k, "candidate_count", self.candidate_count)?;
        let events = count(k, "event_count", self.event_count)?;
        if !(1..=64).contains(&self.max_total) {
            return Err(format!("{k}: max_total must be in 1..=64"));
        }
        if !(1..=self.max_total).contains(&self.max_expiry) {
            return Err(format!("{k}: max_expiry must be in 1..=max_total"));
        }
        length(
            k,
            "masks",
            self.masks.len(),
            product(k, "masks", portfolios, candidates)?,
        )?;
        for (name, len) in [
            ("candidate_index", self.candidate_index.len()),
            ("entry_time", self.entry_time.len()),
            ("due_time", self.due_time.len()),
            ("expiry_seconds", self.expiry_seconds.len()),
            ("hypothetical_valid", self.hypothetical_valid.len()),
            ("standalone_admitted", self.standalone_admitted.len()),
        ] {
            length(k, name, len, events)?;
        }
        for &index in self.candidate_index {
            if index < 0 || index as usize >= candidates {
                return Err(format!(
                    "{k}: candidate_index {index} outside candidate_count"
                ));
            }
        }
        product(k, "accepted", portfolios, events)?;
        Ok(())
    }

    fn reference(&self) -> Vec<u8> {
        let mut accepted = vec![0; self.portfolio_count as usize * self.event_count as usize];
        for portfolio in 0..self.portfolio_count as usize {
            let mut open_due = [0_i64; 64];
            let mut open_expiry = [0_i32; 64];
            let mut open_count = 0;
            for event in 0..self.event_count as usize {
                if self.hypothetical_valid[event] == 0 || self.standalone_admitted[event] == 0 {
                    continue;
                }
                if self.masks[portfolio * self.candidate_count as usize
                    + self.candidate_index[event] as usize]
                    == 0
                {
                    continue;
                }
                let entry = self.entry_time[event];
                let mut retained = 0;
                for index in 0..open_count {
                    if open_due[index] > entry {
                        open_due[retained] = open_due[index];
                        open_expiry[retained] = open_expiry[index];
                        retained += 1;
                    }
                }
                open_count = retained;
                if open_count >= self.max_total as usize {
                    continue;
                }
                let expiry = self.expiry_seconds[event];
                let expiry_count = open_expiry[..open_count]
                    .iter()
                    .filter(|&&value| value == expiry)
                    .count();
                if expiry_count >= self.max_expiry as usize {
                    continue;
                }
                accepted[portfolio * self.event_count as usize + event] = 1;
                open_due[open_count] = self.due_time[event];
                open_expiry[open_count] = expiry;
                open_count += 1;
            }
        }
        accepted
    }
}

impl Returns<'_> {
    pub(crate) fn validate(&self) -> Result<(), String> {
        let k = "path_drawdown";
        let observations = count(k, "observation_count", self.observation_count)?;
        let portfolios = count(k, "portfolio_count", self.portfolio_count)?;
        length(
            k,
            "returns",
            self.returns.len(),
            product(k, "returns", observations, portfolios)?,
        )
    }

    fn reference(&self) -> Drawdown {
        let mut result = Drawdown {
            max_drawdown: Vec::new(),
            ulcer_index: Vec::new(),
        };
        for portfolio in 0..self.portfolio_count as usize {
            let (mut equity, mut peak, mut largest, mut square_sum) = (0.0, 0.0, 0.0, 0.0);
            for observation in 0..self.observation_count as usize {
                equity +=
                    self.returns[observation * self.portfolio_count as usize + portfolio] as f64;
                if equity > peak {
                    peak = equity;
                }
                let drawdown = peak - equity;
                if drawdown > largest {
                    largest = drawdown;
                }
                // `sum += drawdown * drawdown` in the kernel contracts to one fused
                // multiply-add under nvcc's default `--fmad=true`; the reference fuses too.
                square_sum = drawdown.mul_add(drawdown, square_sum);
            }
            result.max_drawdown.push(largest);
            result.ulcer_index.push(if self.observation_count > 0 {
                (square_sum / self.observation_count as f64).sqrt()
            } else {
                0.0
            });
        }
        result
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Capacity<'a> {
    pub masks: &'a [u8],
    pub candidate_index: &'a [i32],
    pub entry_time: &'a [i64],
    pub due_time: &'a [i64],
    pub expiry_seconds: &'a [i32],
    pub hypothetical_valid: &'a [u8],
    pub standalone_admitted: &'a [u8],
    pub portfolio_count: i32,
    pub candidate_count: i32,
    pub event_count: i32,
    pub max_total: i32,
    pub max_expiry: i32,
}

/// Executes `replay_capacity` with the original typed buffers and scalars.
/// Output pointer arguments become owned buffers; shapes are checked before dispatch.
pub fn replay_capacity(
    backend: &Backend,
    masks: &[u8],
    candidate_index: &[i32],
    entry_time: &[i64],
    due_time: &[i64],
    expiry_seconds: &[i32],
    hypothetical_valid: &[u8],
    standalone_admitted: &[u8],
    portfolio_count: i32,
    candidate_count: i32,
    event_count: i32,
    max_total: i32,
    max_expiry: i32,
) -> Result<Measured<Vec<u8>>, String> {
    let input = Capacity {
        masks,
        candidate_index,
        entry_time,
        due_time,
        expiry_seconds,
        hypothetical_valid,
        standalone_admitted,
        portfolio_count,
        candidate_count,
        event_count,
        max_total,
        max_expiry,
    };
    input.validate()?;
    match backend {
        Backend::Cpu => Ok(crate::cpu(|| input.reference())),
        #[cfg(feature = "cuda")]
        Backend::Cuda(device) => device.replay_capacity(input),
    }
}

/// Raw output buffers of `path_drawdown`, in kernel ABI order.
#[derive(Debug, Clone, PartialEq)]
pub struct Drawdown {
    /// The `max_drawdown` output, one element per launched item.
    pub max_drawdown: Vec<f64>,
    /// The `ulcer_index` output, one element per launched item.
    pub ulcer_index: Vec<f64>,
}

#[derive(Clone, Copy)]
pub(crate) struct Returns<'a> {
    pub returns: &'a [f32],
    pub observation_count: i32,
    pub portfolio_count: i32,
}

/// Executes `path_drawdown` with the original typed buffers and scalars.
/// Output pointer arguments become owned buffers; shapes are checked before dispatch.
pub fn path_drawdown(
    backend: &Backend,
    returns: &[f32],
    observation_count: i32,
    portfolio_count: i32,
) -> Result<Measured<Drawdown>, String> {
    let input = Returns {
        returns,
        observation_count,
        portfolio_count,
    };
    input.validate()?;
    match backend {
        Backend::Cpu => Ok(crate::cpu(|| input.reference())),
        #[cfg(feature = "cuda")]
        Backend::Cuda(device) => device.path_drawdown(input),
    }
}
