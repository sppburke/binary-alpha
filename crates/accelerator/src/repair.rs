//! Repair kernel contracts and operation-ordered central-processor references.
//!
//! Candidate offsets delimit chronological event slices. Failure words are
//! `[event][word_count]`; policy masks are `[policy][word_count]`. Any intersecting
//! bit blocks entry. Outcome codes are win 1, loss 0, tie 2, and unresolved -1.
//! Entries use `[start_ms,end_ms)`; settlements require `0 < time < end_ms`.
//! A positive close releases capacity; a negative outcome with a positive close
//! removes its opened count. Loss is -10000 fixed-point units; wins use payout_fp.
//! Metric win/loss/tie counts follow the return's sign, including a zero payout.
//! Drawdown duration and trade-count maxima are independent, and an equal recovery
//! clears the episode without refreshing the strict-peak clock.

use crate::{Backend, Measured, count, length, product};

impl Policies<'_> {
    pub(crate) fn validate(&self) -> Result<(), String> {
        let k = "replay_policies";
        let policies = count(k, "policy_count", self.policy_count)?;
        let words = count(k, "word_count", self.word_count)?;
        let mask_words = product(k, "policy_masks/word_count", policies, words)?;
        // This kernel multiplies policy * word_count as int, unlike its i64 event offset.
        if mask_words > i32::MAX as usize {
            return Err(format!(
                "{k}: policy_masks/word_count index count exceeds i32"
            ));
        }
        let events = self.entry_ms.len();
        for (name, len) in [
            ("settlement_ms", self.settlement_ms.len()),
            ("close_ms", self.close_ms.len()),
            ("outcomes", self.outcomes.len()),
            ("valid_entry", self.valid_entry.len()),
        ] {
            length(k, name, len, events)?;
        }
        length(
            k,
            "failure_words",
            self.failure_words.len(),
            product(k, "failure_words/word_count", events, words)?,
        )?;
        length(k, "policy_masks", self.policy_masks.len(), mask_words)?;
        length(k, "policy_candidate", self.policy_candidate.len(), policies)?;
        if self.candidate_offsets.is_empty() || self.candidate_offsets.len() - 1 > i32::MAX as usize
        {
            return Err(format!(
                "{k}: candidate_offsets must contain an i32-sized candidate table and terminal offset"
            ));
        }
        let mut previous = 0;
        for (index, &offset) in self.candidate_offsets.iter().enumerate() {
            if offset < previous || offset as u64 > events as u64 {
                return Err(format!(
                    "{k}: candidate_offsets must be monotone and within entry_ms"
                ));
            }
            // Each policy's counters are int; the aggregate event table uses i64 offsets.
            if index > 0 && offset - previous > i64::from(i32::MAX) {
                return Err(format!("{k}: per-candidate event count exceeds i32"));
            }
            previous = offset;
        }
        for &candidate in self.policy_candidate {
            if candidate < 0 || candidate as usize >= self.candidate_offsets.len() - 1 {
                return Err(format!(
                    "{k}: policy_candidate {candidate} outside candidate_offsets"
                ));
            }
        }
        Ok(())
    }

    fn reference(&self) -> Replay {
        let mut result = Replay::empty();
        for policy in 0..self.policy_count as usize {
            let candidate = self.policy_candidate[policy] as usize;
            let first = self.candidate_offsets[candidate] as usize;
            let last = self.candidate_offsets[candidate + 1] as usize;
            let mut state = State::default();
            let mut open_event: Option<usize> = None;
            for event in first..last {
                let entry = self.entry_ms[event];
                if entry < self.start_ms {
                    continue;
                }
                if entry >= self.end_ms {
                    break;
                }
                if let Some(open) = open_event
                    && self.close_ms[open] > 0
                    && self.close_ms[open] <= entry
                {
                    state.settle(self, open);
                    open_event = None;
                }
                if self.valid_entry[event] == 0 {
                    continue;
                }
                let words = self.word_count as usize;
                let blocked = (0..words).any(|word| {
                    self.failure_words[event * words + word]
                        & self.policy_masks[policy * words + word]
                        != 0
                });
                if blocked || open_event.is_some() {
                    continue;
                }
                open_event = Some(event);
                if state.curve_start_ms == 0 {
                    state.curve_start_ms = entry;
                }
                state.opened += 1;
            }
            if let Some(open) = open_event {
                state.settle(self, open);
            }
            if state.dd_start_ms > 0 && state.last_settle_ms > 0 {
                state.close_drawdown(state.last_settle_ms);
            }
            result.push(state);
        }
        result
    }
}

#[derive(Default)]
struct State {
    opened: i32,
    settled: i32,
    wins: i32,
    losses: i32,
    ties: i32,
    loss_streak: i32,
    longest_loss_streak: i32,
    net: i64,
    peak: i64,
    max_dd: i64,
    gross_profit: i64,
    gross_loss: i64,
    sum_squares: i64,
    downside_squares: i64,
    dd_start_ms: i64,
    last_settle_ms: i64,
    peak_time_ms: i64,
    curve_start_ms: i64,
    dd_start_trade: i32,
    peak_trade: i32,
    longest_dd_trades: i32,
    longest_dd_ms: i64,
}

impl State {
    fn close_drawdown(&mut self, time: i64) {
        // Duration and trade count have independent maxima in this kernel.
        let duration = time.wrapping_sub(self.dd_start_ms);
        let trades = self.settled - self.dd_start_trade;
        if duration > self.longest_dd_ms {
            self.longest_dd_ms = duration;
        }
        if trades > self.longest_dd_trades {
            self.longest_dd_trades = trades;
        }
    }

    fn settle(&mut self, input: &Policies<'_>, event: usize) {
        let outcome = input.outcomes[event];
        let time = input.settlement_ms[event];
        if outcome >= 0 && time > 0 && time < input.end_ms {
            let value = if outcome == 1 {
                input.payout_fp
            } else if outcome == 0 {
                -10000
            } else {
                0
            };
            self.settled += 1;
            self.last_settle_ms = time;
            self.net = self.net.wrapping_add(value);
            let square = value.wrapping_mul(value);
            self.sum_squares = self.sum_squares.wrapping_add(square);
            if value > 0 {
                self.wins += 1;
                self.gross_profit = self.gross_profit.wrapping_add(value);
                self.loss_streak = 0;
            } else if value < 0 {
                self.losses += 1;
                self.gross_loss = self.gross_loss.wrapping_sub(value);
                self.downside_squares = self.downside_squares.wrapping_add(square);
                self.loss_streak += 1;
                if self.loss_streak > self.longest_loss_streak {
                    self.longest_loss_streak = self.loss_streak;
                }
            } else {
                self.ties += 1;
                self.loss_streak = 0;
            }
            if self.net >= self.peak {
                if self.dd_start_ms > 0 {
                    self.close_drawdown(time);
                }
                if self.net > self.peak {
                    self.peak = self.net;
                    self.peak_time_ms = time;
                    self.peak_trade = self.settled;
                }
                self.dd_start_ms = 0;
            } else {
                let drawdown = self.peak.wrapping_sub(self.net);
                if drawdown > self.max_dd {
                    self.max_dd = drawdown;
                }
                if drawdown > 0 && self.dd_start_ms == 0 {
                    self.dd_start_ms = if self.peak_time_ms > 0 {
                        self.peak_time_ms
                    } else if self.curve_start_ms > 0 {
                        self.curve_start_ms
                    } else {
                        time
                    };
                    self.dd_start_trade = self.peak_trade;
                }
            }
        } else if outcome < 0 && input.close_ms[event] > 0 && self.opened > 0 {
            self.opened -= 1;
        }
    }
}

impl Replay {
    fn empty() -> Self {
        Self {
            out_opened: Vec::new(),
            out_settled: Vec::new(),
            out_wins: Vec::new(),
            out_losses: Vec::new(),
            out_ties: Vec::new(),
            out_net_fp: Vec::new(),
            out_max_dd_fp: Vec::new(),
            out_longest_dd_ms: Vec::new(),
            out_longest_dd_trades: Vec::new(),
            out_longest_loss_streak: Vec::new(),
            out_gross_profit_fp: Vec::new(),
            out_gross_loss_fp: Vec::new(),
            out_sum_returns_fp: Vec::new(),
            out_sum_squares_fp2: Vec::new(),
            out_downside_squares_fp2: Vec::new(),
        }
    }

    fn push(&mut self, state: State) {
        self.out_opened.push(state.opened);
        self.out_settled.push(state.settled);
        self.out_wins.push(state.wins);
        self.out_losses.push(state.losses);
        self.out_ties.push(state.ties);
        self.out_net_fp.push(state.net);
        self.out_max_dd_fp.push(state.max_dd);
        self.out_longest_dd_ms.push(state.longest_dd_ms);
        self.out_longest_dd_trades.push(state.longest_dd_trades);
        self.out_longest_loss_streak.push(state.longest_loss_streak);
        self.out_gross_profit_fp.push(state.gross_profit);
        self.out_gross_loss_fp.push(state.gross_loss);
        self.out_sum_returns_fp.push(state.net);
        self.out_sum_squares_fp2.push(state.sum_squares);
        self.out_downside_squares_fp2.push(state.downside_squares);
    }
}

/// Raw output buffers of `replay_policies`, in kernel ABI order.
#[derive(Debug, Clone, PartialEq)]
pub struct Replay {
    /// The `out_opened` output, one element per launched item.
    pub out_opened: Vec<i32>,
    /// The `out_settled` output, one element per launched item.
    pub out_settled: Vec<i32>,
    /// The `out_wins` output, one element per launched item.
    pub out_wins: Vec<i32>,
    /// The `out_losses` output, one element per launched item.
    pub out_losses: Vec<i32>,
    /// The `out_ties` output, one element per launched item.
    pub out_ties: Vec<i32>,
    /// The `out_net_fp` output, one element per launched item.
    pub out_net_fp: Vec<i64>,
    /// The `out_max_dd_fp` output, one element per launched item.
    pub out_max_dd_fp: Vec<i64>,
    /// The `out_longest_dd_ms` output, one element per launched item.
    pub out_longest_dd_ms: Vec<i64>,
    /// The `out_longest_dd_trades` output, one element per launched item.
    pub out_longest_dd_trades: Vec<i32>,
    /// The `out_longest_loss_streak` output, one element per launched item.
    pub out_longest_loss_streak: Vec<i32>,
    /// The `out_gross_profit_fp` output, one element per launched item.
    pub out_gross_profit_fp: Vec<i64>,
    /// The `out_gross_loss_fp` output, one element per launched item.
    pub out_gross_loss_fp: Vec<i64>,
    /// The `out_sum_returns_fp` output, one element per launched item.
    pub out_sum_returns_fp: Vec<i64>,
    /// The `out_sum_squares_fp2` output, one element per launched item.
    pub out_sum_squares_fp2: Vec<i64>,
    /// The `out_downside_squares_fp2` output, one element per launched item.
    pub out_downside_squares_fp2: Vec<i64>,
}

#[derive(Clone, Copy)]
pub(crate) struct Policies<'a> {
    pub candidate_offsets: &'a [i64],
    pub entry_ms: &'a [i64],
    pub settlement_ms: &'a [i64],
    pub close_ms: &'a [i64],
    pub outcomes: &'a [i8],
    pub valid_entry: &'a [u8],
    pub failure_words: &'a [u64],
    pub word_count: i32,
    pub policy_candidate: &'a [i32],
    pub policy_masks: &'a [u64],
    pub policy_count: i32,
    pub start_ms: i64,
    pub end_ms: i64,
    pub payout_fp: i64,
}

/// Executes `replay_policies` with the original typed buffers and scalars.
/// Output pointer arguments become owned buffers; shapes are checked before dispatch.
pub fn replay_policies(
    backend: &Backend,
    candidate_offsets: &[i64],
    entry_ms: &[i64],
    settlement_ms: &[i64],
    close_ms: &[i64],
    outcomes: &[i8],
    valid_entry: &[u8],
    failure_words: &[u64],
    word_count: i32,
    policy_candidate: &[i32],
    policy_masks: &[u64],
    policy_count: i32,
    start_ms: i64,
    end_ms: i64,
    payout_fp: i64,
) -> Result<Measured<Replay>, String> {
    let input = Policies {
        candidate_offsets,
        entry_ms,
        settlement_ms,
        close_ms,
        outcomes,
        valid_entry,
        failure_words,
        word_count,
        policy_candidate,
        policy_masks,
        policy_count,
        start_ms,
        end_ms,
        payout_fp,
    };
    input.validate()?;
    match backend {
        Backend::Cpu => Ok(crate::cpu(|| input.reference())),
        #[cfg(feature = "cuda")]
        Backend::Cuda(device) => device.replay_policies(input),
    }
}
