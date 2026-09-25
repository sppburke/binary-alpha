//! Variable-length feature equality, chronological capacity-one scoring, and signal reconstruction.
//!
//! Feature codes are feature-major `[feature][row]`. Candidate offsets delimit
//! nonempty conjunctions of feature/code equality conditions. Split scope 2 warms capacity without
//! scoring; other nonzero scopes score. Full rows contain 21 i64 values, basic rows
//! their first eight; slot 10 stores the exact bits of the f64 squared-drawdown sum.
//! Times, producer flags, and row order are preserved, never inferred or reordered.
//!
//! Output columns: 0 total, 1 wins, 2 losses, 3 ties, 4 invalid, 5 buy signals,
//! 6 sell signals, 7 net (payout per win, -100 per loss), 8 maximum drawdown,
//! 9 longest loss streak, 10 squared drawdown f64 bits, 11 longest underwater
//! trade span, 12 trade span paired with longest drawdown duration, 13–15 worst
//! rolling 20/50/100 sums (zero before a complete window), 16 valid path count,
//! 17 squared returns, 18 downside squares, 19 longest underwater milliseconds,
//! 20 longest drawdown milliseconds. Reconstruction emits `[candidate][row]` u8
//! admission flags, including admitted entries whose outcome is invalid.

use crate::{Backend, Measured, count, length, product};
use std::ops::Range;

/// A contiguous column block; a tuple may contain one block for each condition slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnBlock {
    pub columns: Range<usize>,
}

/// Conservative peak device allocation for a tuple and its concurrent batches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnBlockPlan {
    pub blocks: Vec<ColumnBlock>,
}

/// Exact screening allocation shape for one resident tuple and one reusable batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScreenShape {
    pub rows: usize,
    pub slots: usize,
    pub batch: usize,
    pub tile_strides: Vec<usize>,
    pub largest_tile: usize,
    /// Function local memory at maximum resident threads; an initial-width hint only.
    pub local_hint_bytes: usize,
}

impl ScreenShape {
    pub fn logical_bytes(&self, columns: usize, list_entries: usize) -> Result<usize, String> {
        let columns = columns
            .checked_mul(self.slots)
            .ok_or("screen blocks: column slots overflow")?;
        let list_entries = list_entries
            .checked_mul(self.slots)
            .ok_or("screen blocks: sparse slots overflow")?;
        self.exact_bytes(columns, list_entries)
    }

    /// Actual simultaneous allocation after a block tuple has built its scoped index.
    pub fn exact_bytes(&self, columns: usize, list_entries: usize) -> Result<usize, String> {
        if self.rows > i32::MAX as usize
            || self.slots == 0
            || self.batch == 0
            || self.batch > i32::MAX as usize
            || self.largest_tile == 0
            || self.largest_tile > 8
            || self.tile_strides.is_empty()
            || columns > i32::MAX as usize
            || list_entries > i32::MAX as usize
            || self
                .batch
                .checked_mul(self.slots)
                .is_none_or(|n| n > i32::MAX as usize)
        {
            return Err("screen blocks: invalid i32 shape".into());
        }
        let rows = self.rows as u128;
        let cols = columns as u128;
        let lists = list_entries as u128;
        let batch = self.batch as u128;
        // Feature matrix, key offsets and lists, entry times, split mask, every packed tile,
        // conditions, buckets, candidate offsets, driver IDs, and compact output.
        let total = 2 * cols * rows
            + 4 * (cols + 1)
            + 4 * lists
            + rows * (8 + 1 + self.tile_strides.iter().map(|&n| n as u128).sum::<u128>())
            + batch * (self.slots as u128 * 6 + 4 + 4 + self.largest_tile as u128 * 5 * 4)
            + 4;
        usize::try_from(total).map_err(|_| "screen blocks: byte size overflows usize".into())
    }
}

/// Plan exact logical sizes. The allocator-unit estimate only narrows the initial width.
pub fn plan_screen_blocks(
    row_list_lengths: &[usize],
    shape: &ScreenShape,
    budget: usize,
    unit_hint: Option<usize>,
    max_width: usize,
) -> Result<ColumnBlockPlan, String> {
    let mut blocks = Vec::new();
    let mut start = 0;
    let mut list_len = 0usize;
    for (index, &length) in row_list_lengths.iter().enumerate() {
        if length > i32::MAX as usize {
            return Err(format!(
                "screen blocks: column {index} sparse list exceeds i32"
            ));
        }
        let next = list_len.checked_add(length);
        let columns = index + 1 - start;
        let logical = next.and_then(|n| shape.logical_bytes(columns, n).ok());
        let hinted = logical.map(|bytes| {
            unit_hint
                .filter(|&unit| unit > 0 && unit <= budget)
                .map_or(bytes, |unit| bytes.div_ceil(unit).saturating_mul(unit))
                .saturating_add(shape.local_hint_bytes)
        });
        if columns <= max_width
            && logical.is_some_and(|bytes| bytes <= budget)
            && (columns == 1 || hinted.is_some_and(|bytes| bytes <= budget))
        {
            list_len = next.expect("checked");
            continue;
        }
        if start == index {
            return Err(format!(
                "screen blocks: one-column block {index} needs {} logical bytes, free budget {budget}",
                shape.logical_bytes(1, length)?
            ));
        }
        blocks.push(ColumnBlock {
            columns: start..index,
        });
        start = index;
        list_len = length;
        let one = shape.logical_bytes(1, length)?;
        if one > budget {
            return Err(format!(
                "screen blocks: one-column block {index} needs {one} logical bytes, free budget {budget}"
            ));
        }
    }
    if start < row_list_lengths.len() {
        blocks.push(ColumnBlock {
            columns: start..row_list_lengths.len(),
        });
    }
    Ok(ColumnBlockPlan { blocks })
}

/// Greedily packs columns under a device's reported free-byte budget. `row_list_lengths` gives
/// one chronological sparse list per column; the caller may pass a smaller budget in tests.
pub fn plan_column_blocks(
    row_list_lengths: &[usize],
    row_count: usize,
    max_conditions: usize,
    batch_capacity: usize,
    workers: usize,
    split_masks: usize,
    budget_bytes: usize,
) -> Result<ColumnBlockPlan, String> {
    plan_column_blocks_with_granularity(
        row_list_lengths,
        row_count,
        max_conditions,
        batch_capacity,
        workers,
        split_masks,
        (budget_bytes, 1),
    )
}

/// Plans against the allocator pool's physical reservation unit rather than only
/// requested buffer lengths. A unit of one retains the logical-byte model.
pub fn plan_column_blocks_with_granularity(
    row_list_lengths: &[usize],
    row_count: usize,
    max_conditions: usize,
    batch_capacity: usize,
    workers: usize,
    split_masks: usize,
    budget_and_granularity: (usize, usize),
) -> Result<ColumnBlockPlan, String> {
    let (budget_bytes, granularity) = budget_and_granularity;
    if row_count > i32::MAX as usize
        || max_conditions == 0
        || batch_capacity == 0
        || workers == 0
        || split_masks == 0
        || batch_capacity > i32::MAX as usize
        || batch_capacity
            .checked_mul(max_conditions)
            .is_none_or(|n| n > i32::MAX as usize)
        || granularity == 0
    {
        return Err("column blocks: invalid row, condition, batch, worker, or split bound".into());
    }
    let rows = row_count as u128;
    let slots = max_conditions as u128;
    let unit = granularity as u128;
    // Ordered rows, three time arrays, four flags, and the resident split masks.
    let resident_row_bytes = rows * (36 + split_masks as u128);
    // Feature IDs, bucket codes, candidate offsets, sparse driver IDs, and two full 21-i64 outputs.
    let batch_bytes = batch_capacity as u128 * (slots * 6 + 4 + 4 + 2 * 21 * 8) + 4;
    let concurrent_batch_bytes = batch_bytes * workers as u128;
    let budget = budget_bytes as u128;
    let fits = |columns: usize, list_len: usize| -> bool {
        if columns > i32::MAX as usize
            || columns
                .checked_mul(max_conditions)
                .is_none_or(|n| n > i32::MAX as usize)
            || list_len > i32::MAX as usize
            || list_len
                .checked_mul(max_conditions)
                .is_none_or(|n| n > i32::MAX as usize)
        {
            return false;
        }
        let feature_bytes = slots * columns as u128 * rows * 2;
        let sparse_bytes = 4 * (slots * columns as u128 + 1 + slots * list_len as u128);
        let requested = resident_row_bytes + concurrent_batch_bytes + feature_bytes + sparse_bytes;
        requested.div_ceil(unit) * unit <= budget
    };
    let mut blocks = Vec::new();
    let mut start = 0;
    let mut list_len = 0_usize;
    for (index, &length) in row_list_lengths.iter().enumerate() {
        if length > i32::MAX as usize {
            return Err(format!(
                "column blocks: column {index} sparse row list exceeds i32"
            ));
        }
        let next_len = list_len.checked_add(length);
        if next_len.is_some_and(|n| fits(index + 1 - start, n)) {
            list_len = next_len.expect("checked");
            continue;
        }
        if start == index || !fits(1, length) {
            return Err(format!(
                "column blocks: column {index} cannot fit one-column block in device budget or i32 sparse bounds"
            ));
        }
        blocks.push(ColumnBlock {
            columns: start..index,
        });
        start = index;
        list_len = length;
    }
    if start < row_list_lengths.len() {
        blocks.push(ColumnBlock {
            columns: start..row_list_lengths.len(),
        });
    }
    Ok(ColumnBlockPlan { blocks })
}

/// Shared search-stage buffers, uploaded once by a resident CUDA workspace.
#[derive(Clone, Copy)]
pub struct SearchBuffers<'a> {
    /// Feature-major `[feature_count][row_count]` equality codes.
    pub feature_codes: &'a [i16],
    /// Number of encoded feature vectors.
    pub feature_count: i32,
    /// Number of base rows.
    pub row_count: i32,
    /// Chronological row indices for dense scoring and reconstruction.
    pub ordered_rows: &'a [i64],
    /// Actual entry times, retaining the kernel ABI's historical argument name.
    pub decision_time_ms: &'a [i64],
    /// Actual capacity release times; nonpositive values cannot open.
    pub release_time_ms: &'a [i64],
    /// Settlement timestamps used only by full path metrics.
    pub settlement_time_ms: &'a [i64],
    /// Nonzero indicates a valid settled outcome.
    pub valid: &'a [u8],
    /// Buy-side win flag.
    pub buy_win: &'a [u8],
    /// Sell-side win flag.
    pub sell_win: &'a [u8],
    /// Tie flag (takes priority over either win flag).
    pub tie: &'a [u8],
}

/// Flattened equality conditions; candidate `c` owns offsets `[c]..[c + 1]`.
#[derive(Clone, Copy)]
pub struct CandidateConditions<'a> {
    /// Encoded feature index for each condition.
    pub condition_feature: &'a [i32],
    /// Fitted-encoding equality code for each condition.
    pub condition_bucket: &'a [i16],
    /// Strictly increasing offsets, from zero to the condition count.
    pub candidate_offsets: &'a [i32],
    /// Candidate count, also the launch item count.
    pub candidate_count: i32,
}

/// Sparse chronological driver lists; every visited row rechecks every condition.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SparseIndex<'a> {
    /// Driver key per candidate, or a negative value for no driver.
    pub candidate_driver_key: &'a [i32],
    /// Monotone offsets including the terminal offset into `key_chrono_rows`.
    pub key_chrono_offsets: &'a [i32],
    /// Concatenated chronological driver row lists.
    pub key_chrono_rows: &'a [i32],
}

/// Tuple-owned sparse key table, independent of each candidate batch's driver IDs.
#[derive(Clone, Copy)]
pub struct SparseKeys<'a> {
    pub key_chrono_offsets: &'a [i32],
    pub key_chrono_rows: &'a [i32],
}

pub(crate) fn validate_tuple_outcome(
    original: SearchBuffers<'_>,
    replacement: SearchBuffers<'_>,
    split_mask: &[u8],
) -> Result<(), String> {
    if original.row_count != replacement.row_count
        || original.feature_count != replacement.feature_count
        || !std::ptr::eq(original.feature_codes, replacement.feature_codes)
        || !std::ptr::eq(original.ordered_rows, replacement.ordered_rows)
        || !std::ptr::eq(original.decision_time_ms, replacement.decision_time_ms)
    {
        return Err(
            "resident tuple: feature matrix or entry order changed between expiries".into(),
        );
    }
    let rows = original.row_count as usize;
    for (name, len) in [
        ("split_mask", split_mask.len()),
        ("decision_time_ms", replacement.decision_time_ms.len()),
        ("release_time_ms", replacement.release_time_ms.len()),
        ("settlement_time_ms", replacement.settlement_time_ms.len()),
        ("valid", replacement.valid.len()),
        ("buy_win", replacement.buy_win.len()),
        ("sell_win", replacement.sell_win.len()),
        ("tie", replacement.tie.len()),
    ] {
        length("resident tuple", name, len, rows)?;
    }
    Ok(())
}

pub(crate) fn validate_tuple(
    buffers: SearchBuffers<'_>,
    split_masks: &[&[u8]],
    keys: SparseKeys<'_>,
) -> Result<(), String> {
    let Some(&first) = split_masks.first() else {
        return Err("resident tuple: no split masks".into());
    };
    Request {
        kind: 6,
        buffers,
        split_mask: first,
        candidates: CandidateConditions {
            condition_feature: &[],
            condition_bucket: &[],
            candidate_offsets: &[0],
            candidate_count: 0,
        },
        sparse: Some(SparseIndex {
            candidate_driver_key: &[],
            key_chrono_offsets: keys.key_chrono_offsets,
            key_chrono_rows: keys.key_chrono_rows,
        }),
        expiry_ms: 0,
        direction_code: 1,
        payout_basis: 0,
    }
    .validate()?;
    for split_mask in split_masks.iter().skip(1) {
        length(
            "resident tuple",
            "split_mask",
            split_mask.len(),
            buffers.row_count as usize,
        )?;
    }
    Ok(())
}

/// CPU reference for the resident tuple contract. Shared arrays and sparse rows are checked
/// once; each batch still checks its own conditions, offsets, driver IDs, and output bounds.
pub struct CpuSparseTuple<'a> {
    buffers: SearchBuffers<'a>,
    split_masks: Vec<&'a [u8]>,
    keys: SparseKeys<'a>,
}

#[cfg(test)]
thread_local! {
    static CPU_TUPLE_CONSTRUCTIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl<'a> CpuSparseTuple<'a> {
    pub fn new(
        buffers: SearchBuffers<'a>,
        split_masks: &[&'a [u8]],
        keys: SparseKeys<'a>,
    ) -> Result<Self, String> {
        validate_tuple(buffers, split_masks, keys)?;
        #[cfg(test)]
        CPU_TUPLE_CONSTRUCTIONS.with(|count| count.set(count.get() + 1));
        Ok(Self {
            buffers,
            split_masks: split_masks.to_vec(),
            keys,
        })
    }

    #[cfg(test)]
    pub(crate) fn construction_count() -> usize {
        CPU_TUPLE_CONSTRUCTIONS.with(std::cell::Cell::get)
    }

    pub fn score_batch(
        &self,
        split: usize,
        candidates: CandidateConditions<'_>,
        driver_keys: &[i32],
        expiry_ms: i64,
        payout_basis: i64,
    ) -> Result<Measured<DualScores>, String> {
        let split_mask = *self
            .split_masks
            .get(split)
            .ok_or("resident tuple: split index outside split masks")?;
        let input = Request {
            kind: 6,
            buffers: self.buffers,
            split_mask,
            candidates,
            sparse: Some(SparseIndex {
                candidate_driver_key: driver_keys,
                key_chrono_offsets: self.keys.key_chrono_offsets,
                key_chrono_rows: self.keys.key_chrono_rows,
            }),
            expiry_ms,
            direction_code: 1,
            payout_basis,
        };
        input.validate_resident_batch()?;
        Ok(crate::cpu(|| input.reference()))
    }

    /// Basic sparse dual reference for schema-2 screening, with eight values per direction.
    pub fn score_screen_batch(
        &self,
        split: usize,
        candidates: CandidateConditions<'_>,
        driver_keys: &[i32],
        expiry_ms: i64,
    ) -> Result<Measured<DualScores>, String> {
        let split_mask = *self
            .split_masks
            .get(split)
            .ok_or("resident tuple: split index outside split masks")?;
        let input = Request {
            kind: 7,
            buffers: self.buffers,
            split_mask,
            candidates,
            sparse: Some(SparseIndex {
                candidate_driver_key: driver_keys,
                key_chrono_offsets: self.keys.key_chrono_offsets,
                key_chrono_rows: self.keys.key_chrono_rows,
            }),
            expiry_ms,
            direction_code: 1,
            payout_basis: 0,
        };
        input.validate_resident_batch()?;
        Ok(crate::cpu(|| input.reference()))
    }

    /// Keep the validated feature matrix and sparse index while changing expiry outcomes.
    pub fn set_outcome(
        &mut self,
        buffers: SearchBuffers<'a>,
        split_mask: &'a [u8],
    ) -> Result<(), String> {
        validate_tuple_outcome(self.buffers, buffers, split_mask)?;
        self.buffers = buffers;
        self.split_masks[0] = split_mask;
        Ok(())
    }
}

/// Two direction-specific output buffers, each `[candidate][21 or 8]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DualScores {
    /// Buy results, first in the dual kernel ABI.
    pub buy_output: Vec<i64>,
    /// Sell results, second in the dual kernel ABI.
    pub sell_output: Vec<i64>,
}

#[derive(Clone, Copy)]
pub(crate) struct Request<'a> {
    pub kind: usize,
    pub buffers: SearchBuffers<'a>,
    pub split_mask: &'a [u8],
    pub candidates: CandidateConditions<'a>,
    pub sparse: Option<SparseIndex<'a>>,
    pub expiry_ms: i64,
    pub direction_code: i32,
    pub payout_basis: i64,
}

impl Request<'_> {
    pub fn kernel(&self) -> &'static str {
        crate::KERNEL_SOURCES[self.kind].0
    }
    pub fn full(&self) -> bool {
        matches!(self.kind, 0 | 1 | 4 | 6)
    }
    pub fn dual(&self) -> bool {
        matches!(self.kind, 1 | 3 | 6 | 7)
    }
    pub fn width(&self) -> usize {
        if self.full() { 21 } else { 8 }
    }

    pub fn validate(&self) -> Result<(), String> {
        self.validate_mode(false)
    }

    /// Tuple-owned matrix, row buffers, and sparse lists were validated at workspace creation.
    pub(crate) fn validate_resident_batch(&self) -> Result<(), String> {
        self.validate_mode(true)
    }

    fn validate_mode(&self, resident: bool) -> Result<(), String> {
        let k = self.kernel();
        let b = self.buffers;
        let rows = count(k, "row_count", b.row_count)?;
        let features = count(k, "feature_count", b.feature_count)?;
        let candidates = count(k, "candidate_count", self.candidates.candidate_count)?;
        if !resident {
            length(
                k,
                "feature_codes",
                b.feature_codes.len(),
                product(k, "feature_codes", features, rows)?,
            )?;
        }
        if !resident {
            length(k, "split_mask", self.split_mask.len(), rows)?;
        }
        if !resident {
            for (name, len) in [
                ("decision_time_ms", b.decision_time_ms.len()),
                ("release_time_ms", b.release_time_ms.len()),
            ] {
                length(k, name, len, rows)?;
            }
            if self.full() {
                length(k, "settlement_time_ms", b.settlement_time_ms.len(), rows)?;
            }
            if self.kind != 8 {
                for (name, len) in [
                    ("valid", b.valid.len()),
                    ("buy_win", b.buy_win.len()),
                    ("sell_win", b.sell_win.len()),
                    ("tie", b.tie.len()),
                ] {
                    length(k, name, len, rows)?;
                }
            }
        }
        let c = self.candidates;
        let conditions = c.condition_feature.len();
        if conditions > i32::MAX as usize {
            return Err(format!("{k}: condition count exceeds i32"));
        }
        length(k, "condition_bucket", c.condition_bucket.len(), conditions)?;
        length(
            k,
            "candidate_offsets",
            c.candidate_offsets.len(),
            candidates + 1,
        )?;
        if c.candidate_offsets[0] != 0
            || c.candidate_offsets[candidates] as i64 != conditions as i64
            || c.candidate_offsets.windows(2).any(|w| w[0] >= w[1])
        {
            return Err(format!(
                "{k}: candidate_offsets must increase from zero to the condition count with at least one condition per candidate"
            ));
        }
        for &feature in c.condition_feature {
            if feature < 0 || feature as usize >= features {
                return Err(format!(
                    "{k}: condition_feature index {feature} outside feature_count {features}"
                ));
            }
        }
        if let Some(sparse) = self.sparse {
            if !resident && b.ordered_rows.len() > rows {
                return Err(format!("{k}: ordered_rows length exceeds row_count"));
            }
            length(
                k,
                "candidate_driver_key",
                sparse.candidate_driver_key.len(),
                candidates,
            )?;
            if sparse.key_chrono_offsets.is_empty() {
                return Err(format!(
                    "{k}: key_chrono_offsets requires a terminal offset"
                ));
            }
            if sparse.key_chrono_rows.len() > i32::MAX as usize
                || sparse.key_chrono_offsets.len() - 1 > i32::MAX as usize
            {
                return Err(format!(
                    "{k}: key_chrono_rows/key_chrono_offsets count exceeds i32"
                ));
            }
            if !resident {
                let mut previous = 0;
                for &offset in sparse.key_chrono_offsets {
                    if offset < previous || offset as usize > sparse.key_chrono_rows.len() {
                        return Err(format!(
                            "{k}: key_chrono_offsets must be monotone and within key_chrono_rows"
                        ));
                    }
                    previous = offset;
                }
                for &row in sparse.key_chrono_rows {
                    if row < 0 || row as usize >= rows {
                        return Err(format!(
                            "{k}: key_chrono_rows index {row} outside row_count"
                        ));
                    }
                }
            }
            for &key in sparse.candidate_driver_key {
                if key >= 0 && key as usize >= sparse.key_chrono_offsets.len() - 1 {
                    return Err(format!(
                        "{k}: candidate_driver_key {key} outside key_chrono_offsets"
                    ));
                }
            }
        } else {
            length(k, "ordered_rows", b.ordered_rows.len(), rows)?;
            for &row in b.ordered_rows {
                if row < 0 || row as u64 >= rows as u64 {
                    return Err(format!("{k}: ordered_rows index {row} outside row_count"));
                }
            }
        }
        product(
            k,
            "output",
            candidates,
            if self.kind == 8 { rows } else { self.width() },
        )?;
        Ok(())
    }

    fn matches(&self, candidate: usize, row: usize) -> bool {
        let c = self.candidates;
        (c.candidate_offsets[candidate]..c.candidate_offsets[candidate + 1]).all(|condition| {
            let feature = c.condition_feature[condition as usize];
            self.buffers.feature_codes[feature as usize * self.buffers.row_count as usize + row]
                == c.condition_bucket[condition as usize]
        })
    }

    fn rows(&self, candidate: usize, mut visit: impl FnMut(usize)) {
        if let Some(sparse) = self.sparse {
            let key = sparse.candidate_driver_key[candidate];
            if key < 0 {
                return;
            }
            let first = sparse.key_chrono_offsets[key as usize] as usize;
            let last = sparse.key_chrono_offsets[key as usize + 1] as usize;
            for &row in &sparse.key_chrono_rows[first..last] {
                visit(row as usize);
            }
        } else {
            for &row in self.buffers.ordered_rows {
                visit(row as usize);
            }
        }
    }

    fn reference(&self) -> DualScores {
        let _ = self.expiry_ms; // Unused by every preserved search kernel.
        let mut result = DualScores {
            buy_output: Vec::new(),
            sell_output: Vec::new(),
        };
        for candidate in 0..self.candidates.candidate_count as usize {
            let mut active_due = -9223372036854775807_i64;
            let (mut total, mut invalid, mut ties) = (0_i64, 0_i64, 0_i64);
            let mut buy = Path::new();
            let mut sell = Path::new();
            self.rows(candidate, |row| {
                let scope = self.split_mask[row];
                if scope == 0 || !self.matches(candidate, row) {
                    return;
                }
                let decision = self.buffers.decision_time_ms[row];
                let release = self.buffers.release_time_ms[row];
                if scope == 2 {
                    if active_due <= decision && release > 0 {
                        active_due = release;
                    }
                    return;
                }
                total += 1;
                if active_due > decision || release <= 0 {
                    invalid += 1;
                    return;
                }
                let mut buy_units = 0;
                let mut sell_units = 0;
                if self.buffers.valid[row] == 0 {
                    invalid += 1;
                } else if self.buffers.tie[row] != 0 {
                    ties += 1;
                } else if self.dual() {
                    // Dual kernels give the buy flag priority even for the sell output.
                    if self.buffers.buy_win[row] != 0 {
                        buy.wins += 1;
                        sell.losses += 1;
                        buy_units = self.payout_basis;
                        sell_units = -100;
                    } else if self.buffers.sell_win[row] != 0 {
                        sell.wins += 1;
                        buy.losses += 1;
                        sell_units = self.payout_basis;
                        buy_units = -100;
                    }
                } else {
                    let (win, loss) = if self.direction_code == 1 {
                        (self.buffers.buy_win[row], self.buffers.sell_win[row])
                    } else {
                        (self.buffers.sell_win[row], self.buffers.buy_win[row])
                    };
                    if win != 0 {
                        buy.wins += 1;
                        buy_units = self.payout_basis;
                    } else if loss != 0 {
                        buy.losses += 1;
                        buy_units = -100;
                    }
                }
                if self.full() && self.buffers.valid[row] != 0 {
                    buy.observe(buy_units, decision, self.buffers.settlement_time_ms[row]);
                    if self.dual() {
                        sell.observe(sell_units, decision, self.buffers.settlement_time_ms[row]);
                    }
                }
                active_due = release;
            });
            let direction = if self.dual() { 1 } else { self.direction_code };
            result.buy_output.extend_from_slice(
                &buy.finish(total, ties, invalid, direction, self.payout_basis)[..self.width()],
            );
            if self.dual() {
                result.sell_output.extend_from_slice(
                    &sell.finish(total, ties, invalid, -1, self.payout_basis)[..self.width()],
                );
            }
        }
        result
    }

    fn reconstruct(&self) -> Vec<u8> {
        let _ = self.expiry_ms; // Release buffers own actual admission timing.
        let mut output =
            vec![0; self.candidates.candidate_count as usize * self.buffers.row_count as usize];
        for candidate in 0..self.candidates.candidate_count as usize {
            let mut active_due = -9223372036854775807_i64;
            self.rows(candidate, |row| {
                let scope = self.split_mask[row];
                if scope == 0 || !self.matches(candidate, row) {
                    return;
                }
                let decision = self.buffers.decision_time_ms[row];
                let release = self.buffers.release_time_ms[row];
                if scope == 2 {
                    if active_due <= decision && release > 0 {
                        active_due = release;
                    }
                    return;
                }
                if active_due > decision || release <= 0 {
                    return;
                }
                output[candidate * self.buffers.row_count as usize + row] = 1;
                active_due = release;
            });
        }
        output
    }
}

struct Path {
    wins: i64,
    losses: i64,
    equity: i64,
    peak: i64,
    max_drawdown: i64,
    max_drawdown_trades: i64,
    drawdown_square_sum: f64,
    trade_square_sum: i64,
    downside_square_sum: i64,
    underwater_start_trade: i64,
    underwater_start_ms: i64,
    peak_trade_index: i64,
    peak_decision_ms: i64,
    final_settlement_ms: i64,
    valid_trade_index: i64,
    longest_underwater: i64,
    longest_underwater_ms: i64,
    current_losses: i64,
    max_losses: i64,
    max_drawdown_ms: i64,
    rolling: [i64; 100],
    rolling20: i64,
    rolling50: i64,
    rolling100: i64,
    worst20: i64,
    worst50: i64,
    worst100: i64,
}

impl Path {
    fn new() -> Self {
        Self {
            wins: 0,
            losses: 0,
            equity: 0,
            peak: 0,
            max_drawdown: 0,
            max_drawdown_trades: 0,
            drawdown_square_sum: 0.0,
            trade_square_sum: 0,
            downside_square_sum: 0,
            underwater_start_trade: -1,
            underwater_start_ms: -1,
            peak_trade_index: 0,
            peak_decision_ms: -1,
            final_settlement_ms: -1,
            valid_trade_index: 0,
            longest_underwater: 0,
            longest_underwater_ms: 0,
            current_losses: 0,
            max_losses: 0,
            max_drawdown_ms: 0,
            rolling: [0; 100],
            rolling20: 0,
            rolling50: 0,
            rolling100: 0,
            worst20: i64::MAX,
            worst50: i64::MAX,
            worst100: i64::MAX,
        }
    }

    fn duration(&mut self, settlement: i64, terminal: bool) {
        let trades = self.valid_trade_index - self.peak_trade_index;
        let duration = settlement.wrapping_sub(self.peak_decision_ms);
        if duration > self.max_drawdown_ms
            || (duration == self.max_drawdown_ms && trades > self.max_drawdown_trades)
        {
            self.max_drawdown_ms = duration;
            self.max_drawdown_trades = trades;
        }
        let underwater = self.valid_trade_index - self.underwater_start_trade + i64::from(terminal);
        let underwater_ms = settlement.wrapping_sub(self.underwater_start_ms);
        if underwater > self.longest_underwater {
            self.longest_underwater = underwater;
        }
        if underwater_ms > self.longest_underwater_ms {
            self.longest_underwater_ms = underwater_ms;
        }
    }

    fn observe(&mut self, units: i64, decision: i64, settlement: i64) {
        let ring_slot = (self.valid_trade_index % 100) as usize;
        let replaced = self.rolling[ring_slot];
        self.rolling[ring_slot] = units;
        self.rolling100 = self.rolling100.wrapping_add(units.wrapping_sub(replaced));
        self.rolling50 = self.rolling50.wrapping_add(units);
        self.rolling20 = self.rolling20.wrapping_add(units);
        if self.valid_trade_index >= 50 {
            self.rolling50 = self
                .rolling50
                .wrapping_sub(self.rolling[((self.valid_trade_index - 50) % 100) as usize]);
        }
        if self.valid_trade_index >= 20 {
            self.rolling20 = self
                .rolling20
                .wrapping_sub(self.rolling[((self.valid_trade_index - 20) % 100) as usize]);
        }
        if self.valid_trade_index >= 19 && self.rolling20 < self.worst20 {
            self.worst20 = self.rolling20;
        }
        if self.valid_trade_index >= 49 && self.rolling50 < self.worst50 {
            self.worst50 = self.rolling50;
        }
        if self.valid_trade_index >= 99 && self.rolling100 < self.worst100 {
            self.worst100 = self.rolling100;
        }
        self.valid_trade_index += 1;
        self.final_settlement_ms = settlement;
        if self.peak_decision_ms < 0 {
            self.peak_decision_ms = decision;
        }
        self.equity = self.equity.wrapping_add(units);
        self.trade_square_sum = self
            .trade_square_sum
            .wrapping_add(units.wrapping_mul(units));
        if units < 0 {
            self.downside_square_sum = self
                .downside_square_sum
                .wrapping_add(units.wrapping_mul(units));
        }
        if units < 0 {
            self.current_losses += 1;
            if self.current_losses > self.max_losses {
                self.max_losses = self.current_losses;
            }
        } else {
            self.current_losses = 0;
        }
        if self.equity >= self.peak {
            if self.underwater_start_trade >= 0 {
                self.duration(settlement, false);
            }
            if self.equity > self.peak {
                self.peak = self.equity;
            }
            self.peak_trade_index = self.valid_trade_index;
            self.peak_decision_ms = settlement;
            self.underwater_start_trade = -1;
            self.underwater_start_ms = -1;
        }
        let drawdown = self.peak.wrapping_sub(self.equity);
        // The kernel text is `sum += (double)d * (double)d`; nvcc's default contraction
        // (`--fmad=true`) emits one fused multiply-add for it, so the reference fuses too.
        self.drawdown_square_sum =
            (drawdown as f64).mul_add(drawdown as f64, self.drawdown_square_sum);
        if drawdown > self.max_drawdown {
            self.max_drawdown = drawdown;
        }
        if drawdown > 0 && self.underwater_start_trade < 0 {
            self.underwater_start_trade = self.valid_trade_index;
            self.underwater_start_ms = settlement;
        }
    }

    fn finish(
        mut self,
        total: i64,
        ties: i64,
        invalid: i64,
        direction: i32,
        payout: i64,
    ) -> [i64; 21] {
        if self.underwater_start_trade >= 0 && self.final_settlement_ms >= 0 {
            self.duration(self.final_settlement_ms, true);
        }
        [
            total,
            self.wins,
            self.losses,
            ties,
            invalid,
            if direction == 1 { total } else { 0 },
            if direction == -1 { total } else { 0 },
            self.wins
                .wrapping_mul(payout)
                .wrapping_sub(self.losses.wrapping_mul(100)),
            self.max_drawdown,
            self.max_losses,
            self.drawdown_square_sum.to_bits() as i64,
            self.longest_underwater,
            self.max_drawdown_trades,
            if self.worst20 == i64::MAX {
                0
            } else {
                self.worst20
            },
            if self.worst50 == i64::MAX {
                0
            } else {
                self.worst50
            },
            if self.worst100 == i64::MAX {
                0
            } else {
                self.worst100
            },
            self.valid_trade_index,
            self.trade_square_sum,
            self.downside_square_sum,
            self.longest_underwater_ms,
            self.max_drawdown_ms,
        ]
    }
}

fn inferred_features(
    kernel: &str,
    codes: &[i16],
    rows: i32,
    condition_feature: &[i32],
) -> Result<i32, String> {
    let rows = count(kernel, "row_count", rows)?;
    // With no rows the ABI carries no feature shape. No code is dereferenced; retain
    // the minimum shape consistent with the conditions for the empty operation.
    let features = codes.len().checked_div(rows).unwrap_or_else(|| {
        (condition_feature
            .iter()
            .copied()
            .max()
            .unwrap_or(-1)
            .max(-1) as i64
            + 1) as usize
    });
    i32::try_from(features)
        .map_err(|_| format!("{kernel}: feature_codes feature count exceeds i32"))
}

/// Executes `score_bucket_plans_cap1`; input order and dtypes match the native kernel ABI.
/// Output pointers become owned buffers. Validation runs for either backend.
pub fn score_bucket_plans_cap1(
    backend: &Backend,
    feature_codes: &[i16],
    condition_feature: &[i32],
    condition_bucket: &[i16],
    candidate_offsets: &[i32],
    split_mask: &[u8],
    ordered_rows: &[i64],
    decision_time_ms: &[i64],
    release_time_ms: &[i64],
    settlement_time_ms: &[i64],
    valid: &[u8],
    buy_win: &[u8],
    sell_win: &[u8],
    tie: &[u8],
    candidate_count: i32,
    row_count: i32,
    feature_count: i32,
    expiry_ms: i64,
    direction_code: i32,
    payout_basis: i64,
) -> Result<Measured<Vec<i64>>, String> {
    let input = Request {
        kind: 0,
        buffers: SearchBuffers {
            feature_codes,
            feature_count,
            row_count,
            ordered_rows,
            decision_time_ms,
            release_time_ms,
            settlement_time_ms,
            valid,
            buy_win,
            sell_win,
            tie,
        },
        split_mask,
        candidates: CandidateConditions {
            condition_feature,
            condition_bucket,
            candidate_offsets,
            candidate_count,
        },
        sparse: None,
        expiry_ms,
        direction_code,
        payout_basis,
    };
    input.validate()?;
    let result = match backend {
        Backend::Cpu => crate::cpu(|| input.reference()),
        #[cfg(feature = "cuda")]
        Backend::Cuda(device) => device.score(input)?,
    };
    Ok(Measured {
        output: result.output.buy_output,
        timings: result.timings,
    })
}

/// Executes `score_bucket_plans_cap1_dual`; input order and dtypes match the native kernel ABI.
/// Output pointers become owned buffers. Validation runs for either backend.
pub fn score_bucket_plans_cap1_dual(
    backend: &Backend,
    feature_codes: &[i16],
    condition_feature: &[i32],
    condition_bucket: &[i16],
    candidate_offsets: &[i32],
    split_mask: &[u8],
    ordered_rows: &[i64],
    decision_time_ms: &[i64],
    release_time_ms: &[i64],
    settlement_time_ms: &[i64],
    valid: &[u8],
    buy_win: &[u8],
    sell_win: &[u8],
    tie: &[u8],
    candidate_count: i32,
    row_count: i32,
    feature_count: i32,
    expiry_ms: i64,
    payout_basis: i64,
) -> Result<Measured<DualScores>, String> {
    let input = Request {
        kind: 1,
        buffers: SearchBuffers {
            feature_codes,
            feature_count,
            row_count,
            ordered_rows,
            decision_time_ms,
            release_time_ms,
            settlement_time_ms,
            valid,
            buy_win,
            sell_win,
            tie,
        },
        split_mask,
        candidates: CandidateConditions {
            condition_feature,
            condition_bucket,
            candidate_offsets,
            candidate_count,
        },
        sparse: None,
        expiry_ms,
        direction_code: 1,
        payout_basis,
    };
    input.validate()?;
    let result = match backend {
        Backend::Cpu => crate::cpu(|| input.reference()),
        #[cfg(feature = "cuda")]
        Backend::Cuda(device) => device.score(input)?,
    };
    Ok(result)
}

/// Executes `score_bucket_plans_cap1_basic`; input order and dtypes match the native kernel ABI.
/// Output pointers become owned buffers. Validation runs for either backend.
pub fn score_bucket_plans_cap1_basic(
    backend: &Backend,
    feature_codes: &[i16],
    condition_feature: &[i32],
    condition_bucket: &[i16],
    candidate_offsets: &[i32],
    split_mask: &[u8],
    ordered_rows: &[i64],
    decision_time_ms: &[i64],
    release_time_ms: &[i64],
    valid: &[u8],
    buy_win: &[u8],
    sell_win: &[u8],
    tie: &[u8],
    candidate_count: i32,
    row_count: i32,
    expiry_ms: i64,
    direction_code: i32,
    payout_basis: i64,
) -> Result<Measured<Vec<i64>>, String> {
    let feature_count = inferred_features(
        "score_bucket_plans_cap1_basic",
        feature_codes,
        row_count,
        condition_feature,
    )?;
    let input = Request {
        kind: 2,
        buffers: SearchBuffers {
            feature_codes,
            feature_count,
            row_count,
            ordered_rows,
            decision_time_ms,
            release_time_ms,
            settlement_time_ms: &[],
            valid,
            buy_win,
            sell_win,
            tie,
        },
        split_mask,
        candidates: CandidateConditions {
            condition_feature,
            condition_bucket,
            candidate_offsets,
            candidate_count,
        },
        sparse: None,
        expiry_ms,
        direction_code,
        payout_basis,
    };
    input.validate()?;
    let result = match backend {
        Backend::Cpu => crate::cpu(|| input.reference()),
        #[cfg(feature = "cuda")]
        Backend::Cuda(device) => device.score(input)?,
    };
    Ok(Measured {
        output: result.output.buy_output,
        timings: result.timings,
    })
}

/// Executes `score_bucket_plans_cap1_basic_dual`; input order and dtypes match the native kernel ABI.
/// Output pointers become owned buffers. Validation runs for either backend.
pub fn score_bucket_plans_cap1_basic_dual(
    backend: &Backend,
    feature_codes: &[i16],
    condition_feature: &[i32],
    condition_bucket: &[i16],
    candidate_offsets: &[i32],
    split_mask: &[u8],
    ordered_rows: &[i64],
    decision_time_ms: &[i64],
    release_time_ms: &[i64],
    valid: &[u8],
    buy_win: &[u8],
    sell_win: &[u8],
    tie: &[u8],
    candidate_count: i32,
    row_count: i32,
    expiry_ms: i64,
    payout_basis: i64,
) -> Result<Measured<DualScores>, String> {
    let feature_count = inferred_features(
        "score_bucket_plans_cap1_basic_dual",
        feature_codes,
        row_count,
        condition_feature,
    )?;
    let input = Request {
        kind: 3,
        buffers: SearchBuffers {
            feature_codes,
            feature_count,
            row_count,
            ordered_rows,
            decision_time_ms,
            release_time_ms,
            settlement_time_ms: &[],
            valid,
            buy_win,
            sell_win,
            tie,
        },
        split_mask,
        candidates: CandidateConditions {
            condition_feature,
            condition_bucket,
            candidate_offsets,
            candidate_count,
        },
        sparse: None,
        expiry_ms,
        direction_code: 1,
        payout_basis,
    };
    input.validate()?;
    let result = match backend {
        Backend::Cpu => crate::cpu(|| input.reference()),
        #[cfg(feature = "cuda")]
        Backend::Cuda(device) => device.score(input)?,
    };
    Ok(result)
}

/// Executes `score_bucket_plans_cap1_sparse`; input order and dtypes match the native kernel ABI.
/// Output pointers become owned buffers. Validation runs for either backend.
pub fn score_bucket_plans_cap1_sparse(
    backend: &Backend,
    feature_codes: &[i16],
    condition_feature: &[i32],
    condition_bucket: &[i16],
    candidate_offsets: &[i32],
    candidate_driver_key: &[i32],
    key_chrono_offsets: &[i32],
    key_chrono_rows: &[i32],
    split_mask: &[u8],
    decision_time_ms: &[i64],
    release_time_ms: &[i64],
    settlement_time_ms: &[i64],
    valid: &[u8],
    buy_win: &[u8],
    sell_win: &[u8],
    tie: &[u8],
    candidate_count: i32,
    row_count: i32,
    expiry_ms: i64,
    direction_code: i32,
    payout_basis: i64,
) -> Result<Measured<Vec<i64>>, String> {
    let feature_count = inferred_features(
        "score_bucket_plans_cap1_sparse",
        feature_codes,
        row_count,
        condition_feature,
    )?;
    let input = Request {
        kind: 4,
        buffers: SearchBuffers {
            feature_codes,
            feature_count,
            row_count,
            ordered_rows: &[],
            decision_time_ms,
            release_time_ms,
            settlement_time_ms,
            valid,
            buy_win,
            sell_win,
            tie,
        },
        split_mask,
        candidates: CandidateConditions {
            condition_feature,
            condition_bucket,
            candidate_offsets,
            candidate_count,
        },
        sparse: Some(SparseIndex {
            candidate_driver_key,
            key_chrono_offsets,
            key_chrono_rows,
        }),
        expiry_ms,
        direction_code,
        payout_basis,
    };
    input.validate()?;
    let result = match backend {
        Backend::Cpu => crate::cpu(|| input.reference()),
        #[cfg(feature = "cuda")]
        Backend::Cuda(device) => device.score(input)?,
    };
    Ok(Measured {
        output: result.output.buy_output,
        timings: result.timings,
    })
}

/// Executes `score_bucket_plans_cap1_basic_sparse`; input order and dtypes match the native kernel ABI.
/// Output pointers become owned buffers. Validation runs for either backend.
pub fn score_bucket_plans_cap1_basic_sparse(
    backend: &Backend,
    feature_codes: &[i16],
    condition_feature: &[i32],
    condition_bucket: &[i16],
    candidate_offsets: &[i32],
    candidate_driver_key: &[i32],
    key_chrono_offsets: &[i32],
    key_chrono_rows: &[i32],
    split_mask: &[u8],
    decision_time_ms: &[i64],
    release_time_ms: &[i64],
    valid: &[u8],
    buy_win: &[u8],
    sell_win: &[u8],
    tie: &[u8],
    candidate_count: i32,
    row_count: i32,
    expiry_ms: i64,
    direction_code: i32,
    payout_basis: i64,
) -> Result<Measured<Vec<i64>>, String> {
    let feature_count = inferred_features(
        "score_bucket_plans_cap1_basic_sparse",
        feature_codes,
        row_count,
        condition_feature,
    )?;
    let input = Request {
        kind: 5,
        buffers: SearchBuffers {
            feature_codes,
            feature_count,
            row_count,
            ordered_rows: &[],
            decision_time_ms,
            release_time_ms,
            settlement_time_ms: &[],
            valid,
            buy_win,
            sell_win,
            tie,
        },
        split_mask,
        candidates: CandidateConditions {
            condition_feature,
            condition_bucket,
            candidate_offsets,
            candidate_count,
        },
        sparse: Some(SparseIndex {
            candidate_driver_key,
            key_chrono_offsets,
            key_chrono_rows,
        }),
        expiry_ms,
        direction_code,
        payout_basis,
    };
    input.validate()?;
    let result = match backend {
        Backend::Cpu => crate::cpu(|| input.reference()),
        #[cfg(feature = "cuda")]
        Backend::Cuda(device) => device.score(input)?,
    };
    Ok(Measured {
        output: result.output.buy_output,
        timings: result.timings,
    })
}

/// Executes `score_bucket_plans_cap1_sparse_dual`; input order and dtypes match the native kernel ABI.
/// Output pointers become owned buffers. Validation runs for either backend.
pub fn score_bucket_plans_cap1_sparse_dual(
    backend: &Backend,
    feature_codes: &[i16],
    condition_feature: &[i32],
    condition_bucket: &[i16],
    candidate_offsets: &[i32],
    candidate_driver_key: &[i32],
    key_chrono_offsets: &[i32],
    key_chrono_rows: &[i32],
    split_mask: &[u8],
    decision_time_ms: &[i64],
    release_time_ms: &[i64],
    settlement_time_ms: &[i64],
    valid: &[u8],
    buy_win: &[u8],
    sell_win: &[u8],
    tie: &[u8],
    candidate_count: i32,
    row_count: i32,
    expiry_ms: i64,
    payout_basis: i64,
) -> Result<Measured<DualScores>, String> {
    let feature_count = inferred_features(
        "score_bucket_plans_cap1_sparse_dual",
        feature_codes,
        row_count,
        condition_feature,
    )?;
    let input = Request {
        kind: 6,
        buffers: SearchBuffers {
            feature_codes,
            feature_count,
            row_count,
            ordered_rows: &[],
            decision_time_ms,
            release_time_ms,
            settlement_time_ms,
            valid,
            buy_win,
            sell_win,
            tie,
        },
        split_mask,
        candidates: CandidateConditions {
            condition_feature,
            condition_bucket,
            candidate_offsets,
            candidate_count,
        },
        sparse: Some(SparseIndex {
            candidate_driver_key,
            key_chrono_offsets,
            key_chrono_rows,
        }),
        expiry_ms,
        direction_code: 1,
        payout_basis,
    };
    input.validate()?;
    let result = match backend {
        Backend::Cpu => crate::cpu(|| input.reference()),
        #[cfg(feature = "cuda")]
        Backend::Cuda(device) => device.score(input)?,
    };
    Ok(result)
}

/// Executes `score_bucket_plans_cap1_basic_sparse_dual`; input order and dtypes match the native kernel ABI.
/// Output pointers become owned buffers. Validation runs for either backend.
pub fn score_bucket_plans_cap1_basic_sparse_dual(
    backend: &Backend,
    feature_codes: &[i16],
    condition_feature: &[i32],
    condition_bucket: &[i16],
    candidate_offsets: &[i32],
    candidate_driver_key: &[i32],
    key_chrono_offsets: &[i32],
    key_chrono_rows: &[i32],
    split_mask: &[u8],
    decision_time_ms: &[i64],
    release_time_ms: &[i64],
    valid: &[u8],
    buy_win: &[u8],
    sell_win: &[u8],
    tie: &[u8],
    candidate_count: i32,
    row_count: i32,
    expiry_ms: i64,
    payout_basis: i64,
) -> Result<Measured<DualScores>, String> {
    let feature_count = inferred_features(
        "score_bucket_plans_cap1_basic_sparse_dual",
        feature_codes,
        row_count,
        condition_feature,
    )?;
    let input = Request {
        kind: 7,
        buffers: SearchBuffers {
            feature_codes,
            feature_count,
            row_count,
            ordered_rows: &[],
            decision_time_ms,
            release_time_ms,
            settlement_time_ms: &[],
            valid,
            buy_win,
            sell_win,
            tie,
        },
        split_mask,
        candidates: CandidateConditions {
            condition_feature,
            condition_bucket,
            candidate_offsets,
            candidate_count,
        },
        sparse: Some(SparseIndex {
            candidate_driver_key,
            key_chrono_offsets,
            key_chrono_rows,
        }),
        expiry_ms,
        direction_code: 1,
        payout_basis,
    };
    input.validate()?;
    let result = match backend {
        Backend::Cpu => crate::cpu(|| input.reference()),
        #[cfg(feature = "cuda")]
        Backend::Cuda(device) => device.score(input)?,
    };
    Ok(result)
}

/// Executes `reconstruct_signal_masks_cap1`; input order and dtypes match the native kernel ABI.
/// Output pointers become owned buffers. Validation runs for either backend.
pub fn reconstruct_signal_masks_cap1(
    backend: &Backend,
    feature_codes: &[i16],
    condition_feature: &[i32],
    condition_bucket: &[i16],
    candidate_offsets: &[i32],
    split_mask: &[u8],
    ordered_rows: &[i64],
    decision_time_ms: &[i64],
    release_time_ms: &[i64],
    candidate_count: i32,
    row_count: i32,
    expiry_ms: i64,
) -> Result<Measured<Vec<u8>>, String> {
    let feature_count = inferred_features(
        "reconstruct_signal_masks_cap1",
        feature_codes,
        row_count,
        condition_feature,
    )?;
    let input = Request {
        kind: 8,
        buffers: SearchBuffers {
            feature_codes,
            feature_count,
            row_count,
            ordered_rows,
            decision_time_ms,
            release_time_ms,
            settlement_time_ms: &[],
            valid: &[],
            buy_win: &[],
            sell_win: &[],
            tie: &[],
        },
        split_mask,
        candidates: CandidateConditions {
            condition_feature,
            condition_bucket,
            candidate_offsets,
            candidate_count,
        },
        sparse: None,
        expiry_ms,
        direction_code: 1,
        payout_basis: 0,
    };
    input.validate()?;
    let result = match backend {
        Backend::Cpu => crate::cpu(|| input.reconstruct()),
        #[cfg(feature = "cuda")]
        Backend::Cuda(device) => device.reconstruct(input)?,
    };
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    // All settled rows win buy and lose sell. This lets the independent evaluator
    // derive every path metric from the admitted rows in closed form, without Path.
    fn independent(
        b: SearchBuffers<'_>,
        mask: &[u8],
        conditions: &[(usize, i16)],
    ) -> (Vec<u8>, Vec<u8>, [i64; 21], [i64; 21]) {
        let rows = b.row_count as usize;
        let membership: Vec<u8> = (0..rows)
            .map(|row| {
                u8::from(
                    conditions
                        .iter()
                        .all(|&(feature, bucket)| b.feature_codes[feature * rows + row] == bucket),
                )
            })
            .collect();
        let mut admitted = vec![0; rows];
        let mut reserved_until = None;
        let mut total = 0;
        let mut settled = Vec::new();
        for &row in b.ordered_rows {
            let row = row as usize;
            if membership[row] == 0 || mask[row] == 0 {
                continue;
            }
            if mask[row] != 2 {
                total += 1;
            }
            if b.release_time_ms[row] <= 0
                || reserved_until.is_some_and(|due| due > b.decision_time_ms[row])
            {
                continue;
            }
            reserved_until = Some(b.release_time_ms[row]);
            if mask[row] != 2 {
                admitted[row] = 1;
                if b.valid[row] != 0 {
                    settled.push(row);
                }
            }
        }
        let n = settled.len() as i64;
        let mut buy = [0; 21];
        buy[..8].copy_from_slice(&[total, n, 0, 0, total - n, total, 0, 92 * n]);
        buy[16] = n;
        buy[17] = 92 * 92 * n;
        let mut sell = [0; 21];
        sell[..10].copy_from_slice(&[total, 0, n, 0, total - n, 0, total, -100 * n, 100 * n, n]);
        sell[10] = ((10_000 * n * (n + 1) * (2 * n + 1) / 6) as f64).to_bits() as i64;
        sell[11] = n;
        sell[12] = n;
        sell[16] = n;
        sell[17] = 10_000 * n;
        sell[18] = 10_000 * n;
        if let (Some(&first), Some(&last)) = (settled.first(), settled.last()) {
            sell[19] = b.settlement_time_ms[last] - b.settlement_time_ms[first];
            sell[20] = b.settlement_time_ms[last] - b.decision_time_ms[first];
        }
        assert!(n > 0 && n < 20); // No complete rolling window in this fixture.
        (membership, admitted, buy, sell)
    }

    fn wide_conditions(backend: &Backend) {
        for multiframe in [false, true] {
            // Eight columns, each with a distinct failing row beyond the first four
            // conditions. Other-stream columns are prepared by latest-row alignment.
            let entry: Vec<i64> = (0..32).map(|row| 1000 + row * 10).collect();
            let codes: Vec<i16> = (0..8)
                .flat_map(|feature| {
                    let stride = if multiframe && feature >= 4 { 30 } else { 10 };
                    let source: Vec<_> = (0..32)
                        .map(|row| {
                            (
                                1000 + row * stride,
                                if row == feature { -1 } else { feature as i16 },
                            )
                        })
                        .collect();
                    entry
                        .iter()
                        .map(move |time| source.iter().rfind(|(at, _)| at <= time).unwrap().1)
                })
                .collect();
            let ordered: Vec<i64> = (0..32).collect();
            let release: Vec<i64> = entry
                .iter()
                .enumerate()
                .map(|(r, t)| if r == 27 { 0 } else { t + 20 })
                .collect();
            let settlement: Vec<i64> = entry.iter().map(|t| t + 20).collect();
            let valid: Vec<u8> = (0..32).map(|r| u8::from(r != 28)).collect();
            let split: Vec<u8> = (0..32)
                .map(|r| {
                    if r < 2 {
                        2
                    } else if r == 30 {
                        0
                    } else {
                        1
                    }
                })
                .collect();
            let b = SearchBuffers {
                feature_codes: &codes,
                feature_count: 8,
                row_count: 32,
                ordered_rows: &ordered,
                decision_time_ms: &entry,
                release_time_ms: &release,
                settlement_time_ms: &settlement,
                valid: &valid,
                buy_win: &[1; 32],
                sell_win: &[0; 32],
                tie: &[0; 32],
            };
            // Pack different lengths together so candidate offsets cannot be mistaken
            // for a common stride. The expected evaluator uses separate condition lists.
            let conditions: Vec<Vec<(usize, i16)>> = [5, 8]
                .into_iter()
                .map(|n| (0..n).map(|f| (f, f as i16)).collect())
                .collect();
            let features: Vec<i32> = conditions
                .iter()
                .flatten()
                .map(|&(f, _)| f as i32)
                .collect();
            let buckets: Vec<i16> = conditions.iter().flatten().map(|&(_, b)| b).collect();
            let expected: Vec<_> = conditions
                .iter()
                .map(|c| independent(b, &split, c))
                .collect();
            let pre: Vec<u8> = expected.iter().flat_map(|e| e.0.iter().copied()).collect();
            let post: Vec<u8> = expected.iter().flat_map(|e| e.1.iter().copied()).collect();
            assert_ne!(pre[..32], pre[32..]);
            assert_ne!(pre, post);
            let mut input = Request {
                kind: 8,
                buffers: b,
                split_mask: &split,
                candidates: CandidateConditions {
                    condition_feature: &features,
                    condition_bucket: &buckets,
                    candidate_offsets: &[0, 5, 13],
                    candidate_count: 2,
                },
                sparse: None,
                expiry_ms: 20,
                direction_code: 1,
                payout_basis: 92,
            };
            let reconstruct = |request: Request<'_>| {
                request.validate().unwrap();
                let cpu = request.reconstruct();
                #[cfg(feature = "cuda")]
                if let Backend::Cuda(device) = backend {
                    assert_eq!(device.reconstruct(request).unwrap().output, cpu);
                }
                cpu
            };
            assert_eq!(reconstruct(input), post);
            // Neutral admission isolates equality membership before capacity and folds.
            assert_eq!(
                reconstruct(Request {
                    buffers: SearchBuffers {
                        release_time_ms: &entry,
                        ..b
                    },
                    split_mask: &[1; 32],
                    ..input
                }),
                pre
            );
            let sparse_rows: Vec<i32> = (0..32).collect();
            for kind in 0..8 {
                input.kind = kind;
                input.sparse = (kind >= 4).then_some(SparseIndex {
                    candidate_driver_key: &[0, 0],
                    key_chrono_offsets: &[0, 32],
                    key_chrono_rows: &sparse_rows,
                });
                for direction in [1, -1] {
                    input.direction_code = direction;
                    input.validate().unwrap();
                    let output = input.reference();
                    let expected_buy: Vec<i64> = expected
                        .iter()
                        .flat_map(|e| {
                            let row = if input.dual() || direction == 1 {
                                &e.2
                            } else {
                                &e.3
                            };
                            row[..input.width()].iter().copied()
                        })
                        .collect();
                    assert_eq!(
                        output.buy_output, expected_buy,
                        "kind {kind}, direction {direction}, multiframe {multiframe}"
                    );
                    if input.dual() {
                        let expected_sell: Vec<i64> = expected
                            .iter()
                            .flat_map(|e| e.3[..input.width()].iter().copied())
                            .collect();
                        assert_eq!(output.sell_output, expected_sell);
                    }
                    #[cfg(feature = "cuda")]
                    if let Backend::Cuda(device) = backend {
                        assert_eq!(device.score(input).unwrap().output, output);
                    }
                }
            }
        }
        let _ = backend;
    }

    #[test]
    fn five_and_eight_conditions_match_independent_evaluator() {
        wide_conditions(&Backend::Cpu);
    }

    #[cfg(feature = "cuda")]
    #[test]
    fn five_and_eight_device_conditions_match_independent_evaluator() {
        wide_conditions(&Backend::cuda(0).expect("required CUDA device 0"));
    }
}
