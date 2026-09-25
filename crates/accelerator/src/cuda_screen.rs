//! Screening-only resident buffers. Full-output tuple and chunk APIs stay unchanged.

use super::*;

/// Packed per-row outcomes for one tile of at most eight distinct expiries.
#[derive(Clone, Copy)]
pub struct ScreenTile<'a> {
    pub bytes: &'a [u8],
    pub active: i32,
    pub stride: i32,
}

struct DeviceTile {
    packed: CudaSlice<u8>,
    active: i32,
    stride: i32,
}

/// A validated tuple with one reusable maximum-shape candidate and output allocation.
pub struct ScreenTuple<'a> {
    device: &'a Device,
    buffers: SearchBuffers<'a>,
    split_mask: &'a [u8],
    keys: SparseKeys<'a>,
    feature_codes: CudaSlice<i16>,
    key_offsets: CudaSlice<i32>,
    key_rows: CudaSlice<i32>,
    entry_times: CudaSlice<i64>,
    split: CudaSlice<u8>,
    tiles: Vec<DeviceTile>,
    features: CudaSlice<i32>,
    buckets: CudaSlice<i16>,
    offsets: CudaSlice<i32>,
    drivers: CudaSlice<i32>,
    output: CudaSlice<i32>,
    capacity: usize,
    slots: usize,
    /// Allocation, upload, synchronization, and first-launch cost.
    pub timings: Timings,
    pub free_before: usize,
    pub free_prelaunch: usize,
    pub free_after: usize,
    pub pool_before: (usize, usize),
    pub pool_prelaunch: (usize, usize),
    pub pool_after: (usize, usize),
}

impl Device {
    /// Warm the fused function and its allocator before reading the planning budget.
    pub fn screen_warmup_free_bytes(&self) -> Result<usize, String> {
        let packed = [0_u8; 16];
        let buffers = SearchBuffers {
            feature_codes: &[0],
            feature_count: 1,
            row_count: 1,
            ordered_rows: &[0],
            decision_time_ms: &[1],
            release_time_ms: &[2],
            settlement_time_ms: &[2],
            valid: &[1],
            buy_win: &[1],
            sell_win: &[0],
            tie: &[0],
        };
        let tuple = self.screen_tuple_workspace(
            buffers,
            &[1],
            SparseKeys {
                key_chrono_offsets: &[0, 1],
                key_chrono_rows: &[0],
            },
            &[ScreenTile {
                bytes: &packed,
                active: 1,
                stride: 16,
            }],
            1,
            1,
        )?;
        drop(tuple);
        self.sync("score_screen_fused", "warmup release")?;
        Ok(self.memory_info()?.0)
    }

    /// Preallocate the exact simultaneous buffers and launch once before scoring this tuple.
    pub fn screen_tuple_workspace<'a>(
        &'a self,
        buffers: SearchBuffers<'a>,
        split_mask: &'a [u8],
        keys: SparseKeys<'a>,
        tiles: &[ScreenTile<'a>],
        capacity: usize,
        slots: usize,
    ) -> Result<ScreenTuple<'a>, String> {
        let k = "score_screen_fused";
        crate::search::validate_tuple(buffers, &[split_mask], keys)?;
        if capacity == 0
            || capacity > i32::MAX as usize
            || slots == 0
            || capacity
                .checked_mul(slots)
                .is_none_or(|n| n > i32::MAX as usize)
        {
            return Err("score_screen_fused: invalid batch capacity or condition slots".into());
        }
        if tiles.is_empty() {
            return Err("score_screen_fused: no expiry tiles".into());
        }
        for tile in tiles {
            if !(1..=8).contains(&tile.active)
                || tile.stride < tile.active * 9
                || tile.stride % 8 != 0
                || tile.bytes.len() != (buffers.row_count as usize) * tile.stride as usize
            {
                return Err("score_screen_fused: invalid packed outcome tile".into());
            }
        }
        self.sync(k, "before preallocation")?;
        let free_before = self.memory_info()?.0;
        let pool_before = self.pool_usage()?;
        let started = Instant::now();
        let tile_buffers = tiles
            .iter()
            .map(|tile| {
                Ok(DeviceTile {
                    packed: self.upload(k, "packed_outcomes", tile.bytes)?,
                    active: tile.active,
                    stride: tile.stride,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let mut result = ScreenTuple {
            device: self,
            buffers,
            split_mask,
            keys,
            feature_codes: self.upload(k, "feature_codes", buffers.feature_codes)?,
            key_offsets: self.upload(k, "key_chrono_offsets", keys.key_chrono_offsets)?,
            key_rows: self.upload(k, "key_chrono_rows", keys.key_chrono_rows)?,
            entry_times: self.upload(k, "entry_time_ms", buffers.decision_time_ms)?,
            split: self.upload(k, "split_mask", split_mask)?,
            tiles: tile_buffers,
            features: self.zeros(k, "condition_feature", capacity * slots)?,
            buckets: self.zeros(k, "condition_bucket", capacity * slots)?,
            offsets: self.zeros(k, "candidate_offsets", capacity + 1)?,
            drivers: self.zeros(k, "candidate_driver_key", capacity)?,
            output: self.zeros(
                k,
                "output",
                capacity
                    * tiles
                        .iter()
                        .map(|tile| tile.active as usize)
                        .max()
                        .unwrap_or(1)
                    * 5,
            )?,
            capacity,
            slots,
            timings: Timings::default(),
            free_before,
            free_prelaunch: 0,
            free_after: 0,
            pool_before,
            pool_prelaunch: (0, 0),
            pool_after: (0, 0),
        };
        self.sync(k, "preallocation")?;
        result.free_prelaunch = self.memory_info()?.0;
        result.pool_prelaunch = self.pool_usage()?;
        // Fill the allocated batch at its maximum launch count. Negative drivers
        // avoid a potentially huge sparse walk while exercising the full grid.
        let probe_features = vec![0_i32; capacity];
        let probe_buckets = vec![0_i16; capacity];
        let probe_count = i32::try_from(capacity)
            .map_err(|_| "score_screen_fused: probe capacity exceeds i32")?;
        let probe_offsets: Vec<i32> = (0..=probe_count).collect();
        let probe_drivers = vec![-1_i32; capacity];
        let probe = CandidateConditions {
            condition_feature: &probe_features,
            condition_bucket: &probe_buckets,
            candidate_offsets: &probe_offsets,
            candidate_count: probe_count,
        };
        result.score_batch(probe, &probe_drivers, 0)?;
        self.sync(k, "first launch")?;
        result.free_after = self.memory_info()?.0;
        result.pool_after = self.pool_usage()?;
        result.timings = Timings {
            upload: started.elapsed(),
            allocated_bytes: result.allocated_bytes(),
            ..Timings::default()
        };
        Ok(result)
    }
}

impl ScreenTuple<'_> {
    pub fn allocated_bytes(&self) -> usize {
        self.feature_codes.num_bytes()
            + self.key_offsets.num_bytes()
            + self.key_rows.num_bytes()
            + self.entry_times.num_bytes()
            + self.split.num_bytes()
            + self
                .tiles
                .iter()
                .map(|tile| tile.packed.num_bytes())
                .sum::<usize>()
            + self.features.num_bytes()
            + self.buckets.num_bytes()
            + self.offsets.num_bytes()
            + self.drivers.num_bytes()
            + self.output.num_bytes()
    }

    pub fn score_batch(
        &mut self,
        candidates: CandidateConditions<'_>,
        driver_keys: &[i32],
        tile: usize,
    ) -> Result<Measured<Vec<i32>>, String> {
        let k = "score_screen_fused";
        let tile = self
            .tiles
            .get(tile)
            .ok_or("score_screen_fused: expiry tile out of range")?;
        let request = Request {
            kind: 7,
            buffers: self.buffers,
            split_mask: self.split_mask,
            candidates,
            sparse: Some(SparseIndex {
                candidate_driver_key: driver_keys,
                key_chrono_offsets: self.keys.key_chrono_offsets,
                key_chrono_rows: self.keys.key_chrono_rows,
            }),
            expiry_ms: 0,
            direction_code: 1,
            payout_basis: 0,
        };
        request.validate_resident_batch()?;
        let count = candidates.candidate_count as usize;
        if count > self.capacity || candidates.condition_feature.len() > self.capacity * self.slots
        {
            return Err("score_screen_fused: batch exceeds preallocated capacity".into());
        }
        self.device.sync(k, "before batch upload")?;
        let started = Instant::now();
        self.device
            .stream
            .memcpy_htod(
                candidates.condition_feature,
                &mut self
                    .features
                    .slice_mut(0..candidates.condition_feature.len()),
            )
            .map_err(|e| error(k, "condition_feature", e))?;
        self.device
            .record_upload(std::mem::size_of_val(candidates.condition_feature));
        self.device
            .stream
            .memcpy_htod(
                candidates.condition_bucket,
                &mut self.buckets.slice_mut(0..candidates.condition_bucket.len()),
            )
            .map_err(|e| error(k, "condition_bucket", e))?;
        self.device
            .record_upload(std::mem::size_of_val(candidates.condition_bucket));
        self.device
            .stream
            .memcpy_htod(
                candidates.candidate_offsets,
                &mut self
                    .offsets
                    .slice_mut(0..candidates.candidate_offsets.len()),
            )
            .map_err(|e| error(k, "candidate_offsets", e))?;
        self.device
            .record_upload(std::mem::size_of_val(candidates.candidate_offsets));
        self.device
            .stream
            .memcpy_htod(
                driver_keys,
                &mut self.drivers.slice_mut(0..driver_keys.len()),
            )
            .map_err(|e| error(k, "candidate_driver_key", e))?;
        self.device
            .record_upload(std::mem::size_of_val(driver_keys));
        self.device.sync(k, "batch upload")?;
        let upload = started.elapsed();
        let started = Instant::now();
        if count > 0 {
            let mut builder = self
                .device
                .stream
                .launch_builder(&self.device.functions[13]);
            builder.arg(&self.feature_codes);
            builder.arg(&self.features);
            builder.arg(&self.buckets);
            builder.arg(&self.offsets);
            builder.arg(&self.drivers);
            builder.arg(&self.key_offsets);
            builder.arg(&self.key_rows);
            builder.arg(&self.split);
            builder.arg(&self.entry_times);
            builder.arg(&tile.packed);
            builder.arg(&mut self.output);
            builder.arg(&candidates.candidate_count);
            builder.arg(&self.buffers.row_count);
            builder.arg(&tile.active);
            builder.arg(&tile.stride);
            // SAFETY: tuple and batch validation establish every indexed buffer bound;
            // all typed arguments remain alive until the stream is synchronized.
            unsafe { builder.launch(self.device.launch_config(13, candidates.candidate_count)?) }
                .map_err(|e| error(k, "launch", e))?;
        }
        self.device.sync(k, "execute")?;
        let execute = started.elapsed();
        let started = Instant::now();
        let active_len = count * tile.active as usize * 5;
        let output = self
            .device
            .stream
            .clone_dtoh(&self.output.slice(0..active_len))
            .map_err(|e| error(k, "output", e))?;
        self.device.sync(k, "download")?;
        Ok(Measured {
            output,
            timings: Timings {
                upload,
                execute,
                download: started.elapsed(),
                allocated_bytes: self.allocated_bytes(),
            },
        })
    }
}
