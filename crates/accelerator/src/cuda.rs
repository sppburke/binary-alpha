//! A single context, default stream, and ahead-of-time module per device.
//! Safe typed buffers own all allocations; only validated kernel launches are unsafe.

use crate::search::{
    CandidateConditions, DualScores, Request, SearchBuffers, SparseIndex, SparseKeys,
};
use crate::{KERNEL_SOURCES, MODULE_CUBIN, Measured, SCREEN_KERNEL_SOURCE, Timings};
use cudarc::driver::sys::CUdevice_attribute as Attribute;
use cudarc::driver::{
    CudaContext, CudaFunction, CudaModule, CudaSlice, CudaStream, DeviceRepr, LaunchConfig,
    PushKernelArg, ValidAsZeroBits,
};
use cudarc::nvrtc::Ptx;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

#[path = "cuda_screen.rs"]
mod screen;
pub use screen::{ScreenTile, ScreenTuple};

extern "C" fn no_dynamic_shared_memory(_: i32) -> usize {
    0
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceCapacity {
    pub sm_count: u32,
    pub warp_size: u32,
    pub threads_per_sm: u32,
    pub threads_per_block: u32,
    pub blocks_per_sm: u32,
    pub registers_per_sm: u32,
    pub registers_per_block: u32,
    pub shared_per_sm: u32,
    pub shared_per_block: u32,
    pub l2_bytes: u32,
    pub free_bytes: usize,
    pub total_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionCapacity {
    pub registers: u32,
    pub local_bytes: u32,
    pub max_threads_per_block: u32,
    pub derived_threads: u32,
    pub active_blocks_per_sm: u32,
}

/// Observed identity of the opened device and driver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceInfo {
    /// Driver-reported device name.
    pub name: String,
    /// Driver-reported (major, minor) compute capability.
    pub compute_capability: (i32, i32),
    /// CUDA driver API version as `cuDriverGetVersion` reports it (13000 for 13.0).
    pub driver_version: i32,
    pub build_target: &'static str,
    pub build_target_source: &'static str,
    pub capacity: DeviceCapacity,
}

/// An opened device with one loaded native module and its fourteen entry points.
pub struct Device {
    context: Arc<CudaContext>,
    stream: Arc<CudaStream>,
    _module: Arc<CudaModule>,
    functions: Vec<CudaFunction>,
    function_capacity: Vec<FunctionCapacity>,
    info: Box<DeviceInfo>,
    reservation_unit: Option<usize>,
    uploaded_bytes: AtomicUsize,
}

fn error(kernel: &str, argument: &str, error: impl std::fmt::Display) -> String {
    format!("{kernel}: {argument}: {error}")
}

fn positive_attribute(context: &CudaContext, attribute: Attribute) -> Result<u32, String> {
    let value = context
        .attribute(attribute)
        .map_err(|e| error("CUDA device", "attribute", e))?;
    u32::try_from(value)
        .ok()
        .filter(|&n| n > 0)
        .ok_or_else(|| format!("CUDA device: invalid {attribute:?} value {value}"))
}

fn function_capacity(function: &CudaFunction) -> Result<FunctionCapacity, String> {
    let (_, derived_threads) = function
        .occupancy_max_potential_block_size(no_dynamic_shared_memory, 0, 0, None)
        .map_err(|e| error("CUDA function", "occupancy", e))?;
    let active_blocks_per_sm = function
        .occupancy_max_active_blocks_per_multiprocessor(derived_threads, 0, None)
        .map_err(|e| error("CUDA function", "active blocks", e))?;
    let convert = |name, value| {
        u32::try_from(value).map_err(|_| format!("CUDA function: {name} is negative"))
    };
    Ok(FunctionCapacity {
        registers: convert(
            "registers",
            function
                .num_regs()
                .map_err(|e| error("CUDA function", "registers", e))?,
        )?,
        local_bytes: convert(
            "local bytes",
            function
                .local_size_bytes()
                .map_err(|e| error("CUDA function", "local bytes", e))?,
        )?,
        max_threads_per_block: convert(
            "maximum threads",
            function
                .max_threads_per_block()
                .map_err(|e| error("CUDA function", "maximum threads", e))?,
        )?,
        derived_threads,
        active_blocks_per_sm,
    })
}

impl Device {
    /// Opens the ordinal and loads the embedded native module once; never compiles source.
    pub fn open(ordinal: usize) -> Result<Self, String> {
        if ordinal > i32::MAX as usize {
            return Err(format!("CUDA device ordinal {ordinal} exceeds i32"));
        }
        // cudarc's dynamic loader panics when the driver library is absent.
        let context = std::panic::catch_unwind(|| CudaContext::new(ordinal))
            .map_err(|_| format!("CUDA device ordinal {ordinal}: driver library unavailable"))?
            .map_err(|e| error("CUDA device", "ordinal", e))?;
        let name = context
            .name()
            .map_err(|e| error("CUDA device", "name", e))?;
        let compute_capability = context
            .compute_capability()
            .map_err(|e| error("CUDA device", "compute_capability", e))?;
        let mut driver_version = 0;
        // The only foreign call outside a launch: cudarc exposes no safe driver-version query.
        // The pointer is a valid, initialized `i32` for the call's duration.
        let status = unsafe { cudarc::driver::sys::cuDriverGetVersion(&mut driver_version) };
        if status != cudarc::driver::sys::CUresult::CUDA_SUCCESS {
            return Err(error(
                "CUDA device",
                "driver_version",
                format!("{status:?}"),
            ));
        }
        let (free_bytes, total_bytes) = context
            .mem_get_info()
            .map_err(|e| error("CUDA device", "memory", e))?;
        let capacity = DeviceCapacity {
            sm_count: positive_attribute(
                &context,
                Attribute::CU_DEVICE_ATTRIBUTE_MULTIPROCESSOR_COUNT,
            )?,
            warp_size: positive_attribute(&context, Attribute::CU_DEVICE_ATTRIBUTE_WARP_SIZE)?,
            threads_per_sm: positive_attribute(
                &context,
                Attribute::CU_DEVICE_ATTRIBUTE_MAX_THREADS_PER_MULTIPROCESSOR,
            )?,
            threads_per_block: positive_attribute(
                &context,
                Attribute::CU_DEVICE_ATTRIBUTE_MAX_THREADS_PER_BLOCK,
            )?,
            blocks_per_sm: positive_attribute(
                &context,
                Attribute::CU_DEVICE_ATTRIBUTE_MAX_BLOCKS_PER_MULTIPROCESSOR,
            )?,
            registers_per_sm: positive_attribute(
                &context,
                Attribute::CU_DEVICE_ATTRIBUTE_MAX_REGISTERS_PER_MULTIPROCESSOR,
            )?,
            registers_per_block: positive_attribute(
                &context,
                Attribute::CU_DEVICE_ATTRIBUTE_MAX_REGISTERS_PER_BLOCK,
            )?,
            shared_per_sm: positive_attribute(
                &context,
                Attribute::CU_DEVICE_ATTRIBUTE_MAX_SHARED_MEMORY_PER_MULTIPROCESSOR,
            )?,
            shared_per_block: positive_attribute(
                &context,
                Attribute::CU_DEVICE_ATTRIBUTE_MAX_SHARED_MEMORY_PER_BLOCK,
            )?,
            l2_bytes: positive_attribute(&context, Attribute::CU_DEVICE_ATTRIBUTE_L2_CACHE_SIZE)?,
            free_bytes,
            total_bytes,
        };
        let info = DeviceInfo {
            name,
            compute_capability,
            driver_version,
            build_target: env!("BINARY_ALPHA_CUDA_ARCH"),
            build_target_source: env!("BINARY_ALPHA_CUDA_ARCH_SOURCE"),
            capacity,
        };
        let stream = context.default_stream();
        let module = context
            .load_module(Ptx::from_binary(MODULE_CUBIN.to_vec()))
            .map_err(|e| format!("CUDA module: BINARY_ALPHA_CUDA_ARCH={} has no compatible image for {} (sm_{}{}): {e}", info.build_target, info.name, compute_capability.0, compute_capability.1))?;
        let functions: Vec<CudaFunction> = KERNEL_SOURCES
            .iter()
            .chain(std::iter::once(&SCREEN_KERNEL_SOURCE))
            .map(|(name, _)| {
                module
                    .load_function(name)
                    .map_err(|e| format!("{name}: BINARY_ALPHA_CUDA_ARCH={} has no compatible image for sm_{}{}: {e}", info.build_target, compute_capability.0, compute_capability.1))
            })
            .collect::<Result<_, _>>()?;
        let function_capacity = functions
            .iter()
            .map(function_capacity)
            .collect::<Result<Vec<_>, _>>()?;
        let mut device = Self {
            context,
            stream,
            _module: module,
            functions,
            function_capacity,
            info: Box::new(info),
            reservation_unit: None,
            uploaded_bytes: AtomicUsize::new(0),
        };
        device.reservation_unit = device.measure_reservation_unit().unwrap_or(None);
        Ok(device)
    }

    /// Identity read when the device was opened.
    pub fn info(&self) -> &DeviceInfo {
        &self.info
    }

    pub fn screening_function(&self) -> &FunctionCapacity {
        &self.function_capacity[13]
    }

    pub fn screening_threads(&self) -> Result<(u32, &'static str), String> {
        let derived = self.screening_function().derived_threads;
        let Some(value) = std::env::var_os("BINARY_ALPHA_SCREEN_THREADS_PER_BLOCK") else {
            return Ok((derived, "derived"));
        };
        let text = value.to_string_lossy();
        let requested: u32 = text.parse().map_err(|_| {
            "BINARY_ALPHA_SCREEN_THREADS_PER_BLOCK: expected a positive integer".to_string()
        })?;
        let limit = self
            .info
            .capacity
            .threads_per_block
            .min(self.screening_function().max_threads_per_block);
        if requested == 0
            || requested > limit
            || !requested.is_multiple_of(self.info.capacity.warp_size)
        {
            return Err(format!(
                "BINARY_ALPHA_SCREEN_THREADS_PER_BLOCK: {requested} must be a positive warp multiple at most {limit}"
            ));
        }
        let active = self.functions[13]
            .occupancy_max_active_blocks_per_multiprocessor(requested, 0, None)
            .map_err(|e| error("BINARY_ALPHA_SCREEN_THREADS_PER_BLOCK", "occupancy", e))?;
        if active == 0 {
            return Err(
                "BINARY_ALPHA_SCREEN_THREADS_PER_BLOCK: no active blocks at requested width".into(),
            );
        }
        Ok((requested, "override"))
    }

    pub fn screening_batch_capacity(&self) -> Result<usize, String> {
        let (threads, _) = self.screening_threads()?;
        let blocks = self.functions[13]
            .occupancy_max_active_blocks_per_multiprocessor(threads, 0, None)
            .map_err(|e| error("score_screen_fused", "occupancy", e))?;
        (threads as usize)
            .checked_mul(blocks as usize)
            .and_then(|n| n.checked_mul(self.info.capacity.sm_count as usize))
            .and_then(|n| n.checked_mul(4))
            .map(|n| n.min(i32::MAX as usize))
            .ok_or("screening batch capacity overflows".into())
    }

    pub fn screening_local_hint_bytes(&self) -> Result<usize, String> {
        let (threads, _) = self.screening_threads()?;
        let blocks = self.functions[13]
            .occupancy_max_active_blocks_per_multiprocessor(threads, 0, None)
            .map_err(|e| error("score_screen_fused", "occupancy", e))?;
        (threads as usize)
            .checked_mul(blocks as usize)
            .and_then(|n| n.checked_mul(self.info.capacity.sm_count as usize))
            .and_then(|n| n.checked_mul(self.screening_function().local_bytes as usize))
            .ok_or("screening local memory estimate overflows".into())
    }

    fn launch_config(&self, function: usize, count: i32) -> Result<LaunchConfig, String> {
        let threads = if function == 13 {
            self.screening_threads()?.0
        } else {
            self.function_capacity[function].derived_threads
        };
        Ok(LaunchConfig {
            grid_dim: ((count as u32).div_ceil(threads), 1, 1),
            block_dim: (threads, 1, 1),
            shared_mem_bytes: 0,
        })
    }

    /// Driver-reported (free, total) bytes, distinct from operation allocation accounting.
    pub fn memory_info(&self) -> Result<(usize, usize), String> {
        self.context
            .mem_get_info()
            .map_err(|e| error("CUDA device", "memory_info", e))
    }

    pub fn reservation_unit(&self) -> Option<usize> {
        self.reservation_unit
    }

    /// Successful host-to-device copy bytes on this opened context.
    pub fn uploaded_bytes(&self) -> usize {
        self.uploaded_bytes.load(Ordering::Relaxed)
    }

    fn record_upload(&self, bytes: usize) {
        self.uploaded_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// (reserved, used) bytes in the device's default async allocation pool.
    pub fn pool_usage(&self) -> Result<(usize, usize), String> {
        use cudarc::driver::sys::{CUmemPool_attribute_enum as PoolAttribute, CUresult};
        let mut pool = std::ptr::null_mut();
        // SAFETY: the CUDA device ordinal is valid for this opened context; output storage
        // is initialized and each attribute receives a writable 64-bit value.
        let status = unsafe {
            cudarc::driver::sys::cuDeviceGetDefaultMemPool(&mut pool, self.context.ordinal() as i32)
        };
        if status != CUresult::CUDA_SUCCESS {
            return Err(format!("CUDA pool: default pool query failed: {status:?}"));
        }
        let read = |attribute| {
            let mut bytes = 0_u64;
            let status = unsafe {
                cudarc::driver::sys::cuMemPoolGetAttribute(
                    pool,
                    attribute,
                    (&mut bytes as *mut u64).cast(),
                )
            };
            if status == CUresult::CUDA_SUCCESS {
                Ok(bytes as usize)
            } else {
                Err(format!("CUDA pool: attribute query failed: {status:?}"))
            }
        };
        Ok((
            read(PoolAttribute::CU_MEMPOOL_ATTR_RESERVED_MEM_CURRENT)?,
            read(PoolAttribute::CU_MEMPOOL_ATTR_USED_MEM_CURRENT)?,
        ))
    }

    fn measure_reservation_unit(&self) -> Result<Option<usize>, String> {
        let before = self.pool_usage()?.0;
        let one = self.zeros::<u8>("CUDA pool probe", "one byte", 1)?;
        self.sync("CUDA pool probe", "one byte")?;
        let after_one = self.pool_usage()?.0;
        drop(one);
        self.sync("CUDA pool probe", "release one byte")?;
        let Some(unit) = after_one.checked_sub(before).filter(|&n| n > 0) else {
            return Ok(None);
        };
        let same = self.zeros::<u8>("CUDA pool probe", "unit bytes", unit)?;
        self.sync("CUDA pool probe", "unit bytes")?;
        let after_unit = self.pool_usage()?.0;
        drop(same);
        self.sync("CUDA pool probe", "release unit bytes")?;
        let plus = self.zeros::<u8>("CUDA pool probe", "unit plus one bytes", unit + 1)?;
        self.sync("CUDA pool probe", "unit plus one bytes")?;
        let after_plus = self.pool_usage()?.0;
        drop(plus);
        self.sync("CUDA pool probe", "release unit plus one bytes")?;
        Ok(
            (after_unit == after_one && after_plus == after_one.saturating_add(unit))
                .then_some(unit),
        )
    }

    /// Establish the sparse dual kernel's device-local stack reservation before planning.
    /// The planner then measures the free memory of this same physical device and context.
    pub fn search_planning_free_bytes(&self, batch_capacity: usize) -> Result<usize, String> {
        let count = i32::try_from(batch_capacity)
            .map_err(|_| "search planning: batch capacity exceeds i32")?;
        if count == 0 {
            return Err("search planning: batch capacity is zero".into());
        }
        let features = vec![0_i32; batch_capacity];
        let buckets = vec![0_i16; batch_capacity];
        let offsets: Vec<i32> = (0..=count).collect();
        let drivers = vec![0_i32; batch_capacity];
        let request = Request {
            kind: 6,
            buffers: SearchBuffers {
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
            },
            split_mask: &[1],
            candidates: CandidateConditions {
                condition_feature: &features,
                condition_bucket: &buckets,
                candidate_offsets: &offsets,
                candidate_count: count,
            },
            sparse: Some(SparseIndex {
                candidate_driver_key: &drivers,
                key_chrono_offsets: &[0, 1],
                key_chrono_rows: &[0],
            }),
            expiry_ms: 0,
            direction_code: 1,
            payout_basis: 0,
        };
        request.validate()?;
        self.score(request)?;
        self.sync("search planning", "release")?;
        Ok(self.memory_info()?.0)
    }

    fn sync(&self, kernel: &str, phase: &str) -> Result<(), String> {
        self.stream
            .synchronize()
            .map_err(|e| error(kernel, phase, e))
    }

    fn upload<T: DeviceRepr>(
        &self,
        kernel: &str,
        name: &str,
        values: &[T],
    ) -> Result<CudaSlice<T>, String> {
        let uploaded = self
            .stream
            .clone_htod(values)
            .map_err(|e| error(kernel, name, e))?;
        self.record_upload(std::mem::size_of_val(values));
        Ok(uploaded)
    }

    fn zeros<T: DeviceRepr + ValidAsZeroBits>(
        &self,
        kernel: &str,
        name: &str,
        length: usize,
    ) -> Result<CudaSlice<T>, String> {
        self.stream
            .alloc_zeros(length)
            .map_err(|e| error(kernel, name, e))
    }

    fn download<T: DeviceRepr>(
        &self,
        kernel: &str,
        name: &str,
        values: &CudaSlice<T>,
    ) -> Result<Vec<T>, String> {
        self.stream
            .clone_dtoh(values)
            .map_err(|e| error(kernel, name, e))
    }
}

impl Device {
    pub(crate) fn bootstrap_path_metrics(
        &self,
        input: crate::bootstrap::Paths<'_>,
    ) -> Result<Measured<crate::bootstrap::Metrics>, String> {
        // The public operation validates once before this private dispatch path.
        let k = "bootstrap_path_metrics";
        self.sync(k, "before upload")?;
        let start = Instant::now();
        let paths = self.upload(k, "paths", input.paths)?;
        let mut max_drawdowns =
            self.zeros::<f64>(k, "max_drawdowns", input.simulations as usize)?;
        let mut longest_underwater =
            self.zeros::<i64>(k, "longest_underwater", input.simulations as usize)?;
        let mut negative_rolling =
            self.zeros::<i64>(k, "negative_rolling", input.simulations as usize)?;
        self.sync(k, "upload")?;
        let upload = start.elapsed();
        let allocated_bytes = paths.num_bytes()
            + max_drawdowns.num_bytes()
            + longest_underwater.num_bytes()
            + negative_rolling.num_bytes();
        let start = Instant::now();
        if input.simulations > 0 {
            let mut builder = self.stream.launch_builder(&self.functions[9]);
            builder.arg(&paths);
            builder.arg(&mut max_drawdowns);
            builder.arg(&mut longest_underwater);
            builder.arg(&mut negative_rolling);
            builder.arg(&input.simulations);
            builder.arg(&input.trade_count);
            builder.arg(&input.rolling_horizon);
            // SAFETY: validation proves every buffer length, index, offset, and local-array
            // bound; typed arguments follow this symbol's ABI and remain alive through sync.
            unsafe { builder.launch(self.launch_config(9, input.simulations)?) }
                .map_err(|e| error(k, "launch", e))?;
        }
        self.sync(k, "execute")?;
        let execute = start.elapsed();
        let start = Instant::now();
        let max_drawdowns = self.download(k, "max_drawdowns", &max_drawdowns)?;
        let longest_underwater = self.download(k, "longest_underwater", &longest_underwater)?;
        let negative_rolling = self.download(k, "negative_rolling", &negative_rolling)?;
        self.sync(k, "download")?;
        let download = start.elapsed();
        Ok(Measured {
            output: crate::bootstrap::Metrics {
                max_drawdowns,
                longest_underwater,
                negative_rolling,
            },
            timings: Timings {
                upload,
                execute,
                download,
                allocated_bytes,
            },
        })
    }
}

impl Device {
    pub(crate) fn replay_capacity(
        &self,
        input: crate::portfolio::Capacity<'_>,
    ) -> Result<Measured<Vec<u8>>, String> {
        // The public operation validates once before this private dispatch path.
        let k = "replay_capacity";
        self.sync(k, "before upload")?;
        let start = Instant::now();
        let masks = self.upload(k, "masks", input.masks)?;
        let candidate_index = self.upload(k, "candidate_index", input.candidate_index)?;
        let entry_time = self.upload(k, "entry_time", input.entry_time)?;
        let due_time = self.upload(k, "due_time", input.due_time)?;
        let expiry_seconds = self.upload(k, "expiry_seconds", input.expiry_seconds)?;
        let hypothetical_valid = self.upload(k, "hypothetical_valid", input.hypothetical_valid)?;
        let standalone_admitted =
            self.upload(k, "standalone_admitted", input.standalone_admitted)?;
        let mut accepted = self.zeros::<u8>(
            k,
            "accepted",
            input.portfolio_count as usize * input.event_count as usize,
        )?;
        self.sync(k, "upload")?;
        let upload = start.elapsed();
        let allocated_bytes = masks.num_bytes()
            + candidate_index.num_bytes()
            + entry_time.num_bytes()
            + due_time.num_bytes()
            + expiry_seconds.num_bytes()
            + hypothetical_valid.num_bytes()
            + standalone_admitted.num_bytes()
            + accepted.num_bytes();
        let start = Instant::now();
        if input.portfolio_count > 0 {
            let mut builder = self.stream.launch_builder(&self.functions[10]);
            builder.arg(&masks);
            builder.arg(&candidate_index);
            builder.arg(&entry_time);
            builder.arg(&due_time);
            builder.arg(&expiry_seconds);
            builder.arg(&hypothetical_valid);
            builder.arg(&standalone_admitted);
            builder.arg(&mut accepted);
            builder.arg(&input.portfolio_count);
            builder.arg(&input.candidate_count);
            builder.arg(&input.event_count);
            builder.arg(&input.max_total);
            builder.arg(&input.max_expiry);
            // SAFETY: validation proves every buffer length, index, offset, and local-array
            // bound; typed arguments follow this symbol's ABI and remain alive through sync.
            unsafe { builder.launch(self.launch_config(10, input.portfolio_count)?) }
                .map_err(|e| error(k, "launch", e))?;
        }
        self.sync(k, "execute")?;
        let execute = start.elapsed();
        let start = Instant::now();
        let accepted = self.download(k, "accepted", &accepted)?;
        self.sync(k, "download")?;
        let download = start.elapsed();
        Ok(Measured {
            output: accepted,
            timings: Timings {
                upload,
                execute,
                download,
                allocated_bytes,
            },
        })
    }
}

impl Device {
    pub(crate) fn path_drawdown(
        &self,
        input: crate::portfolio::Returns<'_>,
    ) -> Result<Measured<crate::portfolio::Drawdown>, String> {
        // The public operation validates once before this private dispatch path.
        let k = "path_drawdown";
        self.sync(k, "before upload")?;
        let start = Instant::now();
        let returns = self.upload(k, "returns", input.returns)?;
        let mut max_drawdown =
            self.zeros::<f64>(k, "max_drawdown", input.portfolio_count as usize)?;
        let mut ulcer_index =
            self.zeros::<f64>(k, "ulcer_index", input.portfolio_count as usize)?;
        self.sync(k, "upload")?;
        let upload = start.elapsed();
        let allocated_bytes =
            returns.num_bytes() + max_drawdown.num_bytes() + ulcer_index.num_bytes();
        let start = Instant::now();
        if input.portfolio_count > 0 {
            let mut builder = self.stream.launch_builder(&self.functions[11]);
            builder.arg(&returns);
            builder.arg(&mut max_drawdown);
            builder.arg(&mut ulcer_index);
            builder.arg(&input.observation_count);
            builder.arg(&input.portfolio_count);
            // SAFETY: validation proves every buffer length, index, offset, and local-array
            // bound; typed arguments follow this symbol's ABI and remain alive through sync.
            unsafe { builder.launch(self.launch_config(11, input.portfolio_count)?) }
                .map_err(|e| error(k, "launch", e))?;
        }
        self.sync(k, "execute")?;
        let execute = start.elapsed();
        let start = Instant::now();
        let max_drawdown = self.download(k, "max_drawdown", &max_drawdown)?;
        let ulcer_index = self.download(k, "ulcer_index", &ulcer_index)?;
        self.sync(k, "download")?;
        let download = start.elapsed();
        Ok(Measured {
            output: crate::portfolio::Drawdown {
                max_drawdown,
                ulcer_index,
            },
            timings: Timings {
                upload,
                execute,
                download,
                allocated_bytes,
            },
        })
    }
}

impl Device {
    pub(crate) fn replay_policies(
        &self,
        input: crate::repair::Policies<'_>,
    ) -> Result<Measured<crate::repair::Replay>, String> {
        // The public operation validates once before this private dispatch path.
        let k = "replay_policies";
        self.sync(k, "before upload")?;
        let start = Instant::now();
        let candidate_offsets = self.upload(k, "candidate_offsets", input.candidate_offsets)?;
        let entry_ms = self.upload(k, "entry_ms", input.entry_ms)?;
        let settlement_ms = self.upload(k, "settlement_ms", input.settlement_ms)?;
        let close_ms = self.upload(k, "close_ms", input.close_ms)?;
        let outcomes = self.upload(k, "outcomes", input.outcomes)?;
        let valid_entry = self.upload(k, "valid_entry", input.valid_entry)?;
        let failure_words = self.upload(k, "failure_words", input.failure_words)?;
        let policy_candidate = self.upload(k, "policy_candidate", input.policy_candidate)?;
        let policy_masks = self.upload(k, "policy_masks", input.policy_masks)?;
        let mut out_opened = self.zeros::<i32>(k, "out_opened", input.policy_count as usize)?;
        let mut out_settled = self.zeros::<i32>(k, "out_settled", input.policy_count as usize)?;
        let mut out_wins = self.zeros::<i32>(k, "out_wins", input.policy_count as usize)?;
        let mut out_losses = self.zeros::<i32>(k, "out_losses", input.policy_count as usize)?;
        let mut out_ties = self.zeros::<i32>(k, "out_ties", input.policy_count as usize)?;
        let mut out_net_fp = self.zeros::<i64>(k, "out_net_fp", input.policy_count as usize)?;
        let mut out_max_dd_fp =
            self.zeros::<i64>(k, "out_max_dd_fp", input.policy_count as usize)?;
        let mut out_longest_dd_ms =
            self.zeros::<i64>(k, "out_longest_dd_ms", input.policy_count as usize)?;
        let mut out_longest_dd_trades =
            self.zeros::<i32>(k, "out_longest_dd_trades", input.policy_count as usize)?;
        let mut out_longest_loss_streak =
            self.zeros::<i32>(k, "out_longest_loss_streak", input.policy_count as usize)?;
        let mut out_gross_profit_fp =
            self.zeros::<i64>(k, "out_gross_profit_fp", input.policy_count as usize)?;
        let mut out_gross_loss_fp =
            self.zeros::<i64>(k, "out_gross_loss_fp", input.policy_count as usize)?;
        let mut out_sum_returns_fp =
            self.zeros::<i64>(k, "out_sum_returns_fp", input.policy_count as usize)?;
        let mut out_sum_squares_fp2 =
            self.zeros::<i64>(k, "out_sum_squares_fp2", input.policy_count as usize)?;
        let mut out_downside_squares_fp2 =
            self.zeros::<i64>(k, "out_downside_squares_fp2", input.policy_count as usize)?;
        self.sync(k, "upload")?;
        let upload = start.elapsed();
        let allocated_bytes = candidate_offsets.num_bytes()
            + entry_ms.num_bytes()
            + settlement_ms.num_bytes()
            + close_ms.num_bytes()
            + outcomes.num_bytes()
            + valid_entry.num_bytes()
            + failure_words.num_bytes()
            + policy_candidate.num_bytes()
            + policy_masks.num_bytes()
            + out_opened.num_bytes()
            + out_settled.num_bytes()
            + out_wins.num_bytes()
            + out_losses.num_bytes()
            + out_ties.num_bytes()
            + out_net_fp.num_bytes()
            + out_max_dd_fp.num_bytes()
            + out_longest_dd_ms.num_bytes()
            + out_longest_dd_trades.num_bytes()
            + out_longest_loss_streak.num_bytes()
            + out_gross_profit_fp.num_bytes()
            + out_gross_loss_fp.num_bytes()
            + out_sum_returns_fp.num_bytes()
            + out_sum_squares_fp2.num_bytes()
            + out_downside_squares_fp2.num_bytes();
        let start = Instant::now();
        if input.policy_count > 0 {
            let mut builder = self.stream.launch_builder(&self.functions[12]);
            builder.arg(&candidate_offsets);
            builder.arg(&entry_ms);
            builder.arg(&settlement_ms);
            builder.arg(&close_ms);
            builder.arg(&outcomes);
            builder.arg(&valid_entry);
            builder.arg(&failure_words);
            builder.arg(&input.word_count);
            builder.arg(&policy_candidate);
            builder.arg(&policy_masks);
            builder.arg(&input.policy_count);
            builder.arg(&input.start_ms);
            builder.arg(&input.end_ms);
            builder.arg(&input.payout_fp);
            builder.arg(&mut out_opened);
            builder.arg(&mut out_settled);
            builder.arg(&mut out_wins);
            builder.arg(&mut out_losses);
            builder.arg(&mut out_ties);
            builder.arg(&mut out_net_fp);
            builder.arg(&mut out_max_dd_fp);
            builder.arg(&mut out_longest_dd_ms);
            builder.arg(&mut out_longest_dd_trades);
            builder.arg(&mut out_longest_loss_streak);
            builder.arg(&mut out_gross_profit_fp);
            builder.arg(&mut out_gross_loss_fp);
            builder.arg(&mut out_sum_returns_fp);
            builder.arg(&mut out_sum_squares_fp2);
            builder.arg(&mut out_downside_squares_fp2);
            // SAFETY: validation proves every buffer length, index, offset, and local-array
            // bound; typed arguments follow this symbol's ABI and remain alive through sync.
            unsafe { builder.launch(self.launch_config(12, input.policy_count)?) }
                .map_err(|e| error(k, "launch", e))?;
        }
        self.sync(k, "execute")?;
        let execute = start.elapsed();
        let start = Instant::now();
        let out_opened = self.download(k, "out_opened", &out_opened)?;
        let out_settled = self.download(k, "out_settled", &out_settled)?;
        let out_wins = self.download(k, "out_wins", &out_wins)?;
        let out_losses = self.download(k, "out_losses", &out_losses)?;
        let out_ties = self.download(k, "out_ties", &out_ties)?;
        let out_net_fp = self.download(k, "out_net_fp", &out_net_fp)?;
        let out_max_dd_fp = self.download(k, "out_max_dd_fp", &out_max_dd_fp)?;
        let out_longest_dd_ms = self.download(k, "out_longest_dd_ms", &out_longest_dd_ms)?;
        let out_longest_dd_trades =
            self.download(k, "out_longest_dd_trades", &out_longest_dd_trades)?;
        let out_longest_loss_streak =
            self.download(k, "out_longest_loss_streak", &out_longest_loss_streak)?;
        let out_gross_profit_fp = self.download(k, "out_gross_profit_fp", &out_gross_profit_fp)?;
        let out_gross_loss_fp = self.download(k, "out_gross_loss_fp", &out_gross_loss_fp)?;
        let out_sum_returns_fp = self.download(k, "out_sum_returns_fp", &out_sum_returns_fp)?;
        let out_sum_squares_fp2 = self.download(k, "out_sum_squares_fp2", &out_sum_squares_fp2)?;
        let out_downside_squares_fp2 =
            self.download(k, "out_downside_squares_fp2", &out_downside_squares_fp2)?;
        self.sync(k, "download")?;
        let download = start.elapsed();
        Ok(Measured {
            output: crate::repair::Replay {
                out_opened,
                out_settled,
                out_wins,
                out_losses,
                out_ties,
                out_net_fp,
                out_max_dd_fp,
                out_longest_dd_ms,
                out_longest_dd_trades,
                out_longest_loss_streak,
                out_gross_profit_fp,
                out_gross_loss_fp,
                out_sum_returns_fp,
                out_sum_squares_fp2,
                out_downside_squares_fp2,
            },
            timings: Timings {
                upload,
                execute,
                download,
                allocated_bytes,
            },
        })
    }
}

struct Shared {
    feature_codes: CudaSlice<i16>,
    ordered_rows: CudaSlice<i64>,
    decision_time_ms: CudaSlice<i64>,
    release_time_ms: CudaSlice<i64>,
    settlement_time_ms: CudaSlice<i64>,
    valid: CudaSlice<u8>,
    buy_win: CudaSlice<u8>,
    sell_win: CudaSlice<u8>,
    tie: CudaSlice<u8>,
    split_masks: Vec<CudaSlice<u8>>,
}

impl Shared {
    fn bytes(&self) -> usize {
        self.feature_codes.num_bytes()
            + self.ordered_rows.num_bytes()
            + self.decision_time_ms.num_bytes()
            + self.release_time_ms.num_bytes()
            + self.settlement_time_ms.num_bytes()
            + self.valid.num_bytes()
            + self.buy_win.num_bytes()
            + self.sell_win.num_bytes()
            + self.tie.num_bytes()
            + self
                .split_masks
                .iter()
                .map(CudaSlice::num_bytes)
                .sum::<usize>()
    }
}

struct CandidateBuffers {
    condition_feature: CudaSlice<i32>,
    condition_bucket: CudaSlice<i16>,
    candidate_offsets: CudaSlice<i32>,
    sparse: Option<(CudaSlice<i32>, CudaSlice<i32>, CudaSlice<i32>)>,
}

impl CandidateBuffers {
    fn bytes(&self) -> usize {
        self.condition_feature.num_bytes()
            + self.condition_bucket.num_bytes()
            + self.candidate_offsets.num_bytes()
            + self.sparse.as_ref().map_or(0, |(keys, offsets, rows)| {
                keys.num_bytes() + offsets.num_bytes() + rows.num_bytes()
            })
    }
}

/// A candidate chunk tied to its workspace, borrowing immutable host inputs for validation.
/// Scoring and reconstruction borrow these same device allocations for the chunk's lifetime.
pub struct ResidentChunk<'stage, 'data> {
    workspace: &'stage ResidentSearch<'data>,
    candidates: CandidateConditions<'stage>,
    sparse: Option<SparseIndex<'stage>>,
    uploaded: CandidateBuffers,
    /// One-time candidate allocation and upload cost; bytes exclude shared workspace inputs.
    pub timings: Timings,
}

/// Shared encoded features, chronology, times, flags, and split masks kept on the device.
/// `upload_candidates` retains each candidate chunk for repeated scoring and reconstruction.
/// `timings` records the one-time shared upload separately from chunk and operation costs.
pub struct ResidentSearch<'a> {
    device: &'a Device,
    buffers: SearchBuffers<'a>,
    split_masks: Vec<&'a [u8]>,
    shared: Shared,
    /// One-time shared allocation and upload cost.
    pub timings: Timings,
}

/// One tuple's matrix, chronological buffers, and sparse key lists reside on one device.
pub struct ResidentTuple<'a> {
    workspace: ResidentSearch<'a>,
    keys: SparseKeys<'a>,
    offsets: CudaSlice<i32>,
    rows: CudaSlice<i32>,
    pub timings: Timings,
}

/// A batch uploads only its conditions, offsets, and driver IDs.
pub struct ResidentTupleBatch<'stage, 'data> {
    tuple: &'stage ResidentTuple<'data>,
    candidates: CandidateConditions<'stage>,
    driver_keys: &'stage [i32],
    uploaded: CandidateBuffers,
    drivers: CudaSlice<i32>,
    pub timings: Timings,
}

impl Device {
    pub fn search_tuple_workspace<'a>(
        &'a self,
        buffers: SearchBuffers<'a>,
        split_masks: &[&'a [u8]],
        keys: SparseKeys<'a>,
    ) -> Result<ResidentTuple<'a>, String> {
        let validation_started = Instant::now();
        crate::search::validate_tuple(buffers, split_masks, keys).map_err(|error| {
            if error.starts_with("resident tuple: split_mask length") {
                "resident tuple: split_mask length differs from row_count".into()
            } else {
                error
            }
        })?;
        let validation = validation_started.elapsed();
        let shared = self.shared(buffers, split_masks, "search tuple")?;
        let workspace = ResidentSearch {
            device: self,
            buffers,
            split_masks: split_masks.to_vec(),
            shared: shared.output,
            timings: shared.timings,
        };
        self.sync("search tuple", "before upload")?;
        let started = Instant::now();
        let offsets = self.upload(
            "search tuple",
            "key_chrono_offsets",
            keys.key_chrono_offsets,
        )?;
        let rows = self.upload("search tuple", "key_chrono_rows", keys.key_chrono_rows)?;
        self.sync("search tuple", "upload")?;
        let timings = Timings {
            upload: validation + workspace.timings.upload + started.elapsed(),
            allocated_bytes: workspace.timings.allocated_bytes
                + offsets.num_bytes()
                + rows.num_bytes(),
            ..Timings::default()
        };
        Ok(ResidentTuple {
            workspace,
            keys,
            offsets,
            rows,
            timings,
        })
    }
}

impl<'data> ResidentTuple<'data> {
    /// Replace only expiry-dependent arrays; the feature matrix and sparse keys stay resident.
    pub fn set_outcome(
        &mut self,
        buffers: SearchBuffers<'data>,
        split_mask: &'data [u8],
    ) -> Result<Timings, String> {
        let started = Instant::now();
        crate::search::validate_tuple_outcome(self.workspace.buffers, buffers, split_mask)?;
        let device = self.workspace.device;
        device.sync("search tuple outcome", "before upload")?;
        let shared = &mut self.workspace.shared;
        device
            .stream
            .memcpy_htod(buffers.release_time_ms, &mut shared.release_time_ms)
            .map_err(|source| error("search tuple outcome", "release_time_ms", source))?;
        device
            .stream
            .memcpy_htod(buffers.settlement_time_ms, &mut shared.settlement_time_ms)
            .map_err(|source| error("search tuple outcome", "settlement_time_ms", source))?;
        device
            .stream
            .memcpy_htod(buffers.valid, &mut shared.valid)
            .map_err(|source| error("search tuple outcome", "valid", source))?;
        device
            .stream
            .memcpy_htod(buffers.buy_win, &mut shared.buy_win)
            .map_err(|source| error("search tuple outcome", "buy_win", source))?;
        device
            .stream
            .memcpy_htod(buffers.sell_win, &mut shared.sell_win)
            .map_err(|source| error("search tuple outcome", "sell_win", source))?;
        device
            .stream
            .memcpy_htod(buffers.tie, &mut shared.tie)
            .map_err(|source| error("search tuple outcome", "tie", source))?;
        device
            .stream
            .memcpy_htod(split_mask, &mut shared.split_masks[0])
            .map_err(|source| error("search tuple outcome", "split_mask", source))?;
        device.sync("search tuple outcome", "upload")?;
        self.workspace.buffers = buffers;
        self.workspace.split_masks[0] = split_mask;
        Ok(Timings {
            upload: started.elapsed(),
            allocated_bytes: self.timings.allocated_bytes,
            ..Timings::default()
        })
    }

    pub fn upload_batch<'stage>(
        &'stage self,
        candidates: CandidateConditions<'stage>,
        driver_keys: &'stage [i32],
    ) -> Result<ResidentTupleBatch<'stage, 'data>, String> {
        let input = Request {
            kind: 6,
            buffers: self.workspace.buffers,
            split_mask: self.workspace.split_masks[0],
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
        input.validate_resident_batch()?;
        let uploaded =
            self.workspace
                .device
                .candidate_buffers(candidates, None, "search tuple batch")?;
        let started = Instant::now();
        let drivers = self.workspace.device.upload(
            "search tuple batch",
            "candidate_driver_key",
            driver_keys,
        )?;
        self.workspace.device.sync("search tuple batch", "upload")?;
        let timings = Timings {
            upload: uploaded.timings.upload + started.elapsed(),
            allocated_bytes: uploaded.timings.allocated_bytes + drivers.num_bytes(),
            ..Timings::default()
        };
        Ok(ResidentTupleBatch {
            tuple: self,
            candidates,
            driver_keys,
            uploaded: uploaded.output,
            drivers,
            timings,
        })
    }
}

impl ResidentTupleBatch<'_, '_> {
    pub fn score_sparse_dual(
        &self,
        split: usize,
        expiry_ms: i64,
        payout_basis: i64,
    ) -> Result<Measured<DualScores>, String> {
        let split_mask = *self
            .tuple
            .workspace
            .split_masks
            .get(split)
            .ok_or("resident tuple: split index outside split masks")?;
        let input = Request {
            kind: 6,
            buffers: self.tuple.workspace.buffers,
            split_mask,
            candidates: self.candidates,
            sparse: Some(SparseIndex {
                candidate_driver_key: self.driver_keys,
                key_chrono_offsets: self.tuple.keys.key_chrono_offsets,
                key_chrono_rows: self.tuple.keys.key_chrono_rows,
            }),
            expiry_ms,
            direction_code: 1,
            payout_basis,
        };
        self.tuple.workspace.device.score_resident(
            input,
            &self.tuple.workspace.shared,
            &self.uploaded,
            split,
            Some((&self.drivers, &self.tuple.offsets, &self.tuple.rows)),
        )
    }
}

impl Device {
    /// Uploads all shared search buffers and every split once for reuse by K1–K9.
    pub fn search_workspace<'a>(
        &'a self,
        buffers: SearchBuffers<'a>,
        split_masks: &[&'a [u8]],
    ) -> Result<ResidentSearch<'a>, String> {
        for &split_mask in split_masks {
            Request {
                kind: 0,
                buffers,
                split_mask,
                candidates: CandidateConditions {
                    condition_feature: &[],
                    condition_bucket: &[],
                    candidate_offsets: &[0],
                    candidate_count: 0,
                },
                sparse: None,
                expiry_ms: 0,
                direction_code: 1,
                payout_basis: 0,
            }
            .validate()?;
        }
        let uploaded = self.shared(buffers, split_masks, "score_bucket_plans_cap1")?;
        Ok(ResidentSearch {
            device: self,
            buffers,
            split_masks: split_masks.to_vec(),
            shared: uploaded.output,
            timings: uploaded.timings,
        })
    }

    fn shared(
        &self,
        buffers: SearchBuffers<'_>,
        split_masks: &[&[u8]],
        k: &str,
    ) -> Result<Measured<Shared>, String> {
        self.sync(k, "before upload")?;
        let start = Instant::now();
        let output = Shared {
            feature_codes: self.upload(k, "feature_codes", buffers.feature_codes)?,
            ordered_rows: self.upload(k, "ordered_rows", buffers.ordered_rows)?,
            decision_time_ms: self.upload(k, "decision_time_ms", buffers.decision_time_ms)?,
            release_time_ms: self.upload(k, "release_time_ms", buffers.release_time_ms)?,
            settlement_time_ms: self.upload(k, "settlement_time_ms", buffers.settlement_time_ms)?,
            valid: self.upload(k, "valid", buffers.valid)?,
            buy_win: self.upload(k, "buy_win", buffers.buy_win)?,
            sell_win: self.upload(k, "sell_win", buffers.sell_win)?,
            tie: self.upload(k, "tie", buffers.tie)?,
            split_masks: split_masks
                .iter()
                .map(|mask| self.upload(k, "split_mask", mask))
                .collect::<Result<_, _>>()?,
        };
        self.sync(k, "upload")?;
        let timings = Timings {
            upload: start.elapsed(),
            allocated_bytes: output.bytes(),
            ..Timings::default()
        };
        Ok(Measured { output, timings })
    }

    fn candidate_buffers(
        &self,
        candidates: CandidateConditions<'_>,
        sparse: Option<SparseIndex<'_>>,
        k: &str,
    ) -> Result<Measured<CandidateBuffers>, String> {
        self.sync(k, "before candidate upload")?;
        let start = Instant::now();
        let output = CandidateBuffers {
            condition_feature: self.upload(k, "condition_feature", candidates.condition_feature)?,
            condition_bucket: self.upload(k, "condition_bucket", candidates.condition_bucket)?,
            candidate_offsets: self.upload(k, "candidate_offsets", candidates.candidate_offsets)?,
            sparse: sparse
                .map(|index| -> Result<_, String> {
                    Ok((
                        self.upload(k, "candidate_driver_key", index.candidate_driver_key)?,
                        self.upload(k, "key_chrono_offsets", index.key_chrono_offsets)?,
                        self.upload(k, "key_chrono_rows", index.key_chrono_rows)?,
                    ))
                })
                .transpose()?,
        };
        self.sync(k, "candidate upload")?;
        let timings = Timings {
            upload: start.elapsed(),
            allocated_bytes: output.bytes(),
            ..Timings::default()
        };
        Ok(Measured { output, timings })
    }

    // The public search operation validates once before entering this upload/launch path.
    pub(crate) fn score(&self, input: Request<'_>) -> Result<Measured<DualScores>, String> {
        let shared = self.shared(input.buffers, &[input.split_mask], input.kernel())?;
        let candidates = self.candidate_buffers(input.candidates, input.sparse, input.kernel())?;
        let mut result = self.score_resident(input, &shared.output, &candidates.output, 0, None)?;
        result.timings.upload += shared.timings.upload + candidates.timings.upload;
        Ok(result)
    }

    // The public reconstruction operation validates once before upload.
    pub(crate) fn reconstruct(&self, input: Request<'_>) -> Result<Measured<Vec<u8>>, String> {
        let shared = self.shared(input.buffers, &[input.split_mask], input.kernel())?;
        let candidates = self.candidate_buffers(input.candidates, None, input.kernel())?;
        let mut result = self.reconstruct_resident(input, &shared.output, &candidates.output, 0)?;
        result.timings.upload += shared.timings.upload + candidates.timings.upload;
        Ok(result)
    }
}

impl Device {
    fn score_resident(
        &self,
        input: Request<'_>,
        shared: &Shared,
        candidates: &CandidateBuffers,
        split: usize,
        sparse_override: Option<(&CudaSlice<i32>, &CudaSlice<i32>, &CudaSlice<i32>)>,
    ) -> Result<Measured<DualScores>, String> {
        let k = input.kernel();
        self.sync(k, "before upload")?;
        let start = Instant::now();
        let sparse =
            sparse_override.or_else(|| candidates.sparse.as_ref().map(|(a, b, c)| (a, b, c)));
        let length = input.candidates.candidate_count as usize * input.width();
        let mut buy_output = self.zeros::<i64>(k, "buy_output/output", length)?;
        let mut sell_output =
            self.zeros::<i64>(k, "sell_output", if input.dual() { length } else { 0 })?;
        self.sync(k, "upload")?;
        let upload = start.elapsed();
        let allocated_bytes = shared.bytes()
            + candidates.bytes()
            + sparse_override.map_or(0, |(a, b, c)| a.num_bytes() + b.num_bytes() + c.num_bytes())
            + buy_output.num_bytes()
            + sell_output.num_bytes();
        let start = Instant::now();
        if input.candidates.candidate_count > 0 {
            let mut builder = self.stream.launch_builder(&self.functions[input.kind]);
            builder.arg(&shared.feature_codes);
            builder.arg(&candidates.condition_feature);
            builder.arg(&candidates.condition_bucket);
            builder.arg(&candidates.candidate_offsets);
            match input.kind {
                0 => {
                    builder.arg(&shared.split_masks[split]);
                    builder.arg(&shared.ordered_rows);
                    builder.arg(&shared.decision_time_ms);
                    builder.arg(&shared.release_time_ms);
                    builder.arg(&shared.settlement_time_ms);
                    builder.arg(&shared.valid);
                    builder.arg(&shared.buy_win);
                    builder.arg(&shared.sell_win);
                    builder.arg(&shared.tie);
                    builder.arg(&mut buy_output);
                    builder.arg(&input.candidates.candidate_count);
                    builder.arg(&input.buffers.row_count);
                    builder.arg(&input.buffers.feature_count);
                    builder.arg(&input.expiry_ms);
                    builder.arg(&input.direction_code);
                    builder.arg(&input.payout_basis);
                }
                1 => {
                    builder.arg(&shared.split_masks[split]);
                    builder.arg(&shared.ordered_rows);
                    builder.arg(&shared.decision_time_ms);
                    builder.arg(&shared.release_time_ms);
                    builder.arg(&shared.settlement_time_ms);
                    builder.arg(&shared.valid);
                    builder.arg(&shared.buy_win);
                    builder.arg(&shared.sell_win);
                    builder.arg(&shared.tie);
                    builder.arg(&mut buy_output);
                    builder.arg(&mut sell_output);
                    builder.arg(&input.candidates.candidate_count);
                    builder.arg(&input.buffers.row_count);
                    builder.arg(&input.buffers.feature_count);
                    builder.arg(&input.expiry_ms);
                    builder.arg(&input.payout_basis);
                }
                2 => {
                    builder.arg(&shared.split_masks[split]);
                    builder.arg(&shared.ordered_rows);
                    builder.arg(&shared.decision_time_ms);
                    builder.arg(&shared.release_time_ms);
                    builder.arg(&shared.valid);
                    builder.arg(&shared.buy_win);
                    builder.arg(&shared.sell_win);
                    builder.arg(&shared.tie);
                    builder.arg(&mut buy_output);
                    builder.arg(&input.candidates.candidate_count);
                    builder.arg(&input.buffers.row_count);
                    builder.arg(&input.expiry_ms);
                    builder.arg(&input.direction_code);
                    builder.arg(&input.payout_basis);
                }
                3 => {
                    builder.arg(&shared.split_masks[split]);
                    builder.arg(&shared.ordered_rows);
                    builder.arg(&shared.decision_time_ms);
                    builder.arg(&shared.release_time_ms);
                    builder.arg(&shared.valid);
                    builder.arg(&shared.buy_win);
                    builder.arg(&shared.sell_win);
                    builder.arg(&shared.tie);
                    builder.arg(&mut buy_output);
                    builder.arg(&mut sell_output);
                    builder.arg(&input.candidates.candidate_count);
                    builder.arg(&input.buffers.row_count);
                    builder.arg(&input.expiry_ms);
                    builder.arg(&input.payout_basis);
                }
                4 => {
                    let (candidate_driver_key, key_chrono_offsets, key_chrono_rows) =
                        sparse.expect("sparse request validated");
                    builder.arg(candidate_driver_key);
                    builder.arg(key_chrono_offsets);
                    builder.arg(key_chrono_rows);
                    builder.arg(&shared.split_masks[split]);
                    builder.arg(&shared.decision_time_ms);
                    builder.arg(&shared.release_time_ms);
                    builder.arg(&shared.settlement_time_ms);
                    builder.arg(&shared.valid);
                    builder.arg(&shared.buy_win);
                    builder.arg(&shared.sell_win);
                    builder.arg(&shared.tie);
                    builder.arg(&mut buy_output);
                    builder.arg(&input.candidates.candidate_count);
                    builder.arg(&input.buffers.row_count);
                    builder.arg(&input.expiry_ms);
                    builder.arg(&input.direction_code);
                    builder.arg(&input.payout_basis);
                }
                5 => {
                    let (candidate_driver_key, key_chrono_offsets, key_chrono_rows) =
                        sparse.expect("sparse request validated");
                    builder.arg(candidate_driver_key);
                    builder.arg(key_chrono_offsets);
                    builder.arg(key_chrono_rows);
                    builder.arg(&shared.split_masks[split]);
                    builder.arg(&shared.decision_time_ms);
                    builder.arg(&shared.release_time_ms);
                    builder.arg(&shared.valid);
                    builder.arg(&shared.buy_win);
                    builder.arg(&shared.sell_win);
                    builder.arg(&shared.tie);
                    builder.arg(&mut buy_output);
                    builder.arg(&input.candidates.candidate_count);
                    builder.arg(&input.buffers.row_count);
                    builder.arg(&input.expiry_ms);
                    builder.arg(&input.direction_code);
                    builder.arg(&input.payout_basis);
                }
                6 => {
                    let (candidate_driver_key, key_chrono_offsets, key_chrono_rows) =
                        sparse.expect("sparse request validated");
                    builder.arg(candidate_driver_key);
                    builder.arg(key_chrono_offsets);
                    builder.arg(key_chrono_rows);
                    builder.arg(&shared.split_masks[split]);
                    builder.arg(&shared.decision_time_ms);
                    builder.arg(&shared.release_time_ms);
                    builder.arg(&shared.settlement_time_ms);
                    builder.arg(&shared.valid);
                    builder.arg(&shared.buy_win);
                    builder.arg(&shared.sell_win);
                    builder.arg(&shared.tie);
                    builder.arg(&mut buy_output);
                    builder.arg(&mut sell_output);
                    builder.arg(&input.candidates.candidate_count);
                    builder.arg(&input.buffers.row_count);
                    builder.arg(&input.expiry_ms);
                    builder.arg(&input.payout_basis);
                }
                7 => {
                    let (candidate_driver_key, key_chrono_offsets, key_chrono_rows) =
                        sparse.expect("sparse request validated");
                    builder.arg(candidate_driver_key);
                    builder.arg(key_chrono_offsets);
                    builder.arg(key_chrono_rows);
                    builder.arg(&shared.split_masks[split]);
                    builder.arg(&shared.decision_time_ms);
                    builder.arg(&shared.release_time_ms);
                    builder.arg(&shared.valid);
                    builder.arg(&shared.buy_win);
                    builder.arg(&shared.sell_win);
                    builder.arg(&shared.tie);
                    builder.arg(&mut buy_output);
                    builder.arg(&mut sell_output);
                    builder.arg(&input.candidates.candidate_count);
                    builder.arg(&input.buffers.row_count);
                    builder.arg(&input.expiry_ms);
                    builder.arg(&input.payout_basis);
                }
                _ => unreachable!("scoring request uses one of K1–K8"),
            }
            // SAFETY: shared validation proves buffer shapes and every active feature,
            // row, and sparse offset; arguments follow the selected symbol's exact ABI.
            // All inputs and writable outputs live through the synchronized launch.
            unsafe {
                builder.launch(self.launch_config(input.kind, input.candidates.candidate_count)?)
            }
            .map_err(|e| error(k, "launch", e))?;
        }
        self.sync(k, "execute")?;
        let execute = start.elapsed();
        let start = Instant::now();
        let output = DualScores {
            buy_output: self.download(k, "buy_output/output", &buy_output)?,
            sell_output: self.download(k, "sell_output", &sell_output)?,
        };
        self.sync(k, "download")?;
        Ok(Measured {
            output,
            timings: Timings {
                upload,
                execute,
                download: start.elapsed(),
                allocated_bytes,
            },
        })
    }
}

impl Device {
    fn reconstruct_resident(
        &self,
        input: Request<'_>,
        shared: &Shared,
        candidates: &CandidateBuffers,
        split: usize,
    ) -> Result<Measured<Vec<u8>>, String> {
        let k = input.kernel();
        self.sync(k, "before upload")?;
        let start = Instant::now();
        let mut output = self.zeros::<u8>(
            k,
            "output",
            input.candidates.candidate_count as usize * input.buffers.row_count as usize,
        )?;
        self.sync(k, "upload")?;
        let upload = start.elapsed();
        let allocated_bytes = shared.bytes() + candidates.bytes() + output.num_bytes();
        let start = Instant::now();
        if input.candidates.candidate_count > 0 {
            let mut builder = self.stream.launch_builder(&self.functions[input.kind]);
            builder.arg(&shared.feature_codes);
            builder.arg(&candidates.condition_feature);
            builder.arg(&candidates.condition_bucket);
            builder.arg(&candidates.candidate_offsets);
            builder.arg(&shared.split_masks[split]);
            builder.arg(&shared.ordered_rows);
            builder.arg(&shared.decision_time_ms);
            builder.arg(&shared.release_time_ms);
            builder.arg(&mut output);
            builder.arg(&input.candidates.candidate_count);
            builder.arg(&input.buffers.row_count);
            builder.arg(&input.expiry_ms);
            // SAFETY: shared validation proves buffer shapes and every active feature,
            // row, and sparse offset; arguments follow the selected symbol's exact ABI.
            // All inputs and writable outputs live through the synchronized launch.
            unsafe { builder.launch(self.launch_config(8, input.candidates.candidate_count)?) }
                .map_err(|e| error(k, "launch", e))?;
        }
        self.sync(k, "execute")?;
        let execute = start.elapsed();
        let start = Instant::now();
        let output = self.download(k, "output", &output)?;
        self.sync(k, "download")?;
        Ok(Measured {
            output,
            timings: Timings {
                upload,
                execute,
                download: start.elapsed(),
                allocated_bytes,
            },
        })
    }
}

impl<'data> ResidentSearch<'data> {
    /// Uploads flattened conditions, buckets, offsets, and an optional sparse index once.
    /// The returned chunk can score either direction, screen, and reconstruct repeatedly.
    pub fn upload_candidates<'stage>(
        &'stage self,
        candidates: CandidateConditions<'stage>,
        sparse: Option<SparseIndex<'stage>>,
    ) -> Result<ResidentChunk<'stage, 'data>, String> {
        let split_mask = *self
            .split_masks
            .first()
            .ok_or("candidate upload requires a resident split")?;
        Request {
            kind: if sparse.is_some() { 4 } else { 0 },
            buffers: self.buffers,
            split_mask,
            candidates,
            sparse,
            expiry_ms: 0,
            direction_code: 1,
            payout_basis: 0,
        }
        .validate()?;
        let uploaded =
            self.device
                .candidate_buffers(candidates, sparse, "search candidate chunk")?;
        Ok(ResidentChunk {
            workspace: self,
            candidates,
            sparse,
            uploaded: uploaded.output,
            timings: uploaded.timings,
        })
    }
}

impl ResidentChunk<'_, '_> {
    /// Runs `score_bucket_plans_cap1` borrowing this chunk and the selected resident split.
    pub fn score_bucket_plans_cap1(
        &self,
        split: usize,
        expiry_ms: i64,
        direction_code: i32,
        payout_basis: i64,
    ) -> Result<Measured<Vec<i64>>, String> {
        let split_mask = *self.workspace.split_masks.get(split).ok_or_else(|| {
            format!("score_bucket_plans_cap1: split index {split} outside split_masks")
        })?;
        let input = Request {
            kind: 0,
            buffers: self.workspace.buffers,
            split_mask,
            candidates: self.candidates,
            sparse: None,
            expiry_ms,
            direction_code,
            payout_basis,
        };
        input.validate()?;
        let result = self.workspace.device.score_resident(
            input,
            &self.workspace.shared,
            &self.uploaded,
            split,
            None,
        )?;
        Ok(Measured {
            output: result.output.buy_output,
            timings: result.timings,
        })
    }
    /// Runs `score_bucket_plans_cap1_dual` borrowing this chunk and the selected resident split.
    pub fn score_bucket_plans_cap1_dual(
        &self,
        split: usize,
        expiry_ms: i64,
        payout_basis: i64,
    ) -> Result<Measured<DualScores>, String> {
        let split_mask = *self.workspace.split_masks.get(split).ok_or_else(|| {
            format!("score_bucket_plans_cap1_dual: split index {split} outside split_masks")
        })?;
        let input = Request {
            kind: 1,
            buffers: self.workspace.buffers,
            split_mask,
            candidates: self.candidates,
            sparse: None,
            expiry_ms,
            direction_code: 1,
            payout_basis,
        };
        input.validate()?;
        let result = self.workspace.device.score_resident(
            input,
            &self.workspace.shared,
            &self.uploaded,
            split,
            None,
        )?;
        Ok(result)
    }
    /// Runs `score_bucket_plans_cap1_basic` borrowing this chunk and the selected resident split.
    pub fn score_bucket_plans_cap1_basic(
        &self,
        split: usize,
        expiry_ms: i64,
        direction_code: i32,
        payout_basis: i64,
    ) -> Result<Measured<Vec<i64>>, String> {
        let split_mask = *self.workspace.split_masks.get(split).ok_or_else(|| {
            format!("score_bucket_plans_cap1_basic: split index {split} outside split_masks")
        })?;
        let input = Request {
            kind: 2,
            buffers: self.workspace.buffers,
            split_mask,
            candidates: self.candidates,
            sparse: None,
            expiry_ms,
            direction_code,
            payout_basis,
        };
        input.validate()?;
        let result = self.workspace.device.score_resident(
            input,
            &self.workspace.shared,
            &self.uploaded,
            split,
            None,
        )?;
        Ok(Measured {
            output: result.output.buy_output,
            timings: result.timings,
        })
    }
    /// Runs `score_bucket_plans_cap1_basic_dual` borrowing this chunk and the selected resident split.
    pub fn score_bucket_plans_cap1_basic_dual(
        &self,
        split: usize,
        expiry_ms: i64,
        payout_basis: i64,
    ) -> Result<Measured<DualScores>, String> {
        let split_mask = *self.workspace.split_masks.get(split).ok_or_else(|| {
            format!("score_bucket_plans_cap1_basic_dual: split index {split} outside split_masks")
        })?;
        let input = Request {
            kind: 3,
            buffers: self.workspace.buffers,
            split_mask,
            candidates: self.candidates,
            sparse: None,
            expiry_ms,
            direction_code: 1,
            payout_basis,
        };
        input.validate()?;
        let result = self.workspace.device.score_resident(
            input,
            &self.workspace.shared,
            &self.uploaded,
            split,
            None,
        )?;
        Ok(result)
    }
    /// Runs `score_bucket_plans_cap1_sparse` borrowing this chunk and the selected resident split.
    pub fn score_bucket_plans_cap1_sparse(
        &self,
        split: usize,
        expiry_ms: i64,
        direction_code: i32,
        payout_basis: i64,
    ) -> Result<Measured<Vec<i64>>, String> {
        let split_mask = *self.workspace.split_masks.get(split).ok_or_else(|| {
            format!("score_bucket_plans_cap1_sparse: split index {split} outside split_masks")
        })?;
        let input = Request {
            kind: 4,
            buffers: self.workspace.buffers,
            split_mask,
            candidates: self.candidates,
            sparse: Some(self.sparse.ok_or_else(|| {
                format!(
                    "{}: resident chunk has no sparse index",
                    crate::KERNEL_SOURCES[4].0
                )
            })?),
            expiry_ms,
            direction_code,
            payout_basis,
        };
        input.validate()?;
        let result = self.workspace.device.score_resident(
            input,
            &self.workspace.shared,
            &self.uploaded,
            split,
            None,
        )?;
        Ok(Measured {
            output: result.output.buy_output,
            timings: result.timings,
        })
    }
    /// Runs `score_bucket_plans_cap1_basic_sparse` borrowing this chunk and the selected resident split.
    pub fn score_bucket_plans_cap1_basic_sparse(
        &self,
        split: usize,
        expiry_ms: i64,
        direction_code: i32,
        payout_basis: i64,
    ) -> Result<Measured<Vec<i64>>, String> {
        let split_mask = *self.workspace.split_masks.get(split).ok_or_else(|| {
            format!("score_bucket_plans_cap1_basic_sparse: split index {split} outside split_masks")
        })?;
        let input = Request {
            kind: 5,
            buffers: self.workspace.buffers,
            split_mask,
            candidates: self.candidates,
            sparse: Some(self.sparse.ok_or_else(|| {
                format!(
                    "{}: resident chunk has no sparse index",
                    crate::KERNEL_SOURCES[5].0
                )
            })?),
            expiry_ms,
            direction_code,
            payout_basis,
        };
        input.validate()?;
        let result = self.workspace.device.score_resident(
            input,
            &self.workspace.shared,
            &self.uploaded,
            split,
            None,
        )?;
        Ok(Measured {
            output: result.output.buy_output,
            timings: result.timings,
        })
    }
    /// Runs `score_bucket_plans_cap1_sparse_dual` borrowing this chunk and the selected resident split.
    pub fn score_bucket_plans_cap1_sparse_dual(
        &self,
        split: usize,
        expiry_ms: i64,
        payout_basis: i64,
    ) -> Result<Measured<DualScores>, String> {
        let split_mask = *self.workspace.split_masks.get(split).ok_or_else(|| {
            format!("score_bucket_plans_cap1_sparse_dual: split index {split} outside split_masks")
        })?;
        let input = Request {
            kind: 6,
            buffers: self.workspace.buffers,
            split_mask,
            candidates: self.candidates,
            sparse: Some(self.sparse.ok_or_else(|| {
                format!(
                    "{}: resident chunk has no sparse index",
                    crate::KERNEL_SOURCES[6].0
                )
            })?),
            expiry_ms,
            direction_code: 1,
            payout_basis,
        };
        input.validate()?;
        let result = self.workspace.device.score_resident(
            input,
            &self.workspace.shared,
            &self.uploaded,
            split,
            None,
        )?;
        Ok(result)
    }
    /// Runs `score_bucket_plans_cap1_basic_sparse_dual` borrowing this chunk and the selected resident split.
    pub fn score_bucket_plans_cap1_basic_sparse_dual(
        &self,
        split: usize,
        expiry_ms: i64,
        payout_basis: i64,
    ) -> Result<Measured<DualScores>, String> {
        let split_mask = *self.workspace.split_masks.get(split).ok_or_else(|| {
            format!(
                "score_bucket_plans_cap1_basic_sparse_dual: split index {split} outside split_masks"
            )
        })?;
        let input = Request {
            kind: 7,
            buffers: self.workspace.buffers,
            split_mask,
            candidates: self.candidates,
            sparse: Some(self.sparse.ok_or_else(|| {
                format!(
                    "{}: resident chunk has no sparse index",
                    crate::KERNEL_SOURCES[7].0
                )
            })?),
            expiry_ms,
            direction_code: 1,
            payout_basis,
        };
        input.validate()?;
        let result = self.workspace.device.score_resident(
            input,
            &self.workspace.shared,
            &self.uploaded,
            split,
            None,
        )?;
        Ok(result)
    }
    /// Runs `reconstruct_signal_masks_cap1` borrowing this chunk and the selected resident split.
    pub fn reconstruct_signal_masks_cap1(
        &self,
        split: usize,
        expiry_ms: i64,
    ) -> Result<Measured<Vec<u8>>, String> {
        let split_mask = *self.workspace.split_masks.get(split).ok_or_else(|| {
            format!("reconstruct_signal_masks_cap1: split index {split} outside split_masks")
        })?;
        let input = Request {
            kind: 8,
            buffers: self.workspace.buffers,
            split_mask,
            candidates: self.candidates,
            sparse: None,
            expiry_ms,
            direction_code: 1,
            payout_basis: 0,
        };
        input.validate()?;
        let result = self.workspace.device.reconstruct_resident(
            input,
            &self.workspace.shared,
            &self.uploaded,
            split,
        )?;
        Ok(result)
    }
}
