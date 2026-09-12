//! Extraction parity: literal CuPy inputs always; immutable device capture and the
//! connected development replay only through the two explicitly ignored tests.

mod common;

use binary_alpha_accelerator::{
    Backend, KERNEL_SOURCES, Measured, Timings, bootstrap, portfolio, repair, search,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const LEGACY_COMMIT: &str = "b509964cd1c40180e9d98b0e55a95699b0abe9ed";

/// Recorded CUDA payload digests from the schema-1 capture at LEGACY_COMMIT.
/// Schema 2 retains file provenance but omits the duplicate kernel ABI metadata.
#[cfg(feature = "cuda")]
const LEGACY_KERNEL_DIGESTS: &[(&str, &str)] = &[
    (
        "score_bucket_plans_cap1",
        "b7fbd0a8559fcef341c90a1c14f61a96b4bb817962cea78171d2ed5d45148da8",
    ),
    (
        "bootstrap_path_metrics",
        "08787ee13438ea5615b1716db1c2f300fbcd558de700f3e050d145164cf2b4a3",
    ),
    (
        "replay_capacity",
        "575e16c4c2a8a1b5a03aacbf0d5f8bc78aad2521118380e0deae924466a5a836",
    ),
    (
        "path_drawdown",
        "790b0e99a2bed348fdbb7c2c67bea71c7be26a1548b75232ad9b7340706b6a69",
    ),
    (
        "replay_policies",
        "a11103708b3d22a4a37a30bed51abfa3e418477981e7dc603fa8ff195f5f48d0",
    ),
    (
        "score_bucket_plans_cap1_dual",
        "bd561563b1cf518afca112caa9a54f2dfa13e421fd2378c87726fd2629bd8b00",
    ),
    (
        "score_bucket_plans_cap1_basic",
        "7f539e03452794c0244c2bbe7a82223fcbcec9f7d1523e760cb8ca9365285038",
    ),
    (
        "score_bucket_plans_cap1_basic_dual",
        "d218f1ecc7419537398beab1fea189a7f862fcab92ec155e94726d1ad514456d",
    ),
    (
        "score_bucket_plans_cap1_sparse",
        "ee4353f52d96e81e0cd95c6e7c38cf9e91937448410bab8a42d6cc8901b3be3a",
    ),
    (
        "score_bucket_plans_cap1_basic_sparse",
        "9c1123fbf02d847bfaa1d09b12ca0a973e571c19a01ddbb22bf3df1e4e47d0cc",
    ),
    (
        "score_bucket_plans_cap1_sparse_dual",
        "18c3fcc4a5d59bb3a4c8ce4dea9fb660f06295e66697bdf11a6fa6e99f7523d0",
    ),
    (
        "score_bucket_plans_cap1_basic_sparse_dual",
        "ac4d67f8d84c104bb24e8f96673de1782bf0e4b6a712078b7ee75a5ee982793b",
    ),
    (
        "reconstruct_signal_masks_cap1",
        "738ff7b107e2d375af589626f6613fb9ae1a0d8af09f3187c7dc91418c6d167d",
    ),
];

/// The only comparison policy. Raw outputs never use a numerical tolerance.
/// Decimal places mean unittest's round(actual - expected, places) == 0.
const COMPARISONS: &[(&str, &str, &str)] = &[
    (
        "all integer fields, shapes, masks, membership",
        "exact",
        "exact",
    ),
    (
        "all raw device floats; search slot 10",
        "stored IEEE bits",
        "stored IEEE bits",
    ),
    (
        "search counts, loss streak, drawdown trades",
        "exact",
        "exact",
    ),
    (
        "search net, max drawdown units/hours, underwater hours",
        "places=10",
        "stored IEEE bits",
    ),
    (
        "search ulcer, profit factor, Sharpe, Sortino, rolling minima",
        "places=9; absent exact",
        "stored IEEE bits",
    ),
    (
        "bootstrap aggregate and hand calculations",
        "exact",
        "stored IEEE bits",
    ),
    ("repair counts, net, max drawdown", "exact", "exact"),
    (
        "repair longest drawdown hours",
        "places=7",
        "stored IEEE bits",
    ),
    ("portfolio capacity membership", "exact", "exact"),
    (
        "portfolio host static objective (outside kernel scope)",
        "rtol=1e-4, atol=1e-5",
        "not a kernel output",
    ),
    (
        "portfolio host static trade rates (outside kernel scope)",
        "rtol=1e-5, atol=1e-5",
        "not a kernel output",
    ),
    (
        "portfolio host path objective/rate (outside kernel scope)",
        "places=4/5",
        "not a kernel output",
    ),
];

/// Owned ABI arrays retain their original shape and exact floating-point bits.
#[derive(Clone, Debug)]
struct Buffer {
    shape: Vec<usize>,
    data: Data,
}

macro_rules! buffers {
    ($(($variant:ident, $ty:ty, $dtype:literal, $get:ident)),+ $(,)?) => {
        #[derive(Clone, Debug)]
        enum Data { $($variant(Vec<$ty>)),+ }
        impl Buffer {
            fn dtype(&self) -> &'static str { match &self.data { $(Data::$variant(_) => $dtype),+ } }
            fn bytes(&self) -> Vec<u8> { match &self.data { $(Data::$variant(v) => v.iter().flat_map(|v| v.to_le_bytes()).collect()),+ } }
            fn len(&self) -> usize { match &self.data { $(Data::$variant(v) => v.len()),+ } }
            $(fn $get(&self) -> &[$ty] { match &self.data { Data::$variant(v) => v, _ => panic!("expected {}, got {}", $dtype, self.dtype()) } })+
            #[cfg(feature = "cuda")]
            fn from_bytes(dtype: &str, shape: Vec<usize>, bytes: &[u8]) -> Self {
                let data = match dtype {
                    $($dtype => { let (chunks, tail) = bytes.as_chunks::<{std::mem::size_of::<$ty>()}>(); assert!(tail.is_empty()); Data::$variant(chunks.iter().map(|c| <$ty>::from_le_bytes(*c)).collect()) }),+,
                    _ => panic!("unknown dtype {dtype}")
                };
                let b = Self { shape, data }; b.validate(); b
            }
        }
        $(impl From<Vec<$ty>> for Buffer {
            fn from(v: Vec<$ty>) -> Self { Self { shape: vec![v.len()], data: Data::$variant(v) } }
        })+
    }
}
buffers!(
    (I8, i8, "int8", i8s),
    (U8, u8, "uint8", u8s),
    (I16, i16, "int16", i16s),
    (I32, i32, "int32", i32s),
    (I64, i64, "int64", i64s),
    (U64, u64, "uint64", u64s),
    (F32, f32, "float32", f32s),
    (F64, f64, "float64", f64s)
);

impl Buffer {
    fn validate(&self) {
        assert_eq!(
            self.len(),
            self.shape
                .iter()
                .try_fold(1_usize, |n, &d| n.checked_mul(d))
                .expect("buffer shape overflow"),
            "{} {:?}",
            self.dtype(),
            self.shape
        );
    }
    fn fixture(v: &Value) -> Self {
        let shape = v["shape"]
            .as_array()
            .expect("typed buffer shape")
            .iter()
            .map(|v| usize::try_from(v.as_u64().unwrap()).unwrap())
            .collect();
        let dtype = v["dtype"].as_str().unwrap();
        let values = v["values"].as_array().expect("flat compact values");
        let bits = if matches!(dtype, "float32" | "float64") {
            let bits = v["bits"].as_array().expect("floating bits");
            assert_eq!(values.len(), bits.len());
            bits
        } else {
            assert!(v.get("bits").is_none());
            values
        };
        macro_rules! signed {
            ($variant:ident,$ty:ty) => {
                Data::$variant(
                    values
                        .iter()
                        .map(|v| <$ty>::try_from(v.as_i64().unwrap()).unwrap())
                        .collect(),
                )
            };
        }
        let data = match dtype {
            "int8" => signed!(I8, i8),
            "uint8" => Data::U8(
                values
                    .iter()
                    .map(|v| u8::try_from(v.as_u64().unwrap()).unwrap())
                    .collect(),
            ),
            "int16" => signed!(I16, i16),
            "int32" => signed!(I32, i32),
            "int64" => signed!(I64, i64),
            "uint64" => Data::U64(values.iter().map(|v| v.as_u64().unwrap()).collect()),
            "float64" => Data::F64(
                bits.iter()
                    .map(|v| f64::from_bits(v.as_str().unwrap().parse().unwrap()))
                    .collect(),
            ),
            "float32" => Data::F32(
                bits.iter()
                    .map(|v| f32::from_bits(v.as_str().unwrap().parse().unwrap()))
                    .collect(),
            ),
            _ => panic!("unknown fixture dtype {dtype}"),
        };
        let b = Self { shape, data };
        b.validate();
        b
    }
    #[cfg(feature = "cuda")]
    fn shaped(mut self, shape: Vec<usize>) -> Self {
        self.shape = shape;
        self.validate();
        self
    }
}
type Buffers = BTreeMap<String, Buffer>;

/// One launch's complete inputs and outputs, independent of provenance and timing.
#[derive(Clone)]
struct Case {
    id: String,
    symbol: String,
    inputs: Buffers,
    outputs: Buffers,
}

fn fixture() -> Value {
    common::manifest_json(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/phase07_legacy_cases.json"),
    )
}

/// Compact scalar arguments are JSON integers; their widths belong to the ABI.
fn fixture_argument(name: &str, value: &Value) -> Buffer {
    if value.is_object() {
        return Buffer::fixture(value);
    }
    let value = value.as_i64().expect("integer ABI scalar");
    let data = match name {
        "expiry_ms" | "payout_basis" | "start_ms" | "end_ms" | "payout_fp" => {
            Data::I64(vec![value])
        }
        "candidate_count" | "row_count" | "feature_count" | "direction_code" | "simulations"
        | "trade_count" | "rolling_horizon" | "word_count" | "policy_count" | "event_count"
        | "portfolio_count" | "max_total" | "max_expiry" | "observation_count" => {
            Data::I32(vec![i32::try_from(value).unwrap()])
        }
        _ => panic!("unknown ABI scalar {name}"),
    };
    Buffer {
        shape: vec![],
        data,
    }
}

fn fixture_buffers(value: &Value) -> Buffers {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(name, value)| (name.clone(), fixture_argument(name, value)))
        .collect()
}

/// Bind shared schema-2 inputs to each original launch in its recorded order.
fn literal_cases(f: &Value) -> Vec<Case> {
    assert_eq!(f["schema_version"], 2);
    assert_eq!(f["source_commit"], LEGACY_COMMIT);
    assert_eq!(f["blocked"].as_array().unwrap().len(), 0);
    let mut cases = Vec::new();
    let mut push = |c: &Value, index: usize, launch: &Value, inputs: Buffers| {
        cases.push(Case {
            id: format!("{}-{index}", c["case_id"].as_str().unwrap()),
            symbol: launch["kernel"].as_str().unwrap().into(),
            inputs,
            outputs: fixture_buffers(&launch["raw_output"]),
        });
    };
    for c in f["search"].as_array().unwrap() {
        for (i, launch) in c["launches"].as_array().unwrap().iter().enumerate() {
            let symbol = launch["kernel"].as_str().unwrap();
            let sparse = symbol.contains("sparse");
            let reconstruct = symbol == "reconstruct_signal_masks_cap1";
            let mut inputs: Buffers = c["inputs"]
                .as_object()
                .unwrap()
                .iter()
                .filter(|(name, _)| match name.as_str() {
                    "ordered_rows" => !sparse,
                    "candidate_driver_key" | "key_chrono_offsets" | "key_chrono_rows" => sparse,
                    "settlement_time_ms" => !reconstruct && !symbol.contains("basic"),
                    "valid" | "buy_win" | "sell_win" | "tie" | "payout_basis" => !reconstruct,
                    _ => true,
                })
                .map(|(name, value)| {
                    let value = if name == "split_mask" {
                        &value[launch["split"].as_str().unwrap()]
                    } else {
                        value
                    };
                    (name.clone(), fixture_argument(name, value))
                })
                .collect();
            if let Some(direction) = launch.get("direction_code") {
                inputs.insert(
                    "direction_code".into(),
                    fixture_argument("direction_code", direction),
                );
            }
            // As before, feature_count also supports validation by the resident safe API.
            push(c, i, launch, inputs);
        }
    }
    for c in f["bootstrap"].as_array().unwrap() {
        let inputs = ["paths", "simulations", "trade_count", "rolling_horizon"]
            .into_iter()
            .map(|name| (name.into(), fixture_argument(name, &c[name])))
            .collect();
        push(c, 0, c, inputs);
    }
    for c in f["repair"].as_array().unwrap() {
        for (i, launch) in c["policy_runs"].as_array().unwrap().iter().enumerate() {
            let mut inputs = fixture_buffers(&c["inputs"]);
            for name in ["policy_candidate", "policy_masks", "policy_count"] {
                inputs.insert(name.into(), fixture_argument(name, &launch[name]));
            }
            // The schema records logical word matrices; the original ABI used flat buffers.
            for name in ["failure_words", "policy_masks"] {
                let buffer = inputs.get_mut(name).unwrap();
                buffer.shape = vec![buffer.len()];
            }
            push(c, i, launch, inputs);
        }
    }
    for c in f["portfolio"].as_array().unwrap() {
        let mut launches: Vec<_> = ["path_drawdown", "capacity"]
            .into_iter()
            .flat_map(|key| c[key].as_array().unwrap())
            .collect();
        launches.sort_by_key(|launch| launch["launch"].as_u64().unwrap());
        for (i, launch) in launches.into_iter().enumerate() {
            assert_eq!(launch["launch"].as_u64().unwrap(), i as u64);
            let mut inputs = fixture_buffers(&launch["inputs"]);
            if launch["kernel"] == "replay_capacity" {
                inputs.insert("masks".into(), Buffer::fixture(&c["masks"]));
            }
            push(c, i, launch, inputs);
        }
    }
    assert_eq!(
        ["search", "bootstrap", "repair", "portfolio"]
            .into_iter()
            .map(|group| f[group].as_array().unwrap().len())
            .sum::<usize>(),
        12
    );
    assert_eq!(cases.len(), 34);
    assert_eq!(
        cases
            .iter()
            .map(|c| c.symbol.as_str())
            .collect::<BTreeSet<_>>(),
        KERNEL_SOURCES.iter().map(|x| x.0).collect()
    );
    cases
}

/// Report the first differing byte, keeping large governed failures readable.
fn compare(context: &str, actual: &Buffers, expected: &Buffers) {
    assert_eq!(
        actual.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>(),
        "{context}"
    );
    for (name, a) in actual {
        let b = &expected[name];
        assert_eq!(
            (a.dtype(), &a.shape),
            (b.dtype(), &b.shape),
            "{context}/{name}"
        );
        let (a, b) = (a.bytes(), b.bytes());
        assert_eq!(a.len(), b.len(), "{context}/{name}");
        if let Some(index) = a.iter().zip(&b).position(|(a, b)| a != b) {
            panic!(
                "{context}/{name}: byte {index}: {} != {} (unclassified raw difference)",
                a[index], b[index]
            );
        }
    }
}

/// Apply recorded output shapes after the API returns its flat owned allocations.
struct KernelRun {
    output: Buffers,
    timings: Timings,
    #[cfg(feature = "cuda")]
    decode: std::time::Duration,
}

fn outputs<T>(case: &Case, measured: Measured<T>, decode: impl FnOnce(T) -> Buffers) -> KernelRun {
    #[cfg(feature = "cuda")]
    let started = std::time::Instant::now();
    let mut output = decode(measured.output);
    for (name, b) in &mut output {
        b.shape = case.outputs[name].shape.clone();
        b.validate();
    }
    KernelRun {
        output,
        timings: measured.timings,
        #[cfg(feature = "cuda")]
        decode: started.elapsed(),
    }
}

/// Dispatch exactly the public operation ABI; CPU and device share these inputs.
fn run(backend: &Backend, case: &Case) -> KernelRun {
    let b = &case.inputs;
    match case.symbol.as_str() {
        "score_bucket_plans_cap1" => outputs(
            case,
            search::score_bucket_plans_cap1(
                backend,
                b["feature_codes"].i16s(),
                b["feature1"].i32s(),
                b["bucket1"].i16s(),
                b["feature2"].i32s(),
                b["bucket2"].i16s(),
                b["feature3"].i32s(),
                b["bucket3"].i16s(),
                b["feature4"].i32s(),
                b["bucket4"].i16s(),
                b["split_mask"].u8s(),
                b["ordered_rows"].i64s(),
                b["decision_time_ms"].i64s(),
                b["release_time_ms"].i64s(),
                b["settlement_time_ms"].i64s(),
                b["valid"].u8s(),
                b["buy_win"].u8s(),
                b["sell_win"].u8s(),
                b["tie"].u8s(),
                b["candidate_count"].i32s()[0],
                b["row_count"].i32s()[0],
                b["feature_count"].i32s()[0],
                b["expiry_ms"].i64s()[0],
                b["direction_code"].i32s()[0],
                b["payout_basis"].i64s()[0],
            )
            .unwrap(),
            |v| BTreeMap::from([("output".into(), Buffer::from(v))]),
        ),
        "score_bucket_plans_cap1_dual" => outputs(
            case,
            search::score_bucket_plans_cap1_dual(
                backend,
                b["feature_codes"].i16s(),
                b["feature1"].i32s(),
                b["bucket1"].i16s(),
                b["feature2"].i32s(),
                b["bucket2"].i16s(),
                b["feature3"].i32s(),
                b["bucket3"].i16s(),
                b["feature4"].i32s(),
                b["bucket4"].i16s(),
                b["split_mask"].u8s(),
                b["ordered_rows"].i64s(),
                b["decision_time_ms"].i64s(),
                b["release_time_ms"].i64s(),
                b["settlement_time_ms"].i64s(),
                b["valid"].u8s(),
                b["buy_win"].u8s(),
                b["sell_win"].u8s(),
                b["tie"].u8s(),
                b["candidate_count"].i32s()[0],
                b["row_count"].i32s()[0],
                b["feature_count"].i32s()[0],
                b["expiry_ms"].i64s()[0],
                b["payout_basis"].i64s()[0],
            )
            .unwrap(),
            |v| {
                BTreeMap::from([
                    ("buy_output".into(), Buffer::from(v.buy_output)),
                    ("sell_output".into(), Buffer::from(v.sell_output)),
                ])
            },
        ),
        "score_bucket_plans_cap1_basic" => outputs(
            case,
            search::score_bucket_plans_cap1_basic(
                backend,
                b["feature_codes"].i16s(),
                b["feature1"].i32s(),
                b["bucket1"].i16s(),
                b["feature2"].i32s(),
                b["bucket2"].i16s(),
                b["feature3"].i32s(),
                b["bucket3"].i16s(),
                b["feature4"].i32s(),
                b["bucket4"].i16s(),
                b["split_mask"].u8s(),
                b["ordered_rows"].i64s(),
                b["decision_time_ms"].i64s(),
                b["release_time_ms"].i64s(),
                b["valid"].u8s(),
                b["buy_win"].u8s(),
                b["sell_win"].u8s(),
                b["tie"].u8s(),
                b["candidate_count"].i32s()[0],
                b["row_count"].i32s()[0],
                b["expiry_ms"].i64s()[0],
                b["direction_code"].i32s()[0],
                b["payout_basis"].i64s()[0],
            )
            .unwrap(),
            |v| BTreeMap::from([("output".into(), Buffer::from(v))]),
        ),
        "score_bucket_plans_cap1_basic_dual" => outputs(
            case,
            search::score_bucket_plans_cap1_basic_dual(
                backend,
                b["feature_codes"].i16s(),
                b["feature1"].i32s(),
                b["bucket1"].i16s(),
                b["feature2"].i32s(),
                b["bucket2"].i16s(),
                b["feature3"].i32s(),
                b["bucket3"].i16s(),
                b["feature4"].i32s(),
                b["bucket4"].i16s(),
                b["split_mask"].u8s(),
                b["ordered_rows"].i64s(),
                b["decision_time_ms"].i64s(),
                b["release_time_ms"].i64s(),
                b["valid"].u8s(),
                b["buy_win"].u8s(),
                b["sell_win"].u8s(),
                b["tie"].u8s(),
                b["candidate_count"].i32s()[0],
                b["row_count"].i32s()[0],
                b["expiry_ms"].i64s()[0],
                b["payout_basis"].i64s()[0],
            )
            .unwrap(),
            |v| {
                BTreeMap::from([
                    ("buy_output".into(), Buffer::from(v.buy_output)),
                    ("sell_output".into(), Buffer::from(v.sell_output)),
                ])
            },
        ),
        "score_bucket_plans_cap1_sparse" => outputs(
            case,
            search::score_bucket_plans_cap1_sparse(
                backend,
                b["feature_codes"].i16s(),
                b["feature1"].i32s(),
                b["bucket1"].i16s(),
                b["feature2"].i32s(),
                b["bucket2"].i16s(),
                b["feature3"].i32s(),
                b["bucket3"].i16s(),
                b["feature4"].i32s(),
                b["bucket4"].i16s(),
                b["candidate_driver_key"].i32s(),
                b["key_chrono_offsets"].i32s(),
                b["key_chrono_rows"].i32s(),
                b["split_mask"].u8s(),
                b["decision_time_ms"].i64s(),
                b["release_time_ms"].i64s(),
                b["settlement_time_ms"].i64s(),
                b["valid"].u8s(),
                b["buy_win"].u8s(),
                b["sell_win"].u8s(),
                b["tie"].u8s(),
                b["candidate_count"].i32s()[0],
                b["row_count"].i32s()[0],
                b["expiry_ms"].i64s()[0],
                b["direction_code"].i32s()[0],
                b["payout_basis"].i64s()[0],
            )
            .unwrap(),
            |v| BTreeMap::from([("output".into(), Buffer::from(v))]),
        ),
        "score_bucket_plans_cap1_basic_sparse" => outputs(
            case,
            search::score_bucket_plans_cap1_basic_sparse(
                backend,
                b["feature_codes"].i16s(),
                b["feature1"].i32s(),
                b["bucket1"].i16s(),
                b["feature2"].i32s(),
                b["bucket2"].i16s(),
                b["feature3"].i32s(),
                b["bucket3"].i16s(),
                b["feature4"].i32s(),
                b["bucket4"].i16s(),
                b["candidate_driver_key"].i32s(),
                b["key_chrono_offsets"].i32s(),
                b["key_chrono_rows"].i32s(),
                b["split_mask"].u8s(),
                b["decision_time_ms"].i64s(),
                b["release_time_ms"].i64s(),
                b["valid"].u8s(),
                b["buy_win"].u8s(),
                b["sell_win"].u8s(),
                b["tie"].u8s(),
                b["candidate_count"].i32s()[0],
                b["row_count"].i32s()[0],
                b["expiry_ms"].i64s()[0],
                b["direction_code"].i32s()[0],
                b["payout_basis"].i64s()[0],
            )
            .unwrap(),
            |v| BTreeMap::from([("output".into(), Buffer::from(v))]),
        ),
        "score_bucket_plans_cap1_sparse_dual" => outputs(
            case,
            search::score_bucket_plans_cap1_sparse_dual(
                backend,
                b["feature_codes"].i16s(),
                b["feature1"].i32s(),
                b["bucket1"].i16s(),
                b["feature2"].i32s(),
                b["bucket2"].i16s(),
                b["feature3"].i32s(),
                b["bucket3"].i16s(),
                b["feature4"].i32s(),
                b["bucket4"].i16s(),
                b["candidate_driver_key"].i32s(),
                b["key_chrono_offsets"].i32s(),
                b["key_chrono_rows"].i32s(),
                b["split_mask"].u8s(),
                b["decision_time_ms"].i64s(),
                b["release_time_ms"].i64s(),
                b["settlement_time_ms"].i64s(),
                b["valid"].u8s(),
                b["buy_win"].u8s(),
                b["sell_win"].u8s(),
                b["tie"].u8s(),
                b["candidate_count"].i32s()[0],
                b["row_count"].i32s()[0],
                b["expiry_ms"].i64s()[0],
                b["payout_basis"].i64s()[0],
            )
            .unwrap(),
            |v| {
                BTreeMap::from([
                    ("buy_output".into(), Buffer::from(v.buy_output)),
                    ("sell_output".into(), Buffer::from(v.sell_output)),
                ])
            },
        ),
        "score_bucket_plans_cap1_basic_sparse_dual" => outputs(
            case,
            search::score_bucket_plans_cap1_basic_sparse_dual(
                backend,
                b["feature_codes"].i16s(),
                b["feature1"].i32s(),
                b["bucket1"].i16s(),
                b["feature2"].i32s(),
                b["bucket2"].i16s(),
                b["feature3"].i32s(),
                b["bucket3"].i16s(),
                b["feature4"].i32s(),
                b["bucket4"].i16s(),
                b["candidate_driver_key"].i32s(),
                b["key_chrono_offsets"].i32s(),
                b["key_chrono_rows"].i32s(),
                b["split_mask"].u8s(),
                b["decision_time_ms"].i64s(),
                b["release_time_ms"].i64s(),
                b["valid"].u8s(),
                b["buy_win"].u8s(),
                b["sell_win"].u8s(),
                b["tie"].u8s(),
                b["candidate_count"].i32s()[0],
                b["row_count"].i32s()[0],
                b["expiry_ms"].i64s()[0],
                b["payout_basis"].i64s()[0],
            )
            .unwrap(),
            |v| {
                BTreeMap::from([
                    ("buy_output".into(), Buffer::from(v.buy_output)),
                    ("sell_output".into(), Buffer::from(v.sell_output)),
                ])
            },
        ),
        "reconstruct_signal_masks_cap1" => outputs(
            case,
            search::reconstruct_signal_masks_cap1(
                backend,
                b["feature_codes"].i16s(),
                b["feature1"].i32s(),
                b["bucket1"].i16s(),
                b["feature2"].i32s(),
                b["bucket2"].i16s(),
                b["feature3"].i32s(),
                b["bucket3"].i16s(),
                b["feature4"].i32s(),
                b["bucket4"].i16s(),
                b["split_mask"].u8s(),
                b["ordered_rows"].i64s(),
                b["decision_time_ms"].i64s(),
                b["release_time_ms"].i64s(),
                b["candidate_count"].i32s()[0],
                b["row_count"].i32s()[0],
                b["expiry_ms"].i64s()[0],
            )
            .unwrap(),
            |v| BTreeMap::from([("output".into(), Buffer::from(v))]),
        ),
        "bootstrap_path_metrics" => outputs(
            case,
            bootstrap::bootstrap_path_metrics(
                backend,
                b["paths"].f64s(),
                b["simulations"].i32s()[0],
                b["trade_count"].i32s()[0],
                b["rolling_horizon"].i32s()[0],
            )
            .unwrap(),
            |v| {
                BTreeMap::from([
                    ("max_drawdowns".into(), Buffer::from(v.max_drawdowns)),
                    (
                        "longest_underwater".into(),
                        Buffer::from(v.longest_underwater),
                    ),
                    ("negative_rolling".into(), Buffer::from(v.negative_rolling)),
                ])
            },
        ),
        "replay_policies" => outputs(
            case,
            repair::replay_policies(
                backend,
                b["candidate_offsets"].i64s(),
                b["entry_ms"].i64s(),
                b["settlement_ms"].i64s(),
                b["close_ms"].i64s(),
                b["outcomes"].i8s(),
                b["valid_entry"].u8s(),
                b["failure_words"].u64s(),
                b["word_count"].i32s()[0],
                b["policy_candidate"].i32s(),
                b["policy_masks"].u64s(),
                b["policy_count"].i32s()[0],
                b["start_ms"].i64s()[0],
                b["end_ms"].i64s()[0],
                b["payout_fp"].i64s()[0],
            )
            .unwrap(),
            |v| {
                BTreeMap::from([
                    ("out_opened".into(), Buffer::from(v.out_opened)),
                    ("out_settled".into(), Buffer::from(v.out_settled)),
                    ("out_wins".into(), Buffer::from(v.out_wins)),
                    ("out_losses".into(), Buffer::from(v.out_losses)),
                    ("out_ties".into(), Buffer::from(v.out_ties)),
                    ("out_net_fp".into(), Buffer::from(v.out_net_fp)),
                    ("out_max_dd_fp".into(), Buffer::from(v.out_max_dd_fp)),
                    (
                        "out_longest_dd_ms".into(),
                        Buffer::from(v.out_longest_dd_ms),
                    ),
                    (
                        "out_longest_dd_trades".into(),
                        Buffer::from(v.out_longest_dd_trades),
                    ),
                    (
                        "out_longest_loss_streak".into(),
                        Buffer::from(v.out_longest_loss_streak),
                    ),
                    (
                        "out_gross_profit_fp".into(),
                        Buffer::from(v.out_gross_profit_fp),
                    ),
                    (
                        "out_gross_loss_fp".into(),
                        Buffer::from(v.out_gross_loss_fp),
                    ),
                    (
                        "out_sum_returns_fp".into(),
                        Buffer::from(v.out_sum_returns_fp),
                    ),
                    (
                        "out_sum_squares_fp2".into(),
                        Buffer::from(v.out_sum_squares_fp2),
                    ),
                    (
                        "out_downside_squares_fp2".into(),
                        Buffer::from(v.out_downside_squares_fp2),
                    ),
                ])
            },
        ),
        "replay_capacity" => outputs(
            case,
            portfolio::replay_capacity(
                backend,
                b["masks"].u8s(),
                b["candidate_index"].i32s(),
                b["entry_time"].i64s(),
                b["due_time"].i64s(),
                b["expiry_seconds"].i32s(),
                b["hypothetical_valid"].u8s(),
                b["standalone_admitted"].u8s(),
                b["portfolio_count"].i32s()[0],
                b["candidate_count"].i32s()[0],
                b["event_count"].i32s()[0],
                b["max_total"].i32s()[0],
                b["max_expiry"].i32s()[0],
            )
            .unwrap(),
            |v| BTreeMap::from([("accepted".into(), Buffer::from(v))]),
        ),
        "path_drawdown" => outputs(
            case,
            portfolio::path_drawdown(
                backend,
                b["returns"].f32s(),
                b["observation_count"].i32s()[0],
                b["portfolio_count"].i32s()[0],
            )
            .unwrap(),
            |v| {
                BTreeMap::from([
                    ("max_drawdown".into(), Buffer::from(v.max_drawdown)),
                    ("ulcer_index".into(), Buffer::from(v.ulcer_index)),
                ])
            },
        ),
        other => panic!("unknown kernel {other}"),
    }
}

/// Launch on a resident workspace so all variants reuse the shared feature/outcome/mask buffers.
#[cfg(feature = "cuda")]
fn run_resident(
    workspace: &binary_alpha_accelerator::cuda::ResidentSearch<'_>,
    case: &Case,
) -> KernelRun {
    let b = &case.inputs;
    let candidates = slots(b);
    match case.symbol.as_str() {
        "score_bucket_plans_cap1" => outputs(
            case,
            workspace
                .score_bucket_plans_cap1(
                    candidates,
                    0,
                    b["expiry_ms"].i64s()[0],
                    b["direction_code"].i32s()[0],
                    b["payout_basis"].i64s()[0],
                )
                .unwrap(),
            |v| BTreeMap::from([("output".into(), Buffer::from(v))]),
        ),
        "score_bucket_plans_cap1_dual" => outputs(
            case,
            workspace
                .score_bucket_plans_cap1_dual(
                    candidates,
                    0,
                    b["expiry_ms"].i64s()[0],
                    b["payout_basis"].i64s()[0],
                )
                .unwrap(),
            |v| {
                BTreeMap::from([
                    ("buy_output".into(), Buffer::from(v.buy_output)),
                    ("sell_output".into(), Buffer::from(v.sell_output)),
                ])
            },
        ),
        "score_bucket_plans_cap1_basic" => outputs(
            case,
            workspace
                .score_bucket_plans_cap1_basic(
                    candidates,
                    0,
                    b["expiry_ms"].i64s()[0],
                    b["direction_code"].i32s()[0],
                    b["payout_basis"].i64s()[0],
                )
                .unwrap(),
            |v| BTreeMap::from([("output".into(), Buffer::from(v))]),
        ),
        "score_bucket_plans_cap1_basic_dual" => outputs(
            case,
            workspace
                .score_bucket_plans_cap1_basic_dual(
                    candidates,
                    0,
                    b["expiry_ms"].i64s()[0],
                    b["payout_basis"].i64s()[0],
                )
                .unwrap(),
            |v| {
                BTreeMap::from([
                    ("buy_output".into(), Buffer::from(v.buy_output)),
                    ("sell_output".into(), Buffer::from(v.sell_output)),
                ])
            },
        ),
        "score_bucket_plans_cap1_sparse" => outputs(
            case,
            workspace
                .score_bucket_plans_cap1_sparse(
                    candidates,
                    0,
                    sparse(b),
                    b["expiry_ms"].i64s()[0],
                    b["direction_code"].i32s()[0],
                    b["payout_basis"].i64s()[0],
                )
                .unwrap(),
            |v| BTreeMap::from([("output".into(), Buffer::from(v))]),
        ),
        "score_bucket_plans_cap1_basic_sparse" => outputs(
            case,
            workspace
                .score_bucket_plans_cap1_basic_sparse(
                    candidates,
                    0,
                    sparse(b),
                    b["expiry_ms"].i64s()[0],
                    b["direction_code"].i32s()[0],
                    b["payout_basis"].i64s()[0],
                )
                .unwrap(),
            |v| BTreeMap::from([("output".into(), Buffer::from(v))]),
        ),
        "score_bucket_plans_cap1_sparse_dual" => outputs(
            case,
            workspace
                .score_bucket_plans_cap1_sparse_dual(
                    candidates,
                    0,
                    sparse(b),
                    b["expiry_ms"].i64s()[0],
                    b["payout_basis"].i64s()[0],
                )
                .unwrap(),
            |v| {
                BTreeMap::from([
                    ("buy_output".into(), Buffer::from(v.buy_output)),
                    ("sell_output".into(), Buffer::from(v.sell_output)),
                ])
            },
        ),
        "score_bucket_plans_cap1_basic_sparse_dual" => outputs(
            case,
            workspace
                .score_bucket_plans_cap1_basic_sparse_dual(
                    candidates,
                    0,
                    sparse(b),
                    b["expiry_ms"].i64s()[0],
                    b["payout_basis"].i64s()[0],
                )
                .unwrap(),
            |v| {
                BTreeMap::from([
                    ("buy_output".into(), Buffer::from(v.buy_output)),
                    ("sell_output".into(), Buffer::from(v.sell_output)),
                ])
            },
        ),
        "reconstruct_signal_masks_cap1" => outputs(
            case,
            workspace
                .reconstruct_signal_masks_cap1(candidates, 0, b["expiry_ms"].i64s()[0])
                .unwrap(),
            |v| BTreeMap::from([("output".into(), Buffer::from(v))]),
        ),
        other => panic!("not a search kernel: {other}"),
    }
}

/// The legacy scalar decoder, limited to fields the source tests actually assert.
fn search_metrics(row: &[i64], payout: i64) -> Value {
    let round = |v: f64| format!("{v:.10}").parse::<f64>().unwrap();
    let (wins, losses, n) = (
        row[1] as f64,
        row[2] as f64,
        (row[1] + row[2] + row[3]) as f64,
    );
    let payout = payout as f64 / 100.0;
    let net = wins * payout - losses;
    let mean = if n > 0.0 { net / n } else { 0.0 };
    let variance = if n > 1.0 {
        ((wins * payout * payout + losses - net * net / n) / (n - 1.0)).max(0.0)
    } else {
        0.0
    };
    let downside = if n > 1.0 {
        ((losses - losses * losses / n) / (n - 1.0)).max(0.0)
    } else {
        0.0
    };
    json!({"total_signals":row[0],"wins":row[1],"losses":row[2],"ties":row[3],"invalid":row[4],"valid_trades":row[1]+row[2]+row[3],
        "net_units":round(net),"max_drawdown_units":round(row[8] as f64/100.0),"max_consecutive_losses":row[9],
        "ulcer_index_units":round(if n>0.0 {(f64::from_bits(row[10] as u64)/n).sqrt()/100.0} else {0.0}),
        "max_drawdown_trades":row[12],"max_drawdown_hours":round(row[20] as f64/3_600_000.0),"longest_underwater_hours":round(row[19] as f64/3_600_000.0),
        "profit_factor":(losses>0.0).then(||round(wins*payout/losses)),
        "sharpe_per_trade":(variance>0.0).then(||round(mean/variance.sqrt())),
        "sortino_per_trade":(downside>0.0).then(||round(mean/downside.sqrt())),
        "worst_rolling_20_units":(n>=20.0).then(||round(row[13] as f64/100.0)),
        "worst_rolling_50_units":(n>=50.0).then(||round(row[14] as f64/100.0)),
        "worst_rolling_100_units":(n>=100.0).then(||round(row[15] as f64/100.0))})
}

/// Use the source unittest decimal-place comparison, never a relative tolerance.
fn decimal_equal(actual: &Value, expected: &Value, places: Option<u64>, context: &str) {
    if let Some(places) = places.filter(|_| !actual.is_null() && !expected.is_null()) {
        let delta = actual.as_f64().unwrap() - expected.as_f64().unwrap();
        let rounded = format!("{delta:.precision$}", precision = places as usize)
            .parse::<f64>()
            .unwrap();
        assert_eq!(
            rounded, 0.0,
            "{context}: {actual} != {expected}, places={places:?}"
        );
    } else {
        assert_eq!(actual, expected, "{context}");
    }
}

/// The original warm-up rule is chronological, not a row-number or clock shortcut.
fn split_mask(
    execution_time_ms: &[i64],
    ordered: &[i64],
    eligible: &[bool],
    start: i64,
    end: i64,
) -> Vec<u8> {
    let selected: Vec<bool> = execution_time_ms
        .iter()
        .zip(eligible)
        .map(|(&c, &e)| e && c >= start && c < end)
        .collect();
    let mut scope = vec![0; execution_time_ms.len()];
    if let Some(first) = ordered.iter().position(|&r| selected[r as usize]) {
        for &r in &ordered[..first] {
            if eligible[r as usize] {
                scope[r as usize] = 2;
            }
        }
        for (i, &v) in selected.iter().enumerate() {
            if v {
                scope[i] = 1;
            }
        }
    }
    scope
}

/// TS-D columns alone permit absent floats, represented by paired null values/bits.
fn expected_column(v: &Value) -> Vec<Value> {
    if v["dtype"] == "int64" {
        let buffer = Buffer::fixture(v);
        assert_eq!(buffer.shape, [256]);
        return buffer.i64s().iter().map(|value| json!(value)).collect();
    }
    assert_eq!(v["dtype"], "float64");
    assert_eq!(v["shape"], json!([256]));
    let values = v["values"].as_array().unwrap();
    let bits = v["bits"].as_array().unwrap();
    assert_eq!(values.len(), 256);
    assert_eq!(bits.len(), values.len());
    values
        .iter()
        .zip(bits)
        .map(|(value, bits)| {
            if bits.is_null() {
                assert!(value.is_null(), "absent expectation needs paired nulls");
                Value::Null
            } else {
                let expected = f64::from_bits(bits.as_str().unwrap().parse().unwrap());
                assert!(
                    expected.is_finite(),
                    "TS-D expectations are finite or absent"
                );
                assert!(value.is_number());
                json!(expected)
            }
        })
        .collect()
}

/// Check independent legacy expectations against newly computed results, not captured actuals.
fn independent_expectations(f: &Value, cases: &[Case], results: &[Buffers]) {
    let result = |id: &str| &results[cases.iter().position(|c| c.id == id).unwrap()];
    for c in f["search"].as_array().unwrap() {
        let id = c["case_id"].as_str().unwrap();
        if id == "TS-D" {
            for direction in ["BUY", "SELL"] {
                let expectations: Vec<_> = c["expectations"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|e| e["scope"]["direction"] == direction)
                    .collect();
                assert_eq!(expectations.len(), 18);
                for expectation in expectations {
                    let scope = &expectation["scope"];
                    assert_eq!(scope["candidate"], "all");
                    let launch = scope["launch"].as_u64().unwrap();
                    let raw = result(&format!("{id}-{launch}"));
                    let rows = raw[if direction == "BUY" {
                        "buy_output"
                    } else {
                        "sell_output"
                    }]
                    .i64s();
                    let field = expectation["field"].as_str().unwrap();
                    let expected = expected_column(&expectation["expected"]);
                    let places = match field {
                        "net_units" | "max_drawdown_units" | "max_drawdown_hours" => Some(10),
                        "ulcer_index_units"
                        | "profit_factor"
                        | "sharpe_per_trade"
                        | "sortino_per_trade"
                        | "worst_rolling_20_units"
                        | "worst_rolling_50_units"
                        | "worst_rolling_100_units" => Some(9),
                        _ => None,
                    };
                    // All-null columns record only assertIsNone, without a numerical rule.
                    let rule = if expected.iter().all(Value::is_null) {
                        json!("exact")
                    } else {
                        places.map_or(json!("exact"), |n| json!({"places": n}))
                    };
                    assert_eq!(expectation["rule"], rule, "comparison rules are frozen");
                    assert_eq!(rows.len(), expected.len() * 21);
                    for (i, expected) in expected.iter().enumerate() {
                        let actual = search_metrics(&rows[i * 21..(i + 1) * 21], 92);
                        decimal_equal(
                            &actual[field],
                            expected,
                            expectation["rule"]["places"].as_u64(),
                            &format!("{id}/{direction}/{i}/{field}"),
                        );
                    }
                }
            }
        } else if matches!(id, "TS-A" | "TS-B") {
            let actual = search_metrics(
                result(&format!("{id}-0"))["output"].i64s(),
                if id == "TS-A" { 92 } else { 100 },
            );
            for expectation in c["expectations"].as_array().unwrap() {
                let field = expectation["field"].as_str().unwrap();
                let rule = &expectation["rule"];
                assert!(rule == "exact" || rule["places"].is_u64());
                decimal_equal(
                    &actual[field],
                    &expectation["expected"],
                    rule["places"].as_u64(),
                    expectation["source"].as_str().unwrap(),
                );
            }
        }
    }
    // TS-C explicitly compares all eight variants, both single directions, and basic prefixes.
    let buy = result("TS-C-0")["output"].i64s();
    let sell = result("TS-C-1")["output"].i64s();
    for (i, outputs) in results
        .iter()
        .enumerate()
        .filter(|(i, _)| cases[*i].id.starts_with("TS-C-"))
    {
        let case = &cases[i];
        for (name, b) in outputs {
            let direction = if name == "buy_output" {
                1
            } else if name == "sell_output" {
                -1
            } else {
                case.inputs["direction_code"].i32s()[0]
            };
            let expected = if direction == 1 { buy } else { sell };
            assert_eq!(
                b.i64s(),
                &expected[..b.len()],
                "{} {name} eight-way equivalence",
                case.id
            );
        }
    }
    assert_eq!(&result("TS-E-0")["output"].i64s()[..5], &[6, 1, 1, 0, 4]);
    assert_eq!(result("TS-E-1")["output"].u8s(), &[1, 0, 0, 1, 0, 1]);
    assert_eq!(&result("TS-F-0")["output"].i64s()[..5], &[2, 1, 0, 0, 1]);
    assert_eq!(&result("TS-F-1")["output"].i64s()[..5], &[2, 1, 0, 0, 1]);
    assert_eq!(result("TS-F-2")["output"].u8s(), &[0, 0, 1]);
    assert_eq!(
        cases.iter().find(|c| c.id == "TS-F-0").unwrap().inputs["split_mask"].u8s(),
        &[2, 1, 1]
    );

    for (id, drawdown, underwater, negative) in [
        ("TB-extra-all-loss-0", 5.0, 5, 4),
        ("TB-extra-all-win-0", 0.0, 0, 0),
    ] {
        let r = result(id);
        assert!(r["max_drawdowns"].f64s().iter().all(|&v| v == drawdown));
        assert!(
            r["longest_underwater"]
                .i64s()
                .iter()
                .all(|&v| v == underwater)
        );
        assert!(r["negative_rolling"].i64s().iter().all(|&v| v == negative));
    }
    let r = result("TB-C-0");
    let oracle = &f["bootstrap"][0]["numpy_branch_output"];
    assert_eq!(
        r["max_drawdowns"].bytes(),
        Buffer::fixture(&oracle["max_drawdowns"]).bytes()
    );
    assert_eq!(
        r["longest_underwater"].i64s(),
        Buffer::fixture(&oracle["longest_underwater"]).i64s()
    );
    assert_eq!(
        r["negative_rolling"].i64s().iter().sum::<i64>(),
        oracle["negative_rolling"].as_i64().unwrap()
    );
    let quantile = |values: &[f64], q: f64| {
        let mut sorted = values.to_vec();
        sorted.sort_by(f64::total_cmp);
        let position = (sorted.len() - 1) as f64 * q;
        let lo = position.floor() as usize;
        let hi = position.ceil() as usize;
        format!(
            "{:.10}",
            sorted[lo] + (sorted[hi] - sorted[lo]) * (position - lo as f64)
        )
        .parse::<f64>()
        .unwrap()
    };
    let aggregate = &f["bootstrap"][0]["aggregate_results"]["numpy"];
    for (key, value) in [
        (
            "median_max_drawdown_units",
            quantile(r["max_drawdowns"].f64s(), 0.5),
        ),
        (
            "p95_max_drawdown_units",
            quantile(r["max_drawdowns"].f64s(), 0.95),
        ),
        (
            "p99_max_drawdown_units",
            quantile(r["max_drawdowns"].f64s(), 0.99),
        ),
        (
            "p95_longest_underwater_trades",
            quantile(
                &r["longest_underwater"]
                    .i64s()
                    .iter()
                    .map(|&v| v as f64)
                    .collect::<Vec<_>>(),
                0.95,
            ),
        ),
        (
            "probability_exceeding_drawdown_limit",
            r["max_drawdowns"]
                .f64s()
                .iter()
                .filter(|&&v| v > 2.0)
                .count() as f64
                / 32.0,
        ),
        (
            "probability_negative_rolling_horizon",
            r["negative_rolling"].i64s().iter().sum::<i64>() as f64 / 128.0,
        ),
    ] {
        assert_eq!(value, aggregate[key].as_f64().unwrap(), "TB-C {key}");
    }

    for (i, name) in [(0, "cpu_baseline"), (1, "cpu_repaired")] {
        let r = result(&format!("TR-A-{i}"));
        let oracle = &f["repair"][0]["cpu_reference"][i]["report"];
        for key in ["opened", "settled", "wins", "losses", "ties"] {
            assert_eq!(
                i64::from(r[&format!("out_{key}")].i32s()[0]),
                oracle[key].as_i64().unwrap(),
                "TR-A {name} {key}"
            );
        }
        for (key, raw) in [
            ("net_units", "out_net_fp"),
            ("max_drawdown_units", "out_max_dd_fp"),
        ] {
            assert_eq!(
                r[raw].i64s()[0] as f64 / 10000.0,
                oracle[key].as_f64().unwrap()
            );
        }
    }
    decimal_equal(
        &json!(result("TR-A-1")["out_longest_dd_ms"].i64s()[0] as f64 / 3_600_000.0),
        &json!(3000.0 / 3_600_000.0),
        Some(7),
        "repair longest drawdown",
    );
    assert_ne!(
        result("TR-A-0")["out_wins"].i32s(),
        result("TR-A-1")["out_wins"].i32s()
    );
    let extra = result("TR-extra-non-equivalences-0");
    assert_eq!(
        (
            extra["out_opened"].i32s()[0],
            extra["out_settled"].i32s()[0]
        ),
        (1, 0)
    );
    assert_eq!(
        (
            extra["out_longest_dd_ms"].i64s()[1],
            extra["out_longest_dd_trades"].i32s()[1]
        ),
        (99500, 5)
    );
    // The CPU capacity oracle is indexed by the exact selected candidate set.
    for (i, case) in cases
        .iter()
        .enumerate()
        .filter(|(_, c)| c.symbol == "replay_capacity")
    {
        let count = case.inputs["candidate_count"].i32s()[0] as usize;
        let events = case.inputs["event_count"].i32s()[0] as usize;
        for (p, mask) in case.inputs["masks"].u8s().chunks_exact(count).enumerate() {
            let selected: Vec<i64> = mask
                .iter()
                .enumerate()
                .filter_map(|(i, &m)| (m != 0).then_some(i as i64))
                .collect();
            let oracle = f["portfolio"][0]["capacity"]
                .as_array()
                .unwrap()
                .iter()
                .find(|launch| format!("TP-C-{}", launch["launch"].as_u64().unwrap()) == case.id)
                .unwrap()["cpu_reference"]
                .as_array()
                .unwrap()
                .iter()
                .find(|o| Buffer::fixture(&o["inputs"]["candidate_indices"]).i64s() == selected)
                .unwrap();
            let accepted: Vec<usize> = results[i]["accepted"].u8s()[p * events..(p + 1) * events]
                .iter()
                .enumerate()
                .filter_map(|(i, &v)| (v != 0).then_some(i))
                .collect();
            assert_eq!(json!(accepted), oracle["report"]["accepted_event_indices"]);
        }
    }
}

#[test]
fn literal_legacy_parity() {
    assert_eq!(
        split_mask(
            &[4000, 1000, 3000, 3000, 5000],
            &[1, 2, 3, 0, 4],
            &[true, true, true, false, true],
            3000,
            5000
        ),
        [1, 2, 1, 0, 0]
    );
    assert_eq!(
        split_mask(
            &[4000, 1000, 3000, 3000, 5000],
            &[1, 2, 3, 0, 4],
            &[true, true, true, false, true],
            6000,
            7000
        ),
        [0; 5]
    );
    assert!(!COMPARISONS.is_empty());
    let f = fixture();
    let cases = literal_cases(&f);
    let results: Vec<_> = cases
        .iter()
        .map(|case| {
            let measured = run(&Backend::Cpu, case);
            assert_eq!(measured.timings.allocated_bytes, 0);
            let output = measured.output;
            compare(&case.id, &output, &case.outputs);
            output
        })
        .collect();
    independent_expectations(&f, &cases, &results);
    println!(
        "literal parity: 12 cases, 34 launches, 13 kernel symbols, 512 independently checked search candidates; TS-C eight-way equivalence exact"
    );
}

#[cfg(feature = "cuda")]
fn slots(b: &Buffers) -> search::CandidateSlots<'_> {
    search::CandidateSlots {
        features: [
            b["feature1"].i32s(),
            b["feature2"].i32s(),
            b["feature3"].i32s(),
            b["feature4"].i32s(),
        ],
        buckets: [
            b["bucket1"].i16s(),
            b["bucket2"].i16s(),
            b["bucket3"].i16s(),
            b["bucket4"].i16s(),
        ],
        candidate_count: b["candidate_count"].i32s()[0],
    }
}
#[cfg(feature = "cuda")]
fn sparse(b: &Buffers) -> search::SparseIndex<'_> {
    search::SparseIndex {
        candidate_driver_key: b["candidate_driver_key"].i32s(),
        key_chrono_offsets: b["key_chrono_offsets"].i32s(),
        key_chrono_rows: b["key_chrono_rows"].i32s(),
    }
}

#[cfg(feature = "cuda")]
mod governed {
    use super::*;
    use binary_alpha_accelerator::{MODULE_BUILD, MODULE_CUBIN, cuda::Device};
    use binary_alpha_engine::config::{Config, StreamKey};
    use binary_alpha_engine::dataset::{DatasetRole, GenerationManifest};
    use binary_alpha_engine::execution::{
        Comparator, Direction, EventKind, FinancialEvent, ReplayManifest, Threshold,
        signal_logic_identity,
    };
    use binary_alpha_engine::features::{
        FeatureManifest, FeaturePlan, FittedEncoding, Kind, ProjectionKind, Value as FeatureValue,
    };
    use binary_alpha_engine::outcomes::{
        InvalidReason, Outcome, OutcomeBuilder, OutcomeManifest, TICK_PRICE_OBJECT_PATH,
        TICK_TIME_OBJECT_PATH, stream_object_paths,
    };
    use common::{
        LegacyCsv, Scratch, in_process_peak_kb, manifest_json, read_le, read_table, sha256, timed,
    };
    use sha2::{Digest, Sha256};
    use std::fs::{self, File};
    use std::io::{BufRead, Write};
    use std::path::{Component, PathBuf};
    use std::process::Command;
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    const STREAM: StreamKey = StreamKey {
        duration_seconds: 30,
        offset_seconds: 15,
    };
    const SOURCE_FILES: [&str; 8] = [
        "trex/strategy_searcher_gpu/cupy_engine.py",
        "trex/strategy_searcher/backtest/stability.py",
        "trex/portfolio_optimizer/cupy_engine.py",
        "trex/strategy_repair/gpu_replay.py",
        "trex/strategy_searcher_gpu/tests/test_backtester_metric_parity.py",
        "trex/strategy_searcher_gpu/tests/test_bootstrap_stability.py",
        "trex/tests/test_strategy_repair.py",
        "trex/tests/test_portfolio_optimizer.py",
    ];

    #[derive(Clone)]
    struct Candidate {
        id: String,
        expiry: i64,
        direction: i32,
        feature: i32,
        bucket: i16,
    }
    struct Prepared {
        scratch: Scratch,
        config: Config,
        source: String,
        reference_root: PathBuf,
        phase06: Value,
        identities: Value,
        candidates: Vec<Candidate>,
        close: Vec<i64>,
        stages: Vec<Vec<Case>>,
    }

    /// A fresh, retained receipt namespace. A completed test scratch is never deleted.
    fn scratch() -> Scratch {
        let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "phase07_{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(root.parent().unwrap()).unwrap();
        fs::create_dir(&root).unwrap();
        Scratch { root }
    }
    fn micros(text: &str) -> i64 {
        binary_alpha_engine::market::parse_event_time_micros(text).unwrap()
    }
    fn millis(value: i64) -> i64 {
        assert_eq!(
            value % 1000,
            0,
            "CUDA millisecond conversion would lose timestamp precision: {value}"
        );
        value / 1000
    }
    fn local(uri: &str) -> PathBuf {
        PathBuf::from(
            uri.strip_prefix("file://")
                .expect("governed proof requires local development manifests"),
        )
    }
    fn store(path: &Path) -> &Path {
        path.parent().unwrap().parent().unwrap().parent().unwrap()
    }
    fn digest(bytes: &[u8]) -> String {
        Sha256::digest(bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }
    fn scalar32(v: usize) -> Buffer {
        Buffer::from(vec![i32::try_from(v).unwrap()]).shaped(vec![])
    }
    fn scalar64(v: i64) -> Buffer {
        Buffer::from(vec![v]).shaped(vec![])
    }
    fn read_manifest(uri: &str) -> (PathBuf, Value) {
        let path = local(uri);
        let value = manifest_json(&path);
        assert_eq!(
            value["role"], "development",
            "refuse non-development manifest before opening any objects: {uri}"
        );
        let output = common::binary_alpha(&["data", "verify", "--manifest", uri]);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).starts_with("verified "));
        (path, value)
    }
    fn object(manifest: &Value, root: &Path, name: &str) -> PathBuf {
        root.join(
            manifest["objects"]
                .as_array()
                .unwrap()
                .iter()
                .find(|o| o["path"] == name)
                .unwrap()["key"]
                .as_str()
                .unwrap(),
        )
    }

    /// Read the exact bound generations and reproduce the legacy host's buffers through
    /// generic readers. This single preparation is used by both capture and comparison.
    fn prepare() -> Prepared {
        let wrapper = std::env::var("BINARY_ALPHA_TEST_CONFIG")
            .expect("BINARY_ALPHA_TEST_CONFIG names the governed wrapper");
        let wrapper = manifest_json(Path::new(&wrapper));
        let source = fs::read_to_string(wrapper["application_config"].as_str().unwrap()).unwrap();
        let config = Config::parse(&source).unwrap();
        let settings = config
            .replay
            .as_ref()
            .expect("governed configuration has replay");
        assert_eq!(settings.role, DatasetRole::Development);
        assert!(
            config.accelerator.is_none(),
            "frozen replay configuration is unmodified"
        );
        assert_eq!(settings.inputs.len(), 1);
        let phase06 = manifest_json(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/phase06_reference.json"),
        );
        let reference_root = PathBuf::from(wrapper["reference_root"].as_str().unwrap());
        for file in phase06["reference_files"].as_array().unwrap() {
            let path = reference_root.join(file["path"].as_str().unwrap());
            assert_eq!(
                sha256(&path),
                file["sha256"].as_str().unwrap(),
                "{}",
                path.display()
            );
        }
        let input = &settings.inputs[0];
        let (tick_path, tick_json) = read_manifest(&input.tick_manifest.to_string());
        let tick = GenerationManifest::from_json(&fs::read(&tick_path).unwrap()).unwrap();
        assert_eq!(tick.row_count, phase06["source_rows"].as_u64().unwrap());
        assert_eq!(
            tick.inputs[0].sha256,
            phase06["source_sha256"].as_str().unwrap()
        );
        let (feature_path, feature_json) = read_manifest(&input.feature_manifest.to_string());
        let feature = FeatureManifest::from_json(&fs::read(&feature_path).unwrap()).unwrap();
        let plan = FeaturePlan::from_json(
            &fs::read(object(&feature_json, store(&feature_path), "plan.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(plan.identity(), feature.plan_identity);
        assert_eq!(plan.development_generation, tick.generation);
        let stream = plan.stream(STREAM).expect("the governed stream is 30s/15s");
        assert!(
            stream.encodings.is_empty(),
            "governed plan must not contain fitted encodings"
        );
        let (outcome_path, outcome_json) = read_manifest(
            &input
                .outcome_manifest
                .as_ref()
                .expect("governed replay binds Phase 05 outcomes")
                .to_string(),
        );
        let outcome = OutcomeManifest::from_json(&fs::read(&outcome_path).unwrap()).unwrap();
        assert_eq!(outcome.tick_generation, tick.generation);
        assert_eq!(outcome.feature_generation, feature.generation);
        let builder = OutcomeBuilder::new(
            outcome.rule.clone(),
            read_le(
                &object(&outcome_json, store(&outcome_path), TICK_TIME_OBJECT_PATH),
                i64::from_le_bytes,
            ),
            read_le(
                &object(&outcome_json, store(&outcome_path), TICK_PRICE_OBJECT_PATH),
                i64::from_le_bytes,
            ),
        )
        .unwrap();
        let times = builder.times();
        assert_eq!(times.len() as u64, tick.row_count);
        for &time in times {
            millis(time);
        }
        let paths = stream_object_paths(30, 15);
        let references = read_le(
            &object(&outcome_json, store(&outcome_path), &paths[0]),
            i64::from_le_bytes,
        );
        let entries = read_le(
            &object(&outcome_json, store(&outcome_path), &paths[1]),
            u32::from_le_bytes,
        );
        let settlements = read_le(
            &object(&outcome_json, store(&outcome_path), &paths[2]),
            u32::from_le_bytes,
        );
        let reasons = fs::read(object(&outcome_json, store(&outcome_path), &paths[3])).unwrap();
        let (names, rows) = read_table(&object(
            &feature_json,
            store(&feature_path),
            "rows/30s_15s.parquet",
        ));
        let column = |name: &str| {
            names
                .iter()
                .position(|n| n == name)
                .unwrap_or_else(|| panic!("feature column {name} absent"))
        };
        let time_column = |name: &str| {
            rows.iter()
                .map(|r| match r[column(name)] {
                    Some(FeatureValue::Time(t) | FeatureValue::Int(t)) => t,
                    _ => panic!("{name} must be a timestamp"),
                })
                .collect::<Vec<_>>()
        };
        let close = time_column("close_time_micros");
        assert_eq!(close, references);
        // Unavailable rows remain excluded; the engine observes each available row once.
        let known: Vec<Option<i64>> = rows
            .iter()
            .map(|r| match r[column("known_at_micros")] {
                Some(FeatureValue::Time(t) | FeatureValue::Int(t)) => Some(t),
                None => None,
                _ => panic!("known_at must be a timestamp"),
            })
            .collect();
        let outputs: Vec<String> = settings
            .strategies
            .iter()
            .map(|s| {
                assert_eq!(
                    s.base_stream, STREAM,
                    "governed strategies require base stream 30s/15s"
                );
                assert_eq!(
                    s.conditions.len(),
                    1,
                    "governed strategies require exactly one condition"
                );
                assert!(
                    s.repair.is_empty(),
                    "governed strategy may not add repair conditions"
                );
                let c = &s.conditions[0];
                assert_eq!(c.stream, STREAM);
                assert_eq!(
                    c.comparator,
                    Comparator::Eq,
                    "only eq is preserved by this capture"
                );
                assert!(
                    matches!(c.threshold, Threshold::Text(_)),
                    "governed threshold must be text"
                );
                assert_eq!(s.plan_identity, plan.identity());
                assert_eq!(
                    stream.outputs[stream.output_index(&c.output).unwrap()].kind,
                    Kind::Text
                );
                c.output.clone()
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut encoded = Vec::new();
        let mut encodings = Vec::new();
        for output in &outputs {
            let values: Vec<_> = rows.iter().map(|r| r[column(output)].clone()).collect();
            let mut encoding = FittedEncoding {
                output: output.clone(),
                input: output.clone(),
                encoding: ProjectionKind::Category,
                edges: None,
                input_divisor: 1.0,
                labels: Vec::new(),
            };
            encoding.fit(&values, plan.max_labels).unwrap();
            assert!(encoding.labels.len() <= i16::MAX as usize);
            let mut codes = encoding.encode(&values);
            let readiness = plan.readiness_of(output);
            for (i, row) in rows.iter().enumerate() {
                if readiness
                    .flags
                    .iter()
                    .any(|flag| row[column(flag)] != Some(FeatureValue::Bool(true)))
                    || matches!(&values[i],Some(FeatureValue::Text(t)) if readiness.unready.iter().any(|v|v==t.as_ref()))
                {
                    codes[i] = -1;
                }
            }
            encoded.extend(codes);
            encodings.push(encoding);
        }
        let mut candidates = Vec::new();
        let frozen = phase06["candidates"].as_array().unwrap();
        assert_eq!(settings.bindings.len(), 24);
        assert_eq!(frozen.len(), 24);
        for (binding, expected) in settings.bindings.iter().zip(frozen) {
            assert_eq!(binding.id, expected["candidate_id"].as_str().unwrap());
            assert_eq!(binding.strategy, expected["strategy"].as_str().unwrap());
            let strategy = settings
                .strategies
                .iter()
                .find(|s| s.id == binding.strategy)
                .unwrap();
            let condition = &strategy.conditions[0];
            let Threshold::Text(label) = &condition.threshold else {
                unreachable!()
            };
            assert_eq!(condition.output, expected["output"].as_str().unwrap());
            assert_eq!(label, expected["value"].as_str().unwrap());
            let contract = settings
                .contracts
                .iter()
                .find(|c| c.id == binding.contract)
                .unwrap();
            assert_eq!(contract.duration_micros % 1_000_000, 0);
            let expiry = contract.duration_micros / 1_000_000;
            assert_eq!(expiry, expected["expiry_seconds"].as_i64().unwrap());
            assert!(matches!(expiry, 60 | 90));
            assert_eq!(
                contract.direction.to_string(),
                expected["direction"].as_str().unwrap().to_lowercase()
            );
            // Checked decimal ownership: the frozen 1.92 return is exactly 92 integer percent.
            assert_eq!(contract.win.gross_return.to_string(), "1.92");
            assert_eq!(
                contract.settlement.max_tick_gap_micros,
                outcome.rule.max_tick_gap_micros
            );
            let feature = outputs.iter().position(|o| o == &condition.output).unwrap();
            let bucket = encodings[feature]
                .labels
                .iter()
                .position(|v| v == label)
                .expect("threshold absent from fitted development labels");
            candidates.push(Candidate {
                id: binding.id.clone(),
                expiry,
                direction: if contract.direction == Direction::Buy {
                    1
                } else {
                    -1
                },
                feature: feature as i32,
                bucket: i16::try_from(bucket).unwrap(),
            });
        }
        let n = close.len();
        assert_eq!(entries.len(), n);
        let gaps: Vec<usize> = times
            .windows(2)
            .enumerate()
            .filter_map(|(i, t)| (t[1] - t[0] > outcome.rule.max_tick_gap_micros).then_some(i))
            .collect();
        // The source emits only the last base row at a shared entry tick.
        let mut last_entry = BTreeMap::new();
        for (i, &entry) in entries.iter().enumerate() {
            if entry != outcome.missing_index {
                last_entry.insert(entry, i);
            }
        }
        let eligible: Vec<bool> = entries
            .iter()
            .enumerate()
            .map(|(i, &entry)| known[i].is_some() && last_entry.get(&entry) == Some(&i))
            .collect();
        let entry_times: Vec<i64> = entries
            .iter()
            .enumerate()
            .map(|(i, &entry)| {
                millis(if entry == outcome.missing_index {
                    close[i]
                } else {
                    times[entry as usize]
                })
            })
            .collect();
        for (i, &entry) in entries.iter().enumerate() {
            if eligible[i] {
                assert_eq!(
                    known[i],
                    Some(times[entry as usize]),
                    "Phase05 entry tick and feature availability differ at row {i}"
                );
            }
        }
        let mut ordered: Vec<i64> = (0..n as i64).collect();
        ordered.sort_by_key(|&r| entry_times[r as usize]);
        let start = micros(&settings.decision_start);
        let end = micros(&settings.decision_end);
        let mut windows = vec![("full".to_string(), start, end)];
        for split in settings
            .splits
            .as_ref()
            .expect("governed splits are frozen")
        {
            windows.push((
                split.name.clone(),
                micros(&split.start).max(start),
                micros(&split.end).min(end),
            ));
        }
        let mut stages = Vec::new();
        for expiry in [60, 90] {
            let expiry_column = outcome
                .rule
                .expiry_seconds
                .iter()
                .position(|&e| i64::from(e) == expiry)
                .unwrap();
            let mut release = vec![0_i64; n];
            let mut settlement = vec![0_i64; n];
            let mut valid = vec![0_u8; n];
            let mut buy = vec![0_u8; n];
            let mut sell = vec![0_u8; n];
            let mut tie = vec![0_u8; n];
            for i in 0..n {
                let cell = builder
                    .cell(
                        entries[i],
                        expiry_column,
                        settlements[i * outcome.rule.expiry_seconds.len() + expiry_column],
                        reasons[i * outcome.rule.expiry_seconds.len() + expiry_column],
                    )
                    .unwrap();
                let Some(entry) = cell.entry else { continue };
                if !eligible[i] || cell.reason == InvalidReason::StaleEntry {
                    continue;
                }
                let e = entry.index as usize;
                if e > 0 && times[e] - times[e - 1] > outcome.rule.max_tick_gap_micros {
                    continue;
                }
                let due = cell.due_time_micros.unwrap();
                let gap = gaps
                    .get(gaps.partition_point(|&g| g < e))
                    .copied()
                    .filter(|&g| times[g] < due);
                if let Some(gap) = gap {
                    assert!(
                        matches!(
                            cell.reason,
                            InvalidReason::InternalGap
                                | InvalidReason::StaleSettlement
                                | InvalidReason::NoSettlement
                        ),
                        "unclassified crossed-gap label {:?}",
                        cell.reason
                    );
                    release[i] = millis(times[gap + 1]);
                } else if let Some(end) = cell.settlement {
                    assert_eq!(
                        cell.reason,
                        InvalidReason::Valid,
                        "unclassified settled label at row {i}"
                    );
                    release[i] = millis(end.event_time_micros);
                    settlement[i] = release[i];
                    valid[i] = 1;
                    match cell.outcome.unwrap() {
                        Outcome::BuyWin => buy[i] = 1,
                        Outcome::SellWin => sell[i] = 1,
                        Outcome::Tie => tie[i] = 1,
                    }
                } else {
                    assert_eq!(cell.reason, InvalidReason::NoSettlement);
                    release[i] = i64::MAX;
                }
            }
            let shared = BTreeMap::from([
                (
                    "feature_codes".into(),
                    Buffer::from(encoded.clone()).shaped(vec![outputs.len(), n]),
                ),
                ("feature_count".into(), scalar32(outputs.len())),
                ("row_count".into(), scalar32(n)),
                ("ordered_rows".into(), Buffer::from(ordered.clone())),
                ("decision_time_ms".into(), Buffer::from(entry_times.clone())),
                ("release_time_ms".into(), Buffer::from(release)),
                ("settlement_time_ms".into(), Buffer::from(settlement)),
                ("valid".into(), Buffer::from(valid)),
                ("buy_win".into(), Buffer::from(buy)),
                ("sell_win".into(), Buffer::from(sell)),
                ("tie".into(), Buffer::from(tie)),
                ("expiry_ms".into(), scalar64(expiry * 1000)),
                ("payout_basis".into(), scalar64(92)),
            ]);
            for (name, start, end) in &windows {
                let mut shared = shared.clone();
                shared.insert(
                    "split_mask".into(),
                    Buffer::from(split_mask(
                        &entry_times,
                        &ordered,
                        &eligible,
                        millis(*start),
                        millis(*end),
                    )),
                );
                let selected: Vec<_> = candidates
                    .iter()
                    .filter(|c| c.expiry == expiry)
                    .cloned()
                    .collect();
                stages.push(search_cases(
                    &format!("governed-{expiry}-{name}"),
                    &shared,
                    &selected,
                ));
            }
        }
        let identities = json!({"configuration":config.content_hash(),"tick_generation":tick.generation,"feature_generation":feature.generation,"outcome_generation":outcome.generation,"plan_identity":plan.identity(),"strategies":settings.strategies.iter().map(|s|(&s.id,signal_logic_identity(s))).collect::<BTreeMap<_,_>>(),"bindings":settings.bindings,"contracts":settings.contracts,"splits":settings.splits,"decision_start":settings.decision_start,"decision_end":settings.decision_end,"encodings":encodings,"phase06_fixture_sha256":sha256(&Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/phase06_reference.json")),"tick_manifest_sha256":digest(&serde_json::to_vec(&tick_json).unwrap())});
        println!(
            "prepared {} base rows, {} condition outputs, 24 candidate bindings, {} expiry/split stages",
            n,
            outputs.len(),
            stages.len()
        );
        Prepared {
            scratch: scratch(),
            config,
            source,
            reference_root,
            phase06,
            identities,
            candidates,
            close,
            stages,
        }
    }

    /// Build dense, sparse, dual, and direction-specific calls from one expiry/split.
    fn search_cases(prefix: &str, shared: &Buffers, candidates: &[Candidate]) -> Vec<Case> {
        let mut cases = Vec::new();
        for (kind, (symbol, _)) in KERNEL_SOURCES[..9].iter().enumerate() {
            let dual = matches!(kind, 1 | 3 | 6 | 7 | 8);
            for direction in if dual { vec![0] } else { vec![1, -1] } {
                let selected: Vec<_> = candidates
                    .iter()
                    .filter(|c| direction == 0 || c.direction == direction)
                    .collect();
                assert!(!selected.is_empty());
                let mut b = shared.clone();
                let n = selected.len();
                let rows = b["row_count"].i32s()[0] as usize;
                b.insert("candidate_count".into(), scalar32(n));
                b.insert(
                    "direction_code".into(),
                    Buffer::from(vec![direction]).shaped(vec![]),
                );
                for slot in 1..=4 {
                    b.insert(
                        format!("feature{slot}"),
                        Buffer::from(
                            selected
                                .iter()
                                .map(|c| if slot == 1 { c.feature } else { -1 })
                                .collect::<Vec<i32>>(),
                        ),
                    );
                    b.insert(
                        format!("bucket{slot}"),
                        Buffer::from(
                            selected
                                .iter()
                                .map(|c| if slot == 1 { c.bucket } else { -1 })
                                .collect::<Vec<i16>>(),
                        ),
                    );
                }
                // Sorted distinct keys, lists in ordered-row order; every candidate chooses
                // its shortest active-condition list (one condition in this frozen workload).
                let keys: Vec<_> = selected
                    .iter()
                    .map(|c| (c.feature, c.bucket))
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                let mut offsets = vec![0_i32];
                let mut chrono = Vec::new();
                for &(feature, bucket) in &keys {
                    chrono.extend(
                        b["ordered_rows"]
                            .i64s()
                            .iter()
                            .filter(|&&r| {
                                b["feature_codes"].i16s()[feature as usize * rows + r as usize]
                                    == bucket
                            })
                            .map(|&r| i32::try_from(r).unwrap()),
                    );
                    offsets.push(i32::try_from(chrono.len()).unwrap());
                }
                b.insert(
                    "candidate_driver_key".into(),
                    Buffer::from(
                        selected
                            .iter()
                            .map(|c| {
                                keys.iter()
                                    .position(|k| *k == (c.feature, c.bucket))
                                    .unwrap() as i32
                            })
                            .collect::<Vec<_>>(),
                    ),
                );
                b.insert("key_chrono_offsets".into(), Buffer::from(offsets));
                b.insert("key_chrono_rows".into(), Buffer::from(chrono));
                let shape = vec![
                    n,
                    if kind == 8 {
                        rows
                    } else if matches!(kind, 2 | 3 | 5 | 7) {
                        8
                    } else {
                        21
                    },
                ];
                let output = if kind == 8 {
                    Buffer::from(vec![0_u8; n * rows]).shaped(shape)
                } else {
                    Buffer::from(vec![0_i64; shape.iter().product()]).shaped(shape)
                };
                let expected = if matches!(kind, 1 | 3 | 6 | 7) {
                    BTreeMap::from([
                        ("buy_output".into(), output.clone()),
                        ("sell_output".into(), output),
                    ])
                } else {
                    BTreeMap::from([("output".into(), output)])
                };
                cases.push(Case {
                    id: format!("{prefix}-{symbol}-{direction}"),
                    symbol: symbol.to_string(),
                    inputs: b,
                    outputs: expected,
                });
            }
        }
        cases
    }

    fn search_buffers(b: &Buffers) -> search::SearchBuffers<'_> {
        search::SearchBuffers {
            feature_codes: b["feature_codes"].i16s(),
            feature_count: b["feature_count"].i32s()[0],
            row_count: b["row_count"].i32s()[0],
            ordered_rows: b["ordered_rows"].i64s(),
            decision_time_ms: b["decision_time_ms"].i64s(),
            release_time_ms: b["release_time_ms"].i64s(),
            settlement_time_ms: b["settlement_time_ms"].i64s(),
            valid: b["valid"].u8s(),
            buy_win: b["buy_win"].u8s(),
            sell_win: b["sell_win"].u8s(),
            tie: b["tie"].u8s(),
        }
    }

    /// Locate full-window governed results by expiry without guessing candidate order.
    fn full<'a>(stages: &'a [Vec<Case>], expiry: i64, symbol: &str) -> &'a Case {
        let prefix = format!("governed-{expiry}-full-");
        stages
            .iter()
            .flatten()
            .find(|c| c.id.starts_with(&prefix) && c.symbol == symbol)
            .unwrap()
    }
    fn matches(b: &Buffers, c: &Candidate, row: usize) -> bool {
        b["split_mask"].u8s()[row] == 1
            && b["feature_codes"].i16s()
                [c.feature as usize * b["row_count"].i32s()[0] as usize + row]
                == c.bucket
    }

    /// Deterministic analysis inputs use governed admissions and events. The literal sampling
    /// matrix is part of the captured inputs; it does not implement a sampling generator.
    fn analysis_cases(prepared: &Prepared, stages: &[Vec<Case>]) -> Vec<Vec<Case>> {
        let mut paths = Vec::new();
        const SELECTED: [usize; 4] = [0, 1, 12, 13];
        const INDICES: [[usize; 8]; 4] = [
            [0, 1, 2, 3, 4, 5, 6, 7],
            [7, 6, 5, 4, 3, 2, 1, 0],
            [0, 0, 2, 2, 4, 4, 6, 6],
            [1, 3, 5, 7, 1, 3, 5, 7],
        ];
        let mut events = Vec::new();
        let mut offsets = vec![0_i64];
        let mut entry = Vec::new();
        let mut settlement = Vec::new();
        let mut close = Vec::new();
        let mut outcomes = Vec::new();
        let mut valid_entry = Vec::new();
        let mut failures = Vec::new();
        let mut aggregate = vec![0_f32; prepared.close.len() * 3];
        for (index, candidate) in prepared.candidates.iter().enumerate() {
            let mask_case = full(stages, candidate.expiry, "reconstruct_signal_masks_cap1");
            let b = &mask_case.inputs;
            let within = prepared
                .candidates
                .iter()
                .filter(|c| c.expiry == candidate.expiry)
                .position(|c| c.id == candidate.id)
                .unwrap();
            let mask = &mask_case.outputs["output"].u8s()
                [within * prepared.close.len()..(within + 1) * prepared.close.len()];
            let mut returns = Vec::new();
            for &row in b["ordered_rows"].i64s() {
                let r = row as usize;
                if !matches(b, candidate, r) {
                    continue;
                }
                let v = b["valid"].u8s()[r];
                let tie = b["tie"].u8s()[r];
                let win = if candidate.direction == 1 {
                    b["buy_win"].u8s()[r]
                } else {
                    b["sell_win"].u8s()[r]
                };
                let outcome = if v == 0 {
                    -1_i8
                } else if tie != 0 {
                    2
                } else if win != 0 {
                    1
                } else {
                    0
                };
                let release = b["release_time_ms"].i64s()[r];
                let start = b["decision_time_ms"].i64s()[r];
                entry.push(start);
                settlement.push(b["settlement_time_ms"].i64s()[r]);
                close.push(release);
                outcomes.push(outcome);
                valid_entry.push(u8::from(release > 0));
                let codes = b["feature_codes"].i16s();
                // Literal policy bit 0: first encoded output has code 0; bit 1: second
                // encoded output is unavailable. Both are known at the decision row.
                assert!(b["feature_count"].i32s()[0] >= 2);
                failures.push(
                    u64::from(codes[r] == 0)
                        | (u64::from(codes[prepared.close.len() + r] < 0) << 1),
                );
                events.push((
                    start,
                    index as i32,
                    r,
                    release,
                    v,
                    mask[r],
                    candidate.expiry as i32,
                ));
                if mask[r] != 0 && v != 0 {
                    let units = if tie != 0 {
                        0.0
                    } else if win != 0 {
                        0.92
                    } else {
                        -1.0
                    };
                    returns.push(units);
                    for p in 0..3 {
                        if p == 2 || (p == 0 && index < 2) || (p == 1 && (2..4).contains(&index)) {
                            aggregate[r * 3 + p] += units as f32;
                        }
                    }
                }
            }
            offsets.push(entry.len() as i64);
            if SELECTED.contains(&index) {
                assert!(returns.len() >= 8);
                for indices in INDICES {
                    paths.extend(indices.map(|i| returns[i]));
                }
            }
        }
        let bootstrap = Case {
            id: "governed-bootstrap".into(),
            symbol: "bootstrap_path_metrics".into(),
            inputs: BTreeMap::from([
                ("paths".into(), Buffer::from(paths).shaped(vec![16, 8])),
                ("simulations".into(), scalar32(16)),
                ("trade_count".into(), scalar32(8)),
                ("rolling_horizon".into(), scalar32(3)),
                (
                    "source_candidate_indices".into(),
                    Buffer::from(SELECTED.map(|v| v as i32).to_vec()),
                ),
                (
                    "sample_indices".into(),
                    Buffer::from(
                        INDICES
                            .into_iter()
                            .flatten()
                            .map(|v| v as i32)
                            .collect::<Vec<_>>(),
                    )
                    .shaped(vec![4, 8]),
                ),
            ]),
            outputs: BTreeMap::from([
                ("max_drawdowns".into(), Buffer::from(vec![0_f64; 16])),
                ("longest_underwater".into(), Buffer::from(vec![0_i64; 16])),
                ("negative_rolling".into(), Buffer::from(vec![0_i64; 16])),
            ]),
        };
        let policies: Vec<i32> = (0..24).flat_map(|i| [i, i, i]).collect();
        let policy_masks: Vec<u64> = (0..24).flat_map(|_| [0, 1, 2]).collect();
        let mut repair_outputs = Buffers::new();
        for name in [
            "out_opened",
            "out_settled",
            "out_wins",
            "out_losses",
            "out_ties",
            "out_longest_dd_trades",
            "out_longest_loss_streak",
        ] {
            repair_outputs.insert(name.into(), Buffer::from(vec![0_i32; 72]));
        }
        for name in [
            "out_net_fp",
            "out_max_dd_fp",
            "out_longest_dd_ms",
            "out_gross_profit_fp",
            "out_gross_loss_fp",
            "out_sum_returns_fp",
            "out_sum_squares_fp2",
            "out_downside_squares_fp2",
        ] {
            repair_outputs.insert(name.into(), Buffer::from(vec![0_i64; 72]));
        }
        let settings = prepared.config.replay.as_ref().unwrap();
        let repair = Case {
            id: "governed-repair".into(),
            symbol: "replay_policies".into(),
            inputs: BTreeMap::from([
                ("candidate_offsets".into(), Buffer::from(offsets)),
                ("entry_ms".into(), Buffer::from(entry)),
                ("settlement_ms".into(), Buffer::from(settlement)),
                ("close_ms".into(), Buffer::from(close)),
                ("outcomes".into(), Buffer::from(outcomes)),
                ("valid_entry".into(), Buffer::from(valid_entry)),
                ("failure_words".into(), Buffer::from(failures)),
                ("word_count".into(), scalar32(1)),
                ("policy_candidate".into(), Buffer::from(policies)),
                ("policy_masks".into(), Buffer::from(policy_masks)),
                ("policy_count".into(), scalar32(72)),
                (
                    "start_ms".into(),
                    scalar64(millis(micros(&settings.decision_start))),
                ),
                (
                    "end_ms".into(),
                    scalar64(millis(micros(&settings.decision_end))),
                ),
                ("payout_fp".into(), scalar64(9200)),
            ]),
            outputs: repair_outputs,
        };
        events.sort_by_key(|&(time, candidate, row, _, _, _, _)| (time, candidate, row));
        let n = events.len();
        let mut capacity = Vec::new();
        for (total, expiry) in [(1, 1), (4, 2), (64, 32)] {
            capacity.push(Case {
                id: format!("governed-capacity-{total}-{expiry}"),
                symbol: "replay_capacity".into(),
                inputs: BTreeMap::from([
                    (
                        "masks".into(),
                        Buffer::from(
                            (0..3)
                                .flat_map(|p| {
                                    (0..24).map(move |i| {
                                        u8::from(
                                            p == 2
                                                || (p == 0 && i < 2)
                                                || (p == 1 && (2..4).contains(&i)),
                                        )
                                    })
                                })
                                .collect::<Vec<_>>(),
                        )
                        .shaped(vec![3, 24]),
                    ),
                    (
                        "candidate_index".into(),
                        Buffer::from(events.iter().map(|e| e.1).collect::<Vec<_>>()),
                    ),
                    (
                        "entry_time".into(),
                        Buffer::from(events.iter().map(|e| e.0).collect::<Vec<_>>()),
                    ),
                    (
                        "due_time".into(),
                        Buffer::from(events.iter().map(|e| e.3).collect::<Vec<_>>()),
                    ),
                    (
                        "expiry_seconds".into(),
                        Buffer::from(events.iter().map(|e| e.6).collect::<Vec<_>>()),
                    ),
                    (
                        "hypothetical_valid".into(),
                        Buffer::from(events.iter().map(|e| e.4).collect::<Vec<_>>()),
                    ),
                    (
                        "standalone_admitted".into(),
                        Buffer::from(events.iter().map(|e| e.5).collect::<Vec<_>>()),
                    ),
                    ("portfolio_count".into(), scalar32(3)),
                    ("candidate_count".into(), scalar32(24)),
                    ("event_count".into(), scalar32(n)),
                    ("max_total".into(), scalar32(total)),
                    ("max_expiry".into(), scalar32(expiry)),
                ]),
                outputs: BTreeMap::from([(
                    "accepted".into(),
                    Buffer::from(vec![0_u8; 3 * n]).shaped(vec![3, n]),
                )]),
            });
        }
        let drawdown = Case {
            id: "governed-drawdown".into(),
            symbol: "path_drawdown".into(),
            inputs: BTreeMap::from([
                (
                    "returns".into(),
                    Buffer::from(aggregate).shaped(vec![prepared.close.len(), 3]),
                ),
                ("observation_count".into(), scalar32(prepared.close.len())),
                ("portfolio_count".into(), scalar32(3)),
            ]),
            outputs: BTreeMap::from([
                ("max_drawdown".into(), Buffer::from(vec![0_f64; 3])),
                ("ulcer_index".into(), Buffer::from(vec![0_f64; 3])),
            ]),
        };
        println!(
            "derived stages: bootstrap 16 paths x 8 trades; repair 24 candidates x 3 policies over {n} events; capacity 3 portfolios x {n} events at limits 1/1, 4/2, 64/32; drawdown {} rows x 3 portfolios",
            prepared.close.len()
        );
        vec![vec![bootstrap], vec![repair], capacity, vec![drawdown]]
    }

    /// Only two storage values change; parse both copies and prove every other field equal.
    fn replay(prepared: &Prepared) -> (ReplayManifest, BTreeMap<String, BTreeSet<i64>>, Value) {
        let mut in_storage = false;
        let mut changed = 0;
        let mut text = String::new();
        for line in prepared.source.lines() {
            let trim = line.trim();
            if trim.starts_with('[') {
                in_storage = trim == "[storage]";
            }
            if in_storage && trim.split('=').next().unwrap().trim() == "historical_data_dir" {
                text.push_str("historical_data_dir = \"retained\"\n");
                changed += 1;
            } else if in_storage && trim.split('=').next().unwrap().trim() == "publication_uri" {
                text.push_str(&format!(
                    "publication_uri = \"file://{}\"\n",
                    prepared.scratch.path("published").display()
                ));
                changed += 1;
            } else {
                text.push_str(line);
                text.push('\n');
            }
        }
        assert_eq!(changed, 2, "both exact storage fields must be present");
        let copied = Config::parse(&text).unwrap();
        let mut restored = copied.clone();
        restored.storage = prepared.config.storage.clone();
        assert_eq!(
            restored, prepared.config,
            "configuration copy changes only storage"
        );
        let path = prepared.scratch.path("replay.toml");
        create(&path, text.as_bytes());
        let (lines, wall, peak) = timed(&["replay", "--config", path.to_str().unwrap()]);
        assert_eq!(lines.len(), 2);
        println!(
            "replay: {}\nreconstruction: {}\nreplay wall {wall:.3} s, child peak {peak} kB",
            lines[0], lines[1]
        );
        let generation = common::generation(&lines[0]);
        let root = prepared.scratch.path("published");
        let manifest_path = root.join(format!("manifests/{generation}/ready.json"));
        let (verified, verify_wall, verify_peak) = timed(&[
            "data",
            "verify",
            "--manifest",
            &format!("file://{}", manifest_path.display()),
        ]);
        assert_eq!(
            verified,
            lines[1..],
            "verification must reconstruct the identical ledger"
        );
        let bytes = fs::read(&manifest_path).unwrap();
        let manifest = ReplayManifest::from_json(&bytes).unwrap();
        assert_eq!(manifest.config_hash, copied.content_hash());
        assert_eq!(manifest.code_revision, env!("BINARY_ALPHA_CODE_REVISION"));
        assert_eq!(manifest.instruments.len(), 1);
        let bound = &manifest.instruments[0];
        assert_eq!(
            bound.tick_generation,
            prepared.identities["tick_generation"]
        );
        assert_eq!(
            bound.feature_generation,
            prepared.identities["feature_generation"]
        );
        assert_eq!(
            bound.outcome_generation.as_deref(),
            prepared.identities["outcome_generation"].as_str()
        );
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        let events = object(
            &json,
            &root,
            binary_alpha_engine::execution::EVENTS_OBJECT_PATH,
        );
        let mut signals: BTreeMap<String, BTreeSet<i64>> = BTreeMap::new();
        let mut count = 0;
        let mut dispositions: BTreeMap<String, u64> = BTreeMap::new();
        for line in std::io::BufReader::new(File::open(events).unwrap()).lines() {
            let event = FinancialEvent::from_line(line.unwrap().as_bytes()).unwrap();
            count += 1;
            if let EventKind::Signal {
                binding,
                close_time_micros,
                disposition,
                ..
            } = event.kind
            {
                assert!(
                    signals
                        .entry(binding)
                        .or_default()
                        .insert(close_time_micros),
                    "duplicate ledger signal key"
                );
                *dispositions.entry(disposition.to_string()).or_default() += 1;
            }
        }
        assert_eq!(count, manifest.events);
        println!(
            "ledger: {} events, {} signals, state {}, summary {}; dispositions {}",
            count,
            signals.values().map(BTreeSet::len).sum::<usize>(),
            manifest.final_state_identity,
            manifest.summary_identity,
            json!(dispositions)
        );
        let receipt = json!({"configuration_copy_content_hash":copied.content_hash(),"configuration_copy_sha256":sha256(&path),"wall_seconds":wall,"child_peak_kb":peak,"verify_wall_seconds":verify_wall,"verify_child_peak_kb":verify_peak,"event_count":manifest.events,"final_state_identity":manifest.final_state_identity,"summary_identity":manifest.summary_identity,"dispositions":dispositions});
        (manifest, signals, receipt)
    }

    /// Join historical rows by signal number; compare every candidate's membership and totals.
    fn connected(
        p: &Prepared,
        stages: &[Vec<Case>],
        ledger: &BTreeMap<String, BTreeSet<i64>>,
    ) -> Value {
        let mut trades = LegacyCsv::open(&p.reference_root.join("parity_cpu_run_v2/trades.csv"));
        let mut settled = BTreeMap::new();
        while let Some(row) = trades.next_row() {
            let number = row[trades.column("signal_number")].parse::<u64>().unwrap();
            assert!(
                settled
                    .insert(
                        number,
                        (
                            row[trades.column("candidate_id")].clone(),
                            row[trades.column("outcome")].clone(),
                            micros(&row[trades.column("row_decision_time_utc")]),
                            micros(&row[trades.column("settlement_tick_time_utc")])
                        )
                    )
                    .is_none()
            );
        }
        let mut invalid = LegacyCsv::open(
            &p.reference_root
                .join("parity_cpu_run_v2/invalid_trades.csv"),
        );
        let mut invalidated = BTreeMap::new();
        while let Some(row) = invalid.next_row() {
            let number = row[invalid.column("signal_number")].parse::<u64>().unwrap();
            assert!(!settled.contains_key(&number));
            assert!(
                invalidated
                    .insert(
                        number,
                        (
                            row[invalid.column("candidate_id")].clone(),
                            micros(&row[invalid.column("row_decision_time_utc")]),
                            micros(&row[invalid.column("gap_end_time_utc")])
                        )
                    )
                    .is_none()
            );
        }
        let row_of: BTreeMap<i64, usize> =
            p.close.iter().enumerate().map(|(i, &c)| (c, i)).collect();
        let mut signals = LegacyCsv::open(&p.reference_root.join("parity_cpu_run_v2/signals.csv"));
        let mut reference: BTreeMap<String, BTreeSet<i64>> = BTreeMap::new();
        let mut opened: BTreeMap<String, BTreeSet<i64>> = BTreeMap::new();
        let mut counts: BTreeMap<String, BTreeMap<&str, u64>> = p
            .candidates
            .iter()
            .map(|c| {
                (
                    c.id.clone(),
                    BTreeMap::from([
                        ("raw_signals", 0),
                        ("opened", 0),
                        ("settled", 0),
                        ("invalidated", 0),
                        ("blocked_by_strategy_capacity", 0),
                        ("blocked_by_entry_gap", 0),
                        ("wins", 0),
                        ("losses", 0),
                        ("ties", 0),
                    ]),
                )
            })
            .collect();
        let mut seen_trades = BTreeSet::new();
        let mut seen_invalid = BTreeSet::new();
        while let Some(row) = signals.next_row() {
            let id = &row[signals.column("candidate_id")];
            let close = micros(&row[signals.column("row_decision_time_utc")]);
            let number = row[signals.column("signal_number")].parse::<u64>().unwrap();
            let c = p
                .candidates
                .iter()
                .find(|c| &c.id == id)
                .expect("unknown reference candidate");
            assert_eq!(
                row[signals.column("direction")].to_lowercase(),
                if c.direction == 1 { "buy" } else { "sell" }
            );
            assert_eq!(
                row[signals.column("expiry_seconds")]
                    .parse::<i64>()
                    .unwrap(),
                c.expiry
            );
            assert!(
                reference.entry(id.clone()).or_default().insert(close),
                "duplicate reference signal key"
            );
            let stats = counts.get_mut(id).unwrap();
            *stats.get_mut("raw_signals").unwrap() += 1;
            let b = &full(stages, c.expiry, "reconstruct_signal_masks_cap1").inputs;
            let r = row_of[&close];
            if row[signals.column("opened_trade")] == "1" {
                assert!(opened.entry(id.clone()).or_default().insert(close));
                *stats.get_mut("opened").unwrap() += 1;
                if let Some((bound, outcome, trade_close, settlement)) = settled.get(&number) {
                    assert_eq!((bound, *trade_close), (id, close));
                    assert_eq!(millis(*settlement), b["settlement_time_ms"].i64s()[r]);
                    assert_eq!(b["valid"].u8s()[r], 1);
                    let expected = if b["tie"].u8s()[r] != 0 {
                        "tie"
                    } else if (c.direction == 1 && b["buy_win"].u8s()[r] != 0)
                        || (c.direction == -1 && b["sell_win"].u8s()[r] != 0)
                    {
                        "win"
                    } else {
                        "loss"
                    };
                    assert_eq!(
                        outcome, expected,
                        "{id} {close}: unclassified outcome difference"
                    );
                    *stats
                        .get_mut(match expected {
                            "win" => "wins",
                            "loss" => "losses",
                            _ => "ties",
                        })
                        .unwrap() += 1;
                    *stats.get_mut("settled").unwrap() += 1;
                    assert!(seen_trades.insert(number));
                } else {
                    let (bound, invalid_close, gap_end) = invalidated
                        .get(&number)
                        .expect("opened signal has neither settled trade nor invalidation");
                    assert_eq!((bound, *invalid_close), (id, close));
                    assert_eq!(b["valid"].u8s()[r], 0);
                    assert_eq!(b["release_time_ms"].i64s()[r], millis(*gap_end));
                    *stats.get_mut("invalidated").unwrap() += 1;
                    assert!(seen_invalid.insert(number));
                }
            } else {
                assert!(!settled.contains_key(&number) && !invalidated.contains_key(&number));
                let reason = &row[signals.column("block_reasons")];
                let key = match reason.as_str() {
                    "[\"max_open_trades_per_strategy\"]" => "blocked_by_strategy_capacity",
                    "[\"invalid_recent_tick_gap\"]" => "blocked_by_entry_gap",
                    _ => panic!("{id} {close}: unclassified reference block {reason}"),
                };
                *stats.get_mut(key).unwrap() += 1;
            }
        }
        assert_eq!(seen_trades.len(), settled.len());
        assert_eq!(seen_invalid.len(), invalidated.len());
        assert_eq!(
            reference, *ledger,
            "reference signals equal replay ledger keys"
        );
        assert_eq!(ledger.len(), 24);
        let mut totals: BTreeMap<&str, u64> = BTreeMap::new();
        for c in &p.candidates {
            let reconstruction = full(stages, c.expiry, "reconstruct_signal_masks_cap1");
            let b = &reconstruction.inputs;
            let position = p
                .candidates
                .iter()
                .filter(|v| v.expiry == c.expiry)
                .position(|v| v.id == c.id)
                .unwrap();
            let pre: BTreeSet<_> = p
                .close
                .iter()
                .enumerate()
                .filter_map(|(r, &time)| matches(b, c, r).then_some(time))
                .collect();
            assert_eq!(pre, ledger[&c.id], "{} pre-capacity membership", c.id);
            let mask = &reconstruction.outputs["output"].u8s()
                [position * p.close.len()..(position + 1) * p.close.len()];
            let post: BTreeSet<_> = p
                .close
                .iter()
                .zip(mask)
                .filter_map(|(&time, &m)| (m == 1).then_some(time))
                .collect();
            assert_eq!(post, opened[&c.id], "{} post-capacity membership", c.id);
            let scoring = full(stages, c.expiry, "score_bucket_plans_cap1_dual");
            let raw = &scoring.outputs[if c.direction == 1 {
                "buy_output"
            } else {
                "sell_output"
            }]
            .i64s()[position * 21..(position + 1) * 21];
            let stats = &counts[&c.id];
            for (slot, key) in [(0, "raw_signals"), (1, "wins"), (2, "losses"), (3, "ties")] {
                assert_eq!(raw[slot], stats[key] as i64, "{} {key}", c.id);
            }
            assert_eq!(
                raw[4],
                (stats["invalidated"]
                    + stats["blocked_by_strategy_capacity"]
                    + stats["blocked_by_entry_gap"]) as i64
            );
            assert_eq!(raw[7], raw[1] * 92 - raw[2] * 100);
            for (&key, &v) in stats {
                *totals.entry(key).or_default() += v;
            }
            println!(
                "candidate {} pre={} post={} kernel_invalid={} counts={}",
                c.id,
                pre.len(),
                post.len(),
                raw[4],
                json!(stats)
            );
        }
        for (&key, &total) in &totals {
            assert_eq!(
                total,
                p.phase06["totals"][key].as_u64().unwrap(),
                "total {key}"
            );
        }
        println!("connected proof totals: {}", json!(totals));
        json!({"candidates":counts,"totals":totals,"unclassified_differences":0})
    }

    /// File publication is create-only, including receipts and the final manifest.
    fn create(path: &Path, bytes: &[u8]) {
        let mut file = File::create_new(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
    }
    fn json_bytes(v: &Value) -> Vec<u8> {
        let mut bytes = serde_json::to_vec_pretty(v).unwrap();
        bytes.push(b'\n');
        bytes
    }
    fn device_info(device: &Device) -> Value {
        let info = device.info();
        json!({"name":info.name,"compute_capability":info.compute_capability,"driver_version":info.driver_version,"module_build":serde_json::from_str::<Value>(MODULE_BUILD).unwrap(),"module_sha256":digest(MODULE_CUBIN)})
    }
    fn kernel_digests() -> BTreeMap<&'static str, String> {
        KERNEL_SOURCES
            .iter()
            .map(|&(symbol, source)| (symbol, digest(source.as_bytes())))
            .collect()
    }

    /// Refuse stale build metadata as well as dirty or unidentifiable working trees.
    fn clean_revision() -> String {
        let git = |args: &[&str]| {
            let out = Command::new("git").args(args).output().unwrap();
            assert!(out.status.success());
            String::from_utf8(out.stdout).unwrap().trim().to_string()
        };
        let revision = git(&["rev-parse", "HEAD"]);
        assert_eq!(
            revision,
            env!("BINARY_ALPHA_CODE_REVISION"),
            "rebuild at the clean reviewed commit"
        );
        assert!(
            git(&["status", "--porcelain"]).is_empty(),
            "capture/parity requires a clean committed tree; the primary must commit first"
        );
        revision
    }

    /// Hash the four source files and four tests at the pinned commit, verifying the
    /// readable checkout bytes against `git show` without altering its Git state.
    fn legacy_sources() -> Value {
        let mut files = BTreeMap::new();
        for path in SOURCE_FILES {
            let pinned = Command::new("git")
                .args([
                    "-C",
                    "/mnt/data/rexi3",
                    "show",
                    &format!("{LEGACY_COMMIT}:{path}"),
                ])
                .output()
                .unwrap();
            assert!(pinned.status.success());
            let disk = fs::read(Path::new("/mnt/data/rexi3").join(path)).unwrap();
            assert_eq!(
                digest(&disk),
                digest(&pinned.stdout),
                "legacy checkout differs from pinned file {path}"
            );
            files.insert(path, digest(&disk));
        }
        json!(files)
    }

    /// Measure one warm-up plus five repetitions. Oracle comparisons and publication are
    /// outside the timers; shared search inputs stay resident across a stage's launches.
    fn measure(backend: &Backend, cases: &[Case]) -> (Vec<Case>, Value) {
        let Backend::Cuda(device) = backend else {
            panic!("device evidence requires CUDA")
        };
        let mut samples = Vec::new();
        let mut recorded = Vec::new();
        for iteration in 0..6 {
            let (free_before, total_memory) = device.memory_info().unwrap();
            let start = Instant::now();
            let resident =
                if cases[0].id.starts_with("governed-") && cases[0].symbol.contains("cap1") {
                    Some(
                        device
                            .search_workspace(
                                search_buffers(&cases[0].inputs),
                                &[cases[0].inputs["split_mask"].u8s()],
                            )
                            .unwrap(),
                    )
                } else {
                    None
                };
            let mut upload = resident
                .as_ref()
                .map_or(0.0, |r| r.timings.upload.as_secs_f64());
            let mut execute = 0.0;
            let mut download = 0.0;
            let mut decode = 0.0;
            let mut allocated = 0;
            let mut outputs = Vec::new();
            for case in cases {
                let measured = if let Some(workspace) = &resident {
                    run_resident(workspace, case)
                } else {
                    run(backend, case)
                };
                upload += measured.timings.upload.as_secs_f64();
                execute += measured.timings.execute.as_secs_f64();
                download += measured.timings.download.as_secs_f64();
                decode += measured.decode.as_secs_f64();
                allocated = allocated.max(measured.timings.allocated_bytes);
                outputs.push(measured.output);
            }
            let total = start.elapsed().as_secs_f64();
            let (free_after, _) = device.memory_info().unwrap();
            drop(resident);
            // All checks happen after the measured interval, including the warm-up checks.
            for (index, (case, output)) in cases.iter().zip(&outputs).enumerate() {
                let cpu = run(&Backend::Cpu, case).output;
                compare(&format!("{} CPU/device", case.id), output, &cpu);
                if iteration > 0 {
                    let previous: &Case = &recorded[index];
                    compare(
                        &format!("{} repeated bits", case.id),
                        output,
                        &previous.outputs,
                    );
                }
            }
            if iteration == 0 {
                recorded = cases
                    .iter()
                    .zip(outputs)
                    .map(|(c, output)| Case {
                        outputs: output,
                        ..c.clone()
                    })
                    .collect();
            } else {
                samples.push(json!({"upload_seconds":upload,"execute_seconds":execute,"download_seconds":download,"decode_seconds":decode,"total_seconds":total,"allocated_device_buffer_bytes":allocated,"driver_used_memory_delta_bytes":free_before as i128-free_after as i128,"driver_total_memory_bytes":total_memory}));
            }
        }
        let mut medians = serde_json::Map::new();
        for name in [
            "upload_seconds",
            "execute_seconds",
            "download_seconds",
            "decode_seconds",
            "total_seconds",
            "allocated_device_buffer_bytes",
            "driver_used_memory_delta_bytes",
        ] {
            let mut values: Vec<f64> = samples.iter().map(|s| s[name].as_f64().unwrap()).collect();
            values.sort_by(f64::total_cmp);
            medians.insert(name.into(), json!(values[2]));
        }
        let receipt = json!({"stage":cases[0].id,"cases":cases.iter().map(|c|&c.id).collect::<Vec<_>>(),"warmups":1,"repetitions":5,"statistic":"median","synchronization":"included in upload, execute and download; execution ends after stream synchronization","allocated_bytes_semantics":"maximum simultaneous bytes reported by an operation, including resident shared inputs","driver_delta_semantics":"used memory after launches before workspace release minus before workspace allocation","medians":medians,"samples":samples,"test_process_peak_kb":in_process_peak_kb()});
        println!(
            "stage {} launches={} median total={:.6} s allocated={} bytes driver delta={} bytes",
            cases[0].id,
            cases.len(),
            receipt["medians"]["total_seconds"].as_f64().unwrap(),
            receipt["medians"]["allocated_device_buffer_bytes"],
            receipt["medians"]["driver_used_memory_delta_bytes"]
        );
        (recorded, receipt)
    }

    /// Record every scalar and buffer as little-endian bytes under a single case directory.
    fn write_cases(root: &Path, stages: &[Vec<Case>]) -> Value {
        fs::create_dir(root.join("buffers")).unwrap();
        let mut records = Vec::new();
        for cases in stages {
            let mut stage = Vec::new();
            for case in cases {
                safe_component(&case.id);
                fs::create_dir(root.join("buffers").join(&case.id)).unwrap();
                let mut record = json!({"id":case.id,"symbol":case.symbol});
                for (direction, buffers) in [("inputs", &case.inputs), ("outputs", &case.outputs)] {
                    let mut saved = BTreeMap::new();
                    for (name, buffer) in buffers {
                        safe_component(name);
                        let relative = format!("buffers/{}/{}-{name}.bin", case.id, direction);
                        let bytes = buffer.bytes();
                        create(&root.join(&relative), &bytes);
                        saved.insert(name,json!({"path":relative,"element_type":buffer.dtype(),"shape":buffer.shape,"bytes":bytes.len(),"sha256":digest(&bytes)}));
                    }
                    record[direction] = json!(saved);
                }
                stage.push(record);
            }
            records.push(stage);
        }
        json!(records)
    }
    fn safe_component(name: &str) {
        assert_eq!(Path::new(name).components().count(), 1);
        assert!(matches!(
            Path::new(name).components().next(),
            Some(Component::Normal(_))
        ));
    }

    /// Verify identity before parsing, then verify every buffer's path, bytes, shape and hash.
    /// This routine never creates a file or follows a reference symlink outside its root.
    fn read_reference(path: &Path) -> (Value, Value, Vec<Vec<Case>>, Value) {
        let pin = manifest_json(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/phase07_reference.json"),
        );
        assert_eq!(
            sha256(path),
            pin["reference_identity"].as_str().unwrap(),
            "substituted reference manifest"
        );
        let manifest = manifest_json(path);
        assert_eq!(manifest["schema_version"], 1);
        assert_eq!(manifest["source_commit"], LEGACY_COMMIT);
        assert_eq!(manifest["target_commit"], pin["target_commit"]);
        assert_eq!(manifest["comparison_rules"], json!(COMPARISONS));
        assert_eq!(
            manifest["kernel_source_sha256"],
            pin["kernel_source_sha256"]
        );
        assert_eq!(manifest["summary"], pin["summary"]);
        assert_eq!(manifest["stage_summary"], pin["stage_summary"]);
        assert_eq!(
            manifest["literal_fixture_sha256"],
            sha256(
                &Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures/phase07_legacy_cases.json")
            )
        );
        let root = path.parent().unwrap().canonicalize().unwrap();
        let baseline_path = root.join("capture-run.json");
        assert!(baseline_path.canonicalize().unwrap().starts_with(&root));
        assert_eq!(
            sha256(&baseline_path),
            pin["capture_run_sha256"].as_str().unwrap(),
            "substituted extraction timing receipt"
        );
        let baseline = manifest_json(&baseline_path);
        assert_eq!(baseline["reference_identity"], pin["reference_identity"]);
        let mut seen = BTreeSet::new();
        let mut stages = Vec::new();
        for stage in manifest["stages"].as_array().unwrap() {
            let mut cases = Vec::new();
            for c in stage.as_array().unwrap() {
                let id = c["id"].as_str().unwrap();
                safe_component(id);
                assert!(seen.insert(id.to_string()));
                let read = |direction: &str| {
                    let mut buffers = Buffers::new();
                    for (name, b) in c[direction].as_object().unwrap() {
                        safe_component(name);
                        let relative = format!("buffers/{id}/{direction}-{name}.bin");
                        assert_eq!(b["path"], relative);
                        let path = root.join(&relative);
                        assert!(path.canonicalize().unwrap().starts_with(&root));
                        let bytes = fs::read(&path).unwrap();
                        assert_eq!(bytes.len() as u64, b["bytes"].as_u64().unwrap());
                        assert_eq!(digest(&bytes), b["sha256"].as_str().unwrap(), "{relative}");
                        let shape = b["shape"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|v| usize::try_from(v.as_u64().unwrap()).unwrap())
                            .collect();
                        buffers.insert(
                            name.clone(),
                            Buffer::from_bytes(b["element_type"].as_str().unwrap(), shape, &bytes),
                        );
                    }
                    buffers
                };
                cases.push(Case {
                    id: id.into(),
                    symbol: c["symbol"].as_str().unwrap().into(),
                    inputs: read("inputs"),
                    outputs: read("outputs"),
                });
            }
            assert!(!cases.is_empty());
            stages.push(cases);
        }
        assert_eq!(symbol_cases(&stages), manifest["kernel_cases"]);
        (manifest, baseline, stages, pin)
    }
    fn symbol_cases(stages: &[Vec<Case>]) -> Value {
        let mut symbols: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for case in stages.iter().flatten() {
            symbols.entry(&case.symbol).or_default().push(&case.id);
        }
        assert_eq!(
            symbols.keys().copied().collect::<BTreeSet<_>>(),
            KERNEL_SOURCES.iter().map(|k| k.0).collect()
        );
        json!(symbols)
    }

    /// Select recorded stages by exact IDs and prove freshly prepared inputs equal them.
    fn bound_stage<'a>(prepared: &[Case], recorded: &'a [Vec<Case>]) -> &'a [Case] {
        let stage = recorded
            .iter()
            .find(|s| s[0].id == prepared[0].id)
            .expect("reference has no matching stage");
        assert_eq!(stage.len(), prepared.len());
        for (a, b) in prepared.iter().zip(stage) {
            assert_eq!((&a.id, &a.symbol), (&b.id, &b.symbol));
            compare(
                &format!("{} prepared/recorded inputs", a.id),
                &a.inputs,
                &b.inputs,
            );
        }
        stage
    }

    /// Freeze per-launch dimensions and diagnostic counters alongside the stage byte sizes.
    fn stage_summary(cases: &[Case]) -> Value {
        let launches: Vec<_> = cases
            .iter()
            .map(|case| {
                let dimensions: BTreeMap<_, _> = case
                    .inputs
                    .iter()
                    .filter_map(|(name, b)| {
                        if !b.shape.is_empty() {
                            return None;
                        }
                        let value = match &b.data {
                            Data::I32(v) => i64::from(v[0]),
                            Data::I64(v) => v[0],
                            _ => panic!("unexpected scalar type"),
                        };
                        Some((name.clone(), value))
                    })
                    .collect();
                let mut counts = BTreeMap::new();
                for (name, b) in &case.outputs {
                    if case.symbol.starts_with("score_bucket") {
                        let width = if case.symbol.contains("basic") { 8 } else { 21 };
                        for (slot, field) in ["signals", "wins", "losses", "ties", "invalid"]
                            .iter()
                            .enumerate()
                        {
                            counts.insert(
                                format!("{name}/{field}"),
                                b.i64s()
                                    .chunks_exact(width)
                                    .map(|row| row[slot])
                                    .sum::<i64>(),
                            );
                        }
                    } else if b.dtype() == "uint8" {
                        counts.insert(
                            format!("{name}/membership"),
                            b.u8s().iter().filter(|&&v| v != 0).count() as i64,
                        );
                    } else if b.dtype() == "int32" {
                        counts.insert(
                            name.clone(),
                            b.i32s().iter().map(|&v| i64::from(v)).sum::<i64>(),
                        );
                    }
                }
                json!({"case":case.id,"symbol":case.symbol,"dimensions":dimensions,"counts":counts})
            })
            .collect();
        json!({"stage":cases[0].id,"launches":launches,"input_bytes":cases.iter().flat_map(|c|c.inputs.values()).map(|b|b.bytes().len()).sum::<usize>(),"output_bytes":cases.iter().flat_map(|c|c.outputs.values()).map(|b|b.bytes().len()).sum::<usize>()})
    }

    /// Capture at the extraction commit or replay the immutable typed reference at the
    /// current clean commit. The two modes share preparation, device runs and ledger proof.
    pub(super) fn execute(capture: bool) {
        if cfg!(debug_assertions) {
            panic!("capture/parity requires the optimized release profile");
        }
        let revision = clean_revision();
        let started = Instant::now();
        let output = if capture {
            let path = PathBuf::from(
                std::env::var("BINARY_ALPHA_CUDA_REFERENCE_OUTPUT")
                    .expect("capture output must name a new directory"),
            );
            fs::create_dir(&path).unwrap_or_else(|e| {
                panic!("capture directory must not exist: {}: {e}", path.display())
            });
            Some(path)
        } else {
            None
        };
        let reference = (!capture).then(|| {
            read_reference(Path::new(
                &std::env::var("BINARY_ALPHA_CUDA_REFERENCE").expect("reference.json is required"),
            ))
        });
        let prepared = prepare();
        if let Some((manifest, _, _, _)) = &reference {
            assert_eq!(
                manifest["inputs"], prepared.identities,
                "frozen configuration, generations, strategies or encoding changed"
            );
        }
        let backend = Backend::cuda(0).unwrap();
        let Backend::Cuda(device) = &backend else {
            unreachable!()
        };
        let environment = device_info(device);
        let f = fixture();
        let literals = literal_cases(&f);
        if capture {
            for &(symbol, expected) in LEGACY_KERNEL_DIGESTS {
                assert_eq!(
                    kernel_digests()[symbol],
                    expected,
                    "capture requires unchanged extracted kernels"
                );
            }
        }
        let mut expected_stages: Vec<Vec<Case>> =
            literals.iter().cloned().map(|c| vec![c]).collect();
        expected_stages.extend(prepared.stages.clone());
        let mut completed = Vec::new();
        let mut receipts = Vec::new();
        for stage in &expected_stages {
            let source = if let Some((_, _, recorded, _)) = &reference {
                bound_stage(stage, recorded)
            } else {
                stage
            };
            let (result, receipt) = measure(&backend, source);
            if !capture || !stage[0].id.starts_with("governed-") {
                for (a, b) in result.iter().zip(source) {
                    compare(&format!("{} recorded output", a.id), &a.outputs, &b.outputs);
                }
            }
            completed.push(result);
            receipts.push(receipt);
        }
        let literal_results: Vec<_> = completed
            .iter()
            .take(literals.len())
            .map(|s| s[0].outputs.clone())
            .collect();
        independent_expectations(&f, &literals, &literal_results);
        for stage in analysis_cases(&prepared, &completed) {
            let source = if let Some((_, _, recorded, _)) = &reference {
                bound_stage(&stage, recorded)
            } else {
                &stage
            };
            let (result, receipt) = measure(&backend, source);
            if !capture {
                for (a, b) in result.iter().zip(source) {
                    compare(&a.id, &a.outputs, &b.outputs);
                }
            }
            completed.push(result);
            receipts.push(receipt);
        }
        if let Some((_, _, recorded, _)) = &reference {
            assert_eq!(
                completed
                    .iter()
                    .flatten()
                    .map(|c| &c.id)
                    .collect::<Vec<_>>(),
                recorded.iter().flatten().map(|c| &c.id).collect::<Vec<_>>()
            );
        }
        let (ledger, signals, replay_receipt) = replay(&prepared);
        let summary = connected(&prepared, &completed, &signals);
        let ledger_identity = json!({"event_count":ledger.events,"final_state_identity":ledger.final_state_identity,"summary_identity":ledger.summary_identity});
        if let Some((manifest, _, _, _)) = &reference {
            assert_eq!(
                manifest["ledger"], ledger_identity,
                "canonical Engine replay identity changed"
            );
            assert_eq!(manifest["summary"], summary);
        }
        let stage_summary: Vec<_> = completed.iter().map(|s| stage_summary(s)).collect();
        println!("per-stage summary counts: {}", json!(stage_summary));
        if let Some((manifest, _, _, _)) = &reference {
            assert_eq!(manifest["stage_summary"], json!(stage_summary));
        }
        let mut receipt = json!({"schema_version":1,"target_commit":revision,"source_commit":LEGACY_COMMIT,"device":environment,"stages":receipts,"replay":replay_receipt,"test_process_peak_kb":in_process_peak_kb(),"whole_test_wall_seconds":started.elapsed().as_secs_f64(),"summary":summary,"stage_summary":stage_summary,"timing_comparison":"Rust extraction baseline; original CuPy host comparison is unavailable here","production_operator_tasks":"none","linked_matching_sentry_issues":"none"});
        // The median of the five complete repetition totals, preserving stage boundaries.
        let mut path_times = [0_f64; 5];
        for stage in receipt["stages"].as_array().unwrap() {
            for (i, sample) in stage["samples"].as_array().unwrap().iter().enumerate() {
                path_times[i] += sample["total_seconds"].as_f64().unwrap();
            }
        }
        path_times.sort_by(f64::total_cmp);
        receipt["completed_path_median_seconds"] = json!(path_times[2]);
        if let Some(root) = output {
            let stages = write_cases(&root, &completed);
            let manifest = json!({"schema_version":1,"source_commit":LEGACY_COMMIT,"target_commit":revision,"legacy_sources":legacy_sources(),"literal_fixture_sha256":sha256(&Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/phase07_legacy_cases.json")),"comparison_rules":COMPARISONS,"kernel_cases":symbol_cases(&completed),"kernel_source_sha256":kernel_digests(),"inputs":prepared.identities,"device":environment,"ledger":ledger_identity,"summary":summary,"stage_summary":stage_summary,"stages":stages});
            let bytes = json_bytes(&manifest);
            let identity = digest(&bytes);
            receipt["reference_identity"] = json!(identity);
            let receipt_bytes = json_bytes(&receipt);
            create(&root.join("capture-run.json"), &receipt_bytes);
            create(&root.join("reference.json"), &bytes);
            let pin = json!({"schema_version":1,"reference_identity":identity,"capture_run_sha256":digest(&receipt_bytes),"source_commit":LEGACY_COMMIT,"target_commit":revision,"summary":summary,"stage_summary":stage_summary,"kernel_source_sha256":kernel_digests()});
            create(
                &Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures/phase07_reference.json"),
                &json_bytes(&pin),
            );
            println!(
                "reference identity {identity}\nreference {}\ncapture receipt SHA-256 {}",
                root.join("reference.json").display(),
                digest(&receipt_bytes)
            );
        } else {
            let (_, baseline, _, pin) = reference.unwrap();
            let ratio = path_times[2] / baseline["completed_path_median_seconds"].as_f64().unwrap();
            receipt["reference_identity"] = pin["reference_identity"].clone();
            receipt["extraction_timing_ratio"] = json!(ratio);
            receipt["timing_status"] = json!(if ratio > 1.10 {
                "regression"
            } else {
                "within 1.10"
            });
            println!(
                "completed path median {:.6} s; extraction baseline ratio {ratio:.6}: {}",
                path_times[2],
                if ratio > 1.10 {
                    "regression"
                } else {
                    "within 1.10"
                }
            );
            let path = prepared.scratch.path("capture-run.json");
            let bytes = json_bytes(&receipt);
            create(&path, &bytes);
            println!("run receipt {} SHA-256 {}", path.display(), digest(&bytes));
        }
    }
}

#[cfg(feature = "cuda")]
#[test]
#[ignore]
fn capture_legacy_reference() {
    governed::execute(true);
}

#[cfg(feature = "cuda")]
#[test]
#[ignore]
fn governed_parity() {
    governed::execute(false);
}
