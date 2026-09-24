//! Literal kernel contracts from the source-test inventory; no generated random fixtures.

use crate::{
    Backend,
    bootstrap::bootstrap_path_metrics,
    portfolio::{path_drawdown, replay_capacity},
    repair::{Replay, replay_policies},
    search::*,
};

fn bits(value: f64) -> i64 {
    value.to_bits() as i64
}

fn blocks_cover_columns(blocks: &[ColumnBlock], columns: usize) -> bool {
    blocks.first().is_some_and(|block| block.columns.start == 0)
        && blocks
            .last()
            .is_some_and(|block| block.columns.end == columns)
        && blocks.iter().all(|block| !block.columns.is_empty())
        && blocks
            .windows(2)
            .all(|pair| pair[0].columns.end == pair[1].columns.start)
}

#[test]
fn column_blocks_bound_memory_and_sparse_indices() {
    let plan = plan_column_blocks(&[4; 6], 4, 2, 1, 1, 1, 600).unwrap();
    assert!(plan.blocks.len() >= 3, "forced small budget: {plan:?}");
    assert!(blocks_cover_columns(&plan.blocks, 6));
    assert!(
        plan_column_blocks(&[4], 4, 2, 1, 1, 1, 1)
            .unwrap_err()
            .contains("one-column")
    );
    assert!(
        plan_column_blocks(&[i32::MAX as usize + 1], 4, 1, 1, 1, 1, usize::MAX)
            .unwrap_err()
            .contains("exceeds i32")
    );
    assert!(
        plan_column_blocks(&[i32::MAX as usize], 4, 2, 1, 1, 1, usize::MAX)
            .unwrap_err()
            .contains("i32 sparse bounds")
    );
}

#[test]
fn column_blocks_count_physical_pool_reservation() {
    // One row, one column, and one batch request 405 bytes in total.
    let unit = 32 * 1024 * 1024;
    let required = unit;
    let plan = plan_column_blocks_with_granularity(&[1], 1, 1, 1, 1, 1, (required, unit)).unwrap();
    assert_eq!(plan.blocks[0].columns, 0..1);
    assert!(
        plan_column_blocks_with_granularity(&[1], 1, 1, 1, 1, 1, (required - 1, unit))
            .unwrap_err()
            .contains("one-column")
    );
}

#[test]
fn column_blocks_split_before_sparse_key_count_exceeds_i32() {
    let plan = plan_column_blocks(&[0; 32_768], 1, 65_536, 1, 1, 1, 14_000_000_000).unwrap();
    assert!(plan.blocks.len() > 1);
    assert!(blocks_cover_columns(&plan.blocks, 32_768));
    assert!(
        plan.blocks
            .iter()
            .all(|block| block.columns.len() * 65_536 <= i32::MAX as usize)
    );
}

#[test]
fn column_block_coverage_detects_skipped_column() {
    let skipped = [
        ColumnBlock { columns: 0..1 },
        ColumnBlock { columns: 2..3 },
        ColumnBlock { columns: 3..6 },
    ];
    assert_eq!(skipped.first().unwrap().columns.start, 0);
    assert_eq!(skipped.last().unwrap().columns.end, 6);
    assert_eq!(skipped.len(), 3);
    assert!(!blocks_cover_columns(&skipped, 6));
}

#[test]
fn sparse_tuple_rejects_ordered_rows_beyond_planned_residency() {
    let plan = plan_column_blocks(&[1], 1, 1, 1, 1, 1, 405).unwrap();
    assert_eq!(plan.blocks[0].columns, 0..1);
    let ordered_rows = [0; 1_000];
    let request = crate::search::Request {
        kind: 6,
        buffers: SearchBuffers {
            feature_codes: &[0],
            feature_count: 1,
            row_count: 1,
            ordered_rows: &ordered_rows,
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
            condition_feature: &[0],
            condition_bucket: &[0],
            candidate_offsets: &[0, 1],
            candidate_count: 1,
        },
        sparse: Some(SparseIndex {
            candidate_driver_key: &[0],
            key_chrono_offsets: &[0, 1],
            key_chrono_rows: &[0],
        }),
        expiry_ms: 1,
        direction_code: 1,
        payout_basis: 92,
    };
    assert!(request.validate().unwrap_err().contains("ordered_rows"));
}

#[test]
fn cpu_resident_sparse_batches_match_one_shot_across_forced_blocks_and_expiries() {
    let case = &search_cases()[0];
    let rows = case.entry.len();
    let plan = plan_column_blocks(&[rows; 6], rows, 2, 1, 1, 1, 600).unwrap();
    assert!(plan.blocks.len() >= 3);
    assert!(blocks_cover_columns(&plan.blocks, 6));
    let ordered: Vec<i64> = (0..rows as i64).collect();
    let sparse_rows: Vec<i32> = (0..rows as i32).collect();
    let offsets = [0, rows as i32];
    let split_masks = [&case.split[..]];
    let before = CpuSparseTuple::construction_count();
    for block in &plan.blocks {
        let codes: Vec<i16> = (0..block.columns.len())
            .flat_map(|column| (0..rows).map(move |row| ((column + row) % 2) as i16))
            .collect();
        let buy_variants = [case.buy.clone(), vec![0; rows], vec![1; rows]];
        let release_variants = [
            case.release.clone(),
            case.release.iter().map(|time| time + 200).collect(),
            case.release.iter().map(|time| time + 400).collect(),
        ];
        let buffers = |variant: usize| SearchBuffers {
            feature_codes: &codes,
            feature_count: block.columns.len() as i32,
            row_count: rows as i32,
            ordered_rows: &ordered,
            decision_time_ms: &case.entry,
            release_time_ms: &release_variants[variant],
            settlement_time_ms: &case.settlement,
            valid: &case.valid,
            buy_win: &buy_variants[variant],
            sell_win: &case.sell,
            tie: &case.tie,
        };
        let mut tuple = CpuSparseTuple::new(
            buffers(0),
            &split_masks,
            SparseKeys {
                key_chrono_offsets: &offsets,
                key_chrono_rows: &sparse_rows,
            },
        )
        .unwrap();
        for bucket in [0_i16, 1] {
            let features = [0_i32];
            let buckets = [bucket];
            let candidate_offsets = [0_i32, 1];
            let drivers = [0_i32];
            let candidates = CandidateConditions {
                condition_feature: &features,
                condition_bucket: &buckets,
                candidate_offsets: &candidate_offsets,
                candidate_count: 1,
            };
            for (variant, expiry) in [1, case.expiry_ms, 120_000].into_iter().enumerate() {
                tuple.set_outcome(buffers(variant), &case.split).unwrap();
                let resident = tuple
                    .score_batch(0, candidates, &drivers, expiry, case.payout)
                    .unwrap();
                let one_shot = score_bucket_plans_cap1_sparse_dual(
                    &Backend::Cpu,
                    &codes,
                    &features,
                    &buckets,
                    &candidate_offsets,
                    &drivers,
                    &offsets,
                    &sparse_rows,
                    &case.split,
                    &case.entry,
                    &release_variants[variant],
                    &case.settlement,
                    &case.valid,
                    &buy_variants[variant],
                    &case.sell,
                    &case.tie,
                    1,
                    rows as i32,
                    expiry,
                    case.payout,
                )
                .unwrap();
                assert_eq!(resident.output, one_shot.output);
            }
        }
    }
    assert_eq!(
        CpuSparseTuple::construction_count() - before,
        plan.blocks.len()
    );
}

#[cfg(feature = "cuda")]
#[test]
fn cuda_resident_tuple_matches_cpu_reference() {
    let Ok(device) = crate::cuda::Device::open(0) else {
        return;
    };
    let case = &search_cases()[0];
    let rows = case.entry.len();
    assert!(
        !device
            .memory_info()
            .and_then(|(free, _)| plan_column_blocks(&[rows], rows, 1, 1, 1, 1, free))
            .unwrap()
            .blocks
            .is_empty()
    );
    let codes = vec![0_i16; rows];
    let ordered: Vec<i64> = (0..rows as i64).collect();
    let sparse_rows: Vec<i32> = (0..rows as i32).collect();
    let offsets = [0, rows as i32];
    let alternate_release: Vec<i64> = case.release.iter().map(|time| time + 200).collect();
    let alternate_buy = vec![0_u8; rows];
    let buffers = SearchBuffers {
        feature_codes: &codes,
        feature_count: 1,
        row_count: rows as i32,
        ordered_rows: &ordered,
        decision_time_ms: &case.entry,
        release_time_ms: &case.release,
        settlement_time_ms: &case.settlement,
        valid: &case.valid,
        buy_win: &case.buy,
        sell_win: &case.sell,
        tie: &case.tie,
    };
    let mut tuple = device
        .search_tuple_workspace(
            buffers,
            &[&case.split],
            SparseKeys {
                key_chrono_offsets: &offsets,
                key_chrono_rows: &sparse_rows,
            },
        )
        .unwrap();
    let features = [0_i32];
    let buckets = [0_i16];
    let candidate_offsets = [0_i32, 1];
    let drivers = [0_i32];
    let batch = tuple
        .upload_batch(
            CandidateConditions {
                condition_feature: &features,
                condition_bucket: &buckets,
                candidate_offsets: &candidate_offsets,
                candidate_count: 1,
            },
            &drivers,
        )
        .unwrap();
    for expiry in [1, case.expiry_ms, 120_000] {
        let actual = batch.score_sparse_dual(0, expiry, case.payout).unwrap();
        let expected = score_bucket_plans_cap1_sparse_dual(
            &Backend::Cpu,
            &codes,
            &features,
            &buckets,
            &candidate_offsets,
            &drivers,
            &offsets,
            &sparse_rows,
            &case.split,
            &case.entry,
            &case.release,
            &case.settlement,
            &case.valid,
            &case.buy,
            &case.sell,
            &case.tie,
            1,
            rows as i32,
            expiry,
            case.payout,
        )
        .unwrap();
        assert_eq!(actual.output, expected.output);
    }
    drop(batch);
    let alternate = SearchBuffers {
        release_time_ms: &alternate_release,
        buy_win: &alternate_buy,
        ..buffers
    };
    let transfer = tuple.set_outcome(alternate, &case.split).unwrap();
    assert!(transfer.upload > std::time::Duration::ZERO);
    assert_eq!(transfer.allocated_bytes, tuple.timings.allocated_bytes);
    let batch = tuple
        .upload_batch(
            CandidateConditions {
                condition_feature: &features,
                condition_bucket: &buckets,
                candidate_offsets: &candidate_offsets,
                candidate_count: 1,
            },
            &drivers,
        )
        .unwrap();
    let actual = batch
        .score_sparse_dual(0, case.expiry_ms, case.payout)
        .unwrap();
    let expected = score_bucket_plans_cap1_sparse_dual(
        &Backend::Cpu,
        &codes,
        &features,
        &buckets,
        &candidate_offsets,
        &drivers,
        &offsets,
        &sparse_rows,
        &case.split,
        &case.entry,
        &alternate_release,
        &case.settlement,
        &case.valid,
        &alternate_buy,
        &case.sell,
        &case.tie,
        1,
        rows as i32,
        case.expiry_ms,
        case.payout,
    )
    .unwrap();
    assert_eq!(actual.output, expected.output);
}

struct SearchCase {
    entry: Vec<i64>,
    release: Vec<i64>,
    settlement: Vec<i64>,
    valid: Vec<u8>,
    buy: Vec<u8>,
    sell: Vec<u8>,
    tie: Vec<u8>,
    split: Vec<u8>,
    payout: i64,
    /// The legacy launch scalar the kernels receive and ignore.
    expiry_ms: i64,
    expected_buy: [i64; 21],
    expected_sell: [i64; 21],
    mask: Vec<u8>,
}

fn search_cases() -> Vec<SearchCase> {
    let base = 1770000000000_i64;
    let mut cases = Vec::new();
    // TS-A: equity [-100,-8,84,-16], drawdowns [100,8,0,100]. The first
    // episode recovers after three trades, 150 seconds from the initial entry.
    let entry = (0..4).map(|i| base + i * 60000).collect::<Vec<_>>();
    cases.push(SearchCase {
        release: entry.iter().map(|t| t + 30000).collect(),
        settlement: entry.iter().map(|t| t + 30000).collect(),
        entry,
        valid: vec![1; 4],
        buy: vec![0, 1, 1, 0],
        sell: vec![1, 0, 0, 1],
        tie: vec![0; 4],
        split: vec![1; 4],
        payout: 92,
        expiry_ms: 30000,
        expected_buy: [
            4,
            2,
            2,
            0,
            0,
            4,
            0,
            -16,
            100,
            1,
            bits(20064.0),
            2,
            3,
            0,
            0,
            0,
            4,
            36928,
            20000,
            120000,
            150000,
        ],
        expected_sell: [
            4,
            2,
            2,
            0,
            0,
            0,
            4,
            -16,
            200,
            2,
            bits(61664.0),
            3,
            3,
            0,
            0,
            0,
            4,
            36928,
            20000,
            120000,
            180000,
        ],
        mask: vec![1; 4],
    });
    // TS-B: an equal recovery and then a tie refresh the peak's clock. The
    // later buy drawdown is 120 seconds, not the intervening 100-hour gap.
    let entry = vec![
        base,
        base + 60000,
        base + 360000000,
        base + 360060000,
        base + 360120000,
    ];
    cases.push(SearchCase {
        release: entry.iter().map(|t| t + 30000).collect(),
        settlement: entry.iter().map(|t| t + 30000).collect(),
        entry,
        valid: vec![1; 5],
        buy: vec![0, 1, 0, 0, 1],
        sell: vec![1, 0, 0, 1, 0],
        tie: vec![0, 0, 1, 0, 0],
        split: vec![1; 5],
        payout: 100,
        expiry_ms: 30000,
        expected_buy: [
            5,
            2,
            2,
            1,
            0,
            5,
            0,
            0,
            100,
            1,
            bits(20000.0),
            1,
            2,
            0,
            0,
            0,
            5,
            40000,
            20000,
            60000,
            120000,
        ],
        expected_sell: [
            5,
            2,
            2,
            1,
            0,
            0,
            5,
            0,
            100,
            1,
            bits(30000.0),
            2,
            3,
            0,
            0,
            0,
            5,
            40000,
            20000,
            360000000,
            360060000,
        ],
        mask: vec![1; 5],
    });
    // TS-C: same equivalence contract as the legacy eight-way device test,
    // with all raw fields independently fixed here.
    let entry = (0..5).map(|i| base + i * 60000).collect::<Vec<_>>();
    cases.push(SearchCase {
        release: entry.iter().map(|t| t + 30000).collect(),
        settlement: entry.iter().map(|t| t + 30000).collect(),
        entry,
        valid: vec![1; 5],
        buy: vec![0, 1, 1, 0, 1],
        sell: vec![1, 0, 0, 1, 0],
        tie: vec![0; 5],
        split: vec![1; 5],
        payout: 92,
        expiry_ms: 30000,
        expected_buy: [
            5,
            3,
            2,
            0,
            0,
            5,
            0,
            76,
            100,
            1,
            bits(20128.0),
            2,
            3,
            0,
            0,
            0,
            5,
            45392,
            20000,
            120000,
            150000,
        ],
        expected_sell: [
            5,
            2,
            3,
            0,
            0,
            0,
            5,
            -116,
            208,
            2,
            bits(104928.0),
            4,
            4,
            0,
            0,
            0,
            5,
            46928,
            30000,
            180000,
            240000,
        ],
        mask: vec![1; 5],
    });
    // TS-E: row 0 is invalid but holds capacity until 70000. Only rows 3
    // and 5 settle; the raw count includes all six matching signals.
    cases.push(SearchCase {
        entry: vec![1000, 2000, 3000, 70000, 71000, 74000],
        release: vec![70000, 70000, 0, 74000, 74000, 76000],
        settlement: vec![0, 0, 0, 74000, 0, 76000],
        valid: vec![0, 0, 0, 1, 0, 1],
        buy: vec![0, 0, 0, 0, 0, 1],
        sell: vec![0, 0, 0, 1, 0, 0],
        tie: vec![0; 6],
        split: vec![1; 6],
        payout: 92,
        expiry_ms: 2000,
        expected_buy: [
            6,
            1,
            1,
            0,
            4,
            6,
            0,
            -8,
            100,
            1,
            bits(10064.0),
            2,
            2,
            0,
            0,
            0,
            2,
            18464,
            10000,
            2000,
            6000,
        ],
        expected_sell: [
            6,
            1,
            1,
            0,
            4,
            0,
            6,
            -8,
            100,
            1,
            bits(10000.0),
            1,
            1,
            0,
            0,
            0,
            2,
            18464,
            10000,
            0,
            2000,
        ],
        mask: vec![1, 0, 0, 1, 0, 1],
    });
    // TS-F: scope 2 opens row 0 only as warm-up. Row 1 is blocked; row 2
    // opens exactly when warm-up releases. No warm-up score or mask bit leaks.
    cases.push(SearchCase {
        entry: vec![1000, 2000, 3000],
        release: vec![3000, 4000, 5000],
        settlement: vec![3000, 4000, 5000],
        valid: vec![1; 3],
        buy: vec![1, 0, 1],
        sell: vec![0, 1, 0],
        tie: vec![0; 3],
        split: vec![2, 1, 1],
        payout: 92,
        expiry_ms: 2000,
        expected_buy: [
            2, 1, 0, 0, 1, 2, 0, 92, 0, 0, 0, 0, 0, 0, 0, 0, 1, 8464, 0, 0, 0,
        ],
        expected_sell: [
            2,
            0,
            1,
            0,
            1,
            0,
            2,
            -100,
            100,
            1,
            bits(10000.0),
            1,
            1,
            0,
            0,
            0,
            1,
            10000,
            10000,
            0,
            2000,
        ],
        mask: vec![0, 0, 1],
    });
    // A 105-trade all-loss path wraps the 100-slot ring. Squared drawdowns
    // sum to 10000 * (1² + ... + 105²) = 3914050000 exactly.
    let entry = (0..105).map(|i| base + i * 60000).collect::<Vec<_>>();
    cases.push(SearchCase {
        release: entry.iter().map(|t| t + 30000).collect(),
        settlement: entry.iter().map(|t| t + 30000).collect(),
        entry,
        valid: vec![1; 105],
        buy: vec![0; 105],
        sell: vec![1; 105],
        tie: vec![0; 105],
        split: vec![1; 105],
        payout: 92,
        expiry_ms: 30000,
        expected_buy: [
            105,
            0,
            105,
            0,
            0,
            105,
            0,
            -10500,
            10500,
            105,
            bits(3914050000.0),
            105,
            105,
            -2000,
            -5000,
            -10000,
            105,
            1050000,
            1050000,
            6240000,
            6270000,
        ],
        expected_sell: [
            105, 105, 0, 0, 0, 0, 105, 9660, 0, 0, 0, 0, 0, 1840, 4600, 9200, 105, 888720, 0, 0, 0,
        ],
        mask: vec![1; 105],
    });

    cases
}

fn search_literals(backend: &Backend) {
    for case in search_cases() {
        let rows = case.entry.len() as i32;
        let codes = vec![0_i16; rows as usize];
        let ordered = (0..i64::from(rows)).collect::<Vec<_>>();
        let sparse_rows = (0..rows).collect::<Vec<_>>();
        let offsets = [0, rows];
        for (direction, expected) in [(1, &case.expected_buy), (-1, &case.expected_sell)] {
            let result = score_bucket_plans_cap1(
                backend,
                &codes,
                &[0],
                &[0],
                &[0, 1],
                &case.split,
                &ordered,
                &case.entry,
                &case.release,
                &case.settlement,
                &case.valid,
                &case.buy,
                &case.sell,
                &case.tie,
                1,
                rows,
                1,
                case.expiry_ms,
                direction,
                case.payout,
            )
            .unwrap()
            .output;
            assert_eq!(
                result,
                expected[..21],
                "score_bucket_plans_cap1 direction {direction}"
            );
        }
        let result = score_bucket_plans_cap1_dual(
            backend,
            &codes,
            &[0],
            &[0],
            &[0, 1],
            &case.split,
            &ordered,
            &case.entry,
            &case.release,
            &case.settlement,
            &case.valid,
            &case.buy,
            &case.sell,
            &case.tie,
            1,
            rows,
            1,
            case.expiry_ms,
            case.payout,
        )
        .unwrap()
        .output;
        assert_eq!(
            result.buy_output,
            case.expected_buy[..21],
            "score_bucket_plans_cap1_dual buy"
        );
        assert_eq!(
            result.sell_output,
            case.expected_sell[..21],
            "score_bucket_plans_cap1_dual sell"
        );
        for (direction, expected) in [(1, &case.expected_buy), (-1, &case.expected_sell)] {
            let result = score_bucket_plans_cap1_basic(
                backend,
                &codes,
                &[0],
                &[0],
                &[0, 1],
                &case.split,
                &ordered,
                &case.entry,
                &case.release,
                &case.valid,
                &case.buy,
                &case.sell,
                &case.tie,
                1,
                rows,
                case.expiry_ms,
                direction,
                case.payout,
            )
            .unwrap()
            .output;
            assert_eq!(
                result,
                expected[..8],
                "score_bucket_plans_cap1_basic direction {direction}"
            );
        }
        let result = score_bucket_plans_cap1_basic_dual(
            backend,
            &codes,
            &[0],
            &[0],
            &[0, 1],
            &case.split,
            &ordered,
            &case.entry,
            &case.release,
            &case.valid,
            &case.buy,
            &case.sell,
            &case.tie,
            1,
            rows,
            case.expiry_ms,
            case.payout,
        )
        .unwrap()
        .output;
        assert_eq!(
            result.buy_output,
            case.expected_buy[..8],
            "score_bucket_plans_cap1_basic_dual buy"
        );
        assert_eq!(
            result.sell_output,
            case.expected_sell[..8],
            "score_bucket_plans_cap1_basic_dual sell"
        );
        for (direction, expected) in [(1, &case.expected_buy), (-1, &case.expected_sell)] {
            let result = score_bucket_plans_cap1_sparse(
                backend,
                &codes,
                &[0],
                &[0],
                &[0, 1],
                &[0],
                &offsets,
                &sparse_rows,
                &case.split,
                &case.entry,
                &case.release,
                &case.settlement,
                &case.valid,
                &case.buy,
                &case.sell,
                &case.tie,
                1,
                rows,
                case.expiry_ms,
                direction,
                case.payout,
            )
            .unwrap()
            .output;
            assert_eq!(
                result,
                expected[..21],
                "score_bucket_plans_cap1_sparse direction {direction}"
            );
        }
        for (direction, expected) in [(1, &case.expected_buy), (-1, &case.expected_sell)] {
            let result = score_bucket_plans_cap1_basic_sparse(
                backend,
                &codes,
                &[0],
                &[0],
                &[0, 1],
                &[0],
                &offsets,
                &sparse_rows,
                &case.split,
                &case.entry,
                &case.release,
                &case.valid,
                &case.buy,
                &case.sell,
                &case.tie,
                1,
                rows,
                case.expiry_ms,
                direction,
                case.payout,
            )
            .unwrap()
            .output;
            assert_eq!(
                result,
                expected[..8],
                "score_bucket_plans_cap1_basic_sparse direction {direction}"
            );
        }
        let result = score_bucket_plans_cap1_sparse_dual(
            backend,
            &codes,
            &[0],
            &[0],
            &[0, 1],
            &[0],
            &offsets,
            &sparse_rows,
            &case.split,
            &case.entry,
            &case.release,
            &case.settlement,
            &case.valid,
            &case.buy,
            &case.sell,
            &case.tie,
            1,
            rows,
            case.expiry_ms,
            case.payout,
        )
        .unwrap()
        .output;
        assert_eq!(
            result.buy_output,
            case.expected_buy[..21],
            "score_bucket_plans_cap1_sparse_dual buy"
        );
        assert_eq!(
            result.sell_output,
            case.expected_sell[..21],
            "score_bucket_plans_cap1_sparse_dual sell"
        );
        let result = score_bucket_plans_cap1_basic_sparse_dual(
            backend,
            &codes,
            &[0],
            &[0],
            &[0, 1],
            &[0],
            &offsets,
            &sparse_rows,
            &case.split,
            &case.entry,
            &case.release,
            &case.valid,
            &case.buy,
            &case.sell,
            &case.tie,
            1,
            rows,
            case.expiry_ms,
            case.payout,
        )
        .unwrap()
        .output;
        assert_eq!(
            result.buy_output,
            case.expected_buy[..8],
            "score_bucket_plans_cap1_basic_sparse_dual buy"
        );
        assert_eq!(
            result.sell_output,
            case.expected_sell[..8],
            "score_bucket_plans_cap1_basic_sparse_dual sell"
        );
        let result = reconstruct_signal_masks_cap1(
            backend,
            &codes,
            &[0],
            &[0],
            &[0, 1],
            &case.split,
            &ordered,
            &case.entry,
            &case.release,
            1,
            rows,
            case.expiry_ms,
        )
        .unwrap()
        .output;
        assert_eq!(result, case.mask, "reconstruct_signal_masks_cap1");
    }
}

fn other_literals(backend: &Backend) {
    // TB-A: five losses imply drawdown 5, five underwater trades, four negative
    // windows of width two. TB-B: six wins yield zero for all three outputs.
    for (path, horizon, drawdown, underwater, negative) in [
        (vec![-1.0; 5], 2, 5.0_f64, 5, 4),
        (vec![0.92; 6], 3, 0.0, 0, 0),
    ] {
        let actual = bootstrap_path_metrics(backend, &path, 1, path.len() as i32, horizon)
            .unwrap()
            .output;
        let reference = bootstrap_path_metrics(&Backend::Cpu, &path, 1, path.len() as i32, horizon)
            .unwrap()
            .output;
        assert_eq!(actual.max_drawdowns[0].to_bits(), drawdown.to_bits());
        assert_eq!(actual.longest_underwater, [underwater]);
        assert_eq!(actual.negative_rolling, [negative]);
        assert_eq!(actual, reference);
    }
    // TP-B: a due time equal to entry releases capacity before admission.
    let capacity = |b: &Backend| {
        replay_capacity(
            b,
            &[1, 1, 1],
            &[0, 1, 1, 2],
            &[0, 10000, 100000, 210000],
            &[100000, 110000, 200000, 310000],
            &[30; 4],
            &[1; 4],
            &[1; 4],
            1,
            3,
            4,
            1,
            1,
        )
        .unwrap()
        .output
    };
    assert_eq!(capacity(backend), [1, 0, 1, 1]);
    assert_eq!(capacity(backend), capacity(&Backend::Cpu));
    // TP-A: [1,0,-2,3] has drawdowns [0,0,2,0], RMS 1. A second path
    // proves observation-major layout and the cast of each f32 before summing.
    let path = |b: &Backend| {
        path_drawdown(b, &[1.0, 0.5, 0.0, -1.0, -2.0, 0.25, 3.0, 0.5], 4, 2)
            .unwrap()
            .output
    };
    let actual = path(backend);
    assert_eq!(
        actual
            .max_drawdown
            .iter()
            .map(|v| v.to_bits())
            .collect::<Vec<_>>(),
        [2.0_f64.to_bits(), 1.0_f64.to_bits()]
    );
    assert_eq!(
        actual
            .ulcer_index
            .iter()
            .map(|v| v.to_bits())
            .collect::<Vec<_>>(),
        [1.0_f64.to_bits(), (1.625_f64 / 4.0).sqrt().to_bits()]
    );
    assert_eq!(actual, path(&Backend::Cpu));
    // A fractional return separates fused from unfused squared-drawdown accumulation: the
    // device contracts `sum += d * d` into one fused multiply-add, and so does the reference.
    let fractional = path_drawdown(backend, &[-1.0, -1.0, -0.1], 3, 1)
        .unwrap()
        .output;
    let (mut equity, mut peak, mut fused, mut unfused) = (0.0_f64, 0.0_f64, 0.0_f64, 0.0_f64);
    for value in [-1.0_f32, -1.0, -0.1] {
        equity += f64::from(value);
        if equity > peak {
            peak = equity;
        }
        let drawdown = peak - equity;
        fused = drawdown.mul_add(drawdown, fused);
        unfused += drawdown * drawdown;
    }
    assert_ne!(fused.to_bits(), unfused.to_bits());
    assert_eq!(
        fractional.ulcer_index[0].to_bits(),
        (fused / 3.0).sqrt().to_bits()
    );
    assert_eq!(
        fractional,
        path_drawdown(&Backend::Cpu, &[-1.0, -1.0, -0.1], 3, 1)
            .unwrap()
            .output
    );
    // TR-A: baseline opens events 0 and 2 => two wins, +18400. Policy bit 0
    // blocks event 0, so events 1 and 2 give one loss and one win => -800.
    // The repaired curve starts at entry 2000 and ends at settlement 5000:
    // duration 3000, two trades, max drawdown 10000, squares 184640000.
    let repair = |b: &Backend| {
        replay_policies(
            b,
            &[0, 3],
            &[1000, 2000, 4000],
            &[3000, 3500, 5000],
            &[3000, 3500, 5000],
            &[1, 0, 1],
            &[1; 3],
            &[1, 0, 0],
            1,
            &[0, 0],
            &[0, 1],
            2,
            0,
            10000,
            9200,
        )
        .unwrap()
        .output
    };
    assert_eq!(
        repair(backend),
        Replay {
            out_opened: vec![2, 2],
            out_settled: vec![2, 2],
            out_wins: vec![2, 1],
            out_losses: vec![0, 1],
            out_ties: vec![0, 0],
            out_net_fp: vec![18400, -800],
            out_max_dd_fp: vec![0, 10000],
            out_longest_dd_ms: vec![0, 3000],
            out_longest_dd_trades: vec![0, 2],
            out_longest_loss_streak: vec![0, 1],
            out_gross_profit_fp: vec![18400, 9200],
            out_gross_loss_fp: vec![0, 10000],
            out_sum_returns_fp: vec![18400, -800],
            out_sum_squares_fp2: vec![169280000, 184640000],
            out_downside_squares_fp2: vec![0, 100000000],
        }
    );
    assert_eq!(repair(backend), repair(&Backend::Cpu));
}

#[test]
fn literal_references_cover_all_thirteen_kernels() {
    search_literals(&Backend::Cpu);
    other_literals(&Backend::Cpu);
}

#[cfg(feature = "cuda")]
#[test]
fn literal_device_results_equal_the_references_exactly() {
    let backend = Backend::Cuda(crate::cuda::Device::open(0).expect("required CUDA device 0"));
    search_literals(&backend);
    other_literals(&backend);
    boundary_literals(&backend);
    let Backend::Cuda(device) = &backend else {
        unreachable!()
    };
    resident_literals(device);
}

#[test]
fn validation_precedes_backend_dispatch() {
    assert!(
        replay_policies(
            &Backend::Cpu,
            &[0],
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            2,
            &[],
            &[],
            i32::MAX,
            0,
            1,
            9200
        )
        .unwrap_err()
        .contains("policy_masks/word_count index count exceeds i32")
    );
    let error = bootstrap_path_metrics(&Backend::Cpu, &[1.0], 1, 2, 1).unwrap_err();
    assert!(error.contains("bootstrap_path_metrics: paths"), "{error}");
    assert!(
        replay_capacity(
            &Backend::Cpu,
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            0,
            0,
            0,
            65,
            1
        )
        .unwrap_err()
        .contains("replay_capacity: max_total")
    );
    assert!(
        replay_capacity(
            &Backend::Cpu,
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            0,
            0,
            0,
            1,
            0
        )
        .unwrap_err()
        .contains("replay_capacity: max_expiry")
    );
    assert!(
        path_drawdown(&Backend::Cpu, &[], -1, 1)
            .unwrap_err()
            .contains("path_drawdown: observation_count")
    );
    assert!(
        replay_policies(
            &Backend::Cpu,
            &[0, 1],
            &[1],
            &[2],
            &[2],
            &[1],
            &[1],
            &[],
            1,
            &[0],
            &[0],
            1,
            0,
            3,
            9200
        )
        .unwrap_err()
        .contains("replay_policies: failure_words")
    );
    #[cfg(not(feature = "cuda"))]
    assert!(Backend::cuda(0).err().unwrap().contains("`cuda` feature"));

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
    let input = crate::search::Request {
        kind: 0,
        buffers,
        split_mask: &[1],
        candidates: CandidateConditions {
            condition_feature: &[0],
            condition_bucket: &[0],
            candidate_offsets: &[0, 1],
            candidate_count: 1,
        },
        sparse: None,
        expiry_ms: 1,
        direction_code: 1,
        payout_basis: 92,
    };
    input.validate().unwrap();
    let mut bad = input;
    bad.candidates.condition_feature = &[-1];
    assert!(
        bad.validate()
            .unwrap_err()
            .contains("score_bucket_plans_cap1: condition_feature")
    );
    bad = input;
    bad.candidates.condition_feature = &[1];
    assert!(bad.validate().unwrap_err().contains("condition_feature"));
    bad = input;
    bad.candidates.condition_bucket = &[];
    assert!(bad.validate().unwrap_err().contains("condition_bucket"));
    for offsets in [
        &[][..],
        &[0][..],
        &[1, 1][..],
        &[0, 0][..],
        &[0, 2][..],
        &[0, -1][..],
    ] {
        bad = input;
        bad.candidates.candidate_offsets = offsets;
        assert!(bad.validate().unwrap_err().contains("candidate_offsets"));
    }
    bad = input;
    bad.candidates = CandidateConditions {
        condition_feature: &[0, 0],
        condition_bucket: &[0, 0],
        candidate_offsets: &[0, 2, 1, 2],
        candidate_count: 3,
    };
    assert!(bad.validate().unwrap_err().contains("candidate_offsets"));
    // Zero candidates have a terminal zero; a candidate may never own zero conditions.
    bad.candidates = CandidateConditions {
        condition_feature: &[],
        condition_bucket: &[],
        candidate_offsets: &[0],
        candidate_count: 0,
    };
    bad.validate().unwrap();
    bad = input;
    bad.buffers.ordered_rows = &[1];
    assert!(bad.validate().unwrap_err().contains("ordered_rows"));
    for (offsets, rows, driver, field) in [
        (&[1, 0][..], &[0][..], &[0][..], "key_chrono_offsets"),
        (&[0, 2][..], &[0][..], &[0][..], "key_chrono_offsets"),
        (&[0, 1][..], &[1][..], &[0][..], "key_chrono_rows"),
        (&[0, 1][..], &[0][..], &[1][..], "candidate_driver_key"),
    ] {
        bad = input;
        bad.kind = 4;
        bad.sparse = Some(SparseIndex {
            candidate_driver_key: driver,
            key_chrono_offsets: offsets,
            key_chrono_rows: rows,
        });
        let error = bad.validate().unwrap_err();
        assert!(
            error.contains("score_bucket_plans_cap1_sparse") && error.contains(field),
            "{error}"
        );
    }
}

#[cfg(feature = "cuda")]
fn resident_literals(device: &crate::cuda::Device) {
    for case in search_cases() {
        let rows = case.entry.len() as i32;
        let codes = vec![0_i16; rows as usize];
        let ordered = (0..i64::from(rows)).collect::<Vec<_>>();
        let sparse_rows = (0..rows).collect::<Vec<_>>();
        let offsets = [0, rows];
        let excluded = vec![0; rows as usize];
        let buffers = SearchBuffers {
            feature_codes: &codes,
            feature_count: 1,
            row_count: rows,
            ordered_rows: &ordered,
            decision_time_ms: &case.entry,
            release_time_ms: &case.release,
            settlement_time_ms: &case.settlement,
            valid: &case.valid,
            buy_win: &case.buy,
            sell_win: &case.sell,
            tie: &case.tie,
        };
        let resident = device
            .search_workspace(buffers, &[&case.split, &excluded])
            .unwrap();
        // 2-byte codes, four 8-byte arrays, four flag bytes, and two split bytes.
        assert_eq!(resident.timings.allocated_bytes, rows as usize * 40);
        let conditions = CandidateConditions {
            condition_feature: &[0],
            condition_bucket: &[0],
            candidate_offsets: &[0, 1],
            candidate_count: 1,
        };
        let sparse = SparseIndex {
            candidate_driver_key: &[0],
            key_chrono_offsets: &offsets,
            key_chrono_rows: &sparse_rows,
        };
        let chunk = resident
            .upload_candidates(conditions, Some(sparse))
            .unwrap();
        // One feature (4), bucket (2), two candidate offsets (8), one driver (4),
        // two sparse offsets (8), and one sparse row index (4) per input row.
        let candidate_bytes = 26 + rows as usize * 4;
        assert_eq!(chunk.timings.allocated_bytes, candidate_bytes);
        let input_bytes = resident.timings.allocated_bytes + candidate_bytes;
        for (direction, expected) in [(1, &case.expected_buy), (-1, &case.expected_sell)] {
            let result = chunk
                .score_bucket_plans_cap1(0, case.expiry_ms, direction, case.payout)
                .unwrap();
            assert_eq!(
                result.timings.allocated_bytes,
                input_bytes + result.output.len() * 8
            );
            assert_eq!(result.output, expected[..21]);
        }
        let result = chunk
            .score_bucket_plans_cap1_dual(0, case.expiry_ms, case.payout)
            .unwrap();
        assert_eq!(
            result.timings.allocated_bytes,
            input_bytes + (result.output.buy_output.len() + result.output.sell_output.len()) * 8
        );
        assert_eq!(result.output.buy_output, case.expected_buy[..21]);
        assert_eq!(result.output.sell_output, case.expected_sell[..21]);
        for (direction, expected) in [(1, &case.expected_buy), (-1, &case.expected_sell)] {
            let result = chunk
                .score_bucket_plans_cap1_basic(0, case.expiry_ms, direction, case.payout)
                .unwrap();
            assert_eq!(
                result.timings.allocated_bytes,
                input_bytes + result.output.len() * 8
            );
            assert_eq!(result.output, expected[..8]);
        }
        let result = chunk
            .score_bucket_plans_cap1_basic_dual(0, case.expiry_ms, case.payout)
            .unwrap();
        assert_eq!(
            result.timings.allocated_bytes,
            input_bytes + (result.output.buy_output.len() + result.output.sell_output.len()) * 8
        );
        assert_eq!(result.output.buy_output, case.expected_buy[..8]);
        assert_eq!(result.output.sell_output, case.expected_sell[..8]);
        for (direction, expected) in [(1, &case.expected_buy), (-1, &case.expected_sell)] {
            let result = chunk
                .score_bucket_plans_cap1_sparse(0, case.expiry_ms, direction, case.payout)
                .unwrap();
            assert_eq!(
                result.timings.allocated_bytes,
                input_bytes + result.output.len() * 8
            );
            assert_eq!(result.output, expected[..21]);
        }
        for (direction, expected) in [(1, &case.expected_buy), (-1, &case.expected_sell)] {
            let result = chunk
                .score_bucket_plans_cap1_basic_sparse(0, case.expiry_ms, direction, case.payout)
                .unwrap();
            assert_eq!(
                result.timings.allocated_bytes,
                input_bytes + result.output.len() * 8
            );
            assert_eq!(result.output, expected[..8]);
        }
        let result = chunk
            .score_bucket_plans_cap1_sparse_dual(0, case.expiry_ms, case.payout)
            .unwrap();
        assert_eq!(
            result.timings.allocated_bytes,
            input_bytes + (result.output.buy_output.len() + result.output.sell_output.len()) * 8
        );
        assert_eq!(result.output.buy_output, case.expected_buy[..21]);
        assert_eq!(result.output.sell_output, case.expected_sell[..21]);
        let result = chunk
            .score_bucket_plans_cap1_basic_sparse_dual(0, case.expiry_ms, case.payout)
            .unwrap();
        assert_eq!(
            result.timings.allocated_bytes,
            input_bytes + (result.output.buy_output.len() + result.output.sell_output.len()) * 8
        );
        assert_eq!(result.output.buy_output, case.expected_buy[..8]);
        assert_eq!(result.output.sell_output, case.expected_sell[..8]);
        let result = chunk
            .reconstruct_signal_masks_cap1(0, case.expiry_ms)
            .unwrap();
        assert_eq!(
            result.timings.allocated_bytes,
            input_bytes + result.output.len()
        );
        assert_eq!(result.output, case.mask);
        assert_eq!(
            chunk
                .reconstruct_signal_masks_cap1(1, case.expiry_ms)
                .unwrap()
                .output,
            excluded
        );
        drop(chunk);
        let dense = resident.upload_candidates(conditions, None).unwrap();
        assert_eq!(dense.timings.allocated_bytes, 14);
        assert!(
            dense
                .score_bucket_plans_cap1_sparse(0, case.expiry_ms, 1, case.payout)
                .unwrap_err()
                .contains("no sparse index")
        );
        assert_eq!(
            dense
                .reconstruct_signal_masks_cap1(0, case.expiry_ms)
                .unwrap()
                .output,
            case.mask
        );
    }
}

fn boundary_literals(backend: &Backend) {
    // Four conditions are active, and 129 candidates require two 128-thread blocks.
    // Row two fails only the fourth slot. Chronology is deliberately not row order.
    let condition_feature = [0, 1, 2, 3].repeat(129);
    let condition_bucket = [0, 1, 2, 3].repeat(129);
    let candidate_offsets = (0..=129).map(|c| c * 4).collect::<Vec<_>>();
    let codes = [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 9, 3];
    let run = |b: &Backend| {
        reconstruct_signal_masks_cap1(
            b,
            &codes,
            &condition_feature,
            &condition_bucket,
            &candidate_offsets,
            &[1; 4],
            &[3, 1, 0, 2],
            &[3, 2, 4, 1],
            &[4, 3, 5, 2],
            129,
            4,
            1,
        )
        .unwrap()
        .output
    };
    assert_eq!(run(backend), [1, 1, 0, 1].repeat(129));
    assert_eq!(run(backend), run(&Backend::Cpu));
    // Bucket -1 is a real equality comparison against the missing-code value.
    assert_eq!(
        reconstruct_signal_masks_cap1(
            backend,
            &[-1, 0],
            &[0],
            &[-1],
            &[0, 1],
            &[1, 1],
            &[0, 1],
            &[1, 2],
            &[2, 3],
            1,
            2,
            1
        )
        .unwrap()
        .output,
        [1, 0]
    );
    // Malformed double win flags preserve the source priority: single sell wins;
    // dual sell loses because both dual outputs test buy_win first.
    let single = score_bucket_plans_cap1_basic(
        backend,
        &[0],
        &[0],
        &[0],
        &[0, 1],
        &[1],
        &[0],
        &[1],
        &[2],
        &[1],
        &[1],
        &[1],
        &[0],
        1,
        1,
        1,
        -1,
        92,
    )
    .unwrap()
    .output;
    let dual = score_bucket_plans_cap1_basic_dual(
        backend,
        &[0],
        &[0],
        &[0],
        &[0, 1],
        &[1],
        &[0],
        &[1],
        &[2],
        &[1],
        &[1],
        &[1],
        &[0],
        1,
        1,
        1,
        92,
    )
    .unwrap()
    .output;
    assert_eq!(single, [1, 1, 0, 0, 0, 0, 1, 92]);
    assert_eq!(dual.buy_output, [1, 1, 0, 0, 0, 1, 0, 92]);
    assert_eq!(dual.sell_output, [1, 0, 1, 0, 0, 0, 1, -100]);
    // The 64-slot arrays fill exactly, reject the 65th, and compact completely
    // before an entry at the due timestamp.
    let mut entries = vec![1; 65];
    entries.push(100);
    let mut expected = vec![1; 64];
    expected.extend([0, 1]);
    assert_eq!(
        replay_capacity(
            backend,
            &[1],
            &[0; 66],
            &entries,
            &[100; 66],
            &[30; 66],
            &[1; 66],
            &[1; 66],
            1,
            1,
            66,
            64,
            64
        )
        .unwrap()
        .output,
        expected
    );
    // Positive-close guard: an unresolved zero-close first event keeps capacity,
    // blocks the second, and remains counted as opened but not settled.
    let unresolved = replay_policies(
        backend,
        &[0, 2],
        &[1000, 2000],
        &[0, 3000],
        &[0, 3000],
        &[-1, 1],
        &[1, 1],
        &[0, 0],
        1,
        &[0],
        &[0],
        1,
        0,
        10000,
        9200,
    )
    .unwrap()
    .output;
    assert_eq!(
        unresolved,
        Replay {
            out_opened: vec![1],
            out_settled: vec![0],
            out_wins: vec![0],
            out_losses: vec![0],
            out_ties: vec![0],
            out_net_fp: vec![0],
            out_max_dd_fp: vec![0],
            out_longest_dd_ms: vec![0],
            out_longest_dd_trades: vec![0],
            out_longest_loss_streak: vec![0],
            out_gross_profit_fp: vec![0],
            out_gross_loss_fp: vec![0],
            out_sum_returns_fp: vec![0],
            out_sum_squares_fp2: vec![0],
            out_downside_squares_fp2: vec![0],
        }
    );
    // The longest duration and most trades occur in different repair episodes.
    // First: -10000,+20000 spans 99 ms and two trades. Second: four losses
    // then two wins spans 6 ms and six trades. Both maxima must survive.
    let times = [2, 100, 101, 102, 103, 104, 105, 106];
    let entry = [1, 99, 100, 101, 102, 103, 104, 105];
    let repaired = replay_policies(
        backend,
        &[0, 8],
        &entry,
        &times,
        &times,
        &[0, 1, 0, 0, 0, 0, 1, 1],
        &[1; 8],
        &[],
        0,
        &[0],
        &[],
        1,
        0,
        1000,
        20000,
    )
    .unwrap()
    .output;
    assert_eq!(
        repaired,
        Replay {
            out_opened: vec![8],
            out_settled: vec![8],
            out_wins: vec![3],
            out_losses: vec![5],
            out_ties: vec![0],
            out_net_fp: vec![10000],
            out_max_dd_fp: vec![40000],
            out_longest_dd_ms: vec![99],
            out_longest_dd_trades: vec![6],
            out_longest_loss_streak: vec![4],
            out_gross_profit_fp: vec![60000],
            out_gross_loss_fp: vec![50000],
            out_sum_returns_fp: vec![10000],
            out_sum_squares_fp2: vec![1700000000],
            out_downside_squares_fp2: vec![500000000],
        }
    );
}

#[test]
fn reference_boundaries_preserve_the_kernel_rules() {
    boundary_literals(&Backend::Cpu);
}
