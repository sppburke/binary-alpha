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
                &[-1],
                &[-1],
                &[-1],
                &[-1],
                &[-1],
                &[-1],
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
            &[-1],
            &[-1],
            &[-1],
            &[-1],
            &[-1],
            &[-1],
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
                &[-1],
                &[-1],
                &[-1],
                &[-1],
                &[-1],
                &[-1],
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
            &[-1],
            &[-1],
            &[-1],
            &[-1],
            &[-1],
            &[-1],
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
                &[-1],
                &[-1],
                &[-1],
                &[-1],
                &[-1],
                &[-1],
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
                &[-1],
                &[-1],
                &[-1],
                &[-1],
                &[-1],
                &[-1],
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
            &[-1],
            &[-1],
            &[-1],
            &[-1],
            &[-1],
            &[-1],
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
            &[-1],
            &[-1],
            &[-1],
            &[-1],
            &[-1],
            &[-1],
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
            &[-1],
            &[-1],
            &[-1],
            &[-1],
            &[-1],
            &[-1],
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
        candidates: CandidateSlots {
            features: [&[0], &[-1], &[-1], &[-1]],
            buckets: [&[0]; 4],
            candidate_count: 1,
        },
        sparse: None,
        expiry_ms: 1,
        direction_code: 1,
        payout_basis: 92,
    };
    input.validate().unwrap();
    let mut bad = input;
    bad.candidates.features[0] = &[-1];
    assert!(
        bad.validate()
            .unwrap_err()
            .contains("score_bucket_plans_cap1: feature1")
    );
    bad = input;
    bad.candidates.features[3] = &[1];
    assert!(bad.validate().unwrap_err().contains("feature4"));
    bad = input;
    bad.candidates.buckets[2] = &[];
    assert!(bad.validate().unwrap_err().contains("bucket3"));
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
        let slots = CandidateSlots {
            features: [&[0], &[-1], &[-1], &[-1]],
            buckets: [&[0], &[-1], &[-1], &[-1]],
            candidate_count: 1,
        };
        let sparse = SparseIndex {
            candidate_driver_key: &[0],
            key_chrono_offsets: &offsets,
            key_chrono_rows: &sparse_rows,
        };
        for (direction, expected) in [(1, &case.expected_buy), (-1, &case.expected_sell)] {
            let result = resident
                .score_bucket_plans_cap1(slots, 0, case.expiry_ms, direction, case.payout)
                .unwrap();
            assert!(result.timings.allocated_bytes >= resident.timings.allocated_bytes);
            assert_eq!(result.output, expected[..21]);
        }
        let result = resident
            .score_bucket_plans_cap1_dual(slots, 0, case.expiry_ms, case.payout)
            .unwrap();
        assert!(result.timings.allocated_bytes >= resident.timings.allocated_bytes);
        assert_eq!(result.output.buy_output, case.expected_buy[..21]);
        assert_eq!(result.output.sell_output, case.expected_sell[..21]);
        for (direction, expected) in [(1, &case.expected_buy), (-1, &case.expected_sell)] {
            let result = resident
                .score_bucket_plans_cap1_basic(slots, 0, case.expiry_ms, direction, case.payout)
                .unwrap();
            assert!(result.timings.allocated_bytes >= resident.timings.allocated_bytes);
            assert_eq!(result.output, expected[..8]);
        }
        let result = resident
            .score_bucket_plans_cap1_basic_dual(slots, 0, case.expiry_ms, case.payout)
            .unwrap();
        assert!(result.timings.allocated_bytes >= resident.timings.allocated_bytes);
        assert_eq!(result.output.buy_output, case.expected_buy[..8]);
        assert_eq!(result.output.sell_output, case.expected_sell[..8]);
        for (direction, expected) in [(1, &case.expected_buy), (-1, &case.expected_sell)] {
            let result = resident
                .score_bucket_plans_cap1_sparse(
                    slots,
                    0,
                    sparse,
                    case.expiry_ms,
                    direction,
                    case.payout,
                )
                .unwrap();
            assert!(result.timings.allocated_bytes >= resident.timings.allocated_bytes);
            assert_eq!(result.output, expected[..21]);
        }
        for (direction, expected) in [(1, &case.expected_buy), (-1, &case.expected_sell)] {
            let result = resident
                .score_bucket_plans_cap1_basic_sparse(
                    slots,
                    0,
                    sparse,
                    case.expiry_ms,
                    direction,
                    case.payout,
                )
                .unwrap();
            assert!(result.timings.allocated_bytes >= resident.timings.allocated_bytes);
            assert_eq!(result.output, expected[..8]);
        }
        let result = resident
            .score_bucket_plans_cap1_sparse_dual(slots, 0, sparse, case.expiry_ms, case.payout)
            .unwrap();
        assert!(result.timings.allocated_bytes >= resident.timings.allocated_bytes);
        assert_eq!(result.output.buy_output, case.expected_buy[..21]);
        assert_eq!(result.output.sell_output, case.expected_sell[..21]);
        let result = resident
            .score_bucket_plans_cap1_basic_sparse_dual(
                slots,
                0,
                sparse,
                case.expiry_ms,
                case.payout,
            )
            .unwrap();
        assert!(result.timings.allocated_bytes >= resident.timings.allocated_bytes);
        assert_eq!(result.output.buy_output, case.expected_buy[..8]);
        assert_eq!(result.output.sell_output, case.expected_sell[..8]);
        let result = resident
            .reconstruct_signal_masks_cap1(slots, 0, case.expiry_ms)
            .unwrap();
        assert!(result.timings.allocated_bytes >= resident.timings.allocated_bytes);
        assert_eq!(result.output, case.mask);
        assert_eq!(
            resident
                .reconstruct_signal_masks_cap1(slots, 1, case.expiry_ms)
                .unwrap()
                .output,
            excluded
        );
    }
}

fn boundary_literals(backend: &Backend) {
    // All four slots are active, and 129 candidates require two 128-thread blocks.
    // Row two fails only the fourth slot. Chronology is deliberately not row order.
    let feature1 = vec![0; 129];
    let feature2 = vec![1; 129];
    let feature3 = vec![2; 129];
    let feature4 = vec![3; 129];
    let bucket1 = vec![0; 129];
    let bucket2 = vec![1; 129];
    let bucket3 = vec![2; 129];
    let bucket4 = vec![3; 129];
    let codes = [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 9, 3];
    let run = |b: &Backend| {
        reconstruct_signal_masks_cap1(
            b,
            &codes,
            &feature1,
            &bucket1,
            &feature2,
            &bucket2,
            &feature3,
            &bucket3,
            &feature4,
            &bucket4,
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
    // An inactive feature ignores even a non-sentinel bucket code. An active
    // bucket -1 is a real equality comparison against the missing-code value.
    assert_eq!(
        reconstruct_signal_masks_cap1(
            backend,
            &[-1, 0],
            &[0],
            &[-1],
            &[-1],
            &[32767],
            &[-1],
            &[5],
            &[-1],
            &[3],
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
        &[-1],
        &[-1],
        &[-1],
        &[-1],
        &[-1],
        &[-1],
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
        &[-1],
        &[-1],
        &[-1],
        &[-1],
        &[-1],
        &[-1],
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
