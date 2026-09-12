# Kernel sources

The original digests below identify the thirteen extracted string payloads, including whitespace,
from legacy commit `b509964cd1c40180e9d98b0e55a95699b0abe9ed`. The order below is the
module build order. Only `replay_policies` carried the legacy compile option
`--std=c++11`; the other twelve had no explicit compile options.

| Symbol | Original SHA-256 | Legacy file and RawKernel line |
|---|---|---|
| `score_bucket_plans_cap1` | `b7fbd0a8559fcef341c90a1c14f61a96b4bb817962cea78171d2ed5d45148da8` | [source:58](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/strategy_searcher_gpu/cupy_engine.py#L58) |
| `score_bucket_plans_cap1_dual` | `bd561563b1cf518afca112caa9a54f2dfa13e421fd2378c87726fd2629bd8b00` | [source:316](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/strategy_searcher_gpu/cupy_engine.py#L316) |
| `score_bucket_plans_cap1_basic` | `7f539e03452794c0244c2bbe7a82223fcbcec9f7d1523e760cb8ca9365285038` | [source:686](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/strategy_searcher_gpu/cupy_engine.py#L686) |
| `score_bucket_plans_cap1_basic_dual` | `d218f1ecc7419537398beab1fea189a7f862fcab92ec155e94726d1ad514456d` | [source:809](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/strategy_searcher_gpu/cupy_engine.py#L809) |
| `score_bucket_plans_cap1_sparse` | `ee4353f52d96e81e0cd95c6e7c38cf9e91937448410bab8a42d6cc8901b3be3a` | [source:938](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/strategy_searcher_gpu/cupy_engine.py#L938) |
| `score_bucket_plans_cap1_basic_sparse` | `9c1123fbf02d847bfaa1d09b12ca0a973e571c19a01ddbb22bf3df1e4e47d0cc` | [source:1204](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/strategy_searcher_gpu/cupy_engine.py#L1204) |
| `score_bucket_plans_cap1_sparse_dual` | `18c3fcc4a5d59bb3a4c8ce4dea9fb660f06295e66697bdf11a6fa6e99f7523d0` | [source:1336](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/strategy_searcher_gpu/cupy_engine.py#L1336) |
| `score_bucket_plans_cap1_basic_sparse_dual` | `ac4d67f8d84c104bb24e8f96673de1782bf0e4b6a712078b7ee75a5ee982793b` | [source:1620](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/strategy_searcher_gpu/cupy_engine.py#L1620) |
| `reconstruct_signal_masks_cap1` | `738ff7b107e2d375af589626f6613fb9ae1a0d8af09f3187c7dc91418c6d167d` | [source:1758](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/strategy_searcher_gpu/cupy_engine.py#L1758) |
| `bootstrap_path_metrics` | `08787ee13438ea5615b1716db1c2f300fbcd558de700f3e050d145164cf2b4a3` | [source:195](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/strategy_searcher/backtest/stability.py#L195) |
| `replay_capacity` | `575e16c4c2a8a1b5a03aacbf0d5f8bc78aad2521118380e0deae924466a5a836` | [source:371](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/portfolio_optimizer/cupy_engine.py#L371) |
| `path_drawdown` | `790b0e99a2bed348fdbb7c2c67bea71c7be26a1548b75232ad9b7340706b6a69` | [source:431](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/portfolio_optimizer/cupy_engine.py#L431) |
| `replay_policies` | `a11103708b3d22a4a37a30bed51abfa3e418477981e7dc603fa8ff195f5f48d0` | [source:151](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/strategy_repair/gpu_replay.py#L151) |

## Flattened condition layout

Extraction commit `151ba60` retains the original payloads above and owns the immutable acceptance
reference. Phase 07 replaces only the nine search kernels' fixed feature/bucket arguments,
per-candidate slot loads, and per-row equality checks with `condition_feature` (`int`),
`condition_bucket` (`short`), and `candidate_offsets` (`int`). Offsets have length
`candidate_count + 1`, begin at zero, end at the condition count, and delimit at least one condition
per candidate. Each row tests the same feature-major code equality in condition order. All other
statements, arithmetic, types, initial values, and operation order remain unchanged; bootstrap,
capacity, path drawdown, and repair sources still match their original digests. The fixture adapter
retains slot order and drops unused `-1` features without changing the captured input buffers.

| Search symbol | Flattened SHA-256 |
| --- | --- |
| `reconstruct_signal_masks_cap1` | `dfb40d132b6ba6e312c1b1c2578d7c08047b55c61a99062734b58a11a2bedc85` |
| `score_bucket_plans_cap1` | `39d7edee015ec2a1f33ed933b699bb523f3896d51e3b6929a9772314c6b82f82` |
| `score_bucket_plans_cap1_basic` | `4652498aa734ea6fe3c093ffa08da12437ee466abe5765310d61af39a6838169` |
| `score_bucket_plans_cap1_basic_dual` | `87b73a2eb13bfb3dfbc2930bdc22c22512fcaf03863688f632af29b5e6b67519` |
| `score_bucket_plans_cap1_basic_sparse` | `e54331eff12d84cefb04a075fd4e94730252018b3b470aed7c05854e09d4cc2a` |
| `score_bucket_plans_cap1_basic_sparse_dual` | `d1b5c69b22cab4bd38dcaa35d952b891d012ee753898222089958ad8af57642b` |
| `score_bucket_plans_cap1_dual` | `3390c4bb3541143575dd3a0b8397a0fcba2b8eeaf9514aff9f697b0a178f630c` |
| `score_bucket_plans_cap1_sparse` | `11a9833ac81f9660a2ad0a3c33df117834e8d45e9546a5fedac421c0dfc39c21` |
| `score_bucket_plans_cap1_sparse_dual` | `1d82b571f7b7191371259da9fc1e1138f2e81caaca0f3d2b99cf14b18c1c128f` |

## Fixture provenance labels

The governed wrapper supplies local paths for these labels. Fixtures and reference
manifests retain only labels and digests; this table binds each label to its pinned source.

| Label | Pinned source |
|---|---|
| `search_kernels` | [source](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/strategy_searcher_gpu/cupy_engine.py) |
| `bootstrap_kernel` | [source](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/strategy_searcher/backtest/stability.py) |
| `portfolio_kernels` | [source](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/portfolio_optimizer/cupy_engine.py) |
| `repair_kernel` | [source](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/strategy_repair/gpu_replay.py) |
| `search_tests` | [source](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/strategy_searcher_gpu/tests/test_backtester_metric_parity.py) |
| `bootstrap_tests` | [source](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/strategy_searcher_gpu/tests/test_bootstrap_stability.py) |
| `repair_tests` | [source](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/tests/test_strategy_repair.py) |
| `portfolio_tests` | [source](https://github.com/oniram93/rexi3/blob/b509964cd1c40180e9d98b0e55a95699b0abe9ed/trex/tests/test_portfolio_optimizer.py) |
