# Kernel sources

These thirteen files preserve the extracted string bytes, including whitespace, from
legacy commit `b509964cd1c40180e9d98b0e55a95699b0abe9ed`. The order below is the
module build order. Only `replay_policies` carried the legacy compile option
`--std=c++11`; the other twelve had no explicit compile options.

| Symbol | SHA-256 | Legacy file and RawKernel line |
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
