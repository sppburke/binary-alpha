# Binary Alpha

Binary Alpha is a Linux-first, configuration-driven binary-options research and execution system
written in Rust, with an optional NVIDIA CUDA accelerator for offline kernel operations. Its core
is broker-, instrument-, strategy-, account-, and currency-neutral. Broker access uses only authorized application programming interfaces and WebSockets; no browser,
Document Object Model, Chrome DevTools Protocol, profile, cookie, or click-execution path exists.

## Modes

The configuration vocabulary freezes four run modes: `research`, `replay`, `paper`, and `live` (see
[docs/contracts.md](docs/contracts.md)). The current checkout validates configuration documents,
imports existing historical data into immutable published generations, audits each published
generation through its configured instrument stream into a profile and finalized causal candles,
builds feature and future-only outcome generations, replays governed historical inputs through the
one execution engine into a reconstructable financial ledger, and verifies every kind of
generation. The accelerator supplies thirteen retained device kernels and their deterministic
central-processor references; it has no production consumer. The checkout executes no live, paper,
or broker mode. Browser-driven operation, click execution, and any live, paper, certification,
deployment, or production action without its own authorization are unsupported.

## Build and entry points

```sh
rustup toolchain install        # installs the toolchain pinned in rust-toolchain.toml with rustfmt and clippy
cargo build --workspace --locked
BINARY_ALPHA_NVCC=/home/sean/.local/cuda/13.4.1/bin/nvcc BINARY_ALPHA_HOST_COMPILER=/usr/bin/gcc cargo build --workspace --locked --features binary-alpha-app/cuda
cargo run --locked -p binary-alpha-app -- --help
cargo run --locked -p binary-alpha-app -- config validate --config configs/example.toml
cargo run --release --locked -p binary-alpha-app -- data import --config PATH
cargo run --release --locked -p binary-alpha-app -- data audit --config PATH --manifest URI
cargo run --release --locked -p binary-alpha-app -- data verify --manifest URI
cargo run --release --locked -p binary-alpha-app -- features build --config PATH
cargo run --release --locked -p binary-alpha-app -- outcomes build --config PATH
cargo run --release --locked -p binary-alpha-app -- replay --config PATH
BINARY_ALPHA_TEST_CONFIG=PATH BINARY_ALPHA_CUDA_REFERENCE_OUTPUT=NEW_DIRECTORY cargo test --release --locked -p binary-alpha-app --features cuda --test phase07_cuda_parity capture_legacy_reference -- --exact --ignored --nocapture
BINARY_ALPHA_TEST_CONFIG=PATH BINARY_ALPHA_CUDA_REFERENCE=MANIFEST cargo test --release --locked -p binary-alpha-app --features cuda --test phase07_cuda_parity governed_parity -- --exact --ignored --nocapture
```

`binary-alpha config validate --config PATH` prints the content hash and the canonical document to
standard output and mutates nothing. `binary-alpha data import --config PATH` copies the declared
sources (native tick files, daily tick archives, and five-second bar collections) into the retained
historical-data folder named by `storage.historical_data_dir`, normalizes ticks, validates bars,
publishes every object and one ready manifest per dataset to
`storage.publication_uri`, and mirrors the manifest locally; `binary-alpha data audit --config PATH
--manifest URI` feeds one published generation through the `[[instruments]]` entry that maps it and
publishes its profile, one candle object per configured stream, and a stream manifest the same way;
`binary-alpha features build --config PATH` resolves or applies one feature plan per
`[[features.instruments]]` entry over a published stream generation and publishes the plan, rows,
events, and encoded rows as a feature generation; `binary-alpha outcomes build --config PATH`
labels every decision row of the `[outcomes]` feature generation against its tick generation and
publishes the future-only outcome generation; `binary-alpha replay --config PATH` feeds the
`[replay]` inputs through the engine with the configured simulation and publishes the ledger and
summary as a replay generation; `binary-alpha data verify --manifest URI`
re-reads one generation of any kind from its manifest and objects alone. All six are documented in
[docs/contracts.md](docs/contracts.md) and [docs/operations.md](docs/operations.md). The example
configuration retains data in the repository-local `historical_data/` folder, which Git ignores,
and declares one instrument. The governed-fixture proof of the instrument stream is
`BINARY_ALPHA_TEST_CONFIG=PATH cargo test --locked -p binary-alpha-app --test phase03_instrument_stream -- --ignored --nocapture`,
where `PATH` is an untracked JSON document naming the published Phase 02 generations and the
legacy parity files; the feature-engine proof is
`BINARY_ALPHA_TEST_CONFIG=PATH cargo test --locked -p binary-alpha-app --test phase04_feature_engine -- --ignored --nocapture`,
where `PATH` names the research configuration and the reference root; the outcome proof is
`BINARY_ALPHA_TEST_CONFIG=PATH cargo test --locked -p binary-alpha-app --test phase05_future_outcomes -- --ignored --nocapture`
with the same document shape; the engine proof is
`BINARY_ALPHA_TEST_CONFIG=PATH cargo test --locked -p binary-alpha-app --test phase06_engine_parity -- --ignored --nocapture`
with the same document shape, comparing every reference disposition, outcome, and path with the
frozen candidate mapping. The accelerator proof uses the same governed wrapper, with eight
`legacy_sources: [{label, path}]` entries whose absolute paths resolve the pinned
[fixture sources](crates/accelerator/kernels/SOURCES.md#fixture-provenance-labels), and the immutable
manifest named by `BINARY_ALPHA_CUDA_REFERENCE`. The capture command creates a fresh reference and
never replaces the acceptance reference captured at extraction commit `151ba60`; final parity
reads that reference through the four-slot fixture adapter. Neither command opens holdout data.
Verification runs `cargo fmt --all --check`,
`cargo clippy --workspace --all-targets --locked -- -D warnings`,
`cargo test --workspace --locked`, and `cargo build --workspace --locked`; the
ordinary workflow in `.github/workflows/ci.yml` runs the default feature set and validates
`configs/example.toml`. The separate `.github/workflows/cuda.yml` runs the CUDA-enabled locked
build, static analysis, device tests, and governed parity on the registered NVIDIA runner.

The repository-only runner `binary-alpha-cuda-quantum` uses actions/runner `2.337.0` on `quantum`
with labels `self-hosted`, `linux`, `x64`, and `binary-alpha-cuda-quantum`. The workflow supplies
`BINARY_ALPHA_NVCC=/home/sean/.local/cuda/13.4.1/bin/nvcc`,
`BINARY_ALPHA_HOST_COMPILER=/usr/bin/gcc`,
`BINARY_ALPHA_TEST_CONFIG=/mnt/data/issue-7-scratch/phase06_test_config.json`, and
`BINARY_ALPHA_CUDA_REFERENCE=/mnt/data/binary-alpha-phase07-reference/attempt2/reference.json`.
For manual runs, export the same variables in the runner process environment; the governed wrapper
must resolve its existing development inputs and reference files. `BINARY_ALPHA_CUDA_ARCH` is
optional and defaults to the proved `sm_120` target. The workflow never regenerates expectations.
The authorized offline setup and runner rollback are recorded in [operations](docs/operations.md).

## Layout

| Path | Owns |
| --- | --- |
| `crates/engine` | Package `binary-alpha-engine`: configuration validation and identity, immutable tick and bar records, dataset roles and capabilities, generation identity, ready manifests, the instrument stream with its profile, candles, and stream manifest, the feature engine and frozen plans, future-only outcome labels, and the execution engine with exact money, its ledger, and its summaries. No files, network, cloud, broker, command-line, or device calls. |
| `crates/app` | Package `binary-alpha-app`: the `binary-alpha` executable, configuration loading, historical-data import, instrument audit, feature and outcome builds, replay, verification, Parquet input and output, the filesystem and Google Cloud Storage artifact stores, and all other external adapters. |
| `crates/accelerator` | Package `binary-alpha-accelerator`: the thirteen retained device kernels, the ahead-of-time CUDA build behind the `cuda` feature, the device host over cudarc, and the central-processor reference of every kernel. No engine or application dependency. |
| `configs/example.toml` | The checked-in example configuration; it contains only implemented fields, one instrument, and no credentials. |
| `docs/` | [architecture](docs/architecture.md), [contracts](docs/contracts.md), [migration map](docs/migration-map.md), and [operations](docs/operations.md). |

## Version policy

At the start of each implementing pull request, verify official release metadata for the newest
stable language toolchains and every required library, including the existing dependency graph.
Select the newest mutually compatible stable releases for the supported platforms and devices, and
use the newest stable language edition or standard supported by the selected compilers. Record the
lookup date, release sources, exact selections, and any necessary older-version exception with its
evidenced compiler, dependency, platform, or driver constraint in the implementing pull request.
Installed or cached versions are not evidence of the newest release.

`rust-toolchain.toml` pins the exact selected Rust release and its formatting and static-analysis
components. `Cargo.toml` owns the selected edition, direct packages, and features; `Cargo.lock`
records the exact direct and transitive resolution. Builds and verification use these recorded
selections, never floating channels or automatic upgrades. The phase that adds a device toolchain
pins it under the same policy. This policy changes compiler and library selections only, never pinned
source revisions, dataset identities, broker schemas, or completed evidence.

The device toolchain lookup on 2026-09-11 selected the newest stable CUDA toolkit redistributable,
`13.4.1` (released 2026-09-09), with nvcc `13.4.59`, from the
[NVIDIA redistributable index](https://developer.download.nvidia.com/compute/cuda/redist/), and
host compiler gcc `13.3.0`. The CUDA workflow pins and verifies both nvcc `13.4.59`
and gcc `13.3.0`; the build passes the exact `BINARY_ALPHA_HOST_COMPILER` path to
nvcc with `-ccbin` and records both compiler versions in the embedded `module.json`.
When the host path is unset, provenance records the default `gcc` version.
The manifest and lockfile pin cudarc `0.19.9`, the newest stable release
at that lookup, published 2026-08-11 on [crates.io](https://crates.io/crates/cudarc/0.19.9).
The driver-API feature is `cuda-13000`: runner driver `580.173.02` exposes driver API `13.0`, and
cudarc `0.19.9` has no `cuda-13040` feature. This compatibility exception selects the driver's
interface independently of the compiler; the compiled `sm_120` native binaries were proved to
load on that driver. cudarc uses `std`, `driver`, `dynamic-loading`, and `nvrtc` with default
features disabled. The `nvrtc` feature exposes its safe precompiled-binary loader; runtime
compilation is not used. Build flags retain `--std=c++11` without fast math.

## Roadmap

1. [#2](https://github.com/sppburke/binary-alpha/issues/2) Phase 01 — Establish the Rust repository structure and system contracts
2. [#3](https://github.com/sppburke/binary-alpha/issues/3) Phase 02 — Import and publish immutable historical datasets
3. [#4](https://github.com/sppburke/binary-alpha/issues/4) Phase 03 — Audit instruments and build configurable causal candles
4. [#5](https://github.com/sppburke/binary-alpha/issues/5) Phase 04 — Build the causal feature and regime engine
5. [#6](https://github.com/sppburke/binary-alpha/issues/6) Phase 05 — Build future-only binary-expiry outcomes
6. [#7](https://github.com/sppburke/binary-alpha/issues/7) Phase 06 — Build one deterministic strategy, replay, settlement, and risk engine
7. [#8](https://github.com/sppburke/binary-alpha/issues/8) Phase 07 — Preserve and port every existing NVIDIA CUDA kernel behind Rust
8. [#9](https://github.com/sppburke/binary-alpha/issues/9) Phase 08 — Build candidate search, chronological evaluation, false-discovery control, and stability analysis
9. [#10](https://github.com/sppburke/binary-alpha/issues/10) Phase 09 — Unify repair, portfolio selection, and risk tuning
10. [#11](https://github.com/sppburke/binary-alpha/issues/11) Phase 10 — Add broker-neutral direct WebSocket contracts and a Deriv adapter
11. [#12](https://github.com/sppburke/binary-alpha/issues/12) Phase 11 — Deliver one-command research, optimization, and locked-holdout certification
12. [#13](https://github.com/sppburke/binary-alpha/issues/13) Phase 12 — Ship one ordered browser-free live runtime and resumable cutover
