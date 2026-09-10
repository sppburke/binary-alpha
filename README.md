# Binary Alpha

Binary Alpha is a Linux-first, configuration-driven binary-options research and execution system
written in Rust. Its core is broker-, instrument-, strategy-, account-, and currency-neutral. Broker
access uses only authorized application programming interfaces and WebSockets; no browser,
Document Object Model, Chrome DevTools Protocol, profile, cookie, or click-execution path exists.

## Modes

The configuration vocabulary freezes four run modes: `research`, `replay`, `paper`, and `live` (see
[docs/contracts.md](docs/contracts.md)). The current checkout validates configuration documents,
imports existing historical data into immutable published generations, and verifies them; it executes
no mode. Browser-driven operation, click execution, and any live, paper, certification, deployment,
or production action without its own authorization are unsupported.

## Build and entry points

```sh
rustup toolchain install        # installs the toolchain pinned in rust-toolchain.toml with rustfmt and clippy
cargo build --workspace --locked
cargo run --locked -p binary-alpha-app -- --help
cargo run --locked -p binary-alpha-app -- config validate --config configs/example.toml
cargo run --release --locked -p binary-alpha-app -- data import --config PATH
cargo run --release --locked -p binary-alpha-app -- data verify --manifest URI
```

`binary-alpha config validate --config PATH` prints the content hash and the canonical document to
standard output and mutates nothing. `binary-alpha data import --config PATH` copies the declared
sources (native tick files, daily tick archives, and five-second bar collections) into the retained
historical-data folder named by `storage.historical_data_dir`, normalizes ticks, validates bars,
publishes every object and one ready manifest per dataset to
`storage.publication_uri`, and mirrors the manifest locally; `binary-alpha data verify --manifest URI`
re-reads one generation from its manifest and objects alone. Both are documented in
[docs/contracts.md](docs/contracts.md) and [docs/operations.md](docs/operations.md). The example
configuration retains data in the repository-local `historical_data/` folder, which Git ignores.
Verification runs `cargo fmt --all --check`,
`cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`,
`cargo test --workspace --all-features --locked`, and `cargo build --workspace --locked`; the
workflow in `.github/workflows/ci.yml` runs the same commands plus the validation of
`configs/example.toml`.

## Layout

| Path | Owns |
| --- | --- |
| `crates/engine` | Package `binary-alpha-engine`: configuration validation and identity, immutable tick and bar records, dataset roles and capabilities, generation identity, and ready manifests. No files, network, cloud, broker, command-line, or device calls. |
| `crates/app` | Package `binary-alpha-app`: the `binary-alpha` executable, configuration loading, historical-data import and verification, Parquet input and output, the filesystem and Google Cloud Storage artifact stores, and all other external adapters. |
| `configs/example.toml` | The checked-in example configuration; it contains only implemented fields and no credentials. |
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
