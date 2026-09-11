# Architecture

Binary Alpha is one process built from one Cargo workspace with two packages. Dependencies point one
way: `binary-alpha-app` depends on `binary-alpha-engine`; the engine depends on nothing in the
application and makes no file, network, cloud, broker, command-line, or device call. Strategies and
models emit typed intent; only execution communicates with a broker; mode adapters change
capabilities and input or output, never core semantics.

## Data flow implemented by the current checkout

```
configuration file ──▶ app: read text ──▶ engine: validate, canonicalize, hash ──▶ app: standard output

declared sources ──▶ app: enumerate, hash ──▶ engine: parse ticks, validate bars, identify generation
   ──▶ app: normalize, retain in the historical-data folder, publish objects ──▶ engine: ready manifest
   ──▶ app: publish the manifest last, mirror it locally ──▶ standard output

ready manifest URI ──▶ app: read manifest and objects from one store ──▶ engine: parse, validate rows
   ──▶ app: compare bytes, hashes, rows, coverage ──▶ standard output

dataset ready manifest URI ──▶ app: bind the configured instrument, decode records in order
   ──▶ engine: InstrumentStream audit, finalized candles, profile ──▶ app: candle objects, profile,
   stream manifest through the same store ──▶ standard output

input and profile manifest URIs ──▶ app: bind roles and identity, resolve or load the plan
   ──▶ engine: FeatureEngine over InstrumentStream, rows and events per accepted candle
   ──▶ app: temporary tables, one-column encoder fit and apply ──▶ engine: frozen plan, identities
   ──▶ app: plan, rows, events, encoded rows, feature manifest through the same store
   ──▶ app: reconstruct from the manifest ──▶ standard output

tick and feature manifest URIs ──▶ app: bind roles and identities, load the ticks, read the row clocks
   ──▶ engine: fold quality flags, label every decision row and expiry ──▶ app: little-endian
   arrays and matrices, outcome manifest through the same store ──▶ app: reconstruct from the
   manifest ──▶ standard output

tick, feature, and outcome manifest URIs ──▶ app: bind identities, resolve the run definition,
   merge ticks and rows by availability ──▶ engine: evaluate, admit, reserve, settle, account, risk
   ──▶ app: simulated acceptances, ledger and summary through the same store ──▶ app: restore the
   ledger through the engine ──▶ standard output
```

`binary-alpha config validate --config PATH` reads the document; the engine parses it into typed
values, rejects unknown fields and unsupported values with field-specific errors, serializes the
canonical form, and computes the versioned content hash; the application prints the hash and the
canonical document. `binary-alpha data import --config PATH` and
`binary-alpha data verify --manifest URI` are the historical-data paths described in
[docs/contracts.md](contracts.md), section "Historical datasets"; the engine supplies the records,
validators, identities, and manifest, and the application supplies file, Parquet, and cloud effects
through one artifact-store interface with a filesystem implementation and a Google Cloud Storage
implementation. `binary-alpha data audit --config PATH --manifest URI` is the instrument-stream
path described in section "Instrument streams": the engine owns the ordered state machine, the
profile, the candles, and the stream manifest, and the application feeds it one record at a time
from the published generation and publishes the outputs through the same store.
`binary-alpha features build --config PATH` is the feature path described in section "Feature
plans": the engine owns the compiled output table, the frozen plan, the per-stream feature state
over the same instrument stream, the events, and the encoder; the application binds the input and
profile generations, streams records through the engine into temporary tables, fits and applies
encodings one column at a time, and publishes the feature generation through the same store.
`binary-alpha outcomes build --config PATH` is the future-only label path described in section
"Outcomes": the engine owns the label rule, the reader, and the outcome manifest; the application
binds the tick and feature generations, loads the ticks and the rows' reference clocks, writes
the arrays and matrices, and publishes the outcome generation through the same store.
`binary-alpha replay --config PATH` is the execution path described in section "Execution": the
engine owns the exact money, the records, every decision and posting, the ledger, its restoration,
and the summary; the application binds the inputs, feeds ticks and feature rows in availability
order with the configured simulated acceptances, and publishes the replay generation through the
same store after restoring it.

## Data flow owned by later phases

The same single process grows along one causal path and one chronological path. Each stage below
names the issue that implements it; nothing on this list exists in the current checkout.

```
historical import (#3) ──┐
                         ├─▶ instrument stream and candles (#4) ─▶ features and regimes (#5) ─▶ outcomes (#6)
live feed (#11) ─────────┘                                                                            │
                                                                                                      ▼
artifacts: Google Cloud Storage, Supabase references (#3)  ◀── strategy, replay, settlement, accounting, risk (#7, this checkout)
                                                                     ▲                     │
accelerator with central-processor reference (#8) ─▶ candidate search and evaluation (#9)  │
                                                     repair, portfolio, risk tuning (#10)  │
research, optimization, certification (#12) ◀──────────────────────────────────────────────┘
live runtime and cutover (#13) ─▶ execution ─▶ broker adapter (#11)
```

## Semantic owners

| Concern | Owner | Introduced by |
| --- | --- | --- |
| Configuration meaning, validation, canonical form, content hash | `binary-alpha-engine`, module `config` | this checkout |
| Reading configuration from a path, command-line surface, exit status | `binary-alpha-app` | this checkout |
| Immutable tick and bar records, dataset roles, source capability, generation identity, ready manifests | `binary-alpha-engine`, modules `market` and `dataset` | this checkout |
| Source enumeration, normalization, the retained historical-data folder, publication, verification | `binary-alpha-app`, modules `import`, `verify`, `archive`, `store` | this checkout |
| Instrument definitions, the ordered instrument stream, profile, finalized candles, stream manifests | `binary-alpha-engine`, modules `config` and `stream` | this checkout ([#4](https://github.com/sppburke/binary-alpha/issues/4)) |
| Feeding a published generation through its stream and publishing candle objects and profiles | `binary-alpha-app`, modules `audit` and `archive` | this checkout ([#4](https://github.com/sppburke/binary-alpha/issues/4)) |
| Compiled feature outputs, frozen feature plans, per-stream feature state and events, the encoder, feature manifests | `binary-alpha-engine`, module `features` | this checkout ([#5](https://github.com/sppburke/binary-alpha/issues/5)) |
| Binding input and profile generations, temporary tables, column-wise fitting and encoding, feature publication and reconstruction | `binary-alpha-app`, modules `features` and `archive` | this checkout ([#5](https://github.com/sppburke/binary-alpha/issues/5)) |
| The future-only label rule, the outcome reader, outcome identities and manifests | `binary-alpha-engine`, module `outcomes` | this checkout ([#6](https://github.com/sppburke/binary-alpha/issues/6)) |
| Binding tick and feature generations, the little-endian outcome objects, outcome publication and reconstruction | `binary-alpha-app`, module `outcomes` | this checkout ([#6](https://github.com/sppburke/binary-alpha/issues/6)) |
| Exact money, strategy and deployment records, chronological admission, settlement, accounting, risk, the ledger and its restoration, summaries | `binary-alpha-engine`, module `execution` | this checkout ([#7](https://github.com/sppburke/binary-alpha/issues/7)) |
| Binding replay inputs, the availability merge, the historical simulation, replay publication and reconstruction | `binary-alpha-app`, module `replay` | this checkout ([#7](https://github.com/sppburke/binary-alpha/issues/7)) |
| Device kernels behind a reviewed safe boundary | a new accelerator package, only when the unsafe boundary is real | [#8](https://github.com/sppburke/binary-alpha/issues/8) |
| Candidate search, evaluation, repair, portfolio, risk tuning | `binary-alpha-engine` with thin application entry points | [#9](https://github.com/sppburke/binary-alpha/issues/9), [#10](https://github.com/sppburke/binary-alpha/issues/10) |
| Broker contracts and adapters, secrets resolution | `binary-alpha-app` | [#11](https://github.com/sppburke/binary-alpha/issues/11) |
| Research orchestration, holdout grants, certification | `binary-alpha-app` over engine stages | [#12](https://github.com/sppburke/binary-alpha/issues/12) |
| Live runtime, authorization, resumable cutover | `binary-alpha-app` | [#13](https://github.com/sppburke/binary-alpha/issues/13) |

The engine stays free of external effects so that development, evaluation, optimization,
certification, replay, and live operation run the same validated semantics; adapters in the
application supply inputs and consume outputs.
