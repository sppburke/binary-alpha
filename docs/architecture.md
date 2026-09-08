# Architecture

Binary Alpha is one process built from one Cargo workspace with two packages. Dependencies point one
way: `binary-alpha-app` depends on `binary-alpha-engine`; the engine depends on nothing in the
application and makes no file, network, cloud, broker, command-line, or device call. Strategies and
models emit typed intent; only execution communicates with a broker; mode adapters change
capabilities and input or output, never core semantics.

## Data flow implemented by the current checkout

```
configuration file ──▶ app: read text ──▶ engine: validate, canonicalize, hash ──▶ app: standard output
```

`binary-alpha config validate --config PATH` is the only runtime path. The application reads the
document; the engine parses it into typed values, rejects unknown fields and unsupported values with
field-specific errors, serializes the canonical form, and computes the versioned content hash; the
application prints the hash and the canonical document.

## Data flow owned by later phases

The same single process grows along one causal path and one chronological path. Each stage below
names the issue that implements it; nothing on this list exists in the current checkout.

```
historical import (#3) ──┐
                         ├─▶ instrument stream and candles (#4) ─▶ features and regimes (#5) ─▶ outcomes (#6)
live feed (#11) ─────────┘                                                                            │
                                                                                                      ▼
artifacts: Google Cloud Storage, Supabase references (#3)  ◀── strategy, replay, settlement, accounting, risk (#7)
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
| Immutable tick and bar records, dataset roles, source capability, manifests | `binary-alpha-engine` | [#3](https://github.com/sppburke/binary-alpha/issues/3) |
| Instrument profile and causal candles | `binary-alpha-engine` | [#4](https://github.com/sppburke/binary-alpha/issues/4) |
| Causal features and regimes | `binary-alpha-engine` | [#5](https://github.com/sppburke/binary-alpha/issues/5) |
| Future-only binary-expiry outcomes | `binary-alpha-engine` | [#6](https://github.com/sppburke/binary-alpha/issues/6) |
| Strategy intent, chronological execution, settlement, accounting, risk | `binary-alpha-engine` | [#7](https://github.com/sppburke/binary-alpha/issues/7) |
| Device kernels behind a reviewed safe boundary | a new accelerator package, only when the unsafe boundary is real | [#8](https://github.com/sppburke/binary-alpha/issues/8) |
| Candidate search, evaluation, repair, portfolio, risk tuning | `binary-alpha-engine` with thin application entry points | [#9](https://github.com/sppburke/binary-alpha/issues/9), [#10](https://github.com/sppburke/binary-alpha/issues/10) |
| Broker contracts and adapters, cloud storage, secrets resolution | `binary-alpha-app` | [#3](https://github.com/sppburke/binary-alpha/issues/3), [#11](https://github.com/sppburke/binary-alpha/issues/11) |
| Research orchestration, holdout grants, certification | `binary-alpha-app` over engine stages | [#12](https://github.com/sppburke/binary-alpha/issues/12) |
| Live runtime, authorization, resumable cutover | `binary-alpha-app` | [#13](https://github.com/sppburke/binary-alpha/issues/13) |

The engine stays free of external effects so that development, evaluation, optimization,
certification, replay, and live operation run the same validated semantics; adapters in the
application supply inputs and consume outputs.
