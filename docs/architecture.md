# Architecture

Binary Alpha is one process built from one Cargo workspace with three packages. Dependencies point one
way: `binary-alpha-app` depends on `binary-alpha-engine` and uses `binary-alpha-accelerator` for
offline kernel proof; the accelerator depends on neither package. The engine depends on nothing in the
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

research table and governance declaration ──▶ app: permit every declared input, publish the attempt
   intent ──▶ audit, feature, outcome, search, and portfolio owners per instrument ──▶ engine:
   frozen stage, qualification descriptor ──▶ app: claim the outer populations, apply the refit
   plans, replay every scenario ──▶ engine: projection, verdict, aggregate ──▶ app: run record and
   manifest (the bundle), awaiting authorization ──▶ operator grant ──▶ app: protected claims,
   receipt, certification context ──▶ the same plans and scenarios over holdout ──▶ app:
   certification record and manifest ──▶ standard output

verified public bundle records ──▶ engine: live_policy, exact baseline and broker request templates
   ──▶ existing execution definition and financial identity owners

ordered financial events ──▶ app: append-only journal, closed segments
account ownership and prepared signal ──▶ app: PostgreSQL lease and dispatch-claim transactions
exact deployment bindings ──▶ app: immutable authorization through the existing artifact store
```

The Phase 12 configuration, projection, journal, control, and authorization owners above are
implemented. Their ordered application composition below is **design-specified** (binding design
3.4–3.6, 4.3, and 5); the current `live.rs` exports the implemented support modules. The receipt,
recorded transport, deployment-manifest, and live-command consumers remain to be reconciled with
the checkout. This is the Phase 12 flow, with no additional feature or financial authority:

```text
broker market/account tasks or recorded transports (input/output only)
   ──▶ one ordered ingress lane: connection generation, receipt sequence, source clocks
   ──▶ one shared causal feature state: existing InstrumentStream and FeatureEngine per instrument
   ──▶ one Engine: ordered evaluation, capacity, risk, reservations, settlement, accounting
   ──▶ one execution adapter: rate admission ─▶ committed dispatch claim ─▶ eligibility check/write
   ──▶ typed broker observations ──▶ the same Engine

every transition ──▶ one single-writer journal ──▶ one publisher through the existing store
   ──▶ verified immutable closed segments, ledger, compatibility receipt
verified bundle + exact configuration ──▶ immutable deployment manifest ──▶ exact entry authorization
```

One process owns one deployment bundle and one execution account across all configured instruments
and strategies. Broker tasks may perform concurrent network input/output; they never evaluate a
strategy or mutate financial state, and execute only prepared intents from the ordered owner.
Each accepted market event enters the shared feature owner once; each available base row is
evaluated once in frozen binding order. Keep synchronous broker/storage operations and lease
renewal outside the ordered decision task; the remote dispatch-claim commit is the required
pre-purchase durability boundary. This issue #13 requirement governs the inline broker/storage
calls sketched in design 3.5; application task isolation still needs implementation proof.
The same Engine restores the journal and applies authoritative
account evidence. Entry disabling preserves account observation, settlement, reconciliation,
journaling, and cloud retry. Contracts and implementation status are in
[Live runtime](contracts.md#live-runtime); separately authorized handoff is in
[operations](operations.md#live-runtime).

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
`binary-alpha research run --config PATH` and `binary-alpha holdout grant create` are the
research path described in section "Research": the engine owns the governance declaration and
read permits, every research record and identity, the lowering into the existing tables, the
qualification descriptor and verdicts, and the certification context; the application owns the
fixed sequence over the existing owners, the conditional governance records, and the verifiers.
`binary-alpha search --config PATH` is the candidate-search path described in section "Search":
the engine owns enumeration, family identity, the model score and its adjustment, the sampler,
the gates and the ranking; the application binds one development and one optional evaluation
input, lowers every condition and replays every survivor through the replay owner, scores the
family through the accelerator boundary, resamples settlement paths through the retained
bootstrap primitive, and publishes the family generation through the same store after verifying
it.

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
| Device kernels behind a reviewed safe boundary; central-processor reference as the raw-result oracle, Engine as the final chronological audit | `binary-alpha-accelerator` | this checkout ([#8](https://github.com/sppburke/binary-alpha/issues/8)) |
| Candidate enumeration, family identity, the binomial model score and its adjustment, the stationary-block sampler, development gates and ranking, the family records | `binary-alpha-engine`, module `search` | this checkout ([#9](https://github.com/sppburke/binary-alpha/issues/9)) |
| Binding search inputs, the lowering and chunk replays, device scoring, stability resampling, family publication and verification | `binary-alpha-app`, module `search` | this checkout ([#9](https://github.com/sppburke/binary-alpha/issues/9)) |
| Repair, portfolio, risk tuning | `binary-alpha-engine` with thin application entry points | [#10](https://github.com/sppburke/binary-alpha/issues/10) |
| Broker contracts and adapters, secrets resolution | `binary-alpha-app` | [#11](https://github.com/sppburke/binary-alpha/issues/11) |
| Governance declarations and read permits, research records and identities, lowering into the existing tables, qualification, the certification context | `binary-alpha-engine`, module `research` | this checkout ([#12](https://github.com/sppburke/binary-alpha/issues/12)) |
| The research sequence over the existing owners, governance records, grants, receipts, research verification | `binary-alpha-app`, module `research` | this checkout ([#12](https://github.com/sppburke/binary-alpha/issues/12)) |
| Live configuration, baseline projection, exact economic comparison | `binary-alpha-engine`, modules `config`, `research`, `execution` | this checkout ([#13](https://github.com/sppburke/binary-alpha/issues/13)) |
| Durable local records, hash chain, segment restoration and cleanup | `binary-alpha-app`, module `live::journal` | this checkout ([#13](https://github.com/sppburke/binary-alpha/issues/13)) |
| Two-table migration, lease/claim transactions, encrypted PostgreSQL connection, fake control | `binary-alpha-app`, module `live::control` | this checkout ([#13](https://github.com/sppburke/binary-alpha/issues/13)) |
| Immutable authorization object, identity, creation and read validation | `binary-alpha-app`, module `live::authorization`, through the existing `store` | this checkout ([#13](https://github.com/sppburke/binary-alpha/issues/13)) |
| Transport receipt provenance and purchase preparation/write boundary | `binary-alpha-app`, modules `broker` and `broker::deriv_options` | this checkout ([#13](https://github.com/sppburke/binary-alpha/issues/13)) |
| One ingress lane, live definition, ordered runtime, recovery, entry gates, health, deployment manifest and publication | `binary-alpha-app`, module `live`, reusing `features`, `replay`, `research`, and `store` owners | Phase 12, design-specified pending application composition |
| Deterministic execution-compatibility receipt | `binary-alpha-app`, module `live::receipt` | Phase 12, design-specified pending implementation |
| Recorded broker transports and replay clock | `binary-alpha-app`, module `broker::transport` | Phase 12, design-specified pending implementation |
| Live commands and operator authorization command | `binary-alpha-app`, `main` dispatch into `live` | Phase 12, design-specified pending registration |
| Resumable account handoff and rollback | Authorized operator, [procedure](operations.md#live-runtime) | Phase 12; production execution requires separate authorization |

The engine stays free of external effects so that development, evaluation, optimization,
certification, replay, and live operation run the same validated semantics; adapters in the
application supply inputs and consume outputs.
