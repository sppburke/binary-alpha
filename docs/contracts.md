# System contracts

These contracts bind every phase of Binary Alpha. The current checkout implements configuration,
historical datasets, causal instrument streams, features and outcomes, the shared execution engine,
NVIDIA CUDA kernels, candidate search and portfolio selection, and the Phase 10 broker adapters,
history acquisition and non-purchasing inspection, research and certification, and the Phase 12
ordered live runtime, projection, journal, control, authorization, recorded replay, and compatibility
receipts. Specification intent, checkout
implementation, observed runtime state, immutable measured artifacts, and hosted Git state are
distinct kinds of truth and are never substituted for one another.

## Time

Seven clocks are distinct. A record names which clock each timestamp carries; no field is
"the timestamp".

| Clock | Meaning | Set by |
| --- | --- | --- |
| Provider event time | The time the provider attaches to a tick, bar, quote, contract, or settlement. | Provider |
| Local receipt time | The time this process received the payload. | Ingestion |
| Engine decision time | The time a decision is stamped. It never precedes the provider event time of any input the decision used. In replay it is derived from the input stream, never from the wall clock. | Engine |
| Order dispatch time | The time execution handed an order to the broker transport. | Execution |
| Entry time | The contract start the broker confirmed. | Broker, recorded by execution |
| Due time | The contract expiry the broker confirmed. | Broker, recorded by execution |
| Settlement time | The time the outcome became final. | Broker or settlement rule, recorded by settlement |

Timestamps are Coordinated Universal Time with a declared precision. Durations carry explicit
units; a bare number is never a duration. A computation stamped at decision time `D` may read only
inputs whose provider event time is at or before `D`; in live operation the input must also have been
received. Replay and live operation apply the same rule through the same implementation.

## Ordering and provenance

Every ingested payload retains its source identity, the provider sequence when the provider supplies
one, a local receipt sequence that is monotonic per source, the parser and adapter version, and a
payload identity. Gaps, duplicates, stale data, reconnects, backpressure, and reconciliation are
recorded as explicit events with their clocks; none is silent, and none is repaired by fabrication.
Cross-source merges order by provider event time, then a configured stable source order, then a
canonical record order, so the merged sequence is reproducible. Historical inputs imported from
existing files carry no observed receipt metadata: their local receipt time and receipt sequence
are unavailable and are never fabricated. Live ingestion retains local receipt sequencing.

## Instruments, ticks, and bars

`Tick` and `Bar` are distinct records. A source declares which of them it can provide. There is no
universal market event with optional fields, and no implicit conversion from bars to ticks. A
bar-only source cannot satisfy a request that needs a native tick path or tick count. Historical
research accepts bars under the [outcome observation binding](#outcomes). An instrument is a
neutral typed identifier bound to a broker and a provider symbol,
rendered `BROKER:PROVIDER_SYMBOL`; both parts are non-empty and contain no ASCII control character.
The owning phase declares its currency metadata and records its observed profile rather than
assuming one.

## Currency and money

Currency, amount, price or tick, quantity, probability, payout, fee, foreign-exchange rate, side,
timestamp, and settlement values are distinct types. At money, order, accounting, and risk boundaries
an amount is a checked integer or decimal in a declared currency and scale; binary floating point is
allowed only inside feature, model, and device computation. Rounding rules, conversion ownership,
the foreign-exchange source and its freshness, the reporting currency, and exposure aggregation are
declared explicitly by the phase that introduces them. A payout is a property of one quoted contract
at one time; the system never keeps a global remembered payout.

## Artifacts

Artifacts are immutable and identified by content. Research artifacts may be published to a local
filesystem store or Google Cloud Storage; non-research run modes require Google Cloud Storage;
Supabase stores references and proved transactional or metadata needs, never duplicate
bulk or execution truth. Every artifact records its schema version, the producing code revision, the
resolved configuration hash, and the identities of its inputs. A completed evidence identity is never
overwritten.

## Dataset roles

A dataset generation carries exactly one role: development, evaluation, or holdout. Development and
evaluation feed research. Holdout is one-way: its objects, metrics, artifacts, credentials,
observations, pass or fail detail, and retries cannot influence features, tuning, ranking, stopping,
defaults, or another research iteration. Candidate and configuration identity freeze before terminal
certification.

## Strategy

Strategies and models emit typed intent. Only execution communicates with a broker. One causal
ingestion, data, and feature implementation and one chronological execution, settlement, accounting,
and risk implementation serve development, evaluation, optimization, certification, replay, and live
operation. Mode adapters change capabilities and input or output, never core semantics.

## Settlement and order state

An order is in exactly one of: not sent, sent, acknowledged, accepted, rejected, partially filled or
open, possibly sent, settled, or reconciled. A submission in the unknown or possibly-sent state is
never retried before reconciliation. Settlement uses the confirmed entry and due times and the
declared settlement rule; a tie is settled by the rule, never assumed.

## Holdout

Locked holdout data, its access grants, and its certification results are protected state. Research
success, certification, merge, deployment, paper operation, and live operation are distinct states
with distinct authorizations; none implies the next.

## Configuration

The configuration document uses Tom's Obvious, Minimal Language (TOML). The engine package owns its
meaning, validation, canonical form, and content hash; the application package owns reading it
from a path.

### Schema version 1

| Field | Type | Accepted values |
| --- | --- | --- |
| `schema_version` | integer | `1` |
| `run_mode` | string | `research`, `replay`, `paper`, `live` |
| `storage.historical_data_dir` | string | a non-empty path of the retained historical-data folder; a relative path resolves against the configuration file's directory |
| `storage.publication_uri` | string | `gs://BUCKET` or `gs://BUCKET/PREFIX` in every run mode; `file:///ABSOLUTE/DIR` only with `run_mode = "research"`, for all research, including splits, research runs, holdout grants, and certification |
| `import.sources` | array of tables | optional; consumed only by `data import`, which requires at least one entry |
| `split` | table | optional; consumed only by `data split`; declares `namespace`, development daily-root `sources`, nonempty `development` and `evaluation` arrays, and a `holdout` array that may be empty; every window is a whole-day `{ start, end }` range |
| `instruments` | array of tables | optional; maps audit generations and selected broker history/live instruments |
| `features.instruments` | array of tables | optional; consumed only by `features build`, which requires at least one entry |
| `outcomes` | table | optional; consumed only by `outcomes build`, which requires it |
| `replay` | table | optional; historical simulation through `replay`, with exact contracts, envelopes and risk policies described under [Execution](#execution) |
| `accelerator.backend` | string | optional section; explicit offline backend `cpu` or `cuda` |
| `search` | table | optional; consumed only by `search`, which requires it |
| `portfolio` | table | optional; consumed only by `portfolio optimize`, which requires it |
| `research` | table | optional; consumed only by `research run` and `holdout grant create`, which require it; described under [Research](#research) |
| `brokers` | array of tables | optional; unique broker ids and compiled `deriv` or `pocket_option` connection settings |
| `history` | table | optional; required by `data fetch` and `broker inspect` |
| `inspect` | table | optional; required by `broker inspect` |

The optional `research` table follows `portfolio`, and the optional broker tables follow it in
canonical order: `[[brokers]]`, `[history]`, then `[inspect]`. All reject unknown fields and are omitted when absent, preserving existing
configuration hashes. Every broker entry starts with `kind` and then `id`. A Deriv entry then
contains `public_endpoint`, `bootstrap_endpoint`, `app_id`, optional `credential`, optional
`account_class` (`demo` or `real`, required with a credential), and optional `budgets`.
`app_id` is a non-secret application identifier sent as `Deriv-App-ID` during bootstrap.
`budgets` contains `trade`, `account`, `portfolio`, and `other`, each with positive `per_minute`
and `per_hour` no greater than the limits in [Broker access](#broker-access); absence uses those
limits. A Pocket Option entry instead contains `endpoint`, optional `origin`, required
`credential`, `account_class` (`demo` or `real`), `server_offset_minutes` (no default), and
optional `credential_command` (a program and its arguments, first element nonempty). The
program prints a fresh authentication object to standard output; the application runs it when
the referenced variable is unset and once more after a first connection fails, then retries the
connection with the printed object. The program is operator tooling outside this repository and
its output never enters a diagnostic.
Credentials are environment-variable names, never their values.

`history` declares `broker`, a nonempty unique list of provider-symbol `instruments`, `role`
(`development` or `evaluation`; `holdout` is rejected), `start`, `end`, and optional positive
`refresh_interval_seconds`. Start and end are universal-time text forming a nonempty half-open
range. Every selection must resolve to a declared broker and a matching `[[instruments]]` entry
with the same native granularity. The additional history fields are:

- `native_granularity`: `{ kind = "tick" }` by default, or
  `{ kind = "bar", period_seconds = N }` with a positive unsigned 16-bit period. No other keys
  are accepted; `period_seconds` is required for bars and forbidden for ticks. Fetch admits only
  five-second bars.
- `seeds`: an empty list by default. Each entry contains `provider_symbol`, `manifest` (a
  `file://` or `gs://` ready-manifest location), and `source_identity` (exactly sixty-four
  lowercase hexadecimal digits). The symbol must occur in `history.instruments` and may have
  only one seed. The binding is checked against the configured broker before broker connection;
  a digest alone does not establish that an operator's archive came from that source context.
- `overlap_seconds`, `max_pages`, and `max_elapsed_seconds`: optional positive unsigned 32-bit
  integers with no configured default. The pipeline requires all three and uses them for
  frontier overlap and per-invocation acquisition limits. Standalone `data fetch` retains its
  explicit-range, foreground behavior without these page/time limits.

Canonical output omits tick `native_granularity`, empty `seeds`, and absent overlap/page/time
limits. A seed entry rejects unknown keys. Existing configurations that omit these fields keep
their canonical form.

`inspect` declares positive `live_observations` and `live_seconds`,
then optional `proposal = { stake = "10", duration_seconds = 15 }` with positive exact stake and
duration. Proposal inspection requires the history broker to support execution and have a
credential reference. Capability checks run in `Config::validate`, before connection. `ws://` and
`http://` endpoints are permitted only under `run_mode = "research"`;
otherwise WebSocket endpoints use `wss://` and the bootstrap uses `https://`.

The new execution fields retain their enclosing records' canonical order:
`replay.contracts[].semantics` follows `settlement`,
`replay.bindings[].envelope.semantics` follows `settlement_rule`, and
`replay.risk_policies[].max_proposal_age_micros` follows `pause`. They are optional and omitted
when absent. Broker-authoritative bindings require `rise_fall_strict_v1` in both contract and
envelope, and a finite non-negative local proposal-age limit; historical bindings omit semantics.

Every `import.sources` entry declares `kind`, `path` (a relative path resolves against the
configuration file's directory), `broker`, and `role` (`development` or `evaluation`; `holdout` is
rejected). Kind `tick_csv` names one native tick file and also declares `provider_symbol`,
`source_symbol` (the symbol text every row must carry), and `price_scale` (`0` to `18`). Kind
`bar_parquet_collection` names one collection root and also declares `manifest` (the collection
manifest inside that root), an optional `provenance` list of further files inside that root, and
optional `instruments` containing collection asset names. An absent selection imports every
listed asset and is omitted from canonical output. A present selection must be nonempty, contain
unique nonempty names without ASCII (American Standard Code for Information Interchange) control
characters, and name assets present in the collection. Configuration validates the names and
duplicates; import checks membership and filters before opening unselected asset trees.
Kind `tick_parquet_daily` names one daily tick archive root and also declares `price_scale` and
`instruments`, a non-empty list of unique directory names beneath that root. `manifest` and
`provenance` entries contain only normal path components; each `instruments` entry is one normal
path component without an ASCII control character. Broker and symbol values are non-empty and
contain no ASCII control character. Duplicate source paths are rejected.

Every `instruments` entry declares, in this order: `broker` and `provider_symbol` (the neutral
instrument identity `BROKER:PROVIDER_SYMBOL` the entry maps); `base_currency` (optional; absent for
a stock, index, or commodity, never invented) and `quote_currency`, both non-empty codes without an
ASCII control character; `price_scale` (`0` to `18`), the integer-unit scale of every candle price,
which an integer-unit source must already carry and into which a binary floating-point bar source
converts exactly or is rejected; `native_granularity`, `{ kind = "tick" }` or
`{ kind = "bar", period_seconds = N }` with a positive period and no other key, which the audited
generation must provide; the optional check tables `gap` (`max_seconds` and a larger
`reopen_seconds`), `frozen` (positive `min_observations` and `min_seconds`), `jump` (positive
`min_basis_points`), and `span` (`min_percent`, `0` to `100`), each enabled by its presence; the optional `sessions` list of weekly windows, each with a unique non-empty
`name` and `open_seconds` less than `close_seconds` at most `604800` (seconds since Monday
00:00 Coordinated Universal Time), pairwise non-overlapping and non-empty when present; and the
non-empty `candles` list, each stream declaring a positive `duration_seconds`, an
`offset_seconds` below the duration, and the optional `min_observations` and
`hard_min_observations` counts. A bar instrument's durations and offsets are multiples of its bar
period. Two entries may map one identity only at different native granularities; the same
duration and offset pair is listed once per entry. Ordering, causality, interval boundaries,
finite values, and source capability are stream invariants that no field relaxes.

Every `features.instruments` entry declares, in this order: `role` (`development` or
`evaluation`; `holdout` is rejected before anything is resolved); `input_manifest`, the ready
manifest of the Phase 02 generation to compute on; `profile_manifest`, the ready manifest of the
Phase 03 stream generation whose recorded definition binds the candle streams, quality checks,
and price scale and whose profile records the source capabilities; and either `frozen_plan`, the
ready manifest of a completed feature generation whose plan is applied unchanged, or the new-plan
settings below, which require `role = "development"`. Manifest locations use the
`manifests/GENERATION/ready.json` grammar of `data verify`. One `profile_manifest` may serve
several entries (a fit and the frozen applications of its plan); every resolved instrument, role,
and stream has one owning entry, checked at build before anything is streamed. A frozen plan admits no
new-plan setting. The new-plan settings are all optional at parse time; the resolver requires
exactly the ones the selected outputs and their compiled prerequisites need and names a missing
one: `streams`, unique positive duration and smaller offset pairs, each a stream of the bound
definition; `outputs`, the string `all_supported` or a unique non-empty list of compiled output
identifiers; `moving_average_periods`, sorted unique integers greater than one; `rolling_window`
and `min_history`, declared together with `1 <= min_history <= rolling_window`; `structure`, the
table of `swing_left`, `swing_right`, `rolling_windows` (positive, sorted, unique),
`direction_window` (one of the rolling windows), `trend_efficiency_threshold` and
`range_efficiency_threshold` in `[0, 1]`, finite non-negative `trend_min_abs_momentum_bps`,
finite positive strictly increasing `compression_ratio_threshold`, `expanded_ratio_threshold`,
and `extreme_ratio_threshold`, and positive `pullback_min_trend_age`,
`trend_reset_sideways_bars`, and `failed_breakout_max_bars`; `price_epsilon`, non-negative
decimal price text (`"0"` is strict equality) whose units resolve at the bound profile's price
scale when the plan resolves; `tick_path_streams`, a unique subset of `streams`; and `encodings`, a table of
`max_labels` (`1` to `32768`) and `outputs`, a unique list of `{ output, bins }` entries where
`bins` is absent for a category output or a compiled projection and is either
`"development_fifths"` or a finite strictly increasing list of right-closed edges for a numeric
output. Section [feature plans](#feature-plans) gives the resolution rules.

Section [outcomes](#outcomes) gives the fields of the `outcomes` table.

The optional `accelerator` table declares `backend` (`cpu` or `cuda`) and rejects
unknown fields. It follows `replay` in canonical order. Every application command
loading a configuration rejects `cuda` when built without the `cuda` feature, with a diagnostic
naming the missing feature. `cpu` selects the central-processor reference; `cuda` requests the
ahead-of-time native module and fails clearly if the binary, driver, device, or module is unavailable
or incompatible.
There is no implicit device fallback. Omitting the section preserves the previous
configuration hash. This offline selection does not change Engine execution;
accelerator results have no production consumer in this phase.

Device conditions are equality tests on fitted-encoding codes in a feature-major matrix. Flattened
`condition_feature` and `condition_bucket` buffers and `candidate_offsets` delimit a nonempty,
variable-length conjunction per candidate; there is no four-condition storage limit. The host
prepares buffers from the published generations, applies the Engine's readiness and unready rules
while encoding, and aligns other-stream columns to base rows by the Engine's latest-row rule.
The kernels perform neither alignment nor non-equality comparisons. The central-processor
reference reproduces each kernel's arithmetic and operation order exactly and is the raw-result
oracle. The Engine is the final chronological and financial audit: device results never suppress,
admit, or settle Engine observations.

Every field is required and has no default, except that the `import` table, the `provenance`
list, the `instruments` list, the `features` table, the `outcomes` table, the `accelerator`
table, the `replay`, `search`, `portfolio`, `brokers`, `history`, and `inspect` sections, and the optional
instrument and feature fields named above may be absent. Any
other field is rejected as unknown, so a raw secret value has no place to live. Validation opens
no source or destination and mutates nothing.

### Deferred entries

Live runtime settings are defined in [Live runtime](#live-runtime); terminal certification grants
are defined in [Research](#grant-claims-receipt-and-certification).
Broker connections, funded accounts, features, contract terms, research splits, objectives, and
risk policies already have implemented owners. New fields must retain neutral typed identifiers,
explicit durations and clocks, exact currency-bearing amounts, credential references, and rejection
of unsupported combinations; none may relax causality, holdout isolation, or financial invariants.

### Run modes

A run mode selects capabilities and input or output, never semantics. `research` runs development,
evaluation, optimization, and, under a separate operator grant, locked-holdout certification;
`replay` drives the live runtime from a recorded event log without broker mutation; `paper` runs the
live path without real orders; `live` places real orders under its own authorization.

### Canonical form

The canonical document serializes the validated configuration with keys in the schema-table order,
one key per line, standard TOML formatting, double-quoted strings, no comments, and a trailing
newline. Top-level keys come first; the `[storage]` table, each `[[import.sources]]` entry, and each
`[[instruments]]` entry follow
in schema order, each introduced by one blank line and its header, with a source entry's `kind`
first and its remaining keys in the order of the table above. An instrument entry lists its scalar
keys in the declared order, then its `[instruments.native_granularity]`, `[instruments.gap]`,
`[instruments.frozen]`, `[instruments.jump]`, and `[instruments.span]` tables, then its
`[[instruments.sessions]]` and `[[instruments.candles]]` entries, each present table or entry
introduced by one blank line and its header. An instrument entry rendered alone as a document in
the same order is its canonical definition, the text a stream generation's identity hashes. Each
`[[features.instruments]]` entry follows the instrument entries, its scalar keys in the declared
order, then its `[features.instruments.structure]` and `[features.instruments.encodings]` tables
and `[[features.instruments.encodings.outputs]]` entries. Two
documents with the same values have the same canonical form regardless of key order, whitespace,
or comments.

### Content hash, version 3

The content hash is SHA-256 over the bytes `binary-alpha config hash v3`, one line feed, and the
canonical document. It is rendered as `v3:sha256:` followed by sixty-four lowercase hexadecimal
digits. Any change to the hash input or to the canonical form increments the version prefix; the
optional `features` table changed neither for a document that omits it, so such a document keeps
its version-3 identity.
Version 2 hashed the canonical form without `instruments` under the domain
`binary-alpha config hash v2`, and version 1 the two-field envelope of the first checkout under
`binary-alpha config hash v1`; a hash recorded under an earlier version is never reinterpreted.

### Validation output

`binary-alpha config validate --config PATH` writes to standard output the line
`# content-hash: v3:sha256:...` terminated by a line feed, then the canonical document, and exits
with status 0. It writes nothing else and mutates nothing. On failure it writes nothing to standard
output, writes one diagnostic to standard error, and exits with status 1: a document error names the
offending or missing key and, for a present value, its line and column; an unreadable path is
reported with the operating-system error. Validating the canonical output again yields the same
canonical document and hash.

## Historical datasets

The engine owns the immutable `Tick` and `Bar` records, dataset roles, source capabilities,
instrument identity, generation identity, and the ready manifest; the application owns reading
sources, the retained historical-data folder, publication, and verification.

### Layout and identity

Every retained or published object is content-addressed at the relative key `objects/SHA256HEX`
beneath both the historical-data folder and the publication destination. A generation's identity is
the SHA-256, rendered as sixty-four lowercase hexadecimal digits, of the UTF-8 text
`binary-alpha dataset generation v1`, one line feed, then the broker, provider symbol, source kind,
and role each followed by one line feed, then the price scale and one line feed for tick sources,
then one line `ROLE`, tab, `PATH`, tab, `SHA256HEX`, tab, `BYTES`, line feed per input object sorted
by path, where `ROLE` is `source` or `provenance` and `PATH` is the object's path relative to its
dataset root. Normalized outputs are not part of the identity, so equal inputs always name the same
generation. Its ready manifest lives at `manifests/GENERATION/ready.json` beneath the same two roots
and is the dataset's publication record. The research data pipeline's archive catalog references
these original ready manifests without changing them. Object paths, broker, and provider symbol
contain no ASCII control character.

### Ready manifest

The ready manifest is pretty-printed JSON with two-space indentation, keys in the order below, and
one trailing line feed. It records `schema_version` (`1`); `generation`; `broker`; `provider_symbol`;
`instrument` (`BROKER:PROVIDER_SYMBOL`); `role`; `source_kind` (`tick_csv`, `tick_parquet_daily`,
`bar_parquet`, or `broker_history`);
`native_granularity` (`{"kind": "tick"}` or `{"kind": "bar", "period_seconds": N}`); `time_unit`
(`microsecond` for ticks, `second` for bars); `price_representation`
(`{"kind": "integer_units", "scale": N}` or `{"kind": "binary_float64"}`); `coverage` with
`first_event_time` and `last_event_time` rendered as `YYYY-MM-DDTHH:MM:SS.ffffffZ`; `row_count`;
`capabilities` (`["ticks"]` or `["bars"]`); `config_hash`; `code_revision` (the producing Git commit,
`-dirty` appended for a dirty tree, or `unavailable`); `inputs` (every original input location with
`bytes` and `sha256`); `interval` (the bar archive's contract with its `provenance`, `null` for
ticks); and `objects`, each with `role` (`source`, `provenance`, or `normalized`), `path`, `key`,
`bytes`, `sha256`, `crc32c`, and `generation`. The last two are the destination's values and are
`null` when the destination is the filesystem implementation. Consumers derive every rejection from
`capabilities`: a request for a capability the list lacks fails with a machine-readable reason that
names the required capability, the provided list, the instrument, and the generation.

### Tick sources

A native tick file has the exact header `time_utc,symbol,price`. Every row carries the declared
`source_symbol`; a timestamp `YYYY-MM-DDTHH:MM:SS[.fraction]Z` with at most six fraction digits,
converted to Unix microseconds by integer arithmetic; and decimal price text converted directly into
checked signed 64-bit units at the declared `price_scale`, rejecting overflow, non-numeric text, and
more fraction digits than the scale. Rows keep their input order; backwards time and two rows at one
timestamp with different units are rejected, while identical repeated rows are accepted once each.
The normalized object `normalized/ticks.parquet` is Zstandard Parquet with the required `int64`
columns `event_time_micros` (`TIMESTAMP(MICROS, true)`) and `price_units`, and file metadata
`broker`, `provider_symbol`, `price_scale`, and `dataset_schema_version`. The raw file is retained
byte-for-byte as the `source` object. No provider sequence, receipt sequence, or receipt time is
fabricated.

### Daily tick archive sources

A daily tick archive root holds one directory per instrument, and `data import` opens only the
listed directories. Each listed directory must resolve beneath the archive root, must neither
contain nor lie inside either destination, and holds only regular files named
`NAME_YYYY-MM-DD_ticks.parquet` and `NAME_YYYY-MM-DD_ticks.meta.json`, where `NAME` is the
directory name and the date is a valid calendar date; any other file, nested directory, or symbolic
link is rejected, and at least one Parquet file is required. Each metadata file is JSON whose
`calendar` is `UTC`, whose `date` equals the date in its name, and whose `ticks` is a non-negative
integer; every metadata file in the directory records the same `symbol`, which is the generation's
provider symbol. A Parquet day requires its metadata file, whose `ticks` must equal the file's row
count; a metadata-only day must record `ticks` `0`. Every Parquet file carries exactly the schema
`datetime_utc` (`int64`, `TIMESTAMP(NANOS, true)`) and `price` (`double`), both optional. Per row:
no nulls, a timestamp on a whole microsecond inside the file's calendar day, and a price whose
shortest round-trip decimal rendering converts under the tick-source price rule at the declared
`price_scale`. Rows keep their order, across days in date order, under the tick-source sequence
rules. Parquet days are `source` objects and metadata files are `provenance` objects, each at its
file name, in date order; metadata bytes parsed while planning must be the bytes retained. The
normalized object, its file metadata, `time_unit`, price representation, and capabilities are
exactly those of a tick source, the ready manifest records `source_kind` `tick_parquet_daily`,
and `data verify` decodes the generation from its normalized object exactly as a `tick_csv`
generation. Archive bytes are never re-encoded.

### Bar sources

A collection manifest lists assets; an optional source `instruments` selection limits the assets
opened. Each selected asset root lies inside the collection root and holds
`dataset/` with the listed monthly Parquet files. Every listed file must carry exactly the archive
schema (symbol, symbol identifier, timestamp, Unix seconds, server seconds, five doubles, period)
with Zstandard column chunks. Embedded interval metadata, when present, must equal the declared
contract; when absent, the collection manifest must declare the contract and its provenance, which
the ready manifest preserves (`parquet_metadata` when every file embeds it). Per row: no nulls, the
symbol equals the asset, one constant symbol identifier equal to the manifest's expected or recorded
identifier, the period equals the declared one, finite prices and volume, non-negative volume, high
at least the greater of open and close, low at most the lesser of open and close, Unix seconds on the
period grid, the timestamp column equal to the Unix seconds, the server seconds minus the Unix
seconds equal to the recorded server offset, and strictly increasing time across files in manifest
order. Per-file rows and SHA-256 equal the manifest, and the total equals the recorded row count.
The collection manifest must declare exactly the approved contract (left-closed, `5s`,
`[timestamp,timestamp+5s)`, label `left`, offset `0`, origin `unix_epoch_utc`, semantics
`bar_start`) and an expected or recorded symbol identifier for every asset. Listed files are
`source` objects in manifest order; every other regular file beneath the asset root, the collection
manifest, and the declared `provenance` files follow as `provenance` objects, the collection-level
ones at `collection/NAME`. Symbolic links anywhere in the declared inventory are rejected. Parquet
bytes are never re-encoded. A bar generation provides only `bars`.

### Broker-history bars and coverage

A native broker-history bar generation admits exactly `source_kind = "broker_history"`,
`price_representation = { kind = "binary_float64" }`,
`native_granularity = { kind = "bar", period_seconds = 5 }`, `time_unit = "second"`, the
five-second interval contract, and `capabilities = ["bars"]`. It contains source objects,
`provenance/coverage.json`, and exactly one normalized object, `normalized/bars.parquet`.
Tick generations keep `normalized/ticks.parquet` and their existing representation.

`write_bars` writes Zstandard Parquet with these eleven optional columns, populated without nulls:

| Column | Physical type and annotation | Value |
| --- | --- | --- |
| `symbol` | byte array, UTF8 (Unicode Transformation Format, 8-bit) | provider symbol |
| `symbol_id` | int32, signed 32-bit | constant provider identifier |
| `timestamp_utc` | int64, `TIMESTAMP(MICROS,true)` | bar start in Unix microseconds |
| `unix_utc_s` | int64, signed 64-bit | bar start in Unix seconds |
| `server_time_s` | int64, signed 64-bit | bar start plus configured server offset in seconds |
| `open`, `high`, `low`, `close`, `volume` | double | provider bar values |
| `period_s` | int32, unsigned 16-bit | `5` |

Embedded metadata is `closed = left`, `frequency = 5s`,
`interval = [timestamp,timestamp+5s)`, `label = left`, `offset_seconds = 0`,
`origin = unix_epoch_utc`, and `timestamp_semantics = bar_start`. Interval provenance is
`parquet_metadata` in the ready manifest; it is not an extra file-metadata key. Only complete
bars with start at or after the acquisition start and end at or before the cutoff enter the
new rows. Volume remains the provider's value; first/last tick times are not invented.

The bundle and schema-1 coverage details below describe legacy-v1 publication. Daily-v2
publication uses the typed coverage and daily occurrence contract in [Market data layout v2](#market-data-layout-v2-daily).

Matched history-page bytes are retained before row decoding. Individual pages stay locally
retained under `objects/SHA256HEX` for pending-intent replay;
publication concatenates the acquisition's pages in request order, without framing, into one
`Source` object at `raw/pages.bin`. Replayed and new pages are included, and local page objects
are never removed. A reused generation or acquisition without pages adds no bundle. A matched
response whose rows fail validation remains a retained diagnostic without a ready manifest naming
it. Envelope mismatches fail before this retention.

The JavaScript Object Notation (JSON) coverage record remains schema version 1. It records
`source_identity`, `broker`, `provider_symbol`, `role`, `requested` and optional `verified`
ranges (`start`, `end`), optional `actual` (`first`, `last`), `rows`, `pages`, and
`shortfall`. Each page has `path`, `sha256`, `bytes`, optional `anchor`, `rows`, and optional
`first`/`last`. Additions are `native_granularity` (absent means tick; tick is omitted when
written), optional `seed = { generation, source_identity }`, and each page's optional
`receipt_time` (the local receipt time of its request, which a resumed acquisition carries into
its operation receipt). Each shortfall has `reason` and
an `unresolved` range. Reasons include `empty_page`, `no_progress`, `budget`, and
`unresolved_tail`; when a primary shortfall and a tail coexist, `tail_shortfall` records the
tail separately. Optional `bundle = { sha256: string, bytes: u64 }` identifies the current
acquisition's `raw/pages.bin`; each bundled page has `offset: u64` and its bundle's `path`,
while `sha256` and `bytes` still identify that page's slice. In Rust these are
`HistoryCoverage.bundle: Option<ObjectIdentitySummary>` and `PageCoverage.offset: Option<u64>`;
absent values deserialize to `None` and are omitted when serialized. The cumulative index
carries previous acquisitions unchanged except that their `raw/pages.bin` paths become
`raw/BASELINE_GENERATION/pages.bin`, avoiding collisions even for identical bundle bytes.
Verification binds the current bundle summary to its source object, checks every current and
carried bundle's index tiles its bytes exactly with contiguous explicit offsets and lengths,
and verifies each slice's SHA-256 as well as each object's digest. Legacy individual-page
objects and entries without offsets remain valid and are carried without rebundling.
Missing optional additions are not synthesized into old immutable records.

For pipeline advances, the acquisition frontier is the latest retained tick time or the end of
the latest complete bar. A new acquisition starts at
`max(history.start, frontier - overlap_seconds)`, even when older seed coverage has gaps.
A pending acquisition instead keeps its pinned baseline, start, cutoff, and retained pages.
For legacy v1, the seed's original manifest is retained as `seed/ready.json`, with its raw/provenance objects
beneath `seed/`; lineage uses generation and source identity without a producer-location
dependency. Seeds must match instrument, role, native representation, and source identity.
A seeded advance or extending whole-window fetch is never narrowed: a later `history.start`
than its first retained row or a cutoff before its retained frontier fails before broker
connection. An explicit window on a daily baseline with a continuation, ending no later than
its retained frontier, is a supplement. It is exempt from seeded narrowing (`data pipeline update`
refuses a start before the job's configured `history.start`) and acquires from its explicit start
with overlap equality checked from that start. Its new claim records only what the broker verified inside the window;
it does not merge the continuation's verified range. Retained rows are not clipped.
An older inherited gap remains in the retained data and provenance; frontier advancement does
not establish continuous coverage or repair history outside the overlap.

For legacy v1, if a reread adds no rows and leaves verified coverage and shortfalls unchanged, fetch reuses the
prior dataset and its original manifest. New request receipts still record `anchor`, `sha256`,
`bytes`, `rows`, and local `receipt_time` in the pipeline operation receipt; identical raw
bytes do not require duplicate objects or a new dataset. Coverage page counts are retained page
entries, not a count of all requests.

### Commands

`binary-alpha data import --config PATH` enumerates only the declared inventory, rejects a source
inside the historical-data folder or a `file://` destination and either of those inside a listed
asset root or listed archive directory before it enumerates or writes anything, hashes every input, retains each input in the
historical-data folder while checking that the copied bytes still carry that identity, validates and
normalizes each dataset from its retained copy, creates each missing destination object with a
generation-match-zero precondition while verifying the returned size and checksum, reuses an
identical existing object, fails on different content at the same key without replacing anything,
publishes the ready manifest last, and mirrors it byte-for-byte into the historical-data folder.
Every run does that complete work; a ready manifest already at the destination must describe
exactly the same generation, objects, row count, and coverage as the fresh result, and its committed
bytes are the ones mirrored. Collection-level files are resolved through symbolic links and must
stay beneath the collection root; the collection manifest's bytes parsed for expectations must be
the bytes retained. It writes one line per dataset to standard output:
`published INSTRUMENT ROLE generation GENERATION rows N objects K reused R` followed either by
`[hash S retain S validate S publish S]` stage durations in seconds or by `(already published)`
when the ready manifest already existed.

`binary-alpha data split --config PATH` cuts one daily dataset generation per instrument and
whole-day window from the development roots in `split.sources`. Bounds are half-open UTC
midnights. Development windows may overlap, and identical windows coalesce; evaluation and
holdout windows are disjoint from each other and every development window. Each source names
a distinct instrument. Every window contains observations. All sources, windows, and locations
are checked before retention or publication starts. The retained folder, the destination, and the
destination's namespace location resolve through any alias to locations that neither lie inside
nor contain a source store and are never at or below a managed pipeline store.

Development and evaluation each require at least one window. `holdout = []` publishes no holdout
generation or population in the declaration; `research run` still requires a declared holdout
input for every instrument.

Each slice preserves the selected observation day objects, including duplicate occurrences and
empty inventory days, and carries reduced coverage evidence and split lineage naming the source
manifest and canonical window. Pages stay in the source root. Coverage uses the first and last
nonempty day. The command validates and reads back each retained slice before immutable
publication, then prints `published INSTRUMENT ROLE generation GENERATION rows N objects K reused R`.
After all slices, it publishes `NAMESPACE/declaration-IDENTITY.json` and prints `declaration URI`.
Each generation is one population, with stable `INSTRUMENT:YYYY-MM-DD` tokens for every day in
its window. The unsliced root is absent from the declaration. Reruns reuse completed generations
and identical declarations; an interrupted run retains completed evidence. Research inputs
outside this declaration require an explicit addition by the operator. Evaluation slices support
ordinary verification and frozen feature application; holdout stays protected, and audit accepts
development only.

`binary-alpha data verify --manifest URI` accepts a `file://` or `gs://` location ending in
`manifests/GENERATION/ready.json`, resolves object keys against the prefix before `manifests/`, reads
every object from that store alone, and asserts byte count, SHA-256, and any recorded CRC32C against
the bytes read; the store's own checksum and generation when the store reports them (only `gs://`
reads do; for `file://` reads a recorded Google generation is provenance); and, for data objects,
the reconstructed row count and coverage. A manifest is trusted only after its generation is
sixty-four hexadecimal digits that match its recorded inputs, its keys are content-addressed, and
its paths are unique and clean. It writes one line to standard output:
`verified INSTRUMENT ROLE generation GENERATION rows N objects K bytes B`. A manifest whose
top-level `kind` is `instrument_stream` is verified as a stream generation (see
[instrument streams](#instrument-streams)); a manifest with no `kind` is a dataset generation; any
other kind is rejected. With `--config PATH`, the configuration's `research.study` declaration
permits the target before it is opened and refuses an undeclared dataset (see
[Research](#research)).

`data import`, `data split`, `data audit`, and `data verify` exit with status 0 on success and, on any failure,
write nothing further to standard output, write one diagnostic to standard error, and exit with
status 1. None removes source files, retained objects, or published objects.

## Market data layout v2 (daily)

The following is the normative owner-adopted layout standard, revision 3. It applies to
datasets of every role (`tick_parquet_daily`, `bar_parquet`, `broker_history`) and development
instrument streams. The managed pipeline produces development roots; `data split` produces
bounded research generations, including evaluation and holdout. Derived research artifacts retain their own formats. The historical
dataset and instrument-stream layout documented elsewhere here is **legacy layout v1**, readable
until verified retirement. An absent manifest `layout` means v1; `layout = "daily-v2"` selects
this contract. Types, codecs, shared v1/v2 observation readers, v2 audit candle publication,
v2 verification, offline v1 migration, direct v2 imports and updates, the archive-root registry, daily archive/list/pull/restore,
and [retirement](retirement.md) are implemented and integrated using deterministic fixtures.
Operations on existing operator archives require separate authorization and are not exercised by these fixtures.

### 1. Families

Every data-bearing object is one Parquet file per UTC day `D` (`YYYY-MM-DD`) and per family. Dataset manifests own observations and pages; stream manifests own candles and the aggregate profile. Existing object roles are kept (`normalized` for observations and candles, `source` for pages).

| Family | Logical path | Rows | Day rule |
| --- | --- | --- | --- |
| Observations | `observations/D.parquet` | Deriv: normalized ticks (existing tick schema). Pocket: normalized 5 s bars keeping all eleven provider columns (symbol, symbol_id, timestamp_utc, unix_utc_s, server_time_s, open, high, low, close, volume, period_s) | Tick event time, or bar start time, in `[D, D+1)`; repeated ticks keep multiplicity and order |
| Pages | `pages/D.parquet` | One row per provider response occurrence (section 2) | Nonempty page: UTC day of its last event. Empty page: UTC day of `request_anchor_utc`. Otherwise: UTC day of its receipt time. A page with none of the three is never deleted and is reported as unresolved |
| Candles | `candles/<N>s_<O>s/D.parquet` | Session grid candles with explicit `fill` provenance | Candle open time; one continuous stream state across all days |

Single per-generation metadata (not market rows): the ready manifest with its ordered day inventory, `provenance/coverage.json` (acquisition coverage and unresolved ranges; no page index), `provenance/lineage.json` (section 5), and the stream `profile.json`.

#### Session-aware continuous candles

New `daily-v2` candle writing requires an explicit `instruments.session`: either
`{ kind = "always" }`, or `{ kind = "weekly", timezone = "America/New_York",
open = { day = "sunday", time = "17:00:00" }, close = { day = "friday", time = "17:00:00" },
closed_dates = [], early_closes = [] }`. Weekdays are lowercase English names; clock values
are `HH:MM:SS`. Dates and early-close clocks are local to that zone. An early close is
`{ date = "YYYY-MM-DD", time = "HH:MM:SS" }`; a closed date removes its entire local day. A
date written `MM-DD` applies every year (`02-29` in leap years only); a dated early close takes
precedence over a yearly one.
Only `UTC` (offset zero) and `America/New_York` are supported; other zones are refused
when configuration is parsed. New York uses UTC−5 standard time and UTC−4 daylight time,
from the second Sunday in March at 02:00 local to the first Sunday in November at 02:00 local.
The explicit US rule applies from 2007 onward; earlier New York dates are refused precisely,
including dated overrides at parse time. Calendar arithmetic reuses the repository's pure
Gregorian functions without a timezone dependency or host zoneinfo. Spring local boundaries
in `[02:00,03:00)` are skipped; autumn ones in `[01:00,02:00)` are ambiguous. Both are refused,
never shifted or assigned a fold. Recurring boundaries are checked on their occurrence date. The old plural `sessions` field remains
profile-only windows. Missing singular `session` is never silently defaulted during v2 writing.
The canonical instrument definition binds this calendar, including exceptions, into identity.

`candles/<N>s_<O>s/D.parquet` contains exactly one row per eligible epoch-grid bucket,
strictly increasing across UTC partitions. Membership depends on the bucket's open instant:
`interval_open <= bucket_open <= interval_close`, including both regular weekly and dated
early closes. The bucket can extend beyond close; its duration and epoch offset are never
shortened or shifted. A bucket opening before the session open remains excluded even if it
straddles that open. Closed local dates remove their entire date, while an explicit weekly
or early close at local midnight includes that instant on a non-closed date.
For Deriv FX, `[20:55:00,20:55:05)` at Friday close and `[22:00:00,22:00:05)` at an early
close belong to the session. A 60-second bucket opening at either close also belongs, as does
an offset bucket opening before close and ending after it. Pocket non-OTC's Friday 17:00:00
America/New_York bucket belongs; a broker-delivered flat zero-volume bar there is `source`.
With no observation there, an `engine` fill may be emitted under the coverage and pending
rules below. Closing quotes therefore remain in finalized candles. Observations outside
eligible buckets remain losslessly in observations.
No candle is emitted before the first in-session, non-source-fill finalized candle. After that
first price is established, later sessions begin filling at their scheduled open from the prior
session's last close. Carrying that reference price creates no rows during closed time.

Daily candles append required UTF8 `fill`: `none` for market candles, `source` for a delivered
flat-OHLC, zero-volume candle, `engine` for a created zero-observation candle. Zero volume alone
is insufficient: historical Pocket bars can change price while reporting zero volume. Source
fills retain their observation counts, prices, volume, event clocks and feed gap diagnostics;
`frozen` is additionally true and they are never `clean`. Engine fills have flat OHLC equal to
the preceding close, observations/duplicates zero, volume zero only for volume-bearing sources
(null for ticks), zero activity and jump counts, and no invented feed gap measurements:
`gap_before_micros = null`, `max_gap_inside_micros = 0`, `missing_buckets_before = 0`.
They have `low_activity`, `hard_low_activity`, `frozen`, and `short_span` true, making both
`complete` and `clean` false. Their first/last event clocks identify the carried price's last
source event, possibly in a prior session, and never assert an event inside the filled bucket;
`active_span_micros = 0`. For a bucket fully covered by verified acquisition ranges, synthetic
`known_at` is its logical close watermark, stable when a descendant adds later input. It is
not an invented receipt/event time. In an unresolved range it is the later finalized candle
bounding the interior absence or the final verified extent bounding the snapshot tail, never
before bucket close; such days remain partial/unknown.
The first real candle after engine fills retains its feed diagnostics. Across source fills,
`InstrumentStream` additionally carries the sum of their actually missing buckets to the next
real candle and the maximum observed preceding/interior feed gap into its `gap_before_micros`.
Source rows retain their own diagnostics; the carry clears once consumed by a real candle.
Contiguous source fills add no missing buckets and no invented elapsed gap. Gap flags are
evaluated against these preserved measurements, so inserting rows cannot repair missing feed.

Interior empty buckets are proven absent by later finalized input. Trailing fills require the
exclusive end of authenticated verified acquisition coverage, stop at the last whole bucket
within it, and never replace the stream's unfinished candle. A partial/unknown source day
remains partial/unknown; filling never establishes acquisition completeness. The raw profile
continues to count feed observations and finalized feed candles; its candle count can differ
from the session product count. The existing pending-candle day rule remains in force.
Verification reconstructs the session product from authenticated observations and coverage and
compares every candle and the full reconstructed feed profile, including pending counts,
leading/trailing limits, prices, quality and all timestamps. Closed feed candles still advance
feature ordinals, breaking adjacency across rejected market time.
Legacy immutable daily files without `fill` remain readable. Features exclude session-closed
feed candles and source fills; fills cannot become eligible feature or outcome evidence.


#### Day inventory

Each entry: `date`, `family` (and candle `duration`/`offset`), `object` key or null, `rows`, first/last time or null, and `state`:

- `complete`: evidence covers the whole day and no later input can change this family's output for the day;
- `partial`: some of the day is covered (reason and unresolved intervals recorded; includes the cutoff day and any candle day whose last candle may still be finalized by a later observation);
- `unknown`: source evidence exists without a completeness claim (reason recorded, for example a Deriv historical gap with `complete: false`);
- `empty_known`: evidence covers the whole day with zero rows (for example Deriv `market_closed: true` without clipping); `object` is null.

A descendant inventories every UTC date between its first and last observation day; a date
without rows gets an empty partition and takes its state from acquisition evidence.

A first-time migration of native bars (period `N` seconds tiling the day) also derives
`complete` for an observation day from a validated complete grid: exactly `86,400 / N` rows,
the first bar start at the UTC day start, and the last bar start `N` seconds before the day
end, whatever the legacy source kind. Every admitted bar lies on the period grid and follows
the previous one strictly, so that count between those endpoints occupies every slot; the
cutoff-day rule still applies afterwards. The observation `basis` of such a root names the
grid. A superseding derivation must reproduce the predecessor's `migration-source` claim,
which shares its identity, so it admits the grid exactly when the predecessor's basis names
it and otherwise keeps the source-kind rule (`bar_parquet` only); verified checkpoints,
existing roots, and completed retirement evidence are never relabelled. Ticks are never
inferred from counts.

#### Typed v2 acquisition coverage

For `daily-v2`, `provenance/coverage.json` is the engine-owned
`dataset::coverage::DailyCoverage` contract with `schema_version = 2`. Its required fields are
`broker`, `provider_symbol`, `role`, `native_granularity`, `acquisitions`, and `days`.
It rejects unknown fields and contains no page index. Each acquisition preserves
`acquisition_id`, `source_identity`, `requested`, `verified`, `shortfalls` (each a `reason`
and `unresolved` range), and `unresolved`. Ranges are ordered, nonoverlapping half-open
`{start, end}` UTC intervals, measured in observation event time (bar starts). Empty request
lists preserve imports that made no request claim. Cumulative verified ranges may extend
outside the current request. Verified and unresolved acquisition spans cannot overlap;
every requested span is accounted for, and every shortfall names unresolved coverage.
Migration derives one `v1-history:<generation>` claim per retained v1 history record. A
shortfall range that an older executable recorded inside already verified coverage (before
fetch clipped shortfalls to the unverified part of a request) contributes only its
intersection with the request's unverified part to the claim, one shortfall per remaining
piece (a current fetch trims only the endpoints); the migration `provenance/lineage.json`
lists each such range under `legacy_shortfalls` (`generation`, `acquisition_id`, `field`,
`reason`, `recorded`, `retained`, `basis`) so the retired record stays reconstructible.

Daily updates may add one `native-bar-grid:<fetch acquisition id>` claim for observation days
whose validated native bars occupy every slot of the UTC day and whose inherited day evidence
plus fetched coverage does not already prove the whole day. Its `requested` and `verified`
are those whole days merged into ranges, its `source_identity` is the fetch claim's, and its
`shortfalls` and `unresolved` are empty. Selected days reference it after the fetch claim;
existing acquisitions and the fetch continuation remain unchanged. Tick days never qualify.
A later update omits the claim when no unproved full-grid day remains. A resumed acquisition
uses its original baseline's day evidence, so its grid claim may overlap a sibling snapshot's
claim. Relabelling an unchanged full grid preserves finalized candle rows, including delivered
`source` fills.

Each `(family, date)` has exactly one `DayCoverage` record: `acquisition_ids`, a nonempty
`basis` identifying the retained evidence, `verified` spans, `unresolved` spans, and nullable
`reason`. Only observations and pages belong here. Verified day spans lie within that UTC
day; unresolved spans must be their exact complement. No verified span derives `unknown`,
a proper verified subset derives `partial`, and a fully verified day derives `complete`
for nonzero rows or `empty_known` for zero rows. Incomplete days require a reason; complete
days have none. Observation verification must be supported by the referenced acquisitions'
verified ranges. Page evidence independently establishes retention of response occurrences;
market coverage alone never proves page completeness. Producers must leave page days unknown
when that evidence is unavailable. Rows can exist within unresolved spans: unresolved means
incomplete acquisition, not proven absence. Unknown days record the whole day as unresolved.

Verification authenticates and decodes this object, binds its instrument, role and native
granularity to the dataset, and checks the exact day set, every state, reason and unresolved
interval against the inventory, including zero-row days. The evidence is a retained source
claim or, for migrated or updated native bars, a validated complete grid; it is not a new inference
from sparse observations or authority to read external data.

V2 stream manifests also record `source_manifest_uri`. Verification first resolves their
`source_generation` in the stream store (supporting fresh-store restores), then uses that
explicit original reference if the local source is absent. Existing access checks precede
source reads. The source dataset must verify and match the stream's identity and summary;
unavailable evidence fails verification. Audit and verification share candle-day derivation
for every source day, finalized candle-open day and pending day. Coverage through a finalizing
bar concerns its event time (`known_at` minus native period), while retaining the candle-close
bound. Old manifests without a source URI remain verifiable with their source in the same store.

#### Deterministic encoding profile `daily-parquet-v1`

Rows sorted canonically (observations and candles by time then original order; pages by `acquisition_id` then `ordinal`); fixed schema and semantic metadata per family; no generation, cutoff, or run metadata inside daily files; `parquet` crate 59.3.0 as pinned in `Cargo.lock`; Zstandard level 3; dictionary encoding off; statistics setting fixed; one row group per file; fixed numeric data page row and byte limits; every column written in canonical segments of exactly 8,192 values (the last shorter) regardless of how rows arrive upstream; writer `created_by` fixed to the profile name. Equal rows, schema, semantic metadata, and profile produce equal bytes; hash-equality tests vary upstream batch boundaries (777, 1,024, and 65,536 rows) and repeat writes. An unchanged day is referenced by key and never regenerated. A later profile version is a new layout input.

### 2. Page occurrences

Columns: `acquisition_id` (import: SHA-256 of the replaced `raw_pages.ndjson`; broker history: the name of an immutable acquisition record created before the invocation's first request, binding the intent and a unique invocation token; every page progress entry and operation receipt records this identity. Replayed pages keep their original identity and ordinal, while new requests on resumption use the new invocation's acquisition record; legacy requests only held in a pending progress log use the intent name plus the SHA-256 of that log's header); `intent` (nullable, the intent record name, kept separately); `ordinal` (0-based position in that acquisition's raw source order: NDJSON line number, or request order within the receipt or log); `checkpoint_ordinal` (nullable, the checkpoint line's own 0-based position, which can differ from `ordinal`); `order_kind` (`request_order` or `source_file_order`); `payload_sha256`; `payload` (exact response bytes; for NDJSON lines the bytes without the trailing newline, which is what the checkpoint `payload_sha256` covers); `request_token` (opaque provider anchor as sent, nullable); `request_anchor_utc` (nullable; the historical request boundary, not dispatch time: Pocket `UTC = provider_seconds − recorded_offset_minutes × 60`, Deriv `UTC = epoch_seconds`, both converted with checked arithmetic to microseconds); `receipt_time_utc` (nullable); `receipt_state` (`recorded`, `not_recorded_by_source` for recovered checkpoint lines, `absent_in_legacy_record`); `first_event_time`, `last_event_time` (nullable for empty pages); `rows`; `checkpoint` (exact original checkpoint line bytes, nullable); `disposition` (`indexed`, `diagnostic` for retained responses that failed validation or belong to abandoned acquisitions).

Rules: one row per response occurrence, never deduplicated by payload hash; the inventory of occurrences covers every completed operation receipt, every published coverage index, every pending progress log, and every import NDJSON/checkpoint pair. Storage aliases of one occurrence (a single-page object, its bundle slice, a replayed checkpoint of the same request) map to that one occurrence through an explicit alias table in the migration record; requests with different recorded receipt times are different occurrences even when anchor and payload are equal. Checkpoint lines are matched to raw lines by payload hash with occurrence counting in source order, never by provider `index` or position alone, and the raw and checkpoint files are each reconstructed from their own ordinal and framing. Import lineage records the NDJSON framing (line terminator, final newline) and both source-file hashes so the original `raw_pages.ndjson` and `checkpoint.ndjson` can be reconstructed byte for byte from the pages of that acquisition.

### 3. Identity and continuation

- Dataset and stream identities gain the input `layout daily-v2`; v1 identities and every completed record are never rewritten.
- Each instrument gets exactly one v2 continuation root: a v2 dataset carrying all history (import plus every acquisition to date) and its stream. `data pipeline update` seeds and selects priors from the v2 continuation root instead of the v1 import. A migration record per instrument binds v1 generations (import, newest history, stream) to the v2 root with the equality results.
- A descendant references unchanged daily objects by key; it writes new objects only for days whose output changes, per family (new observation days, the previous partial day, and any earlier candle day completed by a newly finalized candle).
- The five pending v1 acquisitions (8,285 retained pages, 36,198,285 bytes) are migrated first: their retained payloads and progress records become v2 page rows with disposition `diagnostic`, the migration record keeps their intents and the abandonment provenance, and only then are they abandoned by the documented rule. Refetched responses are additional occurrences. The IRRUSD_otc intent record is kept as is (it has no progress pair).
- The continuation mapping (v1 import, newest history and stream → v2 root and stream) is persisted as an immutable record and archived with the v2 catalog; it preserves source identity, role, requested and verified coverage, shortfalls, and unresolved ranges. `imported_seed`, `prior`, and `pull` select within the v2 lineage (v2 roots and their descendants), prefer it over v1 while both exist, and restore→update works after v1 is removed. The two existing operator fetch configurations that pin v1 seeds (`~/.config/binary-alpha/pipeline/deriv-fetch.toml`, `pocket-fetch.toml`) are rebound to v2 roots or retired before their targets are deleted. The mapping grants no new read authority; existing permit checks still run before object reads.

The daily coverage object is the only authority for requested and verified acquisition ranges,
shortfalls, unresolved intervals, and day-state evidence. Descendant lineage records contain
`continuation = {acquisition_id, intent, seed}` to select the typed acquisition used for the next fetch.
A supplement keeps the continuation's `acquisition_id` and `intent` and records
`supplement = {acquisition_id, intent}`; an ordinary advance replaces them and drops the
supplement object. Every descendant writes `seed` from its fetch, which normalizes a
broker-history seed. Inherited acquisition claims remain unchanged. Lineage records
do not duplicate a legacy coverage object or page index. They preserve `root_generation`,
`parent_generation`, and the historical `ancestors` identities. Resumed snapshots name prior
snapshots of the same intent, matched through either `continuation.intent` or `supplement.intent`,
as ancestors only after proving their response occurrences remain in the replacement closure.
A supplement's pending binding adds a discriminator to the effective configuration's, so a
pending window and a pending advance never resume each other. Observation day evidence combines
retained verified spans with new acquisition evidence; unresolved ranges are their exact UTC-day
complement. Clipping without a proved covered boundary remains `unknown`. Page completeness
requires occurrence evidence independently of market coverage.

### 4. Archive, transfers, and retirement

- Transfers move to one archive-root registry: content key → `{file_id, session, done, bytes, sha256}`, with atomic reservation across concurrent jobs, persisted incrementally (append-only log plus periodic compact snapshot, never a full rewrite per transfer). Completed entries can be rebuilt from a complete paginated Drive listing (no incomplete results; restart on rejected tokens) and are reused only after size and SHA-256 confirmation (readback when Drive reports no checksum).
- Before retirement, every completed record (intents, operation receipts, catalog receipts) and every configuration reference to a generation, stream, or catalog is inventoried, and each referenced closure is recorded as `protected` or `retired` with the reason; immutable records are never rewritten, retained catalogs keep their exact file-id bindings, and an unresolved dependency is never a deletion candidate.
- Retirement is by reachability, never by age or file name. Retained roots: the newest v2 catalog per job (the catalog file itself, its manifests, and its objects), v2 continuation roots and their streams, pending acquisitions and their retained pages, and in-flight transfers. Under the writer lock an exact deletion inventory (local keys and Drive file ids) is computed, recorded, applied in resumable batches, and each retained closure is re-verified afterwards. Superseded v2 catalogs and replaced partial-day objects are retired by the same rule on later updates.

### 5. Lineage

`provenance/lineage.json` per v2 root: for each replaced v1 object its logical path, key, SHA-256, bytes, and row count; Deriv per-day `.meta.json` contents; Pocket per-instrument `download_manifest.json`, `dataset/manifest.json`, `dataset/hashes.sha256`, `dataset/reports/quality.json` embedded verbatim and the instrument's entry of the shared collection manifest; NDJSON framing; the v1 generation identities it replaces. Markers (`_SUCCESS`, `.conversion.lock`) and the empty shared `gaps.parquet` are recorded by hash only.

### 6. Verification and acceptance

`data verify` on a v2 dataset decodes every daily file, checks day membership, cross-day order and multiplicity, the day inventory against the files, each page's `payload_sha256`, and aggregate rows and coverage; on a v2 stream it verifies each candle day and the aggregate summary. Migration additionally proves, per instrument, before any deletion: identical observation rows in order to the v1 newest history (or import); every NDJSON line, checkpoint line, bundle slice, and single page accounted for exactly once and both import source files reconstructed byte for byte; exact legacy candle evidence reconstruction
under the v1 definition, plus independent verification of the new session product.

A migration may add a missing singular `session` while preserving every other definition field;
an existing calendar cannot change. `candles_equal=true` and `MigrationEquality.candles` mean
that v2 observations reproduce the selected baseline stream's candle columns, summary and
profile under its exact legacy definition (with only the source-generation substitution).
They do not assert equality
of the filled session product with sparse v1 candles. The durable proof names
`basis=legacy_definition_reconstruction`, preserves both definitions and stream summaries,
records the legacy row digest and profile equality, and requires `session_product_verified=true`.
The new product passes `data verify` independently before the migration record is published;
a contradictory session proof fails the retirement eligibility predicate.

Proof version 4 additionally seals `source_preservation` for every mapped v1 dataset and
stream against the final continuation root. Dataset proofs compare exact ordered lossless
rows (including multiplicity and every provider column) over the source's inclusive time
interval. Stream proofs reconstruct that interval under each recorded definition and compare
all candles, summaries, and profile. Only successful per-source proofs grant legacy retirement
authority. Unproved closures remain retained and archived in the catalog's lineage manifests;
restoration verifies those originals as well. The aggregate equality summary describes the
selected baseline only. Earlier proof versions are re-proved in superseding immutable records.
A pre-session configuration may add validated singular session tables only when removing them
reproduces the completed checkpoint's exact binding; the new record names both configuration
hashes, both bindings, the calendars, and its preserved predecessor. Old daily products are
reconstructed separately from verification of the new session product.

Named non-live gates (`cargo test -p binary-alpha-app --test data_pipeline` and the affected suites): one multi-day Deriv and one multi-day Pocket fixture through import, migration, history update, audit, feature/outcome/replay readers, archive, fresh-store restore, retirement, and a later update that uploads zero unchanged objects; covering midnight repeats, a cross-midnight page, an empty page, missing receipt metadata, a historical gap, a partial cutoff day, a weekend-delayed candle finalization, pending-acquisition diagnostics, repeated requests under one intent with unchanged observations (which changes only a page day), v1/v2 coexistence with equal coverage, encoding determinism across batch boundaries, interruption at each phase, and archive-registry rebuild. Assertions are on goal-bearing outputs, traversing every daily partition: identical feature rows and engine state, identical global outcome indices and reasons, identical replay ledger and results, identical recorded warm-up state, exact legacy candle/profile reconstruction and independently
verified session candles, exact page reconstruction, and Pocket outcome and replay parity under the [outcome observation binding](#outcomes); live warm-up still requires ticks.

### Foundation representation and encoding choices

`day_inventory` entries use `first_time` and `last_time` as inclusive bounds of the row's
partition timestamp: event time, bar start, candle open, or the page's day-assignment time.
They are not the page's first event (which may lie on an earlier day), nor candle close.
Times are UTC strings in the existing manifest timestamp format. `unresolved` intervals have
`start` (inclusive) and `end` (exclusive), within the entry's day, sorted and nonoverlapping.
`partial` requires a nonempty reason and intervals; `unknown` requires a nonempty reason.
Zero rows require null time bounds. Only `empty_known` has a null object. Date ordering is
strict within `(family, duration, offset)`; candle duration and offset are seconds. Dataset
row counts equal observation inventory totals; stream summary counts equal candle inventory
totals. Dataset coverage metadata is required; lineage is allowed on descendants and required
by the migration/root workflow, which owns the root distinction. Streams own only candle
objects and the normalized aggregate profile.

Daily bar codecs preserve nulls in all eleven optional provider columns. Execution still uses
the validated legacy `Bar<()>` values; it does not invent values for missing fields. A daily
bar needs an assignable UTC start, using `unix_utc_s` or `timestamp_utc` and checking agreement
when both are present. Both daily candle codec directions enforce the archive non-overlap rule.

Every observation consumer traverses the same authenticated partitions in inventory order,
retaining repeated ticks and continuous stream/feature state. Audit partitions finalized candles
by open day. An unfinished candle keeps its open day partial even when the last observation
falls on a later day. Existing unresolved gaps and reasons survive union with that pending tail.
A complete candle day also requires evidence for later-day inputs affecting candle values or
finalization time; uncertain coverage through candle close or recorded `known_at` prevents a
complete claim. V1 datasets continue to produce v1 streams.

For `daily-parquet-v1`, data page row and byte limits are respectively **8,192** and
**1,048,576**, write batch size is **8,192**, writer version is `PARQUET_1_0`, value encoding
is `PLAIN`, and statistics are **disabled**. Canonical segments count logical column positions,
including nulls; each optional segment supplies its matching definition levels and present
values. Page limits are Parquet's thresholds checked at a write batch boundary, not a hard
maximum on a single binary value. These fixed settings bound page construction without
payload min/max statistics or dependence on caller batch sizes. Readers validate schema,
semantic metadata, profile, ordering and day membership. Equal-time rows retain input order;
no codec sorts or deduplicates them. Checkpoint bytes and their independent ordinal must
be present together. Codecs buffer at most the single daily input passed to a call; bounded
whole-history conversion belongs to the later migration workflow. Unassignable pages return an unresolved error to the
caller, which must retain the source; codecs perform no source deletion.

## Data pipeline

The application-owned research pipeline imports selected originals, extends Deriv ticks or Pocket
Option five-second bars, and archives a dataset plus its matching instrument stream in private
Google Drive. Local filesystem publication is supported for this workflow and for all research,
including holdout grants and certification; non-research run modes publish to Google Cloud Storage.
Catalogs cover only dataset and stream ready manifests and their object dependencies, including raw
data, provenance, normalized data, profiles, and candles. They do not archive feature/model,
outcome, replay, research, or certification generations, and introduce no Google Drive
artifact-store address scheme.

### Pipeline document

The separate Tom's Obvious, Minimal Language (TOML) document has `schema_version = 1`, a required
nonempty `local_root`, required `drive`, optional `governance_manifest`, optional positive
`parallel_jobs` (default 1: how many jobs one producer run works on at a time, each with its own
broker connection and Drive session; per-job report lines stay contiguous and the failure summary
keeps document order), optional positive `parallel_transfers` (default 8: concurrent object uploads
or downloads per job, each worker with its own Drive session), and `jobs` (default empty).
Unknown fields are rejected in the document, Drive settings, and jobs. Relative
`local_root` resolves against this document's directory. Each job requires:

- `id`: unique, nonempty ASCII (American Standard Code for Information Interchange) letters,
  digits, underscores, or hyphens.
- `config`: the core configuration path relative to the pipeline document.
- `evidence`: a readable non-secret source-binding evidence file relative to the pipeline
  document. It is a JSON document whose `source_identity` names the broker source identity
  (see [broker access](#broker-access)) the archive was collected under; other keys are
  free-form notes. Binding refuses a job whose configured broker has a different identity, and
  the file's bytes are hashed into the intent.

Both job paths must be relative, without empty, `.`, or `..` segments. Each core
configuration must use `run_mode = "research"` and declare exactly one history instrument with
an ordinary role; any `import` table it carries is ignored by the pipeline. The core `research`
table is rejected; supply an existing declaration through the pipeline's `governance_manifest`
instead. The pipeline requires tick history for Deriv and five-second bar history for Pocket
Option, positive `overlap_seconds`, `max_pages`, and `max_elapsed_seconds`, and no
`refresh_interval_seconds`. It replaces storage paths with `local_root/store` in an effective
configuration, and supplies the seed and update cutoff without editing the operator's core file.
Consumer-only configurations may omit jobs; update requires them. List/restore need no broker
credentials.

The seed is the readable v2 continuation root of the job's instrument. Existing v1 inputs must
be migrated first. An empty store instead acquires directly from the configured broker within
`history.start` and the pinned cutoff, using the existing unseeded daily fetch owner. No import
or v1 intermediate is needed. A pending intent preserves its original seed list, even when
an interrupted bootstrap has published partial v2 data.

`add-job --config PIPELINE --template CORE --broker B --symbol S [--quote-currency C]
[--price-scale N] [--session FILE]` reuses the template's broker settings, history policy and
instrument candle policy. It requires development/research mode and an explicit singular
`instruments.session` table. It discovers the requested symbol, uses reported precision where
available, and otherwise validates a bounded history sample through the adapter at exact
supported scales. Empty/invalid samples require an explicit scale. Six-uppercase-letter pair
symbols (optional `frx` prefix / `_otc` suffix) supply the quote currency; ambiguous symbols
require the option. A scale failure includes the required digits, never rounds the price.

Registration checkpoints its exact core/evidence bytes before create-once file publication and
appends the pipeline entry last under the writer lock; retry uses that checkpoint and refuses
different input or output bytes. Generated paths are relative `jobs/JOB.toml` and
`evidence/JOB.json`. Existing source-bound imported jobs remain readable; empty-store bootstrap
also requires the explicit calendar. The engine's `Config::parse` and `Session::calendar()`
validate the singular session declaration for registration and acquisition. Canonical core TOML
includes the entire calendar, so the effective configuration hash binds it without a separate
session digest. Plural `sessions` remains a separate profile field.

The `drive` table requires `root_folder_id` (nonempty, with no ASCII control characters, slash,
or single quote), `chunk_bytes` (a positive unsigned 64-bit multiple of `262144`),
`request_timeout_seconds`, and `max_attempts` (positive unsigned 32-bit integers). Optional
`retry_seconds` is a positive unsigned 32-bit per-request transient retry budget, defaulting to
900 wall-clock seconds; the other transfer limits have no defaults. Operator mode also requires
`credential`, an environment-variable name
containing only letters, digits, and underscores, with no leading digit. The process variable
holds user OAuth (Open Authorization) refresh credentials as a JSON (JavaScript Object Notation)
object with string `client_id`, `client_secret`, and `refresh_token` fields. Operator transport
uses Google's fixed secure web endpoints. Optional `loopback_endpoint` is a test fixture only:
a literal `http://127.0.0.1:PORT` or `http://[::1]:PORT` base without whitespace or trailing
slash, with `credential` absent and synthetic credentials supplied internally. It never resolves
operator credentials. See [the example](../configs/data-pipeline.example.toml).

### Local state and commands

The managed root separates `store/` (retained and published objects, also the importer's
retained folder and publication root) and `pipeline_state/`. State and per-job state directories
have mode `0700`. Update holds the nonblocking `pipeline_state/writer.lock` for the producer run;
pull/restore also hold that lock and the host-local archive-root lock. Use one writer host per archive root.

Under `pipeline_state/`:

- `records/JOB-intent-HASH32.json` and `records/JOB-receipt-HASH32.json` are immutable,
  content-named records; `HASH32` is the first 32 hexadecimal digits of the record's SHA-256
  (Secure Hash Algorithm, 256-bit). Intents retain command/job, configuration hashes, evidence
  digest, and update cutoff/seed binding. Receipts retain status, generations, coverage, request
  receipts, catalog receipt, and pending state.
- `records/JOB-catalog-DATASET16-STREAM16.json` records the catalog's `file_id`, `sha256`,
  and `bytes`; the generation prefixes are 16 hexadecimal digits.
- `JOB/update.toml` holds the effective core configuration of the last update.
- `JOB/progress.json` holds a pending intent name, effective configuration binding, and
  `progress` with `baseline`, `start`, and `cutoff`, written once.
  `JOB/progress.pages.jsonl` appends one compact JSON page record and newline per retained page,
  using one write followed by a flush; replayed pages are not appended again. Old inline
  `pages` are read first, then complete log lines. A missing log means zero appended pages;
  a trailing partial line is ignored, reported, and truncated before appending. Pages are retained
  before this checkpoint advances; resumption decodes them and continues backward. A page whose
  rows contradict the retained rows fails the run before it is checkpointed, so a resumed
  intent never replays a conflicting page; completion removes both progress files. Resume with
  update at the same cutoff; never remove progress files manually to abandon or bypass a binding.
- `registry/snapshot.json` (version `1`) binds `archive_root` and the fixture `endpoint`, and
  stores a `sequence` watermark, `files`, pending `legacy` aliases, imported job names, and
  the completed-rebuild flag. Every `files` key is `objects/SHA256HEX`, including the byte
  identities of ready manifests and catalogs; values are `{file_id, session, done, bytes, sha256, aliases}`. Logical `job/key` aliases
  let retirement pin the dataset/stream closure of an unfinished manifest or catalog transfer.
  Older entries without aliases conservatively retain all local and archived closures.
- `registry/events.ndjson` appends synced `{sequence, change}` records. Changes are `put`
  (key and entry), `remove` (a stale completed key on rebuild), `legacy` (job/key alias and
  old entry), `imported` (job), or `rebuilt`.
  A flushed, atomically renamed snapshot compacts every 256 events before truncating the log;
  replay skips its watermark and discards only an unfinished trailing line. Reservations and
  sessions are synced before upload bytes, and a shared per-content lease covers completion.
  A journal or snapshot write failure stops further transfers until the registry is reopened;
  rebuild excludes concurrent transfers. Old `JOB/transfers.json` files are imported once
  without rewriting them; new per-job transfer files are never written: completed entries require remote identity confirmation, and unfinished
  ids/sessions resume on their first use. Started legacy catalogs retain the original per-job
  object and manifest bindings so their resumed bytes do not change during deduplication.
  Session capabilities stay inside the private pipeline-state directory.
- `downloads/` holds temporary `FILE_ID.catalog`, `GENERATION.manifest`, and
  `SHA256HEX.partial` downloads.
- `registrations/JOB.json` seals the requested template/session/options digest and exact
  generated core/evidence bytes before add-job publishes files and appends its pipeline entry.

A pending intent binds the archive root and the evidence digest it was opened under as well;
a resumed invocation whose `drive.root_folder_id` or evidence file differs fails with the pending
intent identity.

Mutable checkpoints use flushed atomic replacement. Completed records and store objects use
create-once publication; different content at an existing key is a conflict.

```text
binary-alpha data import --config CORE
binary-alpha data pipeline migrate --config PIPELINE [--job ID]
binary-alpha data pipeline update --config PIPELINE [--end END]
binary-alpha data pipeline archive --config PIPELINE [--job ID]
binary-alpha data pipeline add-job --config PIPELINE --template CORE --broker B --symbol S [--quote-currency C] [--price-scale N] [--session FILE]
binary-alpha data pipeline retire --config PIPELINE [--job ID] [--whole-job] [--plan | --apply PLAN_FILE]
binary-alpha data pipeline remove-job --config PIPELINE --job ID
binary-alpha data pipeline list --config PIPELINE --broker BROKER --symbol SYMBOL
binary-alpha data pipeline pull --config PIPELINE --broker BROKER --symbol SYMBOL
binary-alpha data pipeline restore --config PIPELINE --catalog FILE_ID --sha256 SHA256 --broker BROKER --symbol SYMBOL
binary-alpha data pipeline restore --config PIPELINE --all
```

Import is the existing command (see [Historical datasets](#historical-datasets)) run with
`storage.historical_data_dir` and a `file://` `storage.publication_uri` both naming
`local_root/store`; it may import every instrument of an archive in one run.

Archive selects the newest local `daily-v2` dataset for each selected job by coverage end,
then lineage (ambiguous daily ancestry is refused), verifies its existing
matching stream, and publishes the closure without
broker acquisition. It shares update's archive owner and archive-root registry. Registry loss
rebuilds completed entries from a full root listing; `incompleteSearch` is refused, rejected
page tokens restart pagination, and every candidate requires size/SHA-256 confirmation (byte
readback without a reported checksum). Unfinished reservations survive explicit rebuilds.
Existing immutable catalog receipts retain their exact file-id bindings.
At equal coverage, a generation named in another candidate's hash-confirmed
`provenance/lineage.json` is a predecessor. Generation references are bare identifiers in JSON
values; selection does not impose migration/acquisition field names or read ancestor closures.
The lineage owner supplies the same daily selection policy to update, archive, pull, and retirement.
The lineage object is archived and restored with the other manifest-owned objects. Descendant
catalogs also carry their continuation root and its stream in `lineage_manifests`, with all objects
in the catalog's shared inventory. Restore authenticates, installs, and verifies every such manifest. Migration roots and their descendants also carry a `records` inventory: hash/size/file-id bindings for
the shared verified migration receipt, its alias JSONL, and immutable source intents/receipts named
by root lineage. These bytes are restored unchanged under `pipeline_state/records`; record keys
must be one safe filename beneath `records/`. Registry rebuild and archive reuse include these
records. Pull repairs a missing record even when all manifests are already local.

Migration uses `DailyCoverage`, the same typed coverage owner as import, update, and verify.
Retained requested/verified ranges and shortfalls stay in acquisition records; day states and
unresolved intervals are validated against that contract. A cutoff with no independently verified
interval remains `unknown`. Both legacy and current import checkpoint field names use the same
page occurrence decoder, including checked provider offsets and recorded receipt timestamps.
A legacy daily catalog containing only a descendant may use that self-contained descendant as a
stable readable seed; subsequent updates preserve the immutable logical root identity.

All producer, pull/restore, and retirement commands acquire the managed-store writer lock and
then a host-local archive-root lock. Pull holds both continuously from selection through restore.
Retirement uses the registry owner's read-only replay, including snapshot watermarks, removals,
logical aliases, and legacy transfer files. Completed reclamation records do not pin removed
single-page representations. A missing completed remote binding is reusable only after an exact
completed retirement record authorizes its removal; unrelated remote loss remains an error.
Deferred single-page cleanup can use a verified descendant after its original publication is
retired, but only when it retains every exact receipt occurrence. Missing replacement proof
defers cleanup. Daily import staging has a durable ownership journal so interrupted imports can
reclaim their own unreferenced source copies on retry without adopting pre-existing objects.
Retiring v1 manifests requires a matching immutable, verified migration record as specified in
[retirement](retirement.md); native v2 ancestry retirement does not require migration evidence.
Retirement plans use schema 2, refuse older apply plans, and preserve completed evidence when
seal scratch files survive a crash. Every Drive delete attempt rechecks name and content identity.

Update follows the v2 root or bootstraps an empty job; `END` is `YYYY-MM-DDTHH:MM:SS[.ffffff]Z`. Each job
acquires a bounded extension, audits and verifies its result, and archives it independently.
A pending acquisition keeps its original cutoff, baseline, start, and pages on rerun, even after
a partial snapshot was archived. A conflicting `--end` or effective configuration fails with the
pending intent identity. Page/time budgets are excluded from that configuration binding and may
change for a resumed invocation. For a job with no pending acquisition, `--end` supplies the
cutoff or, if omitted, the job samples the clock. Reaching the acquisition start or a terminal
provider shortfall closes acquisition; it does not certify complete historical coverage.

Producer output includes the existing import/fetch/audit lines and these pipeline lines:

```text
pipeline update JOB INSTRUMENT cutoff END status STATUS requested START END verified START END shortfall REASON dataset G stream S catalog FILE_ID sha256 HASH
pipeline job JOB failed: REASON
```

Missing verified bounds are `none none`; missing shortfall, generations, or catalog fields are
`none`. Update status is `pending` while acquisition remains open, `no_data` when closed
without a catalog, `archived_with_gaps` when a catalog exists with a primary shortfall other than
`unresolved_tail`, and `archived` otherwise. A tail alone can therefore be `archived`;
read coverage and provenance to assess gaps. A pending or archived-with-gaps update line appears
inside `pipeline job JOB failed: pipeline update ...`. Other jobs still run. Successful commands
exit 0; operation failures exit 1 with a diagnostic on standard error, including
`pipeline: N job(s) failed: JOB, ...` for per-job failures. `no_data` is a successful command
outcome, not evidence of acquired data.

### Archive catalog and transfer

A catalog is pretty-printed JSON with a trailing newline and every field below:

| Field | Meaning |
| --- | --- |
| `schema_version` | `1` |
| `layout` | `"daily-v2"` for daily dataset/stream pairs; absent for v1 catalogs |
| `lineage_manifests` | Optional additional pinned ready manifests: the continuation root and its stream for a daily descendant |
| `job` | producer job identifier |
| `broker`, `provider_symbol`, `instrument` | dataset source identifiers |
| `role` | dataset role, development or evaluation for this workflow |
| `source_kind`, `native_granularity` | original dataset kind and native representation |
| `coverage` | `first_event_time`, `last_event_time` from the dataset |
| `row_count` | dataset rows |
| `dataset`, `stream` | each has `generation`, `key`, `sha256`, `bytes`, `file_id` |
| `objects` | unique dependencies, each with `key`, `sha256`, `bytes`, `file_id` |

The manifest keys are `manifests/GENERATION/ready.json`; object keys are
`objects/SHA256HEX`. Catalog coverage describes actual endpoints; detailed acquisition coverage
remains in the dataset's provenance object. The remote catalog name is
`catalog-DATASET16-STREAM16.json`, objects are named `object-SHA256HEX`, and manifests
`manifest-GENERATION.json`. Names are not unique authority: retain the catalog file identifier
and expected digest.

The producer derives the closure from validated dataset/stream manifests and checks their source
generation, instrument, and role linkage. It persists pre-generated Drive file identifiers before
uploading. Resumable sessions are checkpointed before sending bytes; resumed sessions query status
for their acknowledged offset. Expired sessions restart under the same file identifier; an
ambiguous completion or status 409 reconciles that identifier. Transport failures, HTTP 429, and
5xx retry within `retry_seconds` from the first attempt, waiting 250 ms then doubling up to 30 s;
`max_attempts` limits 401 token refresh attempts and resumable-session restarts. Upload completion
compares size and SHA-256,
reading back and hashing when Drive supplies no checksum. Different content is never replaced.
Objects upload before manifests, and the catalog uploads last after those transfers confirm.
The local catalog receipt is then published.

Every reused transfer, including a previously archived closure, is confirmed again through the
same owner as a completed upload: an existing non-trashed remote file with the recorded size and
SHA-256, hashing the bytes read back when Drive supplies no checksum. Drive is not
provider-enforced immutable storage.
Refresh/access tokens, response bodies that might echo them, and resumable session locations
are not printed.

### List and restore

List follows archive-root catalog listing pages to completion, confirms each catalog's remote size
and checksum (with readback when absent), downloads and validates the catalog documents, filters by broker/symbol, and sorts
the resulting lines. It reads no market-data objects. The line is:

```text
catalog FILE_ID sha256 HASH INSTRUMENT ROLE NATIVE layout LAYOUT dataset G stream S coverage FIRST LAST rows N bytes B
```

`NATIVE` is `tick` or `5-second bar`; bytes sum unique catalog objects and both ready
manifests, excluding the catalog itself.

Pull is the consumer's one step: it enumerates the instrument's catalogs exactly as `list` does,
prefers the v2 lineage whenever present, then selects by coverage end and its unique
lineage descendant (v1 ties use dataset generation), and, unless both of its ready
manifests already exist in this configuration's managed store, restores it exactly as `restore`
would with that catalog's identifier and digest. It prints the `restored …` line, or
`pulled INSTRUMENT ROLE dataset URI stream URI catalog FILE_ID (already local)` when nothing was
fetched. It fails when the archive holds no catalog for the instrument.

Restore pins one catalog file identifier and expected SHA-256, checks broker/symbol assertions,
and uses that catalog's finite set of manifests and objects as its allowset. A supplied
`governance_manifest` is loaded first; its declaration permits the dataset before its manifest
read and checks the stream generation. Without a declaration, ordinary metadata classification
still rejects holdout before child-object reads. The dataset manifest must describe the ordinary
catalog generation, and the stream must derive from it with the same role. Every dependency must
match a catalog object key, hash, and size, and every catalog object must be named by one of the
two manifests, before object downloads. A catalog pin cannot override
a declaration's denial; supply any known study binding.

Partial downloads resume by byte range and require the final byte count and SHA-256. A corrupt
partial is discarded once and downloaded afresh. Identical completed local objects are reused;
conflicts are not replaced. Objects install first, original manifests last, then the existing
`data verify` owner verifies both dataset and stream. Producer paths are not needed and
manifest bytes and generation identities do not change. The output supplies local uniform
resource identifiers (URIs) for a new consumer configuration. After all pinned closure checks
pass, restore publishes the catalog's canonical receipt using the pinned file ID, byte count,
SHA-256, and evidence inventory. This recovers the newest receipt that cannot be included
inside its own catalog, preserving every producer record byte for byte. The output supplies
the verified manifest locations:

```text
restored INSTRUMENT ROLE dataset DATASET_URI stream STREAM_URI objects N installed I reused R
```

## Broker access

`crates/app/src/broker` owns synchronous market-data and options method groups over the shared
WebSocket transport. Engine records remain neutral; only app wire readers know provider fields.

| Adapter | History | Live market data | Options execution contract |
| --- | --- | --- | --- |
| `deriv` | raw ticks | ticks, acknowledged cancellation | proposals, claimed purchases, account transactions, contract facts, portfolio and statement |
| `pocket_option` | raw ticks and native five-second bars | streams, cancellation sent without acknowledgement | unsupported |

The Deriv public connection supplies discovery, tick history and subscriptions. Authenticated
connections first GET `{bootstrap_endpoint}/accounts` with a resolved bearer credential and
`Deriv-App-ID`, select the single active account of the declared class, then POST
`{bootstrap_endpoint}/accounts/{account_id}/otp` without a body and connect directly to the returned
address. Its path must be `/trading/v1/options/ws/{account_class}`. The adapter checks account and
currency on balance, and supplied currency on account events. Provider errors retain only the code
and request name; malformed-response diagnostics name the field without echoing its value. Inspection
reports omit provider error messages, keeping echoed login ids and authenticated addresses private.
Numeric fields are decoded from original bytes with `RawValue` and the shared exact
Decimal/price-unit readers. Outgoing amounts are unquoted exact tokens. Used handwritten structures
are pinned to `crates/app/schemas/deriv/production_v20260819_0`; its source inventory records the
release, archive digest, selected fields and absence of an upstream license statement. Builds do
not download schemas. Discovery returning `RateLimit` is unavailable evidence, never a successful
instrument inventory. Targeted `contracts_for` preserves the observed CALL/PUT `barriers:1` mapping.

Pocket Option uses the configured WebSocket address verbatim, including the operator-supplied
`EIO=4&transport=websocket` query. Socket.IO framing is the observed `0` opening, `40` namespace,
`42` event and single `451-` binary attachment subset. Text heartbeat `2` answers `3`; WebSocket
Ping answers Pong with the same payload. The `auth` argument is an opaque operator-held JSON object
resolved from the named environment variable. Fresh `successauth` and
`successupdateBalance.isDemo` matching the declared class precede market commands; selected symbols
must occur in the observed 19-element `updateAssets` rows. Incomplete attachments never become
observations. The adapter does not log authentication, renew credentials or generate chart points.

For candle history, Pocket Option sends `loadHistoryPeriod` with `asset`, incrementing
`index`, a provider-clock `time` anchor, `offset = 200`, and `period = 5`. It accepts
`loadHistoryPeriodFast` only with matching asset, index, and period. Row objects carry
`symbol_id`, `time`, `open`, `high`, `low`, `close`, and `volume`; unknown row keys fail.
Price text converts to exact integer units at the configured scale, and its persisted double
must round-trip to those units. The configured provider offset is subtracted before checking a
whole-second bar start on the five-second grid. Prices and volume must be finite, volume
non-negative, price bounds consistent, and time strictly increasing. The symbol identifier must
be constant within pages and across the retained lineage. These checks do not independently
establish the operator's clock/source mapping. This research pipeline acquires no Pocket ticks
and makes no tick subscription or order request.

Live records retain provider event time, local receipt time, the same full source identity as fetch, connection
generation, receipt sequence and payload SHA-256. Neither pinned provider has a durable tick
sequence; the receipt sequence detects internal loss/reordering but proves no provider completeness.
Every explicit reconnect starts a new generation and a continuity break, requiring resubscription
and a fresh causal stream rebuilt from verified history before entries resume. The local reconnect
proof rebuilds `InstrumentStream` from a verified generation, then feeds live rows; live rows alone
do not finalize a candle before that warm-up. Phase 12 owns the production readiness gate.
Deriv epochs are seconds.
Pocket Option fractional provider seconds convert exactly to microseconds after subtracting the
configured offset times 60 seconds; request anchors convert back to the provider clock. That offset,
account class, endpoint and pinned schema/mapping bind both history and live source identity. The observed 120 minutes is not a
universal default. Market ticks from both adapters use the same `Tick` and `InstrumentStream` owners.

Deriv request admission enforces both sliding windows per connection. Configuration may lower them.
Unhandled failures stop the caller; there is no automatic retry or endpoint fallback.

| Group | Used requests | Per minute | Per hour |
| --- | --- | ---: | ---: |
| Trade | proposal, buy, proposal_open_contract | 360 | 14400 |
| Account | balance, statement | 100 | 2000 |
| Portfolio | portfolio | 30 | 1500 |
| Other | discovery, history, ticks, forget, transaction subscription | 220 | 14400 |

`binary-alpha data fetch --config PATH` acquires each selected instrument sequentially. One pass
freezes its requested end, anchors its first page there, pages backward, validates chronological rows, applies exact local
`[start,end)` bounds and removes only identical page-boundary overlap. Within-page repeats remain
source observations. Every pass preserves all previously verified rows in normalized output and
checks incoming overlap against them before resume-bound filtering, including multiplicity;
a missing verified row or changed price stops publication, including after restart.
Deriv requests 100 tick rows with its seconds anchor. Pocket `changeSymbol` requests period 1;
`loadHistoryPeriod` uses the earliest provider-clock token, index, offset 200 and period 1, while
matching the observed period-0 reply by asset and index. Initial replies must have period 1 and
older replies period 0; other periods fail. Empty or non-progressing pages report an
unresolved prefix, never historical exhaustion. An initially empty pass retains raw evidence and
coverage but cannot publish a dataset manifest requiring actual first/last events. Newly verified
coverage ends one microsecond after the last received observation, capped at the requested end;
a received point at or beyond that end establishes the upper bound. Connected prior and new
verified ranges are united without shortening the prior end; restart selects the greatest verified
end and, among equal ends, the earliest start. An unverified suffix remains `unresolved_tail` and
is requested again from the merged verified end. If a prefix and tail are both unresolved, `shortfall` preserves the prefix and `tail_shortfall`
records the tail; neither is skipped.

The shared import publication owner retains and publishes immutable `broker_history` generations:
raw response objects, `normalized/ticks.parquet` (or `normalized/bars.parquet` for native bars),
and `provenance/coverage.json`. The coverage record's version 1 separates requested range,
verified range, actual first/last times, row count,
page hashes/anchors and shortfalls. The `broker_history` dataset manifest stays at schema version 1
with the versioned `provenance/coverage.json` object. The source kind requires the native capability
and corresponding objects described under [Historical datasets](#historical-datasets).
Only ready publication advances verified progress.
Interrupted objects remain reusable; restarting repairs an unresolved prefix before extending the
suffix. A refresh interval completes the initial range, then waits between sequential passes whose
new end is sampled once. Unchanged content/coverage reuses the generation and ensures its objects
and manifest exist at the current destination, without refetching a completed range. Standalone
fetch stays in the foreground; the research pipeline's optional weekly service is described in
[operations](operations.md#data-pipeline).

Each instrument reports
`fetch ROLE INSTRUMENT generation G requested START END verified START END rows N pages P objects O reused R shortfall REASON [fetch S publish S]`,
with `none` for unavailable verified bounds or no shortfall. Reuse adds `(already published)` or
`(no new data)`. `data verify` and `data audit` consume these generations through the existing owners.

`binary-alpha broker inspect --config PATH` checks discovery, targeted contracts, finite history,
per-instrument live observations until the row target or deadline, then cancellation with a separate
uncapped observation window. It keeps consuming receipts for continuity after an instrument reaches
its target. Credentialed Deriv inspection checks bootstrap/balance, records the identifier-only
transaction acknowledgement without posting cash, and, when configured, requests one CALL and one
PUT proposal per history instrument. It reports exact ask price, payout, original spot text, spot
time and longcode. Economic admission belongs to Engine and is not computed by inspection. It never
buys. The JSON report preserves per-check verified/unavailable/observed results, is retained by
content address and local inspection name, published through the artifact store, and ends with
`inspection URI`. Built-adapter external acceptance requires retaining an inspection authorized for
the exact provider, account and action; the local fixtures do not establish that acceptance.

## Instrument streams

The engine owns the `InstrumentStream` state machine, its `Observation` input, `Candle` output,
`InstrumentProfile`, and the stream manifest and generation identity; the application owns
reading a published generation, feeding it in order, and publishing the outputs.

### Records and clocks

A stream is bound to one configured instrument and one source generation whose capabilities,
native granularity, and, for an integer-unit source, price scale agree with the definition;
a tick instrument bound to a bar-only generation is refused with the machine-readable capability
error, and a bar instrument's profile records every tick calculation (`tick_count`, `tick_path`,
`tick_gaps`, `entry_tick`, `tick_settlement`) as unsupported with that same reason. Every record
carries a provider event time and a known-at time: a tick is known at its event time; a bar
starts at its event time and is known at its end. Bar prices convert exactly to integer units at
the instrument's price scale or are rejected. Records arrive in event-time order, one at a time,
through the same `push` for historical, replay, and live feeds; a refused record (backwards time,
a bar that does not strictly follow the previous bar, a tick with a different price at the
previous tick's event time, a bar off its grid or of another period, a non-finite or negative
volume, a contradicted high/low relationship, a bar whose volume would push a candle's summed
volume out of the finite range, a time beyond `i64::MAX / 4` microseconds either side of the
epoch (about 73,000 years, so every interval boundary and difference stays representable), or a
record of the other granularity) is reported
with its reason, event time, known-at time, and source generation, and leaves the state
unchanged. A tick identical to the previous tick is accepted, counted as a duplicate, and folded
like the source retained it. The one observed move between consecutive records is from the
previous close to the record's open; a bar's high and low bound its prices and the observed
price step but never form a path, because their order inside the bar is unobserved. The time
between consecutive records is measured from the previous record's known-at time to the
record's event time (zero for contiguous bars), so a gap is uncovered time; the cadence is
measured between event times. Nothing is filled, interpolated, defaulted, or inferred from a
symbol.

### Candles

Each configured stream buckets records into left-closed intervals of `duration_seconds` whose
boundaries lie `offset_seconds` after the Unix-epoch grid. A candle finalizes only when a record
whose known-at time reaches the interval's close arrives: a record inside the interval is folded
first and finalizes it when its own known-at time reaches the close (a bar ending at the close),
while a record at or after the close finalizes it without contributing and opens the next
interval. The candle's known-at time is that record's known-at time. The end of input finalizes
nothing; the unfinished last interval is withheld and its record count is reported in the
profile. Missing intervals are never emitted. A finalized candle records its open and close time,
known-at time, first and last event time, active span (last known-at time minus first event
time), open, high, low, and close units, record and duplicate counts, the summed source volume
for bars, the time from the previous record to its first record (absent for the first record of
the stream), the longest inter-arrival time inside it, the number of missing intervals before it,
the most records in one run of one unchanged price and the longest span of such a run
(independent maxima), and the
largest relative move in whole basis points (`floor(10000 · |move| / |previous price|)`, exact
in integer arithmetic, `i64::MAX` at most (the candle column's limit, far past the 2^53 basis
points where the reference's floating-point value stops being exact), undefined and skipped
after a zero price) over the
moves that enter or lie inside it, in each of three inter-arrival contexts: contiguous (the
time before the move is at most `gap.max_seconds`), delayed (over that but under
`gap.reopen_seconds`), and reopen (at least `gap.reopen_seconds`); without a `gap` check every
move is contiguous.

The enabled checks decide the flags, with the same comparisons the pinned resampler and research
policy applied: `low_activity` and `hard_low_activity` when the record count is below the
stream's `min_observations` or `hard_min_observations`; `gap_before` and `gap_inside` when the
gap before or the longest inter-arrival inside exceeds `gap.max_seconds`; `missing_before` when an
interval before it is missing; `frozen` when a run reaches `frozen.min_observations` records or
`frozen.min_seconds`; `jump`, `delayed_jump`, and `reopen_jump` when the largest move of that
context reaches `jump.min_basis_points`; and `short_span` when the active span is below
`span.min_percent` of the duration. `complete` is the absence of every gap flag and of hard low
activity; `clean` is the strict eligibility verdict, the absence of every flag. The pinned
resampler is a reference with three limitations the target does not reproduce: it parses every
timestamp to whole milliseconds through binary floating point (a sub-millisecond time is
truncated and a whole-millisecond time can shift by one), it parses prices as binary floating
point (two prices that differ at the configured scale but share one binary value are one price
to it), and it treats a run of one price that starts at the Unix epoch as absent; the target
keeps exact microseconds, exact units, and every run.

### Profile

The profile records only facts a validator or a feature-compatibility check consumes: the
instrument identity, currencies, and price scale; the source generation, kind, role, native
granularity, scale, and capabilities; record and duplicate counts and first and last event time;
the cadence (event-time micros between consecutive records by bit length, where bucket `k`
holds values in `[2^(k-1), 2^k)` and bucket `0` holds zeros); observed prices (minimum, maximum,
the number of nonzero moves between consecutive records, and the greatest common divisor of
every nonzero difference between a record's prices and the previous close, or the record's own
open for the first record, as the observed price step); gaps over the threshold (count, longest, total); closed frozen runs that met the
thresholds (count, most records in one run, longest span of one run); moves in whole basis points by bit length
with the counts that reached the jump threshold in each context; records inside each
session window and outside every window; per stream the finalized count, the withheld record
count, records per finalized candle by bit length, and how many candles carried each flag,
`complete`, and `clean`; and the supported calculations. Every count except the withheld record
count covers only closed windows, so a longer input extends the profile and never revises what a
shorter input reported, and the
finalized candles of any prefix are a prefix of the full output. The profile is evidence, never
self-modifying configuration.

### Stream generations

`binary-alpha data audit --config PATH --manifest URI` reads the dataset ready manifest at `URI`
through the same store grammar as `data verify`, refuses a holdout generation before reading any
object (research never audits holdout data; certification is a separate authorization), binds
the generation to the configured
instrument that maps its identity and native granularity (an identity mapped only at another
granularity is bound so that the capability error names what the source lacks; an unmapped
identity is an error, never a default), verifies and decodes every data object in manifest order
through the Phase 02 readers, feeds every record, requires the observed record count and
coverage to equal the manifest's `row_count` and `coverage` before it publishes anything (candle
rows stream into temporary files under the retained folder while the input is read), and writes
one candle object per stream and the profile. The stream generation's identity is the SHA-256, rendered as sixty-four lowercase
hexadecimal digits, of the UTF-8 text `binary-alpha instrument stream generation v1`, one line
feed, the source generation, one line feed, and the instrument's canonical definition. Its objects
are `profile.json` (the profile as pretty-printed JSON with two-space indentation and one trailing
line feed) and `candles/DURATIONs_OFFSETs.parquet` per stream (Zstandard Parquet with the fixed
`binary_alpha_candles` schema, `TIMESTAMP(MICROS, true)` clocks, signed 64-bit units and counts,
optional `volume` and `gap_before_micros`, and boolean flags, plus file metadata `broker`,
`provider_symbol`, `price_scale`, `duration_seconds`, `offset_seconds`, and
`stream_schema_version`), all with the object role `normalized`. They are retained in the
historical-data folder and published under the content-addressed keys and create-once rules of
dataset generations; the ready manifest at `manifests/GENERATION/ready.json` is published last
and mirrored, and a committed manifest must describe the same generation, role, observations,
coverage, streams, and objects. It records, in order, `kind` (`instrument_stream`),
`schema_version` (`1`), `generation`, `broker`, `provider_symbol`, `instrument`, `role`,
`source_generation`, `source_kind`, `definition` (the instrument entry), `config_hash`,
`code_revision`, `observations`, `coverage`, `streams` (per stream the duration, offset, row
count, first open time, and last close time), and `objects`. The command writes one line to
standard output:
`audited INSTRUMENT ROLE generation GENERATION from SOURCE observations N candles C objects K reused R`
followed by `[stream S publish S]` stage durations in seconds or by `(already published)`.
`data verify` on a stream generation asserts every object's bytes and hashes, decodes the profile
and every candle object, checks that they describe the manifest's instrument, price scale,
source generation, kind, and role, observations, coverage, and per-stream rows and bounds (every
volume finite), and writes
`verified INSTRUMENT ROLE generation GENERATION candles C objects K bytes B`.

## Feature plans

The engine owns the finite compiled output table, the immutable `FeaturePlan`, the
`FeatureEngine` that extends the instrument stream with per-stream feature state, the structure
and sequence events, and the pure encoder; the application owns reading the input and profile
generations, the temporary tables, one-column-at-a-time fitting and encoding, publication, and
reconstruction. Every formula reproduces the pinned reference (legacy revision
`b509964cd1c40180e9d98b0e55a95699b0abe9ed`) at its evidenced precision.

### Resolution

`binary-alpha features build --config PATH` resolves every `features.instruments` entry, then
builds each in order. Every resolved instrument, role, and stream has one owning entry; a second
entry owning one is refused before anything is streamed or published. Resolution reads the input
ready manifest and the profile ready manifest as metadata first: the input is
a dataset generation whose role equals the declared `role` (holdout never enters), the profile
is an instrument stream generation of role `development` for the same instrument identity and
native granularity, and only then is the profile object read. A new plan requires the input to
be the generation the profile was audited from; a frozen plan requires the profile reference
the plan was frozen under, and the input may be a development or evaluation generation of that
instrument. Certification against holdout data is a separate authorization that this command
never performs.

The bound definition supplies the candle streams, quality checks, and price scale; the profile
supplies whether individual ticks exist (every tick calculation supported). A new plan freezes
the entry's formula settings, then compiles the output table for those settings and resolves
every output per stream against its prerequisites: individual ticks for tick counts, tick-path,
tick-volume means and ratios, tick gap, jump, and frozen diagnostics, the quality regime
component, and the composite regime; the stream's presence in `tick_path_streams` for the
eighteen tick-path outputs (any configured tick stream may carry a path); the definition's
`gap`, `frozen`, `jump`, `min_observations`, and `hard_min_observations` for the flags that
read them; the `structure` settings for rolling, swing, event, and state outputs, with rolling
windows 5 and 20 for compression, `range_to_avg20`, and `structure_state`, and windows 5, 10,
and 20 plus `price_epsilon` for the trend regime; `price_epsilon` for sequence outputs;
`rolling_window` and `min_history` for prior-history ratios; and each configured
moving-average period for its outputs, with 20 and 50 for the pair outputs. Under
`all_supported` every unmet output is excluded with the exact missing prerequisite; under a
named list an unmet or unknown output is an error naming the missing prerequisite, and the
candle identity and clock outputs (open, close, known-at, first and last event time, ordinal,
and the four unit prices) are always selected. Each selected output records its kind, stage,
whether it is predictive, and a readiness text stating when it is available and what an
unavailable value means; the engine folds ticks and holds rolling, structure, sequence,
prior-history, and moving-average state only for the stages and periods the selected outputs
read. Bars never
substitute volume, counts, zero diagnostics, or `clean`; bar-compatible geometry, patterns,
prior ratios, moving averages, returns, momentum, efficiency, structure, sequences, and the
trend, volatility, structure, transition, and bias regime components remain available.

`rolling_statistics_v1` is present only when the plan's recorded `definitions.statistics`
names it. New plans record that definition; plans without it retain their original
`all_supported` membership and raw identity. For each configured structure window `w`, the
following use the last `w` accepted candles, oldest to newest. Return statistics use the
unrounded `bps_change` values used by `Rolling::update`, before `return_1_bps` is rounded
for publication; a return that rounds to zero remains nonzero in these calculations.
Thus `w` returns require a preceding candle. `return_std_{w}_bps` is the population
standard deviation; `return_skew_{w}` is the population third central
moment divided by variance to the power 3/2; `return_kurtosis_{w}` is the population fourth
central moment divided by squared variance, minus 3. `return_autocorr_{w}` is Pearson
correlation of returns 0..w-2 and 1..w-1. `sign_reversal_rate_{w}` is the opposite-sign
share of adjacent pairs for which both returns are nonzero, requiring at least two
adjacent pair positions and at least one eligible pair. `up_move_ratio_{w}` is
positive-return sum divided by absolute-return sum.
`trend_r2_{w}` is `1 - SSE/SST` from ordinary least squares of close units on indices
0..w-1; `trend_residual_{w}_bps` is 10000 times (last close minus fitted last close)
divided by last close. `range_position_{w}` is (last close minus minimum low) divided by
(maximum high minus minimum low). The standard deviation, up ratio, and range position
start at `w = 2`; skew, R², and residual at `w = 3`; kurtosis, autocorrelation, and
reversal rate at `w = 4`. A missing preceding candle, unfilled window, zero return
variance for skew/kurtosis, zero series variance for autocorrelation, zero absolute-return
sum for up ratio, zero close variance for R², zero last close for residual, and zero
high-low span for range position yield unavailable values. All non-finite computations
are unavailable. Each finite emitted statistic is rounded with `six` once. A flat return
window has standard deviation zero when filled.

Per candle, `range_overlap` is the positive overlap of current and prior high-low ranges
divided by the current high-low range; it is unavailable without an immediately preceding
accepted candle with no skipped or rejected stream interval, or with zero current range.
`candle_pattern` uses the same prior requirement and is unavailable when it is unmet.
It compares opposite non-doji bodies in exact price units, using `is_doji` as computed
from six-rounded `body_to_range <= 0.10`. It is `bullish_engulfing` or `bearish_engulfing`
when current body endpoints enclose the prior body, otherwise `bullish_harami` or
`bearish_harami` when the current
body is inside it; equality counts, engulfing takes priority, and every other case with
an adjacent prior, including a doji, is `none`. Every statistics output records these
warmup and degenerate rules in its own `readiness` text. Both bar and tick-built candles
use this one causal computation.

### Computation

One ordered chain per stream consumes the Phase 03 stream's records: the tick path and the
floating jump magnitudes of an interval are folded from ordered accepted ticks before its
candle finalizes (the move entering an interval is ignored by the path, an identical repeat is a
flat move, and only the last `ceil(directional moves / 3)` nonzero signs are retained); every
finalized candle advances the ordinal; only a candle whose Phase 03 verdict is `clean` becomes a
row, in this order: candle facts, tick-path summary, anatomy, rolling structure, swings and
events, sequences, candle shape with moving averages, and regime components. Structure, swing,
sequence, and rolling state advance across every accepted candle; the prior-history ratios and
moving averages reset when the accepted candle's ordinal is not the previous accepted ordinal
plus one. A swing is confirmed only after the configured right-side candles close; its event
close is the center candle's close and its confirm close the confirming candle's close, and the
row that confirms it applies it before that row's own events. A row carries its candle's close
time (the logical decision clock the reference calls `row_decision_time_utc`) and its known-at
time (actual availability); no clock is backdated.

Prices convert from canonical integer units to binary floating point exactly where the
reference parsed decimal text; anatomy, rolling, and moving-average arithmetic follow the
reference's operation order (window sums use the reference interpreter's compensated
left-to-right `sum`); and the normalized values the reference wrote to six-place text
before a later stage read them (`body_bps`, `range_bps`, wick basis points, `close_position`,
`body_to_range`, wick ratios, returns, momentum, efficiency, means, `range_to_avg20`,
distances, ratios, and moving-average basis points) are stored as those six-place values, while
tick-path categories read the unrounded ratios, moving-average state stays unrounded, and
`ema{p}` is stored unrounded. Canonical units (`open_units`, `high_units`, `low_units`,
`close_units`, `body_units`, `range_units`, wick units, swing and confirmed-swing units, event
price and level units) are exact integers, and candle direction, color, tick-move sign and
flatness, and the sequence epsilon comparison are decided on exact units (a unit difference
beyond signed 64-bit is unavailable, never wrapped); time-valued members carry microseconds. Category
vocabularies, thresholds, and comparisons are the reference's, and the gap-class ladder,
tick-path thresholds, shape thresholds, moving-average states, and regime rules are versioned
compiled policies named in the plan, not settings.

### Encoding and freeze

The formula settings and the raw identity (SHA-256 over
`binary-alpha feature raw identity v1`, the stream generation, the profile object hash, the
development generation, the canonical settings, and the definition versions) freeze before
computation; the raw rows carry that identity and never a plan hash. After the rows exist, the
application rereads one selected column at a time and fits each encoding on every development
row: a category or boolean output labels its text (empty text is `none`, missing is
`missing`); a compiled `NAME_bucketed` projection classifies its input into the source-defined
right-closed bins with the first edge included; a compiled `NAME_dev_quantile` projection or a
`development_fifths` output cuts at the linear-interpolated development quantiles 0.2, 0.4,
0.6, and 0.8 with duplicate cuts removed and unbounded tails, and fewer than four distinct
development values yield no labels (a compiled projection of a `_micros` duration input,
including the active-span quantile, first divides by the plan's recorded `input_divisor` of
1000, so its edges and labels are the reference's millisecond values); bin labels are
`LEFT_to_RIGHT` in the reference's
six-significant-digit general format, and duplicate labels are an error rather than merged
intervals. Labels rank by development count descending then text ascending, are limited to
`max_labels`, and take zero-based signed 16-bit codes; a missing, unseen, out-of-range, or
uncoded (`""`, `missing`, `none`, `<NA>`, `nan`, `NaT`) label encodes as `-1`, and raw values
stay beside their codes.

With `encodings.outputs = "all_supported"`, the plan attempts development fifths for
every selected predictive numeric output and category fitting for every selected predictive
text or boolean output. Each encoding's output is a deterministic distinct name; `input`
names the raw column. Only development rows passing that input's plan-declared readiness
flags and `value_ready` contribute to automatic fitting. No ready values produce no edges
or labels. Fewer than four distinct ready numeric values or duplicate six-significant-digit
interval labels leave automatic numeric edges absent and labels empty. Explicit encoding
lists retain fitting on all development rows and their duplicate-label error.

The fitted plan records the fit windows (rows and first and last
decision time per stream), every label list and edge list, and its identity is SHA-256 over
`binary-alpha feature plan v1` and the plan's JSON bytes. Applying a frozen plan recomputes
rows under its settings and encodes under its labels without refitting; no artifact records
wall-clock time. A consumer may address one development-fifths interval by its zero-based
low-to-high ordinal `0` to `4`: the label is the `LEFT_to_RIGHT` text of the right-closed bin
between the fitted cuts and the unbounded tails, and it resolves only when the fit produced four
distinct cuts and retained that label under `max_labels`; frequency-ranked codes are never
ordinals.

### Feature generations

A feature generation's identity is SHA-256 over `binary-alpha feature generation v1`, one line
feed, the plan identity, one line feed, and the input generation. Its objects, all role
`normalized` under the content-addressed create-once rules of dataset generations, are
`plan.json` (the plan as pretty-printed JSON with one trailing line feed) and, per stream,
`rows/DURATIONs_OFFSETs.parquet` (one optional column per selected output in plan order, schema
`binary_alpha_feature_rows`), `events/structure_DURATIONs_OFFSETs.parquet`
(`binary_alpha_structure_events`: event identity, type, direction, event and confirm close,
the confirming candle's known-at time, rows and ordinals, price and level units, and the
reference kind and close), `events/sequence_DURATIONs_OFFSETs.parquet`
(`binary_alpha_sequence_events`: event identity, row and ordinal, decision close, the confirming
candle's known-at time, swing type and side, prices and clocks of the swing and the previous
same-side swing, and the sequence and bias after it), and, only for a stream with encodings,
`encoded/DURATIONs_OFFSETs.parquet` (`binary_alpha_encoded_rows`: one required 16-bit column per
encoding). A manifest naming any other object is invalid. Every table is Zstandard Parquet with
footer metadata `broker`, `provider_symbol`,
`price_scale`, `duration_seconds`, `offset_seconds`, `raw_identity` (rows and events) or
`plan_identity` (encoded rows), and `feature_schema_version`. The ready manifest at
`manifests/GENERATION/ready.json`, published last and mirrored, records `kind`
(`feature_generation`), `schema_version` (`1`), `generation`, `broker`, `provider_symbol`,
`instrument`, `role`, `input_generation`, `plan_identity`, `frozen_from` (the generation the
plan was frozen by, or `null` for a fit), `profile_generation`, `config_hash`,
`code_revision`, `observations`, `streams` (per stream the rows, structure and sequence event
counts, and first and last decision time), and `objects`. Before publishing, the command
requires the observed records and coverage to equal the input manifest and, for a fit, the
recomputed profile to equal the bound profile; after publishing the objects and before the
ready manifest, it reconstructs the generation from the published objects under the manifest
bytes about to become ready, so a generation its verifier rejects is never marked ready. It
writes two lines to standard output:
`features INSTRUMENT ROLE generation GENERATION plan PLAN input INPUT observations N rows R events E objects K reused U`
followed by `[stream S fit S encode S publish S]` stage durations in seconds or by
`(already published)`, then the reconstruction line below. `data verify` on a feature
generation asserts every object's bytes and hashes, decodes the plan and checks its identity,
instrument, profile, fit, and streams against the manifest and the manifest's object set against
the plan's, checks every table's columns,
footer metadata, and row count against the plan and manifest and the rows' decision-time
bounds, and writes
`verified INSTRUMENT ROLE generation GENERATION rows R events E objects K bytes B`.

## Outcomes

A future-only binary-expiry outcome is a historical research label, never a decision-time
feature or part of the live feature graph: for one decision row of a feature generation and one
expiry it names the tick a contract would have entered at, the tick it would have settled at,
and whether a buy or a sell would have paid. The engine module `outcomes` owns the label rule,
the reader, the identities, and the manifest; `binary-alpha outcomes build --config PATH` binds
the inputs, publishes the generation, and reconstructs it; `data verify` re-reads it. No feature
implementation reads an outcome, and research reads labels only after feature identity and any
fitted encodings are frozen.

### Configuration

The optional `outcomes` table declares, in this order: `role` (`development` or `evaluation`;
`holdout` is rejected before anything is resolved); `tick_manifest`, the ready manifest of the
Phase 02 observation generation; `feature_manifest`, the ready manifest of the Phase 04 feature
generation computed from that observation generation; `expiry_seconds`, a non-empty sorted unique list
of positive seconds (the reference's 30 through 300 seconds is a fixture choice, never a limit);
the non-negative millisecond thresholds `max_entry_delay_ms`, `max_settlement_delay_ms`,
`max_tick_gap_ms`, and `true_jump_max_gap_ms`; `true_jump_basis_points`, positive decimal text
such as `"5"` or `"2.5"` with at most eighteen fraction digits, parsed through the exact price
boundary and compared exactly; and the positive `frozen_min_ticks` and `frozen_min_ms`. A
millisecond threshold that overflows microseconds is rejected. Manifest locations use the
`manifests/GENERATION/ready.json` grammar of `data verify`. Omitting the table preserves every
existing configuration identity. The build requires `run_mode = "research"`.

### Binding

An observation generation is a tick generation, or a bar generation in which each bar is priced
at its close at the bar's end, the first instant that close is known. The `tick_manifest` must
name a dataset ready manifest carrying the declared role; unauthorized holdout is refused on
manifest bytes alone. Integer-unit ticks retain their scale, which must equal the fitted plan's
scale. Float bars convert exactly to integer units at the plan's instrument scale, rejecting a
price that needs more fraction digits. The existing `tick_generation`, `tick_manifest`, and
`tick_count` fields and tick array paths name this bound observation series; tick identities and
the label algorithm are unchanged. In the label rule below, a tick names one member of this
bound observation series.

For bars, labels select the first bar end at or after the row's logical close, then the first bar
end at or after that entry plus the expiry. Entry and settlement delay bounds measure those
actual differences: zero when aligned, and more than one period only when a bar is missing.
`max_tick_gap_ms` bounds adjacent bar-end spacing, whose native cadence is one period;
`frozen_min_ticks` counts consecutive equal closes; `true_jump_max_gap_ms` limits the adjacent-end
spacing over which the close-to-close jump test applies. Reason precedence, including no
settlement at the tail, is unchanged. These observations describe sampled closes, never the path
inside a bar. Tight thresholds remain valid; no period floor is imposed. Historical replay
retains its own availability and acceptance clocks and execution-contract thresholds, separate
from outcome thresholds: delayed simulated acceptance between bar ends uses the latest close.

The feature manifest must be a feature generation whose `input_generation` is the observation
generation; its plan is read and checked as a frozen plan, and every stream's rows table must
carry the plan's frozen `raw_identity` in its
footer. The `close_time_micros` column of each stream is read in physical order and must equal
the feature manifest's row count and first and last decision times before anything is labeled;
outcome row `i` of a stream is feature row `i` of the same `(duration_seconds, offset_seconds)`
stream. The command does not rebuild features, fit encodings, or resolve holdout.

### Label rule

Every tick of the generation is loaded in order; a backwards time and more ticks than can be
indexed below the missing index are rejected. Three flags are folded once over the whole
generation: a transition into a tick is a gap when its inter-arrival exceeds `max_tick_gap_ms`;
it is a true jump when its inter-arrival is at most `true_jump_max_gap_ms` and
`10000 · |move| ≥ true_jump_basis_points · |previous price|`, compared exactly and never after a
zero price (the pinned builder divides by the signed previous price, so a negative previous
price is a documented departure that no registered instrument reaches); and every member of a run of one unchanged price is frozen when the run reaches
`frozen_min_ticks` ticks or `frozen_min_ms` elapsed. Names declare units; every comparison is
made in native microseconds and price units.

The reference time of a decision row is its `close_time_micros`, the logical decision clock of
Phase 04; its `known_at_micros` remains its actual availability, and these labels establish no
executable decision or broker entry evidence. The entry tick is the first tick at or after the
reference time. For each expiry the due time is the entry tick's time plus the expiry, the
settlement tick is the first tick at or after the due time, and the cell's reason is the first
that applies in this order, stored as its position: `0` valid; `1` no entry (no tick at or after
the reference time); `2` stale entry (the entry tick is more than `max_entry_delay_ms` after the
reference time); `3` no settlement; `4` stale settlement (the settlement tick is more than
`max_settlement_delay_ms` after the due time); `5` internal gap; `6` frozen run; `7` true jump.
Gap and jump checks cover the transitions after the entry tick through the transition into the
settlement tick; the frozen check covers the entry tick through the settlement tick. A later
reason never overwrites an earlier one. A missing entry or settlement leaves its dependent
fields unavailable, an invalid cell is never a result, and a valid cell whose settlement price
equals its entry price is a tie, never an assumed loss; otherwise a higher settlement price pays
a buy and a lower one pays a sell.

### Outcome generations

An outcome generation's identity is SHA-256 over `binary-alpha outcome generation v1`, one line
feed, the tick generation, one line feed, the feature generation, one line feed, and the JSON of
the resolved rule (expiries in seconds, thresholds in microseconds, the jump text, and the
frozen thresholds); the domain names the outcome definition, so a change to the label rule
changes every identity. Its objects, all role `normalized` under the content-addressed
create-once rules of dataset generations, are little-endian arrays: `ticks/event_time_micros.bin`
and `ticks/price_units.bin` (one signed 64-bit value per tick) and, per stream,
`reference/DURATIONs_OFFSETs.bin` (one signed 64-bit reference time per row),
`entry/DURATIONs_OFFSETs.bin` (one unsigned 32-bit entry index per row),
`settlement/DURATIONs_OFFSETs.bin` (the row-major rows by expiries unsigned 32-bit
settlement-index matrix), and `reason/DURATIONs_OFFSETs.bin` (the row-major unsigned 8-bit
reason matrix). The maximum unsigned 32-bit value is the missing index. Due times, entry and
settlement times and prices, and results are derived through the reader, never stored. The
ready manifest at `manifests/GENERATION/ready.json`, published last and mirrored, records
`kind` (`outcome_generation`), `schema_version` (`1`), `generation`, `broker`,
`provider_symbol`, `instrument`, `role`, `tick_generation`, `tick_manifest`,
`feature_generation`, `feature_manifest`, `raw_identity`, `config_hash`, `code_revision`,
`rule`, `reference_clock` (`feature_row_close_time`), `time_unit` (`microsecond`),
`price_representation`, `missing_index`, `tick_count`, `streams` (per stream the rows and
first and last reference time), and `objects`. Wall time and peak memory belong to run
evidence, never to the artifact.

The command writes
`outcomes INSTRUMENT ROLE generation GENERATION tick TICK feature FEATURE ticks N rows R cells C objects K reused U`
followed by `[load S label S publish S]` stage durations in seconds or by `(already published)`,
then the reconstruction line. After publishing the objects and before the ready manifest, it
reconstructs the generation from the published objects under the manifest bytes about to become
ready. Parsing a manifest rejects one whose manifest references do not name its recorded
generations, whose rule is invalid, or whose tick count cannot be indexed below the missing
index. `data verify` on an outcome generation asserts every object's bytes, hashes, and
dimensions, reads the tick arrays (under the tick sequence rules) and every stream's reference
times back, recomputes every entry index, settlement index, and reason under the manifest's
rule, compares them with the stored arrays, and writes
`verified INSTRUMENT ROLE generation GENERATION rows R cells C objects K bytes B`.

## Execution

The engine module `execution` is the sole owner of strategy evaluation, chronological admission,
settlement, accounting, and risk. Historical replay, research, and live adapters hand it
observations in availability order and it returns canonical `FinancialEvent` records; applying
those records back through the same event-application function restores every financial state and
summary projection. `binary-alpha replay --config PATH` binds governed historical inputs, runs the
configured simulation through the engine, and publishes the ledger as an `engine_replay`
generation that `data verify` restores. Reports are projections of the ledger, never decision
owners; outcome labels are diagnostics that authorize no admission, settlement, refund, or
capacity release.

### Exact arithmetic

Money is a checked signed 128-bit coefficient at a decimal scale of 0 through 18, parsed through
the shared decimal boundary from plain decimal text such as `"9.50"`. Identity compares the
normalized value; postings keep the declared account scale; rescaling, addition, subtraction, and
multiplication are checked and reject overflow or any lost precision. No rounding or binary
floating point touches purchase, cash, payout, fee, conversion, or risk values; feature and model
arithmetic remains validated floating point. Cross-currency arithmetic happens only through the
conversion function.

### Configuration

The optional `replay` table declares, in canonical order: `role` (`development` or
`evaluation`; `holdout` is rejected before any manifest is read), the nonempty half-open decision
interval `decision_start` and `decision_end`, `reporting_currency`, `reporting_scale` (0 through
18), `max_rate_age_micros` (non-negative), then the arrays `inputs` (one `tick_manifest`,
`feature_manifest`, and optional `outcome_manifest` per instrument; list order resolves equal-time
ties), optional `splits` (unique names over nonoverlapping half-open intervals inside the decision
window), `accounts` (unique `id`, `broker`, `currency`, `scale`, non-negative `initial_cash`
representable at the scale), `strategies` (unique `id`, `plan_identity`, `base_stream`, a nonempty
`conditions` conjunction, and an optional `repair` conjunction; each condition names its `stream`,
`output`, `comparator` among `eq`, `ne`, `lt`, `le`, `gt`, `ge`, and a typed `threshold`: text and
booleans compare only with `eq` and `ne`, numbers must be finite), `bindings` (ordered; unique
`id`, references to a strategy, account, `BROKER:PROVIDER_SYMBOL` instrument at the account's
broker, contract in the account's currency, and risk policy, plus the frozen `envelope`),
`contracts` (unique `id`, `direction`, positive `duration_micros`, `currency`, positive `stake`
and `quoted_cost`, non-negative `entry_fee`, exhaustive `win`, `loss`, and `tie` cashflows of
non-negative `gross_return` and `terminal_fee`, `settlement` with `rule` (`price_at_due_v1` or
`broker_authoritative_v1`), `max_settlement_delay_micros`, and `max_tick_gap_micros`, then optional
`semantics` (`rise_fall_strict_v1`)), `risk_policies` (unique `id`; optional
positive `max_open_per_strategy`, `max_open_per_duration`, `max_open_per_instrument`,
`max_open_per_account`, and `max_open_total`, where absence is no limit; `same_entry` as `all` or
`first`; `deduplicate_signal_logic`; non-negative `max_feature_age_micros` and
`max_quote_age_micros`; optional positive `max_unresolved_loss_per_account` and
`max_unresolved_loss_total`; optional `pause` with positive `drawdown` and `duration_micros`; optional non-negative
`max_proposal_age_micros`, required for broker-authoritative bindings), and
optional `rates` (unique `id`, distinct `source_currency` and `reporting_currency`, `provider`,
`provider_time`, `available_at` no earlier than the provider time, positive `rate` in
reporting-currency units per source unit), and the optional `scenario` descriptor (`schema_version`
`1`, an identifier `id`, and a non-negative `acceptance_delay_micros`; see
[Research](#research)), whose omission is immediate acceptance with every existing identity
preserved. Every contract amount, loss limit, and pause threshold
must be representable at the scale of each account it binds to. Two bindings sharing an account,
instrument, contract duration, same-entry key, or deduplication key must declare that scope's
policy identically, including absent versus configured limits; two bindings of one deployment
strategy on one account are rejected. Omitting the table preserves every existing configuration
identity. The command requires `run_mode = "research"`.

### Identities and records

The signal-logic identity is SHA-256 over `binary-alpha signal logic v1`, the plan identity, the
base stream, and the conditions in canonical order with exact duplicates removed (a text threshold
as an escaped JSON string, so no value can spell a second condition); it excludes
contract direction, duration, and economics. The deployment-strategy identity is SHA-256 over
`binary-alpha deployment strategy v1`, the signal logic, direction, duration, currency, and the
envelope's JSON with every amount normalized, so equal money values written at different scales
share one identity. Quotes belong to events. The envelope states the maximum purchase cost, entry fee,
and each outcome's terminal fee, the minimum winning net return
(`gross_payout - quoted_cost - entry_fee - win_terminal_fee`), and the settlement rule a quote must
declare, plus optional `semantics`; a quote at equal terms passes. Broker deployment identities
include `rise_fall_strict_v1` and the settlement authority through that frozen envelope.

### Binding

Each input's `tick_manifest` binds an observation generation under the [outcome binding](#outcomes)
at the declared role; its feature manifest must be computed from that generation for the same
instrument and role. Its fitted plan carries the price scale required by that binding, and every
stream's first and last decision times lie inside the decision window; an
optional outcome manifest must label exactly those two generations. A strategy binds through its
plan identity to exactly one input; every condition names a compiled output or fitted encoding of
a plan stream with a threshold of the output's kind (an encoding compares its label text). The
feature owner's readiness of a value is resolved from the frozen plan: its readiness flags
(`is_ema{p}_ready` for the moving-average family, `tick_path_ready` for the tick-path buckets)
are read beside it, and its declared not-ready text values (`not_ready`, `unknown_warmup`,
`insufficient_tick_path`, `warming_up`, and the state outputs' `unknown`) are recorded with it;
a value whose flag is not true or that reads a not-ready value fails its condition before any
comparison. An optional outcome
manifest must label the bound tick and feature generations and the plan's raw rows. The resolved
run definition records, per instrument, the identities and only the streams and columns the
strategies read, in frozen-plan order; the adapter reads exactly those columns. Historical
source files carry no local receipts, so availability follows provider order and the definition
tags `availability = "provider_order_simulation"`.

### Decisions

Observations at one availability time apply in source order before any decision at that time:
the caller orders ticks, feature rows, proposals and external purchase, contract, cash and
reconciliation facts; an expired account pause ends first. A tick updates
the paths of every accepted obligation of its instrument in acceptance order and settles those due
under `price_at_due_v1`; an unresolved obligation is no longer driven by ticks. The engine keeps
one latest available row per stream: a row must close strictly later than the installed row, an
identical redelivery is a no-op, an older or conflicting row fails, and two rows of one stream at
one availability time fail. It evaluates each newly installed base row once, traversing installed
base streams in frozen-plan order and their bindings in configured order. A failed step ends the
engine: its state may hold observations the ledger does not, so every later step is refused and
the adapter restores a fresh engine from the ledger instead of retrying. A
condition fails when its stream has no row, when that row closes after the base row, when the
value is unavailable, or when a readiness flag of the value is not true; the engine never searches
backward. A signal is decided once per binding and base close time, and the selection and
deduplication slots of an instant are claimed by its signal records and consulted only at that
instant: a row redelivered to a restored engine, whose row state is not ledger state, is
installed but never decided again, a candidate the uninterrupted engine refused a slot is
refused it again, and a record whose disposition disagrees with the occupied slots fails. A matching signal is always a ledger
record with one disposition, decided in this order: `same_entry_duplicate` (`first` selection
per account, instrument, contract duration, and entry event; a blocked first candidate keeps the
slot), `duplicate_logic` (repeated signal logic per account, instrument, and entry event when
deduplication is enabled), `repair_blocked`, `no_quote`, `stale_feature` (decision time minus
logical close time beyond the maximum; equality passes), `stale_quote` (decision time minus the
quote's provider time beyond the maximum), `gap_at_entry` (the inter-arrival into the quote tick
exceeds the contract's maximum tick gap; a repeated tick at the same time keeps that
inter-arrival), `no_proposal` or `stale_proposal` for broker-authoritative bindings, `account_paused`, `account_blocked`, `quote_rejected`
(the envelope), `capacity_strategy`, `capacity_duration`, `capacity_instrument`,
`capacity_account`, `capacity_total` (the prospective count may equal a maximum), `insufficient_cash`,
`unresolved_loss_account`, `unresolved_loss_total`, `conversion_unavailable`, or `admitted`; the
record names the rate identities a total unresolved-loss conversion used. No signal is evaluated before `decision_start` or at or after `decision_end`; ticks and confirmations
after `decision_end` still settle existing obligations. Split labels follow the decision time and
stay with the obligation through settlement.

### Cash, reservations, and exposure

With `A = quoted_cost + entry_fee` and `F = max(0, max over outcomes of terminal_fee -
gross_return)`, an admitted signal reserves `A + F`, requires native cash minus unpaid
reservations to cover it, and prepares the command `BINDING/CLOSE_TIME_MICROS`; this is not a socket write.
Simulated acceptance debits `A`
exactly once, keeps `A` as paid basis, and reserves only `F`. The worst unresolved loss of an open
obligation is `max(0, A + max over outcomes of terminal_fee - gross_return)`; capacity counts and
unresolved exposure include every sent, acknowledged, accepted, and possibly sent obligation. An
acknowledgement records the broker's receipt and posts nothing. Acceptance, rejection, proven
not-sent, and possibly-sent transitions are permitted only from a sent or acknowledged command. A
rejected or proven not-sent command releases its reservation and capacity without debit. A
possibly sent command keeps its full reservation, blocks new entries for its account, and waits
for reconciliation; nothing is retried and no acceptance is taken for it. An account's block is
the set of commands and unmatched transactions awaiting reconciliation, with reasons for possibly
sent commands, purchased or settled discrepancies, and unmatched cash; each reconciliation removes only its own
command, and entries stay blocked while any remains. A settled discrepancy is lifted only by a
reconciliation stating the same settlement, because no corrective posting exists; a
contradicting resolution is a reconciliation failure. Settled equity is native cash plus the paid basis of open
contracts; completed profit is the credit minus the paid basis.

### Settlement

Historical observations follow the [outcome binding](#outcomes); the thresholds below belong to
the execution contract. Under `price_at_due_v1` the configured simulation accepts an admitted
command at its simulated acceptance time with the current available quote as entry price,
preserving the quote observation's provider time; the due time is the entry time plus the contract
duration. The first observed tick at or after the due time
settles when its delay is at most `max_settlement_delay_micros`; the outcome compares the
settlement price with the entry price for the contract direction, an equal price is a tie. Ticks
at or before the entry time are continuity evidence but not path or settlement evidence.
Continuity is the obligation's own evidence: the quote tick recorded at acceptance and every
tick it observed since, never ticks the instrument saw while the command was unaccepted or
before an engine was restored. A tick more than `max_tick_gap_micros` after that evidence, a
later settlement tick, or an exhausted input window leaves the obligation `unresolved` with its
reason, evidence, and path so far; it keeps its paid basis, capacity, and exposure until an authoritative
settlement or reconciliation. Every settlement credits the actual `gross_return - terminal_fee`,
releases the remaining reservation and capacity once, and records the path; a tick too late to
settle is not path evidence, and an authoritative settlement's path is the path the ledger last
recorded for the obligation (empty at acceptance, then its unresolved record; the same in a
restored engine) followed by its own price at its provider time. A tick at a time already seen
must repeat its price; another price at the same time fails. A confirmed cashflow that
contradicts the frozen terms is a `discrepancy`; a net terminal debit beyond the remaining
reservation is a `deficit`; either blocks the account until reconciliation without fabricating
the configured amount. A reconciliation resolves an open command as not sent, accepted (posting
the purchase and keeping the terminal reserve), or settled with its actual cashflow, or lifts with
zero postings the block a settled discrepancy left, and records the block that remains on the
account. Every signal, acceptance, settlement, reconciliation, and pause record is checked when it
is applied: a signal must name its binding's instrument, base stream, identities, and split, its
clocks must be ones its decision could have seen (close no later than known, known and quote no
later than the decision, the decision inside the window, and an admitted signal within both
freshness bounds with a quote), the state-based admission checks run again on an admitted signal (an account whose
drawdown has reached its pause threshold counts as paused, so an omitted pause fails at the
admission it would have blocked), an acceptance's quote time is no later than its entry and the
entry no earlier than the command's dispatch and no later than the decision, a settlement's
time is its source's provider time and no earlier than the entry, a settled reconciliation's evidence, including one lifting a settled discrepancy's block, is no earlier than the entry or, without a proved acceptance, the dispatch, every posting is recomputed from the obligation and the frozen
terms, and a pause must state the account's exact epoch drawdown at or beyond its threshold with
the deadline the policy's duration gives and must directly follow, at the same decision time, the
settlement or reconciliation that made it due, an expired pause must end before any other record at or after
its deadline, and a ledger cannot end with a pause still due or expired or, after the definition,
with a supplied rate due, so a record that
disagrees or a ledger that omits one fails at generation and at restoration alike. An external event's payload
is its transition fields and its source's provider time, availability, and simulation flag: the
same identity with the exact payload is a no-op, before and after restoration; the same identity
with another payload fails. Version-1 external identities also distinguish equal amounts at
different scales; version 2 compares evidence amounts by value and retains account-scale postings; a ledger that
applies one external identity twice, or a record whose source is available after its decision
time, fails.

### Broker-authoritative obligations

`broker_authoritative_v1` requires a proposal for the binding and `rise_fall_strict_v1` in both
contract and envelope. Templates carry zero outcome returns; proposals supply their own exact
terms. CALL/Rise wins strictly above entry and PUT/Fall strictly below entry; equality loses the
stake under the pinned subset. Loss and tie gross returns are zero, and every outcome's fees must
satisfy the envelope. The options method group implemented by `DerivOptions` prepares proposals only
for demo USD accounts and the strict `callput` CALL/PUT mapping measured in `deriv-demo-run-01`.
Other classes/currencies are rejected before a proposal request. A nonzero proposal `commission`
is unresolved fee inclusion and produces no proposal; the demonstrated cash table has no separately
charged fee.

A proposal records connection generation plus provider id, a SHA-256 of normalized broker,
configured account, instrument, currency, direction, duration, stake and semantics, exact terms,
spot/time, transport response receipt time, schema and payload digest. Serialized account and
`BROKER:PROVIDER_SYMBOL` fields let Engine validate binding scope and recompute the canonical request
hash at installation and restored admission. New proposals replace the current binding quote;
admitted signals freeze their own proposal and reservation. `max_proposal_age_micros` bounds age
from receipt locally; spot time is not a quote issuance or valid-until clock. Before dispatch,
the live runtime durably binds deployment, command, claim, fencing token, maximum proposal age,
and the admitted signal in its dispatch claim. The signal carries binding/account/instrument,
proposal/request/payload identity, economics, and reservation; the prepared maximum price is the
proposal's quoted cost. Phase 11 bundles bind settlement authority, semantic identity and all envelope bounds.
No Phase 10 application command purchases; the library refuses an empty dispatch claim, reports
pre-write failure separately, and retains written claims and possibly-sent command identities.
Changing the claim cannot permit another write for a possibly-sent command. Request ids provide
correlation, not provider idempotency.

Purchase acceptance posts the actual debit once with contract/transaction references, purchase time,
expected start and proposed payout. Entry price/time and confirmed expiry may be absent. A larger
debit remains an accepted liability with exact paid basis, exposure, deficit and an account block;
`Reconciliation { Purchased }` confirms the same evidence to lift that block without a second debit.
`confirmed` records monotonically fill entry, start and expiry: absent values never erase facts,
equal redelivery is a no-op, and changed known facts fail. Same-status terminal observations merge
previously missing exit price/time or transaction references. Consistent updates after closure do
not repost cash: terminal enrichment is retained and contract updates are idempotent no-ops against
retained purchase/confirmation facts. Nullable open-contract members are absent facts; an object
containing only `contract_id` adds no observation. Expected start remains
separate; a buy transaction's approximate `date_expiry` never confirms expiry.

Broker ticks provide continuity only and never settle or free capacity. A matched terminal
`won`/`lost` fact requires its linked exact sell cash transaction, including an explicit zero for a
loss; terminal evidence alone remains `awaiting_cash`. `sell` action and `is_sold` do not determine
outcome. Sources retain contract/payload update identity, terminal status/time, and
`deriv:transaction:TRANSACTION_ID` for cash from both stream and statement. Duplicate cash across
restoration and sources posts once. Cash and purchase uniqueness is scoped by configured account;
unmatched-cash reconciliation addresses `transaction:ACCOUNT:TRANSACTION_REF`. Unknown cash blocks
the account until matching purchase or
terminal evidence, or an explicit `External` reconciliation, resolves it. External sold/cancelled
status plus actual cash produces `Reconciled`/`ExternallyClosed` and a separate closure count,
never a directional win/loss/tie. Reconciliation cannot contradict recorded terminal status or cash:
`gross_return - terminal_fee` must equal the recorded sell amount by value. That cash is already the
net posting; no further fee is deducted from it.

Financial settlement needs no fabricated path. Confirmed entry and exit alone form authoritative
path diagnostics; missing entry/exit leaves them unavailable. Sparse exit time before expiry is
preserved. Statement windows include the complete target second by sending exclusive
`date_to = through_secs + 1`, paging by 100 while a full page is returned. Portfolio provides open
liabilities using `underlying_symbol`; its local fixture is synthetic from the pinned schema.
Statement results carry `StatementRow { cash, payout }`; `recover_purchase` uses the buy row alone,
including its payout and `transaction_time` as purchase time, without a lost acknowledgement.

The financial ledger retains admitted proposals; normalized proposals received before a signal are
re-supplied by the input replay boundary after a restart, like ticks and feature rows. Re-supplying
the proposal and continuing produces identical ledger bytes. The admitted signal is the prepared
command, and `sent_micros` records its preparation time. Provider purchase clocks carry whole
seconds, so Engine accepts a purchase no earlier than the second the command was prepared in and
no later than the decision. Phase 12 binds the durable dispatch claim before any write; the adapter
preserves the provider purchase clock unchanged.

### Pause, conversion, and projections

Completed profit and its epoch peak start at zero; the epoch drawdown is the peak minus completed
profit. After a settlement, a configured pause starts when the drawdown reaches the threshold and
ends at that decision time plus the duration; settlements continue while paused and do not extend
it; at or after the deadline the pause ends and the epoch peak resets to the current completed
profit, while the lifetime peak and maximum drawdown stay separate. Conversion selects, for a
source and reporting currency at a decision time, the latest supplied rate whose provider and
availability times are no later than the decision and whose provider age is at most
`max_rate_age_micros`, multiplies once with checked arithmetic, and rejects lost precision;
same-currency amounts only rescale. The reporting-currency projection is observed at the run
definition, at each supplied rate's own availability time (a `rate_available` record emitted
before the market observations of the step that reaches it, and at the end of the run for the
rates available by `decision_end`; an expired pause ends when a rate observation advances the
clock past its deadline; a rate record is at its availability or at the first later record time
and precedes every other record at that time, at generation and at restoration alike; so every
valuation between market observations is recorded and a rate change is visible without an
account posting), and after every record that changes an
account: an admitted signal, an acceptance, a release, a settlement, or a reconciliation. Missing or stale rates, or an aggregate the reporting scale cannot
hold, leave that observation unavailable (and missing or stale rates make the total
unresolved-loss limit unavailable, which blocks admissions that need it); native history is
never substituted and the native record the observation follows stands, while native cash, account pause, and confirmed
native settlement never depend on conversion. The path of a contract
is tracked in integer price units as `(current - entry) × direction` including the settlement tick:
final move, maximum favorable and adverse excursion with their earliest times (starting at the
entry time), first favorable and adverse times, and ordering flags that require both times in
strict order. The nonfinancial projection `move / |entry| × 10000` renders ten decimal places with
round-to-nearest, ties-to-even, and is absent with its zero-denominator reason at a zero entry
price.

### Ledger, summary, and replay generations

The ledger is one canonical compact JSON record per line in `ledger/events.jsonl`, each carrying a
contiguous `sequence` from zero, the decision `time_micros`, and a tagged `kind`: the
`run_definition` (the complete resolved run) first, then `signal`, `acknowledged`, `accepted`,
`released`, `possibly_sent`, `confirmed`, `cash_observed`, `settled`, `unresolved`, `reconciled`, `rate_available`,
`pause_started`, and `pause_ended` records
with their exact postings and provenance (`source` identity, provider and availability times, and
whether it is a configured simulation). Tick and feature data are referenced inputs, never copied.
Restoration applies every record through the same function that generated it: a missing
predecessor, an illegal transition, a posting disagreeing with its obligation, or altered bytes
fails. `summary.json` is the projection restored from the ledger: the accounts' final states, the
signal dispositions, outcome counts, open and unresolved obligations, and completed profit by
currency grouped by portfolio, binding, contract duration, instrument, and declared split (a grouped
total beyond the representable range is unavailable), and the reporting-currency settled equity,
unresolved loss, peak, maximum drawdown, and used rate identities. The final-state
identity is SHA-256 over `binary-alpha engine state v1` and the JSON of the accounts, open
obligations, capacity, and sequence; the summary identity is SHA-256 over
`binary-alpha engine summary v1` and the summary bytes.

A replay generation's identity is SHA-256 over `binary-alpha engine replay v1`, the configuration
hash, and each instrument's identity, tick generation, feature generation, plan identity, and
outcome generation, one per line. Its objects, both role `normalized` under the content-addressed
create-once rules of dataset generations, are the ledger and the summary. The ready manifest at
`manifests/GENERATION/ready.json`, published last and mirrored, records `kind` (`engine_replay`),
`schema_version` (`1` for simulated history, `2` for broker-authoritative runs), `generation`, `role`, `config_hash`, `code_revision`, `availability`,
`decision_start`, `decision_end`, `instruments`, `events`, `final_state_identity`,
`summary_identity`, and `objects`; a manifest whose `generation` is not the identity of its
`config_hash` and `instruments` is rejected. The command writes
`replay ROLE generation GENERATION instruments N events E signals S accepted A settled T unresolved U objects 2 reused R`
followed by `[load S simulate S publish S]` or `(already published)`, then the reconstruction line.
Before the manifest becomes ready it reconstructs the generation from the published objects under
the manifest bytes; an identical completed identity is reused and a conflict fails without
overwrite. `data verify` on a replay generation asserts both objects' bytes and hashes, restores
the ledger through the engine, compares the restored event count, definition, final-state identity,
and summary identity with the manifest and the restored summary bytes with the published summary,
and writes
`verified ROLE generation GENERATION events E signals S accepted A settled T unresolved U objects 2 bytes B`.
Version 2 retains proposal-bound signals, partial liabilities, confirmations, terminal/cash evidence
and external closures; version 1 rejects those records and its completed artifact bytes and identities
remain unchanged. Historical replay, search and portfolio refuse broker-authoritative inputs.
Restoring the ledger alone proves the financial state; input replay through the command proves
that signals and paths were derived from the inputs.

## Search

The engine module `search` owns candidate enumeration, family identity, the model score and its
adjustment, the stationary-block sampler, the development gates and the ranking; the application
module `search` binds the inputs, runs the lowering and chunk replays through the replay owner,
scores the family through the accelerator boundary, resamples settlement paths through the retained
bootstrap primitive, and publishes the family. `binary-alpha search --config PATH` requires
`run_mode = "research"`. The engine remains the only financial authority: device counts rank and,
under heuristic scope, prune; they never settle or rank financially. Every score is a model-based
diagnostic; nothing claims a controlled false-discovery rate, an interval guarantee, or positive
expected return.

### Configuration

The optional `search` table declares, in canonical order: `scope` (`exhaustive` replays every
member; `heuristic` requires `screen` and replays only members it keeps), `seed`, positive
`chunk_size` and `max_candidates`, `min_conditions` and `max_conditions` with
`1 <= min <= max`, non-negative `embargo_micros` at least every
contract's `duration_micros + settlement.max_settlement_delay_micros`, `base_stream`,
`development` and optional `evaluation` (each a `decision_start`, `decision_end`, exactly one
`inputs` entry as in `replay`, and optional `splits`, no evaluation split named `none`; the development input
must name its `outcome_manifest`; `evaluation.decision_start - development.decision_end` must be
at least the embargo), a nonempty `conditions` menu (a named entry has a `stream`, `output`,
`comparator` and nonempty ordered `thresholds`; a generation rule has a `stream`,
`output = "*"`, `comparator = "eq"` and no `thresholds`), the `contracts` (existing contract terms with
unique ids, no two agreeing in every field but their id, and every duration a whole number of
seconds among the bound outcome generation's expiries), the `account` template (`broker`,
`currency`, `scale`, `initial_cash`), one `risk_policy` (existing fields; `max_open_per_duration`,
`max_open_per_instrument`, `max_open_total` and `max_unresolved_loss_total` must be absent because
they span accounts), the `envelope`, the `gates` (`min_settled`, `max_unresolved`,
`min_net_profit`), the optional `screen` (`max_adjusted_score` in `[0, 1]`, optional positive
`top`), and `stability` (positive `block_length`, `simulations`, `rolling_horizon`). The
synthesized replay tables are validated by the execution rules. Omitting the table preserves every
existing configuration identity; the `accelerator` table selects the backend, `cpu` when absent.

### Stages and identities

After binding the fitted development plan, each generation rule expands in plan order to every
retained, coded label of each encoding on its stream with at least two labels. It skips unready
labels and encodings whose output name collides with a raw output. Named thresholds and generated
conditions deduplicate in menu order. The ordered resolved table and its hash are stored beside
the declared rules. Sizing uses the resolved condition count, checked binomial arithmetic and
`max_candidates` before family allocation. If fewer than `min_conditions` remain, the schema-2
family has zero enumerated and applicable members, no retained members, lowering or replay chunks;
it does not open an optional evaluation input.
Otherwise members enumerate by condition count, lexicographic condition indices and configured
contracts fastest. Each member's zero-based global index binds its `m{index}` replay ID.

A fitted-label equality projects directly when it names a distinct-name encoding and its label
is retained in the fitted plan. The projection uses the latest causally installed row per stream
after all rows installed at the same time; a latest row closing after the base close yields no
match, even if an older row matched. Readiness and the fitted code use the engine projection
shared with `holds`. All other conditions, including equalities for dropped labels, use one
development lowering replay with one unfunded single-condition strategy per fallback condition;
each `signal` marks a base row where that condition held. Lowering is absent when all conditions
project. The code matrix consists of requested projected encoding columns and lowered 0/1 columns.
Every synthesized replay carries only the schema version, run mode, storage and its role's replay
table (contracts in configured order, `max_rate_age_micros = 0`, no rates), so its generation is
independent of backend and of the other role. The base rows are the base stream's reference rows
of the bound outcome generation; for each contract duration the device rows are derived with the
outcome reader's cell: a row without an entry tick is masked out, the decision clock is the entry
tick time, `valid` is set only for `valid` cells, the outcome flags follow the cell, and the
release clock is the settlement tick time when valid and the nominal due time otherwise. The
dual kernel compares its clock arguments only, so they carry microsecond times unchanged under
their retained `_ms` names. Schema-2 slots use the base row's installation time inside the
development window and require a stored entry tick at or after installation; rows without an
entry are masked. The sparse scorer orders rows by stored entry time then row index, constructs
chronological lists for each requested `(column, code)`, and drives each candidate by its least
frequent condition while checking its whole conjunction. Screening uses the basic sparse
transition's total, directional wins, ties and invalid counts; losses are the other direction's
wins. A fused CUDA launch scores up to eight distinct expiries per tile from packed outcome rows.
The CPU reference uses the basic sparse dual scorer once per distinct expiry. A device
free-memory budget determines column blocks from resident row, sparse-list, packed-outcome,
candidate and compact-output allocations. Candidates stream by nondecreasing block tuple and
are ordered within each batch by driver and global rank; global combinatorial ranks place their
counts in the compact whole-family array. CUDA batches follow a deterministic round robin over
every configured `[accelerator] devices` entry.
Schema-1 family verification retains the identity of the original thirteen kernel sources;
schema-2 family identity includes the fused screening source as the fourteenth.
The created search report lists `columns C blocks K tuples T replans N` before the visit
counters; `replans` counts plans halved after a tuple failed to fit, and the command writes
nothing to standard error when it succeeds.
`validation_visits` counts sparse tuple-index entries once per constructed workspace: once
for CPU, or once per configured CUDA device entry, including repeated device ordinals.
Candidate-driver row visits are reported separately.
The report also records the device name, compute capability, build target, launch threads,
batch size, memory budget, allocator-unit hint, their derived or override sources, and observed
free and pool memory around tuple preallocation. These diagnostic fields are outside the
configuration hash, family generation, and published `family.json` identity.

The statistic of a member applies when `W = winning_net() >= 0`,
`L = purchase() + loss.terminal_fee - loss.gross_return > 0`, and the tie nets exactly zero,
with `p0 = L / (W + L)` from the aligned coefficients; `W = 0` or zero decisive trials gives score
one, and an inapplicable member records its reason. The score is the one-sided exact binomial
upper tail on decisive counts summed in log space away from the mode; the adjusted value is the
reverse cumulative minimum of `min(1, m * p / rank)` after sorting by score then member order
over the applicable members. Heuristic scope screens members whose adjusted value exceeds
`max_adjusted_score`, beyond the first `top` by adjusted value then order, and every inapplicable
member; screened members are never replayed.

Only retained survivors become schema-2 `Member` records, ordered by `global_index`; logic
identities are computed for those members. Survivors are replayed in canonical chunks of
`chunk_size`, one account, strategy and binding per
member. A completed chunk generation is reused only after its own verifier restores it and its
manifest records the same instruments, configuration hash and code revision. The development
group of a member is its binding's summary group (a zero group when it never signalled); profit
is the account-currency entry, zero when absent with no settlements, unavailable when the engine
recorded no total. The gates pass when `settled >= min_settled`, `unresolved <= max_unresolved`
and available profit `>= min_net_profit`; ranking is net profit descending, settled count
descending, then member order. Evaluation replays the passing members with `role = "evaluation"`
and the declared splits only after the development result is complete, and the shared ledger
projection attributes every `accepted`, `released`, `settled` and `unresolved` record to its
admitted `signal`'s binding and split (`none` outside every declared split). Stability draws
`simulations` circular stationary-block index paths per passing member and role over its
settlement profits in ledger order (sampler `stationary_block_sha256_v1`: SHA-256 over the domain
`binary-alpha search sampler v1\n`, the little-endian 64-bit seed, the stratum
`{logic identity}/{contract id}/{role}`, a zero byte and the little-endian 32-bit replicate, then a
little-endian 64-bit counter from zero per digest whose four little-endian 64-bit words are
consumed in order; a uniform draw below `m` rejects words at or above the largest multiple of
`m`; the first index is uniform, and each later step restarts uniformly exactly when a uniform
draw below `block_length` is zero, else advances circularly), evaluates them with the retained
bootstrap primitive, and reports the median and 95th-percentile maximum drawdown, the
95th-percentile longest underwater run (linear interpolation at `(simulations - 1) * q`) and the
negative rolling-window share over `simulations * (N - rolling_horizon + 1)` windows; fewer than
two settlements or a horizon beyond them is `unavailable` with its reason.

### Family generations

The family generation publishes one object, `family.json`: the declared `search` table, the plan
identity and base stream, the SHA-256 identity of the retained kernel sources, the sampler
version, the applicable count, the schema-2 resolved condition table and hash, and retained
members (global index, conditions, contract, logic identity, raw counts, null, applicability,
score, adjusted value, development group, gate reason, rank,
evaluation group, evaluation split groups, stability outcomes), and the lowering and chunk
generation references with their summary identities; pretty-printed JSON in declared field order
with one trailing newline, and identical on every backend. The ready manifest records `kind`
(`search_family`), `schema_version` (`2` for new families, `1` for legacy readers),
`generation`, `config_hash`, `code_revision`, the
ordered `inputs` (role, instrument, tick, feature, plan and outcome identities), `members`, and
`objects`. The generation is SHA-256 over `binary-alpha search family v1\n`, the configuration
hash and the code revision each followed by a newline, then one line per input
(`role instrument tick feature plan outcome`, a dash for an absent outcome) followed by a newline.
The schema-2 manifest's `members` and the report's `members` count the complete enumerated
family, including screened and inapplicable members; `Family.members.len()` counts retained
survivors. Verification re-expands the recorded rules against the recorded fitted development
plan, requires exact ordered equality with the resolved table, checks optional lowering against
only fallback conditions, rebuilds projections, and re-scores every global member through the
same sparse scorer. It recomputes applicability, exact BH adjustments, the survivor index set,
gates and ranks, then verifies each retained member's replay chunks and their definitions against
the recorded members, and compares groups and split groups with verified summaries and the ledger
projection. A configured `data verify
--config` uses its accelerator devices only for schema-2 re-scoring; without one it uses CPU
batches fanned across cores. Within a command, successful full verification is cached by URI
after the development-only role guard. Certified and ordinary contexts have separate cache keys;
a separate command re-scores. Schema-1 families retain
their full-member, full-lowering and close-time CPU verification path and remain readable.

The command writes
`search SCOPE generation GENERATION members M applicable A screened S replayed R passed P evaluated E objects 1`
followed by stage timings, resolved columns, block tuples, re-plans, sparse-list entries, sparse-list construction visits, tuple-index validation visits, candidate-driver row visits, host-to-device transfer bytes, and peak resident memory or `(already published)`, then the
verification line. `data verify` writes
`verified search generation GENERATION members M applicable A replayed R passed P objects 1 bytes B`.

## Portfolio selection

The engine module `portfolio` owns the finite enumeration of complete joint policies, the logical
and resolved form of every choice, the projection and gates of a verified restored engine, the
frozen objective, tie breaks and ranking, and the selection records; the application module
`portfolio` binds the inputs, builds the fold, refit and outer feature generations through the
feature owner, publishes every joint replay through the replay owner, and publishes and verifies
the selection. `binary-alpha portfolio optimize --config PATH` requires `run_mode = "research"`.
Selection uses development data only; the chosen policy freezes before one optional outer
evaluation. Every result compares separately funded folds; it is never a continuous equity path,
a certification, or evidence of trading profitability.

### Configuration

The optional `portfolio` table declares, in canonical order: `families` (nonempty, distinct
ready-manifest locations of development-only search families), positive `max_policies`, positive
`embargo_micros`, the `objective` (`profit_then_drawdown`: larger completed net profit then lower
drawdown; `drawdown_then_profit`: lower drawdown then larger profit), the `gates` (positive
`min_settled`, `max_unresolved`, `min_profit`, non-negative `max_drawdown`, optional
`min_decisive`, and optional `min_win_rate` in `[0, 1]`, in the reporting currency where
applicable), the shared funded `accounts`, the reporting contract (`reporting_currency`,
`reporting_scale`, `max_rate_age_micros` and optional `rates`, as in `replay`), the nonempty explicit base
universe `members` (each a `family` index, a `member` index of that family and optional
`ordinals`, each naming a `condition` index of the member and an interval `ordinal` `0` to `4`),
nonempty `repairs` (a unique `id` and a conjunction of existing conditions; an empty conjunction
is no repair), nonempty `bindings` (a unique `id`, an `account`, an `instrument` as
`BROKER:PROVIDER_SYMBOL` and nonempty `alternatives`, each one complete `contract` whose `id` is
unique across every alternative and one `envelope` that admits it), nonempty `subsets` (each a
nonempty ordered list of `deployments`, one `member`, `repair` and `binding` index each), nonempty
`risk_policies` (existing fields, unique ids), nonempty `folds` (each a `cutoff`, a
`decision_start` at least the embargo after the cutoff, a `decision_end` and nonempty `inputs`,
each one development `fit` entry in the `features.instruments` form without a frozen plan and one
`assessment_manifest` development observation generation under the [outcome binding](#outcomes)),
the `refit` (a `cutoff` and nonempty development `fits`) and the optional `evaluation` (a window at least the embargo after the refit
cutoff, nonempty `inputs` evaluation observation manifests and optional `splits`). The declared count,
computed with checked arithmetic as the sum over subsets of the product of each deployment's
alternative count, times the number of risk policies, must neither overflow nor exceed
`max_policies`; the embargo must be at least every alternative's duration plus its permitted
settlement delay; one contract identity names one contract, so identical terms may repeat under
their identity while conflicting terms may not. Accounts, every alternative, every risk policy,
the rates, the reporting contract and the first fold's window are validated by the execution rules
before any choice is enumerated. For schema-2 families, `member` names a retained global family
index; an absent or screened index fails before folds. Schema-1 families use their all-member
vector position. Omitting the table preserves every existing configuration
identity.
Standalone `portfolio optimize` always uses explicit members and subsets; it does not accept
`[portfolio.generate]`.

### Stages and identities

Every declared development input is read on its manifest bytes before any output exists: a fit
resolves through the feature owner and its last observation must be known strictly before its
cutoff under the [outcome binding](#outcomes); an assessment observation generation must carry the
development role (holdout is refused) and the fit's instrument; every binding's instrument must have one input in every fold and in the refit. The
optional evaluation inputs are not read at all until selection and refit succeed; only then are
their manifests read for role and instrument and their objects opened. Each family is read through
the typed development-only reader: every manifest input must be development before `family.json`
is opened; the family must carry no evaluation window, no lowering or chunk of another role and no
member evaluation group, split group or stability entry before any referenced generation is
followed; then the family verifies exactly as `data verify` does, whose chunk reader checks each
referenced replay manifest's own role and summary before restoring it. Nothing is stripped to make
an input acceptable.

The logical universe is the resolved members' conditions with their ordinals; an ordinal
condition compares text with `eq` or `ne`. Choices enumerate in declared order: subsets, then each
deployment's alternatives with the last deployment cycling fastest, then risk policies;
deployments keep their subset position as `d{position}`. A choice's logical form carries the plan
identity `logical:INSTRUMENT` of its deployment's instrument and renders every ordinal as the text
`interval ORDINAL`; its identity is SHA-256 over `binary-alpha portfolio choice v1\n` and the JSON
of its strategies, bindings, contracts and risk policy. Structure applies the execution rules to the logical table with the
first fold's window and inputs, which they never open: a rejected choice, such as two deployments
of one member differing only by repair under equal terms and envelope on one account, records its
reason and is never replayed.

Per fold, each instrument's fit builds a new plan on its own profile's source generation and the
assessment applies that plan through `frozen_plan` under the same profile reference, each through
the feature owner (a completed identical generation is reused). Each structurally valid choice
resolves under the fold's plans: a literal condition keeps its threshold; an ordinal condition on
a development-fifths encoding takes that plan's interval label, and a missing, collapsed or
omitted interval makes the choice inapplicable for that fold; a condition naming a
development-fifths encoding without an ordinal, or an ordinal on another output, is an error. The
resolved policy replays jointly through the replay owner with the shared accounts, the fold
window, the assessment inputs and no splits, as a synthesized configuration carrying only the
schema, run mode, storage and that table; a completed generation is reused only after its own
verifier restores it. The projection reads the verified restored engine: settlement support first
(`settled >= min_settled`, `unresolved <= max_unresolved`), then every account's native completed
profit converted by the engine at the restored ledger's final event time
(`Summary.last_time_micros`) with the replay's reporting currency, scale, rates and freshness,
summed with checked arithmetic and its rate identities retained, then the engine's reporting
drawdown, which passes only with zero unavailable reporting observations. A missing or stale rate
or an unavailable drawdown fails the choice; an arithmetic error stops the command. A choice passes
when every fold passes; its profit is the sum of the fold profits and its drawdown the largest
fold drawdown. Ranking orders passing choices by the objective, then fewer deployments, then the
canonical identity ascending, writing one-based ranks; the first is selected. No standalone
profit, score or admission flag prunes.

When either decisive gate is configured, the projection also records wins, losses, and ties.
Only wins and losses are decisive: zero decisive trades or fewer than `min_decisive` fails for
insufficient evidence, even when ties satisfy `min_settled`. With sufficient decisive trades,
`wins / (wins + losses)` below `min_win_rate` fails the economic gate using exact decimal
comparison. Without decisive gates, these counts are absent from the projection record.

Only a selected choice is refitted: each refit fit builds a new plan on the full permitted
development generation and the choice re-resolves under it; an inapplicable refit is terminal
(`refit_inapplicable`) and never chooses the next rank, and a resolved choice whose conditions or
repairs name a column the refit plan does not compile is an error. Only a refitted choice is evaluated: each
evaluation input applies its instrument's refit plan through `frozen_plan`, and one continuous
joint replay with `role = "evaluation"` and the declared splits projects and gates the frozen
choice once; splits attribute records without resetting cash, exposure or unresolved obligations.
A failing outer projection is `outer_rejected` with the frozen choice recorded unchanged. With no
passing choice the result is `no_feasible_policy` with no refit and no outer read.

### Selection generations

The selection generation publishes one object, `selection.json`: the resolved configuration,
whose content hash the manifest binds, every family (generation, plan identity, base stream and
source members with their logic identity, contract and declaring bases), the logical members, the declared, rejected,
valid and passing counts, every fold's fit and assessment generations, every choice (subset,
alternatives, risk policy, identity, structural rejection, fold results with their replay
generation and summary identity, projection and inapplicability, aggregate profit and drawdown,
failure and rank), the selected index, the refit generations, the frozen policy, the outer result
(feature generations, replay reference, projection and split groups) and the terminal `state`
(`selected`, `no_feasible_policy`, `refit_inapplicable` with its reason, `outer_rejected` with its
reason); only `selected` carries a deployable candidate, never a certification. The ready manifest
records `kind` (`portfolio_selection`), `schema_version` (`2` when any source family is schema 2,
otherwise `1`), `generation`, `config_hash`,
`code_revision`, the `families` generations, `state` and `objects`; the generation is SHA-256 over
`binary-alpha portfolio selection v1\n`, the configuration hash, the code revision and every
family generation, each followed by a newline, so extending a grid changes the identity even when
the winner is unchanged. The command writes
`portfolio generation GENERATION declared D rejected R valid V passing P state S objects 1`
followed by `[bind S folds S refit S publish S]` or `(already published)`, then the verification
line. An interruption preserves every completed replay and feature generation and publishes no
selection; the rerun reuses them and recomputes the rest. A schema-2 selection stores only
resolved source members, each keyed by its retained `global_index`; schema-1 selections retain
their all-member source records and remain readable. A generated schema-2 selection also records
the declared `generate` rule in its resolved configuration. Before folds, selection and its
verifier re-derive the complete ordered members and singleton subsets from verified development
families, ranks, fitted interval edges, bindings, and condition-free repair zero, and require exact
equality with that configuration, including an empty resolution. `data verify` on a selection checks
the recorded configuration's hash and schema against the manifest, re-reads the families through
the development-only reader, checks every resolved global source index, and re-enumerates the
choices, identities and structural rejections,
verifies every recorded fit, assessment, refit and outer feature generation through the feature
verifier and re-resolves every configured fit through the feature owner against the recorded plan
before its fit and against its cutoff, restores every recorded replay through its verifier and checks its definition against the table
rebuilt for that choice and fold, recomputes every projection, gate, aggregate, rank, the frozen
policy and its compilation under the refit plans, and the terminal state, and writes
`verified portfolio generation GENERATION declared D rejected R valid V passing P state S objects 1 bytes B`.

## Research

`binary-alpha research run --config PATH` is the one-command study: one operating-system process
calling the existing owners in a fixed order. It prepares every declared instrument (profile,
features, outcomes, family), selects one joint policy through the portfolio owner with evaluation
disabled, publishes the immutable frozen stage, claims the outer populations, assesses the frozen
policy under every scenario, publishes the run record, and exits successfully in the state
`awaiting_holdout_authorization`. The same command resumes the same identity: with the exact
operator grant, every protected population claim, and the consumption receipt it creates the
internal certification context, applies the same frozen plans and scenarios to the holdout
generations, and publishes one certified or rejected result. No fitting, ranking, scenario choice,
or later research reads a holdout observation or result. The engine module `research` owns every
record, identity, permit rule, and qualification; the application module `research` owns the
sequence, the governance effects, and the verifiers.

### Configuration

The optional `research` table declares, in canonical order: `study` (`study` and `attempt`
identifiers, `governance_manifest` as `file:///DIR/FILE.json` or `gs://BUCKET/KEY` naming the
declaration object, optional `predecessors` attempt identifiers other than the attempt itself,
and the non-empty declared `changes`); the ordered `instruments` (each `instrument` as
`BROKER:PROVIDER_SYMBOL` mapping one configured `[[instruments]]` entry, `source_manifest` (the
development family-source observation generation under the [outcome binding](#outcomes)),
`features` (exactly the optional new-plan settings of a `[[features.instruments]]` entry), `outcomes` (exactly the `outcomes` table without its role
and manifests), and `search` (the `search` table's settings with its development
`decision_start` and `decision_end` and without inputs or evaluation)); `folds` (each `cutoff`,
`decision_start`, `decision_end`, and one `{ fit_manifest, assessment_manifest }` per instrument
in instrument order); `refit` (`cutoff` and one development fit observation generation per instrument);
`evaluation` and `holdout` (each an evaluation window: `decision_start`, `decision_end`, one
observation generation per instrument, optional `splits`; holdout references are validated for syntax
and declared role only and are never opened before certification); `portfolio` (exactly the
`portfolio` table without families, folds, refit, and evaluation; a member's family index is its
instrument index; optional `[research.portfolio.generate] top = N` replaces only `members` and
`subsets` after the fitted development plans and ranks exist); optional `scenarios` (each a unique identifier `id` other than `baseline`, a
non-negative `acceptance_delay_micros`, and `alternatives` naming every portfolio binding exactly
once with an exact `contract` and `envelope`; equal contract identifiers within one scenario carry
equal terms); and `qualification` (`claim`, which must be `empirical_policy_qualification_v1`, and
`gates`, the exact `min_settled`, `max_unresolved`, `min_profit`, `max_drawdown`, and optional
`min_decisive` and `min_win_rate` every
scenario must satisfy, where `min_profit` is the minimum economically useful improvement over the
analytic zero-profit benchmark on the same initial capital). Validation lowers the declared
settings into the existing feature, outcome, search, and portfolio tables with the source
manifests standing in for unpublished generations and applies their validators, under the
evaluation window, under the holdout window, under the qualification gates, and under every
scenario's alternatives, and checks each later window's own `splits` under the execution split
rules, so no later structural rejection consumes a claim. Omitting the table preserves every
existing configuration identity; the
table follows `portfolio` and precedes `[[brokers]]` in canonical order. The command requires
`run_mode = "research"`.

Generation requires positive `top` and a condition-free `repairs[0]`; its syntax and the remaining
portfolio declarations are validated before search. After every development family verifies,
the portfolio owner takes up to `top` passing members in rank order per instrument, skipping a
member unless each development-fifths threshold identifies exactly one low-to-high interval
through the fitted edges and retained `interval_label`. Each generated member uses its schema-2
global index and the derived ordinals. Its singleton subset uses repair zero and exactly one
binding on that instrument with a sole alternative equal to the ranked contract and search
envelope; zero or multiple matching bindings fail. The fully resolved portfolio is validated
before folds. When no member is eligible, generated members and subsets are both empty, so the
portfolio enumerates zero choices and publishes `no_feasible_policy`; research publishes the
matching run without reading evaluation or holdout. Explicit portfolios still require nonempty
members and subsets. Research verification independently rebuilds the resolved configuration
from the verified families and fitted plans before any outer assessment read.

The optional `replay.scenario` descriptor, version `1`, carries `schema_version` (`1`), an
identifier `id`, and a non-negative `acceptance_delay_micros`. Every admitted command's synthetic
acceptance is scheduled at its checked decision time plus the delay, independently of later
prices, and delivered only through its own instrument's evidence horizon, the availability of that
instrument's last input tick, including equality; another instrument's later evidence cannot
extend it. Queued responses merge with input observations by availability time: same-time ticks
and rows precede the responses, which precede new decisions, in instrument and canonical command
order. A delivered response is an `accepted` record whose entry time is the response time and
whose entry price and price time are the instrument's latest causally available tick at that time;
the engine validates the clocks, derives the due time, and posts as for every acceptance. A response
beyond its horizon is never delivered: the command stays unaccepted with its reservation and
`Engine::finish` records it unresolved. Zero delay and an omitted descriptor take the existing
immediate second same-time engine step and produce identical ledger records apart from the run
definition; the descriptor is part of the replay table and therefore of every replay identity.
This is a declared synthetic delay and fill convention, not measured broker execution fidelity.

### Governance declaration and read permits

The declaration is a JSON object (`schema_version` `1`, `operator`, the authoritative `root`
store, an identifier `namespace`, and `populations`). Each population declares `id`, `role`,
`instrument`, `source`, `coverage`, its `generations` (every dataset generation identity that
carries it; a generation belongs to one population), its complete sorted non-empty `tokens`
(overlapping populations share a token), and optional `exposure` history (`study`, `attempt`,
`role`). A token declared on a holdout population is never declared on a development or
evaluation population, and no population is recorded as exposed on the other side of the protected
boundary; either is rejected when the declaration is read. Its identity is SHA-256 over
`binary-alpha governance declaration v1\n` and its canonical JSON. Every governance record lives
beneath `ROOT/NAMESPACE/`: `attempts/STUDY/ATTEMPT/intent.json`, `assessment-use/TOKEN`,
`holdout-use/TOKEN`, `grants/RESEARCH.json`, and `receipts/GRANT_HASH.json`, each created once by
the store's conditional creation and confirmed by exact readback; a different existing record is a
conflict that replaces nothing.

Every dataset ready-manifest reader obtains a permit from the shared helper before opening its
target: the feature, outcome, replay, search, portfolio, audit, fetch-cache, and verification
paths. With a declaration (the configuration's `research.study`, supplied to `data verify` with
`--config PATH`), the target generation must be declared and its declared role must be the role
the reader expects; a holdout target requires the certification context that names it, and an
undeclared target is refused. Without a declaration an ordinary reader keeps its existing
post-read role guard and holdout is refused. A ready manifest is the public reference envelope
of its generation: a reader opens it to learn the role and references it must permit, and opens
no object of a generation it may not read. A derived generation whose manifest carries the
holdout role or whose declared input generation is holdout (a stream, feature, outcome, replay,
or family of holdout data, whatever its own label says) is parsed, restored, or verified only
within the certification context naming its dataset generations. Only the
application research owner creates that context, from the matching run manifest, grant, and
receipt; no configuration flag or role relabelling grants access. The fetch cache traversal lists
the mirror through the store, reads only the declaration's generations of the fetched instrument
and role when a declaration is present, refuses a candidate location holding another
generation's manifest, and verifies the selected prior under the same declaration.

### Stages and identities

The run identity is SHA-256 over `binary-alpha research run v1\n`, the configuration hash, the
code revision, and the declaration identity, each followed by a newline; it is computed before any
work; an existing run manifest of that identity passes the complete run verifier and then
resumes from its recorded state, and an existing frozen stage of that identity restores every
child through its verifier instead of recomputing it. Before any read the run permits every
declared input in its declared role and for its declared instrument, checks every predecessor
intent exists under the same study, root, and namespace (a changed governance root fails
freshness rather than creating authority) and that every population it used keeps its side of
the protected boundary in this declaration (an exposed token never becomes protected), and
creates the intent (configuration hash, code revision, declaration,
root, namespace, predecessors, changes, and the populations used with their roles and tokens); an
attempt that already ran with another configuration is refused and must be declared as a new
attempt with the old one as predecessor. Per instrument the run audits the source generation
(reusing one profile per generation, including every fold and refit fit source), fits the
features, builds the outcomes, and searches once with evaluation disabled; the search child
retains the declared `accelerator`. It then lowers the portfolio table (families in instrument
order, every fit under its published profile, no evaluation) and selects. The frozen stage
`manifests/RESEARCH/frozen.json` (run identity, intent key, declaration identity, every
instrument's source, profile, feature, outcome, and family generations, the selection generation,
the scenario definitions, and the qualification descriptor) is published before any outer claim.
The descriptor freezes the claim, `look = "fixed_horizon_once"`, `benchmark =
"analytic_zero_profit"`, the objective, the qualification gates, the reporting currency and scale,
the initial accounts, both later-role windows, the per-instrument evidence-horizon rule, the
scenario identifiers with `baseline` first, `market_inference = "unavailable"`, and the recorded
absence of an observation-model or uncertainty justification.

A selected policy claims `assessment-use/TOKEN` for every token of the evaluation populations in
canonical order, each bound to the study, attempt, run, frozen-stage identity, declaration, and
complete token set, before any evaluation object is opened; the complete set is read back once
more. The refit plans apply to the evaluation generations through the feature owner, and each
scenario replays once through the replay owner: `baseline` under the frozen policy's own terms
with no descriptor, then each declared scenario under its delay and its alternatives, where
deployment `d{p}` takes the contract and envelope declared for the portfolio binding the selected
subset deploys at position `p`. Each replay is projected under the qualification gates through
the existing restored-engine projection and qualified: insufficient settlement support, an
unavailable conversion, or an unavailable drawdown observation is `insufficient_evidence`; any
remaining gate failure is `economic_failure`; otherwise the scenario passes. The finite set
aggregates once: any insufficient scenario makes the evidence insufficient, otherwise any economic
failure rejects, otherwise the policy passes. A failing scenario never removes a scenario or
chooses a replacement. Completed no-feasible selection, inapplicable refit, and every non-passing
outer result are terminal and open no holdout.

The run generation publishes one object, `research.json`: the resolved configuration, the
declaration identity, the intent key, the frozen-stage identity, every instrument record, the
selection generation, the descriptor carried unchanged, the claim keys, every scenario result
(the applied feature generations, the replay reference, the projection with its splits, and the
verdict), and the `state` (`no_feasible_policy`, `refit_inapplicable` with its reason,
`outer_rejected` with its verdict, or `awaiting_holdout_authorization`). In the awaiting state
this record is the DeploymentBundle the live consumer parses: it references immutable
evidence rather than duplicating it, and it is not certified or executable: the engine's
bundle-completeness check (`Run::complete_bundle`) proves the frozen stage, the version-one claim,
and one passing result per frozen scenario and authorizes nothing, and live consumption
additionally requires this run's verified `certified` certification manifest. The ready manifest
records `kind` (`research_run`), `schema_version` (`1`), `generation`, `config_hash`,
`code_revision`, `declaration`, `selection`, `state`, and `objects`; the bundle hash a grant
binds is the object's SHA-256, and the manifest is published only after the run verifier accepts
its exact bytes. The command writes every child owner's report lines, then
`research generation GENERATION scenarios N` with `[bind S development S selection S outer S
publish S] peak_rss_kb K` or `(already published)`, the verification line, and
`research generation GENERATION state STATE selection SELECTION`; timings and memory are
receipts outside every identity.

### Grant, claims, receipt, and certification

`binary-alpha holdout grant create --config PATH --bundle-manifest URI --holdout-manifest URI
--reason TEXT` loads `research.study`, validates its declaration, requires the bundle manifest to
be the run of this configuration and declaration in the awaiting state and to pass the complete
run verifier, requires one `--holdout-manifest` per instrument in instrument order equal to the
declared holdout references,
and creates `grants/RESEARCH.json` once: `schema_version`, `research`, `bundle_sha256`,
`holdout` (instrument and exact ready-manifest location), `declaration`, `root`, `namespace`, the
complete sorted protected `tokens` of the declared holdout populations, `operator` (the local
account that ran the command, from the `USER` environment variable, or `unavailable`; not an
authenticated store principal), `reason`, `created_at`, and `hash` (SHA-256 over
`binary-alpha holdout grant v1\n` and the record with an empty hash). It never opens a holdout
object; an existing grant for the same bundle and population is reported, any other existing grant
is refused, and the operator identity that creates grants must not be able to overwrite them. It
writes `holdout grant HASH research GENERATION at URI`.

On resumption the run validates the grant (run, bundle hash, holdout references, declaration,
root, namespace, tokens) before any claim; a mismatch fails before holdout resolution or any
mutation, and an absent grant leaves the run awaiting. It then claims `holdout-use/TOKEN` for
every protected token in canonical order (each bound as above plus the grant hash), reads the
complete set back, and creates `receipts/GRANT_HASH.json` (grant, run, bundle, holdout
references, claim keys, declaration). A receipt from another run means the grant is consumed;
a conflicting claim fails before any holdout read and leaves every earlier claim in place; no
claim is ever released. The certification context is created only from the matching run
manifest, grant, and receipt, and permits only the grant's holdout generations. A completed
certification of this run and grant is verified within that context and returned; otherwise the
refit plans apply to the holdout generations and every scenario replays with `role = "holdout"`
exactly as the outer assessment did (the command writes `research scenario ID replay GENERATION`
per scenario in place of the replay owner's report, so no support count leaves the evidence
objects), and the result is published after the certification verifier accepts its exact bytes:
object `certification.json`
(run, bundle hash, frozen-stage identity, grant, receipt, claims, holdout references, every
scenario result, and the verdict with its reason) and a ready manifest with `kind`
(`research_certification`), `schema_version` (`1`), `generation` (SHA-256 over
`binary-alpha research certification v1\n`, the run generation, and the grant hash), `research`,
`bundle_sha256`, `grant`, `receipt`, `state` (`certified` or `rejected`), and `objects`. The
command writes `research certification GENERATION state STATE` with `[certify S] peak_rss_kb K`
or `(already published)` and the verification line. Every terminal result consumes the attempt;
`certified` retains its meaning only with the version-one claim attached: this one frozen policy
satisfied its predeclared financial, support, and scenario rules on the named historical
populations. It is not established positive expected profit, market false-discovery control, a
posterior probability, or execution proof.

### Verification

`data verify` on a run generation checks the manifest against the record, re-lowers the recorded
research configuration, restores every child through its own verifier, and compares each with the
configuration: each instrument's profile (a development
stream of its source), feature generation (the configured fit under that profile, resolved before
its fit and compared with the recorded plan), outcome generation (the configured rule over that
source and feature, by identity), and family (whose search table, child configuration hash, and
input instrument equal the lowered search), the selection (through the selection verifier, whose
recorded configuration must equal the lowered portfolio table), the frozen stage, and, under the
declaration the run was frozen under, the intent and every claim by exact reconstruction, and,
for a selected policy, every scenario: each applied feature generation through the recorded refit,
each replay restored and checked against the exact lowered table with its descriptor, the
projection and verdict recomputed, and the aggregate state; it writes
`verified research generation GENERATION state STATE instruments N scenarios M objects 1 bytes B`.
On a certification generation without the matching context it checks the envelope (the run,
bundle hash, and, with `--config`, the grant, every protected claim by exact reconstruction, and
the receipt's consumption of that grant) without resolving a protected child and
writes `verified research certification GENERATION state STATE envelope only: protected evidence is
verified within the authorized certification run`; within the matching context it re-derives every
holdout scenario and writes
`verified research certification GENERATION state STATE scenarios M objects 1 bytes B`.

Interruption at any point preserves every completed child, claim, and record; the rerun reuses each
completed generation after its own verifier restores it, recreates nothing that exists, and reaches
the same identities. The filesystem store appends `OPERATION KEY` (`head`, `read_to`, `put_new`,
`local_path`, `list`, and `probe` of a listed manifest) to the file named by the
`BINARY_ALPHA_STORE_LOG` environment variable when it is set, as test instrumentation outside every
identity.

## Live runtime

Phase 12 runs one process per deployment bundle and execution account. The implemented boundaries
are [configuration](../crates/engine/src/config.rs), the
[pure projection](../crates/engine/src/research.rs), the
[economic comparison](../crates/engine/src/execution.rs), and the application
[journal](../crates/app/src/live/journal.rs), [control](../crates/app/src/live/control.rs), and
[authorization](../crates/app/src/live/authorization.rs) modules. The
[runtime](../crates/app/src/live.rs), [ordered ingress owner](../crates/app/src/live/owner.rs),
[I/O workers](../crates/app/src/live/workers.rs), [receipt](../crates/app/src/live/receipt.rs),
[recorded transports](../crates/app/src/broker/transport.rs), and
[command dispatch](../crates/app/src/main.rs) compose these owners. No deployment or broker proof
is implied. [Execution](#execution), [Broker access](#broker-access), and [Research](#research)
retain their existing authorities.

### Configuration

The optional `[live]` table participates in the resolved configuration hash. Omission preserves
existing configuration identities; the application `skeleton` clears it. Unknown fields are
rejected in this table and its child tables. `[live]` and the historical `[replay]` table cannot
coexist. A manifest uniform resource identifier (`URI`) uses the existing `ManifestUri` grammar:
`file:///ROOT/manifests/GENERATION/ready.json` or
`gs://BUCKET/PREFIX/manifests/GENERATION/ready.json`, where `GENERATION` is sixty-four lowercase
hexadecimal digits. Relative paths are non-empty, have no leading slash, and contain no empty,
`.` or `..` component.

| Field | Rule |
| --- | --- |
| `live.execution_contract` | Exactly `historical_baseline_to_broker_v1`. |
| `live.bundle_manifest` | Ready manifest of the awaiting research run carrying the complete DeploymentBundle. |
| `live.certification_manifest` | Ready manifest of the matching certified generation; consumption reads only the public envelope. |
| `live.broker` | Neutral broker identifier naming a `[[brokers]]` entry. |
| `live.account` | Non-empty logical account identifier; projection requires the frozen portfolio's sole account. Provider login identifiers remain inside the adapter. |
| `live.warmup` | One verified development tick generation per frozen instrument, in frozen order, with matching instrument and price scale. Its last event precedes the observation window; replayed row count and coverage must match its manifest. |
| `live.compatibility` | Required measurement table below; frozen before observations. |
| `live.compatibility.observation_start` | Inclusive start, parsed by the shared event-time parser. |
| `live.compatibility.observation_end` | Exclusive end, strictly after the start. |
| `live.compatibility.required_account_class` | `demo` or `real`; names the evidence required for promotion. |
| `live.compatibility.min_samples` | Positive integer support required for every mandatory dimension. |
| `live.journal` | Required local segment and spool settings below. |
| `live.journal.dir` | Relative path under the configuration directory. |
| `live.journal.segment_records` | Positive integer records per full segment. |
| `live.journal.max_spool_bytes` | Positive measured bound in bytes for the open segment plus closed segments awaiting upload verification. Entries are disabled when `spool_bytes() >= max_spool_bytes`. |
| `live.control` | Required encrypted database connection and lease settings below, including in recorded replay configurations; replay uses fake control. |
| `live.control.host` | Endpoint hostname; connection address and verified certificate name are the same. |
| `live.control.port` | Unsigned 16-bit endpoint port. |
| `live.control.database` | Database name. |
| `live.control.user` | Database user. |
| `live.control.credential` | Environment variable name holding the password, never its value: letters or `_` first, then letters, digits or `_`. |
| `live.control.root_certificate` | Relative path under the configuration directory to the supplied trusted root certificate in Privacy-Enhanced Mail (`PEM`) encoding. |
| `live.control.owner` | This process's cooperative lease owner identity. |
| `live.control.lease_ttl_micros` | Positive lease lifetime in microseconds. |
| `live.control.renewal_interval_micros` | Positive microseconds, strictly below the lease lifetime. |
| `live.control.safety_margin_micros` | Non-negative measured microseconds; checked interval plus margin must be strictly below the lifetime. |
| `live.replay` | Required for `research`/`replay` with `[live]`; forbidden for `paper`/`live`. |
| `live.replay.broker_log` | Relative path under the configuration directory to the [recorded broker-event log](#recorded-broker-event-log). |

The loader implements the table/mode checks. Command dispatch and runtime mode checks reject
conflicting command/mode combinations before external
mutation. Existing [broker capability checks](#broker-access) still apply.

| Command | `run_mode` | Input and execution | Publication |
| --- | --- | --- | --- |
| `live replay --config PATH` | `research` | Recorded broker frames, fake clock and control; no broker connection or broker credential resolution. | Filesystem or Google Cloud Storage. |
| `live replay --config PATH` | `replay` | Same recorded broker semantics and fake transports. | Google Cloud Storage under its own authorization; filesystem publication is rejected. |
| `live run --config PATH` | `paper` | Authorized feed and account observation; prepared intents become proven not sent, with no purchase claim or socket write. | Google Cloud Storage. |
| `live run --config PATH` | `live` | Every entry requires the exact authorization below. The current adapter permits proposals only for demo USD. | Google Cloud Storage. |
| `live authorization create …` | No run-mode argument | Operator-only creation from deployment and bundle manifests. | The deployment manifest's store root. |

`paper` and `live` require `[live]` and a broker credential reference and account class. A broker
credential reference in a replay configuration is allowed but remains unused by recorded replay.
Paper observation still requires separate feed/account authorization. Real observation cannot
prove unobserved purchases or settlements. `live replay` consumes the recorded log to exhaustion;
`live run` requests shutdown at `observation_end`, after queued rows and dispatches drain.

### Projection

`research::live_policy` returns `LivePolicy`: the derived schema-2 `Replay` table, exact baseline
terms, baseline acceptance delay `0`, refit references, and `LiveSource`. It implements these nine
rules over already verified records; application reference verification remains a separate duty.

1. `Run::complete_bundle` must pass; the run's frozen hash must equal the supplied frozen bytes,
   and the manifest state must be `awaiting_holdout_authorization`. The application must supply
   the verified selection named by the run; the pure function does not resolve that reference.
2. The public certification must have state `certified` and match both the research generation
   and bundle hash.
3. `Selection::frozen` must contain a policy. The research portfolio has exactly one account with
   the requested logical identifier and broker, and every selected binding references it.
   A second account is refused even if no binding uses it; accounts and bindings are never pruned.
4. Every selected risk policy already declares `max_proposal_age_micros`. Absence makes the
   bundle ineligible; no deployment-time default is added.
5. Every baseline contract uses `price_at_due_v1` with zero loss/tie gross return and zero loss/tie
   terminal fee. Refund on equality is incompatible with `rise_fall_strict_v1`.
6. Derive separate request templates preserving identifier, direction, duration, currency, stake,
   and settlement bounds. Set quoted cost to stake, entry fee and all placeholder outcome cashflows
   to zero, settlement to `broker_authoritative_v1`, and semantics to `rise_fall_strict_v1`.
   Clone bindings in frozen order, changing only their envelope settlement authority and semantic
   identity to match. Placeholders are request syntax, not assessed economics.
7. Require one supplied input per frozen instrument and validate the resulting `Replay` through
   the existing validator. It has development role, the configured observation window, no splits
   or scenario, the assessed account, every frozen strategy/binding/risk policy, the frozen
   reporting currency and scale, and the portfolio's conversion rates and freshness bound.
8. Retain the original baseline contracts separately in their original order, with the same
   refit references. The baseline scenario delay is zero.
9. Bind `LiveSource.policy` to `Policy::identity()`, alongside the research, bundle, frozen-stage,
   selection, and certification identities.

`live::definition` verifies the run and public certification with
ordinary access and no certification context, resolves selection/refit through their existing
owners, and uses the existing replay binder to produce `RunDefinition`. Its input tick generation
is `Frozen.instruments[i].source`; its feature generation is `Selection.refit[i].generation`.
Warm-up references do not replace these source identities. The derived definition uses the
existing [execution identity](#identities-and-records) owner and the full runtime configuration
hash, `schema_version = 2`, and `availability = "ordered_broker_receipts_v1"`. Configured instrument
definitions must equal the frozen refit definitions, including currency and price scale. Only the
Deriv options adapter is accepted. Source bundle, frozen stage, scenarios, qualification, and
certification bytes are read without mutation; no protected certification child is opened.

`ContractTerms::same_economics` implements exact checked-decimal value comparison for direction,
duration, currency, stake, quoted cost, entry fee, and every win/loss/tie gross return and terminal
fee. Equal values with different decimal scales compare equal. A different payout, even a higher
one, fails. Provider quote identity and derived settlement/semantic identity are checked
separately; [Engine envelope and liability checks](#broker-authoritative-obligations) still apply.
`Runtime::offer` withdraws the previous binding proposal before installing a replacement. It checks
exact economics, settlement, semantics, proposal identity, and canonical request identity before
reservation. A mismatch journals the complete refused proposal; a failed proposal request journals
`proposal = null` and its reason. Dispatch rechecks economics, settlement, semantics, proposal age,
and lease deadline before queuing the write to the account worker. It never widens an envelope,
takes scenario extrema, interpolates, mixes bindings from scenarios, or chooses a scenario per command.

The complete selected policy preserves fitted features, strategy order, risk, conversion policy,
reporting currency, and economic capital. Startup entry admission requires broker balance to equal
the Engine account's cash, preserving broker, currency, and scale; restart restores actual cash
and obligations from journal and broker evidence instead of resetting historical initial cash.
After startup recovery applies the recovered settlements, the runtime requests balance again.
While the balance veto is set, each settlement requests another snapshot; equality with restored
cash clears the veto. An outstanding pre-settlement read requires a subsequent snapshot.
Unsupported broker, account class, instrument, currency, or duration remains rejected by the
adapter. `empirical_policy_qualification_v1` retains its [historical meaning](#research); this
projection adds no execution fidelity or new certification claim.

### Journal

The journal owner writes one JavaScript Object Notation (`JSON`) record per newline-delimited
line. `schema_version` is not a field in each serialized record. The envelope contains `sequence`, `previous_sha256`, `time_micros`,
`deployment`, and flattened `kind` fields. Sequence starts at 1 and increases across segments.
`previous_sha256` is the lowercase SHA-256 (256-bit Secure Hash Algorithm) hash of the previous
serialized record bytes, excluding the newline; the first record uses sixty-four zeroes.

| `kind` | Payload |
| --- | --- |
| `started` | `config_hash`, `definition`, `code_revision`. |
| `ledger` | One exact `FinancialEvent` as `event`; the runtime journals every drained Engine event in order. |
| `refused` | `binding`, `proposal` (complete proposal or `null` on request failure), `reason`. |
| `due_tick` | `command`, `provider_time_micros`, `price_units`; first subsequent tick at or after confirmed expiry for that command's instrument. |
| `claimed` | `command`, `claim`, `token`; remote commit completed. |
| `written` | `command`, `claim`; recorded before queuing the socket write, not proof of a write or acceptance. |
| `lease` | `state` (`acquired`, `renewed`, `released`, `lost`) and `token`. |
| `discontinuity` | Recovery `reason`. |

`Journal::append` writes and synchronizes each line with `sync_data`. The sole open file is
`<dir>/open.jsonl`; closed files are `<first>-<last>.jsonl`, with each sequence zero-padded to
twenty digits. Rotation occurs only at `segment_records`; a partial open tail is never rotated
or uploaded and stays local. `Journal::open` checks contiguous sequence, deployment, record hashes,
complete lines, and filename ranges. It refuses malformed committed evidence. The newest open-tail record
is unanchored until a successor exists; the open segment is not lossless under host or disk loss.

Closed objects use `live/<deployment>/journal/<first>-<last>.jsonl`. The caller publishes through
the existing [artifact owner](#artifacts), verifies the object identity with `put_new` and `head`,
then calls `mark_uploaded`, which renames the local file with `.uploaded`. Only full uploaded
segments are eligible for `remove_uploaded`. `restore` walks deterministic full-segment names
from sequence 1 and restores cleaned files before `open`;
the fetch callback must verify each cloud identity. `spool_bytes` counts the open file and closed
files not marked uploaded, with checked addition. No duplicate event database is added.

The storage worker uploads one closed segment at a time; the ordered owner marks and cleans it
after byte/hash verification. Failed segments remain local and retry on the renewal cadence.
Startup restores full archived segments before opening the journal. Entry disabling at
`max_spool_bytes` preserves settlement, reconciliation, journaling, and publication retry.
Shutdown stops and drains broker workers, stops renewal, disables entries, releases the lease,
uploads and verifies full deterministic segments, then publishes the receipt and final manifest.
Only deterministic `live replay` also publishes the existing
[schema-2 ledger generation](#ledger-summary-and-replay-generations).
The partial open tail stays local. Upload failure or a checkpoint interruption prevents final
publication. Broker obligations are not synthetically settled.

### Leases and dispatch claims

`live/control.rs` owns exactly two PostgreSQL tables. `MIGRATION_SQL` is an idempotent Structured
Query Language (`SQL`) migration: `CREATE TABLE IF NOT EXISTS` and the table comment
`binary-alpha live control schema v1` on both tables. No schema-version table is added. Google
Cloud Storage remains the immutable financial history owner.

| `live_leases` column | Type and rule |
| --- | --- |
| `broker`, `account` | `text`; composite primary key, neutral broker and logical account. |
| `owner`, `deployment` | Non-null `text`; cooperative owner identity and deployment hash. |
| `token` | Non-null `bigint`; monotonically increasing fencing token. |
| `acquired_at`, `expires_at`, `updated_at` | Non-null `timestamptz`; database acquisition, expiry, and update times. |

| `live_dispatch_claims` column | Type and rule |
| --- | --- |
| `broker`, `account`, `command` | `text`; composite primary key using the existing neutral command identity. |
| `claim`, `proposal`, `request` | Non-null `text`; dispatch claim and proposal/request correlation identities. |
| `deployment` | Non-null `text`; originating deployment hash. |
| `token` | Non-null `bigint`; originating fencing token. |
| `payload` | Non-null `jsonb`; serialized `Claim`, described below. |
| `state` | Non-null `text`; the Rust reader accepts `claimed`, `not_sent`, `possibly_sent`, `accepted`, `rejected`, or `reconciled`. The migration adds no SQL state constraint. |
| `contract_ref`, `transaction_ref` | Nullable `text`; authoritative identities when known. |
| `created_at`, `updated_at` | Non-null `timestamptz`, default `clock_timestamp()`. |

`Claim` carries `command`, `claim`, `deployment`, `token`, `max_proposal_age_micros`, the exact
admitted `signal: FinancialEvent`, `state`, `contract_ref`, and `transaction_ref`. The signal
preserves binding, instrument, proposal identity and request, complete economic/semantic terms,
spot and receipt clocks, and reservation; it is the existing recovery format, not a second terms
model. Insertion checks admitted disposition, command, account, broker/instrument, reservation,
and lease deployment/token. State and known references are read from their current columns over
the original payload when recovering a row.

Acquire, renew, release, claim, and claim update each run in one transaction, serializing on the
same lease row through `LOCK_SQL`, ending in `FOR UPDATE`. After that statement returns,
`CLOCK_SQL` executes `SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint`.
Expiry comparisons use this post-lock signed Unix-epoch microsecond value.

| Transaction | Effect |
| --- | --- |
| Acquire | Absent row: insert token 1, with a unique conflict returning no lease. Expired row: replace owner/deployment, increment token, and set acquisition, expiry, and update times. An unexpired row returns no lease. |
| Renew | Require matching owner/token and expiry strictly after current database time. Set expiry to current time plus lifetime and update time; preserve the token. A renewal behind release cannot revive it. |
| Release | Require matching owner/token; set expiry and update time to current database time. Preserve the row and token. Mismatch returns false. |
| Claim | Require current owner/token and an unexpired lease, validate the claim binding, insert, and commit before purchase write. Unique conflict returns the existing claim as `Replay`; lease failure returns `LeaseLost`. |
| Update claim | Require current unexpired owner/token; update state and supplied known references only when the row's originating token is no greater than the current token. The current owner can reconcile an older claim; a stale owner cannot mutate it. |

Errors await rollback before returning. `unresolved` returns every state except `reconciled`,
ordered by creation time then command. `delete_reconciled` is idempotent and deletes only a row
already in that state; it does not itself verify cloud archival. `Runtime::finish` deletes a
reconciled row only after its complete journal lifecycle lies in verified full segments, before
publishing the ledger, receipt, and final manifest. A lifecycle in the open tail retains its claim
until that segment fills and verifies. Unresolved claims are never removed; interruption around
deletion resumes from the verified segments.

`Postgres::connect` uses the configured trusted roots and hostname verification with encrypted
connections required, through `tokio-postgres-rustls::MakeRustlsConnect::new`. Trust is local to
that connection; no process-wide override is installed. Use Supabase's direct endpoint or its
documented session pooler when the deployment network needs it; transaction pooling, Supabase
Storage, Supabase Realtime, and additional control tables are outside this boundary. `FakeControl`
shares in-memory rows and monotonic simulated database time between clones; scripted partition
and lost-response faults support deterministic replay, not database proof.

`lease_deadline` maps local send time `t0` and receive time `t1` from a successful acquire/renew
response. Production uses an `Instant` anchor; recorded replay uses the shared replay clock:

```text
deadline_local = t0 + (expires_at_micros - server_now_micros)
                - (t1 - t0) - safety_margin_micros
```

Production renewal runs on a dedicated control connection and returns typed lease ingress.
Failed renewal disables entries; missing results reach the conservative local deadline. Recorded
replay renews fake control on the ordered owner. Once shutdown release begins, entries remain
disabled even if its response is lost. The next acquisition increments the token. Lease vetoes
preserve account observation, settlement, reconciliation, journal commitment, and cloud retry;
a renewal worker that exits after connection loss disables entries and preserves observation.
Only a renewal worker panic is fatal.

The broker adapter already splits rate admission/encoding (`prepare_purchase`) from write
(`write_purchase`) with an opaque prepared-command token. Dispatch finishes
rate admission, checks proposal age and lease deadline, commits the matching remote claim,
then rechecks proposal age, prepared terms, and lease deadline before queuing the write.
Prepared command, immutable bundle baseline, and remote
claim must agree before write. Before claim commit no purchase write is allowed;
loss can release only that non-external reservation and restart records a discontinuity. At or
after commit, host loss is possibly sent until authoritative broker reconciliation proves the
outcome. Proven pre-write expiry releases through the Engine; uncertain write is never retried.
On a lost claim response, the running owner enters reconciliation and uses its own proof that it
queued no write to resolve not sent. That proof does not transfer to a successor after host loss.
An empty broker snapshot cannot prove unsent while the old dispatcher can resume. A process pause
or queue delay after the last check may still permit a late write; retain the claim and block
successor entries until dispatch uncertainty is resolved. Resolution requires broker evidence,
this instance's own no-write proof, or an operator update of the durable row after confirming the
predecessor cannot write: `not_sent`, or `accepted` with broker contract and transaction references.
`Runtime::refresh_claims` consumes that update on the reconciliation cadence; accepted references
still require matching broker purchase evidence. Row-lock order decides whether a claim commits before
release or fails after it. Deriv does not enforce the fencing token; the
[operator handoff](operations.md#live-runtime) excludes a non-cooperative legacy process.

### Authorization

`Authorization` is an immutable object with `schema_version = 1`, `deployment`, `configuration`,
`bundle_sha256`, `broker`, `account`, `operator`, `reason`, and `hash`. Every binding must match;
operator and reason must be non-empty. The content hash covers every field except `hash`, under
`binary-alpha live authorization v1\n`. The deterministic key is
`live/authorizations/<digest>.json`, where `digest` hashes that same domain plus deployment bytes.
No timestamp enters the object. `create` uses the common `put_new` owner, with
generation-match-zero in Google Cloud Storage and exact readback: identical creation reuses the
object, while changed content at that key conflicts. `read` checks schema, deployment, and content
integrity; the ordered ingress owner additionally validates all expected running bindings.

The operator-only command:

```text
binary-alpha live authorization create --deployment-manifest URI --bundle-manifest URI --broker ID --account ID --reason TEXT
```

The command verifies deployment kind/schema/hash/key, the public research run, bundle hash,
research generation, and broker/account agreement. It records local `USER` as operator, using
`unavailable` when the variable is absent, and stages temporary publication files through
`Store::filesystem(std::env::temp_dir())`, using the existing storage owner. Creation uses the deployment manifest's store root. Configure a distinct
operator Google identity to create but not overwrite authorizations and the runtime identity to
read them. Every `live` entry, including demo, checks deployment, resolved configuration, certified
bundle hash, broker, and account. Replay and paper do not require this object. Missing, unreadable,
or mismatched authorization leaves the process observation-only and is rechecked on the renewal
cadence. Lost creation response is resolved by reading and validating the existing object.
A changed binding requires a new exact
authorization, not a mutable permission table or time gate. A compatibility receipt is not an
authorization to connect or purchase.

### Deployment manifest

`DeploymentManifest` has `kind = "live_deployment"`, `schema_version = 1`,
`execution_contract`, `research`, `bundle_sha256`,
`frozen`, `selection`, `policy`, `certification`, `definition`, `config_hash`, `code_revision`,
`broker`, `account`, and `hash`. `definition` is the existing `replay_generation_id` of the derived
definition. `hash` covers every other field under `binary-alpha live deployment v1\n`.
`live run`/`live replay` publish `live/deployments/<hash>.json` after warm-up validation and before
journal restoration and lease acquisition through `put_new`, reusing identical content.
`--deployment-manifest URI` names this object. It binds source
qualification and policy references without creating another bundle or changing certification.

### Compatibility receipt

`live::receipt::compute` reads journaled facts and no clock. `RECEIPT_SCHEMA_VERSION = 1`.
The serialized fields are `schema_version`, `deployment`, `definition`, `bundle_sha256`,
`research`, `frozen`, `certification`, `execution_contract`, `broker`, `account_class`,
`required_account_class`, `instruments`, `contracts`, `currency`, `observation`, `scenarios`,
`clock_basis`, `min_samples`, `ledger`, `dimensions`, and `promotion`. `observation` contains
`decision_start` and `decision_end`; records are selected by their application `time_micros` in
that half-open window. `scenarios` contains frozen scenario identities followed by hashes of
selected binding envelopes and configured alternative envelopes. `ledger` is the frozen definition
identity. Journal segments and runtime measurements belong to the final manifest below.
Publication is create-once at `live/<deployment>/receipts/<content-sha256>.json`.

Each dimension carries `name`, `status`, `samples`, `required`, `bound`, and nullable `reason`.
The three serialized statuses are `matched`, `outside_envelope`, and `unavailable`. `reason` is
a string or `null`; accumulated details are separated by `; `. Missing facts produce `unavailable`
unless an outside-envelope fact already takes precedence. An otherwise matched dimension below
`min_samples` is `unavailable` with reason `<samples> of <required> required samples`.
All six dimensions are mandatory, in this order:

| Dimension | Evidence, bound, and failure |
| --- | --- |
| `economics_scope` | Samples count accepted liabilities and reconciled purchases. `bound` lists baseline contract identifiers separated by commas. Different proposal economics, accepted/settled discrepancy or deficit, different terminal return/fee, or external closure is outside the envelope. Missing signal or settlement baseline is unavailable. |
| `offer_availability_rejection` | Samples count admitted signals and refused offers. Bound: `every admitted command accepted at the assessed terms, no rejections`. A refused offer or rejected release is outside the envelope; zero samples are unavailable with `no rejection-model evidence`. |
| `quote_age_entry_price` | Samples count confirmed entry prices. Each must equal the signal quote price; decision minus quote time must be non-negative and at most that binding's `max_quote_age_micros`. `bound` lists each risk identifier, quote-age limit, and `entry_price_units=signal.quote_price_units`. Missing entry/quote facts are unavailable. |
| `acceptance_delay` | Samples count accepted liabilities and reconciled purchases with a signal. Round decision time down to whole seconds; purchase time minus that value must be zero. Bound: `0 microseconds (whole-second resolution)`. Missing purchase time is unavailable. |
| `contract_timing` | Samples count commands with confirmed entry and expiry. Bound: `expiry=entry_time+duration; exit_time=expiry; start=entry_time; exit_price=first_due_tick_price`. All four equalities and due-tick time at or after expiry must hold. Missing entry, start, expiry, terminal exit price/time, or due tick is unavailable. |
| `funds_release` | Samples require confirmed expiry, terminal and matching cash availability, and Engine release. Bound: `expiry_to_evidence=0; evidence_to_application=0; total=0 microseconds`. Nonzero component intervals are outside the envelope; missing evidence or arithmetic overflow is unavailable. Per-command intervals are serialized in `reason`, including matched zero intervals. |

Bounds come from the frozen assessment, not tolerances chosen after observation. Its baseline has
one complete exact contract map, immediate synthetic acceptance, and historical settlement on the
first eligible tick at or after entry plus duration. A stress scenario's fixed delay is not a
certified interval, and scenarios cannot be combined per command. The runtime journals the first
subsequent instrument tick at or after confirmed expiry as `due_tick`. Timing compares its price
with the terminal exit price, start with entry, and exit time with expiry.

`CLOCK_BASIS` is `unix_epoch_micros; provider seconds × 1_000_000`. Preserve provider
timestamps, transport receipt/availability, decision, and Engine application time separately as in
[Time](#time). For funds release, evidence availability is the maximum of terminal-source
availability and the matching `CashObserved` source availability; application is the
`Settled`/`Reconciled` financial record's `time_micros`. `expiry_to_evidence`,
`evidence_to_application`, and `total` are reason-text measurements, not separate JSON fields.
Zero credit still needs proved reconciliation.
Duplicate or reordered facts cannot release before the last required fact or release twice.
The implemented `AccountEvent::Cash`, purchase outcomes, and statement rows carry transport
receipt time through normalization; dequeue time does not replace it. Runtime dispatch delays
are recorded separately in the final manifest.

Promotion is eligible only when every dimension is `matched`, every sample count meets the frozen
`min_samples`, and actual account class equals `required_account_class`. `promotion` contains
`eligible` and `reasons`; reasons list failing dimension names followed by `account_class` on
mismatch. A matched dimension below minimum support still vetoes promotion.
Demo and real evidence stay distinct. Without confirmed entry and purchase facts, quote/entry,
fill, timing, and settlement dimensions remain unavailable. A passing synthetic fixture
proves the contract path and still grants no real entry authority.

A nonpassing mandatory dimension vetoes promotion. During observation, the first dimension in
fixed order with status `outside_envelope` adds a persistent entry veto; `unavailable` or low
support alone does not disable runtime entries. Observation, settlement, and reconciliation
continue. A receipt cannot widen terms, change stake/risk/features, drop scenarios, amend
certification, change the horizon, rerun holdout, or authorize another observation. A modeling
mismatch needs separately reviewed development/protocol work and fresh eligible confirmation
under the existing [holdout rules](#holdout). Retained Deriv timing and changed-payout fixtures
are nonpassing evidence, never successful execution-fidelity evidence.

### Recorded broker-event log

`RecordedConnector`, `RecordedHttp`, and `ReplayClock` in `broker/transport.rs` feed the same
normalizers and broker-authoritative Engine path as `live run`. Each JSON line is a received frame
or optional expected send, in receipt order:

```json
{"session":"market","at":1000000,"frame":"<received text>"}
{"session":"account","expect":"<text sent>"}
{"session":"bootstrap","at":1000001,"frame":"<response text>"}
```

`session` is `market`, `account`, or `bootstrap`. A frame requires `at`; an expectation may also
carry `at`. All supplied timestamps must be nondecreasing, including equal-time lines in file
order. `ReplayClock` advances when the head record is consumed; a worker retains a received frame
until its ordered result is delivered. An `expect` line requires an exact byte match after
top-level `req_id` correlation. Correlation preserves decimal tokens and nested `echo_req`.
Bootstrap expectations are `GET <url>` or `POST <url>`; headers are ignored. Without expectations,
outbound requests bind response identifiers and subscription scope from the retained frames.
Mismatches, decoding failures, and stalled logs fail before final publication. Replay uses fake
control and no broker credential resolver or network transport. Restart validates the recorded
`ledger`, `refused`, and `due_tick` prefix and reproduces the financial ledger and receipt; journal
segments and final manifests can differ.

### Final manifest and command output

`FinalManifest` contains `deployment`, `definition` (the frozen run definition identity),
`ledger` (optional ready-manifest URI), `ledger_generation` (optional generation identity),
`receipt` (object URI), `journal_segments`, `open_tail`, and `measurements`. Only verified full
segments are listed, each with `key`, `sha256`, and `bytes`. `open_tail` is `null` or the local
tail's `first_sequence`, `last_sequence`, `sha256`, and `bytes`; it names no uploaded object.
`measurements` maps command identifiers to `market_event_to_decision_micros`,
`claim_to_socket_write_micros`, and `decision_to_acceptance_micros`; absent measurements are `null`.
These measure market transport receipt to decision, claim completion to the worker's write-call
start, and decision to the accepted write-call return. The final manifest is create-once at
`live/<deployment>/final/<content-sha256>.json`; its hash includes segments and measurements.

For `live run` in paper/live mode, the verified journal is the financial record. Finish publishes
no `engine_replay` generation: `ledger` and `ledger_generation` are `null`, and the final manifest
binds the verified full journal segments, local open-tail range and SHA-256, receipt URI, and
definition identity. A continued run extends the journal and publishes a new final manifest
without changing retained publications. Deterministic one-shot `live replay` still publishes
through `replay::publish_ledger`; identical content reuses the generation key. The receipt's
`ledger` field is the definition identity in both modes.

Successful runtime commands print:

```text
live MODE deployment HASH receipt URI eligible BOOL
live final manifest URI
```

`MODE` is `replay`, `paper`, or `live`; `BOOL` is `true` or `false`. Authorization creation prints
`live authorization HASH at URI`.

### Health

The owner writes `<journal dir>/health.json` at startup, after state changes observed by its
periodic pass, on the renewal cadence, and at finish or checkpoint interruption. Serialized fields:

| Fields | Value |
| --- | --- |
| `bundle_sha256`, `broker`, `account`, `account_class` | Bundle and logical account bindings; class is `demo` or `real`. |
| `lease_owner`, `fencing_token`, `lease_deadline_micros` | Cooperative owner, token, and conservative local deadline. |
| `connection_generation`, `receipt_sequence` | Last accepted market generation and receipt sequence. |
| `last_event_age_micros` | Clock minus last provider event time, updated on renewal cadence; `null` before an event. |
| `warmup` | A ready base row has been observed for every bound instrument since startup or break. |
| `journal_sequence` | Last appended journal sequence. |
| `open_commands`, `uncertain_commands` | Engine open count; unresolved financial count plus claimed/possibly-sent rows not already counted as possibly sent. |
| `cloud_pending_segments`, `cloud_failed_segments` | Closed segments awaiting verified upload; segments with upload errors. |
| `pending_rows`, `pending_proposals` | Queued base rows and outstanding binding proposal requests. |
| `balance_reconciled` | Latest returned broker balance equals Engine cash. |
| `entries` | `{"state":"enabled"}` or `{"state":"disabled","reason":"..."}`; veto reasons are sorted and joined by `; `. |
| `risk` | Current Engine `AccountState`: `id`, `currency`, `scale`, `cash`, `reserved`, `paid_basis`, `unresolved_loss`, `completed_profit`, `epoch_peak`, `lifetime_peak`, `max_drawdown`, `open`, optional `paused_until_micros`, and non-empty `blocked`. |

`entries` reports runtime vetoes; `risk` carries Engine admission state separately.

Startup verifies deployment/configuration and journal `started` bindings. Paper/live restore the
Engine and claim-backed signals, rebuild causal features from warm-up, record restart discontinuity,
and request broker reconciliation. Unclaimed, unwritten reservations release as not sent;
journaled dispatches without control rows remain uncertain. Ambiguous predecessor claims stay
possibly sent until matched broker evidence or an operator-proven `not_sent` row resolves them;
an empty statement/portfolio or elapsed time never supplies that proof. Live ticks are not journaled.
Incomplete warm-up, continuity loss, balance divergence, unresolved dispatch, liability
discrepancy/deficit, spool bound, lease loss, missing live authorization, or outside-envelope
compatibility disables entries. Engine risk and quote/proposal freshness checks also govern
admission; stale queued rows are pruned. Accepted purchases without entry/due facts remain
paid open exposure under [broker-authoritative obligations](#broker-authoritative-obligations).
Account observation, settlement, reconciliation, journal, and cloud retry remain active; a market
tick never settles a broker-authoritative obligation. No new monitoring service or Sentry
integration is introduced.
