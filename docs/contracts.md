# System contracts

These contracts bind every phase of Binary Alpha. The current checkout implements configuration,
historical datasets, causal instrument streams, features and outcomes, the shared execution engine,
NVIDIA CUDA kernels, candidate search and portfolio selection, and the Phase 10 broker adapters,
history acquisition and non-purchasing inspection. It has no live, paper, or production execution
runtime. Specification intent, checkout
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
bar-only source cannot satisfy a request that needs a tick path, a tick count, an entry tick, or tick
settlement. An instrument is a neutral typed identifier bound to a broker and a provider symbol,
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

Artifacts are immutable and identified by content. Google Cloud Storage owns bulk data and
artifacts; Supabase stores references and proved transactional or metadata needs, never duplicate
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

The configuration document is TOML. The engine package owns its meaning, validation, canonical form,
and content hash; the application package owns reading it from a path.

### Schema version 1

| Field | Type | Accepted values |
| --- | --- | --- |
| `schema_version` | integer | `1` |
| `run_mode` | string | `research`, `replay`, `paper`, `live` |
| `storage.historical_data_dir` | string | a non-empty path of the retained historical-data folder; a relative path resolves against the configuration file's directory |
| `storage.publication_uri` | string | `gs://BUCKET` or `gs://BUCKET/PREFIX` in every run mode; `file:///ABSOLUTE/DIR` only with `run_mode = "research"`, as the non-live test boundary |
| `import.sources` | array of tables | optional; consumed only by `data import`, which requires at least one entry |
| `instruments` | array of tables | optional; maps audit generations and selected broker history/live instruments |
| `features.instruments` | array of tables | optional; consumed only by `features build`, which requires at least one entry |
| `outcomes` | table | optional; consumed only by `outcomes build`, which requires it |
| `replay` | table | optional; historical simulation through `replay`, with exact contracts, envelopes and risk policies described under [Execution](#execution) |
| `accelerator.backend` | string | optional section; explicit offline backend `cpu` or `cuda` |
| `search` | table | optional; consumed only by `search`, which requires it |
| `portfolio` | table | optional; consumed only by `portfolio optimize`, which requires it |
| `brokers` | array of tables | optional; unique broker ids and compiled `deriv` or `pocket_option` connection settings |
| `history` | table | optional; required by `data fetch` and `broker inspect` |
| `inspect` | table | optional; required by `broker inspect` |

The optional broker tables follow `portfolio` in canonical order: `[[brokers]]`, `[history]`,
then `[inspect]`. All reject unknown fields and are omitted when absent, preserving existing
configuration hashes. Every broker entry starts with `kind` and then `id`. A Deriv entry then
contains `public_endpoint`, `bootstrap_endpoint`, `app_id`, optional `credential`, optional
`account_class` (`demo` or `real`, required with a credential), and optional `budgets`.
`app_id` is a non-secret application identifier sent as `Deriv-App-ID` during bootstrap.
`budgets` contains `trade`, `account`, `portfolio`, and `other`, each with positive `per_minute`
and `per_hour` no greater than the limits in [Broker access](#broker-access); absence uses those
limits. A Pocket Option entry instead contains `endpoint`, optional `origin`, required
`credential`, `account_class` (`demo` or `real`), and `server_offset_minutes` (no default).
Credentials are environment-variable names, never their values.

`history` declares `broker`, a nonempty unique list of provider-symbol `instruments`, `role`
(`development` or `evaluation`; `holdout` is rejected), `start`, `end`, and optional positive
`refresh_interval_seconds`. Start and end are universal-time text forming a nonempty half-open
range. Every selection must resolve to a declared broker and a matching `[[instruments]]` entry
with tick native granularity. `inspect` declares positive `live_observations` and `live_seconds`,
then optional `proposal = { stake = "10", duration_seconds = 15 }` with positive exact stake and
duration. Proposal inspection requires the history broker to support execution and have a
credential reference. Capability checks run during application configuration loading, before
connection. `ws://` and `http://` endpoints are permitted only under `run_mode = "research"`;
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
manifest inside that root) and an optional `provenance` list of further files inside that root.
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

Live runtime settings and terminal certification grants remain deferred to their owning phases.
Broker connections, funded accounts, features, contract terms, research splits, objectives, and
risk policies already have implemented owners. New fields must retain neutral typed identifiers,
explicit durations and clocks, exact currency-bearing amounts, credential references, and rejection
of unsupported combinations; none may relax causality, holdout isolation, or financial invariants.

### Run modes

A run mode selects capabilities and input or output, never semantics. `research` runs development,
evaluation, optimization, and, under a separate operator grant, locked-holdout certification;
`replay` drives the live runtime from a recorded event log without broker mutation; `paper` runs the
live path without real orders; `live` places real orders under its own authorization. The current
checkout implements offline research, historical replay, bounded broker history downloads, live
market subscriptions and non-purchasing inspection. It provides no live, paper, production execution,
or locked-holdout certification entry point.

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
and is the sole publication record; there is no other catalog or authority. Object paths, broker, and
provider symbol contain no ASCII control character.

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

A collection manifest lists assets; each asset root lies inside the collection root and holds
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
other kind is rejected.

`data import`, `data audit`, and `data verify` exit with status 0 on success and, on any failure,
write nothing further to standard output, write one diagnostic to standard error, and exit with
status 1. None removes source files, retained objects, or published objects.

## Broker access

`crates/app/src/broker` owns synchronous market-data and options method groups over the shared
WebSocket transport. Engine records remain neutral; only app wire readers know provider fields.

| Adapter | History | Live market data | Options execution contract |
| --- | --- | --- | --- |
| `deriv` | raw ticks | ticks, acknowledged cancellation | proposals, claimed purchases, account transactions, contract facts, portfolio and statement |
| `pocket_option` | raw ticks | streams, cancellation sent without acknowledgement | unsupported |

The Deriv public connection supplies discovery, tick history and subscriptions. Authenticated
connections first GET `{bootstrap_endpoint}/accounts` with a resolved bearer credential and
`Deriv-App-ID`, select the single active account of the declared class, then POST
`{bootstrap_endpoint}/accounts/{account_id}/otp` without a body and connect directly to the returned
address. Its path must be `/trading/v1/options/ws/{account_class}`. The adapter checks account and
currency on balance, and currency on account events; provider login ids and authenticated addresses
stay private. Numeric fields are decoded from original bytes with `RawValue` and the shared exact
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

Live records retain provider event time, local receipt time, source/parser identity, connection
generation, receipt sequence and payload SHA-256. Neither pinned provider has a durable tick
sequence; the receipt sequence detects internal loss/reordering but proves no provider completeness.
Every explicit reconnect starts a new generation and a continuity break, requiring resubscription
and causal warm-up rebuilt from verified history before entries resume. Deriv epochs are seconds.
Pocket Option fractional provider seconds convert exactly to microseconds after subtracting the
configured offset times 60 seconds; request anchors convert back to the provider clock. That offset,
account class, endpoint and mapping bind history source identity. The observed 120 minutes is not a
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
freezes its requested end, pages backward, validates chronological rows, applies exact local
`[start,end)` bounds and removes only identical page-boundary overlap. Within-page repeats remain
source observations. Repairing a shortfall preserves the verified suffix: every overlapping row,
including multiplicity, must agree, and a missing verified row or changed price stops publication.
Deriv requests 100 tick rows with its seconds anchor. Pocket `changeSymbol` requests period 1;
`loadHistoryPeriod` uses the earliest provider-clock token, index, offset 200 and period 1, while
matching the observed period-0 reply by asset and index. Empty or non-progressing pages report an
unresolved prefix, never historical exhaustion. An initially empty pass retains raw evidence and
coverage but cannot publish a dataset manifest requiring actual first/last events.

The shared import publication owner retains and publishes immutable `broker_history` generations:
raw response objects, `normalized/ticks.parquet`, and `provenance/coverage.json`. The coverage
record's version 1 separates requested range, verified range, actual first/last times, row count,
page hashes/anchors and shortfall. The dataset ready manifest remains version 1; the new source kind
requires tick capability and those objects. Only ready publication advances verified progress.
Interrupted objects remain reusable; restarting repairs an unresolved prefix before extending the
suffix. A refresh interval completes the initial range, then waits between sequential passes whose
new end is sampled once. Unchanged content/coverage reuses the generation and ensures its objects
and manifest exist at the current destination, without refetching a completed range. No scheduler
or service is introduced.

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
stay beside their codes. The fitted plan records the fit windows (rows and first and last
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
Phase 02 tick generation; `feature_manifest`, the ready manifest of the Phase 04 feature
generation computed from that tick generation; `expiry_seconds`, a non-empty sorted unique list
of positive seconds (the reference's 30 through 300 seconds is a fixture choice, never a limit);
the non-negative millisecond thresholds `max_entry_delay_ms`, `max_settlement_delay_ms`,
`max_tick_gap_ms`, and `true_jump_max_gap_ms`; `true_jump_basis_points`, positive decimal text
such as `"5"` or `"2.5"` with at most eighteen fraction digits, parsed through the exact price
boundary and compared exactly; and the positive `frozen_min_ticks` and `frozen_min_ms`. A
millisecond threshold that overflows microseconds is rejected. Manifest locations use the
`manifests/GENERATION/ready.json` grammar of `data verify`. Omitting the table preserves every
existing configuration identity. The build requires `run_mode = "research"`.

### Binding

The tick manifest must be a dataset ready manifest whose generation provides ticks in integer
price units at microsecond event times and carries the declared role; a holdout generation and
a bar generation are refused on the manifest bytes alone. The feature manifest must be a feature
generation whose `input_generation` is the tick generation; its plan is read and checked as a
frozen plan, and every stream's rows table must carry the plan's frozen `raw_identity` in its
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
reporting-currency units per source unit). Every contract amount, loss limit, and pause threshold
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

Each input's tick manifest must be a dataset ready manifest providing ticks in integer price
units at the declared role; its feature manifest must be a feature generation computed from that
tick generation for the same instrument and role whose fitted plan carries the ticks' price scale
and whose every stream's first and last decision times lie inside the decision window; an
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

Under `price_at_due_v1` the configured simulation accepts an admitted command at the decision time
with the current available quote as entry price, preserving the quote tick's provider time; the due
time is the entry time plus the contract duration. The first observed tick at or after the due time
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
reservation is a `deficit`; either blocks the account pending reconciliation without fabricating
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
satisfy the envelope. Deriv proposals carry zero separately charged fees in this measured mapping;
no commission or universal fee-inclusion rule is inferred for other accounts or contract types.

A proposal records connection generation plus provider id, a SHA-256 of normalized broker,
configured account, instrument, currency, direction, duration, stake and semantics, exact terms,
spot/time, receipt time, schema and payload digest. New proposals replace the current binding quote;
admitted signals freeze their own proposal and reservation. `max_proposal_age_micros` bounds age
from receipt locally; spot time is not a quote issuance or valid-until clock. Before dispatch,
Phase 12 must durably bind deployment and command identity, binding/account/instrument, full
proposal/request/payload identity and economics, maximum purchase price and reservation in its
claim. Phase 11 bundles bind settlement authority, semantic identity and all envelope bounds.
No Phase 10 application command purchases; the library refuses an empty dispatch claim, reports
pre-write failure separately, and retains every written claim so an uncertain submission cannot be
retried. Request ids provide correlation, not provider idempotency.

Purchase acceptance posts the actual debit once with contract/transaction references, purchase time,
expected start and proposed payout. Entry price/time and confirmed expiry may be absent. A larger
debit remains an accepted liability with exact paid basis, exposure, deficit and an account block;
`Reconciliation { Purchased }` confirms the same evidence to lift that block without a second debit.
`confirmed` records monotonically fill entry, start and expiry: absent values never erase facts,
equal redelivery is a no-op, and contradictions require reconciliation. Expected start remains
separate; a buy transaction's approximate `date_expiry` never confirms expiry.

Broker ticks provide continuity only and never settle or free capacity. A matched terminal
`won`/`lost` fact requires its linked exact sell cash transaction, including an explicit zero for a
loss; terminal evidence alone remains `awaiting_cash`. `sell` action and `is_sold` do not determine
outcome. Sources retain contract/payload update identity, terminal status/time, and
`deriv:transaction:TRANSACTION_ID` for cash from both stream and statement. Duplicate cash across
restoration and sources posts once. Unknown cash blocks the account until matching purchase or
terminal evidence, or an explicit `External` reconciliation, resolves it. External sold/cancelled
status plus actual cash produces `Reconciled`/`ExternallyClosed` and a separate closure count,
never a directional win/loss/tie. Reconciliation cannot contradict already recorded terminal or
cash facts.

Financial settlement needs no fabricated path. Confirmed entry and exit alone form authoritative
path diagnostics; missing entry/exit leaves them unavailable. Sparse exit time before expiry is
preserved. Statement windows include the complete target second by sending exclusive
`date_to = through_secs + 1`, paging by 100 while a full page is returned. Portfolio provides open
liabilities; its local fixture is synthetic from the pinned schema. `CashFact` alone does not carry
purchase payout or purchase time: recovery must obtain those from purchase/contract evidence, not
infer them from the proposal or the cash transaction clock.

One current Engine constraint remains relevant to the Phase 12 handoff: the financial ledger
retains an admitted proposal but does not restore a proposal received before any signal; market,
feature and unadmitted proposal inputs need an input replay boundary. Provider purchase clocks carry
whole seconds, so Engine accepts a purchase whose time is no earlier than the second the command was
dispatched in and no later than the decision; the adapter preserves the provider clock unchanged.

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
`1 <= min <= max <= distinct conditions`, non-negative `embargo_micros` at least every
contract's `duration_micros + settlement.max_settlement_delay_micros`, `base_stream`,
`development` and optional `evaluation` (each a `decision_start`, `decision_end`, exactly one
`inputs` entry as in `replay`, and optional `splits`, no evaluation split named `none`; the development input
must name its `outcome_manifest`; `evaluation.decision_start - development.decision_end` must be
at least the embargo), a nonempty `conditions` menu (each entry a `stream`, `output`,
`comparator` and nonempty ordered `thresholds`), the `contracts` (existing contract terms with
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

The menu expands to distinct conditions in menu order. Candidates are every combination of
`min_conditions..=max_conditions` conditions in increasing count then lexicographic index order,
deduplicated by signal-logic identity (the first combination keeps it). A member is one candidate
paired with one contract in configured order; members keep their zero-based global index
`m{index}` everywhere. The family size is computed with checked arithmetic before any allocation.

Lowering runs one development replay whose strategies are one single-condition strategy per
distinct condition (`c{index}`), each on its own unfunded account with the first contract, the
configured policy and envelope; each `signal` record marks a base row where that condition held.
Every synthesized replay carries only the schema version, run mode, storage and its role's replay
table (contracts in configured order, `max_rate_age_micros = 0`, no rates), so its generation is
independent of backend and of the other role. The base rows are the base stream's reference rows
of the bound outcome generation; for each contract duration the device rows are derived with the
outcome reader's cell: a row without an entry tick is masked out, the decision clock is the entry
tick time, `valid` is set only for `valid` cells, the outcome flags follow the cell, and the
release clock is the settlement tick time when valid and the nominal due time otherwise. The
basic dual kernel compares its clock arguments only, so they carry microsecond times unchanged
under their retained `_ms` names; rows are ordered by decision time then index, the mask is one
inside the development window, the equality bucket is one, `payout_basis` is zero, and columns
zero to four (total, wins, losses, ties, invalid) are read.

The statistic of a member applies when `W = winning_net() >= 0`,
`L = purchase() + loss.terminal_fee - loss.gross_return > 0`, and the tie nets exactly zero,
with `p0 = L / (W + L)` from the aligned coefficients; `W = 0` or zero decisive trials gives score
one, and an inapplicable member records its reason. The score is the one-sided exact binomial
upper tail on decisive counts summed in log space away from the mode; the adjusted value is the
reverse cumulative minimum of `min(1, m * p / rank)` after sorting by score then member order
over the applicable members. Heuristic scope screens members whose adjusted value exceeds
`max_adjusted_score`, beyond the first `top` by adjusted value then order, and every inapplicable
member; screened members are never replayed.

Survivors are replayed in canonical chunks of `chunk_size`, one account, strategy and binding per
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

The family generation publishes one object, `family.json`: the resolved `search` table, the plan
identity and base stream, the SHA-256 identity of the retained kernel sources, the sampler
version, the applicable count, every member (conditions, contract, logic identity, raw counts,
null, applicability, score, adjusted value, screen reason, development group, gate reason, rank,
evaluation group, evaluation split groups, stability outcomes), and the lowering and chunk
generation references with their summary identities; pretty-printed JSON in declared field order
with one trailing newline, and identical on every backend. The ready manifest records `kind`
(`search_family`), `schema_version` (`1`), `generation`, `config_hash`, `code_revision`, the
ordered `inputs` (role, instrument, tick, feature, plan and outcome identities), `members`, and
`objects`. The generation is SHA-256 over `binary-alpha search family v1\n`, the configuration
hash and the code revision each followed by a newline, then one line per input
(`role instrument tick feature plan outcome`, a dash for an absent outcome) followed by a newline.
The command writes
`search SCOPE generation GENERATION members M applicable A screened S replayed R passed P evaluated E objects 1`
followed by the stage timings and peak resident memory or `(already published)`, then the
verification line. `data verify` on a family generation validates the manifest and object,
re-enumerates the members from the recorded table, restores every referenced replay through its
verifier and checks its definition against the table synthesized for its recorded members,
recomputes the raw counts through the central-processor kernel from the verified lowering records
and the bound outcome objects, compares every group and split group with the verified summaries
and the ledger projection, recomputes stability, applicability, scores, adjustments, screen
decisions, gates and ranks, and writes
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
`min_settled`, `max_unresolved`, `min_profit`, non-negative `max_drawdown`, in the reporting
currency), the shared funded `accounts`, the reporting contract (`reporting_currency`,
`reporting_scale`, `max_rate_age_micros` and optional `rates`, as in `replay`), the nonempty base
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
`assessment_manifest` development tick generation), the `refit` (a `cutoff` and nonempty
development `fits`) and the optional `evaluation` (a window at least the embargo after the refit
cutoff, nonempty `inputs` evaluation tick manifests and optional `splits`). The declared count,
computed with checked arithmetic as the sum over subsets of the product of each deployment's
alternative count, times the number of risk policies, must neither overflow nor exceed
`max_policies`; the embargo must be at least every alternative's duration plus its permitted
settlement delay; one contract identity names one contract, so identical terms may repeat under
their identity while conflicting terms may not. Accounts, every alternative, every risk policy,
the rates, the reporting contract and the first fold's window are validated by the execution rules
before any choice is enumerated. Omitting the table preserves every existing configuration
identity.

### Stages and identities

Every declared development input is read on its manifest bytes before any output exists: a fit
resolves through the feature owner and its whole coverage must end before its cutoff; an
assessment tick generation must carry the development role (holdout is refused) and the fit's
instrument; every binding's instrument must have one input in every fold and in the refit. The
optional evaluation inputs are not read at all until selection and refit succeed; only then are
their manifests read for role and instrument and their objects opened. Each family is read through
the typed development-only reader: every manifest input must be development before `family.json`
is opened; the family must carry no evaluation window, no lowering or chunk of another role and no
member evaluation group, split group or stability entry before any referenced generation is
followed; then the family verifies exactly as `data verify` does, whose chunk reader checks each
referenced replay manifest's own role and summary before restoring it. Nothing is stripped to make
an input acceptable.

The logical universe is the declared members' conditions with their ordinals; an ordinal
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
whose content hash the manifest binds, every family (generation, plan identity, base stream and every source member with its logic
identity, contract and the bases that declare it), the logical members, the declared, rejected,
valid and passing counts, every fold's fit and assessment generations, every choice (subset,
alternatives, risk policy, identity, structural rejection, fold results with their replay
generation and summary identity, projection and inapplicability, aggregate profit and drawdown,
failure and rank), the selected index, the refit generations, the frozen policy, the outer result
(feature generations, replay reference, projection and split groups) and the terminal `state`
(`selected`, `no_feasible_policy`, `refit_inapplicable` with its reason, `outer_rejected` with its
reason); only `selected` carries a deployable candidate, never a certification. The ready manifest
records `kind` (`portfolio_selection`), `schema_version` (`1`), `generation`, `config_hash`,
`code_revision`, the `families` generations, `state` and `objects`; the generation is SHA-256 over
`binary-alpha portfolio selection v1\n`, the configuration hash, the code revision and every
family generation, each followed by a newline, so extending a grid changes the identity even when
the winner is unchanged. The command writes
`portfolio generation GENERATION declared D rejected R valid V passing P state S objects 1`
followed by `[bind S folds S refit S publish S]` or `(already published)`, then the verification
line. An interruption preserves every completed replay and feature generation and publishes no
selection; the rerun reuses them and recomputes the rest. `data verify` on a selection checks
the recorded configuration's hash against the manifest, re-reads the families through the
development-only reader, re-enumerates the choices, identities and structural rejections,
verifies every recorded fit, assessment, refit and outer feature generation through the feature
verifier and re-resolves every configured fit through the feature owner against the recorded plan
before its fit and against its cutoff, restores every recorded replay through its verifier and checks its definition against the table
rebuilt for that choice and fold, recomputes every projection, gate, aggregate, rank, the frozen
policy and its compilation under the refit plans, and the terminal state, and writes
`verified portfolio generation GENERATION declared D rejected R valid V passing P state S objects 1 bytes B`.
