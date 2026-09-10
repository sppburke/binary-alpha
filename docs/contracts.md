# System contracts

These contracts bind every phase of Binary Alpha. The current checkout implements the
[configuration](#configuration), [historical datasets](#historical-datasets), and
[instrument streams](#instrument-streams) sections; the other
sections are frozen now so that later phases implement them once, in one place, without
reinterpretation. Specification intent, checkout
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
| `instruments` | array of tables | optional; consumed only by `data audit`, which requires the entry that maps the audited generation |

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

Every field is required and has no default, except that the `import` table, the `provenance`
list, the `instruments` list, and the optional instrument fields named above may be absent. Any
other field is rejected as unknown, so a raw secret value has no place to live. Validation opens
no source or destination and mutates nothing.

### Deferred entries

The envelope will also carry lists of brokers, accounts, feature definitions, contract terms,
research splits, objectives, risk policies, and live settings. The phase that first consumes each
one adds it to the table above together with its
validation: neutral typed identifiers rather than strings with implicit meaning; durations and times
with explicit units; currency-bearing exact amounts parsed from decimal text without binary floating
point; credentials only as references that the application resolves outside the document; and
rejection of duplicate identifiers, invalid references, unsupported combinations, and any value that
would relax causal ordering, holdout isolation, or a financial invariant. None of these exists in
the current checkout.

### Run modes

A run mode selects capabilities and input or output, never semantics. `research` runs development,
evaluation, optimization, and, under a separate operator grant, locked-holdout certification;
`replay` drives the live runtime from a recorded event log without broker mutation; `paper` runs the
live path without real orders; `live` places real orders under its own authorization. The current
checkout validates the value and executes no mode.

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
the same order is its canonical definition, the text a stream generation's identity hashes. Two
documents with the same values have the same canonical form regardless of key order, whitespace,
or comments.

### Content hash, version 3

The content hash is SHA-256 over the bytes `binary-alpha config hash v3`, one line feed, and the
canonical document. It is rendered as `v3:sha256:` followed by sixty-four lowercase hexadecimal
digits. Any change to the hash input or to the canonical form increments the version prefix.
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
or `bar_parquet`);
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
volume out of the finite range, or a record of the other granularity) is reported
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
in integer arithmetic, `u32::MAX` at most, undefined and skipped after a zero price) over the
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
resampler is a reference with two limitations the target does not reproduce: it parses every
timestamp to whole milliseconds through binary floating point (a sub-millisecond time is
truncated and a whole-millisecond time can shift by one), and it treats a run of one price that
starts at the Unix epoch as absent; the target keeps exact microseconds and every run.

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
