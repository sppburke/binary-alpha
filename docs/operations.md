# Operations

## Authorization boundaries

No real feed, session, order, certification, production mutation, migration, deployment, or deletion
occurs without authorization for that exact action. Research success, certification, merge,
deployment, paper operation, and live operation are separate authorizations. Ordinary verification
uses deterministic fixtures, fakes, emulators, recorded frames, or an explicitly authorized sandbox,
never a live, locked-holdout, or production system.

Broker access uses only authorized application programming interfaces and WebSockets. No browser,
Document Object Model, Chrome DevTools Protocol, browser profile, cookie, or click-execution path
exists or will be added.

## Secrets

Configuration documents and the repository contain no secret values. Where a phase needs a
credential, the configuration names a reference and the application resolves it from the process
environment or an authorized secret store at run time; the Google Cloud Storage client uses
Application Default Credentials and the configuration names only the bucket and prefix. Credentials, broker and account material,
proprietary source data, locked holdout, completed evidence, and production cloud state are protected
state.

## Artifact ownership

Google Cloud Storage owns immutable bulk data and artifacts. Supabase owns only a proved
transactional control or metadata need and stores references, never duplicate bulk or execution
truth. Writes and operator procedures are resumable, so a second agent can continue from the last
checkpoint. A completed evidence identity is never overwritten.

## Historical data

`storage.historical_data_dir` names the retained local copy shared by `data import` and the later
history downloaders; `storage.publication_uri` names the durable destination (see
[docs/contracts.md](contracts.md), section "Historical datasets"). The Google client resolves
Application Default Credentials from the process environment; the configuration carries only the
bucket and prefix. A `file://` destination is the non-live test boundary, accepted only under
`run_mode = "research"`, and is never production truth.

Import: configure the folder, the destination, and the explicit `import.sources` inventory, then run
`binary-alpha data import --config PATH`. Sources may live anywhere outside the folder and the
destination; nothing outside the declared inventory is opened, and a `tick_parquet_daily` source
opens only its listed directories. Source files are never moved, renamed, or rewritten. Every object is retained locally and published under the same content-addressed key;
the ready manifest is published last and mirrored locally. The command is resumable: re-running it
after an interruption at any point reuses identical existing objects, finishes the missing ones, and
completes the local mirror; different content at an existing key stops the command without replacing
either copy. No quiescence is required; readers of the source files continue during import.

Verify: `binary-alpha data verify --manifest URI` re-reads one generation from its ready manifest and
objects alone, from either the destination or the retained mirror.

Audit: declare the instrument under `[[instruments]]` (identity, currencies, price scale, native
granularity, the enabled checks, sessions, and candle streams), then run
`binary-alpha data audit --config PATH --manifest URI` against the published dataset generation.
The command reads the generation from the store the manifest names, verifies every data object as
it decodes it, feeds the instrument stream in order, retains the profile and candle objects in the
historical-data folder, publishes them and the stream manifest last to `storage.publication_uri`,
and mirrors the manifest. It is resumable and idempotent the same way import is: rerunning it reuses
identical objects and a committed manifest, and different content at an existing key stops it
without replacing anything. A generation whose identity no instrument maps is an error; no
instrument is ever defaulted, and a holdout generation is refused. Verify a stream generation
with the same `data verify` command.

Outcomes: declare the `[outcomes]` table (the role, the tick and feature ready manifests, the
expiries, and the label thresholds; see [docs/contracts.md](contracts.md), section "Outcomes"),
then run `binary-alpha outcomes build --config PATH`. The command reads both generations from the
stores their manifests name, verifies every object as it reads it, labels every decision row
against the complete tick generation, retains the arrays and matrices in the historical-data
folder, publishes them to `storage.publication_uri`, reconstructs the generation from the
published objects, and publishes and mirrors the manifest last. It is resumable and idempotent
the same way import is. A declared holdout role, a holdout or bar generation, and a feature
generation computed from another tick generation are refused before any row is read. Verify an
outcome generation with the same `data verify` command, which recomputes every label from the
published ticks and reference times.

Replay: declare the `[replay]` table (the role and decision window, the tick, feature, and
optional outcome ready manifests per instrument, the funded accounts, strategies, ordered
bindings, contract templates, risk policies, and the reporting-currency contract; see
[docs/contracts.md](contracts.md), section "Execution"), then run
`binary-alpha replay --config PATH`. The command reads every generation from the stores their
manifests name, verifies every object as it reads it, feeds ticks and feature rows through the
engine in availability order with the configured simulated acceptances, retains the ledger and
summary in the historical-data folder, publishes them to `storage.publication_uri`, restores the
generation from the published ledger, and publishes and mirrors the manifest last. It is resumable
and idempotent the same way import is. A declared holdout role, a holdout or bar generation, a
feature generation of another tick generation or instrument, an outcome generation of other
inputs, decision times outside the declared window, a strategy naming an output its frozen plan
does not compile, and conflicting shared policies are refused before any tick is read. Verify a
replay generation with the same `data verify` command, which restores the ledger record by record.
Historical replay performs no broker, live, paper, or production action and needs no operator task.

Portfolio: declare the `[portfolio]` table (the development-only families, the base members and
repairs, the bindings with their exact contract and envelope alternatives, the ordered subsets,
the risk policies, the inner folds, the refit, and the optional evaluation; see
[docs/contracts.md](contracts.md), section "Portfolio selection"), then run
`binary-alpha portfolio optimize --config PATH`. The command reads every declared input on its
manifest bytes and refuses a holdout, later-role, or ill-formed input before any output, builds
the fold, refit, and outer feature generations and publishes every joint replay through the same
owners as `features build` and `replay`, retains and publishes the selection object, and
publishes and mirrors the manifest last after verification. It is resumable and idempotent the
same way import is: an interrupted run leaves completed replay and feature generations and no
selection, and the rerun reuses every completed generation after its own verifier restores it.
Verify a selection with the same `data verify` command, which restores every referenced replay.
Selection performs no broker, live, paper, or production action and needs no operator task.

Rollout to Google Cloud Storage: discover and reuse existing projects, buckets, identities, and
regions first; create nothing in a region whose name begins `us-west`; provision the bucket and a
least-privilege identity that can read and create objects but not create or delete buckets, outside
the application; then run the import above. Rollback reverts the application and configuration
change; source files, the retained copy, and published generations stay intact.

## Offline NVIDIA runner

Issue [#8](https://github.com/sppburke/binary-alpha/issues/8) authorized the offline compiler setup
and build-runner registration on the existing `quantum` machine. Runner
`binary-alpha-cuda-quantum`, actions/runner `2.337.0`, was registered on 2026-09-11 to this repository
only, outside the source checkout. This made no production or cloud change and did not change the
driver. The normal runner process executes `.github/workflows/cuda.yml`; no service-manager or
container setup is required. The pinned toolkit, driver compatibility, and required
`BINARY_ALPHA_NVCC`, `BINARY_ALPHA_TEST_CONFIG`, and `BINARY_ALPHA_CUDA_REFERENCE` environment
variables are recorded in [README.md](../README.md). Verify that this runner identity is online
before dispatching the governed proof. The workflow reads the immutable attempt-5 reference, a
quiet-device capture at the extraction commit, and never recaptures expected results.

Runner rollback stops and unregisters only `binary-alpha-cuda-quantum`; keep the existing driver
and unrelated machine setup. Accelerator rollback selects the explicit central-processor backend
and reverts the accelerator change while preserving completed evidence. Production operator tasks:
none. Linked matching Sentry issues: none.

## Regions

Create nothing in a region whose name begins `us-west`. This applies to every bucket, database,
service, runner, and secret.

## Rollback

A repository change rolls back by reverting its merge commit. Published dataset, stream, feature,
outcome, and replay generations are
immutable and are never deleted by rollback; the retained historical-data folder and the original
source files stay intact, and a consumer selects the prior generation by its identity. Schema, broker, and other production state do not exist at this phase; the
phase that creates any of them records its own cause-specific verification and rollback before it
ships. Production work minimizes downtime, prefers
a safe non-quiescent alternative when one preserves proof and rollback, checkpoints each mutation for
resumption, verifies the cause-specific result, and retains rollback.

## Closeout reporting

Every plan and delivery report states production operator tasks and linked matching Sentry issues,
using `none` where evidence proves none. Reporting does not create a Sentry project or integration.
An already linked matching Sentry issue is closed only after deployed proof, with no waiting period.
