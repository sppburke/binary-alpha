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
state. Broker `credential` fields name process environment variables. Deriv resolves a bearer token;
Pocket Option resolves the complete opaque JSON authentication object. Keep values out of
configuration, logs and evidence. Credential renewal is an operator action, performed either by
hand or by the operator's own renewal program named in the broker's `credential_command`; the
application runs that program when the variable is unset and once more after a rejected session,
and never contains a browser or login path itself. A Pocket Option renewal program that logs in
with an account login and password from the environment and prints the session object is the
operator's, kept outside this repository together with those values.

For Google Drive, `drive.credential` names a process environment variable holding user OAuth
(Open Authorization) refresh credentials as a JavaScript Object Notation (JSON) object with string
`client_id`, `client_secret`, and `refresh_token` fields. Only the variable name enters the
pipeline configuration. Refresh tokens, access tokens, and resumable session locations are never
printed. Keep the service's values in the private
`/etc/binary-alpha/data-pipeline/<instance>.env` environment file; do not copy it into evidence.
The session checkpoint is also private state, not a report.

## Artifact ownership

Google Cloud Storage owns immutable bulk data and artifacts. Supabase owns only a proved
transactional control or metadata need and stores references, never duplicate bulk or execution
truth. Writes and operator procedures are resumable, so a second agent can continue from the last
checkpoint. A completed evidence identity is never overwritten.

The research pipeline may archive ordinary development/evaluation market datasets and their
stream outputs privately in Google Drive under immutable catalogs and publish them locally.
Google Cloud Storage retains every production, certification, and holdout authority.

## Historical data

`storage.historical_data_dir` names the retained local copy shared by `data import` and `data fetch`; `storage.publication_uri` names the durable destination (see
[docs/contracts.md](contracts.md), section "Historical datasets"). The Google client resolves
Application Default Credentials from the process environment; the configuration carries only the
bucket and prefix. A `file://` destination is accepted only under `run_mode = "research"` for
non-live tests and the research data pipeline, and is never production truth.

Import: configure the folder, the destination, and the explicit `import.sources` inventory, then run
`binary-alpha data import --config PATH`. Sources may live anywhere outside the folder and the
destination; nothing outside the declared inventory is opened, and a `tick_parquet_daily` source
opens only its listed directories. Source files are never moved, renamed, or rewritten. Every object is retained locally and published under the same content-addressed key;
the ready manifest is published last and mirrored locally. The command is resumable: re-running it
after an interruption at any point reuses identical existing objects, finishes the missing ones, and
completes the local mirror; different content at an existing key stops the command without replacing
either copy. No quiescence is required; readers of the source files continue during import.

Fetch: configure `[[brokers]]`, tick `[[instruments]]`, `[history]`, the retained folder and destination.
Choose a finite `[start,end)` and optionally `refresh_interval_seconds`, then, with authorization for
the exact provider/account/action, run `binary-alpha data fetch --config PATH`. Refresh runs in the
foreground; stop it to end ongoing downloads and restart with the same
configuration to resume verified work. See [broker fetch contracts](contracts.md#broker-access)
for pagination, coverage, shortfalls and publication mechanics. Removing `[history]` restores the
offline import/audit workflow.

Inspect: after authorization for the exact provider, account and non-purchasing checks, configure
`[inspect]` and run `binary-alpha broker inspect --config PATH`. It checks bounded history and live
subscriptions/cancellation; credentialed Deriv also checks balance, transaction acknowledgement and
optional CALL/PUT proposal economics. It never buys. Retain the report at its printed `inspection URI`
and its local content-addressed copy; an unavailable check is not external acceptance. Schema-2
execution fixtures prove the library boundary only; deployment and durable dispatch remain Phase 12.

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

Outcomes: declare the `[outcomes]` table (the role, the observation and feature ready manifests, the
expiries, and the label thresholds; see [docs/contracts.md](contracts.md), section "Outcomes"),
then run `binary-alpha outcomes build --config PATH`. The command reads both generations from the
stores their manifests name, verifies every object as it reads it, labels every decision row
against the complete observation generation under the [outcome binding](contracts.md#outcomes),
retains the arrays and matrices in the historical-data folder, publishes them to
`storage.publication_uri`, reconstructs the generation from the published objects, and publishes and mirrors the manifest last. It is resumable and idempotent
the same way import is. A declared holdout role, an unauthorized holdout generation, and a feature
generation computed from another observation generation are refused before any row is read. Verify an
outcome generation with the same `data verify` command, which recomputes every label from the
published observations and reference times.

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

Cut research roles: declare `[split]` with a namespace, one development daily-root source per
instrument, and whole-day development, evaluation, and holdout windows. Run
`binary-alpha data split --config PATH` with a retained folder and destination outside the source
stores and managed pipeline stores. Use the reviewed `storage.publication_uri` on Google Cloud
Storage with existing credentials for real holdout authority; local publication is for fixtures.
The command prints the bounded generations and their governance declaration location.

Give the source one development window and make its search window equal to that range: historical
replay refuses feature decisions outside the search window. For each fold, give the fit a window
ending at its cutoff and the assessment a window equal to its decision window, with the start
at least the embargo after the cutoff. Give the refit its own fit window; an identical source
window reuses the same generation. Reserve one disjoint evaluation window and one disjoint
holdout window per attempt, because each day's token is claimed once. Name these generations
and the printed declaration in the research configuration. The unsliced root is never a research
input. This command requires no production operator task or downtime; matching Sentry issues:
none. It provisions no bucket and changes no broker, archive, or managed store.

Research: use the declaration from `data split`, or author a non-sensitive governance declaration
from authorized records (operator,
authoritative root, namespace, and every population with its role, instrument, source, coverage,
generation aliases, sorted stable conflict tokens, and exposure history; see
[docs/contracts.md](contracts.md), section "Research") and publish it at the location
`research.study.governance_manifest` names; never open holdout data to discover aliases, overlaps,
or prior outcomes, and record an unknown mapping as unavailable. Declare the `[research]` table
and run `binary-alpha research run --config PATH`. The command permits every declared input
before any read, creates the attempt intent beneath the authoritative root, publishes every child
through the same owners as the individual commands, publishes the frozen stage and, after
verification, the run record, and exits successfully awaiting holdout authorization; rerun it to
resume the same identity after any interruption (a published frozen stage or run is verified and
restored, never recomputed). Copy protected holdout objects to the approved bucket only under a separate
logged byte-transfer authorization that preserves bytes, hashes, and role. After the run reports
its identity, an operator with a distinct identity that may create but not overwrite grant objects
runs `binary-alpha holdout grant create --config PATH --bundle-manifest URI --holdout-manifest URI
--reason TEXT` (one `--holdout-manifest` per instrument, in instrument order); the grant never
enters tracked configuration. Rerunning `research run` then claims the protected population,
creates the receipt, and publishes one certified or rejected result; another agent may rerun the
same command safely, because every transfer, grant, claim, receipt, and generation uses
deterministic identities and conditional creation and the grant is consumed once. A rejected
result is terminal for that frozen run: it never tunes, reranks, retries, or opens another stage.
No live service quiescence or downtime is involved.

Rollout to Google Cloud Storage: discover and reuse existing projects, buckets, identities, and
regions first; create nothing in a region whose name begins `us-west`; provision the bucket and a
least-privilege identity that can read and create objects but not create or delete buckets, outside
the application; then run the import above. Rollback reverts the application and configuration
change; source files, the retained copy, and published generations stay intact.

## Data pipeline

Use [the pipeline example](../configs/data-pipeline.example.toml) and
[the implemented contracts](contracts.md#data-pipeline) to prepare the non-secret pipeline
document, sibling core configurations, and source-binding evidence. The managed store under
`local_root/store/` is the one system-owned copy of every dataset. Existing originals enter
through `data import`; new instruments bootstrap through `update`. Use one writer host per Google Drive
archive root and the same managed root for manual and timer producers. One archive root must
have one producer registry (`local_root/pipeline_state/registry`). Separate managed roots keep
independent reservations and cached listings: if one registry was initialized before another
producer uploaded content, it can upload unchanged content again under new file IDs. Sharing
an archive root across those registries does not deduplicate uploads. Update holds
`pipeline_state/writer.lock`; a second producer fails immediately. Pull and restore also hold
the store writer lock and host-local archive-root lock.

Set `drive.retry_seconds` to the per-request wall-clock budget for transient transport errors,
HTTP 429, and 5xx (default 900 seconds); retries wait 250 ms initially, doubling to a 30-second
cap. `drive.max_attempts` still limits 401 token refresh attempts and resumable-session restarts.
HTTP 403 reasons `userRateLimitExceeded` and `rateLimitExceeded` use that same backoff and
budget; other 403s fail immediately, with the Drive reason included in the report.
On a recorded session's status query or a chunk PUT, either rate-limit 403 instead abandons the session within `drive.max_attempts`, reconciling any existing file under the pre-generated id or clearing the checkpoint and starting a replacement for that same id, with chunk-triggered replacements waiting 1, 2, 4 seconds and so on by restart count up to 30 seconds and session creation still using the retry budget.

### Consent and credentials

Initial user OAuth (Open Authorization) consent is an operator task using supported Google
tooling. Obtain refresh credentials with `https://www.googleapis.com/auth/drive.file` scope for
an app-created or explicitly granted archive root; a folder identifier alone grants nothing.
Keep the archive private and check the actual grant before unattended operation. See
[Google Drive scopes](https://developers.google.com/workspace/drive/api/guides/api-specific-auth).
An external OAuth app left in Testing issues refresh tokens that expire after seven days when
requesting this scope. Complete the appropriate consent setup before weekly use; see
[Google's token lifetime rules](https://developers.google.com/identity/protocols/oauth2#expiration).

Place the pipeline configuration at `/etc/binary-alpha/data-pipeline/<instance>.toml` and
credential environment values at `/etc/binary-alpha/data-pipeline/<instance>.env`, with private
permissions (mode `0600` for the environment file). The instance name is the existing non-root
operating-system user. The service manager reads the environment file; manual commands need those
variables in their own process environment. Core configurations and evidence paths resolve
relative to the pipeline file; use an absolute managed `local_root` for an installed service.
Supply any known study declaration as `governance_manifest`; a pinned catalog never overrides
its denial. The core job configuration must omit its `research` table.

### First migration of existing v1 data

Keep the timer inactive and other writers frozen. Preserve the original store and all records
until every job has passed fresh-store restoration and verification. These steps use the
pipeline's supported command owners; no conversion, upload, or deletion scripts are required.

1. Run `binary-alpha data pipeline migrate --config PIPELINE`. For a bounded subset add
   `--job ID`. This is offline and creates verified v2 continuation roots plus migration evidence.
   On a first-time migration, a native-bar day holding a validated complete grid (17,280
   five-second bars from the UTC day start) is `complete` whatever the legacy source kind; a
   day without a complete grid keeps the state its retained claims support. An already
   verified checkpoint is reused as `already_verified`, so existing roots keep their recorded
   day states.
2. Run `binary-alpha data pipeline archive --config PIPELINE` for every migrated job.
   Retain the printed catalog IDs and SHA-256 digests.
3. Create a consumer document with the same archive root, a fresh, empty `local_root`, and
   `parallel_jobs` set to the number of instruments to restore at once. Run
   `data pipeline restore --config CONSUMER --all`: it selects every instrument's newest
   verified catalog (the same selection as `pull`), prints `selected ... catalog FILE_ID
   sha256 SHA256` and `restored ...` for each, and fails at the end naming any instrument
   that did not restore. To pin one catalog instead, run `restore --catalog FILE_ID
   --sha256 SHA256 --broker BROKER --symbol SYMBOL`. Then run `data verify --manifest URI`
   on the printed dataset and stream URIs. Restore also authenticates lineage manifests and
   the exact immutable evidence-record closure. Inspect the coverage and unresolved ranges.
4. Rebind active fetch/consumer configurations that still pin v1 manifests, through their
   documented configuration fields. Keep frozen evidence unchanged. A remaining reference
   protects its closure and prevents its deletion.
5. Run `binary-alpha data pipeline retire --config PIPELINE --plan`. Review every
   `references` entry, retained root, exact local path, remote file ID, digest, and byte total.
6. Only after that review run `binary-alpha data pipeline retire --config PIPELINE
   --apply PLAN_FILE`. Preserve the sealed plan, progress journal, and completion record.
   The plan fingerprints every `.toml` under the pipeline configuration directory, the job
   files, `pipeline_state`, and `store/manifests`: adding, editing, or removing any of them
   (a temporary batch document included) between sealing and the completion record makes
   apply refuse with `stale plan`, so clean up only after `plan-SHA256.retired.json` exists.
   Ordinary retirement preserves the current v2 root/catalog and requires verified migration
   evidence before removing v1. It is distinct from whole-instrument removal below.

After interruption, rerun the same migrate, archive, or restore command: each owner resumes
its checkpoints or verifies identical completed bytes. A changed source/configuration is a
conflict, never permission to overwrite evidence. The explicit exception is a completed
pre-session migration: adding validated `instruments.session` tables alone may be bound into
a proof-version-4 superseding receipt. Every other configuration field and evidence byte must
reproduce the original binding. Predecessor receipts remain unchanged. Migration reconstructs
old daily candles under their recorded definition and independently verifies the new product.
Every mapped v1 source needs its own preservation proof; divergent or unproved sources stay
in the retained archive closure and appear as protected in the retirement plan. Fresh restore
also reconstructs the current catalog receipt, so record comparisons include that receipt.
After retirement apply starts, keep the
pipeline, job files, records, and registry fixed and resume **that exact plan**; its durable
fence rejects other writers until retained verification and the completion record succeed.
Before apply starts, a stale plan may be replaced by a newly reviewed plan.

### Register a new instrument

Prepare one broker policy template using
[Deriv](../configs/data-pipeline-deriv.example.toml) or
[Pocket](../configs/data-pipeline-pocket.example.toml), with the authorized endpoint/source
context, credential references, history start/end and budgets, and candle policy. Registration
replaces the template instrument; a separate hand-generated template for each symbol is unnecessary.

```text
binary-alpha data pipeline add-job --config PIPELINE --template CORE --broker BROKER --symbol SYMBOL
binary-alpha data pipeline update --config PIPELINE --end END
```

Use `--quote-currency C` for synthetics, equities, or another ambiguous symbol. Only a
six-uppercase-letter currency pair (optionally `frx` / `_otc`) supplies an inferred quote.
Discovery supplies price precision where reported. Pocket reports no precision, so registration
decodes one bounded native-history sample at supported exact scales. An empty/unusable sample
requires explicit `--price-scale N`. A later price needing more digits fails with that count;
never round or edit stored observations. Correct the declared scale only after checking the
provider evidence; a pending intent retains its original configuration binding.

An explicit singular `[instruments.session]` is mandatory; alternatively `--session FILE`
reads a TOML file containing the table's fields. `kind = "always"` is an explicit 24/7 policy.
The exact weekly shape is `kind = "weekly"`, `timezone = "UTC"` or `"America/New_York"`,
`open = { day = "sunday", time = "17:00:00" }`, `close` in the same shape, and optional
`closed_dates = ["2026-12-25"]` / `early_closes = [{date="2026-11-27",time="13:00:00"}]`.
Weekdays are full lowercase English names. Never substitute the old plural profile windows
`[[instruments.sessions]]`. The engine validates both `always` and weekly calendars through
`Config::parse` and `Session::calendar()`. Registration and acquisition preserve the session
in canonical core TOML, so a calendar change changes the pending intent's configuration binding.

Registration writes `jobs/JOB.toml` and `evidence/JOB.json` and appends the pipeline entry last.
It checkpoints the exact discovered output under `pipeline_state/registrations` first. On
interruption repeat the same command/template/options; it completes from those bytes without
rediscovery. Existing different bytes are a conflict. An already listed instrument is reported
as already registered. Keep registration records, configuration and source evidence.
The first update starts at `history.start` and publishes a native v2 continuation root directly:
there is no `data import` prerequisite. Missing calendars on an empty-store job are refused.

### Remove an instrument

Complete pending acquisition/migration and in-flight transfers first. Keep its job entry and
configuration intact through apply, and keep other writers frozen. Remove external active
references through their owning configuration; shared objects and unrelated consumers remain
protected. This explicitly removes current data as well as obsolete data, unlike ordinary retirement.

```text
binary-alpha data pipeline retire --config PIPELINE --job ID --whole-job --plan
# Review PLAN_FILE: all v1/v2 manifests, catalogs, object keys, exact remote IDs and records.
binary-alpha data pipeline retire --config PIPELINE --job ID --whole-job --apply PLAN_FILE
binary-alpha data pipeline remove-job --config PIPELINE --job ID
```

Whole-job retirement makes every owned ordinary v1/v2 dataset, stream, catalog and unshared
data object eligible, including remote-only catalog closures. It refuses unresolved retained
dependencies. Immutable local and archived records remain and their dependencies are marked
retired in the plan. A completed whole-job plan prevents a timer from recreating that job.
`remove-job` requires that completion and atomically removes its document entry; repeat it
after a lost reply. It retains configuration/evidence files. Never edit the pipeline document
between plan and completion. Interrupted deletion resumes with the identical `--apply` plan.

### Weekly update, consumer pull, and original imports

The checked-in example uses `parallel_jobs = 4`, `parallel_transfers = 8`,
8 MiB chunks, `request_timeout_seconds = 300`, `max_attempts = 5` and explicit
`retry_seconds = 900`. The broker templates use `overlap_seconds = 60`,
`max_elapsed_seconds = 3600`, and 6,000 Deriv / 20,000 Pocket pages per invocation.
Pocket uses `history_pages_in_flight = 8`, `origin = "https://pocketoption.com"` and
`credential_command = ["/absolute/path/to/pocket-auth"]` (an executable/argv array, not a
shell string). The operator supplies that executable; it prints auth JSON to stdout.
These are portable starting settings, not a promise of coverage within an hour:
20,000 Pocket pages span about 45 days at 195 seconds/page, but provider and elapsed limits
may stop sooner; Deriv's tick density determines its span.

Run `data pipeline update --config PIPELINE` weekly. It follows the v2 lineage, writes only
changed daily observation/page/candle partitions, reuses unchanged days by key, and archives
new objects plus cumulative record closure through the archive-root registry. Partial-day
and request-evidence changes can require new files even when older observations are unchanged.
The weekly update also relabels retained complete native-bar grids whose days lack whole-day
coverage, recording a separate `native-bar-grid` acquisition claim. It preserves retained
observations and finalized candle rows, including delivered `source` fills. After the outstanding
acquisition completes, observation days before each root's new cutoff day should be `complete`
or `empty_known`, except these retained-evidence residuals (inventoried 2026-09-21):

- Every Pocket root: 2025-05-19, with 9,180 retained bars from 11:15 UTC.
- Every Deriv root: 2025-08-10, with zero ticks and recorded `complete=false`;
  2025-12-24 and 2025-12-31, with instrument-specific counts of 78,922–79,196 ticks and
  recorded `complete=false`; 2025-12-25 and 2026-01-01, with zero ticks and no completeness claim.

These days need separately authorized historical re-acquisition. An inventoried cutoff day
remains `partial` with some verified coverage or `unknown` with none; candle days follow the
existing session, finalizer, and pending-candle audit rules. Verify the newest ready manifest
per instrument and an `archived` update report for all 17 jobs after the outstanding run.

A pending update retains its pinned cutoff and exact seed binding, including an initially empty
seed list. Repeat the command to resume; do not move the cutoff or delete progress files.
Page/time budgets may be increased through their declared configuration fields. After a
completed acquisition whose transfer was interrupted, rerun update or archive and let the
registry verify/reuse completed transfers: the registry lists the archive root once when it
opens and reuses every completed upload whose listed size and SHA-256 equal the local identity
without a per-file request; any other case is confirmed per file as before. A remote file
that changes, vanishes, or is trashed after that listing is not noticed within the same run;
the next run's listing, a fresh-store restore (which fails on any downloaded bytes that differ
from the catalog's recorded identity), and retirement's per-file retained verification catch
it, as they did for a file that changed after a per-file confirmation. Drive ids are generated 100 at a time. Consumer pull/restore
resumes partial downloads.

Never hand-generate per-instrument job files, infer a calendar, round prices to force a job
through, rewrite immutable manifests/records, clear transfer/progress state to bypass a conflict,
bulk-upload content with an external script, or delete store/Drive objects by filename or age.
Use add-job, update, migrate, archive, pull/restore, retire and remove-job for these operations.

Import every raw archive once with the existing importer, pointing both storage fields at the
managed store, for example:

```text
binary-alpha data import --config CORE
```

where `CORE` declares `[storage] historical_data_dir = "<local_root>/store"` and
`publication_uri = "file://<local_root>/store"` with the `[[import.sources]]` inventory
(a `tick_parquet_daily` root with every Deriv symbol directory, a `bar_parquet_collection`
with its manifest for Pocket Option). Verify the generations (`data verify`). The pipeline
subsequently reads its managed store. Preserve external originals: retirement deletes only
its exact inventoried managed-store and archive objects, never an arbitrary source directory.

For each source, establish the broker/account class, selected instrument, seed provenance, and
clock mapping from authorized evidence (the production Pocket Option endpoint closes the
namespace immediately unless the broker entry sets `origin = "https://pocketoption.com"`, observed
2026-09-16), and record the resulting broker source identity in the
job's evidence file (`{"source_identity": "…"}` plus notes); binding refuses a configured broker
whose identity differs, and the refusal names both identities. A Pocket archive from a different
account/source context must not be relabeled to match a demo endpoint. Configure Deriv tick history and Pocket
five-second bar history with explicit positive overlap, page, and time limits and no foreground
refresh. Pocket Option candle pages contain 40 five-second bars ending at the anchor inclusive,
so backward paging steps by 195 seconds with a one-bar overlap; the
2026-09-16 real-account measurement was about 4.0, 16.6, and 32.9 pages/s with 1, 4, and 8
requests in flight, respectively (about 0.24 seconds per batch).
Set the Pocket broker's `history_pages_in_flight` to a positive count (default 8) to control
candle-page prefetch on each connection.
Pocket history reconnects and resends outstanding requests with fresh indexes after a response timeout (at most three retries per page), or before sending after more than 25 seconds without a server frame, sharing the adapter's 20-reconnect limit.
With exact broker and Drive authorization, run a bounded update:

```text
binary-alpha data pipeline update --config PIPELINE [--end END]
```

`END` uses `YYYY-MM-DDTHH:MM:SS[.ffffff]Z`. With no pending acquisition, omission samples the
current time for each job. Rerun the same command to resume interruption: pending intent preserves
its cutoff, baseline, start, and retained pages even if a partial snapshot was archived.
`pipeline_state/JOB/progress.json` is the header written once; `progress.pages.jsonl` beside it
appends one flushed JSON line per retained page, and completion removes both files.
Resume reads legacy inline pages first, then complete log lines; a missing log contributes no pages,
and an interrupted trailing line is discarded with `progress log: 1 partial line ignored` so that
page is fetched again from its prior cursor.
A conflicting `--end` or effective core configuration fails with the pending intent identity.
Page/time budgets may change on resume. Preserve `pipeline_state/` and the managed store; transfer sessions and pre-generated file identifiers reconcile interrupted uploads.
After acquisition closes, a later update can select a new cutoff. Set `parallel_jobs` in the
pipeline document to work several instruments at once (one connection each); raise it gradually,
because provider rate limits per connection and per application identifier are not published,
and the measured single-connection rate on Deriv is about 1.3 pages of 1000 ticks per second.
Set `parallel_transfers` (default 8) to bound concurrent object uploads or downloads within each
job; manifests and the catalog are still published last.

Read every job's report and receipt. `pending` leaves acquisition open, possibly with an archived
partial snapshot. `archived_with_gaps` records a catalog with a primary shortfall other than
`unresolved_tail`. Both fail the job and appear inside `pipeline job JOB failed: ...`.
`archived` means the closure was archived under the implemented checks; a provider tail alone
can have this status, and inherited gaps may remain outside the overlap. `no_data` means closed
without a catalog and exits successfully. Neither exit 0 nor an archive status establishes
complete market coverage. Exit 1 means an operation failed or at least one job remained pending
or had the reported gaps; another job's successful archive remains usable.
Each job's report, including any failure cause, is written and flushed as soon as it finishes,
with its lines kept together; the final failure summary still lists all failed jobs.

A consumer host (for example a cloud GPU machine that cloned the repository) pulls the newest
archived generation of each instrument it needs; the command restores only when the generation is
not already local, so it is safe to run before every research stage:

```text
binary-alpha data pipeline pull --config CONSUMER --broker BROKER --symbol SYMBOL
```

To inspect or pin an older snapshot, list catalog metadata for the broker and provider symbol,
then restore a chosen identifier and SHA-256 (Secure Hash Algorithm, 256-bit) digest:

```text
binary-alpha data pipeline list --config PIPELINE --broker BROKER --symbol SYMBOL
binary-alpha data pipeline restore --config CONSUMER --catalog FILE_ID --sha256 SHA256 --broker BROKER --symbol SYMBOL
binary-alpha data pipeline restore --config CONSUMER --all
```

`restore --all` restores every instrument archived on the root, selecting each newest verified
catalog exactly as `pull` does, `parallel_jobs` instruments at a time with `parallel_transfers`
workers each; each instrument's report lines stay together and the final failure summary lists
every instrument that failed. Rerunning it resumes partial downloads and reuses installed objects.

For example, the initial selectors are `deriv`/`frxEURUSD` and
`pocket_option`/`AEDCNY_otc`; the job identifier `pocket` is not the broker selector.
The consumer may omit jobs and needs no broker credentials. Choose its `local_root` explicitly.
List reports catalog identifier/digest, instrument, role, native kind, dataset/stream generations,
actual coverage endpoints, rows, and closure bytes. Restore downloads only the pinned catalog
closure, installs objects then original manifests, verifies both generations through `data verify`,
and prints their local manifest locations. Use those locations in a new consumer configuration;
leave frozen configurations and manifest bytes unchanged. Rerun the same restore to resume partial
downloads or reuse identical installed objects.

### Weekly activation and rollback

Real Drive acceptance evidence is not yet retained: it is unavailable, not passing. Finalization
of a zero-byte object upload against real Drive is unverified. Before enabling real operation,
retain a finite source update and small archive/restore acceptance with the actual authorized
credentials, root, source context, and cutoff. Report source acceptance separately from transfer
correctness. The synthetic `data_pipeline` suite covers empty-store registration, daily updates,
incremental archive/restore, ordinary and whole-job retirement, interruption, scope denial,
and the shipped schedule; it does not establish external acceptance.

Timer installation is a later, separately authorized operator action. Install the executable at
`/usr/local/bin/binary-alpha`, prepare the instance files above, and ensure the configured local
storage is mounted. The service requires `local-fs.target` and orders after it and
`network-online.target`; network ordering does not prove broker or Drive reachability.
Then copy the shipped templates and activate the timer, replacing `<user>` with the instance user:

```sh
sudo cp ops/systemd/binary-alpha-data-backfill@.service \
  ops/systemd/binary-alpha-data-backfill@.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now 'binary-alpha-data-backfill@<user>.timer'
```

The service is `Type=oneshot`, `User=%i`, `UMask=0077`, with the private environment file and
`ExecStart=/usr/local/bin/binary-alpha data pipeline update --config /etc/binary-alpha/data-pipeline/%i.toml`.
The timer specifies `OnCalendar=Sat *-*-* 06:00:00 America/Chicago`, `Persistent=true`,
`AccuracySec=1s`, and `WantedBy=timers.target`. Persistent activation catches a missed calendar
event only. It does not retry failed application work or prove completion. There is no automatic
service restart loop. Resume manually with the same command or
`sudo systemctl start 'binary-alpha-data-backfill@<user>.service'`.

Rollback disables only this timer with
`sudo systemctl disable --now 'binary-alpha-data-backfill@<user>.timer'` and stops the producer
before another request or transfer. Disabling the timer does not stop an already running service;
stop that instance too if active. Retain partial state, receipts, source archives, and completed
local/remote objects. Select the prior verified catalog, compatible dataset/configuration, and
executable. Unrelated readers need no quiescence. Older binaries may reject native bar-history
manifests; keep their prior compatible generations.

Production operator tasks for this research infrastructure: none. Real source/Drive acceptance
and timer installation above require separate exact authorization. Linked matching Sentry issues:
unknown; none supplied for this delivery.

## Live runtime

Phase 12 implements the [ordered runtime, projection, journal, control, authorization,
compatibility receipt, recorded transports, deployment manifest, and live
commands](contracts.md#live-runtime).
This procedure records the separately authorized rollout required by issue
[#13](https://github.com/sppburke/binary-alpha/issues/13); it records no completed production action.

### Rollout and handoff

1. Discover and reuse an existing Supabase project and approved Google resources. If a resource
   must be created, create nothing in a region whose name begins `us-west`; choose the existing
   compute region or a measured permitted region.
2. Apply the `live_leases` and `live_dispatch_claims` migration idempotently. Record its schema
   version and result so another agent can resume. Grant the runtime only the row operations
   needed for its account lease and dispatch claims. The implemented `Postgres::migrate` uses
   `MIGRATION_SQL`, with `binary-alpha live control schema v1` recorded as a comment on both
   tables; it adds no third table. Use the direct endpoint, or the documented session pooler when
   required by the deployment network, with supplied trusted roots and hostname verification.
3. Deploy the binary inactive with approved secrets and a certified DeploymentBundle. The next
   authorized runtime start publishes its immutable deployment manifest after verification and
   warm-up. Keep entries disabled until the exact entry authorization exists.
4. Run immutable replay, paper mode, the complete Deriv demo workflow, and cause-specific cloud
   and lease checks. These observations and purchases each require their exact authorization.
   Create the exact demo entry authorization below before demo purchases. Preserve the required
   account-class distinctions and frozen compatibility support.
5. Start the production instance observation-only. Warm features, replay its journal, reconcile
   transactions/open contracts/balance, and prove cloud publication.
6. Initial migration from a non-cooperative legacy runtime uses a short per-account entry handoff:
   stop new legacy submissions, preserve observation and settlement, resolve every ambiguous
   dispatch, acquire the new lease, then keep the new owner observation-only. Existing accepted
   contracts continue to settlement. Deriv does not enforce the cooperative fencing token;
   stopping legacy submissions is an operator duty.
   Resolve an ambiguous predecessor dispatch only from broker evidence, the dispatching instance's
   own no-write proof, or an operator update after confirming the predecessor cannot write.
   For the exact broker/account/command row in `live_dispatch_claims`, record `state = not_sent`
   when confirmed unsent, or `state = accepted` with verified `contract_ref` and `transaction_ref`.
   Read back the row and retain the confirming evidence. The runtime consumes the update on its
   reconciliation cadence; accepted references still require matching broker purchase evidence.
7. For a supported execution account, require replay, demo, reconciliation, deployment, account,
   bundle, lease, and passing execution-compatibility proof under the frozen account-class
   requirements. Use the operator-only command below to create the exact entry authorization
   before enabling new entries. Discover and validate the deterministic object before creating it so another agent can
   resume after a lost response. Absence, conflict, or mismatch leaves observation, settlement,
   and reconciliation active but new entries disabled.
8. Later target-to-target deploys transfer the account lease transactionally. The old owner stops
   new submissions before release and may continue observation; the new owner begins only after
   lease, claims, broker state, and entry authorization are proven. Once release begins
   the old owner stays entry-disabled even if the response is lost. Resolve uncertain release by
   readback or expiry; the next acquisition has a greater fencing token.
9. Checkpoint every command by deployment hash, migration version, lease fencing token, journal
   sequence, and object generation so another agent can resume safely. Keep command result and
   exact configuration, code revision, environment, broker/account class, and evidence window with
   those identities; preserve completed evidence.
10. Verify source continuity, warm-up, bundle identity, lease ownership, broker balance, open
    contracts, unresolved dispatch claims, journal commitment, cloud objects, and final manifests
    before declaring production complete. Measure market-event-to-decision and claim-to-socket
    delays on the deployment host, separately from decision-to-acceptance delay; verify bounded
    queues and no sustained growth. A refused incompatible receipt is refusal proof, not a passing
    execution-fidelity result.
    Only full deterministic journal segments upload, verify, and clean. Partial open segments
    remain local without rotation or upload. Verify the final manifest's full segments, published
    ledger generation, and local open-tail range and hash.
11. Rollback stops new entries, continues settlement/reconciliation, resolves ambiguous dispatches,
    transfers the lease only when safe, and restores the previous binary and certified bundle.
    Never delete legacy data or evidence during cutover. Retain unresolved claims; remove a
    reconciled terminal claim only after its complete journal lifecycle is in verified full
    segments in Google Cloud Storage. A claim with lifecycle records in the open tail stays until
    that segment fills and verifies. If the previous binary/bundle cannot satisfy
    current bindings, keep entries disabled.

No global service downtime is required. The only intended interruption is the shortest safe
account-specific new-entry handoff; observation, reconciliation, and settlement remain active.
Commands in steps 4 and 7:

```sh
binary-alpha live replay --config PATH
binary-alpha live run --config PATH
binary-alpha live authorization create --deployment-manifest URI --bundle-manifest URI --broker ID --account ID --reason TEXT
```

`live replay` accepts `research` or `replay`; the filesystem publication boundary still requires
`research`. It reads a recorded broker-event log and never connects to a broker or resolves its
credential. `live run` accepts `paper` or `live`; paper keeps account observations but does not
purchase. Every `live` entry, including demo, requires the exact authorization object. The current
options adapter permits proposals only for demo USD; a real account can supply observations but
cannot obtain a supported purchase proposal. Configure the operator's Google identity to create
but not overwrite authorization objects and the runtime identity to read them. Repeat creation
after response loss with the same bindings, operator, and reason. Any deployment/configuration/bundle/broker/account change
requires a new exact authorization. See the [command matrix](contracts.md#live-runtime),
[authorization object](contracts.md#authorization), and
[compatibility receipt](contracts.md#compatibility-receipt).

### PostgreSQL control gate

The required non-live `postgres_control` gate runs against an isolated non-production PostgreSQL
database with two actual control sessions and a raw test session for row-lock ordering. It applies
the production migration and lease/claim statements, with a fake broker and the existing
Engine/journal recovery. Ordinary fake-control tests cannot replace it. The procedure itself
does not authorize a production migration.

`BINARY_ALPHA_TEST_CONFIG` names an untracked JavaScript Object Notation (`JSON`) document. The
implemented wrapper in
[phase12_live_runtime/control.rs](../crates/app/tests/phase12_live_runtime/control.rs) reads exactly
these connection and negative-certificate inputs:

```json
{
  "control": {
    "host": "localhost",
    "port": 5432,
    "database": "phase12_control",
    "user": "phase12_test",
    "credential": "BINARY_ALPHA_TEST_DATABASE_PASSWORD",
    "root_certificate": "/ABSOLUTE/PATH/trusted-root.pem"
  },
  "wrong_root_certificate": "/ABSOLUTE/PATH/unrelated-root.pem",
  "wrong_host": "127.0.0.1"
}
```

Replace the example connection with the approved isolated endpoint. The password is read from the
environment variable named by `control.credential`; the JSON contains its name, never its value.
Certificate files contain trusted roots, not secrets. Use
absolute paths for reproducibility; the test passes these paths directly, without resolving them
against the JSON file. The correct root and hostname must validate the endpoint. `wrong_host`
must reach that same test server under a name its certificate excludes; the current negative
assertion specifically expects `127.0.0.1`. The supplied incorrect root must fail issuer
verification. The implemented wrapper does not read runtime owner or lease timing settings;
it chooses those within its test cases.

Record the isolated server/database/schema identity, schema comment/version, configuration
identity, and clean code revision with the gate result. The wrapper has no separate schema field;
confirm the connection's actual schema before running its idempotent migration. Run:

```sh
cargo test --locked -p binary-alpha-app --test phase12_live_runtime
BINARY_ALPHA_TEST_CONFIG=PATH cargo test --locked -p binary-alpha-app --test phase12_live_runtime postgres_control -- --exact --ignored --nocapture
```

The gate must prove migration repeated twice, acquisition contention, renewal blocked behind
release, claim insertion racing release/acquisition, expiry while waiting for the lease row lock,
stale-token refusal, lost commit response/readback, and post-claim reconstruction of exact
exposure without an unauthorized or ambiguous purchase write. The clock query occurs after the
row lock returns, as specified in [Leases and dispatch claims](contracts.md#leases-and-dispatch-claims).
Incorrect root or hostname must fail before control mutation. Preserve the returned rows/tokens,
durable claim contents, recovery result, and test output. An unavailable database, credential,
certificate, or endpoint is unavailable evidence, never a passing fake result. This documentation
change does not run the gate or establish its result.

The broader Phase 12 delivery also requires retained Phase 10 execution and Phase 11 research
tests, the complete deterministic live replay fixture, and the workspace and selected-feature
workflow gates at one clean commit. Separately authorized demo and deployment proofs remain
external acceptance.

Production operator tasks: the separately authorized rollout, account handoff, verification, and
rollback in steps 1–11; none executed by this documentation change.

Linked matching Sentry issues: none. Issue #13 records no target Sentry configuration or linked
matching issue. Rediscover at deployment; do not create a project solely for this phase. If an
implementation pull request links a matching issue, close it immediately after its corresponding
production proof in steps 4–10, with no waiting period.

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
outcome, broker-history, replay, family, selection, research, and certification generations and
every governance record (intent, claim, grant, receipt) are
immutable and are never deleted by rollback; a research rollback selects the last certified
bundle or disables promotion and never deletes a rejected bundle, grant, receipt, source object,
or prior generation; the retained historical-data folder and the original
source files stay intact, and a consumer selects the prior generation by its identity. Pipeline
rollback also retains catalogs and partial state and disables its timer as described in
[Data pipeline](#data-pipeline). Phase 12
implements the control schema and broker durability boundaries; production state exists only
after separately authorized operation. Its account-specific verification and rollback are recorded
in [Live runtime](#live-runtime), including preservation of journal, authorization, and dispatch
evidence. Production work minimizes downtime, prefers
a safe non-quiescent alternative when one preserves proof and rollback, checkpoints each mutation for
resumption, verifies the cause-specific result, and retains rollback.

## Closeout reporting

Every plan and delivery report states production operator tasks and linked matching Sentry issues,
using `none` where evidence proves none. Reporting does not create a Sentry project or integration.
An already linked matching Sentry issue is closed only after deployed proof, with no waiting period.

Production operator tasks: Phase 12 rollout and rollback above, under separate authorization;
none executed by this documentation change.

Linked matching Sentry issues: none.
