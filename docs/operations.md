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
destination; nothing outside the declared inventory is opened. Source files are never moved, renamed,
or rewritten. Every object is retained locally and published under the same content-addressed key;
the ready manifest is published last and mirrored locally. The command is resumable: re-running it
after an interruption at any point reuses identical existing objects, finishes the missing ones, and
completes the local mirror; different content at an existing key stops the command without replacing
either copy. No quiescence is required; readers of the source files continue during import.

Verify: `binary-alpha data verify --manifest URI` re-reads one generation from its ready manifest and
objects alone, from either the destination or the retained mirror.

Rollout to Google Cloud Storage: discover and reuse existing projects, buckets, identities, and
regions first; create nothing in a region whose name begins `us-west`; provision the bucket and a
least-privilege identity that can read and create objects but not create or delete buckets, outside
the application; then run the import above. Rollback reverts the application and configuration
change; source files, the retained copy, and published generations stay intact.

## Regions

Create nothing in a region whose name begins `us-west`. This applies to every bucket, database,
service, runner, and secret.

## Rollback

A repository change rolls back by reverting its merge commit. Published dataset generations are
immutable and are never deleted by rollback; the retained historical-data folder and the original
source files stay intact. Schema, broker, and other production state do not exist at this phase; the
phase that creates any of them records its own cause-specific verification and rollback before it
ships. Production work minimizes downtime, prefers
a safe non-quiescent alternative when one preserves proof and rollback, checkpoints each mutation for
resumption, verifies the cause-specific result, and retains rollback.

## Closeout reporting

Every plan and delivery report states production operator tasks and linked matching Sentry issues,
using `none` where evidence proves none. Reporting does not create a Sentry project or integration.
An already linked matching Sentry issue is closed only after deployed proof, with no waiting period.
