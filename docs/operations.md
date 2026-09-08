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

Configuration documents and the repository contain no secret values. Where a later phase needs a
credential, the configuration names a reference and the application resolves it from the process
environment or an authorized secret store at run time. Credentials, broker and account material,
proprietary source data, locked holdout, completed evidence, and production cloud state are protected
state.

## Artifact ownership

Google Cloud Storage owns immutable bulk data and artifacts. Supabase owns only a proved
transactional control or metadata need and stores references, never duplicate bulk or execution
truth. Writes and operator procedures are resumable, so a second agent can continue from the last
checkpoint. A completed evidence identity is never overwritten.

## Regions

Create nothing in a region whose name begins `us-west`. This applies to every bucket, database,
service, runner, and secret.

## Rollback

A repository change rolls back by reverting its merge commit. Data, schema, broker, cloud, and
production state do not exist at this phase; the phase that creates any of them records its own
cause-specific verification and rollback before it ships. Production work minimizes downtime, prefers
a safe non-quiescent alternative when one preserves proof and rollback, checkpoints each mutation for
resumption, verifies the cause-specific result, and retains rollback.

## Closeout reporting

Every plan and delivery report states production operator tasks and linked matching Sentry issues,
using `none` where evidence proves none. Reporting does not create a Sentry project or integration.
An already linked matching Sentry issue is closed only after deployed proof, with no waiting period.
