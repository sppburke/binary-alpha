# Pipeline retirement

`binary-alpha data pipeline retire --config PIPELINE [--job ID] [--plan]` creates a plan.
Omitting `--plan` has the same effect. `--apply PLAN_FILE` applies or resumes that exact plan;
it conflicts with `--plan`. Supplying `--job` with apply must match the sealed plan's sole job.
Planning performs reads and writes its immutable plan, without deleting data.

Both modes hold the managed store's writer lock and a host-wide lock for the Drive endpoint
and archive-root pair. Pipeline producers, pull, and restore share this archive lock, including
restores to another managed store on this host. This is not a distributed lock across hosts.
Apply seals an archive reservation beside the shared lock before creating its progress journal
and before retained verification or deletion. The reservation binds the canonical owner plan
and its SHA-256; every store checks it under the same archive lock. An immutable store binding
also fences the owner if a crash occurs before its local journal is created. Reservation seals
use atomic publication with file and directory sync; torn temporary files grant no ownership.
Only verified completion releases the reservation, including recovery after completion was
sealed but the process stopped before release. Until matching verified `.retired.json` exists,
other producer commands and new retirement plans refuse with the unfinished plan's path. Empty and torn
journals also fence the store. Only the identical sealed plan may resume. Keep configurations
and all writers on other hosts frozen through completion; the persistent fence is local.

The implementation reuses `Store`, `Drive`, and `data verify`; it does not duplicate their
storage or daily decoding rules. A job becomes eligible only when a verified daily dataset
and matching stream have an archived catalog. The shared lineage selector chooses the newest
v2 catalog by coverage end, then proved ancestry at equal coverage; ambiguous branches are
refused. Its dataset ancestry must reach the instrument's one readable daily continuation
root with `provenance/lineage.json`. Proven superseded v2 ancestry is independently eligible.
An ancestry name is considered v2 only after checking its local manifest or its archived,
hash-pinned manifest; a descendant cannot expand the root's verified v1 replacement mapping.

Retiring local legacy manifests additionally requires a completed immutable migration record
under `pipeline_state/records` matching the root's mapping. Lineage names and coverage
containment alone grant no v1 deletion authority. Other legacy streams sharing a replaced
source remain protected unless explicitly included in the verified mapping. Native v2 roots need no migration record;
a fresh store without legacy manifests can retire proved v2 ancestors while preserving any
unresolved legacy archive closures.

The migration evidence boundary requires JSON fields `schema_version: 1`, `job`,
`phase: "verified"`, `v1_generations` (the exact legacy dataset identities in root lineage),
`v1_stream` (the newest legacy stream, or null when none exists), `v1_streams` (all mapped
legacy streams), `v2_root`, and `v2_stream` (a retained
daily stream sourced from that root). Its `equality` object must contain Boolean `true` for
`observations` (ordered rows and multiplicity), `pages` (every occurrence and alias accounted
for), and `source_files` (byte-exact import NDJSON and checkpoint reconstruction). `candles`
must be `true` when a legacy stream exists, or `null` (not applicable) when the mapping has
no legacy streams. Absence of a comparison is never reported as measured candle equality.
When local legacy replacement is requested, missing, converted, failed, or mismatched evidence
refuses planning. The shared `lineage::MigrationRecord` is emitted by `migrate` only after its
ordered observations, occurrence census, source reconstruction, candles, profile, and integrated
`data verify` checks succeed. Full measured proofs accompany the equality summary. The retained
v2 catalog must archive the exact receipt, alias table, and named immutable source records.
It also retains the receipt's exact v2 stream and its objects if a later configuration selects
a different stream definition, so fresh restoration preserves the verified comparison target.
A verified migration checkpoint is completed evidence only when it matches that archived receipt;
converted or mismatched checkpoints and unresolved pending acquisitions remain protected.
Hash-bound alias entries authorize retirement of replaced standalone page copies as well as bundles;
unmapped receipt pages remain protected. The verified receipt's `storage_aliases` additionally
names byte-identical standalone page keys absent from manifest lists. These keys are local
candidates, still protected by pending acquisitions and by receipt occurrences outside the
selected migration census. Sharing a content hash with a migrated occurrence does not exempt
another instrument, job, or unproved receipt from retaining its required source. They do not create additional response occurrences. The receipt's
`predecessor_jobs` extends ownership to historical catalogs and records of those jobs;
only its verified generation mapping grants generation deletion authority. Historical records
and completed transfer metadata remain intact, with retired closures marked in the plan.
Only fields from the exact verified receipt bound into the retained catalog grant this authority.

Proof-version upgrades rebuild the root from the strict v1 proof plus the existing daily
continuation. Re-proving only the v1 root would lose subsequent acquisitions; rewriting old
roots or descendants would break immutable identities. The converter therefore keeps the
strict v1 observation/page/source-file/candle equality proof, names its baseline manifests,
and separately measures continuation preservation before publishing the superseding receipt.
It selects the latest observation-complete daily history and keeps its observation keys and
coverage (including the current acquisition, shortfalls, and unresolved ranges). It unions
page occurrences by acquisition and ordinal across all former roots and descendants, rejecting
conflicting metadata. Equal day contents reuse the existing key. Audit replays the continuous
stream using the latest parent stream to reuse unchanged candle partitions.

Every former observation and finalized candle day must be an ordered subsequence of its
replacement, including repeated rows and all provider columns; incomparable histories stop
before supersession. Every predecessor receipt's named daily stream must be present and
verified. The published page partitions are independently reread to prove every former page
occurrence survives exactly. The immutable
`continuation_preservation` proof binds the old root, new dataset/stream, and each covered
former dataset/stream closure. Selection checks that proof; retirement requires its exact
closure bindings before a former migration catalog becomes a candidate. Unproved catalogs
remain retained. Legacy supersession records without this proof can enter only migration's
full-chain repair census, with all former roots present locally; they cannot authorize
ordinary selection or retirement. Completed records are never rewritten.
After restoration, a superseded receipt and its bound alias table receive the migrated-source
exemption only when the selected preservation proof covers that receipt's exact root and stream.

Receipt resolution authenticates daily page closures as well as v1 bundles and standalone
payloads. Daily aliases name the page object, day, acquisition, and ordinal, so verification
never needs to recreate a reclaimed standalone object. A worker caches one authenticated
page day and discards that cache at the independent proof boundary and on every job exit.

Retained roots include the continuation root and its stream, the newest eligible catalog and its
exact remote file bindings, configurations, pending acquisitions and pages, and in-flight
transfers. Proven superseded v2 descendants, streams, catalogs, and replaced partial-day
objects are candidates only when no retained root needs them. All other jobs and non-pipeline manifests retain their dependencies. A legacy
generation outside the mapping remains protected. Completed records are inventoried and
remain on disk even when their mapped closure is retired. Receipt-only pages with no mapped
manifest closure are protected; an unknown record dependency protects its resolved closure.
Archived `records/` entries are immutable and retained.
Local reachability uses content keys; remote reachability uses exact file IDs from retained
catalogs and in-flight transfers. Retaining one remote copy never protects an obsolete duplicate
with the same content key. A pending local page alone does not retain an obsolete remote copy.

Configuration inventory scans the pipeline document, every declared job configuration, and
TOML files beneath the document's directory, excluding the managed data root. Mutable pending
state and registry/transfer JSON or newline-delimited JSON are inventoried. The shared registry
owner replays `registry/snapshot.json` and `registry/events.ndjson`, including watermarks,
removals, logical aliases, and imported legacy bindings. Legacy
job-level `transfers.json` and archive-root `registry.json`, `registry.jsonl`,
`registry.ndjson`, `registry.snapshot.json`, `registry.log.jsonl`, or `registry.log` entries
use content keys with `file_id` and Boolean `done`; registry identity is relative to
`pipeline_state`, independent of ancestor directory names. Acquisition progress references
are inventoried separately. An archive-root `transfers.json` is also supported;
unfinished catalog transfers pin both generation identities before the catalog exists.
Unresolved or torn state fails closed. Symlinks in scanned trees are refused.

Plans live at `pipeline_state/retirement/plan-SHA256.json`, where SHA256 hashes the exact
serialized bytes. Schema version 2 contains:

| Field | Meaning |
| --- | --- |
| `store`, `archive_root`, `jobs` | Exact application scope |
| `state` | Absolute manifest, record, mutable-state, configuration and governance file paths mapped to byte count and SHA-256 |
| `remote_state` | Complete archive-root listing keyed by Drive file ID, including name, size, checksum when available, and trash state |
| `references` | Source document, referenced closure, `protected`/`retired`, and reason |
| `retained_manifests`, `retained_objects`, `retained_drive` | Verification roots, content identities, and exact retained remote bindings |
| `delete_drive` | File ID, logical key, checked name, byte count and SHA-256 for every remote deletion |
| `delete_local` | Store-relative manifest directory or content key, with every exact member path and byte identity |
| `totals` | Manifest-directory and object counts, local bytes, Drive-file count and Drive bytes |

Apply refuses changes to the sealed plan, scope, manifests, records, registry, configuration,
governance, remote inventory, or surviving deletion targets. Apply rejects schema-1 plans,
including unfinished ones, because they predate the migration and pending-reference safety
checks; produce a new plan under the current checks. Completed historical records remain
readable and immutable. Catalog bindings must belong to
the complete root listing. Missing checksums are established by readback. The only permitted
missing deletion targets on resume are operations authorized by durable progress.

Drive deletions precede local deletions. Each file's size/digest and recorded name are checked
before every DELETE attempt, including transient, authentication, and lost-reply retries;
the latest confirmation metadata must still match and be untrashed. A 404 is successful
resumption. Local removal unlinks only inventoried files
and then empty manifest directories. Batches contain at most 32 operations, never crossing
the Drive/local boundary. Full retained verification runs before application/resumption and
before completion. Each batch checks impact by exact paths (including manifest-directory
descendants) and remote file IDs and reverifies any retained closure it touches. Reachability
plans must have disjoint retained/deletion inventories, so normal batches touch none; this
avoids repeatedly decoding the whole store for each 32 unreachable files.

`plan-SHA256.progress.jsonseq` is append-only: ASCII record separator (`0x1e`), one JSON event,
and newline. Events contain `plan`, zero-based `index`, and `phase` (`begin` or `done`). A
durable `begin` precedes mutation. A torn trailing frame stays untouched; a complete frame
continues the operation on resume. `plan-SHA256.retired.json` is an immutable completion
record containing `plan_sha256`, `removed_drive`, `removed_local`, and `retained_verified`.
Subsequent inventories recognize historical retired references from these completed plans.
Seal scratch files use exclusive creation with collision retries, so crash leftovers can
never truncate an inode linked to an already published plan or completion record.

The retirement integration tests use deterministic multi-day data and a paginated loopback
Drive server, including fresh-store restoration and verification. The daily end-to-end test additionally runs real command owners for offline migration,
archive, fresh-store restoration, two updates at the same cutoff, and retirement for multi-day
Deriv and Pocket v1 fixtures. All transport is loopback fake data; no production deletion or
external broker acquisition is exercised.

Production operator tasks for this code delivery: none. Matching Sentry issues: unknown; none supplied.
