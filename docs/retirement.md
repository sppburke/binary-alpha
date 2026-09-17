# Pipeline retirement

`binary-alpha data pipeline retire --config PIPELINE [--job ID] [--plan]` creates a plan.
Omitting `--plan` has the same effect. `--apply PLAN_FILE` applies or resumes that exact plan;
it conflicts with `--plan`. Supplying `--job` with apply must match the sealed plan's sole job.
Planning performs reads and writes its immutable plan, without deleting data.

Both modes hold the managed store's writer lock and a host-wide lock for the Drive endpoint
and archive-root pair. Pipeline producers, pull, and restore share this archive lock, including
restores to another managed store on this host. This is not a distributed lock across hosts.

The implementation reuses `Store`, `Drive`, and `data verify`; it does not duplicate their
storage or daily decoding rules. A job becomes eligible only when a verified daily dataset
and matching stream have an archived catalog. The newest v2 catalog is chosen by the shared lineage selector: coverage end, then proved
ancestry at equal coverage; ambiguous branches are refused. Its dataset ancestry must reach the instrument's one
daily continuation root with `provenance/lineage.json`. That immutable mapping is migration's
authority for which legacy generations were replaced; retirement independently verifies the
daily storage closure, but does not repeat migration's row/occurrence equality proof.

Retained roots include the continuation root and its stream, the newest eligible catalog and its
exact remote file bindings, configurations, pending acquisitions and pages, and in-flight
transfers. Proven superseded v2 descendants, streams, catalogs, and replaced partial-day
objects are candidates only when no retained root needs them. All other jobs and non-pipeline manifests retain their dependencies. A legacy
generation outside the mapping remains protected. Completed records are inventoried and
remain on disk even when their mapped closure is retired. Receipt-only pages with no mapped
manifest closure are protected; an unknown record dependency protects its resolved closure.
Archived `records/` entries are immutable and retained.

Configuration inventory scans the pipeline document, every declared job configuration, and
TOML files beneath the document's directory, excluding the managed data root. Mutable pending
state and registry/transfer JSON or newline-delimited JSON are inventoried. Legacy
`transfers.json` and registry entries use content keys with `file_id` and Boolean `done`;
unfinished catalog transfers pin both generation identities before the catalog exists.
Unresolved or torn state fails closed. Symlinks in scanned trees are refused.

Plans live at `pipeline_state/retirement/plan-SHA256.json`, where SHA256 hashes the exact
serialized bytes. Schema version 1 contains:

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
governance, remote inventory, or surviving deletion targets. Catalog bindings must belong to
the complete root listing. Missing checksums are established by readback. The only permitted
missing deletion targets on resume are operations authorized by durable progress.

Drive deletions precede local deletions. Each file's size/digest and recorded name are checked
before deletion; 404 is successful resumption. Local removal unlinks only inventoried files
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

The retirement integration tests use deterministic multi-day data and a paginated loopback
Drive server, including fresh-store restoration and verification. They do not run migration,
broker acquisition, or production deletion. Expanded catalog publication and migration
equality are owned by the separate migration/archive modules.

Production operator tasks for this code delivery: none. Matching linked Sentry issues: none.
