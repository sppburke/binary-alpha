---
name: feature-spec
description: >-
  Plan an unbuilt Binary Alpha experiment, feature, fix, refactor, migration,
  roadmap package, or operator change as one independently reviewed,
  implementation-ready GitHub issue. Use when the endpoint is a durable
  implementation handoff. Do not implement, review completed code, execute
  research or certification, access holdout data, or operate a broker.
---

# Feature Specification

Endpoint: exactly one of (1) a reviewed filed issue, (2) an existing owning issue or pull request, (3) an evidence-backed no-change result, or (4) the exact reviewed title and body plus the filing blocker. Then stop. `AGENTS.md` applies without restatement.

## Boundaries

Planning authorizes read-only repository, hosted Git, and material external-contract discovery. It does not authorize implementation, experiment or certification execution, holdout access, broker contact, production mutation, deployment, migration, or deletion. Ask the user only when repository evidence cannot resolve a choice that changes behavior, authority, persistence, an external contract, rollout, verification, or consequential risk.

## Inspect and classify

1. Derive the repository from `origin`. Record `HEAD`, clean-tree state, local tracking state, and hosted `main` without confusing one for another.
2. Read the request, `AGENTS.md`, relevant checked-in contracts, current owners and consumers, configuration, schemas, tests, documentation, history, issues, pull requests, and comments. Inspect current official external documentation only when an external contract is material.
3. Classify the work as `experiment`, `infrastructure`, `operational or migration`, `roadmap`, or justified `mixed`. Apply only relevant gates.
4. Answer exactly: **Can the goal be accomplished with the tools and data already available?** Name the strongest simpler repository-native alternative and the strongest complete alternative. If an existing owner already satisfies the intent, return the owning issue, pull request, command, or evidence-backed no-change result.

## Resolve evidence before design

Build two complete inventories:

- A material-claim ledger for every premise that can change behavior, architecture, data, research validity, authorization, operations, scope, or proof. Precommit a falsifier and resolve each claim as supported, contradicted, or evidence gap from the highest applicable authority.
- A named-surface inventory for every path, symbol, type, command, configuration key, schema, package, artifact, service, external contract, material numeric claim, and citation. Check set claims per member or with a batch falsifier that records every result.

Do not stop after finding one defect. Finish both inventories. Report genuinely unavailable proof as `Checked / Showed / Unknown / Needed`; close every avoidable evidence gap before filing.

## Choose the minimum complete design

Find the semantic owner and every affected consumer before proposing a surface. Fix the evidenced class within the observed blast radius rather than hard-coding an example. Justify every new durable path, public type, command, configuration field, schema, package, artifact, service, queue, store, or operator state by a net-new behavior, data shape, external contract, performance invariant, or operational boundary.

Compare the chosen design with:

1. the strongest simpler complete reuse of existing owners and tools; and
2. the strongest complete class-level alternative when the proposal may be underfit.

Delete unsupported hardening, fallback, redundancy, compatibility machinery, placeholder, and unrelated cleanup. Simplification inside the proved blast radius is in scope when it reduces parallel ownership.

## Apply proportional gates

Resolve the applicable union of:

- revision authority, semantic ownership, consumers, configuration, schemas, history, and external contracts;
- Rust public surface, dependency direction, concurrency, unsafe or foreign-function boundaries, and package ownership;
- causal time, provenance, replay parity, research splits, selection freeze, and holdout isolation;
- broker, instrument, account, currency, WebSocket, order-state, settlement, money, risk, and reconciliation behavior;
- Google Cloud Storage, Supabase, publication, idempotency, resumption, recovery, and secret boundaries;
- NVIDIA CUDA reference parity, determinism, device evidence, and unavailable-hardware posture;
- per-member validation for every named set; and
- operator target, prerequisites, downtime, non-quiescent choice, resumable checkpoints, verification, rollback, region restriction, and linked Sentry disposition.

Do not prescribe a domain mechanism when current evidence or the requested blast radius does not require it.

## Produce one implementation-ready plan

Prefer one phase. Add another only for a proved independently deployable, migration, compatibility, authorization, or blast-radius boundary. Each phase must be coherent and independently verifiable; never use a follow-up phase to defer an avoidable decision.

The issue must contain:

- intent, user-visible or operator-visible after-state, evidence, resolved decisions, and explicit non-goals;
- exact owned paths and contracts, ordered implementation steps, reuse points, affected consumers, and failure behavior;
- acceptance criteria traced to commands, inputs, asserted outputs, and retained evidence;
- plan-kind rejection or do-not-ship gates, rollback, and cross-cutting impact;
- either `none` or exact post-production operator tasks and linked matching Sentry issues; and
- rejected alternatives and filing metadata.

For runtime-changing work, each goal-bearing criterion names the proposed exact non-live command, representative inputs, affected owners and consumers, and asserted outputs. Imports, unit results, and exit status alone are insufficient.

For every proposed third-party package, record authorization, necessity against the standard library and current packages, owning manifest and lockfile changes, and the exact future compile or import acceptance proof. A plan review requires this planned proof, not a result from code that does not exist yet.

Experiments precommit the hypothesis, dataset roles and splits, falsifier, selection freeze, and terminal result. Infrastructure names its real integrating consumer, failure and recovery behavior, rollback, and goal-bearing integrated proof; scaffold-, import-, or exit-status-only success is rejected. Operational or migration work specifies exact targets, preflight, idempotent checkpoints, safe resumption, downtime or non-quiescent choice, verification, and rollback. Roadmaps give every phase a meaningful acceptance boundary and add no placeholder machinery. Mixed work takes the applicable union.

## Review and file

Apply the current runtime's independent-review rule to the complete draft. Bind the reviewer by path to this repository's `.agents/skills/plan-review/SKILL.md`; a bare same-name selector may resolve elsewhere. The review must exclude the desired verdict. Verify every finding, consolidate accepted deltas into one body, and obtain `approve` for the exact fingerprint after every material revision.

Immediately before filing, recheck open and closed issues and pull requests for an owner or duplicate. Print the exact reviewed title and body, transport the body byte-safely through standard input or a restricted temporary file, and verify the hosted title and body. Ambiguous creation output requires an exact-title query before any retry. Apply labels, assignee, milestone, or project only when one unambiguous repository convention exists.

Return the filed link or another defined endpoint and stop. Do not implement the issue.
