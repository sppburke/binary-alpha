---
name: plan-review
description: >-
  Adjudicate one exact, not-yet-implemented Binary Alpha plan, specification,
  or GitHub issue for correctness, repository fit, minimum-complete scope,
  executability, and safety. Return approve, approve with revisions, reject,
  or blocked with exact deltas. Do not author or edit the plan, implement it,
  review completed code, access holdout or live systems, authorize deployment,
  or recursively launch plan review.
---

# Plan Review

Endpoint: one evidence-backed verdict and exact deltas for one unimplemented artifact. Never mutate the artifact, repository, hosted issue, external system, or production state. `AGENTS.md` applies without restatement.

## Bind the artifact

Read the consolidated body and every comment. The body is authority; comments are context, evidence, or review receipts until incorporated. Contradictory append-only text is not a coherent plan. No body means `blocked`.

Derive the repository from `origin`. Record the artifact fingerprint, repository `HEAD`, clean-tree state, local tracking state, and hosted `main`. Inspect the current owners, consumers, configuration, schemas, tests, documentation, history, issues, and pull requests rather than asking the user to search.

Classify the plan as `experiment`, `infrastructure`, `operational or migration`, `roadmap`, or justified `mixed`. Normalize intent, observable after-state, authorization, constraints, non-goals, named surfaces, sequence, failure behavior, acceptance, closeout, and rollback as explicit, inferred, missing, or contradicted.

## Complete both inventories before verdict

1. **Material claims:** enumerate every premise affecting behavior, ownership, contracts, data, research validity, financial or live safety, authorization, operations, scope, rollback, or proof. Precommit a falsifier, then resolve each exactly once as `supported`, `contradicted`, or `evidence-gap` from the highest applicable authority.
2. **Named surfaces:** check every path, symbol, type, command, configuration key, schema, package, artifact, output, external contract, material numeric claim, and citation in one sweep. Check a claimed set per member or with a batch falsifier that records every member.

Finish both inventories even after a verdict-determining defect. Report balanced claim counts and resolved/total named surfaces. Record a genuine gap as `Checked / Showed / Unknown / Needed`; absence of proof is never a pass or automatically a defect.

## Test fit, reuse, and proof

Answer exactly: **Can the goal be accomplished with the tools and data already available?**

Trace the applicable path from configuration and command entry through ingress, causal features, strategy or model, chronological execution, settlement, risk, persistence, and replay or live consumers. Enumerate consumers of each changed shared contract.

Test two counterfactuals:

- whether the strongest simpler repository-native owner or tool satisfies the complete goal without the proposed surface; and
- whether omitting the strongest complete class-level design leaves duplicated semantics, an incomplete owner, or inconsistent consumers.

Challenge every new abstraction, package, fallback, duplicate path, migration, operator step, phase, and follow-up. Delete unnecessary surfaces and safeguards unsupported by a binding contract or direct evidence in the current blast radius. Do not require alternatives theater for an already established minimum-complete shape.

A runtime-changing plan without a representative, plan-named, non-live integrated proof through affected owners and consumers to asserted goal-bearing output is Blocking. Imports, unit results, and exit status alone are deficient proof. A proof that is well specified but requires genuinely unavailable environment evidence is an evidence gap, not that defect.

For a proposed third-party package, require reviewed-issue authorization, necessity against the standard library and current packages, the planned checked-in manifest and lockfile owner, and a named future compile or import acceptance proof. Missing any one of these is Blocking. Do not demand post-implementation results while reviewing unbuilt work.

## Apply proportional domain gates

Check only the domains the plan can change:

- idiomatic Rust ownership, dependency direction, public contracts, concurrency, and the smallest safe unsafe or foreign-function boundary;
- one causal feature path and one chronological execution, settlement, accounting, and risk path across research, replay, certification, and live modes;
- causal time, provenance, selection freeze, research validity, and one-way holdout isolation;
- broker, instrument, account, currency, WebSocket, money, order-state, settlement, risk, and reconciliation semantics;
- Google Cloud Storage and Supabase ownership, idempotent publication, resumption, recovery, and secrets;
- NVIDIA CUDA reference parity, determinism, and honest device evidence;
- authorization, operator steps, downtime, non-quiescent alternatives, resumability, region restrictions, verification, rollback, and linked Sentry disposition.

Do not require a Sentry integration. Verify that the plan names only an already linked matching issue or records `none`.

## Findings and deltas

- **Blocking:** a proved material defect in intent, correctness, safety, integrity, contract, scope, rollback, or authoritative verification.
- **Should Fix:** a nonmaterial exact correction to wording, ownership, scope, evidence, or handoff quality.
- **Consider:** a supported optional trade-off or simplification.
- **Evidence Gap:** unresolved proof state, separate from severity.

Every Blocking and Should Fix finding cites claim-matched evidence and gives exactly one checked delta: `add`, `replace`, `delete`, or `move`; target section; superseded exact text or `none`; and exact minimum replacement. Check every delta against reuse, target contracts, user intent, and minimum-complete scope.

Apply verdict precedence after both inventories:

1. any Blocking finding or contradicted material claim: `reject`;
2. otherwise any exhausted material evidence gap: `blocked`;
3. otherwise any Should Fix: `approve with revisions`;
4. otherwise: `approve`.

Approval binds only to the fingerprinted consolidated artifact. `approve with revisions` is not implementation authority: consolidate the deltas and fully re-review the exact body. A comment cannot shadow contradictory body text.

## Independent lens and output

Do not recursively launch another plan review. Use only bounded read-only evidence or adversarial lenses when they materially improve coverage, and verify every finding; agent prose is not authority.

Return coverage and execution mode; artifact and repository fingerprints; verdict; findings with exact deltas; the strongest simpler and strongest complete alternatives; complexity and boundary assessment; and evidence gaps. Do not implement or rewrite the plan.
