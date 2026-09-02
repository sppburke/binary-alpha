---
name: dev-cycle
description: >-
  Deliver one exact approved Binary Alpha issue through verified squash merge
  to hosted main. Use for implementation, shipping, or dev-cycle requests bound
  to an approved consolidated issue. Do not use for planning, standalone
  diagnosis or code review, research or certification execution, broker
  operation, deployment without separate authorization, or local-only edits.
---

# Development Cycle

Default endpoint: verified hosted merge and conservative cleanup.

`analyse -> plan gate -> isolate -> implement -> verify -> exact-diff review -> one pull request and current-head checks -> resync -> expected-head merge -> verify hosted main -> cleanup`

`AGENTS.md` applies without restatement. An explicit user stop boundary pauses this lifecycle at that boundary; preserve the branch, worktree, pull request, and evidence needed for inspection or resumption and perform no later action.

## Bind and refresh

1. Read the exact consolidated issue body, every comment and review receipt, related pull requests and review threads, and relevant checked-in contracts.
2. Record the artifact fingerprint, primary-checkout path, branch and status, `HEAD`, local tracking state, and hosted `main`. Fetch hosted `main`, then inspect recent merges, open pull requests, remote branches, and worktrees for satisfied, conflicting, or overlapping work.
3. Reuse approval only when the body is unchanged and refreshed owners, consumers, contracts, current main, issue state, and verification inputs do not falsify it. Route a rejected, blocked, stale, or unconsolidated plan to `.agents/skills/plan-review/SKILL.md`.
4. Classify the planned diff as documentation, Rust, tooling, or the applicable union. Confirm the requested delivery endpoint and every separately authorized external action.

If current evidence shows the task already satisfied or obsolete, report the owner or proof and stop without a branch.

## Isolate

Create a sibling worktree and a dedicated feature branch from the exact recorded hosted base. Verify the merge base, clean worktree, and semantic and file overlap; coordinate or resolve any contested surface before editing. Use one primary writer; helpers remain read-only. Preserve the primary checkout exactly.

## Implement the minimum complete change

Implement one coherent phase unless the approved issue proves an independently deployable, migration, compatibility, authorization, or blast-radius boundary. Extend the semantic owner and all affected consumers; reuse current healthy patterns; fix the evidenced class rather than one example; do not broaden the approved scope silently.

Add focused deterministic coverage beside each changed behavior. Keep fixtures, clocks, random sources, network inputs, and storage isolated and reproducible. Never commit secrets, live configuration, protected data, generated credentials, or unrelated workspace changes.

A newly required package must be authorized by the approved issue, owned by checked-in manifests and the lockfile, and verified at the reviewed commit. Incidental defects may enter this cycle only after consolidation and approval when they remain inside the proved blast radius; otherwise file them separately and do not implement them here.

## Derive and run gates

Derive exact commands from checked-in task tooling, manifests, toolchain, lockfile, continuous-integration configuration, and the approved issue at execution time. Never invent a command or claim a nonexistent gate.

Run focused checks first, then the applicable union of documentation, Rust, application-programming-interface, asynchronous, foreign-function, causality, replay, holdout, NVIDIA CUDA, broker, WebSocket, financial, storage, configuration, migration, and scenario gates. Formatting precedes the final code gate. Any later change invalidates affected evidence.

Ordinary tests never access live, locked-holdout, or production systems. Use deterministic fixtures, fakes, emulators, recorded frames, or an explicitly authorized sandbox.

Runtime-changing work runs the issue's plan-named non-live integrated proof before publication. Inspect the asserted goal-bearing outputs through every affected owner and consumer; an exit status alone is insufficient. A required goal-bearing gate must pass. An implementation failure returns to implementation; deficient or stale proof returns to plan review.

For documentation and skill-only changes, run the exact structural, link, frontmatter, content, discovery, and routing checks named by the issue plus `git diff --check`. Do not run a Rust gate merely because the workflow advanced.

Record the exact commit and clean-tree state, command, result, configuration, fixture or dataset identity, environment or device, and retained artifact for each required gate. A failure is pre-existing only when reproduced on an unmodified disposable worktree at the same hosted revision and environment.

## Commit and independent review

Commit only intended paths with a concise imperative message; keep the worktree clean. Review the complete `origin/main...HEAD` diff independently at that exact commit for:

- correctness, intent, failure behavior, and affected consumers;
- semantic-owner and repository-pattern reuse, boundary direction, and simplification;
- protected state, secrets, generated output, and third-party-package compliance;
- verification completeness and operator or deployment effects; and
- applicable causal, holdout, replay, NVIDIA CUDA, broker, financial, storage, concurrency, and live-safety invariants.

Each finding states claim and impact, falsifier, cited diff or test evidence, and minimum fix. Disposition it as fixed, declined with evidence because it is wrong, intended, out of scope, or over-engineered, or deferred only when genuinely outside scope. Effort is not a reason. Re-run affected gates and review each unreviewed delta; restart full review after a material scope, architecture, shared-contract, or base-diff change.

## Synchronize and publish

Fetch hosted `main` immediately before push. If it advanced, integrate it semantically, inspect the incoming range, and rerun every affected gate and review, including integrated runtime proof when applicable.

Bind verification and review to one clean candidate commit. Before push, prove hosted `main` is its ancestor and the verified and reviewed commit identifiers both equal `HEAD`. Push the feature branch without force.

Query for an existing pull request for the exact head before creating one. Use byte-safe body transport. The pull-request body includes the source issue, behavior and rationale, scope classification, exact files, commands and results, scenario disposition, exceptions, review outcomes and reasoned declines, replay and financial impact, external documentation checked, deployment and configuration impact, rollback, and `Shortcuts / hacks taken: none` or the exact exceptions. Verify returned base, head commit, state, draft status, title, body, and URL.

## Checks, resynchronization, and merge

Wait for every expected hosted check on the current pull-request head. Inspect and fix failures, rerun affected local gates and review, push under the same guard, and wait again. Read every new issue or pull-request comment, review, and inline thread; disposition every finding.

If no workflow exists and hosted pull-request checks report none, record exactly `no automated checks configured`; never describe that state as green.

Immediately before merge, fetch hosted `main` again. If it advanced, integrate it and repeat affected verification, review, push, and hosted checks. Verify the pull request is open, non-draft, targets `main`, and its remote head equals the reviewed and verified head. Squash merge with an expected-head guard. A nonzero or ambiguous merge result requires inspecting hosted truth; never retry blindly.

## Verify main and clean up

Confirm the hosted merge commit is on `main` and wait for that commit's expected workflow. Do not treat an unrelated run as proof. If the workflow fails, preserve evidence and run only an authorized scoped correction cycle.

After verified hosted main, update and close the source issue and remove only proven task-owned worktree and branches. Closeout explicitly states operator tasks, downtime, non-quiescent choice, resumable checkpoints, region, rollback, and linked Sentry disposition, using `none` where proved. Refresh the primary checkout only by a clean fast-forward. Never reset, force-push, force-remove a dirty worktree, or delete ambiguous state.

## Failure boundary and report

On identity, authentication, check, review, merge, or clean-tree ambiguity, preserve the branch, pull request, worktree, and evidence. Report `Checked / Showed / Unknown / Needed` and stop; do not fabricate success or broaden scope.

Report the pull-request URL, delivered behavior, exact verification, review findings and dispositions, replay and financial impact, deployment and operator closeout, hosted-main state, cleanup state, and `Shortcuts / hacks taken: none` or exact exceptions.
