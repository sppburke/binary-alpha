# Binary Alpha Agent Contract

`AGENTS.md` is the sole physical root contract; `CLAUDE.md` is its relative symbolic link. Physical skill bodies live only at `.agents/skills/<name>/SKILL.md`; `.claude/skills/<name>` contains relative directory links only. Never replace a link with a copy. Skills specialize workflows but cannot weaken the user's instructions, task scope, protected-state rules, financial or live safety, or holdout isolation.

## Workflow routing

- Use `.agents/skills/feature-spec/SKILL.md` when an unbuilt change needs an implementation-ready GitHub issue.
- Use `.agents/skills/plan-review/SKILL.md` to adjudicate one exact, unimplemented plan.
- Use `.agents/skills/dev-cycle/SKILL.md` to implement an approved consolidated issue through verified squash merge to hosted `main`.

Research execution, locked-holdout certification, broker operation, and production operation are not implied by these workflows.

## System identity

Binary Alpha is a Linux-first Rust, configuration-driven binary-options research and execution system. Its core is broker-, instrument-, strategy-, account-, and currency-neutral. Deriv is the first planned strong adapter, not a semantic owner or a default encoded in the core. Broker access uses only authorized application programming interfaces and WebSockets; no browser, Document Object Model, Chrome DevTools Protocol, profile, cookie, or click-execution path may ship.

Checked-in Cargo manifests, once present, own the Rust edition and package graph. The checked-in toolchain and lockfile own exact compiler and dependency versions. Hosted issues establish intended behavior; only the current checkout establishes implemented interfaces.

## Revision and evidence truth

Distinguish specification intent, checkout implementation, observed runtime state, immutable measured artifacts, and hosted Git state. Before revision-sensitive claims, distinguish `HEAD`, the local tracking reference, and hosted `main`. Bind material claims to the exact revision and clean-tree state and, as applicable, environment or device, broker and account class, instrument and currency, dataset and split, configuration digest, and time window.

For each material claim record the claim, primary evidence, precommitted falsifier, and result. Unresolved proof is `Checked / Showed / Unknown / Needed`. Missing credentials, data, hardware, continuous integration, or external state is unavailable, never passing. Artifacts, logs, tests, and helper summaries prove only what the primary inspects directly.

Runtime-changing work runs the reviewed plan's named non-live entry point with representative inputs through every affected owner and consumer to an asserted goal-bearing output. Imports, parsers, unit results, and exit status alone do not prove integration. Every required goal-bearing gate passes before delivery; deficient proof returns to planning, while genuinely unavailable environment evidence is reported honestly.

## Architecture invariants

Use one causal ingestion, data, and feature implementation and one chronological execution, settlement, accounting, and risk implementation across development, evaluation, optimization, certification, replay, and live operation. Strategies and models emit typed intent; only execution communicates with a broker. Mode adapters alter capabilities and input or output, not core semantics.

Preserve provider event time, local receipt time, engine decision time, dispatch time, entry time, due time, and settlement time as applicable. Preserve source identity, ordering and sequence, parser and adapter version, payload identity, and explicit gap, duplicate, stale, reconnect, backpressure, and reconciliation behavior.

## Research and holdout

A configuration-driven research surface may orchestrate historical stages, but candidate and configuration identity freeze before terminal certification. Holdout objects, metrics, artifacts, credentials, observations, pass or fail detail, and retries cannot influence features, tuning, ranking, stopping, defaults, or another iteration. Research success, certification, merge, deployment, paper operation, and live operation are distinct states and authorizations.

## Financial and broker correctness

Use typed currency, amount, price or tick, quantity, probability, payout, fee, foreign exchange, side, timestamp, and settlement values. Money, order, accounting, and risk boundaries use checked decimal, fixed-point, or integer units. Validated floating point is limited to feature, model, and NVIDIA CUDA computation. Define rounding, conversion ownership, foreign-exchange source, time, freshness, reporting currency, and exposure aggregation explicitly. Never retain a global remembered payout.

Distinguish not sent, sent, acknowledged, accepted, rejected, partially filled or open, possibly sent, settled, and reconciled. Never retry an unknown or possibly sent submission without reconciliation.

## Storage authority

For research, Google Cloud Storage is an optional publication location for immutable data and artifacts; choosing it does not grant holdout or certification access. Supabase owns only a proved transactional control or metadata need and stores references rather than duplicate bulk or execution truth. Writes and operator procedures are resumable. Do not prescribe buffering, fallback, retention, or recovery until the implementing issue or current checkout proves the failure model.

A private Google Drive archive owned by this research pipeline may hold ordinary development/evaluation market datasets and their stream outputs under immutable catalogs, and local filesystem publication is supported for all research, including splits, research runs, holdout grants, and certification. Non-research run modes publish to Google Cloud Storage because configuration validation requires it.

## Rust and NVIDIA CUDA

Design idiomatic Rust from contracts rather than translating legacy files. First-party production, replay, command-line, and test paths remain Rust. Isolate required unsafe or foreign-function code behind the smallest reviewed safe boundary. NVIDIA CUDA requires a deterministic central-processor reference and declared parity; never label central-processor evidence as graphics-processor evidence.

## Protected state and authorization

Protect credentials, broker and account material, proprietary source data, locked holdout, completed evidence, and production cloud state. Never overwrite a completed evidence identity. No real feed, session, order, certification, production mutation, migration, deployment, or deletion occurs without authorization for that exact action. Ordinary verification uses deterministic fixtures, fakes, emulators, or an explicitly authorized sandbox.

## Engineering

Preserve intent and record scope changes. Extend the semantic owner and every affected consumer; fix the evidenced class, not one example. Among materially distinct, evidence-supported repository-native options, compare the strongest simpler reuse and strongest complete alternative, then choose the smallest complete resolution that preserves correctness and intent while minimizing new code, ownership, blast radius, and implementation time; do not accept a locally convenient design when another complete option is better on those grounds. Add safeguards, fallbacks, or redundancy only for a binding contract or direct current-revision evidence. Preserve dirty user work, avoid unrelated refactors, run focused then broad gates, and bind proof to an exact clean commit.

A third-party package requires explicit authorization in the reviewed issue, evidence that the standard library and current packages are insufficient, checked-in manifest and lockfile ownership, and compile or import proof at the reviewed commit. A machine-local installation is not portable evidence.

## Operator closeout

Every plan and delivery report states production operator tasks and linked matching Sentry issues, using `none` where evidence proves none. This reporting rule does not authorize creating a Sentry project or integration. Separately authorized production work minimizes downtime, uses a safe non-quiescent alternative when it preserves proof and rollback, checkpoints each mutation for resumption, verifies the cause-specific result, and retains rollback. Close only an already linked matching Sentry issue after deployed proof, with no waiting period.

## Delegation

Nontrivial work uses bounded independent evidence and adversarial review available in the current runtime. The primary verifies every material finding against its source; agent prose is not proof. Keep one primary writer for a worktree and delegate read-only, independent lenses.

## GitHub

Derive the owner and repository from `origin`. Never print or persist credentials or switch authentication globally. Preserve work, branches, pull requests, and evidence when identity, authentication, check, or merge state is ambiguous.
