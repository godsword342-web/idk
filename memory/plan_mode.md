# Pyramid Implementation Plan v2

Updated: 2026-11-11. This is the active implementation roadmap; older research numbers are hypotheses, not acceptance thresholds.

## Goal

Build the provider-agnostic three-layer system from the research: a capable L1 planner/orchestrator, isolated configurable L2 workers, and deterministic L3 tools/workflows. Keep the system useful while advancing phases: new behavior stays opt-in until its tests and exit criteria pass. Verification is a feature to implement and measure, not a reason to stop all other work.

## Reconciled Design Decisions

- **L1 owns decomposition.** The planner with global context emits task contracts, dependencies, boundaries, and join criteria. Do not add a separate divider model.
- **L2 workers are isolated.** Fresh task context, only granted tools, no parent-plan writes, and no nested Task calls by default. Results are validated envelopes, concise summaries, and artifact references, not transcripts.
- **L3 is deterministic.** Scheduling, rate limits, budgets, permissions, locks, schemas, and event recording stay in code.
- **Single is the default.** Wide is a per-node mode only when there are at least three useful independent branches and the work is read-heavy or has demonstrably disjoint writes. Sequential, tightly coupled, or tool-heavy work remains single.
- **Recursion is evidence-triggered recovery.** Explore in L1 first; re-decompose only after validation failure, tool-error budget exhaustion, or verifier evidence. Start with depth 2. Do not make recursion or fan-out unconditional.
- **One writer per resource.** Use explicit path/resource scopes. Serialize writers; add staging/worktrees before parallel code changes.
- **Keep model IDs in configuration.** Routing policy, contracts, tests, and event schemas must not hard-code vendor/model names. Each benchmark run records the IDs actually used. Model changes require a new matched experiment, not an architecture rewrite.
- **Use a local blackboard/event log, not agent chat.** SQLite plus an in-process queue fits this single-process runtime. Redis Streams is a later multi-process option; Redis Pub/Sub is not a durable control log.
- **Speculation is late and read-only.** If evaluated in Phase 4, a cheap proposer may launch only side-effect-free work; its results cannot become authoritative before confirmation.

The sources disagree about whether recursion should be primary, whether a fixed 15-minute spawn cutoff is useful, and whether depth should be 2 or 3. Use the detailed spec's single-default gate and depth 2. Measure alternate thresholds later; do not encode fixed paper/vendor results as facts about this workload.

## Current State

- **Phase 0:** A 30-task repo-derived set ran in three configurations (90 live trials), and 12 outputs across four task shapes received human scores. Automated acceptance: single-T2 15/30, single-T3 20/30, Task-enabled T3 19/30. On the 23 parallel-shaped cases, Task-enabled T3 was 12/23 versus 13/23 for single-T3, with lower mean automated quality (0.681 vs 0.725), more latency (382s vs 267s), and more tokens (244,373 vs 177,450). Cost is unknown and not the current objective.
- **Important Phase 0 limitation:** That live report predates Task/child telemetry. It cannot confirm which Task calls ran or whether children used configured worker tiers. The runner now records Task invocations, child requests/model IDs, and lifecycle counts. Run a matched telemetry-enabled set before claiming a Pyramid or subagent result.
- **Phase 1 foundations present:** deterministic split gate, validated Task DAG, configurable model tiers, per-node budgets, output-schema validation, file scopes and process-local leases, provider concurrency and opt-in RPM/TPM limits, Phase 0 runner/reviewer, child telemetry, and SQLite Task lifecycle events.
- **Phase 1 gaps:** telemetry is not a resumable event-sourced scheduler; lifecycle writes are not transactionally coupled to state changes; there is no content-addressed/versioned artifact store or idempotent retry ledger; deterministic acceptance remains narrow and command checks are intentionally disabled without an OS sandbox.
- **Phase 2 status:** an opt-in independent fresh-context verifier is implemented on Task workers. It uses a distinct configured model, Read-only access, bounded verifier budgets, and a typed verdict; rejection fails the node and blocks dependents. It has offline integration coverage but has not been quality-calibrated live. Join consistency, automatic tier escalation/advisor, bounded failed-leaf re-decomposition, and worktree/staging integration remain gaps. Existing file leases are process-local.
- **Phase 3/4:** versioned tool/workflow quarantine and promotion, safe sandboxing, speculation, and learned routing are not implemented.
- **Security boundary:** path and permission checks are application controls, not OS isolation. Bash and generated tools must not be exposed to benchmark workers until a real sandbox exists.

## Implementation Sequence

Advance implementation and evaluation in parallel. Do not wait for a perfect benchmark before building the verifier or persistence controls; keep each new capability opt-in until its focused tests pass. Do not promote wide execution as the default until a worker-verified matched evaluation shows a repeatable quality-preserving efficiency gain.

### Phase 0 Closure and Calibration

1. Rerun the same task set with the new child telemetry. Keep T2/T3 model IDs fixed for the matched comparison and record them in the report.
2. Confirm Task invocations, child request counts, child model IDs, worker lifecycle outcomes, tokens, and latency are present for Task-enabled runs. If workers do not execute, fix task prompts/gate eligibility before interpreting efficiency.
3. Add gate-eligible three-branch cases and representative real coding-agent tasks. Keep sequential controls to catch regressions.
4. Review the current negative efficiency result by shape; do not change the default split policy based on one corpus or a single model pair.

**Exit:** complete matched results with non-null Task telemetry, human review across shapes, and a report that distinguishes “not measured” from zero. Efficiency means quality-preserving latency/token improvement; price is optional and unknown prices remain null.

### Phase 1: Orchestration Core Completion

1. **Contracts and boundaries:** define a schema version for task, event, result, verifier verdict, and artifact reference. Validate on producer and consumer boundaries; reject incompatible major versions.
2. **Budgets and routing:** preserve complexity-to-tier rules as configuration, apply per-node and per-run limits, and validate provider RPM/TPM behavior with observed usage. Current token admission is approximate, so keep limits configurable with headroom.
3. **Durable runs:** move Task events toward a versioned append-only run log with explicit run/event IDs and timestamps. Make state transitions recoverable; add idempotency keys before retrying side effects. SQLite WAL is sufficient for the current single-process deployment.
4. **Artifacts:** store output bytes by content hash, track size/hash/version, and pass references rather than file contents through planner context. Preserve readable workspace copies for integration and review.
5. **V0/V1 validation:** require valid result envelopes and deterministic checks before dependents start. Expand safe checks for response, JSON/schema, and confined file outputs. Do not execute arbitrary model-authored commands without an OS sandbox.

**Exit:** deterministic tests prove invalid contracts/results never reach dependents; provider limits and budgets are exercised with a fake clock/provider; event and artifact references survive process restart; retries cannot duplicate a declared side effect.

### Phase 2: Verification and Recovery

1. **V2 verifier hardening:** the opt-in fresh-context verifier now exists with a configurable model ID distinct from the worker route, Read-only access, typed verdict, and per-verifier budgets. Expand tests for malformed verdicts, rejection, timeout/cancellation, candidate-content prompt injection, and verifier unavailability. Keep it opt-in until these pass and live quality is measured.
2. **Join verification:** compare dependency outputs against frozen interfaces and declared schemas before synthesis. A failed join blocks the join node and records the reason.
3. **Evidence-based escalation:** retry or raise tier only for typed schema/acceptance failures, verifier rejection, loop detection, or tool-error budget exhaustion. Bound attempts and preserve attempt IDs/usage.
4. **ADaPT recovery:** permit re-decomposition of failed leaves only, with depth cap 2 initially. Children cannot spawn children unless a future explicit capability and depth policy says otherwise.
5. **Write isolation:** add staging/worktree integration for code-writing DAGs. Keep one integrator as the only writer to the shared branch; use prefix/resource locks and stale-read checks.

**Exit:** adversarial fixtures demonstrate that malformed, incorrect, or inconsistent worker results are blocked; verifier false accepts/false rejects are measured; failed leaves escalate within the bound; cancellation leaves no untracked writes; sequential tasks remain single-agent.

### Phase 3: Reusable Tools and Workflows

Add a versioned manifest (schemas, capabilities, tests, owner, status), session-only quarantine, sandboxed tests without secrets, explicit review/promotion, and pinned versions. Never permit self-promotion. Mine event history for repeated sequences only as suggestions.

**Exit:** a candidate passes sandboxed tests, is reused successfully across distinct tasks, does not exceed declared capabilities, and is explicitly approved. Otherwise keep it session-scoped or discard it.

### Phase 4: Speculation and Learned Routing

Only after stable event data and earlier phases show value: compare routing against fixed-tier baselines; try a cheap proposer; permit speculation only for read-only, cancellable work whose output cannot commit before confirmation. Run ablations for verifier, escalation, fan-out, and router. Keep all routes model-configurable.

**Exit:** each added layer demonstrates a repeatable quality-preserving efficiency improvement on held-out tasks. Remove layers that add latency/tokens without measurable quality benefit.

## Industry-Grade Readiness Gate

Do not call this production/industry-grade until OS-level sandboxing and secret isolation exist, durable state can resume safely after a crash, event/artifact access is audited, verifier and escalation failure modes are tested, resource locks survive concurrent processes, provider budgets are observed/reconciled, and the benchmark includes real target workloads. The current project is a capable prototype with active Phase 1 work, not a finished production agent platform.

## Immediate Next Work

1. Run the full telemetry-enabled matched benchmark; the existing live report cannot prove worker execution.
2. Implement content-addressed artifact storage and typed/versioned run events with restart tests.
3. Harden the implemented opt-in verifier; add the join-consistency gate and tests that invalid, rejected, or inconsistent results cannot reach dependents.
4. Add staged/worktree integration before permitting code-writing branches.
5. Reassess the roadmap from the new telemetry and human-reviewed results; keep fan-out opt-in meanwhile.