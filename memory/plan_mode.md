I’ll switch to planning only. I’ll reconcile the three research writeups with the current runtime, call out their conflicting thresholds where they differ, and lay out phases with explicit exit criteria; I won’t change implementation files.

Read 

Read 

Read , lines 1 to 330

Read , lines 80 to 490

Read 

I’ve reconciled the sources against the current code. I’ll treat the detailed Pyramid spec as the governing roadmap, the other answers as hypotheses to test, and keep the three layers explicit. One tension to resolve in the plan: the spec’s split gate is “single by default; wide for independent branches; escalate failed leaves,” while one answer calls recursion primary. I’ll model recursion as evidence-triggered subtree work, not unconditional spawning, and use depth 2 from the ADaPT spec until data supports changing it.

Created 6 todos

**Plan Mode**

No implementation files changed in this turn. I’ve set the roadmap in the task list and reconciled the three research answers with the detailed Pyramid spec.

**Architecture**

Keep the three layers explicit:

- **L1: Planner/orchestrator.** Owns the goal, task division, split estimate, contracts, dependencies, frozen interfaces, re-planning decisions, verification, and final synthesis. Do not add a separate divider model.
- **L2: Adaptive workers.** Fresh, task-scoped context; only granted tools; no ability to spawn workers or modify the parent plan. Return a validated result envelope, a summary capped at 300 tokens, and artifact references.
- **L3: Deterministic tools and workflows.** Existing filesystem/Bash tools first. Composed workflows can be added later; generated tools remain quarantined until sandboxed, tested, and reviewed.

Scheduling, permissions, model limits, budgets, locks, schemas, and event recording belong in deterministic control-plane code, not another model call.

**How To Divide Work**

Use one deterministic split gate:

- **Single:** no useful estimate, tightly sequential work, tool-heavy work, or an estimate of eight steps or fewer.
- **Wide:** at least three independent branches with read-heavy work or demonstrably disjoint write scopes. Execute a DAG; parallelize eligible read-only nodes and serialize writers.
- **Explore then escalate:** uncertain work stays with L1 first. Re-decompose only when results, validation, or failures provide evidence that more work is needed.

This resolves the “wide versus recursive” tension: it is not a choice between two global architectures. Wide is a node’s execution mode; recursion is a bounded response to evidence. Start with **depth 2**, as the detailed spec’s ADaPT rule says. Another answer suggests depth 3, so test that later rather than silently choosing the larger limit.

The supplied “15-minute” spawn threshold conflicts with the spec’s step/branch gate and is not established for this project. Treat it as an evaluation feature, not a hard-coded rule. Likewise, keep fan-out at `min(independent branches, provider capacity, budget, 8)`, with actual concurrency bounded by the scheduler and measured provider limits.

**Build Order**

1. **Phase 0: Evaluation.** Build a repeatable harness for 30 representative tasks across breadth research, multi-file code, data pipelines, and sequential reasoning. Record single-T3 and single-T2 baselines with deterministic checks, quality, tokens, calls, latency, cost, and human review. The harness should also support comparing the Pyramid once its Phase 1 path is ready. The current `phase0_gate.py` validates a report; it does not run the benchmark. No real task set or evaluation results are present yet.

2. **Phase 1: Orchestration core.** Complete versioned task/result contracts, a trajectory-aware rules-first tier router, the split gate, an async DAG scheduler, per-provider RPM/TPM limits, per-node budgets, and SQLite WAL event records plus content-addressed artifacts. Enforce V0 envelope validation and V1 deterministic acceptance checks before dependent nodes consume results. Keep Task available; estimates that do not pass the gate simply stay with L1.

3. **Phase 2: Containment and recovery.** Add a clean-context verifier for higher-risk results, join consistency checks, evidence-triggered tier escalation, advisor calls, and ADaPT-style re-decomposition up to depth 2. Add path/resource locks and worktree or staging-based integration so parallel code changes do not directly collide in the shared tree.

4. **Phase 3: Reuse.** Add versioned workflow/tool entries, session quarantine, sandboxed tests without secrets, capability checks, and explicit promotion review. Use SQLite plus an in-process queue for this single-process project; consider Redis Streams only if it becomes multi-process. Do not use Redis Pub/Sub as the durable control log.

5. **Phase 4: Speculation and learned routing.** Only after measured wins: use a cheap proposer, verify against the authoritative plan, and permit speculative work only when it is read-only and its results cannot commit before confirmation. Train a learned router from logged outcomes; compare ablations.

**Conflicting Research Claims**

The fixed figures in the pasted answers, including the 15-minute threshold, cost reductions, and error-amplification multipliers, should be treated as hypotheses from different evaluation setups, not guaranteed results for this workload. The project’s own Phase 0 data decides which layers earn their place. The detailed spec’s Phase 0 kill criterion remains: don’t make the full Pyramid the default unless it improves cost per success on parallel tasks without reducing quality.

**Current Baseline**

The repository already has a DAG scheduler, file scopes/leases, tier configuration, task budgets, typed worker results, provider concurrency semaphores, and the deterministic split gate. The last full offline suite passed, but Task integration uses a fake query: it does not establish live model behavior. Main gaps are the real Phase 0 runner/data, RPM/TPM quotas, V1 breadth, independent verification, recursive escalation, durable event/artifact handling, worktree staging, and the tool/workflow forge.