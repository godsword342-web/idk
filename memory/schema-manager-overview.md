# Schema Manager Current State

Last updated: 2026-11-01. This snapshot is the source of truth for the current working tree; older roadmap notes below are historical and may be superseded.

## What it is
A Python coding-agent runtime under `schema manger/`. The query loop is provider-agnostic through LiteLLM; internal messages and registered tools are converted at the API boundary. Do not treat it as a Claude Code source replica: several behaviors are local implementations or explicit deviations.

## Request flow
1. `query.py` maintains conversation state, shapes context, injects plan/summary/relevant file memories, calls the model, and continues or terminates after tools.
2. `query_deps.py` forwards the query's selected model, explicit fallback, source, and token limit to `api.stream()`.
3. `api.py` normalizes provider events through LiteLLM and applies bounded retry/fallback policy. Registered tool schemas are sent in the request; `tool.py` emits standard OpenAI function schemas.
4. Model tool calls go through `StreamingToolExecutor` or `tool_orchestration.py`. The executor applies concurrency-safety policy; tool-batch concurrency is separate from the Task DAG scheduler.
5. `execute.py` validates calls, checks permissions, prompts when needed, applies safety gates, executes the tool, and returns a model-facing result.
6. `permissions.py` applies hard denies, configured rules, tool-specific checks, permission modes, and auto-mode classification.

## Main building blocks
- `tool.py`, `registry.py`, `tools/`: shared tool contract, explicit registry, and filesystem/Bash tools.
- `todo.py`, `agent_tasks.py`, `task_scheduler.py`: model-invoked plan updates, isolated Task child query loops, validated DAG scheduling, dependency payloads, and file leases.
- `messages.py`, `api.py`, `query_deps.py`: message types, LiteLLM streaming/error policy, and model adapter.
- `state.py`: per-session in-memory state; `store.py`: append-only SQLite history, FTS5 search, summary DAG, and persisted plans.
- `compact.py`, `query_token_budget.py`: token estimates, compaction, and continuation controls.
- `system_context.py`, `memdir.py`, `memory_injector.py`, `memory_tools.py`: system context, selectable file memories, per-turn injection, MemorySearch, and MemoryExpand.
- `ui.py`: Textual chat UI, plan panel, tool cards, permission modal, and runtime activity notices.
- `path_safety.py`, `permission_rules.py`, `denial_tracking.py`, `loop_guard.py`: safety and repeated-action controls.

## Maturity notes
The core query, API, tool execution, permissions, filesystem tools, persistence, Task DAG, memory injection, and Textual UI are implemented and covered by offline script tests. The model decides whether to call TodoWrite, Task, MemorySearch, or MemoryExpand; the runtime enforces their contracts only after a call. Current tests prove behavior using fake model/query boundaries, not that a live model will choose a tool.

There is no OS-level shell sandbox in this folder. `path_safety.py` and permission gates are application safeguards, not bubblewrap/Seatbelt isolation. Some hooks/context-recovery areas in `compact.py` remain partial. The app has no provider-wide concurrency semaphore, no ToolSearch/deferred-schema expansion, and no FastMCP client integration.

`Tool.prompt` is currently not included by `to_openai_schema()`; the provider receives tool description and parameters. `should_defer` is metadata only. Full schemas for the tools available in the current query context are sent each request, so do not claim Anthropic `defer_loading` or `tool_reference` behavior here.

Runtime activity notices now show model request/retry/fallback/failure status; Task child lifecycle and dependencies; TodoWrite count updates; automatic memory source/count; and explicit memory operation counts/source IDs. They intentionally omit prompt text, API keys, memory body, and raw provider error payloads. The PlanPanel is separate from this activity stream.

The current editor buffer `schema manger/experiments/stream_writer.py` is not present on disk: the `experiments/` directory is absent from the checkout. It is an unverified scratch loop that sleeps synchronously and appends `output_log.txt`; do not treat it as production or as implemented async streaming.

## Current provider behavior
- `requirements.txt` currently pins `litellm==1.105.0`; provider credentials are read from environment/`.env` and must never be copied into this note.
- `api.py` retries transient 429/5xx/network failures a bounded number of times, honors `Retry-After` up to its configured wait budget, treats hard-quota 429 as fail-fast, and does not retry a 404 on the same model.
- Cross-model failover uses explicitly configured alternates (`fallback_model` or `LITELLM_FALLBACK_MODELS`) for 404, overload, and connection failures. Rate limits do not trigger cross-model failover.
- Provider errors are normalized to safe query-facing messages. Context overflow maps to the existing compaction recovery signal.
- The originally reported NVIDIA 404 still needs a redacted provider response/request ID to identify its cause. No live call using the user's key has been made.
- A prior MCP compatibility report involved LiteLLM's optional experimental MCP modules, not this folder's regular `litellm.acompletion` path. This project has no MCP client use; do not upgrade/pin MCP solely based on that optional import without validating package constraints.

## Current orchestration and verification
- TodoWrite persists the plan to AppState and SQLite; the UI PlanPanel reads the stored plan. Completed plans are cleared. Strict caps are opt-in.
- Task accepts up to eight DAG nodes; the scheduler allows up to four ready read-only workers and serializes writers. File scopes, dependency links, and write leases are validated. Child query tests still use a fake query, so live/model selection is unproven.
- Task nodes now accept complexity (1-5), an optional T1/T2/T3 hint, non-goals, output JSON Schema, deterministic file-existence checks, and per-node token/call/time/tool-error budgets. `worker_tiers.yaml` maps tiers to models; complexity defaults map 1-2 to T1, 3-4 to T2, and 5 to T3. Child queries and compaction use the resolved worker model. Workers return validated result envelopes with bounded summaries and SHA-256 artifact references; dependent workers receive the envelope and the parent receives summaries/artifacts, not raw worker output. Shell-command acceptance checks, full artifact storage, and independent LLM verification remain unimplemented.
- Task remains available. Its deterministic split gate defaults to single-agent when no estimate is supplied, the work is sequential, or estimated steps are at most eight. Wide execution requires at least three independent branches and a read-heavy or disjoint-write estimate; DAG root counts are checked against the estimate. Uncertain work returns to the parent for exploration. `phase0_gate.py` validates baseline reports; `phase0_eval.py` validates datasets, summarizes metrics, and applies the Pyramid kill criteria. These are evaluation utilities, not runtime kill switches. The evaluator is implemented, but no real benchmark dataset/results have been supplied or run.
- Provider requests now share per-provider concurrency semaphores across model, compaction, and classifier calls. `AGENT_PROVIDER_MAX_CONCURRENCY` defaults to four; `AGENT_PROVIDER_MAX_CONCURRENCY_<PROVIDER>` overrides an individual provider. This does not yet enforce RPM or TPM quotas.
- Memory injection runs per query from the active plan, root summary, and selected project-memory files. MemorySearch/MemoryExpand are model-callable and session-scoped. The automatic injection and explicit tool calls now produce safe UI activity notices.
- The last full run of every `test_*.py` script in `schema manger/` passed after the deterministic split gate was added. Task integration tests use a fake query; no live worker/model calls were made. Editor diagnostics reported no errors and `git diff --check` was clean.
- Tests are executable scripts; `pytest 'schema manger/'` previously found no pytest tests. Use `cd 'schema manger' && for test in test_*.py; do python "$test" || exit $?; done` for the established full-suite check.
- No commit was made. Worktree includes other user changes and generated `__pycache__`/`output_log.txt`; do not clean/revert them without authorization.

## Next Work
1. Supply 30 representative tasks across the four Phase 0 shapes, then run `python phase0_eval.py --dataset <tasks.json> --report <results.json> --output <summary.json>` after collecting single-T3 and single-T2 baselines. Record deterministic checks, quality, tokens, calls, latency, cost, and human review; no live evaluation has been run.
2. Calibrate split estimates against those baselines, then compare the full Pyramid. Apply the documented quality/cost kill criteria before making wide fan-out the default.
3. Add per-provider RPM/TPM token buckets on top of concurrency slots; retain the Task scheduler's separate DAG/read/write limits.
4. Add independent V2 verification and join consistency checks; consider full content-addressed artifact storage and durable run events.
5. Build a deterministic production-query test with scripted parent and child model responses. Verify parent tool selection, tier routing through the provider adapter, child event visibility, dependency results, and permissions without live API calls.
6. Consider adding `Tool.prompt` to provider-visible descriptions so the model sees the intended use guidance. Test description limits and provider compatibility first.
7. Keep a separate opt-in live NVIDIA smoke test and diagnose the intermittent 404 only from redacted status/body/request-ID evidence.
8. Treat OS-level sandboxing and provider-neutral ToolSearch as separate design projects; neither exists in the current runtime.

## Historical Notes
The sections in the prior version of this document below described an earlier memory/context implementation phase. Preserve those details as history, but use the current-state sections above for present behavior.

The initial architecture review excluded `main.py`; it was inspected and updated during the implementation phase below.

## Historical Implementation Roadmap (superseded by current state above)

The persistence checks passed before implementation: `test_todo.py` and `test_store.py`.

### Completed in this phase
- Hardened `memdir.py`: absolute/local path validation, project-setting override rejection, symlink-escape protection, project-root keyed caching, remote memory directory support, one-day freshness warnings, and SHA-256 no-op writes.
- Added per-turn context assembly for the persisted plan, summary, and up to five relevant file memories. Memory selection is deduplicated across a session and routed through the query's existing single system-reminder injection.
- Added read-only `MemorySearch` and `MemoryExpand` tools for FTS5 search and exact DAG expansion.
- Connected CLI registration and SQLite persistence. Query messages are archived before context shaping; compaction summaries become summary-DAG nodes, and expansion retains chronological order.
- Made microcompacted tool results point to their archived message IDs, so `MemoryExpand` can recover them. Preserved loop-guard state through streaming tool contexts and registered memory reads as safe/idempotent.
- Synchronized the compacted active transcript back to the CLI's message history and connected `CLAUDE_CODE_MAX_TURN_TOKENS` to the existing diminishing-returns guard.
- Fixed two verified execution defects encountered in regression tests: Bash path safety was evaluating whole command strings as paths, and `execute.py` discarded the tool mapper's `is_error` result.

### Remaining before calling the context-window phase complete
1. `Store.get_active_view_messages()` now supplies the query after a summary exists, using a 40K default budget and keeping recent user/tool groups intact. Its estimate is character-based, and one indivisible newest group can exceed the budget; validate with the provider tokenizer before claiming the ~29K target.
2. Before the first persisted summary exists, the full transcript still relies on the existing auto-compaction trigger. Reconcile the cited 87% figure with the repository's current 167K threshold before changing it.
3. Load user-level memory settings in CLI setup. Project-level memory-path settings must remain rejected; the resolver supports an explicit user setting but the CLI does not load one yet.

Team sync, AST indexing, sandboxing, HRR, and automatic memory extraction remain separate deferred projects. The complete offline regression suite passed after implementation; persistence, query compaction, memory injection, memory tools, active views, and recovery pointers also have focused tests.

## Historical Next Session Notes (superseded where noted above)
- `plans.py` is now correctly named but remains unused: no imports or runtime call sites. Keep it deferred until a real Markdown Plan Mode lifecycle (enter/exit/resume/display) is implemented; do not merge it into the active SQLite `TodoWrite` plan path. Remove it only if Plan Mode is abandoned.
- Provider config in `api.py`: short `nvidia/...` names normalize to `nvidia_nim/nvidia/...`; legacy `NVIDIA_API_KEY` maps to LiteLLM's `NVIDIA_NIM_API_KEY`; Gemini catalog uses `gemini-3.8-flash`. The old statement that cross-provider fallbacks only happen after rate limits is superseded; use the current provider behavior section above.
- API tests and smoke tests passed after those provider changes. The most recent `python main.py -p "hi"` terminal status was exit code 0; inspect the actual response if live provider behavior needs confirmation.
- Provider credentials were exposed in a chat attachment. Rotate those keys; never copy their values into this note or another chat.
- The full offline suite passed after the context adapter and provider changes. No commit was made; this workspace note is intended to be included if the user commits the work.
