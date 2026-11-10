"""Offline integration tests for the model-facing Task tool."""
import asyncio
import json
import os
import tempfile

import agent_tasks
from agent_tasks import AgentTasksInput, AgentTasksTool
from execute import execute_tool
from api import get_active_model
from messages import ToolResultMessage, create_assistant_message
from query_transitions import Terminal
from registry import clear, register
from state import AppState, FileStateCache, ToolPermissionContext
from task_scheduler import FileLeaseRegistry, TaskFileScope
from split_gate import SplitEstimate
from tool import ToolUseContext
from tools.read import ReadTool
from tools.write import WriteTool


def make_context(root: str) -> ToolUseContext:
    state = AppState(
        messages=[],
        tool_permission_context=ToolPermissionContext(mode="bypassPermissions"),
        read_file_cache=FileStateCache(),
    )
    context = ToolUseContext(
        get_app_state=lambda: state,
        set_app_state=lambda _update: None,
        read_file_state=state.read_file_cache,
    )
    context.project_root = root
    context.session_id = "parent-session"
    context.rendered_system_prompt = "Focused worker system prompt"
    return context


async def main():
    with tempfile.TemporaryDirectory() as root:
        first_path = os.path.join(root, "first.py")
        second_path = os.path.join(root, "second.py")
        output_path = os.path.join(root, "combined.py")
        started = 0
        active_readers = 0
        active_writers = 0
        read_barrier = asyncio.Event()
        prompts: dict[str, str] = {}
        worker_tools: dict[str, frozenset[str]] = {}
        worker_models: dict[str, str] = {}
        worker_budgets: dict[str, tuple[int, int]] = {}
        progress_events = []
        slow_worker_closed = asyncio.Event()

        single_result = await AgentTasksTool.call(
            AgentTasksInput(tasks=[{
                "id": "inline", "instruction": "Keep this small task inline",
            }]),
            make_context(root),
        )
        assert single_result.data["mode"] == "single"
        assert not single_result.data["tasks"]

        async def fake_query(params):
            nonlocal started, active_readers, active_writers
            worker = params.tool_use_context
            prompt = params.messages[0].content
            task_id = prompt.split("\n", 1)[0].removeprefix("Task ID: ")
            prompts[task_id] = prompt
            worker_tools[task_id] = worker.allowed_tool_names
            worker_models[task_id] = worker.model_override
            worker_budgets[task_id] = (
                params.max_turns, params.max_output_tokens_override,
            )

            if task_id == "tool-error":
                yield ToolResultMessage(
                    tool_use_id="failed-call", content="failed", is_error=True,
                )
                yield Terminal(
                    reason=(
                        "aborted_tools"
                        if worker.abort_controller.aborted else "completed"
                    ),
                )
                return
            if task_id == "slow":
                try:
                    await worker.abort_controller.wait()
                finally:
                    slow_worker_closed.set()
                yield Terminal(
                    reason=(
                        "aborted_tools"
                        if worker.abort_controller.aborted else "completed"
                    ),
                )
                return

            if task_id in ("first", "second"):
                active_readers += 1
                started += 1
                if started == 2:
                    read_barrier.set()
                await asyncio.wait_for(read_barrier.wait(), timeout=1)
                assert active_writers == 0
                output = f"{task_id} output"
                active_readers -= 1
            elif task_id == "schema":
                output = json.dumps({
                    "summary": "Typed result",
                    "output": {"answer": "42"},
                })
            elif task_id == "schema-invalid":
                output = json.dumps({
                    "summary": "Invalid typed result",
                    "output": {"answer": 42},
                })
            elif task_id == "token-limit":
                output = "over budget"
            else:
                assert active_readers == 0
                assert worker.allowed_tool_names == frozenset({"Read", "Edit", "Write"})
                assert worker.file_scope.check_access("Write", output_path) is None
                assert worker.file_scope.check_access("Write", second_path) is not None
                active_writers += 1
                output = "combined output"
                active_writers -= 1

            message = create_assistant_message(
                content=output,
                stop_reason="end_turn",
                usage={"output_tokens": 5} if task_id == "token-limit" else None,
            )
            params.messages.append(message)
            yield message
            yield Terminal(
                reason=(
                    "aborted_tools"
                    if worker.abort_controller.aborted else "completed"
                ),
            )

        original_query = agent_tasks.query
        agent_tasks.query = fake_query
        parent_context = make_context(root)
        parent_controller = type(
            "ParentController", (), {"aborted": False, "reason": None},
        )()
        parent_context.abort_controller = parent_controller
        parent_context._fallback_model = "test/parent-fallback"
        try:
            result = await AgentTasksTool.call(
                AgentTasksInput(tasks=[
                    {
                        "id": "first", "instruction": "Inspect first",
                        "read_files": [first_path], "tier_hint": "T1",
                    },
                    {
                        "id": "second", "instruction": "Inspect second",
                        "read_files": [second_path], "complexity": 5,
                    },
                    {
                        "id": "schema", "instruction": "Return a typed answer",
                        "output_schema": {
                            "type": "object",
                            "properties": {"answer": {"type": "string"}},
                            "required": ["answer"],
                            "additionalProperties": False,
                        },
                        "budget": {
                            "max_tokens": 400, "max_calls": 2,
                            "max_seconds": 60, "max_tool_errors": 1,
                        },
                    },
                    {
                        "id": "schema-invalid",
                        "instruction": "Return an invalid typed answer",
                        "output_schema": {
                            "type": "object",
                            "properties": {"answer": {"type": "string"}},
                            "required": ["answer"],
                            "additionalProperties": False,
                        },
                    },
                    {
                        "id": "tool-error", "instruction": "Stop after one tool error",
                        "budget": {"max_tool_errors": 1},
                    },
                    {
                        "id": "slow", "instruction": "Must finish before timeout",
                        "budget": {"max_seconds": 1},
                    },
                    {
                        "id": "token-limit", "instruction": "Stop at the token ceiling",
                        "budget": {"max_tokens": 4},
                    },
                    {
                        "id": "combine",
                        "instruction": "Combine the findings",
                        "needs": ["first", "second"],
                        "write_files": [output_path],
                        "complexity": 1,
                    },
                ], estimate=SplitEstimate(
                    estimated_steps=12,
                    sequential_dependency=False,
                    independent_branches=7,
                    read_heavy=True,
                    disjoint_writes=False,
                    uncertain_steps=False,
                )),
                parent_context,
                on_progress=progress_events.append,
            )
        finally:
            agent_tasks.query = original_query

        outcomes = result.data["tasks"]
        assert result.data["mode"] == "wide"
        assert [outcome["status"] for outcome in outcomes] == [
            "succeeded", "succeeded", "succeeded", "failed", "failed",
            "failed", "failed", "succeeded",
        ]
        assert "first output" in prompts["combine"]
        assert "second output" in prompts["combine"]
        assert worker_tools["first"] == frozenset({"Read"})
        assert "Task" not in worker_tools["combine"]
        assert worker_models["first"] == "groq/openai/gpt-oss-20b"
        assert worker_models["second"] == "test/parent-fallback"
        assert worker_models["combine"] == "groq/openai/gpt-oss-20b"
        assert worker_budgets["schema"] == (2, 400)
        assert worker_budgets["token-limit"] == (8, 4)
        assert next(item for item in outcomes if item["id"] == "schema")["summary"] == "Typed result"
        invalid_result = next(
            item for item in outcomes if item["id"] == "schema-invalid"
        )
        assert "schema" in invalid_result["error"]
        assert "tool error budget" in next(
            item for item in outcomes if item["id"] == "tool-error"
        )["error"]
        assert "TimeoutError" in next(
            item for item in outcomes if item["id"] == "slow"
        )["error"]
        assert "token budget" in next(
            item for item in outcomes if item["id"] == "token-limit"
        )["error"]
        assert slow_worker_closed.is_set()
        assert '"answer"' not in json.dumps(result.data)
        assert not parent_controller.aborted
        assert sorted(
            (event["task_id"], event["status"]) for event in progress_events
        ) == sorted([
            ("first", "started"), ("second", "started"),
            ("first", "succeeded"), ("second", "succeeded"),
            ("schema", "started"), ("schema", "succeeded"),
            ("schema-invalid", "started"), ("schema-invalid", "failed"),
            ("tool-error", "started"), ("tool-error", "failed"),
            ("slow", "started"), ("slow", "failed"),
            ("token-limit", "started"), ("token-limit", "failed"),
            ("combine", "started"), ("combine", "succeeded"),
        ])
        assert next(
            index for index, event in enumerate(progress_events)
            if event["task_id"] == "combine" and event["status"] == "started"
        ) > max(
            index for index, event in enumerate(progress_events)
            if event["task_id"] in ("first", "second")
            and event["status"] == "succeeded"
        )

        clear()
        register(ReadTool)
        register(WriteTool)
        leases = FileLeaseRegistry(root)
        canonical_allowed = leases.resolve(first_path)
        scope = TaskFileScope(
            task_id="scoped",
            project_root=root,
            read_paths=frozenset({canonical_allowed}),
            write_paths=frozenset(),
            leases=leases,
        )
        restricted = ToolUseContext()
        restricted.allowed_tool_names = frozenset({"Read"})
        restricted.file_scope = scope
        denied = await execute_tool(
            {"id": "outside", "name": "Read", "input": {"file_path": second_path}},
            restricted,
        )
        assert denied.is_error
        assert "not permitted to read" in denied.result_message.content

        disallowed = await execute_tool(
            {"id": "write", "name": "Write", "input": {
                "file_path": output_path, "content": "nope",
            }},
            restricted,
        )
        assert disallowed.is_error
        assert "not available in this task context" in disallowed.result_message.content

    print("Task tool integration and file-scope tests passed")


if __name__ == "__main__":
    asyncio.run(main())