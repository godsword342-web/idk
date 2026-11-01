"""Offline integration tests for the model-facing Task tool."""
import asyncio
import os
import tempfile

import agent_tasks
from agent_tasks import AgentTasksInput, AgentTasksTool
from execute import execute_tool
from messages import create_assistant_message
from query_transitions import Terminal
from registry import clear, register
from state import AppState, FileStateCache, ToolPermissionContext
from task_scheduler import FileLeaseRegistry, TaskFileScope
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
        progress_events = []

        async def fake_query(params):
            nonlocal started, active_readers, active_writers
            worker = params.tool_use_context
            prompt = params.messages[0].content
            task_id = prompt.split("\n", 1)[0].removeprefix("Task ID: ")
            prompts[task_id] = prompt
            worker_tools[task_id] = worker.allowed_tool_names

            if task_id in ("first", "second"):
                active_readers += 1
                started += 1
                if started == 2:
                    read_barrier.set()
                await asyncio.wait_for(read_barrier.wait(), timeout=1)
                assert active_writers == 0
                output = f"{task_id} output"
                active_readers -= 1
            else:
                assert active_readers == 0
                assert worker.allowed_tool_names == frozenset({"Read", "Edit", "Write"})
                assert worker.file_scope.check_access("Write", output_path) is None
                assert worker.file_scope.check_access("Write", second_path) is not None
                active_writers += 1
                output = "combined output"
                active_writers -= 1

            message = create_assistant_message(content=output, stop_reason="end_turn")
            params.messages.append(message)
            yield message
            yield Terminal(reason="completed")

        original_query = agent_tasks.query
        agent_tasks.query = fake_query
        try:
            result = await AgentTasksTool.call(
                AgentTasksInput(tasks=[
                    {"id": "first", "instruction": "Inspect first", "read_files": [first_path]},
                    {"id": "second", "instruction": "Inspect second", "read_files": [second_path]},
                    {
                        "id": "combine",
                        "instruction": "Combine the findings",
                        "needs": ["first", "second"],
                        "write_files": [output_path],
                    },
                ]),
                make_context(root),
                on_progress=progress_events.append,
            )
        finally:
            agent_tasks.query = original_query

        outcomes = result.data["tasks"]
        assert [outcome["status"] for outcome in outcomes] == [
            "succeeded", "succeeded", "succeeded",
        ]
        assert '"first": "first output"' in prompts["combine"]
        assert '"second": "second output"' in prompts["combine"]
        assert worker_tools["first"] == frozenset({"Read"})
        assert "Task" not in worker_tools["combine"]
        assert sorted(
            (event["task_id"], event["status"]) for event in progress_events
        ) == sorted([
            ("first", "started"), ("second", "started"),
            ("first", "succeeded"), ("second", "succeeded"),
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