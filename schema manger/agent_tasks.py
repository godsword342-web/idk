"""Model-facing DAG task tool backed by isolated child query contexts."""
from __future__ import annotations

import copy
import json
import os
import uuid
from typing import Any

from pydantic import BaseModel, Field

from loop_guard import LoopGuardState
from messages import create_user_message
from permission_types import PermissionAskDecision
from query import QueryParams, query
from query_transitions import Terminal
from state import AppState, FileStateCache
from task_scheduler import (
    TaskFileScope,
    TaskOutcome,
    TaskSpec,
    TaskStatus,
    run_task_graph,
    validate_task_graph,
)
from tool import ToolResult, ToolUseContext, ValidationResult, build_tool


MAX_TASKS = 8
MAX_PARALLEL_READ_TASKS = 4
MAX_DEPENDENCY_CONTEXT_CHARS = 12_000
MAX_TASK_OUTPUT_CHARS = 8_000
MAX_TASK_RESULT_CHARS = 40_000
READ_ONLY_WORKER_TOOLS = frozenset({"Read"})
WRITE_WORKER_TOOLS = frozenset({"Read", "Edit", "Write"})


class AgentTasksInput(BaseModel):
    tasks: list[TaskSpec] = Field(
        min_length=1,
        max_length=MAX_TASKS,
        description=(
            "A DAG of focused tasks. Use needs to declare output dependencies; "
            "declare every file a task may read or write."
        ),
    )


def _project_root(context: ToolUseContext) -> str:
    return os.path.realpath(
        os.path.abspath(getattr(context, "project_root", None) or os.getcwd())
    )


def _validate_input(input: AgentTasksInput, context: ToolUseContext) -> ValidationResult:
    try:
        validate_task_graph(input.tasks, _project_root(context))
    except ValueError as error:
        return ValidationResult(result=False, message=str(error), error_code=1)
    return ValidationResult(result=True)


def _dependency_context(dependencies: dict[str, str]) -> str:
    remaining = MAX_DEPENDENCY_CONTEXT_CHARS
    payload: dict[str, str] = {}
    for task_id, output in dependencies.items():
        if remaining <= 0:
            payload[task_id] = "[omitted: dependency context budget reached]"
            continue
        content = output[:remaining]
        if len(content) < len(output):
            content += "\n[dependency output truncated]"
        payload[task_id] = content
        remaining -= len(content)
    return json.dumps(payload, ensure_ascii=True)


def _task_prompt(
    task: TaskSpec,
    dependencies: dict[str, str],
    project_root: str,
) -> str:
    read_scope = "\n".join(f"- {path}" for path in task.read_files) or "- (none)"
    write_scope = "\n".join(f"- {path}" for path in task.write_files) or "- (none)"
    mode_note = (
        "This task may edit only its declared write files. Read each existing "
        "file before editing it."
        if task.write_files else
        "This is a read-only task. Do not modify files."
    )
    return (
        f"Task ID: {task.id}\n"
        f"Project root: {project_root}\n\n"
        f"Assignment:\n{task.instruction}\n\n"
        f"Declared readable files:\n{read_scope}\n\n"
        f"Declared writable files:\n{write_scope}\n\n"
        f"Dependency outputs (JSON, keyed by task ID):\n"
        f"{_dependency_context(dependencies)}\n\n"
        f"{mode_note}\n"
        "Work only on this assignment. Do not delegate or attempt tools that "
        "were not provided. Return a concise result with concrete file paths "
        "and findings, or summarize the edits you made."
    )


def _child_context(
    parent: ToolUseContext,
    task: TaskSpec,
    scope: TaskFileScope,
    allowed_tools: frozenset[str],
) -> ToolUseContext:
    parent_state = parent.get_app_state()
    child_state = AppState(
        messages=[],
        tool_permission_context=copy.deepcopy(parent_state.tool_permission_context),
        read_file_cache=FileStateCache(),
        tasks={},
        agent_name_registry={},
        mode=parent_state.mode,
    )
    state_ref = [child_state]

    def set_child_state(update) -> None:
        if callable(update):
            state_ref[0] = update(state_ref[0])

    child = copy.copy(parent)
    child.get_app_state = lambda: state_ref[0]
    child.set_app_state = set_child_state
    child.read_file_state = child_state.read_file_cache
    child.messages = []
    child.agent_id = task.id
    child.agent_type = "task_worker"
    child.allowed_tool_names = allowed_tools
    child.file_scope = scope
    child.project_root = scope.project_root
    child.session_id = f"task-{uuid.uuid4().hex}"
    child.set_in_progress_tool_use_ids = None
    child.set_has_interruptible_tool_in_progress = None
    child._loop_guard = LoopGuardState()
    child._store = getattr(parent, "_store", None)
    child.archive_messages = lambda _messages: None
    child._stored_message_ids = []
    child._stored_message_uuids = set()
    child._summary_source_offset = 0
    return child


async def _run_child_task(
    task: TaskSpec,
    dependencies: dict[str, str],
    scope: TaskFileScope,
    parent: ToolUseContext,
    can_use_tool: Any,
) -> str:
    allowed = WRITE_WORKER_TOOLS if task.write_files else READ_ONLY_WORKER_TOOLS
    child = _child_context(parent, task, scope, allowed)
    base_prompt = getattr(parent, "rendered_system_prompt", None)
    system_prompt = base_prompt or (
        "You are a focused coding task worker. Be accurate, respect the "
        "declared file scope, and return a concise result."
    )
    params = QueryParams(
        messages=[create_user_message(_task_prompt(task, dependencies, scope.project_root))],
        system_prompt=system_prompt,
        tool_use_context=child,
        can_use_tool=can_use_tool,
        query_source="subagent_task",
        max_turns=8,
    )

    terminal: Terminal | None = None
    async for event in query(params):
        if isinstance(event, Terminal):
            terminal = event
        elif getattr(event, "type", None) == "assistant" and getattr(event, "is_api_error", False):
            raise RuntimeError(getattr(event, "content", None) or "Worker model request failed")

    if terminal is None or terminal.reason != "completed":
        reason = terminal.reason if terminal is not None else "no terminal result"
        raise RuntimeError(f"Worker ended with {reason}")

    for message in reversed(params.messages):
        if (
            getattr(message, "type", None) == "assistant"
            and getattr(message, "content", None)
            and not getattr(message, "tool_calls", None)
        ):
            return str(message.content)[:MAX_TASK_OUTPUT_CHARS]
    raise RuntimeError("Worker completed without a text result")


async def _call(
    input: AgentTasksInput,
    context: ToolUseContext,
    can_use_tool: Any = None,
    parent_message: Any = None,
    on_progress: Any = None,
) -> ToolResult:
    root = _project_root(context)

    async def runner(task: TaskSpec, dependencies: dict[str, str], scope: TaskFileScope) -> str:
        return await _run_child_task(task, dependencies, scope, context, can_use_tool)

    outcomes = await run_task_graph(
        input.tasks,
        runner,
        project_root=root,
        max_parallel=MAX_PARALLEL_READ_TASKS,
        on_event=(
            lambda event: on_progress(event)
            if on_progress is not None else None
        ),
    )
    return ToolResult(data={
        "tasks": [
            {
                "id": outcome.task_id,
                "status": outcome.status.value,
                "output": outcome.output,
                "error": outcome.error,
            }
            for outcome in outcomes
        ],
    })


def _map_result(data: dict, tool_use_id: str) -> dict:
    content = json.dumps(data.get("tasks", []), ensure_ascii=True)
    if len(content) > MAX_TASK_RESULT_CHARS:
        content = content[:MAX_TASK_RESULT_CHARS] + "\n[task results truncated]"
    return {
        "type": "tool_result",
        "tool_use_id": tool_use_id,
        "content": content,
    }


async def _check_permissions(input: AgentTasksInput, context: ToolUseContext):
    count = len(input.tasks)
    return PermissionAskDecision(
        message=f"Run {count} isolated task worker{'s' if count != 1 else ''}?",
    )


AgentTasksTool = build_tool(
    name="Task",
    description="Run a validated DAG of isolated coding task workers",
    prompt=(
        "Decompose substantial work into focused tasks. Use `needs` when a "
        "task requires another task's output. Declare exact read_files and "
        "write_files. Read-only tasks may run concurrently; any task with "
        "write_files runs exclusively. Overlapping write scopes require an "
        "explicit dependency path."
    ),
    input_schema=AgentTasksInput,
    call=_call,
    map_tool_result_to_tool_result_block_param=_map_result,
    render_tool_use_message=lambda input, _options: (
        f"Task({len(input.tasks)} workers)"
        if isinstance(input, AgentTasksInput)
        else "Task"
    ),
    max_result_size_chars=MAX_TASK_RESULT_CHARS,
    is_read_only=lambda _input: False,
    is_concurrency_safe=lambda _input: False,
    to_auto_classifier_input=lambda input: ", ".join(
        f"{task.id}: {task.instruction[:160]}" for task in input.tasks
    ),
    validate_input=_validate_input,
    check_permissions=_check_permissions,
)


__all__ = ["AgentTasksInput", "AgentTasksTool", "MAX_TASKS"]