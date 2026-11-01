"""Offline tests for DAG validation, dependency payloads, and file leases."""
import asyncio
import os
import tempfile

from task_scheduler import (
    FileLeaseConflict,
    FileLeaseRegistry,
    TaskFileScope,
    TaskSpec,
    TaskStatus,
    run_task_graph,
    validate_task_graph,
)


async def main():
    with tempfile.TemporaryDirectory() as root:
        os.makedirs(os.path.join(root, "src"))
        alpha_path = os.path.join(root, "src", "alpha.py")
        beta_path = os.path.join(root, "src", "beta.py")
        tasks = [
            TaskSpec(id="alpha", instruction="Inspect alpha", read_files=[alpha_path]),
            TaskSpec(id="beta", instruction="Inspect beta", read_files=[beta_path]),
            TaskSpec(
                id="summary", instruction="Summarize findings",
                needs=["alpha", "beta"],
            ),
        ]
        active = 0
        max_active = 0
        received = {}
        events = []

        async def runner(spec, dependencies, scope):
            nonlocal active, max_active
            received[spec.id] = dependencies
            if spec.id in ("alpha", "beta"):
                active += 1
                max_active = max(max_active, active)
                await asyncio.sleep(0.01)
                active -= 1
                return f"{spec.id} findings"
            assert dependencies == {
                "alpha": "alpha findings",
                "beta": "beta findings",
            }
            return "combined"

        results = await run_task_graph(
            tasks, runner, project_root=root, max_parallel=2,
            on_event=events.append,
        )
        assert [result.status for result in results] == [
            TaskStatus.SUCCEEDED, TaskStatus.SUCCEEDED, TaskStatus.SUCCEEDED,
        ]
        assert max_active == 2
        assert received["summary"] == {
            "alpha": "alpha findings",
            "beta": "beta findings",
        }
        assert [(event["task_id"], event["status"]) for event in events] == [
            ("alpha", "started"), ("beta", "started"),
            ("alpha", "succeeded"), ("beta", "succeeded"),
            ("summary", "started"), ("summary", "succeeded"),
        ]
        assert all("instruction" not in event and "write_files" not in event for event in events)
        assert all(
            "duration_ms" in event
            for event in events if event["status"] in ("succeeded", "failed")
        )

        async def failing_runner(spec, dependencies, scope):
            if spec.id == "broken":
                raise RuntimeError("expected failure")
            return "ok"

        failed = await run_task_graph([
            TaskSpec(id="broken", instruction="Fail"),
            TaskSpec(id="blocked", instruction="Needs broken", needs=["broken"]),
            TaskSpec(id="independent", instruction="Continue independently"),
        ], failing_runner, project_root=root, on_event=events.append)
        assert [result.status for result in failed] == [
            TaskStatus.FAILED, TaskStatus.BLOCKED, TaskStatus.SUCCEEDED,
        ]
        assert any(
            event["task_id"] == "blocked" and event["status"] == "blocked"
            for event in events
        )

        writers = [
            TaskSpec(id="writer-a", instruction="write A", write_files=[alpha_path]),
            TaskSpec(id="writer-b", instruction="write B", write_files=[alpha_path]),
        ]
        try:
            validate_task_graph(writers, root)
            raise AssertionError("unordered overlapping writes should be rejected")
        except ValueError as error:
            assert "overlapping write scopes" in str(error)

        ordered_writers = [
            writers[0],
            TaskSpec(
                id="writer-b", instruction="write B", needs=["writer-a"],
                write_files=[alpha_path],
            ),
        ]
        validated = validate_task_graph(ordered_writers, root)
        leases = FileLeaseRegistry(root)
        leases.acquire("writer-a", validated[0].write_paths)
        scope = TaskFileScope(
            "writer-a", root, validated[0].read_paths, validated[0].write_paths, leases,
        )
        assert scope.check_access("Write", alpha_path) is None
        assert scope.check_access("Write", beta_path) is not None
        assert scope.check_access("Read", beta_path) is not None
        try:
            leases.acquire("writer-b", validated[1].write_paths)
            raise AssertionError("an active lease conflict should fail")
        except FileLeaseConflict:
            pass
        leases.release("writer-a")

        invalid_graphs = [
            [TaskSpec(id="one", instruction="one", needs=["missing"])],
            [
                TaskSpec(id="one", instruction="one", needs=["two"]),
                TaskSpec(id="two", instruction="two", needs=["one"]),
            ],
        ]
        for invalid in invalid_graphs:
            try:
                validate_task_graph(invalid, root)
                raise AssertionError("invalid task graph should be rejected")
            except ValueError:
                pass

        try:
            validate_task_graph([
                TaskSpec(id="escape", instruction="outside", read_files=["../outside.py"]),
            ], root)
            raise AssertionError("out-of-project paths should be rejected")
        except ValueError as error:
            assert "outside the project" in str(error)

    print("Task DAG scheduler and file lease tests passed")


if __name__ == "__main__":
    asyncio.run(main())