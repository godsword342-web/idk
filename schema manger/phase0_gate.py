"""Validate the Phase 0 baseline report required by the evaluation roadmap."""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, model_validator


TaskShape = Literal[
    "breadth_research",
    "multi_file_code",
    "data_pipeline",
    "sequential_reasoning",
]
BaselineName = Literal["single_t3", "single_t2", "pyramid"]
REQUIRED_CASE_COUNT = 30
MIN_PILOT_CASE_COUNT = 8
REQUIRED_HUMAN_REVIEW_COUNT = 3


class BaselineResult(BaseModel):
    task_id: str = Field(min_length=1)
    shape: TaskShape
    baseline: BaselineName
    model_id: str | None = None
    checks_passed: bool
    quality_score: float = Field(ge=0, le=1)
    tokens: int = Field(ge=0)
    api_calls: int = Field(ge=0)
    duration_seconds: float = Field(ge=0)
    cost_usd: float | None = Field(default=None, ge=0)
    task_invocations: int | None = Field(default=None, ge=0)
    child_api_calls: int | None = Field(default=None, ge=0)
    child_provider_attempts: int | None = Field(default=None, ge=0)
    child_model_ids: list[str] | None = None
    task_worker_count: int | None = Field(default=None, ge=0)
    task_worker_successes: int | None = Field(default=None, ge=0)
    task_worker_failures: int | None = Field(default=None, ge=0)
    human_reviewed: bool = False
    human_quality_score: float | None = Field(default=None, ge=0, le=1)
    human_review_notes: str | None = Field(default=None, max_length=2_000)
    artifact_dir: str | None = None
    response_path: str | None = None


class Phase0Report(BaseModel):
    schema_version: Literal["1.0"]
    results: list[BaselineResult] = Field(min_length=MIN_PILOT_CASE_COUNT * 2)

    @model_validator(mode="after")
    def _validate_baseline_matrix(self) -> "Phase0Report":
        grouped: dict[str, dict[str, BaselineResult]] = {}
        for result in self.results:
            baselines = grouped.setdefault(result.task_id, {})
            if result.baseline in baselines:
                raise ValueError(
                    f"Duplicate result for {result.task_id}/{result.baseline}"
                )
            baselines[result.baseline] = result

        if not MIN_PILOT_CASE_COUNT <= len(grouped) <= REQUIRED_CASE_COUNT:
            raise ValueError(
                f"Phase 0 requires between {MIN_PILOT_CASE_COUNT} and "
                f"{REQUIRED_CASE_COUNT} tasks"
            )

        shapes: set[str] = set()
        reviewed: set[str] = set()
        for task_id, baselines in grouped.items():
            if not {"single_t2", "single_t3"}.issubset(baselines):
                raise ValueError(f"Task {task_id} needs both single_t2 and single_t3 results")
            if "pyramid" in baselines and len(baselines) != 3:
                raise ValueError(f"Task {task_id} has an incomplete Pyramid result")
            task_shapes = {result.shape for result in baselines.values()}
            if len(task_shapes) != 1:
                raise ValueError(f"Task {task_id} has inconsistent task shapes")
            shapes.update(task_shapes)
            if any(result.human_reviewed for result in baselines.values()):
                reviewed.add(task_id)

        if shapes != {
            "breadth_research", "multi_file_code",
            "data_pipeline", "sequential_reasoning",
        }:
            raise ValueError("Phase 0 must cover all four required task shapes")
        if len(reviewed) < REQUIRED_HUMAN_REVIEW_COUNT:
            raise ValueError("Phase 0 requires human review of at least three tasks")
        return self


def load_phase0_report(path: str | os.PathLike[str] | None = None) -> Phase0Report:
    configured = path or os.environ.get("PYRAMID_PHASE0_REPORT")
    if not configured:
        raise ValueError("PYRAMID_PHASE0_REPORT is not configured")
    report_path = Path(configured).expanduser()
    try:
        payload = report_path.read_text(encoding="utf-8")
    except OSError as error:
        raise ValueError("Phase 0 report cannot be read") from error
    try:
        return Phase0Report.model_validate_json(payload)
    except Exception as error:
        raise ValueError("Phase 0 report is incomplete or invalid") from error


def main() -> int:
    report_path = sys.argv[1] if len(sys.argv) > 1 else None
    try:
        report = load_phase0_report(report_path)
    except ValueError as error:
        print(f"Phase 0 report invalid: {error}", file=sys.stderr)
        return 1
    print(
        f"Phase 0 report valid: {len({result.task_id for result in report.results})} tasks, "
        f"both single_t2 and single_t3 baselines, all four shapes."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "BaselineResult", "Phase0Report", "load_phase0_report",
]