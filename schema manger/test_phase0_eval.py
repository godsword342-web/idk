"""Offline Phase 0 dataset and comparison-metric tests."""
import json
import tempfile
from pathlib import Path

from phase0_eval import (
    EvaluationDataset,
    assess_pyramid,
    report_json,
    summarize_results,
)
from phase0_gate import Phase0Report


SHAPES = [
    "breadth_research",
    "multi_file_code",
    "data_pipeline",
    "sequential_reasoning",
]


def make_results(pyramid_cost: float | None = None):
    results = []
    for index in range(30):
        task_id = f"case-{index:02d}"
        shape = SHAPES[index % len(SHAPES)]
        for baseline, cost, quality in (
            ("single_t2", 0.04, 0.76),
            ("single_t3", 0.10, 0.82),
        ):
            results.append({
                "task_id": task_id,
                "shape": shape,
                "baseline": baseline,
                "checks_passed": True,
                "quality_score": quality,
                "tokens": 1000,
                "api_calls": 4,
                "duration_seconds": 10 + index,
                "cost_usd": cost,
                "human_reviewed": index < 3,
            })
        if pyramid_cost is not None:
            results.append({
                "task_id": task_id,
                "shape": shape,
                "baseline": "pyramid",
                "checks_passed": True,
                "quality_score": 0.82,
                "tokens": 700,
                "api_calls": 5,
                "duration_seconds": 8 + index,
                "cost_usd": pyramid_cost,
                "human_reviewed": index < 3,
            })
    return results


def main():
    dataset = EvaluationDataset.model_validate({
        "schema_version": "1.0",
        "tasks": [
            {
                "task_id": f"case-{index:02d}",
                "shape": SHAPES[index % len(SHAPES)],
                "prompt": f"Representative benchmark task {index}",
                "acceptance": [{"kind": "assert", "spec": "result is correct"}],
            }
            for index in range(30)
        ],
    })
    assert len(dataset.tasks) == 30
    try:
        EvaluationDataset.model_validate({
            "schema_version": "1.0", "tasks": dataset.tasks[:29],
        })
        raise AssertionError("a short dataset should fail validation")
    except ValueError:
        pass

    baseline_report = Phase0Report.model_validate({
        "schema_version": "1.0", "results": make_results(),
    })
    summaries = summarize_results(baseline_report.results)
    assert summaries["single_t3:breadth_research"]["success_rate"] == 1.0
    assert summaries["single_t3:breadth_research"]["cost_per_success_usd"] == 0.1

    passing_report = Phase0Report.model_validate({
        "schema_version": "1.0", "results": make_results(pyramid_cost=0.06),
    })
    passing = assess_pyramid(passing_report)
    assert passing["passes_kill_criteria"] is True
    assert round(passing["cost_reduction"], 2) == 0.4

    failing_report = Phase0Report.model_validate({
        "schema_version": "1.0", "results": make_results(pyramid_cost=0.09),
    })
    assert assess_pyramid(failing_report)["passes_kill_criteria"] is False
    assert json.loads(report_json(passing_report))["pyramid_assessment"][
        "passes_kill_criteria"
    ] is True

    with tempfile.TemporaryDirectory() as temporary:
        dataset_path = Path(temporary) / "tasks.json"
        report_path = Path(temporary) / "results.json"
        summary_path = Path(temporary) / "summary.json"
        dataset_path.write_text(dataset.model_dump_json(), encoding="utf-8")
        report_path.write_text(passing_report.model_dump_json(), encoding="utf-8")
        from phase0_eval import main as eval_main

        assert eval_main([
            "--dataset", str(dataset_path),
            "--report", str(report_path),
            "--output", str(summary_path),
        ]) == 0
        assert json.loads(summary_path.read_text(encoding="utf-8"))[
            "pyramid_assessment"
        ]["passes_kill_criteria"] is True

        from phase0_eval import validate_report_matches_dataset

        mismatch_dataset = EvaluationDataset.model_validate({
            "schema_version": "1.0",
            "tasks": [
                task.model_dump() if task.task_id != "case-29" else {
                    **task.model_dump(), "task_id": "different-case",
                }
                for task in dataset.tasks
            ],
        })
        try:
            validate_report_matches_dataset(mismatch_dataset, passing_report)
            raise AssertionError("mismatched report should fail")
        except ValueError as error:
            assert "task IDs" in str(error)

    print("Phase 0 evaluation metrics and kill-criteria tests passed")


if __name__ == "__main__":
    main()