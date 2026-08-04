from __future__ import annotations

import asyncio
import json
import unittest
from collections import Counter
from pathlib import Path

from agentstorm_worker.adapters import AdapterFactoryContext
from agentstorm_worker.benchmarks.sre import SREFakeAdapter, create_adapter, validate_tool_usage
from agentstorm_worker.config import load_dataset, load_run_config
from agentstorm_worker.runner import WorkloadRunner

ROOT = Path(__file__).resolve().parents[2]


class SREBenchmarkTest(unittest.TestCase):
    def test_dataset_covers_four_incident_families_and_passes_end_to_end(self) -> None:
        cases = load_dataset(ROOT / "examples/datasets/sre-incidents.jsonl")
        config = load_run_config(ROOT / "examples/run.sre.local.json")
        fixtures = json.loads(
            (ROOT / "worker/src/agentstorm_worker/benchmarks/sre_fixtures.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(len(cases), 32)
        self.assertEqual(
            {fixture["root_cause"] for fixture in fixtures},
            {
                "database_pool_exhaustion",
                "memory_leak",
                "downstream_dependency_timeout",
                "bad_release",
            },
        )
        self.assertEqual(set(Counter(fixture["root_cause"] for fixture in fixtures).values()), {8})
        adapter = create_adapter(AdapterFactoryContext(provider="fake", model="", base_url=""))
        self.assertIsInstance(adapter, SREFakeAdapter)
        results, summary = asyncio.run(WorkloadRunner(config, adapter).execute(cases))
        self.assertEqual((summary.total, summary.succeeded, summary.failed), (32, 32, 0))
        self.assertEqual(summary.model_call_count, 160)
        self.assertEqual(summary.tool_call_count, 128)
        self.assertEqual(summary.model_calls_per_successful_agent, 5)
        self.assertEqual(summary.tool_calls_per_successful_agent, 4)
        for result in results:
            self.assertEqual(
                result.tool_path,
                ["get_incident", "query_metrics", "search_logs", "lookup_runbook"],
            )
            self.assertEqual((result.model_call_count, result.tool_call_count), (5, 4))
            self.assertEqual(json.loads(result.output)["incident_id"], result.case_id)

    def test_tool_assertion_accepts_metrics_logs_swap_and_rejects_bad_dependencies(self) -> None:
        case = load_dataset(ROOT / "examples/datasets/sre-incidents.jsonl")[0]
        adapter = SREFakeAdapter()
        response = asyncio.run(adapter.run(case))
        calls = response.metadata["agentstorm.sre.tool_calls"]
        context = {"case_id": case.case_id, "metadata": response.metadata}
        self.assertTrue(validate_tool_usage(context, {})["passed"])

        swapped = [calls[0], calls[2], calls[1], calls[3]]
        self.assertTrue(
            validate_tool_usage(
                {"case_id": case.case_id, "metadata": {"agentstorm.sre.tool_calls": swapped}},
                {},
            )["passed"]
        )
        duplicate = [calls[0], calls[1], calls[1], calls[3]]
        self.assertFalse(
            validate_tool_usage(
                {"case_id": case.case_id, "metadata": {"agentstorm.sre.tool_calls": duplicate}},
                {},
            )["passed"]
        )
        wrong = [*calls]
        wrong[-1] = {"name": "lookup_runbook", "arguments": {"error_signature": "wrong"}}
        self.assertFalse(
            validate_tool_usage(
                {"case_id": case.case_id, "metadata": {"agentstorm.sre.tool_calls": wrong}},
                {},
            )["passed"]
        )

    def test_search_logs_tool_error_is_reported_as_tool_failure(self) -> None:
        raw = json.loads((ROOT / "examples/run.sre.local.json").read_text(encoding="utf-8"))
        raw["reliability"] = {
            "seed": 42,
            "scenario": {
                "sourceName": "sre-faults",
                "sourceKey": "scenario.json",
                "digest": "sha256:" + "a" * 64,
                "scenario": {
                    "apiVersion": "agentstorm.io/v1alpha1",
                    "kind": "FaultScenario",
                    "rules": [
                        {
                            "name": "logs-fail",
                            "fault": "tool_error",
                            "probability": 1,
                            "attempts": [1],
                            "toolName": "search_logs",
                        }
                    ],
                },
            },
        }
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            config = load_run_config(path)
        case = load_dataset(ROOT / "examples/datasets/sre-incidents.jsonl")[0]
        result = asyncio.run(WorkloadRunner(config, SREFakeAdapter()).execute([case]))[0][0]
        self.assertFalse(result.success)
        self.assertEqual(
            (result.failure_category, result.error_code), ("tool", "injected_tool_error")
        )
        self.assertEqual(result.tool_path, ["get_incident", "query_metrics", "search_logs"])
