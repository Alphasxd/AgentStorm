from __future__ import annotations

import asyncio
import json
import unittest
from collections import Counter
from pathlib import Path

from jsonschema import Draft202012Validator

from agentstorm_worker.adapters import AdapterFactoryContext
from agentstorm_worker.benchmarks.sre import (
    SREFakeAdapter,
    _AGENTS_INSTRUCTIONS,
    _COMPATIBLE_INSTRUCTIONS,
    _compatible_tool_schemas,
    _create_openai_compatible_adapter,
    compatible_request_fields,
    create_adapter,
    get_incident,
    lookup_runbook,
    query_metrics,
    search_logs,
    validate_tool_usage,
)
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

    def test_compatible_factory_uses_the_shared_tool_schemas(self) -> None:
        context = AdapterFactoryContext(
            provider="openai-compatible",
            model="pinned-model-id",
            base_url="https://example.invalid/v1",
        )
        recording = _create_openai_compatible_adapter(context)
        schemas = _compatible_tool_schemas()
        self.assertEqual(recording._adapter._tool_schemas, schemas)
        self.assertEqual(
            [schema["function"]["name"] for schema in schemas],
            ["get_incident", "query_metrics", "search_logs", "lookup_runbook"],
        )

    def test_required_diagnosis_values_come_from_that_cases_tools(self) -> None:
        for instructions in (_COMPATIBLE_INSTRUCTIONS, _AGENTS_INSTRUCTIONS):
            self.assertIn("verbatim", instructions)
            self.assertIn("JSON array of strings", instructions)
        cases = load_dataset(ROOT / "examples/datasets/sre-incidents.jsonl")
        for case in cases:
            schema = next(
                assertion.schema
                for assertion in case.assertions
                if assertion.type == "json_schema" and isinstance(assertion.schema, dict)
            )
            incident, metrics, logs, runbook = asyncio.run(_tool_chain(case.case_id))
            # Exact JSON string values from this case's tool results. The shared
            # instruction text is not a source, so listing every enum there would fail.
            values = _string_values([incident, metrics, logs, runbook])
            answer = {
                "incident_id": incident["incident_id"],
                "root_cause": runbook["root_cause"],
                "severity": runbook["severity"],
                "recommended_action": runbook["recommended_action"],
                "evidence": runbook["evidence"],
                "runbook": runbook["runbook"],
            }
            Draft202012Validator(schema).validate(answer)
            self.assertEqual(answer["evidence"], runbook["evidence"])
            self.assertGreaterEqual(len(answer["evidence"]), 2)
            self.assertTrue(all(isinstance(item, str) for item in answer["evidence"]))
            for name, const in _schema_consts(schema).items():
                self.assertEqual(answer[name], const)
                self.assertIn(const, values)
                if name == "incident_id":
                    self.assertIn(const, case.prompt)
                else:
                    self.assertNotIn(const, case.prompt)

    def test_compatible_sampling_accepts_provider_fields_and_keeps_the_pin(self) -> None:
        fields = compatible_request_fields(
            AdapterFactoryContext(
                provider="openai-compatible",
                model="glm-4.7-flash",
                base_url="https://example.invalid/v4",
                max_output_tokens=2048,
                request_fields={"thinking": {"type": "disabled"}},
            )
        )
        self.assertEqual(fields["temperature"], 0)
        self.assertEqual(fields["max_tokens"], 2048)
        self.assertEqual(fields["thinking"], {"type": "disabled"})
        recording = create_adapter(
            AdapterFactoryContext(
                provider="openai-compatible",
                model="glm-4.7-flash",
                base_url="https://example.invalid/v4",
                http_retry_attempts=5,
                http_retry_max_wait_seconds=45,
            )
        )
        self.assertEqual(recording._adapter._max_http_attempts, 5)
        self.assertEqual(recording._adapter._retry_max, 45.0)
        with self.assertRaisesRegex(ValueError, "reserved"):
            compatible_request_fields(
                AdapterFactoryContext(
                    provider="openai-compatible",
                    model="glm-4.7-flash",
                    base_url="https://example.invalid/v4",
                    request_fields={"max_tokens": 9},
                )
            )


async def _tool_chain(case_id: str) -> tuple[dict, dict, dict, dict]:
    incident = await get_incident(case_id)
    metrics = await query_metrics(incident["service"], incident["window"])
    logs = await search_logs(incident["service"], incident["window"])
    runbook = await lookup_runbook(str(logs["error_signature"]))
    return incident, metrics, logs, runbook


def _string_values(value: object) -> set[str]:
    found: set[str] = set()
    if isinstance(value, str):
        found.add(value)
    elif isinstance(value, dict):
        for item in value.values():
            found.update(_string_values(item))
    elif isinstance(value, list):
        for item in value:
            found.update(_string_values(item))
    return found


def _schema_consts(schema: dict) -> dict[str, str]:
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return {}
    consts: dict[str, str] = {}
    for name, definition in properties.items():
        if isinstance(definition, dict) and "const" in definition:
            const = definition["const"]
            if not isinstance(const, str):
                raise AssertionError(f"{name} const must be a string")
            consts[name] = const
    return consts
