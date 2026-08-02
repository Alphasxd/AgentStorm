from __future__ import annotations

import json
import unittest
from typing import Any

from agentstorm_worker.config import (
    DatasetConfig,
    EvaluationConfig,
    RunConfig,
    SourceConfig,
    TargetConfig,
    WorkloadConfig,
)
from agentstorm_worker.models import AssertionOutcome, CaseResult, RunSummary
from agentstorm_worker.results import ResultClient, ResultSinkConfig


class FakeResponse:
    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return b"{}"


class RecordingOpener:
    def __init__(self) -> None:
        self.requests: list[Any] = []

    def __call__(self, request: Any, timeout: float) -> FakeResponse:
        self.requests.append((request, timeout))
        return FakeResponse()


def run_config() -> RunConfig:
    return RunConfig(
        run_id="run-1",
        source=SourceConfig(namespace="default", name="demo"),
        dataset=DatasetConfig(name="demo-dataset", key="cases.jsonl"),
        target=TargetConfig(provider="fake"),
        workload=WorkloadConfig(),
        evaluation=EvaluationConfig(min_success_rate=1, max_error_rate=0),
    )


def run_summary() -> RunSummary:
    return RunSummary(
        run_id="run-1",
        shard_index=0,
        shard_count=2,
        duration_ms=12,
        total=1,
        succeeded=0,
        failed=1,
        success_rate=0,
        error_rate=1,
        p95_latency_ms=10,
        input_tokens=3,
        output_tokens=5,
        thresholds_passed=False,
        threshold_failures=["failed"],
    )


class ResultClientTest(unittest.TestCase):
    def test_registers_and_uploads_without_sensitive_content_by_default(self) -> None:
        opener = RecordingOpener()
        client = ResultClient(
            ResultSinkConfig(base_url="http://results.example", write_token="test-token"),
            opener=opener,
        )
        result = CaseResult(
            case_id="case with space",
            iteration=0,
            success=False,
            latency_ms=10,
            failure_kind="assertion",
            output="private output",
            error="private error",
            input_tokens=3,
            output_tokens=5,
            tool_path=["safe.lookup"],
            assertions=[
                AssertionOutcome(
                    index=0,
                    type="contains",
                    passed=False,
                    reason_code="mismatch",
                    message="private expected value",
                )
            ],
        )

        client.register_run(run_config(), expected_shards=2)
        client.upload_shard("run-1", 0, [result], run_summary())

        self.assertEqual(len(opener.requests), 2)
        registration_request, registration_timeout = opener.requests[0]
        self.assertEqual(registration_request.get_header("Idempotency-key"), "run/run-1")
        self.assertEqual(registration_timeout, 30)
        registration = json.loads(registration_request.data)
        self.assertEqual(registration["source"]["namespace"], "default")
        self.assertEqual(registration["expected_shards"], 2)

        shard_request, _ = opener.requests[1]
        shard = json.loads(shard_request.data)
        case = shard["cases"][0]
        self.assertEqual(
            case["idempotency_key"], "run/run-1/case/case+with+space/iteration/0"
        )
        self.assertEqual(case["failure_kind"], "assertion")
        self.assertEqual(case["tool_path"], ["safe.lookup"])
        self.assertEqual(
            case["assertions"],
            [
                {
                    "index": 0,
                    "type": "contains",
                    "passed": False,
                    "reason_code": "mismatch",
                }
            ],
        )
        self.assertNotIn("output", case)
        self.assertNotIn("error", case)

    def test_sensitive_content_requires_explicit_opt_in(self) -> None:
        opener = RecordingOpener()
        client = ResultClient(
            ResultSinkConfig(
                base_url="https://results.example",
                write_token="test-token",
                include_sensitive=True,
            ),
            opener=opener,
        )
        result = CaseResult(
            case_id="case-1",
            iteration=0,
            success=False,
            latency_ms=1,
            failure_kind="provider",
            output="sensitive output",
            error="sensitive error",
            assertions=[
                AssertionOutcome(
                    index=0,
                    type="python",
                    passed=False,
                    reason_code="exception",
                    message="sensitive assertion message",
                )
            ],
        )

        client.upload_shard("run-1", 0, [result], run_summary())

        case = json.loads(opener.requests[0][0].data)["cases"][0]
        self.assertEqual(case["output"], "sensitive output")
        self.assertEqual(case["error"], "sensitive error")
        self.assertEqual(case["assertions"][0]["message"], "sensitive assertion message")

    def test_rejects_credentials_in_result_url(self) -> None:
        with self.assertRaisesRegex(ValueError, "must not contain credentials"):
            ResultClient(
                ResultSinkConfig(
                    base_url="https://user:password@results.example",
                    write_token="test-token",
                )
            )


if __name__ == "__main__":
    unittest.main()
