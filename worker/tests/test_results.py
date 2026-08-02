from __future__ import annotations

import json
import unittest
from typing import Any

from agentstorm_worker.config import (
    CircuitBreakerConfig,
    DatasetConfig,
    EvaluationConfig,
    FaultRuleConfig,
    FaultScenarioConfig,
    PricingConfig,
    ReliabilityConfig,
    RetryConfig,
    RunConfig,
    SourceConfig,
    TargetConfig,
    WorkloadConfig,
)
from agentstorm_worker.models import AssertionOutcome, AttemptResult, CaseResult, RunSummary
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
            failure_category="evaluation",
            error_code="assertion_failed",
            attempts=[
                AttemptResult(
                    number=1,
                    latency_ms=8,
                    outcome="succeeded",
                    retry_decision="not_needed",
                    input_tokens=3,
                    output_tokens=5,
                )
            ],
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
        self.assertEqual(case["failure_category"], "evaluation")
        self.assertEqual(case["error_code"], "assertion_failed")
        self.assertEqual(case["attempts"][0]["number"], 1)
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

    def test_registers_immutable_price_snapshot(self) -> None:
        opener = RecordingOpener()
        client = ResultClient(
            ResultSinkConfig(base_url="http://results.example", write_token="test-token"),
            opener=opener,
        )
        config = run_config()
        config = RunConfig(
            run_id=config.run_id,
            source=config.source,
            dataset=config.dataset,
            target=TargetConfig(
                provider="fake",
                pricing=PricingConfig("2.5", "10"),
            ),
            workload=config.workload,
            evaluation=config.evaluation,
        )

        client.register_run(config, expected_shards=2)

        registration = json.loads(opener.requests[0][0].data)
        self.assertEqual(
            registration["target"]["pricing"],
            {
                "input_usd_per_million_tokens": "2.5",
                "output_usd_per_million_tokens": "10",
            },
        )

    def test_registers_full_reliability_snapshot(self) -> None:
        opener = RecordingOpener()
        client = ResultClient(
            ResultSinkConfig(base_url="http://results.example", write_token="test-token"),
            opener=opener,
        )
        base = run_config()
        config = RunConfig(
            run_id=base.run_id,
            source=base.source,
            dataset=base.dataset,
            target=base.target,
            workload=base.workload,
            evaluation=base.evaluation,
            reliability=ReliabilityConfig(
                seed=42,
                retry=RetryConfig(max_attempts=3, jitter_ratio=0.2),
                circuit_breaker=CircuitBreakerConfig(5, 30000),
                scenario=FaultScenarioConfig(
                    source_name="faults",
                    source_key="scenario.json",
                    digest="sha256:" + "a" * 64,
                    rules=(
                        FaultRuleConfig(
                            name="first",
                            fault="rate_limit",
                            probability=0.25,
                            case_ids=("case-a",),
                            iterations=(0,),
                            attempts=(1,),
                        ),
                    ),
                ),
            ),
        )

        client.register_run(config, expected_shards=2)

        reliability = json.loads(opener.requests[0][0].data)["reliability"]
        self.assertEqual(reliability["seed"], 42)
        self.assertEqual(reliability["retry"]["max_attempts"], 3)
        self.assertEqual(reliability["circuit_breaker"]["failure_threshold"], 5)
        self.assertEqual(reliability["scenario"]["source"]["name"], "faults")
        self.assertEqual(
            reliability["scenario"]["document"]["rules"][0]["caseIDs"],
            ["case-a"],
        )

    def test_marks_terminal_without_error_content(self) -> None:
        opener = RecordingOpener()
        client = ResultClient(
            ResultSinkConfig(base_url="http://results.example", write_token="test-token"),
            opener=opener,
        )

        client.mark_terminal("run-1", "cancelled", "worker_terminated")

        request, _ = opener.requests[0]
        self.assertTrue(request.full_url.endswith("/v1/runs/run-1/terminal"))
        self.assertEqual(
            request.get_header("Idempotency-key"), "run/run-1/terminal/cancelled"
        )
        self.assertEqual(
            json.loads(request.data),
            {"status": "cancelled", "reason_code": "worker_terminated"},
        )


if __name__ == "__main__":
    unittest.main()
