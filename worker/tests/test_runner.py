from __future__ import annotations

import unittest

from agentstorm_worker.adapters import ToolLifecycleEvent
from agentstorm_worker.adapters.fake import FakeAdapter
from agentstorm_worker.config import (
    DatasetConfig,
    EvaluationConfig,
    RunConfig,
    SourceConfig,
    TargetConfig,
    PricingConfig,
    FaultRuleConfig,
    FaultScenarioConfig,
    ReliabilityConfig,
    WorkloadConfig,
)
from agentstorm_worker.models import AdapterResponse, AssertionSpec, TestCase
from agentstorm_worker.runner import WorkloadRunner


class RunnerTest(unittest.IsolatedAsyncioTestCase):
    async def test_executes_only_assigned_shard_and_evaluates_thresholds(self) -> None:
        config = RunConfig(
            run_id="demo",
            source=SourceConfig(namespace="default", name="demo"),
            dataset=DatasetConfig(name="demo-dataset", key="cases.jsonl"),
            target=TargetConfig(provider="fake"),
            workload=WorkloadConfig(concurrency=2, iterations=2, timeout_seconds=1),
            evaluation=EvaluationConfig(min_success_rate=1.0, max_error_rate=0.0),
        )
        cases = [
            TestCase(case_id="a", prompt="alpha", expected_contains="alpha"),
            TestCase(case_id="b", prompt="beta", expected_contains="beta"),
            TestCase(case_id="c", prompt="gamma", expected_contains="gamma"),
        ]
        runner = WorkloadRunner(config, FakeAdapter(delay_seconds=0), shard_index=1, shard_count=2)

        results, summary = await runner.execute(cases)

        self.assertEqual([result.case_id for result in results], ["b", "b"])
        self.assertEqual(summary.total, 2)
        self.assertGreaterEqual(summary.duration_ms, 0)
        self.assertTrue(summary.thresholds_passed)

    async def test_evaluates_all_assertions_after_one_provider_call(self) -> None:
        adapter = AssertionAdapter()
        case = TestCase(
            case_id="all",
            prompt="hello",
            expected_contains="response",
            assertions=(
                AssertionSpec(type="exact", value="response hello"),
                AssertionSpec(type="contains", value="hello"),
                AssertionSpec(type="regex", pattern=r"^response\shello$"),
                AssertionSpec(type="tool_path", path=("safe.lookup",)),
                AssertionSpec(type="latency", max_ms=1000),
                AssertionSpec(
                    type="python",
                    entrypoint="test_runner:custom_assertion",
                    config={"suffix": "hello"},
                ),
            ),
        )
        runner = WorkloadRunner(test_config(), adapter)

        results, _ = await runner.execute([case])

        self.assertEqual(adapter.calls, 1)
        self.assertTrue(results[0].success)
        self.assertEqual(results[0].tool_path, ["safe.lookup"])
        self.assertEqual(len(results[0].assertions), 7)
        self.assertTrue(all(outcome.passed for outcome in results[0].assertions))

    async def test_json_schema_and_failures_have_stable_reasons(self) -> None:
        adapter = AssertionAdapter(output='{"answer":"ok"}')
        case = TestCase(
            case_id="json",
            prompt="json",
            assertions=(
                AssertionSpec(
                    type="json_schema",
                    schema={
                        "type": "object",
                        "required": ["answer"],
                        "properties": {"answer": {"const": "ok"}},
                    },
                ),
                AssertionSpec(type="exact", value="not-the-output"),
            ),
        )

        results, _ = await WorkloadRunner(test_config(), adapter).execute([case])

        self.assertFalse(results[0].success)
        self.assertEqual(
            [outcome.reason_code for outcome in results[0].assertions],
            ["passed", "mismatch"],
        )

    async def test_python_assertion_exception_is_a_failure(self) -> None:
        case = TestCase(
            case_id="python",
            prompt="hello",
            assertions=(
                AssertionSpec(type="python", entrypoint="test_runner:raising_assertion"),
            ),
        )

        results, _ = await WorkloadRunner(test_config(), AssertionAdapter()).execute([case])

        self.assertFalse(results[0].success)
        self.assertEqual(results[0].assertions[0].reason_code, "exception")

    async def test_price_snapshot_populates_case_and_summary_cost(self) -> None:
        config = test_config()
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

        results, summary = await WorkloadRunner(config, AssertionAdapter()).execute(
            [TestCase(case_id="priced", prompt="hello")]
        )

        self.assertEqual(results[0].input_cost_usd, "0.000005000000")
        self.assertEqual(results[0].output_cost_usd, "0.000030000000")
        self.assertEqual(results[0].cost_usd, "0.000035000000")
        self.assertEqual(summary.cost_usd, "0.000035000000")

    async def test_injected_failures_have_stable_categories_and_usage(self) -> None:
        base = test_config()
        config = RunConfig(
            run_id=base.run_id,
            source=base.source,
            dataset=base.dataset,
            target=TargetConfig(provider="fake", pricing=PricingConfig("2.5", "10")),
            workload=base.workload,
            evaluation=base.evaluation,
            reliability=ReliabilityConfig(
                seed=42,
                scenario=FaultScenarioConfig(
                    source_name="faults",
                    source_key="scenario.json",
                    digest="sha256:" + "a" * 64,
                    rules=(
                        FaultRuleConfig("malformed", "malformed_response", 1),
                    ),
                ),
            ),
        )
        adapter = AssertionAdapter()
        results, _ = await WorkloadRunner(config, adapter).execute(
            [TestCase(case_id="malformed", prompt="hello")]
        )
        result = results[0]
        self.assertEqual(adapter.calls, 1)
        self.assertFalse(result.success)
        self.assertEqual(result.failure_kind, "provider")
        self.assertEqual(result.failure_category, "provider")
        self.assertEqual(result.error_code, "malformed_response")
        self.assertEqual((result.input_tokens, result.output_tokens), (2, 3))
        self.assertEqual(result.cost_usd, "0.000035000000")

    async def test_assertion_failure_is_evaluation_not_provider(self) -> None:
        result = (
            await WorkloadRunner(test_config(), AssertionAdapter()).execute(
                [
                    TestCase(
                        case_id="quality",
                        prompt="hello",
                        assertions=(AssertionSpec(type="exact", value="wrong"),),
                    )
                ]
            )
        )[0][0]
        self.assertEqual(result.failure_kind, "assertion")
        self.assertEqual(result.failure_category, "evaluation")
        self.assertEqual(result.error_code, "assertion_failed")


def test_config() -> RunConfig:
    return RunConfig(
        run_id="assertions",
        source=SourceConfig(namespace="default", name="assertions"),
        dataset=DatasetConfig(name="dataset", key="cases.jsonl"),
        target=TargetConfig(provider="fake"),
        workload=WorkloadConfig(timeout_seconds=2),
        evaluation=EvaluationConfig(),
    )


class AssertionAdapter:
    def __init__(self, output: str = "response hello") -> None:
        self.output = output
        self.calls = 0

    async def run(self, case: TestCase, lifecycle: object | None = None) -> AdapterResponse:
        del case
        self.calls += 1
        if lifecycle is not None:
            event = ToolLifecycleEvent(invocation_id="tool-1", name="safe.lookup")
            lifecycle.tool_started(event)  # type: ignore[attr-defined]
            lifecycle.tool_finished(event)  # type: ignore[attr-defined]
        return AdapterResponse(output=self.output, input_tokens=2, output_tokens=3)


def custom_assertion(context: dict[str, object], config: dict[str, object]) -> bool:
    return str(context["output"]).endswith(str(config["suffix"]))


def raising_assertion(context: dict[str, object], config: dict[str, object]) -> bool:
    del context, config
    raise RuntimeError("private assertion detail")


if __name__ == "__main__":
    unittest.main()
