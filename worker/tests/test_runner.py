from __future__ import annotations

import unittest

from agentstorm_worker.adapters.fake import FakeAdapter
from agentstorm_worker.config import EvaluationConfig, RunConfig, TargetConfig, WorkloadConfig
from agentstorm_worker.models import TestCase
from agentstorm_worker.runner import WorkloadRunner


class RunnerTest(unittest.IsolatedAsyncioTestCase):
    async def test_executes_only_assigned_shard_and_evaluates_thresholds(self) -> None:
        config = RunConfig(
            run_id="demo",
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
        self.assertTrue(summary.thresholds_passed)


if __name__ == "__main__":
    unittest.main()
