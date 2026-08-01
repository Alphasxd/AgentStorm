from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from agentstorm_worker.config import load_dataset, load_run_config


class ConfigTest(unittest.TestCase):
    def test_loads_controller_config_and_dataset(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = root / "run.json"
            dataset_path = root / "cases.jsonl"
            config_path.write_text(
                json.dumps(
                    {
                        "run_id": "demo",
                        "target": {"provider": "fake"},
                        "workload": {"concurrency": 2, "iterations": 3, "timeout_seconds": 10},
                        "evaluation": {"minSuccessRate": 1.0},
                    }
                ),
                encoding="utf-8",
            )
            dataset_path.write_text(
                '{"id":"case-1","input":"hello","expected_contains":"hello"}\n',
                encoding="utf-8",
            )

            config = load_run_config(config_path)
            cases = load_dataset(dataset_path)

            self.assertEqual(config.workload.iterations, 3)
            self.assertEqual(config.evaluation.min_success_rate, 1.0)
            self.assertEqual(cases[0].case_id, "case-1")


if __name__ == "__main__":
    unittest.main()
