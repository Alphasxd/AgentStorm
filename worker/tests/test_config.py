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
                        "source": {"namespace": "default", "name": "demo"},
                        "dataset": {"name": "demo-dataset", "key": "cases.jsonl"},
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
            self.assertEqual(config.source.namespace, "default")
            self.assertEqual(config.dataset.name, "demo-dataset")
            self.assertEqual(config.evaluation.min_success_rate, 1.0)
            self.assertEqual(cases[0].case_id, "case-1")

    def test_rejects_duplicate_case_ids(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            dataset_path = Path(directory) / "cases.jsonl"
            dataset_path.write_text(
                '{"id":"duplicate","input":"first"}\n'
                '{"id":"duplicate","input":"second"}\n',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "duplicate dataset case id"):
                load_dataset(dataset_path)

    def test_loads_all_assertion_types_and_legacy_contains(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            dataset_path = Path(directory) / "cases.jsonl"
            dataset_path.write_text(
                json.dumps(
                    {
                        "id": "assertions",
                        "input": "hello",
                        "expected_contains": "legacy",
                        "assertions": [
                            {"type": "exact", "value": "hello"},
                            {"type": "contains", "value": "ell"},
                            {"type": "regex", "pattern": "h.*o"},
                            {"type": "json_schema", "schema": {"type": "object"}},
                            {"type": "tool_path", "path": ["lookup"]},
                            {"type": "latency", "max_ms": 10},
                            {
                                "type": "python",
                                "entrypoint": "example:check",
                                "config": {"key": "value"},
                            },
                        ],
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            case = load_dataset(dataset_path)[0]

            self.assertEqual(len(case.effective_assertions()), 8)
            self.assertEqual(case.effective_assertions()[0].type, "contains")
            self.assertEqual(case.assertions[-1].entrypoint, "example:check")

    def test_rejects_invalid_assertion_shape(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            dataset_path = Path(directory) / "cases.jsonl"
            dataset_path.write_text(
                '{"input":"hello","assertions":[{"type":"latency","max_ms":0}]}\n',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "max_ms must be positive"):
                load_dataset(dataset_path)


if __name__ == "__main__":
    unittest.main()
