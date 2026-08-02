from __future__ import annotations

import argparse
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agentstorm_worker.__main__ import _finish_cancelled, execute
from agentstorm_worker.adapters import AgentLifecycle
from agentstorm_worker.models import AdapterResponse, RunSummary


class OrderingAdapter:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def run(
        self, case: object, lifecycle: AgentLifecycle | None = None
    ) -> AdapterResponse:
        del case, lifecycle
        self.events.append("provider")
        return AdapterResponse(output="hello")


class RecordingResultClient:
    def __init__(
        self,
        events: list[str],
        fail_registration: bool = False,
        fail_upload: bool = False,
    ) -> None:
        self.events = events
        self.fail_registration = fail_registration
        self.fail_upload = fail_upload

    def register_run(self, config: object, expected_shards: int) -> None:
        self.events.append("register")
        if self.fail_registration:
            raise RuntimeError("registration failed")

    def upload_shard(self, *args: object) -> None:
        self.events.append("upload")
        if self.fail_upload:
            raise RuntimeError("upload failed")

    def mark_terminal(self, run_id: str, status: str, reason_code: str) -> None:
        del run_id, status, reason_code
        self.events.append("terminal")


class MainTest(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_flush_marks_terminal_before_partial_upload(self) -> None:
        events: list[str] = []
        client = RecordingResultClient(events)
        summary = RunSummary(
            run_id="run-1",
            shard_index=0,
            shard_count=1,
            duration_ms=1,
            total=0,
            succeeded=0,
            failed=0,
            success_rate=0,
            error_rate=0,
            p95_latency_ms=0,
            input_tokens=0,
            output_tokens=0,
            thresholds_passed=True,
            threshold_failures=[],
        )
        with tempfile.TemporaryDirectory() as directory:
            await _finish_cancelled(client, "run-1", 0, [], summary, directory)
        self.assertEqual(events, ["terminal", "upload"])

    async def test_registers_before_provider_and_uploads_after_execution(self) -> None:
        events: list[str] = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = root / "run.json"
            dataset_path = root / "cases.jsonl"
            config_path.write_text(
                json.dumps(
                    {
                        "run_id": "run-1",
                        "target": {"provider": "fake"},
                        "workload": {"timeout_seconds": 1},
                    }
                ),
                encoding="utf-8",
            )
            dataset_path.write_text('{"id":"case-1","input":"hello"}\n', encoding="utf-8")
            args = argparse.Namespace(
                command="run",
                config=str(config_path),
                dataset=str(dataset_path),
                output=str(root / "out"),
            )
            result_client = RecordingResultClient(events)
            with (
                patch(
                    "agentstorm_worker.__main__.ResultClient.from_environment",
                    return_value=result_client,
                ),
                patch(
                    "agentstorm_worker.__main__.create_adapter",
                    return_value=OrderingAdapter(events),
                ),
                patch.dict(
                    "os.environ",
                    {"AGENTSTORM_SHARD_INDEX": "0", "AGENTSTORM_SHARD_COUNT": "1"},
                ),
            ):
                exit_code = await execute(args)

        self.assertEqual(exit_code, 0)
        self.assertEqual(events, ["register", "provider", "upload"])

    async def test_registration_failure_prevents_provider_calls(self) -> None:
        events: list[str] = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = root / "run.json"
            dataset_path = root / "cases.jsonl"
            config_path.write_text(
                json.dumps({"run_id": "run-1", "target": {"provider": "fake"}}),
                encoding="utf-8",
            )
            dataset_path.write_text('{"id":"case-1","input":"hello"}\n', encoding="utf-8")
            args = argparse.Namespace(
                command="run",
                config=str(config_path),
                dataset=str(dataset_path),
                output=str(root / "out"),
            )
            with (
                patch(
                    "agentstorm_worker.__main__.ResultClient.from_environment",
                    return_value=RecordingResultClient(events, fail_registration=True),
                ),
                patch(
                    "agentstorm_worker.__main__.create_adapter",
                    return_value=OrderingAdapter(events),
                ),
            ):
                exit_code = await execute(args)

        self.assertEqual(exit_code, 3)
        self.assertEqual(events, ["register"])

    async def test_upload_failure_fails_the_worker(self) -> None:
        events: list[str] = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = root / "run.json"
            dataset_path = root / "cases.jsonl"
            config_path.write_text(
                json.dumps({"run_id": "run-1", "target": {"provider": "fake"}}),
                encoding="utf-8",
            )
            dataset_path.write_text('{"id":"case-1","input":"hello"}\n', encoding="utf-8")
            args = argparse.Namespace(
                command="run",
                config=str(config_path),
                dataset=str(dataset_path),
                output=str(root / "out"),
            )
            with (
                patch(
                    "agentstorm_worker.__main__.ResultClient.from_environment",
                    return_value=RecordingResultClient(events, fail_upload=True),
                ),
                patch(
                    "agentstorm_worker.__main__.create_adapter",
                    return_value=OrderingAdapter(events),
                ),
                patch.dict(
                    "os.environ",
                    {"AGENTSTORM_SHARD_INDEX": "0", "AGENTSTORM_SHARD_COUNT": "1"},
                ),
            ):
                exit_code = await execute(args)

        self.assertEqual(exit_code, 3)
        self.assertEqual(events, ["register", "provider", "upload", "terminal"])


if __name__ == "__main__":
    unittest.main()
