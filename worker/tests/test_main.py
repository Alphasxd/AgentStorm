from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agentstorm_worker.__main__ import _finish_cancelled, execute
from agentstorm_worker.adapters import AgentLifecycle
from agentstorm_worker.models import AdapterResponse, RunSummary
from agentstorm_worker.results import QueueClaim, ResultSinkError


class OrderingAdapter:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def run(
        self, case: object, lifecycle: AgentLifecycle | None = None
    ) -> AdapterResponse:
        del case, lifecycle
        self.events.append("provider")
        return AdapterResponse(output="hello")


class BlockingAdapter:
    async def run(
        self, case: object, lifecycle: AgentLifecycle | None = None
    ) -> AdapterResponse:
        del case, lifecycle
        await asyncio.sleep(60)
        return AdapterResponse(output="unreachable")


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


class QueueResultClient(RecordingResultClient):
    def __init__(
        self,
        events: list[str],
        fail_renew: bool = False,
        fail_upload: bool = False,
    ) -> None:
        super().__init__(events, fail_upload=fail_upload)
        self.fail_renew = fail_renew

    def upload_shard(self, *args: object) -> None:
        self.events.append("upload")
        if self.fail_upload:
            raise ResultSinkError("result API is unavailable")

    def claim_shard(self, run_id: str, worker_id: str) -> QueueClaim:
        del run_id, worker_id
        self.events.append("claim")
        return QueueClaim(
            shard_index=0 if self.fail_renew else 1,
            lease_token="queue-lease",
            renew_after_ms=1 if self.fail_renew else 60000,
        )

    def renew_shard(self, run_id: str, shard_index: int, lease_token: str) -> int:
        del run_id, shard_index, lease_token
        self.events.append("renew")
        if self.fail_renew:
            raise RuntimeError("lease lost")
        return 60000


class MainTest(unittest.IsolatedAsyncioTestCase):
    async def test_queue_lease_loss_requeues_shard_without_terminating_run(self) -> None:
        events: list[str] = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = root / "run.json"
            dataset_path = root / "cases.jsonl"
            config_path.write_text(
                json.dumps({"run_id": "run-1", "target": {"provider": "fake"}}),
                encoding="utf-8",
            )
            dataset_path.write_text('{"id":"case-1","input":"one"}\n', encoding="utf-8")
            args = argparse.Namespace(
                command="run", config=str(config_path), dataset=str(dataset_path), output=str(root / "out")
            )
            client = QueueResultClient(events, fail_renew=True)
            with (
                patch("agentstorm_worker.__main__.ResultClient.from_environment", return_value=client),
                patch("agentstorm_worker.__main__.create_adapter", return_value=BlockingAdapter()),
                patch.dict(
                    "os.environ",
                    {
                        "AGENTSTORM_RESULT_PRE_REGISTERED": "true",
                        "AGENTSTORM_QUEUE_MODE": "true",
                        "AGENTSTORM_SHARD_COUNT": "1",
                        "AGENTSTORM_WORKER_ID": "worker-1",
                    },
                    clear=False,
                ),
            ):
                exit_code = await execute(args)

        self.assertEqual(exit_code, 3)
        self.assertIn("renew", events)
        self.assertNotIn("terminal", events)

    async def test_queued_worker_claims_before_provider_and_uploads_with_lease(self) -> None:
        events: list[str] = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = root / "run.json"
            dataset_path = root / "cases.jsonl"
            config_path.write_text(
                json.dumps({"run_id": "run-1", "target": {"provider": "fake"}}),
                encoding="utf-8",
            )
            dataset_path.write_text(
                '{"id":"case-0","input":"zero"}\n{"id":"case-1","input":"one"}\n',
                encoding="utf-8",
            )
            args = argparse.Namespace(
                command="run", config=str(config_path), dataset=str(dataset_path), output=str(root / "out")
            )
            client = QueueResultClient(events)
            with (
                patch("agentstorm_worker.__main__.ResultClient.from_environment", return_value=client),
                patch("agentstorm_worker.__main__.create_adapter", return_value=OrderingAdapter(events)),
                patch.dict(
                    "os.environ",
                    {
                        "AGENTSTORM_RESULT_PRE_REGISTERED": "true",
                        "AGENTSTORM_QUEUE_MODE": "true",
                        "AGENTSTORM_SHARD_COUNT": "2",
                        "AGENTSTORM_WORKER_ID": "worker-1",
                    },
                    clear=False,
                ),
            ):
                exit_code = await execute(args)

        self.assertEqual(exit_code, 0)
        self.assertEqual(events, ["claim", "provider", "upload"])

    async def test_queued_upload_failure_requeues_without_terminating_run(self) -> None:
        events: list[str] = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = root / "run.json"
            dataset_path = root / "cases.jsonl"
            config_path.write_text(
                json.dumps({"run_id": "run-1", "target": {"provider": "fake"}}),
                encoding="utf-8",
            )
            dataset_path.write_text(
                '{"id":"case-0","input":"zero"}\n'
                '{"id":"case-1","input":"one"}\n',
                encoding="utf-8",
            )
            args = argparse.Namespace(
                command="run",
                config=str(config_path),
                dataset=str(dataset_path),
                output=str(root / "out"),
            )
            client = QueueResultClient(events, fail_upload=True)
            with (
                patch(
                    "agentstorm_worker.__main__.ResultClient.from_environment",
                    return_value=client,
                ),
                patch(
                    "agentstorm_worker.__main__.create_adapter",
                    return_value=OrderingAdapter(events),
                ),
                patch.dict(
                    "os.environ",
                    {
                        "AGENTSTORM_RESULT_PRE_REGISTERED": "true",
                        "AGENTSTORM_QUEUE_MODE": "true",
                        "AGENTSTORM_SHARD_COUNT": "2",
                        "AGENTSTORM_WORKER_ID": "worker-1",
                    },
                    clear=False,
                ),
            ):
                exit_code = await execute(args)

        self.assertEqual(exit_code, 3)
        self.assertEqual(events, ["claim", "provider", "upload"])

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
