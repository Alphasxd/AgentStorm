from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

import client
import generate
import provider


TOKEN = "unit-test-read-token"
RUN_ID = "replay-run"


def result_case(
    case_id: str = "case-1",
    iteration: int = 0,
    output: str = "durable output",
) -> dict[str, Any]:
    return {
        "idempotency_key": f"{case_id}-{iteration}",
        "case_id": case_id,
        "iteration": iteration,
        "success": True,
        "latency_ms": 12.6,
        "input_tokens": 4,
        "output_tokens": 7,
        "output": output,
        "tool_path": ["fake.echo"],
        "assertions": [
            {"index": 0, "type": "contains", "passed": True, "reason_code": "passed"}
        ],
        "input_cost_usd": "0.000004000000",
        "output_cost_usd": "0.000014000000",
        "cost_usd": "0.000018000000",
    }


class StubState:
    def __init__(self, status: str, pages: dict[str, dict[str, Any]]) -> None:
        self.status = status
        self.pages = pages
        self.requests: list[str] = []
        self.authorizations: list[str] = []


@contextmanager
def stub_api(state: StubState) -> Iterator[str]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            state.requests.append(self.path)
            state.authorizations.append(self.headers.get("Authorization", ""))
            parsed = urlsplit(self.path)
            if parsed.path == f"/v1/runs/{RUN_ID}":
                self.send_json({"id": RUN_ID, "status": state.status})
                return
            if parsed.path == f"/v1/runs/{RUN_ID}/cases":
                cursor = parse_qs(parsed.query).get("cursor", [""])[0]
                page = state.pages.get(cursor)
                if page is None:
                    self.send_error(400)
                    return
                self.send_json(page)
                return
            self.send_error(404)

        def send_json(self, payload: dict[str, Any]) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


class ResultAPIClientTests(unittest.TestCase):
    def test_reads_all_pages_with_bearer_token(self) -> None:
        state = StubState(
            "complete",
            {
                "": {"cases": [result_case()], "next_cursor": "page-2"},
                "page-2": {"cases": [result_case("case-2")]},
            },
        )
        with stub_api(state) as base_url:
            run, cases = client.ResultAPIClient(base_url, TOKEN).completed_replay(RUN_ID)
        self.assertEqual(run["status"], "complete")
        self.assertEqual([item["case_id"] for item in cases], ["case-1", "case-2"])
        self.assertTrue(any("cursor=page-2" in path for path in state.requests))
        self.assertEqual(set(state.authorizations), {f"Bearer {TOKEN}"})

    def test_rejects_incomplete_run_before_case_read(self) -> None:
        state = StubState("collecting", {"": {"cases": []}})
        with stub_api(state) as base_url:
            with self.assertRaisesRegex(client.ResultAPIError, "not complete"):
                client.ResultAPIClient(base_url, TOKEN).completed_replay(RUN_ID)
        self.assertEqual(len(state.requests), 1)

    def test_rejects_missing_durable_output(self) -> None:
        case = result_case()
        del case["output"]
        state = StubState("complete", {"": {"cases": [case]}})
        with stub_api(state) as base_url:
            with self.assertRaisesRegex(client.ResultAPIError, "durable output is unavailable"):
                client.ResultAPIClient(base_url, TOKEN).completed_replay(RUN_ID)

    def test_url_rejects_credentials(self) -> None:
        with self.assertRaisesRegex(client.ResultAPIError, "must not contain credentials"):
            client.normalize_base_url("https://token@example.test")


class GeneratorTests(unittest.TestCase):
    def test_maps_supported_assertions_without_output_or_token(self) -> None:
        api_case = result_case(output="MODEL_OUTPUT_CANARY")
        state = StubState("complete", {"": {"cases": [api_case]}})
        dataset = {
            "id": "case-1",
            "input": "prompt",
            "expected_contains": "legacy",
            "assertions": [
                {"type": "exact", "value": "answer"},
                {"type": "contains", "value": "needle"},
                {"type": "regex", "pattern": "^answer"},
                {"type": "json_schema", "schema": {"type": "object"}},
                {"type": "latency", "max_ms": 100},
                {"type": "tool_path", "path": ["fake.echo"]},
                {"type": "python", "entrypoint": "trusted:check"},
            ],
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset_path = root / "cases.jsonl"
            output_path = root / "promptfoo.json"
            dataset_path.write_text(json.dumps(dataset) + "\n", encoding="utf-8")
            with stub_api(state) as base_url, patch.dict(
                os.environ, {client.READ_TOKEN_ENV: TOKEN}, clear=False
            ):
                generate.generate(base_url, RUN_ID, dataset_path, output_path)
            rendered = output_path.read_text(encoding="utf-8")
            config = json.loads(rendered)
        self.assertNotIn(TOKEN, rendered)
        self.assertNotIn("MODEL_OUTPUT_CANARY", rendered)
        self.assertEqual(
            [item["type"] for item in config["tests"][0]["assert"]],
            ["contains", "equals", "contains", "regex", "is-json"],
        )
        metadata = config["tests"][0]["metadata"]["agentstorm"]
        self.assertEqual(metadata["tool_path"], ["fake.echo"])
        self.assertEqual(metadata["assertions"][0]["type"], "contains")
        self.assertNotIn("message", metadata["assertions"][0])
        self.assertEqual(
            set(config["providers"][0]["config"]), {"resultApiUrl", "runId"}
        )

    def test_rejects_result_for_unknown_dataset_case(self) -> None:
        with self.assertRaisesRegex(client.ResultAPIError, "unknown dataset case"):
            generate.build_config(
                "https://results.example.test",
                RUN_ID,
                {"known": {"id": "known", "input": "prompt"}},
                [result_case("unknown")],
            )


class ProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        provider.clear_cache()

    def test_replays_output_usage_cost_and_metadata_without_refetch(self) -> None:
        state = StubState("complete", {"": {"cases": [result_case()]}})
        with stub_api(state) as base_url, patch.dict(
            os.environ, {client.READ_TOKEN_ENV: TOKEN}, clear=False
        ):
            options = {"config": {"resultApiUrl": base_url, "runId": RUN_ID}}
            context = {
                "vars": {"agentstorm_case_id": "case-1", "agentstorm_iteration": 0}
            }
            first = provider.call_api("ignored", options, context)
            second = provider.call_api("ignored again", options, context)
        self.assertEqual(first, second)
        self.assertEqual(first["output"], "durable output")
        self.assertEqual(
            first["tokenUsage"],
            {"prompt": 4, "completion": 7, "total": 11, "numRequests": 0},
        )
        self.assertEqual(first["cost"], 0.000018)
        self.assertEqual(first["latencyMs"], 13)
        self.assertEqual(
            first["metadata"]["agentstorm"]["tool_path"], ["fake.echo"]
        )
        self.assertEqual(len(state.requests), 2)

    def test_returns_safe_error_when_token_is_missing(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            response = provider.call_api(
                "ignored",
                {
                    "config": {
                        "resultApiUrl": "https://results.example.test",
                        "runId": RUN_ID,
                    }
                },
                {
                    "vars": {
                        "agentstorm_case_id": "case-1",
                        "agentstorm_iteration": 0,
                    }
                },
            )
        self.assertEqual(response["output"], "")
        self.assertIn(client.READ_TOKEN_ENV, response["error"])


if __name__ == "__main__":
    unittest.main()
