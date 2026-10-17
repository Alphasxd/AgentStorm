from __future__ import annotations

import asyncio
import json
import os
import socket
import struct
import threading
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from agentstorm_worker.adapters import create_adapter
from agentstorm_worker.adapters.base import ToolLifecycleEvent
from agentstorm_worker.adapters.openai_compatible import (
    FINISH_REASON_KEY,
    PROVIDER_ERROR_CODE_KEY,
    RATE_LIMIT_CODES_KEY,
    RATE_LIMIT_EVENTS_KEY,
    UNKNOWN_CALL_INPUT_TOKENS,
    UNKNOWN_CALL_OUTPUT_TOKENS,
    ChatTool,
    OpenAICompatibleAdapter,
    retry_after_seconds,
    retry_delay_seconds,
    validate_base_url,
)
from agentstorm_worker.benchmarks.sre import _unwrap_json_fence, create_adapter as create_sre
from agentstorm_worker.config import load_run_config
from agentstorm_worker.execution_errors import ExecutionError
from agentstorm_worker.models import TestCase
from agentstorm_worker.runner import WorkloadRunner


class RecordingLifecycle:
    def __init__(self) -> None:
        self.started: list[str] = []
        self.finished: list[str] = []

    def tool_started(self, event: ToolLifecycleEvent) -> None:
        self.started.append(event.name)

    def tool_finished(self, event: ToolLifecycleEvent, error_type: str = "") -> None:
        self.finished.append(error_type or event.name)

    def handed_off(self, event: object) -> None:
        del event


def _serve(test: unittest.TestCase, respond) -> str:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length) if length else b""
            status, payload = respond(self.headers, json.loads(raw.decode("utf-8")))
            encoded = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: Any) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def close() -> None:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    test.addCleanup(close)
    host, port = server.server_address[:2]
    return f"http://{host}:{port}/v1"


def _drop_after_headers(test: unittest.TestCase, status: int) -> str:
    """Accept the request, send headers, then reset before the body."""
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)
    port = listener.getsockname()[1]

    def serve() -> None:
        while True:
            try:
                connection, _ = listener.accept()
            except OSError:
                return
            try:
                connection.settimeout(2)
                received = b""
                while b"\r\n\r\n" not in received:
                    chunk = connection.recv(4096)
                    if not chunk:
                        break
                    received += chunk
                reason = "OK" if status == 200 else "Error"
                header = (
                    f"HTTP/1.1 {status} {reason}\r\n"
                    "Content-Type: application/json\r\n"
                    "Content-Length: 100\r\n"
                    "Connection: close\r\n"
                    "\r\n"
                )
                connection.sendall(header.encode("ascii"))
                connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            except OSError:
                pass
            finally:
                connection.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()

    def close() -> None:
        listener.close()
        thread.join(timeout=2)

    test.addCleanup(close)
    return f"http://127.0.0.1:{port}/v1"


def _serve_with_headers(test: unittest.TestCase, respond) -> str:
    """respond(headers, body) returns (status, payload, extra headers)."""

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length) if length else b""
            status, payload, extra = respond(self.headers, json.loads(raw.decode("utf-8")))
            encoded = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            for name, value in extra.items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: Any) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def close() -> None:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    test.addCleanup(close)
    host, port = server.server_address[:2]
    return f"http://{host}:{port}/v1"


def _completion(content: str, usage: dict[str, int] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "choices": [{"message": {"role": "assistant", "content": content}}],
    }
    if usage is not None:
        payload["usage"] = usage
    return payload


class OpenAICompatibleAdapterTest(unittest.TestCase):
    def test_completion_sums_usage_and_counts_model_calls(self) -> None:
        def respond(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            self.assertEqual(body["model"], "pinned-test")
            self.assertFalse(body["stream"])
            self.assertNotIn("Authorization", headers)
            return 200, _completion("answer", {"prompt_tokens": 3, "completion_tokens": 5})

        base_url = _serve(self, respond)
        adapter = OpenAICompatibleAdapter("pinned-test", base_url, api_key="")
        response = asyncio.run(adapter.run(TestCase(case_id="1", prompt="hello")))
        self.assertEqual(response.output, "answer")
        self.assertEqual((response.input_tokens, response.output_tokens), (3, 5))
        self.assertEqual(response.model_call_count, 1)

    def test_tool_loop_records_lifecycle_and_accepts_alias_usage(self) -> None:
        calls: list[dict[str, Any]] = []

        async def lookup(city: str) -> dict[str, str]:
            return {"city": city}

        def respond(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            del headers
            calls.append(body)
            if len(calls) == 1:
                return 200, {
                    "choices": [
                        {
                            "message": {
                                "role": "assistant",
                                "content": None,
                                "tool_calls": [
                                    {
                                        "id": "call-1",
                                        "type": "function",
                                        "function": {
                                            "name": "lookup",
                                            "arguments": {"city": "Shanghai"},
                                        },
                                    }
                                ],
                            }
                        }
                    ],
                    "usage": {"input_tokens": 4, "output_tokens": 2},
                }
            self.assertEqual(body["messages"][-1]["role"], "tool")
            self.assertEqual(json.loads(body["messages"][-1]["content"]), {"city": "Shanghai"})
            return 200, _completion("done", {"prompt_tokens": 6, "completion_tokens": 1})

        base_url = _serve(self, respond)
        lifecycle = RecordingLifecycle()
        adapter = OpenAICompatibleAdapter(
            "pinned-test",
            base_url,
            api_key="",
            tools=[
                ChatTool(
                    "lookup",
                    "Lookup a city.",
                    {"type": "object", "properties": {"city": {"type": "string"}}},
                    lookup,
                )
            ],
        )
        response = asyncio.run(
            adapter.run(TestCase(case_id="1", prompt="weather"), lifecycle=lifecycle)
        )
        self.assertEqual(response.output, "done")
        self.assertEqual(response.model_call_count, 2)
        self.assertEqual((response.input_tokens, response.output_tokens), (10, 3))
        self.assertEqual(lifecycle.started, ["lookup"])
        self.assertEqual(lifecycle.finished, ["lookup"])

    def test_missing_usage_stays_unknown_without_becoming_zero(self) -> None:
        def respond(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            del headers, body
            return 200, _completion("answer", None)

        base_url = _serve(self, respond)
        adapter = OpenAICompatibleAdapter("pinned-test", base_url, api_key="")
        response = asyncio.run(adapter.run(TestCase(case_id="1", prompt="hello")))
        self.assertIsNone(response.input_tokens)
        self.assertIsNone(response.output_tokens)
        self.assertEqual(response.model_call_count, 1)

    def test_http_errors_hide_bodies_and_credentials(self) -> None:
        secret = "super-secret-token"

        def respond(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            del body
            self.assertEqual(headers["Authorization"], "Bearer " + secret)
            return 401, {"error": {"message": secret}}

        base_url = _serve(self, respond)
        adapter = OpenAICompatibleAdapter("pinned-test", base_url, api_key=secret)
        with self.assertRaises(ExecutionError) as raised:
            asyncio.run(adapter.run(TestCase(case_id="1", prompt="hello")))
        self.assertEqual(str(raised.exception), "provider/http_4xx")
        self.assertNotIn(secret, str(raised.exception))
        self.assertFalse(raised.exception.safe_to_retry)

    def test_provider_error_code_is_allowlisted_and_secrets_are_dropped(self) -> None:
        secret = "super-secret-token"
        calls = {"n": 0}

        def respond(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any], dict[str, str]]:
            del headers, body
            calls["n"] += 1
            if calls["n"] == 1:
                return 401, {"error": {"code": secret, "message": secret}}, {}
            return (
                429,
                {"error": {"code": 1305, "message": secret + " overloaded for " + secret}},
                {"Retry-After": "0"},
            )

        base_url = _serve_with_headers(self, respond)
        leaked = OpenAICompatibleAdapter(
            "pinned-test", base_url, api_key=secret, max_http_attempts=1, retry_sleep=_no_sleep
        )
        with self.assertRaises(ExecutionError) as hidden:
            asyncio.run(leaked.run(TestCase(case_id="1", prompt="hello")))
        self.assertNotIn(secret, str(hidden.exception))
        self.assertNotIn(
            PROVIDER_ERROR_CODE_KEY, hidden.exception.response.metadata
        )

        coded = OpenAICompatibleAdapter(
            "pinned-test", base_url, api_key=secret, max_http_attempts=1, retry_sleep=_no_sleep
        )
        with self.assertRaises(ExecutionError) as raised:
            asyncio.run(coded.run(TestCase(case_id="1", prompt="hello")))
        self.assertEqual(raised.exception.code, "rate_limited")
        self.assertEqual(raised.exception.response.metadata[PROVIDER_ERROR_CODE_KEY], "1305")
        self.assertEqual(raised.exception.response.metadata[RATE_LIMIT_CODES_KEY], ["1305"])
        self.assertNotIn(secret, str(raised.exception))
        self.assertNotIn("overloaded", str(raised.exception))
        rendered = json.dumps(raised.exception.response.metadata)
        self.assertNotIn(secret, rendered)
        self.assertNotIn("overloaded", rendered)

    def test_rate_limit_reuses_runner_retry_and_price_snapshot(self) -> None:
        state = {"calls": 0}

        def respond(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            del headers, body
            state["calls"] += 1
            if state["calls"] == 1:
                return 429, {"error": {"message": "slow down"}}
            return 200, _completion("ok", {"prompt_tokens": 10, "completion_tokens": 5})

        base_url = _serve(self, respond)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(
                json.dumps(
                    {
                        "target": {
                            "provider": "openai-compatible",
                            "model": "pinned-test",
                            "baseURL": base_url,
                            "pricing": {
                                "inputUSDPerMillionTokens": "2",
                                "outputUSDPerMillionTokens": "4",
                            },
                        },
                        "workload": {"concurrency": 1, "iterations": 1, "timeout_seconds": 30},
                        "reliability": {
                            "retry": {
                                "maxAttempts": 2,
                                "initialBackoffMs": 1,
                                "jitterRatio": 0,
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            config = load_run_config(path)
        adapter = OpenAICompatibleAdapter(
            "pinned-test", base_url, api_key="", max_http_attempts=1, retry_sleep=_no_sleep
        )
        result = asyncio.run(
            WorkloadRunner(config, adapter).execute([TestCase(case_id="1", prompt="hello")])
        )[0][0]
        self.assertTrue(result.success)
        self.assertEqual(result.attempts[0].error_code, "rate_limited")
        self.assertEqual(result.attempts[0].retry_decision, "retry_safe")
        self.assertEqual(len(result.attempts), 2)
        self.assertEqual(result.cost_usd, "0.000040000000")
        self.assertEqual(state["calls"], 2)

    def test_rate_limit_retries_one_call_and_keeps_completed_tools(self) -> None:
        calls: list[dict[str, Any]] = []
        tool_runs = {"n": 0}
        slept: list[float] = []

        async def lookup() -> dict[str, str]:
            tool_runs["n"] += 1
            return {"ok": "yes"}

        async def record_sleep(delay: float) -> None:
            slept.append(delay)

        def respond(
            headers: Any, body: dict[str, Any]
        ) -> tuple[int, dict[str, Any], dict[str, str]]:
            del headers
            calls.append(body)
            if len(calls) == 1:
                return 200, {
                    "choices": [
                        {
                            "finish_reason": "tool_calls",
                            "message": {
                                "role": "assistant",
                                "content": "",
                                "tool_calls": [
                                    {
                                        "id": "call-1",
                                        "type": "function",
                                        "function": {"name": "lookup", "arguments": {}},
                                    }
                                ],
                            },
                        }
                    ],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 4},
                }, {}
            if len(calls) == 2:
                self.assertEqual(body["messages"][-1]["role"], "tool")
                return 429, {"error": {"code": 1305, "message": "overloaded"}}, {"Retry-After": "2"}
            self.assertIn("thinking", body)
            self.assertEqual(body["max_tokens"], 64)
            return 200, _completion("done", {"prompt_tokens": 6, "completion_tokens": 1}), {}

        base_url = _serve_with_headers(self, respond)
        adapter = OpenAICompatibleAdapter(
            "pinned-test",
            base_url,
            api_key="",
            request_fields={"max_tokens": 64, "thinking": {"type": "disabled"}},
            retry_sleep=record_sleep,
            tools=[
                ChatTool(
                    "lookup",
                    "Lookup.",
                    {"type": "object", "properties": {}},
                    lookup,
                )
            ],
        )
        response = asyncio.run(adapter.run(TestCase(case_id="1", prompt="hello")))
        self.assertEqual(response.output, "done")
        self.assertEqual(tool_runs["n"], 1)
        self.assertEqual(len(calls), 3)
        self.assertEqual(slept, [2.0])
        self.assertEqual((response.input_tokens, response.output_tokens), (16, 5))
        self.assertEqual(response.model_call_count, 2)
        self.assertEqual(response.metadata[RATE_LIMIT_EVENTS_KEY], 1)
        self.assertEqual(response.metadata[RATE_LIMIT_CODES_KEY], ["1305"])
        self.assertNotIn("agentstorm.usage.unknown_calls", response.metadata)
        self.assertNotIn("overloaded", json.dumps(response.metadata))

    def test_ambiguous_5xx_after_a_tool_call_is_priced_and_not_retried(self) -> None:
        calls = {"n": 0}
        tool_runs = {"n": 0}
        slept: list[float] = []

        async def lookup() -> dict[str, str]:
            tool_runs["n"] += 1
            return {"ok": "yes"}

        async def record_sleep(delay: float) -> None:
            slept.append(delay)

        def respond(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            del headers, body
            calls["n"] += 1
            if calls["n"] == 1:
                return 200, {
                    "choices": [
                        {
                            "finish_reason": "tool_calls",
                            "message": {
                                "role": "assistant",
                                "content": "",
                                "tool_calls": [
                                    {
                                        "id": "call-1",
                                        "type": "function",
                                        "function": {"name": "lookup", "arguments": {}},
                                    }
                                ],
                            },
                        }
                    ],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 4},
                }
            return 502, {"error": {"code": "bad_gateway", "message": "later"}}

        base_url = _serve(self, respond)
        adapter = OpenAICompatibleAdapter(
            "pinned-test",
            base_url,
            api_key="",
            request_fields={"max_tokens": 64},
            retry_sleep=record_sleep,
            tools=[
                ChatTool(
                    "lookup",
                    "Lookup.",
                    {"type": "object", "properties": {}},
                    lookup,
                )
            ],
        )
        with self.assertRaises(ExecutionError) as raised:
            asyncio.run(adapter.run(TestCase(case_id="1", prompt="hello")))
        self.assertEqual(calls["n"], 2)
        self.assertEqual(tool_runs["n"], 1)
        self.assertEqual(slept, [])
        self.assertEqual(raised.exception.code, "http_5xx")
        self.assertTrue(raised.exception.ambiguous)
        self.assertFalse(raised.exception.safe_to_retry)
        response = raised.exception.response
        self.assertEqual(response.metadata[PROVIDER_ERROR_CODE_KEY], "bad_gateway")
        self.assertNotIn(RATE_LIMIT_EVENTS_KEY, response.metadata)
        self.assertEqual(response.model_call_count, 1)
        self.assertEqual(response.input_tokens, 10 + UNKNOWN_CALL_INPUT_TOKENS)
        self.assertEqual(response.output_tokens, 4 + 64)
        self.assertEqual(response.metadata["agentstorm.usage.unknown_calls"], 1)
        self.assertNotIn("later", str(raised.exception))

    def test_ambiguous_5xx_is_single_shot_and_priced_once(self) -> None:
        for status in (502, 503, 504):
            with self.subTest(status=status):
                calls = {"n": 0}

                def respond(
                    headers: Any, body: dict[str, Any], status: int = status
                ) -> tuple[int, dict[str, Any]]:
                    del headers, body
                    calls["n"] += 1
                    return status, {"error": {"code": 1305, "message": "overloaded body"}}

                base_url = _serve(self, respond)
                adapter = OpenAICompatibleAdapter(
                    "pinned-test",
                    base_url,
                    api_key="",
                    request_fields={"max_tokens": 32},
                    retry_sleep=_no_sleep,
                )
                with self.assertRaises(ExecutionError) as raised:
                    asyncio.run(adapter.run(TestCase(case_id="1", prompt="hello")))
                self.assertEqual(calls["n"], 1)
                self.assertEqual(raised.exception.code, "http_5xx")
                self.assertTrue(raised.exception.ambiguous)
                self.assertEqual(
                    raised.exception.response.metadata[PROVIDER_ERROR_CODE_KEY], "1305"
                )
                self.assertNotIn(RATE_LIMIT_EVENTS_KEY, raised.exception.response.metadata)
                self.assertEqual(raised.exception.response.model_call_count, 0)
                self.assertEqual(
                    raised.exception.response.input_tokens, UNKNOWN_CALL_INPUT_TOKENS
                )
                self.assertEqual(raised.exception.response.output_tokens, 32)
                self.assertEqual(
                    raised.exception.response.metadata["agentstorm.usage.unknown_calls"], 1
                )
                self.assertNotIn("overloaded body", str(raised.exception))

    def test_ambiguous_case_retry_prices_every_attempt(self) -> None:
        calls = {"n": 0}

        def respond(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            del headers, body
            calls["n"] += 1
            if calls["n"] == 1:
                return 502, {"error": {"code": "bad_gateway", "message": "hidden"}}
            return 200, _completion("done", {"prompt_tokens": 10, "completion_tokens": 5})

        base_url = _serve(self, respond)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(
                json.dumps(
                    {
                        "target": {
                            "provider": "openai-compatible",
                            "model": "pinned-test",
                            "baseURL": base_url,
                            "pricing": {
                                "inputUSDPerMillionTokens": "1",
                                "outputUSDPerMillionTokens": "2",
                            },
                        },
                        "workload": {"concurrency": 1, "iterations": 1, "timeout_seconds": 30},
                        "reliability": {
                            "retry": {
                                "maxAttempts": 2,
                                "initialBackoffMs": 1,
                                "jitterRatio": 0,
                                "allowAmbiguousRetries": True,
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            config = load_run_config(path)
        adapter = OpenAICompatibleAdapter(
            "pinned-test",
            base_url,
            api_key="",
            request_fields={"max_tokens": 32},
            retry_sleep=_no_sleep,
        )
        result = asyncio.run(
            WorkloadRunner(config, adapter).execute([TestCase(case_id="1", prompt="hello")])
        )[0][0]
        self.assertTrue(result.success)
        self.assertEqual(len(result.attempts), 2)
        self.assertEqual(result.attempts[0].error_code, "http_5xx")
        self.assertEqual(result.attempts[0].retry_decision, "retry_ambiguous")
        self.assertEqual(calls["n"], 2)
        self.assertEqual(result.input_tokens, UNKNOWN_CALL_INPUT_TOKENS + 10)
        self.assertEqual(result.output_tokens, 32 + 5)
        self.assertEqual(result.cost_usd, "0.008276000000")

        blocked_calls = {"n": 0}

        def blocked(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            del headers, body
            blocked_calls["n"] += 1
            return 503, {"error": {"code": 1305, "message": "hidden"}}

        blocked_url = _serve(self, blocked)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(
                json.dumps(
                    {
                        "target": {
                            "provider": "openai-compatible",
                            "model": "pinned-test",
                            "baseURL": blocked_url,
                            "pricing": {
                                "inputUSDPerMillionTokens": "1",
                                "outputUSDPerMillionTokens": "2",
                            },
                        },
                        "workload": {"concurrency": 1, "iterations": 1, "timeout_seconds": 30},
                        "reliability": {
                            "retry": {
                                "maxAttempts": 2,
                                "initialBackoffMs": 1,
                                "jitterRatio": 0,
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            blocked_config = load_run_config(path)
        blocked_adapter = OpenAICompatibleAdapter(
            "pinned-test",
            blocked_url,
            api_key="",
            request_fields={"max_tokens": 32},
            retry_sleep=_no_sleep,
        )
        blocked_result = asyncio.run(
            WorkloadRunner(blocked_config, blocked_adapter).execute(
                [TestCase(case_id="1", prompt="hello")]
            )
        )[0][0]
        self.assertFalse(blocked_result.success)
        self.assertEqual(len(blocked_result.attempts), 1)
        self.assertEqual(blocked_result.attempts[0].retry_decision, "ambiguous_blocked")
        self.assertEqual(blocked_calls["n"], 1)
        self.assertEqual(blocked_result.input_tokens, UNKNOWN_CALL_INPUT_TOKENS)
        self.assertEqual(blocked_result.output_tokens, 32)
        self.assertEqual(blocked_result.cost_usd, "0.008256000000")

    def test_http_retry_attempts_and_max_wait_come_from_target_config(self) -> None:
        calls = {"n": 0}
        slept: list[float] = []

        async def record_sleep(delay: float) -> None:
            slept.append(delay)

        def respond(
            headers: Any, body: dict[str, Any]
        ) -> tuple[int, dict[str, Any], dict[str, str]]:
            del headers, body
            calls["n"] += 1
            if calls["n"] < 2:
                return 429, {"error": {"code": 1305, "message": "overloaded"}}, {"Retry-After": "1000"}
            return 200, _completion("done", {"prompt_tokens": 4, "completion_tokens": 1}), {}

        base_url = _serve_with_headers(self, respond)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(
                json.dumps(
                    {
                        "target": {
                            "provider": "openai-compatible",
                            "model": "pinned-test",
                            "baseURL": base_url,
                            "httpRetryAttempts": 2,
                            "httpRetryMaxWaitSeconds": 30,
                        }
                    }
                ),
                encoding="utf-8",
            )
            config = load_run_config(path)
        self.assertEqual(config.target.http_retry_attempts, 2)
        self.assertEqual(config.target.http_retry_max_wait_seconds, 30)
        adapter = create_adapter(
            config.target.provider,
            model=config.target.model,
            base_url=config.target.base_url,
            http_retry_attempts=config.target.http_retry_attempts,
            http_retry_max_wait_seconds=config.target.http_retry_max_wait_seconds,
        )
        adapter._sleep = record_sleep
        response = asyncio.run(adapter.run(TestCase(case_id="1", prompt="hello")))
        self.assertEqual(response.output, "done")
        self.assertEqual(calls["n"], 2)
        self.assertEqual(slept, [30.0])
        self.assertNotIn("agentstorm.usage.unknown_calls", response.metadata)

        for overrides in (
            {"httpRetryAttempts": 0},
            {"httpRetryAttempts": 17},
            {"httpRetryAttempts": True},
            {"httpRetryMaxWaitSeconds": 0},
            {"httpRetryMaxWaitSeconds": 301},
        ):
            with self.subTest(overrides=overrides):
                with TemporaryDirectory() as directory:
                    path = Path(directory) / "run.json"
                    path.write_text(
                        json.dumps(
                            {
                                "target": {
                                    "provider": "openai-compatible",
                                    "model": "pinned-test",
                                    "baseURL": base_url,
                                    **overrides,
                                }
                            }
                        ),
                        encoding="utf-8",
                    )
                    with self.assertRaisesRegex(ValueError, "httpRetry"):
                        load_run_config(path)

    def test_length_with_empty_content_is_truncated_output(self) -> None:
        def respond(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            del headers, body
            return 200, {
                "choices": [
                    {
                        "finish_reason": "length",
                        "message": {"role": "assistant", "content": " \n"},
                    }
                ],
                "usage": {"prompt_tokens": 3, "completion_tokens": 512},
            }

        base_url = _serve(self, respond)
        adapter = OpenAICompatibleAdapter("pinned-test", base_url, api_key="")
        with self.assertRaises(ExecutionError) as raised:
            asyncio.run(adapter.run(TestCase(case_id="1", prompt="hello")))
        self.assertEqual(str(raised.exception), "provider/output_truncated")
        self.assertEqual(raised.exception.failure_kind, "provider")
        self.assertFalse(raised.exception.ambiguous)
        self.assertFalse(raised.exception.safe_to_retry)
        self.assertEqual(raised.exception.response.metadata[FINISH_REASON_KEY], "length")
        self.assertEqual((raised.exception.response.input_tokens, raised.exception.response.output_tokens), (3, 512))
        self.assertEqual(raised.exception.response.model_call_count, 1)

        def nonempty(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            del headers, body
            return 200, {
                "choices": [
                    {
                        "finish_reason": "length",
                        "message": {"role": "assistant", "content": "{\"partial\":true}"},
                    }
                ],
                "usage": {"prompt_tokens": 2, "completion_tokens": 4},
            }

        partial = OpenAICompatibleAdapter("pinned-test", _serve(self, nonempty), api_key="")
        response = asyncio.run(partial.run(TestCase(case_id="1", prompt="hello")))
        self.assertEqual(response.output, "{\"partial\":true}")
        self.assertEqual(response.metadata[FINISH_REASON_KEY], "length")

        def blank(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            del headers, body
            return 200, _completion("", {"prompt_tokens": 1, "completion_tokens": 0})

        empty = OpenAICompatibleAdapter("pinned-test", _serve(self, blank), api_key="")
        answer = asyncio.run(empty.run(TestCase(case_id="1", prompt="hello")))
        self.assertEqual(answer.output, "")

    def test_retry_after_delta_and_http_date(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(retry_after_seconds({"Retry-After": "1.5"}, now=now), 1.5)
        self.assertEqual(
            retry_after_seconds({"Retry-After": "Thu, 01 Jan 2026 00:00:02 GMT"}, now=now),
            2.0,
        )
        self.assertIsNone(retry_after_seconds({"Retry-After": "soon"}, now=now))
        self.assertEqual(retry_delay_seconds(1, 30, 0.5, 8), 8)
        self.assertEqual(retry_delay_seconds(2, None, 0.5, 8), 1.0)

    def test_case_retry_keeps_only_the_latest_tool_path(self) -> None:
        calls = {"n": 0}

        async def lookup() -> dict[str, str]:
            return {"ok": "yes"}

        def respond(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            del headers, body
            calls["n"] += 1
            if calls["n"] in {1, 3}:
                return 200, {
                    "choices": [
                        {
                            "message": {
                                "role": "assistant",
                                "content": None,
                                "tool_calls": [
                                    {
                                        "id": f"call-{calls['n']}",
                                        "type": "function",
                                        "function": {"name": "lookup", "arguments": {}},
                                    }
                                ],
                            }
                        }
                    ],
                    "usage": {"prompt_tokens": 2, "completion_tokens": 1},
                }
            if calls["n"] == 2:
                return 429, {"error": {"code": "1302", "message": "concurrency"}}
            return 200, _completion("ok", {"prompt_tokens": 3, "completion_tokens": 1})

        base_url = _serve(self, respond)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(
                json.dumps(
                    {
                        "target": {
                            "provider": "openai-compatible",
                            "model": "pinned-test",
                            "baseURL": base_url,
                        },
                        "workload": {"concurrency": 1, "iterations": 1, "timeout_seconds": 30},
                        "reliability": {
                            "retry": {"maxAttempts": 2, "initialBackoffMs": 1, "jitterRatio": 0}
                        },
                    }
                ),
                encoding="utf-8",
            )
            config = load_run_config(path)
        adapter = OpenAICompatibleAdapter(
            "pinned-test",
            base_url,
            api_key="",
            max_http_attempts=1,
            retry_sleep=_no_sleep,
            tools=[
                ChatTool("lookup", "Lookup.", {"type": "object", "properties": {}}, lookup)
            ],
        )
        result = asyncio.run(
            WorkloadRunner(config, adapter).execute([TestCase(case_id="1", prompt="hello")])
        )[0][0]
        self.assertTrue(result.success)
        self.assertEqual(result.tool_path, ["lookup"])
        self.assertEqual(result.attempts[0].provider_error_code, "1302")
        self.assertEqual(result.attempts[0].rate_limit_events, 1)
        self.assertEqual(result.provider_error_code, "1302")
        self.assertEqual(result.in_call_retry_count, 1)
        self.assertEqual(calls["n"], 4)

    def test_successful_in_call_429_keeps_the_last_provider_code_on_the_case(self) -> None:
        calls = {"n": 0}

        def respond(
            headers: Any, body: dict[str, Any]
        ) -> tuple[int, dict[str, Any], dict[str, str]]:
            del headers, body
            calls["n"] += 1
            if calls["n"] == 1:
                return 429, {"error": {"code": 1305, "message": "overloaded"}}, {}
            if calls["n"] == 2:
                return 429, {"error": {"code": "1302", "message": "concurrency"}}, {}
            return 200, _completion("done", {"prompt_tokens": 4, "completion_tokens": 1}), {}

        base_url = _serve_with_headers(self, respond)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(
                json.dumps(
                    {
                        "target": {
                            "provider": "openai-compatible",
                            "model": "pinned-test",
                            "baseURL": base_url,
                            "pricing": {
                                "inputUSDPerMillionTokens": "1",
                                "outputUSDPerMillionTokens": "2",
                            },
                        },
                        "workload": {"concurrency": 1, "iterations": 1, "timeout_seconds": 30},
                    }
                ),
                encoding="utf-8",
            )
            config = load_run_config(path)
        adapter = OpenAICompatibleAdapter(
            "pinned-test", base_url, api_key="", retry_sleep=_no_sleep
        )
        result = asyncio.run(
            WorkloadRunner(config, adapter).execute([TestCase(case_id="INC-001", prompt="hello")])
        )[0][0]
        self.assertTrue(result.success)
        self.assertEqual(len(result.attempts), 1)
        self.assertEqual(result.attempts[0].error_code, "")
        self.assertEqual(result.attempts[0].provider_error_code, "1302")
        self.assertEqual(result.attempts[0].rate_limit_events, 2)
        self.assertEqual(result.provider_error_code, "1302")
        self.assertEqual(result.in_call_retry_count, 2)
        self.assertEqual(calls["n"], 3)
        self.assertNotIn("overloaded", json.dumps(result.to_dict()))
        self.assertNotIn("concurrency", json.dumps(result.to_dict()))

    def test_malformed_completion_is_ambiguous_and_content_free(self) -> None:
        def respond(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            del headers, body
            return 200, {"secret": "do-not-leak"}

        base_url = _serve(self, respond)
        adapter = OpenAICompatibleAdapter("pinned-test", base_url, api_key="")
        with self.assertRaises(ExecutionError) as raised:
            asyncio.run(adapter.run(TestCase(case_id="1", prompt="hello")))
        self.assertEqual(raised.exception.code, "malformed_response")
        self.assertTrue(raised.exception.ambiguous)
        self.assertNotIn("do-not-leak", str(raised.exception))
        self.assertEqual(raised.exception.response.model_call_count, 1)

    def test_truncated_body_is_an_ambiguous_connection_error(self) -> None:
        for status in (200, 500):
            with self.subTest(status=status):
                base_url = _drop_after_headers(self, status)
                adapter = OpenAICompatibleAdapter(
                    "pinned-test",
                    base_url,
                    api_key="",
                    request_timeout_seconds=2,
                    request_fields={"max_tokens": 32},
                )
                with self.assertRaises(ExecutionError) as raised:
                    asyncio.run(adapter.run(TestCase(case_id="1", prompt="hello")))
                self.assertEqual(raised.exception.code, "connection_error")
                self.assertTrue(raised.exception.ambiguous)
                self.assertTrue(raised.exception.usage_complete)
                self.assertEqual(raised.exception.response.model_call_count, 0)
                self.assertEqual(raised.exception.response.input_tokens, UNKNOWN_CALL_INPUT_TOKENS)
                self.assertEqual(raised.exception.response.output_tokens, 32)
                self.assertEqual(
                    raised.exception.response.metadata["agentstorm.usage.unknown_calls"], 1
                )

    def test_tool_execution_error_closes_the_lifecycle_span(self) -> None:
        async def fail() -> dict[str, str]:
            raise ExecutionError("tool", "injected_fault", "tool")

        def respond(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            del headers, body
            return 200, {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": "call-1",
                                    "type": "function",
                                    "function": {"name": "fail", "arguments": {}},
                                }
                            ],
                        }
                    }
                ],
                "usage": {"prompt_tokens": 2, "completion_tokens": 1},
            }

        base_url = _serve(self, respond)
        lifecycle = RecordingLifecycle()
        adapter = OpenAICompatibleAdapter(
            "pinned-test",
            base_url,
            api_key="",
            tools=[
                ChatTool(
                    "fail",
                    "Fail on purpose.",
                    {"type": "object", "properties": {}},
                    fail,
                )
            ],
        )
        with self.assertRaises(ExecutionError) as raised:
            asyncio.run(adapter.run(TestCase(case_id="1", prompt="hello"), lifecycle=lifecycle))
        self.assertEqual(raised.exception.code, "injected_fault")
        self.assertEqual(lifecycle.started, ["fail"])
        self.assertEqual(lifecycle.finished, ["injected_fault"])

    def test_cancelled_call_is_reraised(self) -> None:
        started = threading.Event()
        release = threading.Event()

        def respond(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            del headers, body
            started.set()
            release.wait(timeout=5)
            return 200, _completion("late", {"prompt_tokens": 9, "completion_tokens": 9})

        base_url = _serve(self, respond)
        adapter = OpenAICompatibleAdapter(
            "pinned-test", base_url, api_key="", request_timeout_seconds=30
        )

        async def invoke() -> None:
            task = asyncio.create_task(adapter.run(TestCase(case_id="1", prompt="hello")))
            if not await asyncio.to_thread(started.wait, 2):
                raise AssertionError("server did not accept the request")
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        try:
            asyncio.run(invoke())
        finally:
            release.set()

    def test_stop_event_cancels_the_inflight_call(self) -> None:
        started = threading.Event()
        release = threading.Event()

        def respond(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            del headers, body
            started.set()
            release.wait(timeout=5)
            return 200, _completion("late", {"prompt_tokens": 9, "completion_tokens": 9})

        base_url = _serve(self, respond)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(
                json.dumps(
                    {
                        "target": {
                            "provider": "openai-compatible",
                            "model": "pinned-test",
                            "baseURL": base_url,
                        },
                        "workload": {"concurrency": 1, "iterations": 1, "timeout_seconds": 30},
                    }
                ),
                encoding="utf-8",
            )
            config = load_run_config(path)
        adapter = OpenAICompatibleAdapter("pinned-test", base_url, api_key="")
        stop = asyncio.Event()

        async def invoke() -> tuple[list[Any], Any]:
            task = asyncio.create_task(
                WorkloadRunner(config, adapter).execute(
                    [TestCase(case_id="1", prompt="hello")], stop_event=stop
                )
            )
            if not await asyncio.to_thread(started.wait, 2):
                raise AssertionError("server did not accept the request")
            stop.set()
            return await task

        try:
            results, summary = asyncio.run(invoke())
        finally:
            release.set()
        self.assertEqual(results, [])
        self.assertEqual(summary.total, 0)

    def test_http_5xx_keeps_earlier_tokens_and_prices_the_missing_call(self) -> None:
        state = {"calls": 0}

        async def lookup() -> dict[str, str]:
            return {"ok": "yes"}

        def respond(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            del headers, body
            state["calls"] += 1
            if state["calls"] == 1:
                return 200, {
                    "choices": [
                        {
                            "message": {
                                "role": "assistant",
                                "content": None,
                                "tool_calls": [
                                    {
                                        "id": "call-1",
                                        "type": "function",
                                        "function": {"name": "lookup", "arguments": {}},
                                    }
                                ],
                            }
                        }
                    ],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 4},
                }
            return 500, {"error": {"message": "busy"}}

        base_url = _serve(self, respond)
        adapter = OpenAICompatibleAdapter(
            "pinned-test",
            base_url,
            api_key="",
            request_fields={"max_tokens": 32},
            tools=[
                ChatTool(
                    "lookup",
                    "Lookup.",
                    {"type": "object", "properties": {}},
                    lookup,
                )
            ],
        )
        with self.assertRaises(ExecutionError) as raised:
            asyncio.run(adapter.run(TestCase(case_id="1", prompt="hello")))
        self.assertEqual(raised.exception.code, "http_5xx")
        self.assertTrue(raised.exception.ambiguous)
        self.assertTrue(raised.exception.usage_complete)
        self.assertEqual(raised.exception.response.model_call_count, 1)
        self.assertEqual(
            raised.exception.response.input_tokens, 10 + UNKNOWN_CALL_INPUT_TOKENS
        )
        self.assertEqual(raised.exception.response.output_tokens, 4 + 32)
        self.assertEqual(raised.exception.response.metadata["agentstorm.usage.unknown_calls"], 1)
        self.assertEqual(state["calls"], 2)

    def test_transient_5xx_still_prices_the_case(self) -> None:
        calls = {"n": 0}

        def respond(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            del headers, body
            calls["n"] += 1
            return 500, {"error": {"message": "busy"}}

        base_url = _serve(self, respond)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(
                json.dumps(
                    {
                        "target": {
                            "provider": "openai-compatible",
                            "model": "pinned-test",
                            "baseURL": base_url,
                            "pricing": {
                                "inputUSDPerMillionTokens": "1",
                                "outputUSDPerMillionTokens": "2",
                            },
                        },
                        "workload": {"concurrency": 1, "iterations": 1, "timeout_seconds": 30},
                    }
                ),
                encoding="utf-8",
            )
            config = load_run_config(path)
        adapter = OpenAICompatibleAdapter(
            "pinned-test", base_url, api_key="", request_fields={"max_tokens": 32}
        )
        result = asyncio.run(
            WorkloadRunner(config, adapter).execute([TestCase(case_id="1", prompt="hello")])
        )[0][0]
        self.assertFalse(result.success)
        self.assertEqual(result.error_code, "http_5xx")
        self.assertTrue(result.usage_complete)
        self.assertEqual(result.input_tokens, UNKNOWN_CALL_INPUT_TOKENS)
        self.assertEqual(result.output_tokens, 32)
        self.assertEqual(result.model_call_count, 0)
        self.assertIsNotNone(result.cost_usd)
        self.assertGreater(Decimal(result.cost_usd), 0)
        self.assertEqual(calls["n"], 1)

    def test_connection_error_does_not_invent_a_model_call(self) -> None:
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        adapter = OpenAICompatibleAdapter(
            "pinned-test",
            f"http://127.0.0.1:{port}/v1",
            api_key="",
            request_timeout_seconds=0.2,
            request_fields={"max_tokens": 32},
        )
        with self.assertRaises(ExecutionError) as raised:
            asyncio.run(adapter.run(TestCase(case_id="1", prompt="hello")))
        self.assertEqual(raised.exception.code, "connection_error")
        self.assertEqual(raised.exception.response.model_call_count, 0)
        self.assertTrue(raised.exception.usage_complete)
        self.assertEqual(raised.exception.response.input_tokens, UNKNOWN_CALL_INPUT_TOKENS)
        self.assertEqual(raised.exception.response.output_tokens, 32)
        self.assertEqual(
            raised.exception.response.metadata["agentstorm.usage.unknown_calls"], 1
        )

    def test_missing_max_tokens_charges_the_named_output_allowance(self) -> None:
        self.assertGreater(UNKNOWN_CALL_OUTPUT_TOKENS, 512)
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        adapter = OpenAICompatibleAdapter(
            "pinned-test",
            f"http://127.0.0.1:{port}/v1",
            api_key="",
            request_timeout_seconds=0.2,
        )
        with self.assertRaises(ExecutionError) as raised:
            asyncio.run(adapter.run(TestCase(case_id="1", prompt="hello")))
        self.assertEqual(raised.exception.code, "connection_error")
        self.assertEqual(raised.exception.response.model_call_count, 0)
        self.assertEqual(raised.exception.response.output_tokens, UNKNOWN_CALL_OUTPUT_TOKENS)
        self.assertEqual(
            raised.exception.response.metadata["agentstorm.usage.unknown_calls"], 1
        )

    def test_rejects_credential_and_method_urls(self) -> None:
        with self.assertRaisesRegex(ValueError, "credentials"):
            validate_base_url("https://user:secret@example.invalid/v1")
        with self.assertRaisesRegex(ValueError, "chat completions path"):
            validate_base_url("https://example.invalid/v1/chat/completions")
        with self.assertRaisesRegex(ValueError, "pinned model"):
            OpenAICompatibleAdapter("latest model", "https://example.invalid/v1")

    def test_builtin_provider_requires_a_pinned_endpoint(self) -> None:
        with self.assertRaisesRegex(ValueError, "pinned model"):
            create_adapter("openai-compatible")
        adapter = create_adapter(
            "openai-compatible", model="pinned-test", base_url="https://example.invalid/v1"
        )
        self.assertIsInstance(adapter, OpenAICompatibleAdapter)

    def test_sre_wrapper_unwraps_a_json_fence(self) -> None:
        self.assertEqual(_unwrap_json_fence('```json\n{"ok":true}\n```'), '{"ok":true}')
        self.assertEqual(_unwrap_json_fence('{"ok":true}'), '{"ok":true}')


async def _no_sleep(delay: float) -> None:
    del delay

    def test_sre_compatible_factory_rejects_other_providers(self) -> None:
        from agentstorm_worker.adapters import AdapterFactoryContext

        with self.assertRaisesRegex(ValueError, "openai-compatible"):
            create_sre(AdapterFactoryContext(provider="other", model="m", base_url="https://x"))

    def test_environment_key_is_not_required_for_an_open_server(self) -> None:
        previous = os.environ.pop("OPENAI_API_KEY", None)
        self.addCleanup(lambda: os.environ.__setitem__("OPENAI_API_KEY", previous) if previous else None)

        def respond(headers: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            del body
            self.assertNotIn("Authorization", headers)
            return 200, _completion("open", {"prompt_tokens": 1, "completion_tokens": 1})

        base_url = _serve(self, respond)
        adapter = create_adapter("openai-compatible", model="pinned-test", base_url=base_url)
        response = asyncio.run(adapter.run(TestCase(case_id="1", prompt="hello")))
        self.assertEqual(response.output, "open")


if __name__ == "__main__":
    unittest.main()
