"""In-process OpenAI-compatible server for the no-cost SRE protocol test.

The server is a scripted chat model, not a domestic or self-hosted inference
result. It requests the four SRE tools in a valid order and emits the fixture
diagnosis only after those tool results are present.
"""

from __future__ import annotations

import json
import re
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

_INCIDENT_ID = re.compile(r"INC-\d+")
_FIXTURES = (
    Path(__file__).resolve().parents[2]
    / "worker/src/agentstorm_worker/benchmarks/sre_fixtures.json"
)


def load_fixtures(path: Path | None = None) -> dict[str, dict[str, Any]]:
    raw = json.loads((path or _FIXTURES).read_text(encoding="utf-8"))
    return {item["incident_id"]: item for item in raw}


def diagnosis(incident: dict[str, Any]) -> dict[str, Any]:
    return {
        "incident_id": incident["incident_id"],
        "root_cause": incident["root_cause"],
        "severity": incident["severity"],
        "recommended_action": incident["recommended_action"],
        "evidence": [
            f"metrics:{incident['metrics']['summary']}",
            f"logs:{incident['error_signature']}",
        ],
        "runbook": incident["runbook"],
    }


def scripted_completion(
    body: dict[str, Any], fixtures: dict[str, dict[str, Any]]
) -> tuple[int, dict[str, Any]]:
    """Return the next chat completion for one scripted SRE conversation."""
    messages = body.get("messages")
    model = body.get("model")
    if not isinstance(model, str) or not model or not isinstance(messages, list):
        return 400, {"error": {"message": "invalid chat request"}}
    user_text = ""
    for message in messages:
        if isinstance(message, dict) and message.get("role") == "user":
            user_text = str(message.get("content") or "")
            break
    match = _INCIDENT_ID.search(user_text)
    if match is None or match.group(0) not in fixtures:
        return 400, {"error": {"message": "unknown incident"}}
    incident = fixtures[match.group(0)]
    completed = _completed_tools(messages)
    turn = sum(
        1
        for message in messages
        if isinstance(message, dict) and message.get("role") == "assistant"
    )
    if "get_incident" not in completed:
        return 200, _tool_response(
            model,
            turn,
            [("get_incident", {"incident_id": incident["incident_id"]})],
        )
    pending = [
        name
        for name in ("query_metrics", "search_logs")
        if name not in completed
    ]
    if pending:
        arguments = {
            "service": str(completed["get_incident"]["service"]),
            "window": str(completed["get_incident"]["window"]),
        }
        return 200, _tool_response(model, turn, [(name, arguments) for name in pending])
    if "lookup_runbook" not in completed:
        signature = str(completed["search_logs"]["error_signature"])
        return 200, _tool_response(
            model, turn, [("lookup_runbook", {"error_signature": signature})]
        )
    content = json.dumps(diagnosis(incident), sort_keys=True, separators=(",", ":"))
    return 200, _message_response(model, turn, content)


def _completed_tools(messages: list[Any]) -> dict[str, Any]:
    names: dict[str, str] = {}
    for message in messages:
        if not isinstance(message, dict) or message.get("role") != "assistant":
            continue
        for call in message.get("tool_calls") or []:
            if not isinstance(call, dict):
                continue
            function = call.get("function")
            call_id = call.get("id")
            if isinstance(function, dict) and isinstance(call_id, str):
                name = function.get("name")
                if isinstance(name, str):
                    names[call_id] = name
    completed: dict[str, Any] = {}
    for message in messages:
        if not isinstance(message, dict) or message.get("role") != "tool":
            continue
        name = names.get(str(message.get("tool_call_id") or ""))
        if name is None:
            continue
        try:
            completed[name] = json.loads(str(message.get("content") or ""))
        except json.JSONDecodeError:
            completed[name] = {}
    return completed


def _tool_response(
    model: str, turn: int, calls: list[tuple[str, dict[str, Any]]]
) -> dict[str, Any]:
    return {
        "id": f"chatcmpl-mock-{turn}",
        "object": "chat.completion",
        "model": model,
        "choices": [
            {
                "index": 0,
                "finish_reason": "tool_calls",
                "message": {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": f"call-{name}-{turn}",
                            "type": "function",
                            "function": {
                                "name": name,
                                "arguments": json.dumps(arguments, separators=(",", ":")),
                            },
                        }
                        for name, arguments in calls
                    ],
                },
            }
        ],
        "usage": {"prompt_tokens": 30 + turn, "completion_tokens": 12},
    }


def _message_response(model: str, turn: int, content: str) -> dict[str, Any]:
    return {
        "id": f"chatcmpl-mock-{turn}",
        "object": "chat.completion",
        "model": model,
        "choices": [
            {
                "index": 0,
                "finish_reason": "stop",
                "message": {"role": "assistant", "content": content},
            }
        ],
        "usage": {"prompt_tokens": 48 + turn, "completion_tokens": 40},
    }


class _MockServer(ThreadingHTTPServer):
    fixtures: dict[str, dict[str, Any]]
    reject_authorization: bool


class _Handler(BaseHTTPRequestHandler):
    server: _MockServer

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler name
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length else b""
        if self.path.split("?", 1)[0].rstrip("/") != "/v1/chat/completions":
            self._send(404, {"error": {"message": "not found"}})
            return
        if self.server.reject_authorization and self.headers.get("Authorization"):
            self._send(400, {"error": {"message": "unexpected authorization"}})
            return
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._send(400, {"error": {"message": "invalid json"}})
            return
        if not isinstance(body, dict):
            self._send(400, {"error": {"message": "invalid chat request"}})
            return
        status, payload = scripted_completion(body, self.server.fixtures)
        self._send(status, payload)

    def log_message(self, format: str, *args: Any) -> None:
        del format, args

    def _send(self, status: int, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


@contextmanager
def serve(
    fixtures: dict[str, dict[str, Any]] | None = None,
    *,
    reject_authorization: bool = True,
) -> Iterator[str]:
    """Serve the scripted model on loopback and yield its `/v1` base URL."""
    server = _MockServer(("127.0.0.1", 0), _Handler)
    server.fixtures = fixtures or load_fixtures()
    server.reject_authorization = reject_authorization
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address[:2]
        yield f"http://{host}:{port}/v1"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
