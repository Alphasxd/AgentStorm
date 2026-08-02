#!/usr/bin/env python3
"""No-model Result API stub used by the pinned Promptfoo CLI smoke test."""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit


RUN_ID = "replay-run"
TOKEN = "promptfoo-ci-read-token"


def result_case(
    case_id: str,
    output: str,
    assertion_type: str,
    index: int = 0,
) -> dict[str, Any]:
    return {
        "idempotency_key": f"{case_id}-0",
        "case_id": case_id,
        "iteration": 0,
        "success": True,
        "latency_ms": 8.25,
        "input_tokens": 3,
        "output_tokens": 5,
        "output": output,
        "tool_path": ["fake.echo"],
        "assertions": [
            {
                "index": index,
                "type": assertion_type,
                "passed": True,
                "reason_code": "passed",
            }
        ],
        "input_cost_usd": "0.000003000000",
        "output_cost_usd": "0.000010000000",
        "cost_usd": "0.000013000000",
    }


CASES = [
    result_case("exact-case", "alpha", "exact"),
    result_case("contains-case", "prefix needle suffix", "contains"),
    result_case("regex-case", "ticket-123", "regex"),
    result_case("schema-case", '{"ok":true}', "json_schema"),
    {
        **result_case(
            "agentstorm-only-case",
            "UNMAPPED_MODEL_OUTPUT_CANARY",
            "latency",
        ),
        "assertions": [
            {"index": 0, "type": "latency", "passed": True, "reason_code": "passed"},
            {"index": 1, "type": "tool_path", "passed": True, "reason_code": "passed"},
            {"index": 2, "type": "python", "passed": True, "reason_code": "passed"},
        ],
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port-file", required=True, type=Path)
    parser.add_argument("--request-log", required=True, type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            with args.request_log.open("a", encoding="utf-8") as request_log:
                request_log.write(self.path + "\n")
            if self.headers.get("Authorization") != f"Bearer {TOKEN}":
                self.send_json(401, {"code": "unauthorized"})
                return
            parsed = urlsplit(self.path)
            if parsed.path == f"/v1/runs/{RUN_ID}":
                self.send_json(200, {"id": RUN_ID, "status": "complete"})
                return
            if parsed.path == f"/v1/runs/{RUN_ID}/cases":
                cursor = parse_qs(parsed.query).get("cursor", [""])[0]
                if cursor == "":
                    self.send_json(
                        200, {"cases": CASES[:2], "next_cursor": "second-page"}
                    )
                    return
                if cursor == "second-page":
                    self.send_json(200, {"cases": CASES[2:]})
                    return
                self.send_json(400, {"code": "invalid_cursor"})
                return
            self.send_json(404, {"code": "not_found"})

        def send_json(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    args.port_file.write_text(str(server.server_port), encoding="utf-8")
    server.serve_forever()


if __name__ == "__main__":
    main()
