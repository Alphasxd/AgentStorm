"""Small, dependency-free client for replaying durable AgentStorm results."""

from __future__ import annotations

import json
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen


READ_TOKEN_ENV = "AGENTSTORM_RESULT_READ_TOKEN"


class ResultAPIError(RuntimeError):
    """A safe-to-display Result API replay error."""


def read_token_from_environment() -> str:
    token = os.environ.get(READ_TOKEN_ENV, "")
    if not token:
        raise ResultAPIError(f"{READ_TOKEN_ENV} is required")
    return token


def normalize_base_url(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ResultAPIError("Result API URL must be an absolute HTTP(S) URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ResultAPIError(
            "Result API URL must not contain credentials, a query, or a fragment"
        )
    path = parsed.path.rstrip("/")
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


class ResultAPIClient:
    def __init__(self, base_url: str, token: str, timeout_seconds: float = 15.0) -> None:
        self.base_url = normalize_base_url(base_url)
        if not token:
            raise ResultAPIError("Result API read token is required")
        self._token = token
        self._timeout_seconds = timeout_seconds

    def get_run(self, run_id: str) -> dict[str, Any]:
        return self._get(f"/v1/runs/{quote_run_id(run_id)}")

    def list_all_cases(self, run_id: str) -> list[dict[str, Any]]:
        cases: list[dict[str, Any]] = []
        cursor = ""
        seen_cursors: set[str] = set()
        while True:
            query: dict[str, str | int] = {"limit": 500}
            if cursor:
                query["cursor"] = cursor
            page = self._get(
                f"/v1/runs/{quote_run_id(run_id)}/cases?{urlencode(query)}"
            )
            raw_cases = page.get("cases")
            if not isinstance(raw_cases, list):
                raise ResultAPIError("Result API case page is missing a cases array")
            if not all(isinstance(item, dict) for item in raw_cases):
                raise ResultAPIError("Result API case page contains an invalid case")
            cases.extend(raw_cases)
            next_cursor = page.get("next_cursor")
            if next_cursor in {None, ""}:
                return cases
            if not isinstance(next_cursor, str):
                raise ResultAPIError("Result API returned an invalid pagination cursor")
            if next_cursor in seen_cursors:
                raise ResultAPIError("Result API repeated a pagination cursor")
            seen_cursors.add(next_cursor)
            cursor = next_cursor

    def completed_replay(self, run_id: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        run = self.get_run(run_id)
        if run.get("status") != "complete":
            raise ResultAPIError(f"run {run_id!r} is not complete")
        cases = self.list_all_cases(run_id)
        if not cases:
            raise ResultAPIError(f"run {run_id!r} has no durable cases")
        missing = [
            case_identity(item) for item in cases if not isinstance(item.get("output"), str)
        ]
        if missing:
            preview = ", ".join(missing[:3])
            raise ResultAPIError(
                "durable output is unavailable for run "
                f"{run_id!r}; enable sensitive durable results before execution "
                f"(missing: {preview})"
            )
        return run, cases

    def _get(self, path: str) -> dict[str, Any]:
        request = Request(
            f"{self.base_url}{path}",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self._token}",
            },
            method="GET",
        )
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                payload = json.load(response)
        except HTTPError as exc:
            raise ResultAPIError(
                f"Result API request failed with HTTP {exc.code}"
            ) from None
        except (URLError, TimeoutError, json.JSONDecodeError, OSError):
            raise ResultAPIError("Result API request failed") from None
        if not isinstance(payload, dict):
            raise ResultAPIError("Result API returned a non-object response")
        return payload


def quote_run_id(run_id: str) -> str:
    if not run_id:
        raise ResultAPIError("run ID is required")
    return quote(run_id, safe="")


def case_identity(case: dict[str, Any]) -> str:
    return f"{case.get('case_id', '<unknown>')}#{case.get('iteration', '<unknown>')}"
