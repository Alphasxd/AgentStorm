from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable

from .config import RunConfig
from .models import CaseResult, RunSummary


class ResultSinkError(RuntimeError):
    """A sanitized failure while communicating with the Result API."""


@dataclass(frozen=True)
class ResultSinkConfig:
    base_url: str
    write_token: str
    include_sensitive: bool = False
    timeout_seconds: float = 30.0


class ResultClient:
    def __init__(
        self,
        config: ResultSinkConfig,
        opener: Callable[..., Any] = urllib.request.urlopen,
    ) -> None:
        parsed = urllib.parse.urlsplit(config.base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("AGENTSTORM_RESULT_API_URL must be an HTTP(S) URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError(
                "AGENTSTORM_RESULT_API_URL must not contain credentials, query, or fragment"
            )
        if not config.write_token.strip():
            raise ValueError(
                "AGENTSTORM_RESULT_WRITE_TOKEN is required when the result sink is enabled"
            )
        if config.timeout_seconds <= 0:
            raise ValueError("AGENTSTORM_RESULT_TIMEOUT_SECONDS must be positive")
        self._config = ResultSinkConfig(
            base_url=config.base_url.rstrip("/"),
            write_token=config.write_token,
            include_sensitive=config.include_sensitive,
            timeout_seconds=config.timeout_seconds,
        )
        self._opener = opener

    @classmethod
    def from_environment(cls) -> ResultClient | None:
        base_url = os.getenv("AGENTSTORM_RESULT_API_URL", "").strip()
        if not base_url:
            return None
        return cls(
            ResultSinkConfig(
                base_url=base_url,
                write_token=os.getenv("AGENTSTORM_RESULT_WRITE_TOKEN", ""),
                include_sensitive=_environment_boolean(
                    "AGENTSTORM_INCLUDE_SENSITIVE_RESULTS", default=False
                ),
                timeout_seconds=float(os.getenv("AGENTSTORM_RESULT_TIMEOUT_SECONDS", "30")),
            )
        )

    def register_run(self, config: RunConfig, expected_shards: int) -> None:
        payload = {
            "schema_version": "v1alpha1",
            "expected_shards": expected_shards,
            "source": {
                "namespace": config.source.namespace,
                "name": config.source.name,
            },
            "target": {
                "provider": config.target.provider,
                "model": config.target.model,
            },
            "dataset": {
                "name": config.dataset.name,
                "key": config.dataset.key,
            },
            "evaluation": {
                "min_success_rate": config.evaluation.min_success_rate,
                "max_failure_rate": config.evaluation.max_error_rate,
                "max_p95_ms": config.evaluation.max_p95_latency_ms,
            },
        }
        self._put(
            f"/v1/runs/{urllib.parse.quote(config.run_id, safe='')}",
            f"run/{config.run_id}",
            payload,
        )

    def upload_shard(
        self,
        run_id: str,
        shard_index: int,
        results: list[CaseResult],
        summary: RunSummary,
    ) -> None:
        cases = [self._case_payload(run_id, result) for result in results]
        payload = {
            "schema_version": "v1alpha1",
            "summary": {
                "total": summary.total,
                "succeeded": summary.succeeded,
                "failed": summary.failed,
                "duration_ms": summary.duration_ms,
                "input_tokens": summary.input_tokens,
                "output_tokens": summary.output_tokens,
            },
            "cases": cases,
        }
        self._put(
            f"/v1/runs/{urllib.parse.quote(run_id, safe='')}/shards/{shard_index}",
            f"run/{run_id}/shard/{shard_index}",
            payload,
        )

    def _case_payload(self, run_id: str, result: CaseResult) -> dict[str, Any]:
        case_key = urllib.parse.quote_plus(result.case_id, safe="")
        payload: dict[str, Any] = {
            "idempotency_key": (
                f"run/{run_id}/case/{case_key}/iteration/{result.iteration}"
            ),
            "case_id": result.case_id,
            "iteration": result.iteration,
            "success": result.success,
            "latency_ms": result.latency_ms,
            "input_tokens": result.input_tokens or 0,
            "output_tokens": result.output_tokens or 0,
        }
        if result.failure_kind:
            payload["failure_kind"] = result.failure_kind
        if result.tool_path:
            payload["tool_path"] = result.tool_path
        if result.assertions:
            payload["assertions"] = [
                {
                    "index": outcome.index,
                    "type": outcome.type,
                    "passed": outcome.passed,
                    "reason_code": outcome.reason_code,
                    **(
                        {"message": outcome.message}
                        if self._config.include_sensitive and outcome.message is not None
                        else {}
                    ),
                }
                for outcome in result.assertions
            ]
        if self._config.include_sensitive:
            if result.output:
                payload["output"] = result.output
            if result.error:
                payload["error"] = result.error
        return payload

    def _put(self, path: str, idempotency_key: str, payload: dict[str, Any]) -> None:
        request = urllib.request.Request(
            self._config.base_url + path,
            data=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
            method="PUT",
            headers={
                "Authorization": f"Bearer {self._config.write_token}",
                "Content-Type": "application/json",
                "Idempotency-Key": idempotency_key,
            },
        )
        try:
            with self._opener(request, timeout=self._config.timeout_seconds) as response:
                response.read()
        except urllib.error.HTTPError as exc:
            raise ResultSinkError(f"result API returned HTTP {exc.code}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise ResultSinkError("result API is unavailable") from exc


def _environment_boolean(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError(f"{name} must be true or false")
