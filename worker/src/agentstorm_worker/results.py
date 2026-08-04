from __future__ import annotations

import hashlib
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


class SchedulerBusy(ResultSinkError):
    def __init__(self, retry_after_ms: int) -> None:
        super().__init__("distributed scheduler capacity is unavailable")
        self.retry_after_ms = max(1, retry_after_ms)


@dataclass(frozen=True)
class QueueClaim:
    shard_index: int
    lease_token: str
    renew_after_ms: int


@dataclass(frozen=True)
class PermitGrant:
    permit_id: str
    lease_token: str
    renew_after_ms: int


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
                **(
                    {"adapter_entrypoint": config.target.adapter_entrypoint}
                    if config.target.adapter_entrypoint
                    else {}
                ),
                **(
                    {
                        "pricing": {
                            "input_usd_per_million_tokens": (
                                config.target.pricing.input_usd_per_million_tokens
                            ),
                            "output_usd_per_million_tokens": (
                                config.target.pricing.output_usd_per_million_tokens
                            ),
                        }
                    }
                    if config.target.pricing is not None
                    else {}
                ),
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
            **(
                {"reliability": _reliability_payload(config)}
                if config.reliability is not None
                else {}
            ),
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
        lease_token: str | None = None,
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
                "usage_complete": summary.usage_complete,
                "model_call_count": summary.model_call_count,
                "tool_call_count": summary.tool_call_count,
            },
            "cases": cases,
        }
        headers = {"X-AgentStorm-Queue-Lease": lease_token} if lease_token else None
        self._put(
            f"/v1/runs/{urllib.parse.quote(run_id, safe='')}/shards/{shard_index}",
            f"run/{run_id}/shard/{shard_index}",
            payload,
            extra_headers=headers,
        )

    def claim_shard(self, run_id: str, worker_id: str) -> QueueClaim | None:
        payload = self._post(
            f"/v1/runs/{urllib.parse.quote(run_id, safe='')}/queue/claims",
            {"worker_id": worker_id},
        )
        if payload is None:
            return None
        return QueueClaim(
            shard_index=int(payload["shard_index"]),
            lease_token=str(payload["lease_token"]),
            renew_after_ms=int(payload["renew_after_ms"]),
        )

    def renew_shard(self, run_id: str, shard_index: int, lease_token: str) -> int:
        payload = self._post(
            f"/v1/runs/{urllib.parse.quote(run_id, safe='')}/queue/shards/{shard_index}/renew",
            {"lease_token": lease_token},
        )
        if payload is None:
            raise ResultSinkError("result API returned an empty lease response")
        return int(payload["renew_after_ms"])

    def acquire_permit(
        self, run_id: str, request_id: str, worker_id: str, provider: str
    ) -> PermitGrant:
        if len(request_id) != 64:
            request_id = hashlib.sha256(request_id.encode("utf-8")).hexdigest()
        payload = self._post(
            f"/v1/runs/{urllib.parse.quote(run_id, safe='')}/permits",
            {"request_id": request_id, "worker_id": worker_id, "provider": provider},
        )
        if payload is None:
            raise ResultSinkError("result API returned an empty permit response")
        return PermitGrant(
            permit_id=str(payload["permit_id"]),
            lease_token=str(payload["lease_token"]),
            renew_after_ms=int(payload["renew_after_ms"]),
        )

    def renew_permit(self, run_id: str, grant: PermitGrant) -> int:
        payload = self._post(
            f"/v1/runs/{urllib.parse.quote(run_id, safe='')}/permits/{grant.permit_id}/renew",
            {"lease_token": grant.lease_token},
        )
        if payload is None:
            raise ResultSinkError("result API returned an empty permit lease response")
        return int(payload["renew_after_ms"])

    def release_permit(self, run_id: str, grant: PermitGrant) -> None:
        self._post(
            f"/v1/runs/{urllib.parse.quote(run_id, safe='')}/permits/{grant.permit_id}/release",
            {"lease_token": grant.lease_token},
        )

    def _case_payload(self, run_id: str, result: CaseResult) -> dict[str, Any]:
        case_key = urllib.parse.quote_plus(result.case_id, safe="")
        payload: dict[str, Any] = {
            "idempotency_key": (f"run/{run_id}/case/{case_key}/iteration/{result.iteration}"),
            "case_id": result.case_id,
            "iteration": result.iteration,
            "success": result.success,
            "latency_ms": result.latency_ms,
            "input_tokens": result.input_tokens or 0,
            "output_tokens": result.output_tokens or 0,
            "usage_complete": result.usage_complete,
            "model_call_count": result.model_call_count,
            "tool_call_count": result.tool_call_count,
        }
        if result.failure_kind:
            payload["failure_kind"] = result.failure_kind
        if result.failure_category:
            payload["failure_category"] = result.failure_category
        if result.error_code:
            payload["error_code"] = result.error_code
        if result.attempts:
            payload["attempts"] = [
                {
                    "number": attempt.number,
                    "latency_ms": attempt.latency_ms,
                    "outcome": attempt.outcome,
                    "ambiguous": attempt.ambiguous,
                    "retry_decision": attempt.retry_decision,
                    "backoff_ms": attempt.backoff_ms,
                    "input_tokens": attempt.input_tokens or 0,
                    "output_tokens": attempt.output_tokens or 0,
                    "usage_complete": attempt.usage_complete,
                    "model_call_count": attempt.model_call_count,
                    "tool_call_count": attempt.tool_call_count,
                    **(
                        {"failure_category": attempt.failure_category}
                        if attempt.failure_category
                        else {}
                    ),
                    **({"error_code": attempt.error_code} if attempt.error_code else {}),
                    **({"injected_rule": attempt.injected_rule} if attempt.injected_rule else {}),
                    **(
                        {"injected_fault": attempt.injected_fault} if attempt.injected_fault else {}
                    ),
                    **(
                        {"circuit_events": attempt.circuit_events} if attempt.circuit_events else {}
                    ),
                }
                for attempt in result.attempts
            ]
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

    def mark_terminal(self, run_id: str, status: str, reason_code: str) -> None:
        if status not in {"cancelled", "harness_failed"}:
            raise ValueError("terminal status must be cancelled or harness_failed")
        self._put(
            f"/v1/runs/{urllib.parse.quote(run_id, safe='')}/terminal",
            f"run/{run_id}/terminal/{status}",
            {"status": status, "reason_code": reason_code},
        )

    def _put(
        self,
        path: str,
        idempotency_key: str,
        payload: dict[str, Any],
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        headers = {
            "Authorization": f"Bearer {self._config.write_token}",
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_key,
        }
        if extra_headers:
            headers.update(extra_headers)
        request = urllib.request.Request(
            self._config.base_url + path,
            data=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
            method="PUT",
            headers=headers,
        )
        try:
            with self._opener(request, timeout=self._config.timeout_seconds) as response:
                response.read()
        except urllib.error.HTTPError as exc:
            raise ResultSinkError(f"result API returned HTTP {exc.code}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise ResultSinkError("result API is unavailable") from exc

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        request = urllib.request.Request(
            self._config.base_url + path,
            data=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {self._config.write_token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with self._opener(request, timeout=self._config.timeout_seconds) as response:
                status = getattr(response, "status", 200)
                body = response.read()
                if status == 204:
                    return None
                decoded = json.loads(body or b"{}")
                if not isinstance(decoded, dict):
                    raise ResultSinkError("result API returned an invalid JSON response")
                return decoded
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                try:
                    decoded = json.loads(exc.read() or b"{}")
                    retry_after_ms = int(decoded.get("retry_after_ms", 1000))
                except (ValueError, TypeError, json.JSONDecodeError):
                    retry_after_ms = 1000
                raise SchedulerBusy(retry_after_ms) from exc
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


def _reliability_payload(config: RunConfig) -> dict[str, Any]:
    reliability = config.reliability
    assert reliability is not None
    retry = reliability.retry
    payload: dict[str, Any] = {
        "retry": {
            "max_attempts": retry.max_attempts,
            "initial_backoff_ms": retry.initial_backoff_ms,
            "max_backoff_ms": retry.max_backoff_ms,
            "max_cumulative_backoff_ms": retry.max_cumulative_backoff_ms,
            "jitter_ratio": retry.jitter_ratio,
            "allow_ambiguous_retries": retry.allow_ambiguous_retries,
        }
    }
    if reliability.seed is not None:
        payload["seed"] = reliability.seed
    if reliability.circuit_breaker is not None:
        payload["circuit_breaker"] = {
            "failure_threshold": reliability.circuit_breaker.failure_threshold,
            "open_duration_ms": reliability.circuit_breaker.open_duration_ms,
        }
    if reliability.scenario is not None:
        scenario = reliability.scenario
        rules: list[dict[str, Any]] = []
        for rule in scenario.rules:
            rule_payload: dict[str, Any] = {
                "name": rule.name,
                "fault": rule.fault,
                "probability": rule.probability,
                "attempts": list(rule.attempts),
            }
            if rule.case_ids:
                rule_payload["caseIDs"] = list(rule.case_ids)
            if rule.iterations:
                rule_payload["iterations"] = list(rule.iterations)
            if rule.delay_ms is not None:
                rule_payload["delayMs"] = rule.delay_ms
            if rule.status_code is not None:
                rule_payload["statusCode"] = rule.status_code
            if rule.tool_name:
                rule_payload["toolName"] = rule.tool_name
            rules.append(rule_payload)
        payload["scenario"] = {
            "source": {"name": scenario.source_name, "key": scenario.source_key},
            "digest": scenario.digest,
            "document": {
                "apiVersion": "agentstorm.io/v1alpha1",
                "kind": "FaultScenario",
                "rules": rules,
            },
        }
    return payload
