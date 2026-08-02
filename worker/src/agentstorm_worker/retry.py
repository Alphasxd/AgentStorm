from __future__ import annotations

import hashlib

from .config import RetryConfig
from .execution_errors import ExecutionError


def retry_backoff_ms(
    config: RetryConfig,
    seed: int | None,
    case_id: str,
    iteration: int,
    failed_attempt: int,
) -> int:
    exponential = config.initial_backoff_ms * (2 ** (failed_attempt - 1))
    capped = min(exponential, config.max_backoff_ms)
    if config.jitter_ratio == 0:
        return capped
    material = "\x00".join(
        (str(seed or 0), case_id, str(iteration), str(failed_attempt), "retry-jitter")
    ).encode("utf-8")
    draw = int.from_bytes(hashlib.sha256(material).digest()[:8], "big") / 2**64
    factor = 1 - config.jitter_ratio + (2 * config.jitter_ratio * draw)
    return min(config.max_backoff_ms, max(0, round(capped * factor)))


def retry_eligibility(error: ExecutionError, config: RetryConfig) -> str:
    if error.category != "provider" or error.code == "circuit_open":
        return "not_retryable"
    if error.safe_to_retry:
        return "retry_safe"
    if error.ambiguous:
        return "retry_ambiguous" if config.allow_ambiguous_retries else "ambiguous_blocked"
    return "not_retryable"
