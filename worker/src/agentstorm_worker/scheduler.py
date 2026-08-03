from __future__ import annotations

import asyncio
import hashlib
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator

from .execution_errors import ExecutionError
from .results import PermitGrant, ResultClient, ResultSinkError, SchedulerBusy


class DistributedLimiter:
    """Coordinates provider attempts through Result API backed PostgreSQL leases."""

    def __init__(
        self,
        client: ResultClient,
        run_id: str,
        worker_id: str,
        provider: str,
    ) -> None:
        self._client = client
        self._run_id = run_id
        self._worker_id = worker_id
        self._provider = provider
        self.lease_lost = asyncio.Event()

    @asynccontextmanager
    async def permit(
        self,
        case_id: str,
        iteration: int,
        attempt: int,
        stop_event: asyncio.Event,
        deadline: float,
    ) -> AsyncIterator[None]:
        request_id = hashlib.sha256(
            f"{self._run_id}\0{self._worker_id}\0{case_id}\0{iteration}\0{attempt}".encode(
                "utf-8"
            )
        ).hexdigest()
        grant = await self._wait_for_grant(request_id, stop_event, deadline)
        if stop_event.is_set():
            raise asyncio.CancelledError
        owner = asyncio.current_task()
        renewal_failed = asyncio.Event()

        async def renew() -> None:
            delay = grant.renew_after_ms / 1000
            while True:
                await asyncio.sleep(delay)
                try:
                    delay = (
                        await asyncio.to_thread(
                            self._client.renew_permit, self._run_id, grant
                        )
                    ) / 1000
                except Exception:  # noqa: BLE001 - scheduler boundary
                    renewal_failed.set()
                    self.lease_lost.set()
                    stop_event.set()
                    if owner is not None:
                        owner.cancel()
                    return

        renew_task = asyncio.create_task(renew())
        try:
            try:
                yield
            except asyncio.CancelledError:
                if renewal_failed.is_set():
                    raise ExecutionError(
                        "harness", "distributed_permit_lost", "harness"
                    ) from None
                raise
            if renewal_failed.is_set():
                raise ExecutionError(
                    "harness", "distributed_permit_lost", "harness"
                )
        finally:
            renew_task.cancel()
            await asyncio.gather(renew_task, return_exceptions=True)
            try:
                await asyncio.to_thread(
                    self._client.release_permit, self._run_id, grant
                )
            except Exception:  # noqa: BLE001 - lease expiry is self-healing
                pass

    async def _wait_for_grant(
        self, request_id: str, stop_event: asyncio.Event, deadline: float
    ) -> PermitGrant:
        while not stop_event.is_set():
            if time.perf_counter() >= deadline:
                raise ExecutionError(
                    "provider", "distributed_limit_timeout", "provider"
                )
            try:
                return await asyncio.to_thread(
                    self._client.acquire_permit,
                    self._run_id,
                    request_id,
                    self._worker_id,
                    self._provider,
                )
            except SchedulerBusy as exc:
                remaining = max(0.0, deadline - time.perf_counter())
                delay = min(exc.retry_after_ms / 1000, remaining)
                if delay <= 0:
                    break
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=delay)
                except TimeoutError:
                    continue
            except ResultSinkError:
                raise ExecutionError(
                    "harness",
                    "distributed_scheduler_unavailable",
                    "harness",
                ) from None
        if stop_event.is_set():
            raise asyncio.CancelledError
        raise ExecutionError("provider", "distributed_limit_timeout", "provider")
