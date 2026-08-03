from __future__ import annotations

import asyncio
import time
import unittest

from agentstorm_worker.execution_errors import ExecutionError
from agentstorm_worker.results import PermitGrant, ResultSinkError
from agentstorm_worker.scheduler import DistributedLimiter


class PermitClient:
    def __init__(self) -> None:
        self.request_ids: list[str] = []
        self.released = 0

    def acquire_permit(
        self, run_id: str, request_id: str, worker_id: str, provider: str
    ) -> PermitGrant:
        del run_id, worker_id, provider
        self.request_ids.append(request_id)
        return PermitGrant(
            permit_id="a" * 32,
            lease_token="permit-token",
            renew_after_ms=60000,
        )

    def renew_permit(self, run_id: str, grant: PermitGrant) -> int:
        del run_id, grant
        return 60000

    def release_permit(self, run_id: str, grant: PermitGrant) -> None:
        del run_id, grant
        self.released += 1


class UnavailablePermitClient(PermitClient):
    def acquire_permit(
        self, run_id: str, request_id: str, worker_id: str, provider: str
    ) -> PermitGrant:
        del run_id, request_id, worker_id, provider
        raise ResultSinkError("result API is unavailable")


class DistributedLimiterTest(unittest.IsolatedAsyncioTestCase):
    async def test_permit_identity_is_unique_per_worker_and_released(self) -> None:
        client = PermitClient()
        stop = asyncio.Event()
        deadline = time.perf_counter() + 5
        first = DistributedLimiter(client, "run-1", "worker-a", "fake")
        second = DistributedLimiter(client, "run-1", "worker-b", "fake")

        async with first.permit("case-1", 0, 1, stop, deadline):
            pass
        async with second.permit("case-1", 0, 1, stop, deadline):
            pass

        self.assertEqual(len(client.request_ids), 2)
        self.assertNotEqual(client.request_ids[0], client.request_ids[1])
        self.assertEqual(client.released, 2)

    async def test_scheduler_transport_failure_is_harness_not_provider(self) -> None:
        limiter = DistributedLimiter(
            UnavailablePermitClient(), "run-1", "worker-a", "fake"
        )
        with self.assertRaises(ExecutionError) as raised:
            async with limiter.permit(
                "case-1",
                0,
                1,
                asyncio.Event(),
                time.perf_counter() + 5,
            ):
                pass

        self.assertEqual(raised.exception.category, "harness")
        self.assertEqual(
            raised.exception.code, "distributed_scheduler_unavailable"
        )


if __name__ == "__main__":
    unittest.main()
