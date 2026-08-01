from __future__ import annotations

import asyncio

from ..models import AdapterResponse, TestCase


class FakeAdapter:
    """Deterministic adapter used by tests and no-cost demos."""

    def __init__(self, delay_seconds: float = 0.001) -> None:
        self._delay_seconds = delay_seconds

    async def run(self, case: TestCase) -> AdapterResponse:
        await asyncio.sleep(self._delay_seconds)
        return AdapterResponse(output=f"fake response: {case.prompt}")
