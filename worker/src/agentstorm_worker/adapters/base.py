from __future__ import annotations

from typing import Protocol

from ..models import AdapterResponse, TestCase


class AgentAdapter(Protocol):
    async def run(self, case: TestCase) -> AdapterResponse:
        """Execute one test case."""
