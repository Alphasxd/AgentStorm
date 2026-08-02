from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass

from .config import CircuitBreakerConfig
from .execution_errors import ExecutionError


@dataclass(frozen=True)
class CircuitPermit:
    probe: bool = False
    events: tuple[str, ...] = ()


class CircuitBreaker:
    def __init__(
        self,
        config: CircuitBreakerConfig | None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._config = config
        self._clock = clock
        self._lock = asyncio.Lock()
        self._state = "closed"
        self._consecutive_failures = 0
        self._open_until = 0.0
        self._probe_in_flight = False

    async def acquire(self) -> CircuitPermit:
        if self._config is None:
            return CircuitPermit()
        async with self._lock:
            now = self._clock()
            if self._state == "closed":
                return CircuitPermit()
            if self._state == "open" and now >= self._open_until and not self._probe_in_flight:
                self._state = "half_open"
                self._probe_in_flight = True
                return CircuitPermit(probe=True, events=("half_open",))
            raise ExecutionError(
                "provider",
                "circuit_open",
                "provider",
                usage_complete=True,
                circuit_events=("reject",),
            )

    async def record_terminal(
        self,
        permit: CircuitPermit,
        *,
        provider_succeeded: bool,
        failure_category: str = "",
    ) -> tuple[str, ...]:
        if self._config is None:
            return ()
        async with self._lock:
            now = self._clock()
            events: list[str] = []
            if permit.probe:
                self._probe_in_flight = False
                if provider_succeeded:
                    self._state = "closed"
                    self._consecutive_failures = 0
                    events.append("close")
                elif failure_category == "provider":
                    self._state = "open"
                    self._open_until = now + self._config.open_duration_ms / 1000
                    events.append("open")
                else:
                    # A non-provider outcome does not count. Let the next case probe immediately.
                    self._state = "open"
                    self._open_until = now
                return tuple(events)

            if provider_succeeded:
                self._consecutive_failures = 0
                if self._state != "closed":
                    self._state = "closed"
                    self._probe_in_flight = False
                    events.append("close")
                return tuple(events)
            if failure_category != "provider":
                return ()
            if self._state == "closed":
                self._consecutive_failures += 1
                if self._consecutive_failures >= self._config.failure_threshold:
                    self._state = "open"
                    self._open_until = now + self._config.open_duration_ms / 1000
                    events.append("open")
            elif self._state == "open":
                self._open_until = max(
                    self._open_until, now + self._config.open_duration_ms / 1000
                )
            return tuple(events)
