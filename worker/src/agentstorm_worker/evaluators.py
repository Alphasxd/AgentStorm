from __future__ import annotations

import asyncio
import importlib
import json
import re
from dataclasses import dataclass
from typing import Any, Callable

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError

from .models import AssertionOutcome, AssertionSpec, TestCase


@dataclass(frozen=True)
class EvaluationContext:
    case_id: str
    prompt: str
    output: str
    latency_ms: float
    tool_path: list[str]
    metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "prompt": self.prompt,
            "output": self.output,
            "latency_ms": self.latency_ms,
            "tool_path": list(self.tool_path),
            "metadata": self.metadata,
        }


PythonAssertion = Callable[[dict[str, Any], dict[str, Any]], object]


class EvaluatorRegistry:
    def __init__(self, cases: list[TestCase]) -> None:
        self._regexes: dict[tuple[str, int], re.Pattern[str]] = {}
        self._schemas: dict[tuple[str, int], Draft202012Validator] = {}
        self._python: dict[tuple[str, int], PythonAssertion] = {}
        for case in cases:
            for index, assertion in enumerate(case.effective_assertions()):
                self._prepare(case.case_id, index, assertion)

    def _prepare(self, case_id: str, index: int, assertion: AssertionSpec) -> None:
        key = (case_id, index)
        if assertion.type == "regex":
            assert assertion.pattern is not None
            self._regexes[key] = re.compile(assertion.pattern)
        elif assertion.type == "json_schema":
            assert assertion.schema is not None
            Draft202012Validator.check_schema(assertion.schema)
            self._schemas[key] = Draft202012Validator(assertion.schema)
        elif assertion.type == "python":
            assert assertion.entrypoint is not None
            module_name, function_name = assertion.entrypoint.split(":", 1)
            function = getattr(importlib.import_module(module_name), function_name, None)
            if not callable(function):
                raise ValueError(
                    f"python assertion entrypoint {assertion.entrypoint!r} is not callable"
                )
            self._python[key] = function

    async def evaluate(
        self, case: TestCase, context: EvaluationContext
    ) -> list[AssertionOutcome]:
        outcomes: list[AssertionOutcome] = []
        for index, assertion in enumerate(case.effective_assertions()):
            outcomes.append(await self._evaluate_one(case.case_id, index, assertion, context))
        return outcomes

    async def _evaluate_one(
        self,
        case_id: str,
        index: int,
        assertion: AssertionSpec,
        context: EvaluationContext,
    ) -> AssertionOutcome:
        try:
            passed, reason_code, message = await self._invoke(
                (case_id, index), assertion, context
            )
        except Exception as exc:  # noqa: BLE001 - assertion failures are test results
            return AssertionOutcome(
                index=index,
                type=assertion.type,
                passed=False,
                reason_code="exception",
                message=f"{type(exc).__name__}: {exc}",
            )
        return AssertionOutcome(
            index=index,
            type=assertion.type,
            passed=passed,
            reason_code=reason_code,
            message=message,
        )

    async def _invoke(
        self,
        key: tuple[str, int],
        assertion: AssertionSpec,
        context: EvaluationContext,
    ) -> tuple[bool, str, str | None]:
        if assertion.type == "exact":
            passed = context.output == assertion.value
            return passed, "passed" if passed else "mismatch", None
        if assertion.type == "contains":
            assert assertion.value is not None
            passed = assertion.value in context.output
            return passed, "passed" if passed else "mismatch", None
        if assertion.type == "regex":
            passed = self._regexes[key].search(context.output) is not None
            return passed, "passed" if passed else "mismatch", None
        if assertion.type == "json_schema":
            try:
                value = json.loads(context.output)
            except json.JSONDecodeError as exc:
                return False, "invalid_json", str(exc)
            try:
                self._schemas[key].validate(value)
            except ValidationError as exc:
                return False, "schema_violation", exc.message
            return True, "passed", None
        if assertion.type == "tool_path":
            passed = context.tool_path == list(assertion.path)
            return passed, "passed" if passed else "path_mismatch", None
        if assertion.type == "latency":
            assert assertion.max_ms is not None
            passed = context.latency_ms <= assertion.max_ms
            return passed, "passed" if passed else "latency_exceeded", None
        if assertion.type == "python":
            function = self._python[key]
            raw = await asyncio.to_thread(function, context.to_dict(), assertion.config)
            if isinstance(raw, bool):
                return raw, "passed" if raw else "mismatch", None
            if isinstance(raw, dict) and isinstance(raw.get("passed"), bool):
                message = raw.get("message")
                if message is not None and not isinstance(message, str):
                    raise TypeError("python assertion message must be a string")
                passed = bool(raw["passed"])
                return passed, "passed" if passed else "mismatch", message
            raise TypeError("python assertion must return bool or a passed/message object")
        raise ValueError(f"unsupported assertion type {assertion.type!r}")


__all__ = ["EvaluationContext", "EvaluatorRegistry", "SchemaError"]
