"""Provider-neutral OpenAI Chat Completions client.

The adapter speaks the portable subset used by OpenAI-compatible domestic cloud
endpoints and self-hosted servers such as vLLM and SGLang: ``POST {baseURL}/chat/completions``
with bearer auth, function tools, and non-streaming JSON. It does not import a
vendor SDK. Retry, rate limits, faults, price snapshots, and traces stay in the
existing Worker middleware; this module only turns HTTP into ``AdapterResponse``
or ``ExecutionError``.
"""

from __future__ import annotations

import asyncio
import http.client
import json
import os
import urllib.error
import urllib.request
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from typing import Any
from urllib.parse import urlsplit

from ..execution_errors import ExecutionError
from ..models import AdapterResponse, TestCase
from .base import AgentLifecycle, ToolLifecycleEvent

_MAX_RESPONSE_BYTES = 2_000_000
_MAX_URL_LENGTH = 2048
_MAX_MODEL_LENGTH = 256
_RESERVED_REQUEST_FIELDS = {"model", "messages", "stream", "tools", "tool_choice"}
# Distinct from a proven model-call count of zero.
_LEDGER_COUNT = object()
# Allowance for one chat request that did not return usage. A 4-characters-per-token
# approximation of the largest scripted SRE request is under 1,000 tokens; later
# calls also resend prior assistant messages. 8192 covers that history with margin.
# The endpoint does not enforce this number.
UNKNOWN_CALL_INPUT_TOKENS = 8192
# Output charge for one failed call when the request does not set max_tokens.
# The generic adapter sends no max_tokens, so the provider may already have
# generated more than the 512-token SRE benchmark cap. 8192 matches the input
# allowance: a conservative estimate, not a meter reading and not a request
# cap. A positive integer max_tokens on the request is charged instead. This
# can still be below a model's maximum output.
UNKNOWN_CALL_OUTPUT_TOKENS = 8192


class _RejectRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        raise urllib.error.HTTPError(req.full_url, code, "redirect", headers, fp)


_OPENER = urllib.request.build_opener(_RejectRedirect)


@dataclass(frozen=True)
class ChatTool:
    """One local function exposed as an OpenAI-compatible tool."""

    name: str
    description: str
    parameters: dict[str, Any]
    handler: Callable[..., Awaitable[Any]]

    def schema(self) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }


def validate_base_url(base_url: str) -> None:
    """Reject endpoint URLs that could hide credentials or change the RPC path."""
    if (
        not isinstance(base_url, str)
        or not base_url
        or base_url != base_url.strip()
        or len(base_url) > _MAX_URL_LENGTH
        or any(character in base_url for character in "\r\n\t")
    ):
        raise ValueError(
            "target.baseURL must be an absolute http or https URL without credentials"
        )
    parsed = urlsplit(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError(
            "target.baseURL must be an absolute http or https URL without credentials"
        )
    if parsed.username or parsed.password:
        raise ValueError("target.baseURL must not contain credentials")
    if parsed.query or parsed.fragment:
        raise ValueError("target.baseURL must not contain a query or fragment")
    path = parsed.path.rstrip("/")
    if path.endswith("/chat/completions"):
        raise ValueError("target.baseURL must be the API root, not the chat completions path")


def validate_pinned_model(model: str) -> None:
    if (
        not isinstance(model, str)
        or not model
        or model != model.strip()
        or len(model.encode("utf-8")) > _MAX_MODEL_LENGTH
        or any(character.isspace() for character in model)
    ):
        raise ValueError("target.model must be a pinned model id without whitespace")


def chat_completions_url(base_url: str) -> str:
    return base_url.rstrip("/") + "/chat/completions"


@dataclass
class _Ledger:
    """Token and model-call totals proven by completed HTTP responses.

    ``model_calls`` counts responses the adapter finished. A timeout, ``5xx``,
    redirect, or connection failure is left out, even when the provider may
    already have accepted that request. ``unknown_calls`` is the marker for
    those omitted calls. ``response()`` copies it to
    ``metadata["agentstorm.usage.unknown_calls"]`` and does not add it to
    ``model_call_count``.
    """

    model_calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    usage_known: bool = True
    unknown_calls: int = 0

    def add_usage(self, usage: tuple[int, int] | None) -> None:
        if usage is None:
            self.usage_known = False
            return
        self.input_tokens += usage[0]
        self.output_tokens += usage[1]

    def charge_unknown_call(self, input_tokens: int, output_tokens: int) -> bool:
        """Price one call that returned no usage, and keep earlier measured tokens.

        Returns whether the ledger can still be priced. A prior response that
        omitted usage leaves the whole attempt unpriced.
        """
        self.unknown_calls += 1
        if not self.usage_known:
            return False
        self.input_tokens += input_tokens
        self.output_tokens += output_tokens
        return True

    def response(
        self, output: str = "", *, model_calls: int | None | object = _LEDGER_COUNT
    ) -> AdapterResponse:
        calls = self.model_calls if model_calls is _LEDGER_COUNT else model_calls
        metadata: dict[str, Any] = {}
        if self.unknown_calls:
            metadata["agentstorm.usage.unknown_calls"] = self.unknown_calls
        return AdapterResponse(
            output=output,
            input_tokens=self.input_tokens if self.usage_known else None,
            output_tokens=self.output_tokens if self.usage_known else None,
            model_call_count=calls,
            metadata=metadata,
        )


class OpenAICompatibleAdapter:
    """Chat Completions tool loop over one pinned model and base URL."""

    def __init__(
        self,
        model: str,
        base_url: str,
        *,
        instructions: str = "",
        tools: list[ChatTool] | None = None,
        api_key: str | None = None,
        max_model_calls: int = 8,
        request_timeout_seconds: float = 120,
        request_fields: dict[str, Any] | None = None,
        agent_name: str = "openai-compatible",
        unknown_input_tokens: int = UNKNOWN_CALL_INPUT_TOKENS,
    ) -> None:
        validate_pinned_model(model)
        validate_base_url(base_url)
        if max_model_calls < 1 or max_model_calls > 32:
            raise ValueError("max_model_calls must be between 1 and 32")
        if request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be positive")
        if isinstance(unknown_input_tokens, bool) or not isinstance(unknown_input_tokens, int):
            raise ValueError("unknown_input_tokens must be a positive integer")
        if unknown_input_tokens < 1:
            raise ValueError("unknown_input_tokens must be a positive integer")
        extra = dict(request_fields or {})
        reserved = sorted(set(extra) & _RESERVED_REQUEST_FIELDS)
        if reserved:
            raise ValueError(f"request field {reserved[0]} is reserved")
        if tools:
            names = [tool.name for tool in tools]
            if len(names) != len(set(names)) or any(not tool.name for tool in tools):
                raise ValueError("tool names must be unique and non-empty")
        self._model = model
        self._url = chat_completions_url(base_url)
        self._instructions = instructions
        self._tools = {tool.name: tool for tool in tools or []}
        self._tool_schemas = [tool.schema() for tool in tools or []]
        self._api_key = api_key
        self._max_model_calls = max_model_calls
        # urllib applies this to each socket operation, not the whole request.
        self._timeout = request_timeout_seconds
        self._extra = extra
        self._agent_name = agent_name
        self._unknown_input_tokens = unknown_input_tokens
        self._unknown_output_tokens = _output_allowance(extra)

    async def run(self, case: TestCase, lifecycle: AgentLifecycle | None = None) -> AdapterResponse:
        api_key = self._api_key if self._api_key is not None else os.environ.get("OPENAI_API_KEY", "")
        messages: list[dict[str, Any]] = []
        if self._instructions:
            messages.append({"role": "system", "content": self._instructions})
        messages.append({"role": "user", "content": case.prompt})
        ledger = _Ledger()
        tool_count = 0
        while ledger.model_calls < self._max_model_calls:
            payload: dict[str, Any] = {
                "model": self._model,
                "messages": messages,
                "stream": False,
                **self._extra,
            }
            if self._tool_schemas:
                payload["tools"] = self._tool_schemas
                payload["tool_choice"] = "auto"
            try:
                status, body = await asyncio.to_thread(self._post, payload, api_key.strip(), ledger)
            except asyncio.CancelledError:
                # Cancelling the task does not interrupt a socket operation that
                # is already blocked. urllib still applies its timeout to each
                # socket operation. Do not turn the cancel into a priced provider
                # error; the runner records it as cancelled.
                ledger.usage_known = False
                raise
            if status != 200:
                raise _status_error(
                    status, ledger, self._unknown_input_tokens, self._unknown_output_tokens
                )
            ledger.model_calls += 1
            try:
                message, usage = _parse_completion(body)
            except ValueError as exc:
                ledger.usage_known = False
                raise _error(
                    "malformed_response",
                    ambiguous=True,
                    ledger=ledger,
                    usage_complete=False,
                ) from exc
            ledger.add_usage(usage)
            tool_calls = message.get("tool_calls") or []
            if tool_calls:
                if ledger.model_calls >= self._max_model_calls:
                    break
                messages.append(message)
                tool_count += await self._execute_tools(tool_calls, messages, lifecycle, ledger)
                continue
            content = _message_text(message.get("content"))
            if content is None:
                raise _error(
                    "model_behavior",
                    ambiguous=True,
                    ledger=ledger,
                    usage_complete=ledger.usage_known,
                )
            return replace(ledger.response(content), tool_call_count=tool_count)
        raise _error(
            "max_turns_exceeded",
            category="evaluation",
            failure_kind="assertion",
            ledger=ledger,
            usage_complete=ledger.usage_known,
        )

    def _post(
        self, payload: dict[str, Any], api_key: str, ledger: _Ledger
    ) -> tuple[int, dict[str, Any] | None]:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "agentstorm-worker",
        }
        if api_key:
            headers["Authorization"] = "Bearer " + api_key
        request = urllib.request.Request(self._url, data=raw, headers=headers, method="POST")
        try:
            with _OPENER.open(request, timeout=self._timeout) as response:
                status = int(response.status)
                downloaded = response.read(_MAX_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as exc:
            status = int(exc.code)
            try:
                downloaded = exc.read(_MAX_RESPONSE_BYTES + 1)
            except (http.client.HTTPException, OSError) as read_error:
                raise self._unpriced("connection_error", ledger) from read_error
        except TimeoutError as exc:
            raise self._unpriced("timeout", ledger, failure_kind="timeout") from exc
        except urllib.error.URLError as exc:
            if isinstance(exc.reason, TimeoutError):
                raise self._unpriced("timeout", ledger, failure_kind="timeout") from exc
            raise self._unpriced("connection_error", ledger) from exc
        except (http.client.HTTPException, OSError) as exc:
            raise self._unpriced("connection_error", ledger) from exc
        if len(downloaded) > _MAX_RESPONSE_BYTES or not downloaded:
            return status, None
        try:
            parsed = json.loads(downloaded.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return status, None
        if not isinstance(parsed, dict):
            return status, None
        return status, parsed

    def _unpriced(
        self, code: str, ledger: _Ledger, *, failure_kind: str = "provider"
    ) -> ExecutionError:
        return _priced_unknown(
            ledger,
            code,
            self._unknown_input_tokens,
            self._unknown_output_tokens,
            failure_kind=failure_kind,
        )

    async def _execute_tools(
        self,
        tool_calls: list[Any],
        messages: list[dict[str, Any]],
        lifecycle: AgentLifecycle | None,
        ledger: _Ledger,
    ) -> int:
        executed = 0
        for call in tool_calls:
            name, arguments, call_id = _parse_tool_call(call)
            if name is None or arguments is None or call_id is None:
                raise _error(
                    "model_behavior",
                    ambiguous=True,
                    ledger=ledger,
                    usage_complete=ledger.usage_known,
                )
            tool = self._tools.get(name)
            if tool is None:
                raise _error(
                    "model_behavior",
                    ambiguous=True,
                    ledger=ledger,
                    usage_complete=ledger.usage_known,
                )
            event = ToolLifecycleEvent(
                invocation_id=call_id,
                name=name,
                call_id=call_id,
                agent_name=self._agent_name,
                arguments=arguments,
            )
            if lifecycle is not None:
                lifecycle.tool_started(event)
            try:
                result = await tool.handler(**arguments)
                content = json.dumps(
                    result, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
            except ExecutionError as exc:
                if lifecycle is not None:
                    lifecycle.tool_finished(replace(event, result=None), error_type=exc.code)
                raise
            except TypeError as exc:
                if lifecycle is not None:
                    lifecycle.tool_finished(
                        replace(event, result=None), error_type="model_behavior"
                    )
                raise _error(
                    "model_behavior",
                    ambiguous=True,
                    ledger=ledger,
                    usage_complete=ledger.usage_known,
                ) from exc
            except Exception as exc:
                if lifecycle is not None:
                    lifecycle.tool_finished(replace(event, result=None), error_type="tool_error")
                raise ExecutionError(
                    "tool",
                    "tool_error",
                    "tool",
                    response=ledger.response(),
                    usage_complete=ledger.usage_known,
                ) from exc
            if lifecycle is not None:
                lifecycle.tool_finished(replace(event, result=result))
            messages.append({"role": "tool", "tool_call_id": call_id, "content": content})
            executed += 1
        return executed


def _parse_completion(body: dict[str, Any] | None) -> tuple[dict[str, Any], tuple[int, int] | None]:
    if not isinstance(body, dict):
        raise ValueError("completion body is not an object")
    choices = body.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        raise ValueError("completion choices are missing")
    message = choices[0].get("message")
    if not isinstance(message, dict):
        raise ValueError("completion message is missing")
    assistant: dict[str, Any] = {"role": "assistant", "content": message.get("content")}
    tool_calls = message.get("tool_calls")
    if tool_calls is not None:
        if not isinstance(tool_calls, list):
            raise ValueError("tool_calls is not an array")
        assistant["tool_calls"] = tool_calls
    return assistant, _usage(body.get("usage"))


def _parse_tool_call(call: Any) -> tuple[str | None, dict[str, Any] | None, str | None]:
    if not isinstance(call, dict):
        return None, None, None
    function = call.get("function")
    if not isinstance(function, dict):
        return None, None, None
    name = function.get("name")
    call_id = call.get("id")
    if not isinstance(name, str) or not name or not isinstance(call_id, str) or not call_id:
        return None, None, None
    raw_arguments = function.get("arguments", "{}")
    if isinstance(raw_arguments, str):
        try:
            arguments = json.loads(raw_arguments) if raw_arguments else {}
        except json.JSONDecodeError:
            return None, None, None
    elif isinstance(raw_arguments, dict):
        arguments = raw_arguments
    else:
        return None, None, None
    if not isinstance(arguments, dict):
        return None, None, None
    return name, arguments, call_id


def _usage(value: Any) -> tuple[int, int] | None:
    if not isinstance(value, dict):
        return None
    input_tokens = _token_count(value.get("prompt_tokens", value.get("input_tokens")))
    output_tokens = _token_count(value.get("completion_tokens", value.get("output_tokens")))
    if input_tokens is None or output_tokens is None:
        return None
    return input_tokens, output_tokens


def _token_count(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value >= 0:
        return value
    if isinstance(value, float) and value.is_integer() and value >= 0 and value <= 2**53:
        return int(value)
    return None


def _message_text(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and isinstance(item.get("text"), str):
                parts.append(item["text"])
            else:
                return None
        return "".join(parts)
    return None


def _output_allowance(fields: dict[str, Any]) -> int:
    value = fields.get("max_tokens", UNKNOWN_CALL_OUTPUT_TOKENS)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        return UNKNOWN_CALL_OUTPUT_TOKENS
    return value


def _priced_unknown(
    ledger: _Ledger,
    code: str,
    input_allowance: int,
    output_allowance: int,
    *,
    failure_kind: str = "provider",
) -> ExecutionError:
    """Keep measured tokens and price the one call that returned no usage."""
    usage_complete = ledger.charge_unknown_call(input_allowance, output_allowance)
    return _error(
        code,
        failure_kind=failure_kind,
        ambiguous=True,
        ledger=ledger,
        usage_complete=usage_complete,
    )


def _status_error(
    status: int, ledger: _Ledger, input_allowance: int, output_allowance: int
) -> ExecutionError:
    if status == 429:
        return _error(
            "rate_limited",
            safe_to_retry=True,
            ledger=ledger,
            usage_complete=ledger.usage_known,
        )
    if status == 408:
        return _priced_unknown(
            ledger, "timeout", input_allowance, output_allowance, failure_kind="timeout"
        )
    if 300 <= status < 400:
        return _priced_unknown(ledger, "http_redirect", input_allowance, output_allowance)
    if 500 <= status <= 599:
        return _priced_unknown(ledger, "http_5xx", input_allowance, output_allowance)
    if 400 <= status <= 499:
        return _error("http_4xx", ledger=ledger, usage_complete=ledger.usage_known)
    return _priced_unknown(ledger, "unknown_error", input_allowance, output_allowance)


def _error(
    code: str,
    *,
    category: str = "provider",
    failure_kind: str = "provider",
    ambiguous: bool = False,
    safe_to_retry: bool = False,
    ledger: _Ledger | None = None,
    model_calls: int | None | object = _LEDGER_COUNT,
    usage_complete: bool = False,
) -> ExecutionError:
    response = ledger.response(model_calls=model_calls) if ledger is not None else None
    return ExecutionError(
        category,
        code,
        failure_kind,
        ambiguous=ambiguous,
        safe_to_retry=safe_to_retry,
        response=response,
        usage_complete=usage_complete,
    )
