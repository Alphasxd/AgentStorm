"""Provider-neutral OpenAI Chat Completions client.

The adapter speaks the portable subset used by OpenAI-compatible hosted APIs
and self-hosted servers such as vLLM and SGLang: ``POST {baseURL}/chat/completions``
with bearer auth, function tools, and non-streaming JSON. It does not import a
vendor SDK. Case retry, faults, price snapshots, and traces stay in the Worker
middleware. This module retries one chat call on a retryable HTTP status so a
429 in the middle of a tool loop does not discard completed turns, and it turns
HTTP into ``AdapterResponse`` or ``ExecutionError``.
"""

from __future__ import annotations

import asyncio
import http.client
import json
import math
import os
import re
import urllib.error
import urllib.request
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlsplit

from ..execution_errors import ExecutionError
from ..models import AdapterResponse, TestCase
from .base import AgentLifecycle, ToolLifecycleEvent

_MAX_RESPONSE_BYTES = 2_000_000
_MAX_URL_LENGTH = 2048
_MAX_MODEL_LENGTH = 256
_RESERVED_REQUEST_FIELDS = {"model", "messages", "stream", "tools", "tool_choice"}
# Sampling keys that target config sets explicitly, so requestFields cannot
# disagree with the recorded max_tokens or the SRE temperature pin.
_CONFIG_RESERVED_REQUEST_FIELDS = _RESERVED_REQUEST_FIELDS | {"temperature", "max_tokens"}
_REQUEST_FIELD_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_PROVIDER_CODE = re.compile(r"^[A-Za-z0-9_.-]{1,32}$")
_MAX_REQUEST_FIELDS = 16
_MAX_REQUEST_FIELD_BYTES = 1024
_MAX_REQUEST_FIELD_DEPTH = 6
_MAX_OUTPUT_TOKENS = 131072
# 429 is rate limiting. 502, 503, and 504 are the retryable gateway statuses.
# Other 5xx responses stay single-shot so a deterministic fault is not retried.
RETRYABLE_HTTP_STATUSES = frozenset({429, 502, 503, 504})
DEFAULT_HTTP_RETRY_ATTEMPTS = 3
DEFAULT_HTTP_RETRY_INITIAL_SECONDS = 0.5
DEFAULT_HTTP_RETRY_MAX_SECONDS = 8.0
PROVIDER_ERROR_CODE_KEY = "agentstorm.provider.error_code"
FINISH_REASON_KEY = "agentstorm.provider.finish_reason"
RATE_LIMIT_EVENTS_KEY = "agentstorm.provider.rate_limit_events"
RATE_LIMIT_CODES_KEY = "agentstorm.provider.rate_limit_codes"
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


def validate_max_output_tokens(value: Any) -> int:
    """Return a completion cap the spend estimate and the request can share."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("target.maxTokens must be an integer")
    if not 1 <= value <= _MAX_OUTPUT_TOKENS:
        raise ValueError("target.maxTokens must be between 1 and 131072")
    return value


def validate_request_fields(
    fields: Any, *, reject_keys: set[str] | None = None
) -> dict[str, Any]:
    """Return provider JSON fields that cannot replace the chat-completions contract."""
    if fields is None:
        return {}
    if not isinstance(fields, dict):
        raise ValueError("target.requestFields must be an object")
    if len(fields) > _MAX_REQUEST_FIELDS:
        raise ValueError("target.requestFields must contain at most 16 entries")
    blocked = set(_RESERVED_REQUEST_FIELDS)
    if reject_keys:
        blocked.update(reject_keys)
    cleaned: dict[str, Any] = {}
    for key, value in fields.items():
        if not isinstance(key, str) or _REQUEST_FIELD_KEY.fullmatch(key) is None:
            raise ValueError("target.requestFields keys must be identifiers")
        if key in blocked:
            raise ValueError(f"request field {key} is reserved")
        _json_value(value, 0)
        encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        if len(encoded.encode("utf-8")) > _MAX_REQUEST_FIELD_BYTES:
            raise ValueError("target.requestFields values must be at most 1024 bytes")
        cleaned[key] = value
    return cleaned


def allowlisted_provider_code(body: Any, secret: str = "") -> str:
    """Return a short provider error code, or an empty string when it is unsafe to keep."""
    if not isinstance(body, dict):
        return ""
    error = body.get("error")
    if not isinstance(error, dict):
        return ""
    code = error.get("code")
    if isinstance(code, bool) or code is None:
        return ""
    if isinstance(code, int):
        text = str(code)
    elif isinstance(code, str):
        text = code
    else:
        return ""
    if _PROVIDER_CODE.fullmatch(text) is None:
        return ""
    if secret and (text == secret or (len(secret) >= 8 and secret in text)):
        return ""
    return text


def retry_after_seconds(
    headers: Mapping[str, str] | Any | None,
    *,
    now: datetime | None = None,
) -> float | None:
    """Parse Retry-After as delta-seconds or an HTTP date. Invalid values are ignored."""
    if headers is None:
        return None
    getter = getattr(headers, "get", None)
    raw = getter("Retry-After") if callable(getter) else None
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        delta = float(text)
    except ValueError:
        delta = None
    if delta is not None and math.isfinite(delta):
        return max(0.0, delta)
    try:
        moment = parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    clock = now or datetime.now(timezone.utc)
    return max(0.0, (moment - clock).total_seconds())


def retry_delay_seconds(
    failed_attempts: int, retry_after: float | None, initial: float, maximum: float
) -> float:
    """Backoff for one retry. Retry-After wins and is capped; there is no jitter."""
    fallback = min(maximum, initial * (2 ** max(0, failed_attempts - 1)))
    if retry_after is None:
        return fallback
    return min(maximum, max(0.0, retry_after))


def _json_value(value: Any, depth: int) -> None:
    if depth > _MAX_REQUEST_FIELD_DEPTH:
        raise ValueError("target.requestFields is nested too deeply")
    if value is None or isinstance(value, str) or isinstance(value, bool):
        return
    if isinstance(value, int):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("target.requestFields values must be JSON values")
        return
    if isinstance(value, list):
        for item in value:
            _json_value(item, depth + 1)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("target.requestFields values must be JSON values")
            _json_value(item, depth + 1)
        return
    raise ValueError("target.requestFields values must be JSON values")


def _allowlisted_token(value: Any) -> str:
    if isinstance(value, bool) or value is None:
        return ""
    text = str(value) if isinstance(value, int) else value if isinstance(value, str) else ""
    if _PROVIDER_CODE.fullmatch(text) is None:
        return ""
    return text


def chat_completions_url(base_url: str) -> str:
    return base_url.rstrip("/") + "/chat/completions"


@dataclass
class _RateLimitEvent:
    status: int
    code: str


@dataclass
class _Ledger:
    """Token and model-call totals proven by completed HTTP responses.

    ``model_calls`` counts responses the adapter finished. A timeout, ``5xx``,
    redirect, or connection failure is left out, even when the provider may
    already have accepted that request. ``unknown_calls`` is the marker for
    those omitted calls. ``response()`` copies it to
    ``metadata["agentstorm.usage.unknown_calls"]`` and does not add it to
    ``model_call_count``. Retryable HTTP responses are recorded and are not
    model calls. A 429 adds no tokens. A terminal ``5xx`` is priced once by
    the caller, not once per retry.
    """

    model_calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    usage_known: bool = True
    unknown_calls: int = 0
    rate_limit_events: list[_RateLimitEvent] = field(default_factory=list)

    def note_retryable(self, status: int, code: str) -> None:
        self.rate_limit_events.append(_RateLimitEvent(status, code))

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
        if self.rate_limit_events:
            metadata[RATE_LIMIT_EVENTS_KEY] = len(self.rate_limit_events)
            codes = [event.code for event in self.rate_limit_events if event.code]
            if codes:
                metadata[RATE_LIMIT_CODES_KEY] = codes
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
        max_http_attempts: int = DEFAULT_HTTP_RETRY_ATTEMPTS,
        retry_initial_backoff_seconds: float = DEFAULT_HTTP_RETRY_INITIAL_SECONDS,
        retry_max_backoff_seconds: float = DEFAULT_HTTP_RETRY_MAX_SECONDS,
        retry_sleep: Callable[[float], Awaitable[None]] | None = None,
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
        if (
            isinstance(max_http_attempts, bool)
            or not isinstance(max_http_attempts, int)
            or not 1 <= max_http_attempts <= 8
        ):
            raise ValueError("max_http_attempts must be between 1 and 8")
        if (
            not isinstance(retry_initial_backoff_seconds, (int, float))
            or isinstance(retry_initial_backoff_seconds, bool)
            or not isinstance(retry_max_backoff_seconds, (int, float))
            or isinstance(retry_max_backoff_seconds, bool)
            or retry_initial_backoff_seconds < 0
            or retry_max_backoff_seconds < retry_initial_backoff_seconds
        ):
            raise ValueError("retry backoff must be non-negative and capped")
        extra = validate_request_fields(request_fields)
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
        self._max_http_attempts = max_http_attempts
        self._retry_initial = float(retry_initial_backoff_seconds)
        self._retry_max = float(retry_max_backoff_seconds)
        self._sleep = retry_sleep or asyncio.sleep

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
                status, body = await self._post_with_retry(payload, api_key.strip(), ledger)
            except asyncio.CancelledError:
                # Cancelling the task does not interrupt a socket operation that
                # is already blocked. urllib still applies its timeout to each
                # socket operation. Do not turn the cancel into a priced provider
                # error; the runner records it as cancelled.
                ledger.usage_known = False
                raise
            if status != 200:
                raise _status_error(
                    status,
                    ledger,
                    self._unknown_input_tokens,
                    self._unknown_output_tokens,
                    body=body,
                    api_key=api_key.strip(),
                )
            ledger.model_calls += 1
            try:
                message, usage, finish_reason = _parse_completion(body)
            except ValueError as exc:
                ledger.usage_known = False
                raise _error(
                    "malformed_response",
                    ambiguous=True,
                    ledger=ledger,
                    usage_complete=False,
                ) from exc
            ledger.add_usage(usage)
            finish = _allowlisted_token(finish_reason)
            tool_calls = message.get("tool_calls") or []
            if tool_calls:
                if ledger.model_calls >= self._max_model_calls:
                    break
                messages.append(message)
                tool_count += await self._execute_tools(tool_calls, messages, lifecycle, ledger)
                continue
            content = _message_text(message.get("content"))
            if finish == "length" and not (content or "").strip():
                raise _error(
                    "output_truncated",
                    ledger=ledger,
                    usage_complete=ledger.usage_known,
                    metadata={FINISH_REASON_KEY: finish},
                )
            if content is None:
                raise _error(
                    "model_behavior",
                    ambiguous=True,
                    ledger=ledger,
                    usage_complete=ledger.usage_known,
                )
            response = replace(ledger.response(content), tool_call_count=tool_count)
            if finish:
                metadata = dict(response.metadata)
                metadata[FINISH_REASON_KEY] = finish
                response = replace(response, metadata=metadata)
            return response
        raise _error(
            "max_turns_exceeded",
            category="evaluation",
            failure_kind="assertion",
            ledger=ledger,
            usage_complete=ledger.usage_known,
        )

    async def _post_with_retry(
        self, payload: dict[str, Any], api_key: str, ledger: _Ledger
    ) -> tuple[int, dict[str, Any] | None]:
        """Retry one chat call. Completed tool turns stay in ``payload``."""
        tries = 0
        while True:
            tries += 1
            status, body, headers = await asyncio.to_thread(self._post, payload, api_key, ledger)
            if status not in RETRYABLE_HTTP_STATUSES:
                return status, body
            ledger.note_retryable(status, allowlisted_provider_code(body, api_key))
            if tries >= self._max_http_attempts:
                return status, body
            delay = retry_delay_seconds(
                tries,
                retry_after_seconds(headers),
                self._retry_initial,
                self._retry_max,
            )
            await self._sleep(delay)

    def _post(
        self, payload: dict[str, Any], api_key: str, ledger: _Ledger
    ) -> tuple[int, dict[str, Any] | None, Any]:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "agentstorm-worker",
        }
        if api_key:
            headers["Authorization"] = "Bearer " + api_key
        request = urllib.request.Request(self._url, data=raw, headers=headers, method="POST")
        response_headers: Any = None
        try:
            with _OPENER.open(request, timeout=self._timeout) as response:
                status = int(response.status)
                response_headers = response.headers
                downloaded = response.read(_MAX_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as exc:
            status = int(exc.code)
            response_headers = exc.headers
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
            return status, None, response_headers
        try:
            parsed = json.loads(downloaded.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return status, None, response_headers
        if not isinstance(parsed, dict):
            return status, None, response_headers
        return status, parsed, response_headers

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


def _parse_completion(
    body: dict[str, Any] | None,
) -> tuple[dict[str, Any], tuple[int, int] | None, str]:
    if not isinstance(body, dict):
        raise ValueError("completion body is not an object")
    choices = body.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        raise ValueError("completion choices are missing")
    message = choices[0].get("message")
    if not isinstance(message, dict):
        raise ValueError("completion message is missing")
    finish_reason = choices[0].get("finish_reason")
    if finish_reason is not None and not isinstance(finish_reason, str):
        raise ValueError("finish_reason is invalid")
    assistant: dict[str, Any] = {"role": "assistant", "content": message.get("content")}
    tool_calls = message.get("tool_calls")
    if tool_calls is not None:
        if not isinstance(tool_calls, list):
            raise ValueError("tool_calls is not an array")
        assistant["tool_calls"] = tool_calls
    return assistant, _usage(body.get("usage")), finish_reason or ""


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
    metadata: dict[str, Any] | None = None,
) -> ExecutionError:
    """Keep measured tokens and price the one call that returned no usage."""
    usage_complete = ledger.charge_unknown_call(input_allowance, output_allowance)
    return _error(
        code,
        failure_kind=failure_kind,
        ambiguous=True,
        ledger=ledger,
        usage_complete=usage_complete,
        metadata=metadata,
    )


def _status_error(
    status: int,
    ledger: _Ledger,
    input_allowance: int,
    output_allowance: int,
    *,
    body: dict[str, Any] | None = None,
    api_key: str = "",
) -> ExecutionError:
    metadata: dict[str, Any] = {}
    provider_code = allowlisted_provider_code(body, api_key)
    if provider_code:
        metadata[PROVIDER_ERROR_CODE_KEY] = provider_code
    if status == 429:
        return _error(
            "rate_limited",
            safe_to_retry=True,
            ledger=ledger,
            usage_complete=ledger.usage_known,
            metadata=metadata,
        )
    if status == 408:
        return _priced_unknown(
            ledger,
            "timeout",
            input_allowance,
            output_allowance,
            failure_kind="timeout",
            metadata=metadata,
        )
    if 300 <= status < 400:
        return _priced_unknown(
            ledger, "http_redirect", input_allowance, output_allowance, metadata=metadata
        )
    if 500 <= status <= 599:
        return _priced_unknown(
            ledger, "http_5xx", input_allowance, output_allowance, metadata=metadata
        )
    if 400 <= status <= 499:
        return _error(
            "http_4xx", ledger=ledger, usage_complete=ledger.usage_known, metadata=metadata
        )
    return _priced_unknown(
        ledger, "unknown_error", input_allowance, output_allowance, metadata=metadata
    )


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
    metadata: dict[str, Any] | None = None,
) -> ExecutionError:
    response = ledger.response(model_calls=model_calls) if ledger is not None else None
    if response is not None and metadata:
        merged = dict(response.metadata)
        merged.update(metadata)
        response = replace(response, metadata=merged)
    return ExecutionError(
        category,
        code,
        failure_kind,
        ambiguous=ambiguous,
        safe_to_retry=safe_to_retry,
        response=response,
        usage_complete=usage_complete,
    )
