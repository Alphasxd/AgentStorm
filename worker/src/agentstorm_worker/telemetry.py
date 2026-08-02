from __future__ import annotations

import os
import json
import re
from contextlib import AbstractContextManager, contextmanager, nullcontext
from importlib.metadata import PackageNotFoundError, version
from typing import Iterator, Protocol

from .config import TelemetryConfig

AttributeValue = str | bool | int | float


class TraceSpan(Protocol):
    def set_attribute(self, name: str, value: AttributeValue) -> None: ...

    def set_error(self, error_type: str) -> None: ...


class DetachedTraceSpan(TraceSpan, Protocol):
    def end(self) -> None: ...


class TelemetryClient(Protocol):
    def start_span(
        self, name: str, attributes: dict[str, AttributeValue]
    ) -> AbstractContextManager[TraceSpan]: ...

    def start_detached_span(
        self, name: str, attributes: dict[str, AttributeValue]
    ) -> DetachedTraceSpan: ...

    def shutdown(self) -> None: ...


class _NoopSpan:
    def set_attribute(self, name: str, value: AttributeValue) -> None:
        del name, value

    def set_error(self, error_type: str) -> None:
        del error_type

    def end(self) -> None:
        return None


class NoopTelemetry:
    def start_span(
        self, name: str, attributes: dict[str, AttributeValue]
    ) -> AbstractContextManager[TraceSpan]:
        del name, attributes
        return nullcontext(_NoopSpan())

    def start_detached_span(
        self, name: str, attributes: dict[str, AttributeValue]
    ) -> DetachedTraceSpan:
        del name, attributes
        return _NoopSpan()

    def shutdown(self) -> None:
        return None


_SENSITIVE_KEY_SUFFIXES = (
    "apikey",
    "auth",
    "authorization",
    "cookie",
    "credential",
    "credentials",
    "passphrase",
    "password",
    "privatekey",
    "secret",
    "sessionid",
    "token",
)
_UNSUPPORTED = object()
_MAX_CONTENT_BYTES = 2048


class ContentSanitizer:
    def __init__(self, patterns: tuple[str, ...] = ()) -> None:
        self._patterns = tuple(re.compile(pattern) for pattern in patterns)

    def string(self, value: str) -> str:
        for pattern in self._patterns:
            value = pattern.sub("[REDACTED]", value)
        encoded = value.encode("utf-8")
        if len(encoded) <= _MAX_CONTENT_BYTES:
            return value
        return encoded[:_MAX_CONTENT_BYTES].decode("utf-8", errors="ignore")

    def attribute(self, value: AttributeValue) -> AttributeValue:
        return self.string(value) if isinstance(value, str) else value

    def sensitive_key(self, value: str) -> bool:
        return _sensitive_key(value)

    def content(self, value: object) -> str | None:
        if isinstance(value, str):
            return self.string(value)
        cleaned = self._clean(value)
        if cleaned is _UNSUPPORTED:
            return None
        try:
            rendered = json.dumps(
                cleaned,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError):
            return None
        return self.string(rendered)

    def _clean(self, value: object) -> object:
        if value is None or isinstance(value, (bool, int, float)):
            return value
        if isinstance(value, str):
            return self.string(value)
        if isinstance(value, dict):
            cleaned: dict[str, object] = {}
            for key, item in value.items():
                if not isinstance(key, str) or _sensitive_key(key):
                    continue
                sanitized = self._clean(item)
                if sanitized is not _UNSUPPORTED:
                    cleaned[key] = sanitized
            return cleaned
        if isinstance(value, (list, tuple)):
            cleaned_items = []
            for item in value:
                sanitized = self._clean(item)
                if sanitized is not _UNSUPPORTED:
                    cleaned_items.append(sanitized)
            return cleaned_items
        return _UNSUPPORTED


def _sensitive_key(value: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", value.lower())
    return any(normalized.endswith(suffix) for suffix in _SENSITIVE_KEY_SUFFIXES)


class _SanitizedSpan:
    def __init__(self, span: TraceSpan, sanitizer: ContentSanitizer) -> None:
        self._span = span
        self._sanitizer = sanitizer

    def set_attribute(self, name: str, value: AttributeValue) -> None:
        self._span.set_attribute(name, self._sanitizer.attribute(value))

    def set_error(self, error_type: str) -> None:
        self._span.set_error(error_type)


class _SanitizedDetachedSpan(_SanitizedSpan):
    def __init__(self, span: DetachedTraceSpan, sanitizer: ContentSanitizer) -> None:
        super().__init__(span, sanitizer)
        self._detached_span = span

    def end(self) -> None:
        self._detached_span.end()


class SanitizingTelemetry:
    def __init__(self, client: TelemetryClient, sanitizer: ContentSanitizer) -> None:
        self._client = client
        self._sanitizer = sanitizer

    @contextmanager
    def start_span(
        self, name: str, attributes: dict[str, AttributeValue]
    ) -> Iterator[TraceSpan]:
        sanitized = {
            key: self._sanitizer.attribute(value) for key, value in attributes.items()
        }
        with self._client.start_span(name, sanitized) as span:
            yield _SanitizedSpan(span, self._sanitizer)

    def start_detached_span(
        self, name: str, attributes: dict[str, AttributeValue]
    ) -> DetachedTraceSpan:
        sanitized = {
            key: self._sanitizer.attribute(value) for key, value in attributes.items()
        }
        return _SanitizedDetachedSpan(
            self._client.start_detached_span(name, sanitized), self._sanitizer
        )

    def shutdown(self) -> None:
        self._client.shutdown()


def telemetry_with_policy(client: TelemetryClient, config: TelemetryConfig) -> TelemetryClient:
    if config.content_mode != "redacted" or isinstance(client, SanitizingTelemetry):
        return client
    return SanitizingTelemetry(client, ContentSanitizer(config.redaction.patterns))


class _OpenTelemetrySpan:
    def __init__(self, tracer: object, name: str, attributes: dict[str, AttributeValue]) -> None:
        self._scope = tracer.start_as_current_span(  # type: ignore[attr-defined]
            name,
            attributes=attributes,
            record_exception=False,
            set_status_on_exception=False,
        )
        self._span: object | None = None

    def __enter__(self) -> _OpenTelemetrySpan:
        self._span = self._scope.__enter__()  # type: ignore[attr-defined]
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool | None:
        return self._scope.__exit__(exc_type, exc, traceback)  # type: ignore[attr-defined,no-any-return]

    def set_attribute(self, name: str, value: AttributeValue) -> None:
        if self._span is not None:
            self._span.set_attribute(name, value)  # type: ignore[attr-defined]

    def set_error(self, error_type: str) -> None:
        if self._span is None:
            return
        from opentelemetry.trace import Status, StatusCode

        self._span.set_attribute("error.type", error_type)  # type: ignore[attr-defined]
        self._span.set_status(Status(StatusCode.ERROR))  # type: ignore[attr-defined]


class _OpenTelemetryDetachedSpan:
    def __init__(self, tracer: object, name: str, attributes: dict[str, AttributeValue]) -> None:
        self._span = tracer.start_span(name, attributes=attributes)  # type: ignore[attr-defined]
        self._ended = False

    def set_attribute(self, name: str, value: AttributeValue) -> None:
        if not self._ended:
            self._span.set_attribute(name, value)  # type: ignore[attr-defined]

    def set_error(self, error_type: str) -> None:
        if self._ended:
            return
        from opentelemetry.trace import Status, StatusCode

        self._span.set_attribute("error.type", error_type)  # type: ignore[attr-defined]
        self._span.set_status(Status(StatusCode.ERROR))  # type: ignore[attr-defined]

    def end(self) -> None:
        if self._ended:
            return
        self._span.end()  # type: ignore[attr-defined]
        self._ended = True


class OpenTelemetryClient:
    def __init__(self, tracer: object, provider: object) -> None:
        self._tracer = tracer
        self._provider = provider

    def start_span(
        self, name: str, attributes: dict[str, AttributeValue]
    ) -> AbstractContextManager[TraceSpan]:
        return _OpenTelemetrySpan(self._tracer, name, attributes)

    def start_detached_span(
        self, name: str, attributes: dict[str, AttributeValue]
    ) -> DetachedTraceSpan:
        return _OpenTelemetryDetachedSpan(self._tracer, name, attributes)

    def shutdown(self) -> None:
        self._provider.shutdown()  # type: ignore[attr-defined]


def telemetry_from_environment(config: TelemetryConfig | None = None) -> TelemetryClient:
    config = config or TelemetryConfig()
    enabled = _boolean_environment("AGENTSTORM_OTEL_ENABLED", False)
    if not enabled:
        if config.content_mode == "redacted":
            raise RuntimeError("redacted telemetry requires OpenTelemetry to be enabled")
        return NoopTelemetry()
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError as exc:
        raise RuntimeError(
            "OpenTelemetry is enabled but telemetry dependencies are not installed; "
            "install agentstorm-worker[telemetry]"
        ) from exc

    service_name = os.getenv("OTEL_SERVICE_NAME", "agentstorm-worker")
    provider = TracerProvider(
        resource=Resource.create(
            {
                "service.name": service_name,
                "service.version": _package_version(),
            }
        )
    )
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    return telemetry_with_policy(
        OpenTelemetryClient(provider.get_tracer("agentstorm.worker"), provider), config
    )


def _boolean_environment(name: str, fallback: bool) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return fallback
    normalized = value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError(f"{name} must be true or false")


def _package_version() -> str:
    try:
        return version("agentstorm-worker")
    except PackageNotFoundError:
        return "development"
