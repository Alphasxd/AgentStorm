from __future__ import annotations

import os
from contextlib import AbstractContextManager, nullcontext
from importlib.metadata import PackageNotFoundError, version
from typing import Protocol

AttributeValue = str | bool | int | float


class TraceSpan(Protocol):
    def set_attribute(self, name: str, value: AttributeValue) -> None: ...

    def set_error(self, error_type: str) -> None: ...


class TelemetryClient(Protocol):
    def start_span(
        self, name: str, attributes: dict[str, AttributeValue]
    ) -> AbstractContextManager[TraceSpan]: ...

    def shutdown(self) -> None: ...


class _NoopSpan:
    def set_attribute(self, name: str, value: AttributeValue) -> None:
        del name, value

    def set_error(self, error_type: str) -> None:
        del error_type


class NoopTelemetry:
    def start_span(
        self, name: str, attributes: dict[str, AttributeValue]
    ) -> AbstractContextManager[TraceSpan]:
        del name, attributes
        return nullcontext(_NoopSpan())

    def shutdown(self) -> None:
        return None


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


class OpenTelemetryClient:
    def __init__(self, tracer: object, provider: object) -> None:
        self._tracer = tracer
        self._provider = provider

    def start_span(
        self, name: str, attributes: dict[str, AttributeValue]
    ) -> AbstractContextManager[TraceSpan]:
        return _OpenTelemetrySpan(self._tracer, name, attributes)

    def shutdown(self) -> None:
        self._provider.shutdown()  # type: ignore[attr-defined]


def telemetry_from_environment() -> TelemetryClient:
    enabled = _boolean_environment("AGENTSTORM_OTEL_ENABLED", False)
    if not enabled:
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
    return OpenTelemetryClient(provider.get_tracer("agentstorm.worker"), provider)


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
