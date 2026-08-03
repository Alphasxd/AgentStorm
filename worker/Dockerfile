FROM python:3.13-slim@sha256:6771159cd4fa5d9bba1258caf0b82e6b73458c694d178ad97c5e925c2d0e1a91
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app
RUN useradd --create-home --uid 10001 agentstorm
COPY pyproject.toml README.md ./
COPY src src
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install '.[openai,telemetry]'
USER 10001:10001
ENTRYPOINT ["python", "-m", "agentstorm_worker"]
CMD ["run"]
