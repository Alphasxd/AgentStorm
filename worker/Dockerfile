FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app
RUN useradd --create-home --uid 10001 agentstorm
COPY pyproject.toml README.md ./
COPY src src
RUN pip install --no-cache-dir '.[openai]'
USER 10001:10001
ENTRYPOINT ["python", "-m", "agentstorm_worker"]
CMD ["run"]
