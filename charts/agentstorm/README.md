# AgentStorm Helm chart

The chart installs the `AgentTestRun` CRD and one namespace-scoped Controller by default. It does
not bundle PostgreSQL, S3-compatible storage, or KEDA.

```bash
helm install agentstorm oci://ghcr.io/alphasxd/charts/agentstorm \
  --version 0.6.0 --namespace agentstorm-system --create-namespace
```

Use `controller.image.digest` and `resultApi.image.digest` to override tags with `sha256:...` image-
index digests. Worker images remain explicit per `AgentTestRun.runner.image`.

## Optional Result API

`resultApi.enabled=true` requires:

- `resultApi.existingStorageSecret`, with keys for the PostgreSQL URL and S3 access/secret keys;
- `resultApi.existingAuthSecret`, with separate write/read token keys;
- `resultApi.s3Endpoint` and non-secret bucket/region settings.

The chart never accepts token, password, database URL, or S3 credential values directly. It creates
no Secret and only emits `secretKeyRef` entries. PostgreSQL and object storage must already exist.

## Optional KEDA

`keda.enabled=true` requires the KEDA CRDs/operator plus an enabled in-chart or external Result API.
The queue-status URL is credential-free and must be protected by network policy outside trusted
namespaces. Distributed limits are admission controls, not billing guarantees.

`networkPolicy.enabled` is off by default because API Server, DNS, OTLP, external Provider, and
external storage addresses differ between clusters. Enable and extend the supplied baseline only
after verifying the CNI-specific egress requirements.
