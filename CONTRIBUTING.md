# Contributing

## Development checks

```bash
make fmt
make vet
make test
make test-envtest
make test-benchmark
HELM3=/path/to/helm-3.21.0 HELM4=/path/to/helm-4.2.2 make helm-test
```

`make test-envtest` downloads pinned Kubernetes control-plane binaries into the ignored `.cache/`
directory and verifies CRD defaults, CEL immutability, and the status subresource against a real API
server. Use `make e2e-local` for the full no-cost local cluster path.

The E2E script builds and loads `agentstorm-controller:dev` and `agentstorm-worker:dev` by default.
Set `CONTROLLER_IMAGE` and `WORKER_IMAGE` to test other references. For already-published images, set
`SKIP_IMAGE_BUILD=true` and `LOAD_LOCAL_IMAGES=false` so kind pulls the requested references instead
of loading local Docker images.

When a macOS host proxy points at `127.0.0.1` or `localhost`, the E2E script translates it to
`host.docker.internal` only while creating a kind cluster. Set `KIND_PROXY_URL` to override the proxy
used for kind creation. An existing cluster whose node has a loopback proxy is rejected with a
recreation command; the script never changes the host-wide proxy configuration.

The Result API uses PostgreSQL for run metadata and MinIO-compatible S3 storage for gzipped raw shard
payloads. Its integration tests are opt-in and require `AGENTSTORM_TEST_DATABASE_URL` plus the
`AGENTSTORM_TEST_S3_*` variables before running `make test-results-integration`.

Run the complete development result stack on kind or OrbStack with:

```bash
CLUSTER_PROVIDER=kind make e2e-results-local
```

This command first runs the ordinary namespace-scoped controller E2E, then builds and loads the
current Result API, deploys the development-only PostgreSQL/MinIO overlay, checks Result API Secret
gating, runs two concurrent two-shard workloads, and verifies durable run, case, and comparison
queries. Source-image runs also deploy a development-only OpenTelemetry Collector, verify worker
run/case/provider/evaluator spans, persist them in Tempo, scrape Result API metrics into Prometheus,
and load the provisioned Grafana dashboard. The E2E queries a deterministic failed-case trace,
validates recording rules and dashboard drill-down queries, restarts Tempo and Prometheus to prove
PVC recovery, and asserts that prompts and expected values are absent from traces while case IDs,
run IDs, and bearer tokens are absent from Prometheus labels. Its
`agentstorm-result-storage` and `agentstorm-result-auth` Secrets contain test-only values, are labeled
`app.kubernetes.io/managed-by=agentstorm-e2e`, and never replace Secrets without that label. The
result E2E explicitly disables NetworkPolicies only in its local compatibility overlay because some
nested kind/OrbStack CNI combinations mis-handle standard port-only rules; the normal namespace E2E
and public `config/results` manifest retain those policies. Set `KEEP_E2E_RESOURCES=false` to delete
the result and observability stacks and their PVCs after the check.

Changes to the CRD must update all of the following in one pull request:

- `api/v1alpha1` Go types
- generated/deep-copy behavior
- CRD OpenAPI schema
- sample manifests
- controller and worker configuration contract
- relevant documentation and tests

Keep provider integrations behind the worker adapter interface. The controller must not import model
SDKs or depend on prompt/tool schemas.

See [the Adapter plugin guide](docs/adapter-plugins.md) before adding a Provider or domain Agent.
Changes are merged with rebase so the main history stays linear; CI runs on the pull request, and a
successful rebase merge does not trigger a duplicate main-branch pipeline.

Never commit API keys, real prompts, proprietary datasets, raw production traces, or generated result
files containing sensitive content.
